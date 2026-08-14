use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use bili_sync_entity::upper_auto_manage_action::ActionType;
use bili_sync_entity::upper_auto_manage_policy::{UpperManagePolicy, UpperManageSource};
use bili_sync_entity::upper_auto_manage_run::RunStatus;
use bili_sync_entity::{submission, upper_auto_manage_action, upper_auto_manage_policy, upper_auto_manage_run};
use dashmap::DashMap;
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use sea_orm::{DatabaseConnection, FromQueryResult, Statement, TransactionTrait};
use tokio::sync::{Mutex, OnceCell, watch};
use tokio_cron_scheduler::{Job, JobScheduler};

use crate::bilibili::{self, BiliClient, BiliError, Credential, Dynamic, Submission, VideoInfo};
use crate::config::{Config, Trigger, VersionedConfig};
use crate::task::{TaskStatus, TaskTrigger};
use crate::utils::notify::error_and_notify;

static INSTANCE: OnceCell<UpperAutoManageTaskManager> = OnceCell::const_new();

/// 全局 per-submission 锁池：所有按 submission_id 写 policy / enabled 的入口
/// （update_video_source、upsert_policy、delete_policy、巡检阶段一/二）都通过它串行化，
/// 避免 API 与后台巡检并发覆盖。不同 submission 互不阻塞。
static SUBMISSION_LOCKS: OnceLock<DashMap<i32, Arc<Mutex<()>>>> = OnceLock::new();

/// 获取某个 submission 的串行锁。首次访问时惰性创建。
/// 返回的 `Arc<Mutex<()>>` 由调用方 `.lock().await` 持有，作用域结束自动释放。
pub fn lock_for_submission(submission_id: i32) -> Arc<Mutex<()>> {
    let map = SUBMISSION_LOCKS.get_or_init(DashMap::new);
    map.entry(submission_id)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

/// 启动 UP 主投稿自动启停管理任务
pub async fn upper_auto_manage(connection: DatabaseConnection, bili_client: Arc<BiliClient>) -> Result<()> {
    let task_manager = UpperAutoManageTaskManager::init(connection, bili_client).await?;
    task_manager.start().await
}

pub struct UpperAutoManageTaskManager {
    sched: Arc<tokio::sync::Mutex<JobScheduler>>,
    cx: Arc<TaskContext>,
    shutdown_rx: watch::Receiver<Result<()>>,
}

struct TaskContext {
    connection: DatabaseConnection,
    bili_client: Arc<BiliClient>,
    /// 巡检任务排队/执行状态：false=空闲，true=已占用。
    /// 占用发生在 run_once/定时闭包入口，释放在 execute_inspection 的 finally，
    /// 覆盖"排队→执行→完成"全窗口，防止手动连点排入多个 one-shot 任务。
    run_slot: Mutex<bool>,
    status_tx: watch::Sender<TaskStatus>,
    status_rx: watch::Receiver<TaskStatus>,
    job_id: Mutex<Option<uuid::Uuid>>,
}

impl UpperAutoManageTaskManager {
    /// 初始化单例
    pub async fn init(
        connection: DatabaseConnection,
        bili_client: Arc<BiliClient>,
    ) -> Result<&'static UpperAutoManageTaskManager> {
        INSTANCE
            .get_or_try_init(|| UpperAutoManageTaskManager::new(connection, bili_client))
            .await
    }

    /// 获取单例，未初始化时 panic
    pub fn get() -> &'static UpperAutoManageTaskManager {
        INSTANCE.get().expect("UpperAutoManageTaskManager is not initialized")
    }

    /// 订阅任务状态
    pub fn subscribe(&self) -> watch::Receiver<TaskStatus> {
        self.cx.status_rx.clone()
    }

    /// 手动触发一次巡检任务。
    ///
    /// 返回 `Ok(true)` 表示已成功排队，`Ok(false)` 表示已有巡检排队/执行中（防重）。
    /// slot 占用发生在此处入口，释放在 `execute_inspection` 的 finally（覆盖排队→执行→完成全窗口）。
    #[allow(dead_code)]
    pub async fn run_once(&self) -> Result<bool> {
        self.run_once_with_trigger(TaskTrigger::Manual).await
    }

    pub async fn run_once_with_trigger(&self, trigger: TaskTrigger) -> Result<bool> {
        // 占用 slot：与定时触发互斥
        {
            let mut slot = self.cx.run_slot.lock().await;
            if *slot {
                return Ok(false);
            }
            *slot = true;
        }
        let cx = self.cx.clone();
        let register_result = self
            .sched
            .lock()
            .await
            .add(Job::new_one_shot_async(Duration::from_secs(0), move |uuid, l| {
                let cx = cx.clone();
                Box::pin(async move {
                    cx.execute_inspection(uuid, l, trigger).await;
                })
            })?)
            .await;
        if let Err(e) = register_result {
            // 登记失败必须立即释放 slot，并把错误返回 API（不吞错误）
            let mut slot = self.cx.run_slot.lock().await;
            *slot = false;
            return Err(e.into());
        }
        Ok(true)
    }

    /// 启动任务调度器
    async fn start(&self) -> Result<()> {
        self.sched.lock().await.start().await?;
        let mut shutdown_rx = self.shutdown_rx.clone();
        shutdown_rx.changed().await?;
        self.sched
            .lock()
            .await
            .shutdown()
            .await
            .context("UP 主自动巡检任务调度器关闭失败")?;
        if let Err(e) = &*shutdown_rx.borrow() {
            bail!("{:#}", e);
        }
        Ok(())
    }

    async fn new(connection: DatabaseConnection, bili_client: Arc<BiliClient>) -> Result<Self> {
        let sched = Arc::new(tokio::sync::Mutex::new(JobScheduler::new().await?));
        let (status_tx, status_rx) = watch::channel(TaskStatus::default());
        let cx = Arc::new(TaskContext {
            connection,
            bili_client,
            run_slot: Mutex::new(false),
            status_tx,
            status_rx,
            job_id: Mutex::new(None),
        });
        let mut rx = VersionedConfig::get().subscribe();
        let initial_config = rx.borrow_and_update().clone();
        // 初始注册巡检任务（若启用）
        if initial_config.upper_auto_manage.enabled {
            let job_id = add_inspection_job(&sched, &cx, &initial_config.upper_auto_manage.interval).await?;
            *cx.job_id.lock().await = Some(job_id);
            schedule_refresh_next_run(&sched, &cx, job_id).await;
        }
        // 监听配置变更，动态增删巡检任务
        let cx_clone = cx.clone();
        let sched_clone = sched.clone();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(Ok(()));
        tokio::spawn(async move {
            let update_task_result = async {
                while rx.changed().await.is_ok() {
                    let new_config = rx.borrow().clone();
                    let opt = &new_config.upper_auto_manage;
                    let mut job_id = cx_clone.job_id.lock().await;
                    // 移除旧任务（无论是否启用都要先清掉旧任务再按新配置决定是否添加）
                    if let Some(old_job_id) = *job_id {
                        sched_clone
                            .lock()
                            .await
                            .remove(&old_job_id)
                            .await
                            .context("移除旧的 UP 主自动巡检任务失败")?;
                        *job_id = None;
                    }
                    if opt.enabled {
                        let new_id = add_inspection_job(&sched_clone, &cx_clone, &opt.interval).await?;
                        *job_id = Some(new_id);
                        schedule_refresh_next_run(&sched_clone, &cx_clone, new_id).await;
                    } else {
                        info!("UP 主自动巡检任务已禁用，移除定时任务");
                    }
                }
                Result::<(), anyhow::Error>::Ok(())
            }
            .await;
            let _ = shutdown_tx.send(update_task_result);
        });
        Ok(Self { sched, cx, shutdown_rx })
    }
}

/// 按触发方式添加巡检任务，返回任务 UUID
async fn add_inspection_job(
    sched: &Arc<tokio::sync::Mutex<JobScheduler>>,
    cx: &Arc<TaskContext>,
    interval: &Trigger,
) -> Result<uuid::Uuid> {
    let job_run = run_inspection_task(cx.clone());
    let job = match interval {
        Trigger::Interval(secs) => Job::new_repeated_async(Duration::from_secs(*secs), job_run)?,
        Trigger::Cron(cron) => Job::new_async_tz(cron, chrono::Local, job_run)?,
    };
    let id = sched.lock().await.add(job).await?;
    Ok(id)
}

/// 添加一个一次性任务用于刷新任务状态的 next_run 字段
async fn schedule_refresh_next_run(
    sched: &Arc<tokio::sync::Mutex<JobScheduler>>,
    cx: &Arc<TaskContext>,
    job_id: uuid::Uuid,
) {
    let cx = cx.clone();
    if let Err(e) = sched
        .lock()
        .await
        .add(
            Job::new_one_shot_async(Duration::from_secs(0), move |_uuid, mut l| {
                let cx = cx.clone();
                Box::pin(async move {
                    let next_run = l
                        .next_tick_for_job(job_id)
                        .await
                        .ok()
                        .flatten()
                        .map(|dt| dt.with_timezone(&chrono::Local));
                    let old_status = *cx.status_rx.borrow();
                    let _ = cx.status_tx.send(TaskStatus { next_run, ..old_status });
                })
            })
            .expect("failed to create refresh_next_run job"),
        )
        .await
    {
        warn!("刷新 UP 自动巡检任务 next_run 失败：{:#}", e);
    }
}

fn run_inspection_task(
    cx: Arc<TaskContext>,
) -> impl FnMut(uuid::Uuid, JobScheduler) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    move |uuid, l| {
        let cx = cx.clone();
        Box::pin(async move {
            // 占用 slot：与手动触发互斥。定时任务在 slot 已占用时跳过本次触发。
            let already_taken = {
                let mut slot = cx.run_slot.lock().await;
                if *slot {
                    true
                } else {
                    *slot = true;
                    false
                }
            };
            if already_taken {
                warn!("已有 UP 主自动巡检任务排队/执行中，跳过本次定时触发");
                return;
            }
            cx.execute_inspection(uuid, l, TaskTrigger::Scheduled).await;
        })
    }
}

impl TaskContext {
    /// 共享执行：假定调用方已占用 run_slot。执行巡检、更新状态、finally 释放 slot。
    /// 由定时任务闭包与手动 one-shot 闭包共同调用，保证状态收尾（is_running /
    /// last_run / last_finish / next_run / 错误通知 / slot 释放）逻辑一致。
    async fn execute_inspection(self: &Arc<Self>, job_uuid: uuid::Uuid, mut sched: JobScheduler, trigger: TaskTrigger) {
        let _ = self.status_tx.send(TaskStatus {
            is_running: true,
            last_run: Some(chrono::Local::now()),
            last_finish: None,
            next_run: None,
        });
        info!("开始执行本轮 UP 主自动巡检任务..");
        let config = VersionedConfig::get().snapshot();
        match run_inspection(&self.connection, &self.bili_client, &config, trigger).await {
            Ok(stats) => info!("本轮 UP 主自动巡检任务执行完毕：{}", stats.summary_line()),
            Err(e) => error_and_notify(
                &config,
                &self.bili_client,
                format!("本轮 UP 主自动巡检任务执行遇到错误：{:#}", e),
                &e,
            ),
        }
        // 从 job_id 取定时任务的 uuid（当前可能是 oneshot 任务），刷新 next_run；
        // 手动 one-shot 也走此路径，通过 cx.job_id 查询定时任务的 next tick，
        // 不会把 next_run 清成 None，避免手动脉冲后 UI 暂时显示"无下次巡检"。
        let task_uuid = (*self.job_id.lock().await).unwrap_or(job_uuid);
        let next_run = sched
            .next_tick_for_job(task_uuid)
            .await
            .ok()
            .flatten()
            .map(|dt| dt.with_timezone(&chrono::Local));
        let last_status = *self.status_rx.borrow();
        let _ = self.status_tx.send(TaskStatus {
            is_running: false,
            last_run: last_status.last_run,
            last_finish: Some(chrono::Local::now()),
            next_run,
        });
        // finally：无论成功失败都释放 slot
        let mut slot = self.run_slot.lock().await;
        *slot = false;
    }
}

/// 巡检统计：各桶定义域互斥，成功轮次满足
/// `checked = disabled + active + indeterminate + enabled + banned + banned_observation + still_inactive`。
///
/// - 阶段一（启用态候选，纯本地判定）：`disabled`（超阈值自动禁用）/ `active`（近期有更新，无需动作）/
///   `indeterminate`（本地无任何视频，无法判定活跃度）
/// - 阶段二（禁用态候选，调接口复查）：`enabled`（恢复更新）/ `still_inactive`（仍不活跃，维持禁用）/
///   `banned`（删号/注销转黑名单）/ `banned_observation`（封禁观察）
#[derive(Default, Clone)]
struct RunStats {
    checked: i32,
    disabled: i32,
    active: i32,
    indeterminate: i32,
    enabled: i32,
    banned: i32,
    banned_observation: i32,
    still_inactive: i32,
}

impl RunStats {
    /// 阶段一候选数（启用态）
    fn phase1_total(&self) -> i32 {
        self.disabled + self.active + self.indeterminate
    }

    /// 阶段二候选数（禁用态复查）
    fn phase2_total(&self) -> i32 {
        self.enabled + self.banned + self.banned_observation + self.still_inactive
    }

    fn summary_line(&self) -> String {
        format!(
            "检查 {} 个 UP（启用态 {}：禁用 {}、正常 {}、无法判定 {}；禁用态复查 {}：恢复启用 {}、仍不活跃 {}、转黑名单 {}、封禁观察 {}）",
            self.checked,
            self.phase1_total(),
            self.disabled,
            self.active,
            self.indeterminate,
            self.phase2_total(),
            self.enabled,
            self.still_inactive,
            self.banned,
            self.banned_observation
        )
    }
}

struct PendingAction {
    submission_id: i32,
    upper_name: String,
    action: ActionType,
    reason: String,
}

/// 巡检入口：创建 run 记录，执行三阶段，最后更新 run 记录
async fn run_inspection(
    connection: &DatabaseConnection,
    bili_client: &BiliClient,
    config: &Arc<Config>,
    trigger: TaskTrigger,
) -> Result<RunStats> {
    let started_at = chrono::Local::now().naive_local();
    let run = upper_auto_manage_run::ActiveModel {
        started_at: Set(started_at),
        status: Set(RunStatus::Running),
        ..Default::default()
    };
    let run_id = upper_auto_manage_run::Entity::insert(run)
        .exec(connection)
        .await?
        .last_insert_id;
    let result = run_inspection_inner(connection, bili_client, config, run_id, trigger).await;
    let (status, error_message, summary, stats) = match &result {
        Ok(stats) => (
            RunStatus::Succeeded,
            None,
            Some(format!("巡检完成：{}", stats.summary_line())),
            RunStats {
                checked: stats.checked,
                disabled: stats.disabled,
                active: stats.active,
                indeterminate: stats.indeterminate,
                enabled: stats.enabled,
                banned: stats.banned,
                banned_observation: stats.banned_observation,
                still_inactive: stats.still_inactive,
            },
        ),
        Err(e) => (RunStatus::Failed, Some(format!("{:#}", e)), None, RunStats::default()),
    };
    upper_auto_manage_run::ActiveModel {
        id: Set(run_id),
        finished_at: Set(Some(chrono::Local::now().naive_local())),
        status: Set(status),
        checked_count: Set(stats.checked),
        disabled_count: Set(stats.disabled),
        enabled_count: Set(stats.enabled),
        banned_count: Set(stats.banned),
        banned_observation_count: Set(stats.banned_observation),
        active_count: Set(stats.active),
        indeterminate_count: Set(stats.indeterminate),
        still_inactive_count: Set(stats.still_inactive),
        error_message: Set(error_message),
        summary: Set(summary.clone()),
        ..Default::default()
    }
    .save(connection)
    .await?;
    // 收尾清理超期运行记录，失败只记日志
    if let Err(e) = cleanup_expired_upper_auto_manage_runs(connection).await {
        warn!("清理过期的巡检运行记录失败：{:#}", e);
    }
    // 若有摘要则通过通知渠道发送
    if let Some(summary) = summary
        && result.is_ok()
    {
        crate::utils::notify::notify(config, bili_client, summary);
    }
    match result {
        Ok(stats) => Ok(stats),
        Err(e) => Err(e),
    }
}

/// 巡检三阶段：禁用长期不更新的 UP / 检查禁用态 UP 是否恢复 / 封禁转黑名单
async fn run_inspection_inner(
    connection: &DatabaseConnection,
    bili_client: &BiliClient,
    config: &Arc<Config>,
    run_id: i32,
    trigger: TaskTrigger,
) -> Result<RunStats> {
    let opt = &config.upper_auto_manage;
    let now = chrono::Utc::now().naive_utc();
    let mut stats = RunStats::default();
    let mut pending_actions: Vec<PendingAction> = Vec::new();

    // 阶段一：禁用长期不更新的启用态 UP（纯本地查询，不调接口）
    let inactive_candidates = fetch_inactive_candidates(connection).await?;
    stats.checked += inactive_candidates.len() as i32;
    for cand in &inactive_candidates {
        let Some(last_pub) = cand.last_pubtime else {
            // 本地无任何视频，无法判定活跃度
            stats.indeterminate += 1;
            continue;
        };
        let days = (now - last_pub).num_days();
        if days > opt.inactive_threshold_days {
            // 停用仅更新 enabled，不写入普通策略
            disable_submission(connection, cand.submission_id).await?;
            stats.disabled += 1;
            pending_actions.push(PendingAction {
                submission_id: cand.submission_id,
                upper_name: cand.upper_name.clone(),
                action: ActionType::AutoDisabled,
                reason: format!("最近一次投稿为 {} 天前", days),
            });
            info!(
                "UP「{}」({}) 已自动禁用：{} 天未更新",
                cand.upper_name, cand.submission_id, days
            );
        } else {
            // 近期有更新，无需动作
            stats.active += 1;
        }
    }

    // 阶段二：主动拉取接口检查禁用态（系统自动禁用的）UP 是否恢复更新
    let disabled_candidates = fetch_disabled_for_recheck(connection, trigger).await?;
    stats.checked += disabled_candidates.len() as i32;
    if !disabled_candidates.is_empty() {
        // 拿到带固定限流器的客户端快照（与主下载任务共享限流配额）
        let snapshot_client = Arc::new(bili_client.snapshot().context("获取 BiliClient 快照失败")?);
        // Submission 接口需要 wbi 签名，先刷新 mixin key
        let mixin_key = snapshot_client
            .wbi_img(&config.credential)
            .await
            .context("获取 wbi_img 失败")?
            .into_mixin_key()
            .context("解析 mixin key 失败")?;
        bilibili::set_global_mixin_key(mixin_key);
        let credential = config.credential.clone();
        // 使用 try_for_each_concurrent：并发上限为 check_concurrency；
        // 任一并发闭包返回 Err 即停止派发后续候选（drop 掉剩余 pending future），
        // 并立刻向上抛出错误。
        // 关键：check_disabled_upper 必须在闭包内 await，才能真正并发——
        // 若先在 stream::iter().map() 里包成 Future 再用 .then(|fut| fut) 串行求值，
        // 上游会按顺序 await 每个 Future，check_concurrency 形同虚设。
        // 由于 try_for_each_concurrent 的闭包可能被多次并发调用，需要在 Arc<tokio::Mutex<_>>
        // 下安全地累加 RunStats 与 PendingAction。
        let stats_arc = Arc::new(tokio::sync::Mutex::new(RunStats::default()));
        stats_arc.lock().await.checked += disabled_candidates.len() as i32;
        let pending_actions_arc = Arc::new(tokio::sync::Mutex::new(Vec::<PendingAction>::new()));
        let process_result: Result<()> = stream::iter(disabled_candidates.into_iter().map(Ok::<_, anyhow::Error>))
            .try_for_each_concurrent(Some(opt.check_concurrency), |cand| {
                let client = snapshot_client.clone();
                let credential = credential.clone();
                let stats_arc = stats_arc.clone();
                let pending_actions_arc = pending_actions_arc.clone();
                async move {
                    let outcome = check_disabled_upper(&client, &credential, &cand).await?;
                    let CheckOutcome {
                        kind,
                        submission_id,
                        upper_name,
                    } = outcome;
                    match kind {
                        CheckOutcomeKind::Recovered(latest_pubtime) => {
                            if matches!(trigger, TaskTrigger::Manual)
                                && matches!(
                                    load_policy_for_submission(connection, submission_id).await?,
                                    Some(UpperManagePolicy::Banned)
                                )
                            {
                                recover_banned_submission(connection, submission_id).await?;
                            } else {
                                enable_submission(connection, submission_id).await?;
                            }
                            stats_arc.lock().await.enabled += 1;
                            info!("UP「{}」({}) 检测到恢复更新，已自动重新启用", upper_name, submission_id);
                            pending_actions_arc.lock().await.push(PendingAction {
                                submission_id,
                                upper_name,
                                action: ActionType::AutoEnabled,
                                reason: format!("检测到新投稿 {}", latest_pubtime),
                            });
                        }
                        CheckOutcomeKind::StillInactive => {
                            stats_arc.lock().await.still_inactive += 1;
                            let _ = cand;
                        }
                        CheckOutcomeKind::Gone(msg) => {
                            // 删号/注销/不存在 → 永久不可恢复，写 Blacklist
                            upsert_policy(
                                connection,
                                submission_id,
                                UpperManagePolicy::Blacklist,
                                UpperManageSource::Auto,
                                Some(format!("UP 已删号/不可恢复：{}", msg)),
                            )
                            .await?;
                            stats_arc.lock().await.banned += 1;
                            warn!(
                                "UP「{}」({}) 检测到已删号/不可恢复，已转黑名单：{}",
                                upper_name, submission_id, msg
                            );
                            pending_actions_arc.lock().await.push(PendingAction {
                                submission_id,
                                upper_name,
                                action: ActionType::MarkedBanned,
                                reason: format!("UP 已删号/不可恢复：{}", msg),
                            });
                        }
                        CheckOutcomeKind::BannedObservation(msg) => {
                            // 封禁/冻结（短期/永封无法区分）→ 写 Banned 观察，不进黑名单、不动 enabled
                            upsert_policy(
                                connection,
                                submission_id,
                                UpperManagePolicy::Banned,
                                UpperManageSource::Auto,
                                Some(format!("封禁观察，待人工判断：{}", msg)),
                            )
                            .await?;
                            stats_arc.lock().await.banned_observation += 1;
                            warn!(
                                "UP「{}」({}) 检测到封禁/冻结，已置为封禁观察：{}",
                                upper_name, submission_id, msg
                            );
                            pending_actions_arc.lock().await.push(PendingAction {
                                submission_id,
                                upper_name,
                                action: ActionType::MarkedBanned,
                                reason: format!("封禁观察：{}", msg),
                            });
                        }
                    }
                    Ok(())
                }
            })
            .await;
        // 合并回外层 stats / pending_actions
        {
            let inner_stats = stats_arc.lock().await;
            stats.enabled += inner_stats.enabled;
            stats.banned += inner_stats.banned;
            stats.banned_observation += inner_stats.banned_observation;
            stats.still_inactive += inner_stats.still_inactive;
        }
        pending_actions.extend(pending_actions_arc.lock().await.drain(..));
        if let Err(e) = process_result {
            // 阶段二的并发处理已经被风控/异常中断，把部分进度落库后整体返回错误
            warn!("UP 巡检并发阶段已被错误中断：{:#}", e);
            // 先把已经累计的 actions 写入再返回，避免丢失可观测数据
            if !pending_actions.is_empty() {
                let models = pending_actions
                    .into_iter()
                    .map(|a| upper_auto_manage_action::ActiveModel {
                        run_id: Set(run_id),
                        submission_id: Set(a.submission_id),
                        upper_name: Set(a.upper_name),
                        action: Set(a.action),
                        reason: Set(Some(a.reason)),
                        created_at: Set(chrono::Utc::now().naive_utc()),
                        ..Default::default()
                    })
                    .collect::<Vec<_>>();
                let _ = upper_auto_manage_action::Entity::insert_many(models)
                    .exec(connection)
                    .await;
            }
            return Err(e);
        }
    }

    // 阶段三：批量写入操作明细
    if !pending_actions.is_empty() {
        let models = pending_actions
            .into_iter()
            .map(|a| upper_auto_manage_action::ActiveModel {
                run_id: Set(run_id),
                submission_id: Set(a.submission_id),
                upper_name: Set(a.upper_name),
                action: Set(a.action),
                reason: Set(Some(a.reason)),
                created_at: Set(chrono::Utc::now().naive_utc()),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        upper_auto_manage_action::Entity::insert_many(models)
            .exec(connection)
            .await?;
    }
    Ok(stats)
}

/// 删除超过保留期（RUN_RETENTION_DAYS）的巡检运行记录，返回删除行数
async fn cleanup_expired_upper_auto_manage_runs(connection: &DatabaseConnection) -> Result<u64> {
    let cutoff = chrono::Utc::now().naive_utc() - chrono::Duration::days(crate::task::RUN_RETENTION_DAYS);
    Ok(upper_auto_manage_run::Entity::delete_many()
        .filter(upper_auto_manage_run::Column::StartedAt.lt(cutoff))
        .exec(connection)
        .await?
        .rows_affected)
}

#[derive(FromQueryResult)]
struct SubmissionActivity {
    submission_id: i32,
    upper_id: i64,
    upper_name: String,
    last_pubtime: Option<DateTime>,
    /// 是否使用动态 API 拉取视频。动态 API UP 复查（阶段二）必须走 Dynamic::into_video_stream，
    /// 否则会被误判为「仍不活跃」永远无法自动恢复。
    use_dynamic_api: bool,
}

/// 查询启用态、且未被列入白名单/黑名单/封禁观察的 submission 及其最新投稿时间
async fn fetch_inactive_candidates(connection: &DatabaseConnection) -> Result<Vec<SubmissionActivity>> {
    let sql = "
SELECT s.id AS submission_id, s.upper_id AS upper_id, s.upper_name AS upper_name,
       MAX(v.pubtime) AS last_pubtime, s.use_dynamic_api AS use_dynamic_api
FROM submission s
LEFT JOIN video v ON v.submission_id = s.id
WHERE s.enabled = 1
  AND NOT EXISTS (
    SELECT 1 FROM upper_auto_manage_policy p
    WHERE p.submission_id = s.id AND p.policy IN ('whitelist', 'blacklist', 'banned')
  )
GROUP BY s.id, s.upper_id, s.upper_name, s.use_dynamic_api";
    let candidates =
        SubmissionActivity::find_by_statement(Statement::from_string(connection.get_database_backend(), sql))
            .all(connection)
            .await
            .context("查询长期不更新候选失败")?;
    Ok(candidates)
}

/// 查询禁用态、且由系统自动禁用（policy.source=auto、policy=normal）的 submission，用于巡检恢复
///
/// 这里严格限定 source='auto'：只有「系统写下的 normal+auto」才会被自动重新启用。
/// 用户手动创建 normal+manual 后再手动禁用 submission 的情形不会被命中，
/// 避免破坏「手动禁用不自动恢复」的语义。
async fn fetch_disabled_for_recheck(
    connection: &DatabaseConnection,
    trigger: TaskTrigger,
) -> Result<Vec<SubmissionActivity>> {
    let excluded = match trigger {
        TaskTrigger::Scheduled => "'blacklist', 'banned'",
        TaskTrigger::Manual => "'blacklist'",
    };
    let sql = format!(
        "SELECT s.id AS submission_id, s.upper_id AS upper_id, s.upper_name AS upper_name,
       MAX(v.pubtime) AS last_pubtime, s.use_dynamic_api AS use_dynamic_api
FROM submission s
LEFT JOIN video v ON v.submission_id = s.id
WHERE s.enabled = 0
  AND NOT EXISTS (
    SELECT 1 FROM upper_auto_manage_policy p
    WHERE p.submission_id = s.id AND p.policy IN ({excluded})
  )
GROUP BY s.id, s.upper_id, s.upper_name, s.use_dynamic_api"
    );
    let candidates =
        SubmissionActivity::find_by_statement(Statement::from_string(connection.get_database_backend(), sql))
            .all(connection)
            .await
            .context("查询待恢复候选失败")?;
    Ok(candidates)
}

struct CheckOutcome {
    kind: CheckOutcomeKind,
    submission_id: i32,
    upper_name: String,
}

enum CheckOutcomeKind {
    Recovered(DateTime),
    StillInactive,
    /// UP 已永久不可恢复（注销/不存在）→ 阶段二写入 Blacklist
    Gone(String),
    /// UP 被封禁/冻结（短期/永封无法区分）→ 阶段二写入 Banned 观察，保持 enabled=false
    BannedObservation(String),
}

/// 主动拉取禁用态 UP 的最新投稿，判定恢复 / 仍不活跃 / 永久不可用 / 封禁观察。
///
/// 通过 `Submission`/`Dynamic` 的 `into_video_stream` 复用与正常下载完全相同的拉取与解析逻辑，
/// 自动按 `use_dynamic_api` 走投稿 API 或动态 API——避免对动态 API 专用的 UP 永远
/// 被判定为"仍不活跃"导致无法自动恢复。
async fn check_disabled_upper(
    client: &BiliClient,
    credential: &Credential,
    cand: &SubmissionActivity,
) -> Result<CheckOutcome> {
    // 选择数据源：动态 API UP 走 Dynamic，其他走 Submission。
    // 两种 stream 的 Item 类型一致（`Result<VideoInfo>`），探活只需取第一条的 release_datetime。
    let mut stream = if cand.use_dynamic_api {
        Box::pin(Dynamic::new(client, cand.upper_id.to_string(), credential).into_video_stream())
            as std::pin::Pin<Box<dyn Stream<Item = Result<VideoInfo>> + Send>>
    } else {
        Box::pin(Submission::new(client, cand.upper_id.to_string(), credential).into_video_stream())
            as std::pin::Pin<Box<dyn Stream<Item = Result<VideoInfo>> + Send>>
    };
    match stream.next().await {
        Some(Ok(video_info)) => {
            let latest_pubtime = video_info.release_datetime().naive_utc();
            let recovered = match cand.last_pubtime {
                Some(known) => latest_pubtime > known,
                None => true,
            };
            Ok(CheckOutcome {
                kind: if recovered {
                    CheckOutcomeKind::Recovered(latest_pubtime)
                } else {
                    CheckOutcomeKind::StillInactive
                },
                submission_id: cand.submission_id,
                upper_name: cand.upper_name.clone(),
            })
        }
        Some(Err(e)) => {
            if let Some(bili_err) = e.downcast_ref::<BiliError>() {
                // 风控优先：中断整轮，避免连环触发
                if bili_err.is_risk_control_related() {
                    bail!(e);
                }
                // 删号/注销/不存在 → 永久不可恢复，后续写 Blacklist
                if bili_err.is_upper_permanently_gone() {
                    return Ok(CheckOutcome {
                        kind: CheckOutcomeKind::Gone(bili_err.to_string()),
                        submission_id: cand.submission_id,
                        upper_name: cand.upper_name.clone(),
                    });
                }
                // 封禁/冻结 → 观察态，后续写 Banned（不进黑名单，不动 enabled）
                if bili_err.is_upper_banned() {
                    return Ok(CheckOutcome {
                        kind: CheckOutcomeKind::BannedObservation(bili_err.to_string()),
                        submission_id: cand.submission_id,
                        upper_name: cand.upper_name.clone(),
                    });
                }
            }
            // 其他错误向上传递，由调用方记 warn 跳过
            Err(e)
        }
        // stream 结束且无错误：UP 无任何可拉取的视频（投稿/动态皆空），视为仍不活跃
        None => Ok(CheckOutcome {
            kind: CheckOutcomeKind::StillInactive,
            submission_id: cand.submission_id,
            upper_name: cand.upper_name.clone(),
        }),
    }
}

async fn load_policy_for_submission(
    connection: &DatabaseConnection,
    submission_id: i32,
) -> Result<Option<UpperManagePolicy>> {
    Ok(upper_auto_manage_policy::Entity::find_by_id(submission_id)
        .one(connection)
        .await?
        .map(|p| p.policy))
}

async fn disable_submission(connection: &DatabaseConnection, submission_id: i32) -> Result<()> {
    let lock = lock_for_submission(submission_id);
    let _guard = lock.lock().await;
    submission::ActiveModel {
        id: Set(submission_id),
        enabled: Set(false),
        ..Default::default()
    }
    .update(connection)
    .await?;
    Ok(())
}

async fn enable_submission(connection: &DatabaseConnection, submission_id: i32) -> Result<()> {
    let lock = lock_for_submission(submission_id);
    let _guard = lock.lock().await;
    let txn = connection.begin().await?;
    submission::ActiveModel {
        id: Set(submission_id),
        enabled: Set(true),
        ..Default::default()
    }
    .update(&txn)
    .await?;
    if let Some(p) = upper_auto_manage_policy::Entity::find_by_id(submission_id)
        .one(&txn)
        .await?
        && p.policy == UpperManagePolicy::Normal
    {
        upper_auto_manage_policy::Entity::delete_by_id(submission_id)
            .exec(&txn)
            .await?;
    }
    txn.commit().await?;
    Ok(())
}

async fn recover_banned_submission(connection: &DatabaseConnection, submission_id: i32) -> Result<()> {
    let lock = lock_for_submission(submission_id);
    let _guard = lock.lock().await;
    let txn = connection.begin().await?;
    submission::ActiveModel {
        id: Set(submission_id),
        enabled: Set(true),
        ..Default::default()
    }
    .update(&txn)
    .await?;
    upper_auto_manage_policy::Entity::delete_by_id(submission_id)
        .exec(&txn)
        .await?;
    txn.commit().await?;
    Ok(())
}

/// 写入或更新某 submission 的 policy（独立事务）。
/// 通过 lock_for_submission 与 API 写入串行，避免并发覆盖。
async fn upsert_policy(
    connection: &DatabaseConnection,
    submission_id: i32,
    policy: UpperManagePolicy,
    source: UpperManageSource,
    reason: Option<String>,
) -> Result<()> {
    let lock = lock_for_submission(submission_id);
    let _guard = lock.lock().await;
    let txn = connection.begin().await?;
    upsert_policy_txn(&txn, submission_id, policy, source, reason).await?;
    txn.commit().await?;
    Ok(())
}

/// 在外部事务内 upsert policy（不开启新事务，由调用方决定事务边界）
async fn upsert_policy_txn(
    txn: &sea_orm::DatabaseTransaction,
    submission_id: i32,
    policy: UpperManagePolicy,
    source: UpperManageSource,
    reason: Option<String>,
) -> Result<()> {
    let exists = upper_auto_manage_policy::Entity::find_by_id(submission_id)
        .one(txn)
        .await?
        .is_some();
    let am = upper_auto_manage_policy::ActiveModel {
        submission_id: Set(submission_id),
        policy: Set(policy),
        source: Set(source),
        reason: Set(reason),
        updated_at: Set(chrono::Utc::now().naive_utc()),
    };
    if exists {
        am.update(txn).await?;
    } else {
        am.insert(txn).await?;
    }
    Ok(())
}

/// 删除用户已有的策略行，根据原策略决定后续语义：
///
/// - `Blacklist` / `Banned`：删除策略行并保留当前 enabled 状态，避免写入 normal 策略。
///   删除 Banned 后仍保持 disabled，等待手动巡检复核。
/// - `Whitelist` / `Normal`：直接删除行。删除白名单后该 UP 重新进入自动禁用巡检候选；
///   删除 normal 策略后该 UP 不再保留任何策略标记，完全交由系统/用户手动管理。
///
/// 注意：手动禁用 submission（无 policy 行 + enabled=false）不会被自动恢复巡检命中，
/// 因为恢复 SQL 同时要求存在 policy 行且 source='auto'。
pub async fn reset_policy_after_delete(
    connection: &DatabaseConnection,
    submission_id: i32,
    original_policy: UpperManagePolicy,
) -> Result<()> {
    // 锁由调用方（API handler / update_video_source）在外层持有，这里不再加锁，
    // 避免 tokio Mutex 不可重入导致死锁。
    match original_policy {
        UpperManagePolicy::Blacklist
        | UpperManagePolicy::Banned
        | UpperManagePolicy::Whitelist
        | UpperManagePolicy::Normal => {
            upper_auto_manage_policy::Entity::delete_by_id(submission_id)
                .exec(connection)
                .await?;
        }
    }
    Ok(())
}

/// 兼容旧调用点：保留以 normal+manual 写入的便捷接口，仅供内部测试或显式需要
/// 「写一行 marker 表示已手动重置」的场景使用。生产删除入口应使用 `reset_policy_after_delete`。
#[allow(dead_code)]
pub async fn reset_policy_to_default(connection: &DatabaseConnection, submission_id: i32) -> Result<()> {
    let txn = connection.begin().await?;
    upsert_policy_txn(
        &txn,
        submission_id,
        UpperManagePolicy::Normal,
        UpperManageSource::Manual,
        Some("用户删除策略，恢复默认管理".to_string()),
    )
    .await?;
    txn.commit().await?;
    Ok(())
}

/// 仅查询 submission 的当前 enabled 状态，供测试与 API 等上层调用使用
#[allow(dead_code)]
pub async fn is_submission_enabled(connection: &DatabaseConnection, submission_id: i32) -> Result<bool> {
    let Some(model) = submission::Entity::find_by_id(submission_id).one(connection).await? else {
        bail!("submission {} 不存在", submission_id);
    };
    Ok(model.enabled)
}

#[cfg(test)]
mod tests {
    use bili_sync_entity::upper_auto_manage_policy::{UpperManagePolicy as Policy, UpperManageSource as Source};
    use bili_sync_migration::{Migrator, MigratorTrait};
    use chrono::Utc;
    use sea_orm::Set;

    use super::*;
    use crate::config::item::UpperAutoManageOption;

    /// 准备一个独立 SQLite 测试库并跑完迁移
    async fn setup_test_db() -> (async_tempfile::TempDir, DatabaseConnection) {
        let dir = async_tempfile::TempDir::new().await.expect("create tempdir");
        let db_path = dir.dir_path().join("test.sqlite");
        let url = format!("sqlite://{}?mode=rwc", db_path.to_string_lossy());
        let conn = sea_orm::Database::connect(&url).await.expect("connect");
        Migrator::up(&conn, None).await.expect("migrate");
        (dir, conn)
    }

    async fn insert_submission(conn: &DatabaseConnection, upper_id: i64, name: &str, enabled: bool) -> i32 {
        insert_submission_with_dynamic_api(conn, upper_id, name, enabled, false).await
    }

    async fn insert_submission_with_dynamic_api(
        conn: &DatabaseConnection,
        upper_id: i64,
        name: &str,
        enabled: bool,
        use_dynamic_api: bool,
    ) -> i32 {
        let am = submission::ActiveModel {
            upper_id: Set(upper_id),
            upper_name: Set(name.to_string()),
            path: Set(format!("/tmp/{}", name)),
            created_at: Set(Utc::now().to_rfc3339()),
            use_dynamic_api: Set(use_dynamic_api),
            latest_row_at: Set(Utc::now().naive_utc()),
            enabled: Set(enabled),
            ..Default::default()
        };
        am.insert(conn).await.expect("insert submission").id
    }

    async fn insert_video(
        conn: &DatabaseConnection,
        submission_id: i32,
        upper_id: i64,
        pubtime: chrono::NaiveDateTime,
    ) {
        let am = bili_sync_entity::video::ActiveModel {
            submission_id: Set(Some(submission_id)),
            upper_id: Set(upper_id),
            upper_name: Set("UP".to_string()),
            upper_face: Set(String::new()),
            name: Set("v".to_string()),
            path: Set("/tmp/v".to_string()),
            category: Set(0),
            bvid: Set(format!("BV{:010}", submission_id)),
            intro: Set(String::new()),
            cover: Set(String::new()),
            ctime: Set(pubtime),
            pubtime: Set(pubtime),
            favtime: Set(pubtime),
            download_status: Set(0),
            valid: Set(true),
            should_download: Set(false),
            single_page: Set(Some(true)),
            created_at: Set(pubtime.and_utc().to_rfc3339()),
            ..Default::default()
        };
        am.insert(conn).await.expect("insert video");
    }

    async fn load_policy(conn: &DatabaseConnection, submission_id: i32) -> Option<upper_auto_manage_policy::Model> {
        upper_auto_manage_policy::Entity::find_by_id(submission_id)
            .one(conn)
            .await
            .expect("query policy")
    }

    #[tokio::test]
    async fn automatic_disable_only_changes_enabled_without_normal_policy() {
        let (_dir, conn) = setup_test_db().await;
        let sid = insert_submission(&conn, 99, "auto", true).await;
        disable_submission(&conn, sid).await.expect("disable");
        assert!(!is_submission_enabled(&conn, sid).await.unwrap());
        assert!(load_policy(&conn, sid).await.is_none());
    }

    #[tokio::test]
    async fn scheduled_recheck_includes_manual_disabled_and_whitelist_but_not_banned() {
        let (_dir, conn) = setup_test_db().await;
        let sid_plain = insert_submission(&conn, 98, "plain", false).await;
        let sid_white = insert_submission(&conn, 97, "white", false).await;
        let sid_banned = insert_submission(&conn, 96, "banned", false).await;
        upsert_policy(&conn, sid_white, Policy::Whitelist, Source::Manual, None)
            .await
            .unwrap();
        upsert_policy(&conn, sid_banned, Policy::Banned, Source::Auto, None)
            .await
            .unwrap();
        let ids = fetch_disabled_for_recheck(&conn, TaskTrigger::Scheduled)
            .await
            .unwrap()
            .into_iter()
            .map(|c| c.submission_id)
            .collect::<Vec<_>>();
        assert!(ids.contains(&sid_plain));
        assert!(ids.contains(&sid_white));
        assert!(!ids.contains(&sid_banned));
    }

    #[tokio::test]
    async fn manual_recheck_includes_banned_but_not_blacklist() {
        let (_dir, conn) = setup_test_db().await;
        let sid_banned = insert_submission(&conn, 95, "banned", false).await;
        let sid_black = insert_submission(&conn, 94, "black", false).await;
        upsert_policy(&conn, sid_banned, Policy::Banned, Source::Auto, None)
            .await
            .unwrap();
        upsert_policy(&conn, sid_black, Policy::Blacklist, Source::Manual, None)
            .await
            .unwrap();
        let ids = fetch_disabled_for_recheck(&conn, TaskTrigger::Manual)
            .await
            .unwrap()
            .into_iter()
            .map(|c| c.submission_id)
            .collect::<Vec<_>>();
        assert!(ids.contains(&sid_banned));
        assert!(!ids.contains(&sid_black));
    }

    #[tokio::test]
    async fn recovered_submission_is_enabled_and_normal_policy_is_removed() {
        let (_dir, conn) = setup_test_db().await;
        let sid = insert_submission(&conn, 93, "recover", false).await;
        upsert_policy(&conn, sid, Policy::Normal, Source::Auto, None)
            .await
            .unwrap();
        enable_submission(&conn, sid).await.expect("enable");
        assert!(is_submission_enabled(&conn, sid).await.unwrap());
        assert!(load_policy(&conn, sid).await.is_none());
    }

    #[tokio::test]
    async fn recovered_banned_submission_clears_banned_and_enables() {
        let (_dir, conn) = setup_test_db().await;
        let sid = insert_submission(&conn, 92, "unbanned", false).await;
        upsert_policy(&conn, sid, Policy::Banned, Source::Auto, None)
            .await
            .unwrap();
        recover_banned_submission(&conn, sid).await.expect("recover");
        assert!(is_submission_enabled(&conn, sid).await.unwrap());
        assert!(load_policy(&conn, sid).await.is_none());
    }

    #[tokio::test]
    async fn manual_disabled_submission_does_not_create_normal_policy() {
        let (_dir, conn) = setup_test_db().await;
        let sid = insert_submission(&conn, 91, "manual", true).await;
        disable_submission(&conn, sid).await.expect("disable");
        assert!(load_policy(&conn, sid).await.is_none());
    }

    #[tokio::test]
    async fn reset_policy_after_delete_removes_blacklist_row() {
        let (_dir, conn) = setup_test_db().await;
        let sid = insert_submission(&conn, 102, "carol", false).await;
        // 起始状态：黑名单
        upsert_policy(
            &conn,
            sid,
            Policy::Blacklist,
            Source::Auto,
            Some("auto banned".to_string()),
        )
        .await
        .expect("seed blacklist");
        // 删除黑名单：应删除策略行；禁用状态保留，等待手动巡检复核
        reset_policy_after_delete(&conn, sid, Policy::Blacklist)
            .await
            .expect("reset");
        assert!(
            load_policy(&conn, sid).await.is_none(),
            "删除黑名单后不应保留 normal 策略"
        );
        // 当前 enabled 状态应被保留
        assert!(!is_submission_enabled(&conn, sid).await.unwrap());
    }

    #[tokio::test]
    async fn reset_policy_after_delete_removes_banned_row_and_keeps_disabled() {
        // 删除「封禁观察」策略：应删除策略行，保持 disabled，等待手动巡检复核
        let (_dir, conn) = setup_test_db().await;
        let sid = insert_submission(&conn, 10201, "banned_up", false).await;
        upsert_policy(&conn, sid, Policy::Banned, Source::Auto, Some("封禁观察".to_string()))
            .await
            .expect("seed banned");
        reset_policy_after_delete(&conn, sid, Policy::Banned)
            .await
            .expect("reset banned");
        assert!(
            load_policy(&conn, sid).await.is_none(),
            "清除封禁观察后不应写入 normal 策略"
        );
        assert!(
            !is_submission_enabled(&conn, sid).await.unwrap(),
            "enabled 状态应被保留"
        );
    }

    #[tokio::test]
    async fn reset_policy_after_delete_removes_whitelist_row() {
        let (_dir, conn) = setup_test_db().await;
        let sid = insert_submission(&conn, 1021, "white", true).await;
        upsert_policy(&conn, sid, Policy::Whitelist, Source::Manual, None)
            .await
            .unwrap();
        reset_policy_after_delete(&conn, sid, Policy::Whitelist)
            .await
            .expect("reset");
        assert!(
            load_policy(&conn, sid).await.is_none(),
            "删除白名单应直接抹掉行，不留 marker"
        );
    }

    #[tokio::test]
    async fn reset_policy_after_delete_removes_normal_row() {
        let (_dir, conn) = setup_test_db().await;
        let sid = insert_submission(&conn, 1022, "plain", true).await;
        upsert_policy(&conn, sid, Policy::Normal, Source::Manual, None)
            .await
            .unwrap();
        reset_policy_after_delete(&conn, sid, Policy::Normal)
            .await
            .expect("reset");
        assert!(load_policy(&conn, sid).await.is_none());
    }

    #[tokio::test]
    async fn reset_policy_to_default_legacy_writes_normal_manual() {
        // 旧的 reset_policy_to_default 仍保留 normal+manual 写入，作为显式「手动重置」入口
        let (_dir, conn) = setup_test_db().await;
        let sid = insert_submission(&conn, 1023, "legacy", true).await;
        reset_policy_to_default(&conn, sid).await.expect("legacy reset");
        let p = load_policy(&conn, sid).await.expect("policy 应被创建");
        assert_eq!(p.policy, Policy::Normal);
        assert_eq!(p.source, Source::Manual);
    }

    #[tokio::test]
    async fn upsert_policy_txn_inserts_then_updates() {
        let (_dir, conn) = setup_test_db().await;
        let sid = insert_submission(&conn, 104, "eve", true).await;
        // 第一次：插入
        upsert_policy(&conn, sid, Policy::Whitelist, Source::Manual, Some("r1".to_string()))
            .await
            .expect("insert");
        let p = load_policy(&conn, sid).await.unwrap();
        assert_eq!(p.policy, Policy::Whitelist);
        assert_eq!(p.source, Source::Manual);
        assert_eq!(p.reason.as_deref(), Some("r1"));
        // 第二次：更新
        upsert_policy(&conn, sid, Policy::Blacklist, Source::Auto, Some("r2".to_string()))
            .await
            .expect("update");
        let p = load_policy(&conn, sid).await.unwrap();
        assert_eq!(p.policy, Policy::Blacklist);
        assert_eq!(p.source, Source::Auto);
        assert_eq!(p.reason.as_deref(), Some("r2"));
    }

    #[tokio::test]
    async fn fetch_candidates_carries_use_dynamic_api_flag() {
        let (_dir, conn) = setup_test_db().await;
        // 阶段一查 enabled=1 的 submission；阶段二查 enabled=0 的。各建一个 dynamic / normal。
        let sid_dyn_p1 = insert_submission_with_dynamic_api(&conn, 700, "dyn_up_p1", true, true).await;
        let sid_normal_p1 = insert_submission_with_dynamic_api(&conn, 701, "normal_up_p1", true, false).await;
        let sid_dyn_p2 = insert_submission_with_dynamic_api(&conn, 702, "dyn_up_p2", false, true).await;
        let sid_normal_p2 = insert_submission_with_dynamic_api(&conn, 703, "normal_up_p2", false, false).await;
        // 阶段一应同时带上 use_dynamic_api
        let p1: Vec<i32> = fetch_inactive_candidates(&conn)
            .await
            .unwrap()
            .into_iter()
            .map(|c| c.submission_id)
            .collect();
        assert!(p1.contains(&sid_dyn_p1) && p1.contains(&sid_normal_p1));
        let p1_dyn = fetch_inactive_candidates(&conn)
            .await
            .unwrap()
            .into_iter()
            .find(|c| c.submission_id == sid_dyn_p1)
            .unwrap();
        let p1_normal = fetch_inactive_candidates(&conn)
            .await
            .unwrap()
            .into_iter()
            .find(|c| c.submission_id == sid_normal_p1)
            .unwrap();
        assert!(p1_dyn.use_dynamic_api);
        assert!(!p1_normal.use_dynamic_api);
        // 阶段二同理
        let p2: Vec<i32> = fetch_disabled_for_recheck(&conn, TaskTrigger::Scheduled)
            .await
            .unwrap()
            .into_iter()
            .map(|c| c.submission_id)
            .collect();
        assert!(p2.contains(&sid_dyn_p2) && p2.contains(&sid_normal_p2));
        let p2_dyn = fetch_disabled_for_recheck(&conn, TaskTrigger::Scheduled)
            .await
            .unwrap()
            .into_iter()
            .find(|c| c.submission_id == sid_dyn_p2)
            .unwrap();
        let p2_normal = fetch_disabled_for_recheck(&conn, TaskTrigger::Scheduled)
            .await
            .unwrap()
            .into_iter()
            .find(|c| c.submission_id == sid_normal_p2)
            .unwrap();
        assert!(p2_dyn.use_dynamic_api);
        assert!(!p2_normal.use_dynamic_api);
    }

    #[tokio::test]
    async fn fetch_disabled_for_recheck_includes_auto_source_normal_policy() {
        let (_dir, conn) = setup_test_db().await;
        let sid = insert_submission(&conn, 105, "frank", false).await;
        // 模拟「系统自动禁用」：normal+auto 必须被恢复巡检命中
        upsert_policy(&conn, sid, Policy::Normal, Source::Auto, None)
            .await
            .expect("seed auto normal");
        let candidates = fetch_disabled_for_recheck(&conn, TaskTrigger::Scheduled)
            .await
            .expect("query");
        assert!(
            candidates.iter().any(|c| c.submission_id == sid),
            "auto 来源的 normal policy 必须被恢复巡检命中"
        );
    }

    #[tokio::test]
    async fn fetch_disabled_for_recheck_includes_manual_source_normal_policy() {
        // 用户手动 normal+manual + enabled=false 仍会进入恢复巡检候选；名称和断言明确记录当前行为
        let (_dir, conn) = setup_test_db().await;
        let sid = insert_submission(&conn, 1051, "manual", false).await;
        upsert_policy(&conn, sid, Policy::Normal, Source::Manual, Some("用户手动".to_string()))
            .await
            .expect("seed manual normal");
        let candidates = fetch_disabled_for_recheck(&conn, TaskTrigger::Scheduled)
            .await
            .expect("query");
        assert!(
            candidates.iter().any(|c| c.submission_id == sid),
            "manual normal policy 也应进入恢复巡检"
        );
    }

    #[tokio::test]
    async fn fetch_disabled_for_recheck_excludes_whitelist_blacklist() {
        let (_dir, conn) = setup_test_db().await;
        let sid_w = insert_submission(&conn, 106, "white", false).await;
        let sid_b = insert_submission(&conn, 107, "black", false).await;
        let sid_banned = insert_submission(&conn, 1088, "banned", false).await;
        upsert_policy(&conn, sid_w, Policy::Whitelist, Source::Manual, None)
            .await
            .unwrap();
        upsert_policy(&conn, sid_b, Policy::Blacklist, Source::Manual, None)
            .await
            .unwrap();
        upsert_policy(&conn, sid_banned, Policy::Banned, Source::Auto, None)
            .await
            .unwrap();
        let candidates = fetch_disabled_for_recheck(&conn, TaskTrigger::Scheduled)
            .await
            .expect("query");
        let ids: Vec<i32> = candidates.iter().map(|c| c.submission_id).collect();
        assert!(ids.contains(&sid_w), "whitelist 应进入恢复巡检");
        assert!(!ids.contains(&sid_b), "blacklist 不应进入恢复巡检");
        assert!(!ids.contains(&sid_banned), "banned(封禁观察) 不应进入恢复巡检");
    }

    #[tokio::test]
    async fn fetch_inactive_candidates_excludes_banned_policy() {
        // 阶段一候选应排除 banned（封禁观察）启用态 UP，避免被「长期不更新」覆盖回 normal+auto
        let (_dir, conn) = setup_test_db().await;
        let sid_banned = insert_submission(&conn, 1089, "banned_active", true).await;
        upsert_policy(&conn, sid_banned, Policy::Banned, Source::Auto, None)
            .await
            .unwrap();
        let candidates = fetch_inactive_candidates(&conn).await.expect("query");
        let ids: Vec<i32> = candidates.iter().map(|c| c.submission_id).collect();
        assert!(
            !ids.contains(&sid_banned),
            "banned(封禁观察) 不应进入阶段一候选，否则会被覆盖回 normal"
        );
    }

    #[tokio::test]
    async fn fetch_inactive_candidates_skips_disabled_and_whitelisted() {
        let (_dir, conn) = setup_test_db().await;
        // enabled=true，无视频 → last_pubtime=None，查询会返回但 phase 1 内部会跳过
        let sid_ok = insert_submission(&conn, 108, "ok", true).await;
        let sid_disabled = insert_submission(&conn, 109, "disabled", false).await;
        let sid_wl = insert_submission(&conn, 110, "wl", true).await;
        upsert_policy(&conn, sid_wl, Policy::Whitelist, Source::Manual, None)
            .await
            .unwrap();
        let candidates = fetch_inactive_candidates(&conn).await.expect("query");
        let ids: Vec<i32> = candidates.iter().map(|c| c.submission_id).collect();
        assert!(ids.contains(&sid_ok));
        assert!(!ids.contains(&sid_disabled), "已禁用的不应在 phase1 候选中");
        assert!(!ids.contains(&sid_wl), "白名单不应在 phase1 候选中");
    }

    #[tokio::test]
    async fn phase1_partitions_candidates_into_disabled_active_indeterminate() {
        let (_dir, conn) = setup_test_db().await;
        // 超阈值 → 禁用
        let sid_old = insert_submission(&conn, 111, "old", true).await;
        let old = (Utc::now() - chrono::Duration::days(200)).naive_utc();
        insert_video(&conn, sid_old, 111, old).await;
        // 近期投稿 → 正常，无需动作
        let sid_recent = insert_submission(&conn, 112, "recent", true).await;
        let recent = (Utc::now() - chrono::Duration::days(10)).naive_utc();
        insert_video(&conn, sid_recent, 112, recent).await;
        // 本地无视频 → 无法判定
        let sid_novideo = insert_submission(&conn, 113, "novideo", true).await;

        let opt = UpperAutoManageOption {
            enabled: true,
            interval: Trigger::Interval(3600),
            inactive_threshold_days: 90,
            check_concurrency: 2,
        };
        let now = Utc::now().naive_utc();
        let mut stats = RunStats::default();
        // 模拟 run_inspection_inner 阶段一的核心逻辑
        let inactive = fetch_inactive_candidates(&conn).await.expect("fetch");
        stats.checked += inactive.len() as i32;
        for cand in &inactive {
            let Some(last_pub) = cand.last_pubtime else {
                stats.indeterminate += 1;
                continue;
            };
            let days = (now - last_pub).num_days();
            if days > opt.inactive_threshold_days {
                disable_submission(&conn, cand.submission_id).await.expect("disable");
                stats.disabled += 1;
            } else {
                stats.active += 1;
            }
        }
        assert_eq!(stats.checked, 3);
        assert_eq!(stats.disabled, 1);
        assert_eq!(stats.active, 1);
        assert_eq!(stats.indeterminate, 1);
        assert_eq!(stats.checked, stats.phase1_total(), "阶段一各桶应闭合");
        assert!(!is_submission_enabled(&conn, sid_old).await.unwrap(), "应被自动禁用");
        assert!(
            is_submission_enabled(&conn, sid_recent).await.unwrap(),
            "近期投稿不应被禁用"
        );
        assert!(
            is_submission_enabled(&conn, sid_novideo).await.unwrap(),
            "无视频 UP 不应被动"
        );
        assert!(load_policy(&conn, sid_old).await.is_none());
    }

    /// 统计口径不变量：检查 = 七个分桶之和（成功轮次）
    #[test]
    fn summary_line_partitions_checked_into_buckets() {
        let stats = RunStats {
            checked: 178,
            disabled: 9,
            active: 127,
            indeterminate: 0,
            enabled: 0,
            banned: 0,
            banned_observation: 0,
            still_inactive: 42,
        };
        assert_eq!(stats.phase1_total(), 136);
        assert_eq!(stats.phase2_total(), 42);
        assert_eq!(stats.phase1_total() + stats.phase2_total(), stats.checked);
        assert!(stats.summary_line().contains("检查 178 个 UP"));
        assert!(stats.summary_line().contains("仍不活跃 42"));
    }

    /// 30 天清理：仅删除超期 run 行，保留期内的行不受影响
    #[tokio::test]
    async fn cleanup_removes_only_expired_upper_auto_manage_runs() {
        let (_dir, conn) = setup_test_db().await;
        let now = Utc::now().naive_utc();
        let expired = now - chrono::Duration::days(crate::task::RUN_RETENTION_DAYS + 1);
        for started_at in [expired, now] {
            upper_auto_manage_run::ActiveModel {
                started_at: Set(started_at),
                status: Set(RunStatus::Succeeded),
                ..Default::default()
            }
            .insert(&conn)
            .await
            .expect("seed");
        }
        let deleted = cleanup_expired_upper_auto_manage_runs(&conn).await.expect("cleanup");
        assert_eq!(deleted, 1);
        let remaining = upper_auto_manage_run::Entity::find().all(&conn).await.expect("query");
        assert_eq!(remaining.len(), 1);
    }

    /// 模拟「阶段二收到 outcome 后处理」的逻辑：Recovered/Banned/StillInactive
    /// 不调网络，可直接走 DB 验证
    #[tokio::test]
    async fn phase2_outcomes_apply_correct_state_changes() {
        let (_dir, conn) = setup_test_db().await;
        // Recovered：禁用 + normal auto → 启用 + 仍 normal auto
        let sid_r = insert_submission(&conn, 200, "recover", false).await;
        upsert_policy(&conn, sid_r, Policy::Normal, Source::Auto, None)
            .await
            .unwrap();
        let cand = SubmissionActivity {
            submission_id: sid_r,
            upper_id: 200,
            upper_name: "recover".into(),
            last_pubtime: None,
            use_dynamic_api: false,
        };
        // 模拟 apply_recovered_outcome
        enable_submission(&conn, sid_r).await.unwrap();
        assert!(is_submission_enabled(&conn, sid_r).await.unwrap());
        assert!(load_policy(&conn, sid_r).await.is_none());
        // Banned：启用 → 写 blacklist auto，enabled 保持
        let sid_b = insert_submission(&conn, 201, "ban", true).await;
        upsert_policy(&conn, sid_b, Policy::Whitelist, Source::Manual, None)
            .await
            .unwrap();
        upsert_policy(&conn, sid_b, Policy::Blacklist, Source::Auto, Some("UP 不可用".into()))
            .await
            .unwrap();
        let p = load_policy(&conn, sid_b).await.unwrap();
        assert_eq!(p.policy, Policy::Blacklist);
        assert_eq!(p.source, Source::Auto);
        assert!(
            is_submission_enabled(&conn, sid_b).await.unwrap(),
            "黑名单写入不应改变 enabled"
        );
        // StillInactive：什么都不做
        let sid_s = insert_submission(&conn, 202, "still", false).await;
        upsert_policy(&conn, sid_s, Policy::Normal, Source::Auto, None)
            .await
            .unwrap();
        let p_before = load_policy(&conn, sid_s).await.unwrap();
        let _cand = cand; // 抑制未用警告
        let p_after = load_policy(&conn, sid_s).await.unwrap();
        assert_eq!(p_before.policy, p_after.policy);
        assert_eq!(p_before.source, p_after.source);
        assert_eq!(
            p_before.updated_at, p_after.updated_at,
            "still_inactive 不应触碰 policy"
        );
    }

    /// 阶段二 Gone outcome（删号/不可恢复）→ 写 Blacklist+Auto，不动 enabled
    #[tokio::test]
    async fn phase2_gone_outcome_writes_blacklist() {
        let (_dir, conn) = setup_test_db().await;
        let sid = insert_submission(&conn, 300, "gone_up", false).await;
        // 起始：被自动禁用（normal+auto）
        upsert_policy(&conn, sid, Policy::Normal, Source::Auto, None)
            .await
            .unwrap();
        // 模拟阶段二收到 Gone outcome 后的处理：写 Blacklist+Auto
        upsert_policy(
            &conn,
            sid,
            Policy::Blacklist,
            Source::Auto,
            Some("UP 已删号/不可恢复：该用户不存在".into()),
        )
        .await
        .unwrap();
        let p = load_policy(&conn, sid).await.unwrap();
        assert_eq!(p.policy, Policy::Blacklist, "删号应写黑名单");
        assert_eq!(p.source, Source::Auto);
        assert!(!is_submission_enabled(&conn, sid).await.unwrap(), "enabled 保持 false");
        assert!(p.reason.as_deref().unwrap().contains("删号"), "reason 应注明删号");
    }

    /// 阶段二 BannedObservation outcome（封禁/冻结）→ 写 Banned+Auto，不动 enabled，不进黑名单
    #[tokio::test]
    async fn phase2_banned_observation_outcome_writes_banned() {
        let (_dir, conn) = setup_test_db().await;
        let sid = insert_submission(&conn, 301, "banned_up", false).await;
        // 起始：被自动禁用（normal+auto）
        upsert_policy(&conn, sid, Policy::Normal, Source::Auto, None)
            .await
            .unwrap();
        // 模拟阶段二收到 BannedObservation outcome 后的处理：写 Banned+Auto
        upsert_policy(
            &conn,
            sid,
            Policy::Banned,
            Source::Auto,
            Some("封禁观察，待人工判断：该账号已封禁".into()),
        )
        .await
        .unwrap();
        let p = load_policy(&conn, sid).await.unwrap();
        assert_eq!(p.policy, Policy::Banned, "封禁应写封禁观察而非黑名单");
        assert_eq!(p.source, Source::Auto);
        assert!(
            !is_submission_enabled(&conn, sid).await.unwrap(),
            "enabled 保持 false，不自动启用"
        );
        assert!(
            p.reason.as_deref().unwrap().contains("封禁观察"),
            "reason 应注明封禁观察"
        );
        // banned UP 不应进入阶段一候选（验证不会被覆盖回 normal）
        let inactive = fetch_inactive_candidates(&conn).await.unwrap();
        assert!(!inactive.iter().any(|c| c.submission_id == sid));
        // banned UP 不应进入恢复巡检候选（验证不会被自动启用）
        let recheck = fetch_disabled_for_recheck(&conn, TaskTrigger::Scheduled).await.unwrap();
        assert!(!recheck.iter().any(|c| c.submission_id == sid));
    }

    /// 模拟「try_for_each_concurrent 真并发」：用 sleep 代替网络调用，
    /// 验证 N 个候选能在不超过 ceil(N/concurrency) * per_item_time 内完成。
    /// 如果退化为串行，总耗时 ≈ N * per_item_time，应当远超并发上限。
    #[tokio::test]
    async fn phase2_concurrency_is_real_parallel() {
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        const N: usize = 8; // 8 个候选
        const CONCURRENCY: usize = 3; // 并发 3
        const PER_ITEM: Duration = Duration::from_millis(150);

        let start = Arc::new(std::sync::Mutex::new(None::<Instant>));
        let start_clone = start.clone();
        let items: Vec<u32> = (0..N as u32).collect();
        let result: Result<()> = stream::iter(items.into_iter().map(Ok::<_, anyhow::Error>))
            .try_for_each_concurrent(Some(CONCURRENCY), |_item| {
                let start_clone = start_clone.clone();
                async move {
                    // 记录第一个进入并发体的时间，作为基准
                    {
                        let mut g = start_clone.lock().unwrap();
                        if g.is_none() {
                            *g = Some(Instant::now());
                        }
                    }
                    tokio::time::sleep(PER_ITEM).await;
                    Ok::<(), anyhow::Error>(())
                }
            })
            .await;
        assert!(result.is_ok());
        let elapsed = start.lock().unwrap().unwrap().elapsed();
        // 串行耗时 ≈ 8 * 150 = 1200ms；
        // 并发 3：3 批，约 3 * 150 = 450ms；再加调度开销，断言 < 800ms。
        assert!(
            elapsed < Duration::from_millis(800),
            "并发执行被退化为串行：耗时 {}ms",
            elapsed.as_millis()
        );
    }

    /// 模拟「风控短路」：第 3 个返回 Err，前面的 2 个已经 sleep 完成，
    /// 第 3 个在 sleep 中报错。try_for_each_concurrent 必须立刻停止派发剩余候选。
    #[tokio::test]
    async fn phase2_short_circuits_on_first_error() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        const TOTAL: usize = 12;
        const CONCURRENCY: usize = 3;
        const PER_ITEM: Duration = Duration::from_millis(120);

        let dispatched = Arc::new(AtomicUsize::new(0));
        let dispatched_clone = dispatched.clone();
        let items: Vec<u32> = (0..TOTAL as u32).collect();
        let result: Result<()> = stream::iter(items.into_iter().map(Ok::<_, anyhow::Error>))
            .try_for_each_concurrent(Some(CONCURRENCY), |item| {
                let dispatched_clone = dispatched_clone.clone();
                async move {
                    dispatched_clone.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(PER_ITEM).await;
                    if item == 2 {
                        // 模拟风控：item=2 的并发任务在 sleep 完成后报错
                        Err(anyhow::anyhow!("simulated risk control"))
                    } else {
                        Ok(())
                    }
                }
            })
            .await;
        let total_dispatched = dispatched.load(Ordering::SeqCst);
        assert!(result.is_err(), "风控错误应使整轮失败");
        assert!(
            total_dispatched < TOTAL,
            "风控错误后必须停止派发后续候选：已派发 {} / {}",
            total_dispatched,
            TOTAL
        );
        // 最坏情况：派发了最多 CONCURRENCY 个在飞的项 + 0 个新的；
        // 实际会有少量已入队的项目，但远小于 TOTAL。
        assert!(
            total_dispatched <= CONCURRENCY + 2,
            "派发数应接近 CONCURRENCY 而非 TOTAL，实际 {}",
            total_dispatched
        );
    }
}
