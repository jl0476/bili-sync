use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use bili_sync_entity::{
    upper_auto_manage_action, upper_auto_manage_policy, upper_auto_manage_run, submission,
};
use bili_sync_entity::upper_auto_manage_action::ActionType;
use bili_sync_entity::upper_auto_manage_policy::{UpperManagePolicy, UpperManageSource};
use bili_sync_entity::upper_auto_manage_run::RunStatus;
use futures::stream::{self, StreamExt};
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use sea_orm::{DatabaseConnection, FromQueryResult, Statement, TransactionTrait};
use tokio::sync::{OnceCell, watch};
use tokio_cron_scheduler::{Job, JobScheduler};

use crate::bilibili::{self, BiliClient, BiliError, Credential, Submission};
use crate::config::{Config, Trigger, VersionedConfig};
use crate::task::TaskStatus;
use crate::utils::notify::error_and_notify;

static INSTANCE: OnceCell<UpperAutoManageTaskManager> = OnceCell::const_new();

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
    running: tokio::sync::Mutex<()>,
    status_tx: watch::Sender<TaskStatus>,
    status_rx: watch::Receiver<TaskStatus>,
    job_id: tokio::sync::Mutex<Option<uuid::Uuid>>,
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
        INSTANCE
            .get()
            .expect("UpperAutoManageTaskManager is not initialized")
    }

    /// 订阅任务状态
    pub fn subscribe(&self) -> watch::Receiver<TaskStatus> {
        self.cx.status_rx.clone()
    }

    /// 手动触发一次巡检任务
    pub async fn run_once(&self) -> Result<()> {
        self.sched
            .lock()
            .await
            .add(Job::new_one_shot_async(
                Duration::from_secs(0),
                run_inspection_task(self.cx.clone()),
            )?)
            .await?;
        Ok(())
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
            running: tokio::sync::Mutex::new(()),
            status_tx,
            status_rx,
            job_id: tokio::sync::Mutex::new(None),
        });
        let mut rx = VersionedConfig::get().subscribe();
        let initial_config = rx.borrow_and_update().clone();
        // 初始注册巡检任务（若启用）
        if initial_config.upper_auto_manage.enabled {
            let job_id =
                add_inspection_job(&sched, &cx, &initial_config.upper_auto_manage.interval).await?;
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
    move |uuid, mut l| {
        let cx = cx.clone();
        Box::pin(async move {
            let Ok(_lock) = cx.running.try_lock() else {
                warn!("上一轮 UP 主自动巡检任务尚未结束，跳过本次执行..");
                return;
            };
            let _ = cx.status_tx.send(TaskStatus {
                is_running: true,
                last_run: Some(chrono::Local::now()),
                last_finish: None,
                next_run: None,
            });
            info!("开始执行本轮 UP 主自动巡检任务..");
            let config = VersionedConfig::get().snapshot();
            match run_inspection(&cx.connection, &cx.bili_client, &config).await {
                Ok(stats) => info!(
                    "本轮 UP 主自动巡检任务执行完毕：检查 {}，禁用 {}，启用 {}，封禁 {}，跳过 {}",
                    stats.checked, stats.disabled, stats.enabled, stats.banned, stats.skipped
                ),
                Err(e) => error_and_notify(
                    &config,
                    &cx.bili_client,
                    format!("本轮 UP 主自动巡检任务执行遇到错误：{:#}", e),
                    &e,
                ),
            }
            // 从 job_id 取真实 uuid（当前可能是 oneshot 任务），刷新 next_run
            let task_uuid = (*cx.job_id.lock().await).unwrap_or(uuid);
            let next_run = l
                .next_tick_for_job(task_uuid)
                .await
                .ok()
                .flatten()
                .map(|dt| dt.with_timezone(&chrono::Local));
            let last_status = *cx.status_rx.borrow();
            let _ = cx.status_tx.send(TaskStatus {
                is_running: false,
                last_run: last_status.last_run,
                last_finish: Some(chrono::Local::now()),
                next_run,
            });
        })
    }
}

#[derive(Default)]
struct RunStats {
    checked: i32,
    disabled: i32,
    enabled: i32,
    banned: i32,
    skipped: i32,
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
) -> Result<RunStats> {
    let started_at = chrono::Utc::now().naive_utc();
    let run = upper_auto_manage_run::ActiveModel {
        started_at: Set(started_at),
        status: Set(RunStatus::Running),
        ..Default::default()
    };
    let run_id = upper_auto_manage_run::Entity::insert(run)
        .exec(connection)
        .await?
        .last_insert_id;
    let result = run_inspection_inner(connection, bili_client, config, run_id).await;
    let (status, error_message, summary, stats) = match &result {
        Ok(stats) => (
            RunStatus::Succeeded,
            None,
            Some(format!(
                "巡检完成：检查 {} 个 UP，禁用 {}，启用 {}，封禁转黑名单 {}，跳过 {}",
                stats.checked, stats.disabled, stats.enabled, stats.banned, stats.skipped
            )),
            RunStats {
                checked: stats.checked,
                disabled: stats.disabled,
                enabled: stats.enabled,
                banned: stats.banned,
                skipped: stats.skipped,
            },
        ),
        Err(e) => (RunStatus::Failed, Some(format!("{:#}", e)), None, RunStats::default()),
    };
    upper_auto_manage_run::ActiveModel {
        id: Set(run_id),
        finished_at: Set(Some(chrono::Utc::now().naive_utc())),
        status: Set(status),
        checked_count: Set(stats.checked),
        disabled_count: Set(stats.disabled),
        enabled_count: Set(stats.enabled),
        banned_count: Set(stats.banned),
        skipped_count: Set(stats.skipped),
        error_message: Set(error_message),
        summary: Set(summary.clone()),
        ..Default::default()
    }
    .save(connection)
    .await?;
    // 若有摘要则通过通知渠道发送
    if let Some(summary) = summary {
        if result.is_ok() {
            crate::utils::notify::notify(config, bili_client, summary);
        }
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
            // 本地无任何视频，无法判定活跃度，跳过
            stats.skipped += 1;
            continue;
        };
        let days = (now - last_pub).num_days();
        if days > opt.inactive_threshold_days {
            set_submission_enabled(connection, cand.submission_id, false).await?;
            upsert_policy(
                connection,
                cand.submission_id,
                UpperManagePolicy::Normal,
                UpperManageSource::Auto,
                Some(format!("{} 天未更新", days)),
            )
            .await?;
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
        }
    }

    // 阶段二：主动拉取接口检查禁用态（系统自动禁用的）UP 是否恢复更新
    let disabled_candidates = fetch_disabled_for_recheck(connection).await?;
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
        let outcomes = stream::iter(disabled_candidates.into_iter())
            .map(|cand| {
                let client = snapshot_client.clone();
                let credential = credential.clone();
                async move { check_disabled_upper(&client, &credential, &cand).await }
            })
            .buffer_unordered(opt.check_concurrency)
            .collect::<Vec<_>>()
            .await;
        for outcome in outcomes {
            match outcome {
                Ok(CheckOutcome { kind, submission_id, upper_name }) => match kind {
                    CheckOutcomeKind::Recovered(latest_pubtime) => {
                        set_submission_enabled(connection, submission_id, true).await?;
                        upsert_policy(
                            connection,
                            submission_id,
                            UpperManagePolicy::Normal,
                            UpperManageSource::Auto,
                            Some(format!("检测到新投稿，时间 {}", latest_pubtime)),
                        )
                        .await?;
                        stats.enabled += 1;
                        info!("UP「{}」({}) 检测到恢复更新，已自动重新启用", upper_name, submission_id);
                        pending_actions.push(PendingAction {
                            submission_id,
                            upper_name,
                            action: ActionType::AutoEnabled,
                            reason: format!("检测到新投稿 {}", latest_pubtime),
                        });
                    }
                    CheckOutcomeKind::StillInactive => {
                        stats.skipped += 1;
                    }
                    CheckOutcomeKind::Banned(msg) => {
                        upsert_policy(
                            connection,
                            submission_id,
                            UpperManagePolicy::Blacklist,
                            UpperManageSource::Auto,
                            Some(format!("UP 不可用：{}", msg)),
                        )
                        .await?;
                        stats.banned += 1;
                        warn!("UP「{}」({}) 检测到不可用，已转黑名单：{}", upper_name, submission_id, msg);
                        pending_actions.push(PendingAction {
                            submission_id,
                            upper_name,
                            action: ActionType::MarkedBanned,
                            reason: format!("UP 不可用：{}", msg),
                        });
                    }
                },
                Err(e) => {
                    warn!("检查禁用态 UP 时出错，已跳过：{:#}", e);
                    stats.skipped += 1;
                }
            }
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

#[derive(FromQueryResult)]
struct SubmissionActivity {
    submission_id: i32,
    upper_id: i64,
    upper_name: String,
    last_pubtime: Option<DateTime>,
}

/// 查询启用态、且未被列入白名单/黑名单的 submission 及其最新投稿时间
async fn fetch_inactive_candidates(connection: &DatabaseConnection) -> Result<Vec<SubmissionActivity>> {
    let sql = "
SELECT s.id AS submission_id, s.upper_id AS upper_id, s.upper_name AS upper_name, MAX(v.pubtime) AS last_pubtime
FROM submission s
LEFT JOIN video v ON v.submission_id = s.id
WHERE s.enabled = 1
  AND NOT EXISTS (
    SELECT 1 FROM upper_auto_manage_policy p
    WHERE p.submission_id = s.id AND p.policy IN ('whitelist', 'blacklist')
  )
GROUP BY s.id, s.upper_id, s.upper_name";
    let candidates = SubmissionActivity::find_by_statement(Statement::from_string(
        connection.get_database_backend(),
        sql,
    ))
    .all(connection)
    .await
    .context("查询长期不更新候选失败")?;
    Ok(candidates)
}

/// 查询禁用态、且由系统自动禁用（policy.source=auto、policy=normal）的 submission，用于巡检恢复
async fn fetch_disabled_for_recheck(connection: &DatabaseConnection) -> Result<Vec<SubmissionActivity>> {
    let sql = "
SELECT s.id AS submission_id, s.upper_id AS upper_id, s.upper_name AS upper_name, MAX(v.pubtime) AS last_pubtime
FROM submission s
LEFT JOIN video v ON v.submission_id = s.id
INNER JOIN upper_auto_manage_policy p ON p.submission_id = s.id
WHERE s.enabled = 0
  AND p.policy = 'normal'
  AND p.source = 'auto'
GROUP BY s.id, s.upper_id, s.upper_name";
    let candidates = SubmissionActivity::find_by_statement(Statement::from_string(
        connection.get_database_backend(),
        sql,
    ))
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
    Banned(String),
}

/// 主动拉取禁用态 UP 的最新投稿，判定恢复 / 仍不活跃 / 不可用
async fn check_disabled_upper(
    client: &BiliClient,
    credential: &Credential,
    cand: &SubmissionActivity,
) -> Result<CheckOutcome> {
    let sub = Submission::new(client, cand.upper_id.to_string(), credential);
    match sub.get_videos(1).await {
        Ok(videos_json) => {
            let vlist = &videos_json["data"]["list"]["vlist"];
            if vlist.as_array().is_none_or(|a| a.is_empty()) {
                // UP 无投稿（新号或投稿全删），视为仍不活跃
                return Ok(CheckOutcome {
                    kind: CheckOutcomeKind::StillInactive,
                    submission_id: cand.submission_id,
                    upper_name: cand.upper_name.clone(),
                });
            }
            let latest_pubdate = vlist[0]["pubdate"]
                .as_i64()
                .with_context(|| format!("解析 UP {} 投稿 pubdate 失败", cand.upper_id))?;
            let latest_pubtime = chrono::DateTime::from_timestamp(latest_pubdate, 0)
                .map(|dt| dt.naive_utc())
                .context("invalid pubdate timestamp")?;
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
        Err(e) => {
            if let Some(bili_err) = e.downcast_ref::<BiliError>() {
                // 风控优先：中断整轮，避免连环触发
                if bili_err.is_risk_control_related() {
                    bail!(e);
                }
                if bili_err.is_upper_unavailable() {
                    return Ok(CheckOutcome {
                        kind: CheckOutcomeKind::Banned(bili_err.to_string()),
                        submission_id: cand.submission_id,
                        upper_name: cand.upper_name.clone(),
                    });
                }
            }
            // 其他错误向上传递，由调用方记 warn 跳过
            Err(e)
        }
    }
}

/// 更新 submission.enabled
async fn set_submission_enabled(connection: &DatabaseConnection, id: i32, enabled: bool) -> Result<()> {
    submission::ActiveModel {
        id: Set(id),
        enabled: Set(enabled),
        ..Default::default()
    }
    .update(connection)
    .await?;
    Ok(())
}

/// 写入或更新某 submission 的 policy（事务内 find + insert/update）
async fn upsert_policy(
    connection: &DatabaseConnection,
    submission_id: i32,
    policy: UpperManagePolicy,
    source: UpperManageSource,
    reason: Option<String>,
) -> Result<()> {
    let txn = connection.begin().await?;
    let exists = upper_auto_manage_policy::Entity::find_by_id(submission_id)
        .one(&txn)
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
        am.update(&txn).await?;
    } else {
        am.insert(&txn).await?;
    }
    txn.commit().await?;
    Ok(())
}
