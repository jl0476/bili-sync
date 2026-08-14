use anyhow::Context;
use axum::extract::{Extension, Path, Query};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use bili_sync_entity::upper_auto_manage_policy::{UpperManagePolicy, UpperManageSource};
use bili_sync_entity::{submission, upper_auto_manage_action, upper_auto_manage_policy, upper_auto_manage_run};
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, FromQueryResult, PaginatorTrait, QueryFilter, QueryOrder, Statement,
};
use serde::{Deserialize, Serialize};

use crate::api::error::InnerApiError;
use crate::api::wrapper::{ApiError, ApiResponse};
use crate::config::{Trigger, VersionedConfig};
use crate::task::{TaskStatus, UpperAutoManageTaskManager};

pub(super) fn router() -> Router {
    Router::new()
        .route("/upper-auto-manage/status", get(get_status))
        .route("/upper-auto-manage/run", post(trigger_run))
        .route("/upper-auto-manage/runs", get(list_runs))
        .route("/upper-auto-manage/runs/{run_id}/actions", get(list_run_actions))
        .route("/upper-auto-manage/actions", get(list_actions))
        .route("/upper-auto-manage/policies", get(list_policies))
        .route(
            "/upper-auto-manage/policies/{submission_id}",
            put(upsert_policy).delete(delete_policy),
        )
        // 未被白/黑名单保护的投稿源列表，供前端为「普通 UP」首次创建策略时挑选目标
        .route("/upper-auto-manage/candidates", get(list_candidates))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpperAutoManageStatusResponse {
    pub enabled: bool,
    pub interval: Trigger,
    pub inactive_threshold_days: i64,
    pub check_concurrency: usize,
    pub task_status: TaskStatus,
    pub last_run: Option<RunDto>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunDto {
    pub id: i32,
    pub started_at: DateTime,
    pub finished_at: Option<DateTime>,
    pub status: bili_sync_entity::upper_auto_manage_run::RunStatus,
    pub checked_count: i32,
    pub disabled_count: i32,
    pub enabled_count: i32,
    pub banned_count: i32,
    pub banned_observation_count: i32,
    pub active_count: i32,
    pub indeterminate_count: i32,
    pub still_inactive_count: i32,
    pub error_message: Option<String>,
    pub summary: Option<String>,
}

impl From<upper_auto_manage_run::Model> for RunDto {
    fn from(m: upper_auto_manage_run::Model) -> Self {
        Self {
            id: m.id,
            started_at: m.started_at,
            finished_at: m.finished_at,
            status: m.status,
            checked_count: m.checked_count,
            disabled_count: m.disabled_count,
            enabled_count: m.enabled_count,
            banned_count: m.banned_count,
            banned_observation_count: m.banned_observation_count,
            active_count: m.active_count,
            indeterminate_count: m.indeterminate_count,
            still_inactive_count: m.still_inactive_count,
            error_message: m.error_message,
            summary: m.summary,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionDto {
    pub id: i32,
    pub run_id: i32,
    pub submission_id: i32,
    pub upper_name: String,
    pub action: bili_sync_entity::upper_auto_manage_action::ActionType,
    pub reason: Option<String>,
    pub created_at: DateTime,
}

impl From<upper_auto_manage_action::Model> for ActionDto {
    fn from(m: upper_auto_manage_action::Model) -> Self {
        Self {
            id: m.id,
            run_id: m.run_id,
            submission_id: m.submission_id,
            upper_name: m.upper_name,
            action: m.action,
            reason: m.reason,
            created_at: m.created_at,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyDto {
    pub submission_id: i32,
    pub policy: UpperManagePolicy,
    pub source: UpperManageSource,
    pub reason: Option<String>,
    pub updated_at: DateTime,
    pub upper_id: i64,
    pub upper_name: String,
    pub enabled: bool,
}

/// 候选投稿源：所有尚未被白/黑名单保护的 submission
/// 用于前端「为普通 UP 首次创建策略」的选择列表
#[derive(Serialize, FromQueryResult)]
#[serde(rename_all = "camelCase")]
pub struct CandidateDto {
    pub submission_id: i32,
    pub upper_id: i64,
    pub upper_name: String,
    pub enabled: bool,
    pub policy: Option<UpperManagePolicy>,
    pub source: Option<UpperManageSource>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListResponse<T> {
    pub total_count: u64,
    pub items: Vec<T>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PageQuery {
    pub page: Option<u64>,
    pub page_size: Option<u64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionQuery {
    pub run_id: Option<i32>,
    pub submission_id: Option<i32>,
    pub action: Option<String>,
    pub page: Option<u64>,
    pub page_size: Option<u64>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PolicyQuery {
    pub policy: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpsertPolicyRequest {
    pub policy: String,
    pub reason: Option<String>,
}

async fn get_status(
    Extension(db): Extension<DatabaseConnection>,
) -> Result<ApiResponse<UpperAutoManageStatusResponse>, ApiError> {
    let config = VersionedConfig::get().snapshot();
    let opt = &config.upper_auto_manage;
    let task_status = *UpperAutoManageTaskManager::get().subscribe().borrow();
    let last_run = upper_auto_manage_run::Entity::find()
        .order_by_desc(upper_auto_manage_run::Column::StartedAt)
        .one(&db)
        .await?
        .map(RunDto::from);
    Ok(ApiResponse::ok(UpperAutoManageStatusResponse {
        enabled: opt.enabled,
        interval: opt.interval.clone(),
        inactive_threshold_days: opt.inactive_threshold_days,
        check_concurrency: opt.check_concurrency,
        task_status,
        last_run,
    }))
}

async fn trigger_run() -> Result<ApiResponse<bool>, ApiError> {
    let queued = UpperAutoManageTaskManager::get()
        .run_once_with_trigger(crate::task::TaskTrigger::Manual)
        .await?;
    Ok(ApiResponse::ok(queued))
}

async fn list_runs(
    Extension(db): Extension<DatabaseConnection>,
    Query(q): Query<PageQuery>,
) -> Result<ApiResponse<ListResponse<RunDto>>, ApiError> {
    let page = q.page.unwrap_or(0);
    let page_size = q.page_size.unwrap_or(20).clamp(1, 100);
    let paginator = upper_auto_manage_run::Entity::find()
        .order_by_desc(upper_auto_manage_run::Column::StartedAt)
        .paginate(&db, page_size);
    let total = paginator.num_items().await?;
    let runs = paginator.fetch_page(page).await?;
    Ok(ApiResponse::ok(ListResponse {
        total_count: total,
        items: runs.into_iter().map(RunDto::from).collect(),
    }))
}

async fn list_run_actions(
    Extension(db): Extension<DatabaseConnection>,
    Path(run_id): Path<i32>,
    Query(q): Query<PageQuery>,
) -> Result<ApiResponse<ListResponse<ActionDto>>, ApiError> {
    let page = q.page.unwrap_or(0);
    let page_size = q.page_size.unwrap_or(50).clamp(1, 200);
    let paginator = upper_auto_manage_action::Entity::find()
        .filter(upper_auto_manage_action::Column::RunId.eq(run_id))
        .order_by_desc(upper_auto_manage_action::Column::CreatedAt)
        .paginate(&db, page_size);
    let total = paginator.num_items().await?;
    let actions = paginator.fetch_page(page).await?;
    Ok(ApiResponse::ok(ListResponse {
        total_count: total,
        items: actions.into_iter().map(ActionDto::from).collect(),
    }))
}

async fn list_actions(
    Extension(db): Extension<DatabaseConnection>,
    Query(q): Query<ActionQuery>,
) -> Result<ApiResponse<ListResponse<ActionDto>>, ApiError> {
    let page = q.page.unwrap_or(0);
    let page_size = q.page_size.unwrap_or(50).clamp(1, 200);
    let mut query = upper_auto_manage_action::Entity::find();
    if let Some(run_id) = q.run_id {
        query = query.filter(upper_auto_manage_action::Column::RunId.eq(run_id));
    }
    if let Some(submission_id) = q.submission_id {
        query = query.filter(upper_auto_manage_action::Column::SubmissionId.eq(submission_id));
    }
    if let Some(action) = q.action.as_deref() {
        let action = parse_action(action)?;
        query = query.filter(upper_auto_manage_action::Column::Action.eq(action));
    }
    let paginator = query
        .order_by_desc(upper_auto_manage_action::Column::CreatedAt)
        .paginate(&db, page_size);
    let total = paginator.num_items().await?;
    let actions = paginator.fetch_page(page).await?;
    Ok(ApiResponse::ok(ListResponse {
        total_count: total,
        items: actions.into_iter().map(ActionDto::from).collect(),
    }))
}

async fn list_policies(
    Extension(db): Extension<DatabaseConnection>,
    Query(q): Query<PolicyQuery>,
) -> Result<ApiResponse<Vec<PolicyDto>>, ApiError> {
    let mut query = upper_auto_manage_policy::Entity::find().find_also_related(submission::Entity);
    if let Some(policy) = q.policy.as_deref() {
        let policy = parse_policy(policy)?;
        query = query.filter(upper_auto_manage_policy::Column::Policy.eq(policy));
    }
    let rows = query.all(&db).await?;
    let policies = rows
        .into_iter()
        .filter_map(|(p, s)| {
            let s = s?;
            Some(PolicyDto {
                submission_id: p.submission_id,
                policy: p.policy,
                source: p.source,
                reason: p.reason,
                updated_at: p.updated_at,
                upper_id: s.upper_id,
                upper_name: s.upper_name,
                enabled: s.enabled,
            })
        })
        .collect();
    Ok(ApiResponse::ok(policies))
}

async fn upsert_policy(
    Path(submission_id): Path<i32>,
    Extension(db): Extension<DatabaseConnection>,
    Json(req): Json<UpsertPolicyRequest>,
) -> Result<ApiResponse<bool>, ApiError> {
    // 校验 submission 存在
    let submission_exists = submission::Entity::find_by_id(submission_id).one(&db).await?.is_some();
    if !submission_exists {
        return Err(InnerApiError::NotFound(submission_id).into());
    }
    let policy = parse_policy(&req.policy)?;
    let now = chrono::Utc::now().naive_utc();
    // 通过 per-submission 锁与巡检/启停串行，避免并发覆盖
    let lock = crate::task::lock_for_submission(submission_id);
    let _guard = lock.lock().await;
    // 显式 find + insert/update：ActiveModel::save() 在主键已 Set 时不一定走 insert 路径，
    // 之前普通 UP（无 policy 行）调用本接口会更新 0 行失败。
    // 改为「先查询，不存在则 insert，存在则 update」，确保两种情况都生效。
    // 手动 API 一律写入 Manual 来源，避免 UI 把「由人设置」误标为自动处理。
    let existing = upper_auto_manage_policy::Entity::find_by_id(submission_id)
        .one(&db)
        .await?;
    if existing.is_some() {
        upper_auto_manage_policy::ActiveModel {
            submission_id: Set(submission_id),
            policy: Set(policy),
            source: Set(UpperManageSource::Manual),
            reason: Set(req.reason),
            updated_at: Set(now),
        }
        .update(&db)
        .await?;
    } else {
        upper_auto_manage_policy::ActiveModel {
            submission_id: Set(submission_id),
            policy: Set(policy),
            source: Set(UpperManageSource::Manual),
            reason: Set(req.reason),
            updated_at: Set(now),
        }
        .insert(&db)
        .await?;
    }
    Ok(ApiResponse::ok(true))
}

async fn delete_policy(
    Path(submission_id): Path<i32>,
    Extension(db): Extension<DatabaseConnection>,
) -> Result<ApiResponse<bool>, ApiError> {
    // 校验 submission 存在
    let submission_exists = submission::Entity::find_by_id(submission_id).one(&db).await?.is_some();
    if !submission_exists {
        return Err(InnerApiError::NotFound(submission_id).into());
    }
    // 通过 per-submission 锁串行：「读旧策略 → reset」必须原子，避免巡检并发覆盖
    let lock = crate::task::lock_for_submission(submission_id);
    let _guard = lock.lock().await;
    // 先取出原策略，按其类型决定后续语义：
    //   - blacklist/banned → 改写为 normal+auto，允许自动恢复巡检重新启用
    //   - whitelist/normal → 直接删除策略行
    let existing = upper_auto_manage_policy::Entity::find_by_id(submission_id)
        .one(&db)
        .await?;
    match existing {
        Some(p) => crate::task::reset_policy_after_delete(&db, submission_id, p.policy).await?,
        None => {
            // 没有策略行：直接当作成功（no-op）
        }
    }
    Ok(ApiResponse::ok(true))
}

/// 列出所有可作为「首次创建策略」目标的投稿源
///
/// 返回所有 submission，包括当前没有策略行的（policy/source 字段为 null）。
/// 已存在白/黑名单策略的 UP 也返回，便于用户在同页修改或删除其策略。
async fn list_candidates(
    Extension(db): Extension<DatabaseConnection>,
) -> Result<ApiResponse<Vec<CandidateDto>>, ApiError> {
    let rows: Vec<CandidateDto> = CandidateDto::find_by_statement(Statement::from_string(
        db.get_database_backend(),
        "
SELECT s.id AS submission_id,
       s.upper_id AS upper_id,
       s.upper_name AS upper_name,
       s.enabled AS enabled,
       p.policy AS policy,
       p.source AS source
FROM submission s
LEFT JOIN upper_auto_manage_policy p ON p.submission_id = s.id
ORDER BY s.upper_name ASC
",
    ))
    .all(&db)
    .await
    .context("查询候选投稿源失败")?;
    Ok(ApiResponse::ok(rows))
}

fn parse_policy(s: &str) -> Result<UpperManagePolicy, ApiError> {
    match s {
        "normal" => Ok(UpperManagePolicy::Normal),
        "whitelist" => Ok(UpperManagePolicy::Whitelist),
        "blacklist" => Ok(UpperManagePolicy::Blacklist),
        "banned" => Ok(UpperManagePolicy::Banned),
        _ => Err(InnerApiError::BadRequest(format!("invalid policy: {s}")).into()),
    }
}

fn parse_action(s: &str) -> Result<bili_sync_entity::upper_auto_manage_action::ActionType, ApiError> {
    match s {
        "auto_disabled" => Ok(bili_sync_entity::upper_auto_manage_action::ActionType::AutoDisabled),
        "auto_enabled" => Ok(bili_sync_entity::upper_auto_manage_action::ActionType::AutoEnabled),
        "marked_banned" => Ok(bili_sync_entity::upper_auto_manage_action::ActionType::MarkedBanned),
        _ => Err(InnerApiError::BadRequest(format!("invalid action: {s}")).into()),
    }
}
