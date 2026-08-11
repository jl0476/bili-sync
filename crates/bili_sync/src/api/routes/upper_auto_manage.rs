use axum::extract::{Extension, Path, Query};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use bili_sync_entity::{submission, upper_auto_manage_action, upper_auto_manage_policy, upper_auto_manage_run};
use bili_sync_entity::upper_auto_manage_policy::{UpperManagePolicy, UpperManageSource};
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder, TransactionTrait};
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
        .route(
            "/upper-auto-manage/runs/{run_id}/actions",
            get(list_run_actions),
        )
        .route("/upper-auto-manage/actions", get(list_actions))
        .route("/upper-auto-manage/policies", get(list_policies))
        .route(
            "/upper-auto-manage/policies/{submission_id}",
            put(upsert_policy).delete(delete_policy),
        )
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
    pub skipped_count: i32,
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
            skipped_count: m.skipped_count,
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
    UpperAutoManageTaskManager::get().run_once().await?;
    Ok(ApiResponse::ok(true))
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
    let submission_exists = submission::Entity::find_by_id(submission_id)
        .one(&db)
        .await?
        .is_some();
    if !submission_exists {
        return Err(InnerApiError::NotFound(submission_id).into());
    }
    let policy = parse_policy(&req.policy)?;
    let txn = db.begin().await?;
    let existing = upper_auto_manage_policy::Entity::find_by_id(submission_id)
        .one(&txn)
        .await?;
    let now = chrono::Utc::now().naive_utc();
    if existing.is_some() {
        upper_auto_manage_policy::ActiveModel {
            submission_id: Set(submission_id),
            policy: Set(policy),
            reason: Set(req.reason),
            updated_at: Set(now),
            ..Default::default()
        }
        .update(&txn)
        .await?;
    } else {
        upper_auto_manage_policy::ActiveModel {
            submission_id: Set(submission_id),
            policy: Set(policy),
            source: Set(UpperManageSource::Auto),
            reason: Set(req.reason),
            updated_at: Set(now),
        }
        .insert(&txn)
        .await?;
    }
    txn.commit().await?;
    Ok(ApiResponse::ok(true))
}

async fn delete_policy(
    Path(submission_id): Path<i32>,
    Extension(db): Extension<DatabaseConnection>,
) -> Result<ApiResponse<bool>, ApiError> {
    upper_auto_manage_policy::Entity::delete_by_id(submission_id)
        .exec(&db)
        .await?;
    Ok(ApiResponse::ok(true))
}

fn parse_policy(s: &str) -> Result<UpperManagePolicy, ApiError> {
    match s {
        "normal" => Ok(UpperManagePolicy::Normal),
        "whitelist" => Ok(UpperManagePolicy::Whitelist),
        "blacklist" => Ok(UpperManagePolicy::Blacklist),
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
