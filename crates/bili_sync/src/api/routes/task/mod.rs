use anyhow::Result;
use axum::Router;
use axum::extract::{Extension, Query};
use axum::routing::{get, post};
use bili_sync_entity::download_run;
use bili_sync_entity::download_run::RunTrigger;
use bili_sync_entity::upper_auto_manage_run::RunStatus;
use sea_orm::{DatabaseConnection, EntityTrait, PaginatorTrait, QueryOrder};
use serde::Serialize;

use super::upper_auto_manage::{ListResponse, PageQuery};
use crate::api::wrapper::{ApiError, ApiResponse};
use crate::config::{Trigger, VersionedConfig};
use crate::task::{DownloadTaskManager, TaskStatus};

pub(super) fn router() -> Router {
    Router::new()
        .route("/task/download", post(new_download_task))
        .route("/task/download/status", get(get_download_status))
        .route("/task/download/runs", get(list_download_runs))
}

pub async fn new_download_task() -> Result<ApiResponse<bool>, ApiError> {
    DownloadTaskManager::get().download_once().await?;
    Ok(ApiResponse::ok(true))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadRunDto {
    pub id: i32,
    pub started_at: chrono::NaiveDateTime,
    pub finished_at: Option<chrono::NaiveDateTime>,
    pub status: RunStatus,
    pub trigger: RunTrigger,
    pub error_message: Option<String>,
}

impl From<download_run::Model> for DownloadRunDto {
    fn from(m: download_run::Model) -> Self {
        Self {
            id: m.id,
            started_at: m.started_at,
            finished_at: m.finished_at,
            status: m.status,
            trigger: m.trigger,
            error_message: m.error_message,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadTaskStatusResponse {
    pub task_status: TaskStatus,
    pub interval: Trigger,
    pub last_run: Option<DownloadRunDto>,
}

async fn get_download_status(
    Extension(db): Extension<DatabaseConnection>,
) -> Result<ApiResponse<DownloadTaskStatusResponse>, ApiError> {
    let interval = VersionedConfig::get().read().interval.clone();
    let task_status = *DownloadTaskManager::get().subscribe().borrow();
    let last_run = download_run::Entity::find()
        .order_by_desc(download_run::Column::StartedAt)
        .one(&db)
        .await?
        .map(DownloadRunDto::from);
    Ok(ApiResponse::ok(DownloadTaskStatusResponse {
        task_status,
        interval,
        last_run,
    }))
}

async fn list_download_runs(
    Extension(db): Extension<DatabaseConnection>,
    Query(q): Query<PageQuery>,
) -> Result<ApiResponse<ListResponse<DownloadRunDto>>, ApiError> {
    let page = q.page.unwrap_or(0);
    let page_size = q.page_size.unwrap_or(20).clamp(1, 100);
    let paginator = download_run::Entity::find()
        .order_by_desc(download_run::Column::StartedAt)
        .paginate(&db, page_size);
    let total = paginator.num_items().await?;
    let runs = paginator.fetch_page(page).await?;
    Ok(ApiResponse::ok(ListResponse {
        total_count: total,
        items: runs.into_iter().map(DownloadRunDto::from).collect(),
    }))
}
