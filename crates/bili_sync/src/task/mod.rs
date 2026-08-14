mod http_server;
mod upper_auto_manage;
mod video_downloader;

pub use http_server::http_server;
pub use upper_auto_manage::{
    UpperAutoManageTaskManager, lock_for_submission, reset_policy_after_delete, upper_auto_manage,
};
pub use video_downloader::{DownloadTaskManager, TaskStatus, video_downloader};

/// 后台任务的触发方式：定时调度或用户手动触发
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskTrigger {
    Scheduled,
    Manual,
}

/// 后台任务运行历史（download_run / upper_auto_manage_run）的保留天数，
/// 各任务在收尾时按 started_at 清理超期行。
pub(crate) const RUN_RETENTION_DAYS: i64 = 30;
