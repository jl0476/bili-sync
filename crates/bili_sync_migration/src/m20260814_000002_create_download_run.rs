use sea_orm_migration::prelude::*;

/// 创建 download_run 表：每轮视频下载扫描任务一行记录（开始/结束/状态/触发方式），
/// 用于展示执行耗时与运行历史，保留 30 天由任务收尾自动清理。
#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(DownloadRun::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(DownloadRun::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(DownloadRun::StartedAt).timestamp().not_null())
                    .col(ColumnDef::new(DownloadRun::FinishedAt).timestamp().null())
                    .col(ColumnDef::new(DownloadRun::Status).text().not_null())
                    .col(ColumnDef::new(DownloadRun::Trigger).text().not_null())
                    .col(ColumnDef::new(DownloadRun::ErrorMessage).text().null())
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_download_run_started_at")
                    .table(DownloadRun::Table)
                    .col(DownloadRun::StartedAt)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(DownloadRun::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum DownloadRun {
    Table,
    Id,
    StartedAt,
    FinishedAt,
    Status,
    Trigger,
    ErrorMessage,
}
