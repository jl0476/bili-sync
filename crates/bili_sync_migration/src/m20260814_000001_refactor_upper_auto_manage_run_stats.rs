use sea_orm_migration::prelude::*;

/// 重构巡检统计口径：
/// - skipped 语义收窄为 still_inactive（仅阶段二「复查仍不活跃」）
/// - 新增 active（阶段一「近期有更新，无需动作」）
/// - 新增 indeterminate（阶段一「本地无任何视频，无法判定」）
#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(UpperAutoManageRun::Table)
                    .add_column(
                        ColumnDef::new(UpperAutoManageRun::ActiveCount)
                            .integer()
                            .not_null()
                            .default(0),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(UpperAutoManageRun::Table)
                    .add_column(
                        ColumnDef::new(UpperAutoManageRun::IndeterminateCount)
                            .integer()
                            .not_null()
                            .default(0),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(UpperAutoManageRun::Table)
                    .rename_column(UpperAutoManageRun::SkippedCount, UpperAutoManageRun::StillInactiveCount)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(UpperAutoManageRun::Table)
                    .rename_column(UpperAutoManageRun::StillInactiveCount, UpperAutoManageRun::SkippedCount)
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(UpperAutoManageRun::Table)
                    .drop_column(UpperAutoManageRun::IndeterminateCount)
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(UpperAutoManageRun::Table)
                    .drop_column(UpperAutoManageRun::ActiveCount)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum UpperAutoManageRun {
    Table,
    ActiveCount,
    IndeterminateCount,
    SkippedCount,
    StillInactiveCount,
}
