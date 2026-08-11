use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // 表 1: upper_auto_manage_policy（每个 submission 至多一行，1:1 关联）
        manager
            .create_table(
                Table::create()
                    .table(UpperAutoManagePolicy::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(UpperAutoManagePolicy::SubmissionId)
                            .integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(UpperAutoManagePolicy::Policy)
                            .text()
                            .not_null()
                            .default("normal"),
                    )
                    .col(
                        ColumnDef::new(UpperAutoManagePolicy::Source)
                            .text()
                            .not_null()
                            .default("auto"),
                    )
                    .col(ColumnDef::new(UpperAutoManagePolicy::Reason).text().null())
                    .col(
                        ColumnDef::new(UpperAutoManagePolicy::UpdatedAt)
                            .timestamp()
                            .default(Expr::current_timestamp())
                            .not_null(),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(UpperAutoManagePolicy::Table, UpperAutoManagePolicy::SubmissionId)
                            .to(Submission::Table, Submission::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        // 表 2: upper_auto_manage_run（每次巡检任务执行一行摘要）
        manager
            .create_table(
                Table::create()
                    .table(UpperAutoManageRun::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(UpperAutoManageRun::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(UpperAutoManageRun::StartedAt).timestamp().not_null())
                    .col(ColumnDef::new(UpperAutoManageRun::FinishedAt).timestamp().null())
                    .col(ColumnDef::new(UpperAutoManageRun::Status).text().not_null())
                    .col(
                        ColumnDef::new(UpperAutoManageRun::CheckedCount)
                            .integer()
                            .not_null()
                            .default(0),
                    )
                    .col(
                        ColumnDef::new(UpperAutoManageRun::DisabledCount)
                            .integer()
                            .not_null()
                            .default(0),
                    )
                    .col(
                        ColumnDef::new(UpperAutoManageRun::EnabledCount)
                            .integer()
                            .not_null()
                            .default(0),
                    )
                    .col(
                        ColumnDef::new(UpperAutoManageRun::BannedCount)
                            .integer()
                            .not_null()
                            .default(0),
                    )
                    .col(
                        ColumnDef::new(UpperAutoManageRun::SkippedCount)
                            .integer()
                            .not_null()
                            .default(0),
                    )
                    .col(ColumnDef::new(UpperAutoManageRun::ErrorMessage).text().null())
                    .col(ColumnDef::new(UpperAutoManageRun::Summary).text().null())
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_upper_auto_manage_run_started_at")
                    .table(UpperAutoManageRun::Table)
                    .col(UpperAutoManageRun::StartedAt)
                    .to_owned(),
            )
            .await?;
        // 表 3: upper_auto_manage_action（每个操作一行明细）
        manager
            .create_table(
                Table::create()
                    .table(UpperAutoManageAction::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(UpperAutoManageAction::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(UpperAutoManageAction::RunId).integer().not_null())
                    .col(ColumnDef::new(UpperAutoManageAction::SubmissionId).integer().not_null())
                    .col(ColumnDef::new(UpperAutoManageAction::UpperName).text().not_null())
                    .col(ColumnDef::new(UpperAutoManageAction::Action).text().not_null())
                    .col(ColumnDef::new(UpperAutoManageAction::Reason).text().null())
                    .col(
                        ColumnDef::new(UpperAutoManageAction::CreatedAt)
                            .timestamp()
                            .default(Expr::current_timestamp())
                            .not_null(),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(UpperAutoManageAction::Table, UpperAutoManageAction::RunId)
                            .to(UpperAutoManageRun::Table, UpperAutoManageRun::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(UpperAutoManageAction::Table, UpperAutoManageAction::SubmissionId)
                            .to(Submission::Table, Submission::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_upper_auto_manage_action_run_id")
                    .table(UpperAutoManageAction::Table)
                    .col(UpperAutoManageAction::RunId)
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_upper_auto_manage_action_submission_id")
                    .table(UpperAutoManageAction::Table)
                    .col(UpperAutoManageAction::SubmissionId)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(UpperAutoManageAction::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(UpperAutoManageRun::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(UpperAutoManagePolicy::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum Submission {
    Table,
    Id,
}

#[derive(DeriveIden)]
enum UpperAutoManagePolicy {
    Table,
    SubmissionId,
    Policy,
    Source,
    Reason,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum UpperAutoManageRun {
    Table,
    Id,
    StartedAt,
    FinishedAt,
    Status,
    CheckedCount,
    DisabledCount,
    EnabledCount,
    BannedCount,
    SkippedCount,
    ErrorMessage,
    Summary,
}

#[derive(DeriveIden)]
enum UpperAutoManageAction {
    Table,
    Id,
    RunId,
    SubmissionId,
    UpperName,
    Action,
    Reason,
    CreatedAt,
}
