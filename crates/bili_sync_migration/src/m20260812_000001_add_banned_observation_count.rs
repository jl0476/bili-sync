use sea_orm_migration::prelude::*;

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
                        ColumnDef::new(UpperAutoManageRun::BannedObservationCount)
                            .integer()
                            .not_null()
                            .default(0),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(UpperAutoManageRun::Table)
                    .drop_column(UpperAutoManageRun::BannedObservationCount)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum UpperAutoManageRun {
    Table,
    BannedObservationCount,
}
