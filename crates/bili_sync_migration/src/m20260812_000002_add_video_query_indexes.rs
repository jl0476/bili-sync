use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (name, column) in [
            ("idx_video_favorite_id_id", Video::FavoriteId),
            ("idx_video_submission_id_id", Video::SubmissionId),
            ("idx_video_collection_id_id", Video::CollectionId),
            ("idx_video_watch_later_id_id", Video::WatchLaterId),
        ] {
            manager
                .create_index(
                    Index::create()
                        .table(Video::Table)
                        .name(name)
                        .col(column)
                        .col(Video::Id)
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for name in [
            "idx_video_favorite_id_id",
            "idx_video_submission_id_id",
            "idx_video_collection_id_id",
            "idx_video_watch_later_id_id",
        ] {
            manager
                .drop_index(Index::drop().table(Video::Table).name(name).to_owned())
                .await?;
        }

        Ok(())
    }
}

#[derive(DeriveIden)]
enum Video {
    Table,
    Id,
    FavoriteId,
    CollectionId,
    SubmissionId,
    WatchLaterId,
}
