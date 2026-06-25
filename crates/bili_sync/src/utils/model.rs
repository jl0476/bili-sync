use std::collections::HashMap;

use anyhow::{Context, Result, anyhow};
use bili_sync_entity::*;
use itertools::Itertools;
use rand::seq::SliceRandom;
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use sea_orm::sea_query::{Expr, OnConflict, SimpleExpr};
use sea_orm::{ConnectionTrait, DatabaseTransaction, IdenStatic, QuerySelect, QueryTrait, Statement};

use crate::adapter::{VideoSource, VideoSourceEnum};
use crate::bilibili::VideoInfo;
use crate::config::Config;
use crate::utils::status::{PageStatus, STATUS_COMPLETED, VideoStatus};

/// 筛选未填充的视频
pub async fn filter_unfilled_videos(
    additional_expr: SimpleExpr,
    conn: &DatabaseConnection,
) -> Result<Vec<video::Model>> {
    video::Entity::find()
        .filter(
            video::Column::Valid
                .eq(true)
                .and(video::Column::DownloadStatus.eq(0))
                .and(video::Column::Category.eq(2))
                .and(video::Column::SinglePage.is_null())
                .and(additional_expr),
        )
        .all(conn)
        .await
        .context("filter unfilled videos failed")
}

/// 筛选未处理完成的视频和视频页
pub async fn filter_unhandled_video_pages(
    additional_expr: SimpleExpr,
    connection: &DatabaseConnection,
) -> Result<Vec<(video::Model, Vec<page::Model>)>> {
    // 跨源去重：排除 bvid 已在其他源下载完成的视频
    let completed_bvids = video::Entity::find()
        .filter(video::Column::DownloadStatus.gte(STATUS_COMPLETED))
        .select_only()
        .column(video::Column::Bvid)
        .as_query()
        .to_owned();

    video::Entity::find()
        .filter(
            video::Column::Valid
                .eq(true)
                .and(video::Column::DownloadStatus.lt(STATUS_COMPLETED))
                .and(video::Column::Category.eq(2))
                .and(video::Column::SinglePage.is_not_null())
                .and(video::Column::ShouldDownload.eq(true))
                .and(additional_expr)
                .and(Expr::col(video::Column::Bvid).not_in_subquery(completed_bvids)),
        )
        .find_with_related(page::Entity)
        .all(connection)
        .await
        .context("filter unhandled video pages failed")
}

/// 标记跨源重复视频为已完成，并引用已完成视频的下载路径，避免重复下载。
/// 对 video 及其所有 page 都标记完成状态并引用源视频的路径，返回被标记的视频数量。
/// 每个被标记的视频会单独打印一条 info 日志，包含视频标题、BV 号与视频源名。
pub async fn mark_cross_source_duplicates(
    additional_expr: SimpleExpr,
    connection: &DatabaseConnection,
    source_name: &str,
) -> Result<usize> {
    // 查出当前源中 bvid 已在其他源下载完成、但自身未完成的视频
    let duplicate_videos = video::Entity::find()
        .filter(additional_expr)
        .filter(video::Column::Valid.eq(true))
        .filter(video::Column::ShouldDownload.eq(true))
        .filter(video::Column::DownloadStatus.lt(STATUS_COMPLETED))
        .filter(video::Column::SinglePage.is_not_null())
        .filter(
            video::Column::Bvid.in_subquery(
                video::Entity::find()
                    .filter(video::Column::DownloadStatus.gte(STATUS_COMPLETED))
                    .select_only()
                    .column(video::Column::Bvid)
                    .as_query()
                    .to_owned(),
            ),
        )
        .all(connection)
        .await?;

    if duplicate_videos.is_empty() {
        return Ok(0);
    }

    // 被去重视频的 id -> bvid 映射，用于后续 page 按 pid 匹配源视频的 page
    let duplicate_bvid_by_id: HashMap<i32, String> = duplicate_videos.iter().map(|v| (v.id, v.bvid.clone())).collect();
    let bvids: Vec<String> = duplicate_videos.iter().map(|v| v.bvid.clone()).collect();

    // 查出这些 bvid 对应的、已下载完成且 path 非空的视频（源视频）
    let completed_videos = video::Entity::find()
        .filter(video::Column::Bvid.is_in(bvids))
        .filter(video::Column::DownloadStatus.gte(STATUS_COMPLETED))
        .filter(video::Column::Path.ne(""))
        .all(connection)
        .await?;

    if completed_videos.is_empty() {
        return Ok(0);
    }

    let completed_path_by_bvid: HashMap<String, String> = completed_videos
        .iter()
        .map(|v| (v.bvid.clone(), v.path.clone()))
        .collect();
    let completed_bvid_by_id: HashMap<i32, String> = completed_videos.iter().map(|v| (v.id, v.bvid.clone())).collect();
    let completed_video_ids: Vec<i32> = completed_videos.iter().map(|v| v.id).collect();

    // 仅保留能找到引用 path 的被去重视频，组装 video ActiveModel
    let completed_video_status: u32 = VideoStatus::from([7u32, 7, 7, 7, 7]).into();
    let to_mark: Vec<(video::Model, String)> = duplicate_videos
        .into_iter()
        .filter_map(|v| {
            let path = completed_path_by_bvid.get(&v.bvid).filter(|p| !p.is_empty())?.clone();
            Some((v, path))
        })
        .collect();

    if to_mark.is_empty() {
        return Ok(0);
    }

    let marked_count = to_mark.len();
    let marked_video_ids: Vec<i32> = to_mark.iter().map(|(v, _)| v.id).collect();
    let video_models: Vec<video::ActiveModel> = to_mark
        .into_iter()
        .map(|(v, path)| {
            info!(
                "跨源去重：视频「{}」({}) 已在视频源「{}」下载完成，已标记为完成并引用路径",
                v.name, v.bvid, source_name
            );
            let mut am: video::ActiveModel = v.into();
            am.download_status = Set(completed_video_status);
            am.path = Set(path);
            am
        })
        .collect();
    update_videos_model(video_models, connection).await?;

    // 处理对应的 pages：标记完成并引用源视频对应 (bvid, pid) 的 page.path
    let (duplicate_pages, source_pages) = tokio::try_join!(
        page::Entity::find()
            .filter(page::Column::VideoId.is_in(marked_video_ids.clone()))
            .all(connection),
        page::Entity::find()
            .filter(page::Column::VideoId.is_in(completed_video_ids.clone()))
            .all(connection),
    )?;

    // 源视频的 pages，按 (bvid, pid) -> page.path
    let source_page_path: HashMap<(String, i32), String> = source_pages
        .iter()
        .filter_map(|p| {
            let bvid = completed_bvid_by_id.get(&p.video_id)?;
            let path = p.path.as_ref().filter(|s| !s.is_empty())?;
            Some(((bvid.clone(), p.pid), path.clone()))
        })
        .collect();

    let completed_page_status: u32 = PageStatus::from([7u32, 7, 7, 7, 7]).into();
    let page_models: Vec<page::ActiveModel> = duplicate_pages
        .into_iter()
        .filter_map(|p| {
            let bvid = duplicate_bvid_by_id.get(&p.video_id)?;
            let path = source_page_path.get(&(bvid.clone(), p.pid))?;
            let mut am: page::ActiveModel = p.into();
            am.download_status = Set(completed_page_status);
            am.path = Set(Some(path.clone()));
            Some(am)
        })
        .collect();

    if !page_models.is_empty() {
        update_pages_model(page_models, connection).await?;
    }

    Ok(marked_count)
}

/// 尝试创建 Video Model，如果发生冲突则忽略
pub async fn create_videos(
    videos_info: Vec<VideoInfo>,
    video_source: &VideoSourceEnum,
    connection: &DatabaseConnection,
) -> Result<()> {
    let video_models = videos_info
        .into_iter()
        .map(|v| {
            let mut model = v.into_simple_model();
            video_source.set_relation_id(&mut model);
            model
        })
        .collect::<Vec<_>>();
    video::Entity::insert_many(video_models)
        // 这里想表达的是 on 索引名，但 sea-orm 的 api 似乎只支持列名而不支持索引名，好在留空可以达到相同的目的
        .on_conflict(OnConflict::new().do_nothing().to_owned())
        .do_nothing()
        .exec(connection)
        .await?;
    Ok(())
}

/// 尝试创建 Page Model，如果发生冲突则忽略
pub async fn create_pages(pages_model: Vec<page::ActiveModel>, connection: &DatabaseTransaction) -> Result<()> {
    let mut pages = pages_model.into_iter();
    loop {
        // 这里 insert_many 要求 IntoIterator，vec 上调用 chunks 返回的类型不匹配，需要 to_vec 做 clone
        // itertools 的 into_iter().chunks() 由于 !Send 也无法直接使用
        // 暂时手写 take + collect 作为避免 clone 的折中方案
        let page_chunk = pages.by_ref().take(200).collect::<Vec<_>>();
        if page_chunk.is_empty() {
            break;
        }
        page::Entity::insert_many(page_chunk)
            .on_conflict(
                OnConflict::columns([page::Column::VideoId, page::Column::Pid])
                    .do_nothing()
                    .to_owned(),
            )
            .do_nothing()
            .exec(connection)
            .await?;
    }
    Ok(())
}

/// 更新视频 model 的详情字段
pub async fn update_video_detail_models(
    videos: Vec<video::ActiveModel>,
    connection: &DatabaseTransaction,
) -> Result<()> {
    if videos.is_empty() {
        return Ok(());
    }
    let columns = [
        video::Column::Id,
        video::Column::CollectionId,
        video::Column::FavoriteId,
        video::Column::WatchLaterId,
        video::Column::SubmissionId,
        video::Column::UpperId,
        video::Column::UpperName,
        video::Column::UpperFace,
        video::Column::Staff,
        video::Column::Name,
        video::Column::Bvid,
        video::Column::Intro,
        video::Column::Cover,
        video::Column::Ctime,
        video::Column::Pubtime,
        video::Column::Favtime,
        video::Column::DownloadStatus,
        video::Column::Valid,
        video::Column::ShouldDownload,
        video::Column::Tags,
        video::Column::SinglePage,
    ];
    let row = format!("({})", std::iter::repeat_n("?", columns.len()).join(", "));
    let rows = std::iter::repeat_n(row.as_str(), videos.len()).join(", ");
    let mut values = Vec::with_capacity(videos.len() * columns.len());
    for video in videos {
        for column in columns {
            values.push(
                video
                    .get(column)
                    .into_value()
                    .ok_or_else(|| anyhow!("video column {} is not set", column.as_str()))?,
            );
        }
    }
    let sql = format!(
        "WITH tempdata({}) AS (VALUES {}) \
        UPDATE video \
        SET {} \
        FROM tempdata \
        WHERE video.id = tempdata.id",
        columns.iter().map(IdenStatic::as_str).join(", "),
        rows,
        columns
            .iter()
            .skip(1)
            .map(|column| {
                let column = column.as_str();
                format!("{} = tempdata.{}", column, column)
            })
            .join(", ")
    );
    connection
        .execute(Statement::from_sql_and_values(
            connection.get_database_backend(),
            sql,
            values,
        ))
        .await?;
    Ok(())
}

/// 将视频标记为失效
pub async fn set_video_models_invalid(video_ids: Vec<i32>, connection: &DatabaseTransaction) -> Result<()> {
    if video_ids.is_empty() {
        return Ok(());
    }
    video::Entity::update_many()
        .filter(video::Column::Id.is_in(video_ids))
        .col_expr(video::Column::Valid, Expr::value(false))
        .exec(connection)
        .await?;
    Ok(())
}

/// 更新视频 model 的下载状态
pub async fn update_videos_model(videos: Vec<video::ActiveModel>, connection: &DatabaseConnection) -> Result<()> {
    video::Entity::insert_many(videos)
        .on_conflict(
            OnConflict::column(video::Column::Id)
                .update_columns([video::Column::DownloadStatus, video::Column::Path])
                .to_owned(),
        )
        .exec(connection)
        .await?;
    Ok(())
}

/// 更新视频页 model 的下载状态
pub async fn update_pages_model(pages: Vec<page::ActiveModel>, connection: &DatabaseConnection) -> Result<()> {
    let query = page::Entity::insert_many(pages).on_conflict(
        OnConflict::column(page::Column::Id)
            .update_columns([page::Column::DownloadStatus, page::Column::Path])
            .to_owned(),
    );
    query.exec(connection).await?;
    Ok(())
}

/// 获取所有已经启用的视频源
pub async fn get_enabled_video_sources(connection: &DatabaseConnection) -> Result<Vec<VideoSourceEnum>> {
    let (favorite, watch_later, submission, collection) = tokio::try_join!(
        favorite::Entity::find()
            .filter(favorite::Column::Enabled.eq(true))
            .all(connection),
        watch_later::Entity::find()
            .filter(watch_later::Column::Enabled.eq(true))
            .all(connection),
        submission::Entity::find()
            .filter(submission::Column::Enabled.eq(true))
            .all(connection),
        collection::Entity::find()
            .filter(collection::Column::Enabled.eq(true))
            .all(connection),
    )?;
    let mut sources = Vec::with_capacity(favorite.len() + watch_later.len() + submission.len() + collection.len());
    sources.extend(favorite.into_iter().map(VideoSourceEnum::from));
    sources.extend(watch_later.into_iter().map(VideoSourceEnum::from));
    sources.extend(submission.into_iter().map(VideoSourceEnum::from));
    sources.extend(collection.into_iter().map(VideoSourceEnum::from));
    // 此处将视频源随机打乱顺序，从概率上确保每个视频源都有机会优先执行，避免后面视频源的长期饥饿问题
    sources.shuffle(&mut rand::rng());
    Ok(sources)
}

/// 从数据库中加载配置
pub async fn load_db_config(connection: &DatabaseConnection) -> Result<Option<Result<Config>>> {
    Ok(bili_sync_entity::config::Entity::find_by_id(1)
        .one(connection)
        .await?
        .map(|model| {
            serde_json::from_str(&model.data).map_err(|e| anyhow!("Failed to deserialize config data: {}", e))
        }))
}

/// 保存配置到数据库
pub async fn save_db_config(config: &Config, connection: &DatabaseConnection) -> Result<()> {
    let data = serde_json::to_string(config).context("Failed to serialize config data")?;
    let model = bili_sync_entity::config::ActiveModel {
        id: Set(1),
        data: Set(data),
        ..Default::default()
    };
    bili_sync_entity::config::Entity::insert(model)
        .on_conflict(
            OnConflict::column(bili_sync_entity::config::Column::Id)
                .update_column(bili_sync_entity::config::Column::Data)
                .to_owned(),
        )
        .exec(connection)
        .await
        .context("Failed to save config to database")?;
    Ok(())
}
