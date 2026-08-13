# 数据库性能优化方案

## 1. 文档目的

本文用于指导 BiliSync 数据库性能优化的开发、发布和验收，重点解决视频页面加载时间过长的问题。

本文基于 `2026-08-12` 对实际数据库 `data.sqlite` 的只读分析结果编写，不包含对用户数据库的直接修改。

## 2. 分析结论

实际数据库规模如下：

| 项目 | 数量 |
| --- | ---: |
| `video` | 31,079 行 |
| `page` | 31,527 行 |
| `submission` | 126 行 |
| `favorite` | 5 行 |
| 数据库大小 | 约 31.3 MiB |
| 完整性检查 | `ok` |
| SQLite 模式 | WAL |

当前主要瓶颈在 `video` 表：

1. 按收藏夹、UP 主等来源筛选时没有对应的单列索引。
2. 视频列表接口每次加载都会执行精确 `COUNT(*)`。
3. 标题和 BV 搜索使用 `%关键词%`，普通 B-tree 索引无法有效使用。
4. 列表分页使用 `OFFSET`，页码越大，扫描和跳过的数据越多。
5. `page` 表已经存在 `(video_id, pid)` 索引，不是当前视频列表页面的主要瓶颈。

实际执行计划中，以下查询均出现 `SCAN video`：

- 默认列表查询；
- 收藏夹筛选；
- 投稿用户筛选；
- 标题/BV 搜索；
- 状态筛选。

## 3. 优化目标

### 3.1 近期目标

- 让来源筛选和来源统计使用索引。
- 保持现有 API 和前端分页行为不变。
- 通过数据库迁移自动完成升级。
- 迁移可回滚，不破坏现有业务数据。

### 3.2 中长期目标

- 减少或取消列表页的精确总数统计。
- 使用游标分页替代深度 `OFFSET` 分页。
- 视频数量达到更大规模后，引入 SQLite FTS5 搜索。

## 4. 第一阶段：新增视频来源索引

### 4.1 推荐索引

新增以下四个复合索引：

```sql
CREATE INDEX idx_video_favorite_id_id
ON video(favorite_id, id DESC);

CREATE INDEX idx_video_submission_id_id
ON video(submission_id, id DESC);

CREATE INDEX idx_video_collection_id_id
ON video(collection_id, id DESC);

CREATE INDEX idx_video_watch_later_id_id
ON video(watch_later_id, id DESC);
```

索引字段顺序必须是“筛选字段在前、排序字段在后”，以覆盖如下查询：

```sql
SELECT id, name, bvid
FROM video
WHERE favorite_id = ?
ORDER BY id DESC
LIMIT 20;
```

以及：

```sql
SELECT COUNT(*)
FROM video
WHERE favorite_id = ?;
```

### 4.2 推荐实现方式

新增迁移文件：

```text
crates/bili_sync_migration/src/m20260812_000001_add_video_query_indexes.rs
```

迁移应使用项目现有的 SeaORM Migration 风格，并实现 `up` 与 `down`。

核心结构如下：

```rust
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_index(
                Index::create()
                    .table(Video::Table)
                    .name("idx_video_favorite_id_id")
                    .col(Video::FavoriteId)
                    .col(Video::Id)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .table(Video::Table)
                    .name("idx_video_submission_id_id")
                    .col(Video::SubmissionId)
                    .col(Video::Id)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .table(Video::Table)
                    .name("idx_video_collection_id_id")
                    .col(Video::CollectionId)
                    .col(Video::Id)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .table(Video::Table)
                    .name("idx_video_watch_later_id_id")
                    .col(Video::WatchLaterId)
                    .col(Video::Id)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for name in [
            "idx_video_favorite_id_id",
            "idx_video_submission_id_id",
            "idx_video_collection_id_id",
            "idx_video_watch_later_id_id",
        ] {
            manager
                .drop_index(
                    Index::drop()
                        .table(Video::Table)
                        .name(name)
                        .to_owned(),
                )
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
```

如果当前 SeaORM 版本明确支持索引排序，可以将 `.col(Video::Id)` 改为降序索引；使用普通索引也可以由 SQLite 反向扫描，兼容性更好。

## 5. 第二阶段：验证索引效果

数据库迁移完成后执行：

```sql
ANALYZE;
PRAGMA optimize;
```

然后检查索引：

```sql
PRAGMA index_list('video');
```

检查来源查询执行计划：

```sql
EXPLAIN QUERY PLAN
SELECT id, name, bvid
FROM video
WHERE favorite_id = 222131
ORDER BY id DESC
LIMIT 20;
```

预期结果应包含：

```text
SEARCH video USING INDEX idx_video_favorite_id_id
```

不应再出现：

```text
SCAN video
```

同时验证统计查询：

```sql
EXPLAIN QUERY PLAN
SELECT COUNT(*)
FROM video
WHERE favorite_id = 222131;
```

## 6. 第三阶段：优化视频列表接口

修改文件：

```text
crates/bili_sync/src/api/routes/videos/mod.rs
```

当前接口每次加载列表都会执行：

```rust
let total_count = query.clone().count(&db).await?;
```

然后再执行分页查询。索引完成后，来源筛选的统计性能会改善，但默认列表仍然需要扫描或统计全部视频。

### 6.1 短期方案

第一阶段暂时保留 `total_count`，只新增索引，降低变更风险。

### 6.2 中期方案

增加 `has_next` 字段，避免每次精确统计总数：

```json
{
  "videos": [],
  "has_next": true
}
```

服务端查询 `page_size + 1` 条数据：

```sql
SELECT ...
FROM video
ORDER BY id DESC
LIMIT 21;
```

如果返回 21 条，则只向前端返回 20 条并设置 `has_next = true`。

## 7. 第四阶段：使用游标分页

当前分页方式：

```sql
ORDER BY id DESC
LIMIT 20 OFFSET 10000;
```

推荐改为基于 `id` 的游标分页：

```sql
SELECT id, name, bvid
FROM video
WHERE id < ?
ORDER BY id DESC
LIMIT 20;
```

接口可以增加：

```text
/api/videos?cursor=28500&page_size=20
```

返回：

```json
{
  "videos": [],
  "next_cursor": 28480,
  "has_next": true
}
```

建议实施顺序：

1. 保留现有 `page` 和 `page_size` 参数。
2. 新增可选 `cursor` 参数。
3. 前端优先使用 cursor 浏览。
4. 验证稳定后，再考虑移除深度页码跳转。

## 8. 第五阶段：优化搜索

当前搜索等价于：

```sql
WHERE name LIKE '%关键词%'
   OR bvid LIKE '%关键词%'
```

普通索引无法有效优化前置通配符。

### 8.1 短期方案

- 搜索词少于 2 个字符时不执行请求。
- 前端增加输入防抖。
- BV 号优先使用精确匹配或前缀匹配。
- 暂不为 `name` 建普通索引，因为对 `%关键词%` 基本无效。

### 8.2 中长期方案：SQLite FTS5

当视频数量达到 10 万以上，或搜索成为主要使用场景时，再考虑：

```sql
CREATE VIRTUAL TABLE video_fts USING fts5(
    name,
    bvid,
    content='video',
    content_rowid='id'
);
```

FTS5 需要同步处理视频新增、更新和删除，建议单独设计迁移和测试，不与第一阶段索引同时上线。

## 9. 第六阶段：状态筛选

状态筛选使用 `download_status` 位运算，例如：

```sql
(download_status >> ?) & 7
```

普通索引对这种表达式的帮助有限，因此暂不建议仅为 `download_status` 创建索引。

如果“正常/跳过/无效”筛选使用频繁，可以增加：

```sql
CREATE INDEX idx_video_valid_should_download_id
ON video(valid, should_download, id);
```

该索引主要服务于：

```sql
WHERE valid = 1
  AND should_download = 1
ORDER BY id DESC;
```

当前不建议立即拆分 `download_status`。拆分会涉及状态迁移、下载流程、批量更新和兼容性测试，适合在数据规模进一步增长后评估。

## 10. `page` 表处理建议

当前 `page` 表已有：

```text
idx_page_video_id_pid(video_id, pid)
```

单个视频详情查询已经能够使用该索引，因此当前不建议对 `page` 表进行结构调整。

只有在出现以下场景时再单独优化：

- 批量重置所有视频明显变慢；
- 批量更新状态明显变慢；
- 删除大量视频时出现长时间锁等待；
- `page` 行数增长到 `video` 的数倍以上。

## 11. 数据库维护

当前数据库完整性正常：

```text
PRAGMA integrity_check = ok
```

检测到一定空闲页，但不是当前页面慢的主要原因。

不要在下载任务运行时执行 `VACUUM`。

如需维护，应按以下流程执行：

1. 停止 BiliSync。
2. 备份数据库及 WAL 文件。
3. 确认剩余磁盘空间至少为数据库大小的两倍。
4. 执行：

```sql
VACUUM;
ANALYZE;
PRAGMA optimize;
PRAGMA integrity_check;
```

5. 确认完整性检查返回 `ok` 后再启动服务。

## 12. 发布与回滚流程

### 12.1 发布前

停止服务并备份：

```text
data.sqlite
data.sqlite-wal
data.sqlite-shm
```

或者使用 SQLite 在线备份：

```sql
.backup 'data.sqlite.backup'
```

不要在数据库仍被服务写入时只复制主数据库文件而忽略 WAL 文件。

### 12.2 发布

1. 发布包含新迁移的程序。
2. 启动程序，等待自动迁移完成。
3. 执行 `ANALYZE` 和 `PRAGMA optimize`。
4. 检查 `seaql_migrations`。
5. 检查 `PRAGMA index_list('video')`。
6. 验证执行计划和页面响应时间。

### 12.3 回滚

如果仅需要撤销索引，可执行对应 migration 的 `down`，或回滚程序版本。

索引删除不会删除业务数据。

如果发生数据库迁移异常，停止服务后使用发布前备份恢复数据库，再启动旧版本程序。

## 13. 验收标准

### 数据库层

- `PRAGMA integrity_check` 返回 `ok`。
- 新索引出现在 `PRAGMA index_list('video')`。
- 来源筛选不再执行 `SCAN video`。
- 来源 `COUNT(*)` 使用对应来源索引。

### 接口层

至少测试以下场景：

1. 默认视频列表。
2. 收藏夹筛选。
3. 投稿用户筛选。
4. 无效视频筛选。
5. 等待状态筛选。
6. 标题搜索。
7. 第 1 页和较大页码。
8. 单个视频详情。

### 性能层

建议记录优化前后的：

- HTTP 请求总耗时；
- 数据库 `COUNT(*)` 耗时；
- 分页查询耗时；
- 首屏显示耗时；
- NAS 磁盘 I/O 和 SQLite 锁等待。

## 14. 推荐落地顺序

### 必做

1. 新增四个来源复合索引。
2. 执行 `ANALYZE`。
3. 验证执行计划。
4. 观察生产环境页面耗时。

### 第二阶段

1. 增加 `has_next`。
2. 减少默认列表的精确 `COUNT(*)`。
3. 增加 cursor 分页。

### 后续评估

1. SQLite FTS5 搜索。
2. 状态字段拆分。
3. 大规模历史数据归档。

## 15. 不建议当前执行的操作

- 不要直接删除历史视频数据。
- 不要在服务运行期间执行 `VACUUM`。
- 不要只为 `%关键词%` 搜索创建普通标题索引。
- 不要立即拆分 `download_status`。
- 不要优先改造 `page` 表。
- 不要一次同时上线索引、FTS5、游标分页和状态重构。

