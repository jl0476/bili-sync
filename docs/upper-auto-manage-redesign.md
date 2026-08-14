# UP 自动管理策略重构设计文档

## 1. 背景与目标

`bili-sync` 的 UP 主自动启停功能（`crates/bili_sync/src/task/upper_auto_manage.rs`）上线后收到两类用户反馈：

1. **手动禁用的 UP 永远不会自动恢复**。用户在前端把某个 UP 设为 disabled 后，即使该 UP 之后又发了新投稿，也只能手动重新启用——这与"自动管理"的初衷相悖。
2. **封禁与删号混为一谈**。当前阶段二把"UP 不可用"全部归类为 `Banned` 并写入 `Blacklist`，导致：
   - **删号 / 注销 / 不存在**的 UP 进了黑名单——这部分语义是对的，但 reason 没有区分原因，用户看着一条"账号已封禁"提示无法判断到底是临时封禁还是永久删号。
   - **短期封禁**的 UP 也进了黑名单——B 站账号封禁有"短期封禁"和"永封"两种，前者到期自动解封，但当前实现会让一个可能恢复的账号被永久打入黑名单。

本次重构目标：

- 手动禁用（无策略的普通 UP）→ 巡检阶段二检测到新投稿时自动重新启用。
- 黑名单 → 永不自动恢复（保持现状）。
- UP 删号 / 注销 / 不存在 → 写入黑名单，reason 明确"UP 已删号/不可恢复"。
- UP 封禁 / 冻结 → 进入"封禁观察"状态（不进黑名单、不进恢复候选），reason 明确"封禁观察，待人工判断"；用户可在策略表直接删除以恢复默认巡检。
- 高级策略（白名单 / 黑名单 / 封禁观察）保护：处于这些策略下的 UP，**前端 / 后端拒绝**通过 `update_video_source` 切换 `enabled`——必须先去 `/upper-auto-manage` 调整策略。这样状态机闭环最严谨，避免手动启停与策略语义错位。
- 设置页 UP 自动管理 Tab 修复换行、补说明文案、补"立即执行巡检"按钮（按钮带后端防重）。

非目标：

- 不重构通知系统、不改 summary 之外的配置字段、不做国际化文案改造。
- 不引入新的 `ActionType` 变体（操作历史里仍然只有 auto_disabled / auto_enabled / marked_banned，依靠 reason 前缀区分场景）。
- 不提供"白名单 UP 手动禁用后保留白名单身份并自动恢复"的语义（详见 §3.10）。

## 2. 现状分析

### 2.1 数据模型

`upper_auto_manage_policy` 表（migration `m20260811_000001_upper_auto_manage.rs`）：

| 列 | 类型 | 说明 |
|---|---|---|
| `submission_id` | INTEGER PK | 关联 submission |
| `policy` | TEXT NOT NULL DEFAULT 'normal' | 策略：`normal` / `whitelist` / `blacklist` |
| `source` | TEXT NOT NULL DEFAULT 'auto' | 来源：`auto`（巡检自动写入）/ `manual`（用户手动） |
| `reason` | TEXT NULL | 自由文本说明 |
| `updated_at` | TIMESTAMP | 更新时间 |

后端枚举（`crates/bili_sync_entity/src/entities/upper_auto_manage_policy.rs:17-31`）：

```rust
#[sea_orm(rs_type = "String", db_type = "String(StringLen::N(16))")]
pub enum UpperManagePolicy {
    #[sea_orm(string_value = "normal")]    Normal,
    #[sea_orm(string_value = "whitelist")] Whitelist,
    #[sea_orm(string_value = "blacklist")] Blacklist,
}
```

DB 实际列类型是 `TEXT`（migration `.text().not_null().default("normal")`），所以 enum 加变体**不需要 migration**——只需：
1. entity 加新变体（带 `#[sea_orm(string_value = "...")]`）。
2. 所有模式匹配补全新臂。
3. 后端 `parse_policy`（`crates/bili_sync/src/api/routes/upper_auto_manage.rs:383-390`）加分支。
4. 前端 `types.ts:358` 类型联合加字面量。

### 2.2 巡检三阶段语义（`upper_auto_manage.rs`）

| 阶段 | 函数 | SQL / 行为 | 现有问题 |
|---|---|---|---|
| 一 | `fetch_inactive_candidates` (line 523) | `WHERE s.enabled=1 AND policy NOT IN ('whitelist','blacklist')` → 长期不更新（`days > inactive_threshold_days`）→ 调 `disable_submission_with_policy(Normal, Auto, "N 天未更新")` | Banned 行不在排除列表，会被阶段一当作"长期不更新"覆盖回 Normal+Auto |
| 二 | `fetch_disabled_for_recheck` (line 547) | `WHERE s.enabled=0 AND policy='normal' AND source='auto'` → 调 `Submission::get_videos(1)` 拉最新投稿 → 若 `latest_pubtime > 本地已知最新` 则恢复 | 手动禁用的 UP（`update_video_source` 只改 `enabled`、不写 policy 行）**永远不会进入候选** |
| 二 Banned 分支 | `check_disabled_upper` (line 578) → `CheckOutcomeKind::Banned(msg)` → `upsert_policy(Blacklist, Auto, msg)` | 把"该用户不存在 / 已注销 / 账号已封禁 / 已被冻结"等 8 个关键词混在一起 → 全部转黑名单 | 删号和短期封禁不分；永封/短期都进黑名单 |
| 三 | actions 批量 insert | — | — |

恢复巡检 SQL 的 `policy='normal' AND source='auto'` 是关键约束——它天然保证：黑名单不自动恢复、白名单不进入恢复候选。新增的"封禁观察"只要不进这两个值，就自动满足"不自动启用"语义。

### 2.3 手动禁用的代码路径

唯一的 submission 启停入口是 `PUT /video-sources/{type}/{id}` → `update_video_source`（`crates/bili_sync/src/api/routes/video_sources/mod.rs:202-271`），"submissions" 分支在 line 226-236：

```rust
"submissions" => submission::Entity::find_by_id(id).one(&db).await?.map(|model| {
    let mut active_model: submission::ActiveModel = model.into();
    active_model.path = Set(request.path);
    active_model.enabled = Set(request.enabled);   // ← 只改 enabled
    active_model.rule = Set(request.rule);
    active_model.filter_option = Set(filter_option);
    if let Some(use_dynamic_api) = request.use_dynamic_api {
        active_model.use_dynamic_api = Set(use_dynamic_api);
    }
    _ActiveModel::Submission(active_model)
}),
```

`active_model.save(&db)` 之前没有任何 `upper_auto_manage_policy::Entity` 访问——所以**手动禁用 = 完全不写 policy 行**，恢复巡检自然不会命中。

### 2.4 错误判定（`crates/bili_sync/src/bilibili/error.rs:43`）

```rust
pub fn is_upper_unavailable(&self) -> bool {
    if let BiliError::ErrorResponse { message, .. } = self && let Some(msg) = message {
        const KEYWORDS: &[&str] = &[
            "该用户不存在", "用户不存在",
            "账号已封禁", "账号被封禁", "已注销",
            "已被冻结", "已被封禁", "空间已封禁",
        ];
        return KEYWORDS.iter().any(|kw| msg.contains(kw));
    }
    false
}
```

8 个关键词在语义上其实是**两类**：
- **不可恢复类**：`该用户不存在` / `用户不存在` / `已注销` → 账号已不存在，巡检无法恢复，只能黑名单。
- **可能恢复类**：`账号已封禁` / `账号被封禁` / `已被冻结` / `已被封禁` / `空间已封禁` → 可能是短期封禁，到期自动解封。

### 2.5 前端现状

`web/src/routes/settings/+page.svelte`：
- line 278 `Tabs.List class="grid w-full grid-cols-6"`，但实际有 7 个 Tab → 第 7 个（UP 自动管理）被挤到第二行。
- UP 自动管理 Tab（line 828-871）只有 4 个配置项，没有"立即执行巡检"入口；`api.triggerUpperAutoManageRun()` 后端已暴露，但 UI 只在 `/upper-auto-manage` 页用。

`web/src/routes/upper-auto-manage/+page.svelte`：
- `policyLabel` / `policyVariant`（line 87-95）只覆盖 normal/whitelist/blacklist。
- 策略表格（L580-643）显示策略 + 来源 + reason，操作列只有 select 改策略 + 删除按钮。
- 「最近一次统计」栏（L331-346）是 3 列：禁用 / 启用 / 转黑名单。
- 删除策略 `reset_policy_after_delete`（`upper_auto_manage.rs:728`）match 只有 Blacklist / Whitelist / Normal 三臂。

## 3. 设计决策

### 3.1 数据模型：给 `UpperManagePolicy` 加新变体 `Banned`

**决策**：直接给 `UpperManagePolicy` 加新变体 `Banned`（`string_value = "banned"`），不复用现有 `Banned(String)` 命名。

**为什么这样选**：

| 候选方案 | 评价 |
|---|---|
| A. 给枚举加 `Banned` 变体（**采用**） | DB 零改动；与现有 `policy/source/reason` 三元组正交；保留 source 维度可区分 Auto/Manual；reason 自由文本表达语义 |
| B. 引入独立状态字段 | 增加 schema 复杂度，需 migration；前端 / 后端多处需要 join 两套字段 |
| C. 在 reason 里靠关键词区分 | 脆弱；UI 无法做"只看封禁观察"的筛选；操作历史里 reason 字段会污染业务判定 |

**为什么 Banned 这个名字而不是 Suspended / BannedObservation**：
- 前端用户能直接读懂「封禁观察」四个字，跨团队沟通零成本。
- 数据库存储 `banned` 字符串足够短，与 `normal/whitelist/blacklist` 风格一致。
- 通过 reason 区分"封禁观察"和"删号"两种触发场景，reason 内容不参与代码逻辑判断。

### 3.2 错误关键词拆分

新增两个独立函数：

```rust
pub fn is_upper_permanently_gone(&self) -> bool {
    // "该用户不存在" / "用户不存在" / "已注销"
}

pub fn is_upper_banned(&self) -> bool {
    // "账号已封禁" / "账号被封禁" / "已被冻结" / "已被封禁" / "空间已封禁"
}
```

`is_upper_unavailable()` 保留（向后兼容），等于以上两者的并集。三个函数共享单元测试。

**为什么不靠 B 站 error code（如 -2xx）区分短期 vs 永封**：
- 当前实现注释明确写了"不能用 code 判定：`-404` 既表示用户不存在也表示视频不存在"。
- 实测中封禁消息字段文本是唯一可靠信号；如果某天 B 站接口新增 code（如 -210/-220），可再迭代，本期不做。

### 3.3 手动启停的事务与策略联动

**核心修正**：手动启停必须原子化，高级策略保护拒绝修改 `enabled`，且 API 与后台巡检共享同一套并发保护（SQLite 不支持 `SELECT … FOR UPDATE`）。

#### Normal + Manual 状态的处理

`PUT /upper-auto-manage/policies/{id}` 对任何手动改动都写 `source=Manual`，包括用户显式选 "正常"。因此 `Normal+Manual` 是**正常可达状态**，不能当作"不可能状态"忽略。

规则：**把 `Normal+Manual` 与 `Normal+Auto` 统一视为"普通可恢复态"**——手动禁用时一律改写为 `Normal+Auto`（覆盖 source），让它进入阶段二恢复巡检候选；手动重新启用时删除该行。

> 副带改动：策略页 `upsert_policy` 对手动创建 `Normal` 的语义不再特殊——它只是"用户显式重置策略标记"，与自动禁用写的 `Normal+Auto` 在恢复巡检视角下等价。前端策略表 `changePolicy` 选 normal 的入口保留（用户可用它把 Blacklist/Banned 改回 normal 以重新纳入巡检）。

#### 并发控制（SQLite 可落地）

SQLite 不支持 `SELECT … FOR UPDATE`，WAL 模式下 `db.begin()` 提供写事务串行（写锁互斥），但不能自动保证"读旧状态→决策→更新"的串行语义。而且后台巡检任务（阶段一/二会直接 `UPDATE submission.enabled` 和 `upsert_policy`）与 API 更新必须共享同一保护，否则两个 API 请求之间串行了，仍可能与巡检任务并发覆盖。

**采用方案：应用级 per-submission 串行 + 事务内重读**。

新增 `DashMap<i32, Arc<Mutex<()>>>` 作为提交源级锁池（`crates/bili_sync/src/task/upper_auto_manage.rs` 导出 `pub fn lock_for_submission(id) -> Arc<Mutex<()>>`）：

- `update_video_source` 的 "submissions" 分支、巡检阶段一/二对单个 submission 的所有读写，**都先 `lock_for_submission(id).lock().await` 再操作**。
- 拿到锁后，在 `db.begin()` 事务内**重新读取** `submission` 与 `upper_auto_manage_policy` 当前行（不依赖入参里的旧状态），用最新值决策。
- SQLite 写锁由 `db.begin()` 兜底（即使锁池漏了，DB 层仍串行），per-submission Mutex 只是减少跨 UP 误锁、保证读改写的应用层原子性。

这样三个并发源（两个 API 请求 + 一个巡检任务）对同一 submission 永远串行。

#### 事务化流程（修正版）

```
1. guard = lock_for_submission(id).lock().await   // 应用级 per-submission 锁
2. txn = db.begin()                                // SQLite 写事务
3. SELECT submission WHERE id=?                    // 事务内重读最新行
4. SELECT upper_auto_manage_policy WHERE submission_id=?
5. 根据 (prior_enabled, request.enabled, prior_policy, prior_source) 决策：
   - 高级策略保护：prior_policy ∈ {Whitelist, Blacklist, Banned} 且
     request.enabled ≠ prior_enabled → 返回 409 Conflict
       "该 UP 受 <策略名> 保护，请先在 UP 自动管理页面调整策略"
   - 普通可恢复态（policy=None 或 Normal(任意source)）：
     * prior_enabled=true && request.enabled=false
       → submission.update(enabled=false) + upsert_policy(Normal, Auto, "用户手动禁用")
     * prior_enabled=false && request.enabled=true
       → submission.update(enabled=true) + delete_policy_by_id(submission_id)
     * enabled 不变 → 仅更新 path/rule/filter_option 等其他字段，不动 enabled/policy
6. txn.commit()
7. guard 随函数返回 drop
```

**关键点**：
- per-submission 锁覆盖 API 与巡检任务，避免三者并发覆盖。
- 事务内重读 prior 状态作为决策依据。
- `submission.update` 与 `policy.upsert/delete` 在同一事务，**任一失败整体回滚**——杜绝"已禁用但 policy 行没写成功"的孤立状态。
- `delete_policy_by_id` 新增工具函数：

```rust
// crates/bili_sync/src/task/upper_auto_manage.rs
pub async fn delete_policy_by_id(
    connection: &DatabaseConnection,
    submission_id: i32,
) -> Result<()> {
    upper_auto_manage_policy::Entity::delete_by_id(submission_id)
        .exec(connection)
        .await?;
    Ok(())
}
```

#### 状态机对照表（修正版）

| prior_policy | prior_source | prior_enabled | request.enabled | 行为 |
|---|---|---|---|---|
| None | — | true | false | ✅ upsert `Normal+Auto` |
| None | — | false | true | ✅ delete policy 行（若存在） |
| None | — | X | X | 无操作（仅更新其他字段） |
| Normal | Auto/Manual | true | false | ✅ 改写为 `Normal+Auto`（覆盖 source）|
| Normal | Auto/Manual | false | true | ✅ delete policy 行 |
| Normal | Auto/Manual | X | X | 无操作 |
| Whitelist / Blacklist / Banned | * | X | ≠ X | ❌ **409 拒绝**：受策略保护 |
| Whitelist / Blacklist / Banned | * | X | X | 无操作（仅更新其他字段，enabled 不动）|

#### 后台巡检共享同一锁池

阶段一 `disable_submission_with_policy` 与阶段二 `enable_submission_with_policy` / `upsert_policy(Blacklist/Banned)` 在写单个 submission 前，也必须 `lock_for_submission(id).lock().await`，确保与 API 串行。`try_for_each_concurrent` 并发的是**不同 submission**，同一 submission 在一轮巡检内只会被处理一次，锁不会自死锁。

#### 策略页 API 同样进锁（关键）

`PUT /upper-auto-manage/policies/{id}`（`upsert_policy`）和 `DELETE /upper-auto-manage/policies/{id}`（→`reset_policy_after_delete`）也直接读写同一条 policy 行，**必须进入同一把 `lock_for_submission(id)`**。否则仍有覆盖窗口：巡检刚判定"已恢复"准备写 `Normal+Auto` 时，用户同时改成黑名单，写入顺序不确定，用户的高级策略可能被后台巡检覆盖。

锁覆盖清单（所有按 submission_id 写 policy 的入口）：

| 入口 | 函数 | 锁要求 |
|---|---|---|
| 视频来源启停 | `update_video_source`（submissions 分支） | `lock_for_submission(id)` + 事务 |
| 策略页设置 | `upsert_policy` | `lock_for_submission(id)` + 事务 |
| 策略页删除 | `delete_policy` → `reset_policy_after_delete` | `lock_for_submission(id)` + 事务 |
| 巡检阶段一禁用 | `disable_submission_with_policy` | `lock_for_submission(id)` + 事务 |
| 巡检阶段二恢复/封禁 | `enable_submission_with_policy` / `upsert_policy(Banned/Blacklist)` | `lock_for_submission(id)` + 事务 |

对 `reset_policy_after_delete` 这种"先读旧策略再决定后续（Blacklist→Normal+Auto / Whitelist/Normal→删行）"的操作，必须在**锁内 + 同一事务内**完成"读旧→决策→写"，不能拆成两次查询。

`reset_policy_after_delete` 签名调整，支持接收已开启的事务句柄（或在外层 `update_video_source`/`delete_policy` 内 `db.begin()` 后把 `&txn` 传入），保证整条链路原子。`task/mod.rs` re-export `lock_for_submission` 供 API 层调用。

#### 前端错误处理

`web/src/routes/video-sources/+page.svelte` 编辑对话框收到 409 时 toast：

```
该 UP 受「黑名单/白名单/封禁观察」保护，请前往「UP 自动管理」页面调整策略
```

### 3.4 阶段二 outcome 拆分

`CheckOutcomeKind` 重构：

```rust
enum CheckOutcomeKind {
    Recovered(DateTime),
    StillInactive,
    Gone(String),                    // 删号/不可恢复 → 写 Blacklist
    BannedObservation(String),       // 封禁/冻结 → 写 Banned，不动 enabled
}
```

理由：
- 删除原 `Banned(String)` 避免与新 enum `UpperManagePolicy::Banned` 同名混淆。
- `Gone` 语义直白，强调"账号已不可逆地不存在"。

阶段二 match 行为表：

| Outcome | 写 policy | enabled | reason | action | stats 字段 |
|---|---|---|---|---|---|
| `Recovered(pubtime)` | `Normal+Auto`（覆盖现有） | false→true | "检测到新投稿，时间 {pubtime}" | `AutoEnabled` | `enabled` +1 |
| `StillInactive` | 不写 | false→false | — | — | `still_inactive` +1 |
| `Gone(msg)` | `Blacklist+Auto`（覆盖） | 保持 false | "UP 已删号/不可恢复：{msg}" | `MarkedBanned` | `banned` +1 |
| `BannedObservation(msg)` | `Banned+Auto`（覆盖） | 保持 false | "封禁观察，待人工判断：{msg}" | `MarkedBanned` | `banned_observation` +1 |

### 3.5 阶段一排除 SQL 扩展

`fetch_inactive_candidates` 把 `IN ('whitelist', 'blacklist')` 改为 `IN ('whitelist', 'blacklist', 'banned')`：

- **必要性**：如果 Banned UP 超过 inactive_threshold_days，阶段一会按"长期不更新"逻辑写 `Normal+Auto`，把 Banned 覆盖回 Normal+Auto，破坏"封禁观察"语义。
- **影响**：零——现有数据没有任何 banned 行，行为对历史数据等价。

恢复巡检 SQL（`fetch_disabled_for_recheck`）**不改**：本来就 `policy='normal' AND source='auto'`，Banned 自动不被命中。

### 3.6 删除策略语义

`reset_policy_after_delete` 加 `Banned` 臂，沿用 Blacklist 同语义（改写为 `Normal+Auto` 并保留当前 enabled 状态）：

```rust
UpperManagePolicy::Banned => {
    let txn = connection.begin().await?;
    upsert_policy_txn(&txn, submission_id,
        UpperManagePolicy::Normal, UpperManageSource::Auto,
        Some("用户清除封禁观察，允许自动恢复".to_string())).await?;
    txn.commit().await?;
}
```

理由：与 Blacklist 保持一致——删 = 允许自动恢复，符合用户「删除策略 = 恢复默认巡检」的直觉。如果用户希望先解除观察再单独 enable，可以分两步：先删 Banned 解除观察（enabled 仍 false），再去 `/video-sources` 编辑对话框手动启用。

### 3.7 RunStats 扩展

新增 `banned_observation: i32` 字段；`RunDto`（API 层）同步加 `banned_observation_count` 字段；前端 `UpperAutoManageRun.bannedObservationCount` 对应；统计栏从 3 列变 4 列。

#### 统计口径重构（m20260814_000001）

原口径的问题：`checked` 混合启用态候选与禁用态复查两个总体；`skipped` 混合「本地无视频无法判定」与「复查仍不活跃」两种语义；阶段一中「近期有更新」的正常 UP 计入 checked 但不进任何桶，数字不闭合。

重构后各桶定义域互斥，成功轮次满足 `checked = disabled + active + indeterminate + enabled + banned + banned_observation + still_inactive`：

| 桶 | 阶段 | 定义 |
|---|---|---|
| `checked` | 合计 | 阶段一候选 + 阶段二候选 |
| `disabled` | 一（启用态） | 超过阈值未更新 → 自动禁用 |
| `active` | 一（启用态） | 近期有更新，无需动作（新增列） |
| `indeterminate` | 一（启用态） | 本地无任何视频，无法判定（新增列） |
| `enabled` | 二（禁用态复查） | 恢复更新 → 重新启用 |
| `still_inactive` | 二（禁用态复查） | 无新投稿，维持禁用（原 `skipped_count` 重命名） |
| `banned` | 二 | 删号/注销 → 转黑名单 |
| `banned_observation` | 二 | 封禁/冻结 → 观察 |

summary 字符串统一为：

```
巡检完成：检查 {checked} 个 UP（启用态 {p1}：禁用 {disabled}、正常 {active}、无法判定 {indeterminate}；禁用态复查 {p2}：恢复启用 {enabled}、仍不活跃 {still_inactive}、转黑名单 {banned}、封禁观察 {banned_observation}）
```

迁移：`upper_auto_manage_run` 新增 `active_count`、`indeterminate_count`，`skipped_count` 重命名为 `still_inactive_count`（历史行的混合语义数据整体归入仍不活跃，接受误差）。`RunDto` 与前端 `UpperAutoManageRun` 同步加 `activeCount`、`indeterminateCount`、`stillInactiveCount`；统计栏从 4 格变 8 格（两行）。

### 3.7.1 巡检任务后端防重（修正版）

问题：现有 `cx.running` Mutex 只防止"并发执行"，但 one-shot 任务的"已排队但尚未开始执行"窗口内仍可加入多个任务；且 `sched.add()` 是异步登记、登记完闭包立即返回、锁立即释放，真正巡检稍后才跑。

**核心要求**：手动 one-shot 与定时任务必须**复用同一个共享执行函数**，不能让手动路径绕开现有 `run_inspection_task` 的状态收尾（更新 `is_running`、记录 `last_run/last_finish/next_run`、统一错误通知）。否则前端"等 WebSocket `is_running=false` 后解锁"不成立，手动任务状态会不一致。

#### 方案：单一 `run_slot` 状态 + 共享执行函数 `execute_inspection`

提取共享执行函数，把"占用 slot 后的所有事"收敛到一处：

```rust
// crates/bili_sync/src/task/upper_auto_manage.rs
impl TaskContext {
    /// 共享执行：假定调用方已占用 run_slot。执行巡检、更新状态、finally 释放 slot。
    /// 由定时任务闭包与手动 one-shot 闭包共同调用。
    /// scheduler 由调用方传入（one-shot 与定时任务都持有调度器句柄）。
    async fn execute_inspection(
        self: &Arc<Self>,
        job_uuid: uuid::Uuid,      // 当前触发的 job id（one-shot 自身 / 定时任务）
        sched: JobScheduler,       // 用于查询定时任务 next tick（手动路径也要传）
    ) {
        // 1. is_running = true（沿用现有 status_tx 推送）
        let _ = self.status_tx.send(TaskStatus {
            is_running: true,
            last_run: Some(chrono::Local::now()),
            last_finish: None,
            next_run: None,
        });
        info!("开始执行本轮 UP 主自动巡检任务..");
        // 2. 执行巡检（含错误通知，沿用 run_inspection_task 逻辑）
        let config = VersionedConfig::get().snapshot();
        match run_inspection(&self.connection, &self.bili_client, &config).await {
            Ok(stats) => info!("...完成..."),
            Err(e) => error_and_notify(&config, &self.bili_client, format!("...{:#}", e), &e),
        }
        // 3. 刷新 next_run：从 cx.job_id 取定时任务的 uuid 查询 next tick
        //    （手动 one-shot 也要查定时任务的下一次运行，不能清成 None，
        //     否则 UI 会暂时错误显示"无下次巡检"）
        let task_uuid = (*self.job_id.lock().await).unwrap_or(job_uuid);
        let next_run = sched
            .next_tick_for_job(task_uuid)
            .await
            .ok()
            .flatten()
            .map(|dt| dt.with_timezone(&chrono::Local));
        // 4. is_running = false + last_finish（沿用现有 status 收尾）
        let last_status = *self.status_rx.borrow();
        let _ = self.status_tx.send(TaskStatus {
            is_running: false,
            last_run: last_status.last_run,
            last_finish: Some(chrono::Local::now()),
            next_run,
        });
        // 5. finally：释放 slot（无论成功失败）
        let mut slot = self.run_slot.lock().await;
        *slot = false;
    }
}
```

#### 定时任务闭包（改造 `run_inspection_task`）

```rust
move |uuid, mut l| {                   // 与现有代码一致的 (uuid, scheduler) 两参数签名
    let cx = cx.clone();
    Box::pin(async move {
        // 占用 slot：与手动触发互斥
        let mut slot = cx.run_slot.lock().await;
        if *slot {
            warn!("已有巡检任务排队/执行中，跳过本次定时触发");
            return;
        }
        *slot = true;
        drop(slot);
        // 复用共享执行，传入 scheduler 以查询定时任务 next tick
        cx.execute_inspection(uuid, l).await;
    })
}
```

#### 手动 `run_once`

```rust
pub async fn run_once(&self) -> Result<bool> {
    let mut slot = self.cx.run_slot.lock().await;
    if *slot {
        return Ok(false);  // 已排队或执行中
    }
    *slot = true;
    drop(slot);
    let cx = self.cx.clone();
    let register_result = self.sched.lock().await
        .add(Job::new_one_shot_async(
            Duration::from_secs(0),
            move |uuid, l| {            // (uuid, scheduler) 两参数，与现有签名一致
                let cx = cx.clone();
                Box::pin(async move {
                    // 复用共享执行：one-shot 也传入 scheduler，
                    // execute_inspection 内部通过 cx.job_id 查询定时任务 next tick，
                    // 不会把手动脉冲后的 next_run 清成 None
                    cx.execute_inspection(uuid, l).await;
                })
            },
        )?)
        .await;
    if let Err(e) = register_result {
        // 登记失败必须立即释放 slot，并把错误返回 API
        let mut slot = self.cx.run_slot.lock().await;
        *slot = false;
        return Err(e.into());
    }
    Ok(true)
}
```

#### 关键修正点

1. `execute_inspection` 是**唯一**的状态收尾路径，定时与手动都走它——`is_running`、`last_run/last_finish`、`next_run`、错误通知、slot 释放全部在此完成，不存在"手动路径漏更新状态"。
2. one-shot 闭包签名是 `move |uuid, l|`（两参数，与现有 `run_inspection_task` 一致），不是单参数；手动路径同样传入 `JobScheduler`，`execute_inspection` 通过 `cx.job_id` 查询**定时任务**的 next tick，**不会**把 next_run 清成 None——避免手动脉冲后 UI 暂时显示"无下次巡检"。
3. `run_slot` 是 `Mutex<bool>`（不是 `Mutex<()>` + guard）——锁本身会释放，状态必须独立保存。
4. 占用发生在入口（`run_once` 或定时闭包）；释放发生在 `execute_inspection` 的 finally。
5. `sched.add()` 失败立即释放 slot 并把 `Err` 返回 API（不吞错误）。
6. 现有 `cx.running: Mutex<()>` 可被 `run_slot` 取代（或保留作为双重保险，但主逻辑迁移到 `run_slot`）。

#### 前端按钮

```ts
async function triggerManualRun() {
    if (manualTriggering) return;
    manualTriggering = true;
    try {
        const res = await api.triggerUpperAutoManageRun();
        if (!res.data) {
            toast.warning('已有巡检任务进行中');
            // 不立即解锁，等 WebSocket 推送 is_running=false
        } else {
            toast.success('已触发巡检');
        }
    } catch (e) {
        toast.error('触发失败', { description: (e as ApiError).message });
    } finally {
        manualTriggering = false;  // 本地兜底，真正解锁依赖 WS
    }
}
```

### 3.8 设置页 UI

#### grid 修复
`grid-cols-6` → `grid-cols-7`，第 7 个 Tab 自然在同一行。

#### 顶部说明卡片
放在 `Tabs.Root` 上方（一行说明 + 跳转链接 + 三种策略语义简介），不放在每个 Tab 内：

> 「UP 自动管理 Tab 控制自动启停巡检的启用与频率；具体策略（白名单 / 黑名单 / 封禁观察）请前往 [UP 自动管理页面] 配置。巡检会自动恢复手动禁用的 UP（在检测到新投稿时）；删号 UP 会自动加黑名单；封禁 UP 会进入观察状态，不会自动启用也不会自动加黑名单。」

#### 立即执行巡检按钮
放在 UP 自动管理 Tab 末尾，与其他 Tab 内"操作"型控件保持视觉一致（Label + 说明 + 右侧按钮）：

```svelte
<Separator />
<div class="flex items-center justify-between rounded-lg border p-4">
    <div class="space-y-1">
        <Label>手动触发巡检</Label>
        <p class="text-muted-foreground text-xs">立刻执行一次 UP 主自动启停巡检，无需等待定时器</p>
    </div>
    <Button onclick={triggerManualRun} disabled={manualTriggering}>
        <PlayIcon class="mr-2 h-4 w-4" />立即执行巡检
    </Button>
</div>
```

`triggerManualRun` 函数与 `/upper-auto-manage` 页 `triggerRun` 逻辑一致，但**不共享状态**——保持页面内聚，避免跨页状态污染。

#### 图标 import
新增 `import PlayIcon from '@lucide/svelte/icons/play';`。

### 3.9 策略表格与 UI 适配

| UI 元素 | 改动 |
|---|---|
| `policyLabel` | 加 `'banned': '封禁观察'` |
| `policyVariant` | 加 `'banned': 'outline'`（区别于 destructive 黑名单） |
| 策略筛选下拉 | 加 `<option value="banned">封禁观察</option>` |
| 策略修改下拉 | 同上 |
| 「最近一次统计」栏 | `grid-cols-3` → `grid-cols-4`，新增"封禁观察"单元格 |
| 说明文案 | 三种策略并列说明 |
| 删除确认弹窗 | Banned 时显示「清除后该 UP 将重新由巡检系统评估是否恢复/禁用」 |
| 空态文案 | 补充「或在巡检中被识别为封禁/冻结的 UP」 |

### 3.10 关于"白名单 UP 手动禁用后保留白名单身份并自动恢复"

**结论**：本期不支持此语义，采用"高级策略保护直接拒绝修改 enabled"方案。

**为什么不可行**：`upper_auto_manage_policy` 的主键是 `submission_id`（migration line 22-29），一行就是一个 UP，**不能同时存在 `whitelist` 和 `Normal+Auto` 两行策略**。要保留白名单身份且可自动恢复，必须引入新状态字段（如 `auto_recover: bool`），引入 schema 复杂度。

**采用的语义**：白名单 / 黑名单 / 封禁观察状态下的 UP，**前端 / 后端拒绝**通过 `update_video_source` 切换 `enabled`，要求用户去 `/upper-auto-manage` 调整策略。这与状态机闭环最严谨的取舍一致——高级策略的修改必须显式经过策略表，不能从视频来源页绕路。

**用户视角的体验**：白名单 UP 想暂时停扫，直接去 `/upper-auto-manage` 把策略改成 `Normal` 或加 Banned；想恢复白名单，再改回来。视频来源页只对**无策略**的普通 UP 提供启用/禁用，且这些操作会联动到自动恢复。

## 4. 关键文件清单

### 后端
- `crates/bili_sync_entity/src/entities/upper_auto_manage_policy.rs` — 加 `Banned` 变体
- `crates/bili_sync_entity/src/entities/upper_auto_manage_action.rs` — 不改
- `crates/bili_sync_entity/src/entities/upper_auto_manage_run.rs` — `Model` 加 `banned_observation_count: i32` 字段
- `crates/bili_sync_migration/src/m202608XX_000002_add_banned_observation_count.rs` — **新增 migration**：在 `upper_auto_manage_run` 表 ADD COLUMN `banned_observation_count INTEGER NOT NULL DEFAULT 0`；提供 up/down
- `crates/bili_sync_migration/src/lib.rs` — 注册新 migration
- `crates/bili_sync/src/bilibili/error.rs` — 拆 `is_upper_permanently_gone` / `is_upper_banned`
- `crates/bili_sync/src/task/upper_auto_manage.rs` — `CheckOutcomeKind` 拆分；阶段一 SQL；reset match；新增 `delete_policy_by_id`；新增 per-submission 锁池 `lock_for_submission(id)`（供 API + 巡检共用）；阶段一/二写操作加锁；`reset_policy_after_delete` 改为接收事务句柄；提取 `TaskContext::execute_inspection` 共享执行函数（状态收尾 + slot 释放）；`run_inspection_task` 与 `run_once` 均复用 `execute_inspection`；`run_slot: Mutex<bool>` 取代/补充 `cx.running`
- `crates/bili_sync/src/task/mod.rs` — re-export `delete_policy_by_id` 与 `lock_for_submission`
- `crates/bili_sync/src/api/error.rs` — 新增 `InnerApiError::PolicyProtected(String)`
- `crates/bili_sync/src/api/wrapper.rs` — `ApiError::into_response` 的 match 加 `PolicyProtected => 409 Conflict` 分支；`ApiResponse` 新增 `conflict()` 构造器
- `crates/bili_sync/src/api/routes/upper_auto_manage.rs` — `RunDto` 扩字段；`parse_policy` 加 banned；`trigger_run` 返回 `bool`（沿用 `run_once` 结果）；**`upsert_policy` 与 `delete_policy` 进 `lock_for_submission(id)` + 事务**
- `crates/bili_sync/src/api/routes/video_sources/mod.rs` — `update_video_source` 事务化（`lock_for_submission` + db.begin + 事务内重读 + 高级策略拒绝 409 + 联动策略写入/清理）

### 前端
- `web/src/lib/types.ts` — `UpperManagePolicy` + `UpperAutoManageRun` 类型扩展
- `web/src/routes/settings/+page.svelte` — grid + 说明 + 按钮（按钮按防重逻辑处理 false 响应）
- `web/src/routes/upper-auto-manage/+page.svelte` — 映射函数 + select 选项 + 统计栏 + 文案
- `web/src/routes/video-sources/+page.svelte` — 编辑对话框收到 409 时显示策略保护 toast

### 数据库
**必须**新增 migration（阻断）：`upper_auto_manage_run` 表加 `banned_observation_count` 列。其他 enum 变体不需要 migration。

## 5. 测试策略

### 单元测试
- `error.rs::tests` — 三个函数（unavailable / permanently_gone / banned）关键词矩阵断言
- `upper_auto_manage.rs::tests` —
  - `fetch_disabled_for_recheck_excludes_whitelist_blacklist` 扩展加 Banned 候选
  - `fetch_inactive_candidates_skips_disabled_and_whitelisted` 扩展加 Banned 启用态候选
  - `phase2_outcomes_apply_correct_state_changes` 拆分为 `phase2_gone_outcome_writes_blacklist` 和 `phase2_banned_observation_outcome_writes_banned`
  - 新增 `reset_policy_after_delete_rewrites_banned_to_normal_auto`
  - 新增 `run_once_returns_false_when_slot_taken`（防重，slot 已占用时返 false）
  - 新增 `run_once_releases_slot_on_register_failure`（`sched.add` 失败后 slot 回 Idle）
  - 新增 `run_once_releases_slot_after_inspection_finishes`（巡检完成后 slot 回 Idle）
  - 新增 `scheduled_job_skips_when_slot_taken`（定时任务在 slot 占用时跳过）
  - 新增 `manual_and_scheduled_share_status_finalize`（手动 one-shot 与定时任务都更新 `is_running`/`last_run`/`last_finish`，状态收尾一致）
- `video_sources/mod.rs::tests`（新增模块）：
  - `update_video_source_disables_normal_submission_writes_normal_auto_policy`（启用→禁用，事务内一次性）
  - `update_video_source_disables_normal_manual_submission_rewrites_to_auto`（Normal+Manual → 禁用 → 改写为 Normal+Auto）
  - `update_video_source_enables_normal_auto_submission_deletes_policy`（禁用→启用，清理）
  - `update_video_source_rejects_enabled_change_under_high_level_policy`（whitelist/blacklist/banned 三种状态下切换 enabled 都返 409）
  - `update_video_source_no_op_when_enabled_unchanged`（边界）
  - `update_video_source_concurrent_requests_serialized_per_submission`（两并发请求串行，无脏写）
  - `update_video_source_rolls_back_on_policy_write_failure`（策略写入失败时 submission 也回滚）
- `api/routes/upper_auto_manage.rs::tests`（新增模块）—
  - `upsert_policy_serializes_with_inspection`（巡检阶段二与 `upsert_policy` 并发时，用户写黑名单不会被巡检的 Normal+Auto 覆盖）
  - `delete_policy_reset_under_lock`（删除策略的"读旧→决策→写"在锁内原子完成）
- `api/wrapper.rs::tests`（新增）—
  - `policy_protected_error_maps_to_409_conflict`：构造 `ApiError::from(InnerApiError::PolicyProtected(..))`，断言 `IntoResponse` 输出 HTTP status 409（而非 500）

### 集成测试（数据库层）
- `tests/upper_auto_manage_migration.rs`（新增）—
  - `migration_adds_banned_observation_count_with_default_zero`：跑 migration，断言新列存在且旧 run 行的 `banned_observation_count=0`
  - `rollback_migration_drops_banned_observation_count`：回滚后再升级无异常
- `tests/inspection_concurrency.rs`（新增）—
  - `inspection_and_api_update_share_submission_lock`：手动触发巡检的同时并发 `update_video_source`，断言两者对同一 submission 串行、无中间状态

### 端到端验证脚本
- 启动 dev 服务（`just debug` 或 `cargo run` + `bun run dev`）
- 用真实 B 站账号登录 + 订阅 1-2 个 UP（含已知停更 UP）
- 测试路径：
  1. 设置页 7 Tab 单行展示。
  2. 设置页改完阈值 → 点"立即执行巡检"→ toast 成功。
  3. **快速连点"立即执行巡检"按钮 5 次**：toast 显示"已有巡检任务进行中"，后续按钮 loading，等 WebSocket `is_running=false` 后解锁。
  4. 手动禁用某个无策略的 UP → `/upper-auto-manage` 出现 Normal+Auto 策略行。
  5. 模拟新投稿（手动改 DB 或等待下次巡检）→ 该 UP 自动重新启用，policy 行被清除。
  6. 把同一 UP 设为白名单 → 视频来源页尝试禁用 → **HTTP 409** + toast 报错且状态未变。
  7. 在策略页把某 UP 手动选"正常"（产生 Normal+Manual）→ 视频来源页禁用 → 改写为 Normal+Auto，可恢复。
  8. 制造"该用户不存在"（mock 或删号 UP）→ 策略列表出现 Blacklist+Auto，reason 含「删号」。
  9. 制造"该账号已封禁" → 策略列表出现 Banned+Auto，reason 含「封禁观察」。
  10. 删除 Banned 策略 → 该 UP 回到正常巡检候选。

## 6. 风险与缓解

| 风险 | 等级 | 缓解 |
|---|---|---|
| 手动禁用 → 自动恢复误启用 | 低 | 仅在「prior_enabled=true && request.enabled=false 且 prior_policy ∈ {None, Normal}」时写/改写 Normal+Auto；用户手动重新启用时删除该行；事务内重读 prior 状态 |
| 阶段一 SQL 改字面量影响历史 | 无 | 线上零 banned 数据，行为对历史数据等价 |
| `update_video_source` 多一次 policy 查询 + 锁开销 | 低 | `submission_id` 是 PK 索引；per-submission 锁仅影响同一 UP 的并发，不同 UP 不互斥 |
| 通知 summary 字符串变化 | 低 | 旧版"转黑名单 N"，新版"转黑名单 N，封禁观察 M"——兼容现有通知模板 |
| 前端 TS 类型扩展未覆盖所有 union | 低 | `UpperManagePolicy` 是 union，新增 'banned' 后所有 switch / map / select 都已显式列出，TS 编译器会强制提醒遗漏 |
| 黑名单 / Banned 命名混淆 | 中 | 通过 `policyLabel`/`policyVariant` 中文映射「黑名单」/「封禁观察」与 reason 前缀「删号」/「封禁观察」彻底区分 |
| 连点"立即执行巡检"导致多次排队 | 中 | `run_slot` 单一状态机：占用在 `run_once` 入口，释放在 `execute_inspection` 的 finally；定时任务共享 slot；`sched.add` 失败立即释放并返回错误 |
| 手动触发巡检状态不一致 | 中 | 定时与手动 one-shot 共用 `execute_inspection`，状态收尾（`is_running`/`last_run`/`last_finish`/`next_run`/错误通知）统一在此完成，不绕开 |
| migration 升级失败导致旧库不可用 | 低 | 新列带 `DEFAULT 0`，对历史 run 数据零侵入；提供 down migration |
| 高级策略与手动启停语义错位 | 中 | 后端 409 拒绝（`wrapper.rs` 映射）+ 前端 toast 引导去策略表修改；状态机闭环最严谨 |
| `Normal+Manual` 被误判为不可达 | 中 | 明确规则化为"普通可恢复态"，手动禁用改写为 Normal+Auto；单测覆盖该路径 |
| 巡检与 API / 策略页并发覆盖策略 | 中 | per-submission 锁池 `lock_for_submission` 覆盖**全部 5 个**写 policy 入口（`update_video_source`、`upsert_policy`、`delete_policy`/`reset_policy_after_delete`、阶段一、阶段二）；`reset_policy_after_delete` 接收事务句柄保证"读旧→决策→写"原子 |
| `PolicyProtected` 误落 500 | 中 | `wrapper.rs::into_response` 显式 match 新增分支返 409；单测断言 HTTP status |

## 7. 后续可扩展点（本期不做）

- `ActionType` 加 `MarkedBanObservation` 变体，让操作明细可按 action 筛选封禁/删号。
- 通知模板支持按 reason 类型路由不同 channel。
- 阶段二结果可让用户配置"自动解封观察"天数（如果有更明确的信号）。
- 候选策略表加"自动启用最近 N 天的封禁观察"功能。