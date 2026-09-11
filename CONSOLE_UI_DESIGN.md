# 迁移控制台 UI 设计逻辑（CONSOLE_UI_DESIGN.md）

> 本文件是 Web 控制台（`-mode=console`，`pkg/console/ui/console.html`）交互设计的**唯一事实来源**。
> 目的：把用户反复纠正过的行为固化成契约，避免再被改回旧的、错误的样子。
> **任何改动 console 的 UI 或迁移进度上报前，先读本文件，改完对照本文件自检。**
>
> 三个阶段与后端映射：
> - **评估/检测** → `pkg/assess`、`pkg/remediate`、`/api/assess`、`/api/remediate`
> - **迁移/实时进度** → `pkg/migration`、`pkg/metrics`（registry + control）、`launch()`
> - **验证/校验** → `pkg/verify`、`/api/verify`

---

## 0. 铁律（最容易被改错的三条，先看这里）

1. **检测这一步绝不修改源库。** 只做三件事：① 列出有问题的数据 ② 说明怎么修 ③ 给按钮让用户**自己选择**是否修。
   **禁止**「先自动改掉、再给用户一个撤销按钮」这种反向逻辑。用户选择的修复只是**登记意向**，真正的转换发生在迁移写入目标库时，源库始终不动。
2. **实时进度按「库.集合」逐集合显示，不做「按数据库汇总」。** 屏幕上出现 `xxx.全量汇总` / `xxx.增量汇总` 这种整库聚合行，就是 bug（见 §3.4、§5）。
3. **暂停 / 继续 / 停止三个按钮必须始终在场**，按 running / paused / finished 三态启用禁用（见 §3.3）。它们「消失」或「全灰」通常不是 UI 删了按钮，而是任务提前失败进了 `done/failed` 态——要顺着根因查，别去动按钮逻辑。

---

## 1. 阶段一：评估 / 检测（Assessment）

### 1.1 目标
只读扫描源库，产出一份「迁移前体检报告」+ 一份「调优建议」。**只读，永不写源库。**

### 1.2 发现项分级（severity）
| 级别 | 语义 | UI「操作」列应显示 |
|---|---|---|
| **A**（`SeverityAutoFix`） | 迁移器写入时会**自动**处理（如 `_id` 类型转换、`__x__→_x_` 字段名重写、超长字段名 stringify） | `迁移时自动修复 / 无需操作`（绿色小字，**不给按钮**） |
| **B**（`SeverityBlock`） | 会**阻断**该集合迁移，必须处理 | 有修复建议 → `选择修复` 按钮；无建议 → `需人工处理` |
| **C**（`SeverityWarn`） | 警告，不阻断 | 同 B：可选择修复或人工处理 |

> ⚠️ 常见回归：把 A 类也走到「无自动修复 / 需人工处理」分支。A 类**天生就是自动修复、无需人工**，写成「修复不了」是错的。渲染时先按 `severity` 分支，A 类直接出绿色自动修复说明。

### 1.3 「选择修复」的解耦模型（核心交互）
点一次修复按钮 **≠** 重新全量扫描。流程拆成两步：

1. **登记（opt-in）**：点「选择修复」→ 前端 `POST /api/remediate` 把这条修复意向写进 `remediation-plan.json`，然后**只在本地把该行重绘**成「已选择修复 ✓ ／ 不修复」——**不触发服务端重新扫描**。
   - 前端用 `PENDING`（`Map`，key=`rule|db|coll`）+ `LAST_REPORT`/`LAST_TUNING` 缓存来本地重绘。
2. **重新检测（用户主动，且只点一次）**：用户勾完所有想修的项，点「重新检测（模拟已选 N 项修复）」→ 才真正重新扫描，展示「模拟应用你选择的修复后，还剩哪些没解决」。

按钮/状态文案（必须与此一致）：
- 未选：`选择修复`
- 已选：`已选择修复 ✓` + `不修复`（取消登记，本地删掉 `PENDING` 项并重绘）
- 第二张表标题：`你已选择修复的项（源库不改动，迁移写入时才转换；不想修就点「不修复了」）`
- 取消动作叫 `不修复了`，不叫「撤销」（「撤销」暗示已经改过，违反铁律 1）

### 1.4 源库始终不改
所有「修复」都是登记进 `remediation-plan.json` 的**计划**，迁移时由 transform 层在写目标库前套用。控制台任何按钮都不得直接改源库文档。

---

## 2. 阶段二：迁移模式与驱动路由（决定实时进度长什么样）

> 这一节是理解 §3 UI 的前提：**同一个进度表，走不同代码路径会长成两种完全不同的样子。**

### 2.1 两种模式（UI 单选）
- `migrate`：**全量迁移**（一次性，只做初始全量，不追增量）
- `live`：**全量 + 增量追尾**（先全量，全量完成后转增量）

### 2.2 两条驱动路径
| 条件 | 走的路径 | 用什么驱动 | 进度行粒度 |
|---|---|---|---|
| `live` **且** 源 `replicationMethod=oplog-legacy`（MongoDB 3.0/3.2） | `startOplogReplicationLegacy`（mgo 双驱动） | **mgo 传统驱动**，能连 3.0 | **逐集合**（legacy 复制器自己上报，见 `oplog_replicator_legacy.go`） |
| 其它（`migrate` 全量，或非 legacy 源） | 通用 backfill 路径（`migrator.go:244` 起） | **现代 Go 驱动**（要求 wire version ≥ 6，即 MongoDB ≥ 3.6） | `pollBackfill`/`pollIncremental` → **按库聚合**（`全量汇总`/`增量汇总`） |

### 2.3 ⚠️ 关键约束：MongoDB 3.0 源不能走现代驱动
现代 Go 驱动连 3.0 会直接报：
```
failed to ping MongoDB: server at <host> reports wire version 3,
but this version of the Go driver requires at least 6 (MongoDB 3.6)
```
因此对 `oplog-legacy` 源：
- **只有 `live` 模式能正常工作**（初始全量由 legacy 复制器用 mgo 完成）。
- 目前 **`migrate`（全量-only）模式对 3.0 源是坏的**——它走现代驱动 backfill，必然 wire version 3 失败。
- **设计要求**：对 legacy 源，全量也必须走 mgo 路径。实现上二选一（见 §6 待修复）：
  (a) `migrate`+legacy 源 → 路由到 legacy 的全量 backfill；或
  (b) 检测到 legacy 源时 UI 锁定/自动选 `live`，并提示「3.0 源仅支持全量+增量路径」。

> 截图里 `analytics.全量汇总` 等三行 + `wire version 3` 报错 = 用户用**默认的 `migrate` 单选**跑了 3.0 源，落到现代驱动路径：既连不上、又显示成「汇总」行。三个症状同一个根因。

---

## 3. 阶段二 UI：实时进度（Live Progress）

### 3.1 任务头
显示 `状态徽标 · 时间`、`job 状态`（created/assessing/initial-load/live/verifying/done/failed/paused 的中文映射见 `console.html` 的状态字典）、以及三个控制按钮。
- **本任务死信计数 chip**：`DLQ_TOTAL > 0` 时，在运行中/暂停任务（以及最新一条已结束任务）头部显示红色「死信 N 条」chip，点击展开 DLQ 明细面板。控制台同一时刻只跑一个任务、`/api/dlq` 读当前工作目录的 DLQ，所以该计数即本任务的死信数。详见 §8。

### 3.2 进度表列
`库.集合 | 阶段 | 进度 | % | 吞吐 | ETA | Lag`

- **库.集合**：**逐集合**一行（如 `analytics.events`），**不是** `analytics.全量汇总`。
- **阶段**徽标：`全量`（`ph-init`）/ `增量`（`ph-live`）/ `增量·已停`（`ph-stopped`）。
  - 徽标必须 `display:inline-block; white-space:nowrap;`，否则「增量」两个字会按字换行、错位（`.ph`、`.sev` 已加，别删）。
- **排序**：先活跃（running/pending）后完成；活跃内**先全量、后增量**，再按名字。体现「先全量再增量」的心智模型。
- **Lag**：按**oplog 位置**算（tailer 已消费到的最新 oplog 秒 − 该集合最后事件秒），空闲时应 ≈ 0；**不要**用 `now - lastEvent` 的墙钟差（会显示成几小时的假延迟）。见 `oplog_replicator_legacy.go` 的 `reportLiveLoop`。

### 3.3 控制按钮三态（`console.html` control 区）
| 任务态 | 暂停 | 继续 | 停止 |
|---|---|---|---|
| running | 可点 | 禁用 | 可点（红色 `on-stop`） |
| paused | 禁用（显示「已暂停」） | 可点（绿色 `on-resume`） | 可点 |
| finished（done/failed） | 禁用 | 禁用 | 禁用（显示「已停止」） |

停止要能真正中断在途工作（通过 context 取消），不是只改个标签。

### 3.4 「汇总行」= 回归信号
表里出现 `*.全量汇总` / `*.增量汇总` 说明当前跑在**现代驱动聚合路径**（`pollBackfill`/`pollIncremental`）。对 legacy 源这既是错的驱动、又是错的粒度。正确的 legacy 路径应逐集合上报（`oplog_replicator_legacy.go:584` 附近的 per-collection 全量行 + `reportLiveLoop` 的 per-collection 增量行）。

---

## 4. 阶段三：迁移后校验（Verify）

- 位置：进度表下方，`验证迁移结果` 按钮 + `同时比对内容哈希（慢）` 复选。
- 逻辑：对每个「源→目标」集合比对文档计数；勾选哈希则额外比对内容哈希。
- `_id` 被转换过的文档：喂 `id-mapping.jsonl` 给校验，让转换后的文档能对上，而不是误报不一致。
- 提示语：`全量完成后再点`。

---

## 5. 状态文件与测试残留纪律（与 UI 表现直接相关）

- 每次控制台「开始」都是全新一跑：`launch()` 先调 `CleanResidualState(".")` 清掉按 pair 序号命名的残留：`oplogTimestamp-*.json`、`resumeToken-*.json`、`initialMigrationState-*.json`、`dlq-*.jsonl`、`id-mapping*.jsonl`、`backfillCheckpoint-*.json`、remediation 审计日志。
  - 不清会导致：新一跑的库→序号映射撞上上一跑的文件 → 某 pair 因陈旧 DLQ 中止 / safety violation / 误判「初始迁移已完成」直接跳增量（这会让实时进度「一开始就显示增量」）。
- **`remediation-plan.json` 不在清理名单里**——它是操作员真实的修复选择，必须跨跑存活。
- **用 curl 直接打真控制台的 `/api/remediate` 测完，必须把 `remediation-plan.json` 清回 `{"items":[]}`。** 否则残留的测试项会让检测页显示成「已应用修复 + 撤销」，看起来像违反了铁律 1（这类残留污染已多次造成来回返工）。

---

## 6. 当前实现与本设计的偏差（待修复清单）

1. **`migrate` 全量模式对 oplog-legacy(3.0) 源不可用**：走现代驱动 → wire version 3 失败。需按 §2.3 让 legacy 源的全量也走 mgo，或 UI 层对 legacy 源禁用纯 `migrate`。
2. **现代路径的进度行是按库聚合的 `全量汇总/增量汇总`**（`migrator.go:207/209`），违反 §3「逐集合」。legacy 路径已逐集合；聚合行只应作为 fallback，且不应出现在 legacy 源的正常迁移里。
3. **UI 默认单选是 `migrate`**（`console.html:191`）。对本项目的 3.0 源，默认就会踩坑 1。检测到 legacy 源时应默认/引导到 `live`。

---

## 7. 索引策略（`_id` 强制建 + 二级索引可关 + live 延迟建 + 进度指示）

> 状态：**已实现并固化（2026-08-20，full-only 真机通过）**。设计过程见 `docs/index-strategy.md`。改动此节任一约束前**先问用户**。
> 背景铁律：Firestore MongoDB-compat **不会自动建 `_id` 索引**（真 MongoDB 会）；不显式建，按 `_id` 的查询/排序就没有索引支撑。

### 7.1 两类索引的解耦（核心决策）
- **`_id` 索引：强制建，无开关。** 每个目标集合都建 `{_id:1}`（名 `_id_`），与 `SyncAllIndexes`、与「复制二级索引」开关**都无关**。幂等：target 已有 `_id_` 就跳过。
- **二级索引：默认建，可关。** 控制台 UI 开关 `#syncSecIdx`（默认勾选）「复制源库二级索引到目标」。关掉 → 只建 `_id`、跳过源库二级索引。

### 7.2 UI 接线（别再各自为政）
- `console.html`：`#syncSecIdx`（默认 `checked`）；`start()` 传 `body.syncSecondaryIndexes`。
- `console.go`：`startRequest.SyncSecondaryIndexes *bool`（**指针**，`nil→true`，兼容老前端）→ `applyIndexOptions(cfg, …)` 把每个 pair 的 `Target.SyncAllIndexes` 设成开关值。
- **`SyncAllIndexes` 只控二级索引**；`_id` 走独立必建路径。三处 modern 调用点 + `migrator.go` 的 `migrate` 分支已**去掉** `SyncAllIndexes || len(Indexes)>0` 门控，改为无条件调用 `syncIndexes`（内部只把二级受开关约束）。→ 别再把 `_id` 塞回门控里。

### 7.3 建索引时机（**永远在数据之后**，全版本统一）
在数据前建索引会让 Firestore 每插一条就重建索引，极慢（实测每个 `_id` 索引 ~1m16s，shop 三集合前置建索引导致 UI 空白约 4 分钟）。故「何时建」这条规则**不分版本**，由 `pkg/migration/deferred_index.go` 的 `DeferredIndexController` **单点持有**；每条路径只注入自己那份「怎么建」的原语，绝不各写一遍「何时建」。
- **共享编排层 `DeferredIndexController`**（2026-08-24 抽出，Phase 1）：
  - `Observe(ctx, lagSeconds)`：喂一个复制 lag 采样（秒；负数=idle/未知，算已追平）。连续 `IndexBuildLagStableChecks`（默认 3）个采样 ≤ `IndexBuildLagThresholdSeconds`（默认 5s）→ 后台异步触发一次（`sync.Once`），不阻塞上报循环。
  - `BuildNow(ctx)`：**同步**建一次（`sync.Once`，与 `Observe/Trigger` 共享 once 门）。全量-only 路径用它——一次性任务的索引必须建完才算完成。
  - `run()`：`SetIndexConcurrency` → 调注入的 `build` 原语（异步启动）→ `WaitForIndexCreation` 统一等待 → `logFailedIndexes`。两参数在 config，`ApplyDefaults` 兜底。
- **三条路径各注入自己的 build 原语，时机全走上面这一套**：
  - **modern live**（change stream，`client_stream.go`）：`build=migrator.syncIndexes`；一个 2s goroutine 把 `incrementalStatsManager.RecentLagSeconds()` 喂给 `Observe`。**已从 before-data 改为延迟建**（旧文档说 modern「保留 before-data 时机」——已作废）。
  - **legacy live**（mgo oplog，`oplog_replicator_legacy.go`）：`build=syncIndexesLegacy`；`reportLiveLoop` 每周期把该周期 per-collection 最大 lag 喂给 `Observe`。（`reportLiveLoop` 的启动已去掉 console 门控，CLI 下也能触发延迟建，与 modern 一致。）
  - **纯全量 migrate**（`migrator.go`）：backfill `wg.Wait()` 之后 `BuildNow(ctx)`（`ctx.Err()==nil` 才建）。
- `IndexOnly` 模式：三条路径各自在数据迁移**之前**直接建索引 + 等待后返回（这是 index-only 语义，不迁数据），不进 `DeferredIndexController`。

### 7.4 进度指示「创建索引 M/N」
- 数据源：`db/mongodb.go` 的原子计数器 `indexTotal/indexDone/indexBuilding` + `IndexProgress()`。
- 上报：`DeferredIndexController.PollProgress(ctx)`（2s，`total>0` 才 emit；`migrator==nil` 即 CLI 无控制台时直接 no-op）→ `migrator.reportIndexProgress` → `metrics.Registry.SetIndexProgress`（并入 `Status().indexProgress`，按 job 键）。**三条路径共用这一个 poller**（旧的 legacy `pollIndexProgress` 已删）。
- 渲染：`console.html` 在 job 块显示「创建索引 M/N + 进度条 + 正在建：coll[name]」；`total==0` 不显示。

### 7.5 索引预算不能重复计数 `_id`（评估页）
源枚举 `ListIndexes` **已含**每集合的 `_id_`。故 Firestore 目标持有的索引数 = `二级(=源总数−集合数) + 每集合一个 _id(=集合数)`，**不能**在含 `_id_` 的源总数上再 `+集合数`（那是把 `_id` 数了两遍，会虚高、可能误触发预算告警）。`assess.go`：`IndexStat.Secondary/IDIndexes/Total`，`CheckIndexBudget` 用 `stat.Total`；评估页表头「源二级索引 / +_id 索引 / 合计」。

### 7.6 待用户后续真机验证（不影响已固化部分）
- live 三态路径（积压不建 → lag 追平触发 → 进度走动 → 完成、无 schema-change 争用）尚未真机跑过；代码已就位。
- full-only「开关关」只建 `_id` 的路径。

> 修这三项时严格对照 §0 铁律与 §2/§3，改完在真控制台用 3.0 源 `live` 跑一遍验证：应看到逐集合的 `全量` 行、全量完成后转 `增量`、三按钮三态正常、Lag 空闲≈0、无任何 `汇总` 行。

## 8. 死信队列（DLQ）：非阻断迁移 + 手动修复后回收

设计原则：**坏数据不阻断整体迁移**。不符合基本转换规则的文档（超大字段、超深嵌套、保留键/集合名等）进 DLQ 并**记录详细错误原因**，其余文档照常迁移；失败数在 UI 可见；用户线下修复后可**重新迁移**。全版本（modern change-stream / legacy mgo oplog / 纯全量 migrate）走**同一套判定与回收逻辑**（一套设计服务所有版本，各版本仅按能力微调适配）。

### 8.1 非阻断：只放宽「初始后闸」，保留「预检闸」
- **共享判定单点**：`pkg/migration/initial_outcome.go` 的 `resolveInitialMigrationOutcome(dlq, totalFailedCount, log)` 是三条路径**唯一**的初始迁移终态判定；`dlqActiveCount` 统一计数（nil / `NopDLQWriter` 记 0）。三条路径（`client_stream.go` / `oplog_replicator.go` / `oplog_replicator_legacy.go`）的「初始后闸」都调它：算出 `completed_with_failures` 后**记录警告 + 保存状态 + 照常进增量**，不再 `return … Aborting replication`。改任一条前先确认另外两条是同一套，别再各写一遍。
- **预检闸（保留，CLI 安全阀）**：三条路径的开跑前闸（`client_stream.go`/`oplog_replicator.go`/`oplog_replicator_legacy.go` 的 pre-run gate）与 migrate 模式的 `resolveCompletedWithFailures` 仍会在**上一跑遗留 `completed_with_failures` 或残留 DLQ** 时拒绝**重启**——防止静默跳过未完成的初始载入。即：本次跑不阻断；但要重启，得先清零/回收 DLQ。

### 8.2 UI 呈现
- 顶部 `#dlqBanner` 横幅 + 任务头「死信 N 条」chip（§3.1），均由 `/api/dlq` 轮询驱动（`DLQ_TOTAL` / `DLQ_BY_COLL` / `DLQ_REPORT`）。
- DLQ 明细面板 `renderDlqPanel` 按源集合聚合，逐条可展开文档快照与错误原因。

### 8.3 回收：`retry-dlq` + 回源重读
- **默认（重放快照）**：`retry-dlq` 用 DLQ 中保存的文档快照重写目标，适合「配置/规则修复」类问题。
- **回源重读（源数据修复场景）**：CLI `-dlq-resync-from-source`（配置 `retryConfig.resyncFromSource`）。`reprocessDLQLoop` 收到非 nil `sourceDocFetcher` 时，按 `record.DocumentID`（源 _id）**回源重拉最新文档**再迁移；**源里已删的当已解决跳过**（不动目标），源读错误则留在 DLQ。
  - modern 源走 `newModernSourceFetcher`（Go driver `FindOne`）；legacy 源走 `newLegacySourceFetcher`（mgo `FindId` + `modernIDToMgo` 反查 + `convertMgoBSONToInterface` 转 modern 类型再写目标）。
  - 假设 `DocumentID == 源 _id`（除非 `_id` 本身类型非法被 `convertInvalidIds` 改写，属罕见；真实坏数据场景 _id 不变）。
- **UI 重试按钮**：DLQ 面板顶部「重试回收」+「回源重读」勾选框 → `POST /api/dlq/retry {resync}` → `launchRetry`。**仅在无活跃迁移时可点**（否则与运行中任务抢同一 DLQ 文件，前后端都拦）。`launchRetry` 复用上次运行的 `lastCfg`（保证 pair/DLQ 序号对齐）、**不清残留状态**（DLQ 文件正是它的输入，`startJob(cfg, "retry-dlq", cleanState=false)`）。

> 已本地自测（modern 27020 / legacy 27017 双源）：回源重读命中→写入源最新副本、源已删→跳过、源读错→留 DLQ；默认快照重放→写入存档快照。单测见 `reprocess_dlq_test.go`（`TestReprocessDLQLoopSourceResync`）与 `initial_outcome_test.go`。**非阻断初始迁移的端到端**需真机 Firestore 的约束才能产出 DLQ，尚待真控制台验证（逻辑已单测覆盖、三路径经共享单点统一）。
