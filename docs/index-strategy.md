# 设计 spec：索引策略（_id 索引 + live 延迟建索引 + 进度指示）

> 状态：**已固化（2026-08-20）**。MX0–MX3 + MX1b 已实现、编译/单测通过，full-only 真机验证通过；已固化进 `CONSOLE_UI_DESIGN.md §7`，**本文件转为设计过程归档，勿再据此改代码**。live 三态路径与 full-only「开关关」路径的真机复验列为后续（不影响已固化部分；改动前先问用户）。
>
> ## 实现落点（2026-08-20，已 `go build` + `go test ./...` 全绿）
> - **MX0**：`console.html` 加 `#syncSecIdx` 开关（默认勾选）+ `start()` 传 `syncSecondaryIndexes`；`console.go` `startRequest.SyncSecondaryIndexes *bool`（nil→true）→ `applyIndexOptions()` 把每个 pair 的 `Target.SyncAllIndexes` 设成开关值。
> - **MX1**：`db/mongodb.go` 加 `CreateIDIndexAsync`（建 `{_id:1}` 名 `_id_`）+ 进度计数器 `indexTotal/indexDone/indexBuilding` + `IndexProgress()`。`migrator.go syncIndexes` 与 `oplog_replicator_legacy.go syncIndexesLegacy` 都在 `SyncAllIndexes` 门控**之前**加了「无条件为每个目标集合建 `_id`（幂等，`targetHasIndex*` 跳过已存在）」的一段。
> - **`_id` 必建路径接线**：三处 modern 调用点（`initial_migrator.go:50`、`client_stream.go:349` 与 `:539`）与 `migrator.go:286` 的 `migrate` 分支，**移除了 `SyncAllIndexes || len(Indexes)>0` 门控**，改为无条件调用 `syncIndexes`（内部只把二级索引受开关约束，`_id` 恒建）。legacy 的 IndexOnly-checkpoint 分支同样去门控。
> - **MX2**（仅 legacy，全测路径）：`performInitialMigration` **删除**迁数据前的建索引段（保留 IndexOnly 提前返回，移到函数开头）。`fullOnly` 分支改为**全量后**建索引 + `WaitForIndexCreation`。live 走 `reportLiveLoop` 的 lag 门 → `maybeStartDeferredIndexBuild`（`sync.Once`，后台异步）。config 加 `IndexBuildLagThresholdSeconds`(默认5)/`IndexBuildLagStableChecks`(默认3) + `ApplyDefaults`。「caughtUp」定义：本周期无任何上报集合 lag > 阈值（空 liveStats=idle 也算达标）。modern 路径按范围决策**保留原有 before-data 时机**，只补 `_id`。
> - **MX3**：`metrics.Registry` 加 `IndexProgress` 结构 + `SetIndexProgress`/`IndexProgressAll` + 并入 `Status()`。`observer.go reportIndexProgress`。legacy `pollIndexProgress`（2s，`total>0` 才 emit，随 `StartReplication` 全程启动）。`console.html` 在 job 块渲染「创建索引 M/N + 进度条 + 正在建：coll[name]」。
> - **MX1b**：`assess.go` 修正预算重复计数——`Secondary = Source − Collections`；Firestore `Total = Secondary + Collections`（不再在含 `_id_` 的数上再加一遍）；`CheckIndexBudget` 用 `stat.Total`。`IndexStat` 加 `Secondary` 字段；文本报告与 `console.html` 表头改「源二级索引」并渲染 `x.secondary`。

> ~~状态：**设计已定，待实现**。~~验证通过后固化进 `CONSOLE_UI_DESIGN.md`，本文件归档。
> 关联：`docs/legacy-full-only.md`；`CONSOLE_UI_DESIGN.md`。
> 现状锚点：`syncIndexes`（`migrator.go:1436`，显式跳过 `_id_`）；现策略是「先建索引→等建完→再迁数据」（`client_stream.go:383` 注释：并发写会触发 Firestore schema-change 错误）；异步建索引机制 `SetIndexConcurrency`/`CreateIndexFromDefinitionAsync`/`WaitForIndexCreation`（`pkg/db/mongodb.go`）。
>
> **2026-08-20 真机验证发现的致命缺口：控制台根本不建索引。** legacy 全量建索引代码（`oplog_replicator_legacy.go:344-370`）被 `pair.Target.SyncAllIndexes || len(pair.Target.Indexes)>0` 门控；而 `pkg/console/console.go` 构建 config 时**从不设置** `SyncAllIndexes`/`Indexes`/`IndexConcurrency`，`console.html` 里**也没有任何建索引开关**。结果：经控制台跑的迁移 `SyncAllIndexes` 恒为 false → `syncIndexesLegacy` 从未被调用 → target 上**二级索引和 `_id` 索引都不建**（真机日志里连一行 `Syncing indexes...` 都没有）。评估页的「索引预算」（`console.html:639-650`）只是读源库算配额的**预览**，不触发任何创建。→ 见新增模块 **MX0**。

## 1. 已锁定的决策（与用户确认）
1. **显式创建 `_id` 索引**：Firestore MongoDB-compat 不会自动建 `_id` 索引，必须在 target 上显式建 `{_id: 1}`。当前 `syncIndexes` 跳过 `_id_` 的逻辑要改：`_id` 索引要建，其余重复索引仍按名去重。**full-only 与 live 都适用。**（Firestore 上 `_id` 索引的确切形态/名字，实现时 M4 真机验证。）
6. **控制台建索引开关（2026-08-20 新增，与用户确认）**：`_id` 索引与二级索引**解耦**——
   - **`_id` 索引：强制建，无开关。** 它是 Firestore 正确性/可用性刚需（不建就无法按 `_id` 查询）。无论开关如何、无论 `SyncAllIndexes` 真假，每个目标集合都建 `{_id: 1}`。
   - **二级索引：默认建，给一个开关可关。** 控制台 UI 加一个「复制源库二级索引」开关，**默认开**；关掉则只建 `_id`、跳过源库二级索引（用户可迁完自行在 Firestore 端建）。
   - **控制台必须把这套接进 config**（`console.go` 设 `SyncAllIndexes` = 开关值、并设 `IndexConcurrency`），否则 MX1/MX2/MX3 全部形同虚设——这就是 MX0 要补的接线。
   - 现有门控 `SyncAllIndexes || len(Indexes)>0` 的语义随之调整：`SyncAllIndexes` 只控**二级**索引；`_id` 走一条不受该门控约束的独立必建路径（MX1 保证）。
2. **live 延迟建索引**：不再「先建索引再迁数据」。live 模式改为——全量 + 增量积压期间**不建**二级索引；等增量**追到接近实时（lag ≤ 阈值且稳定）**才开始异步建索引。目的：避开高写入积压期的 schema-change 争用与超慢构建。
3. **触发条件**：`lag ≤ indexBuildLagThresholdSeconds`（默认 **5s**）且**连续稳定** `indexBuildLagStableChecks` 个上报周期（默认 3 次，防抖动误触发），全库整体达标后触发一次，异步建所有待建索引。参数写进 config，可调。
4. **进度指示**：因 GCP 建索引耗时长，live 增量阶段控制台要显示：`正在创建索引 M/N 完成`（总数 N、已完成 M、当前在建的 `库.集合[字段]`）。数据源用异步建索引机制上报到 metrics registry，再由 console.html 渲染。
5. **full-only 的索引时机**：无增量阶段，故在**全量加载完成后立即**同步索引（含 `_id`），`WaitForIndexCreation` 等建完再结束任务。

## 2. 模块拆分

> 依赖顺序：**MX0 → MX1 → MX2/MX3 → MX4**。MX0 不做，后面全都不会被触发（这就是真机迁移「一个索引都没建」的直接原因），故列为第一优先。

### MX0 — 控制台接线 + 二级索引开关（`pkg/console/console.go` + `pkg/console/ui/console.html`）
- **console.html**：在「模式/高级」区加一个开关「复制源库二级索引（默认开）」，`id=syncSecondaryIndexes`，`checked`。提示文案：关掉则只建 `_id` 索引，二级索引迁完自行在 Firestore 端建。
- **console.go**：构建 config 的 `Target` 时——
  - `SyncAllIndexes` = 该开关值（控二级索引）；
  - `IndexConcurrency` = 一个合理默认（跟随「推荐并发配置」或固定值，实现时定）；
  - **不要**用 `SyncAllIndexes` 去控 `_id`——`_id` 由 MX1 的独立必建路径保证，与开关无关。
- 自检：开关开 → 日志出现 `Syncing indexes...`；开关关 → 不出现该行、但 MX1 的 `_id` 建索引仍执行。

### MX1 — `_id` 索引创建（`pkg/migration/migrator.go` + `pkg/db/mongodb.go`）
- 改 `syncIndexes`：不再无条件跳过 `_id_`；对每个目标集合显式建 `{_id: 1}`（若 target 尚无）。其余索引仍按名去重、按原逻辑建。
- **`_id` 必建路径与 `SyncAllIndexes` 解耦**：即使 `SyncAllIndexes=false`（用户关了二级索引开关），`_id` 索引仍要建。即建索引入口不能整体被 `SyncAllIndexes || len(Indexes)>0` 门控挡在外面——需要一条「无论如何都为每个目标集合建 `_id`」的路径，二级索引部分再受开关约束。
- legacy 路径的 `syncIndexesLegacy` 同样处理 `_id`。
- 真机验证 Firestore 上 `_id` 索引的建立方式与幂等性（重复运行不报错）。

### MX2 — live 延迟到 near-real-time 建索引（`pkg/migration/migrator.go` / `oplog_replicator*.go`）
- live 模式：**移除**「迁数据前先建索引并等待」这一步（仅 live；`migrate`/full-only 不变）。
- 在增量上报回路里加一个 lag 门：整体 lag ≤ 阈值且连续稳定达标 → 触发一次异步 `syncIndexes`。
- 触发前状态：索引「待建」；触发后：索引「建设中」；完成：索引「已完成」。只触发一次。

### MX3 — 索引进度指示（`pkg/metrics/registry.go` + `pkg/console/ui/console.html`）
- metrics 暴露：索引总数 N、已完成 M、是否在建、当前在建项。
- console.html 在实时进度区加一行/一块：`创建索引 M/N（正在建：coll.field）`；未开始时显示「待增量追平后创建索引」。

### MX4 — 验证 + 固化
- `go build` + `go test`。
- 真机三条路径都验：
  1. **full-only + 开关开**：全量做完 → 建 `_id` + 二级索引 → M/N 进度走动 → 任务结束；target 上 `_id` 与源库二级索引都在。
  2. **full-only + 开关关**：只建 `_id`，日志无 `Syncing indexes...`（二级），target 上只有 `_id`。
  3. **live + 开关开**：全量→增量积压(lag 高，不建索引)→lag 追平至阈值稳定→触发异步建索引→进度 M/N 走动→完成；确认无 schema-change 争用错误、`_id` 已建。
- 固化进 `CONSOLE_UI_DESIGN.md`（新增「索引策略」小节）。

## 3. 风险与取舍
- 改动 live 现有「建完再写」的防 schema-change 保护 → 靠「近实时才建」把并发写压到稳态低速来规避；仍需真机确认 Firestore 在低速写下建索引不报 schema-change。
- 全量+积压期间无二级索引：对本工具的写入（按 `_id` upsert/replace）无影响；不影响正确性，只是这期间目标库不可用于二级索引查询（迁移期本就不该被查询）。
