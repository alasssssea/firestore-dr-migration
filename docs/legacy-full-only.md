# 设计 spec：legacy(3.0) 源「仅全量」迁移（full-only）

> 状态：**设计已定，待实现**（实现并验证通过后，把结论固化进 `CONSOLE_UI_DESIGN.md`，本文件转为归档）。
> 关联：`CONSOLE_UI_DESIGN.md` §2、§6；DESIGN.md §1 #1（静默丢数据）。

## 1. 已锁定的决策（与用户确认过）
1. **入口**：复用现有「全量迁移(migrate)」单选。对 `oplog-legacy` 源，`migrate` = full-only（走 mgo）；`live` = 全量+增量（不变）。**不新增 UI 控件。**
2. **语义**：full-only 是**终点**——做完全量就停，不进 oplog tailing，不为「日后续增量」保留 oplog 起点。要增量就一次性跑 `live`。
3. **适用场景**：静态库、活库**都要支持**。因为终点、且不追增量，活库上 full-only **会漏掉扫描期间及之后的写入**。
4. **护栏**：选 `migrate` + legacy 源时，UI 与日志都要**醒目告警**：「仅全量不追增量，扫描期间的写入不会被迁移；需要完整请用『全量+增量』」。（无法可靠探测源库是否在写，故对 full-only 一律提示。）
5. **索引**：full-only 在**全量加载完成后、返回前**同步索引（含显式创建 `_id` 索引，见 `docs/index-strategy.md`），等建完再结束任务。full-only 无增量阶段，故不涉及「等 lag 低位」。
6. **失败语义**：沿用现有 legacy 初始迁移——有 DLQ 失败 → `completed_with_failures` → 报错终止，DLQ 留待重放。保持一致，不特殊化。

## 2. 现状与切口（已核对代码）
- `live`+legacy 已走 `startOplogReplicationLegacy`（mgo，逐集合上报，能连 3.0）。
- 该路径 `StartReplication` 的流程：`performInitialMigration`（mgo 全量）→ 然后 `tailOplog`（增量）。
- **天然切口**在 `oplog_replicator_legacy.go:324`：`IndexOnly` 模式就是在全量做完后 `return nil` 不进 tailing。full-only 复用同一切口即可。
- 当前 bug：`migrate`+legacy 落到 `migrator.go:244` 的**现代驱动**路径 → 连 3.0 报 `wire version 3`，且进度是 `全量汇总` 聚合行。

## 3. 模块拆分（小、可独立验证）

### M1 — 后端路由（`pkg/migration/migrator.go`）
- 扩展 `takesLegacyReplicator`（:200）与 legacy 分支条件（:214）：`(mode==live||live-only||migrate) && replicationMethod==oplog-legacy` 都走 legacy 路径。
- 相应扩展 poller 护栏（:206-211）：legacy full-only 也自行逐集合上报，必须**跳过** `pollBackfill/pollIncremental` 的 `全量汇总/增量汇总`。
- 给 `startOplogReplicationLegacy` 增加 `fullOnly bool`（由 `mode==migrate` 推出），透传给 `StartReplication`。

### M2 — legacy 复制器早退（`pkg/migration/oplog_replicator_legacy.go`）
- `StartReplication` 加 `fullOnly` 参数；在全量完成后、`tailOplog` 之前（:324 附近，仿 `IndexOnly`）：`if fullOnly { log("仅全量完成，跳过增量追尾"); return nil }`。
- 全量完成状态照写 `completed`（无害、与现有一致）。

### M3 — UI 护栏与阶段显示（`pkg/console/ui/console.html`）
- 选 `migrate` 单选时，若源是 legacy，在模式区/开始前显示醒目告警（见决策 4 文案）。
- 确认 full-only 任务的阶段徽标只出 `全量`、不出 `增量`；进度逐集合、无 `汇总` 行。

### M4 — 构建 + 回归验证
- `go build ./... && go test ./pkg/migration ./pkg/console ...`。
- 真机 3.0 `migrate` full-only 跑一遍，验收：逐集合 `全量` 行、无 `汇总`、全量做完即停（无 `增量`）、暂停/继续/停止三态正常、无 `wire version 3`、护栏告警可见。
- 跑前清残留（保留 `remediation-plan.json`）。

### M5 — 固化
- 把已验证行为写回 `CONSOLE_UI_DESIGN.md`（§2 更新路由表、§6 勾销第 1/2/3 条），本 spec 标记归档，**后续不再改动**。

## 4. 执行顺序
M1 → M2（后端一起）→ M3（UI）→ M4（验证）→ M5（固化）。每步做完自测通过再进下一步。
