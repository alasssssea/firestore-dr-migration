# Firestore 容灾迁移工具设计（DESIGN.md）

> ## ⚠️ 本 fork 的范围（先读这段）
>
> 本仓库（`firestore-DR-migration`，二进制 `fsdr`）是 `gsbingo17/mongodb-to-new` 的 fork，**只做单区域 Firestore → 多区域 Firestore（MongoDB 兼容）的迁移**（容灾/位置变更）。相对上游，本 fork 做了如下收窄：
>
> - **只支持 Firestore → Firestore**。已删除全部 MongoDB 自建源、`oplog` / `oplog-legacy` 复制、旧版 `mgo`/`gtm`/`hashring` 驱动代码。增量只有单条 `db.watch()` 变更流路径。
> - **单变更流**：Firestore 变更流的 watch 管道不支持 `$expr` 哈希分区 stage，故 `incrementalStreamPartitions` 被强制为 `1`（无多分区 CDC）。
> - **不做 `_id` 转换**：源已是 Firestore，`_id` 本就合法，原样透传（`convertInvalidIds` 强制关闭），保证目标逐条等于源、verify 能对上。
> - **工具不建库**：目标多区域库须由用户预先建好并提供连接串（上游 §7 的 provisioning 在本 fork 不启用）。
> - **工具不冻结源写入**：只发「割接就绪」信号，由用户在应用层停写，工具再排空增量。
> - **变更流只做预检**：live 前若源库未建 change stream，工具报错并打印启用步骤（Cloud Console，无 gcloud 命令），不自动启用。
>
> **下面从「§0 现状速览」起是上游的原始评审与设计记录**，保留以存档设计理由；其中涉及 oplog / 源版本自动检测 / provisioning / id-mapping / `BuildSourceURI` 的部分，在本 fork 已按上面的收窄失效或改写，阅读时以本段为准。§6（连接串）、§2（评估）的核心仍适用。

---

## 0. 现状速览

- 语言/结构：Go，约 7.4k 行；`cmd/migrate` 入口 + `pkg/{config,db,logger,migration}`。
- 能力：`migrate`（一次性全量）/ `live`（全量 + 增量）；三种增量方式 `changestream` / `oplog` / `oplog-legacy`（兼容 3.0/3.2 老库，用 mgo 双驱动）；多级并行、断点续传（resume token / oplog timestamp）、DLQ、重试拆批、Firestore 兼容 transform（`__x__→_x_`、超长字段名对象 stringify、非法 `_id` 转字符串）、索引同步（含 index-only）。
- 本地既有改动：`pkg/db/mongodb.go` 把索引构建并发 `indexSemaphore` 从 1 调到 4（实验性，见 §9 注意事项）。

---

## 1. 缺陷清单（按严重度）

### 🔴 正确性 / 数据完整性
1. **Live 初始迁移中途崩溃会静默丢数据**。是否做初始迁移只看 resume token 文件是否存在（`client_stream.go`）。抓 token 后立即存盘，若初始迁移中途崩溃，重启时 token 已存在 → 跳过初始迁移，直接追增量 → 抓 token 时已存在、之后未被改动的文档永久丢失且无报错。**无「初始迁移完成」标记**。
   - 修：checkpoint 增加显式状态 `{phase, initialDone, perCollectionDone}`，只有 `initialDone=true` 才进增量；初始迁移本身支持断点续传。
2. **完全没有迁移后校验**（见 §3）。
3. **`_id` 类型转换有损、可能主键碰撞**：`fmt.Sprintf("%v", id)` 把 `int 5 → "5"`，可能与已有 `"5"` 撞；且静默改主键破坏跨集合引用。→ 必须持久化 id-mapping 并写审计（也是 online 校验的前置，见 §3）。
4. `extractFailedIndicesFromError` 是空壳（`retry.go`），恒返回 nil → 非法 `_id` 时全批扫描而非定位失败项。

### 🔴 安全
5. **连接串（含账号密码）被打进 Info 日志**（`migrator.go:159/166/359/367`）。→ 打印前脱敏 `//user:***@`。

### 🟠 可靠性 / 运维
6. **checkpoint / DLQ 只存本地文件**，不适合 Cloud Run 等无状态环境（实例回收即丢，直接触发 #1）。→ 支持 GCS/Firestore/Redis 外部持久化。
7. **resume token 序列化极脆弱**（`resumetoken.go` 用 `fmt.Sprintf("%v")` + 手工解析 `map[_data:...]` 字符串前缀）。→ 直接用 BSON/Extended JSON 原样存取。
8. **无指标 / 无健康端点**：进度只进日志（其实百分比已算好，只是被 `log.Infof` 丢掉）。无复制延迟 lag、无 `/metrics`、无 `/healthz`。→ 见 §4。
9. **错误分类靠 `strings.Contains`**（`retry.go`），版本/语言敏感。→ 改用 MongoDB 错误码（duplicate key `11000` 已正确用码）。
10. **DLQ 只写不读**，无重放工具（用户被迫手写 `deduplicate.js`）。→ 加 `-mode=replay-dlq`。

### 🟡 代码质量
11. **批处理骨架三份复制**（`migrator.go` migrateCollection / migrateCollectionParallel / `client_stream.go` 初始迁移），且**行为不一致**：migrate 走 `RetryManager`（重试+拆批），client_stream 初始迁移是手写 InsertMany+逐条 upsert，**没走 RetryManager**。→ 抽公用 `copyCollection()`。
12. **`ConvertInvalidIds` 永远无法关闭**（`config.go:208`：`if !x { x = true }` 覆盖显式 false）。→ 用 `*bool` 区分未设置/显式 false。
13. **零测试**。`transform.go`、`resumetoken.go` 是纯函数极易测。→ 优先补单测 + testcontainers 端到端。
14. **硬编码魔法值/节流不一致**：`migrator.go:616` 每批 `time.Sleep(5ms)`，但并行/client_stream 路径没有。→ 统一或去除。

---

## 2. 迁移前评估 `-mode=assess`

扫源库，把每条 Firestore MongoDB-compat 限制翻译成检测规则，输出**分级报告**：A=工具能自动改（提示）/ B=硬阻断（改不了，人工先处理）/ C=会静默劣化（警告）。

| Firestore 限制 | 检查 | 类别 | 高效检测 |
|---|---|---|---|
| 文档 ≤ 16 MiB | 单文档 BSON 大小 | B | 服务端 `$bsonSize`（Mongo 4.4+）过滤 |
| 嵌套 ≤ 20 层 | map/array 深度 | B | 客户端抽样递归 |
| 字段名 ≤ 1500B / 不能 `__.*__` | 字段名长度+模式 | A/B | 注意 transform.go 阈值是 1000，真实限制 1500 |
| 字段路径 ≤ 1500B | 累积路径 | B | 递归累加 |
| 字段值 ≤ 4MiB−89B | 单 value | B | 抽样 |
| `_id` 类型/≤1500B/string 不能 `__.*__` | `_id` | A/B | 全扫 `_id`（便宜）；非法 `_id` 先报再决定是否转 |
| 集合名（`$`/`__.*__`/`system.`/≤1500B） | 集合名 | B | `listCollections` 一次查 |
| 每库索引 ≤ 1000 | 全库索引数 | **B** | `listIndexes` 求和（含 §9 要补的 `_id` 索引！） |
| 索引字段 ≤ 100 / 条目 ≤ 40000/文档 / 单条 ≤ 7.5KiB / 总和 ≤ 8MiB | 索引 | B | 解析定义 + 抽样被索引数组/长字段 |
| ≤ 100 数据库/project | 目标库数 | **B** | 见 §7（provisioning 预算） |

策略：静态检查（集合名/索引）全库跑（近零成本）；文档级检查小集合全扫、大集合 `$sample` + 服务端 `$bsonSize` 兜底。

---

## 3. 迁移后校验 `-mode=verify`（含在线迁移难点）

**核心坑**：工具自身会改数据（transform、`_id` 转换），所以**不能拿源原始 hash 对目标 hash**——被改过的文档会全假阳性。校验必须**先对源套用同一套 transform**，再 canonical 化（key 排序、类型归一、忽略 BSON 顺序）后比 hash；`_id` 被转换的文档要靠持久化的 **id-mapping** 才能对上号（与 #3 耦合）。

**全量**：`count + 分段 hash`。

**在线（源在变，移动靶）**——引入逻辑时钟把移动靶降维成静态快照：
- **权威：收敛后静默窗口比对（cutover verify）**。先靠 lag 指标（§4）追平 → 冻结源写入/切走 → 等 target 应用位点 ≥ 源 oplog head → 两边静止跑全量比对。这是最终「迁移成功」的背书。
- **运行期管道校验**（不冻结，给信心）：每条落 target 的文档回读，与 change stream 事件 `fullDocument` 比对；`count(源)-count(目标)` 应在 lag 附近波动而非单调扩大。
- **运行期抽样对账**（不冻结）：用 `updatedAt`/`_id` 水位线取「早于 W 且此后未改」的稳定子集，按 `_id` 区间做 Merkle/分段 hash，只对不一致段下钻。

`verify` 内部必须复用迁移的 transform + id-mapping。

---

## 4. 指标 / 进度条 / ETA / lag

引擎从「日志副产品」改为发布**结构化进度事件**到 stats registry；`/metrics`（Prometheus）给运维，`/api/status`（SSE/WebSocket）给 UI。

- **初始全量：进度条 + ETA**。总量已知（`CountDocuments` + `collStats.size`）；**按字节比按文档数准**（文档大小不均）；速率用 EWMA；`ETA = 剩余字节 / 当前总吞吐`（总吞吐已含并行度）。**诚实标注**：冷启动 ETA 不稳；**索引构建时间不在数据吞吐里**（现异步 fire-and-forget，大集合建索引可能比迁数据久），须单列阶段，别让进度条 100% 误导用户。
- **增量：无进度条，换 lag 面板**。显示复制延迟（change stream `clusterTime` 距 now）+ ops/sec。**lag≈0 = 可 cutover 校验的信号**（接 §3）。

---

## 5. UI 控制面

**形态：单二进制 + `go:embed` 内嵌前端**（`./migrate ui` 起 localhost:8080）。CLI 模式完全保留（无头/Cloud Run/CI）。不用 Electron。

```
[Web UI (React, embed)]
      │ REST + SSE/WebSocket
[Control API (新增 Go)]  — Job 生命周期 / 配置发现·生成·校验 / assess·verify 调度 / start·pause·stop
      │ 进度事件总线 + Job 状态持久化
[Migration Engine (现有核心, 改造成会汇报)]
      │
[Source Mongo] → [Firestore]
```

引擎需补三样：**Job 模型 + 持久化**（状态机 `created→assessing→initial-load→live→verifying→done/failed`，落外部存储，顺带解决 §1.6 checkpoint 外部化）、**进度/统计总线**、**控制指令**（per-job/per-collection start/pause/resume/stop/retry-DLQ）。

---

## 6. 配置向导 + 连接串 Builder

**原则：JSON 不该由人手写，应作为向导/UI 的机器产物**（CLI/Cloud Run 仍用它）。痛点根因与解法：
- 「不同源版本写法不一致」→ 根因是让用户填 `replicationMethod`。**改为自动检测**（`buildInfo`/`isMaster`/wire version/是否副本集）→ 自动选 changestream/oplog/oplog-legacy（`replicationMethod:"auto"` 默认）。
- 「多库/选表烦」→ **先发现再勾选**：`listDatabases`/`listCollections`/`collStats` 列出库表（含文档数/大小/索引数）→ 勾选 + 批量映射规则 + 个别覆盖。

### 连接串 Builder（规则驱动 / 源·目标不对称 / 源版本感知）

**踩坑要点：source 越老「少写」，target Firestore「必须写全」，两者相反,用户手写极易搞反。**

| 场景 | 关键 auth 参数 |
|---|---|
| Source：特别老 MongoDB（2.x/3.0/3.2, oplog-legacy） | `authSource=admin`，**省略 `authMechanism`**（让 driver 协商降级；硬写 SCRAM-SHA-256 会报错） |
| Source：现代 MongoDB（4.0+） | 可省略（默认协商 SCRAM-SHA-256），按需 `authSource` |
| Target：Firestore + SCRAM | **必须 `authMechanism=SCRAM-SHA-256`**，否则报错；+ 强制 `loadBalanced=true&tls=true&retryWrites=false` |
| Target：Firestore + 服务账号（长跑推荐） | `authMechanism=MONGODB-OIDC` + `authMechanismProperties=ENVIRONMENT:gcp,TOKEN_RESOURCE:FIRESTORE` |

原因：auth 机制随版本演进（MONGODB-CR<3.0 → SCRAM-SHA-1 @3.0 → SCRAM-SHA-256 @4.0）；现代 driver 靠 `saslSupportedMechs` 协商，对老库硬指定新机制会失败；Firestore 的 load-balanced+TLS 托管入口不会自动协商到位，须显式。向导只采集 `主机/账号/密码`，其余按源版本+目标类型自动补齐。

---

## 7. 目标侧 Provisioning（Firestore 建库 + 连接串自动装配）

**关键前提：Firestore 每个数据库是独立 endpoint，不共享连接串。**
```
mongodb://UID.LOCATION.firestore.goog:443/DB_ID?loadBalanced=true&tls=true&retryWrites=false
```
UID 是**创建后**才生成的 UUID4（`gcloud firestore databases describe --format='yaml(locationId,uid)'` 取）。Firestore **无 instance→database 层级**，源 N 个库 → N 个 Firestore 库 → **N 条各异连接串**，且必须「先建库 → describe 取 UID → 才能拼串」。这是「config 必须由工具生成」的铁证。

**工作流**：
```
源发现 → 勾选库 → 名字 sanitize+校验 → [生成命令 | 一键创建] → describe 取 UID
  → 选认证(在线迁移强制 OIDC-SA) → 拼连接串 → 注入 config → assess → migrate
```

约束：
- **建库命令**：`gcloud firestore databases create --database=ID --location=LOCATION --edition=enterprise --enable-mongodb-compatible-data-access`。**enterprise edition 是 MongoDB 兼容硬前提**；location 必填且基本不可改（默认东京 `asia-northeast1`）；删库后 5 分钟内不能复用同名 ID。
- **库名规则严**：仅小写字母/数字/连字符、首字母、末字母或数字、4–63 字符、非 UUID。Mongo 库名常违规（`db` 太短、`MyApp` 大写、`user_data` 下划线）→ 必须 sanitize + 用户确认 + 撞名检查 + **100 库/project 上限核对**（进 assess）。
- **认证生命周期**：临时 access token 仅 1 小时（在线迁移会中途失效，禁用）；SCRAM 密码创建时只显示一次；长跑用 **MONGODB-OIDC + 服务账号**。
- **`retryWrites=false` 强制**：工具必须完全靠自身 RetryManager（现已如此），不得依赖 driver 重试；须复核无处写 `retryWrites=true`。
- **两档并存**：默认「生成命令/Terraform」（工具不需 Firestore Admin 权限，安全可审计）；可选「一键创建」（需授权，调 Admin API + poll LRO + describe 回填）。

---

## 8. Target-aware 索引同步

现在无条件跳过 `_id_`（`migrator.go:1110,1149`；README 第 204 行），假设目标会自建——对 MongoDB 目标成立，对 Firestore **错误**。

官方确认：**Firestore MongoDB-compat 默认不建任何索引，单字段索引非自动**，`_id` 无例外。→ 迁到 Firestore 后 `_id` 上无索引。

细节：
- **点查 `find({_id})` 不需要索引**（主键定位天然快）；**真正需要的是 `_id` 上的排序/范围**（`sort({_id:1})`、分页、`$gt/$lt`）——很多应用依赖「按 `_id` 排序≈按时间」的隐含行为，迁后会退化成全扫。
- 要建的是**普通单字段 `_id` 索引，去掉 unique**（主键唯一性天然保证）；名字用 `_id_1` 之类。
- Firestore「每次只能建一个索引」→ 现有串行异步建索引机制正好兜住，加一个 `_id` 只是多塞一个。
- **吃 1000 索引/库预算**：每集合 +1，集合多时须进 assess 核算。

解法：索引同步**目标类型感知**——目标 MongoDB 维持跳过 `_id_`；目标 Firestore 把 `_id_` 翻译成显式普通单字段 `_id` 索引补上。目标类型靠自动检测（`buildInfo`）或 `targetType` 配置。**默认为 Firestore 目标补 `_id` 索引，可关**（应用从不按 `_id` 排序则关掉省预算）。

> 更本质：工具目前「半个身子」是 Firestore-aware——transform 是 Firestore 专用，但索引同步却假设标准 MongoDB。这条一并纠偏。

---

## 9. 分期路线（每期独立可交付）

- **第一期（性价比最高，可先无 UI）**：①源版本自动检测（去掉手填 `replicationMethod`）②配置生成+校验（可先交互式 CLI 向导）③结构化进度/指标 API + ETA。做完这一期，「配置难写」「不知进度」两个最痛点即解。
- **第二期**：发现 + 映射向导 Web UI（列库表、勾选、配映射、跑 assess）。
- **第三期**：完整 Job 生命周期控制台（进度条/ETA/lag、verify、DLQ 重放、暂停/续跑、目标侧 provisioning 一键创建）。

**穿插的正确性硬修**（应尽早，独立于分期）：#1 崩溃丢数据、#5 凭据脱敏、#12 ConvertInvalidIds 指针化、#13 纯函数单测。

### 注意事项 / 待决
- 本地 `mongodb.go` 把索引并发 1→4：与「串行防 Firestore 跨事务争用」的原始意图相悖，需在真实 Firestore 上验证是否引发 contention，再决定保留/回退/做成可配置。
- id-mapping 的存储格式与 verify 的耦合需在第一期定型（否则在线校验无法落地）。

## 10. 实现进度（本轮全部落地）

以下条目均已实现并有单测，`go build/vet/test ./...` 全绿：

- **#1 崩溃丢数据硬修**：`checkpoint.go` 用独立 `.initial-done` 标记区分「token 已抓」与「初始迁移已完成」，只有标记存在才进增量；否则重跑初始迁移（幂等 upsert）。
- **#5 凭据脱敏 / #12 / #13 等第一期硬修**：完成。
- **§2 `-mode=assess`**（`pkg/assess`）：迁移前 Firestore 兼容性抽样检查，阻断性问题 exit 1。
- **§3 `-mode=verify`**（`pkg/verify`）：count 恒比 + 可选内容 hash（`-verify-hash`）；复用迁移的 `TransformFieldNames` + id-mapping（`-id-map`）；XOR 顺序无关指纹，只对不一致集合下钻。
- **§4 指标/进度/ETA/lag**（`pkg/metrics` + `pkg/progress`）：结构化进度事件总线；`/metrics`（Prometheus）、`/healthz`、`/readyz`；按字节 EWMA 吞吐 + ETA（诚实标注可靠性）。
- **§5 UI 控制面**（`pkg/metrics/http.go` + `ui/`，`-metrics-addr`）：`go:embed` 仪表盘 + `/api/status`（JSON）+ `/api/stream`（SSE）+ `/api/control`（pause/resume/stop）；Job 状态机 + 协作式暂停/续跑/停止（batch 边界 `controlWait`）。
- **§6 连接串 Builder**：`BuildFirestoreURIOIDC`（服务账号 OIDC，长跑免密码推荐）/ `BuildSourceURI`（老源省略 authMechanism）；向导集成。
- **§7 目标侧 provisioning**（`pkg/provision`，`-mode=provision`）：生成 `gcloud firestore databases create --edition=enterprise --enable-mongodb-compatible-data-access` 计划/脚本；默认东京 `asia-northeast1`；DB ID 规则校验。
- **#6 checkpoint 外部化**（`pkg/migration/store.go`，`-checkpoint-location`）：`CheckpointStore` 接口 + `LocalStore`（可设 `Root`）+ `GCSStore`（走 `gcloud storage` CLI，无重依赖）；resume token 备份轮转与初始迁移标记全部经 store，`gs://bucket/prefix` 使 Cloud Run 等无状态环境不再丢 checkpoint（直接消解 #1 的无状态诱因）。
- **#10 DLQ 重放**（`pkg/migration/dlq_replay.go`，`-mode=replay-dlq`）：DLQ 文档改存 canonical Extended JSON（无损保留 int64/ObjectID 等），幂等 upsert 重放；`-dlq-dry-run` / `-dlq-failed-out`。
- **#11 抽公用 copyCollection**：live 初始迁移（`client_stream.go`）不再手写 InsertMany+逐条 upsert，改调用共享的 `migrator.migrateCollection`，与 migrate 模式**行为一致**：RetryManager（重试+拆批+非法 _id 转换）、按字节 ETA、协作暂停/停止、DLQ 兜底。批处理彻底失败的「毒批」统一写 DLQ 并继续（非 ctx 取消），migrate 模式也随之获得 DLQ 兜底能力。

> 说明：`oplog_replicator.go` / `oplog_replicator_legacy.go` 各自的 `migrateCollection`（oplog 路径、后者用 legacy `mgo` 驱动）不在 #11 范围内，保持原状。
