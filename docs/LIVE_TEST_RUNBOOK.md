# fsdr Live 迁移测试 Runbook

> 目的:从零跑通一次 **Firestore(MongoDB 兼容)regional → multi-region** 的 live 迁移测试
> (初始全量 + change stream 增量 + 持续混合写 + 一致性校验),并沉淀所有踩过的坑。
> 每次重来照这个走即可,不用再摸索。

**项目**:`rock-collector-342013` ｜ **区域约定**:source=`us-central1`(regional),target=`nam5`(multi-region)
**工作目录**:`~/firestore/firestore-DR-migration`

---

## ⚠️ 三个必踩的坑(先看这个)

1. **change stream 只能在 Console 手动建,没有 gcloud/REST/mongosh 命令。**
   - fsdr 需要 **database scope**(整库)的 change stream,不是 collection scope。
   - 新建后**需要几分钟才 active**,期间 fsdr 会报 `Database scope change stream is not active`。要轮询重试。
2. **mongosh 用 OIDC(`ENVIRONMENT:gcp`)连不上会挂死**(Go driver 正常)。要在源库建索引等操作,用仓库里的 `cmd/mkindex` 小工具走 Go driver,别用 mongosh。
3. **loadgen 混合写有 burst-drain 设计缺陷**,带 update/delete 时总吞吐**卡在 ~500/s**,打不到 1500/s。纯 insert 能到 6000/s。详见"已知限制"。

---

## 前置

- ADC 已就绪(本机 metadata SA 即可,连接串用 `authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:gcp,TOKEN_RESOURCE:FIRESTORE`)。
- 建 change stream 需要 `roles/datastore.indexAdmin`。
- 二进制已编译:`fsdr`、`loadgen`(在工作目录)。`go` 可用(1.25)。

---

## 步骤

### 1. 建两个 Enterprise 库(source + target)
```bash
P=rock-collector-342013
gcloud firestore databases create --database=us-db2   --edition=enterprise --location=us-central1 --project=$P
gcloud firestore databases create --database=target-db2 --edition=enterprise --location=nam5       --project=$P
```

### 2. 取 uid,拼连接串
```bash
gcloud firestore databases describe --database=us-db2   --project=$P --format="value(uid,locationId)"
gcloud firestore databases describe --database=target-db2 --project=$P --format="value(uid,locationId)"
```
连接串格式(host = `<uid>.<region>.firestore.goog:443`,path = 库名):
```
mongodb://<uid>.<region>.firestore.goog:443/<dbname>?loadBalanced=true&tls=true&retryWrites=false&authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:gcp,TOKEN_RESOURCE:FIRESTORE
```

### 3. 写 config
拷 `firestore_dr_us-db2.json` 改连接串即可(source `replicationMethod: changestream`,target `syncAllIndexes: true`)。

### 4. Seed 基线数据(纯 insert,快)
```bash
SRC='mongodb://<src-uid>.us-central1.firestore.goog:443/us-db2?loadBalanced=true&tls=true&retryWrites=false&authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:gcp,TOKEN_RESOURCE:FIRESTORE'
./loadgen -uri "$SRC" -coll big_events -batch 200 -workers 40 -upd 0 -del 0 -rate 5000 -dur 11s
# ~48k 条 / 11s。loadgen 会自动从已有最大 seq 续写,不会撞键。
```

### 5. 🔴 手动建 change stream(Console)
1. Databases 页 → 选 **source** 库(us-db2)→ 进 Firestore Studio。
2. Explorer → **Change streams** 节点 → More actions → **Create change stream**。
3. **scope 选整个 database**(不是单 collection),设保留期(≤7 天),Save。
4. **等几分钟变 active。** 保留期必须覆盖初始全量耗时。

### 6. 源库建 `{seq:1}` 索引(让 loadgen 的 update 走索引,不然全表扫)
```bash
go run ./cmd/mkindex -uri "$SRC" -db us-db2 -coll big_events
```

### 7. 起 fsdr live(带激活轮询重试)
change stream 刚建时会报 not active,用重试循环:
```bash
for i in 1 2 3 4 5; do
  rm -f resumeToken-global*.json* initialMigrationState-global.json
  nohup ./fsdr -mode live -config firestore_dr_us-db2.json -metrics-addr :9090 > fsdr-live.log 2>&1 &
  FPID=$!; sleep 20
  if kill -0 $FPID 2>/dev/null && ! grep -q level=error fsdr-live.log; then echo "OK PID=$FPID"; break; fi
  kill $FPID 2>/dev/null; echo "not active, wait 120s"; sleep 120
done
```
成功后日志:`Initial migration completed ... Starting incremental replication ... Created database-level change stream ... Starting event distributor with 48 workers`。

### 8. 持续混合写(注意 ~500/s 上限)
```bash
./loadgen -uri "$SRC" -coll big_events -batch 100 -workers 120 -upd 0.12 -del 0.01 -rate 1600 -dur 20m > loadgen-live.log 2>&1 &
```

### 9. 监控(真指标看 fsdr 侧,不是 loadgen 瞬时曲线)
```bash
grep 'Change stream statistics' fsdr-live.log | tail -1   # Read == WorkerReceived 即零积压
wc -l dlq-global.jsonl                                     # DLQ 应接近 0
```

### 10. 校验(先停写、等 CDC 排空)
```bash
kill <loadgen-pid>; sleep 25
./fsdr -mode verify -config firestore_dr_us-db2.json               # count 级
./fsdr -mode verify -verify-hash -config firestore_dr_us-db2.json  # 内容 hash 级
```

### 11. Teardown(计费资源,必须清)
```bash
kill <fsdr-pid> <loadgen-pid> 2>/dev/null
gcloud firestore databases delete --database=us-db2   --project=$P --quiet
gcloud firestore databases delete --database=target-db2 --project=$P --quiet
# change stream 随库删除一并消失;无需单独删。
```

---

## 📐 incrementalWorkerCount 怎么正确估

48 是特定 QPS + nam5 写延迟下测出来的,**QPS 变了要重算**。模型:

> 保证 lag 不涨 ⇔ 目标端写吞吐 ≥ CDC 写入速率
>
> **N_workers ≥ (CDC写入QPS × L) / Beff × 安全系数(1.5–2×)**

- **L** = 一次 bulkWrite 打到目标(nam5 跨区)的往返延迟 —— **必须实测**(从 `:9090` metrics / stats 日志)。
- **Beff** = 每次 bulkWrite 实际装的 op 数(≤`incrementalWriteBatchSize`=128)。update/delete 频繁命中刚插入的 _id 会提前切组 → Beff 变小 → 需要更多 worker。**Beff 是最敏感的量,必须实测。**
- **做法**:先按目标 QPS 跑几分钟标定出 L、Beff,再代入公式,再靠观测收敛。

**加 worker 无效、瓶颈转移的信号:**
- `incrementalIncomingQueueSize`(8192)/`incrementalProcessingQueueSize`(4096)打满 → 瓶颈在 worker,**加 N**(同步抬 `targetMaxPoolSize`,默认 256)。
- worker 没满但 lag 涨 → 单个 change stream reader 读不过来 → **加 `incrementalStreamPartitions`**,不是加 worker。
- 热点 _id(同 id 的 op 为保序钉在同一 worker 串行)→ 加 worker 无解。

---

## 已知限制 / 坑归档

- **loadgen burst-drain**:限速器是 `time.Ticker`(`bps=rate/batch`),只在每次迭代顶部放行;放行后那段逐条 `UpdateOne`/`DeleteOne` 不受限速。worker 一多 → 所有 worker 同步插一批后齐刷刷进 update 排水,insert 冻结(ins/s=0),平均总吞吐被拖到 ~500/s。**要真正稳定驱动 1500/s 混合,得改 loadgen:把 update/delete 拆成各自独立限速的 goroutine 池,与 insert 解耦。** 当前未改。
- **DLQ `context deadline exceeded`**:稳态写超时是 90s([parallel.go:955](../pkg/migration/parallel.go#L955)),只有 shutdown 路径用 10s([parallel.go:951](../pkg/migration/parallel.go#L951))。上一轮那 1400+ 条死信,大部分是 **kill fsdr 时** in-flight 批被 10s 上下文掐的,不是稳态失败。稳态 DLQ ≈ 0。
- **resumeToken 文件 mtime 不刷新 ≠ 落后**:checkpoint 每 `checkpointIntervalMinutes`(5min)才落盘;判断是否追平看 stats 的 `Read == WorkerReceived`。

---

## 本次测试结果(2026-09-11)

- source `us-db2`(us-central1) uid `075df03a-524a-4764-8833-1265609e779d`,target `target-db2`(nam5) uid `10007c4e-a9b2-40f9-9fc2-fe502d63107d`。
- 初始全量:**49,600 docs / 27s / 0 失败**。
- 增量:48 worker,单 database-level change stream;混合写峰值 CDC ~760 events/s,**Read==WorkerReceived 全程零积压**,DLQ≈0。
- 混合写实际吞吐 ~500/s(受 loadgen burst-drain 限制,非 fsdr)。
- 最终一致性:**count 281546=281546 ✅,hash=match ✅**。
- 结论:全链路(全量+CDC+混合写+校验)在当前量级下正确且无 lag。**worker 估算需在更高真实 QPS 下再标定**(需先修 loadgen)。
