# Telemetry & Statistics Guide

This document defines the telemetry metrics tracked, compiled, and logged by both the `BackfillStatsManager` (for the initial migration phase) and the `IncrementalStatsManager` (for the live incremental replication phase).

---

## 1. Change Stream (Live Replication) Stats

### 1. Queue Fullness
*   **What it measures**: The current buffer load (fill percentage) of the pipeline stage queues:
    *   **Ingest Queue**: Buffer utilization for events fetched by `source readers` waiting for the `partition router`.
    *   **Batching Queue**: Buffer utilization for events dispatched to `transformer and batchers`.
    *   **Batch Write Queue**: Buffer utilization for finalized batches waiting for `target writers` to commit.
*   **When it is measured**: Instantly snapshotted at the end of every statistics reporting interval (e.g., every 10 seconds).
*   **What it implies**:
    *   *High Ingest Queue Fullness*: The `partition router` is a CPU bottleneck and cannot dispatch events fast enough.
    *   *High Batching Queue Fullness*: The `transformer and batcher` threads are CPU-bound and cannot process events fast enough.
    *   *High Batch Write Queue Fullness*: The target database writers are waiting on commits, causing backlog.
*   **How to solve**:
    *   If Ingest is full: Enhance CPU allocations or review keys for hash collisions.
    *   If Batching is full: Increase the number of partition workers (`IncrementalStreamPartitions`).
    *   If Batch Write is full: Scale up target database write capacity or check for database lock contention.

### 2. Queue Delays
*   **What it measures**: The average time (latency) an event spends waiting inside channel queue buffers between processing stages:
    *   **Ingest Queue Delay**: Time waiting from ingestion (`source readers`) to routing (`partition router`).
    *   **Batching Queue Delay**: Time waiting from routing (`partition router`) to transformation/batching (`transformer and batcher`).
*   **When it is measured**: Stopwatch-timed on every single event at dequeue time.
*   **What it implies**: High delay indicates queue starvation or that consumer threads are congested, creating transit latency spikes.
*   **How to solve**:
    *   Increase consumer thread counts (`IncrementalStreamPartitions`).
    *   Profile consumer CPU loops for garbage collection stalls or lock contention.

### 3. Queue Stalls (Backpressure)
*   **What it measures**: The cumulative duration that producer threads are completely blocked because a downstream queue buffer is 100% full:
    *   **Batching Queue Stall**: Time the `partition router` was blocked waiting for a worker's input queue to open up.
    *   **Batch Write Queue Stall**: Time a `transformer and batcher` was blocked waiting for the DB execution channel (`batchWriteQueue`) to open up.
*   **When it is measured**: Stopwatch-timed when pushes block for greater than 1 millisecond.
*   **What it implies**: Severe backpressure. Downstream consumers are fully saturated, causing upstream stages to freeze.
*   **How to solve**:
    *   If *Batching Queue Stall* is high: A specific worker partition is choked. Check for high data/hash key skew (e.g., one document ID receiving millions of updates).
    *   If *Batch Write Queue Stall* is high: The target database is severely throttled. Scale target write capacity, optimize batch write sizes, or reduce index write hotspots.

### 4. Pipeline Lags
*   **Event-to-Read Lag**: Time elapsed between MongoDB Oplog commit and reader ingestion.
    *   *What it implies*: Network delays between MongoDB and the replicator, or high MongoDB CPU load stalling oplog retrieval.
    *   *How to solve*: Deploy the replicator closer to MongoDB or check source host resources. Increase the number of change streams to parallelize oplog reads.
*   **Read-to-worker-receive Lag**: Time an event spent waiting in pipeline queue buffers.
    *   *What it implies*: High delay indicates queue starvation or that consumer threads are congested, creating transit latency spikes.
    *   *How to solve*: Increase consumer thread counts (`IncrementalStreamPartitions`).
*   **Receive-to-Apply Lag**: Time elapsed from worker ingestion to target DB commit.
    *   *What it implies*: Target database commit latency.
    *   *How to solve*: Scale up target database write capacity.
*   **End-to-end Lag**: Total time from source oplog change to target database persistence.
    *   *What it implies*: Overall pipeline transit time.

### 5. BulkWrite Latency
*   **What it measures**: Commits write speed percentiles (p50, p90, p99, p100) and average latencies split by operation type (inserts, updates, deletes).
*   **When it is measured**: Stopwatch-timed around every bulk write execution.
*   **What it implies**: Slow p99/p100 indicates write contention, row lock conflicts, hot-spotting, or database indexing overhead.
*   **How to solve**:
    *   Avoid hotspots.
    * Scale up target database write capacity.
    *   Optimize batch sizes (`IncrementalWriteBatchSize`) to find the sweet spot for the target.

### 6. ChangeStream Read Performance
*   **What it measures**: Next-call latency of the MongoDB change stream cursors, both globally and per sharded partition.
*   **When it is measured**: On every sharded change stream `Next()` fetch.
*   **What it implies**: High next latency indicates slow network transit, source cursor exhaustion, or source database read bottlenecks.
*   **How to solve**: Optimize source MongoDB oplog capacity or increase network bandwidth. Add more change stream partitions to parallelize oplog reads.

### 7. Connection Pool Stats
*   **What it measures**: Opened/closed socket counts, open/in-use socket loads, checkouts checkouts, and checkout wait delays for both source and target database connections.
*   **When it is measured**: Hooked into MongoDB Driver connection pool events.
*   **What it implies**: High Checkout Wait times indicate that the thread pool is starved for database connections (blocking waiting for an available socket).
*   **How to solve**: Increase the connection pool maximum size limit (`maxPoolSize`) in connection string configurations.

### 8. Worker Throughput (QPS) Distribution
*   **What it measures**: Throughput statistics across all active workers (active worker count, p50, p90, p99, and p100 QPS).
*   **When it is measured**: Aggregated lock-freely on worker event executions and calculated during stats reporting.
*   **What it implies**: Significant differences between p50 and p100 worker throughput indicate key distribution hotspots or unequal partition load skew.
*   **How to solve**: Review key hashing logic to ensure documents are distributed uniformly across workers.

### 9. Group Flushes
*   **What it measures**: Count of batch flushes categorized by their trigger reasons:
    *   `optype`: Flush triggered by an incoming event of a different DML operation type (e.g., delete after insert), when groupByDistinctId is disabled.
    *   `namespace`: Flush triggered by an event from a different collection.
    *   `batchfull`: Flush triggered because the batch size limit was reached.
    *   `collision`: Flush triggered due to document ID collision, when groupByDistinctId is enabled.
    *   `timeout`: Flush triggered because the flush interval duration elapsed.
*   **When it is measured**: On every batch write queue dispatch.
*   **What it implies**: A high count of `timeout` or `optype` flushes indicates that batches are being flushed before they are full, reducing bulk write optimization efficiency.
*   **How to solve**:
    *   If `timeout` flushes are high: The stream throughput is low. This is normal and expected during inactive periods.

### 10. Sequential Retries & Error Tracking
*   **What it measures**: Cumulative sequential retries, duplicate key errors, and structured breakdowns of database execution errors.
*   **When it is measured**: Recorded upon database commit failures.
*   **What it implies**: High sequential retries indicate persistent lock contention, deadlocks, or network drops.
*   **How to solve**: Increase Spanner/Firestore transaction retry timeout limits, optimize write batches to decrease row locking overlap, or review the Dead Letter Queue (DLQ) to examine persistently unprocessable payloads.

---

## 2. Initial Backfill Stats

### 1. Progress & Remaining Time
*   **What it measures**: Overall percentage progress, total processed vs target document count, and the estimated remaining duration (Remaining) until backfill completion.
*   **When it is measured**: Continuously updated during backfill writes and computed at statistics intervals using the overall throughput rate.
*   **What it implies**: Real-time visibility into the backfill phase progress.

### 2. Ingestion Stalls (Reader Backpressure)
*   **What it measures**: The cumulative duration that reader threads (or cursor partition loops) spent blocked waiting to push read document batches into worker channel buffers.
*   **When it is measured**: Stopwatch-timed when writing to the worker channel blocks.
*   **What it implies**: Severe write-path bottlenecks. Downstream workers are saturated writing to the target database, causing the document read cursor to stall.
*   **How to solve**: Increase target database write capacity, adjust worker parallelism, or inspect Spanner/Firestore commit queue locks.

### 3. Read Performance
*   **What it measures**: Performance of document retrieval from the source database:
    *   **Fetch Latency**: MongoDB cursor next-call fetch latency percentiles (p50, p99, p100).
    *   **Document Size**: The average document size in bytes, total bytes read, and overall read throughput rate (MB/sec).
*   **What it implies**: Slow fetch latency indicates reading bottlenecks on the source database or network. High document sizes imply larger payload serialization/deserialization overhead.

### 4. BulkWrite Latency & Worker QPS
*   **What it measures**: Target database bulk write commit times (p50, p90, p99, p100) and active worker write QPS distributions.
*   **What it implies**: Identifies write latency bottlenecks on Spanner/Firestore and worker load distribution/contention.

### 5. Error & DLQ Tracking
*   **What it measures**: Cumulative duplicate key errors and DLQ'ed document counts.
*   **What it implies**: Tracks skip counts of duplicate documents (Case 1) and writes to Dead Letter Queue for unprocessable payloads (Case 2).

### 6. Connection Pool Performance
*   **What it measures**: Real-time socket utilization, checkout succeed/fail rates, and average checkout wait latency for both source and target connections during backfill.
*   **What it implies**: Identifies connection pool starvation and socket leaks during heavy migration traffic.

