# Configuration & CLI Reference

> This is the detailed configuration and command-line reference. For an overview, requirements, supported versions, and the **web console** (the recommended way to run a migration), see the main [README](../README.md).
>
> Most operators never need this file — the [web console](../README.md#web-console) builds the configuration from a form and runs the migration in-process. Use this reference when driving the tool from the CLI with a hand-written config file, or when tuning advanced parameters.

## Table of Contents

- [Installation](#installation)
- [Configuration File](#configuration-file)
  - [Full Database Migration (Automatic Collection Detection)](#full-database-migration-automatic-collection-detection)
  - [Global Database-Level Upsert](#global-database-level-upsert)
  - [Specific Collections Migration](#specific-collections-migration)
  - [Configuration Options](#configuration-options)
  - [Index Synchronization Configuration](#index-synchronization-configuration)
  - [Index-Only Replication](#index-only-replication)
  - [Replication Method Configuration](#replication-method-configuration)
- [CLI Usage](#cli-usage)
- [Performance & Internals](#performance--internals)
- [Setting Up a Single-Node Replica Set for Development](#setting-up-a-single-node-replica-set-for-development)
- [Project Structure](#project-structure)

## Installation

1. Clone this repository:

   ```bash
   git clone https://github.com/gsbingo17/mongodb-to-new.git
   cd mongodb-to-new
   ```

2. Build the application:

   ```bash
   go mod tidy
   go build -o migrate ./cmd/migrate
   ```

## Configuration File

Create a `mongodb_replication_config.json` file: This file defines the replication settings, including the source and target MongoDB connection details. A complete sample configuration file with all available options is provided in `sample_config.json`.

Here's a basic example:

### Full Database Migration (Automatic Collection Detection)

```json
{
  "databasePairs": [
    {
      "source": {
        "connectionString": "mongodb://localhost:27017?replicaSet=rs0",
        "database": "source_db"
      },
      "target": {
        "connectionString": "mongodb://localhost:27017",
        "database": "target_db"
      }
    }
  ],
  "saveThreshold": 1000,
  "checkpointInterval": 5,
  "forceOrderedOperations": false,
  "flushIntervalMs": 500
}
```

When no collections are specified, the tool will automatically detect all collections in the source database and migrate them to the target database with the same collection names.

### Global Database-Level Upsert

If you want to enable upsert mode globally for all collections (both explicitly mapped and auto-detected collections), you can specify `"upsertMode": true` at the target database level:

```json
{
  "databasePairs": [
    {
      "source": {
        "connectionString": "mongodb://localhost:27017/?replicaSet=rs0",
        "database": "source_db"
      },
      "target": {
        "connectionString": "mongodb://localhost:27017",
        "database": "target_db",
        "upsertMode": true
      }
    }
  ],
  "saveThreshold": 1000
}
```

### Specific Collections Migration

If you want to migrate only specific collections or rename collections during migration, you can specify them explicitly:

```json
{
  "databasePairs": [
    {
      "source": {
        "connectionString": "mongodb://localhost:27017/?replicaSet=rs0",
        "database": "source_db"
      },
      "target": {
        "connectionString": "mongodb://localhost:27017",
        "database": "target_db",
        "collections": [
          {
            "sourceCollection": "source_collection",
            "targetCollection": "target_collection",
            "upsertMode": true
          }
        ]
      }
    }
  ],
  "saveThreshold": 1000
}
```

### Configuration Options

#### Database Configuration
- **databasePairs**: An array of objects, each defining a source MongoDB database and a target MongoDB database to replicate.
- **connectionString**: The MongoDB connection string for source and target databases.
- **database**: The name of the MongoDB database for source and target.
- **upsertMode**: (Optional, target-level) Whether to use upsert operations globally by default for all collections in this target database instead of standard inserts. Default is false.
- **collections**: (Optional) An array of objects, each defining a source MongoDB collection and a target MongoDB collection to replicate. If omitted, all collections will be migrated with the same names.
  - **sourceCollection**: The name of the collection in the source database.
  - **targetCollection**: The name of the collection in the target database.
  - **upsertMode**: (Optional) Whether to use upsert operations instead of inserts for this specific collection (overrides/complements the database-level default). Default is false.

#### Checkpoint Configuration
- **saveThreshold**: The number of changes to process before saving the resume token (for live replication).
- **checkpointInterval**: The time interval in minutes to save the resume token regardless of the number of changes (default: 5).

#### Performance Configuration
- **initialReadBatchSize**: Number of documents to read in a batch during initial migration (default: 8192).
- **initialWriteBatchSize**: Number of documents to write in a batch during initial migration (default: 128).
- **initialChannelBufferSize**: Size of channel buffer for batches during initial migration (default: 10).
- **initialMigrationWorkers**: Number of worker goroutines for batch processing during standard migration (default: 5).
- **concurrentCollections**: Number of collections to process concurrently (default: 4).
- **incrementalReadBatchSize**: Number of change events to read at once (default: 8192).
- **incrementalStreamPartitions**: Number of parallel sharded change stream readers at MongoDB source level (default: 1).
- **incrementalWriteBatchSize**: Maximum size of operation groups (default: 128).
- **incrementalWorkerCount**: Number of worker goroutines for incremental replication (default: number of CPU cores).
- **statsIntervalMinutes**: Interval for reporting change stream statistics in minutes (default: 5).
- **groupOpsByDistinctId**: Enable key-collision grouping in live replication instead of optype-based grouping (default: false).
- **flushIntervalMs**: Flush interval in milliseconds for operation groups (default: 500).
- **targetMinPoolSize**: Minimum MongoDB connection pool size for target database (default: 128).
- **targetMaxPoolSize**: Maximum MongoDB connection pool size for target database (default: 256).
- **incrementalIncomingQueueSize**: Buffer size of the concurrent workers' raw events queue channel (default: 8192).
- **incrementalProcessingQueueSize**: Buffer size of the concurrent workers' writing batches queue channel (default: 4096). Bounding this to a small number (e.g. 2 or 4) applies strict in-memory backpressure, preventing memory backups and capping Queue Latency under slow writes.
- **forceOrderedOperations**: Whether to force ordered operations for all operation types (default: false). When false, insert and delete operations use unordered bulk writes for better performance, while update and replace operations always use ordered bulk writes to ensure consistency.

#### Write Ramp-Up Configuration (Initial Backfill Throttling)
- **backfillRampUp**: Configuration for linear write QPS throttling during the initial migration (backfill) phase to prevent overloading target databases (e.g. Cloud Spanner) before autoscaling reacts.
  - **enabled**: Set to `true` to enable write QPS throttling (default: `false`).
  - **startQps**: The initial QPS rate limit from which the migration writes start (default: `0.0`).
  - **rampRatePerMin**: The linear rate (QPS increase per minute) at which the throttler ceiling grows. Set to `0` or omit to allow the write speed limit to grow linearly forever without any ceiling cap. (default: `10000.0` - reaching 100K QPS in 10 minutes). Note: The throttler automatically disables itself (sets limit to Infinity) once the allowed rate reaches 100K QPS.
  - **updateIntervalMs**: The interval in milliseconds at which the throttler background worker recalculates and applies the new rate limit (default: `1000` ms / 1 second).

#### Parallel Reads Configuration
- **parallelReadsEnabled**: Enable parallel reads for large collections (default: true).
- **idTypeForPartition**: BSON Partitioning ID strategy type for collection partitioning. Supported values:
  - `"auto"` (default): Automatically detects strategy by sampling `sampleSize` documents and choosing `"objectid"`, `"numeric"`, or `"mixed"` accordingly.
  - `"mixed"`: Generic range-sampling strategy suitable for collections with mixed ID types.
  - `"objectid"`: Optimized strategy for collections where `_id` values are mainly BSON `ObjectID`s.
  - `"numeric"`: Optimized strategy for collections where `_id` values are numbers (integers or doubles).
- **maxReadPartitions**: Maximum number of partitions for parallel reads (default: 8).
- **minDocsPerPartition**: Minimum number of documents per partition (default: 10000).
- **minDocsForParallelReads**: Minimum collection size for parallel reads (default: 50000).
- **sampleSize**: Number of documents to sample for partitioning (default: 1000).
- **workersPerPartition**: Number of worker goroutines per partition for parallel batch processing (default: 3).

#### Retry Configuration
- **retryConfig**: Configuration for retry mechanisms.
  - **maxRetries**: Maximum number of retries (default: 5).
  - **baseDelayMs**: Base delay in milliseconds (default: 100).
  - **maxDelayMs**: Maximum delay in milliseconds (default: 5000).
  - **enableBatchSplitting**: Enable batch splitting for contention errors (default: true).
  - **minBatchSize**: Minimum batch size for splitting (default: 10).
  - **convertInvalidIds**: Automatically convert invalid `_id` types to string (default: `true`).
    - **In Live Paths (Live Backfill & Live Incremental streaming):** Proactively detects unsupported `_id` datatypes before writing to the target, and serializes them into deterministic type-prefixed strings.
      * *Example BSON ID conversion:*
        - **Source ID:** `_id: [1, 2] (Array)`
        - **Target ID:** `_id: "_converted:array:[1,2]" (String)`
      * *Supported type mappings:* `bool` (`_converted:bool:`), `int` (`_converted:int:`), `int32` (`_converted:int32:`), `double` (`_converted:double:`), `float` (`_converted:float:`), `datetime` (`_converted:datetime:`), `binary` (`_converted:binary:`), `array` (`_converted:array:`), `document` (`_converted:document:`).
    - **In Normal Backfill (`-mode=migrate`):** Reactively catches database write failures (due to invalid `_id` types), splits the batch, converts failing invalid `_id` values to string using simple formatting, and retries.
      * *Example BSON ID conversion:*
        - **Source ID:** `_id: [1, 2] (Array)`
        - **Target ID:** `_id: "[1 2]" (String)`

#### Field Transformation Configuration
- **dropEmptyFieldNames**: Automatically remove empty field names (e.g., `""`) from document keys to satisfy target compatibility (default: `false`).
  * *Example transformation:*
    - **Source payload:** `{ "_id": "test", "": "empty-val", "name": "user" }`
    - **Target payload:** `{ "_id": "test", "name": "user" }`
- **convertLongFieldNamesInNestedDocs**: Automatically stringify nested subdocuments containing field names exceeding 1,000 characters to a JSON string to satisfy target key-length compatibility constraints (default: `false`).
  * *Example transformation:*
    - **Source payload:** `{ "nested": { "<long_key_1001_chars>": "value" } }`
    - **Target payload:** `{ "nested": "{\"<long_key_1001_chars>\":\"value\"}" }`

### Index Synchronization Configuration
- **syncAllIndexes**: (Optional) When set to `true`, automatically syncs all indexes (excluding `_id_`) from every source collection to the corresponding target collection. Default is `false`.
- **indexOnly**: (Optional) When set to `true`, the tool **only syncs indexes** and skips all data migration and incremental replication. The process exits after all indexes are created. Must be used with `syncAllIndexes: true` or explicit `indexes` configuration. Default is `false`.
- **indexConcurrency**: (Optional) Maximum number of concurrent asynchronous index builds (default: 1). Use `1` for Firestore targets to serialize builds and avoid cross-transaction contention. Use a higher value (e.g., `4`) for regular MongoDB targets to speed up index creation.
- **indexes**: (Optional) An array of index configurations for synchronizing specific indexes from source to target collections.
  - **sourceCollection**: The name of the source collection containing the indexes to sync.
  - **indexNames**: An array of index names to synchronize (the tool will retrieve the full index definitions from the source).

**Index Sync Behavior:**
- Index synchronization occurs **only during initial migration** (not during incremental replication)
- Indexes are created on the target collection **before data migration** begins
- **Skip existing indexes**: If an index already exists on the target collection, it is skipped (no duplicate creation attempts)
- The tool automatically resolves the target collection name:
  - If a mapping is defined in the `collections` configuration, it uses the mapped target collection name
  - If no mapping is found, it assumes the target collection has the same name as the source collection
- **Non-blocking errors**: If index creation fails, the tool logs a warning and continues with data migration
- **Preserves existing indexes**: Indexes already present on the target collection that don't exist in the source are kept unchanged
- The `_id_` index is automatically skipped as it's created by MongoDB
- **Async with configurable concurrency**: Index builds are launched asynchronously with a configurable concurrency limit (controlled by `indexConcurrency`, default 1). Use `1` for Firestore targets to serialize builds and avoid cross-transaction contention; use a higher value for regular MongoDB targets. Each build uses a dedicated client with no socket timeout so long-running index builds are not killed.
- **Compound index key order preservation**: When reading index definitions from the source, the tool preserves the exact field order of compound indexes (e.g., `{a:1, b:1}` is kept distinct from `{b:1, a:1}`). This is critical for compound indexes where field order determines query optimization.
- **TTL index support**: TTL indexes (`expireAfterSeconds`) are correctly handled regardless of the BSON numeric type returned by the source (int32, int64, or float64).
- **Failed index tracking**: Any index that fails to be created (due to connection errors, context cancellation, contention exhaustion, or unsupported index types) is recorded in an internal tracking list. After all index builds complete, a summary of failed indexes is logged with collection name, index name, and error details.

> **Note (web console):** When migrating with the web console, the `_id` index is **always created** on every target collection regardless of `syncAllIndexes`, and secondary index sync is controlled by a UI toggle. Indexes are built **after** data. See the [Web Console](../README.md#web-console) section and [`CONSOLE_UI_DESIGN.md`](../CONSOLE_UI_DESIGN.md) §7.

**Example Configuration:**
```json
{
  "databasePairs": [
    {
      "source": {
        "connectionString": "mongodb://localhost:27017/?replicaSet=rs0",
        "database": "source_db"
      },
      "target": {
        "connectionString": "mongodb://localhost:27017",
        "database": "target_db",
        "collections": [
          {
            "sourceCollection": "users",
            "targetCollection": "app_users"
          }
        ],
        "indexes": [
          {
            "sourceCollection": "users",
            "indexNames": ["email_1", "created_at_-1"]
          },
          {
            "sourceCollection": "orders",
            "indexNames": ["user_id_1", "status_1_created_at_-1"]
          }
        ]
      }
    }
  ]
}
```

In this example:
- The `email_1` and `created_at_-1` indexes from the `users` collection will be created on the `app_users` collection (following the collection mapping)
- The `user_id_1` and `status_1_created_at_-1` indexes from the `orders` collection will be created on the `orders` collection (same name, no mapping)

### Index-Only Replication

If you want to **only sync indexes** without migrating any data, set `indexOnly` to `true`. This is useful when:
- You want to pre-create indexes on the target before running a full migration
- You need to sync indexes independently of data migration
- You want to verify index compatibility with the target (e.g., Firestore)

```json
{
  "databasePairs": [
    {
      "source": {
        "connectionString": "mongodb://localhost:27017/?replicaSet=rs0",
        "database": "source_db",
        "replicationMethod": "oplog-legacy"
      },
      "target": {
        "connectionString": "mongodb://target:27017",
        "database": "target_db",
        "syncAllIndexes": true,
        "indexOnly": true
      }
    }
  ],
  "indexConcurrency": 1
}
```

**Index-Only Replication Behavior:**
- Works with all modes: `migrate`, `changestream`, `oplog`, and `oplog-legacy`
- Reads all index definitions from the source database
- Skips indexes that already exist on the target (no duplicate creation)
- Creates indexes asynchronously with configurable concurrency (`indexConcurrency`, default 1 for Firestore safety)
- Waits for all index builds to complete before exiting
- **Failed index summary**: After all builds complete, any failed indexes are logged with collection name, index name, and error details
- **No data is migrated** — only index definitions are synced
- **No incremental replication** — the process exits after indexes are created (no oplog tailing or change stream)

You can run it with either mode:
```bash
# Using migrate mode (simplest — no replica set needed for target)
./migrate -mode=migrate

# Using live mode (will sync indexes and exit without starting replication)
./migrate -mode=live
```

### Replication Method Configuration
- **replicationMethod**: (Optional) Specifies the replication method for live mode. Possible values:
  - `"changestream"` (default): Uses MongoDB change streams for incremental replication (requires MongoDB 3.6+ with replica set)
  - `"oplog"`: Uses MongoDB oplog tailing for incremental replication (works with older MongoDB versions)

> For the version/wire-protocol prerequisites of each replication method and a quick decision guide, see the main [README → Supported Source Versions & Replication Methods](../README.md#supported-source-versions--replication-methods).

**Oplog-Based Replication:**

For databases that don't support change streams or for legacy MongoDB versions, you can use oplog-based replication:

```json
{
  "databasePairs": [
    {
      "source": {
        "connectionString": "mongodb://legacy:27017/?replicaSet=rs0",
        "database": "legacy_db",
        "replicationMethod": "oplog"
      },
      "target": {
        "connectionString": "mongodb://localhost:27017",
        "database": "new_db",
        "collections": [
          {
            "sourceCollection": "orders",
            "targetCollection": "orders"
          }
        ]
      }
    }
  ]
}
```

**Legacy MongoDB Support (MongoDB 3.0/3.2):**

For very old MongoDB versions (3.0, 3.2) that use wire protocol version 3, use the `oplog-legacy` replication method:

```json
{
  "databasePairs": [
    {
      "source": {
        "connectionString": "mongodb://oldserver:27017/?replicaSet=rs0",
        "database": "legacy_db",
        "replicationMethod": "oplog-legacy"
      },
      "target": {
        "connectionString": "mongodb://newserver:27018",
        "database": "modern_db"
      }
    }
  ]
}
```

When no `collections` are specified (as above), the tool will automatically detect all collections in the source database using the legacy mgo driver and migrate them to the target database with the same collection names. You can also specify explicit collection mappings if you want to rename collections during migration:

```json
{
  "databasePairs": [
    {
      "source": {
        "connectionString": "mongodb://oldserver:27017/?replicaSet=rs0",
        "database": "legacy_db",
        "replicationMethod": "oplog-legacy"
      },
      "target": {
        "connectionString": "mongodb://newserver:27018",
        "database": "modern_db",
        "collections": [
          { "sourceCollection": "users", "targetCollection": "app_users" },
          { "sourceCollection": "orders", "targetCollection": "orders" }
        ]
      }
    }
  ]
}
```

**Legacy Mode Implementation:**
- Uses **mgo driver** for source MongoDB (supports wire version 3)
- Uses **modern mongo-driver** for target MongoDB (supports wire version 12+)
- Both drivers coexist in the same binary without conflicts
- Leverages GTM legacy library with mgo for oplog tailing
- Supports full initial migration + incremental replication
- Perfect for migrating from MongoDB 3.0/3.2 to modern MongoDB/Firestore

**When to Use oplog-legacy:**
- Source MongoDB version 3.0, 3.2, or 3.4 (wire version 3)
- Target MongoDB is modern version (3.6+ or wire version 6+)
- You need to bridge the gap between very old and very new MongoDB versions

**When to Use Oplog Replication (Standard):**
- Source database doesn't support change streams
- Migrating from MongoDB versions earlier than 3.6
- Source MongoDB doesn't have change streams enabled
- You need lower-level access to the operation log

**Oplog Replication Behavior:**
- Requires source MongoDB to be running as a replica set (oplog only exists on replica sets)
- Uses the GTM (Go Tail Mongo) library for robust oplog tailing
- Automatically handles reconnection and resume from last processed timestamp
- Stores resume position in `oplogTimestamp-global.json` file
- Same seamless initial + incremental migration flow as change streams:
  1. Captures current oplog timestamp before initial migration
  2. Performs full initial migration (including index sync if configured)
  3. Starts tailing oplog from captured timestamp to catch all changes during migration
- Filters operations to only process configured collections
- Supports insert, update, and delete operations
- Automatically converts oplog operations to unified event format

**Oplog vs Change Streams:**

| Feature | Change Streams | Oplog |
|---------|---------------|-------|
| MongoDB Version | 3.6+ | All versions with replica set |
| API Level | High-level, structured events | Low-level, raw oplog entries |
| Server-side Filtering | Yes | No (filtered client-side) |
| Resume Token | Opaque binary token | Timestamp-based |
| Recommended For | Modern MongoDB (3.6+) | Legacy MongoDB or special cases |

## CLI Usage

> The web console (`./migrate -mode=console`) covers `assess`, `migrate`/`live`, and `verify` from the browser without any of the flags below. See the main [README → Web Console](../README.md#web-console). The modes here are for scripting, CI, and advanced use.

1. Migrate Mode:

   To perform a one-time migration of data from source MongoDB to target MongoDB:

   ```bash
   ./migrate -mode=migrate
   ```

2. Live Mode:

   To set up live replication using MongoDB change streams (runs initial data migration and then transitions to continuous replication):

   ```bash
   ./migrate -mode=live
   ```

   The application will continuously listen for changes in the specified MongoDB collections and replicate them to the target MongoDB.

3. Live-Only Mode:

   To perform real-time incremental replication only, skipping the initial data copy phase:

   ```bash
   ./migrate -mode=live-only
   ```

   You can also specify a custom historical starting point:
   ```bash
   ./migrate -mode=live-only -live-start-timestamp=2026-05-20T21:00:00Z
   ```

4. Capture Resume Token Mode:

   To pre-capture the current MongoDB Change Stream resume token at time $T_0$ before launching an external or decoupled backfill (e.g. `mongodump`/`mongorestore`):

   ```bash
   ./migrate -mode=capture-resume-token
   ```

   This connects to the source MongoDB, extracts the present resume token, writes checkpoint files (`resumeToken-*.json` and `initialMigrationState-*.json`), and exits immediately with code `0`. Once the external backfill finishes, you can start live CDC seamlessly with:
   ```bash
   ./migrate -mode=live-only
   ```

5. Retry DLQ Mode:

   To reprocess records from Dead Letter Queue (DLQ) files:

   ```bash
   ./migrate -mode=retry-dlq
   ```

   Add `-dlq-resync-from-source` to re-read each failed document fresh from the SOURCE by `_id` (picking up a source-side fix) instead of replaying the stored DLQ snapshot; source-deleted docs are treated as resolved.

6. Assess Mode:

   To run a read-only pre-migration compatibility assessment against the source and target, print the report, and exit non-zero if any hard-blocking issue is found:

   ```bash
   ./migrate -mode=assess
   ```

   Assess honors a saved `remediation-plan.json` so the CLI check matches the console.

7. Verify Mode:

   To run a post-migration verification comparing source vs target document counts, print the report, and exit non-zero on any discrepancy:

   ```bash
   ./migrate -mode=verify

   # Also compare document content hashes and reconcile converted _ids.
   ./migrate -mode=verify -verify-hash -id-map=id-mapping.jsonl
   ```

8. Wizard Mode:

   To interactively generate a `mongodb_replication_config.json` (or the `-config` path) and exit:

   ```bash
   ./migrate -mode=wizard
   ```

9. Console Mode:

   To serve the browser-based control console (default `:9090`, override with `-metrics-addr`):

   ```bash
   ./migrate -mode=console
   ```

   See the main [README → Web Console](../README.md#web-console) for the full walkthrough.

10. Dry Run and Compatibility Reports:

    To dry run a migration (which runs connectivity and compatibility validation, checks target database support for various datatypes/key limits, and samples source collections to output partitioning recommendations):

    ```bash
    ./migrate -mode=migrate -dry-run
    ```

    **How it works:**
    - **Target Connection & Validation:** The tool connects to the target database specified in the configuration file to verify connectivity and compatibility. It prints target support reports for `_id` types, long nested fields, and empty key names without writing actual records.
    - **In Backfill Modes (`-mode=migrate` or `live` initial phase):** It runs target checks, samples a small subset of documents (e.g. 1000) from source collections to recommend partition boundaries and ID strategy configurations, and exits immediately. It skips scanning/reading full collection payloads.
    - **In Incremental Modes (`-mode=live-only` or `live` streaming phase):** It starts change stream/oplog replication threads, pulls events from source database partitions, and outputs real-time ingestion lag stats. However, it discards events before formatting/sending writes to the target. This is useful for safely measuring network pre-fetching performance and verifying that the configured sharded change stream partitioning (`incrementalStreamPartitions`) functions correctly on active production setups.
    - **Safety:** No write operations or state checkpoints are committed to either source or target systems.

11. Additional Options:

    ```bash
    ./migrate -help
    ```

    This will display all available command-line options:

    ```
    Options:
      -config string
            Path to configuration file (default "mongodb_replication_config.json")
      -mode string
            Operation mode: 'migrate', 'live', 'live-only', 'retry-dlq', 'capture-resume-token',
            'console', 'wizard', 'assess', or 'verify' (default "migrate")
      -log-level string
            Log level: debug, info, warn, error (default "info")
      -log-file string
            Path to log file (logs to both stdout and file when specified)
      -live-start-timestamp string
            Start timestamp for live-only replication (Unix epoch seconds or RFC3339 format)
      -metrics-addr string
            If set (e.g. ":9090"), serve the control plane: /healthz, /readyz, /metrics, the status
            API, and the dashboard UI. In console mode, the listen address (default ":9090")
      -verify-hash
            In verify mode, also compare document content hashes (not just counts)
      -id-map string
            In verify mode, path to the id-mapping JSONL (to reconcile converted _ids)
      -dlq-resync-from-source
            In retry-dlq mode, re-read each failed document fresh from the SOURCE by _id instead of
            replaying the stored DLQ snapshot; source-deleted docs are treated as resolved
      -dry-run
            Dry run mode (skips writes, outputs partitioning recommendations on backfill)
      -help
            Display this help information
    ```

## Performance & Internals

### Multi-level Parallelism

The application implements parallelism at multiple levels to maximize performance:

1. **Collection-Level Parallelism**:
   - Multiple collections are processed concurrently
   - Controlled by the `concurrentCollections` parameter (default: 4)
   - Each collection is processed in its own goroutine
   - A semaphore limits the maximum number of concurrent collections
   - Higher values allow more collections to be migrated simultaneously

2. **Batch-Level Parallelism in Standard Migration**:
   - For collections that don't use partitioning (smaller collections)
   - Controlled by the `initialMigrationWorkers` parameter (default: 5)
   - Documents are read sequentially but processed in batches by multiple workers
   - Each worker processes batches in parallel

3. **Partition-Level Parallelism**:
   - For large collections (size >= `minDocsForParallelReads`)
   - The collection is divided into partitions based on document ID ranges
   - Controlled by the `maxReadPartitions` parameter (default: 8)
   - Each partition is processed in its own goroutine with its own cursor
   - Partitions are created using sampling to ensure even distribution

4. **Batch-Level Parallelism within Partitions**:
   - Within each partition, batches are processed by multiple workers
   - Controlled by the `workersPerPartition` parameter (default: 3)
   - Documents are read sequentially within each partition but processed in parallel
   - Provides an additional level of parallelism for large collections

5. **Change Stream Parallelism** (Live Mode):
   - **Sharded Ingestion Partitions**: Controlled by `incrementalStreamPartitions`. The system spawns parallel sharded change streams at the MongoDB source level
   - **Hashing-based Server-Side Filtering**: At ingestion time, the system splits the change streams lock-freely using a server-side modulo hash filter on document ID values:
     `hash(documentKey._id) % totalPartitions == partitionIndex`
     This ensures that each of the parallel change streams receives a completely disjoint, non-overlapping subset of oplog events, enabling parallelized high-throughput ingestion
   - **Worker Hash Distribution**: Within the replicator, the `partition router` further distributes events across `transformer and batcher` worker threads (controlled by `incrementalWorkerCount`) using document ID key hashing
   - **Sequential Consistency**: This ensures that all operations for the same document ID always go to the same worker and are processed in their strict chronological sequence

### Tuning Parallelism Parameters

For optimal performance, consider these guidelines:

1. **concurrentCollections**:
   - Set based on the number and size of collections
   - Higher values process more collections simultaneously
   - Consider memory constraints when setting this value
   - For systems with many small collections, higher values (8-16) may improve throughput
   - For systems with few large collections, lower values (2-4) may be more efficient

2. **initialMigrationWorkers**:
   - Set based on available CPU cores and I/O capacity
   - Controls batch processing parallelism for standard migration
   - For CPU-bound workloads: set to number of available cores
   - For I/O-bound workloads: can be set higher than available cores

3. **maxReadPartitions**:
   - Controls how many partitions large collections are divided into
   - Higher values create more partitions but with smaller document ranges
   - Optimal values typically range from 4-16 depending on collection size

4. **workersPerPartition**:
   - Controls batch processing parallelism within each partition
   - For balanced resource allocation: total_cores ÷ maxReadPartitions
   - Avoid setting too high to prevent contention within partitions

> The web console's assessment recommends a tuning set (workers, partitions, granularity) based on the host CPU count and the measured data volume, and pre-fills these inputs; you can override them before starting.

### Enhanced Checkpoint Mechanism

The application implements a robust checkpoint mechanism using a single client-level resume token:

1. **Initial Replication Process**:
   - When starting in live mode, the tool first checks for an existing global resume token
   - If a resume token exists, it begins incremental replication immediately from that point
   - If no resume token exists (new replication):
     1. The tool creates a change stream and obtains an initial resume token
     2. It performs a full migration of all collections
     3. After full migration completes, incremental replication starts using the initial resume token, capturing all changes that occurred after the initial migration

2. **Client-Level Resume Token**:
   - A single global resume token is used for the client-level change stream
   - This token acts as a checkpoint that covers all databases and collections
   - Stored in a file named `resumeToken-global.json`
   - Automatically backed up before being updated to prevent corruption
   - Dual checkpoint timing mechanism:
     - **Count-based checkpoints**: Save after processing the number of changes specified by `saveThreshold`
     - **Time-based checkpoints**: Save at the interval specified by `checkpointInterval` (in minutes) regardless of the number of changes

3. **Failure Recovery Process**:
   - If replication fails or the process is interrupted:
     1. On restart, the tool loads the last saved global resume token
     2. Replication resumes precisely from the last checkpoint
     3. No data is lost or duplicated during the recovery

### Parallel Processing in Live Mode

The application implements a sophisticated parallel processing system for change stream events in live mode:

1. **Hash-Based Distribution**: Operations are distributed to workers based on document ID hash, ensuring that operations for the same document always go to the same worker.

2. **Data-Driven Processing**: Within each worker, operations are grouped by namespace and operation type. A new group is created whenever:
   - The operation type changes
   - The namespace changes
   - The current group reaches the maximum size

3. **Sequential Group Processing**: Groups are processed in strict sequential order within each worker, ensuring data consistency.

4. **Optimized Bulk Writes**: Operations within a group are executed as bulk writes:
   - Insert and delete operations use unordered bulk writes for better performance
   - Update and replace operations use ordered bulk writes to ensure consistency
   - The `forceOrderedOperations` configuration option can force ordered operations for all types

5. **Efficient Error Handling**: If a bulk operation fails, the system falls back to individual operations for the failed items, ensuring robustness.

### Parallel Reads for Large Collections

For large collections, the application uses parallel reads to speed up the initial migration:

1. **Intelligent Partitioning**: The collection is partitioned based on the _id field type:
   - For ObjectIDs: Uses timestamp-based or sampling-based partitioning
   - For numeric IDs: Uses range-based or sampling-based partitioning
   - For other types: Uses hash-based partitioning with the $mod operator

2. **Adaptive Partition Count**: The number of partitions is calculated based on collection size and configuration parameters.

3. **Two-Level Parallelism**:
   - **Partition-Level Parallelism**: Each partition is processed in parallel, with its own cursor
   - **Batch-Level Parallelism**: Within each partition, multiple worker goroutines process batches in parallel
   - **Configurable Worker Count**: The number of workers per partition can be configured using the `workersPerPartition` parameter

4. **Efficient Batch Distribution**: Within each partition, batches are distributed to workers through channels, allowing for optimal resource utilization.

### Robust Retry Mechanism

The application includes a sophisticated retry mechanism for handling errors:

1. **Error Classification**: Errors are classified into different types:
   - Connection errors: Network-related issues
   - Contention errors: Lock timeouts, write conflicts, etc.
   - Other errors: Any other type of error

2. **Exponential Backoff**: Retries use exponential backoff with jitter to avoid thundering herd problems.

3. **Batch Splitting**: For contention errors, batches are progressively split to reduce contention.

4. **Special Handling**: Different error types receive specialized handling:
   - Contention errors: Fixed delay before retry
   - Duplicate key errors: Automatic fallback to upsert operations
   - Connection errors: Exponential backoff with the full batch
   - Invalid _id type errors: Automatic conversion of _id fields to strings when enabled

5. **_id Type Conversion**: When `convertInvalidIds` is enabled:
   - Detects errors like "_id must be an objectId, string, long; found int"
   - Automatically converts problematic _id fields to strings
   - Logs the conversion details for troubleshooting
   - Retries the operation with the converted _id fields
   - Only converts _id fields that cause errors, preserving the original types when possible

## Setting Up a Single-Node Replica Set for Development

If you're developing locally and want to test the live replication feature, you can set up a single-node replica set:

1. Start MongoDB with the replica set option:

   ```bash
   mongod --replSet rs0 --dbpath /path/to/data/directory
   ```

2. Initialize the replica set:

   ```bash
   mongosh
   > rs.initiate()
   ```

3. Verify the replica set status:

   ```bash
   > rs.status()
   ```

This will allow you to use change streams, which are required for the live replication feature.

## Project Structure

- `cmd/migrate/`: Contains the main application entry point.
- `pkg/config/`: Configuration handling.
- `pkg/db/`: MongoDB connection and operations.
- `pkg/logger/`: Logging utilities.
- `pkg/migration/`: Migration and replication logic.
  - `client_stream.go`: Client-level change stream implementation
  - `oplog_replicator.go`: Oplog-based replication implementation using GTM
  - `oplog_timestamp.go`: Oplog timestamp tracking and persistence
  - `oplog_converter.go`: GTM operation to event conversion
  - `migrator.go`: Core migration and replication logic
  - `resumetoken.go`: Resume token management
  - `parallel.go`: Parallel processing implementation for live mode
  - `parallel_read.go`: Parallel read implementation for large collections
  - `retry.go`: Retry mechanisms with exponential backoff and batch splitting
- `pkg/assess/`: Read-only pre-migration compatibility assessment and tuning recommendation.
- `pkg/remediate/`: Remediation plan (opt-in fixes) persistence and application.
- `pkg/verify/`: Post-migration count and content-hash verification.
- `pkg/idmap/`: Records/looks up converted `_id` mappings (`id-mapping.jsonl`) so verification reconciles them.
- `pkg/partition/`: Dependency-free partition-count helper shared by the engine and the assessor.
- `pkg/console/`: Browser-based control console (server, HTTP API, and embedded UI).
- `pkg/wizard/`: Interactive configuration-file generator.
- `pkg/metrics/`: Control plane — metrics registry, job status, and cooperative pause/resume/stop.
