package db

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// FailedIndex records an index that failed to be created on the target.
type FailedIndex struct {
	Collection string // Target collection name
	IndexName  string // Index name from the source definition
	Error      error  // The error that caused the failure
}

// MongoDB represents a MongoDB connection
type MongoDB struct {
	client         *mongo.Client
	database       *mongo.Database
	log            *logger.Logger
	indexSemaphore chan struct{}  // limits concurrent async index builds to prevent Firestore cross-transaction contention
	indexWg        sync.WaitGroup // tracks in-flight async index creation goroutines
	failedIndexes  []FailedIndex  // indexes that failed to be created (populated by async builds)
	failedIndexMu  sync.Mutex     // protects failedIndexes

	// Index-build progress (for the console's "创建索引 M/N" indicator). indexTotal
	// counts every async build launched this run; indexDone counts those finished
	// (success OR failure). indexBuilding names the index currently being sent to
	// the server ("coll[name]"), "" when idle. All read via IndexProgress().
	indexTotal    int64        // atomic
	indexDone     int64        // atomic
	indexBuilding atomic.Value // string; the index currently being built
}

// NewMongoDB creates a new MongoDB connection with pool size and idle timeouts configured dynamically
func NewMongoDB(connectionString, databaseName string, minPoolSize, maxPoolSize uint64, maxConnIdleTime time.Duration, poolMonitor *event.PoolMonitor, log *logger.Logger) (*MongoDB, error) {
	// Set client options
	clientOptions := options.Client().
		ApplyURI(connectionString).
		SetMaxPoolSize(maxPoolSize).
		SetMinPoolSize(minPoolSize).
		SetConnectTimeout(30 * time.Second).
		SetSocketTimeout(120 * time.Second)

	if maxConnIdleTime > 0 {
		clientOptions.SetMaxConnIdleTime(maxConnIdleTime)
	}

	if poolMonitor != nil {
		clientOptions.SetPoolMonitor(poolMonitor)
	}

	// Connect + ping with bounded retry. Firestore's MongoDB-compat endpoint
	// occasionally drops the initial TLS/handshake ("socket was unexpectedly
	// closed: EOF") — a transient, retryable condition rather than a
	// misconfiguration. A single Ping would surface that blip as a hard startup
	// failure, so retry a few times with exponential backoff before giving up.
	// Non-transient errors (auth, bad host, missing db) fail fast so the operator
	// is not left waiting on a hopeless loop, and either way the final error
	// carries actionable guidance.
	const maxAttempts = 5
	var client *mongo.Client
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		client, lastErr = mongo.Connect(ctx, clientOptions)
		if lastErr == nil {
			// mongo.Connect is lazy; Ping forces the actual handshake where the
			// transient EOF shows up.
			lastErr = client.Ping(ctx, nil)
		}
		cancel()
		if lastErr == nil {
			break // connected and verified
		}
		// Tear down the half-open client before retrying or returning.
		if client != nil {
			dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = client.Disconnect(dctx)
			dcancel()
			client = nil
		}
		if !isTransientConnectError(lastErr) || attempt == maxAttempts {
			break
		}
		delay := connectBackoff(attempt)
		if log != nil {
			log.Warnf("连接数据库失败（第 %d/%d 次，可重试的瞬时错误）：%v；%v 后重试", attempt, maxAttempts, lastErr, delay)
		}
		time.Sleep(delay)
	}
	if lastErr != nil {
		return nil, connectError(lastErr)
	}

	// Get database
	database := client.Database(databaseName)

	return &MongoDB{
		client:         client,
		database:       database,
		log:            log,
		indexSemaphore: make(chan struct{}, 1), // serialize async index builds to prevent Firestore cross-transaction contention
	}, nil
}

// isTransientConnectError reports whether a connect/ping failure is the kind
// that typically clears on its own (network blips, Firestore's handshake EOF,
// server-selection timeouts) and is therefore worth retrying. Auth failures and
// malformed hosts are intentionally excluded — retrying those only wastes time.
func isTransientConnectError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, sub := range []string{
		"socket was unexpectedly closed",
		"EOF",
		"connection reset by peer",
		"broken pipe",
		"i/o timeout",
		"deadline exceeded",
		"Deadline exceeded",
		"DeadlineExceeded",
		"server selection error",
		"connection refused",
		"no reachable servers",
	} {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// connectBackoff returns the wait before the next connect attempt: 2s, 4s, 8s,
// 16s, capped at 30s (attempt is 1-based).
func connectBackoff(attempt int) time.Duration {
	d := time.Duration(int64(1)<<uint(attempt)) * time.Second
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

// connectError wraps a final (post-retry) connection failure with guidance the
// operator can act on, split by whether the failure looked transient or like a
// configuration problem. The raw connection string is deliberately not echoed —
// it can carry a password.
func connectError(err error) error {
	if isTransientConnectError(err) {
		return fmt.Errorf("连接数据库失败（已重试 5 次仍未成功，属瞬时网络/握手错误如 EOF）：%w\n"+
			"  排查建议：\n"+
			"    1) 确认该 Firestore 库存在且状态 READY：gcloud firestore databases describe --database=<id>\n"+
			"    2) 确认连接串 Endpoint 主机名 uid.<location>.firestore.goog 与该库当前 uid/location 一致（删库重建后 uid 会变，Endpoint 必须同步更新）\n"+
			"    3) 确认本机到 *.firestore.goog:443 网络可达（VPC / 防火墙 / 出口代理）\n"+
			"    4) Firestore 偶发握手 EOF 通常会自行恢复，可稍后整体重跑", err)
	}
	return fmt.Errorf("连接数据库失败（非瞬时错误，通常是认证或连接串配置问题）：%w\n"+
		"  排查建议：\n"+
		"    1) 检查用户名/密码或 OIDC 服务账号是否正确、是否已过期\n"+
		"    2) 连接串是否带 retryWrites=false（Firestore 必需）\n"+
		"    3) 目标库是否为 ENTERPRISE 版且已开启 MongoDB 兼容数据访问", err)
}

// SetIndexConcurrency replaces the index build semaphore with one of the given capacity.
// Use n=1 (default) for Firestore targets to serialize builds and avoid cross-transaction
// contention. Use a higher value (e.g. 4) for regular MongoDB targets.
// Must be called before any async index builds are launched.
func (m *MongoDB) SetIndexConcurrency(n int) {
	if n <= 0 {
		n = 1
	}
	m.indexSemaphore = make(chan struct{}, n)
	m.log.Infof("Index build concurrency set to %d", n)
}

// WaitForIndexCreation blocks until all async index creation goroutines have finished.
// Used by index-only mode to ensure the process doesn't exit before indexes are created.
func (m *MongoDB) WaitForIndexCreation() {
	m.indexWg.Wait()
}

// IndexProgress reports async index-build progress: total launched, total finished
// (success or failure), and the index currently being built ("coll[name]", or ""
// when idle). Safe for concurrent reads while builds are in flight — this is what
// the console's "创建索引 M/N" indicator polls.
func (m *MongoDB) IndexProgress() (total, done int, building string) {
	total = int(atomic.LoadInt64(&m.indexTotal))
	done = int(atomic.LoadInt64(&m.indexDone))
	if v, ok := m.indexBuilding.Load().(string); ok {
		building = v
	}
	return total, done, building
}

// GetFailedIndexes returns a copy of the failed index list.
// Call this after WaitForIndexCreation() to inspect which indexes could not be created.
func (m *MongoDB) GetFailedIndexes() []FailedIndex {
	m.failedIndexMu.Lock()
	defer m.failedIndexMu.Unlock()
	result := make([]FailedIndex, len(m.failedIndexes))
	copy(result, m.failedIndexes)
	return result
}

// recordFailedIndex appends a failed index entry (thread-safe).
func (m *MongoDB) recordFailedIndex(collection, indexName string, err error) {
	m.failedIndexMu.Lock()
	defer m.failedIndexMu.Unlock()
	m.failedIndexes = append(m.failedIndexes, FailedIndex{
		Collection: collection,
		IndexName:  indexName,
		Error:      err,
	})
}

// Close closes the MongoDB connection
func (m *MongoDB) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}

// GetCollection returns a MongoDB collection
func (m *MongoDB) GetCollection(collectionName string) *mongo.Collection {
	return m.database.Collection(collectionName)
}

// ListCollections returns a list of all collection names in the database
func (m *MongoDB) ListCollections(ctx context.Context) ([]string, error) {
	collections, err := m.database.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("failed to list collections: %w", err)
	}
	return collections, nil
}

// GetDatabaseName returns the database name
func (m *MongoDB) GetDatabaseName() string {
	return m.database.Name()
}

// GetClient returns the MongoDB client
func (m *MongoDB) GetClient() *mongo.Client {
	return m.client
}

// CreateChangeStream creates a change stream for a collection
func (m *MongoDB) CreateChangeStream(ctx context.Context, collectionName string, resumeToken interface{}) (*mongo.ChangeStream, error) {
	collection := m.GetCollection(collectionName)

	// Set pipeline for full document lookup on updates
	pipeline := mongo.Pipeline{}

	// Set options
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if resumeToken != nil {
		opts.SetResumeAfter(resumeToken)
	}

	// Create change stream
	changeStream, err := collection.Watch(ctx, pipeline, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create change stream for collection %s: %w%s", collectionName, err, firestoreChangeStreamHint())
	}

	return changeStream, nil
}

// firestoreChangeStreamHint returns an actionable, multi-line hint appended to
// change-stream creation errors. In Firestore (MongoDB compatibility) change
// streams are a Preview feature that MUST be created manually per database via
// the Google Cloud console before a cursor can be opened — there is no gcloud
// command and no automatic enablement. A failure to open a change stream is
// most commonly caused by the source database not having one created yet.
func firestoreChangeStreamHint() string {
	return "\n\nHint: live replication requires a change stream on the SOURCE Firestore database." +
		"\nFirestore change streams (Preview) must be created MANUALLY — there is no gcloud command and no automatic enablement." +
		"\nCreate one in the Google Cloud console before running live/live-only mode:" +
		"\n  1. Open the Databases page, select the source Firestore (MongoDB compatibility) database (opens Firestore Studio)." +
		"\n  2. In the Explorer panel, find the 'Change streams' node -> More actions -> 'Create change stream'." +
		"\n  3. Set a name, scope (the database/collections you migrate), and a retention period (max 7 days), then Save." +
		"\n     Ensure the copy phase finishes within the retention window, or CDC catch-up will have a gap." +
		"\nRequires the Datastore Index Admin role (roles/datastore.indexAdmin)." +
		"\nDocs: https://docs.cloud.google.com/firestore/mongodb-compatibility/docs/change-streams"
}

// CreateClientLevelChangeStream creates a change stream at the database level.
// This watches for changes across all collections in the configured database.
// Note: Previously this used client.Watch() which targets the admin database internally,
// but Firestore's MongoDB-compatible API does not support the admin database. Using
// database.Watch() avoids this issue while providing identical change event structure
// (each event still includes the full ns.db and ns.coll fields).
// It accepts both a resumeToken and a cdcStartTime:
// - If resumeToken is provided, it takes precedence and instructs the driver to resume from a specific checkpoint.
// - If resumeToken is nil but cdcStartTime is specified, it configures SetStartAtOperationTime to begin reading changes from that exact historical moment.
func (m *MongoDB) CreateClientLevelChangeStream(ctx context.Context, resumeToken interface{}, cdcStartTime *primitive.Timestamp, batchSize int, pipeline mongo.Pipeline) (*mongo.ChangeStream, error) {
	if pipeline == nil {
		pipeline = mongo.Pipeline{}
	}

	// Set options
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if resumeToken != nil {
		// Resume replication from a previously saved checkpoint token
		opts.SetResumeAfter(resumeToken)
	} else if cdcStartTime != nil {
		// If no resume token exists but the user provided a historical starting point,
		// set the stream's start point to that specific cluster operation time.
		opts.SetStartAtOperationTime(cdcStartTime)
		m.log.Infof("Starting change stream at operation time: %s", time.Unix(int64(cdcStartTime.T), 0).UTC().Format(time.RFC3339))
	}

	// Set batch size if provided
	if batchSize > 0 {
		opts.SetBatchSize(int32(batchSize))
		m.log.Infof("Setting change stream batch size to %d", batchSize)
	}

	// Create database-level change stream (watches all collections in the configured database)
	changeStream, err := m.database.Watch(ctx, pipeline, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create change stream for database '%s': %w%s", m.database.Name(), err, firestoreChangeStreamHint())
	}

	m.log.Infof("Created database-level change stream watching all collections in database '%s'", m.database.Name())
	return changeStream, nil
}

// ListIndexes returns all indexes for a collection.
// The "key" field in each returned bson.M is guaranteed to be a bson.D (ordered)
// so that compound index field order is preserved correctly.
func (m *MongoDB) ListIndexes(ctx context.Context, collectionName string) ([]bson.M, error) {
	collection := m.GetCollection(collectionName)
	cursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list indexes for collection %s: %w", collectionName, err)
	}
	defer cursor.Close(ctx)

	var indexes []bson.M
	for cursor.Next(ctx) {
		// Decode the full document as bson.M for general field access
		var indexDoc bson.M
		if err := cursor.Decode(&indexDoc); err != nil {
			return nil, fmt.Errorf("failed to decode index: %w", err)
		}

		// Extract the "key" field from the raw BSON as bson.D to preserve field order.
		// bson.M (map) does not guarantee iteration order, which matters for compound indexes
		// where {a:1, b:1} is different from {b:1, a:1}.
		rawDoc := cursor.Current
		keyVal, err := rawDoc.LookupErr("key")
		if err == nil {
			var orderedKeys bson.D
			if unmarshalErr := keyVal.Unmarshal(&orderedKeys); unmarshalErr == nil {
				indexDoc["key"] = orderedKeys
			}
		}

		indexes = append(indexes, indexDoc)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate indexes: %w", err)
	}

	return indexes, nil
}

// isContentionError checks if an error is a Firestore cross-transaction contention error.
func isContentionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "cross-transaction contention") ||
		(strings.Contains(errStr, "Aborted") && strings.Contains(errStr, "contention")) ||
		strings.Contains(errStr, "too much contention")
}

// CreateIndexFromDefinitionAsync creates an index asynchronously in a goroutine.
// It uses a dedicated MongoDB client with no socket timeout so index builds can run
// as long as needed without being killed by the main client's 120-second socket timeout.
//
// Throttling: A semaphore serializes index builds to prevent Firestore
// cross-transaction contention. Goroutines queue up and execute according to the
// configured concurrency (default 1 = fully serialized).
//
// Retry: If a contention error still occurs (e.g., from an external process), the
// operation retries up to 3 times with 30s/60s/120s delays.
//
// Failed indexes are recorded and can be retrieved via GetFailedIndexes() after
// WaitForIndexCreation() completes.
func (m *MongoDB) CreateIndexFromDefinitionAsync(connectionString, collectionName string, indexDef bson.M) {
	// Extract index name for logging
	indexName, _ := indexDef["name"].(string)

	// Track this goroutine so callers can wait for all index builds to finish
	m.indexWg.Add(1)
	// Progress accounting for the console indicator: count the build as launched now,
	// count it as done when the goroutine returns (whether it succeeded or failed).
	atomic.AddInt64(&m.indexTotal, 1)

	go func() {
		defer m.indexWg.Done()
		defer atomic.AddInt64(&m.indexDone, 1)
		// Acquire semaphore — blocks until a slot is available.
		// This serializes index creation to avoid Firestore cross-transaction contention.
		if m.indexSemaphore != nil {
			m.log.Debugf("[async] Index '%s' on '%s': waiting for semaphore...", indexName, collectionName)
			m.indexSemaphore <- struct{}{}
			defer func() { <-m.indexSemaphore }()
		}

		startTime := time.Now()

		// Create a dedicated client with no socket timeout for long-running index builds
		clientOptions := options.Client().
			ApplyURI(connectionString).
			SetMaxPoolSize(4).
			SetMinPoolSize(1).
			SetConnectTimeout(30 * time.Second)
		// Intentionally NO SetSocketTimeout — index builds can take hours

		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Hour)
		defer cancel()

		client, err := mongo.Connect(ctx, clientOptions)
		if err != nil {
			m.log.Errorf("[async] Failed to connect for index '%s' on '%s': %v", indexName, collectionName, err)
			m.recordFailedIndex(collectionName, indexName, fmt.Errorf("connection failed: %w", err))
			return
		}
		defer client.Disconnect(ctx)

		// Create the index using the dedicated client, with retry for contention errors
		collection := client.Database(m.database.Name()).Collection(collectionName)

		const maxRetries = 3
		retryDelays := []time.Duration{30 * time.Second, 60 * time.Second, 120 * time.Second}

		var lastErr error
		for attempt := 0; attempt <= maxRetries; attempt++ {
			if attempt > 0 {
				delay := retryDelays[attempt-1]
				m.log.Infof("[async] Retrying index '%s' on '%s' (attempt %d/%d) after %v...",
					indexName, collectionName, attempt, maxRetries, delay)
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					m.log.Warnf("[async] Context canceled while waiting to retry index '%s' on '%s'", indexName, collectionName)
					m.recordFailedIndex(collectionName, indexName, fmt.Errorf("context canceled during retry: %w", ctx.Err()))
					return
				}
			}

			lastErr = m.createIndexOnCollection(ctx, collection, collectionName, indexDef)
			if lastErr == nil {
				m.log.Infof("[async] Successfully created index '%s' on '%s' (took %v)",
					indexName, collectionName, time.Since(startTime).Round(time.Second))

				// Small cooldown after success to reduce pressure on Firestore metadata
				time.Sleep(2 * time.Second)
				return
			}

			// Only retry on contention errors
			if !isContentionError(lastErr) {
				break
			}

			m.log.Warnf("[async] Contention error creating index '%s' on '%s': %v (attempt %d/%d, took %v so far)",
				indexName, collectionName, lastErr, attempt+1, maxRetries+1, time.Since(startTime).Round(time.Second))
		}

		// All attempts exhausted or non-contention error — record as failed
		m.log.Warnf("[async] Failed to create index '%s' on '%s': %v (took %v)",
			indexName, collectionName, lastErr, time.Since(startTime).Round(time.Second))
		m.recordFailedIndex(collectionName, indexName, lastErr)
	}()
}

// CreateIDIndexAsync launches an async build of the {_id: 1} index (named "_id_")
// on a target collection. Firestore's MongoDB-compat API — unlike real MongoDB —
// does NOT auto-create the _id index, so migrations must create it explicitly or
// _id lookups/ordering have no backing index. Reuses the throttled/retrying async
// path. Idempotent at the call site (skip when target already has "_id_").
func (m *MongoDB) CreateIDIndexAsync(connectionString, collectionName string) {
	idDef := bson.M{"name": "_id_", "key": bson.D{{Key: "_id", Value: 1}}}
	m.CreateIndexFromDefinitionAsync(connectionString, collectionName, idDef)
}

// createIndexOnCollection is a helper that creates an index on a given collection.
// It contains the shared index model building logic used by the async path.
func (m *MongoDB) createIndexOnCollection(ctx context.Context, collection *mongo.Collection, collectionName string, indexDef bson.M) error {
	indexName, ok := indexDef["name"].(string)
	if !ok {
		return fmt.Errorf("index definition missing 'name' field")
	}

	keysRaw, ok := indexDef["key"]
	if !ok {
		return fmt.Errorf("index definition missing 'key' field")
	}

	var keys bson.D
	switch k := keysRaw.(type) {
	case bson.D:
		keys = k
	case bson.M:
		for key, value := range k {
			keys = append(keys, bson.E{Key: key, Value: value})
		}
	default:
		return fmt.Errorf("unexpected type for index keys: %T", keysRaw)
	}

	indexModel := mongo.IndexModel{Keys: keys}
	opts := options.Index().SetName(indexName)

	if unique, ok := indexDef["unique"].(bool); ok && unique {
		opts.SetUnique(true)
	}
	if sparse, ok := indexDef["sparse"].(bool); ok && sparse {
		opts.SetSparse(true)
	}

	// Handle TTL (expireAfterSeconds) — the server may return int32, int64, or float64
	// depending on the BSON encoding or deserialization path.
	if val, ok := indexDef["expireAfterSeconds"]; ok {
		switch v := val.(type) {
		case int32:
			opts.SetExpireAfterSeconds(v)
		case int64:
			opts.SetExpireAfterSeconds(int32(v))
		case float64:
			opts.SetExpireAfterSeconds(int32(v))
		}
	}

	if partialFilter, ok := indexDef["partialFilterExpression"]; ok {
		opts.SetPartialFilterExpression(partialFilter)
	}
	if defaultLanguage, ok := indexDef["default_language"].(string); ok {
		opts.SetDefaultLanguage(defaultLanguage)
	}
	if languageOverride, ok := indexDef["language_override"].(string); ok {
		opts.SetLanguageOverride(languageOverride)
	}
	if weights, ok := indexDef["weights"]; ok {
		opts.SetWeights(weights)
	}
	opts.SetBackground(true)

	indexModel.Options = opts

	m.log.Infof("[async] Sending index creation request for '%s' on collection '%s' to Firestore...", indexName, collectionName)
	// Publish which index is being built so the console can show "正在建：coll[name]".
	m.indexBuilding.Store(fmt.Sprintf("%s[%s]", collectionName, indexName))
	defer m.indexBuilding.Store("")
	startTime := time.Now()
	_, err := collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		m.log.Infof("[async] Received ERROR response for index '%s' on collection '%s' after %v: %v",
			indexName, collectionName, time.Since(startTime).Round(time.Second), err)
		return fmt.Errorf("failed to create index '%s' on collection %s: %w", indexName, collectionName, err)
	}
	m.log.Infof("[async] Received SUCCESS response for index '%s' on collection '%s' after %v",
		indexName, collectionName, time.Since(startTime).Round(time.Second))
	return nil
}
