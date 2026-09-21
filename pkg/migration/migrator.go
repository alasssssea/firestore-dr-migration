package migration

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"firestore-dr-migration/pkg/config"
	"firestore-dr-migration/pkg/db"
	"firestore-dr-migration/pkg/idmap"
	"firestore-dr-migration/pkg/logger"
	"firestore-dr-migration/pkg/metrics"
	"firestore-dr-migration/pkg/partition"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Migrator handles the migration and replication process
type Migrator struct {
	config        *config.Config
	log           *logger.Logger
	LiveStartTime *primitive.Timestamp
	DryRun        bool
	isLive        bool
	CheckpointDir string

	// Control plane (optional; wired by the web console via AttachControlPlane).
	// When nil the migrator behaves exactly as the CLI. See observer.go.
	reg     *metrics.Registry
	control *metrics.Control
	jobID   string

	// cfgMu guards the runtime-mutable slice of config (partition knobs, per-collection
	// tuning, ConcurrentCollections) so the console can hot-update them mid-run
	// (Reconfig) while dispatch goroutines read them via effectivePartitioning.
	cfgMu sync.RWMutex
	// collSem, when set by the change-stream backfill dispatcher, caps how many
	// collections load at once and can be resized live via Reconfig. nil in CLI mode.
	collSem *resizableSem
}

// getCheckpointDir returns the configured checkpoint directory, defaulting to current directory if unset.
func (m *Migrator) getCheckpointDir() string {
	if m.CheckpointDir != "" {
		return m.CheckpointDir
	}
	return "."
}

// NewMigrator creates a new migrator
func NewMigrator(config *config.Config, log *logger.Logger) *Migrator {
	return &Migrator{
		config: config,
		log:    log,
	}
}

// Start starts the migration or replication process
func (m *Migrator) Start(ctx context.Context, mode string) error {
	m.isLive = (mode == "live" || mode == "live-only")

	// Validate mode
	if mode != "migrate" && mode != "live" && mode != "live-only" && mode != "retry-dlq" {
		return fmt.Errorf("invalid mode: %s, must be 'migrate', 'live', 'live-only', or 'retry-dlq'", mode)
	}

	// When driven by the web console, translate a cooperative Stop() into
	// context cancellation so a user "停止" actually halts in-flight work. The
	// console maps the resulting clean cancellation to a terminal state itself.
	// No-op for the CLI.
	if m.controlPlaneAttached() {
		var cancel context.CancelFunc
		ctx, cancel = m.watchStop(ctx)
		defer cancel()
	}

	m.log.Infof("Starting Firestore -> Firestore %s process", mode)

	if m.DryRun {
		m.log.Info("[Dry Run] Running target compatibility check reports before starting migration")
		if err := RunCompatibilityTest(ctx, m.config, m.DryRun, m.log); err != nil {
			m.log.Warnf("Target compatibility check connection check failed: %v", err)
		}
	}

	// When _id conversion is enabled, record every invalid-_id → string mapping
	// to a shared id-map file so `-mode=verify` can reconcile a rewritten target
	// document back to its source _id. One shared, mutex-guarded store per run;
	// every FieldTransformer built afterwards picks it up automatically. Restored
	// to a no-op on return so a later run/verify starts from a clean sink. Skipped
	// on dry runs (no writes) — the file lives beside the run's checkpoints.
	if m.config.RetryConfig.ConvertInvalidIds && !m.DryRun {
		idMapPath := filepath.Join(m.getCheckpointDir(), "id-mapping.jsonl")
		if store, err := idmap.OpenFileStore(idMapPath); err != nil {
			m.log.Warnf("Could not open id-map %s (converted _ids will not be recorded for verify): %v", idMapPath, err)
		} else {
			SetSharedIDStore(store)
			m.log.Infof("Recording _id conversions to %s for verification", idMapPath)
			defer func() {
				SetSharedIDStore(idmap.NopStore{})
				if cerr := store.Close(); cerr != nil {
					m.log.Warnf("Error closing id-map file: %v", cerr)
				}
			}()
		}
	}

	if mode == "migrate" || mode == "retry-dlq" {
		// Migrate mode: process each database pair sequentially
		var pairErrors []string
		for i, pair := range m.config.DatabasePairs {
			m.log.Infof("Processing database pair %d/%d", i+1, len(m.config.DatabasePairs))
			if err := m.processDatabasePair(ctx, pair, i, mode); err != nil {
				if err == context.Canceled {
					m.log.Info("Processing stopped due to user interrupt (Ctrl+C)")
					break
				}
				m.log.Errorf("Error processing database pair %d: %v", i+1, err)
				m.reportPairError(pair.Source.Database, err)
				pairErrors = append(pairErrors, fmt.Sprintf("pair %d (%s): %v", i+1, pair.Source.Database, err))
			}
		}
		// Never report success when a pair failed: that message previously printed
		// unconditionally, masking dropped partitions / data loss as a clean run.
		if len(pairErrors) > 0 {
			return fmt.Errorf("migration finished with errors in %d database pair(s): %s", len(pairErrors), strings.Join(pairErrors, "; "))
		}
		if mode == "retry-dlq" {
			m.log.Info("DLQ reprocessing completed successfully")
		} else {
			m.log.Info("Migration completed successfully")
		}
		return nil
	}

	// Live mode: process all database pairs concurrently
	m.log.Infof("Starting live replication for %d database pair(s) concurrently", len(m.config.DatabasePairs))

	var wg sync.WaitGroup
	for i, pair := range m.config.DatabasePairs {
		wg.Add(1)
		go func(index int, dbPair config.DatabasePair) {
			defer wg.Done()
			m.log.Infof("Starting database pair %d/%d", index+1, len(m.config.DatabasePairs))
			if err := m.processDatabasePair(ctx, dbPair, index, mode); err != nil {
				if err == context.Canceled {
					m.log.Infof("Database pair %d stopped due to context cancellation", index+1)
				} else {
					m.log.Errorf("Error processing database pair %d: %v", index+1, err)
					// Surface the failure in the console so the database doesn't
					// silently disappear from the dashboard.
					m.reportPairError(dbPair.Source.Database, err)
				}
			}
		}(i, pair)
	}

	// Wait for interrupt signal or all pairs to complete
	m.log.Info("Live replication active. Press Ctrl+C to stop.")

	shutdownCtx, cancelFunc := context.WithCancel(ctx)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		m.log.Infof("Received %s signal. Initiating graceful shutdown...", sig)
		cancelFunc()
	}()

	// Also cancel when all database pairs complete (e.g., all indexOnly pairs finish)
	go func() {
		wg.Wait()
		m.log.Info("All database pairs completed. Shutting down.")
		cancelFunc()
	}()

	<-shutdownCtx.Done()

	// Wait for all database pairs to finish shutting down
	wg.Wait()

	m.log.Info("Shutdown complete.")
	return nil
}

// getCheckpointPath generates a per-pair checkpoint file path
// For single database pair configs, it uses the legacy "global" naming for backward compatibility
func (m *Migrator) getCheckpointPath(prefix string, pairIndex int) string {
	if len(m.config.DatabasePairs) == 1 {
		return fmt.Sprintf("%s-global.json", prefix)
	}
	return fmt.Sprintf("%s-pair%d.json", prefix, pairIndex)
}

// getDLQPath generates a per-pair DLQ file path
func (m *Migrator) getDLQPath(pairIndex int) string {
	if len(m.config.DatabasePairs) == 1 {
		return "dlq-global.jsonl"
	}
	return fmt.Sprintf("dlq-pair%d.jsonl", pairIndex)
}

// getInitialMigrationStatePath generates a per-pair initial migration state file path
func (m *Migrator) getInitialMigrationStatePath(pairIndex int) string {
	if len(m.config.DatabasePairs) == 1 {
		return "initialMigrationState-global.json"
	}
	return fmt.Sprintf("initialMigrationState-pair%d.json", pairIndex)
}

// processDatabasePair processes a single database pair
func (m *Migrator) processDatabasePair(ctx context.Context, pair config.DatabasePair, pairIndex int, mode string) error {
	if mode == "retry-dlq" {
		return m.reprocessDLQ(ctx, pair, pairIndex)
	}

	liveOnly := mode == "live-only"

	// Initialize shared stats tracking for this database pair
	statsInterval := time.Duration(m.config.StatsIntervalMinutes) * time.Minute
	incrementalStatsManager := NewIncrementalStatsManager(m.log, statsInterval, m.config.GroupOpsByDistinctId)
	incrementalStatsManager.DryRun = m.DryRun

	backfillStatsManager := NewBackfillStatsManager(m.log, statsInterval, incrementalStatsManager)
	backfillStatsManager.DryRun = m.DryRun

	// Start backfill stats manager
	backfillStatsCtx, cancelBackfillStats := context.WithCancel(ctx)
	defer cancelBackfillStats()
	backfillStatsManager.Start(backfillStatsCtx)

	// When attached to the web console, mirror progress into the dashboard PER
	// COLLECTION: one 全量 (initial) row per collection driven by the backfill
	// manager, and — for live modes — one 增量 (live) row per collection driven by
	// the incremental manager. Both pollers key rows by db.coll, never an
	// aggregate-by-database summary. Cheap no-ops for the CLI. The second arg is a
	// db fallback for namespaces without a "db." prefix; the third is unused.
	if m.controlPlaneAttached() {
		m.pollBackfill(backfillStatsCtx, pair.Source.Database, "", backfillStatsManager)
		if mode == "live" || mode == "live-only" {
			m.pollIncremental(backfillStatsCtx, pair.Source.Database, "", incrementalStatsManager)
		}
	}

	// Connect to source MongoDB (modern driver)
	m.log.Infof("Connecting to source MongoDB at %s (MinPoolSize: 128, MaxPoolSize: 256)", pair.Source.ConnectionString)
	sourceDB, err := db.NewMongoDB(pair.Source.ConnectionString, pair.Source.Database, 128, 256, 0, incrementalStatsManager.GetSourcePoolMonitor(), m.log) // Source uses static pool size (min 128, max 256)
	if err != nil {
		return fmt.Errorf("failed to connect to source MongoDB: %w", err)
	}

	// Get maximum connection idle timeout for target
	maxConnIdleTimeTarget := time.Duration(m.config.TargetMaxConnIdleSeconds) * time.Second

	// Connect to target MongoDB
	m.log.Infof("Connecting to target MongoDB at %s (MinPoolSize: %d, MaxPoolSize: %d, MaxIdleTime: %v)", pair.Target.ConnectionString, m.config.TargetMinPoolSize, m.config.TargetMaxPoolSize, maxConnIdleTimeTarget)
	targetDB, err := db.NewMongoDB(pair.Target.ConnectionString, pair.Target.Database, uint64(m.config.TargetMinPoolSize), uint64(m.config.TargetMaxPoolSize), maxConnIdleTimeTarget, incrementalStatsManager.GetTargetPoolMonitor(), m.log)
	if err != nil {
		return fmt.Errorf("failed to connect to target MongoDB: %w", err)
	}

	// Determine collections to process
	collections, err := m.getCollectionsToProcess(ctx, sourceDB, pair.Target.Collections)
	if err != nil {
		return fmt.Errorf("failed to determine collections to process: %w", err)
	}

	// Apply database target-level default UpsertMode if active
	if pair.Target.UpsertMode {
		for i := range collections {
			collections[i].UpsertMode = true
		}
	}

	// When attached to the console in a live mode, start the slow source↔target
	// count-reconciliation poller so each collection row shows the ground-truth
	// "behind by N docs" gap (not just the event-time lag). Uses the same context
	// as the other pollers so it stops when the pair ends.
	if m.controlPlaneAttached() && (mode == "live" || mode == "live-only") {
		m.pollCounts(backfillStatsCtx, pair.Source.Database, sourceDB, targetDB, collections)
	}

	// Index timing (migrate / full-only mode): indexes are built AFTER the data
	// load completes (see below, after wg.Wait), never before — building them up
	// front makes Firestore re-index on every inserted document and stalls the whole
	// load. The only exception is IndexOnly mode, whose entire purpose is to create
	// indexes. This matches the legacy full-only path and the live paths' deferred
	// build (all share the same "after load" rule via DeferredIndexController).
	if mode == "migrate" && pair.Target.IndexOnly {
		m.log.Info("IndexOnly mode enabled. Syncing indexes, then skipping data migration.")
		if m.config.IndexConcurrency > 0 {
			targetDB.SetIndexConcurrency(m.config.IndexConcurrency)
		}
		if err := m.syncIndexes(ctx, sourceDB, targetDB, pair, collections); err != nil {
			m.log.Warnf("Index sync encountered issues: %v", err)
		}
		m.log.Info("IndexOnly mode: waiting for all async index creation to complete...")
		targetDB.WaitForIndexCreation()
		m.logFailedIndexes(targetDB)
		m.log.Info("IndexOnly mode: all indexes synced. Skipping data migration.")
		return nil
	}

	// Process each collection
	if mode == "migrate" {
		// For migrate mode, use a wait group to process collections in parallel
		var wg sync.WaitGroup
		// Collect collection-level failures so a failed collection fails the whole
		// pair instead of being silently swallowed. Without this, a partition that
		// blew Firestore's 128 MiB limit (or any other read error) was only logged
		// while the run still reported "Migration completed successfully" — hiding
		// real data loss. Guarded by a mutex: the goroutines below write concurrently.
		var failMu sync.Mutex
		var failedCollections []string
		// Create a semaphore to limit concurrency
		// Use the dedicated parameter for concurrent collections
		concurrentCollections := m.config.ConcurrentCollections
		m.log.Infof("Processing up to %d collections concurrently", concurrentCollections)
		semaphore := make(chan struct{}, concurrentCollections)

		throttlerCtx, throttlerCancel := context.WithCancel(ctx)
		defer throttlerCancel()

		// Initialize the throttler for backfill traffic writes with burst size based on batch size
		burstSize := 2 * m.config.InitialWriteBatchSize
		throttler := NewWriteThrottler(m.config.BackfillRampUp, burstSize)
		if throttler != nil {
			throttler.StartRampUp(throttlerCtx)
			backfillStatsManager.SetThrottler(throttler)
		}

		for _, collConfig := range collections {
			wg.Add(1)
			// Acquire semaphore
			semaphore <- struct{}{}

			// Start migration in a goroutine
			go func(collConfig config.CollectionConfig) {
				defer wg.Done()
				defer func() { <-semaphore }() // Release semaphore when done

				opts := MigrateOptions{
					DLQ:                  nil, // fail-fast
					StatsManager:         nil,
					BackfillStatsManager: backfillStatsManager,
					UpsertMode:           collConfig.UpsertMode,
					Throttler:            throttler,
				}
				if _, _, err := m.migrateCollection(ctx, sourceDB, targetDB, collConfig, opts); err != nil {
					if err == context.Canceled {
						m.log.Infof("Migration of collection %s interrupted due to user interrupt (Ctrl+C)", collConfig.SourceCollection)
						// Don't report as an error
					} else {
						m.log.Errorf("Error migrating collection %s: %v", collConfig.SourceCollection, err)
						// Record the failure so the pair does not report success.
						// We still continue with the other collections so one bad
						// collection does not abort the whole run, but the run must
						// end in a failed state so the loss is never hidden.
						failMu.Lock()
						failedCollections = append(failedCollections, collConfig.SourceCollection)
						failMu.Unlock()
					}
				}
			}(collConfig)
		}

		// Wait for all migrations to complete
		wg.Wait()
		backfillStatsManager.ReportStats(true)
		backfillStatsManager.Stop()

		// Build indexes now that the one-shot full load is complete — never before
		// (see the index-timing note above). Synchronous: this terminal job must not
		// report done until its indexes exist. Skipped on interruption so we don't
		// index a partially-loaded collection. Shared controller keeps the "after
		// load" rule single-source across every path.
		if ctx.Err() == nil {
			deferredIdx := NewDeferredIndexController(m, targetDB, m.config, m.log, func(ctx context.Context) {
				if err := m.syncIndexes(ctx, sourceDB, targetDB, pair, collections); err != nil {
					m.log.Warnf("Index sync encountered issues: %v", err)
				}
			})
			deferredIdx.BuildNow(ctx)
		}

		// A collection that failed above (e.g. a read that blew the 128 MiB limit
		// beyond what the page-shrink backstop could recover) must fail the pair.
		// Reporting success here would silently hide missing documents.
		if len(failedCollections) > 0 {
			if cerr := sourceDB.Close(ctx); cerr != nil {
				m.log.Errorf("Error closing source MongoDB connection: %v", cerr)
			}
			if cerr := targetDB.Close(ctx); cerr != nil {
				m.log.Errorf("Error closing target MongoDB connection: %v", cerr)
			}
			return fmt.Errorf("migration failed for %d collection(s): %v", len(failedCollections), failedCollections)
		}
	} else if mode == "live" || mode == "live-only" {
		// Use client-level change stream for live replication
		if err := m.startClientLevelReplication(ctx, sourceDB, targetDB, pair.Source.Database, pair.Target.Database, collections, pair, pairIndex, liveOnly, incrementalStatsManager, backfillStatsManager); err != nil {
			// We don't need to check for context.Canceled here anymore as it's handled in the lower layers
			return fmt.Errorf("error starting client-level replication: %w", err)
		}
	}

	// If in migrate mode, close connections
	if mode == "migrate" {
		if err := sourceDB.Close(ctx); err != nil {
			m.log.Errorf("Error closing source MongoDB connection: %v", err)
		}
		if err := targetDB.Close(ctx); err != nil {
			m.log.Errorf("Error closing target MongoDB connection: %v", err)
		}
	}

	return nil
}

// startClientLevelReplication starts replication using change streams.
func (m *Migrator) startClientLevelReplication(ctx context.Context, sourceDB, targetDB *db.MongoDB, sourceDBName, targetDBName string, collections []config.CollectionConfig, pair config.DatabasePair, pairIndex int, liveOnly bool, incrementalStatsManager *IncrementalStatsManager, backfillStatsManager *BackfillStatsManager) error {
	m.log.Info("Starting replication using method: changestream")
	return m.startChangeStreamReplication(ctx, sourceDB, targetDB, sourceDBName, targetDBName, collections, pair, pairIndex, liveOnly, incrementalStatsManager, backfillStatsManager)
}

// startChangeStreamReplication starts replication using change streams
func (m *Migrator) startChangeStreamReplication(ctx context.Context, sourceDB, targetDB *db.MongoDB, sourceDBName, targetDBName string, collections []config.CollectionConfig, pair config.DatabasePair, pairIndex int, liveOnly bool, incrementalStatsManager *IncrementalStatsManager, backfillStatsManager *BackfillStatsManager) error {
	m.log.Info("Starting change stream-based replication for all collections")

	// Create client-level replicator
	replicator := NewClientLevelReplicator(sourceDB, targetDB, m.config, m.log)
	replicator.SetIncrementalStatsManager(incrementalStatsManager)
	replicator.SetBackfillStatsManager(backfillStatsManager)
	replicator.DryRun = m.DryRun

	// Add all collections to the replicator
	for _, collConfig := range collections {
		// Add collection to replicator
		replicator.AddCollection(sourceDBName, targetDBName, collConfig)
	}

	// Load global resume token if it exists (per-pair path)
	globalResumeTokenPath := m.getCheckpointPath("resumeToken", pairIndex)
	m.log.Infof("Using checkpoint file: %s", globalResumeTokenPath)
	globalResumeToken, err := LoadResumeToken(globalResumeTokenPath)
	if err != nil {
		m.log.Warnf("Error loading global resume token: %v. Will start from the beginning.", err)
		globalResumeToken = nil
	}

	// Load initial migration state
	initialMigrationStatePath := m.getInitialMigrationStatePath(pairIndex)
	m.log.Infof("Using initial migration state file: %s", initialMigrationStatePath)
	initialMigrationState, err := LoadInitialMigrationState(initialMigrationStatePath)
	if err != nil {
		return fmt.Errorf("failed to load initial migration state: %w", err)
	}

	// Strict DLQ safety checks based on initial migration state
	dlqPath := m.getDLQPath(pairIndex)
	if initialMigrationState == nil || !initialMigrationState.IsCompleted() {
		if err := BackupAndClearDLQ(dlqPath, m.log); err != nil {
			return fmt.Errorf("failed to backup and clear DLQ: %w", err)
		}
	} else if initialMigrationState.Status == StatusCompletedWithFailures {
		reset, gerr := m.resolveCompletedWithFailures(initialMigrationStatePath, dlqPath, pair.Source.Database)
		if gerr != nil {
			return gerr
		}
		if reset {
			initialMigrationState = nil
		}
	}

	// Create DLQ writer for this database pair
	dlq, err := NewDLQWriter(dlqPath, m.log)
	if err != nil {
		m.log.Warnf("Failed to create DLQ writer at %s: %v (continuing without DLQ)", dlqPath, err)
		dlq = nil
	}
	var dlqInterface DLQ = &NopDLQWriter{}
	if dlq != nil {
		dlqInterface = dlq
		defer dlq.Close()
	}
	replicator.SetDLQ(dlqInterface)
	if incrementalStatsManager != nil {
		incrementalStatsManager.SetDLQ(dlqInterface)
	}
	if backfillStatsManager != nil {
		backfillStatsManager.SetDLQ(dlqInterface)
	}

	// Start client-level replication (which will handle index sync during initial migration)
	return replicator.StartReplication(ctx, globalResumeToken, globalResumeTokenPath, initialMigrationState, initialMigrationStatePath, pair, liveOnly, m.LiveStartTime, m)
}

// resolveCompletedWithFailures decides what to do when a pair's initial
// migration state is marked completed_with_failures before starting incremental
// replication.
//
//   - If the DLQ still holds active failed records, those are genuine,
//     retryable per-document failures: block so the operator reprocesses them
//     (DLQ retry) or clears the queue first. This protects data integrity.
//   - If the DLQ has NO active records, the marker is stale/inconsistent —
//     typically from an earlier run that was interrupted (context cancelled),
//     whose un-migrated remainder was counted as "failed" but never DLQ'd.
//     Permanently banning the whole database in that case leaves no recovery
//     path, so instead reset the state (and clear any residual DLQ) and report
//     reset=true so the caller re-runs the initial migration cleanly.
func (m *Migrator) resolveCompletedWithFailures(statePath, dlqPath, sourceDB string) (reset bool, err error) {
	hasFailed, err := HasActiveFailedRecords(dlqPath)
	if err != nil {
		return false, fmt.Errorf("failed to check DLQ status: %w", err)
	}
	if hasFailed {
		return false, fmt.Errorf("cannot start incremental replication for %q: the previous initial migration left failed documents in the dead letter queue (%s). Reprocess them with DLQ retry (or clear the queue) before continuing", sourceDB, dlqPath)
	}
	m.log.Warnf("Database %q is marked completed_with_failures but its DLQ (%s) has no active records — this is a stale/interrupted state, not real failures. Resetting it and re-running the initial migration instead of blocking the database.",
		sourceDB, dlqPath)
	if err := DeleteInitialMigrationState(statePath); err != nil {
		return false, fmt.Errorf("failed to reset stale initial migration state: %w", err)
	}
	if err := BackupAndClearDLQ(dlqPath, m.log); err != nil {
		return false, fmt.Errorf("failed to backup and clear DLQ: %w", err)
	}
	return true, nil
}

// getCollectionsToProcess determines which collections to process
func (m *Migrator) getCollectionsToProcess(ctx context.Context, sourceDB *db.MongoDB, configCollections []config.CollectionConfig) ([]config.CollectionConfig, error) {
	// If collections are specified in config, use them
	if len(configCollections) > 0 {
		m.log.Infof("Using %d collections specified in config", len(configCollections))
		return configCollections, nil
	}

	// Otherwise, auto-detect all collections in the source database
	m.log.Info("No collections specified in config. Auto-detecting all collections...")
	sourceCollections, err := sourceDB.ListCollections(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list collections: %w", err)
	}

	m.log.Infof("Found %d collections in source database: %v", len(sourceCollections), sourceCollections)

	// Create collection configs with same name for source and target
	var collections []config.CollectionConfig
	for _, collName := range sourceCollections {
		collections = append(collections, config.CollectionConfig{
			SourceCollection: collName,
			TargetCollection: collName,
		})
	}

	return collections, nil
}

// MigrateOptions defines parameters to tune the backfill behavior dynamically
type MigrateOptions struct {
	DLQ                  DLQ                      // If provided, failures are routed here and migration continues (resilient mode)
	StatsManager         *IncrementalStatsManager // If provided, statistics are updated thread-safely (live mode stats)
	BackfillStatsManager *BackfillStatsManager    // If provided, backfill statistics are recorded thread-safely
	UpsertMode           bool                     // Use upsert instead of insert (from CollectionConfig)
	Throttler            *WriteThrottler          // Write throttler for initial backfill QPS ramp-up
}

type backfillBatchItem struct {
	batch []interface{}
	seq   uint64
}

// effectivePartitioning returns the partitioned-load knobs for one source
// collection: the per-collection override where set (config's
// TargetConfig.CollectionTuning, keyed by source collection name), otherwise the
// global config value. dbName is the SOURCE database name. This is the single
// resolution point so whole-database and explicit-collection modes behave
// identically, and only the partition levers are per-collection.
func (m *Migrator) effectivePartitioning(dbName, collName string) (parallelEnabled bool, maxParts, workersPerPart, minDocsPerPart, minDocsForParallel int) {
	// Read under RLock so a concurrent console Reconfig (write lock) sees a
	// consistent snapshot of the global knobs + per-collection tuning map.
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()

	parallelEnabled = m.config.ParallelReadsEnabled
	maxParts = m.config.MaxReadPartitions
	workersPerPart = m.config.WorkersPerPartition
	minDocsPerPart = m.config.MinDocsPerPartition
	minDocsForParallel = m.config.MinDocsForParallelReads

	for i := range m.config.DatabasePairs {
		p := &m.config.DatabasePairs[i]
		if p.Source.Database != dbName {
			continue
		}
		if ov, ok := p.Target.CollectionTuning[collName]; ok {
			if ov.ParallelReadsEnabled != nil {
				parallelEnabled = *ov.ParallelReadsEnabled
			}
			if ov.MaxReadPartitions > 0 {
				maxParts = ov.MaxReadPartitions
			}
			if ov.WorkersPerPartition > 0 {
				workersPerPart = ov.WorkersPerPartition
			}
			if ov.MinDocsPerPartition > 0 {
				minDocsPerPart = ov.MinDocsPerPartition
			}
			if ov.MinDocsForParallelReads > 0 {
				minDocsForParallel = ov.MinDocsForParallelReads
			}
		}
		break // the pair for this source db is found; stop scanning
	}
	// Resolve the "auto" sentinel for per-partition write workers. 0 (unset) maps
	// to a modest default that keeps target write pressure sane; multi-region
	// BulkWrite latency (~230ms/batch) means a few workers per partition already
	// saturate a partition's read feed, and total write concurrency is governed by
	// the partition-concurrency limit (partitionConcurrency) anyway.
	if workersPerPart <= 0 {
		workersPerPart = 4
	}
	return
}

// partitionConcurrency decides how many partitions the parallel backfill runs at
// once. Partition COUNT is a correctness knob (each must fit Firestore's 128 MiB
// query limit), so a large collection can produce dozens or hundreds of
// partitions; running them all concurrently would exhaust the connection pools.
// This decouples the two: create many small partitions, but only process a
// bounded number simultaneously. The bound is derived from the target pool
// (each active partition uses ~workersPerPart write connections) and capped so a
// huge collection cannot spawn unbounded concurrency.
func partitionConcurrency(targetMaxPool, workersPerPart, numPartitions int) int {
	// Concurrency here is a THROUGHPUT knob, not a correctness one. Firestore's
	// 128 MiB query-memory ceiling is enforced strictly PER QUERY, not against a
	// shared instance budget: measured directly, 8 simultaneous keyset range-sort
	// walks over big_events ran with ZERO blows and scaled linearly (~94k docs/s
	// aggregate). The 128 MiB blows that plagued earlier runs were NOT caused by
	// read concurrency — they came from the resume path issuing the
	// {_id:{$exists:false}} "skip" sentinel as a real query (an unindexable
	// predicate that forces a full-collection materialization; see readPartition's
	// skip guard). With that fixed, bounded partition concurrency is safe and
	// desirable. This cap only keeps a huge partition count from exhausting the
	// target connection pool (each active partition uses ~workersPerPart writers).
	const hardCap = 8
	if workersPerPart < 1 {
		workersPerPart = 1
	}
	if targetMaxPool <= 0 {
		targetMaxPool = 256
	}
	// Reserve one connection per active partition for overhead beyond its writers.
	c := targetMaxPool / (workersPerPart + 1)
	if c > hardCap {
		c = hardCap
	}
	if c < 1 {
		c = 1
	}
	if numPartitions > 0 && c > numPartitions {
		c = numPartitions
	}
	return c
}

// keysetAndPageFilter is the SAFE-but-slow keyset page filter: it ANDs the
// partition filter (any shape) with a separate _id>lastReadID clause. It is
// always correct, but on Firestore's MongoDB-compatible endpoint two separate
// _id predicates in one $and defeat the _id index — the planner degrades into a
// full range scan + in-memory sort (measured ~68x slower at limit 1 than a
// single merged predicate, and the direct cause of the 128 MiB blow under read
// concurrency). Used only as the fallback for filter shapes mergeKeysetPageFilter
// cannot safely rewrite.
func keysetAndPageFilter(filter bson.D, lastReadID interface{}) bson.D {
	return bson.D{{Key: "$and", Value: bson.A{filter, bson.D{{Key: "_id", Value: bson.D{{Key: "$gt", Value: lastReadID}}}}}}}
}

// mergeKeysetPageFilter folds the keyset lower bound (_id > lastReadID) INTO the
// partition's own _id predicate, producing a single contiguous _id range like
// {_id: {$gt: lastReadID, $lt: partitionEnd}}. A single _id predicate lets the
// endpoint serve the page straight from the _id index instead of the pathological
// two-predicate $and, which is what keeps the sort buffer bounded and avoids the
// 128 MiB blow. It only rewrites the simple single-`_id`-condition partition shape
// the partitioner emits for a uniform-type collection; for anything else (e.g. a
// multi-type $or partition, or an _id condition carrying operators other than the
// range bounds) it falls back to the always-correct AND form. Dropping the
// partition's own lower bound ($gt/$gte) is safe because lastReadID has already
// advanced at or past it (the first page, before any read, still uses the raw
// partition filter).
func mergeKeysetPageFilter(filter bson.D, lastReadID interface{}) bson.D {
	if len(filter) != 1 || filter[0].Key != "_id" {
		return keysetAndPageFilter(filter, lastReadID)
	}
	cond, ok := filter[0].Value.(bson.D)
	if !ok {
		return keysetAndPageFilter(filter, lastReadID)
	}
	merged := bson.D{{Key: "$gt", Value: lastReadID}}
	for _, c := range cond {
		switch c.Key {
		case "$lt", "$lte":
			merged = append(merged, c) // keep the partition's upper bound
		case "$gt", "$gte":
			// superseded by $gt:lastReadID; drop to keep a single contiguous range
		default:
			// $exists/$type/etc.: not a plain range, not safe to merge
			return keysetAndPageFilter(filter, lastReadID)
		}
	}
	return bson.D{{Key: "_id", Value: merged}}
}

// idRangeBounds describes a partition filter of the simple contiguous-_id-range
// form {_id: {$gt|$gte: lower, $lt|$lte: upper}} (either bound optional). ok is
// false for any filter that is NOT a pure _id range — e.g. one carrying $type,
// $exists, a $or of type clauses, or a non-_id top-level key — in which case the
// caller must use the legacy both-bounds page filter.
type idRangeBounds struct {
	lower          interface{}
	hasLower       bool
	lowerInclusive bool // true for $gte
	upper          interface{}
	hasUpper       bool
	upperInclusive bool // true for $lte
	ok             bool
}

// parseIDRangeBounds extracts the contiguous-_id-range bounds from a partition
// filter so the keyset walk can query with an OPEN lower bound and enforce the
// upper bound CLIENT-SIDE. This is the fix for the endgame 128 MiB blow: on
// Firestore's MongoDB-compatible endpoint a TWO-bounded _id query whose limit
// exceeds the number of documents remaining in the range materializes far more
// than the range and blows the 128 MiB ceiling (verified: {$gt:a,$lt:b} with
// limit 4096 over a ~100-doc range blows, while either bound alone is clean).
// Every partition's final page(s) hit exactly that shape, so instead we issue an
// open {_id:{$gt:lastReadID}} query (always index-served and clean) and stop when
// a decoded _id reaches the upper bound. Only the plain-range shape is converted;
// anything exotic returns ok=false and keeps the safe legacy path.
func parseIDRangeBounds(filter bson.D) idRangeBounds {
	var b idRangeBounds
	if len(filter) != 1 || filter[0].Key != "_id" {
		return b
	}
	cond, isD := filter[0].Value.(bson.D)
	if !isD || len(cond) == 0 {
		return b
	}
	for _, c := range cond {
		switch c.Key {
		case "$gt":
			b.lower, b.hasLower, b.lowerInclusive = c.Value, true, false
		case "$gte":
			b.lower, b.hasLower, b.lowerInclusive = c.Value, true, true
		case "$lt":
			b.upper, b.hasUpper, b.upperInclusive = c.Value, true, false
		case "$lte":
			b.upper, b.hasUpper, b.upperInclusive = c.Value, true, true
		default:
			// $type / $exists / anything else: not a pure range.
			return idRangeBounds{}
		}
	}
	b.ok = true
	return b
}

// migrateCollection performs a one-time migration of a collection with parallel batch processing
func (m *Migrator) migrateCollection(ctx context.Context, sourceDB, targetDB *db.MongoDB, collConfig config.CollectionConfig, opts MigrateOptions) (int64, int64, error) {
	// Get source and target collections
	sourceCollection := sourceDB.GetCollection(collConfig.SourceCollection)
	targetCollection := targetDB.GetCollection(collConfig.TargetCollection)

	// Source namespace ("db.coll") for per-collection backfill progress rows.
	bfNS := sourceDB.GetDatabaseName() + "." + collConfig.SourceCollection

	m.log.Infof("Migrating collection: %s.%s to %s.%s", sourceDB.GetDatabaseName(), collConfig.SourceCollection, targetDB.GetDatabaseName(), collConfig.TargetCollection)

	// Get total count for progress reporting
	totalCount, err := sourceCollection.EstimatedDocumentCount(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to count documents: %w", err)
	}

	m.log.Infof("Found %d documents to migrate", totalCount)

	if m.DryRun {
		m.log.Infof("[Dry Run] Skipping actual data migration for collection %s", collConfig.SourceCollection)
		// Perform sampling analysis to recommend ID partitioning strategy
		partitioner := NewCollectionPartitioner(
			sourceCollection,
			m.log,
			m.config.MaxReadPartitions,
			m.config.MinDocsPerPartition,
			m.config.SampleSize,
			m.config.IDTypeForPartition,
		)
		recType, err := partitioner.RecommendIDPartitioning(ctx)
		if err != nil {
			m.log.Warnf("[Dry Run] Failed to recommend ID partitioning for collection %s: %v", collConfig.SourceCollection, err)
		} else {
			m.log.Infof("[Dry Run] Recommended ID partitioning strategy for collection %s: %s", collConfig.SourceCollection, recType)
		}
		return 0, 0, nil
	}

	if opts.BackfillStatsManager != nil {
		opts.BackfillStatsManager.AddTargetCount(bfNS, totalCount)
	}

	// If no documents, we're done
	if totalCount == 0 {
		m.log.Infof("No documents to migrate for collection %s", collConfig.SourceCollection)
		return 0, 0, nil
	}

	// Check if parallel reads are enabled and collection is large enough. Both the
	// enable flag and the size threshold honor any per-collection override, so a
	// byte-heavy/low-count straggler can be partitioned even when the global
	// threshold would leave it on a single cursor.
	parallelEnabled, _, _, _, minDocsForParallel := m.effectivePartitioning(sourceDB.GetDatabaseName(), collConfig.SourceCollection)
	if parallelEnabled && totalCount >= int64(minDocsForParallel) {
		m.log.Infof("Using parallel reads for large collection: %s (%d documents)", collConfig.SourceCollection, totalCount)
		return m.migrateCollectionParallel(ctx, sourceDB, targetDB, collConfig, totalCount, opts)
	}

	// Set up batch processing using configuration parameters
	readBatchSize := m.config.InitialReadBatchSize
	writeBatchSize := m.config.InitialWriteBatchSize

	m.log.Infof("Using read batch size: %d, write batch size: %d", readBatchSize, writeBatchSize)

	// In sequential backfill mode, the collection is treated as a single partition (0 of 1)
	const partitionIndex = 0
	const totalSplits = 1

	// Evaluate backfill resumption plan for sequential migration (partition count: 1)
	checkpointDir := m.getCheckpointDir()
	plan, err := DetermineBackfillResumptionPlan(checkpointDir, sourceDB.GetDatabaseName(), collConfig.SourceCollection, totalSplits)

	var resumeFilter bson.D
	var previouslyMigratedDocs int64

	// Initialize default fresh checkpoint state
	checkpoint := &PartitionCheckpoint{
		Database:                sourceDB.GetDatabaseName(),
		Collection:              collConfig.SourceCollection,
		PartitionIndex:          partitionIndex,
		TotalSplits:             totalSplits,
		ApproximateDocsMigrated: 0,
		TypeProgress:            make(map[BSONType]*TypeRangeBoundary),
		UpdatedAt:               time.Now().UTC(),
	}

	if err != nil {
		m.log.Warnf("[%s.%s] Error determining backfill resumption plan: %v. Starting fresh.", sourceDB.GetDatabaseName(), collConfig.SourceCollection, err)
		plan = &BackfillResumptionPlan{Mode: ResumptionModeFresh}
	}

	switch plan.Mode {
	case ResumptionModeDirect:
		checkpointPath := GetPartitionCheckpointPath(checkpointDir, sourceDB.GetDatabaseName(), collConfig.SourceCollection, partitionIndex, totalSplits)
		loadedCP, loadErr := LoadPartitionCheckpoint(checkpointPath)
		if loadErr != nil || loadedCP == nil {
			m.log.Warnf("[%s.%s] Failed to reload checkpoint from %s: %v. Falling back to fresh start.", sourceDB.GetDatabaseName(), collConfig.SourceCollection, checkpointPath, loadErr)
			plan.Mode = ResumptionModeFresh
		} else {
			checkpoint = loadedCP
			resumeFilter = plan.PartitionFilters[partitionIndex]
			previouslyMigratedDocs = plan.TotalDocsMigrated()
			m.log.Infof("[%s.%s] Resuming sequential initial backfill from checkpoint: ~%d docs previously migrated (filter: %+v)",
				sourceDB.GetDatabaseName(), collConfig.SourceCollection, previouslyMigratedDocs, resumeFilter)
		}

	case ResumptionModeResampleWithGlobalMin:
		previouslyMigratedDocs = plan.TotalDocsMigrated()
		checkpoint.ApproximateDocsMigrated = previouslyMigratedDocs
		for bType, minID := range plan.GlobalMinSafeIDs {
			checkpoint.TypeProgress[bType] = &TypeRangeBoundary{
				BSONType:    bType,
				SavedLastID: minID,
			}
		}
		filter, filterErr := BuildPartitionFilterFromCheckpoint(checkpoint)
		if filterErr != nil {
			m.log.Warnf("[%s.%s] Failed to build filter from global min checkpoint: %v. Falling back to fresh start.", sourceDB.GetDatabaseName(), collConfig.SourceCollection, filterErr)
			plan.Mode = ResumptionModeFresh
			checkpoint.ApproximateDocsMigrated = 0
			checkpoint.TypeProgress = make(map[BSONType]*TypeRangeBoundary)
			previouslyMigratedDocs = 0
		} else {
			resumeFilter = filter
			m.log.Infof("[%s.%s] Resuming sequential initial backfill with global min safe IDs across historical partitions: ~%d docs previously migrated (filter: %+v)",
				sourceDB.GetDatabaseName(), collConfig.SourceCollection, previouslyMigratedDocs, resumeFilter)
		}
	}

	if plan.Mode != ResumptionModeDirect {
		// Clean up any stale or corrupted checkpoint files from disk for non-direct runs
		if err := DeletePartitionCheckpoints(checkpointDir, sourceDB.GetDatabaseName(), collConfig.SourceCollection); err != nil {
			m.log.Warnf("[%s.%s] Failed to delete stale/corrupted checkpoint files: %v",
				sourceDB.GetDatabaseName(), collConfig.SourceCollection, err)
		}
	}

	if plan.Mode == ResumptionModeFresh {
		types, discErr := DiscoverPresentBSONTypes(ctx, sourceCollection)
		if discErr != nil {
			m.log.Warnf("[%s.%s] Failed to discover BSON types for initial backfill: %v", sourceDB.GetDatabaseName(), collConfig.SourceCollection, discErr)
		} else {
			for _, t := range types {
				checkpoint.TypeProgress[t] = &TypeRangeBoundary{BSONType: t}
			}
		}
		m.log.Infof("[%s.%s] Starting fresh sequential initial backfill (discovered types: %v)", sourceDB.GetDatabaseName(), collConfig.SourceCollection, types)
	}

	checkpointPath := GetPartitionCheckpointPath(checkpointDir, sourceDB.GetDatabaseName(), collConfig.SourceCollection, partitionIndex, totalSplits)
	checkpointInterval := time.Duration(m.config.CheckpointIntervalMinutes) * time.Minute
	saveThreshold := m.config.SaveThreshold

	tracker := NewBackfillPartitionTracker(m.log, checkpoint, checkpointPath, checkpointInterval, saveThreshold)
	tracker.Start(ctx)
	defer tracker.Close()

	// Create retry manager for batch processing.
	// [Safety Fix 8: Invalid ID Conversion] Firestore target APIs only support string, int64, or ObjectId document keys;
	// arrays or nested subdocuments trigger terminal errors. Enabling ConvertInvalidIds
	// transforms invalid types to strings to avoid write failures.
	retryManager := NewRetryManager(
		m.config.RetryConfig.MaxRetries,
		time.Duration(m.config.RetryConfig.BaseDelayMs)*time.Millisecond,
		time.Duration(m.config.RetryConfig.MaxDelayMs)*time.Millisecond,
		m.config.RetryConfig.EnableBatchSplitting,
		m.config.RetryConfig.MinBatchSize,
		m.config.RetryConfig.ConvertInvalidIds,
		m.log,
	)

	// [Firestore compat] Firestore's MongoDB-compatible endpoint rejects noCursorTimeout
	// ("Unsupported fields in find request: [noCursorTimeout]"), so we must NOT set it here.
	// Upstream used SetNoCursorTimeout(true) to keep a MongoDB replica-set cursor alive under
	// backpressure; Firestore has no equivalent server-side cursor-timeout knob, and backfill
	// resumption is instead guaranteed by the _id-ordered $gte checkpoint filter below.
	findFilter := bson.D{}
	if resumeFilter != nil {
		findFilter = resumeFilter
	}
	// [Backfill Resumption Safety] Sort by _id ascending to guarantee monotonic traversal order.
	// The resumption filter uses $gte on the last checkpointed _id, so correct ordering is required
	// to ensure no documents are skipped or duplicated upon resume.
	cursor, err := sourceCollection.Find(ctx, findFilter, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetBatchSize(int32(readBatchSize)))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create cursor: %w", err)
	}
	// [Safety Fix 5: Memory Leak on Server] Ensure cursor is closed upon termination to prevent active server-side cursor leaks
	// and connection starvation on the source MongoDB replica set.
	defer cursor.Close(ctx)

	// Set up parallel batch processing
	var wg sync.WaitGroup
	channelBufferSize := m.config.InitialChannelBufferSize
	batchChan := make(chan backfillBatchItem, channelBufferSize) // Buffer for batches
	errorChan := make(chan error, 1)                             // Channel for errors
	doneChan := make(chan struct{})                              // Channel to signal completion

	// Track progress
	var successCount int64
	var failedCount int64
	var migratedCount int64
	var lastLoggedPercentage int = -1 // Start at -1 to ensure 0% is logged
	var mu sync.Mutex                 // Mutex for thread-safe updates to successCount, failedCount, migratedCount, and lastLoggedPercentage

	proactiveSkipEnabled := &atomic.Bool{}

	// Start worker pool for parallel batch processing
	workerCount := m.config.InitialMigrationWorkers
	m.log.Infof("Starting %d workers for parallel document batch processing", workerCount)

	for i := 0; i < workerCount; i++ {
		if i > 0 && m.config.BackfillRampUp.Enabled && m.config.BackfillRampUp.UseStaggeredWorkers && m.config.BackfillRampUp.WorkerDelayMs > 0 {
			m.log.Infof("Staggering worker startup: delaying worker %d startup by %dms...", i, m.config.BackfillRampUp.WorkerDelayMs)
			select {
			case <-ctx.Done():
				close(batchChan)
				wg.Wait()
				return 0, 0, ctx.Err()
			case <-time.After(time.Duration(m.config.BackfillRampUp.WorkerDelayMs) * time.Millisecond):
			}
		}
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for item := range batchChan {
				if opts.BackfillStatsManager != nil {
					opts.BackfillStatsManager.RecordWorkerReceived(int64(len(item.batch)))
				}
				succeeded, failed, err := m.writeBatch(ctx, targetCollection, item.batch, sourceDB.GetDatabaseName(), collConfig.SourceCollection, opts, retryManager, workerID, proactiveSkipEnabled)
				if err != nil {
					select {
					case errorChan <- fmt.Errorf("worker %d failed to process batch: %w", workerID, err):
					default:
					}
					return
				}

				if ctx.Err() == nil {
					tracker.AckBatch(item.seq, succeeded)
				}

				// Update progress
				mu.Lock()
				successCount += succeeded
				failedCount += failed
				migratedCount += int64(len(item.batch))
				currentSuccess := successCount
				currentFailed := failedCount

				cumulativeCount := previouslyMigratedDocs + successCount + failedCount
				if totalCount > 0 && cumulativeCount > totalCount {
					cumulativeCount = totalCount
				}
				currentPercentage := int(float64(cumulativeCount) / float64(totalCount) * 10)

				// Only log when crossing a 10% threshold at the collection level
				// and update lastLoggedPercentage atomically to prevent multiple logs
				shouldLog := false
				if currentPercentage > lastLoggedPercentage {
					lastLoggedPercentage = currentPercentage
					shouldLog = true
				}
				mu.Unlock()

				// Log outside the mutex lock to reduce lock contention
				if shouldLog {
					if failedCount > 0 {
						m.log.Infof("Collection %s progress: ~%d/%d documents (%.0f%%) [This run: %d successful, %d failed]",
							collConfig.SourceCollection, cumulativeCount, totalCount, float64(currentPercentage)*10, currentSuccess, currentFailed)
					} else {
						m.log.Infof("Collection %s progress: ~%d/%d documents (%.0f%%)",
							collConfig.SourceCollection, cumulativeCount, totalCount, float64(currentPercentage)*10)
					}
				}
			}
		}(i)
	}

	// Start a goroutine to close channels when all batches are processed
	go func() {
		wg.Wait()
		close(doneChan)
	}()

	// Process documents and create batches
	var batch []interface{}
	var batchCount int

	for {
		// Check for errors from workers
		select {
		case err := <-errorChan:
			cursor.Close(ctx)
			close(batchChan)
			return successCount, failedCount, err
		default:
			// No errors, continue processing
		}

		// Get next document
		readStart := time.Now()
		hasNext := cursor.Next(ctx)
		if !hasNext {
			break
		}

		// Decode document
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			close(batchChan)
			return successCount, failedCount, fmt.Errorf("failed to decode document: %w", err)
		}
		readDuration := time.Since(readStart)

		if opts.BackfillStatsManager != nil {
			opts.BackfillStatsManager.RecordRead(bfNS, readDuration, len(cursor.Current))
		}

		// Add to batch
		batch = append(batch, doc)
		batchCount++

		// Send batch if it reaches the write batch size
		if batchCount >= writeBatchSize {
			seq := tracker.RegisterBatch(batch)
			item := backfillBatchItem{batch: batch, seq: seq}
			sendStart := time.Now()
			select {
			case batchChan <- item:
				if opts.BackfillStatsManager != nil {
					opts.BackfillStatsManager.RecordIngestQueueStall(time.Since(sendStart))
				}
				// Batch sent to worker
			case err := <-errorChan:
				// Error from a worker
				cursor.Close(ctx)
				close(batchChan)
				return successCount, failedCount, err
			case <-ctx.Done():
				// Context cancelled
				cursor.Close(ctx)
				close(batchChan)
				m.log.Info("Batch processing interrupted due to context cancellation")
				return successCount, failedCount, context.Canceled // Return context.Canceled for consistent error handling
			}

			// Reset batch
			batch = nil
			batchCount = 0

			// Add a small delay between batches to reduce contention
			time.Sleep(5 * time.Millisecond)
		}
	}

	// Check for cursor errors
	if err := cursor.Err(); err != nil {
		close(batchChan)
		return successCount, failedCount, fmt.Errorf("cursor error: %w", err)
	}

	// Process any remaining documents
	if len(batch) > 0 {
		seq := tracker.RegisterBatch(batch)
		item := backfillBatchItem{batch: batch, seq: seq}
		sendStart := time.Now()
		select {
		case batchChan <- item:
			if opts.BackfillStatsManager != nil {
				opts.BackfillStatsManager.RecordIngestQueueStall(time.Since(sendStart))
			}
			// Final batch sent to worker
		case err := <-errorChan:
			// Error from a worker
			close(batchChan)
			return successCount, failedCount, err
		case <-ctx.Done():
			// Context cancelled
			close(batchChan)
			m.log.Info("Final batch processing interrupted due to context cancellation")
			return successCount, failedCount, context.Canceled // Return context.Canceled for consistent error handling
		}
	}

	// Close batch channel to signal workers to exit
	close(batchChan)

	// Wait for all workers to finish or for an error
	select {
	case <-doneChan:
		// All workers finished successfully
	case err := <-errorChan:
		// Error from a worker
		return successCount, failedCount, err
	case <-ctx.Done():
		// Context cancelled
		m.log.Info("Migration interrupted due to context cancellation")
		return successCount, failedCount, context.Canceled // Return context.Canceled for consistent error handling
	}

	if failedCount > 0 {
		m.log.Warnf("Migration for %s completed with %d failures! Successful: %d, Failed: %d, Total: %d (check DLQ for failed documents)",
			collConfig.SourceCollection, failedCount, successCount, failedCount, migratedCount)
	} else {
		m.log.Infof("Migration for %s completed successfully! Total documents: %d",
			collConfig.SourceCollection, migratedCount)
	}

	// Always clean up backfill checkpoints when the full collection scan completes. If failedCount > 0, the failed documents will be found in the DLQ, and they should be handled explicitly and separately by users.
	tracker.MarkCompleted()
	if err := DeletePartitionCheckpoints(checkpointDir, sourceDB.GetDatabaseName(), collConfig.SourceCollection); err != nil {
		m.log.Warnf("[%s.%s] Failed to delete checkpoint files on completion: %v", sourceDB.GetDatabaseName(), collConfig.SourceCollection, err)
	}
	return successCount, failedCount, nil
}

// processBatch processes a batch of documents
func processBatch(ctx context.Context, collection *mongo.Collection, batch []interface{}, useUpsert bool, dbName, collName string, transformer *FieldTransformer) error {
	if len(batch) == 0 {
		return nil
	}

	transformedBatch, transErr := transformer.TransformBatch(batch, dbName, collName)
	if transErr != nil {
		return fmt.Errorf("failed to transform field names: %w", transErr)
	}
	batch = transformedBatch

	// If upsert mode is enabled, use upsert operations directly
	if useUpsert {
		var models []mongo.WriteModel
		for _, doc := range batch {
			// Extract the _id from the document
			var id interface{}
			switch d := doc.(type) {
			case bson.D:
				for _, elem := range d {
					if elem.Key == "_id" {
						id = elem.Value
						break
					}
				}
			case bson.M:
				id = d["_id"]
			}

			if id != nil {
				// Create a replace model with upsert
				model := mongo.NewReplaceOneModel().
					SetFilter(bson.M{"_id": id}).
					SetReplacement(doc).
					SetUpsert(true)
				models = append(models, model)
			}
		}

		// Execute the bulk write with the upsert models
		if len(models) > 0 {
			_, err := collection.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
			return err
		}
		return nil
	}

	// Try to insert the batch
	_, err := collection.InsertMany(ctx, batch, options.InsertMany().SetOrdered(false))
	if err == nil {
		return nil
	}

	// If there's an error, check if it's a bulk write error with duplicate key errors
	var bulkWriteErr mongo.BulkWriteException
	if errors.As(err, &bulkWriteErr) {
		// Check if all errors are duplicate key errors
		allDuplicateKeyErrors := true
		for _, writeErr := range bulkWriteErr.WriteErrors {
			if writeErr.Code != 11000 { // 11000 is the code for duplicate key error
				allDuplicateKeyErrors = false
				break
			}
		}

		if allDuplicateKeyErrors {
			// Use upsert for the failed documents
			var models []mongo.WriteModel
			failedIndices := make(map[int]bool)

			// Mark the failed indices
			for _, writeErr := range bulkWriteErr.WriteErrors {
				failedIndices[writeErr.Index] = true
			}

			// Create upsert models for the failed documents
			for i, doc := range batch {
				if failedIndices[i] {
					// Extract the _id from the document
					var id interface{}
					switch d := doc.(type) {
					case bson.D:
						for _, elem := range d {
							if elem.Key == "_id" {
								id = elem.Value
								break
							}
						}
					case bson.M:
						id = d["_id"]
					}

					if id != nil {
						// Create a replace model with upsert
						model := mongo.NewReplaceOneModel().
							SetFilter(bson.M{"_id": id}).
							SetReplacement(doc).
							SetUpsert(true)
						models = append(models, model)
					}
				}
			}

			// Execute the bulk write with the upsert models
			if len(models) > 0 {
				_, err := collection.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
				return err
			}

			// If no models were created, return nil
			return nil
		}
	}

	// For other errors, return the original error
	return err
}

// Note: The startLiveReplication function has been replaced by the client-level
// change stream approach in the startClientLevelReplication function.

// migrateCollectionParallel performs a one-time migration of a collection using parallel reads
func (m *Migrator) migrateCollectionParallel(ctx context.Context, sourceDB, targetDB *db.MongoDB, collConfig config.CollectionConfig, totalCount int64, opts MigrateOptions) (int64, int64, error) {
	sourceCollection := sourceDB.GetCollection(collConfig.SourceCollection)
	targetCollection := targetDB.GetCollection(collConfig.TargetCollection)

	checkpointDir := m.getCheckpointDir()

	// Source namespace ("db.coll") for per-collection backfill progress rows.
	bfNS := sourceDB.GetDatabaseName() + "." + collConfig.SourceCollection

	// Resolve partition knobs with any per-collection override applied (the global
	// value otherwise). Same resolution as the enable gate in migrateCollection.
	// The actual partitioner is created inside the resample/fresh branches below;
	// these effective values feed both those partitioners and expectedPartitions
	// so the checkpoint arity stays consistent with a per-collection override.
	_, maxParts, workersPerPart, minDocsPerPart, _ := m.effectivePartitioning(sourceDB.GetDatabaseName(), collConfig.SourceCollection)
	_ = minDocsPerPart // superseded by the byte-aware sizing below; kept for override resolution symmetry

	// Byte-aware partition sizing. Firestore's MongoDB-compatible endpoint caps a
	// single query at 128 MiB of memory, so partitions must be sized by BYTES, not
	// by a static document-count knob: estimate the average document size and split
	// so each partition's scan stays under the per-partition budget. This is the
	// single count reused for both the checkpoint arity (expectedPartitions) and the
	// actual split (SetForcedPartitionCount on the partitioners below), so the two
	// can never drift.
	//
	// Partition size is the PRIMARY control on per-query sort memory. This endpoint's
	// range-sort (find(_id in [start,end)).sort(_id:1)) materializes the whole matched
	// _id RANGE in memory to order it — the .limit() does NOT bound that buffer — so a
	// query's memory scales with the PARTITION's byte span, not the page size. Large
	// partitions therefore blow the 128 MiB ceiling under concurrency no matter how
	// small the page is; small partitions keep every sort buffer small. We size each
	// partition to a small byte budget and rely on there being many of them (bounded
	// read concurrency, below, keeps the aggregate in check). SampleSize must be large
	// enough to yield this many partition boundaries.
	//
	// avgDocSize is floored at a realistic minimum before it feeds the sizing math.
	// EstimateAvgDocSize reads Firestore's collStats.avgObjSize / a $sample, which on
	// this endpoint was observed to UNDER-report by ~2x (estimated ~483 B while the
	// live backfill measured 1024 B/doc). Undercounting makes partitions too large and
	// is exactly what blew the limit under concurrency, so we never size against an
	// estimate below the measured floor.
	const sortSafePartitionBudgetBytes int64 = 8 << 20
	const minAvgDocSizeForSizing int64 = 1024
	avgDocSize := EstimateAvgDocSize(ctx, sourceCollection, m.config.SampleSize, m.log)
	if avgDocSize < minAvgDocSizeForSizing {
		avgDocSize = minAvgDocSizeForSizing
	}
	byteAwareCount, capOverridden := partition.CountByBytes(totalCount, avgDocSize, sortSafePartitionBudgetBytes, maxParts)
	if capOverridden {
		m.log.Warnf("[%s.%s] maxReadPartitions=%d is below the %d partitions needed to keep each partition under Firestore's 128 MiB query limit (%d docs, ~%d bytes/doc); overriding to %d for safety",
			sourceDB.GetDatabaseName(), collConfig.SourceCollection, maxParts, byteAwareCount, totalCount, avgDocSize, byteAwareCount)
	}
	// The partitioner derives boundaries by stepping through SampleSize sampled _ids;
	// it needs several samples per partition or the integer step collapses to 0 and
	// every partition gets identical (overlapping) boundaries — which would re-scan
	// some ranges and, worse, skip others. Byte-safe sizing wants MANY small
	// partitions, so guard the invariant: if the byte-safe count approaches the sample
	// budget, clamp it and warn loudly that SampleSize should be raised. Clamping makes
	// partitions larger (closer to the 128 MiB risk), so this is a signal to fix config,
	// not a silent fallback.
	if maxSafePartitions := m.config.SampleSize / 2; maxSafePartitions >= 1 && byteAwareCount > maxSafePartitions {
		m.log.Warnf("[%s.%s] byte-safe partitioning wants %d partitions but SampleSize=%d only supports ~%d; clamping to %d. RAISE sampleSize (>= 2x the desired partition count) to keep partitions small enough for Firestore's 128 MiB limit under load",
			sourceDB.GetDatabaseName(), collConfig.SourceCollection, byteAwareCount, m.config.SampleSize, maxSafePartitions, maxSafePartitions)
		byteAwareCount = maxSafePartitions
	}
	m.log.Infof("[%s.%s] Byte-aware partitioning: %d docs, ~%d bytes/doc, %d MiB/partition budget => %d partitions",
		sourceDB.GetDatabaseName(), collConfig.SourceCollection, totalCount, avgDocSize, sortSafePartitionBudgetBytes>>20, byteAwareCount)

	// This byte-aware count is authoritative for both checkpoint arity and the
	// partitioner split.
	expectedPartitions := byteAwareCount

	// Evaluate backfill resumption plan for parallel migration
	plan, err := DetermineBackfillResumptionPlan(checkpointDir, sourceDB.GetDatabaseName(), collConfig.SourceCollection, expectedPartitions)
	if err != nil {
		m.log.Warnf("[%s.%s] Error determining backfill resumption plan: %v. Starting fresh.", sourceDB.GetDatabaseName(), collConfig.SourceCollection, err)
		plan = &BackfillResumptionPlan{Mode: ResumptionModeFresh}
	}

	var partitions []bson.D
	var partitionCheckpoints []*PartitionCheckpoint
	var previouslyMigratedDocs int64

	switch plan.Mode {
	case ResumptionModeDirect:
		// Verify and reload all partition checkpoints from disk before updating partition state
		allLoaded := true
		loadedCheckpoints := make([]*PartitionCheckpoint, len(plan.PartitionFilters))
		for i := range plan.PartitionFilters {
			cpPath := GetPartitionCheckpointPath(checkpointDir, sourceDB.GetDatabaseName(), collConfig.SourceCollection, i, len(plan.PartitionFilters))
			cp, loadErr := LoadPartitionCheckpoint(cpPath)
			if loadErr != nil || cp == nil {
				m.log.Warnf("[%s.%s] Failed to reload partition %d checkpoint from %s: %v. Falling back to fresh start.",
					sourceDB.GetDatabaseName(), collConfig.SourceCollection, i, cpPath, loadErr)
				allLoaded = false
				plan.Mode = ResumptionModeFresh
				break
			}
			loadedCheckpoints[i] = cp
		}

		if allLoaded {
			partitions = plan.PartitionFilters
			partitionCheckpoints = loadedCheckpoints
			previouslyMigratedDocs = plan.TotalDocsMigrated()
			m.log.Infof("[%s.%s] Resuming parallel initial backfill directly with %d partitions: ~%d docs previously migrated",
				sourceDB.GetDatabaseName(), collConfig.SourceCollection, len(partitions), previouslyMigratedDocs)
		}
	}

	if plan.Mode != ResumptionModeDirect {
		// Clean up any stale or corrupted checkpoint files from disk for non-direct runs
		if err := DeletePartitionCheckpoints(checkpointDir, sourceDB.GetDatabaseName(), collConfig.SourceCollection); err != nil {
			m.log.Warnf("[%s.%s] Failed to delete stale/corrupted checkpoint files: %v",
				sourceDB.GetDatabaseName(), collConfig.SourceCollection, err)
		}
	}

	if plan.Mode == ResumptionModeResampleWithGlobalMin {
		// Complete historical checkpoints present but partition count changed: fresh sampling and clamp lower bound with global min IDs.
		previouslyMigratedDocs = plan.TotalDocsMigrated()

		partitioner := NewCollectionPartitioner(
			sourceCollection,
			m.log,
			maxParts,
			minDocsPerPart,
			m.config.SampleSize,
			m.config.IDTypeForPartition,
		)
		partitioner.SetForcedPartitionCount(byteAwareCount)
		var partErr error
		rawPartitions, partErr := partitioner.Partition(ctx)
		if partErr != nil {
			return 0, 0, fmt.Errorf("failed to create partitions: %w", partErr)
		}

		// Clamp every partition lower bound to global min safe IDs and skip completed slices
		partitions = ClampPartitionsWithGlobalMinSafeIDs(rawPartitions, plan.GlobalMinSafeIDs)

		partitionCheckpoints = make([]*PartitionCheckpoint, len(partitions))
		for i := range partitions {
			typeProgress := ExtractTypeRangeBoundariesFromFilter(rawPartitions[i])
			if IsFilterSkipped(partitions[i]) {
				for _, boundary := range typeProgress {
					boundary.SavedLastID = boundary.RangeEndID
					if boundary.SavedLastID == nil {
						boundary.SavedLastID = plan.GlobalMinSafeIDs[boundary.BSONType]
					}
				}
			}

			cp := &PartitionCheckpoint{
				Database:                sourceDB.GetDatabaseName(),
				Collection:              collConfig.SourceCollection,
				PartitionIndex:          i,
				TotalSplits:             len(partitions),
				ApproximateDocsMigrated: 0,
				TypeProgress:            typeProgress,
				UpdatedAt:               time.Now().UTC(),
			}
			if i == 0 {
				cp.ApproximateDocsMigrated = previouslyMigratedDocs
			}
			partitionCheckpoints[i] = cp
		}
		m.log.Infof("[%s.%s] Resuming parallel initial backfill with fresh sampling across %d partitions (clamped with global min safe IDs): ~%d docs previously migrated",
			sourceDB.GetDatabaseName(), collConfig.SourceCollection, len(partitions), previouslyMigratedDocs)
	}

	if plan.Mode == ResumptionModeFresh {
		partitioner := NewCollectionPartitioner(
			sourceCollection,
			m.log,
			maxParts,
			minDocsPerPart,
			m.config.SampleSize,
			m.config.IDTypeForPartition,
		)
		partitioner.SetForcedPartitionCount(byteAwareCount)
		var partErr error
		partitions, partErr = partitioner.Partition(ctx)
		if partErr != nil {
			return 0, 0, fmt.Errorf("failed to create partitions: %w", partErr)
		}

		types, discErr := DiscoverPresentBSONTypes(ctx, sourceCollection)
		if discErr != nil {
			m.log.Warnf("[%s.%s] Failed to discover BSON types for parallel backfill: %v", sourceDB.GetDatabaseName(), collConfig.SourceCollection, discErr)
		}

		partitionCheckpoints = make([]*PartitionCheckpoint, len(partitions))
		for i := range partitions {
			typeProgress := ExtractTypeRangeBoundariesFromFilter(partitions[i])
			for _, t := range types {
				if typeProgress[t] == nil {
					typeProgress[t] = &TypeRangeBoundary{BSONType: t}
				}
			}

			cp := &PartitionCheckpoint{
				Database:                sourceDB.GetDatabaseName(),
				Collection:              collConfig.SourceCollection,
				PartitionIndex:          i,
				TotalSplits:             len(partitions),
				ApproximateDocsMigrated: 0,
				TypeProgress:            typeProgress,
				UpdatedAt:               time.Now().UTC(),
			}
			partitionCheckpoints[i] = cp
		}
		previouslyMigratedDocs = 0
		m.log.Infof("Created %d partitions for fresh parallel backfill of collection %s (discovered types: %v)", len(partitions), collConfig.SourceCollection, types)
	}

	// Create retry manager for batch processing
	retryManager := NewRetryManager(
		m.config.RetryConfig.MaxRetries,
		time.Duration(m.config.RetryConfig.BaseDelayMs)*time.Millisecond,
		time.Duration(m.config.RetryConfig.MaxDelayMs)*time.Millisecond,
		m.config.RetryConfig.EnableBatchSplitting,
		m.config.RetryConfig.MinBatchSize,
		m.config.RetryConfig.ConvertInvalidIds,
		m.log,
	)

	checkpointInterval := time.Duration(m.config.CheckpointIntervalMinutes) * time.Minute
	saveThreshold := m.config.SaveThreshold

	// Process partitions in parallel
	var wg sync.WaitGroup
	errorChan := make(chan error, len(partitions))
	doneChan := make(chan struct{})

	// Track progress
	var successCount int64
	var failedCount int64
	var migratedCount int64
	var mu sync.Mutex
	var lastLoggedPercentage int = -1 // Start at -1 to ensure 0% is logged

	// Start a goroutine to periodically report progress at the collection level
	progressCtx, cancelProgress := context.WithCancel(ctx)
	defer cancelProgress()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				mu.Lock()
				currentSuccess := successCount
				currentFailed := failedCount
				cumulativeCount := previouslyMigratedDocs + migratedCount
				if totalCount > 0 && cumulativeCount > totalCount {
					cumulativeCount = totalCount
				}
				currentPercentage := int(float64(cumulativeCount) / float64(totalCount) * 10)

				// Only log when crossing a 10% threshold
				if currentPercentage > lastLoggedPercentage {
					if failedCount > 0 {
						m.log.Infof("Collection %s progress: ~%d/%d documents (%.0f%%) [This run: %d successful, %d failed]",
							collConfig.SourceCollection, cumulativeCount, totalCount, float64(currentPercentage)*10, currentSuccess, currentFailed)
					} else {
						m.log.Infof("Collection %s progress: ~%d/%d documents (%.0f%%)",
							collConfig.SourceCollection, cumulativeCount, totalCount, float64(currentPercentage)*10)
					}
					lastLoggedPercentage = currentPercentage
				}
				mu.Unlock()
			case <-progressCtx.Done():
				return
			case <-doneChan:
				// Log final progress
				mu.Lock()
				cumulativeCount := previouslyMigratedDocs + migratedCount
				if totalCount > 0 && cumulativeCount > totalCount {
					cumulativeCount = totalCount
				}
				m.log.Infof("Collection %s completed: ~%d/%d documents (100%%)",
					collConfig.SourceCollection, cumulativeCount, totalCount)
				mu.Unlock()
				return
			}
		}
	}()

	// Bound how many partitions run concurrently. Byte-aware sizing can produce
	// far more partitions than we want live at once; the semaphore caps concurrent
	// cursors/writers so the connection pools are not overwhelmed, while still
	// letting every partition run eventually.
	concurrency := partitionConcurrency(m.config.TargetMaxPoolSize, workersPerPart, len(partitions))
	m.log.Infof("[%s.%s] Processing %d partitions, up to %d concurrently (%d workers each)",
		sourceDB.GetDatabaseName(), collConfig.SourceCollection, len(partitions), concurrency, workersPerPart)
	partitionSem := make(chan struct{}, concurrency)

	// Process each partition
	for i, partition := range partitions {
		wg.Add(1)
		partitionSem <- struct{}{}

		go func(partitionIndex int, filter bson.D, checkpoint *PartitionCheckpoint) {
			defer wg.Done()
			defer func() { <-partitionSem }()

			m.log.Debugf("Starting partition %d with filter: %v", partitionIndex, filter)

			checkpointPath := GetPartitionCheckpointPath(checkpointDir, sourceDB.GetDatabaseName(), collConfig.SourceCollection, partitionIndex, len(partitions))
			tracker := NewBackfillPartitionTracker(m.log, checkpoint, checkpointPath, checkpointInterval, saveThreshold)
			tracker.Start(ctx)
			defer tracker.Close()

			// Set up parallel batch processing within this partition
			var partitionWg sync.WaitGroup
			partitionBatchChan := make(chan backfillBatchItem, m.config.InitialChannelBufferSize) // Buffer for batches
			partitionErrorChan := make(chan error, 1)                                             // Channel for errors
			partitionDoneChan := make(chan struct{})                                              // Channel to signal completion

			// Track progress for this partition
			var partitionMigratedCount int64
			var partitionMu sync.Mutex // Mutex for thread-safe updates to partitionMigratedCount

			// Start worker pool for this partition (per-collection override applied)
			workerCount := workersPerPart
			if workerCount < 1 {
				workerCount = 1 // Ensure at least 1 worker per partition
			}

			proactiveSkipEnabled := &atomic.Bool{}

			m.log.Debugf("Starting %d workers for partition %d", workerCount, partitionIndex)

			for w := 0; w < workerCount; w++ {
				partitionWg.Add(1)
				go func(workerID int) {
					defer partitionWg.Done()

					for item := range partitionBatchChan {
						if opts.BackfillStatsManager != nil {
							opts.BackfillStatsManager.RecordWorkerReceived(int64(len(item.batch)))
						}
						globalWorkerID := partitionIndex*100 + workerID
						succeeded, failed, err := m.writeBatch(ctx, targetCollection, item.batch, sourceDB.GetDatabaseName(), collConfig.SourceCollection, opts, retryManager, globalWorkerID, proactiveSkipEnabled)
						if err != nil {
							select {
							case partitionErrorChan <- fmt.Errorf("worker %d in partition %d failed: %w", workerID, partitionIndex, err):
							default:
							}
							return
						}

						if ctx.Err() == nil {
							tracker.AckBatch(item.seq, succeeded)
						}

						// Update progress
						partitionMu.Lock()
						partitionMigratedCount += int64(len(item.batch))
						partitionMu.Unlock()

						// Update overall progress counter
						mu.Lock()
						successCount += succeeded
						failedCount += failed
						migratedCount += int64(len(item.batch))
						mu.Unlock()
					}
				}(w)
			}

			// Start a goroutine to close channels when all batches are processed
			go func() {
				partitionWg.Wait()
				close(partitionDoneChan)
			}()

			// Process documents and create batches via keyset (seek) pagination.
			// Firestore's MongoDB-compatible endpoint buffers a query's full result
			// in memory and rejects any single query that would exceed its 128 MiB
			// ceiling, so we never issue one unbounded range scan over a partition.
			// Instead we walk the partition's _id range in bounded pages: each query
			// is find(range ∧ _id > lastReadID).sort(_id:1).limit(pageSize). The sort
			// is mandatory — without it this endpoint does NOT return an _id-range
			// scan in _id order, which would make keyset seeking skip documents. The
			// sort's cost is that the endpoint materializes the scanned range in
			// memory, so correctness against the 128 MiB limit rests on the range
			// being small: partitions are sized (below) so a whole partition sorts
			// well under the limit, and the page-shrink backstop halves pageSize on
			// any residual blow. Seeking past the last _id read means a page that
			// fails part-way is retried from exactly where it stopped, never
			// re-emitting a buffered document.
			var batch []interface{}
			var batchCount int
			var lastReadID interface{}
			var haveLast bool

			// Parse the partition's _id range once. When it is a plain contiguous
			// range (the common case), the keyset walk queries with an OPEN lower
			// bound and enforces the upper bound CLIENT-SIDE (pageReachedEnd), which
			// avoids the two-bounded endgame query that blows Firestore's 128 MiB
			// limit. Exotic filters ($type/$or/$exists) keep the legacy both-bounds
			// page filter + page-shrink backstop.
			bounds := parseIDRangeBounds(filter)
			var pageReachedEnd bool // set by readPage when the upper bound is reached

			// Page size in documents. The page is the PRIMARY control on per-query
			// memory now: each keyset query sorts and buffers at most this many bytes'
			// worth of documents, so with bounded read concurrency the aggregate stays
			// far under Firestore's 128 MiB ceiling (e.g. 4 MiB/page * 8 concurrent
			// partitions ~= 32 MiB). Sized against the floored avg-doc-size; the
			// halving retry and floor back-off below cover byte-heavy regions the
			// estimate still misses.
			const pageBudgetBytes int64 = 4 << 20
			pageSize := pageBudgetBytes / avgDocSize
			if pageSize < 1000 {
				pageSize = 1000
			}
			if pageSize > 50000 {
				pageSize = 50000
			}

			// readPage reads up to pageSize documents matching pageFilter in _id
			// order, batching them out to the workers. It reports how many documents
			// it emitted and whether it stopped because the query hit the 128 MiB
			// memory limit (memLimited), so the driver can shrink pageSize and retry
			// the unread remainder. [Firestore compat] noCursorTimeout is rejected by
			// the endpoint; omit it.
			readPage := func(pageFilter bson.D, pageSize int64) (emitted int, memLimited bool, err error) {
				// [Firestore compat] SetSort(_id) is REQUIRED for keyset correctness:
				// an unsorted _id-range scan does NOT come back in _id order on this
				// endpoint (verified — sessions/products returned descending pairs),
				// which would make _id>lastReadID seeking skip data. The sort's cost
				// is that the endpoint materializes the scanned range in memory, so a
				// too-dense partition blows the 128 MiB limit; we keep that bounded by
				// sizing partitions small enough to sort whole (see the partition
				// budget where this collection is split) and by the page-shrink
				// backstop below. The decode loop still asserts ascending order as a
				// cheap defense-in-depth against any future regression.
				cursor, cerr := sourceCollection.Find(ctx, pageFilter, options.Find().
					SetSort(bson.D{{Key: "_id", Value: 1}}).
					SetLimit(pageSize).
					SetBatchSize(int32(m.config.InitialReadBatchSize)))
				if cerr != nil {
					// [Firestore compat] The 128 MiB memory-limit error can surface
					// HERE, at cursor creation, not only during iteration: the driver
					// executes the query (and fetches the first batch) eagerly, so a
					// page that over-buffers fails Find() itself. This path must report
					// memLimited exactly like the cursor.Err() path below, otherwise
					// the page-shrink backstop never runs and the whole partition is
					// abandoned (silent data loss).
					if isQueryMemoryLimitError(cerr) {
						return 0, true, cerr
					}
					return 0, false, fmt.Errorf("failed to create cursor for partition %d: %w", partitionIndex, cerr)
				}
				// [Safety Fix 5: Memory Leak on Server] Ensure the cursor is closed to
				// prevent server-side resource leakage on every page.
				defer cursor.Close(ctx)

				for {
					// Check for errors from workers
					select {
					case werr := <-partitionErrorChan:
						return emitted, false, werr
					default:
						// No errors, continue processing
					}

					// Get next document
					readStart := time.Now()
					if !cursor.Next(ctx) {
						break
					}

					// Decode document
					var doc bson.D
					if decErr := cursor.Decode(&doc); decErr != nil {
						return emitted, false, fmt.Errorf("failed to decode document in partition %d: %w", partitionIndex, decErr)
					}
					readDuration := time.Since(readStart)

					if opts.BackfillStatsManager != nil {
						opts.BackfillStatsManager.RecordRead(bfNS, readDuration, len(cursor.Current))
					}

					// Advance the keyset cursor position AND prove the scan is
					// strictly ascending in _id. Keyset seeking (_id > lastReadID) is
					// only correct if each page arrives in ascending _id order; the
					// server-side sort above guarantees that, and this per-document
					// assertion is cheap defense-in-depth. It is not paranoia: an
					// UNSORTED scan on this endpoint was observed returning descending
					// _id pairs, which would silently skip every document between an
					// out-of-order pair. So if ordering is ever violated (or an _id is
					// of a type we cannot compare) we FAIL LOUD rather than lose data.
					if id, ok := bsonIDValue(doc); ok {
						// CLIENT-SIDE upper bound. Keyset pages query with an open
						// lower bound (no $lt), so the cursor keeps returning _ids past
						// this partition's end (into the next partition's territory).
						// Stop at the boundary WITHOUT emitting the boundary document:
						// partitions are half-open [start, end), so an _id == end (or
						// beyond) belongs to the next partition and is read there. This
						// is what makes the open-lower query safe — no doc is skipped or
						// double-migrated.
						if bounds.ok && bounds.hasUpper {
							if cmp, cok := compareBSONID(id, bounds.upper); cok {
								past := cmp >= 0
								if bounds.upperInclusive {
									past = cmp > 0
								}
								if past {
									pageReachedEnd = true
									break
								}
							} else {
								return emitted, false, fmt.Errorf("partition %d: cannot compare _id to partition upper bound (got %T vs %T); aborting to avoid silent data loss", partitionIndex, id, bounds.upper)
							}
						}
						if haveLast {
							cmp, cok := compareBSONID(id, lastReadID)
							if !cok {
								return emitted, false, fmt.Errorf("partition %d: cannot compare _id types to verify scan order (got %T after %T); refusing to continue unsorted keyset read to avoid silent data loss", partitionIndex, id, lastReadID)
							}
							if cmp <= 0 {
								return emitted, false, fmt.Errorf("partition %d: unsorted range scan returned _id out of ascending order (%v after %v); aborting to avoid skipping documents", partitionIndex, id, lastReadID)
							}
						}
						lastReadID = id
						haveLast = true
					} else {
						return emitted, false, fmt.Errorf("partition %d: document has no _id; cannot keyset-page safely", partitionIndex)
					}

					// Add to batch
					batch = append(batch, doc)
					batchCount++
					emitted++

					if batchCount >= m.config.InitialWriteBatchSize {
						seq := tracker.RegisterBatch(batch)
						item := backfillBatchItem{batch: batch, seq: seq}
						sendStart := time.Now()
						select {
						case partitionBatchChan <- item:
							if opts.BackfillStatsManager != nil {
								opts.BackfillStatsManager.RecordIngestQueueStall(time.Since(sendStart))
							}
							// Batch sent to worker
						case werr := <-partitionErrorChan:
							// Error from a worker
							return emitted, false, werr
						case <-ctx.Done():
							// Context cancelled
							return emitted, false, ctx.Err()
						}

						// Reset batch
						batch = nil
						batchCount = 0
					}
				}

				// Check for cursor errors
				if cerr := cursor.Err(); cerr != nil {
					if isQueryMemoryLimitError(cerr) {
						return emitted, true, cerr
					}
					return emitted, false, fmt.Errorf("cursor error in partition %d: %w", partitionIndex, cerr)
				}
				return emitted, false, nil
			}

			// Drive the keyset walk: page through the partition until a short page
			// signals the range is exhausted, shrinking pageSize on any 128 MiB hit.
			// The page-shrink handles byte-heavy regions; the floor-retry below
			// handles the residual case where even a single-document sort blows,
			// which cannot be a real per-query size problem and is therefore a
			// transient shared-memory-pressure blow (see partitionConcurrency).
			const maxFloorRetries = 8
			const floorRecoverPageSize int64 = 1000
			floorRetries := 0
			readPartition := func() error {
				// A partition whose filter is the "skip" sentinel ({_id:{$exists:false}})
				// has already been fully migrated in a prior run (the resume planner
				// emits this for completed partitions). It must NOT be executed as a
				// query: on Firestore's MongoDB-compatible endpoint {_id:{$exists:false}}
				// cannot use the _id index (a "field is absent" predicate is
				// unindexable), so the endpoint scans and materializes the ENTIRE
				// collection and blows the 128 MiB query-memory limit — even at limit 1
				// and even with no sort (verified). Skipping the read entirely is both
				// correct (the partition is done, zero documents remain) and the fix for
				// the resume-path 128 MiB blow.
				if IsFilterSkipped(filter) {
					m.log.Debugf("[%s.%s] partition %d already complete (skip sentinel); no read issued",
						sourceDB.GetDatabaseName(), collConfig.SourceCollection, partitionIndex)
					return nil
				}
				for {
					// Build the page filter. Fast path (bounds.ok): an OPEN
					// lower-bound query with NO upper bound — {_id:{$gt:lastReadID}}
					// once we have read a document, else the partition's own lower
					// bound (or {} for a partition that starts at the collection
					// minimum). The upper bound is enforced client-side in readPage.
					// This is what avoids the two-bounded endgame 128 MiB blow. Legacy
					// path (exotic filter): the merged both-bounds filter + page-shrink.
					var pageFilter bson.D
					if bounds.ok {
						switch {
						case haveLast:
							pageFilter = bson.D{{Key: "_id", Value: bson.D{{Key: "$gt", Value: lastReadID}}}}
						case bounds.hasLower && bounds.lowerInclusive:
							pageFilter = bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: bounds.lower}}}}
						case bounds.hasLower:
							pageFilter = bson.D{{Key: "_id", Value: bson.D{{Key: "$gt", Value: bounds.lower}}}}
						default:
							pageFilter = bson.D{} // partition starts at collection min
						}
					} else {
						pageFilter = filter
						if haveLast {
							pageFilter = mergeKeysetPageFilter(filter, lastReadID)
						}
					}

					pageReachedEnd = false
					emitted, memLimited, err := readPage(pageFilter, pageSize)
					if err != nil {
						if memLimited && pageSize > 1 {
							newSize := pageSize / 2
							if newSize < 1 {
								newSize = 1
							}
							m.log.Warnf("[%s.%s] partition %d hit Firestore's 128 MiB query limit; shrinking page size %d -> %d and retrying the unread remainder",
								sourceDB.GetDatabaseName(), collConfig.SourceCollection, partitionIndex, pageSize, newSize)
							pageSize = newSize
							continue
						}
						if memLimited && pageSize == 1 {
							// A one-document sort cannot legitimately need 128 MiB, so
							// this is transient pressure from other concurrent sorts on
							// the shared endpoint. Back off (giving peers time to finish
							// and release memory) and retry the SAME single-doc page.
							// Keyset seeking means the retry resumes exactly where we
							// stopped, never re-emitting a document.
							if floorRetries < maxFloorRetries {
								floorRetries++
								backoff := time.Duration(1<<uint(floorRetries-1)) * time.Second
								if backoff > 5*time.Second {
									backoff = 5 * time.Second
								}
								m.log.Warnf("[%s.%s] partition %d hit the 128 MiB limit even at page size 1 (transient concurrent-memory pressure); backing off %s and retrying (%d/%d)",
									sourceDB.GetDatabaseName(), collConfig.SourceCollection, partitionIndex, backoff, floorRetries, maxFloorRetries)
								select {
								case <-time.After(backoff):
								case <-ctx.Done():
									return ctx.Err()
								}
								continue
							}
							return fmt.Errorf("partition %d: Firestore's 128 MiB query limit persists at page size 1 after %d back-off retries: %w", partitionIndex, maxFloorRetries, err)
						}
						return err
					}

					// The partition is exhausted when either we reached its upper
					// bound client-side (fast path), or a page came back shorter than
					// the limit it was issued with (end of the collection, or the
					// legacy both-bounds range ran out). The short-page check MUST use
					// the page size actually issued, before any recovery rewrites it.
					if pageReachedEnd || int64(emitted) < pageSize {
						return nil
					}

					// A full page succeeded: clear the transient-blow counter. If we had
					// crawled down to a tiny page, recover to a modest size so the rest
					// of a large partition does not read one document at a time.
					floorRetries = 0
					if pageSize < floorRecoverPageSize {
						pageSize = floorRecoverPageSize
					}
				}
			}

			if err := readPartition(); err != nil {
				close(partitionBatchChan)
				errorChan <- err
				return
			}

			// Process any remaining documents (flushed once, after every sub-range).
			if len(batch) > 0 {
				seq := tracker.RegisterBatch(batch)
				item := backfillBatchItem{batch: batch, seq: seq}
				sendStart := time.Now()
				select {
				case partitionBatchChan <- item:
					if opts.BackfillStatsManager != nil {
						opts.BackfillStatsManager.RecordIngestQueueStall(time.Since(sendStart))
					}
					// Final batch sent to worker
				case err := <-partitionErrorChan:
					// Error from a worker
					close(partitionBatchChan)
					errorChan <- err
					return
				case <-ctx.Done():
					// Context cancelled
					close(partitionBatchChan)
					errorChan <- ctx.Err()
					return
				}
			}

			// Close batch channel to signal workers to exit
			close(partitionBatchChan)

			// Wait for all workers to finish or for an error
			select {
			case <-partitionDoneChan:
				// All workers finished successfully
			case err := <-partitionErrorChan:
				// Error from a worker
				errorChan <- err
				return
			case <-ctx.Done():
				// Context cancelled
				errorChan <- ctx.Err()
				return
			}

			// Mark partition backfill completed in tracker
			tracker.MarkCompleted()

			// Log partition completion at debug level only
			m.log.Debugf("Partition %d completed: %d documents",
				partitionIndex, partitionMigratedCount)
		}(i, partition, partitionCheckpoints[i])
	}

	// Wait for all partitions to complete or for an error
	go func() {
		wg.Wait()
		close(errorChan)
		close(doneChan) // Signal progress reporting goroutine to exit
	}()

	// Check for errors
	for err := range errorChan {
		if err != nil {
			return successCount, failedCount, err
		}
	}

	if failedCount > 0 {
		m.log.Warnf("Parallel migration for %s completed with %d failures! Successful: %d, Failed: %d, Total: %d",
			collConfig.SourceCollection, failedCount, successCount, failedCount, migratedCount)
	} else {
		m.log.Infof("Parallel migration for %s completed successfully! Total documents: %d",
			collConfig.SourceCollection, migratedCount)
	}

	// Always clean up backfill checkpoints when the full collection scan completes.
	if err := DeletePartitionCheckpoints(checkpointDir, sourceDB.GetDatabaseName(), collConfig.SourceCollection); err != nil {
		m.log.Warnf("[%s.%s] Failed to delete checkpoint files on completion: %v", sourceDB.GetDatabaseName(), collConfig.SourceCollection, err)
	}

	return successCount, failedCount, nil
}

// Helper functions for min/max
// func min(a, b int) int {
// 	if a < b {
// 		return a
// 	}
// 	return b
// }

// syncIndexes synchronizes indexes from source to target collections.
// When pair.Target.SyncAllIndexes is true, it syncs ALL indexes (except _id_) for every
// collection in the collections list. Otherwise it uses the explicit pair.Target.Indexes config.
func (m *Migrator) syncIndexes(ctx context.Context, sourceDB, targetDB *db.MongoDB, pair config.DatabasePair, collections []config.CollectionConfig) error {
	if m.DryRun {
		m.log.Info("[Dry Run] Skipping index synchronization")
		return nil
	}

	m.log.Info("Starting async index synchronization (fire-and-forget)...")

	var indexCount int

	// Always create the _id index on every target collection, independent of
	// SyncAllIndexes. Firestore does NOT auto-create it (real MongoDB does), so
	// without this there is no index backing _id lookups/ordering. Idempotent:
	// skipped when the target already has "_id_".
	for _, collConfig := range collections {
		targetCollName := collConfig.TargetCollection
		if m.targetHasIndex(ctx, targetDB, targetCollName, "_id_") {
			m.log.Infof("_id index already exists on target collection '%s', skipping", targetCollName)
			continue
		}
		m.log.Infof("Launching async _id index creation on target collection '%s'", targetCollName)
		targetDB.CreateIDIndexAsync(pair.Target.ConnectionString, targetCollName)
		indexCount++
	}

	if pair.Target.SyncAllIndexes {
		// Auto-sync all indexes for every migrated collection
		m.log.Info("SyncAllIndexes enabled: syncing all indexes (excluding _id_) for all collections")

		for _, collConfig := range collections {
			// Get all indexes from source collection
			sourceIndexes, err := sourceDB.ListIndexes(ctx, collConfig.SourceCollection)
			if err != nil {
				m.log.Warnf("Failed to list indexes for %s: %v (continuing anyway)", collConfig.SourceCollection, err)
				continue
			}

			targetCollName := collConfig.TargetCollection

			// List existing indexes on the target to skip already-created ones
			existingIndexNames := make(map[string]bool)
			targetIndexes, err := targetDB.ListIndexes(ctx, targetCollName)
			if err != nil {
				m.log.Debugf("Could not list target indexes for %s: %v (will attempt all)", targetCollName, err)
			} else {
				for _, idx := range targetIndexes {
					if name, ok := idx["name"].(string); ok {
						existingIndexNames[name] = true
					}
				}
			}

			for _, indexDef := range sourceIndexes {
				indexName, ok := indexDef["name"].(string)
				if !ok {
					m.log.Warnf("Index definition missing name field: %v", indexDef)
					continue
				}

				// Skip _id_ index (MongoDB creates this automatically)
				if indexName == "_id_" {
					continue
				}

				// Skip if index already exists on target
				if existingIndexNames[indexName] {
					m.log.Infof("Index '%s' already exists on target collection '%s', skipping", indexName, targetCollName)
					continue
				}

				// Fire-and-forget: launch async index creation with a dedicated client
				m.log.Infof("Launching async index creation: '%s' on target collection '%s'", indexName, targetCollName)
				targetDB.CreateIndexFromDefinitionAsync(pair.Target.ConnectionString, targetCollName, indexDef)
				indexCount++
			}
		}
	}

	// Also process explicit index configs if provided
	for _, indexConfig := range pair.Target.Indexes {
		// Get all indexes from source collection
		sourceIndexes, err := sourceDB.ListIndexes(ctx, indexConfig.SourceCollection)
		if err != nil {
			m.log.Warnf("Failed to list indexes for %s: %v (continuing anyway)", indexConfig.SourceCollection, err)
			continue
		}

		// Get target collection name
		targetCollName := m.getTargetCollectionName(indexConfig.SourceCollection, pair)

		// List existing indexes on the target to skip already-created ones
		existingIndexNames := make(map[string]bool)
		targetIndexes, err := targetDB.ListIndexes(ctx, targetCollName)
		if err != nil {
			m.log.Debugf("Could not list target indexes for %s: %v (will attempt all)", targetCollName, err)
		} else {
			for _, idx := range targetIndexes {
				if name, ok := idx["name"].(string); ok {
					existingIndexNames[name] = true
				}
			}
		}

		// Filter to only requested indexes
		for _, indexDef := range sourceIndexes {
			indexName, ok := indexDef["name"].(string)
			if !ok {
				m.log.Warnf("Index definition missing name field: %v", indexDef)
				continue
			}

			// Skip _id_ index (MongoDB creates this automatically)
			if indexName == "_id_" {
				continue
			}

			// Check if this index is in the requested list
			found := false
			for _, requestedName := range indexConfig.IndexNames {
				if indexName == requestedName {
					found = true
					break
				}
			}

			if !found {
				continue
			}

			// Skip if index already exists on target
			if existingIndexNames[indexName] {
				m.log.Infof("Index '%s' already exists on target collection '%s', skipping", indexName, targetCollName)
				continue
			}

			// Fire-and-forget: launch async index creation with a dedicated client
			m.log.Infof("Launching async index creation: '%s' on target collection '%s'", indexName, targetCollName)
			targetDB.CreateIndexFromDefinitionAsync(pair.Target.ConnectionString, targetCollName, indexDef)
			indexCount++
		}
	}

	m.log.Infof("Launched %d async index creation tasks. Proceeding with data migration.", indexCount)
	return nil
}

// targetHasIndex reports whether the target collection already has an index with
// the given name. Errors (e.g. collection not yet created) are treated as "no",
// so the caller attempts creation. Used to keep index creation idempotent.
func (m *Migrator) targetHasIndex(ctx context.Context, targetDB *db.MongoDB, collection, indexName string) bool {
	idxs, err := targetDB.ListIndexes(ctx, collection)
	if err != nil {
		return false
	}
	for _, idx := range idxs {
		if n, ok := idx["name"].(string); ok && n == indexName {
			return true
		}
	}
	return false
}

// getTargetCollectionName gets the target collection name for a source collection
func (m *Migrator) getTargetCollectionName(sourceCollName string, pair config.DatabasePair) string {
	// Search through collections configuration for explicit mapping
	for _, coll := range pair.Target.Collections {
		if coll.SourceCollection == sourceCollName {
			return coll.TargetCollection
		}
	}

	// If not found in explicit config, assume same name as source
	m.log.Debugf("No explicit mapping for %s, using same collection name on target", sourceCollName)
	return sourceCollName
}

// logFailedIndexes checks for and logs any indexes that failed to be created.
func (m *Migrator) logFailedIndexes(targetDB *db.MongoDB) {
	failed := targetDB.GetFailedIndexes()
	if len(failed) == 0 {
		return
	}
	m.log.Warnf("=== %d INDEX(ES) FAILED TO CREATE ===", len(failed))
	for i, f := range failed {
		m.log.Warnf("  [%d] Collection: %s, Index: %s, Error: %v", i+1, f.Collection, f.IndexName, f.Error)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// writeBatch processes and writes a batch of documents.
// - If opts.DLQ is provided, it uses resilient writes (upsert fallback + DLQ routing) and returns counts.
// - If opts.DLQ is nil, it uses fail-fast writes (standard InsertMany + RetryManager) and aborts on errors.
func (m *Migrator) writeBatch(ctx context.Context, targetCol *mongo.Collection, batch []interface{}, sourceDB, sourceCollection string, opts MigrateOptions, retryManager *RetryManager, workerID int, proactiveSkipEnabled *atomic.Bool) (int64, int64, error) {
	convertInvalidIds := m.config.RetryConfig.ConvertInvalidIds && m.isLive
	transformer := NewFieldTransformer(m.config.DropEmptyFieldNames, m.config.ConvertLongFieldNamesInNestedDocs, convertInvalidIds, m.log)

	// Source namespace ("db.coll") for per-collection backfill progress rows.
	bfNS := sourceDB + "." + sourceCollection

	writeStart := time.Now()

	if opts.DLQ != nil {
		originalBatch := make([]interface{}, len(batch))
		copy(originalBatch, batch)

		// Resilient Mode: Use DLQ Fallback (exactly identical to client_stream.go)
		transformedBatch, err := transformer.TransformBatch(batch, sourceDB, sourceCollection)
		if err != nil {
			m.log.Errorf("Field name transformation failed for batch in %s.%s: %v", sourceDB, sourceCollection, err)
			for _, doc := range batch {
				docID := extractDocID(doc)
				opts.DLQ.WriteFailed(sourceDB, sourceCollection, docID, err, "initial", "insert", doc, time.Time{})
			}
			writeDuration := time.Since(writeStart)
			if opts.BackfillStatsManager != nil {
				opts.BackfillStatsManager.RecordBulkWrite(writeDuration)
				opts.BackfillStatsManager.RecordWriteResult(bfNS, 0, int64(len(batch)), 0, int64(len(batch)), workerID)
			}
			return 0, int64(len(batch)), nil
		}
		batch = transformedBatch

		var batchFailed int64
		var duplicateKeys int64
		var dlqCount int64

		if proactiveSkipEnabled != nil && proactiveSkipEnabled.Load() {
			var ids []interface{}
			for _, doc := range batch {
				id := extractDocID(doc)
				if id != nil {
					ids = append(ids, id)
				}
			}

			if len(ids) > 0 {
				findCtx, cancelFind := context.WithTimeout(ctx, 30*time.Second)
				filter := bson.M{"_id": bson.M{"$in": ids}}
				projectionOpts := options.Find().SetProjection(bson.M{"_id": 1})
				cursor, findErr := targetCol.Find(findCtx, filter, projectionOpts)
				if findErr == nil {
					existingKeys := make(map[string]bool)
					for cursor.Next(findCtx) {
						var res struct {
							ID interface{} `bson:"_id"`
						}
						if err := cursor.Decode(&res); err == nil && res.ID != nil {
							existingKeys[toComparableIDKey(res.ID)] = true
						}
					}
					cursor.Close(findCtx)
					cancelFind()

					if len(existingKeys) == len(batch) {
						duplicateKeys = int64(len(batch))
						m.log.Debugf("[%s.%s] Proactively skipped entire batch of %d documents (already exist)", sourceDB, sourceCollection, len(batch))
						if opts.BackfillStatsManager != nil {
							opts.BackfillStatsManager.RecordWriteResult(bfNS, 0, 0, duplicateKeys, 0, workerID)
						}
						return 0, 0, nil
					} else if len(existingKeys) > 0 {
						var filteredBatch []interface{}
						var filteredOriginal []interface{}
						var skippedCount int64
						for idx, doc := range batch {
							id := extractDocID(doc)
							if existingKeys[toComparableIDKey(id)] {
								skippedCount++
							} else {
								filteredBatch = append(filteredBatch, doc)
								filteredOriginal = append(filteredOriginal, originalBatch[idx])
							}
						}
						m.log.Debugf("[%s.%s] Proactively skipped %d/%d duplicate documents", sourceDB, sourceCollection, skippedCount, len(batch))
						duplicateKeys += skippedCount
						batch = filteredBatch
						originalBatch = filteredOriginal
						proactiveSkipEnabled.Store(false)
					} else {
						proactiveSkipEnabled.Store(false)
					}
				} else {
					m.log.Warnf("[%s.%s] Proactive ID presence check failed: %v", sourceDB, sourceCollection, findErr)
					cancelFind()
				}
			}
		}

		// [Safety Fix 2: Connection Pool Starvation] Wait on Rate Limiting BEFORE checking out connections/sessions from target database pool.
		// This prevents worker threads from holding onto checked-out sessions in an idle state while blocked by rate limits,
		// which would starve other threads/replicators of available pool connections.
		if opts.Throttler != nil {
			if err := opts.Throttler.Wait(ctx, len(batch)); err != nil {
				return 0, 0, err
			}
		}

		// [Safety Fix 3: Context Timeout Shrinkage] Construct the query-level timeout context AFTER returning from the throttler wait block.
		// Constructing it beforehand would cause the rate-limit wait delay to count against the execution timeout
		// (timeout shrinkage), leading to premature write context cancellation failures.
		bulkCtx, cancelBulk := context.WithTimeout(ctx, 90*time.Second)
		bulkStart := time.Now()
		_, insertErr := targetCol.InsertMany(bulkCtx, batch, options.InsertMany().SetOrdered(false))
		bulkDuration := time.Since(bulkStart)
		cancelBulk()

		if opts.Throttler != nil {
			opts.Throttler.ReportResult(bulkDuration, isSystemError(insertErr))
		}

		if insertErr == nil {
			if opts.BackfillStatsManager != nil {
				opts.BackfillStatsManager.RecordBulkWrite(bulkDuration)
				opts.BackfillStatsManager.RecordWriteResult(bfNS, int64(len(batch)), 0, 0, 0, workerID)
			}
			return int64(len(batch)), 0, nil
		}

		if opts.BackfillStatsManager != nil {
			opts.BackfillStatsManager.RecordBulkWrite(bulkDuration)
		}

		// Use errors.As instead of direct type assertion (insertErr.(mongo.BulkWriteException))
		// because the driver or retry wrapper may wrap the underlying BulkWriteException.
		var bulkWriteException mongo.BulkWriteException
		ok := errors.As(insertErr, &bulkWriteException)
		if ok {
			m.log.Debugf("Bulk insert partially failed for %s.%s: %d failed",
				sourceDB, sourceCollection, len(bulkWriteException.WriteErrors))

			if len(bulkWriteException.WriteErrors) == len(batch) {
				allDuplicates := true
				for _, writeErr := range bulkWriteException.WriteErrors {
					if writeErr.Code != 11000 {
						allDuplicates = false
						break
					}
				}
				if allDuplicates && proactiveSkipEnabled != nil {
					proactiveSkipEnabled.Store(true)
				}
			}

			for _, writeErr := range bulkWriteException.WriteErrors {
				var errDocID interface{}
				if writeErr.Index < len(batch) {
					errDocID = extractDocID(batch[writeErr.Index])
				}

				m.log.Debugf("[%s.%s] Insert error at index %d, _id=%v: %v", sourceDB, sourceCollection, writeErr.Index, errDocID, writeErr.Message)

				if writeErr.Code == 11000 && writeErr.Index < len(batch) {
					// Case 1: The server responded successfully but flagged a duplicate key error (code 11000).
					// Since we have 100% certainty that the document exists on the target and this is a backfill
					// of identical data, we can safely skip overwriting it to optimize performance and disk writes.
					m.log.Debugf("[%s.%s] Skipping duplicate document _id=%v at index %d", sourceDB, sourceCollection, errDocID, writeErr.Index)
					duplicateKeys++
				} else if writeErr.Index < len(batch) {
					// Note: We do not classify the error here to decide whether to skip the fallback.
					// Attempting fallback write first is safer: (1) if classification regexes are incomplete,
					// we avoid dropping valid writes; (2) some errors are batch-specific and succeed individually.
					doc := batch[writeErr.Index]
					id := extractDocID(doc)

					if id != nil {
						filter := bson.M{"_id": id}
						if opts.BackfillStatsManager != nil {
							opts.BackfillStatsManager.IncrementSequentialRetries("replace", 1)
						}
						err := m.executeWithRetry(ctx, retryManager, func() error {
							fallbackCtx, cancelFallback := context.WithTimeout(ctx, 10*time.Second)
							defer cancelFallback()
							_, replaceErr := targetCol.ReplaceOne(fallbackCtx, filter, doc, options.Replace().SetUpsert(true))
							return replaceErr
						})
						if err != nil {
							m.log.Errorf("[%s.%s] Retry upsert failed for document _id=%v: %v", sourceDB, sourceCollection, id, err)
							batchFailed++
							dlqCount++
							opts.DLQ.WriteFailed(sourceDB, sourceCollection, id, err, "initial", "insert", originalBatch[writeErr.Index], time.Time{})
						}
					} else {
						batchFailed++
						dlqCount++
						opts.DLQ.WriteFailed(sourceDB, sourceCollection, nil, fmt.Errorf("missing _id"), "initial", "insert", originalBatch[writeErr.Index], time.Time{})
					}
				}
			}
		} else {
			// Handle non-bulk write errors (e.g. network partition or context timeout)
			bulkRetrySucceeded := false
			if retryManager != nil && insertErr != context.Canceled {
				errType := retryManager.ClassifyError(insertErr)
				if errType == ErrorTypeConnection || errType == ErrorTypeContention {
					m.log.Infof("Transient error detected for %s.%s. Retrying bulk insert with backoff...", sourceDB, sourceCollection)
					retryErr := retryManager.RetryWithBackoff(ctx, func() error {
						retryCtx, cancelRetry := context.WithTimeout(ctx, 90*time.Second)
						defer cancelRetry()
						_, retryInsertErr := targetCol.InsertMany(retryCtx, batch, options.InsertMany().SetOrdered(false))
						return retryInsertErr
					})
					if retryErr == nil {
						m.log.Infof("Bulk insert for %s.%s succeeded after retry", sourceDB, sourceCollection)
						bulkRetrySucceeded = true
					} else {
						// Optimization: If the bulk retry failed solely due to duplicate key errors,
						// it means the documents were successfully written during the first (timed-out) attempt
						// and the remaining documents succeeded in the retry attempt. We can skip the slow fallback.
						var bulkWriteException mongo.BulkWriteException
						if errors.As(retryErr, &bulkWriteException) {
							allDuplicates := true
							for _, writeErr := range bulkWriteException.WriteErrors {
								if writeErr.Code != 11000 {
									allDuplicates = false
									break
								}
							}
							if allDuplicates {
								successCount := int64(len(batch) - len(bulkWriteException.WriteErrors))
								duplicateKeys = int64(len(bulkWriteException.WriteErrors))
								m.log.Infof("Bulk insert for %s.%s succeeded after retry with %d duplicate keys skipped (Optimization)",
									sourceDB, sourceCollection, duplicateKeys)
								bulkRetrySucceeded = true
								if proactiveSkipEnabled != nil {
									proactiveSkipEnabled.Store(true)
								}
								if opts.BackfillStatsManager != nil {
									opts.BackfillStatsManager.RecordWriteResult(bfNS, successCount, 0, duplicateKeys, 0, workerID)
								}
								return successCount, 0, nil
							}
						}
						m.log.Warnf("Bulk insert for %s.%s still failed after retries: %v. Falling back to individual operations.", sourceDB, sourceCollection, retryErr)
					}
				}
			}

			if !bulkRetrySucceeded {
				// Fall back to individual operations with upsert for all documents
				for idx, doc := range batch {
					if opts.BackfillStatsManager != nil {
						opts.BackfillStatsManager.IncrementSequentialRetries("insert", 1)
					}
					err := m.executeWithRetry(ctx, retryManager, func() error {
						fallbackCtx, cancelFallback := context.WithTimeout(ctx, 10*time.Second)
						defer cancelFallback()
						_, insertErr := targetCol.InsertOne(fallbackCtx, doc)
						return insertErr
					})
					if err != nil {
						// [Safety Fix 4: Resilient Fallback Stale Overwrites]
						// If the individual InsertOne fails with a duplicate key error (code 11000) during resilient fallback,
						// we skip replacing it. Using ReplaceOne(upsert: true) here could revert newer updates or deletions
						// applied concurrently by the live stream replicator.
						isDup := false
						if writeErr, ok := err.(mongo.WriteException); ok {
							for _, we := range writeErr.WriteErrors {
								if we.Code == 11000 {
									isDup = true
									break
								}
							}
						}
						if !isDup && (strings.Contains(err.Error(), "duplicate key error") || strings.Contains(err.Error(), "E11000")) {
							isDup = true
						}

						if isDup {
							m.log.Debugf("[%s.%s] Fallback insert found existing document _id=%v, skipping overwrite to prevent stale reversion", sourceDB, sourceCollection, extractDocID(doc))
							duplicateKeys++
						} else {
							docID := extractDocID(doc)
							batchFailed++
							dlqCount++
							if opts.DLQ != nil {
								opts.DLQ.WriteFailed(sourceDB, sourceCollection, docID, err, "initial", "insert", originalBatch[idx], time.Time{})
							}
						}
					}
				}
			}
		}

		successCount := int64(len(batch)) - batchFailed
		if opts.BackfillStatsManager != nil {
			opts.BackfillStatsManager.RecordWriteResult(bfNS, successCount, batchFailed, duplicateKeys, dlqCount, workerID)
		}
		return successCount, batchFailed, nil
	} else {
		// Fail-Fast Mode (standard mode=migrate behavior)
		if opts.Throttler != nil {
			if err := opts.Throttler.Wait(ctx, len(batch)); err != nil {
				return 0, 0, err
			}
		}
		err := retryManager.RetryWithSplit(ctx, batch, sourceCollection, func(b []interface{}) error {
			bulkCtx, cancelBulk := context.WithTimeout(ctx, 90*time.Second)
			defer cancelBulk()
			return processBatch(bulkCtx, targetCol, b, opts.UpsertMode, sourceDB, sourceCollection, transformer)
		})
		writeDuration := time.Since(writeStart)
		if opts.Throttler != nil {
			opts.Throttler.ReportResult(writeDuration, isSystemError(err))
		}
		if err != nil {
			if opts.BackfillStatsManager != nil {
				opts.BackfillStatsManager.RecordBulkWrite(writeDuration)
				opts.BackfillStatsManager.RecordWriteResult(bfNS, 0, int64(len(batch)), 0, 0, workerID)
			}
			return 0, int64(len(batch)), err
		}
		if opts.BackfillStatsManager != nil {
			opts.BackfillStatsManager.RecordBulkWrite(writeDuration)
			opts.BackfillStatsManager.RecordWriteResult(bfNS, int64(len(batch)), 0, 0, 0, workerID)
		}
		return int64(len(batch)), 0, nil
	}
}

func (m *Migrator) executeWithRetry(ctx context.Context, retryManager *RetryManager, op func() error) error {
	err := op()
	if err != nil && retryManager != nil && err != context.Canceled && ctx.Err() != context.Canceled {
		errType := retryManager.ClassifyError(err)
		if errType == ErrorTypeConnection || errType == ErrorTypeContention {
			m.log.Infof("Transient error during fallback execution. Retrying with backoff...")
			err = retryManager.RetryWithBackoff(ctx, op)
		}
	}
	return err
}

// TargetCollection defines the minimal DB operations interface required for DLQ reprocessing,
// allowing it to be mocked in unit tests.
type TargetCollection interface {
	ReplaceOne(ctx context.Context, filter interface{}, replacement interface{}, opts ...*options.ReplaceOptions) (*mongo.UpdateResult, error)
	DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error)
}

// reprocessDLQ reads the DLQ file for a database pair, re-applies failures, and writes new failures to a new DLQ.
func (m *Migrator) reprocessDLQ(ctx context.Context, pair config.DatabasePair, pairIndex int) (runErr error) {
	statePath := m.getInitialMigrationStatePath(pairIndex)
	initialMigrationState, err := LoadInitialMigrationState(statePath)
	if err != nil {
		return fmt.Errorf("failed to load initial migration state: %w", err)
	}

	dlqPath := m.getDLQPath(pairIndex)
	tempPath := dlqPath + ".retry-temp"

	// Recover leftover temporary file from a crashed previous run if it exists.
	// Discards any active partial file and restores the original temp file.
	if err := m.restoreLeftoverTempDLQ(dlqPath, tempPath); err != nil {
		return fmt.Errorf("failed to restore leftover temp DLQ file: %w", err)
	}

	m.log.Infof("Starting DLQ reprocessing for pair %d (%s -> %s) using file: %s", pairIndex, pair.Source.Database, pair.Target.Database, dlqPath)

	// Check if the DLQ file exists
	if _, err := os.Stat(dlqPath); os.IsNotExist(err) {
		m.log.Infof("No DLQ file found at %s. Skipping retry.", dlqPath)
		return nil
	}

	// Rename the DLQ file so we can read it while opening a new clean DLQ file for new failures
	if err := os.Rename(dlqPath, tempPath); err != nil {
		return fmt.Errorf("failed to rename DLQ file for reprocessing: %w", err)
	}
	defer func() {
		if runErr == nil {
			_ = os.Remove(tempPath)
		} else {
			m.log.Warnf("DLQ retry failed: %v. Restoring original DLQ file to preserve integrity.", runErr)
			_ = os.Remove(dlqPath) // Discard incomplete active retry file
			if restoreErr := os.Rename(tempPath, dlqPath); restoreErr != nil {
				m.log.Errorf("DLQ recovery: Failed to restore leftover temp DLQ file: %v", restoreErr)
			}
		}
	}()

	targetDB, err := db.NewMongoDB(pair.Target.ConnectionString, pair.Target.Database, 1, 10, 0, nil, m.log)
	if err != nil {
		return fmt.Errorf("failed to connect to target MongoDB: %w", err)
	}
	defer func() {
		if err := targetDB.Close(ctx); err != nil {
			m.log.Errorf("Error closing target DB: %v", err)
		}
	}()

	// Open new DLQ writer for any failures that occur during retry
	newDLQ, err := NewDLQWriter(dlqPath, m.log)
	if err != nil {
		return fmt.Errorf("failed to create new DLQ writer: %w", err)
	}
	defer newDLQ.Close()

	getCollection := func(collectionName string) TargetCollection {
		return targetDB.GetCollection(collectionName)
	}

	// Optional source-resync: when enabled, each failed document is re-read fresh
	// from the SOURCE by _id (instead of replaying the stored DLQ snapshot), so a
	// user who fixed the offending data at the source gets the corrected copy
	// migrated. Source-deleted documents are treated as resolved. Snapshot replay
	// remains the default.
	var sourceFetch sourceDocFetcher
	if m.config != nil && m.config.RetryConfig.ResyncFromSource {
		var closeSource func()
		m.log.Infof("DLQ retry: source-resync enabled — re-reading failed documents from source %s", pair.Source.Database)
		sourceFetch, closeSource, err = m.newModernSourceFetcher(ctx, pair.Source.ConnectionString, pair.Source.Database)
		if err != nil {
			return fmt.Errorf("failed to connect to source MongoDB for DLQ source-resync: %w", err)
		}
		defer closeSource()
	}

	phase, failedCount, err := m.reprocessDLQLoop(ctx, tempPath, newDLQ, getCollection, sourceFetch)
	if err != nil {
		runErr = err
		return runErr
	}

	// Sync initial migration state if we successfully completed reprocessing for the initial phase
	// and the original status was StatusCompletedWithFailures.
	// If the state was StatusInProgress, we do not mark it as completed because it was interrupted mid-run.
	if phase == "initial" && initialMigrationState != nil && initialMigrationState.Status == StatusCompletedWithFailures {
		statePath := m.getInitialMigrationStatePath(pairIndex)
		if failedCount == 0 {
			m.log.Infof("DLQ retry: Initial migration completed successfully. Updating state file to StatusCompleted.")
			if stateErr := SaveInitialMigrationState(statePath, StatusCompleted, 0); stateErr != nil {
				m.log.Errorf("Failed to update initial migration state: %v", stateErr)
			}
		} else {
			m.log.Warnf("DLQ retry: Initial migration completed with %d remaining failures. Updating state file.", failedCount)
			if stateErr := SaveInitialMigrationState(statePath, StatusCompletedWithFailures, failedCount); stateErr != nil {
				m.log.Errorf("Failed to update initial migration state: %v", stateErr)
			}
		}
	}

	return nil
}

// sourceDocFetcher re-reads a single source document by _id during retry-dlq
// source-resync. It returns the document, whether it still exists, and any error.
type sourceDocFetcher func(collection string, id interface{}) (interface{}, bool, error)

// newModernSourceFetcher opens a modern-driver connection to the source and
// returns a sourceDocFetcher plus a close func, for retry-dlq source-resync
// against MongoDB ≥ 3.6 / change-stream sources.
func (m *Migrator) newModernSourceFetcher(ctx context.Context, connectionString, database string) (sourceDocFetcher, func(), error) {
	src, err := db.NewMongoDB(connectionString, database, 1, 10, 0, nil, m.log)
	if err != nil {
		return nil, nil, err
	}
	fetch := func(collection string, id interface{}) (interface{}, bool, error) {
		var doc bson.M
		ferr := src.GetCollection(collection).FindOne(ctx, bson.M{"_id": id}).Decode(&doc)
		if ferr == mongo.ErrNoDocuments {
			return nil, false, nil
		}
		if ferr != nil {
			return nil, false, ferr
		}
		return doc, true, nil
	}
	return fetch, func() { _ = src.Close(ctx) }, nil
}

// reprocessDLQLoop processes records inside the DLQ temp file, writing them back to target using the getCollection helper.
// It performs a chronological scan to de-duplicate updates and ignore resolved records, ensuring zero stale write overwrites.
// When sourceFetch is non-nil (source-resync), each non-delete failure is re-read
// fresh from the source by _id instead of replaying the stored snapshot.
func (m *Migrator) reprocessDLQLoop(ctx context.Context, tempPath string, newDLQ *DLQWriter, getCollection func(string) TargetCollection, sourceFetch sourceDocFetcher) (phase string, failedCount int64, runErr error) {
	file, err := os.Open(tempPath)
	if err != nil {
		return "", 0, fmt.Errorf("failed to open temp DLQ file: %w", err)
	}
	defer file.Close()

	// Read all lines into memory first to facilitate chronological scan and EOF analysis
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return "", 0, fmt.Errorf("error reading temp DLQ file: %w", err)
	}

	convertInvalidIds := true
	if m.config != nil {
		convertInvalidIds = m.config.RetryConfig.ConvertInvalidIds
	}
	transformer := NewFieldTransformer(
		m.config.DropEmptyFieldNames,
		m.config.ConvertLongFieldNamesInNestedDocs,
		convertInvalidIds,
		m.log,
	)

	type activeFailure struct {
		record  DLQRecord
		rawLine string
	}
	failuresMap := make(map[string]*activeFailure)
	var expectedPhase string

	// Pass 1: Chronological Scan & De-duplication
	for i, line := range lines {
		if i == 0 {
			var header struct {
				DLQVersion string `bson:"dlqVersion"`
			}
			if err := bson.UnmarshalExtJSON([]byte(line), false, &header); err == nil && header.DLQVersion != "" {
				if header.DLQVersion != DLQVersion {
					runErr = fmt.Errorf("DLQ retry: Safety violation: encountered unsupported DLQ version %q (expected %q). Please use the correct migration tool version to reprocess this file.", header.DLQVersion, DLQVersion)
					return "", 0, runErr
				}
				m.log.Infof("DLQ retry: DLQ file version %q matches tool DLQ version %q", header.DLQVersion, DLQVersion)
				continue // skip version header line
			}
		}

		var record DLQRecord
		if err := bson.UnmarshalExtJSON([]byte(line), false, &record); err != nil {
			// Gracefully skip corrupted lines at the very end of the file (survive partial write crashes)
			if i == len(lines)-1 {
				m.log.Warnf("DLQ retry: Skipping corrupted line at EOF (probable mid-write crash fragment): %v. Line: %s", err, line)
				continue
			}
			// Middle-file corruption represents a safety violation; abort replay immediately
			runErr = fmt.Errorf("DLQ retry: Failed to unmarshal record: %w. Line: %s", err, line)
			return "", 0, runErr
		}

		// Enforce single-phase validation
		if expectedPhase == "" {
			expectedPhase = record.Phase
			m.log.Infof("DLQ retry: Set expected phase to %q based on first record", expectedPhase)
		} else if record.Phase != expectedPhase {
			runErr = fmt.Errorf("DLQ retry: Safety violation: encountered mixed phases in DLQ file (expected %q, got %q). Reprocessing aborted.", expectedPhase, record.Phase)
			return "", 0, runErr
		}

		// Unique key per document identity
		if record.ResolvedID != nil {
			uniqueKey := MakeDLQKey(record.SourceDB, record.SourceCollection, record.ResolvedID)
			delete(failuresMap, uniqueKey)
		} else {
			uniqueKey := MakeDLQKey(record.SourceDB, record.SourceCollection, record.DocumentID)
			failuresMap[uniqueKey] = &activeFailure{
				record:  record,
				rawLine: line,
			}
		}
	}

	var processed, succeeded, failed, resolved int64

	// Pass 2: Execute Replays
	for _, f := range failuresMap {
		if ctx.Err() != nil {
			runErr = ctx.Err()
			return "", 0, runErr
		}

		processed++
		record := f.record

		// Original change-event time, preserved on any re-failure so DLQ ordering
		// stays chronologically correct across retries.
		var eventTime time.Time
		if record.EventTime != "" {
			if t, err := time.Parse(time.RFC3339, record.EventTime); err == nil {
				eventTime = t
			}
		}

		m.log.Debugf("Retrying document [db=%s, collection=%s, id=%v, op=%s]",
			record.SourceDB, record.SourceCollection, record.DocumentID, record.OpType)

		targetColl := getCollection(record.SourceCollection)
		var writeErr error

		if record.OpType == "delete" {
			// A delete replays the same way regardless of source-resync: the source
			// already dropped this document, so we remove it from the target.
			_, writeErr = targetColl.DeleteOne(ctx, bson.M{"_id": record.DocumentID})
		} else { // insert, update, replace, mixed
			// Default: replay the stored snapshot. With source-resync, re-read the
			// current document from the source so a source-side fix is picked up.
			docSnapshot := record.Document
			if sourceFetch != nil {
				fresh, found, ferr := sourceFetch(record.SourceCollection, record.DocumentID)
				if ferr != nil {
					m.log.Errorf("DLQ retry (source-resync): failed to re-read document %v from source %s.%s: %v",
						record.DocumentID, record.SourceDB, record.SourceCollection, ferr)
					failed++
					newDLQ.WriteFailed(record.SourceDB, record.SourceCollection, record.DocumentID,
						fmt.Errorf("source-resync read failed: %w", ferr), record.Phase, record.OpType, record.Document, eventTime)
					continue
				}
				if !found {
					// The source no longer has this document — the user resolved the
					// failure by deleting the offending source doc. Treat as resolved
					// and skip; leave the target untouched.
					m.log.Infof("DLQ retry (source-resync): document %v not found in source %s.%s (deleted) — treating as resolved and skipping.",
						record.DocumentID, record.SourceDB, record.SourceCollection)
					resolved++
					continue
				}
				docSnapshot = fresh
			}

			var docToReplace interface{} = docSnapshot
			if docSnapshot != nil {
				transformed, err := transformer.Transform(docSnapshot, record.SourceDB, record.SourceCollection, record.DocumentID)
				if err != nil {
					m.log.Errorf("DLQ retry: Failed to transform document %v: %v", record.DocumentID, err)
					failed++
					newDLQ.WriteFailed(record.SourceDB, record.SourceCollection, record.DocumentID, err, record.Phase, record.OpType, docSnapshot, eventTime)
					continue
				}
				docToReplace = transformed
			}
			opts := options.Replace().SetUpsert(true)
			_, writeErr = targetColl.ReplaceOne(ctx, bson.M{"_id": record.DocumentID}, docToReplace, opts)
		}

		if writeErr != nil {
			m.log.Errorf("DLQ retry: Failed to write document %v to target: %v", record.DocumentID, writeErr)
			failed++
			newDLQ.WriteFailed(record.SourceDB, record.SourceCollection, record.DocumentID, writeErr, record.Phase, record.OpType, record.Document, eventTime)
		} else {
			m.log.Debugf("DLQ retry: Successfully recovered document %v", record.DocumentID)
			succeeded++
		}
	}

	m.log.Infof("DLQ reprocessing complete: processed %d, succeeded %d, resolved(source-deleted) %d, failed %d", processed, succeeded, resolved, failed)
	return expectedPhase, failed, nil
}

// restoreLeftoverTempDLQ checks if a temporary DLQ retry file exists from a crashed run.
// If both active and temp files exist, it renames the temp file back to the active file.
// If only the temp file exists, it renames it to the active file.
func (m *Migrator) restoreLeftoverTempDLQ(dlqPath, tempPath string) error {
	// If temp file does not exist, nothing to restore
	if _, err := os.Stat(tempPath); os.IsNotExist(err) {
		return nil
	}

	// If active file does not exist, we can safely rename the temp file to active file
	if _, err := os.Stat(dlqPath); os.IsNotExist(err) {
		m.log.Warnf("DLQ recovery: Leftover retry-temp file found from previous crashed run. Restoring to %s.", dlqPath)
		return os.Rename(tempPath, dlqPath)
	}

	// Both files exist! Since we are doing "discard and restore" crash safety:
	// We discard the incomplete active file (from the crashed run) and restore the temp file as active.
	m.log.Warnf("DLQ recovery: Leftover retry-temp file and incomplete active file found. Discarding active file and restoring %s to %s.", tempPath, dlqPath)
	_ = os.Remove(dlqPath)
	return os.Rename(tempPath, dlqPath)
}

// isSystemError checks if the write error is a general database/network error rather than a benign duplicate key error.
func isSystemError(err error) bool {
	if err == nil || err == context.Canceled {
		return false
	}
	var bulkWriteException mongo.BulkWriteException
	if errors.As(err, &bulkWriteException) {
		for _, writeErr := range bulkWriteException.WriteErrors {
			if writeErr.Code != 11000 { // 11000 is duplicate key
				return true
			}
		}
		return false
	}
	return true
}
