package migration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"firestore-dr-migration/pkg/config"
	"firestore-dr-migration/pkg/db"
	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// ClientLevelReplicator handles replication using a client-level change stream
type ClientLevelReplicator struct {
	sourceDB                *db.MongoDB
	targetDB                *db.MongoDB
	config                  *config.Config
	log                     *logger.Logger
	collectionMap           map[string]map[string]string                  // Map of database -> source collection -> target collection
	collectionConfigs       map[string]map[string]config.CollectionConfig // Map of database -> source collection -> full config
	mu                      sync.Mutex                                    // Mutex for thread-safe operations
	dlq                     DLQ                                           // Dead Letter Queue for failed documents
	incrementalStatsManager *IncrementalStatsManager                      // Statistics manager
	backfillStatsManager    *BackfillStatsManager                         // Backfill statistics manager
	DryRun                  bool                                          // Dry run flag
}

// NewClientLevelReplicator creates a new client-level replicator
func NewClientLevelReplicator(sourceDB, targetDB *db.MongoDB, cfg *config.Config, log *logger.Logger) *ClientLevelReplicator {
	return &ClientLevelReplicator{
		sourceDB:          sourceDB,
		targetDB:          targetDB,
		config:            cfg,
		log:               log,
		collectionMap:     make(map[string]map[string]string),
		collectionConfigs: make(map[string]map[string]config.CollectionConfig),
	}
}

// SetIncrementalStatsManager sets the stats manager for this replicator
func (r *ClientLevelReplicator) SetIncrementalStatsManager(sm *IncrementalStatsManager) {
	r.incrementalStatsManager = sm
}

// SetBackfillStatsManager sets the backfill stats manager for this replicator
func (r *ClientLevelReplicator) SetBackfillStatsManager(sm *BackfillStatsManager) {
	r.backfillStatsManager = sm
}

// SetDLQ sets the Dead Letter Queue writer for this replicator
func (r *ClientLevelReplicator) SetDLQ(dlq DLQ) {
	r.dlq = dlq
}

// AddCollection adds a collection to be watched
func (r *ClientLevelReplicator) AddCollection(sourceDB, targetDB string, collConfig config.CollectionConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Initialize maps if needed
	if r.collectionMap[sourceDB] == nil {
		r.collectionMap[sourceDB] = make(map[string]string)
	}
	if r.collectionConfigs == nil {
		r.collectionConfigs = make(map[string]map[string]config.CollectionConfig)
	}
	if r.collectionConfigs[sourceDB] == nil {
		r.collectionConfigs[sourceDB] = make(map[string]config.CollectionConfig)
	}

	// Add collection mapping
	r.collectionMap[sourceDB][collConfig.SourceCollection] = collConfig.TargetCollection
	r.collectionConfigs[sourceDB][collConfig.SourceCollection] = collConfig

	r.log.Infof("Added collection mapping: %s.%s -> %s.%s (UpsertMode: %t)",
		sourceDB, collConfig.SourceCollection, targetDB, collConfig.TargetCollection, collConfig.UpsertMode)
}

// StartReplication starts the client-level replication
func (r *ClientLevelReplicator) StartReplication(ctx context.Context, globalResumeToken interface{}, globalResumeTokenPath string, initialMigrationState *InitialMigrationState, initialMigrationStatePath string, pair config.DatabasePair, liveOnly bool, liveStartTime *primitive.Timestamp, migrator *Migrator) error {
	if r.log == nil {
		r.log = logger.New()
	}
	partitions := 1
	if r.config != nil {
		partitions = r.config.IncrementalStreamPartitions
	}

	// Scan for any files matching: resumeToken-<db>-<coll>-partition-*-of-*.json
	dir := filepath.Dir(globalResumeTokenPath)
	files, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read checkpoint directory %s: %w", dir, err)
	}

	diskCheckpoints := ScanPartitionCheckpoints(files, globalResumeTokenPath)
	historicalTotal, existingPaths, usingCurrentPartitionFormat := ResolveActiveCheckpoints(diskCheckpoints)

	if historicalTotal > 0 {
		if historicalTotal != partitions || usingCurrentPartitionFormat {
			// --- PARTITION TRANSITION OR FORMAT UPGRADE PATH ---
			r.log.Infof("[Startup] Partition scaling/upgrade transition detected: historical partitions = %d, configured partitions = %d. Safe watermark resolution active.", historicalTotal, partitions)

			var minToken interface{}
			var minTime time.Time
			var minFile string

			// Assert that all historical checkpoints are present to prevent silent data loss
			for i := 0; i < historicalTotal; i++ {
				oldPath, exists := existingPaths[i]
				if !exists {
					// CRITICAL SAFETY EXCEPTION: Abort immediately if any historical checkpoint is missing
					return fmt.Errorf("safety violation: historical partition checkpoint file for partition %d of %d is missing on disk. Recovery aborted to prevent silent data loss. Please restore the file or start fresh", i+1, historicalTotal)
				}

				r.log.Infof("[Startup] [Watermark Assessment] Loading historical checkpoint %d/%d: %s", i+1, historicalTotal, filepath.Base(oldPath))
				token, err := LoadResumeToken(oldPath)
				if err != nil || token == nil {
					return fmt.Errorf("failed to load historical partition checkpoint %s: %w", oldPath, err)
				}

				// Load JSON timestamp metadata
				data, err := os.ReadFile(oldPath)
				if err == nil {
					var rt ResumeToken
					if err := json.Unmarshal(data, &rt); err == nil {
						eventTime := rt.Timestamp
						if eventTime.IsZero() {
							eventTime = time.Unix(0, 0) // Fallback for untimestamped tokens
						}
						if minTime.IsZero() || eventTime.Before(minTime) {
							minTime = eventTime
							minToken = token
							minFile = oldPath
						}
					}
				}
			}

			if minToken == nil {
				return fmt.Errorf("safety violation: cannot transition partition count because no valid event timestamps were found in checkpoint files")
			}

			r.log.Infof("[Startup] [Watermark Resolution] Safe unified minimum watermark resolved from %s (timestamp: %s).", filepath.Base(minFile), minTime.UTC().Format(time.RFC3339))
			r.log.Infof("[Startup] [Watermark Resolution] Initializing all %d new partition checkpoints with resolved watermark.", partitions)

			// 1. Initialize all new partition checkpoints
			for i := 0; i < partitions; i++ {
				newPath := GetPartitionResumeTokenPath(globalResumeTokenPath, i, partitions)
				if err := SaveResumeToken(newPath, minToken, minTime); err != nil {
					return fmt.Errorf("[Partition %d] failed to save converted partition checkpoint: %w", i+1, err)
				}
				r.log.Infof("[Startup] [Watermark Resolution] Saved new partition checkpoint: %s", filepath.Base(newPath))
			}

			// 2. Clean up the historical partition checkpoints
			r.log.Info("[Startup] [Watermark Resolution] Cleaning up stale historical partition checkpoints from disk.")
			for _, oldPath := range existingPaths {
				if err := DeleteResumeToken(oldPath); err != nil {
					r.log.Warnf("Failed to clean up stale partition checkpoint %s: %v", oldPath, err)
				} else {
					r.log.Infof("[Startup] [Watermark Resolution] Deleted stale checkpoint: %s", filepath.Base(oldPath))
				}
			}

			globalResumeToken = minToken
		} else {
			// --- NORMAL STARTUP / RESUME PATH ---
			r.log.Infof("[Startup] Normal resume path active. Verifying all %d partition checkpoints are healthy on disk.", partitions)

			// Ensure that all expected partition files exist and are valid
			for i := 0; i < partitions; i++ {
				oldPath, exists := existingPaths[i]
				if !exists {
					expectedPath := GetPartitionResumeTokenPath(globalResumeTokenPath, i, partitions)
					// CRITICAL SAFETY EXCEPTION: Missing checkpoint detected on normal resume
					return fmt.Errorf("safety violation: expected partition checkpoint file %s (partition %d of %d) is missing on disk. Fallback aborted to prevent data loss", filepath.Base(expectedPath), i+1, partitions)
				}

				token, err := LoadResumeToken(oldPath)
				if err != nil || token == nil {
					return fmt.Errorf("fatal: partition checkpoint file %s exists but is empty or unreadable: %v", oldPath, err)
				}
				r.log.Infof("[Startup] Verified healthy partition checkpoint %d/%d: %s", i+1, partitions, filepath.Base(oldPath))
			}

			// Populate globalResumeToken from the oldest partition file to satisfy safety invariants
			var minToken interface{}
			var minTime time.Time
			for i := 0; i < partitions; i++ {
				path := existingPaths[i]
				token, _ := LoadResumeToken(path)
				if token != nil {
					data, err := os.ReadFile(path)
					if err == nil {
						var rt ResumeToken
						if err := json.Unmarshal(data, &rt); err == nil {
							eventTime := rt.Timestamp
							if eventTime.IsZero() {
								eventTime = time.Unix(0, 0)
							}
							if minTime.IsZero() || eventTime.Before(minTime) {
								minTime = eventTime
								minToken = token
							}
						}
					}
				}
			}
			if minToken != nil {
				globalResumeToken = minToken
			}
		}
	} else {
		// --- CASE C: LEGACY NAMING (NON-PARTITIONED) UPGRADE ---
		if globalResumeToken != nil {
			r.log.Infof("[Startup] Legacy single change stream checkpoint detected: %s. Upgrading to partitioned change stream (%d partitions configured).", filepath.Base(globalResumeTokenPath), partitions)
			for i := 0; i < partitions; i++ {
				partitionPath := GetPartitionResumeTokenPath(globalResumeTokenPath, i, partitions)
				if err := SaveResumeToken(partitionPath, globalResumeToken); err != nil {
					return fmt.Errorf("[Partition %d] failed to initialize partition checkpoint: %w", i+1, err)
				}
				r.log.Infof("[Startup] Initialized partition checkpoint %d/%d: %s", i+1, partitions, filepath.Base(partitionPath))
			}
		}
	}

	// Abort if the initial migration state was completed with failures, or if DLQ has entries
	if initialMigrationState != nil && initialMigrationState.Status == StatusCompletedWithFailures {
		return fmt.Errorf("cannot start replication: initial migration completed with failures in a previous run")
	}
	if r.dlq != nil {
		if _, isNop := r.dlq.(*NopDLQWriter); !isNop {
			if r.dlq.Count() > 0 {
				return fmt.Errorf("cannot start replication: DLQ contains failed documents")
			}
		}
	}

	// Enforce safety invariants between Initial Migration State and Resume Token Checkpoint
	if liveStartTime != nil && globalResumeToken != nil {
		return fmt.Errorf("safety violation: a custom live-start-timestamp is specified, but a global resume token checkpoint already exists. Clean up checkpoint file or omit live-start-timestamp to resume from the last checkpoint")
	}

	if liveStartTime == nil {
		if initialMigrationState == nil {
			if globalResumeToken != nil {
				return fmt.Errorf("safety violation: initial migration state file does not exist, but a global resume token checkpoint exists. Clean up checkpoint file or ensure state is in sync before proceeding")
			}
		} else if initialMigrationState.Status == StatusCompleted || initialMigrationState.Status == StatusSkipped {
			if globalResumeToken == nil {
				return fmt.Errorf("safety violation: initial migration state is marked as %s, but no global resume token checkpoint was found. Clean up state file or restore checkpoint before proceeding", initialMigrationState.Status)
			}
		}
	}

	var needsInitialMigration bool

	// We need to run initial migration if no state file exists OR if it is not marked completed
	if initialMigrationState == nil || !initialMigrationState.IsCompleted() {
		if liveOnly {
			r.log.Info("Live-only mode enabled. Skipping initial migration phase.")
			// Critical File-System State Checkpoint:
			// If we cannot persist the StatusSkipped state to disk, we exit with a terminal error.
			// Continuing silently would cause subsequent startup runs to attempt the backfill again,
			// leading to massive duplicate processing or index-recreation errors.
			if err := SaveInitialMigrationState(initialMigrationStatePath, StatusSkipped, 0); err != nil {
				return fmt.Errorf("failed to save initial migration state as skipped: %w", err)
			}
			needsInitialMigration = false
			initialMigrationState = &InitialMigrationState{
				Status: StatusSkipped,
			}
		} else {
			needsInitialMigration = true
		}
	}

	// If no resume token is available, and no custom liveStartTime is specified, we need to capture the
	// current cursor state of the database so that we have a valid checkpoint to resume replication from.
	// Note: If liveStartTime is provided, we don't capture a startup resume token, as the client-level
	// change stream will be configured to start replication directly from the specified liveStartTime.
	if globalResumeToken == nil && liveStartTime == nil {
		if liveOnly {
			r.log.Info("No global resume token found in live-only mode. Obtaining current resume token to start incremental replication.")
		} else {
			r.log.Info("No global resume token found. Creating a new one and will perform initial migration.")
		}

		tokenDoc, err := CaptureAndSaveInitialResumeToken(ctx, r.sourceDB, r.config.IncrementalStreamPartitions, globalResumeTokenPath, r.log)
		if err != nil {
			return err
		}
		globalResumeToken = tokenDoc
	} else if liveStartTime != nil {
		r.log.Infof("No resume token available. Starting replication from liveStartTime: %s", time.Unix(int64(liveStartTime.T), 0).UTC().Format(time.RFC3339))
	} else {
		r.log.Info("Global resume token available. Starting incremental replication.")
	}

	// Perform initial migration if needed
	if needsInitialMigration {
		if !r.DryRun {
			if err := SaveInitialMigrationState(initialMigrationStatePath, StatusInProgress, 0); err != nil {
				return fmt.Errorf("failed to save initial migration state as incomplete: %w", err)
			}
		}
		initialMigrationStart := time.Now()
		r.log.Info("Performing initial migration for all collections")

		// IndexOnly mode: build indexes up front and return without migrating data.
		// This is the ONLY case where indexes are built before a data load, because
		// creating indexes IS the whole job here.
		if pair.Target.IndexOnly {
			r.log.Info("IndexOnly mode enabled. Syncing indexes, then skipping data migration.")
			if migrator.config.IndexConcurrency > 0 {
				r.targetDB.SetIndexConcurrency(migrator.config.IndexConcurrency)
			}
			var collections []config.CollectionConfig
			for _, colls := range r.collectionConfigs {
				for _, collConfig := range colls {
					collections = append(collections, collConfig)
				}
			}
			if err := migrator.syncIndexes(ctx, r.sourceDB, r.targetDB, pair, collections); err != nil {
				r.log.Warnf("Index sync encountered issues: %v", err)
			}
			r.log.Info("IndexOnly mode: waiting for all async index creation to complete...")
			r.targetDB.WaitForIndexCreation()
			migrator.logFailedIndexes(r.targetDB)
			if err := SaveInitialMigrationState(initialMigrationStatePath, StatusCompleted, 0); err != nil {
				r.log.Errorf("Error saving initial migration state as complete: %v", err)
			}
			return nil
		}

		// NOTE: indexes are intentionally NOT built here. Building them before the
		// backfill makes Firestore re-index on every inserted document, stalling the
		// whole load behind a multi-minute build (and leaving the console blank the
		// entire time). They are deferred until after the backfill loads and
		// replication lag has settled — see the DeferredIndexController launched
		// before change-stream processing below. This matches the legacy oplog path.

		throttlerCtx, throttlerCancel := context.WithCancel(ctx)
		defer throttlerCancel()

		// Initialize the throttler for backfill traffic writes
		burstSize := 2 * r.config.InitialWriteBatchSize
		throttler := NewWriteThrottler(r.config.BackfillRampUp, burstSize)
		if throttler != nil {
			throttler.StartRampUp(throttlerCtx)
			if r.backfillStatsManager != nil {
				r.backfillStatsManager.SetThrottler(throttler)
			}
		}

		// Use a semaphore to limit the number of concurrent collection migrations
		concurrentCollections := r.config.ConcurrentCollections
		if concurrentCollections <= 0 {
			concurrentCollections = 4
		}
		r.log.Infof("Processing up to %d collections concurrently", concurrentCollections)
		// Resizable so the console can raise/lower collection concurrency on a
		// running job (Migrator.Reconfig) without a restart. Registered on the
		// migrator so /api/reconfig can reach it; shrinking never preempts
		// in-flight collections (see resizableSem).
		sem := newResizableSem(concurrentCollections)
		if migrator != nil {
			migrator.setCollSem(sem)
			defer migrator.clearCollSem(sem)
		}
		var wg sync.WaitGroup

		// Track overall statistics
		var totalMigratedCount int64
		var totalFailedCount int64
		var completedCollections int64
		var mu sync.Mutex // Mutex for thread-safe updates to statistics

		// Track critical errors
		var criticalErr error
		var errOnce sync.Once

		totalCollections := 0
		for _, colls := range r.collectionConfigs {
			totalCollections += len(colls)
		}

		// Iterate through all collections in the map
	dispatch:
		for sourceDB, collections := range r.collectionConfigs {
			for sourceCollection, collConfig := range collections {
				// Acquire a slot; unlike a fixed channel this respects ctx and a
				// live limit change. On cancellation stop dispatching further work.
				if err := sem.Acquire(ctx); err != nil {
					break dispatch
				}
				wg.Add(1)

				go func(sourceDB, sourceCollection string, collConfig config.CollectionConfig) {
					defer wg.Done()
					defer sem.Release()

					r.log.Infof("Starting initial migration for %s.%s to %s", sourceDB, sourceCollection, collConfig.TargetCollection)

					opts := MigrateOptions{
						DLQ:                  r.dlq,
						StatsManager:         r.incrementalStatsManager,
						BackfillStatsManager: r.backfillStatsManager,
						UpsertMode:           true, // resilient mode always performs upserts on duplicates
						Throttler:            throttler,
					}

					succeeded, failed, err := migrator.migrateCollection(ctx, r.sourceDB, r.targetDB, collConfig, opts)
					if err != nil {
						errOnce.Do(func() {
							criticalErr = fmt.Errorf("critical error during migration of collection %s.%s: %w", sourceDB, sourceCollection, err)
						})
					}

					// Update overall statistics and log overall progress
					mu.Lock()
					totalMigratedCount += succeeded + failed
					totalFailedCount += failed
					completedCollections++
					r.log.Infof("Overall progress: %d/%d collections completed", completedCollections, totalCollections)
					mu.Unlock()
				}(sourceDB, sourceCollection, collConfig)
			}
		}

		// Wait for all collection migrations to complete
		wg.Wait()
		if r.backfillStatsManager != nil {
			r.backfillStatsManager.ReportStats(true)
			r.backfillStatsManager.Stop()
		}

		if criticalErr != nil {
			return criticalErr
		}

		initialMigrationDuration := time.Since(initialMigrationStart)
		var failurePercentage float64
		if totalMigratedCount > 0 {
			failurePercentage = (float64(totalFailedCount) * 100.0) / float64(totalMigratedCount)
		}
		r.log.Infof("Initial migration completed in %.2f seconds. Total collections: %d, Total documents: %d (Success: %d, Failed: %d, Failure Rate: %.2f%%)",
			initialMigrationDuration.Seconds(), totalCollections, totalMigratedCount, totalMigratedCount-totalFailedCount, totalFailedCount, failurePercentage)

		// Issue warning if the sum of all failed collections does not match actual DLQ write count
		if r.dlq != nil {
			if _, isNop := r.dlq.(*NopDLQWriter); !isNop {
				dlqCount := r.dlq.Count()
				if totalFailedCount != dlqCount {
					r.log.Warnf("DLQ Metric mismatch: sum of failed counts across collections (%d) does not match DLQ written count (%d)",
						totalFailedCount, dlqCount)
				}
			}
		}

		// A cancelled context means the run was INTERRUPTED, not finished. The
		// un-migrated remainder is counted as "failed" but not DLQ'd (that's the
		// mismatch warned about above), so marking completed_with_failures would
		// ban the database on the next run with an empty DLQ and no recovery
		// path. Leave the state in-progress for a clean re-run.
		if ctx.Err() != nil {
			r.log.Warnf("Initial migration for %s interrupted (context cancelled); ~%d documents remain un-migrated. Leaving state in-progress for a clean re-run.",
				pair.Source.Database, totalFailedCount)
			return ctx.Err()
		}

		// Determine the terminal status. completed_with_failures is NON-BLOCKING:
		// failed documents are captured in the DLQ with their error reason and
		// recovered later via retry-dlq; every other document migrated normally
		// and replication proceeds. (Shared across all replication paths.)
		status, _ := resolveInitialMigrationOutcome(r.dlq, totalFailedCount, r.log)

		if !r.DryRun {
			if err := SaveInitialMigrationState(initialMigrationStatePath, status, totalFailedCount); err != nil {
				return fmt.Errorf("failed to save initial migration state as complete: %w", err)
			}
		}

		r.log.Info("Starting incremental replication.")
	} else {
		r.log.Info("Initial migration already marked as completed. Skipping.")
	}

	// Index-Only mode: sync indexes (if not already done during initial migration) and exit
	if pair.Target.IndexOnly {
		if !needsInitialMigration {
			// Resume token exists, so initial migration was skipped — sync indexes
			// now. Always runs (even with SyncAllIndexes off) so the _id index gets
			// built; secondary indexes remain gated inside syncIndexes.
			r.log.Info("IndexOnly mode: resume token exists, performing index sync directly")
			// Build collections list from collectionConfigs

			// Configure index build concurrency before launching any async builds
			if migrator.config.IndexConcurrency > 0 {
				r.targetDB.SetIndexConcurrency(migrator.config.IndexConcurrency)
			}

			var collections []config.CollectionConfig
			for _, colls := range r.collectionConfigs {
				for _, collConfig := range colls {
					collections = append(collections, collConfig)
				}
			}
			if err := migrator.syncIndexes(ctx, r.sourceDB, r.targetDB, pair, collections); err != nil {
				r.log.Warnf("Index sync encountered issues: %v", err)
			}
			r.log.Info("IndexOnly mode: waiting for all async index creation to complete...")
			r.targetDB.WaitForIndexCreation()
			migrator.logFailedIndexes(r.targetDB)
		}
		r.log.Info("IndexOnly mode: skipping change stream. Index replication complete.")
		return nil
	}

	// Load partition-level resume tokens from their independent files.
	// Suffix path names are generated using partition indices (e.g. resumeToken-pair0-0.json, resumeToken-pair0-1.json).
	var partitionTokens []interface{}
	for i := 0; i < r.config.IncrementalStreamPartitions; i++ {
		partitionPath := GetPartitionResumeTokenPath(globalResumeTokenPath, i, r.config.IncrementalStreamPartitions)
		token, err := LoadResumeToken(partitionPath)
		if err != nil || token == nil {
			// If a token is missing/nil here (despite our check), it's a fatal failure in partitioned mode.
			if r.config.IncrementalStreamPartitions > 1 {
				return fmt.Errorf("fatal: partition checkpoint file %s exists but is empty or unreadable: %v", partitionPath, err)
			}
			// Fallback is only allowed in legacy single-stream mode
			r.log.Warnf("[Partition %d] No valid partition checkpoint found at %s (falling back to global checkpoint: %v, token: %v)", i, partitionPath, err, token)
			token = globalResumeToken
		}
		partitionTokens = append(partitionTokens, token)
	}

	var changeStreams []*mongo.ChangeStream
	var openError error

	// Open the partitioned change streams in parallel. Each change stream aggregates with a
	// server-side BSON aggregation pipeline stage filtering for its corresponding deterministic partition index.
	r.log.Infof("Starting %d client-level change streams for all databases and collections", r.config.IncrementalStreamPartitions)
	for i := 0; i < r.config.IncrementalStreamPartitions; i++ {
		var token interface{}
		if len(partitionTokens) > i {
			token = partitionTokens[i]
		}

		// Build a zero-JS, loopless FNV-inspired BSON aggregation pipeline stage with server-side database & collection filtering
		pipeline := BuildPartitionPipeline(i, r.config.IncrementalStreamPartitions, pair.Source.Database, pair.Target.Collections)
		r.log.Infof("[Partition %d/%d] Creating client-level change stream (ResumeToken: %v)", i+1, r.config.IncrementalStreamPartitions, token != nil)

		stream, err := r.sourceDB.CreateClientLevelChangeStream(
			ctx,
			token,
			liveStartTime,
			r.config.IncrementalReadBatchSize,
			pipeline,
		)
		if err != nil {
			openError = err
			break
		}
		changeStreams = append(changeStreams, stream)
	}

	if openError != nil {
		// Close any successfully opened change streams first before fallback
		for _, stream := range changeStreams {
			if stream != nil {
				stream.Close(ctx)
			}
		}

		// Check if the error is due to the resume token being too old
		if strings.Contains(openError.Error(), "ChangeStreamHistoryLost") ||
			strings.Contains(openError.Error(), "Resume of change stream was not possible") {
			r.log.Warn("Resume token is too old and no longer in the oplog. Deleting resume token files and starting fresh.")

			// Delete all partition and global resume token files
			for p := 0; p < r.config.IncrementalStreamPartitions; p++ {
				path := GetPartitionResumeTokenPath(globalResumeTokenPath, p, r.config.IncrementalStreamPartitions)
				if err := DeleteResumeToken(path); err != nil {
					r.log.Errorf("Error deleting partition resume token file %s: %v", path, err)
				}
			}
			if err := DeleteResumeToken(globalResumeTokenPath); err != nil {
				r.log.Errorf("Error deleting global resume token file: %v", err)
			}

			// Delete the initial migration state file
			if err := DeleteInitialMigrationState(initialMigrationStatePath); err != nil {
				r.log.Errorf("Error deleting initial migration state file: %v", err)
			}

			// Perform initial migration again
			r.log.Info("Starting fresh with initial migration...")
			return r.StartReplication(ctx, nil, globalResumeTokenPath, nil, initialMigrationStatePath, pair, liveOnly, nil, migrator)
		}

		return fmt.Errorf("failed to create client-level change stream partition: %w", openError)
	}

	defer func() {
		for _, stream := range changeStreams {
			if stream != nil {
				stream.Close(ctx)
			}
		}
	}()

	// Create event distributor for parallel processing
	r.log.Infof("Starting parallel change stream processing with %d workers", r.config.IncrementalWorkerCount)
	distributor := NewEventDistributor(
		ctx,
		r.sourceDB,
		r.targetDB,
		r.collectionConfigs,
		changeStreams,
		r.log,
		globalResumeTokenPath,
		time.Duration(r.config.CheckpointIntervalMinutes)*time.Minute,
		r.config.SaveThreshold,
		r.config.IncrementalWorkerCount,
		r.config.IncrementalWriteBatchSize,
		r.config.ForceOrderedOperations,
		time.Duration(r.config.FlushIntervalMs)*time.Millisecond,
		r.config,
		r.dlq,
		r.incrementalStatsManager,
	)
	distributor.DryRun = r.DryRun

	// Deferred index build: the backfill is done and the change stream is open, so
	// build indexes once replication lag has settled — never up front, so Firestore
	// does not re-index on every backfilled/streamed write. This is the same rule
	// the legacy path applies; both share DeferredIndexController so they cannot
	// drift apart. Idempotent on resume (indexes already exist -> no-op build).
	deferredIdx := NewDeferredIndexController(migrator, r.targetDB, r.config, r.log, func(ctx context.Context) {
		var collections []config.CollectionConfig
		for _, colls := range r.collectionConfigs {
			for _, collConfig := range colls {
				collections = append(collections, collConfig)
			}
		}
		if err := migrator.syncIndexes(ctx, r.sourceDB, r.targetDB, pair, collections); err != nil {
			r.log.Warnf("Deferred index sync encountered issues: %v", err)
		}
	})
	go deferredIdx.PollProgress(ctx)
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				lag := -1.0
				if r.incrementalStatsManager != nil {
					lag = r.incrementalStatsManager.RecentLagSeconds()
				}
				deferredIdx.Observe(ctx, lag)
			}
		}
	}()

	// Start event distribution
	err = distributor.Start()
	// Don't propagate context.Canceled as an error
	if err == context.Canceled {
		r.log.Info("Replication stopped due to context cancellation")
		return nil
	}
	return err
}

// CaptureAndSaveInitialResumeToken opens a temporary client-level change stream on sourceDB,
// extracts the initial resume token from the cursor, unmarshals it to bson.M, and saves it
// to all partition checkpoint files corresponding to globalResumeTokenPath.
// If partitions == 1, it also writes the legacy globalResumeTokenPath.
func CaptureAndSaveInitialResumeToken(ctx context.Context, sourceDB *db.MongoDB, partitions int, globalResumeTokenPath string, log *logger.Logger) (bson.M, error) {
	if partitions <= 0 {
		partitions = 1
	}

	log.Info("Creating client-level change stream to capture initial resume token...")
	initialChangeStream, err := sourceDB.CreateClientLevelChangeStream(ctx, nil, nil, 0, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create initial client-level change stream: %w", err)
	}
	defer initialChangeStream.Close(ctx)

	initialResumeToken := initialChangeStream.ResumeToken()
	if len(initialResumeToken) == 0 {
		return nil, fmt.Errorf("captured empty resume token from change stream")
	}
	log.Infof("Obtained initial resume token: %v", initialResumeToken)

	var initialResumeTokenDoc bson.M
	if err := bson.Unmarshal(initialResumeToken, &initialResumeTokenDoc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal initial resume token BSON: %w", err)
	}
	log.Infof("Converted initial resume token: %v", initialResumeTokenDoc)

	for i := 0; i < partitions; i++ {
		partitionPath := GetPartitionResumeTokenPath(globalResumeTokenPath, i, partitions)
		if err := SaveResumeToken(partitionPath, initialResumeTokenDoc); err != nil {
			return nil, fmt.Errorf("[Partition %d] failed to save initial partition resume token to %s: %w", i+1, partitionPath, err)
		}
		log.Infof("[Partition %d/%d] Saved initial partition resume token to: %s", i+1, partitions, partitionPath)
	}

	if partitions == 1 {
		if err := SaveResumeToken(globalResumeTokenPath, initialResumeTokenDoc); err != nil {
			return nil, fmt.Errorf("failed to save global resume token checkpoint to %s: %w", globalResumeTokenPath, err)
		}
		log.Infof("Saved global resume token checkpoint to: %s", globalResumeTokenPath)
	}

	return initialResumeTokenDoc, nil
}
