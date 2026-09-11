package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
)

// Config represents the main configuration structure
type Config struct {
	DatabasePairs             []DatabasePair `json:"databasePairs"`
	SaveThreshold             int            `json:"saveThreshold"`             // Number of processed events before saving a resume token checkpoint
	CheckpointIntervalMinutes int            `json:"checkpointIntervalMinutes"` // Checkpoint interval in minutes
	ForceOrderedOperations    bool           `json:"forceOrderedOperations"`    // Force ordered operations for all types
	FlushIntervalMs           int            `json:"flushIntervalMs"`           // Flush interval in milliseconds
	TargetMaxConnIdleSeconds  int            `json:"targetMaxConnIdleSeconds"`  // Maximum connection idle time for target in seconds
	TargetMinPoolSize         int            `json:"targetMinPoolSize"`         // Minimum connection pool size for target
	TargetMaxPoolSize         int            `json:"targetMaxPoolSize"`         // Maximum connection pool size for target
	IndexConcurrency          int            `json:"indexConcurrency"`          // Max concurrent async index builds (default 1 for Firestore)

	// Deferred index-build timing (legacy live mode). Secondary indexes are built
	// AFTER the initial full load. In live mode the build is further deferred until
	// replication lag has settled, so index creation does not contend with the
	// catch-up write burst on Firestore.
	IndexBuildLagThresholdSeconds int `json:"indexBuildLagThresholdSeconds"` // Max lag (s) considered "caught up" enough to start building indexes (default 5)
	IndexBuildLagStableChecks     int `json:"indexBuildLagStableChecks"`     // Consecutive low-lag report cycles required before triggering the build (default 3)

	// Cutover readiness (console). A live collection is "ready to cut over" when
	// its lag is at/under this threshold (or it is idle/caught up) with zero
	// failed writes and zero DLQ entries, held stable for this many consecutive
	// report cycles. Surfaced to the console so the operator can safely stop the
	// source before verifying. These are display-only thresholds; the frontend
	// computes the ready/amber/red signal from them.
	CutoverLagThresholdSeconds int `json:"cutoverLagThresholdSeconds"` // Max lag (s) still considered "caught up" for cutover (default 5)
	CutoverStableChecks        int `json:"cutoverStableChecks"`        // Consecutive healthy cycles required before showing "ready" (default 3)

	// Parameters for initial migration
	InitialReadBatchSize     int `json:"initialReadBatchSize"`     // Number of documents to read in a batch during initial migration
	InitialWriteBatchSize    int `json:"initialWriteBatchSize"`    // Number of documents to write in a batch during initial migration
	InitialChannelBufferSize int `json:"initialChannelBufferSize"` // Size of channel buffer for batches during initial migration
	InitialMigrationWorkers  int `json:"initialMigrationWorkers"`  // Number of worker goroutines for batch processing
	ConcurrentCollections    int `json:"concurrentCollections"`    // Number of collections to process concurrently

	// Parameters for incremental replication
	IncrementalReadBatchSize       int  `json:"incrementalReadBatchSize"`       // Number of change events to read at once
	IncrementalStreamPartitions    int  `json:"incrementalStreamPartitions"`    // Number of parallel change stream partitions
	IncrementalWriteBatchSize      int  `json:"incrementalWriteBatchSize"`      // Maximum size of operation groups
	IncrementalWorkerCount         int  `json:"incrementalWorkerCount"`         // Number of worker goroutines
	StatsIntervalMinutes           int  `json:"statsIntervalMinutes"`           // Interval for reporting change stream statistics in minutes
	GroupOpsByDistinctId           bool `json:"groupOpsByDistinctId"`           // Enable key-collision grouping instead of optype grouping
	IncrementalIncomingQueueSize   int  `json:"incrementalIncomingQueueSize"`   // Buffer size of workers' raw events queue
	IncrementalProcessingQueueSize int  `json:"incrementalProcessingQueueSize"` // Buffer size of workers' writing batches queue

	// Parallel read configuration for large collections
	ParallelReadsEnabled    bool   `json:"parallelReadsEnabled"`    // Enable parallel reads for large collections
	MaxReadPartitions       int    `json:"maxReadPartitions"`       // Maximum number of partitions for parallel reads
	MinDocsPerPartition     int    `json:"minDocsPerPartition"`     // Minimum number of documents per partition
	MinDocsForParallelReads int    `json:"minDocsForParallelReads"` // Minimum collection size for parallel reads
	SampleSize              int    `json:"sampleSize"`              // Number of documents to sample for partitioning
	WorkersPerPartition     int    `json:"workersPerPartition"`     // Number of worker goroutines per partition
	IDTypeForPartition      string `json:"idTypeForPartition"`      // Partitioning ID type: "mixed", "objectid", or "numeric"

	// Write ramp-up configuration for initial migration
	BackfillRampUp BackfillRampUpConfig `json:"backfillRampUp"`

	// Retry configuration
	RetryConfig RetryConfig `json:"retryConfig"` // Configuration for retry mechanisms

	// Drop empty field names (defaults to false)
	DropEmptyFieldNames bool `json:"dropEmptyFieldNames"`

	// Convert long field names in nested documents (defaults to false)
	ConvertLongFieldNamesInNestedDocs bool `json:"convertLongFieldNamesInNestedDocs"`
}

// BackfillRampUpConfig represents write ramp-up configuration for initial backfill
type BackfillRampUpConfig struct {
	Enabled             bool    `json:"enabled"`
	Strategy            string  `json:"strategy"` // "static" or "adaptive"
	StartQps            float64 `json:"startQps"`
	RampRatePerMin      float64 `json:"rampRatePerMin"`
	UpdateIntervalMs    int     `json:"updateIntervalMs"`
	UseStaggeredWorkers bool    `json:"useStaggeredWorkers"`
	WorkerDelayMs       int     `json:"workerDelayMs"`
}

// RetryConfig represents retry configuration
type RetryConfig struct {
	MaxRetries           int  `json:"maxRetries"`           // Maximum number of retries
	BaseDelayMs          int  `json:"baseDelayMs"`          // Base delay in milliseconds
	MaxDelayMs           int  `json:"maxDelayMs"`           // Maximum delay in milliseconds
	EnableBatchSplitting bool `json:"enableBatchSplitting"` // Enable batch splitting for contention errors
	MinBatchSize         int  `json:"minBatchSize"`         // Minimum batch size for splitting
	ConvertInvalidIds    bool `json:"convertInvalidIds"`    // Convert invalid _id types to string
	ResyncFromSource     bool `json:"resyncFromSource"`     // In retry-dlq mode, re-read each failed doc from the SOURCE by _id (fresh copy) instead of replaying the DLQ snapshot; source-deleted docs are treated as resolved
}

// DatabasePair represents a source and target database pair
type DatabasePair struct {
	Source SourceConfig `json:"source"`
	Target TargetConfig `json:"target"`
}

// SourceConfig represents the source MongoDB configuration
type SourceConfig struct {
	ConnectionString  string `json:"connectionString"`
	Database          string `json:"database"`
	ReplicationMethod string `json:"replicationMethod,omitempty"` // "changestream" (default) or "oplog"
}

// TargetConfig represents the target MongoDB configuration
type TargetConfig struct {
	ConnectionString string             `json:"connectionString"`
	Database         string             `json:"database"`
	Collections      []CollectionConfig `json:"collections,omitempty"`
	SyncAllIndexes   bool               `json:"syncAllIndexes,omitempty"` // Sync all indexes (excluding _id_) from source
	IndexOnly        bool               `json:"indexOnly,omitempty"`      // Only sync indexes, skip data migration
	UpsertMode       bool               `json:"upsertMode,omitempty"`     // Use upsert by default for all collections in this database target
	Indexes          []IndexSyncConfig  `json:"indexes,omitempty"`
	// CollectionTuning holds optional PER-COLLECTION overrides of the partitioned
	// initial-load knobs, keyed by SOURCE collection name. It is independent of
	// Collections, so it applies in whole-database mode too (where Collections is
	// empty). Absent entries — and zero/nil fields within an entry — inherit the
	// global config. This lets a single straggler collection be partitioned
	// independently without changing the global settings.
	CollectionTuning map[string]CollectionTuning `json:"collectionTuning,omitempty"`
}

// CollectionTuning holds optional per-collection overrides for the partitioned
// initial-load knobs. Only the partition levers are per-collection; the
// cross-collection knobs (ConcurrentCollections, worker counts) stay global. A
// nil ParallelReadsEnabled or a zero int means "inherit the global value". The
// motivating case is a collection with few but very large documents: it never
// crosses the global doc-count threshold (MinDocsForParallelReads) so it loads
// on a single cursor and stalls; overriding just that collection forces it to
// partition.
type CollectionTuning struct {
	ParallelReadsEnabled    *bool `json:"parallelReadsEnabled,omitempty"`
	MaxReadPartitions       int   `json:"maxReadPartitions,omitempty"`
	WorkersPerPartition     int   `json:"workersPerPartition,omitempty"`
	MinDocsPerPartition     int   `json:"minDocsPerPartition,omitempty"`
	MinDocsForParallelReads int   `json:"minDocsForParallelReads,omitempty"`
}

// CollectionConfig represents a collection mapping
type CollectionConfig struct {
	SourceCollection string `json:"sourceCollection"`
	TargetCollection string `json:"targetCollection"`
	UpsertMode       bool   `json:"upsertMode,omitempty"` // Use upsert by default instead of insert
}

// IndexSyncConfig represents index sync configuration for a collection
type IndexSyncConfig struct {
	SourceCollection string   `json:"sourceCollection"`
	IndexNames       []string `json:"indexNames"`
}

// LoadConfig loads the configuration from a file
func LoadConfig(configPath string) (*Config, error) {
	// Set default config path if not provided
	if configPath == "" {
		configPath = "mongodb_replication_config.json"
	}

	// Read the config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	// Parse the config strictly (fail on unrecognized fields)
	var config Config
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("error parsing config file (unrecognized configuration fields present): %w", err)
	}

	// Validate the config
	if err := validateConfig(&config); err != nil {
		return nil, err
	}

	// Apply defaults to every unset tunable.
	ApplyDefaults(&config)

	return &config, nil
}

// ApplyDefaults fills in default values for any tunable left at its zero value.
// It is shared by LoadConfig (file-based configs) and by in-memory config
// builders such as the wizard/console, so a programmatically-assembled config
// gets the exact same defaults as one parsed from JSON. Safe to call on an
// already-defaulted config (idempotent).
func ApplyDefaults(config *Config) {
	// Set default save threshold if not provided
	if config.SaveThreshold <= 0 {
		config.SaveThreshold = 100
	}

	// Set default checkpoint interval in minutes if not provided
	if config.CheckpointIntervalMinutes <= 0 {
		config.CheckpointIntervalMinutes = 5 // Default to 5 minutes
	}

	// Set default values for incremental replication parameters
	if config.IncrementalReadBatchSize <= 0 {
		config.IncrementalReadBatchSize = 8192 // Default to 8192 change events
	}

	// This fork targets Firestore, which does not support multi-partition change
	// streams (they require an $expr hash stage in the change-stream pipeline).
	// Force a single stream regardless of any configured value.
	config.IncrementalStreamPartitions = 1

	if config.IndexConcurrency <= 0 {
		config.IndexConcurrency = 1 // Serialize secondary-index builds (Firestore-safe)
	}

	if config.IncrementalWriteBatchSize <= 0 {
		config.IncrementalWriteBatchSize = 128 // Default to 128 operations per group
	}

	if config.IncrementalWorkerCount <= 0 {
		// The incremental apply workers are I/O-bound: each one spends most of its
		// time waiting on a bulk write to the target (against a multi-region
		// Firestore target, bulk-write latency is ~150-200ms). Sizing this to the
		// CPU count therefore under-provisions concurrency — the right number is
		// driven by target write latency, not cores. Empirically (single-region ->
		// nam5), 8 workers cap apply at ~470 events/s while 48 reach ~1,800/s, so a
		// CPU-count default badly lags production single-collection peaks
		// (~1,000 doc-changes/s). Default to 32, which comfortably covers that peak;
		// raise it further for higher sustained write rates (see docs/CONFIGURATION.md).
		config.IncrementalWorkerCount = 32
	}

	if config.IncrementalIncomingQueueSize <= 0 {
		config.IncrementalIncomingQueueSize = 8192 // Default to 8192 change events
	}

	if config.IncrementalProcessingQueueSize <= 0 {
		config.IncrementalProcessingQueueSize = 4096 // Default to 4096 groups
	}

	if config.StatsIntervalMinutes <= 0 {
		config.StatsIntervalMinutes = 5 // Default to 5 minutes
	}

	// Set default flush interval if not provided
	if config.FlushIntervalMs <= 0 {
		config.FlushIntervalMs = 500 // Default to 500 milliseconds
	}

	// Set default min and max pool size if not provided
	if config.TargetMinPoolSize <= 0 {
		config.TargetMinPoolSize = 128
	}
	if config.TargetMaxPoolSize <= 0 {
		config.TargetMaxPoolSize = 256
	}
	// Ensure max pool size is >= min pool size
	if config.TargetMaxPoolSize < config.TargetMinPoolSize {
		config.TargetMaxPoolSize = config.TargetMinPoolSize
	}

	// Set default values for initial migration parameters
	if config.InitialReadBatchSize <= 0 {
		config.InitialReadBatchSize = 8192 // Default to 8192 documents
	}

	if config.InitialWriteBatchSize <= 0 {
		config.InitialWriteBatchSize = 128 // Default to 128 documents
	}

	if config.InitialChannelBufferSize <= 0 {
		config.InitialChannelBufferSize = 10 // Default to buffer for 10 batches
	}

	if config.InitialMigrationWorkers <= 0 {
		config.InitialMigrationWorkers = 5 // Default to 5 worker goroutines
	}

	if config.ConcurrentCollections <= 0 {
		config.ConcurrentCollections = 4 // Default to 4 concurrent collections
	}

	if config.IndexBuildLagThresholdSeconds <= 0 {
		config.IndexBuildLagThresholdSeconds = 5 // Default: treat lag <= 5s as caught up
	}

	if config.IndexBuildLagStableChecks <= 0 {
		config.IndexBuildLagStableChecks = 3 // Default: 3 consecutive low-lag cycles
	}

	if config.CutoverLagThresholdSeconds <= 0 {
		config.CutoverLagThresholdSeconds = 5 // Default: lag <= 5s counts as caught up for cutover
	}

	if config.CutoverStableChecks <= 0 {
		config.CutoverStableChecks = 3 // Default: 3 healthy cycles before showing "ready"
	}

	// Already set default values for incremental replication parameters above

	// Set default values for parallel reads
	if config.MaxReadPartitions <= 0 {
		config.MaxReadPartitions = 8 // Default to 8 partitions
	}

	if config.MinDocsPerPartition <= 0 {
		config.MinDocsPerPartition = 10000 // Default to 10,000 docs per partition
	}

	if config.MinDocsForParallelReads <= 0 {
		config.MinDocsForParallelReads = 50000 // Default to 50,000 docs for parallel reads
	}

	if config.SampleSize <= 0 {
		config.SampleSize = 1000 // Default to 1,000 samples
	}

	// WorkersPerPartition intentionally left at 0 when unset: 0 is the "auto"
	// sentinel resolved by the engine (effectivePartitioning) from the target's
	// write capacity, rather than a fixed default here.

	if config.IDTypeForPartition == "" {
		config.IDTypeForPartition = "auto"
	}

	// Set default values for retry configuration
	if config.RetryConfig.MaxRetries <= 0 {
		config.RetryConfig.MaxRetries = 5 // Default to 5 retries
	}

	if config.RetryConfig.BaseDelayMs <= 0 {
		config.RetryConfig.BaseDelayMs = 100 // Default to 100ms base delay
	}

	if config.RetryConfig.MaxDelayMs <= 0 {
		config.RetryConfig.MaxDelayMs = 5000 // Default to 5s max delay
	}

	if config.RetryConfig.MinBatchSize <= 0 {
		config.RetryConfig.MinBatchSize = 10 // Default to 10 docs per batch
	}

	// This fork migrates Firestore -> Firestore, so every source _id was already
	// accepted by Firestore and is legal on the target. Rewriting it (int->string
	// conversion) would change the document key and make the target diverge from
	// the source, breaking count/hash verification. Force _id passthrough: never
	// convert. (The transform/idmap engine stays intact but is inert with this off.)
	config.RetryConfig.ConvertInvalidIds = false

	// Initialize default values for BackfillRampUpConfig
	// targetQps == 0 or omitted signifies uncapped linear growth (no ceiling).
	if config.BackfillRampUp.RampRatePerMin <= 0 {
		config.BackfillRampUp.RampRatePerMin = 10000.0 // Balanced profile: 10K QPS increase per minute
	}
	if config.BackfillRampUp.UpdateIntervalMs <= 0 {
		config.BackfillRampUp.UpdateIntervalMs = 1000 // Default to 1 second
	}
	if config.BackfillRampUp.Strategy == "" {
		config.BackfillRampUp.Strategy = "static"
	}

	// No backward compatibility needed anymore
}

// validateConfig validates the configuration
func validateConfig(config *Config) error {
	if len(config.DatabasePairs) == 0 {
		return fmt.Errorf("no database pairs specified in config")
	}

	for i, pair := range config.DatabasePairs {
		// Validate source config
		if pair.Source.ConnectionString == "" {
			return fmt.Errorf("source connection string is required for database pair %d", i)
		}
		if pair.Source.Database == "" {
			return fmt.Errorf("source database name is required for database pair %d", i)
		}

		// Validate target config
		if pair.Target.ConnectionString == "" {
			return fmt.Errorf("target connection string is required for database pair %d", i)
		}
		if pair.Target.Database == "" {
			return fmt.Errorf("target database name is required for database pair %d", i)
		}

		// Validate collection configs if provided
		for j, coll := range pair.Target.Collections {
			if coll.SourceCollection == "" {
				return fmt.Errorf("source collection name is required for collection mapping at index %d in database pair %d", j, i)
			}
			if coll.TargetCollection == "" {
				return fmt.Errorf("target collection name is required for collection mapping at index %d in database pair %d", j, i)
			}
		}
	}

	if config.IDTypeForPartition != "" && config.IDTypeForPartition != "auto" && config.IDTypeForPartition != "mixed" && config.IDTypeForPartition != "objectid" && config.IDTypeForPartition != "numeric" {
		return fmt.Errorf("invalid idTypeForPartition: %s. Must be 'auto', 'mixed', 'objectid', or 'numeric'", config.IDTypeForPartition)
	}

	return nil
}

// GetMaxWorkersForLive returns the maximum concurrent workers based on the operation mode.
// It only considers initial migration workers for 'migrate' and 'live' modes,
// and uses strictly the incremental worker count for 'live-only' mode.
func (c *Config) GetMaxWorkersForLive(mode string) int {
	maxWorkers := c.IncrementalWorkerCount
	if mode != "live-only" {
		maxWorkers = max(maxWorkers, c.ConcurrentCollections*c.InitialMigrationWorkers)
	}
	return maxWorkers
}
