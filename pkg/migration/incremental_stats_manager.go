package migration

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/event"
)

const (
	opInsert  = 0
	opUpdate  = 1
	opDelete  = 2
	opReplace = 3
	opMixed   = 4
)

func opIndex(opType string) int {
	switch opType {
	case "insert":
		return opInsert
	case "update":
		return opUpdate
	case "delete":
		return opDelete
	case "replace":
		return opReplace
	case "mixed":
		return opMixed
	}
	return -1
}

type opStats struct {
	workerReceived        int64
	processed             int64
	failed                int64
	dlq                   int64
	totalBulkWriteLatency int64
	bulkWriteLatencyCount int64
	sequentialRetries     int64
}

type poolMonitorStats struct {
	opened       int64
	closed       int64
	open         int64
	inUse        int64
	succeeded    int64
	failed       int64
	returned     int64
	waitDuration int64
}

// PartitionReadStats stores incremental read latency and byte metrics for a specific thread partition.
// It includes manual struct alignment padding to exactly 64 bytes to prevent CPU Cache Line Bouncing (False Sharing)
// when multiple reader goroutines are concurrently writing to adjacent memory sectors!
type PartitionReadStats struct {
	TotalReadLatencyNs int64    // 8 bytes
	ReadCount          int64    // 8 bytes
	TotalReadSizeBytes int64    // 8 bytes
	_                  [40]byte // Padding to align to exactly 64-byte CPU cache lines
}

// IncrementalStatsManager coordinates statistics and replication lag tracking thread-safely
type IncrementalStatsManager struct {
	mu                   sync.Mutex
	lastStatsTime        time.Time
	lagTracker           *LagTracker
	log                  *logger.Logger
	statsInterval        time.Duration
	groupOpsByDistinctId bool
	DryRun               bool

	activeFailedIDs      map[string]string
	activeFailedMu       *sync.RWMutex

	bulkWriteLatenciesHistogram [30000]int64

	// Partition-level read performance statistics (flat array of size 128 for lock-free max speeds!)
	partitionReadStats [128]PartitionReadStats

	// Unified operation statistics
	opsStats [5]opStats

	// Scalar counters updated lock-freely via atomic package
	updatedThenDeletedSinceLastStats      int64
	sequentialRetriesSinceLastStats       int64
	orderedBulkWritesSinceLastStats       int64
	orderedBulkWritesSizeSinceLastStats   int64
	unorderedBulkWritesSinceLastStats     int64
	unorderedBulkWritesSizeSinceLastStats int64
	timeoutFlushesSinceLastStats          int64
	duplicateKeysSinceLastStats           int64
	resolvedCount                         int64
	resolvedCountSinceLastStats           int64

	// Read latency and size tracking (atomically updated)
	totalReadLatencyNs int64
	readCount          int64
	totalReadSizeBytes int64

	// Group flush reasons (atomically tracked)
	groupFlushesOpType    int64
	groupFlushesBatchFull int64
	groupFlushesNamespace int64
	groupFlushesCollision int64

	// Histograms (atomically tracked using fixed length arrays)
	orderedSizesHistogram   [4096]int64
	unorderedSizesHistogram [4096]int64
	readNextLatenciesHistogram [30000]int64
	readSizesHistogram         [16384]int64



	// Worker processed tracking (lock-free fixed-size array)
	workerProcessedSinceLastStats [4096]int64

	// DLQ and failure breakdown tracking
	failureMu        sync.Mutex
	failureBreakdown map[string]map[string]int64 // opType -> simplified error message -> count

	skippedEventsMu sync.Mutex
	skippedEvents   map[string]int64 // opType -> count (resets every reporting interval)

	// Connection Pool stats (atomically tracked)
	sourcePool poolMonitorStats
	targetPool poolMonitorStats

	// Queue snapshot support (protected by mu lock)
	workers     []*Worker
	ingestQueue chan QueueEvent
	dlq         DLQ

	// Stall / Backpressure stats (atomically tracked)
	batchingQueueStallNs   int64
	batchWriteQueueStallNs int64

	// Queue latency delays (atomically tracked)
	ingestQueueWaitNs      int64
	ingestQueueWaitCount   int64
	batchingQueueWaitNs    int64
	batchingQueueWaitCount int64

	// Per-namespace incremental counters (console per-collection live rows).
	// The change-stream path aggregates stats by opType; the console needs them
	// per "db.coll" so the operator can see, at cutover time, exactly which
	// collection is behind or has failures. Populated in RecordLags — the single
	// choke point where every applied/failed op passes through — keyed by
	// op.Namespace. recentLagNs is the end-to-end lag (SuccessTime-EventTime) of
	// the most recently applied op, used as the live lag reading (-1 when idle).
	nsMu        sync.Mutex
	nsStats     map[string]*nsCounter
	recentLagNs int64 // atomic; -1 means no recent applied op (idle)
}

// nsCounter holds cumulative per-namespace incremental counts for the console.
type nsCounter struct {
	processed int64
	failed    int64
	dlq       int64
}

// NsIncrementalStat is a snapshot of one namespace's cumulative incremental
// counters, consumed by the console observer to emit per-collection live rows.
type NsIncrementalStat struct {
	Namespace string // "db.coll"
	Processed int64
	Failed    int64
	DLQ       int64
}

// SetDLQ sets the DLQ reference for monitoring.
func (sm *IncrementalStatsManager) SetDLQ(dlq DLQ) {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.dlq = dlq
}

// RegisterQueues registers the worker pool and ingest queue for monitoring in the IncrementalStatsManager
func (sm *IncrementalStatsManager) RegisterQueues(workers []*Worker, ingestQueue chan QueueEvent) {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.workers = workers
	sm.ingestQueue = ingestQueue
}

// RecordBatchingQueueStall increments the cumulative distributor channel block stall duration
func (sm *IncrementalStatsManager) RecordBatchingQueueStall(d time.Duration) {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.batchingQueueStallNs, int64(d))
}

// RecordBatchWriteQueueStall increments the cumulative worker batch write queue block stall duration
func (sm *IncrementalStatsManager) RecordBatchWriteQueueStall(d time.Duration) {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.batchWriteQueueStallNs, int64(d))
}

// RecordQueueDelays tracks individual ingest and batching queue wait latencies
func (sm *IncrementalStatsManager) RecordQueueDelays(ingestDelay time.Duration, batchingDelay time.Duration) {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.ingestQueueWaitNs, int64(ingestDelay))
	atomic.AddInt64(&sm.ingestQueueWaitCount, 1)
	atomic.AddInt64(&sm.batchingQueueWaitNs, int64(batchingDelay))
	atomic.AddInt64(&sm.batchingQueueWaitCount, 1)
}

// getQueueDepthsSnapshot returns a formatted string of queue depths and utilization percentages safely
func (sm *IncrementalStatsManager) getQueueDepthsSnapshot() string {
	if sm == nil {
		return "N/A"
	}
	sm.mu.Lock()
	workers := sm.workers
	ingestQueue := sm.ingestQueue
	sm.mu.Unlock()

	if len(workers) == 0 {
		return "N/A"
	}

	var totalBatchingLen, totalBatchingCap int64
	var totalBatchWriteLen, totalBatchWriteCap int64
	var maxBatchingUtil float64
	var maxBatchingWorkerID int

	for _, w := range workers {
		if w == nil {
			continue
		}
		incLen := int64(len(w.batchingQueue))
		incCap := int64(cap(w.batchingQueue))
		procLen := int64(len(w.batchWriteQueue))
		procCap := int64(cap(w.batchWriteQueue))

		totalBatchingLen += incLen
		totalBatchingCap += incCap
		totalBatchWriteLen += procLen
		totalBatchWriteCap += procCap

		if incCap > 0 {
			util := float64(incLen) / float64(incCap)
			if util > maxBatchingUtil {
				maxBatchingUtil = util
				maxBatchingWorkerID = w.id
			}
		}
	}

	ingestUtil := 0.0
	if ingestQueue != nil && cap(ingestQueue) > 0 {
		ingestUtil = float64(len(ingestQueue)) / float64(cap(ingestQueue)) * 100
	}

	avgBatchingUtil := 0.0
	if totalBatchingCap > 0 {
		avgBatchingUtil = float64(totalBatchingLen) / float64(totalBatchingCap) * 100
	}

	avgBatchWriteUtil := 0.0
	if totalBatchWriteCap > 0 {
		avgBatchWriteUtil = float64(totalBatchWriteLen) / float64(totalBatchWriteCap) * 100
	}

	return fmt.Sprintf(
		"Ingest: %.1f%% | Batching: [avg: %.1f%%, max: %.1f%% on Worker %d] | Batch Write: [avg: %.1f%%]",
		ingestUtil, avgBatchingUtil, maxBatchingUtil*100, maxBatchingWorkerID, avgBatchWriteUtil,
	)
}

// NewIncrementalStatsManager creates a new IncrementalStatsManager
func NewIncrementalStatsManager(log *logger.Logger, interval time.Duration, groupOpsByDistinctId ...bool) *IncrementalStatsManager {
	distinctId := false
	if len(groupOpsByDistinctId) > 0 {
		distinctId = groupOpsByDistinctId[0]
	}
	return &IncrementalStatsManager{
		lastStatsTime:        time.Now(),
		lagTracker:           NewLagTracker(),
		log:                  log,
		statsInterval:        interval,
		groupOpsByDistinctId: distinctId,
		failureBreakdown:     make(map[string]map[string]int64),
		skippedEvents:        make(map[string]int64),
		nsStats:              make(map[string]*nsCounter),
		recentLagNs:          -1,
	}
}

// recordNamespaceOp updates the per-namespace console counters for one op. Cheap
// map lookup under nsMu; namespace is "db.coll". Called from RecordLags only.
func (sm *IncrementalStatsManager) recordNamespaceOp(namespace string, success, dlqed bool) {
	if namespace == "" {
		return
	}
	sm.nsMu.Lock()
	c := sm.nsStats[namespace]
	if c == nil {
		c = &nsCounter{}
		sm.nsStats[namespace] = c
	}
	if success {
		c.processed++
	} else {
		c.failed++
		if dlqed {
			c.dlq++
		}
	}
	sm.nsMu.Unlock()
}

// NamespaceStatsSnapshot returns a copy of the per-namespace counters for the
// console. Cheap; called every ~2s by the observer poll loop.
func (sm *IncrementalStatsManager) NamespaceStatsSnapshot() []NsIncrementalStat {
	if sm == nil {
		return nil
	}
	sm.nsMu.Lock()
	defer sm.nsMu.Unlock()
	out := make([]NsIncrementalStat, 0, len(sm.nsStats))
	for ns, c := range sm.nsStats {
		out = append(out, NsIncrementalStat{
			Namespace: ns,
			Processed: c.processed,
			Failed:    c.failed,
			DLQ:       c.dlq,
		})
	}
	return out
}

// RecentLagSeconds returns the end-to-end lag (seconds) of the most recently
// applied op, or -1 when no op has been applied yet / the stream is idle. Used
// as the live lag reading for the change-stream (modern) path, where lag is a
// shared stream-position property rather than per-collection.
func (sm *IncrementalStatsManager) RecentLagSeconds() float64 {
	if sm == nil {
		return -1
	}
	ns := atomic.LoadInt64(&sm.recentLagNs)
	if ns < 0 {
		return -1
	}
	return float64(ns) / float64(time.Second)
}

// RecordLags records processing lag for a batch of operations and increments processed events count
func (sm *IncrementalStatsManager) RecordLags(ops []WriteOperation) {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	if sm.lagTracker != nil {
		sm.lagTracker.RecordLags(ops)
	}
	sm.mu.Unlock()

	for _, op := range ops {
		if !op.SuccessTime.IsZero() {
			sm.IncrementEventsProcessed(op.OpType, 1)
			sm.recordNamespaceOp(op.Namespace, true, false)
			// Track the freshest end-to-end lag for the console live reading.
			if !op.EventTime.IsZero() {
				atomic.StoreInt64(&sm.recentLagNs, int64(op.SuccessTime.Sub(op.EventTime)))
			}
		} else {
			sm.IncrementEventsFailed(op.OpType, op.DLQed, op.Error)
			sm.recordNamespaceOp(op.Namespace, false, op.DLQed)
		}
	}
}

// Start begins the periodic statistics reporting loop
func (sm *IncrementalStatsManager) Start(ctx context.Context) {
	ticker := time.NewTicker(sm.statsInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if sm.DryRun {
					sm.ReportDryRunStats()
				} else {
					sm.ReportStats()
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// IncrementUpdatedThenDeleted increments the count of updates skipped due to missing fullDocument thread-safely and lock-freely, and counts it in worker processed QPS.
func (sm *IncrementalStatsManager) IncrementUpdatedThenDeleted(workerID int) {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.updatedThenDeletedSinceLastStats, 1)

	if workerID >= 0 && workerID < 4096 {
		atomic.AddInt64(&sm.workerProcessedSinceLastStats[workerID], 1)
	}
}

// GetUpdatedThenDeleted returns the updatedThenDeleted count thread-safely and lock-freely
func (sm *IncrementalStatsManager) GetUpdatedThenDeleted() int {
	if sm == nil {
		return 0
	}
	return int(atomic.LoadInt64(&sm.updatedThenDeletedSinceLastStats))
}

// GetGroupFlushReasonCount returns the count of group flushes for a specific reason thread-safely and lock-freely
func (sm *IncrementalStatsManager) GetGroupFlushReasonCount(reason string) int {
	if sm == nil {
		return 0
	}
	var count int64
	switch reason {
	case "optype":
		count = atomic.LoadInt64(&sm.groupFlushesOpType)
	case "batchfull":
		count = atomic.LoadInt64(&sm.groupFlushesBatchFull)
	case "namespace":
		count = atomic.LoadInt64(&sm.groupFlushesNamespace)
	case "collision":
		count = atomic.LoadInt64(&sm.groupFlushesCollision)
	}
	return int(count)
}

// IncrementSequentialRetries increments the count of sequential retries thread-safely and lock-freely
func (sm *IncrementalStatsManager) IncrementSequentialRetries(opType string, count int) {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.sequentialRetriesSinceLastStats, int64(count))

	idx := opIndex(opType)
	if idx >= 0 {
		atomic.AddInt64(&sm.opsStats[idx].sequentialRetries, int64(count))
	}
}

// GetSequentialRetries returns the sequential retries count thread-safely and lock-freely
func (sm *IncrementalStatsManager) GetSequentialRetries() int {
	if sm == nil {
		return 0
	}
	return int(atomic.LoadInt64(&sm.sequentialRetriesSinceLastStats))
}

// GetOrderedBulkWrites returns the ordered bulk writes count thread-safely and lock-freely
func (sm *IncrementalStatsManager) GetOrderedBulkWrites() int {
	if sm == nil {
		return 0
	}
	return int(atomic.LoadInt64(&sm.orderedBulkWritesSinceLastStats))
}

// GetOrderedBulkWritesSize returns the ordered bulk writes total size thread-safely and lock-freely
func (sm *IncrementalStatsManager) GetOrderedBulkWritesSize() int {
	if sm == nil {
		return 0
	}
	return int(atomic.LoadInt64(&sm.orderedBulkWritesSizeSinceLastStats))
}

// GetUnorderedBulkWrites returns the unordered bulk writes count thread-safely and lock-freely
func (sm *IncrementalStatsManager) GetUnorderedBulkWrites() int {
	if sm == nil {
		return 0
	}
	return int(atomic.LoadInt64(&sm.unorderedBulkWritesSinceLastStats))
}

// GetUnorderedBulkWritesSize returns the unordered bulk writes total size thread-safely and lock-freely
func (sm *IncrementalStatsManager) GetUnorderedBulkWritesSize() int {
	if sm == nil {
		return 0
	}
	return int(atomic.LoadInt64(&sm.unorderedBulkWritesSizeSinceLastStats))
}

// RecordBulkWrite records a bulk write execution with the given size and ordered status thread-safely and lock-freely
func (sm *IncrementalStatsManager) RecordBulkWrite(size int, isOrdered bool) {
	if sm == nil {
		return
	}

	clampedSize := size
	if clampedSize < 0 {
		clampedSize = 0
	} else if clampedSize >= 4096 {
		clampedSize = 4095
	}

	if isOrdered {
		atomic.AddInt64(&sm.orderedBulkWritesSinceLastStats, 1)
		atomic.AddInt64(&sm.orderedBulkWritesSizeSinceLastStats, int64(size))
		atomic.AddInt64(&sm.orderedSizesHistogram[clampedSize], 1)
	} else {
		atomic.AddInt64(&sm.unorderedBulkWritesSinceLastStats, 1)
		atomic.AddInt64(&sm.unorderedBulkWritesSizeSinceLastStats, int64(size))
		atomic.AddInt64(&sm.unorderedSizesHistogram[clampedSize], 1)
	}
}

// RecordLatency records operation execution latency metrics thread-safely and lock-freely
func (sm *IncrementalStatsManager) RecordLatency(opType string, ops []WriteOperation, bulkOpLatency time.Duration, workerID int, success bool) {
	if sm == nil || len(ops) == 0 {
		return
	}

	latencyMs := int64(bulkOpLatency / time.Millisecond)
	if latencyMs < 0 {
		latencyMs = 0
	} else if latencyMs >= 30000 {
		latencyMs = 29999
	}
	atomic.AddInt64(&sm.bulkWriteLatenciesHistogram[latencyMs], 1)

	dbLatency := int64(bulkOpLatency)
	if sm.groupOpsByDistinctId {
		atomic.AddInt64(&sm.opsStats[opMixed].totalBulkWriteLatency, dbLatency)
		atomic.AddInt64(&sm.opsStats[opMixed].bulkWriteLatencyCount, 1)
	} else {
		idx := opIndex(opType)
		if idx >= 0 {
			atomic.AddInt64(&sm.opsStats[idx].totalBulkWriteLatency, dbLatency)
			atomic.AddInt64(&sm.opsStats[idx].bulkWriteLatencyCount, 1)
		}
	}

	if success {
		if workerID >= 0 && workerID < 4096 {
			atomic.AddInt64(&sm.workerProcessedSinceLastStats[workerID], int64(len(ops)))
		}
	}
}

// GetAvgBulkWriteLatency returns the average bulk write execution latency for a specific operation type thread-safely and lock-freely
func (sm *IncrementalStatsManager) GetAvgBulkWriteLatency(opType string) time.Duration {
	if sm == nil {
		return 0
	}
	var total, count int64
	idx := opIndex(opType)
	if idx >= 0 {
		total = atomic.LoadInt64(&sm.opsStats[idx].totalBulkWriteLatency)
		count = atomic.LoadInt64(&sm.opsStats[idx].bulkWriteLatencyCount)
	}
	if count == 0 {
		return 0
	}
	return time.Duration(total / count)
}

// IncrementTimeoutFlushes increments the count of timeout flushes thread-safely and lock-freely
func (sm *IncrementalStatsManager) IncrementTimeoutFlushes() {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.timeoutFlushesSinceLastStats, 1)
}

// GetTimeoutFlushes returns the timeout flushes count thread-safely and lock-freely
func (sm *IncrementalStatsManager) GetTimeoutFlushes() int {
	if sm == nil {
		return 0
	}
	return int(atomic.LoadInt64(&sm.timeoutFlushesSinceLastStats))
}

// IncrementDuplicateKeys increments the count of duplicate key errors thread-safely and lock-freely
func (sm *IncrementalStatsManager) IncrementDuplicateKeys(count int64) {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.duplicateKeysSinceLastStats, count)
}

// GetDuplicateKeys returns the duplicate keys count thread-safely and lock-freely
func (sm *IncrementalStatsManager) GetDuplicateKeys() int {
	if sm == nil {
		return 0
	}
	return int(atomic.LoadInt64(&sm.duplicateKeysSinceLastStats))
}

// IncrementGroupFlushReason increments the count of group flushes by reason thread-safely and lock-freely
func (sm *IncrementalStatsManager) IncrementGroupFlushReason(reason string) {
	if sm == nil {
		return
	}
	switch reason {
	case "optype":
		atomic.AddInt64(&sm.groupFlushesOpType, 1)
	case "batchfull":
		atomic.AddInt64(&sm.groupFlushesBatchFull, 1)
	case "namespace":
		atomic.AddInt64(&sm.groupFlushesNamespace, 1)
	case "collision":
		atomic.AddInt64(&sm.groupFlushesCollision, 1)
	}
}

// IncrementWorkerProcessed increments the count of successfully processed events by a specific worker thread-safely
func (sm *IncrementalStatsManager) IncrementWorkerProcessed(workerID int, count int) {
	if sm == nil {
		return
	}
	if workerID >= 0 && workerID < 4096 {
		atomic.AddInt64(&sm.workerProcessedSinceLastStats[workerID], int64(count))
	}
}

// IncrementEventsWorkerReceived increments the count of worker received input events by operation type thread-safely and lock-freely
func (sm *IncrementalStatsManager) IncrementEventsWorkerReceived(opType string) {
	if sm == nil {
		return
	}
	idx := opIndex(opType)
	if idx >= 0 {
		atomic.AddInt64(&sm.opsStats[idx].workerReceived, 1)
	}
}

// RecordReadMetric records changeStream.Next latency and document size metrics thread-safely and lock-freely by partition stream index.
func (sm *IncrementalStatsManager) RecordReadMetric(streamIndex int, latency time.Duration, sizeBytes int) {
	if sm == nil {
		return
	}

	// Update legacy/global counters first to maintain full global telemetry compatibility
	atomic.AddInt64(&sm.totalReadLatencyNs, int64(latency))
	atomic.AddInt64(&sm.readCount, 1)
	atomic.AddInt64(&sm.totalReadSizeBytes, int64(sizeBytes))

	latencyMs := int64(latency / time.Millisecond)
	if latencyMs < 0 {
		latencyMs = 0
	} else if latencyMs >= 30000 {
		latencyMs = 29999
	}
	atomic.AddInt64(&sm.readNextLatenciesHistogram[latencyMs], 1)

	sizeKb := int64(sizeBytes / 1024)
	if sizeKb < 0 {
		sizeKb = 0
	} else if sizeKb >= 16384 {
		sizeKb = 16383
	}
	atomic.AddInt64(&sm.readSizesHistogram[sizeKb], 1)



	// Guard boundaries to protect the fixed-size partition array allocation limits
	if streamIndex >= 0 && streamIndex < 128 {
		stats := &sm.partitionReadStats[streamIndex]
		atomic.AddInt64(&stats.TotalReadLatencyNs, int64(latency))
		atomic.AddInt64(&stats.ReadCount, 1)
		atomic.AddInt64(&stats.TotalReadSizeBytes, int64(sizeBytes))
	}
}

// RecordDryRunLag records only the event-to-read lag in dry-run mode.
func (sm *IncrementalStatsManager) RecordDryRunLag(eventTime, readTime time.Time) {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	if sm.lagTracker != nil {
		sm.lagTracker.RecordEventToRead(eventTime, readTime)
	}
	sm.mu.Unlock()
}


// IncrementEventsProcessed increments the count of successfully processed events by operation type thread-safely and lock-freely
func (sm *IncrementalStatsManager) IncrementEventsProcessed(opType string, count int64) {
	if sm == nil {
		return
	}
	idx := opIndex(opType)
	if idx >= 0 {
		atomic.AddInt64(&sm.opsStats[idx].processed, count)
	}
}

// RecordSkippedEvent records a skipped change event of a specific type thread-safely.
// This supports tracking skipped DDL, non-DML, or unprocessable events.
func (sm *IncrementalStatsManager) RecordSkippedEvent(opType string) {
	if sm == nil {
		return
	}
	sm.skippedEventsMu.Lock()
	defer sm.skippedEventsMu.Unlock()
	if sm.skippedEvents == nil {
		sm.skippedEvents = make(map[string]int64)
	}
	sm.skippedEvents[opType]++
}

// RecordDLQResolution records document DLQ resolution counts.
func (sm *IncrementalStatsManager) RecordDLQResolution(count int64) {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.resolvedCount, count)
	atomic.AddInt64(&sm.resolvedCountSinceLastStats, count)
}

// SetActiveFailedIDs sets the reference to the shared active failed document IDs map and its lock
func (sm *IncrementalStatsManager) SetActiveFailedIDs(m map[string]string, mu *sync.RWMutex) {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	sm.activeFailedIDs = m
	sm.activeFailedMu = mu
	sm.mu.Unlock()
}

// GetSkippedEventsCount returns the count of skipped events of the given type thread-safely
func (sm *IncrementalStatsManager) GetSkippedEventsCount(opType string) int64 {
	if sm == nil {
		return 0
	}
	sm.skippedEventsMu.Lock()
	defer sm.skippedEventsMu.Unlock()
	if sm.skippedEvents == nil {
		return 0
	}
	return sm.skippedEvents[opType]
}

func simplifyError(errStr string) string {
	if strings.Contains(errStr, "connection(") && strings.Contains(errStr, "socket was unexpectedly closed: EOF") {
		start := strings.Index(errStr, "connection(")
		if start != -1 {
			end := strings.Index(errStr[start:], ")")
			if end != -1 {
				return "connection(...)" + errStr[start+end+1:]
			}
		}
	}
	return errStr
}

// IncrementEventsFailed increments the count of failed write operations by operation type thread-safely and lock-freely
func (sm *IncrementalStatsManager) IncrementEventsFailed(opType string, dlqed bool, err error) {
	if sm == nil {
		return
	}
	idx := opIndex(opType)
	if idx >= 0 {
		atomic.AddInt64(&sm.opsStats[idx].failed, 1)
		if dlqed {
			atomic.AddInt64(&sm.opsStats[idx].dlq, 1)
		}
	}

	sm.failureMu.Lock()
	defer sm.failureMu.Unlock()

	if sm.failureBreakdown == nil {
		sm.failureBreakdown = make(map[string]map[string]int64)
	}

	if err != nil {
		errMsg := simplifyError(err.Error())
		if sm.failureBreakdown[opType] == nil {
			sm.failureBreakdown[opType] = make(map[string]int64)
		}
		sm.failureBreakdown[opType][errMsg]++
	}
}

// GetDLQCount returns the count of DLQ'ed documents of the given type thread-safely and lock-freely
func (sm *IncrementalStatsManager) GetDLQCount(opType string) int {
	if sm == nil {
		return 0
	}
	idx := opIndex(opType)
	if idx >= 0 {
		return int(atomic.LoadInt64(&sm.opsStats[idx].dlq))
	}
	return 0
}

// GetSourcePoolMonitor returns a MongoDB client PoolMonitor for the source connection pool.
func (sm *IncrementalStatsManager) GetSourcePoolMonitor() *event.PoolMonitor {
	if sm == nil {
		return nil
	}
	return sm.createPoolMonitor(&sm.sourcePool)
}

// GetTargetPoolMonitor returns a MongoDB client PoolMonitor for the target connection pool.
func (sm *IncrementalStatsManager) GetTargetPoolMonitor() *event.PoolMonitor {
	if sm == nil {
		return nil
	}
	return sm.createPoolMonitor(&sm.targetPool)
}

func (sm *IncrementalStatsManager) createPoolMonitor(stats *poolMonitorStats) *event.PoolMonitor {
	return &event.PoolMonitor{
		Event: func(evt *event.PoolEvent) {
			switch evt.Type {
			case event.ConnectionCreated:
				atomic.AddInt64(&stats.opened, 1)
				atomic.AddInt64(&stats.open, 1)
			case event.ConnectionClosed:
				atomic.AddInt64(&stats.closed, 1)
				atomic.AddInt64(&stats.open, -1)
			case event.GetSucceeded:
				atomic.AddInt64(&stats.inUse, 1)
				atomic.AddInt64(&stats.succeeded, 1)
				atomic.AddInt64(&stats.waitDuration, int64(evt.Duration))
			case event.GetFailed:
				atomic.AddInt64(&stats.failed, 1)
				atomic.AddInt64(&stats.waitDuration, int64(evt.Duration))
			case event.ConnectionReturned:
				atomic.AddInt64(&stats.inUse, -1)
				atomic.AddInt64(&stats.returned, 1)
			}
		},
	}
}

func (sm *IncrementalStatsManager) formatPoolStats(stats *poolMonitorStats, name string, duration time.Duration) string {
	opened := atomic.SwapInt64(&stats.opened, 0)
	closed := atomic.SwapInt64(&stats.closed, 0)
	open := atomic.LoadInt64(&stats.open)
	inUse := atomic.LoadInt64(&stats.inUse)
	succeeded := atomic.SwapInt64(&stats.succeeded, 0)
	failed := atomic.SwapInt64(&stats.failed, 0)
	returned := atomic.SwapInt64(&stats.returned, 0)
	waitDuration := atomic.SwapInt64(&stats.waitDuration, 0)

	var rateOpened, rateClosed, rateSucceeded, rateFailed, rateReturned float64
	if duration.Seconds() > 0 {
		rateOpened = float64(opened) / duration.Seconds()
		rateClosed = float64(closed) / duration.Seconds()
		rateSucceeded = float64(succeeded) / duration.Seconds()
		rateFailed = float64(failed) / duration.Seconds()
		rateReturned = float64(returned) / duration.Seconds()
	}

	attempts := succeeded + failed
	avgWaitStr := "N/A"
	if attempts > 0 {
		avgWaitStr = (time.Duration(waitDuration) / time.Duration(attempts)).Round(time.Microsecond).String()
	}

	idle := open - inUse
	if idle < 0 {
		idle = 0
	}

	return fmt.Sprintf("%s: Connections: [Opened: %d (%.2f/sec), Closed: %d (%.2f/sec), Open: %d, In-Use: %d, Idle: %d] Checkouts: [Succeeded: %d (%.2f/sec), Failed: %d (%.2f/sec), Returned: %d (%.2f/sec), Avg Wait: %s]",
		name, opened, rateOpened, closed, rateClosed, open, inUse, idle,
		succeeded, rateSucceeded, failed, rateFailed, returned, rateReturned, avgWaitStr)
}

// GetProcessedCount returns the count of processed events of the given type thread-safely and lock-freely
func (sm *IncrementalStatsManager) GetProcessedCount(opType string) int {
	if sm == nil {
		return 0
	}
	idx := opIndex(opType)
	if idx >= 0 {
		return int(atomic.LoadInt64(&sm.opsStats[idx].processed))
	}
	return 0
}

// TotalProcessed returns the total number of change events applied to the target
// across all operation types. Lock-free; safe for concurrent reads by external
// observers (the web console).
func (sm *IncrementalStatsManager) TotalProcessed() int64 {
	if sm == nil {
		return 0
	}
	var total int64
	for i := 0; i <= opMixed; i++ {
		total += atomic.LoadInt64(&sm.opsStats[i].processed)
	}
	return total
}

// GetWorkerReceivedCount returns the count of worker received events of the given type thread-safely and lock-freely
func (sm *IncrementalStatsManager) GetWorkerReceivedCount(opType string) int {
	if sm == nil {
		return 0
	}
	idx := opIndex(opType)
	if idx >= 0 {
		return int(atomic.LoadInt64(&sm.opsStats[idx].workerReceived))
	}
	return 0
}

// GetFailedCount returns the count of failed events of the given type thread-safely and lock-freely
func (sm *IncrementalStatsManager) GetFailedCount(opType string) int {
	if sm == nil {
		return 0
	}
	idx := opIndex(opType)
	if idx >= 0 {
		return int(atomic.LoadInt64(&sm.opsStats[idx].failed))
	}
	return 0
}

// ReportStats logs the accumulated change stream and lag statistics, resetting internal counters
func (sm *IncrementalStatsManager) ReportStats() {
	// Reset and load atomic counters atomically using atomic.SwapInt64
	updatedThenDeleted := atomic.SwapInt64(&sm.updatedThenDeletedSinceLastStats, 0)
	sequentialRetries := atomic.SwapInt64(&sm.sequentialRetriesSinceLastStats, 0)
	orderedWrites := atomic.SwapInt64(&sm.orderedBulkWritesSinceLastStats, 0)
	orderedWritesSize := atomic.SwapInt64(&sm.orderedBulkWritesSizeSinceLastStats, 0)
	unorderedWrites := atomic.SwapInt64(&sm.unorderedBulkWritesSinceLastStats, 0)
	unorderedWritesSize := atomic.SwapInt64(&sm.unorderedBulkWritesSizeSinceLastStats, 0)
	timeoutFlushes := atomic.SwapInt64(&sm.timeoutFlushesSinceLastStats, 0)
	duplicateKeys := atomic.SwapInt64(&sm.duplicateKeysSinceLastStats, 0)
	resolvedCount := atomic.SwapInt64(&sm.resolvedCountSinceLastStats, 0)

	netFailedCount := 0
	sm.mu.Lock()
	if sm.activeFailedIDs != nil && sm.activeFailedMu != nil {
		sm.activeFailedMu.RLock()
		netFailedCount = len(sm.activeFailedIDs)
		sm.activeFailedMu.RUnlock()
	}
	sm.mu.Unlock()

	totalReadLatencyNs := atomic.SwapInt64(&sm.totalReadLatencyNs, 0)
	readCount := atomic.SwapInt64(&sm.readCount, 0)
	totalReadSizeBytes := atomic.SwapInt64(&sm.totalReadSizeBytes, 0)

	batchingQueueStall := time.Duration(atomic.SwapInt64(&sm.batchingQueueStallNs, 0))
	batchWriteQueueStall := time.Duration(atomic.SwapInt64(&sm.batchWriteQueueStallNs, 0))

	ingestQueueWaitNs := atomic.SwapInt64(&sm.ingestQueueWaitNs, 0)
	ingestQueueWaitCount := atomic.SwapInt64(&sm.ingestQueueWaitCount, 0)
	batchingQueueWaitNs := atomic.SwapInt64(&sm.batchingQueueWaitNs, 0)
	batchingQueueWaitCount := atomic.SwapInt64(&sm.batchingQueueWaitCount, 0)

	var latenciesSnapshot [30000]int64
	var totalWriteCount int64
	for i := 0; i < 30000; i++ {
		val := atomic.SwapInt64(&sm.bulkWriteLatenciesHistogram[i], 0)
		latenciesSnapshot[i] = val
		totalWriteCount += val
	}



	groupFlushesOpType := atomic.SwapInt64(&sm.groupFlushesOpType, 0)
	groupFlushesBatchFull := atomic.SwapInt64(&sm.groupFlushesBatchFull, 0)
	groupFlushesNamespace := atomic.SwapInt64(&sm.groupFlushesNamespace, 0)
	groupFlushesCollision := atomic.SwapInt64(&sm.groupFlushesCollision, 0)

	sm.failureMu.Lock()
	breakdown := sm.failureBreakdown
	sm.failureBreakdown = make(map[string]map[string]int64)
	sm.failureMu.Unlock()

	sm.skippedEventsMu.Lock()
	intervalSkipped := sm.skippedEvents
	sm.skippedEvents = make(map[string]int64)
	sm.skippedEventsMu.Unlock()

	var swapped [5]opStats
	for i := 0; i < 5; i++ {
		swapped[i].workerReceived = atomic.SwapInt64(&sm.opsStats[i].workerReceived, 0)
		swapped[i].processed = atomic.SwapInt64(&sm.opsStats[i].processed, 0)
		swapped[i].failed = atomic.SwapInt64(&sm.opsStats[i].failed, 0)
		swapped[i].dlq = atomic.SwapInt64(&sm.opsStats[i].dlq, 0)
		swapped[i].totalBulkWriteLatency = atomic.SwapInt64(&sm.opsStats[i].totalBulkWriteLatency, 0)
		swapped[i].bulkWriteLatencyCount = atomic.SwapInt64(&sm.opsStats[i].bulkWriteLatencyCount, 0)
		swapped[i].sequentialRetries = atomic.SwapInt64(&sm.opsStats[i].sequentialRetries, 0)
	}

	// Extract worker processed metrics atomically without locks
	workerProcessed := make(map[int]int64)
	for i := 0; i < 4096; i++ {
		count := atomic.SwapInt64(&sm.workerProcessedSinceLastStats[i], 0)
		if count > 0 {
			workerProcessed[i] = count
		}
	}

	// Load histograms under lock
	var orderedSizes [4096]int64
	var unorderedSizes [4096]int64
	for i := 0; i < 4096; i++ {
		orderedSizes[i] = atomic.SwapInt64(&sm.orderedSizesHistogram[i], 0)
		unorderedSizes[i] = atomic.SwapInt64(&sm.unorderedSizesHistogram[i], 0)
	}

	sm.mu.Lock()
	now := time.Now()
	duration := now.Sub(sm.lastStatsTime)
	sm.lastStatsTime = now

	var lagRes LagFlushResult
	if sm.lagTracker != nil {
		lagRes = sm.lagTracker.Flush()
	}
	sm.mu.Unlock()

	avgIngestQueueDelay := time.Duration(0)
	if ingestQueueWaitCount > 0 {
		avgIngestQueueDelay = time.Duration(ingestQueueWaitNs / ingestQueueWaitCount)
	}
	avgBatchingQueueDelay := time.Duration(0)
	if batchingQueueWaitCount > 0 {
		avgBatchingQueueDelay = time.Duration(batchingQueueWaitNs / batchingQueueWaitCount)
	}
	queueDepthsStr := sm.getQueueDepthsSnapshot()

	eventsWorkerReceived := swapped[opInsert].workerReceived + swapped[opUpdate].workerReceived + swapped[opDelete].workerReceived + swapped[opReplace].workerReceived + swapped[opMixed].workerReceived
	eventsFailed := swapped[opInsert].failed + swapped[opUpdate].failed + swapped[opDelete].failed + swapped[opReplace].failed + swapped[opMixed].failed

	insertsWorkerReceived := swapped[opInsert].workerReceived
	updatesWorkerReceived := swapped[opUpdate].workerReceived + swapped[opReplace].workerReceived
	deletesWorkerReceived := swapped[opDelete].workerReceived

	insertsFailed := swapped[opInsert].failed
	updatesFailed := swapped[opUpdate].failed + swapped[opReplace].failed
	deletesFailed := swapped[opDelete].failed

	var eventsProcessed, eventsApplied int64
	var insertsProcessed, updatesProcessed, deletesProcessed int64

	eventsProcessed = swapped[opInsert].processed + swapped[opUpdate].processed + swapped[opDelete].processed + swapped[opReplace].processed + swapped[opMixed].processed + updatedThenDeleted
	eventsApplied = swapped[opInsert].processed + swapped[opUpdate].processed + swapped[opDelete].processed + swapped[opReplace].processed + swapped[opMixed].processed

	insertsProcessed = swapped[opInsert].processed
	updatesProcessed = swapped[opUpdate].processed + swapped[opReplace].processed
	deletesProcessed = swapped[opDelete].processed
	avgReadLatency := time.Duration(0)
	avgReadSize := 0.0
	if readCount > 0 {
		avgReadLatency = time.Duration(totalReadLatencyNs / readCount)
		avgReadSize = float64(totalReadSizeBytes) / float64(readCount)
	}

	// Swap read next latencies histogram
	readNextLatenciesSnapshot := make([]int64, 30000)
	for i := 0; i < 30000; i++ {
		readNextLatenciesSnapshot[i] = atomic.SwapInt64(&sm.readNextLatenciesHistogram[i], 0)
	}

	// Calculate next latency percentiles (p50, p99, p100)
	readLatencyRes := calculatePercentilesFromHistogram(readNextLatenciesSnapshot, []float64{0.50, 0.99, 1.00})
	p50ReadNext := formatLag(time.Duration(readLatencyRes[0]) * time.Millisecond)
	p99ReadNext := formatLag(time.Duration(readLatencyRes[1]) * time.Millisecond)
	p100ReadNext := formatLag(time.Duration(readLatencyRes[2]) * time.Millisecond)

	// Swap read sizes histogram
	readSizesSnapshot := make([]int64, 16384)
	for i := 0; i < 16384; i++ {
		readSizesSnapshot[i] = atomic.SwapInt64(&sm.readSizesHistogram[i], 0)
	}

	// Calculate read sizes percentiles (p50, p90, p100)
	readSizesRes := calculatePercentilesFromHistogram(readSizesSnapshot, []float64{0.50, 0.90, 1.00})
	formatSize := func(kb int) string {
		if kb == -1 {
			return "N/A"
		}
		return formatBytes(int64(kb) * 1024)
	}
	p50ReadSize := formatSize(readSizesRes[0])
	p90ReadSize := formatSize(readSizesRes[1])
	p100ReadSize := formatSize(readSizesRes[2])

	// Reset partition-specific metrics to avoid memory accumulation
	for i := 0; i < 128; i++ {
		stats := &sm.partitionReadStats[i]
		atomic.StoreInt64(&stats.TotalReadLatencyNs, 0)
		atomic.StoreInt64(&stats.ReadCount, 0)
		atomic.StoreInt64(&stats.TotalReadSizeBytes, 0)
	}



	var rateWorkerReceived, rateProcessed, rateApplied, rateFailed, rateUpdatedThenDeleted float64
	var rateInsertsProcessed, rateUpdatesProcessed, rateDeletesProcessed float64
	var rateInsertsFailed, rateUpdatesFailed, rateDeletesFailed float64
	var rateSequentialRetries, rateOrderedWrites, rateUnorderedWrites float64
	var rateTimeoutFlushes, rateGroupFlushesOpType, rateGroupFlushesBatchFull, rateGroupFlushesNamespace, rateGroupFlushesCollision float64
	var rateDuplicateKeys float64

	var rateInsertsWorkerReceived, rateUpdatesWorkerReceived, rateDeletesWorkerReceived float64

	if duration.Seconds() > 0 {
		rateWorkerReceived = float64(eventsWorkerReceived) / duration.Seconds()
		rateProcessed = float64(eventsProcessed) / duration.Seconds()
		rateApplied = float64(eventsApplied) / duration.Seconds()
		rateFailed = float64(eventsFailed) / duration.Seconds()
		rateUpdatedThenDeleted = float64(updatedThenDeleted) / duration.Seconds()
		rateSequentialRetries = float64(sequentialRetries) / duration.Seconds()
		rateOrderedWrites = float64(orderedWrites) / duration.Seconds()
		rateUnorderedWrites = float64(unorderedWrites) / duration.Seconds()
		rateTimeoutFlushes = float64(timeoutFlushes) / duration.Seconds()
		rateDuplicateKeys = float64(duplicateKeys) / duration.Seconds()

		rateGroupFlushesOpType = float64(groupFlushesOpType) / duration.Seconds()
		rateGroupFlushesBatchFull = float64(groupFlushesBatchFull) / duration.Seconds()
		rateGroupFlushesNamespace = float64(groupFlushesNamespace) / duration.Seconds()
		rateGroupFlushesCollision = float64(groupFlushesCollision) / duration.Seconds()

		rateInsertsWorkerReceived = float64(insertsWorkerReceived) / duration.Seconds()
		rateUpdatesWorkerReceived = float64(updatesWorkerReceived) / duration.Seconds()
		rateDeletesWorkerReceived = float64(deletesWorkerReceived) / duration.Seconds()

		rateInsertsProcessed = float64(insertsProcessed) / duration.Seconds()
		rateUpdatesProcessed = float64(updatesProcessed) / duration.Seconds()
		rateDeletesProcessed = float64(deletesProcessed) / duration.Seconds()

		rateInsertsFailed = float64(insertsFailed) / duration.Seconds()
		rateUpdatesFailed = float64(updatesFailed) / duration.Seconds()
		rateDeletesFailed = float64(deletesFailed) / duration.Seconds()
	}

	// Calculate ordered and unordered write percentiles lock-freely and allocation-freely
	orderedRes := calculatePercentilesFromHistogram(orderedSizes[:], []float64{0.50, 0.90, 1.00})
	p50Ordered, p90Ordered, p100Ordered := orderedRes[0], orderedRes[1], orderedRes[2]

	unorderedRes := calculatePercentilesFromHistogram(unorderedSizes[:], []float64{0.50, 0.90, 1.00})
	p50Unordered, p90Unordered, p100Unordered := unorderedRes[0], unorderedRes[1], unorderedRes[2]

	avgOrderedSizeStr := ""
	if orderedWrites > 0 {
		avgOrderedSizeStr = fmt.Sprintf(" (avg size: %.1f, p50: %d, p90: %d, p100: %d)", float64(orderedWritesSize)/float64(orderedWrites), p50Ordered, p90Ordered, p100Ordered)
	}
	avgUnorderedSizeStr := ""
	if unorderedWrites > 0 {
		avgUnorderedSizeStr = fmt.Sprintf(" (avg size: %.1f, p50: %d, p90: %d, p100: %d)", float64(unorderedWritesSize)/float64(unorderedWrites), p50Unordered, p90Unordered, p100Unordered)
	}

	getLatencyStr := func(total int64, count int64) string {
		if count > 0 {
			avg := time.Duration(total / count)
			return avg.Round(time.Millisecond).String()
		}
		return "N/A"
	}

	var avgInsertDb, avgUpdateDb, avgDeleteDb, avgReplaceDb, avgMixedDb string

	avgMixedDb = getLatencyStr(swapped[opMixed].totalBulkWriteLatency, swapped[opMixed].bulkWriteLatencyCount)

	if !sm.groupOpsByDistinctId {
		avgInsertDb = getLatencyStr(swapped[opInsert].totalBulkWriteLatency, swapped[opInsert].bulkWriteLatencyCount)
		avgUpdateDb = getLatencyStr(swapped[opUpdate].totalBulkWriteLatency, swapped[opUpdate].bulkWriteLatencyCount)
		avgDeleteDb = getLatencyStr(swapped[opDelete].totalBulkWriteLatency, swapped[opDelete].bulkWriteLatencyCount)
		avgReplaceDb = getLatencyStr(swapped[opReplace].totalBulkWriteLatency, swapped[opReplace].bulkWriteLatencyCount)
	}

	// Calculate active workers and percentiles
	var workerQps []float64
	for _, count := range workerProcessed {
		if count > 0 {
			qps := float64(count) / duration.Seconds()
			workerQps = append(workerQps, qps)
		}
	}
	activeWorkers := len(workerQps)

	var wQpsP50, wQpsP90, wQpsP99, wQpsP100 float64
	if activeWorkers > 0 {
		sort.Float64s(workerQps)
		wQpsP50 = workerQps[int(float64(activeWorkers)*0.50)]
		wQpsP90 = workerQps[int(float64(activeWorkers)*0.90)]
		wQpsP99 = workerQps[int(float64(activeWorkers)*0.99)]
		wQpsP100 = workerQps[activeWorkers-1]
	}

	// Calculate bulk write latency percentiles (p50/p90/p99/p100)
	latencyRes := calculatePercentilesFromHistogram(latenciesSnapshot[:], []float64{0.50, 0.90, 0.99, 1.00})
	formatMs := func(ms int) string {
		if ms == -1 {
			return "N/A"
		}
		return (time.Duration(ms) * time.Millisecond).String()
	}
	p50Latency := formatMs(latencyRes[0])
	p90Latency := formatMs(latencyRes[1])
	p99Latency := formatMs(latencyRes[2])
	p100Latency := formatMs(latencyRes[3])

	var dbLatencyMsg string
	if sm.groupOpsByDistinctId {
		dbLatencyMsg = avgMixedDb
	} else {
		dbLatencyMsg = fmt.Sprintf("[insert: %s, update: %s, delete: %s, replace: %s, mixed: %s]",
			avgInsertDb, avgUpdateDb, avgDeleteDb, avgReplaceDb, avgMixedDb)
	}

	sequentialRetriesBreakdown := ""
	if sequentialRetries > 0 {
		sequentialRetriesBreakdown = fmt.Sprintf(" [Inserts: %d, Updates: %d, Deletes: %d, Replaces: %d]",
			swapped[opInsert].sequentialRetries, swapped[opUpdate].sequentialRetries, swapped[opDelete].sequentialRetries, swapped[opReplace].sequentialRetries)
	}


	// Format DLQ stats
	dlqOpsStr := "0"
	var dlqOps []string
	opNames := []string{"insert", "update", "delete", "replace", "mixed"}
	for _, op := range opNames {
		idx := opIndex(op)
		if idx >= 0 && swapped[idx].dlq > 0 {
			dlqOps = append(dlqOps, fmt.Sprintf("%s: %d", op, swapped[idx].dlq))
		}
	}
	if len(dlqOps) > 0 {
		dlqOpsStr = "[" + strings.Join(dlqOps, ", ") + "]"
	}
	dlqStatsStr := fmt.Sprintf("      * DLQ'ed: %s (Resolved: %d) [Active Failed: %d]", dlqOpsStr, resolvedCount, netFailedCount)

	// Extract and format connection pool stats for source and target
	sourcePoolStr := sm.formatPoolStats(&sm.sourcePool, "Source", duration)
	targetPoolStr := sm.formatPoolStats(&sm.targetPool, "Target", duration)

	poolStatsStr := fmt.Sprintf("  - Connection Pool:\n"+
		"      * %s\n"+
		"      * %s", sourcePoolStr, targetPoolStr)

	// Format Failure Breakdown succinctly
	failureBreakdownStr := "      * Failure Details (Error/Op):"
	var opBreakdowns []string
	var opsSorted []string
	for op := range breakdown {
		opsSorted = append(opsSorted, op)
	}
	sort.Strings(opsSorted)

	for _, op := range opsSorted {
		errors := breakdown[op]
		if len(errors) > 0 {
			var errParts []string
			var errTextsSorted []string
			for errText := range errors {
				errTextsSorted = append(errTextsSorted, errText)
			}
			sort.Strings(errTextsSorted)

			for _, errText := range errTextsSorted {
				count := errors[errText]
				errParts = append(errParts, fmt.Sprintf("%s: %d", errText, count))
			}
			opBreakdowns = append(opBreakdowns, fmt.Sprintf("%s: {%s}", op, strings.Join(errParts, ", ")))
		}
	}
	if len(opBreakdowns) > 0 {
		failureBreakdownStr += " [" + strings.Join(opBreakdowns, ", ") + "]"
	} else {
		failureBreakdownStr += " 0"
	}

	_ = intervalSkipped



	headerStr := "Change stream statistics"

	processedLine := fmt.Sprintf("  - Processed:      %d (%.2f events/sec) (applied: %d (%.2f/sec)) [Inserts: %d (%.2f/sec), Deletes: %d (%.2f/sec), Updates: %d (%.2f/sec), updatedThenDeleted: %d (%.2f/sec)]",
		eventsProcessed, rateProcessed,
		eventsApplied, rateApplied,
		insertsProcessed, rateInsertsProcessed,
		deletesProcessed, rateDeletesProcessed,
		updatesProcessed, rateUpdatesProcessed,
		updatedThenDeleted, rateUpdatedThenDeleted)

	var rateRead float64
	if duration.Seconds() > 0 {
		rateRead = float64(readCount) / duration.Seconds()
	}

	msg := fmt.Sprintf(headerStr+" (last %v):\n"+
		"  - Read:           %d (%.2f events/sec)\n"+
		"  - WorkerReceived: %d (%.2f events/sec) [Inserts: %d (%.2f/sec), Deletes: %d (%.2f/sec), Updates: %d (%.2f/sec)]\n"+
		"%s\n"+
		"  - Failed:         %d (%.2f events/sec) [Inserts: %d (%.2f/sec), Deletes: %d (%.2f/sec), Updates: %d (%.2f/sec)]\n"+
		"  - BulkWrite Latency: [p50: %s, p90: %s, p99: %s, p100: %s] (avg detail: %s)\n"+
		"  - Worker QPS: [Active: %d] [p50: %.2f, p90: %.2f, p99: %.2f, p100: %.2f]\n"+
		"  - Lags:\n"+
		"      * Event-to-Read:         %s\n"+
		"      * Read-to-worker-receive: %s\n"+
		"      * Receive-to-Apply:      %s\n"+
		"      * End-to-end:            %s\n"+
		"      * End-to-end-with-retry: %s\n"+
		"  - Ordered BulkWrites: %d (%.2f/sec)%s\n"+
		"  - Unordered BulkWrites: %d (%.2f/sec)%s\n"+
		"  - Group Flushes: [optype: %d (%.2f/sec), namespace: %d (%.2f/sec), batchfull: %d (%.2f/sec), id collision: %d (%.2f/sec), timeout: %d (%.2f/sec)]\n"+
		"  - Sequential Retries: %d (%.2f/sec)%s\n"+
		"  - Errors\n"+
		"      * Duplicate Key Errors: %d (%.2f/sec)\n"+
		"%s\n"+
		"%s\n"+
		"  - Queue Performance:\n"+
		"      * Queue Fullness: %s\n"+
		"      * Queue Delays: [Ingest Queue: %s, Batching Queue: %s]\n"+
		"      * Queue Stalls (Backpressure): [Batching Queue Stall (Distributor blocked): %s, Batch Write Queue Stall (Worker blocked): %s]\n"+
		"  - ChangeStream Read Performance:\n"+
		"      * Global Next Latency: avg %s (p50: %s, p99: %s, p100: %s)\n"+
		"      * Event Size: avg %.1f bytes (total %s, rate %s/sec) (p50: %s, p90: %s, p100: %s)\n"+
		"%s",
		duration.Round(time.Second),
		readCount, rateRead,
		eventsWorkerReceived, rateWorkerReceived, insertsWorkerReceived, rateInsertsWorkerReceived, deletesWorkerReceived, rateDeletesWorkerReceived, updatesWorkerReceived, rateUpdatesWorkerReceived,
		processedLine,
		eventsFailed, rateFailed, insertsFailed, rateInsertsFailed, deletesFailed, rateDeletesFailed, updatesFailed, rateUpdatesFailed,
		p50Latency, p90Latency, p99Latency, p100Latency, dbLatencyMsg,
		activeWorkers,
		wQpsP50, wQpsP90, wQpsP99, wQpsP100,
		formatLag(lagRes.EventToReadLag),
		formatLag(lagRes.ReadToWorkerReceiveLag),
		formatLag(lagRes.ReceiveToApplyLag),
		formatLag(lagRes.EndToEndLag),
		formatLag(lagRes.EndToEndWithRetryLag),
		orderedWrites, rateOrderedWrites, avgOrderedSizeStr,
		unorderedWrites, rateUnorderedWrites, avgUnorderedSizeStr,
		groupFlushesOpType, rateGroupFlushesOpType, groupFlushesNamespace, rateGroupFlushesNamespace, groupFlushesBatchFull, rateGroupFlushesBatchFull, groupFlushesCollision, rateGroupFlushesCollision, timeoutFlushes, rateTimeoutFlushes,
		sequentialRetries, rateSequentialRetries, sequentialRetriesBreakdown,
		duplicateKeys, rateDuplicateKeys,
		failureBreakdownStr,
		dlqStatsStr,
		queueDepthsStr,
		formatLag(avgIngestQueueDelay),
		formatLag(avgBatchingQueueDelay),
		formatLag(batchingQueueStall),
		formatLag(batchWriteQueueStall),
		avgReadLatency.Round(time.Microsecond),
		p50ReadNext, p99ReadNext, p100ReadNext,
		avgReadSize,
		formatBytes(totalReadSizeBytes),
		formatBytes(int64(float64(totalReadSizeBytes)/duration.Seconds())),
		p50ReadSize, p90ReadSize, p100ReadSize,
		poolStatsStr)

	sm.log.Info(msg)

	sm.mu.Lock()
	dlq := sm.dlq
	sm.mu.Unlock()

	if dlq != nil {
		count := dlq.Count()
		if count > 100 {
			sm.log.Warnf("DLQ WARNING: The Dead Letter Queue contains %d failed documents! Please check the DLQ file.", count)
		}
	}
}

// ReportDryRunStats logs the dry-run statistics, resetting dry-run specific counters.
func (sm *IncrementalStatsManager) ReportDryRunStats() {
	// Reset and load dry-run atomic counters atomically
	totalReadLatencyNs := atomic.SwapInt64(&sm.totalReadLatencyNs, 0)
	readCount := atomic.SwapInt64(&sm.readCount, 0)
	totalReadSizeBytes := atomic.SwapInt64(&sm.totalReadSizeBytes, 0)

	// Swap read next latencies histogram
	readNextLatenciesSnapshot := make([]int64, 30000)
	for i := 0; i < 30000; i++ {
		readNextLatenciesSnapshot[i] = atomic.SwapInt64(&sm.readNextLatenciesHistogram[i], 0)
	}

	// Calculate next latency percentiles (p50, p99, p100)
	latencyRes := calculatePercentilesFromHistogram(readNextLatenciesSnapshot, []float64{0.50, 0.99, 1.00})
	p50ReadNext := formatLag(time.Duration(latencyRes[0]) * time.Millisecond)
	p99ReadNext := formatLag(time.Duration(latencyRes[1]) * time.Millisecond)
	p100ReadNext := formatLag(time.Duration(latencyRes[2]) * time.Millisecond)

	// Swap read sizes histogram
	readSizesSnapshot := make([]int64, 16384)
	for i := 0; i < 16384; i++ {
		readSizesSnapshot[i] = atomic.SwapInt64(&sm.readSizesHistogram[i], 0)
	}

	// Calculate read sizes percentiles (p50, p90, p100)
	readSizesRes := calculatePercentilesFromHistogram(readSizesSnapshot, []float64{0.50, 0.90, 1.00})
	formatSize := func(kb int) string {
		if kb == -1 {
			return "N/A"
		}
		return formatBytes(int64(kb) * 1024)
	}
	p50ReadSize := formatSize(readSizesRes[0])
	p90ReadSize := formatSize(readSizesRes[1])
	p100ReadSize := formatSize(readSizesRes[2])

	// Reset partition-specific metrics to avoid memory accumulation
	for i := 0; i < 128; i++ {
		stats := &sm.partitionReadStats[i]
		atomic.StoreInt64(&stats.TotalReadLatencyNs, 0)
		atomic.StoreInt64(&stats.ReadCount, 0)
		atomic.StoreInt64(&stats.TotalReadSizeBytes, 0)
	}

	sm.mu.Lock()
	now := time.Now()
	duration := now.Sub(sm.lastStatsTime)
	sm.lastStatsTime = now
	var lagRes LagFlushResult
	if sm.lagTracker != nil {
		lagRes = sm.lagTracker.Flush()
	}
	sm.mu.Unlock()

	var rateProcessed float64
	if duration.Seconds() > 0 {
		rateProcessed = float64(readCount) / duration.Seconds()
	}

	avgLatency := time.Duration(0)
	avgSize := 0.0
	if readCount > 0 {
		avgLatency = time.Duration(totalReadLatencyNs / readCount)
		avgSize = float64(totalReadSizeBytes) / float64(readCount)
	}

	// Format connection pool stats
	sourcePoolStr := sm.formatPoolStats(&sm.sourcePool, "Source", duration)
	targetPoolStr := sm.formatPoolStats(&sm.targetPool, "Target", duration)

	var dryRunByteRate float64
	if duration.Seconds() > 0 {
		dryRunByteRate = float64(totalReadSizeBytes) / duration.Seconds()
	}

	msg := fmt.Sprintf("Change stream statistics in dry-run live-only mode (last %v):\n"+
		"  - Read: %d (%.2f events/sec)\n"+
		"  - ChangeStream Read Performance:\n"+
		"      * Global Next Latency: avg %s (p50: %s, p99: %s, p100: %s)\n"+
		"      * Event Size: avg %.1f bytes (total %s, rate %s/sec) (p50: %s, p90: %s, p100: %s)\n"+
		"  - Lags:\n"+
		"      * Event-to-Read: %s\n"+
		"  - Connection Pool:\n"+
		"      * %s\n"+
		"      * %s",
		duration.Round(time.Second),
		readCount, rateProcessed,
		avgLatency.Round(time.Microsecond),
		p50ReadNext, p99ReadNext, p100ReadNext,
		avgSize,
		formatBytes(totalReadSizeBytes),
		formatBytes(int64(dryRunByteRate)),
		p50ReadSize, p90ReadSize, p100ReadSize,
		formatLag(lagRes.EventToReadLag),
		sourcePoolStr,
		targetPoolStr)

	sm.log.Info(msg)
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// calculatePercentilesFromHistogram returns the values at specified percentile thresholds from an integer-indexed histogram
func calculatePercentilesFromHistogram(histogram []int64, thresholds []float64) []int {
	if len(thresholds) == 0 {
		return nil
	}

	var totalCount int64
	for _, count := range histogram {
		totalCount += count
	}

	results := make([]int, len(thresholds))
	for i := range results {
		results[i] = -1
	}

	if totalCount <= 0 {
		return results
	}

	// Pre-calculate target counts for each threshold to avoid float conversion overhead inside the hot loop
	targets := make([]int64, len(thresholds))
	for i, t := range thresholds {
		targets[i] = int64(float64(totalCount) * t)
	}

	var cumulativeCount int64
	for idx, count := range histogram {
		if count == 0 {
			continue
		}
		cumulativeCount += count

		for i, target := range targets {
			if results[i] == -1 && cumulativeCount >= target {
				results[i] = idx
			}
		}
	}

	// For thresholds not met due to float rounding or trailing zero-sized bins, fill with the last active index
	lastActiveIdx := -1
	for i := len(histogram) - 1; i >= 0; i-- {
		if histogram[i] > 0 {
			lastActiveIdx = i
			break
		}
	}
	for i, res := range results {
		if res == -1 {
			results[i] = lastActiveIdx
		}
	}

	return results
}

// swapPartitionStats atomically swaps and returns active partition statistics snapshot and indices lock-freely
func (sm *IncrementalStatsManager) swapPartitionStats() ([]PartitionReadStats, []int) {
	var partitionSnapshot []PartitionReadStats
	var partitionIndices []int
	for i := 0; i < 128; i++ {
		stats := &sm.partitionReadStats[i]
		latency := atomic.SwapInt64(&stats.TotalReadLatencyNs, 0)
		count := atomic.SwapInt64(&stats.ReadCount, 0)
		size := atomic.SwapInt64(&stats.TotalReadSizeBytes, 0)
		if count > 0 {
			partitionSnapshot = append(partitionSnapshot, PartitionReadStats{
				TotalReadLatencyNs: latency,
				ReadCount:          count,
				TotalReadSizeBytes: size,
			})
			partitionIndices = append(partitionIndices, i)
		}
	}
	return partitionSnapshot, partitionIndices
}

// formatPartitionStats builds a detailed partition-scoped read statistics telemetry log block string
func formatPartitionStats(snapshot []PartitionReadStats, indices []int, duration time.Duration, roundResolution time.Duration) string {
	var partitionReadStr string
	for idx, stats := range snapshot {
		i := indices[idx]
		avgLatency := time.Duration(0)
		avgSize := 0.0
		if stats.ReadCount > 0 {
			avgLatency = time.Duration(stats.TotalReadLatencyNs / stats.ReadCount)
			avgSize = float64(stats.TotalReadSizeBytes) / float64(stats.ReadCount)
		}
		var qps float64
		if duration.Seconds() > 0 {
			qps = float64(stats.ReadCount) / duration.Seconds()
		}
		partitionReadStr += fmt.Sprintf("      * [Partition %d] Next Latency: %s | Event Size: avg %.1f bytes (reads: %d, %.2f events/sec, total %s)\n",
			i, avgLatency.Round(roundResolution).String(), avgSize, stats.ReadCount, qps, formatBytes(stats.TotalReadSizeBytes))
	}
	if len(partitionReadStr) > 0 {
		partitionReadStr = "\n" + strings.TrimSuffix(partitionReadStr, "\n")
	}
	return partitionReadStr
}

func formatLag(d time.Duration) string {
	if d == 0 {
		return "N/A"
	}
	return d.Round(time.Millisecond).String()
}

// GetSourcePoolStatsString returns the formatted pool stats for source database
func (sm *IncrementalStatsManager) GetSourcePoolStatsString(duration time.Duration) string {
	if sm == nil {
		return "Source: Pool stats unavailable"
	}
	return sm.formatPoolStats(&sm.sourcePool, "Source", duration)
}

// GetTargetPoolStatsString returns the formatted pool stats for target database
func (sm *IncrementalStatsManager) GetTargetPoolStatsString(duration time.Duration) string {
	if sm == nil {
		return "Target: Pool stats unavailable"
	}
	return sm.formatPoolStats(&sm.targetPool, "Target", duration)
}

