package migration

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"firestore-dr-migration/pkg/logger"
	"golang.org/x/time/rate"
)

// BackfillStatsManager tracks and logs performance metrics specifically for the one-time backfill phase.
type BackfillStatsManager struct {
	mu            sync.Mutex
	log           *logger.Logger
	lastStatsTime time.Time
	startTime     time.Time
	interval      time.Duration
	DryRun        bool
	incStats      *IncrementalStatsManager // Reference to connection pool monitor
	stopped       atomic.Bool

	// Read metrics (atomically tracked)
	readCount                  int64
	totalReadLatencyNs         int64
	readNextLatenciesHistogram [30000]int64 // fetch latency in ms
	readSizesHistogram         [16384]int64 // document size in KB

	// Write metrics (atomically tracked)
	workerReceived        int64
	successCount          int64
	failedCount           int64
	duplicateKeys         int64
	dlqCount              int64
	resolvedCount         int64

	// Bulk write latency
	totalBulkWriteLatencyNs   int64
	bulkWriteCount            int64
	bulkWriteLatencyHistogram [30000]int64 // bulk write latency in ms

	// Retry metrics (atomically tracked)
	sequentialRetries          int64
	sequentialRetriesInserts   int64
	sequentialRetriesUpdates   int64
	sequentialRetriesDeletes   int64
	sequentialRetriesReplaces  int64

	// Worker metrics
	workerProcessedSinceLastStats [4096]int64

	// Progress & ETC
	targetCount         int64
	cumulativeProcessed int64

	// Ingestion backpressure
	ingestQueueStallNs int64

	// Per-namespace backfill counters (console per-collection 全量 rows). The
	// aggregate atomics above drive the throughput log lines; the console needs
	// the same numbers split per "db.coll" so the operator sees per-collection
	// full-load progress instead of a single "全量汇总" aggregate row. Recorded at
	// the same choke points as the aggregate counters (AddTargetCount / RecordRead
	// / RecordWriteResult), keyed by namespace. Mirrors the incremental path's
	// per-namespace tracking.
	nsMu    sync.Mutex
	nsStats map[string]*backfillNsCounter

	// Write Throttler (optional reference for stats reporting)
	throttler *WriteThrottler
	dlq       DLQ
}

// backfillNsCounter holds cumulative per-namespace backfill counts for the
// console. succeeded mirrors the aggregate successCount (documents written), so
// the per-collection progress bar matches the overall one.
type backfillNsCounter struct {
	target    int64
	succeeded int64
	read      int64
}

// NsBackfillStat is a snapshot of one namespace's cumulative backfill counters,
// consumed by the console observer to emit per-collection 全量 (initial) rows.
type NsBackfillStat struct {
	Namespace string // "db.coll"
	Target    int64
	Succeeded int64
	Read      int64
}

// NewBackfillStatsManager creates a new BackfillStatsManager
func NewBackfillStatsManager(log *logger.Logger, interval time.Duration, incStats *IncrementalStatsManager) *BackfillStatsManager {
	now := time.Now()
	return &BackfillStatsManager{
		log:           log,
		interval:      interval,
		startTime:     now,
		lastStatsTime: now,
		incStats:      incStats,
		nsStats:       make(map[string]*backfillNsCounter),
	}
}

// SetDLQ sets the DLQ reference for monitoring.
func (sm *BackfillStatsManager) SetDLQ(dlq DLQ) {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.dlq = dlq
}

// SetThrottler sets the write throttler reference for metrics reporting.
func (sm *BackfillStatsManager) SetThrottler(wt *WriteThrottler) {
	if sm == nil {
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.throttler = wt
}

// Start runs the periodic statistics reporting loop.
func (sm *BackfillStatsManager) Start(ctx context.Context) {
	if sm == nil || sm.interval <= 0 {
		return
	}
	ticker := time.NewTicker(sm.interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if sm.stopped.Load() {
					return
				}
				sm.ReportStats(false)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop stops the background reporting loop and prevents further prints.
func (sm *BackfillStatsManager) Stop() {
	if sm == nil {
		return
	}
	sm.stopped.Store(true)
}

// nsCounterFor returns the per-namespace counter for namespace, creating it on
// first use. The caller must hold sm.nsMu.
func (sm *BackfillStatsManager) nsCounterFor(namespace string) *backfillNsCounter {
	c := sm.nsStats[namespace]
	if c == nil {
		c = &backfillNsCounter{}
		sm.nsStats[namespace] = c
	}
	return c
}

// RecordRead records metrics for a document fetched from the source database
// cursor. namespace is the source "db.coll" the document was read from (empty is
// tolerated: only the aggregate counters update).
func (sm *BackfillStatsManager) RecordRead(namespace string, latency time.Duration, sizeBytes int) {
	if sm == nil {
		return
	}
	if namespace != "" {
		sm.nsMu.Lock()
		sm.nsCounterFor(namespace).read++
		sm.nsMu.Unlock()
	}
	atomic.AddInt64(&sm.readCount, 1)
	atomic.AddInt64(&sm.totalReadLatencyNs, int64(latency))

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
}

// RecordWorkerReceived increments the count of documents received by write workers.
func (sm *BackfillStatsManager) RecordWorkerReceived(count int64) {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.workerReceived, count)
}

// RecordWriteResult records document write result counts. namespace is the
// source "db.coll" the batch belongs to (empty is tolerated: only the aggregate
// counters update). Only succeeded is tracked per-namespace, mirroring the
// aggregate successCount the console progress bar reflects.
func (sm *BackfillStatsManager) RecordWriteResult(namespace string, succeeded, failed, duplicates, dlq int64, workerID int) {
	if sm == nil {
		return
	}
	if namespace != "" && succeeded != 0 {
		sm.nsMu.Lock()
		sm.nsCounterFor(namespace).succeeded += succeeded
		sm.nsMu.Unlock()
	}
	atomic.AddInt64(&sm.successCount, succeeded)
	atomic.AddInt64(&sm.failedCount, failed)
	atomic.AddInt64(&sm.duplicateKeys, duplicates)
	atomic.AddInt64(&sm.dlqCount, dlq)
	atomic.AddInt64(&sm.cumulativeProcessed, succeeded+failed+duplicates)

	if workerID >= 0 && workerID < 4096 {
		atomic.AddInt64(&sm.workerProcessedSinceLastStats[workerID], succeeded+failed+duplicates)
	}
}

// RecordDLQResolution records document DLQ resolution counts.
func (sm *BackfillStatsManager) RecordDLQResolution(count int64) {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.resolvedCount, count)
}

// RecordBulkWrite records bulk write latency.
func (sm *BackfillStatsManager) RecordBulkWrite(latency time.Duration) {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.totalBulkWriteLatencyNs, int64(latency))
	atomic.AddInt64(&sm.bulkWriteCount, 1)

	latencyMs := int64(latency / time.Millisecond)
	if latencyMs < 0 {
		latencyMs = 0
	} else if latencyMs >= 30000 {
		latencyMs = 29999
	}
	atomic.AddInt64(&sm.bulkWriteLatencyHistogram[latencyMs], 1)
}

// IncrementSequentialRetries increments the count of sequential retries thread-safely and lock-freely
func (sm *BackfillStatsManager) IncrementSequentialRetries(opType string, count int64) {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.sequentialRetries, count)
	switch opType {
	case "insert":
		atomic.AddInt64(&sm.sequentialRetriesInserts, count)
	case "update":
		atomic.AddInt64(&sm.sequentialRetriesUpdates, count)
	case "delete":
		atomic.AddInt64(&sm.sequentialRetriesDeletes, count)
	case "replace":
		atomic.AddInt64(&sm.sequentialRetriesReplaces, count)
	}
}

// AddTargetCount increases the expected total document count of the backfill.
// namespace is the source "db.coll" the count belongs to (empty is tolerated:
// only the aggregate target updates). Called once per collection at the start of
// its load, so the per-namespace target is known before writes begin.
func (sm *BackfillStatsManager) AddTargetCount(namespace string, count int64) {
	if sm == nil {
		return
	}
	if namespace != "" {
		sm.nsMu.Lock()
		sm.nsCounterFor(namespace).target += count
		sm.nsMu.Unlock()
	}
	atomic.AddInt64(&sm.targetCount, count)
}

// ProgressSnapshot returns a consistent read of backfill progress for external
// observers (the web console). targetCount is the number of documents scheduled
// for migration (0 until counting completes), succeeded is the number written so
// far, read is the number fetched from the source, and elapsed is time since the
// manager started. All reads are atomic; safe to call concurrently.
func (sm *BackfillStatsManager) ProgressSnapshot() (targetCount, succeeded, read int64, elapsed time.Duration) {
	if sm == nil {
		return 0, 0, 0, 0
	}
	targetCount = atomic.LoadInt64(&sm.targetCount)
	succeeded = atomic.LoadInt64(&sm.successCount)
	read = atomic.LoadInt64(&sm.readCount)
	// startTime is set once at construction and never mutated, so it is safe to
	// read without the mutex.
	if !sm.startTime.IsZero() {
		elapsed = time.Since(sm.startTime)
	}
	return targetCount, succeeded, read, elapsed
}

// NamespaceBackfillSnapshot returns a copy of the per-namespace backfill counters
// for the console. Cheap; called every ~2s by the observer poll loop to emit one
// 全量 (initial) row per collection.
func (sm *BackfillStatsManager) NamespaceBackfillSnapshot() []NsBackfillStat {
	if sm == nil {
		return nil
	}
	sm.nsMu.Lock()
	defer sm.nsMu.Unlock()
	out := make([]NsBackfillStat, 0, len(sm.nsStats))
	for ns, c := range sm.nsStats {
		out = append(out, NsBackfillStat{
			Namespace: ns,
			Target:    c.target,
			Succeeded: c.succeeded,
			Read:      c.read,
		})
	}
	return out
}

// RecordIngestQueueStall records time spent waiting for worker channels to accept batches.
func (sm *BackfillStatsManager) RecordIngestQueueStall(d time.Duration) {
	if sm == nil {
		return
	}
	atomic.AddInt64(&sm.ingestQueueStallNs, d.Nanoseconds())
}

// ReportStats logs the metrics, resetting interval counters.
func (sm *BackfillStatsManager) ReportStats(isFinal bool) {
	if sm == nil {
		return
	}

	// Swap atomic metrics atomically
	readCount := atomic.SwapInt64(&sm.readCount, 0)
	totalReadLatencyNs := atomic.SwapInt64(&sm.totalReadLatencyNs, 0)
	workerReceived := atomic.SwapInt64(&sm.workerReceived, 0)
	successCount := atomic.SwapInt64(&sm.successCount, 0)
	failedCount := atomic.SwapInt64(&sm.failedCount, 0)
	duplicateKeys := atomic.SwapInt64(&sm.duplicateKeys, 0)
	dlqCount := atomic.SwapInt64(&sm.dlqCount, 0)
	resolvedCount := atomic.SwapInt64(&sm.resolvedCount, 0)
	ingestQueueStall := time.Duration(atomic.SwapInt64(&sm.ingestQueueStallNs, 0))

	// Swap bulk write latency and sequential retries metrics
	totalBulkWriteLatencyNs := atomic.SwapInt64(&sm.totalBulkWriteLatencyNs, 0)
	bulkWriteCount := atomic.SwapInt64(&sm.bulkWriteCount, 0)
	sequentialRetries := atomic.SwapInt64(&sm.sequentialRetries, 0)
	sequentialRetriesInserts := atomic.SwapInt64(&sm.sequentialRetriesInserts, 0)
	sequentialRetriesUpdates := atomic.SwapInt64(&sm.sequentialRetriesUpdates, 0)
	sequentialRetriesDeletes := atomic.SwapInt64(&sm.sequentialRetriesDeletes, 0)
	sequentialRetriesReplaces := atomic.SwapInt64(&sm.sequentialRetriesReplaces, 0)

	targetCount := atomic.LoadInt64(&sm.targetCount)
	cumulativeProcessed := atomic.LoadInt64(&sm.cumulativeProcessed)

	var readNextLatenciesSnapshot [30000]int64
	for i := 0; i < 30000; i++ {
		readNextLatenciesSnapshot[i] = atomic.SwapInt64(&sm.readNextLatenciesHistogram[i], 0)
	}

	var readSizesSnapshot [16384]int64
	var totalReadSizeBytes int64
	for i := 0; i < 16384; i++ {
		val := atomic.SwapInt64(&sm.readSizesHistogram[i], 0)
		readSizesSnapshot[i] = val
		totalReadSizeBytes += val * 1024
	}

	var bulkWriteLatenciesSnapshot [30000]int64
	for i := 0; i < 30000; i++ {
		bulkWriteLatenciesSnapshot[i] = atomic.SwapInt64(&sm.bulkWriteLatencyHistogram[i], 0)
	}

	workerProcessed := make(map[int]int64)
	for i := 0; i < 4096; i++ {
		count := atomic.SwapInt64(&sm.workerProcessedSinceLastStats[i], 0)
		if count > 0 {
			workerProcessed[i] = count
		}
	}

	sm.mu.Lock()
	now := time.Now()
	duration := now.Sub(sm.lastStatsTime)
	sm.lastStatsTime = now
	wt := sm.throttler
	sm.mu.Unlock()

	if duration.Seconds() <= 0 {
		return
	}

	rateRead := float64(readCount) / duration.Seconds()
	rateReceived := float64(workerReceived) / duration.Seconds()
	rateSuccess := float64(successCount) / duration.Seconds()
	rateFailed := float64(failedCount) / duration.Seconds()
	rateDuplicates := float64(duplicateKeys) / duration.Seconds()
	rateProcessed := float64(successCount+failedCount+duplicateKeys) / duration.Seconds()

	var rateSequentialRetries float64
	if duration.Seconds() > 0 {
		rateSequentialRetries = float64(sequentialRetries) / duration.Seconds()
	}
	sequentialRetriesBreakdown := ""
	if sequentialRetries > 0 {
		sequentialRetriesBreakdown = fmt.Sprintf(" [Inserts: %d, Updates: %d, Deletes: %d, Replaces: %d]",
			sequentialRetriesInserts, sequentialRetriesUpdates, sequentialRetriesDeletes, sequentialRetriesReplaces)
	}

	// Calculate read fetch latency percentiles (p50, p99, p100)
	readLatencyRes := calculatePercentilesFromHistogram(readNextLatenciesSnapshot[:], []float64{0.50, 0.99, 1.00})
	p50Read := formatLag(time.Duration(readLatencyRes[0]) * time.Millisecond)
	p99Read := formatLag(time.Duration(readLatencyRes[1]) * time.Millisecond)
	p100Read := formatLag(time.Duration(readLatencyRes[2]) * time.Millisecond)

	// Calculate read sizes percentiles (p50, p90, p100)
	readSizesRes := calculatePercentilesFromHistogram(readSizesSnapshot[:], []float64{0.50, 0.90, 1.00})
	formatSize := func(kb int) string {
		if kb == -1 {
			return "N/A"
		}
		return formatBytes(int64(kb) * 1024)
	}
	p50Size := formatSize(readSizesRes[0])
	p90Size := formatSize(readSizesRes[1])
	p100Size := formatSize(readSizesRes[2])

	// Calculate bulk write latency percentiles (p50, p90, p99, p100)
	bulkWriteLatencyRes := calculatePercentilesFromHistogram(bulkWriteLatenciesSnapshot[:], []float64{0.50, 0.90, 0.99, 1.00})
	p50BulkWrite := formatMs(bulkWriteLatencyRes[0])
	p90BulkWrite := formatMs(bulkWriteLatencyRes[1])
	p99BulkWrite := formatMs(bulkWriteLatencyRes[2])
	p100BulkWrite := formatMs(bulkWriteLatencyRes[3])

	// Calculate worker QPS stats
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

	// Calculate read fetch latency avg
	avgReadLatency := time.Duration(0)
	if readCount > 0 {
		avgReadLatency = time.Duration(totalReadLatencyNs / readCount)
	}
	avgReadSize := 0.0
	if readCount > 0 {
		avgReadSize = float64(totalReadSizeBytes) / float64(readCount)
	}
	avgBulkWriteLatency := time.Duration(0)
	if bulkWriteCount > 0 {
		avgBulkWriteLatency = time.Duration(totalBulkWriteLatencyNs / bulkWriteCount)
	}

	header := "Initial Backfill statistics"
	if isFinal {
		header = "Initial Backfill FINAL statistics summary"
		duration = now.Sub(sm.startTime)
	}

	progressStr := ""
	if targetCount > 0 {
		pct := float64(cumulativeProcessed) / float64(targetCount) * 100.0
		if pct > 100.0 {
			pct = 100.0
		}

		remainingStr := "N/A"
		totalDuration := now.Sub(sm.startTime)
		if totalDuration.Seconds() > 0 {
			overallRate := float64(cumulativeProcessed) / totalDuration.Seconds()
			if overallRate > 0 {
				remainingDocs := targetCount - cumulativeProcessed
				if remainingDocs < 0 {
					remainingDocs = 0
				}
				remainingDuration := time.Duration(float64(remainingDocs)/overallRate) * time.Second
				remainingStr = remainingDuration.Round(time.Second).String()
			}
		}

		progressStr = fmt.Sprintf("  - Progress:         %.1f%% (%d / %d docs) [Remaining: %s]\n",
			pct, cumulativeProcessed, targetCount, remainingStr)
	}

	throttlerStr := ""
	if wt != nil {
		limit := wt.Limit()
		if limit == rate.Inf {
			throttlerStr = "  - Throttler:      Inactive (Limit: Inf QPS)\n"
		} else {
			throttlerStr = fmt.Sprintf("  - Throttler:      Active (Limit: %.2f QPS)\n", float64(limit))
		}
	}

	backpressureStr := fmt.Sprintf("  - Ingestion Stalls: [Cursor blocked waiting for workers: %s]\n",
		formatLag(ingestQueueStall))

	poolStatsStr := ""
	if sm.incStats != nil {
		poolStatsStr = fmt.Sprintf("\n  - Connection Pool:\n      * %s\n      * %s",
			sm.incStats.GetSourcePoolStatsString(duration),
			sm.incStats.GetTargetPoolStatsString(duration),
		)
	}

	rateBytesRead := float64(totalReadSizeBytes) / duration.Seconds()

	msg := fmt.Sprintf(header+" (duration: %v):\n"+
		"%s"+
		"%s"+
		"  - Read:           %d (%.2f docs/sec)\n"+
		"  - WorkerReceived: %d (%.2f docs/sec)\n"+
		"  - Processed (Total): %d (%.2f docs/sec) [Applied: %d (%.2f/sec), Skipped Duplicates: %d (%.2f/sec)]\n"+
		"  - Failed:         %d (%.2f docs/sec)\n"+
		"  - BulkWrite Latency:    [p50: %s, p90: %s, p99: %s, p100: %s] (avg: %s)\n"+
		"  - Sequential Retries:   %d (%.2f/sec)%s\n"+
		"  - Worker QPS: [Active: %d] [p50: %.2f, p90: %.2f, p99: %.2f, p100: %.2f]\n"+
		"  - Errors\n"+
		"      * Duplicate Key Errors: %d (%.2f/sec)\n"+
		"      * DLQ'ed: %d (Resolved: %d)\n"+
		"  - Read Performance:\n"+
		"      * Fetch Latency: avg %s (p50: %s, p99: %s, p100: %s)\n"+
		"      * Document Size: avg %.1f bytes (total %s, rate %s/sec) (p50: %s, p90: %s, p100: %s)\n"+
		"%s%s",
		duration.Round(time.Second),
		progressStr,
		throttlerStr,
		readCount, rateRead,
		workerReceived, rateReceived,
		successCount+failedCount+duplicateKeys, rateProcessed, successCount, rateSuccess, duplicateKeys, rateDuplicates,
		failedCount, rateFailed,
		p50BulkWrite, p90BulkWrite, p99BulkWrite, p100BulkWrite, avgBulkWriteLatency.Round(time.Millisecond),
		sequentialRetries, rateSequentialRetries, sequentialRetriesBreakdown,
		activeWorkers, wQpsP50, wQpsP90, wQpsP99, wQpsP100,
		duplicateKeys, rateDuplicates,
		dlqCount, resolvedCount,
		avgReadLatency.Round(time.Microsecond), p50Read, p99Read, p100Read,
		avgReadSize, formatBytes(totalReadSizeBytes), formatBytes(int64(rateBytesRead)), p50Size, p90Size, p100Size,
		backpressureStr,
		poolStatsStr,
	)

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

func formatMs(ms int) string {
	if ms == -1 {
		return "N/A"
	}
	return (time.Duration(ms) * time.Millisecond).String()
}
