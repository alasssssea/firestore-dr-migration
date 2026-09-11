package migration

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"firestore-dr-migration/pkg/logger"
)

func TestIncrementalStatsManagerEventCounting(t *testing.T) {
	log := logger.New()
	sm := NewIncrementalStatsManager(log, 5*time.Minute)

	now := time.Now()

	// Record batch of 5 operations of different types (SuccessTime must be set to count them as processed)
	ops5 := []WriteOperation{
		{OpType: "insert", SuccessTime: now},
		{OpType: "update", SuccessTime: now},
		{OpType: "delete", SuccessTime: now},
		{OpType: "replace", SuccessTime: now},
		{OpType: "insert", SuccessTime: now},
	}
	sm.RecordLags(ops5)
	
	totalProcessed5 := sm.GetProcessedCount("insert") + sm.GetProcessedCount("update") + sm.GetProcessedCount("replace") + sm.GetProcessedCount("delete")
	if totalProcessed5 != 5 {
		t.Errorf("expected total processed 5, got %d", totalProcessed5)
	}
	if sm.GetProcessedCount("insert") != 2 {
		t.Errorf("expected inserts processed count 2, got %d", sm.GetProcessedCount("insert"))
	}
	if sm.GetProcessedCount("update")+sm.GetProcessedCount("replace") != 2 {
		t.Errorf("expected updates+replaces processed count 2, got %d", sm.GetProcessedCount("update")+sm.GetProcessedCount("replace"))
	}
	if sm.GetProcessedCount("delete") != 1 {
		t.Errorf("expected deletes processed count 1, got %d", sm.GetProcessedCount("delete"))
	}

	// Record batch of 10 operations
	ops10 := make([]WriteOperation, 10)
	for i := range ops10 {
		ops10[i].OpType = "insert"
		ops10[i].SuccessTime = now
	}
	sm.RecordLags(ops10)
	
	totalProcessed15 := sm.GetProcessedCount("insert") + sm.GetProcessedCount("update") + sm.GetProcessedCount("replace") + sm.GetProcessedCount("delete")
	if totalProcessed15 != 15 {
		t.Errorf("expected total processed 15, got %d", totalProcessed15)
	}
	if sm.GetProcessedCount("insert") != 12 {
		t.Errorf("expected inserts processed count 12, got %d", sm.GetProcessedCount("insert"))
	}
}

func TestIncrementalStatsManagerReportStats(t *testing.T) {
	log := logger.New()
	sm := NewIncrementalStatsManager(log, 5*time.Minute)

	now := time.Now()
	ops := []WriteOperation{
		{EventTime: now.Add(-250 * time.Millisecond), OpType: "insert", SuccessTime: now},
	}
	sm.RecordLags(ops)

	// Call ReportStats
	sm.ReportStats()

	// Counters should reset to 0 after report
	if sm.GetProcessedCount("insert") != 0 {
		t.Errorf("expected inserts processed count to reset to 0, got %d", sm.GetProcessedCount("insert"))
	}
	
	res := sm.lagTracker.Flush()
	if res.EndToEndLag != 0 {
		t.Errorf("expected lagTracker metrics to reset to 0, got %v", res.EndToEndLag)
	}
}

func TestIncrementalStatsManagerRecordLagsAndStart(t *testing.T) {
	log := logger.New()
	// Use a very small interval for testing Start
	sm := NewIncrementalStatsManager(log, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sm.Start(ctx)

	now := time.Now()
	ops := []WriteOperation{
		{EventTime: now.Add(-100 * time.Millisecond), SuccessTime: now},
		{EventTime: now.Add(-200 * time.Millisecond), SuccessTime: now},
	}

	// Call RecordLags on IncrementalStatsManager
	sm.RecordLags(ops)

	// Wait for periodic ticker to fire
	time.Sleep(25 * time.Millisecond)

	// The counters should have been flushed/reset to 0 by ReportStats inside the ticker goroutine
	sm.mu.Lock()
	res := sm.lagTracker.Flush()
	sm.mu.Unlock()

	if res.EndToEndLag != 0 {
		t.Errorf("expected lag count to reset to 0 after periodic ticker, got %v", res.EndToEndLag)
	}
}

func TestIncrementalStatsManagerConcurrency(t *testing.T) {
	log := logger.New()
	sm := NewIncrementalStatsManager(log, 5*time.Minute)

	var wg sync.WaitGroup
	workersCount := 25
	iterations := 150
	now := time.Now()

	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				sm.RecordLags([]WriteOperation{
					{EventTime: now.Add(-1 * time.Millisecond), OpType: "insert", SuccessTime: now},
				})
			}
		}()
	}

	wg.Wait()

	expectedEvents := workersCount * iterations
	processedEvents := sm.GetProcessedCount("insert")
	if processedEvents != expectedEvents {
		t.Errorf("expected %d events, got %d", expectedEvents, processedEvents)
	}
	
	res := sm.lagTracker.Flush()
	if res.EndToEndLag != 1*time.Millisecond {
		t.Errorf("expected EndToEndLag 1ms, got %v", res.EndToEndLag)
	}

	res2 := sm.lagTracker.Flush()
	if res2.EndToEndLag != 0 {
		t.Errorf("expected lagTracker to reset to 0, got %v", res2.EndToEndLag)
	}
}

func TestIncrementalStatsManagerMetrics(t *testing.T) {
	log := logger.New()
	sm := NewIncrementalStatsManager(log, 5*time.Minute)

	// Test single increment for all three
	sm.IncrementUpdatedThenDeleted(0)
	sm.IncrementEventsWorkerReceived("insert")
	sm.IncrementEventsFailed("insert", false, nil)
	sm.IncrementSequentialRetries("insert", 3)
	sm.RecordBulkWrite(42, true)
	sm.RecordBulkWrite(10, false)
	sm.IncrementTimeoutFlushes()
	sm.IncrementDuplicateKeys(5)

	missingCount := sm.GetUpdatedThenDeleted()
	inputCount := sm.GetWorkerReceivedCount("insert")
	failedCount := sm.GetFailedCount("insert")
	seqRetriesCount := sm.GetSequentialRetries()
	orderedWritesCount := sm.GetOrderedBulkWrites()
	orderedWritesSize := sm.GetOrderedBulkWritesSize()
	unorderedWritesCount := sm.GetUnorderedBulkWrites()
	unorderedWritesSize := sm.GetUnorderedBulkWritesSize()
	timeoutFlushesCount := sm.GetTimeoutFlushes()
	duplicateKeysCount := sm.GetDuplicateKeys()

	if missingCount != 1 {
		t.Errorf("expected updatedThenDeletedSinceLastStats 1, got %d", missingCount)
	}
	if inputCount != 1 {
		t.Errorf("expected insertsWorkerReceivedSinceLastStats 1, got %d", inputCount)
	}
	if failedCount != 1 {
		t.Errorf("expected insertsFailedSinceLastStats 1, got %d", failedCount)
	}
	if seqRetriesCount != 3 {
		t.Errorf("expected sequential retries 3, got %d", seqRetriesCount)
	}
	if orderedWritesCount != 1 {
		t.Errorf("expected ordered bulk writes 1, got %d", orderedWritesCount)
	}
	if orderedWritesSize != 42 {
		t.Errorf("expected ordered bulk writes size 42, got %d", orderedWritesSize)
	}
	if unorderedWritesCount != 1 {
		t.Errorf("expected unordered bulk writes 1, got %d", unorderedWritesCount)
	}
	if unorderedWritesSize != 10 {
		t.Errorf("expected unordered bulk writes size 10, got %d", unorderedWritesSize)
	}
	if timeoutFlushesCount != 1 {
		t.Errorf("expected timeout flushes 1, got %d", timeoutFlushesCount)
	}
	if duplicateKeysCount != 5 {
		t.Errorf("expected duplicate keys 5, got %d", duplicateKeysCount)
	}

	// Test concurrent increments
	var wg sync.WaitGroup
	workersCount := 10
	iterations := 100
	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				sm.IncrementUpdatedThenDeleted(0)
				sm.IncrementEventsWorkerReceived("insert")
				sm.IncrementEventsFailed("insert", false, nil)
			}
		}()
	}
	wg.Wait()

	missingCount = sm.GetUpdatedThenDeleted()
	inputCount = sm.GetWorkerReceivedCount("insert")
	failedCount = sm.GetFailedCount("insert")

	expectedCount := 1 + workersCount*iterations
	if missingCount != expectedCount {
		t.Errorf("expected updatedThenDeletedSinceLastStats %d, got %d", expectedCount, missingCount)
	}
	if inputCount != expectedCount {
		t.Errorf("expected insertsWorkerReceivedSinceLastStats %d, got %d", expectedCount, inputCount)
	}
	if failedCount != expectedCount {
		t.Errorf("expected insertsFailedSinceLastStats %d, got %d", expectedCount, failedCount)
	}

	// Verify ReportStats logs without panic
	sm.ReportStats()

	// Counters should reset after ReportStats
	missingCountAfter := sm.GetUpdatedThenDeleted()

	if missingCountAfter != 0 {
		t.Errorf("expected updatedThenDeletedSinceLastStats to reset to 0, got %d", missingCountAfter)
	}
	if sm.GetSequentialRetries() != 0 {
		t.Errorf("expected sequential retries to reset to 0, got %d", sm.GetSequentialRetries())
	}
	if sm.GetOrderedBulkWrites() != 0 {
		t.Errorf("expected ordered bulk writes to reset to 0, got %d", sm.GetOrderedBulkWrites())
	}
	if sm.GetOrderedBulkWritesSize() != 0 {
		t.Errorf("expected ordered bulk writes size to reset to 0, got %d", sm.GetOrderedBulkWritesSize())
	}
	if sm.GetUnorderedBulkWrites() != 0 {
		t.Errorf("expected unordered bulk writes to reset to 0, got %d", sm.GetUnorderedBulkWrites())
	}
	if sm.GetUnorderedBulkWritesSize() != 0 {
		t.Errorf("expected unordered bulk writes size to reset to 0, got %d", sm.GetUnorderedBulkWritesSize())
	}
	if sm.GetTimeoutFlushes() != 0 {
		t.Errorf("expected timeout flushes to reset to 0, got %d", sm.GetTimeoutFlushes())
	}
	if sm.GetDuplicateKeys() != 0 {
		t.Errorf("expected duplicate keys to reset to 0, got %d", sm.GetDuplicateKeys())
	}
	if sm.GetWorkerReceivedCount("insert") != 0 || sm.GetWorkerReceivedCount("update") != 0 || sm.GetWorkerReceivedCount("delete") != 0 {
		t.Errorf("expected workerReceived counters to reset to 0")
	}
	if sm.GetProcessedCount("insert") != 0 || sm.GetProcessedCount("update") != 0 || sm.GetProcessedCount("delete") != 0 {
		t.Errorf("expected processed counters to reset to 0")
	}
	if sm.GetFailedCount("insert") != 0 || sm.GetFailedCount("update") != 0 || sm.GetFailedCount("delete") != 0 {
		t.Errorf("expected failed counters to reset to 0")
	}
}

func TestIncrementalStatsManagerLatencies(t *testing.T) {
	log := logger.New()
	sm := NewIncrementalStatsManager(log, 5*time.Minute)

	// Test RecordLatency for insert
	now := time.Now()
	// For ops1: ReadTime = now - 150ms, WorkerReceiveTime = now - 100ms (transit lag: 50ms, buffering lag: 100ms)
	ops1 := []WriteOperation{
		{ReadTime: now.Add(-150 * time.Millisecond), WorkerReceiveTime: now.Add(-100 * time.Millisecond), SuccessTime: now},
		{ReadTime: now.Add(-120 * time.Millisecond), WorkerReceiveTime: now.Add(-50 * time.Millisecond), SuccessTime: now},
	}
	// For ops2: ReadTime = now - 350ms, WorkerReceiveTime = now - 200ms (transit lag: 150ms, buffering lag: 200ms)
	ops2 := []WriteOperation{
		{ReadTime: now.Add(-350 * time.Millisecond), WorkerReceiveTime: now.Add(-200 * time.Millisecond), SuccessTime: now},
		{ReadTime: now.Add(-300 * time.Millisecond), WorkerReceiveTime: now.Add(-150 * time.Millisecond), SuccessTime: now},
	}

	sm.RecordLatency("insert", ops1, 50*time.Millisecond, 0, true)
	sm.RecordLatency("insert", ops2, 150*time.Millisecond, 0, true)
	
	sm.RecordLags(ops1)
	sm.RecordLags(ops2)

	lagRes := sm.lagTracker.Flush()

	// Expected average transit: (50ms + 150ms) / 2 = 100ms
	if lagRes.ReadToWorkerReceiveLag < 90*time.Millisecond || lagRes.ReadToWorkerReceiveLag > 110*time.Millisecond {
		t.Errorf("expected average transit latency around 100ms, got %v", lagRes.ReadToWorkerReceiveLag)
	}

	// Expected average buffering: (100ms + 50ms + 200ms + 150ms) / 4 = 125ms
	if lagRes.ReceiveToApplyLag < 115*time.Millisecond || lagRes.ReceiveToApplyLag > 135*time.Millisecond {
		t.Errorf("expected average buffering latency around 125ms, got %v", lagRes.ReceiveToApplyLag)
	}

	avgBulkOpInsert := sm.GetAvgBulkWriteLatency("insert")
	// Expected average bulk op latency: (50ms + 150ms) / 2 = 100ms
	if avgBulkOpInsert < 90*time.Millisecond || avgBulkOpInsert > 110*time.Millisecond {
		t.Errorf("expected average insert bulkwrite latency around 100ms, got %v", avgBulkOpInsert)
	}

	// Test RecordLatency for update
	ops3 := []WriteOperation{
		{ReadTime: now.Add(-500 * time.Millisecond), WorkerReceiveTime: now.Add(-400 * time.Millisecond), SuccessTime: now},
	}
	sm.RecordLatency("update", ops3, 300*time.Millisecond, 0, true)
	sm.RecordLags(ops3)

	lagResUpdate := sm.lagTracker.Flush()

	if lagResUpdate.ReadToWorkerReceiveLag != 100*time.Millisecond {
		t.Errorf("expected update transit latency 100ms, got %v", lagResUpdate.ReadToWorkerReceiveLag)
	}

	if lagResUpdate.ReceiveToApplyLag != 400*time.Millisecond {
		t.Errorf("expected update buffering latency 400ms, got %v", lagResUpdate.ReceiveToApplyLag)
	}

	avgBulkOpUpdate := sm.GetAvgBulkWriteLatency("update")
	if avgBulkOpUpdate != 300*time.Millisecond {
		t.Errorf("expected update bulkwrite latency 300ms, got %v", avgBulkOpUpdate)
	}

	// Test group flushes by reason metrics
	if sm.GetGroupFlushReasonCount("optype") != 0 {
		t.Errorf("expected initial optype flushes count to be 0, got %d", sm.GetGroupFlushReasonCount("optype"))
	}
	sm.IncrementGroupFlushReason("optype")
	sm.IncrementGroupFlushReason("optype")
	sm.IncrementGroupFlushReason("batchfull")
	if sm.GetGroupFlushReasonCount("optype") != 2 {
		t.Errorf("expected optype flushes count to be 2, got %d", sm.GetGroupFlushReasonCount("optype"))
	}
	if sm.GetGroupFlushReasonCount("batchfull") != 1 {
		t.Errorf("expected batchfull flushes count to be 1, got %d", sm.GetGroupFlushReasonCount("batchfull"))
	}

	// Verify ReportStats resets them
	sm.ReportStats()

	if sm.GetAvgBulkWriteLatency("insert") != 0 {
		t.Errorf("expected avg insert bulkwrite latency to reset to 0, got %v", sm.GetAvgBulkWriteLatency("insert"))
	}
	if sm.GetGroupFlushReasonCount("optype") != 0 {
		t.Errorf("expected optype flushes count to reset to 0, got %d", sm.GetGroupFlushReasonCount("optype"))
	}
	if sm.GetGroupFlushReasonCount("batchfull") != 0 {
		t.Errorf("expected batchfull flushes count to reset to 0, got %d", sm.GetGroupFlushReasonCount("batchfull"))
	}
}

// BenchmarkIncrementalStatsManagerRecordLatency measures stats gathering hot-path latency under high concurrent load
func BenchmarkIncrementalStatsManagerRecordLatency(b *testing.B) {
	log := logger.New()
	log.SetLevel("error")
	sm := NewIncrementalStatsManager(log, 5*time.Minute)
	now := time.Now()
	ops := []WriteOperation{{ReadTime: now.Add(-20 * time.Millisecond), WorkerReceiveTime: now.Add(-10 * time.Millisecond), OpType: "mixed"}}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.RecordLatency("mixed", ops, 5*time.Millisecond, i%8, true)
			i++
		}
	})
}

// BenchmarkIncrementalStatsManagerIncrementWorkerReceived measures simple counter increment performance under high concurrent load
func BenchmarkIncrementalStatsManagerIncrementWorkerReceived(b *testing.B) {
	log := logger.New()
	log.SetLevel("error")
	sm := NewIncrementalStatsManager(log, 5*time.Minute)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sm.IncrementEventsWorkerReceived("insert")
		}
	})
}

// TestIncrementalStatsManagerDLQAndFailureBreakdownNormalization verifies that connection failure strings are normalized and DLQ metrics increments correctly.
func TestIncrementalStatsManagerDLQAndFailureBreakdownNormalization(t *testing.T) {
	log := logger.New()
	sm := NewIncrementalStatsManager(log, 5*time.Minute)

	err1 := fmt.Errorf("connection(5bc95d74-1eb9-46fa-9445-39e5b4514544.us-east4.firestore.goog:443[-27608]) socket was unexpectedly closed: EOF")
	err2 := fmt.Errorf("connection(other-conn.us-east4.firestore.goog:443[-28000]) socket was unexpectedly closed: EOF")
	err3 := fmt.Errorf("Document already exists")

	// Act: record failures
	sm.IncrementEventsFailed("insert", true, err1)
	sm.IncrementEventsFailed("insert", true, err2)
	sm.IncrementEventsFailed("update", false, err3)

	// Assert
	sm.failureMu.Lock()
	dlqCountInsert := sm.GetDLQCount("insert")
	dlqCountUpdate := sm.GetDLQCount("update")
	breakdownInsertEOF := sm.failureBreakdown["insert"]["connection(...) socket was unexpectedly closed: EOF"]
	breakdownUpdateDup := sm.failureBreakdown["update"]["Document already exists"]
	sm.failureMu.Unlock()

	if dlqCountInsert != 2 {
		t.Errorf("expected DLQ'ed inserts 2, got %d", dlqCountInsert)
	}
	if dlqCountUpdate != 0 {
		t.Errorf("expected DLQ'ed updates 0, got %d", dlqCountUpdate)
	}
	if breakdownInsertEOF != 2 {
		t.Errorf("expected normalized and grouped insert EOF count 2, got %d", breakdownInsertEOF)
	}
	if breakdownUpdateDup != 1 {
		t.Errorf("expected update duplicate count 1, got %d", breakdownUpdateDup)
	}
}

// TestIncrementalStatsManagerDLQAndFailureBreakdownReportStatsReset verifies that ReportStats resets all internal failure metrics state.
func TestIncrementalStatsManagerDLQAndFailureBreakdownReportStatsReset(t *testing.T) {
	log := logger.New()
	sm := NewIncrementalStatsManager(log, 5*time.Minute)

	err1 := fmt.Errorf("connection(5bc95d74-1eb9-46fa-9445-39e5b4514544.us-east4.firestore.goog:443[-27608]) socket was unexpectedly closed: EOF")
	sm.IncrementEventsFailed("insert", true, err1)

	// Act: run telemetry report reset
	sm.ReportStats()

	// Assert: verify cleared/reset states
	sm.failureMu.Lock()
	if sm.GetDLQCount("insert") != 0 || len(sm.failureBreakdown) != 0 {
		t.Errorf("expected DLQ stats and failure breakdown to reset after ReportStats")
	}
	sm.failureMu.Unlock()
}

// TestIncrementalStatsManagerPartitionReadMetrics validates that the IncrementalStatsManager correctly records, grows, and snapshots performance telemetry for individual stream partitions.
func TestIncrementalStatsManagerPartitionReadMetrics(t *testing.T) {
	log := logger.New()
	sm := NewIncrementalStatsManager(log, 5*time.Minute)

	// Act 1: Record read metrics for Partition 0
	sm.RecordReadMetric(0, 10*time.Millisecond, 1024)
	sm.RecordReadMetric(0, 20*time.Millisecond, 2048)

	// Act 2: Record read metrics for Partition 2 (should trigger dynamic grow checks)
	sm.RecordReadMetric(2, 5*time.Millisecond, 512)

	// Assert 1: Verify the dynamic slice has grown to accommodate partition index 2 (length 3)
	p0 := sm.partitionReadStats[0]
	p1 := sm.partitionReadStats[1]
	p2 := sm.partitionReadStats[2]

	// Partition 0 stats assertions
	if p0.ReadCount != 2 {
		t.Errorf("expected Partition 0 read count 2, got %d", p0.ReadCount)
	}
	if p0.TotalReadSizeBytes != 3072 {
		t.Errorf("expected Partition 0 total size 3072, got %d", p0.TotalReadSizeBytes)
	}

	// Partition 1 stats assertions (should be initialized to zero fields)
	if p1.ReadCount != 0 {
		t.Errorf("expected Partition 1 read count 0, got %d", p1.ReadCount)
	}

	// Partition 2 stats assertions
	if p2.ReadCount != 1 {
		t.Errorf("expected Partition 2 read count 1, got %d", p2.ReadCount)
	}
	if p2.TotalReadSizeBytes != 512 {
		t.Errorf("expected Partition 2 total size 512, got %d", p2.TotalReadSizeBytes)
	}

	// Assert 2: Verify legacy/global backward compatibility counter values are updated
	globalReadCount := atomic.LoadInt64(&sm.readCount)
	globalSize := atomic.LoadInt64(&sm.totalReadSizeBytes)
	if globalReadCount != 3 {
		t.Errorf("expected legacy global read count to be 3, got %d", globalReadCount)
	}
	if globalSize != 3584 {
		t.Errorf("expected legacy global size to be 3584, got %d", globalSize)
	}

	// Act 3: Call ReportStats and check that partition stats are reported and reset
	sm.ReportStats()

	// Assert 3: Verification post reset
	p0ResetCount := atomic.LoadInt64(&sm.partitionReadStats[0].ReadCount)
	p2ResetCount := atomic.LoadInt64(&sm.partitionReadStats[2].ReadCount)

	if p0ResetCount != 0 {
		t.Errorf("expected Partition 0 read count to be reset to 0, got %d", p0ResetCount)
	}
	if p2ResetCount != 0 {
		t.Errorf("expected Partition 2 read count to be reset to 0, got %d", p2ResetCount)
	}
}

// TestIncrementalStatsManagerQueueObservability verifies queue timing, stalling, and snapshot registration interfaces
func TestIncrementalStatsManagerQueueObservability(t *testing.T) {
	log := logger.New()
	sm := NewIncrementalStatsManager(log, 5*time.Minute)

	// Test recording delays and stalls
	sm.RecordBatchingQueueStall(15 * time.Millisecond)
	sm.RecordBatchWriteQueueStall(35 * time.Millisecond)
	sm.RecordQueueDelays(5*time.Millisecond, 12*time.Millisecond)

	// Register empty/nil channels to verify no panics
	sm.RegisterQueues(nil, nil)

	// Read and reset stats
	batchingStall := atomic.LoadInt64(&sm.batchingQueueStallNs)
	batchWriteStall := atomic.LoadInt64(&sm.batchWriteQueueStallNs)
	ingestDelay := atomic.LoadInt64(&sm.ingestQueueWaitNs)
	batchingDelay := atomic.LoadInt64(&sm.batchingQueueWaitNs)

	if batchingStall != int64(15*time.Millisecond) {
		t.Errorf("expected batching queue stall 15ms, got %v", time.Duration(batchingStall))
	}
	if batchWriteStall != int64(35*time.Millisecond) {
		t.Errorf("expected batch write queue stall 35ms, got %v", time.Duration(batchWriteStall))
	}
	if ingestDelay != int64(5*time.Millisecond) {
		t.Errorf("expected ingest queue wait delay 5ms, got %v", time.Duration(ingestDelay))
	}
	if batchingDelay != int64(12*time.Millisecond) {
		t.Errorf("expected batching queue wait delay 12ms, got %v", time.Duration(batchingDelay))
	}

	// Trigger ReportStats to verify formatting runs without panic and resets fields
	sm.ReportStats()

	if atomic.LoadInt64(&sm.batchingQueueStallNs) != 0 {
		t.Errorf("expected batchingQueueStallNs to be reset to 0")
	}
}

// TestCalculatePercentilesFromHistogram verifies the generic histogram percentile calculation helper
func TestCalculatePercentilesFromHistogram(t *testing.T) {
	// Simple linear histogram with counts: index 0: 10, index 1: 20, index 2: 70 (total = 100)
	histogram := []int64{10, 20, 70}
	
	// Thresholds: p50 (0.50), p90 (0.90), p100 (1.00)
	res := calculatePercentilesFromHistogram(histogram, []float64{0.50, 0.90, 1.00})
	
	if res[0] != 2 || res[1] != 2 || res[2] != 2 {
		t.Errorf("expected [2, 2, 2], got %v", res)
	}

	// Another histogram where bins partition thresholds cleanly:
	// index 0: 50, index 1: 40, index 2: 10 (total = 100)
	histogram2 := []int64{50, 40, 10}
	res2 := calculatePercentilesFromHistogram(histogram2, []float64{0.50, 0.90, 1.00})
	if res2[0] != 0 || res2[1] != 1 || res2[2] != 2 {
		t.Errorf("expected [0, 1, 2], got %v", res2)
	}
}

func TestIncrementalStatsManagerSkippedEvents(t *testing.T) {
	log := logger.New()
	sm := NewIncrementalStatsManager(log, 0, false)

	// Record some skipped events by type
	sm.RecordSkippedEvent("dropIndexes")
	sm.RecordSkippedEvent("dropIndexes")
	sm.RecordSkippedEvent("rename")
	sm.RecordSkippedEvent("update-doc-missing")

	// Verify counts are correct
	if sm.GetSkippedEventsCount("dropIndexes") != 2 {
		t.Errorf("expected 2 skipped dropIndexes, got %d", sm.GetSkippedEventsCount("dropIndexes"))
	}
	if sm.GetSkippedEventsCount("rename") != 1 {
		t.Errorf("expected 1 skipped rename, got %d", sm.GetSkippedEventsCount("rename"))
	}
	if sm.GetSkippedEventsCount("update-doc-missing") != 1 {
		t.Errorf("expected 1 skipped update-doc-missing, got %d", sm.GetSkippedEventsCount("update-doc-missing"))
	}
	if sm.GetSkippedEventsCount("non-existent") != 0 {
		t.Errorf("expected 0 skipped non-existent, got %d", sm.GetSkippedEventsCount("non-existent"))
	}

	// Verify that ReportStats resets the interval skipped counts
	sm.skippedEventsMu.Lock()
	intervalSkipped := sm.skippedEvents
	sm.skippedEvents = make(map[string]int64)
	sm.skippedEventsMu.Unlock()

	if intervalSkipped["dropIndexes"] != 2 || intervalSkipped["rename"] != 1 || intervalSkipped["update-doc-missing"] != 1 {
		t.Errorf("expected correct interval skipped values, got %+v", intervalSkipped)
	}

	// Verify they are reset to 0 in active tracker
	if sm.GetSkippedEventsCount("dropIndexes") != 0 {
		t.Errorf("expected 0 skipped dropIndexes after reset, got %d", sm.GetSkippedEventsCount("dropIndexes"))
	}
}

func TestIncrementalStatsManagerDryRunLag(t *testing.T) {
	log := logger.New()
	sm := NewIncrementalStatsManager(log, 5*time.Minute)
	sm.DryRun = true

	now := time.Now()
	// Simulate dry-run lag recording
	sm.RecordDryRunLag(now.Add(-100*time.Millisecond), now)
	sm.RecordDryRunLag(now.Add(-200*time.Millisecond), now)

	// Simulate next latency recordings
	sm.RecordReadMetric(0, 10*time.Millisecond, 1024)
	sm.RecordReadMetric(0, 100*time.Millisecond, 2048)

	// Call ReportDryRunStats to output the stats and flush
	sm.ReportDryRunStats()

	// Verify that lag tracker is flushed/reset
	sm.mu.Lock()
	res := sm.lagTracker.Flush()
	sm.mu.Unlock()
	if res.EventToReadLag != 0 {
		t.Errorf("expected EventToReadLag to be reset to 0, got %v", res.EventToReadLag)
	}

	// Verify that readNextLatenciesHistogram is reset to 0
	for i, count := range sm.readNextLatenciesHistogram {
		if count != 0 {
			t.Errorf("expected readNextLatenciesHistogram[%d] to be reset to 0, got %d", i, count)
		}
	}

	// Verify that readSizesHistogram is reset to 0
	for i, count := range sm.readSizesHistogram {
		if count != 0 {
			t.Errorf("expected readSizesHistogram[%d] to be reset to 0, got %d", i, count)
		}
	}
}

type mockDLQ struct {
	count int64
}

func (m *mockDLQ) WriteFailed(sourceDB, sourceCollection string, documentID interface{}, err error, phase, opType string, document interface{}, eventTime time.Time) {}
func (m *mockDLQ) WriteResolved(sourceDB, sourceCollection string, documentID interface{}, phase string, eventTime time.Time) {}
func (m *mockDLQ) Count() int64 { return atomic.LoadInt64(&m.count) }
func (m *mockDLQ) FilePath() string { return "" }
func (m *mockDLQ) Close() {}

func TestIncrementalStatsManagerDLQWarning(t *testing.T) {
	log := logger.New()
	var logBuf bytes.Buffer
	log.Logger.SetOutput(&logBuf)

	sm := NewIncrementalStatsManager(log, 5*time.Minute)

	// 1. DLQ is nil -> no warning
	sm.ReportStats()
	if strings.Contains(logBuf.String(), "DLQ WARNING") {
		t.Errorf("did not expect DLQ WARNING with nil DLQ, got log: %s", logBuf.String())
	}
	logBuf.Reset()

	// 2. DLQ count <= 100 -> no warning
	mock := &mockDLQ{count: 100}
	sm.SetDLQ(mock)
	sm.ReportStats()
	if strings.Contains(logBuf.String(), "DLQ WARNING") {
		t.Errorf("did not expect DLQ WARNING with count 100, got log: %s", logBuf.String())
	}
	logBuf.Reset()

	// 3. DLQ count > 100 -> warning
	atomic.StoreInt64(&mock.count, 101)
	sm.ReportStats()
	if !strings.Contains(logBuf.String(), "DLQ WARNING") {
		t.Errorf("expected DLQ WARNING with count 101, got log: %s", logBuf.String())
	}
}



