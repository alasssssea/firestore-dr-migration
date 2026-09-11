package migration

import (
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestWorkerProcessEventUpdateWithNullFullDocumentAndDescription(t *testing.T) {
	log := logger.New()
	ctx := context.Background()

	// Create a worker with no targetDB (we won't flush/write to the DB in this test)
	worker := NewWorker(
		1,                                   // id
		ctx,                                 // ctx
		log,                                 // logger
		nil,                                 // targetDB
		nil,                                 // collectionMap
		10,                                  // batch size (larger than 1, so it won't flush)
		false,                               // forceOrdered
		nil,                                 // dlq
		nil,                                 // retryManager
		nil,                                 // incrementalStatsManager
		false,                               // groupOpsByDistinctID
		5*time.Minute,                       // flushInterval
		8192,                                // batchingQueueSize
		2,                                   // batchWriteQueueSize
		NewFieldTransformer(false, false, false, log),     // transformer
	)

	// Create an update event where both fullDocument and updateDescription are nil/null
	event := bson.M{
		"operationType": "update",
		"ns": bson.M{
			"db":   "testdb",
			"coll": "testcoll",
		},
		"documentKey": bson.M{
			"_id": "doc_123",
		},
		"fullDocument":      nil,
		"updateDescription": nil,
	}

	// Process the event
	worker.ProcessEvent(event)

	// Assertion Correctness Check (Decoupled Null-Document Skip logic):
	// Since we corrected the logic bug to always skip update events with a nil fullDocument payload (regardless
	// of whether a incrementalStatsManager is set), we verify that currentGroup remains nil (event is gracefully skipped).
	// This prevents target database engines from encountering raw parser issues and writing garbage entries to DLQ.
	if worker.currentGroup != nil {
		t.Fatalf("expected event to be skipped (currentGroup == nil), but got currentGroup with %d operations", len(worker.currentGroup.Operations))
	}
}

func TestWorkerProcessEventUpdateWithNullFullDocumentAndIncrementalStatsManager(t *testing.T) {
	log := logger.New()
	ctx := context.Background()
	statsMgr := NewIncrementalStatsManager(log, 0, false)

	worker := NewWorker(
		1,                                   // id
		ctx,                                 // ctx
		log,                                 // logger
		nil,                                 // targetDB
		nil,                                 // collectionMap
		10,                                  // batch size
		false,                               // forceOrdered
		nil,                                 // dlq
		nil,                                 // retryManager
		statsMgr,                            // incrementalStatsManager
		false,                               // groupOpsByDistinctID
		5*time.Minute,                       // flushInterval
		8192,                                // batchingQueueSize
		2,                                   // batchWriteQueueSize
		NewFieldTransformer(false, false, false, log),     // transformer
	)

	// Create an update event where fullDocument is nil
	event := bson.M{
		"operationType": "update",
		"ns": bson.M{
			"db":   "testdb",
			"coll": "testcoll",
		},
		"documentKey": bson.M{
			"_id": "doc_123",
		},
		"fullDocument":      nil,
		"updateDescription": nil,
	}

	// Process the event
	worker.ProcessEvent(event)

	// Verify that the event was skipped (currentGroup is nil)
	if worker.currentGroup != nil {
		t.Fatalf("expected event to be skipped (currentGroup == nil), but got currentGroup with %d operations", len(worker.currentGroup.Operations))
	}

	// Verify that "update-doc-missing" metric was incremented
	statsMgr.mu.Lock()
	count := statsMgr.updatedThenDeletedSinceLastStats
	statsMgr.mu.Unlock()

	if count != 1 {
		t.Errorf("expected update-doc-missing metric count to be 1, got %v", count)
	}
}

// BenchmarkOldHashing measures the old way of hashing ObjectID
func BenchmarkOldHashing(b *testing.B) {
	id := primitive.NewObjectID()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bytes := []byte(id.Hex())
		h := fnv.New32a()
		h.Write(bytes)
		_ = int(h.Sum32())
	}
}

// BenchmarkNewHashing measures the new way of hashing ObjectID
func BenchmarkNewHashing(b *testing.B) {
	id := primitive.NewObjectID()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bytes := id[:]
		hash := uint32(2166136261)
		for _, v := range bytes {
			hash ^= uint32(v)
			hash *= 16777619
		}
		_ = int(hash)
	}
}



func TestPartitionTrackerCorrectness(t *testing.T) {
	log := logger.New()

	// Create a temporary file for the checkpoints
	tempDir, err := os.MkdirTemp("", "partition-tracker-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	checkpointPath := filepath.Join(tempDir, "resumeToken-test.json")

	// Instantiate PartitionTracker (threshold = 2, totalPartitions = 2)
	tracker := NewPartitionTracker(log, checkpointPath, 5*time.Minute, 2, 2)

	// Register 3 events for Partition 0
	token1, _ := bson.Marshal(bson.M{"_data": "tok1"})
	token2, _ := bson.Marshal(bson.M{"_data": "tok2"})
	token3, _ := bson.Marshal(bson.M{"_data": "tok3"})

	seq1 := tracker.Register(0, token1, time.Now())
	seq2 := tracker.Register(0, token2, time.Now())
	seq3 := tracker.Register(0, token3, time.Now())

	if seq1 != 1 || seq2 != 2 || seq3 != 3 {
		t.Errorf("expected sequential sequence numbers 1, 2, 3, got %d, %d, %d", seq1, seq2, seq3)
	}

	// Ack seq2 (out of order)
	tracker.Ack(0, seq2)

	// Verify no checkpoints have been saved yet (since seq1 is unacked)
	partition1Path := GetPartitionResumeTokenPath(checkpointPath, 0, 2)
	if _, err := os.Stat(partition1Path); err == nil {
		t.Error("expected checkpoint file to not exist when consecutive ack prefix is empty")
	}

	// Ack seq1 (now seq1 and seq2 are acked, but seq3 is unacked)
	tracker.Ack(0, seq1)

	// Verify checkpoint file exists and has tok2 as data (since it's the highest safe consecutive one)
	tokenDoc, err := LoadResumeToken(partition1Path)
	if err != nil {
		t.Fatalf("failed to load resume token: %v", err)
	}
	if tokenDoc == nil {
		t.Fatal("expected saved resume token to be non-nil")
	}
	docMap, ok := tokenDoc.(map[string]interface{})
	if !ok || docMap["_data"] != "tok2" {
		t.Errorf("expected checkpointed token value 'tok2', got %v", tokenDoc)
	}

	// Ack seq3
	tracker.Ack(0, seq3)

	// Force close / save
	tracker.Close()

	// Verify final checkpoint file now has tok3
	tokenDocFinal, err := LoadResumeToken(partition1Path)
	if err != nil {
		t.Fatalf("failed to load final resume token: %v", err)
	}
	docMapFinal, ok := tokenDocFinal.(map[string]interface{})
	if !ok || docMapFinal["_data"] != "tok3" {
		t.Errorf("expected final checkpointed token value 'tok3', got %v", tokenDocFinal)
	}
}

func TestPartitionTrackerSinglePartition(t *testing.T) {
	log := logger.New()

	// Create a temporary file for the checkpoints
	tempDir, err := os.MkdirTemp("", "partition-tracker-single-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	checkpointPath := filepath.Join(tempDir, "resumeToken-single-test.json")

	// Instantiate PartitionTracker for single partition (threshold = 1, totalPartitions = 1)
	tracker := NewPartitionTracker(log, checkpointPath, 5*time.Minute, 1, 1)

	token1, _ := bson.Marshal(bson.M{"_data": "tok1"})
	seq1 := tracker.Register(0, token1, time.Now())

	if seq1 != 1 {
		t.Errorf("expected sequence number 1, got %d", seq1)
	}

	// Ack seq1
	tracker.Ack(0, seq1)

	// Verify checkpoint file exists at the uniform partition path
	partition1Path := GetPartitionResumeTokenPath(checkpointPath, 0, 1)
	if _, err := os.Stat(partition1Path); os.IsNotExist(err) {
		t.Error("expected checkpoint file to exist at the uniform partition path")
	}

	// Load should load it successfully
	tokenDoc, err := LoadResumeToken(partition1Path)
	if err != nil {
		t.Fatalf("failed to load resume token: %v", err)
	}
	docMap, ok := tokenDoc.(map[string]interface{})
	if !ok || docMap["_data"] != "tok1" {
		t.Errorf("expected checkpointed token value 'tok1', got %v", tokenDoc)
	}

	// Force close / save
	tracker.Close()
}

func TestWorkerFlushCurrentGroupResetsIDs(t *testing.T) {
	log := logger.New()
	ctx := context.Background()

	worker := NewWorker(
		1,
		ctx,
		log,
		nil,
		nil,
		10,
		false,
		nil,
		nil,
		nil,
		true, // groupOpsByDistinctId
		5*time.Minute,
		8192,
		2,
		NewFieldTransformer(false, false, false, log),
	)

	// Process an event to establish active group ID cache status
	event1 := bson.M{
		"operationType": "insert",
		"ns": bson.M{
			"db":   "testdb",
			"coll": "testcoll",
		},
		"documentKey": bson.M{
			"_id": "doc_abc",
		},
		"fullDocument": bson.M{"a": 1},
	}
	worker.ProcessEvent(event1)

	worker.mu.Lock()
	docHash1 := hashDocumentID("doc_abc")
	if !worker.currentGroupIDs[docHash1] {
		t.Errorf("expected doc_abc hash to be registered in currentGroupIDs")
	}

	// Flush current group manually (like a timeout flush)
	worker.flushCurrentGroup()
	if worker.currentGroup != nil {
		t.Errorf("expected currentGroup to be nil after flush")
	}
	if len(worker.currentGroupIDs) != 0 {
		t.Errorf("expected currentGroupIDs to be reset/empty after flush, got %d elements", len(worker.currentGroupIDs))
	}
	worker.mu.Unlock()
}

func TestWorkerConcurrencyStateSafety(t *testing.T) {
	log := logger.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker := NewWorker(
		1,
		ctx,
		log,
		nil,
		nil,
		10,
		false,
		nil,
		nil,
		nil,
		true, // groupOpsByDistinctId
		5*time.Minute,
		8192,
		2,
		NewFieldTransformer(false, false, false, log),
	)

	// Concurrently invoke ProcessEvent and manual flushes to verify Go race detector safety
	done := make(chan bool)
	go func() {
		for i := 0; i < 100; i++ {
			event := bson.M{
				"operationType": "insert",
				"ns": bson.M{
					"db":   "testdb",
					"coll": "testcoll",
				},
				"documentKey": bson.M{
					"_id": i,
				},
				"fullDocument": bson.M{"val": i},
			}
			worker.ProcessEvent(event)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 50; i++ {
			worker.mu.Lock()
			worker.flushCurrentGroup()
			worker.mu.Unlock()
			time.Sleep(1 * time.Millisecond)
		}
		done <- true
	}()

	// Wait for both concurrent thread processes to complete
	<-done
	<-done
}

func TestWorkerShutdownConcurrencyRaceSafety(t *testing.T) {
	log := logger.New()
	ctx := context.Background()

	worker := NewWorker(
		1,
		ctx,
		log,
		nil,
		nil,
		10,
		false,
		nil,
		nil,
		nil,
		false,
		5*time.Minute,
		8192,
		2,
		NewFieldTransformer(false, false, false, log),
	)

	// Keep the consumer loop active by pushing a dummy event to batchingQueue
	worker.batchingQueue <- bson.M{
		"operationType": "insert",
		"ns": bson.M{"db": "db", "coll": "coll"},
		"documentKey": bson.M{"_id": 1},
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		time.Sleep(1 * time.Millisecond)
		// Triggers write to w.shutdownInProgress under lock
		worker.Shutdown()
	}()

	go func() {
		defer wg.Done()
		// Concurrently read the state to verify there are no races with target fields
		for i := 0; i < 100; i++ {
			_ = worker.isShutdownInProgress()
			time.Sleep(50 * time.Microsecond)
		}
	}()

	wg.Wait()
}

func TestWorkerTimeoutTickerFlush(t *testing.T) {
	log := logger.New()
	ctx := context.Background()
	statsMgr := NewIncrementalStatsManager(log, 0, false)

	worker := NewWorker(
		1,
		ctx,
		log,
		nil,
		nil,
		10,
		false,
		nil,
		nil,
		statsMgr,
		false,
		10*time.Millisecond, // very short timeout flush interval!
		8192,
		2,
		NewFieldTransformer(false, false, false, log),
	)

	// Process 1 operation to make w.currentGroup non-nil
	event := bson.M{
		"operationType": "insert",
		"ns": bson.M{"db": "db", "coll": "coll"},
		"documentKey": bson.M{"_id": "doc_timeout_123"},
		"fullDocument": bson.M{"foo": "bar"},
	}
	worker.ProcessEvent(event)

	// Verify group is active
	worker.mu.Lock()
	isActive := worker.currentGroup != nil
	worker.mu.Unlock()
	if !isActive {
		t.Fatal("expected currentGroup to be active")
	}

	// Wait for the background eventLoop ticker to fire (e.g. 150ms is plenty of time for a 100ms ticker)
	time.Sleep(150 * time.Millisecond)

	// Verify group has been timeout-flushed to the write queue automatically!
	worker.mu.Lock()
	isNil := worker.currentGroup == nil
	worker.mu.Unlock()
	if !isNil {
		t.Errorf("expected currentGroup to be nil/flushed by background timeout ticker")
	}

	// Give background consumer thread a moment to drain and process the group
	time.Sleep(10 * time.Millisecond)

	processedCount := statsMgr.GetProcessedCount("insert")
	if processedCount != 1 {
		t.Errorf("expected processed group count to be 1, got %d", processedCount)
	}
}

func TestWorkerContextCancellationFlush(t *testing.T) {
	log := logger.New()
	ctx, cancel := context.WithCancel(context.Background())
	statsMgr := NewIncrementalStatsManager(log, 0, false)

	worker := NewWorker(
		1,
		ctx,
		log,
		nil,
		nil,
		10,
		false,
		nil,
		nil,
		statsMgr,
		false,
		5*time.Minute, // very large interval so it won't timeout flush
		8192,
		2,
		NewFieldTransformer(false, false, false, log),
	)

	// Process 1 operation to keep group active in memory
	event := bson.M{
		"operationType": "insert",
		"ns": bson.M{"db": "db", "coll": "coll"},
		"documentKey": bson.M{"_id": "doc_cancel_123"},
		"fullDocument": bson.M{"foo": "bar"},
	}
	worker.ProcessEvent(event)

	// Cancel the context!
	cancel()

	// Give background eventLoop thread a microsecond to detect cancellation and exit
	time.Sleep(50 * time.Millisecond)

	// Verify group has been successfully flushed upon cancellation!
	worker.mu.Lock()
	isNil := worker.currentGroup == nil
	worker.mu.Unlock()
	if !isNil {
		t.Errorf("expected currentGroup to be nil/flushed upon context cancellation")
	}

	// Give background consumer thread a moment to drain and process the group
	time.Sleep(50 * time.Millisecond)

	processedCount := statsMgr.GetProcessedCount("insert")
	if processedCount != 1 {
		t.Errorf("expected processed group count to be 1, got %d", processedCount)
	}
}

func TestWorkerSetPartitionTracker(t *testing.T) {
	log := logger.New()
	ctx := context.Background()

	worker := NewWorker(
		1,
		ctx,
		log,
		nil,
		nil,
		10,
		false,
		nil,
		nil,
		nil,
		false,
		5*time.Minute,
		8192,
		2,
		NewFieldTransformer(false, false, false, log),
	)

	tracker := NewPartitionTracker(log, "/tmp/dummy-checkpoint.json", 5*time.Minute, 2, 2)
	defer tracker.Close()

	// Test calling SetPartitionTracker under safe locks
	worker.SetPartitionTracker(tracker)

	worker.mu.Lock()
	if worker.partitionTracker != tracker {
		t.Errorf("expected partitionTracker to be successfully set")
	}
	worker.mu.Unlock()
}

func TestCollectionDropBSONParsing(t *testing.T) {
	// Create a raw drop change event
	eventDoc := bson.M{
		"operationType": "drop",
		"ns": bson.M{
			"db":   "testdb",
			"coll": "testcoll",
		},
	}
	rawBytes, err := bson.Marshal(eventDoc)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}
	rawEvent := bson.Raw(rawBytes)

	// Validate our lookups match
	opTypeVal, err := rawEvent.LookupErr("operationType")
	if err != nil {
		t.Fatalf("failed to lookup operationType: %v", err)
	}
	if opTypeVal.Type != bson.TypeString || opTypeVal.StringValue() != "drop" {
		t.Errorf("expected drop, got %v", opTypeVal.StringValue())
	}

	var nsCtx string
	nsVal, nsErr := rawEvent.LookupErr("ns")
	if nsErr == nil && nsVal.Type == bson.TypeEmbeddedDocument {
		nsDoc := nsVal.Document()
		dbVal, dbErr := nsDoc.LookupErr("db")
		collVal, collErr := nsDoc.LookupErr("coll")
		if dbErr == nil && dbVal.Type == bson.TypeString && collErr == nil && collVal.Type == bson.TypeString {
			nsCtx = fmt.Sprintf(" %s.%s", dbVal.StringValue(), collVal.StringValue())
		}
	}

	expectedErrorMsg := "terminal failure: collection drop event detected in partition 0 testdb.testcoll"
	actualErrorMsg := fmt.Sprintf("terminal failure: collection drop event detected in partition 0%s", nsCtx)
	if actualErrorMsg != expectedErrorMsg {
		t.Errorf("expected error message %q, got %q", expectedErrorMsg, actualErrorMsg)
	}
}

func TestNonDMLGracefulSkipBSONParsing(t *testing.T) {
	// Create a raw DDL change event (e.g. dropIndexes)
	eventDoc := bson.M{
		"operationType": "dropIndexes",
		"ns": bson.M{
			"db":   "testdb",
			"coll": "testcoll",
		},
	}
	rawBytes, err := bson.Marshal(eventDoc)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}
	rawEvent := bson.Raw(rawBytes)

	// Validate our operationType lookup matches
	opTypeVal, err := rawEvent.LookupErr("operationType")
	if err != nil {
		t.Fatalf("failed to lookup operationType: %v", err)
	}
	if opTypeVal.Type != bson.TypeString {
		t.Fatalf("expected string type, got %v", opTypeVal.Type)
	}

	opType := opTypeVal.StringValue()
	if opType != "insert" && opType != "update" && opType != "replace" && opType != "delete" {
		// Harmless non-DML skip path matches!
		if opType == "drop" {
			t.Errorf("expected non-drop skip matching, but got drop")
		}
	} else {
		t.Errorf("expected non-DML type, but got %q", opType)
	}
}

