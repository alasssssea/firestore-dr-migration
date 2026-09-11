package migration

import (
	"bytes"
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestBackfillTracker_InOrderAcks(t *testing.T) {
	tmpDir := t.TempDir()
	cpPath := filepath.Join(tmpDir, "backfillCheckpoint-db-coll-partition-0-of-1.json")

	cp := &PartitionCheckpoint{
		Database:       "db",
		Collection:     "coll",
		PartitionIndex: 0,
		TotalSplits:    1,
		TypeProgress:   make(map[BSONType]*TypeRangeBoundary),
	}

	tracker := NewBackfillPartitionTracker(logger.New(), cp, cpPath, 1*time.Minute, 100)

	oid1 := primitive.NewObjectID()
	oid2 := primitive.NewObjectID()
	oid3 := primitive.NewObjectID()

	b1 := []interface{}{bson.D{{Key: "_id", Value: oid1}}}
	b2 := []interface{}{bson.D{{Key: "_id", Value: oid2}}}
	b3 := []interface{}{bson.D{{Key: "_id", Value: oid3}}}

	seq1 := tracker.RegisterBatch(b1)
	seq2 := tracker.RegisterBatch(b2)
	seq3 := tracker.RegisterBatch(b3)

	if seq1 != 1 || seq2 != 2 || seq3 != 3 {
		t.Fatalf("expected sequence numbers 1, 2, 3; got %d, %d, %d", seq1, seq2, seq3)
	}

	tracker.AckBatch(seq1, 1)
	tracker.AckBatch(seq2, 1)
	tracker.AckBatch(seq3, 1)

	tracker.Close()

	updatedCP := tracker.GetCheckpoint()
	if updatedCP.ApproximateDocsMigrated != 3 {
		t.Errorf("expected ApproximateDocsMigrated 3, got %d", updatedCP.ApproximateDocsMigrated)
	}
	if updatedCP.TypeProgress[BSONTypeObjectID] == nil || updatedCP.TypeProgress[BSONTypeObjectID].SavedLastID != oid3 {
		t.Errorf("expected SavedLastID %v, got %v", oid3, updatedCP.TypeProgress[BSONTypeObjectID].SavedLastID)
	}

	// Verify loaded from disk
	loadedCP, err := LoadPartitionCheckpoint(cpPath)
	if err != nil {
		t.Fatalf("failed to load checkpoint from disk: %v", err)
	}
	if loadedCP.ApproximateDocsMigrated != 3 {
		t.Errorf("expected loaded ApproximateDocsMigrated 3, got %d", loadedCP.ApproximateDocsMigrated)
	}
}

func TestBackfillTracker_OutOfOrderAcks(t *testing.T) {
	tmpDir := t.TempDir()
	cpPath := filepath.Join(tmpDir, "backfillCheckpoint-db-coll-partition-0-of-1.json")

	cp := &PartitionCheckpoint{
		Database:       "db",
		Collection:     "coll",
		PartitionIndex: 0,
		TotalSplits:    1,
		TypeProgress:   make(map[BSONType]*TypeRangeBoundary),
	}

	tracker := NewBackfillPartitionTracker(logger.New(), cp, cpPath, 1*time.Minute, 100)

	oid1 := primitive.NewObjectID()
	oid2 := primitive.NewObjectID()
	oid3 := primitive.NewObjectID()
	oid4 := primitive.NewObjectID()

	seq1 := tracker.RegisterBatch([]interface{}{bson.D{{Key: "_id", Value: oid1}}})
	seq2 := tracker.RegisterBatch([]interface{}{bson.D{{Key: "_id", Value: oid2}}})
	seq3 := tracker.RegisterBatch([]interface{}{bson.D{{Key: "_id", Value: oid3}}})
	seq4 := tracker.RegisterBatch([]interface{}{bson.D{{Key: "_id", Value: oid4}}})

	// Ack out of order: 2, 4, 3 (1 is still missing)
	tracker.AckBatch(seq2, 10)
	tracker.AckBatch(seq4, 10)
	tracker.AckBatch(seq3, 10)

	// Since seq1 is not acked, nothing should be committed yet
	tracker.mu.Lock()
	tracker.saveCheckpointsLocked()
	tracker.mu.Unlock()

	if cp.ApproximateDocsMigrated != 0 {
		t.Errorf("expected 0 migrated docs before seq 1 ack, got %d", cp.ApproximateDocsMigrated)
	}

	// Now ack seq1
	tracker.AckBatch(seq1, 10)

	tracker.Close()

	updatedCP := tracker.GetCheckpoint()
	if updatedCP.ApproximateDocsMigrated != 40 {
		t.Errorf("expected 40 migrated docs after all acks, got %d", updatedCP.ApproximateDocsMigrated)
	}
	if updatedCP.TypeProgress[BSONTypeObjectID].SavedLastID != oid4 {
		t.Errorf("expected SavedLastID %v, got %v", oid4, updatedCP.TypeProgress[BSONTypeObjectID].SavedLastID)
	}
}

func TestBackfillTracker_GapSafety(t *testing.T) {
	tmpDir := t.TempDir()
	cpPath := filepath.Join(tmpDir, "backfillCheckpoint-db-coll-partition-0-of-1.json")

	cp := &PartitionCheckpoint{
		Database:       "db",
		Collection:     "coll",
		PartitionIndex: 0,
		TotalSplits:    1,
		TypeProgress:   make(map[BSONType]*TypeRangeBoundary),
	}

	tracker := NewBackfillPartitionTracker(logger.New(), cp, cpPath, 1*time.Minute, 100)

	oid1 := primitive.NewObjectID()
	oid2 := primitive.NewObjectID()
	oid3 := primitive.NewObjectID()
	oid4 := primitive.NewObjectID()

	seq1 := tracker.RegisterBatch([]interface{}{bson.D{{Key: "_id", Value: oid1}}})
	seq2 := tracker.RegisterBatch([]interface{}{bson.D{{Key: "_id", Value: oid2}}})
	_ = tracker.RegisterBatch([]interface{}{bson.D{{Key: "_id", Value: oid3}}}) // seq 3 left unacknowledged
	seq4 := tracker.RegisterBatch([]interface{}{bson.D{{Key: "_id", Value: oid4}}})

	tracker.AckBatch(seq1, 5)
	tracker.AckBatch(seq2, 5)
	tracker.AckBatch(seq4, 5) // seq 4 acked ahead of seq 3

	tracker.Close()

	// Watermark must stop strictly at seq 2
	updatedCP := tracker.GetCheckpoint()
	if updatedCP.ApproximateDocsMigrated != 10 {
		t.Errorf("expected 10 migrated docs (only batches 1 and 2), got %d", updatedCP.ApproximateDocsMigrated)
	}
	if updatedCP.TypeProgress[BSONTypeObjectID].SavedLastID != oid2 {
		t.Errorf("expected SavedLastID %v (from batch 2), got %v", oid2, updatedCP.TypeProgress[BSONTypeObjectID].SavedLastID)
	}
}

func TestBackfillTracker_MixedBSONTypes(t *testing.T) {
	tmpDir := t.TempDir()
	cpPath := filepath.Join(tmpDir, "backfillCheckpoint-db-coll-partition-0-of-1.json")

	cp := &PartitionCheckpoint{
		Database:       "db",
		Collection:     "coll",
		PartitionIndex: 0,
		TotalSplits:    1,
		TypeProgress:   make(map[BSONType]*TypeRangeBoundary),
	}

	tracker := NewBackfillPartitionTracker(logger.New(), cp, cpPath, 1*time.Minute, 100)

	oid := primitive.NewObjectID()
	binData := primitive.Binary{Subtype: 0x04, Data: []byte("uuid-1234")}

	batch1 := []interface{}{
		bson.D{{Key: "_id", Value: int64(100)}},
		bson.D{{Key: "_id", Value: "user_a"}},
	}
	batch2 := []interface{}{
		bson.D{{Key: "_id", Value: int64(200)}},
		bson.D{{Key: "_id", Value: "user_z"}},
		bson.D{{Key: "_id", Value: oid}},
		bson.D{{Key: "_id", Value: binData}},
	}

	seq1 := tracker.RegisterBatch(batch1)
	seq2 := tracker.RegisterBatch(batch2)

	tracker.AckBatch(seq1, 2)
	tracker.AckBatch(seq2, 4)

	tracker.Close()

	updatedCP := tracker.GetCheckpoint()
	if updatedCP.ApproximateDocsMigrated != 6 {
		t.Errorf("expected 6 migrated docs, got %d", updatedCP.ApproximateDocsMigrated)
	}
	if updatedCP.TypeProgress[BSONTypeNumber].SavedLastID != int64(200) {
		t.Errorf("expected Number SavedLastID 200, got %v", updatedCP.TypeProgress[BSONTypeNumber].SavedLastID)
	}
	if updatedCP.TypeProgress[BSONTypeString].SavedLastID != "user_z" {
		t.Errorf("expected String SavedLastID 'user_z', got %v", updatedCP.TypeProgress[BSONTypeString].SavedLastID)
	}
	if updatedCP.TypeProgress[BSONTypeObjectID].SavedLastID != oid {
		t.Errorf("expected ObjectID SavedLastID %v, got %v", oid, updatedCP.TypeProgress[BSONTypeObjectID].SavedLastID)
	}
	if savedBin, ok := updatedCP.TypeProgress[BSONTypeBinary].SavedLastID.(primitive.Binary); !ok || !bytes.Equal(savedBin.Data, binData.Data) {
		t.Errorf("expected Binary SavedLastID %v, got %v", binData, updatedCP.TypeProgress[BSONTypeBinary].SavedLastID)
	}
}

func TestBackfillTracker_ThresholdAndTickerSave(t *testing.T) {
	tmpDir := t.TempDir()
	cpPath := filepath.Join(tmpDir, "backfillCheckpoint-db-coll-partition-0-of-1.json")

	cp := &PartitionCheckpoint{
		Database:       "db",
		Collection:     "coll",
		PartitionIndex: 0,
		TotalSplits:    1,
		TypeProgress:   make(map[BSONType]*TypeRangeBoundary),
	}

	// Test volume threshold trigger
	tracker := NewBackfillPartitionTracker(logger.New(), cp, cpPath, 1*time.Hour, 2)

	seq1 := tracker.RegisterBatch([]interface{}{bson.D{{Key: "_id", Value: "id_1"}}})
	seq2 := tracker.RegisterBatch([]interface{}{bson.D{{Key: "_id", Value: "id_2"}}})

	tracker.AckBatch(seq1, 1) // below threshold 2
	if _, err := os.Stat(cpPath); !os.IsNotExist(err) {
		t.Fatalf("checkpoint should not exist before reaching saveThreshold")
	}

	tracker.AckBatch(seq2, 1) // reaches threshold 2
	if _, err := os.Stat(cpPath); err != nil {
		t.Fatalf("checkpoint should exist after reaching saveThreshold: %v", err)
	}

	_ = os.Remove(cpPath)

	// Test periodic ticker trigger
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tickerTracker := NewBackfillPartitionTracker(logger.New(), cp, cpPath, 20*time.Millisecond, 100)
	tickerTracker.Start(ctx)

	seq3 := tickerTracker.RegisterBatch([]interface{}{bson.D{{Key: "_id", Value: "id_3"}}})
	tickerTracker.AckBatch(seq3, 1) // 1 ack is below threshold 100

	time.Sleep(60 * time.Millisecond) // Wait for ticker to trigger

	if _, err := os.Stat(cpPath); err != nil {
		t.Fatalf("checkpoint should exist after ticker interval: %v", err)
	}
	tickerTracker.Close()
}

func TestBackfillTracker_EmptyPathDoesNotWriteDisk(t *testing.T) {
	cp := &PartitionCheckpoint{
		Database:       "db",
		Collection:     "coll",
		PartitionIndex: 0,
		TotalSplits:    1,
		TypeProgress:   make(map[BSONType]*TypeRangeBoundary),
	}

	tracker := NewBackfillPartitionTracker(logger.New(), cp, "", 1*time.Minute, 1) // empty checkpointPath

	seq := tracker.RegisterBatch([]interface{}{bson.D{{Key: "_id", Value: "doc_1"}}})
	tracker.AckBatch(seq, 1)
	tracker.Close()

	updatedCP := tracker.GetCheckpoint()
	if updatedCP.TypeProgress[BSONTypeString].SavedLastID != "doc_1" {
		t.Errorf("expected in-memory update when checkpointPath is empty")
	}
}

func TestBackfillTracker_ConcurrentStress(t *testing.T) {
	tmpDir := t.TempDir()
	cpPath := filepath.Join(tmpDir, "backfillCheckpoint-db-coll-partition-0-of-1.json")

	cp := &PartitionCheckpoint{
		Database:       "db",
		Collection:     "coll",
		PartitionIndex: 0,
		TotalSplits:    1,
		TypeProgress:   make(map[BSONType]*TypeRangeBoundary),
	}

	tracker := NewBackfillPartitionTracker(logger.New(), cp, cpPath, 50*time.Millisecond, 20)

	const totalBatches = 200
	const numWorkers = 8

	type registeredBatch struct {
		seq   uint64
		maxID int64
	}

	batches := make([]registeredBatch, totalBatches)
	for i := 0; i < totalBatches; i++ {
		maxID := int64(i + 1)
		doc := bson.D{{Key: "_id", Value: maxID}}
		seq := tracker.RegisterBatch([]interface{}{doc})
		batches[i] = registeredBatch{seq: seq, maxID: maxID}
	}

	// Shuffle batches to simulate randomized worker completions
	rand.Shuffle(len(batches), func(i, j int) {
		batches[i], batches[j] = batches[j], batches[i]
	})

	batchChan := make(chan registeredBatch, totalBatches)
	for _, b := range batches {
		batchChan <- b
	}
	close(batchChan)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range batchChan {
				time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
				tracker.AckBatch(b.seq, 1)
			}
		}()
	}

	wg.Wait()
	tracker.Close()

	updatedCP := tracker.GetCheckpoint()
	if updatedCP.ApproximateDocsMigrated != totalBatches {
		t.Errorf("expected %d approximate migrated docs, got %d", totalBatches, updatedCP.ApproximateDocsMigrated)
	}
	if updatedCP.TypeProgress[BSONTypeNumber].SavedLastID != int64(totalBatches) {
		t.Errorf("expected Number SavedLastID %d, got %v", totalBatches, updatedCP.TypeProgress[BSONTypeNumber].SavedLastID)
	}
}

func TestBackfillTracker_SaveFailureRetriesWithoutLoss(t *testing.T) {
	tmpDir := t.TempDir()
	validPath := filepath.Join(tmpDir, "backfillCheckpoint-db-coll-partition-0-of-1.json")

	blockingFile := filepath.Join(tmpDir, "blocking-file")
	if err := os.WriteFile(blockingFile, []byte("block"), 0644); err != nil {
		t.Fatalf("failed to create blocking file: %v", err)
	}
	invalidPath := filepath.Join(blockingFile, "sub", "checkpoint.json")

	cp := &PartitionCheckpoint{
		Database:       "db",
		Collection:     "coll",
		PartitionIndex: 0,
		TotalSplits:    1,
		TypeProgress:   make(map[BSONType]*TypeRangeBoundary),
	}

	// Initialize tracker with invalid path to simulate disk I/O failure
	tracker := NewBackfillPartitionTracker(logger.New(), cp, invalidPath, 1*time.Minute, 100)

	seq1 := tracker.RegisterBatch([]interface{}{bson.D{{Key: "_id", Value: "id_1"}}})
	tracker.AckBatch(seq1, 10)

	tracker.mu.Lock()
	tracker.saveCheckpointsLocked()
	tracker.mu.Unlock()

	// Since disk write failed, pendingSeqs and batches should NOT be pruned
	tracker.mu.Lock()
	if len(tracker.pendingSeqs) != 1 {
		t.Errorf("expected pendingSeqs to retain 1 batch on failure, got %d", len(tracker.pendingSeqs))
	}
	if len(tracker.batches) != 1 {
		t.Errorf("expected batches to retain 1 batch on failure, got %d", len(tracker.batches))
	}
	// Switch to valid path and retry
	tracker.checkpointPath = validPath
	tracker.saveCheckpointsLocked()
	tracker.mu.Unlock()

	updatedCP := tracker.GetCheckpoint()
	if len(tracker.pendingSeqs) != 0 {
		t.Errorf("expected pendingSeqs to be pruned after successful save, got %d", len(tracker.pendingSeqs))
	}
	if updatedCP.ApproximateDocsMigrated != 10 {
		t.Errorf("expected ApproximateDocsMigrated 10, got %d", updatedCP.ApproximateDocsMigrated)
	}
	if updatedCP.TypeProgress[BSONTypeString].SavedLastID != "id_1" {
		t.Errorf("expected SavedLastID 'id_1', got %v", updatedCP.TypeProgress[BSONTypeString].SavedLastID)
	}

	loaded, err := LoadPartitionCheckpoint(validPath)
	if err != nil {
		t.Fatalf("failed to load checkpoint from disk: %v", err)
	}
	if loaded.ApproximateDocsMigrated != 10 {
		t.Errorf("expected loaded ApproximateDocsMigrated 10, got %d", loaded.ApproximateDocsMigrated)
	}
}
