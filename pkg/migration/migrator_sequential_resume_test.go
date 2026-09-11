package migration

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"firestore-dr-migration/pkg/config"
	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestMigrator_SequentialResumption_DeterminePlan_Fresh(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "test_db"
	collName := "test_coll"

	plan, err := DetermineBackfillResumptionPlan(tmpDir, dbName, collName, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.Mode != ResumptionModeFresh {
		t.Errorf("expected ResumptionModeFresh, got %v", plan.Mode)
	}
	if plan.TotalDocsMigrated() != 0 {
		t.Errorf("expected TotalDocsMigrated 0, got %d", plan.TotalDocsMigrated())
	}
}

func TestMigrator_SequentialResumption_DeterminePlan_DirectResume(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "test_db"
	collName := "test_coll"
	oid, _ := primitive.ObjectIDFromHex("60a000000000000000000045")

	cp := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          0,
		TotalSplits:             1,
		ApproximateDocsMigrated: 12500,
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeObjectID: {BSONType: BSONTypeObjectID, SavedLastID: oid},
		},
		UpdatedAt: time.Now().UTC(),
	}

	checkpointPath := GetPartitionCheckpointPath(tmpDir, dbName, collName, 0, 1)
	if err := SavePartitionCheckpoint(checkpointPath, cp); err != nil {
		t.Fatalf("failed to save checkpoint: %v", err)
	}

	plan, err := DetermineBackfillResumptionPlan(tmpDir, dbName, collName, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.Mode != ResumptionModeDirect {
		t.Fatalf("expected ResumptionModeDirect, got %v", plan.Mode)
	}
	if plan.TotalDocsMigrated() != 12500 {
		t.Errorf("expected TotalDocsMigrated 12500, got %d", plan.TotalDocsMigrated())
	}
	if len(plan.PartitionFilters) != 1 {
		t.Fatalf("expected 1 partition filter, got %d", len(plan.PartitionFilters))
	}

	expectedFilter := bson.D{{Key: "_id", Value: bson.D{
		{Key: "$type", Value: "objectId"},
		{Key: "$gte", Value: oid},
	}}}

	filterBytes, _ := bson.Marshal(plan.PartitionFilters[0])
	expectedBytes, _ := bson.Marshal(expectedFilter)
	if !bytes.Equal(filterBytes, expectedBytes) {
		t.Errorf("filter mismatch: got %+v, want %+v", plan.PartitionFilters[0], expectedFilter)
	}
}

func TestMigrator_SequentialResumption_DeterminePlan_ResampleWithGlobalMin(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "test_db"
	collName := "test_coll"

	oid1 := primitive.NewObjectID()
	time.Sleep(5 * time.Millisecond)
	oid2 := primitive.NewObjectID()

	// Prior run had 2 partitions, new run is sequential (1 partition)
	cp0 := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          0,
		TotalSplits:             2,
		ApproximateDocsMigrated: 5000,
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeEndID: oid2, SavedLastID: oid1},
		},
		UpdatedAt: time.Now().UTC(),
	}
	cp1 := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          1,
		TotalSplits:             2,
		ApproximateDocsMigrated: 8000,
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeStartID: oid2, SavedLastID: oid2},
		},
		UpdatedAt: time.Now().UTC(),
	}

	_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, dbName, collName, 0, 2), cp0)
	_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, dbName, collName, 1, 2), cp1)

	plan, err := DetermineBackfillResumptionPlan(tmpDir, dbName, collName, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.Mode != ResumptionModeResampleWithGlobalMin {
		t.Fatalf("expected ResumptionModeResampleWithGlobalMin, got %v", plan.Mode)
	}
	if plan.TotalDocsMigrated() != 13000 {
		t.Fatalf("expected TotalDocsMigrated 13000, got %d", plan.TotalDocsMigrated())
	}
	if plan.GlobalMinSafeIDs[BSONTypeObjectID] != oid1 {
		t.Fatalf("expected global min safe ID %v, got %v", oid1, plan.GlobalMinSafeIDs[BSONTypeObjectID])
	}

	rebuiltCP := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          0,
		TotalSplits:             1,
		TypeProgress:            make(map[BSONType]*TypeRangeBoundary),
		ApproximateDocsMigrated: plan.TotalDocsMigrated(),
		UpdatedAt:               time.Now().UTC(),
	}
	for bType, minID := range plan.GlobalMinSafeIDs {
		rebuiltCP.TypeProgress[bType] = &TypeRangeBoundary{BSONType: bType, SavedLastID: minID}
	}

	filter, filterErr := BuildPartitionFilterFromCheckpoint(rebuiltCP)
	if filterErr != nil {
		t.Fatalf("failed to build partition filter: %v", filterErr)
	}
	if len(filter) == 0 {
		t.Fatalf("expected non-empty resume filter")
	}
}

func TestMigrator_SequentialResumption_TrackerBatchRegistrationAndAck(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "test_db"
	collName := "test_coll"
	checkpointPath := GetPartitionCheckpointPath(tmpDir, dbName, collName, 0, 1)

	initialCP := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          0,
		TotalSplits:             1,
		ApproximateDocsMigrated: 0,
		TypeProgress:            make(map[BSONType]*TypeRangeBoundary),
		UpdatedAt:               time.Now().UTC(),
	}

	log := logger.New()
	tracker := NewBackfillPartitionTracker(log, initialCP, checkpointPath, 5*time.Minute, 100)

	oid1 := primitive.NewObjectID()
	oid2 := primitive.NewObjectID()
	oid3 := primitive.NewObjectID()

	batch1 := []interface{}{
		bson.D{{Key: "_id", Value: oid1}, {Key: "val", Value: "1"}},
		bson.D{{Key: "_id", Value: int64(100)}, {Key: "val", Value: "num1"}},
		bson.D{{Key: "_id", Value: "user_a"}, {Key: "val", Value: "str1"}},
	}
	batch2 := []interface{}{
		bson.D{{Key: "_id", Value: oid2}, {Key: "val", Value: "2"}},
		bson.D{{Key: "_id", Value: int64(200)}, {Key: "val", Value: "num2"}},
		bson.D{{Key: "_id", Value: "user_b"}, {Key: "val", Value: "str2"}},
	}
	batch3 := []interface{}{
		bson.D{{Key: "_id", Value: oid3}, {Key: "val", Value: "3"}},
		bson.D{{Key: "_id", Value: int64(300)}, {Key: "val", Value: "num3"}},
		bson.D{{Key: "_id", Value: "user_c"}, {Key: "val", Value: "str3"}},
	}

	seq1 := tracker.RegisterBatch(batch1)
	seq2 := tracker.RegisterBatch(batch2)
	seq3 := tracker.RegisterBatch(batch3)

	if seq1 != 1 || seq2 != 2 || seq3 != 3 {
		t.Fatalf("expected sequence numbers 1, 2, 3; got %d, %d, %d", seq1, seq2, seq3)
	}

	// ACK batch 2 first (out of order)
	tracker.AckBatch(seq2, int64(len(batch2)))

	// Close forces save of contiguous watermarks
	tracker.Close()

	// Since batch 1 is unacked, checkpoint on disk should not exist or have 0 docs
	loadedCP, err := LoadPartitionCheckpoint(checkpointPath)
	if err != nil {
		t.Fatalf("unexpected error loading checkpoint: %v", err)
	}
	if loadedCP != nil && loadedCP.ApproximateDocsMigrated != 0 {
		t.Errorf("expected 0 docs migrated before batch 1 acked, got %d", loadedCP.ApproximateDocsMigrated)
	}

	// Now ACK batch 1 and batch 3
	tracker.AckBatch(seq1, int64(len(batch1)))
	tracker.AckBatch(seq3, int64(len(batch3)))
	tracker.Close()

	loadedCP, err = LoadPartitionCheckpoint(checkpointPath)
	if err != nil {
		t.Fatalf("unexpected error loading checkpoint: %v", err)
	}
	if loadedCP == nil {
		t.Fatalf("expected saved checkpoint on disk, got nil")
	}

	if loadedCP.ApproximateDocsMigrated != 9 {
		t.Errorf("expected ApproximateDocsMigrated 9, got %d", loadedCP.ApproximateDocsMigrated)
	}
	if loadedCP.TypeProgress[BSONTypeObjectID] == nil || loadedCP.TypeProgress[BSONTypeObjectID].SavedLastID != oid3 {
		t.Errorf("expected ObjectID SavedLastID %v, got %v", oid3, loadedCP.TypeProgress[BSONTypeObjectID])
	}
	if loadedCP.TypeProgress[BSONTypeNumber] == nil || loadedCP.TypeProgress[BSONTypeNumber].SavedLastID != int64(300) {
		t.Errorf("expected Number SavedLastID 300, got %v", loadedCP.TypeProgress[BSONTypeNumber])
	}
	if loadedCP.TypeProgress[BSONTypeString] == nil || loadedCP.TypeProgress[BSONTypeString].SavedLastID != "user_c" {
		t.Errorf("expected String SavedLastID 'user_c', got %v", loadedCP.TypeProgress[BSONTypeString])
	}
}

func TestMigrator_SequentialResumption_CleanupOnCompletion(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "test_db"
	collName := "test_coll"
	checkpointPath := GetPartitionCheckpointPath(tmpDir, dbName, collName, 0, 1)

	initialCP := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          0,
		TotalSplits:             1,
		ApproximateDocsMigrated: 0,
		TypeProgress:            make(map[BSONType]*TypeRangeBoundary),
		UpdatedAt:               time.Now().UTC(),
	}

	tracker := NewBackfillPartitionTracker(logger.New(), initialCP, checkpointPath, 5*time.Minute, 1)
	batch := []interface{}{
		bson.D{{Key: "_id", Value: "user_9999"}},
	}
	seq := tracker.RegisterBatch(batch)
	tracker.AckBatch(seq, 1) // flushes to disk because saveThreshold = 1

	// Create a leftover .tmp file to test cleanup
	tmpFile := filepath.Join(tmpDir, "backfillCheckpoint-test_db-test_coll-partition-0-of-1.json.tmp")
	_ = os.WriteFile(tmpFile, []byte("temporary data"), 0644)

	if _, err := os.Stat(checkpointPath); os.IsNotExist(err) {
		t.Fatalf("checkpoint file should exist before cleanup")
	}

	// Simulate migrateCollection finishing: MarkCompleted followed by DeletePartitionCheckpoints
	tracker.MarkCompleted()
	if err := DeletePartitionCheckpoints(tmpDir, dbName, collName); err != nil {
		t.Fatalf("failed to delete partition checkpoints: %v", err)
	}

	// Simulate deferred tracker.Close() running on function exit
	tracker.Close()

	if _, err := os.Stat(checkpointPath); !os.IsNotExist(err) {
		t.Fatalf("checkpoint file should have remained deleted and not resurrected by Close()")
	}
	if _, err := os.Stat(tmpFile); !os.IsNotExist(err) {
		t.Fatalf("temporary checkpoint file should have been deleted")
	}
}

func TestMigrator_SequentialResumption_DryRunSkipsCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	migrator := &Migrator{
		config: &config.Config{
			SaveThreshold:             100,
			CheckpointIntervalMinutes: 1,
		},
		log:           logger.New(),
		DryRun:        true,
		CheckpointDir: tmpDir,
	}

	if !migrator.DryRun {
		t.Fatalf("expected DryRun to be true")
	}

	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("failed to read temp dir: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected 0 checkpoint files in dry run mode, found %d", len(files))
	}
}

func TestMigrator_SequentialResumption_DefaultCheckpointDir(t *testing.T) {
	migrator := NewMigrator(&config.Config{}, logger.New())
	if migrator.getCheckpointDir() != "." {
		t.Errorf("expected default checkpoint dir '.', got '%s'", migrator.getCheckpointDir())
	}

	migrator.CheckpointDir = "/custom/path"
	if migrator.getCheckpointDir() != "/custom/path" {
		t.Errorf("expected custom checkpoint dir '/custom/path', got '%s'", migrator.getCheckpointDir())
	}

	migrator.CheckpointDir = ""
	if migrator.getCheckpointDir() != "." {
		t.Errorf("expected empty checkpoint dir to resolve to '.', got '%s'", migrator.getCheckpointDir())
	}
}

func TestMigrator_SequentialResumption_MixedTypeFilterWithUnreachedTypes(t *testing.T) {
	cp := &PartitionCheckpoint{
		Database:       "bin_eval_mixed_db",
		Collection:     "random_key_types",
		PartitionIndex: 0,
		TotalSplits:    1,
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeNumber:   {BSONType: BSONTypeNumber, SavedLastID: int64(87000)},
			BSONTypeString:   {BSONType: BSONTypeString, SavedLastID: nil},
			BSONTypeBinary:   {BSONType: BSONTypeBinary, SavedLastID: nil},
			BSONTypeObjectID: {BSONType: BSONTypeObjectID, SavedLastID: nil},
		},
		UpdatedAt: time.Now().UTC(),
	}

	filter, err := BuildPartitionFilterFromCheckpoint(cp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Filter should be an $or with 4 clauses:
	// 1 for in-progress number ($gte: 87000) and 3 for unreached types (string, binData, objectId)
	orVal, ok := filter.Map()["$or"].([]bson.D)
	if !ok {
		t.Fatalf("expected $or with slice of bson.D, got %+v", filter)
	}
	if len(orVal) != 4 {
		t.Fatalf("expected 4 clauses in $or filter, got %d", len(orVal))
	}
}

func TestMigrator_SequentialResumption_CorruptedCheckpointFallback(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "test_db"
	collName := "corrupted_coll"

	// Write invalid JSON bytes to simulate a corrupted or half-written checkpoint file
	checkpointPath := GetPartitionCheckpointPath(tmpDir, dbName, collName, 0, 1)
	if err := os.WriteFile(checkpointPath, []byte("{\"database\": \"test_db\", \"collection\": broken json..."), 0644); err != nil {
		t.Fatalf("failed to write corrupted checkpoint: %v", err)
	}

	plan, err := DetermineBackfillResumptionPlan(tmpDir, dbName, collName, 1)
	if err != nil {
		t.Fatalf("unexpected error determining plan: %v", err)
	}

	if plan.Mode != ResumptionModeFresh {
		t.Errorf("expected fallback to ResumptionModeFresh for corrupted checkpoint, got %v", plan.Mode)
	}
	if plan.TotalDocsMigrated() != 0 {
		t.Errorf("expected 0 docs migrated for fallback, got %d", plan.TotalDocsMigrated())
	}
}

func TestMigrator_SequentialResumption_TrackerPeriodicFlush(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "prod_orders_db"
	collName := "orders"
	checkpointPath := GetPartitionCheckpointPath(tmpDir, dbName, collName, 0, 1)

	initialCP := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          0,
		TotalSplits:             1,
		ApproximateDocsMigrated: 0,
		TypeProgress:            make(map[BSONType]*TypeRangeBoundary),
		UpdatedAt:               time.Now().UTC(),
	}

	log := logger.New()
	// Set saveThreshold to 2 to test automatic threshold-triggered flushes
	tracker := NewBackfillPartitionTracker(log, initialCP, checkpointPath, 1*time.Hour, 2)

	oid1 := primitive.NewObjectID()
	oid2 := primitive.NewObjectID()

	batch1 := []interface{}{
		bson.D{{Key: "_id", Value: oid1}, {Key: "amount", Value: 99.99}},
	}
	batch2 := []interface{}{
		bson.D{{Key: "_id", Value: oid2}, {Key: "amount", Value: 149.50}},
	}

	seq1 := tracker.RegisterBatch(batch1)
	seq2 := tracker.RegisterBatch(batch2)

	// Ack batch 1 (count = 1 < threshold 2) -> should NOT trigger disk flush yet
	tracker.AckBatch(seq1, 1)
	loaded, err := LoadPartitionCheckpoint(checkpointPath)
	if err != nil {
		t.Fatalf("unexpected error checking checkpoint: %v", err)
	}
	if loaded != nil {
		t.Errorf("expected no checkpoint file before threshold reached, got %+v", loaded)
	}

	// Ack batch 2 (count = 2 == threshold 2) -> triggers automatic disk flush
	tracker.AckBatch(seq2, 1)
	loaded, err = LoadPartitionCheckpoint(checkpointPath)
	if err != nil {
		t.Fatalf("unexpected error checking checkpoint: %v", err)
	}
	if loaded == nil {
		t.Fatalf("expected checkpoint file to be created after reaching saveThreshold")
	}
	if loaded.ApproximateDocsMigrated != 2 {
		t.Errorf("expected 2 docs migrated, got %d", loaded.ApproximateDocsMigrated)
	}
	if loaded.TypeProgress[BSONTypeObjectID] == nil || loaded.TypeProgress[BSONTypeObjectID].SavedLastID != oid2 {
		t.Errorf("expected SavedLastID %v, got %v", oid2, loaded.TypeProgress[BSONTypeObjectID])
	}

	tracker.Close()
}
