package migration

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestMigrator_ParallelResumption_DeterminePlan_DirectResume(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "test_db"
	collName := "test_coll"
	totalSplits := 4

	oid0_end, _ := primitive.ObjectIDFromHex("60a000000000000000000010")
	oid0_saved, _ := primitive.ObjectIDFromHex("60a000000000000000000005")

	oid1_start := oid0_end
	oid1_end, _ := primitive.ObjectIDFromHex("60a000000000000000000020")
	oid1_saved, _ := primitive.ObjectIDFromHex("60a000000000000000000015")

	oid2_start := oid1_end
	oid2_end, _ := primitive.ObjectIDFromHex("60a000000000000000000030")
	oid2_saved, _ := primitive.ObjectIDFromHex("60a000000000000000000025")

	oid3_start := oid2_end
	oid3_saved, _ := primitive.ObjectIDFromHex("60a000000000000000000035")

	cps := []*PartitionCheckpoint{
		{
			Database:                dbName,
			Collection:              collName,
			PartitionIndex:          0,
			TotalSplits:             totalSplits,
			ApproximateDocsMigrated: 1000,
			TypeProgress: map[BSONType]*TypeRangeBoundary{
				BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeEndID: oid0_end, SavedLastID: oid0_saved},
			},
			UpdatedAt: time.Now().UTC(),
		},
		{
			Database:                dbName,
			Collection:              collName,
			PartitionIndex:          1,
			TotalSplits:             totalSplits,
			ApproximateDocsMigrated: 2000,
			TypeProgress: map[BSONType]*TypeRangeBoundary{
				BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeStartID: oid1_start, RangeEndID: oid1_end, SavedLastID: oid1_saved},
			},
			UpdatedAt: time.Now().UTC(),
		},
		{
			Database:                dbName,
			Collection:              collName,
			PartitionIndex:          2,
			TotalSplits:             totalSplits,
			ApproximateDocsMigrated: 3000,
			TypeProgress: map[BSONType]*TypeRangeBoundary{
				BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeStartID: oid2_start, RangeEndID: oid2_end, SavedLastID: oid2_saved},
			},
			UpdatedAt: time.Now().UTC(),
		},
		{
			Database:                dbName,
			Collection:              collName,
			PartitionIndex:          3,
			TotalSplits:             totalSplits,
			ApproximateDocsMigrated: 4000,
			TypeProgress: map[BSONType]*TypeRangeBoundary{
				BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeStartID: oid3_start, SavedLastID: oid3_saved},
			},
			UpdatedAt: time.Now().UTC(),
		},
	}

	for i, cp := range cps {
		cpPath := GetPartitionCheckpointPath(tmpDir, dbName, collName, i, totalSplits)
		if err := SavePartitionCheckpoint(cpPath, cp); err != nil {
			t.Fatalf("failed to save checkpoint for partition %d: %v", i, err)
		}
	}

	plan, err := DetermineBackfillResumptionPlan(tmpDir, dbName, collName, totalSplits)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.Mode != ResumptionModeDirect {
		t.Fatalf("expected ResumptionModeDirect, got %v", plan.Mode)
	}
	if plan.TotalDocsMigrated() != 10000 {
		t.Errorf("expected TotalDocsMigrated 10000, got %d", plan.TotalDocsMigrated())
	}
	if len(plan.PartitionFilters) != totalSplits {
		t.Fatalf("expected %d partition filters, got %d", totalSplits, len(plan.PartitionFilters))
	}

	// Verify partition 0 filter
	expected0 := bson.D{{Key: "_id", Value: bson.D{
		{Key: "$type", Value: "objectId"},
		{Key: "$gte", Value: oid0_saved},
		{Key: "$lt", Value: oid0_end},
	}}}
	if diff := cmp.Diff(expected0, plan.PartitionFilters[0]); diff != "" {
		t.Errorf("partition 0 filter mismatch (-want +got):\n%s", diff)
	}

	// Verify partition 3 filter (unbounded upper end)
	expected3 := bson.D{{Key: "_id", Value: bson.D{
		{Key: "$type", Value: "objectId"},
		{Key: "$gte", Value: oid3_saved},
	}}}
	if diff := cmp.Diff(expected3, plan.PartitionFilters[3]); diff != "" {
		t.Errorf("partition 3 filter mismatch (-want +got):\n%s", diff)
	}
}

func TestMigrator_ParallelResumption_DeterminePlan_SubsetCompleted(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "test_db"
	collName := "test_coll"
	totalSplits := 3

	oid0_end, _ := primitive.ObjectIDFromHex("60a000000000000000000010")
	oid1_end, _ := primitive.ObjectIDFromHex("60a000000000000000000020")
	oid2_saved, _ := primitive.ObjectIDFromHex("60a000000000000000000025")

	// Partition 0 is 100% completed: SavedLastID reached RangeEndID
	cp0 := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          0,
		TotalSplits:             totalSplits,
		ApproximateDocsMigrated: 5000,
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeEndID: oid0_end, SavedLastID: oid0_end},
		},
		UpdatedAt: time.Now().UTC(),
	}

	// Partition 1 is 100% completed: SavedLastID reached RangeEndID
	cp1 := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          1,
		TotalSplits:             totalSplits,
		ApproximateDocsMigrated: 5000,
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeStartID: oid0_end, RangeEndID: oid1_end, SavedLastID: oid1_end},
		},
		UpdatedAt: time.Now().UTC(),
	}

	// Partition 2 is in-flight: SavedLastID is partially through
	cp2 := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          2,
		TotalSplits:             totalSplits,
		ApproximateDocsMigrated: 2500,
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeStartID: oid1_end, SavedLastID: oid2_saved},
		},
		UpdatedAt: time.Now().UTC(),
	}

	_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, dbName, collName, 0, totalSplits), cp0)
	_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, dbName, collName, 1, totalSplits), cp1)
	_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, dbName, collName, 2, totalSplits), cp2)

	plan, err := DetermineBackfillResumptionPlan(tmpDir, dbName, collName, totalSplits)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.Mode != ResumptionModeDirect {
		t.Fatalf("expected ResumptionModeDirect, got %v", plan.Mode)
	}
	if plan.TotalDocsMigrated() != 12500 {
		t.Errorf("expected TotalDocsMigrated 12500, got %d", plan.TotalDocsMigrated())
	}

	// Partitions 0 and 1 must produce $exists: false (0 documents scanned)
	emptyFilter := bson.D{{Key: "_id", Value: bson.D{{Key: "$exists", Value: false}}}}
	if diff := cmp.Diff(emptyFilter, plan.PartitionFilters[0]); diff != "" {
		t.Errorf("partition 0 completed filter mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(emptyFilter, plan.PartitionFilters[1]); diff != "" {
		t.Errorf("partition 1 completed filter mismatch (-want +got):\n%s", diff)
	}

	// Partition 2 must resume from oid2_saved
	expected2 := bson.D{{Key: "_id", Value: bson.D{
		{Key: "$type", Value: "objectId"},
		{Key: "$gte", Value: oid2_saved},
	}}}
	if diff := cmp.Diff(expected2, plan.PartitionFilters[2]); diff != "" {
		t.Errorf("partition 2 filter mismatch (-want +got):\n%s", diff)
	}
}

func TestMigrator_ParallelResumption_DeterminePlan_ResampleWithGlobalMin(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "test_db"
	collName := "test_coll"
	historicalSplits := 2

	oid1, _ := primitive.ObjectIDFromHex("60a000000000000000000010")
	oid2, _ := primitive.ObjectIDFromHex("60a000000000000000000050")
	oid3, _ := primitive.ObjectIDFromHex("60a000000000000000000090")

	// Historical run had 2 partitions with mixed types
	cp0 := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          0,
		TotalSplits:             historicalSplits,
		ApproximateDocsMigrated: 3000,
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeNumber:   {BSONType: BSONTypeNumber, RangeEndID: int64(500), SavedLastID: int64(250)},
			BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeEndID: oid2, SavedLastID: oid1},
			BSONTypeString:   {BSONType: BSONTypeString, RangeEndID: "m", SavedLastID: "d"},
		},
		UpdatedAt: time.Now().UTC(),
	}
	cp1 := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          1,
		TotalSplits:             historicalSplits,
		ApproximateDocsMigrated: 4000,
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeNumber:   {BSONType: BSONTypeNumber, RangeStartID: int64(500), SavedLastID: int64(750)},
			BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeStartID: oid2, SavedLastID: oid3},
			BSONTypeString:   {BSONType: BSONTypeString, RangeStartID: "m", SavedLastID: "t"},
		},
		UpdatedAt: time.Now().UTC(),
	}

	_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, dbName, collName, 0, historicalSplits), cp0)
	_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, dbName, collName, 1, historicalSplits), cp1)

	// New run is configured for 4 partitions
	plan, err := DetermineBackfillResumptionPlan(tmpDir, dbName, collName, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.Mode != ResumptionModeResampleWithGlobalMin {
		t.Fatalf("expected ResumptionModeResampleWithGlobalMin, got %v", plan.Mode)
	}
	if plan.TotalDocsMigrated() != 7000 {
		t.Errorf("expected TotalDocsMigrated 7000, got %d", plan.TotalDocsMigrated())
	}
	if plan.GlobalMinSafeIDs[BSONTypeNumber] != int64(250) {
		t.Errorf("expected Number GlobalMin 250, got %v", plan.GlobalMinSafeIDs[BSONTypeNumber])
	}
	if plan.GlobalMinSafeIDs[BSONTypeObjectID] != oid1 {
		t.Errorf("expected ObjectID GlobalMin %v, got %v", oid1, plan.GlobalMinSafeIDs[BSONTypeObjectID])
	}
	if plan.GlobalMinSafeIDs[BSONTypeString] != "d" {
		t.Errorf("expected String GlobalMin 'd', got %v", plan.GlobalMinSafeIDs[BSONTypeString])
	}
}

func TestMigrator_ParallelResumption_MultiPartitionTrackerWatermarking(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "test_db"
	collName := "test_coll"
	totalSplits := 2
	log := logger.New()

	cp0Path := GetPartitionCheckpointPath(tmpDir, dbName, collName, 0, totalSplits)
	cp1Path := GetPartitionCheckpointPath(tmpDir, dbName, collName, 1, totalSplits)

	cp0 := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          0,
		TotalSplits:             totalSplits,
		ApproximateDocsMigrated: 0,
		TypeProgress:            make(map[BSONType]*TypeRangeBoundary),
		UpdatedAt:               time.Now().UTC(),
	}
	cp1 := &PartitionCheckpoint{
		Database:                dbName,
		Collection:              collName,
		PartitionIndex:          1,
		TotalSplits:             totalSplits,
		ApproximateDocsMigrated: 0,
		TypeProgress:            make(map[BSONType]*TypeRangeBoundary),
		UpdatedAt:               time.Now().UTC(),
	}

	tracker0 := NewBackfillPartitionTracker(log, cp0, cp0Path, 5*time.Minute, 100)
	tracker1 := NewBackfillPartitionTracker(log, cp1, cp1Path, 5*time.Minute, 100)

	oidA1 := primitive.NewObjectID()
	oidA2 := primitive.NewObjectID()
	oidB1 := primitive.NewObjectID()
	oidB2 := primitive.NewObjectID()

	// Tracker 0: Register 2 batches
	seq0_1 := tracker0.RegisterBatch([]interface{}{bson.D{{Key: "_id", Value: oidA1}}})
	seq0_2 := tracker0.RegisterBatch([]interface{}{bson.D{{Key: "_id", Value: oidA2}}})

	// Tracker 1: Register 2 batches
	seq1_1 := tracker1.RegisterBatch([]interface{}{bson.D{{Key: "_id", Value: oidB1}}})
	seq1_2 := tracker1.RegisterBatch([]interface{}{bson.D{{Key: "_id", Value: oidB2}}})

	var wg sync.WaitGroup
	wg.Add(2)

	// Concurrently simulate workers acknowledging batches
	go func() {
		defer wg.Done()
		tracker0.AckBatch(seq0_2, 1) // Out of order: ack batch 2 before batch 1
		time.Sleep(10 * time.Millisecond)
		tracker0.AckBatch(seq0_1, 1)
		tracker0.Close()
	}()

	go func() {
		defer wg.Done()
		tracker1.AckBatch(seq1_1, 1)
		tracker1.AckBatch(seq1_2, 1)
		tracker1.Close()
	}()

	wg.Wait()

	// Verify Tracker 0 checkpoint
	loaded0, err := LoadPartitionCheckpoint(cp0Path)
	if err != nil || loaded0 == nil {
		t.Fatalf("failed to load partition 0 checkpoint: %v", err)
	}
	if loaded0.ApproximateDocsMigrated != 2 {
		t.Errorf("expected partition 0 migrated count 2, got %d", loaded0.ApproximateDocsMigrated)
	}
	if loaded0.TypeProgress[BSONTypeObjectID] == nil || loaded0.TypeProgress[BSONTypeObjectID].SavedLastID != oidA2 {
		t.Errorf("expected partition 0 SavedLastID %v, got %v", oidA2, loaded0.TypeProgress[BSONTypeObjectID])
	}

	// Verify Tracker 1 checkpoint
	loaded1, err := LoadPartitionCheckpoint(cp1Path)
	if err != nil || loaded1 == nil {
		t.Fatalf("failed to load partition 1 checkpoint: %v", err)
	}
	if loaded1.ApproximateDocsMigrated != 2 {
		t.Errorf("expected partition 1 migrated count 2, got %d", loaded1.ApproximateDocsMigrated)
	}
	if loaded1.TypeProgress[BSONTypeObjectID] == nil || loaded1.TypeProgress[BSONTypeObjectID].SavedLastID != oidB2 {
		t.Errorf("expected partition 1 SavedLastID %v, got %v", oidB2, loaded1.TypeProgress[BSONTypeObjectID])
	}
}

func TestMigrator_ParallelResumption_CleanupOnCompletion(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "test_db"
	collName := "test_coll"
	totalSplits := 2
	log := logger.New()

	for i := 0; i < totalSplits; i++ {
		cpPath := GetPartitionCheckpointPath(tmpDir, dbName, collName, i, totalSplits)
		cp := &PartitionCheckpoint{
			Database:                dbName,
			Collection:              collName,
			PartitionIndex:          i,
			TotalSplits:             totalSplits,
			ApproximateDocsMigrated: 100,
			TypeProgress:            map[BSONType]*TypeRangeBoundary{BSONTypeObjectID: {BSONType: BSONTypeObjectID}},
			UpdatedAt:               time.Now().UTC(),
		}
		tracker := NewBackfillPartitionTracker(log, cp, cpPath, 5*time.Minute, 1)
		seq := tracker.RegisterBatch([]interface{}{bson.D{{Key: "_id", Value: primitive.NewObjectID()}}})
		tracker.AckBatch(seq, 1) // flushes to disk because saveThreshold = 1

		// Also create a leftover .tmp file
		tmpFile := filepath.Join(tmpDir, filepath.Base(cpPath)+".tmp")
		_ = os.WriteFile(tmpFile, []byte("temporary data"), 0644)

		tracker.MarkCompleted()
		tracker.Close()
	}

	// Verify files exist before deletion
	cps, err := ListPartitionCheckpoints(tmpDir, dbName, collName)
	if err != nil || len(cps) != totalSplits {
		t.Fatalf("expected %d checkpoints before deletion, got %d (err: %v)", totalSplits, len(cps), err)
	}

	// Delete on successful collection migration
	if err := DeletePartitionCheckpoints(tmpDir, dbName, collName); err != nil {
		t.Fatalf("failed to delete checkpoints: %v", err)
	}

	// Verify files are cleaned up
	cpsAfter, err := ListPartitionCheckpoints(tmpDir, dbName, collName)
	if err != nil {
		t.Fatalf("unexpected error listing checkpoints: %v", err)
	}
	if len(cpsAfter) != 0 {
		t.Errorf("expected 0 checkpoints after cleanup, got %d", len(cpsAfter))
	}
}

func TestMigrator_ParallelResumption_CorruptedCheckpointFallbackToFresh(t *testing.T) {
	tmpDir := t.TempDir()
	dbName := "test_db"
	collName := "test_coll"
	totalSplits := 3

	// Only write partition 0 and 2 (missing partition 1)
	cp0 := &PartitionCheckpoint{
		Database:       dbName,
		Collection:     collName,
		PartitionIndex: 0,
		TotalSplits:    totalSplits,
		TypeProgress:   map[BSONType]*TypeRangeBoundary{BSONTypeObjectID: {BSONType: BSONTypeObjectID}},
	}
	cp2 := &PartitionCheckpoint{
		Database:       dbName,
		Collection:     collName,
		PartitionIndex: 2,
		TotalSplits:    totalSplits,
		TypeProgress:   map[BSONType]*TypeRangeBoundary{BSONTypeObjectID: {BSONType: BSONTypeObjectID}},
	}

	_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, dbName, collName, 0, totalSplits), cp0)
	_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, dbName, collName, 2, totalSplits), cp2)

	plan, err := DetermineBackfillResumptionPlan(tmpDir, dbName, collName, totalSplits)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.Mode != ResumptionModeFresh {
		t.Errorf("expected ResumptionModeFresh for corrupted/missing partition checkpoint set, got %v", plan.Mode)
	}
	if plan.TotalDocsMigrated() != 0 {
		t.Errorf("expected TotalDocsMigrated 0 on fresh fallback, got %d", plan.TotalDocsMigrated())
	}
}

func TestMigrator_ParallelResumption_RawByteFilterEquivalence(t *testing.T) {
	oid, _ := primitive.ObjectIDFromHex("60a000000000000000000088")
	filter := bson.D{{Key: "_id", Value: bson.D{
		{Key: "$type", Value: "objectId"},
		{Key: "$gte", Value: oid},
	}}}

	filterBytes, err := bson.Marshal(filter)
	if err != nil {
		t.Fatalf("failed to marshal filter: %v", err)
	}

	var roundtrip bson.D
	if err := bson.Unmarshal(filterBytes, &roundtrip); err != nil {
		t.Fatalf("failed to unmarshal filter: %v", err)
	}

	roundtripBytes, _ := bson.Marshal(roundtrip)
	if !bytes.Equal(filterBytes, roundtripBytes) {
		t.Errorf("expected byte-exact filter roundtrip equivalence")
	}
}

func TestCalculatePartitionCount(t *testing.T) {
	tests := []struct {
		name                string
		totalCount          int64
		minDocsPerPartition int
		maxPartitions       int
		expected            int
	}{
		{
			name:                "SmallCollectionBelowMinDocs",
			totalCount:          500,
			minDocsPerPartition: 1000,
			maxPartitions:       10,
			expected:            1,
		},
		{
			name:                "ModerateCollectionExactMultiple",
			totalCount:          4000,
			minDocsPerPartition: 1000,
			maxPartitions:       10,
			expected:            4,
		},
		{
			name:                "ModerateCollectionWithRemainder",
			totalCount:          4500,
			minDocsPerPartition: 1000,
			maxPartitions:       10,
			expected:            4,
		},
		{
			name:                "HugeCollectionCappedAtMaxPartitions",
			totalCount:          100000,
			minDocsPerPartition: 1000,
			maxPartitions:       8,
			expected:            8,
		},
		{
			name:                "ZeroDocumentCount",
			totalCount:          0,
			minDocsPerPartition: 1000,
			maxPartitions:       10,
			expected:            1,
		},
		{
			name:                "NonPositiveConfigsGuards",
			totalCount:          5000,
			minDocsPerPartition: 0,
			maxPartitions:       0,
			expected:            1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := CalculatePartitionCount(tc.totalCount, tc.minDocsPerPartition, tc.maxPartitions)
			if got != tc.expected {
				t.Errorf("CalculatePartitionCount(%d, %d, %d) = %d; want %d",
					tc.totalCount, tc.minDocsPerPartition, tc.maxPartitions, got, tc.expected)
			}
		})
	}
}

func TestClampPartitionsWithGlobalMinSafeIDs_MultiPartitionReconfiguration(t *testing.T) {
	// Raw 4 partitions for string _id: ["", "e"), ["e", "j"), ["j", "p"), ["p", +inf)
	rawPartitions := []bson.D{
		{{Key: "_id", Value: bson.D{{Key: "$type", Value: "string"}, {Key: "$lt", Value: "e"}}}},
		{{Key: "_id", Value: bson.D{{Key: "$type", Value: "string"}, {Key: "$gte", Value: "e"}, {Key: "$lt", Value: "j"}}}},
		{{Key: "_id", Value: bson.D{{Key: "$type", Value: "string"}, {Key: "$gte", Value: "j"}, {Key: "$lt", Value: "p"}}}},
		{{Key: "_id", Value: bson.D{{Key: "$type", Value: "string"}, {Key: "$gte", Value: "p"}}}},
	}

	// Safe watermark is at string "m" (which falls inside partition 2 ["j", "p"))
	globalMinSafeIDs := map[BSONType]any{
		BSONTypeString: "m",
	}

	clamped := ClampPartitionsWithGlobalMinSafeIDs(rawPartitions, globalMinSafeIDs)

	if len(clamped) != 4 {
		t.Fatalf("expected 4 partitions, got %d", len(clamped))
	}

	// Partition 0: entirely < "m" -> skipped ($exists: false)
	expected0 := bson.D{{Key: "_id", Value: bson.D{{Key: "$exists", Value: false}}}}
	if diff := cmp.Diff(expected0, clamped[0]); diff != "" {
		t.Errorf("partition 0 mismatch (-want +got):\n%s", diff)
	}

	// Partition 1: entirely < "m" -> skipped ($exists: false)
	expected1 := bson.D{{Key: "_id", Value: bson.D{{Key: "$exists", Value: false}}}}
	if diff := cmp.Diff(expected1, clamped[1]); diff != "" {
		t.Errorf("partition 1 mismatch (-want +got):\n%s", diff)
	}

	// Partition 2: straddles "m" -> clamped lower bound $gte: "m", $lt: "p"
	expected2 := bson.D{{Key: "_id", Value: bson.D{
		{Key: "$type", Value: "string"},
		{Key: "$gte", Value: "m"},
		{Key: "$lt", Value: "p"},
	}}}
	if diff := cmp.Diff(expected2, clamped[2]); diff != "" {
		t.Errorf("partition 2 mismatch (-want +got):\n%s", diff)
	}

	// Partition 3: entirely >= "p" > "m" -> untouched
	expected3 := bson.D{{Key: "_id", Value: bson.D{
		{Key: "$type", Value: "string"},
		{Key: "$gte", Value: "p"},
	}}}
	if diff := cmp.Diff(expected3, clamped[3]); diff != "" {
		t.Errorf("partition 3 mismatch (-want +got):\n%s", diff)
	}
}

func TestClampPartitionsWithGlobalMinSafeIDs_MixedTypesDifferentProgress(t *testing.T) {
	// Partition 1 has mixed types in $or:
	// - String: ["e", "j") -> entirely below "m"
	// - Number: [50, 100) -> straddles number watermark 75
	partition1 := bson.D{
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: "string"}, {Key: "$gte", Value: "e"}, {Key: "$lt", Value: "j"}}}},
			bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: "number"}, {Key: "$gte", Value: int64(50)}, {Key: "$lt", Value: int64(100)}}}},
		}},
	}

	globalMinSafeIDs := map[BSONType]any{
		BSONTypeString: "m",
		BSONTypeNumber: int64(75),
	}

	clamped := ClampPartitionsWithGlobalMinSafeIDs([]bson.D{partition1}, globalMinSafeIDs)

	if len(clamped) != 1 {
		t.Fatalf("expected 1 partition, got %d", len(clamped))
	}

	// String clause was dropped because it's completely below "m".
	// Number clause was clamped to $gte: 75, $lt: 100.
	// Because only 1 clause remains, it unrolls from $or into a single type clause.
	expected := bson.D{{Key: "_id", Value: bson.D{
		{Key: "$type", Value: "number"},
		{Key: "$gte", Value: int64(75)},
		{Key: "$lt", Value: int64(100)},
	}}}
	if diff := cmp.Diff(expected, clamped[0]); diff != "" {
		t.Errorf("mixed type partition mismatch (-want +got):\n%s", diff)
	}
}

func TestClampPartitionsWithGlobalMinSafeIDs_ObjectIDInference(t *testing.T) {
	oid1, _ := primitive.ObjectIDFromHex("60a000000000000000000010")
	oid2, _ := primitive.ObjectIDFromHex("60a000000000000000000020")
	oid3, _ := primitive.ObjectIDFromHex("60a000000000000000000030")

	// 3 partitions with implicit ObjectID type (no explicit $type field)
	rawPartitions := []bson.D{
		{{Key: "_id", Value: bson.D{{Key: "$lt", Value: oid1}}}},
		{{Key: "_id", Value: bson.D{{Key: "$gte", Value: oid1}, {Key: "$lt", Value: oid3}}}},
		{{Key: "_id", Value: bson.D{{Key: "$gte", Value: oid3}}}},
	}

	globalMinSafeIDs := map[BSONType]any{
		BSONTypeObjectID: oid2,
	}

	clamped := ClampPartitionsWithGlobalMinSafeIDs(rawPartitions, globalMinSafeIDs)

	// Partition 0: < oid1 <= oid2 -> skipped ($exists: false)
	expected0 := bson.D{{Key: "_id", Value: bson.D{{Key: "$exists", Value: false}}}}
	if diff := cmp.Diff(expected0, clamped[0]); diff != "" {
		t.Errorf("partition 0 mismatch (-want +got):\n%s", diff)
	}

	// Partition 1: oid1 < oid2 < oid3 -> clamped to $gte: oid2, $lt: oid3
	expected1 := bson.D{{Key: "_id", Value: bson.D{
		{Key: "$type", Value: "objectId"},
		{Key: "$gte", Value: oid2},
		{Key: "$lt", Value: oid3},
	}}}
	if diff := cmp.Diff(expected1, clamped[1]); diff != "" {
		t.Errorf("partition 1 mismatch (-want +got):\n%s", diff)
	}

	// Partition 2: >= oid3 > oid2 -> untouched
	expected2 := bson.D{{Key: "_id", Value: bson.D{
		{Key: "$type", Value: "objectId"},
		{Key: "$gte", Value: oid3},
	}}}
	if diff := cmp.Diff(expected2, clamped[2]); diff != "" {
		t.Errorf("partition 2 mismatch (-want +got):\n%s", diff)
	}
}

func TestClampPartitionsWithGlobalMinSafeIDs_UnpartitionedSingleFilter(t *testing.T) {
	oid, _ := primitive.ObjectIDFromHex("60a000000000000000000050")
	globalMinSafeIDs := map[BSONType]any{
		BSONTypeObjectID: oid,
		BSONTypeString:   "user_100",
	}

	clamped := ClampPartitionsWithGlobalMinSafeIDs([]bson.D{{}}, globalMinSafeIDs)

	if len(clamped) != 1 {
		t.Fatalf("expected 1 partition, got %d", len(clamped))
	}

	// Should produce $or with both types starting at their safe watermark
	expected := bson.D{
		{Key: "$or", Value: []bson.D{
			{{Key: "_id", Value: bson.D{{Key: "$type", Value: "objectId"}, {Key: "$gte", Value: oid}}}},
			{{Key: "_id", Value: bson.D{{Key: "$type", Value: "string"}, {Key: "$gte", Value: "user_100"}}}},
		}},
	}
	if diff := cmp.Diff(expected, clamped[0]); diff != "" {
		t.Errorf("unpartitioned filter clamp mismatch (-want +got):\n%s", diff)
	}
}

func TestIsFilterSkipped(t *testing.T) {
	skippedFilter := bson.D{{Key: "_id", Value: bson.D{{Key: "$exists", Value: false}}}}
	if !IsFilterSkipped(skippedFilter) {
		t.Errorf("expected IsFilterSkipped to return true for {_id: {$exists: false}}")
	}

	activeFilter := bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: "string"}, {Key: "$gte", Value: "abc"}}}}
	if IsFilterSkipped(activeFilter) {
		t.Errorf("expected IsFilterSkipped to return false for active filter")
	}

	emptyFilter := bson.D{}
	if IsFilterSkipped(emptyFilter) {
		t.Errorf("expected IsFilterSkipped to return false for empty filter")
	}
}

func TestExtractTypeRangeBoundariesFromFilter(t *testing.T) {
	oid1, _ := primitive.ObjectIDFromHex("60a000000000000000000010")
	oid2, _ := primitive.ObjectIDFromHex("60a000000000000000000020")

	// Multi-type $or filter:
	// - string: ["a", "z")
	// - objectId: [oid1, oid2)
	// - number: < 1000
	filter := bson.D{
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: "string"}, {Key: "$gte", Value: "a"}, {Key: "$lt", Value: "z"}}}},
			bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: "objectId"}, {Key: "$gte", Value: oid1}, {Key: "$lt", Value: oid2}}}},
			bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: "number"}, {Key: "$lt", Value: int64(1000)}}}},
		}},
	}

	boundaries := ExtractTypeRangeBoundariesFromFilter(filter)

	if len(boundaries) != 3 {
		t.Fatalf("expected 3 boundaries, got %d", len(boundaries))
	}

	// String boundary
	strB := boundaries[BSONTypeString]
	if strB == nil || strB.RangeStartID != "a" || strB.RangeEndID != "z" {
		t.Errorf("string boundary mismatch: %+v", strB)
	}

	// ObjectID boundary
	oidB := boundaries[BSONTypeObjectID]
	if oidB == nil || oidB.RangeStartID != oid1 || oidB.RangeEndID != oid2 {
		t.Errorf("objectID boundary mismatch: %+v", oidB)
	}

	// Number boundary (unbounded start)
	numB := boundaries[BSONTypeNumber]
	if numB == nil || numB.RangeStartID != nil || numB.RangeEndID != int64(1000) {
		t.Errorf("number boundary mismatch: %+v", numB)
	}
}

func TestBackfillPartitionTracker_MarkCompleted_CompletedPartitionSaved(t *testing.T) {
	tmpDir := t.TempDir()
	log := logger.New()

	cpPath := filepath.Join(tmpDir, "backfillCheckpoint-testdb-testcoll-partition-0-of-4.json")
	oidEnd, _ := primitive.ObjectIDFromHex("60a000000000000000000010")

	// Partition 0 is skipped because it was already completed (SavedLastID == RangeEndID)
	cp := &PartitionCheckpoint{
		Database:                "testdb",
		Collection:              "testcoll",
		PartitionIndex:          0,
		TotalSplits:             4,
		ApproximateDocsMigrated: 0,
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeObjectID: {
				BSONType:    BSONTypeObjectID,
				RangeEndID:  oidEnd,
				SavedLastID: oidEnd,
			},
		},
		UpdatedAt: time.Now().UTC(),
	}

	tracker := NewBackfillPartitionTracker(log, cp, cpPath, time.Minute, 100)
	tracker.Start(context.Background())

	// Simulate 0 batches read/acked, partition finishes immediately and calls MarkCompleted
	tracker.MarkCompleted()
	tracker.Close()

	// Verify checkpoint file is physically written to disk
	loadedCP, err := LoadPartitionCheckpoint(cpPath)
	if err != nil {
		t.Fatalf("expected checkpoint to be saved to disk, got err: %v", err)
	}
	if loadedCP == nil {
		t.Fatalf("expected non-nil loaded checkpoint")
	}
	if loadedCP.PartitionIndex != 0 || loadedCP.TotalSplits != 4 {
		t.Errorf("unexpected loaded checkpoint metadata: %+v", loadedCP)
	}
	if loadedCP.TypeProgress[BSONTypeObjectID].SavedLastID != oidEnd {
		t.Errorf("expected SavedLastID to be %v, got %v", oidEnd, loadedCP.TypeProgress[BSONTypeObjectID].SavedLastID)
	}
}
