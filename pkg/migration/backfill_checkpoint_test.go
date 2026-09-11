package migration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestBackfillCheckpoint_LoadNonExistentReturnsNil(t *testing.T) {
	tmpDir := t.TempDir()
	nonExistentPath := filepath.Join(tmpDir, "backfillCheckpoint-testdb-testcoll-partition-0-of-4.json")

	cp, err := LoadPartitionCheckpoint(nonExistentPath)

	if err != nil {
		t.Fatalf("unexpected error loading non-existent checkpoint: %v", err)
	}
	if cp != nil {
		t.Errorf("expected checkpoint to be nil, got %+v", cp)
	}
}

func TestBackfillCheckpoint_SaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	db := "testdb"
	coll := "testcoll"
	partitionIndex := 1
	totalSplits := 4
	filePath := GetPartitionCheckpointPath(tmpDir, db, coll, partitionIndex, totalSplits)
	oidStart, _ := primitive.ObjectIDFromHex("60a000000000000000000000")
	oidEnd, _ := primitive.ObjectIDFromHex("60a000000000000000000099")
	oidSaved, _ := primitive.ObjectIDFromHex("60a000000000000000000045")
	expectedTime := time.Date(2026, 8, 5, 14, 0, 0, 0, time.UTC)
	cp := &PartitionCheckpoint{
		Database:                db,
		Collection:              coll,
		PartitionIndex:          partitionIndex,
		TotalSplits:             totalSplits,
		ApproximateDocsMigrated: 45000,
		UpdatedAt:               expectedTime,
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeNumber: {
				BSONType:     BSONTypeNumber,
				RangeStartID: int64(100),
				RangeEndID:   int64(1000),
				SavedLastID:  int64(500),
			},
			BSONTypeObjectID: {
				BSONType:     BSONTypeObjectID,
				RangeStartID: oidStart,
				RangeEndID:   oidEnd,
				SavedLastID:  oidSaved,
			},
			BSONTypeString: {
				BSONType:     BSONTypeString,
				RangeStartID: "prefix_000",
				RangeEndID:   "prefix_999",
				SavedLastID:  "prefix_500",
			},
		},
	}

	if err := SavePartitionCheckpoint(filePath, cp); err != nil {
		t.Fatalf("failed to save partition checkpoint: %v", err)
	}
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Fatalf("expected checkpoint file to exist at %s, but it does not", filePath)
	}
	loaded, err := LoadPartitionCheckpoint(filePath)
	if err != nil {
		t.Fatalf("failed to load partition checkpoint: %v", err)
	}

	if loaded == nil {
		t.Fatalf("expected loaded checkpoint to be non-nil")
	}
	if diff := cmp.Diff(cp, loaded); diff != "" {
		t.Errorf("loaded checkpoint mismatch (-want +got):\n%s", diff)
	}
}

func TestBackfillCheckpoint_ExtendedJSONTypeFidelity(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "type-fidelity.json")

	oid, _ := primitive.ObjectIDFromHex("60a000000000000000000045")
	int64Val := int64(9876543210)
	int32Val := int32(12345)

	cp := &PartitionCheckpoint{
		Database:       "typedb",
		Collection:     "typecoll",
		PartitionIndex: 0,
		TotalSplits:    1,
		TypeProgress: map[BSONType]*TypeRangeBoundary{
			BSONTypeObjectID: {
				BSONType:    BSONTypeObjectID,
				SavedLastID: oid,
			},
			BSONTypeNumber: {
				BSONType:    BSONTypeNumber,
				SavedLastID: int64Val,
			},
			BSONTypeString: {
				BSONType:    BSONTypeString,
				SavedLastID: "sample-string-id",
			},
		},
	}

	if err := SavePartitionCheckpoint(filePath, cp); err != nil {
		t.Fatalf("failed to save checkpoint: %v", err)
	}

	loaded, err := LoadPartitionCheckpoint(filePath)
	if err != nil {
		t.Fatalf("failed to load checkpoint: %v", err)
	}

	// Verify ObjectID type preservation
	loadedOID, ok := loaded.TypeProgress[BSONTypeObjectID].SavedLastID.(primitive.ObjectID)
	if !ok {
		t.Fatalf("expected SavedLastID to be primitive.ObjectID, got %T (%v)", loaded.TypeProgress[BSONTypeObjectID].SavedLastID, loaded.TypeProgress[BSONTypeObjectID].SavedLastID)
	}
	if loadedOID != oid {
		t.Errorf("expected ObjectID %v, got %v", oid, loadedOID)
	}

	// Verify Int64 type preservation
	loadedInt64, ok := loaded.TypeProgress[BSONTypeNumber].SavedLastID.(int64)
	if !ok {
		t.Fatalf("expected SavedLastID to be int64, got %T (%v)", loaded.TypeProgress[BSONTypeNumber].SavedLastID, loaded.TypeProgress[BSONTypeNumber].SavedLastID)
	}
	if loadedInt64 != int64Val {
		t.Errorf("expected int64 %d, got %d", int64Val, loadedInt64)
	}

	// Verify String type preservation
	loadedStr, ok := loaded.TypeProgress[BSONTypeString].SavedLastID.(string)
	if !ok {
		t.Fatalf("expected SavedLastID to be string, got %T (%v)", loaded.TypeProgress[BSONTypeString].SavedLastID, loaded.TypeProgress[BSONTypeString].SavedLastID)
	}
	if loadedStr != "sample-string-id" {
		t.Errorf("expected string %q, got %q", "sample-string-id", loadedStr)
	}

	// Also verify int32 within number
	cp.TypeProgress[BSONTypeNumber].SavedLastID = int32Val
	if err := SavePartitionCheckpoint(filePath, cp); err != nil {
		t.Fatalf("failed to save checkpoint with int32: %v", err)
	}
	loadedInt32CP, err := LoadPartitionCheckpoint(filePath)
	if err != nil {
		t.Fatalf("failed to load checkpoint: %v", err)
	}
	loadedInt32, ok := loadedInt32CP.TypeProgress[BSONTypeNumber].SavedLastID.(int32)
	if !ok {
		t.Fatalf("expected SavedLastID to be int32, got %T (%v)", loadedInt32CP.TypeProgress[BSONTypeNumber].SavedLastID, loadedInt32CP.TypeProgress[BSONTypeNumber].SavedLastID)
	}
	if loadedInt32 != int32Val {
		t.Errorf("expected int32 %d, got %d", int32Val, loadedInt32)
	}
}

func TestBackfillCheckpoint_ListSorted(t *testing.T) {
	tmpDir := t.TempDir()
	db := "testdb"
	coll := "testcoll"
	totalSplits := 4

	// Write partitions in non-sequential order (3, 1, 0, 2)
	indices := []int{3, 1, 0, 2}
	for _, idx := range indices {
		filePath := GetPartitionCheckpointPath(tmpDir, db, coll, idx, totalSplits)
		cp := &PartitionCheckpoint{
			Database:       db,
			Collection:     coll,
			PartitionIndex: idx,
			TotalSplits:    totalSplits,
		}
		if err := SavePartitionCheckpoint(filePath, cp); err != nil {
			t.Fatalf("failed to save checkpoint for partition %d: %v", idx, err)
		}
	}

	// Write an unrelated checkpoint for a different collection
	unrelatedPath := GetPartitionCheckpointPath(tmpDir, db, "othercoll", 0, 1)
	unrelatedCP := &PartitionCheckpoint{
		Database:       db,
		Collection:     "othercoll",
		PartitionIndex: 0,
		TotalSplits:    1,
	}
	if err := SavePartitionCheckpoint(unrelatedPath, unrelatedCP); err != nil {
		t.Fatalf("failed to save unrelated checkpoint: %v", err)
	}

	// List checkpoints for testcoll
	checkpoints, err := ListPartitionCheckpoints(tmpDir, db, coll)
	if err != nil {
		t.Fatalf("failed to list partition checkpoints: %v", err)
	}

	if len(checkpoints) != totalSplits {
		t.Fatalf("expected %d checkpoints, got %d", totalSplits, len(checkpoints))
	}

	for expectedIdx, cp := range checkpoints {
		if cp.PartitionIndex != expectedIdx {
			t.Errorf("expected checkpoint at index %d to have PartitionIndex %d, got %d", expectedIdx, expectedIdx, cp.PartitionIndex)
		}
		if cp.Collection != coll {
			t.Errorf("expected collection %q, got %q", coll, cp.Collection)
		}
	}

	// List checkpoints for non-existent directory returns empty slice
	emptyCPs, err := ListPartitionCheckpoints(filepath.Join(tmpDir, "nonexistent"), db, coll)
	if err != nil {
		t.Fatalf("unexpected error listing non-existent directory: %v", err)
	}
	if len(emptyCPs) != 0 {
		t.Errorf("expected 0 checkpoints for non-existent directory, got %d", len(emptyCPs))
	}
}

func TestBackfillCheckpoint_DeletePartitionCheckpoints(t *testing.T) {
	tmpDir := t.TempDir()
	db := "testdb"
	collA := "collA"
	collB := "collB"

	// Create checkpoints for collA
	for i := 0; i < 2; i++ {
		path := GetPartitionCheckpointPath(tmpDir, db, collA, i, 2)
		cp := &PartitionCheckpoint{Database: db, Collection: collA, PartitionIndex: i, TotalSplits: 2}
		if err := SavePartitionCheckpoint(path, cp); err != nil {
			t.Fatalf("failed to save checkpoint: %v", err)
		}
	}

	// Create checkpoints for collB
	for i := 0; i < 2; i++ {
		path := GetPartitionCheckpointPath(tmpDir, db, collB, i, 2)
		cp := &PartitionCheckpoint{Database: db, Collection: collB, PartitionIndex: i, TotalSplits: 2}
		if err := SavePartitionCheckpoint(path, cp); err != nil {
			t.Fatalf("failed to save checkpoint: %v", err)
		}
	}

	// Delete collA checkpoints
	if err := DeletePartitionCheckpoints(tmpDir, db, collA); err != nil {
		t.Fatalf("failed to delete collA checkpoints: %v", err)
	}

	// collA should have 0 checkpoints
	collACheckpoints, err := ListPartitionCheckpoints(tmpDir, db, collA)
	if err != nil {
		t.Fatalf("failed to list collA checkpoints: %v", err)
	}
	if len(collACheckpoints) != 0 {
		t.Errorf("expected 0 collA checkpoints, got %d", len(collACheckpoints))
	}

	// collB should still have 2 checkpoints
	collBCheckpoints, err := ListPartitionCheckpoints(tmpDir, db, collB)
	if err != nil {
		t.Fatalf("failed to list collB checkpoints: %v", err)
	}
	if len(collBCheckpoints) != 2 {
		t.Errorf("expected 2 collB checkpoints, got %d", len(collBCheckpoints))
	}

	// Deleting in non-existent directory succeeds with nil error
	if err := DeletePartitionCheckpoints(filepath.Join(tmpDir, "nonexistent"), db, collA); err != nil {
		t.Errorf("unexpected error deleting non-existent dir: %v", err)
	}
}

func TestBackfillCheckpoint_LoadInvalidJSONReturnsError(t *testing.T) {
	tmpDir := t.TempDir()
	invalidJSONPath := filepath.Join(tmpDir, "invalid.json")

	if err := os.WriteFile(invalidJSONPath, []byte("{invalid-json:"), 0644); err != nil {
		t.Fatalf("failed to write invalid JSON file: %v", err)
	}

	cp, err := LoadPartitionCheckpoint(invalidJSONPath)
	if err == nil {
		t.Fatalf("expected unmarshal error loading invalid JSON checkpoint, got nil")
	}
	if cp != nil {
		t.Errorf("expected nil checkpoint on error, got %+v", cp)
	}
}

func TestBackfillCheckpoint_SaveNilReturnsError(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "nil-test.json")

	err := SavePartitionCheckpoint(path, nil)
	if err == nil {
		t.Fatalf("expected error when saving nil checkpoint, got nil")
	}
}

func TestGetBSONType(t *testing.T) {
	oid, _ := primitive.ObjectIDFromHex("60a000000000000000000045")
	now := time.Now().UTC()
	dt := primitive.NewDateTimeFromTime(now)
	ts := primitive.Timestamp{T: 12345, I: 1}
	bin := primitive.Binary{Subtype: 0x00, Data: []byte{0x01, 0x02, 0x03}}
	dec, _ := primitive.ParseDecimal128("123.456")

	tests := []struct {
		name     string
		input    any
		expected BSONType
	}{
		{"Nil", nil, ""},
		{"ObjectID", oid, BSONTypeObjectID},
		{"String", "hello world", BSONTypeString},
		{"Int", int(42), BSONTypeNumber},
		{"Int32", int32(42), BSONTypeNumber},
		{"Int64", int64(42), BSONTypeNumber},
		{"Float32", float32(42.5), BSONTypeNumber},
		{"Float64", float64(42.5), BSONTypeNumber},
		{"Decimal128", dec, BSONTypeNumber},
		{"Time", now, BSONTypeDate},
		{"PrimitiveDateTime", dt, BSONTypeDate},
		{"Timestamp", ts, BSONTypeTimestamp},
		{"Binary", bin, BSONTypeBinary},
		{"ByteSlice", []byte{0x01, 0x02}, BSONTypeBinary},
		{"BoolTrue", true, BSONTypeBool},
		{"BoolFalse", false, BSONTypeBool},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetBSONType(tt.input)
			if got != tt.expected {
				t.Errorf("GetBSONType(%v) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestDiscoverPresentBSONTypes_NilCollection(t *testing.T) {
	_, err := DiscoverPresentBSONTypes(context.Background(), nil)
	if err == nil {
		t.Fatalf("expected error when discovering BSON types on nil collection, got nil")
	}
}

func TestDiscoverPresentBSONTypeCounts_NilCollection(t *testing.T) {
	_, err := DiscoverPresentBSONTypeCounts(context.Background(), nil, 2000)
	if err == nil {
		t.Fatalf("expected error when discovering BSON type counts on nil collection, got nil")
	}
}

func TestParseBSONType(t *testing.T) {
	tests := []struct {
		input    string
		expected BSONType
	}{
		{"int", BSONTypeNumber},
		{"long", BSONTypeNumber},
		{"double", BSONTypeNumber},
		{"decimal", BSONTypeNumber},
		{"objectId", BSONTypeObjectID},
		{"string", BSONTypeString},
		{"binData", BSONTypeBinary},
		{"date", BSONTypeDate},
		{"timestamp", BSONTypeTimestamp},
		{"bool", BSONTypeBool},
		{"custom_unknown", BSONType("custom_unknown")},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ParseBSONType(tt.input)
			if got != tt.expected {
				t.Errorf("ParseBSONType(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

