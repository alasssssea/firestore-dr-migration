package migration

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestBackfillResumption_ValidateCheckpointSet(t *testing.T) {
	t.Run("ValidCompleteSet", func(t *testing.T) {
		cps := []*PartitionCheckpoint{
			{
				PartitionIndex: 0,
				TotalSplits:    3,
				TypeProgress: map[BSONType]*TypeRangeBoundary{
					BSONTypeObjectID: {BSONType: BSONTypeObjectID, SavedLastID: primitive.NewObjectID()},
				},
			},
			{
				PartitionIndex: 1,
				TotalSplits:    3,
				TypeProgress: map[BSONType]*TypeRangeBoundary{
					BSONTypeObjectID: {BSONType: BSONTypeObjectID, SavedLastID: primitive.NewObjectID()},
				},
			},
			{
				PartitionIndex: 2,
				TotalSplits:    3,
				TypeProgress: map[BSONType]*TypeRangeBoundary{
					BSONTypeObjectID: {BSONType: BSONTypeObjectID, SavedLastID: primitive.NewObjectID()},
				},
			},
		}

		if !ValidateCheckpointSet(cps, 3) {
			t.Errorf("expected ValidateCheckpointSet to return true for valid 3-partition set")
		}
	})

	t.Run("IncompleteCountMismatch", func(t *testing.T) {
		cps := []*PartitionCheckpoint{
			{
				PartitionIndex: 0,
				TotalSplits:    4,
				TypeProgress:   map[BSONType]*TypeRangeBoundary{BSONTypeObjectID: {BSONType: BSONTypeObjectID}},
			},
			{
				PartitionIndex: 1,
				TotalSplits:    4,
				TypeProgress:   map[BSONType]*TypeRangeBoundary{BSONTypeObjectID: {BSONType: BSONTypeObjectID}},
			},
		}

		if ValidateCheckpointSet(cps, 4) {
			t.Errorf("expected ValidateCheckpointSet to return false when only 2 of 4 checkpoints present")
		}
	})

	t.Run("MixedTotalSplitsFromStaleRuns", func(t *testing.T) {
		cps := []*PartitionCheckpoint{
			{
				PartitionIndex: 0,
				TotalSplits:    3,
				TypeProgress:   map[BSONType]*TypeRangeBoundary{BSONTypeObjectID: {BSONType: BSONTypeObjectID}},
			},
			{
				PartitionIndex: 1,
				TotalSplits:    3,
				TypeProgress:   map[BSONType]*TypeRangeBoundary{BSONTypeObjectID: {BSONType: BSONTypeObjectID}},
			},
			{
				PartitionIndex: 2,
				TotalSplits:    8, // Stale leftover file with 8 splits
				TypeProgress:   map[BSONType]*TypeRangeBoundary{BSONTypeObjectID: {BSONType: BSONTypeObjectID}},
			},
		}

		if ValidateCheckpointSet(cps, 3) {
			t.Errorf("expected ValidateCheckpointSet to return false when checkpoint files have mixed TotalSplits")
		}
	})

	t.Run("NonContiguousPartitionIndices", func(t *testing.T) {
		cps := []*PartitionCheckpoint{
			{
				PartitionIndex: 0,
				TotalSplits:    3,
				TypeProgress:   map[BSONType]*TypeRangeBoundary{BSONTypeObjectID: {BSONType: BSONTypeObjectID}},
			},
			{
				PartitionIndex: 2, // Gaps in index (missing index 1)
				TotalSplits:    3,
				TypeProgress:   map[BSONType]*TypeRangeBoundary{BSONTypeObjectID: {BSONType: BSONTypeObjectID}},
			},
			{
				PartitionIndex: 3,
				TotalSplits:    3,
				TypeProgress:   map[BSONType]*TypeRangeBoundary{BSONTypeObjectID: {BSONType: BSONTypeObjectID}},
			},
		}

		if ValidateCheckpointSet(cps, 3) {
			t.Errorf("expected ValidateCheckpointSet to return false for non-contiguous indices")
		}
	})

	t.Run("NilOrEmptyTypeProgress", func(t *testing.T) {
		cps := []*PartitionCheckpoint{
			{
				PartitionIndex: 0,
				TotalSplits:    1,
				TypeProgress:   nil,
			},
		}

		if ValidateCheckpointSet(cps, 1) {
			t.Errorf("expected ValidateCheckpointSet to return false when TypeProgress is nil")
		}

		cps[0].TypeProgress = make(map[BSONType]*TypeRangeBoundary)
		if ValidateCheckpointSet(cps, 1) {
			t.Errorf("expected ValidateCheckpointSet to return false when TypeProgress is empty")
		}
	})

	t.Run("NilCheckpointInSlice", func(t *testing.T) {
		cps := []*PartitionCheckpoint{nil}
		if ValidateCheckpointSet(cps, 1) {
			t.Errorf("expected ValidateCheckpointSet to return false for nil checkpoint")
		}
	})
}

func TestBackfillResumption_BuildPartitionFilter_SingleTypeObjectID(t *testing.T) {
	oidStart, _ := primitive.ObjectIDFromHex("60a000000000000000000000")
	oidSaved, _ := primitive.ObjectIDFromHex("60a000000000000000000045")
	oidEnd, _ := primitive.ObjectIDFromHex("60a000000000000000000099")

	t.Run("UnstartedPartition0", func(t *testing.T) {
		cp := &PartitionCheckpoint{
			PartitionIndex: 0,
			TotalSplits:    4,
			TypeProgress: map[BSONType]*TypeRangeBoundary{
				BSONTypeObjectID: {
					BSONType:   BSONTypeObjectID,
					RangeEndID: oidEnd,
				},
			},
		}

		filter, err := BuildPartitionFilterFromCheckpoint(cp)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "$type", Value: "objectId"},
				{Key: "$lt", Value: oidEnd},
			}},
		}

		if diff := cmp.Diff(expected, filter); diff != "" {
			t.Errorf("unstarted partition 0 filter mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("ResumedPartition0WithSavedLastID", func(t *testing.T) {
		cp := &PartitionCheckpoint{
			PartitionIndex: 0,
			TotalSplits:    4,
			TypeProgress: map[BSONType]*TypeRangeBoundary{
				BSONTypeObjectID: {
					BSONType:    BSONTypeObjectID,
					RangeEndID:  oidEnd,
					SavedLastID: oidSaved,
				},
			},
		}

		filter, err := BuildPartitionFilterFromCheckpoint(cp)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "$type", Value: "objectId"},
				{Key: "$gte", Value: oidSaved},
				{Key: "$lt", Value: oidEnd},
			}},
		}

		if diff := cmp.Diff(expected, filter); diff != "" {
			t.Errorf("resumed partition 0 filter mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("MiddlePartitionWithProgress", func(t *testing.T) {
		cp := &PartitionCheckpoint{
			PartitionIndex: 1,
			TotalSplits:    4,
			TypeProgress: map[BSONType]*TypeRangeBoundary{
				BSONTypeObjectID: {
					BSONType:     BSONTypeObjectID,
					RangeStartID: oidStart,
					RangeEndID:   oidEnd,
					SavedLastID:  oidSaved,
				},
			},
		}

		filter, err := BuildPartitionFilterFromCheckpoint(cp)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "$type", Value: "objectId"},
				{Key: "$gte", Value: oidSaved},
				{Key: "$lt", Value: oidEnd},
			}},
		}

		if diff := cmp.Diff(expected, filter); diff != "" {
			t.Errorf("middle partition filter mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("ResumedLastPartitionUnboundedEnd", func(t *testing.T) {
		cp := &PartitionCheckpoint{
			PartitionIndex: 3,
			TotalSplits:    4,
			TypeProgress: map[BSONType]*TypeRangeBoundary{
				BSONTypeObjectID: {
					BSONType:     BSONTypeObjectID,
					RangeStartID: oidStart,
					SavedLastID:  oidSaved,
				},
			},
		}

		filter, err := BuildPartitionFilterFromCheckpoint(cp)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "$type", Value: "objectId"},
				{Key: "$gte", Value: oidSaved},
			}},
		}

		if diff := cmp.Diff(expected, filter); diff != "" {
			t.Errorf("last partition filter mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("CompletedPartitionMatchesZeroDocs", func(t *testing.T) {
		cp := &PartitionCheckpoint{
			PartitionIndex: 1,
			TotalSplits:    4,
			TypeProgress: map[BSONType]*TypeRangeBoundary{
				BSONTypeObjectID: {
					BSONType:     BSONTypeObjectID,
					RangeStartID: oidStart,
					RangeEndID:   oidEnd,
					SavedLastID:  oidEnd, // SavedLastID reached RangeEndID
				},
			},
		}

		filter, err := BuildPartitionFilterFromCheckpoint(cp)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := bson.D{
			{Key: "_id", Value: bson.D{{Key: "$exists", Value: false}}},
		}

		if diff := cmp.Diff(expected, filter); diff != "" {
			t.Errorf("completed partition filter mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("NilCheckpointReturnsError", func(t *testing.T) {
		_, err := BuildPartitionFilterFromCheckpoint(nil)
		if err == nil {
			t.Errorf("expected error for nil checkpoint, got nil")
		}
	})

	t.Run("EmptyTypeProgressReturnsError", func(t *testing.T) {
		cp := &PartitionCheckpoint{
			PartitionIndex: 0,
			TotalSplits:    1,
			TypeProgress:   map[BSONType]*TypeRangeBoundary{},
		}
		_, err := BuildPartitionFilterFromCheckpoint(cp)
		if err == nil {
			t.Errorf("expected error for checkpoint with empty type progress, got nil")
		}
	})

	t.Run("MissingBothBoundsReturnsErrorWhenSplitsGreaterThanOne", func(t *testing.T) {
		cp := &PartitionCheckpoint{
			PartitionIndex: 0,
			TotalSplits:    3,
			TypeProgress: map[BSONType]*TypeRangeBoundary{
				BSONTypeObjectID: {
					BSONType:     BSONTypeObjectID,
					SavedLastID:  oidSaved,
					RangeStartID: nil,
					RangeEndID:   nil, // Both missing when TotalSplits = 3
				},
			},
		}
		_, err := BuildPartitionFilterFromCheckpoint(cp)
		if err == nil {
			t.Errorf("expected error for partition missing both start and end bounds when TotalSplits > 1, got nil")
		}
	})
}

func TestBackfillResumption_BuildPartitionFilter_MixedTypeOr(t *testing.T) {
	oidStart, _ := primitive.ObjectIDFromHex("60a000000000000000000000")
	oidSaved, _ := primitive.ObjectIDFromHex("60a000000000000000000045")
	oidEnd, _ := primitive.ObjectIDFromHex("60a000000000000000000099")

	cp := &PartitionCheckpoint{
		PartitionIndex: 1,
		TotalSplits:    2,
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

	filter, err := BuildPartitionFilterFromCheckpoint(cp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Keys sorted deterministically: "number", "objectId", "string"
	expected := bson.D{
		{Key: "$or", Value: []bson.D{
			{
				{Key: "_id", Value: bson.D{
					{Key: "$type", Value: "number"},
					{Key: "$gte", Value: int64(500)},
					{Key: "$lt", Value: int64(1000)},
				}},
			},
			{
				{Key: "_id", Value: bson.D{
					{Key: "$type", Value: "objectId"},
					{Key: "$gte", Value: oidSaved},
					{Key: "$lt", Value: oidEnd},
				}},
			},
			{
				{Key: "_id", Value: bson.D{
					{Key: "$type", Value: "string"},
					{Key: "$gte", Value: "prefix_500"},
					{Key: "$lt", Value: "prefix_999"},
				}},
			},
		}},
	}

	if diff := cmp.Diff(expected, filter); diff != "" {
		t.Errorf("mixed type filter mismatch (-want +got):\n%s", diff)
	}
}

func TestBackfillResumption_CompareBSONValues(t *testing.T) {
	t.Run("ObjectID", func(t *testing.T) {
		oid1, _ := primitive.ObjectIDFromHex("60a000000000000000000001")
		oid2, _ := primitive.ObjectIDFromHex("60a000000000000000000002")

		cmpVal, err := CompareBSONValues(oid1, oid2)
		if err != nil || cmpVal >= 0 {
			t.Errorf("expected oid1 < oid2, got cmp=%d err=%v", cmpVal, err)
		}

		cmpVal, err = CompareBSONValues(oid1, oid1)
		if err != nil || cmpVal != 0 {
			t.Errorf("expected oid1 == oid1, got cmp=%d err=%v", cmpVal, err)
		}
	})

	t.Run("Numbers", func(t *testing.T) {
		// int64 vs int64
		cmpVal, err := CompareBSONValues(int64(9), int64(10))
		if err != nil || cmpVal >= 0 {
			t.Errorf("expected 9 < 10, got cmp=%d err=%v", cmpVal, err)
		}

		// int32 vs int64
		cmpVal, err = CompareBSONValues(int32(100), int64(100))
		if err != nil || cmpVal != 0 {
			t.Errorf("expected int32(100) == int64(100), got cmp=%d err=%v", cmpVal, err)
		}

		// float64 vs int64
		cmpVal, err = CompareBSONValues(float64(50.5), int64(50))
		if err != nil || cmpVal <= 0 {
			t.Errorf("expected 50.5 > 50, got cmp=%d err=%v", cmpVal, err)
		}
	})

	t.Run("String", func(t *testing.T) {
		cmpVal, err := CompareBSONValues("alpha", "beta")
		if err != nil || cmpVal >= 0 {
			t.Errorf("expected alpha < beta, got cmp=%d err=%v", cmpVal, err)
		}
	})

	t.Run("DateTimeAndTime", func(t *testing.T) {
		time1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		time2 := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)

		cmpVal, err := CompareBSONValues(time1, time2)
		if err != nil || cmpVal >= 0 {
			t.Errorf("expected time1 < time2, got cmp=%d err=%v", cmpVal, err)
		}

		dt1 := primitive.NewDateTimeFromTime(time1)
		dt2 := primitive.NewDateTimeFromTime(time2)
		cmpVal, err = CompareBSONValues(dt1, dt2)
		if err != nil || cmpVal >= 0 {
			t.Errorf("expected dt1 < dt2, got cmp=%d err=%v", cmpVal, err)
		}
	})

	t.Run("Timestamp", func(t *testing.T) {
		ts1 := primitive.Timestamp{T: 100, I: 1}
		ts2 := primitive.Timestamp{T: 100, I: 2}
		ts3 := primitive.Timestamp{T: 200, I: 1}

		cmpVal, err := CompareBSONValues(ts1, ts2)
		if err != nil || cmpVal >= 0 {
			t.Errorf("expected ts1 < ts2, got cmp=%d err=%v", cmpVal, err)
		}

		cmpVal, err = CompareBSONValues(ts2, ts3)
		if err != nil || cmpVal >= 0 {
			t.Errorf("expected ts2 < ts3, got cmp=%d err=%v", cmpVal, err)
		}
	})

	t.Run("Binary", func(t *testing.T) {
		bin1 := primitive.Binary{Data: []byte{0x01, 0x02}}
		bin2 := primitive.Binary{Data: []byte{0x01, 0x03}}

		cmpVal, err := CompareBSONValues(bin1, bin2)
		if err != nil || cmpVal >= 0 {
			t.Errorf("expected bin1 < bin2, got cmp=%d err=%v", cmpVal, err)
		}
	})

	t.Run("Bool", func(t *testing.T) {
		cmpVal, err := CompareBSONValues(false, true)
		if err != nil || cmpVal >= 0 {
			t.Errorf("expected false < true, got cmp=%d err=%v", cmpVal, err)
		}
	})

	t.Run("NilValues", func(t *testing.T) {
		cmpVal, err := CompareBSONValues(nil, nil)
		if err != nil || cmpVal != 0 {
			t.Errorf("expected nil == nil, got cmp=%d err=%v", cmpVal, err)
		}

		cmpVal, err = CompareBSONValues(nil, "something")
		if err != nil || cmpVal >= 0 {
			t.Errorf("expected nil < something, got cmp=%d err=%v", cmpVal, err)
		}
	})
}

func TestBackfillResumption_ExtractGlobalMinSafeIDs(t *testing.T) {
	oid1, _ := primitive.ObjectIDFromHex("60a000000000000000000010")
	oid2, _ := primitive.ObjectIDFromHex("60a000000000000000000020")
	oid3, _ := primitive.ObjectIDFromHex("60a000000000000000000030")

	t.Run("AllPartitionsHaveProgress", func(t *testing.T) {
		cps := []*PartitionCheckpoint{
			{
				PartitionIndex: 0,
				TotalSplits:    3,
				TypeProgress: map[BSONType]*TypeRangeBoundary{
					BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeEndID: oid2, SavedLastID: oid1},
					BSONTypeNumber:   {BSONType: BSONTypeNumber, RangeEndID: int64(200), SavedLastID: int64(100)},
				},
			},
			{
				PartitionIndex: 1,
				TotalSplits:    3,
				TypeProgress: map[BSONType]*TypeRangeBoundary{
					BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeStartID: oid2, RangeEndID: oid3, SavedLastID: oid2},
					BSONTypeNumber:   {BSONType: BSONTypeNumber, RangeStartID: int64(200), RangeEndID: int64(300), SavedLastID: int64(250)},
				},
			},
			{
				PartitionIndex: 2,
				TotalSplits:    3,
				TypeProgress: map[BSONType]*TypeRangeBoundary{
					BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeStartID: oid3, SavedLastID: oid3},
					BSONTypeNumber:   {BSONType: BSONTypeNumber, RangeStartID: int64(300), SavedLastID: int64(300)},
				},
			},
		}

		globalMins, err := ExtractGlobalMinSafeIDs(cps)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if globalMins[BSONTypeObjectID] != oid1 {
			t.Errorf("expected global min ObjectID %v, got %v", oid1, globalMins[BSONTypeObjectID])
		}
		if globalMins[BSONTypeNumber] != int64(100) {
			t.Errorf("expected global min Number 100, got %v", globalMins[BSONTypeNumber])
		}
	})

	t.Run("Partition0UnstartedReturnsNil", func(t *testing.T) {
		cps := []*PartitionCheckpoint{
			{
				PartitionIndex: 0,
				TotalSplits:    2,
				TypeProgress: map[BSONType]*TypeRangeBoundary{
					BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeEndID: oid2}, // Unstarted, RangeStartID & SavedLastID are nil
				},
			},
			{
				PartitionIndex: 1,
				TotalSplits:    2,
				TypeProgress: map[BSONType]*TypeRangeBoundary{
					BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeStartID: oid2, SavedLastID: oid3},
				},
			},
		}

		globalMins, err := ExtractGlobalMinSafeIDs(cps)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if globalMins[BSONTypeObjectID] != nil {
			t.Errorf("expected global min ObjectID to be nil when partition 0 is unstarted, got %v", globalMins[BSONTypeObjectID])
		}
	})

	t.Run("MixedTypesWithPartialProgress", func(t *testing.T) {
		cps := []*PartitionCheckpoint{
			{
				PartitionIndex: 0,
				TotalSplits:    2,
				TypeProgress: map[BSONType]*TypeRangeBoundary{
					BSONTypeNumber:   {BSONType: BSONTypeNumber, RangeEndID: int64(100), SavedLastID: int64(100)}, // Completed
					BSONTypeString:   {BSONType: BSONTypeString, RangeEndID: "z", SavedLastID: "a"},               // Partially completed
					BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeEndID: oid2},                              // Unstarted
				},
			},
			{
				PartitionIndex: 1,
				TotalSplits:    2,
				TypeProgress: map[BSONType]*TypeRangeBoundary{
					BSONTypeNumber:   {BSONType: BSONTypeNumber, RangeStartID: int64(100), SavedLastID: int64(150)},
					BSONTypeString:   {BSONType: BSONTypeString, RangeStartID: "z", SavedLastID: "m"},
					BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeStartID: oid2, SavedLastID: oid3},
				},
			},
		}

		globalMins, err := ExtractGlobalMinSafeIDs(cps)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Number global min is 150 (partition 0 completed up to 100 and is ignored; active partition 1 reached 150)
		if globalMins[BSONTypeNumber] != int64(150) {
			t.Errorf("expected GlobalMin Number 150, got %v", globalMins[BSONTypeNumber])
		}
		// String global min is "a" (partition 0 is currently at "a")
		if globalMins[BSONTypeString] != "a" {
			t.Errorf("expected GlobalMin String 'a', got %v", globalMins[BSONTypeString])
		}
		// ObjectID global min is nil (partition 0 has not started objectIds, so scan from beginning of objectIds)
		if globalMins[BSONTypeObjectID] != nil {
			t.Errorf("expected GlobalMin ObjectID nil (unbounded start), got %v", globalMins[BSONTypeObjectID])
		}
	})

	t.Run("EmptyCheckpointsReturnsError", func(t *testing.T) {
		_, err := ExtractGlobalMinSafeIDs(nil)
		if err == nil {
			t.Errorf("expected error for empty checkpoints, got nil")
		}
	})

	t.Run("NilCheckpointInSliceReturnsError", func(t *testing.T) {
		cps := []*PartitionCheckpoint{nil}
		_, err := ExtractGlobalMinSafeIDs(cps)
		if err == nil {
			t.Errorf("expected error for slice containing nil checkpoint, got nil")
		}
	})
}

func TestBackfillResumption_DetermineBackfillResumptionPlan(t *testing.T) {
	db := "testdb"
	coll := "testcoll"

	t.Run("EmptyDirReturnsFresh", func(t *testing.T) {
		tmpDir := t.TempDir()
		plan, err := DetermineBackfillResumptionPlan(tmpDir, db, coll, 4)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if plan.Mode != ResumptionModeFresh {
			t.Errorf("expected ResumptionModeFresh for empty dir, got %v", plan.Mode)
		}
	})

	t.Run("MatchingPartitionsDirectResumption", func(t *testing.T) {
		tmpDir := t.TempDir()
		totalSplits := 2
		oid1, _ := primitive.ObjectIDFromHex("60a000000000000000000010")
		oid2, _ := primitive.ObjectIDFromHex("60a000000000000000000050")

		cp0 := &PartitionCheckpoint{
			Database:                db,
			Collection:              coll,
			PartitionIndex:          0,
			TotalSplits:             totalSplits,
			ApproximateDocsMigrated: 1000,
			TypeProgress: map[BSONType]*TypeRangeBoundary{
				BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeEndID: oid2, SavedLastID: oid1},
			},
		}
		cp1 := &PartitionCheckpoint{
			Database:                db,
			Collection:              coll,
			PartitionIndex:          1,
			TotalSplits:             totalSplits,
			ApproximateDocsMigrated: 2000,
			TypeProgress: map[BSONType]*TypeRangeBoundary{
				BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeStartID: oid2, SavedLastID: oid2},
			},
		}

		_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, db, coll, 0, totalSplits), cp0)
		_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, db, coll, 1, totalSplits), cp1)

		plan, err := DetermineBackfillResumptionPlan(tmpDir, db, coll, 2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if plan.Mode != ResumptionModeDirect {
			t.Errorf("expected ResumptionModeDirect, got %v", plan.Mode)
		}
		if plan.TotalDocsMigrated() != 3000 {
			t.Errorf("expected TotalDocsMigrated to be 3000, got %d", plan.TotalDocsMigrated())
		}
		if len(plan.PartitionInitialDocs) != 2 || plan.PartitionInitialDocs[0] != 1000 || plan.PartitionInitialDocs[1] != 2000 {
			t.Errorf("expected PartitionInitialDocs [1000, 2000], got %v", plan.PartitionInitialDocs)
		}
		if len(plan.PartitionFilters) != 2 {
			t.Fatalf("expected 2 partition filters, got %d", len(plan.PartitionFilters))
		}
	})

	t.Run("MatchingPartitionsSubsetCompleted", func(t *testing.T) {
		tmpDir := t.TempDir()
		totalSplits := 2
		oid1, _ := primitive.ObjectIDFromHex("60a000000000000000000010")
		oid2, _ := primitive.ObjectIDFromHex("60a000000000000000000050")

		// Partition 0 is 100% COMPLETED: SavedLastID reached RangeEndID (oid2)
		cp0 := &PartitionCheckpoint{
			Database:                db,
			Collection:              coll,
			PartitionIndex:          0,
			TotalSplits:             totalSplits,
			ApproximateDocsMigrated: 1000,
			TypeProgress: map[BSONType]*TypeRangeBoundary{
				BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeEndID: oid2, SavedLastID: oid2},
			},
		}
		// Partition 1 is partially completed
		cp1 := &PartitionCheckpoint{
			Database:                db,
			Collection:              coll,
			PartitionIndex:          1,
			TotalSplits:             totalSplits,
			ApproximateDocsMigrated: 500,
			TypeProgress: map[BSONType]*TypeRangeBoundary{
				BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeStartID: oid2, SavedLastID: oid1},
			},
		}

		_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, db, coll, 0, totalSplits), cp0)
		_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, db, coll, 1, totalSplits), cp1)

		plan, err := DetermineBackfillResumptionPlan(tmpDir, db, coll, 2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if plan.Mode != ResumptionModeDirect {
			t.Errorf("expected ResumptionModeDirect, got %v", plan.Mode)
		}
		if plan.TotalDocsMigrated() != 1500 {
			t.Errorf("expected TotalDocsMigrated 1500, got %d", plan.TotalDocsMigrated())
		}
		// Partition 0 filter must match 0 documents because it is completed
		expectedFilter0 := bson.D{{Key: "_id", Value: bson.D{{Key: "$exists", Value: false}}}}
		if diff := cmp.Diff(expectedFilter0, plan.PartitionFilters[0]); diff != "" {
			t.Errorf("partition 0 completed filter mismatch (-want +got):\n%s", diff)
		}
		// Partition 1 filter must resume with $gte: oid1
		expectedFilter1 := bson.D{{Key: "_id", Value: bson.D{
			{Key: "$type", Value: "objectId"},
			{Key: "$gte", Value: oid1},
		}}}
		if diff := cmp.Diff(expectedFilter1, plan.PartitionFilters[1]); diff != "" {
			t.Errorf("partition 1 filter mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("ReconfiguredPartitionCountResamplesWithGlobalMin", func(t *testing.T) {
		tmpDir := t.TempDir()
		historicalSplits := 2
		oid1, _ := primitive.ObjectIDFromHex("60a000000000000000000010")
		oid2, _ := primitive.ObjectIDFromHex("60a000000000000000000050")

		cp0 := &PartitionCheckpoint{
			Database:                db,
			Collection:              coll,
			PartitionIndex:          0,
			TotalSplits:             historicalSplits,
			ApproximateDocsMigrated: 1500,
			TypeProgress: map[BSONType]*TypeRangeBoundary{
				BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeEndID: oid2, SavedLastID: oid1},
			},
		}
		cp1 := &PartitionCheckpoint{
			Database:                db,
			Collection:              coll,
			PartitionIndex:          1,
			TotalSplits:             historicalSplits,
			ApproximateDocsMigrated: 2500,
			TypeProgress: map[BSONType]*TypeRangeBoundary{
				BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeStartID: oid2, SavedLastID: oid2},
			},
		}

		_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, db, coll, 0, historicalSplits), cp0)
		_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, db, coll, 1, historicalSplits), cp1)

		// New run expects 4 splits (changed from 2)
		plan, err := DetermineBackfillResumptionPlan(tmpDir, db, coll, 4)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if plan.Mode != ResumptionModeResampleWithGlobalMin {
			t.Errorf("expected ResumptionModeResampleWithGlobalMin, got %v", plan.Mode)
		}
		if plan.TotalDocsMigrated() != 4000 {
			t.Errorf("expected TotalDocsMigrated to be 4000, got %d", plan.TotalDocsMigrated())
		}
		if len(plan.PartitionInitialDocs) != 2 || plan.PartitionInitialDocs[0] != 1500 || plan.PartitionInitialDocs[1] != 2500 {
			t.Errorf("expected PartitionInitialDocs [1500, 2500], got %v", plan.PartitionInitialDocs)
		}
		if plan.GlobalMinSafeIDs[BSONTypeObjectID] != oid1 {
			t.Errorf("expected GlobalMinSafeID %v, got %v", oid1, plan.GlobalMinSafeIDs[BSONTypeObjectID])
		}
	})

	t.Run("ReconfiguredPartitionCountMixedTypes", func(t *testing.T) {
		tmpDir := t.TempDir()
		historicalSplits := 2
		oid1, _ := primitive.ObjectIDFromHex("60a000000000000000000010")
		oid2, _ := primitive.ObjectIDFromHex("60a000000000000000000050")
		oid3, _ := primitive.ObjectIDFromHex("60a000000000000000000090")

		cp0 := &PartitionCheckpoint{
			Database:                db,
			Collection:              coll,
			PartitionIndex:          0,
			TotalSplits:             historicalSplits,
			ApproximateDocsMigrated: 1200,
			TypeProgress: map[BSONType]*TypeRangeBoundary{
				BSONTypeNumber:   {BSONType: BSONTypeNumber, RangeEndID: int64(500), SavedLastID: int64(250)},
				BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeEndID: oid2, SavedLastID: oid1},
				BSONTypeString:   {BSONType: BSONTypeString, RangeEndID: "m", SavedLastID: "d"},
			},
		}
		cp1 := &PartitionCheckpoint{
			Database:                db,
			Collection:              coll,
			PartitionIndex:          1,
			TotalSplits:             historicalSplits,
			ApproximateDocsMigrated: 1800,
			TypeProgress: map[BSONType]*TypeRangeBoundary{
				BSONTypeNumber:   {BSONType: BSONTypeNumber, RangeStartID: int64(500), SavedLastID: int64(750)},
				BSONTypeObjectID: {BSONType: BSONTypeObjectID, RangeStartID: oid2, SavedLastID: oid3},
				BSONTypeString:   {BSONType: BSONTypeString, RangeStartID: "m", SavedLastID: "t"},
			},
		}

		_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, db, coll, 0, historicalSplits), cp0)
		_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, db, coll, 1, historicalSplits), cp1)

		// Reconfigured to 4 splits
		plan, err := DetermineBackfillResumptionPlan(tmpDir, db, coll, 4)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if plan.Mode != ResumptionModeResampleWithGlobalMin {
			t.Errorf("expected ResumptionModeResampleWithGlobalMin, got %v", plan.Mode)
		}
		if plan.TotalDocsMigrated() != 3000 {
			t.Errorf("expected TotalDocsMigrated 3000, got %d", plan.TotalDocsMigrated())
		}
		if plan.GlobalMinSafeIDs[BSONTypeNumber] != int64(250) {
			t.Errorf("expected GlobalMin Number 250, got %v", plan.GlobalMinSafeIDs[BSONTypeNumber])
		}
		if plan.GlobalMinSafeIDs[BSONTypeObjectID] != oid1 {
			t.Errorf("expected GlobalMin ObjectID %v, got %v", oid1, plan.GlobalMinSafeIDs[BSONTypeObjectID])
		}
		if plan.GlobalMinSafeIDs[BSONTypeString] != "d" {
			t.Errorf("expected GlobalMin String 'd', got %v", plan.GlobalMinSafeIDs[BSONTypeString])
		}
	})

	t.Run("IncompleteCheckpointsFallbackToFreshStart", func(t *testing.T) {
		tmpDir := t.TempDir()
		totalSplits := 3

		// Only save partition 0 and 2 (missing partition 1)
		cp0 := &PartitionCheckpoint{
			Database:       db,
			Collection:     coll,
			PartitionIndex: 0,
			TotalSplits:    totalSplits,
			TypeProgress:   map[BSONType]*TypeRangeBoundary{BSONTypeObjectID: {BSONType: BSONTypeObjectID}},
		}
		cp2 := &PartitionCheckpoint{
			Database:       db,
			Collection:     coll,
			PartitionIndex: 2,
			TotalSplits:    totalSplits,
			TypeProgress:   map[BSONType]*TypeRangeBoundary{BSONTypeObjectID: {BSONType: BSONTypeObjectID}},
		}

		_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, db, coll, 0, totalSplits), cp0)
		_ = SavePartitionCheckpoint(GetPartitionCheckpointPath(tmpDir, db, coll, 2, totalSplits), cp2)

		plan, err := DetermineBackfillResumptionPlan(tmpDir, db, coll, 3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if plan.Mode != ResumptionModeFresh {
			t.Errorf("expected ResumptionModeFresh for incomplete checkpoint set, got %v", plan.Mode)
		}
	})
}
