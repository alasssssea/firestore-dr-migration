package migration

import (
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestBuildPartitionFilters_StringIDs(t *testing.T) {
	sampledIDs := make([]interface{}, 1000)
	for i := 0; i < 1000; i++ {
		sampledIDs[i] = fmt.Sprintf("id_%04d", i)
	}

	partitionCount := 8
	filters := buildPartitionFilters(sampledIDs, partitionCount)

	if len(filters) != partitionCount {
		t.Fatalf("expected %d filters, got %d", partitionCount, len(filters))
	}

	// step = 1000 / 8 = 125
	// Boundaries should be at: 125, 250, 375, 500, 625, 750, 875

	expectedEnd0 := "id_0125"
	filter0 := filters[0]
	val0, err := getBSONValue(filter0, "_id", "$lt")
	if err != nil {
		t.Fatalf("failed to parse filter 0: %v", err)
	}
	if val0 != expectedEnd0 {
		t.Errorf("expected first partition upper bound %q, got %q", expectedEnd0, val0)
	}

	expectedStart1 := "id_0125"
	expectedEnd1 := "id_0250"
	filter1 := filters[1]
	start1, err := getBSONValue(filter1, "_id", "$gte")
	if err != nil {
		t.Fatalf("failed to parse start of filter 1: %v", err)
	}
	end1, err := getBSONValue(filter1, "_id", "$lt")
	if err != nil {
		t.Fatalf("failed to parse end of filter 1: %v", err)
	}
	if start1 != expectedStart1 || end1 != expectedEnd1 {
		t.Errorf("expected partition 1 bounds [%q, %q), got [%q, %q)", expectedStart1, expectedEnd1, start1, end1)
	}

	expectedStart7 := "id_0875"
	filter7 := filters[7]
	start7, err := getBSONValue(filter7, "_id", "$gte")
	if err != nil {
		t.Fatalf("failed to parse filter 7: %v", err)
	}
	if start7 != expectedStart7 {
		t.Errorf("expected last partition lower bound %q, got %q", expectedStart7, start7)
	}
}

func TestBuildPartitionFilters_NumericIDs(t *testing.T) {
	sampledIDs := make([]interface{}, 12)
	for i := 0; i < 12; i++ {
		sampledIDs[i] = i * 10
	}

	partitionCount := 3
	filters := buildPartitionFilters(sampledIDs, partitionCount)

	if len(filters) != partitionCount {
		t.Fatalf("expected %d filters, got %d", partitionCount, len(filters))
	}

	// step = 12 / 3 = 4
	// Boundaries should be at: sampledIDs[4] (40), sampledIDs[8] (80)

	// Partition 0: < 40
	filter0 := filters[0]
	val0, err := getBSONValue(filter0, "_id", "$lt")
	if err != nil {
		t.Fatal(err)
	}
	if val0 != 40 {
		t.Errorf("expected partition 0 upper bound 40, got %v", val0)
	}

	// Partition 1: [40, 80)
	filter1 := filters[1]
	start1, err := getBSONValue(filter1, "_id", "$gte")
	if err != nil {
		t.Fatal(err)
	}
	end1, err := getBSONValue(filter1, "_id", "$lt")
	if err != nil {
		t.Fatal(err)
	}
	if start1 != 40 || end1 != 80 {
		t.Errorf("expected partition 1 bounds [40, 80), got [%v, %v)", start1, end1)
	}

	// Partition 2: >= 80
	filter2 := filters[2]
	start2, err := getBSONValue(filter2, "_id", "$gte")
	if err != nil {
		t.Fatal(err)
	}
	if start2 != 80 {
		t.Errorf("expected partition 2 lower bound 80, got %v", start2)
	}
}

// Helper helper to extract value from BSON filter
func getBSONValue(filter bson.D, key, op string) (interface{}, error) {
	for _, elem := range filter {
		if elem.Key == key {
			subD, ok := elem.Value.(bson.D)
			if !ok {
				return nil, fmt.Errorf("value for key %q is not bson.D", key)
			}
			for _, subElem := range subD {
				if subElem.Key == op {
					return subElem.Value, nil
				}
			}
			return nil, fmt.Errorf("operator %q not found inside key %q", op, key)
		}
	}
	return nil, fmt.Errorf("key %q not found in filter", key)
}

func TestCreatePartitionsGroupedByType_SingleType_ObjectId(t *testing.T) {
	numSplits := 4
	sampledIDs := []interface{}{"oid1", "oid2", "oid3", "oid4", "oid5", "oid6", "oid7", "oid8"}
	oidSlices := buildTypeScopedPartitionFilters(sampledIDs, numSplits, "objectId")

	slicesPerType := map[string][]bson.D{
		"objectId": oidSlices,
	}

	merged := mergeTypeSlices(slicesPerType, numSplits)
	if len(merged) != numSplits {
		t.Fatalf("expected %d merged filters, got %d", numSplits, len(merged))
	}

	// For a single type, the filter should not be wrapped in an $or
	for i, f := range merged {
		for _, elem := range f {
			if elem.Key == "$or" {
				t.Errorf("partition %d should not have $or wrapper for a single type", i)
			}
		}
	}
}

func TestCreatePartitionsGroupedByType_MixedTypes_SampleAndFallback(t *testing.T) {
	numSplits := 4
	// Simulate "objectId" (>= 2000 docs -> quantile slices)
	sampledIDs := []interface{}{"oid1", "oid2", "oid3", "oid4", "oid5", "oid6", "oid7", "oid8"}
	oidSlices := buildTypeScopedPartitionFilters(sampledIDs, numSplits, "objectId")

	// Simulate "string" (< 2000 docs -> uniform fallback slices)
	strSlices := createStringUniformSlices("string", numSplits)

	slicesPerType := map[string][]bson.D{
		"objectId": oidSlices,
		"string":   strSlices,
	}

	merged := mergeTypeSlices(slicesPerType, numSplits)
	if len(merged) != numSplits {
		t.Fatalf("expected %d merged filters, got %d", numSplits, len(merged))
	}

	// For >1 types, every filter should be wrapped in an $or with exactly 2 clauses
	for i, f := range merged {
		var foundOr bool
		for _, elem := range f {
			if elem.Key == "$or" {
				foundOr = true
				orArr, ok := elem.Value.(bson.A)
				if !ok {
					t.Fatalf("partition %d $or value is not bson.A", i)
				}
				if len(orArr) != 2 {
					t.Errorf("expected 2 clauses in partition %d $or, got %d", i, len(orArr))
				}
			}
		}
		if !foundOr {
			t.Errorf("partition %d expected $or wrapper for mixed types", i)
		}
	}
}

func TestCreatePartitionsGroupedByType_AllUniformFallback(t *testing.T) {
	numSplits := 4
	numSlices := createNumberUniformSlices(numSplits)
	strSlices := createStringUniformSlices("string", numSplits)

	slicesPerType := map[string][]bson.D{
		"number": numSlices,
		"string": strSlices,
	}

	merged := mergeTypeSlices(slicesPerType, numSplits)
	if len(merged) != numSplits {
		t.Fatalf("expected %d merged filters, got %d", numSplits, len(merged))
	}

	for i, f := range merged {
		var foundOr bool
		for _, elem := range f {
			if elem.Key == "$or" {
				foundOr = true
				orArr, ok := elem.Value.(bson.A)
				if !ok {
					t.Fatalf("partition %d $or value is not bson.A", i)
				}
				if len(orArr) != 2 {
					t.Errorf("expected 2 clauses in partition %d $or, got %d", i, len(orArr))
				}
			}
		}
		if !foundOr {
			t.Errorf("partition %d expected $or wrapper for uniform fallback mixed types", i)
		}
	}
}

