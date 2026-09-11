package migration

import (
	"encoding/json"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"firestore-dr-migration/pkg/idmap"
	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// helper function to assert success in non-collision tests
func mustTransform(t *testing.T, doc interface{}, log *logger.Logger, dbName, collName string, docID interface{}) interface{} {
	t.Helper()
	transformer := NewFieldTransformer(true, true, false, log)
	res, err := transformer.Transform(doc, dbName, collName, docID)
	if err != nil {
		t.Fatalf("Transform failed unexpectedly: %v", err)
	}
	return res
}

// helper function to assert batch success in non-collision tests
func mustTransformBatch(t *testing.T, batch []interface{}, log *logger.Logger, dbName, collName string) []interface{} {
	t.Helper()
	transformer := NewFieldTransformer(true, true, false, log)
	res, err := transformer.TransformBatch(batch, dbName, collName)
	if err != nil {
		t.Fatalf("TransformBatch failed unexpectedly: %v", err)
	}
	return res
}

func TestTransformRenameFieldNames(t *testing.T) {
	log := logger.New()

	// Define table-driven test cases for clear visibility of input vs expected output
	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
	}{
		{
			name: "bson.M mapping: __name__ and ___ should not be renamed",
			input: bson.M{
				"__name__": "alice",
				"___":      "triple-underscore",
				"normal":   "value",
				"_____3_______": "3	underscores",
			},
			expected: bson.M{
				"__name__": "alice",
				"___":      "triple-underscore",
				"normal":   "value",
				"_____3_______": "3	underscores",
			},
		},
		{
			name: "map[string]interface{} mapping: __height__ should not be renamed",
			input: map[string]interface{}{
				"__height__": 180,
			},
			expected: map[string]interface{}{
				"__height__": 180,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			output := mustTransform(t, tc.input, log, "db", "coll", "id")
			if diff := cmp.Diff(tc.expected, output); diff != "" {
				t.Errorf("\n[FAIL] %s mismatch (-want +got):\n%s", tc.name, diff)
			}
		})
	}
}

func TestTransformBsonDOrderedRenaming(t *testing.T) {
	log := logger.New()

	// bson.D needs individual element verification to maintain precise field ordering assertion
	input := bson.D{
		{Key: "__age__", Value: 30},
		{Key: "____", Value: "four-underscores"},
	}

	expected := bson.D{
		{Key: "__age__", Value: 30},
		{Key: "____", Value: "four-underscores"},
	}

	output := mustTransform(t, input, log, "db", "coll", "id").(bson.D)

	if diff := cmp.Diff(expected, output); diff != "" {
		t.Errorf("\n[FAIL] bson.D Ordered Renaming mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformStringifyLongNestedKeys(t *testing.T) {
	log := logger.New()
	longKey := strings.Repeat("a", 1001)

	// 1. Root-level long key: Must remain unmodified (warnings only, no stringification)
	rootInput := bson.M{longKey: "root-long"}
	rootExpected := bson.M{longKey: "root-long"}
	rootOutput := mustTransform(t, rootInput, log, "db", "coll", "id")

	if diff := cmp.Diff(rootExpected, rootOutput); diff != "" {
		t.Errorf("\n[FAIL] Root-level long key handling mismatch (-want +got):\n%s", diff)
	}

	// 2. Nested document with long key: Entire sub-document should be stringified to JSON
	nestedInput := bson.M{
		"nested": bson.M{
			longKey: "nested-long",
		},
	}
	nestedOutput := mustTransform(t, nestedInput, log, "db", "coll", "id").(bson.M)

	expectedVal, ok := nestedOutput["nested"].(string)
	if !ok {
		t.Fatalf("\n[FAIL] Long key nested stringify did not produce JSON string, got type: %T", nestedOutput["nested"])
	}

	var decoded bson.M
	if err := json.Unmarshal([]byte(expectedVal), &decoded); err != nil {
		t.Fatalf("\n[FAIL] Failed to unmarshal stringified nested object: %v", err)
	}

	if decoded[longKey] != "nested-long" {
		t.Errorf("\n[FAIL] Decoded nested stringify does not contain expected key/value, got: %v", decoded)
	}

	// 3. Nested bson.D with long key: Entire sub-document should be stringified to JSON
	nestedDInput := bson.D{
		{Key: "nested", Value: bson.D{
			{Key: longKey, Value: "nested-long-d"},
		}},
	}
	nestedDOutput := mustTransform(t, nestedDInput, log, "db", "coll", "id").(bson.D)

	var nestedDVal string
	for _, elem := range nestedDOutput {
		if elem.Key == "nested" {
			nestedDVal = elem.Value.(string)
		}
	}

	var decodedD bson.M
	if err := json.Unmarshal([]byte(nestedDVal), &decodedD); err != nil {
		t.Fatalf("\n[FAIL] Failed to unmarshal stringified nested bson.D object: %v", err)
	}

	if decodedD[longKey] != "nested-long-d" {
		t.Errorf("\n[FAIL] Decoded nested bson.D stringify does not contain expected key/value, got: %v", decodedD)
	}

	// 4. Nested map[string]interface{} with long key: Entire sub-document should be stringified to JSON
	nestedMapInput := bson.M{
		"nested": map[string]interface{}{
			longKey: "nested-long-map",
		},
	}
	nestedMapOutput := mustTransform(t, nestedMapInput, log, "db", "coll", "id").(bson.M)

	nestedMapVal, ok := nestedMapOutput["nested"].(string)
	if !ok {
		t.Fatalf("\n[FAIL] Long key nested map did not produce JSON string, got type: %T", nestedMapOutput["nested"])
	}

	var decodedMap bson.M
	if err := json.Unmarshal([]byte(nestedMapVal), &decodedMap); err != nil {
		t.Fatalf("\n[FAIL] Failed to unmarshal stringified nested map object: %v", err)
	}

	if decodedMap[longKey] != "nested-long-map" {
		t.Errorf("\n[FAIL] Decoded nested map stringify does not contain expected key/value, got: %v", decodedMap)
	}
}

func TestTransformArraysAndBatches(t *testing.T) {
	log := logger.New()

	// 1. Standard slice: []interface{}
	arrayInput := bson.M{
		"arr": []interface{}{
			bson.M{"__elem__": "val"},
			"regular-string",
		},
	}
	arrayExpected := bson.M{
		"arr": []interface{}{
			bson.M{"__elem__": "val"},
			"regular-string",
		},
	}
	arrayOutput := mustTransform(t, arrayInput, log, "db", "coll", "id")

	if diff := cmp.Diff(arrayExpected, arrayOutput); diff != "" {
		t.Errorf("\n[FAIL] []interface{} Array processing mismatch (-want +got):\n%s", diff)
	}

	// 2. BSON array: bson.A
	bsonAInput := bson.M{
		"arr": bson.A{
			bson.M{"__elem__": "val"},
			"regular-string",
		},
	}
	bsonAExpected := bson.M{
		"arr": bson.A{
			bson.M{"__elem__": "val"},
			"regular-string",
		},
	}
	bsonAOutput := mustTransform(t, bsonAInput, log, "db", "coll", "id")

	if diff := cmp.Diff(bsonAExpected, bsonAOutput); diff != "" {
		t.Errorf("\n[FAIL] bson.A Array processing mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformObjectIDPreserved(t *testing.T) {
	log := logger.New()
	oid := primitive.NewObjectID()
	nestedOid := primitive.NewObjectID()

	doc := bson.M{
		"_id":  oid,
		"name": "object_id_preservation",
		"data": bson.M{
			"nested_id": nestedOid,
		},
	}
	expected := bson.M{
		"_id":  oid,
		"name": "object_id_preservation",
		"data": bson.M{
			"nested_id": nestedOid,
		},
	}

	res := mustTransform(t, doc, log, "db", "coll", oid)
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] ObjectID preservation mismatch (-want +got):\n%s", diff)
	}
}


func TestTransformPositiveInfinity(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"_id":         primitive.NewObjectID(),
		"name":        "positive_infinity_value",
		"description": "A document storing a floating-point Positive Infinity value.",
		"data":        math.Inf(1),
	}
	res := mustTransform(t, doc, log, "db", "coll", "id").(bson.M)
	if res["data"] != math.Inf(1) {
		t.Errorf("expected Positive Infinity to be preserved, got: %v", res["data"])
	}
}

func TestTransformNegativeInfinity(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"_id":         primitive.NewObjectID(),
		"name":        "negative_infinity_value",
		"description": "A document storing a floating-point Negative Infinity value.",
		"data":        math.Inf(-1),
	}
	res := mustTransform(t, doc, log, "db", "coll", "id").(bson.M)
	if res["data"] != math.Inf(-1) {
		t.Errorf("expected Negative Infinity to be preserved, got: %v", res["data"])
	}
}

func TestTransformNaNValue(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"_id":         primitive.NewObjectID(),
		"name":        "nan_value",
		"description": "A document storing a floating-point Not-a-Number (NaN) value.",
		"data":        math.NaN(),
	}
	res := mustTransform(t, doc, log, "db", "coll", "id").(bson.M)
	val, ok := res["data"].(float64)
	if !ok || !math.IsNaN(val) {
		t.Errorf("expected NaN to be preserved, got: %v", res["data"])
	}
}

func TestTransformCase1StandardDoubleUnderscoredKey(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 1: Standard double-underscored key",
		"description": "A standard key matching the '__my_key__' format. Expected: Unchanged",
		"data": bson.M{
			"__my_key__": "value_1",
		},
	}
	expected := bson.M{
		"name":        "Case 1: Standard double-underscored key",
		"description": "A standard key matching the '__my_key__' format. Expected: Unchanged",
		"data": bson.M{
			"__my_key__": "value_1",
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 1 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformCase2StandardRegularKey(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 2: Standard regular key (No transform)",
		"description": "A clean regular string key without underscores. Expected: Unchanged ('my_key')",
		"data": bson.M{
			"my_key": "value_2",
		},
	}
	expected := bson.M{
		"name":        "Case 2: Standard regular key (No transform)",
		"description": "A clean regular string key without underscores. Expected: Unchanged ('my_key')",
		"data": bson.M{
			"my_key": "value_2",
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 2 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformCase3NestedObjectWithMatchingKeys(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 3: Nested object with matching keys",
		"description": "Recursively matches keys at multiple object levels. Expected: Both unchanged",
		"data": bson.M{
			"__outer__": bson.M{
				"__inner__": "nested_value",
			},
		},
	}
	expected := bson.M{
		"name":        "Case 3: Nested object with matching keys",
		"description": "Recursively matches keys at multiple object levels. Expected: Both unchanged",
		"data": bson.M{
			"__outer__": bson.M{
				"__inner__": "nested_value",
			},
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 3 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformCase4MixedRegularAndDoubleUnderscoredKeys(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 4: Mixed regular and double-underscored keys",
		"description": "Ensures regular keys and double-underscored keys in the same object are left untouched.",
		"data": bson.M{
			"__my_key__":  "will_rename",
			"regular_key": "will_not_change",
		},
	}
	expected := bson.M{
		"name":        "Case 4: Mixed regular and double-underscored keys",
		"description": "Ensures regular keys and double-underscored keys in the same object are left untouched.",
		"data": bson.M{
			"__my_key__":  "will_rename",
			"regular_key": "will_not_change",
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 4 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformCase5ArrayContainingMatchingObjects(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 5: Array containing matching objects",
		"description": "Recurses into array elements without transforming keys. Expected: Both '__id__' keys unchanged",
		"data": bson.M{
			"items": []interface{}{
				bson.M{"__id__": 1, "name": "item_a"},
				bson.M{"__id__": 2, "name": "item_b"},
			},
		},
	}
	expected := bson.M{
		"name":        "Case 5: Array containing matching objects",
		"description": "Recurses into array elements without transforming keys. Expected: Both '__id__' keys unchanged",
		"data": bson.M{
			"items": []interface{}{
				bson.M{"__id__": 1, "name": "item_a"},
				bson.M{"__id__": 2, "name": "item_b"},
			},
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 5 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformCase6DeeplyNestedMapWithMixedKeyStyles(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 6: Deeply nested map with mixed key styles",
		"description": "A deep object graph with double-underscored structural keys. Expected: Unchanged",
		"data": bson.M{
			"__root__": bson.M{
				"regular_level": bson.M{
					"__leaf__": "deep_value",
				},
			},
		},
	}
	expected := bson.M{
		"name":        "Case 6: Deeply nested map with mixed key styles",
		"description": "A deep object graph with double-underscored structural keys. Expected: Unchanged",
		"data": bson.M{
			"__root__": bson.M{
				"regular_level": bson.M{
					"__leaf__": "deep_value",
				},
			},
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 6 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformCase7MixedTypesInsideAnArray(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 7: Mixed types inside an array",
		"description": "Ensures non-object types inside arrays (strings, numbers) are bypassed safely.",
		"data": bson.M{
			"__list__": []interface{}{
				"plain_string",
				123,
				bson.M{"__nested_key__": "value_7"},
			},
		},
	}
	expected := bson.M{
		"name":        "Case 7: Mixed types inside an array",
		"description": "Ensures non-object types inside arrays (strings, numbers) are bypassed safely.",
		"data": bson.M{
			"__list__": []interface{}{
				"plain_string",
				123,
				bson.M{"__nested_key__": "value_7"},
			},
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 7 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformCase8NegativeTestLeadingUnderscoresOnly(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 8: Negative Test - Leading underscores only",
		"description": "A key starting with '__' but not ending with it. Expected: Unchanged ('__left_only')",
		"data": bson.M{
			"__left_only": "value_8",
		},
	}
	expected := bson.M{
		"name":        "Case 8: Negative Test - Leading underscores only",
		"description": "A key starting with '__' but not ending with it. Expected: Unchanged ('__left_only')",
		"data": bson.M{
			"__left_only": "value_8",
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 8 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformCase9NegativeTestTrailingUnderscoresOnly(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 9: Negative Test - Trailing underscores only",
		"description": "A key ending with '__' but not starting with it. Expected: Unchanged ('right_only__')",
		"data": bson.M{
			"right_only__": "value_9",
		},
	}
	expected := bson.M{
		"name":        "Case 9: Negative Test - Trailing underscores only",
		"description": "A key ending with '__' but not starting with it. Expected: Unchanged ('right_only__')",
		"data": bson.M{
			"right_only__": "value_9",
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 9 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformCase10EmptyAndNullValuesWithMatchingKeys(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 10: Empty and null values with matching keys",
		"description": "Ensures the recursive step doesn't crash on null properties or empty objects.",
		"data": bson.M{
			"__null_key__":     nil,
			"__empty_object__": bson.M{},
		},
	}
	expected := bson.M{
		"name":        "Case 10: Empty and null values with matching keys",
		"description": "Ensures the recursive step doesn't crash on null properties or empty objects.",
		"data": bson.M{
			"__null_key__":     nil,
			"__empty_object__": bson.M{},
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 10 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEmptyFieldKeys(t *testing.T) {
	log := logger.New()

	// 1. bson.M: empty key should be omitted
	docM := bson.M{
		"":     "empty-key-value",
		"keep": "keep-value",
	}
	expectedM := bson.M{
		"keep": "keep-value",
	}
	resM := mustTransform(t, docM, log, "db", "coll", "id")
	if diff := cmp.Diff(expectedM, resM); diff != "" {
		t.Errorf("expected empty key in bson.M to be removed, mismatch (-want +got):\n%s", diff)
	}

	// 2. bson.D: empty key should be omitted
	docD := bson.D{
		{Key: "", Value: "empty-key-val"},
		{Key: "keep", Value: "keep-val"},
	}
	expectedD := bson.D{
		{Key: "keep", Value: "keep-val"},
	}
	resD := mustTransform(t, docD, log, "db", "coll", "id")
	if diff := cmp.Diff(expectedD, resD); diff != "" {
		t.Errorf("expected empty key in bson.D to be removed, mismatch (-want +got):\n%s", diff)
	}

	// 3. map[string]interface{}: empty key should be omitted
	docMap := map[string]interface{}{
		"":     "empty-map-val",
		"keep": "keep-map-val",
	}
	expectedMap := map[string]interface{}{
		"keep": "keep-map-val",
	}
	resMap := mustTransform(t, docMap, log, "db", "coll", "id")
	if diff := cmp.Diff(expectedMap, resMap); diff != "" {
		t.Errorf("expected empty key in map to be removed, mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformExtractDocID(t *testing.T) {
	log := logger.New()

	// 1. bson.D with _id key
	docD := bson.D{
		{Key: "_id", Value: "d_id"},
		{Key: "other", Value: "value"},
	}
	batchD := []interface{}{docD}
	resD := mustTransformBatch(t, batchD, log, "db", "coll")
	if len(resD) != 1 {
		t.Fatal("TransformBatch failed")
	}

	// 2. bson.M with _id key
	docM := bson.M{
		"_id": "m_id",
	}
	batchM := []interface{}{docM}
	resM := mustTransformBatch(t, batchM, log, "db", "coll")
	if len(resM) != 1 {
		t.Fatal("TransformBatch failed")
	}

	// 3. map[string]interface{} with _id key
	docMap := map[string]interface{}{
		"_id": "map_id",
	}
	batchMap := []interface{}{docMap}
	resMap := mustTransformBatch(t, batchMap, log, "db", "coll")
	if len(resMap) != 1 {
		t.Fatal("TransformBatch failed")
	}

	// 4. Unsupported type or no _id
	batchUnsupported := []interface{}{
		"string_type",
		bson.D{{Key: "no_id", Value: "val"}},
	}
	resUnsupported := mustTransformBatch(t, batchUnsupported, log, "db", "coll")
	if len(resUnsupported) != 2 {
		t.Fatal("TransformBatch failed")
	}
}



func TestTransformStringifyUnsupportedJSONValue(t *testing.T) {
	log := logger.New()
	longKey := strings.Repeat("a", 1001)

	unsupportedChan := make(chan int)

	doc := bson.M{
		"nested": bson.M{
			longKey:       "long-val",
			"invalid_val": unsupportedChan,
		},
	}

	transformer := NewFieldTransformer(true, true, false, log)
	_, err := transformer.Transform(doc, "db", "coll", "id")
	if err == nil {
		t.Error("expected error when stringifying unsupported JSON value in nested object with long keys, got nil")
	} else if !strings.Contains(err.Error(), "failed to stringify") {
		t.Errorf("expected failed to stringify error, got: %v", err)
	}
}

func TestTransformSeparateFlags(t *testing.T) {
	log := logger.New()
	longKey := strings.Repeat("a", 1001)

	// Sample document containing both issues:
	// - empty key field name ("")
	// - long nested field name (longKey)
	getInputDoc := func() bson.M {
		return bson.M{
			"":     "empty-key-val",
			"keep": "keep-val",
			"nested": bson.M{
				longKey: "long-val",
			},
		}
	}

	// Case 1: Only DropEmptyFieldNames is true
	t.Run("Only DropEmptyFieldNames", func(t *testing.T) {
		transformer := NewFieldTransformer(true, false, false, log)
		res, err := transformer.Transform(getInputDoc(), "db", "coll", "id")
		if err != nil {
			t.Fatalf("Transform failed: %v", err)
		}
		doc := res.(bson.M)

		// Empty key should be removed
		if _, exists := doc[""]; exists {
			t.Error("expected empty key to be removed")
		}

		// Keep key should remain
		if doc["keep"] != "keep-val" {
			t.Errorf("expected keep-val, got %v", doc["keep"])
		}

		// Nested long key should NOT be stringified (remains bson.M)
		nested, ok := doc["nested"].(bson.M)
		if !ok {
			t.Fatalf("expected nested to remain bson.M, got type: %T", doc["nested"])
		}
		if nested[longKey] != "long-val" {
			t.Errorf("expected long-val, got %v", nested[longKey])
		}
	})

	// Case 2: Only ConvertLongFieldNamesInNestedDocs is true
	t.Run("Only ConvertLongFieldNamesInNestedDocs", func(t *testing.T) {
		transformer := NewFieldTransformer(false, true, false, log)
		res, err := transformer.Transform(getInputDoc(), "db", "coll", "id")
		if err != nil {
			t.Fatalf("Transform failed: %v", err)
		}
		doc := res.(bson.M)

		// Empty key should NOT be removed
		if doc[""] != "empty-key-val" {
			t.Errorf("expected empty key to remain, got %v", doc[""])
		}

		// Keep key should remain
		if doc["keep"] != "keep-val" {
			t.Errorf("expected keep-val, got %v", doc["keep"])
		}

		// Nested long key should BE stringified (becomes JSON string)
		nestedVal, ok := doc["nested"].(string)
		if !ok {
			t.Fatalf("expected nested to be stringified to JSON string, got type: %T", doc["nested"])
		}
		var decoded bson.M
		if err := json.Unmarshal([]byte(nestedVal), &decoded); err != nil {
			t.Fatalf("failed to decode nested stringified object: %v", err)
		}
		if decoded[longKey] != "long-val" {
			t.Errorf("expected decoded long-val, got %v", decoded[longKey])
		}
	})

	// Case 3: Both disabled
	t.Run("Both disabled", func(t *testing.T) {
		transformer := NewFieldTransformer(false, false, false, log)
		res, err := transformer.Transform(getInputDoc(), "db", "coll", "id")
		if err != nil {
			t.Fatalf("Transform failed: %v", err)
		}
		doc := res.(bson.M)

		// Empty key should NOT be removed
		if doc[""] != "empty-key-val" {
			t.Errorf("expected empty key to remain, got %v", doc[""])
		}

		// Nested long key should NOT be stringified
		nested, ok := doc["nested"].(bson.M)
		if !ok {
			t.Fatalf("expected nested to remain bson.M, got type: %T", doc["nested"])
		}
		if nested[longKey] != "long-val" {
			t.Errorf("expected long-val, got %v", nested[longKey])
		}
	})
}

func TestTransformProactiveIDConversion(t *testing.T) {
	log := logger.New()
	transformer := NewFieldTransformer(false, false, true, log)

	t.Run("Valid types should remain unchanged", func(t *testing.T) {
		objectID := primitive.NewObjectID()
		docs := []bson.M{
			{"_id": objectID, "name": "objectID"},
			{"_id": "string-id", "name": "string"},
			{"_id": int64(123456), "name": "int64"},
		}

		for _, original := range docs {
			res, err := transformer.Transform(original, "db", "coll", "id")
			if err != nil {
				t.Fatalf("Transform failed: %v", err)
			}
			doc := res.(bson.M)
			if doc["_id"] != original["_id"] {
				t.Errorf("expected _id %v, got %v", original["_id"], doc["_id"])
			}
			if _, exists := doc["_migrationIdConverted"]; exists {
				t.Errorf("expected no migration conversion flag for type %T", original["_id"])
			}
		}
	})

	t.Run("Invalid types should be proactively converted", func(t *testing.T) {
		type testCase struct {
			originalID   interface{}
			expectedID   string
			expectedType string
		}
		cases := []testCase{
			{originalID: true, expectedID: "_converted:bool:true", expectedType: "bool"},
			{originalID: int(987), expectedID: "_converted:int:987", expectedType: "int"},
			{originalID: int32(456), expectedID: "_converted:int32:456", expectedType: "int32"},
			{originalID: float64(12.34), expectedID: "_converted:double:12.34", expectedType: "float64"},
			{originalID: bson.A{1, 2}, expectedID: "_converted:array:[1,2]", expectedType: "primitive.A"},
			{originalID: bson.D{{Key: "x", Value: "y"}}, expectedID: "_converted:document:[{\"Key\":\"x\",\"Value\":\"y\"}]", expectedType: "primitive.D"},
		}

		for _, tc := range cases {
			original := bson.M{"_id": tc.originalID}
			res, err := transformer.Transform(original, "db", "coll", "id")
			if err != nil {
				t.Fatalf("Transform failed: %v", err)
			}
			doc := res.(bson.M)
			if doc["_id"] != tc.expectedID {
				t.Errorf("expected converted _id to be %s, got %v (original type: %s)", tc.expectedID, doc["_id"], tc.expectedType)
			}
			if _, exists := doc["_migrationIdConverted"]; exists {
				t.Errorf("expected no _migrationIdConverted flag, but found it")
			}
			if _, exists := doc["_migrationOriginalIdType"]; exists {
				t.Errorf("expected no _migrationOriginalIdType flag, but found it")
			}
		}
	})
}

func BenchmarkTransformStandardDoc(b *testing.B) {
	log := logger.New()
	log.SetLevel("error")
	transformer := NewFieldTransformer(true, true, true, log)
	doc := bson.M{
		"_id":  "valid_string_id",
		"name": "john doe",
		"age":  30,
		"tags": bson.A{"user", "admin"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = transformer.Transform(doc, "db", "coll", "valid_string_id")
	}
}

func BenchmarkTransformInvalidIDDoc(b *testing.B) {
	log := logger.New()
	log.SetLevel("error")
	transformer := NewFieldTransformer(true, true, true, log)
	doc := bson.M{
		"_id":  bson.A{1, 2, 3},
		"name": "john doe",
		"age":  30,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = transformer.Transform(doc, "db", "coll", bson.A{1, 2, 3})
	}
}

func BenchmarkTransformLongNestedKeyDoc(b *testing.B) {
	log := logger.New()
	log.SetLevel("error")
	transformer := NewFieldTransformer(true, true, true, log)
	longKey := string(make([]byte, 1001))
	doc := bson.M{
		"_id": "valid_string_id",
		"nested": bson.M{
			longKey: "value",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = transformer.Transform(doc, "db", "coll", "valid_string_id")
	}
}

func TestToComparableIDKey(t *testing.T) {
	// 1. Ensure primitive.Binary (UUID) is hashable and does not panic in a map
	binID := primitive.Binary{Subtype: 4, Data: []byte("12345678-1234-1234-1234-123456789abc")}
	binKey := toComparableIDKey(binID)
	if binKey == "" {
		t.Fatalf("expected non-empty key for primitive.Binary")
	}

	m := make(map[string]bool)
	m[binKey] = true
	if !m[binKey] {
		t.Fatalf("expected binKey to be present in map")
	}

	// 2. Ensure different BSON types with identical scalar values produce distinct keys (zero collisions)
	strID := "100"
	int32ID := int32(100)
	int64ID := int64(100)
	doubleID := float64(100.0)
	oid := primitive.NewObjectID()

	strKey := toComparableIDKey(strID)
	int32Key := toComparableIDKey(int32ID)
	int64Key := toComparableIDKey(int64ID)
	doubleKey := toComparableIDKey(doubleID)
	oidKey := toComparableIDKey(oid)

	keys := map[string]string{
		"string":   strKey,
		"int32":    int32Key,
		"int64":    int64Key,
		"double":   doubleKey,
		"objectID": oidKey,
		"binary":   binKey,
	}

	seen := make(map[string]string)
	for typeName, key := range keys {
		if origType, exists := seen[key]; exists {
			t.Fatalf("collision detected between type %s and %s: key=%s", typeName, origType, key)
		}
		seen[key] = typeName
	}
}

// TestProactiveIDConversionRecordsIDMap proves the id-map wiring end to end:
// when a shared id-map store is installed, a proactive _id conversion is
// persisted so -mode=verify can reconnect the rewritten target document to its
// source _id. The mapping must be keyed by the SOURCE collection name and by the
// original typed value (the same key verify's mapper.Lookup uses).
func TestProactiveIDConversionRecordsIDMap(t *testing.T) {
	log := logger.New()

	store, err := idmap.OpenFileStore(filepath.Join(t.TempDir(), "id-mapping.jsonl"))
	if err != nil {
		t.Fatalf("OpenFileStore failed: %v", err)
	}
	defer store.Close()

	// Install the shared store, then build the transformer so it captures it.
	// Always restore the no-op sink so other tests are unaffected.
	SetSharedIDStore(store)
	defer SetSharedIDStore(idmap.NopStore{})
	transformer := NewFieldTransformer(false, false, true, log)

	origID := primitive.Binary{Subtype: 0x00, Data: []byte{0xDE, 0xAD, 0xBE, 0xEF}}
	res, err := transformer.Transform(bson.M{"_id": origID, "n": 1}, "db", "orders", "id")
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}
	newID := res.(bson.M)["_id"].(string)
	if newID != "_converted:binary:deadbeef" {
		t.Fatalf("unexpected converted _id: %q", newID)
	}

	// Lookup exactly as verify does: by source collection name + original value.
	got, ok := store.Lookup("orders", origID)
	if !ok {
		t.Fatalf("mapping not recorded for source collection %q", "orders")
	}
	if got != newID {
		t.Errorf("id-map returned %q, want %q", got, newID)
	}

	// A different collection must not resolve the same original _id.
	if _, ok := store.Lookup("customers", origID); ok {
		t.Errorf("mapping leaked across collections")
	}
}

// TestProactiveIDConversionNoStoreByDefault confirms the default sink is a no-op:
// a transformer built without a shared store still converts, but records nothing
// (and never panics), so existing paths and tests are unaffected.
func TestProactiveIDConversionNoStoreByDefault(t *testing.T) {
	SetSharedIDStore(idmap.NopStore{}) // explicit default
	transformer := NewFieldTransformer(false, false, true, logger.New())
	res, err := transformer.Transform(bson.M{"_id": int32(7)}, "db", "coll", "id")
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}
	if got := res.(bson.M)["_id"].(string); got != "_converted:int32:7" {
		t.Fatalf("unexpected converted _id: %q", got)
	}
}



