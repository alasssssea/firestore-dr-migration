package assess

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func hasRule(fs []Finding, rule string, sev Severity) bool {
	for _, f := range fs {
		if f.Rule == rule && f.Severity == sev {
			return true
		}
	}
	return false
}

func TestCheckCollectionName(t *testing.T) {
	if hasRule(CheckCollectionName("users"), "collection-name-reserved", SeverityBlock) {
		t.Error("plain name should be fine")
	}
	if !hasRule(CheckCollectionName("__meta__"), "collection-name-reserved", SeverityBlock) {
		t.Error("reserved __x__ name should block")
	}
	if !hasRule(CheckCollectionName("a$b"), "collection-name-dollar", SeverityBlock) {
		t.Error("dollar name should block")
	}
	if !hasRule(CheckCollectionName("system.js"), "collection-name-system", SeverityBlock) {
		t.Error("system. prefix should block")
	}
}

func TestCheckID(t *testing.T) {
	if len(CheckID(primitive.NewObjectID())) != 0 {
		t.Error("ObjectID _id should be clean")
	}
	if len(CheckID(int64(42))) != 0 {
		t.Error("int64 _id should be clean")
	}
	if !hasRule(CheckID(3.14), "id-type-convert", SeverityAutoFix) {
		t.Error("float _id should be flagged as auto-convert")
	}
	// int32 is NOT stored as-is by the engine (isValidIDType accepts only
	// ObjectID/string/int64), so the assessment must predict a conversion.
	if !hasRule(CheckID(int32(7)), "id-type-convert", SeverityAutoFix) {
		t.Error("int32 _id should be flagged as auto-convert (engine converts it)")
	}
	if IsValidIDType(int32(7)) {
		t.Error("IsValidIDType must reject int32 to match the engine")
	}
	if !IsValidIDType(int64(7)) || !IsValidIDType("s") || !IsValidIDType(primitive.NewObjectID()) {
		t.Error("IsValidIDType must accept ObjectID/string/int64")
	}
	if !hasRule(CheckID("__x__"), "id-reserved", SeverityBlock) {
		t.Error("reserved string _id should block")
	}
	if !hasRule(CheckID(strings.Repeat("a", MaxIDBytes+1)), "id-length", SeverityBlock) {
		t.Error("overlong string _id should block")
	}
}

func TestCheckDocument_ReservedFieldBlocks(t *testing.T) {
	// Firestore rejects __x__ field names; block until the operator approves the
	// sanitize-field-names remediation (previously this was mislabeled auto-fix but
	// nothing rewrote it — the __x__ fields silently failed at Firestore).
	doc := bson.M{"_id": primitive.NewObjectID(), "__weird__": 1}
	if !hasRule(CheckDocument(doc), "field-name-reserved", SeverityBlock) {
		t.Error("reserved field name should block")
	}
}

func TestCheckDocument_Depth(t *testing.T) {
	// Build a document nested deeper than MaxDepth.
	inner := bson.M{"leaf": 1}
	for i := 0; i < MaxDepth+2; i++ {
		inner = bson.M{"a": inner}
	}
	doc := bson.M{"_id": "x", "root": inner}
	if !hasRule(CheckDocument(doc), "nesting-depth", SeverityBlock) {
		t.Error("over-deep nesting should block")
	}
}

func TestCheckDocument_ValueSize(t *testing.T) {
	big := strings.Repeat("x", MaxValueBytes+10)
	doc := bson.M{"_id": "x", "blob": big}
	if !hasRule(CheckDocument(doc), "value-size", SeverityBlock) {
		t.Error("oversized string value should block")
	}
}

func TestCheckDocument_Clean(t *testing.T) {
	doc := bson.M{"_id": primitive.NewObjectID(), "name": "ok", "n": int64(5),
		"nested": bson.M{"a": 1, "b": []interface{}{1, 2, 3}}}
	for _, f := range CheckDocument(doc) {
		if f.Severity == SeverityBlock {
			t.Errorf("clean doc produced a blocking finding: %s", f)
		}
	}
}

func TestCheckIndexBudget(t *testing.T) {
	if hasRule(CheckIndexBudget("db", 10), "index-budget", SeverityBlock) {
		t.Error("small index count should not block")
	}
	if !hasRule(CheckIndexBudget("db", MaxIndexesPerDB+1), "index-budget", SeverityBlock) {
		t.Error("over-budget index count should block")
	}
	if !hasRule(CheckIndexBudget("db", MaxIndexesPerDB*95/100), "index-budget", SeverityWarn) {
		t.Error("near-budget index count should warn")
	}
}

func TestCheckDatabaseBudget(t *testing.T) {
	if hasRule(CheckDatabaseBudget(50), "database-budget", SeverityBlock) {
		t.Error("50 dbs should be fine")
	}
	if !hasRule(CheckDatabaseBudget(MaxDatabasesPerProject+1), "database-budget", SeverityBlock) {
		t.Error("over 100 dbs should block")
	}
}
