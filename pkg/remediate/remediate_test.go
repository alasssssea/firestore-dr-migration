package remediate

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestSanitizeReserved(t *testing.T) {
	cases := map[string]string{
		"__reserved__": "_reserved_",
		"__proto__":    "_proto_",
		"___x___":      "_x_",
		"normal":       "normal",
	}
	for in, want := range cases {
		if got := sanitizeReserved(in); got != want {
			t.Errorf("sanitizeReserved(%q)=%q want %q", in, got, want)
		}
		if reservedName.MatchString(sanitizeReserved(in)) {
			t.Errorf("sanitizeReserved(%q) still reserved", in)
		}
	}
}

func TestRewriteID(t *testing.T) {
	p := &Plan{}
	p.Add(Remediation{Rule: "id-reserved", Database: "d", Collection: "orders"})
	out := p.ApplyDoc("d", "orders", bson.M{"_id": "__proto__", "x": 1})
	if out["_id"] != "_proto_" {
		t.Fatalf("_id=%v want _proto_", out["_id"])
	}
}

func TestSerializeSubtreeDepth(t *testing.T) {
	// Build 25 levels of nesting.
	var nested interface{} = bson.M{"v": 1}
	for i := 0; i < 25; i++ {
		nested = bson.M{"level": nested}
	}
	p := &Plan{}
	p.Add(Remediation{Rule: "nesting-depth", Database: "d", Collection: "deep"})
	out := p.ApplyDoc("d", "deep", bson.M{"data": nested})
	if got := maxDepth(out, 1); got > MaxDepth {
		t.Fatalf("post-fix depth %d exceeds %d", got, MaxDepth)
	}
}

func TestSerializeSubtreePath(t *testing.T) {
	seg := strings.Repeat("k", 200)
	var v interface{} = 1
	for i := 0; i < 8; i++ {
		v = bson.M{seg: v}
	}
	p := &Plan{}
	p.Add(Remediation{Rule: "field-path-length", Database: "d", Collection: "lp"})
	out := p.ApplyDoc("d", "lp", bson.M{"data": v})
	if got := maxPath(out, ""); got > MaxFieldPathBytes {
		t.Fatalf("post-fix path %d exceeds %d", got, MaxFieldPathBytes)
	}
}

func TestRenameField(t *testing.T) {
	long := strings.Repeat("f", 2000)
	p := &Plan{}
	p.Add(Remediation{Rule: "field-name-length", Database: "d", Collection: "wf"})
	out := p.ApplyDoc("d", "wf", bson.M{long: 1, "ok": 2})
	for k := range out {
		if len(k) > MaxFieldNameBytes {
			t.Fatalf("field name %d still exceeds %d", len(k), MaxFieldNameBytes)
		}
	}
	if _, ok := out["ok"]; !ok {
		t.Fatal("short field lost")
	}
}

func TestChunkValue(t *testing.T) {
	big := strings.Repeat("A", 5*1024*1024)
	p := &Plan{}
	p.Add(Remediation{Rule: "value-size", Database: "d", Collection: "products"})
	out := p.ApplyDoc("d", "products", bson.M{"blob": big})
	m, ok := out["blob"].(bson.M)
	if !ok {
		t.Fatalf("blob not chunked: %T", out["blob"])
	}
	parts := m["parts"].(bson.A)
	for _, pt := range parts {
		if len(pt.(string)) > MaxValueBytes {
			t.Fatalf("chunk part %d exceeds %d", len(pt.(string)), MaxValueBytes)
		}
	}
	if m["len"].(int) != len(big) {
		t.Fatalf("recorded len wrong")
	}
}

func TestCollectionNameRename(t *testing.T) {
	p := &Plan{}
	p.Add(Remediation{Rule: "collection-name-reserved", Database: "d", Collection: "__reserved__"})
	if got := p.CollectionName("d", "__reserved__"); got != "_reserved_" {
		t.Fatalf("CollectionName=%q want _reserved_", got)
	}
	if got := p.CollectionName("d", "normal"); got != "normal" {
		t.Fatalf("unmapped collection changed: %q", got)
	}
}

func TestApplyDocAuditReportsChanges(t *testing.T) {
	plan := &Plan{}
	plan.Add(Remediation{Rule: "id-reserved", Database: "d", Collection: "c"})
	plan.Add(Remediation{Rule: "value-size", Database: "d", Collection: "c"})
	plan.Add(Remediation{Rule: "field-name-length", Database: "d", Collection: "c"})

	long := strings.Repeat("f", 2000)
	big := strings.Repeat("A", 5*1024*1024)
	_, changes := plan.ApplyDocAudit("d", "c", bson.M{"_id": "__proto__", "blob": big, long: 1, "ok": 2})

	byStrategy := map[Strategy]Change{}
	for _, c := range changes {
		byStrategy[c.Strategy] = c
	}
	if c, ok := byStrategy[StrategyRewriteID]; !ok || c.Path != "_id" {
		t.Errorf("missing/incorrect rewrite-id change: %+v", byStrategy)
	}
	if c, ok := byStrategy[StrategyChunkValue]; !ok || c.Path != "blob" {
		t.Errorf("missing/incorrect chunk-value change: %+v", byStrategy)
	}
	if _, ok := byStrategy[StrategyRenameField]; !ok {
		t.Errorf("missing rename-field change: %+v", byStrategy)
	}
	// An unchanged doc yields no changes.
	if _, ch := plan.ApplyDocAudit("d", "c", bson.M{"_id": "fine", "ok": 2}); len(ch) != 0 {
		t.Errorf("expected no changes for clean doc, got %+v", ch)
	}
}

func TestSanitizeFieldNames(t *testing.T) {
	p := &Plan{}
	p.Add(Remediation{Rule: "field-name-reserved", Database: "d", Collection: "c"})
	doc := bson.M{
		"_id":    "__proto__", // left to rewrite-id, not touched here
		"__a__":  1,
		"nested": bson.M{"__b__": 2, "ok": 3},
		"arr":    bson.A{bson.M{"__c__": 4}},
	}
	out, changes := p.ApplyDocAudit("d", "c", doc)

	if _, bad := out["__a__"]; bad {
		t.Errorf("top-level reserved field not sanitized: %v", out)
	}
	if out["_a_"] != 1 {
		t.Errorf("_a_ missing/wrong: %v", out["_a_"])
	}
	n := out["nested"].(bson.M)
	if _, bad := n["__b__"]; bad || n["_b_"] != 2 || n["ok"] != 3 {
		t.Errorf("nested not sanitized: %v", n)
	}
	a := out["arr"].(bson.A)
	inner := a[0].(bson.M)
	if _, bad := inner["__c__"]; bad || inner["_c_"] != 4 {
		t.Errorf("array-element field not sanitized: %v", inner)
	}
	// _id is NOT the sanitize strategy's job.
	if out["_id"] != "__proto__" {
		t.Errorf("_id should be untouched by sanitize-field-names: %v", out["_id"])
	}
	// Every reserved key produced an audited change with before/after/violation.
	if len(changes) != 3 {
		t.Fatalf("expected 3 changes, got %d: %+v", len(changes), changes)
	}
	for _, c := range changes {
		if c.Before == "" || c.After == "" || c.Violation == "" {
			t.Errorf("change missing before/after/violation: %+v", c)
		}
		if reservedName.MatchString(c.After) {
			t.Errorf("after value still reserved: %q", c.After)
		}
	}
	// Source untouched.
	if _, ok := doc["__a__"]; !ok {
		t.Errorf("source mutated")
	}
}

func TestSanitizeFieldNamesCollision(t *testing.T) {
	p := &Plan{}
	p.Add(Remediation{Rule: "field-name-reserved", Database: "d", Collection: "c"})
	// Both "__x__" and the sanitized target "_x_" already present → no clobber.
	out := p.ApplyDoc("d", "c", bson.M{"__x__": 1, "_x_": 2})
	if out["_x_"] != 2 {
		t.Errorf("existing _x_ clobbered: %v", out)
	}
	if len(out) != 2 {
		t.Errorf("field dropped on collision: %v", out)
	}
}

func TestWildcardCollection(t *testing.T) {
	p := &Plan{}
	p.Add(Remediation{Rule: "field-name-reserved", Database: "d", Collection: WildcardCollection})
	// Applies to ANY collection in db d.
	for _, coll := range []string{"users", "orders", "anything"} {
		out := p.ApplyDoc("d", coll, bson.M{"__meta__": 1})
		if out["_meta_"] != 1 {
			t.Errorf("wildcard not applied to %q: %v", coll, out)
		}
	}
	// But not to another database.
	if out := p.ApplyDoc("other", "users", bson.M{"__meta__": 1}); out["_meta_"] == 1 {
		t.Errorf("wildcard leaked across databases: %v", out)
	}
}

func TestChangeAuditFieldsPopulated(t *testing.T) {
	p := &Plan{}
	p.Add(Remediation{Rule: "id-reserved", Database: "d", Collection: "c"})
	p.Add(Remediation{Rule: "value-size", Database: "d", Collection: "c"})
	big := strings.Repeat("A", 5*1024*1024)
	_, changes := p.ApplyDocAudit("d", "c", bson.M{"_id": "__proto__", "blob": big})
	for _, c := range changes {
		if c.Before == "" || c.After == "" || c.Violation == "" {
			t.Errorf("%s change missing audit fields: %+v", c.Strategy, c)
		}
		// Large before values must be summarized, not stored whole.
		if len(c.Before) > 300 {
			t.Errorf("before not summarized (%d bytes): %s", len(c.Before), c.Strategy)
		}
	}
}

func TestAddIdempotentAndRemove(t *testing.T) {
	p := &Plan{}
	p.Add(Remediation{Rule: "value-size", Database: "d", Collection: "c"})
	p.Add(Remediation{Rule: "value-size", Database: "d", Collection: "c"})
	if len(p.Items) != 1 {
		t.Fatalf("Add not idempotent: %d items", len(p.Items))
	}
	if p.Items[0].Strategy != StrategyChunkValue {
		t.Fatalf("default strategy not filled: %q", p.Items[0].Strategy)
	}
	if !p.Remove("value-size", "d", "c") || len(p.Items) != 0 {
		t.Fatalf("Remove failed")
	}
}

/* ---- test helpers ---- */

func maxDepth(v interface{}, depth int) int {
	switch m := v.(type) {
	case bson.M:
		mx := depth
		for _, cv := range m {
			if d := maxDepth(cv, depth+1); d > mx {
				mx = d
			}
		}
		return mx
	default:
		return depth
	}
}

func maxPath(v interface{}, path string) int {
	m, ok := v.(bson.M)
	if !ok {
		return len(path)
	}
	mx := len(path)
	for k, cv := range m {
		np := k
		if path != "" {
			np = path + "." + k
		}
		if d := maxPath(cv, np); d > mx {
			mx = d
		}
	}
	return mx
}
