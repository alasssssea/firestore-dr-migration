package migration

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"firestore-dr-migration/pkg/logger"
	"firestore-dr-migration/pkg/remediate"
	"go.mongodb.org/mongo-driver/bson"
)

// withCleanAuditDir chdirs into a temp dir and resets the audit singleton so a
// test's remediation writes land in an isolated remediation-audit.jsonl and never
// pollute the package directory. It restores everything on cleanup.
func withCleanAuditDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	orig, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	auditMu.Lock()
	auditFile = nil
	auditFailed = false
	renameSeen = map[string]bool{}
	auditMu.Unlock()
	t.Cleanup(func() {
		auditMu.Lock()
		if auditFile != nil {
			auditFile.Close()
			auditFile = nil
		}
		auditMu.Unlock()
		os.Chdir(orig)
	})
	return dir
}

// newPlanTransformer builds a FieldTransformer whose only active transform is the
// given remediation plan (the boolean field transforms are all off), so these
// tests isolate the migration-time application of pkg/remediate.
func newPlanTransformer(plan *remediate.Plan) *FieldTransformer {
	return &FieldTransformer{log: logger.New(), plan: plan}
}

// TestRemediationAppliedAtMigration proves the operator-approved plan is applied
// on the write path — i.e. the assessment's "apply fix" simulation and the real
// migration produce the same clean document. Source is never mutated (Transform
// returns a new value).
func TestRemediationAppliedAtMigration(t *testing.T) {
	withCleanAuditDir(t)
	plan := &remediate.Plan{}
	plan.Add(remediate.Remediation{Rule: "id-reserved", Database: "d", Collection: "orders"})
	plan.Add(remediate.Remediation{Rule: "value-size", Database: "d", Collection: "orders"})
	tr := newPlanTransformer(plan)

	big := strings.Repeat("A", 5*1024*1024)
	doc := bson.M{"_id": "__proto__", "blob": big, "keep": 1}
	out, err := tr.Transform(doc, "d", "orders", "__proto__")
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}
	m, ok := out.(bson.M)
	if !ok {
		t.Fatalf("expected bson.M, got %T", out)
	}
	if m["_id"] != "_proto_" {
		t.Errorf("_id = %v, want _proto_", m["_id"])
	}
	if _, ok := m["blob"].(bson.M); !ok {
		t.Errorf("blob not chunked: %T", m["blob"])
	}
	if m["keep"] != 1 {
		t.Errorf("unrelated field mangled: %v", m["keep"])
	}
	// Source untouched.
	if doc["_id"] != "__proto__" {
		t.Errorf("source _id mutated: %v", doc["_id"])
	}
}

// TestNoPlanNoChange confirms a collection with no remediation is passed through
// untouched (and cheaply — no order-losing conversion) when no field transforms
// are enabled.
func TestNoPlanNoChange(t *testing.T) {
	tr := newPlanTransformer(&remediate.Plan{})
	doc := bson.D{{Key: "_id", Value: "__proto__"}, {Key: "x", Value: 1}}
	out, err := tr.Transform(doc, "d", "orders", "__proto__")
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}
	if _, ok := out.(bson.D); !ok {
		t.Fatalf("expected untouched bson.D, got %T", out)
	}
}

// TestRemediationAuditLog proves that applying a plan on the write path writes a
// forensic JSONL record of exactly what changed, per document.
func TestRemediationAuditLog(t *testing.T) {
	dir := withCleanAuditDir(t)

	plan := &remediate.Plan{}
	plan.Add(remediate.Remediation{Rule: "id-reserved", Database: "d", Collection: "orders"})
	tr := newPlanTransformer(plan)

	if _, err := tr.Transform(bson.M{"_id": "__proto__", "x": 1}, "d", "orders", "__proto__"); err != nil {
		t.Fatalf("Transform: %v", err)
	}
	// Flush the shared handle so the read below sees the line.
	auditMu.Lock()
	if auditFile != nil {
		auditFile.Sync()
	}
	auditMu.Unlock()

	f, err := os.Open(filepath.Join(dir, RemediationAuditPath))
	if err != nil {
		t.Fatalf("audit log not written: %v", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		t.Fatal("audit log empty")
	}
	var rec auditRecord
	if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
		t.Fatalf("bad audit JSON: %v", err)
	}
	if rec.Database != "d" || rec.Collection != "orders" {
		t.Errorf("wrong location: %+v", rec)
	}
	if rec.OldDocID != "__proto__" || rec.DocID != "_proto_" {
		t.Errorf("expected _id __proto__→_proto_, got old=%v new=%v", rec.OldDocID, rec.DocID)
	}
	if len(rec.Changes) != 1 || rec.Changes[0].Path != "_id" {
		t.Errorf("expected one _id change, got %+v", rec.Changes)
	}
}

// TestAuditLogForensicDetail proves the write-path audit JSONL captures the three
// things the operator asked for on every fixed field: 原数据(before), 报错(violation),
// 修完(after) — including the systemic sanitize-field-names case registered DB-wide
// via the wildcard collection.
func TestAuditLogForensicDetail(t *testing.T) {
	dir := withCleanAuditDir(t)

	plan := &remediate.Plan{}
	plan.Add(remediate.Remediation{Rule: "field-name-reserved", Database: "d", Collection: remediate.WildcardCollection})
	tr := newPlanTransformer(plan)

	// Any collection in db d is covered by the wildcard.
	doc := bson.M{"_id": "ok", "__meta__": "hello", "nested": bson.M{"__tag__": 1}}
	if _, err := tr.Transform(doc, "d", "anything", "ok"); err != nil {
		t.Fatalf("Transform: %v", err)
	}
	auditMu.Lock()
	if auditFile != nil {
		auditFile.Sync()
	}
	auditMu.Unlock()

	f, err := os.Open(filepath.Join(dir, RemediationAuditPath))
	if err != nil {
		t.Fatalf("audit log not written: %v", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		t.Fatal("audit log empty")
	}
	var rec auditRecord
	if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
		t.Fatalf("bad audit JSON: %v", err)
	}
	if rec.Collection != "anything" {
		t.Errorf("wildcard did not apply to arbitrary collection: %+v", rec)
	}
	byBefore := map[string]remediate.Change{}
	for _, c := range rec.Changes {
		if c.Before == "" || c.After == "" || c.Violation == "" {
			t.Errorf("change missing 原数据/报错/修完: %+v", c)
		}
		byBefore[c.Before] = c
	}
	if c, ok := byBefore["__meta__"]; !ok || c.After != "_meta_" {
		t.Errorf("top-level __meta__→_meta_ not audited: %+v", rec.Changes)
	}
	if c, ok := byBefore["__tag__"]; !ok || c.After != "_tag_" || c.Path != "nested.__tag__" {
		t.Errorf("nested __tag__ not audited with path: %+v", rec.Changes)
	}
}

// TestSanitizeTargetName covers the collection-rename write-path helper used by
// both the initial and parallel migrators.
func TestSanitizeTargetName(t *testing.T) {
	plan := &remediate.Plan{}
	plan.Add(remediate.Remediation{Rule: "collection-name-reserved", Database: "d", Collection: "__reserved__"})
	tr := newPlanTransformer(plan)

	if got := tr.SanitizeTargetName("d", "__reserved__", "__reserved__"); got != "_reserved_" {
		t.Errorf("reserved target not sanitized: %q", got)
	}
	// A collection with no rename fix is left alone even if its name looks reserved.
	if got := tr.SanitizeTargetName("d", "other", "__other__"); got != "__other__" {
		t.Errorf("unmapped collection changed: %q", got)
	}
}
