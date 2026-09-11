package migration

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// TestCleanResidualState proves the cleanup removes exactly the per-run artifacts
// (across every naming scheme) and leaves unrelated files untouched — so a fresh
// console Start never inherits a previous run's index-named DLQ/state.
func TestCleanResidualState(t *testing.T) {
	dir := t.TempDir()

	residual := []string{
		"oplogTimestamp-pair0.json",
		"oplogTimestamp-global.json",
		"resumeToken-pair1.json",
		"initialMigrationState-pair0.json",
		"initialMigrationState-global.json",
		"dlq-pair0.jsonl",
		"dlq-pair2.jsonl",
		"id-mapping.jsonl",
		"id-mapping-pair1.jsonl",
		"backfillCheckpoint-analytics-events_big-partition-0-of-4.json",
		RemediationAuditPath,
	}
	keep := []string{
		"config.json",
		"console.log",
		"remediation-plan.json", // the operator's fix plan must survive a rerun
		"notes.txt",
	}
	for _, f := range append(append([]string{}, residual...), keep...) {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o644); err != nil {
			t.Fatalf("seed %s: %v", f, err)
		}
	}

	removed, err := CleanResidualState(dir, nil)
	if err != nil {
		t.Fatalf("CleanResidualState: %v", err)
	}

	wantRemoved := append([]string{}, residual...)
	sort.Strings(wantRemoved)
	if len(removed) != len(wantRemoved) {
		t.Fatalf("removed %d files, want %d: %v", len(removed), len(wantRemoved), removed)
	}
	for i := range wantRemoved {
		if removed[i] != wantRemoved[i] {
			t.Errorf("removed[%d]=%q want %q", i, removed[i], wantRemoved[i])
		}
	}
	for _, f := range residual {
		if _, err := os.Stat(filepath.Join(dir, f)); !os.IsNotExist(err) {
			t.Errorf("residual file %s should have been removed", f)
		}
	}
	for _, f := range keep {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("unrelated file %s must NOT be removed: %v", f, err)
		}
	}
}
