package migration

import (
	"os"
	"path/filepath"
	"sort"

	"firestore-dr-migration/pkg/logger"
)

// residualStatePatterns are the per-run working-directory artifacts a migration
// leaves behind. Crucially they are named by pair INDEX (dlq-pair0.jsonl,
// initialMigrationState-pair1.json, oplogTimestamp-pair2.json, …) or by
// (db,collection) — never by anything tying them to the specific run that
// created them. So when a later run's database→index mapping differs from an
// earlier one's (e.g. run A migrated database_1/2/3 → pair0/1/2; run B migrates
// analytics/manytables/shop_prod → pair0/1/2), run B silently inherits run A's
// files and then:
//   - aborts a pair on a stale DLQ ("previous initial migration left failed
//     documents in the dead letter queue"), or
//   - hits a safety violation (oplog checkpoint exists, state file doesn't), or
//   - sees a stale "initial migration completed" state and SKIPS the initial
//     load, jumping straight to incremental against an empty target.
//
// CleanResidualState wipes them so every fresh start is genuinely fresh.
var residualStatePatterns = []string{
	"oplogTimestamp-*.json",       // legacy oplog position checkpoints
	"resumeToken-*.json",          // modern change-stream resume tokens
	"initialMigrationState-*.json", // per-pair initial-load status
	"dlq-*.jsonl",                 // dead-letter queues
	"id-mapping*.jsonl",           // original→new _id map
	"backfillCheckpoint-*.json",   // per-partition backfill progress
	RemediationAuditPath,          // remediation forensic log
}

// CleanResidualState removes all per-run migration artifacts from dir and resets
// the process-global remediation audit writer, returning the base names removed.
//
// It is meant for a FRESH start. The web console always starts a brand-new job
// (there is no resume-from-checkpoint button), so wiping stale state before each
// run is correct there. Do NOT call it on a CLI resume path, where the whole
// point is to continue from these files.
func CleanResidualState(dir string, log *logger.Logger) ([]string, error) {
	if dir == "" {
		dir = "."
	}
	var removed []string
	for _, pat := range residualStatePatterns {
		matches, err := filepath.Glob(filepath.Join(dir, pat))
		if err != nil {
			return removed, err // only ErrBadPattern; patterns are constant, so never in practice
		}
		for _, m := range matches {
			if err := os.Remove(m); err != nil && !os.IsNotExist(err) {
				if log != nil {
					log.Warnf("Could not remove residual state file %s: %v", m, err)
				}
				continue
			}
			removed = append(removed, filepath.Base(m))
		}
	}
	// Drop the cached audit fd so the next run reopens a fresh file even inside a
	// long-lived console process.
	ResetAuditLog()
	sort.Strings(removed)
	if log != nil && len(removed) > 0 {
		log.Infof("Fresh start: cleaned %d residual state file(s) from previous run(s): %v", len(removed), removed)
	}
	return removed, nil
}
