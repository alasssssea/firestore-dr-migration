package migration

import "firestore-dr-migration/pkg/logger"

// dlqActiveCount returns the DLQ's record count, treating a nil writer and the
// no-op writer as zero. Every replication path counts DLQ entries through this
// one helper so the "how many failed" number is computed identically everywhere.
func dlqActiveCount(dlq DLQ) int64 {
	if dlq == nil {
		return 0
	}
	if _, isNop := dlq.(*NopDLQWriter); isNop {
		return 0
	}
	return dlq.Count()
}

// resolveInitialMigrationOutcome computes the terminal status of the initial
// migration from the per-document failure count and the DLQ, and logs a summary.
//
// Non-blocking policy (single source of truth for all replication paths): a
// completed_with_failures outcome does NOT abort replication. Documents that
// violate a basic conversion rule (e.g. a field over Firestore's size limit, a
// reserved key/collection name) are captured in the DLQ with their error reason;
// every other document migrates normally and the caller proceeds straight to
// incremental replication. Failed documents are recovered afterwards with
// `-mode retry-dlq` (optionally re-reading the fixed source doc), or — while a
// live job is still running — automatically, when the user's source edit produces
// a change event that succeeds and writes a resolution tombstone.
//
// The returned status is still recorded in the initial-migration state file for
// reporting, and the pre-run resume gate continues to refuse to *resume* a
// completed_with_failures state (that guard prevents silently skipping an
// unfinished initial load); this helper only governs whether THIS run proceeds.
func resolveInitialMigrationOutcome(dlq DLQ, totalFailedCount int64, log *logger.Logger) (status string, dlqCount int64) {
	dlqCount = dlqActiveCount(dlq)
	if totalFailedCount > 0 || dlqCount > 0 {
		log.Warnf("Initial migration completed with %d failed document(s) captured in the dead-letter queue (DLQ total: %d). "+
			"Continuing replication — all other documents migrated normally and incremental replication will proceed. "+
			"Failed documents remain in the DLQ with their error reason and can be recovered with -mode retry-dlq.",
			totalFailedCount, dlqCount)
		return StatusCompletedWithFailures, dlqCount
	}
	return StatusCompleted, dlqCount
}
