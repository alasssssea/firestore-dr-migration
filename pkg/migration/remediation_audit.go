package migration

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"firestore-dr-migration/pkg/logger"
	"firestore-dr-migration/pkg/remediate"
)

// RemediationAuditPath is the JSONL file the migrator appends to whenever a
// remediation transform (pkg/remediate) actually changes a document or renames a
// collection on the write path. It is the forensic record of "what the fix
// changed", kept so a later data discrepancy in Firestore can be traced back to
// the exact transform that produced it. One JSON object per line.
//
// It joins the other per-run residual files (oplogTimestamp-*.json,
// initialMigrationState-*.json, dlq-*.jsonl, id-mapping*.jsonl) that should be
// cleaned before re-running a migration so audits from different runs don't mix.
const RemediationAuditPath = "remediation-audit.jsonl"

var (
	auditMu     sync.Mutex
	auditFile   *os.File
	auditFailed bool
	renameSeen  = map[string]bool{} // db|source → already logged this collection rename
)

// ResetAuditLog closes the process-global audit writer and clears its dedup
// state so the next run opens a fresh remediation-audit.jsonl. Without this, a
// long-lived console process would keep appending to (or holding an fd on) a
// prior run's audit file even after CleanResidualState removes it on disk.
func ResetAuditLog() {
	auditMu.Lock()
	defer auditMu.Unlock()
	if auditFile != nil {
		_ = auditFile.Close()
		auditFile = nil
	}
	auditFailed = false
	renameSeen = map[string]bool{}
}

// auditRecord is one line of RemediationAuditPath.
type auditRecord struct {
	Time             string             `json:"time"`
	Database         string             `json:"database"`
	Collection       string             `json:"collection"`
	TargetCollection string             `json:"targetCollection,omitempty"` // set when renamed
	DocID            interface{}        `json:"docId,omitempty"`
	OldDocID         interface{}        `json:"oldDocId,omitempty"` // set when _id was rewritten
	Changes          []remediate.Change `json:"changes"`
}

// openAuditLocked ensures auditFile is open. Caller must hold auditMu. Returns
// false if the file could not be opened (logged once, then suppressed).
func openAuditLocked(log *logger.Logger) bool {
	if auditFailed {
		return false
	}
	if auditFile != nil {
		return true
	}
	f, err := os.OpenFile(RemediationAuditPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		auditFailed = true
		if log != nil {
			log.Warnf("Could not open remediation audit log %s: %v — applied fixes will not be recorded", RemediationAuditPath, err)
		}
		return false
	}
	auditFile = f
	if log != nil {
		log.Infof("Recording applied remediations to %s (审计留档，便于日后复查)", RemediationAuditPath)
	}
	return true
}

// writeAuditLocked marshals and appends one record. Caller must hold auditMu.
func writeAuditLocked(rec auditRecord) {
	data, err := json.Marshal(rec)
	if err != nil {
		return
	}
	_, _ = auditFile.Write(append(data, '\n'))
}

// recordRemediations appends a per-document audit line describing every change a
// remediation made to that document. A no-op when there are no changes. Safe for
// concurrent callers (all migrator workers share one audit file).
func recordRemediations(log *logger.Logger, db, coll, target string, docID, oldID interface{}, changes []remediate.Change) {
	if len(changes) == 0 {
		return
	}
	auditMu.Lock()
	defer auditMu.Unlock()
	if !openAuditLocked(log) {
		return
	}
	rec := auditRecord{
		Time:       time.Now().UTC().Format(time.RFC3339Nano),
		Database:   db,
		Collection: coll,
		DocID:      docID,
		Changes:    changes,
	}
	if target != "" && target != coll {
		rec.TargetCollection = target
	}
	if oldID != nil && fmt.Sprintf("%v", oldID) != fmt.Sprintf("%v", docID) {
		rec.OldDocID = oldID
	}
	writeAuditLocked(rec)
}

// recordCollectionRename logs a rename-collection remediation once per collection
// (the fix applies to every document, but the fact is per-collection). Deduped by
// (db, source) so it appears a single time in the audit log.
func recordCollectionRename(log *logger.Logger, db, source, target string) {
	if source == target {
		return
	}
	auditMu.Lock()
	defer auditMu.Unlock()
	key := db + "|" + source
	if renameSeen[key] {
		return
	}
	renameSeen[key] = true
	if !openAuditLocked(log) {
		return
	}
	if log != nil {
		log.Infof("Remediation: collection %s.%s → %s (reserved-name rewrite; app 需按新名访问)", db, source, target)
	}
	writeAuditLocked(auditRecord{
		Time:             time.Now().UTC().Format(time.RFC3339Nano),
		Database:         db,
		Collection:       source,
		TargetCollection: target,
		Changes: []remediate.Change{{
			Rule:     "collection-name-reserved",
			Strategy: remediate.StrategyRenameCollection,
			Path:     "",
			Detail:   fmt.Sprintf("%q → %q", source, target),
		}},
	})
}
