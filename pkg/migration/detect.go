package migration

import (
	"firestore-dr-migration/pkg/db"
)

// ReplicationDecision is the outcome of selecting a live-replication method for
// a detected source. This build targets Firestore (MongoDB compatibility) and
// supports change streams only, so Method is always "changestream".
type ReplicationDecision struct {
	Method  string // always "changestream"
	Warning string // non-empty when there is a caveat the operator should see
}

// ResolveReplicationMethod selects the live-replication method for a Firestore
// (MongoDB compatibility) source.
//
// Firestore has no replica-set / oplog concept — live replication is driven
// entirely by a change stream, which must be created manually on the source
// database beforehand (there is no gcloud command and no auto-enable). The tool
// cannot tell at this stage whether that change stream already exists, so the
// returned Warning is a reminder, not a hard error: full copy (-mode=migrate)
// never needs one; the live modes (live / live-only) do.
func ResolveReplicationMethod(info *db.SourceServerInfo) ReplicationDecision {
	return ReplicationDecision{
		Method:  "changestream",
		Warning: "Live modes (live / live-only) require a change stream enabled on the source database first. There is no gcloud command and no auto-enable — create it manually, per database, in the Google Cloud console (Firestore Studio → Change streams). Full copy (-mode=migrate) does not need one.",
	}
}
