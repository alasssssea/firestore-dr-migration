package migration

import (
	"firestore-dr-migration/pkg/db"
)

// ReplicationDecision is the outcome of auto-selecting a replication method
// from a detected source server.
type ReplicationDecision struct {
	Method  string // always "changestream"
	Warning string // non-empty when there is a caveat the operator should see
}

// ResolveReplicationMethod maps a detected source server to the live
// replication method. This build supports change streams only, so the Method is
// always "changestream".
//
// Change streams require a replica set. If the source is a standalone, the
// returned Warning explains that only full migration works.
func ResolveReplicationMethod(info *db.SourceServerInfo) ReplicationDecision {
	d := ReplicationDecision{Method: "changestream"}

	if !info.IsReplicaSet {
		d.Warning = "source is not a replica set; live replication (change streams) requires a replica set. Only full migration (-mode=migrate) is supported for a standalone source."
	}
	return d
}
