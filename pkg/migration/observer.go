package migration

import (
	"context"
	"strings"
	"time"

	"firestore-dr-migration/pkg/metrics"
	"firestore-dr-migration/pkg/progress"
)

// This file wires the migrator to the web console's metrics registry and
// cooperative control plane. It is entirely optional: when nothing is attached
// (the CLI case) every method here is a cheap nil-guarded no-op, so the engine
// behaves exactly as upstream. The console attaches a plane so it can render
// real-time per-phase progress and drive pause/resume/stop.

// AttachControlPlane wires the migrator to a metrics registry, a cooperative
// control, and a job id. Call before Start. Passing nil for reg/control leaves
// the migrator in pure-CLI mode.
func (m *Migrator) AttachControlPlane(reg *metrics.Registry, control *metrics.Control, jobID string) {
	m.reg = reg
	m.control = control
	m.jobID = jobID
}

// Close releases control-plane resources. Kept for API symmetry with the
// console's launch/teardown; currently a no-op because all observer goroutines
// are tied to the migration context and exit when it is cancelled.
func (m *Migrator) Close() {}

// controlPlaneAttached reports whether a console is observing this migrator.
func (m *Migrator) controlPlaneAttached() bool { return m.reg != nil }

// setJobState reflects a lifecycle transition on the job row when attached.
func (m *Migrator) setJobState(mode string, state metrics.JobState) {
	if m.reg == nil {
		return
	}
	m.reg.UpsertJob(m.jobID, mode, state)
}

// markState transitions the attached job's lifecycle state (preserving its
// mode, which UpsertJob only sets on first insert). No-op when detached.
func (m *Migrator) markState(state metrics.JobState) {
	if m.reg == nil {
		return
	}
	m.reg.UpsertJob(m.jobID, "", state)
}

// checkControl honors a cooperative pause/stop request at a safe point. It
// returns metrics.ErrStopped when a stop was requested, ctx.Err() on
// cancellation, or nil to proceed. No-op (nil) when no control is attached.
func (m *Migrator) checkControl(ctx context.Context) error {
	if m.control == nil {
		return nil
	}
	return m.control.Wait(ctx)
}

// watchStop turns a console Stop() into context cancellation. It returns a
// derived context the caller should use for the migration, plus a cancel to
// defer. When no control is attached it returns the original context unchanged.
func (m *Migrator) watchStop(ctx context.Context) (context.Context, context.CancelFunc) {
	if m.control == nil {
		return ctx, func() {}
	}
	derived, cancel := context.WithCancel(ctx)
	go func() {
		// Wait returns ErrStopped once Stop() is called; any other return
		// (nil on resume, ctx error) means we should also stop watching.
		for {
			if err := m.control.Wait(derived); err != nil {
				cancel()
				return
			}
			// Not stopped and not paused right now; avoid a busy loop.
			select {
			case <-derived.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}()
	return derived, cancel
}

// reportInitial publishes a full-load (全量/initial) progress row for a database.
// targetDocs is the number of documents scheduled (0 = still counting → the UI
// renders an indeterminate bar); doneDocs is written-so-far; docsPerSec is the
// live throughput.
func (m *Migrator) reportInitial(dbName, coll string, targetDocs, doneDocs int64, docsPerSec float64) {
	if m.reg == nil {
		return
	}
	snap := progress.Snapshot{
		TotalDocs:  targetDocs,
		DoneDocs:   doneDocs,
		DocsPerSec: docsPerSec,
		ETA:        time.Duration(-1),
	}
	if targetDocs > 0 {
		snap.PercentDocs = float64(doneDocs) / float64(targetDocs) * 100
		if snap.PercentDocs > 100 {
			snap.PercentDocs = 100
		}
	}
	m.reg.SetCollection(metrics.CollectionMetric{
		Job:        m.jobID,
		Database:   dbName,
		Collection: coll,
		Phase:      "initial",
		Snapshot:   snap,
		LagSeconds: -1,
	})
}

// reportLive publishes an incremental (增量/live) progress row for a collection.
// events is the cumulative change-event count applied, failed is the cumulative
// failed-write count, eventsPerSec is the live rate, and lagSeconds is how far
// behind the source the tail is (-1 if unknown/idle). The console combines
// failed with the file-based DLQ count to compute cutover readiness.
func (m *Migrator) reportLive(dbName, coll string, events, failed int64, eventsPerSec, lagSeconds float64) {
	if m.reg == nil {
		return
	}
	m.reg.SetCollection(metrics.CollectionMetric{
		Job:        m.jobID,
		Database:   dbName,
		Collection: coll,
		Phase:      "live",
		Snapshot: progress.Snapshot{
			DoneDocs:   events,
			DocsPerSec: eventsPerSec,
			ETA:        time.Duration(-1),
		},
		LagSeconds: lagSeconds,
		Failed:     failed,
	})
}

// reportIndexProgress publishes the deferred index-build progress (M of N built,
// plus the index currently under construction) for this job. total==0 clears/hides
// the row. No-op when detached.
func (m *Migrator) reportIndexProgress(total, done int, building string) {
	if m.reg == nil {
		return
	}
	m.reg.SetIndexProgress(metrics.IndexProgress{
		Job:      m.jobID,
		Total:    total,
		Done:     done,
		Building: building,
	})
}

// reportPairError publishes a visible "error" row for a whole database pair
// that could not start (or keep) replication — e.g. a blocked initial-migration
// state. Without this the failure only reached the logs and the database simply
// vanished from the console. No-op when detached.
func (m *Migrator) reportPairError(dbName string, err error) {
	if m.reg == nil || err == nil {
		return
	}
	msg := err.Error()
	m.reg.SetCollection(metrics.CollectionMetric{
		Job:        m.jobID,
		Database:   dbName,
		Collection: "(整库)",
		Phase:      "error",
		Error:      msg,
		LagSeconds: -1,
	})
}

// pollBackfill runs a 2s ticker that mirrors a BackfillStatsManager's
// per-namespace counters into the console as one "initial" (全量) row PER
// COLLECTION (db.coll), so the operator sees exactly which collection is still
// loading rather than a single "全量汇总" aggregate row — matching the
// per-collection contract the incremental path already follows. It stops when
// ctx is cancelled. Cheap no-op when no console is attached. Runs in its own
// goroutine; the caller need not wait for it. dbFallback is used only for
// namespaces that lack a "db." prefix.
func (m *Migrator) pollBackfill(ctx context.Context, dbFallback, _ string, sm *BackfillStatsManager) {
	if m.reg == nil || sm == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		// Per-namespace previous succeeded count + timestamp, for rate.
		prev := make(map[string]int64)
		prevT := time.Now()
		for {
			select {
			case <-ctx.Done():
				// Emit a final snapshot so each bar lands on its true value.
				m.emitBackfill(dbFallback, sm, prev, &prevT)
				return
			case <-ticker.C:
				m.emitBackfill(dbFallback, sm, prev, &prevT)
			}
		}
	}()
}

// emitBackfill publishes one 全量 (initial) row per source collection from the
// backfill manager's per-namespace snapshot. prev/prevT carry the previous
// succeeded counts and tick time so throughput is a per-collection delta rate.
//
// Handoff to the live phase: once a collection starts receiving change events
// (it appears in the incremental manager's per-namespace snapshot), the
// incremental poller owns its row and emits it as a 增量 (live) row on the SAME
// db.coll key. To avoid the two pollers fighting over that key every tick — which
// makes the row flicker between the 全量 and 增量 phases — the backfill poller
// stops emitting a namespace the moment it has gone live. A fully-loaded
// collection with no changes yet keeps its 全量 100% row (it never disappears);
// it flips to 增量 exactly once, when its first event lands.
func (m *Migrator) emitBackfill(dbFallback string, sm *BackfillStatsManager, prev map[string]int64, prevT *time.Time) {
	now := time.Now()
	dt := now.Sub(*prevT).Seconds()

	// Namespaces the incremental phase has taken over; skip them here.
	live := make(map[string]bool)
	if sm.incStats != nil {
		for _, ns := range sm.incStats.NamespaceStatsSnapshot() {
			live[ns.Namespace] = true
		}
	}

	for _, ns := range sm.NamespaceBackfillSnapshot() {
		if live[ns.Namespace] {
			continue // handed off to the live (增量) poller
		}
		// A collection that has only been counted (target set) but not yet read
		// or written still shows as a determinate 0% row; a truly empty one
		// (target 0, nothing read/written) is skipped so it doesn't linger as an
		// indeterminate bar.
		if ns.Target == 0 && ns.Succeeded == 0 && ns.Read == 0 {
			continue
		}
		dbName, coll := splitNamespace(ns.Namespace, dbFallback)
		var rate float64
		if dt > 0 {
			rate = float64(ns.Succeeded-prev[ns.Namespace]) / dt
		}
		prev[ns.Namespace] = ns.Succeeded
		m.reportInitial(dbName, coll, ns.Target, ns.Succeeded, rate)
	}
	*prevT = now
}

// pollIncremental mirrors an IncrementalStatsManager's per-namespace counters
// into the console as one "live" (增量) row PER COLLECTION (db.coll), so the
// operator sees exactly which collection is behind or failing at cutover time —
// rather than a single "增量汇总" aggregate row. The change-stream path tracks
// lag as a shared stream property, so every collection row carries the same
// recent end-to-end lag (-1 when idle/caught up). Cheap no-op when detached.
// dbFallback is used only for namespaces that lack a "db." prefix.
func (m *Migrator) pollIncremental(ctx context.Context, dbFallback, _ string, sm *IncrementalStatsManager) {
	if m.reg == nil || sm == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		// Per-namespace previous applied count + timestamp, for rate.
		prev := make(map[string]int64)
		prevT := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				lag := sm.RecentLagSeconds()
				dt := now.Sub(prevT).Seconds()
				for _, ns := range sm.NamespaceStatsSnapshot() {
					dbName, coll := splitNamespace(ns.Namespace, dbFallback)
					var rate float64
					if dt > 0 {
						rate = float64(ns.Processed-prev[ns.Namespace]) / dt
					}
					prev[ns.Namespace] = ns.Processed
					m.reportLive(dbName, coll, ns.Processed, ns.Failed, rate, lag)
				}
				prevT = now
			}
		}
	}()
}

// splitNamespace splits a "db.coll" namespace into database and collection. When
// the namespace has no dot (unexpected), it is treated as a bare collection name
// under dbFallback.
func splitNamespace(ns, dbFallback string) (string, string) {
	if i := strings.IndexByte(ns, '.'); i >= 0 {
		return ns[:i], ns[i+1:]
	}
	return dbFallback, ns
}
