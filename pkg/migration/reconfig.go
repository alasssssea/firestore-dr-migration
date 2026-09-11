package migration

import (
	"fmt"

	"firestore-dr-migration/pkg/config"
)

// ReconfigRequest carries a live tuning change from the console. All fields are
// optional; only the ones that are set are applied.
//
//   - Collection == "" → a GLOBAL change: mutate the config defaults (and, if
//     given, resize the live collection-concurrency semaphore). Affects every
//     collection dispatched from now on; collections already loading are untouched.
//   - Collection != "" → a per-collection override upserted into that pair's
//     CollectionTuning. It takes effect the next time that collection is dispatched,
//     so it helps a table still QUEUED behind the concurrency cap — a table already
//     loading keeps the tuning it started with (in-flight re-partitioning is Layer B).
type ReconfigRequest struct {
	Database   string // source database (required when Collection is set)
	Collection string // source collection; empty = global

	ConcurrentCollections int // <=0 = leave unchanged

	// Partition knobs. Zero = leave unchanged.
	MaxReadPartitions   int
	WorkersPerPartition int
	MinDocsPerPartition int
}

// Reconfig applies a live tuning change to a running job without restarting it or
// touching any persisted state. It never disturbs an in-flight collection: new
// values are read by effectivePartitioning only when the next collection is
// dispatched, and a ConcurrentCollections change resizes the semaphore in a way
// that never preempts current holders.
func (m *Migrator) Reconfig(req ReconfigRequest) error {
	// Live collection-concurrency cap (affects future Acquire only).
	if req.ConcurrentCollections > 0 {
		m.cfgMu.Lock()
		m.config.ConcurrentCollections = req.ConcurrentCollections
		sem := m.collSem
		m.cfgMu.Unlock()
		if sem != nil {
			sem.SetLimit(req.ConcurrentCollections)
		}
	}

	if req.Collection == "" {
		// Global partition-knob defaults.
		m.cfgMu.Lock()
		if req.MaxReadPartitions > 0 {
			m.config.MaxReadPartitions = req.MaxReadPartitions
		}
		if req.WorkersPerPartition > 0 {
			m.config.WorkersPerPartition = req.WorkersPerPartition
		}
		if req.MinDocsPerPartition > 0 {
			m.config.MinDocsPerPartition = req.MinDocsPerPartition
		}
		m.cfgMu.Unlock()
		return nil
	}

	// Per-collection override upsert.
	if req.Database == "" {
		return fmt.Errorf("database is required for a per-collection reconfig")
	}
	m.cfgMu.Lock()
	defer m.cfgMu.Unlock()
	for i := range m.config.DatabasePairs {
		p := &m.config.DatabasePairs[i]
		if p.Source.Database != req.Database {
			continue
		}
		if p.Target.CollectionTuning == nil {
			p.Target.CollectionTuning = map[string]config.CollectionTuning{}
		}
		ov := p.Target.CollectionTuning[req.Collection]
		if req.MaxReadPartitions > 0 {
			ov.MaxReadPartitions = req.MaxReadPartitions
		}
		if req.WorkersPerPartition > 0 {
			ov.WorkersPerPartition = req.WorkersPerPartition
		}
		if req.MinDocsPerPartition > 0 {
			ov.MinDocsPerPartition = req.MinDocsPerPartition
		}
		// A table the operator is explicitly tuning should read in parallel even if
		// its doc-count sits below the global trigger (the byte-heavy straggler case).
		if ov.ParallelReadsEnabled == nil {
			t := true
			ov.ParallelReadsEnabled = &t
		}
		if ov.MinDocsForParallelReads == 0 {
			ov.MinDocsForParallelReads = 1 // ensure the parallel gate opens for it
		}
		p.Target.CollectionTuning[req.Collection] = ov
		return nil
	}
	return fmt.Errorf("no database pair for source database %q", req.Database)
}

// setCollSem registers the live collection-concurrency semaphore so Reconfig can
// resize it. Called by the change-stream backfill dispatcher at start.
func (m *Migrator) setCollSem(s *resizableSem) {
	m.cfgMu.Lock()
	m.collSem = s
	m.cfgMu.Unlock()
}

// clearCollSem detaches the semaphore when the dispatcher returns, but only if it
// is still the one we registered (guards against clobbering a newer run).
func (m *Migrator) clearCollSem(s *resizableSem) {
	m.cfgMu.Lock()
	if m.collSem == s {
		m.collSem = nil
	}
	m.cfgMu.Unlock()
}
