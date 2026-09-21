// Package metrics implements the observability and control plane from DESIGN §4
// and §5: a thread-safe registry of per-collection progress and replication lag,
// a Job lifecycle model, a cooperative pause/resume/stop control, and HTTP
// surfaces (/healthz, /readyz, /metrics, JSON status, and an SSE stream) fronted
// by a small embedded dashboard.
package metrics

import (
	"context"
	"sort"
	"sync"
	"time"

	"firestore-dr-migration/pkg/progress"
)

// CollectionMetric is the latest progress/lag for one collection within a job.
type CollectionMetric struct {
	Job        string            `json:"job"`
	Database   string            `json:"database"`
	Collection string            `json:"collection"`
	Phase      string            `json:"phase"` // "initial", "live", "error", or "stopped"
	Snapshot   progress.Snapshot `json:"snapshot"`
	// LagSeconds is the replication lag for live phase (seconds behind source);
	// -1 when not applicable / unknown / idle (caught up).
	LagSeconds float64 `json:"lagSeconds"`
	// Failed is the cumulative count of failed writes for this collection in the
	// live phase (change events that did not apply). Non-terminal failures are
	// retried; terminal ones also land in the DLQ. Zero for initial phase. The
	// console uses this, together with the DLQ count, as a cutover-readiness gate.
	Failed int64 `json:"failed,omitempty"`
	// Error carries a human-readable reason when Phase is "error" (e.g. a pair
	// that could not start replication). Empty otherwise.
	Error string `json:"error,omitempty"`
	// Source/Target document counts and their difference, filled during the live
	// phase by a slow count-reconciliation poller (see observer.pollCounts). Unlike
	// LagSeconds (a change-stream event-time lag that reads ~0 even when documents
	// are silently missing), CountDiff is the ground-truth "behind by N docs" gap —
	// the only signal that catches a backfill that dropped pre-token documents.
	// CountsKnown is false until the first reconciliation lands (UI shows "对账中…").
	SourceCount int64 `json:"sourceCount"`
	TargetCount int64 `json:"targetCount"`
	CountDiff   int64 `json:"countDiff"` // SourceCount - TargetCount (may be <0 if target briefly ahead)
	CountsKnown bool  `json:"countsKnown"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

// collCounts holds the latest source/target document counts for one collection,
// stored separately from CollectionMetric so the fast 2s live poller (which
// replaces the whole metric each tick) never clobbers the slow count poller's
// values; they are merged in on read.
type collCounts struct {
	source int64
	target int64
	known  bool
	at     time.Time
}

// JobState is a point in the job lifecycle state machine (DESIGN §5).
type JobState string

const (
	StateCreated     JobState = "created"
	StateAssessing   JobState = "assessing"
	StateInitialLoad JobState = "initial-load"
	StateLive        JobState = "live"
	StateVerifying   JobState = "verifying"
	StateDone        JobState = "done"
	StateFailed      JobState = "failed"
	StatePaused      JobState = "paused"
)

// Job is a tracked migration run.
type Job struct {
	ID        string    `json:"id"`
	Mode      string    `json:"mode"`
	State     JobState  `json:"state"`
	Error     string    `json:"error,omitempty"`
	StartedAt time.Time `json:"startedAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// IndexProgress captures the deferred index-build phase for a job. Firestore
// builds indexes serially and after the data load, so this surfaces "M of N
// built (currently building coll[name])" to the console. Total==0 means no
// index build has started (the UI hides the row).
type IndexProgress struct {
	Job       string    `json:"job"`
	Total     int       `json:"total"`
	Done      int       `json:"done"`
	Building  string    `json:"building"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// Registry holds all live metrics and job state. Safe for concurrent use.
type Registry struct {
	mu            sync.RWMutex
	collections   map[string]CollectionMetric // key: job\x00db\x00coll
	counts        map[string]collCounts       // key: job\x00db\x00coll (live count reconciliation)
	jobs          map[string]*Job
	indexProgress map[string]IndexProgress // key: job
	ready         bool
	now           func() time.Time

	// Cutover thresholds, seeded from config, echoed to the console so the
	// frontend computes the ready/amber/red signal with the same numbers the
	// engine uses. Zero means "use the frontend default".
	cutoverLagThreshold float64
	cutoverStableChecks int
}

// NewRegistry creates an empty registry marked not-ready.
func NewRegistry() *Registry {
	return &Registry{
		collections:   make(map[string]CollectionMetric),
		counts:        make(map[string]collCounts),
		jobs:          make(map[string]*Job),
		indexProgress: make(map[string]IndexProgress),
		now:           time.Now,
	}
}

func collKey(job, db, coll string) string { return job + "\x00" + db + "\x00" + coll }

// SetCollection records the latest metric for a collection.
func (r *Registry) SetCollection(m CollectionMetric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m.UpdatedAt.IsZero() {
		m.UpdatedAt = r.now()
	}
	r.collections[collKey(m.Job, m.Database, m.Collection)] = m
}

// SetCollectionCounts records the latest source/target document counts for a
// collection, merged into its metric on read. Stored in a side map so the fast
// live poller's SetCollection (which replaces the whole metric every 2s) does not
// wipe these slow-cadence counts. No-op for a job/db/coll that never appears.
func (r *Registry) SetCollectionCounts(job, db, coll string, source, target int64) {
	r.mu.Lock()
	r.counts[collKey(job, db, coll)] = collCounts{source: source, target: target, known: true, at: r.now()}
	r.mu.Unlock()
}

// Collections returns all collection metrics, sorted for stable output. Any
// recorded source/target counts are merged into each metric here.
func (r *Registry) Collections() []CollectionMetric {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]CollectionMetric, 0, len(r.collections))
	for k, m := range r.collections {
		if c, ok := r.counts[k]; ok && c.known {
			m.SourceCount = c.source
			m.TargetCount = c.target
			m.CountDiff = c.source - c.target
			m.CountsKnown = true
		}
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Job != out[j].Job {
			return out[i].Job < out[j].Job
		}
		if out[i].Database != out[j].Database {
			return out[i].Database < out[j].Database
		}
		return out[i].Collection < out[j].Collection
	})
	return out
}

// SetIndexProgress records the latest deferred index-build progress for a job.
func (r *Registry) SetIndexProgress(p IndexProgress) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p.UpdatedAt.IsZero() {
		p.UpdatedAt = r.now()
	}
	r.indexProgress[p.Job] = p
}

// IndexProgressAll returns all jobs' index-build progress, sorted by job.
func (r *Registry) IndexProgressAll() []IndexProgress {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]IndexProgress, 0, len(r.indexProgress))
	for _, p := range r.indexProgress {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Job < out[j].Job })
	return out
}

// UpsertJob creates or updates a job's state.
func (r *Registry) UpsertJob(id, mode string, state JobState) *Job {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	j, ok := r.jobs[id]
	if !ok {
		j = &Job{ID: id, Mode: mode, StartedAt: now}
		r.jobs[id] = j
	}
	j.State = state
	j.UpdatedAt = now
	return j
}

// FailJob marks a job failed with an error message.
func (r *Registry) FailJob(id string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if j, ok := r.jobs[id]; ok {
		j.State = StateFailed
		if err != nil {
			j.Error = err.Error()
		}
		j.UpdatedAt = r.now()
	}
}

// Jobs returns all jobs sorted by ID.
func (r *Registry) Jobs() []Job {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Job, 0, len(r.jobs))
	for _, j := range r.jobs {
		out = append(out, *j)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// PurgeInactive removes finished jobs (done/failed) together with their
// collection and index-progress rows, leaving any still-active job untouched.
// It returns the number of jobs purged. The console's session-reset calls this
// so a completed run's rows stop lingering on the dashboard. Callers must ensure
// no job is mid-run (the console gates reset on that) before relying on a clean
// slate.
func (r *Registry) PurgeInactive() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	dead := make(map[string]bool)
	for id, j := range r.jobs {
		if j.State == StateDone || j.State == StateFailed {
			dead[id] = true
			delete(r.jobs, id)
		}
	}
	for k, m := range r.collections {
		if dead[m.Job] {
			delete(r.collections, k)
			delete(r.counts, k)
		}
	}
	for id := range r.indexProgress {
		if dead[id] {
			delete(r.indexProgress, id)
		}
	}
	return len(dead)
}

// SetCutoverThresholds seeds the cutover-readiness thresholds echoed in Status.
// Called once at console startup from the loaded config.
func (r *Registry) SetCutoverThresholds(lagSeconds float64, stableChecks int) {
	r.mu.Lock()
	r.cutoverLagThreshold = lagSeconds
	r.cutoverStableChecks = stableChecks
	r.mu.Unlock()
}

// SetReady toggles readiness (used by /readyz).
func (r *Registry) SetReady(ready bool) {
	r.mu.Lock()
	r.ready = ready
	r.mu.Unlock()
}

// Ready reports readiness.
func (r *Registry) Ready() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ready
}

// Status is a consolidated JSON-friendly snapshot of the whole registry.
type Status struct {
	Ready         bool               `json:"ready"`
	Jobs          []Job              `json:"jobs"`
	Collections   []CollectionMetric `json:"collections"`
	IndexProgress []IndexProgress    `json:"indexProgress"`
	// Cutover thresholds so the frontend renders the readiness signal with the
	// engine's configured numbers (0 → frontend falls back to its own default).
	CutoverLagThreshold float64   `json:"cutoverLagThreshold"`
	CutoverStableChecks int       `json:"cutoverStableChecks"`
	Timestamp           time.Time `json:"timestamp"`
}

// Status assembles a consolidated snapshot.
func (r *Registry) Status() Status {
	r.mu.RLock()
	lagThreshold := r.cutoverLagThreshold
	stableChecks := r.cutoverStableChecks
	r.mu.RUnlock()
	return Status{
		Ready:               r.Ready(),
		Jobs:                r.Jobs(),
		Collections:         r.Collections(),
		IndexProgress:       r.IndexProgressAll(),
		CutoverLagThreshold: lagThreshold,
		CutoverStableChecks: stableChecks,
		Timestamp:           r.now(),
	}
}

// --- Cooperative control (pause/resume/stop) ---

// Control is a cooperative pause/resume/stop signal a worker polls at safe
// points (e.g. between batches). It does not forcibly interrupt in-flight work;
// it gates progression, so the migration always stops at a consistent boundary.
type Control struct {
	mu      sync.Mutex
	paused  bool
	stopped bool
	resume  chan struct{} // closed and replaced on each resume
}

// NewControl returns a running (not paused, not stopped) control.
func NewControl() *Control {
	return &Control{resume: make(chan struct{})}
}

// Pause requests the worker to hold at its next safe point.
func (c *Control) Pause() {
	c.mu.Lock()
	c.paused = true
	c.mu.Unlock()
}

// Resume releases a paused worker.
func (c *Control) Resume() {
	c.mu.Lock()
	if c.paused {
		c.paused = false
		close(c.resume)
		c.resume = make(chan struct{})
	}
	c.mu.Unlock()
}

// Stop requests the worker to terminate at its next safe point. A stopped
// control also unblocks any paused worker.
func (c *Control) Stop() {
	c.mu.Lock()
	c.stopped = true
	if c.paused {
		c.paused = false
		close(c.resume)
		c.resume = make(chan struct{})
	}
	c.mu.Unlock()
}

// Stopped reports whether a stop has been requested.
func (c *Control) Stopped() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopped
}

// ErrStopped is returned by Wait when a stop was requested.
var ErrStopped = errStopped{}

type errStopped struct{}

func (errStopped) Error() string { return "migration stopped by control" }

// Wait blocks while paused, returning ErrStopped if a stop was requested and the
// context's error if it is cancelled first. It returns nil to proceed. Call this
// at safe points between units of work.
func (c *Control) Wait(ctx context.Context) error {
	for {
		c.mu.Lock()
		if c.stopped {
			c.mu.Unlock()
			return ErrStopped
		}
		if !c.paused {
			c.mu.Unlock()
			return nil
		}
		ch := c.resume
		c.mu.Unlock()

		select {
		case <-ch:
			// re-check state on next loop iteration
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
