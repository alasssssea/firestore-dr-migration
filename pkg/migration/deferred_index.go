package migration

import (
	"context"
	"sync"
	"time"

	"firestore-dr-migration/pkg/config"
	"firestore-dr-migration/pkg/db"
	"firestore-dr-migration/pkg/logger"
)

// DeferredIndexController encapsulates the ONE canonical rule that every migration
// path shares: indexes are built AFTER the data is loaded — never before or during
// the initial write flood. Firestore re-indexes on every inserted document, so an
// up-front index build makes the whole backfill contend with re-indexing and drag
// on for minutes with no visible progress. For live paths the build is deferred
// further, until replication lag has settled, so it does not fight the change-stream
// catch-up burst either; for full-only paths (no CDC) it runs the moment the
// backfill finishes.
//
// This logic used to live only in the legacy oplog replicator, which is why the
// modern change-stream path silently kept building indexes up front (blocking the
// entire backfill behind a multi-minute index build, so the console showed nothing
// running). Centralizing the timing/trigger/once/progress logic here is what stops
// the modern and legacy paths from drifting apart again: each path injects only its
// version-specific build primitive (modern: Migrator.syncIndexes; legacy:
// syncIndexesLegacy); the WHEN is single-source.
type DeferredIndexController struct {
	migrator     *Migrator
	targetDB     *db.MongoDB
	log          *logger.Logger
	lagThreshold float64
	stableChecks int
	concurrency  int
	// build launches the (async) index creation on the target. It must NOT block
	// on completion — the controller waits via WaitForIndexCreation itself so the
	// wait is uniform across paths.
	build func(ctx context.Context)

	once         sync.Once
	lowLagStreak int // consecutive caught-up samples seen by Observe
}

// NewDeferredIndexController wires a controller from the shared config knobs
// (IndexBuildLagThresholdSeconds / IndexBuildLagStableChecks / IndexConcurrency).
// build is the version-specific primitive that launches async index creation on
// targetDB.
func NewDeferredIndexController(m *Migrator, targetDB *db.MongoDB, cfg *config.Config, log *logger.Logger, build func(ctx context.Context)) *DeferredIndexController {
	return &DeferredIndexController{
		migrator:     m,
		targetDB:     targetDB,
		log:          log,
		lagThreshold: float64(cfg.IndexBuildLagThresholdSeconds),
		stableChecks: cfg.IndexBuildLagStableChecks,
		concurrency:  cfg.IndexConcurrency,
		build:        build,
	}
}

// Observe feeds one replication-lag sample (in seconds; a negative value means
// idle/unknown, which counts as caught up). Once stableChecks consecutive samples
// are at or below the lag threshold it fires the deferred build exactly once, in
// the background, so it never blocks the caller's reporting loop. Drive it from a
// single loop — it is not safe for concurrent callers.
func (d *DeferredIndexController) Observe(ctx context.Context, lagSeconds float64) {
	if lagSeconds <= d.lagThreshold {
		d.lowLagStreak++
		if d.lowLagStreak >= d.stableChecks {
			d.Trigger(ctx)
		}
	} else {
		d.lowLagStreak = 0
	}
}

// Trigger launches the deferred index build exactly once, in the background.
// Repeated calls are no-ops. Live paths reach it through Observe once lag settles.
func (d *DeferredIndexController) Trigger(ctx context.Context) {
	d.once.Do(func() {
		go func() {
			d.log.Info("Replication lag settled; starting deferred index build.")
			d.run(ctx)
			d.log.Info("Deferred index build complete.")
		}()
	})
}

// BuildNow runs the build synchronously (blocking) exactly once. Full-only paths
// use it because the one-shot job must not be reported complete until its indexes
// exist. It shares the once-guard with Trigger, so whichever fires first wins.
func (d *DeferredIndexController) BuildNow(ctx context.Context) {
	d.once.Do(func() {
		d.log.Info("Data load complete; building indexes now (after load).")
		d.run(ctx)
		d.log.Info("Index build complete.")
	})
}

// run performs the actual build: set concurrency, launch async creation, then wait
// for all builds to finish and log any failures. Shared by Trigger and BuildNow.
func (d *DeferredIndexController) run(ctx context.Context) {
	if d.targetDB != nil && d.concurrency > 0 {
		d.targetDB.SetIndexConcurrency(d.concurrency)
	}
	if d.build != nil {
		d.build(ctx)
	}
	if d.targetDB != nil {
		d.targetDB.WaitForIndexCreation()
	}
	if d.migrator != nil {
		d.migrator.logFailedIndexes(d.targetDB)
	}
}

// PollProgress mirrors the target's async index-build counters into the console as
// a "创建索引 M/N" row every 2 seconds, until ctx is cancelled. It only emits once a
// build has actually launched (total > 0). No-op when no console is attached.
func (d *DeferredIndexController) PollProgress(ctx context.Context) {
	if d.migrator == nil {
		return
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Emit one final snapshot so the row lands on its true value.
			if total, done, building := d.targetDB.IndexProgress(); total > 0 {
				d.migrator.reportIndexProgress(total, done, building)
			}
			return
		case <-ticker.C:
			if total, done, building := d.targetDB.IndexProgress(); total > 0 {
				d.migrator.reportIndexProgress(total, done, building)
			}
		}
	}
}
