package migration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"firestore-dr-migration/pkg/logger"
)

// newTestController builds a controller with no DB/migrator (run() is nil-guarded)
// so we can exercise the pure trigger/streak/once logic. build signals `fired` on
// every invocation; the returned func waits (with timeout) for the next signal.
func newTestController(t *testing.T, threshold float64, stable int) (*DeferredIndexController, *int32, func() bool) {
	t.Helper()
	var count int32
	fired := make(chan struct{}, 16)
	d := &DeferredIndexController{
		log:          logger.New(),
		lagThreshold: threshold,
		stableChecks: stable,
		build: func(context.Context) {
			atomic.AddInt32(&count, 1)
			fired <- struct{}{}
		},
	}
	waitFired := func() bool {
		select {
		case <-fired:
			return true
		case <-time.After(2 * time.Second):
			return false
		}
	}
	return d, &count, waitFired
}

// After stableChecks consecutive caught-up samples the build fires exactly once,
// and stays fired no matter how many more samples arrive.
func TestDeferredIndex_TriggersOnceAfterStableStreak(t *testing.T) {
	d, count, waitFired := newTestController(t, 5, 3)
	ctx := context.Background()

	// Two caught-up samples: not enough yet.
	d.Observe(ctx, 0)
	d.Observe(ctx, 3)
	if got := atomic.LoadInt32(count); got != 0 {
		t.Fatalf("build fired too early after 2 samples: count=%d", got)
	}

	// Third caught-up sample crosses the streak -> fires.
	d.Observe(ctx, 5) // lag == threshold still counts as caught up
	if !waitFired() {
		t.Fatal("build did not fire after 3 consecutive caught-up samples")
	}

	// Many more samples must not fire it again (sync.Once).
	for i := 0; i < 10; i++ {
		d.Observe(ctx, 0)
	}
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt32(count); got != 1 {
		t.Fatalf("build fired more than once: count=%d", got)
	}
}

// A sample above the threshold resets the streak, so the build waits for a fresh
// run of stableChecks caught-up samples.
func TestDeferredIndex_LagResetsStreak(t *testing.T) {
	d, count, waitFired := newTestController(t, 5, 3)
	ctx := context.Background()

	d.Observe(ctx, 0)
	d.Observe(ctx, 0)
	d.Observe(ctx, 20) // over threshold -> reset
	d.Observe(ctx, 0)
	d.Observe(ctx, 0)
	if got := atomic.LoadInt32(count); got != 0 {
		t.Fatalf("build fired despite a mid-streak lag spike: count=%d", got)
	}

	d.Observe(ctx, 0) // completes the fresh streak
	if !waitFired() {
		t.Fatal("build did not fire after the streak recovered")
	}
}

// A negative lag (idle/unknown source) counts as caught up.
func TestDeferredIndex_IdleCountsAsCaughtUp(t *testing.T) {
	d, _, waitFired := newTestController(t, 5, 3)
	ctx := context.Background()
	d.Observe(ctx, -1)
	d.Observe(ctx, -1)
	d.Observe(ctx, -1)
	if !waitFired() {
		t.Fatal("idle (lag=-1) samples should count as caught up and trigger the build")
	}
}

// BuildNow runs synchronously and shares the once-guard with Trigger/Observe, so a
// later lag-settled trigger is a no-op (full-only path already built the indexes).
func TestDeferredIndex_BuildNowIsSyncAndOnce(t *testing.T) {
	d, count, _ := newTestController(t, 5, 3)
	ctx := context.Background()

	d.BuildNow(ctx) // synchronous
	if got := atomic.LoadInt32(count); got != 1 {
		t.Fatalf("BuildNow should build synchronously exactly once: count=%d", got)
	}

	d.Trigger(ctx)
	for i := 0; i < 5; i++ {
		d.Observe(ctx, 0)
	}
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt32(count); got != 1 {
		t.Fatalf("build fired again after BuildNow: count=%d", got)
	}
}
