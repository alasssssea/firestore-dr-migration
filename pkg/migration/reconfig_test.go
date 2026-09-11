package migration

import (
	"context"
	"sync"
	"testing"
	"time"

	"firestore-dr-migration/pkg/config"
	"firestore-dr-migration/pkg/logger"
)

// TestResizableSemGrowShrink proves the limit is respected, growing admits more
// holders, and shrinking never preempts current holders (only future Acquire).
func TestResizableSemGrowShrink(t *testing.T) {
	s := newResizableSem(2)
	ctx := context.Background()

	// Fill both slots.
	if err := s.Acquire(ctx); err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	if err := s.Acquire(ctx); err != nil {
		t.Fatalf("acquire 2: %v", err)
	}

	// Third acquire must block until we grow or release.
	got := make(chan struct{})
	go func() {
		_ = s.Acquire(ctx)
		close(got)
	}()
	select {
	case <-got:
		t.Fatal("third Acquire returned while at limit")
	case <-time.After(50 * time.Millisecond):
	}

	// Growing to 3 must admit the waiter.
	s.SetLimit(3)
	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("grow did not admit the waiting Acquire")
	}

	// Now held==3, limit==3. Shrink to 1: current holders keep running (no panic,
	// no preemption); a new Acquire must block.
	s.SetLimit(1)
	blocked := make(chan struct{})
	go func() {
		_ = s.Acquire(ctx)
		close(blocked)
	}()
	select {
	case <-blocked:
		t.Fatal("Acquire succeeded despite being over the shrunk limit")
	case <-time.After(50 * time.Millisecond):
	}

	// Release enough to fall under the new limit (3 held → need to drop to 0 before
	// limit 1 admits the waiter).
	s.Release()
	s.Release()
	s.Release()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("Acquire never admitted after releasing under the shrunk limit")
	}
}

// TestResizableSemAcquireCtxCancel proves a blocked Acquire returns on ctx cancel
// without leaking a slot.
func TestResizableSemAcquireCtxCancel(t *testing.T) {
	s := newResizableSem(1)
	if err := s.Acquire(context.Background()); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Acquire(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected ctx error, got nil (slot leaked)")
		}
	case <-time.After(time.Second):
		t.Fatal("Acquire did not return after ctx cancel")
	}
}

// TestReconfigGlobalAndPerCollection proves Reconfig updates the global defaults
// and upserts a per-collection override that effectivePartitioning then reads.
func TestReconfigGlobalAndPerCollection(t *testing.T) {
	cfg := &config.Config{
		ParallelReadsEnabled:    true,
		MaxReadPartitions:       8,
		WorkersPerPartition:     3,
		MinDocsPerPartition:     10000,
		MinDocsForParallelReads: 50000,
		ConcurrentCollections:   4,
		DatabasePairs: []config.DatabasePair{
			{Source: config.SourceConfig{Database: "shop"}, Target: config.TargetConfig{Database: "shop"}},
		},
	}
	m := NewMigrator(cfg, logger.New())

	// Global change.
	if err := m.Reconfig(ReconfigRequest{MaxReadPartitions: 16, WorkersPerPartition: 5}); err != nil {
		t.Fatalf("global reconfig: %v", err)
	}
	_, maxParts, wpp, _, _ := m.effectivePartitioning("shop", "other")
	if maxParts != 16 || wpp != 5 {
		t.Fatalf("global not applied: maxParts=%d wpp=%d", maxParts, wpp)
	}

	// Per-collection override for a byte-heavy straggler with a low doc count.
	if err := m.Reconfig(ReconfigRequest{Database: "shop", Collection: "orders", MaxReadPartitions: 32, WorkersPerPartition: 4}); err != nil {
		t.Fatalf("per-coll reconfig: %v", err)
	}
	enabled, mp, w, _, trigger := m.effectivePartitioning("shop", "orders")
	if !enabled || mp != 32 || w != 4 {
		t.Fatalf("per-coll not applied: enabled=%v mp=%d w=%d", enabled, mp, w)
	}
	if trigger != 1 {
		t.Fatalf("expected parallel gate opened (trigger=1) for tuned straggler, got %d", trigger)
	}

	// Unknown database is an error.
	if err := m.Reconfig(ReconfigRequest{Database: "nope", Collection: "x", MaxReadPartitions: 2}); err == nil {
		t.Fatal("expected error for unknown database")
	}
}

// TestReconfigConcurrentWithReads runs Reconfig against effectivePartitioning
// concurrently under -race to catch missing locking.
func TestReconfigConcurrentWithReads(t *testing.T) {
	cfg := &config.Config{
		MaxReadPartitions: 8, WorkersPerPartition: 3, MinDocsPerPartition: 10000,
		DatabasePairs: []config.DatabasePair{{Source: config.SourceConfig{Database: "d"}, Target: config.TargetConfig{Database: "d"}}},
	}
	m := NewMigrator(cfg, logger.New())
	var wg sync.WaitGroup
	// A cancelled context's Done() is a CLOSED channel, so every goroutine sees the
	// stop signal (unlike a time.After channel, whose single value only one receiver
	// would get).
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					m.effectivePartitioning("d", "c")
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := 1
		for {
			select {
			case <-ctx.Done():
				return
			default:
				n++
				_ = m.Reconfig(ReconfigRequest{MaxReadPartitions: n%32 + 1})
				_ = m.Reconfig(ReconfigRequest{Database: "d", Collection: "c", WorkersPerPartition: n%8 + 1})
			}
		}
	}()
	wg.Wait()
}
