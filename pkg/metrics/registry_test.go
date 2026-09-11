package metrics

import (
	"context"
	"testing"
	"time"

	"firestore-dr-migration/pkg/progress"
)

func TestRegistryCollectionsSorted(t *testing.T) {
	r := NewRegistry()
	r.SetCollection(CollectionMetric{Job: "j1", Database: "b", Collection: "z"})
	r.SetCollection(CollectionMetric{Job: "j1", Database: "a", Collection: "y"})
	r.SetCollection(CollectionMetric{Job: "j1", Database: "a", Collection: "x"})
	got := r.Collections()
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
	if got[0].Database != "a" || got[0].Collection != "x" {
		t.Errorf("unexpected sort order: %+v", got)
	}
}

func TestRegistryJobLifecycle(t *testing.T) {
	r := NewRegistry()
	r.UpsertJob("job-1", "live", StateCreated)
	r.UpsertJob("job-1", "live", StateInitialLoad)
	jobs := r.Jobs()
	if len(jobs) != 1 || jobs[0].State != StateInitialLoad {
		t.Fatalf("expected single job in initial-load, got %+v", jobs)
	}
	r.FailJob("job-1", context.DeadlineExceeded)
	if r.Jobs()[0].State != StateFailed || r.Jobs()[0].Error == "" {
		t.Errorf("expected failed with error, got %+v", r.Jobs()[0])
	}
}

func TestRegistryReadiness(t *testing.T) {
	r := NewRegistry()
	if r.Ready() {
		t.Error("should start not-ready")
	}
	r.SetReady(true)
	if !r.Ready() {
		t.Error("should be ready")
	}
}

func TestControlPauseResume(t *testing.T) {
	c := NewControl()
	// Not paused: Wait returns immediately.
	if err := c.Wait(context.Background()); err != nil {
		t.Fatalf("unexpected: %v", err)
	}

	c.Pause()
	done := make(chan error, 1)
	go func() { done <- c.Wait(context.Background()) }()

	select {
	case <-done:
		t.Fatal("Wait should block while paused")
	case <-time.After(50 * time.Millisecond):
	}

	c.Resume()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Wait after resume: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Wait did not unblock after resume")
	}
}

func TestControlStopUnblocksAndReports(t *testing.T) {
	c := NewControl()
	c.Pause()
	done := make(chan error, 1)
	go func() { done <- c.Wait(context.Background()) }()
	c.Stop()
	select {
	case err := <-done:
		if err != ErrStopped {
			t.Fatalf("expected ErrStopped, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not unblock paused Wait")
	}
	if !c.Stopped() {
		t.Error("Stopped() should be true")
	}
}

func TestControlWaitContextCancel(t *testing.T) {
	c := NewControl()
	c.Pause()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Wait(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Wait did not observe context cancel")
	}
}

func TestStatusIncludesSnapshot(t *testing.T) {
	r := NewRegistry()
	r.SetReady(true)
	r.SetCollection(CollectionMetric{
		Job: "j", Database: "d", Collection: "c", Phase: "initial",
		Snapshot: progress.Snapshot{TotalDocs: 100, DoneDocs: 50, PercentDocs: 50}, LagSeconds: -1,
	})
	st := r.Status()
	if !st.Ready || len(st.Collections) != 1 || st.Collections[0].Snapshot.DoneDocs != 50 {
		t.Errorf("unexpected status: %+v", st)
	}
}
