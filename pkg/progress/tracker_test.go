package progress

import (
	"testing"
	"time"
)

// fakeClock returns a controllable time source for deterministic tests.
type fakeClock struct{ t time.Time }

func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }
func (c *fakeClock) now() time.Time          { return c.t }

func newTestTracker(totalDocs, totalBytes int64) (*Tracker, *fakeClock) {
	clk := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	return NewTrackerWithClock(totalDocs, totalBytes, clk.now), clk
}

func TestTracker_PercentAndCounts(t *testing.T) {
	tr, clk := newTestTracker(100, 1000)
	tr.AddProgress(25, 250)
	clk.advance(1 * time.Second)
	s := tr.Snapshot()

	if s.DoneDocs != 25 || s.DoneBytes != 250 {
		t.Fatalf("counts wrong: %d docs, %d bytes", s.DoneDocs, s.DoneBytes)
	}
	if s.PercentDocs != 25 {
		t.Errorf("PercentDocs = %v, want 25", s.PercentDocs)
	}
	if s.PercentBytes != 25 {
		t.Errorf("PercentBytes = %v, want 25", s.PercentBytes)
	}
}

func TestTracker_ETAByBytes(t *testing.T) {
	tr, clk := newTestTracker(0, 1000)

	// Warm up with a steady 100 B/s over several samples.
	for i := 0; i < 5; i++ {
		tr.AddProgress(0, 100)
		clk.advance(1 * time.Second)
		tr.Snapshot()
	}
	// 500 of 1000 bytes done at ~100 B/s -> ~5s remaining.
	s := tr.Snapshot()
	if s.DoneBytes != 500 {
		t.Fatalf("DoneBytes = %d, want 500", s.DoneBytes)
	}
	if !s.ETAReliable {
		t.Errorf("expected ETA to be reliable after warm-up, got unreliable")
	}
	if s.ETA <= 0 {
		t.Fatalf("expected positive ETA, got %v", s.ETA)
	}
	// EWMA of a constant 100 B/s series is 100; remaining 500 B -> 5s.
	if s.ETA < 3*time.Second || s.ETA > 8*time.Second {
		t.Errorf("ETA = %v, want roughly 5s", s.ETA)
	}
}

func TestTracker_ETAUnreliableAtColdStart(t *testing.T) {
	tr, clk := newTestTracker(0, 1000)
	tr.AddProgress(0, 100)
	clk.advance(500 * time.Millisecond) // < 3s warm-up
	s := tr.Snapshot()
	if s.ETAReliable {
		t.Errorf("ETA should be unreliable during cold start")
	}
}

func TestTracker_ETAUnknownWithoutTotals(t *testing.T) {
	tr, clk := newTestTracker(0, 0)
	tr.AddProgress(10, 100)
	clk.advance(1 * time.Second)
	s := tr.Snapshot()
	if s.ETA != time.Duration(-1) {
		t.Errorf("ETA = %v, want -1 (unknown) when totals unknown", s.ETA)
	}
	if s.ETAReliable {
		t.Errorf("ETA cannot be reliable without totals")
	}
}

func TestTracker_ETAZeroWhenComplete(t *testing.T) {
	tr, clk := newTestTracker(0, 1000)
	for i := 0; i < 3; i++ {
		tr.AddProgress(0, 400)
		clk.advance(1 * time.Second)
		tr.Snapshot()
	}
	// 1200 >= 1000 bytes -> remaining <= 0 -> ETA 0.
	s := tr.Snapshot()
	if s.ETA != 0 {
		t.Errorf("ETA = %v, want 0 when over-complete", s.ETA)
	}
}

func TestTracker_PercentClampedTo100(t *testing.T) {
	tr, clk := newTestTracker(10, 100)
	tr.AddProgress(20, 200) // over-report
	clk.advance(1 * time.Second)
	s := tr.Snapshot()
	if s.PercentDocs != 100 || s.PercentBytes != 100 {
		t.Errorf("percents not clamped: docs=%v bytes=%v", s.PercentDocs, s.PercentBytes)
	}
}

func TestTracker_DocBasedFallback(t *testing.T) {
	// Totals known for docs but not bytes -> ETA uses doc average rate.
	tr, clk := newTestTracker(100, 0)
	for i := 0; i < 4; i++ {
		tr.AddProgress(10, 0)
		clk.advance(1 * time.Second)
		tr.Snapshot()
	}
	s := tr.Snapshot()
	if s.ETA <= 0 {
		t.Fatalf("expected positive doc-based ETA, got %v", s.ETA)
	}
}

func TestSnapshot_Format(t *testing.T) {
	tr, clk := newTestTracker(100, 2048)
	tr.AddProgress(50, 1024)
	clk.advance(1 * time.Second)
	got := tr.Snapshot().Format()
	if got == "" {
		t.Fatal("Format returned empty string")
	}
	// Should contain a percentage and the docs fraction.
	if !contains(got, "50/100 docs") {
		t.Errorf("Format missing docs fraction: %q", got)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
