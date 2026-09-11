// Package progress provides a thread-safe progress tracker with byte-based ETA
// estimation for the initial (full) migration. It implements the honest-ETA
// approach from DESIGN §4: progress is measured by bytes (documents vary widely
// in size), throughput is smoothed with an EWMA, and the ETA is flagged
// unreliable during cold start so the caller can present it truthfully.
package progress

import (
	"fmt"
	"sync"
	"time"
)

// Snapshot is an immutable view of progress at one instant.
type Snapshot struct {
	TotalDocs    int64         `json:"totalDocs"`
	DoneDocs     int64         `json:"doneDocs"`
	TotalBytes   int64         `json:"totalBytes"`
	DoneBytes    int64         `json:"doneBytes"`
	PercentDocs  float64       `json:"percentDocs"`  // 0..100, 0 if TotalDocs unknown
	PercentBytes float64       `json:"percentBytes"` // 0..100, 0 if TotalBytes unknown
	DocsPerSec   float64       `json:"docsPerSec"`   // overall average
	BytesPerSec  float64       `json:"bytesPerSec"`  // EWMA-smoothed
	Elapsed      time.Duration `json:"elapsed"`
	ETA          time.Duration `json:"eta"`         // time.Duration(-1) when unknown
	ETAReliable  bool          `json:"etaReliable"` // false during cold start / insufficient samples
}

// Tracker accumulates migration progress and computes ETA.
type Tracker struct {
	mu sync.Mutex

	totalDocs  int64
	totalBytes int64
	doneDocs   int64
	doneBytes  int64

	now   func() time.Time
	start time.Time

	alpha           float64 // EWMA smoothing factor (0..1)
	ewmaBytesPerSec float64
	samples         int
	lastSampleTime  time.Time
	lastSampleBytes int64
}

// NewTracker creates a Tracker for the given known totals. Either total may be
// zero if unknown; ETA and percentages degrade gracefully in that case.
func NewTracker(totalDocs, totalBytes int64) *Tracker {
	return NewTrackerWithClock(totalDocs, totalBytes, time.Now)
}

// NewTrackerWithClock is like NewTracker but with an injectable clock for tests.
func NewTrackerWithClock(totalDocs, totalBytes int64, now func() time.Time) *Tracker {
	start := now()
	return &Tracker{
		totalDocs:      totalDocs,
		totalBytes:     totalBytes,
		now:            now,
		start:          start,
		alpha:          0.3,
		lastSampleTime: start,
	}
}

// AddProgress records that docs documents totalling bytes have been migrated.
func (t *Tracker) AddProgress(docs, bytes int64) {
	t.mu.Lock()
	t.doneDocs += docs
	t.doneBytes += bytes
	t.mu.Unlock()
}

// Snapshot updates the smoothed throughput and returns the current state. It is
// intended to be called on a fixed cadence (e.g. by a reporter goroutine); each
// call advances the EWMA using the bytes accumulated since the previous call.
func (t *Tracker) Snapshot() Snapshot {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.now()
	elapsed := now.Sub(t.start)

	// Update EWMA of bytes/sec from the delta since the last sample.
	dt := now.Sub(t.lastSampleTime).Seconds()
	if dt > 0 {
		inst := float64(t.doneBytes-t.lastSampleBytes) / dt
		if t.samples == 0 {
			t.ewmaBytesPerSec = inst
		} else {
			t.ewmaBytesPerSec = t.alpha*inst + (1-t.alpha)*t.ewmaBytesPerSec
		}
		t.samples++
		t.lastSampleTime = now
		t.lastSampleBytes = t.doneBytes
	}

	s := Snapshot{
		TotalDocs:   t.totalDocs,
		DoneDocs:    t.doneDocs,
		TotalBytes:  t.totalBytes,
		DoneBytes:   t.doneBytes,
		BytesPerSec: t.ewmaBytesPerSec,
		Elapsed:     elapsed,
		ETA:         time.Duration(-1),
	}

	elapsedSec := elapsed.Seconds()
	if elapsedSec > 0 {
		s.DocsPerSec = float64(t.doneDocs) / elapsedSec
	}
	if t.totalDocs > 0 {
		s.PercentDocs = clampPct(float64(t.doneDocs) / float64(t.totalDocs) * 100)
	}
	if t.totalBytes > 0 {
		s.PercentBytes = clampPct(float64(t.doneBytes) / float64(t.totalBytes) * 100)
	}

	// ETA: prefer byte-based (EWMA); fall back to doc-based average rate.
	s.ETA, s.ETAReliable = t.estimateETA(s, elapsed)
	return s
}

// estimateETA computes remaining time. Caller holds the lock.
func (t *Tracker) estimateETA(s Snapshot, elapsed time.Duration) (time.Duration, bool) {
	// ETA is only trustworthy after a couple of samples and a few seconds of
	// warm-up; before that the throughput estimate swings wildly.
	reliable := t.samples >= 2 && elapsed >= 3*time.Second

	if t.totalBytes > 0 && t.ewmaBytesPerSec > 0 {
		remaining := float64(t.totalBytes - t.doneBytes)
		if remaining <= 0 {
			return 0, reliable
		}
		return time.Duration(remaining/t.ewmaBytesPerSec) * time.Second, reliable
	}

	// Doc-based fallback using overall average rate.
	if t.totalDocs > 0 && s.DocsPerSec > 0 {
		remaining := float64(t.totalDocs - t.doneDocs)
		if remaining <= 0 {
			return 0, reliable
		}
		return time.Duration(remaining/s.DocsPerSec) * time.Second, reliable
	}

	return time.Duration(-1), false
}

func clampPct(p float64) float64 {
	if p < 0 {
		return 0
	}
	if p > 100 {
		return 100
	}
	return p
}

// Format renders a compact one-line human-readable summary.
func (s Snapshot) Format() string {
	pct := s.PercentBytes
	if s.TotalBytes == 0 {
		pct = s.PercentDocs
	}
	eta := "unknown"
	if s.ETA >= 0 {
		eta = s.ETA.Round(time.Second).String()
		if !s.ETAReliable {
			eta += " (warming up)"
		}
	}
	return fmt.Sprintf("%.1f%% | %d/%d docs | %s/%s | %s/s | ETA %s",
		pct, s.DoneDocs, s.TotalDocs,
		humanBytes(s.DoneBytes), humanBytes(s.TotalBytes),
		humanBytes(int64(s.BytesPerSec)), eta)
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(b)/float64(div), "KMGTPE"[exp])
}
