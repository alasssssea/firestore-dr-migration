package migration

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"firestore-dr-migration/pkg/config"
	"golang.org/x/time/rate"
)

// WriteThrottler wraps golang.org/x/time/rate.Limiter to dynamically
// ramp up or adjust database write QPS based on feedback or time.
type WriteThrottler struct {
	mu         sync.Mutex
	limiter    *rate.Limiter
	config     config.BackfillRampUpConfig
	start      time.Time
	disabled   atomic.Bool
	currentQps float64

	// Lock-free statistics window for adaptive feedback
	totalLatencyMs atomic.Uint64
	writeCount     atomic.Uint64
	errorCount     atomic.Uint64
}

// NewWriteThrottler constructs a WriteThrottler.
// If the throttler is disabled (Enabled is false), it returns nil.
func NewWriteThrottler(cfg config.BackfillRampUpConfig, burst int) *WriteThrottler {
	if !cfg.Enabled || cfg.UseStaggeredWorkers {
		return nil
	}

	startQps := cfg.StartQps
	if startQps < 0 {
		startQps = 0
	}

	// Ensure burst is at least 1
	if burst < 1 {
		burst = 1
	}

	wt := &WriteThrottler{
		limiter:    rate.NewLimiter(rate.Limit(startQps), burst),
		config:     cfg,
		start:      time.Now(),
		currentQps: startQps,
	}

	if startQps >= 100000.0 {
		wt.disabled.Store(true)
	}

	return wt
}

// IsDisabled returns whether the throttler has been disabled.
func (wt *WriteThrottler) IsDisabled() bool {
	if wt == nil {
		return true
	}
	return wt.disabled.Load()
}

// Limit returns the current QPS limit.
func (wt *WriteThrottler) Limit() rate.Limit {
	if wt == nil || wt.IsDisabled() {
		return rate.Inf
	}
	wt.mu.Lock()
	defer wt.mu.Unlock()
	return wt.limiter.Limit()
}

// ReportResult updates the throttler statistics for adaptive feedback.
// It is thread-safe and lock-free for the caller.
func (wt *WriteThrottler) ReportResult(duration time.Duration, isSystemError bool) {
	if wt == nil || wt.IsDisabled() || wt.config.Strategy != "adaptive" {
		return
	}

	wt.totalLatencyMs.Add(uint64(duration.Milliseconds()))
	wt.writeCount.Add(1)

	if isSystemError {
		wt.errorCount.Add(1)
	}
}

// StartRampUp runs a background goroutine that updates the rate limit at fixed intervals.
func (wt *WriteThrottler) StartRampUp(ctx context.Context) {
	if wt == nil || wt.IsDisabled() {
		return
	}

	updateInterval := time.Duration(wt.config.UpdateIntervalMs) * time.Millisecond
	if updateInterval <= 0 {
		updateInterval = time.Second
	}

	ticker := time.NewTicker(updateInterval)
	go func() {
		defer ticker.Stop()
		rampRatePerSec := wt.config.RampRatePerMin / 60.0
		intervalSecs := updateInterval.Seconds()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if wt.IsDisabled() {
					return
				}

				wt.mu.Lock()
				if wt.config.Strategy == "adaptive" {
					// Read and reset statistics window
					totalMs := wt.totalLatencyMs.Swap(0)
					count := wt.writeCount.Swap(0)
					errs := wt.errorCount.Swap(0)

					if count > 0 {
						avgLatency := time.Duration(totalMs/count) * time.Millisecond

						// Multiplicative Decrease if we see connection errors or latency > 50s
						if errs > 0 || avgLatency > 50*time.Second {
							wt.currentQps = wt.currentQps * 0.90
							if wt.currentQps < wt.config.StartQps {
								wt.currentQps = wt.config.StartQps
							}
						} else if avgLatency < 10*time.Second {
							// Additive Increase when healthy
							multiplier := 1.0
							if avgLatency < 1*time.Second {
								multiplier = 2.0 // Double the ramp rate if database latencies are excellent (<1s)
							}
							wt.currentQps = wt.currentQps + (multiplier * rampRatePerSec * intervalSecs)
						}
					} else {
						// If idle (no writes), continue to probe upwards slowly
						wt.currentQps = wt.currentQps + (rampRatePerSec * intervalSecs)
					}
				} else {
					// Default static time-based ramp up
					elapsedSecs := time.Since(wt.start).Seconds()
					wt.currentQps = wt.config.StartQps + rampRatePerSec*elapsedSecs
				}

				if wt.currentQps >= 100000.0 {
					wt.disabled.Store(true)
					wt.mu.Unlock()
					return
				}

				wt.limiter.SetLimit(rate.Limit(wt.currentQps))
				wt.mu.Unlock()
			}
		}
	}()
}

// Wait blocks until the required number of tokens are available.
// If the context is canceled or expired, it returns the context error.
func (wt *WriteThrottler) Wait(ctx context.Context, count int) error {
	if wt == nil || wt.IsDisabled() {
		return nil
	}
	if count <= 0 {
		return nil
	}
	return wt.limiter.WaitN(ctx, count)
}
