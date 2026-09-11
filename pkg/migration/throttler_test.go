package migration

import (
	"context"
	"testing"
	"time"

	"firestore-dr-migration/pkg/config"
	"golang.org/x/time/rate"
)

func TestNewWriteThrottler(t *testing.T) {
	// Disabled
	cfgDisabled := config.BackfillRampUpConfig{
		Enabled: false,
	}
	if throttler := NewWriteThrottler(cfgDisabled, 2000); throttler != nil {
		t.Errorf("Expected nil throttler when disabled")
	}

	// Enabled but with staggered workers enabled -> should return nil throttler
	cfgStaggered := config.BackfillRampUpConfig{
		Enabled:             true,
		UseStaggeredWorkers: true,
	}
	if throttler := NewWriteThrottler(cfgStaggered, 2000); throttler != nil {
		t.Errorf("Expected nil throttler when UseStaggeredWorkers is enabled")
	}

	// Enabled
	cfgEnabled := config.BackfillRampUpConfig{
		Enabled:          true,
		StartQps:         100.0,
		RampRatePerMin:   600.0,
		UpdateIntervalMs: 100,
	}
	throttler := NewWriteThrottler(cfgEnabled, 2000)
	if throttler == nil {
		t.Fatalf("Expected non-nil throttler")
	}

	if throttler.Limit() != rate.Limit(100.0) {
		t.Errorf("Expected starting limit to be 100.0, got %v", throttler.Limit())
	}

	// Enabled and high starting QPS (>= 100K) -> should start disabled
	cfgHighQps := config.BackfillRampUpConfig{
		Enabled:          true,
		StartQps:         100000.0,
		RampRatePerMin:   600.0,
		UpdateIntervalMs: 100,
	}
	throttlerHigh := NewWriteThrottler(cfgHighQps, 2000)
	if throttlerHigh == nil {
		t.Fatalf("Expected non-nil throttler")
	}
	if throttlerHigh.Limit() != rate.Inf {
		t.Errorf("Expected starting limit to be Inf, got %v", throttlerHigh.Limit())
	}
}

func TestWriteThrottlerRampUp(t *testing.T) {
	cfg := config.BackfillRampUpConfig{
		Enabled:          true,
		StartQps:         100.0,
		RampRatePerMin:   6000.0, // 100 QPS increase per second
		UpdateIntervalMs: 50,
	}

	throttler := NewWriteThrottler(cfg, 2000)
	if throttler == nil {
		t.Fatalf("Expected non-nil throttler")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	throttler.StartRampUp(ctx)

	// Wait 250ms -> should ramp up by ~25 QPS (100 -> ~125)
	time.Sleep(250 * time.Millisecond)

	limit := throttler.Limit()
	if limit <= rate.Limit(100.0) || limit >= rate.Limit(150.0) {
		t.Errorf("Expected limit to have ramped up to between 100 and 150, got %v", limit)
	}

	// Wait another 1.5 seconds -> limit should grow beyond 200 QPS since there is no ceiling cap
	time.Sleep(1500 * time.Millisecond)
	limit = throttler.Limit()
	if limit <= rate.Limit(200.0) {
		t.Errorf("Expected limit to continue growing beyond start value, got %v", limit)
	}
}

func TestWriteThrottlerRampUpDisable(t *testing.T) {
	cfg := config.BackfillRampUpConfig{
		Enabled:          true,
		StartQps:         99900.0,
		RampRatePerMin:   12000.0, // 200 QPS increase per second
		UpdateIntervalMs: 10,
	}

	throttler := NewWriteThrottler(cfg, 2000)
	if throttler == nil {
		t.Fatalf("Expected non-nil throttler")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	throttler.StartRampUp(ctx)

	// Wait 1 second (ramp rate is 200/sec, so it should hit 100K QPS and disable)
	time.Sleep(1 * time.Second)

	limit := throttler.Limit()
	if limit != rate.Inf {
		t.Errorf("Expected limit to be Inf after hitting 100K QPS, got %v", limit)
	}
}

func TestWriteThrottlerWait(t *testing.T) {
	// Setup a throttler with a very low limit (2 tokens per second) to test blocking behavior
	cfg := config.BackfillRampUpConfig{
		Enabled:          true,
		StartQps:         2.0,
		RampRatePerMin:   0.0,
		UpdateIntervalMs: 100,
	}

	throttler := NewWriteThrottler(cfg, 2)
	if throttler == nil {
		t.Fatalf("Expected non-nil throttler")
	}

	ctx := context.Background()

	// First wait of 1 token should succeed immediately
	start := time.Now()
	err := throttler.Wait(ctx, 1)
	if err != nil {
		t.Fatalf("Unexpected wait error: %v", err)
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Errorf("Expected immediate return, but took %v", time.Since(start))
	}

	// Requesting 2 more tokens should force a wait (at least ~500ms since limit is 2/sec)
	start = time.Now()
	err = throttler.Wait(ctx, 2)
	if err != nil {
		t.Fatalf("Unexpected wait error: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 400*time.Millisecond {
		t.Errorf("Expected wait to block for at least 400ms, but only took %v", elapsed)
	}
}

func BenchmarkThrottlerDisabled(b *testing.B) {
	var throttler *WriteThrottler // nil
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if throttler != nil {
			_ = throttler.Wait(ctx, 1)
		}
	}
}

func BenchmarkThrottlerEnabledNoLimit(b *testing.B) {
	cfg := config.BackfillRampUpConfig{
		Enabled:          true,
		StartQps:         100000000.0, // extremely high QPS to prevent blocking
		RampRatePerMin:   0.0,
		UpdateIntervalMs: 1000,
	}
	throttler := NewWriteThrottler(cfg, 100000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = throttler.Wait(ctx, 1)
	}
}
