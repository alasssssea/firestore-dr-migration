package migration

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"firestore-dr-migration/pkg/config"
	"firestore-dr-migration/pkg/logger"
)

func TestBackfillStatsManager(t *testing.T) {
	log := logger.New()
	sm := NewBackfillStatsManager(log, 100*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm.Start(ctx)

	// Record some reads
	sm.RecordRead("shop.orders", 10*time.Millisecond, 500)
	sm.RecordRead("shop.orders", 20*time.Millisecond, 1500) // 1.5 KB
	sm.RecordRead("shop.orders", 100*time.Millisecond, 10240) // 10 KB

	sm.AddTargetCount("shop.orders", 10)
	sm.RecordIngestQueueStall(100 * time.Millisecond)

	// Record worker received
	sm.RecordWorkerReceived(3)

	// Record some writes
	sm.RecordBulkWrite(50 * time.Millisecond)
	sm.RecordWriteResult("shop.orders", 2, 1, 1, 1, 0)
	sm.IncrementSequentialRetries("replace", 1)

	// Per-namespace snapshot should carry the same numbers keyed by db.coll so
	// the console can render one 全量 row per collection (not an aggregate).
	nsSnap := sm.NamespaceBackfillSnapshot()
	if len(nsSnap) != 1 || nsSnap[0].Namespace != "shop.orders" {
		t.Fatalf("expected one per-namespace stat for shop.orders, got %+v", nsSnap)
	}
	if nsSnap[0].Target != 10 || nsSnap[0].Succeeded != 2 || nsSnap[0].Read != 3 {
		t.Errorf("per-namespace counters wrong: %+v", nsSnap[0])
	}

	// Trigger manual stats report
	sm.ReportStats(false)

	// Test stats manager with a throttler (active)
	cfgActive := config.BackfillRampUpConfig{
		Enabled:          true,
		StartQps:         500.0,
		RampRatePerMin:   0,
		UpdateIntervalMs: 100,
	}
	wtActive := NewWriteThrottler(cfgActive, 100)
	sm.SetThrottler(wtActive)
	sm.ReportStats(false)

	// Test stats manager with a throttler (inactive/Inf QPS)
	cfgHigh := config.BackfillRampUpConfig{
		Enabled:          true,
		StartQps:         100000.0,
		RampRatePerMin:   0,
		UpdateIntervalMs: 100,
	}
	wtHigh := NewWriteThrottler(cfgHigh, 100)
	sm.SetThrottler(wtHigh)
	sm.ReportStats(false)

	// Wait a bit to ensure ticker fires (doesn't fail/panic)
	time.Sleep(150 * time.Millisecond)

	// Final report
	sm.ReportStats(true)
}

func TestBackfillStatsManagerDLQWarning(t *testing.T) {
	log := logger.New()
	var logBuf bytes.Buffer
	log.Logger.SetOutput(&logBuf)

	sm := NewBackfillStatsManager(log, 5*time.Minute, nil)

	// 1. DLQ is nil -> no warning
	sm.ReportStats(false)
	if strings.Contains(logBuf.String(), "DLQ WARNING") {
		t.Errorf("did not expect DLQ WARNING with nil DLQ, got log: %s", logBuf.String())
	}
	logBuf.Reset()

	// 2. DLQ count <= 100 -> no warning
	mock := &mockDLQ{count: 100}
	sm.SetDLQ(mock)
	sm.ReportStats(false)
	if strings.Contains(logBuf.String(), "DLQ WARNING") {
		t.Errorf("did not expect DLQ WARNING with count 100, got log: %s", logBuf.String())
	}
	logBuf.Reset()

	// 3. DLQ count > 100 -> warning
	atomic.StoreInt64(&mock.count, 101)
	sm.ReportStats(false)
	if !strings.Contains(logBuf.String(), "DLQ WARNING") {
		t.Errorf("expected DLQ WARNING with count 101, got log: %s", logBuf.String())
	}
}

