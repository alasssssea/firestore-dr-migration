package migration

import (
	"math"
	"testing"
	"time"
)

func TestLagTrackerAccumulationAndFlush(t *testing.T) {
	tracker := NewLagTracker()

	now := time.Now()

	// op1: Succeeded after retry (SuccessAfterRetry: true)
	// EventTime = now - 500ms, ReadTime = now - 400ms, WorkerReceiveTime = now - 200ms, SuccessTime = now - 100ms
	// readToEventTimeLag: 100ms
	// workerReceivedToReadTimeLag: 200ms
	// successWithRetryTimeToEventTime: 400ms
	// successWithRetryLagToWorkerReceivedTime: 100ms
	op1 := WriteOperation{
		EventTime:         now.Add(-500 * time.Millisecond),
		ReadTime:          now.Add(-400 * time.Millisecond),
		WorkerReceiveTime: now.Add(-200 * time.Millisecond),
		SuccessTime:       now.Add(-100 * time.Millisecond),
		SuccessAfterRetry: true,
	}

	// op2: Ignored (no SuccessTime)
	op2 := WriteOperation{
		EventTime:         now.Add(-500 * time.Millisecond),
		ReadTime:          now.Add(-400 * time.Millisecond),
		WorkerReceiveTime: now.Add(-200 * time.Millisecond),
	}

	// op3: Succeeded on first attempt (SuccessAfterRetry: false)
	// EventTime = now - 1000ms, ReadTime = now - 900ms, WorkerReceiveTime = now - 700ms, SuccessTime = now - 600ms
	// readToEventTimeLag: 100ms
	// workerReceivedToReadTimeLag: 200ms
	// successTimeToWorkerReceivedLag: 100ms
	// successTimeToEventTimeLag: 400ms
	op3 := WriteOperation{
		EventTime:         now.Add(-1000 * time.Millisecond),
		ReadTime:          now.Add(-900 * time.Millisecond),
		WorkerReceiveTime: now.Add(-700 * time.Millisecond),
		SuccessTime:       now.Add(-600 * time.Millisecond),
		SuccessAfterRetry: false,
	}

	tracker.RecordLags([]WriteOperation{op1, op2, op3})

	// Flush the metrics
	res := tracker.Flush()

	// readToEventTimeLag: (100ms + 100ms) / 2 = 100ms
	if res.EventToReadLag != 100*time.Millisecond {
		t.Errorf("expected EventToReadLag 100ms, got %v", res.EventToReadLag)
	}
	// workerReceivedToReadTimeLag: (200ms + 200ms) / 2 = 200ms
	if res.ReadToWorkerReceiveLag != 200*time.Millisecond {
		t.Errorf("expected ReadToWorkerReceiveLag 200ms, got %v", res.ReadToWorkerReceiveLag)
	}
	
	// successTimeToWorkerReceivedLag (from op3 only): 100ms
	if res.ReceiveToApplyLag != 100*time.Millisecond {
		t.Errorf("expected ReceiveToApplyLag 100ms, got %v", res.ReceiveToApplyLag)
	}
	// successTimeToEventTimeLag (from op3 only): 400ms
	if res.EndToEndLag != 400*time.Millisecond {
		t.Errorf("expected EndToEndLag 400ms, got %v", res.EndToEndLag)
	}
	
	// successWithRetryTimeToEventTime (from op1 only): 400ms
	if res.EndToEndWithRetryLag != 400*time.Millisecond {
		t.Errorf("expected EndToEndWithRetryLag 400ms, got %v", res.EndToEndWithRetryLag)
	}
	// successWithRetryLagToWorkerReceivedTime (from op1 only): 100ms
	if res.ReceiveToApplyWithRetryLag != 100*time.Millisecond {
		t.Errorf("expected ReceiveToApplyWithRetryLag 100ms, got %v", res.ReceiveToApplyWithRetryLag)
	}

	// Verify counters reset after Flush
	res2 := tracker.Flush()
	if res2.EventToReadLag != 0 || res2.ReadToWorkerReceiveLag != 0 || res2.ReceiveToApplyLag != 0 || res2.EndToEndLag != 0 || res2.EndToEndWithRetryLag != 0 || res2.ReceiveToApplyWithRetryLag != 0 {
		t.Errorf("expected flushed metrics to reset to 0, got %+v", res2)
	}
}

func TestLagTrackerHighThroughputOverflow(t *testing.T) {
	tracker := NewLagTracker()

	// Simulate 6,000,000 operations, each with 2 hours (7,200s) of lag.
	// Total accumulated nanoseconds = 6,000,000 * 7,200 * 10^9 = 4.32 * 10^19 ns.
	// In signed int64, MaxInt64 is ~9.22 * 10^18 ns.
	// With the previous int64 accumulator, 4.32 * 10^19 would wrap around multiple times,
	// resulting in a negative average duration.
	eventTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	readTime := eventTime.Add(2 * time.Hour)

	const numOps = 6000000
	for i := 0; i < numOps; i++ {
		tracker.RecordEventToRead(eventTime, readTime)
	}

	res := tracker.Flush()
	expected := 2 * time.Hour
	if res.EventToReadLag != expected {
		t.Errorf("expected EventToReadLag %v, got %v", expected, res.EventToReadLag)
	}

	// Verify reset
	resAfterReset := tracker.Flush()
	if resAfterReset.EventToReadLag != 0 {
		t.Errorf("expected EventToReadLag after flush to be 0, got %v", resAfterReset.EventToReadLag)
	}
}

func TestLagTrackerBoundarySaturation(t *testing.T) {
	mPositive := metricAccumulator{
		totalNs: float64(math.MaxInt64) * 2,
		count:   1,
	}
	if mPositive.Average() != time.Duration(math.MaxInt64) {
		t.Errorf("expected MaxInt64 saturation, got %v", mPositive.Average())
	}

	mNegative := metricAccumulator{
		totalNs: float64(math.MinInt64) * 2,
		count:   1,
	}
	if mNegative.Average() != time.Duration(math.MinInt64) {
		t.Errorf("expected MinInt64 saturation, got %v", mNegative.Average())
	}
}
