package migration

import (
	"math"
	"time"
)

// LagFlushResult represents the averaged lag metrics flushed from LagTracker
type LagFlushResult struct {
	EventToReadLag             time.Duration
	ReadToWorkerReceiveLag     time.Duration
	ReceiveToApplyLag          time.Duration
	EndToEndLag                time.Duration
	EndToEndWithRetryLag       time.Duration
	ReceiveToApplyWithRetryLag time.Duration
}

// metricAccumulator unifies duration sum and event count for simple averaging
// without signed 64-bit integer overflow under high-throughput conditions.
type metricAccumulator struct {
	totalNs float64
	count   int64
}

func (m *metricAccumulator) Add(d time.Duration) {
	m.totalNs += float64(d.Nanoseconds())
	m.count++
}

func (m *metricAccumulator) Average() time.Duration {
	if m.count > 0 {
		avgNs := m.totalNs / float64(m.count)
		if avgNs >= float64(math.MaxInt64) {
			return time.Duration(math.MaxInt64)
		}
		if avgNs <= float64(math.MinInt64) {
			return time.Duration(math.MinInt64)
		}
		return time.Duration(avgNs)
	}
	return 0
}

func (m *metricAccumulator) Reset() {
	m.totalNs = 0
	m.count = 0
}

// LagTracker tracks replication lag measurements
type LagTracker struct {
	eventToRead               metricAccumulator
	readToWorkerReceive       metricAccumulator
	receiveToApply            metricAccumulator
	endToEnd                  metricAccumulator
	endToEndWithRetry         metricAccumulator
	receiveToApplyWithRetry   metricAccumulator
}

// NewPartitionTracker creates a new instance of LagTracker (NewLagTracker signature alias)
func NewLagTracker() *LagTracker {
	return &LagTracker{}
}

// RecordLags records the replication lag metrics for a batch of write operations
func (lt *LagTracker) RecordLags(ops []WriteOperation) {
	for _, op := range ops {
		if op.SuccessTime.IsZero() {
			continue
		}

		if !op.ReadTime.IsZero() && !op.EventTime.IsZero() {
			lt.eventToRead.Add(op.ReadTime.Sub(op.EventTime))
		}

		if !op.WorkerReceiveTime.IsZero() && !op.ReadTime.IsZero() {
			lt.readToWorkerReceive.Add(op.WorkerReceiveTime.Sub(op.ReadTime))
		}

		if op.SuccessAfterRetry {
			if !op.EventTime.IsZero() {
				lt.endToEndWithRetry.Add(op.SuccessTime.Sub(op.EventTime))
			}
			if !op.WorkerReceiveTime.IsZero() {
				lt.receiveToApplyWithRetry.Add(op.SuccessTime.Sub(op.WorkerReceiveTime))
			}
		} else {
			if !op.WorkerReceiveTime.IsZero() {
				lt.receiveToApply.Add(op.SuccessTime.Sub(op.WorkerReceiveTime))
			}
			if !op.EventTime.IsZero() {
				lt.endToEnd.Add(op.SuccessTime.Sub(op.EventTime))
			}
		}
	}
}

// RecordEventToRead records only the event-to-read lag metric.
// This is useful in dry-run mode where full WriteOperations are not constructed.
func (lt *LagTracker) RecordEventToRead(eventTime, readTime time.Time) {
	if !readTime.IsZero() && !eventTime.IsZero() {
		lt.eventToRead.Add(readTime.Sub(eventTime))
	}
}


// Flush returns the accumulated lag averages, then resets internal counters
func (lt *LagTracker) Flush() LagFlushResult {
	res := LagFlushResult{
		EventToReadLag:             lt.eventToRead.Average(),
		ReadToWorkerReceiveLag:     lt.readToWorkerReceive.Average(),
		ReceiveToApplyLag:          lt.receiveToApply.Average(),
		EndToEndLag:                lt.endToEnd.Average(),
		EndToEndWithRetryLag:       lt.endToEndWithRetry.Average(),
		ReceiveToApplyWithRetryLag: lt.receiveToApplyWithRetry.Average(),
	}

	lt.eventToRead.Reset()
	lt.readToWorkerReceive.Reset()
	lt.receiveToApply.Reset()
	lt.endToEnd.Reset()
	lt.endToEndWithRetry.Reset()
	lt.receiveToApplyWithRetry.Reset()

	return res
}
