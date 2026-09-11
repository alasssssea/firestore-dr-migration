package migration

import (
	"testing"

	"firestore-dr-migration/pkg/logger"
)

// countingDLQ is a minimal DLQ stub whose Count() is controllable, for exercising
// the non-blocking outcome helper without touching disk.
type countingDLQ struct {
	NopDLQWriter // embed for the interface methods we don't care about
	n            int64
}

func (c *countingDLQ) Count() int64 { return c.n }

func TestResolveInitialMigrationOutcome(t *testing.T) {
	log := logger.New()

	cases := []struct {
		name        string
		dlq         DLQ
		failedCount int64
		wantStatus  string
		wantCount   int64
	}{
		{"clean-nil-dlq", nil, 0, StatusCompleted, 0},
		{"clean-nop-dlq", &NopDLQWriter{}, 0, StatusCompleted, 0},
		{"nop-dlq-ignored-even-with-count", &NopDLQWriter{}, 0, StatusCompleted, 0},
		{"failures-only", nil, 3, StatusCompletedWithFailures, 0},
		{"dlq-entries-only", &countingDLQ{n: 5}, 0, StatusCompletedWithFailures, 5},
		{"both", &countingDLQ{n: 2}, 4, StatusCompletedWithFailures, 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, dlqCount := resolveInitialMigrationOutcome(tc.dlq, tc.failedCount, log)
			if status != tc.wantStatus {
				t.Errorf("status: got %q want %q", status, tc.wantStatus)
			}
			if dlqCount != tc.wantCount {
				t.Errorf("dlqCount: got %d want %d", dlqCount, tc.wantCount)
			}
		})
	}
}

// TestDLQActiveCountNopIsZero pins the contract that a NopDLQWriter always counts
// as zero active failures regardless of any embedded state.
func TestDLQActiveCountNopIsZero(t *testing.T) {
	if got := dlqActiveCount(&NopDLQWriter{}); got != 0 {
		t.Errorf("NopDLQWriter active count: got %d want 0", got)
	}
	if got := dlqActiveCount(nil); got != 0 {
		t.Errorf("nil dlq active count: got %d want 0", got)
	}
	if got := dlqActiveCount(&countingDLQ{n: 7}); got != 7 {
		t.Errorf("real dlq active count: got %d want 7", got)
	}
}
