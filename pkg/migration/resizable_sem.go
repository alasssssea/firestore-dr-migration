package migration

import (
	"context"
	"sync"
)

// resizableSem is a counting semaphore whose limit can change at runtime. It caps
// how many collections load concurrently (ConcurrentCollections) so the operator
// can speed up or throttle a RUNNING job without a restart (see Migrator.Reconfig).
//
// Growing the limit admits waiters immediately; shrinking NEVER preempts in-flight
// holders — it only makes future Acquire calls block until enough holders Release
// to fall under the new limit. This upholds the "never disturb a table that is
// already loading" rule.
type resizableSem struct {
	mu    sync.Mutex
	cond  *sync.Cond
	limit int
	held  int
}

func newResizableSem(limit int) *resizableSem {
	if limit < 1 {
		limit = 1
	}
	s := &resizableSem{limit: limit}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Acquire blocks until a slot is free or ctx is cancelled. On cancellation it
// returns ctx.Err() without taking a slot.
func (s *resizableSem) Acquire(ctx context.Context) error {
	// Wake the parked waiter if ctx is cancelled while it sits in cond.Wait.
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer stop()

	s.mu.Lock()
	defer s.mu.Unlock()
	for s.held >= s.limit {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.cond.Wait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.held++
	return nil
}

// Release returns a slot and wakes a waiter.
func (s *resizableSem) Release() {
	s.mu.Lock()
	if s.held > 0 {
		s.held--
	}
	s.cond.Broadcast()
	s.mu.Unlock()
}

// SetLimit changes the maximum number of concurrent holders. Growing admits more
// waiters at once; shrinking takes effect as current holders release.
func (s *resizableSem) SetLimit(limit int) {
	if limit < 1 {
		limit = 1
	}
	s.mu.Lock()
	s.limit = limit
	s.cond.Broadcast()
	s.mu.Unlock()
}

// Limit returns the current limit.
func (s *resizableSem) Limit() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.limit
}
