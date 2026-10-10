//go:build latency_acceptance

package metadata

import (
	"context"
	"errors"
)

// QuiesceForLatencyAcceptance joins the fixture's background SQL worker before
// an external-package test holds the connection pool. This test-only method
// requires FTS disabled and a quiescent caller; it does not stop caller-owned
// operations. The worker stays stopped for the remainder of this fixture.
func (s *Store) QuiesceForLatencyAcceptance(ctx context.Context) error {
	if s.ftsStop != nil {
		return errors.New("latency acceptance requires JM_FTS_DEFER=0")
	}
	if s.pruneLadderGCStop == nil {
		return nil
	}
	s.pruneLadderGCStopOnce.Do(func() { close(s.pruneLadderGCStop) })
	select {
	case <-s.pruneLadderGCDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SetCacheBudgetForLatencyAcceptance sets the budget before the caller starts
// its owned eviction. It does not itself evict or alter the serving substrate.
func (s *Store) SetCacheBudgetForLatencyAcceptance(budget int) {
	s.mu.Lock()
	s.maxCacheSize = budget
	s.mu.Unlock()
}

// EvictForLatencyAcceptance runs the production eviction, including pin lookup.
func (s *Store) EvictForLatencyAcceptance() { s.evictWithPinnedSet() }

// WriterBusyForLatencyAcceptance observes a writer without holding up readers.
// The fixture must join other workers to attribute this to its target writer.
func (s *Store) WriterBusyForLatencyAcceptance() bool {
	if !s.writeMu.TryLock() {
		return true
	}
	s.writeMu.Unlock()
	return false
}

// CacheLockActiveForLatencyAcceptance observes real eviction capture/apply work.
// Capture now uses bounded read locks, which should allow concurrent navigation.
func (s *Store) CacheLockActiveForLatencyAcceptance() bool {
	if !s.mu.TryLock() {
		return true
	}
	s.mu.Unlock()
	return false
}

// PathCachedForLatencyAcceptance inspects residency without durable fallback.
func (s *Store) PathCachedForLatencyAcceptance(path string) bool {
	return s.lookupByPathRAM(path) != nil
}
