package metadata

import (
	"testing"
	"time"
)

// The fixed clock exercises an entire expiry cohort without sleeping for TTL.
// In production the caller holds mu and passes time.Now once per insertion.
func TestShadowExpiryCohortDoesNotRescanPerInsertion(t *testing.T) {
	s := &Store{}
	start := time.Now()
	for i := 0; i < 2048; i++ {
		s.shadowEvictedAtLocked(MakeEntry("file", false, int64(i), start, uint64(i)), start.Add(time.Duration(i)*time.Millisecond))
	}
	first := start.Add(ShadowTTL + 100*time.Millisecond)
	s.shadowEvictedAtLocked(MakeEntry("new", false, 1, start, 9000), first)
	finishShadowExpiryForTest(t, s, first)
	if _, ok := s.recentlyEvicted[99]; ok {
		t.Fatal("expired record survived scheduled scan")
	}
	if _, ok := s.recentlyEvicted[100]; !ok {
		t.Fatal("record was expired at its exact deadline rather than after it")
	}
	deadline := s.shadowSweepAfter
	if deadline.Before(first.Add(shadowSweepInterval)) {
		t.Fatal("staggered cohort can immediately trigger another full scan")
	}
	for i := 1; i < 100; i++ {
		s.shadowEvictedAtLocked(MakeEntry("new", false, 1, start, uint64(9000+i)), first.Add(time.Duration(i)*time.Millisecond))
	}
	if _, ok := s.recentlyEvicted[100]; !ok {
		t.Fatal("cohort was rescanned during cooldown")
	}
	if s.shadowSweepAfter != deadline {
		t.Fatal("insertions moved the cleanup deadline")
	}
	s.shadowEvictedAtLocked(MakeEntry("after", false, 1, start, 10000), deadline)
	finishShadowExpiryForTest(t, s, deadline)
	if _, ok := s.recentlyEvicted[100]; ok {
		t.Fatal("expired cohort was not removed after cooldown")
	}
	if _, ok := s.recentlyEvicted[1500]; !ok {
		t.Fatal("unexpired record removed")
	}
}

func TestShadowExpiryRefreshAndDeletedIdentitySurviveOldDeadline(t *testing.T) {
	s := &Store{}
	start := time.Now()
	for i := 0; i < 2048; i++ {
		s.shadowEvictedAtLocked(MakeEntry("old", false, 1, start, uint64(i)), start)
	}
	refreshed := start.Add(ShadowTTL - time.Second)
	s.shadowEvictedAtLocked(MakeEntry("replacement", false, 4096, start, 7), refreshed)
	rec := s.recentlyEvicted[7]
	rec.Deleted = true
	s.recentlyEvicted[7] = rec
	s.shadowEvictedAtLocked(MakeEntry("trigger", false, 1, start, 9000), start.Add(ShadowTTL+time.Nanosecond))
	finishShadowExpiryForTest(t, s, start.Add(ShadowTTL+time.Nanosecond))
	if len(s.recentlyEvicted) != 2 {
		t.Fatalf("stale cohort retained: count=%d", len(s.recentlyEvicted))
	}
	got, ok := s.recentlyEvicted[7]
	if !ok || !got.Deleted || got.Path != "replacement" || got.Size != 4096 || !got.ExpiresAt.Equal(refreshed.Add(ShadowTTL)) {
		t.Fatal("old expiry removed or changed refreshed deleted generation")
	}
	// An expired record retained until cleanup must never become recoverable.
	s.recentlyEvicted[99] = evictedShadow{ExpiresAt: time.Now().Add(-time.Second)}
	if _, ok := s.LookupRecentlyEvicted(99); ok {
		t.Fatal("physical cleanup delay extended visible TTL")
	}
}

// Physical cleanup now advances in bounded chunks; expiry/refresh/cooldown
// assertions above remain unchanged once that finite pass has completed.
func finishShadowExpiryForTest(t *testing.T, s *Store, now time.Time) {
	t.Helper()
	for n := 0; s.shadowSweep != nil; n++ {
		if n > 100 {
			t.Fatal("shadow sweep did not terminate")
		}
		before := len(s.recentlyEvicted)
		s.sweepShadowChunkLocked(now)
		if before-len(s.recentlyEvicted) > evictionChunk {
			t.Fatal("unbounded shadow cleanup")
		}
	}
}
