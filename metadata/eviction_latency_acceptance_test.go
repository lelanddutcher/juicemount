//go:build latency_acceptance

package metadata

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// Exercise actual eviction CPU work, without any SQL or backend delay. Once
// eviction owns the serving lock, known metadata must regain service promptly.
func TestEvictionShadowBatchKeepsCachedNavigationResponsive(t *testing.T) {
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_SERVE_FROM_SQLITE", "0")
	t.Setenv("JM_READDIR_PAGINATED", "0")
	const count, budget = 32768, 16384
	s, err := OpenWithMaxCacheSize(filepath.Join(t.TempDir(), "mirror.db"), budget)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.QuiesceForLatencyAcceptance(ctx); err != nil {
		t.Fatal(err)
	}
	stamp := time.Unix(1700000000, 0)
	s.InsertToCache(MakeEntry("known", true, 0, stamp, 10))
	s.InsertToCache(MakeEntry("known/clip.mov", false, 4096, stamp, 11))
	s.SetPinChecker(&fakePinChecker{paths: map[string]struct{}{"known/clip.mov": {}}})
	for i := 0; i < count; i++ {
		s.InsertToCache(MakeEntry(fmt.Sprintf("bulk/file-%05d", i), false, 1, stamp.Add(time.Duration(i)*time.Second), uint64(1000+i)))
	}
	done := make(chan time.Duration, 1)
	go func() { start := time.Now(); s.evictWithPinnedSet(); done <- time.Since(start) }()
	joined := false
	defer func() {
		if !joined {
			select {
			case <-done:
			case <-time.After(20 * time.Second):
				t.Error("eviction did not join")
			}
		}
	}()
	deadline := time.Now().Add(time.Second)
	locked := false
	for time.Now().Before(deadline) {
		if !s.mu.TryLock() {
			locked = true
			break
		}
		s.mu.Unlock()
		time.Sleep(100 * time.Microsecond)
	}
	if !locked {
		t.Fatal("fixture did not observe eviction owning the serving lock")
	}
	start := time.Now()
	e := s.lookupByPathRAM("known/clip.mov")
	i := s.lookupByInodeRAM(11)
	children, err := s.listChildrenRAM("known")
	elapsed := time.Since(start)
	if e == nil || i == nil || e.Size != 4096 || i.Path != "known/clip.mov" || err != nil || len(children) != 1 || children[0].Inode != 11 {
		t.Fatal("eviction corrupted pinned navigation")
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("cached navigation blocked %s behind eviction; budget100ms", elapsed)
	}
	select {
	case total := <-done:
		joined = true
		t.Logf("eviction=%s cached_navigation=%s entries=%d budget=%d", total, elapsed, count, budget)
	case <-time.After(20 * time.Second):
		t.Fatal("eviction did not finish")
	}
	if size, _ := s.CacheStats(); size != budget {
		t.Fatalf("residency=%d want=%d", size, budget)
	}
	if s.lookupByPathRAM("bulk/file-00000") != nil || s.lookupByPathRAM("bulk/file-32767") == nil {
		t.Fatal("eviction ordering changed")
	}
	if _, ok := s.LookupRecentlyEvicted(1000); !ok {
		t.Fatal("evicted file lost its recovery shadow")
	}
}
