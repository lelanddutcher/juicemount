package metadata

import (
	"sync"
	"testing"
	"time"
)

// Store lookups are consumed after the map lock is released by NFS GETATTR,
// READDIR and sidecar paths. A writer must not mutate the object those readers
// hold; race detection originally caught UpdateSize writing Size/Mtime while
// FileInfo read the same fields.
func TestLookupSnapshotsRemainStableDuringUpdate(t *testing.T) {
	s := newTestStore(t)
	base := time.Unix(1_700_000_000, 0)
	if err := s.Insert(MakeEntry("video.mov", false, 1, base, 42)); err != nil {
		t.Fatal(err)
	}

	before := s.LookupByPath("video.mov")
	if before == nil {
		t.Fatal("initial lookup returned nil")
	}
	before.CacheGetAttr([]byte{1, 2, 3})

	const iterations = 100
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 2; i <= iterations+1; i++ {
			if err := s.UpdateSize("video.mov", int64(i), base.Add(time.Duration(i)*time.Second)); err != nil {
				t.Errorf("UpdateSize(%d): %v", i, err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations*10; i++ {
			e := s.LookupByPath("video.mov")
			if e == nil {
				t.Error("concurrent lookup returned nil")
				return
			}
			fi := e.FileInfo()
			_ = fi.Size()
			_ = fi.ModTime()
			_ = fi.Mode()
			_ = e.CachedGetAttr()
		}
	}()
	wg.Wait()

	// The pre-update snapshot and its generation-specific GETATTR bytes stay
	// immutable even after the cache's authoritative entry advances.
	if before.Size != 1 || !before.Mtime.Equal(base) {
		t.Fatalf("old snapshot mutated: size=%d mtime=%v", before.Size, before.Mtime)
	}
	if got := before.CachedGetAttr(); len(got) != 3 {
		t.Fatalf("old snapshot GETATTR cache was invalidated, len=%d", len(got))
	}

	after := s.LookupByPath("video.mov")
	if after.Size != iterations+1 {
		t.Fatalf("final size=%d, want %d", after.Size, iterations+1)
	}
	if got := after.CachedGetAttr(); got != nil {
		t.Fatalf("new generation inherited stale GETATTR bytes: %v", got)
	}
}
