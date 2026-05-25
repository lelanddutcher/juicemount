package metadata

import (
	"fmt"
	"testing"
	"time"
)

// fakePinChecker lets unit tests drive the pinned set without spinning up
// the real pin.Store SQLite layer.
type fakePinChecker struct {
	paths map[string]struct{}
	err   error // when non-nil, PinnedPaths returns this error to exercise fail-safe paths
}

func (f *fakePinChecker) PinnedPaths() (map[string]struct{}, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := make(map[string]struct{}, len(f.paths))
	for p := range f.paths {
		out[p] = struct{}{}
	}
	return out, nil
}

// QA-30 Layer C — evictOldest must retain pinned files even when they are
// older than the eviction cutoff and the cache budget would otherwise drop
// them. Pinning is the user's explicit offline-availability contract;
// dropping a pinned file's cache mapping causes its kernel-cached NFS
// handle to surface as ESTALE on next access.
func TestEvictOldest_RetainsPinnedFilesPastBudget(t *testing.T) {
	const maxCache = 50

	store, err := OpenWithMaxCacheSize(":memory:", maxCache)
	if err != nil {
		t.Fatalf("OpenWithMaxCacheSize: %v", err)
	}
	defer store.Close()

	// Pin 10 of the OLDEST files. Without QA-30 these would all be
	// evicted in favor of the newest 50.
	pinned := make(map[string]struct{}, 10)
	for i := 0; i < 10; i++ {
		pinned[fmt.Sprintf("dir/file_%04d.txt", i)] = struct{}{}
	}
	store.SetPinChecker(&fakePinChecker{paths: pinned})

	// Insert 200 files, file_0000 oldest, file_0199 newest.
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	entries := make([]*Entry, 200)
	for i := 0; i < 200; i++ {
		entries[i] = MakeEntry(
			fmt.Sprintf("dir/file_%04d.txt", i),
			false,
			int64(i*100),
			baseTime.Add(time.Duration(i)*time.Second),
			uint64(1000+i),
		)
	}
	if err := store.BulkInsert(entries, 500); err != nil {
		t.Fatalf("BulkInsert: %v", err)
	}

	// All 10 pinned files (oldest) must survive eviction.
	for i := 0; i < 10; i++ {
		p := fmt.Sprintf("dir/file_%04d.txt", i)
		if got := store.LookupByPath(p); got == nil {
			t.Errorf("pinned old entry %s was evicted; QA-30 violated", p)
		}
		// The inode mapping must also survive — that's the whole point.
		if got := store.LookupByInode(uint64(1000 + i)); got == nil {
			t.Errorf("pinned inode %d (%s) was evicted from inodeCache", 1000+i, p)
		}
	}

	// Newest files should still be in cache (LRU behavior preserved for
	// non-pinned files).
	if got := store.LookupByPath("dir/file_0199.txt"); got == nil {
		t.Error("newest entry should remain in cache")
	}

	// Cache size now: 10 pinned + budget for non-pinned. Verify we don't
	// exceed maxCache too dramatically — the architect-approved policy is
	// that mandatory (dirs+pinned) is allowed to push past maxCache, but
	// the non-pinned-file budget should be (maxCache - mandatory).
	// Mandatory = 10 pinned (no dirs in this test).
	// Budget = 50 - 10 = 40 non-pinned files.
	// Total: 10 + 40 = 50, fits exactly.
	pathSize, inodeSize := store.CacheStats()
	if pathSize != inodeSize {
		t.Errorf("path/inode cache out of sync: path=%d inode=%d", pathSize, inodeSize)
	}
	if pathSize > maxCache+10 {
		t.Errorf("cache grew well past budget: size=%d max=%d", pathSize, maxCache)
	}
}

// QA-30: nil PinChecker is safe (back-compat for tests / non-pin paths).
func TestEvictOldest_NilPinCheckerIsSafe(t *testing.T) {
	const maxCache = 50
	store, err := OpenWithMaxCacheSize(":memory:", maxCache)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	// No SetPinChecker call — pinChecker remains nil.

	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	entries := make([]*Entry, 200)
	for i := 0; i < 200; i++ {
		entries[i] = MakeEntry(
			fmt.Sprintf("dir/file_%04d.txt", i),
			false, int64(i*100),
			baseTime.Add(time.Duration(i)*time.Second),
			uint64(1000+i),
		)
	}
	if err := store.BulkInsert(entries, 500); err != nil {
		t.Fatalf("BulkInsert: %v", err)
	}
	// Existing LRU behavior preserved: oldest gone, newest kept.
	if got := store.LookupByPath("dir/file_0000.txt"); got != nil {
		t.Error("oldest should be evicted with nil PinChecker")
	}
	if got := store.LookupByPath("dir/file_0199.txt"); got == nil {
		t.Error("newest should be retained with nil PinChecker")
	}
}

// QA-30: pinnedSetPublic returns an empty map (not nil) when no checker
// is wired. Callers (syncMetadata) iterate it safely.
func TestPinnedSetPublic_NilChecker(t *testing.T) {
	store, err := OpenWithMaxCacheSize(":memory:", 100)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	set, err := store.pinnedSetPublic()
	if err != nil {
		t.Fatalf("nil-checker case should not error, got %v", err)
	}
	if set == nil {
		t.Fatal("pinnedSetPublic must return non-nil even when checker is nil")
	}
	if len(set) != 0 {
		t.Errorf("nil-checker set should be empty, got %d entries", len(set))
	}
}

// QA-30: pinnedSetPublic delegates to the installed checker.
func TestPinnedSetPublic_DelegatesToChecker(t *testing.T) {
	store, err := OpenWithMaxCacheSize(":memory:", 100)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	store.SetPinChecker(&fakePinChecker{
		paths: map[string]struct{}{"/a": {}, "/b": {}},
	})
	set, perr := store.pinnedSetPublic()
	if perr != nil {
		t.Fatalf("unexpected error: %v", perr)
	}
	if _, ok := set["/a"]; !ok {
		t.Error("/a missing from pinned set")
	}
	if _, ok := set["/b"]; !ok {
		t.Error("/b missing from pinned set")
	}
	if len(set) != 2 {
		t.Errorf("expected 2 pinned, got %d", len(set))
	}
}

// QA-30 HIGH-2 fail-safe: PinChecker error must propagate (caller's
// responsibility to skip prune/eviction).
func TestPinnedSetPublic_PropagatesError(t *testing.T) {
	store, err := OpenWithMaxCacheSize(":memory:", 100)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	store.SetPinChecker(&fakePinChecker{err: fmt.Errorf("simulated DB hiccup")})
	_, perr := store.pinnedSetPublic()
	if perr == nil {
		t.Fatal("expected error to propagate from PinChecker")
	}
}

// QA-30 HIGH-2 fail-safe: evictOldest must SKIP eviction entirely when
// PinnedPaths errors. Without this, a transient pin-DB hiccup would
// unprotect every pinned file and re-create the bug.
func TestEvictOldest_SkipsOnPinCheckerError(t *testing.T) {
	const maxCache = 50
	store, err := OpenWithMaxCacheSize(":memory:", maxCache)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	// PinChecker that always errors.
	store.SetPinChecker(&fakePinChecker{err: fmt.Errorf("simulated pin DB error")})

	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	entries := make([]*Entry, 200)
	for i := 0; i < 200; i++ {
		entries[i] = MakeEntry(
			fmt.Sprintf("dir/file_%04d.txt", i),
			false, int64(i*100),
			baseTime.Add(time.Duration(i)*time.Second),
			uint64(1000+i),
		)
	}
	if err := store.BulkInsert(entries, 500); err != nil {
		t.Fatalf("BulkInsert: %v", err)
	}

	// Eviction should have been SKIPPED. All 200 entries remain.
	pathSize, _ := store.CacheStats()
	if pathSize != 200 {
		t.Errorf("eviction should have been skipped on pin-checker error; got cache size %d, want 200", pathSize)
	}
}
