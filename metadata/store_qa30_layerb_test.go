package metadata

import (
	"fmt"
	"testing"
	"time"
)

// QA-30 Layer B: when an entry is removed via Delete, it must be parked
// in the recently-evicted shadow map keyed by inode, with the original
// metadata available for recovery.
func TestShadow_PopulatedOnDelete(t *testing.T) {
	store, err := OpenWithMaxCacheSize(":memory:", 100)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	e := MakeEntry("foo/bar.mov", false, 1024, time.Now(), 12345)
	if err := store.Insert(e); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := store.Delete(e.Path); err != nil {
		t.Fatalf("delete: %v", err)
	}

	rec, ok := store.LookupRecentlyEvicted(12345)
	if !ok {
		t.Fatal("evicted shadow not found")
	}
	if rec.Path != "foo/bar.mov" {
		t.Errorf("shadow path wrong: got %q", rec.Path)
	}
	if rec.Size != 1024 {
		t.Errorf("shadow size wrong: got %d", rec.Size)
	}
}

// QA-30 Layer B: shadow populated on DeleteFromCache too.
func TestShadow_PopulatedOnDeleteFromCache(t *testing.T) {
	store, err := OpenWithMaxCacheSize(":memory:", 100)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	e := MakeEntry("baz.txt", false, 42, time.Now(), 999)
	store.InsertToCache(e)
	store.DeleteFromCache(e.Path)
	if _, ok := store.LookupRecentlyEvicted(999); !ok {
		t.Fatal("expected shadow entry after DeleteFromCache")
	}
}

// QA-30 Layer B: shadow populated on DeletePaths.
func TestShadow_PopulatedOnDeletePaths(t *testing.T) {
	store, err := OpenWithMaxCacheSize(":memory:", 100)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	for i := 0; i < 5; i++ {
		e := MakeEntry(fmt.Sprintf("p%d", i), false, int64(i), time.Now(), uint64(1000+i))
		if err := store.Insert(e); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if err := store.DeletePaths([]string{"p0", "p2", "p4"}); err != nil {
		t.Fatalf("DeletePaths: %v", err)
	}
	for _, want := range []uint64{1000, 1002, 1004} {
		if _, ok := store.LookupRecentlyEvicted(want); !ok {
			t.Errorf("shadow missing for inode %d", want)
		}
	}
}

// QA-30 Layer B: shadow respects TTL — entries past expiry don't return.
func TestShadow_RespectsTTL(t *testing.T) {
	store, err := OpenWithMaxCacheSize(":memory:", 100)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	// Manually inject a past-expiry shadow.
	store.mu.Lock()
	if store.recentlyEvicted == nil {
		store.recentlyEvicted = make(map[uint64]evictedShadow)
	}
	store.recentlyEvicted[7777] = evictedShadow{
		Path:      "ghost",
		Size:      1,
		ExpiresAt: time.Now().Add(-time.Minute), // already expired
	}
	store.mu.Unlock()
	if _, ok := store.LookupRecentlyEvicted(7777); ok {
		t.Error("expired shadow entry should not return ok")
	}
}

// QA-30 Layer B: RecoverShadow promotes the shadow back into the live
// caches and clears the shadow entry.
func TestShadow_RecoverShadow(t *testing.T) {
	store, err := OpenWithMaxCacheSize(":memory:", 100)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	e := MakeEntry("doc.txt", false, 500, time.Now(), 5555)
	store.Insert(e)
	store.Delete(e.Path)

	rec, ok := store.LookupRecentlyEvicted(5555)
	if !ok {
		t.Fatal("setup: shadow missing")
	}

	recovered := store.RecoverShadow(rec, 5555)
	if recovered == nil {
		t.Fatal("RecoverShadow returned nil")
	}
	if got := store.LookupByInode(5555); got == nil {
		t.Error("after recover, LookupByInode should succeed")
	}
	if got := store.LookupByPath("doc.txt"); got == nil {
		t.Error("after recover, LookupByPath should succeed")
	}
	// Shadow should have been cleared.
	if _, ok := store.LookupRecentlyEvicted(5555); ok {
		t.Error("shadow should be cleared after successful recovery")
	}
}

// QA-30 Layer B: shadow populated on evictOldest for entries that drop out
// of the LRU budget.
func TestShadow_PopulatedOnEvictOldest(t *testing.T) {
	const maxCache = 20
	store, err := OpenWithMaxCacheSize(":memory:", maxCache)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	var entries []*Entry
	for i := 0; i < 50; i++ {
		e := MakeEntry(
			fmt.Sprintf("f/%04d", i),
			false, int64(i), baseTime.Add(time.Duration(i)*time.Second),
			uint64(2000+i),
		)
		entries = append(entries, e)
	}
	if err := store.BulkInsert(entries, 500); err != nil {
		t.Fatalf("BulkInsert: %v", err)
	}
	// Oldest 30 should have been evicted → in shadow.
	for i := 0; i < 30; i++ {
		inode := uint64(2000 + i)
		if _, ok := store.LookupRecentlyEvicted(inode); !ok {
			t.Errorf("evicted file %04d (inode=%d) should be in shadow", i, inode)
			break
		}
	}
}
