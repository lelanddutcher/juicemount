package metadata

import (
	"fmt"
	"testing"
	"time"
)

func TestCacheEviction(t *testing.T) {
	const maxCache = 100

	store, err := OpenWithMaxCacheSize(":memory:", maxCache)
	if err != nil {
		t.Fatalf("OpenWithMaxCacheSize: %v", err)
	}
	defer store.Close()

	// Insert 200 entries with ascending mtime so we can verify
	// the newest entries survive eviction.
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

	pathSize, inodeSize := store.CacheStats()

	// Cache should be bounded to maxCacheSize.
	if pathSize > maxCache {
		t.Errorf("pathCache size = %d, want <= %d", pathSize, maxCache)
	}
	if inodeSize > maxCache {
		t.Errorf("inodeCache size = %d, want <= %d", inodeSize, maxCache)
	}

	// The most recent entries (highest mtime) should be retained.
	// Entry 199 has the latest mtime and should be in the cache.
	newest := store.LookupByPath("dir/file_0199.txt")
	if newest == nil {
		t.Error("newest entry (file_0199.txt) should be in cache after eviction")
	}

	// Entry 0 has the oldest mtime and should be evicted.
	oldest := store.LookupByPath("dir/file_0000.txt")
	if oldest != nil {
		t.Error("oldest entry (file_0000.txt) should have been evicted")
	}

	// Verify that entries near the boundary are correct.
	// With 200 entries and maxCache=100, entries 100-199 should survive.
	for i := 100; i < 200; i++ {
		p := fmt.Sprintf("dir/file_%04d.txt", i)
		if store.LookupByPath(p) == nil {
			t.Errorf("entry %s (recent) should be in cache", p)
		}
	}
	for i := 0; i < 100; i++ {
		p := fmt.Sprintf("dir/file_%04d.txt", i)
		if store.LookupByPath(p) != nil {
			t.Errorf("entry %s (old) should have been evicted", p)
		}
	}
}

func TestCacheStatsReturnsCorrectSizes(t *testing.T) {
	store, err := OpenWithMaxCacheSize(":memory:", 1000)
	if err != nil {
		t.Fatalf("OpenWithMaxCacheSize: %v", err)
	}
	defer store.Close()

	now := time.Now()
	for i := 0; i < 50; i++ {
		e := MakeEntry(fmt.Sprintf("f/%d.txt", i), false, 0, now, uint64(i+1))
		if err := store.Insert(e); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	pathSize, inodeSize := store.CacheStats()
	if pathSize != 50 {
		t.Errorf("pathCacheSize = %d, want 50", pathSize)
	}
	if inodeSize != 50 {
		t.Errorf("inodeCacheSize = %d, want 50", inodeSize)
	}
}

func TestCacheEvictionOnRebuild(t *testing.T) {
	const maxCache = 50

	store, err := OpenWithMaxCacheSize(":memory:", maxCache)
	if err != nil {
		t.Fatalf("OpenWithMaxCacheSize: %v", err)
	}
	defer store.Close()

	// Insert entries directly to DB via BulkInsert (which calls evictOldest).
	baseTime := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	entries := make([]*Entry, 100)
	for i := 0; i < 100; i++ {
		entries[i] = MakeEntry(
			fmt.Sprintf("rebuild/%d", i),
			false, 0,
			baseTime.Add(time.Duration(i)*time.Minute),
			uint64(2000+i),
		)
	}
	if err := store.BulkInsert(entries, 500); err != nil {
		t.Fatalf("BulkInsert: %v", err)
	}

	pathSize, _ := store.CacheStats()
	if pathSize > maxCache {
		t.Errorf("after rebuild, pathCache size = %d, want <= %d", pathSize, maxCache)
	}
}
