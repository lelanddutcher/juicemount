package metadata

import (
	"testing"
	"time"
)

func TestDurableServeFallbackCompletesEvictedDirectory(t *testing.T) {
	t.Setenv("JM_DURABLE_SERVE_FALLBACK", "1")

	store, err := OpenWithMaxCacheSize(":memory:", 3)
	if err != nil {
		t.Fatalf("OpenWithMaxCacheSize: %v", err)
	}
	defer store.Close()

	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	paths := []string{
		"media/clip-a.mov",
		"media/clip-b.mov",
		"media/clip-c.mov",
		"media/clip-d.mov",
		"media/clip-e.mov",
	}
	entries := []*Entry{MakeEntry("media", true, 0, base, 10)}
	for i, entryPath := range paths {
		entries = append(entries, MakeEntry(
			entryPath,
			false,
			int64(i+1),
			base.Add(time.Duration(i)*time.Second),
			uint64(20+i),
		))
	}
	if err := store.BulkInsert(entries, 32); err != nil {
		t.Fatalf("BulkInsert: %v", err)
	}

	if !store.cacheEvicted.Load() {
		t.Fatal("expected bounded RAM shadow to record eviction")
	}
	if got := store.lookupByPathRAM(paths[0]); got != nil {
		t.Fatalf("oldest file unexpectedly remained in RAM: %+v", got)
	}
	if got := store.LookupByPath(paths[0]); got == nil || got.Inode != 20 {
		t.Fatalf("durable point fallback did not recover evicted file: %+v", got)
	}
	if got := store.LookupByInode(20); got == nil || got.Path != paths[0] {
		t.Fatalf("durable inode fallback did not recover evicted file: %+v", got)
	}

	children, err := store.ListChildrenForReadDir("media")
	if err != nil {
		t.Fatalf("ListChildrenForReadDir: %v", err)
	}
	if len(children) != len(paths) {
		t.Fatalf("durable READDIR returned %d children, want all %d", len(children), len(paths))
	}
}
