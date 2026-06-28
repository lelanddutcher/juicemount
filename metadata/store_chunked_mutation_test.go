package metadata

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// withChunk temporarily shrinks cacheMutationChunk so a modest delta exercises
// the multi-chunk lock-yield path deterministically, restoring it after.
func withChunk(t *testing.T, n int) {
	t.Helper()
	prev := cacheMutationChunk
	cacheMutationChunk = n
	t.Cleanup(func() { cacheMutationChunk = prev })
}

// mkEntriesBase builds `count` distinct file entries under `prefix`, assigning
// inodes from `inodeBase` so callers can keep separate batches in disjoint
// inode namespaces (a shared inode would trigger evictInodeOrphanLocked and
// re-home the path — correct production behavior, but not what these tests
// mean to exercise).
func mkEntriesBase(prefix string, count int, inodeBase uint64) []*Entry {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	out := make([]*Entry, count)
	for i := 0; i < count; i++ {
		out[i] = MakeEntry(
			fmt.Sprintf("%s/file_%05d.mov", prefix, i),
			false,
			int64(i*1024),
			base.Add(time.Duration(i)*time.Second),
			inodeBase+uint64(i),
		)
	}
	return out
}

func mkEntries(prefix string, count int) []*Entry {
	return mkEntriesBase(prefix, count, 2_000_000)
}

// TestBulkInsertChunkedCorrectness proves that splitting BulkInsert's cache
// rebuild across multiple bounded s.mu.Lock acquisitions produces the SAME
// final cache state as a single critical section — every entry resolves by
// path AND by inode, and childrenIdx is populated. maxCacheSize is huge so
// evictOldest never fires (the production case), isolating the chunk loop.
func TestBulkInsertChunkedCorrectness(t *testing.T) {
	withChunk(t, 64) // force ~16 chunks over 1000 entries

	store, err := OpenWithMaxCacheSize(":memory:", DefaultMaxCacheSize)
	if err != nil {
		t.Fatalf("OpenWithMaxCacheSize: %v", err)
	}
	defer store.Close()

	const n = 1000
	entries := mkEntries("movies", n)
	if err := store.BulkInsert(entries, 500); err != nil {
		t.Fatalf("BulkInsert: %v", err)
	}

	pathSize, inodeSize := store.CacheStats()
	if pathSize != n || inodeSize != n {
		t.Fatalf("after chunked BulkInsert: pathCache=%d inodeCache=%d, want %d/%d",
			pathSize, inodeSize, n, n)
	}
	for _, e := range entries {
		if got := store.LookupByPath(e.Path); got == nil || got.Inode != e.Inode {
			t.Fatalf("LookupByPath(%q) missing/wrong after chunked insert", e.Path)
		}
		if got := store.LookupByInode(e.Inode); got == nil || got.Path != e.Path {
			t.Fatalf("LookupByInode(%d) missing/wrong after chunked insert", e.Inode)
		}
	}
	// childrenIdx must list every child under the parent dir.
	kids, err := store.ListChildren("movies")
	if err != nil {
		t.Fatalf("ListChildren(movies): %v", err)
	}
	if len(kids) != n {
		t.Fatalf("ListChildren(movies)=%d, want %d", len(kids), n)
	}
}

// TestDeletePathsChunkedCorrectness proves the symmetric chunked removal path:
// after a chunked DeletePaths the targeted entries are gone from both caches
// and childrenIdx, and untouched entries remain.
func TestDeletePathsChunkedCorrectness(t *testing.T) {
	withChunk(t, 64)

	store, err := OpenWithMaxCacheSize(":memory:", DefaultMaxCacheSize)
	if err != nil {
		t.Fatalf("OpenWithMaxCacheSize: %v", err)
	}
	defer store.Close()

	const n = 1000
	entries := mkEntries("clips", n)
	if err := store.BulkInsert(entries, 500); err != nil {
		t.Fatalf("BulkInsert: %v", err)
	}

	// Delete the first 600 paths across many chunks; keep the last 400.
	const del = 600
	paths := make([]string, del)
	for i := 0; i < del; i++ {
		paths[i] = entries[i].Path
	}
	if err := store.DeletePaths(paths); err != nil {
		t.Fatalf("DeletePaths: %v", err)
	}

	for i := 0; i < del; i++ {
		if store.LookupByPath(entries[i].Path) != nil {
			t.Fatalf("entry %q should be gone after chunked DeletePaths", entries[i].Path)
		}
		if store.LookupByInode(entries[i].Inode) != nil {
			t.Fatalf("inode %d should be gone after chunked DeletePaths", entries[i].Inode)
		}
	}
	for i := del; i < n; i++ {
		if store.LookupByPath(entries[i].Path) == nil {
			t.Fatalf("entry %q (kept) should survive chunked DeletePaths", entries[i].Path)
		}
	}
	kids, err := store.ListChildren("clips")
	if err != nil {
		t.Fatalf("ListChildren(clips): %v", err)
	}
	if len(kids) != n-del {
		t.Fatalf("ListChildren(clips)=%d, want %d", len(kids), n-del)
	}
}

// TestChunkedBulkInsertYieldsLockToReaders is the load-bearing regression for
// the drain-latency tail fix: while a large BulkInsert applies its cache delta,
// a concurrent serve-path reader (LookupByPath under s.mu.RLock) must make
// progress — it must NOT be parked for the entire delta. With chunking the
// reader interleaves between chunks and completes many lookups before the
// writer finishes. We assert a non-trivial number of reads land DURING the
// write window. (Pre-fix, the single long s.mu.Lock would let through ~0–1.)
func TestChunkedBulkInsertYieldsLockToReaders(t *testing.T) {
	withChunk(t, 16) // many tiny chunks => many yield points

	store, err := OpenWithMaxCacheSize(":memory:", DefaultMaxCacheSize)
	if err != nil {
		t.Fatalf("OpenWithMaxCacheSize: %v", err)
	}
	defer store.Close()

	// Seed one cached entry the reader will hit (a "cached file" the drain
	// gate stats while the reconcile applies its delta). Disjoint inode
	// namespace from the delta so it isn't re-homed by evictInodeOrphanLocked.
	seed := mkEntriesBase("warm", 1, 9_000_000)
	if err := store.BulkInsert(seed, 500); err != nil {
		t.Fatalf("seed BulkInsert: %v", err)
	}
	warmPath := seed[0].Path

	// A large delta whose per-chunk lock holds are short, with enough total
	// chunks (n/16) that a reader spinning in parallel can slip in repeatedly.
	const n = 5000
	delta := mkEntriesBase("reconcile", n, 2_000_000)

	var reads int64
	writeDone := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-writeDone:
				return
			default:
				if store.LookupByPath(warmPath) != nil {
					atomic.AddInt64(&reads, 1)
				}
			}
		}
	}()

	if err := store.BulkInsert(delta, 500); err != nil {
		t.Fatalf("delta BulkInsert: %v", err)
	}
	close(writeDone)
	wg.Wait()

	got := atomic.LoadInt64(&reads)
	if got < 10 {
		t.Fatalf("reader made only %d cache reads during chunked BulkInsert; "+
			"expected the write lock to be yielded between chunks (got starved)", got)
	}
	t.Logf("reader completed %d cache reads concurrently with a %d-entry chunked BulkInsert", got, n)

	// Sanity: the delta fully applied.
	if ps, _ := store.CacheStats(); ps != n+1 {
		t.Fatalf("pathCache=%d after delta, want %d", ps, n+1)
	}
}
