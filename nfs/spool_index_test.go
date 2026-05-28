package nfs

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSpoolIndexInsertLookupDelete(t *testing.T) {
	idx := NewSpoolIndex()
	e := &SpoolEntry{nfsPath: "/a.mov"}

	if _, ok := idx.Lookup("/a.mov"); ok {
		t.Fatalf("expected miss on empty index")
	}

	idx.Insert("/a.mov", e)
	got, ok := idx.Lookup("/a.mov")
	if !ok || got != e {
		t.Fatalf("expected hit returning inserted entry")
	}
	if n := idx.Len(); n != 1 {
		t.Errorf("len=%d, want 1", n)
	}

	idx.Delete("/a.mov")
	if _, ok := idx.Lookup("/a.mov"); ok {
		t.Fatalf("expected miss after delete")
	}
}

func TestSpoolIndexInsertReplaces(t *testing.T) {
	idx := NewSpoolIndex()
	e1 := &SpoolEntry{nfsPath: "/x"}
	e2 := &SpoolEntry{nfsPath: "/x"}
	idx.Insert("/x", e1)
	idx.Insert("/x", e2)
	got, _ := idx.Lookup("/x")
	if got != e2 {
		t.Fatalf("expected second Insert to replace first")
	}
}

// TestSpoolIndexConcurrentReadsHotPath is the QA-35-flavored stress test:
// under heavy concurrent Lookups (the read hot path) with occasional
// inserts/deletes, the index stays consistent and the RLock-only reads
// don't deadlock or panic. We also assert that an empty-index Lookup
// stays cheap under pressure.
func TestSpoolIndexConcurrentReadsHotPath(t *testing.T) {
	idx := NewSpoolIndex()

	// Seed with 1000 entries.
	for i := 0; i < 1000; i++ {
		idx.Insert("/file"+strconv.Itoa(i), &SpoolEntry{})
	}

	const readers = 64
	const writers = 4
	const duration = 100 * time.Millisecond

	var stop atomic.Bool
	var hits, misses atomic.Int64
	var wg sync.WaitGroup

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for !stop.Load() {
				path := "/file" + strconv.Itoa(r%2000) // 50% hit rate
				if _, ok := idx.Lookup(path); ok {
					hits.Add(1)
				} else {
					misses.Add(1)
				}
			}
		}(r)
	}
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for !stop.Load() {
				path := "/churn" + strconv.Itoa(w) + "-" + strconv.Itoa(i%50)
				idx.Insert(path, &SpoolEntry{})
				idx.Delete(path)
				i++
			}
		}(w)
	}

	time.Sleep(duration)
	stop.Store(true)
	wg.Wait()

	total := hits.Load() + misses.Load()
	if total < 100 {
		t.Errorf("very low lookup throughput: %d total in %v — index may be deadlocked", total, duration)
	}
}

// BenchmarkSpoolIndexEmptyLookup measures the cost of a Lookup miss on an
// empty index. This is the QA-35 perf gate for slice D: the read path
// must take ~no cost when no writes are active. Should be in low ns.
func BenchmarkSpoolIndexEmptyLookup(b *testing.B) {
	idx := NewSpoolIndex()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Lookup("/not-in-spool/some/long/path.mov")
	}
}

// BenchmarkSpoolIndexHitLookup measures a Lookup hit. Establishes the
// upper bound on read-path tax when there IS an active spool entry for
// the path being read.
func BenchmarkSpoolIndexHitLookup(b *testing.B) {
	idx := NewSpoolIndex()
	idx.Insert("/hot.mov", &SpoolEntry{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Lookup("/hot.mov")
	}
}
