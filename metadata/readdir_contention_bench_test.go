package metadata

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// BenchmarkListChildrenBigDir_Quiet measures the readdir listing cost for a
// 5000-entry flat dir with NO concurrent write load — the "quiet" baseline.
func BenchmarkListChildrenBigDir_Quiet(b *testing.B) {
	s := benchStoreWithBigDir(b, 5000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.ListChildren("bigdir"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkListChildrenBigDir_UnderWriteLoad measures the SAME listing while N
// background goroutines hammer BulkInsert to OTHER dirs — reproducing #95 (a
// listing of an unrelated dir stalling while a copy/reconcile writes the store).
// Compare mean + reported max against the quiet baseline to see the contention.
func BenchmarkListChildrenBigDir_UnderWriteLoad(b *testing.B) {
	s := benchStoreWithBigDir(b, 5000)
	now := time.Now()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			base := 100000 + wid*1000000
			for {
				select {
				case <-stop:
					return
				default:
				}
				d := make([]*Entry, 0, 500)
				for i := 0; i < 500; i++ {
					id := base + i
					d = append(d, MakeEntry(fmt.Sprintf("wdir%d/f%08d", wid, id), false, 1, now, uint64(id)))
				}
				_ = s.BulkInsert(d, 500)
				base += 500
			}
		}(w)
	}
	// let the writers get going
	time.Sleep(20 * time.Millisecond)

	var maxNs int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t0 := time.Now()
		if _, err := s.ListChildren("bigdir"); err != nil {
			b.Fatal(err)
		}
		if d := time.Since(t0).Nanoseconds(); d > maxNs {
			maxNs = d
		}
	}
	b.StopTimer()
	close(stop)
	wg.Wait()
	b.ReportMetric(float64(maxNs)/1e6, "max-ms")
}

func benchStoreWithBigDir(b *testing.B, n int) *Store {
	b.Helper()
	s, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = s.Close() })
	now := time.Now()
	if err := s.Insert(MakeEntry("bigdir", true, 0, now, 1)); err != nil {
		b.Fatalf("Insert bigdir: %v", err)
	}
	ents := make([]*Entry, 0, n)
	for i := 0; i < n; i++ {
		ents = append(ents, MakeEntry(fmt.Sprintf("bigdir/f%05d.dat", i), false, 4096, now, uint64(1000+i)))
	}
	if err := s.BulkInsert(ents, 500); err != nil {
		b.Fatalf("BulkInsert bigdir: %v", err)
	}
	return s
}
