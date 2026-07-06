package metadata

import (
	"fmt"
	"testing"
	"time"
)

// benchSeedBigDir builds a store with a single directory holding n children,
// modeling the 10,774-child DCIM dir that vetoed a naive full-scan port
// (serving-layer-decision.md §1: 12.7ms p99 / 6.26MB / 215,517 allocs as one
// whole-dir scan+copy). The benchmarks below prove ListChildrenPage keeps a
// single page well under the RPC budget with low allocs.
func benchSeedBigDir(b *testing.B, n int) *Store {
	b.Helper()
	s, err := Open(":memory:")
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	now := time.Unix(1_700_000_000, 0)
	if err := s.Insert(MakeEntry("DCIM", true, 0, now, 1)); err != nil {
		b.Fatalf("insert parent: %v", err)
	}
	ents := make([]*Entry, 0, n)
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("DCIM/IMG_%08d.CR3", i)
		ents = append(ents, MakeEntry(p, false, int64(i*4096), now, uint64(1000+i)))
	}
	if err := s.BulkInsert(ents, 5000); err != nil {
		b.Fatalf("bulk insert: %v", err)
	}
	return s
}

// BenchmarkListChildrenPage_BigDir measures ONE READDIR page against a
// 10,774-child dir. A real NFS READDIR RPC carries ~128-256 entries per page
// (bounded by obj.Count / HandleLimit in nfs_onreaddir.go), so 256 models the
// per-RPC page — this is the veto-mitigation proof: page latency must be well
// under the RPC budget (target < 1ms/page). The composite idx_parent_name index
// is what makes this an O(limit) index seek instead of an O(all-children) temp
// b-tree sort (which was ~2.2ms/page regardless of LIMIT — see the schema
// comment). Run:
//
//	go test ./metadata/ -run=^$ -bench=BenchmarkListChildrenPage_BigDir -benchmem
func BenchmarkListChildrenPage_BigDir(b *testing.B) {
	const children = 10774
	const pageSize = 256 // realistic per-RPC READDIR page
	s := benchSeedBigDir(b, children)
	defer s.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// A representative middle page (past the first, so the cursor path is
		// exercised, not just LIMIT-from-start).
		_, _, err := s.ListChildrenPage("DCIM", "IMG_00005000.CR3", pageSize)
		if err != nil {
			b.Fatalf("page: %v", err)
		}
	}
}

// BenchmarkListChildrenPage_FirstPage measures the first page (empty cursor),
// which is what a fresh Finder open hits first.
func BenchmarkListChildrenPage_FirstPage(b *testing.B) {
	const children = 10774
	const pageSize = 256
	s := benchSeedBigDir(b, children)
	defer s.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := s.ListChildrenPage("DCIM", "", pageSize)
		if err != nil {
			b.Fatalf("page: %v", err)
		}
	}
}

// BenchmarkListChildrenPage_FullPage measures a large 1000-row page for the
// whole-dir-build context (listChildrenSQLite pages at 1000). A single 1000-row
// page is ~1ms; the protocol never requests one this large per RPC.
func BenchmarkListChildrenPage_FullPage(b *testing.B) {
	const children = 10774
	const pageSize = 1000
	s := benchSeedBigDir(b, children)
	defer s.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := s.ListChildrenPage("DCIM", "IMG_00005000.CR3", pageSize)
		if err != nil {
			b.Fatalf("page: %v", err)
		}
	}
}

// BenchmarkListChildren_BigDir_RAMvsSQLite contrasts the whole-dir build cost:
// the RAM map copy vs the internally-paged SQLite build, for context on the
// design's idle-latency numbers (RAM big-dir ListChildren ~0.6-20µs; SQLite
// whole-dir is a sum of pages). The single-PAGE benchmark above is the one that
// matters for the protocol hot path.
func BenchmarkListChildren_BigDir_RAM(b *testing.B) {
	const children = 10774
	s := benchSeedBigDir(b, children)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.listChildrenRAM("DCIM"); err != nil {
			b.Fatalf("ram: %v", err)
		}
	}
}

func BenchmarkListChildren_BigDir_SQLite(b *testing.B) {
	const children = 10774
	s := benchSeedBigDir(b, children)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.listChildrenSQLite("DCIM"); err != nil {
			b.Fatalf("sqlite: %v", err)
		}
	}
}

// BenchmarkLookupByPath_RAM is the RAM-side point-lookup baseline (the
// LOOKUP/GETATTR hot path). The SQLite counterpart is in the Item 2 accessors
// bench file (BenchmarkLookupByPath_SQLite) — the design's "idle point lookup
// 85ns RAM -> ~7µs SQLite" claim.
func BenchmarkLookupByPath_RAM(b *testing.B) {
	s := benchSeedBigDir(b, 10774)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.lookupByPathRAM("DCIM/IMG_00005000.CR3")
	}
}
