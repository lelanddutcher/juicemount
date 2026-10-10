package metadata

import "testing"

// BenchmarkLookupByPath_SQLite is the SQLite-side point-lookup (Item 2), the
// counterpart to BenchmarkLookupByPath_RAM. The SQLite path remains materially
// slower than the RAM snapshot path; its win is under a concurrent writer (WAL
// readers snapshot around the writer), not idle latency.
// benchSeedBigDir lives in serve_sqlite_bench_test.go (Item 1).
func BenchmarkLookupByPath_SQLite(b *testing.B) {
	s := benchSeedBigDir(b, 10774)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.lookupByPathSQLite("DCIM/IMG_00005000.CR3")
	}
}
