package metadata

import "testing"

// BenchmarkLookupByPath_SQLite is the SQLite-side point-lookup (Item 2), the
// counterpart to BenchmarkLookupByPath_RAM. Measured ~3.1-5µs vs ~16ns RAM on
// Apple M2 Pro — both sub-RPC-budget; the SQLite path's win is under a
// concurrent writer (WAL readers snapshot around the writer), not idle latency.
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
