package nfs

import (
	"sync/atomic"

	"github.com/lelanddutcher/juicemount/internal/metrics"
)

// FD AND BUFFER OBSERVABILITY (L5, 2026-08-06).
//
// THE GAP. `FDPool.Stats()` (fdpool.go) and `MemoryBuffer.Stats()` (membuf.go)
// both existed and both worked — and NOTHING CALLED EITHER. The consumer side
// was built too: health.MemoryStats carries FDPoolOpen/FDPoolActive and
// health/monitor.go:425 copies them into the reported status. The only missing
// link was a caller for SetStatsProvider, which had none anywhere in the repo.
// So the whole chain was inert and the process ran with ZERO file-descriptor
// visibility.
//
// That matters here specifically: the 2026-07-28 audit measured ~1 orphaned fd
// per invalidation under a rename storm (held for fdOrphanGrace), i.e. a real
// fd-exhaustion path — and there is no Setrlimit anywhere in the repo either. A
// leak would present as an unexplained process abort with no gauge to point at.
//
// Registered through the metrics provider hook rather than wired at a call site,
// for the same reason as the FUSE data gate (fusedatagate.go): the defect being
// fixed IS a forgotten wiring call, so the fix must not be one. A provider costs
// one closure per /metrics scrape and nothing on any hot path.
//
// The pool is per-handler rather than a package singleton, so the live one is
// published here at handler construction. Reads are lock-free; a nil pointer
// (no handler yet — the metrics endpoint answers before the mount comes up)
// reports absent rather than zero, so "not running" is never misread as
// "running with an empty pool".
var currentFDStats atomic.Pointer[fdStatsSource]

type fdStatsSource struct {
	pool   *FDPool
	memBuf *MemoryBuffer
}

// publishFDStatsSource makes a handler's pool and buffer visible to /metrics.
// Called from the handler constructor; last one wins, which is correct — the
// process serves one mount at a time and a remount should report the new pool.
func publishFDStatsSource(pool *FDPool, memBuf *MemoryBuffer) {
	currentFDStats.Store(&fdStatsSource{pool: pool, memBuf: memBuf})
}

// LiveFDStats reports the current pool and buffer occupancy for consumers
// outside this package, ok=false when no mount is up.
//
// Exists because health.MemoryStats needs the same numbers /metrics reports, and
// health cannot import nfs. Returning ok rather than zeros keeps "no mount" and
// "empty pool" distinguishable at every consumer, not just in the JSON.
func LiveFDStats() (open, active, memBufEntries int, memBufMB float64, ok bool) {
	src := currentFDStats.Load()
	if src == nil || src.pool == nil {
		return 0, 0, 0, 0, false
	}
	open, active = src.pool.Stats()
	if src.memBuf != nil {
		memBufEntries, memBufMB, _, _, _ = src.memBuf.Stats()
	}
	return open, active, memBufEntries, memBufMB, true
}

func init() {
	metrics.Default().SetFDPoolProvider(func() *metrics.FDPoolSnapshot {
		src := currentFDStats.Load()
		if src == nil || src.pool == nil {
			return nil // absent, not zero — see the note above
		}
		open, active := src.pool.Stats()
		snap := &metrics.FDPoolSnapshot{Open: open, Active: active}
		if src.memBuf != nil {
			buffered, totalMB, hits, misses, evicts := src.memBuf.Stats()
			snap.MemBufEntries = buffered
			snap.MemBufMB = totalMB
			snap.MemBufHits = hits
			snap.MemBufMisses = misses
			snap.MemBufEvicts = evicts
		}
		return snap
	})
}
