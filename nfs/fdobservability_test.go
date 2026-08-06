package nfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/metrics"
)

// The fd pool must be VISIBLE in /metrics.
//
// FDPool.Stats() and MemoryBuffer.Stats() both existed and worked; health's
// MemoryStats was built to carry the numbers and monitor.go:425 already copied
// them into the reported status. The one missing link was a caller for
// SetStatsProvider, which had none anywhere — so the whole chain was inert and
// the process ran with zero descriptor visibility, despite a measured
// orphaned-fd path under rename storms and no Setrlimit in the repo.
//
// This guards the WIRING. A test of FDPool.Stats() alone would have passed
// happily for every month the gap was live, because the accessor was never the
// broken part.
func TestFDPoolIsReportedInMetrics(t *testing.T) {
	pool := NewFDPool()
	mb := NewMemoryBuffer(1<<20, 1<<24)
	publishFDStatsSource(pool, mb)

	snap := metrics.Default().Snapshot()
	if snap.FDPool == nil {
		t.Fatal("metrics snapshot has no fd_pool section — descriptor occupancy is " +
			"invisible; is the init() provider registration in fdobservability.go still there?")
	}

	// An empty pool must report zero, not absent: absent is reserved for "no
	// mount is up", and conflating the two would hide a leak behind a nil.
	if snap.FDPool.Open != 0 || snap.FDPool.Active != 0 {
		t.Errorf("fresh pool reports open=%d active=%d, want 0/0",
			snap.FDPool.Open, snap.FDPool.Active)
	}

	// Now hold a real descriptor and confirm the gauge MOVES. A snapshot that
	// always reported zero would satisfy the nil-check above while telling us
	// nothing — which is the failure this whole change exists to prevent.
	dir := t.TempDir()
	path := filepath.Join(dir, "held.bin")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Get(path); err != nil {
		t.Skipf("pool.Get failed in this environment: %v", err)
	}
	defer pool.Release(path)

	live := metrics.Default().Snapshot()
	if live.FDPool.Open == 0 {
		t.Error("held a descriptor and fd_pool.open is still 0 — the gauge is " +
			"constant, so it cannot show an fd leak")
	}
	if live.FDPool.Active == 0 {
		t.Error("descriptor is referenced but fd_pool.active is 0 — open-vs-active " +
			"is the orphan-leak signature (open climbing, active flat) and it " +
			"needs both halves to be real")
	}
}

// No mount yet must report ABSENT, never zero. "Nothing is running" and
// "running with an empty pool" are different facts, and an operator reading a
// flat zero during an fd investigation would draw the wrong conclusion.
func TestFDPoolReportsAbsentWhenNoHandlerExists(t *testing.T) {
	saved := currentFDStats.Load()
	currentFDStats.Store(nil)
	defer currentFDStats.Store(saved)

	if snap := metrics.Default().Snapshot(); snap.FDPool != nil {
		t.Errorf("no handler published, yet fd_pool reported %+v — absent and "+
			"empty must stay distinguishable", snap.FDPool)
	}
}
