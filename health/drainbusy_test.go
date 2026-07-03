package health

import (
	"math"
	"path/filepath"
	"testing"
	"time"
)

// TestBusyIngestingPredicate locks the #89 online busy-suppression predicate:
// busyIngesting() is the shared decision the three stat/readdir TIMEOUT sites
// (checkFUSE stat, checkFUSE readdir, checkNFS stat) consult before flipping to
// degraded. It is the ONLINE analogue of pin.IsOffline() — a legitimate heavy
// ingest (the drain's synchronous MinIO PUTs saturating the FUSE queue) is
// "busy", not "wedged".
//
// Truth table:
//   - drain InFlight > 0                         -> busy (suppress the timeout)
//   - InFlight == 0 but a drain completed within
//     DrainBusyRecentWindow                       -> busy (covers the burst lull)
//   - InFlight == 0 and last success is stale     -> NOT busy (a real wedge still degrades)
//   - no drain probe wired (nil)                  -> NOT busy (pre-drainer no-op)
func TestBusyIngestingPredicate(t *testing.T) {
	cfg := Config{FUSEPath: fusePath(t)}

	// --- no probe wired: never busy (pre-drainer no-op) ---
	m := New(cfg)
	if m.busyIngesting() {
		t.Fatal("no drain probe wired: busyIngesting must be false (pre-drainer no-op)")
	}

	// --- InFlight > 0: busy regardless of last-success age ---
	m.SetDrainProbe(func() (int64, time.Duration) {
		return 3, time.Duration(math.MaxInt64) // 3 in flight, no completed drain yet
	})
	if !m.busyIngesting() {
		t.Fatal("InFlight>0: busyIngesting must be true (active ingest saturating the FUSE queue)")
	}

	// --- InFlight == 0 but a very recent completed drain: busy (burst lull) ---
	m.SetDrainProbe(func() (int64, time.Duration) {
		return 0, DrainBusyRecentWindow / 2 // completed well within the window
	})
	if !m.busyIngesting() {
		t.Fatal("InFlight==0 + recent drain success: busyIngesting must be true (burst lull between PUTs)")
	}

	// --- InFlight == 0 and a STALE last success: NOT busy (a real wedge degrades) ---
	m.SetDrainProbe(func() (int64, time.Duration) {
		return 0, DrainBusyRecentWindow + time.Second // last success outside the window
	})
	if m.busyIngesting() {
		t.Fatal("InFlight==0 + stale last-success: busyIngesting must be FALSE (a genuine wedge must still degrade)")
	}

	// --- SetDrainProbe(nil) restores the no-op ---
	m.SetDrainProbe(nil)
	if m.busyIngesting() {
		t.Fatal("SetDrainProbe(nil): busyIngesting must be false again (no-op restored)")
	}
}

// TestCheckNFSResolvedErrorAlwaysDegrades proves the suppression is TIMEOUT-only:
// a RESOLVED probe error (here: the mount-point directory does not exist, so the
// inner os.Stat returns IsNotExist and the goroutine resolves via `done`) must
// STILL report degraded even while a heavy ingest is in flight. The
// busyIngesting() suppression is wired ONLY into the `<-time.After(...)` timeout
// branch, never the `case st := <-done` resolved branch — so a genuine
// error/ENOTCONN can never be masked as "busy". This guards the exact bug the
// #89 fix must NOT introduce: silencing a real fault during an ingest.
func TestCheckNFSResolvedErrorAlwaysDegrades(t *testing.T) {
	cfg := Config{
		// A path that certainly does not exist -> os.Stat returns IsNotExist,
		// which the inner goroutine resolves as "not mounted" (Healthy:false)
		// via `done` — the FAST, resolved-error path, not the timeout branch.
		NFSMountPoint: filepath.Join(t.TempDir(), "definitely-not-a-mount-point"),
	}
	m := New(cfg)
	// Wire a probe reporting a HEAVY ingest — if the suppression were wrongly
	// applied to the resolved-error branch, this would flip the report healthy.
	m.SetDrainProbe(func() (int64, time.Duration) {
		return 5, 0 // 5 in flight, drain just completed — maximally "busy"
	})

	st := m.checkNFS()
	if st.Healthy {
		t.Fatalf("resolved probe error during heavy ingest must STILL degrade, got Healthy=true msg=%q", st.Message)
	}
}

// TestCheckNFSNotConfiguredIsHealthy is a guard that the busy wiring did not
// disturb the trivial not-configured path (empty NFSMountPoint -> healthy).
func TestCheckNFSNotConfiguredIsHealthy(t *testing.T) {
	m := New(Config{NFSMountPoint: ""})
	m.SetDrainProbe(func() (int64, time.Duration) { return 9, 0 }) // busy, but irrelevant
	st := m.checkNFS()
	if !st.Healthy {
		t.Fatalf("empty NFSMountPoint must be healthy (not configured), got Healthy=false msg=%q", st.Message)
	}
}
