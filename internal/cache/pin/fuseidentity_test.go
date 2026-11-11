package pin

import (
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A plain temp directory shares its parent's fsid → the gate must FAIL it.
// This is the exact 2026-07-01 incident shape: kext approval lost, juicefs
// mount absent, mountpoint silently a local dir absorbing drains.
func TestFUSEIdentityPlainDirFails(t *testing.T) {
	t.Cleanup(ResetFUSEIdentityForTest)
	dir := t.TempDir()
	SetFUSEIdentityPath(dir)
	ok, reason := FUSEIdentityState()
	if ok {
		t.Fatalf("plain directory passed the identity gate (reason=%q) — this is the misdirected-drain data-loss hole", reason)
	}
}

// /dev is a devfs mount on every macOS system — a real mounted filesystem
// whose fsid differs from its parent /. The gate must PASS it. (We can't
// mount a FUSE fs in a unit test; any real mountpoint proves the fsid
// comparison. Skipped on non-darwin.)
func TestFUSEIdentityRealMountPasses(t *testing.T) {
	t.Cleanup(ResetFUSEIdentityForTest)
	if _, err := os.Stat("/dev/null"); err != nil {
		t.Skip("no /dev — not a darwin-like system")
	}
	SetFUSEIdentityPath("/dev")
	ok, reason := FUSEIdentityState()
	if !ok {
		t.Fatalf("real mountpoint /dev failed the identity gate: %s", reason)
	}
}

func TestFUSEIdentityUnconfiguredIsInert(t *testing.T) {
	t.Cleanup(ResetFUSEIdentityForTest)
	ResetFUSEIdentityForTest()
	if ok, reason := FUSEIdentityState(); !ok {
		t.Fatalf("unconfigured gate must be inert (got %s)", reason)
	}
}

func TestFUSEIdentityMissingPathFails(t *testing.T) {
	t.Cleanup(ResetFUSEIdentityForTest)
	SetFUSEIdentityPath("/nonexistent/jm-fuse-identity-test")
	if ok, _ := FUSEIdentityState(); ok {
		t.Fatal("missing mountpoint passed the identity gate")
	}
}

func TestFUSEIdentityKillSwitch(t *testing.T) {
	t.Cleanup(ResetFUSEIdentityForTest)
	dir := t.TempDir()
	SetFUSEIdentityPath(dir)
	t.Setenv("JM_FUSE_IDENTITY_GATE", "0")
	if ok, _ := FUSEIdentityState(); !ok {
		t.Fatal("kill switch JM_FUSE_IDENTITY_GATE=0 did not disable the gate")
	}
}

// The cache must serve within TTL (no per-call statfs) and invalidate on
// SetFUSEIdentityPath.
func TestFUSEIdentityCacheAndInvalidate(t *testing.T) {
	t.Cleanup(ResetFUSEIdentityForTest)
	dir := t.TempDir()
	SetFUSEIdentityPath(dir)
	ok1, _ := FUSEIdentityState()
	start := time.Now()
	ok2, _ := FUSEIdentityState() // cached — must be fast
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("cached identity read took %v", elapsed)
	}
	if ok1 != ok2 {
		t.Fatalf("cached result diverged: %v vs %v", ok1, ok2)
	}
	SetFUSEIdentityPath("/dev")
	if ok3, reason := FUSEIdentityState(); !ok3 {
		t.Fatalf("cache not invalidated on SetFUSEIdentityPath: %s", reason)
	}
}

// A probe whose statfs returned has freed the single-flight slot by the time
// its caller has the answer, so the next consult runs its own probe instead
// of reading "already in flight" (a wedge). Under -race the old order (slot
// freed after the result was sent) failed a few dozen of 2000 consults.
func TestFUSEIdentitySequentialProbesNotInFlight(t *testing.T) {
	t.Cleanup(ResetFUSEIdentityForTest)
	SetFUSEIdentityPath(t.TempDir())
	for i := 0; i < 2000; i++ {
		if _, reason := FUSEIdentityFresh(); strings.Contains(reason, "in flight") {
			t.Fatalf("consult %d: %s, though every earlier probe had returned", i, reason)
		}
	}
}

func withIdentityProbe(t *testing.T, probe func(string) (bool, string), statfsTimeout, reprobe time.Duration) {
	t.Helper()
	identityProbeMu.Lock()
	oldProbe, oldTimeout, oldReprobe := statfsIdentityProbe, fuseIdentityStatfsTimeout, fuseIdentityReprobeAfter
	statfsIdentityProbe, fuseIdentityStatfsTimeout, fuseIdentityReprobeAfter = probe, statfsTimeout, reprobe
	identityProbeMu.Unlock()
	t.Cleanup(func() {
		// Released probes finish before the next test starts.
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			identityProbeMu.Lock()
			n := len(identityProbes)
			identityProbeMu.Unlock()
			if n == 0 {
				break
			}
		}
		identityProbeMu.Lock()
		statfsIdentityProbe, fuseIdentityStatfsTimeout, fuseIdentityReprobeAfter = oldProbe, oldTimeout, oldReprobe
		identityProbeMu.Unlock()
		ResetFUSEIdentityForTest()
	})
	SetFUSEIdentityPath(t.TempDir())
}

// A consult during a slow (not stuck) statfs shares its answer instead of
// reading "already in flight" as a wedge.
func TestFUSEIdentityConcurrentConsultsShareOneProbe(t *testing.T) {
	var calls atomic.Int32
	withIdentityProbe(t, func(string) (bool, string) {
		calls.Add(1)
		time.Sleep(200 * time.Millisecond) // two round trips on a distant link
		return true, "ok"
	}, 3*time.Second, 30*time.Second)
	var wg sync.WaitGroup
	var bad atomic.Int32
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _ := FUSEIdentityFresh(); !ok {
				bad.Add(1)
			}
		}()
	}
	wg.Wait()
	if bad.Load() != 0 || calls.Load() != 1 {
		t.Fatalf("%d consults failed, %d probes; want 0 and 1", bad.Load(), calls.Load())
	}
}

// One statfs that never returns must not pause the gate for good: after
// fuseIdentityReprobeAfter a new probe runs, and stuck probes are capped.
func TestFUSEIdentityRecoversFromAStuckProbe(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	var stuck atomic.Bool
	stuck.Store(true)
	withIdentityProbe(t, func(string) (bool, string) {
		if stuck.Load() {
			<-release
			return false, "late"
		}
		return true, "ok"
	}, 50*time.Millisecond, 100*time.Millisecond)

	if ok, reason := FUSEIdentityFresh(); ok || !strings.Contains(reason, "timed out") {
		t.Fatalf("stuck probe: %v %q", ok, reason)
	}
	stuck.Store(false)
	time.Sleep(150 * time.Millisecond)
	if ok, reason := FUSEIdentityFresh(); !ok {
		t.Fatalf("no recovery after a stuck probe: %q", reason)
	}

	// Cap: with fuseIdentityMaxStuck probes stuck, consults fail fast.
	stuck.Store(true)
	for i := 1; i < fuseIdentityMaxStuck; i++ {
		time.Sleep(150 * time.Millisecond)
		_, _ = FUSEIdentityFresh()
	}
	time.Sleep(150 * time.Millisecond)
	start := time.Now()
	ok, reason := FUSEIdentityFresh()
	if ok || !strings.Contains(reason, "not answering") || time.Since(start) > 40*time.Millisecond {
		t.Fatalf("at the cap: %v %q after %v", ok, reason, time.Since(start))
	}
}
