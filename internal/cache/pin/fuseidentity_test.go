package pin

import (
	"os"
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
