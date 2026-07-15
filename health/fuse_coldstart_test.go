package health

import (
	"testing"
	"time"
)

// TestColdStartGraceFromEnv covers the cold-start escalation-grace knob (the
// 2026-06-25 fix for the post-redeploy macFUSE wedge): a sane env value wins, 0
// disables the grace, and garbage falls back to the 6-minute default.
func TestColdStartGraceFromEnv(t *testing.T) {
	t.Setenv("JM_FUSE_COLDSTART_GRACE_SEC", "120")
	if got := coldStartGraceFromEnv(); got != 120*time.Second {
		t.Errorf("env=120 → %v, want 2m", got)
	}
	t.Setenv("JM_FUSE_COLDSTART_GRACE_SEC", "0")
	if got := coldStartGraceFromEnv(); got != 0 {
		t.Errorf("env=0 must disable the grace, got %v", got)
	}
	t.Setenv("JM_FUSE_COLDSTART_GRACE_SEC", "-5")
	if got := coldStartGraceFromEnv(); got != 6*time.Minute {
		t.Errorf("negative must fall back to default, got %v", got)
	}
	t.Setenv("JM_FUSE_COLDSTART_GRACE_SEC", "garbage")
	if got := coldStartGraceFromEnv(); got != 6*time.Minute {
		t.Errorf("garbage must fall back to default, got %v", got)
	}
}

// TestMountVerifyTimeout locks the 2026-07-15 regression fix (0.4.0 RC, field):
// the launch-verify budget must cover a large-LOCAL-cache cold warm-up (~30s on
// a FAST LAN), not just slow LINKS. The old 15s LAN/fast base false-failed the
// verification and drove the SIGKILL→zombie-mount→remount thrash. Guards against
// reverting the base below 60s, and covers the JM_FUSE_VERIFY_SEC override.
func TestMountVerifyTimeout(t *testing.T) {
	fm := &FUSEManager{}
	// A sane positive env value wins outright, regardless of link class.
	t.Setenv("JM_FUSE_VERIFY_SEC", "42")
	if got := fm.mountVerifyTimeout(); got != 42*time.Second {
		t.Fatalf("env override → %v, want 42s", got)
	}
	// Non-positive / garbage / empty env is ignored → the class base applies,
	// which must be at least 60s on EVERY link class (default 60, slow 90,
	// metered 120) so a slow local warm-up is never false-failed.
	for _, bad := range []string{"0", "-1", "garbage", ""} {
		t.Setenv("JM_FUSE_VERIFY_SEC", bad)
		if got := fm.mountVerifyTimeout(); got < 60*time.Second {
			t.Fatalf("verify timeout with env=%q = %v, want >= 60s (15s was too short for local cache-warm)", bad, got)
		}
	}
}
