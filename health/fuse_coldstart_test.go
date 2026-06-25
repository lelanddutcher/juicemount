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
