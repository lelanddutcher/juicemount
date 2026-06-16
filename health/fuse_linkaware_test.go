package health

import (
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// TestWatchdogThresholdsLinkAware locks in the revert-safety invariant: on
// medium/fast (LAN) the watchdog escalation thresholds are BYTE-FOR-BYTE the
// historical values, and only slow/metered scale up. This is the guard against a
// 10GbE regression — if it ever fails, the LAN watchdog behavior changed.
func TestWatchdogThresholdsLinkAware(t *testing.T) {
	if !fuseWatchdogLinkAware {
		t.Skip("JM_FUSE_WATCHDOG_LINKAWARE=0 set; link-aware scaling disabled")
	}
	p := netprofile.Default()
	defer p.ForceClass(nil)
	fm := &FUSEManager{}

	cases := []struct {
		class      netprofile.LinkClass
		wantTicks  int
		wantConfirm time.Duration
	}{
		{netprofile.ClassMedium, FUSEStaleEscalateTicks, fuseConfirmProbeTimeout},   // == historical
		{netprofile.ClassFast, FUSEStaleEscalateTicks, fuseConfirmProbeTimeout},     // == historical (no LAN change)
		{netprofile.ClassSlow, FUSEStaleEscalateTicks * 2, 45 * time.Second},
		{netprofile.ClassMetered, FUSEStaleEscalateTicks * 3, 90 * time.Second},
	}
	for _, tc := range cases {
		c := tc.class
		p.ForceClass(&c)
		if got := fm.staleEscalateTicks(); got != tc.wantTicks {
			t.Errorf("class=%s staleEscalateTicks=%d, want %d", tc.class, got, tc.wantTicks)
		}
		if got := fm.confirmProbeTimeout(); got != tc.wantConfirm {
			t.Errorf("class=%s confirmProbeTimeout=%v, want %v", tc.class, got, tc.wantConfirm)
		}
	}

	// Medium must equal the package historical constants exactly.
	med := netprofile.ClassMedium
	p.ForceClass(&med)
	if fm.staleEscalateTicks() != FUSEStaleEscalateTicks || fm.confirmProbeTimeout() != fuseConfirmProbeTimeout {
		t.Fatal("medium-class watchdog thresholds drifted from historical constants (LAN regression)")
	}
}
