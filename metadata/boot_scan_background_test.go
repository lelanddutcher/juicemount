package metadata

import "testing"

// The freshness skip is a 15-MINUTE window (bootScanFreshWindow). A laptop that
// sleeps overnight blows it every morning and then pays a SYNCHRONOUS full-tree
// SCAN over whatever link it wakes on — measured live at 297k entries / 163s on
// a tunnel. That is the user-visible "Rebuilding index" and a metered-link burn
// for a mirror that is, in practice, almost entirely still correct.
//
// On a far link the SCAN must run BEHIND the recalled mirror, not in front of
// it. On a LAN it is cheap and stays synchronous.
func TestBootGapFill_DeferredOnSlowLink(t *testing.T) {
	t.Setenv("JM_BOOT_SCAN_BACKGROUND", "")
	t.Setenv("JM_NET_LATENCY_CEILING", "")

	driveNetprofileHighLatency(t)

	rc := &RedisClient{}
	if !rc.deferBootGapFillToBackground() {
		t.Error("boot gap-fill still synchronous on a far link — this is the " +
			"overnight-sleep 'Rebuilding index' the user is blocked on")
	}
}

// LAN behaviour must be untouched: there the SCAN is seconds, and running it
// up front is the strictly more correct choice.
func TestBootGapFill_SynchronousOnLAN(t *testing.T) {
	t.Setenv("JM_BOOT_SCAN_BACKGROUND", "")
	t.Setenv("JM_NET_LATENCY_CEILING", "")

	settleNetprofileLAN(t)

	rc := &RedisClient{}
	if rc.deferBootGapFillToBackground() {
		t.Error("deferred the SCAN on a LAN — it is cheap there and belongs up front")
	}
}

func TestBootGapFill_KillSwitch(t *testing.T) {
	t.Setenv("JM_BOOT_SCAN_BACKGROUND", "0")
	driveNetprofileHighLatency(t)
	rc := &RedisClient{}
	if rc.deferBootGapFillToBackground() {
		t.Error("JM_BOOT_SCAN_BACKGROUND=0 did not restore the synchronous SCAN")
	}
}
