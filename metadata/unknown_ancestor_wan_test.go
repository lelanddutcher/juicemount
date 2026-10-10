package metadata

import "testing"

// The farm mints a new derivative directory per asset under .juicemount/…, a
// namespace the mirror deliberately never holds (#78). Each is a brand-new
// unknown inode, so the once-per-inode dedup cannot help: observed on a real
// cellular link 2026-07-29, NINE promoted full SCANs in twelve hours, every one
// a different inode. That is the user-reported "index rebuilding for seemingly
// no reason", and a full-tree SCAN over a phone tunnel is the most expensive
// thing this client can do — for an ancestor the SCAN provably cannot establish.
//
// On a far link the promotion must be skipped and left to the backstop.
func TestUnknownAncestor_NoPromotedScanOnHighLatency(t *testing.T) {
	t.Setenv("JM_UNKNOWN_ANCESTOR_SCAN_ON_WAN", "")
	t.Setenv("JM_NET_LATENCY_CEILING", "")

	driveNetprofileHighLatency(t)

	rc := &RedisClient{}
	var triggered int
	rc.testTriggerSync = func() { triggered++ }

	rc.noteUnknownAncestor(4242)

	if triggered != 0 {
		t.Errorf("promoted %d full SCAN(s) on a high-latency link — this is the "+
			"user-visible 'index rebuilding' and it cannot establish the filtered "+
			"ancestor that triggered it", triggered)
	}
}

// A tunnel can be physically cellular even when the VPN endpoint is nearby.
// This was the release-candidate failure: startup RTT measured 5 ms through
// Tailscale, the profile chose Medium, and the promoted SCAN saturated the
// hotspot. Interface class must independently protect that route.
func TestUnknownAncestor_NoPromotedScanOnLowLatencyTunnel(t *testing.T) {
	t.Setenv("JM_UNKNOWN_ANCESTOR_SCAN_ON_WAN", "")
	t.Setenv("JM_NET_LATENCY_CEILING", "")
	settleNetprofileLAN(t)
	SetClassSignals(func() string { return "utun6" }, func() bool { return true })
	t.Cleanup(func() { SetClassSignals(nil, nil) })

	rc := &RedisClient{}
	var triggered int
	rc.testTriggerSync = func() { triggered++ }
	rc.noteUnknownAncestor(5252)

	if triggered != 0 {
		t.Errorf("promoted %d full SCAN(s) on a low-RTT tunnel; tunnel may be a billed cellular underlay", triggered)
	}
}

// On a LAN the promotion is cheap and must still happen, or a genuinely
// out-of-order user event would never be reconciled until the backstop.
func TestUnknownAncestor_StillPromotesOnLAN(t *testing.T) {
	t.Setenv("JM_UNKNOWN_ANCESTOR_SCAN_ON_WAN", "")
	t.Setenv("JM_NET_LATENCY_CEILING", "")

	settleNetprofileLAN(t)
	SetClassSignals(func() string { return "en21" }, func() bool { return true })
	t.Cleanup(func() { SetClassSignals(nil, nil) })

	rc := &RedisClient{}
	var triggered int
	rc.testTriggerSync = func() { triggered++ }

	rc.noteUnknownAncestor(9999)

	if triggered != 1 {
		t.Errorf("promoted %d SCANs on a LAN, want 1 — the LAN path must be unchanged", triggered)
	}
}
