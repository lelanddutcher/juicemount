package main

import "testing"

// TestBackendFailureBudgetIsConfirmedPair pins the founder-facing outage
// contract. One failed probe is not enough: on a lossy WAN a single lost SYN
// or PING failed the probe and flipped the volume offline (ENXIO to apps).
// Two consecutive failures are required; health.Reachability runs the second
// probe after a short confirm delay. Real outage detection on a LAN is 4-8 s:
// about 4.3 s with no recent traffic, about 8.3 s in normal use, when recent
// Redis replies hold off failures for the 4 s liveness window. More than two
// would push LAN detection toward the ~22 s Finder stall an extra timer once
// caused.
func TestBackendFailureBudgetIsConfirmedPair(t *testing.T) {
	if backendFailuresToOffline != 2 {
		t.Fatalf("backendFailuresToOffline=%d, want 2 (one failure plus one fast confirm)", backendFailuresToOffline)
	}
}
