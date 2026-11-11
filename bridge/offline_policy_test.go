package main

import "testing"

// TestBackendFailureBudgetIsConfirmedPair pins the founder-facing outage
// contract. One failed probe is not enough: on a lossy WAN a single lost SYN
// or PING failed the probe and flipped the volume offline (ENXIO to apps).
// Two consecutive failures are required; health.Reachability runs the second
// probe after a short confirm delay, so real outage detection stays near one
// interval plus two probe timeouts. More than two would push LAN detection
// back toward the multi-second Finder stalls an extra timer once caused.
func TestBackendFailureBudgetIsConfirmedPair(t *testing.T) {
	if backendFailuresToOffline != 2 {
		t.Fatalf("backendFailuresToOffline=%d, want 2 (one failure plus one fast confirm)", backendFailuresToOffline)
	}
}
