package main

import "testing"

// TestBackendFailureBudgetStaysSingleProbe pins the founder-facing outage
// contract. health.Reachability already performs a protocol-level Redis PING
// and suppresses failures when a recent drain proves the path live; requiring
// another failed probe here reintroduces multi-second Finder/FUSE stalls.
func TestBackendFailureBudgetStaysSingleProbe(t *testing.T) {
	if backendFailuresToOffline != 1 {
		t.Fatalf("backendFailuresToOffline=%d, want 1 bounded protocol-level failure", backendFailuresToOffline)
	}
}
