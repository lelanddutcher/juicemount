package main

import (
	"testing"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

func TestNetworkCostClass(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		expensive, constrained bool
		forced                 bool
	}{
		{name: "ordinary", forced: false},
		{name: "expensive", expensive: true, forced: true},
		{name: "constrained", constrained: true, forced: true},
		{name: "both", expensive: true, constrained: true, forced: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			class, forced := networkCostClass(tc.expensive, tc.constrained)
			if forced != tc.forced {
				t.Fatalf("forced = %v, want %v", forced, tc.forced)
			}
			if forced && class != netprofile.ClassMetered {
				t.Fatalf("class = %s, want metered", class)
			}
		})
	}
}

func TestHostNetworkCostSnapshotIsSanitizedAndComplete(t *testing.T) {
	hostNetworkCost.Lock()
	previousInitialized := hostNetworkCost.initialized
	previousExpensive := hostNetworkCost.expensive
	previousConstrained := hostNetworkCost.constrained
	hostNetworkCost.initialized = true
	hostNetworkCost.expensive = true
	hostNetworkCost.constrained = false
	hostNetworkCost.Unlock()
	t.Cleanup(func() {
		hostNetworkCost.Lock()
		hostNetworkCost.initialized = previousInitialized
		hostNetworkCost.expensive = previousExpensive
		hostNetworkCost.constrained = previousConstrained
		hostNetworkCost.Unlock()
	})

	observed, expensive, constrained := hostNetworkCostSnapshot()
	if !observed || !expensive || constrained {
		t.Fatalf("snapshot = observed:%v expensive:%v constrained:%v", observed, expensive, constrained)
	}
}
