package nfs

// Audit P0: opportunistic warmers must not draw from the FOREGROUND budget.
//
// The `._` sidecar warmer runs up to 48-way and the thumbnail hydrator runs
// alongside it; both called openFileWithTimeout, which drew from nfsLstatGate —
// the 24-slot gate an NFS RPC blocks on. On a LAN each open returns in ~1ms so
// the warmers drain faster than anyone notices. At cellular RTT each open costs
// a full round trip, so warmers could hold every foreground slot for seconds and
// a user navigating at that moment queued BEHIND work nobody was waiting for.
//
// If these tests fail, navigation latency is once again coupled to warm volume.

import (
	"os"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/metrics"
)

// TestWarmSourcesDoNotUseForegroundGate is the P0 assertion.
func TestWarmSourcesDoNotUseForegroundGate(t *testing.T) {
	for _, src := range []metrics.FUSESource{
		metrics.FUSESrcSidecarWarm,
		metrics.FUSESrcThumbWarm,
	} {
		gate, label := fuseGateForSource(src)
		if gate == nfsLstatGate {
			t.Errorf("%v draws from nfsLstatGate — opportunistic warming can starve foreground navigation", src)
		}
		if gate != warmGate {
			t.Errorf("%v routed to an unexpected gate", src)
		}
		if label != metrics.FUSEGateWarm {
			t.Errorf("%v labelled %v, want FUSEGateWarm — /metrics would misattribute the budget", src, label)
		}
	}
}

// Foreground must KEEP the foreground gate. The split is only useful if the
// thing being protected still has its own budget.
func TestForegroundKeepsForegroundGate(t *testing.T) {
	gate, label := fuseGateForSource(metrics.FUSESrcForeground)
	if gate != nfsLstatGate {
		t.Fatal("foreground no longer draws from nfsLstatGate — the hot path lost its dedicated budget")
	}
	if label != metrics.FUSEGateNFSLstat {
		t.Fatalf("foreground labelled %v, want FUSEGateNFSLstat", label)
	}
}

// Unclassified sources fall back to the foreground gate. Conservative on
// purpose: an unlabelled caller is more likely to be latency-sensitive than
// opportunistic, and mis-routing real work to a small background gate would be
// a latency regression rather than a missed optimisation.
func TestUnknownSourceFallsBackToForeground(t *testing.T) {
	for _, src := range []metrics.FUSESource{
		metrics.FUSESrcPhantomPurge,
		metrics.FUSESrcOther,
	} {
		if gate, _ := fuseGateForSource(src); gate != nfsLstatGate {
			t.Errorf("%v did not fall back to the foreground gate", src)
		}
	}
}

// The warm gate must be genuinely separate capacity, not an alias, or warmers
// and foreground would still contend for the same slots.
func TestWarmGateIsSeparateCapacity(t *testing.T) {
	if warmGate == nfsLstatGate {
		t.Fatal("warmGate IS nfsLstatGate — the split is a no-op")
	}
	if cap(warmGate) <= 0 {
		t.Fatalf("warmGate cap = %d; a zero/negative cap would deadlock every warm", cap(warmGate))
	}
	// Throttled, not strangled: warming is what makes the NEXT navigation cheap.
	if cap(warmGate) >= cap(nfsLstatGate) {
		t.Errorf("warmGate cap %d >= foreground cap %d — background work must not be able to "+
			"claim as much concurrency as the hot path", cap(warmGate), cap(nfsLstatGate))
	}
}

func TestWarmGateCapOverride(t *testing.T) {
	t.Setenv("JM_WARM_GATE", "3")
	if got := warmGateCap(); got != 3 {
		t.Errorf("JM_WARM_GATE=3 gave cap %d", got)
	}
	t.Setenv("JM_WARM_GATE", "")
	if got := warmGateCap(); got != 8 {
		t.Errorf("default warm gate cap = %d, want 8", got)
	}
	// Garbage must not silently produce a 0-cap (deadlock) gate.
	t.Setenv("JM_WARM_GATE", "banana")
	if got := warmGateCap(); got != 8 {
		t.Errorf("unparseable JM_WARM_GATE gave cap %d, want the 8 default", got)
	}
	t.Setenv("JM_WARM_GATE", "-4")
	if got := warmGateCap(); got != 8 {
		t.Errorf("negative JM_WARM_GATE gave cap %d, want the 8 default", got)
	}
	_ = os.Unsetenv("JM_WARM_GATE")
}
