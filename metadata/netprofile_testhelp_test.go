package metadata

import (
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// netprofile.Default() is a PROCESS SINGLETON shared by every test in this
// package, and several behaviours are now latency-gated (the unknown-ancestor
// SCAN promotion, the boot gap-fill deferral). A test that drives the link
// high-latency and does not put it back silently changes the meaning of every
// test that runs after it — which is exactly how TestUnknownAncestorPromotion-
// Limiter started failing only when run alongside a newer test.
//
// Both helpers register a Cleanup that settles the link back to LAN.

func settleNetprofileLAN(t *testing.T) {
	t.Helper()
	np := netprofile.Default()
	for i := 0; i < 60; i++ {
		np.ObserveRTT(300 * time.Microsecond)
	}
	if np.HighLatency() {
		t.Fatal("could not settle netprofile to LAN")
	}
}

func driveNetprofileHighLatency(t *testing.T) {
	t.Helper()
	np := netprofile.Default()
	for i := 0; i < 6; i++ {
		np.ObserveRTT(300 * time.Millisecond)
	}
	if !np.HighLatency() {
		t.Fatal("could not drive netprofile to high-latency")
	}
	t.Cleanup(func() { settleNetprofileLAN(t) })
}
