package nfs

import (
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// W1. Measured 2026-07-29 on a real cellular link, and again OFFLINE:
//
//	sidecar_warm  calls=264  timeouts=258 (98%)  mean=791ms  populated=2
//	foreground    calls=6228 timeouts=10  (0.2%) mean=38.5ms
//
// The warmer was not failing because warming is hopeless. nfs/sidecar.go's own
// field note records "a single `._` read = 777ms" on cellular, and
// fuseStatTimeout is 800ms — a 23ms margin that every jitter spike blows. 258
// reads each burned the full budget and populated nothing.
//
// fuseStatTimeout ALREADY documents 2s for exactly this case, gated on
// JM_WAN_MODE=1 — set in nothing but test files.
func TestWarmOpTimeout_LongerOnHighLatency(t *testing.T) {
	t.Setenv("JM_WARM_OP_TIMEOUT_MS", "")
	t.Setenv("JM_NET_LATENCY_CEILING", "")

	np := netprofile.Default()
	for i := 0; i < 60; i++ { // settle LAN-fast first
		np.ObserveRTT(300 * time.Microsecond)
	}
	if got := warmOpTimeout(); got != fuseStatTimeout {
		t.Fatalf("LAN warm timeout = %v, want the foreground default %v", got, fuseStatTimeout)
	}

	for i := 0; i < 6; i++ { // drive high-latency
		np.ObserveRTT(300 * time.Millisecond)
	}
	if !np.HighLatency() {
		t.Fatal("precondition: netprofile did not enter high-latency")
	}
	got := warmOpTimeout()
	if got < 2*time.Second {
		t.Errorf("warm timeout on a far link = %v, want >= 2s — a 777ms `._` read "+
			"under an 800ms budget is why 258 of 264 warms timed out", got)
	}
	if got <= fuseStatTimeout {
		t.Errorf("warm timeout %v did not exceed the foreground bound %v", got, fuseStatTimeout)
	}
}

// The FOREGROUND bound must be untouched. It is healthy at 800ms (10 timeouts
// in 6228 calls), and a longer foreground FUSE wait is the JUKEBOX generator
// behind "connection interrupted". Raising it is a separate decision.
func TestWarmOpTimeout_ForegroundBoundUnchanged(t *testing.T) {
	t.Setenv("JM_WAN_MODE", "")
	if fuseStatTimeout != 800*time.Millisecond {
		t.Errorf("fuseStatTimeout = %v, want 800ms — W1 must not move the "+
			"foreground budget", fuseStatTimeout)
	}
}

func TestWarmOpTimeout_ExplicitOverride(t *testing.T) {
	t.Setenv("JM_WARM_OP_TIMEOUT_MS", "5000")
	if got := warmOpTimeout(); got != 5*time.Second {
		t.Errorf("explicit override gave %v, want 5s", got)
	}
	t.Setenv("JM_WARM_OP_TIMEOUT_MS", "garbage")
	np := netprofile.Default()
	for i := 0; i < 60; i++ {
		np.ObserveRTT(300 * time.Microsecond)
	}
	if got := warmOpTimeout(); got != fuseStatTimeout {
		t.Errorf("unparseable override gave %v, want the LAN default %v", got, fuseStatTimeout)
	}
}
