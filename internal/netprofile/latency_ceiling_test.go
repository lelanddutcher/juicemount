package netprofile

import (
	"testing"
	"time"
)

// TestLatencyCeiling_RealCellularTunnel is built from a link measured on
// 2026-07-29, Tailscale over cellular:
//
//	route utun9 · real RTT 156ms avg (105-349, stddev 86)
//	netprofile said: class=fast  bandwidth=518.9 Mbps  rtt_ms=105
//
// Bandwidth-only classification called that "fast". Eight protections gate on
// the class, and the SYNCHRONOUS cold-directory populate is gated to ClassFast
// specifically — so the least LAN-like link possible was getting the LAN
// foreground path. A far link must never classify above Slow.
func TestLatencyCeiling_RealCellularTunnel(t *testing.T) {
	t.Setenv("JM_NET_LATENCY_CEILING", "")
	p := New()
	// Fat pipe: 518.9 Mbps ≈ 68 MB/s — comfortably "fast" by bandwidth alone.
	p.ObserveRTT(156 * time.Millisecond)
	p.mu.Lock()
	p.bwBps = 512 * 1024 * 1024
	p.haveBW = true
	p.mu.Unlock()

	if got := p.Class(); got != ClassSlow {
		t.Fatalf("Class() = %v on a 156ms/518Mbps cellular tunnel, want slow — "+
			"bandwidth-only classification is what gave a phone tunnel the LAN path", got)
	}
	if !p.HighLatency() {
		t.Error("HighLatency() = false at 156ms RTT")
	}
}

// A genuinely fast AND near link must be unaffected — this is the 10GbE case,
// and a regression here would throttle the LAN.
func TestLatencyCeiling_LANUnaffected(t *testing.T) {
	t.Setenv("JM_NET_LATENCY_CEILING", "")
	p := New()
	p.ObserveRTT(400 * time.Microsecond)
	p.mu.Lock()
	p.bwBps = 512 * 1024 * 1024
	p.haveBW = true
	p.mu.Unlock()

	if got := p.Class(); got != ClassFast {
		t.Fatalf("Class() = %v on 0.4ms/512MB-s LAN, want fast", got)
	}
	if p.HighLatency() {
		t.Error("HighLatency() = true on a sub-ms LAN")
	}
}

// Hysteresis: a link that crosses the enter threshold must NOT clear on the
// first sample back under it, or a jittery cellular link (observed stddev 86ms)
// flaps the class on every probe and every class-gated protection oscillates.
func TestLatencyCeiling_Hysteresis(t *testing.T) {
	t.Setenv("JM_NET_LATENCY_CEILING", "")
	p := New()
	p.ObserveRTT(300 * time.Millisecond)
	for i := 0; i < 5; i++ { // smooth upward
		p.ObserveRTT(300 * time.Millisecond)
	}
	if !p.HighLatency() {
		t.Fatal("did not enter high-latency at a sustained 300ms")
	}
	// Between exit (100ms) and enter (150ms): must STAY high.
	for i := 0; i < 10; i++ {
		p.ObserveRTT(120 * time.Millisecond)
	}
	if !p.HighLatency() {
		t.Error("left high-latency inside the 100-150ms dead zone — that is a flap")
	}
	// Sustained well below exit: must clear.
	for i := 0; i < 40; i++ {
		p.ObserveRTT(5 * time.Millisecond)
	}
	if p.HighLatency() {
		t.Error("stayed high-latency after RTT settled at 5ms — the ceiling is sticky")
	}
}

// Metered is already stricter than Slow and must not be relaxed UP to Slow by
// the ceiling — the ceiling only ever downgrades.
func TestLatencyCeiling_NeverRelaxesMetered(t *testing.T) {
	t.Setenv("JM_NET_LATENCY_CEILING", "")
	p := New()
	p.ObserveRTT(300 * time.Millisecond)
	p.mu.Lock()
	p.bwBps = 1 * 1024 * 1024 // 1 MB/s → metered
	p.haveBW = true
	p.mu.Unlock()

	if got := p.Class(); got != ClassMetered {
		t.Fatalf("Class() = %v, want metered — the ceiling must only downgrade", got)
	}
}

// The kill switch restores bandwidth-only classification exactly.
func TestLatencyCeiling_KillSwitch(t *testing.T) {
	t.Setenv("JM_NET_LATENCY_CEILING", "0")
	p := New()
	p.ObserveRTT(300 * time.Millisecond)
	p.mu.Lock()
	p.bwBps = 512 * 1024 * 1024
	p.haveBW = true
	p.mu.Unlock()

	if got := p.Class(); got != ClassFast {
		t.Fatalf("Class() = %v with JM_NET_LATENCY_CEILING=0, want fast (pre-fix behavior)", got)
	}
}
