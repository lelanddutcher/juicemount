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

// A Mac can accumulate a near-LAN EWMA for hours, then hand off to a phone.
// The first far sample must clamp immediately rather than waiting many 2s
// probe periods for the smoothed average to climb past the threshold.
func TestLatencyCeiling_HandoffClampsOnFirstFarSample(t *testing.T) {
	p := New()
	for i := 0; i < 20; i++ {
		p.ObserveRTT(time.Millisecond)
	}
	p.mu.Lock()
	p.bwBps = 512 * 1024 * 1024
	p.haveBW = true
	p.mu.Unlock()
	if got := p.Class(); got != ClassFast {
		t.Fatalf("precondition: near fat link class=%v, want fast", got)
	}
	p.ObserveRTT(80 * time.Millisecond)
	if got := p.Class(); got != ClassSlow {
		t.Fatalf("first 80ms handoff sample class=%v, want immediate slow clamp", got)
	}
}

// A host-reported metered (cost) link is stricter than Slow and must not be
// relaxed UP to Slow by the ceiling — the ceiling only ever downgrades.
func TestLatencyCeiling_NeverRelaxesMetered(t *testing.T) {
	t.Setenv("JM_NET_LATENCY_CEILING", "")
	p := New()
	cost := ClassMetered
	p.SetNetworkCostClass(&cost)
	p.ObserveRTT(300 * time.Millisecond)
	p.mu.Lock()
	p.bwBps = 1 * 1024 * 1024
	p.haveBW = true
	p.mu.Unlock()

	if got := p.Class(); got != ClassMetered {
		t.Fatalf("Class() = %v, want metered — the ceiling must only downgrade", got)
	}
}

// The original form of the test above, without a cost flag: a 300 ms,
// 1 MB/s link. The ceiling still never relaxes its READ policy: the class is
// slow (the slow-RTT floor, for the drain), and speculative reads stay
// metered.
func TestLatencyCeiling_NeverRelaxesThinFarReadPolicy(t *testing.T) {
	t.Setenv("JM_NET_LATENCY_CEILING", "")
	t.Setenv("JM_NET_SLOW_RTT_FLOOR", "")
	p := New()
	p.ObserveRTT(300 * time.Millisecond)
	p.mu.Lock()
	p.bwBps = 1 * 1024 * 1024
	p.haveBW = true
	p.mu.Unlock()
	if got := p.ReadPolicyClass(); got != ClassMetered {
		t.Fatalf("ReadPolicyClass() = %v, want metered", got)
	}
	if ra := p.Readahead(); ra.Workers != 1 || ra.ParallelFetch {
		t.Fatalf("readahead %+v, want metered", ra)
	}
}

// The 2026-11-11 laptop lab: a far link (200 ms) classed slow by its RTT went
// metered after one cold read measured under 3 MiB/s, and the drain lost its
// slots. Low throughput on a far link is distance, not cost: it stays slow and
// reports LowBandwidth.
func TestSlowRTTLinkWithSlowReadStaysSlow(t *testing.T) {
	t.Setenv("JM_NET_LATENCY_CEILING", "")
	t.Setenv("JM_NET_SLOW_RTT_FLOOR", "")
	p := New()
	p.ObserveRTT(200 * time.Millisecond)
	if got := p.Class(); got != ClassSlow {
		t.Fatalf("precondition: 200 ms link class %v, want slow", got)
	}
	sampleFor(p, 0.5*1024*1024, 8) // 0.5 MiB/s cold reads, as measured abroad
	snap := p.Snapshot()
	if snap.Class != ClassSlow {
		t.Fatalf("far link after a slow read: class %v, want slow (bwBps=%.0f)", snap.Class, snap.BytesPerSec)
	}
	if !snap.HaveBW || !snap.LowBandwidth || snap.ReadClass != ClassMetered {
		t.Fatalf("snapshot %+v: want the low-bandwidth measurement kept and the metered read class", snap)
	}
}

// Review H1/M2: a far, thin link without a cost flag may be unflagged
// cellular (Android or MiFi hotspot, a cellular router, in-flight Wi-Fi),
// typically 60-150 ms at about 1 MiB/s. Its class stays slow for the drain,
// but speculative downloads keep the METERED policy: one read-ahead worker,
// one block, no parallel fetch, JuiceFS prefetch 0.
func TestUnflaggedThinFarLinkKeepsMeteredReadPolicy(t *testing.T) {
	t.Setenv("JM_NET_LATENCY_CEILING", "")
	t.Setenv("JM_NET_SLOW_RTT_FLOOR", "")
	for _, rtt := range []time.Duration{45, 60, 90, 120, 150} {
		rtt := rtt * time.Millisecond
		t.Run(rtt.String(), func(t *testing.T) {
			p := New()
			p.ObserveRTT(rtt)
			sampleFor(p, 1*1024*1024, 8)
			if got := p.Class(); got != ClassSlow {
				t.Fatalf("class %v, want slow (the drain's class)", got)
			}
			if got := p.ReadPolicyClass(); got != ClassMetered {
				t.Fatalf("read policy class %v, want metered", got)
			}
			ra := p.Readahead()
			if ra.Workers != 1 || ra.Blocks != 1 || ra.ParallelFetch || ra.SeqThreshold != 6 {
				t.Fatalf("readahead %+v, want the metered policy", ra)
			}
			if jp := p.JuiceFS(); jp.Prefetch != 0 || jp.BufferSizeMB != 256 {
				t.Fatalf("juicefs %+v, want the metered policy (prefetch 0)", jp)
			}
		})
	}
}

// A far link with enough bandwidth keeps the slow read policy: the read class
// follows the floor only while the link is thin.
func TestFarLinkWithBandwidthKeepsSlowReadPolicy(t *testing.T) {
	t.Setenv("JM_NET_LATENCY_CEILING", "")
	p := New()
	p.ObserveRTT(200 * time.Millisecond)
	sampleFor(p, 8*1024*1024, 8)
	if p.Class() != ClassSlow || p.ReadPolicyClass() != ClassSlow {
		t.Fatalf("class %v read %v, want slow/slow", p.Class(), p.ReadPolicyClass())
	}
	if ra := p.Readahead(); ra.Workers != 4 || !ra.ParallelFetch {
		t.Fatalf("readahead %+v: want the slow policy", ra)
	}
}

// Real metered stays metered: the host cost signal, an operator force, and a
// NEAR link that measures under 3 MiB/s (a VPN over an unflagged cellular
// underlay) all keep ClassMetered.
func TestRealMeteredStaysMetered(t *testing.T) {
	t.Setenv("JM_NET_LATENCY_CEILING", "")
	t.Setenv("JM_NET_SLOW_RTT_FLOOR", "")
	t.Run("host-cost-far", func(t *testing.T) {
		p := New()
		cost := ClassMetered
		p.SetNetworkCostClass(&cost)
		p.ObserveRTT(200 * time.Millisecond)
		sampleFor(p, 0.5*1024*1024, 8)
		if got := p.Class(); got != ClassMetered {
			t.Fatalf("class %v, want metered", got)
		}
		if ra := p.Readahead(); ra.Workers != 1 || ra.ParallelFetch {
			t.Fatalf("readahead %+v: real cellular must keep the metered policy", ra)
		}
	})
	t.Run("forced", func(t *testing.T) {
		p := New()
		c := ClassMetered
		p.ForceClass(&c)
		p.ObserveRTT(200 * time.Millisecond)
		sampleFor(p, 50*1024*1024, 8)
		if got := p.Class(); got != ClassMetered {
			t.Fatalf("class %v, want metered", got)
		}
	})
	t.Run("near-low-bandwidth", func(t *testing.T) {
		p := New()
		p.ObserveRTT(5 * time.Millisecond)
		sampleFor(p, 1*1024*1024, 8)
		if got := p.Class(); got != ClassMetered {
			t.Fatalf("class %v, want metered", got)
		}
	})
	t.Run("kill-switch", func(t *testing.T) {
		t.Setenv("JM_NET_SLOW_RTT_FLOOR", "0")
		p := New()
		p.ObserveRTT(200 * time.Millisecond)
		sampleFor(p, 0.5*1024*1024, 8)
		if got := p.Class(); got != ClassMetered {
			t.Fatalf("class %v with JM_NET_SLOW_RTT_FLOOR=0, want the old metered", got)
		}
	})
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
