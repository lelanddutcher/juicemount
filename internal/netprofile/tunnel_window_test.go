package netprofile

import (
	"testing"
	"time"
)

const mib = 1024 * 1024

// concurrentReads feeds n reads of size bytes, inflight at a time, each
// taking dur, in steady state: one completes every dur/inflight.
func concurrentReads(p *Profile, c *fakeClock, n, inflight int, size int64, dur time.Duration) {
	step := dur / time.Duration(inflight)
	for i := 0; i < n; i++ {
		c.advance(step)
		p.ObserveThroughput(size, dur)
	}
}

// Concurrent reads longer than the flush interval: the estimate is the
// link's aggregate rate, not one request's. Before, each window opened at
// its first read's start (time the last window had already divided by), and
// a read longer than bwWindowFlush flushed the window alone: 4 × 1 MiB reads
// of 300 ms in flight (13.3 MiB/s) measured 3.3 MiB/s, under the metered
// threshold.
func TestWindowConcurrentLongReadsMeasureAggregate(t *testing.T) {
	p := New()
	c := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	p.now = c.now
	concurrentReads(p, c, 60, 4, mib, 300*time.Millisecond)
	got := p.Snapshot().BytesPerSec / mib
	want := 4.0 / 0.3
	if got < want*0.8 || got > want*1.2 {
		t.Fatalf("estimate %.1f MiB/s, want about %.1f MiB/s (4 reads in flight)", got, want)
	}
	if c := p.Class(); c != ClassSlow {
		t.Fatalf("class %s, want slow (13 MiB/s)", c)
	}
}

// One serial stream stays one stream: back-to-back 1 MiB reads of 1 s each
// on a thin, near link still measure 1 MiB/s and stay metered.
func TestWindowSerialThinLinkStaysMetered(t *testing.T) {
	p := New()
	c := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	p.now = c.now
	p.ObserveRTT(5 * time.Millisecond)
	concurrentReads(p, c, 20, 1, mib, time.Second)
	got := p.Snapshot().BytesPerSec / mib
	if got < 0.8 || got > 1.2 {
		t.Fatalf("estimate %.2f MiB/s, want about 1", got)
	}
	if c := p.Class(); c != ClassMetered {
		t.Fatalf("class %s, want metered", c)
	}
}

// A read that ran across many windows (a stalled GET) must not land all its
// bytes in one short window: on a thin link that spike could lift the class
// out of metered.
func TestWindowStalledReadIsNoSpike(t *testing.T) {
	p := New()
	c := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	p.now = c.now
	p.ObserveRTT(5 * time.Millisecond)
	// 1 MiB/s: 256 KiB reads of 250 ms, back to back, for 10 s, while one
	// 4 MiB read is stalled for those 10 s.
	for i := 0; i < 40; i++ {
		c.advance(250 * time.Millisecond)
		p.ObserveThroughput(256*1024, 250*time.Millisecond)
	}
	p.ObserveThroughput(4*mib, 10*time.Second)
	for i := 0; i < 8; i++ {
		c.advance(250 * time.Millisecond)
		p.ObserveThroughput(256*1024, 250*time.Millisecond)
	}
	if got := p.Snapshot().BytesPerSec / mib; got > 3 {
		t.Fatalf("estimate %.2f MiB/s after a stalled read; want under the 3 MiB/s metered threshold", got)
	}
	if c := p.Class(); c != ClassMetered {
		t.Fatalf("class %s, want metered", c)
	}
}

// A tunnel is slow until measured; then the measurement decides, capped at
// medium.
func TestTunnelRouteFollowsMeasurement(t *testing.T) {
	t.Setenv("JM_NET_TUNNEL_MEASURED", "")
	p := New()
	p.TunnelRoute(true)
	p.ObserveRTT(2 * time.Millisecond) // a nearby tunnel endpoint
	if c := p.Class(); c != ClassSlow {
		t.Fatalf("unmeasured tunnel at 2 ms: class %s, want slow", c)
	}
	if c, ok := p.RouteCeiling(); !ok || c != ClassSlow {
		t.Fatalf("unmeasured tunnel ceiling %s %v, want slow", c, ok)
	}
	c := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	p.now = c.now
	block := func() { // one 4 MiB read at 40 MiB/s, back to back
		c.advance(100 * time.Millisecond)
		p.ObserveThroughput(4*mib, 100*time.Millisecond)
	}
	for p.Snapshot().ThroughputN < tunnelMeasuredSamples-1 {
		block()
	}
	if c := p.Class(); c != ClassSlow {
		t.Fatalf("tunnel with %d samples: class %s, want slow until %d", tunnelMeasuredSamples-1, c, tunnelMeasuredSamples)
	}
	for p.Snapshot().ThroughputN < tunnelMeasuredSamples {
		block()
	}
	if c := p.Class(); c != ClassMedium {
		t.Fatalf("measured 40 MiB/s tunnel: class %s, want medium", c)
	}
	if c, ok := p.RouteCeiling(); !ok || c != ClassMedium {
		t.Fatalf("measured tunnel ceiling %s %v, want medium", c, ok)
	}

	fat := New()
	fat.TunnelRoute(true)
	fat.ObserveRTT(time.Millisecond)
	sampleFor(fat, 800*mib, 8)
	if c := fat.Class(); c != ClassMedium {
		t.Fatalf("measured 800 MiB/s tunnel: class %s, want medium (a tunnel never earns fast)", c)
	}

	mid := New()
	mid.TunnelRoute(true)
	mid.ObserveRTT(12 * time.Millisecond)
	sampleFor(mid, 15*mib, 8)
	if c := mid.Class(); c != ClassSlow {
		t.Fatalf("measured 15 MiB/s tunnel: class %s, want slow by bandwidth", c)
	}

	thin := New()
	thin.TunnelRoute(true)
	thin.ObserveRTT(2 * time.Millisecond)
	sampleFor(thin, 1*mib, 8)
	if c := thin.Class(); c != ClassMetered {
		t.Fatalf("thin near tunnel (unflagged hotspot): class %s, want metered", c)
	}

	far := New()
	far.TunnelRoute(true)
	far.ObserveRTT(200 * time.Millisecond)
	sampleFor(far, 60*mib, 8)
	if c := far.Class(); c != ClassSlow || !far.HighLatency() {
		t.Fatalf("far fat tunnel: class %s high latency %v, want slow and high latency", c, far.HighLatency())
	}

	cost := New()
	cost.TunnelRoute(true)
	sampleFor(cost, 60*mib, 8)
	metered := ClassMetered
	cost.SetNetworkCostClass(&metered)
	if c := cost.Class(); c != ClassMetered {
		t.Fatalf("host-flagged expensive tunnel: class %s, want metered", c)
	}

	p.TunnelRoute(false)
	if _, ok := p.RouteCeiling(); ok {
		t.Fatal("route ceiling still set after the tunnel route was lifted")
	}
}

func TestTunnelRouteKillSwitch(t *testing.T) {
	t.Setenv("JM_NET_TUNNEL_MEASURED", "0")
	p := New()
	p.TunnelRoute(true)
	sampleFor(p, 60*mib, 8)
	if c := p.Class(); c != ClassSlow {
		t.Fatalf("JM_NET_TUNNEL_MEASURED=0: class %s, want the fixed slow cap", c)
	}
}

// The explicit ceiling (a failed route lookup before any success) stays
// slow whatever the tunnel rule allows.
func TestExplicitCeilingBeatsMeasuredTunnel(t *testing.T) {
	p := New()
	p.TunnelRoute(true)
	sampleFor(p, 60*mib, 8)
	slow := ClassSlow
	p.CeilingClass(&slow)
	if c := p.Class(); c != ClassSlow {
		t.Fatalf("explicit slow ceiling over a measured tunnel: class %s, want slow", c)
	}
}
