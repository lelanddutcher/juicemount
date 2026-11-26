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
	p.SetBackendRoute("utun4", true)
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
	fat.SetBackendRoute("utun4", true)
	fat.ObserveRTT(time.Millisecond)
	sampleFor(fat, 800*mib, 8)
	if c := fat.Class(); c != ClassMedium {
		t.Fatalf("measured 800 MiB/s tunnel: class %s, want medium (a tunnel never earns fast)", c)
	}

	mid := New()
	mid.SetBackendRoute("utun4", true)
	mid.ObserveRTT(12 * time.Millisecond)
	sampleFor(mid, 15*mib, 8)
	if c := mid.Class(); c != ClassSlow {
		t.Fatalf("measured 15 MiB/s tunnel: class %s, want slow by bandwidth", c)
	}

	thin := New()
	thin.SetBackendRoute("utun4", true)
	thin.ObserveRTT(2 * time.Millisecond)
	sampleFor(thin, 1*mib, 8)
	if c := thin.Class(); c != ClassMetered {
		t.Fatalf("thin near tunnel (unflagged hotspot): class %s, want metered", c)
	}

	far := New()
	far.SetBackendRoute("utun4", true)
	far.ObserveRTT(200 * time.Millisecond)
	sampleFor(far, 60*mib, 8)
	if c := far.Class(); c != ClassSlow || !far.HighLatency() {
		t.Fatalf("far fat tunnel: class %s high latency %v, want slow and high latency", c, far.HighLatency())
	}

	cost := New()
	cost.SetBackendRoute("utun4", true)
	sampleFor(cost, 60*mib, 8)
	metered := ClassMetered
	cost.SetNetworkCostClass(&metered)
	if c := cost.Class(); c != ClassMetered {
		t.Fatalf("host-flagged expensive tunnel: class %s, want metered", c)
	}

	p.SetBackendRoute("en0", false)
	if _, ok := p.RouteCeiling(); ok {
		t.Fatal("route ceiling still set after the tunnel route was lifted")
	}
}

func TestTunnelRouteKillSwitch(t *testing.T) {
	t.Setenv("JM_NET_TUNNEL_MEASURED", "0")
	p := New()
	p.SetBackendRoute("utun4", true)
	sampleFor(p, 60*mib, 8)
	if c := p.Class(); c != ClassSlow {
		t.Fatalf("JM_NET_TUNNEL_MEASURED=0: class %s, want the fixed slow cap", c)
	}
}

// The explicit ceiling (a failed route lookup before any success) stays
// slow whatever the tunnel rule allows.
func TestExplicitCeilingBeatsMeasuredTunnel(t *testing.T) {
	p := New()
	p.SetBackendRoute("utun4", true)
	sampleFor(p, 60*mib, 8)
	slow := ClassSlow
	p.CeilingClass(&slow)
	if c := p.Class(); c != ClassSlow {
		t.Fatalf("explicit slow ceiling over a measured tunnel: class %s, want slow", c)
	}
}

// PR #110 review H1: a Mac measured on its LAN (no tunnel) that moves to an
// unflagged network behind a tunnel starts the tunnel over: slow until 4 new
// samples, with none of the LAN estimate left.
func TestTunnelAfterLANStartsOverMeasured(t *testing.T) {
	t.Setenv("JM_NET_TUNNEL_MEASURED", "")
	p := New()
	c := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	p.now = c.now
	p.SetBackendRoute("en0", false)
	p.ObserveRTT(time.Millisecond)
	block := func(bps float64) {
		d := time.Duration(float64(4*mib) / bps * float64(time.Second))
		c.advance(d)
		p.ObserveThroughput(4*mib, d)
	}
	for p.Snapshot().ThroughputN < 16 {
		block(100 * mib)
	}
	if cl := p.Class(); cl != ClassMedium {
		t.Fatalf("LAN at 100 MiB/s: class %s, want medium", cl)
	}
	c.advance(time.Second) // the move: no reads for a while
	p.SetBackendRoute("utun4", true)
	if s := p.Snapshot(); s.HaveBW || s.ThroughputN != 0 {
		t.Fatalf("tunnel on kept the LAN measurement: have %v, samples %d", s.HaveBW, s.ThroughputN)
	}
	if cl := p.Class(); cl != ClassSlow {
		t.Fatalf("first moment on the tunnel: class %s, want slow", cl)
	}
	for p.Snapshot().ThroughputN < tunnelMeasuredSamples-1 {
		block(40 * mib)
	}
	if cl := p.Class(); cl != ClassSlow {
		t.Fatalf("tunnel with %d new samples: class %s, want slow", tunnelMeasuredSamples-1, cl)
	}
	for p.Snapshot().ThroughputN < tunnelMeasuredSamples {
		block(40 * mib)
	}
	if cl := p.Class(); cl != ClassMedium {
		t.Fatalf("tunnel measured at 40 MiB/s: class %s, want medium", cl)
	}
	// The same lookup again changes nothing; another interface starts over.
	p.SetBackendRoute("utun4", true)
	if cl := p.Class(); cl != ClassMedium {
		t.Fatalf("repeated lookup reset the tunnel: class %s", cl)
	}
	p.SetBackendRoute("utun7", true)
	if cl := p.Class(); cl != ClassSlow {
		t.Fatalf("new tunnel interface: class %s, want slow", cl)
	}
}

// PR #110 review H2: a stalled read must not freeze the estimate. 40 MiB/s,
// then a 300 s read completes and the link carries 0.25 MiB/s in back-to-back
// 256 KiB reads: the estimate falls and the class drops within seconds.
func TestStalledReadDoesNotFreezeEstimate(t *testing.T) {
	p := New()
	c := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	p.now = c.now
	p.ObserveRTT(5 * time.Millisecond)
	for i := 0; i < 20; i++ {
		c.advance(100 * time.Millisecond)
		p.ObserveThroughput(4*mib, 100*time.Millisecond)
	}
	if cl := p.Class(); cl != ClassMedium {
		t.Fatalf("40 MiB/s: class %s, want medium", cl)
	}
	c.advance(50 * time.Millisecond)
	p.ObserveThroughput(4*mib, 300*time.Second)
	before := p.Snapshot().ThroughputN
	var slowAt, meteredAt time.Duration
	for el := time.Duration(0); el < 120*time.Second; el += time.Second {
		c.advance(time.Second)
		p.ObserveThroughput(256*1024, time.Second)
		if slowAt == 0 && p.Class() < ClassMedium {
			slowAt = el + time.Second
		}
		if meteredAt == 0 && p.Class() == ClassMetered {
			meteredAt = el + time.Second
		}
	}
	s := p.Snapshot()
	if s.ThroughputN-before < 60 {
		t.Fatalf("only %d samples in 120 s after the stall; the estimate froze", s.ThroughputN-before)
	}
	if slowAt == 0 || slowAt > 5*time.Second {
		t.Fatalf("class left medium after %v, want within 5 s", slowAt)
	}
	if meteredAt == 0 || meteredAt > 20*time.Second {
		t.Fatalf("class reached metered after %v, want within 20 s", meteredAt)
	}
	if got := s.BytesPerSec / mib; got > 0.5 {
		t.Fatalf("estimate %.2f MiB/s after 120 s at 0.25 MiB/s", got)
	}
}

// JM_NET_BW_WINDOW_CARRY=0 restores the old windows: 4 reads of 300 ms in
// flight measure one request's rate again (3.3 MiB/s).
func TestWindowCarryKillSwitch(t *testing.T) {
	t.Setenv("JM_NET_BW_WINDOW_CARRY", "0")
	p := New()
	c := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	p.now = c.now
	concurrentReads(p, c, 60, 4, mib, 300*time.Millisecond)
	if got := p.Snapshot().BytesPerSec / mib; got < 3.2 || got > 3.4 {
		t.Fatalf("carry off: estimate %.2f MiB/s, want the old 3.33", got)
	}
}
