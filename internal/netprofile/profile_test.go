package netprofile

import (
	"testing"
	"time"
)

func mb(f float64) int64 { return int64(f * 1024 * 1024) }

// fakeClock drives the windowed throughput estimator deterministically.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time      { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// sampleFor feeds `blocks` 4 MB backend reads at the given bytes/sec, driving an
// installed fake clock so the aggregate-bytes/wall-time window sees real
// elapsed intervals. Each read advances the clock by its own duration (the
// sequential, non-overlapping case).
func sampleFor(p *Profile, bps float64, blocks int) {
	c := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	p.now = c.now
	bytes := int64(4 * 1024 * 1024) // one 4 MB block
	dur := time.Duration(float64(bytes) / bps * float64(time.Second))
	for i := 0; i < blocks; i++ {
		c.advance(dur)
		p.ObserveThroughput(bytes, dur)
	}
}

func TestUnknownDefaultsToMedium(t *testing.T) {
	p := New()
	if c := p.Class(); c != ClassMedium {
		t.Fatalf("no-signal class = %v, want medium (unchanged behavior)", c)
	}
	ra := p.Readahead()
	// Medium must equal the historical hard-coded defaults exactly.
	if ra.SeqThreshold != 3 || ra.Blocks != 8 || ra.Workers != 4 || !ra.Enabled {
		t.Fatalf("medium policy %+v != historical default {Enabled:true Seq:3 Blocks:8 Workers:4}", ra)
	}
}

func TestBandwidthClassification(t *testing.T) {
	cases := []struct {
		name  string
		mbps  float64
		want  LinkClass
	}{
		{"cellular", 5, ClassSlow},   // 5 MB/s is >3 (metered) and <30 (slow) → slow
		{"weak-cellular", 2, ClassMetered},
		{"good-wifi", 100, ClassMedium},
		{"ten-gig", 600, ClassFast},
		{"three-gbit", 375, ClassFast}, // ~3 Gbit/s observed on 10GbE → already fast bucket
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := New()
			sampleFor(p, tc.mbps*1024*1024, 8)
			if c := p.Class(); c != tc.want {
				snap := p.Snapshot()
				t.Fatalf("%g MB/s → %v, want %v (bwBps=%.0f n=%d)", tc.mbps, c, tc.want, snap.BytesPerSec, snap.ThroughputN)
			}
		})
	}
}

func TestMeteredSuppressesReadahead(t *testing.T) {
	p := New()
	sampleFor(p, 1.5*1024*1024, 6) // 1.5 MB/s → metered
	ra := p.Readahead()
	if ra.Enabled {
		t.Fatalf("metered link must disable server readahead, got %+v", ra)
	}
	if ra.Blocks > 1 {
		t.Fatalf("metered link must not prefetch ahead, got Blocks=%d", ra.Blocks)
	}
}

func TestFastIsMoreAggressiveThanMedium(t *testing.T) {
	fast := New()
	sampleFor(fast, 800*1024*1024, 8)
	med := New()
	sampleFor(med, 100*1024*1024, 8)
	fp, mp := fast.Readahead(), med.Readahead()
	if !(fp.Blocks > mp.Blocks && fp.Workers >= mp.Workers && fp.SeqThreshold <= mp.SeqThreshold) {
		t.Fatalf("fast %+v must be strictly more aggressive than medium %+v", fp, mp)
	}
}

func TestRTTBootstrapBeforeBandwidth(t *testing.T) {
	// LAN sub-ms RTT, no throughput yet → presume fast (aggressive prefetch).
	lan := New()
	lan.ObserveRTT(400 * time.Microsecond)
	if c := lan.Class(); c != ClassFast {
		t.Fatalf("sub-ms RTT bootstrap → %v, want fast", c)
	}
	if !lan.Snapshot().BootstrappedRTT {
		t.Fatal("expected BootstrappedRTT=true before any throughput sample")
	}
	// High RTT, no throughput → presume slow (conservative).
	wan := New()
	wan.ObserveRTT(56 * time.Millisecond)
	if c := wan.Class(); c != ClassSlow {
		t.Fatalf("56ms RTT bootstrap → %v, want slow", c)
	}
}

func TestBandwidthOverridesRTTBootstrap(t *testing.T) {
	// A 56ms-RTT link that turns out to be high-bandwidth should reclassify up
	// once throughput samples arrive (RTT bootstrap was only a placeholder).
	p := New()
	p.ObserveRTT(56 * time.Millisecond) // would bootstrap to slow
	sampleFor(p, 300*1024*1024, 8)      // but it's actually fast
	if c := p.Class(); c != ClassFast {
		t.Fatalf("high-bw 56ms link → %v, want fast (bw overrides rtt bootstrap)", c)
	}
	if p.Snapshot().BootstrappedRTT {
		t.Fatal("BootstrappedRTT should be false once bandwidth is known")
	}
}

func TestCacheHitsDoNotInflateBandwidth(t *testing.T) {
	p := New()
	// 200 "cache hit" reads: 4 MB in 50µs each (~80 GB/s) — must be ignored.
	for i := 0; i < 200; i++ {
		p.ObserveThroughput(4*1024*1024, 50*time.Microsecond)
	}
	if p.Snapshot().HaveBW {
		t.Fatal("sub-threshold cache-hit reads must not register as bandwidth samples")
	}
	// One real cellular block then classifies correctly.
	sampleFor(p, 5*1024*1024, 1)
	if c := p.Class(); c != ClassSlow {
		t.Fatalf("after one real 5 MB/s sample → %v, want slow", c)
	}
}

func TestTinySamplesIgnored(t *testing.T) {
	p := New()
	p.ObserveThroughput(4096, 10*time.Millisecond) // 4 KB — below minThroughputBytes
	if p.Snapshot().HaveBW {
		t.Fatal("4 KB sample must be ignored (below min bytes)")
	}
}

func TestJuiceFSPolicyByClass(t *testing.T) {
	// medium (no signal) == historical mount defaults exactly.
	med := New().JuiceFS()
	if med.BufferSizeMB != 4096 || med.Prefetch != 3 {
		t.Fatalf("medium juicefs policy %+v != historical {4096,3}", med)
	}
	// metered: prefetch off, buffer still >= a single media file.
	metered := New()
	sampleFor(metered, 1.5*1024*1024, 6)
	mp := metered.JuiceFS()
	if mp.Prefetch != 0 {
		t.Fatalf("metered must set --prefetch 0, got %d", mp.Prefetch)
	}
	if mp.BufferSizeMB < 256 {
		t.Fatalf("metered buffer %d MB too small to absorb a single media write", mp.BufferSizeMB)
	}
	// fast: wider prefetch than medium to fill the pipe.
	fast := New()
	sampleFor(fast, 800*1024*1024, 8)
	fp := fast.JuiceFS()
	if fp.Prefetch <= med.Prefetch {
		t.Fatalf("fast prefetch %d must exceed medium %d (fill 10GbE)", fp.Prefetch, med.Prefetch)
	}
}

func TestWindowedAggregatesConcurrentReads(t *testing.T) {
	// The concurrent-storm case: 1 MB reads each REPORT a 100 ms (queued)
	// duration but complete 10 ms apart on the wall clock. Per-read bytes/dur
	// would say ~10 MB/s; the aggregate-over-wall-time estimate must be much
	// higher because the bytes really moved concurrently. This is the exact
	// under-count the live deploy exposed (36 KB/s during a readahead storm).
	p := New()
	c := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	p.now = c.now
	for i := 0; i < 12; i++ {
		c.advance(10 * time.Millisecond)
		p.ObserveThroughput(1<<20, 100*time.Millisecond)
	}
	bw := p.Snapshot().BytesPerSec
	perRead := float64(int64(1<<20)) / 0.1 // 10 MB/s
	if bw <= perRead*2 {
		t.Fatalf("windowed bw %.0f B/s must be >> per-read %.0f B/s (concurrency captured)", bw, perRead)
	}
}

func TestForceClass(t *testing.T) {
	p := New()
	sampleFor(p, 800*1024*1024, 8) // would be fast
	c := ClassMetered
	p.ForceClass(&c)
	if got := p.Class(); got != ClassMetered {
		t.Fatalf("ForceClass(metered) → %v, want metered despite fast bandwidth", got)
	}
	if p.Readahead().Enabled {
		t.Fatal("forced metered must disable readahead")
	}
	p.ForceClass(nil)
	if got := p.Class(); got != ClassFast {
		t.Fatalf("after ForceClass(nil) → %v, want fast (auto resumes)", got)
	}
}
