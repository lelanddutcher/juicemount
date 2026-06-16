package netprofile

import (
	"testing"
	"time"
)

func mb(f float64) int64 { return int64(f * 1024 * 1024) }

// A real backend transfer sample: `bytes` moved over the time implied by `bps`.
func sampleFor(p *Profile, bps float64, blocks int) {
	bytes := int64(4 * 1024 * 1024) // one 4 MB block
	dur := time.Duration(float64(bytes) / bps * float64(time.Second))
	for i := 0; i < blocks; i++ {
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
