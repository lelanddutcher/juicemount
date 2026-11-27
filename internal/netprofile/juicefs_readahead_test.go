package netprofile

import (
	"testing"
	"time"
)

// warm folds in RTT and throughput samples until the estimate settles.
func warm(p *Profile, rtt time.Duration, bytesPerSec float64) {
	for i := 0; i < 20; i++ {
		p.ObserveRTT(rtt)
	}
	sampleFor(p, bytesPerSec, 8)
}

// juicefs read-ahead (patch 0035): on for near links that are not metered,
// off for far, thin or cost-flagged links, and off with no RTT sample.
func TestJuiceFSReadaheadByLink(t *testing.T) {
	t.Setenv("JM_JFS_READAHEAD_MB", "")
	const mib = 1 << 20
	if got := New().JuiceFSReadaheadMiB(false); got != -1 {
		t.Fatalf("no RTT yet: %d, want -1 (not known)", got)
	}
	p := New()
	for i := 0; i < 20; i++ {
		p.ObserveRTT(12 * time.Millisecond)
	}
	if got := p.JuiceFSReadaheadMiB(false); got != -1 {
		t.Fatalf("RTT but no bandwidth: %d, want -1 (not known)", got)
	}
	sampleFor(p, 40*mib, juiceFSReadaheadMinSamples-1)
	if got := p.JuiceFSReadaheadMiB(false); got != -1 {
		t.Fatalf("%d bandwidth samples: %d, want -1 (not known)", juiceFSReadaheadMinSamples-1, got)
	}
	cases := []struct {
		name string
		rtt  time.Duration
		bw   float64
		want int
	}{
		{"near fast tunnel (medium)", 12 * time.Millisecond, 40 * mib, juiceFSReadaheadMiB},
		{"near, on the slow side of 30 MiB/s", 15 * time.Millisecond, 20 * mib, juiceFSReadaheadMiB},
		{"lan", 500 * time.Microsecond, 400 * mib, juiceFSReadaheadMiB},
		{"near but thin (metered)", 20 * time.Millisecond, 1.5 * mib, 0},
		{"abroad", 200 * time.Millisecond, 3.5 * mib, 0},
		{"abroad and thin", 200 * time.Millisecond, 1 * mib, 0},
		{"queued near link under load", 60 * time.Millisecond, 25 * mib, juiceFSReadaheadMiB},
	}
	for _, c := range cases {
		p := New()
		warm(p, c.rtt, c.bw)
		if got := p.JuiceFSReadaheadMiB(false); got != c.want {
			s := p.Snapshot()
			t.Errorf("%s: %d MiB, want %d (class %s, read class %s, rtt %s, bw %.1f MiB/s)",
				c.name, got, c.want, s.Class, s.ReadClass, s.RTT, s.BytesPerSec/mib)
		}
	}

	p = New()
	warm(p, 12*time.Millisecond, 40*mib)
	metered := ClassMetered
	p.SetNetworkCostClass(&metered)
	if got := p.JuiceFSReadaheadMiB(false); got != 0 {
		t.Fatalf("host marks the link expensive: %d, want 0", got)
	}
	p.SetNetworkCostClass(nil)
	t.Setenv("JM_JFS_READAHEAD_MB", "8")
	if got := p.JuiceFSReadaheadMiB(false); got != 8 {
		t.Fatalf("JM_JFS_READAHEAD_MB=8: %d", got)
	}
	t.Setenv("JM_JFS_READAHEAD_MB", "0")
	if got := p.JuiceFSReadaheadMiB(false); got != 0 {
		t.Fatalf("JM_JFS_READAHEAD_MB=0 (rollback): %d", got)
	}

	t.Setenv("JM_JFS_READAHEAD_MB", "")

	// On: the bandwidth estimate alone does not turn it off (it reads low
	// while juicefs serves the reads); the host cost signal, a forced class
	// and a far RTT do.
	p = New()
	warm(p, 12*time.Millisecond, 40*mib)
	sampleFor(p, 1*mib, 20) // the estimate falls under 3 MiB/s
	if c := p.ReadPolicyClass(); c != ClassMetered {
		t.Fatalf("setup: read class %s, want metered by throughput", c)
	}
	if got := p.JuiceFSReadaheadMiB(false); got != 0 {
		t.Fatalf("off, measured metered: %d, want 0 (it must not turn on)", got)
	}
	if got := p.JuiceFSReadaheadMiB(true); got != juiceFSReadaheadMiB {
		t.Fatalf("on, measured metered: %d, want it kept on", got)
	}
	p.SetNetworkCostClass(&metered)
	if got := p.JuiceFSReadaheadMiB(true); got != 0 {
		t.Fatalf("on, host marks the link expensive: %d, want 0", got)
	}
	p.SetNetworkCostClass(nil)
	p.ForceClass(&metered)
	if got := p.JuiceFSReadaheadMiB(true); got != 0 {
		t.Fatalf("on, forced metered: %d, want 0", got)
	}
	p.ForceClass(nil)
	for i := 0; i < 40; i++ {
		p.ObserveRTT(200 * time.Millisecond)
	}
	if got := p.JuiceFSReadaheadMiB(true); got != 0 {
		t.Fatalf("on, far RTT: %d, want 0", got)
	}
}

// While juicefs reads ahead, the bandwidth estimate (biased low) does not
// make the class metered; the host cost signal still does.
func TestEngineReadaheadKeepsClassOutOfMetered(t *testing.T) {
	const mib = 1 << 20
	p := New()
	warm(p, 12*time.Millisecond, 40*mib)
	p.SetEngineReadahead(true)
	sampleFor(p, 1*mib, 20)
	if c, rc := p.Class(), p.ReadPolicyClass(); c != ClassSlow || rc != ClassSlow {
		t.Fatalf("engine read-ahead on, estimate 1 MiB/s: class %s / read class %s, want slow", c, rc)
	}
	if !p.Snapshot().LowBandwidth {
		t.Fatal("the estimate itself must still report low bandwidth")
	}
	metered := ClassMetered
	p.SetNetworkCostClass(&metered)
	if c := p.Class(); c != ClassMetered {
		t.Fatalf("host cost signal: class %s, want metered", c)
	}
	p.SetNetworkCostClass(nil)
	p.SetEngineReadahead(true) // a second mount
	p.SetEngineReadahead(false)
	if c := p.Class(); c != ClassSlow {
		t.Fatalf("one of two mounts still reads ahead: class %s, want slow", c)
	}
	p.SetEngineReadahead(false)
	p.SetEngineReadahead(false) // extra offs do not go below zero
	if c := p.Class(); c != ClassMetered {
		t.Fatalf("read-ahead off everywhere: class %s, want metered by throughput", c)
	}
	p.SetEngineReadahead(true)
	if c := p.Class(); c != ClassSlow {
		t.Fatalf("on again after extra offs: class %s, want slow", c)
	}
}
