package netprofile

import (
	"testing"
	"time"
)

func TestParseFakeLink(t *testing.T) {
	fl, err := ParseFakeLink("rtt=300ms,bw_down=2Mbps,bw_up=1Mbps,loss=0.5%,class=metered")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if fl.RTT != 300*time.Millisecond || fl.BWDown != 2e6 || fl.BWUp != 1e6 || fl.LossPct != 0.5 || fl.Class != "metered" {
		t.Fatalf("parsed %+v", fl)
	}
	// bare-number bandwidth = bps; unitless rtt = ms; G/K units
	fl, err = ParseFakeLink("rtt=250,bw_down=10Gbps")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if fl.RTT != 250*time.Millisecond || fl.BWDown != 10e9 {
		t.Fatalf("parsed rtt=%v bw=%f", fl.RTT, fl.BWDown)
	}
	for _, bad := range []string{"", "rtt=", "foo=1", "rtt=-5ms", "loss=100", "class=warp", "bw_down=abc"} {
		if _, err := ParseFakeLink(bad); err == nil {
			t.Errorf("spec %q should fail", bad)
		}
	}
}

func TestFakeLinkCanonicalProfile(t *testing.T) {
	fl, err := ParseFakeLink("class=metered,loss=0.5%,bw_up=1Mbps,rtt=300.25ms,bw_down=2Mbps")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	const want = "rtt_ms=300.25,bw_down_bps=2000000,bw_up_bps=1000000,loss_pct=0.5,class=metered"
	if got := fl.CanonicalProfile(); got != want {
		t.Fatalf("canonical profile = %q, want %q", got, want)
	}

	// The canonical form is built only from typed, parsed fields: ordering and
	// spelling of the source string never survive into metrics provenance.
	fl2, err := ParseFakeLink("bw_down=2000000,rtt=300.25,bw_up=1000000,loss=.5,class=metered")
	if err != nil {
		t.Fatalf("parse reordered spec: %v", err)
	}
	if got := fl2.CanonicalProfile(); got != want {
		t.Fatalf("reordered canonical profile = %q, want %q", got, want)
	}
	if got := (*FakeLink)(nil).CanonicalProfile(); got != "" {
		t.Fatalf("nil canonical profile = %q, want empty", got)
	}
}

func TestFakeLinkOverridesRTT(t *testing.T) {
	old := fakeLink
	fakeLink = &FakeLink{RTT: 300 * time.Millisecond, BWDown: 2e6, Class: "metered"}
	defer func() { fakeLink = old }()

	p := New()
	for i := 0; i < 20; i++ {
		p.ObserveRTT(200 * time.Microsecond) // real LAN sample — must be replaced
	}

	p.mu.Lock()
	rtt, highLatency := p.rtt, p.highLatency
	p.mu.Unlock()

	if rtt < 270*time.Millisecond || rtt > 330*time.Millisecond {
		t.Fatalf("rtt = %v, want ~300ms (synthetic override)", rtt)
	}
	if !highLatency {
		t.Fatal("300ms synthetic RTT should trip the high-latency flag (hysteresis exercised)")
	}
}

func TestFakeLinkOverridesThroughput(t *testing.T) {
	old := fakeLink
	fakeLink = &FakeLink{RTT: 300 * time.Millisecond, BWDown: 2e6}
	defer func() { fakeLink = old }()

	p := New()
	// Push enough bytes/time to cross the window flush thresholds.
	p.ObserveThroughput(500<<20, 60*time.Second)

	p.mu.Lock()
	bw, haveBW := p.bwBps, p.haveBW
	p.mu.Unlock()

	if !haveBW {
		t.Fatal("throughput not folded")
	}
	if bw < 1.9e6 || bw > 2.1e6 {
		t.Fatalf("bwBps = %f, want ~2e6 (synthetic override)", bw)
	}
	c, _ := p.classLocked()
	if c != ClassMetered {
		t.Fatalf("class = %v, want metered from synthetic bandwidth", c)
	}
}
