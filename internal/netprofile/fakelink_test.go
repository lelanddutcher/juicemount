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
