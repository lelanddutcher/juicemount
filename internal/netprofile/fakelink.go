package netprofile

// FakeLink (dev feature): simulate a constrained link WITHOUT shaping packets.
//
//	JM_NET_FAKE_LINK="rtt=300ms,bw_down=2Mbps,bw_up=1Mbps,loss=0.5%"
//
// When set, every measured signal entering the profile is REPLACED by the
// synthetic value, so all adaptive policies downstream (buffer sizing,
// readahead, drain gating, cold-populate skip, acdir choices) behave exactly
// as they would on that link while the actual network stays fast. This is
// decision-level simulation: it validates POLICY instantly and free. It does
// NOT validate real wire timing (timeouts/retries through actual pain) — use
// true packet shaping or a real hotspot for that before trusting numbers.
//
// Guardrail (refuse-to-report doctrine): FakeLinkActive() is exported so any
// benchmark/harness must check it and label its output "simulated" loudly.
// The loader logs a prominent warning once per process.

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FakeLink is the parsed form of JM_NET_FAKE_LINK.
type FakeLink struct {
	RTT     time.Duration
	BWDown  float64 // bits per second
	BWUp    float64 // bits per second
	LossPct float64
	Class   string // optional explicit class override: metered|slow|medium|fast
}

var (
	fakeOnce    sync.Once
	fakeLink    *FakeLink
	fakeLoadErr error
)

func loadFakeLink() {
	fakeOnce.Do(func() {
		spec := os.Getenv("JM_NET_FAKE_LINK")
		if spec == "" {
			return
		}
		fl, err := ParseFakeLink(spec)
		if err != nil {
			fakeLoadErr = err
			log.Printf("[netprofile] FAKE LINK SPEC INVALID (%v) — running on real measurements", err)
			return
		}
		fakeLink = fl
		log.Printf("[netprofile] *** SIMULATED LINK ACTIVE *** rtt=%v bw_down=%.2fMbps bw_up=%.2fMbps loss=%.2f%% class=%q — "+
			"all adaptive decisions are running against SYNTHETIC measurements; do not report real-link numbers from this process",
			fl.RTT, fl.BWDown/1e6, fl.BWUp/1e6, fl.LossPct, fl.Class)
	})
}

// ParseFakeLink parses the comma-separated k=v spec. Units: rtt in
// ms/s; bandwidth in Kbps/Mbps/Gbps (bare number = bps); loss in percent;
// class one of metered|slow|medium|fast.
func ParseFakeLink(spec string) (*FakeLink, error) {
	fl := &FakeLink{}
	sawAny := false
	for _, part := range strings.Split(spec, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || k == "" || v == "" {
			return nil, fmt.Errorf("bad token %q (want k=v)", part)
		}
		sawAny = true
		var err error
		switch strings.ToLower(k) {
		case "rtt":
			fl.RTT, err = parseDuration(v)
			if err == nil && fl.RTT <= 0 {
				err = fmt.Errorf("rtt must be > 0")
			}
		case "bw_down", "bw_up":
			bps, e := parseBandwidth(v)
			if e != nil {
				err = e
			} else if k == "bw_down" {
				fl.BWDown = bps
			} else {
				fl.BWUp = bps
			}
		case "loss":
			v = strings.TrimSuffix(v, "%")
			fl.LossPct, err = strconv.ParseFloat(v, 64)
			if err == nil && (fl.LossPct < 0 || fl.LossPct >= 100) {
				err = fmt.Errorf("loss must be in [0,100)")
			}
		case "class":
			switch strings.ToLower(v) {
			case "metered", "slow", "medium", "fast":
				fl.Class = strings.ToLower(v)
			default:
				err = fmt.Errorf("unknown class %q", v)
			}
		default:
			err = fmt.Errorf("unknown key %q", k)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", k, err)
		}
	}
	if !sawAny {
		return nil, fmt.Errorf("empty spec")
	}
	return fl, nil
}

func parseDuration(v string) (time.Duration, error) {
	if ms, err := strconv.ParseFloat(v, 64); err == nil {
		return time.Duration(ms * float64(time.Millisecond)), nil
	}
	return time.ParseDuration(v)
}

func parseBandwidth(v string) (float64, error) {
	upper := strings.ToUpper(v)
	mult := 1.0
	switch {
	case strings.HasSuffix(upper, "GBPS"), strings.HasSuffix(upper, "G"):
		mult, upper = 1e9, trimUnit(upper, []string{"GBPS", "G"})
	case strings.HasSuffix(upper, "MBPS"), strings.HasSuffix(upper, "M"):
		mult, upper = 1e6, trimUnit(upper, []string{"MBPS", "M"})
	case strings.HasSuffix(upper, "KBPS"), strings.HasSuffix(upper, "K"):
		mult, upper = 1e3, trimUnit(upper, []string{"KBPS", "K"})
	}
	f, err := strconv.ParseFloat(upper, 64)
	if err != nil {
		return 0, fmt.Errorf("bandwidth %q: %w", v, err)
	}
	return f * mult, nil
}

func trimUnit(s string, suffixes []string) string {
	for _, suf := range suffixes {
		if strings.HasSuffix(s, suf+"BPS") || s == suf {
			return strings.TrimSuffix(strings.TrimSuffix(s, "BPS"), suf)
		}
	}
	return strings.TrimSuffix(strings.TrimSuffix(s, "BPS"), suffixes[len(suffixes)-1])
}

// FakeLinkActive reports whether a simulated link is loaded. Harnesses MUST
// check this and label results "simulated".
func FakeLinkActive() bool {
	loadFakeLink()
	return fakeLink != nil
}

// FakeLinkSpec returns the parsed synthetic link, or nil.
func FakeLinkSpec() *FakeLink {
	loadFakeLink()
	return fakeLink
}

// FakeLinkLoadErr surfaces a malformed spec to callers that want to warn.
func FakeLinkLoadErr() error {
	loadFakeLink()
	return fakeLoadErr
}

// applyToProfile seeds forcedClass from the fake spec (if provided) at
// construction. Called from New() after default init; harmless when unset.
func applyToProfile(p *Profile) {
	loadFakeLink()
	if fakeLink == nil || p == nil || fakeLink.Class == "" {
		return
	}
	var c LinkClass
	switch fakeLink.Class {
	case "metered":
		c = ClassMetered
	case "slow":
		c = ClassSlow
	case "fast":
		c = ClassFast
	default:
		c = ClassMedium
	}
	p.forcedClass = &c
}
