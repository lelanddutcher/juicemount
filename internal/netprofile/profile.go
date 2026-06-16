// Package netprofile is a process-wide, network-aware link estimator.
//
// It answers one question for the rest of the system: "how fast and how far
// is the backend right now?" — and turns that into concrete prefetch/readahead
// policy. There is exactly one network under a running app, so the profile is a
// process singleton (netprofile.Default()), fed from two cheap, passive signals
// that already flow through the hot paths:
//
//   - RTT: the reachability monitor's successful TCP-dial latency
//     (health.Reachability, RFC-6298 smoothed) — folded in via ObserveRTT.
//   - Bandwidth: the duration of real backend block fetches on the read path
//     (cold reads through JuiceFS → MinIO) — folded in via ObserveThroughput.
//
// Both are PASSIVE — we never inject probe traffic onto a link we suspect is
// already degraded (the same non-negotiable the reachability monitor follows).
//
// The motivating measurements (2026-06-15, docs/TUNING/01-bandwidth §Read
// amplification): a 4 KB read of a cold file pulled the whole 53 MB file because
// three link-unaware prefetchers (NFS client readahead, our ReadaheadManager,
// juicefs --prefetch) stack. On cellular that is metered-data ruin; on 10GbE the
// SAME prefetchers are too shallow and starve the pipe (~3 Gbit/s of 10). One
// bandwidth-aware policy fixes both ends: dial DOWN when slow/metered, UP when
// fast.
package netprofile

import (
	"os"
	"strconv"
	"sync"
	"time"
)

// LinkClass is a coarse bucket the readahead policy keys off of. Ordered slow→fast.
type LinkClass int

const (
	ClassMetered LinkClass = iota // <~3 MB/s — cellular / weak; minimize fetch, protect data cap
	ClassSlow                     // ~3–30 MB/s — congested Wi-Fi / modest WAN; gentle prefetch
	ClassMedium                   // ~30–200 MB/s — good Wi-Fi / GbE; current defaults
	ClassFast                     // >~200 MB/s — 10GbE / LAN; aggressive prefetch to fill the pipe
)

func (c LinkClass) String() string {
	switch c {
	case ClassMetered:
		return "metered"
	case ClassSlow:
		return "slow"
	case ClassMedium:
		return "medium"
	case ClassFast:
		return "fast"
	default:
		return "unknown"
	}
}

// ReadaheadPolicy is the concrete knob set the ReadaheadManager consumes. Values
// are chosen so ClassMedium == the historical hard-coded defaults (no LAN
// regression), ClassFast is strictly MORE aggressive (chase 10GbE), and the slow
// classes strictly LESS (stop the WAN whole-file over-fetch).
type ReadaheadPolicy struct {
	Enabled      bool // false → suppress our server-side readahead entirely (metered)
	SeqThreshold int  // consecutive sequential reads before triggering
	Blocks       int  // 4 MB blocks to prefetch ahead once triggered
	Workers      int  // max concurrent prefetch goroutines
}

// Snapshot is an immutable read of the profile for metrics/observability.
type Snapshot struct {
	Class           LinkClass
	RTT             time.Duration // smoothed; 0 if no sample yet
	BytesPerSec     float64       // smoothed cold-read throughput; 0 if no sample yet
	HaveRTT         bool
	HaveBW          bool
	ThroughputN     int64 // count of throughput samples folded in
	BootstrappedRTT bool  // class derived from RTT (no BW yet)
}

// classification thresholds (bytes/sec). Overridable via env for field tuning.
var (
	thrMeteredBps = 3.0 * 1024 * 1024   // < → metered
	thrSlowBps    = 30.0 * 1024 * 1024  // < → slow
	thrFastBps    = 200.0 * 1024 * 1024 // >= → fast; between slow & fast → medium
)

// Profile is the concurrency-safe link estimator. The zero value is not usable;
// construct with New().
type Profile struct {
	mu          sync.RWMutex
	rtt         time.Duration
	rttvar      time.Duration
	haveRTT     bool
	bwBps       float64
	haveBW      bool
	bwSamples   int64
	lastBWAt    time.Time
	forcedClass *LinkClass // operator/OS override (e.g. detected metered link); nil = auto
}

// New builds a fresh profile in the "unknown → medium-safe" state: until a
// sample arrives, policy is the historical defaults so behavior is unchanged.
func New() *Profile { return &Profile{} }

var (
	def     *Profile
	defOnce sync.Once
)

// Default returns the process-wide singleton, applying env overrides once.
func Default() *Profile {
	defOnce.Do(func() {
		def = New()
		applyEnvThresholds()
		// JM_NET_FORCE_CLASS=metered|slow|medium|fast pins the class (test/field).
		if v := os.Getenv("JM_NET_FORCE_CLASS"); v != "" {
			if c, ok := parseClass(v); ok {
				def.forcedClass = &c
			}
		}
	})
	return def
}

func parseClass(s string) (LinkClass, bool) {
	switch s {
	case "metered":
		return ClassMetered, true
	case "slow":
		return ClassSlow, true
	case "medium":
		return ClassMedium, true
	case "fast":
		return ClassFast, true
	}
	return 0, false
}

func applyEnvThresholds() {
	if v := os.Getenv("JM_NET_METERED_MBPS"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			thrMeteredBps = f * 1024 * 1024
		}
	}
	if v := os.Getenv("JM_NET_SLOW_MBPS"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			thrSlowBps = f * 1024 * 1024
		}
	}
	if v := os.Getenv("JM_NET_FAST_MBPS"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			thrFastBps = f * 1024 * 1024
		}
	}
}

// ObserveRTT folds a successful-probe dial latency into the smoothed RTT
// (RFC-6298 style: alpha=1/8, beta=1/4). Cheap; safe from any goroutine.
func (p *Profile) ObserveRTT(sample time.Duration) {
	if sample <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.haveRTT {
		p.rtt = sample
		p.rttvar = sample / 2
		p.haveRTT = true
		return
	}
	diff := p.rtt - sample
	if diff < 0 {
		diff = -diff
	}
	p.rttvar += (diff - p.rttvar) / 4
	p.rtt += (sample - p.rtt) / 8
}

// minThroughputBytes / minThroughputDur gate which read samples count as a real
// backend transfer. A 4 MB cache hit returns in microseconds and would inflate
// the estimate toward "infinitely fast", so we only fold in samples that moved
// enough bytes over enough time to reflect the wire, not the SSD cache.
const (
	minThroughputBytes = 256 * 1024
	minThroughputDur   = 3 * time.Millisecond
)

// ObserveThroughput folds a measured backend transfer (bytes over dur) into the
// smoothed bandwidth estimate. Samples too small/fast to reflect the wire (cache
// hits) are ignored. EWMA alpha=1/4 so the estimate tracks link changes (Wi-Fi↔
// cellular) within a handful of cold blocks without thrashing on one slow chunk.
func (p *Profile) ObserveThroughput(bytes int64, dur time.Duration) {
	if bytes < minThroughputBytes || dur < minThroughputDur {
		return
	}
	bps := float64(bytes) / dur.Seconds()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bwSamples++
	p.lastBWAt = time.Now()
	if !p.haveBW {
		p.bwBps = bps
		p.haveBW = true
		return
	}
	p.bwBps += (bps - p.bwBps) / 4
}

// ForceClass pins the link class (e.g. when the OS reports a metered/expensive
// interface). Pass nil to return to automatic classification.
func (p *Profile) ForceClass(c *LinkClass) {
	p.mu.Lock()
	p.forcedClass = c
	p.mu.Unlock()
}

// class computes the current class under the read lock held by the caller.
func (p *Profile) classLocked() (LinkClass, bool) {
	if p.forcedClass != nil {
		return *p.forcedClass, false
	}
	// Bandwidth is the primary, most trustworthy signal once we have it.
	if p.haveBW {
		switch {
		case p.bwBps < thrMeteredBps:
			return ClassMetered, false
		case p.bwBps < thrSlowBps:
			return ClassSlow, false
		case p.bwBps < thrFastBps:
			return ClassMedium, false
		default:
			return ClassFast, false
		}
	}
	// Bootstrap from RTT before any throughput sample. Conservative: a high-RTT
	// link is assumed slow until bandwidth proves otherwise (safe direction —
	// under-prefetch briefly rather than over-fetch a metered link). A sub-ms
	// RTT is a LAN/10GbE, where aggressive prefetch is exactly what we want.
	if p.haveRTT {
		switch {
		case p.rtt < 2*time.Millisecond:
			return ClassFast, true
		case p.rtt < 20*time.Millisecond:
			return ClassMedium, true
		default:
			return ClassSlow, true
		}
	}
	// No signal at all → historical defaults (medium == unchanged behavior).
	return ClassMedium, false
}

// Class returns the current link class.
func (p *Profile) Class() LinkClass {
	p.mu.RLock()
	defer p.mu.RUnlock()
	c, _ := p.classLocked()
	return c
}

// Readahead maps the current class to concrete prefetch policy. ClassMedium is
// byte-for-byte the historical default; nothing regresses on a normal LAN.
func (p *Profile) Readahead() ReadaheadPolicy {
	switch p.Class() {
	case ClassMetered:
		// Cellular / weak / metered: do NOT prefetch ahead. Serve exactly the
		// block touched (still subject to the 4 MB BlockSize minimum), require a
		// strong sequential signal so an xattr/Quick-Look probe never escalates
		// to a whole-file pull, and protect the user's data cap.
		return ReadaheadPolicy{Enabled: false, SeqThreshold: 6, Blocks: 1, Workers: 1}
	case ClassSlow:
		return ReadaheadPolicy{Enabled: true, SeqThreshold: 4, Blocks: 2, Workers: 2}
	case ClassFast:
		// 10GbE / LAN: go deep + wide to keep enough 4 MB blocks in flight to
		// MinIO to actually fill the pipe (addresses the ~3 Gbit/s-of-10 starve).
		return ReadaheadPolicy{Enabled: true, SeqThreshold: 2, Blocks: 16, Workers: 8}
	default: // ClassMedium — the historical hard-coded defaults
		return ReadaheadPolicy{Enabled: true, SeqThreshold: 3, Blocks: 8, Workers: 4}
	}
}

// Snapshot returns an immutable view for metrics/observability.
func (p *Profile) Snapshot() Snapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	c, boot := p.classLocked()
	return Snapshot{
		Class:           c,
		RTT:             p.rtt,
		BytesPerSec:     p.bwBps,
		HaveRTT:         p.haveRTT,
		HaveBW:          p.haveBW,
		ThroughputN:     p.bwSamples,
		BootstrappedRTT: boot,
	}
}
