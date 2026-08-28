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
	"sort"
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

// ReadaheadPolicy is the concrete knob set the live ReadaheadManager consumes.
// This is the one layer that may scale up after mount: unlike JuiceFS and the
// kernel NFS client, it can react to every RTT/throughput reclassification
// without tearing down Finder's open handles.
type ReadaheadPolicy struct {
	Enabled      bool // false → suppress our server-side readahead entirely (kill-switch/override)
	SeqThreshold int  // consecutive sequential reads before triggering
	Blocks       int  // 4 MB blocks to prefetch ahead once triggered
	Workers      int  // max concurrent prefetch goroutines
}

// JuiceFSPolicy is the juicefs MOUNT-time flag set. These are dominant
// prefetchers (validated 2026-06-15: with server readahead capped at 2 blocks,
// a cold 4 KB read still pulled a whole 64 MB file). Because the flags can only
// change on a disruptive remount, read prefetch is intentionally conservative
// for every startup class; only the live server layer scales upward.
//
// Note --buffer-size also backs write-burst absorption, but writes land on the
// SPOOL first and durability is independent of it (writeback is off), so a
// smaller buffer on a slow link costs only throughput — which is already
// upload-bound there. We never shrink it below a single media file's size.
type JuiceFSPolicy struct {
	BufferSizeMB int // --buffer-size
	Prefetch     int // --prefetch (concurrent blocks)
}

// JuiceFS maps the startup link class to mount-time flags. JuiceFS cannot
// safely retune these after a handoff without a disruptive FUSE remount, so
// prefetch stays at a transition-safe floor for every non-metered class. The
// live server ReadaheadManager is solely responsible for scaling up on a
// measured fast link.
func (p *Profile) JuiceFS() JuiceFSPolicy {
	switch p.Class() {
	case ClassMetered:
		// Minimize readahead + metered-data waste AND bound the shutdown/remount
		// FlushAll (2026-07-10: a correctly-classified 1 GB slow-class buffer took
		// 4m17s to flush dirty data to MinIO over a cellular tunnel when the mount
		// worker exited under drain saturation — turning a routine restart into a
		// ~3-min mount outage). 256 MB still absorbs any single CR3/RAW write:
		// writes land on the SPOOL first (durability is independent of this
		// buffer), so a smaller FUSE buffer only backpressures the drainer to
		// match MinIO throughput — which on a metered link it is already bound to
		// — while capping the worst-case flush to ~256 MB. prefetch 0 kills the
		// concurrent block pull. Field-tunable via JM_JFS_BUFFER_MB.
		return JuiceFSPolicy{BufferSizeMB: 256, Prefetch: 0}
	case ClassSlow:
		// 512 MB (was 1024): a 2× cut to the worst-case shutdown flush with
		// negligible throughput cost on a 3-30 MB/s link (see ClassMetered).
		return JuiceFSPolicy{BufferSizeMB: 512, Prefetch: 1}
	case ClassFast:
		return JuiceFSPolicy{BufferSizeMB: 4096, Prefetch: 1}
	default: // Medium: large write buffer, conservative immutable prefetch floor
		return JuiceFSPolicy{BufferSizeMB: 4096, Prefetch: 1}
	}
}

// NFSReadahead returns the macOS NFS-client `readahead` mount option. This is
// the prefetcher that turns a single small touch into a
// sequential burst (16 × rsize) which juicefs then extends to a whole-file pull
// (validated 2026-06-15: juicefs --buffer-size/--prefetch alone did NOT cap it;
// the client-side fan-out is what triggers juicefs's sequential readahead).
//
// Mount options cannot be changed safely after a Wi-Fi→cellular handoff without
// unmounting Finder. Therefore this layer is deliberately fixed at the metered-
// safe floor for EVERY startup class. The live server ReadaheadManager scales up
// on a measured fast link; the kernel layer never retains a LAN-sized burst after
// a handoff. A smaller value is also strictly safer for the concurrent-read
// truncation family (#18/#19, originally observed at 128).
func (p *Profile) NFSReadahead() int {
	_ = p
	return 2
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

	// Latency thresholds for the high-latency ceiling, with hysteresis so a
	// jittery link cannot flap the class. Above 40ms, round trips—not headline
	// bandwidth—already dominate Finder metadata/open latency, so the link may
	// not retain the LAN/medium prefetch path. Clear only after smoothed RTT is
	// back below 20ms. LAN and normal local Wi-Fi remain comfortably below it.
	rttHighEnter = 40 * time.Millisecond
	rttHighExit  = 20 * time.Millisecond
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
	forcedClass *LinkClass // operator/OS override (e.g. detected metered link); nil = auto

	// highLatency is the LATENCY dimension of classification, kept separate from
	// bandwidth because the two are independent and a link can be fast AND far.
	// Hysteretic: set when smoothed RTT rises above rttHighEnter, cleared only
	// when it falls back under rttHighExit, so a jittery cellular link cannot
	// flap the class (measured on a real Tailscale-over-cellular link: RTT
	// 105-349ms, stddev 86ms — a single threshold would oscillate every probe).
	highLatency bool

	// Windowed throughput accumulator. Bytes are divided by the UNION of active
	// transfer intervals, not the span between the first and last completion.
	// That preserves concurrent aggregate throughput while excluding Finder or
	// an NLE's think time between otherwise-fast reads. The old wall-span
	// denominator turned 256 KiB LAN reads arriving 300 ms apart into a 1.6 MiB/s
	// "metered" link even when each transfer itself sustained 50 MiB/s.
	winStart     time.Time
	winLastEnd   time.Time
	winBytes     int64
	winIntervals []throughputInterval

	// now is the clock (injectable for tests). Production uses time.Now.
	now func() time.Time
}

type throughputInterval struct {
	start time.Time
	end   time.Time
}

// New builds a fresh profile in the "unknown → medium-safe" state: until a
// sample arrives, policy is the historical defaults so behavior is unchanged.
func New() *Profile {
	p := &Profile{now: time.Now}
	applyToProfile(p)
	return p
}

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
		// MASTER KILL-SWITCH (revert safety). JM_NET_ADAPTIVE=0 pins the link class
		// to medium == the historical hard-coded defaults for EVERY consumer
		// (server ReadaheadManager, juicefs --buffer-size/--prefetch, NFS-client
		// readahead). One env var fully disables all #16 adaptive-link behavior and
		// restores byte-for-byte original behavior — the escape hatch for returning
		// to 10GbE if the adaptive (esp. fast-class) policies cause trouble. Takes
		// precedence over JM_NET_FORCE_CLASS. See docs/TUNING/REVERT_LOG.md.
		if os.Getenv("JM_NET_ADAPTIVE") == "0" {
			c := ClassMedium
			def.forcedClass = &c
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
	if fl := FakeLinkSpec(); fl != nil {
		// Simulated link: replace the measurement. A ±10% sawtooth keeps the
		// RTT-variance smoothing and the hysteretic high-latency flag honest
		// under synthetic input (a perfectly constant signal would leave
		// rttvar at its initial value and never exercise the flap logic).
		phase := (time.Now().UnixNano() / int64(500*time.Millisecond)) % 2
		jitter := time.Duration(float64(fl.RTT) * 0.10 * float64(phase))
		sample = fl.RTT + jitter
	}
	if sample <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.haveRTT {
		p.rtt = sample
		p.rttvar = sample / 2
		p.haveRTT = true
		// Evaluate the ceiling on the FIRST sample too. Omitting this meant a
		// link that was already far when the app started never tripped the flag
		// until a second sample arrived — and on a link whose very first probe
		// is 156ms, that is precisely the case we care about.
		p.updateHighLatencyLocked()
		return
	}
	diff := p.rtt - sample
	if diff < 0 {
		diff = -diff
	}
	p.rttvar += (diff - p.rttvar) / 4
	p.rtt += (sample - p.rtt) / 8
	p.updateHighLatencyLocked()
	// Handoff fast-path: EWMA is intentionally stable, but stability is the wrong
	// direction on a LAN→cellular transition. One real far-link probe must clamp
	// speculative reads immediately; otherwise an old 1ms average can take many
	// probe periods to cross the ceiling while Finder keeps the LAN policy.
	if sample >= rttHighEnter {
		p.highLatency = true
	}
}

// updateHighLatencyLocked maintains the hysteretic high-latency flag. Caller
// holds p.mu for writing.
func (p *Profile) updateHighLatencyLocked() {
	if p.highLatency {
		if p.rtt < rttHighExit {
			p.highLatency = false
		}
		return
	}
	if p.rtt >= rttHighEnter {
		p.highLatency = true
	}
}

// HighLatency reports whether the link is currently far, independent of how
// much bandwidth it has. Consumers that care about ROUND TRIPS rather than
// throughput (metadata chatter, per-file opens, cold directory populates)
// should gate on this rather than on Class().
func (p *Profile) HighLatency() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.highLatency
}

// minThroughputBytes / minThroughputDur gate which read samples count as a real
// backend transfer. A 4 MB cache hit returns in microseconds and would inflate
// the estimate toward "infinitely fast", so we only fold in samples that moved
// enough bytes over enough time to reflect the wire, not the SSD cache.
const (
	minThroughputBytes = 256 * 1024
	minThroughputDur   = 3 * time.Millisecond
)

// throughput window tuning.
const (
	// A window flushes to a rate sample once it spans this long OR accumulates
	// this many bytes — whichever first. The time bound keeps a slow link's
	// divisor a real interval (not a sub-ms blip); the byte bound lets a FAST
	// link (where a whole read sequence may be < 120 ms) still register, dividing
	// a healthy multi-MB sum by a small-but-real elapsed.
	bwWindowFlush      = 120 * time.Millisecond
	bwWindowFlushBytes = 8 << 20 // 8 MiB
	// Reads more than this far apart start a fresh window — we don't want an idle
	// gap diluting the aggregate-bytes/wall-time rate.
	bwIdleGap = 400 * time.Millisecond
)

// ObserveThroughput folds a measured backend transfer into the bandwidth
// estimate using aggregate bytes divided by the union of active transfer time.
// Overlapping reads therefore retain their aggregate throughput, while idle
// application think time between reads cannot dilute a fast wire into the
// metered bucket. Cache hits (sub-threshold bytes/dur) are ignored so only wire
// transfers move the estimate. The windowed rate is EWMA-smoothed (alpha=1/4)
// so it tracks link changes within a few windows without thrashing.
//
// `dur` is the read's own duration, used both as the cache-hit filter and to
// anchor a fresh window's start (the read began ~dur before now).
func (p *Profile) ObserveThroughput(bytes int64, dur time.Duration) {
	if bytes < minThroughputBytes || dur < minThroughputDur {
		return
	}
	now := p.now()
	started := now.Add(-dur)
	p.mu.Lock()
	defer p.mu.Unlock()

	// Start a fresh window if this is the first sample or the link went idle.
	if p.winStart.IsZero() || now.Sub(p.winLastEnd) > bwIdleGap {
		p.winStart = started
		p.winLastEnd = time.Time{}
		p.winBytes = 0
		p.winIntervals = p.winIntervals[:0]
	}
	if started.Before(p.winStart) {
		p.winStart = started
	}
	p.winBytes += bytes
	if now.After(p.winLastEnd) {
		p.winLastEnd = now
	}
	p.winIntervals = append(p.winIntervals, throughputInterval{start: started, end: now})

	span := p.winLastEnd.Sub(p.winStart)
	if span < bwWindowFlush && p.winBytes < bwWindowFlushBytes {
		return // keep accumulating; not enough wall-time or bytes yet
	}
	active := activeTransferDuration(p.winIntervals)
	if active <= 0 {
		return // guard against a zero divisor (clock didn't advance)
	}
	rate := float64(p.winBytes) / active.Seconds()
	if fl := FakeLinkSpec(); fl != nil && fl.BWDown > 0 {
		rate = fl.BWDown
	}
	// Tumble the window so concurrent reads in the next interval aren't
	// double-counted against this one's already-folded bytes.
	p.winStart = time.Time{}
	p.winLastEnd = time.Time{}
	p.winBytes = 0
	p.winIntervals = p.winIntervals[:0]

	p.bwSamples++
	if !p.haveBW {
		p.bwBps = rate
		p.haveBW = true
		return
	}
	p.bwBps += (rate - p.bwBps) / 4
}

// activeTransferDuration returns the union of the supplied intervals. The
// window is capped by bwWindowFlushBytes (at most 32 minimum-sized samples), so
// sorting this tiny slice keeps the accounting exact without putting a
// persistent timer or probe on the read path.
func activeTransferDuration(intervals []throughputInterval) time.Duration {
	if len(intervals) == 0 {
		return 0
	}
	sort.Slice(intervals, func(i, j int) bool {
		if intervals[i].start.Equal(intervals[j].start) {
			return intervals[i].end.Before(intervals[j].end)
		}
		return intervals[i].start.Before(intervals[j].start)
	})
	start, end := intervals[0].start, intervals[0].end
	var total time.Duration
	for _, interval := range intervals[1:] {
		if interval.start.After(end) {
			total += end.Sub(start)
			start, end = interval.start, interval.end
			continue
		}
		if interval.end.After(end) {
			end = interval.end
		}
	}
	return total + end.Sub(start)
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
		byBW := ClassFast
		switch {
		case p.bwBps < thrMeteredBps:
			byBW = ClassMetered
		case p.bwBps < thrSlowBps:
			byBW = ClassSlow
		case p.bwBps < thrFastBps:
			byBW = ClassMedium
		}
		// LATENCY CEILING — bandwidth alone is not the link.
		//
		// This branch used to return byBW directly, and RTT was never consulted
		// again once a single throughput sample existed. Measured live on
		// 2026-07-29 over Tailscale-on-cellular:
		//
		//	route utun9 · real RTT 156ms avg (105-349, stddev 86)
		//	netprofile: class=fast  bandwidth=518.9 Mbps  rtt_ms=105
		//
		// It HELD a 105ms RTT and still said "fast", because a phone tunnel can
		// carry real throughput while every round trip costs 100ms+. Eight
		// protections gate on this class, and the synchronous cold-directory
		// populate is gated to ClassFast specifically — so a link that could not
		// be less LAN-like was getting the LAN foreground path.
		//
		// A far link is capped at ClassSlow no matter how fat the pipe is:
		// what makes navigation and file opens hurt is ROUND TRIPS, not
		// megabits, and every consumer of "slow" is doing round-trip-reducing
		// work. Metered stays metered (it is already stricter). Kill switch
		// JM_NET_LATENCY_CEILING=0 restores bandwidth-only classification
		// byte-identically (WAN-tuning revert discipline).
		// LinkClass is ordered slow→fast (Metered=0 … Fast=3), so "faster than
		// Slow" is >, and Metered (which is already stricter) is left alone.
		if p.highLatency && latencyCeilingEnabled() && byBW > ClassSlow {
			return ClassSlow, false
		}
		return byBW, false
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
		// Cellular / weak / metered: keep one bounded 8 MiB window in
		// flight after a strong sequential signal. Disabling this layer entirely
		// serialized foreground subreads across the tunnel: a live 40 Mbit/s,
		// 80 ms shaped Link delivered only ~7.9 Mbit/s. One worker avoids widening
		// the proven FUSE safety ceiling while two consecutive blocks eliminate
		// gaps between short prefetch rounds. The six-block guard below still has
		// to be crossed and caps speculative exposure at 8 MiB.
		return ReadaheadPolicy{Enabled: true, SeqThreshold: 6, Blocks: 2, Workers: 1}
	case ClassSlow:
		return ReadaheadPolicy{Enabled: true, SeqThreshold: 4, Blocks: 2, Workers: 2}
	case ClassFast:
		// 10GbE / LAN: go deep + wide to keep enough 4 MB blocks in flight to
		// MinIO to actually fill the pipe (addresses the ~3 Gbit/s-of-10 starve).
		return ReadaheadPolicy{Enabled: true, SeqThreshold: 2, Blocks: 16, Workers: 8}
	default: // ClassMedium — bounded; Fast alone earns the deep/wide path
		return ReadaheadPolicy{Enabled: true, SeqThreshold: 3, Blocks: 4, Workers: 2}
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

// latencyCeilingEnabled reports whether the RTT ceiling may downgrade a
// bandwidth-derived class. Default ON; JM_NET_LATENCY_CEILING=0 restores
// bandwidth-only classification byte-identically, per the WAN-tuning revert
// discipline (a cellular tuning change must always be revertible without a
// rebuild).
func latencyCeilingEnabled() bool {
	return os.Getenv("JM_NET_LATENCY_CEILING") != "0"
}
