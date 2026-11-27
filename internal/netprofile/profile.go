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
	// ClassMetered means the link COSTS data: the host OS marks the path
	// expensive or constrained (cellular, a phone hotspot, Low Data Mode), an
	// operator forces it, or a NEAR link measures under ~3 MB/s. A FAR link
	// (high RTT) that measures under ~3 MB/s is ClassSlow with LowBandwidth
	// set, not metered: see slowRTTFloor in classLocked.
	ClassMetered LinkClass = iota // cost; minimize fetch, protect data cap
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
	// ParallelFetch selects the windowed prefetch: block-aligned, each block
	// fetched once, up to Workers blocks of one stream in flight together.
	// false keeps the historical serial round (one 4 MB buffer at a time).
	ParallelFetch bool
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
	switch p.ReadPolicyClass() {
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

// JuiceFSReadaheadMiB is the read-ahead of one sequential reader inside
// juicefs, in MiB (0 = off; -1 = not known yet). The mount keeps
// --max-readahead 1M (off); the live link follower (nfs/jfs_link_policy.go, juicefs patch 0035) sets this
// value while the volume is mounted, so it can never outlive a handoff.
//
// Why juicefs and not the NFS layer: macFUSE runs one read of a file at a
// time (nfs/demand_first.go rule 5), so the NFS client's readahead and the
// NFS layer's ReadaheadManager cannot put more than one demand span of a
// file on the link at once. A 12 ms, 300 Mbit/s tunnel read one file at
// 128-156 Mbit/s on the laptop (2026-11-26); juicefs's own read-ahead
// fetches whole blocks ahead of the reader in parallel and is the one layer
// that survives both macFUSE and a later engine-direct front.
//
// On while the link is near and not metered: smoothed RTT under
// juiceFSReadaheadMaxRTT and neither class nor read class metered. Not known
// (-1) until the link has an RTT and juiceFSReadaheadMinSamples bandwidth
// samples: before that a thin link looks near and unmetered. The follower
// keeps read-ahead off while it is not known, and changes it only after the
// same answer held for 10 s (nfs/jfs_link_policy.go readaheadHold). Slow
// counts: a near link at 150-250 Mbit/s alternates slow/medium on the
// 30 MiB/s line, and it is exactly the link that needs it. Far links
// (abroad) and metered or thin links keep today's behaviour. The juicefs
// side starts a reader's read-ahead only after two blocks of contiguous
// reading, so previews and scrub jumps pull nothing extra.
//
// on says whether juicefs read-ahead is on now. While it is, a metered
// verdict that comes only from the bandwidth estimate does not turn it off:
// with read-ahead most reads are served from juicefs's memory in under
// minThroughputDur and are not counted, and the reads that are counted are
// the ones that waited for a block still in flight, so the estimate falls
// far below the link (lab, 2026-11-27: 250 -> 20 Mbit/s in 30 s while the
// file arrived at 160 Mbit/s). The host's cost signal, a forced class and a
// far RTT still turn it off, and turning it on still needs a not-metered
// measurement taken with it off.
// JM_JFS_READAHEAD_MB overrides the size; 0 turns it off everywhere.
func (p *Profile) JuiceFSReadaheadMiB(on bool) int {
	size := juiceFSReadaheadMiB
	if v := os.Getenv("JM_JFS_READAHEAD_MB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			// juicefs refuses more than 1 GiB, and with it the whole
			// policy message (timeouts and demand-first too).
			size = min(n, juiceFSReadaheadMaxMiB)
		}
	}
	if size == 0 {
		return 0
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	explicitMetered := p.forcedClass != nil && *p.forcedClass == ClassMetered ||
		p.forcedClass == nil && p.networkCostClass != nil && *p.networkCostClass == ClassMetered
	measuredMetered := p.haveBW && p.bwBps < thrMeteredBps
	known := p.haveRTT && p.haveBW && p.bwSamples >= juiceFSReadaheadMinSamples
	switch {
	case explicitMetered:
		return 0
	case !known:
		return -1
	case p.rtt >= juiceFSReadaheadMaxRTT:
		return 0
	case measuredMetered && !on:
		return 0
	}
	return size
}

// SetEngineReadahead records that juicefs read-ahead turned on (true) or off
// (false) on one mount; the link follower calls it on each change.
func (p *Profile) SetEngineReadahead(on bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if on {
		p.engineReadahead++
	} else if p.engineReadahead > 0 {
		p.engineReadahead--
	}
}

// juiceFSReadaheadMiB and juiceFSReadaheadMaxRTT: see JuiceFSReadaheadMiB.
// 16 MiB is four blocks in flight. Lab, 12 ms / 300 Mbit/s tunnel, macFUSE
// model (2026-11-27): 16 MiB read 198-215 Mbit/s, 32 MiB 138-140 (eight
// blocks share the link evenly, so the block the reader needs next lands
// late and the reader moves in bursts), off 122-130. 100 ms sits far from both the near links (12-30 ms under load) and the
// abroad profile (200 ms idle, 270 ms reading), so load-induced queueing
// does not flip it the way it flips the 40 ms high-latency flag.
const (
	juiceFSReadaheadMiB        = 16
	juiceFSReadaheadMaxRTT     = 100 * time.Millisecond
	juiceFSReadaheadMinSamples = 4
	juiceFSReadaheadMaxMiB     = 1024
)

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
	// LowBandwidth is true when the measured throughput is under the metered
	// threshold, whatever the class. On a far link the class stays slow (see
	// classLocked) and ReadClass is metered.
	LowBandwidth bool
	// ReadClass is ReadPolicyClass: the class read-ahead and prefetch use.
	ReadClass LinkClass
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
	mu               sync.RWMutex
	rtt              time.Duration
	rttvar           time.Duration
	haveRTT          bool
	bwBps            float64
	haveBW           bool
	bwSamples        int64
	forcedClass      *LinkClass // explicit operator/test override; nil = auto
	networkCostClass *LinkClass // host OS expensive/constrained signal; nil = no cost override
	ceilingClass     *LinkClass // route safety ceiling (a failed route lookup); nil = uncapped
	// tunnelRoute: the route to the backend crosses a tunnel (utun,
	// tailscale0). routeIface is the interface of the last route lookup.
	// See SetBackendRoute and tunnelCeilingLocked.
	tunnelRoute bool
	routeIface  string
	// engineReadahead counts the juicefs mounts whose read-ahead is on now
	// (SetEngineReadahead). While it is above 0 the bandwidth estimate does
	// not make the class metered: see classLocked.
	engineReadahead int

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
	// winCarry is set while the window continues the one flushed just
	// before it (no idle gap between them). Its intervals are clipped at
	// winStart, the previous flush, so time already divided into the last
	// sample is not divided again. winMaxDur is the longest read in it.
	winCarry  bool
	winMaxDur time.Duration

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

// NewWithClock is New with an injected clock for the throughput windows
// (tests in other packages that drive the profile with a fake clock).
func NewWithClock(now func() time.Time) *Profile {
	p := New()
	p.now = now
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
	// bwCarryMaxWait: a continued window waits to span a read up to this
	// long; a longer read counts only its share inside the window.
	bwCarryMaxWait = 2 * time.Second
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
	carry := bwWindowCarryEnabled()
	p.mu.Lock()
	defer p.mu.Unlock()

	// Start a fresh window if this is the first sample or the link went idle:
	// this read started more than bwIdleGap after the last one ended. (A
	// long read that started while others were still running is not idle
	// time, however long ago the last one completed.) Without carry (the
	// kill switch) idle is measured from this read's end, as before.
	idle := started.Sub(p.winLastEnd) > bwIdleGap
	if !carry {
		idle = now.Sub(p.winLastEnd) > bwIdleGap
	}
	if p.winStart.IsZero() || idle {
		p.winStart = started
		p.winLastEnd = time.Time{}
		p.winBytes = 0
		p.winIntervals = p.winIntervals[:0]
		p.winCarry = false
		p.winMaxDur = 0
	}
	if started.Before(p.winStart) {
		if p.winCarry {
			// CONTINUED WINDOW: the part of this read before winStart was
			// already in the previous window's divisor (as the time of the
			// reads that completed then). Dividing it again made every
			// window's divisor about one read duration too long. With reads
			// longer than bwWindowFlush the first read of each window
			// flushed it alone, so the estimate was one request's rate,
			// not the link's: a 12 ms tunnel carrying 190-240 Mbit/s of
			// concurrent reads measured 70-120 Mbit/s (lab, 2026-11-26).
			//
			// A read longer than bwCarryMaxWait (a stalled GET) counts only
			// the share of its bytes inside this window. Counting it whole
			// would land a long read's bytes in one short window (a spike
			// that could lift a thin link out of metered); waiting for the
			// window to span it would freeze the estimate for the read's
			// whole length, just when the link is degrading.
			if dur > bwCarryMaxWait {
				bytes = int64(float64(bytes) * float64(now.Sub(p.winStart)) / float64(dur))
			}
			started = p.winStart
		} else {
			p.winStart = started
		}
	}
	p.winBytes += bytes
	if now.After(p.winLastEnd) {
		p.winLastEnd = now
	}
	if d := min(dur, bwCarryMaxWait); d > p.winMaxDur {
		p.winMaxDur = d
	}
	p.winIntervals = append(p.winIntervals, throughputInterval{start: started, end: now})

	span := p.winLastEnd.Sub(p.winStart)
	if span < bwWindowFlush && p.winBytes < bwWindowFlushBytes {
		return // keep accumulating; not enough wall-time or bytes yet
	}
	if p.winCarry && span < p.winMaxDur {
		// A continued window counts a clipped read's whole bytes (reads up
		// to bwCarryMaxWait), so it must be at least as long as that read.
		// Counting only the share inside the window instead loses the part
		// of every read still in flight at each flush: 4 reads of 300 ms in
		// flight (13.3 MiB/s) then measure 5 MiB/s.
		return
	}
	active := activeTransferDuration(p.winIntervals)
	if active <= 0 {
		return // guard against a zero divisor (clock didn't advance)
	}
	rate := float64(p.winBytes) / active.Seconds()
	if fl := FakeLinkSpec(); fl != nil && fl.BWDown > 0 {
		rate = fl.BWDown
	}
	// Tumble the window. The next one continues from here (winCarry) until
	// the link goes idle: its reads are clipped at this flush, so neither
	// their bytes nor their time is counted twice.
	p.winStart = now
	p.winLastEnd = now
	p.winBytes = 0
	p.winIntervals = p.winIntervals[:0]
	p.winCarry = true
	p.winMaxDur = 0
	if !carry {
		// JM_NET_BW_WINDOW_CARRY=0: each window starts at its first read.
		p.winStart, p.winLastEnd, p.winCarry = time.Time{}, time.Time{}, false
	}

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

// ForceClass pins the link class for an explicit operator or test override.
// Pass nil to return to host-cost/automatic classification.
func (p *Profile) ForceClass(c *LinkClass) {
	p.mu.Lock()
	p.forcedClass = c
	p.mu.Unlock()
}

// ForcedClass reports the explicit ForceClass override, if one is set. It is
// read-only: tests use it to pin a class and then restore the caller's prior
// override instead of clearing it.
func (p *Profile) ForcedClass() (LinkClass, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.forcedClass == nil {
		return 0, false
	}
	return *p.forcedClass, true
}

// SetNetworkCostClass records the host OS's expensive/constrained path signal
// without sharing the explicit-operator ForceClass slot. Explicit force keeps
// precedence; in automatic mode a positive cost signal is authoritative until
// the OS clears it. Pass nil to clear the signal.
func (p *Profile) SetNetworkCostClass(c *LinkClass) {
	p.mu.Lock()
	p.networkCostClass = c
	p.mu.Unlock()
}

// CeilingClass caps automatic classification without pinning it. The route
// lookup uses it when it fails before any success (health.RouteCeiling).
// Metered observations remain Metered, and an explicit ForceClass still wins.
// Pass nil to remove the cap.
func (p *Profile) CeilingClass(c *LinkClass) {
	p.mu.Lock()
	p.ceilingClass = c
	p.mu.Unlock()
}

// SetBackendRoute records the interface a successful route lookup gave for
// the backend and whether it is a tunnel (health.ApplyBackendRouteCeiling).
// See tunnelCeilingLocked for what a tunnel caps.
//
// When the tunnel turns on, or the interface changes, the bandwidth
// measurements are dropped: they belong to the old path. Otherwise a Mac
// measured on its home LAN that moved to an unflagged network behind a
// tunnel kept the LAN estimate and its 4 samples, so the tunnel was medium
// at once and stayed there until the estimate decayed (PR #110 review H1).
// The RTT stays: its hysteresis and the one-sample far-link clamp handle a
// path change, and a far probe sets the high-latency flag at once.
//
// Known limit: a change of underlay under the same tunnel interface (home
// fiber to an unflagged hotspot, Tailscale keeping utunN) is not a route
// change, so nothing resets; the estimate has to decay.
func (p *Profile) SetBackendRoute(iface string, tunnel bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if (tunnel && !p.tunnelRoute) || (p.routeIface != "" && iface != p.routeIface) {
		p.resetBandwidthLocked()
	}
	p.tunnelRoute, p.routeIface = tunnel, iface
}

// resetBandwidthLocked drops the bandwidth estimate and the open window.
func (p *Profile) resetBandwidthLocked() {
	p.bwBps = 0
	p.haveBW = false
	p.bwSamples = 0
	p.winStart = time.Time{}
	p.winLastEnd = time.Time{}
	p.winBytes = 0
	p.winIntervals = p.winIntervals[:0]
	p.winCarry = false
	p.winMaxDur = 0
}

// tunnelMeasuredSamples is how many bandwidth samples a tunnel needs before
// its class follows the measurement.
const tunnelMeasuredSamples = 4

// tunnelCeilingLocked is the cap a tunnel route puts on automatic
// classification, or nil.
//
// Before the link is measured, a tunnel is capped at slow. A nearby tunnel
// endpoint can answer in 2 ms while the physical underlay is a phone hotspot,
// and Network.framework does not reliably mark every VPN as expensive, so an
// RTT bootstrap through a tunnel must not earn the medium or fast policy.
//
// Once tunnelMeasuredSamples bandwidth samples exist, the class follows the
// measurement, capped at medium. A thin tunnel is still metered (near and
// under 3 MiB/s) or slow (far, or under 30 MiB/s) by its own numbers. A fast
// one is not called slow for being a tunnel: a direct Tailscale path on
// fiber (12 ms, about 300 Mbit/s) was classed like a 200 ms link abroad (real
// remote run, 2026-11-26). Fast stays for routes without a tunnel.
// JM_NET_TUNNEL_MEASURED=0 restores the fixed slow cap.
func (p *Profile) tunnelCeilingLocked() *LinkClass {
	if !p.tunnelRoute {
		return nil
	}
	c := ClassSlow
	if p.haveBW && p.bwSamples >= tunnelMeasuredSamples && tunnelMeasuredEnabled() {
		c = ClassMedium
	}
	return &c
}

// RouteCeiling reports the cap the backend route puts on the class now, if
// any (for diagnostics).
func (p *Profile) RouteCeiling() (LinkClass, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if c := p.routeCeilingLocked(); c != nil {
		return *c, true
	}
	return 0, false
}

// routeCeilingLocked is the stricter of the explicit ceiling and the tunnel
// ceiling.
func (p *Profile) routeCeilingLocked() *LinkClass {
	c := p.ceilingClass
	if t := p.tunnelCeilingLocked(); t != nil && (c == nil || *t < *c) {
		c = t
	}
	return c
}

// ResetForTransportChange invalidates every passive measurement tied to the
// previous physical path. An adaptive Link proxy can move between an encrypted
// tunnel and a verified direct-LAN connection without restarting the process;
// carrying the old path's bandwidth EWMA across that boundary made the first
// LAN cold read retain cellular readahead (and the reverse handoff could retain
// LAN optimism). With no fresh samples Class() returns the transition-safe
// medium policy until the existing RTT/read observers classify the new path.
//
// Explicit operator force and host-cost state are deliberately preserved. A
// metered interface remains metered across proxy reconnects, while automatic
// mode forgets only measurements, not configuration.
func (p *Profile) ResetForTransportChange() {
	p.mu.Lock()
	p.rtt = 0
	p.rttvar = 0
	p.haveRTT = false
	p.bwBps = 0
	p.haveBW = false
	p.bwSamples = 0
	p.highLatency = false
	p.winStart = time.Time{}
	p.winLastEnd = time.Time{}
	p.winBytes = 0
	p.winIntervals = p.winIntervals[:0]
	p.winCarry = false
	p.winMaxDur = 0
	p.mu.Unlock()
}

// class computes the current class under the read lock held by the caller.
func (p *Profile) classLocked() (LinkClass, bool) {
	if p.forcedClass != nil {
		return *p.forcedClass, false
	}
	if p.networkCostClass != nil {
		return *p.networkCostClass, false
	}
	ceiling := p.routeCeilingLocked()
	bounded := func(class LinkClass, boot bool) (LinkClass, bool) {
		if ceiling != nil && class > *ceiling {
			return *ceiling, boot
		}
		return class, boot
	}
	// Bandwidth is the primary, most trustworthy signal once we have it.
	if p.haveBW {
		byBW := ClassFast
		switch {
		case p.bwBps < thrMeteredBps:
			byBW = ClassMetered
			// SLOW-RTT FLOOR: low bandwidth on a far link is not a cost.
			//
			// Metered exists to protect a data cap. The host OS reports
			// that directly (networkCostClass, checked above), and an
			// operator can force it. The throughput rule is the fallback
			// for a near link that carries little, such as a VPN whose
			// cellular underlay the OS does not flag. A far link (RTT
			// above the high-latency ceiling) is already ClassSlow by its
			// RTT, and a cold read on it measures under 3 MB/s because
			// one TCP stream over 200 ms cannot carry more: it says how
			// far the server is, not what the data costs. Measured on
			// the founder's laptop 2026-11-11 (abroad, 200 ms, main
			// 47e140a): the first cold reads flipped slow to metered, the
			// FUSE data gate went from width 8 to 2, the drain kept one
			// slot, and 2 of 201 saved files reached the server in
			// 10 minutes (148 before). So a far link stays slow and
			// reports LowBandwidth, for the drain and the gate width.
			// Speculative downloads still follow the metered policy:
			// see ReadPolicyClass. JM_NET_SLOW_RTT_FLOOR=0 restores the
			// throughput-only metered rule.
			if p.highLatency && slowRTTFloorEnabled() {
				byBW = ClassSlow
			}
			// ENGINE READ-AHEAD: while juicefs reads ahead for a reader
			// (JuiceFSReadaheadMiB), most reads are served from its
			// memory too fast to count, and the counted ones waited for a
			// block in flight, so the estimate reads far below the link
			// (lab 2026-11-27: 250 -> 20 Mbit/s while the file arrived at
			// 160). That is not a thin link: the class stays slow. The
			// host's cost signal still makes it metered (checked above).
			if p.engineReadahead > 0 {
				byBW = ClassSlow
			}
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
		// work. Metered stays metered (it is already stricter; a far link
		// never reaches here as metered by throughput, see SLOW-RTT FLOOR). Kill switch
		// JM_NET_LATENCY_CEILING=0 restores bandwidth-only classification
		// byte-identically (WAN-tuning revert discipline).
		// LinkClass is ordered slow→fast (Metered=0 … Fast=3), so "faster than
		// Slow" is >, and Metered (which is already stricter) is left alone.
		if p.highLatency && latencyCeilingEnabled() && byBW > ClassSlow {
			return bounded(ClassSlow, false)
		}
		return bounded(byBW, false)
	}
	// Bootstrap from RTT before any throughput sample. Conservative: a high-RTT
	// link is assumed slow until bandwidth proves otherwise (safe direction —
	// under-prefetch briefly rather than over-fetch a metered link). A sub-ms
	// RTT is a LAN/10GbE, where aggressive prefetch is exactly what we want.
	if p.haveRTT {
		switch {
		case p.rtt < 2*time.Millisecond:
			return bounded(ClassFast, true)
		case p.rtt < 20*time.Millisecond:
			return bounded(ClassMedium, true)
		default:
			return bounded(ClassSlow, true)
		}
	}
	// No signal at all → historical defaults (medium == unchanged behavior).
	return bounded(ClassMedium, false)
}

// Class returns the current link class.
func (p *Profile) Class() LinkClass {
	p.mu.RLock()
	defer p.mu.RUnlock()
	c, _ := p.classLocked()
	return c
}

// ReadPolicyClass is the class that decides how much the app may download
// speculatively (read-ahead, JuiceFS prefetch). It is Class(), except on a
// link the slow-RTT floor kept at ClassSlow (far, measured under 3 MiB/s, no
// cost signal): there it is ClassMetered.
//
// The floor exists so the DRAIN and the FUSE gate width are not cut on a far
// link. It must not open speculative downloads on a link that may still cost
// money: cellular without the host's expensive flag (an Android or MiFi
// hotspot, a cellular router, in-flight Wi-Fi) is also far and thin, typically
// 60-150 ms at about 1 MiB/s, and the throughput rule was its only protection.
// So a far, thin link keeps the metered read policy (one worker, one block,
// prefetch 0) and the slow drain and gate width.
func (p *Profile) ReadPolicyClass() LinkClass {
	p.mu.RLock()
	defer p.mu.RUnlock()
	c, _ := p.classLocked()
	if c == ClassSlow && p.slowRTTFloorAppliedLocked() {
		return ClassMetered
	}
	return c
}

// slowRTTFloorAppliedLocked reports whether automatic classification would be
// ClassMetered by throughput but the slow-RTT floor kept it at ClassSlow.
// Explicit force and the host cost class decide the class themselves.
func (p *Profile) slowRTTFloorAppliedLocked() bool {
	return p.forcedClass == nil && p.networkCostClass == nil &&
		p.haveBW && p.bwBps < thrMeteredBps && p.highLatency && slowRTTFloorEnabled()
}

// Readahead maps the current class to concrete prefetch policy. ClassMedium is
// byte-for-byte the historical default; nothing regresses on a normal LAN.
func (p *Profile) Readahead() ReadaheadPolicy {
	switch p.ReadPolicyClass() {
	case ClassMetered:
		// Cellular / weak / metered: keep exactly one bounded 4 MiB block in
		// flight after a strong sequential signal. Disabling this layer entirely
		// serialized foreground subreads across the tunnel: a live 40 Mbit/s,
		// 80 ms shaped Link delivered only ~7.9 Mbit/s. One worker fills the
		// bandwidth-delay product without turning a Quick Look/xattr probe into a
		// whole-file pull; the six-block guard below still has to be crossed.
		return ReadaheadPolicy{Enabled: true, SeqThreshold: 6, Blocks: 1, Workers: 1}
	case ClassSlow:
		// Distant or mid-rate WAN (the RTT ceiling puts every far link here).
		// One serial 4 MB block in flight used about half of a 30 Mbit/s,
		// 200 ms link: each block paid a full request round trip before its
		// bytes moved. Several blocks in flight cover that idle time: Workers
		// 4, which the NFS layer caps one below the background FUSE share, so
		// 3 in practice. A six-block (24 MiB) window keeps more queued behind
		// them. The short-run guard in the NFS layer still holds back
		// preview-sized runs.
		return ReadaheadPolicy{Enabled: true, SeqThreshold: 4, Blocks: 6, Workers: 4, ParallelFetch: true}
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
	readClass := c
	if c == ClassSlow && p.slowRTTFloorAppliedLocked() {
		readClass = ClassMetered
	}
	return Snapshot{
		Class:           c,
		RTT:             p.rtt,
		BytesPerSec:     p.bwBps,
		HaveRTT:         p.haveRTT,
		HaveBW:          p.haveBW,
		ThroughputN:     p.bwSamples,
		BootstrappedRTT: boot,
		LowBandwidth:    p.haveBW && p.bwBps < thrMeteredBps,
		ReadClass:       readClass,
	}
}

// slowRTTFloorEnabled reports whether a far link keeps ClassSlow when its
// measured throughput is under the metered threshold. Default ON;
// JM_NET_SLOW_RTT_FLOOR=0 restores the throughput-only metered rule (WAN-tuning
// revert discipline: revertible without a rebuild).
func slowRTTFloorEnabled() bool {
	return os.Getenv("JM_NET_SLOW_RTT_FLOOR") != "0"
}

// bwWindowCarryEnabled reports whether a bandwidth window continues the one
// before it (clipped at the last flush). Default ON; JM_NET_BW_WINDOW_CARRY=0
// restores windows that each start at their first read.
func bwWindowCarryEnabled() bool {
	return os.Getenv("JM_NET_BW_WINDOW_CARRY") != "0"
}

// tunnelMeasuredEnabled reports whether a measured tunnel is classed by its
// measurement (capped at medium) instead of the fixed slow cap. Default ON;
// JM_NET_TUNNEL_MEASURED=0 restores the fixed cap without a rebuild.
func tunnelMeasuredEnabled() bool {
	return os.Getenv("JM_NET_TUNNEL_MEASURED") != "0"
}

// latencyCeilingEnabled reports whether the RTT ceiling may downgrade a
// bandwidth-derived class. Default ON; JM_NET_LATENCY_CEILING=0 restores
// bandwidth-only classification byte-identically, per the WAN-tuning revert
// discipline (a cellular tuning change must always be revertible without a
// rebuild).
func latencyCeilingEnabled() bool {
	return os.Getenv("JM_NET_LATENCY_CEILING") != "0"
}
