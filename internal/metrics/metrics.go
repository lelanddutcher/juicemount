// Package metrics implements zero-dependency in-process metrics for the
// NFS server: per-RPC latency histograms, a few global counters, and a
// tiny HTTP server that exposes /metrics and /health as JSON.
//
// The histogram is a fixed bucketed log/linear approximation. It avoids
// pulling in Prometheus or HDR — totals and percentiles are derived from
// the bucket counts. Buckets are spaced to cover sub-microsecond fast
// paths up through multi-second slow RPCs.
package metrics

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// RPCType is the canonical name of an NFS procedure that we time.
type RPCType string

// Known RPC types. Keep this list in sync with the dispatch table in
// internal/nfs/nfs.go — anything not listed here will be silently
// recorded under "OTHER".
const (
	RPCGetAttr     RPCType = "GETATTR"
	RPCSetAttr     RPCType = "SETATTR"
	RPCLookup      RPCType = "LOOKUP"
	RPCAccess      RPCType = "ACCESS"
	RPCRead        RPCType = "READ"
	RPCWrite       RPCType = "WRITE"
	RPCCreate      RPCType = "CREATE"
	RPCRemove      RPCType = "REMOVE"
	RPCRename      RPCType = "RENAME"
	RPCMkdir       RPCType = "MKDIR"
	RPCRmdir       RPCType = "RMDIR"
	RPCReadDir     RPCType = "READDIR"
	RPCReadDirPlus RPCType = "READDIRPLUS"
	RPCFSStat      RPCType = "FSSTAT"
	RPCFSInfo      RPCType = "FSINFO"
	RPCPathConf    RPCType = "PATHCONF"
	RPCCommit      RPCType = "COMMIT"
	RPCOther       RPCType = "OTHER"
)

// trackedTypes is the ordered list of RPC types we expose, even when
// they have zero samples. Keeping the order stable makes JSON output
// pleasant to read.
var trackedTypes = []RPCType{
	RPCGetAttr, RPCSetAttr, RPCLookup, RPCAccess,
	RPCRead, RPCWrite, RPCCreate, RPCRemove, RPCRename,
	RPCMkdir, RPCRmdir, RPCReadDir, RPCReadDirPlus,
	RPCFSStat, RPCFSInfo, RPCPathConf, RPCCommit,
	RPCOther,
}

// numBuckets is the fixed number of latency buckets. Keeping this a
// const lets histogram.buckets be a stack-allocated array.
const numBuckets = 22

// histBuckets are bucket upper bounds in microseconds. The pattern is
// roughly 1, 2, 5 × 10^k from 1us to 10s. Anything slower lands in the
// final overflow bucket.
var histBuckets = [numBuckets]float64{
	1, 2, 5,
	10, 20, 50,
	100, 200, 500,
	1_000, 2_000, 5_000,
	10_000, 20_000, 50_000,
	100_000, 200_000, 500_000,
	1_000_000, 2_000_000, 5_000_000,
	10_000_000,
}

// histogram is a simple bucketed histogram with running totals.
// All fields are guarded by an atomic-only contract: the buckets array
// is updated via atomic.AddUint64 so there is no per-record lock.
type histogram struct {
	buckets [numBuckets]uint64
	count   atomic.Uint64
	sumUs   atomic.Uint64 // running sum of latencies in microseconds
	maxUs   atomic.Uint64 // monotonically rising max
}

func (h *histogram) record(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	uus := uint64(us)
	h.count.Add(1)
	h.sumUs.Add(uus)
	for {
		cur := h.maxUs.Load()
		if uus <= cur {
			break
		}
		if h.maxUs.CompareAndSwap(cur, uus) {
			break
		}
	}

	// Find the first bucket whose upper bound >= us.
	idx := sort.SearchFloat64s(histBuckets[:], float64(us))
	if idx >= len(h.buckets) {
		idx = len(h.buckets) - 1
	}
	atomic.AddUint64(&h.buckets[idx], 1)
}

// snapshot returns a stable copy of the histogram state.
func (h *histogram) snapshot() histogramSnapshot {
	var out histogramSnapshot
	out.Count = h.count.Load()
	out.SumUs = h.sumUs.Load()
	out.MaxUs = h.maxUs.Load()
	for i := range h.buckets {
		out.Buckets[i] = atomic.LoadUint64(&h.buckets[i])
	}
	return out
}

type histogramSnapshot struct {
	Count   uint64
	SumUs   uint64
	MaxUs   uint64
	Buckets [numBuckets]uint64
}

// percentileUs returns an approximate percentile (0..1) in microseconds.
// We linearly interpolate inside the matching bucket to avoid a
// step-function look on small sample counts.
func (s histogramSnapshot) percentileUs(p float64) float64 {
	if s.Count == 0 {
		return 0
	}
	target := uint64(math.Ceil(float64(s.Count) * p))
	if target == 0 {
		target = 1
	}
	var cum uint64
	for i, c := range s.Buckets {
		next := cum + c
		if target <= next {
			lo := 0.0
			if i > 0 {
				lo = histBuckets[i-1]
			}
			hi := histBuckets[i]
			if c == 0 {
				return hi
			}
			frac := float64(target-cum) / float64(c)
			return lo + frac*(hi-lo)
		}
		cum = next
	}
	// Should be unreachable, but fall back to the top bucket bound.
	return histBuckets[len(histBuckets)-1]
}

// Registry holds all metrics for a process. Tests and embedded uses can
// create their own; the server uses a global one through Default().
type Registry struct {
	startedAt time.Time

	histsMu sync.RWMutex
	hists   map[RPCType]*histogram

	// Global counters
	rpcTotal   atomic.Uint64
	rpcErrors  atomic.Uint64
	bytesRead  atomic.Uint64
	bytesWrite atomic.Uint64

	// Read-path resilience counters. A cold JuiceFS/MinIO chunk fetch under
	// heavy concurrent read load can return a TRANSIENT EIO that the read
	// path now retries (cachedFile.ReadAt) instead of surfacing as NFS3ERR_IO
	// — which the kernel turns into EIO for read() consumers and SIGBUS for
	// mmap consumers (many NLEs mmap media → crash), with no data lost.
	// readRetries counts retried subreads; readFails counts the ones that
	// still failed after exhausting retries (the genuinely-bad case worth an
	// alert). Both were SILENT before (rpc_errors stayed 0), which made the
	// concurrent-readback SIGBUS hard to diagnose (2026-06-14).
	readRetries atomic.Uint64
	readFails   atomic.Uint64

	// Nav-latency observability counters (WAVE 0). These are QA-35-safe:
	// each is a single atomic.Uint64 increment on an EXISTING fork point in
	// the readdir / lookup / read / readahead paths — never a per-RPC FUSE
	// syscall or a hot-path lock. They exist so every nav-latency fix is
	// gradeable by a /metrics diff instead of an external fs_usage/accesslog
	// session (see NAV_LATENCY_DOSSIER §5).
	//
	// readdirMirrorHit  — a warm READDIR served from the RAM mirror fast path.
	// readdirEmptyRefill— a zero-row (unmirrored) dir returned EMPTY + kicked
	//                     an async refresh (the empty-then-pop-in case).
	// readdirColdShed   — an async dir-refresh SHED because all refresh workers
	//                     were busy (previously SILENT; grades the S3/S4 fixes).
	readdirMirrorHit   atomic.Uint64
	readdirEmptyRefill atomic.Uint64
	readdirColdShed    atomic.Uint64

	// lookupHit / lookupNoent — the LOOKUP resolve outcome (name resolved from
	// the cache vs returned NoEnt). One inc per LOOKUP that reaches the
	// child-resolution branch.
	lookupHit   atomic.Uint64
	lookupNoent atomic.Uint64

	// readColdSubread / readWarmSubread — THE KEY grader (RC-1/RC-3). Split off
	// the EXISTING warm/cold discriminator on the FUSE read path: a subread
	// whose wall-clock duration crosses the same threshold ObserveThroughput
	// uses to treat a sample as a real backend transfer (a cold MinIO GET over
	// the link) is counted cold; a sub-threshold subread (SSD block-cache hit)
	// is counted warm. No new syscall — the read latency is already measured
	// for ObserveThroughput. The cold ratio is the grader for whether preview
	// READs hit MinIO or the local SSD cache.
	readColdSubread atomic.Uint64
	readWarmSubread atomic.Uint64

	// readQoS* — grades #4 (INSTANT-NAV read-QoS, slow/metered links only).
	// queued = a read hit a full lane and waited; failOpen = a wait crossed
	// its bound and proceeded ungated; prefetchShed = a readahead round was
	// dropped because the bulk lane was contended.
	readQoSQueued       atomic.Uint64
	readQoSFailOpen     atomic.Uint64
	readQoSPrefetchShed atomic.Uint64

	// thumbWarm* — grades #1 (hydration pack). hydrated = posters pulled
	// into the local thumb cache; negative = inodes with no ready thumbnail
	// (TTL'd, not re-probed per browse); shed = a warm pass abandoned at the
	// QoS bulk lane (interactive traffic had priority).
	thumbWarmHydrated atomic.Uint64
	thumbWarmNegative atomic.Uint64
	thumbWarmShed     atomic.Uint64

	// backendBlipParked — grades #9: data-plane ops re-mapped to JUKEBOX
	// during a backend blip window instead of failing hard.
	backendBlipParked atomic.Uint64

	// sidecar* — grades the `._` AppleDouble body cache (nav crux). hit/miss on
	// the read path; warmPopulated = sidecars pre-read by the readdir warmer.
	sidecarCacheHit      atomic.Uint64
	sidecarCacheMiss     atomic.Uint64
	sidecarCachePut      atomic.Uint64
	sidecarWarmPopulated atomic.Uint64

	// readaheadTriggered / readaheadPrefetchedBlocks — grades S2. One inc per
	// readahead schedule (SeqThreshold trip); prefetched-blocks accumulates the
	// block count actually pulled by the background prefetch.
	// readaheadSuppressed — one inc per SeqThreshold trip that the S2 short-run
	// guard capped (a preview probe that would have escalated to the 64MB window
	// but was skipped). Rising counter = the guard is doing its job.
	readaheadTriggered        atomic.Uint64
	readaheadPrefetchedBlocks atomic.Uint64
	readaheadSuppressed       atomic.Uint64

	// H2 stale-recovery graders (S6). FromHandle's evicted-inode recovery path
	// (nfs/handler.go tryRecoverEvicted). Warm-mount expectation is a FLAT ~0 on
	// all three; a spike localizes a FromHandle stale-storm. QA-35-safe — each is
	// a single atomic increment on an EXISTING FromHandle fork point, never a new
	// FUSE syscall or a hot-path lock.
	//
	// recoverLstat   — a FromHandle inode-cache MISS entered the evicted-recovery
	//                  branch (tryRecoverEvicted): the cache-miss that may fall to
	//                  a FUSE Lstat. The attempt/denominator counter.
	// recoverStale   — the had-shadow STALE outcome: both recovery paths failed
	//                  but a Layer-B shadow still existed within ShadowTTL (a
	//                  prune/rename orphan whose handle the client still holds).
	// recoverSuccess — the ms-cost success: an evicted inode re-confirmed via the
	//                  FUSE Lstat and promoted back to the live cache.
	recoverLstat   atomic.Uint64
	recoverStale   atomic.Uint64
	recoverSuccess atomic.Uint64

	// H1 read-admission head-of-line grader (S6). The per-connection serve loop
	// blocks acquiring the shared rpcSem before dispatching a non-WRITE RPC
	// (internal/nfs/conn.go). When the semaphore is saturated the reader parks,
	// head-of-line-blocking every following LOOKUP/GETATTR/READ on that mount.
	// Graded WITHOUT a lock or a histogram: a monotonic CAS-max gauge (µs) plus
	// two bucketed atomic counters. The admission FAST path (a free slot) records
	// NOTHING — no time.Now(), no atomic — so the warm expectation is a literal 0
	// and the counter adds zero measurable cost to the uncontended per-RPC path
	// (QA-35). A fat tail here PROVES admission HOL before any readSem surgery.
	//
	// rpcAdmitWaitMaxUs    — monotonic max admission wait seen, µs (CAS-max gauge).
	// rpcAdmitWaitOver1ms  — count of admissions that blocked ≥ 1ms.
	// rpcAdmitWaitOver10ms — count of admissions that blocked ≥ 10ms.
	rpcAdmitWaitMaxUs    atomic.Uint64
	rpcAdmitWaitOver1ms  atomic.Uint64
	rpcAdmitWaitOver10ms atomic.Uint64

	// readdirVerifierHit / readdirFsReaddir — grades whether a paged
	// READDIR(PLUS) scroll was served from the verifier/cookie cache
	// (DataForVerifier hit → no ReadDir ran) or fell to a full fs.ReadDir
	// (internal/nfs/nfs_onreaddir.go). A high fs-readdir share during a scroll =
	// the verifier cache is missing paged reads.
	readdirVerifierHit atomic.Uint64
	readdirFsReaddir   atomic.Uint64

	// zeroTailSuspect (#104) — a spool entry finalized with unwritten hole(s)
	// below its written size (interrupted preallocate-then-write download: the
	// file drains FULL-SIZE with a zero tail; Premiere black-frames it).
	// Detection-only: the drain proceeds unchanged; this counter + the /spool
	// per-entry flag are the surfacing. Cold-path increment (once per suspect
	// finalize), QA-35-safe.
	zeroTailSuspect atomic.Uint64

	// FUSE metadata attribution (per-source, per-op counters + gate
	// saturation). Fixed preallocated arrays of atomics — see fuse_attrib.go
	// for the rationale and the hot-path cost contract. Embedded rather than
	// spelled out here so the arrays and their provider hook live next to the
	// enums that index them.
	fuseAttribState

	// Health hook — set by main.go so /health can answer accurately.
	healthMu sync.RWMutex
	healthFn func() HealthSnapshot

	// Network hook — set by the bridge so /metrics can report the adaptive
	// link estimate (class, RTT, bandwidth, live readahead policy). Decoupled
	// via a provider so this package needn't import netprofile.
	netMu sync.RWMutex
	netFn func() *NetworkSnapshot

	// FUSE data-gate hook — set by package nfs so /metrics can report the
	// concurrency ceiling added after the 2026-08-05 double kernel panic.
	// A provider (not counters pushed from the gate) because occupancy is a
	// GAUGE read once per scrape: pushing it would add an atomic store to
	// every admit/release on the FUSE data hot path, which the gate exists to
	// keep cheap. Decoupled this way so metrics needn't import nfs (nfs
	// already imports metrics — the reverse would be a cycle).
	dataGateMu sync.RWMutex
	dataGateFn func() *FUSEDataGateSnapshot
}

// NetworkSnapshot mirrors the netprofile link estimate for /metrics.
type NetworkSnapshot struct {
	Class           string  `json:"class"`
	RTTMs           float64 `json:"rtt_ms"`
	BandwidthMBps   float64 `json:"bandwidth_mbps"`
	HaveRTT         bool    `json:"have_rtt"`
	HaveBandwidth   bool    `json:"have_bandwidth"`
	ThroughputN     int64   `json:"throughput_samples"`
	BootstrappedRTT bool    `json:"bootstrapped_from_rtt"`
	// Live readahead policy derived from the class.
	ReadaheadEnabled bool `json:"readahead_enabled"`
	ReadaheadSeq     int  `json:"readahead_seq_threshold"`
	ReadaheadBlocks  int  `json:"readahead_blocks_ahead"`
	ReadaheadWorkers int  `json:"readahead_workers"`
}

// FUSEDataGateSnapshot mirrors the FUSE data-syscall concurrency ceiling for
// /metrics (nfs/fusedatagate.go).
//
// WHY THIS IS REPORTED AT ALL: the ceiling shipped in response to two kernel
// panics on 2026-08-05, and until this hook existed it had ZERO readers — the
// protection was unobservable in the field, so "is it engaging?", "is it
// shedding real work?" and "is the kill switch set?" were all unanswerable
// after an incident. A guard nobody can measure is a guard nobody can trust.
//
// Width is the CURRENT ceiling, not the configured one: the gate lowers itself
// on slow/metered links, so width moving is expected and is itself the signal
// that class-based narrowing engaged.
type FUSEDataGateSnapshot struct {
	// Enabled is false when JM_FUSE_DATA_GATE=0 disabled the ceiling. Reported
	// explicitly so a zero InUse/Width is never mistaken for an idle gate.
	Enabled bool `json:"enabled"`
	// InUse is the number of FUSE data syscalls admitted and not yet released.
	InUse int `json:"in_use"`
	// Width is the ceiling in force right now (class-dependent).
	Width int `json:"width"`
	// Refusals counts admissions denied since boot. A rising count means the
	// session is saturated and work is being shed — the intended behaviour,
	// but also the number that should correlate with any user-visible stall.
	Refusals int64 `json:"refusals"`
}

// HealthSnapshot is the JSON-friendly payload returned by /health.
//
// `components` deliberately has NO `,omitempty`: handleHealth normalizes a nil
// map to {} before encoding, so the field is ALWAYS a JSON object — never null
// and never absent. A null or missing `components` makes the Swift HealthProbe
// decoder throw and abort the entire decode (the same class of bug as
// CacheStatus roots:null — the stuck offline toggle).
type HealthSnapshot struct {
	Healthy    bool              `json:"healthy"`
	Components map[string]string `json:"components"`
	Reason     string            `json:"reason,omitempty"`
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		startedAt: time.Now(),
		hists:     make(map[RPCType]*histogram),
	}
}

var defaultRegistry = NewRegistry()

// Default returns the process-wide registry.
func Default() *Registry { return defaultRegistry }

// SetHealthProvider registers a callback used by /health.
func (r *Registry) SetHealthProvider(fn func() HealthSnapshot) {
	r.healthMu.Lock()
	defer r.healthMu.Unlock()
	r.healthFn = fn
}

// SetNetworkProvider registers a callback used by /metrics to report the
// adaptive link estimate. Safe to leave unset (the field is then omitted).
func (r *Registry) SetNetworkProvider(fn func() *NetworkSnapshot) {
	r.netMu.Lock()
	defer r.netMu.Unlock()
	r.netFn = fn
}

// SetFUSEDataGateProvider registers a callback used by /metrics to report the
// FUSE data-syscall ceiling. Safe to leave unset (the field is then omitted).
// package nfs registers this from an init() deliberately: the defect being
// fixed here is an accessor that existed with no callers, and an init() cannot
// be forgotten at a wiring site the way an explicit Set* call can.
func (r *Registry) SetFUSEDataGateProvider(fn func() *FUSEDataGateSnapshot) {
	r.dataGateMu.Lock()
	defer r.dataGateMu.Unlock()
	r.dataGateFn = fn
}

// histFor returns (and lazily creates) the histogram for an RPC type.
func (r *Registry) histFor(t RPCType) *histogram {
	r.histsMu.RLock()
	h := r.hists[t]
	r.histsMu.RUnlock()
	if h != nil {
		return h
	}
	r.histsMu.Lock()
	defer r.histsMu.Unlock()
	if h = r.hists[t]; h != nil {
		return h
	}
	h = &histogram{}
	r.hists[t] = h
	return h
}

// Observe records a single RPC's outcome. Pass err != nil on failure.
func (r *Registry) Observe(t RPCType, d time.Duration, err error) {
	r.histFor(t).record(d)
	r.rpcTotal.Add(1)
	if err != nil {
		r.rpcErrors.Add(1)
	}
}

// AddBytesRead increments the read-bytes counter.
func (r *Registry) AddBytesRead(n int64) {
	if n > 0 {
		r.bytesRead.Add(uint64(n))
	}
}

// AddBytesWritten increments the written-bytes counter.
func (r *Registry) AddBytesWritten(n int64) {
	if n > 0 {
		r.bytesWrite.Add(uint64(n))
	}
}

// IncReadRetry records that a transient cold-read EIO was retried.
func (r *Registry) IncReadRetry() { r.readRetries.Add(1) }

// IncReadFail records that a cold read still failed after exhausting retries
// (the genuinely-bad case — bytes were surfaced to the client as an error).
func (r *Registry) IncReadFail() { r.readFails.Add(1) }

// --- Nav-latency observability counters (WAVE 0). All QA-35-safe atomics. ---

// IncReaddirMirrorHit records a warm READDIR served from the RAM mirror.
func (r *Registry) IncReaddirMirrorHit() { r.readdirMirrorHit.Add(1) }

// IncReaddirEmptyRefill records a zero-row dir returned empty + async-refreshed.
func (r *Registry) IncReaddirEmptyRefill() { r.readdirEmptyRefill.Add(1) }

// IncReaddirColdShed records an async dir-refresh shed because all workers were
// busy (previously silent — the grader for the S3/S4 refresh fixes).
func (r *Registry) IncReaddirColdShed() { r.readdirColdShed.Add(1) }

// IncLookupHit records a LOOKUP whose child name resolved.
func (r *Registry) IncLookupHit() { r.lookupHit.Add(1) }

// IncLookupNoent records a LOOKUP that returned NoEnt (name not present).
func (r *Registry) IncLookupNoent() { r.lookupNoent.Add(1) }

// IncReadColdSubread records a FUSE subread served cold (real backend/MinIO GET
// over the link) — see the discriminator in cachedFile.ReadAt.
func (r *Registry) IncReadColdSubread() { r.readColdSubread.Add(1) }

// IncReadWarmSubread records a FUSE subread served warm (local SSD block cache).
func (r *Registry) IncReadWarmSubread() { r.readWarmSubread.Add(1) }

// IncReadQoSQueued records a read that found its QoS lane full and waited.
func (r *Registry) IncReadQoSQueued() { r.readQoSQueued.Add(1) }

// IncReadQoSFailOpen records a QoS wait that crossed its bound and proceeded
// ungated (the fail-open guarantee — shaping degraded, nothing broke).
func (r *Registry) IncReadQoSFailOpen() { r.readQoSFailOpen.Add(1) }

// IncReadQoSPrefetchShed records a readahead round dropped because the bulk
// lane was contended (bandwidth handed back to interactive reads).
func (r *Registry) IncReadQoSPrefetchShed() { r.readQoSPrefetchShed.Add(1) }

// IncThumbWarmHydrated records one thumbnail blob hydrated into the local cache.
func (r *Registry) IncThumbWarmHydrated() { r.thumbWarmHydrated.Add(1) }

// IncThumbWarmNegative records an inode found to have no ready thumbnail.
func (r *Registry) IncThumbWarmNegative() { r.thumbWarmNegative.Add(1) }

// IncThumbWarmShed records a warm pass abandoned at the read-QoS bulk lane.
func (r *Registry) IncThumbWarmShed() { r.thumbWarmShed.Add(1) }

// IncBackendBlipParked records a data-plane op parked (JUKEBOX) across a
// backend blip instead of failing hard.
func (r *Registry) IncBackendBlipParked() { r.backendBlipParked.Add(1) }

// IncSidecarCacheHit records a `._` sidecar read served from the RAM cache.
func (r *Registry) IncSidecarCacheHit() { r.sidecarCacheHit.Add(1) }

// IncSidecarCacheMiss records a `._` sidecar read that fell through to FUSE.
func (r *Registry) IncSidecarCacheMiss() { r.sidecarCacheMiss.Add(1) }

// IncSidecarCachePut records a complete `._` body inserted into the cache.
func (r *Registry) IncSidecarCachePut() { r.sidecarCachePut.Add(1) }

// IncSidecarWarmPopulated records a `._` sidecar pre-read by the readdir warmer.
func (r *Registry) IncSidecarWarmPopulated() { r.sidecarWarmPopulated.Add(1) }

// IncReadaheadTriggered records one readahead schedule (SeqThreshold trip).
func (r *Registry) IncReadaheadTriggered() { r.readaheadTriggered.Add(1) }

// AddReadaheadPrefetchedBlocks adds the count of blocks a prefetch pass pulled.
func (r *Registry) AddReadaheadPrefetchedBlocks(n int64) {
	if n > 0 {
		r.readaheadPrefetchedBlocks.Add(uint64(n))
	}
}

// IncReadaheadSuppressed records a SeqThreshold trip that the S2 short-run guard
// capped instead of escalating to the full prefetch window (a preview probe).
func (r *Registry) IncReadaheadSuppressed() { r.readaheadSuppressed.Add(1) }

// --- H2 stale-recovery graders (S6). QA-35-safe atomics on FromHandle. ---

// IncRecoverLstat records a FromHandle inode-cache miss that entered the
// evicted-inode recovery branch (tryRecoverEvicted) — the attempt/denominator
// counter for the H2 stale-recovery path.
func (r *Registry) IncRecoverLstat() { r.recoverLstat.Add(1) }

// IncRecoverStale records a had-shadow STALE outcome: FromHandle recovery failed
// but a Layer-B shadow still existed within ShadowTTL (a prune/rename orphan the
// client still holds a handle for).
func (r *Registry) IncRecoverStale() { r.recoverStale.Add(1) }

// IncRecoverSuccess records an evicted inode re-confirmed via the FUSE Lstat and
// promoted back to the live cache — the ms-cost recovery success case.
func (r *Registry) IncRecoverSuccess() { r.recoverSuccess.Add(1) }

// --- H1 read-admission head-of-line grader (S6). Atomic-only, no lock. ---

// ObserveAdmitWait records how long the per-connection serve loop blocked
// acquiring the rpcSem admission slot. Callers MUST invoke it ONLY when the
// acquire actually blocked — a zero-wait fast-path acquire records nothing, so
// the uncontended per-RPC path pays no cost at all. Mechanism: a monotonic
// CAS-max gauge plus two threshold buckets — all atomic, never a lock or a
// syscall. A sub-microsecond wait (rounds to 0 µs) is dropped by the us<=0 guard.
func (r *Registry) ObserveAdmitWait(d time.Duration) {
	us := d.Microseconds()
	if us <= 0 {
		return
	}
	uus := uint64(us)
	for {
		cur := r.rpcAdmitWaitMaxUs.Load()
		if uus <= cur {
			break
		}
		if r.rpcAdmitWaitMaxUs.CompareAndSwap(cur, uus) {
			break
		}
	}
	if d >= time.Millisecond {
		r.rpcAdmitWaitOver1ms.Add(1)
	}
	if d >= 10*time.Millisecond {
		r.rpcAdmitWaitOver10ms.Add(1)
	}
}

// --- Paged-readdir cache grader (S6). ---

// IncReaddirVerifierHit records a READDIR(PLUS) served from the verifier/cookie
// cache (DataForVerifier hit — no fs.ReadDir ran).
func (r *Registry) IncReaddirVerifierHit() { r.readdirVerifierHit.Add(1) }

// IncReaddirFsReaddir records a READDIR(PLUS) that fell to a full fs.ReadDir.
func (r *Registry) IncReaddirFsReaddir() { r.readdirFsReaddir.Add(1) }

// IncZeroTailSuspect records a spool entry that finalized with unwritten
// hole(s) below its written size — the interrupted preallocate-then-write
// download signature (#104). Detection-only; the drain is never gated on it.
func (r *Registry) IncZeroTailSuspect() { r.zeroTailSuspect.Add(1) }

// Snapshot is the JSON shape returned by /metrics.
type Snapshot struct {
	UptimeSec    int64  `json:"uptime_sec"`
	RPCTotal     uint64 `json:"rpc_total"`
	RPCErrors    uint64 `json:"rpc_errors"`
	BytesRead    uint64 `json:"bytes_read"`
	BytesWritten uint64 `json:"bytes_written"`
	ReadRetries  uint64 `json:"read_retries"`
	ReadFails    uint64 `json:"read_fails"`

	// Nav-latency observability counters (WAVE 0). See NAV_LATENCY_DOSSIER §5.
	ReaddirMirrorHit          uint64 `json:"readdir_mirror_hit"`
	ReaddirEmptyRefill        uint64 `json:"readdir_empty_refill"`
	ReaddirColdShed           uint64 `json:"readdir_cold_shed"`
	LookupHit                 uint64 `json:"lookup_hit"`
	LookupNoent               uint64 `json:"lookup_noent"`
	ReadColdSubread           uint64 `json:"read_cold_subread"`
	ReadWarmSubread           uint64 `json:"read_warm_subread"`
	ReadQoSQueued             uint64 `json:"read_qos_queued"`
	ReadQoSFailOpen           uint64 `json:"read_qos_fail_open"`
	ReadQoSPrefetchShed       uint64 `json:"read_qos_prefetch_shed"`
	ThumbWarmHydrated         uint64 `json:"thumb_warm_hydrated"`
	ThumbWarmNegative         uint64 `json:"thumb_warm_negative"`
	ThumbWarmShed             uint64 `json:"thumb_warm_shed"`
	BackendBlipParked         uint64 `json:"backend_blip_parked"`
	SidecarCacheHit           uint64 `json:"sidecar_cache_hit"`
	SidecarCacheMiss          uint64 `json:"sidecar_cache_miss"`
	SidecarCachePut           uint64 `json:"sidecar_cache_put"`
	SidecarWarmPopulated      uint64 `json:"sidecar_warm_populated"`
	ReadaheadTriggered        uint64 `json:"readahead_triggered"`
	ReadaheadPrefetchedBlocks uint64 `json:"readahead_prefetched_blocks"`
	ReadaheadSuppressed       uint64 `json:"readahead_suppressed"`

	// H1/H2 nav-latency graders (S6). See NAV_LATENCY_DOSSIER §5-6.
	// recover_* grade FromHandle stale-recovery (H2); rpc_admit_wait_* grade
	// read-admission head-of-line block (H1); readdir_verifier_hit /
	// readdir_fs_readdir grade paged-readdir cache service.
	RecoverLstat         uint64 `json:"recover_lstat_total"`
	RecoverStale         uint64 `json:"recover_stale_total"`
	RecoverSuccess       uint64 `json:"recover_success_total"`
	RPCAdmitWaitUs       uint64 `json:"rpc_admit_wait_us"`
	RPCAdmitWaitOver1ms  uint64 `json:"rpc_admit_wait_over_1ms"`
	RPCAdmitWaitOver10ms uint64 `json:"rpc_admit_wait_over_10ms"`
	ReaddirVerifierHit   uint64 `json:"readdir_verifier_hit"`
	ReaddirFsReaddir     uint64 `json:"readdir_fs_readdir"`

	// Spool zero-tail detection (#104): entries that finalized with unwritten
	// hole(s) below their written size (drained full-size with zero tails).
	ZeroTailSuspect uint64 `json:"zero_tail_suspect_total"`

	// FUSE metadata attribution: WHO issued each bounded FUSE metadata
	// syscall, what it cost, how much of that was queueing for a gate, and
	// how saturated the three gates got. See fuse_attrib.go.
	FUSEAttrib FUSEAttribSnapshot `json:"fuse_attrib"`

	RPCs    map[string]RPCSnapshot `json:"rpcs"`
	Network *NetworkSnapshot       `json:"network,omitempty"`

	// FUSEDataGate reports the post-panic concurrency ceiling. See
	// FUSEDataGateSnapshot.
	FUSEDataGate *FUSEDataGateSnapshot `json:"fuse_data_gate,omitempty"`
}

// RPCSnapshot is the per-RPC JSON shape.
type RPCSnapshot struct {
	Count  uint64  `json:"count"`
	MeanUs float64 `json:"mean_us"`
	MaxUs  uint64  `json:"max_us"`
	P50Us  float64 `json:"p50_us"`
	P95Us  float64 `json:"p95_us"`
	P99Us  float64 `json:"p99_us"`
}

// Snapshot builds a self-contained metrics view.
func (r *Registry) Snapshot() Snapshot {
	out := Snapshot{
		UptimeSec:    int64(time.Since(r.startedAt).Seconds()),
		RPCTotal:     r.rpcTotal.Load(),
		RPCErrors:    r.rpcErrors.Load(),
		BytesRead:    r.bytesRead.Load(),
		BytesWritten: r.bytesWrite.Load(),
		ReadRetries:  r.readRetries.Load(),
		ReadFails:    r.readFails.Load(),

		ReaddirMirrorHit:          r.readdirMirrorHit.Load(),
		ReaddirEmptyRefill:        r.readdirEmptyRefill.Load(),
		ReaddirColdShed:           r.readdirColdShed.Load(),
		LookupHit:                 r.lookupHit.Load(),
		LookupNoent:               r.lookupNoent.Load(),
		ReadColdSubread:           r.readColdSubread.Load(),
		ReadWarmSubread:           r.readWarmSubread.Load(),
		ReadQoSQueued:             r.readQoSQueued.Load(),
		ReadQoSFailOpen:           r.readQoSFailOpen.Load(),
		ReadQoSPrefetchShed:       r.readQoSPrefetchShed.Load(),
		ThumbWarmHydrated:         r.thumbWarmHydrated.Load(),
		ThumbWarmNegative:         r.thumbWarmNegative.Load(),
		ThumbWarmShed:             r.thumbWarmShed.Load(),
		BackendBlipParked:         r.backendBlipParked.Load(),
		SidecarCacheHit:           r.sidecarCacheHit.Load(),
		SidecarCacheMiss:          r.sidecarCacheMiss.Load(),
		SidecarCachePut:           r.sidecarCachePut.Load(),
		SidecarWarmPopulated:      r.sidecarWarmPopulated.Load(),
		ReadaheadTriggered:        r.readaheadTriggered.Load(),
		ReadaheadPrefetchedBlocks: r.readaheadPrefetchedBlocks.Load(),
		ReadaheadSuppressed:       r.readaheadSuppressed.Load(),

		RecoverLstat:         r.recoverLstat.Load(),
		RecoverStale:         r.recoverStale.Load(),
		RecoverSuccess:       r.recoverSuccess.Load(),
		RPCAdmitWaitUs:       r.rpcAdmitWaitMaxUs.Load(),
		RPCAdmitWaitOver1ms:  r.rpcAdmitWaitOver1ms.Load(),
		RPCAdmitWaitOver10ms: r.rpcAdmitWaitOver10ms.Load(),
		ReaddirVerifierHit:   r.readdirVerifierHit.Load(),
		ReaddirFsReaddir:     r.readdirFsReaddir.Load(),

		ZeroTailSuspect: r.zeroTailSuspect.Load(),

		FUSEAttrib: r.snapshotFUSEAttrib(),

		RPCs: make(map[string]RPCSnapshot, len(trackedTypes)),
	}

	r.netMu.RLock()
	netFn := r.netFn
	r.netMu.RUnlock()
	if netFn != nil {
		out.Network = netFn()
	}

	r.dataGateMu.RLock()
	dataGateFn := r.dataGateFn
	r.dataGateMu.RUnlock()
	if dataGateFn != nil {
		out.FUSEDataGate = dataGateFn()
	}

	r.histsMu.RLock()
	defer r.histsMu.RUnlock()

	// Always emit the canonical types so the JSON shape is stable, even
	// before the first sample arrives.
	emitted := make(map[RPCType]struct{}, len(trackedTypes))
	for _, t := range trackedTypes {
		emitted[t] = struct{}{}
		h := r.hists[t]
		if h == nil {
			out.RPCs[string(t)] = RPCSnapshot{}
			continue
		}
		out.RPCs[string(t)] = makeRPCSnapshot(h.snapshot())
	}
	// Anything else seen at runtime that's not in the canonical list
	// (defensive — should not happen with the current dispatch).
	for t, h := range r.hists {
		if _, ok := emitted[t]; ok {
			continue
		}
		out.RPCs[string(t)] = makeRPCSnapshot(h.snapshot())
	}
	return out
}

func makeRPCSnapshot(s histogramSnapshot) RPCSnapshot {
	if s.Count == 0 {
		return RPCSnapshot{}
	}
	mean := float64(s.SumUs) / float64(s.Count)
	return RPCSnapshot{
		Count:  s.Count,
		MeanUs: mean,
		MaxUs:  s.MaxUs,
		P50Us:  s.percentileUs(0.50),
		P95Us:  s.percentileUs(0.95),
		P99Us:  s.percentileUs(0.99),
	}
}

// Server runs an HTTP listener that exposes /metrics and /health.
type Server struct {
	registry *Registry
	addr     string
	listener net.Listener
	// listener6 is the companion ::1 listener. See Start: a client resolving
	// "localhost" gets ::1 FIRST on macOS, so an IPv4-only bind is refused for
	// any client that does not fall back.
	listener6 net.Listener
	httpSrv   *http.Server

	// ExtraRoutes lets callers (e.g. cbridge) register additional handlers
	// on the same listener — Pin/Unpin/CacheStatus/Offline endpoints live
	// here. Set BEFORE calling Start().
	ExtraRoutes map[string]http.HandlerFunc
}

// NewServer creates a metrics HTTP server bound to addr (e.g. 127.0.0.1:11050).
// The listener is opened by Start().
func NewServer(addr string, reg *Registry) *Server {
	if reg == nil {
		reg = Default()
	}
	return &Server{addr: addr, registry: reg}
}

// Addr returns the actual listening address (after Start).
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

// Start binds the listener and serves on a background goroutine.
func (s *Server) Start() error {
	if s.addr == "" {
		return fmt.Errorf("metrics: empty address")
	}
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("metrics listen %s: %w", s.addr, err)
	}
	s.listener = l

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/health", s.handleHealth)
	for path, h := range s.ExtraRoutes {
		mux.HandleFunc(path, h)
	}
	mux.HandleFunc("/", s.handleIndex) // catch-all last

	s.httpSrv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		_ = s.httpSrv.Serve(l)
	}()

	// ALSO BIND IPv6 LOOPBACK.
	//
	// macOS resolves "localhost" to ::1 BEFORE 127.0.0.1. This server bound
	// IPv4 only, so a client that connects to the first resolved address and
	// does not fall back gets ECONNREFUSED and concludes the control plane is
	// down — while curl, which does fall back, reports it perfectly healthy.
	// Measured 2026-08-04: http://[::1]:11050 refused, http://127.0.0.1:11050
	// 200, and that is the shape of a consumer reporting "no success" against a
	// control plane that answers fine from a shell.
	//
	// Loopback ONLY, deliberately: this is a second loopback family, NOT a
	// widening of exposure. The control plane is unauthenticated and must never
	// be reachable off-box.
	//
	// Best-effort: a machine with IPv6 disabled must still start. The IPv4
	// listener above remains the one whose failure is fatal.
	// Port comes from the BOUND listener, not from s.addr: with an ephemeral
	// ":0" the configured port is 0, so deriving from s.addr would put the ::1
	// listener on a DIFFERENT random port than the IPv4 one — the companion
	// would exist and still not answer where the client is looking.
	if host, _, perr := net.SplitHostPort(s.addr); perr == nil && isLoopbackHost(host) {
		boundPort := strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
		if l6, e6 := net.Listen("tcp6", net.JoinHostPort("::1", boundPort)); e6 == nil {
			s.listener6 = l6
			go func() {
				_ = s.httpSrv.Serve(l6)
			}()
		}
	}
	return nil
}

// isLoopbackHost reports whether a bind host is loopback, so the ::1 companion
// is added ONLY when we are already loopback-bound — never for a wider bind.
func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Stop closes the HTTP server and listener.
func (s *Server) Stop() {
	if s.httpSrv != nil {
		_ = s.httpSrv.Close() // closes every listener it is serving, incl. ::1
	}
	if s.listener6 != nil {
		_ = s.listener6.Close()
	}
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(s.registry.Snapshot())
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.registry.healthMu.RLock()
	fn := s.registry.healthFn
	s.registry.healthMu.RUnlock()

	var snap HealthSnapshot
	if fn != nil {
		snap = fn()
	} else {
		// No provider yet — assume healthy if the server is up.
		snap = HealthSnapshot{Healthy: true}
	}
	// Never emit a null or absent `components`: a nil Go map marshals to JSON
	// null, which makes the Swift HealthProbe decoder throw valueNotFound and
	// abort the whole decode (same root cause as CacheStatus roots:null — the
	// stuck offline toggle). Normalize to an empty object so the JSON is always
	// `"components": {}` for the fn==nil and monitor-stopped paths.
	if snap.Components == nil {
		snap.Components = map[string]string{}
	}

	w.Header().Set("Content-Type", "application/json")
	if !snap.Healthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(snap)
}

// handleIndex serves the human-readable route banner at the exact root path
// and returns 404 for anything else. This handler is registered against the
// ServeMux subtree pattern "/", so it receives every request that no
// longer-prefix exact route (/metrics, /health, /whoami, /residency, the
// /manager and /debug/pprof subtrees, …) already claimed. Without the
// path guard below, a truly-unknown route such as GET /definitely-not-a-real-
// route would fall through here and get an HTTP 200 — the "catch-all 200 trap"
// OpenLoupe flagged: capability probing can't distinguish a served route from
// an unserved one. Registered routes are unaffected because ServeMux always
// dispatches the longest matching pattern, so they never reach this function.
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = fmt.Fprint(w, "JuiceMount metrics\n  /metrics\n  /health\n")
}
