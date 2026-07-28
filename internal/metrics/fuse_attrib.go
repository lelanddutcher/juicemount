package metrics

// FUSE metadata attribution (per-SOURCE, per-OP).
//
// WHY THIS EXISTS
//
// A cellular session (≈500ms RTT) burned 67 MINUTES of cumulative FUSE
// metadata wait — Lookup 9322 calls / 2476 s, GetAttr 4597 / 1018 s, StatFS
// 1727 / 397 s, Read 192 / 59 s — against only 77 object GETs. The link was
// consumed by METADATA, not data. But nothing in the process could say WHO
// issued those syscalls: the bounded FUSE helpers in nfs/handler.go took
// (path, timeout[, gate]) and incremented NOTHING, there is no context
// propagation and no goroutine tagging, and lookup_hit/lookup_noent are
// per-NFS-RPC counters that by construction only ever see FOREGROUND traffic.
//
// Retuning timeouts/gates/TTLs on top of an unattributed 67 minutes is
// guessing. This is the attribution layer that has to land first: every
// bounded FUSE metadata call records (source, op) → count, total nanos, gate
// wait, timeouts. From that you can compute the per-source mean (a 266 ms
// average is the whole problem), see which sources generate the JUKEBOX
// timeouts, and — critically — separate time spent QUEUEING for a gate from
// time spent in the syscall, because today's budget conflates the two.
//
// COST DISCIPLINE (QA-35 / feedback_perf_hot_path)
//
// These helpers sit on the NFS serving path, so the recording path is:
//   - a FIXED 2-D array of atomic cells, preallocated with the Registry and
//     indexed by the two enums — no map, no lock, no allocation, no string
//     formatting on the hot path;
//   - three atomic adds (+1 conditional) per call;
//   - a gate-wait clock read ONLY when the gate acquire actually BLOCKED
//     (the uncontended acquire records a literal zero and reads no clock) —
//     the same discipline internal/nfs/conn.go's H1 admission grader uses.
// The two remaining time.Now() calls are dwarfed by what the helpers already
// pay per call (a time.Timer, a buffered channel and a goroutine) and by the
// FUSE syscall itself. See BenchmarkObserveFUSECall.

import (
	"sync"
	"sync/atomic"
	"time"
)

// FUSESource identifies WHO issued a bounded FUSE metadata syscall — the
// deliverable this whole file exists for. Typed (not a string) so the record
// path is an array index, never a map lookup or an allocation.
type FUSESource uint8

const (
	// FUSESrcForeground: an NFS RPC is blocked on this syscall — a client
	// syscall (Finder, an NLE, ls) is waiting for it to return. This is the
	// only class where latency is directly user-visible.
	FUSESrcForeground FUSESource = iota
	// FUSESrcPrefetch: the background directory prefetcher (prefetchChildren)
	// warming a dir's children into the mirror after a READDIR.
	FUSESrcPrefetch
	// FUSESrcDirRefresh: the U7 async unmirrored-dir refresh worker
	// (refreshUnmirroredDir) — the empty-then-pop-in mirror warm.
	FUSESrcDirRefresh
	// FUSESrcSidecarWarm: the `._` AppleDouble sidecar warmer (warmSidecar),
	// fanned out up to sidecarWarmSem(3) × sidecarWarmParallel(16) = 48 ways.
	FUSESrcSidecarWarm
	// FUSESrcThumbWarm: the farm-thumbnail hydration warmer (hydrateOne).
	FUSESrcThumbWarm
	// FUSESrcPhantomPurge: the async phantom-entry confirmation
	// (asyncConfirmPhantomPurge) — off the Stat hot path, but still FUSE.
	FUSESrcPhantomPurge
	// FUSESrcDrainer: the spool drainer landing bytes/symlinks on FUSE.
	// Reserved: the drainer currently uses RAW os.* calls that go through no
	// bounded helper at all, so this stays 0 until that changes. A non-zero
	// value here means someone routed drain work through the helpers.
	FUSESrcDrainer
	// FUSESrcOther: unclassified. A non-zero value means a new call site was
	// added without classifying it — treat it as a bug, not a bucket.
	FUSESrcOther

	// NumFUSESources is the number of tracked sources.
	NumFUSESources
)

var fuseSourceNames = [NumFUSESources]string{
	FUSESrcForeground:   "foreground",
	FUSESrcPrefetch:     "prefetch",
	FUSESrcDirRefresh:   "dir_refresh",
	FUSESrcSidecarWarm:  "sidecar_warm",
	FUSESrcThumbWarm:    "thumb_warm",
	FUSESrcPhantomPurge: "phantom_purge",
	FUSESrcDrainer:      "drainer",
	FUSESrcOther:        "other",
}

// String returns the /metrics key for a source.
func (s FUSESource) String() string {
	if s >= NumFUSESources {
		return "other"
	}
	return fuseSourceNames[s]
}

// FUSEOp identifies WHICH bounded FUSE metadata syscall ran. One value per
// bounded helper in nfs/handler.go.
type FUSEOp uint8

const (
	FUSEOpStat          FUSEOp = iota // statWithTimeout        → os.Stat
	FUSEOpLstat                       // lstatWithTimeout       → os.Lstat
	FUSEOpLstatNotExist               // lstatNotExistWithTimeout → os.Lstat (existence only)
	FUSEOpReadDir                     // readDirWithTimeout     → os.ReadDir
	FUSEOpDirEntryInfo                // infoWithTimeout        → os.DirEntry.Info (a lazy lstat)
	FUSEOpOpen                        // openFileWithTimeout    → os.OpenFile
	FUSEOpFstat                       // cachedFile/billyFile LiveSize → (*os.File).Stat
	FUSEOpSymlink                     // symlinkWithTimeout     → os.Symlink
	FUSEOpMkdirAll                    // mkdirAllWithTimeout    → os.MkdirAll
	FUSEOpChmod                       // chmodWithTimeout       → os.Chmod

	// NumFUSEOps is the number of tracked ops.
	NumFUSEOps
)

var fuseOpNames = [NumFUSEOps]string{
	FUSEOpStat:          "stat",
	FUSEOpLstat:         "lstat",
	FUSEOpLstatNotExist: "lstat_notexist",
	FUSEOpReadDir:       "readdir",
	FUSEOpDirEntryInfo:  "direntry_info",
	FUSEOpOpen:          "open",
	FUSEOpFstat:         "fstat",
	FUSEOpSymlink:       "symlink",
	FUSEOpMkdirAll:      "mkdirall",
	FUSEOpChmod:         "chmod",
}

// String returns the /metrics key for an op.
func (o FUSEOp) String() string {
	if o >= NumFUSEOps {
		return "unknown"
	}
	return fuseOpNames[o]
}

// FUSEGate identifies which bounded concurrency gate a call drew from. The
// gates are the exact head-of-line point for navigation and were completely
// unmeasured before this: nfsLstatGate (24) is the FOREGROUND budget,
// prefetchGate (2) the BACKGROUND readdir budget, fuseFstatGate (8) the
// open-fd fstat budget.
type FUSEGate uint8

const (
	FUSEGateNFSLstat FUSEGate = iota // nfsLstatGate  — foreground hot-path budget
	FUSEGatePrefetch                 // prefetchGate  — background readdir budget
	FUSEGateFstat                    // fuseFstatGate — LiveSize fstat budget

	// NumFUSEGates is the number of tracked gates.
	NumFUSEGates

	// FUSEGateNone marks a call that drew from no gate. Deliberately >=
	// NumFUSEGates so it is skipped by the gate recording path.
	FUSEGateNone FUSEGate = 255
)

var fuseGateNames = [NumFUSEGates]string{
	FUSEGateNFSLstat: "nfs_lstat",
	FUSEGatePrefetch: "prefetch",
	FUSEGateFstat:    "fuse_fstat",
}

// FUSEOutcome is how a bounded call ended.
type FUSEOutcome uint8

const (
	// FUSEOutcomeOK: the syscall returned inside its budget (it may still
	// have returned an error such as ENOENT — that is a completed call).
	FUSEOutcomeOK FUSEOutcome = iota
	// FUSEOutcomeTimeout: the gate was acquired but the syscall did not
	// return inside the budget. This is the NFS3ERR_JUKEBOX generator.
	FUSEOutcomeTimeout
	// FUSEOutcomeGateTimeout: the budget expired while still QUEUEING for the
	// gate — the syscall never even started. Pure head-of-line blocking; the
	// caller paid the full timeout for zero FUSE work.
	FUSEOutcomeGateTimeout
)

// fuseCell is one (source, op) slot. Atomics only — no lock, no allocation.
type fuseCell struct {
	calls        atomic.Uint64
	totalNs      atomic.Uint64 // gate wait + syscall wait: what the caller actually paid
	gateWaitNs   atomic.Uint64 // subset of totalNs spent QUEUEING, not in the syscall
	timeouts     atomic.Uint64 // ok=false for any reason
	gateTimeouts atomic.Uint64 // subset of timeouts: never acquired the gate
}

// fuseGateCell is one gate's cumulative saturation record.
type fuseGateCell struct {
	acquires atomic.Uint64 // slots successfully taken
	blocked  atomic.Uint64 // acquires that had to WAIT (gate was full on arrival)
	waitNs   atomic.Uint64 // total time spent queueing for this gate
	timeouts atomic.Uint64 // acquires abandoned at the deadline
	maxDepth atomic.Uint64 // high-water occupancy observed at acquire (CAS-max)
}

// FUSEGateLevel is the LIVE occupancy of one gate, sampled at scrape time by
// the provider the nfs package registers (metrics can't import nfs).
type FUSEGateLevel struct {
	Depth int `json:"depth"`
	Cap   int `json:"cap"`
}

// SetFUSEGateProvider registers a callback that reports live gate occupancy,
// indexed by FUSEGate. Safe to leave unset (depth/cap then read 0).
func (r *Registry) SetFUSEGateProvider(fn func() [NumFUSEGates]FUSEGateLevel) {
	r.fuseGateMu.Lock()
	defer r.fuseGateMu.Unlock()
	r.fuseGateFn = fn
}

// ObserveFUSECall records ONE bounded FUSE metadata syscall.
//
// gateWait MUST be 0 when the gate acquire did not block — callers use a
// non-blocking try first, so the uncontended path reads no clock and reports
// an exact zero (mirrors ObserveAdmitWait's contract). gateDepth is the gate
// occupancy observed immediately AFTER acquiring (1..cap); pass 0 when no
// slot was taken. total is the caller-observed wall time for the whole call.
//
// Hot-path cost: 3-5 atomic adds and (only on a first-of-its-kind saturation)
// one CAS. No lock, no map, no allocation. See BenchmarkObserveFUSECall.
func (r *Registry) ObserveFUSECall(
	src FUSESource, op FUSEOp, gate FUSEGate, gateDepth int,
	gateWait, total time.Duration, outcome FUSEOutcome,
) {
	if src >= NumFUSESources {
		src = FUSESrcOther
	}
	if op >= NumFUSEOps {
		return // unknown op: drop rather than corrupt a neighbouring slot
	}

	c := &r.fuseCells[src][op]
	c.calls.Add(1)
	if total > 0 {
		c.totalNs.Add(uint64(total.Nanoseconds()))
	}
	var waitNs uint64
	if gateWait > 0 {
		waitNs = uint64(gateWait.Nanoseconds())
		c.gateWaitNs.Add(waitNs)
	}
	switch outcome {
	case FUSEOutcomeTimeout:
		c.timeouts.Add(1)
	case FUSEOutcomeGateTimeout:
		c.timeouts.Add(1)
		c.gateTimeouts.Add(1)
	}

	if gate >= NumFUSEGates {
		return // FUSEGateNone / out of range — no gate to attribute
	}
	g := &r.fuseGates[gate]
	if waitNs > 0 {
		g.waitNs.Add(waitNs)
	}
	if outcome == FUSEOutcomeGateTimeout {
		g.timeouts.Add(1)
		return // no slot was taken, so no acquire and no depth sample
	}
	g.acquires.Add(1)
	if waitNs > 0 {
		g.blocked.Add(1)
	}
	if gateDepth > 0 {
		d := uint64(gateDepth)
		for {
			cur := g.maxDepth.Load()
			if d <= cur {
				break
			}
			if g.maxDepth.CompareAndSwap(cur, d) {
				break
			}
		}
	}
}

// --- snapshot types -------------------------------------------------------

// FUSEStatsSnapshot is the JSON shape of one aggregate (a source total, an op
// total, or a single (source, op) cell).
type FUSEStatsSnapshot struct {
	Calls        uint64  `json:"calls"`
	TotalNs      uint64  `json:"total_ns"`
	MeanUs       float64 `json:"mean_us"`
	GateWaitNs   uint64  `json:"gate_wait_ns"`
	Timeouts     uint64  `json:"timeouts"`
	GateTimeouts uint64  `json:"gate_timeouts"`
}

// loadCell reads one atomic cell into a plain value ONCE, so the per-source
// and per-op rollups are built from the same observation as the cell row.
func loadCell(c *fuseCell) FUSEStatsSnapshot {
	return FUSEStatsSnapshot{
		Calls:        c.calls.Load(),
		TotalNs:      c.totalNs.Load(),
		GateWaitNs:   c.gateWaitNs.Load(),
		Timeouts:     c.timeouts.Load(),
		GateTimeouts: c.gateTimeouts.Load(),
	}
}

func (s *FUSEStatsSnapshot) add(v FUSEStatsSnapshot) {
	s.Calls += v.Calls
	s.TotalNs += v.TotalNs
	s.GateWaitNs += v.GateWaitNs
	s.Timeouts += v.Timeouts
	s.GateTimeouts += v.GateTimeouts
}

func (s *FUSEStatsSnapshot) finish() {
	if s.Calls > 0 {
		s.MeanUs = float64(s.TotalNs) / float64(s.Calls) / 1000.0
	}
}

// FUSECellSnapshot is one (source, op) pair. Only NON-ZERO cells are emitted
// (8×10 mostly-zero objects would bury the signal); the stable per-source and
// per-op maps above it always carry every key.
type FUSECellSnapshot struct {
	Source string `json:"source"`
	Op     string `json:"op"`
	FUSEStatsSnapshot
}

// FUSEGateSnapshot is one gate's live level plus its cumulative saturation.
type FUSEGateSnapshot struct {
	Depth    int    `json:"depth"`
	Cap      int    `json:"cap"`
	MaxDepth uint64 `json:"max_depth"`
	Acquires uint64 `json:"acquires"`
	Blocked  uint64 `json:"blocked"`
	WaitNs   uint64 `json:"wait_ns"`
	Timeouts uint64 `json:"timeouts"`
}

// FUSEAttribSnapshot is the whole attribution view served under /metrics.
type FUSEAttribSnapshot struct {
	BySource map[string]FUSEStatsSnapshot `json:"by_source"`
	ByOp     map[string]FUSEStatsSnapshot `json:"by_op"`
	Cells    []FUSECellSnapshot           `json:"cells"`
	Gates    map[string]FUSEGateSnapshot  `json:"gates"`
}

// snapshotFUSEAttrib builds the attribution view. Cold path (one /metrics
// scrape); allocation here is irrelevant.
func (r *Registry) snapshotFUSEAttrib() FUSEAttribSnapshot {
	// NOTE: Cells is an EMPTY slice, never nil. A nil Go slice marshals to
	// JSON null, and a null where an array is declared aborts the whole Swift
	// decode (the CacheStatus roots:null / HealthProbe components:null bug
	// class). Same reason handleHealth normalizes `components` to {}.
	out := FUSEAttribSnapshot{
		BySource: make(map[string]FUSEStatsSnapshot, NumFUSESources),
		ByOp:     make(map[string]FUSEStatsSnapshot, NumFUSEOps),
		Cells:    []FUSECellSnapshot{},
		Gates:    make(map[string]FUSEGateSnapshot, NumFUSEGates),
	}

	// Every source and every op key is ALWAYS present so the JSON shape is
	// stable before the first sample (same contract as trackedTypes).
	bySrc := make([]FUSEStatsSnapshot, NumFUSESources)
	byOp := make([]FUSEStatsSnapshot, NumFUSEOps)
	for s := FUSESource(0); s < NumFUSESources; s++ {
		for o := FUSEOp(0); o < NumFUSEOps; o++ {
			cell := loadCell(&r.fuseCells[s][o])
			if cell.Calls == 0 {
				continue
			}
			bySrc[s].add(cell)
			byOp[o].add(cell)
			cell.finish()
			out.Cells = append(out.Cells, FUSECellSnapshot{
				Source:            s.String(),
				Op:                o.String(),
				FUSEStatsSnapshot: cell,
			})
		}
	}
	for s := FUSESource(0); s < NumFUSESources; s++ {
		v := bySrc[s]
		v.finish()
		out.BySource[s.String()] = v
	}
	for o := FUSEOp(0); o < NumFUSEOps; o++ {
		v := byOp[o]
		v.finish()
		out.ByOp[o.String()] = v
	}

	r.fuseGateMu.RLock()
	gateFn := r.fuseGateFn
	r.fuseGateMu.RUnlock()
	var levels [NumFUSEGates]FUSEGateLevel
	if gateFn != nil {
		levels = gateFn()
	}
	for g := FUSEGate(0); g < NumFUSEGates; g++ {
		cell := &r.fuseGates[g]
		out.Gates[fuseGateNames[g]] = FUSEGateSnapshot{
			Depth:    levels[g].Depth,
			Cap:      levels[g].Cap,
			MaxDepth: cell.maxDepth.Load(),
			Acquires: cell.acquires.Load(),
			Blocked:  cell.blocked.Load(),
			WaitNs:   cell.waitNs.Load(),
			Timeouts: cell.timeouts.Load(),
		}
	}
	return out
}

// fuseAttribState is embedded in Registry (see metrics.go). It is a separate
// type only so the fixed arrays and their provider hook stay in this file.
type fuseAttribState struct {
	fuseCells [NumFUSESources][NumFUSEOps]fuseCell
	fuseGates [NumFUSEGates]fuseGateCell

	fuseGateMu sync.RWMutex
	fuseGateFn func() [NumFUSEGates]FUSEGateLevel
}
