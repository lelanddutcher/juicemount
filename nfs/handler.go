package nfs

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"net"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/internal/testguard"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/metadata"
)

// JuiceMountHandler implements the go-nfs Handler interface, serving
// metadata from SQLite and proxying file I/O to JuiceFS FUSE.
type JuiceMountHandler struct {
	export         exportIdentity // immutable for this handler/server lifetime
	store          *metadata.Store
	fusePath       string                 // path to hidden JuiceFS FUSE mount
	mountPoint     atomic.Pointer[string] // user-facing NFS mount; published with pinStore
	cacheReader    atomic.Pointer[cache.Reader]
	cacheBinding   atomic.Pointer[cacheBinding]
	demandMu       sync.Mutex // handler-wide, including a replaced cache binding
	demandActive   bool
	demandStopping bool
	demandDone     chan struct{}
	fdPool         *FDPool
	readahead      *ReadaheadManager
	memBuf         *MemoryBuffer
	redisClient    atomic.Pointer[metadata.RedisClient] // for publishing events
	pinStore       atomic.Pointer[pin.Store]            // optional; gates reads when offline mode is on
	presence       atomic.Pointer[PresenceTracker]      // optional; who-has-this-open (Tier-1 #3)
	thumbWarmer    atomic.Pointer[ThumbWarmer]          // optional (#1 hydration pack); nil-safe
	sidecar        *sidecarCache                        // `._` AppleDouble body cache (nav crux); nil-safe
	blipHook       func() bool                          // test override for backendBlipActive (#9)
	// Acceptance-only zero-byte remote-content budget. Immutable after
	// construction; the production read hot path pays one predictable branch,
	// never an os.Getenv call.
	testRefuseFUSEContentReads bool
	metadataTrace              *metadataMutationTrace // constructor-only opt-in; nil adds no observations
	testBeforeInPlaceMutation  func(path, operation string) error
	testAfterInPlaceMutation   func(path, operation string)
	testReadAfterAdmission     func(path string)
	testCacheBeforeIdentity    func(path string)
	testReadAfterServingCheck  func(path string)
	// testAfterInPlaceReadBound runs between the pre-read in-place hole check
	// and the content read (B-9 mid-read hole race).
	testAfterInPlaceReadBound func(path string)
	testReadCachedInfo        func(path string)
	testSyncOnlineMutation    func(*os.File) error
	// Acceptance-only seams: the read hook must perform the real fd.ReadAt;
	// publication models the external JuiceFS daemon cache, after that read.
	previewReadAt    func(*os.File, []byte, int64) (int, error)
	previewAfterRead func(previewJob, []byte, int64)
	previewFinished  func(previewJob)

	// Synthetic inode counter for locally-created entries (atomic)
	inodeCounter atomic.Uint64

	// Write size tracking: path → latest known size (for WCC accuracy).
	// Sticky — entries persist after writers close (QA-16: concurrent
	// closes must not lose the high-water mark).
	writeSizeMu sync.Mutex
	writeSizes  map[string]int64
	// writeSizeAt is the last-touch timestamp for each writeSizes entry, so
	// evictStaleWriteSizes can age out marks whose path went away without
	// passing through juiceFS.Remove/Rename (a remote delete, a reconcile
	// prune, a crashed writer). Kept as a SIDE map rather than widening
	// writeSizes so every existing reader/writer of writeSizes is untouched.
	// Guarded by writeSizeMu; lazily allocated (some tests build the handler
	// as a struct literal).
	writeSizeAt map[string]time.Time
	// Per-path syscall/publication ordering; never held by readers.
	inPlaceMutationMu      sync.Mutex
	inPlaceMutations       map[string]*inPlaceMutationLock
	namespace              namespaceCoordinator
	continuations          continuationGate
	metadataPrepareMu      sync.Mutex
	metadataPrepareStop    chan struct{}
	metadataPrepareDone    chan struct{}
	metadataPrepareStopped bool

	// Active writer refcount: path → number of in-flight write handles.
	// Used by phantom-purge gate to distinguish "active writer right now"
	// from "was ever written" (writeSizes is sticky so it can't answer
	// the first question). Incremented when OpenFile returns a writeFile;
	// decremented on writeFile.Close; entry deleted when count reaches 0.
	// (QA-19 fix, 2026-05-17)
	activeWritersMu sync.Mutex
	activeWriters   map[string]int
	// activeWriterGen advances on every write-open. The sidecar snapshotter
	// samples it before reading a complete metadata body and re-checks it before
	// publishing that body into the cache. This closes the writer-started-and-
	// finished-during-read race without holding the global writer mutex across a
	// (bounded) FUSE read.
	activeWriterGen map[string]uint64

	// READDIRPLUS cookie tracking
	verifierMu sync.Mutex
	verifiers  map[string]verifierData

	// Directory prefetch tracking — prevents redundant prefetches
	prefetchMu  sync.Mutex
	prefetched  map[string]time.Time
	prefetchSem chan struct{} // limits concurrent prefetch goroutines

	// sidecarWarmSem bounds concurrent `._` sidecar warm passes (nav crux):
	// on a readdir, a background pass pre-reads the dir's `._` bodies into the
	// sidecar cache so Finder's per-entry AppleDouble reads are RAM-served.
	sidecarWarmSem chan struct{}
	sidecarWarmMu  sync.Mutex
	sidecarWarmed  map[string]time.Time // dir → last warm (dedupe)

	// Futility breaker (see sidecarWarmDirAsync). Guarded by sidecarWarmMu.
	// Measured 2026-07-29 on cellular AND offline: 264 warm reads, 258 of them
	// burning the full 800ms timeout, 2 entries populated. The warmer never
	// looked at its own outcome, so it retried forever.
	sidecarWarmFailStreak int
	sidecarWarmTrippedAt  time.Time

	// Async phantom-purge dedup (RC drain-latency fix, 2026-06-28). The
	// Stat hot path no longer FUSE-Lstats inline to confirm a phantom; it
	// serves the cached FileInfo and confirms in the background. This set
	// is the singleflight key so a Stat storm on the SAME path (Finder
	// fires Stat-backed ops constantly during a copy) launches at most ONE
	// confirmation goroutine + ONE FUSE Lstat per path in flight, instead
	// of N. Keyed by in-mount filename; entry cleared when the goroutine
	// finishes. See asyncConfirmPhantomPurge + QA-35 / feedback_perf_hot_path.
	// inPlaceHoles guards the IN-PLACE FUSE write path against serving an
	// unwritten region as zeros (task #6). Its read side is one atomic load
	// when no hole exists anywhere — see inplace_contig.go.
	inPlaceHoles inPlaceTracker

	phantomPurgeMu       sync.Mutex
	phantomPurgeInFlight map[string]struct{}

	// U7 async unmirrored-dir refresh (V2.3). When an ONLINE readdir finds
	// ZERO mirror rows for a directory, the RPC returns the mirror's answer
	// (empty) immediately and a background goroutine does the bounded FUSE
	// readdir + mirror insert so the NEXT readdir sees the children — the
	// READDIR RPC itself never blocks on FUSE (root readdir was measured
	// hanging >25s over a cellular relay on the old foreground fallback).
	// dirRefreshInFlight is the per-directory singleflight key set (same
	// pattern as phantomPurgeInFlight): a Finder storm on one directory
	// coalesces to ONE refresh goroutine. dirRefreshSem caps concurrently-
	// refreshing directories (non-blocking acquire at dispatch — excess is
	// shed, and the next readdir on that directory re-fires the refresh).
	dirRefreshMu       sync.Mutex
	dirRefreshInFlight map[string]struct{}
	dirRefreshSem      chan struct{}
	dirRefreshActive   int
	dirRefreshStopping bool
	dirRefreshDone     chan struct{}

	// Verifier cleanup lifecycle
	verifierStop        chan struct{}
	stopMu              sync.Mutex
	stopDone            bool          // guarded by stopMu; set only after every subsystem stops
	spoolFinalizeDone   chan struct{} // guarded by stopMu; retained across Stop retries
	spoolFinalizeCancel context.CancelFunc

	// QA-30 Layer B: singleflight per inode for FromHandle recovery so a
	// burst of identical stale-handle retries (DaVinci can fire 50+/sec
	// on a single inode) collapses to one Lstat + one re-insert. The
	// negative side caches recently-failed recoveries so we don't re-Lstat
	// for a genuinely-gone inode on every retry within the same burst.
	recoveryMu       sync.Mutex
	recoveryInFlight map[uint64]chan struct{} // inode → done-when-closed
	recoveryNegative map[uint64]time.Time     // inode → expiry; if now < expiry, skip

	// Spool routing (Option 2, slice C). When non-nil, O_CREATE writes
	// are routed through the spool instead of going directly to FUSE,
	// decoupling Finder write ack from MinIO upload completion. Gated
	// by JM_SPOOL_ENABLE at startup; nil means the existing fdPool
	// writeFile path is used (the pre-spool behavior). Read path is
	// NOT consulted here in slice C — slice D adds the 3-tier read
	// lookup. Files written via spool are temporarily invisible to
	// reads until the drainer copies them to FUSE (documented
	// limitation in docs/ROADMAP/option-2-spool.md).
	spool            atomic.Pointer[SpoolStore]
	drainer          atomic.Pointer[Drainer]
	spoolLifecycleMu sync.Mutex
	spoolSweeperStop func()        // guarded by spoolLifecycleMu
	spoolSweeperDone chan struct{} // guarded by spoolLifecycleMu
}

type verifierData struct {
	verifier   uint64
	entries    []os.FileInfo
	lastAccess time.Time
}

// lstatNotExistWithTimeout runs os.Lstat in a goroutine bounded by the
// given timeout. Returns (isNotExist, ok) where ok=false means the Lstat
// took longer than the timeout and the caller should fall back to a
// conservative path. On timeout the spawned goroutine is leaked; it
// will terminate naturally when the underlying FUSE syscall returns.
// That's preferable to blocking the request goroutine forever.
// nfsLstatGate caps concurrent in-flight Lstats spawned by lstatWithTimeout
// and lstatNotExistWithTimeout. Under a FUSE wedge (the scenario these
// timeouts exist to handle), each call leaks a goroutine until the
// underlying Lstat returns — which under a sustained wedge is "never."
// Bounded gate prevents thousands of leaked goroutines during a stale-
// handle storm (QA-30 Layer B code review HIGH-1). When saturated, callers
// bail on the gate wait within their own deadline (treated as "FUSE
// degraded, fail conservative").
//
// 24 slots (up from 8): these bounded helpers are now on the per-RPC HOT
// path (juiceFS.Stat/Lstat/ReadDir/OpenFile fall-throughs), not just the
// rare phantom-purge path. Under a JuiceFS wedge the NFS server's 128
// concurrent-RPC budget (rpcSem) was being fully consumed by handlers stuck
// in unbounded os.Lstat/os.Stat, which stopped the server reading new
// requests and staled the whole mount (Finder "error 100060"). Bounding the
// FUSE syscalls converts that into fast per-RPC errors that free the RPC
// slot immediately; the gate caps how many goroutines can be parked in a
// stuck syscall at once (≤24) while healthy cold-cache concurrency flows.
var nfsLstatGate = make(chan struct{}, 24)

// prefetchGate caps concurrent in-flight FUSE ReadDirs spawned by the
// BACKGROUND directory prefetcher (prefetchChildren). It is SEPARATE from
// nfsLstatGate on purpose (RC drain-latency fix, 2026-06-28): the prefetcher
// used to share nfsLstatGate, so under spool-drain load its FUSE ReadDirs —
// blocked on the saturated JuiceFS daemon — consumed the same 24 slots that
// genuine FOREGROUND cache-miss metadata RPCs need, starving them and head-of-
// line-blocking Finder's single NFS connection (the spinner). Background
// prefetch is a best-effort nav-latency optimization, never correctness, so it
// gets its own tiny budget that it can fully saturate without ever touching the
// foreground gate. Bounded small (2) so prefetch itself can't pile onto the
// daemon during a drain. The existing prefetchSem (cap 4) still limits prefetch
// goroutine concurrency; this only stops the FUSE-syscall budget from being
// shared. See QA-35 / feedback_perf_hot_path: never let background work consume
// the foreground hot-path FUSE budget.
//
// Also drawn on by the U7 async unmirrored-dir refresh (refreshUnmirroredDir)
// — the same class of work (a background mirror-warming FUSE ReadDir), so it
// shares this background budget rather than growing total background readdir
// pressure on the daemon. Its goroutine count is separately capped by
// dirRefreshSem (4).
var prefetchGate = make(chan struct{}, 2)

// fuseFstatGate caps concurrent LiveSize fstats (task #65 read-during-drain
// fix). SEPARATE from nfsLstatGate for the same reason as prefetchGate: under a
// drain burst, many NFS reads hit their last (partial) chunk simultaneously and
// each calls LiveSize -> an fstat of the open FUSE fd. Sharing the 24-slot
// foreground gate would let those fstats starve genuine cache-miss metadata RPCs
// (the prefetchGate class of bug). An fstat of an OPEN fd (FGETATTR) returns in
// microseconds on a healthy daemon, so a small budget (8) is ample throughput
// while never touching the foreground hot-path FUSE budget. See QA-35 /
// feedback_perf_hot_path.
var fuseFstatGate = make(chan struct{}, 8)

// warmGate caps concurrent FUSE opens spawned by the OPPORTUNISTIC WARMERS —
// the `._` AppleDouble sidecar warmer (nfs/sidecar.go) and the farm-thumbnail
// hydrator (nfs/thumbwarm.go). SEPARATE from nfsLstatGate, for the same reason
// prefetchGate and fuseFstatGate are (audit P0, 2026-07-28).
//
// Both warmers used to draw from nfsLstatGate — the 24-slot FOREGROUND budget,
// the one an NFS RPC blocks on. The sidecar warmer alone runs effectively
// 48-way (handler.go dirWarm cap 3 x sidecar.go 16) and can enqueue hundreds of
// opens for a single directory. On a LAN each open returns in ~1ms and the
// warmers drain out of the gate faster than a human can notice. On a
// high-latency link each open costs a full round trip, so the warmers can hold
// every one of the 24 foreground slots for seconds at a time — and a user
// navigating at that moment queues BEHIND OPPORTUNISTIC WORK. That inverts the
// intended priority: warming exists to make navigation feel fast, and instead
// it was competing with it.
//
// The whole point of a warmer is that nobody is waiting on it, so it must never
// be able to starve something that IS being waited on. 8 (not 2 like
// prefetchGate) because warming is the mechanism that makes the NEXT navigation
// cheap — throttle it, don't strangle it. Tunable via JM_WARM_GATE.
var warmGate = make(chan struct{}, warmGateCap())

func warmGateCap() int {
	if v := os.Getenv("JM_WARM_GATE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 8
}

// fuseGateForSource routes a FUSE call to the right concurrency budget from the
// attribution label the caller already passes. Deriving the gate from the source
// (rather than each call site naming a gate) means a warmer cannot accidentally
// land on the foreground budget: labelling it correctly for metrics IS what puts
// it on the background gate.
//
// Only the two warmers are re-routed here. Everything else keeps nfsLstatGate,
// deliberately — prefetch and dir-refresh have their own tuned paths and moving
// them is a separate change with its own risk.
func fuseGateForSource(src metrics.FUSESource) (chan struct{}, metrics.FUSEGate) {
	switch src {
	case metrics.FUSESrcSidecarWarm, metrics.FUSESrcThumbWarm:
		return warmGate, metrics.FUSEGateWarm
	default:
		return nfsLstatGate, metrics.FUSEGateNFSLstat
	}
}

// errFUSETimeout is returned by the bounded FUSE helpers when a JuiceFS
// syscall doesn't complete within its deadline (the mount is wedged/slow).
// It is the shared nfslib.ErrFUSETimeout sentinel so the RPC boundary
// (internal/nfs/conn.go) can map it to NFS3ERR_JUKEBOX — the client then
// treats it as "server busy, retry" instead of a permanent error that would
// abort the Finder operation.
var errFUSETimeout = nfslib.ErrFUSETimeout

// fuseStatTimeout is the base bound for hot-path FUSE metadata operations.
// Foreground READ descriptor acquisition uses readOpenTimeout below.
// Long enough that a healthy cold-cache fetch (Redis + a MinIO range get on a
// slow link) completes, short enough that a wedge fails the RPC well within
// the client's mount timeout.
//
// Default is WAN-aware: 800ms on LAN, 2s when JM_WAN_MODE=1. On a LAN the
// hot-path stats are existence checks served from the SQLite mirror first
// (FUSE is fallback-only), and a genuine cold fetch is Redis (~1ms) + a
// MinIO range GET (tens of ms) — 800ms is ample headroom while still
// failing a wedge fast. 800ms was validated against a real recursive
// CFexpress-card Finder copy (2026-06-14): with the cache-only CREATE/LOOKUP
// path it kept CREATE/LOOKUP tail latency ~10x under the soft-mount timeout
// and the deep-tree copy cleared the metadata danger zone that used to trip
// "error 100060". Baking it in as the LAN default makes that durable (the
// prior launchctl setenv was reboot-only). On WAN (high-RTT MinIO over
// Tailscale/cellular) a cold range GET legitimately takes longer, so the
// default stays 2s there to avoid spurious JUKEBOX retries.
//
// An explicit JM_FUSE_OP_TIMEOUT_MS always wins over the WAN-aware default.
var fuseStatTimeout = func() time.Duration {
	if v := os.Getenv("JM_FUSE_OP_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Millisecond
		}
	}
	if os.Getenv("JM_WAN_MODE") == "1" {
		return 2 * time.Second
	}
	return 800 * time.Millisecond
}()

// readOpenTimeout bounds descriptor acquisition for a foreground READ. A cold
// absolute-path open can spend more than 800ms resolving metadata on a healthy
// remote link, before any body read begins. Follow the live profile so a Wi-Fi
// to WAN transition does not require relaunching with JM_WAN_MODE. Stat/Lstat,
// ReadDir and mutation budgets remain independent. Explicit operator bounds win.
func readOpenTimeout() time.Duration {
	if v := os.Getenv("JM_FUSE_OP_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Millisecond
		}
	}
	p := netprofile.Default()
	return readOpenTimeoutFor(p.Class(), p.HighLatency(), fuseStatTimeout)
}

func readOpenTimeoutFor(class netprofile.LinkClass, highLatency bool, base time.Duration) time.Duration {
	if highLatency || class == netprofile.ClassSlow || class == netprofile.ClassMetered {
		if base < 2*time.Second {
			return 2 * time.Second
		}
	}
	return base
}

// mutationOpTimeout is the deadline for FUSE mutations that must complete
// before an NFS RPC can be acknowledged (currently mkdir and symlink; chmod is
// best-effort but shares the same helper).
//
// Keep the measured 800 ms LAN ceiling: it prevents a genuinely wedged FUSE
// mount from parking Finder's RPC slots. On a measured metered/slow or
// high-latency Link, however, 800 ms is not a wedge detector. A healthy remote
// mkdir was observed completing just beyond that boundary, so the server sent
// NFS3ERR_JUKEBOX and macOS surfaced the retry as EACCES/"Permission denied".
// Use the already-live netprofile signal rather than JM_WAN_MODE, which is
// fixed at process start and cannot follow Wi-Fi/cellular transitions.
//
// JM_MUTATION_OP_TIMEOUT_MS is a field/debug override. It is intentionally
// separate from JM_FUSE_OP_TIMEOUT_MS so an operator can widen mutation
// acknowledgement without also making every foreground stat/read wait longer.
func mutationOpTimeout() time.Duration {
	if v := os.Getenv("JM_MUTATION_OP_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Millisecond
		}
	}
	p := netprofile.Default()
	return mutationOpTimeoutFor(p.Class(), p.HighLatency(), fuseStatTimeout)
}

func mutationOpTimeoutFor(class netprofile.LinkClass, highLatency bool, base time.Duration) time.Duration {
	if highLatency || class == netprofile.ClassMetered || class == netprofile.ClassSlow {
		if base < 2*time.Second {
			return 2 * time.Second
		}
	}
	return base
}

// syncColdPopulateTimeout bounds the fast-link foreground wait for the whole
// refresh, including child stats and SQL publication. On timeout the same
// background job continues and a later request sees the populated mirror. Tunable
// via JM_SYNC_COLD_POPULATE_MS for live experimentation; the kill-switch is the
// separate JM_SYNC_COLD_POPULATE=0 (see syncColdPopulateEnabled).
var syncColdPopulateTimeout = func() time.Duration {
	if v := os.Getenv("JM_SYNC_COLD_POPULATE_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Millisecond
		}
	}
	return 50 * time.Millisecond
}()

// syncColdPopulateEnabled is the S3 kill-switch (WAVE 1). Default ON; set
// JM_SYNC_COLD_POPULATE=0 to restore the prior async-only empty-then-pop-in
// behavior. Read per call — only on the zero-row cold path, never the hot path.
func syncColdPopulateEnabled() bool {
	return os.Getenv("JM_SYNC_COLD_POPULATE") != "0"
}

// asyncDirRefreshEnabled gates the U7 async unmirrored-dir refresh. Default
// ON: an online readdir of a directory with zero mirror rows returns the
// mirror's (empty) answer immediately and refreshes the mirror from FUSE in
// the background. JM_ASYNC_DIR_REFRESH=0 restores the prior FOREGROUND
// bounded-FUSE fallback byte-identically (the old code path is kept intact
// behind this switch). Read per call — this only executes on the zero-row
// cold path, never on the serve-from-mirror hot path — so tests (and a live
// launchctl setenv) can flip it without a rebuild.
func asyncDirRefreshEnabled() bool {
	return os.Getenv("JM_ASYNC_DIR_REFRESH") != "0"
}

// statWithTimeout is the os.Stat sibling of lstatWithTimeout. ok=false means
// the underlying Stat didn't complete within the timeout (FUSE wedged).
//
// src attributes the call to whoever issued it (see nfs/fusemetrics.go). It
// is instrumentation only — it changes no timeout, gate, retry or error map.
func statWithTimeout(src metrics.FUSESource, p string, timeout time.Duration) (fi os.FileInfo, err error, ok bool) {
	start := time.Now()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	gateWait, depth, acquired := acquireFUSEGate(nfsLstatGate, timer)
	if !acquired {
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpStat, metrics.FUSEGateNFSLstat,
			0, gateWait, time.Since(start), metrics.FUSEOutcomeGateTimeout)
		return nil, nil, false
	}
	type result struct {
		fi  os.FileInfo
		err error
	}
	ch := make(chan result, 1)
	go func() {
		fi, err := os.Stat(p)
		ch <- result{fi: fi, err: err}
		<-nfsLstatGate // release only after Stat actually returns
	}()
	select {
	case r := <-ch:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpStat, metrics.FUSEGateNFSLstat,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeOK)
		return r.fi, r.err, true
	case <-timer.C:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpStat, metrics.FUSEGateNFSLstat,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeTimeout)
		return nil, nil, false
	}
}

// --- bounded MUTATION syscalls (task #70: .app/.framework bundle copy hang) ---
//
// The read path (stat/lstat/readdir/open) was hardened with *WithTimeout so an
// unbounded FUSE syscall can't park on the RPC reader path and stall the whole
// mount (the 100060 class). The MUTATION syscalls on the SYMLINK/MKDIR/SETATTR
// paths were left UNbounded — and a .app/.framework is the first workload to
// drive SYMLINK + a symlink-FOLLOWING SETATTR (os.Chmod follows the link), so
// under spool-drain load they parked on a loaded JuiceFS, held rpcSem reader
// slots, and stalled the mount. These three mirror statWithTimeout exactly:
// acquire nfsLstatGate (or time out), run the syscall on a goroutine that
// releases the gate only after it returns, and surface ok=false on a wedge so
// the caller returns errFUSETimeout → NFS3ERR_JUKEBOX (client retries) instead
// of the mount stalling. A leaked goroutine completes harmlessly (ch is
// buffered) and releases the gate when the wedged syscall eventually returns.

// symlinkWithTimeout is the bounded os.Symlink sibling. ok=false → FUSE wedged.
func symlinkWithTimeout(src metrics.FUSESource, target, p string, timeout time.Duration) (err error, ok bool) {
	start := time.Now()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	gateWait, depth, acquired := acquireFUSEGate(nfsLstatGate, timer)
	if !acquired {
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpSymlink, metrics.FUSEGateNFSLstat,
			0, gateWait, time.Since(start), metrics.FUSEOutcomeGateTimeout)
		return nil, false
	}
	ch := make(chan error, 1)
	go func() {
		e := os.Symlink(target, p)
		ch <- e
		<-nfsLstatGate // release only after the syscall actually returns
	}()
	select {
	case e := <-ch:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpSymlink, metrics.FUSEGateNFSLstat,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeOK)
		return e, true
	case <-timer.C:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpSymlink, metrics.FUSEGateNFSLstat,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeTimeout)
		return nil, false
	}
}

// mkdirAllWithTimeout is the bounded os.MkdirAll sibling. ok=false → FUSE wedged.
func mkdirAllWithTimeout(src metrics.FUSESource, p string, perm os.FileMode, timeout time.Duration) (err error, ok bool) {
	start := time.Now()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	gateWait, depth, acquired := acquireFUSEGate(nfsLstatGate, timer)
	if !acquired {
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpMkdirAll, metrics.FUSEGateNFSLstat,
			0, gateWait, time.Since(start), metrics.FUSEOutcomeGateTimeout)
		return nil, false
	}
	ch := make(chan error, 1)
	go func() {
		e := os.MkdirAll(p, perm)
		ch <- e
		<-nfsLstatGate
	}()
	select {
	case e := <-ch:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpMkdirAll, metrics.FUSEGateNFSLstat,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeOK)
		return e, true
	case <-timer.C:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpMkdirAll, metrics.FUSEGateNFSLstat,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeTimeout)
		return nil, false
	}
}

// chmodWithTimeout is the bounded os.Chmod sibling. os.Chmod FOLLOWS symlinks,
// so a framework's nested links drive it into the most contended JuiceFS
// resolution — exactly the path that wedged. ok=false → FUSE wedged.
func chmodWithTimeout(src metrics.FUSESource, p string, mode os.FileMode, timeout time.Duration) (err error, ok bool) {
	start := time.Now()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	gateWait, depth, acquired := acquireFUSEGate(nfsLstatGate, timer)
	if !acquired {
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpChmod, metrics.FUSEGateNFSLstat,
			0, gateWait, time.Since(start), metrics.FUSEOutcomeGateTimeout)
		return nil, false
	}
	ch := make(chan error, 1)
	go func() {
		e := os.Chmod(p, mode)
		ch <- e
		<-nfsLstatGate
	}()
	select {
	case e := <-ch:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpChmod, metrics.FUSEGateNFSLstat,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeOK)
		return e, true
	case <-timer.C:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpChmod, metrics.FUSEGateNFSLstat,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeTimeout)
		return nil, false
	}
}

// readDirWithTimeout is the os.ReadDir sibling. ok=false → FUSE wedged.
// gate bounds concurrent in-flight ReadDirs: foreground cold-readdir RPCs pass
// nfsLstatGate (the shared hot-path budget); the BACKGROUND prefetcher passes
// prefetchGate so its FUSE ReadDirs can never consume foreground slots during a
// spool drain (RC drain-latency fix). See prefetchGate's doc.
func readDirWithTimeout(src metrics.FUSESource, p string, timeout time.Duration, gate chan struct{}) (ents []os.DirEntry, err error, ok bool) {
	start := time.Now()
	gid := fuseGateID(gate)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	gateWait, depth, acquired := acquireFUSEGate(gate, timer)
	if !acquired {
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpReadDir, gid,
			0, gateWait, time.Since(start), metrics.FUSEOutcomeGateTimeout)
		return nil, nil, false
	}
	type result struct {
		ents []os.DirEntry
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		ents, err := os.ReadDir(p)
		ch <- result{ents: ents, err: err}
		<-gate
	}()
	select {
	case r := <-ch:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpReadDir, gid,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeOK)
		return r.ents, r.err, true
	case <-timer.C:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpReadDir, gid,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeTimeout)
		return nil, nil, false
	}
}

// infoWithTimeout is the os.DirEntry.Info sibling. On darwin a DirEntry from
// a FUSE-backed os.ReadDir carries its type (macFUSE supplies d_type) but NOT
// its FileInfo, so Info() is a LAZY lstat(2) against FUSE — the same
// wedge-able syscall every other *WithTimeout helper here bounds. Batch-3
// adversarial review #0: the U7 refresh worker's per-child Info() loop was
// untimed and ungated, so a wedged/slow juicefs parked the worker goroutine
// indefinitely while it held its dirRefreshSem slot AND the directory's
// singleflight key — four such parks and async dir refresh was silently dead
// for the rest of the session (unmirrored dirs listed empty forever, no log,
// no fallback).
//
// ok=false → the lstat didn't complete inside the budget; callers must treat
// the WHOLE listing pass as abandoned (break, not continue) so worker cleanup
// (sem/key release) runs promptly. Gate-parameterized like readDirWithTimeout
// (QA-35 discipline): the foreground cold-readdir fallback passes
// nfsLstatGate (the shared hot-path budget); background workers (U7 async
// refresh) pass prefetchGate so they can never consume foreground slots. On
// timeout the spawned goroutine still holds its gate slot until the lstat
// actually returns — the same bounded leak every sibling accepts.
func infoWithTimeout(src metrics.FUSESource, de os.DirEntry, timeout time.Duration, gate chan struct{}) (fi os.FileInfo, err error, ok bool) {
	start := time.Now()
	gid := fuseGateID(gate)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	gateWait, depth, acquired := acquireFUSEGate(gate, timer)
	if !acquired {
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpDirEntryInfo, gid,
			0, gateWait, time.Since(start), metrics.FUSEOutcomeGateTimeout)
		return nil, nil, false
	}
	type result struct {
		fi  os.FileInfo
		err error
	}
	ch := make(chan result, 1)
	go func() {
		fi, err := de.Info()
		ch <- result{fi: fi, err: err}
		<-gate // release slot only after Info actually returns
	}()
	select {
	case r := <-ch:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpDirEntryInfo, gid,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeOK)
		return r.fi, r.err, true
	case <-timer.C:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpDirEntryInfo, gid,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeTimeout)
		return nil, nil, false
	}
}

// openFileWithTimeout is the os.OpenFile sibling. ok=false → FUSE wedged.
// On timeout the leaked goroutine's *os.File (if the open eventually
// succeeds) is closed so we don't leak an fd.
// NOTE (attribution): this helper hard-codes nfsLstatGate — the 24-slot
// FOREGROUND budget — and BOTH background warmers call it (nfs/sidecar.go
// warmSidecar, up to 48-way; nfs/thumbwarm.go hydrateOne). That is the
// prefetchGate/fuseFstatGate doctrine violation the attribution counters
// exist to size. Measure first: the src label makes warmer-vs-foreground
// consumption of this gate visible in /metrics. Do NOT change the gate here.
func openFileWithTimeout(src metrics.FUSESource, p string, flag int, perm os.FileMode, timeout time.Duration) (f *os.File, err error, ok bool) {
	return openWithTimeout(src, timeout, func() (*os.File, error) {
		return os.OpenFile(p, flag, perm)
	})
}

// openWithTimeout is the gate+timeout core, parameterised by the opener so a
// caller can supply a HARDENED open (e.g. derivatives.OpenRegularUnder's
// component-wise walk) and still get the wedged-FUSE bound. Everything about
// gating, metrics and the bail-out fd close is identical.
func openWithTimeout(src metrics.FUSESource, timeout time.Duration, open func() (*os.File, error)) (f *os.File, err error, ok bool) {
	start := time.Now()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	// Warmers draw from warmGate, not the foreground budget — see
	// fuseGateForSource / warmGate.
	gate, gateLabel := fuseGateForSource(src)
	gateWait, depth, acquired := acquireFUSEGate(gate, timer)
	if !acquired {
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpOpen, gateLabel,
			0, gateWait, time.Since(start), metrics.FUSEOutcomeGateTimeout)
		return nil, nil, false
	}
	type result struct {
		f   *os.File
		err error
	}
	ch := make(chan result, 1)
	go func() {
		f, err := open()
		ch <- result{f: f, err: err}
		<-gate
	}()
	select {
	case r := <-ch:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpOpen, gateLabel,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeOK)
		return r.f, r.err, true
	case <-timer.C:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpOpen, gateLabel,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeTimeout)
		// Close the fd if the open completes after we've bailed.
		go func() {
			if r := <-ch; r.f != nil {
				_ = r.f.Close()
			}
		}()
		return nil, nil, false
	}
}

// pooledReadOpenWithTimeout is openWithTimeout's FDPool-aware sibling. A
// successful FDPool.Get returns a reference that must be balanced with
// Release, not by closing the shared descriptor. On timeout a cleanup watcher
// waits for the late open and releases that reference; this closes the
// open-during-cancel leak without corrupting the pool's ref accounting.
func pooledReadOpenWithTimeout(src metrics.FUSESource, pool *FDPool, p string, timeout time.Duration) (f *os.File, err error, ok bool) {
	start := time.Now()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	gate, gateLabel := fuseGateForSource(src)
	gateWait, depth, acquired := acquireFUSEGate(gate, timer)
	if !acquired {
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpOpen, gateLabel,
			0, gateWait, time.Since(start), metrics.FUSEOutcomeGateTimeout)
		return nil, nil, false
	}
	type result struct {
		f   *os.File
		err error
	}
	ch := make(chan result, 1)
	go func() {
		fd, openErr := pool.Get(p)
		ch <- result{f: fd, err: openErr}
		<-gate
	}()
	select {
	case r := <-ch:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpOpen, gateLabel,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeOK)
		return r.f, r.err, true
	case <-timer.C:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpOpen, gateLabel,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeTimeout)
		go func() {
			if r := <-ch; r.err == nil && r.f != nil {
				pool.Release(p)
			}
		}()
		return nil, nil, false
	}
}

func lstatNotExistWithTimeout(src metrics.FUSESource, p string, timeout time.Duration) (isNotExist, ok bool) {
	start := time.Now()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	gateWait, depth, acquired := acquireFUSEGate(nfsLstatGate, timer)
	if !acquired {
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpLstatNotExist, metrics.FUSEGateNFSLstat,
			0, gateWait, time.Since(start), metrics.FUSEOutcomeGateTimeout)
		return false, false
	}
	type result struct {
		err error
	}
	ch := make(chan result, 1)
	go func() {
		_, err := os.Lstat(p)
		ch <- result{err: err}
		<-nfsLstatGate // release slot only after Lstat actually returns
	}()
	select {
	case r := <-ch:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpLstatNotExist, metrics.FUSEGateNFSLstat,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeOK)
		return os.IsNotExist(r.err), true
	case <-timer.C:
		// Worker still holds gate until its Lstat returns; bounded leak.
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpLstatNotExist, metrics.FUSEGateNFSLstat,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeTimeout)
		return false, false
	}
}

// lstatWithTimeout is the FileInfo-returning sibling of
// lstatNotExistWithTimeout. ok=false means the underlying Lstat didn't
// complete within the timeout; callers should fall back to a safe default
// (typically: treat the entry as unknown rather than blocking the request
// goroutine on a wedged FUSE daemon).
func lstatWithTimeout(src metrics.FUSESource, p string, timeout time.Duration) (fi os.FileInfo, ok bool) {
	start := time.Now()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	// QA-30 Layer B HIGH-1: bounded gate so a FUSE wedge can't leak
	// unbounded goroutines. Shared with lstatNotExistWithTimeout.
	gateWait, depth, acquired := acquireFUSEGate(nfsLstatGate, timer)
	if !acquired {
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpLstat, metrics.FUSEGateNFSLstat,
			0, gateWait, time.Since(start), metrics.FUSEOutcomeGateTimeout)
		return nil, false
	}
	type result struct {
		fi  os.FileInfo
		err error
	}
	ch := make(chan result, 1)
	go func() {
		fi, err := os.Lstat(p)
		ch <- result{fi: fi, err: err}
		<-nfsLstatGate // release slot only after Lstat actually returns
	}()
	select {
	case r := <-ch:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpLstat, metrics.FUSEGateNFSLstat,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeOK)
		if r.err != nil {
			return nil, true // call completed but failed (e.g., ENOENT); caller decides
		}
		return r.fi, true
	case <-timer.C:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpLstat, metrics.FUSEGateNFSLstat,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeTimeout)
		return nil, false
	}
}

// HandlerOption customizes construction-time tuning of a handler.
// Applied once in NewHandler — none of these mutate a live handler.
type HandlerOption func(*handlerOptions)

type handlerOptions struct {
	memBufThreshold      int64 // bytes; <= 0 → DefaultMemBufThreshold
	memBufBudget         int64 // bytes; <= 0 → DefaultMemBufBudget
	nativePreviewWarmSet bool
	nativePreviewWarm    bool
}

// WithNativePreviewWarm copies a mount-scoped opt-in. Nil retains the legacy
// process environment switch; an explicit false overrides an ambient true.
func WithNativePreviewWarm(enabled *bool) HandlerOption {
	return func(o *handlerOptions) {
		if enabled != nil {
			o.nativePreviewWarmSet = true
			o.nativePreviewWarm = *enabled
		}
	}
}

// WithMemBufLimits sets the memory-buffer file-size threshold and total
// heap budget, in bytes. Values <= 0 keep the package defaults — callers
// can pass an unset (zero) config value straight through (LB-4 back-compat
// with config JSON written before these knobs existed).
func WithMemBufLimits(thresholdBytes, budgetBytes int64) HandlerOption {
	return func(o *handlerOptions) {
		o.memBufThreshold = thresholdBytes
		o.memBufBudget = budgetBytes
	}
}

func NewHandler(store *metadata.Store, fusePath string, opts ...HandlerOption) *JuiceMountHandler {
	var ho handlerOptions
	for _, opt := range opts {
		opt(&ho)
	}
	var previewOptIn []bool
	if ho.nativePreviewWarmSet {
		previewOptIn = []bool{ho.nativePreviewWarm}
	}
	fdPool := NewFDPool()
	h := &JuiceMountHandler{
		export:    newExportIdentity(rand.Reader),
		store:     store,
		fusePath:  fusePath,
		fdPool:    fdPool,
		readahead: NewReadaheadManager(fusePath, fdPool, netprofile.Default(), previewOptIn...),
		// NewMemoryBuffer maps <= 0 to the package defaults.
		memBuf:                     NewMemoryBuffer(ho.memBufThreshold, ho.memBufBudget),
		writeSizes:                 make(map[string]int64),
		activeWriters:              make(map[string]int),
		activeWriterGen:            make(map[string]uint64),
		verifiers:                  make(map[string]verifierData),
		prefetched:                 make(map[string]time.Time),
		prefetchSem:                make(chan struct{}, 4), // max 4 concurrent prefetches
		phantomPurgeInFlight:       make(map[string]struct{}),
		dirRefreshInFlight:         make(map[string]struct{}),
		dirRefreshSem:              make(chan struct{}, 4), // max 4 concurrently-refreshing dirs (U7)
		verifierStop:               make(chan struct{}),
		sidecar:                    newSidecarCache(),      // `._` AppleDouble body cache (nav crux)
		sidecarWarmSem:             make(chan struct{}, 3), // max 3 concurrent dir warm passes
		sidecarWarmed:              make(map[string]time.Time),
		testRefuseFUSEContentReads: testguard.RefuseFUSEContentReads(),
		metadataTrace:              newMetadataMutationTrace(os.Getenv("JM_DIAGNOSTIC_METADATA_TRACE_PREFIX"), os.Getenv("JM_APP_ISOLATION")),
	}
	h.readahead.refuseFUSEContentReads = h.testRefuseFUSEContentReads
	h.readahead.previewRun = h.runNativePreview
	h.readahead.mappingCapture = h.captureReadaheadMapping
	h.memBuf.refuseFUSEContentReads = h.testRefuseFUSEContentReads
	// Subtree-rename → pooled-fd invalidation (serving-path integrity).
	// metadata/ owns the "a subtree moved" signal but cannot import nfs/, so
	// RenameSubtree publishes it through this hook and we translate the two
	// in-mount directory paths into FUSE paths for the pool. juiceFS.Rename
	// also invalidates both ends directly — this makes RenameSubtree
	// self-defending for any other caller, and the two are idempotent.
	if store != nil {
		store.SetOnSubtreeRenamed(func(oldDir, newDir string) {
			h.invalidatePooledFDTree(oldDir)
			h.invalidatePooledFDTree(newDir)
		})
		// R1: REMOTE mutations (a peer, the farm, OpenLoupe — this is an
		// explicitly multi-writer product) reached the mirror and stopped there,
		// so a pooled fd survived a delete/rename it never saw and kept serving
		// the previous inode behind that name. Same hook idiom, same reason
		// metadata/ can't do it itself.
		store.SetOnPathInvalidated(h.onRemotePathInvalidated)
		// Same-inode overwrites keep pooled FUSE descriptors valid but replace
		// JuiceFS slice IDs, so byte-cache invalidation is a separate seam.
		// Full reconcile uses the coarse reset to cover missed push events.
		store.SetOnContentInvalidated(h.onContentInvalidated)
		store.SetOnScanContentInvalidated(h.onScanContentInvalidated)
		store.SetOnContentReset(h.onContentReset)
	}

	// L5 (2026-08-06): publish the fd pool + memory buffer so /metrics can
	// report descriptor occupancy. Both Stats() accessors already existed and
	// neither had a caller, so the process ran with zero fd visibility — see
	// fdobservability.go for why that matters (orphaned-fd leak path, no
	// Setrlimit anywhere in the repo).
	publishFDStatsSource(fdPool, h.memBuf)

	go h.verifierCleanupLoop(60*time.Second, 5*time.Minute)
	return h
}

// SetCacheReader attaches a direct SSD cache reader for bypassing FUSE on cached reads.
func (h *JuiceMountHandler) SetCacheReader(cr *cache.Reader) {
	h.cacheReader.Store(cr)
}

// invalidateReadCaches drops any cached bytes for path from BOTH the memory
// buffer and the direct-SSD slice cache, so a read after an overwrite / rename
// / delete / re-drain never serves the PREVIOUS generation's content. The
// slice cache maps inode -> JuiceFS slice IDs and was otherwise NEVER
// invalidated in production, so an in-place overwrite (same inode, new slice
// IDs) kept returning the old blocks (silent stale-content corruption on the
// re-export / NLE-relink round-trip). Invalidating is cheap and idempotent —
// a spurious invalidation just forces a correct FUSE/MinIO re-fetch — so we
// call it broadly. The inode is resolved from the live metadata cache; if the
// entry is already evicted (delete/rename source) there is nothing cached to
// serve under it, so a miss is fine.
// NOTE (2026-07-28): invalidateReadCaches deliberately does NOT invalidate the
// FDPool. It is called from writeFile.Close, which the go-nfs fork runs on
// EVERY WRITE RPC (OpenFile→Write→Close per RPC) — dropping the pooled write fd
// there would reopen the file on FUSE (~60 ms) once per RPC, which is the exact
// cost the pool exists to amortize, and would do it for a path whose identity
// never changed. Pooled-fd invalidation is wired explicitly at the two sites
// where a path's identity actually changes (juiceFS.Rename and juiceFS.Remove)
// via invalidatePooledFDs / invalidatePooledFDTree below.
func (h *JuiceMountHandler) invalidateReadCaches(path string) {
	if h.memBuf != nil {
		h.memBuf.Invalidate(path)
	}
	if cr := h.cacheReader.Load(); cr != nil {
		if e := h.store.LookupByPath(path); e != nil {
			cr.InvalidateSliceCache(e.Inode)
		}
	}
}

// invalidatePooledFDs drops the pooled read AND write fds for one in-mount
// path, so no later RPC can be served an fd that still points at the PREVIOUS
// inode behind that name. Takes the in-mount filename (the form the billy
// layer hands us) and converts to the FUSE path the pool is keyed by.
// See FDPool.Invalidate for the corruption modes this closes.
func (h *JuiceMountHandler) invalidatePooledFDs(inMountPath string) {
	if h == nil || h.fdPool == nil || inMountPath == "" {
		return
	}
	h.fdPool.Invalidate(path.Join(h.fusePath, inMountPath))
}

// invalidatePooledFDTree is invalidatePooledFDs extended to everything pooled
// BENEATH the path as well — the directory-rename case, where one syscall
// re-parents every descendant and thus staleness every descendant's pooled fd
// at once. Safe (and cheap) to call for a plain file: the subtree is empty.
func (h *JuiceMountHandler) invalidatePooledFDTree(inMountPath string) {
	if h == nil || h.fdPool == nil || inMountPath == "" {
		return
	}
	h.fdPool.InvalidateTree(path.Join(h.fusePath, inMountPath))
}

// onRemotePathInvalidated is the consumer end of Store.SetOnPathInvalidated
// (R1): a delete or rename reached us as a metadata pub/sub event and was
// applied to the mirror only. The FDPool is keyed by path alone and never saw
// it, so
//
//	read X.mov here (fd pooled) → a peer deletes X.mov → the mirror entry drops
//	→ someone recreates X.mov and it re-mirrors → OpenFile takes the `e != nil`
//	branch → fdPool.Get hands back the fd to the DELETED inode
//
// served the previous generation's bytes with no error anywhere — C1/C3 with a
// remote actor.
//
// READ SLOTS ONLY — the name is a misnomer inherited from the original patch
// and the misconception behind it. `juicemount:metadata` is NOT a remote-only
// channel: it replays this process's own writes, and MetadataEvent carries no
// origin field, so applyEvent cannot distinguish them. Invalidating the WRITE
// slot here therefore tore down our own in-flight write — it silently lost
// xattrs on every copy, because macOS rewrites the ._ AppleDouble sidecar once
// per xattr and each drain published an event back at us. See
// FDPool.InvalidateReads for the full trace and the qa-battery evidence.
//
// A directory drops its whole pooled read subtree; a file takes the exact-key
// path, because InvalidateTree is an O(len(entries)) scan under the single pool
// mutex (see juiceFS.Rename for why that matters).
func (h *JuiceMountHandler) onRemotePathInvalidated(inMountPath string, isDir bool) {
	if isDir {
		h.invalidatePooledReadFDTree(inMountPath)
		return
	}
	h.invalidatePooledReadFDs(inMountPath)
}

// onContentInvalidated drops derived byte caches while leaving pooled FUSE
// descriptors alone. Metadata fires this BEFORE mutating the mirror, which
// lets invalidateReadCaches resolve the previous inode for an overwrite,
// delete, or rename destination. A directory replacement invalidates all slice
// mappings conservatively because one event can cover an entire subtree.
func (h *JuiceMountHandler) onContentInvalidated(inMountPath string, isDir bool) {
	if h == nil || inMountPath == "" {
		return
	}
	if isDir {
		if h.memBuf != nil {
			h.memBuf.InvalidateTree(inMountPath)
		}
		if cr := h.cacheReader.Load(); cr != nil {
			cr.InvalidateAll()
		}
		return
	}
	h.invalidateReadCaches(inMountPath)
}

// onScanContentInvalidated is called while Store holds its conditional
// publication locks. It must not query Store: the supplied inode is an exact
// pre-mutation RAM or SQL row.
func (h *JuiceMountHandler) onScanContentInvalidated(inMountPath string, oldInode uint64) {
	if h == nil || inMountPath == "" {
		return
	}
	if h.memBuf != nil {
		h.memBuf.Invalidate(inMountPath)
	}
	if cr := h.cacheReader.Load(); cr != nil {
		cr.InvalidateSliceCache(oldInode)
	}
}

// onContentReset is the recovery/backstop seam: after a complete metadata
// reconciliation (or a targeted subtree prune), no locally cached slice map is
// trusted to have observed every intervening content event.
func (h *JuiceMountHandler) onContentReset() {
	if h == nil {
		return
	}
	if h.memBuf != nil {
		h.memBuf.InvalidateAll()
	}
	if cr := h.cacheReader.Load(); cr != nil {
		cr.InvalidateAll()
	}
}

// invalidatePooledReadFDs / invalidatePooledReadFDTree are the read-slot-only
// counterparts of invalidatePooledFDs / invalidatePooledFDTree, for mutations
// this process learned about SECOND-HAND rather than performed itself.
func (h *JuiceMountHandler) invalidatePooledReadFDs(inMountPath string) {
	if h == nil || h.fdPool == nil || inMountPath == "" {
		return
	}
	h.fdPool.InvalidateReads(path.Join(h.fusePath, inMountPath))
}

func (h *JuiceMountHandler) invalidatePooledReadFDTree(inMountPath string) {
	if h == nil || h.fdPool == nil || inMountPath == "" {
		return
	}
	h.fdPool.InvalidateReadsTree(path.Join(h.fusePath, inMountPath))
}

// SetPinStore attaches the pin registry and the user-facing mount point that
// the pin keys are anchored to. When offline mode is on, the read path
// consults this store to fail-fast on un-pinned/un-cached files.
//
// mountPoint is the path the user mounts to (e.g. "/Volumes/zpool"). It is
// used as the prefix when canonicalizing in-mount filenames into the
// absolute paths the pin store keys on.
func (h *JuiceMountHandler) SetPinStore(ps *pin.Store, mountPoint string) {
	// Publish the mount point before the store. A reader that observes the
	// store is therefore guaranteed to observe the matching canonical prefix.
	mp := mountPoint
	h.mountPoint.Store(&mp)
	h.pinStore.Store(ps)
}

// SetThumbWarmer attaches the #1 hydration-pack warmer (optional; the
// readdir hook is nil-safe). Wired by the bridge after the derivative
// index + thumb cache exist.
func (h *JuiceMountHandler) SetThumbWarmer(w *ThumbWarmer) { h.thumbWarmer.Store(w) }

// FlushStaleFDs invalidates every pooled FUSE fd (#12) — called by the
// bridge after a watchdog remount, when all pooled fds reference the dead
// mount. Returns (closed, marked) for the caller's log line.
func (h *JuiceMountHandler) FlushStaleFDs() (closed, marked int) {
	if h == nil || h.fdPool == nil {
		return 0, 0
	}
	return h.fdPool.FlushStale()
}

// canonicalize converts an in-mount relative path (the form go-nfs hands us
// in OpenFile) into the absolute path that the pin store keys on. It is
// tolerant of the various shapes filenames arrive in:
//
//   - "Film Projects/foo.mov"     → "/Volumes/zpool/Film Projects/foo.mov"
//   - "/Film Projects/foo.mov"    → "/Volumes/zpool/Film Projects/foo.mov"
//   - "/Volumes/zpool/foo.mov"    → "/Volumes/zpool/foo.mov" (already absolute)
//
// Falls back to the legacy hardcoded prefix when no mount point is set.
func (h *JuiceMountHandler) canonicalize(filename string) string {
	mp := ""
	if configured := h.mountPoint.Load(); configured != nil {
		mp = *configured
	}
	if mp == "" {
		mp = "/Volumes/zpool"
	}
	// Strip any trailing slash on the mount point for clean concat.
	for len(mp) > 1 && mp[len(mp)-1] == '/' {
		mp = mp[:len(mp)-1]
	}
	// If the filename is already absolute and under the mount, it's already canonical.
	if len(filename) > 0 && filename[0] == '/' {
		// already-absolute under mount?
		if len(filename) >= len(mp) && filename[:len(mp)] == mp {
			return filename
		}
		// leading-slash relative — strip it and concat
		return mp + filename
	}
	return mp + "/" + filename
}

// isPinnedReady reports whether the canonical-form path is in the pin
// store with status=Ready. Used by the offline-mode open gate.
// Indexed lookup — safe to call on every OpenFile.
func (h *JuiceMountHandler) isPinnedReady(canonicalPath string) bool {
	ps := h.pinStore.Load()
	if ps == nil {
		return false
	}
	return ps.IsPinnedReady(canonicalPath)
}

// SetSpool attaches the JuiceMount-side write spool and its drainer, and
// starts the idle-finalize sweeper.
//
// When set (non-nil): juiceFS.Create routes new files through the spool, and
// juiceFS.OpenFile routes subsequent WRITE RPCs to the spool for any path
// with an active spool entry — decoupling Finder write ack from MinIO upload
// completion (writes land on local SSD; the drainer copies to FUSE in the
// background). Pre-spool behavior is preserved when nil: the legacy
// Create/writeFile/fdPool path runs unchanged. Gated by JM_SPOOL_ENABLE at
// the call sites (cmd/jm5/main.go, bridge/cbridge.go).
//
// Must be called before drainer.Start (it registers the drain-complete
// callback, which worker goroutines read).
func (h *JuiceMountHandler) SetSpool(spool *SpoolStore, drainer *Drainer) {
	h.spoolLifecycleMu.Lock()
	defer h.spoolLifecycleMu.Unlock()

	if drainer != nil {
		// Post-drain hook: once a spooled file lands in FUSE, sync its real
		// size into the metadata cache and publish a create event — the
		// spool analogue of writeFile.Close's UpdateSize+publishEvent, which
		// the spool path can't do at write time because the bytes aren't in
		// FUSE yet. Without this, Stat reports the Create-time size 0 until
		// the next Redis reconcile.
		drainer.SetOnDrainAdmission(func(name string) *namespaceOperation {
			return h.namespace.begin([]string{name})
		})
		drainer.SetOnDrainEffects(h.applyLegacyDrainEffects)
		drainer.SetOnDrainSucceeded(h.publishLegacyDrainReconcile)
		// Qualify the row and publish its exact size atomically before shadow
		// eviction. Canceled rows must never reach a path-only size publisher.
		drainer.SetOnDrainCommit(h.store.DrainComplete)
		// Preserve a verified bounded Finder-metadata body until the next
		// rewrite. The callback runs after size publication and before spool
		// cleanup on both per-file and batched drain paths.
		// Exact source bytes are staged by the drainer and applied under the
		// committed-generation guard, rather than rediscovered by path.
		// Empty-`._`-sidecar elision (2026-08-18): lets the drainer complete a
		// metadata-free AppleDouble row without a backend create, removing its
		// mirror entry so the path honestly reads as absent. See
		// onSidecarSkipped / appleDoubleIsDefaultEmpty in sidecar.go.
		// Lever 1 (JM_DRAIN_BATCH_INSERT): batched form of the size-publish +
		// mark-done pair. When the flag is on, the drainer coalesces many files'
		// metadata writes and commits them via this hook in ONE cross-table
		// SQLite transaction (size published BEFORE mark-done per file, task
		// #65). A no-op cost when the flag is off (the drainer never calls it).
		//
		drainer.SetOnBatchDrainCompleteExact(h.store.BatchDrainCompleteExact)
		// Post-materialize hook: once a deferred offline symlink is os.Symlink'd
		// onto FUSE at reconnect, clear its LocalOnly flag — it's now a real
		// backend entry, so the reconcile prune must treat it like any other
		// (no longer the "local-only, don't prune" exemption). The drainer can't
		// reach the entries store directly, so it calls back here.
		drainer.SetOnSymlinkMaterialized(h.onSymlinkMaterialized)
	}
	if spool != nil {
		spool.bindConflictHandler(h)
		// QA-37: wire the sibling metadata.SpoolStore (same DB, spool.Meta())
		// into the entries Store so BatchDrainComplete can mark spool_entries
		// done under SpoolStore.writeMu — serializing the batched mark-done
		// against DeleteActiveByPath / MarkDone (the cancel↔drain race).
		// Independent of the drainer: BatchDrainComplete fails closed if the
		// batch-insert lever ever flushes without this wired. Done here so it is
		// set whenever a spool is attached, even when SetSpool is called with a
		// nil drainer (the drainer's hooks are wired separately by the caller).
		h.store.SetSpoolStore(spool.Meta())

		// NFS closes the file after every WRITE RPC, so finalize is driven
		// by quiescence (idle sweeper), not by Close. Stopped in StopHandler.
		//
		// The idle window must be LONGER than the realistic gap between a
		// single file's consecutive WRITE RPCs under concurrent load. With the
		// old 3s default, a large-offload Finder copy (many files in flight)
		// would interleave RPCs so that one file's writes were >3s apart; the
		// sweeper then finalized it MID-COPY, and the next WRITE for that path
		// hit OpenWrite's 30s reopen-wait (waiting for the backed-up drainer to
		// evict the just-finalized entry) — which exceeds the 40s soft-mount
		// timeout and aborts the whole copy with "error 100060" (ETIMEDOUT).
		// A generous window keeps actively-copied files in `writing` so they
		// are never finalized out from under an in-progress copy. Drain
		// throughput is unaffected (the drainers run continuously on the
		// backlog); only the per-file drain START is delayed by the window.
		// Tunable via JM_SPOOL_SWEEP_IDLE_SEC for unusual workloads.
		idle := 30 * time.Second
		if v := os.Getenv("JM_SPOOL_SWEEP_IDLE_SEC"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				idle = time.Duration(n) * time.Second
			}
		}
		h.spoolSweeperStop = spool.StartSweeper(idle, 0)
	}

	if drainer != nil && drainer.replacementDispatch != nil {
		drainer.replacementDispatch.resume = func(ctx context.Context, id int64) error {
			return h.resumeQueuedReplacement(ctx, drainer, id, func(w metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
				return openReplacementFUSE(drainer.fuseRoot, w)
			})
		}
	}
	// Publish the configured objects only after every callback and the sweeper
	// are wired. NFS begins accepting requests before optional bridge services
	// finish startup, so readers must see either nil or a complete spool setup.
	h.drainer.Store(drainer)
	h.spool.Store(spool)
}

// publishDrainedSize syncs the real drained size and backend mtime into the metadata cache. It
// runs BEFORE the spool index entry is evicted (task #65) so no fresh read can
// snapshot a stale 0/partial Entry.Size in the post-eviction window. A completed
// spool file is an authoritative whole-file image, so this must assign the exact
// size rather than use UpdateSize's concurrent-writer MAX semantics: offline
// replacement can legitimately shrink an existing file, including to zero. On
// a later replacement this path-only legacy hook is unsafe; it is no longer
// wired as the production precommit publisher. Successful callback ownership
// remains a separate boundary.
func (h *JuiceMountHandler) publishDrainedSize(nfsPath string, size int64, mtime time.Time) {
	if mtime.IsZero() {
		mtime = time.Now()
	}
	if err := h.store.SetSizeExact(nfsPath, size, mtime); err != nil {
		jmlog.Warn("publishDrainedSize: SetSizeExact failed (will heal on reconcile)", "path", nfsPath, "error", err.Error())
	}
}

// onSpoolDrained runs after the drainer copies a spooled file into FUSE. It
// updates the metadata cache with the real size/backend mtime and publishes a create event
// so other JuiceMount clients see the file. Inode/local_only are left to the
// normal Redis reconcile (identical to the legacy create lifecycle).
// onSpoolDrained is retained for legacy observers only. A path, size and
// timestamp carry no completion-generation authority and cannot publish effects.
func (h *JuiceMountHandler) onSpoolDrained(string, int64, time.Time) {}

func (h *JuiceMountHandler) publishLegacyDrainReconcile(name string) {
	canonical := strings.TrimPrefix(name, "/")
	if canonical == "" || canonical == "." || strings.HasPrefix(canonical, "/") ||
		path.Clean("/"+canonical) != "/"+canonical {
		return
	}
	h.publishEvent(metadata.MetadataEvent{Op: "reconcile", Path: canonical})
}

// applyLegacyDrainEffects uses the receipt's durable FULL hold and exact mirror
// generation. The retained FD is the original destination description; no
// pathname is reopened for mode. Only bounded local caches run inside the
// Store's generation fence. The reconcile event is a path-only hint.
func (h *JuiceMountHandler) applyLegacyDrainEffects(item metadata.DrainCommitItem, receipt metadata.DrainCommitResult, fd *os.File, body []byte) error {
	if fd == nil || h == nil || h.store == nil {
		return metadata.ErrMirrorConflict
	}
	if err := h.store.ApplyLegacyDrainEffects(item, receipt, func(metadata.DrainCommitResult) {}); err != nil {
		return err
	}
	owned, err := fd.Stat()
	if err != nil || !owned.Mode().IsRegular() || owned.Size() != item.Size {
		return metadata.ErrMirrorConflict
	}
	st, ok := owned.Sys().(*syscall.Stat_t)
	if !ok || fmt.Sprintf("%d:%d", uint64(st.Dev), st.Ino) != receipt.BackingIdentity {
		return metadata.ErrMirrorConflict
	}
	observedSidecar := h.sidecar.captureEntry(item.NFSPath)
	observedMemBuf := h.memBuf.captureEntry(item.NFSPath)
	var trace *metadataMutationInterval
	if h.metadataTrace != nil {
		trace = h.metadataTrace.beginCompletion(item, receipt)
		if trace != nil {
			trace.before(h.fusePath, fd)
		}
	}
	chmodErr := fd.Chmod(receipt.Mode.Perm())
	if trace != nil {
		trace.after(h.fusePath, fd, chmodErr)
	}
	if err := chmodErr; err != nil {
		if !errors.Is(err, syscall.EPERM) {
			return err
		}
		fi, statErr := fd.Stat()
		if statErr != nil || fi.Mode().Perm() != receipt.Mode.Perm() {
			return err
		}
	}
	fi, err := fd.Stat()
	if err != nil || fi.Mode().Perm() != receipt.Mode.Perm() {
		return metadata.ErrMirrorConflict
	}
	if err := h.store.ApplyLegacyDrainEffects(item, receipt, func(metadata.DrainCommitResult) {
		if len(body) != 0 && cacheableMetaName(path.Base(item.NFSPath)) {
			h.sidecar.putIfEntry(item.NFSPath, observedSidecar, body, receipt.Mtime.Unix(), receipt.Size)
		}
		if cr := h.cacheReader.Load(); cr != nil && receipt.Inode != 0 {
			cr.InvalidateSliceCache(receipt.Inode)
		}
		h.memBuf.invalidateIfEntry(item.NFSPath, observedMemBuf)
	}); err != nil {
		return err
	}
	return nil
}

// onSymlinkMaterialized runs after the drainer os.Symlinks a deferred offline
// symlink onto FUSE at reconnect. The link's cache entry was inserted LocalOnly
// at offline-create time (juiceFS.Symlink); now that it's on the backend, clear
// that flag so the reconcile prune treats it as a normal entry. Best-effort: a
// ClearLocalOnly failure just leaves the entry flagged local-only (browsable,
// merely prune-exempt) until the next reconcile — never a copy failure.
func (h *JuiceMountHandler) onSymlinkMaterialized(linkPath string) {
	if err := h.store.ClearLocalOnly(linkPath); err != nil {
		jmlog.Warn("onSymlinkMaterialized: clear local_only failed",
			"path", linkPath, "error", err.Error())
	}
}

// SetPresence attaches the cross-Mac who-has-this-open tracker (Tier-1 #3).
// Nil disables presence reporting entirely.
func (h *JuiceMountHandler) SetPresence(pt *PresenceTracker) { h.presence.Store(pt) }

// SetRedisClient attaches a Redis client for publishing metadata events.
func (h *JuiceMountHandler) SetRedisClient(rc *metadata.RedisClient) {
	h.redisClient.Store(rc)
}

// nextSyntheticInode returns a unique inode for locally-created entries.
// Uses high bit set to distinguish from JuiceFS inodes.
func (h *JuiceMountHandler) nextSyntheticInode() uint64 {
	return h.inodeCounter.Add(1) | (1 << 63)
}

// incActiveWriter records that a new write handle is in flight for this path.
// Pairs with decActiveWriter on writeFile.Close. The Stat phantom-purge gate
// reads this count to decide whether the cache entry must be preserved (any
// nonzero count means the writer's NFS handle is in active use; deleting
// the inode cache entry would surface as ESTALE on the writer's next RPC).
func (h *JuiceMountHandler) incActiveWriter(path string) {
	h.incActiveWriterFrom(path, "unspecified")
}

func (h *JuiceMountHandler) incActiveWriterFrom(path, source string) {
	h.activeWritersMu.Lock()
	if h.activeWriters == nil {
		h.activeWriters = make(map[string]int)
	}
	if h.activeWriterGen == nil {
		h.activeWriterGen = make(map[string]uint64)
	}
	first := h.activeWriters[path] == 0
	h.activeWriters[path]++
	h.activeWriterGen[path]++
	generation := h.activeWriterGen[path]
	writers := h.activeWriters[path]
	if first {
		if pt := h.presence.Load(); pt != nil {
			pt.Open(path)
		}
	}
	h.activeWritersMu.Unlock()
	// A write handle opening on a `._` sidecar means its body is changing —
	// drop any cached copy so a subsequent read repopulates from the fresh
	// file (belt-and-suspenders with the mtime/size mirror-validation).
	if h.sidecar != nil && cacheableMetaName(path[strings.LastIndexByte(path, '/')+1:]) {
		if old, invalidated := h.sidecar.invalidate(path); invalidated {
			jmlog.Debug("sidecar cache invalidated on write-open",
				"path", path,
				"source", source,
				"writer_generation", generation,
				"writer_count", writers,
				"cache_mtime", old.mtime,
				"cache_size", old.size,
				"cache_len", old.length)
		}
	}
}

// decActiveWriter releases one in-flight handle reference. Deletes the map
// entry when the count returns to zero so the map doesn't grow unbounded
// across the process lifetime.
func (h *JuiceMountHandler) decActiveWriter(path string) {
	h.activeWritersMu.Lock()
	if c, ok := h.activeWriters[path]; ok {
		if c <= 1 {
			delete(h.activeWriters, path)
			if pt := h.presence.Load(); pt != nil {
				pt.Close(path)
			}
		} else {
			h.activeWriters[path] = c - 1
		}
	}
	h.activeWritersMu.Unlock()
}

// hasActiveWriter reports whether any write handle is currently in flight
// for the given path. The phantom-purge gate consults this to avoid
// invalidating an active writer's NFS handle. (QA-19 fix.)
func (h *JuiceMountHandler) hasActiveWriter(path string) bool {
	h.activeWritersMu.Lock()
	defer h.activeWritersMu.Unlock()
	return h.activeWriters[path] > 0
}

// trackWriteSize records the high-water mark of written size for a path.
// QA-16 fix (2026-05-17): uses MAX semantics, not absolute set. Under
// concurrent WRITE RPCs each writeFile instance reports the size after
// ITS write. Without MAX, a late RPC writing at a low offset would
// shrink the tracked size, even though earlier RPCs already wrote past
// it. The tracked value is the file's logical size, not any individual
// RPC's contribution — so it must only grow.
func (h *JuiceMountHandler) trackWriteSize(path string, size int64) {
	h.writeSizeMu.Lock()
	if cur, ok := h.writeSizes[path]; !ok || size > cur {
		h.writeSizes[path] = size
	}
	h.touchWriteSizeLocked(path)
	h.writeSizeMu.Unlock()
}

// touchWriteSizeLocked stamps a writeSizes entry as freshly written so the
// age-out sweep (evictStaleWriteSizes) measures idleness, not absolute age.
// Caller holds writeSizeMu. Lazily allocates because struct-literal handlers
// in tests don't run the NewHandler ctor.
func (h *JuiceMountHandler) touchWriteSizeLocked(path string) {
	if h.writeSizeAt == nil {
		h.writeSizeAt = make(map[string]time.Time)
	}
	h.writeSizeAt[path] = time.Now()
}

// clearWriteSize removes the sticky write-size high-water mark for a path.
// Called by juiceFS.Remove: the mark is MAX-only, so leaving it behind lets
// the NEXT (possibly much smaller) file created at the same name inherit the
// DELETED file's size, and Stat over-reports it → mmap readers get a
// kernel zero-fill past real EOF (C4 / #104). See the Remove call site.
func (h *JuiceMountHandler) clearWriteSize(path string) {
	h.writeSizeMu.Lock()
	delete(h.writeSizes, path)
	delete(h.writeSizeAt, path)
	h.writeSizeMu.Unlock()
}

// clampWriteSize forces the tracked size DOWN to size if the current
// high-water mark exceeds it. Truncate is the one writer operation that is
// an authoritative size statement rather than a positional contribution —
// MAX semantics (trackWriteSize) would otherwise resurrect the stale
// pre-truncate size in Stat after the spool entry drains. Concurrent writes
// past the truncation point re-raise the mark via trackWriteSize as usual.
func (h *JuiceMountHandler) clampWriteSize(path string, size int64) {
	h.writeSizeMu.Lock()
	if cur, ok := h.writeSizes[path]; ok && cur > size {
		h.writeSizes[path] = size
		h.touchWriteSizeLocked(path)
	}
	h.writeSizeMu.Unlock()
}

// prefetchChildren scans a directory on FUSE and caches all children into SQLite.
// This runs in the background so that subsequent Stat() calls are instant.
// Also spawns bounded sub-prefetches for immediate subdirectories (one level)
// for Finder's "expanding disclosure triangle" pattern.
func (h *JuiceMountHandler) prefetchChildren(dirname string) {
	// This is speculative remote work, never correctness. A warm READDIR is
	// already fully served by the local mirror; re-reading the same directory
	// through FUSE on a slow/metered link turns one Finder click into Redis plus
	// per-child object/metadata traffic. Suppress it before acquiring any gate or
	// touching FUSE. The live profile makes this react to Wi-Fi/cellular handoff.
	if !allowSpeculativeDirectoryWarm(netprofile.Default().Class()) {
		return
	}
	// Batch-3 adversarial review #2: never warm a scan-filtered namespace
	// (.trash/, .juicemount/) — the #78 open-GC removed their rows and the
	// push/prune paths ignore them by design, so a prefetch here re-mirrored
	// the derivative tree (the ~112k-row source) every session.
	if metadata.ScanFilteredPath(dirname) {
		return
	}
	h.prefetchMu.Lock()
	if t, ok := h.prefetched[dirname]; ok && time.Since(t) < 30*time.Second {
		h.prefetchMu.Unlock()
		return // recently prefetched
	}
	h.prefetched[dirname] = time.Now()
	h.prefetchMu.Unlock()

	fusePath := path.Join(h.fusePath, dirname)
	if dirname == "." {
		fusePath = h.fusePath
	}

	readStart := time.Now()
	// Bounded: this background scan must never park its goroutine (and its OS
	// thread) on a wedged/slow FUSE mount. readDirWithTimeout returns ok=false
	// past the deadline; we abandon the best-effort prefetch rather than leak
	// a blocked thread (crash 2026-06-14). Uses prefetchGate (cap 2), SEPARATE
	// from the foreground nfsLstatGate (RC drain-latency fix): under spool-drain
	// load these background ReadDirs block on the saturated FUSE daemon, and if
	// they shared nfsLstatGate they'd starve foreground cache-miss metadata RPCs
	// and head-of-line-block Finder's NFS connection. See prefetchGate's doc.
	dirEntries, err, ok := readDirWithTimeout(metrics.FUSESrcPrefetch, fusePath, fuseStatTimeout, prefetchGate)
	if !ok {
		return
	}
	if err != nil {
		return
	}
	// A slow directory read is a self-calibrating signal that we're on a
	// high-latency link (cellular / Tailscale / distant backend). It gates the
	// recursive subdir fan-out below — see the comment at that loop.
	readElapsed := time.Since(readStart)

	// S4 (WAVE 1, RC-7): parallelize the per-child de.Info() lstats with a
	// K-bounded worker pool (coldListFanout, class-scaled) so a high-latency
	// link collapses N×RTT → (N/K)×RTT. Store.LookupByPath (RLock) and
	// nextSyntheticInode (atomic) are concurrency-safe; results are collected
	// by index to keep the mirror-insert + subdir order deterministic. This is
	// the background prefetch path (already off the RPC hot path); K only caps
	// how many lstats race, and the readdir was already bounded by prefetchGate.
	// JM_COLD_LIST_FANOUT=0 forces K=1 (serial), restoring prior behavior.
	type prefetchResult struct {
		entry  *metadata.Entry // non-nil ⇒ needs mirror insert
		subdir string          // non-empty ⇒ a subdirectory to one-level pre-warm
	}
	results := make([]prefetchResult, len(dirEntries))
	fanout := coldListFanout(netprofile.Default().Class())
	sem := make(chan struct{}, fanout)
	var wg sync.WaitGroup
	for i, de := range dirEntries {
		// Skip AppleDouble/._ sidecars in the scan: they're filtered out of
		// NFS listings anyway, and each one is a wasted FUSE round-trip
		// (lookup → ENOENT) on a remote link. NOTE: only skipped in this
		// directory-SCAN path; the per-path Stat the kernel uses after a
		// create still lets ._* through (QA-13).
		if strings.HasPrefix(de.Name(), "._") {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, de os.DirEntry) {
			defer wg.Done()
			defer func() { <-sem }()
			info, err := de.Info()
			if err != nil {
				// Don't fail the scan on one bad entry. This only skips ADDING a
				// new mirror row; a child already in the mirror is untouched (we
				// never delete here), so a transient stat failure can't erase a
				// known file from listings.
				jmlog.Debug("prefetch: stat failed for child, skipping insert",
					"dir", dirname, "name", de.Name(), "error", err.Error())
				return
			}
			var inode uint64
			if st, ok := info.Sys().(*syscall.Stat_t); ok {
				inode = st.Ino
			} else {
				inode = h.nextSyntheticInode()
			}
			childPath := path.Join(dirname, info.Name())
			if dirname == "." {
				childPath = info.Name()
			}

			// Batch-3 adversarial review #2: skip scan-filtered children entirely
			// — never mirrored, and never added to subdirs (the fast-link fan-out
			// below was descending from a root READDIR into .juicemount and
			// re-mirroring the derivative tree one level per navigation).
			if metadata.ScanFilteredPath(childPath) {
				return
			}

			var r prefetchResult
			// Only insert if not already cached (cheap pre-filter; the atomic
			// presence check lives in BulkInsertAbsent — see below)
			if h.store.LookupByPath(childPath) == nil {
				r.entry = metadata.MakeEntry(childPath, info.IsDir(), info.Size(), info.ModTime(), inode)
			}
			// Track subdirectories for one-level-deep prefetch
			if info.IsDir() {
				r.subdir = childPath
			}
			results[i] = r
		}(i, de)
	}
	wg.Wait()

	// Assemble in original order (deterministic).
	toInsert := make([]*metadata.Entry, 0, len(dirEntries))
	var subdirs []string
	for _, r := range results {
		if r.entry != nil {
			toInsert = append(toInsert, r.entry)
		}
		if r.subdir != "" {
			subdirs = append(subdirs, r.subdir)
		}
	}

	if len(toInsert) > 0 {
		// Batch-3 adversarial review #1: insert-if-absent, checked inside the
		// store's own critical sections — a fresher row (push event/CREATE/
		// reconcile) landing between the LookupByPath filter above and this
		// batch must never be overwritten by our older FUSE snapshot. Same
		// TOCTOU (and fix) as refreshUnmirroredDir.
		h.store.BulkInsertAbsent(toInsert, 500)
	}

	// Gate the recursive subdir fan-out (remote folder-nav perf fix). The
	// directory the user actually navigated to was just refreshed above (one
	// round-trip). The recursion below PRE-WARMS subdirs they haven't opened
	// yet — a LAN-only optimization that turns into a link flood on
	// cellular/WAN: one folder open was measured triggering ~680 sequential
	// FUSE→Redis round-trips, ~100 ms each on cellular. Skip the fan-out when:
	//   - we're offline (user or auto): a backend scan can't/shouldn't run, or
	//   - the link is slow: readElapsed is well above warm-LAN readdir times
	//     (single-digit ms) and well below cellular RTT.
	const prefetchSlowLinkThreshold = 35 * time.Millisecond
	if pin.IsOffline() || readElapsed > prefetchSlowLinkThreshold {
		if readElapsed > prefetchSlowLinkThreshold {
			jmlog.Debug("prefetch: high-latency link — skipping subdir fan-out",
				"dir", dirname, "readdir_ms", readElapsed.Milliseconds())
		}
		return
	}

	// Fast link: still skip any subdir whose children are ALREADY mirrored —
	// re-reading a subtree we already hold is pure redundant round-trips even
	// on LAN. Bounded by the semaphore; non-blocking acquire.
	for _, sub := range subdirs {
		if kids, _ := h.store.ListChildren(sub); len(kids) > 0 {
			continue // already mirrored — nothing to warm
		}
		h.prefetchMu.Lock()
		_, already := h.prefetched[sub]
		h.prefetchMu.Unlock()
		if already {
			continue
		}
		select {
		case h.prefetchSem <- struct{}{}:
			go func(dir string) {
				defer func() { <-h.prefetchSem }()
				h.prefetchChildren(dir)
			}(sub)
		default:
			// all workers busy, skip
		}
	}
}

// asyncConfirmPhantomPurge confirms-and-purges a suspected phantom cache entry
// OFF the synchronous Stat hot path (RC drain-latency fix, 2026-06-28 —
// QA-35 / feedback_perf_hot_path). The caller has already served the cached
// FileInfo; this runs the bounded FUSE Lstat in the background under
// nfsLstatGate and ONLY purges on a CONFIRMED isNotExist. On timeout/race it
// does nothing — keep the entry — exactly the prior conservative behavior, just
// no longer head-of-line-blocking Finder's NFS connection while a spool drain
// saturates the FUSE daemon.
//
// Deduped per-path (singleflight via phantomPurgeInFlight): Finder issues
// Stat-backed ops on the same path in a tight storm during a copy, so without
// dedup each Stat would launch its own goroutine + FUSE Lstat. We keep at most
// ONE confirmation in flight per path; concurrent Stats on a path already being
// confirmed return immediately. Uses fuseStatTimeout (the WAN-aware hot-path
// FUSE budget), NOT the old hardcoded 2s.
// phantomConfirmMode is the JM_PHANTOM_CONFIRM override for the slow-link
// gate in asyncConfirmPhantomPurge: "1" = always confirm (pre-2026-07-13
// behavior), "0" = never confirm, anything else = class-gated (skip on
// Metered/Slow). Read once at init — the gate runs per non-dir Stat.
var phantomConfirmMode = os.Getenv("JM_PHANTOM_CONFIRM")

func (h *JuiceMountHandler) asyncConfirmPhantomPurge(filename, fusePath string) {
	// Slow-link gate (2026-07-13, cellular nav diagnosis): each confirmation
	// is one background FUSE Lstat — µs on LAN, but 100-500ms over a tunnel,
	// and ONE 500-entry Finder listing spawns hundreds of them (deduped per
	// path, but every path is distinct). Live-measured on cellular: they
	// saturate nfsLstatGate + the link + juicefs itself (the recurring
	// "mount table query timed out" checkFUSE flap), turning a mirror-served
	// 1s listing into 10-40s — while the payoff is only phantom-entry
	// cleanup that the keyspace push now delivers within seconds anyway
	// (deletions arrive as events; this check predates push). On
	// Metered/Slow links skip the confirmation entirely: the mirror stays
	// authoritative and push + the LAN-gated SCAN own convergence.
	// JM_PHANTOM_CONFIRM=1 forces the confirm everywhere; =0 disables it
	// everywhere. Fast/Medium behavior is byte-for-byte unchanged.
	if phantomConfirmMode == "0" {
		return
	}
	if phantomConfirmMode != "1" {
		if c := netprofile.Default().Class(); c == netprofile.ClassMetered || c == netprofile.ClassSlow {
			return
		}
	}

	h.phantomPurgeMu.Lock()
	if _, inFlight := h.phantomPurgeInFlight[filename]; inFlight {
		h.phantomPurgeMu.Unlock()
		return // a confirmation for this path is already running — coalesce
	}
	h.phantomPurgeInFlight[filename] = struct{}{}
	h.phantomPurgeMu.Unlock()

	go func() {
		defer func() {
			h.phantomPurgeMu.Lock()
			delete(h.phantomPurgeInFlight, filename)
			h.phantomPurgeMu.Unlock()
		}()

		// Re-check the guards that can have changed since the Stat served:
		// a writer may have opened, or the file may have started draining,
		// between serving the cached entry and this goroutine running. Purging
		// then would STALE a live handle. Cheap in-memory checks, no FUSE.
		if h.hasActiveWriter(filename) {
			return
		}
		if h.fdPool != nil && h.fdPool.HasOpenRefs(fusePath) {
			return
		}
		if spool := h.spool.Load(); spool != nil && spool.HasPending(filename) {
			return
		}
		// V2.3 G0: a plain-dir mountpoint (kext not loaded / mount absent)
		// makes Lstat-ENOENT meaningless — every backend file "confirms" as
		// a phantom. Keep the entry; a future Stat reverifies once the mount
		// is real.
		if !pin.FUSEIdentityOK() {
			return
		}

		isNotExist, ok := lstatNotExistWithTimeout(metrics.FUSESrcPhantomPurge, fusePath, fuseStatTimeout)
		if ok && isNotExist {
			// Confirm the entry is still the same phantom before deleting —
			// a concurrent recreate/drain could have re-inserted it.
			if h.store.LookupByPath(filename) == nil {
				return
			}
			jmlog.Warn("purging phantom file (stale cache, confirmed async)", "path", filename)
			h.store.DeleteFromCache(filename)
			go h.store.Delete(filename)
		}
		// !ok (timeout) or exists → keep the entry; a future Stat reverifies.
	}()
}

// publishEvent sends a metadata event via Redis SUBSCRIBE if a client is configured.
func (h *JuiceMountHandler) publishEvent(evt metadata.MetadataEvent) {
	rc := h.redisClient.Load()
	if rc == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rc.PublishEvent(ctx, evt)
	}()
}

// writeSizeMaxAge bounds how long a sticky writeSizes high-water mark may sit
// untouched before evictStaleWriteSizes drops it (C4 backstop).
//
// The mark exists to bridge the window between "a writer produced bytes" and
// "the mirror/SQLite size caught up" — milliseconds to seconds in practice
// (writeFile.Close → store.UpdateSize; spool drain → onSpoolDrained; reconcile
// → real backend size). 10 minutes is far beyond any legitimate bridging
// window, so the sweep can only ever hit marks whose path is genuinely gone,
// while still bounding the C4 stale-HIGH-size exposure for deletions that
// never pass through juiceFS.Remove (remote delete, reconcile prune, crashed
// writer). It is idleness, not absolute age: any new write re-stamps the entry.
const writeSizeMaxAge = 10 * time.Minute

// verifierCleanupLoop periodically removes stale verifier, prefetch and
// write-size entries.
func (h *JuiceMountHandler) verifierCleanupLoop(interval, ttl time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			h.evictStaleVerifiers(ttl)
			h.evictStalePrefetched(2 * time.Minute)
			h.evictStaleWriteSizes(writeSizeMaxAge)
			// A writer that died mid-hole would otherwise leak its record
			// forever, keeping the in-place gate non-zero and taxing every read
			// in the process with a map lookup. Dropping it costs no safety:
			// planReadAt already stops holding once the writer goes quiet.
			h.inPlaceHoles.evictStale(writeSizeMaxAge)
		case <-h.verifierStop:
			return
		}
	}
}

// evictStaleWriteSizes drops writeSizes marks that have gone untouched for
// longer than ttl (C4 backstop — see writeSizeMaxAge). Returns the number
// evicted, for tests.
//
// A stale-HIGH mark is a CORRUPTION source, not just clutter: Stat/Lstat serve
// it whenever it exceeds the mirror size, so an mmap reader faults past real
// EOF and the kernel zero-fills the gap. juiceFS.Remove now clears the mark
// directly; this catches the paths that vanish without a Remove RPC.
//
// Two guards keep the sweep from ever UNDER-reporting a live file's size:
//   - an active write handle (a slow/paused ingest can legitimately idle past
//     ttl mid-file), and
//   - a spool entry still pending drain (an offline copy can sit for hours),
//
// in both of which cases the mark is still the authoritative size.
//
// Lock discipline: candidates are snapshotted under writeSizeMu, the guards
// are evaluated with NO lock held (they take activeWritersMu / the spool's own
// locks), then the deletes re-take writeSizeMu and re-verify the timestamp
// hasn't moved. This deliberately avoids nesting writeSizeMu around any other
// lock.
func (h *JuiceMountHandler) evictStaleWriteSizes(ttl time.Duration) int {
	cutoff := time.Now().Add(-ttl)

	h.writeSizeMu.Lock()
	var candidates []string
	for k := range h.writeSizes {
		at, ok := h.writeSizeAt[k]
		if !ok {
			// No timestamp (pre-existing entry, or a struct-literal handler):
			// adopt it NOW rather than evicting on unknown age. It ages out on
			// a later tick if nothing touches it.
			h.touchWriteSizeLocked(k)
			continue
		}
		if at.Before(cutoff) {
			candidates = append(candidates, k)
		}
	}
	h.writeSizeMu.Unlock()

	if len(candidates) == 0 {
		return 0
	}

	evicted := 0
	for _, k := range candidates {
		if h.hasActiveWriter(k) {
			continue
		}
		if spool := h.spool.Load(); spool != nil && spool.HasPending(k) {
			continue
		}
		h.writeSizeMu.Lock()
		if at, ok := h.writeSizeAt[k]; ok && at.Before(cutoff) {
			delete(h.writeSizes, k)
			delete(h.writeSizeAt, k)
			evicted++
		}
		h.writeSizeMu.Unlock()
	}
	if evicted > 0 {
		jmlog.Debug("writeSizes: aged out stale high-water marks", "count", evicted)
	}
	return evicted
}

// evictStalePrefetched removes prefetch tracking entries older than ttl.
func (h *JuiceMountHandler) evictStalePrefetched(ttl time.Duration) {
	cutoff := time.Now().Add(-ttl)
	h.prefetchMu.Lock()
	for k, t := range h.prefetched {
		if t.Before(cutoff) {
			delete(h.prefetched, k)
		}
	}
	h.prefetchMu.Unlock()
}

// evictStaleVerifiers removes verifier entries whose lastAccess is older than ttl.
func (h *JuiceMountHandler) evictStaleVerifiers(ttl time.Duration) {
	cutoff := time.Now().Add(-ttl)
	h.verifierMu.Lock()
	for k, vd := range h.verifiers {
		if vd.lastAccess.Before(cutoff) {
			delete(h.verifiers, k)
		}
	}
	h.verifierMu.Unlock()
}

// Stop cleans up handler resources.
//
// Shutdown order (slice C): drainer FIRST, then spool, then the older
// subsystems. The drainer holds in-flight goroutines that touch the
// spool, and the spool's index/db are still needed during the drainer
// drain-window. Old fdPool and verifierStop sweep after that.
func (h *JuiceMountHandler) StopHandler() bool {
	return h.stopHandlerWithDeadline(30 * time.Second)
}

func (h *JuiceMountHandler) stopHandlerWithDeadline(deadline time.Duration) bool {
	h.stopMu.Lock()
	defer h.stopMu.Unlock()
	if h.stopDone {
		return true
	}
	previewDone := h.readahead.beginPreviewStop()
	metadataDone := h.beginMetadataPreparationStop()
	cacheDone := h.beginBoundCacheStop()
	demandDone := h.beginDemandStop()
	refreshDone := h.beginDirRefreshStop()
	h.continuations.beginStop()
	if !h.namespace.stop(deadline) {
		return false
	}
	if !h.continuations.stop(deadline) {
		return false
	}
	select {
	case <-previewDone:
	case <-time.After(deadline):
		// Retain spool, FD pool, session, and store while source/proof owns them.
		return false
	}
	select {
	case <-metadataDone:
	case <-time.After(deadline):
		// The original descriptor/Reader call still owns handler dependencies.
		return false
	}
	select {
	case <-cacheDone:
	case <-time.After(deadline):
		return false
	}
	select {
	case <-demandDone:
	case <-time.After(deadline):
		return false
	}
	select {
	case <-refreshDone:
	case <-time.After(deadline):
		// A refresh can still be publishing to the mirror. Keep every backing
		// resource alive and retain closed admission until a later retry.
		return false
	}

	h.spoolLifecycleMu.Lock()
	stopSweeper := h.spoolSweeperStop
	sweeperDone := h.spoolSweeperDone
	if stopSweeper != nil && sweeperDone == nil {
		sweeperDone = make(chan struct{})
		h.spoolSweeperDone = sweeperDone
		go func() {
			stopSweeper()
			close(sweeperDone)
		}()
	}
	drainer := h.drainer.Load()
	spool := h.spool.Load()
	h.spoolLifecycleMu.Unlock()
	if sweeperDone != nil {
		select {
		case <-sweeperDone:
			h.spoolLifecycleMu.Lock()
			h.spoolSweeperStop = nil
			h.spoolSweeperDone = nil
			h.spoolLifecycleMu.Unlock()
		case <-time.After(deadline):
			return false
		}
	}
	if drainer != nil {
		if !drainer.Stop(deadline) {
			return false
		}
	}
	if spool != nil {
		if !h.stopSpoolFinalization(spool, deadline) {
			return false
		}
		spool.Stop()
	}
	if h.verifierStop != nil {
		close(h.verifierStop)
	}
	if h.readahead != nil {
		h.readahead.Stop()
	}
	if h.memBuf != nil {
		h.memBuf.Stop()
	}
	if h.fdPool != nil {
		h.fdPool.Stop()
	}
	h.stopDone = true
	return true
}

// Mount handles the NFS MOUNT RPC.
func (h *JuiceMountHandler) Mount(ctx context.Context, conn net.Conn, req nfslib.MountRequest) (status nfslib.MountStatus, hndl billy.Filesystem, auths []nfslib.AuthFlavor) {
	if h.ExportPath() == "" || string(req.Dirpath) != h.ExportPath() {
		return nfslib.MountStatusErrAcces, nil, nil
	}
	return nfslib.MountStatusOk, &juiceFS{handler: h}, []nfslib.AuthFlavor{nfslib.AuthFlavorNull}
}

// Change returns the change interface (for write ops).
func (h *JuiceMountHandler) Change(fs billy.Filesystem) billy.Change {
	if _, ok := fs.(*conflictHandleFS); ok {
		return conflictHandleChange{}
	}
	// macOS can issue a final metadata operation after unlinking an AppleDouble
	// sidecar or removing a directory whose NFS handle it still holds.
	// FromHandle represents that still-valid open-but-unlinked handle with a
	// handle-only filesystem. The name must stay absent from the live mirror,
	// while the late operation succeeds against the old handle just as it would
	// against an unlinked Unix fd.
	if _, ok := fs.(*deletedHandleFS); ok {
		return deletedHandleChange{}
	}
	return &juiceChange{handler: h}
}

// FSStat fills in filesystem statistics.
func (h *JuiceMountHandler) FSStat(ctx context.Context, f billy.Filesystem, stat *nfslib.FSStat) error {
	stat.TotalSize = 1 << 40 // 1TB
	stat.FreeSize = 1 << 39  // 512GB
	stat.AvailableSize = 1 << 39
	stat.TotalFiles = 1 << 20
	stat.FreeFiles = 1 << 19
	stat.AvailableFiles = 1 << 19
	stat.CacheHint = 0
	return nil
}

// ToHandle converts a path to an NFS file handle.
// Binds the inode to this server lifetime, including synthetic and root handles.
func (h *JuiceMountHandler) ToHandle(f billy.Filesystem, path []string) []byte {
	if h.export.validate() != nil {
		return nil
	}
	fullPath := strings.Join(path, "/")

	// Root directory
	if fullPath == "" || fullPath == "." {
		return h.export.handle(1)
	}
	if spool := h.spool.Load(); spool != nil {
		if view := spool.conflictRead(fullPath); view != nil {
			if view.inode == 0 {
				return nil
			}
			return h.export.handle(view.inode)
		}
	}

	// Look up inode from metadata store
	e := h.store.LookupByPath(fullPath)
	if e != nil {
		// Record the synthetic→path mapping for handles handed out via THIS
		// branch too. The create/readdir path assigns COUNTER-based synthetic
		// inodes (nextSyntheticInode = counter|high-bit) that land in the cache,
		// so ToHandle returns them here — not only via the fnv64a fallback
		// below. Both kinds can lose their inodeCache entry when the reconcile
		// swaps in JuiceFS's real inode, stranding the client's handle → ESTALE.
		// RecordSyntheticHandle no-ops for real inodes.
		h.store.RecordSyntheticHandle(e.Inode, fullPath)
		return h.export.handle(e.Inode)
	}

	// Fallback: entry not in cache yet (just created, or FUSE-only).
	// Generate a deterministic handle AND insert a cache entry so that
	// FromHandle can resolve it. Without this, the handle is unresolvable
	// and the client gets NFSStatusStale on subsequent operations.
	hash := fnv.New64a()
	hash.Write([]byte(fullPath))
	inode := hash.Sum64() | (1 << 63) // high bit set = synthetic
	if spool := h.spool.Load(); spool != nil {
		if view, revoked := spool.conflictHandle(inode); view != nil || revoked {
			inode = spool.allocateConflictSafeInode(h)
			if inode == 0 {
				return nil
			}
		}
	}

	// QA-18 fix (2026-05-17): determine the REAL IsDir from FUSE before
	// fabricating the cache entry. Previously this hardcoded IsDir=false,
	// which under cache pressure (BulkInsert evictOldest pushing a parent
	// directory out of the cache after rapid sibling creates) re-created
	// the directory's cache entry as a regular file. Subsequent NFS
	// LOOKUPs for that path saw type=NF3REG, and any child operation
	// (create/open/mkdir under it) returned ENOTDIR to the client.
	// Reproducer: scripts/qa-suite/02-finder.sh "1000 × 1 KiB" test at
	// file-390 ("Not a directory").
	//
	// Lstat against FUSE adds one syscall, but this path only fires on
	// a cache miss — already a slow path. Guard with the same 2-second
	// FUSE-wedge timeout the phantom-purge uses so a stalled juicefs
	// daemon can't block every NFS LOOKUP that lands in this fallback.
	// On Lstat failure / timeout / nil-fi, we fall back to the original
	// hardcoded false; the entry will be corrected on the next sync or
	// LOOKUP that succeeds.
	isDir := false
	// (parameter `path` shadows the package here; use string concat.)
	if fi, ok := lstatWithTimeout(metrics.FUSESrcForeground, h.fusePath+"/"+fullPath, 2*time.Second); ok && fi != nil {
		isDir = fi.IsDir()
	}
	entry := metadata.MakeEntry(fullPath, isDir, 0, time.Now(), inode)
	h.store.InsertToCache(entry)
	go h.store.Insert(entry) // persist async

	// Remember this synthetic handle → path mapping PERMANENTLY (until FIFO
	// eviction), separate from the inodeCache. The 30s reconcile replaces this
	// path's synthetic inode with JuiceFS's real inode, dropping the synthetic
	// key from inodeCache — but the client keeps using this synthetic handle for
	// the file's lifetime. Without this record, its next op → FromHandle miss →
	// ESTALE (error 100070, and the retry-storm path to 100060) mid-copy.
	h.store.RecordSyntheticHandle(inode, fullPath)

	return h.export.handle(inode)
}

// FromHandle resolves an NFS file handle back to a filesystem and path.
func (h *JuiceMountHandler) FromHandle(handle []byte) (billy.Filesystem, []string, error) {
	inode, err := h.export.inode(handle)
	if err != nil {
		return nil, nil, err
	}

	// Root directory
	if inode == 1 {
		return &juiceFS{handler: h}, []string{}, nil
	}
	if spool := h.spool.Load(); spool != nil {
		if view, revoked := spool.conflictHandle(inode); view != nil {
			return &conflictHandleFS{juiceFS: &juiceFS{handler: h}, view: view}, splitPath(view.name), nil
		} else if revoked {
			return nil, nil, &nfslib.NFSStatusError{NFSStatus: nfslib.NFSStatusStale}
		}
	}

	// A synthetic handle can outlive two pathname generations: reconcile first
	// redirects its inode-cache key to the real JuiceFS entry, then Finder
	// unlinks and recreates the same AppleDouble name. The redirect intentionally
	// remains resolvable, but it now points at a deleted entry object while the
	// path cache owns a different live generation. Preserve Unix handle identity
	// by serving that OLD generation as a handle-only tombstone. Without this
	// pre-check, the late Finder WRITE is routed to the recreated pathname and an
	// offline transition either corrupts generation identity or returns
	// NFS3ERR_ACCES because the new file has no complete spool shadow.
	if shadow, ok := h.store.LookupDeletedAlias(inode); ok &&
		(shadow.IsDir || isSidecarName(path.Base(shadow.Path))) {
		return h.deletedHandleFromShadow(inode, shadow)
	}

	// Look up by inode
	e := h.store.LookupByInode(inode)
	if e == nil {
		// REMOVE is authoritative even when the entry was born offline with a
		// synthetic inode. Finder legitimately keeps that old handle and can send
		// continuation WRITEs after unlinking an AppleDouble name. Consult the
		// explicitly-deleted shadow before either eviction recovery or the
		// synthetic path fallback: recovering the latter as an ordinary juiceFS
		// path would route the late WRITE through offline in-place safety, see no
		// spool image (REMOVE cancelled it), and return NFS3ERR_ACCES.
		if shadow, ok := h.store.LookupRecentlyEvicted(inode); ok && shadow.Deleted &&
			(shadow.IsDir || isSidecarName(path.Base(shadow.Path))) {
			return h.deletedHandleFromShadow(inode, shadow)
		}

		// QA-30 Layer B (2026-05-25): before returning STALE, try one-shot
		// recovery from the recently-evicted shadow map. If the entry was
		// removed by a prune/eviction within the last ShadowTTL (5 min)
		// AND FUSE confirms the path still exists, re-insert it and serve
		// normally. Singleflight by inode so DaVinci's scrub retries don't
		// cascade into N redundant Lstat calls.
		//
		// S6 (H2 grader): one inc as the FromHandle cache-miss ENTERS the
		// evicted-recovery branch — the attempt/denominator for the stale-
		// recovery path. Warm-mount expectation ~0; a spike localizes a
		// stale-storm. Atomic-only, no new syscall (the Lstat, if any, is
		// inside tryRecoverEvicted and already existed).
		metrics.Default().IncRecoverLstat()
		recovered, confirmedGone := h.tryRecoverEvicted(inode)
		if recovered != nil {
			parts := splitPath(recovered.Path)
			return h.continuationFS(inode, recovered), parts, nil
		}

		// Finder writes AppleDouble metadata through a normal NFS handle, then
		// may unlink the `._` file and send a late SETATTR/COMMIT on that handle.
		// macOS also sends a late GETATTR after RMDIR while walking a tree it is
		// deleting. A confirmed FUSE ENOENT means the unlink/rmdir won; returning
		// ESTALE for either still-held handle turns a successful delete into a
		// user-visible Finder error. Serve a handle-only tombstone instead:
		// Stat/Lstat expose the shadowed attributes and Change is a no-op, but the
		// entry is NOT promoted back into pathCache/childrenIdx, so READDIR and
		// LOOKUP continue to see the name as deleted. Deleted principal files
		// retain normal ESTALE behavior.
		if confirmedGone {
			if shadow, ok := h.store.LookupRecentlyEvicted(inode); ok &&
				(shadow.IsDir || isSidecarName(path.Base(shadow.Path))) {
				return h.deletedHandleFromShadow(inode, shadow)
			}
		}

		// Synthetic-handle recovery (2026-06-14). A synthetic inode (high bit)
		// that ToHandle handed out for a not-yet-persisted path loses its
		// inodeCache entry when the reconcile swaps in JuiceFS's real inode —
		// but the client still holds the synthetic handle. Resolve it from the
		// persistent synthetic-handle map so the op proceeds on the path instead
		// of failing the client with ESTALE (error 100070 / retry-storm 100060
		// mid-copy). tryRecoverEvicted above deliberately skips synthetic inodes
		// (no Redis shadow); this is their recovery path.
		if inode&(1<<63) != 0 {
			if p, ok := h.store.SyntheticHandlePath(inode); ok {
				return &juiceFS{handler: h}, splitPath(p), nil
			}
		}

		// QA-25 diagnostic (2026-05-20): log every STALE so we can
		// see exactly which inode the kernel is presenting and
		// correlate against what we have in the cache. Remove after
		// QA-25 is closed; the cost of this Warn-level log is small.
		//
		// Residual-STALE attribution (2026-06-28): by this point both
		// recovery paths above have already FAILED for this inode —
		// tryRecoverEvicted (real inodes: shadow + FUSE-Lstat re-confirm)
		// and SyntheticHandlePath (synthetic inodes). The remaining question
		// is WHICH source dropped the entry. `had_shadow` distinguishes a
		// prune/eviction orphan (a Layer-B shadow exists within ShadowTTL, so
		// scopedPrune/syncMetadata's DeletePaths — or evictOldest — removed an
		// entry whose handle Finder still holds, and recovery's racy FUSE
		// Lstat then said "gone") from a genuinely-foreign handle (no shadow).
		// `path_sidecar` flags a `._`-AppleDouble base name — the reconcile
		// prune has NO `._`-skip (unlike the Stat/Open phantom-purge), so a
		// sidecar prune-orphan is the leading residual-STALE hypothesis. This
		// is observability-only; it changes no prune/recovery behavior.
		pathSample := "(no entry)"
		hadShadow := false
		if shadow, ok := h.store.LookupRecentlyEvicted(inode); ok {
			hadShadow = true
			pathSample = shadow.Path
			// S6 (H2 grader): the had-shadow STALE case. Both recovery paths
			// above already FAILED for this inode, yet a Layer-B shadow still
			// exists within ShadowTTL — a prune/rename orphan whose handle the
			// client still holds. Counting only the had-shadow subset (not
			// genuinely-foreign handles) is what makes this a stale-storm
			// localizer. Atomic-only; the shadow lookup already ran for the log.
			metrics.Default().IncRecoverStale()
		}
		ps, is := h.store.CacheStats()
		jmlog.Warn("FromHandle STALE",
			"inode", fmt.Sprintf("%x", inode),
			"inode_synthetic", inode&(1<<63) != 0,
			"had_shadow", hadShadow,
			"path_sidecar", strings.HasPrefix(path.Base(pathSample), "._"),
			"pathCache_size", ps,
			"inodeCache_size", is,
			"sample", pathSample,
		)
		return nil, nil, &nfslib.NFSStatusError{NFSStatus: nfslib.NFSStatusStale}
	}

	parts := splitPath(e.Path)
	return h.continuationFS(inode, e), parts, nil
}

func (h *JuiceMountHandler) deletedHandleFromShadow(inode uint64, shadow metadata.EvictedShadow) (billy.Filesystem, []string, error) {
	entry := &metadata.Entry{
		Path:       shadow.Path,
		Name:       shadow.Name,
		ParentPath: shadow.ParentPath,
		Mode:       shadow.Mode,
		Size:       shadow.Size,
		Mtime:      shadow.Mtime,
		IsDir:      shadow.IsDir,
		Inode:      inode,
	}
	return &deletedHandleFS{
		juiceFS:     &juiceFS{handler: h},
		deletedPath: strings.TrimLeft(shadow.Path, "/"),
		info:        entry.FileInfo(),
	}, splitPath(shadow.Path), nil
}

// tryRecoverEvicted is QA-30 Layer B's recovery path. Called by FromHandle
// on cache-miss for a real (non-synthetic) inode. Looks up the recently-
// evicted shadow map; if present AND FUSE confirms the path still exists,
// re-inserts the entry into the cache and returns it. Otherwise returns
// nil (caller falls through to STALE).
//
// Singleflight by inode: a DaVinci scrub-storm produces 50+ identical
// FromHandle calls per second on the same stale inode. Without
// singleflight, each would race to Lstat + re-insert. The first call
// owns the recovery; later concurrent calls wait on the shared
// done-channel and then re-Lookup the cache (which the first call has
// either populated, or skipped via the negative cache).
//
// Negative cache (recoveryNegative) bounds the Lstat rate on confirmed-
// gone handles. Default TTL 5s — long enough to absorb a typical retry
// burst, short enough that a real "file came back" recovery isn't
// blocked forever.
func (h *JuiceMountHandler) tryRecoverEvicted(inode uint64) (*metadata.Entry, bool) {
	// Synthetic inodes (high bit set) have no shadow entry — they're
	// counter-based ToHandle fallbacks, not real juicefs inodes.
	if inode&(1<<63) != 0 {
		return nil, false
	}
	// Negative cache check.
	h.recoveryMu.Lock()
	if exp, ok := h.recoveryNegative[inode]; ok {
		if time.Now().Before(exp) {
			h.recoveryMu.Unlock()
			return nil, true
		}
		delete(h.recoveryNegative, inode)
	}
	// Singleflight: if another goroutine is already recovering this
	// inode, wait on its done channel then retry the cache lookup.
	if done, ok := h.recoveryInFlight[inode]; ok {
		h.recoveryMu.Unlock()
		<-done
		// Whoever ran the recovery has either populated the cache or
		// installed a negative entry. Re-lookup.
		if recovered := h.store.LookupByInode(inode); recovered != nil {
			return recovered, false
		}
		h.recoveryMu.Lock()
		exp, confirmedGone := h.recoveryNegative[inode]
		h.recoveryMu.Unlock()
		return nil, confirmedGone && time.Now().Before(exp)
	}
	// Become the owner.
	if h.recoveryInFlight == nil {
		h.recoveryInFlight = make(map[uint64]chan struct{}, 8)
	}
	if h.recoveryNegative == nil {
		h.recoveryNegative = make(map[uint64]time.Time, 8)
	}
	done := make(chan struct{})
	h.recoveryInFlight[inode] = done
	h.recoveryMu.Unlock()

	// Cleanup when we return.
	defer func() {
		h.recoveryMu.Lock()
		delete(h.recoveryInFlight, inode)
		close(done)
		h.recoveryMu.Unlock()
	}()

	// Look up the shadow record.
	shadow, ok := h.store.LookupRecentlyEvicted(inode)
	if !ok {
		// Not in shadow → real STALE. Cache the negative briefly so
		// burst retries skip the lookup.
		h.recoveryMu.Lock()
		h.recoveryNegative[inode] = time.Now().Add(5 * time.Second)
		h.recoveryMu.Unlock()
		return nil, false
	}

	// Task #92: a spool-resident (not-yet-drained) write logically EXISTS even
	// though it is invisible to a FUSE Lstat — its bytes are on the spool, not
	// yet in JuiceFS. This is the dominant case for `._` AppleDouble sidecars
	// during a Finder copy: hundreds are created in a burst; one gets evicted
	// into the shadow map under cache churn while still spooling; Finder
	// re-references its (real, path-stable) handle; the FUSE Lstat below returns
	// ENOENT; recovery "fails"; a 5s negative is cached (recoveryNegative); and
	// then EVERY retry of that handle short-circuits to STALE for 5s → the copy
	// stalls / "connection interrupted." The scopedPrune spoolPending guard and
	// the Stat/Open phantom-purge already spare such paths (spool.go:515); this
	// recovery path was the one place that still Lstat'd FUSE without consulting
	// the spool. Recover straight from the spool shadow and skip the doomed
	// (2s-timeout) Lstat entirely — HasPending keys by the same no-leading-slash
	// store path scheme used to build the FUSE path below.
	if spool := h.spool.Load(); spool != nil && spool.HasPending(strings.TrimLeft(shadow.Path, "/")) {
		recovered := h.store.RecoverShadow(shadow, inode)
		jmlog.Info("FromHandle recovered spool-pending evicted entry",
			"inode", fmt.Sprintf("%x", inode),
			"path", shadow.Path,
		)
		return recovered, false
	}

	// Verify the path actually exists in FUSE before recovering.
	fusePath := h.fusePath + "/" + strings.TrimLeft(shadow.Path, "/")
	fi, fok := lstatWithTimeout(metrics.FUSESrcForeground, fusePath, 2*time.Second)
	if !fok {
		// Lstat timed out — FUSE is degraded. Don't recover, don't
		// cache negative (might succeed next time).
		return nil, false
	}
	if fi == nil {
		// File is genuinely gone. Cache negative so burst retries skip.
		h.recoveryMu.Lock()
		h.recoveryNegative[inode] = time.Now().Add(5 * time.Second)
		h.recoveryMu.Unlock()
		return nil, true
	}

	// File exists. Promote the shadow back to live cache.
	recovered := h.store.RecoverShadow(shadow, inode)
	// S6 (H2 grader): the ms-cost success case — an evicted inode re-confirmed
	// via the FUSE Lstat above and promoted back to the live cache. (The
	// spool-pending fast recovery at the top of this function is deliberately
	// NOT counted here: it skips the Lstat and is not the ms-cost path.)
	metrics.Default().IncRecoverSuccess()
	jmlog.Info("FromHandle recovered evicted entry",
		"inode", fmt.Sprintf("%x", inode),
		"path", shadow.Path,
	)
	return recovered, false
}

func (h *JuiceMountHandler) InvalidateHandle(f billy.Filesystem, handle []byte) error {
	return nil // deterministic handles never go stale
}

func (h *JuiceMountHandler) HandleLimit() int {
	return 10000 // deterministic handles have no real limit; large value
	// ensures READDIRPLUS batches many entries per response
}

// CachingHandler methods for READDIRPLUS
func (h *JuiceMountHandler) VerifierFor(path string, contents []os.FileInfo) uint64 {
	hash := fnv.New64a()
	hash.Write([]byte(path))
	var b [8]byte
	for _, fi := range contents {
		hash.Write([]byte(fi.Name()))
		binary.BigEndian.PutUint64(b[:], uint64(fi.ModTime().Unix()))
		hash.Write(b[:])
	}
	v := hash.Sum64()

	h.verifierMu.Lock()
	h.verifiers[path] = verifierData{verifier: v, entries: contents, lastAccess: time.Now()}
	h.verifierMu.Unlock()

	return v
}

func (h *JuiceMountHandler) DataForVerifier(path string, verifier uint64) []os.FileInfo {
	h.verifierMu.Lock()
	defer h.verifierMu.Unlock()
	if vd, ok := h.verifiers[path]; ok && vd.verifier == verifier {
		vd.lastAccess = time.Now()
		h.verifiers[path] = vd
		return vd.entries
	}
	return nil
}

// splitPath splits "a/b/c" into ["a", "b", "c"]
func splitPath(p string) []string {
	p = strings.TrimPrefix(p, "/")
	if p == "" || p == "." {
		return []string{}
	}
	return strings.Split(p, "/")
}

// juiceFS implements billy.Filesystem, serving metadata from SQLite
// and proxying file I/O to the JuiceFS FUSE mount.
type juiceFS struct {
	handler *JuiceMountHandler
}

// deletedHandleFS represents a recently-unlinked AppleDouble file or removed
// directory for operations arriving through its already-issued NFS handle. It
// deliberately embeds the ordinary filesystem for capability/join behavior but
// overrides only metadata lookup for the exact deleted path. It is never
// inserted into the metadata store, so it cannot resurrect the name in a
// directory listing.
type deletedHandleFS struct {
	*juiceFS
	deletedPath string
	info        os.FileInfo
}

func (fs *deletedHandleFS) isDeletedPath(name string) bool {
	return strings.TrimLeft(path.Clean(name), "/") == fs.deletedPath
}

func (fs *deletedHandleFS) Stat(name string) (os.FileInfo, error) {
	if fs.isDeletedPath(name) {
		return fs.info, nil
	}
	return fs.juiceFS.Stat(name)
}

func (fs *deletedHandleFS) Lstat(name string) (os.FileInfo, error) {
	if fs.isDeletedPath(name) {
		return fs.info, nil
	}
	return fs.juiceFS.Lstat(name)
}

func (fs *deletedHandleFS) Open(name string) (billy.File, error) {
	return fs.OpenFile(name, os.O_RDONLY, 0)
}

func (fs *deletedHandleFS) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	if !fs.isDeletedPath(name) {
		return fs.juiceFS.OpenFile(name, flag, perm)
	}
	if fs.info.IsDir() {
		return nil, &os.PathError{Op: "open", Path: name, Err: syscall.EISDIR}
	}
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_TRUNC) == 0 {
		return nil, os.ErrNotExist
	}
	// The namespace entry is gone, but Unix/NFS handle semantics keep the old
	// generation usable until the client releases its handle. The AppleDouble
	// bytes are disposable after unlink: acknowledge positioned continuation
	// writes without recreating a spool/FUSE name. This is deliberately scoped
	// to a deletedHandleFS, which FromHandle creates only for an explicitly
	// removed sidecar (or a confirmed-gone legacy sidecar).
	return &deletedHandleFile{name: path.Base(name)}, nil
}

// deletedHandleFile is a handle-only sink for an already-unlinked AppleDouble
// generation. It implements billy.File and io.WriterAt; no operation can
// recreate the deleted pathname or consume spool capacity.
type deletedHandleFile struct {
	name   string
	offset int64
}

func (f *deletedHandleFile) Name() string                      { return f.name }
func (f *deletedHandleFile) Read([]byte) (int, error)          { return 0, io.EOF }
func (f *deletedHandleFile) ReadAt([]byte, int64) (int, error) { return 0, io.EOF }
func (f *deletedHandleFile) Write(p []byte) (int, error) {
	f.offset += int64(len(p))
	return len(p), nil
}
func (f *deletedHandleFile) WriteAt(p []byte, _ int64) (int, error) { return len(p), nil }
func (f *deletedHandleFile) Seek(offset int64, whence int) (int64, error) {
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = f.offset + offset
	case io.SeekEnd:
		next = offset
	default:
		return 0, syscall.EINVAL
	}
	if next < 0 {
		return 0, syscall.EINVAL
	}
	f.offset = next
	return next, nil
}
func (f *deletedHandleFile) Close() error         { return nil }
func (f *deletedHandleFile) Lock() error          { return nil }
func (f *deletedHandleFile) Unlock() error        { return nil }
func (f *deletedHandleFile) Truncate(int64) error { return nil }

func (jfs *juiceFS) fullPath(filename string) string {
	return path.Join(jfs.handler.fusePath, filename)
}

// cacheProbeHit returns true when the SSD cache reader can serve the
// first block of `e`. Returns false on any miss, timeout, or absent
// cacheReader. Used by OpenFile's C.2/QA-12 path to allow opens of
// "recently cached" files in offline mode.
//
// 200ms is the timeout — see the call site for the rationale.
func (jfs *juiceFS) cacheProbeHit(e *metadata.Entry) bool {
	if e == nil {
		return false
	}
	cr := jfs.handler.cacheReader.Load()
	if cr == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	var scratch [4096]byte
	identity, _ := jfs.handler.store.LookupCachedFileIdentity(e.Path)
	if _, bound := cr.VolumeBinding(); bound && identity.Version != e.Version {
		return false
	}
	n, err := jfs.handler.readCacheBlock(ctx, cr, identity, e.Inode, 0, scratch[:], 200*time.Millisecond)
	return err == nil && n > 0
}

func (jfs *juiceFS) entryToFileInfo(e *metadata.Entry) os.FileInfo {
	if e != nil && e.IsDir {
		mtime := jfs.handler.store.DirectoryVisibilityMtime(e.Path, e.Mtime)
		return e.FileInfoWithModTime(mtime)
	}
	return e.FileInfo()
}

func (jfs *juiceFS) rootFileInfo() os.FileInfo {
	return &rootDirInfo{mtime: jfs.handler.store.DirectoryVisibilityMtime(".", rootMtime)}
}

func (jfs *juiceFS) Stat(filename string) (os.FileInfo, error) {
	if err := jfs.handler.namespace.servingError(filename); err != nil {
		return nil, err
	}
	if filename == "" || filename == "." || filename == "/" {
		return jfs.rootFileInfo(), nil
	}
	filename = strings.TrimPrefix(filename, "/")
	if spool := jfs.handler.spool.Load(); spool != nil {
		if view := spool.conflictRead(filename); view != nil {
			if err := view.check(); err != nil {
				return nil, err
			}
			return view.info(), nil
		}
	}

	// Slice D: spool shadow. If a writer just landed bytes for this
	// path AND the drainer hasn't copied them to FUSE yet, the entry
	// lives only in the in-memory spool index (the metadata store
	// has nothing yet, FUSE has nothing yet). Without this short-
	// circuit, Stat would fall through to FUSE and return ENOENT
	// for an actively-being-written file.
	//
	// QA-35 perf-discipline: the empty-spool Lookup is 8.4 ns
	// (measured in slice A benchmarks). Adding it BEFORE the
	// writeSizes lock + metadata lookup keeps the hot path's
	// per-RPC tax at a no-op when no writes are in flight.
	if spool := jfs.handler.spool.Load(); spool != nil {
		if e, ok := spool.LookupActive(filename); ok {
			return spoolFileInfoForEntry(path.Base(filename), e), nil
		}
	}

	// NO fast-reject of macOS metadata names here. QA-13 (2026-05-17)
	// already removed `._*` after the name-based ErrNotExist broke
	// copyfile(3): Finder/cp create the file then immediately stat it to
	// confirm, and a name-based ENOENT made the kernel conclude the create
	// failed → 0-byte truncation / error. The SAME bug bit the volume
	// system dirs (.fseventsd/.Spotlight-V100/.Trashes/.TemporaryItems/
	// .VolumeIcon.icns/Icon\r) when a user copied a whole volume/card ROOT:
	// Finder created .fseventsd, stat'd it, got our ErrNotExist, and aborted
	// the copy with -36 at the tail (2026-06-14). Anything onCreate lets
	// through MUST round-trip through Stat/Lstat/ReadDir or copying it fails.
	// The lost micro-optimization (skipping a LookupByPath for these probes)
	// is now negligible — LookupByPath is a cache-only map read, and absent
	// names still return ErrNotExist from the lookup below, identically.

	// Check if there's a tracked write size (in-flight write)
	jfs.handler.writeSizeMu.Lock()
	writeSize, hasWriteSize := jfs.handler.writeSizes[filename]
	jfs.handler.writeSizeMu.Unlock()

	e := jfs.handler.store.LookupByPath(filename)
	if e != nil {
		// For non-directory entries, verify the file still exists on FUSE.
		// Stale cache entries (from failed copies or deleted files) cause
		// Finder to report "already exists" for files that don't exist.
		//
		// Bounded: a wedged FUSE daemon (MinIO unreachable, JuiceFS hung,
		// SSD I/O stalled) would otherwise park this syscall forever and,
		// under the current per-connection sequential RPC dispatch, freeze
		// every other RPC on that connection — which is Finder's only NFS
		// connection. 2 s is well above a healthy FUSE stat (~µs) and well
		// below the threshold where Finder starts showing a beachball.
		// On timeout we conservatively keep the cache entry (assume the
		// file still exists) rather than block.
		if !e.IsDir {
			// Gate the phantom-file purge on metadata-authority health.
			// 2026-05-16 incident: Redis at 127.0.0.1 was unreachable
			// for ~30 min overnight. During the outage JuiceFS couldn't
			// resolve its own metadata for some paths and FUSE returned
			// ENOENT for files that genuinely exist. The phantom-purge
			// path then deleted those cache entries; when Redis came
			// back, the SQLite store was inconsistent with Redis, and
			// NFS handles that had been issued before the purge pointed
			// at a different inode — producing "Stale NFS file handle"
			// errors on user attempts to write into affected directories.
			//
			// Fix: if Redis was disconnected or reconnected within the
			// cooldown window, skip the purge. A single FUSE-says-gone
			// observation isn't trustworthy when the metadata authority
			// just blipped. The file may genuinely have been deleted —
			// in which case the next stat after the cooldown will catch
			// it — but the cost of being wrong here (stale handle that
			// requires a remount to recover) is much higher than the
			// cost of a few extra seconds of "this stale entry exists."
			if jfs.handler.hasActiveWriter(filename) {
				// QA-19 fix (2026-05-17): NEVER purge a file with an
				// in-flight write handle. The NFS file handle is the
				// inode (see ToHandle/FromHandle); deleting the cache
				// entry removes inodeCache[inode] and the writer's next
				// WRITE RPC gets NFS3ERR_STALE. A concurrent GETATTR
				// (NFSv3 kernel clients periodically refresh attrs under
				// sustained I/O) racing a writeback-mode juicefs sync
				// can momentarily observe ENOENT from FUSE even though
				// the writer's data is in the writeback buffer.
				//
				// Uses the activeWriters refcount (not the sticky
				// writeSizes map) so the gate self-clears the moment
				// the last writer closes. A file that's stale AND has
				// no active writer still gets purged on the next stat.
				// Reproducer: scripts/qa-suite/04-fio.sh seqwrite-1m
				// firing at ~52s into a 512 MiB write.
			} else if jfs.handler.fdPool != nil && jfs.handler.fdPool.HasOpenRefs(jfs.fullPath(filename)) {
				// QA-35 (2026-05-26): NEVER purge a file with an active
				// reader OR writer. fdPool.HasOpenRefs returns true when
				// at least one outstanding Get OR GetWrite holds a FD on
				// this path (read AND write slots checked post-QA-37).
				// Either is proof the file is not a phantom — if anything
				// holds a FD, the kernel keeps the inode alive and ENOENT
				// from Lstat must be transient. The typical phantom-purge
				// trigger is ENOENT from Lstat during a transient juicefs
				// staging-block upload spike; an active holder rules that
				// out as the explanation.
				//
				// Trade-off: this is a heuristic, NOT a proof of dentry
				// existence. The kernel keeps the inode alive while a
				// FD is open, so a remote `juicefs rmr` from another
				// client could leave us with an open FD whose dentry is
				// genuinely gone. In that narrow case we serve stale
				// FileInfo until the reader closes — acceptable for the
				// playback workload (operator controls remote deletes
				// during active sessions). Worst-case staleness clears
				// when the FD is released and the next Stat reverifies.
				//
				// This eliminates one FUSE Lstat per GETATTR/LOOKUP on
				// any file Resolve, Finder Quick Look, or any other
				// long-open reader has open — the dominant per-metadata-
				// RPC tax in the playback workload.
			} else if rc := jfs.handler.redisClient.Load(); rc != nil && rc.RecentlyDegraded(60*time.Second) {
				// Treat the cache entry as authoritative. Skip purge.
			} else if strings.HasPrefix(path.Base(filename), "._") {
				// QA-31 (2026-06-28): NEVER phantom-purge a ._AppleDouble sidecar.
				// macOS writes one ._X per copied item; the handler deliberately
				// scan-filters ._* out of the backend SCAN (see ReadDir), so the
				// SCAN never re-confirms them and "FUSE says ENOENT" is an
				// unreliable phantom signal for them — a ._ file already drained
				// out of the spool index (HasPending==false) but still transiently
				// ENOENT on FUSE would be wrongly purged, destroying the inode→
				// handle mapping → FromHandle STALE → intermittent copy stall
				// (the residual 1-per-960 that slipped the spool guard). ._
				// sidecars are transient metadata; a lingering cache entry is
				// harmless (overwritten on the next create), so keeping it is
				// strictly safer than risking a STALE on Finder's open handle.
			} else if spool := jfs.handler.spool.Load(); spool != nil && spool.HasPending(filename) {
				// QA-30 Layer D, on-Stat purge twin (2026-06-28): NEVER purge a
				// spool-pending file. A file Finder wrote-and-closed sits in the
				// spool — no active writer, no open FD, not yet on FUSE — until
				// the drainer flushes it. FUSE ENOENT here is EXPECTED, not proof
				// of a phantom. Purging destroys the cache entry → the inode the
				// NFS handle resolves to vanishes → Finder's next FromHandle on
				// that path returns STALE → the copy freezes. filterSpoolPending
				// guards the reconcile prune identically (metadata/keyspace.go);
				// this is the missing guard for the Stat/Open phantom-purge.
				// Repro: a Finder copy of Desktop+Downloads writes one ._AppleDouble
				// per item; each is purged in the closed-but-not-drained window →
				// a burst of "FromHandle STALE" → hard stall at ~2.4 GB. HasPending
				// self-clears on drain success and on terminal drain failure (via
				// EvictIndex), so this can never perma-pin a stale entry.
				jmlog.Debug("stat ENOENT but spool-pending — NOT purging (in-flight, queued to drain)", "path", filename)
			} else {
				// RC drain-latency fix (2026-06-28) — QA-35 / feedback_perf_hot_path:
				// the phantom-purge confirmation must NEVER FUSE-syscall on the
				// SYNCHRONOUS Stat path. Idle, the os.Lstat below is µs; but under
				// a spool drain pushes durable copy checkpoints through the SAME
				// JuiceFS/FUSE daemon, so the Lstat
				// blocks up to its full timeout. Each slow Stat holds an
				// nfsLstatGate slot AND an rpcSem slot; the reader admits each
				// non-WRITE RPC on rpcSem BEFORE spawning the handler goroutine
				// (internal/nfs/conn.go), so once those drain it parks and EVERY
				// subsequent READDIR on Finder's single TCP connection waits →
				// the directory-open spinner. Finder fires Stat-backed ops
				// constantly during a copy (onRead/onRemove/onRename/onCreate/
				// readlink/link/mknod/symlink all call fs.Stat), so the gate fills
				// exactly when the drain is hot. A regression of the "never FUSE-
				// syscall per RPC on the hot path" rule.
				//
				// Fix: SERVE THE CACHED FileInfo IMMEDIATELY (like every guard
				// branch above) and confirm the phantom in the BACKGROUND. Only a
				// CONFIRMED isNotExist purges; a timeout/race keeps the entry —
				// exactly the prior conservative behavior, just off the hot path.
				// No new STALE risk: we serve MORE conservatively (keep the
				// entry), never less. Deduped per-path (singleflight) so a Stat
				// storm on one path launches ONE goroutine + ONE FUSE Lstat, not N.
				jfs.handler.asyncConfirmPhantomPurge(filename, jfs.fullPath(filename))
			}
		}

		// If we have a tracked write size that's larger, use it
		if hasWriteSize && writeSize > e.Size {
			clone := *e
			clone.Size = writeSize
			clone.ResetGetAttrCache()
			// #100: KEEP the cached mtime — do NOT re-sample time.Now() here.
			// This branch fires in the post-drain "sticky writeSizes" window
			// (spool LookupActive has cleared but writeSizes still holds the
			// high-water size and writeSize > e.Size). A fresh time.Now() makes
			// the mtime JITTER on every stat, so every WRITE/COMMIT post-op
			// mtime (wcc) and every GETATTR returns a different value. The macOS
			// NFS client treats the file as "impossibly changing" and stalls its
			// next getxattr on the vnode lock until timeout (~73 s) — the real
			// cause of the "copies take forever" fsetxattr hang writing a ._
			// AppleDouble sidecar. e.Mtime is stable across stats and the real
			// mtime lands when the cache entry refreshes post-drain. Same bug
			// class as the #99 root-dir mtime jitter (a stable-but-slightly-
			// stale mtime is fine for wcc; a jittering one is catastrophic).
			return jfs.entryToFileInfo(&clone), nil
		}
		return jfs.entryToFileInfo(e), nil
	}

	// [JM6 tier-1.7] Offline fail-fast for un-pinned, un-cached files.
	//
	// We reach this fallback when SQLite metadata didn't know about
	// the path. Falling through to os.Stat() on FUSE would query
	// JuiceFS → Redis (the metadata authority). When we're offline,
	// that query hangs or times out — bad UX for Finder.
	//
	// Refuse fast with pin.ErrOfflineNotAvailable. The NFS protocol
	// layer (nfs_ongetattr.go / nfs_onlookup.go) maps the sentinel
	// to NFSStatusNXIO. Why NXIO rather than NOENT: NOENT causes
	// macOS to invalidate its file handle cache for the path; after
	// recovery the file would NOT reappear without a remount. NXIO
	// surfaces as "I/O error" to apps but preserves the handle cache,
	// so post-recovery the next Stat succeeds and Finder shows the
	// file again automatically.
	//
	// Pinned-and-ready files bypass this: by construction the pin
	// store knows about them, the FUSE path serves from local cache
	// without touching Redis.
	if pin.IsOffline() && jfs.handler.pinStore.Load() != nil {
		canonical := jfs.handler.canonicalize(filename)
		if !jfs.handler.isPinnedReady(canonical) {
			jmlog.Debug("offline: refusing stat of un-pinned, un-cached file",
				"path", filename, "canonical", canonical)
			return nil, pin.ErrOfflineNotAvailable
		}
	}

	// Fallback: stat the file on FUSE directly and cache it. BOUNDED: a
	// wedged JuiceFS makes os.Stat hang forever; on the per-RPC hot path
	// that exhausts the NFS server's concurrency budget and stales the whole
	// mount. statWithTimeout returns ok=false on a wedge so we fail this RPC
	// fast (errFUSETimeout → JUKEBOX) and free the slot instead of blocking.
	fusePath := jfs.fullPath(filename)
	info, err, ok := statWithTimeout(metrics.FUSESrcForeground, fusePath, fuseStatTimeout)
	if !ok {
		return nil, errFUSETimeout
	}
	if err != nil {
		return nil, os.ErrNotExist
	}

	// Cache this stat result into SQLite so we don't fall back again
	var inode uint64
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		inode = st.Ino
	} else {
		inode = jfs.handler.nextSyntheticInode()
	}
	entry := metadata.MakeEntry(filename, info.IsDir(), info.Size(), info.ModTime(), inode)
	go jfs.handler.store.Insert(entry)

	// Override with tracked write size if available
	if hasWriteSize && writeSize > info.Size() {
		return &writeSizeInfo{FileInfo: info, size: writeSize}, nil
	}
	return info, nil
}

// writeSizeInfo wraps FileInfo to override the size with tracked write data.
type writeSizeInfo struct {
	os.FileInfo
	size int64
}

func (w *writeSizeInfo) Size() int64 { return w.size }

// Lstat is the fast-path for NFS GETATTR. It is called from
// internal/nfs/nfs_ongetattr.go which then uses Entry's atomic GETATTR cache
// to skip XDR encoding entirely.
//
// QA-35 (2026-05-26): GETATTR runs at high frequency (every kernel attr
// cache refresh — typically every 3 s per open file, more under sustained
// I/O). Routing it through Stat's phantom-purge gate burned one FUSE
// Lstat round-trip per GETATTR for every cache-miss file (the open-FD and
// hasActiveWriter gates only cover held files). On 100 files that is 100
// FUSE syscalls per attr-cache cycle.
//
// Bypass rationale: GETATTR operates on an NFS handle the client already
// owns, i.e. the cache must have known about this inode at handle-issue
// time. If the metadata cache still has the entry, that is sufficient
// truth for GETATTR — no need to revalidate against FUSE on every refresh.
// On cache miss we fall through to Stat, which does the full phantom-purge
// dance (the FUSE fallback path) — this is rare for GETATTR because the
// existence-of-handle implies existence-of-entry in the common case.
func (jfs *juiceFS) Lstat(filename string) (os.FileInfo, error) {
	if err := jfs.handler.namespace.servingError(filename); err != nil {
		return nil, err
	}
	if filename == "" || filename == "." || filename == "/" {
		return jfs.rootFileInfo(), nil
	}
	filename = strings.TrimPrefix(filename, "/")
	if spool := jfs.handler.spool.Load(); spool != nil {
		if view := spool.conflictRead(filename); view != nil {
			if err := view.check(); err != nil {
				return nil, err
			}
			return view.info(), nil
		}
	}

	// Slice D: spool shadow. Same QA-35-disciplined pre-check Stat does
	// — empty-spool lookup is ~8 ns and gates the lookup before the
	// macOS-metadata filter so an in-flight file with one of those
	// base names is still served from spool. (Unlikely but valid.)
	if spool := jfs.handler.spool.Load(); spool != nil {
		if e, ok := spool.LookupActive(filename); ok {
			return spoolFileInfoForEntry(path.Base(filename), e), nil
		}
	}

	// Lstat mirrors Stat: NO name-based fast-reject of macOS metadata names.
	// Rejecting them by name broke copying a volume/card root (the .fseventsd
	// -36, 2026-06-14) — see the Stat comment above. Round-trip them; an
	// absent name still returns ErrNotExist from the cache lookup below.

	// Tracked write size for in-flight writes (sticky map — see Stat).
	jfs.handler.writeSizeMu.Lock()
	writeSize, hasWriteSize := jfs.handler.writeSizes[filename]
	jfs.handler.writeSizeMu.Unlock()

	if e := jfs.handler.store.LookupByPath(filename); e != nil {
		if hasWriteSize && writeSize > e.Size {
			clone := *e
			clone.Size = writeSize
			clone.ResetGetAttrCache()
			// #100: KEEP the cached mtime — do NOT re-sample time.Now() here.
			// This branch fires in the post-drain "sticky writeSizes" window
			// (spool LookupActive has cleared but writeSizes still holds the
			// high-water size and writeSize > e.Size). A fresh time.Now() makes
			// the mtime JITTER on every stat, so every WRITE/COMMIT post-op
			// mtime (wcc) and every GETATTR returns a different value. The macOS
			// NFS client treats the file as "impossibly changing" and stalls its
			// next getxattr on the vnode lock until timeout (~73 s) — the real
			// cause of the "copies take forever" fsetxattr hang writing a ._
			// AppleDouble sidecar. e.Mtime is stable across stats and the real
			// mtime lands when the cache entry refreshes post-drain. Same bug
			// class as the #99 root-dir mtime jitter (a stable-but-slightly-
			// stale mtime is fine for wcc; a jittering one is catastrophic).
			return jfs.entryToFileInfo(&clone), nil
		}
		return jfs.entryToFileInfo(e), nil
	}
	// Match Stat's offline cache-miss policy before the no-follow symlink
	// probe. A cold Lstat must not enter FUSE merely to discover that the
	// subsequent Stat would refuse this unpinned path. Spool and mirror hits
	// above remain available, as do pinned-ready and no-pin-store fallbacks.
	if pin.IsOffline() && jfs.handler.pinStore.Load() != nil {
		if !jfs.handler.isPinnedReady(jfs.handler.canonicalize(filename)) {
			return nil, pin.ErrOfflineNotAvailable
		}
	}

	// Cache miss. Stat (the fall-through below) uses os.Stat, which FOLLOWS a
	// symlink — so an evicted (or backend-resident-but-uncached) symlink would
	// mis-report as its target's type, or ENOENT if the target is dangling, and
	// the client would never issue READLINK. Lstat must NOT follow. Do a single
	// no-follow probe (lstatWithTimeout → os.Lstat, the same no-follow helper
	// the phantom-purge gate uses) and, ONLY if the path is a symlink, mint a
	// ModeSymlink entry. Regular files and dirs — and a wedge/ENOENT — fall
	// through to Stat unchanged, so the regular-file hot path is untouched.
	fusePath := jfs.fullPath(filename)
	if fi, ok := lstatWithTimeout(metrics.FUSESrcForeground, fusePath, fuseStatTimeout); ok && fi != nil && fi.Mode()&os.ModeSymlink != 0 {
		var inode uint64
		if st, ok := fi.Sys().(*syscall.Stat_t); ok && st.Ino != 0 {
			inode = st.Ino
		} else {
			inode = jfs.handler.nextSyntheticInode()
		}
		entry := metadata.MakeEntry(filename, false, fi.Size(), fi.ModTime(), inode)
		entry.Mode = (entry.Mode &^ os.ModeType) | os.ModeSymlink
		// Seed the cache so the next GETATTR is a cache hit (and READLINK
		// resolves the link the client now knows is a symlink).
		jfs.handler.store.InsertToCache(entry)
		go jfs.handler.store.Insert(entry)
		return jfs.entryToFileInfo(entry), nil
	}

	return jfs.Stat(filename)
}

func (jfs *juiceFS) ReadDir(dirname string) ([]os.FileInfo, error) {
	if err := jfs.handler.namespace.servingError(dirname); err != nil {
		return nil, err
	}
	if dirname == "" || dirname == "." || dirname == "/" {
		dirname = "."
	} else {
		dirname = strings.TrimPrefix(dirname, "/")
	}

	// Item 1 (serving-layer-decision.md): under JM_READDIR_PAGINATED (or the
	// SQLite serve substrate), build the whole listing by streaming idx_parent
	// in bounded pages with pooled scratch instead of one giant whole-dir map
	// copy — kills the big-dir veto (the 10,774-child DCIM dir was 12.7ms /
	// 6.26MB / 215k allocs as a single scan+copy). Default off = RAM whole-dir
	// copy, unchanged. The returned SET is identical either way; the protocol
	// layer still hashes+caches+paginates the full listing by index.
	children, err := jfs.handler.store.ListChildrenForReadDir(dirname)
	if err != nil {
		return nil, err
	}
	var conflictChildren []*conflictReadView
	if spool := jfs.handler.spool.Load(); spool != nil {
		conflictChildren = spool.conflictChildren(dirname)
	}

	if len(children) > 0 || len(conflictChildren) > 0 {
		// Reuse the one ordering pass for both returned infos and preview
		// selection. The RAM mirror's map iteration is otherwise unstable.
		sort.Slice(children, func(i, j int) bool {
			return children[i].Name < children[j].Name
		})
		infos := make([]os.FileInfo, 0, len(children)+len(conflictChildren))
		for _, e := range children {
			base := e.Name
			if spool := jfs.handler.spool.Load(); spool != nil && spool.conflictRead(e.Path) != nil {
				continue // the accepted local size/inode replaces the backend row
			}
			// Do NOT hide `._*` AppleDouble sidecars: they are created,
			// stored, and stat/open/read-accessible (QA-13 removed them from
			// the Stat/Open filter at handler.go ~1167 so they round-trip).
			// Hiding them ONLY from readdir made them exist-but-unlisted —
			// confirmed 2026-06-14 by a real Finder copy of EOS_DIGITAL/DCIM
			// where `._<dir>` sidecars stat'd fine over NFS but never appeared
			// in `ls`. That breaks rsync / Carbon Copy Cloner / Finder-compare
			// (they enumerate via readdir, see the sidecar "missing", and
			// re-copy it every run) and loses the apparent resource-fork /
			// Finder-info metadata on a round-trip. List them like any entry.
			//
			// Still hide the macOS VOLUME-level system dirs: these are managed
			// by macOS at the mount root, not user data, and surfacing them can
			// make Finder try to manage them on our backend.
			if base == ".Spotlight-V100" ||
				base == ".Trashes" ||
				base == ".fseventsd" ||
				base == ".TemporaryItems" {
				continue
			}
			infos = append(infos, jfs.entryToFileInfo(e))
		}
		for _, view := range conflictChildren {
			if err := view.check(); err != nil {
				return nil, err
			}
			infos = append(infos, view.info())
		}
		// The NFS directory cookie is positional; keep the merged listing
		// deterministic after replacing a backend entry with a local view.
		sort.Slice(infos, func(i, j int) bool { return infos[i].Name() < infos[j].Name() })
		// Proactively prefetch subdirectories' children in background so
		// Finder's subsequent navigation is instant. Two guards make this
		// safe under a bulk recursive walk (crash 2026-06-14, threads→5200+
		// toward the 8192 cap):
		//
		//   1. Skip when offline. prefetchChildren's os.ReadDir hits
		//      JuiceFS→Redis, which is unreachable offline; the read blocks
		//      until JuiceFS gives up. The synchronous offline path below
		//      (line ~1416) returns empty precisely to avoid that hang —
		//      spawning it here in the background would reintroduce it, one
		//      unbounded goroutine per directory.
		//   2. Bound the fan-out. A bare `go prefetchChildren` per READDIR
		//      turned a deep walk into thousands of concurrent ungated FUSE
		//      reads, each pinning an OS thread. Acquire a prefetchSem slot
		//      non-blocking and skip if the pool is busy — prefetch is a
		//      best-effort nav-latency optimization, not correctness, so
		//      shedding it under load is the right trade.
		if !pin.IsOffline() && allowSpeculativeDirectoryWarm(netprofile.Default().Class()) {
			select {
			case jfs.handler.prefetchSem <- struct{}{}:
				go func() {
					defer func() { <-jfs.handler.prefetchSem }()
					jfs.handler.prefetchChildren(dirname)
				}()
			default:
				// prefetch pool busy — shed this one
			}
			// #1 hydration pack: hydrate this dir's farm thumbnails into
			// the local cache in the background. Non-blocking enqueue
			// (TTL-deduped inside); nil when the warmer isn't wired.
			if warmer := jfs.handler.thumbWarmer.Load(); warmer != nil {
				warmer.WarmDirAsync(dirname)
			}
			// Nav crux: front-run Finder's per-entry `._` AppleDouble reads
			// by warming this dir's sidecar bodies into the RAM cache.
			jfs.handler.sidecarWarmDirAsync(dirname)
		}
		jfs.handler.scheduleNativePreviews(path.Clean(dirname), children)
		// WAVE 0: one inc per warm readdir served from the RAM mirror fast path
		// (the len(children)>0 branch). QA-35-safe single atomic increment.
		metrics.Default().IncReaddirMirrorHit()
		return infos, nil
	}
	jfs.handler.readahead.clearPreviewCursors(path.Clean(dirname))

	// [JM6 tier-1.7] Offline fail-fast: an empty SQLite ListChildren
	// result while offline likely means we just haven't synced this
	// directory yet — Redis is unreachable. Falling through to
	// os.ReadDir(fusePath) would query JuiceFS → Redis and hang or
	// time out. Return empty fast. The next ReadDir after the network
	// returns will populate via sync. Pinned content above this
	// directory is unaffected (those entries are in SQLite already).
	if pin.IsOffline() {
		jmlog.Debug("offline: empty readdir (no synced entries available)",
			"dirname", dirname)
		return []os.FileInfo{}, nil
	}

	// The mirror has ZERO rows for this directory and we're ONLINE — a
	// genuinely unmirrored (or genuinely empty) directory.

	// Batch-3 adversarial review #2/#4: scan-filtered namespaces (.trash/,
	// .juicemount/ — metadata.ScanFilteredPath) are deliberately GC'd from
	// the mirror at open and deliberately never re-mirrored by the push
	// paths (#78: the SCAN can never confirm them, so mirrored rows cycled
	// in the prune ladder forever). Zero mirror rows here is therefore the
	// DESIGNED state for them, not a cache miss: return empty immediately —
	// no FUSE readdir (async OR foreground), no mirror insert. Without this,
	// any walker entering .trash/.juicemount (ls -a, rsync, Spotlight,
	// OpenLoupe) re-mirrored the whole filtered tree every session, undoing
	// the GC. Cheap in-memory string check — QA-35 hot-path safe.
	if metadata.ScanFilteredPath(dirname) {
		return []os.FileInfo{}, nil
	}

	fusePath := jfs.fullPath(dirname)

	// U7 (V2.3): NEVER block the READDIR RPC on FUSE. The old foreground
	// fallback below bounds the FUSE readdir with fuseStatTimeout, but over a
	// slow link (or ANY FUSE slowness — root readdir was measured hanging
	// >25s over a cellular relay) Finder sits on "loading" for up to a minute
	// on such directories. Instead: return the mirror's answer (empty)
	// IMMEDIATELY and kick an ASYNC bounded FUSE readdir that upserts the
	// discovered children into the mirror, so the NEXT readdir (Finder
	// retries/refreshes on its own) serves them from the mirror hot path.
	// QA-35 hot-path discipline: this RPC path does only in-memory checks —
	// the FUSE readdir happens strictly on the background goroutine.
	if asyncDirRefreshEnabled() {
		// S3 (WAVE 1, RC-6): on a FAST link ONLY, do a BOUNDED synchronous
		// populate before returning, so Finder's FIRST readdir carries the
		// children instead of an empty listing that pops in later (or, worst
		// case, stays empty until acdirmax on an out-of-band deep link).
		//
		// HOT-PATH SAFETY (the invariants this fix must preserve):
		//   - This code is UNREACHABLE for a warm dir: the len(children)>0 RAM
		//     fast path at the top of ReadDir already returned. We are here ONLY
		//     because the mirror has ZERO rows for this directory.
		//   - It is TIME-BOUNDED by syncColdPopulateTimeout (~50ms) — it can
		//     never keep the foreground waiting beyond that complete-operation budget.
		//   - It NEVER changes offline/G0 behavior: the offline early-return and
		//     pin.FUSEIdentityOK gates above/inside still apply; on any timeout,
		//     error or zero children, return the current empty mirror answer.
		//     A job still in progress continues under the background budget.
		// Kill-switch JM_SYNC_COLD_POPULATE=0; class-gated to ClassFast.
		if syncColdPopulateEnabled() && netprofile.Default().Class() == netprofile.ClassFast {
			if infos, ok := jfs.syncColdPopulateBounded(dirname, fusePath); ok {
				return infos, nil // one readdir, populated
			}
			// The fast path already dispatched or coalesced a job. Even if it
			// completed just as our wait expired, do not dispatch it again.
			metrics.Default().IncReaddirEmptyRefill()
			return []os.FileInfo{}, nil
		}
		// WAVE 0: a zero-row (unmirrored) dir returned EMPTY + kicked an async
		// refresh — the empty-then-pop-in case. One atomic inc.
		metrics.Default().IncReaddirEmptyRefill()
		jfs.maybeAsyncRefreshDir(dirname, fusePath)
		return []os.FileInfo{}, nil
	}

	// JM_ASYNC_DIR_REFRESH=0 kill switch: the prior FOREGROUND fallback —
	// read directly from FUSE and cache into SQLite. BOUNDED so a wedged
	// JuiceFS can't hang the READDIR RPC and exhaust the server's
	// concurrency budget (see statWithTimeout rationale).
	// Foreground cold READDIR — a genuine cache-miss metadata RPC on Finder's
	// hot path, so it uses the shared nfsLstatGate (NOT prefetchGate).
	dirEntries, err, ok := readDirWithTimeout(metrics.FUSESrcForeground, fusePath, fuseStatTimeout, nfsLstatGate)
	if !ok {
		return nil, errFUSETimeout
	}
	if err != nil {
		return nil, err
	}
	// Foreground RPC path → per-child stats draw from the shared hot-path
	// budget (nfsLstatGate), same as the readDirWithTimeout above.
	infos, toInsert := jfs.coldDirListing(metrics.FUSESrcForeground, dirname, dirEntries, nfsLstatGate)

	// Bulk-insert into SQLite synchronously so subsequent Stat() calls
	// from Finder (which follow immediately after READDIR) hit the cache
	if len(toInsert) > 0 {
		jfs.handler.store.BulkInsert(toInsert, 500)
	}

	return infos, nil
}

// allowSpeculativeDirectoryWarm is the single policy gate for background
// FUSE work launched by a mirror-served READDIR. Medium/fast links may spend
// bandwidth to front-run a likely next click. Slow/metered links must preserve
// the defining cellular invariant: listing known metadata is a local-only
// operation. Foreground reads remain available in every class.
func allowSpeculativeDirectoryWarm(class netprofile.LinkClass) bool {
	return class >= netprofile.ClassMedium
}

// coldDirListing converts the result of a bounded FUSE ReadDir on dirname
// into (a) the FileInfos a foreground READDIR returns to the client and
// (b) the mirror entries to insert. Extracted from the foreground cold-
// readdir fallback so the U7 async refresh builds entries via the SAME code
// path — one entry-construction path, two callers.
//
// gate bounds the per-child de.Info() lstats (batch-3 adversarial review #0
// — see infoWithTimeout): the foreground fallback passes nfsLstatGate, the
// U7 async worker passes prefetchGate. On a timeout the listing pass is
// ABANDONED so the async worker always returns promptly and releases its
// dirRefreshSem slot + singleflight key; the partial toInsert is safe (inserts
// are insert-only and the next readdir re-fires the refresh).
//
// S4 (WAVE 1, RC-7): the per-child lstats run through a K-bounded worker pool
// (coldListFanout, class-scaled) instead of serially, collapsing N×RTT →
// (N/K)×RTT on a high-latency link. Each worker still calls infoWithTimeout,
// which acquires the SAME `gate` (nfsLstatGate foreground / prefetchGate
// background) and honors the same per-child timeout — so the global lstat
// budget and anti-flood discipline are preserved; the fan-out only lets up to K
// of those already-gated lstats be in flight at once. This is the background /
// non-default-foreground fallback path, NOT the RAM serve.
//
// src travels WITH gate (they always agree: foreground↔nfsLstatGate,
// background↔prefetchGate) so the per-child lstats are attributed to whoever
// actually issued the listing — this function is the one place a single body
// serves both a blocked client RPC and a background mirror warm.
func (jfs *juiceFS) coldDirListing(src metrics.FUSESource, dirname string, dirEntries []os.DirEntry, gate chan struct{}) ([]os.FileInfo, []*metadata.Entry) {
	type result struct {
		info    os.FileInfo
		entry   *metadata.Entry // nil when scan-filtered (in listing, not mirrored)
		present bool            // stat succeeded and entry belongs in the listing
	}
	results := make([]result, len(dirEntries))

	// wedged is set the first time any worker's stat times out (ok==false).
	// Once set, still-pending workers short-circuit so we stop hammering a mount
	// that just proved unresponsive — preserving the serial version's
	// abandon-on-wedge intent within a bounded pool.
	var wedged atomic.Bool
	var wedgedName atomic.Value // string: the child whose stat first timed out

	fanout := coldListFanout(netprofile.Default().Class())
	sem := make(chan struct{}, fanout)
	var wg sync.WaitGroup

	for i, de := range dirEntries {
		if wedged.Load() {
			break // FUSE already proved unresponsive — don't dispatch more lstats
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, de os.DirEntry) {
			defer wg.Done()
			defer func() { <-sem }()
			if wedged.Load() {
				return
			}
			info, err, ok := infoWithTimeout(src, de, fuseStatTimeout, gate)
			if !ok {
				// FUSE wedged/slow mid-listing. Flag it so later workers bail.
				if wedged.CompareAndSwap(false, true) {
					wedgedName.Store(de.Name())
				}
				return
			}
			if err != nil {
				// Log rather than silently dropping. A stat failure here omits the
				// entry from the returned listing, which presents as a folder that
				// "didn't fully load"; macOS caches that partial result until the
				// attr-cache expires or a refresh.
				jmlog.Debug("readdir: stat failed for child — omitting from cold listing",
					"dir", dirname, "name", de.Name(), "error", err.Error())
				return
			}

			// Extract real inode from FUSE stat
			var inode uint64
			if st, ok := info.Sys().(*syscall.Stat_t); ok {
				inode = st.Ino
			} else {
				inode = jfs.handler.nextSyntheticInode()
			}
			childPath := path.Join(dirname, info.Name())
			if dirname == "." {
				childPath = info.Name()
			}
			r := result{info: info, present: true}
			// Batch-3 adversarial review #2/#4: never MIRROR a scan-filtered
			// namespace from a FUSE-sourced listing (#78 invariant). The entry
			// stays in the RETURNED listing (foreground behavior unchanged); it
			// just never lands in the mirror (entry left nil).
			if !metadata.ScanFilteredPath(childPath) {
				r.entry = metadata.MakeEntry(childPath, info.IsDir(), info.Size(), info.ModTime(), inode)
			}
			results[i] = r
		}(i, de)
	}
	wg.Wait()

	// Collect in the ORIGINAL dirEntries order (deterministic, matches the prior
	// serial version). Skipped/failed children leave a zero-value (present=false)
	// slot that we drop here.
	infos := make([]os.FileInfo, 0, len(dirEntries))
	toInsert := make([]*metadata.Entry, 0, len(dirEntries))
	for _, r := range results {
		if !r.present {
			continue
		}
		infos = append(infos, r.info)
		if r.entry != nil {
			toInsert = append(toInsert, r.entry)
		}
	}
	if wedged.Load() {
		name, _ := wedgedName.Load().(string)
		jmlog.Warn("readdir: per-child stat timed out — abandoning cold listing",
			"dir", dirname, "name", name, "collected", len(infos), "total", len(dirEntries))
	}
	return infos, toInsert
}

// coldListFanout returns the S4 per-directory lstat fan-out width (WAVE 1,
// RC-7), class-scaled: a fast link parallelizes wide to collapse per-child RTT,
// a metered/slow link stays gentler. JM_COLD_LIST_FANOUT=0 is the kill-switch —
// it forces K=1 (serial), restoring the prior behavior exactly. The fan-out is
// bounded by the shared gate (nfsLstatGate / prefetchGate) inside infoWithTimeout
// regardless, so K only caps how many gated lstats race at once — it can never
// exceed the global budget or re-create the anti-flood-gated round-trip storm.
func coldListFanout(class netprofile.LinkClass) int {
	if os.Getenv("JM_COLD_LIST_FANOUT") == "0" {
		return 1 // kill-switch: serial, prior behavior
	}
	switch class {
	case netprofile.ClassMetered, netprofile.ClassSlow:
		return 8
	default: // ClassMedium, ClassFast
		return 24
	}
}

// syncColdPopulateBounded waits briefly for the entire cold refresh: directory
// read, child stats, and mirror publication. A timeout leaves the same bounded
// background job running; it does not duplicate work or abandon SQL publication.
func (jfs *juiceFS) syncColdPopulateBounded(dirname, fusePath string) ([]os.FileInfo, bool) {
	timer := time.NewTimer(syncColdPopulateTimeout)
	defer timer.Stop()
	result := make(chan []os.FileInfo, 1)
	if !jfs.handler.startDirRefresh(dirname, func() {
		result <- jfs.refreshUnmirroredDir(dirname, fusePath)
	}) {
		return nil, false
	}
	select {
	case infos := <-result:
		return infos, len(infos) > 0
	case <-timer.C:
		return nil, false
	}
}

// maybeAsyncRefreshDir shares the fast-link job's per-directory key and worker
// budget. A timed-out foreground wait therefore cannot start a second refresh.
func (jfs *juiceFS) maybeAsyncRefreshDir(dirname, fusePath string) {
	jfs.handler.startDirRefresh(dirname, func() { jfs.refreshUnmirroredDir(dirname, fusePath) })
}

// startDirRefresh admits at most the existing refresh budget, without waiting
// or spawning a goroutine when saturated. Admission and stop share one mutex.
func (h *JuiceMountHandler) startDirRefresh(dirname string, work func()) bool {
	h.dirRefreshMu.Lock()
	defer h.dirRefreshMu.Unlock()
	if h.dirRefreshStopping {
		return false
	}
	if _, inFlight := h.dirRefreshInFlight[dirname]; inFlight {
		return false
	}
	select {
	case h.dirRefreshSem <- struct{}{}:
	default:
		metrics.Default().IncReaddirColdShed()
		return false
	}
	h.dirRefreshInFlight[dirname] = struct{}{}
	h.dirRefreshActive++
	go func() {
		defer func() {
			h.dirRefreshMu.Lock()
			delete(h.dirRefreshInFlight, dirname)
			<-h.dirRefreshSem
			h.dirRefreshActive--
			if h.dirRefreshStopping && h.dirRefreshActive == 0 {
				close(h.dirRefreshDone)
			}
			h.dirRefreshMu.Unlock()
		}()
		work()
	}()
	return true
}

// beginDirRefreshStop permanently closes admission and returns one completion
// channel for all stop retries. No timeout releases a live job's resources.
func (h *JuiceMountHandler) beginDirRefreshStop() <-chan struct{} {
	h.dirRefreshMu.Lock()
	defer h.dirRefreshMu.Unlock()
	if !h.dirRefreshStopping {
		h.dirRefreshStopping = true
		h.dirRefreshDone = make(chan struct{})
		if h.dirRefreshActive == 0 {
			close(h.dirRefreshDone)
		}
	}
	return h.dirRefreshDone
}

// refreshUnmirroredDir is the U7 background worker: a bounded FUSE readdir
// of a directory the mirror had zero rows for, INSERTING the discovered
// children into the mirror (SQLite + in-memory cache via BulkInsert) so the
// next readdir serves them from the mirror. Runs strictly OFF the RPC path.
//
// Correctness bounds:
//   - Re-checks offline + the G0 FUSE-identity gate here (both can flip
//     between dispatch and run; and against a plain-dir mountpoint the
//     readdir would "succeed" on the boot SSD and mirror garbage).
//   - INSERT-only: children already present in the mirror are skipped, and
//     the skip is enforced ATOMICALLY by BulkInsertAbsent (INSERT OR IGNORE +
//     in-lock cache check — batch-3 review #1), so an async result can never
//     overwrite a fresher row — pruning stays the reconcile's job.
//   - A genuinely empty directory inserts nothing and records nothing; the
//     next readdir re-fires the singleflight, which is acceptable.
//
// The FUSE readdir draws from prefetchGate (the BACKGROUND readdir budget),
// never nfsLstatGate — background work must not consume the foreground
// hot-path FUSE budget (QA-35 / RC drain-latency fix). Timeout is
// fuseStatTimeout, the same WAN-aware budget the foreground fallback uses.
func (jfs *juiceFS) refreshUnmirroredDir(dirname, fusePath string) []os.FileInfo {
	h := jfs.handler

	// Offline engaged since dispatch: the offline readdir path has its own
	// empty-fast semantics — generate no backend traffic.
	if pin.IsOffline() {
		return nil
	}
	// V2.3 G0: a plain-dir mountpoint (kext not loaded / mount absent) makes
	// the readdir answer meaningless — do not scan it, do not insert from it.
	if !pin.FUSEIdentityOK() {
		return nil
	}
	// Batch-3 adversarial review #2 (defense in depth): never scan or mirror
	// a scan-filtered namespace even if a future caller dispatches one —
	// ReadDir short-circuits these before dispatch today.
	if metadata.ScanFilteredPath(dirname) {
		return nil
	}

	dirEntries, err, ok := readDirWithTimeout(metrics.FUSESrcDirRefresh, fusePath, fuseStatTimeout, prefetchGate)
	if !ok {
		return nil // FUSE wedged/slow — give up; next readdir re-fires
	}
	if err != nil {
		return nil
	}

	// Background worker → per-child stats draw from prefetchGate (never the
	// foreground budget); a wedged stat BREAKS the listing pass so this
	// worker always returns and its deferred sem/key release runs (batch-3
	// adversarial review #0).
	infos, discovered := jfs.coldDirListing(metrics.FUSESrcDirRefresh, dirname, dirEntries, prefetchGate)

	// INSERT-only, enforced ATOMICALLY by the store (batch-3 adversarial
	// review #1): the LookupByPath pre-filter below is only a cheap
	// batch-size reducer. A row that lands BETWEEN this filter and the batch
	// commit (a push event completing a remote write, a CREATE, the
	// reconcile) is fresher than our FUSE snapshot and must win —
	// BulkInsertAbsent re-checks presence inside the store's own critical
	// sections (INSERT OR IGNORE + skip-if-present cache mutation), so the
	// stale snapshot can never clobber it (stale GETATTR size / phantom
	// re-insert class).
	toInsert := make([]*metadata.Entry, 0, len(discovered))
	for _, e := range discovered {
		if h.store.LookupByPath(e.Path) == nil {
			toInsert = append(toInsert, e)
		}
	}
	if len(toInsert) > 0 {
		if err := h.store.BulkInsertAbsent(toInsert, 500); err != nil {
			return nil
		}
		jmlog.Debug("async dir refresh: mirrored unmirrored directory",
			"dir", dirname, "children", len(toInsert))
	}
	return infos
}

// StatCacheOnly returns the FileInfo for filename if the metadata cache knows
// it, WITHOUT any FUSE round-trip. found=false means the path is not in our
// local view. The guarded-CREATE existence checks use this instead of fs.Stat
// so a brand-new file (definitionally absent from the cache) doesn't pay an
// 800ms FUSE existence-stat each — the dominant cost that made a recursive
// deep-tree Finder copy (tens of thousands of new files → tens of thousands of
// cache-miss stats, bottlenecked through nfsLstatGate) saturate the metadata
// hot path and trip the kernel soft-mount timeout ("error 100060"), while a
// flat copy of large files (few new files) sailed through (2026-06-14, root-
// caused via a faithful Finder reproduction). It does NOT prove backend
// non-existence; use only where an optimistic "not present locally → proceed"
// is correct (a CREATE whose fs.Create is the real arbiter).
func (jfs *juiceFS) StatCacheOnly(filename string) (os.FileInfo, bool) {
	if jfs.handler.namespace.servingError(filename) != nil {
		return nil, false
	}
	filename = strings.TrimPrefix(filename, "/")
	if spool := jfs.handler.spool.Load(); spool != nil {
		if view := spool.conflictRead(filename); view != nil {
			if view.check() == nil {
				return view.info(), true
			}
			return nil, false
		}
	}
	if e := jfs.handler.store.LookupByPath(filename); e != nil {
		return jfs.entryToFileInfo(e), true
	}
	// Flicker fix: a name being actively written/downloaded lives only in the
	// spool index until it drains, and its metadata-cache entry races Insert/
	// Delete during a rename cascade (e.g. Chrome's Unconfirmed.crdownload →
	// final → "(1)" download churn). Without this fallback a LOOKUP that lands
	// in that window returns NoEnt for a file that DOES exist → the macOS NFS
	// client surfaces a transient "connection interrupted" and the file
	// flickers in and out (the emoji found⇄noent flicker was this exact path).
	// Mirror the spool short-circuit Stat/Lstat/OpenFile already do so LOOKUP
	// agrees with them. ~8 ns when the spool is empty (QA-35 benchmarked), so
	// the guarded-CREATE hot path this also serves is unaffected — and a
	// genuinely-new name (the CREATE case) is NOT in the spool, so CREATE still
	// correctly sees "absent" and proceeds.
	if spool := jfs.handler.spool.Load(); spool != nil {
		if e, ok := spool.LookupActive(filename); ok {
			return spoolFileInfoForEntry(path.Base(filename), e), true
		}
	}
	return nil, false
}

// File operations — proxy to JuiceFS FUSE
func (jfs *juiceFS) Open(filename string) (billy.File, error) {
	return jfs.OpenFile(filename, os.O_RDONLY, 0)
}

func (jfs *juiceFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	return jfs.openFileBound(filename, flag, perm, nil, time.Time{})
}

func (jfs *juiceFS) openFileBound(filename string, flag int, perm os.FileMode, cohort *continuationFS, deadline time.Time) (billy.File, error) {
	if err := jfs.handler.namespace.servingError(filename); err != nil {
		return nil, err
	}
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0 {
		filename = strings.TrimPrefix(filename, "/")
		work := func(op *namespaceOperation) (billy.File, error) {
			expected, err := jfs.handler.store.CaptureMirror(filename)
			if err != nil {
				return nil, err
			}
			if cohort != nil {
				if jfs.handler.spool.Load() != cohort.spool || cohort.entry.Inode() != cohort.inode || expected.Path != cohort.path || expected.Incarnation != cohort.incarnation || expected.Incarnation == "" {
					return nil, metadata.ErrMirrorConflict
				}
				current := jfs.handler.store.LookupByInode(cohort.inode)
				if current == nil || current.Path != cohort.path || current.Inode != cohort.inode || current.Version.Incarnation != cohort.incarnation {
					return nil, metadata.ErrMirrorConflict
				}
			}
			openFlags := flag &^ os.O_TRUNC
			if flag&os.O_TRUNC != 0 && openFlags&(os.O_RDWR|os.O_WRONLY) == 0 {
				openFlags |= os.O_WRONLY
			}
			f, err := jfs.openFileExpected(filename, openFlags, perm, cohort)
			if err != nil {
				return nil, err
			}
			if wf, ok := f.(*writeFile); ok {
				if expected.Incarnation == "" {
					op.mutated = flag&os.O_CREATE != 0
					info, statErr := wf.File.Stat()
					if statErr != nil {
						wf.release()
						return nil, statErr
					}
					e := metadata.MakeEntry(filename, info.IsDir(), info.Size(), info.ModTime(), wf.entry.Inode)
					e.Mode, e.LocalOnly = info.Mode(), true
					op.checkpoint("publish", filename)
					published, publishErr := jfs.handler.store.PublishExpected(expected, e)
					if publishErr != nil {
						wf.release()
						return nil, publishErr
					}
					expected = published.Version
				}
				if err := wf.bind(expected); err != nil {
					wf.release()
					return nil, err
				}
				if flag&os.O_TRUNC != 0 {
					if err := wf.truncateOwned(0, op); err != nil {
						wf.release()
						return nil, err
					}
				}
			} else if flag&os.O_TRUNC != 0 {
				op.mutated = true
				if err := f.Truncate(0); err != nil {
					_ = f.Close()
					return nil, err
				}
			}
			return f, nil
		}
		if cohort != nil {
			return runNamespaceOperationUntil(jfs.handler, []string{filename}, deadline, work, closeNamespaceFile)
		}
		return runNamespaceOperation(jfs.handler, []string{filename}, work, closeNamespaceFile)
	}
	return jfs.openFile(filename, flag, perm)
}

func (jfs *juiceFS) openFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	return jfs.openFileExpected(filename, flag, perm, nil)
}

func (jfs *juiceFS) openFileExpected(filename string, flag int, perm os.FileMode, cohort *continuationFS) (billy.File, error) {
	filename = strings.TrimPrefix(filename, "/")

	// Detect write intent
	isWrite := flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0
	if spool := jfs.handler.spool.Load(); spool != nil {
		if view := spool.conflictRead(filename); view != nil {
			if isWrite {
				return nil, syscall.EROFS
			}
			return view.open()
		}
		if spool.ReadFault(filename) {
			return nil, syscall.EIO // never serve an older backend image
		}
	}

	// Slice D: spool shadow for the READ path. If the requested file
	// is currently in the spool index (a writer's bytes are durable on
	// our local SSD but haven't yet been copied to FUSE by the drainer),
	// serve reads from the spool file directly. This closes the slice-C
	// "briefly invisible" gap.
	//
	// Write opens (O_CREATE / O_RDWR / O_WRONLY) bypass this and fall
	// through to the existing write-path branches below — the write
	// integration in slice C already routes O_CREATE through the spool.
	//
	// QA-35 perf-discipline: the empty-spool Lookup is 8.4 ns
	// (benchmarked in slice A). Adding it here BEFORE the metadata
	// lookup keeps the read-OpenFile hot path's per-RPC overhead at
	// a no-op when no writes are in flight.
	if spool := jfs.handler.spool.Load(); !isWrite && spool != nil {
		if sentry, ok := spool.LookupActive(filename); ok {
			return &spoolReadFile{
				name:  filename,
				entry: sentry,
				// #100: a ._ AppleDouble sidecar read must never JUKEBOX-hold an
				// in-flight sparse hole (the Quarantine getxattr-during-fsetxattr
				// self-read → ~73s hang). Detect it once here.
				isAppleDouble: strings.HasPrefix(path.Base(filename), "._"),
			}, nil
		}
	}

	// A qualified Finder image uses only resident mirror state and a saved
	// selected-volume binding. Select it before LookupByPath (which can fall
	// back to SQLite after RAM eviction) and before the offline cache probe.
	if !isWrite {
		if shot := jfs.handler.beginSidecarSnapshotRead(filename); shot != nil {
			return &cachedFile{
				snapshotProof: shot, name: filename, handler: jfs.handler,
				cachedInfo: shot.fileInfo(), inode: shot.snapshot.Inode,
				fileSize: shot.snapshot.Size,
			}, nil
		}
	}

	// Look up the entry to get inode and size for cache reads
	e := jfs.handler.store.LookupByPath(filename)

	// Offline-mode gate: refuse OPEN of un-pinned files when offline.
	// This is the strict guarantee — we don't let kernel page cache or
	// FUSE buffering accidentally serve un-pinned bytes when the user
	// is on cellular and explicitly asked us to fail fast.
	//
	// Pinned-and-ready files are allowed through. New and replacement writes
	// are allowed through the durable local spool; unsafe in-place writes are
	// rejected below rather than entering an unavailable FUSE backend.
	// Compute pinned-ness once per open. We use this both for the offline
	// gate below AND to skip the read-time offline EIO on pinned files
	// (JuiceFS LRU serves from local SSD; no backend round-trip needed).
	var isPinned bool
	var pinAdmission pin.Entry
	var canonical string
	hasPinStore := jfs.handler.pinStore.Load() != nil
	if !isWrite && hasPinStore {
		canonical = jfs.handler.canonicalize(filename)
		pinAdmission, isPinned = jfs.handler.pinStore.Load().ReadyAdmission(canonical)
	}

	if !isWrite && pin.IsOffline() && hasPinStore && !isPinned {
		// C.2 fix (QA-12, 2026-05-17): before refusing the open,
		// probe whether the SSD cache can serve the first block of
		// this file. If it can, the file is "recently cached" and
		// the user-intent of offline mode (don't trigger backend
		// traffic on un-pinned reads) is still respected — the read
		// path is cache-priority-correct downstream (cachedFile.ReadAt
		// runs the cache reader BEFORE the per-read offline gate at
		// handler.go:1052). Only refuse if the cache can't help.
		//
		// 200ms timeout: covers Redis localhost LRange (sub-ms warm,
		// ≤50ms under load) + first-open APFS dir-cache miss
		// (~10-40ms) + ReadAt of one 4 KiB block. Tight enough that
		// a hanging Redis can't stall OpenFile but loose enough that
		// a legitimately-cached file on a sleepy cold system doesn't
		// get false-refused.
		if !jfs.cacheProbeHit(e) {
			jmlog.Debug("offline: refusing open of un-pinned file",
				"in_mount", filename,
				"canonical", canonical)
			// pin.ErrOfflineNotAvailable → NFS protocol layer maps to
			// NFSStatusNXIO. See Stat fallback comment for the cache-
			// preservation rationale (NXIO doesn't invalidate kernel
			// file handles; NoEnt does).
			return nil, pin.ErrOfflineNotAvailable
		}
		jmlog.Debug("offline: allowing open of cached un-pinned file (probe hit)",
			"in_mount", filename,
			"inode", func() uint64 {
				if e != nil {
					return e.Inode
				}
				return 0
			}())
	}

	// For read-only opens, try to use fd pool + cache reader
	if !isWrite && e != nil {
		fusePath := jfs.fullPath(filename)
		newCachedFile := func(fd *os.File) *cachedFile {
			if hook := jfs.handler.testCacheBeforeIdentity; hook != nil {
				hook(filename)
			}
			identity, identityKnown := jfs.handler.store.LookupCachedFileIdentity(filename)
			conflict := !identityKnown && e.MtimeNsecKnown && !e.LocalOnly && e.Version.Incarnation != "" && e.Version.Revision > 0
			if identityKnown && (identity.Version != e.Version || identity.Inode != e.Inode || identity.Size != e.Size || identity.MtimeNS != e.Mtime.UnixNano()) {
				conflict = true
			}
			return &cachedFile{
				cacheIdentityConflict: conflict,
				cacheIdentity:         identity,
				name:                  filename,
				fuseFD:                fd,
				fusePath:              fusePath,
				fdPool:                jfs.handler.fdPool,
				handler:               jfs.handler,
				cacheReader:           jfs.handler.cacheReader.Load(),
				readahead:             jfs.handler.readahead,
				memBuf:                jfs.handler.memBuf,
				inode:                 e.Inode,
				fileSize:              e.Size,
				pinned:                isPinned,
				pinGeneration:         pinAdmission.Generation,
				// QA-31 + HIGH-1 fix: value-copy snapshot, NOT a pointer
				// to the live Entry. The Entry can be mutated in-place by
				// concurrent UpdateSize on writeback paths.
				cachedInfo: &snapshotFileInfo{
					name:  e.Name,
					size:  e.Size,
					mode:  e.Mode,
					mtime: e.Mtime,
					isDir: e.IsDir,
					inode: e.Inode,
				},
			}
		}
		if proof := jfs.handler.strongSidecarRead(filename); proof != nil {
			f := newCachedFile(nil)
			f.sidecarProof = proof
			return f, nil
		}
		// Do not eagerly open JuiceFS/FUSE when the verified SSD reader is
		// available. NFSv3 has no stateful OPEN operation: this OpenFile call is
		// part of the READ RPC, and an eager FUSE open paid remote metadata before
		// ReadAt had a chance to prove the requested bytes were local (measured
		// 6.7s on the first cellular read despite 128/128 direct SSD hits). A cache
		// miss lazily acquires the same pooled FUSE descriptor inside ReadAt, so
		// cold/error semantics stay on the same RPC while cache hits avoid FUSE.
		if cr := jfs.handler.cacheReader.Load(); cr != nil && cacheReaderServeEnabled {
			return newCachedFile(nil), nil
		}
		if pinAdmission.Generation != "" {
			return nil, pin.ErrOfflineNotAvailable
		}
		if err := jfs.handler.cacheBackendError(); err != nil {
			return nil, err
		}
		fd, err, ok := pooledReadOpenWithTimeout(metrics.FUSESrcForeground, jfs.handler.fdPool, fusePath, readOpenTimeout())
		if !ok {
			return nil, errFUSETimeout
		}
		if err != nil {
			if os.IsNotExist(err) {
				// QA-32 (2026-05-25): the phantom-purge here used to fire
				// unconditionally. That destroyed pinned-file cache entries
				// during write-upload windows when FUSE returns ENOENT for
				// legitimate files because juicefs is busy uploading
				// staging blocks. Layer C protects the prune path; this
				// open path bypassed it. Three guards now match Stat()'s
				// phantom-purge in spirit:
				//   1. NEVER purge a pinned file. Pinning is the user's
				//      explicit contract. If FUSE says ENOENT on a pinned
				//      path, FUSE is wrong, not the cache.
				//   2. NEVER purge while there's an active writer for
				//      this path — concurrent OpenFile races with writes.
				//   3. Verify via a 2-second-budgeted Lstat that the file
				//      really doesn't exist before destroying the entry
				//      (FUSE's fdPool.Get ENOENT can be a fast lie under
				//      load; an explicit Lstat re-probes deterministically).
				canonical := jfs.handler.canonicalize(filename)
				if jfs.handler.isPinnedReady(canonical) {
					jmlog.Debug("open ENOENT on pinned file — NOT purging (FUSE likely busy)",
						"path", filename)
					return nil, err
				}
				if jfs.handler.hasActiveWriter(filename) {
					jmlog.Debug("open ENOENT but active writer present — NOT purging",
						"path", filename)
					return nil, err
				}
				if spool := jfs.handler.spool.Load(); spool != nil && spool.HasPending(filename) {
					// QA-30 Layer D twin (2026-06-28): spool-pending file —
					// written+closed but not yet drained, so FUSE ENOENT is
					// expected, not a phantom. Purging it destroys the cache
					// entry and STALEs Finder's handle (FromHandle), freezing
					// the copy. Keep the entry; the read just returns ENOENT
					// until the drainer lands it. See the Stat purge gate.
					jmlog.Debug("open ENOENT but spool-pending — NOT purging (queued to drain)",
						"path", filename)
					return nil, err
				}
				if strings.HasPrefix(path.Base(filename), "._") {
					// QA-31 (2026-06-28): NEVER phantom-purge a ._AppleDouble sidecar
					// on open. They are scan-filtered from the backend SCAN, so FUSE
					// ENOENT is an unreliable phantom signal; a drained-and-evicted ._
					// file (HasPending==false) that is transiently ENOENT would be
					// wrongly purged -> FromHandle STALE -> intermittent stall. Sidecars
					// are transient; keep the entry, just return ENOENT. See the Stat gate.
					jmlog.Debug("open ENOENT on ._ sidecar - NOT purging (scan-filtered, transient)",
						"path", filename)
					return nil, err
				}
				// V2.3 G0: with the mountpoint a plain directory (macFUSE kext
				// not loaded / mount absent), FUSE Lstat-ENOENT is true for
				// EVERY backend file not locally present — purging on it would
				// erode the mirror wholesale. Identity first, then Lstat.
				if identOK, identReason := pin.FUSEIdentityState(); !identOK {
					jmlog.Debug("open ENOENT but FUSE identity gate failed — NOT purging",
						"path", filename, "reason", identReason)
					return nil, err
				}
				isNotExist, ok := lstatNotExistWithTimeout(metrics.FUSESrcForeground, fusePath, 2*time.Second)
				if !ok {
					jmlog.Debug("open ENOENT but Lstat-verify timed out — NOT purging (FUSE degraded)",
						"path", filename)
					return nil, err
				}
				if !isNotExist {
					jmlog.Debug("open ENOENT but Lstat-verify says file exists — NOT purging (FUSE racy)",
						"path", filename)
					return nil, err
				}
				jmlog.Warn("purging phantom entry on open", "path", filename)
				jfs.handler.store.DeleteFromCache(filename)
				go jfs.handler.store.Delete(filename)
			}
			return nil, err
		}
		return newCachedFile(fd), nil
	}

	// For writes, use fd pool to avoid re-opening on every NFS WRITE RPC.
	// The go-nfs library calls OpenFile→Write→Close on every WRITE RPC,
	// so pooling the fd saves an open() + close() syscall per RPC.
	fullPath := jfs.fullPath(filename)

	if isWrite {
		// Spool-routed write (Option 2): route to the spool ONLY when this
		// path already has an active spool entry (created via juiceFS.Create
		// and not yet drained). NFS WRITE RPCs arrive as OpenFile(O_RDWR)
		// with NO O_CREATE (internal/nfs/nfs_onwrite.go), so the spool keys
		// off the index — keying off O_CREATE made the spool unreachable
		// over NFS entirely (Finding 1). A path with NO spool entry is an
		// in-place modify of a file that already lives in FUSE; that MUST
		// stay on the legacy fdPool path so the drainer never truncates it
		// via os.Create.
		if spool := jfs.handler.spool.Load(); spool != nil {
			_, active := spool.LookupActive(filename)
			if active || cohort != nil {
				var sentry *SpoolEntry
				var err error
				if cohort != nil {
					if spool != cohort.spool {
						return nil, ErrSpoolBusy
					}
					sentry, err = spool.OpenWriteExpected(filename, cohort.entry, cohort.rowID)
				} else {
					sentry, err = spool.OpenWrite(filename)
				}
				if err != nil {
					return nil, err
				}
				// Keep the synthetic inode stable for the entry's lifetime
				// (Stat/Lstat shadow + NFS handle). SetInode is idempotent;
				// the inode was already assigned at Create time, so this is
				// normally a no-op — the fallback only fires for the unusual
				// case of an indexed entry with no inode yet.
				if sentry.Inode() == 0 {
					inode := jfs.handler.nextSyntheticInode()
					if e != nil {
						inode = e.Inode
					}
					sentry.SetInode(inode)
				}
				// QA-19 phantom-purge gate; released in spoolWriteFile.Close.
				jfs.handler.incActiveWriterFrom(filename, "open-active-spool")
				return &spoolWriteFile{
					name:    filename,
					entry:   sentry,
					handler: jfs.handler,
				}, nil
			}
		}
		if cohort != nil {
			return nil, ErrSpoolBusy
		}

		// A complete cached AppleDouble/.DS_Store body makes an in-place
		// metadata rewrite safe to keep on the local spool even while online.
		// Finder issues these rewrites as multiple independent NFS WRITE RPCs.
		// If the first online RPC fell back to the direct FUSE fd, its write-open
		// invalidated the only complete image; a route flip before the next range
		// RPC then had neither an active shadow nor safe untouched bytes and was
		// correctly (but user-visibly) refused with NFS3ERR_ACCES. Seed a fresh
		// spool generation before invalidating the cache so the same authoritative
		// whole-file image spans the online -> offline transition. Ordinary media
		// never enters this branch, and incomplete metadata still fails closed.
		if seed, ok := jfs.handler.offlineMetadataSeed(filename); ok {
			spool := jfs.handler.spool.Load()
			if spool == nil {
				if pin.IsOffline() {
					return nil, pin.ErrOfflineNotAvailable
				}
			} else {
				sentry, err := spool.OpenWriteSeeded(filename, seed)
				if err != nil {
					return nil, err
				}
				inode := jfs.handler.nextSyntheticInode()
				if e != nil {
					inode = e.Inode
				}
				sentry.SetInode(inode)
				jfs.handler.incActiveWriterFrom(filename, "open-seeded-online")
				return &spoolWriteFile{name: filename, entry: sentry, handler: jfs.handler}, nil
			}
		}

		// No spool shadow means this is an in-place edit of a backend-resident
		// file. During a route outage that path cannot be made durable locally
		// without first knowing the untouched byte ranges, so fail promptly.
		// SETATTR{size=0} for a whole-file replacement creates the spool shadow
		// through TruncateFile before its WRITE RPCs reach this branch.
		if pin.IsOffline() {
			// Finder can revisit an already-drained `._` sidecar (or .DS_Store)
			// after the route flips offline. If we retained its COMPLETE previous
			// body, atomically seed a new spool image and apply the in-place WRITE
			// there. This preserves untouched ranges and keeps the whole Finder
			// copy alive. No complete image means no safe copy-up: fail closed.
			if seed, ok := jfs.handler.offlineMetadataSeed(filename); ok {
				spool := jfs.handler.spool.Load()
				if spool == nil {
					return nil, pin.ErrOfflineNotAvailable
				}
				sentry, err := spool.OpenWriteSeeded(filename, seed)
				if err != nil {
					return nil, err
				}
				inode := jfs.handler.nextSyntheticInode()
				if e != nil {
					inode = e.Inode
				}
				sentry.SetInode(inode)
				jfs.handler.incActiveWriterFrom(filename, "open-seeded-offline")
				return &spoolWriteFile{name: filename, entry: sentry, handler: jfs.handler}, nil
			}
			mirrorMtime, mirrorSize, mirrorOK := jfs.handler.sidecarCurrentMeta(filename)
			cacheState := jfs.handler.sidecar.state(filename)
			jmlog.Warn("offline: refusing unsafe in-place write without spool shadow",
				"in_mount", filename,
				"mirror_present", mirrorOK,
				"mirror_mtime", mirrorMtime,
				"mirror_size", mirrorSize,
				"cache_present", cacheState.present,
				"cache_mtime", cacheState.mtime,
				"cache_size", cacheState.size,
				"cache_len", cacheState.length,
				"writer_active", jfs.handler.hasActiveWriter(filename))
			return nil, pin.ErrOfflineNotAvailable
		}

		if err := jfs.handler.cacheBackendError(); err != nil {
			return nil, err
		}
		fd, err := jfs.handler.fdPool.GetWrite(fullPath, flag, perm)
		if err != nil {
			return nil, err
		}
		inode := uint64(0)
		if e != nil {
			inode = e.Inode
		} else {
			inode = jfs.handler.nextSyntheticInode()
		}
		entry := &metadata.Entry{Path: filename, Inode: inode}
		// QA-19: register the active writer BEFORE returning. Pair
		// with decActiveWriter in writeFile.Close so the phantom-purge
		// gate in Stat() can see this writer is in flight.
		jfs.handler.incActiveWriterFrom(filename, "open-direct-backend")
		// baseSize is the contiguous prefix this writer starts from: the file's
		// existing content is all valid, so a hole only begins above it. Taken
		// from the mirror entry already in hand — no syscall. e == nil means a
		// brand-new file, whose valid prefix is genuinely 0.
		var baseSize int64
		if e != nil {
			baseSize = e.Size
		}
		return &writeFile{
			File:     fd,
			name:     filename,
			handler:  jfs.handler,
			entry:    entry,
			fdPool:   jfs.handler.fdPool,
			fusePath: fullPath,
			baseSize: baseSize,
		}, nil
	}

	if err := jfs.handler.cacheBackendError(); err != nil {
		return nil, err
	}
	// BOUNDED open: a wedged JuiceFS can't hang this OPEN/READ RPC path.
	f, err, ok := openFileWithTimeout(metrics.FUSESrcForeground, fullPath, flag, perm, readOpenTimeout())
	if !ok {
		return nil, errFUSETimeout
	}
	if err != nil {
		return nil, err
	}
	// Carry forward the same pin snapshot we computed at the top of OpenFile.
	// Writes don't gate (writes are always allowed); reads through this
	// branch (e == nil, no metadata cache) pick up the same offline policy
	// as the cachedFile branch.
	return &billyFile{File: f, name: filename, pinned: isPinned, handler: jfs.handler}, nil
}

// TruncateFile implements internal/nfs.FileTruncater. While online the normal
// OpenFile+Truncate path retains existing in-place semantics. While offline, an
// active spool entry is already safe and likewise uses the normal path. For a
// backend-resident file with no spool shadow, only truncate-to-zero is a safe
// local operation: it declares a complete replacement and lets all subsequent
// WRITE RPCs populate a fresh durable spool image. Non-zero truncates would
// require copying unavailable old bytes, so they fail immediately.
func (jfs *juiceFS) TruncateFile(filename string, size int64) (bool, error) {
	if !pin.IsOffline() {
		return false, nil
	}
	filename = strings.TrimPrefix(filename, "/")
	spool := jfs.handler.spool.Load()
	if spool == nil {
		return true, pin.ErrOfflineNotAvailable
	}
	if _, active := spool.LookupActive(filename); active {
		return false, nil
	}
	if size != 0 {
		jmlog.Warn("offline: refusing unsafe partial truncate without spool shadow",
			"in_mount", filename, "size", size)
		return true, pin.ErrOfflineNotAvailable
	}

	entry := jfs.handler.store.LookupByPath(filename)
	if entry == nil || entry.IsDir {
		return true, os.ErrNotExist
	}
	sentry, err := spool.OpenWrite(filename)
	if err != nil {
		return true, err
	}
	inode := entry.Inode
	if inode == 0 {
		inode = jfs.handler.nextSyntheticInode()
	}
	sentry.SetInode(inode)
	jfs.handler.clampWriteSize(filename, 0)
	if err := jfs.handler.store.SetSizeExact(filename, 0, time.Now()); err != nil {
		// This entry was created by this method and has not been exposed to a
		// writer yet, so cancellation is a complete rollback.
		spool.CancelForDelete(filename)
		return true, err
	}
	// OpenWrite owns one per-operation reference. SETATTR itself does not
	// return a file handle, so release it now; subsequent WRITE RPCs reopen the
	// same entry and hold their own references.
	sentry.ReleaseHandle()
	return true, nil
}

// CommitFile satisfies the internal/nfs Committer interface: it fsyncs any
// buffered spool data for path to stable storage (the NFS COMMIT / FILE_SYNC
// durability barrier) WITHOUT finalizing the entry, so a power loss after the
// client's fsync/commit can't lose acknowledged bytes. No-op when the path is
// not currently spooled (already drained, or spool disabled — those bytes are
// durable via their own path).
func (jfs *juiceFS) CommitFile(path string) error {
	if err := jfs.handler.namespace.servingError(path); err != nil {
		return err
	}
	spool := jfs.handler.spool.Load()
	if spool == nil {
		return nil
	}
	path = strings.TrimPrefix(path, "/")
	if e, ok := spool.Index().Lookup(path); ok {
		start := time.Now()
		err := e.Sync()
		// #105: mark committed so the sweeper finalizes even a large entry on the
		// short idle — a Premiere save/export close finalizes ~seconds after its
		// COMMIT instead of waiting the full window (safe via the reopen-defer).
		e.MarkCommitted()
		// #105 COMMIT instrumentation: capture macOS's COMMIT cadence on a real
		// export. written_end = how many bytes the client has asked to make
		// durable; since_last_write_ms distinguishes a close-time COMMIT (large,
		// after writes stopped) from a periodic mid-write COMMIT (writes still
		// flowing). This decides whether a COMMIT is a reliable "done" signal we
		// can finalize on, or periodic (needs the reopen-safety guard first).
		// COMMITs are infrequent vs WRITEs, so Info is not a hot-path flood.
		jmlog.Info("spool: COMMIT",
			"path", path,
			"written_end", e.WrittenEnd(),
			"since_last_write_ms", time.Since(e.LastWrite()).Milliseconds(),
			"sync_ms", time.Since(start).Milliseconds())
		return err
	}
	return nil
}

func (jfs *juiceFS) Create(filename string) (billy.File, error) {
	filename = strings.TrimPrefix(filename, "/")
	return runNamespaceOperation(jfs.handler, []string{filename}, func(op *namespaceOperation) (billy.File, error) {
		return jfs.create(filename, op)
	}, closeNamespaceFile)
}

func (jfs *juiceFS) create(filename string, op *namespaceOperation) (billy.File, error) {
	if spool := jfs.handler.spool.Load(); spool != nil && spool.ReadFault(filename) {
		return nil, syscall.EIO
	}
	now := time.Now()

	// Spool-routed create (Option 2). The CREATE RPC is the new-file entry
	// point (internal/nfs/nfs_oncreate.go calls fs.Create). When the spool
	// is enabled we open a spool entry instead of creating the file on FUSE;
	// the drainer materializes it in FUSE later. We STILL insert the
	// metadata entry (same synthetic inode as the legacy path) so the NFS
	// handle resolves via ToHandle/FromHandle and the file appears in
	// directory listings immediately. Stat/Lstat/READ are shadowed by the
	// spool index (real growing size) until the drainer lands the file, at
	// which point onSpoolDrained syncs the final size into this entry.
	if spool := jfs.handler.spool.Load(); spool != nil {
		inode := jfs.handler.nextSyntheticInode()
		e := metadata.MakeEntry(filename, false, 0, now, inode)
		e.LocalOnly = true
		// The bounded RPC waits for publication; a slow SQLite writer keeps
		// namespace ownership after the RPC returns JUKEBOX.
		sentry, err := spool.OpenWrite(filename)
		if err != nil {
			return nil, err
		}
		op.mutated = true
		sentry.SetInode(inode)
		op.checkpoint("publish", filename)
		if err := jfs.handler.store.Insert(e); err != nil {
			sentry.ReleaseHandle()
			return nil, err
		}
		jfs.handler.incActiveWriterFrom(filename, "create-spool")
		return &spoolWriteFile{name: filename, entry: sentry, handler: jfs.handler}, nil
	}

	// An unchecked CREATE can name an existing file. Route its truncate
	// through the same durable protocol as O_TRUNC, without a second namespace
	// admission while this operation owns the name.
	createExpected, err := jfs.handler.store.CaptureMirror(filename)
	if err != nil {
		return nil, err
	}
	if createExpected.Incarnation != "" {
		file, err := jfs.openFile(filename, os.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		wf, ok := file.(*writeFile)
		if !ok {
			_ = file.Close()
			return nil, metadata.ErrMirrorConflict
		}
		if err := wf.bind(createExpected); err != nil {
			wf.release()
			return nil, err
		}
		if err := wf.truncateOwned(0, op); err != nil {
			wf.release()
			return nil, err
		}
		return wf, nil
	}

	// Reserve the absent name durably before O_EXCL. Any ambiguous result keeps
	// the reservation and refuses the next startup rather than adopting a name
	// whose backing and mirror may disagree.
	mountGeneration, err := currentMountGeneration(jfs.handler.fusePath)
	if err != nil {
		return nil, err
	}
	parents, err := jfs.openCreateParent(filename, mountGeneration, op)
	if err != nil {
		return nil, err
	}
	defer parents.close()
	if err := parents.verify(); err != nil {
		return nil, err
	}
	createExpected, err = jfs.handler.store.CaptureMirror(filename)
	if err != nil {
		return nil, err
	}
	if createExpected.Incarnation != "" {
		return nil, metadata.ErrMirrorConflict
	}
	createIntent, err := jfs.handler.store.PrepareCreate(createExpected, mountGeneration)
	if err != nil {
		return nil, err
	}
	op.poison = true
	op.checkpoint("create-intent", filename)
	if err := parents.verify(); err != nil {
		return nil, err
	}
	// O_EXCL refuses an unmirrored existing pathname instead of truncating it.
	fullPath := jfs.fullPath(filename)
	f, err := parents.create(path.Base(filename), fullPath)
	if err != nil {
		if errors.Is(err, syscall.EEXIST) {
			if verifyErr := parents.verify(); verifyErr != nil {
				return nil, verifyErr
			}
			if abortErr := jfs.handler.store.AbortPreparedCreate(createIntent); abortErr != nil {
				return nil, abortErr
			}
			op.poison = false
		}
		return nil, err
	}
	op.mutated = true
	defer func() {
		if op.poison {
			_ = f.Close()
		}
	}()
	if err := verifyCreatedFileMount(f, jfs.handler.fusePath, mountGeneration); err != nil {
		return nil, err
	}
	if err := parents.verify(); err != nil {
		return nil, err
	}
	createdInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if err := jfs.handler.syncOnlineMutation(f); err != nil {
		return nil, err
	}
	if err := verifyCreatedFileMount(f, jfs.handler.fusePath, mountGeneration); err != nil {
		return nil, err
	}
	if err := parents.verify(); err != nil {
		return nil, err
	}

	// Publish durably before handing the new name to subsequent operations.
	e := metadata.MakeEntry(filename, false, 0, now, jfs.handler.nextSyntheticInode())
	e.LocalOnly = true
	op.checkpoint("publish", filename)
	committed, err := jfs.handler.store.CompletePreparedCreate(createIntent, e)
	if err != nil {
		return nil, err
	}
	e = committed
	op.poison = false

	// QA-19: Create returns a writeFile whose Close calls decActiveWriter.
	// Match it with an inc here so the phantom-purge gate sees the writer.
	// Without this, the NFS CREATE → first-write window (which is exactly
	// fio seqwrite's startup path) gets no protection.
	jfs.handler.incActiveWriterFrom(filename, "create-backend")

	wf := &writeFile{
		File:     f,
		name:     filename,
		handler:  jfs.handler,
		entry:    e,
		fusePath: fullPath,
		bound:    e.Version,
		identity: createdInfo,
	}
	return wf, nil
}

func (jfs *juiceFS) Rename(oldpath, newpath string) error {
	oldpath = strings.TrimPrefix(oldpath, "/")
	newpath = strings.TrimPrefix(newpath, "/")
	return jfs.namespaceMutation([]string{oldpath, newpath}, func(op *namespaceOperation) error {
		return jfs.rename(oldpath, newpath, op)
	})
}

func (jfs *juiceFS) rename(oldpath, newpath string, op *namespaceOperation) error {
	if spool := jfs.handler.spool.Load(); spool != nil && (spool.ReadFaultUnder(oldpath) || spool.ReadFaultUnder(newpath)) {
		return syscall.EIO // unresolved saved bytes require explicit recovery
	}
	// Snapshot the source type/locality before the spool migration re-keys it.
	// A new LocalOnly regular file exists solely in the SSD spool; probing its
	// nonexistent source with os.Rename forces a synchronous remote FUSE lookup
	// just to receive ENOENT. Under drain load that measured 223ms and delayed an
	// otherwise-cached Stat on the same NFS connection past the 200ms release
	// ceiling. We use this snapshot for the safe no-source fast path below and
	// later for exact-vs-tree FD invalidation.
	oldEntry := jfs.handler.store.LookupByPath(oldpath)
	destEntry := jfs.handler.store.LookupByPath(newpath)
	if oldpath != newpath {
		if err := jfs.checkKnownRenameDestination(oldEntry, destEntry, newpath); err != nil {
			return err
		}
	}
	var sourceToken, destinationToken metadata.MirrorToken
	if oldEntry != nil && !oldEntry.Version.IsZero() {
		var err error
		sourceToken, destinationToken, err = jfs.handler.store.CaptureRename(oldpath, newpath)
		if err != nil {
			return err
		}
		if sourceToken != oldEntry.Version || (destEntry != nil && destinationToken != destEntry.Version) ||
			(destEntry == nil && destinationToken.Incarnation != "") {
			return metadata.ErrMirrorConflict
		}
	}

	if handled, err := jfs.acceptOfflineReplacement(oldEntry, destEntry, op); handled {
		return err
	}
	if spool := jfs.handler.spool.Load(); spool != nil {
		if err := spool.meta.CheckReplacementUnder(oldpath, newpath); err != nil {
			return err
		}
	}

	// Spool-aware rename (Phase-1 BUG 1). Order matters:
	//
	//  1. Cancel any active spool entry at the DESTINATION. POSIX rename
	//     replaces dst; without the cancel, dst's old entry keeps shadowing
	//     reads and its queued drain later overwrites the renamed file with
	//     the replaced file's bytes (same hazard class as QA-37 deletes).
	//  2. Migrate the SOURCE's active entry (and, for directory renames,
	//     every entry under oldpath+"/") to the new path — index key, SQL
	//     row, and drain target. This MUST happen before the FUSE rename:
	//     once the migration commits, an in-flight drain that already
	//     claimed the old target observes done=false at MarkDrainComplete
	//     and undoes its FUSE write instead of resurrecting the old path.
	//
	// Migration failure fails the RPC: proceeding with a FUSE rename while
	// the spool still targets the old path is exactly the silent-resurrect
	// bug this exists to fix. The client retries cleanly.
	migrated := 0
	if spool := jfs.handler.spool.Load(); spool != nil {
		// Cancel any active destination entry UNCONDITIONALLY (adversarial-
		// review BUG C) — gating on LookupActive missed rows recovered at
		// boot, which are deliberately NOT re-indexed (RecoverOnBoot): a
		// rename over such a row left its stale drain queued, able to land
		// after ours and overwrite the renamed file with the replaced file's
		// bytes. CancelForDelete is a no-op when nothing exists (same
		// unconditional pattern as Remove).
		changed, cancelErr := spool.cancelForDelete(newpath)
		op.mutated = changed
		if cancelErr != nil {
			return cancelErr
		}
		n, needSignal, err := spool.MigrateForRename(oldpath, newpath)
		if err != nil {
			op.poison = true // a failed durable migration needs recovery
			jmlog.Warn("rename: spool migration failed — failing RPC",
				"old", oldpath, "new", newpath, "error", err.Error())
			return err
		}
		migrated = n
		op.mutated = op.mutated || n > 0
		if needSignal {
			// Wake the drainer only AFTER our FUSE rename below. Waking it
			// here raced us: draining a just-migrated entry MkdirAll's the
			// destination, and our own os.Rename then failed EEXIST against
			// the directory we had just caused to exist. See
			// MigrateForRename's note and JuiceMount task #2.
			defer spool.SignalReady()
		}
	}

	// Execute on FUSE. A purely-spooled file hasn't been drained yet, so it
	// does not exist on FUSE — ENOENT here is the EXPECTED case when the
	// spool migration moved entries, not a failure (the drain materializes
	// the new path). Any other combination keeps the legacy contract:
	// errors propagate (onRename maps them to NFS statuses).
	// KNOWN BUG (2026-08-03, JuiceMount task #2): this rename can fail EEXIST
	// because our OWN drainer materialized the destination. The drainer creates
	// an entry's parent with os.MkdirAll (drainer.go:699); for a directory
	// rename we have just re-keyed every entry under oldpath to newpath, so a
	// drain firing before this line creates newpath and the rename then loses
	// to it. JuiceFS does not implement the POSIX "rename replaces an empty
	// target directory" case, so this is fatal rather than replaceable.
	//
	// Measured: a 300-file folder moved in Finder immediately after being
	// written failed ~50% of the time (identically on stock 0.4.0), leaving the
	// folder split across source and destination. Settled folders never fail —
	// nothing is in the spool to migrate or drain.
	//
	// Two fixes were tried and MEASURED NOT TO WORK; do not retry them:
	//   1. Deferring the drainer wake out of MigrateForRename (kept anyway as
	//      correct hardening — we should not kick a drainer into a race we are
	//      about to lose — but the drainer also ticks on its own schedule).
	//   2. rmdir-the-destination-and-retry. The destination is NOT empty by the
	//      time we get here: the drainer has already landed `._` sidecars, so
	//      os.Remove correctly refuses, and reclaiming it would mean deleting
	//      drained data. 6/8 still failed (-48/-47).
	// The real fix is mutual exclusion between a directory rename and drains
	// targeting that subtree.
	// Fast path for a purely-spooled new regular file and an actually-absent
	// destination: the spool migration above IS the rename. There is no source
	// inode in FUSE to move. The metadata mirror is the NFS namespace authority;
	// require it to show no destination so POSIX replacement/type-check semantics
	// still take the legacy path. This check is deliberately local: replacing a
	// known-nonexistent remote probe with another remote Lstat would preserve the
	// very head-of-line delay this fast path removes.
	skipFUSERename := false
	if migrated > 0 && oldEntry != nil && !oldEntry.IsDir && oldEntry.LocalOnly && destEntry == nil {
		skipFUSERename = true
	}
	if !skipFUSERename {
		if err := os.Rename(jfs.fullPath(oldpath), jfs.fullPath(newpath)); err != nil {
			if !(migrated > 0 && os.IsNotExist(err)) {
				return err
			}
			// The source exists only in the spool. Keep any complete destination
			// until the drainer installs a verified replacement atomically or
			// publishes a conflict copy. Local reads remain owned by the spool
			// shadow; recovery restores that authority before serving.
		}
	}
	op.mutated = true
	op.checkpoint("publish", oldpath)

	// Invalidate both read caches for both ends: the old path is gone, and a
	// replaced destination must not serve the pre-rename bytes from the memory
	// buffer OR the direct-SSD slice cache.
	jfs.handler.invalidateReadCaches(oldpath)
	jfs.handler.invalidateReadCaches(newpath)

	// Drop the POOLED FDs for both ends (C1/C2, 2026-07-28). The FDPool is
	// keyed by path alone, so after this rename:
	//
	//   - oldpath's pooled READ fd still points at the file that just MOVED.
	//     A new file created at oldpath would be read as the moved file's
	//     bytes — right length, wrong content, no error (C1).
	//   - oldpath's pooled WRITE fd is worse: an in-place rewrite of oldpath
	//     (the legacy non-spool write path — atomic-save "save v1 aside, write
	//     a fresh original") would land every WriteAt INSIDE the renamed-away
	//     file, destroying the archive AND never writing the new file (C2).
	//   - newpath's pooled fds point at whatever POSIX rename just replaced
	//     (or unlinked) at the destination.
	//
	// Tree-scoped ONLY for a directory: a DIRECTORY rename staleness every
	// descendant's pooled fd in the same syscall, and doing it off the pool's
	// own key set (rather than the mirror's descendant list) stays correct even
	// when the mirror has evicted a descendant. Must run AFTER the os.Rename
	// above so a concurrent Get can't immediately re-pool the pre-rename inode.
	//
	// A FILE rename takes the exact-key path instead. InvalidateTree is an
	// O(len(p.entries)) scan held under the SINGLE pool mutex, and a .app/ditto
	// bundle copy is a rename PER FILE while p.entries holds thousands of keys
	// from the copy's own parallel WRITE RPCs — two full scans per rename on the
	// exact lock that produced the 2026-06-14 convoy (93 goroutines wedged in
	// GetWrite → "error 100060"; see evictLoop). The type is authoritative for
	// BOTH ends: POSIX rename cannot change it (file→dir is EISDIR, dir→file is
	// ENOTDIR), so a successful rename of a file has a file at both names. An
	// unmirrored source (oldEntry nil) falls back to the tree scan.
	if oldEntry != nil && !oldEntry.IsDir {
		jfs.handler.invalidatePooledFDs(oldpath)
		jfs.handler.invalidatePooledFDs(newpath)
	} else {
		jfs.handler.invalidatePooledFDTree(oldpath)
		jfs.handler.invalidatePooledFDTree(newpath)
	}

	// Carry the in-flight write-size high-water mark across the rename so
	// (a) Stat at the new path stays accurate for a file still being
	// written, and (b) a FUTURE file created at the old path doesn't
	// inherit a stale inflated size from the sticky map (QA-16 MAX
	// semantics never shrink).
	jfs.handler.writeSizeMu.Lock()
	if sz, ok := jfs.handler.writeSizes[oldpath]; ok {
		delete(jfs.handler.writeSizes, oldpath)
		delete(jfs.handler.writeSizeAt, oldpath)
		if cur, ok := jfs.handler.writeSizes[newpath]; !ok || sz > cur {
			jfs.handler.writeSizes[newpath] = sz
			jfs.handler.touchWriteSizeLocked(newpath)
		}
	}
	jfs.handler.writeSizeMu.Unlock()

	// Update visibility and durable rows while both names remain reserved.
	// oldEntry was resolved above (the pooled-fd invalidation needs its type);
	// nothing between there and here mutates the mirror.
	if oldEntry != nil {
		// The Store preserves mode, locality and logical incarnation while
		// moving the complete durable subtree in one transaction.
		if !sourceToken.IsZero() {
			if _, err := jfs.handler.store.RenameExpected(sourceToken, destinationToken); err != nil {
				return err
			}
		} else {
			// RAM-only legacy observations have no durable token. Retain the
			// legacy publication path until their producers capture versions.
			clone := *oldEntry
			clone.Path, clone.Name, clone.ParentPath = newpath, path.Base(newpath), path.Dir(newpath)
			clone.ResetGetAttrCache()
			if err := jfs.handler.store.Delete(oldpath); err != nil {
				return err
			}
			if err := jfs.handler.store.Insert(&clone); err != nil {
				return err
			}
			if oldEntry.IsDir {
				if _, err := jfs.handler.store.RenameSubtreeSync(oldpath, newpath); err != nil {
					return err
				}
			}
		}

		// Publish rename event. IsSymlink must ride along: applyEvent (self-write
		// pub/sub AND peers) rebuilds the destination entry from the event alone,
		// and without the discriminator it collapses a renamed symlink back to a
		// 0644 regular file — re-clobbering the type the clone above preserved.
		jfs.handler.publishEvent(metadata.MetadataEvent{
			Op: "rename", Path: newpath, OldPath: oldpath,
			Size: oldEntry.Size, Mtime: oldEntry.Mtime.Unix(),
			Inode: oldEntry.Inode, IsDir: oldEntry.IsDir,
			IsSymlink: oldEntry.Mode&os.ModeSymlink != 0,
		})
	} else {
		// An unmirrored source still publishes before releasing either name.
		if err := jfs.handler.store.Delete(oldpath); err != nil {
			return err
		}
		// Lstat, not Stat: a renamed SYMLINK must be recorded as itself.
		// os.Stat follows the link, so this branch used to insert the
		// TARGET's type/size under the link's path (same type-flip class
		// as the MakeEntry clobber above), or skip dangling links entirely.
		info, err := os.Lstat(jfs.fullPath(newpath))
		if err == nil {
			var inode uint64
			if st, ok := info.Sys().(*syscall.Stat_t); ok {
				inode = st.Ino
			} else {
				inode = jfs.handler.nextSyntheticInode()
			}
			e := metadata.MakeEntry(newpath, info.IsDir(), info.Size(), info.ModTime(), inode)
			e.Mode = info.Mode()
			if err := jfs.handler.store.Insert(e); err != nil {
				return err
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (jfs *juiceFS) Remove(filename string) error {
	filename = strings.TrimPrefix(filename, "/")
	return jfs.namespaceMutation([]string{filename}, func(op *namespaceOperation) error {
		return jfs.remove(filename, op)
	})
}

func (jfs *juiceFS) remove(filename string, op *namespaceOperation) error {
	if spool := jfs.handler.spool.Load(); spool != nil && spool.ReadFaultUnder(filename) {
		return syscall.EIO
	}

	// Invalidate both read caches BEFORE the entry is evicted below (so the
	// inode is still resolvable for the slice-cache drop).
	jfs.handler.invalidateReadCaches(filename)

	// QA-37: cancel any in-flight spool entry FIRST, so a pending or
	// mid-flight drain can't resurrect the file we're about to delete. A
	// drain already copying to FUSE undoes its write when it finds the row
	// gone at MarkDrainComplete.
	if spool := jfs.handler.spool.Load(); spool != nil {
		changed, err := spool.cancelForDelete(filename)
		op.mutated = changed
		if err != nil {
			return err
		}
	}

	e := jfs.handler.store.LookupByPath(filename)

	// The physical unlink is the acknowledgement boundary. The previous code
	// discarded os.Remove's error, evicted the mirror and returned NFS success.
	// If JuiceFS rejected or stalled that unlink, keyspace reconciliation saw
	// the still-live dentry and reinserted it just as Finder re-created the same
	// name: the deterministic rm-rf -> immediate re-copy "-48 already an item"
	// failure. Keep the mirror authoritative until a bounded FUSE remove either
	// succeeds or proves the path is already absent. The enclosing worker
	// retains ownership if the waiting RPC times out.
	op.checkpoint("remove", filename)
	removeErr := os.Remove(jfs.fullPath(filename))
	if removeErr != nil && !os.IsNotExist(removeErr) {
		jmlog.Warn("FUSE remove failed; preserving mirror entry",
			"path", filename, "error", removeErr.Error())
		return removeErr
	}
	op.mutated = true
	op.checkpoint("publish", filename)

	// Evict the live namespace immediately AFTER the dentry is gone. The
	// deleted shadow keeps a retained NFS handle safe without relisting the old
	// generation; the durable delete can then serialize behind a bulk insert
	// without exposing the name to a back-to-back CREATE.
	jfs.handler.store.DeleteFromCacheForUnlink(filename)
	if err := jfs.handler.store.Delete(filename); err != nil {
		return err
	}

	// Drop the POOLED FDs for this path (C3, 2026-07-28). Run AFTER the FUSE
	// unlink, so a Get racing this Remove can only re-pool an fd for a file
	// that still exists. Without this, `rm take.mov` → recreate → write took
	// GetWrite's cached fd to the UNLINKED inode: every byte went to a ghost
	// the kernel reclaimed on close, the recreated file stayed empty, and no
	// layer returned an error. The read slot is the same bug in the read
	// direction (serving the deleted generation's bytes to a recreated name).
	jfs.handler.invalidatePooledFDs(filename)

	// Drop the sticky write-size high-water mark (C4, 2026-07-28). writeSizes
	// is MAX-only (trackWriteSize) and Stat/Lstat report it whenever it exceeds
	// the mirror's size, so a delete that left it behind made the NEXT file at
	// this name inherit the DELETED file's size: copy a 10 GB file to X.mov →
	// rm X.mov → create a 100 KB X.mov → stat reports 10 GB → an mmap reader
	// (every NLE) faults past 100 KB and the kernel ZERO-FILLS to the reported
	// size. That is the #104 Premiere black-frame signature, reachable today.
	// Rename already carries/clears the mark (see Rename); Remove never did.
	jfs.handler.clearWriteSize(filename)
	jfs.handler.inPlaceHoles.forget(filename)

	// Publish delete event
	if e != nil {
		jfs.handler.publishEvent(metadata.MetadataEvent{
			Op: "delete", Path: filename, Inode: e.Inode,
		})
	}
	return nil
}

func (jfs *juiceFS) MkdirAll(dirname string, perm os.FileMode) error {
	dirname = strings.TrimPrefix(dirname, "/")
	return jfs.namespaceMutation([]string{dirname}, func(op *namespaceOperation) error {
		return jfs.mkdirAll(dirname, perm, op)
	})
}

func (jfs *juiceFS) mkdirAll(dirname string, perm os.FileMode, op *namespaceOperation) error {

	// Create on FUSE — but NOT when offline. A FUSE mkdir needs JuiceFS→Redis,
	// which is unreachable offline; the call hangs or fails and an offline
	// Finder folder copy aborts with "the device disappeared" (-36). Lazy
	// dir-creation (2026-06-14): offline we ONLY record the dir in the metadata
	// cache below (LocalOnly, so it's browsable immediately AND reconcile won't
	// prune it), and the drainer materializes it on FUSE — os.MkdirAll the
	// parent of each spooled file — when the files inside it drain after
	// reconnect. So an offline copy spools cleanly and the tree appears for the
	// user; the backend catches up online.
	if !pin.IsOffline() {
		// The namespace runner bounds the RPC, retaining the worker on timeout.
		err := os.MkdirAll(jfs.fullPath(dirname), perm)
		if err != nil {
			return err
		}
	}
	op.mutated = true

	// Insert into in-memory cache FIRST (instant visibility for NFS stats).
	// LocalOnly=true both flags "not yet on the backend" (the drainer/reconcile
	// clear it once it lands in Redis) and protects it from the reconcile prune
	// while it's only local — essential for offline-created dirs.
	//
	// REAL INODE AT BIRTH (#70 root cause, 2026-07-10): a dir minted with a
	// SYNTHETIC inode serves that as its NFS fileid; seconds later the keyspace
	// push reconciles the dir and swaps in JuiceFS's REAL inode — and the macOS
	// client, seeing the fileid CHANGE for a name whose handle it holds,
	// invalidates it → ESTALE on the very next op through that handle. During a
	// bundle copy (ditto/Finder) the parent-dir handles are held throughout, so
	// framework symlink creates died with "Stale NFS file handle" (reproduced:
	// mkdir → fileid 92233...860 → 4s later 1326119). Online the dir ALREADY
	// exists on FUSE here, so capture its real inode with one bounded Lstat —
	// the fileid then never changes. Synthetic remains the offline/Lstat-fail
	// fallback (offline dirs materialize at reconnect, when no copy holds them).
	now := time.Now()
	inode := uint64(0)
	if !pin.IsOffline() {
		if fi, ok := lstatWithTimeout(metrics.FUSESrcForeground, jfs.fullPath(dirname), fuseStatTimeout); ok && fi != nil {
			if st, sok := fi.Sys().(*syscall.Stat_t); sok && st.Ino != 0 {
				inode = st.Ino
			}
		}
	}
	if inode == 0 {
		inode = jfs.handler.nextSyntheticInode()
	}
	e := metadata.MakeEntry(dirname, true, 0, now, inode)
	e.LocalOnly = true
	op.checkpoint("publish", dirname)
	if err := jfs.handler.store.Insert(e); err != nil {
		return err
	}

	jfs.handler.publishEvent(metadata.MetadataEvent{
		Op: "create", Path: dirname, Mtime: now.Unix(),
		Inode: e.Inode, IsDir: true,
	})
	return nil
}

// Remaining billy.Filesystem stubs
func (jfs *juiceFS) Join(elem ...string) string { return path.Join(elem...) }
func (jfs *juiceFS) TempFile(dir, prefix string) (billy.File, error) {
	return nil, fmt.Errorf("not implemented")
}

// Symlink creates a symbolic link at `link` pointing at `target`, exposed over
// NFS via the registered SYMLINK procedure (internal/nfs/nfs_onsymlink.go).
//
// WHY THIS EXISTS (the real bug): macOS project bundles/packages — .fcpbundle,
// Premiere/Resolve project folders, .app, *.framework (Versions/Current), dylib
// libraries — contain INTERNAL symlinks. Finder copying any such bundle issues
// a SYMLINK RPC; while this returned "not supported" the WHOLE copy aborted
// with "you don't have permission to access some of the items" (and `ln -s`
// gave -5000 / EPERM). Plain-file copies have no symlinks, which is why every
// synthetic test false-greened. The underlying JuiceFS FUSE mount is a POSIX
// fs, so os.Symlink/os.Readlink/os.Lstat work directly against it.
//
// `target` is stored VERBATIM (it may be relative like "A" or
// "Versions/Current/Lib", or absolute) — passed straight through to
// os.Symlink, never resolved. A symlink carries no data, so it does NOT route
// through the write spool: we create it on FUSE and synchronously mirror it
// into the metadata cache (the same InsertToCache path Create uses) so the
// follow-up LOOKUP/GETATTR/READLINK and handle resolution see it immediately.
func (jfs *juiceFS) Symlink(target, link string) error {
	link = strings.TrimPrefix(link, "/")
	return jfs.namespaceMutation([]string{link}, func(op *namespaceOperation) error {
		return jfs.symlink(target, link, op)
	})
}

func (jfs *juiceFS) symlink(target, link string, op *namespaceOperation) error {

	fusePath := jfs.fullPath(link)

	// Create on FUSE — but NOT when offline. A FUSE symlink needs JuiceFS→Redis/
	// backend, which is unreachable offline; the call fails and the onSymlink
	// wire layer maps the error to NFSStatusAccess → Finder aborts the WHOLE
	// bundle copy with "you don't have permission to access some of the items".
	// That breaks copying ANY package with internal symlinks (.app, .framework,
	// .fcpbundle, Premiere/Resolve projects) while offline — a real offline-
	// ingest workflow. So offline we mirror MkdirAll's lazy-creation pattern:
	// DON'T os.Symlink; record the link in the metadata cache below (LocalOnly,
	// for instant NFS visibility AND reconcile-prune protection) and PERSIST
	// (link, target) so it survives an app restart AND the drainer materializes
	// it on FUSE at reconnect (see Drainer.materializePendingSymlinks). The copy
	// succeeds; the backend catches up online.
	offline := pin.IsOffline()
	if !offline {
		// The namespace runner bounds the RPC, retaining the worker on timeout.
		err := os.Symlink(target, fusePath)
		if err != nil {
			// os.Symlink wraps the syscall errno in *os.LinkError; the onSymlink
			// wire layer already pre-checks existence and maps to NFSStatusExist,
			// but map here too so a direct/raced EEXIST is reported faithfully
			// (onSymlink turns a generic error into NFSStatusAccess otherwise).
			if os.IsExist(err) {
				return os.ErrExist
			}
			return err
		}
		op.mutated = true
	} else if spool := jfs.handler.spool.Load(); spool != nil {
		// PERSIST the deferred link BEFORE inserting into the cache, so a crash
		// in the (instant) window between the two leaves a durable record rather
		// than a cache-only link that vanishes on restart with nothing to
		// materialize. UPSERT keyed on link_path: a re-created link just updates
		// the target. A nil spool means spool/offline-defer is disabled, so we
		// fall through to the in-memory mirror only (best-effort, non-durable) —
		// the same posture MkdirAll has with no spool wired.
		if err := spool.Meta().PutPendingSymlink(link, target); err != nil {
			return err
		}
		op.mutated = true
	}

	// Mirror the new symlink into the metadata cache so it is instantly
	// visible to LOOKUP/GETATTR/READDIR/handle-resolution — same pattern as
	// Create's InsertToCache + synthetic inode + async SQLite write. Online we
	// Lstat the freshly-created link (NOT Stat — Stat follows the link and a
	// dangling target would ENOENT) to capture its real size/mtime; the Entry's
	// Mode carries os.ModeSymlink so a cache-hit Lstat reports type=symlink and
	// NFS clients then issue READLINK. Lstat failure is non-fatal: the link IS
	// on FUSE, so fall back to a zero-size/now entry. Offline there is no on-FUSE
	// link to Lstat, so we use a zero-size/now entry directly (the link byte size
	// is cosmetic for NFS type classification — READLINK serves the real target).
	now := time.Now()
	var size int64
	inode := uint64(0)
	if !offline {
		if fi, err := os.Lstat(fusePath); err == nil {
			size = fi.Size()
			now = fi.ModTime()
			// REAL INODE AT BIRTH (#70): this Lstat already runs for size/mtime
			// — also take the real inode so the fileid never swaps under the
			// client when the push-reconcile later sees this link (the swap is
			// what ESTALE'd framework symlinks mid-bundle-copy; see MkdirAll).
			if st, sok := fi.Sys().(*syscall.Stat_t); sok && st.Ino != 0 {
				inode = st.Ino
			}
		}
	}
	if inode == 0 {
		inode = jfs.handler.nextSyntheticInode()
	}
	e := metadata.MakeEntry(link, false, size, now, inode)
	e.Mode = (e.Mode &^ os.ModeType) | os.ModeSymlink
	e.LocalOnly = true
	op.checkpoint("publish", link)
	if err := jfs.handler.store.Insert(e); err != nil {
		return err
	}

	jfs.handler.publishEvent(metadata.MetadataEvent{
		Op: "create", Path: link, Mtime: now.Unix(), Inode: e.Inode,
		// Carry the symlink type so a subscriber (self-write pub/sub or a peer
		// JuiceMount) re-applies this event as ModeSymlink, not a 0644 regular
		// file — otherwise applyEvent would downgrade the link we just minted.
		IsSymlink: true,
	})
	return nil
}

// Readlink returns the verbatim target stored in the symlink at `link`,
// serving the NFS READLINK procedure (internal/nfs/nfs_onreadlink.go). The
// target is returned exactly as stored — relative or absolute — never
// resolved. os.Readlink reports ENOENT/EINVAL, which onReadLink maps to the
// correct NFS status.
func (jfs *juiceFS) Readlink(link string) (string, error) {
	link = strings.TrimPrefix(link, "/")
	target, err := os.Readlink(jfs.fullPath(link))
	if err == nil {
		return target, nil
	}
	// Offline-defer fallback: a symlink created while offline was NOT written to
	// FUSE (juiceFS.Symlink defers it), so os.Readlink ENOENTs until the drainer
	// materializes it on reconnect. Serve the verbatim target from the pending-
	// symlink store so READLINK succeeds during an offline bundle copy (the
	// client issues READLINK right after the SYMLINK it just made). Only consult
	// the store on a not-exist error and only when a spool is wired — a genuine
	// EINVAL (not a symlink) or other error must still propagate so onReadLink
	// maps it faithfully.
	if spool := jfs.handler.spool.Load(); os.IsNotExist(err) && spool != nil {
		if t, gerr := spool.Meta().GetPendingSymlink(link); gerr == nil {
			return t, nil
		}
	}
	return target, err
}
func (jfs *juiceFS) Chroot(p string) (billy.Filesystem, error) {
	return nil, fmt.Errorf("not supported")
}
func (jfs *juiceFS) Root() string { return "/" }

// juiceChange implements billy.Change for write operations.
type juiceChange struct {
	handler *JuiceMountHandler
}

// deletedHandleChange accepts late metadata updates on an already-unlinked
// AppleDouble handle without recreating the deleted path in the live mirror.
type deletedHandleChange struct{}

func (deletedHandleChange) Chmod(string, os.FileMode) error            { return nil }
func (deletedHandleChange) Chown(string, int, int) error               { return nil }
func (deletedHandleChange) Lchown(string, int, int) error              { return nil }
func (deletedHandleChange) Chtimes(string, time.Time, time.Time) error { return nil }

// Chmod applies an NFS SETATTR{mode} so a copied file keeps its source perms
// (the read-only bit in particular: a `chmod 444` source that Finder copies
// must read back 0444, not 0644). Previously a no-op, which is why a read-only
// file became writable after a Finder copy.
//
// Two-part apply, both bounded (this is the write/SETATTR path, NOT the hot
// read path — no per-read FUSE here):
//
//  1. Best-effort os.Chmod on the FUSE file. Succeeds for a file already on
//     FUSE (in-place modify). For a spool-pending file (written+closed, not yet
//     drained to FUSE) the path doesn't exist yet, so this ENOENTs — tolerated,
//     not fatal: the cached/persisted Mode below is the authority NFS GETATTR
//     serves, and that's what the client reads back.
//  2. Persist the perm bits to the metadata store (cache + SQLite) via
//     UpdateMode. Type bits are preserved there, so a stray mode on a directory
//     can never flip it to a regular file. This is the durable fix — it
//     survives the os.Create(0644) the drainer uses when it lands the spool
//     file on FUSE, because GETATTR reads the store, not a fresh FUSE stat.
//
// Spool/drain untouched: we never open, truncate, or re-create the spool entry;
// the FUSE chmod is best-effort and the store update is metadata-only.
func (jc *juiceChange) Chmod(name string, mode os.FileMode) error {
	h := jc.handler
	rel := strings.TrimPrefix(name, "/")
	type chmodResult struct {
		observed, spoolChecked, spoolPending, backendAttempted, backendApplied bool
		backendErrno                                                           int
		errnoKnown                                                             bool
		backendError                                                           string
	}
	result, err := runNamespaceOperation(h, []string{rel}, func(op *namespaceOperation) (chmodResult, error) {
		// Admission precedes this metadata write. During a legacy FULL hold,
		// UpdateMode refuses before any backend chmod can touch the path.
		if err := h.store.UpdateMode(rel, mode); err != nil {
			if errors.Is(err, metadata.ErrMirrorConflict) {
				return chmodResult{}, errFUSETimeout
			}
			var pending *metadata.PendingDrainCompletion
			if errors.As(err, &pending) {
				return chmodResult{}, errFUSETimeout
			}
			return chmodResult{}, err
		}
		result := chmodResult{observed: true}
		backendEligible := !pin.IsOffline() && h.fusePath != ""
		if backendEligible {
			spool := h.spool.Load()
			result.spoolChecked = spool != nil
			result.spoolPending = spool != nil && spool.HasPending(rel)
		}
		if backendEligible && !result.spoolPending {
			fusePath := path.Join(h.fusePath, rel)
			// Run the raw syscall in this worker. On RPC timeout the worker
			// retains the namespace lease until the kernel call really ends.
			start := time.Now()
			gateTimer := time.NewTimer(mutationOpTimeout())
			gateWait, depth, acquired := acquireFUSEGate(nfsLstatGate, gateTimer)
			gateTimer.Stop()
			if !acquired {
				metrics.Default().ObserveFUSECall(metrics.FUSESrcForeground, metrics.FUSEOpChmod, metrics.FUSEGateNFSLstat,
					0, gateWait, time.Since(start), metrics.FUSEOutcomeGateTimeout)
				return result, errFUSETimeout
			}
			var trace *metadataMutationInterval
			if h.metadataTrace != nil {
				trace = h.metadataTrace.beginChmod(rel, mode.Perm(), result.spoolChecked, result.spoolPending)
				if trace != nil {
					trace.before(h.fusePath, nil)
				}
			}
			result.backendAttempted = true
			chmodErr := os.Chmod(fusePath, mode.Perm())
			if trace != nil {
				trace.after(h.fusePath, nil, chmodErr)
			}
			result.backendApplied = chmodErr == nil
			if chmodErr == nil {
				result.errnoKnown = true
			} else {
				var errno syscall.Errno
				if errors.As(chmodErr, &errno) {
					result.errnoKnown = true
					result.backendErrno = int(errno)
				} else {
					result.backendError = chmodErr.Error()
				}
			}
			<-nfsLstatGate
			metrics.Default().ObserveFUSECall(metrics.FUSESrcForeground, metrics.FUSEOpChmod, metrics.FUSEGateNFSLstat,
				depth, gateWait, time.Since(start), metrics.FUSEOutcomeOK)
			if err := chmodErr; err != nil && !os.IsNotExist(err) {
				jmlog.Debug("Chmod: FUSE chmod failed (metadata mode remains authority)", "path", rel, "err", err)
			}
		}
		return result, nil
	}, nil)
	// A joined result is known only after the namespace worker has finished.
	// A timed-out worker retains its lease; its later result is not inferred.
	if result.observed && strings.HasPrefix(path.Base(rel), "._") {
		var backendErrno any
		if result.errnoKnown {
			backendErrno = result.backendErrno
		}
		jmlog.Info("nfs: sidecar chmod result", "path", rel,
			"mode", fmt.Sprintf("%#o", mode.Perm()),
			"spool_checked", result.spoolChecked,
			"spool_pending", result.spoolPending,
			"backend_attempted", result.backendAttempted,
			"backend_applied", result.backendApplied,
			"backend_errno", backendErrno, "backend_error", result.backendError)
	}
	return err
}
func (jc *juiceChange) Chown(name string, uid, gid int) error             { return nil }
func (jc *juiceChange) Lchown(name string, uid, gid int) error            { return nil }
func (jc *juiceChange) Chtimes(name string, atime, mtime time.Time) error { return nil }

// cachedFile implements billy.File with two-tier read path:
// 1. Direct SSD pread (bypasses FUSE) — if cache reader is available and block is cached
// 2. JuiceFS FUSE pread (fallback) — populates SSD cache for future reads
const (
	// fuseReadMaxRetries bounds how many times cachedFile.ReadAt re-attempts a
	// zero-progress transient FUSE/MinIO read error before surfacing it. 4
	// attempts with the staged backoff below totals ~0.5s of sleeps — tiny
	// against the ~40s soft-mount per-RPC budget (timeo=400) — and converted
	// the observed ~2% concurrent-cold-read failure rate to zero in validation.
	fuseReadMaxRetries = 4
	// fuseReadRetryBackoff is multiplied by the (1-based) attempt number, so
	// the sleeps are 50,100,150,200 ms. Short enough to stay well inside the
	// kernel per-RPC window, long enough to let a contended MinIO fetch / FUSE
	// loader slot free up between tries.
	fuseReadRetryBackoff = 50 * time.Millisecond

	// offlineLocalReadTimeout bounds an OFFLINE un-pinned FUSE read so a
	// locally-cached block is served while a read that would need an S3/MinIO
	// fetch is refused fast (the offline tarpit guarantee). A JuiceFS LRU hit
	// returns in well under this; an S3 GET on a slow link is 30s+, so this
	// cleanly separates "served from local cache" from "needs the backend".
	offlineLocalReadTimeout = 1500 * time.Millisecond

	// offlinePinnedReadTimeout is the same bound for PINNED files (#43,
	// 2026-06-22). Pinned files are SUPPOSED to be resident, so we wait longer
	// before concluding a block was evicted — this avoids false-refusing a
	// slow-but-local read under heavy concurrent load, while still cleanly
	// separating a local hit (ms) from an S3 fetch (30s+). The old code trusted
	// the pin "Ready" flag and let pinned reads fall through UNBOUNDED; after
	// LRU eviction that flag goes stale and the unbounded read tarpitted on S3
	// (offline) or returned empties — the root cause of "offline access to a
	// pinned reel broke."
	offlinePinnedReadTimeout = 4 * time.Second
)

// fuseReadTimeout bounds one foreground FUSE pread. The NFS layer subdivides
// large requests into 256 KiB reads, so this is a per-subread ceiling rather
// than a whole-file deadline. It must stay comfortably below macOS's ~40 s
// soft-mount retry window: an interrupted Finder copy previously left a dozen
// preads blocked for 24-141 s, consuming RPC and FUSE-data slots long after the
// client cancelled. A far/slow link gets a larger healthy-read allowance while
// still failing a true wedge early enough for JUKEBOX retry.
func fuseReadTimeout() time.Duration {
	if v := os.Getenv("JM_FUSE_READ_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Millisecond
		}
	}
	p := netprofile.Default()
	return fuseReadTimeoutFor(p.Class(), p.HighLatency())
}

func fuseReadTimeoutFor(class netprofile.LinkClass, highLatency bool) time.Duration {
	if highLatency || class == netprofile.ClassMetered || class == netprofile.ClassSlow {
		return 8 * time.Second
	}
	return 4 * time.Second
}

// readAtBounded runs fd.ReadAt in a goroutine and returns (n, err, true) if it
// finishes within timeout, or (0, nil, false) on timeout. MUST be given a
// PRIVATE buffer: on timeout the goroutine keeps running and writes to buf, so
// buf must NOT be the caller's reusable/pooled slice (else a late write
// corrupts a subsequent RPC). Used by the offline read path to probe whether
// an un-pinned block is locally servable without tarpitting on a backend fetch.
func readAtBounded(fd *os.File, buf []byte, off int64, timeout time.Duration) (int, error, bool) {
	return readAtCallBounded(fd.ReadAt, buf, off, timeout, nil)
}

// readAtCallBounded is readAtBounded's testable core. afterReturn runs in the
// syscall goroutine, even when the caller has already timed out. The online
// FUSE path uses it to retain its data-gate slot until the abandoned pread
// ACTUALLY returns; releasing at the deadline would let every retry start one
// more wedged syscall and defeat the concurrency ceiling.
func readAtCallBounded(read func([]byte, int64) (int, error), buf []byte, off int64, timeout time.Duration, afterReturn func()) (int, error, bool) {
	type res struct {
		n   int
		err error
	}
	ch := make(chan res, 1)
	go func() {
		n, err := read(buf, off)
		if afterReturn != nil {
			afterReturn()
		}
		ch <- res{n, err}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case r := <-ch:
		return r.n, r.err, true
	case <-timer.C:
		return 0, nil, false
	}
}

type cachedFile struct {
	snapshotProof         *sidecarSnapshotHandle // immutable read-only body, attrs and terminal size
	sidecarProof          *sidecarReadProof      // immutable body pinned through the entire READ RPC
	sourceProofMu         sync.Mutex
	sourceProof           *sidecarReadProof // authoritative size observation, captured before a FUSE body read
	completedSourceProof  *sidecarReadProof // first complete FUSE read; protected by sourceProofMu
	name                  string
	fuseFD                *os.File
	fusePath              string
	fdPool                *FDPool
	handler               *JuiceMountHandler // for clampWriteSize on Truncate
	cacheReader           *cache.Reader
	cacheIdentity         metadata.CachedFileIdentity
	cacheIdentityConflict bool
	readahead             *ReadaheadManager
	memBuf                *MemoryBuffer
	inode                 uint64
	fileSize              int64
	fdMu                  sync.Mutex // guards lazy fuseFD acquisition and Close
	closed                bool

	// Decided at OpenFile time: whether this file passed the pin check.
	// Pinned files are allowed to fall through to FUSE during offline mode
	// (JuiceFS serves from its local LRU; no backend round-trip).
	// Un-pinned files never reach this struct in offline mode (open-time
	// gate refuses them before we construct the cachedFile).
	pinned        bool
	pinGeneration string // retained owned-authority generation; no FUSE fallback

	// QA-31 (2026-05-25): VALUE snapshot of the file's metadata at Open
	// time. Exposed via CachedInfo() to satisfy
	// internal/nfs.CachedInfoProvider — onRead uses this for the size-
	// clamp and the post-op attrs of every READ RPC, eliminating two
	// FUSE Stat round-trips per RPC.
	//
	// IMPORTANT (QA-31 code-review HIGH-1): this is a VALUE-COPY of the
	// metadata.Entry's scalars, NOT a pointer wrapper around the live
	// Entry. The live Entry can be mutated in-place by concurrent paths
	// (e.g. UpdateSize on writeback) under the Store's mu.Lock with no
	// synchronization between the writer and a reader going through
	// FileInfo.Size(). Holding a value snapshot eliminates the race
	// entirely on the cachedFile side; staleness was already acceptable
	// per NFS post-op-attrs semantics (advisory; clients revalidate via
	// GETATTR).
	cachedInfo *snapshotFileInfo
}

// fuseReadRetrySource is intentionally a method interface, not a per-read
// closure: the production cachedFile and a deterministic test double can both
// exercise the same retry loop without a per-read closure.
type fuseReadRetrySource interface {
	boundedFUSEReadAt([]byte, int64) (int, error)
}

func prematureFUSEEOF(n int, err error, off, fileSize int64) bool {
	return fileSize > 0 && errors.Is(err, io.EOF) && off+int64(n) < fileSize
}

func shouldRetryColdFUSERead(n int, err error, off, fileSize int64) bool {
	zeroEIO := err != nil && n == 0 && !errors.Is(err, io.EOF) &&
		!errors.Is(err, errFUSETimeout) && !pin.IsOfflineNotAvailable(err)
	return zeroEIO || (n == 0 && prematureFUSEEOF(n, err, off, fileSize))
}

func retryColdFUSERead(source fuseReadRetrySource, p []byte, off, fileSize int64, name string, n int, err error) (int, error) {
	// A timed-out pread still owns its private buffer and data-gate slot. No
	// retry may overlap that unknown syscall, including after earlier EIO/EOF.
	if !shouldRetryColdFUSERead(n, err, off, fileSize) {
		return n, err
	}
	attempts := 0
	for attempt := 1; attempt <= fuseReadMaxRetries; attempt++ {
		time.Sleep(fuseReadRetryBackoff * time.Duration(attempt))
		metrics.Default().IncReadRetry()
		attempts = attempt
		n, err = source.boundedFUSEReadAt(p, off)
		if errors.Is(err, errFUSETimeout) || pin.IsOfflineNotAvailable(err) {
			break // unknown in-flight pread, or offline; never launch another
		}
		if n > 0 || err == nil {
			jmlog.Debug("FUSE read recovered on retry", "path", name, "off", off, "attempt", attempt)
			break
		}
		if errors.Is(err, io.EOF) && !prematureFUSEEOF(n, err, off, fileSize) {
			break // genuine EOF at/after fileSize
		}
	}
	if err != nil && n == 0 && !errors.Is(err, io.EOF) &&
		!errors.Is(err, errFUSETimeout) && !pin.IsOfflineNotAvailable(err) {
		metrics.Default().IncReadFail()
		jmlog.Warn("cold FUSE read failed after retry; surfacing error to client",
			"path", name, "off", off, "attempts", attempts, "err", err)
	}
	return n, err
}

// ensureFuseFD lazily acquires the pooled JuiceFS descriptor only after the
// direct SSD and RAM serving paths miss. This is the seam that keeps a verified
// cache hit from paying remote metadata merely to construct cachedFile.
func (f *cachedFile) ensureFuseFD() error {
	if err := f.handler.cacheBackendError(); err != nil {
		return err
	}
	f.fdMu.Lock()
	defer f.fdMu.Unlock()
	if f.closed {
		return os.ErrClosed
	}
	if f.fuseFD != nil {
		return nil
	}
	if f.fdPool == nil {
		fd, err, ok := openFileWithTimeout(metrics.FUSESrcForeground, f.fusePath, os.O_RDONLY, 0, readOpenTimeout())
		if !ok {
			return errFUSETimeout
		}
		if err != nil {
			return err
		}
		f.fuseFD = fd
		return nil
	}
	fd, err, ok := pooledReadOpenWithTimeout(metrics.FUSESrcForeground, f.fdPool, f.fusePath, readOpenTimeout())
	if !ok {
		return errFUSETimeout
	}
	if err != nil {
		return err
	}
	f.fuseFD = fd
	return nil
}

// boundedFUSEReadAt performs one online FUSE pread with both the global data
// ceiling and a wall-clock deadline. The caller buffer is never handed to a
// goroutine that may outlive the RPC: a timed-out pread retains its private
// buffer and its data-gate slot until the kernel call really returns.
func boundedFUSEFileReadAt(fd *os.File, p []byte, off int64, bound time.Duration) (int, error) {
	releaseData, dataOK := acquireFUSEData()
	if !dataOK {
		noteFUSEDataRefused()
		return 0, errFUSETimeout
	}
	tmp := offlineReadBuf(len(p))
	n, err, done := readAtCallBounded(fd.ReadAt, tmp, off, bound, releaseData)
	if !done {
		return 0, errFUSETimeout
	}
	if n > 0 {
		copy(p, tmp[:n])
	}
	releaseOfflineReadBuf(tmp, true)
	return n, err
}

func (f *cachedFile) boundedFUSEReadAt(p []byte, off int64) (int, error) {
	bound := fuseReadTimeout()
	n, err := boundedFUSEFileReadAt(f.fuseFD, p, off, bound)
	if errors.Is(err, errFUSETimeout) {
		jmlog.Warn("FUSE pread timed out or saturated; returning retryable JUKEBOX",
			"path", f.name, "offset", off, "len", len(p), "timeout_ms", bound.Milliseconds())
	}
	return n, err
}

// snapshotFileInfo is a frozen-at-construction os.FileInfo for cachedFile.
// All fields are copied by value at Open time; no aliasing of the live
// metadata.Entry.
type snapshotFileInfo struct {
	name  string
	size  int64
	mode  os.FileMode
	mtime time.Time
	isDir bool
	inode uint64
}

func (s *snapshotFileInfo) Name() string       { return s.name }
func (s *snapshotFileInfo) Size() int64        { return s.size }
func (s *snapshotFileInfo) Mode() os.FileMode  { return s.mode }
func (s *snapshotFileInfo) ModTime() time.Time { return s.mtime }
func (s *snapshotFileInfo) IsDir() bool        { return s.isDir }
func (s *snapshotFileInfo) Sys() any {
	return &syscall.Stat_t{
		Ino:   s.inode,
		Uid:   snapshotUID,
		Gid:   snapshotGID,
		Nlink: 1,
	}
}

// snapshotUID/snapshotGID mirror metadata/types.go's currentUID/currentGID
// so the Sys() result matches what GETATTR returns through the metadata
// path. Initialized once at process start.
var (
	snapshotUID = uint32(os.Getuid())
	snapshotGID = uint32(os.Getgid())
)

// CachedInfo implements internal/nfs.CachedInfoProvider. Returns the
// file's metadata as observed at Open time (immutable value snapshot;
// safe to read concurrently with mutations on the source Entry).
func (f *cachedFile) CachedInfo() os.FileInfo {
	if f.handler != nil && f.handler.testReadCachedInfo != nil {
		f.handler.testReadCachedInfo(f.name)
	}
	if f.cachedInfo == nil {
		return nil
	}
	return f.cachedInfo
}

// AuthoritativeSize never substitutes mirror metadata after an open/stat
// failure: a stale-low mirror would turn that failure into a truncated READ.
func (f *cachedFile) AuthoritativeSize() (int64, error) {
	if f.pinGeneration != "" {
		return f.ownedPinSize()
	}
	f.sourceProofMu.Lock()
	f.sourceProof = nil
	f.sourceProofMu.Unlock()
	proof := f.handler.beginStrongSidecarRead(f.name)
	if err := f.ensureFuseFD(); err != nil {
		return 0, err
	}
	info, err := boundedDescriptorStat(f.fuseFD)
	if err != nil {
		return 0, err
	}
	if sidecarInfoMatches(proof, info) {
		f.sourceProofMu.Lock()
		f.sourceProof = proof
		f.sourceProofMu.Unlock()
	}
	return info.Size(), nil
}

// LiveSize preserves the optional legacy interface for callers outside READ.
func (f *cachedFile) LiveSize() (int64, bool) {
	size, err := f.AuthoritativeSize()
	return size, err == nil
}

func boundedDescriptorStat(fd *os.File) (os.FileInfo, error) {
	return boundedDescriptorStatFromSource(metrics.FUSESrcForeground, fd, fuseStatTimeout)
}

func boundedDescriptorStatFromSource(src metrics.FUSESource, fd *os.File, bound time.Duration) (os.FileInfo, error) {
	gate, gateLabel := fuseFstatGate, metrics.FUSEGateFstat
	if src == metrics.FUSESrcSidecarWarm {
		gate, gateLabel = fuseGateForSource(src)
	}
	start := time.Now()
	timer := time.NewTimer(bound)
	defer timer.Stop()
	gateWait, depth, acquired := acquireFUSEGate(gate, timer)
	if !acquired {
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpFstat, gateLabel, 0, gateWait, time.Since(start), metrics.FUSEOutcomeGateTimeout)
		return nil, errFUSETimeout
	}
	type result struct {
		info os.FileInfo
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		fi, err := fd.Stat()
		ch <- result{fi, err}
		<-gate
	}()
	select {
	case r := <-ch:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpFstat, gateLabel, depth, gateWait, time.Since(start), metrics.FUSEOutcomeOK)
		return r.info, r.err
	case <-timer.C:
		metrics.Default().ObserveFUSECall(src, metrics.FUSEOpFstat, gateLabel, depth, gateWait, time.Since(start), metrics.FUSEOutcomeTimeout)
		return nil, errFUSETimeout
	}
}

func (f *cachedFile) Name() string { return f.name }

// cacheReaderServeEnabled gates the Priority-2 direct-SSD-cache serving read.
// DEFAULT ON after the RC repair. The previous implementation was disabled
// after measured silent torn reads because it used raw/overlapping Redis slice
// records, guessed every block was 4 MiB, crossed slice/block boundaries,
// ignored JuiceFS's checksum trailer, and pooled descriptors across eviction.
//
// cache.Reader now mirrors JuiceFS 1.3's newest-wins slice overlay and exact
// cache filename layout, clamps every segment, opens immutable files per read,
// verifies CsExtend CRC32C for every touched 32 KiB range, returns all-or-
// nothing, and invalidates mappings on local/remote mutation plus reconcile.
// JM_ENABLE_CACHE_READER=0 remains the emergency kill switch: every miss or
// rejected block falls through to coherent FUSE without surfacing partial data.
var cacheReaderServeEnabled = os.Getenv("JM_ENABLE_CACHE_READER") != "0"

// memBufServeEnabled gates serving reads from the in-RAM small-file buffer.
// DISABLED by default (2026-07-05): the RAM buffer can cache a file that it
// loaded during a transient partial-size window (the #85 stale/truncated
// GETATTR-size window while a fresh write is still draining) — it reads only
// the partial length, caches it AS COMPLETE, and never re-validates. It then
// serves that truncated image from RAM until the process restarts (RAM is
// cleared), so an image that finished draining correctly still renders as a
// BLACK FRAME in Premiere until JuiceMount is relaunched — "exclusively image
// media, fixed by a restart" (RC field report). Video bypasses membuf (over
// the size threshold), which is why only images were affected. Same class of
// stale-cache bug that already keeps the SSD block cache (Priority 2) OFF; the
// coherent FUSE path (Priority 3) is correct AND fast for small files, so
// disabling this "costs almost nothing." Re-enable only after membuf gains
// size-revalidation + invalidation-on-content-change.
var memBufServeEnabled = os.Getenv("JM_ENABLE_MEMBUF_SERVE") == "1"

// coldSubreadDur is the wall-clock threshold that classifies a Priority-3 FUSE
// subread as COLD (a real backend/MinIO GET over the link) vs WARM (an SSD
// block-cache hit). It MIRRORS internal/netprofile.minThroughputDur (3ms) — the
// SAME discriminator ObserveThroughput uses to decide whether a read sample
// reflects the wire or the SSD cache. Reusing that threshold keeps the
// read_cold_subread / read_warm_subread counters consistent with the link
// estimator, and — crucially (QA-35) — the signal is the read's OWN measured
// duration (already captured as readStart for ObserveThroughput), so classifying
// it adds NO syscall and NO lock, just one atomic increment.
const coldSubreadDur = 3 * time.Millisecond

// IncompleteAt implements internal/nfs.incompleteReader so onRead's size-clamp
// branch (nfs_onread.go, the off >= size case) can tell "genuine EOF" from "the
// bytes have not arrived yet".
//
// Without this, a client whose GETATTR raced ahead of the writes reads past the
// size it was told, gets Count=0, and treats a partially-arrived file as
// COMPLETE. ReadAt above covers off < size; this covers off >= size. The spool
// path carries both halves for the same reason.
func (f *cachedFile) IncompleteAt(off int64) bool {
	if f.handler == nil {
		return false
	}
	plan, tracked := f.handler.inPlaceHoles.inPlaceReadPlan(f.name, off)
	held := tracked && plan == planHold
	if held {
		f.handler.inPlaceHoles.logHold(f.name, off)
	}
	return held
}

func (h *JuiceMountHandler) beginReadAdmission(name string) (*readAdmission, error) {
	if h == nil {
		return nil, nil
	}
	r, err := h.namespace.admitRead(name)
	if err == nil && h.testReadAfterAdmission != nil {
		h.testReadAfterAdmission(name)
	}
	return r, err
}

func (f *cachedFile) BeginReadAdmission() (func() error, func(), error) {
	r, err := f.handler.beginReadAdmission(f.name)
	if err != nil {
		return nil, nil, err
	}
	check := func() error {
		if r != nil {
			if err := r.check(); err != nil {
				return err
			}
		}
		return f.checkSidecarProof()
	}
	var release func()
	if r != nil {
		release = r.release
	}
	return check, release, nil
}

func (f *cachedFile) ReadAt(p []byte, off int64) (int, error) {
	var trace cachedReadTrace
	return f.readAtTraced(p, off, &trace)
}

func (f *cachedFile) readAtTraced(p []byte, off int64, trace *cachedReadTrace) (int, error) {
	start := time.Now()
	if trace != nil {
		*trace = cachedReadTrace{FinalRoute: "not_attempted", Reader: cache.ReadTrace{Outcome: "not_attempted"}}
	}
	r, err := f.handler.beginReadAdmission(f.name)
	if err != nil {
		if trace != nil {
			trace.finish(start, 0, err)
		}
		return 0, err
	}
	n, readErr := f.readAtUnprotectedTraced(p, off, trace)
	if r != nil {
		r.finishBytes(p, &n, &readErr)
	}
	if trace != nil {
		trace.finish(start, n, readErr)
		if trace.logEligible && trace.TotalNS > int64(200*time.Millisecond) {
			jmlog.Warn("slow cached read", "path", f.name, "offset", off, "len", len(p),
				"elapsed_ms", time.Duration(trace.TotalNS).Milliseconds(),
				"cache_path_initially_eligible", trace.CachePathInitiallyEligible,
				"qualification_ns", trace.QualificationNS, "cache_read_ns", trace.CacheReadNS,
				"mapping_lookup_ns", trace.Reader.MappingLookupNS,
				"mapping_lookup_count", trace.Reader.MappingLookupCount,
				"mapping_remote_or_shared_count", trace.Reader.MappingRemoteSharedCount,
				"verified_block_ns", trace.Reader.VerifiedBlockNS, "cache_outcome", trace.Reader.Outcome,
				"descriptor_acquire_ns", trace.DescriptorAcquireNS,
				"fuse_read_ns", trace.FUSEReadNS, "retry_ns", trace.RetryNS,
				"unaccounted_ns", trace.UnaccountedNS, "total_ns", trace.TotalNS,
				"final_route", trace.FinalRoute, "returned_bytes", trace.ReturnedBytes,
				"final_result_class", trace.FinalResultClass)
		}
	}
	return n, readErr
}

func (f *cachedFile) readAtUnprotected(p []byte, off int64) (int, error) {
	return f.readAtUnprotectedTraced(p, off, nil)
}

func (f *cachedFile) readAtUnprotectedTraced(p []byte, off int64, trace *cachedReadTrace) (int, error) {
	if f.cacheIdentityConflict {
		return 0, syscall.EAGAIN
	}
	if f.handler != nil {
		if err := f.handler.namespace.servingError(f.name); err != nil {
			return 0, err
		}
		if f.handler.testReadAfterServingCheck != nil {
			f.handler.testReadAfterServingCheck(f.name)
		}
	}
	if trace != nil {
		trace.logEligible = true
	}
	// Priority 0: NEVER serve an unwritten region of an in-flight file.
	//
	// This wraps every serving priority below, for the same reason planReadAt
	// puts its punched check first: a hole is below every cache's notion of
	// valid content, so any path that consults a cache before the hole check
	// serves zeros as file content. The guard classifies the whole range (a
	// read starting in the valid prefix is cut at the hole, B-9) and rechecks
	// after the read. See inplace_contig.go for the defect and the two traps.
	//
	// Cost when no file anywhere has a hole — which is the normal state, and
	// includes every ordinary sequential copy — is one atomic load per check.
	return f.handler.guardInPlaceRead(f.name, p, off, func(q []byte) (int, error) {
		return f.readAtRouted(q, off, trace)
	})
}

// readAtRouted serves a read already admitted by guardInPlaceRead. It must not
// be called from anywhere else: every route below trusts that p does not reach
// into a tracked in-place hole.
func (f *cachedFile) readAtRouted(p []byte, off int64, trace *cachedReadTrace) (int, error) {
	if f.pinGeneration != "" {
		return f.readOwnedPinResident(p, off, trace)
	}

	// A pinned complete-size proof cannot fall back after a size clamp: doing
	// so could silently truncate a successor to the old body's length.
	if f.snapshotProof != nil {
		if trace != nil {
			trace.FinalRoute = "snapshot_proof"
		}
		return f.handler.readSidecarSnapshot(f.snapshotProof, p, off)
	}
	if f.sidecarProof != nil {
		if trace != nil {
			trace.FinalRoute = "sidecar_proof"
		}
		return f.handler.readStrongSidecar(f.sidecarProof, p, off)
	}

	// Priority 1: Memory buffer (zero-syscall, for small files like .prproj, LUTs)
	if memBufServeEnabled && f.memBuf != nil {
		n, hit := f.memBuf.ReadAt(f.name, p, off, f.fileSize, f.fusePath)
		if hit {
			if trace != nil {
				trace.FinalRoute = "memory_buffer"
			}
			if f.readahead != nil {
				f.readahead.OnRead(f.inode, off, n, f.name)
			}
			metrics.Default().AddBytesRead(int64(n))
			// [JM6] Surface EOF when the buffered file ran out under us.
			// memBuf returns (0, true) when off is past the buffered
			// length; callers iterating in a short-read loop (e.g. the
			// new subdivided onRead path) would otherwise re-issue at
			// the same offset forever because they can't distinguish
			// "end of file" from "transient zero-byte response". This
			// path is reached for small files <= 32 KiB that bypass the
			// upstream size-clamp in nfs_onread.go.
			if n == 0 {
				return 0, io.EOF
			}
			return n, nil
		}
	}

	if proof := f.upgradeLegacySidecar(off, len(p)); proof != nil {
		if n, err, handled := f.readLegacySidecarUpgrade(proof, p); handled {
			if trace != nil {
				trace.FinalRoute = "sidecar_upgrade"
			}
			return n, err
		}
	}

	// Priority 2: complete `._` AppleDouble/.DS_Store RAM cache (navigation
	// crux). Keep this ahead of the direct SSD reader: the latter needs a Redis
	// LRANGE the first time it sees each inode. Paying that remote metadata RTT
	// before consulting an already-validated 4 KiB RAM entry turned Finder's
	// per-file xattr crawl into a serial cellular stall even on repeat visits.
	if f.handler != nil {
		if sn, ok := f.handler.sidecarServe(f.name, p, off); ok {
			if trace != nil {
				trace.FinalRoute = "sidecar_ram"
			}
			if sn == 0 {
				return 0, io.EOF
			}
			return sn, nil
		}
	}

	// Priority 2.5: checksum-verified direct SSD cache read (bypasses FUSE and
	// its remote metadata round trips). Any miss or integrity refusal falls
	// through to the coherent FUSE path below; see cacheReaderServeEnabled.
	if f.cacheReader != nil && cacheReaderServeEnabled {
		// An earlier authoritative size check may have captured this exact
		// descriptor's identity. Only a complete selected-volume-bound SSD
		// read can finish that existing proof; snapshot before cache I/O so
		// a concurrent writer or replacement cannot supply new authority.
		cacheProof := f.cachedSidecarSourceProof(off, len(p))
		// A mapping already in RAM never observes this deadline. A first read
		// needs one LRANGE; bound it so an unreachable Redis cannot park an NFS
		// connection for the client's full soft-mount timeout. Far links get room
		// for the verified direct→encrypted handoff; explicit offline mode only
		// admits mappings we already have locally.
		mappingTimeout := 500 * time.Millisecond
		if netprofile.Default().HighLatency() {
			mappingTimeout = 4 * time.Second
		}
		if pin.IsOffline() {
			mappingTimeout = 200 * time.Millisecond
		}
		n, err := f.handler.readCacheBlockTraced(context.Background(), f.cacheReader, f.cacheIdentity, f.inode, off, p, mappingTimeout, trace)
		if errors.Is(err, syscall.EAGAIN) {
			return 0, err
		}
		if errors.Is(err, cache.ErrVolumeBinding) {
			// A positive identity refusal cannot authorize another backend read,
			// including the bounded FUSE fallback used while offline.
			return 0, fmt.Errorf("%w: %w", syscall.EIO, err)
		}
		if err == nil && n > 0 {
			if trace != nil {
				trace.FinalRoute = "verified_ssd"
			}
			metrics.Default().ObserveDirectSSDCacheHit(int64(n))
			// Preserve complete local bytes for a later positioned rewrite.
			// Publication additionally checks the descriptor and current writer,
			// mirror, and volume identities; weak or partial SSD reads stay weak.
			if f.handler != nil && off == 0 {
				f.handler.sidecarMaybePopulate(f.name, off, p[:n], f.fileSize)
				if cacheProof != nil && n == len(p) {
					f.publishSidecarSourceProof(cacheProof, p[:n])
				}
			}
			// Preserve the sequential-read signal across a mixed local/cold run.
			// OnRead prefetches strictly AFTER this range, so it never asks FUSE
			// for the bytes this verified SSD hit just served. Without this call,
			// alternating cache hits and cold blocks reset the WAN detector and
			// serialize every miss across the Link.
			if f.readahead != nil {
				f.readahead.OnRead(f.inode, off, n, f.name)
			}
			metrics.Default().AddBytesRead(int64(n))
			return n, nil
		}
		metrics.Default().IncDirectSSDCacheMiss()
	}

	if err := f.handler.cacheBackendError(); err != nil {
		return 0, err
	}

	// Real-cellular acceptance guard: verified RAM/sidecar/direct-SSD cache
	// hits above remain available, but a miss may not enter FUSE. This is a
	// deliberate zero-byte budget rather than request-byte accounting: JuiceFS
	// can amplify one small pread into a full object-store block GET.
	if err := refuseFUSEContentRead(f.handler != nil && f.handler.testRefuseFUSEContentReads); err != nil {
		return 0, err
	}

	// Offline mode short-circuit: if the user has flipped to offline, we don't
	// fall through to an UNBOUNDED FUSE read. JuiceFS would otherwise try to GET
	// a missing block from S3, which on cellular can take 30+ seconds and
	// tarpits the NLE waiting for the read. We bound the read instead: a block
	// in JuiceFS's local LRU returns fast (served); a block that would need the
	// backend blocks past the bound → refuse with ErrOfflineNotAvailable so the
	// NLE shows "media offline" instead of beachballing.
	//
	// PINNED files are NOT exempt (#43, 2026-06-22). The old code trusted the
	// pin "Ready" flag as proof the bytes were in JuiceFS's LRU and let pinned
	// reads fall through unbounded. But that flag goes STALE after LRU eviction
	// (cache pressure, /cache-clear, watchdog remount): the file is still marked
	// Ready while its blocks are gone. Offline, that produced a silent tarpit /
	// empty read — the root cause of "offline access to a pinned reel broke."
	// We bound pinned reads too, just with a more generous timeout (they're
	// supposed to be resident, so we wait longer before concluding "evicted" to
	// avoid false-refusing a slow-but-local read under load). A genuinely-cached
	// pinned block still returns in well under the bound → served, no regression.
	if pin.IsOffline() {
		if trace != nil {
			trace.FinalRoute = "fuse_offline"
		}
		if err := f.ensureFuseFDTraced(trace); err != nil {
			return 0, pin.ErrOfflineNotAvailable
		}
		// The bytes MIGHT be local — JuiceFS's own LRU has them (recently
		// read/written/just-copied/pinned-and-warm), or the user toggled offline
		// while the network is actually up (Redis/JuiceFS reachable, just don't
		// want S3 traffic). Don't refuse blindly (the old un-pinned behavior,
		// which made a just-copied-then-drained file unreadable offline —
		// 2026-06-14 offline-ingest sprint). Attempt the FUSE read with a bound:
		// a local hit returns within it → serve; a read that would need an
		// S3/MinIO fetch blocks past it → THEN refuse with ErrOfflineNotAvailable,
		// preserving the tarpit-avoidance offline mode exists for. A PRIVATE
		// buffer is used so a bound-exceeded read's still-running goroutine can
		// never scribble into the caller's pooled p.
		bound := offlineLocalReadTimeout
		if f.pinned {
			bound = offlinePinnedReadTimeout
		}
		// The private buffer is REQUIRED (see above) but need not be fresh:
		// `make([]byte, len(p))` here cost a zeroed 1 MiB allocation per read on
		// the path that is supposed to run at disk speed. Measured at 1 MiB,
		// n=3: 205us/read allocating vs 63us pooled, against 57us for the plain
		// online read — i.e. the allocation, not the disk, was most of the
		// offline penalty. Garbage per read drops 1,049,042 B -> 478 B.
		//
		// releaseOfflineReadBuf recycles ONLY when done==true. A bound-exceeded
		// read leaves its goroutine still writing into this buffer, so that one
		// is abandoned to the GC rather than handed to another reader.
		proof := f.captureSidecarSourceProof(off, len(p))
		tmp := offlineReadBuf(len(p))
		fuseStart := time.Time{}
		if trace != nil {
			fuseStart = time.Now()
		}
		bn, berr, done := readAtBounded(f.fuseFD, tmp, off, bound)
		if trace != nil {
			trace.FUSEReadNS += time.Since(fuseStart).Nanoseconds()
		}
		defer releaseOfflineReadBuf(tmp, done)
		if !done || (berr != nil && bn == 0 && !errors.Is(berr, io.EOF)) {
			return 0, pin.ErrOfflineNotAvailable
		}
		if bn > 0 {
			copy(p, tmp[:bn])
			if f.readahead != nil {
				f.readahead.OnRead(f.inode, off, bn, f.name)
			}
			metrics.Default().AddBytesRead(int64(bn))
		}
		// A successful ReaderAt call commonly returns the complete final extent
		// together with io.EOF when the caller's buffer is larger than the file.
		// Finder uses that shape for small AppleDouble bodies before a positioned
		// rewrite. Preserve the complete local evidence so the following offline
		// write can safely seed a spool shadow. sidecarMaybePopulate still admits
		// only bounded metadata names whose byte count exactly matches fileSize.
		if f.handler != nil && off == 0 && bn > 0 &&
			(berr == nil || errors.Is(berr, io.EOF)) {
			f.handler.sidecarMaybePopulate(f.name, off, tmp[:bn], f.fileSize)
			f.publishSidecarSourceProof(proof, tmp[:bn])
		}
		return bn, berr
	}

	// Priority 3: JuiceFS FUSE read (populates SSD cache for next time).
	// Read-QoS (#4, INSTANT-NAV): on slow/metered links, admit through the
	// two-lane gate so a bulk/preview storm can't starve the first-block
	// probes Finder browsing depends on. The token spans the retry loop
	// below (a retrying read must not re-queue). Inert on medium/fast.
	if trace != nil {
		trace.FinalRoute = "fuse_online"
	}
	var demandJob *readaheadMappingJob
	if demandMappingMiss(trace) {
		demandJob = f.handler.captureDemandMapping(f)
		defer func() {
			if demandJob != nil {
				demandJob.release()
			}
		}()
	}
	if err := f.ensureFuseFDTraced(trace); err != nil {
		return 0, f.handler.classifyBlipError(err)
	}
	releaseQoS := defaultReadQoS.acquire(off, len(p))
	qosHeld := true
	defer func() {
		if qosHeld {
			releaseQoS()
		}
	}()
	proof := f.captureSidecarSourceProof(off, len(p))
	readStart := time.Now()
	fuseStart := time.Time{}
	if trace != nil {
		fuseStart = time.Now()
	}
	n, err := f.boundedFUSEReadAt(p, off)
	if trace != nil {
		trace.FUSEReadNS += time.Since(fuseStart).Nanoseconds()
	}
	// Populate the sidecar cache from a COMPLETE single read of a `._` file
	// (off==0 and the read returned the whole file) so the next visit — and
	// every other client's Finder — is RAM-served. Only complete reads cache
	// (the membuf-stale-partial guard); sidecarMaybePopulate re-checks.
	if f.handler != nil && off == 0 && n > 0 &&
		(err == nil || errors.Is(err, io.EOF)) {
		f.handler.sidecarMaybePopulate(f.name, off, p[:n], f.fileSize)
		f.publishSidecarSourceProof(proof, p[:n])
	}
	// [JM6 readback-resilience, 2026-06-14 / 2026-06-15] Two JuiceFS-under-
	// concurrent-load transients corrupt a read even though the bytes at rest
	// are intact (a sequential or retried re-read always succeeds):
	//
	//  (1) ZERO-progress EIO — surfaces as NFS3ERR_IO → EIO/SIGBUS at the
	//      client (NLEs mmap media and CRASH). Root-caused via a 10-way readback
	//      of a 119GB/5387-file shoot: ~1-2.5% of cold reads failed, 0 serially.
	//
	//  (2) PREMATURE EOF — io.EOF returned at an offset BEFORE the real end of
	//      file. Propagating it sets NFS EOF=1 and TRUNCATES the file at the
	//      client. Root-caused 2026-06-15: a 65MB file delivered as exactly its
	//      first 1MB under 14-way reads; the server's own per-read log proved it
	//      served the WHOLE file — the client stopped on the false EOF. SILENT:
	//      read_fails/rpc_errors stayed 0, and a per-read DATA verify caught
	//      nothing because every read's bytes were correct — only the EOF lied.
	//
	// Both retry via the coherent FUSE path. f.fileSize is the Open-time cached
	// size; if 0/unknown we can't judge prematurity so EOF is trusted (no
	// behavior change). A premature EOF arriving WITH data (n>0) has its EOF
	// dropped (return n,nil) so the client reissues for the tail; a premature
	// EOF with no data is retried, and if it persists we surface
	// io.ErrUnexpectedEOF → NFS3ERR_IO so the client ERRORS rather than silently
	// truncating. Offline errors stay fail-fast (the offline UX contract).
	// A bounded-timeout already has the correct retry owner: return JUKEBOX and
	// let the NFS client retry. Starting four more preads here while the first
	// timed-out syscall still owns its data-gate slot would amplify one wedge
	// into five and recreate the cancellation storm this deadline prevents.
	retryNeeded := shouldRetryColdFUSERead(n, err, off, f.fileSize)
	if retryNeeded && trace != nil {
		retryStart := time.Now()
		n, err = retryColdFUSERead(f, p, off, f.fileSize, f.name, n, err)
		trace.RetryNS += time.Since(retryStart).Nanoseconds()
	} else {
		n, err = retryColdFUSERead(f, p, off, f.fileSize, f.name, n, err)
	}
	// Drop a premature EOF that came with data — there is more file beyond
	// off+n, so an EOF here would truncate. Client reissues at off+n.
	if n > 0 && prematureFUSEEOF(n, err, off, f.fileSize) {
		err = nil
	}
	// Still a premature zero+EOF after retries: surface a hard error, never a
	// truncating EOF. io.ErrUnexpectedEOF maps to NFS3ERR_IO in onRead.
	if n == 0 && prematureFUSEEOF(n, err, off, f.fileSize) {
		metrics.Default().IncReadFail()
		jmlog.Warn("premature EOF before fileSize after retries; surfacing EIO not truncating EOF",
			"path", f.name, "off", off, "fileSize", f.fileSize)
		err = io.ErrUnexpectedEOF
	}
	// A PARTIAL read with a transient error (n>0, non-EOF, non-offline): the n
	// bytes are valid (pread filled them before the shortfall); surfacing the
	// error maps to NFS3ERR_IO, which the kernel turns into SIGBUS for an MMAP
	// reader — and NLEs mmap their media, so they CRASH mid-playback. Deliver
	// the partial bytes and DROP the error; the client reissues at off+n for the
	// remainder (re-fetching the cold chunk). mmap-safe analogue of the n==0
	// cold-EIO retry. Found 2026-06-15: 20/692 cold CONCURRENT mmap reads SIGBUS'd
	// here while read_retries stayed 0 — the a3ba369 retry only covers n==0.
	if n > 0 && err != nil && !errors.Is(err, io.EOF) && !pin.IsOfflineNotAvailable(err) {
		metrics.Default().IncReadRetry()
		jmlog.Debug("partial FUSE read salvaged (dropped transient EIO; client reissues)",
			"path", f.name, "off", off, "n", n, "err", err)
		err = nil
	}
	if n > 0 && f.readahead != nil {
		f.readahead.OnRead(f.inode, off, n, f.name)
	}
	if n > 0 {
		metrics.Default().AddBytesRead(int64(n))
		elapsed := time.Since(readStart)
		// WAVE 0 (RC-1/RC-3 grader): split this Priority-3 FUSE subread into
		// cold (real backend/MinIO GET over the link) vs warm (local SSD block
		// cache) using the read's OWN measured duration — the SAME >=3ms
		// discriminator ObserveThroughput uses below. No new syscall: elapsed is
		// already measured for the link estimator. The cold ratio is the grader
		// for whether preview READs hit MinIO or the local SSD cache.
		if elapsed >= coldSubreadDur {
			metrics.Default().IncReadColdSubread()
		} else {
			metrics.Default().IncReadWarmSubread()
		}
		// [#16 phase 2] Feed the link estimator from the MAIN read path. This is
		// the signal the prefetch-only sampler missed (juicefs pre-pulls whole
		// files before our readahead runs, so our prefetch reads were all cache
		// hits). A cold subread here is a real backend transfer; ObserveThroughput
		// filters warm/sub-256KB reads so only wire-speed moves the estimate.
		netprofile.Default().ObserveThroughput(int64(n), elapsed)
	}
	if demandJob != nil && n > 0 && (err == nil || errors.Is(err, io.EOF)) {
		// The source read and its data gates have settled. The metadata-only
		// worker must not hold the bulk QoS lane while Redis is slow.
		releaseQoS()
		qosHeld = false
		demandJob.finishDemand(off, n)
		demandJob = nil
	}
	// #9 blip park: a transport-class hard error during a backend blip
	// window re-maps to the retryable JUKEBOX (see nfs/blip.go). The
	// torn-read guards above (premature-EOF → ErrUnexpectedEOF) and
	// offline errors pass through untouched.
	return n, f.handler.classifyBlipError(err)
}

func (f *cachedFile) Read(p []byte) (int, error) {
	r, err := f.handler.beginReadAdmission(f.name)
	if err != nil {
		return 0, err
	}
	n, readErr := f.readUnprotected(p)
	if r != nil {
		r.finishBytes(p, &n, &readErr)
	}
	return n, readErr
}

func (f *cachedFile) readUnprotected(p []byte) (int, error) {
	if f.handler != nil {
		if err := f.handler.namespace.servingError(f.name); err != nil {
			return 0, err
		}
	}
	if err := refuseFUSEContentRead(f.handler != nil && f.handler.testRefuseFUSEContentReads); err != nil {
		return 0, err
	}
	if err := f.ensureFuseFD(); err != nil {
		return 0, err
	}
	return f.fuseFD.Read(p)
}
func (f *cachedFile) Write(p []byte) (int, error) {
	if err := f.ensureFuseFD(); err != nil {
		return 0, err
	}
	return f.fuseFD.Write(p)
}
func (f *cachedFile) Seek(offset int64, whence int) (int64, error) {
	if err := f.ensureFuseFD(); err != nil {
		return 0, err
	}
	return f.fuseFD.Seek(offset, whence)
}
func (f *cachedFile) Lock() error   { return nil }
func (f *cachedFile) Unlock() error { return nil }
func (f *cachedFile) Truncate(size int64) error {
	// Clamp the sticky write-size high-water DOWN to the truncation point.
	// Without this, a SHRINKING overwrite (truncate to a smaller size, then
	// write less) leaves the old larger high-water in writeSizes — trackWriteSize
	// only RAISES it — and Stat then over-reports the old size, so reads return
	// the truncated tail as ZEROS (corrupt content on a shrink-overwrite).
	if f.handler != nil {
		f.handler.clampWriteSize(f.name, size)
	}
	if err := f.ensureFuseFD(); err != nil {
		return err
	}
	return f.fuseFD.Truncate(size)
}

func (f *cachedFile) Close() error {
	f.fdMu.Lock()
	defer f.fdMu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	if f.fuseFD == nil {
		return nil
	}
	if f.fdPool != nil {
		f.fdPool.Release(f.fusePath)
	} else {
		_ = f.fuseFD.Close()
	}
	f.fuseFD = nil
	return nil
}

type inPlaceMutationLock struct {
	sync.RWMutex
	refs int // users and waiters, guarded by inPlaceMutationMu
}

// lockInPlaceMutation orders physical mutations and their size publications.
// WRITE RPCs remain parallel; truncate excludes them only for the same path.
// Entries include waiters in refs and disappear after the last operation.
func (h *JuiceMountHandler) lockInPlaceMutation(path string, exclusive bool) func() {
	h.inPlaceMutationMu.Lock()
	if h.inPlaceMutations == nil {
		h.inPlaceMutations = make(map[string]*inPlaceMutationLock)
	}
	mu := h.inPlaceMutations[path]
	if mu == nil {
		mu = &inPlaceMutationLock{}
		h.inPlaceMutations[path] = mu
	}
	mu.refs++
	h.inPlaceMutationMu.Unlock()
	if exclusive {
		mu.Lock()
	} else {
		mu.RLock()
	}
	return func() {
		if exclusive {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
		h.inPlaceMutationMu.Lock()
		mu.refs--
		if mu.refs == 0 {
			delete(h.inPlaceMutations, path)
		}
		h.inPlaceMutationMu.Unlock()
	}
}

// writeFile binds a pooled descriptor to one mirror incarnation and publishes
// each mutation before releasing namespace ownership.
type writeFile struct {
	*os.File
	name       string
	handler    *JuiceMountHandler
	entry      *metadata.Entry
	writtenEnd int64 // highest byte position written
	fdPool     *FDPool
	fusePath   string
	// baseSize is the file's size when this writer opened it — the prefix of
	// pre-existing, valid content. A hole can only begin ABOVE it, so seeding
	// the tracker from anything lower would claim real bytes are a hole.
	// go-nfs calls OpenFile+Write+Close per WRITE RPC, so this is re-derived
	// per RPC from the mirror entry (no syscall) and the durable state lives
	// on the handler.
	baseSize int64
	bound    metadata.MirrorToken
	identity os.FileInfo
	closed   bool
}

func (f *writeFile) Name() string  { return f.name }
func (f *writeFile) Lock() error   { return nil }
func (f *writeFile) Unlock() error { return nil }

// Truncate publishes the authoritative size synchronously, including zero.
// The per-path lock orders its syscall and CAS against writes and closes.
func (f *writeFile) Truncate(size int64) error {
	op := f.handler.namespace.begin([]string{f.name})
	if op == nil {
		return errFUSETimeout
	}
	err := f.truncateOwned(size, op)
	op.finish(err)
	return err
}

func (f *writeFile) Write(p []byte) (int, error) {
	return f.writeBound(p, 0, false)
}

// noteHole records this write against the in-place hole tracker. Split out so
// Write, WriteAt and Truncate cannot drift apart on which one remembers to do it.
func (f *writeFile) noteHole(off, end int64) {
	f.handler.inPlaceHoles.noteWrite(f.name, f.baseSize, off, end)
}

func (f *writeFile) WriteAt(p []byte, off int64) (int, error) {
	return f.writeBound(p, off, true)
}

func (f *writeFile) Close() error {
	if f.closed {
		return os.ErrClosed
	}
	op := f.handler.namespace.begin([]string{f.name})
	if op == nil {
		f.release()
		return errFUSETimeout
	}
	unlock := f.handler.lockInPlaceMutation(f.name, true)
	_, err := f.validateBinding()
	defer func() { op.finish(err) }()
	defer unlock()
	defer f.release()
	if err != nil {
		return err
	}
	// Each mutation already published its exact EOF. Close only releases the
	// descriptor; it must never replay a path-keyed high-water contribution.
	f.handler.cacheWrittenMetadataFromFUSE(f.name, f.File)
	return nil
}

func (h *JuiceMountHandler) syncOnlineMutation(f *os.File) error {
	if h.testSyncOnlineMutation != nil {
		return h.testSyncOnlineMutation(f)
	}
	return syncFUSEFile(f)
}

// billyFile wraps os.File to implement billy.File (for writes / non-cached opens).
//
// pinned is captured at OpenFile time, same semantics as cachedFile.pinned —
// a per-open snapshot used to gate the read-time offline check. Without it,
// a file opened during a brief online window and then read after offline
// flips on would bypass the gate and stall on FUSE → backend.
type billyFile struct {
	*os.File
	name    string
	pinned  bool
	handler *JuiceMountHandler // for clampWriteSize on Truncate
}

func (f *billyFile) Name() string  { return f.name }
func (f *billyFile) Lock() error   { return nil }
func (f *billyFile) Unlock() error { return nil }
func (f *billyFile) Truncate(size int64) error {
	// Clamp the write-size high-water down to the truncation point — see
	// cachedFile.Truncate; a shrink-overwrite would otherwise over-report size.
	if f.handler != nil {
		f.handler.clampWriteSize(f.name, size)
	}
	return f.File.Truncate(size)
}

// Read overrides *os.File.Read to enforce the read-time offline gate on
// un-pinned files. Pinned files fall through; ReadAt below is the bounded NFS
// data path (#43) — Read (no offset) isn't used for NFS READ RPCs.
func (f *billyFile) Read(p []byte) (int, error) {
	r, err := f.handler.beginReadAdmission(f.name)
	if err != nil {
		return 0, err
	}
	n, readErr := f.readUnprotected(p)
	if r != nil {
		r.finishBytes(p, &n, &readErr)
	}
	return n, readErr
}

func (f *billyFile) readUnprotected(p []byte) (int, error) {
	if err := f.handler.cacheBackendError(); err != nil {
		return 0, err
	}
	if f.handler != nil {
		if err := f.handler.namespace.servingError(f.name); err != nil {
			return 0, err
		}
	}
	if err := refuseFUSEContentRead(f.handler != nil && f.handler.testRefuseFUSEContentReads); err != nil {
		return 0, err
	}
	if pin.IsOffline() && !f.pinned {
		return 0, pin.ErrOfflineNotAvailable
	}
	return f.File.Read(p)
}

// LiveSize fstats the embedded FUSE fd for the authoritative current size (see
// cachedFile.LiveSize, task #65 — the metadata mirror can lag stale-low during
// an offline->online drain burst and truncate reads). Bounded against a wedge.
func (f *billyFile) LiveSize() (int64, bool) {
	if f.File == nil {
		return 0, false
	}
	// Attribution: FOREGROUND, same as cachedFile.LiveSize — a READ RPC waits.
	start := time.Now()
	timer := time.NewTimer(fuseStatTimeout)
	defer timer.Stop()
	gateWait, depth, acquired := acquireFUSEGate(fuseFstatGate, timer)
	if !acquired {
		metrics.Default().ObserveFUSECall(metrics.FUSESrcForeground, metrics.FUSEOpFstat, metrics.FUSEGateFstat,
			0, gateWait, time.Since(start), metrics.FUSEOutcomeGateTimeout)
		return 0, false
	}
	type result struct {
		sz int64
		ok bool
	}
	ch := make(chan result, 1)
	go func() {
		fi, err := f.File.Stat()
		if err != nil {
			ch <- result{0, false}
		} else {
			ch <- result{fi.Size(), true}
		}
		<-fuseFstatGate
	}()
	select {
	case r := <-ch:
		metrics.Default().ObserveFUSECall(metrics.FUSESrcForeground, metrics.FUSEOpFstat, metrics.FUSEGateFstat,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeOK)
		return r.sz, r.ok
	case <-timer.C:
		metrics.Default().ObserveFUSECall(metrics.FUSESrcForeground, metrics.FUSEOpFstat, metrics.FUSEGateFstat,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeTimeout)
		return 0, false
	}
}

// IncompleteAt lets onRead distinguish a tracked in-flight hole from EOF when
// the metadata-free open path returns a billyFile.
func (f *billyFile) IncompleteAt(off int64) bool {
	if f.handler == nil {
		return false
	}
	plan, tracked := f.handler.inPlaceHoles.inPlaceReadPlan(f.name, off)
	held := tracked && plan == planHold
	if held {
		f.handler.inPlaceHoles.logHold(f.name, off)
	}
	return held
}

// ReadAt is the hot path for NFS READ RPCs (which always carry an offset).
func (f *billyFile) BeginReadAdmission() (func() error, func(), error) {
	r, err := f.handler.beginReadAdmission(f.name)
	if err != nil || r == nil {
		return nil, nil, err
	}
	return r.check, r.release, nil
}

func (f *billyFile) ReadAt(p []byte, off int64) (int, error) {
	r, err := f.handler.beginReadAdmission(f.name)
	if err != nil {
		return 0, err
	}
	n, readErr := f.readAtUnprotected(p, off)
	if r != nil {
		r.finishBytes(p, &n, &readErr)
	}
	return n, readErr
}

func (f *billyFile) readAtUnprotected(p []byte, off int64) (int, error) {
	if err := f.handler.cacheBackendError(); err != nil {
		return 0, err
	}
	if f.handler != nil {
		if err := f.handler.namespace.servingError(f.name); err != nil {
			return 0, err
		}
	}
	if err := refuseFUSEContentRead(f.handler != nil && f.handler.testRefuseFUSEContentReads); err != nil {
		return 0, err
	}
	if pin.IsOffline() && !f.pinned {
		return 0, pin.ErrOfflineNotAvailable
	}
	// The same in-place hole choke point as cachedFile (B-9): this is the
	// metadata-free open path, used for files the mirror has not scanned yet.
	return f.handler.guardInPlaceRead(f.name, p, off, func(q []byte) (int, error) {
		return f.readAtRouted(q, off)
	})
}

// readAtRouted serves a read already admitted by guardInPlaceRead.
func (f *billyFile) readAtRouted(p []byte, off int64) (int, error) {
	if pin.IsOffline() {
		// Pinned (#43, 2026-06-22): don't trust the pin "Ready" flag as proof
		// the bytes are resident — it goes stale after LRU eviction. Bound the
		// read so an evicted block refuses cleanly (ErrOfflineNotAvailable)
		// instead of tarpitting on a backend GET that can't complete offline. A
		// genuinely-local block returns in well under the bound and is served.
		tmp := make([]byte, len(p))
		bn, berr, done := readAtBounded(f.File, tmp, off, offlinePinnedReadTimeout)
		if !done || (berr != nil && bn == 0 && !errors.Is(berr, io.EOF)) {
			return 0, pin.ErrOfflineNotAvailable
		}
		if bn > 0 {
			copy(p, tmp[:bn])
		}
		return bn, berr
	}
	// Read-QoS (#4, INSTANT-NAV): this branch is a FUSE-backed read (the
	// no-metadata-cache open path) — backend-capable, so it shapes through
	// the same two-lane gate as cachedFile.ReadAt. Inert on medium/fast.
	releaseQoS := defaultReadQoS.acquire(off, len(p))
	defer releaseQoS()
	// Same bounded data syscall as cachedFile: an abandoned Finder read may not
	// keep an RPC slot parked for minutes. The private buffer prevents a late
	// pread from scribbling into a recycled NFS response after timeout.
	n, err := boundedFUSEFileReadAt(f.File, p, off, fuseReadTimeout())
	if f.handler == nil {
		return n, err
	}
	return n, f.handler.classifyBlipError(err) // #9 blip park
}

// rootDirInfo is the FileInfo for the root directory.
// Sys() returns a *syscall.Stat_t with the current user's UID/GID so that
// Finder doesn't show the red "no access" badge on the mount root.
type rootDirInfo struct{ mtime time.Time }

func (r *rootDirInfo) Name() string      { return "" }
func (r *rootDirInfo) Size() int64       { return 0 }
func (r *rootDirInfo) Mode() fs.FileMode { return fs.ModeDir | 0755 }

// rootMtime is a STABLE modification time for the synthetic mount root, set once
// at process start. Previously rootDirInfo.ModTime() returned time.Now() on EVERY
// stat, so the root's mtime jittered on every LOOKUP/GETATTR — macOS Tahoe's Finder
// saw the mount root as perpetually modified and re-validated its pre-flight LOOKUPs
// in an infinite loop (~400 LOOKUPs, zero writes), stalling any copy INTO the mount
// root with "connection interrupted" (task #99). A copy into a real subfolder worked
// because a subfolder carries a stable stored mtime (metadata FileInfo.ModTime =
// entry.Mtime). A stable value ends the loop; root-listing freshness is covered by
// the attr-cache TTL + Finder re-reading on navigation + reconcile-driven refresh.
// TODO(#99): bump this when a child is created/removed at the root so clients notice
// root-level changes before the attr-cache TTL, without reintroducing per-stat jitter.
var rootMtime = time.Now()

func (r *rootDirInfo) ModTime() time.Time {
	if !r.mtime.IsZero() {
		return r.mtime
	}
	return rootMtime
}
func (r *rootDirInfo) IsDir() bool { return true }
func (r *rootDirInfo) Sys() any {
	return &syscall.Stat_t{
		Ino:   1,
		Uid:   uint32(os.Getuid()),
		Gid:   uint32(os.Getgid()),
		Nlink: 2,
	}
}

// warmOpTimeout is the per-operation FUSE budget for the OPPORTUNISTIC WARMERS
// (`._` sidecar warmer, farm-thumbnail hydrator). It is deliberately separate
// from fuseStatTimeout, which bounds the FOREGROUND hot path.
//
// WHY (measured 2026-07-29, real cellular link, and again with the mount
// OFFLINE):
//
//	sidecar_warm  calls=264  timeouts=258 (98%)  mean=791ms  populated=2
//	foreground    calls=6228 timeouts=10  (0.2%) mean=38.5ms
//
// The warmer was not failing because warming is hopeless — it was failing
// because it was UNDER-BUDGETED BY 23 MILLISECONDS. nfs/sidecar.go's own field
// note records the measurement: "a single `._` read = 777ms" on cellular, and
// fuseStatTimeout is 800ms. Every jitter spike blows it. So 258 reads each
// burned the full 800ms and populated nothing.
//
// fuseStatTimeout ALREADY specifies 2s for this case — "On WAN (high-RTT MinIO
// over Tailscale/cellular) a cold range GET legitimately takes longer, so the
// default stays 2s there" — gated on JM_WAN_MODE=1, a variable set in nothing
// but test files. Same dead knob that kept the WAN metadata TTLs from ever
// shipping. This routes it off the measured link instead.
//
// ONLY the warmers get the longer budget, deliberately:
//   - The foreground path is HEALTHY at 800ms (10 timeouts in 6228 calls).
//     Raising ITS bound trades a fast failure for a slow one on the RPC a client
//     is blocked on, and a longer foreground FUSE wait is precisely the JUKEBOX
//     generator behind "connection interrupted" (see the mount-facts note in
//     the audit). That is a separate decision with its own evidence bar.
//   - Warmers already run on their own gate (warmGate) and nobody waits on
//     them, so a longer bound costs only background time.
//
// The futility breaker still wraps this: if warming fails anyway, it parks.
// Kill switch JM_WARM_OP_TIMEOUT_MS pins an explicit value.
func warmOpTimeout() time.Duration {
	if v := os.Getenv("JM_WARM_OP_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Millisecond
		}
	}
	if netprofile.Default().HighLatency() {
		// The value fuseStatTimeout already documents for WAN, now reachable.
		if fuseStatTimeout < 2*time.Second {
			return 2 * time.Second
		}
	}
	return fuseStatTimeout
}
