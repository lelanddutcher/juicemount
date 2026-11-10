package nfs

import (
	"errors"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

const (
	// Readahead triggers after this many consecutive sequential reads on the same
	// inode. Historical default; the live value comes from the network profile
	// (netprofile.ReadaheadPolicy.SeqThreshold) when one is wired — medium-class
	// policy equals this exactly, so an unprofiled/LAN run is unchanged.
	sequentialThreshold = 3

	// Number of blocks to prefetch ahead of the current read position.
	readaheadBlocks = 8 // 8 * 4MB = 32MB ahead

	// Max concurrent readahead goroutines across all files. The worker pool is
	// SIZED to the most aggressive (fast-link) policy; the per-link policy caps
	// how many of those slots are actually used, so a slow link stays gentle
	// without re-sizing the semaphore at runtime.
	maxReadaheadWorkers = 8

	// Block size for readahead (matches JuiceFS block size).
	readaheadBlockSize = 4 << 20 // 4MB

	// Stale tracker entries are cleaned up after this duration.
	trackerTTL = 30 * time.Second

	// wanBulkSignalBytes is the amount of contiguous foreground demand that is
	// strong enough to distinguish an actual file read from Finder's small
	// metadata/preview probes. NFS subdivides each 1 MiB READ into 256 KiB
	// ReadAt calls, so the historical hit-count-only guard could not recognize
	// the user's intent reliably when some of those calls were served by the
	// direct SSD cache. One MiB is bounded enough for cellular while allowing a
	// single 4 MiB block to overlap the next foreground reads.
	wanBulkSignalBytes = 1 << 20
	wanBulkSubreadMax  = 256 << 10
)

// windowIdleStop ends a windowed prefetch (ParallelFetch policies) when its
// reader has not read for this long. The window already limits how far ahead
// it can run; this also stops refills for an abandoned stream. Five seconds is
// longer than one cold 4 MB block on a 30 Mbit/s link with four blocks sharing
// it, so a reader that is merely waiting is not dropped.
const windowIdleStop = 5 * time.Second

// errPrefetchStopped marks a windowed block that gave its gate back because
// offline or Stop won after admission.
var errPrefetchStopped = errors.New("readahead: stopped")

// windowBufPool recycles 4 MB block buffers between windowed block reads.
var windowBufPool = sync.Pool{New: func() any {
	b := make([]byte, readaheadBlockSize)
	return &b
}}

// ReadaheadManager detects sequential read patterns and triggers
// background prefetch to warm the JuiceFS SSD cache.
type ReadaheadManager struct {
	fusePath string
	fdPool   *FDPool // reuse pooled fds instead of opening new ones
	// Acceptance-only: never let a local foreground cache hit schedule a
	// speculative FUSE read behind the zero-byte remote-content guard.
	refuseFUSEContentReads bool

	// Test barriers observe real descriptor acquisition and wrap a real ReadAt.
	// Set only before the manager starts workers; nil leaves production unchanged.
	prefetchWorkerEntry  func()
	prefetchWorkerExit   func()
	afterPrefetchAcquire func(*os.File)
	afterPrefetchGate    func()
	prefetchReadAt       func(*os.File, []byte, int64) (int, error)
	// policyOverride replaces the profile's readahead policy (tests and the
	// before/after simulation only).
	policyOverride func() netprofile.ReadaheadPolicy
	// windowIdle is windowIdleStop; tests shorten it before reading.
	windowIdle time.Duration
	// sizeHintTTL is sizeHintTTL; tests shorten it before reading.
	sizeHintTTL time.Duration
	// Installed by the handler before serving. It captures local authority for
	// the scheduled inode before this worker opens/reads its source descriptor.
	mappingCapture func(uint64, string) *readaheadMappingJob

	// profile drives link-aware prefetch policy (depth/width/enable) and is fed
	// throughput samples from prefetch block reads. nil → historical defaults
	// (medium policy), so an unwired manager behaves exactly as before.
	profile *netprofile.Profile

	mu       sync.Mutex
	trackers map[uint64]*readTracker // inode → tracker

	// Semaphore for concurrent readahead goroutines (sized to the fast-link max;
	// the per-link policy.Workers caps how many slots are actually used).
	sem          chan struct{}
	stopCh       chan struct{}
	stopped      bool
	stopOnce     sync.Once
	wg           sync.WaitGroup
	previewQueue chan previewJob
	previewSeen  map[previewKey]time.Time
	// A separate TryLock serializes best-effort directory scan progress without
	// making foreground ReadDir wait for another speculative selector.
	previewScanMu  sync.Mutex
	previewCursors map[previewCursorKey]previewCursor
	previewScanAge uint64
	previewStop    chan struct{}
	previewDone    chan struct{}
	previewOnce    sync.Once
	previewClosed  bool
	previewRun     func(previewJob)
	// Construction-only scalars, set before previewLoop starts. No caller
	// pointer or live environment mutation is retained by the manager.
	previewOptInSet   bool
	previewOptInValue bool

	// activePrefetch is the live count of in-flight prefetch goroutines, gated
	// against the current policy's Workers so a slow link stays gentle.
	activePrefetch atomic.Int64
	// windowInflight counts windowed prefetch blocks in flight across every
	// stream. It is capped below the background gate share (windowBlockCap)
	// so one background slot always stays free for the drainer, membuf, the
	// sidecar warmer and metadata preparation.
	windowInflight atomic.Int64
	// windowCapOverride replaces windowBlockCap when > 0 (simulation only).
	windowCapOverride int

	// Stats
	statsMu    sync.Mutex
	triggered  int64
	prefetched int64
}

// effectivePolicy returns the link-aware readahead policy, falling back to the
// historical hard-coded defaults when no profile is wired (nil-safe).
func (rm *ReadaheadManager) effectivePolicy() netprofile.ReadaheadPolicy {
	if rm.policyOverride != nil {
		return rm.policyOverride()
	}
	if rm.profile == nil {
		return netprofile.ReadaheadPolicy{
			Enabled:      true,
			SeqThreshold: sequentialThreshold,
			Blocks:       readaheadBlocks,
			Workers:      4,
		}
	}
	return rm.profile.Readahead()
}

type readTracker struct {
	lastOffset      int64
	sequentialHits  int
	sequentialBytes int64
	wanSubreadRun   bool
	lastAccess      time.Time
	prefetchedTo    int64 // highest offset we've already prefetched to

	// Windowed prefetch (ParallelFetch policies). All fields are block
	// aligned and guarded by ReadaheadManager.mu. windowNext is the next block
	// to fetch; windowEnd is the end of the window the reader has earned.
	// pumping is true while one pumpWindow goroutine serves this tracker.
	windowNext int64
	windowEnd  int64
	pumping    bool
	// windowEOF is where a windowed block read hit end of file (0 = not
	// seen). The window does not extend past it unless the reader reads
	// past it (the file grew).
	windowEOF int64
	// roundGen identifies the round that owns pumping; a round clears
	// pumping only while it still owns it.
	roundGen uint64
	// sizeHint is the file size a round last saw with fstat, at sizeHintAt.
	// It is a hint, not an EOF: within sizeHintTTL no new round starts for
	// blocks at or past it, and after that a round re-checks, so a file that
	// grows while it is read keeps its prefetch.
	sizeHint   int64
	sizeHintAt time.Time
}

// sizeHintTTL is how long a round's fstat size holds back new rounds at the
// end of a file.
const sizeHintTTL = time.Second

// alignUpBlock rounds off up to the next readahead block boundary.
func alignUpBlock(off int64) int64 {
	return (off + readaheadBlockSize - 1) / readaheadBlockSize * readaheadBlockSize
}

func NewReadaheadManager(fusePath string, fdPool *FDPool, profile *netprofile.Profile, previewOptIn ...bool) *ReadaheadManager {
	rm := &ReadaheadManager{
		fusePath:       fusePath,
		fdPool:         fdPool,
		profile:        profile,
		trackers:       make(map[uint64]*readTracker),
		sem:            make(chan struct{}, maxReadaheadWorkers),
		stopCh:         make(chan struct{}),
		previewQueue:   make(chan previewJob, previewQueueLimit),
		previewSeen:    make(map[previewKey]time.Time),
		previewCursors: make(map[previewCursorKey]previewCursor),
		previewStop:    make(chan struct{}),
		previewDone:    make(chan struct{}),
		windowIdle:     windowIdleStop,
		sizeHintTTL:    sizeHintTTL,
	}
	if len(previewOptIn) != 0 {
		rm.previewOptInSet = true
		rm.previewOptInValue = previewOptIn[0]
	}
	rm.wg.Add(1)
	go rm.previewLoop()
	rm.wg.Add(1)
	go func() {
		defer rm.wg.Done()
		rm.cleanupLoop()
	}()
	return rm
}

// serverReadaheadEnabled is the S2 kill-switch (WAVE 1, RC-4). The server-side
// ReadaheadManager previously had NO env off-switch, violating the kill-switch
// doctrine. JM_SERVER_READAHEAD=0 disables all server prefetch scheduling; every
// other value (incl. unset) keeps the historical behavior. Read per call — this
// only executes on the read path's schedule decision, never a per-RPC syscall.
func serverReadaheadEnabled() bool {
	return os.Getenv("JM_SERVER_READAHEAD") != "0"
}

// smallPreviewRunBlocks returns the S2 short-run guard threshold (WAVE 1, RC-4):
// a sequential run SHORTER than this many blocks is treated as a Finder preview
// probe and is NOT escalated to the full prefetch window — the run keeps
// accumulating, so a GENUINE sequential read still escalates once it grows past
// the guard.
//
// Class-scaled so 10GbE/GbE (Fast/Medium) are UNCHANGED — the guard is DISABLED
// there (0) so LAN prefetch fires exactly as before; only constrained WAN
// classes tighten, where an over-pull crosses the tunnel and costs seconds.
// The value is ABOVE slow's SeqThreshold (4), creating a suppression band at
// hits 4–5; it equals metered's threshold (6), so metered does not schedule
// until a genuine run crosses six sequential blocks. nil profile ⇒
// Medium (0, disabled), so an unwired manager behaves exactly as before.
func smallPreviewRunBlocks(prof *netprofile.Profile) int {
	if prof == nil {
		return 0 // Medium default — guard disabled, historical behavior
	}
	switch prof.Class() {
	case netprofile.ClassMetered, netprofile.ClassSlow:
		// Slow WAN: hold runs under 6 blocks (~24MiB). Above SeqThreshold(=4)
		// so trips at hits 4–5 are suppressed as preview probes; 6+ escalates.
		return 6
	default: // ClassMedium, ClassFast — 10GbE/GbE unchanged
		return 0
	}
}

// OnRead is called by the NFS read handler after every READ operation.
// It tracks the read pattern and triggers readahead if sequential.
func (rm *ReadaheadManager) OnRead(inode uint64, offset int64, size int, filePath string) {
	if rm.refuseFUSEContentReads {
		return
	}
	// S2 kill-switch (WAVE 1): default-on, env-disable. Off the per-RPC FUSE
	// path — a single env read + early return, no lock taken when disabled.
	if !serverReadaheadEnabled() || pin.IsOffline() {
		return
	}

	policy := rm.effectivePolicy()

	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.stopped || pin.IsOffline() {
		return
	}

	tracker, ok := rm.trackers[inode]
	if !ok {
		tracker = &readTracker{
			sequentialBytes: int64(size),
			wanSubreadRun:   size > 0 && size <= wanBulkSubreadMax,
		}
		rm.trackers[inode] = tracker
	}

	tracker.lastAccess = time.Now()

	// Check if this read is sequential (follows the previous read)
	expectedOffset := tracker.lastOffset + readaheadBlockSize
	// Allow some tolerance for reads within the same block or slightly ahead
	isSequential := offset >= tracker.lastOffset && offset <= expectedOffset+readaheadBlockSize

	if isSequential && offset > tracker.lastOffset {
		tracker.sequentialHits++
		tracker.sequentialBytes += int64(size)
		tracker.wanSubreadRun = tracker.wanSubreadRun && size > 0 && size <= wanBulkSubreadMax
	} else {
		tracker.sequentialHits = 0
		if ok {
			tracker.sequentialBytes = int64(size)
			tracker.wanSubreadRun = size > 0 && size <= wanBulkSubreadMax
		}
		// A real seek ends the windowed prefetch: a running pump stops
		// claiming blocks for the old position. A small reorder of concurrent
		// subreads (within one block back) keeps the window.
		if offset+readaheadBlockSize < tracker.lastOffset || offset > expectedOffset+readaheadBlockSize {
			tracker.windowNext, tracker.windowEnd, tracker.windowEOF = 0, 0, 0
		}
	}
	tracker.lastOffset = offset

	// A disabled policy (operator kill-switch/override) still tracks the pattern
	// so a live policy change can trigger on the next read. Production's metered
	// policy is enabled but guarded and limited to one block/one worker.
	if !policy.Enabled {
		return
	}

	// On WAN, a contiguous 1 MiB foreground run is itself a strong signal even
	// when NFS's 256 KiB subdivision has not reached the historical hit count.
	// Medium/Fast links retain the hit-count-only policy exactly.
	strongWANRun := false
	if rm.profile != nil && tracker.wanSubreadRun && tracker.sequentialBytes >= wanBulkSignalBytes {
		class := rm.profile.Class()
		strongWANRun = class == netprofile.ClassMetered || class == netprofile.ClassSlow
	}

	// Trigger readahead after the link-aware hit threshold or a bounded WAN
	// bulk signal.
	if tracker.sequentialHits >= policy.SeqThreshold || strongWANRun {
		// S2 short-run guard (WAVE 1, RC-4): a SHORT sequential run on a WAN link
		// is a Finder preview probe (a few 256KiB subreads of one file), not a
		// real large sequential reel read. Escalating it to the policy's 64MB
		// window whole-file-pulls across the tunnel for nothing. If the run so
		// far is below the class-scaled small-preview threshold, cap (skip) this
		// round instead — the run keeps accumulating, so a GENUINE sequential
		// read still escalates once it grows past the threshold. This is a plain
		// comparison on the already-tracked sequentialHits — NO new syscall.
		// Disabled (threshold 0) on Fast/Medium, so 10GbE/GbE are unchanged.
		if guard := smallPreviewRunBlocks(rm.profile); !strongWANRun && guard > 0 && tracker.sequentialHits < guard {
			metrics.Default().IncReadaheadSuppressed()
			return
		}
		// Offline may have engaged while this read updated its tracker.
		// Admission and WaitGroup registration stay serialized with Stop.
		if pin.IsOffline() {
			return
		}
		if policy.ParallelFetch {
			rm.extendWindowLocked(inode, tracker, offset+int64(size), policy, filePath)
			return
		}
		// Calculate the range to prefetch (depth scales with the link).
		prefetchStart := offset + int64(size)
		prefetchEnd := prefetchStart + int64(policy.Blocks)*readaheadBlockSize

		// Don't re-prefetch ranges we've already covered
		if prefetchEnd <= tracker.prefetchedTo {
			return
		}
		if prefetchStart < tracker.prefetchedTo {
			prefetchStart = tracker.prefetchedTo
		}
		tracker.prefetchedTo = prefetchEnd

		rm.statsMu.Lock()
		rm.triggered++
		rm.statsMu.Unlock()
		// WAVE 0 (grades S2): one inc per readahead schedule (SeqThreshold trip).
		metrics.Default().IncReadaheadTriggered()

		// Fire background prefetch (non-blocking), capped by the policy's worker budget.
		rm.wg.Add(1)
		go func() {
			defer rm.wg.Done()
			rm.prefetchWithInode(inode, filePath, prefetchStart, prefetchEnd, policy.Workers)
		}()
	}
}

// prefetchAllowed checks the current offline and lifecycle state at each
// admission boundary. A syscall already admitted remains owned until it ends.
func (rm *ReadaheadManager) prefetchAllowed() bool {
	if pin.IsOffline() {
		return false
	}
	select {
	case <-rm.stopCh:
		return false
	default:
		return true
	}
}

// prefetch reads blocks from the JuiceFS FUSE mount to warm the SSD cache.
// maxWorkers caps concurrency to the current link policy (the semaphore is sized
// to the fast-link max; a slow link uses fewer of its slots).
func (rm *ReadaheadManager) prefetch(filePath string, start, end int64, maxWorkers int) {
	rm.prefetchWithInode(0, filePath, start, end, maxWorkers)
}

func (rm *ReadaheadManager) prefetchWithInode(inode uint64, filePath string, start, end int64, maxWorkers int) {
	rm.runRound(inode, filePath, maxWorkers, func(fd *os.File, mapping *readaheadMappingJob) {
		rm.serialBlocks(fd, mapping, start, end)
	})
}

// runRound admits one prefetch round and runs body with an open source
// descriptor. It returns false when the round was shed before body ran.
// Admission order: lifecycle, the per-link round budget, the global worker
// semaphore, then one Read-QoS bulk token held for the whole round.
func (rm *ReadaheadManager) runRound(inode uint64, filePath string, maxWorkers int, body func(*os.File, *readaheadMappingJob)) bool {
	if rm.prefetchWorkerExit != nil {
		defer rm.prefetchWorkerExit()
	}
	// Defense in depth for work queued before a lifecycle toggled the guard.
	if rm.prefetchWorkerEntry != nil {
		rm.prefetchWorkerEntry()
	}
	if rm.refuseFUSEContentReads || !rm.prefetchAllowed() {
		return false
	}
	// Gate against the link-aware worker budget BEFORE taking a semaphore slot.
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	if rm.activePrefetch.Add(1) > int64(maxWorkers) {
		rm.activePrefetch.Add(-1)
		return false // over the per-link worker budget — shed this round
	}
	defer rm.activePrefetch.Add(-1)

	select {
	case rm.sem <- struct{}{}:
		defer func() { <-rm.sem }()
	default:
		return false // all workers busy, skip this round
	}

	if !rm.prefetchAllowed() {
		return false
	}

	// Read-QoS (#4, INSTANT-NAV): prefetch is the LOWEST read class. On
	// slow/metered links it only runs when the bulk lane has a free token,
	// and holds it for the whole round below — a contended lane sheds
	// the round entirely (it re-triggers on the next sequential read),
	// handing the bandwidth back to interactive reads. Inert on medium/fast.
	// A windowed round holds ONE token for all its parallel blocks, so the
	// lane keeps the rest of its width for foreground reads.
	qosRelease, qosOK := defaultReadQoS.tryAcquireBulkPrefetch()
	if !qosOK {
		return false
	}
	if !rm.prefetchAllowed() {
		qosRelease()
		return false
	}
	var mapping *readaheadMappingJob
	if inode != 0 && rm.mappingCapture != nil {
		mapping = rm.mappingCapture(inode, filePath)
		if mapping != nil {
			defer mapping.release()
		}
	}

	fusePath := rm.fusePath + "/" + filePath
	ran := false
	// Scope the descriptor before mapping preparation. Metadata traffic must
	// not retain a FUSE source fd or any per-read FUSE data gate.
	func() {
		defer qosRelease()

		// Use FDPool if available, otherwise fall back to direct open
		var fd *os.File
		var usePool bool
		if rm.fdPool != nil {
			var err error
			fd, err = rm.fdPool.Get(fusePath)
			if err != nil {
				return
			}
			usePool = true
		} else {
			var err error
			fd, err = os.Open(fusePath)
			if err != nil {
				return
			}
		}
		defer func() {
			if usePool {
				rm.fdPool.Release(fusePath)
			} else {
				fd.Close()
			}
		}()
		if rm.afterPrefetchAcquire != nil {
			rm.afterPrefetchAcquire(fd)
		}
		if !rm.prefetchAllowed() {
			return
		}
		ran = true
		body(fd, mapping)
	}()
	if mapping != nil {
		mapping.prepare()
	}
	return ran
}

// readBlock issues one prefetch ReadAt through the test hook when set.
func (rm *ReadaheadManager) readBlock(fd *os.File, buf []byte, off int64) (int, error) {
	if rm.prefetchReadAt != nil {
		return rm.prefetchReadAt(fd, buf, off)
	}
	return fd.ReadAt(buf, off)
}

// noteBlockRead records one completed prefetch block in stats, metrics and
// the link estimator.
func (rm *ReadaheadManager) noteBlockRead(n int, dur time.Duration) {
	rm.statsMu.Lock()
	rm.prefetched++
	rm.statsMu.Unlock()
	// WAVE 0 (grades S2): accumulate blocks actually prefetched.
	metrics.Default().AddReadaheadPrefetchedBlocks(1)
	// Feed the link estimator. A cold block is a real backend transfer
	// (slow); a cache hit is sub-ms and gets filtered out inside
	// ObserveThroughput, so only wire-speed samples move the estimate.
	// Overlapping windowed blocks are fine: the estimator divides by the
	// union of active transfer time.
	if rm.profile != nil {
		rm.profile.ObserveThroughput(int64(n), dur)
	}
}

// serialBlocks is the historical round: one 4 MB buffer, one ReadAt at a time
// over [start, end). Fast, Medium and Metered use it unchanged.
func (rm *ReadaheadManager) serialBlocks(fd *os.File, mapping *readaheadMappingJob, start, end int64) {
	buf := make([]byte, readaheadBlockSize)
	for off := start; off < end; off += readaheadBlockSize {
		if !rm.prefetchAllowed() {
			return
		}

		// FUSE data ceiling (2026-08-05 double kernel panic). Prefetch adds up
		// to 8 concurrent readers on a fast link — a third of the 16-wide
		// ceiling — spent entirely on work nobody asked for yet. Use the
		// NON-BLOCKING background acquire: a foreground acquireFUSEData would
		// let speculative reads sit in the 250ms admission window ahead of a
		// user's actual read, inverting the priority this prefetcher exists to
		// serve. If there is no room, drop the block; the next real read will
		// fetch it.
		gateRelease, gateOK := tryAcquireFUSEDataBackground()
		if !gateOK {
			metrics.Default().IncReadQoSPrefetchShed()
			continue
		}
		if rm.afterPrefetchGate != nil {
			rm.afterPrefetchGate()
		}
		// The nonblocking gate is owned even when offline/Stop wins after
		// acquisition. Keep it through an admitted ReadAt and release it once.
		if !rm.prefetchAllowed() {
			gateRelease()
			return
		}
		t0 := time.Now()
		n, err := rm.readBlock(fd, buf, off)
		gateRelease()
		if n > 0 {
			if mapping != nil {
				mapping.record(off, n)
			}
			rm.noteBlockRead(n, time.Since(t0))
		}
		if err != nil {
			break // EOF or error
		}
	}
}

// extendWindowLocked moves the windowed prefetch (ParallelFetch policies)
// forward after a qualifying sequential read that ended at readEnd. The window
// is whole blocks: it starts at the first block boundary at or after readEnd
// (the reader's own block is left to the reader) and covers policy.Blocks
// blocks, but not past a known end of file. Each block is claimed once, so no
// byte is fetched twice. If no round serves this tracker, one starts. Caller
// holds rm.mu.
func (rm *ReadaheadManager) extendWindowLocked(inode uint64, tracker *readTracker, readEnd int64, policy netprofile.ReadaheadPolicy, filePath string) {
	if tracker.windowEOF > 0 && readEnd > tracker.windowEOF {
		tracker.windowEOF = 0 // the file grew
	}
	start := alignUpBlock(readEnd)
	end := start + int64(policy.Blocks)*readaheadBlockSize
	if tracker.windowEOF > 0 {
		if limit := alignUpBlock(tracker.windowEOF); end > limit {
			end = limit
		}
	}
	if tracker.windowNext < start {
		// The reader passed blocks the window had not fetched yet; skip them.
		tracker.windowNext = start
	}
	if end > tracker.windowEnd {
		tracker.windowEnd = end
	}
	if tracker.pumping || tracker.windowNext >= tracker.windowEnd {
		return
	}
	if !tracker.sizeHintAt.IsZero() && time.Since(tracker.sizeHintAt) < rm.sizeHintTTL && tracker.windowNext >= tracker.sizeHint {
		return // at the end of the file as of a moment ago
	}
	tracker.pumping = true
	tracker.roundGen++
	gen := tracker.roundGen
	rm.statsMu.Lock()
	rm.triggered++
	rm.statsMu.Unlock()
	metrics.Default().IncReadaheadTriggered()
	rm.wg.Add(1)
	go func() {
		defer rm.wg.Done()
		rm.pumpWindow(inode, tracker, filePath, policy, gen)
	}()
}

// releaseRoundLocked clears pumping if round gen still owns it. Caller holds
// rm.mu.
func releaseRoundLocked(tr *readTracker, gen uint64) {
	if tr.pumping && tr.roundGen == gen {
		tr.pumping = false
	}
}

// clampWindowEOFLocked records an end of file at eof and stops the window
// there. Caller holds rm.mu.
func (rm *ReadaheadManager) clampWindowEOFLocked(tr *readTracker, eof int64) {
	tr.windowEOF = eof
	if limit := alignUpBlock(eof); tr.windowEnd > limit {
		tr.windowEnd = limit
	}
}

// windowBlockCap is the manager-wide limit on windowed blocks in flight: the
// policy's Workers, but at most one below the background gate share. On Slow
// that is min(4, 4-1) = 3. The gate admission (acquireWindowGate) enforces the
// same bound against everything in flight, foreground included.
func (rm *ReadaheadManager) windowBlockCap(pol netprofile.ReadaheadPolicy) int64 {
	if rm.windowCapOverride > 0 {
		return int64(rm.windowCapOverride)
	}
	limit := int64(pol.Workers)
	if bg := backgroundFUSEDataWidth() - 1; fuseDataGateEnabled && bg < limit {
		limit = bg
	}
	if limit > maxReadaheadWorkers {
		limit = maxReadaheadWorkers
	}
	if limit < 1 {
		limit = 1
	}
	return limit
}

// admitWindowBlock reserves one manager-wide windowed block slot.
func (rm *ReadaheadManager) admitWindowBlock(limit int64) bool {
	for {
		cur := rm.windowInflight.Load()
		if cur >= limit {
			return false
		}
		if rm.windowInflight.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

type windowResult struct {
	off int64
	n   int
	err error
	dur time.Duration
}

// acquireWindowGate admits one windowed block through the FUSE data gate.
// Background admission compares the TOTAL in flight (foreground included)
// against the background width; a windowed block is admitted only below that
// width minus one, so with any mix of foreground reads and readahead the last
// background-admissible slot stays free for the spool drainer, membuf, the
// sidecar warmer and metadata preparation, which poll for it.
func acquireWindowGate() (release func(), ok bool) {
	if !fuseDataGateEnabled {
		return func() {}, true
	}
	limit := backgroundFUSEDataWidth() - 1
	if limit < 1 {
		return nil, false
	}
	if tryAdmitFUSEData(limit) {
		return releaseFUSEData, true
	}
	return nil, false
}

// pumpWindow runs one bounded windowed round for a tracker. A round claims
// at most policy.Blocks blocks, then ends; the next qualifying read starts a
// new round. Bounding the round returns its Read-QoS bulk token and lets
// mapping preparation run for each round's chunks. The round gives up
// pumping as soon as it stops claiming, before it waits out its last blocks
// and before mapping preparation, so the next read can start the next round.
func (rm *ReadaheadManager) pumpWindow(inode uint64, tr *readTracker, filePath string, policy netprofile.ReadaheadPolicy, gen uint64) {
	defer func() {
		rm.mu.Lock()
		releaseRoundLocked(tr, gen)
		rm.mu.Unlock()
	}()
	rm.runRound(inode, filePath, policy.Workers, func(fd *os.File, mapping *readaheadMappingJob) {
		rm.windowBlocks(inode, tr, gen, fd, mapping, policy.Blocks)
	})
}

// windowBlocks runs the parallel block loop of one round. It keeps up to
// windowBlockCap blocks in flight (manager-wide), each a whole aligned block
// read with its own FUSE data slot (acquireWindowGate). It stops claiming
// when the round has claimed maxBlocks, the window is fetched, the reader
// seeks or goes idle (windowIdle), the file ends, the link leaves a
// ParallelFetch class, the cache binding shuts down, the gate is full, or
// offline/Stop. When it stops claiming it releases pumping in the same
// critical section as that decision where the decision depends on the
// window, so a read that extends the window right then starts a new round.
func (rm *ReadaheadManager) windowBlocks(inode uint64, tr *readTracker, gen uint64, fd *os.File, mapping *readaheadMappingJob, maxBlocks int) {
	done := make(chan windowResult, maxReadaheadWorkers)
	inflight, claimedTotal := 0, 0
	stopClaims := false
	sizeLimit := int64(-1) // fstat size, after the first admitted claim
	stop := func() {
		if !stopClaims {
			stopClaims = true
			rm.mu.Lock()
			releaseRoundLocked(tr, gen)
			rm.mu.Unlock()
		}
	}
	type span struct {
		off int64
		n   int
	}
	var spans []span
	defer func() {
		if mapping == nil {
			return
		}
		// Blocks finish out of order; record them in file order.
		sort.Slice(spans, func(i, j int) bool { return spans[i].off < spans[j].off })
		for _, sp := range spans {
			mapping.record(sp.off, sp.n)
		}
	}()
	for {
		claimed, off := false, int64(0)
		if !stopClaims {
			pol := rm.effectivePolicy()
			if !rm.prefetchAllowed() || !pol.ParallelFetch || claimedTotal >= maxBlocks || (mapping != nil && mapping.b.workCtx.Err() != nil) {
				stop()
			} else {
				limit := rm.windowBlockCap(pol)
				rm.mu.Lock()
				live := !rm.stopped && rm.trackers[inode] == tr && time.Since(tr.lastAccess) <= rm.windowIdle
				inFile := sizeLimit < 0 || tr.windowNext < sizeLimit
				switch {
				case live && inFile && tr.windowNext < tr.windowEnd && rm.admitWindowBlock(limit):
					off = tr.windowNext
					tr.windowNext += readaheadBlockSize
					claimed = true
					claimedTotal++
				case inflight == 0:
					// Nothing to claim (window fetched, end of file, reader
					// gone, or every windowed slot is taken by other streams):
					// end the round.
					stopClaims = true
					releaseRoundLocked(tr, gen)
				}
				rm.mu.Unlock()
			}
		}
		if stopClaims && inflight == 0 {
			return
		}

		if claimed {
			gateRelease, ok := acquireWindowGate()
			if ok && sizeLimit < 0 {
				// One fstat per round, after the first admitted claim, so a
				// gate-full round costs no metadata call. It bounds this
				// round only; the tracker keeps it as a short-lived hint.
				if fi, err := fd.Stat(); err == nil && fi.Mode().IsRegular() {
					sizeLimit = fi.Size()
					rm.mu.Lock()
					tr.sizeHint, tr.sizeHintAt = sizeLimit, time.Now()
					rm.mu.Unlock()
				}
				if sizeLimit >= 0 && off >= sizeLimit {
					gateRelease()
					ok = false
				}
			}
			if ok {
				inflight++
				go rm.windowBlock(fd, off, gateRelease, done)
				continue
			}
			// Gate full or past the end of file: give the block back so a
			// later round fetches it rather than skipping it, unless the
			// reader moved past it, and stop claiming.
			rm.windowInflight.Add(-1)
			claimedTotal--
			metrics.Default().IncReadQoSPrefetchShed()
			rm.mu.Lock()
			if tr.windowNext == off+readaheadBlockSize {
				tr.windowNext = off
			}
			stopClaims = true
			releaseRoundLocked(tr, gen)
			rm.mu.Unlock()
			if inflight == 0 {
				return
			}
		}

		r := <-done
		inflight--
		rm.windowInflight.Add(-1)
		if r.n > 0 {
			spans = append(spans, span{r.off, r.n})
			rm.noteBlockRead(r.n, r.dur)
		}
		if r.err != nil {
			if errors.Is(r.err, io.EOF) {
				rm.mu.Lock()
				rm.clampWindowEOFLocked(tr, r.off+int64(r.n))
				rm.mu.Unlock()
			}
			stop() // EOF, error, or offline/Stop after admission
		}
	}
}

// windowBlock reads one aligned block while holding a background gate slot,
// and releases the slot exactly once.
func (rm *ReadaheadManager) windowBlock(fd *os.File, off int64, gateRelease func(), done chan<- windowResult) {
	if rm.afterPrefetchGate != nil {
		rm.afterPrefetchGate()
	}
	if !rm.prefetchAllowed() {
		gateRelease()
		done <- windowResult{off: off, err: errPrefetchStopped}
		return
	}
	bp := windowBufPool.Get().(*[]byte)
	t0 := time.Now()
	n, err := rm.readBlock(fd, *bp, off)
	dur := time.Since(t0)
	gateRelease()
	windowBufPool.Put(bp)
	if err == nil && n < readaheadBlockSize {
		err = io.EOF
	}
	done <- windowResult{off: off, n: n, err: err, dur: dur}
}

// Stats returns readahead statistics.
func (rm *ReadaheadManager) Stats() (triggered, prefetched int64) {
	rm.statsMu.Lock()
	defer rm.statsMu.Unlock()
	return rm.triggered, rm.prefetched
}

// cleanupLoop removes stale tracker entries periodically.
func (rm *ReadaheadManager) cleanupLoop() {
	ticker := time.NewTicker(trackerTTL)
	defer ticker.Stop()
	for {
		select {
		case <-rm.stopCh:
			return
		case <-ticker.C:
			now := time.Now()
			rm.mu.Lock()
			for inode, t := range rm.trackers {
				if now.Sub(t.lastAccess) > trackerTTL {
					delete(rm.trackers, inode)
				}
			}
			rm.mu.Unlock()
		}
	}
}

// Stop shuts down the readahead manager.
func (rm *ReadaheadManager) Stop() {
	rm.beginPreviewStop()
	rm.stopOnce.Do(func() {
		// Serialize the stopped transition with OnRead's WaitGroup.Add. Once
		// stopped is visible, no new prefetch goroutine can be registered, so
		// waiting for the existing set is valid and teardown cannot leak work
		// into the next server/test instance.
		rm.mu.Lock()
		rm.stopped = true
		close(rm.stopCh)
		rm.mu.Unlock()
		rm.wg.Wait()
		triggered, prefetched := rm.Stats()
		log.Printf("readahead: stopped (triggered=%d, prefetched=%d blocks)",
			triggered, prefetched)
	})
}
