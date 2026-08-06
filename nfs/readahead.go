package nfs

import (
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

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
)

// ReadaheadManager detects sequential read patterns and triggers
// background prefetch to warm the JuiceFS SSD cache.
type ReadaheadManager struct {
	fusePath string
	fdPool   *FDPool // reuse pooled fds instead of opening new ones

	// profile drives link-aware prefetch policy (depth/width/enable) and is fed
	// throughput samples from prefetch block reads. nil → historical defaults
	// (medium policy), so an unwired manager behaves exactly as before.
	profile *netprofile.Profile

	mu       sync.Mutex
	trackers map[uint64]*readTracker // inode → tracker

	// Semaphore for concurrent readahead goroutines (sized to the fast-link max;
	// the per-link policy.Workers caps how many slots are actually used).
	sem    chan struct{}
	stopCh chan struct{}

	// activePrefetch is the live count of in-flight prefetch goroutines, gated
	// against the current policy's Workers so a slow link stays gentle.
	activePrefetch atomic.Int64

	// Stats
	statsMu    sync.Mutex
	triggered  int64
	prefetched int64
}

// effectivePolicy returns the link-aware readahead policy, falling back to the
// historical hard-coded defaults when no profile is wired (nil-safe).
func (rm *ReadaheadManager) effectivePolicy() netprofile.ReadaheadPolicy {
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
	lastOffset     int64
	sequentialHits int
	lastAccess     time.Time
	prefetchedTo   int64 // highest offset we've already prefetched to
}

func NewReadaheadManager(fusePath string, fdPool *FDPool, profile *netprofile.Profile) *ReadaheadManager {
	rm := &ReadaheadManager{
		fusePath: fusePath,
		fdPool:   fdPool,
		profile:  profile,
		trackers: make(map[uint64]*readTracker),
		sem:      make(chan struct{}, maxReadaheadWorkers),
		stopCh:   make(chan struct{}),
	}
	go rm.cleanupLoop()
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
// there (0) so LAN prefetch fires exactly as before; only the slow WAN class
// tightens, where an over-pull crosses the tunnel and costs seconds. ClassMetered
// never reaches the trip at all (its policy is Enabled:false — the OnRead early
// return above), so the guard is moot there. The value is set ABOVE the slow
// class's SeqThreshold (4) so it actually creates a suppression band (runs of
// 4–5 blocks are held; 6+ escalate) rather than being inert. nil profile ⇒
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
	// S2 kill-switch (WAVE 1): default-on, env-disable. Off the per-RPC FUSE
	// path — a single env read + early return, no lock taken when disabled.
	if !serverReadaheadEnabled() {
		return
	}

	policy := rm.effectivePolicy()

	rm.mu.Lock()
	defer rm.mu.Unlock()

	tracker, ok := rm.trackers[inode]
	if !ok {
		tracker = &readTracker{}
		rm.trackers[inode] = tracker
	}

	tracker.lastAccess = time.Now()

	// Check if this read is sequential (follows the previous read)
	expectedOffset := tracker.lastOffset + readaheadBlockSize
	// Allow some tolerance for reads within the same block or slightly ahead
	isSequential := offset >= tracker.lastOffset && offset <= expectedOffset+readaheadBlockSize

	if isSequential && offset > tracker.lastOffset {
		tracker.sequentialHits++
	} else {
		tracker.sequentialHits = 0
	}
	tracker.lastOffset = offset

	// Metered/weak link: do NOT prefetch ahead at all. We still keep tracking the
	// pattern (cheap) so that if the link recovers and reclassifies, the very next
	// read can trigger — but on a metered link an xattr/Quick-Look probe must
	// never escalate to a whole-file pull (docs/TUNING/01-bandwidth §Read amp).
	if !policy.Enabled {
		return
	}

	// Trigger readahead after the link-aware threshold of consecutive sequential reads
	if tracker.sequentialHits >= policy.SeqThreshold {
		// S2 short-run guard (WAVE 1, RC-4): a SHORT sequential run on a WAN link
		// is a Finder preview probe (a few 256KiB subreads of one file), not a
		// real large sequential reel read. Escalating it to the policy's 64MB
		// window whole-file-pulls across the tunnel for nothing. If the run so
		// far is below the class-scaled small-preview threshold, cap (skip) this
		// round instead — the run keeps accumulating, so a GENUINE sequential
		// read still escalates once it grows past the threshold. This is a plain
		// comparison on the already-tracked sequentialHits — NO new syscall.
		// Disabled (threshold 0) on Fast/Medium, so 10GbE/GbE are unchanged.
		if guard := smallPreviewRunBlocks(rm.profile); guard > 0 && tracker.sequentialHits < guard {
			metrics.Default().IncReadaheadSuppressed()
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
		go rm.prefetch(filePath, prefetchStart, prefetchEnd, policy.Workers)
	}
}

// prefetch reads blocks from the JuiceFS FUSE mount to warm the SSD cache.
// maxWorkers caps concurrency to the current link policy (the semaphore is sized
// to the fast-link max; a slow link uses fewer of its slots).
func (rm *ReadaheadManager) prefetch(filePath string, start, end int64, maxWorkers int) {
	// Gate against the link-aware worker budget BEFORE taking a semaphore slot.
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	if rm.activePrefetch.Add(1) > int64(maxWorkers) {
		rm.activePrefetch.Add(-1)
		return // over the per-link worker budget — shed this round
	}
	defer rm.activePrefetch.Add(-1)

	select {
	case rm.sem <- struct{}{}:
		defer func() { <-rm.sem }()
	default:
		return // all workers busy, skip this round
	}

	select {
	case <-rm.stopCh:
		return
	default:
	}

	// Read-QoS (#4, INSTANT-NAV): prefetch is the LOWEST read class. On
	// slow/metered links it only runs when the bulk lane has a free token,
	// and holds it for the whole block loop below — a contended lane sheds
	// the round entirely (it re-triggers on the next sequential read),
	// handing the bandwidth back to interactive reads. Inert on medium/fast.
	qosRelease, qosOK := defaultReadQoS.tryAcquireBulk()
	if !qosOK {
		return
	}
	defer qosRelease()

	fusePath := rm.fusePath + "/" + filePath

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

	buf := make([]byte, readaheadBlockSize)
	for off := start; off < end; off += readaheadBlockSize {
		select {
		case <-rm.stopCh:
			return
		default:
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
		t0 := time.Now()
		n, err := fd.ReadAt(buf, off)
		gateRelease()
		if n > 0 {
			rm.statsMu.Lock()
			rm.prefetched++
			rm.statsMu.Unlock()
			// WAVE 0 (grades S2): accumulate blocks actually prefetched.
			metrics.Default().AddReadaheadPrefetchedBlocks(1)
			// Feed the link estimator. A cold block is a real backend transfer
			// (slow); a cache hit is sub-ms and gets filtered out inside
			// ObserveThroughput, so only wire-speed samples move the estimate.
			if rm.profile != nil {
				rm.profile.ObserveThroughput(int64(n), time.Since(t0))
			}
		}
		if err != nil {
			break // EOF or error
		}
	}
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
	close(rm.stopCh)
	log.Printf("readahead: stopped (triggered=%d, prefetched=%d blocks)",
		rm.triggered, rm.prefetched)
}
