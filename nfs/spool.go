package nfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/metadata"
)

// SpoolFilesSubdir is the sub-directory under the spool root that holds
// the on-disk write spool files. The root may also hold sibling dirs
// (`quarantine/`, `manifest.log`) added by later slices.
const SpoolFilesSubdir = "files"

// ErrSpoolFull is returned by OpenWrite/WriteAt when the spool's capacity
// cap would be exceeded. It WRAPS syscall.ENOSPC so the RPC boundary maps
// it to NFS3ERR_NOSPC and clients see a clean, actionable "disk full"
// instead of a generic I/O error: internal/nfs cannot import this package
// (cycle), so the shared sentinel both layers agree on is the syscall
// errno itself — nfsStatusErrorFrom matches it with errors.Is.
// errors.Is(err, ErrSpoolFull) continues to work by identity.
var ErrSpoolFull = fmt.Errorf("spool: capacity full: %w", syscall.ENOSPC)

// ErrSpoolBusy is returned by OpenWrite when a previous entry for the same
// path was finalized but is still draining and did not clear within the
// bounded wait. The handler maps this to a retryable NFS status so the
// client retries (by which point the drain has almost always completed).
var ErrSpoolBusy = errors.New("spool: path busy (prior entry still draining)")

// DefaultStuckEscalationWindow is how long an entry may sit QUIESCENT (no
// writes) with refcount>0 before the sweeper treats the handles as leaked
// and force-finalizes the entry. Far above any legitimate gap between two
// WRITE RPCs on one file (per-RPC handles live for the duration of a single
// WriteAt), far below "stuck forever". See sweepOnce.
const DefaultStuckEscalationWindow = 10 * time.Minute

// SpoolStore is the high-level write-spool API.
//
// Owns:
//   - the spool root directory on local SSD
//   - the in-memory index (path → entry)
//   - the SQLite-backed durable index (via metadata.SpoolStore)
//   - the capacity budget (used bytes vs cap bytes)
//
// Threadsafe — OpenWrite and lookups can race freely. Per-entry state is
// guarded by the entry's own mutex.
type SpoolStore struct {
	root     string
	capacity int64 // 0 means unlimited
	used     atomic.Int64
	meta     *metadata.SpoolStore
	index    *SpoolIndex
	closed   atomic.Bool
	// openShards serializes OpenWrite per-path. Sharded by path hash so a
	// slow create (the new-file path holds its shard across s.meta.Insert +
	// os.OpenFile) blocks only concurrent opens of the SAME file, never
	// writes to other files. Before sharding this was a single global mutex
	// taken on EVERY write RPC (NFS does OpenFile→WriteAt→Close per WRITE);
	// when a new-file Insert stalled behind a reconcile's SQLite work it
	// froze every in-flight copy and tripped the soft-mount timeout
	// (Finder "error 100060" under parallel copy — caught mid-stall via
	// pprof: 12 onWrite handlers blocked on OpenWrite's lock). Same path
	// always maps to the same shard, so the check-then-create dedup is
	// preserved exactly. MigrateForRename locks ALL shards (rare full
	// barrier) since a directory rename re-keys many paths at once.
	openShards [spoolOpenShards]sync.Mutex

	// wakeDrainer is guarded by wakeMu so concurrent SetDrainerWake and
	// signalReady can't race on the func pointer.
	wakeMu      sync.RWMutex
	wakeDrainer func()

	// manifest is the append-only JSONL audit log under <root>/manifest.log.
	// May be nil if open failed at construction; that case becomes a
	// no-op for the audit path (drain still proceeds normally).
	manifest *manifestWriter

	// escalateAfter is the quiescence window after which a refcount>0
	// entry is treated as handle-leaked and force-finalized by the
	// sweeper. Set once at construction (DefaultStuckEscalationWindow);
	// tests shorten it via SetStuckEscalationWindow before concurrency
	// starts. <=0 disables escalation.
	escalateAfter time.Duration
}

// spoolOpenShards is the number of per-path OpenWrite locks. 64 keeps
// cross-file collision probability negligible for realistic parallel-copy
// fan-out (Finder/ditto rarely exceed a handful of concurrent files) while
// costing only 64 mutexes per store. Must stay a power-of-two-friendly small
// constant; the FNV-1a hash below maps a path to its shard.
const spoolOpenShards = 64

// pathShard returns the per-path OpenWrite mutex. Allocation-free (inline
// FNV-1a over the path bytes) so it stays cheap on the per-RPC write hot
// path — the QA-35 perf-discipline gate forbids per-RPC allocation here.
func (s *SpoolStore) pathShard(path string) *sync.Mutex {
	var h uint32 = 2166136261
	for i := 0; i < len(path); i++ {
		h ^= uint32(path[i])
		h *= 16777619
	}
	return &s.openShards[h%spoolOpenShards]
}

// lockAllShards / unlockAllShards take every OpenWrite shard in index order,
// making the caller mutually exclusive with all concurrent OpenWrites — the
// pre-sharding global-mutex behavior, used only by the rare rename barrier.
// Ascending order is deadlock-free vs OpenWrite (which holds at most one
// shard) and vs another all-shards caller (same acquisition order).
func (s *SpoolStore) lockAllShards() {
	for i := range s.openShards {
		s.openShards[i].Lock()
	}
}

func (s *SpoolStore) unlockAllShards() {
	for i := range s.openShards {
		s.openShards[i].Unlock()
	}
}

// SpoolFreeFloorBytes is the disk space the spool leaves free when
// auto-sizing or clamping its capacity, so the OS and the JuiceFS cache that
// shares the same SSD always have headroom. Mirrors the 10 GiB cache floor in
// health/fuse.go but is larger because the spool can hold an entire un-drained
// SD-card burst.
const SpoolFreeFloorBytes = int64(20) << 30 // 20 GiB

// spoolDiskAvail returns the bytes available to this user on the filesystem
// backing dir. dir may not exist yet (NewSpoolStore creates it after the
// config layer computes the default), so it walks up to the nearest existing
// ancestor — any path on the same volume reports the same free space.
func spoolDiskAvail(dir string) (int64, error) {
	for d := dir; ; {
		var st syscall.Statfs_t
		if err := syscall.Statfs(d, &st); err == nil {
			return int64(st.Bavail) * int64(st.Bsize), nil
		}
		parent := filepath.Dir(d)
		if parent == d {
			return 0, fmt.Errorf("spool: statfs: no accessible ancestor of %s", dir)
		}
		d = parent
	}
}

// AutoSpoolCapacity is the default spool capacity when JM_SPOOL_SIZE_GB is
// unset: free disk minus the floor, so the spool sizes to the machine instead
// of a fixed 50 GiB that a large SD-card offload (e.g. an 87 GB RAW shoot)
// would overflow mid-copy. Falls back to 50 GiB if Statfs fails, and never
// returns below 8 GiB. Callers pass the result as NewSpoolStore's capacity.
func AutoSpoolCapacity(dir string) int64 {
	avail, err := spoolDiskAvail(dir)
	if err != nil || avail <= 0 {
		return int64(50) << 30
	}
	c := avail - SpoolFreeFloorBytes
	if c < int64(8)<<30 {
		c = int64(8) << 30
	}
	return c
}

// NewSpoolStore creates the spool root if it doesn't exist and returns an
// empty store. It does NOT recover prior on-disk state — call Recover for
// that (Slice F adds the recovery scrubber; for Slice A this is a no-op).
//
// capacity is in bytes; 0 means unlimited. A positive capacity is clamped to
// the actual free disk (minus SpoolFreeFloorBytes) so a logical budget larger
// than the spool SSD cannot cause a kernel ENOSPC mid-copy — the budget is
// otherwise blind to physical space. meta is the SQLite-backed index (callers
// should have called metadata.InitSpoolSchema on the underlying db first).
func NewSpoolStore(root string, capacity int64, meta *metadata.SpoolStore) (*SpoolStore, error) {
	if root == "" {
		return nil, fmt.Errorf("spool: root path is required")
	}
	if meta == nil {
		return nil, fmt.Errorf("spool: metadata.SpoolStore is required")
	}
	filesDir := filepath.Join(root, SpoolFilesSubdir)
	if err := os.MkdirAll(filesDir, 0o755); err != nil {
		return nil, fmt.Errorf("spool: mkdir %s: %w", filesDir, err)
	}
	if capacity > 0 {
		if avail, err := spoolDiskAvail(root); err == nil && avail > 0 {
			if maxCap := avail - SpoolFreeFloorBytes; maxCap > 0 && capacity > maxCap {
				jmlog.Warn("spool: capacity clamped to free disk",
					"requested_gb", capacity>>30, "clamped_gb", maxCap>>30,
					"free_gb", avail>>30)
				capacity = maxCap
			}
		}
	}
	s := &SpoolStore{
		root:          root,
		capacity:      capacity,
		meta:          meta,
		index:         NewSpoolIndex(),
		escalateAfter: DefaultStuckEscalationWindow,
	}
	if mw, err := newManifestWriter(root); err != nil {
		// Non-fatal — manifest is audit-only. Log and proceed.
		log.Printf("spool: manifest writer disabled: %v", err)
	} else {
		s.manifest = mw
	}
	return s, nil
}

// SetDrainerWake registers a callback invoked when an entry transitions
// to ready (i.e. on Close). The drainer (slice B) uses this to avoid
// polling — it sleeps on a signal channel and wakes when there's work.
// Calling with nil clears the callback. Safe to call concurrently with
// in-flight signalReady invocations.
func (s *SpoolStore) SetDrainerWake(fn func()) {
	s.wakeMu.Lock()
	s.wakeDrainer = fn
	s.wakeMu.Unlock()
}

// Root returns the spool root directory.
func (s *SpoolStore) Root() string { return s.root }

// Capacity returns (used, total) bytes. total=0 means unlimited.
func (s *SpoolStore) Capacity() (used, total int64) {
	return s.used.Load(), s.capacity
}

// Index returns the in-memory lookup table. Exposed so the NFS handler
// (slice D) can take O(1) lookups directly without going through the
// SpoolStore method surface on the hot path.
func (s *SpoolStore) Index() *SpoolIndex { return s.index }

// OpenWrite creates a new spool entry for nfsPath in `writing` state.
// Returns ErrSpoolFull if the capacity budget is already exhausted.
//
// Concurrent OpenWrite for the same nfsPath is serialized by its path shard; if
// the index already has an entry for this path, that entry is returned
// directly (same-path-reopen). This matches the FDPool same-path-dedupe
// semantics so a single Finder copy's multi-RPC write lifecycle reuses
// one spool file.
func (s *SpoolStore) OpenWrite(nfsPath string) (*SpoolEntry, error) {
	// reopenPoll/reopenMaxWait bound the rare wait for the case where a
	// PREVIOUS entry for this exact path was finalized but the drainer
	// hasn't evicted it yet. Reusing a finalized entry would error
	// (write-to-closed); creating a SECOND live entry would race two drains
	// to the same FUSE dest (Finding 4). So we wait for the drain to evict
	// it, then create fresh. The common path — no entry, or an active
	// `writing` entry to reuse — never waits.
	const reopenPoll = 10 * time.Millisecond
	const reopenMaxWait = 30 * time.Second
	var waited time.Duration

	// capStart marks when this open first hit a full spool. The backpressure
	// loop below throttles (polls for the drainer to free headroom) until
	// capacityWaitDeadline elapses, then fails the create with ErrSpoolFull.
	var capStart time.Time

	// Same path always maps to the same shard, so the check-then-create
	// dedup below is serialized exactly as the old global openMu did; opens
	// of OTHER paths now run on other shards concurrently.
	shard := s.pathShard(nfsPath)

	for {
		if s.closed.Load() {
			return nil, fmt.Errorf("spool: store is closed")
		}
		shard.Lock()

		if existing, ok := s.index.Lookup(nfsPath); ok {
			existing.mu.Lock()
			if !existing.closed {
				// Active writing entry: a per-RPC reopen during the same
				// write session, or the create→first-write transition.
				// Reuse it; bump the handle count and touch lastWrite so
				// the sweeper won't finalize it out from under this open.
				existing.refcount++
				existing.lastWrite.Store(time.Now().UnixNano())
				existing.mu.Unlock()
				shard.Unlock()
				return existing, nil
			}
			// Finalized-but-not-yet-drained entry holds this path. Don't
			// reuse (writes would fail) and don't create a competing entry
			// (dup drain). Wait for the drainer to evict it.
			existing.mu.Unlock()
			shard.Unlock()
			if waited >= reopenMaxWait {
				return nil, ErrSpoolBusy
			}
			time.Sleep(reopenPoll)
			waited += reopenPoll
			continue
		}

		// No entry for this path — create a fresh one under the path shard.
		if s.capacity > 0 && s.used.Load() >= s.capacity {
			// Spool full. Release the shard and THROTTLE — poll for the drainer
			// to free headroom rather than hard-fail the create, so a large copy
			// paces itself to drain throughput (the copy slows) instead of
			// aborting the whole Finder copy with NOSPC ("disk full"). Polling
			// and re-checking each tick (vs one fixed wait) is fair under many
			// concurrent full-spool opens — none starves on a lost race, since
			// every waiter keeps re-checking as the drainer frees slots. Bounded
			// by capacityWaitDeadline (well under the soft-mount timeout) so the
			// stall never trips ETIMEDOUT (error 100060); only a genuinely
			// stalled drain reaches the deadline and fails with ErrSpoolFull.
			shard.Unlock()
			if capStart.IsZero() {
				capStart = time.Now()
			}
			if s.closed.Load() || time.Since(capStart) >= capacityWaitDeadline {
				return nil, ErrSpoolFull
			}
			time.Sleep(capacityWaitPoll)
			continue
		}

		// Spool file basename: SHA-256(nfs_path) hex prefix + a microsecond
		// timestamp. SHA prefix avoids filesystem-path-character issues
		// (slashes, spaces, unicode). Timestamp suffix avoids collisions on
		// rapid re-opens after a Delete.
		h := sha256.Sum256([]byte(nfsPath))
		basename := hex.EncodeToString(h[:8]) + fmt.Sprintf("-%d", time.Now().UnixMicro())
		spoolFile := filepath.Join(s.root, SpoolFilesSubdir, basename)

		id, err := s.meta.Insert(nfsPath, spoolFile)
		if err != nil {
			shard.Unlock()
			return nil, err
		}

		f, err := os.OpenFile(spoolFile, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			// Roll back the SQL insert. We DELETE rather than MarkFailed:
			// nothing happened on disk to preserve, and leaving a `failed`
			// row would let a persistent disk failure (full / permissions)
			// grow spool_entries unboundedly because LookupByPath ignores
			// failed rows and every retry would Insert a new one.
			_ = s.meta.Delete(id)
			shard.Unlock()
			return nil, fmt.Errorf("spool: open file: %w", err)
		}

		entry := &SpoolEntry{
			id:        id,
			nfsPath:   nfsPath,
			spoolFile: spoolFile,
			file:      f,
			hasher:    sha256.New(),
			hashValid: true,
			store:     s,
			refcount:  1,
		}
		entry.lastWrite.Store(time.Now().UnixNano())
		s.index.Insert(nfsPath, entry)
		shard.Unlock()
		return entry, nil
	}
}

// sweepOnce finalizes every active `writing` entry that has no open write
// handles and has been quiescent for at least `idle`. Returns the number
// finalized. This is the NFS-compatible replacement for "finalize on Close":
// the per-RPC write path releases handles without finalizing, and this sweep
// (run on a timer by StartSweeper, or directly in tests) ends the file once
// the writer goes idle. Mirrors FDPool.evictLoop.
//
// Escalation (Phase-1 BUG 2): an entry that still holds write handles
// (refcount>0) but has been QUIESCENT far past s.escalateAfter can only be
// the victim of a leaked handle — NFS per-RPC handles live for the duration
// of a single WriteAt, so refcount>0 across minutes of zero writes means an
// error path dropped a billy.File without Close. Pre-escalation behavior was
// a silent skip forever: never finalized, never drained, capacity leaked,
// path phantom-stat'able (43 entries × 5+ hours, 2026-06-08). The
// escalation force-finalizes loudly — the bytes on the spool SSD are exactly
// what a normal idle finalize would have persisted, so finalize+drain
// preserves user data where a fail-and-discard would lose it. Genuinely
// active long writes are immune: continuous writes keep lastWrite fresh, so
// the entry is never quiescent for the full window.
func (s *SpoolStore) sweepOnce(idle time.Duration) int {
	entries := s.index.Snapshot()
	n := 0
	for _, e := range entries {
		if e.finalizeIfIdle(idle) {
			n++
			continue
		}
		if e.escalateIfStuck(s.escalateAfter) {
			n++
		}
	}
	return n
}

// StartSweeper launches the background idle-finalize loop and returns a stop
// function. idle is how long an entry must be quiescent (no open handles, no
// writes) before it is finalized and handed to the drainer; tick is the scan
// cadence. Defaults: idle=3s, tick=1s. Safe to call once.
func (s *SpoolStore) StartSweeper(idle, tick time.Duration) (stop func()) {
	if idle <= 0 {
		idle = 3 * time.Second
	}
	if tick <= 0 {
		tick = 1 * time.Second
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(tick)
		defer t.Stop()
		// QA-38: garbage-collect finished `done` spool rows. DeleteDone was
		// defined but never scheduled, so the table grew unbounded (44k+
		// rows). The /spool status poll then scanned and allocated every row
		// on each poll, burning multiple cores + GC and starving the NFS
		// handlers until the mount wedged and writes hit the soft-mount
		// timeout (Finder "error 100060"). Keep done rows only long enough
		// for the status tail window plus margin, then delete; clear any
		// accumulated backlog once on start.
		const gcInterval = 60 * time.Second
		const doneRetention = 10 * time.Minute
		runGC := func() {
			if n, err := s.meta.DeleteDone(time.Now().Add(-doneRetention)); err != nil {
				jmlog.Warn("spool done-GC failed", "err", err)
			} else if n > 0 {
				jmlog.Info("spool done-GC", "deleted", n)
			}
		}
		runGC()
		gcTick := time.NewTicker(gcInterval)
		defer gcTick.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				s.sweepOnce(idle)
			case <-gcTick.C:
				runGC()
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

// SetStuckEscalationWindow overrides the quiescence window after which a
// refcount>0 entry is force-finalized (DefaultStuckEscalationWindow). Test
// hook — call before the sweeper or any writers start. <=0 disables.
func (s *SpoolStore) SetStuckEscalationWindow(d time.Duration) {
	s.escalateAfter = d
}

// LookupActive returns the in-memory index entry for nfsPath if one
// exists. O(1). Used by the read path in slice D.
func (s *SpoolStore) LookupActive(nfsPath string) (*SpoolEntry, bool) {
	return s.index.Lookup(nfsPath)
}

// Meta returns the underlying SQLite-backed index. Used by the drainer
// (slice B) which lives below the in-memory index abstraction.
func (s *SpoolStore) Meta() *metadata.SpoolStore { return s.meta }

// Stop closes the store. In-flight entries are NOT auto-closed — callers
// must close them first. Idempotent.
func (s *SpoolStore) Stop() {
	s.closed.Store(true)
	if s.manifest != nil {
		_ = s.manifest.Close()
	}
}

// Manifest returns the audit log writer, or nil if it failed to open.
// Exposed for tests and for the drainer (which appends per disposition).
func (s *SpoolStore) Manifest() *manifestWriter { return s.manifest }

// RecoveryReport summarizes what the boot scrubber did. Returned by
// RecoverOnBoot for log aggregation + admin UX.
type RecoveryReport struct {
	// OrphanFilesDeleted: files in the spool dir with no SQL row →
	// removed. Cleans up half-created files from a crash between
	// metadata.Insert and the first WriteAt.
	OrphanFilesDeleted int

	// OrphanRowsFailed: SQL rows in `ready` or `draining` state whose
	// spool file is no longer on disk → MarkFailed (no data to retry).
	OrphanRowsFailed int

	// WritingFailedRows: rows in `writing` state at boot time with NO
	// recoverable data (missing/empty/unreadable spool file) → MarkFailed +
	// delete the partial file.
	WritingFailedRows int

	// WritingResumed: rows in `writing` state at boot time whose spool file
	// holds real data → finalized to `ready` (hashed from disk) and drained,
	// PRESERVING the bytes the client wrote (and may have COMMITted) rather
	// than discarding acknowledged data. The drainer verifies against the
	// disk-derived sha.
	WritingResumed int

	// DrainingReset: rows in `draining` state with spool file present
	// → ResetToReady so the drainer re-attempts the copy.
	DrainingReset int

	// ReadyResumed: rows in `ready` state with spool file present →
	// accounted against the capacity counter; drainer will pick them
	// up on next Start. No SQL state change.
	ReadyResumed int

	// FailedResumed: rows in `failed` state whose spool file is still an
	// INTACT copy (on-disk size == row.Size, row.Size > 0) → reset to
	// `ready` with a fresh attempt budget and re-driven. `failed` is
	// reached by failPermanent after a transient/infra error (FUSE
	// unmounted → "device not configured", MinIO down, disk full); the
	// spool file is the LAST durable copy of the user's photo/video and
	// must never be deleted. Auto-recovering on boot means a restart
	// re-drives it now that the destination is back — the "we can't lose
	// photos" invariant. (A genuine corruption goes to quarantine, whose
	// file lives outside files/ and is not seen here.)
	FailedResumed int
}

// RecoverOnBoot reconciles on-disk spool files against the SQL index.
// Call once after NewSpoolStore + InitSpoolSchema and BEFORE
// drainer.Start, ideally as part of the same critical section as
// SetSpool — the method is NOT safe to run concurrently with active
// writers or the drainer.
//
// State transition rules:
//
//	file orphan (no SQL row)        → delete file
//	row.writing  → mark failed + delete (best-effort) the partial file
//	row.ready    → file present  : add bytes to capacity counter, leave state
//	             → file missing  : mark failed
//	row.draining → file present  : reset to ready (drainer retries)
//	             → file missing  : mark failed
//	row.done     → file present  : reclaim stale leftover (already in MinIO)
//	row.failed   → intact copy (size==row.Size>0): reset to ready + re-drive
//	             → writing-crash partial (row.Size==0): reclaim
//	             → size mismatch / missing: preserve (never delete >0 bytes)
//
// Files inside spool_root/quarantine/ are forensic state preserved by
// the drainer's SHA-mismatch path; they are NOT touched here.
//
// The in-memory index (s.index) is NOT repopulated. `writing`-state
// entries are gone (we marked them failed); reads of those paths will
// fall through to FUSE. `ready`/`draining` paths haven't reached FUSE
// yet either, so reads briefly return ENOENT until the drainer copies
// them in. Same window as slice C's known limitation.
//
// Errors from ListAll or ReadDir are returned; per-row state-transition
// errors are logged but do not abort the reconciliation.
func (s *SpoolStore) RecoverOnBoot(ctx context.Context) (RecoveryReport, error) {
	var report RecoveryReport

	// Slice-F reviewer HIGH fix: enforce the no-concurrent-drainer
	// invariant at runtime. wakeDrainer is set exactly once by
	// drainer.Start via SetDrainerWake; if it's already non-nil here,
	// the drainer is alive and a concurrent releaseCapacity /
	// tryReserveCapacity will race with our Store(0) below. Panic
	// rather than silently corrupt the counter.
	s.wakeMu.RLock()
	wakeSet := s.wakeDrainer != nil
	s.wakeMu.RUnlock()
	if wakeSet {
		panic("nfs/spool: RecoverOnBoot called after drainer.Start — must run before SetSpool/Start")
	}

	// Reset the capacity counter before re-accounting. Idempotent
	// recovery: a second call must produce the same `used` value as
	// the first, not a doubled one. Safe because the guard above
	// proves no drainer / writer is mutating s.used concurrently.
	s.used.Store(0)

	rows, err := s.meta.ListAll()
	if err != nil {
		return report, fmt.Errorf("recover: list rows: %w", err)
	}

	// Build the set of file paths SQL knows about so we can detect
	// orphan files in one pass.
	expectedFiles := make(map[string]struct{}, len(rows))
	for _, r := range rows {
		expectedFiles[r.SpoolFile] = struct{}{}
	}

	// Scan the files dir. We deliberately do NOT recurse — the
	// quarantine subdir (sibling under spool_root) is left alone, and
	// the files dir is flat.
	filesDir := filepath.Join(s.root, SpoolFilesSubdir)
	actualFiles := make(map[string]bool)
	if dirents, err := os.ReadDir(filesDir); err == nil {
		for _, d := range dirents {
			if d.IsDir() {
				continue
			}
			full := filepath.Join(filesDir, d.Name())
			actualFiles[full] = true
			if _, ok := expectedFiles[full]; !ok {
				if rmErr := os.Remove(full); rmErr == nil {
					report.OrphanFilesDeleted++
					log.Printf("spool recover: orphan file deleted: %s", full)
				} else {
					log.Printf("spool recover: orphan file remove failed: %s: %v", full, rmErr)
				}
			}
		}
	} else if !os.IsNotExist(err) {
		// Note: missing files dir is fine on fresh install. Other
		// errors (permission etc) are unusual; log + continue with
		// SQL reconciliation only.
		log.Printf("spool recover: readdir %s: %v", filesDir, err)
	}

	// Reconcile per-row.
	for _, r := range rows {
		if ctx.Err() != nil {
			return report, ctx.Err()
		}
		fileExists := actualFiles[r.SpoolFile]
		switch r.DrainState {
		case metadata.DrainWriting:
			// RESUME the written data instead of discarding it. A crash / power
			// loss after the client COMMITted (onCommit now fsyncs the spool
			// file) must not lose acknowledged bytes — so finalize the on-disk
			// file to `ready` and let the drainer copy+verify it to MinIO. The
			// stored sha is the on-disk hash, making the drain self-consistent.
			// A genuinely partial (uncommitted) file is preserved too; the user
			// sees it and re-copies, which overwrites it — strictly safer than
			// silently deleting data the app reported as copied. Only a
			// missing / empty / unreadable file is failed (no recoverable data).
			var sha []byte
			var sz int64
			var hErr error
			if fileExists {
				sha, sz, hErr = hashSpoolFile(r.SpoolFile)
			}
			if !fileExists || hErr != nil || sz == 0 {
				if _, mErr := s.meta.MarkFailed(r.ID, "writing-state row recovered after crash (no recoverable data)"); mErr != nil {
					log.Printf("spool recover: mark writing→failed %d: %v", r.ID, mErr)
					continue
				}
				if fileExists {
					if rmErr := os.Remove(r.SpoolFile); rmErr != nil && !os.IsNotExist(rmErr) {
						log.Printf("spool recover: writing partial file remove %s: %v", r.SpoolFile, rmErr)
					}
				}
				report.WritingFailedRows++
			} else if mErr := s.meta.MarkReady(r.ID, sz, sha); mErr != nil {
				log.Printf("spool recover: resume writing→ready %d: %v", r.ID, mErr)
				continue
			} else {
				s.used.Add(sz)
				report.WritingResumed++
			}

		case metadata.DrainReady:
			if !fileExists {
				if _, mErr := s.meta.MarkFailed(r.ID, "ready-state row missing spool file after crash"); mErr != nil {
					log.Printf("spool recover: mark ready→failed %d: %v", r.ID, mErr)
					continue
				}
				report.OrphanRowsFailed++
			} else {
				// File present; ready state preserved. Account bytes.
				s.used.Add(r.Size)
				report.ReadyResumed++
			}

		case metadata.DrainDraining:
			if !fileExists {
				if _, mErr := s.meta.MarkFailed(r.ID, "draining-state row missing spool file after crash"); mErr != nil {
					log.Printf("spool recover: mark draining→failed %d: %v", r.ID, mErr)
					continue
				}
				report.OrphanRowsFailed++
			} else {
				if rErr := s.meta.ResetToReady(r.ID); rErr != nil {
					log.Printf("spool recover: reset draining→ready %d: %v", r.ID, rErr)
					continue
				}
				s.used.Add(r.Size)
				report.DrainingReset++
			}

		case metadata.DrainDone:
			// `done` = data is durably in MinIO; MarkDrainComplete already
			// removed the spool file on success. A surviving file is a stale
			// leftover from a crash in the narrow window between the SQL
			// commit and the post-SQL unlink — safe to reclaim. Without this
			// it stays in expectedFiles forever and the orphan scan never
			// touches it. Code-reviewer slice-F HIGH fix.
			if fileExists {
				if rmErr := os.Remove(r.SpoolFile); rmErr != nil {
					log.Printf("spool recover: done-row stale file remove %s: %v", r.SpoolFile, rmErr)
				}
			}

		case metadata.DrainFailed:
			// CRITICAL — "we can't lose photos." A `failed` row's spool file
			// is the LAST durable copy of the user's data. `failed` is
			// reached by failPermanent after a transient/infra error (FUSE
			// unmounted → "device not configured", MinIO down, disk full)
			// where the spool file is a COMPLETE copy. Deleting it here (the
			// previous behavior, lumped with `done`) silently destroyed an
			// intact photo on the next boot — the exact loss this audit
			// found (1–2 GB MP4s gone after a transient unmount).
			//
			// Discriminate by size, never deleting finalized bytes:
			//   • intact (row.Size > 0 && on-disk size == row.Size): the
			//     full copy survived. Reset to `ready` (fresh attempt budget)
			//     so THIS boot re-drives it now that the destination is back.
			//   • writing-crash leftover (row.Size == 0): never finalized,
			//     the user already re-copied — reclaim the partial (slice-F).
			//   • ambiguous (row.Size > 0 but size mismatch): PRESERVE, leave
			//     failed for the operator. We do not delete >0 finalized bytes.
			if !fileExists {
				break // already lost upstream; nothing on disk to recover
			}
			fi, statErr := os.Stat(r.SpoolFile)
			diskSize := int64(-1)
			if statErr == nil {
				diskSize = fi.Size()
			}
			switch {
			case r.Size > 0 && diskSize == r.Size:
				if ok, rErr := s.meta.ResetForRetry(r.ID); rErr != nil {
					log.Printf("spool recover: failed→ready reset %d: %v", r.ID, rErr)
				} else if ok {
					s.used.Add(r.Size)
					report.FailedResumed++
				}
			case r.Size == 0:
				if rmErr := os.Remove(r.SpoolFile); rmErr != nil {
					log.Printf("spool recover: failed-row partial file remove %s: %v", r.SpoolFile, rmErr)
				}
			default:
				log.Printf("spool recover: failed-row file size mismatch (disk=%d row=%d), preserving %s",
					diskSize, r.Size, r.SpoolFile)
			}
		}
	}

	log.Printf("spool recover: %+v", report)
	return report, nil
}

// MarkDrainComplete is the success path for the drainer (slice B). It
// marks the SQL row done; ONLY on SQL success does it remove the spool
// file from disk, release the capacity reservation, and evict the
// in-memory index entry.
//
// nfsPath/spoolFile/size come from the metadata.SpoolRow the drainer
// claimed; the drainer never touches the *SpoolEntry directly (it may
// no longer exist if the writer process restarted between Close and
// drain).
//
// HIGH-2 fix (slice B reviewer): on SQL failure we MUST NOT delete the
// spool file or release capacity. Doing so converts a retryable
// transient SQLite error (busy, WAL checkpoint, disk full on meta
// partition) into irrecoverable data loss for a successfully-drained
// file. The caller (drainOne) sees the SQL error and calls
// failTransient, which retries. Slice F's boot scrubber covers crashes
// between SQL success and the post-SQL cleanup (in that narrow window
// the file exists on disk + capacity stays counted, harmless).
func (s *SpoolStore) MarkDrainComplete(id int64, nfsPath, spoolFile string, size int64) (bool, error) {
	done, err := s.meta.MarkDone(id)
	if err != nil {
		return false, err
	}
	if !done {
		// The row was cancelled (deleted) while this drain was in flight:
		// the NFS layer deleted nfsPath (QA-37). Do NOT remove the spool
		// file, release capacity, or write a manifest "done" record here —
		// CancelForDelete already owns that cleanup. A false return tells the
		// drainer to undo the FUSE write it just made.
		return false, nil
	}
	// Finding 3 fix: evict the in-memory index entry BEFORE removing the
	// spool file. New reads then miss the index and fall through to FUSE
	// (where the drained bytes now live) instead of resolving to the spool
	// and opening a file we're about to delete (ENOENT). Readers already
	// holding an fd are unaffected by the unlink (open-then-unlink Unix
	// semantics).
	if e, ok := s.index.Lookup(nfsPath); ok && e.ID() == id {
		s.index.DeleteIfMatches(nfsPath, e)
	}
	s.releaseCapacity(size)
	if rmErr := os.Remove(spoolFile); rmErr != nil && !os.IsNotExist(rmErr) {
		log.Printf("spool: drain complete %d: remove %s: %v", id, spoolFile, rmErr)
	}
	if s.manifest != nil {
		var (
			shaHex     string
			shaUnknown bool
		)
		if row, gerr := s.meta.Get(id); gerr == nil && row != nil && len(row.SHA256) > 0 {
			shaHex = fmt.Sprintf("%x", row.SHA256)
		} else {
			shaUnknown = true
		}
		if appendErr := s.manifest.Append(ManifestRecord{
			Event:             ManifestEventDrainDone,
			Path:              nfsPath,
			SpoolFile:         spoolFile,
			Size:              size,
			SHA256Hex:         shaHex,
			SHA256Unavailable: shaUnknown,
		}); appendErr != nil {
			// AUDIT LOSS — the drain succeeded but the audit trail
			// no longer reflects it. Operator should investigate
			// (manifest disk full, permission change, etc).
			log.Printf("spool: AUDIT LOSS manifest append done id=%d path=%s: %v",
				id, nfsPath, appendErr)
		}
	}
	return true, nil
}

// CancelForDelete cancels any in-flight spool entry for nfsPath so a pending
// or mid-flight drain cannot resurrect a file the NFS layer is deleting
// (QA-37). It evicts the in-memory index entry, deletes the SQL row(s), and
// removes the spool file(s) + capacity reservation. Safe to call when no
// entry exists (no-op).
//
// Race handling: a drain already past its os.Open of the spool file will
// finish copying to FUSE, then find its row gone at MarkDrainComplete (which
// returns done=false) and undo the FUSE write. DeleteActiveByPath's DELETE and
// the drainer's MarkDone are serialized by SpoolStore.writeMu, so exactly one
// of {complete, cancel} wins and the loser observes the resolved state.
func (s *SpoolStore) CancelForDelete(nfsPath string) {
	// Evict the index entry first so concurrent reads/opens miss the spool
	// and fall through to FUSE.
	e, indexed := s.index.Lookup(nfsPath)
	if indexed {
		s.index.DeleteIfMatches(nfsPath, e)
		// Close the live entry BEFORE its spool file is unlinked below, so a
		// concurrent WriteAt to this path (a delete racing an active write
		// under concurrent NLE/Finder activity) errors cleanly instead of
		// writing into an unlinked fd, which would silently discard the bytes.
		e.cancelClose()
	}
	rows, err := s.meta.DeleteActiveByPath(nfsPath)
	if err != nil {
		log.Printf("spool: cancel-for-delete %q: %v", nfsPath, err)
	}
	for _, r := range rows {
		sz := r.Size
		if indexed && e.ID() == r.ID && e.WrittenEnd() > sz {
			// `writing`-state rows persist size=0 until finalize; the live
			// entry knows the bytes actually reserved against capacity.
			sz = e.WrittenEnd()
		}
		s.releaseCapacity(sz)
		if rmErr := os.Remove(r.SpoolFile); rmErr != nil && !os.IsNotExist(rmErr) {
			log.Printf("spool: cancel-for-delete remove %s: %v", r.SpoolFile, rmErr)
		}
	}
}

// MigrateForRename re-keys every active spool entry under oldPath (the
// exact path, plus — for directory renames — everything under oldPath+"/")
// to the corresponding path under newPath: SQL row, in-memory index key,
// entry identity, and therefore the eventual drain TARGET. Returns the
// number of rows migrated. Phase-1 BUG 1: without this, juiceFS.Rename was
// spool-blind — the old path kept "existing" via the LookupActive shadow
// and the drainer later re-created the OLD path on FUSE (the QA-37
// resurrection class, for rename).
//
// Per-state handling (decided atomically under meta.writeMu, inside
// MigrateActivePaths):
//   - writing / ready: the row's nfs_path is updated in place. The drain
//     hasn't started (drainers read the path at claim time), so the queued
//     copy simply lands at the new path.
//   - draining: the in-flight worker already holds the OLD target. The row
//     is DELETEd (the worker's MarkDrainComplete then returns done=false
//     and it undoes its FUSE write — the QA-37 cancel contract) and a fresh
//     `ready` row is inserted at the new path sharing the same spool file.
//     The entry adopts the new row id so drain-complete eviction matches.
//
// Locking all OpenWrite shards serializes against every concurrent OpenWrite
// so a write RPC can't create a second entry for any migrated path
// mid-migration (a directory rename re-keys many paths, so a single path
// shard is insufficient). Lock order: openShards → meta.writeMu (released) →
// e.mu → index.mu — same direction as OpenWrite, no cycles. Renames are rare
// relative to the per-RPC write path, so the full-barrier cost is acceptable.
func (s *SpoolStore) MigrateForRename(oldPath, newPath string) (int, error) {
	s.lockAllShards()
	defer s.unlockAllShards()

	migs, err := s.meta.MigrateActivePaths(oldPath, newPath)
	if err != nil {
		return 0, err
	}
	requeued := false
	for _, m := range migs {
		if m.Requeued {
			requeued = true
		}
		e, ok := s.index.Lookup(m.OldPath)
		if !ok || e.ID() != m.OldID {
			// Not in the index (e.g. a ready/draining row recovered by the
			// boot scrubber, which doesn't repopulate the index) — the SQL
			// migration alone is sufficient for those.
			continue
		}
		e.adoptRename(m.NewPath, m.NewID)
		s.index.Move(m.OldPath, m.NewPath, e)

		// Close the eviction race: if a drain for this row completed in
		// the window between the SQL commit and the index Move above, its
		// MarkDrainComplete looked up the new path BEFORE the entry was
		// there and evicted nothing — leaving a permanent stale shadow.
		// Re-check the row's state and evict ourselves if it went
		// terminal. DeleteIfMatches is identity-checked, so double
		// eviction is harmless.
		if row, gerr := s.meta.Get(m.NewID); gerr == nil && row != nil &&
			(row.DrainState == metadata.DrainDone || row.DrainState == metadata.DrainFailed) {
			s.index.DeleteIfMatches(m.NewPath, e)
		}
	}
	if requeued {
		// A fresh `ready` row exists; wake the drainer for it.
		s.signalReady()
	}
	return len(migs), nil
}

// MarkDrainRetry is the transient-failure path: bumps drain_attempts +
// last_error and resets the row to ready so the dispatcher picks it up
// again after backoff. Caller is responsible for the backoff delay.
func (s *SpoolStore) MarkDrainRetry(id int64, reason string) error {
	if err := s.meta.IncrementAttempts(id, reason); err != nil {
		return err
	}
	return s.meta.ResetToReady(id)
}

// QuarantineDrain is the SHA-mismatch path: marks the SQL row failed
// THEN moves the spool file to a quarantine subdir (preserving forensic
// state), releases the capacity reservation, and evicts the index entry.
// The manifest log (slice G) will record the quarantine event with
// timestamps + the mismatched SHAs.
//
// Ordering (HIGH-2 follow-on fix): MarkFailed runs BEFORE the rename.
// If SQL fails, the spool file stays in its original location and the
// row remains in `draining` state — the drainer's next attempt will
// re-detect the SHA mismatch and call QuarantineDrain again. If we
// renamed first then SQL-failed, the file would be in quarantine/ but
// the row's spool_file path would point to the original (now missing)
// location; slice F's scrubber would treat it as an orphan and
// failPermanent, losing the forensic copy.
//
// The quarantined file is left on disk. Operators can inspect/recover/
// delete it manually.
func (s *SpoolStore) QuarantineDrain(id int64, nfsPath, spoolFile string, size int64, reason string) error {
	transitioned, err := s.meta.MarkFailed(id, reason)
	if err != nil {
		return err
	}
	quarantineDir := filepath.Join(s.root, "quarantine")
	if err := os.MkdirAll(quarantineDir, 0o755); err != nil {
		log.Printf("spool: quarantine mkdir: %v", err)
		// Continue — the SQL row is already failed; best-effort the
		// file move and the cleanup below.
	}
	dest := filepath.Join(quarantineDir, filepath.Base(spoolFile))
	if err := os.Rename(spoolFile, dest); err != nil && !os.IsNotExist(err) {
		log.Printf("spool: quarantine move %s→%s: %v", spoolFile, dest, err)
	}
	// Same ownership rule as failPermanent (adversarial-review BUG 1): a
	// row deleted/requeued out from under this drain already had its
	// reservation handled by that path — release only on a real transition.
	if transitioned {
		s.releaseCapacity(size)
	}
	if e, ok := s.index.Lookup(nfsPath); ok && e.ID() == id {
		s.index.DeleteIfMatches(nfsPath, e)
	}
	if s.manifest != nil {
		var (
			shaHex     string
			shaUnknown bool
		)
		if row, gerr := s.meta.Get(id); gerr == nil && row != nil && len(row.SHA256) > 0 {
			shaHex = fmt.Sprintf("%x", row.SHA256)
		} else {
			shaUnknown = true
		}
		if appendErr := s.manifest.Append(ManifestRecord{
			Event:             ManifestEventQuarantine,
			Path:              nfsPath,
			SpoolFile:         dest,
			Size:              size,
			SHA256Hex:         shaHex,
			SHA256Unavailable: shaUnknown,
			Reason:            reason,
		}); appendErr != nil {
			// AUDIT LOSS — the quarantine event happened but the
			// audit trail lost it. This is the most serious manifest
			// failure mode because quarantine is the integrity
			// failure case the log exists to document.
			log.Printf("spool: AUDIT LOSS manifest append quarantine id=%d path=%s reason=%q: %v",
				id, nfsPath, reason, appendErr)
		}
	}
	return nil
}

// signalReady notifies the drainer that a new ready entry exists.
// No-op when no drainer callback is registered.
func (s *SpoolStore) signalReady() {
	s.wakeMu.RLock()
	fn := s.wakeDrainer
	s.wakeMu.RUnlock()
	if fn != nil {
		fn()
	}
}

// tryReserveCapacity atomically reserves `delta` bytes against the cap
// using a CAS loop. Returns true if the reservation succeeded (used was
// bumped), false if the reservation would exceed capacity. Cap of 0
// means unlimited and always succeeds (with the add still happening).
//
// Why CAS instead of a check-then-add under per-entry mu: two concurrent
// WriteAt calls on DIFFERENT entries hold different e.mu locks but share
// the same store.used. Without CAS, both could read the same `used`,
// both pass the cap check, both commit — over-filling the cap by up to
// one write-payload-per-concurrent-writer. CAS bounds the over-fill to
// zero.
func (s *SpoolStore) tryReserveCapacity(delta int64) bool {
	if delta <= 0 {
		return true
	}
	for {
		cur := s.used.Load()
		if s.capacity > 0 && cur+delta > s.capacity {
			return false
		}
		if s.used.CompareAndSwap(cur, cur+delta) {
			return true
		}
	}
}

// capacityWaitPoll / capacityWaitDeadline bound the backpressure stall when the
// spool is full. A WriteAt/OpenWrite that can't get capacity waits up to the
// deadline (polling for the drainer to free space) before giving up with
// ErrSpoolFull — converting a hard NOSPC abort of the whole Finder copy into a
// brief stall the NFS client tolerates as slow I/O, pacing ingest to drain
// throughput. The deadline stays well under the NFS soft-mount timeout so the
// stall itself never trips ETIMEDOUT. Vars (not consts) so tests can shorten
// them. This is the flow-control valve the spool previously lacked: before, the
// instant ingest outran drain past the cap, every write hard-failed and the
// copy aborted — fatal for a large SD-card offload.
var (
	capacityWaitPoll = 25 * time.Millisecond
	// capacityWaitDeadline: how long a full-spool WriteAt/OpenWrite throttles,
	// waiting for the drainer to free space, before giving up with ErrSpoolFull.
	// 30s (was 5s) — kept safely under the NFS soft-mount timeout (~40s) so the
	// stall never trips ETIMEDOUT, but long enough that ingest THROTTLES to
	// drain throughput across a sustained large copy instead of hard-failing the
	// whole Finder copy with NOSPC ("disk full") the moment the spool fills
	// (2026-06-14: a 500 GB copy aborted "disk is full" at the cap; the old 10s
	// total OpenWrite budget couldn't outlast a slow drain under concurrency).
	// The drain freeing ~1 file every few hundred ms means each waiter gets its
	// slot well within this window; only a genuinely stalled drain reaches it.
	capacityWaitDeadline = 30 * time.Second
)

// reserveCapacityOrWait reserves delta bytes, blocking up to capacityWaitDeadline
// for the drainer to free space if the cap is currently exhausted. Returns true
// with delta reserved, false on timeout/close. Callers MUST NOT hold a per-entry
// e.mu or path shard across this call — it can block for the full deadline, and
// holding those would stall the sweeper and other opens.
func (s *SpoolStore) reserveCapacityOrWait(delta int64) bool {
	if s.tryReserveCapacity(delta) {
		return true
	}
	deadline := time.Now().Add(capacityWaitDeadline)
	for {
		if s.closed.Load() || time.Now().After(deadline) {
			return false
		}
		time.Sleep(capacityWaitPoll)
		if s.tryReserveCapacity(delta) {
			return true
		}
	}
}

// waitForHeadroom blocks up to capacityWaitDeadline for the spool to drop below
// its cap (the drainer freeing space), so a new-file OpenWrite during a full
// spool stalls instead of hard-failing the copy. Returns true once there is
// headroom, false on timeout/close. Must not be called with a shard held.
func (s *SpoolStore) waitForHeadroom() bool {
	if s.capacity <= 0 || s.used.Load() < s.capacity {
		return true
	}
	deadline := time.Now().Add(capacityWaitDeadline)
	for {
		if s.closed.Load() || time.Now().After(deadline) {
			return false
		}
		time.Sleep(capacityWaitPoll)
		if s.used.Load() < s.capacity {
			return true
		}
	}
}

// releaseCapacity returns reserved bytes to the budget. Used when a
// reserved write partially failed and the actual delta committed to
// disk was less than reserved.
//
// Floored at zero via CAS: `used` represents bytes on the spool disk
// and can never legitimately be negative. A release that would push it
// below zero is an unbacked release (e.g. a row whose reservation was
// dropped across a restart gets retried and later drains) — clamping
// keeps the invariant instead of letting the counter wander negative
// and silently widen the capacity budget.
func (s *SpoolStore) releaseCapacity(delta int64) {
	if delta <= 0 {
		return
	}
	for {
		cur := s.used.Load()
		next := cur - delta
		if next < 0 {
			next = 0
		}
		if s.used.CompareAndSwap(cur, next) {
			return
		}
	}
}

// RetryFailed resets every FAILED row whose spool file still exists on
// disk back to `ready` with a fresh attempt budget, re-reserves its
// bytes against the capacity counter, and wakes the drainer. Rows whose
// spool file is gone (boot-scrubbed writing rows, quarantined rows —
// their file lives under quarantine/, not at spool_file) are skipped:
// there is nothing to retry. Never deletes user bytes. Returns the
// number of rows requeued.
//
// Capacity note: terminal rows are not part of the `used` budget
// (failPermanent releases, RecoverOnBoot doesn't count failed rows), so
// a retried row's bytes are re-added here. The add is unconditional
// rather than tryReserveCapacity — an operator-initiated recovery must
// not fail on the advisory cap; transient over-cap drains away as the
// requeued rows complete.
func (s *SpoolStore) RetryFailed() (int, error) {
	rows, err := s.meta.ListAll()
	if err != nil {
		return 0, err
	}
	n := 0
	skippedStale := 0
	for _, r := range rows {
		if r.DrainState != metadata.DrainFailed || r.SpoolFile == "" {
			continue
		}
		if _, statErr := os.Stat(r.SpoolFile); statErr != nil {
			continue
		}
		// Staleness guard (adversarial-review BUG 3): a NEWER row for the
		// same path — drained, in flight, or even failed-again — means the
		// user acted on the path after this failure. Requeuing the old spool
		// file would clobber newer bytes with stale ones (the drainer's SHA
		// check validates integrity, not freshness). Skip; the row stays
		// failed and ages out of the status view.
		if newer, nErr := s.meta.HasNewerRowForPath(r.NFSPath, r.ID); nErr != nil {
			log.Printf("spool: retry-failed newer-row check %d (%s): %v", r.ID, r.NFSPath, nErr)
			continue
		} else if newer {
			skippedStale++
			continue
		}
		// Reserve BEFORE the state transition (adversarial-review BUG 4):
		// reset-then-reserve let a concurrent CancelForDelete release the
		// not-yet-reserved bytes (clamped at 0) and the late Add then leaked
		// `used` upward until restart. Reserve-then-reset keeps the counter
		// owned by whichever side wins; on a lost reset, undo via the
		// floored release.
		if r.Size > 0 {
			s.used.Add(r.Size)
		}
		ok, resetErr := s.meta.ResetForRetry(r.ID)
		if resetErr != nil || !ok {
			if r.Size > 0 {
				s.releaseCapacity(r.Size)
			}
			if resetErr != nil {
				log.Printf("spool: retry-failed reset %d (%s): %v", r.ID, r.NFSPath, resetErr)
			}
			continue
		}
		n++
	}
	if skippedStale > 0 {
		jmlog.Warn("spool: retry-failed skipped stale rows (newer row exists for path)",
			"skipped", skippedStale)
	}
	if n > 0 {
		jmlog.Info("spool: retry-failed requeued rows", "count", n)
		s.signalReady()
	}
	return n, nil
}

// RecoverStalled force-finalizes every stalled `writing` entry NOW
// instead of waiting for the sweeper's next pass: entries with leaked
// handles quiescent beyond the escalation window go through the same
// escalateIfStuck path (refcount zeroed, fsync + SHA + mark-ready —
// bytes preserved, never deleted), and quiescent refcount-0 entries are
// finalized via the normal idle path. Returns the number of entries
// finalized. This is the action behind
// /spool-recover?action=clear-stalled.
func (s *SpoolStore) RecoverStalled() int {
	window := s.escalateAfter
	if window <= 0 {
		// Escalation disabled — nothing qualifies as stalled.
		return 0
	}
	// sweepOnce(idle=window) finalizes refcount-0 entries idle ≥ window
	// and escalates refcount>0 entries quiescent ≥ s.escalateAfter —
	// exactly the stalled predicate /spool reports.
	n := s.sweepOnce(window)
	if n > 0 {
		jmlog.Info("spool: recover-stalled force-finalized entries", "count", n)
	}
	return n
}

// SpoolEntry is one in-flight or pending-upload file. Holds a write fd to
// the on-disk spool file plus the streaming SHA-256 hasher.
//
// Lifecycle:
//
//	NewSpoolStore.OpenWrite → entry.WriteAt → ... → entry.Close
//	                                                    ↓
//	                                            drain_state=ready
//	                                                    ↓
//	                                        drainer claims via Meta.MarkDraining
//	                                                    ↓
//	                                        drainer reads via OpenForRead
//	                                                    ↓
//	                                        SpoolStore.MarkDone (slice B) deletes file
type SpoolEntry struct {
	id        int64
	nfsPath   string
	spoolFile string

	// Inode assigned at OpenFile time by the handler so Stat/Lstat
	// during the writing/ready/draining lifetime return a stable value.
	// Set via SetInode (once) after OpenWrite returns. Zero until set.
	//
	// atomic.Uint64 (slice D reviewer CRITICAL fix): SetInode and Inode
	// can race across goroutines (OpenFile-write sets; Stat/Lstat read
	// concurrently). Plain field had a data race that -race would catch
	// in a concurrent Stat + OpenWrite interleaving. CompareAndSwap
	// preserves the once-set semantics atomically.
	inode atomic.Uint64

	// lastWrite is atomic Unix-nanoseconds of the most recent WriteAt
	// that extended writtenEnd. Used by the slice D Stat/Lstat shadow
	// so an in-flight file's mtime reflects writer activity instead of
	// returning 1970-epoch. Initialized to OpenWrite time; updated on
	// each writing-extending WriteAt.
	lastWrite atomic.Int64

	mu         sync.RWMutex
	file       *os.File // nil after Close
	writtenEnd int64
	// contiguousEnd is the end of the contiguously-written prefix from offset
	// 0 — i.e. the largest N such that bytes [0,N) all actually contain written
	// data. Distinct from writtenEnd, which is the high-water/allocated size:
	// an ftruncate-preallocate (cp/copyfile/fio) or an out-of-order WRITE jumps
	// writtenEnd past data that hasn't landed yet, leaving a HOLE. Reads must
	// never serve those holes (pread returns them as ZEROS with no error — an
	// NLE reading a still-copying clip would get black frames / corrupt RAW),
	// so the read shadow clamps to contiguousEnd. Monotonic; guarded by mu.
	contiguousEnd int64
	hasher        hash.Hash
	sha256        []byte // populated on Close iff streaming hash is trustworthy
	// hashValid tracks whether the streaming hasher reflects the on-disk
	// contents. False once we observe any out-of-order WriteAt (off <
	// current writtenEnd) — sparse / out-of-order writes make the streaming
	// hash diverge from the file's at-rest hash. The drainer (slice B)
	// re-hashes from disk regardless; this flag tells it whether the
	// streaming SHA is a usable optimization or just noise.
	hashValid bool
	closed    bool

	// refcount is the number of live write handles for this entry (one per
	// in-flight spoolWriteFile). Guarded by mu. OpenWrite increments on
	// create/reuse; ReleaseHandle and Close decrement.
	//
	// CRITICAL: finalize is NOT triggered by refcount hitting zero. NFS does
	// OpenFile→WriteAt→Close on every WRITE RPC (internal/nfs/nfs_onwrite.go),
	// so a finalize-on-refcount-zero would end the file after the first 1 MB
	// chunk. Finalize is instead driven by the idle sweeper (finalizeIfIdle)
	// once refcount==0 and the entry has been quiescent — mirroring how
	// FDPool.evictLoop handles the same per-RPC open/close churn for fds.
	refcount int
	store    *SpoolStore
}

// ID returns the SQLite spool_entries.id this entry is bound to. Locked:
// MigrateForRename can re-bind a draining entry to its requeued row's id
// concurrently with drain-complete / cancel identity checks.
func (e *SpoolEntry) ID() int64 {
	e.mu.RLock()
	id := e.id
	e.mu.RUnlock()
	return id
}

// NFSPath returns the in-mount path this entry shadows. Locked: the path is
// re-keyed by MigrateForRename when the client renames an in-flight file.
func (e *SpoolEntry) NFSPath() string {
	e.mu.RLock()
	p := e.nfsPath
	e.mu.RUnlock()
	return p
}

// adoptRename re-binds the entry to a new NFS path and (for requeued
// draining rows) a new SQL row id. Called only by MigrateForRename, which
// owns the corresponding index re-key; the two updates happen under the
// all-shards OpenWrite barrier (MigrateForRename) so no concurrent OpenWrite
// can observe a half-migrated entry.
func (e *SpoolEntry) adoptRename(newPath string, newID int64) {
	e.mu.Lock()
	e.nfsPath = newPath
	e.id = newID
	e.mu.Unlock()
}

// SpoolFilePath returns the on-disk path of the spool file.
func (e *SpoolEntry) SpoolFilePath() string { return e.spoolFile }

// WrittenEnd returns the current high-water byte count. Safe to call
// concurrently with WriteAt; the value is monotonic.
func (e *SpoolEntry) WrittenEnd() int64 {
	e.mu.RLock()
	n := e.writtenEnd
	e.mu.RUnlock()
	return n
}

// ContiguousEnd returns the end of the contiguously-written prefix — the
// largest N such that bytes [0,N) all contain real written data (no
// preallocation/out-of-order holes). The read shadow uses this as the
// readable boundary so an in-flight file never serves a hole as zeros.
func (e *SpoolEntry) ContiguousEnd() int64 {
	e.mu.RLock()
	n := e.contiguousEnd
	e.mu.RUnlock()
	return n
}

// Sync fsyncs the spool file to stable storage WITHOUT finalizing the entry —
// the writer may continue. This is the NFS COMMIT / FILE_SYNC durability
// barrier: it makes the data written so far survive a power loss (a plain
// WriteAt only reaches the OS page cache). No-op once closed. Without this,
// onCommit was a lie — a client told its fsync succeeded could lose
// acknowledged bytes on power loss before the idle sweeper's finalize fsync.
func (e *SpoolEntry) Sync() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || e.file == nil {
		return nil
	}
	return e.file.Sync()
}

// cancelClose marks the entry closed and closes its fd. Used by CancelForDelete
// BEFORE it unlinks the spool file, so a concurrent WriteAt (which checks
// e.closed under e.mu) errors cleanly instead of writing into a soon-to-be-
// unlinked fd — those bytes would be silently discarded yet the WRITE RPC would
// return OK, a delete-racing-an-active-write data hazard. Idempotent.
func (e *SpoolEntry) cancelClose() {
	e.mu.Lock()
	if !e.closed {
		e.closed = true
		if e.file != nil {
			_ = e.file.Close()
			e.file = nil
		}
	}
	e.mu.Unlock()
}

// SetInode sets the synthetic inode for this entry. Called once by the
// NFS handler immediately after OpenWrite so Stat/Lstat returning a
// FileInfo for this entry reports a stable inode for the lifetime of
// the entry. CAS preserves once-set semantics: only the first non-zero
// SetInode wins; subsequent calls are no-ops.
func (e *SpoolEntry) SetInode(inode uint64) {
	e.inode.CompareAndSwap(0, inode)
}

// Inode returns the synthetic inode set via SetInode. Zero if SetInode
// was never called (drainer rows recovered via boot scrubber will have
// 0 here; callers must guard).
func (e *SpoolEntry) Inode() uint64 { return e.inode.Load() }

// LastWrite returns the time of the most recent extending WriteAt as a
// time.Time. Used by the slice D Stat/Lstat shadow path for mtime.
func (e *SpoolEntry) LastWrite() time.Time {
	return time.Unix(0, e.lastWrite.Load())
}

// SHA256 returns the final streaming SHA-256 hash. Returns nil if Close
// has not completed OR if the entry observed any out-of-order WriteAt
// during its lifetime (in which case the streaming hash is unreliable
// and the drainer must re-hash from disk).
func (e *SpoolEntry) SHA256() []byte {
	e.mu.RLock()
	sha := e.sha256
	e.mu.RUnlock()
	return sha
}

// StreamingHashValid reports whether the streaming SHA-256 reflects the
// on-disk contents. False if any WriteAt arrived out of offset order.
// The drainer uses this to decide whether to trust SHA() or re-hash.
func (e *SpoolEntry) StreamingHashValid() bool {
	e.mu.RLock()
	v := e.hashValid
	e.mu.RUnlock()
	return v
}

// WriteAt appends bytes to the spool file at offset off and folds them
// into the streaming SHA-256 hasher.
//
// Honors the SpoolStore capacity cap: if adding n bytes would push used
// past capacity, returns ErrSpoolFull and writes nothing.
//
// Capacity reservation uses the store's CAS reservation so two concurrent
// writers on DIFFERENT entries can never both pass the cap check and
// over-fill. Any reserved-but-unused bytes (short write) are released
// after the underlying WriteAt returns.
//
// Out-of-order detection: if off < the current writtenEnd, the streaming
// hash diverges from the file's at-rest hash. We mark hashValid=false so
// the drainer knows to re-hash from disk rather than trust SHA256().
func (e *SpoolEntry) WriteAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	e.mu.Lock()
	if e.closed || e.file == nil {
		e.mu.Unlock()
		return 0, fmt.Errorf("spool: write to closed entry")
	}
	newEnd := off + int64(len(p))
	var reserved int64
	if newEnd > e.writtenEnd {
		reserved = newEnd - e.writtenEnd
		if !e.store.tryReserveCapacity(reserved) {
			// Spool full. Release e.mu and apply backpressure instead of
			// hard-failing the WRITE RPC (which aborts the whole Finder copy
			// with NOSPC). Wait briefly for the drainer to free capacity, then
			// re-acquire and re-validate. writtenEnd may have advanced while
			// unlocked (a concurrent write to this file); the refund logic
			// below reconciles `reserved` (computed against the pre-wait
			// writtenEnd) against the then-current writtenEnd — since writtenEnd
			// only moves forward, the actual extension is <= reserved and the
			// excess is refunded, so the accounting stays correct.
			e.mu.Unlock()
			if !e.store.reserveCapacityOrWait(reserved) {
				return 0, ErrSpoolFull
			}
			e.mu.Lock()
			if e.closed || e.file == nil {
				e.store.releaseCapacity(reserved)
				e.mu.Unlock()
				return 0, fmt.Errorf("spool: write to closed entry")
			}
		}
	}
	outOfOrder := off < e.writtenEnd

	n, err := e.file.WriteAt(p, off)
	if n > 0 {
		end := off + int64(n)
		if end > e.writtenEnd {
			actualDelta := end - e.writtenEnd
			if actualDelta < reserved {
				e.store.releaseCapacity(reserved - actualDelta)
			}
			e.writtenEnd = end
			e.lastWrite.Store(time.Now().UnixNano())
		} else if reserved > 0 {
			// Wrote bytes but did not extend writtenEnd — release the
			// whole reservation since we never actually grew the file.
			e.store.releaseCapacity(reserved)
		}
		// Extend the contiguous-written prefix if this write starts at or
		// before its current end (sequential write, or a backfill that closes
		// the gap). A write that starts ABOVE contiguousEnd leaves a hole and
		// does NOT advance it — reads must not see that hole as zeros. This is
		// conservative for fully out-of-order writers (the prefix lags until
		// the gap fills) but never serves unwritten data; the dominant
		// cp/Finder pattern is sequential, so contiguousEnd == writtenEnd.
		if off <= e.contiguousEnd && end > e.contiguousEnd {
			e.contiguousEnd = end
		}
		if outOfOrder {
			e.hashValid = false
		}
		if e.hashValid {
			_, _ = e.hasher.Write(p[:n])
		}
	} else if reserved > 0 {
		// Zero-byte write — refund the reservation entirely.
		e.store.releaseCapacity(reserved)
	}
	e.mu.Unlock()
	return n, err
}

// Truncate resizes the spool file to size and moves writtenEnd to match.
// This is the ftruncate path: NFS SETATTR{size} against an in-flight entry
// routes here via spoolWriteFile.Truncate (fio preallocates with ftruncate
// before writing; cp/copyfile ftruncates to the final size after writing).
//
// Capacity follows the resize: growth is CAS-reserved against the cap
// (ErrSpoolFull on exceed, nothing changed), shrink releases the
// difference. A same-size truncate is a no-op that PRESERVES the streaming
// hash — that's cp's post-write ftruncate(dst, size==writtenEnd), the
// dominant copy workload, and it must keep the drainer's SHA verification.
// Any actual resize invalidates the streaming hash (the hasher saw the
// write stream, not the post-truncate at-rest bytes); the drainer re-hashes
// from disk in that case, same as out-of-order writes.
func (e *SpoolEntry) Truncate(size int64) error {
	if size < 0 {
		return fmt.Errorf("spool: truncate to negative size %d", size)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || e.file == nil {
		return fmt.Errorf("spool: truncate on closed entry")
	}
	if size == e.writtenEnd {
		return nil
	}
	if size > e.writtenEnd {
		delta := size - e.writtenEnd
		if !e.store.tryReserveCapacity(delta) {
			return ErrSpoolFull
		}
		if err := e.file.Truncate(size); err != nil {
			e.store.releaseCapacity(delta)
			return fmt.Errorf("spool: truncate extend: %w", err)
		}
	} else {
		if err := e.file.Truncate(size); err != nil {
			return fmt.Errorf("spool: truncate shrink: %w", err)
		}
		e.store.releaseCapacity(e.writtenEnd - size)
	}
	e.writtenEnd = size
	if e.contiguousEnd > size {
		// Shrink past the contiguous prefix: those bytes are gone.
		e.contiguousEnd = size
	}
	// NB: a GROW/preallocate (size > writtenEnd) leaves contiguousEnd alone —
	// the extended region is an unwritten hole that reads must not serve.
	e.hashValid = false
	// Truncate is writer activity: bump quiescence so the sweeper doesn't
	// finalize between a client's ftruncate and its first WRITE.
	e.lastWrite.Store(time.Now().UnixNano())
	return nil
}

// OpenForRead returns a fresh read-only fd on the spool file. Caller
// is responsible for closing it. Used by:
//   - the drainer (slice B) to stream bytes into the FUSE mount
//   - the read path (slice D) to serve in-flight reads
//
// Returns an error if the entry has been fully drained and the file is
// no longer on disk.
func (e *SpoolEntry) OpenForRead() (*os.File, error) {
	e.mu.RLock()
	path := e.spoolFile
	e.mu.RUnlock()
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("spool: open read: %w", err)
	}
	return f, nil
}

// Close releases one write handle. If it was the last handle, the entry is
// finalized immediately (fsync + SHA + mark-ready + signal drainer). This
// preserves the direct-API semantics existing callers and tests rely on
// (OpenWrite → WriteAt → Close finalizes).
//
// The NFS per-RPC write path does NOT use Close — it uses ReleaseHandle, so
// per-RPC closes don't finalize, and the idle sweeper finalizes once the
// writer is quiescent. See the refcount field doc for why.
func (e *SpoolEntry) Close() error {
	e.mu.Lock()
	if e.refcount > 0 {
		e.refcount--
	}
	if e.refcount > 0 {
		e.mu.Unlock()
		return nil
	}
	return e.finalizeLocked() // releases e.mu
}

// ReleaseHandle drops one write handle WITHOUT finalizing. Used by the NFS
// per-RPC write path (spoolWriteFile.Close): NFS closes the file after every
// WRITE RPC, so finalizing here would end the file after the first chunk.
// The idle sweeper (finalizeIfIdle) finalizes once refcount==0 and quiescent.
func (e *SpoolEntry) ReleaseHandle() {
	e.mu.Lock()
	if e.refcount > 0 {
		e.refcount--
	}
	e.mu.Unlock()
}

// Finalize finalizes the entry unconditionally (if not already finalized).
// Exposed for an explicit end-of-write trigger (e.g. a future NFS COMMIT
// hook) and for tests. The sweeper uses finalizeIfIdle instead.
func (e *SpoolEntry) Finalize() error {
	e.mu.Lock()
	return e.finalizeLocked() // releases e.mu
}

// finalizeIfIdle finalizes iff the entry has no open handles AND has been
// quiescent for at least idle. Returns true if it finalized. This is the
// sweeper's entry point. Taking e.mu around the refcount + closed check makes
// it mutually exclusive with OpenWrite's reuse path (which also takes e.mu
// before incrementing refcount), so the sweeper can never finalize an entry
// a concurrent reopen is about to write to.
func (e *SpoolEntry) finalizeIfIdle(idle time.Duration) bool {
	e.mu.Lock()
	if e.closed || e.refcount != 0 {
		e.mu.Unlock()
		return false
	}
	if time.Since(time.Unix(0, e.lastWrite.Load())) < idle {
		e.mu.Unlock()
		return false
	}
	_ = e.finalizeLocked() // releases e.mu
	return true
}

// escalateIfStuck force-finalizes an entry whose write handles leaked: it
// holds refcount>0 but has been quiescent for at least `window`. Returns
// true if it finalized. See sweepOnce for the full rationale. window<=0
// disables. Loud by design — every escalation is a bug elsewhere (a dropped
// billy.File), and the Warn is the operator's signal to find it.
func (e *SpoolEntry) escalateIfStuck(window time.Duration) bool {
	if window <= 0 {
		return false
	}
	e.mu.Lock()
	if e.closed || e.refcount == 0 {
		e.mu.Unlock()
		return false
	}
	idleFor := time.Since(time.Unix(0, e.lastWrite.Load()))
	if idleFor < window {
		e.mu.Unlock()
		return false
	}
	leaked := e.refcount
	id, path, size := e.id, e.nfsPath, e.writtenEnd
	// Zero the refcount: the handles are gone (leaked), nothing will ever
	// release them. finalizeLocked then runs the normal fsync + SHA +
	// mark-ready path so the bytes drain like any other finalized entry.
	e.refcount = 0
	jmlog.Warn("spool: force-finalizing stuck entry — write handle(s) leaked",
		"path", path,
		"id", id,
		"leaked_handles", leaked,
		"quiescent", idleFor.Round(time.Second).String(),
		"bytes", size,
	)
	if err := e.finalizeLocked(); err != nil { // releases e.mu
		jmlog.Warn("spool: stuck-entry force-finalize failed",
			"path", path, "id", id, "error", err.Error())
	}
	return true
}

// finalizeLocked performs the finalize. MUST be called with e.mu held; it
// releases e.mu before the SQL MarkReady + drainer signal (so we never hold
// the entry lock across SQL).
func (e *SpoolEntry) finalizeLocked() error {
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true

	var firstErr error
	if e.file != nil {
		if err := e.file.Sync(); err != nil {
			firstErr = fmt.Errorf("spool: fsync: %w", err)
		}
		if err := e.file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("spool: close fd: %w", err)
		}
		e.file = nil
	}
	if e.hashValid && e.hasher != nil {
		e.sha256 = e.hasher.Sum(nil)
	}
	finalSize := e.writtenEnd
	finalSha := e.sha256 // nil if streaming hash was invalidated (out-of-order)
	finalSpoolFile := e.spoolFile
	finalID := e.id // capture under mu: rename migration can re-bind id
	e.mu.Unlock()

	// If the streaming hash was invalidated (out-of-order / truncate-resized
	// writes), derive a reference SHA from the finalized on-disk spool file —
	// off the lock, after fsync+close so the on-disk bytes are complete. Without
	// this, row.SHA256 stayed nil and the drainer SKIPPED both its integrity
	// checks (spool-SSD bit-flip and FUSE at-rest), so a corrupt copy of an
	// out-of-order-written file became the only copy with no detection. One
	// extra full read, only for the rare out-of-order case; sequential
	// cp/Finder writes keep the streaming hash and never reach here.
	if finalSha == nil && finalSpoolFile != "" {
		if sha, _, herr := hashSpoolFile(finalSpoolFile); herr == nil {
			finalSha = sha
		}
	}

	if err := e.store.meta.MarkReady(finalID, finalSize, finalSha); err != nil && firstErr == nil {
		firstErr = err
	}
	e.store.signalReady()
	return firstErr
}

// CloseAndDelete is used by tests + the boot-scrubber failure path:
// abandons the entry, removes the spool file, marks the SQL row failed.
// Returns the first error encountered but always attempts every step.
//
// Idempotent — second invocation is a no-op. Crucially, if a regular
// Close() has already finalized the entry (transitioning the SQL row to
// `ready`), CloseAndDelete must NOT clobber that state — we early-out
// when closed and rely on the drainer to handle the ready row normally.
//
// Index removal is identity-checked (DeleteIfMatches) so a scrubber
// cleaning up entry A never accidentally evicts entry B that was
// re-inserted at the same path after A was closed.
func (e *SpoolEntry) CloseAndDelete(reason string) error {
	e.mu.Lock()
	if e.closed {
		nfsPath := e.nfsPath
		e.mu.Unlock()
		// Even if already closed via Close(), make sure we don't leave
		// a stale index entry. DeleteIfMatches is safe: it only removes
		// the entry if WE are the current holder of the path slot.
		e.store.index.DeleteIfMatches(nfsPath, e)
		return nil
	}
	if e.file != nil {
		_ = e.file.Close()
		e.file = nil
	}
	path := e.spoolFile
	written := e.writtenEnd
	// Capture identity under mu — rename migration can re-bind id/nfsPath.
	id := e.id
	nfsPath := e.nfsPath
	e.closed = true
	e.mu.Unlock()

	var firstErr error
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		firstErr = fmt.Errorf("spool: remove file: %w", err)
	} else if err == nil {
		e.store.releaseCapacity(written)
	}
	if _, err := e.store.meta.MarkFailed(id, reason); err != nil && firstErr == nil {
		firstErr = err
	}
	e.store.index.DeleteIfMatches(nfsPath, e)
	return firstErr
}
