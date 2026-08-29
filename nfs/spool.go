package nfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
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
// bounded wait. It WRAPS pin.ErrSpoolBusy so the internal/nfs write handler —
// which cannot import this package — recognizes it via pin.IsSpoolBusy and maps
// it to the retryable NFS3ERR_JUKEBOX, so the client retries (by which point
// the drain has almost always evicted the shadow) instead of aborting the copy
// on a hard EACCES. errors.Is(err, ErrSpoolBusy) still works by identity for
// in-package callers/tests.
var ErrSpoolBusy = fmt.Errorf("spool: path busy (prior entry still draining): %w", pin.ErrSpoolBusy)

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
	capacity int64 // 0 means unlimited — the CONFIGURED cap; see effectiveCapacity
	used     atomic.Int64
	meta     *metadata.SpoolStore
	index    *SpoolIndex
	closed   atomic.Bool

	// Live free-disk sample backing effectiveCapacity. The configured capacity
	// is clamped to free disk ONCE, in NewSpoolStore — a snapshot that goes
	// stale the moment anything else consumes the volume (the JuiceFS cache
	// growing, another app, the user's own downloads). These two make the clamp
	// continuous. Sampled at most once per diskAvailTTL so the per-write
	// admission path stays cheap; Statfs here is on the LOCAL spool disk, never
	// through FUSE.
	// capCeiling is the absolute byte ceiling derived at sample time:
	// usedAtSample + max(0, avail - SpoolFreeFloorBytes). It must be computed
	// from a `used` SNAPSHOT taken together with the avail reading, and must NOT
	// be re-derived from the live `used` on each check — see effectiveCapacity.
	// -1 = never successfully sampled.
	capCeiling  atomic.Int64
	diskAvailAt atomic.Int64 // UnixNano of that sample
	ceilingMu   sync.Mutex   // single-flights the refresh; fast path is lock-free

	// lastReleaseNanos is the wall-clock (UnixNano) of the most recent
	// capacity release — i.e. the last time the drainer (or a write
	// rollback) freed spool bytes. The capacity-stall waiter uses it as a
	// drain-progress heartbeat: while ONLINE, a full spool stalls writes
	// indefinitely (the hard NFS mount makes the client wait) AS LONG AS
	// the drain keeps freeing space; only if NOTHING drains for the wedge
	// backstop window does the stall give up with ErrSpoolFull (a genuinely
	// wedged drain, which the user should see rather than hang forever).
	// While OFFLINE the waiter never gives up — it stalls until the user
	// reconnects and the drain frees headroom. See waitForCapacity.
	lastReleaseNanos atomic.Int64

	// stallWaiters counts writes currently parked in the capacity stall
	// (spool full). Surfaced via metrics so the app can show "offline
	// buffer full — N copies waiting, reconnect to drain" instead of the
	// bare "disk is full" the NFS NOSPC used to produce.
	stallWaiters atomic.Int64
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

const (
	// SpoolFreeFloorBytes is the PREFERRED disk space the spool leaves free when
	// auto-sizing or clamping its capacity, so the OS and the JuiceFS cache that
	// shares the same SSD have headroom. It is larger than the OS hard floor
	// because the spool can hold an entire un-drained SD-card burst.
	SpoolFreeFloorBytes = int64(20) << 30 // 20 GiB

	// SpoolHardFreeFloorBytes is the floor the write path never crosses, even on
	// a machine that was already below the preferred 20 GiB floor when the app
	// started. It mirrors health/cacheFreeFloorBytesConst. Keeping the two at
	// 10 GiB prevents the mount from consuming the operating system's last free
	// space while still leaving a bounded working window for ordinary writes.
	// TestSpoolHardFloorMirrorHasNotDrifted guards the mirror.
	SpoolHardFreeFloorBytes = int64(10) << 30 // 10 GiB

	// spoolLowDiskWorkingSetBytes is the maximum instantaneous admission window
	// below the preferred floor. Without it, 19.9 GiB free becomes the 1-byte
	// sentinel and a 58-byte Finder write parks for the full capacity backstop.
	// A 512 MiB window lets metadata and ordinary documents close and drain; it
	// is small enough that a low-disk machine remains protected. Re-sampling may
	// move this window as completed entries drain, but admission still stops at
	// SpoolHardFreeFloorBytes.
	spoolLowDiskWorkingSetBytes = int64(512) << 20 // 512 MiB
)

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

// AutoSpoolCapacityBudget is the LOGICAL budget used when the user has not set
// an explicit spool size. It is deliberately far larger than any disk: in Auto
// mode the real bound is the LIVE ceiling (effectiveCapacity → cachedCeiling →
// spoolHeadroomBytes), which tracks free disk plus reclaimable JuiceFS cache and
// moves in BOTH directions.
const AutoSpoolCapacityBudget = int64(1) << 50 // 1 PiB

// failOpenMaxBytes bounds the fail-open path. `effectiveCapacity` deliberately
// fails OPEN when free disk is unreadable — an unreadable statfs is not a full
// disk — but with an Auto budget of 1 PiB, "open" would mean unlimited. When we
// cannot see the disk we admit a conservative amount instead of everything.
const failOpenMaxBytes = int64(8) << 30 // 8 GiB

// AutoSpoolCapacity is the default spool capacity when JM_SPOOL_SIZE_GB and the
// preference are unset.
//
// IT IS NO LONGER A DISK SNAPSHOT (2026-08-04). It used to return
// max(8 GiB, avail - SpoolFreeFloorBytes) sampled once at boot, and that value
// became s.capacity for the process lifetime. Because effectiveCapacity is
// min(capacity, ceiling), a boot on a low-disk machine pinned the budget at the
// 8 GiB floor — so the reclaimable-cache headroom (spool_headroom.go) could
// raise the live ceiling to ~100 GiB and a 40 GB camera file would STILL be
// refused, bounded by a number sampled when the app happened to start.
//
// That is the same one-way ratchet removed from NewSpoolStore in b77df6c, one
// level up: a startup snapshot cannot recover, and Auto mode has no business
// carrying a disk-derived number at all. In Auto the live ceiling IS the policy,
// so the configured value gets out of its way.
//
// An EXPLICIT user budget is still honoured exactly as before — it is a ceiling
// the user chose, and effectiveCapacity clamps it live on top.
func AutoSpoolCapacity(dir string) int64 {
	return AutoSpoolCapacityBudget
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
	// THE CONFIGURED CAPACITY IS NOT CLAMPED HERE. It used to be, and that made
	// the clamp a ONE-WAY RATCHET.
	//
	// Found live (2026-08-04): the app started with 21 GiB free, so this baked
	// 1.49 GiB into s.capacity permanently. Disk later recovered to 25.5 GiB —
	// and the spool was still stuck at 1.49 GiB, because effectiveCapacity only
	// ever takes min(configured, live ceiling) and can never rise above a
	// configured value that was itself a startup snapshot. A copy of 2 GiB would
	// stall with ~5.4 GiB of real headroom sitting there. That is the same
	// startup-snapshot bug the live clamp was added to fix, just in the other
	// direction.
	//
	// effectiveCapacity already enforces exactly this bound (used + avail -
	// floor) on every admission and re-samples it once per second, so clamping
	// here bought nothing except the inability to recover. Logged, not applied.
	if capacity > 0 {
		if avail, err := spoolDiskAvail(root); err == nil && avail > 0 {
			if maxCap := avail - SpoolFreeFloorBytes; maxCap > 0 && capacity > maxCap {
				jmlog.Info("spool: free disk is currently below the configured capacity — "+
					"admission is clamped live and will recover as disk frees",
					"configured_gb", capacity>>30, "effective_now_gb", maxCap>>30,
					"free_gb", avail>>30)
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
	// Seed the live ceiling synchronously, before anyone can admit a byte.
	// cachedCeiling's single-flight is non-blocking, so a caller that loses the
	// TryLock race falls through to whatever capCeiling holds — and the zero
	// value would read as the "0 == unlimited" sentinel. -1 means "unknown",
	// which fails OPEN to the configured cap; this first real sample replaces it.
	s.capCeiling.Store(-1)
	s.refreshCeiling()

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

// diskAvailTTL bounds how often the disk ceiling is re-sampled. Short enough
// that a filling volume is noticed within a couple of writes, long enough that a
// burst of small writes doesn't Statfs per write.
const diskAvailTTL = 1 * time.Second

// minClampedCeiling is returned instead of 0 whenever the disk clamp is active
// and computes a ceiling of zero.
//
// WHY (2026-08-03 review, CRITICAL): every admission gate spells "unlimited" as
// `ec <= 0`. A legitimately-computed ceiling of exactly 0 — which happens when
// the spool is EMPTY and free disk is at or under the floor, the normal resting
// state of a chronically low-disk machine between copies — would collide with
// that sentinel and silently re-open admission with no cap at all, at precisely
// the moment the disk is most constrained. A ceiling of 1 byte rejects every
// real reservation while staying safely positive.
const minClampedCeiling = int64(1)

// staleCeilingMax is how old a free-disk sample may get before we stop trusting
// it entirely. Generous relative to diskAvailTTL so ordinary scheduling jitter
// never trips it; short enough that a wedged statfs cannot leave admission
// running on a frozen snapshot indefinitely.
const staleCeilingMax = 30 * diskAvailTTL

// refreshCeiling re-samples free disk and recomputes capCeiling.
//
// The headroom it derives is NOT free disk alone: the JuiceFS block cache shares
// this SSD and is mostly reclaimable copies of objects already durable in MinIO,
// so it counts as space the spool may use. See nfs/spool_headroom.go for the
// formula, the three safety gates (writeback off, never below the pinned set,
// never any I/O on this path), and why a missing figure degrades to the smaller,
// historical number. cacheReclaimSnapshot() is a lock-free snapshot read — this
// function is still just one statfs, exactly as before.
//
// The `used` snapshot MUST be taken here, alongside avail, and the resulting
// ceiling treated as absolute for the whole TTL window. Deriving the ceiling
// from the live `used` on every check was the CRITICAL bug found in review:
// `cur+delta > used+headroom` reduces algebraically to `delta > headroom` PER
// CALL, so the ceiling chased `used` upward and cumulative admission inside one
// window was unbounded — six 9 GiB reservations all cleared a 10 GiB headroom.
// Pinning it to the snapshot makes the window admit at most `headroom` in total,
// which is the entire point of the clamp.
func (s *SpoolStore) refreshCeiling() {
	avail, err := spoolDiskAvail(s.root)
	// Stamp AFTER the syscall returns, from our own clock. Taking `now` before
	// the call (and before any lock) meant a slow statfs stored a timestamp that
	// was already expired, so the very next caller re-refreshed immediately and
	// the single-flight did nothing (2026-08-04 review, MEDIUM).
	now := time.Now().UnixNano()
	if err != nil || avail < 0 {
		// Silent fail-open was flagged in review as unobservable; log once per
		// sample so a machine where statfs reliably fails is diagnosable rather
		// than quietly running unclamped.
		jmlog.Warn("spool: free-disk sample failed — capacity clamp inactive this window",
			"root", s.root, "error", fmt.Sprint(err))
		s.capCeiling.Store(-1)
		s.diskAvailAt.Store(now)
		return
	}
	headroom := spoolHeadroomBytes(avail, cacheReclaimSnapshot())
	ceiling := s.used.Load() + headroom // snapshot, NOT re-read per check
	if ceiling < minClampedCeiling {
		ceiling = minClampedCeiling
	}
	s.capCeiling.Store(ceiling)
	s.diskAvailAt.Store(now)
}

// cachedCeiling returns the sampled absolute ceiling, resampling at most once
// per diskAvailTTL PER STORE. ok=false means free disk is unreadable.
//
// The refresh is single-flighted under ceilingMu. Without it the TTL check was
// per-goroutine, not per-store: N waiters parked in waitForCapacity — a parallel
// Finder copy with several files blocked, i.e. precisely the scenario this
// feature exists for — would all observe the same expiry and each issue its own
// statfs, and on failure each emit its own unrate-limited log line
// (2026-08-04 review, MEDIUM). The fast path stays lock-free; only the expiry
// path takes the mutex, and the re-check inside it means just one of the N
// actually samples.
func (s *SpoolStore) cachedCeiling() (int64, bool) {
	if at := s.diskAvailAt.Load(); at == 0 || time.Now().UnixNano()-at >= int64(diskAvailTTL) {
		// NON-BLOCKING single-flight. TryLock, never Lock: whoever wins does the
		// statfs, everyone else keeps the current value and moves on.
		//
		// A blocking Lock here was a HIGH defect (2026-08-04 review). statfs has
		// no timeout and can hang on a wedged volume, and this function is
		// reached from tryReserveCapacity/effectiveCapacity — which callers
		// invoke while HOLDING a path shard (OpenWrite) or the per-entry e.mu
		// (WriteAt, Truncate). One stuck refresh would therefore serialize every
		// OpenWrite hashing to that shard AND block concurrent readers of a file
		// still being written (the in-flight read-shadow path), which is exactly
		// the class of stall the shard split exists to prevent. It also violated
		// this file's own rule that try() be "cheap and lock-free".
		//
		// A loser using a slightly stale ceiling is normally harmless — one poll
		// of staleness, absorbed by the floor. But it is NOT bounded to one TTL:
		// if the WINNER's statfs is the thing that hangs, it holds the slot for
		// the whole hang and every loser keeps reading the same frozen value.
		// staleCeilingMax below is the backstop for exactly that.
		if s.ceilingMu.TryLock() {
			if at := s.diskAvailAt.Load(); at == 0 || time.Now().UnixNano()-at >= int64(diskAvailTTL) {
				s.refreshCeiling()
			}
			s.ceilingMu.Unlock()
		}
	}
	// Hard staleness backstop. If the sample is far older than the TTL — which
	// happens when the refresh slot is held by a statfs stuck on a wedged volume
	// — we can no longer see the disk, so we stop pretending the last reading is
	// still true. Freeze admission at what is already used rather than keep
	// filling a volume we cannot measure: admitting blind against a stale-roomy
	// ceiling is precisely the founder incident this whole change exists to fix.
	// The resulting stall is bounded by the wedge backstop in waitForCapacity,
	// so this degrades to an honest ErrSpoolFull rather than a hang.
	if at := s.diskAvailAt.Load(); at != 0 && time.Now().UnixNano()-at >= int64(staleCeilingMax) {
		frozen := s.used.Load()
		if frozen < minClampedCeiling {
			frozen = minClampedCeiling // never collide with the 0 == unlimited sentinel
		}
		return frozen, true
	}
	c := s.capCeiling.Load()
	if c < 0 {
		return 0, false
	}
	return c, true
}

// DiskConstrained reports whether the live free-disk clamp — not the configured
// budget — is what is currently blocking admission. True means more spool bytes
// cannot be accepted because the VOLUME is at its floor.
//
// Used by the capacity stall to decide whether waiting can ever help: while
// offline, a stall normally waits forever on the assumption that reconnecting
// lets the drain free space. That assumption is false when the blocker is local
// disk consumed by something other than the spool, and waiting forever there is
// an unbounded Finder hang with no error. See waitForCapacity.
func (s *SpoolStore) DiskConstrained() bool {
	if s.capacity <= 0 {
		return false
	}
	ceiling, ok := s.cachedCeiling()
	if !ok {
		return false
	}
	// The DISK is the blocker only when the clamp is below the configured budget
	// AND there is effectively no headroom left under it. Compare REMAINING
	// headroom rather than `used >= ceiling`: at the floor with an empty spool
	// the ceiling is the 1-byte sentinel, so `0 >= 1` would read as unconstrained
	// exactly when the volume is fullest.
	remaining := ceiling - s.used.Load()
	if remaining < 0 {
		remaining = 0
	}
	return ceiling < s.capacity && remaining <= minClampedCeiling
}

// effectiveCapacity is the cap to enforce RIGHT NOW: the configured capacity,
// further clamped so the spool can never grow into the free-disk floor.
//
// WHY THIS EXISTS (2026-08-03, founder incident: "copying to a JuiceMount volume
// when local disk is low stalls"). NewSpoolStore clamps the configured capacity
// to free disk exactly once, at construction. That snapshot is wrong the moment
// anything else eats the volume — and the thing most likely to eat it is us: the
// JuiceFS cache grows alongside the spool. Live evidence from the incident: a
// configured capacity of ~37 GB while only 50 GiB was free, i.e. capacity plus
// the 20 GiB floor already exceeded the disk.
//
// The failure that produces is not a clean ENOSPC. Admission keeps saying yes
// until `used >= capacity`, but the DISK runs out first, so the write fails
// underneath the spool and the drainer has nowhere to drain — and because a full
// spool is designed to STALL (backpressure, so Finder paces itself instead of
// aborting the copy), the user gets an unbounded hang rather than an error.
//
// SCOPE LIMIT, deliberate: a configured capacity of 0 stays truly unlimited and
// is NOT disk-clamped. "total=0 means unlimited" is the documented /spool
// contract that consumers branch on, and production never runs unlimited —
// AutoSpoolCapacity always yields a positive cap, and both call sites reject a
// sub-1 GiB configuration. Bounding it here would change a public contract to fix
// an incident that cannot occur with it.
func (s *SpoolStore) effectiveCapacity() int64 {
	if s.capacity <= 0 {
		return s.capacity // unlimited stays unlimited (see SCOPE LIMIT above)
	}
	ceiling, ok := s.cachedCeiling()
	if !ok {
		// Fail OPEN — an unreadable statfs is not a full disk. An EXPLICIT user
		// budget is returned in full, unchanged: the user chose that number and
		// losing sight of the disk is not a reason to override them.
		//
		// The AUTO budget is the exception, and only because it is a sentinel
		// rather than a real limit: at 1 PiB an unbounded fail-open would mean
		// "unlimited" at exactly the moment we cannot see the disk. In Auto the
		// live ceiling IS the policy, so with no ceiling there is no policy —
		// admit a conservative amount instead of everything.
		if s.capacity == AutoSpoolCapacityBudget {
			return failOpenMaxBytes
		}
		return s.capacity
	}
	if s.capacity < ceiling {
		return s.capacity
	}
	return ceiling
}

// Capacity returns (used, total) bytes. total=0 means unlimited.
//
// total is the EFFECTIVE capacity — the configured cap clamped to current free
// disk — so /spool and the menu bar show the budget actually being enforced
// rather than a startup snapshot that may be far larger than the disk allows.
func (s *SpoolStore) Capacity() (used, total int64) {
	return s.used.Load(), s.effectiveCapacity()
}

// ConfiguredCapacity returns the capacity as configured at construction, before
// the live free-disk clamp. Diagnostics only — admission must use
// effectiveCapacity.
func (s *SpoolStore) ConfiguredCapacity() int64 { return s.capacity }

// StallWaiters returns the number of writes currently parked in the capacity
// stall (spool full, blocking for the drainer to free headroom). Surfaced via
// /spool so the app can show "offline buffer full — N copies waiting".
func (s *SpoolStore) StallWaiters() int64 {
	return s.stallWaiters.Load()
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
	// sawClosed records that we encountered a FINALIZED (closed) entry for this
	// path — i.e. this is a reopen of a file that is draining, not a brand-new
	// create. If that entry then drains+evicts, we must NOT create a fresh spool
	// entry (its drain would os.Create-truncate the just-drained backend file and
	// clobber it); the write is bounced to the in-place fdPool path instead.
	var sawClosed bool

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
			sawClosed = true
			busyID := existing.id
			existing.mu.Unlock()
			shard.Unlock()
			if waited == 0 {
				// First park for this OpenWrite: a WRITE (classically a
				// faststart moov seek-back at end-of-export) arrived for a path
				// whose prior entry finalized mid-copy and is still draining.
				jmlog.Info("spool: OpenWrite waiting for finalized-not-drained entry to evict",
					"path", nfsPath, "entry_id", busyID)
			}
			if waited >= reopenMaxWait {
				jmlog.Warn("spool: OpenWrite gave up waiting for drain-evict — returning ErrSpoolBusy (client retries via JUKEBOX)",
					"path", nfsPath, "entry_id", busyID,
					"waited_ms", waited.Milliseconds(), "reopen_max_ms", reopenMaxWait.Milliseconds())
				return nil, ErrSpoolBusy
			}
			time.Sleep(reopenPoll)
			waited += reopenPoll
			continue
		}

		// A reopen whose prior (finalized) entry has now drained+evicted: do NOT
		// create a fresh spool entry. That fresh entry's drain would
		// os.Create-truncate the just-drained backend file and clobber it (probe:
		// TestReopenAfterDrainSpoolLevelCorruptionProbe). The file is durably in
		// FUSE now, so bounce the write with ErrSpoolBusy → NFS3ERR_JUKEBOX; the
		// client retries, LookupActive is then false, and the handler routes it
		// to the in-place fdPool path (no truncate). Only a genuinely NEW path
		// (never saw a closed entry) falls through to create a fresh entry.
		if sawClosed {
			shard.Unlock()
			jmlog.Info("spool: OpenWrite reopen after drain-evict — deferring to in-place FUSE path (no destructive fresh entry)",
				"path", nfsPath, "waited_ms", waited.Milliseconds())
			return nil, ErrSpoolBusy
		}

		// No entry for this path — create a fresh one under the path shard.
		if ec := s.effectiveCapacity(); ec > 0 && s.used.Load() >= ec {
			// Spool full. Release the shard and STALL — block (per
			// waitForCapacity) for the drainer to free headroom rather than
			// hard-fail the create, so a large copy paces itself to drain
			// throughput and an OFFLINE copy stalls to zero speed until the
			// user reconnects, instead of aborting the whole Finder copy with
			// NOSPC ("disk full"). waitForHeadroom returns false only on a
			// store close or a genuinely WEDGED online drain (no bytes freed
			// for the backstop window) — then we surface ErrSpoolFull. The
			// hard,intr NFS mount makes the client wait through the stall.
			shard.Unlock()
			if !s.waitForHeadroom() {
				return nil, ErrSpoolFull
			}
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
	// v0.2.1 offline-transition resilience: never finalize/escalate while
	// offline. finalizeIfIdle would CLOSE an in-flight entry whose Finder copy
	// merely PAUSED across the offline toggle (refcount briefly drops to 0
	// between WRITE RPCs); the drainer is paused offline (drainer.go), so the
	// closed entry can't drain, and a resumed WRITE then hits OpenWrite's
	// reopen-poll → reopenMaxWait → ErrSpoolBusy → the client's copy dies
	// (Finder mislabels it "name too long/invalid"). Holding entries in
	// `writing` across the offline window lets a resumed WRITE reuse them via
	// OpenWrite's !closed branch; on reconnect the next sweep finalizes any
	// genuinely-idle entry → drains. Durability is safe across an offline
	// restart: RecoverOnBoot's DrainWriting case hashes the on-disk file (not
	// the DB Size, which stays 0 until finalize) and resumes a `writing` row
	// with real bytes to `ready`, so an offline-ingested file is never lost.
	if pin.IsOffline() {
		return 0
	}
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

// HasPending reports whether volRelPath has a live spool entry that has not
// yet drain-succeeded — i.e. the file was just created/written here and is
// still on local SSD, NOT yet in MinIO/Redis/FUSE. True from juiceFS.Create's
// OpenWrite (s.index.Insert) until the entry is evicted: drain success
// (MarkDrainComplete), delete (CancelForDelete), corruption (QuarantineDrain),
// or rename (Index.Move re-keys it under the new path). Those eviction points
// are exactly the moments the path stops being "live-and-local", so an O(1)
// index membership probe is the precise spool-pending signal.
//
// QA-30 Layer D (NFSv3 sprint): the metadata reconcile's scopedPrune calls
// this (via the injected spoolPending guard on RedisClient) to NEVER prune a
// spool-pending path. Such a path is absent from Redis (not yet drained) and
// absent from FUSE (only on the spool), so the existing Layer A FUSE-Lstat and
// Layer C pin guards both MISS it — leaving the wrong prune to Forget the
// kernel's path-stable (Track B) handle and surface ESTALE mid-copy (live
// build-438 error 100070, a 4596-burst of "FromHandle STALE").
//
// volRelPath MUST be keyed exactly as OpenWrite keys the index: the NFS
// filename with no leading slash and no mount prefix (the JuiceFS-internal /
// store path scheme — Create passes the same string to both store.MakeEntry
// and OpenWrite, so a scopedPrune candidate's store path is already this key).
//
// Lock discipline: this takes ONLY the SpoolIndex RWMutex (a quick independent
// RLock via Index.Lookup) — it never touches s.mu, e.mu, or any path shard, so
// there is no lock-ordering hazard with the spool's other locks. It is a
// reconcile-path call (scopedPrune), kept off the NFS GETATTR/LOOKUP and
// ToHandle/FromHandle hot paths per the QA-35 perf-discipline gate.
func (s *SpoolStore) HasPending(volRelPath string) bool {
	if s == nil || s.index == nil {
		return false
	}
	_, ok := s.index.Lookup(volRelPath)
	return ok
}

// EvictIndex removes the in-memory index entry for nfsPath, but ONLY if the
// currently-indexed entry's row id matches `id` (identity-checked, mirroring
// QuarantineDrain's eviction). A non-matching slot means a newer writer rotated
// in a fresh entry for the same path; we must not stomp it.
//
// Review FIX 3: the drainer's terminal failPermanent path uses this so a
// permanently-failed drain stops reporting HasPending==true forever. Without
// it, the dead entry lingers in the index for the process lifetime (the boot
// scrubber does NOT repopulate the index for failed rows), so QA-30 Layer D
// would treat that path as perpetually spool-pending — making it un-prunable
// even after the user deletes it, leaving a stale store entry + NFS handle that
// can never be cleaned up until restart.
//
// Lock discipline matches HasPending: only the SpoolIndex RWMutex is touched
// (via Lookup + DeleteIfMatches), no s.mu / e.mu / path shard, so there is no
// lock-ordering hazard. Returns true if an entry was actually evicted.
func (s *SpoolStore) EvictIndex(nfsPath string, id int64) bool {
	if s == nil || s.index == nil {
		return false
	}
	if e, ok := s.index.Lookup(nfsPath); ok && e.ID() == id {
		return s.index.DeleteIfMatches(nfsPath, e)
	}
	return false
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
			// A SIZE MATCH NO LONGER PROVES THE FILE IS INTACT.
			//
			// The streaming drain punches drained prefixes out of the spool file,
			// and F_PUNCHHOLE leaves the LOGICAL size unchanged — so a half-punched
			// file passes `diskSize == r.Size` exactly while its first punchedEnd
			// bytes read as zeros. Resuming it would copy the WHOLE file to the
			// backend and overwrite real, already-durable bytes with those zeros:
			// silent, total loss of the streamed prefix.
			//
			// Reading punched_end per row here (rather than widening the four shared
			// row SELECTs) keeps this narrow: it runs once per failed row on boot,
			// not on any hot path. A query error is treated as "possibly punched" —
			// the conservative direction, since preserving a file for the operator
			// costs disk while resuming a punched one costs footage.
			punchedEnd, peErr := s.meta.PunchedEnd(r.ID)
			if peErr != nil {
				log.Printf("spool recover: cannot read punched_end for %d (%v) — "+
					"PRESERVING %s rather than risk re-draining a punched file",
					r.ID, peErr, r.SpoolFile)
				break
			}

			switch {
			case punchedEnd > 0:
				// Streamed and partly reclaimed. The backend already holds
				// [0,punchedEnd) durably; the spool file cannot reconstruct it.
				// Preserve for the operator rather than destroy or re-drain.
				log.Printf("spool recover: row %d was streamed (punched_end=%d) — "+
					"NOT resuming from the spool file, whose first %d bytes are holes. "+
					"The backend holds that prefix; preserving %s",
					r.ID, punchedEnd, punchedEnd, r.SpoolFile)
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
	s.completeDrainCleanup(id, nfsPath, spoolFile, size)
	return true, nil
}

// completeDrainCleanup runs the post-MarkDone side of a successful drain: evict
// the in-memory index entry, release the capacity reservation, note drain
// progress (keep-awake), remove the spool file, and append the audit manifest
// record. Factored out of MarkDrainComplete so the batched drain path
// (JM_DRAIN_BATCH_INSERT — SpoolStore.BatchCompleteDrainCleanup) runs the EXACT
// same cleanup after its single-transaction mark-done commit; the per-file and
// batched paths stay behavior-identical from here on.
//
// MUST be called ONLY after the row's SQL mark-done has durably committed (task
// #65: the spool shadow's eviction — index delete + file removal here — happens
// strictly after the size publish + mark-done are on disk).
func (s *SpoolStore) completeDrainCleanup(id int64, nfsPath, spoolFile string, size int64) {
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
	// Drain-tail keep-awake: a file genuinely reached the backend, so the Mac
	// should stay awake to finish the rest of a post-copy / post-reconnect drain
	// before idle-sleeping. Only the real drain-complete path bumps this (not the
	// shared releaseCapacity), so deletes/refunds never hold the assertion.
	nfslib.NoteDrainProgress()
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
}

// BatchCompleteDrainCleanup runs completeDrainCleanup for one already-committed
// batched drain (JM_DRAIN_BATCH_INSERT). The size publish + mark-done for this
// id were committed in metadata.Store.BatchDrainComplete's single transaction;
// this performs the identical post-commit cleanup the per-file
// MarkDrainComplete does after its own MarkDone. Exported so the drainer's
// batch flusher (a different package method surface than MarkDrainComplete) can
// reuse it verbatim.
func (s *SpoolStore) BatchCompleteDrainCleanup(id int64, nfsPath, spoolFile string, size int64) {
	s.completeDrainCleanup(id, nfsPath, spoolFile, size)
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
func (s *SpoolStore) MigrateForRename(oldPath, newPath string) (int, bool, error) {
	s.lockAllShards()
	defer s.unlockAllShards()

	migs, err := s.meta.MigrateActivePaths(oldPath, newPath)
	if err != nil {
		return 0, false, err
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
	// NOTE (2026-08-03): we deliberately do NOT wake the drainer here, and
	// return the need-to-signal to the caller instead.
	//
	// Waking it inside this call raced the very rename that called us. The
	// drainer materializes an entry by os.MkdirAll'ing its parent
	// (drainer.go:699), so a drain of a just-migrated entry CREATES the
	// destination directory on FUSE — and the caller's os.Rename(old, new)
	// then fails EEXIST because we created `new` ourselves microseconds
	// earlier. JuiceFS does not implement the POSIX "rename replaces an
	// empty target directory" case, so an empty dir is fatal to the rename.
	//
	// Symptom: moving a folder in Finder that had just been written failed
	// ~50% of the time (300-file folder, measured); the folder was left split
	// across source and destination. Settled folders never failed because
	// nothing was in the spool to migrate or drain. See JuiceMount task #2.
	//
	// The caller signals after its rename completes — see juiceFS.Rename.
	return len(migs), requeued, nil
}

// SignalReady wakes the drainer. Exported so a caller that deferred the wake
// (see MigrateForRename) can perform it once its own FUSE work is done.
func (s *SpoolStore) SignalReady() { s.signalReady() }

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
		if ec := s.effectiveCapacity(); ec > 0 && cur+delta > ec {
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
	// capacityWaitDeadline is the ONLINE "wedge backstop": how long the
	// capacity stall tolerates the drain making ZERO progress (no spool bytes
	// freed) before giving up with ErrSpoolFull. It is NOT a fixed total wait —
	// any drain release resets it (see waitForCapacity), so a slow link that
	// keeps draining files stalls/paces INDEFINITELY and never fails; only a
	// genuinely WEDGED drain (online, nothing freed for this whole window)
	// surfaces as NOSPC instead of hanging the client forever.
	//
	// While OFFLINE the stall is unconditional — it waits until the user
	// reconnects and the drain frees headroom, never failing. (Pre-2026-06-15
	// this was a fixed 30s deadline that DID fail: a copy that filled the spool
	// while offline hit "disk is full" because the paused drain could never
	// free space. The user's explicit requirement is that a full spool stall to
	// zero speed and resume on reconnect, not abort.)
	//
	// 2 min (was 30s): the old value was kept under the NFS SOFT-mount timeout
	// (~40s) to avoid ETIMEDOUT, but the mount is now HARD,intr — the client
	// waits across an arbitrarily long stall and the user can cancel — so the
	// backstop is sized for "is the drain actually wedged" rather than the mount
	// timeout. Env JM_SPOOL_WEDGE_BACKSTOP_SEC overrides; 0 = never give up
	// (infinite stall even online).
	capacityWaitDeadline = envBackstop("JM_SPOOL_WEDGE_BACKSTOP_SEC", 2*time.Minute)
)

// envBackstop reads a non-negative seconds value from env (0 allowed = infinite),
// falling back to def. Bad/empty values use def.
func envBackstop(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return time.Duration(n) * time.Second
		}
	}
	return def
}

// waitForCapacity is the spool's flow-control valve. It blocks until try()
// succeeds (capacity reserved / headroom available), the store closes, or — only
// while ONLINE and the drain has frozen for capacityWaitDeadline — gives up
// (false → ErrSpoolFull at the call site). A full spool therefore STALLS the
// copy to zero speed (the hard NFS mount keeps the client waiting) instead of
// failing it: offline ⇒ stall until reconnect; online + draining ⇒ pace to drain
// throughput. try() is re-evaluated each poll and MUST be cheap and lock-free.
//
// Callers MUST NOT hold a path shard or e.mu across this call — it can park for a
// long time and holding those would stall the sweeper and every other open.
//
// touch (may be nil) is invoked each poll while parked. The WriteAt path passes a
// closure that refreshes the entry's lastWrite, so a write parked here for minutes
// (offline buffer full) is NOT mistaken by the sweeper's leaked-handle escalation
// for a quiescent abandoned handle and force-finalized TRUNCATED — that would
// silently lose the rest of the file. New-file OpenWrite has no entry yet and
// passes nil.
func (s *SpoolStore) waitForCapacity(try func() bool, touch func()) bool {
	if try() {
		return true
	}
	s.stallWaiters.Add(1)
	defer s.stallWaiters.Add(-1)
	lastSeen := s.lastReleaseNanos.Load()
	frozenSince := time.Now()
	wasOffline := pin.IsOffline()
	for {
		if s.closed.Load() {
			return false
		}
		if touch != nil {
			touch()
		}
		time.Sleep(capacityWaitPoll)
		if try() {
			return true
		}
		// TWO INDEPENDENT FACTS, never collapsed into one variable.
		//
		// `offline` is REAL connectivity and nothing else — the offline→online
		// edge below is the only correct consumer of it. An earlier version
		// overwrote this var to force the backstop on, which silently poisoned
		// `wasOffline`: once disk-pressure had flipped it, a genuine reconnect
		// was invisible to the edge check and the reconnected drain never got
		// its fresh window (2026-08-04 review, HIGH — measured: reconnecting
		// mid-stall changed the give-up time by 3ms, i.e. not at all).
		offline := pin.IsOffline()
		// `backstopApplies` answers a different question: can waiting EVER help?
		// Online, yes — the drain may free space. Offline, only if the blocker is
		// the link; if the blocker is the local disk (JuiceFS cache, Spotlight,
		// Time Machine, the user's own files) then reconnecting frees nothing and
		// waiting forever is an unbounded Finder hang on a hard mount.
		backstopApplies := !offline || s.DiskConstrained()

		// Reset the wedge window ONLY on real progress, or on a real reconnect.
		//
		// Deliberately NOT reset merely because we are offline, and NOT on a
		// backstopApplies rising edge. Both were resets driven by STATE rather
		// than PROGRESS, and a state that flaps then means the window never
		// closes: DiskConstrained() is driven by a ~1s statfs sampled right at
		// the floor boundary — the resting state of a chronically low-disk
		// machine — so it flaps in normal operation. With a state-driven reset
		// that flapping made the backstop never fire and restored the exact
		// unbounded hang this was meant to remove (2026-08-04 review, CRITICAL —
		// measured: no return within 500ms at 12.5x the deadline).
		//
		// Consequence, accepted knowingly: a long purely-offline stall that later
		// becomes disk-constrained inherits a stale frozenSince and may give up
		// almost at once. That is the right answer — nothing has drained for the
		// whole window AND the volume is now full, so a prompt honest ENOSPC
		// beats another hour of silence.
		if rel := s.lastReleaseNanos.Load(); rel != lastSeen || (wasOffline && !offline) {
			lastSeen = rel
			frozenSince = time.Now()
		}
		wasOffline = offline // pure connectivity — edge detection stays intact
		// Give up only where waiting cannot help and nothing has freed capacity
		// for the whole window. capacityWaitDeadline <= 0 means "stall forever".
		if backstopApplies && capacityWaitDeadline > 0 && time.Since(frozenSince) >= capacityWaitDeadline {
			return false
		}
	}
}

// reserveCapacityOrWait reserves delta bytes, stalling (per waitForCapacity) for
// the drainer to free space if the cap is currently exhausted. Returns true with
// delta reserved, false only on a wedged online drain / store close. Callers MUST
// NOT hold a per-entry e.mu or path shard across this call. touch (may be nil) is
// forwarded to waitForCapacity — see there.
func (s *SpoolStore) reserveCapacityOrWait(delta int64, touch func()) bool {
	return s.waitForCapacity(func() bool { return s.tryReserveCapacity(delta) }, touch)
}

// waitForHeadroom stalls (per waitForCapacity) until the spool drops below its
// cap, so a new-file OpenWrite during a full spool pauses instead of hard-failing
// the copy. Returns true once there is headroom, false only on a wedged online
// drain / store close. Must not be called with a shard held.
func (s *SpoolStore) waitForHeadroom() bool {
	return s.waitForCapacity(func() bool {
		ec := s.effectiveCapacity()
		return ec <= 0 || s.used.Load() < ec
	}, nil)
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
			// Drain-progress heartbeat for the capacity stall waiter. ANY release
			// (drain-complete, fail, write-refund, truncate-shrink) frees spool
			// bytes, so all of them legitimately reset the stall's wedge backstop.
			// NOTE: the keep-awake drain-tail bump (NoteDrainProgress) is NOT here
			// — it fires only on a GENUINE drain-to-backend (MarkDrainComplete),
			// so deletes/refunds don't spuriously hold the Mac awake.
			s.lastReleaseNanos.Store(time.Now().UnixNano())
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

// ClearFailedItem describes one FAILED spool entry that ClearFailed would
// discard. The JSON-friendly preview the UI shows before the user confirms.
type ClearFailedItem struct {
	Path      string `json:"path"`
	Size      int64  `json:"size"`
	Attempts  int    `json:"attempts"`
	LastError string `json:"last_error"`
}

// ClearFailed PERMANENTLY discards FAILED spool rows: it removes the DB row and
// deletes the local spool file (the bytes that never reached the backend). This
// is DESTRUCTIVE — for a file whose source no longer exists, the spool file is
// the only copy. So it mutates ONLY when confirm=true; with confirm=false it
// returns the preview list (and total bytes) and changes nothing, letting the UI
// warn ("N files / X GB never uploaded — discard?") before the user commits.
//
// Failed rows are NOT part of the `used` capacity budget (failPermanent already
// released them, see RetryFailed's note), so we do NOT touch s.used here. A
// NEWER row for the same path is irrelevant to clearing (unlike retry, we're not
// resurrecting bytes) — we still discard this terminal failed row. Returns the
// preview items (always), the count actually cleared (0 when !confirm), and the
// bytes freed.
func (s *SpoolStore) ClearFailed(confirm bool) (items []ClearFailedItem, cleared int, bytes int64, err error) {
	rows, err := s.meta.ListAll()
	if err != nil {
		return nil, 0, 0, err
	}
	for _, r := range rows {
		if r.DrainState != metadata.DrainFailed {
			continue
		}
		items = append(items, ClearFailedItem{
			Path:      r.NFSPath,
			Size:      r.Size,
			Attempts:  r.DrainAttempts,
			LastError: r.LastError,
		})
		if !confirm {
			continue
		}
		// Delete the un-drained local spool file first. A leaked file is a
		// far better failure mode than a dangling DB row, so we proceed to
		// delete the row even if the unlink fails (ENOENT is expected for
		// quarantined/boot-scrubbed rows whose file already moved).
		if r.SpoolFile != "" {
			if rmErr := os.Remove(r.SpoolFile); rmErr != nil && !os.IsNotExist(rmErr) {
				log.Printf("spool: clear-failed remove file %d (%s): %v", r.ID, r.SpoolFile, rmErr)
			}
		}
		if delErr := s.meta.Delete(r.ID); delErr != nil {
			log.Printf("spool: clear-failed delete row %d (%s): %v", r.ID, r.NFSPath, delErr)
			continue
		}
		cleared++
		bytes += r.Size
		jmlog.Info("spool: clear-failed discarded a permanently-failed entry",
			"path", r.NFSPath, "bytes", r.Size, "attempts", r.DrainAttempts, "last_error", r.LastError)
	}
	if confirm && cleared > 0 {
		jmlog.Warn("spool: clear-failed discarded failed entries (un-drained bytes deleted)",
			"cleared", cleared, "bytes", bytes)
	}
	return items, cleared, bytes, nil
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

	// lastJukeboxLogNs throttles the in-flight-hole JUKEBOX diagnostic log
	// (spoolReadFile.ReadAt) to ~1 line per entry per 2s, so a genuine hold
	// with a retrying client can't flood the log (the JM_LOOKUP_TRACE lesson).
	// Atomic — read/written from the read path without holding mu.
	lastJukeboxLogNs atomic.Int64

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
	// streamDest is the path the streaming drain is writing this entry's backend
	// copy to — the hidden sibling from streamTempPath, before the rename into
	// place. Empty unless streaming is active.
	//
	// The read shadow needs it: everything below punchedEnd has been punched out
	// of the spool file and must be served from here instead (planDest). Guarded
	// by mu and read on the read path, so it is taken in the same snapshot as the
	// boundaries it goes with.
	streamDest string

	// punchedEnd is the end of the prefix the streaming drain has copied to the
	// backend AND punched out of this spool file. Monotonic; guarded by mu.
	//
	// Zero unless streaming is active, which keeps every read path byte-identical
	// to the pre-streaming behaviour until the copier actually runs.
	//
	// ORDERING CONTRACT, and it is not negotiable: this value is PUBLISHED before
	// the punch, never after. Readers route on it, so publishing first sends them
	// to the destination while the spool bytes are still intact (harmless);
	// publishing second would leave a window where a reader is routed at a hole
	// and served zeros with no error.
	punchedEnd int64

	// writtenExtents tracks already-written regions that lie ABOVE
	// contiguousEnd — the out-of-order writes macOS's parallel WRITE dispatch
	// produces (each WRITE RPC runs on its own goroutine over one TCP conn, so
	// they land out of order). When a later write fills the gap below such a
	// region, advanceContiguousLocked coalesces it in so contiguousEnd jumps
	// past ALL now-contiguous bytes — instead of the old single-chunk advance
	// that left contiguousEnd stuck far below writtenEnd (a 1GiB cp was observed
	// stuck at 6MiB) until finalize, JUKEBOX-holding end-of-export re-reads of
	// already-written bytes past the client's soft-mount timeout. Coalesced,
	// sorted by start, every entry strictly above contiguousEnd. Guarded by mu.
	// Bounded small in practice (reorder window is shallow; entries collapse as
	// gaps fill). NEVER holds an unwritten range — an extent is recorded only
	// AFTER a successful pwrite, preserving the never-serve-a-hole invariant.
	writtenExtents []spoolExtent
	hasher         hash.Hash
	sha256         []byte // populated on Close iff streaming hash is trustworthy
	// hashValid tracks whether the streaming hasher reflects the on-disk
	// contents. False once we observe any out-of-order WriteAt (off <
	// current writtenEnd) — sparse / out-of-order writes make the streaming
	// hash diverge from the file's at-rest hash. The drainer (slice B)
	// re-hashes from disk regardless; this flag tells it whether the
	// streaming SHA is a usable optimization or just noise.
	hashValid bool
	closed    bool
	// committed records that the client issued an NFS COMMIT for this path — an
	// explicit durability request. It lets the sweeper finalize the entry on the
	// SHORT idle even when large (#105), so a Premiere save/export finalizes ~a
	// few seconds after its close-COMMIT instead of waiting the full window.
	// Safe because a continuation write during the resulting drain defers to the
	// in-place fdPool path (OpenWrite sawClosed) rather than corrupting.
	committed bool

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

// PunchedEnd returns the end of the prefix that has been drained to the backend
// AND punched out of the spool file. Everything below it reads as ZEROS from the
// spool (measured; punch_darwin.go) and must be served from the destination.
// Monotonic — punching cannot be undone.
func (e *SpoolEntry) PunchedEnd() int64 {
	e.mu.RLock()
	n := e.punchedEnd
	e.mu.RUnlock()
	return n
}

// SetStreamDest records where the streaming drain is writing this entry's
// backend copy. Must be set BEFORE the first punch, or the read shadow will have
// nowhere to route a punched read.
func (e *SpoolEntry) SetStreamDest(path string) {
	e.mu.Lock()
	e.streamDest = path
	e.mu.Unlock()
}

// StreamDestPath returns the in-flight backend copy's path, empty if this entry
// is not being streamed.
func (e *SpoolEntry) StreamDestPath() string {
	e.mu.RLock()
	p := e.streamDest
	e.mu.RUnlock()
	return p
}

// writeBelowPunchedLocked routes a write that lands in (or straddles) the
// punched prefix. Caller holds e.mu.
//
// The part below punchedEnd goes to the destination, which is the only place
// those bytes exist. Any part at or above it falls through to the normal spool
// path, so a straddling write is split rather than rejected — a writer has no
// idea where our punch boundary is and must not be penalised for crossing it.
//
// Refuses when there is no destination: that combination (punched bytes, nowhere
// to write them) means the stream was abandoned, and silently writing into a
// hole would be worse than an error the client can retry.
func (e *SpoolEntry) writeBelowPunchedLocked(p []byte, off int64) (int, error) {
	if e.streamDest == "" {
		return 0, fmt.Errorf("spool: write at %d is below punchedEnd %d but the entry "+
			"has no stream destination: refusing to write into a punched hole",
			off, e.punchedEnd)
	}
	// A BELOW-PUNCH WRITE IS AN OUT-OF-ORDER WRITE, and the normal path's
	// bookkeeping must not be skipped just because the bytes go elsewhere.
	//
	// hashValid: the streaming SHA hashes bytes in write order. A seek-back
	// invalidates it — the normal path sets this on `off < writtenEnd`, and this
	// branch returns before reaching that. Leaving it true would hand the drainer
	// a hash that no longer describes the file, and the spool-side SHA check
	// would then QUARANTINE an intact file on the mismatch. (Bug introduced with
	// the redirect itself, caught by auditing the design doc against the code.)
	//
	// lastWrite: the idle sweeper finalizes entries that have gone quiet. A
	// writer that is actively rewriting below the punch is NOT quiet, and
	// finalizing under it would end the file mid-rewrite.
	//
	// writtenEnd / contiguousEnd are deliberately NOT touched: they describe the
	// SPOOL file's extent, and these bytes are not going there.
	e.hashValid = false
	e.lastWrite.Store(time.Now().UnixNano())

	end := off + int64(len(p))
	belowLen := e.punchedEnd - off
	if belowLen > int64(len(p)) {
		belowLen = int64(len(p))
	}

	f, err := os.OpenFile(e.streamDest, os.O_WRONLY, 0o644)
	if err != nil {
		return 0, fmt.Errorf("spool: open stream destination for a below-punch write: %w", err)
	}
	defer f.Close()
	if _, err := f.WriteAt(p[:belowLen], off); err != nil {
		return 0, fmt.Errorf("spool: write [%d,%d) to stream destination: %w",
			off, off+belowLen, err)
	}
	// Durable before returning. The spool no longer holds these bytes, so an
	// unsynced write here is the only copy sitting in a page cache.
	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("spool: sync stream destination after a below-punch write: %w", err)
	}

	if belowLen == int64(len(p)) {
		return len(p), nil
	}
	// Straddling: the remainder is above the punch and belongs in the spool.
	// Released and re-acquired deliberately — writeAtLocked is not re-entrant and
	// the recursion below re-takes the lock.
	rest := p[belowLen:]
	restOff := off + belowLen
	e.mu.Unlock()
	n, err := e.WriteAt(rest, restOff)
	e.mu.Lock()
	if err != nil {
		return int(belowLen), err
	}
	_ = end
	return int(belowLen) + n, nil
}

// IsClosed reports whether the entry has been finalized — no further writes will
// arrive. The streaming session uses it to decide that a copy is FINISHED rather
// than merely paused between WRITE RPCs, which matters because NFS does
// OpenFile->WriteAt->Close on every RPC and refcount therefore says nothing
// about completion.
func (e *SpoolEntry) IsClosed() bool {
	e.mu.RLock()
	c := e.closed
	e.mu.RUnlock()
	return c
}

// publishPunchedEnd advances the punched boundary. Monotonic — a lower value is
// ignored rather than applied, because punching cannot be undone and readers are
// already routed at the destination for everything below the current value.
//
// MUST be called BEFORE the corresponding punchRange. See advanceStream: publish
// first and a reader briefly reads still-present spool bytes (harmless); punch
// first and a reader is routed at a hole and served zeros with no error.
func (e *SpoolEntry) publishPunchedEnd(end int64) {
	e.mu.Lock()
	if end > e.punchedEnd {
		e.punchedEnd = end
	}
	e.mu.Unlock()
}

// readableBoundsWithPunched returns all three read-routing boundaries under ONE
// lock.
//
// Same reason ReadableBounds takes cend and wend together (task #65): a decision
// assembled from two separate snapshots can classify an offset against a state
// that never existed. Adding punchedEnd as a third independently-read value would
// reintroduce exactly that hazard, and here the consequence is worse than a
// mis-classified hole — it is a read routed at punched bytes, which returns zeros
// with no error.
func (e *SpoolEntry) readableBoundsWithPunched() (cend, wend, punched int64) {
	e.mu.RLock()
	cend = e.contiguousEnd
	wend = e.writtenEnd
	punched = e.punchedEnd
	e.mu.RUnlock()
	return
}

// readRouting returns the boundaries AND the destination path in one lock, so a
// read classified as planDest resolves the path from the same snapshot that
// classified it. Splitting them would let the dest be cleared (rename, rollback)
// between the decision and the open, leaving a punched read with nowhere to go.
func (e *SpoolEntry) readRouting() (cend, wend, punched int64, dest string) {
	e.mu.RLock()
	cend = e.contiguousEnd
	wend = e.writtenEnd
	punched = e.punchedEnd
	dest = e.streamDest
	e.mu.RUnlock()
	return
}

// ReadableBounds returns the contiguous-written prefix end (cend — the readable
// boundary) and the high-water written end (wend — the highest offset any WRITE
// has reached) in a SINGLE RLock, so a read past cend is classified against a
// CONSISTENT snapshot: cend<=off<wend is a still-fillable in-flight hole (an
// out-of-order / preallocated writer hasn't filled it yet) → JUKEBOX-hold; off>=
// wend is a genuine past-end (the file may still grow) → EOF. Taking both fields
// under one lock avoids a transient cend>wend mis-read that two separate getters
// could expose to the read classifier (task #65). No I/O under the lock.
func (e *SpoolEntry) ReadableBounds() (cend, wend int64) {
	e.mu.RLock()
	cend = e.contiguousEnd
	wend = e.writtenEnd
	e.mu.RUnlock()
	return
}

// ── STREAMING DRAIN: the sealed prefix (S1) ─────────────────────────────────
//
// WHY THIS EXISTS. A single file copied into JuiceMount is hard-capped at local
// free disk, because peak spool usage is 100% of the file: the drainer only
// takes FINALIZED rows (drainer.go ListReady) and capacity is released only at
// MarkDrainComplete. spool_headroom.go states the consequence outright — "a
// camera file is 20-100 GB and CANNOT free headroom for itself — a spool entry
// only drains at close — so a single such file could never be copied at all".
// For object-backed storage that is backwards.
//
// NEGATIVE RESULT worth keeping: releasing capacity as the DRAINER copies is a
// no-op. Peak usage is reached before the drain starts. The cap only moves if
// bytes are drained and freed WHILE the file is still being written, which is
// what the sealed prefix is for.
//
// sealedEnd is the boundary below which bytes may be copied to the backend and
// then punched out of the spool file (F_PUNCHHOLE returns real blocks on APFS —
// measured, 256 MiB punched = 256 MiB physical freed, logical size unchanged).
//
// CORRECTNESS DOES NOT DEPEND ON THIS PREDICTION. There is no way to know that a
// writer will not seek back and rewrite below the seal — NLEs rewrite headers
// and moov atoms at close. So the streaming path REDIRECTS a write landing below
// sealedEnd to the destination fd rather than the spool. That makes the margins
// below a PERFORMANCE heuristic (keep the common rewrite off the slow FUSE path),
// not a safety boundary. Read that again before "optimising" either constant to
// zero: doing so is legal, merely slower.
const ()

// spoolSealMargin holds back bytes just under contiguousEnd. A writer that seeks
// back a little (chunk retry, small fixup) then stays on the fast spool path
// instead of being redirected through FUSE.
//
// TOGETHER WITH spoolSealHeadReserve THIS SETS THE REAL FLOOR for streaming: a
// file must exceed head-reserve + margin before a single byte is sealable, which
// is a much higher bar than spoolStreamMinSize alone. The gate test caught that
// the hard way — lowering only the threshold left the margins larger than the
// whole fixture, nothing ever sealed, and peak spool occupancy was 100% of the
// file while every unit test still passed.
//
// Var (not const) so tests can scale it alongside the threshold.
var spoolSealMargin int64 = 64 << 20 // 64 MiB

// spoolSealHeadReserve keeps the head of the file unsealed until the entry
// closes. Header/index rewrite-at-close is overwhelmingly at offset 0 (Premiere,
// Resolve, QuickTime moov), so reserving it converts the single most likely
// rewrite from a FUSE redirect into a plain spool write.
var spoolSealHeadReserve int64 = 64 << 20 // 64 MiB

// spoolStreamMinSize is the size below which streaming is not worth its
// complexity: such a file cannot exhaust the disk on its own, and the whole-file
// drain already verifies it with an at-rest SHA re-read.
//
// Var (not const) so a test can lower it and exercise real multi-chunk streaming
// without a multi-gigabyte fixture — the same idiom as cacheMutationChunk in
// metadata/store.go. Production never changes it.
var spoolStreamMinSize int64 = 1 << 30 // 1 GiB

// sealedEndLocked computes the sealed prefix. Caller holds e.mu (R or W).
//
// Returns 0 when nothing is sealable, which is the correct answer for every
// small or early file — callers treat 0 as "nothing to stream yet", never as an
// error.
func (e *SpoolEntry) sealedEndLocked() int64 {
	// Only stream files large enough to be the problem.
	if e.writtenEnd < spoolStreamMinSize {
		return 0
	}
	// Never seal past real data. contiguousEnd already guarantees [0,cend) holds
	// written bytes and never spans a hole; anchoring here inherits that
	// invariant rather than re-deriving it.
	sealed := e.contiguousEnd - spoolSealMargin
	if sealed > e.writtenEnd-spoolSealMargin {
		sealed = e.writtenEnd - spoolSealMargin
	}
	// Hold back the head until close.
	if sealed <= spoolSealHeadReserve {
		return 0
	}
	if sealed < 0 {
		return 0
	}
	return sealed
}

// SealedEnd returns the end of the prefix that may be drained and punched while
// the writer is still active. See sealedEndLocked.
//
// NOT monotonic by construction — contiguousEnd is monotonic, so this only
// retreats if writtenEnd does, which Truncate can cause. Callers must therefore
// track what they have ALREADY punched separately and never re-punch or
// un-punch from this value alone.
func (e *SpoolEntry) SealedEnd() int64 {
	e.mu.RLock()
	n := e.sealedEndLocked()
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
// spoolExtent is a half-open written byte range [start,end) recorded above
// contiguousEnd while an out-of-order writer leaves a gap below it.
type spoolExtent struct{ start, end int64 }

// advanceContiguousLocked records the just-written range [off,end) and advances
// e.contiguousEnd past every already-written region it makes contiguous. MUST
// be called with e.mu held, AFTER the bytes are durably pwritten. It preserves
// the never-serve-a-hole invariant: contiguousEnd only ever moves across ranges
// that were actually written (this range, or a previously-recorded extent),
// never across a still-unwritten gap.
func (e *SpoolEntry) advanceContiguousLocked(off, end int64) {
	// Delegates to the shared implementation so the spool path and the in-place
	// FUSE path can never disagree about what is readable. See advanceContig in
	// inplace_contig.go — the last time this logic was wrong (contiguousEnd not
	// coalescing out-of-order writes) it produced the #107 end-of-export
	// interrupt, and a second copy is how that comes back.
	e.contiguousEnd, e.writtenExtents = advanceContig(e.contiguousEnd, e.writtenExtents, off, end)
}

// insertExtent inserts [s,e) into a start-sorted, coalesced extent slice,
// merging any overlapping OR directly-adjacent ranges. The slice is tiny in
// practice (the client's reorder window is shallow), so a sort+sweep per insert
// is cheaper than the bookkeeping to avoid it.
func insertExtent(exts []spoolExtent, s, e int64) []spoolExtent {
	exts = append(exts, spoolExtent{s, e})
	sort.Slice(exts, func(i, j int) bool { return exts[i].start < exts[j].start })
	out := exts[:0]
	for _, x := range exts {
		if len(out) > 0 && x.start <= out[len(out)-1].end {
			if x.end > out[len(out)-1].end {
				out[len(out)-1].end = x.end
			}
			continue
		}
		out = append(out, x)
	}
	return out
}

// clipExtents drops (or trims) any recorded extent at/above size — used when a
// SETATTR/Truncate shrink discards bytes the extents referenced, so a later
// gap-fill can't advance contiguousEnd past bytes that no longer exist.
func clipExtents(exts []spoolExtent, size int64) []spoolExtent {
	out := exts[:0]
	for _, x := range exts {
		if x.start >= size {
			continue
		}
		if x.end > size {
			x.end = size
		}
		out = append(out, x)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// --- #104 zero-tail detection -------------------------------------------
//
// An interrupted preallocate-then-write download (Chrome→mount, task #104)
// leaves a spool entry whose declared size was pre-set (SETATTR/ftruncate
// grow) or whose out-of-order writes stopped before every gap filled. At
// finalize the entry looks CLOSED and CLEAN — the #65 rule then (correctly)
// treats 0..writtenEnd as the complete file and the drain ships a FULL-SIZE
// file whose unwritten ranges read back as zeros. Premiere black-frames it,
// silently. Detection-only: we count, WARN, and mark the row so /spool and
// the UI can surface it — the drain itself is NEVER blocked, held, or altered
// (a legitimately-sparse file is byte-indistinguishable from an interrupted
// download, so a heuristic must never hold user bytes hostage).

// zeroTailDetectEnabled gates the finalize-time zero-tail detector. Default
// ON — it is detection + marking only. JM_ZERO_TAIL_DETECT=0 kills it
// entirely (counter, WARN, row marker). Read per finalize — a cold path, one
// call per file — so tests can toggle via t.Setenv and operators can flip it
// live, following the drainClassGateEnabled pattern.
func zeroTailDetectEnabled() bool { return os.Getenv("JM_ZERO_TAIL_DETECT") != "0" }

// isAppleDoubleNFSPath reports whether the in-mount path names a ._
// AppleDouble sidecar. Mirrors the read path's isAppleDouble classification
// (handler.go). ._ files are EXCLUDED from zero-tail detection: copyfile
// assembles them with seeks and legitimately leaves sparse padding below
// writtenEnd (the #100 finding), so flagging them would be systematic
// false-positive WARN spam — and they are metadata sidecars, never media an
// NLE decodes frames from.
func isAppleDoubleNFSPath(p string) bool { return strings.HasPrefix(path.Base(p), "._") }

// zeroTailHoleBytes computes how many bytes in [contiguousEnd, writtenEnd)
// were NEVER written: the span above the contiguous prefix minus the
// out-of-order extents that actually landed there. Extents are maintained
// strictly above contiguousEnd and clipped to writtenEnd by the WriteAt/
// Truncate invariants, but the math clamps defensively anyway. A result > 0
// at finalize time means the finalized file contains that many bytes of
// never-written zeros below its size — the zero-tail signature.
func zeroTailHoleBytes(writtenEnd, contiguousEnd int64, exts []spoolExtent) int64 {
	if writtenEnd <= contiguousEnd {
		return 0
	}
	holes := writtenEnd - contiguousEnd
	for _, x := range exts {
		lo, hi := x.start, x.end
		if lo < contiguousEnd {
			lo = contiguousEnd
		}
		if hi > writtenEnd {
			hi = writtenEnd
		}
		if hi > lo {
			holes -= hi - lo
		}
	}
	if holes < 0 {
		return 0 // defensive: overlapping extents can't happen (insertExtent coalesces)
	}
	return holes
}

// zeroTailDetail is the compact JSON blob persisted in the spool row's
// suspect_zero_tail column (metadata.SpoolRow.SuspectZeroTail) and echoed
// per-entry by /spool. Field names are the #104 spec's sidecar shape.
type zeroTailDetail struct {
	DetectedAt string `json:"detected_at"` // RFC3339Nano UTC (manifest convention)
	Size       int64  `json:"size"`        // finalized size (writtenEnd)
	Contiguous int64  `json:"contiguous"`  // contiguous prefix end at finalize
	Holes      int64  `json:"holes"`       // never-written bytes below size
	// Extents is the count of out-of-order written regions still parked above
	// the contiguous prefix at finalize — 0 distinguishes a pure truncated
	// tail (preallocate + sequential writes stopped) from a mid-file gap
	// pattern (out-of-order writer interrupted).
	Extents int `json:"extents,omitempty"`
}

// logInflightJukebox emits a throttled diagnostic for an in-flight-hole JUKEBOX
// hold on a REAL (non-._) file — the end-of-export "connection interrupted"
// smoking gun. Throttled to ~1 line / 2s per entry (atomic CAS, no lock) so a
// genuine hold with a retrying client can't flood the log (the JM_LOOKUP_TRACE
// lesson). Called from the read path (spoolReadFile.ReadAt) without e.mu.
func (e *SpoolEntry) logInflightJukebox(name string, off, cend, wend int64) {
	now := time.Now().UnixNano()
	last := e.lastJukeboxLogNs.Load()
	if now-last < int64(2*time.Second) {
		return
	}
	if !e.lastJukeboxLogNs.CompareAndSwap(last, now) {
		return // another reader just logged for this entry
	}
	jmlog.Warn("spool: in-flight read JUKEBOX-held (offset past contiguous prefix)",
		"path", name, "off", off, "contiguous_end", cend, "written_end", wend,
		"gap", wend-cend,
		"since_last_write_ms", time.Since(time.Unix(0, e.lastWrite.Load())).Milliseconds())
}

func (e *SpoolEntry) WriteAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	e.mu.Lock()
	if e.closed || e.file == nil {
		e.mu.Unlock()
		return 0, fmt.Errorf("spool: write to closed entry")
	}
	// A WRITE BELOW punchedEnd MUST NOT GO TO THE SPOOL FILE.
	//
	// Those bytes were drained to the backend and punched away, and reads of that
	// range are routed to the DESTINATION. Writing them here would put the new
	// data in a hole in the spool file that nothing ever reads, while every
	// reader keeps getting the OLD bytes from the destination — a silently lost
	// write, and a final file with stale content in that range. It would also
	// re-allocate blocks we just reclaimed.
	//
	// This was named in the streaming design from the start ("writes landing
	// below sealedEnd are redirected to the destination") and was missing from
	// the implementation until 2026-08-06. spoolSealHeadReserve keeps the first
	// 64 MiB unpunched, which covers the COMMON rewrite (an NLE rewriting a
	// header/moov at offset 0) — but correctness must not depend on predicting
	// where a writer seeks, so the general case is handled here.
	if e.punchedEnd > 0 && off < e.punchedEnd {
		n, err := e.writeBelowPunchedLocked(p, off)
		e.mu.Unlock()
		return n, err
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
			// touch refreshes lastWrite each poll so the sweeper's leaked-handle
			// escalation never force-finalizes this entry TRUNCATED while the
			// write is parked (offline buffer full can park for many minutes,
			// well past the 10-min escalation window). lastWrite is atomic, safe
			// to store with e.mu released.
			if !e.store.reserveCapacityOrWait(reserved, func() {
				e.lastWrite.Store(time.Now().UnixNano())
			}) {
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
		// Advance the contiguous-written prefix, COALESCING any already-written
		// region this write makes contiguous. The old code bumped contiguousEnd
		// only to this chunk's end, so an out-of-order writer (macOS dispatches
		// WRITE RPCs on parallel goroutines) that filled a gap left contiguousEnd
		// stuck below already-written higher bytes — and a read of those bytes
		// then JUKEBOX-held for up to the 90s stall window, exceeding the client's
		// ~40s soft-mount timeout ("connection interrupted" at end of export).
		// advanceContiguousLocked pulls in every recorded extent above the filled
		// gap; it is called only after a successful pwrite, so it never advances
		// across an unwritten hole (the never-serve-zeros invariant holds).
		e.advanceContiguousLocked(off, end)
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
	// Drop/trim any recorded out-of-order extents the resize discarded, so a
	// later gap-fill can't advance contiguousEnd past bytes that no longer
	// exist. A grow adds only an unwritten hole — no extent to record.
	if len(e.writtenExtents) > 0 {
		e.writtenExtents = clipExtents(e.writtenExtents, size)
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
// smallSpoolFinalizeBytes / smallSpoolFinalizeIdle (#105): a SMALL file is
// written in a single burst with no multi-second mid-write gaps, so it can
// finalize on a much shorter idle WITHOUT the mid-write-finalize→reopen
// corruption risk. That corruption needs a write gap LONGER than the window
// DURING active writing (the sweeper finalizes+drains the partial, then a later
// write reopens a fresh entry whose drain os.Create-truncates the dest → a
// zero-prefixed file). A file that writes in well under a second can't produce
// such a gap. This cuts a Premiere project-save's spool→drain latency from ~30s
// to ~5s. Large files (video exports/copies) keep the full window as the
// guardrail — a slow encode or slow source CAN stall > the window mid-write.
const smallSpoolFinalizeBytes = 16 * 1024 * 1024 // 16 MiB
const smallSpoolFinalizeIdle = 5 * time.Second

// MarkCommitted records that the client issued an NFS COMMIT for this entry —
// an explicit durability request that lets the sweeper finalize it on the short
// idle even when large (#105). Idempotent; safe under concurrent COMMITs.
func (e *SpoolEntry) MarkCommitted() {
	e.mu.Lock()
	e.committed = true
	e.mu.Unlock()
}

func (e *SpoolEntry) finalizeIfIdle(idle time.Duration) bool {
	e.mu.Lock()
	if e.closed || e.refcount != 0 {
		e.mu.Unlock()
		return false
	}
	// #105 fast finalize: a COMMITted entry (the client explicitly asked for
	// durability — a Premiere save/export close) OR a SMALL file (written in one
	// burst) finalizes on the SHORT idle. This cuts a large export's close from
	// the full window to ~the short idle after its close-COMMIT. Safe now that a
	// continuation write during the resulting drain defers to the in-place
	// fdPool path (OpenWrite sawClosed) instead of corrupting; large UNcommitted
	// files keep the full window as the fallback.
	effIdle := idle
	small := e.writtenEnd > 0 && e.writtenEnd <= smallSpoolFinalizeBytes
	if (e.committed || small) && smallSpoolFinalizeIdle < effIdle {
		effIdle = smallSpoolFinalizeIdle
	}
	if time.Since(time.Unix(0, e.lastWrite.Load())) < effIdle {
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
	// #104 zero-tail detection: capture the suspect signature BEFORE the #65
	// advance below — that advance folds contiguousEnd up to writtenEnd and
	// destroys the only evidence that holes existed. This is the single point
	// in the entry's life where (a) the writer is definitively done (every
	// finalize path — idle sweeper, explicit Finalize/Close, stuck-handle
	// escalation — funnels here) and (b) writtenEnd, contiguousEnd, and the
	// out-of-order extent set are all still intact under e.mu. Drain-claim
	// time is too late: the row carries only size+sha by then, and after a
	// restart the in-RAM extent state is gone entirely. Detection-only — the
	// finalize, MarkReady, and drain below proceed IDENTICALLY whether or not
	// this fires.
	var suspect *zeroTailDetail
	suspectPath := ""
	if zeroTailDetectEnabled() && !isAppleDoubleNFSPath(e.nfsPath) {
		if holes := zeroTailHoleBytes(e.writtenEnd, e.contiguousEnd, e.writtenExtents); holes > 0 {
			suspect = &zeroTailDetail{
				DetectedAt: time.Now().UTC().Format(time.RFC3339Nano),
				Size:       e.writtenEnd,
				Contiguous: e.contiguousEnd,
				Holes:      holes,
				Extents:    len(e.writtenExtents),
			}
			suspectPath = e.nfsPath
		}
	}
	// task #65: the writer has CLOSED — every byte it intended to write has landed
	// on the spool file, so 0..writtenEnd is the complete file (any genuine sparse
	// gap reads back as zeros, which IS the file's content). The in-flight
	// contiguousEnd tracker (WriteAt) only advances on in-order writes and LAGS far
	// behind writtenEnd under parallel / out-of-order NFS WRITEs — a 1GiB cp was
	// observed stuck at 6MiB. The Stat read-shadow reports contiguousEnd as the
	// file SIZE, so a lagged value made the NFS client cap reads and silently
	// TRUNCATE the tail of a file read while it drained (the core #65 bug: the
	// client never even issued a tail READ). A finalized file has no more writes
	// coming, so its readable prefix IS its full written extent — advance it.
	if e.contiguousEnd < e.writtenEnd {
		e.contiguousEnd = e.writtenEnd
	}
	finalSha := e.sha256 // nil if streaming hash was invalidated (out-of-order)
	finalSpoolFile := e.spoolFile
	finalID := e.id // capture under mu: rename migration can re-bind id
	e.mu.Unlock()

	// #104 surfacing, off the entry lock: one counter bump, ONE Warn (finalize
	// runs once per entry — the closed guard above makes this unrepeatable),
	// and the row marker. Runs BEFORE MarkReady so the row is still `writing`
	// and no drainer can be racing it; a marker failure is logged and ignored
	// — detection must never fail a finalize.
	if suspect != nil {
		metrics.Default().IncZeroTailSuspect()
		jmlog.Warn("spool: ZERO-TAIL SUSPECT at finalize — unwritten hole(s) will drain as zeros (interrupted download?)",
			"path", suspectPath,
			"size", suspect.Size,
			"written_end", suspect.Size,
			"contiguous_end", suspect.Contiguous,
			"hole_bytes", suspect.Holes,
			"oo_extents", suspect.Extents,
		)
		if detail, merr := json.Marshal(suspect); merr == nil {
			if serr := e.store.meta.MarkSuspectZeroTail(finalID, string(detail)); serr != nil {
				jmlog.Warn("spool: zero-tail suspect row-mark failed",
					"path", suspectPath, "id", finalID, "error", serr.Error())
			}
		}
	}

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
