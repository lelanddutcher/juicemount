package nfs

import (
	"os"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// spool_headroom.go — how much of the local SSD the write spool may consume.
//
// THE BUG (B-1, 2026-08-04). The write spool and the JuiceFS read cache
// arbitrate the SAME local SSD and neither knew the other existed.
// refreshCeiling counted only FREE bytes:
//
//	headroom = avail - SpoolFreeFloorBytes
//
// Measured live: 25.8 GiB free with ~100 GB of JuiceFS block cache on the same
// disk gave the spool 5.79 GiB of headroom. A documented earlier incident (14
// GiB free, same 100 GB cache) gave it the 1-byte sentinel. A camera file is
// 20-100 GB and CANNOT free headroom for itself — a spool entry only drains at
// close — so a single such file could never be copied at all, no matter how long
// the client waited. The bytes it needed were sitting right there in the cache.
//
// Those ~100 GB are pure RECLAIMABLE copies of objects already durable in MinIO.
// Counting them as headroom is legitimate, under three gates:
//
//  1. WRITEBACK MUST BE OFF. With `juicefs --writeback` a cache block can be the
//     ONLY durable copy of a just-written file: close/fsync returns while the
//     upload is still pending. That is a REAL past incident, not a hypothetical
//     — a kill-9 test returned 5 of 50 already-drained Canon CR3s corrupt in
//     MinIO, which is why health/fuse.go disables --writeback. Treating cache
//     blocks as spare in that mode would invite the spool to evict the sole copy
//     of the user's footage. When JM_FUSE_WRITEBACK=1 the entire reclaimable
//     term is dropped and the old avail-minus-floor formula stands EXACTLY.
//
//  2. NEVER COUNT THE PINNED SET. Pinned content is an offline-availability
//     promise. JuiceFS's LRU has no pin awareness, so the only lever we have is
//     to not offer pinned bytes as reclaimable in the first place: they are
//     subtracted before anything is handed to the spool.
//
//  3. NEVER DO I/O HERE. refreshCeiling runs on the write admission path under a
//     non-blocking TryLock, with callers holding a path shard (OpenWrite) or the
//     per-entry e.mu (WriteAt) — see the HIGH defect note in cachedCeiling. So
//     it may not scrape, walk, or take a new lock. Every input below is a
//     snapshot read of the verdict pin.CapacityLoop already recomputes every 60s
//     (pin.Capacity() is one RWMutex-guarded struct copy). Anything stale,
//     missing or unreadable degrades to the OLD, SMALLER number — never to a
//     larger one.
//
// B-2 in health/cachesize.go is the other half: it derives --free-space-ratio so
// JuiceFS's eviction floor sits ABOVE the spool's floor. Without it JuiceFS
// never actually yields the space this file counts on.

const (
	// spoolCacheFlapMarginBytes is hysteresis on the reclaimable-cache term,
	// following the capacityFlapMarginBytes precedent in
	// internal/cache/pin/capacity.go (same 2 GiB, same purpose).
	//
	// Two reasons it is a DEDUCTION rather than a threshold:
	//
	//   - Flap. Handing space to the spool evicts cache, which the prefetcher
	//     may re-warm, which re-pressures the spool. A margin means the last
	//     couple of GiB of cache are never fought over.
	//   - Staleness. The verdict is up to spoolCacheVerdictMaxAge old. When
	//     JuiceFS evicts, the freed bytes appear in `avail` IMMEDIATELY while our
	//     cached gauge still counts them — a transient DOUBLE-COUNT. The margin
	//     absorbs the small end of that; the age gate below bounds the rest.
	spoolCacheFlapMarginBytes = int64(2) << 30 // 2 GiB

	// spoolCacheVerdictMaxAge is how old pin.CapacityVerdict may be before we
	// stop believing anything it says about the cache. pin.CapacityLoop
	// recomputes every 60s, so this is five missed ticks — generous enough that
	// ordinary scheduling never trips it, tight enough that a dead CapacityLoop
	// cannot leave admission running on a figure from an hour ago. Past it the
	// reclaimable term is 0, i.e. today's behaviour.
	spoolCacheVerdictMaxAge = 5 * time.Minute
)

// cacheReclaimInputs is a snapshot of everything the reclaimable-cache term
// depends on. Grouped into one struct so the policy below can be a PURE
// function — the health/cachesize.go resolveCacheSizeMiB pattern — and its tests
// can call the real thing instead of restating its arithmetic.
type cacheReclaimInputs struct {
	// WritebackOn mirrors JM_FUSE_WRITEBACK=1, the env var health/fuse.go reads
	// to decide whether to pass `--writeback` to `juicefs mount`. True means a
	// cache block may be the only durable copy of a file — see gate 1 above.
	WritebackOn bool

	// HaveVerdict is false until pin.CapacityLoop has published its first
	// verdict. VerdictAge is how long ago that happened. Without a verdict we do
	// not know the pinned set, and we must not treat ANY cache as spare while
	// blind to the offline-availability promise.
	HaveVerdict bool
	VerdictAge  time.Duration

	// BlockCacheBytes is juicefs_blockcache_bytes (authoritative);
	// CacheDirBytes is the cache-dir du (fallback). 0 == unavailable, for either.
	BlockCacheBytes int64
	CacheDirBytes   int64

	// PinnedBytes is the pin store's total pinned size — the part of the cache
	// that is NOT ours to spend.
	PinnedBytes int64
}

// reclaimableCacheBytes returns the bytes of JuiceFS block cache the spool may
// treat as available to it. Pure: no I/O, no locks, no clock.
func reclaimableCacheBytes(in cacheReclaimInputs) int64 {
	// GATE 1 — writeback. Cache blocks may be the only durable copy. Nothing is
	// reclaimable; fall back to free disk alone.
	if in.WritebackOn {
		return 0
	}
	// GATE 2 — we must know the pinned set before spending any of the cache. No
	// verdict, or one too old to trust, means we do not.
	if !in.HaveVerdict || in.VerdictAge < 0 || in.VerdictAge > spoolCacheVerdictMaxAge {
		return 0
	}

	// Prefer the authoritative gauge; fall back to the du when it is missing.
	// When BOTH are present take the SMALLER: the du over-counts (it sums
	// staging/temp chunk files that are not addressable cache) and the gauge can
	// linger stale-high (bridge/cbridge.go clamps it down for the same reason,
	// after "Clear Cache" removes chunk files behind the daemon's back).
	// Under-claiming is the only safe direction here.
	cache := in.BlockCacheBytes
	if cache <= 0 {
		cache = in.CacheDirBytes
	} else if in.CacheDirBytes > 0 && in.CacheDirBytes < cache {
		cache = in.CacheDirBytes
	}
	if cache <= 0 {
		return 0
	}

	// GATE 3 — never below the pinned set, then the flap margin on top.
	pinned := in.PinnedBytes
	if pinned < 0 {
		pinned = 0
	}
	reclaimable := cache - pinned - spoolCacheFlapMarginBytes
	if reclaimable < 0 {
		return 0
	}
	return reclaimable
}

// spoolHeadroomBytes is the number of bytes the spool may still consume on this
// volume: free disk PLUS the reclaimable JuiceFS cache, minus the floor the
// spool leaves for the OS. Never negative.
//
// With every gate closed this is exactly the historical `avail -
// SpoolFreeFloorBytes`, which is the required degradation: an unknown cache must
// make the spool smaller, never bigger.
func spoolHeadroomBytes(avail int64, in cacheReclaimInputs) int64 {
	headroom := avail + reclaimableCacheBytes(in) - SpoolFreeFloorBytes
	if headroom < 0 {
		headroom = 0
	}
	return headroom
}

// cacheReclaimHook lets a test substitute the snapshot so refreshCeiling — the
// REAL function, wiring included — can be exercised without a live JuiceFS
// daemon or a particular machine's cache. atomic.Pointer rather than a plain var
// so a test swapping it can never race a background sweeper still calling
// refreshCeiling (the package is tested with -race). nil == production.
var cacheReclaimHook atomic.Pointer[func() cacheReclaimInputs]

// setCacheReclaimHookForTest installs fn (nil restores production behaviour).
func setCacheReclaimHookForTest(fn func() cacheReclaimInputs) {
	if fn == nil {
		cacheReclaimHook.Store(nil)
		return
	}
	cacheReclaimHook.Store(&fn)
}

// cacheReclaimSnapshot reads the current inputs. One atomic load on the
// production path, then liveCacheReclaimInputs.
func cacheReclaimSnapshot() cacheReclaimInputs {
	if fn := cacheReclaimHook.Load(); fn != nil {
		return (*fn)()
	}
	return liveCacheReclaimInputs()
}

// liveCacheReclaimInputs snapshots the real inputs. NON-BLOCKING by
// construction: one env lookup plus one RWMutex-guarded struct copy from
// pin.Capacity(). No scrape, no walk, no statfs, no new lock — see gate 3.
//
// LIMITATION, deliberate: JM_FUSE_WRITEBACK is read from THIS process's
// environment, the same source health/fuse.go uses to build the mount command,
// so the two always agree for a daemon we launched. A juicefs daemon started
// out-of-band with --writeback by something else would not be detected.
func liveCacheReclaimInputs() cacheReclaimInputs {
	v := pin.Capacity()
	in := cacheReclaimInputs{
		WritebackOn:     os.Getenv("JM_FUSE_WRITEBACK") == "1",
		BlockCacheBytes: v.BlockCacheBytes,
		CacheDirBytes:   v.CacheUsageBytes,
		PinnedBytes:     v.PinnedBytes,
	}
	if !v.Computed.IsZero() {
		in.HaveVerdict = true
		in.VerdictAge = time.Since(v.Computed)
	}
	return in
}
