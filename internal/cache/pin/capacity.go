package pin

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// capacity.go — pinned-set vs disk-capacity verdict (R-1, release punch list).
//
// Pinning a directory LARGER than the disk can physically cache used to make
// the prefetcher re-warm forever: every re-read of one pinned file evicts a
// block another pinned file just paid to download (JuiceFS keeps the volume
// above a free-space floor, so it MUST evict). The pinned set can never reach
// "fully resident," so the UI showed perpetual download activity with no
// explanation, and a metered/WAN link burned bandwidth indefinitely.
//
// We cannot make a too-big pinned set fit. What we CAN do:
//   1. DETECT it — compare the pinned set against the disk space the cache can
//      sustainably use (FREE space + what the cache already holds, minus the
//      floor). The historical bug compared against TOTAL disk, which counts the
//      OS and the user's other files as if they were available to the cache.
//   2. SURFACE it — expose the verdict via /cache-status so the menu-bar app can
//      tell the user to free disk or unpin a folder.
//   3. STOP the futile churn — the re-warm loop consults IsOverCapacity() and
//      skips re-warming while over capacity (PullPending already warmed what
//      fits; re-warming only thrashes the LRU).

// CacheFreeFloorBytes mirrors health.cacheFreeFloorBytesConst: JuiceFS keeps at
// least this much free on the cache volume at runtime (via --free-space-ratio).
// The space actually usable BY the cache is therefore
// (current free) + (bytes the cache already occupies) − floor.
//
// The floor is DERIVED AT MOUNT TIME and published here, because it is no longer
// a constant: health.resolveFreeSpaceRatio sets --free-space-ratio so JuiceFS's
// eviction floor sits ABOVE the write spool's 20 GiB admission floor (~30 GiB on
// a typical disk). Otherwise the spool always blocked first and the cache never
// yielded a byte — which is the whole premise of counting reclaimable cache as
// spool headroom.
//
// Reading the stale 10 GiB here OVER-stated sustainable capacity by the
// difference, so the over-capacity banner UNDER-warned and IsOverCapacity() —
// which gates the prefetcher's re-warm — released the futile-churn guard late.
// Both errors were in the unsafe direction.
const CacheFreeFloorBytesDefault = int64(10) << 30 // 10 GiB, pre-derivation

// cacheFreeFloorBytes is the LIVE floor, published by health at mount time via
// SetCacheFreeFloorBytes. Zero means "not yet published" and reads fall back to
// the default, so a verdict computed before the mount is no worse than before.
var cacheFreeFloorBytes atomic.Int64

// SetCacheFreeFloorBytes publishes the free-space floor JuiceFS was actually
// mounted with. Mirrors SetCacheBudgetBytes.
func SetCacheFreeFloorBytes(b int64) {
	if b < 0 {
		b = 0
	}
	cacheFreeFloorBytes.Store(b)
}

// CacheFreeFloorBytes returns the live floor, or the default if none published.
func CacheFreeFloorBytes() int64 {
	if v := cacheFreeFloorBytes.Load(); v > 0 {
		return v
	}
	return CacheFreeFloorBytesDefault
}

// capacityFlapMarginBytes is hysteresis so the verdict doesn't toggle on/off as
// free space jitters by a few hundred MB during normal use. The pinned set must
// exceed sustainable capacity by more than this before we flag over-capacity.
const capacityFlapMarginBytes = int64(2) << 30 // 2 GiB

// CapacityVerdict is a snapshot of whether the pinned set fits the cache disk.
// Surfaced in /cache-status; consumed by the UI to render an over-capacity
// banner and by the prefetcher to gate re-warm.
type CapacityVerdict struct {
	// OverCapacity is true when the pinned set cannot be kept fully resident
	// on the cache disk (exceeds sustainable capacity by more than the flap
	// margin). The actionable signal for the UI.
	OverCapacity bool `json:"over_capacity"`
	// PinnedBytes is the total size of all pinned files (pin store TotalBytes).
	PinnedBytes int64 `json:"pinned_bytes"`
	// CacheCapacityBytes is the most the cache can sustainably hold:
	// disk_free + cache_usage − floor (never negative).
	CacheCapacityBytes int64 `json:"cache_capacity_bytes"`
	// ShortfallBytes is how much the pinned set exceeds sustainable capacity
	// (PinnedBytes − CacheCapacityBytes), clamped to ≥ 0. The number to show
	// the user: "free up N GB or unpin."
	ShortfallBytes int64 `json:"shortfall_bytes"`
	// DiskFreeBytes / CacheUsageBytes are the raw inputs, surfaced for the UI
	// and for debugging the verdict.
	DiskFreeBytes   int64 `json:"disk_free_bytes"`
	CacheUsageBytes int64 `json:"cache_usage_bytes"`
	// CacheFreeFloorBytes is the effective JuiceFS eviction floor selected at
	// mount time. It is surfaced so clients can report the real caching cutoff;
	// a hard-coded 1% UI became false once the floor was made disk- and
	// spool-aware.
	CacheFreeFloorBytes int64 `json:"cache_free_floor_bytes"`
	// BlockCacheBytes is JuiceFS's own `juicefs_blockcache_bytes` gauge — the
	// AUTHORITATIVE on-disk block-cache size — as of Computed. 0 when the gauge
	// is unavailable (no metrics addr, daemon not up, never scraped), in which
	// case CacheUsageBytes (the cache-dir du) is the only figure available.
	//
	// Carried here so a consumer that must not perform I/O can read the gauge
	// from this snapshot. The write spool's admission path is exactly that
	// consumer: nfs.refreshCeiling runs under a TryLock on the write hot path
	// and may not scrape, so it reads this field (see nfs/spool_headroom.go).
	// Before this, the ONLY thing that refreshed the gauge was the /cache-status
	// poll — i.e. it ticked only while the menu-bar popover was open.
	//
	// NOT SERIALIZED, deliberately: contract/spec/schema/cache-status.schema.json
	// declares the `capacity` object "additionalProperties": false, so adding a
	// key here would fail wire conformance. Consumers already receive the same
	// gauge as /cache-status's top-level `cache_used_bytes`.
	BlockCacheBytes int64 `json:"-"`
	// CacheBudgetBytes is the USER-CONFIGURED juicefs --cache-size (0 =
	// unknown/unlimited). When set, it caps sustainable capacity: juicefs
	// will never hold more than the budget no matter how roomy the disk is,
	// so a pinned set larger than the budget is perpetually evicted — the
	// exact futile-rewarm thrash R-1 exists to stop (audit 2026-07-14: the
	// verdict previously compared against DISK capacity only and approved
	// budget-exceeding pin sets).
	CacheBudgetBytes int64 `json:"cache_budget_bytes"`
	// Computed is when this verdict was last recomputed. Zero until the first
	// CapacityLoop pass runs.
	Computed time.Time `json:"-"`
	// ComputedSec is elapsed seconds since Computed, for clients that don't
	// want to do timestamp math. 0 when never computed.
	ComputedSec int64 `json:"computed_sec"`
}

var (
	// overCapacityFlag is the hot-path read for the prefetcher's re-warm
	// gate — cheap atomic, no lock. 1 == over capacity.
	overCapacityFlag atomic.Int32

	capacityMu      sync.RWMutex
	capacityVerdict CapacityVerdict
)

// setCapacityVerdict publishes a freshly computed verdict.
func setCapacityVerdict(v CapacityVerdict) {
	if v.OverCapacity {
		overCapacityFlag.Store(1)
	} else {
		overCapacityFlag.Store(0)
	}
	capacityMu.Lock()
	capacityVerdict = v
	capacityMu.Unlock()
}

// IsOverCapacity reports whether the pinned set exceeds the cache disk. Cheap;
// safe in hot paths (the prefetcher re-warm gate calls it).
func IsOverCapacity() bool { return overCapacityFlag.Load() != 0 }

// Capacity returns the latest verdict snapshot. ComputedSec is filled in at
// read time from Computed.
func Capacity() CapacityVerdict {
	capacityMu.RLock()
	v := capacityVerdict
	capacityMu.RUnlock()
	if !v.Computed.IsZero() {
		v.ComputedSec = int64(time.Since(v.Computed).Seconds())
	}
	return v
}

// ComputeCapacity recomputes the verdict from the pin store's total pinned
// bytes and the live disk/cache footprint, publishes it, and returns it.
//
// cacheBaseDir is the JuiceFS cache root (e.g. ~/.juicefs/cache); empty string
// falls back to the default. The walk of that tree is the only non-trivial
// cost — bounded by sequential SSD stat throughput and run on a slow cadence
// (CapacityLoop), so it never competes with the read hot path.
// cacheBudgetBytes is the user-configured juicefs --cache-size ceiling,
// set once at mount time via SetCacheBudgetBytes. 0 = unknown.
var cacheBudgetBytes atomic.Int64

// SetCacheBudgetBytes records the configured cache budget so capacity
// verdicts cap sustainable capacity at it (see evaluateCapacity).
func SetCacheBudgetBytes(b int64) {
	if b < 0 {
		b = 0
	}
	cacheBudgetBytes.Store(b)
}

func ComputeCapacity(store *Store, cacheBaseDir string) CapacityVerdict {
	if cacheBaseDir == "" {
		cacheBaseDir = defaultCacheBaseDir()
	}
	v := CapacityVerdict{Computed: time.Now(), CacheBudgetBytes: cacheBudgetBytes.Load()}

	if store != nil {
		if agg, err := store.AggregateStats(); err == nil {
			v.PinnedBytes = agg.TotalBytes
		}
	}
	v.DiskFreeBytes = volumeFreeBytesAt(cacheBaseDir)
	v.CacheUsageBytes = dirUsageBytes(cacheBaseDir)
	// Refresh AND record the authoritative block-cache gauge on this same slow
	// cadence. The scrape is a bounded (3s-timeout) HTTP GET, internally
	// throttled to once per 2s; running it HERE — beside a full directory walk,
	// on the 60s CapacityLoop, explicitly off the read hot path — is what lets
	// the spool's admission path consume the gauge as a plain snapshot read with
	// no I/O of its own. A miss leaves the field 0 and the consumer falls back.
	if bc, ok := BlockCacheBytes(); ok {
		v.BlockCacheBytes = bc
	}

	evaluateCapacity(&v)
	setCapacityVerdict(v)
	return v
}

// evaluateCapacity fills the derived fields (CacheCapacityBytes, ShortfallBytes,
// OverCapacity) from the raw inputs (PinnedBytes, DiskFreeBytes, CacheUsageBytes)
// already set on v. Pure function — no disk or store access — so the verdict
// logic is unit-testable independent of the live machine.
func evaluateCapacity(v *CapacityVerdict) {
	// The cache can sustainably hold what's free now PLUS what it already
	// occupies (JuiceFS reuses its own blocks via LRU), minus the floor it must
	// keep free on the volume.
	floor := CacheFreeFloorBytes()
	v.CacheFreeFloorBytes = floor
	capacity := v.DiskFreeBytes + v.CacheUsageBytes - floor
	if capacity < 0 {
		capacity = 0
	}
	// The user's --cache-size budget is a hard ceiling regardless of disk
	// headroom: juicefs evicts past it, so pins beyond it can never stay
	// resident.
	if v.CacheBudgetBytes > 0 && v.CacheBudgetBytes < capacity {
		capacity = v.CacheBudgetBytes
	}
	v.CacheCapacityBytes = capacity

	v.ShortfallBytes = 0
	if shortfall := v.PinnedBytes - capacity; shortfall > 0 {
		v.ShortfallBytes = shortfall
	}
	// Only a pinned set that's actually present AND over by more than the flap
	// margin counts as over-capacity.
	v.OverCapacity = v.PinnedBytes > 0 && v.ShortfallBytes > capacityFlapMarginBytes
}

// defaultCacheBaseDir returns ~/.juicefs/cache — the JuiceFS cache root we mount
// against (the mount command in health/fuse.go does not pass --cache-dir, so
// JuiceFS uses this default).
func defaultCacheBaseDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".juicefs", "cache")
}

// volumeFreeBytesAt statfs-es the nearest existing ancestor of path and returns
// the free bytes on that volume. Returns 0 if nothing along the path resolves.
func volumeFreeBytesAt(path string) int64 {
	p := path
	for p != "" {
		var st syscall.Statfs_t
		if err := syscall.Statfs(p, &st); err == nil {
			return int64(st.Bsize) * int64(st.Bavail)
		}
		parent := filepath.Dir(p)
		if parent == p {
			break
		}
		p = parent
	}
	// Last resort: the boot volume.
	var st syscall.Statfs_t
	if err := syscall.Statfs("/", &st); err == nil {
		return int64(st.Bsize) * int64(st.Bavail)
	}
	return 0
}

// dirUsageBytes sums the logical sizes of every regular file under dir. Best
// effort: walk errors (a chunk file evicted mid-walk, a permission blip) are
// skipped, not fatal. Returns 0 if dir doesn't exist.
func dirUsageBytes(dir string) int64 {
	if dir == "" {
		return 0
	}
	var total int64
	_ = filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // keep going past transient errors
		}
		if d.IsDir() {
			return nil
		}
		if info, ierr := d.Info(); ierr == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}
