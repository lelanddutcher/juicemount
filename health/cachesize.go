package health

import "fmt"

// Cache-size resolution, extracted from FUSEManager.Mount so the policy can be
// tested without a live juicefs binary, a real mount, or a particular disk.
// The test calls THIS function — it does not restate the arithmetic, which is
// how a test ends up agreeing with a bug.

const (
	// cacheFreeFloorBytesConst is the hard free-space floor on the cache volume.
	// A near-full boot disk destabilises the whole system, so the cache is never
	// sized above (total - this).
	cacheFreeFloorBytesConst = int64(10) << 30 // 10 GiB

	// juicefsDefaultCacheMiB is what JuiceFS itself uses when --cache-size is
	// omitted. Mirrored here so that default becomes an ordinary INPUT to the
	// clamp rather than an invisible fallback that bypasses it entirely.
	juicefsDefaultCacheMiB = int64(102400) // 100 GiB

	// spoolFreeFloorBytesConst MIRRORS nfs.SpoolFreeFloorBytes — the free space
	// the write spool refuses to eat into. Mirrored rather than imported so the
	// health package does not take a build dependency on the whole NFS server
	// (the same reason pin.CacheFreeFloorBytes mirrors the constant above).
	// TestSpoolFloorMirrorHasNotDrifted asserts the two stay equal.
	spoolFreeFloorBytesConst = int64(20) << 30 // 20 GiB

	// cacheYieldHeadroomBytes is the slack JuiceFS keeps free ABOVE the spool's
	// own floor, so eviction has already started by the time the spool is near
	// its limit rather than only after the spool has stopped admitting.
	// Sized to absorb a second or two of ingest at 10GbE plus the spool's 1s
	// disk-sample TTL, i.e. the window in which the spool can commit bytes
	// against a reading JuiceFS has not yet reacted to.
	cacheYieldHeadroomBytes = int64(10) << 30 // 10 GiB

	// minFreeSpaceRatio / maxFreeSpaceRatio bound the derived ratio. The lower
	// bound preserves the historical ~1% behaviour on very large disks; the
	// upper bound stops a small disk from reserving an absurd fraction of itself
	// for free space. See resolveFreeSpaceRatio for what the upper clamp costs.
	minFreeSpaceRatio = 0.01
	maxFreeSpaceRatio = 0.25
)

// resolveFreeSpaceRatio derives the --free-space-ratio passed to `juicefs
// mount`: the fraction of the cache volume JuiceFS keeps free by evicting cache.
//
// THE BUG THIS EXISTS TO FIX (B-2, 2026-08-04). Two components arbitrate one
// SSD with two floors, ordered the wrong way round:
//
//   - JuiceFS evicted only once free fell below 10 GiB (the ratio was
//     cacheFreeFloorBytesConst/total, i.e. ~0.01 on a 1 TB disk).
//   - The write spool stops admitting at 20 GiB free (nfs.SpoolFreeFloorBytes).
//
// The spool therefore ALWAYS hit its floor first and JuiceFS never yielded a
// byte. The cache could sit on ~100 GB while copies stalled — and the spool's
// new reclaimable-cache headroom (nfs/spool_headroom.go) counts on that cache
// actually coming back, so without this the other half of the fix is a promise
// nothing keeps.
//
// The fix is ordering, not new machinery: put JuiceFS's eviction floor ABOVE the
// spool's admission floor by the yield headroom and let JuiceFS's own LRU do the
// work.
//
// KNOWN LIMIT: on a disk small enough for maxFreeSpaceRatio to bite (below ~80
// GiB total) the resulting floor lands under the spool's 20 GiB and the ordering
// is not achieved. That disk cannot host both a 20 GiB spool floor and a cache
// anyway; the clamp keeps the failure sane rather than reserving a third of the
// volume. Returns 0 for an unknown disk (caller leaves the config untouched).
func resolveFreeSpaceRatio(totalBytes int64) float64 {
	if totalBytes <= 0 {
		return 0
	}
	ratio := float64(spoolFreeFloorBytesConst+cacheYieldHeadroomBytes) / float64(totalBytes)
	if ratio < minFreeSpaceRatio {
		ratio = minFreeSpaceRatio
	}
	if ratio > maxFreeSpaceRatio {
		ratio = maxFreeSpaceRatio
	}
	// The pre-existing hard guarantee, which must survive intact: never leave
	// the boot disk with less than cacheFreeFloorBytesConst free, because a
	// near-full boot disk destabilises the whole system. On a disk small enough
	// for the upper clamp to bite, the clamped ratio can fall BELOW that floor
	// (0.25 x 30 GiB = 7.5 GiB) — which would WEAKEN a protection that already
	// shipped. The floor wins, even past the clamp.
	if floorRatio := float64(cacheFreeFloorBytesConst) / float64(totalBytes); ratio < floorRatio {
		ratio = floorRatio
	}
	return ratio
}

// resolveFreeSpaceRatioArg decides the --free-space-ratio STRING to hand juicefs
// for a given configured value and disk size. Split out of Mount for the same
// reason resolveCacheSizeMiB was: so the decision — including the "never weaken
// a stricter configured ratio" rule and the wire formatting — is testable
// without a live juicefs binary or a particular disk.
//
// Returns (value, true) when the derived floor is stricter than what is
// configured and the config should be replaced, or ("", false) to leave the
// configured value untouched.
func resolveFreeSpaceRatioArg(configured string, totalBytes int64) (string, bool) {
	derived := resolveFreeSpaceRatio(totalBytes)
	if derived <= 0 {
		return "", false // unknown disk: leave the config alone
	}
	var cur float64
	fmt.Sscanf(configured, "%f", &cur)
	if derived <= cur {
		return "", false // an already-stricter configured ratio is never weakened
	}
	return fmt.Sprintf("%.4f", derived), true
}

// resolveCacheSizeMiB decides the --cache-size (in MiB) to pass to juicefs.
//
// THE BUG THIS EXISTS TO FIX (2026-08-04): the boot disk fell to 14 GiB while
// JuiceFS held ~100 GB of cache, with every guard here intact — because they had
// nothing to act on. `cache_size` is not a key the founder sets, so configuredMiB
// was 0; with no pins the desired size was max(0,0) == 0; the caller's "only
// write a positive, changed value" test then declined to set anything; and the
// mount was built with NO --cache-size flag. JuiceFS applied its own ~100 GiB
// default, a number this policy never saw and therefore never clamped.
//
// A protection that silently does not run is worse than none, because the log
// line it would have emitted is the evidence that it works.
//
// Returns the resolved size and the seeded "configured" byte count used for
// logging (which differs from the caller's input precisely when the JuiceFS
// default was substituted).
func resolveCacheSizeMiB(configuredMiB, pinnedBytes, totalBytes int64) (effectiveMiB, seededConfiguredBytes int64) {
	if totalBytes <= 0 {
		return 0, 0 // unknown disk: caller leaves the config untouched
	}
	configuredBytes := configuredMiB << 20

	// (0) Nothing configured => adopt JuiceFS's own default EXPLICITLY, so the
	// clamp below applies to it and the effective value is visible in logs and
	// /cache-status instead of being decided silently inside juicefs.
	if configuredBytes <= 0 {
		configuredBytes = juicefsDefaultCacheMiB << 20
	}

	// (1)+(2) Respect the configured size; grow it only as far as needed to keep
	// the pinned set resident, so a large pinned project cannot LRU-evict blocks
	// the user already paid to download.
	desired := configuredBytes
	if pinnedBytes > desired {
		desired = pinnedBytes
	}

	// (3) Never size the cache above (total - floor).
	//
	// The `maxForFloor > 0` shape this replaces SKIPPED the clamp entirely on a
	// disk smaller than the floor, which was harmless only while an unconfigured
	// cache resolved to 0 and emitted no flag at all. Once (0) above seeds the
	// JuiceFS default, skipping the clamp means emitting a 100 GiB --cache-size
	// on a 4 GiB disk — strictly worse than the behaviour it replaced. A disk
	// that cannot satisfy the floor gets no cache size from us at all; the caller
	// leaves the config untouched and --free-space-ratio stays the live guard.
	// (Caught by this function's own table test, not by review.)
	maxForFloor := totalBytes - cacheFreeFloorBytesConst
	if maxForFloor <= 0 {
		return 0, configuredBytes
	}
	if desired > maxForFloor {
		desired = maxForFloor
	}
	if desired <= 0 {
		return 0, configuredBytes
	}
	return desired >> 20, configuredBytes
}
