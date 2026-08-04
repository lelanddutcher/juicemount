package health

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
)

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
