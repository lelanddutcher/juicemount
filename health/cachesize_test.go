package health

import "testing"

const gib = int64(1) << 30

// TestResolveCacheSizeRunsWhenUnconfigured is the regression for the 2026-08-04
// incident: the boot disk fell to 14 GiB while JuiceFS held ~100 GB of cache.
// Nothing was broken in the clamp — it simply never ran, because `cache_size` is
// not a key the founder sets, so there was no number to clamp and no flag was
// emitted at all.
func TestResolveCacheSizeRunsWhenUnconfigured(t *testing.T) {
	// A 500 GiB boot disk, nothing configured, nothing pinned — the exact shape
	// of the incident.
	const total = 500 * gib
	got, _ := resolveCacheSizeMiB(0, 0, total)
	if got == 0 {
		t.Fatal("no cache-size resolved with nothing configured — this is the incident: " +
			"juicefs then applies its own ~100 GiB default, unclamped by any policy here")
	}
	// It must be a real clamp against the disk, not merely a number.
	if maxAllowed := (total - cacheFreeFloorBytesConst) >> 20; got > maxAllowed {
		t.Errorf("resolved %d MiB exceeds the floor-derived max %d MiB", got, maxAllowed)
	}
}

func TestResolveCacheSize(t *testing.T) {
	for _, tc := range []struct {
		name                              string
		configuredMiB, pinnedBytes, total int64
		wantMiB                           int64
	}{
		{
			// The incident's disk. 100 GiB of JuiceFS default already fits under
			// the 118 GiB ceiling a 128 GiB disk's floor implies, so the value is
			// passed through — the POINT is that it is now passed EXPLICITLY and
			// is therefore visible and clamped, not decided silently by juicefs.
			name:          "unconfigured is resolved explicitly rather than left to juicefs",
			configuredMiB: 0, pinnedBytes: 0, total: 128 * gib,
			wantMiB: juicefsDefaultCacheMiB,
		},
		{
			// Big disk: the JuiceFS default fits, so it is passed through
			// explicitly rather than left implicit.
			name:          "unconfigured on a large disk pins the default explicitly",
			configuredMiB: 0, pinnedBytes: 0, total: 4000 * gib,
			wantMiB: juicefsDefaultCacheMiB,
		},
		{
			name:          "configured value is respected when it fits",
			configuredMiB: 50 << 10, pinnedBytes: 0, total: 1000 * gib,
			wantMiB: 50 << 10,
		},
		{
			name:          "grows to fit the pinned set",
			configuredMiB: 10 << 10, pinnedBytes: 200 * gib, total: 1000 * gib,
			wantMiB: (200 * gib) >> 20,
		},
		{
			name:          "pinned set still clamped by the free floor",
			configuredMiB: 10 << 10, pinnedBytes: 900 * gib, total: 500 * gib,
			wantMiB: (500*gib - 10*gib) >> 20,
		},
		{
			// Clamped: 100 GiB cannot fit under a 64 GiB disk's 54 GiB ceiling.
			name:          "unconfigured IS clamped when the default does not fit",
			configuredMiB: 0, pinnedBytes: 0, total: 64 * gib,
			wantMiB: (64*gib - 10*gib) >> 20,
		},
		{
			name:          "disk smaller than the floor yields no size",
			configuredMiB: 0, pinnedBytes: 0, total: 4 * gib,
			wantMiB: 0,
		},
		{
			name:          "unknown disk yields no size",
			configuredMiB: 0, pinnedBytes: 0, total: 0,
			wantMiB: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := resolveCacheSizeMiB(tc.configuredMiB, tc.pinnedBytes, tc.total)
			if got != tc.wantMiB {
				t.Errorf("resolveCacheSizeMiB(%d, %d, %d) = %d MiB, want %d MiB",
					tc.configuredMiB, tc.pinnedBytes, tc.total, got, tc.wantMiB)
			}
		})
	}
}

// TestResolveCacheSizeNeverBreachesFloor is the property that actually matters:
// whatever the inputs, the resolved cache can never be sized to leave less than
// the floor free on the cache volume.
func TestResolveCacheSizeNeverBreachesFloor(t *testing.T) {
	for _, total := range []int64{2 * gib, 8 * gib, 10 * gib, 11 * gib, 16 * gib, 128 * gib, 500 * gib, 1000 * gib, 8000 * gib} {
		for _, cfg := range []int64{0, 1 << 10, 100 << 10, 10000 << 10} {
			for _, pinned := range []int64{0, 50 * gib, 900 * gib, 9000 * gib} {
				got, _ := resolveCacheSizeMiB(cfg, pinned, total)
				if got == 0 {
					continue // 0 == "emit no --cache-size", which cannot breach anything
				}
				if got<<20 > total-cacheFreeFloorBytesConst {
					t.Errorf("total=%dGiB cfg=%dMiB pinned=%dGiB → %d MiB breaches the %d GiB floor",
						total/gib, cfg, pinned/gib, got, cacheFreeFloorBytesConst/gib)
				}
			}
		}
	}
}
