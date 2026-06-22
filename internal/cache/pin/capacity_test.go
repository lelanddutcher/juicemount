package pin

import "testing"

const gib = int64(1) << 30

// TestEvaluateCapacity covers the pinned-set-vs-disk verdict math (R-1): the
// sustainable ceiling is (free + cache_usage − floor), and over-capacity needs
// to clear the flap margin so the banner doesn't toggle on free-space jitter.
func TestEvaluateCapacity(t *testing.T) {
	cases := []struct {
		name          string
		pinned        int64
		free          int64
		cacheUsage    int64
		wantOver      bool
		wantCapacity  int64
		wantShortfall int64
	}{
		{
			// 180 GB pinned, 139 GB free, 9 GB already cached on a near-full
			// disk: ceiling = 139 + 9 − 10 = 138 GB; pinned exceeds it by 42 GB.
			// This is the field scenario that motivated R-1.
			name: "pinned exceeds free disk", pinned: 180 * gib, free: 139 * gib, cacheUsage: 9 * gib,
			wantOver: true, wantCapacity: 138 * gib, wantShortfall: 42 * gib,
		},
		{
			// Pinned set already fully resident with plenty of headroom: the
			// cache's own occupancy keeps the ceiling above the pinned size,
			// so this must NOT false-positive.
			name: "fully resident, healthy headroom", pinned: 50 * gib, free: 400 * gib, cacheUsage: 50 * gib,
			wantOver: false, wantCapacity: 440 * gib, wantShortfall: 0,
		},
		{
			// Pinned set bigger than current FREE space but it's exactly the
			// bytes already cached — must not flag (the prior bug would have,
			// comparing pinned to free alone).
			name: "pinned == cached, low free", pinned: 180 * gib, free: 20 * gib, cacheUsage: 180 * gib,
			wantOver: false, wantCapacity: 190 * gib, wantShortfall: 0,
		},
		{
			// Just 1 GB over the ceiling — inside the 2 GiB flap margin, so no
			// banner (anti-flap).
			name: "within flap margin", pinned: 100*gib + 1*gib, free: 110 * gib, cacheUsage: 0,
			wantOver: false, wantCapacity: 100 * gib, wantShortfall: 1 * gib,
		},
		{
			// Nothing pinned: never over capacity regardless of disk state.
			name: "nothing pinned", pinned: 0, free: 1 * gib, cacheUsage: 0,
			wantOver: false, wantCapacity: 0, wantShortfall: 0,
		},
		{
			// Disk already below the floor: capacity clamps to 0, any pinned
			// set is over.
			name: "below floor", pinned: 5 * gib, free: 2 * gib, cacheUsage: 0,
			wantOver: true, wantCapacity: 0, wantShortfall: 5 * gib,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := CapacityVerdict{PinnedBytes: c.pinned, DiskFreeBytes: c.free, CacheUsageBytes: c.cacheUsage}
			evaluateCapacity(&v)
			if v.OverCapacity != c.wantOver {
				t.Errorf("OverCapacity = %v, want %v", v.OverCapacity, c.wantOver)
			}
			if v.CacheCapacityBytes != c.wantCapacity {
				t.Errorf("CacheCapacityBytes = %d GiB, want %d GiB", v.CacheCapacityBytes/gib, c.wantCapacity/gib)
			}
			if v.ShortfallBytes != c.wantShortfall {
				t.Errorf("ShortfallBytes = %d GiB, want %d GiB", v.ShortfallBytes/gib, c.wantShortfall/gib)
			}
		})
	}
}
