package health

import (
	"testing"

	"github.com/lelanddutcher/juicemount/nfs"
)

// freespaceratio_test.go — B-2: JuiceFS's eviction floor must sit ABOVE the
// write spool's admission floor, or the cache never gives its space back.
//
// realisticDiskSizes spans every Mac boot disk the app plausibly runs on.
var realisticDiskSizes = []int64{128 * gib, 256 * gib, 500 * gib, 512 * gib,
	1000 * gib, 1024 * gib, 2000 * gib, 2048 * gib, 4096 * gib, 8192 * gib}

// THE BUG. The ratio used to be cacheFreeFloorBytesConst/total — JuiceFS evicted
// only once free fell below 10 GiB, while the spool already stopped admitting at
// 20 GiB free. The spool blocked itself first and JuiceFS never yielded a byte,
// so the reclaimable cache the spool's headroom formula counts on was never
// actually returned. This is the property that inverts the ordering.
func TestFreeSpaceRatioKeepsJuicefsFloorAboveTheSpoolFloor(t *testing.T) {
	for _, total := range realisticDiskSizes {
		ratio := resolveFreeSpaceRatio(total)
		juicefsFloor := int64(ratio * float64(total))
		if juicefsFloor <= spoolFreeFloorBytesConst {
			t.Errorf("total=%d GiB: ratio %.4f keeps only %d GiB free — at or below the spool's "+
				"%d GiB floor, so the spool blocks first and the cache never yields",
				total/gib, ratio, juicefsFloor/gib, spoolFreeFloorBytesConst/gib)
		}
	}
}

// The old ratio, spelled out, so the regression is unmistakable: on every
// realistic disk the previous formula put the JuiceFS floor BELOW the spool's.
func TestFreeSpaceRatio_OldFormulaWasBelowTheSpoolFloor(t *testing.T) {
	for _, total := range realisticDiskSizes {
		old := float64(cacheFreeFloorBytesConst) / float64(total)
		if int64(old*float64(total)) > spoolFreeFloorBytesConst {
			t.Fatalf("premise wrong: the old ratio on a %d GiB disk already cleared the spool floor", total/gib)
		}
		if resolveFreeSpaceRatio(total) <= old {
			t.Errorf("total=%d GiB: derived ratio %.4f did not raise the old %.4f",
				total/gib, resolveFreeSpaceRatio(total), old)
		}
	}
}

// The pre-existing guarantee this line originally shipped for: never leave the
// boot disk with less than the 10 GiB free floor. Must survive the rewrite for
// EVERY disk size, including the small ones where the 0.25 clamp bites.
func TestFreeSpaceRatioNeverWeakensThe10GiBFloor(t *testing.T) {
	sizes := append([]int64{4 * gib, 8 * gib, 16 * gib, 32 * gib, 40 * gib, 64 * gib, 80 * gib},
		realisticDiskSizes...)
	for _, total := range sizes {
		ratio := resolveFreeSpaceRatio(total)
		if kept := int64(ratio * float64(total)); kept < cacheFreeFloorBytesConst {
			t.Errorf("total=%d GiB: ratio %.4f keeps only %d GiB free, below the %d GiB hard floor",
				total/gib, ratio, kept/gib, cacheFreeFloorBytesConst/gib)
		}
	}
}

// The ratio stays inside its declared bounds on any disk big enough that the
// 10 GiB hard floor doesn't have to override them.
func TestFreeSpaceRatioStaysWithinBounds(t *testing.T) {
	for _, total := range append([]int64{64 * gib}, realisticDiskSizes...) {
		ratio := resolveFreeSpaceRatio(total)
		if ratio < minFreeSpaceRatio || ratio > maxFreeSpaceRatio {
			t.Errorf("total=%d GiB: ratio %.4f outside [%.2f, %.2f]",
				total/gib, ratio, minFreeSpaceRatio, maxFreeSpaceRatio)
		}
	}
	// An unknown disk yields 0 — the caller then leaves the config untouched.
	if got := resolveFreeSpaceRatio(0); got != 0 {
		t.Errorf("unknown disk: got %.4f, want 0", got)
	}
	if got := resolveFreeSpaceRatio(-1); got != 0 {
		t.Errorf("negative total: got %.4f, want 0", got)
	}
}

// The decision Mount actually applies: which ratio string reaches juicefs, and
// the explicit rule that a stricter configured ratio is never weakened.
func TestResolveFreeSpaceRatioArg(t *testing.T) {
	for _, tc := range []struct {
		name        string
		configured  string
		total       int64
		want        string
		wantReplace bool
	}{
		{
			// The common case: nothing configured, a 1 TB disk. 30 GiB / 1000 GiB.
			name:       "unconfigured gets the derived floor",
			configured: "", total: 1000 * gib,
			want: "0.0300", wantReplace: true,
		},
		{
			// The OLD value, still in a config file. It must be raised: 0.01 of a
			// 1 TB disk is 10 GiB, i.e. below the spool's 20 GiB floor.
			name:       "the old 0.01 is raised",
			configured: "0.01", total: 1000 * gib,
			want: "0.0300", wantReplace: true,
		},
		{
			name:       "a stricter configured ratio is never weakened",
			configured: "0.5", total: 1000 * gib,
			wantReplace: false,
		},
		{
			name:       "an equal configured ratio is left alone",
			configured: "0.03", total: 1000 * gib,
			wantReplace: false,
		},
		{
			name:       "an unknown disk leaves the config untouched",
			configured: "0.01", total: 0,
			wantReplace: false,
		},
		{
			// Large disk: the derived value clamps to the 0.01 minimum, which is
			// still 80 GiB free — comfortably above the spool floor.
			name:       "a very large disk clamps to the minimum ratio",
			configured: "", total: 8192 * gib,
			want: "0.0100", wantReplace: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, replace := resolveFreeSpaceRatioArg(tc.configured, tc.total)
			if replace != tc.wantReplace {
				t.Fatalf("replace = %v, want %v (got %q)", replace, tc.wantReplace, got)
			}
			if replace && got != tc.want {
				t.Errorf("ratio arg = %q, want %q", got, tc.want)
			}
			if !replace && got != "" {
				t.Errorf("no replacement expected but got %q", got)
			}
		})
	}
}

// The mirrored constant is the one thing that can silently rot: if the spool's
// floor ever moves, this derivation quietly stops clearing it and B-2 reverts to
// the bug with no other symptom.
func TestSpoolFloorMirrorHasNotDrifted(t *testing.T) {
	if spoolFreeFloorBytesConst != nfs.SpoolFreeFloorBytes {
		t.Fatalf("health.spoolFreeFloorBytesConst = %d but nfs.SpoolFreeFloorBytes = %d — "+
			"the derived --free-space-ratio no longer clears the spool's real floor",
			spoolFreeFloorBytesConst, nfs.SpoolFreeFloorBytes)
	}
}
