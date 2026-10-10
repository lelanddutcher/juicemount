package pin

import "testing"

// The pin capacity verdict must use the floor JuiceFS was ACTUALLY mounted with.
//
// It read a hard-coded 10 GiB while health.resolveFreeSpaceRatio moved the real
// eviction floor to ~30 GiB (above the spool's 20 GiB admission floor, which is
// the premise of counting reclaimable cache as spool headroom). Reading the stale
// value OVER-stated sustainable capacity by the difference, so the over-capacity
// banner UNDER-warned, and IsOverCapacity() gates the prefetcher re-warm so the
// futile-churn guard released late. Both errors in the unsafe direction.
func TestCacheFreeFloorUsesThePublishedValue(t *testing.T) {
	t.Cleanup(func() { cacheFreeFloorBytes.Store(0) })

	cacheFreeFloorBytes.Store(0)
	if got := CacheFreeFloorBytes(); got != CacheFreeFloorBytesDefault {
		t.Errorf("unpublished floor = %d, want the default %d — a verdict computed "+
			"before mount must be no worse than before", got, CacheFreeFloorBytesDefault)
	}

	const derived = int64(30) << 30
	SetCacheFreeFloorBytes(derived)
	if got := CacheFreeFloorBytes(); got != derived {
		t.Errorf("published floor = %d, want %d", got, derived)
	}

	// The published floor must make the verdict MORE conservative, never less —
	// under-warning is the bug being fixed.
	if derived <= CacheFreeFloorBytesDefault {
		t.Fatal("test premise broken: the derived floor should exceed the default")
	}
	SetCacheFreeFloorBytes(-1)
	if got := CacheFreeFloorBytes(); got != CacheFreeFloorBytesDefault {
		t.Errorf("negative floor = %d, want the default", got)
	}
}
