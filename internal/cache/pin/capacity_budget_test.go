package pin

import "testing"

// TestEvaluateCapacityBudgetCeiling pins the audit fix: the user cache budget
// caps sustainable capacity regardless of disk headroom, so a pin set larger
// than the budget is over-capacity even on a roomy disk.
func TestEvaluateCapacityBudgetCeiling(t *testing.T) {
	g := int64(1) << 30
	v := CapacityVerdict{
		PinnedBytes:      150 * g,
		DiskFreeBytes:    800 * g, // roomy disk — the old verdict approved this
		CacheUsageBytes:  90 * g,
		CacheBudgetBytes: 100 * g, // user set 100G cache
	}
	evaluateCapacity(&v)
	if v.CacheCapacityBytes != 100*g {
		t.Fatalf("capacity = %d, want budget ceiling %d", v.CacheCapacityBytes, 100*g)
	}
	if !v.OverCapacity || v.ShortfallBytes != 50*g {
		t.Fatalf("over=%v shortfall=%d — a budget-exceeding pin set must be flagged", v.OverCapacity, v.ShortfallBytes)
	}

	// Under-budget pins on the same disk: fine.
	v2 := CapacityVerdict{PinnedBytes: 50 * g, DiskFreeBytes: 800 * g, CacheUsageBytes: 90 * g, CacheBudgetBytes: 100 * g}
	evaluateCapacity(&v2)
	if v2.OverCapacity {
		t.Fatal("under-budget pin set wrongly flagged")
	}

	// No budget known (0): disk-based capacity, old behavior byte-for-byte.
	v3 := CapacityVerdict{PinnedBytes: 150 * g, DiskFreeBytes: 800 * g, CacheUsageBytes: 90 * g}
	evaluateCapacity(&v3)
	if v3.OverCapacity {
		t.Fatal("no-budget verdict regressed (disk-based capacity should approve)")
	}
}
