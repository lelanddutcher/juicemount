package main

import (
	"syscall"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

func TestCachePolicyFromOwnerTransfersExactFloorAndVolume(t *testing.T) {
	t.Cleanup(func() { pin.SetCachePolicy(0, 0, "", 0, false) })
	root := t.TempDir()
	var st syscall.Stat_t
	if err := syscall.Stat(root, &st); err != nil {
		t.Fatal(err)
	}
	const floor = int64(30) << 30
	cachePolicyFromOwner(volumeOwnerReceipt{
		CacheSizeMB: "1024", CacheFreeFloorBytes: floor,
		CacheVolumePath: root, CacheVolumeDev: uint64(st.Dev), CachePolicyKnown: true,
	})
	v := pin.ComputeCapacity(nil, root)
	if !v.PolicyKnown || v.CacheFreeFloorBytes != floor || v.CacheBudgetBytes != 1<<30 {
		t.Fatalf("owner policy was not applied exactly: %+v", v)
	}
	cachePolicyFromOwner(volumeOwnerReceipt{CacheSizeMB: "1024"})
	if v := pin.ComputeCapacity(nil, root); v.PolicyKnown || pin.CacheBudgetBytes() != 0 {
		t.Fatalf("missing owner policy reused old mount: %+v", v)
	}
}

// Laptop run 2026-11-12 (scenario 5b): a launch with the backend unreachable
// mounts no FUSE, so the owner reports no cache policy. /cache-status then said
// cache_capacity_bytes 407 GB (free disk) and cache_budget_bytes 0. Without an
// admitted policy the capacity must read as unknown (0), never the free disk.
func TestOfflineBootCacheStatusReportsUnknownCapacity(t *testing.T) {
	t.Cleanup(func() { pin.SetCachePolicy(0, 0, "", 0, false) })
	root := t.TempDir()
	store, err := pin.Open(root + "/pins.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Pin("/v/clip.mov", 1<<30, "/v"); err != nil {
		t.Fatal(err)
	}
	// What the engine applies for an owner that started offline.
	cachePolicyFromOwner(volumeOwnerReceipt{})
	v := pin.ComputeCapacity(store, root)
	if v.PolicyKnown {
		t.Fatalf("offline boot reported a known policy: %+v", v)
	}
	if v.CacheCapacityBytes != 0 || v.CacheBudgetBytes != 0 || v.ShortfallBytes != 0 || v.OverCapacity {
		t.Fatalf("unknown policy reported disk-derived capacity: %+v", v)
	}
	if v.DiskFreeBytes <= 0 || v.PinnedBytes != 1<<30 {
		t.Fatalf("raw inputs lost while capacity is unknown: %+v", v)
	}
	if got := pin.Capacity(); got.CacheCapacityBytes != 0 || got.OverCapacity {
		t.Fatalf("published verdict kept disk-derived capacity: %+v", got)
	}

	// Once a mount admits its policy, the real figures return.
	var st syscall.Stat_t
	if err := syscall.Stat(root, &st); err != nil {
		t.Fatal(err)
	}
	cachePolicyFromOwner(volumeOwnerReceipt{
		CacheSizeMB: "3302", CacheFreeFloorBytes: 1 << 20,
		CacheVolumePath: root, CacheVolumeDev: uint64(st.Dev), CachePolicyKnown: true,
	})
	v = pin.ComputeCapacity(store, root)
	if !v.PolicyKnown || v.CacheBudgetBytes != 3302<<20 || v.CacheCapacityBytes == 0 || v.CacheCapacityBytes > 3302<<20 {
		t.Fatalf("admitted policy not reported after the link returned: %+v", v)
	}
}
