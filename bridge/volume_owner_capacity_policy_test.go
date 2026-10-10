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
