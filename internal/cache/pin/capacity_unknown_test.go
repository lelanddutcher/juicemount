package pin

import "testing"

// A pass that cannot read the pin store, under an admitted mount policy, must
// keep the last over-capacity verdict. Clearing it would release the
// prefetcher's re-warm gate (IsOverCapacity) for that pass.
func TestCapacityKeepsLastVerdictWhenOnePassLosesThePolicy(t *testing.T) {
	t.Cleanup(func() { SetCachePolicy(0, 0, "", 0, false) })
	root := t.TempDir()
	free, dev, ok := volumeFreeBytesAtKnown(root)
	if !ok || free < 1<<20 {
		t.Fatal("test cache volume lacks measurable free space")
	}
	budget := int64(1 << 20)
	SetCachePolicy(budget, 1, root, dev, true)
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Pin("/v/big", 4*budget, "/v"); err != nil {
		t.Fatal(err)
	}
	if v := ComputeCapacity(store, root); !v.PolicyKnown || !v.OverCapacity || !IsOverCapacity() {
		t.Fatalf("precondition: known over-capacity verdict: %+v", v)
	}
	_ = store.Close() // the next AggregateStats fails
	v := ComputeCapacity(store, root)
	if v.PolicyKnown {
		t.Fatal("a failed pin-store read reported a known policy")
	}
	if !v.OverCapacity || !IsOverCapacity() || v.PinnedBytes != 4*budget || v.CacheCapacityBytes == 0 {
		t.Fatalf("one failed pass cleared the over-capacity verdict: %+v", v)
	}

	// With no policy ever set, the same pass reports unknown.
	SetCachePolicy(0, 0, "", 0, false)
	if v := ComputeCapacity(nil, root); v.OverCapacity || v.CacheCapacityBytes != 0 || IsOverCapacity() {
		t.Fatalf("no policy still reported capacity: %+v", v)
	}
}
