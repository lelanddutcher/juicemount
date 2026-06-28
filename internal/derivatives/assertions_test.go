package derivatives

import "testing"

// TestAssertLWWAcceptThenReject: a fresh triple is accepted; a strictly-newer one
// for the same (asset_key,namespace,key) wins; an older-or-equal one is rejected
// with the stored winner's asserted_at, and the stored value is untouched.
func TestAssertLWWAcceptThenReject(t *testing.T) {
	s := openTest(t)
	const ak, ns, k = "xxh3:deadbeefdeadbeef", "log_profile", "value"

	// fresh → accept
	r, err := s.AssertLWW(ak, ns, k, `"clog3"`, "ol:a", "2026-06-27T18:40:00Z", 1180417)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Accepted || r.WinningAssertedAt != "2026-06-27T18:40:00Z" {
		t.Fatalf("fresh: %+v, want accepted at 18:40", r)
	}

	// older → reject-stale, winner unchanged
	r, err = s.AssertLWW(ak, ns, k, `"slog3"`, "ol:b", "2026-06-27T10:00:00Z", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Accepted || r.WinningAssertedAt != "2026-06-27T18:40:00Z" {
		t.Fatalf("stale: %+v, want rejected, winner=18:40", r)
	}

	// equal → also reject (strictly-newer wins)
	r, _ = s.AssertLWW(ak, ns, k, `"slog3"`, "ol:b", "2026-06-27T18:40:00Z", 0)
	if r.Accepted {
		t.Fatalf("equal asserted_at must be rejected")
	}

	// the stored value must still be the original clog3
	raws, err := s.AssertionsByAssetKey(ak)
	if err != nil {
		t.Fatal(err)
	}
	if len(raws) != 1 || raws[0].ValueJSON != `"clog3"` {
		t.Fatalf("stored = %+v, want single clog3", raws)
	}

	// newer → accept, replace
	r, _ = s.AssertLWW(ak, ns, k, `"none"`, "ol:c", "2026-06-27T20:00:00Z", 0)
	if !r.Accepted {
		t.Fatalf("newer must be accepted")
	}
	raws, _ = s.AssertionsByAssetKey(ak)
	if raws[0].ValueJSON != `"none"` {
		t.Fatalf("after newer: %s, want none", raws[0].ValueJSON)
	}
}

// TestAssertRetractKept: a retract (value:null) with a newer asserted_at wins and
// is kept as a null-valued row (not deleted) — the un-assert is itself durable.
func TestAssertRetractKept(t *testing.T) {
	s := openTest(t)
	const ak = "xxh3:1111111111111111"
	_, _ = s.AssertLWW(ak, "log_profile", "value", `"clog3"`, "ol:a", "2026-06-27T18:40:00Z", 0)
	r, _ := s.AssertLWW(ak, "log_profile", "value", `null`, "ol:a", "2026-06-27T19:05:00Z", 0)
	if !r.Accepted {
		t.Fatal("retract must be accepted (newer)")
	}
	raws, _ := s.AssertionsByAssetKey(ak)
	if len(raws) != 1 || raws[0].ValueJSON != "null" {
		t.Fatalf("retract row = %+v, want single null", raws)
	}
}

// TestAssertMergeNotClobber: two namespaces on the same asset_key coexist.
func TestAssertMergeNotClobber(t *testing.T) {
	s := openTest(t)
	const ak = "xxh3:2222222222222222"
	_, _ = s.AssertLWW(ak, "log_profile", "value", `"clog3"`, "ol:a", "2026-06-27T18:40:00Z", 0)
	_, _ = s.AssertLWW(ak, "person", "c_3f", `"Bob"`, "ol:a", "2026-06-27T18:41:12Z", 0)
	raws, _ := s.AssertionsByAssetKey(ak)
	if len(raws) != 2 {
		t.Fatalf("got %d, want 2 (log_profile + person)", len(raws))
	}
}

// TestInodeAccelerator: an inode/path query resolves to the asset_key and back.
func TestInodeAccelerator(t *testing.T) {
	s := openTest(t)
	const ak = "xxh3:3333333333333333"
	_, _ = s.AssertLWW(ak, "rating", "value", `5`, "ol:a", "2026-06-27T18:40:00Z", 4242)
	if got, _ := s.AssetKeyForInode(4242); got != ak {
		t.Errorf("AssetKeyForInode = %q, want %q", got, ak)
	}
	if got, _ := s.InodeForAssetKey(ak); got != 4242 {
		t.Errorf("InodeForAssetKey = %d, want 4242", got)
	}
	if got, _ := s.AssetKeyForInode(999999); got != "" {
		t.Errorf("unknown inode = %q, want empty (fail-closed)", got)
	}
}

// TestAssertionsByAssetKeyEmpty: a never-asserted key yields a non-nil empty slice.
func TestAssertionsByAssetKeyEmpty(t *testing.T) {
	s := openTest(t)
	raws, err := s.AssertionsByAssetKey("xxh3:0000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	if raws == nil || len(raws) != 0 {
		t.Errorf("got %v, want non-nil empty slice", raws)
	}
}
