package derivatives

import (
	"encoding/json"
	"reflect"
	"testing"
)

func sp(s string) *string { return &s }
func ip(i int) *int       { return &i }

func openTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// TestKnownAbsent: an inode with no source_assets row is unknown — exists:false,
// nil hash. This drives the /derivatives fail-closed path.
func TestKnownAbsent(t *testing.T) {
	s := openTest(t)
	known, hash := s.Known(999999)
	if known {
		t.Errorf("Known(999999) = true, want false")
	}
	if hash != nil {
		t.Errorf("hash = %v, want nil", *hash)
	}
}

// TestKnownNilHash: a source recorded before its hash is computed is KNOWN but
// reports a nil hash (source_hash:null on the wire).
func TestKnownNilHash(t *testing.T) {
	s := openTest(t)
	if err := s.PutSource(42, nil); err != nil {
		t.Fatalf("PutSource: %v", err)
	}
	known, hash := s.Known(42)
	if !known {
		t.Errorf("Known(42) = false, want true")
	}
	if hash != nil {
		t.Errorf("hash = %v, want nil", *hash)
	}
	// Re-stamp with a hash (the farm finished hashing) — upsert, still one row.
	if err := s.PutSource(42, sp("deadbeef")); err != nil {
		t.Fatalf("PutSource re-stamp: %v", err)
	}
	known, hash = s.Known(42)
	if !known || hash == nil || *hash != "deadbeef" {
		t.Errorf("after re-stamp: known=%v hash=%v, want true/deadbeef", known, hash)
	}
}

// TestManifestEmpty: a known source with no derivative rows yields a nil/empty
// manifest (no error) — the handler turns that into "derivatives": [].
func TestManifestEmpty(t *testing.T) {
	s := openTest(t)
	_ = s.PutSource(7, sp("abc"))
	rows, err := s.Manifest(7)
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("len(rows) = %d, want 0", len(rows))
	}
}

// TestPutDerivUpsert: PutDeriv on the same (inode, kind) overwrites in place —
// status/blob promote when a pending derivative finishes, never duplicating.
func TestPutDerivUpsert(t *testing.T) {
	s := openTest(t)
	_ = s.PutSource(100, sp("h0"))
	if err := s.PutDeriv(100, DerivRow{Kind: "proxy", Status: "pending", Producer: "linux-farm", Version: 1, UpdatedAt: 1}); err != nil {
		t.Fatalf("PutDeriv pending: %v", err)
	}
	if err := s.PutDeriv(100, DerivRow{Kind: "proxy", Status: "ready", Producer: "linux-farm", Version: 1, Hash: sp("h0"), BlobRelPath: sp("proxy.m4s"), MediaType: sp("video/mp4"), UpdatedAt: 2}); err != nil {
		t.Fatalf("PutDeriv ready: %v", err)
	}
	rows, err := s.Manifest(100)
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len(rows) = %d, want 1 (upsert, not insert)", len(rows))
	}
	got := rows[0]
	if got.Status != "ready" || got.BlobRelPath == nil || *got.BlobRelPath != "proxy.m4s" || got.UpdatedAt != 2 {
		t.Errorf("upsert did not promote: %+v", got)
	}
}

// TestManifestOrderAndNullables: rows come back ordered by kind, with nullable
// fields preserved as nil (so the wire emits null, not "").
func TestManifestOrderAndNullables(t *testing.T) {
	s := openTest(t)
	_ = s.PutSource(200, sp("h"))
	_ = s.PutDeriv(200, DerivRow{Kind: "thumbnail", Status: "ready", Producer: "linux-farm", Version: 1, Hash: sp("h"), BlobRelPath: sp("poster.jpg"), MediaType: sp("image/jpeg"), UpdatedAt: 5})
	_ = s.PutDeriv(200, DerivRow{Kind: "embedding", Status: "ready", Producer: "linux-farm", Version: 1, Hash: sp("h"), Model: sp("openclip-vit-b32-v1"), Dim: ip(512), UpdatedAt: 5})
	rows, err := s.Manifest(200)
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	if len(rows) != 2 || rows[0].Kind != "embedding" || rows[1].Kind != "thumbnail" {
		t.Fatalf("order wrong: %+v", rows)
	}
	// embedding: blob/media nil, model/dim set.
	emb := rows[0]
	if emb.BlobRelPath != nil || emb.MediaType != nil || emb.Model == nil || emb.Dim == nil || *emb.Dim != 512 {
		t.Errorf("embedding nullables wrong: %+v", emb)
	}
	// thumbnail: model/dim nil, blob/media set.
	th := rows[1]
	if th.Model != nil || th.Dim != nil || th.BlobRelPath == nil || th.MediaType == nil {
		t.Errorf("thumbnail nullables wrong: %+v", th)
	}
}

// TestMetadataRoundTrip: PutMetadata then Metadata returns the verbatim payload
// plus producer/version/hash; an unproduced kind returns nil (exists:false).
func TestMetadataRoundTrip(t *testing.T) {
	s := openTest(t)
	payload := json.RawMessage(`{"container":"mov","duration_ms":12480}`)
	if err := s.PutMetadata(300, "tech", "linux-farm", 2, sp("hsrc"), payload); err != nil {
		t.Fatalf("PutMetadata: %v", err)
	}
	tm, err := s.Metadata(300, "tech")
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if tm == nil {
		t.Fatal("Metadata = nil, want a row")
	}
	if tm.Producer != "linux-farm" || tm.Version != 2 || tm.Hash == nil || *tm.Hash != "hsrc" {
		t.Errorf("meta fields wrong: %+v", tm)
	}
	var got, want any
	_ = json.Unmarshal(tm.Payload, &got)
	_ = json.Unmarshal(payload, &want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("payload mismatch: got=%v want=%v", got, want)
	}
	// A kind that was never produced ⇒ nil (the /metadata exists:false path).
	if tm2, err := s.Metadata(300, "transcript"); err != nil || tm2 != nil {
		t.Errorf("Metadata(unproduced) = %v, %v; want nil, nil", tm2, err)
	}
}

// i64p is an *int64 helper for source_size in proxy-economics tests.
func i64p(v int64) *int64 { return &v }

// TestListProxyRows: ListProxyRows returns ONLY ready proxy rows (inode +
// source_size); failed proxies and other kinds are excluded. Drives the farm's
// proxy-economics rollup, which stat-walks each returned inode's blob.
func TestListProxyRows(t *testing.T) {
	s := openTest(t)
	// A ready proxy (measurable), a failed proxy (no blob → excluded), and a
	// non-proxy kind (thumb → excluded).
	if err := s.PutDeriv(10, DerivRow{Kind: "proxy", Status: "ready", Producer: "linux-farm", Version: 1, SourceSize: i64p(5_000_000)}); err != nil {
		t.Fatalf("PutDeriv ready proxy: %v", err)
	}
	if err := s.PutDeriv(11, DerivRow{Kind: "proxy", Status: "failed", Producer: "linux-farm", Version: 1, SourceSize: i64p(9_000_000)}); err != nil {
		t.Fatalf("PutDeriv failed proxy: %v", err)
	}
	if err := s.PutDeriv(12, DerivRow{Kind: "thumb", Status: "ready", Producer: "linux-farm", Version: 1}); err != nil {
		t.Fatalf("PutDeriv thumb: %v", err)
	}
	rows, err := s.ListProxyRows()
	if err != nil {
		t.Fatalf("ListProxyRows: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("ListProxyRows len = %d, want 1 (only the ready proxy)", len(rows))
	}
	if rows[0].Inode != 10 || rows[0].SourceSize == nil || *rows[0].SourceSize != 5_000_000 {
		t.Errorf("row = %+v, want inode=10 source_size=5000000", rows[0])
	}
}
