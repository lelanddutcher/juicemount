package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farm"
)

// POST /derivatives/batch (JM-22) is the bulk form of GET /derivatives?inode=N:
// the consumer asks "which of these thousands of inodes do you already have?"
// in one round trip instead of N.
//
// The property that makes it worth having is NOT the saved HTTP overhead — it
// is that the single-inode route runs farm.ReconcileOneSidecar on a miss (a
// filesystem read through the mount), so N single queries drive N filesystem
// reconciles. TestDerivativesBatchDoesNoOnMissReconcile below is therefore the
// load-bearing test of this route, not a nice-to-have.

func withDerivStore(t *testing.T, ds *derivatives.Store, fusePath string) {
	t.Helper()
	globalMu.Lock()
	oldD, oldF := globalDerivStore, globalFUSEPath
	globalDerivStore, globalFUSEPath = ds, fusePath
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalDerivStore, globalFUSEPath = oldD, oldF
		globalMu.Unlock()
	})
}

func postBatch(t *testing.T, inodes []uint64) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]any{"inodes": inodes})
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handleDerivativesBatchHTTP(rr, httptest.NewRequest("POST", "/derivatives/batch", bytes.NewReader(body)))
	return rr
}

func decodeBatch(t *testing.T, rr *httptest.ResponseRecorder) map[string]derivativesBatchEntry {
	t.Helper()
	if rr.Code != 200 {
		t.Fatalf("status %d, want 200; body %s", rr.Code, rr.Body.String())
	}
	var out map[string]derivativesBatchEntry
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("body is not the agreed inode-keyed map (%v): %s", err, rr.Body.String())
	}
	return out
}

func putReady(t *testing.T, ds *derivatives.Store, inode uint64, kind string) {
	t.Helper()
	rel, media := kind+".bin", "application/octet-stream"
	if err := ds.PutDeriv(inode, derivatives.DerivRow{
		Kind: kind, Status: "ready", Producer: "linux-farm", Version: 1,
		BlobRelPath: &rel, MediaType: &media,
	}); err != nil {
		t.Fatal(err)
	}
}

// The agreed shape: a map keyed by decimal inode, ready_kinds + source_hash per
// entry, and an inode the index has never heard of answered as ready_kinds:[]
// rather than omitted or an error.
func TestDerivativesBatchShape(t *testing.T) {
	ds, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	withDerivStore(t, ds, t.TempDir())

	hash := "7f3197aa00000001"
	if err := ds.PutSource(48899, &hash); err != nil {
		t.Fatal(err)
	}
	putReady(t, ds, 48899, "proxy")
	putReady(t, ds, 48899, "poster")
	// A known asset whose farm work FAILED: the row exists but is not ready, so
	// it must not appear in ready_kinds — a consumer treating "failed" as ready
	// would fetch a blob that will never exist.
	failedRel := "waveform.json"
	if err := ds.PutSource(48908, nil); err != nil {
		t.Fatal(err)
	}
	if err := ds.PutDeriv(48908, derivatives.DerivRow{
		Kind: "waveform", Status: "failed", Producer: "linux-farm", Version: 1,
		BlobRelPath: &failedRel,
	}); err != nil {
		t.Fatal(err)
	}

	got := decodeBatch(t, postBatch(t, []uint64{48899, 48908, 99999}))

	if len(got) != 3 {
		t.Fatalf("got %d entries, want 3 (every requested inode must be answered): %v", len(got), got)
	}
	e := got["48899"]
	if len(e.ReadyKinds) != 2 || e.ReadyKinds[0] != "poster" || e.ReadyKinds[1] != "proxy" {
		t.Errorf("48899 ready_kinds = %v, want [poster proxy] (Manifest orders by kind)", e.ReadyKinds)
	}
	if e.SourceHash == nil || *e.SourceHash != hash {
		t.Errorf("48899 source_hash = %v, want %q", e.SourceHash, hash)
	}
	if e := got["48908"]; len(e.ReadyKinds) != 0 {
		t.Errorf("48908 ready_kinds = %v, want [] — a failed row is not a ready one", e.ReadyKinds)
	} else if e.SourceHash != nil {
		t.Errorf("48908 source_hash = %v, want null (the farm has not hashed it)", *e.SourceHash)
	}
	if e := got["99999"]; len(e.ReadyKinds) != 0 || e.SourceHash != nil {
		t.Errorf("unknown inode answered %+v, want {ready_kinds:[], source_hash:null}", e)
	}
	// ready_kinds must serialize as [] and never null: the consumer ranges over
	// it, and a null would be a decode-abort on the Swift side.
	if !bytes.Contains(rrBody(t, []uint64{99999}), []byte(`"ready_kinds":[]`)) {
		t.Error(`unknown inode did not serialize "ready_kinds":[] — a null slice decodes as nil`)
	}
}

func rrBody(t *testing.T, inodes []uint64) []byte {
	t.Helper()
	return postBatch(t, inodes).Body.Bytes()
}

// Over the cap the request is REJECTED, not truncated. A dropped tail comes
// back as an absent key, which a reconciling consumer reads as "the farm has
// nothing for these" — it would regenerate thousands of existing derivatives
// and no error would appear anywhere.
func TestDerivativesBatchRejectsOverCap(t *testing.T) {
	ds, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	withDerivStore(t, ds, t.TempDir())

	inodes := make([]uint64, derivativesBatchMaxInodes+1)
	for i := range inodes {
		inodes[i] = uint64(i + 1)
	}
	rr := postBatch(t, inodes)
	if rr.Code != 400 {
		t.Fatalf("status %d for %d inodes, want 400 (cap %d)", rr.Code, len(inodes), derivativesBatchMaxInodes)
	}

	// And exactly at the cap it is accepted — an off-by-one here would reject a
	// consumer paging at the documented size.
	rr = postBatch(t, inodes[:derivativesBatchMaxInodes])
	if rr.Code != 200 {
		t.Fatalf("status %d at exactly the cap, want 200", rr.Code)
	}
}

// THE load-bearing test. Batch mode must NOT run the on-miss sidecar reconcile
// that GET /derivatives?inode=N runs — that reconcile is a filesystem read
// through the mount, and 1000 of them is the cost this route exists to remove.
//
// Validity gate: the same sidecar is then fed to the SINGLE-inode handler,
// which must ingest it. Without that arm a batch that quietly stopped reading
// the volume for any other reason (wrong path, unreadable file) would score an
// identical pass and prove nothing.
func TestDerivativesBatchDoesNoOnMissReconcile(t *testing.T) {
	const inode = 5150
	tmp := t.TempDir()
	ds, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	withDerivStore(t, ds, tmp)

	// A real farm sidecar on the volume, unknown to the index.
	dir := filepath.Join(tmp, ".juicemount", "derivatives", itoa(inode))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	rel, media, hash := "proxy.mp4", "video/mp4", "7f3197aa0000dead"
	sc := farm.ManifestSidecar{
		Inode: inode, SourceHash: &hash,
		Derivatives: []derivatives.DerivRow{{
			Kind: "proxy", Status: "ready", Producer: "linux-farm", Version: 1,
			BlobRelPath: &rel, MediaType: &media,
		}},
	}
	raw, err := json.Marshal(sc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	got := decodeBatch(t, postBatch(t, []uint64{inode}))
	if e := got[itoa(inode)]; len(e.ReadyKinds) != 0 || e.SourceHash != nil {
		t.Errorf("batch answered %+v for an inode only the SIDECAR knows about — it "+
			"reconciled on a miss. That is a filesystem read per inode, which is exactly "+
			"the cost this route exists to remove (contract (AO) Q3).", e)
	}
	if known, _ := ds.Known(inode); known {
		t.Error("batch ingested the sidecar into the store — it must not touch the volume at all")
	}

	// VALIDITY GATE: the sidecar must be one the single-inode route DOES ingest.
	rr := httptest.NewRecorder()
	handleDerivativesHTTP(rr, httptest.NewRequest("GET", "/derivatives?inode="+itoa(inode), nil))
	var single derivativesResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &single); err != nil {
		t.Fatal(err)
	}
	if !single.Exists {
		t.Fatal("INCONCLUSIVE: the single-inode route did not ingest this sidecar either, " +
			"so the batch arm above proves nothing about reconcile suppression — fix the fixture")
	}
}

// A repeated inode is answered once and costs one query, not two.
func TestDerivativesBatchDedupes(t *testing.T) {
	ds, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	withDerivStore(t, ds, t.TempDir())
	if err := ds.PutSource(7, nil); err != nil {
		t.Fatal(err)
	}
	putReady(t, ds, 7, "poster")

	got := decodeBatch(t, postBatch(t, []uint64{7, 7, 7}))
	if len(got) != 1 || len(got["7"].ReadyKinds) != 1 {
		t.Errorf("dedupe failed: %+v", got)
	}
}

// GET must not be mistaken for the batch route (it carries no body).
func TestDerivativesBatchRejectsGET(t *testing.T) {
	rr := httptest.NewRecorder()
	handleDerivativesBatchHTTP(rr, httptest.NewRequest("GET", "/derivatives/batch", nil))
	if rr.Code != 405 {
		t.Errorf("status %d for GET, want 405", rr.Code)
	}
}
