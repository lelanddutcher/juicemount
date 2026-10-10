package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/metadata"
)

// TestSidecarCannotForgeFarmProvenance reproduces the attack the 2026-08-04
// second adversarial review executed against the widened register route.
//
// The premise of contribute-back is that a SECOND app can write the volume. The
// per-inode manifest.json sidecar lives ON that volume. Before this fix nothing
// in the reconcile path looked at `producer` at all, so a hand-written manifest
// declaring producer:"linux-farm" minted a row the REAL farm never produced —
// which both impersonated the farm and weaponised the register route's own
// producer-precedence guard: every subsequent genuine on-device register for
// that (inode,kind) returned 409, locking the real producer out of its own slot.
//
// The fix keys the guard on Provenance (stamped by the ingesting code path,
// never readable from the file) instead of on the forgeable producer string.
func TestSidecarCannotForgeFarmProvenance(t *testing.T) {
	tmp := t.TempDir() // stands in for the FUSE mount root
	const inode = 600001
	const rel = "Project_Foo/clip_forge.mov"

	src := filepath.Join(tmp, rel)
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, bytes.Repeat([]byte("forge-"), 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	mt := time.Unix(1700000000, 0)
	if err := os.Chtimes(src, mt, mt); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(src)
	liveSize, liveMtime := fi.Size(), fi.ModTime().Unix()

	// THE ATTACK: a non-farm writer authors the sidecar + the blob. Nothing here
	// requires any credential — it is an ordinary file write to a shared volume.
	derivDir := filepath.Join(tmp, ".juicemount", "derivatives", "600001")
	if err := os.MkdirAll(derivDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(derivDir, "poster.jpg"), []byte("\xff\xd8\xff\xe0 not really a jpeg"), 0o644); err != nil {
		t.Fatal(err)
	}
	forgedBlob := "poster.jpg"
	forgedMT := "text/html"
	forgedSize := int64(1)
	forgedMtime := int64(1)
	forged := farm.ManifestSidecar{
		Inode: inode,
		Derivatives: []derivatives.DerivRow{{
			Kind:     "thumbnail",
			Status:   "ready",
			Producer: "linux-farm", // <-- the forgery
			Version:  1,
			// ...and the Content-Type forgery the same row used to carry.
			BlobRelPath: &forgedBlob,
			MediaType:   &forgedMT,
			// ...and a freshness forgery that would pin a wrong derivative "fresh".
			SourceSize:  &forgedSize,
			SourceMtime: &forgedMtime,
		}},
	}
	fb, _ := json.Marshal(forged)
	if err := os.WriteFile(filepath.Join(derivDir, "manifest.json"), fb, 0o644); err != nil {
		t.Fatal(err)
	}

	mstore, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer mstore.Close()
	mstore.InsertToCache(&metadata.Entry{
		Path: rel, Name: "clip_forge.mov", ParentPath: "Project_Foo",
		Inode: inode, Size: liveSize, Mtime: mt,
	})

	dstore, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer dstore.Close()

	globalMu.Lock()
	oldS, oldD, oldF := globalStore, globalDerivStore, globalFUSEPath
	globalStore, globalDerivStore, globalFUSEPath = mstore, dstore, tmp
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalStore, globalDerivStore, globalFUSEPath = oldS, oldD, oldF
		globalMu.Unlock()
	}()

	// Ingest the attacker's sidecar exactly as the on-miss reconcile would.
	found, err := farm.ReconcileOneSidecar(dstore, tmp, inode)
	if err != nil || !found {
		t.Fatalf("reconcile: found=%v err=%v", found, err)
	}

	rows, err := dstore.Manifest(inode)
	if err != nil {
		t.Fatal(err)
	}
	var got *derivatives.DerivRow
	for i := range rows {
		if rows[i].Kind == "thumbnail" {
			got = &rows[i]
		}
	}
	if got == nil {
		t.Fatal("thumbnail row missing after reconcile")
	}

	// The row is still INDEXED — disaster recovery and thumbnail serving keep
	// working — but it is marked with unforgeable provenance...
	if got.Provenance != derivatives.ProvenanceSidecar {
		t.Errorf("provenance = %q, want %q", got.Provenance, derivatives.ProvenanceSidecar)
	}
	// ...and every piece of POLICY it tried to smuggle was overridden.
	if got.MediaType == nil || *got.MediaType != "image/jpeg" {
		t.Errorf("media_type = %v, want image/jpeg (server-assigned, not the sidecar's text/html)", got.MediaType)
	}
	// The vouch is passed THROUGH, not dropped: nulling it disabled the C5
	// stale-derivative gate for every farm-derived asset (see sidecar.go). C5
	// compares it against the LIVE source, which is what makes a stale row
	// withhold — so a present-but-wrong vouch is handled, an absent one is not.
	if got.SourceSize == nil || got.SourceMtime == nil {
		t.Error("source vouch was dropped — that disables the C5 stale gate permanently")
	}

	// KNOWN AND ACCEPTED LIMITATION, asserted so it cannot regress silently.
	//
	// A forged manifest CAN still 409-lock this (inode,kind) against a genuine
	// on-device register, because "produced by the farm" is not an authenticated
	// property on the Mac — every farm row arrives through this same unsigned
	// file. An earlier attempt to fix that keyed the guard on provenance and
	// thereby disabled it for genuine farm rows too, which was strictly worse.
	// Whoever can forge this manifest can also write the blob bytes, so the guard
	// is not the thing standing between them and mischief. Closing it for real
	// requires an authenticated sidecar, not a cleverer predicate.
	body, _ := json.Marshal(map[string]any{
		"inode": inode, "kind": "thumbnail", "producer": "on-device",
		"source_size": liveSize, "source_mtime": liveMtime,
		"blob_rel_path": "poster.jpg",
	})
	rr := httptest.NewRecorder()
	handleDerivativesRegisterHTTP(rr, httptest.NewRequest("POST", "/derivatives/register", bytes.NewReader(body)))
	if rr.Code != 409 {
		t.Fatalf("register over a forged farm row: status %d, want 409 "+
			"(if this now returns 200 the precedence guard has gone dead — "+
			"check that it is not keyed on something every reconciled row carries)",
			rr.Code)
	}
}

// TestRealFarmRowStillWins is the other half: provenance must not be a blanket
// disable of the precedence guard. A row the farm actually minted (no sidecar
// stamp) must still refuse to be overwritten by a consumer contribution.
func TestRealFarmRowStillWins(t *testing.T) {
	tmp := t.TempDir()
	const inode = 600002
	const rel = "Project_Foo/clip_real.mov"

	src := filepath.Join(tmp, rel)
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, bytes.Repeat([]byte("real-"), 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	mt := time.Unix(1700000000, 0)
	_ = os.Chtimes(src, mt, mt)
	fi, _ := os.Stat(src)

	derivDir := filepath.Join(tmp, ".juicemount", "derivatives", "600002")
	_ = os.MkdirAll(derivDir, 0o755)
	_ = os.WriteFile(filepath.Join(derivDir, "poster.jpg"), []byte("jpeg"), 0o644)

	mstore, _ := metadata.Open(":memory:")
	defer mstore.Close()
	mstore.InsertToCache(&metadata.Entry{
		Path: rel, Name: "clip_real.mov", ParentPath: "Project_Foo",
		Inode: inode, Size: fi.Size(), Mtime: mt,
	})
	dstore, _ := derivatives.Open(":memory:")
	defer dstore.Close()

	// SEEDED THE WAY PRODUCTION ACTUALLY DOES IT. The previous version of this
	// test called dstore.PutDeriv directly — a state the Mac can never reach,
	// because farm generation runs only in cmd/jmfarm and every farm row arrives
	// here through ReconcileOneSidecar. That gap is exactly why a change that
	// made this guard a dead branch in production still passed this test.
	blob := "poster.jpg"
	sz, mtm := fi.Size(), fi.ModTime().Unix()
	sc := farm.ManifestSidecar{Inode: inode, Derivatives: []derivatives.DerivRow{{
		Kind: "thumbnail", Status: "ready", Producer: "linux-farm", Version: 1,
		BlobRelPath: &blob, SourceSize: &sz, SourceMtime: &mtm,
	}}}
	sb, _ := json.Marshal(sc)
	if err := os.WriteFile(filepath.Join(derivDir, "manifest.json"), sb, 0o644); err != nil {
		t.Fatal(err)
	}
	if found, err := farm.ReconcileOneSidecar(dstore, tmp, inode); err != nil || !found {
		t.Fatalf("seed reconcile: found=%v err=%v", found, err)
	}

	globalMu.Lock()
	oldS, oldD, oldF := globalStore, globalDerivStore, globalFUSEPath
	globalStore, globalDerivStore, globalFUSEPath = mstore, dstore, tmp
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalStore, globalDerivStore, globalFUSEPath = oldS, oldD, oldF
		globalMu.Unlock()
	}()

	body, _ := json.Marshal(map[string]any{
		"inode": inode, "kind": "thumbnail", "producer": "on-device",
		"source_size": fi.Size(), "source_mtime": fi.ModTime().Unix(),
		"blob_rel_path": "poster.jpg",
	})
	rr := httptest.NewRecorder()
	handleDerivativesRegisterHTTP(rr, httptest.NewRequest("POST", "/derivatives/register", bytes.NewReader(body)))
	if rr.Code != 409 {
		t.Fatalf("consumer overwrote a genuine farm row: status %d (want 409), body %s",
			rr.Code, rr.Body.String())
	}
}

// TestSidecarFilmstripGeometryValidated: geometry comes out of the same
// attacker-writable file and readers DIVIDE by it. cols==0 is a divide-by-zero
// in any `i % cols`; absurd dimensions walk a scrubber into a huge allocation.
func TestSidecarFilmstripGeometryValidated(t *testing.T) {
	for _, tc := range []struct {
		name string
		geo  derivatives.FilmstripGeo
		ok   bool
	}{
		{"sane", derivatives.FilmstripGeo{FrameCount: 100, Cols: 10, Rows: 10, CellW: 160, CellH: 90, IntervalMS: 1000, DurationMS: 100000}, true},
		{"cols-zero-divide-by-zero", derivatives.FilmstripGeo{FrameCount: 100, Cols: 0, Rows: 10, CellW: 160, CellH: 90}, false},
		{"rows-negative", derivatives.FilmstripGeo{FrameCount: 100, Cols: 10, Rows: -1, CellW: 160, CellH: 90}, false},
		{"absurd-dimension", derivatives.FilmstripGeo{FrameCount: 1, Cols: 1 << 20, Rows: 1 << 20, CellW: 160, CellH: 90}, false},
		{"frames-exceed-grid", derivatives.FilmstripGeo{FrameCount: 999, Cols: 2, Rows: 2, CellW: 160, CellH: 90}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			const inode = 600003
			derivDir := filepath.Join(tmp, ".juicemount", "derivatives", "600003")
			if err := os.MkdirAll(derivDir, 0o755); err != nil {
				t.Fatal(err)
			}
			_ = os.WriteFile(filepath.Join(derivDir, "strip.jpg"), []byte("jpeg"), 0o644)
			blob := "strip.jpg"
			geo := tc.geo
			sc := farm.ManifestSidecar{Inode: inode, Derivatives: []derivatives.DerivRow{{
				Kind: "filmstrip", Status: "ready", Producer: "linux-farm", Version: 1,
				BlobRelPath: &blob, Filmstrip: &geo,
			}}}
			b, _ := json.Marshal(sc)
			if err := os.WriteFile(filepath.Join(derivDir, "manifest.json"), b, 0o644); err != nil {
				t.Fatal(err)
			}

			dstore, _ := derivatives.Open(":memory:")
			defer dstore.Close()
			if _, err := farm.ReconcileOneSidecar(dstore, tmp, inode); err != nil {
				t.Fatal(err)
			}
			rows, _ := dstore.Manifest(inode)
			var present bool
			for _, r := range rows {
				if r.Kind == "filmstrip" {
					present = true
				}
			}
			if present != tc.ok {
				t.Errorf("filmstrip row present = %v, want %v (geometry %+v)", present, tc.ok, tc.geo)
			}
		})
	}
}
