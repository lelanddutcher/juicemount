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
	"github.com/lelanddutcher/juicemount/metadata"
)

// A resolvable synthetic inode must SUCCEED, with the blob moved under the real
// inode — not be bounced back to the consumer.
//
// WHY. A derivative blob is addressed by inode, and a consumer that stats a file
// before its write has drained sees a synthetic inode, so it writes the blob
// under a directory whose name will cease to exist. For a file created OFFLINE
// the synthetic inode is held INDEFINITELY, so this was not a race but a
// permanent wall: that whole class of file could never contribute. The consumer
// reported (contract f6f5a5b) that it has not implemented the real_inode
// remedy, and it cannot implement the move itself — WRITE_PLACEMENT §2 forbids
// it from naming paths inside our namespace. So the server resolves and moves.
func TestRegisterResolvesSyntheticInodeAndMovesBlob(t *testing.T) {
	tmp := t.TempDir()
	const relPath = "REEL/offline.mov"
	const synthetic = uint64(1<<63 + 77)
	const real = uint64(1_600_123)

	src := filepath.Join(tmp, relPath)
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, bytes.Repeat([]byte("m"), 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	mt := time.Unix(1700000900, 0)
	_ = os.Chtimes(src, mt, mt)

	ms, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()
	ds, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	// The REAL entry is what the path resolves to (post-reconcile), while the
	// consumer still holds the synthetic id it stat'd earlier.
	ms.InsertToCache(&metadata.Entry{
		Path: relPath, Name: "offline.mov", ParentPath: "REEL",
		Inode: real, Size: 4096, Mtime: mt,
	})
	ms.RecordSyntheticHandle(synthetic, relPath)

	// The consumer wrote its blob under the SYNTHETIC directory, as it must have.
	blob := []byte("JPEGBYTES-from-the-consumer")
	writeBlob(t, tmp, synthetic, "poster.jpg", blob)

	globalMu.Lock()
	oldS, oldD, oldF := globalStore, globalDerivStore, globalFUSEPath
	globalStore, globalDerivStore, globalFUSEPath = ms, ds, tmp
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalStore, globalDerivStore, globalFUSEPath = oldS, oldD, oldF
		globalMu.Unlock()
	}()

	b, _ := json.Marshal(regBody(synthetic, "thumbnail", 4096, mt.Unix()))
	rr := httptest.NewRecorder()
	handleDerivativesRegisterHTTP(rr, httptest.NewRequest("POST", "/derivatives/register", bytes.NewReader(b)))

	if rr.Code != 200 {
		t.Fatalf("status %d, want 200 — a RESOLVABLE synthetic inode must succeed, or an "+
			"offline-created file can never contribute at all; body %s", rr.Code, rr.Body.String())
	}

	// The row must be keyed to the REAL inode, and there must be no row under
	// the synthetic one (that is the orphan this whole path exists to avoid).
	rows, err := ds.Manifest(real)
	if err != nil || len(rows) != 1 || rows[0].Kind != "thumbnail" {
		t.Fatalf("expected one thumbnail row under the real inode %d, got %v (err %v)", real, rows, err)
	}
	if orphan, _ := ds.Manifest(synthetic); len(orphan) != 0 {
		t.Errorf("%d row(s) were left under the synthetic inode %d", len(orphan), synthetic)
	}

	// The BYTES must have moved, not been copied or lost.
	dstPath := filepath.Join(tmp, ".juicemount", "derivatives", itoa(real), "poster.jpg")
	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("blob not present under the real inode: %v — resolving the inode without "+
			"moving the bytes just produces a row pointing at nothing", err)
	}
	if !bytes.Equal(got, blob) {
		t.Errorf("blob content changed during the move")
	}
	srcPath := filepath.Join(tmp, ".juicemount", "derivatives", itoa(synthetic), "poster.jpg")
	if _, err := os.Stat(srcPath); err == nil {
		t.Error("blob still present under the SYNTHETIC inode — it was copied, not moved, " +
			"leaving bytes stranded under a name that ceases to exist")
	}

	// The response must report the inode the row actually landed under, or the
	// consumer records the wrong key for its own artifact.
	var resp struct {
		Inode uint64 `json:"inode"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Inode != real {
		t.Errorf("response inode = %d, want the resolved real inode %d", resp.Inode, real)
	}
}

// The move must never CLOBBER a destination blob — one may already back a
// registered row, and overwriting it would destroy that row's bytes.
func TestRelocateRefusesToClobberExistingBlob(t *testing.T) {
	tmp := t.TempDir()
	const synthetic = uint64(1<<63 + 78)
	const real = uint64(1_600_124)

	writeBlob(t, tmp, synthetic, "poster.jpg", []byte("NEW-contribution"))
	writeBlob(t, tmp, real, "poster.jpg", []byte("EXISTING-and-registered"))

	err := relocateDerivBlob(tmp, synthetic, real, "poster.jpg")
	if err == nil {
		t.Fatal("the move overwrote an existing destination blob — that blob may already " +
			"back a manifest row, whose bytes would be destroyed under it")
	}
	kept, rerr := os.ReadFile(filepath.Join(tmp, ".juicemount", "derivatives", itoa(real), "poster.jpg"))
	if rerr != nil || string(kept) != "EXISTING-and-registered" {
		t.Errorf("destination blob was damaged: %q (err %v)", kept, rerr)
	}
}

// An EMPTY source must not be moved: publishing a 0-byte blob under the real
// inode would be served with 200 and look like a real answer, instead of 404ing
// so the reader regenerates.
func TestRelocateRefusesEmptySource(t *testing.T) {
	tmp := t.TempDir()
	const synthetic = uint64(1<<63 + 79)
	const real = uint64(1_600_125)
	writeBlob(t, tmp, synthetic, "poster.jpg", nil)

	if err := relocateDerivBlob(tmp, synthetic, real, "poster.jpg"); err == nil {
		t.Fatal("an empty blob was moved and would be published as a real answer")
	}
	if _, err := os.Stat(filepath.Join(tmp, ".juicemount", "derivatives", itoa(real), "poster.jpg")); err == nil {
		t.Error("empty blob landed at the destination anyway")
	}
}
