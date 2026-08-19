package farm

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// The move-vs-edit discriminator.
//
// A job targets a DIRECTORY and the watcher enqueues one whenever media settles
// in it — including when a folder is merely moved or reorganised. Without this
// gate every already-derived file in that folder is re-postered,
// re-filmstripped and re-waveformed for content that has not changed a byte.
//
// Being wrong in the SKIP direction is the dangerous one: it leaves a stale
// derivative serving fresh content. So every case below that is not positively
// "same content, same size, ready rows" must regenerate.

func freshStore(t *testing.T) *derivatives.Store {
	t.Helper()
	s, err := derivatives.Open(filepath.Join(t.TempDir(), "d.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func seed(t *testing.T, s *derivatives.Store, inode uint64, hash string, size int64, status string) {
	t.Helper()
	if err := s.PutSource(inode, &hash); err != nil {
		t.Fatalf("PutSource: %v", err)
	}
	sz := size
	if err := s.PutDeriv(inode, derivatives.DerivRow{
		Kind: "thumbnail", Status: status, Producer: "linux-farm", Version: 1,
		Hash: &hash, SourceSize: &sz,
	}); err != nil {
		t.Fatalf("PutDeriv: %v", err)
	}
}

func TestMovedFileIsSkipped(t *testing.T) {
	s := freshStore(t)
	seed(t, s, 4242, "abc123", 1000, "ready")
	// A move: same inode, same content hash, same size — only the path differs.
	fresh, _ := skipIfFresh(s, "/jfs/NEW/LOCATION/clip.mov", 4242, "abc123", 1000, Options{})
	if !fresh {
		t.Error("a moved file was NOT skipped — it will be re-encoded, which is " +
			"exactly the wasted compute this gate exists to prevent")
	}
}

func TestEditedFileIsRegenerated(t *testing.T) {
	s := freshStore(t)
	seed(t, s, 4242, "abc123", 1000, "ready")
	// Content changed: the sampled hash moves.
	if fresh, _ := skipIfFresh(s, "/jfs/clip.mov", 4242, "DIFFERENT", 1000, Options{}); fresh {
		t.Error("an edited file was skipped — a stale derivative would keep serving " +
			"content it was not made from")
	}
	// Same hash but a different length: a sampled hash reads windows, not the
	// whole file, so size is the second half of the test.
	if fresh, _ := skipIfFresh(s, "/jfs/clip.mov", 4242, "abc123", 999, Options{}); fresh {
		t.Error("a size change was skipped — a sampled hash can collide across " +
			"lengths, which is why size is checked too")
	}
}

func TestUnknownOrIncompleteAssetsRegenerate(t *testing.T) {
	s := freshStore(t)
	// Never seen.
	if fresh, _ := skipIfFresh(s, "/jfs/new.mov", 777, "h", 10, Options{}); fresh {
		t.Error("an unknown inode was skipped")
	}
	// Known, but its only row is not ready — the derivative does not exist.
	seed(t, s, 555, "h2", 20, "failed")
	if fresh, _ := skipIfFresh(s, "/jfs/x.mov", 555, "h2", 20, Options{}); fresh {
		t.Error("an asset whose only row is FAILED was skipped — it has no usable " +
			"derivative, so skipping leaves it permanently without one")
	}
}

// A row with no stamped source size predates the read-gate and cannot vouch for
// what it was made from.
func TestUnstampedRowRegenerates(t *testing.T) {
	s := freshStore(t)
	h := "abc123"
	if err := s.PutSource(9001, &h); err != nil {
		t.Fatal(err)
	}
	if err := s.PutDeriv(9001, derivatives.DerivRow{
		Kind: "thumbnail", Status: "ready", Producer: "linux-farm", Version: 1, Hash: &h,
	}); err != nil {
		t.Fatal(err)
	}
	if fresh, _ := skipIfFresh(s, "/jfs/old.mov", 9001, h, 1000, Options{}); fresh {
		t.Error("a row with no stamped source_size was trusted — it cannot vouch " +
			"for the source it was generated from")
	}
}

// The skip must still index the asset. An asset generated before the JM-15 emit
// worked has blobs and rows but no manifest.json, and the generate path is the
// only other place one is written — so skipping without repairing would leave it
// invisible to the consumer forever.
func TestSkipRepairsAMissingManifest(t *testing.T) {
	s := freshStore(t)
	seed(t, s, 4242, "abc123", 1000, "ready")
	mount := t.TempDir()
	fresh, repaired := skipIfFresh(s, "/jfs/clip.mov", 4242, "abc123", 1000, Options{Mount: mount})
	if !fresh {
		t.Fatal("expected a skip")
	}
	if !repaired {
		t.Error("the skip did not write a manifest — the asset stays 'NOT indexed' " +
			"and the consumer never sees it, permanently")
	}
}

// The rows are not the data.
//
// skipIfFresh answered from the database alone until 2026-08-18. When the
// /jfs/.juicemount tree was wiped out of band on 2026-08-05 while the row
// database survived (it lives outside the volume), that made a one-off loss
// permanent: hash and size still matched, a row still said ready, so every
// wiped asset was skipped forever — and the manifest repair inside the skip
// then published a manifest advertising blobs that were gone. 35,623 of 67,970
// ready rows were affected.
func TestAbsentBlobIsNotFresh(t *testing.T) {
	s := freshStore(t)
	mount := t.TempDir()
	h := "abc123"
	if err := s.PutSource(4242, &h); err != nil {
		t.Fatal(err)
	}
	rel := "poster.jpg"
	sz := int64(1000)
	if err := s.PutDeriv(4242, derivatives.DerivRow{
		Kind: "thumbnail", Status: "ready", Producer: "linux-farm", Version: 1,
		Hash: &h, SourceSize: &sz, BlobRelPath: &rel,
	}); err != nil {
		t.Fatal(err)
	}
	// The row is immaculate. The blob is simply not on the volume.
	fresh, repaired := skipIfFresh(s, "/jfs/clip.mov", 4242, h, sz, Options{Mount: mount})
	if fresh {
		t.Error("an asset whose blob is ABSENT was reported fresh — generation is " +
			"skipped, and the skip then publishes a manifest for bytes that do not " +
			"exist, which is exactly the 52% of ready rows measured on 2026-08-18")
	}
	if repaired {
		t.Error("a manifest was written for an asset with no blob — that is how " +
			"inode 48899 ended up holding nothing but a manifest.json")
	}
}

// G2 must still hold: a MOVE changes the path, not the bytes. The blob is
// present, so the asset is fresh and must not be re-encoded.
func TestMovedFileWithBlobPresentStillSkips(t *testing.T) {
	s := freshStore(t)
	mount := t.TempDir()
	h := "abc123"
	if err := s.PutSource(4242, &h); err != nil {
		t.Fatal(err)
	}
	rel := "poster.jpg"
	sz := int64(1000)
	if err := s.PutDeriv(4242, derivatives.DerivRow{
		Kind: "thumbnail", Status: "ready", Producer: "linux-farm", Version: 1,
		Hash: &h, SourceSize: &sz, BlobRelPath: &rel,
	}); err != nil {
		t.Fatal(err)
	}
	// Materialise the blob where the row says it lives.
	dir := filepath.Join(mount, ".juicemount", "derivatives", "4242")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, rel), []byte("jpegbytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	fresh, _ := skipIfFresh(s, "/jfs/NEW/LOCATION/clip.mov", 4242, h, sz, Options{Mount: mount})
	if !fresh {
		t.Error("a MOVED file whose blob is present was re-encoded — the blob check " +
			"has broken the very case G2 exists to prevent")
	}
}

// Metadata-only rows carry no blob_rel_path. There is nothing to verify, and
// requiring a blob for them would re-derive every asset that has one.
func TestRowWithoutBlobPathDoesNotBlockFreshness(t *testing.T) {
	s := freshStore(t)
	mount := t.TempDir()
	h := "abc123"
	if err := s.PutSource(777, &h); err != nil {
		t.Fatal(err)
	}
	sz := int64(10)
	if err := s.PutDeriv(777, derivatives.DerivRow{
		Kind: "thumbnail", Status: "ready", Producer: "linux-farm", Version: 1,
		Hash: &h, SourceSize: &sz, // no BlobRelPath
	}); err != nil {
		t.Fatal(err)
	}
	if fresh, _ := skipIfFresh(s, "/jfs/x.mov", 777, h, sz, Options{Mount: mount}); !fresh {
		t.Error("a metadata-only row (no blob_rel_path) was treated as a missing " +
			"blob, which would re-derive every asset that has one")
	}
}
