package farm

import (
	"encoding/json"
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

// seedTech writes the `tech` metadata row that skipIfFresh reads to work out
// which derivative kinds this asset ought to have.
func seedTech(t *testing.T, s *derivatives.Store, inode uint64, hash, payload string) {
	t.Helper()
	if err := s.PutMetadata(inode, "tech", "linux-farm", 1, &hash, json.RawMessage(payload)); err != nil {
		t.Fatalf("PutMetadata tech: %v", err)
	}
}

func seed(t *testing.T, s *derivatives.Store, inode uint64, hash string, size int64, status string) {
	t.Helper()
	if err := s.PutSource(inode, &hash); err != nil {
		t.Fatalf("PutSource: %v", err)
	}
	sz := size
	// A real farm-derived asset ALWAYS carries `tech` metadata and a `tech` row:
	// on 2026-08-19 the live farm database held 1,801 assets and 0 derivative
	// rows whose inode lacked either. Seeding them is what makes this fixture an
	// asset the farm could actually have produced, and skipIfFresh now reads the
	// metadata to learn which kinds the asset OUGHT to have.
	//
	// The payload declares video and no audio, so a waveform is not expected of
	// it — the tests that care about audio seed their own tech.
	seedTech(t, s, inode, hash, `{"container":"mov","video":{"codec":"h264"},"audio":[]}`)
	if err := s.PutDeriv(inode, derivatives.DerivRow{
		Kind: "tech", Status: status, Producer: "linux-farm", Version: 1,
		Hash: &hash, SourceSize: &sz,
	}); err != nil {
		t.Fatalf("PutDeriv tech: %v", err)
	}
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
	seedTech(t, s, 4242, h, `{"container":"mov","video":{"codec":"h264"},"audio":[]}`)
	if err := s.PutDeriv(4242, derivatives.DerivRow{
		Kind: "tech", Status: "ready", Producer: "linux-farm", Version: 1,
		Hash: &h, SourceSize: &sz,
	}); err != nil {
		t.Fatal(err)
	}
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
	seedTech(t, s, 4242, h, `{"container":"mov","video":{"codec":"h264"},"audio":[]}`)
	if err := s.PutDeriv(4242, derivatives.DerivRow{
		Kind: "tech", Status: "ready", Producer: "linux-farm", Version: 1,
		Hash: &h, SourceSize: &sz,
	}); err != nil {
		t.Fatal(err)
	}
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
	seedTech(t, s, 777, h, `{"container":"mov","video":{"codec":"h264"},"audio":[]}`)
	if err := s.PutDeriv(777, derivatives.DerivRow{
		Kind: "tech", Status: "ready", Producer: "linux-farm", Version: 1,
		Hash: &h, SourceSize: &sz,
	}); err != nil {
		t.Fatal(err)
	}
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

// THE MISSING-KIND GAP.
//
// Every check above this point is satisfied by a SINGLE ready row, so an asset
// carrying poster and filmstrip but no waveform read "fresh" and its waveform
// was never generated — the skip being precisely what stopped it. On 2026-08-19
// that described 1,305 of the farm's 1,801 assets: ffmpeg 4.3.9 could not decode
// the camera's `ipcm` audio, published no row at all, and every later sweep
// skipped them. The ffmpeg 7.0.2 upgrade fixed the decode and recovered none of
// them until this gate learned to ask for COMPLETENESS.

func TestAssetMissingAnExpectedKindIsNotFresh(t *testing.T) {
	s := freshStore(t)
	hash, size := "abc123", int64(1000)
	seed(t, s, 4242, hash, size, "ready")
	// This source HAS audio, so a waveform run owes it a waveform row.
	seedTech(t, s, 4242, hash, `{"container":"mov","video":{"codec":"h264"},"audio":[{"codec":"ipcm"}]}`)
	sz := size
	if err := s.PutDeriv(4242, derivatives.DerivRow{
		Kind: "filmstrip", Status: "ready", Producer: "linux-farm", Version: 1,
		Hash: &hash, SourceSize: &sz,
	}); err != nil {
		t.Fatalf("PutDeriv: %v", err)
	}
	opt := Options{Blobs: true, Filmstrip: true, Waveform: true}
	if fresh, _ := skipIfFresh(s, "/jfs/a/clip.mov", 4242, hash, size, opt); fresh {
		t.Error("an asset with poster+filmstrip and NO waveform row read as fresh — " +
			"this is the skip that stranded 1,305 waveforms behind the ffmpeg upgrade")
	}
}

func TestAssetWithEveryExpectedKindIsFresh(t *testing.T) {
	s := freshStore(t)
	hash, size := "abc123", int64(1000)
	seed(t, s, 4242, hash, size, "ready")
	seedTech(t, s, 4242, hash, `{"container":"mov","video":{"codec":"h264"},"audio":[{"codec":"ipcm"}]}`)
	sz := size
	for _, k := range []string{"filmstrip", "waveform"} {
		if err := s.PutDeriv(4242, derivatives.DerivRow{
			Kind: k, Status: "ready", Producer: "linux-farm", Version: 1,
			Hash: &hash, SourceSize: &sz,
		}); err != nil {
			t.Fatalf("PutDeriv %s: %v", k, err)
		}
	}
	opt := Options{Blobs: true, Filmstrip: true, Waveform: true}
	if fresh, _ := skipIfFresh(s, "/jfs/a/clip.mov", 4242, hash, size, opt); !fresh {
		t.Error("a COMPLETE asset was re-encoded — completeness must not break the " +
			"moved-file skip this gate exists for")
	}
}

// A kind a contributor supplied counts. The farm is not the only producer, and
// re-deriving a poster ClipLogger already made on hardware-accelerated silicon
// is exactly the waste this gate prevents.
func TestContributorRowSatisfiesItsKind(t *testing.T) {
	s := freshStore(t)
	hash, size := "abc123", int64(1000)
	seed(t, s, 4242, hash, size, "ready")
	seedTech(t, s, 4242, hash, `{"container":"mov","video":{"codec":"h264"},"audio":[]}`)
	sz := size
	if err := s.PutDeriv(4242, derivatives.DerivRow{
		Kind: "filmstrip", Status: "ready", Producer: "on-device", Version: 1,
		Hash: &hash, SourceSize: &sz,
	}); err != nil {
		t.Fatalf("PutDeriv: %v", err)
	}
	opt := Options{Blobs: true, Filmstrip: true, Waveform: true}
	if fresh, _ := skipIfFresh(s, "/jfs/a/clip.mov", 4242, hash, size, opt); !fresh {
		t.Error("a contributor-produced filmstrip did not satisfy the filmstrip kind")
	}
}

// A source with no audio is not owed a waveform, or it could never be skipped.
func TestSilentSourceIsNotOwedAWaveform(t *testing.T) {
	s := freshStore(t)
	hash, size := "abc123", int64(1000)
	seed(t, s, 4242, hash, size, "ready")
	sz := size
	if err := s.PutDeriv(4242, derivatives.DerivRow{
		Kind: "filmstrip", Status: "ready", Producer: "linux-farm", Version: 1,
		Hash: &hash, SourceSize: &sz,
	}); err != nil {
		t.Fatalf("PutDeriv: %v", err)
	}
	opt := Options{Blobs: true, Filmstrip: true, Waveform: true}
	if fresh, _ := skipIfFresh(s, "/jfs/a/clip.mov", 4242, hash, size, opt); !fresh {
		t.Error("a silent source was re-encoded for a waveform it can never have — " +
			"that is an asset that regenerates on every sweep, forever")
	}
}

// A failed row means "this artifact will never appear", so it accounts for its
// kind. Treating it as unsatisfied would re-decode a permanently broken source
// on every sweep — and it would also CHANGE today's behaviour, where a failure
// alongside a ready row already skips.
func TestFailedRowAccountsForItsKind(t *testing.T) {
	s := freshStore(t)
	hash, size := "abc123", int64(1000)
	seed(t, s, 4242, hash, size, "ready")
	seedTech(t, s, 4242, hash, `{"container":"mov","video":{"codec":"h264"},"audio":[{"codec":"ipcm"}]}`)
	sz := size
	if err := s.PutDeriv(4242, derivatives.DerivRow{
		Kind: "filmstrip", Status: "ready", Producer: "linux-farm", Version: 1,
		Hash: &hash, SourceSize: &sz,
	}); err != nil {
		t.Fatalf("PutDeriv: %v", err)
	}
	if err := s.PutDeriv(4242, derivatives.DerivRow{
		Kind: "waveform", Status: "failed", Producer: "linux-farm", Version: 1, Hash: &hash,
	}); err != nil {
		t.Fatalf("PutDeriv: %v", err)
	}
	opt := Options{Producer: "linux-farm", Version: 1, Blobs: true, Filmstrip: true, Waveform: true}
	if fresh, _ := skipIfFresh(s, "/jfs/a/clip.mov", 4242, hash, size, opt); !fresh {
		t.Error("a failed waveform did not account for its kind — the source will be " +
			"re-decoded on every sweep for an artifact that can never appear")
	}
}

// A failure is a negative capability result from one exact producer build, not
// a fact about the media forever. Codec/runtime upgrades must get one repair
// attempt; otherwise old failures remain stranded even after the source becomes
// decodable.
func TestFailedRowFromOlderProducerGenerationIsRetried(t *testing.T) {
	s := freshStore(t)
	hash, size := "abc123", int64(1000)
	seed(t, s, 4242, hash, size, "ready")
	seedTech(t, s, 4242, hash, `{"container":"mov","video":{"codec":"h264"},"audio":[{"codec":"ipcm"}]}`)
	sz := size
	if err := s.PutDeriv(4242, derivatives.DerivRow{
		Kind: "filmstrip", Status: "ready", Producer: "linux-farm", Version: 1,
		Hash: &hash, SourceSize: &sz,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutDeriv(4242, derivatives.DerivRow{
		Kind: "waveform", Status: "failed", Producer: "linux-farm", Version: 1,
		Hash: &hash, SourceSize: &sz,
	}); err != nil {
		t.Fatal(err)
	}
	current := Options{Producer: "linux-farm", Version: 2, Blobs: true, Filmstrip: true, Waveform: true}
	if fresh, _ := skipIfFresh(s, "/jfs/a/clip.mov", 4242, hash, size, current); fresh {
		t.Fatal("an older producer generation's failure suppressed the current build's repair attempt")
	}
	otherProducer := current
	otherProducer.Version = 1
	otherProducer.Producer = "macos-node"
	if fresh, _ := skipIfFresh(s, "/jfs/a/clip.mov", 4242, hash, size, otherProducer); fresh {
		t.Fatal("another producer's failure suppressed this producer's repair attempt")
	}
}

// Without `tech` we cannot know which kinds are owed, and skipIfFresh resolves
// every uncertainty toward doing the work.
func TestMissingTechMetadataIsNotFresh(t *testing.T) {
	s := freshStore(t)
	hash, size := "abc123", int64(1000)
	seed(t, s, 4242, hash, size, "ready")
	if err := s.PutMetadata(4242, "tech", "linux-farm", 1, &hash, json.RawMessage(`null`)); err == nil {
		// Overwrite with an unparseable payload: same unknowable answer.
		if err := s.PutMetadata(4242, "tech", "linux-farm", 1, &hash, json.RawMessage(`not json`)); err != nil {
			t.Fatalf("PutMetadata: %v", err)
		}
	}
	if fresh, _ := skipIfFresh(s, "/jfs/a/clip.mov", 4242, hash, size, Options{}); fresh {
		t.Error("an asset whose tech metadata cannot be read was declared fresh")
	}
}

// The blob size gate skips the decode-heavy kinds, so a sub-threshold clip is
// owed nothing but its tech row and must still skip.
func TestSubThresholdClipIsOwedOnlyTech(t *testing.T) {
	s := freshStore(t)
	hash, size := "abc123", int64(1000)
	seed(t, s, 4242, hash, size, "ready")
	seedTech(t, s, 4242, hash, `{"container":"mov","video":{"codec":"h264"},"audio":[{"codec":"ipcm"}]}`)
	opt := Options{Blobs: true, Filmstrip: true, Waveform: true, MinBlobSizeBytes: 20 << 20}
	if fresh, _ := skipIfFresh(s, "/jfs/a/clip.mov", 4242, hash, size, opt); !fresh {
		t.Error("a sub-threshold clip was re-encoded for blobs its own size gate forbids")
	}
}
