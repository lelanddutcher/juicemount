package farm

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// TestBuildQLArgs pins the QL-preview ffmpeg argument contract: bounded
// duration, aspect-preserving fit box, yuv420p decode floor, +faststart for
// byte-range playback, forced MP4 muxer (the staged output name has no .mp4
// extension to infer from).
func TestBuildQLArgs(t *testing.T) {
	args := buildQLArgs("/src/clip.braw", "/out/staged", 960, 20)
	joined := strings.Join(args, " ")
	for _, want := range []string{
		"-t 20",
		"-i /src/clip.braw",
		"scale=w=960:h=960:force_original_aspect_ratio=decrease",
		"-pix_fmt yuv420p",
		"-movflags +faststart",
		"-f mp4 /out/staged",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("args missing %q:\n%s", want, joined)
		}
	}
	// Defaults apply when unset.
	def := buildQLArgs("a", "b", 0, 0)
	if !strings.Contains(strings.Join(def, " "), "-t 20") ||
		!strings.Contains(strings.Join(def, " "), "scale=w=960:h=960") {
		t.Fatalf("defaults not applied: %v", def)
	}
}

// TestGenerateQLPreviewEndToEnd is the real proof (ffmpeg-gated): a generated
// test clip gets a playable MP4 blob + a READY kind="qlpreview" manifest row,
// and a second pass SKIPS instead of re-encoding.
func TestGenerateQLPreviewEndToEnd(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg not available")
	}

	dir := t.TempDir()
	src := filepath.Join(dir, "clip.mov")
	gen := exec.Command(ffmpeg, "-v", "error", "-y",
		"-f", "lavfi", "-i", "testsrc=size=320x240:rate=15:duration=3",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=3",
		"-c:v", "libx264", "-preset", "ultrafast", "-c:a", "aac", src)
	if out, err := gen.CombinedOutput(); err != nil {
		t.Fatalf("generate fixture: %v\n%s", err, out)
	}

	store, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	opt := Options{Producer: "macos-node", Version: 1, Mount: dir,
		FFmpegBin: ffmpeg, QLMaxDim: 320, QLSeconds: 2}

	res := GenerateQLPreview(store, src, opt)
	if res.Err != nil {
		t.Fatalf("GenerateQLPreview: %v", res.Err)
	}
	if !res.Wrote || res.SkippedFresh {
		t.Fatalf("first pass: wrote=%v skipped=%v, want wrote=true", res.Wrote, res.SkippedFresh)
	}

	// The blob exists at the reserved placement.
	blobRel := derivatives.DerivBlobRel(res.Inode, QLBlobName())
	fi, err := derivatives.StatRegularUnder(opt.Mount, blobRel)
	if err != nil {
		t.Fatalf("qlpreview blob missing: %v", err)
	}
	if fi.Size() == 0 {
		t.Fatal("qlpreview blob is empty")
	}

	rows, err := store.Manifest(res.Inode)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range rows {
		if d.Kind == QLPreviewKind && d.Status == "ready" &&
			d.BlobRelPath != nil && *d.BlobRelPath == QLBlobName() &&
			d.MediaType != nil && *d.MediaType == qlPreviewMedia &&
			d.SourceSize != nil && *d.SourceSize > 0 {
			found = true
		}
	}
	if !found {
		t.Fatalf("no ready qlpreview row with vouch in %+v", rows)
	}

	// Second pass on unchanged bytes must skip — sweeps re-run freely.
	res2 := GenerateQLPreview(store, src, opt)
	if res2.Err != nil {
		t.Fatalf("second pass: %v", res2.Err)
	}
	if !res2.SkippedFresh || res2.Wrote {
		t.Fatalf("second pass should skip: skipped=%v wrote=%v", res2.SkippedFresh, res2.Wrote)
	}
}

// TestQLPreviewRowSurvivesSidecarSanitize proves the new kind round-trips the
// JM-15 reconcile path: without the blobMediaTypes/reservedBlobName entries,
// sanitizeSidecarRow would reject every genuine farm row and DR would drop it.
// Also guards the never-round-trips trap: fields set here must survive exactly
// as stored, or the reconcile re-publishes the row forever.
func TestQLPreviewRowSurvivesSidecarSanitize(t *testing.T) {
	hash := "0123456789abcdef"
	rel := QLBlobName()
	mt := qlPreviewMedia
	size := int64(12345)
	mtime := time.Now().Unix()
	row := derivatives.DerivRow{
		Kind: QLPreviewKind, Status: "ready", Producer: "linux-farm", Version: 1,
		Hash: &hash, BlobRelPath: &rel, MediaType: &mt,
		SourceSize: &size, SourceMtime: &mtime,
	}
	clean, ok := sanitizeSidecarRow(row)
	if !ok {
		t.Fatal("sanitizer rejected a genuine qlpreview row")
	}
	if clean.MediaType == nil || *clean.MediaType != qlPreviewMedia {
		t.Fatalf("media type not pinned: %+v", clean.MediaType)
	}
	if clean.BlobRelPath == nil || *clean.BlobRelPath != QLBlobName() {
		t.Fatalf("blob path mangled: %+v", clean.BlobRelPath)
	}
	// A forged path/name under our kind must be rejected outright.
	bad := row
	wrong := "evil.mp4"
	bad.BlobRelPath = &wrong
	if _, ok := sanitizeSidecarRow(bad); ok {
		t.Fatal("sanitizer accepted a foreign blob name on kind qlpreview")
	}
}

// TestExpectedKindsPosterAlways pins the T1.1 "poster always" mirror: with the
// policy on, a video below MinBlobSizeBytes still expects (and so still
// generates) a poster while filmstrip/waveform keep the size gate. The
// freshness gate depends on this list matching Process's gates EXACTLY —
// drift either way makes assets permanently unskippable or silently stale.
func TestExpectedKindsPosterAlways(t *testing.T) {
	tech := &Tech{Video: &VideoTrack{Width: 1920, Height: 1080}, Audio: []AudioTrack{{Codec: "pcm_s16le"}}}
	small := int64(5 << 20) // below a 20MB floor
	opt := Options{Blobs: true, Filmstrip: true, Waveform: true, MinBlobSizeBytes: 20 << 20}

	got := expectedKinds(tech, small, opt)
	for _, k := range got {
		if k == "thumbnail" {
			t.Fatal("small asset expected a poster without PosterAlways")
		}
	}

	opt.PosterAlways = true
	got = expectedKinds(tech, small, opt)
	has := map[string]bool{}
	for _, k := range got {
		has[k] = true
	}
	if !has["thumbnail"] {
		t.Fatalf("PosterAlways must expect a poster: %v", got)
	}
	if has["filmstrip"] || has["waveform"] {
		t.Fatalf("PosterAlways must NOT lift the gate for other blobs: %v", got)
	}
}
