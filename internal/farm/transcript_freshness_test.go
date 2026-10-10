package farm

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

func seedFreshTranscript(t *testing.T) (*derivatives.Store, string, string, uint64, string, int64) {
	t.Helper()
	mount := t.TempDir()
	source := filepath.Join(mount, "source.mp4")
	if err := os.WriteFile(source, []byte("stable source bytes for transcript freshness"), 0o644); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(source)
	if err != nil {
		t.Fatal(err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("source stat has no syscall.Stat_t")
	}
	inode := uint64(st.Ino)
	hash, err := SampleHash(source, info.Size())
	if err != nil {
		t.Fatal(err)
	}

	store := freshStore(t)
	if err := store.PutSource(inode, &hash); err != nil {
		t.Fatal(err)
	}
	rel := derivatives.AIBlobName
	mediaType := "application/json"
	model := "whisper.cpp/medium.en"
	size := info.Size()
	if err := store.PutDeriv(inode, derivatives.DerivRow{
		Kind: "ai", Status: "ready", Producer: "linux-farm", Version: 1,
		Hash: &hash, BlobRelPath: &rel, MediaType: &mediaType, Model: &model,
		SourceSize: &size,
	}); err != nil {
		t.Fatal(err)
	}
	doc := LoupeJSON{
		LoggerVersion: 1,
		SchemaVersion: "1.0",
		Media:         LoupeMedia{HashXXH3: hash},
		AI: &LoupeAI{Transcript: &LoupeTranscript{
			Language: "en", Model: model,
			Segments: []LoupeTranscriptSeg{{StartMs: 0, EndMs: 1000, Text: "hello"}},
		}},
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(mount, ".juicemount", "derivatives", fmt.Sprint(inode))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, rel), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return store, mount, source, inode, hash, size
}

func TestTranscriptFreshRequiresMatchingSourceModelAndBlob(t *testing.T) {
	store, mount, _, inode, hash, size := seedFreshTranscript(t)
	current := Options{Mount: mount, WhisperModel: "/models/ggml-medium.en.bin"}
	if !transcriptFresh(store, inode, hash, size, current) {
		t.Fatal("matching source, model and AI bundle should skip")
	}
	if transcriptFresh(store, inode, hash, size, Options{Mount: mount, WhisperModel: "/models/ggml-large-v3.bin"}) {
		t.Fatal("a different model must regenerate")
	}
	if transcriptFresh(store, inode, "edited-source", size, current) {
		t.Fatal("a changed source hash must regenerate")
	}
	if transcriptFresh(store, inode, hash, size+1, current) {
		t.Fatal("a changed source size must regenerate")
	}
	if transcriptFresh(store, inode, hash, size, Options{Mount: mount, WhisperModel: "/models/ggml-medium.en.bin", RegenerateFresh: true}) {
		t.Fatal("explicit regeneration must bypass transcript freshness")
	}
	if err := os.Remove(filepath.Join(mount, derivatives.DerivBlobRel(inode, derivatives.AIBlobName))); err != nil {
		t.Fatal(err)
	}
	if transcriptFresh(store, inode, hash, size, current) {
		t.Fatal("a ready row must not skip when the AI bundle is missing")
	}
}

func TestTranscriptFreshReconcilesAcrossWorkerStores(t *testing.T) {
	gpu, mount, _, inode, hash, size := seedFreshTranscript(t)
	if err := WriteManifestSidecar(gpu, mount, inode); err != nil {
		t.Fatal(err)
	}

	server := freshStore(t)
	if transcriptFreshIndexed(server, inode, hash, size, Options{Mount: mount, WhisperModel: "/models/ggml-medium.en.bin"}) {
		t.Fatal("the server private cache unexpectedly knew the GPU transcript")
	}
	if !transcriptFresh(server, inode, hash, size, Options{Mount: mount, WhisperModel: "/models/ggml-medium.en.bin"}) {
		t.Fatal("the server did not reconcile the shared GPU transcript")
	}
}

func TestGenerateTranscriptSkipsBeforeProbeWhenCurrent(t *testing.T) {
	store, mount, source, inode, _, _ := seedFreshTranscript(t)
	res := GenerateTranscript(store, source, Options{
		Mount: mount, WhisperModel: "/models/ggml-medium.en.bin",
		FFprobeBin: "/definitely/not/ffprobe", WhisperBin: "/definitely/not/whisper",
	})
	if res.Err != nil {
		t.Fatalf("current transcript reached media tools instead of skipping: %v", res.Err)
	}
	if res.Inode != inode || !res.SkippedFresh || res.HasSpeech {
		t.Fatalf("skip result = %+v, want inode=%d skipped=true", res, inode)
	}
}
