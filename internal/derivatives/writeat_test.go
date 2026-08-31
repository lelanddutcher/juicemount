package derivatives

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// Two writers for the same asset+name must not destroy each other's update. A
// fixed temp name let the second unlink the first's temp mid-write, after which
// the first's rename failed and its data was simply gone.
func TestWriteFileAtConcurrentWritersBothSucceed(t *testing.T) {
	mount := t.TempDir()
	rel := DerivDirRel(880001)
	if err := EnsureDirUnder(mount, rel); err != nil {
		t.Fatal(err)
	}
	dir, err := OpenDirUnder(mount, rel)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = WriteFileAt(dir, "manifest.json",
				[]byte(fmt.Sprintf(`{"writer":%d}`, i)), 0o644)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("writer %d failed: %v", i, err)
		}
	}

	// The winner is whoever renamed last, but the file must be ONE writer's
	// complete content — never truncated, never absent.
	body, err := os.ReadFile(filepath.Join(mount, DerivBlobRel(880001, "manifest.json")))
	if err != nil {
		t.Fatalf("manifest missing after concurrent writes: %v", err)
	}
	if !strings.HasPrefix(string(body), `{"writer":`) || !strings.HasSuffix(string(body), "}") {
		t.Errorf("torn manifest: %q", body)
	}

	// No temp litter left behind.
	ents, _ := os.ReadDir(filepath.Join(mount, rel))
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), ".tmp-") {
			t.Errorf("temp file left behind: %s", e.Name())
		}
	}
}

// The staged name must keep the original extension LAST — ffmpeg picks its
// muxer from it, so ".stage-…-proxy.mp4" working is load-bearing, not cosmetic.
func TestStageNamePreservesExtension(t *testing.T) {
	mount := t.TempDir()
	rel := DerivDirRel(880002)
	if err := EnsureDirUnder(mount, rel); err != nil {
		t.Fatal(err)
	}
	dir, err := OpenDirUnder(mount, rel)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	for _, name := range []string{"proxy.mp4", "poster.jpg", "strip.jpg", "waveform.json"} {
		staged, abs, err := StageNameAt(dir, name)
		if err != nil {
			t.Fatal(err)
		}
		if filepath.Ext(staged) != filepath.Ext(name) {
			t.Errorf("staged %q lost the extension of %q — ffmpeg would pick the wrong muxer", staged, name)
		}
		if filepath.Ext(abs) != filepath.Ext(name) {
			t.Errorf("staged path %q lost the extension of %q", abs, name)
		}
		DiscardStagedAt(dir, staged)
	}
}

func TestStageReaderCopiesCompletedArtifactOnce(t *testing.T) {
	mount := t.TempDir()
	rel := DerivDirRel(880003)
	if err := EnsureDirUnder(mount, rel); err != nil {
		t.Fatal(err)
	}
	dir, err := OpenDirUnder(mount, rel)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	want := bytes.Repeat([]byte("completed-local-proxy"), 300000)
	staged, err := StageReaderAt(dir, "proxy.mp4", bytes.NewReader(want), 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := CommitStagedAt(dir, staged, "proxy.mp4"); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(mount, DerivBlobRel(880003, "proxy.mp4")))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("staged bytes differ: got %d bytes, want %d", len(got), len(want))
	}
}
