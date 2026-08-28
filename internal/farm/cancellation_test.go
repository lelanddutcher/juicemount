package farm

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

func TestGeneratorsHonorPreCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	opt := Options{Context: ctx}

	checks := []struct {
		name string
		err  error
	}{
		{name: "derivatives", err: Process(nil, "/does/not/exist.mov", opt).Err},
		{name: "proxy", err: GenerateProxy(nil, "/does/not/exist.mov", opt).Err},
		{name: "transcript", err: GenerateTranscript(nil, "/does/not/exist.mov", opt).Err},
		{name: "qlpreview", err: GenerateQLPreview(nil, "/does/not/exist.mov", opt).Err},
	}
	for _, check := range checks {
		if !errors.Is(check.err, context.Canceled) {
			t.Errorf("%s error = %v, want context.Canceled", check.name, check.err)
		}
	}
}

func TestGenerateProxyCancellationDoesNotPublishFailure(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.mov")
	if err := os.WriteFile(source, make([]byte, 64*1024), 0o644); err != nil {
		t.Fatal(err)
	}
	probe := filepath.Join(dir, "ffprobe")
	probeScript := `#!/bin/sh
echo '{"streams":[{"codec_type":"video","codec_name":"prores","width":1920,"height":1080,"r_frame_rate":"24/1"}],"format":{"format_name":"mov","duration":"10","size":"65536"}}'
`
	if err := os.WriteFile(probe, []byte(probeScript), 0o755); err != nil {
		t.Fatal(err)
	}
	started := filepath.Join(dir, "ffmpeg.started")
	ffmpeg := filepath.Join(dir, "ffmpeg")
	// Replace the shell with a single blocking process after publishing the
	// start marker. The former busy loop could starve its own 2-second observer
	// when `go test ./...` ran all packages under release-build load, producing a
	// false cancellation failure before the test ever called cancel.
	ffmpegScript := "#!/bin/sh\necho started > " + started + "\nexec tail -f /dev/null\n"
	if err := os.WriteFile(ffmpeg, []byte(ffmpegScript), 0o755); err != nil {
		t.Fatal(err)
	}
	store, err := derivatives.Open(filepath.Join(dir, "derivatives.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan ProxyResult, 1)
	go func() {
		done <- GenerateProxy(store, source, Options{
			Context: ctx, Mount: dir, FFprobeBin: probe, FFmpegBin: ffmpeg,
			Producer: "test", Version: 1,
		})
	}()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(started); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(started); err != nil {
		t.Fatal("fake proxy encoder did not start")
	}
	cancel()

	var result ProxyResult
	select {
	case result = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("GenerateProxy did not stop after cancellation")
	}
	if !errors.Is(result.Err, context.Canceled) {
		t.Fatalf("GenerateProxy error = %v, want context.Canceled", result.Err)
	}
	rows, err := store.Manifest(result.Inode)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Kind == "proxy" {
			t.Fatalf("cancelled proxy published derivative row: %+v", row)
		}
	}
	blobDir := filepath.Join(dir, derivatives.DerivDirRel(result.Inode))
	entries, err := os.ReadDir(blobDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("cancelled proxy left staged/final files: %v", entries)
	}
}
