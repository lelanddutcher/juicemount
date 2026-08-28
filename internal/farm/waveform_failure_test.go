package farm

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

func TestProcessSurfacesNoDecodableWaveform(t *testing.T) {
	mount := t.TempDir()
	source := filepath.Join(mount, "undecodable-audio.mov")
	if err := os.WriteFile(source, []byte("stable media fixture"), 0o644); err != nil {
		t.Fatal(err)
	}

	binDir := t.TempDir()
	ffprobe := filepath.Join(binDir, "ffprobe")
	probeScript := `#!/bin/sh
case " $* " in
  *" -select_streams a "*) printf 'none\n' ;;
  *) printf '%s\n' '{"streams":[{"codec_type":"audio","codec_name":"none","channels":2,"sample_rate":"48000"}],"format":{"format_name":"mov","duration":"1.0","size":"20"}}' ;;
esac
`
	if err := os.WriteFile(ffprobe, []byte(probeScript), 0o755); err != nil {
		t.Fatal(err)
	}

	store, err := derivatives.Open(filepath.Join(t.TempDir(), "derivatives.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	res := Process(store, source, Options{
		Producer: "test", Version: 1, Mount: mount, Waveform: true,
		FFprobeBin: ffprobe, FFmpegBin: filepath.Join(binDir, "ffmpeg"),
	})
	if res.Err != nil {
		t.Fatalf("metadata publication failed: %v", res.Err)
	}
	if res.BlobErr == nil || !strings.Contains(res.BlobErr.Error(), "no decodable samples") {
		t.Fatalf("waveform failure was not surfaced: %v", res.BlobErr)
	}
	stats, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.ByKind["waveform"].Failed != 1 || stats.ByKind["waveform"].Ready != 0 {
		t.Fatalf("waveform stats = %+v, want failed=1 ready=0", stats.ByKind["waveform"])
	}
}
