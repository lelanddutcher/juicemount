package farm

import (
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// TestFoldArgsForCount pins the stream-count → ffmpeg-args mapping (no media file).
func TestFoldArgsForCount(t *testing.T) {
	if _, ok := foldArgsForCount(0, 16000); ok {
		t.Fatal("0 streams must be ok=false (no audio)")
	}
	one, ok := foldArgsForCount(1, 16000)
	if !ok || strings.Join(one, " ") != "-map 0:a:0 -ac 1 -ar 16000" {
		t.Fatalf("1 stream args wrong: %v", one)
	}
	multi, ok := foldArgsForCount(4, 48000)
	if !ok {
		t.Fatal("4 streams must be ok")
	}
	joined := strings.Join(multi, " ")
	// Every stream gets a per-input mono cast (the anti-"No channel layout" fix),
	// all four merge, output forced mono at the requested rate.
	for i := 0; i < 4; i++ {
		if !strings.Contains(joined, "[0:a:"+strconv.Itoa(i)+"]aformat=channel_layouts=mono[a"+strconv.Itoa(i)+"]") {
			t.Fatalf("missing per-input mono cast for stream %d: %s", i, joined)
		}
	}
	if !strings.Contains(joined, "amerge=inputs=4[m]") || !strings.Contains(joined, "-map [m] -ac 1 -ar 48000") {
		t.Fatalf("4-stream merge args wrong: %s", joined)
	}
}

// TestAudioFoldDownMergesAllStreams is the real proof of the silent-data-loss
// fix: a 2-stream file whose FIRST stream is silent and SECOND carries a tone
// (mimicking a dead-first-mic camera clip). The old -map 0:a:0 would extract
// silence; the fold-down amerge must surface the tone. Skips without ffmpeg.
func TestAudioFoldDownMergesAllStreams(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg not available")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "two.mkv")
	gen := exec.Command(ffmpeg, "-v", "error", "-y",
		"-f", "lavfi", "-i", "anullsrc=r=48000:cl=mono",
		"-f", "lavfi", "-i", "sine=frequency=440:r=48000",
		"-map", "0:a", "-map", "1:a", "-t", "1", "-c:a", "pcm_s24le", src)
	if out, err := gen.CombinedOutput(); err != nil {
		t.Fatalf("generate 2-stream fixture: %v\n%s", err, out)
	}

	args, ok, err := audioFoldArgs(ffmpeg, src, 16000)
	if err != nil || !ok {
		t.Fatalf("audioFoldArgs: ok=%v err=%v", ok, err)
	}
	if !strings.Contains(strings.Join(args, " "), "amerge=inputs=2") {
		t.Fatalf("expected 2-stream amerge, got %v", args)
	}

	out := filepath.Join(dir, "fold.wav")
	full := append([]string{"-v", "error", "-y", "-i", src}, args...)
	full = append(full, "-c:a", "pcm_s16le", out)
	if o, err := exec.Command(ffmpeg, full...).CombinedOutput(); err != nil {
		t.Fatalf("fold-down run failed: %v\n%s", err, o)
	}

	mean := meanVolumeDB(t, ffmpeg, out)
	if mean < -60 {
		t.Fatalf("folded output is silent (mean_volume %.1f dB) — the tone stream was dropped (the bug)", mean)
	}
	t.Logf("fold-down mean_volume %.1f dB (tone from stream 1 survived)", mean)
}

var meanVolRe = regexp.MustCompile(`mean_volume:\s*(-?[0-9.]+) dB`)

func meanVolumeDB(t *testing.T, ffmpeg, path string) float64 {
	t.Helper()
	det := exec.Command(ffmpeg, "-v", "info", "-i", path, "-af", "volumedetect", "-f", "null", "-")
	out, _ := det.CombinedOutput()
	m := meanVolRe.FindStringSubmatch(string(out))
	if m == nil {
		t.Fatalf("no mean_volume in volumedetect output:\n%s", out)
	}
	v, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		t.Fatalf("parse mean_volume %q: %v", m[1], err)
	}
	return v
}
