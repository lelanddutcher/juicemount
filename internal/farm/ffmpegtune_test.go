package farm

import (
	"strings"
	"testing"
)

func TestFFmpegThreadArgs(t *testing.T) {
	defer SetFFmpegThreads(0) // restore
	// Uncapped (default): no -threads emitted (preserves ffmpeg auto behavior + old tests).
	SetFFmpegThreads(0)
	if got := ffmpegThreadArgs(); got != nil {
		t.Fatalf("uncapped emitted args: %v", got)
	}
	// Negative clamps to 0.
	SetFFmpegThreads(-4)
	if FFmpegThreads() != 0 || ffmpegThreadArgs() != nil {
		t.Fatal("negative did not clamp to uncapped")
	}
	// Capped: emits -threads/-filter_threads/-filter_complex_threads = N.
	SetFFmpegThreads(3)
	args := ffmpegThreadArgs()
	joined := strings.Join(args, " ")
	for _, want := range []string{"-threads 3", "-filter_threads 3", "-filter_complex_threads 3"} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing %q in %q", want, joined)
		}
	}
	if FFmpegThreads() != 3 {
		t.Fatalf("FFmpegThreads()=%d want 3", FFmpegThreads())
	}
}
