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

func TestProxyThreadArgs(t *testing.T) {
	defer SetProxyThreads(0)
	SetProxyThreads(0) // uncapped default — x264 auto (encode wants threads)
	if got := proxyThreadArgs(); got != nil {
		t.Fatalf("proxy default should be uncapped, got %v", got)
	}
	SetProxyThreads(8)
	if got := proxyThreadArgs(); len(got) != 2 || got[0] != "-threads" || got[1] != "8" {
		t.Fatalf("proxyThreadArgs(8) = %v", got)
	}
	// Proxy cap is INDEPENDENT of the derivative cap.
	SetFFmpegThreads(2)
	defer SetFFmpegThreads(0)
	if proxyThreadArgs()[1] != "8" || ffmpegThreadArgs()[1] != "2" {
		t.Fatal("proxy and derivative thread caps must be independent")
	}
}
