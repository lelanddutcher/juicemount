package farm

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// D5/D2 density ask (CONSUMER_STATUS 2026-07-21) + the short-clip frame floor
// (BACKLOG). These pin the DEFAULTS, which is the whole substance of the ask —
// the geometry shape was already correct.

func TestShortClipFrameFloor(t *testing.T) {
	cases := []struct {
		name   string
		durSec float64
		fps    float64
		want   int
	}{
		// The banding case the consumer reported: a short clip previously got a
		// flat 12 frames and repeated each one 3-6x across a wide strip.
		{"3s at 30fps has 90 frames available, so floor lifts to 32", 3, 30, 32},
		{"10s at 25fps", 10, 25, 32},
		// Never ask for more distinct frames than the clip contains.
		{"1s at 24fps can only offer ~24", 1, 24, 24},
		{"0.5s at 24fps -> 12 available, keeps the historical minimum", 0.5, 24, 12},
		// Unknown fps must not regress below the old behaviour.
		{"unknown fps keeps the flat floor", 3, 0, 12},
		// Long clips are unaffected by the floor and still cap at 144.
		{"120s clip is driven by duration, not the floor", 120, 30, 120},
		{"600s clip caps at 144", 600, 30, 144},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := frameTarget(tc.durSec, tc.fps); got != tc.want {
				t.Errorf("frameTarget(%.2fs, %.0ffps) = %d, want %d", tc.durSec, tc.fps, got, tc.want)
			}
		})
	}
}

func TestFilmstripFallsBackWhenKeyframePassProducesEmptyOutput(t *testing.T) {
	dir := t.TempDir()
	ffmpeg := filepath.Join(dir, "fake-ffmpeg")
	logPath := filepath.Join(dir, "calls.log")
	t.Setenv("JM_FILMSTRIP_TEST_LOG", logPath)
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$JM_FILMSTRIP_TEST_LOG"
out=""
for arg in "$@"; do out="$arg"; done
case " $* " in
  *" -discard nokey "*) : > "$out"; exit 0 ;;
esac
printf 'jpeg-bytes' > "$out"
`
	if err := os.WriteFile(ffmpeg, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	outPath := filepath.Join(dir, "strip.jpg")
	geo, err := Filmstrip(ffmpeg, filepath.Join(dir, "short.mp4"), outPath, 8022, 1920, 1080, 320, 30)
	if err != nil {
		t.Fatalf("Filmstrip fallback: %v", err)
	}
	if geo == nil || geo.FrameCount != 36 || geo.Cols != 12 || geo.Rows != 3 {
		t.Fatalf("unexpected geometry: %+v", geo)
	}
	blob, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(blob) != "jpeg-bytes" {
		t.Fatalf("fallback output = %q", blob)
	}
	calls, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(calls)), "\n")
	if len(lines) != 2 {
		t.Fatalf("ffmpeg calls = %d, want fast pass + fallback: %q", len(lines), calls)
	}
	if !strings.Contains(lines[0], "-discard nokey") {
		t.Fatalf("first pass lost keyframe optimization: %q", lines[0])
	}
	if strings.Contains(lines[1], "-discard nokey") {
		t.Fatalf("fallback still discarded non-keyframes: %q", lines[1])
	}
}

func TestFilmstripKeepsNonemptyKeyframeFastPath(t *testing.T) {
	dir := t.TempDir()
	ffmpeg := filepath.Join(dir, "fake-ffmpeg")
	logPath := filepath.Join(dir, "calls.log")
	t.Setenv("JM_FILMSTRIP_TEST_LOG", logPath)
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$JM_FILMSTRIP_TEST_LOG"
out=""
for arg in "$@"; do out="$arg"; done
printf 'fast-jpeg' > "$out"
`
	if err := os.WriteFile(ffmpeg, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	outPath := filepath.Join(dir, "strip.jpg")
	if _, err := Filmstrip(ffmpeg, filepath.Join(dir, "normal.mp4"), outPath, 60_000, 1920, 1080, 320, 30); err != nil {
		t.Fatalf("Filmstrip fast path: %v", err)
	}
	calls, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(calls)), "\n")
	if len(lines) != 1 {
		t.Fatalf("nonempty fast path invoked ffmpeg %d times, want 1: %q", len(lines), calls)
	}
	if !strings.Contains(lines[0], "-discard nokey") {
		t.Fatalf("fast path lost keyframe optimization: %q", lines[0])
	}
}

func TestFilmstripHardwareFallbackNeverDropsDecoder(t *testing.T) {
	dir := t.TempDir()
	ffmpeg := filepath.Join(dir, "fake-ffmpeg")
	logPath := filepath.Join(dir, "calls.log")
	t.Setenv("JM_FILMSTRIP_TEST_LOG", logPath)
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$JM_FILMSTRIP_TEST_LOG"
out=""
for arg in "$@"; do out="$arg"; done
case " $* " in
  *" -discard nokey "*) : > "$out"; exit 0 ;;
esac
printf 'hardware-jpeg' > "$out"
`
	if err := os.WriteFile(ffmpeg, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	outPath := filepath.Join(dir, "strip.jpg")
	if _, err := FilmstripDecodeContext(context.Background(), ffmpeg, "hevc_vaapi", 10,
		filepath.Join(dir, "main10.mp4"), outPath, 60_000, 1920, 1080, 320, 30); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("calls=%d, want sparse + full decode: %q", len(lines), raw)
	}
	for i, line := range lines {
		if !strings.Contains(line, "-hwaccel vaapi") || !strings.Contains(line, "-hwaccel_output_format vaapi") ||
			!strings.Contains(line, "hwdownload,format=p010le") {
			t.Fatalf("pass %d silently left hardware decode: %q", i+1, line)
		}
	}
}
