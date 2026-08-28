package farm

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestVideoDecodePlanPinsVerifiedHardwarePath(t *testing.T) {
	tests := []struct {
		decoder  string
		depth    int
		wantArgs []string
		wantVF   string
	}{
		{"", 0, nil, ""},
		{"h264_vaapi", 8, []string{"-hwaccel", "vaapi", "-hwaccel_device", "/dev/dri/renderD128", "-hwaccel_output_format", "vaapi"}, "hwdownload,format=nv12,"},
		{"hevc_qsv", 10, []string{"-hwaccel", "qsv", "-hwaccel_device", "/dev/dri/renderD128", "-hwaccel_output_format", "qsv"}, "hwdownload,format=p010le,"},
		{"h264_nvenc", 8, []string{"-hwaccel", "cuda", "-hwaccel_output_format", "cuda"}, "hwdownload,format=nv12,"},
	}
	for _, tc := range tests {
		args, vf, err := videoDecodePlan(tc.decoder, tc.depth)
		if err != nil || !reflect.DeepEqual(args, tc.wantArgs) || vf != tc.wantVF {
			t.Fatalf("plan(%q,%d)=%v %q %v; want %v %q", tc.decoder, tc.depth, args, vf, err, tc.wantArgs, tc.wantVF)
		}
	}
	if _, _, err := videoDecodePlan("libx264", 8); err == nil || !strings.Contains(err.Error(), "unverified") {
		t.Fatalf("software/unknown decoder was admitted: %v", err)
	}
}

func TestThumbnailHardwareDecodeArgumentsReachFFmpeg(t *testing.T) {
	dir := t.TempDir()
	tool := filepath.Join(dir, "fake-ffmpeg")
	logPath := filepath.Join(dir, "args.log")
	t.Setenv("JM_DECODE_ARGS_LOG", logPath)
	script := `#!/bin/sh
printf '%s\n' "$*" > "$JM_DECODE_ARGS_LOG"
out=""
for arg in "$@"; do out="$arg"; done
printf 'jpeg' > "$out"
`
	if err := os.WriteFile(tool, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := ThumbnailDecodeContext(context.Background(), tool, "h264_vaapi", 8,
		filepath.Join(dir, "source.mp4"), filepath.Join(dir, "poster.jpg"), 720, 10_000); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	args := string(raw)
	if !strings.Contains(args, "-hwaccel vaapi") || !strings.Contains(args, "-hwaccel_output_format vaapi") ||
		!strings.Contains(args, "hwdownload,format=nv12") {
		t.Fatalf("thumbnail silently left hardware decode: %q", args)
	}
}
