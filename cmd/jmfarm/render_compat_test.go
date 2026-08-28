package main

import (
	"errors"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

func TestUnsupportedHardwareDecodeReason(t *testing.T) {
	worker := farmqueue.Worker{
		Role:     farmqueue.QueueClassRender,
		Decoders: []string{"h264_vaapi", "hevc_vaapi"},
		Capabilities: []string{
			"decoder:h264_vaapi:profile:main",
			"decoder:h264_vaapi:profile:high",
		},
	}
	tests := []struct {
		name    string
		encoder string
		track   farm.VideoTrack
		want    string
	}{
		{name: "ordinary H264", encoder: "hevc_vaapi", track: farm.VideoTrack{Codec: "h264", PixFmt: "yuv420p", BitDepth: 8}},
		{name: "H264 main profile", encoder: "hevc_vaapi", track: farm.VideoTrack{Codec: "h264", Profile: "Main", PixFmt: "yuv420p", BitDepth: 8}},
		{name: "H264 baseline profile", encoder: "hevc_vaapi", track: farm.VideoTrack{Codec: "h264", Profile: "Baseline", PixFmt: "yuv420p", BitDepth: 8}, want: "profile baseline"},
		{name: "HEVC main10", encoder: "hevc_vaapi", track: farm.VideoTrack{Codec: "hevc", PixFmt: "yuv420p10le", BitDepth: 10}},
		{name: "camera H264 422", encoder: "hevc_vaapi", track: farm.VideoTrack{Codec: "h264", PixFmt: "yuv422p10le", BitDepth: 10}, want: "4:2:0"},
		{name: "H264 high10", encoder: "hevc_vaapi", track: farm.VideoTrack{Codec: "h264", PixFmt: "yuv420p10le", BitDepth: 10}, want: "10-bit"},
		{name: "unverified ProRes", encoder: "hevc_vaapi", track: farm.VideoTrack{Codec: "prores", PixFmt: "yuv422p10le", BitDepth: 10}, want: "no verified"},
		{name: "missing codec decoder", encoder: "hevc_qsv", track: farm.VideoTrack{Codec: "h264", PixFmt: "yuv420p", BitDepth: 8}, want: "h264_qsv"},
		{name: "software encoder", encoder: "libx264", track: farm.VideoTrack{Codec: "h264", PixFmt: "yuv420p", BitDepth: 8}, want: "not a hardware"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := unsupportedHardwareDecodeReason(worker, tc.encoder, &tc.track)
			if tc.want == "" && got != "" {
				t.Fatalf("unexpected rejection: %s", got)
			}
			if tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatalf("reason %q does not contain %q", got, tc.want)
			}
		})
	}
}

func TestPartitionRenderProxyTargetsWithLiveH264AndProRes(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg unavailable")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe unavailable")
	}
	tmp := t.TempDir()
	h264 := filepath.Join(tmp, "camera-h264.mp4")
	prores := filepath.Join(tmp, "camera-prores.mov")
	for _, tc := range []struct {
		path string
		args []string
	}{
		{h264, []string{"-c:v", "libx264", "-pix_fmt", "yuv420p"}},
		{prores, []string{"-c:v", "prores_ks", "-profile:v", "2"}},
	} {
		args := []string{"-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc2=size=64x64:rate=1", "-frames:v", "1"}
		args = append(args, tc.args...)
		args = append(args, tc.path)
		if out, err := exec.Command(ffmpeg, args...).CombinedOutput(); err != nil {
			t.Skipf("ffmpeg fixture encoder unavailable: %v: %s", err, out)
		}
	}

	worker := farmqueue.Worker{
		Role: farmqueue.QueueClassRender, Decoders: []string{"h264_vaapi", "hevc_vaapi"},
		Capabilities: []string{"decoder:h264_vaapi:profile:high"},
	}
	hardware, fallback := partitionRenderProxyTargets(worker, "hevc_vaapi", []string{h264, prores}, probeRenderVideoTrack)
	if !reflect.DeepEqual(hardware, []string{h264}) {
		t.Fatalf("live hardware targets = %v, want only H.264", hardware)
	}
	if len(fallback) != 1 || fallback[0].Path != prores || !strings.Contains(fallback[0].Reason, "prores") {
		t.Fatalf("live fallback targets = %+v, want only ProRes", fallback)
	}
}

func TestPartitionRenderProxyTargetsKeepsCompatibleMajorityOnGPU(t *testing.T) {
	worker := farmqueue.Worker{
		Role:     farmqueue.QueueClassRender,
		Decoders: []string{"h264_vaapi", "hevc_vaapi"},
	}
	targets := []string{"camera-h264.mxf", "camera-prores.mov", "camera-hevc.mp4", "unprobeable.mov", "audio.wav"}
	probe := func(path string) (*farm.VideoTrack, error) {
		switch path {
		case "camera-h264.mxf":
			return &farm.VideoTrack{Codec: "h264", PixFmt: "yuv420p", BitDepth: 8}, nil
		case "camera-prores.mov":
			return &farm.VideoTrack{Codec: "prores", PixFmt: "yuv422p10le", BitDepth: 10}, nil
		case "camera-hevc.mp4":
			return &farm.VideoTrack{Codec: "hevc", PixFmt: "yuv420p10le", BitDepth: 10}, nil
		case "unprobeable.mov":
			return nil, errors.New("truncated header")
		case "audio.wav":
			return nil, nil
		default:
			t.Fatalf("unexpected target %q", path)
			return nil, nil
		}
	}

	hardware, fallback := partitionRenderProxyTargets(worker, "hevc_vaapi", targets, probe)
	if want := []string{"camera-h264.mxf", "camera-hevc.mp4", "audio.wav"}; !reflect.DeepEqual(hardware, want) {
		t.Fatalf("hardware targets = %v, want %v", hardware, want)
	}
	if len(fallback) != 2 || fallback[0].Path != "camera-prores.mov" || fallback[1].Path != "unprobeable.mov" {
		t.Fatalf("fallback targets = %+v", fallback)
	}
	if !strings.Contains(fallback[0].Reason, "no verified hardware decoder") {
		t.Fatalf("ProRes fallback reason = %q", fallback[0].Reason)
	}
	if !strings.Contains(fallback[1].Reason, "truncated header") {
		t.Fatalf("probe fallback reason = %q", fallback[1].Reason)
	}
}

func TestNormalizedDecodeCodec(t *testing.T) {
	for input, want := range map[string]string{
		"h264": "h264", "AVC1": "h264", "hevc": "hevc", "hvc1": "hevc", "prores": "",
	} {
		if got := normalizedDecodeCodec(input); got != want {
			t.Errorf("normalizedDecodeCodec(%q) = %q, want %q", input, got, want)
		}
	}
}
