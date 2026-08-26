package main

import (
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

func TestUnsupportedHardwareDecodeReason(t *testing.T) {
	worker := farmqueue.Worker{
		Role:     farmqueue.QueueClassRender,
		Decoders: []string{"h264_vaapi", "hevc_vaapi"},
	}
	tests := []struct {
		name    string
		encoder string
		track   farm.VideoTrack
		want    string
	}{
		{name: "ordinary H264", encoder: "hevc_vaapi", track: farm.VideoTrack{Codec: "h264", PixFmt: "yuv420p", BitDepth: 8}},
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

func TestNormalizedDecodeCodec(t *testing.T) {
	for input, want := range map[string]string{
		"h264": "h264", "AVC1": "h264", "hevc": "hevc", "hvc1": "hevc", "prores": "",
	} {
		if got := normalizedDecodeCodec(input); got != want {
			t.Errorf("normalizedDecodeCodec(%q) = %q, want %q", input, got, want)
		}
	}
}
