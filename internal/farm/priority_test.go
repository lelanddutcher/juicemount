package farm

import (
	"reflect"
	"testing"
)

func TestClassifyProxyPriority(t *testing.T) {
	cases := []struct {
		name string
		v    *VideoTrack
		want ProxyPriority
	}{
		// The classes the consumer asked us to prefer.
		{"hevc 4:2:2 10-bit", &VideoTrack{Codec: "hevc", PixFmt: "yuv422p10le", Width: 3840, Height: 2160, FPS: 25}, ProxyPriorityHigh},
		{"hevc 4:4:4 12-bit", &VideoTrack{Codec: "hevc", PixFmt: "yuv444p12le", Width: 1920, Height: 1080, FPS: 24}, ProxyPriorityHigh},
		{"h265 alias 4:2:2", &VideoTrack{Codec: "H265", PixFmt: "YUV422P10LE", Width: 1920, Height: 1080, FPS: 24}, ProxyPriorityHigh},
		{"XF-AVC 4K60", &VideoTrack{Codec: "h264", PixFmt: "yuv420p", Width: 3840, Height: 2160, FPS: 59.94}, ProxyPriorityHigh},
		{"h264 4K exactly 50fps", &VideoTrack{Codec: "h264", PixFmt: "yuv420p", Width: 3840, Height: 2160, FPS: 50}, ProxyPriorityHigh},

		// The classes the consumer explicitly said NOT to spend budget on.
		{"AV1 decodes locally now", &VideoTrack{Codec: "av1", PixFmt: "yuv420p10le", Width: 3840, Height: 2160, FPS: 60}, ProxyPriorityNormal},
		{"ordinary HD h264", &VideoTrack{Codec: "h264", PixFmt: "yuv420p", Width: 1920, Height: 1080, FPS: 25}, ProxyPriorityNormal},
		{"4K h264 at 24fps is not the 4K60 class", &VideoTrack{Codec: "h264", PixFmt: "yuv420p", Width: 3840, Height: 2160, FPS: 24}, ProxyPriorityNormal},
		{"h264 60fps but only HD", &VideoTrack{Codec: "h264", PixFmt: "yuv420p", Width: 1920, Height: 1080, FPS: 60}, ProxyPriorityNormal},
		{"hevc 4:2:0 is the easy path", &VideoTrack{Codec: "hevc", PixFmt: "yuv420p10le", Width: 3840, Height: 2160, FPS: 30}, ProxyPriorityNormal},
		{"prores", &VideoTrack{Codec: "prores", PixFmt: "yuv422p10le", Width: 1920, Height: 1080, FPS: 24}, ProxyPriorityNormal},

		// Unknown tech must never be guessed into the fast lane.
		{"nil track", nil, ProxyPriorityNormal},
		{"empty track", &VideoTrack{}, ProxyPriorityNormal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClassifyProxyPriority(tc.v); got != tc.want {
				t.Errorf("ClassifyProxyPriority = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPrioritizeTargets_StablePartition(t *testing.T) {
	heavy := &VideoTrack{Codec: "hevc", PixFmt: "yuv422p10le", Width: 3840, Height: 2160, FPS: 25}
	light := &VideoTrack{Codec: "h264", PixFmt: "yuv420p", Width: 1920, Height: 1080, FPS: 25}

	targets := []string{"a.mov", "b.mxf", "c.mov", "d.mxf", "e.mov"}
	techFor := func(p string) *VideoTrack {
		switch p {
		case "b.mxf", "d.mxf":
			return heavy
		case "a.mov", "c.mov":
			return light
		default:
			return nil // unknown tech — must keep its place among the rest
		}
	}
	got := PrioritizeTargets(targets, techFor)
	want := []string{"b.mxf", "d.mxf", "a.mov", "c.mov", "e.mov"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("PrioritizeTargets = %v, want %v", got, want)
	}
	// The input must not be mutated — a sweep may hold its own copy.
	if targets[0] != "a.mov" {
		t.Errorf("input slice was mutated: %v", targets)
	}
}

func TestPrioritizeTargets_NoOpCases(t *testing.T) {
	in := []string{"a.mov", "b.mov"}
	// No tech at all: order preserved, no reordering churn.
	if got := PrioritizeTargets(in, func(string) *VideoTrack { return nil }); !reflect.DeepEqual(got, in) {
		t.Errorf("all-unknown reordered: %v", got)
	}
	// nil resolver is a no-op rather than a panic.
	if got := PrioritizeTargets(in, nil); !reflect.DeepEqual(got, in) {
		t.Errorf("nil techFor reordered: %v", got)
	}
	// Single element / empty are trivially stable.
	if got := PrioritizeTargets([]string{"only.mov"}, func(string) *VideoTrack { return nil }); len(got) != 1 {
		t.Errorf("single element mangled: %v", got)
	}
}
