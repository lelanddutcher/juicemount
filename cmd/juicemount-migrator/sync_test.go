//go:build migrator_wip
// +build migrator_wip

package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseSyncProgress(t *testing.T) {
	type want struct {
		minEmissions int
		finalFiles   int64
		finalBytes   int64
		finalErrors  int64
		finalETASec  int64
		mustContain  string // optional Current substring
	}
	cases := []struct {
		name string
		in   string
		w    want
	}{
		{
			name: "single copied line, file count only",
			in:   "Scanned 100 entries, copied 50, failed 0\n",
			w:    want{minEmissions: 1, finalFiles: 50, finalErrors: 0},
		},
		{
			name: "size+rate line with ETA",
			in:   "Scanned: 1000, copied: 500 (12.3 MiB/s), skipped: 0, failed: 0, eta: 12s\n",
			w:    want{minEmissions: 1, finalETASec: 12},
		},
		{
			name: "multi-line, last value wins",
			in: "Scanned 10, copied 5, failed 0\n" +
				"Scanned 100, copied 50, failed 1\n" +
				"Scanned 200, copied 100, failed 2\n",
			// throttle is 200ms, but the parser only flushes on EOF
			// past the throttle; we expect at least the final emission.
			w: want{minEmissions: 1, finalFiles: 100, finalErrors: 2},
		},
		{
			name: "copying current path tracking",
			in: "Copying /mnt/source/file1.mov\n" +
				"Scanned 1, copied 1\n",
			w: want{minEmissions: 1, finalFiles: 1, mustContain: "file1.mov"},
		},
		{
			name: "empty input — no events",
			in:   "",
			w:    want{minEmissions: 1}, // final flush still emits a zero event
		},
		{
			name: "garbage lines ignored",
			in: "some random log line\n" +
				"2026/05/26 14:33:21 <INFO> setting up\n" +
				"Scanned 5, copied 5, failed 0\n",
			w: want{minEmissions: 1, finalFiles: 5},
		},
		{
			name: "bytes-only format",
			in:   "Scanned: 100, bytes: 1048576, failed: 0\n",
			w:    want{minEmissions: 1, finalBytes: 1048576},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ch := make(chan ProgressEvent, 64)
			done := make(chan struct{})
			go func() {
				parseSyncProgress(strings.NewReader(tc.in), ch)
				close(ch)
				close(done)
			}()
			// Wait for parse-and-close.
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatalf("parse did not terminate within 2s")
			}
			events := []ProgressEvent{}
			for ev := range ch {
				events = append(events, ev)
			}
			if len(events) < tc.w.minEmissions {
				t.Fatalf("expected at least %d emissions, got %d (events=%v)",
					tc.w.minEmissions, len(events), events)
			}
			last := events[len(events)-1]
			if tc.w.finalFiles != 0 && last.Files != tc.w.finalFiles {
				t.Errorf("final Files=%d, want %d", last.Files, tc.w.finalFiles)
			}
			if tc.w.finalBytes != 0 && last.Bytes != tc.w.finalBytes {
				t.Errorf("final Bytes=%d, want %d", last.Bytes, tc.w.finalBytes)
			}
			if tc.w.finalErrors != 0 && last.Errors != tc.w.finalErrors {
				t.Errorf("final Errors=%d, want %d", last.Errors, tc.w.finalErrors)
			}
			if tc.w.finalETASec != 0 && last.ETASec != tc.w.finalETASec {
				t.Errorf("final ETASec=%d, want %d", last.ETASec, tc.w.finalETASec)
			}
			if tc.w.mustContain != "" && !strings.Contains(last.Current, tc.w.mustContain) {
				t.Errorf("final Current=%q does not contain %q", last.Current, tc.w.mustContain)
			}
		})
	}
}

func TestApplyUnit(t *testing.T) {
	cases := []struct {
		in   float64
		unit string
		want float64
	}{
		{1, "KB", 1024},
		{1, "KIB", 1024},
		{2, "MB", 2 * 1024 * 1024},
		{2, "MIB", 2 * 1024 * 1024},
		{0.5, "GB", 0.5 * 1024 * 1024 * 1024},
		{1, "", 1}, // no unit = pass-through
		{1, "XYZ", 1}, // unknown unit = pass-through
	}
	for _, tc := range cases {
		got := applyUnit(tc.in, tc.unit)
		if got != tc.want {
			t.Errorf("applyUnit(%v, %q) = %v, want %v", tc.in, tc.unit, got, tc.want)
		}
	}
}

func TestNormalizeSyncURI(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"/mnt/source", "file:///mnt/source"},
		{"file:///mnt/source", "file:///mnt/source"},
		{"s3://bucket/path", "s3://bucket/path"},
		{"jfs://zpool/imported", "jfs://zpool/imported"},
	}
	for _, tc := range cases {
		got := normalizeSyncURI(tc.in)
		if got != tc.want {
			t.Errorf("normalizeSyncURI(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
