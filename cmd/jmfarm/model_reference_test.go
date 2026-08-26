package main

import "testing"

func TestNormalizeModelReference(t *testing.T) {
	for in, want := range map[string]string{
		"whisper.cpp/medium.en": "medium.en",
		" medium.en ":           "medium.en",
		"/models/custom.bin":    "/models/custom.bin",
	} {
		if got := normalizeModelReference(in); got != want {
			t.Fatalf("normalizeModelReference(%q) = %q, want %q", in, got, want)
		}
	}
}
