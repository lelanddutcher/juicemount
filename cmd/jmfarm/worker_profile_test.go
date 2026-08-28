package main

import "testing"

func TestWorkerProbeFailureDoesNotMislabelVerifiedAccelerator(t *testing.T) {
	failedAlternatives := []string{"hevc_qsv probe failed", "h264_qsv probe failed"}
	if got := workerProbeFailure(failedAlternatives, true); got != "" {
		t.Fatalf("verified accelerator reported optional probe errors: %q", got)
	}
	if got := workerProbeFailure(failedAlternatives, false); got != "hevc_qsv probe failed; h264_qsv probe failed" {
		t.Fatalf("failed admission lost probe evidence: %q", got)
	}
	if got := workerProbeFailure(nil, false); got != "" {
		t.Fatalf("empty probe errors = %q, want empty", got)
	}
}
