package main

import (
	"reflect"
	"testing"
)

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

func TestStandardDecodeProfileFixtures(t *testing.T) {
	var h264, hevc []string
	for _, fixture := range standardDecodeProfileFixtures("h264") {
		h264 = append(h264, fixture.Name)
	}
	for _, fixture := range standardDecodeProfileFixtures("hevc") {
		hevc = append(hevc, fixture.Name)
	}
	if !reflect.DeepEqual(h264, []string{"constrained_baseline", "main", "high"}) {
		t.Fatalf("H.264 fixtures=%v", h264)
	}
	if !reflect.DeepEqual(hevc, []string{"main", "main10", "rext_422_10", "rext_444_10"}) {
		t.Fatalf("HEVC fixtures=%v", hevc)
	}
	var av1 []string
	for _, fixture := range standardDecodeProfileFixtures("av1") {
		av1 = append(av1, fixture.Name)
	}
	if !reflect.DeepEqual(av1, []string{"main_8", "main_10"}) {
		t.Fatalf("AV1 fixtures=%v", av1)
	}
	if fixtures := standardDecodeProfileFixtures("prores"); len(fixtures) != 0 {
		t.Fatalf("unverified codec fixtures=%v", fixtures)
	}
}

func TestHardwareDecodeProofFilterForcesHardwareFrameDownload(t *testing.T) {
	if got := hardwareDecodeProofFilter(8); got != "hwdownload,format=nv12" {
		t.Fatalf("8-bit proof filter=%q", got)
	}
	if got := hardwareDecodeProofFilter(10); got != "hwdownload,format=p010le" {
		t.Fatalf("10-bit proof filter=%q", got)
	}
}
