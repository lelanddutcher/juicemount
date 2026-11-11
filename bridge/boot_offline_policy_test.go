package main

import (
	"os"
	"strings"
	"testing"
	"time"
)

// samplerOf returns successive samples; a zero duration is a failed sample.
func samplerOf(t *testing.T, samples ...time.Duration) (bootRTTSampler, *int) {
	t.Helper()
	calls := 0
	return func() (bool, time.Duration) {
		if calls >= len(samples) {
			t.Fatalf("resample called %d times, only %d samples scripted", calls+1, len(samples))
		}
		s := samples[calls]
		calls++
		return s > 0, s
	}, &calls
}

// The lab failure (2026-11-11, after-8): a 200 ms + 1% loss link measured one
// 611 ms connect against the 500 ms limit and started offline. A re-measure on
// the same link answers in about one RTT, so the boot must stay online.
func TestBootRTTSlowFirstSampleOnWorkingLinkStaysOnline(t *testing.T) {
	resample, calls := samplerOf(t, 205*time.Millisecond)
	best, slow, samples := confirmSlowBootRTT(611*time.Millisecond, defaultBootDeferRTT, resample)
	if slow {
		t.Fatal("one slow sample on a working 200 ms link deferred the boot to offline")
	}
	if best != 205*time.Millisecond || samples != 2 || *calls != 1 {
		t.Fatalf("best=%v samples=%d calls=%d; want the 205ms re-measure after one extra sample", best, samples, *calls)
	}
}

// A lost resample is evidence of loss, not of a slow link: with only one
// successful slow sample the boot stays online.
func TestBootRTTFailedResamplesDoNotDefer(t *testing.T) {
	resample, calls := samplerOf(t, 0, 0)
	_, slow, samples := confirmSlowBootRTT(611*time.Millisecond, defaultBootDeferRTT, resample)
	if slow || samples != bootRTTSamples || *calls != 2 {
		t.Fatalf("slow=%v samples=%d calls=%d; want online after %d samples", slow, samples, *calls, bootRTTSamples)
	}
}

// A genuinely slow link (base RTT over the limit on every sample) still takes
// the U2 defer, and the boot carries its fastest sample forward.
func TestBootRTTConfirmedSlowLinkDefers(t *testing.T) {
	resample, _ := samplerOf(t, 820*time.Millisecond, 790*time.Millisecond)
	best, slow, samples := confirmSlowBootRTT(900*time.Millisecond, defaultBootDeferRTT, resample)
	if !slow || best != 790*time.Millisecond || samples != 3 {
		t.Fatalf("slow=%v best=%v samples=%d; want a defer at 790ms after 3 samples", slow, best, samples)
	}
}

func TestBootRTTFastFirstSampleTakesNoExtraSample(t *testing.T) {
	resample, calls := samplerOf(t)
	if _, slow, samples := confirmSlowBootRTT(3*time.Millisecond, defaultBootDeferRTT, resample); slow || samples != 1 || *calls != 0 {
		t.Fatalf("slow=%v samples=%d calls=%d; a LAN boot must not pay a re-measure", slow, samples, *calls)
	}
}

func TestBootRTTDisabledAndOwnerRules(t *testing.T) {
	if _, slow, _ := confirmSlowBootRTT(5*time.Second, 0, nil); slow {
		t.Fatal("threshold 0 (JM_BOOT_DEFER_RTT_MS=0) must disable the defer")
	}
	// No resampler (selected-owner proof): the single sample decides, as before.
	if _, slow, samples := confirmSlowBootRTT(900*time.Millisecond, defaultBootDeferRTT, nil); !slow || samples != 1 {
		t.Fatalf("owner single-sample rule: slow=%v samples=%d", slow, samples)
	}
}

func TestBootDeferThresholdEnv(t *testing.T) {
	t.Setenv("JM_BOOT_DEFER_RTT_MS", "")
	if got := bootDeferThreshold(); got != defaultBootDeferRTT {
		t.Fatalf("default threshold = %v", got)
	}
	t.Setenv("JM_BOOT_DEFER_RTT_MS", "1200")
	if got := bootDeferThreshold(); got != 1200*time.Millisecond {
		t.Fatalf("override threshold = %v", got)
	}
	t.Setenv("JM_BOOT_DEFER_RTT_MS", "0")
	if got := bootDeferThreshold(); got != 0 {
		t.Fatalf("disabled threshold = %v", got)
	}
}

// With no saved file list an offline start can show nothing, so a slow-link
// defer is cancelled, and a real offline start says why the volume is empty.
func TestNoSavedFileListPolicy(t *testing.T) {
	if !skipDeferForEmptyMirror(true, false) {
		t.Fatal("a slow boot with no saved file list must start online, not offline and empty")
	}
	if skipDeferForEmptyMirror(true, true) || skipDeferForEmptyMirror(false, false) {
		t.Fatal("the defer is cancelled only when it was taken AND there is no saved file list")
	}
	got := startOfflineReasonFor(offlineStartReason, false)
	if got != noMirrorOfflineReason || !strings.Contains(got, "no saved file list") {
		t.Fatalf("offline start with no saved list: status %q, want the no-saved-list reason", got)
	}
	if got := startOfflineReasonFor(offlineStartReason, true); got != offlineStartReason {
		t.Fatalf("offline start with a saved list: status %q, want the cause unchanged", got)
	}
}

// Wiring guard: the boot path must use the helpers above, and apply the
// empty-mirror rule before the mount and connect lanes read backendUp.
func TestBootOfflinePolicyProductionWiring(t *testing.T) {
	src, err := os.ReadFile("cbridge.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, want := range []string{
		"confirmSlowBootRTT(first, threshold, resample)",
		"skipDeferForEmptyMirror(bootRTTDeferred, mirrorHasEntries)",
		"startOfflineReasonFor(startOfflineReason, mirrorHasEntries)",
		"store.HasEntries()",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("cbridge.go no longer calls %s", want)
		}
	}
	skip := strings.Index(s, "skipDeferForEmptyMirror(bootRTTDeferred, mirrorHasEntries)")
	lanes := strings.Index(s, "connectBootRedis := func()")
	if skip < 0 || lanes < 0 || skip > lanes {
		t.Fatal("the empty-mirror rule must run before the mount/connect lanes are built")
	}
}
