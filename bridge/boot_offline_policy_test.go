package main

import (
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

// A failed re-measure never votes for "online". With the first sample slow and
// both re-measures lost (dial timeouts), no sample proved the link fast: a
// nearly dead link must take the offline path, not the slowest online boot.
func TestBootRTTFailedResamplesStillDefer(t *testing.T) {
	resample, calls := samplerOf(t, 0, 0)
	best, slow, samples := confirmSlowBootRTT(611*time.Millisecond, defaultBootDeferRTT, resample)
	if !slow || best != 611*time.Millisecond || samples != bootRTTSamples || *calls != 2 {
		t.Fatalf("slow=%v best=%v samples=%d calls=%d; want a defer after %d samples", slow, best, samples, *calls, bootRTTSamples)
	}
}

// One lost and one fast re-measure: the fast one proves the link.
func TestBootRTTOneFastResampleAmongLossesStaysOnline(t *testing.T) {
	resample, _ := samplerOf(t, 0, 210*time.Millisecond)
	if best, slow, _ := confirmSlowBootRTT(611*time.Millisecond, defaultBootDeferRTT, resample); slow || best != 210*time.Millisecond {
		t.Fatalf("slow=%v best=%v; want online at 210ms", slow, best)
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

func TestBootRTTDisabled(t *testing.T) {
	if _, slow, _ := confirmSlowBootRTT(5*time.Second, 0, nil); slow {
		t.Fatal("threshold 0 (JM_BOOT_DEFER_RTT_MS=0) must disable the defer")
	}
}

// Selected-owner mode has one proof sample and no cheap re-measure. The lab's
// 611 ms (one delayed packet on a 200 ms link) must not defer; a sample far
// over the limit still does.
func TestBootRTTOwnerSingleSampleRule(t *testing.T) {
	if _, slow, samples := confirmSlowBootRTT(611*time.Millisecond, defaultBootDeferRTT, nil); slow || samples != 1 {
		t.Fatalf("owner 611ms: slow=%v samples=%d; want online", slow, samples)
	}
	if _, slow, _ := confirmSlowBootRTT(2*defaultBootDeferRTT, defaultBootDeferRTT, nil); slow {
		t.Fatal("owner sample exactly at 2x the limit must not defer")
	}
	if _, slow, _ := confirmSlowBootRTT(1200*time.Millisecond, defaultBootDeferRTT, nil); !slow {
		t.Fatal("owner sample far over the limit (1.2 s) must defer")
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

// A real offline start with no saved list says why the volume is empty.
func TestNoSavedFileListStatus(t *testing.T) {
	got := startOfflineReasonFor(offlineStartReason, false)
	if got != noMirrorOfflineReason || !strings.Contains(got, "no saved file list") {
		t.Fatalf("offline start with no saved list: status %q, want the no-saved-list reason", got)
	}
	if got := startOfflineReasonFor(offlineStartReason, true); got != offlineStartReason {
		t.Fatalf("offline start with a saved list: status %q, want the cause unchanged", got)
	}
}

// After an offline start, /activity keeps a file-list status until the first
// full sync after the start lands, so "online" never shows over a list that
// does not exist yet.
func TestFileListRefreshDetailAfterOfflineStart(t *testing.T) {
	boot := time.Unix(1_700_000_000, 0)
	before, after := boot.Add(-time.Hour), boot.Add(time.Minute)
	for _, tc := range []struct {
		name                string
		bootAt, last        time.Time
		hasEntries, offline bool
		wantSub             string
		wantActive          bool
	}{
		{"online start", time.Time{}, before, false, false, "", false},
		{"no list, still offline", boot, time.Time{}, false, true, "No saved file list", false},
		{"no list, back online, not synced", boot, time.Time{}, false, false, "Loading the file list", true},
		{"saved list, back online, last sync before boot", boot, before, true, false, "Refreshing the file list", true},
		{"saved list, still offline", boot, before, true, true, "", false},
		{"synced after the offline start", boot, after, true, false, "", false},
		{"synced an empty volume after the offline start", boot, after, false, false, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, active := fileListRefreshDetail(tc.bootAt, tc.last, tc.hasEntries, tc.offline)
			if (tc.wantSub == "" && d != "") || !strings.Contains(d, tc.wantSub) || active != tc.wantActive {
				t.Fatalf("detail=%q active=%v; want %q active=%v", d, active, tc.wantSub, tc.wantActive)
			}
		})
	}
}
