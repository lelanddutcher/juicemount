package main

import (
	"os"
	"strconv"
	"time"
)

// Boot offline policy (U2 RTT defer and the start-while-offline status).
//
// The U2 defer makes a reachable-but-slow backend take the start-while-offline
// path, so a terrible link does not block boot on a synchronous mount and
// sync. It used to decide on ONE TCP connect sample. On a 200 ms link with 1%
// loss each way, one delayed packet pushed that sample to 611 ms (limit
// 500 ms). A cold install then started offline with an empty mirror, and
// every file returned ENOENT (laptop lab, 2026-11-11, after-8).
//
// The rule now matches the reachability monitor (PR #71): one bad sample is not
// proof. A slow first sample is re-measured, and the boot defers only when at
// least two samples were taken and the FASTEST is still over the limit. The
// fastest sample is the link's base RTT; loss and retransmits only add to it.

const (
	// defaultBootDeferRTT is the U2 limit. JM_BOOT_DEFER_RTT_MS overrides it;
	// 0 disables the defer.
	defaultBootDeferRTT = 500 * time.Millisecond
	// bootRTTSamples is the most connect samples a slow boot takes (the first
	// plus two re-measures). A fast first sample costs nothing extra.
	bootRTTSamples = 3
	// bootRTTSlowSamples is how many successful samples must agree before the
	// boot treats the link as too slow.
	bootRTTSlowSamples = 2
)

// noMirrorOfflineReason is the status when the app starts offline and has no
// saved file list. The volume can show nothing, so say so instead of letting
// every file look missing.
const noMirrorOfflineReason = "Started offline — no saved file list on this Mac yet. Files appear when the server is reachable"

// bootDeferThreshold returns the U2 limit (0 = defer disabled).
func bootDeferThreshold() time.Duration {
	if v := os.Getenv("JM_BOOT_DEFER_RTT_MS"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			if n <= 0 {
				return 0
			}
			return time.Duration(n) * time.Millisecond
		}
	}
	return defaultBootDeferRTT
}

// bootRTTSampler takes one more boot connect sample. ok is false when the
// sample failed (a lost SYN past its timeout); a failure is not evidence of a
// slow link, only of a lossy one.
type bootRTTSampler func() (ok bool, rtt time.Duration)

// confirmSlowBootRTT decides the U2 defer from a first successful sample.
// It returns the RTT to use from now on (the fastest sample) and whether the
// boot must defer. A nil resample keeps the old single-sample rule (the
// selected-owner proof cannot be repeated cheaply).
func confirmSlowBootRTT(first, threshold time.Duration, resample bootRTTSampler) (best time.Duration, slow bool, samples int) {
	best, samples = first, 1
	if threshold <= 0 || first <= threshold {
		return best, false, samples
	}
	if resample == nil {
		return best, true, samples
	}
	successes := 1
	for samples < bootRTTSamples && best > threshold {
		ok, rtt := resample()
		samples++
		if !ok || rtt <= 0 {
			continue
		}
		successes++
		if rtt < best {
			best = rtt
		}
	}
	return best, best > threshold && successes >= bootRTTSlowSamples, samples
}

// skipDeferForEmptyMirror reports whether a U2 defer must be cancelled because
// there is nothing to serve offline. The defer only helps when the offline
// volume has a saved file list to show; with none, a slow online boot shows
// files and an offline one shows an empty volume.
func skipDeferForEmptyMirror(deferred, mirrorHasEntries bool) bool {
	return deferred && !mirrorHasEntries
}

// startOfflineReasonFor returns the status for an offline start. When there is
// no saved file list, the no-mirror reason replaces the cause; the cause is
// still in the log.
func startOfflineReasonFor(cause string, mirrorHasEntries bool) string {
	if !mirrorHasEntries {
		return noMirrorOfflineReason
	}
	return cause
}
