package main

import (
	"os"
	"strconv"
	"sync/atomic"
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
// proof. A slow first sample is re-measured, and the boot starts online only
// when some sample came in at or under the limit. The fastest sample is the
// link's base RTT; loss and retransmits only add to it. A failed re-measure
// never votes for "online": a link that cannot answer is not a fast link.

const (
	// defaultBootDeferRTT is the U2 limit. JM_BOOT_DEFER_RTT_MS overrides it;
	// 0 disables the defer.
	defaultBootDeferRTT = 500 * time.Millisecond
	// bootRTTSamples is the most connect samples a slow boot takes (the first
	// plus two re-measures). A fast first sample costs nothing extra.
	bootRTTSamples = 3
	// ownerSingleSampleFactor: the selected-owner proof is one sample that
	// cannot be repeated cheaply, so it defers only when that sample is far
	// over the limit. One delayed packet (611 ms on a 200 ms link) adds about
	// one RTT, not a full extra limit.
	ownerSingleSampleFactor = 2
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
// It returns the RTT to use from now on (the fastest successful sample) and
// whether the boot must defer. The boot defers unless some sample was at or
// under the threshold; failed re-measures count for nothing. A nil resample
// (the selected-owner proof) defers only above ownerSingleSampleFactor times
// the threshold.
func confirmSlowBootRTT(first, threshold time.Duration, resample bootRTTSampler) (best time.Duration, slow bool, samples int) {
	best, samples = first, 1
	if threshold <= 0 || first <= threshold {
		return best, false, samples
	}
	if resample == nil {
		return best, first > ownerSingleSampleFactor*threshold, samples
	}
	for samples < bootRTTSamples && best > threshold {
		ok, rtt := resample()
		samples++
		if ok && rtt > 0 && rtt < best {
			best = rtt
		}
	}
	return best, best > threshold, samples
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

// offlineBootAt is when this serving session started offline (UnixNano), or 0
// for an online start. /activity uses it to keep saying "the file list is
// still loading" after offline lifts and before the first full sync lands, so
// "online" never shows over a list that does not exist yet.
var offlineBootAt atomic.Int64

// fileListRefreshDetail returns the /activity text for a file list that an
// offline start has not refreshed yet, or "" when nothing is pending. lastSync
// is the completion time of the last successful full sync. While offline with
// a saved list, the offline banner already says what is shown; nothing extra.
func fileListRefreshDetail(bootAt, lastSync time.Time, hasEntries, offline bool) (detail string, active bool) {
	if bootAt.IsZero() || lastSync.After(bootAt) {
		return "", false
	}
	switch {
	case !hasEntries && offline:
		return "No saved file list on this Mac yet — files appear when the server is reachable", false
	case !hasEntries:
		return "Loading the file list from the server…", true
	case offline:
		return "", false
	default:
		return "Refreshing the file list after an offline start…", true
	}
}
