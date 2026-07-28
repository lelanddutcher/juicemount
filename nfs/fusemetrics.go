package nfs

import (
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
)

// FUSE metadata attribution plumbing.
//
// Every bounded FUSE metadata helper in handler.go now takes a
// metrics.FUSESource as its FIRST argument and records (source, op) → calls,
// total nanos, gate-wait nanos, timeouts. The point is to answer a question
// the process previously could not: on a 500ms-RTT link that burned 67
// minutes of cumulative FUSE metadata wait in one session, WHO issued those
// syscalls? Foreground RPCs (a client is blocked) or one of the background
// warmers? Nothing here changes timeouts, gates, retries or error mapping —
// this is measurement only. Acting on it (e.g. moving the warmers off the
// foreground gate) is a separate, later change.
//
// See internal/metrics/fuse_attrib.go for the counter layout and the
// hot-path cost contract.

// acquireFUSEGate takes a slot on a bounded FUSE gate, or gives up when the
// caller's deadline fires.
//
// Semantics are IDENTICAL to the `select { case gate <- struct{}{}: ; case
// <-timer.C: }` this replaces: the non-blocking attempt first is purely so
// the COMMON uncontended acquire costs no clock read and reports an exact
// zero wait — the same discipline internal/nfs/conn.go uses for the H1
// rpcSem admission grader. (The timer is created immediately before this
// call, so it cannot already be armed at the try; there is no case where the
// original select would have chosen the timer and this does not.)
//
// depth is the gate occupancy observed right after taking the slot (1..cap);
// it is the high-water saturation signal for the gate that head-of-line-
// blocks navigation. It is 0 when no slot was taken.
func acquireFUSEGate(gate chan struct{}, timer *time.Timer) (gateWait time.Duration, depth int, ok bool) {
	select {
	case gate <- struct{}{}:
		return 0, len(gate), true
	default:
	}
	waitStart := time.Now()
	select {
	case gate <- struct{}{}:
		return time.Since(waitStart), len(gate), true
	case <-timer.C:
		return time.Since(waitStart), 0, false
	}
}

// fuseGateID maps a gate channel to its metrics identity. Used by the two
// gate-PARAMETERIZED helpers (readDirWithTimeout / infoWithTimeout), where
// the gate is chosen by the caller: foreground readdir passes nfsLstatGate,
// background warmers pass prefetchGate. A few pointer compares.
func fuseGateID(gate chan struct{}) metrics.FUSEGate {
	switch gate {
	case nfsLstatGate:
		return metrics.FUSEGateNFSLstat
	case prefetchGate:
		return metrics.FUSEGatePrefetch
	case fuseFstatGate:
		return metrics.FUSEGateFstat
	}
	return metrics.FUSEGateNone
}

// init registers the live gate-occupancy provider with the default registry.
// internal/metrics cannot import nfs (that would be a cycle), so /metrics
// reads the three gates through this callback — the same provider pattern
// already used for the netprofile link estimate.
func init() {
	metrics.Default().SetFUSEGateProvider(func() [metrics.NumFUSEGates]metrics.FUSEGateLevel {
		var out [metrics.NumFUSEGates]metrics.FUSEGateLevel
		out[metrics.FUSEGateNFSLstat] = metrics.FUSEGateLevel{Depth: len(nfsLstatGate), Cap: cap(nfsLstatGate)}
		out[metrics.FUSEGatePrefetch] = metrics.FUSEGateLevel{Depth: len(prefetchGate), Cap: cap(prefetchGate)}
		out[metrics.FUSEGateFstat] = metrics.FUSEGateLevel{Depth: len(fuseFstatGate), Cap: cap(fuseFstatGate)}
		return out
	})
}
