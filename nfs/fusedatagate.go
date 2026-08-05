package nfs

import (
	"os"
	"strconv"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// A CEILING ON CONCURRENT FUSE *DATA* OPERATIONS — on every link class.
//
// THE INCIDENT THIS EXISTS FOR (2026-08-05). Two kernel panics in three hours
// during a 7,000-file derivative run:
//
//   13:18  macFUSE kext panic, DIRECT:
//          "fuse: fuse_ticket_release: ticket reference count is 0"
//          panicking task juicefs, kext io.macfuse.filesystems.macfuse.25 5.1.3.
//          Preceded at 12:54 by the day's first stall: 62 concurrent in-flight
//          NFS RPCs, ages 23-53s.
//   16:28  watchdog panic, INDIRECT: three nfs.Read RPCs hung in-flight for
//          592s on a degraded session; our own checkFUSE could not even query
//          the mount table; WindowServer missed its check-ins and the kernel
//          killed the machine.
//
// WHY IT WAS POSSIBLE. Every FUSE-concurrency protection we had was gated to
// cellular and inert on a LAN:
//
//   slowGate (drainer)      cap 1   drainLinkIsSlow() -> slow/metered only
//   readQoS (reads)         2 lanes active()          -> slow/metered only
//   write path              none
//   nfsLstatGate            cap 24  all classes, but METADATA syscalls only
//
// So stat/readdir/open were bounded at 24 while bulk ReadAt/WriteAt/Sync — the
// operations that actually saturate a FUSE session — had no ceiling at all.
//
// We learned this in July on cellular (see nfs/drainer.go slowGate: "FUSE
// fsyncs stretched past 120s, the macFUSE session degraded (EBADF), and the
// mount died while draining normally") and fixed it ONLY for slow links,
// because it was diagnosed as uplink saturation. It is not. The trigger is
// CONCURRENCY, and 10GbE simply reaches the dangerous number sooner.
//
// WHAT THIS IS NOT. It does not fix the kext. A refcount underflow in macFUSE
// is macFUSE's bug and no userspace change makes it correct — this only keeps
// us from handing it the load that trips it, until it is fixed upstream.
//
// DEADLOCK DISCIPLINE. A slot is held across ONE syscall and nothing else.
// Never acquire this while holding it, and never acquire another gate while
// holding it — a gated operation that waits on a gated operation wedges the
// pool. Admission uses the existing acquire-or-timeout so a full gate degrades
// to JUKEBOX (the client retries) instead of parking an RPC reader forever.
//
// WHY A SLOT IS DELIBERATELY HELD BY A HUNG SYSCALL. If a FUSE read never
// returns, its slot is never released. That is the intended behaviour, not a
// leak: it is exactly the backpressure that was missing. On 2026-08-05 the
// alternative played out in full — unbounded reads kept being admitted onto an
// already-wedged session until the machine died. Bounded, at most
// fuseDataGateWidth can be stuck, and the next request is told to retry.

// fuseDataGateWidth: how many FUSE data syscalls may be in flight at once.
//
// Chosen against measurement, not taste: 62 concurrent demonstrably killed the
// session, the drain sustains ~10 files/s with 4 workers, and readahead adds up
// to 8 on a fast link. 16 sits above the legitimate steady state and well below
// the number that panicked the box. Tune with JM_FUSE_DATA_GATE; 0 disables the
// ceiling entirely (restores pre-2026-08-05 behaviour — do not, without cause).
func fuseDataGateWidth() int {
	if v := os.Getenv("JM_FUSE_DATA_GATE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return 16
}

var fuseDataGate = func() chan struct{} {
	w := fuseDataGateWidth()
	if w <= 0 {
		return nil // disabled
	}
	return make(chan struct{}, w)
}()

// fuseDataGateWait is how long a data op waits for a slot before giving up.
//
// Bounded well under the client's ~40s soft-mount timeout so a full gate
// surfaces as a retryable JUKEBOX rather than as the ETIMEDOUT ("error 100060")
// that aborts a Finder copy.
const fuseDataGateWait = 10 * time.Second

// slowLinkDataWidth narrows the ceiling further on a link that cannot absorb
// concurrency anyway. The class gate LOWERS the ceiling; it never removes it —
// that inversion is the whole bug this file exists to close.
func slowLinkDataWidth(base int) int {
	switch netprofile.Default().Class() {
	case netprofile.ClassMetered:
		return 2
	case netprofile.ClassSlow:
		return 4
	default:
		return base
	}
}

// acquireFUSEData admits one FUSE data syscall. It ALWAYS returns a non-nil
// release func the caller must invoke exactly once — defer it immediately, so
// a panic or an early return cannot leak the slot.
//
// ok=false means the gate stayed full for fuseDataGateWait; the caller should
// surface errFUSETimeout so the RPC becomes a JUKEBOX retry rather than piling
// another syscall onto a session that is already struggling.
func acquireFUSEData() (release func(), ok bool) {
	g := fuseDataGate
	if g == nil {
		return func() {}, true // explicitly disabled
	}
	// Narrow the effective width on slow links by refusing admission past it,
	// rather than by allocating a second channel — the class can flip live and
	// a resized channel would drop in-flight accounting.
	if w := slowLinkDataWidth(cap(g)); w < cap(g) && len(g) >= w {
		timer := time.NewTimer(fuseDataGateWait)
		defer timer.Stop()
		select {
		case g <- struct{}{}:
			return func() { <-g }, true
		case <-timer.C:
			return func() {}, false
		}
	}
	select {
	case g <- struct{}{}:
		return func() { <-g }, true
	default:
	}
	timer := time.NewTimer(fuseDataGateWait)
	defer timer.Stop()
	select {
	case g <- struct{}{}:
		return func() { <-g }, true
	case <-timer.C:
		return func() {}, false
	}
}

// fuseDataGateDepth reports current occupancy, for /metrics and for tests.
func fuseDataGateDepth() (inUse, capacity int) {
	if fuseDataGate == nil {
		return 0, 0
	}
	return len(fuseDataGate), cap(fuseDataGate)
}
