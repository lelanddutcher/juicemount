package nfs

import (
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// A CEILING ON CONCURRENT FUSE *DATA* OPERATIONS — on every link class.
//
// THE INCIDENT (2026-08-05). Two kernel panics in three hours during a
// 7,000-file run:
//
//	13:18  macFUSE kext panic, DIRECT: "fuse: fuse_ticket_release: ticket
//	       reference count is 0", task juicefs, kext macfuse 5.1.3. Preceded at
//	       12:54 by 62 concurrent in-flight NFS RPCs stalling 23-53s.
//	16:28  watchdog panic, INDIRECT: three nfs.Read RPCs hung 592s on the
//	       degraded session; WindowServer missed check-ins; the kernel killed
//	       the machine.
//
// Every FUSE-concurrency protection we had was gated to cellular and inert on a
// LAN (slowGate cap 1, readQoS "Inert on medium/fast", write path nothing),
// while nfsLstatGate cap 24 covered METADATA syscalls only. So bulk
// ReadAt/WriteAt/Sync had no ceiling at all.
//
// THIS DOES NOT FIX THE KEXT. A refcount underflow in macFUSE is macFUSE's bug.
// This only keeps us from handing it the load that trips it.
//
// ── ADMISSION MUST BE FAST, NOT PATIENT ──────────────────────────────────────
//
// The first version of this waited 10s for a slot. Adversarial review caught
// that it recreated the exact stall the codebase is designed against:
// non-write RPCs hold an rpcSem slot across dispatch (internal/nfs/conn.go),
// and ErrFUSETimeout exists precisely so a handler "returns immediately and
// frees its rpcSem slot rather than parking it on the wedged mount — without
// that, a JuiceFS wedge consumes all 128 in-flight slots, the server stops
// reading requests, and the whole NFS mount goes stale" (internal/nfs/errors.go).
// A 10s park would have let 112 readers sit on rpcSem while 16 progressed,
// starving LOOKUP/GETATTR — which need no FUSE data syscall at all — and making
// the mount unnavigable. Worse, the gate is taken per 256 KiB SUBREAD, and the
// subread deadline is 2s, so a 10s wait blew that budget by 5x.
//
// So the wait is deliberately SHORT. If the ceiling is reached we are already
// at the safe limit; the right move is to shed the request immediately (client
// retries via JUKEBOX) rather than queue behind it holding a connection slot.
const fuseDataGateWait = 250 * time.Millisecond

// fuseDataGateWidth: how many FUSE data syscalls may be in flight at once.
//
// From measurement: 62 concurrent demonstrably killed the session; the drain
// sustains ~10 files/s on 4 workers; readahead adds up to 8 on a fast link. 16
// sits above the legitimate steady state and well below the fatal number.
// JM_FUSE_DATA_GATE tunes it; 0 disables the ceiling entirely.
func fuseDataGateWidth() int {
	if v := os.Getenv("JM_FUSE_DATA_GATE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return 16
}

// A COUNTER, NOT A CHANNEL.
//
// The first version used a buffered channel of cap 16 and tried to narrow the
// effective width on slow links by testing `len(g) >= w` before a blocking
// send. That was a NO-OP: the send only blocks at cap, so with w=2 and 14 slots
// free it admitted instantly. Metered links got 16, not 2 — and the commit
// message advertised protection that did not exist. A variable ceiling needs
// the occupancy check and the admission to be ONE atomic step, which a channel
// cannot express; CAS can.
var fuseDataInFlight atomic.Int64

var fuseDataGateEnabled = fuseDataGateWidth() > 0

// effectiveFUSEDataWidth is the ceiling right now. The class gate LOWERS it and
// never removes it — that inversion is the bug this file exists to close.
func effectiveFUSEDataWidth() int64 {
	base := int64(fuseDataGateWidth())
	switch netprofile.Default().Class() {
	case netprofile.ClassMetered:
		if base > 2 {
			return 2
		}
	case netprofile.ClassSlow:
		if base > 4 {
			return 4
		}
	}
	return base
}

// tryAdmitFUSEData attempts one CAS admission against the current width.
func tryAdmitFUSEData(width int64) bool {
	for {
		cur := fuseDataInFlight.Load()
		if cur >= width {
			return false
		}
		if fuseDataInFlight.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// acquireFUSEData admits one FUSE data syscall. It ALWAYS returns a non-nil
// release the caller must invoke exactly once — defer it immediately so a panic
// or early return cannot leak a slot.
//
// ok=false means the ceiling is occupied; surface errFUSETimeout so the RPC
// becomes a JUKEBOX retry. Refusing is the correct outcome, not a failure: it
// is the backpressure whose absence let reads pile onto a wedged session until
// the machine died.
func acquireFUSEData() (release func(), ok bool) {
	if !fuseDataGateEnabled {
		return func() {}, true
	}
	// Fast path: read the class only if we cannot admit at base width, so the
	// uncontended case never touches netprofile's RWMutex (hot-path discipline).
	base := int64(fuseDataGateWidth())
	if tryAdmitFUSEData(base) {
		// Recheck against the narrowed width; if the class says we should not
		// have been admitted, back out rather than exceed a slow-link ceiling.
		if w := effectiveFUSEDataWidth(); w < base && fuseDataInFlight.Load() > w {
			fuseDataInFlight.Add(-1)
		} else {
			return releaseFUSEData, true
		}
	}
	width := effectiveFUSEDataWidth()
	deadline := time.Now().Add(fuseDataGateWait)
	// Short spin-with-sleep rather than a channel wait: the window is a quarter
	// second, and a slot normally frees in single-digit milliseconds.
	for time.Now().Before(deadline) {
		if tryAdmitFUSEData(width) {
			return releaseFUSEData, true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return func() {}, false
}

func releaseFUSEData() { fuseDataInFlight.Add(-1) }

// fuseDataGateDepth reports occupancy and the current ceiling, for /metrics and
// for tests.
func fuseDataGateDepth() (inUse int, width int) {
	if !fuseDataGateEnabled {
		return 0, 0
	}
	return int(fuseDataInFlight.Load()), int(effectiveFUSEDataWidth())
}

// fuseDataGateRefusals counts admissions denied, so a JUKEBOX storm originating
// HERE is distinguishable in the field from one originating in the spool. A
// gate whose engagement is invisible cannot be tuned or trusted.
var fuseDataGateRefusals atomic.Int64

func noteFUSEDataRefused() { fuseDataGateRefusals.Add(1) }

// FUSEDataGateStats exposes the gate to the metrics endpoint.
func FUSEDataGateStats() (inUse, width int, refusals int64) {
	i, w := fuseDataGateDepth()
	return i, w, fuseDataGateRefusals.Load()
}

// Wire the gate into /metrics at package init.
//
// FUSEDataGateStats shipped on 2026-08-05 with ZERO callers: the ceiling added
// after two kernel panics could not be observed in the field at all. Registering
// from an init() rather than from a wiring site is deliberate — the failure mode
// being fixed is precisely an accessor nobody remembered to call, and this is the
// same class as JM_WAN_MODE (read in six paths, set by nothing). See
// [[project_fuse_concurrency_panics]] and the "dead knobs" doctrine.
//
// Cost is one closure call per /metrics scrape. Nothing is added to admit or
// release, so the data hot path is byte-for-byte unchanged.
func init() {
	metrics.Default().SetFUSEDataGateProvider(func() *metrics.FUSEDataGateSnapshot {
		inUse, width, refusals := FUSEDataGateStats()
		return &metrics.FUSEDataGateSnapshot{
			Enabled:  fuseDataGateEnabled,
			InUse:    inUse,
			Width:    width,
			Refusals: refusals,
		}
	})
}

// tryAcquireFUSEDataBackground admits a BACKGROUND FUSE data syscall without
// waiting at all.
//
// Background work must count toward the ceiling — the macFUSE session does not
// care who issued a syscall, and the sidecar warmer alone can run 48 concurrent
// reads (sidecarWarmSem 3 x sidecarWarmParallel 16), three times the whole
// budget. But it must never DELAY foreground work: nfs/sidecar.go already
// records this lesson for the metadata gate ("48-way opportunistic warming
// could otherwise hold every foreground slot... a user navigating right then
// queued behind work nobody was waiting for"). So a background caller takes a
// slot if one is free and otherwise gives up immediately, skipping the warm.
// BUT "gives up immediately" IS NOT ENOUGH, and the measurement says so. Giving
// up without waiting stops background work from QUEUING ahead of foreground; it
// does nothing to stop background work from having already TAKEN every slot.
// Admitting against the full width means the warmer can hold the entire gate
// and a foreground read then spins out its 250ms and is shed as JUKEBOX.
//
// Measured live 2026-08-17 on WiFi, with the class narrowing the gate to 4:
// in_use 4/4, 565 refusals and climbing, and the gate timeouts concentrated in
// the background source — sidecar_warm 93 timeouts / 46 gate_timeouts against
// foreground's 0 gate_timeouts across 82,842 calls. The reads that DID reach
// FUSE then timed out (foreground 31), which conn.go turns into JUKEBOX. That
// is the retry storm behind the stalled export and the lag before playback
// starts.
//
// So background is capped at HALF the current ceiling, leaving the other half
// permanently available to foreground. Half rather than a fixed reserve because
// the ceiling itself moves with the link class: a fixed reserve that is
// comfortable at width 16 leaves nothing at width 4, which is exactly the case
// that broke. The warmer is opportunistic by construction and simply skips a
// warm it cannot get a slot for.
func backgroundFUSEDataWidth() int64 {
	w := effectiveFUSEDataWidth()
	if w < 2 {
		// A ceiling this narrow is entirely reserved for foreground. On a
		// metered link that is the right answer anyway: opportunistic warming
		// is not what the user is waiting for.
		return 0
	}
	return w / 2
}

func tryAcquireFUSEDataBackground() (release func(), ok bool) {
	if !fuseDataGateEnabled {
		return func() {}, true
	}
	bg := backgroundFUSEDataWidth()
	if bg <= 0 {
		return func() {}, false
	}
	if tryAdmitFUSEData(bg) {
		return releaseFUSEData, true
	}
	return func() {}, false
}
