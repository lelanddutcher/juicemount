package nfs

import (
	"os"
	"strconv"
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// Read-QoS (#4, INSTANT-NAV sprint 2026-07-10). On slow/metered links a
// handful of concurrent COLD bulk reads (a Finder preview storm, QuickLook,
// OpenLoupe scans, big sequential pulls, readahead) saturate the uplink; the
// tiny first-block probe reads that make Finder browsing feel alive then
// queue AT THE NETWORK behind megabytes of in-flight GETs. Nav LISTING is
// immune (mirror-served, RAM) — this protects the probe/preview reads.
//
// Shape, never break:
//
//   - interactive lane (off==0 && len <= readQoSInteractiveMaxLen): the
//     Finder/QuickLook first-block probe signature (the same pattern the
//     WAN-amplification dossier measured) and small-file whole reads.
//     Reserved tokens — never queues behind bulk.
//   - bulk lane: every other backend-capable read; bounded width so a storm
//     can't stack unbounded concurrent GETs onto the link.
//   - prefetch (ReadaheadManager): TryAcquire — contended → shed the round.
//     Prefetch is optional by definition; shedding it under contention is
//     the cheapest bandwidth we can give back to interactive reads.
//   - fail-open: an acquire that waits past its bound proceeds gateless
//     (degrades to pre-QoS behavior). QoS must never turn a slow read into
//     an error or add an unbounded stall — the macOS client's ~40s soft
//     timeout is the hard ceiling and both wait bounds sit far inside it.
//
// Inert (one atomic class read, zero acquires) when the link class is
// medium/fast or JM_READ_QOS=0 — the validated 10GbE profile is untouched
// (cellular-revert-safety discipline; see docs/TUNING/REVERT_LOG.md).
// Class flips apply live per-read, same as the drainer's slowGate.
//
// Warm reads (SSD block-cache hits) do pass through the gate on slow links —
// we can't know warm-vs-cold before issuing the read (coldSubreadDur is a
// post-hoc discriminator). They hold a token for µs–ms, so even a fully-warm
// stream recycles the lane thousands of times per second; the bound only
// bites when reads genuinely block on the wire, which is exactly when we
// want it to.
const (
	// readQoSInteractiveMaxLen classifies a read as interactive: it must
	// start at offset 0 and fit in this length. 256 KiB covers the xattr /
	// QuickLook / Finder-preview first-block probes and small-file whole
	// reads without letting a large sequential stream's first read hog the
	// reserved lane for more than one block.
	readQoSInteractiveMaxLen = 256 << 10

	readQoSBulkWidthDefault        = 4
	readQoSInteractiveWidthDefault = 4

	// Fail-open bounds. Interactive is short — if the reserved lane is
	// somehow full past this, the system is in a state where shaping no
	// longer matters. Bulk is generous but far inside the client's ~40s
	// soft timeout so a queued READ RPC still answers in time.
	readQoSInteractiveMaxWait = 5 * time.Second
	readQoSBulkMaxWait        = 15 * time.Second
)

// readQoS is the two-lane read gate. All fields are set at construction;
// the channels are the semaphores.
type readQoS struct {
	enabled     bool
	bulk        chan struct{}
	interactive chan struct{}
	// classFn is injectable for tests; production uses the netprofile
	// singleton. Read per-acquire so live class flips apply immediately.
	classFn func() netprofile.LinkClass
	// wait bounds — fields (not consts) so tests can shrink them.
	intWait  time.Duration
	bulkWait time.Duration
}

func newReadQoS(bulkWidth, intWidth int, classFn func() netprofile.LinkClass) *readQoS {
	if bulkWidth < 1 {
		bulkWidth = 1
	}
	if intWidth < 1 {
		intWidth = 1
	}
	return &readQoS{
		enabled:     os.Getenv("JM_READ_QOS") != "0",
		bulk:        make(chan struct{}, bulkWidth),
		interactive: make(chan struct{}, intWidth),
		classFn:     classFn,
		intWait:     readQoSInteractiveMaxWait,
		bulkWait:    readQoSBulkMaxWait,
	}
}

// defaultReadQoS is the process-wide gate used by the read paths. Widths are
// env-tunable for field experiments (JM_READ_QOS_BULK / JM_READ_QOS_INT).
var defaultReadQoS = newReadQoS(
	readQoSEnvWidth("JM_READ_QOS_BULK", readQoSBulkWidthDefault),
	readQoSEnvWidth("JM_READ_QOS_INT", readQoSInteractiveWidthDefault),
	func() netprofile.LinkClass { return netprofile.Default().Class() },
)

func readQoSEnvWidth(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > 64 {
		return def
	}
	return n
}

// active reports whether the gate shapes reads right now: enabled AND the
// measured link class is slow/metered (the classes where concurrent GETs
// starve each other — same predicate as the drainer's slowGate).
func (q *readQoS) active() bool {
	if q == nil || !q.enabled {
		return false
	}
	c := q.classFn()
	return c == netprofile.ClassSlow || c == netprofile.ClassMetered
}

// readQoSNoop is the shared zero-cost release for inactive/fail-open paths.
func readQoSNoop() {}

// acquire admits one read of n bytes at offset off. It ALWAYS returns a
// non-nil release func the caller must invoke exactly once (defer it
// immediately). Fail-open: if the lane stays full past its wait bound the
// read proceeds ungated and the release is a no-op.
func (q *readQoS) acquire(off int64, n int) func() {
	if !q.active() {
		return readQoSNoop
	}
	lane, wait := q.bulk, q.bulkWait
	if off == 0 && n <= readQoSInteractiveMaxLen {
		lane, wait = q.interactive, q.intWait
	}
	// Fast path: uncontended.
	select {
	case lane <- struct{}{}:
		return func() { <-lane }
	default:
	}
	metrics.Default().IncReadQoSQueued()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case lane <- struct{}{}:
		return func() { <-lane }
	case <-timer.C:
		metrics.Default().IncReadQoSFailOpen()
		return readQoSNoop
	}
}

// tryAcquireBulk is the prefetch admission: non-blocking. ok=false means the
// bulk lane is contended and the caller should SHED its prefetch round
// entirely (it re-triggers on the next sequential read). When the gate is
// inactive it admits without a token.
func (q *readQoS) tryAcquireBulk() (release func(), ok bool) {
	if !q.active() {
		return readQoSNoop, true
	}
	select {
	case q.bulk <- struct{}{}:
		return func() { <-q.bulk }, true
	default:
		metrics.Default().IncReadQoSPrefetchShed()
		return nil, false
	}
}
