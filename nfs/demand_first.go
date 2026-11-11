package nfs

import (
	"os"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// Demand first, on a slow link (2026-11-11 laptop lab, problem 1).
//
// A "demand read" is a foreground FUSE pread that an NFS client is waiting
// for. On a slow or metered link, it must get its bytes before speculative
// work. The lab showed what happens otherwise, at 200 ms RTT and 1% loss:
// one random 1 MB read started a windowed read-ahead of several 4 MiB blocks,
// the next read shared the link with those blocks, and its pread passed the
// 8 s FUSE bound. The NFS client then retried with JUKEBOX, and every retry
// started one more pread at the same offset.
//
// Three rules, all inert on medium and fast links:
//
//  1. Defer. A read-ahead block waits, at most demandDeferMax, while any
//     demand read is in flight (foregroundDemand).
//  2. Cap. A windowed read-ahead keeps one block in flight until its stream
//     has read a whole block sequentially, then adds one block for each block
//     the reader has read, up to the policy. A scrub (random 1 MiB reads)
//     never gets more than one speculative block.
//  3. Cancel. A deferred block whose reader moved away is given back unread.
//
// The JUKEBOX bound itself (fuseReadTimeout, 8 s on slow links) stays. What
// changes is the retry: a retry of a pread that is still running joins it
// (pendingFUSEReadResults) instead of starting a new pread and taking a new
// FUSE data slot. When the first pread finishes, its bytes answer the next
// retry. A slow but progressing read therefore finishes once, not in a loop.

// demandDeferMax bounds how long one read-ahead block waits for demand reads.
// It equals the slow-link FUSE read bound: a demand read still running after
// that has already been answered with JUKEBOX.
const demandDeferMax = 8 * time.Second

// demandGate counts demand reads in flight.
type demandGate struct {
	mu   sync.Mutex
	n    int
	idle chan struct{} // closed while n == 0
}

func newDemandGate() *demandGate {
	idle := make(chan struct{})
	close(idle)
	return &demandGate{idle: idle}
}

// foregroundDemand counts the online foreground FUSE preads of cachedFile.
// A pread counts until its syscall returns, also after it timed out.
var foregroundDemand = newDemandGate()

// begin marks one demand read in flight. The returned func ends it; extra
// calls do nothing.
func (g *demandGate) begin() func() {
	g.mu.Lock()
	if g.n == 0 {
		g.idle = make(chan struct{})
	}
	g.n++
	g.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			g.n--
			if g.n == 0 {
				close(g.idle)
			}
			g.mu.Unlock()
		})
	}
}

func (g *demandGate) inflight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.n
}

// waitIdle waits until no demand read is in flight, max passes, or stop
// closes. It reports how long it waited and whether the gate was idle.
func (g *demandGate) waitIdle(max time.Duration, stop <-chan struct{}) (time.Duration, bool) {
	g.mu.Lock()
	idle := g.idle
	g.mu.Unlock()
	select {
	case <-idle:
		return 0, true
	default:
	}
	start := time.Now()
	timer := time.NewTimer(max)
	defer timer.Stop()
	select {
	case <-idle:
		return time.Since(start), true
	case <-timer.C:
	case <-stop:
	}
	return time.Since(start), false
}

// demandFirstClass reports the live link class; tests replace it.
var demandFirstClass = func() netprofile.LinkClass { return netprofile.Default().Class() }

// demandFirstActive reports whether the demand-first rules apply now: the
// link is slow or metered, and JM_DEMAND_FIRST is not "0" (field rollback).
func demandFirstActive() bool {
	return demandFirstForClass(demandFirstClass())
}

func demandFirstForClass(c netprofile.LinkClass) bool {
	if os.Getenv("JM_DEMAND_FIRST") == "0" {
		return false
	}
	return c == netprofile.ClassSlow || c == netprofile.ClassMetered
}

// demandStreamCap is how many windowed blocks one stream may have in flight
// under rule 2: one, plus one for each whole block the reader has read in
// its current sequential run, up to max.
func demandStreamCap(sequentialBytes int64, max int) int {
	n := 1 + int(sequentialBytes/readaheadBlockSize)
	if n > max {
		n = max
	}
	if n < 1 {
		n = 1
	}
	return n
}

// pendingReadKey names one foreground pread: the same file, offset and
// length. An NFS client retry after JUKEBOX repeats all three.
type pendingReadKey struct {
	path string
	off  int64
	n    int
}

type readResult struct {
	n   int
	err error
}

// pendingFUSERead is a pread that outlived its bound. buf is its private
// buffer; n and err are valid once done is closed.
type pendingFUSERead struct {
	done chan struct{}
	buf  []byte
	n    int
	err  error
}

// pendingFUSEReads holds preads that timed out, so a retry can join them.
// A finished result is kept for ttl, long enough for the macOS client's
// JUKEBOX back-off (4 s, doubling to 30 s), then dropped.
type pendingFUSEReads struct {
	mu  sync.Mutex
	m   map[pendingReadKey]*pendingFUSERead
	ttl time.Duration
	max int
}

func newPendingFUSEReads(ttl time.Duration, max int) *pendingFUSEReads {
	return &pendingFUSEReads{m: make(map[pendingReadKey]*pendingFUSERead), ttl: ttl, max: max}
}

// pendingFUSEReadResults is the process-wide registry. Its size is also
// bounded by the FUSE data gate: each running entry holds one data slot.
var pendingFUSEReadResults = newPendingFUSEReads(40*time.Second, 64)

// pendingReadJoinEnabled is the rollback switch JM_FUSE_READ_JOIN=0.
func pendingReadJoinEnabled() bool { return os.Getenv("JM_FUSE_READ_JOIN") != "0" }

// adopt registers a pread that timed out. late delivers its result when the
// syscall returns. It reports false (and registers nothing) when the
// registry is full or already holds key.
func (r *pendingFUSEReads) adopt(key pendingReadKey, buf []byte, late <-chan readResult) bool {
	r.mu.Lock()
	if _, ok := r.m[key]; ok || len(r.m) >= r.max {
		r.mu.Unlock()
		return false
	}
	pr := &pendingFUSERead{done: make(chan struct{}), buf: buf}
	r.m[key] = pr
	r.mu.Unlock()
	go func() {
		res := <-late
		pr.n, pr.err = res.n, res.err
		close(pr.done)
		time.AfterFunc(r.ttl, func() { r.drop(key, pr) })
	}()
	return true
}

func (r *pendingFUSEReads) drop(key pendingReadKey, pr *pendingFUSERead) {
	r.mu.Lock()
	if r.m[key] == pr {
		delete(r.m, key)
	}
	r.mu.Unlock()
}

func (r *pendingFUSEReads) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.m)
}

// join serves a retry of key from the pread already registered for it. ok is
// false when there is none; the caller then reads normally. Otherwise it
// waits up to bound: if the pread finished, it copies its bytes into p and
// returns its result (once: the entry is dropped); if not, it returns
// errFUSETimeout, so the client retries again, without a new pread.
func (r *pendingFUSEReads) join(key pendingReadKey, p []byte, bound time.Duration) (n int, err error, ok bool) {
	r.mu.Lock()
	pr := r.m[key]
	r.mu.Unlock()
	if pr == nil {
		return 0, nil, false
	}
	timer := time.NewTimer(bound)
	defer timer.Stop()
	select {
	case <-pr.done:
	case <-timer.C:
		return 0, errFUSETimeout, true
	}
	r.drop(key, pr)
	n = pr.n
	if n > len(p) {
		n = len(p)
	}
	copy(p, pr.buf[:n])
	return n, pr.err, true
}
