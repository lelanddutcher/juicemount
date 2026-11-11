package nfs

import (
	"hash/fnv"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/metadata"
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
// Three rules, all inert on medium and fast links. They also apply on an
// encrypted volume: they only delay or limit speculative block reads, which
// cost a whole-object download there too.
//
//  1. Defer. A read-ahead block waits, at most demandDeferMax, while any
//     demand read is in flight (foregroundDemand).
//  2. Cap. A windowed read-ahead keeps one block in flight until its stream
//     has read a whole block sequentially, then adds one block for each block
//     the reader has read, up to the policy. A scrub (random 1 MiB reads)
//     never gets more than one speculative block.
//  3. Cancel. Blocks are claimed only after the wait, with every stop check
//     run again, so a reader that moved away, a class change, a binding
//     shutdown or offline leaves nothing claimed.
//
// The JUKEBOX bound itself (fuseReadTimeout, 8 s on slow links) stays. What
// changes is the retry: a retry of a pread that is still running joins it
// (pendingFUSEReadResults) instead of starting a new pread and taking a new
// FUSE data slot. When that pread ends, the waiting retry gets its bytes. A
// slow but progressing read therefore finishes once, not in a loop.

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

// pendingReadKey names one foreground pread. A JUKEBOX retry repeats the
// name, offset and length; the rest pins what the bytes came from:
//   - inode, size and mtime are the open-time identity of the file, so a
//     replacement by rename or an in-place write (new mtime, new size) never
//     matches a pread of the old content;
//   - fd is the FUSE descriptor the pread used. A remount (ensureFuseFD after
//     the pool was invalidated) hands out new descriptors, so retries stop
//     joining a pread stuck on the dead mount.
type pendingReadKey struct {
	name    string // in-mount path, for invalidation
	inode   uint64
	size    int64
	mtime   int64
	version metadata.MirrorToken // the mirror revision the open observed
	fd      *os.File
	off     int64
	n       int
}

type readResult struct {
	n   int
	err error
}

// pendingFUSERead is a pread that outlived its bound. buf is its private
// buffer; n and err are valid once done is closed. stale is set when the
// file was invalidated while the pread ran: its bytes then answer nobody.
type pendingFUSERead struct {
	done  chan struct{}
	buf   []byte
	n     int
	err   error
	start time.Time
	stale atomic.Bool
}

// pendingFUSEReads holds preads that timed out and are still running, so a
// retry can join one. An entry leaves the registry when its pread ends: the
// result goes only to retries that joined while it ran, never to a read that
// arrives later (that one starts a fresh pread, which the block cache now
// serves). Nothing finished is retained, so no old bytes can outlive an
// invalidation, and memory is bounded by the reads in flight.
type pendingFUSEReads struct {
	mu  sync.Mutex
	m   map[pendingReadKey]*pendingFUSERead
	max int
	// epochs count invalidations, per hash bucket of the name. A pread reads
	// its bucket before it starts; adopt refuses it if the bucket moved, so
	// an invalidation that lands while the pread runs but before it is
	// registered (the first 8 s) is not lost. A collision only makes a
	// pread unjoinable, never wrong.
	epochs [pendingEpochBuckets]atomic.Uint64
}

const pendingEpochBuckets = 256

func pendingEpochBucket(name string) int {
	h := fnv.New32a()
	h.Write([]byte(name))
	return int(h.Sum32() % pendingEpochBuckets)
}

// epoch is the invalidation count for name, read before a pread starts.
func (r *pendingFUSEReads) epoch(name string) uint64 {
	return r.epochs[pendingEpochBucket(name)].Load()
}

func newPendingFUSEReads(max int) *pendingFUSEReads {
	return &pendingFUSEReads{m: make(map[pendingReadKey]*pendingFUSERead), max: max}
}

// pendingFUSEReadResults is the process-wide registry. Its size is also
// bounded by the FUSE data gate: each running entry holds one data slot.
var pendingFUSEReadResults = newPendingFUSEReads(64)

// pendingReadJoinMaxAge: a retry joins a pread only while it is younger
// than this many bounds. An older pread is presumed wedged (for example on
// a dead mount), and the retry starts its own.
const pendingReadJoinMaxAge = 3

// pendingReadJoinEnabled: the join applies on slow and metered links only
// (where a read legitimately outlives the 8 s bound), and JM_FUSE_READ_JOIN=0
// turns it off.
func pendingReadJoinEnabled() bool {
	return os.Getenv("JM_FUSE_READ_JOIN") != "0" && demandFirstActive()
}

// adopt registers a pread that timed out. late delivers its result when the
// syscall returns. It reports false (and registers nothing) when the
// registry is full or already holds key.
//
// epoch is r.epoch(key.name) read before the pread started: if the name was
// invalidated since, the pread may hold old bytes and is not registered.
func (r *pendingFUSEReads) adopt(key pendingReadKey, buf []byte, late <-chan readResult, epoch uint64) bool {
	r.mu.Lock()
	if _, ok := r.m[key]; ok || len(r.m) >= r.max || r.epoch(key.name) != epoch {
		r.mu.Unlock()
		return false
	}
	pr := &pendingFUSERead{done: make(chan struct{}), buf: buf, start: time.Now()}
	r.m[key] = pr
	r.mu.Unlock()
	go func() {
		res := <-late
		pr.n, pr.err = res.n, res.err
		r.drop(key, pr) // before done: a later read never sees the result
		close(pr.done)
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

// forget drops every entry for name (and, with tree, beneath it) and marks
// it stale, so retries already waiting get JUKEBOX and read again. The
// handler calls it wherever it invalidates a path's read caches or pooled
// descriptors (write, overwrite, rename, remove, remote events).
func (r *pendingFUSEReads) forget(name string, tree bool) {
	if name == "" {
		return
	}
	prefix := strings.TrimSuffix(name, "/") + "/"
	r.mu.Lock()
	if tree {
		for i := range r.epochs {
			r.epochs[i].Add(1)
		}
	} else {
		r.epochs[pendingEpochBucket(name)].Add(1)
	}
	for k, pr := range r.m {
		if k.name == name || tree && strings.HasPrefix(k.name, prefix) {
			pr.stale.Store(true)
			delete(r.m, k)
		}
	}
	r.mu.Unlock()
}

// forgetAll drops every entry and invalidates every running pread (a
// content reset after metadata reconciliation).
func (r *pendingFUSEReads) forgetAll() {
	r.mu.Lock()
	for i := range r.epochs {
		r.epochs[i].Add(1)
	}
	for k, pr := range r.m {
		pr.stale.Store(true)
		delete(r.m, k)
	}
	r.mu.Unlock()
}

func (r *pendingFUSEReads) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.m)
}

// join lets a retry of key wait for the pread already running for it. ok is
// false when there is none (or it is too old); the caller then reads
// normally. Otherwise it waits up to bound: if the pread ends, it copies its
// bytes into p and returns its result; if not, or the file was invalidated,
// it returns errFUSETimeout, so the client retries again without a new pread.
func (r *pendingFUSEReads) join(key pendingReadKey, p []byte, bound time.Duration) (n int, err error, ok bool) {
	r.mu.Lock()
	pr := r.m[key]
	if pr != nil && time.Since(pr.start) > pendingReadJoinMaxAge*bound {
		delete(r.m, key) // presumed wedged; the retry reads on its own
		pr = nil
	}
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
	if pr.stale.Load() {
		return 0, errFUSETimeout, true
	}
	n = pr.n
	if n > len(p) {
		n = len(p)
	}
	copy(p, pr.buf[:n])
	return n, pr.err, true
}
