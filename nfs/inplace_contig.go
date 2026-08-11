package nfs

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// inplace_contig.go — never serve a hole on the IN-PLACE FUSE write path.
//
// THE DEFECT (task #6, probed 2026-08-04 in nfs/inplace_hole_probe_test.go):
// the SPOOL write path clamps every read to a contiguously-written prefix and
// JUKEBOX-holds a read directed into an unwritten region. The in-place FUSE
// write path — taken whenever a WRITE RPC arrives for a path with no active
// spool entry — had no equivalent. A pread of an unwritten region returns
// ZEROS WITH err=nil, which is indistinguishable from real data, so a reader
// of a still-arriving file gets black frames or a corrupt RAW while GETATTR
// tells it the bytes are real file content. Measured, all four probe arms:
//
//	ARM A/B/D  *nfs.cachedFile     IncompleteAt=false  ReadAt => n=4096 err=<nil> allZero=true
//	ARM C      *nfs.spoolReadFile  IncompleteAt=true   ReadAt => n=0    err="not yet written"
//
// Reachable without the spool at all: an in-place modify of an existing file,
// a SETATTR{size} preallocate (ftruncate-to-grow, where the hole persists for
// as long as the app takes to fill it), a drain-evict reopen, and every write
// on an install where the user turned the spool off.
//
// THE TWO TRAPS, and how each is handled:
//
//  1. THIS IS THE PER-READ-RPC HOT PATH. Consulting a mutex-guarded map on
//     every read would tax every read in the system to protect the rare file
//     that has a hole. So the gate is an atomic counter of paths that
//     currently have a hole — `inPlaceHoles` — and the read path's first act
//     is one atomic load. It is zero in the overwhelming majority of the
//     process's life, including during an ordinary sequential copy, because a
//     tracker is created ONLY when a write actually lands above the contiguous
//     prefix and is DELETED the moment the hole fills. Sequential writers
//     never create one.
//
//  2. TURNING HOLE READS INTO JUKEBOX HOLDS IS WHAT CAUSED THE #100 73s-PER-FILE
//     STALL. So the decision is not re-derived here — it is delegated to
//     planReadAt, the same classifier the spool path uses, which already
//     carries the `._` AppleDouble bypass (sidecars are served like a plain
//     server, never held) and bounds the hold by WRITER LIVENESS. A hold
//     therefore self-releases once the writer goes quiet, which caps the blast
//     radius of a wrong prefix to "a read of a file being actively written may
//     be briefly held instead of returning possibly-wrong bytes". That is the
//     intended trade and it is the spool's live-validated posture.
//
// THE SEED. The contiguous prefix of an in-place modify starts at the file's
// size when the writer opened it — the pre-existing content is all valid — not
// at zero. That size comes from the mirror entry already in hand at the write
// branch of OpenFile, so seeding costs no syscall. Getting it too HIGH merely
// misses a hole (status quo). Too LOW would claim valid bytes are a hole; trap
// 2's liveness bound is what keeps that survivable.

// advanceContig folds the write [off,end) into a contiguous prefix plus a set
// of out-of-order extents, returning the new prefix and extent set.
//
// This is the SHARED implementation. SpoolEntry.advanceContiguousLocked
// delegates to it rather than keeping a second copy: the two paths must make
// identical decisions about what is readable, and the last time this logic was
// wrong — contiguousEnd never coalescing out-of-order writes — it produced the
// #107 end-of-export interrupt. One implementation, one place to fix.
func advanceContig(cend int64, exts []spoolExtent, off, end int64) (int64, []spoolExtent) {
	if end <= cend {
		// Entirely within the readable prefix — an in-place overwrite of
		// already-contiguous bytes. Nothing to advance.
		return cend, exts
	}
	if off <= cend {
		// Touches/extends the prefix. Absorb it, then pull in every recorded
		// extent now contiguous with the grown prefix.
		cend = end
		for len(exts) > 0 && exts[0].start <= cend {
			if exts[0].end > cend {
				cend = exts[0].end
			}
			exts = exts[1:]
		}
		if len(exts) == 0 {
			exts = nil // reclaim the backing array
		}
		return cend, exts
	}
	// Starts strictly above the prefix: an out-of-order write leaving a hole
	// below it. Remember it (coalesced); it advances the prefix once the gap
	// below fills.
	return cend, insertExtent(exts, off, end)
}

// inPlaceHole is the per-path record of an in-flight hole on the in-place FUSE
// write path. It exists ONLY while contiguousEnd < writtenEnd — i.e. only while
// there is genuinely something to guard.
type inPlaceHole struct {
	contiguousEnd int64
	writtenEnd    int64
	extents       []spoolExtent
	lastWrite     time.Time
}

// inPlaceTracker holds every path that currently has a hole.
//
// `holes` is the hot-path gate and is the ONLY thing a read touches when no
// hole exists anywhere. Keep it exactly in step with len(m) — a leaked
// increment taxes every read in the process with a map lookup, and a missed
// increment silently disables the guard.
type inPlaceTracker struct {
	mu    sync.Mutex
	m     map[string]*inPlaceHole
	holes atomic.Int64
}

// noteWrite records a write of [off,end) against path, seeding the contiguous
// prefix from base (the file's size when the writer opened it) on first sight.
func (t *inPlaceTracker) noteWrite(path string, base, off, end int64) {
	if end <= off {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	h, ok := t.m[path]
	if !ok {
		// Fast exit for the overwhelmingly common case: a write that extends
		// (or overwrites within) the known-good prefix leaves no hole, so there
		// is nothing to track and no entry is created. A purely sequential
		// copy never allocates here.
		if off <= base {
			return
		}
		h = &inPlaceHole{contiguousEnd: base, writtenEnd: base}
		if t.m == nil {
			t.m = make(map[string]*inPlaceHole)
		}
		t.m[path] = h
		t.holes.Add(1)
	}

	h.contiguousEnd, h.extents = advanceContig(h.contiguousEnd, h.extents, off, end)
	if end > h.writtenEnd {
		h.writtenEnd = end
	}
	h.lastWrite = time.Now()

	// The hole filled. Drop the record so the read path's atomic gate returns
	// to zero and reads stop paying for it.
	if h.contiguousEnd >= h.writtenEnd {
		delete(t.m, path)
		t.holes.Add(-1)
	}
}

// noteTruncate records a resize. Growing an existing file (F_PREALLOCATE /
// ftruncate-to-grow, which is how a SETATTR{size} arrives) creates a hole from
// the current prefix to the new size WITHOUT writing anything into it — probe
// ARM B, where the hole persists for as long as the app takes to fill it.
func (t *inPlaceTracker) noteTruncate(path string, base, size int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	h, ok := t.m[path]
	if !ok {
		if size <= base {
			// Shrink, or no growth: no hole is created. (A shrink of a file we
			// are not tracking cannot expose unwritten bytes.)
			return
		}
		h = &inPlaceHole{contiguousEnd: base, writtenEnd: base}
		if t.m == nil {
			t.m = make(map[string]*inPlaceHole)
		}
		t.m[path] = h
		t.holes.Add(1)
	}

	if size > h.writtenEnd {
		h.writtenEnd = size
	} else {
		// Shrink: nothing above the new size remains, so drop any extent and
		// prefix above it. Clamping the prefix DOWN is required — leaving it
		// high would mark truncated-away bytes readable.
		h.writtenEnd = size
		if h.contiguousEnd > size {
			h.contiguousEnd = size
		}
		h.extents = clampExtents(h.extents, size)
	}
	h.lastWrite = time.Now()

	if h.contiguousEnd >= h.writtenEnd {
		delete(t.m, path)
		t.holes.Add(-1)
	}
}

// clampExtents drops everything at or above limit and truncates a straddler.
func clampExtents(exts []spoolExtent, limit int64) []spoolExtent {
	out := exts[:0]
	for _, x := range exts {
		if x.start >= limit {
			continue
		}
		if x.end > limit {
			x.end = limit
		}
		out = append(out, x)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// routing reports the read routing for path, and whether any hole is tracked.
//
// The atomic pre-check is the whole hot-path story: with no hole anywhere in
// the process this is a single load and no lock.
func (t *inPlaceTracker) routing(path string) (cend, wend int64, writerActive, ok bool) {
	if t.holes.Load() == 0 {
		return 0, 0, false, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	h, found := t.m[path]
	if !found {
		return 0, 0, false, false
	}
	return h.contiguousEnd, h.writtenEnd, time.Since(h.lastWrite) < pin.SpoolIncompleteStallWindow, true
}

// forget drops a path's record (file closed, deleted, renamed, or aged out).
func (t *inPlaceTracker) forget(path string) {
	if t.holes.Load() == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.m[path]; ok {
		delete(t.m, path)
		t.holes.Add(-1)
	}
}

// evictStale drops records whose writer has been gone for longer than ttl, and
// returns how many it dropped.
//
// Without this a writer that dies mid-hole leaks its record forever, which
// keeps the atomic gate non-zero and taxes every read in the process with a map
// lookup. Reads of such a file are already correct without the record —
// planReadAt stops holding once the writer goes quiet — so dropping it costs no
// safety.
func (t *inPlaceTracker) evictStale(ttl time.Duration) int {
	if t.holes.Load() == 0 {
		return 0
	}
	cutoff := time.Now().Add(-ttl)
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for k, h := range t.m {
		if h.lastWrite.Before(cutoff) {
			delete(t.m, k)
			t.holes.Add(-1)
			n++
		}
	}
	return n
}

// inPlaceReadPlan classifies a read of path against any tracked hole. Returns
// (planSpool, false) when nothing is tracked, meaning "read normally".
func (t *inPlaceTracker) inPlaceReadPlan(path string, off int64) (readPlan, bool) {
	cend, wend, active, ok := t.routing(path)
	if !ok {
		return planSpool, false
	}
	return planReadAt(off, readPlanInputs{
		punchedEnd:    0, // in-place writes are never punched; that is streaming-drain-only
		contiguousEnd: cend,
		writtenEnd:    wend,
		writerActive:  active,
		isAppleDouble: isAppleDoubleNFSPath(path),
	}), true
}
