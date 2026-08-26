package nfs

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
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
//     is one atomic load. It is zero during an ordinary sequential copy. The
//     tracker does retain the copy's contiguous prefix on the WRITE side,
//     because go-nfs opens a fresh writeFile per RPC and the metadata mirror
//     can still report size zero while the next sequential RPC arrives. Those
//     no-hole prefix records never make reads consult the map and are aged out
//     with the existing write-size cleanup.
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

// inPlaceHole is the per-path write-prefix record on the in-place FUSE path.
// A record can remain after a hole fills (contiguousEnd == writtenEnd) so the
// next WRITE RPC can seed from what this process actually observed, rather than
// a temporarily stale mirror size. Only records with contiguousEnd <
// writtenEnd contribute to inPlaceTracker.holes or affect reads.
type inPlaceHole struct {
	contiguousEnd int64
	writtenEnd    int64
	extents       []spoolExtent
	lastWrite     time.Time

	// lastLog throttles the JUKEBOX-hold diagnostic (see logHold).
	lastLog time.Time
	// fromTruncate records that this hole was opened by a GROW (F_PREALLOCATE /
	// ftruncate-to-grow) rather than by an out-of-order write. The two look
	// identical in every other field and want opposite responses: an
	// out-of-order-write hole fills in milliseconds as the neighbouring writes
	// land, while a preallocation hole persists for as long as the app takes to
	// fill it — for a video export, the whole encode. Without this field the
	// log cannot tell "the writer is mid-burst" from "the app reserved 40 GB and
	// is 3% of the way through it", which are very different bugs.
	fromTruncate bool
}

// inPlaceTracker holds recent per-path write prefixes and counts the subset
// that currently has a hole.
//
// `holes` is the hot-path gate and is the ONLY thing a read touches when no
// hole exists anywhere. It counts only records where contiguousEnd <
// writtenEnd; m can also contain no-hole prefix records used exclusively by
// the write path. A leaked increment taxes every read in the process with a
// map lookup, and a missed increment silently disables the guard.
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
		// Retain even a no-hole prefix record. go-nfs opens a fresh writeFile
		// for every WRITE RPC, and during a new sequential copy the metadata
		// mirror can still say size=0 when RPC #2 arrives. Discarding RPC #1's
		// prefix made RPC #2 look out-of-order and JUKEBOX-held a fully written
		// file for the whole liveness window.
		h = &inPlaceHole{contiguousEnd: base, writtenEnd: base}
		if t.m == nil {
			t.m = make(map[string]*inPlaceHole)
		}
		t.m[path] = h
	} else if h.contiguousEnd >= h.writtenEnd && base > h.contiguousEnd {
		// With no tracked hole, a newer mirror size is a valid readable
		// prefix. Never apply this while a hole exists: the mirror's logical
		// size may then be only a high-water mark above unwritten bytes.
		h.contiguousEnd = base
		h.writtenEnd = base
	}

	wasHole := h.contiguousEnd < h.writtenEnd
	h.contiguousEnd, h.extents = advanceContig(h.contiguousEnd, h.extents, off, end)
	if end > h.writtenEnd {
		h.writtenEnd = end
	}
	h.lastWrite = time.Now()
	isHole := h.contiguousEnd < h.writtenEnd
	if !wasHole && isHole {
		t.holes.Add(1)
	} else if wasHole && !isHole {
		t.holes.Add(-1)
	}
	if !isHole {
		// A later out-of-order write is a new event; do not mislabel it as
		// preallocation just because this retained prefix record once came from
		// a truncate-to-grow operation.
		h.fromTruncate = false
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
		h = &inPlaceHole{contiguousEnd: base, writtenEnd: base, fromTruncate: true}
		if t.m == nil {
			t.m = make(map[string]*inPlaceHole)
		}
		t.m[path] = h
	} else if h.contiguousEnd >= h.writtenEnd && base > h.contiguousEnd {
		h.contiguousEnd = base
		h.writtenEnd = base
	}

	wasHole := h.contiguousEnd < h.writtenEnd
	if size > h.writtenEnd {
		// A GROW opened (or widened) this hole. Record that even when the entry
		// already existed from an out-of-order write: for the diagnostic, "a
		// preallocation is involved" is the fact that changes the diagnosis.
		h.fromTruncate = true
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
	isHole := h.contiguousEnd < h.writtenEnd
	if !wasHole && isHole {
		t.holes.Add(1)
	} else if wasHole && !isHole {
		t.holes.Add(-1)
	}
	if !isHole {
		h.fromTruncate = false
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

// logHold emits a throttled diagnostic for an in-flight-hole JUKEBOX on the
// IN-PLACE FUSE path.
//
// WHY THIS EXISTS. The SPOOL path has had this diagnostic since 57d320a
// (SpoolEntry.logInflightJukebox) and its in-place twin had none — so a read
// held here produced a JUKEBOX with no path, no offset, and no hole geometry.
// Measured 2026-08-17: the live log carried JUKEBOX-RATE storms of up to 90
// nfs.Read per 15s during real Premiere exports, and ZERO diagnostic lines to
// say which file or which offset, because every one of them came through this
// path. An instrument that does not cover the path that storms is the same
// defect class this codebase keeps paying for.
//
// Throttled ~1 line / 2s per PATH, matching the spool twin, so a retrying
// client cannot flood the log (the JM_LOOKUP_TRACE lesson). Only ever called
// when a read is actually being held, which is rare.
func (t *inPlaceTracker) logHold(path string, off int64) {
	if t.holes.Load() == 0 {
		return
	}
	t.mu.Lock()
	h, ok := t.m[path]
	if !ok {
		t.mu.Unlock()
		return
	}
	now := time.Now()
	if now.Sub(h.lastLog) < 2*time.Second {
		t.mu.Unlock()
		return
	}
	h.lastLog = now
	cend, wend, lw, ft, ext := h.contiguousEnd, h.writtenEnd, h.lastWrite, h.fromTruncate, len(h.extents)
	t.mu.Unlock()

	jmlog.Warn("nfs: in-place read JUKEBOX-held (offset past contiguous prefix)",
		"path", path, "off", off,
		"contiguous_end", cend, "written_end", wend, "gap", wend-cend,
		"since_last_write_ms", time.Since(lw).Milliseconds(),
		// The discriminator: a preallocation hole persists for the whole
		// encode, an out-of-order-write hole fills in milliseconds.
		"from_preallocate", ft, "extents", ext)
}

// forget drops a path's record (file closed, deleted, renamed, or aged out).
func (t *inPlaceTracker) forget(path string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if h, ok := t.m[path]; ok {
		delete(t.m, path)
		if h.contiguousEnd < h.writtenEnd {
			t.holes.Add(-1)
		}
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
	cutoff := time.Now().Add(-ttl)
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for k, h := range t.m {
		if h.lastWrite.Before(cutoff) {
			delete(t.m, k)
			if h.contiguousEnd < h.writtenEnd {
				t.holes.Add(-1)
			}
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
