/*
 * JuiceMount (patch 0035): the read-ahead of one sequential reader follows
 * the link while the volume is mounted.
 *
 * The app mounts with --max-readahead 1M. That is below one block, so the
 * session read-ahead in reader.go never starts: the NFS layer was meant to
 * own read-ahead. On macOS it cannot: macFUSE runs one read of a file at a
 * time, 128 KiB each, so a sequential reader has at most one demand span
 * (patch 0021, about 1 MiB) on the link at a time, whatever the NFS client
 * and the NFS layer's read-ahead ask for. On a 12 ms, 300 Mbit/s path one
 * file read at 128-156 Mbit/s (laptop, 2026-11-26).
 *
 * The JMLinkPolicy message (version 2, jm_link_policy.go) sets the maximum
 * read-ahead here. While a message has set it:
 *
 *   - checkReadahead and cleanupRequests use the live maximum instead of the
 *     mount's --max-readahead (readAheadMax and maxRequests);
 *   - a session starts read-ahead only after jmReadaheadMinBlocks blocks
 *     read contiguously (jmRun), so a preview, a header probe or a scrub
 *     jump (1-4 MiB, plus the NFS client's own read-ahead) never pulls a
 *     block it did not ask for. The session's own total does not do: it
 *     also counts the gap when a read lands up to a block past the
 *     session's end, so two scrub jumps close together passed it (lab,
 *     2026-11-27: 30 jumps in a 1 GiB clip fetched 134 MiB instead of 95);
 *   - a lower maximum shrinks running sessions at their next read; 0 (or
 *     anything below one block) turns read-ahead off.
 *
 * Until a message sets it, everything is the upstream behaviour with the
 * mount's flags.
 */

package vfs

import "sync/atomic"

// jmReadaheadMinBlocks is the sequential run, in blocks, before a session
// starts read-ahead once a message set the maximum.
const jmReadaheadMinBlocks = 2

// jmReadaheadLimit is the largest maximum a message may set (1 GiB).
const jmReadaheadLimit = 1 << 30

// jmRun is the contiguous run of one read session: the reads since the
// last one that started before the run, more than the gap past its end, or
// more than the gap behind its end.
// bytes counts what was read, not the span.
type jmRun struct {
	start, end, bytes uint64
}

// jmRunGap is the forward gap a run tolerates, as a fraction of a block:
// half a block (2 MiB) covers the Mac NFS client's READs in flight, which
// macFUSE hands juicefs out of order.
const jmRunGapDiv = 2

// jmRunBytes folds this read into session idx's run and returns the bytes
// the run has read. Called on every read; it only tracks.
func (f *fileReader) jmRunBytes(idx int, block *frange) uint64 {
	r := &f.jmRuns[idx]
	gap := f.r.blockSize / jmRunGapDiv
	// A read more than gap behind the run's end is a jump back (a loop of
	// a short section, review L1), not a reordered READ: a new run.
	if r.bytes == 0 || block.off < r.start || block.off > r.end+gap || block.off+gap < r.end {
		*r = jmRun{start: block.off, end: block.end(), bytes: block.len}
		return r.bytes
	}
	r.end = max(r.end, block.end())
	r.bytes += block.len
	return r.bytes
}

type jmReadahead struct {
	set atomic.Bool
	max atomic.Uint64
}

// jmReadaheadMax is the maximum read-ahead of one session now: the live
// value once set (never above readAheadTotal), else --max-readahead.
func (r *dataReader) jmReadaheadMax() uint64 {
	if !r.jmReadahead.set.Load() {
		return r.readAheadMax
	}
	return min(r.jmReadahead.max.Load(), r.readAheadTotal)
}

// jmReadaheadMinRun is the sequential run a session needs before it starts
// read-ahead: 0 upstream, jmReadaheadMinBlocks blocks once set.
func (r *dataReader) jmReadaheadMinRun() uint64 {
	if !r.jmReadahead.set.Load() {
		return 0
	}
	return jmReadaheadMinBlocks * r.blockSize
}

// jmClampReadahead bounds a session's read-ahead by the live maximum: a
// lower maximum shrinks it, one below a block turns it off.
func (r *dataReader) jmClampReadahead(ra uint64) uint64 {
	if !r.jmReadahead.set.Load() {
		return ra
	}
	m := r.jmReadaheadMax()
	if m < r.blockSize {
		return 0
	}
	return min(ra, m)
}

// jmMaxRequests is how many read-ahead slices a file keeps outside the
// current read: from the live maximum once set, else the mount's.
func (r *dataReader) jmMaxRequests() int {
	if !r.jmReadahead.set.Load() || r.blockSize == 0 {
		return r.maxRequests
	}
	return int(r.jmReadaheadMax()/r.blockSize)*readSessions + 1
}

// JMReadahead reports the read-ahead in force: the maximum, or 0 when it is
// below one block (off).
func (r *dataReader) JMReadahead() uint64 {
	m := r.jmReadaheadMax()
	if m < r.blockSize {
		return 0
	}
	return m
}

// JMSetReadahead sets the maximum read-ahead (bytes; 0 = off). A value above
// jmReadaheadLimit changes nothing and returns false.
func (r *dataReader) JMSetReadahead(n uint64) bool {
	if n > jmReadaheadLimit {
		return false
	}
	r.jmReadahead.max.Store(n)
	r.jmReadahead.set.Store(true)
	return true
}
