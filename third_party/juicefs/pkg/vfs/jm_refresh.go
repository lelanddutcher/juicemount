/*
 * JuiceMount: the JMRefreshInode control message (patch 0031; patch 0032
 * keeps what did not change, see below).
 *
 * The app sends it when its metadata mirror learns that another client
 * changed a file (a teammate's save). It makes this mount serve the new
 * bytes on the next read of a descriptor that is already open, with no
 * reopen. A reopen is not enough on macOS: macFUSE shares one FUSE handle
 * between all open descriptors of a file, so a second open() never reaches
 * juicefs while the first one is open (laptop run 2026-11-14, bug P1).
 *
 * Three caches hold the old version, and the message drops all three:
 *  1. the open file's slice lists (meta openfiles), and its attributes, read
 *     again from the engine (one metadata round trip);
 *  2. the vfs read buffers of every open reader of the inode, with the
 *     reader's length set from the fresh attributes (an append becomes
 *     readable);
 *  3. the kernel's pages and attributes of the inode (FUSE_NOTIFY_INVAL_INODE).
 *     This runs on the message's goroutine, outside any FUSE request, so it
 *     is synchronous: the reply comes after the kernel took the notify. On
 *     macOS juicefs's own open sends this notify asynchronously, and in the
 *     laptop pin run a read beat it (48 MiB served from stale pages).
 *
 * Patch 0032 (bug F1, laptop run 2026-11-14). A teammate's save on a far link
 * commits parts early, so the app refreshes a played file every few seconds
 * while the save runs. Dropping every buffer and every page each time stalled
 * playback for minutes: each refresh restarted the player's block downloads,
 * and the kernel notify for the whole file waits for every read of the file
 * already in flight, which on a slow link takes seconds. The refresh now
 * reads the fresh slice list of each chunk this mount holds buffers or pages
 * of (one parallel metadata round trip), and
 *  - keeps a read buffer when the pieces of the list that cover its range
 *    are the same as in the list it was read from;
 *  - notifies the kernel only for the byte ranges where a list this mount
 *    read the file from since its last refresh differs from the fresh list
 *    (the kernel's pages came from those lists), or the attributes only when
 *    no byte changed. A changed length, a chunk with no fresh list, too many
 *    ranges, a local write, a length change, or no record of the inode
 *    notify the whole file. A notify that is not accepted keeps the record, so the next
 *    refresh drops those ranges again.
 *
 * Body: version(1) = 1, inode(8).
 *
 * Reply: errno(1); on errno 0 also flags(1), length(8), mtime_ns(8): 18
 * bytes. flags: bit 1 the kernel accepted every notify (attributes and the
 * changed pages; whether macFUSE then purges its UBC pages is not
 * observable here); bit 2 no kernel notify
 * (no FUSE server, or the kernel lacks the notify); bit 4 the notify did not
 * answer within jmRefreshNotifyWait (it may still land); bit 8 the notify
 * failed. EINVAL: an unknown version or a short body. ENOTSUP: the metadata
 * engine has no refresh. Engine errors (ENOENT for a deleted inode) as is.
 */

package vfs

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

const (
	jmRefreshVersion = 1
	jmRefreshReplyOK = 1 + 1 + 8 + 8

	JMRefreshKernelAccepted = 1
	JMRefreshKernelNone     = 2
	JMRefreshKernelTimeout  = 4
	JMRefreshKernelFailed   = 8
)

// jmRefreshNotifyWait bounds the wait for the kernel notify. The notify
// waits for the reads of that file already in the kernel (the vnode lock on
// macOS, the locked pages on Linux). On a far link one such read fetches a
// block over the slow link and can take a few seconds: in the remote lab
// (200 ms, 1% loss) a 1 MiB read took 2-3 s, and with a 2 s bound the
// notify of a played file timed out even when the refresh kept every read
// buffer (patch 0032, bug F1). A timed-out notify makes the app answer the
// READs JUKEBOX, which costs the Mac at least 4 s.
var jmRefreshNotifyWait = 6 * time.Second

// jmRefreshSlowLog: a refresh at least this slow logs its phases.
const jmRefreshSlowLog = time.Second

// jmOpenFileRefresher is the metadata side (pkg/meta/jm_refresh.go).
type jmOpenFileRefresher interface {
	JMRefreshOpenFile(ctx meta.Context, inode Ino, attr *Attr) syscall.Errno
	JMReadChunkFresh(ctx meta.Context, inode Ino, indx uint32, slices *[]meta.Slice) syscall.Errno
}

var jmInodeInvalidator atomic.Pointer[func(Ino, int64, int64) syscall.Errno]

// JMSetInodeInvalidator sets how the refresh drops the kernel's pages and
// attributes of an inode (pkg/fuse sets FUSE_NOTIFY_INVAL_INODE). Offset 0
// and length 0 drop every page, offset -1 only the attributes. It returns
// ENOSYS when the kernel has no such notify.
func JMSetInodeInvalidator(f func(ino Ino, off, length int64) syscall.Errno) {
	if f == nil {
		jmInodeInvalidator.Store(nil)
		return
	}
	jmInodeInvalidator.Store(&f)
}

// jmServedMax caps the lists kept per chunk; past it the chunk counts as
// changed everywhere. jmRangesMax caps the ranged notifies of one refresh;
// past it the whole file is notified. jmServedInodes caps the inodes
// tracked; past it the set starts over (an inode with no record gets a
// whole-file notify).
const (
	jmServedMax    = 8
	jmRangesMax    = 64
	jmServedInodes = 4096
)

// jmServedSet records, by inode and chunk, the slice lists this mount's read
// buffers were read from since the inode's last refresh. The kernel's pages
// of the file came from those lists. A local write or a length change
// forgets the inode: the kernel then holds pages no list describes, and the
// next refresh notifies the whole file.
type jmServedSet struct {
	mu sync.Mutex
	m  map[Ino]map[uint32][][]meta.Slice
}

// record notes that a buffer of chunk indx was read from ss. A nil list
// stands for "unknown": the whole chunk.
func (j *jmServedSet) record(inode Ino, indx uint32, ss []meta.Slice) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.m == nil || len(j.m) >= jmServedInodes && j.m[inode] == nil {
		j.m = make(map[Ino]map[uint32][][]meta.Slice)
	}
	chunks := j.m[inode]
	if chunks == nil {
		chunks = make(map[uint32][][]meta.Slice)
		j.m[inode] = chunks
	}
	lists := chunks[indx]
	for _, l := range lists {
		if l == nil || jmSameRange(l, ss, 0, meta.ChunkSize) {
			return
		}
	}
	if len(lists) >= jmServedMax || ss == nil {
		chunks[indx] = [][]meta.Slice{nil}
		return
	}
	chunks[indx] = append(lists, ss)
}

// jmKnown is ss as a known list: an empty chunk is an empty list, not nil.
func jmKnown(ss []meta.Slice) []meta.Slice {
	if ss == nil {
		return []meta.Slice{}
	}
	return ss
}

func (j *jmServedSet) forget(inode Ino) {
	j.mu.Lock()
	delete(j.m, inode)
	j.mu.Unlock()
}

func (j *jmServedSet) chunks(inode Ino) []uint32 {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []uint32
	for indx := range j.m[inode] {
		out = append(out, indx)
	}
	return out
}

// swap returns the inode's record and leaves, for each recorded chunk with a
// fresh list, that list alone: once the kernel drops the changed ranges,
// its pages hold what fresh describes. ok is false when the inode has no
// record.
func (j *jmServedSet) swap(inode Ino, fresh map[uint32][]meta.Slice) (old map[uint32][][]meta.Slice, ok bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	old, ok = j.m[inode]
	if !ok {
		return nil, false
	}
	next := make(map[uint32][][]meta.Slice, len(old))
	for indx := range old {
		if ss, have := fresh[indx]; have {
			next[indx] = [][]meta.Slice{ss}
		}
	}
	j.m[inode] = next
	return old, true
}

// jmRefreshPlan is what one refresh found: the kernel ranges to drop (file
// offset, length), or whole; and the record it replaced, to put back if the
// kernel does not accept the notify.
type jmRefreshPlan struct {
	whole         bool
	ranges        [][2]uint64
	kept, dropped int
	replaced      map[uint32][][]meta.Slice
}

// jmRefresh drops the read buffers of the inode's open readers that may hold
// old bytes, sets their length, and plans the kernel notify. A buffer being
// filled is marked to fill again (REFRESH); a buffer a read holds is filled
// again; an idle one is deleted. A buffer is kept when the pieces of its
// chunk's list over the buffer's own range are the same in the list it was
// read from and in fresh, and the length did not change: a save that writes
// a 4 KiB block every 8 MiB changes every chunk's list, not the blocks
// between.
func (r *dataReader) jmRefresh(inode Ino, length uint64, fresh map[uint32][]meta.Slice) jmRefreshPlan {
	var plan jmRefreshPlan
	r.visit(inode, func(f *fileReader) {
		sameLength := f.length == length
		if !sameLength {
			plan.whole = true
		}
		f.length = length
		f.visit(func(s *sliceReader) bool {
			if sameLength && s.jmSlices != nil && (s.state == BUSY || s.state == READY) {
				from := uint32(s.block.off % meta.ChunkSize)
				if ss, ok := fresh[s.indx]; ok && jmSameRange(ss, s.jmSlices, from, from+uint32(s.block.len)) {
					plan.kept++
					return true
				}
			}
			s.invalidate()
			plan.dropped++
			return true
		})
	})
	old, ok := r.jmServed.swap(inode, fresh)
	plan.replaced = old
	if !ok {
		plan.whole = true
		return plan
	}
	for indx, lists := range old {
		ss, have := fresh[indx]
		if !have {
			plan.whole = true
			continue
		}
		base := uint64(indx) * meta.ChunkSize
		for _, l := range lists {
			for _, rg := range jmChangedRanges(l, ss) {
				plan.ranges = append(plan.ranges, [2]uint64{base + uint64(rg[0]), uint64(rg[1] - rg[0])})
			}
		}
	}
	plan.ranges = jmMergeRanges(plan.ranges)
	if len(plan.ranges) > jmRangesMax {
		plan.whole = true
	}
	return plan
}

// jmMergeRanges sorts (offset, length) ranges and merges those that touch.
func jmMergeRanges(rs [][2]uint64) [][2]uint64 {
	sort.Slice(rs, func(i, j int) bool { return rs[i][0] < rs[j][0] })
	var out [][2]uint64
	for _, rg := range rs {
		if n := len(out); n > 0 && out[n-1][0]+out[n-1][1] >= rg[0] {
			out[n-1][1] = max(out[n-1][1], rg[0]+rg[1]-out[n-1][0])
			continue
		}
		out = append(out, rg)
	}
	return out
}

// jmRestoreServed puts back the record of a refresh whose notify the kernel
// did not accept: those ranges may still hold old pages.
func (r *dataReader) jmRestoreServed(inode Ino, plan jmRefreshPlan) {
	if plan.replaced == nil {
		r.jmServed.forget(inode) // no record: the next refresh is whole again
		return
	}
	for indx, lists := range plan.replaced {
		for _, l := range lists {
			r.jmServed.record(inode, indx, l)
		}
	}
}

// jmTruncate forgets the inode's record when its length changes. Truncate
// also runs for every attribute reply with the unchanged length
// (VFS.UpdateLength): that keeps the record.
func (r *dataReader) jmTruncate(inode Ino, length uint64) {
	changed, readers := false, 0
	r.visit(inode, func(f *fileReader) {
		readers++
		if f.length != length {
			changed = true
		}
	})
	if changed || readers == 0 {
		r.jmServed.forget(inode)
	}
}

// jmRefreshChunks lists the chunks whose fresh list the refresh needs: those
// with a buffer it could keep or a recorded list.
func (r *dataReader) jmRefreshChunks(inode Ino) []uint32 {
	seen := map[uint32]bool{}
	var out []uint32
	add := func(indx uint32) {
		if !seen[indx] {
			seen[indx] = true
			out = append(out, indx)
		}
	}
	r.visit(inode, func(f *fileReader) {
		f.visit(func(s *sliceReader) bool {
			if s.jmSlices != nil && (s.state == BUSY || s.state == READY) {
				add(s.indx)
			}
			return true
		})
	})
	for _, indx := range r.jmServed.chunks(inode) {
		add(indx)
	}
	return out
}

// jmSameRange reports whether two slice lists of one chunk give the same
// bytes for the chunk positions [from, to).
func jmSameRange(a, b []meta.Slice, from, to uint32) bool {
	ra, rb := jmRange(a, from, to), jmRange(b, from, to)
	if len(ra) != len(rb) {
		return false
	}
	for i := range ra {
		if ra[i] != rb[i] {
			return false
		}
	}
	return true
}

// jmRange returns the pieces of a chunk's slice list (consecutive from
// position 0, holes as Id 0, as buildSlice makes it) that cover [from, to),
// cut to that range. A piece is the slice, its size and the part of it read.
func jmRange(ss []meta.Slice, from, to uint32) []meta.Slice {
	var out []meta.Slice
	var pos uint32
	for _, s := range ss {
		end := pos + s.Len
		if end > from && pos < to {
			lo, hi := max(pos, from), min(end, to)
			out = append(out, jmPiece(s, lo-pos, hi-lo))
		}
		pos = end
		if pos >= to {
			break
		}
	}
	return out
}

// jmPiece is n bytes of s from skip bytes into it. A hole reads zeros
// whatever its size.
func jmPiece(s meta.Slice, skip, n uint32) meta.Slice {
	if s.Id == 0 {
		return meta.Slice{Len: n}
	}
	return meta.Slice{Id: s.Id, Size: s.Size, Off: s.Off + skip, Len: n}
}

// jmChangedRanges returns the chunk positions [from, to) where old and fresh
// give different bytes. A nil old list stands for "unknown": the whole
// chunk. Past the end of either list nothing is known, so a range only one
// list covers counts as changed.
func jmChangedRanges(old, fresh []meta.Slice) [][2]uint32 {
	if old == nil {
		return [][2]uint32{{0, meta.ChunkSize}}
	}
	var out [][2]uint32
	mark := func(lo, hi uint32) {
		if n := len(out); n > 0 && out[n-1][1] == lo {
			out[n-1][1] = hi
			return
		}
		out = append(out, [2]uint32{lo, hi})
	}
	var i, j int
	var ia, jb uint32 // bytes used of old[i], fresh[j]
	var pos uint32
	for i < len(old) || j < len(fresh) {
		switch {
		case i < len(old) && j < len(fresh):
			n := min(old[i].Len-ia, fresh[j].Len-jb)
			if jmPiece(old[i], ia, n) != jmPiece(fresh[j], jb, n) {
				mark(pos, pos+n)
			}
			pos, ia, jb = pos+n, ia+n, jb+n
		case i < len(old):
			n := old[i].Len - ia
			mark(pos, pos+n)
			pos, ia = pos+n, ia+n
		default:
			n := fresh[j].Len - jb
			mark(pos, pos+n)
			pos, jb = pos+n, jb+n
		}
		if i < len(old) && ia == old[i].Len {
			i, ia = i+1, 0
		}
		if j < len(fresh) && jb == fresh[j].Len {
			j, jb = j+1, 0
		}
	}
	return out
}

// jmFreshChunks reads the slice lists of chunks from the engine, in
// parallel. A chunk whose read fails is left out: its buffers are dropped
// and the whole file is notified.
func jmFreshChunks(ctx meta.Context, m jmOpenFileRefresher, inode Ino, chunks []uint32) map[uint32][]meta.Slice {
	fresh := make(map[uint32][]meta.Slice, len(chunks))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, indx := range chunks {
		wg.Add(1)
		go func(indx uint32) {
			defer wg.Done()
			var ss []meta.Slice
			if m.JMReadChunkFresh(ctx, inode, indx, &ss) == 0 {
				mu.Lock()
				fresh[indx] = ss
				mu.Unlock()
			}
		}(indx)
	}
	wg.Wait()
	return fresh
}

func (v *VFS) jmRefreshMsg(ctx meta.Context, r *utils.Buffer, out io.Writer) {
	fail := func(eno syscall.Errno) { _, _ = out.Write([]byte{uint8(eno)}) }
	if r.Left() != 1+8 || r.Get8() != jmRefreshVersion {
		fail(syscall.EINVAL)
		return
	}
	inode := Ino(r.Get64())
	m, ok := v.Meta.(jmOpenFileRefresher)
	if !ok {
		fail(syscall.ENOTSUP)
		return
	}
	start := time.Now()
	var attr Attr
	if eno := m.JMRefreshOpenFile(ctx, inode, &attr); eno != 0 {
		fail(eno)
		return
	}
	metaDone := time.Now()
	length := attr.Length
	var chunks []uint32
	plan := jmRefreshPlan{whole: true}
	dr, isData := v.reader.(*dataReader)
	if attr.Typ == meta.TypeFile && isData {
		if v.writer != nil {
			if w := v.writer.GetLength(inode); w > length {
				length = w // our own write in flight
			}
		}
		// Patch 0032: the lists are read after JMRefreshOpenFile dropped the
		// cached ones.
		chunks = dr.jmRefreshChunks(inode)
		plan = dr.jmRefresh(inode, length, jmFreshChunks(ctx, m, inode, chunks))
	}
	buffersDone := time.Now()
	flags := v.jmRefreshKernel(inode, plan)
	if flags&JMRefreshKernelAccepted == 0 && isData {
		dr.jmRestoreServed(inode, plan)
	}
	// Patch 0032: the phases of a slow refresh, for the app's log beside it
	// (bug F1: the kernel notify waits for the file's reads in flight).
	if total := time.Since(start); total >= jmRefreshSlowLog {
		logger.Infof("refresh inode %d: %s (metadata %s, %d chunk lists %s, buffers kept %d dropped %d, kernel notify %s for %s, flags %d)",
			inode, total.Round(time.Millisecond), metaDone.Sub(start).Round(time.Millisecond), len(chunks),
			buffersDone.Sub(metaDone).Round(time.Millisecond), plan.kept, plan.dropped,
			time.Since(buffersDone).Round(time.Millisecond), plan.describe(), flags)
	}
	w := utils.NewBuffer(jmRefreshReplyOK)
	w.Put8(0)
	w.Put8(flags)
	w.Put64(length)
	w.Put64(uint64(attr.Mtime)*1e9 + uint64(attr.Mtimensec))
	_, _ = out.Write(w.Bytes())
}

func (p jmRefreshPlan) describe() string {
	switch {
	case p.whole:
		return "the whole file"
	case len(p.ranges) == 0:
		return "the attributes"
	}
	var n uint64
	for _, rg := range p.ranges {
		n += rg[1]
	}
	return fmt.Sprintf("%d ranges (%d bytes)", len(p.ranges), n)
}

// jmRefreshKernel sends the notifies of plan, all within
// jmRefreshNotifyWait.
func (v *VFS) jmRefreshKernel(inode Ino, plan jmRefreshPlan) uint8 {
	fp := jmInodeInvalidator.Load()
	if fp == nil {
		return JMRefreshKernelNone
	}
	var ranges [][2]int64
	switch {
	case plan.whole:
		ranges = [][2]int64{{0, 0}}
	case len(plan.ranges) == 0:
		ranges = [][2]int64{{-1, 0}}
	default:
		for _, rg := range plan.ranges {
			ranges = append(ranges, [2]int64{int64(rg[0]), int64(rg[1])})
		}
	}
	done := make(chan syscall.Errno, 1)
	go func() {
		for _, rg := range ranges {
			if eno := (*fp)(inode, rg[0], rg[1]); eno != 0 {
				done <- eno
				return
			}
		}
		done <- 0
	}()
	select {
	case eno := <-done:
		switch eno {
		case 0:
			return JMRefreshKernelAccepted
		case syscall.ENOSYS, syscall.ENOTSUP:
			return JMRefreshKernelNone
		default:
			logger.Warnf("refresh inode %d: kernel notify: %s", inode, eno)
			return JMRefreshKernelFailed
		}
	case <-time.After(jmRefreshNotifyWait):
		logger.Warnf("refresh inode %d: kernel notify did not answer in %s", inode, jmRefreshNotifyWait)
		return JMRefreshKernelTimeout
	}
}
