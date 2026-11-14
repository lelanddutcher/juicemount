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
 *    ranges, a local write, a length change, or no complete record of the
 *    inode notify the whole file; see jmServedSet.
 *
 * Body: version(1), inode(8). Version 1 waits for the kernel notify (up to
 * jmRefreshNotifyWait). Version 2 (patch 0032) waits up to
 * jmRefreshAsyncWait, then answers flag 16 (Pending); the app then sends
 * version 3 with the same inode, which answers errno(1) and flags(1): the
 * final flags, or Pending again at once, or ENOENT when no
 * notify of the inode is pending. So the app's control channel, which its
 * drainer, pins and link follower share, is never held while a notify waits
 * for the reads of the file in flight (review M1 of #97).
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
	jmRefreshVersion      = 1
	jmRefreshVersionAsync = 2 // patch 0032: Pending instead of a long wait
	jmRefreshVersionPoll  = 3 // patch 0032: the flags of a pending notify
	jmRefreshReplyOK      = 1 + 1 + 8 + 8

	JMRefreshKernelAccepted = 1
	JMRefreshKernelNone     = 2
	JMRefreshKernelTimeout  = 4
	JMRefreshKernelFailed   = 8
	JMRefreshKernelPending  = 16
)

// jmRefreshAsyncWait: how long a version 2 refresh waits for its notify
// before it answers Pending. jmRefreshPollWait: how long a poll waits; 0
// answers at once, so a poll holds the app's control channel for one local
// exchange only (the app sleeps between polls).
var (
	jmRefreshAsyncWait = 250 * time.Millisecond
	jmRefreshPollWait  = time.Duration(0)
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
// of the file came from those lists.
//
// Only a complete record can make a refresh ranged. A record is complete
// once a refresh replaced it (swap) and the kernel accepted that refresh's
// notify (confirm): from then on, every page the kernel holds came from a
// list in it. A record that a read creates is partial: pages from before it
// (an earlier open, a local write, a reset) are in no list. A local write, a
// length change and a refused notify make the record partial again (review
// C1 of #97: after a forget, the next read made a fresh record of only the
// chunks read since, and a peer's change of page 0 got an attributes-only
// notify). The complete record also holds the file length it was made at;
// another length makes the refresh whole (review H1: macFUSE clamps a ranged
// notify to the kernel's old size, so the stale tail page of an append would
// stay).
type jmServedSet struct {
	mu    sync.Mutex
	m     map[Ino]*jmServedRecord
	epoch uint64
}

type jmServedRecord struct {
	chunks   map[uint32][][]meta.Slice
	complete bool
	length   uint64 // the file length when the record was made by swap
	epoch    uint64 // changes when the record goes partial or is replaced
}

// record notes that a buffer of chunk indx was read from ss. A nil list
// stands for "unknown": the whole chunk.
func (j *jmServedSet) record(inode Ino, indx uint32, ss []meta.Slice) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.m == nil || len(j.m) >= jmServedInodes && j.m[inode] == nil {
		j.m = make(map[Ino]*jmServedRecord) // every inode partial (absent) again
	}
	rec := j.m[inode]
	if rec == nil {
		j.epoch++
		rec = &jmServedRecord{chunks: make(map[uint32][][]meta.Slice), epoch: j.epoch} // partial
		j.m[inode] = rec
	}
	lists := rec.chunks[indx]
	for _, l := range lists {
		if l == nil || jmSameRange(l, ss, 0, meta.ChunkSize) {
			return
		}
	}
	if len(lists) >= jmServedMax || ss == nil {
		rec.chunks[indx] = [][]meta.Slice{nil}
		return
	}
	rec.chunks[indx] = append(lists, ss)
}

// jmKnown is ss as a known list: an empty chunk is an empty list, not nil.
func jmKnown(ss []meta.Slice) []meta.Slice {
	if ss == nil {
		return []meta.Slice{}
	}
	return ss
}

// forget makes the inode's record partial: the next refresh is whole.
func (j *jmServedSet) forget(inode Ino) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if rec := j.m[inode]; rec != nil {
		j.epoch++
		rec.complete, rec.epoch = false, j.epoch
	}
}

func (j *jmServedSet) chunks(inode Ino) []uint32 {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []uint32
	if rec := j.m[inode]; rec != nil {
		for indx := range rec.chunks {
			out = append(out, indx)
		}
	}
	return out
}

// swap returns the inode's record and replaces it with a pending one that
// holds, for each recorded chunk with a fresh list, that list alone: once the
// kernel drops the changed ranges, its pages hold what fresh describes. The
// pending record becomes complete when confirm gets its token. old is nil
// when the inode has no record.
func (j *jmServedSet) swap(inode Ino, fresh map[uint32][]meta.Slice, length uint64) (old *jmServedRecord, token uint64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.m == nil {
		j.m = make(map[Ino]*jmServedRecord)
	}
	old = j.m[inode]
	j.epoch++
	next := &jmServedRecord{chunks: make(map[uint32][][]meta.Slice), length: length, epoch: j.epoch}
	if old != nil {
		for indx := range old.chunks {
			if ss, have := fresh[indx]; have {
				next.chunks[indx] = [][]meta.Slice{ss}
			}
		}
	}
	j.m[inode] = next
	return old, j.epoch
}

// confirm makes the record swap left complete, unless something made it
// partial since (its epoch changed).
func (j *jmServedSet) confirm(inode Ino, token uint64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if rec := j.m[inode]; rec != nil && rec.epoch == token {
		rec.complete = true
	}
}

// jmRefreshPlan is what one refresh found: the kernel ranges to drop (file
// offset, length), or whole; and the token that confirms the new record.
type jmRefreshPlan struct {
	whole         bool
	ranges        [][2]uint64
	kept, dropped int
	token         uint64
	why           string // why the notify is whole
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
	whole := func(why string) {
		if !plan.whole {
			plan.whole, plan.why = true, why
		}
	}
	r.visit(inode, func(f *fileReader) {
		sameLength := f.length == length
		if !sameLength {
			whole("a reader's length changed")
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
	old, token := r.jmServed.swap(inode, fresh, length)
	plan.token = token
	switch {
	case old == nil:
		whole("no record")
	case !old.complete:
		whole("partial record")
	case old.length != length:
		whole("the length changed")
	}
	if plan.whole {
		return plan
	}
	for indx, lists := range old.chunks {
		ss, have := fresh[indx]
		if !have {
			whole("a chunk list could not be read")
			return plan
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
		whole("too many ranges")
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

// jmTruncate makes the inode's record partial when its length changes.
// Truncate also runs for every attribute reply with the unchanged length
// (VFS.UpdateLength): that keeps the record. A reply with a new length
// makes it partial even when a reader's length was already updated, so the
// next refresh is whole (the record's own length also differs then).
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
	if r.Left() != 1+8 {
		fail(syscall.EINVAL)
		return
	}
	version := r.Get8()
	inode := Ino(r.Get64())
	switch version {
	case jmRefreshVersion, jmRefreshVersionAsync:
	case jmRefreshVersionPoll:
		v.jmRefreshPoll(inode, out)
		return
	default:
		fail(syscall.EINVAL)
		return
	}
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
	plan := jmRefreshPlan{whole: true, why: "not a file"}
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
	job := v.jmStartKernel(inode, plan, func(flags uint8) {
		if isData && flags&(JMRefreshKernelAccepted|JMRefreshKernelNone) != 0 {
			dr.jmServed.confirm(inode, plan.token)
		}
		// Patch 0032: the phases of a slow refresh, for the app's log beside
		// it (bug F1: the kernel notify waits for the file's reads in flight).
		if total := time.Since(start); total >= jmRefreshSlowLog {
			logger.Infof("refresh inode %d: %s (metadata %s, %d chunk lists %s, buffers kept %d dropped %d, kernel notify %s for %s, flags %d)",
				inode, total.Round(time.Millisecond), metaDone.Sub(start).Round(time.Millisecond), len(chunks),
				buffersDone.Sub(metaDone).Round(time.Millisecond), plan.kept, plan.dropped,
				time.Since(buffersDone).Round(time.Millisecond), plan.describe(), flags)
		}
	})
	var flags uint8
	if version == jmRefreshVersion {
		flags = job.wait(jmRefreshNotifyWait)
		if flags == 0 {
			logger.Warnf("refresh inode %d: kernel notify did not answer in %s", inode, jmRefreshNotifyWait)
			flags = JMRefreshKernelTimeout
		}
	} else if flags = job.wait(jmRefreshAsyncWait); flags == 0 {
		// Review M1 of #97: the app's control channel is not held while the
		// notify waits for the reads in flight; it polls.
		jmNotifies.Store(jmNotifyKey{v, inode}, job)
		flags = JMRefreshKernelPending
	}
	w := utils.NewBuffer(jmRefreshReplyOK)
	w.Put8(0)
	w.Put8(flags)
	w.Put64(length)
	w.Put64(uint64(attr.Mtime)*1e9 + uint64(attr.Mtimensec))
	_, _ = out.Write(w.Bytes())
}

// jmRefreshPoll answers version 3: the kernel flags of the inode's pending
// notify, or Pending again (after jmRefreshPollWait, 0: at once). ENOENT:
// no notify of the inode is pending (it was already answered).
func (v *VFS) jmRefreshPoll(inode Ino, out io.Writer) {
	key := jmNotifyKey{v, inode}
	val, ok := jmNotifies.Load(key)
	if !ok {
		_, _ = out.Write([]byte{uint8(syscall.ENOENT)})
		return
	}
	job := val.(*jmNotifyJob)
	flags := job.wait(jmRefreshPollWait)
	if flags == 0 {
		flags = JMRefreshKernelPending
	} else {
		jmNotifies.CompareAndDelete(key, job)
	}
	_, _ = out.Write([]byte{0, flags})
}

type jmNotifyKey struct {
	v   *VFS
	ino Ino
}

// jmNotifies holds the notifies an async refresh left pending, for polls.
var jmNotifies sync.Map

// jmNotifyJob is one refresh's kernel notifies, running on their own
// goroutine. flags is set before done closes.
type jmNotifyJob struct {
	done  chan struct{}
	flags uint8
}

// wait returns the job's flags, or 0 if it did not end within d (at once
// for d 0).
func (j *jmNotifyJob) wait(d time.Duration) uint8 {
	if d <= 0 {
		select {
		case <-j.done:
			return j.flags
		default:
			return 0
		}
	}
	select {
	case <-j.done:
		return j.flags
	case <-time.After(d):
		return 0
	}
}

func (p jmRefreshPlan) describe() string {
	switch {
	case p.whole:
		return "the whole file (" + p.why + ")"
	case len(p.ranges) == 0:
		return "the attributes"
	}
	var n uint64
	for _, rg := range p.ranges {
		n += rg[1]
	}
	return fmt.Sprintf("%d ranges (%d bytes)", len(p.ranges), n)
}

// jmStartKernel sends the notifies of plan on a goroutine and calls finish
// with the flags when they end. A notify waits for the reads of its pages in
// flight, so it may take seconds; it is never cut short (the kernel answers
// it), only the reply stops waiting for it.
func (v *VFS) jmStartKernel(inode Ino, plan jmRefreshPlan, finish func(uint8)) *jmNotifyJob {
	job := &jmNotifyJob{done: make(chan struct{})}
	fp := jmInodeInvalidator.Load()
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
	go func() {
		flags := uint8(JMRefreshKernelAccepted)
		if fp == nil {
			flags = JMRefreshKernelNone
		} else {
			for _, rg := range ranges {
				eno := (*fp)(inode, rg[0], rg[1])
				if eno == 0 {
					continue
				}
				if eno == syscall.ENOSYS || eno == syscall.ENOTSUP {
					flags = JMRefreshKernelNone
				} else {
					logger.Warnf("refresh inode %d: kernel notify: %s", inode, eno)
					flags = JMRefreshKernelFailed
				}
				break
			}
		}
		job.flags = flags
		finish(flags)
		close(job.done)
	}()
	return job
}
