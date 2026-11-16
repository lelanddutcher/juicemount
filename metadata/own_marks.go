package metadata

import (
	"strings"
	"sync"
	"time"
)

// OWN MARKS: this process changed an inode itself, so the attr push must not
// take the echo of that change for another client's late commit
// (mtime_span.go). Two kinds, one mark per own operation:
//
//   - An echo mark (a chmod, an NFS rename, the stage of an app's temp-file
//     save renamed over the file) covers one ctime-only event whose slice
//     signature cannot decide. Any event of the inode uses it up; an event
//     whose row goes to a Store write uses it up only once that write
//     commits, and only the mark it was judged with (its generation), so a
//     row skipped because a foreground operation holds the path keeps the
//     mark, and a mark set during the write survives. While the path is
//     held, the deferral renews the mark, so its 30 s run from the release.
//   - A content mark (an in-place WRITE, an in-place drain) records the
//     exact (size, mtime) JuiceFS holds after the Mac's own change: the
//     FUSE file's mtime after the WRITE synced, or the mtime the drain set.
//     A row from Redis is the Mac's own bytes only on an exact match: then
//     its true mtime is reported (a changed row), or a later ctime-only part
//     under that mtime is the Mac's own. A teammate's save cannot carry the
//     Mac's exact mtime, so it always counts.
//
// Marks are kept in time order: each call drops the expired heads, so its
// cost is the number of marks that expired since, and past ownMarksMax the
// oldest go. A dropped mark costs at most one +1 ns over-count. The lock is
// a leaf: no caller holds it across anything else.

const (
	ownWindow        = 30 * time.Second
	ownContentWindow = 2 * time.Minute
	ownContentKeep   = 4
	ownMarksMax      = 1 << 14
	ownRingSize      = 4 * ownMarksMax // touches kept in time order
)

type ownWrite struct {
	size    int64
	mtimeNS int64
}

type ownMark struct {
	echoAt    time.Time
	echoGen   uint64
	contentAt time.Time
	writes    [ownContentKeep]ownWrite
	n, next   int
	touched   time.Time // the newest fifo entry of this mark
}

type ownTouch struct {
	inode uint64
	at    time.Time
}

type ownMarks struct {
	mu         sync.Mutex
	m          map[uint64]*ownMark
	ring       []ownTouch // the time order: oldest at head, count entries
	head, size int
	gen        uint64
	now        func() time.Time // tests
}

// init sizes the map for the cap and the time order as a fixed ring, so no
// call pays for a map or slice growing (review 99d H2). Store open calls it.
func (o *ownMarks) init() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.m == nil {
		o.initLocked()
	}
}

func (o *ownMarks) initLocked() {
	o.m = make(map[uint64]*ownMark, ownMarksMax)
	o.ring = make([]ownTouch, ownRingSize)
}

func (o *ownMarks) clock() time.Time {
	if o.now != nil {
		return o.now()
	}
	return time.Now()
}

// touchLocked returns inode's mark, creating it, and records the touch.
func (o *ownMarks) touchLocked(inode uint64, now time.Time) *ownMark {
	if o.m == nil {
		o.initLocked()
	}
	mk := o.m[inode]
	if mk == nil {
		mk = &ownMark{}
		o.m[inode] = mk
	}
	mk.touched = now
	if o.size == len(o.ring) {
		o.popLocked() // the ring is full: the oldest entry goes
	}
	o.ring[(o.head+o.size)%len(o.ring)] = ownTouch{inode, now}
	o.size++
	o.pruneLocked(now)
	return mk
}

// popLocked drops the oldest entry, and its mark unless it was touched
// again later.
func (o *ownMarks) popLocked() {
	t := o.ring[o.head]
	o.head = (o.head + 1) % len(o.ring)
	o.size--
	if mk := o.m[t.inode]; mk != nil && mk.touched.Equal(t.at) {
		delete(o.m, t.inode)
	}
}

// pruneLocked drops expired marks from the head of the time order, and the
// oldest past the cap. Its cost is the number of entries it drops.
func (o *ownMarks) pruneLocked(now time.Time) {
	for o.size > 0 && (len(o.m) > ownMarksMax || now.Sub(o.ring[o.head].at) > ownContentWindow) {
		o.popLocked()
	}
}

func (o *ownMarks) noteEcho(inode uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	now := o.clock()
	mk := o.touchLocked(inode, now)
	o.gen++
	mk.echoAt, mk.echoGen = now, o.gen
}

func (o *ownMarks) noteContent(inode uint64, size int64, mtime time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	now := o.clock()
	mk := o.touchLocked(inode, now)
	mk.contentAt = now
	mk.writes[mk.next] = ownWrite{size, mtime.UnixNano()}
	mk.next = (mk.next + 1) % ownContentKeep
	if mk.n < ownContentKeep {
		mk.n++
	}
}

func (o *ownMarks) echoLocked(inode uint64) *ownMark {
	mk := o.m[inode]
	if mk == nil || mk.echoGen == 0 || o.clock().Sub(mk.echoAt) > ownWindow {
		return nil
	}
	return mk
}

// echoGen is inode's fresh echo mark (0: none), not used up.
func (o *ownMarks) echoGen(inode uint64) uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	if mk := o.echoLocked(inode); mk != nil {
		return mk.echoGen
	}
	return 0
}

// takeEcho uses up inode's echo mark: true if it was fresh.
func (o *ownMarks) takeEcho(inode uint64) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	mk := o.m[inode]
	if mk == nil || mk.echoGen == 0 {
		return false
	}
	fresh := o.clock().Sub(mk.echoAt) <= ownWindow
	mk.echoAt, mk.echoGen = time.Time{}, 0
	return fresh
}

// takeEchoIf uses up inode's echo mark only if it is still generation gen.
func (o *ownMarks) takeEchoIf(inode uint64, gen uint64) {
	if gen == 0 {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if mk := o.m[inode]; mk != nil && mk.echoGen == gen {
		mk.echoAt, mk.echoGen = time.Time{}, 0
	}
}

// renewEcho restarts a fresh echo mark's window (its path is held).
func (o *ownMarks) renewEcho(inode uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if mk := o.echoLocked(inode); mk != nil {
		now := o.clock()
		mk.echoAt = now
		o.touchLocked(inode, now)
	}
}

// exact reports whether (size, mtime) is one the Mac's own change produced.
func (o *ownMarks) exact(inode uint64, size int64, mtime time.Time) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	mk := o.m[inode]
	if mk == nil || mk.n == 0 || o.clock().Sub(mk.contentAt) > ownContentWindow {
		return false
	}
	ns := mtime.UnixNano()
	for i := 0; i < mk.n; i++ {
		if w := mk.writes[i]; w.size == size && w.mtimeNS == ns {
			return true
		}
	}
	return false
}

func (o *ownMarks) len() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.m)
}

// ownFile is path's file row (an NFS path may carry a leading slash).
func (s *Store) ownFile(path string) *Entry {
	e := s.LookupByPath(path)
	if e == nil && strings.HasPrefix(path, "/") {
		e = s.LookupByPath(strings.TrimPrefix(path, "/"))
	}
	if e == nil || e.IsDir || e.Inode == 0 {
		return nil
	}
	return e
}

// NoteOwnAttrChange records that this process is about to change the mode or
// owner of path through juicefs (an echo mark).
func (s *Store) NoteOwnAttrChange(path string) { s.NoteOwnWrite(path) }

// NoteOwnWrite records an echo mark for path's inode (a chmod, a rename).
func (s *Store) NoteOwnWrite(path string) {
	if s == nil {
		return
	}
	if e := s.ownFile(path); e != nil {
		s.own.noteEcho(e.Inode)
	}
}

// NoteOwnInode records an echo mark for an inode the mirror may not show yet
// (the stage of a temp-file save, before its rename over the file).
func (s *Store) NoteOwnInode(inode uint64) {
	if s != nil && inode != 0 {
		s.own.noteEcho(inode)
	}
}

// NoteOwnContent records the exact size and mtime JuiceFS holds for path
// after this process's own change of its bytes (a content mark).
func (s *Store) NoteOwnContent(path string, size int64, mtime time.Time) {
	if s == nil {
		return
	}
	if e := s.ownFile(path); e != nil {
		s.own.noteContent(e.Inode, size, mtime)
	}
}

func (s *Store) takeOwnChange(inode uint64) bool { return s.own.takeEcho(inode) }

func (s *Store) ownEchoGen(inode uint64) uint64 { return s.own.echoGen(inode) }

func (s *Store) renewOwnEcho(inode uint64) { s.own.renewEcho(inode) }

// ownBytes: Redis's size and mtime of inode are exactly the Mac's own.
func (s *Store) ownBytes(inode uint64, size int64, mtime time.Time) bool {
	return s.own.exact(inode, size, mtime)
}

// settleOwnMarks runs once a push row e is published (not skipped as
// owned): the event used up the echo mark it was judged with (review N4).
func (s *Store) settleOwnMarks(e *Entry) {
	if e.span != nil {
		s.own.takeEchoIf(e.Inode, e.span.echoGen)
	}
}
