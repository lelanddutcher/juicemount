package metadata

import (
	"context"
	"database/sql"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// ============================================================================
// A save's parts that commit after its final mtime (the mtime-span finding,
// remote lab and laptop, 2026-11-14; review of #99).
//
// THE PROBLEM. juicefs gives every slice of an open file the time of its
// last write; a setattr of the mtime (cp -p, rsync -t, an app dating its
// file) sets that time on every slice still buffered (pkg/vfs/vfs_unix.go,
// UpdateMtime). Each commit (pkg/meta/redis.go doWrite) then writes the
// inode's mtime from its slice, without a max, and its ctime as the commit
// time. So a save's final mtime can reach Redis with its first commit while
// the other parts commit later and change only the ctime; the mtime can
// even step back (M, M', M) when a slice written after the setattr commits
// first. The NFS client on the Mac keeps a file's pages until it sees
// another mtime (a ctime-only change does not drop them: laptop run, 4 of
// 4), so parts it read early stayed old under the final mtime for good.
//
// THE FIX, in two parts.
//
// 1. Late commits are detected (reconcileFileAttrs). Every publication of a
//    file's attributes from Redis (the attr push, a folder reconcile, a SCAN,
//    the gap-fill) records what it read (attrObs): the mtime, the ctime when
//    the path reads it (a SCAN reads mtime and size only) and the slice
//    count when known. The attr push counts a newer ctime under the same
//    mtime as a late commit only with content evidence: the file's chunk
//    lists hold another number of slices than recorded (a commit appends a
//    slice; a chmod, chown, rename or link changes the ctime only). With no
//    recorded slice count (a restart, a SCAN) it counts it, since a late
//    commit must never be missed; this process's own chmod is marked
//    (NoteOwnAttrChange) and its own writes (noteOwnPublication) make the
//    next look a baseline. Observations are evicted by age, never all at
//    once, and only committed rows record one.
//
// 2. The reported mtime never repeats a value the Mac may have cached older
//    bytes under (nfsOverlays). When another client changes the content of a
//    known inode (a new mtime, a new size, or a late commit), the reported
//    mtime is the true one if that is above every value reported for the
//    inode; otherwise (a late commit, or juicefs stepping the mtime back:
//    M, M', M) it is one nanosecond above the highest value reported. The
//    overlay is persisted (table nfs_mtime_overlay) and keyed by inode and
//    true mtime, so a restart, a cache eviction and a row served from SQLite
//    (JM_SERVE_FROM_SQLITE=1) report the same value. The true mtime stays in
//    Entry.Mtime for every identity check against juicefs. The shown second
//    is the second of the highest value reported, which the Mac already
//    showed; only a value at x.999999999 moves it to the next second.
//
// APPS. Only another client's content changes move it away from the true
// mtime: this client's own writes are published by its own paths (the
// drainer, NFS), which report the true mtime, and their echo arrives under an
// mtime the push has not seen, which is only recorded. A teammate's save
// already moved the mtime at its first commit, which an app with the file
// open (Premiere's "modified since last save") already sees; the further
// moves belong to the same save.
// ============================================================================

// nfsOverlay is the reported mtime of an inode while its true mtime is
// trueNS. high is the highest value reported for the inode when it was set.
type nfsOverlay struct {
	trueNS, reportedNS, highNS int64
	owner                      *Store // the Store that persists it
}

// nfsOverlayPerInode bounds the overlays kept per inode (one per true mtime).
const nfsOverlayPerInode = 4

// nfsOverlays is process-wide: Entry.NFSMtime has no Store. Keys include the
// true mtime, so an inode number another volume uses cannot match by chance.
var nfsOverlays = struct {
	mu sync.RWMutex
	m  map[uint64][]nfsOverlay
	n  atomic.Int64
}{m: map[uint64][]nfsOverlay{}}

// ReportedMtime is the mtime the NFS layer reports for inode while its true
// mtime is mtime.
func ReportedMtime(inode uint64, mtime time.Time) time.Time {
	if nfsOverlays.n.Load() == 0 || inode == 0 {
		return mtime
	}
	ns := mtime.UnixNano()
	nfsOverlays.mu.RLock()
	defer nfsOverlays.mu.RUnlock()
	for _, o := range nfsOverlays.m[inode] {
		if o.trueNS == ns {
			return time.Unix(0, o.reportedNS)
		}
	}
	return mtime
}

// NFSMtime is the mtime the NFS layer reports for e (see ReportedMtime).
func (e *Entry) NFSMtime() time.Time {
	if e.IsDir {
		return e.Mtime
	}
	return ReportedMtime(e.Inode, e.Mtime)
}

func overlayFor(inode uint64, trueNS int64) (nfsOverlay, bool) {
	nfsOverlays.mu.RLock()
	defer nfsOverlays.mu.RUnlock()
	for _, o := range nfsOverlays.m[inode] {
		if o.trueNS == trueNS {
			return o, true
		}
	}
	return nfsOverlay{}, false
}

// overlayHigh is the highest value reported under any overlay of inode.
func overlayHigh(inode uint64) int64 {
	nfsOverlays.mu.RLock()
	defer nfsOverlays.mu.RUnlock()
	var h int64
	for _, o := range nfsOverlays.m[inode] {
		h = max64(h, o.highNS)
	}
	return h
}

func setOverlay(inode uint64, o nfsOverlay) {
	nfsOverlays.mu.Lock()
	defer nfsOverlays.mu.Unlock()
	list := nfsOverlays.m[inode]
	for i := range list {
		if list[i].trueNS == o.trueNS {
			list[i] = o
			return
		}
	}
	list = append(list, o)
	if len(list) > nfsOverlayPerInode {
		list = list[len(list)-nfsOverlayPerInode:]
	} else {
		nfsOverlays.n.Add(1)
	}
	nfsOverlays.m[inode] = list
}

// dropOverlaysOf forgets the overlays s published (s closes).
func dropOverlaysOf(s *Store) {
	nfsOverlays.mu.Lock()
	defer nfsOverlays.mu.Unlock()
	for inode, list := range nfsOverlays.m {
		kept := list[:0]
		for _, o := range list {
			if o.owner != s {
				kept = append(kept, o)
			}
		}
		nfsOverlays.n.Add(int64(len(kept) - len(list)))
		if len(kept) == 0 {
			delete(nfsOverlays.m, inode)
		} else {
			nfsOverlays.m[inode] = kept
		}
	}
}

// dropOverlays forgets inode's overlays (its true mtime moved above every
// value reported for it).
func dropOverlays(inode uint64) {
	nfsOverlays.mu.Lock()
	defer nfsOverlays.mu.Unlock()
	if l := nfsOverlays.m[inode]; len(l) > 0 {
		nfsOverlays.n.Add(-int64(len(l)))
		delete(nfsOverlays.m, inode)
	}
}

// overlayBackJump: a true mtime this far below what was reported is a
// deliberate date (cp -p of an old file onto this inode), not juicefs's
// commit order; it is reported as it is.
const overlayBackJump = time.Hour

// nextReported is the reported mtime after another client changed the
// content of inode, whose reported mtime was oldReported, to true mtime m.
// It returns the overlay to keep, or ok false: report m as it is (it is above
// every value reported for the inode).
//
// Otherwise (a late commit under the same mtime, or juicefs stepping the
// mtime back) it is one nanosecond above the highest value reported: never a
// value reported before, and never one of the file's earlier true mtimes
// (those were all reported, so they are at most the highest). Its second is
// the second of the highest value, which is the date the Mac has shown so
// far; only a high value at x.999999999 moves it to the next second.
func nextReported(owner *Store, inode uint64, oldReported, m int64) (o nfsOverlay, ok bool) {
	high := max64(oldReported, overlayHigh(inode))
	if m > high || high-m > int64(overlayBackJump) {
		return nfsOverlay{}, false
	}
	return nfsOverlay{trueNS: m, reportedNS: high + 1, highNS: high + 1, owner: owner}, true
}

// --- persistence ------------------------------------------------------------

const nfsOverlaySchema = `CREATE TABLE IF NOT EXISTS nfs_mtime_overlay (
 inode INTEGER NOT NULL, true_ns INTEGER NOT NULL, reported_ns INTEGER NOT NULL,
 high_ns INTEGER NOT NULL, PRIMARY KEY (inode, true_ns))`

// loadNFSOverlays publishes the overlays s persisted (a restart reports the
// same mtimes as before it).
func (s *Store) loadNFSOverlays() error {
	rows, err := s.db.Query(`SELECT inode, true_ns, reported_ns, high_ns FROM nfs_mtime_overlay`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var inode int64
		o := nfsOverlay{owner: s}
		if err := rows.Scan(&inode, &o.trueNS, &o.reportedNS, &o.highNS); err != nil {
			return err
		}
		setOverlay(uint64(inode), o)
	}
	return rows.Err()
}

// overlayChange is one inode's overlay update, written in the publication's
// transaction and applied to nfsOverlays after it commits.
type overlayChange struct {
	inode uint64
	o     nfsOverlay
	drop  bool
}

func (c overlayChange) write(tx *sql.Tx) error {
	if c.drop {
		_, err := tx.Exec(`DELETE FROM nfs_mtime_overlay WHERE inode=?`, int64(c.inode))
		return err
	}
	if _, err := tx.Exec(`INSERT OR REPLACE INTO nfs_mtime_overlay (inode, true_ns, reported_ns, high_ns) VALUES (?, ?, ?, ?)`,
		int64(c.inode), c.o.trueNS, c.o.reportedNS, c.o.highNS); err != nil {
		return err
	}
	// Keep the newest overlays of the inode only.
	_, err := tx.Exec(`DELETE FROM nfs_mtime_overlay WHERE inode=? AND true_ns NOT IN
 (SELECT true_ns FROM nfs_mtime_overlay WHERE inode=? ORDER BY high_ns DESC LIMIT ?)`,
		int64(c.inode), int64(c.inode), nfsOverlayPerInode)
	return err
}

func (c overlayChange) apply() {
	if c.drop {
		dropOverlays(c.inode)
	} else {
		setOverlay(c.inode, c.o)
	}
}

// --- observations -------------------------------------------------------------

// attrObs is what the last publication from Redis saw of a file.
type attrObs struct {
	mtimeNS      int64
	ctimeNS      int64 // 0: not read (a SCAN reads mtime and size only)
	slices       int64 // -1: not read
	touched      time.Time
	ctimeUnknown bool
	// own: this process published the row itself (Insert, InsertToCache):
	// the next look at this mtime is its echo, a baseline.
	own bool
}

// attrObsMax caps the observations; past it the oldest untouched ones go. A
// late commit matters only during one save, so an observation untouched for
// attrObsIdle is never needed again.
const (
	attrObsMax  = 1 << 16
	attrObsIdle = 10 * time.Minute
)

// ownAttrWindow: a ctime-only change this soon after this process's own
// chmod or chown of the file is that change.
const ownAttrWindow = 30 * time.Second

type attrObservations struct {
	mu  sync.Mutex
	obs map[uint64]*attrObs
	own map[uint64]time.Time
	now func() time.Time // tests
}

func (a *attrObservations) clock() time.Time {
	if a.now != nil {
		return a.now()
	}
	return time.Now()
}

// get returns inode's observation and marks it used (an active save's file
// stays when the map is over its cap).
func (a *attrObservations) get(inode uint64) (attrObs, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	o, ok := a.obs[inode]
	if !ok {
		return attrObs{}, false
	}
	o.touched = a.clock()
	return *o, true
}

// put records o for inode. keepNewer: an observation of the same mtime is not
// replaced (a folder reconcile must not consume a late commit the attr push
// has not judged yet).
func (a *attrObservations) put(inode uint64, o attrObs, keepNewer bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.obs == nil {
		a.obs = make(map[uint64]*attrObs)
	}
	if prev, ok := a.obs[inode]; ok && keepNewer && prev.mtimeNS == o.mtimeNS {
		prev.touched = a.clock()
		return
	}
	if o.ctimeUnknown && o.slices < 0 && !o.own {
		// Nothing to compare with later (a SCAN, the startup load): the
		// push judges the next ctime as unrecorded, so keep no entry.
		delete(a.obs, inode)
		return
	}
	o.touched = a.clock()
	if _, ok := a.obs[inode]; !ok && len(a.obs) >= attrObsMax {
		a.evictLocked()
	}
	a.obs[inode] = &o
}

// evictLocked drops observations idle for attrObsIdle; if none are, the
// least recently touched tenth.
func (a *attrObservations) evictLocked() {
	now := a.clock()
	for k, o := range a.obs {
		if now.Sub(o.touched) > attrObsIdle {
			delete(a.obs, k)
		}
	}
	if len(a.obs) < attrObsMax {
		return
	}
	type kt struct {
		k uint64
		t time.Time
	}
	all := make([]kt, 0, len(a.obs))
	for k, o := range a.obs {
		all = append(all, kt{k, o.touched})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].t.Before(all[j].t) })
	for _, x := range all[:len(all)/10+1] {
		delete(a.obs, x.k)
	}
}

func (a *attrObservations) noteOwn(inode uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.own == nil {
		a.own = make(map[uint64]time.Time)
	}
	now := a.clock()
	if len(a.own) > 4096 {
		for k, t := range a.own {
			if now.Sub(t) > ownAttrWindow {
				delete(a.own, k)
			}
		}
	}
	a.own[inode] = now
}

func (a *attrObservations) ownRecently(inode uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	t, ok := a.own[inode]
	return ok && a.clock().Sub(t) <= ownAttrWindow
}

// attrObservation is what the last publication from Redis saw of inode.
func (s *Store) attrObservation(inode uint64) (attrObs, bool) {
	o, ok := s.attrObs.get(inode)
	if !ok {
		o.slices = -1
	}
	return o, ok
}

// noteAttrObservation records an observation the attr push made without a
// publication (the row already matched).
func (s *Store) noteAttrObservation(inode uint64, mtime, ctime time.Time, slices int64) {
	s.attrObs.put(inode, attrObs{mtimeNS: mtime.UnixNano(), ctimeNS: ctime.UnixNano(), slices: slices}, false)
}

// NoteOwnAttrChange records that this process is about to change the mode or
// owner of path through juicefs: the ctime-only change the attr push sees
// next is not another client's commit (review M1 of #99).
func (s *Store) NoteOwnAttrChange(path string) {
	if s == nil {
		return
	}
	if e := s.LookupByPath(path); e != nil && !e.IsDir {
		s.attrObs.noteOwn(e.Inode)
	}
}

func (s *Store) ownAttrChange(inode uint64) bool { return s.attrObs.ownRecently(inode) }

// noteOwnPublication: this process publishes e itself (its own write, create
// or rename), so the attr push's next look at e's mtime is the echo of it.
func (s *Store) noteOwnPublication(e *Entry) {
	if e == nil || e.IsDir || e.Inode == 0 {
		return
	}
	s.attrObs.put(e.Inode, attrObs{mtimeNS: e.Mtime.UnixNano(), slices: -1, ctimeUnknown: true, own: true}, false)
}

// attrSpan is what a Redis publication read of a file besides its mtime and
// size: ctime is zero when not read (a SCAN reads mtime and size only);
// slices is -1 when not read. lateCommit: the attr push saw another client's
// commit under the same mtime and size.
type attrSpan struct {
	ctime      time.Time
	slices     int64
	lateCommit bool
}

// spanPreimage is what bulkInsertBatchWithEpoch knew of a row before it.
type spanPreimage struct {
	mtimeNS, reportedNS, size int64
	isDir                     bool
}

// mtimeSpanUpdate decides, for one committed Redis publication e of a row
// whose preimage is old (present: the row was in the cache with the same
// inode), the overlay change and the observation to record.
func (s *Store) mtimeSpanUpdate(e *Entry, old spanPreimage, present bool) (change *overlayChange, obs attrObs, keepNewer bool) {
	m := e.Mtime.UnixNano()
	sp := e.span
	e.span = nil // the row goes to the cache
	if sp == nil {
		sp = &attrSpan{slices: -1}
	}
	obs = attrObs{mtimeNS: m, slices: sp.slices, ctimeUnknown: sp.ctime.IsZero()}
	if !sp.ctime.IsZero() {
		obs.ctimeNS = sp.ctime.UnixNano()
	}
	if old.isDir {
		present = false
	}
	if !present {
		// Not in the cache (evicted, or a new path for this inode): the
		// overlays say what was reported for the inode.
		if _, found := overlayFor(e.Inode, m); found && !sp.lateCommit {
			return nil, obs, true // reported as before
		}
		high := overlayHigh(e.Inode)
		if high == 0 && !sp.lateCommit {
			return nil, obs, false
		}
		old = spanPreimage{mtimeNS: -1, reportedNS: max64(high, ReportedMtime(e.Inode, e.Mtime).UnixNano())}
	} else if !sp.lateCommit && old.mtimeNS == m && old.size == e.Size {
		// The same attributes: a baseline only where none exists for them.
		return nil, obs, true
	}
	if o, ok := nextReported(s, e.Inode, old.reportedNS, m); ok {
		return &overlayChange{inode: e.Inode, o: o}, obs, false
	}
	if overlayHigh(e.Inode) != 0 {
		return &overlayChange{inode: e.Inode, drop: true}, obs, false
	}
	return nil, obs, false
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// jfsChunkSize is juicefs's chunk (meta.ChunkSize): the chunk lists of a file
// are c<inode>_0 .. c<inode>_<(size-1)/jfsChunkSize>.
const jfsChunkSize = 64 << 20

// sliceCountMaxChunks bounds the chunk lists read for one file (4096 chunks
// is 256 GiB); past it the count is not read.
const sliceCountMaxChunks = 4096

// readSliceCounts returns the number of slices in the chunk lists of each
// file (-1 when not read). A commit appends a slice to a chunk list; a chmod,
// chown, rename or link does not touch them (review M1 of #99).
func (rc *RedisClient) readSliceCounts(ctx context.Context, files []sliceQuery) []int64 {
	out := make([]int64, len(files))
	for i := range out {
		out[i] = -1
	}
	if rc.testReadSlices != nil {
		for i, f := range files {
			out[i] = rc.testReadSlices(f.inode, f.size)
		}
		return out
	}
	db := rc.redisDB()
	if db == nil {
		return out
	}
	pipe := db.Pipeline()
	cmds := make([][]*redis.IntCmd, len(files))
	for i, f := range files {
		n := int64(1)
		if f.size > 0 {
			n = (f.size-1)/jfsChunkSize + 1
		}
		if n > sliceCountMaxChunks {
			continue
		}
		for c := int64(0); c < n; c++ {
			cmds[i] = append(cmds[i], pipe.LLen(ctx, "c"+strconv.FormatUint(f.inode, 10)+"_"+strconv.FormatInt(c, 10)))
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return out
	}
	for i, list := range cmds {
		if list == nil {
			continue
		}
		var total int64
		ok := true
		for _, c := range list {
			v, err := c.Result()
			if err != nil && err != redis.Nil {
				ok = false
				break
			}
			total += v
		}
		if ok {
			out[i] = total
		}
	}
	return out
}

type sliceQuery struct {
	inode uint64
	size  int64
}

// lateCommitVerdict judges an attr push observation of a row whose mtime and
// size did not change: ctime is what the push read, prev what the last
// publication or push saw.
type lateCommitVerdict int

const (
	verdictNone     lateCommitVerdict = iota // nothing new: record nothing
	verdictBaseline                          // record it, no content change
	verdictSlices                            // needs the slice count
)

func judgeCtime(prev attrObs, havePrev bool, mtimeNS, ctimeNS int64) lateCommitVerdict {
	if !havePrev {
		// Nothing recorded (a restart, an evicted observation): a ctime
		// event under the published mtime may be a late commit. The slice
		// count cannot tell, so it counts; at worst one extra refresh.
		return verdictSlices
	}
	if prev.mtimeNS != mtimeNS || prev.own {
		return verdictBaseline // first look at this mtime, or the echo of our own
	}
	if !prev.ctimeUnknown && ctimeNS <= prev.ctimeNS {
		return verdictNone
	}
	return verdictSlices
}
