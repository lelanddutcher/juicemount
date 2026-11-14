package metadata

import (
	"context"
	"database/sql"
	"encoding/binary"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// ============================================================================
// A save's parts that commit after its final mtime (the mtime-span finding,
// remote lab and laptop, 2026-11-14; reviews of #99).
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
//    the path reads it (a SCAN reads mtime and size only) and the file's
//    slice signature when known. When the push sees a ctime other than the
//    recorded one (any other: ctimes come from different hosts' clocks) and
//    the mtime and size did not change, the slice signature decides: every
//    chunk list's length and its last slice (LLEN and LINDEX -1, in the same
//    pipeline as the attr MGET). A commit appends a slice to its chunk, so
//    the last slice changes; a chmod, chown, rename or link touches no chunk
//    list; a compaction rewrites the head of a list (if it merges the whole
//    list the last slice changes too, which over-counts). The same
//    signature: not a late commit. Another one, or none recorded (a restart,
//    a SCAN): a late commit, unless this process marked the inode for its
//    own change (chmod, drain, create, rename) and the signature cannot
//    decide; the mark is consumed by that one event. Observations are
//    evicted by age, never all at once, and only committed rows record one.
//
// 2. The reported mtime never repeats a value the Mac may have cached older
//    bytes under (nfsOverlays, table nfs_mtime_reported). When another
//    client changes the content of a known inode (a new mtime, a new size,
//    or a late commit), the reported mtime is the true one if that is above
//    every value reported for the inode. A true mtime more than an hour
//    below (a deliberate date, cp -p of an old file) is also reported as it
//    is, but only if no value reported for the inode equals it. Otherwise
//    (a late commit, juicefs stepping the mtime back, an old date coming
//    back: X, N, X) it is one nanosecond above the highest value reported.
//    The values are kept per inode (the last nfsReportedPerInode), keyed by
//    true mtime, persisted in the publication's transaction and loaded at
//    open (rows of inodes no longer mirrored are deleted first), so a
//    restart, a cache eviction and a row served from SQLite
//    (JM_SERVE_FROM_SQLITE=1) report the same value. A row not in the cache
//    takes its preimage from SQLite. The true mtime stays in Entry.Mtime for
//    every identity check against juicefs. The shown second is the second
//    of the highest value reported, which the Mac already showed; only a
//    value at x.999999999 moves it to the next second.
//
// APPS. Only another client's content changes move it away from the true
// mtime: this client's own writes are published by its own paths (the
// drainer, NFS), which report the true mtime. A teammate's save already
// moved the mtime at its first commit, which an app with the file open
// (Premiere's "modified since last save") already sees; the further moves
// belong to the same save.
// ============================================================================

// nfsReported is one value reported for an inode: reportedNS while its true
// mtime is trueNS (reportedNS == trueNS: reported as it is).
type nfsReported struct {
	trueNS, reportedNS int64
	owner              *Store // the Store that persists it
}

// nfsReportedPerInode bounds the values kept per inode (newest last).
const nfsReportedPerInode = 8

// nfsReportedLoadMax bounds the rows loaded at open (newest first).
const nfsReportedLoadMax = 1 << 18

// nfsOverlays is process-wide: Entry.NFSMtime has no Store. Every decision
// filters by the owning Store; keys include the true mtime, so an inode
// number another volume uses cannot match by chance.
var nfsOverlays = struct {
	mu sync.RWMutex
	m  map[uint64][]nfsReported
	n  atomic.Int64
}{m: map[uint64][]nfsReported{}}

// ReportedMtime is the mtime the NFS layer reports for inode while its true
// mtime is mtime.
func ReportedMtime(inode uint64, mtime time.Time) time.Time {
	if nfsOverlays.n.Load() == 0 || inode == 0 {
		return mtime
	}
	ns := mtime.UnixNano()
	nfsOverlays.mu.RLock()
	defer nfsOverlays.mu.RUnlock()
	list := nfsOverlays.m[inode]
	for i := len(list) - 1; i >= 0; i-- {
		if list[i].trueNS == ns {
			return time.Unix(0, list[i].reportedNS)
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

// hasOverlay reports whether owner keeps an overlay for (inode, trueNS).
func hasOverlay(owner *Store, inode uint64, trueNS int64) bool {
	nfsOverlays.mu.RLock()
	defer nfsOverlays.mu.RUnlock()
	for _, r := range nfsOverlays.m[inode] {
		if r.owner == owner && r.trueNS == trueNS {
			return true
		}
	}
	return false
}

// unsetReported forgets owner's overlay for (inode, trueNS): the value is
// reported as it is.
func unsetReported(owner *Store, inode uint64, trueNS int64) {
	nfsOverlays.mu.Lock()
	defer nfsOverlays.mu.Unlock()
	list := nfsOverlays.m[inode]
	for i := range list {
		if list[i].owner == owner && list[i].trueNS == trueNS {
			list = append(list[:i], list[i+1:]...)
			nfsOverlays.n.Add(-1)
			break
		}
	}
	if len(list) == 0 {
		delete(nfsOverlays.m, inode)
	} else {
		nfsOverlays.m[inode] = list
	}
}

func setReported(inode uint64, r nfsReported) {
	nfsOverlays.mu.Lock()
	defer nfsOverlays.mu.Unlock()
	list := nfsOverlays.m[inode]
	for i := range list {
		if list[i].owner == r.owner && list[i].trueNS == r.trueNS {
			list = append(list[:i], list[i+1:]...)
			nfsOverlays.n.Add(-1)
			break
		}
	}
	list = append(list, r)
	nfsOverlays.n.Add(1)
	mine := 0
	for _, x := range list {
		if x.owner == r.owner {
			mine++
		}
	}
	for i := 0; mine > nfsReportedPerInode && i < len(list); {
		if list[i].owner == r.owner {
			list = append(list[:i], list[i+1:]...)
			nfsOverlays.n.Add(-1)
			mine--
			continue
		}
		i++
	}
	nfsOverlays.m[inode] = list
}

// dropOverlaysOf forgets the values s published (s closes).
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

// overlayBackJump: a true mtime this far below what was reported is a
// deliberate date (cp -p of an old file onto this inode), not juicefs's
// commit order; it is reported as it is unless it was reported before.
const overlayBackJump = time.Hour

// nextReported is the reported mtime after another client changed the
// content of inode, whose reported mtime was oldReported, to true mtime m.
// list is what the inode reported before (nfs_mtime_reported, oldest
// first).
//
// One nanosecond above the highest value reported is never a value reported
// before, and never one of the file's earlier true mtimes (those were all
// reported, so they are at most the highest). A true mtime far below is
// reported as it is only if the inode's history does not hold it (review
// M2: X, N, X).
func nextReported(list []nfsReported, oldReported, m int64) int64 {
	high := oldReported
	seen := oldReported == m
	for _, r := range list {
		high = max64(high, r.reportedNS)
		if r.trueNS == m || r.reportedNS == m {
			seen = true
		}
	}
	switch {
	case m > high:
		return m
	case high-m > int64(overlayBackJump) && !seen:
		return m
	}
	return high + 1
}

// --- persistence ------------------------------------------------------------

// nfsReportedSchema: rowid orders each inode's values (INSERT OR REPLACE
// gives a replaced value a new rowid).
const nfsReportedSchema = `CREATE TABLE IF NOT EXISTS nfs_mtime_reported (
 inode INTEGER NOT NULL, true_ns INTEGER NOT NULL, reported_ns INTEGER NOT NULL,
 PRIMARY KEY (inode, true_ns))`

// loadNFSOverlays publishes the values s persisted (a restart reports the
// same mtimes as before it), after deleting those of inodes no longer in the
// mirror (review M4: juicefs does not reuse inode numbers).
func (s *Store) loadNFSOverlays() error {
	if _, err := s.db.Exec(`DELETE FROM nfs_mtime_reported WHERE inode NOT IN (SELECT inode FROM entries)`); err != nil {
		return err
	}
	rows, err := s.db.Query(`SELECT inode, true_ns, reported_ns FROM nfs_mtime_reported
 WHERE reported_ns != true_ns ORDER BY rowid DESC LIMIT ?`, nfsReportedLoadMax)
	if err != nil {
		return err
	}
	type row struct {
		inode uint64
		r     nfsReported
	}
	var all []row
	for rows.Next() {
		var inode int64
		r := nfsReported{owner: s}
		if err := rows.Scan(&inode, &r.trueNS, &r.reportedNS); err != nil {
			rows.Close()
			return err
		}
		all = append(all, row{uint64(inode), r})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for i := len(all) - 1; i >= 0; i-- {
		setReported(all[i].inode, all[i].r)
	}
	return nil
}

// reportedHistory is what inode reported before, oldest first (SQLite only:
// values reported as they are need no RAM).
func reportedHistory(tx *sql.Tx, inode uint64) ([]nfsReported, error) {
	rows, err := tx.Query(`SELECT true_ns, reported_ns FROM nfs_mtime_reported WHERE inode=? ORDER BY rowid`, int64(inode))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []nfsReported
	for rows.Next() {
		var r nfsReported
		if err := rows.Scan(&r.trueNS, &r.reportedNS); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// overlayChange is the values an inode reports now, written in the
// publication's transaction (oldest first) and applied to nfsOverlays after
// it commits.
type overlayChange struct {
	inode uint64
	rs    []nfsReported
}

func (c overlayChange) write(tx *sql.Tx) error {
	for _, r := range c.rs {
		if _, err := tx.Exec(`INSERT OR REPLACE INTO nfs_mtime_reported (inode, true_ns, reported_ns) VALUES (?, ?, ?)`,
			int64(c.inode), r.trueNS, r.reportedNS); err != nil {
			return err
		}
	}
	_, err := tx.Exec(`DELETE FROM nfs_mtime_reported WHERE inode=? AND rowid NOT IN
 (SELECT rowid FROM nfs_mtime_reported WHERE inode=? ORDER BY rowid DESC LIMIT ?)`,
		int64(c.inode), int64(c.inode), nfsReportedPerInode)
	return err
}

func (c overlayChange) apply() {
	for _, r := range c.rs {
		if r.reportedNS == r.trueNS {
			unsetReported(r.owner, c.inode, r.trueNS)
		} else {
			setReported(c.inode, r)
		}
	}
}

// --- slice signatures -------------------------------------------------------

// sliceSig is a file's chunk lists as the push read them: the number of
// slices and a hash of each list's length and last slice. count -1: not read.
type sliceSig struct {
	count int64
	tail  uint64
}

var unknownSig = sliceSig{count: -1}

func (a sliceSig) known() bool { return a.count >= 0 }

// sameContent: both read and equal (no commit between them).
func (a sliceSig) sameContent(b sliceSig) bool { return a.known() && b.known() && a == b }

// jfsChunkSize is juicefs's chunk (meta.ChunkSize): the chunk lists of a file
// are c<inode>_0 .. c<inode>_<(size-1)/jfsChunkSize>.
const jfsChunkSize = 64 << 20

// sliceSigMaxChunks bounds the chunk lists read for one file (4096 chunks is
// 256 GiB), sliceSigBudget those read for one push pipeline; past them the
// signature is not read (every ctime change then counts).
const (
	sliceSigMaxChunks = 4096
	sliceSigBudget    = 1 << 15
)

func chunksFor(size int64) int64 {
	if size <= 0 {
		return 1
	}
	return (size-1)/jfsChunkSize + 1
}

// sliceQuery is one file whose signature the push reads, for its cached
// size (the decoded size may differ; then the signature is not used).
type sliceQuery struct {
	inode uint64
	size  int64
}

type sigCmds struct {
	lens  []*redis.IntCmd
	tails []*redis.StringCmd
}

// queueSliceSigs adds the signature reads of files to pipe.
func queueSliceSigs(ctx context.Context, pipe redis.Pipeliner, files []sliceQuery) []sigCmds {
	out := make([]sigCmds, len(files))
	budget := sliceSigBudget
	for i, f := range files {
		n := chunksFor(f.size)
		if n > sliceSigMaxChunks || int(n) > budget {
			continue
		}
		budget -= int(n)
		for c := int64(0); c < n; c++ {
			key := "c" + strconv.FormatUint(f.inode, 10) + "_" + strconv.FormatInt(c, 10)
			out[i].lens = append(out[i].lens, pipe.LLen(ctx, key))
			out[i].tails = append(out[i].tails, pipe.LIndex(ctx, key, -1))
		}
	}
	return out
}

func (c sigCmds) result() sliceSig {
	if c.lens == nil {
		return unknownSig
	}
	h := fnv.New64a()
	var count int64
	var b [8]byte
	for i := range c.lens {
		n, err := c.lens[i].Result()
		if err != nil && err != redis.Nil {
			return unknownSig
		}
		tail, err := c.tails[i].Result()
		if err != nil && err != redis.Nil {
			return unknownSig
		}
		count += n
		binary.BigEndian.PutUint64(b[:], uint64(n))
		h.Write(b[:])
		binary.BigEndian.PutUint64(b[:], uint64(len(tail)))
		h.Write(b[:])
		h.Write([]byte(tail))
	}
	return sliceSig{count: count, tail: h.Sum64()}
}

// readAttrsAndSigs is the push's one round trip: the attrs of keys (MGET)
// and the slice signatures of files, in one pipeline.
func (rc *RedisClient) readAttrsAndSigs(ctx context.Context, keys []string, files []sliceQuery) ([]interface{}, []sliceSig, error) {
	sigs := make([]sliceSig, len(files))
	if rc.testReadAttrs != nil {
		vals, err := rc.testReadAttrs(keys)
		for i, f := range files {
			sigs[i] = unknownSig
			if rc.testReadSlices != nil {
				sigs[i] = rc.testReadSlices(f.inode, f.size)
			}
		}
		return vals, sigs, err
	}
	pipe := rc.redisDB().Pipeline()
	mget := pipe.MGet(ctx, keys...)
	cmds := queueSliceSigs(ctx, pipe, files)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		if vals, merr := mget.Result(); merr == nil {
			for i := range sigs {
				sigs[i] = unknownSig
			}
			return vals, sigs, nil
		}
		return nil, nil, err
	}
	vals, err := mget.Result()
	for i := range cmds {
		sigs[i] = cmds[i].result()
	}
	return vals, sigs, err
}

// --- observations -------------------------------------------------------------

// attrObs is what the last publication from Redis saw of a file.
type attrObs struct {
	mtimeNS      int64
	ctimeNS      int64 // valid unless ctimeUnknown (a SCAN reads mtime and size only)
	sig          sliceSig
	touched      time.Time
	ctimeUnknown bool
}

func (o attrObs) same(p attrObs) bool {
	return o.mtimeNS == p.mtimeNS && o.ctimeNS == p.ctimeNS && o.ctimeUnknown == p.ctimeUnknown && o.sig == p.sig
}

// attrObsMax caps the observations; past it the oldest untouched ones go. A
// late commit matters only during one save, so an observation untouched for
// attrObsIdle is never needed again.
const (
	attrObsMax  = 1 << 16
	attrObsIdle = 10 * time.Minute
)

// ownWindow: a change the attr push sees this soon after this process's own
// chmod, drain, create or rename of the file may be that change.
const ownWindow = 30 * time.Second

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
		return attrObs{sig: unknownSig}, false
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
	a.putLocked(inode, o, keepNewer)
}

func (a *attrObservations) putLocked(inode uint64, o attrObs, keepNewer bool) {
	if a.obs == nil {
		a.obs = make(map[uint64]*attrObs)
	}
	if prev, ok := a.obs[inode]; ok && keepNewer && prev.mtimeNS == o.mtimeNS {
		prev.touched = a.clock()
		return
	}
	if o.ctimeUnknown && !o.sig.known() {
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

// putIf records o only if inode's observation is still prev (review L4: a
// publication may have recorded a newer one since the push read it).
func (a *attrObservations) putIf(inode uint64, o, prev attrObs, havePrev bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	cur, ok := a.obs[inode]
	if ok != havePrev || (ok && !cur.same(prev)) {
		return
	}
	a.putLocked(inode, o, false)
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
			if now.Sub(t) > ownWindow {
				delete(a.own, k)
			}
		}
	}
	a.own[inode] = now
}

// takeOwn consumes inode's own-change mark: true if it was fresh.
func (a *attrObservations) takeOwn(inode uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	t, ok := a.own[inode]
	delete(a.own, inode)
	return ok && a.clock().Sub(t) <= ownWindow
}

// attrObservation is what the last publication from Redis saw of inode.
func (s *Store) attrObservation(inode uint64) (attrObs, bool) { return s.attrObs.get(inode) }

// NoteOwnAttrChange records that this process is about to change the mode or
// owner of path through juicefs (review M1 of #99).
func (s *Store) NoteOwnAttrChange(path string) { s.NoteOwnWrite(path) }

// NoteOwnWrite records that this process itself changes path in juicefs (a
// chmod, a create, a rename): when the push cannot tell its echo from
// another client's commit by the slice signature, the next event of the
// inode is taken for the echo.
func (s *Store) NoteOwnWrite(path string) {
	if s == nil {
		return
	}
	if e := s.LookupByPath(path); e != nil && !e.IsDir && e.Inode != 0 {
		s.attrObs.noteOwn(e.Inode)
	}
}

func (s *Store) noteOwnInode(inode uint64) {
	if inode != 0 {
		s.attrObs.noteOwn(inode)
	}
}

func (s *Store) takeOwnChange(inode uint64) bool { return s.attrObs.takeOwn(inode) }

// attrSpan is what a Redis publication read of a file besides its mtime and
// size: ctime is zero when not read (a SCAN reads mtime and size only).
// lateCommit: the attr push saw another client's commit under the same
// mtime and size.
type attrSpan struct {
	ctime      time.Time
	sig        sliceSig
	lateCommit bool
}

// spanPreimage is what bulkInsertBatchWithEpoch knew of a row before it.
type spanPreimage struct {
	mtimeNS, reportedNS, size int64
}

// mtimeSpanUpdate decides, for one committed Redis publication e of a file
// row whose preimage is old (present: the row was in the cache or in SQLite
// with the same inode) and whose history is list, the values to keep and the
// observation to record.
func (s *Store) mtimeSpanUpdate(e *Entry, old spanPreimage, present bool, list []nfsReported) (change *overlayChange, obs attrObs, keepNewer bool) {
	m := e.Mtime.UnixNano()
	sp := e.span
	e.span = nil // the row goes to the cache
	if sp == nil {
		sp = &attrSpan{sig: unknownSig}
	}
	obs = attrObs{mtimeNS: m, sig: sp.sig, ctimeUnknown: sp.ctime.IsZero()}
	if !sp.ctime.IsZero() {
		obs.ctimeNS = sp.ctime.UnixNano()
	}
	if !present {
		// A new path for this inode, with no preimage: its history says
		// what was reported for it.
		if len(list) == 0 && !sp.lateCommit {
			return nil, obs, false
		}
		for _, r := range list {
			if r.trueNS == m && !sp.lateCommit {
				return nil, obs, true // reported as before
			}
		}
		old = spanPreimage{mtimeNS: -1, reportedNS: ReportedMtime(e.Inode, e.Mtime).UnixNano()}
	} else if !sp.lateCommit && old.mtimeNS == m && old.size == e.Size {
		// The same attributes: a baseline only where none exists for them.
		return nil, obs, true
	}
	r := nextReported(list, old.reportedNS, m)
	c := &overlayChange{inode: e.Inode}
	if old.mtimeNS >= 0 && old.mtimeNS != m {
		known := false
		for _, h := range list {
			if h.trueNS == old.mtimeNS && h.reportedNS == old.reportedNS {
				known = true
			}
		}
		if !known {
			// The value being left joins the history (it may have been
			// published by a path that keeps none: an own write, the
			// first load), so it is never passed through again (review M2).
			c.rs = append(c.rs, nfsReported{trueNS: old.mtimeNS, reportedNS: old.reportedNS, owner: s})
		}
	}
	c.rs = append(c.rs, nfsReported{trueNS: m, reportedNS: r, owner: s})
	return c, obs, false
}

// sqlSpanPreimage reads the durable row of path inside tx (a row evicted
// from the cache): its true mtime, reported mtime and size.
func sqlSpanPreimage(tx *sql.Tx, path string, inode uint64) (spanPreimage, bool) {
	var (
		ino, isDir, size, secs int64
		nsec                   sql.NullInt64
	)
	if err := tx.QueryRow(`SELECT inode, is_dir, size, mtime, mtime_nsec FROM entries WHERE path=?`, path).
		Scan(&ino, &isDir, &size, &secs, &nsec); err != nil || uint64(ino) != inode || isDir != 0 {
		return spanPreimage{}, false
	}
	var e Entry
	e.restoreMtime(secs, nsec)
	return spanPreimage{mtimeNS: e.Mtime.UnixNano(), reportedNS: ReportedMtime(inode, e.Mtime).UnixNano(), size: size}, true
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// lateCommitVerdict judges an attr push observation of a row whose mtime and
// size did not change.
type lateCommitVerdict int

const (
	verdictNone      lateCommitVerdict = iota // the recorded ctime: nothing new
	verdictSignature                          // the slice signature decides
)

// judgeCtime: any ctime other than the recorded one is an event (review M3:
// ctimes are stamped by different hosts' clocks, so their order means
// nothing); so is one with nothing recorded.
func judgeCtime(prev attrObs, havePrev bool, mtimeNS, ctimeNS int64) lateCommitVerdict {
	if havePrev && prev.mtimeNS == mtimeNS && !prev.ctimeUnknown && prev.ctimeNS == ctimeNS {
		return verdictNone
	}
	return verdictSignature
}
