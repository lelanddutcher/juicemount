package nfs

// Spooled folder creation.
//
// WHY. NFS MKDIR used to create the folder on the JuiceFS mount before it
// answered. On a distant link that is several Redis round trips (a lookup, a
// transaction, a getattr, a chmod), and on the laptop lab (200 ms) it passed
// the 2 s slow-link mutation deadline: the RPC answered JUKEBOX, macOS waited
// its 4 s retry delay, and every mkdir froze the app for 6.0 s.
//
// NOW. MKDIR records the folder in the mirror and in the spool database
// (pending_dirs) and answers at local speed. The drainer creates it on the
// volume later, parents before children, and before any file inside it.
//
// THE FILE ID. macOS treats a changed file id on a folder it holds as a stale
// handle (ESTALE), and that broke bundle copies once (#70: "real inode at
// birth"). So a spooled folder is born with its final inode: the drainer
// reserves numbers from the running juicefs ahead of time (patch 0022,
// JMReserveInodes) and creates the folder with that exact number
// (JMMkdirInode). Without a reserved number (an old juicefs, an empty pool,
// offline) MKDIR keeps the synchronous path, unchanged.
//
// RULES KEPT.
//   - Project leases (H-8B): the drainer takes the project's lease before it
//     creates a folder, as it does before a file; a folder in a project
//     another Mac holds stays on this Mac until the project is free. The
//     RESP proxy still checks the create itself (same transaction as Mkdir).
//   - Identity gate (V2.3 G0): nothing is sent to a mount that fails it.
//   - Namespace ownership: the drainer creates a folder under the same
//     namespace lease the file install takes, so a rename or remove of the
//     folder, or of a parent, never interleaves with its creation.
//
// CONFLICTS.
//   - The name already exists on the volume as a folder: the drainer adopts
//     it. Its inode differs, so the mirror takes the real one (the file id
//     changes once, as for any folder another Mac created first).
//   - The name exists as something else: logged, the row is dropped, and the
//     files inside fail their drain visibly (status shows them).
//   - A parent was removed remotely: the drainer recreates the parent chain,
//     as a file drain already does with os.MkdirAll.
//   - Renamed or removed before it drains: rename moves the pending rows,
//     rmdir drops the row; neither touches the volume.
//   - Offline: rows wait; the drainer is parked until the link is back.

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/metadata"
)

// spooledMkdirEnabled: JM_SPOOL_MKDIR=0 restores synchronous MKDIR.
func spooledMkdirEnabled() bool { return os.Getenv("JM_SPOOL_MKDIR") != "0" }

func syntheticInode(ino uint64) bool { return ino&(1<<63) != 0 }

// ---------------------------------------------------------------------------
// Pending folder set (SpoolStore)

type pendingDirSet struct {
	mu     sync.RWMutex
	byPath map[string]metadata.PendingDir // nil until loaded
}

func (s *SpoolStore) pendingDirsLocked() (map[string]metadata.PendingDir, error) {
	if s.dirs.byPath != nil {
		return s.dirs.byPath, nil
	}
	rows, err := s.meta.ListPendingDirs()
	if err != nil {
		return nil, err
	}
	m := make(map[string]metadata.PendingDir, len(rows))
	for _, r := range rows {
		m[r.Path] = r
	}
	// waiting_folder marks are a cache of this set: drop stale ones.
	if err := s.meta.ClearStaleFolderWaits(); err != nil {
		return nil, err
	}
	s.dirs.byPath = m
	return m, nil
}

// loadPendingDirs reads the durable rows once; callers that see an error keep
// the synchronous behavior.
func (s *SpoolStore) loadPendingDirs() error {
	if s == nil {
		return os.ErrInvalid
	}
	s.dirs.mu.Lock()
	defer s.dirs.mu.Unlock()
	_, err := s.pendingDirsLocked()
	return err
}

func (s *SpoolStore) addPendingDir(dirPath string, inode uint64) error {
	s.dirs.mu.Lock()
	defer s.dirs.mu.Unlock()
	m, err := s.pendingDirsLocked()
	if err != nil {
		return err
	}
	id, err := s.meta.PutPendingDir(dirPath, inode)
	if err != nil {
		return err
	}
	m[dirPath] = metadata.PendingDir{ID: id, Path: dirPath, Inode: inode, CreatedAt: time.Now()}
	return nil
}

// pendingDir returns the row of a folder not yet on the volume.
func (s *SpoolStore) pendingDir(dirPath string) (metadata.PendingDir, bool) {
	if s == nil {
		return metadata.PendingDir{}, false
	}
	s.dirs.mu.RLock()
	defer s.dirs.mu.RUnlock()
	p, ok := s.dirs.byPath[dirPath]
	return p, ok
}

// pendingAncestor reports whether a proper ancestor of p is a pending folder.
// pendingFolderOf returns the row id of the innermost pending folder that p
// is inside, for the waiting marks of ListReadySkipping.
func (s *SpoolStore) pendingFolderOf(p string) (int64, bool) {
	if s == nil {
		return 0, false
	}
	s.dirs.mu.RLock()
	defer s.dirs.mu.RUnlock()
	if len(s.dirs.byPath) == 0 {
		return 0, false
	}
	for d := path.Dir(strings.TrimPrefix(p, "/")); d != "." && d != "/" && d != ""; d = path.Dir(d) {
		if r, ok := s.dirs.byPath[d]; ok {
			return r.ID, true
		}
	}
	return 0, false
}

// pendingAtOrAbove reports whether p or one of its ancestors is pending.
func (s *SpoolStore) pendingAtOrAbove(p string) bool {
	if _, ok := s.pendingDir(p); ok {
		return true
	}
	return s.pendingAncestor(p)
}

func (s *SpoolStore) pendingAncestor(p string) bool {
	if s == nil {
		return false
	}
	s.dirs.mu.RLock()
	defer s.dirs.mu.RUnlock()
	if len(s.dirs.byPath) == 0 {
		return false
	}
	for d := path.Dir(strings.TrimPrefix(p, "/")); d != "." && d != "/" && d != ""; d = path.Dir(d) {
		if _, ok := s.dirs.byPath[d]; ok {
			return true
		}
	}
	return false
}

// pendingAtOrUnder reports whether p, or a folder beneath it, is pending.
func (s *SpoolStore) pendingAtOrUnder(p string) bool {
	if s == nil {
		return false
	}
	s.dirs.mu.RLock()
	defer s.dirs.mu.RUnlock()
	for d := range s.dirs.byPath {
		if metadata.PathAtOrUnder(d, p) {
			return true
		}
	}
	return false
}

// pendingAtOrUnderExcept reports whether a folder strictly beneath p is
// pending.
func (s *SpoolStore) pendingAtOrUnderExcept(p string) bool {
	if s == nil {
		return false
	}
	s.dirs.mu.RLock()
	defer s.dirs.mu.RUnlock()
	for d := range s.dirs.byPath {
		if d != p && metadata.PathAtOrUnder(d, p) {
			return true
		}
	}
	return false
}

func (s *SpoolStore) pendingDirCount() int {
	if s == nil {
		return 0
	}
	s.dirs.mu.RLock()
	defer s.dirs.mu.RUnlock()
	return len(s.dirs.byPath)
}

// listPendingDirs returns the pending folders, oldest row first.
func (s *SpoolStore) listPendingDirs() []metadata.PendingDir {
	s.dirs.mu.RLock()
	out := make([]metadata.PendingDir, 0, len(s.dirs.byPath))
	for _, p := range s.dirs.byPath {
		out = append(out, p)
	}
	s.dirs.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// removePendingDir drops row p if it is still the row of its path.
func (s *SpoolStore) removePendingDir(p metadata.PendingDir) error {
	s.dirs.mu.Lock()
	defer s.dirs.mu.Unlock()
	if _, err := s.meta.DeletePendingDir(p.Path, p.ID); err != nil {
		if !errors.Is(err, metadata.ErrPendingDirMissing) {
			return err
		}
		// Memory and SQLite disagree. Follow SQLite (what a restart loads)
		// and say so: this must never pass silently.
		jmlog.Warn("pending folder row missing from the database; dropping it from memory",
			"path", p.Path, "id", p.ID)
	}
	// Files that waited for this folder may drain now.
	if err := s.meta.ClearFolderWaitsFor(p.ID); err != nil {
		jmlog.Warn("clear folder waits", "path", p.Path, "error", err.Error())
	}
	if cur, ok := s.dirs.byPath[p.Path]; ok && cur.ID == p.ID {
		delete(s.dirs.byPath, p.Path)
	}
	return nil
}

// movePendingDirsInMemoryLocked re-keys the in-memory rows at and beneath
// oldPath after the durable move (MigrateActivePathsWithBase). Caller holds
// s.dirs.mu.
func (s *SpoolStore) movePendingDirsInMemoryLocked(oldPath, newPath string) {
	if oldPath == newPath || len(s.dirs.byPath) == 0 {
		return
	}
	next := make(map[string]metadata.PendingDir, len(s.dirs.byPath))
	for d, p := range s.dirs.byPath {
		if metadata.PathAtOrUnder(d, oldPath) {
			p.Path = newPath + d[len(oldPath):]
		}
		next[p.Path] = p
	}
	s.dirs.byPath = next
}

// ---------------------------------------------------------------------------
// Reserved folder inodes (Drainer)

const (
	dirInodeBatch = 256
	dirInodeLow   = 64
)

type inodeRange struct{ next, end uint64 }

// dirInodePool holds folder inode numbers reserved in the running juicefs.
// A refill is one control message (one Redis round trip) per 256 folders.
type dirInodePool struct {
	ctl *jfsControl

	mu        sync.Mutex
	ranges    []inodeRange
	refilling chan struct{} // non-nil while a refill runs; closed at its end
	disabled  bool          // the juicefs does not support the messages
	retryAt   time.Time     // after a failed refill
	capLogged bool          // the juicefs cap was reported (once per episode)
	closed    bool          // the drainer stopped: no more numbers are taken or reserved
}

// close stops the pool: MKDIR after the drainer stopped takes the
// synchronous path instead of reserving numbers nobody will give back.
func (p *dirInodePool) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
}

func newDirInodePool(ctl *jfsControl) *dirInodePool { return &dirInodePool{ctl: ctl} }

func (p *dirInodePool) availableLocked() uint64 {
	var n uint64
	for _, r := range p.ranges {
		n += r.end - r.next
	}
	return n
}

// take returns a reserved number, waiting up to wait for a refill when the
// pool is empty. ok=false means MKDIR must take the synchronous path.
func (p *dirInodePool) take(wait time.Duration) (uint64, bool) {
	if p == nil {
		return 0, false
	}
	deadline := time.Now().Add(wait)
	for {
		p.mu.Lock()
		if p.disabled || p.closed {
			p.mu.Unlock()
			return 0, false
		}
		for len(p.ranges) > 0 && p.ranges[0].next >= p.ranges[0].end {
			p.ranges = p.ranges[1:]
		}
		if len(p.ranges) > 0 {
			ino := p.ranges[0].next
			p.ranges[0].next++
			low := p.availableLocked() < dirInodeLow
			p.mu.Unlock()
			if low {
				p.refill()
			}
			return ino, true
		}
		done := p.startRefillLocked()
		p.mu.Unlock()
		remaining := time.Until(deadline)
		if done == nil || remaining <= 0 {
			return 0, false
		}
		select {
		case <-done:
		case <-time.After(remaining):
			return 0, false
		}
	}
}

// topUp starts a background reservation when the pool runs low, so the
// next MKDIR finds a number without waiting.
func (p *dirInodePool) topUp() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.availableLocked() < dirInodeLow {
		p.startRefillLocked()
	}
	p.mu.Unlock()
}

// refill starts a background reservation if none runs.
func (p *dirInodePool) refill() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.startRefillLocked()
	p.mu.Unlock()
}

func (p *dirInodePool) startRefillLocked() chan struct{} {
	if p.disabled || p.closed {
		return nil
	}
	if p.refilling != nil {
		return p.refilling
	}
	if time.Now().Before(p.retryAt) || pin.IsOffline() || !pin.FUSEIdentityOK() {
		return nil
	}
	done := make(chan struct{})
	p.refilling = done
	go func() {
		first, count, err := p.ctl.reserveInodes(dirInodeBatch)
		p.mu.Lock()
		defer p.mu.Unlock()
		switch {
		case errors.Is(err, errControlUnsupported):
			p.disabled = true
			jmlog.Info("spooled mkdir off: the juicefs does not reserve folder inodes; MKDIR stays synchronous")
		case errors.Is(err, syscall.EAGAIN):
			// juicefs holds its maximum of reserved numbers (leaked by
			// earlier crashes of this app). MKDIR stays synchronous until
			// juicefs restarts; check again in a minute.
			p.retryAt = time.Now().Add(time.Minute)
			if !p.capLogged {
				p.capLogged = true
				jmlog.Info("juicefs holds its maximum of reserved folder inodes; MKDIR is synchronous until juicefs restarts")
			}
		case err != nil:
			p.retryAt = time.Now().Add(5 * time.Second)
			jmlog.Debug("reserve folder inodes failed", "error", err.Error())
		case count > 0 && p.closed:
			// Stopped while this refill ran: give the numbers back.
			go func() { _, _ = p.ctl.releaseInodes(first, count, 2*time.Second) }()
		case count > 0:
			p.capLogged = false
			p.ranges = append(p.ranges, inodeRange{next: first, end: first + uint64(count)})
		}
		p.refilling = nil
		close(done)
	}()
	return done
}

// invalidate empties the pool after juicefs refused one of its numbers. The
// unused numbers are given back, so a juicefs that did not restart does not
// keep them reserved (its cap is 65,536); a reply of 0 released confirms the
// restart.
func (p *dirInodePool) invalidate() {
	if p == nil {
		return
	}
	held, released, err := p.releaseAll(p.ctl.replyTimeout)
	switch {
	case err != nil:
		jmlog.Info("reserved folder inodes dropped; returning them to juicefs failed", "numbers", held, "error", err.Error())
	case held > 0 && released == 0:
		jmlog.Info("juicefs restarted: its reserved folder inodes are void", "numbers", held)
	default:
		jmlog.Info("reserved folder inodes returned to juicefs", "numbers", held, "released", released)
	}
}

// releaseAll empties the pool and gives its unused numbers back to juicefs.
func (p *dirInodePool) releaseAll(timeout time.Duration) (held uint64, released uint64, err error) {
	if p == nil {
		return 0, 0, nil
	}
	p.mu.Lock()
	ranges := p.ranges
	p.ranges = nil
	p.mu.Unlock()
	for _, r := range ranges {
		for next := r.next; next < r.end; {
			n := r.end - next
			if n > dirInodeReleaseMax {
				n = dirInodeReleaseMax
			}
			held += n
			got, rerr := p.ctl.releaseInodes(next, uint32(n), timeout)
			if rerr != nil && err == nil {
				err = rerr
			}
			released += uint64(got)
			next += n
		}
	}
	return held, released, err
}

// stillHeld reports whether juicefs still holds the pool's numbers, by giving
// one of them back: released means held (not restarted). An empty pool
// reports true: there is nothing to drop.
func (p *dirInodePool) stillHeld() bool {
	p.mu.Lock()
	for len(p.ranges) > 0 && p.ranges[0].next >= p.ranges[0].end {
		p.ranges = p.ranges[1:]
	}
	if len(p.ranges) == 0 {
		p.mu.Unlock()
		return true
	}
	ino := p.ranges[0].next
	p.ranges[0].next++
	p.mu.Unlock()
	released, err := p.ctl.releaseInodes(ino, 1, p.ctl.replyTimeout)
	return err == nil && released == 1
}

// definiteMkdirRefusal mirrors jmDefiniteRefusal in the juicefs patch: after
// these the engine wrote nothing and the number stays reserved.
func definiteMkdirRefusal(err error) bool {
	for _, e := range []syscall.Errno{syscall.EEXIST, syscall.ENOENT, syscall.ENOTDIR, syscall.EACCES,
		syscall.EPERM, syscall.EROFS, syscall.EINVAL, syscall.ENAMETOOLONG, syscall.EDQUOT, syscall.ENOSPC} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

// dirInodeReleaseMax bounds one release message.
const dirInodeReleaseMax = 4096

// ---------------------------------------------------------------------------
// MKDIR (handler)

// spoolMkdir answers MKDIR locally when it can. handled=false means the
// caller must create the folder on the volume now, as before.
func (jfs *juiceFS) spoolMkdir(dirname string, perm os.FileMode, op *namespaceOperation) (handled bool, err error) {
	h := jfs.handler
	spool := h.spool.Load()
	drainer := h.drainer.Load()
	if spool == nil || drainer == nil || drainer.dirInodes == nil || !spooledMkdirEnabled() || pin.IsOffline() {
		return false, nil
	}
	parent := path.Dir(dirname)
	if parent != "." {
		pe := h.store.LookupByPath(parent)
		if pe == nil || !pe.IsDir {
			return false, nil // the mirror does not vouch for the parent
		}
	}
	if spool.loadPendingDirs() != nil {
		return false, nil
	}
	// Half the mutation deadline: a refill is one round trip; if it takes
	// longer, the synchronous create would not be faster.
	ino, ok := drainer.dirInodes.take(mutationOpTimeout() / 2)
	if !ok {
		return false, nil
	}
	// Durable before visible, as for an offline symlink: a crash between the
	// two leaves a row to create, never a folder that vanishes on restart.
	if err := spool.addPendingDir(dirname, ino); err != nil {
		return true, err
	}
	op.mutated = true
	e := metadata.MakeEntry(dirname, true, 0, time.Now(), ino)
	e.Mode = os.ModeDir | perm.Perm()
	e.LocalOnly = true
	op.checkpoint("publish", dirname)
	h.noteSingleChildInsert(dirname)
	if err := h.store.Insert(e); err != nil {
		return true, err
	}
	// Other Macs learn of the folder when it exists on the volume
	// (onDirMaterialized), not before.
	spool.SignalReady()
	return true, nil
}

// onDirMaterialized runs once a pending folder exists on the volume.
func (h *JuiceMountHandler) onDirMaterialized(dirPath string, inode uint64) {
	if err := h.store.ClearLocalOnly(dirPath); err != nil {
		jmlog.Warn("folder created on the volume; clear local_only failed", "path", dirPath, "error", err.Error())
	}
	h.publishEvent(metadata.MetadataEvent{Op: "create", Path: dirPath, Mtime: time.Now().Unix(), Inode: inode, IsDir: true})
}

// ---------------------------------------------------------------------------
// Creating pending folders on the volume (Drainer)

// materializePendingDirs creates every pending folder it can, parents first.
// It returns true while folders remain (a lease, a failure or the link).
func (d *Drainer) materializePendingDirs() bool {
	if d.spool.loadPendingDirs() != nil {
		return true
	}
	if d.spool.pendingDirCount() == 0 {
		return false
	}
	created := false
	remaining := false
	dirs := d.spool.listPendingDirs()
	for len(dirs) > 0 {
		pending := make(map[string]bool, len(dirs))
		for _, p := range dirs {
			pending[p.Path] = true
		}
		var ready, later []metadata.PendingDir
		for _, p := range dirs {
			if hasAncestorIn(pending, p.Path) {
				later = append(later, p)
			} else {
				ready = append(ready, p)
			}
		}
		if len(ready) == 0 {
			break
		}
		ok := make([]bool, len(ready))
		var wg sync.WaitGroup
		sem := make(chan struct{}, 8)
		for i, p := range ready {
			if !d.dirRetry.due(p.ID) {
				continue // backing off after repeated failures (ok[i] false)
			}
			wg.Add(1)
			sem <- struct{}{}
			go func(i int, p metadata.PendingDir) {
				defer func() { <-sem; wg.Done() }()
				ok[i] = d.materializeDir(p)
			}(i, p)
		}
		wg.Wait()
		failed := map[string]bool{}
		for i, p := range ready {
			if ok[i] {
				created = true
			} else {
				failed[p.Path] = true
				remaining = true
			}
		}
		dirs = dirs[:0]
		for _, p := range later {
			if !hasAncestorIn(failed, p.Path) {
				dirs = append(dirs, p)
			}
		}
	}
	if created {
		// Links made inside a folder that was pending wait for it.
		d.symlinkWake.Store(true)
	}
	return remaining
}

// dirRetryBackoff spaces out attempts at a folder that keeps failing (a
// project held by another Mac, a refused create): the first retry is
// immediate, then 1 s doubling to 5 min. Keyed by row id, which a rename
// keeps.
type dirRetryBackoff struct {
	mu   sync.Mutex
	rows map[int64]dirRetryState
}

type dirRetryState struct {
	failures int
	nextAt   time.Time
}

func (b *dirRetryBackoff) due(id int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	st, ok := b.rows[id]
	return !ok || !time.Now().Before(st.nextAt)
}

func (b *dirRetryBackoff) failed(id int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rows == nil {
		b.rows = make(map[int64]dirRetryState)
	}
	st := b.rows[id]
	st.failures++
	var wait time.Duration
	if st.failures >= 2 {
		wait = time.Second << min(st.failures-2, 9)
		if wait > 5*time.Minute {
			wait = 5 * time.Minute
		}
	}
	st.nextAt = time.Now().Add(wait)
	b.rows[id] = st
}

func (b *dirRetryBackoff) clear(id int64) {
	b.mu.Lock()
	delete(b.rows, id)
	b.mu.Unlock()
}

func hasAncestorIn(set map[string]bool, p string) bool {
	for a := path.Dir(p); a != "." && a != "/" && a != ""; a = path.Dir(a) {
		if set[a] {
			return true
		}
	}
	return false
}

// materializeDir creates one pending folder. It returns false when the row
// must stay for a later attempt.
func (d *Drainer) materializeDir(p metadata.PendingDir) bool {
	if pin.IsOffline() || !pin.FUSEIdentityOK() {
		return false
	}
	if leases := d.ProjectLeases(); leases != nil {
		if project := projectOf(p.Path); project != "" {
			if dec, err := leases.EnsureForDrain(project); err == nil && !dec.held && dec.heldBy != "" {
				d.dirRetry.failed(p.ID)
				return false // another Mac holds the project: keep it here
			}
		}
	}
	var op *namespaceOperation
	// Shallow: a client may keep creating entries inside the folder (local
	// while it is pending) during the server round trip.
	if d.onDirAdmission != nil {
		if op = d.onDirAdmission(p.Path); op == nil {
			return false // a client operation owns the name; next round
		}
	}
	err := d.materializeDirOwned(p, op)
	if op != nil {
		op.finish(nil)
	}
	if errors.Is(err, errPendingDirIsFile) && d.onPendingDirNameTaken != nil {
		d.dirRetry.clear(p.ID) // the renamed row is new work, not a retry
		if conflict, rerr := d.onPendingDirNameTaken(p.Path); rerr != nil {
			d.dirRetry.failed(p.ID)
			jmlog.Warn("a file on the volume has this folder's name; renaming the folder to a conflict name failed, will retry",
				"path", p.Path, "error", rerr.Error())
		} else {
			jmlog.Warn("a file on the volume has this folder's name; the folder and everything in it now use a conflict name",
				"path", p.Path, "conflict_folder", conflict)
		}
		return false // the renamed row drains on the next pass
	}
	if err != nil {
		d.dirRetry.failed(p.ID)
		jmlog.Info("folder not created on the volume yet; will retry", "path", p.Path, "error", err.Error())
		return false
	}
	d.dirRetry.clear(p.ID)
	return true
}

// dirCreateLocks serializes creating one folder: the drainer holds only a
// shallow admission, so a client rename inside the folder may create it too.
type dirCreateLocks struct {
	mu    sync.Mutex
	locks map[string]*dirCreateLock
}

type dirCreateLock struct {
	mu   sync.Mutex
	refs int
}

func (l *dirCreateLocks) lock(p string) func() {
	l.mu.Lock()
	if l.locks == nil {
		l.locks = make(map[string]*dirCreateLock)
	}
	e := l.locks[p]
	if e == nil {
		e = &dirCreateLock{}
		l.locks[p] = e
	}
	e.refs++
	l.mu.Unlock()
	e.mu.Lock()
	return func() {
		e.mu.Unlock()
		l.mu.Lock()
		if e.refs--; e.refs == 0 {
			delete(l.locks, p)
		}
		l.mu.Unlock()
	}
}

// materializeDirOwned runs while the caller owns p.Path in the namespace.
func (d *Drainer) materializeDirOwned(p metadata.PendingDir, op *namespaceOperation) error {
	defer d.dirCreate.lock(p.Path)()
	cur, ok := d.spool.pendingDir(p.Path)
	if !ok || cur.ID != p.ID {
		return nil // removed or renamed meanwhile: nothing to create here
	}
	var store *metadata.Store
	if d.spool.Meta() != nil {
		store = d.spool.Meta().Owner()
	}
	mode := os.FileMode(0o755)
	if store != nil {
		if e := store.LookupByPath(p.Path); e != nil && e.IsDir && e.Mode.Perm() != 0 {
			mode = e.Mode.Perm()
		}
	}
	fusePath := filepath.Join(d.fuseRoot, p.Path)
	name := path.Base(p.Path)
	err := error(syscall.ESRCH)
	if !syntheticInode(p.Inode) && d.dirInodes != nil {
		parentIno, perr := d.parentInode(store, path.Dir(p.Path))
		if perr != nil {
			return perr
		}
		err = d.dirInodes.ctl.mkdirInode(parentIno, name, mode, p.Inode)
		if errors.Is(err, syscall.ENOENT) {
			// A stale mirror inode, or the parent was removed remotely:
			// resolve it on the mount, recreating the chain if needed.
			if parentIno, perr = d.parentInodeFromMount(path.Dir(p.Path)); perr != nil {
				return perr
			}
			err = d.dirInodes.ctl.mkdirInode(parentIno, name, mode, p.Inode)
		}
		if err != nil && !errors.Is(err, syscall.ESRCH) && !errors.Is(err, errControlUnsupported) && !definiteMkdirRefusal(err) {
			// juicefs may have committed, and never offers the number again:
			// remember why, for the ESRCH that the next attempt gets.
			d.dirAmbiguous.Store(p.ID, err.Error())
		}
	}
	if errors.Is(err, syscall.ESRCH) && !syntheticInode(p.Inode) && d.dirInodes != nil {
		if fi, lerr := os.Lstat(fusePath); lerr == nil && fi.IsDir() {
			// An earlier attempt that failed ambiguously did commit.
			err = syscall.EEXIST
		} else if cause, ok := d.dirAmbiguous.Load(p.ID); ok {
			// Used up by that failed attempt; the pool is fine.
			jmlog.Info("folder's reserved inode was used up by an earlier failed create; creating the folder plainly",
				"path", p.Path, "earlier_error", cause)
		} else if d.dirInodes.stillHeld() {
			jmlog.Info("juicefs refused this folder's reserved inode but still holds the others; creating the folder plainly",
				"path", p.Path)
		} else {
			// The juicefs that reserved the numbers restarted: they are
			// void. The file id of this folder changes once.
			d.dirInodes.invalidate()
		}
	}
	if errors.Is(err, syscall.ESRCH) || errors.Is(err, errControlUnsupported) {
		err = os.Mkdir(fusePath, mode)
		if errors.Is(err, os.ErrNotExist) {
			if d.spool.pendingAncestor(p.Path) {
				return errPendingParent
			}
			if mErr := os.MkdirAll(filepath.Dir(fusePath), 0o755); mErr != nil {
				return mErr
			}
			err = os.Mkdir(fusePath, mode)
		}
		if err == nil {
			if fi, lerr := os.Lstat(fusePath); lerr == nil {
				if st, ok := fi.Sys().(*syscall.Stat_t); ok && st.Ino != 0 {
					p.Inode = uint64(st.Ino) // published below; reconcile adopts it
				}
			}
		}
	}
	realIno := p.Inode
	if errors.Is(err, os.ErrExist) || errors.Is(err, syscall.EEXIST) {
		fi, lerr := os.Lstat(fusePath)
		if lerr != nil {
			return lerr
		}
		if !fi.IsDir() {
			// The caller renames the folder to a conflict name once it no
			// longer holds the name (materializeDir).
			return errPendingDirIsFile
		}
		if st, ok := fi.Sys().(*syscall.Stat_t); ok && st.Ino != 0 {
			realIno = uint64(st.Ino)
		}
		jmlog.Info("folder already existed on the volume; using it", "path", p.Path,
			"same_inode", realIno == p.Inode)
		err = nil
	}
	if err != nil {
		if isLeaseRefusal(err) {
			if leases := d.ProjectLeases(); leases != nil {
				if project := projectOf(p.Path); project != "" {
					leases.NoteRefused(project, "")
				}
			}
		}
		return err
	}
	if op != nil {
		op.mutated = true
	}
	if err := d.spool.removePendingDir(p); err != nil {
		return err
	}
	d.dirAmbiguous.Delete(p.ID)
	if d.onDirMaterialized != nil {
		d.onDirMaterialized(p.Path, realIno)
	}
	return nil
}

// parentInode resolves the volume inode of folder parent without a round
// trip when the mirror knows it.
func (d *Drainer) parentInode(store *metadata.Store, parent string) (uint64, error) {
	if parent == "." || parent == "" {
		return 1, nil
	}
	if store != nil {
		if e := store.LookupByPath(parent); e != nil && e.IsDir && e.Inode > 1 && !syntheticInode(e.Inode) {
			if _, pending := d.spool.pendingDir(parent); !pending {
				return e.Inode, nil
			}
		}
	}
	return d.parentInodeFromMount(parent)
}

// parentInodeFromMount resolves parent on the mount, recreating it (and its
// own parents) when it was removed remotely, as a file drain does.
func (d *Drainer) parentInodeFromMount(parent string) (uint64, error) {
	if parent == "." || parent == "" {
		return 1, nil
	}
	if d.spool.pendingAtOrAbove(parent) {
		return 0, errPendingParent
	}
	p := filepath.Join(d.fuseRoot, parent)
	fi, err := os.Lstat(p)
	if errors.Is(err, os.ErrNotExist) {
		if err = os.MkdirAll(p, 0o755); err == nil {
			fi, err = os.Lstat(p)
		}
	}
	if err != nil {
		return 0, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || !fi.IsDir() {
		return 0, syscall.ENOTDIR
	}
	return uint64(st.Ino), nil
}

// materializeChainOwned creates, now, the pending folders on the path to p:
// its ancestors, and p itself when includeSelf. The caller owns p in the
// namespace, which covers its ancestors. It is for an operation that must
// find the folder on the volume, such as a rename into it.
func (d *Drainer) materializeChainOwned(p string, includeSelf bool) error {
	var pending []metadata.PendingDir // outermost first
	start := path.Dir(p)
	if includeSelf {
		start = p
	}
	for a := start; a != "." && a != "/" && a != ""; a = path.Dir(a) {
		if row, ok := d.spool.pendingDir(a); ok {
			pending = append([]metadata.PendingDir{row}, pending...)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	// Refuse at once what cannot succeed now, rather than wait for the
	// control-file reply timeout.
	if pin.IsOffline() {
		return errPendingChainOffline
	}
	if !pin.FUSEIdentityOK() {
		return pin.ErrFUSEIdentityGate
	}
	if leases := d.ProjectLeases(); leases != nil {
		if project := projectOf(pending[0].Path); project != "" {
			if dec, err := leases.EnsureForDrain(project); err == nil && !dec.held && dec.heldBy != "" {
				return errPendingChainLease
			}
		}
	}
	for _, row := range pending {
		if err := d.materializeDirOwned(row, nil); err != nil {
			if errors.Is(err, errPendingDirIsFile) {
				d.spool.SignalReady() // the drainer renames it to a conflict name
			}
			return err
		}
		if _, still := d.spool.pendingDir(row.Path); still {
			return syscall.EAGAIN
		}
	}
	d.symlinkWake.Store(true)
	return nil
}

var (
	errPendingChainOffline = errors.New("pending folder: offline")
	errPendingChainLease   = errors.New("pending folder: another Mac holds the project")
	// errPendingParent refuses to create a parent folder by plain mkdir
	// while it is pending: that would give it a new inode, and the file id
	// the client already holds would go stale (#70).
	errPendingParent = errors.New("parent folder is still pending")
	// errPendingDirIsFile: the volume has a file where the folder should go.
	errPendingDirIsFile = errors.New("the volume has a file with this folder's name")
)

// conflictFolderPath is conflictCopyPath for a folder: the tag goes after the
// whole name (a folder has no extension).
func conflictFolderPath(p, host string, now time.Time, n int) string {
	stamp := now.Format("2006-01-02 150405")
	tag := fmt.Sprintf(" (conflict %s %s)", host, stamp)
	if n > 1 {
		tag = fmt.Sprintf(" (conflict %s %s %d)", host, stamp, n)
	}
	return path.Join(path.Dir(p), fitConflictName(path.Base(p), tag))
}

// resolvePendingDirNameTaken moves a pending folder whose name a file on the
// volume has taken, with everything in it, to a conflict name. It uses the
// NFS rename path, so the mirror, the spool rows and the pending folder rows
// move together (one SQLite transaction) and clients are told. The file on
// the volume stays as it is.
func (h *JuiceMountHandler) resolvePendingDirNameTaken(p string) (string, error) {
	spool := h.spool.Load()
	if spool == nil {
		return "", os.ErrInvalid
	}
	jfs := &juiceFS{handler: h}
	host, now := drainConflictHost(), drainConflictNow()
	for n := 1; n <= 100; n++ {
		c := conflictFolderPath(p, host, now, n)
		if h.store.LookupByPath(c) != nil {
			continue
		}
		if _, err := os.Lstat(jfs.fullPath(c)); !errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err := jfs.Rename(p, c); err != nil {
			return "", err
		}
		if err := spool.Meta().RecordFolderConflict(p, c); err != nil {
			jmlog.Warn("folder conflict not recorded", "path", p, "error", err.Error())
		}
		spool.SignalReady()
		return c, nil
	}
	return "", os.ErrExist
}
