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
// reserves numbers from the running juicefs ahead of time (patch 0021,
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
		return err
	}
	if cur, ok := s.dirs.byPath[p.Path]; ok && cur.ID == p.ID {
		delete(s.dirs.byPath, p.Path)
	}
	return nil
}

// movePendingDirs re-keys the rows at and beneath oldPath to newPath.
func (s *SpoolStore) movePendingDirs(oldPath, newPath string) (int, error) {
	s.dirs.mu.Lock()
	defer s.dirs.mu.Unlock()
	m, err := s.pendingDirsLocked()
	if err != nil {
		return 0, err
	}
	moved := false
	for d := range m {
		if metadata.PathAtOrUnder(d, oldPath) {
			moved = true
			break
		}
	}
	if !moved {
		return 0, nil
	}
	n, err := s.meta.MovePendingDirs(oldPath, newPath)
	if err != nil {
		return 0, err
	}
	next := make(map[string]metadata.PendingDir, len(m))
	for d, p := range m {
		if metadata.PathAtOrUnder(d, oldPath) {
			p.Path = newPath + d[len(oldPath):]
		}
		next[p.Path] = p
	}
	s.dirs.byPath = next
	return n, nil
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
		if p.disabled {
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
	if p.disabled {
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
		case err != nil:
			p.retryAt = time.Now().Add(5 * time.Second)
			jmlog.Debug("reserve folder inodes failed", "error", err.Error())
		case count > 0:
			p.ranges = append(p.ranges, inodeRange{next: first, end: first + uint64(count)})
		}
		p.refilling = nil
		close(done)
	}()
	return done
}

// invalidate drops every number: the juicefs that reserved them is gone.
func (p *dirInodePool) invalidate() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.ranges = nil
	p.mu.Unlock()
}

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
	if err != nil {
		jmlog.Info("folder not created on the volume yet; will retry", "path", p.Path, "error", err.Error())
		return false
	}
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
	}
	if errors.Is(err, syscall.ESRCH) || errors.Is(err, errControlUnsupported) {
		if errors.Is(err, syscall.ESRCH) && !syntheticInode(p.Inode) && d.dirInodes != nil {
			// The juicefs that reserved the number restarted. Its numbers
			// are void; the file id of this folder changes once.
			d.dirInodes.invalidate()
			jmlog.Info("reserved folder inode no longer held by juicefs; creating the folder plainly", "path", p.Path)
		}
		err = os.Mkdir(fusePath, mode)
		if errors.Is(err, os.ErrNotExist) {
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
			jmlog.Warn("cannot create folder: the volume has a file with that name; files inside the folder will not drain",
				"path", p.Path)
			_ = d.spool.removePendingDir(p)
			return nil
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
	var chain []string
	start := path.Dir(p)
	if includeSelf {
		start = p
	}
	for a := start; a != "." && a != "/" && a != ""; a = path.Dir(a) {
		chain = append(chain, a)
	}
	for i := len(chain) - 1; i >= 0; i-- {
		row, ok := d.spool.pendingDir(chain[i])
		if !ok {
			continue
		}
		if !pin.FUSEIdentityOK() {
			return pin.ErrFUSEIdentityGate
		}
		if err := d.materializeDirOwned(row, nil); err != nil {
			return err
		}
		if _, still := d.spool.pendingDir(chain[i]); still {
			return syscall.EAGAIN
		}
	}
	return nil
}
