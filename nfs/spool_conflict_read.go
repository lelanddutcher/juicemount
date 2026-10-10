package nfs

import (
	"bytes"
	"crypto/sha256"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

// conflictReadView is the accepted local namespace generation of a save whose
// backend publish conflicted. A remote reconcile cannot retire this generation;
// only a local successor or explicit discard can. It never enters the drain
// index, and therefore never changes HasPending or the durable failed status.
type conflictReadView struct {
	id         int64
	name, file string
	size       int64
	inode      uint64
	mtime      time.Time
	source     unix.Stat_t
	store      *SpoolStore
}

func (v *conflictReadView) info() os.FileInfo {
	return &spoolFileInfo{name: path.Base(v.name), size: v.size, mtime: v.mtime, inode: v.inode}
}

// qualifyConflictRead runs outside metadata's failure callback. A complete,
// unpunched SHA proof is checked once; foreground NFS reads use only RAM and
// local fstat identity checks, never SQLite, FUSE, or a source rehash.
func (s *SpoolStore) qualifyConflictRead(row *metadata.SpoolRow, liveInode uint64) bool {
	if row == nil || row.ID <= 0 || row.DrainState != metadata.DrainFailed ||
		row.RetiredBy != 0 || row.QualificationHold != "" || row.PunchedEnd != 0 ||
		!isConflictCopyFailure(row.LastError) || len(row.SHA256) != sha256.Size {
		return false
	}
	f, err := os.Open(row.SpoolFile)
	if err != nil {
		return false
	}
	defer f.Close()
	var before, after, linked unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &before); err != nil ||
		before.Mode&unix.S_IFMT != unix.S_IFREG || before.Size != row.Size {
		return false
	}
	hash := sha256.New()
	size, err := io.CopyBuffer(hash, f, make([]byte, 1<<20))
	if err != nil || unix.Fstat(int(f.Fd()), &after) != nil ||
		unix.Stat(row.SpoolFile, &linked) != nil || size != row.Size ||
		!bytes.Equal(hash.Sum(nil), row.SHA256) ||
		!sameConflictSource(&before, &after) || !sameConflictSource(&before, &linked) {
		return false
	}
	// Serialize the final durable latest-row check and RAM publication with
	// same-path local successor admission and explicit discard.
	shard := s.pathShard(row.NFSPath)
	shard.Lock()
	defer shard.Unlock()
	// A local successor in any state owns the name, even after its own drain.
	current, err := s.meta.Get(row.ID)
	if err != nil || current == nil || current.NFSPath != row.NFSPath || current.SpoolFile != row.SpoolFile ||
		current.Size != row.Size || current.PunchedEnd != row.PunchedEnd || !bytes.Equal(current.SHA256, row.SHA256) ||
		current.DrainState != metadata.DrainFailed || current.RetiredBy != 0 ||
		current.QualificationHold != "" || !isConflictCopyFailure(current.LastError) {
		return false
	}
	cloneHold, err := s.meta.CloneHold(row.ID)
	if err != nil || cloneHold != "" {
		return false
	}
	newer, err := s.meta.HasNewerRowForPath(row.NFSPath, row.ID)
	if err != nil || newer {
		return false
	}
	v := &conflictReadView{id: row.ID, name: row.NFSPath, file: row.SpoolFile,
		size: row.Size, inode: liveInode, mtime: row.UpdatedAt, source: after, store: s}
	s.conflictMu.Lock()
	defer s.conflictMu.Unlock()
	if s.conflictByPath == nil {
		s.conflictByPath = make(map[string]*conflictReadView)
		s.conflictByInode = make(map[uint64]*conflictReadView)
		s.revokedConflicts = make(map[uint64]bool)
	}
	if old := s.conflictByPath[row.NFSPath]; old != nil && old.id >= row.ID {
		return false
	}
	if v.inode != 0 && !s.liveConflictInodeAvailableLocked(v.inode, row.NFSPath) {
		v.inode = 0
	}
	if v.inode == 0 && s.conflictHandler != nil {
		v.inode = s.allocateConflictInodeLocked(s.conflictHandler)
		if v.inode == 0 {
			return false
		}
	}
	if old := s.conflictByPath[row.NFSPath]; old != nil {
		delete(s.conflictByInode, old.inode)
		s.revokedConflicts[old.inode] = true
	}
	s.conflictByPath[row.NFSPath] = v
	if v.inode != 0 {
		s.conflictByInode[v.inode] = v
	}
	return true
}

// Boot qualification precedes Handler construction. Assign its inode only
// when SetSpool attaches the new export, before requests can be served.
func (s *SpoolStore) bindConflictHandler(h *JuiceMountHandler) {
	s.conflictMu.Lock()
	defer s.conflictMu.Unlock()
	s.conflictHandler = h
	for _, v := range s.conflictByPath {
		if v.inode == 0 {
			v.inode = s.allocateConflictInodeLocked(h)
			if v.inode != 0 {
				s.conflictByInode[v.inode] = v
			}
		}
	}
}

func (s *SpoolStore) liveConflictInodeAvailableLocked(inode uint64, name string) bool {
	if inode == 0 || s.revokedConflicts[inode] {
		return false
	}
	if owner := s.conflictByInode[inode]; owner != nil && owner.name != name {
		return false
	}
	if h := s.conflictHandler; h != nil {
		if e := h.store.LookupByInode(inode); e != nil && e.Path != name {
			return false
		}
		if p, ok := h.store.SyntheticHandlePath(inode); ok && p != name {
			return false
		}
	}
	return true
}

func (s *SpoolStore) allocateConflictInodeLocked(h *JuiceMountHandler) uint64 {
	for attempt := 0; attempt < 1024; attempt++ {
		inode := h.nextSyntheticInode()
		if s.conflictByInode[inode] != nil || s.revokedConflicts[inode] {
			continue
		}
		if h.store.LookupByInode(inode) != nil {
			continue
		}
		if _, ok := h.store.SyntheticHandlePath(inode); ok {
			continue
		}
		return inode
	}
	return 0 // no proof of unique ownership: refuse a handle
}

func (s *SpoolStore) allocateConflictSafeInode(h *JuiceMountHandler) uint64 {
	s.conflictMu.Lock()
	defer s.conflictMu.Unlock()
	return s.allocateConflictInodeLocked(h)
}

func sameConflictSource(a, b *unix.Stat_t) bool {
	return a != nil && b != nil && b.Mode&unix.S_IFMT == unix.S_IFREG &&
		a.Dev == b.Dev && a.Ino == b.Ino && a.Size == b.Size &&
		a.Mtim.Nano() == b.Mtim.Nano() && a.Ctim.Nano() == b.Ctim.Nano()
}

func (s *SpoolStore) conflictRead(path string) *conflictReadView {
	s.conflictMu.RLock()
	v := s.conflictByPath[path]
	s.conflictMu.RUnlock()
	return v
}

func (s *SpoolStore) conflictHandle(inode uint64) (*conflictReadView, bool) {
	s.conflictMu.RLock()
	v, revoked := s.conflictByInode[inode], s.revokedConflicts[inode]
	s.conflictMu.RUnlock()
	return v, revoked
}

func (s *SpoolStore) conflictChildren(dir string) []*conflictReadView {
	s.conflictMu.RLock()
	var out []*conflictReadView
	for name, v := range s.conflictByPath {
		if path.Dir(name) == dir {
			out = append(out, v)
		}
	}
	s.conflictMu.RUnlock()
	return out
}

func (s *SpoolStore) revokeConflictRead(name string) {
	s.conflictMu.Lock()
	if v := s.conflictByPath[name]; v != nil {
		delete(s.conflictByPath, name)
		delete(s.conflictByInode, v.inode)
		s.revokedConflicts[v.inode] = true
	}
	s.conflictMu.Unlock()
}

func (s *SpoolStore) revokeConflictReadIf(name string, id int64) {
	s.conflictMu.Lock()
	if v := s.conflictByPath[name]; v != nil && v.id == id {
		delete(s.conflictByPath, name)
		delete(s.conflictByInode, v.inode)
		s.revokedConflicts[v.inode] = true
	}
	s.conflictMu.Unlock()
}

func (v *conflictReadView) check() error {
	if v.store.conflictRead(v.name) != v {
		return syscall.EIO
	}
	var now unix.Stat_t
	if unix.Stat(v.file, &now) != nil || !sameConflictSource(&v.source, &now) {
		return syscall.EIO
	}
	return nil
}

func (v *conflictReadView) open() (billy.File, error) {
	if err := v.check(); err != nil {
		return nil, err
	}
	f, err := os.Open(v.file)
	if err != nil {
		return nil, syscall.EIO
	}
	var opened unix.Stat_t
	if unix.Fstat(int(f.Fd()), &opened) != nil || !sameConflictSource(&v.source, &opened) || v.store.conflictRead(v.name) != v {
		f.Close()
		return nil, syscall.EIO
	}
	return &conflictReadFile{File: f, view: v}, nil
}

type conflictReadFile struct {
	*os.File
	view *conflictReadView
	mu   sync.Mutex
	pos  int64
}

func (f *conflictReadFile) Name() string              { return f.view.name }
func (f *conflictReadFile) Lock() error               { return nil }
func (f *conflictReadFile) Unlock() error             { return nil }
func (f *conflictReadFile) Write([]byte) (int, error) { return 0, syscall.EROFS }
func (f *conflictReadFile) Truncate(int64) error      { return syscall.EROFS }
func (f *conflictReadFile) Stat() (os.FileInfo, error) {
	if err := f.view.check(); err != nil {
		return nil, err
	}
	return f.view.info(), nil
}
func (f *conflictReadFile) CachedInfo() os.FileInfo { return f.view.info() }
func (f *conflictReadFile) CompleteCachedSize() (int64, bool, error) {
	if err := f.view.check(); err != nil {
		return 0, false, err
	}
	return f.view.size, true, nil
}
func (f *conflictReadFile) BeginReadAdmission() (func() error, func(), error) {
	if err := f.view.check(); err != nil {
		return nil, nil, err
	}
	return f.view.check, nil, nil
}
func (f *conflictReadFile) ReadAt(p []byte, off int64) (int, error) {
	if err := f.view.check(); err != nil {
		return 0, err
	}
	var before unix.Stat_t
	if unix.Fstat(int(f.File.Fd()), &before) != nil || !sameConflictSource(&f.view.source, &before) {
		return 0, syscall.EIO
	}
	if off < 0 {
		return 0, syscall.EINVAL
	}
	if off >= f.view.size {
		return 0, io.EOF
	}
	if int64(len(p)) > f.view.size-off {
		p = p[:f.view.size-off]
	}
	n, err := f.File.ReadAt(p, off)
	var after unix.Stat_t
	if unix.Fstat(int(f.File.Fd()), &after) != nil || !sameConflictSource(&f.view.source, &after) {
		clear(p)
		return 0, syscall.EIO
	}
	if checkErr := f.view.check(); checkErr != nil {
		clear(p)
		return 0, checkErr
	}
	if n != len(p) {
		clear(p)
		return 0, syscall.EIO
	}
	if err == io.EOF {
		err = nil
	}
	return n, err
}
func (f *conflictReadFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.ReadAt(p, f.pos)
	f.pos += int64(n)
	return n, err
}
func (f *conflictReadFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch whence {
	case io.SeekStart:
		f.pos = offset
	case io.SeekCurrent:
		f.pos += offset
	case io.SeekEnd:
		f.pos = f.view.size + offset
	default:
		return 0, syscall.EINVAL
	}
	return f.pos, nil
}

// A held NFS handle remains bound to its accepted local generation. If that
// generation is cleared during an RPC, no operation can fall through to FUSE.
type conflictHandleFS struct {
	*juiceFS
	view *conflictReadView
}

func (fs *conflictHandleFS) match(name string) bool {
	return strings.TrimPrefix(name, "/") == fs.view.name
}
func (fs *conflictHandleFS) Open(name string) (billy.File, error) {
	return fs.OpenFile(name, os.O_RDONLY, 0)
}
func (fs *conflictHandleFS) OpenFile(name string, flag int, _ os.FileMode) (billy.File, error) {
	if err := fs.handler.namespace.servingError(name); err != nil {
		return nil, err
	}
	if !fs.match(name) || flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0 {
		return nil, syscall.EROFS
	}
	return fs.view.open()
}
func (fs *conflictHandleFS) Stat(name string) (os.FileInfo, error) {
	if err := fs.handler.namespace.servingError(name); err != nil {
		return nil, err
	}
	if !fs.match(name) {
		return nil, syscall.EIO
	}
	if err := fs.view.check(); err != nil {
		return nil, err
	}
	return fs.view.info(), nil
}
func (fs *conflictHandleFS) Lstat(name string) (os.FileInfo, error) { return fs.Stat(name) }
func (fs *conflictHandleFS) StatCacheOnly(name string) (os.FileInfo, bool) {
	info, err := fs.Stat(name)
	return info, err == nil
}
func (fs *conflictHandleFS) TruncateFile(string, int64) (bool, error)    { return true, syscall.EROFS }
func (fs *conflictHandleFS) CommitFile(string) error                     { return syscall.EROFS }
func (fs *conflictHandleFS) Create(string) (billy.File, error)           { return nil, syscall.EROFS }
func (fs *conflictHandleFS) Rename(string, string) error                 { return syscall.EROFS }
func (fs *conflictHandleFS) Remove(string) error                         { return syscall.EROFS }
func (fs *conflictHandleFS) MkdirAll(string, os.FileMode) error          { return syscall.EROFS }
func (fs *conflictHandleFS) TempFile(string, string) (billy.File, error) { return nil, syscall.EROFS }
func (fs *conflictHandleFS) Symlink(string, string) error                { return syscall.EROFS }

// Mutations through a held conflict handle must not touch the old backend.
type conflictHandleChange struct{}

func (conflictHandleChange) Chmod(string, os.FileMode) error            { return syscall.EROFS }
func (conflictHandleChange) Chown(string, int, int) error               { return syscall.EROFS }
func (conflictHandleChange) Lchown(string, int, int) error              { return syscall.EROFS }
func (conflictHandleChange) Chtimes(string, time.Time, time.Time) error { return syscall.EROFS }
