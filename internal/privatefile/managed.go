package privatefile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// Identity is compared against live descriptors and the public path before a
// prepared file may be exposed. A same-user adversary with write access to
// these directories remains outside the immutability guarantee.
type Identity struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
	Size   int64  `json:"size"`
	UID    uint32 `json:"uid"`
	Mode   uint32 `json:"mode"`
}

// ReadPrivate pins every path component and reads one owner-only 0600 leaf.
// Its bytes are never reopened after a service has constructed its routes.
func ReadPrivate(path string, max int64) (_ []byte, _ Identity, resultErr error) {
	if max < 1 || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, Identity{}, errors.New("private input path")
	}
	parent, err := walkAbsoluteDirectory(filepath.Dir(path))
	if err != nil {
		return nil, Identity{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, parent.Close()) }()
	if _, err := privateDir(parent); err != nil {
		return nil, Identity{}, err
	}
	file, err := OpenAt(parent, filepath.Base(path), os.O_RDONLY, 0)
	if err != nil {
		return nil, Identity{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	id, err := privateRegular(file)
	if err != nil {
		return nil, Identity{}, err
	}
	if id.Size > max {
		return nil, Identity{}, errors.New("private input too large")
	}
	b, err := io.ReadAll(io.LimitReader(file, max+1))
	if err != nil {
		return nil, Identity{}, err
	}
	if int64(len(b)) != id.Size {
		return nil, Identity{}, errors.New("private input changed while reading")
	}
	after, err := privateRegular(file)
	if err != nil || after != id {
		return nil, Identity{}, errors.New("private input identity changed")
	}
	return b, id, nil
}
func identity(f *os.File) (Identity, uint64, error) {
	if f == nil {
		return Identity{}, 0, errors.New("nil private file")
	}
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		return Identity{}, 0, err
	}
	return Identity{uint64(st.Dev), st.Ino, st.Size, st.Uid, uint32(st.Mode)}, uint64(st.Nlink), nil
}
func privateDir(f *os.File) (Identity, error) {
	id, _, err := identity(f)
	if err != nil {
		return id, err
	}
	if id.Mode&unix.S_IFMT != unix.S_IFDIR || id.Mode&0777 != 0700 || id.UID != uint32(os.Geteuid()) {
		return Identity{}, errors.New("managed directory identity or mode")
	}
	// Directory entry changes alter st.Size on supported filesystems. The
	// pinned directory identity is its device/inode/owner/type/mode, whereas
	// regular files retain their exact byte length in Identity.Size.
	id.Size = 0
	return id, nil
}
func privateRegular(f *os.File) (Identity, error) {
	id, nlink, err := identity(f)
	if err != nil {
		return id, err
	}
	if id.Mode&unix.S_IFMT != unix.S_IFREG || id.Mode&0777 != 0600 || id.UID != uint32(os.Geteuid()) || id.Size < 0 || nlink != 1 {
		return Identity{}, errors.New("managed file identity or mode")
	}
	return id, nil
}
func openDirectoryAt(parent *os.File, name string) (*os.File, error) {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return nil, errors.New("managed directory leaf")
	}
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}
func walkAbsoluteDirectory(path string) (*os.File, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("managed root requires a clean absolute path")
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	current := os.NewFile(uintptr(fd), "/")
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if part == "" {
			continue
		}
		next, err := openDirectoryAt(current, part)
		closeErr := current.Close()
		if err != nil {
			return nil, errors.Join(err, closeErr)
		}
		if closeErr != nil {
			return nil, errors.Join(closeErr, next.Close())
		}
		current = next
	}
	return current, nil
}

type ManagedRoot struct {
	path                        string
	root, attempts, ready       *os.File
	rootID, attemptsID, readyID Identity
	closed                      bool
	closeErr                    error
}

func OpenManagedRoot(path string) (_ *ManagedRoot, err error) {
	root, err := walkAbsoluteDirectory(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, root.Close())
		}
	}()
	rootID, err := privateDir(root)
	if err != nil {
		return nil, err
	}
	attempts, err := openDirectoryAt(root, "attempts")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, attempts.Close())
		}
	}()
	attemptsID, err := privateDir(attempts)
	if err != nil {
		return nil, err
	}
	ready, err := openDirectoryAt(root, "ready")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, ready.Close())
		}
	}()
	readyID, err := privateDir(ready)
	if err != nil {
		return nil, err
	}
	return &ManagedRoot{path: path, root: root, attempts: attempts, ready: ready, rootID: rootID, attemptsID: attemptsID, readyID: readyID}, nil
}
func (r *ManagedRoot) Identity() Identity { return r.rootID }
func (r *ManagedRoot) Close() error {
	if r == nil {
		return nil
	}
	if r.closed {
		return r.closeErr
	}
	r.closed = true
	var err error
	if r.ready != nil {
		err = errors.Join(err, r.ready.Close())
		r.ready = nil
	}
	if r.attempts != nil {
		err = errors.Join(err, r.attempts.Close())
		r.attempts = nil
	}
	if r.root != nil {
		err = errors.Join(err, r.root.Close())
		r.root = nil
	}
	r.closeErr = err
	return r.closeErr
}
func (r *ManagedRoot) Revalidate() (resultErr error) {
	if r == nil || r.root == nil || r.attempts == nil || r.ready == nil {
		return errors.New("managed root closed")
	}
	live, err := walkAbsoluteDirectory(r.path)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, live.Close()) }()
	liveID, err := privateDir(live)
	if err != nil || liveID != r.rootID {
		return errors.New("managed root replaced")
	}
	if id, err := privateDir(r.root); err != nil || id != r.rootID {
		return errors.New("managed root descriptor changed")
	}
	for _, item := range []struct {
		name     string
		file     *os.File
		expected Identity
	}{{"attempts", r.attempts, r.attemptsID}, {"ready", r.ready, r.readyID}} {
		current, err := openDirectoryAt(r.root, item.name)
		if err != nil {
			return err
		}
		id, e := privateDir(current)
		closeErr := current.Close()
		if e != nil || closeErr != nil || id != item.expected {
			return errors.Join(e, closeErr, errors.New("managed child directory replaced"))
		}
		if id, e = privateDir(item.file); e != nil || id != item.expected {
			return errors.New("managed child descriptor changed")
		}
	}
	return nil
}
func validAttemptID(id string) bool {
	if len(id) != 37 || !strings.HasPrefix(id, "dmat_") {
		return false
	}
	for _, c := range id[5:] {
		if c < '0' || c > '9' {
			if c < 'a' || c > 'f' {
				return false
			}
		}
	}
	return id[5:] != "00000000000000000000000000000000"
}

type Attempt struct {
	root       *ManagedRoot
	dir        *os.File
	dirID      Identity
	lock       *os.File
	lockID     Identity
	file       *os.File
	fileID     Identity
	id         string
	closed     bool
	closeErr   error
	releaseErr error
	beforeDone bool
	beforeErr  error
	closeOps   *attemptCloseOps
}

type attemptCloseOps struct {
	data, read, directory, lock func(*os.File) error
}

func closeAttemptFile(op func(*os.File) error, file *os.File) error {
	if op == nil {
		return file.Close()
	}
	return op(file)
}

func (r *ManagedRoot) OpenAttempt(id string) (_ *Attempt, err error) {
	if !validAttemptID(id) {
		return nil, errors.New("managed attempt identity")
	}
	if err := r.Revalidate(); err != nil {
		return nil, err
	}
	if err := unix.Mkdirat(int(r.attempts.Fd()), id, 0700); err != nil {
		return nil, err
	}
	if err := r.attempts.Sync(); err != nil {
		return nil, err
	}
	dir, err := openDirectoryAt(r.attempts, id)
	if err != nil {
		return nil, err
	}
	var lock, file *os.File
	defer func() {
		if err != nil {
			if file != nil {
				err = errors.Join(err, file.Close())
			}
			err = errors.Join(err, dir.Close())
			if lock != nil {
				err = errors.Join(err, lock.Close())
			}
		}
	}()
	dirID, err := privateDir(dir)
	if err != nil {
		return nil, err
	}
	// The permanent lock is created only for a fresh attempt. Flock is tied to
	// this independent open description, so another descriptor in this process
	// cannot acquire ownership while this attempt is live.
	lock, err = OpenAt(dir, "owner.lock", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	lockID, err := privateRegular(lock)
	if err != nil {
		return nil, err
	}
	if lockID.Size != 0 {
		return nil, errors.New("new attempt lock not empty")
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, fmt.Errorf("attempt ownership unavailable: %w", err)
	}
	file, err = OpenAt(dir, "payload.partial", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	fileID, err := privateRegular(file)
	if err != nil {
		return nil, err
	}
	if err := dir.Sync(); err != nil {
		return nil, err
	}
	return &Attempt{root: r, dir: dir, dirID: dirID, lock: lock, lockID: lockID, file: file, fileID: fileID, id: id}, nil
}
func (a *Attempt) Writer() *os.File {
	if a == nil {
		return nil
	}
	return a.file
}
func (a *Attempt) Close() error {
	if a == nil {
		return nil
	}
	if a.closed {
		return a.closeErr
	}
	_ = a.CloseBeforeRelease()
	_ = a.ReleaseLock()
	return a.closeErr
}

// CloseBeforeRelease closes ordinary attempt resources while retaining the
// permanent lock for the caller's bound outcome bookkeeping.
func (a *Attempt) CloseBeforeRelease() error {
	if a == nil {
		return nil
	}
	if a.beforeDone {
		return a.beforeErr
	}
	a.beforeDone = true
	var err error
	if a.file != nil {
		var op func(*os.File) error
		if a.closeOps != nil {
			op = a.closeOps.data
		}
		err = errors.Join(err, closeAttemptFile(op, a.file))
		a.file = nil
	}
	if a.dir != nil {
		var op func(*os.File) error
		if a.closeOps != nil {
			op = a.closeOps.directory
		}
		err = errors.Join(err, closeAttemptFile(op, a.dir))
		a.dir = nil
	}
	a.beforeErr = err
	return err
}

// ReleaseLock is terminal. An error cannot establish whether the lock remains
// held, so callers correct the durable outcome only after this call returns.
func (a *Attempt) ReleaseLock() error {
	if a == nil {
		return nil
	}
	if a.closed {
		return a.releaseErr
	}
	if !a.beforeDone {
		_ = a.CloseBeforeRelease()
	}
	a.closed = true
	var err error
	if a.lock != nil {
		var op func(*os.File) error
		if a.closeOps != nil {
			op = a.closeOps.lock
		}
		err = errors.Join(err, closeAttemptFile(op, a.lock))
		a.lock = nil
	}
	a.releaseErr = err
	a.closeErr = errors.Join(a.beforeErr, err)
	return err
}
func (a *Attempt) Finish() (_ *PreparedFile, err error) {
	if a == nil || a.file == nil {
		return nil, errors.New("attempt closed")
	}
	if err := a.file.Sync(); err != nil {
		return nil, err
	}
	id, err := privateRegular(a.file)
	if err != nil {
		return nil, err
	}
	if id.Device != a.fileID.Device || id.Inode != a.fileID.Inode {
		return nil, errors.New("partial replaced")
	}
	if err := a.file.Close(); err != nil {
		a.file = nil
		return nil, err
	}
	a.file = nil
	read, err := OpenAt(a.dir, "payload.partial", os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, read.Close())
		}
	}()
	current, err := privateRegular(read)
	if err != nil || current != id {
		return nil, errors.New("partial identity changed")
	}
	return &PreparedFile{attempt: a, read: read, fileID: id}, nil
}

type PreparedFile struct {
	attempt *Attempt
	read    *os.File
	fileID  Identity
	exposed bool
	// A nil dispatch uses the fixed production syscalls below. Tests can set
	// a dispatch on one prepared descriptor without affecting other files.
	publication *publicationSyscalls
	closed      bool
	closeErr    error
	releaseErr  error
	beforeDone  bool
	beforeErr   error
}

type publicationSyscalls struct {
	rename      func(int, string, int, string) error
	readySync   func(*os.File) error
	attemptSync func(*os.File) error
}

func (p *PreparedFile) ID() string {
	if p == nil || p.attempt == nil {
		return ""
	}
	return p.attempt.id
}
func (p *PreparedFile) FileIdentity() Identity    { return p.fileID }
func (p *PreparedFile) RootIdentity() Identity    { return p.attempt.root.rootID }
func (p *PreparedFile) AttemptIdentity() Identity { return p.attempt.dirID }
func (p *PreparedFile) LockIdentity() Identity    { return p.attempt.lockID }
func (p *PreparedFile) ReadOnlyFile() *os.File    { return p.read }
func (p *PreparedFile) Close() error {
	if p == nil {
		return nil
	}
	if p.closed {
		return p.closeErr
	}
	_ = p.CloseBeforeRelease()
	_ = p.ReleaseLock()
	return p.closeErr
}
func (p *PreparedFile) CloseBeforeRelease() error {
	if p == nil {
		return nil
	}
	if p.beforeDone {
		return p.beforeErr
	}
	p.beforeDone = true
	var err error
	if p.read != nil {
		var op func(*os.File) error
		if p.attempt.closeOps != nil {
			op = p.attempt.closeOps.read
		}
		err = closeAttemptFile(op, p.read)
		p.read = nil
	}
	p.beforeErr = errors.Join(err, p.attempt.CloseBeforeRelease())
	return p.beforeErr
}
func (p *PreparedFile) ReleaseLock() error {
	if p == nil {
		return nil
	}
	if p.closed {
		return p.releaseErr
	}
	if !p.beforeDone {
		_ = p.CloseBeforeRelease()
	}
	p.closed = true
	p.releaseErr = p.attempt.ReleaseLock()
	p.closeErr = errors.Join(p.beforeErr, p.releaseErr)
	return p.releaseErr
}
func (p *PreparedFile) Revalidate() error {
	if p == nil || p.exposed || p.read == nil || p.attempt == nil || p.attempt.dir == nil || p.attempt.lock == nil {
		return errors.New("prepared file unavailable")
	}
	if err := p.attempt.root.Revalidate(); err != nil {
		return err
	}
	current, err := openDirectoryAt(p.attempt.root.attempts, p.attempt.id)
	if err != nil {
		return err
	}
	id, e := privateDir(current)
	currentCloseErr := current.Close()
	if e != nil || currentCloseErr != nil || id != p.attempt.dirID {
		return errors.Join(e, currentCloseErr, errors.New("attempt directory replaced"))
	}
	lock, err := OpenAt(p.attempt.dir, "owner.lock", os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	lockID, lockErr := privateRegular(lock)
	lockCloseErr := lock.Close()
	if lockErr != nil || lockCloseErr != nil || lockID != p.attempt.lockID {
		return errors.Join(lockErr, lockCloseErr, errors.New("attempt lock replaced"))
	}
	if id, e = privateRegular(p.attempt.lock); e != nil || id != p.attempt.lockID {
		return errors.New("attempt lock descriptor changed")
	}
	currentFile, err := OpenAt(p.attempt.dir, "payload.partial", os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	fileID, e := privateRegular(currentFile)
	fileCloseErr := currentFile.Close()
	if e != nil || fileCloseErr != nil || fileID != p.fileID {
		return errors.Join(e, fileCloseErr, errors.New("prepared file identity changed"))
	}
	if id, e = privateRegular(p.read); e != nil || id != p.fileID {
		return errors.New("prepared descriptor changed")
	}
	return nil
}

// ExposeNoReplace is the sole publication syscall for an owned prepared file.
// exposed is true when the rename happened even if a subsequent sync failed.
func (p *PreparedFile) ExposeNoReplace() (exposed bool, err error) {
	if err := p.Revalidate(); err != nil {
		return false, err
	}
	name := p.attempt.id + ".payload"
	rename := renameNoReplace
	readySync := (*os.File).Sync
	attemptSync := (*os.File).Sync
	if p.publication != nil {
		if p.publication.rename != nil {
			rename = p.publication.rename
		}
		if p.publication.readySync != nil {
			readySync = p.publication.readySync
		}
		if p.publication.attemptSync != nil {
			attemptSync = p.publication.attemptSync
		}
	}
	if err := rename(int(p.attempt.dir.Fd()), "payload.partial", int(p.attempt.root.ready.Fd()), name); err != nil {
		return false, err
	}
	p.exposed = true
	if err := readySync(p.attempt.root.ready); err != nil {
		return true, fmt.Errorf("ready directory sync uncertain: %w", err)
	}
	if err := attemptSync(p.attempt.dir); err != nil {
		return true, fmt.Errorf("attempt directory sync uncertain: %w", err)
	}
	return true, nil
}
func (r *ManagedRoot) ReadyPath(id string) (string, error) {
	if !validAttemptID(id) {
		return "", errors.New("managed attempt identity")
	}
	if err := r.Revalidate(); err != nil {
		return "", err
	}
	return filepath.Join(r.path, "ready", id+".payload"), nil
}
