package privatefile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"golang.org/x/sys/unix"
)

// OwnedReady is an existing, exact final file under an independently acquired
// permanent attempt lock. Opening it never creates or moves an attempt entry.
type OwnedReady struct {
	root                  *ManagedRoot
	dir                   *os.File
	lock                  *os.File
	read                  *os.File
	id                    string
	dirID, lockID, fileID Identity
	closed                bool
	closeErr              error
	releaseErr            error
	beforeDone            bool
	beforeErr             error
	syncOps               *recoverySyncOps
}

type recoverySyncOps struct {
	ready, attempt func(*os.File) error
}

func (r *ManagedRoot) OpenOwnedReady(id string, rootID, dirID, lockID, fileID Identity) (_ *OwnedReady, resultErr error) {
	if !validAttemptID(id) || r == nil || r.rootID != rootID || fileID.Size < 0 || lockID.Size != 0 {
		return nil, errors.New("managed recovery binding unavailable")
	}
	if err := r.Revalidate(); err != nil {
		return nil, err
	}
	dir, err := openDirectoryAt(r.attempts, id)
	if err != nil {
		return nil, err
	}
	var lock, read *os.File
	defer func() {
		if resultErr != nil {
			if read != nil {
				resultErr = errors.Join(resultErr, read.Close())
			}
			resultErr = errors.Join(resultErr, dir.Close())
			if lock != nil {
				resultErr = errors.Join(resultErr, lock.Close())
			}
		}
	}()
	if actual, err := privateDir(dir); err != nil || actual != dirID {
		return nil, errors.Join(err, errors.New("attempt directory binding changed"))
	}
	lock, err = OpenAt(dir, "owner.lock", os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if actual, err := privateRegular(lock); err != nil || actual != lockID {
		return nil, errors.Join(err, errors.New("attempt lock binding changed"))
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, fmt.Errorf("attempt ownership unavailable: %w", err)
	}
	// Check path bindings again after acquiring ownership. A same-user actor
	// capable of changing these private directories is outside this boundary.
	pathDir, err := openDirectoryAt(r.attempts, id)
	if err != nil {
		return nil, err
	}
	actualDir, dirErr := privateDir(pathDir)
	pathCloseErr := pathDir.Close()
	if dirErr != nil || pathCloseErr != nil || actualDir != dirID {
		return nil, errors.Join(dirErr, pathCloseErr, errors.New("attempt path changed after lock"))
	}
	pathLock, err := OpenAt(dir, "owner.lock", os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	actualLock, lockErr := privateRegular(pathLock)
	pathCloseErr = pathLock.Close()
	if lockErr != nil || pathCloseErr != nil || actualLock != lockID {
		return nil, errors.Join(lockErr, pathCloseErr, errors.New("attempt lock path changed after lock"))
	}
	if err := r.Revalidate(); err != nil {
		return nil, err
	}
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	if len(names) != 1 || names[0] != "owner.lock" {
		return nil, errors.New("unexpected attempt entries beside final file")
	}
	read, err = OpenAt(r.ready, id+".payload", os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	if actual, err := privateRegular(read); err != nil || actual != fileID {
		return nil, errors.Join(err, errors.New("ready file binding changed"))
	}
	return &OwnedReady{root: r, dir: dir, lock: lock, read: read, id: id, dirID: dirID, lockID: lockID, fileID: fileID}, nil
}

func (o *OwnedReady) ID() string {
	if o == nil {
		return ""
	}
	return o.id
}
func (o *OwnedReady) FileIdentity() Identity    { return o.fileID }
func (o *OwnedReady) RootIdentity() Identity    { return o.root.rootID }
func (o *OwnedReady) AttemptIdentity() Identity { return o.dirID }
func (o *OwnedReady) LockIdentity() Identity    { return o.lockID }

func (o *OwnedReady) Revalidate() error {
	if o == nil || o.closed || o.root == nil || o.dir == nil || o.lock == nil || o.read == nil {
		return errors.New("owned ready file unavailable")
	}
	if err := o.root.Revalidate(); err != nil {
		return err
	}
	dir, err := openDirectoryAt(o.root.attempts, o.id)
	if err != nil {
		return err
	}
	dirID, dirErr := privateDir(dir)
	dirCloseErr := dir.Close()
	if dirErr != nil || dirCloseErr != nil || dirID != o.dirID {
		return errors.Join(dirErr, dirCloseErr, errors.New("owned attempt directory changed"))
	}
	if id, err := privateDir(o.dir); err != nil || id != o.dirID {
		return errors.Join(err, errors.New("owned attempt descriptor changed"))
	}
	lock, err := OpenAt(o.dir, "owner.lock", os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	lockID, lockErr := privateRegular(lock)
	lockCloseErr := lock.Close()
	if lockErr != nil || lockCloseErr != nil || lockID != o.lockID {
		return errors.Join(lockErr, lockCloseErr, errors.New("owned lock path changed"))
	}
	if id, err := privateRegular(o.lock); err != nil || id != o.lockID {
		return errors.Join(err, errors.New("owned lock descriptor changed"))
	}
	// Readdirnames consumes a directory cursor, so reopen the pinned directory
	// for each exact-entry check instead of assuming a prior call's position.
	entries, err := openDirectoryAt(o.root.attempts, o.id)
	if err != nil {
		return err
	}
	names, readErr := entries.Readdirnames(-1)
	entriesCloseErr := entries.Close()
	if readErr != nil || entriesCloseErr != nil || len(names) != 1 || names[0] != "owner.lock" {
		return errors.Join(readErr, entriesCloseErr, errors.New("owned attempt entries changed"))
	}
	ready, err := OpenAt(o.root.ready, o.id+".payload", os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	readyID, readyErr := privateRegular(ready)
	readyCloseErr := ready.Close()
	if readyErr != nil || readyCloseErr != nil || readyID != o.fileID {
		return errors.Join(readyErr, readyCloseErr, errors.New("ready path changed"))
	}
	if id, err := privateRegular(o.read); err != nil || id != o.fileID {
		return errors.Join(err, errors.New("ready descriptor changed"))
	}
	return nil
}

func (o *OwnedReady) VerifyContent(content continuity.Content) error {
	if err := o.Revalidate(); err != nil {
		return err
	}
	if content.Size != o.fileID.Size {
		return errors.New("ready length differs from recorded content")
	}
	if _, err := o.read.Seek(0, io.SeekStart); err != nil {
		return err
	}
	verified := continuity.VerifyContent(o.read, content)
	_, seekErr := o.read.Seek(0, io.SeekStart)
	return errors.Join(verified, seekErr, o.Revalidate())
}

// SyncExistingFinal repeats the two publication directory durability barriers
// under the independently acquired attempt lock. It never creates or renames.
func (o *OwnedReady) SyncExistingFinal() error {
	if err := o.Revalidate(); err != nil {
		return err
	}
	readySync := (*os.File).Sync
	attemptSync := (*os.File).Sync
	if o.syncOps != nil {
		if o.syncOps.ready != nil {
			readySync = o.syncOps.ready
		}
		if o.syncOps.attempt != nil {
			attemptSync = o.syncOps.attempt
		}
	}
	if err := readySync(o.root.ready); err != nil {
		return fmt.Errorf("existing final ready directory sync: %w", err)
	}
	if err := attemptSync(o.dir); err != nil {
		return fmt.Errorf("existing final attempt directory sync: %w", err)
	}
	return o.Revalidate()
}

func (o *OwnedReady) Path() (string, error) {
	if err := o.Revalidate(); err != nil {
		return "", err
	}
	return filepath.Join(o.root.path, "ready", o.id+".payload"), nil
}

func (o *OwnedReady) Close() error {
	if o == nil {
		return nil
	}
	if o.closed {
		return o.closeErr
	}
	_ = o.CloseBeforeRelease()
	_ = o.ReleaseLock()
	return o.closeErr
}
func (o *OwnedReady) CloseBeforeRelease() error {
	if o == nil {
		return nil
	}
	if o.beforeDone {
		return o.beforeErr
	}
	o.beforeDone = true
	var err error
	if o.read != nil {
		err = errors.Join(err, o.read.Close())
		o.read = nil
	}
	if o.dir != nil {
		err = errors.Join(err, o.dir.Close())
		o.dir = nil
	}
	o.beforeErr = err
	return err
}
func (o *OwnedReady) ReleaseLock() error {
	if o == nil {
		return nil
	}
	if o.closed {
		return o.releaseErr
	}
	if !o.beforeDone {
		_ = o.CloseBeforeRelease()
	}
	o.closed = true
	var err error
	if o.lock != nil {
		err = errors.Join(err, o.lock.Close())
		o.lock = nil
	}
	o.releaseErr = err
	o.closeErr = errors.Join(o.beforeErr, err)
	return err
}

// ReadyPath is available to the original publication owner only after its
// no-replace rename; it verifies that the final leaf is the same prepared inode.
func (p *PreparedFile) ReadyPath() (string, error) {
	if p == nil || p.closed || !p.exposed || p.attempt == nil || p.attempt.dir == nil || p.attempt.lock == nil || p.read == nil {
		return "", errors.New("published file owner unavailable")
	}
	if err := p.attempt.root.Revalidate(); err != nil {
		return "", err
	}
	if id, err := privateDir(p.attempt.dir); err != nil || id != p.attempt.dirID {
		return "", errors.Join(err, errors.New("published attempt directory changed"))
	}
	if id, err := privateRegular(p.attempt.lock); err != nil || id != p.attempt.lockID {
		return "", errors.Join(err, errors.New("published attempt lock changed"))
	}
	entries, err := openDirectoryAt(p.attempt.root.attempts, p.attempt.id)
	if err != nil {
		return "", err
	}
	pathDirID, pathDirErr := privateDir(entries)
	names, readErr := entries.Readdirnames(-1)
	entryCloseErr := entries.Close()
	if pathDirErr != nil || pathDirID != p.attempt.dirID || readErr != nil || entryCloseErr != nil || len(names) != 1 || names[0] != "owner.lock" {
		return "", errors.Join(pathDirErr, readErr, entryCloseErr, errors.New("published attempt entries changed"))
	}
	pathLock, err := OpenAt(p.attempt.dir, "owner.lock", os.O_RDONLY, 0)
	if err != nil {
		return "", err
	}
	pathLockID, pathLockErr := privateRegular(pathLock)
	pathLockCloseErr := pathLock.Close()
	if pathLockErr != nil || pathLockCloseErr != nil || pathLockID != p.attempt.lockID {
		return "", errors.Join(pathLockErr, pathLockCloseErr, errors.New("published lock path changed"))
	}
	ready, err := OpenAt(p.attempt.root.ready, p.attempt.id+".payload", os.O_RDONLY, 0)
	if err != nil {
		return "", err
	}
	id, idErr := privateRegular(ready)
	readyCloseErr := ready.Close()
	if idErr != nil || readyCloseErr != nil || id != p.fileID {
		return "", errors.Join(idErr, readyCloseErr, errors.New("published ready identity changed"))
	}
	if id, err := privateRegular(p.read); err != nil || id != p.fileID {
		return "", errors.Join(err, errors.New("published descriptor changed"))
	}
	return filepath.Join(p.attempt.root.path, "ready", p.attempt.id+".payload"), nil
}
