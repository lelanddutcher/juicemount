package nfs

import (
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/metadata"
)

func (f *writeFile) ReadAt(p []byte, off int64) (int, error) {
	r, err := f.handler.beginReadAdmission(f.name)
	if err != nil {
		return 0, err
	}
	n, readErr := f.File.ReadAt(p, off)
	r.finishBytes(p, &n, &readErr)
	return n, readErr
}

func (f *writeFile) Read(p []byte) (int, error) {
	r, err := f.handler.beginReadAdmission(f.name)
	if err != nil {
		return 0, err
	}
	n, readErr := f.File.Read(p)
	r.finishBytes(p, &n, &readErr)
	return n, readErr
}

func (f *writeFile) Stat() (os.FileInfo, error) {
	if err := f.handler.namespace.servingError(f.name); err != nil {
		return nil, err
	}
	return f.File.Stat()
}

func (f *writeFile) BeginReadAdmission() (func() error, func(), error) {
	r, err := f.handler.beginReadAdmission(f.name)
	if err != nil {
		return nil, nil, err
	}
	return r.check, r.release, nil
}

// bind runs while the foreground namespace owns this pathname. The mirror
// token is not a backing identity: pooled descriptors must be checked too.
func (f *writeFile) bind(expected metadata.MirrorToken) error {
	if expected.Incarnation == "" {
		return metadata.ErrMirrorConflict
	}
	info, err := f.File.Stat()
	if err != nil {
		return err
	}
	current, err := os.Stat(f.fusePath)
	if err != nil {
		return err
	}
	if !os.SameFile(info, current) {
		return metadata.ErrMirrorConflict
	}
	got, err := f.handler.store.CaptureMirror(f.name)
	if err != nil {
		return err
	}
	if got != expected {
		return metadata.ErrMirrorConflict
	}
	f.bound, f.identity = expected, info
	return nil
}

// validateBinding captures the revision BEFORE a syscall. Other writes may
// advance it, but this handle's immutable incarnation and descriptor cannot
// change. The caller owns both the per-path mutation lock and namespace.
func (f *writeFile) validateBinding() (metadata.MirrorToken, error) {
	if f.closed {
		return metadata.MirrorToken{}, os.ErrClosed
	}
	expected, err := f.handler.store.CaptureMirror(f.name)
	if err != nil {
		return metadata.MirrorToken{}, err
	}
	if f.bound.Incarnation == "" || expected.Incarnation != f.bound.Incarnation || expected.Path != f.bound.Path {
		return metadata.MirrorToken{}, metadata.ErrMirrorConflict
	}
	info, err := f.File.Stat()
	if err != nil {
		return metadata.MirrorToken{}, err
	}
	current, err := os.Stat(f.fusePath)
	if err != nil {
		return metadata.MirrorToken{}, err
	}
	if !os.SameFile(f.identity, info) || !os.SameFile(info, current) {
		return metadata.MirrorToken{}, metadata.ErrMirrorConflict
	}
	return expected, nil
}

func (f *writeFile) release() {
	if f.closed {
		return
	}
	f.closed = true
	f.handler.decActiveWriter(f.name)
	if f.fdPool != nil {
		f.fdPool.ReleaseWrite(f.fusePath)
	} else {
		_ = f.File.Close()
	}
}

func fileIdentity(info os.FileInfo) (string, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", metadata.ErrMirrorConflict
	}
	return fmt.Sprintf("%d:%d", stat.Dev, stat.Ino), nil
}

func currentMountGeneration(root string) (string, error) {
	if ok, reason := pin.FUSEIdentityFresh(); !ok {
		return "", fmt.Errorf("fuse identity: %s: %w", reason, pin.ErrFUSEIdentityGate)
	}
	info, err := os.Stat(root)
	if err != nil {
		return "", err
	}
	identity, err := fileIdentity(info)
	if err != nil {
		return "", err
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(root, &fs); err != nil {
		return "", err
	}
	return fmt.Sprintf("fsid=%v,root=%s", fs.Fsid, identity), nil
}

func verifyCreatedFileMount(f *os.File, root, expected string) error {
	if err := pin.FUSEIdentityCheckFD(f.Fd()); err != nil {
		return err
	}
	current, err := currentMountGeneration(root)
	if err != nil {
		return err
	}
	if current != expected {
		return metadata.ErrMirrorConflict
	}
	var fileFS, rootFS syscall.Statfs_t
	if err := syscall.Fstatfs(int(f.Fd()), &fileFS); err != nil {
		return err
	}
	if err := syscall.Statfs(root, &rootFS); err != nil {
		return err
	}
	if fileFS.Fsid != rootFS.Fsid {
		return metadata.ErrMirrorConflict
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	byPath, err := os.Stat(f.Name())
	if err != nil {
		return err
	}
	if !os.SameFile(info, byPath) {
		return metadata.ErrMirrorConflict
	}
	return nil
}

func (f *writeFile) mutationIdentity() (backing, mount string, size int64, err error) {
	if err := pin.FUSEIdentityCheckFD(f.File.Fd()); err != nil {
		return "", "", 0, err
	}
	info, err := f.File.Stat()
	if err != nil {
		return "", "", 0, err
	}
	if !os.SameFile(f.identity, info) {
		return "", "", 0, metadata.ErrMirrorConflict
	}
	backing, err = fileIdentity(info)
	if err != nil {
		return "", "", 0, err
	}
	root, err := os.Stat(f.handler.fusePath)
	if err != nil {
		return "", "", 0, err
	}
	rootIdentity, err := fileIdentity(root)
	if err != nil {
		return "", "", 0, err
	}
	var fileFS, rootFS syscall.Statfs_t
	if err := syscall.Fstatfs(int(f.File.Fd()), &fileFS); err != nil {
		return "", "", 0, err
	}
	if err := syscall.Statfs(f.handler.fusePath, &rootFS); err != nil {
		return "", "", 0, err
	}
	if fileFS.Fsid != rootFS.Fsid {
		return "", "", 0, metadata.ErrMirrorConflict
	}
	mount = fmt.Sprintf("fsid=%v,root=%s", fileFS.Fsid, rootIdentity)
	return backing, mount, info.Size(), nil
}

func (f *writeFile) verifyMutation(m metadata.OnlineMutation) error {
	backing, mount, size, err := f.mutationIdentity()
	if err != nil {
		return err
	}
	current, err := os.Stat(f.fusePath)
	if err != nil {
		return err
	}
	info, err := f.File.Stat()
	if err != nil {
		return err
	}
	if backing != m.BackingIdentity || mount != m.MountGeneration || size != m.TargetEOF || !os.SameFile(info, current) {
		return metadata.ErrMirrorConflict
	}
	return nil
}

func (f *writeFile) publishMutation(m metadata.OnlineMutation) error {
	e, err := f.handler.store.CompleteOnlineMutation(m, time.Now())
	if err != nil {
		return err
	}
	f.handler.writeSizeMu.Lock()
	f.handler.writeSizes[f.name] = e.Size
	f.handler.touchWriteSizeLocked(f.name)
	f.handler.writeSizeMu.Unlock()
	f.handler.invalidateReadCaches(f.name)
	f.handler.publishEvent(metadata.MetadataEvent{Op: "create", Path: f.name, Size: e.Size, Mtime: e.Mtime.Unix(), Inode: e.Inode})
	return nil
}

func (f *writeFile) truncateOwned(size int64, op *namespaceOperation) error {
	if size < 0 {
		return syscall.EINVAL
	}
	unlock := f.handler.lockInPlaceMutation(f.name, true)
	defer unlock()
	expected, err := f.validateBinding()
	if err != nil {
		return err
	}
	backing, mount, oldEOF, err := f.mutationIdentity()
	if err != nil {
		return err
	}
	m, err := f.handler.store.BeginOnlineMutation(expected, metadata.OnlineMutation{
		Kind: "truncate", Path: f.name, BackingIdentity: backing, MountGeneration: mount,
		OldEOF: oldEOF, TargetEOF: size,
	})
	if err != nil {
		return err
	}
	op.blockReads()
	op.poison = true
	if hook := f.handler.testBeforeInPlaceMutation; hook != nil {
		if err := hook(f.name, "truncate"); err != nil {
			return err
		}
	}
	if err := f.File.Truncate(size); err != nil {
		return err
	}
	op.mutated = true
	if hook := f.handler.testAfterInPlaceMutation; hook != nil {
		hook(f.name, "truncate")
	}
	if err := f.verifyMutation(m); err != nil {
		return err
	}
	if err := f.handler.syncOnlineMutation(f.File); err != nil {
		return err
	}
	if err := f.verifyMutation(m); err != nil {
		return err
	}
	if err := f.publishMutation(m); err != nil {
		return err
	}
	f.writtenEnd = size
	f.handler.inPlaceHoles.noteTruncate(f.name, f.baseSize, size)
	if f.baseSize > size {
		f.baseSize = size
	}
	op.poison = false
	return nil
}

func (f *writeFile) writeBound(p []byte, off int64, positioned bool) (n int, err error) {
	op := f.handler.namespace.begin([]string{f.name})
	if op == nil {
		return 0, errFUSETimeout
	}
	defer func() { op.finish(err) }()
	unlock := f.handler.lockInPlaceMutation(f.name, true)
	defer unlock()
	expected, err := f.validateBinding()
	if err != nil {
		return 0, err
	}
	releaseData, ok := acquireFUSEData()
	if !ok {
		return 0, errFUSETimeout
	}
	defer releaseData()
	if positioned && off < 0 {
		return 0, syscall.EINVAL
	}
	if len(p) == 0 {
		return 0, nil
	}
	if !positioned {
		off, err = f.File.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, err
		}
	}
	backing, mount, oldEOF, err := f.mutationIdentity()
	if err != nil {
		return 0, err
	}
	if off > int64(^uint64(0)>>1)-int64(len(p)) {
		return 0, syscall.EINVAL
	}
	m, err := f.handler.store.BeginOnlineMutation(expected, metadata.OnlineMutation{
		Kind: "write", Path: f.name, BackingIdentity: backing, MountGeneration: mount,
		OldEOF: oldEOF, TargetEOF: max(oldEOF, off+int64(len(p))),
		WriteOffset: off, WriteLength: int64(len(p)),
	})
	if err != nil {
		return 0, err
	}
	op.blockReads()
	op.poison = true
	if hook := f.handler.testBeforeInPlaceMutation; hook != nil {
		if err := hook(f.name, "write"); err != nil {
			return 0, err
		}
	}
	if positioned {
		n, err = f.File.WriteAt(p, off)
	} else {
		n, err = f.File.Write(p)
	}
	if n > 0 {
		op.mutated = true
		if hook := f.handler.testAfterInPlaceMutation; hook != nil {
			hook(f.name, "write")
		}
		f.noteHole(off, off+int64(n))
		metrics.Default().AddBytesWritten(int64(n))
	}
	if err != nil {
		return n, f.handler.classifyBlipError(err)
	}
	if n != len(p) {
		return n, io.ErrShortWrite
	}
	if err = f.verifyMutation(m); err != nil {
		return n, err
	}
	if err = f.handler.syncOnlineMutation(f.File); err != nil {
		return n, err
	}
	if err = f.verifyMutation(m); err != nil {
		return n, err
	}
	if err = f.publishMutation(m); err != nil {
		return n, err
	}
	op.poison = false
	return n, nil
}
