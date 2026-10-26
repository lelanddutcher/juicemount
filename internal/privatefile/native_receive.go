package privatefile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

const nativeReceiveMaximum = int64(1 << 30)
const nativeReceiveChunk = 1 << 20

// lockCustody owns the original profile flock for one short file mutation.
// It validates the same pinned namespace and accounting-file identity as the
// full inventory owner, but opens no SQLite connection and scans no rows. A
// network stall or whole-file hash never holds this lock.
func (r *NativeRoot) lockCustody() (_ *NativeControl, resultErr error) {
	if err := r.take(); err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			r.leave()
		}
	}()
	lock, err := OpenAt(r.root, "control.lock", os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, lock.Close())
		}
	}()
	id, err := privateRegular(lock)
	if err != nil || id.Size != 0 {
		return nil, errors.Join(err, ErrNativeCopyChanged)
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, fmt.Errorf("native custody lock: %w", ErrNativeCopyBusy)
	}
	check, err := OpenAt(r.root, "control.lock", os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	checkID, checkErr := privateRegular(check)
	closeErr := check.Close()
	if checkErr != nil || closeErr != nil || checkID != id {
		return nil, errors.Join(checkErr, closeErr, ErrNativeCopyChanged)
	}
	if err := r.Revalidate(); err != nil {
		return nil, err
	}
	if err := r.revalidateInventory(); err != nil {
		return nil, err
	}
	return &NativeControl{root: r, lock: lock, lockID: id}, nil
}

// HashPartial reads only the held, private partial under its copy and control
// owners. The returned prefix is the checkpoint prefix; whole is the actual
// durable tail used for the next same-pin transfer request. Neither digest is
// a server receipt or publication authority.
func (n *NativeCopy) HashPartial(ctx context.Context, prefixLength int64) (actual Identity, prefixSHA256, wholeSHA256 string, resultErr error) {
	if ctx == nil || n == nil || prefixLength < 0 {
		return Identity{}, "", "", ErrNativeCopyChanged
	}
	if err := n.Revalidate(); err != nil {
		return Identity{}, "", "", err
	}
	f, err := OpenAt(n.dir, "payload.partial", os.O_RDONLY, 0)
	if err != nil {
		return Identity{}, "", "", err
	}
	defer func() {
		if e := f.Close(); e != nil {
			resultErr = errors.Join(resultErr, e)
			actual = Identity{}
			prefixSHA256 = ""
			wholeSHA256 = ""
		}
	}()
	actual, err = privateRegular(f)
	if err != nil || actual.Size < prefixLength || actual.Size > nativeReceiveMaximum {
		return Identity{}, "", "", errors.Join(err, ErrNativeCopyChanged)
	}
	prefix, whole := sha256.New(), sha256.New()
	buf := make([]byte, 32768)
	for at := int64(0); at < actual.Size; {
		if err := ctx.Err(); err != nil {
			return Identity{}, "", "", err
		}
		want := min(int64(len(buf)), actual.Size-at)
		read, err := f.ReadAt(buf[:int(want)], at)
		if err != nil || int64(read) != want {
			return Identity{}, "", "", errors.Join(err, io.ErrUnexpectedEOF)
		}
		_, _ = whole.Write(buf[:read])
		if at < prefixLength {
			_, _ = prefix.Write(buf[:int(min(want, prefixLength-at))])
		}
		at += want
	}
	if after, err := privateRegular(f); err != nil || after != actual {
		return Identity{}, "", "", errors.Join(err, ErrNativeCopyChanged)
	}
	if current, err := n.PartialIdentity(); err != nil || current != actual {
		return Identity{}, "", "", errors.Join(err, ErrNativeCopyChanged)
	}
	if err := n.Revalidate(); err != nil {
		return Identity{}, "", "", err
	}
	return actual, hex.EncodeToString(prefix.Sum(nil)), hex.EncodeToString(whole.Sum(nil)), nil
}

// NativeAppend owns one original O_APPEND descriptor. It never seeks,
// truncates, replaces, or returns the descriptor. Close syncs and joins it even
// after a short/failed Write; the caller must inspect the actual tail on retry.
type NativeAppend struct {
	copy     *NativeCopy
	file     *os.File
	id       Identity
	maximum  int64
	closed   bool
	failed   error
	closeErr error
}

func (n *NativeCopy) OpenAppend(expected Identity, maximum int64) (result *NativeAppend, resultErr error) {
	if n == nil || maximum < 0 || maximum > nativeReceiveMaximum || expected.Size < 0 || expected.Size > maximum {
		return nil, ErrNativeCopyChanged
	}
	control, err := n.root.lockCustody()
	if err != nil {
		return nil, err
	}
	defer func() {
		if e := control.Close(); e != nil {
			resultErr = errors.Join(resultErr, e, ErrNativeCopyUncertain)
			if result != nil {
				resultErr = errors.Join(resultErr, result.file.Close())
				result = nil
			}
		}
	}()
	if err := n.Revalidate(); err != nil {
		return nil, err
	}
	f, err := OpenAt(n.dir, "payload.partial", os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, f.Close())
		}
	}()
	id, err := privateRegular(f)
	if err != nil || id != expected {
		return nil, errors.Join(err, ErrNativeCopyChanged)
	}
	if current, err := n.PartialIdentity(); err != nil || current != id {
		return nil, errors.Join(err, ErrNativeCopyChanged)
	}
	return &NativeAppend{copy: n, file: f, id: id, maximum: maximum}, nil
}

func (a *NativeAppend) Write(p []byte) (written int, resultErr error) {
	if a == nil || a.closed || a.file == nil {
		return 0, ErrNativeCopyChanged
	}
	if a.failed != nil {
		return 0, a.failed
	}
	if len(p) == 0 {
		return 0, nil
	}
	if len(p) > nativeReceiveChunk || int64(len(p)) > a.maximum-a.id.Size {
		a.failed = ErrNativeCopyChanged
		return 0, a.failed
	}
	control, err := a.copy.root.lockCustody()
	if err != nil {
		a.failed = err
		return 0, err
	}
	defer func() {
		if e := control.Close(); e != nil {
			resultErr = errors.Join(resultErr, e, ErrNativeCopyUncertain)
			a.failed = resultErr
		}
	}()
	if err := a.copy.Revalidate(); err != nil {
		a.failed = err
		return 0, err
	}
	if current, err := privateRegular(a.file); err != nil || current != a.id {
		a.failed = errors.Join(err, ErrNativeCopyChanged)
		return 0, a.failed
	}
	if current, err := a.copy.PartialIdentity(); err != nil || current != a.id {
		a.failed = errors.Join(err, ErrNativeCopyChanged)
		return 0, a.failed
	}
	written, err = a.file.Write(p)
	if err != nil || written != len(p) {
		a.failed = errors.Join(err, io.ErrShortWrite, ErrNativeCopyUncertain)
		return written, a.failed
	}
	a.id.Size += int64(written)
	if current, err := privateRegular(a.file); err != nil || current != a.id {
		a.failed = errors.Join(err, ErrNativeCopyChanged, ErrNativeCopyUncertain)
		return written, a.failed
	}
	return written, nil
}

func (a *NativeAppend) Close() error {
	if a == nil {
		return nil
	}
	if a.closed {
		return a.closeErr
	}
	a.closed = true
	control, lockErr := a.copy.root.lockCustody()
	if lockErr != nil {
		a.closeErr = errors.Join(lockErr, a.file.Close(), ErrNativeCopyUncertain)
		return a.closeErr
	}
	a.closeErr = errors.Join(a.file.Sync(), a.copy.dir.Sync(), a.file.Close())
	if a.closeErr != nil {
		a.closeErr = errors.Join(a.closeErr, ErrNativeCopyUncertain)
	}
	if current, err := a.copy.PartialIdentity(); err != nil || current.Device != a.id.Device || current.Inode != a.id.Inode || current.UID != a.id.UID || current.Mode != a.id.Mode || current.Size > a.maximum {
		a.closeErr = errors.Join(a.closeErr, err, ErrNativeCopyChanged)
	}
	if err := control.Close(); err != nil {
		a.closeErr = errors.Join(a.closeErr, err, ErrNativeCopyUncertain)
	}
	return a.closeErr
}

// These fixed-purpose custody barriers reacquire the profile control owner
// briefly after the network and whole-file hash have finished. They never
// return a generic file path, descriptor, or arbitrary record kind.
func (n *NativeCopy) WritePreparedOwned(line []byte, target Identity) (resultErr error) {
	if n == nil {
		return ErrNativeCopyChanged
	}
	control, err := n.root.LockControl()
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, control.Close()) }()
	actual, names, err := control.TargetDirectory(n.id)
	if err != nil || actual != target || len(names) != 0 {
		return errors.Join(err, ErrNativeCopyChanged)
	}
	return n.writeRecordUnder(control, "prepared", line)
}
func (n *NativeCopy) SyncPreparedOwned() (resultErr error) {
	if n == nil {
		return ErrNativeCopyChanged
	}
	control, err := n.root.LockControl()
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, control.Close()) }()
	return n.syncLeafUnder(control, "prepared.json")
}
func (n *NativeCopy) WriteVerifiedOwned(line []byte) (resultErr error) {
	if n == nil {
		return ErrNativeCopyChanged
	}
	control, err := n.root.LockControl()
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, control.Close()) }()
	return n.writeRecordUnder(control, "verified", line)
}

func (n *NativeCopy) ReadPreparedOwned() (line []byte, resultErr error) {
	if n == nil {
		return nil, ErrNativeCopyChanged
	}
	control, err := n.root.LockControl()
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, control.Close()) }()
	if err := n.Revalidate(); err != nil {
		return nil, err
	}
	return readNativeRecord(n.dir, "prepared")
}

func (n *NativeCopy) ReadVerifiedOwned() (line []byte, resultErr error) {
	if n == nil {
		return nil, ErrNativeCopyChanged
	}
	control, err := n.root.LockControl()
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, control.Close()) }()
	if err := n.Revalidate(); err != nil {
		return nil, err
	}
	return readNativeRecord(n.dir, "verified")
}
func (n *NativeCopy) SyncVerifiedOwned() (resultErr error) {
	if n == nil {
		return ErrNativeCopyChanged
	}
	control, err := n.root.LockControl()
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, control.Close()) }()
	return n.syncLeafUnder(control, "verified.json")
}
