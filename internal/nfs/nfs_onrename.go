package nfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"reflect"
	"syscall"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

var doubleWccErrorBody = [16]byte{}

// renameErrStatus maps a Rename failure to its NFS status.
//
// A destination collision is NOT an I/O error (2026-08-03). Every other
// create-shaped op in this package — mkdir, mknod, link, symlink — already maps
// EEXIST to NFSStatusExist; rename was the sole holdout and fell through to
// NFSStatusIO. The macOS client surfaces NFS3ERR_IO as ioErr, so Finder aborted
// a folder move with error -36 instead of treating it as a name collision it
// can resolve (merge/replace), and left the folder split across source and
// destination.
//
// Measured before the fix: moving a 300-file folder immediately after writing
// it failed ~50% of the time, on this branch AND on stock 0.4.0. The
// JM_NFS_TRACE line was:
//
//	Rename -> ERR I/O error: rename .../src/big .../dst/big: file exists
//
// ENOTEMPTY is reported separately — NFS3ERR_NOTEMPTY is the specific status
// for "target directory exists and is not empty", and clients distinguish it
// from a plain EEXIST. It must be checked BEFORE the EEXIST case: on some
// platforms a non-empty-target rename satisfies both.
func renameErrStatus(err error) NFSStatus {
	switch {
	case os.IsNotExist(err):
		return NFSStatusNoEnt
	case os.IsPermission(err):
		return NFSStatusAccess
	case errors.Is(err, syscall.ENOTEMPTY):
		return NFSStatusNotEmpty
	case errors.Is(err, os.ErrExist), errors.Is(err, syscall.EEXIST):
		return NFSStatusExist
	default:
		return NFSStatusIO
	}
}

func onRename(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = errFormatterWithBody(doubleWccErrorBody[:])
	from := DirOpArg{}
	err := xdr.Read(w.req.Body, &from)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	fs, fromPath, err := userHandle.FromHandle(from.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}

	to := DirOpArg{}
	if err = xdr.Read(w.req.Body, &to); err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	fs2, toPath, err := userHandle.FromHandle(to.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	// check the two fs are the same
	if !reflect.DeepEqual(fs, fs2) {
		return &NFSStatusError{NFSStatusNotSupp, os.ErrPermission}
	}

	if !billy.CapabilityCheck(fs, billy.WriteCapability) {
		return &NFSStatusError{NFSStatusROFS, os.ErrPermission}
	}

	if len(string(from.Filename)) > PathNameMax || len(string(to.Filename)) > PathNameMax {
		return &NFSStatusError{NFSStatusNameTooLong, os.ErrInvalid}
	}

	fromDirPath := fs.Join(fromPath...)
	fromDirInfo, err := fs.Stat(fromDirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		return &NFSStatusError{NFSStatusIO, err}
	}
	if !fromDirInfo.IsDir() {
		return &NFSStatusError{NFSStatusNotDir, nil}
	}
	preCacheData := ToFileAttribute(fromDirInfo, fromDirPath).AsCache()

	toDirPath := fs.Join(toPath...)
	toDirInfo, err := fs.Stat(toDirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		return &NFSStatusError{NFSStatusIO, err}
	}
	if !toDirInfo.IsDir() {
		return &NFSStatusError{NFSStatusNotDir, nil}
	}
	preDestData := ToFileAttribute(toDirInfo, toDirPath).AsCache()

	oldHandle := userHandle.ToHandle(fs, append(fromPath, string(from.Filename)))

	fromLoc := fs.Join(append(fromPath, string(from.Filename))...)
	toLoc := fs.Join(append(toPath, string(to.Filename))...)

	err = fs.Rename(fromLoc, toLoc)
	if err != nil {
		return &NFSStatusError{renameErrStatus(err), err}
	}

	if err := userHandle.InvalidateHandle(fs, oldHandle); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := WriteWcc(writer, preCacheData, tryStat(fs, fromPath)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WriteWcc(writer, preDestData, tryStat(fs, toPath)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}
