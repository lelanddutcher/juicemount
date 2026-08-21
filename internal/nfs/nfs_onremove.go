package nfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"syscall"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

func onRemove(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = wccDataErrorFormatter
	obj := DirOpArg{}
	if err := xdr.Read(w.req.Body, &obj); err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	fs, path, err := userHandle.FromHandle(obj.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}

	if !billy.CapabilityCheck(fs, billy.WriteCapability) {
		return &NFSStatusError{NFSStatusROFS, os.ErrPermission}
	}

	if len(string(obj.Filename)) > PathNameMax {
		return &NFSStatusError{NFSStatusNameTooLong, nil}
	}

	fullPath := fs.Join(path...)
	dirInfo, err := fs.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		if os.IsPermission(err) {
			return &NFSStatusError{NFSStatusAccess, err}
		}
		return &NFSStatusError{NFSStatusIO, err}
	}
	if !dirInfo.IsDir() {
		return &NFSStatusError{NFSStatusNotDir, nil}
	}
	preCacheData := ToFileAttribute(dirInfo, fullPath).AsCache()

	toDelete := fs.Join(append(path, string(obj.Filename))...)
	toDeleteHandle := userHandle.ToHandle(fs, append(path, string(obj.Filename)))

	err = fs.Remove(toDelete)
	if err != nil {
		if status, mapped := removeErrStatus(err); mapped {
			return status
		}
		return &NFSStatusError{NFSStatusIO, err}
	}

	if err := userHandle.InvalidateHandle(fs, toDeleteHandle); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := WriteWcc(writer, preCacheData, tryStat(fs, path)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}

// removeErrStatus maps a failed fs.Remove/RmDir to the precise NFSv3 status.
// Extracted for direct unit testing. mapped=false → caller falls back to IO.
//
// ENOTEMPTY previously fell through to NFS3ERR_IO, so Finder rendered a
// normal user mistake ("folder isn't empty") as an I/O error.
func removeErrStatus(err error) (*NFSStatusError, bool) {
	switch {
	case os.IsNotExist(err):
		return &NFSStatusError{NFSStatusNoEnt, err}, true
	case os.IsPermission(err):
		return &NFSStatusError{NFSStatusAccess, err}, true
	case errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EEXIST):
		return &NFSStatusError{NFSStatusNotEmpty, err}, true
	}
	return nil, false
}
