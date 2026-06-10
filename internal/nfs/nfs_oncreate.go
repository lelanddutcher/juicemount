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

const (
	createModeUnchecked = 0
	createModeGuarded   = 1
	createModeExclusive = 2
)

func onCreate(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = wccDataErrorFormatter
	obj := DirOpArg{}
	err := xdr.Read(w.req.Body, &obj)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	how, err := xdr.ReadUint32(w.req.Body)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	var attrs *SetFileAttributes
	if how == createModeUnchecked || how == createModeGuarded {
		sattr, err := ReadSetFileAttributes(w.req.Body)
		if err != nil {
			return &NFSStatusError{NFSStatusInval, err}
		}
		attrs = sattr
	} else if how == createModeExclusive {
		// [JM5] Treat exclusive create as unchecked create.
		// macOS uses exclusive mode for atomic file creation. We read and
		// discard the createverf3 verifier, then proceed with normal create.
		// This eliminates the "exclusive mode not supported" error that caused
		// macOS to retry with unchecked mode (adding latency to every create).
		var verf [8]byte
		if err := xdr.Read(w.req.Body, &verf); err != nil {
			return &NFSStatusError{NFSStatusInval, err}
		}
		// No attrs to apply for exclusive creates
	} else {
		// invalid
		return &NFSStatusError{NFSStatusNotSupp, os.ErrInvalid}
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

	newFile := append(path, string(obj.Filename))
	newFilePath := fs.Join(newFile...)

	// [JM5] Existence check: only reject if the entry is a directory.
	// For guarded creates of regular files, proceed with Create() and let
	// the backing FUSE filesystem be the arbiter. The metadata cache may
	// contain stale entries from previous failed copies or Redis sync that
	// would incorrectly block Finder from creating new files.
	if s, err := fs.Stat(newFilePath); err == nil && s.IsDir() {
		return &NFSStatusError{NFSStatusExist, nil}
	}
	// Verify parent directory exists
	if s, err := fs.Stat(fs.Join(path...)); err != nil {
		return &NFSStatusError{NFSStatusAccess, err}
	} else if !s.IsDir() {
		return &NFSStatusError{NFSStatusNotDir, nil}
	}

	file, err := fs.Create(newFilePath)
	if err != nil {
		Log.Errorf("Error Creating: %v", err)
		// A CREATE against a full write spool (nfs.ErrSpoolFull wraps
		// syscall.ENOSPC) must surface as NFS3ERR_NOSPC ("disk full"),
		// not EACCES. Everything else keeps the legacy Access mapping.
		if errors.Is(err, syscall.ENOSPC) {
			return &NFSStatusError{NFSStatusNoSPC, err}
		}
		return &NFSStatusError{NFSStatusAccess, err}
	}
	if err := file.Close(); err != nil {
		Log.Errorf("Error Creating: %v", err)
		return &NFSStatusError{NFSStatusAccess, err}
	}

	fp := userHandle.ToHandle(fs, newFile)
	changer := userHandle.Change(fs)
	if attrs != nil {
		if err := attrs.Apply(changer, fs, newFilePath); err != nil {
			// [JM5] Non-fatal: attribute application can fail for ._resource
			// fork files that don't persist on JuiceFS. Log but don't fail
			// the create — the file was already created successfully.
			Log.Warnf("onCreate: attrs.Apply failed (non-fatal): %v", err)
		}
	}

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	// "handle follows"
	if err := xdr.Write(writer, uint32(1)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := xdr.Write(writer, fp); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WritePostOpAttrs(writer, tryStat(fs, []string{file.Name()})); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	// dir_wcc (we don't include pre_op_attr)
	if err := xdr.Write(writer, uint32(0)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WritePostOpAttrs(writer, tryStat(fs, path)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}
