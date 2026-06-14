package nfs

import (
	"bytes"
	"context"
	"errors"
	"os"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

func lookupSuccessResponse(handle []byte, entPath, dirPath []string, fs billy.Filesystem) ([]byte, error) {
	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return nil, err
	}
	if err := xdr.Write(writer, handle); err != nil {
		return nil, err
	}
	if err := WritePostOpAttrs(writer, tryStat(fs, entPath)); err != nil {
		return nil, err
	}
	if err := WritePostOpAttrs(writer, tryStat(fs, dirPath)); err != nil {
		return nil, err
	}
	return writer.Bytes(), nil
}

func onLookup(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = opAttrErrorFormatter
	obj := DirOpArg{}
	err := xdr.Read(w.req.Body, &obj)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}

	fs, p, err := userHandle.FromHandle(obj.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	// A FUSE-stat TIMEOUT verifying the parent dir must NOT fail the lookup.
	// `p` is a directory by construction (the client holds its handle and is
	// looking up a child in it); under heavy copy load this sanity Lstat can
	// fall through to a contended FUSE mount (parent LRU-evicted mid-large-copy)
	// and exceed the 2s bound. Returning NotDir wrapping ErrFUSETimeout makes
	// the RPC layer reply NFS3ERR_JUKEBOX (conn.go), so the client RETRIES the
	// LOOKUP; during a sustained slow spell those retries storm and accumulate
	// past the ~40s soft-mount timeout → "error 100060" aborts the copy
	// (2026-06-14: a LOOKUP storm of +22k retries was the observed stall). On an
	// ambiguous timeout, skip the check and proceed to the child Lstat below
	// (which resolves from cache, or returns NoEnt so a CREATE can proceed).
	dirInfo, err := fs.Lstat(fs.Join(p...))
	if err != nil {
		if !errors.Is(err, ErrFUSETimeout) {
			return &NFSStatusError{NFSStatusNotDir, err}
		}
	} else if !dirInfo.IsDir() {
		return &NFSStatusError{NFSStatusNotDir, err}
	}

	// Special cases for "." and ".."
	if bytes.Equal(obj.Filename, []byte(".")) {
		resp, err := lookupSuccessResponse(obj.Handle, p, p, fs)
		if err != nil {
			return &NFSStatusError{NFSStatusServerFault, err}
		}
		if err := w.Write(resp); err != nil {
			return &NFSStatusError{NFSStatusServerFault, err}
		}
		return nil
	}
	if bytes.Equal(obj.Filename, []byte("..")) {
		if len(p) == 0 {
			return &NFSStatusError{NFSStatusAccess, os.ErrPermission}
		}
		pPath := p[0 : len(p)-1]
		pHandle := userHandle.ToHandle(fs, pPath)
		resp, err := lookupSuccessResponse(pHandle, pPath, p, fs)
		if err != nil {
			return &NFSStatusError{NFSStatusServerFault, err}
		}
		if err := w.Write(resp); err != nil {
			return &NFSStatusError{NFSStatusServerFault, err}
		}
		return nil
	}

	reqPath := append(p, string(obj.Filename))
	if _, err = fs.Lstat(fs.Join(reqPath...)); err != nil {
		// [JM6 tier-1.7] See nfs_ongetattr.go comment — offline fail-
		// fast must not invalidate the kernel's file handle cache.
		if pin.IsOfflineNotAvailable(err) {
			return &NFSStatusError{NFSStatusNXIO, err}
		}
		return &NFSStatusError{NFSStatusNoEnt, os.ErrNotExist}
	}

	newHandle := userHandle.ToHandle(fs, reqPath)
	resp, err := lookupSuccessResponse(newHandle, reqPath, p, fs)
	if err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := w.Write(resp); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}
