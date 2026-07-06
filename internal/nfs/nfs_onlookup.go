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
	// OFFLINE the same applies: this FUSE Lstat returns ErrOfflineNotAvailable,
	// which must NOT fail the lookup — the parent is a dir by construction and
	// the child resolves from the cache below. Treating it as NotDir aborted an
	// offline copy with "device disappeared" (2026-06-14, offline-ingest).
	dirInfo, err := fs.Lstat(fs.Join(p...))
	if err != nil {
		if !errors.Is(err, ErrFUSETimeout) && !pin.IsOfflineNotAvailable(err) {
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
	// Resolve the child from the metadata cache with NO FUSE round-trip — BOTH
	// online AND offline. A cache MISS returns NoEnt: for a copy that's a
	// brand-new name the client is about to CREATE, and the cache mirrors the
	// full backend tree (the same completeness offline-navigation relies on).
	//
	// OFFLINE this is critical: the kernel does a LOOKUP before EVERY
	// CREATE/MKDIR, and the old offline path took a FUSE Lstat that returns
	// ErrOfflineNotAvailable → NXIO, which aborted an offline copy with Finder
	// "the device disappeared" (2026-06-14, offline-ingest sprint). Returning
	// NoEnt instead lets the create proceed (into the spool / cache). This does
	// NOT invalidate live handles: a file the kernel holds a handle for was
	// looked up before, so it's in the cache → a cache HIT here → resolves
	// fine. Data-unavailability offline is enforced at the READ layer
	// (cachedFile.ReadAt → NXIO), where it belongs — not at lookup. Online this
	// also avoided an 800ms FUSE Lstat per new file (deep-tree "error 100060").
	// Filesystems without the cache-only stat keep the FUSE-Lstat path (offline
	// cache-miss → NXIO via the handle-preservation branch below).
	if cs, ok := fs.(cacheStater); ok {
		if _, found := cs.StatCacheOnly(fs.Join(reqPath...)); !found {
			return &NFSStatusError{NFSStatusNoEnt, os.ErrNotExist}
		}
	} else if _, err = fs.Lstat(fs.Join(reqPath...)); err != nil {
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
