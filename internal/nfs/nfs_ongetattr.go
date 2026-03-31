package nfs

import (
	"context"
	"os"

	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

func onGetAttr(ctx context.Context, w *response, userHandle Handler) error {
	handle, err := xdr.ReadOpaque(w.req.Body)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}

	fs, path, err := userHandle.FromHandle(handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}

	fullPath := fs.Join(path...)
	info, err := fs.Lstat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		return &NFSStatusError{NFSStatusIO, err}
	}

	// [JM5] Fast path: if the underlying FileInfo has pre-serialized XDR
	// bytes (from our metadata cache), skip XDR marshaling entirely.
	// This eliminates reflection-based encoding on the hottest NFS path.
	if fi, ok := info.(*metadata.FileInfo); ok {
		if entry := fi.Entry(); entry != nil && entry.PreSerializedGetAttr != nil {
			if err := w.Write(entry.PreSerializedGetAttr); err != nil {
				return &NFSStatusError{NFSStatusServerFault, err}
			}
			return nil
		}
	}

	// Slow path: compute FileAttribute and encode via XDR.
	attr := ToFileAttribute(info, fullPath)

	// [JM5] Use pooled buffer instead of allocating a new one.
	writer := getResponseBuffer()
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		putResponseBuffer(writer)
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := xdr.Write(writer, attr); err != nil {
		putResponseBuffer(writer)
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	bodyBytes := make([]byte, writer.Len())
	copy(bodyBytes, writer.Bytes())
	putResponseBuffer(writer)

	// Cache the pre-serialized bytes on the Entry for next time.
	if fi, ok := info.(*metadata.FileInfo); ok {
		if entry := fi.Entry(); entry != nil {
			entry.PreSerializedGetAttr = bodyBytes
		}
	}

	if err := w.Write(bodyBytes); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}
