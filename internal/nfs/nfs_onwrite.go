package nfs

import (
	"context"
	"io"
	"math"
	"os"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// writeStability is the level of durability requested with the write
type writeStability uint32

const (
	unstable writeStability = 0
	dataSync writeStability = 1
	fileSync writeStability = 2
)

type writeArgs struct {
	Handle []byte
	Offset uint64
	Count  uint32
	How    uint32
	Data   []byte
}

func onWrite(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = wccDataErrorFormatter
	var req writeArgs
	if err := xdr.Read(w.req.Body, &req); err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}

	fs, path, err := userHandle.FromHandle(req.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	if !billy.CapabilityCheck(fs, billy.WriteCapability) {
		return &NFSStatusError{NFSStatusROFS, os.ErrPermission}
	}
	if len(req.Data) > math.MaxInt32 || req.Count > math.MaxInt32 {
		return &NFSStatusError{NFSStatusFBig, os.ErrInvalid}
	}
	if req.How != uint32(unstable) && req.How != uint32(dataSync) && req.How != uint32(fileSync) {
		return &NFSStatusError{NFSStatusInval, os.ErrInvalid}
	}

	// Skip pre-op stat for performance — it adds a full stat() per WRITE RPC.
	// WCC (weak cache consistency) data is optional per RFC 1813 §2.6.
	fullPath := fs.Join(path...)

	// now the actual op.
	file, err := fs.OpenFile(fullPath, os.O_RDWR, 0644)
	if err != nil {
		return &NFSStatusError{NFSStatusAccess, err}
	}
	if req.Offset > 0 {
		if _, err := file.Seek(int64(req.Offset), io.SeekStart); err != nil {
			return &NFSStatusError{NFSStatusIO, err}
		}
	}
	end := req.Count
	if len(req.Data) < int(end) {
		end = uint32(len(req.Data))
	}
	writtenCount, err := file.Write(req.Data[:end])
	if err != nil {
		Log.Errorf("Error writing: %v", err)
		return &NFSStatusError{NFSStatusIO, err}
	}
	if err := file.Close(); err != nil {
		Log.Errorf("error closing: %v", err)
		return &NFSStatusError{NFSStatusIO, err}
	}

	// [JM5] Use pooled response buffer instead of allocating.
	writer := getResponseBuffer()
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		putResponseBuffer(writer)
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := WriteWcc(writer, nil, tryStat(fs, path)); err != nil {
		putResponseBuffer(writer)
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := xdr.Write(writer, uint32(writtenCount)); err != nil {
		putResponseBuffer(writer)
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := xdr.Write(writer, unstable); err != nil {
		putResponseBuffer(writer)
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := xdr.Write(writer, w.Server.ID); err != nil {
		putResponseBuffer(writer)
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	writeErr := w.Write(writer.Bytes())
	putResponseBuffer(writer)
	if writeErr != nil {
		return &NFSStatusError{NFSStatusServerFault, writeErr}
	}
	return nil
}
