package nfs

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"syscall"

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
		// A WRITE RPC can be the moment the spool's capacity cap trips
		// (handler OpenFile → spool.OpenWrite → ErrSpoolFull, which
		// wraps syscall.ENOSPC). That must surface as NFS3ERR_NOSPC,
		// not EACCES. Everything else keeps the legacy Access mapping.
		if errors.Is(err, syscall.ENOSPC) {
			return &NFSStatusError{NFSStatusNoSPC, err}
		}
		return &NFSStatusError{NFSStatusAccess, err}
	}
	end := req.Count
	if len(req.Data) < int(end) {
		end = uint32(len(req.Data))
	}

	// QA-14 fix (2026-05-17): use WriteAt(data, offset) instead of
	// Seek+Write. Seek+Write is TWO separate syscalls that share the
	// fd's internal position. Under concurrent dispatch (iter 1's
	// change in 691f550), two parallel WRITE RPCs hitting the same
	// pooled fd would race on the offset: RPC_A.Seek(0) +
	// RPC_B.Seek(1MB) + RPC_A.Write + RPC_B.Write interleaves
	// chaotically and writes both RPC payloads at the WRONG offsets.
	// Result: file ends up at the right total size but with bytes
	// shuffled — the exact "size right, content wrong" pattern
	// QA-14 documented.
	//
	// WriteAt(p, off) is a single atomic positioned write (pwrite(2)
	// on Unix) — no fd-internal offset involved, no race possible.
	// billy.File doesn't require WriteAt, so we type-assert to
	// io.WriterAt. Our handler's writeFile implements it (see
	// nfs/handler.go:1112); billyFile embeds *os.File which has
	// WriteAt natively. The fallback path covers the contract.
	var writtenCount int
	if wa, ok := file.(io.WriterAt); ok {
		writtenCount, err = wa.WriteAt(req.Data[:end], int64(req.Offset))
	} else {
		// Fallback for billy.File implementations without WriteAt.
		// NOT race-safe — but the only callers we care about
		// (writeFile, billyFile) both implement WriterAt. Logging
		// this branch so a regression is visible.
		Log.Errorf("write fallback: file %T missing WriteAt — RACE-PRONE; offset=%d", file, req.Offset)
		if req.Offset > 0 {
			if _, err := file.Seek(int64(req.Offset), io.SeekStart); err != nil {
				// Phase-1 BUG 2: every error return after a successful
				// OpenFile must Close — a dropped handle leaks the spool
				// entry's refcount and the entry never finalizes/drains.
				_ = file.Close()
				return &NFSStatusError{NFSStatusIO, err}
			}
		}
		writtenCount, err = file.Write(req.Data[:end])
	}
	if err != nil {
		Log.Errorf("Error writing: %v", err)
		_ = file.Close() // Phase-1 BUG 2: see Seek-error comment above
		// nfsStatusErrorFrom instead of a hard NFSStatusIO so a full
		// write spool (nfs.ErrSpoolFull wraps syscall.ENOSPC) reaches
		// the client as NFS3ERR_NOSPC — "disk full", actionable —
		// rather than a generic I/O error. Unmatched errors still IO.
		return nfsStatusErrorFrom(err)
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
