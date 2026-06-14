package nfs

import (
	"bytes"
	"context"
	"os"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// Committer is optionally implemented by a billy.Filesystem whose writes are
// buffered (spooled to local disk before async upload). NFS COMMIT, and a
// WRITE with FILE_SYNC/DATA_SYNC stability, call CommitFile to force a
// durability barrier (fsync) on the buffered data for a path so it survives a
// power loss — WITHOUT finalizing the file (the writer may continue). A
// filesystem whose writes are already durable need not implement it.
type Committer interface {
	CommitFile(path string) error
}

// onCommit makes the data written to a path durable, honoring the NFSv3 COMMIT
// contract: after a successful COMMIT the client is entitled to drop its copy.
// For a spooled filesystem that means fsyncing the on-disk spool file; a no-op
// COMMIT (the prior behavior) was a durability LIE that lost acknowledged bytes
// on power loss.
func onCommit(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = wccDataErrorFormatter
	handle, err := xdr.ReadOpaque(w.req.Body)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	// The conn will drain the unread offset and count arguments.

	fs, path, err := userHandle.FromHandle(handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	if !billy.CapabilityCheck(fs, billy.WriteCapability) {
		return &NFSStatusError{NFSStatusServerFault, os.ErrPermission}
	}

	// Durability barrier: fsync the spooled bytes for this path before we tell
	// the client the commit succeeded.
	if c, ok := fs.(Committer); ok {
		if cErr := c.CommitFile(fs.Join(path...)); cErr != nil {
			return &NFSStatusError{NFSStatusIO, cErr}
		}
	}

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return err
	}

	// no pre-op cache data.
	if err := xdr.Write(writer, uint32(0)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WritePostOpAttrs(writer, tryStat(fs, path)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	// write the 8 bytes of write verification.
	if err := xdr.Write(writer, w.Server.ID); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}
