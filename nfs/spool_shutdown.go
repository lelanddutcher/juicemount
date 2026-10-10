package nfs

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
	"os"
)

// Captured once from O_EXCL, never reconstructed from a boot/pathname reopen.
func liveSpoolIdentity(fd *os.File) (string, error) {
	var st unix.Stat_t
	if err := unix.Fstat(int(fd.Fd()), &st); err != nil {
		return "", err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Nlink != 1 {
		return "", metadata.ErrSpoolContinuationConflict
	}
	return fmt.Sprintf("%d:%d", uint64(st.Dev), st.Ino), nil
}

// finalizeQuiescent is shutdown-only: RPC/namespace admission, sweeper and
// drainer must already be joined. It never contacts a backend or wakes drain.
// Refused images stay in place; boot will hold unproved writing rows.
func (s *SpoolStore) finalizeQuiescent(ctx context.Context) {
	s.closed.Store(true)
	s.lockAllShards() // join OpenWrite calls admitted before closed was set
	entries := s.index.Snapshot()
	s.unlockAllShards()
	for _, e := range entries {
		if ctx.Err() != nil {
			return
		}
		if err := s.finalizeQuiescentEntry(ctx, e); err != nil {
			jmlog.Warn("spool: shutdown retained unqualified image", "entry_id", e.id, "error", err.Error())
		}
	}
}

func (s *SpoolStore) finalizeQuiescentEntry(ctx context.Context, e *SpoolEntry) error {
	// Namespace is quiescent; the shard also excludes direct spool admission.
	e.mu.RLock()
	path := e.nfsPath
	e.mu.RUnlock()
	shard := s.pathShard(path)
	shard.Lock()
	defer shard.Unlock()
	if s.hasPendingBoundContinuation(e.id) || s.hasPendingBoundContinuationPath(path) {
		return metadata.ErrSpoolContinuationConflict
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	current, ok := s.index.Lookup(path)
	if !ok || current != e || e.retired.Load() || e.readBlocked.Load() || e.replacement != nil || e.refcount != 0 ||
		e.file == nil || e.initialIdentity == "" || e.shutdownSealed || e.coverageLost || e.writeFailed.Load() || e.streamDest != "" || e.punchedEnd != 0 ||
		e.contiguousEnd != e.writtenEnd || len(e.writtenExtents) != 0 {
		e.mu.Unlock()
		return metadata.ErrSpoolContinuationConflict
	}
	e.shutdownSealed = true
	fd, id, file, size := e.file, e.id, e.spoolFile, e.writtenEnd
	e.mu.Unlock() // never enter Store publication locks with e.mu held
	c, err := s.meta.InspectQuiescentSpool(id, path, file, e.Inode())
	if err != nil {
		return err
	}
	if !c.Legacy || c.State != string(metadata.DrainWriting) {
		return metadata.ErrSpoolContinuationConflict
	}
	verify := func() error {
		if _, err := s.qualifySpoolImage(file, e.initialIdentity, size, fd); err != nil {
			return err
		}
		identity, err := liveSpoolIdentity(fd)
		_, n, sizeErr := birthFDIdentity(fd)
		if err != nil || sizeErr != nil || identity != e.initialIdentity || n != size {
			return metadata.ErrSpoolContinuationConflict
		}
		return nil
	}
	if err := verify(); err != nil {
		return err
	}
	before, err := fd.Stat()
	if err != nil {
		return err
	}
	if s.shutdownFault != nil {
		if err := s.shutdownFault("sync"); err != nil {
			return err
		}
	}
	if err := fd.Sync(); err != nil {
		return err
	}
	h := sha256.New()
	buf := make([]byte, 256<<10)
	var offset int64
	for offset < size {
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.shutdownFault != nil {
			if err := s.shutdownFault("hash"); err != nil {
				return err
			}
		}
		take := int64(len(buf))
		if size-offset < take {
			take = size - offset
		}
		n, err := fd.ReadAt(buf[:take], offset)
		if err != nil || int64(n) != take {
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			return fmt.Errorf("shutdown image read: %w", err)
		}
		_, _ = h.Write(buf[:n])
		offset += int64(n)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := verify(); err != nil {
		return err
	}
	after, err := fd.Stat()
	if err != nil {
		return err
	}
	if !before.ModTime().Equal(after.ModTime()) {
		return metadata.ErrSpoolContinuationConflict
	}
	if current, ok := s.index.Lookup(path); !ok || current != e {
		return metadata.ErrSpoolContinuationConflict
	}
	digest := h.Sum(nil)
	if s.shutdownFault != nil {
		if err := s.shutdownFault("cas"); err != nil {
			return err
		}
	}
	if err := s.meta.MarkQuiescentReady(c, size, digest); err != nil {
		return err
	}
	e.mu.Lock()
	e.closed = true
	e.writing.Store(false)
	e.sha256 = digest
	e.file = nil
	e.mu.Unlock()
	// The descriptor was fsynced and durably qualified before closing it.
	return fd.Close()
}

// One worker survives deadline expiry. Retrying Stop waits for that original
// worker, never closes its dependencies or starts a duplicate finalization.
func (h *JuiceMountHandler) stopSpoolFinalization(spool *SpoolStore, deadline time.Duration) bool {
	if h.spoolFinalizeDone == nil {
		h.spoolFinalizeDone = make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		h.spoolFinalizeCancel = cancel
		go func() { defer close(h.spoolFinalizeDone); defer cancel(); spool.finalizeQuiescent(ctx) }()
	}
	select {
	case <-h.spoolFinalizeDone:
		return true
	case <-time.After(deadline):
		h.spoolFinalizeCancel()
		return false
	}
}
