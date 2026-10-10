package nfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

type replacementCleanupCharge struct {
	size        int64
	closeFailed bool
}

func (s *SpoolStore) rememberReplacementCleanupCharge(row *metadata.SpoolRow) error {
	want := replacementCleanupCharge{size: row.Size}
	got, _ := s.replacementCleanupCharges.LoadOrStore(row.ID, want)
	if got != want {
		return metadata.ErrMirrorConflict
	}
	return nil
}

func (s *SpoolStore) refundReplacementCleanup(row *metadata.SpoolRow) error {
	if got, ok := s.replacementCleanupCharges.Load(row.ID); ok {
		charge := got.(replacementCleanupCharge)
		if charge.closeFailed || charge.size != row.Size {
			return metadata.ErrMirrorConflict
		}
		if s.replacementCleanupCharges.CompareAndDelete(row.ID, charge) {
			s.releaseCapacity(charge.size)
		}
	}
	if entry, ok := s.index.Lookup(row.NFSPath); ok && entry.id == row.ID {
		s.index.DeleteIfMatches(row.NFSPath, entry)
	}
	return nil
}

func (s *SpoolStore) replacementCleanupStep(ctx context.Context, phase string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.replacementCleanupFault != nil {
		return s.replacementCleanupFault(phase)
	}
	return nil
}

// cleanupReplacement is dormant until the complete queue workflow qualifies.
// It mutates only private backup custody, never the served generation. A failed
// attempt remains resumable from its canonical journal without poisoning reads.
func (h *JuiceMountHandler) cleanupReplacement(ctx context.Context, id int64) error {
	d, s := h.drainer.Load(), h.spool.Load()
	if d == nil || s == nil || d.spool != s || s.meta.Owner() != h.store || !d.beginReplacement(id) {
		return metadata.ErrMirrorConflict
	}
	defer d.endReplacement(id)
	return h.cleanupReplacementOwned(ctx, id, d)
}

// cleanupReplacementOwned runs beneath the caller's lifecycle/row claim.
func (h *JuiceMountHandler) cleanupReplacementOwned(ctx context.Context, id int64, d *Drainer) error {
	s := d.spool
	if h.drainer.Load() != d || h.spool.Load() != s || s == nil || s.meta.Owner() != h.store {
		return metadata.ErrMirrorConflict
	}
	row, err := s.meta.Get(id)
	if err != nil {
		return err
	}
	op := h.namespace.begin([]string{row.NFSPath})
	if op == nil {
		return metadata.ErrMirrorConflict
	}
	defer op.finish(nil)
	shard := s.pathShard(row.NFSPath)
	shard.Lock()
	defer shard.Unlock()
	row, err = s.meta.Get(id)
	if err != nil {
		return err
	}
	journal, err := s.meta.ReplacementPublication(row)
	if err != nil || journal == nil {
		return errors.Join(err, metadata.ErrMirrorConflict)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if charge, ok := s.replacementCleanupCharges.Load(id); ok && charge.(replacementCleanupCharge).closeFailed {
		return fmt.Errorf("replacement cleanup requires ordinary reopen after close uncertainty: %w", metadata.ErrMirrorConflict)
	}
	if journal.Phase != metadata.ReplacementPublicationCleaned {
		if err := s.finishReplacementCleanup(ctx, row, *journal); err != nil {
			return err
		}
	}
	// This fast path uses the exact charge established in this store epoch,
	// never the mere existence of historical cleaned evidence.
	return s.refundReplacementCleanup(row)
}

// finishReplacementCleanup is also called at boot before capacity is rebuilt.
// It never refunds capacity. All owned FDs close before the caller can refund.
func (s *SpoolStore) finishReplacementCleanup(ctx context.Context, row *metadata.SpoolRow, journal metadata.SpoolReplacementPublication) (retErr error) {
	if journal.Phase != metadata.ReplacementPublicationSettled && journal.Phase != metadata.ReplacementPublicationCleaning || s.meta.Owner() == nil {
		return metadata.ErrMirrorConflict
	}
	w, err := s.meta.ReplacementWitness(row)
	if err != nil || w == nil {
		return errors.Join(err, metadata.ErrMirrorConflict)
	}
	p, err := s.openReplacementDirectories(row.SpoolFile)
	if err != nil {
		return err
	}
	defer func() {
		closeErr := p.closeChecked()
		// Fault injection runs after the real close, never instead of it.
		if s.replacementCleanupFault != nil {
			closeErr = errors.Join(closeErr, s.replacementCleanupFault("closed"))
		}
		if closeErr != nil {
			s.replacementCleanupCharges.Store(row.ID, replacementCleanupCharge{size: row.Size, closeFailed: true})
			retErr = errors.Join(retErr, closeErr)
		}
	}()
	if journal.Phase == metadata.ReplacementPublicationCleaning {
		record, err := journal.SettlementRecord()
		if err != nil || record.RootIdentity != p.rootID || record.FilesIdentity != p.filesID {
			return metadata.ErrMirrorConflict
		}
	}
	fd, err := unix.Openat(int(p.files.Fd()), p.name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		if !errors.Is(err, unix.ENOENT) || journal.Phase != metadata.ReplacementPublicationCleaning {
			return err
		}
	} else {
		p.file, p.ownedFile = os.NewFile(uintptr(fd), row.SpoolFile), true
		p.identity, _, _, err = fdPhaseIdentity(p.file)
		if err != nil || p.identity != w.SourceIdentity {
			return metadata.ErrMirrorConflict
		}
		image, err := p.HashImage()
		if err != nil || image.Size != w.Image.Size || image.MtimeNS != w.Image.MtimeNS || !bytes.Equal(image.SHA256, w.Image.SHA256) {
			return errors.Join(err, metadata.ErrMirrorConflict)
		}
	}
	if journal.Phase == metadata.ReplacementPublicationSettled {
		if err := s.replacementCleanupStep(ctx, "before-intent"); err != nil {
			return err
		}
		next, err := s.meta.Owner().BeginReplacementCleanup(journal, p.rootID, p.filesID, func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return p.VerifyImage(w.Image)
		})
		if err != nil {
			return err
		}
		journal = *next
		if err := s.replacementCleanupStep(ctx, "after-intent"); err != nil {
			return err
		}
	}
	if err := s.replacementCleanupStep(ctx, "before-unlink"); err != nil {
		return err
	}
	if p.file != nil {
		if err := p.VerifyImage(w.Image); err != nil {
			return err
		}
		// This private directory is exclusively managed. Retain the parent and
		// recheck the leaf immediately before unlink; never remove via a full path.
		if err := unix.Unlinkat(int(p.files.Fd()), p.name, 0); err != nil {
			return err
		}
	}
	if err := s.replacementCleanupStep(ctx, "after-unlink"); err != nil {
		return err
	}
	absent := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := p.verifyDirectories(); err != nil {
			return err
		}
		_, _, err := birthEntryIdentity(p.files, p.name)
		if !errors.Is(err, unix.ENOENT) {
			return metadata.ErrMirrorConflict
		}
		return nil
	}
	if err := absent(); err != nil {
		return err
	}
	if err := s.replacementCleanupStep(ctx, "before-sync"); err != nil {
		return err
	}
	if err := p.files.Sync(); err != nil {
		return err
	}
	if err := p.root.Sync(); err != nil {
		return err
	}
	if err := s.replacementCleanupStep(ctx, "after-sync"); err != nil {
		return err
	}
	if err := s.replacementCleanupStep(ctx, "before-finish"); err != nil {
		return err
	}
	if _, err := s.meta.Owner().FinishReplacementCleanup(journal, absent); err != nil {
		return err
	}
	return s.replacementCleanupStep(ctx, "after-finish")
}
