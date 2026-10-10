package nfs

import (
	"context"
	"fmt"

	"github.com/lelanddutcher/juicemount/metadata"
)

// resumeReplacement completes one accepted save from its durable phase. The
// production queue remains sealed until the full lifecycle matrix qualifies.
// Publication callbacks refer to the selected backend; committed and later
// phases never invoke them. Any failure returns without an internal retry.
func (h *JuiceMountHandler) resumeReplacement(ctx context.Context, id int64, factory func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error), verifyBinding func(metadata.CachePathBinding) error) error {
	return h.resumeReplacementAttached(ctx, nil, id, factory, verifyBinding)
}

// A scheduled callback is tied to its original drainer, even across handler
// reattachment. Manual acceptance helpers may omit the expected generation.
func (h *JuiceMountHandler) resumeReplacementAttached(ctx context.Context, expected *Drainer, id int64, factory func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error), verifyBinding func(metadata.CachePathBinding) error) error {
	d, s, store := h.drainer.Load(), h.spool.Load(), h.store
	if d == nil || (expected != nil && d != expected) || s == nil || d.spool != s || s.meta.Owner() != store || !d.beginReplacement(id) {
		return metadata.ErrMirrorConflict
	}
	defer d.endReplacement(id)
	attached := func() bool {
		return h.drainer.Load() == d && h.spool.Load() == s && h.store == store && s.meta.Owner() == store
	}
	read := func() (*metadata.SpoolReplacementPublication, error) {
		if !attached() {
			return nil, metadata.ErrMirrorConflict
		}
		row, err := s.meta.Get(id)
		if err != nil {
			return nil, err
		}
		p, err := s.meta.ReplacementPublication(row)
		if err != nil {
			return nil, err
		}
		if p == nil {
			return nil, metadata.ErrMirrorConflict
		}
		return p, nil
	}
	// Publication, settlement and cleanup are the only three actions. An
	// unchanged/unexpected result is an error, never a reason to spin or retry.
	for action := 0; action < 3; action++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		before, err := read()
		if err != nil {
			return err
		}
		// Linearize phase admission with Stop; release before I/O or metadata locks.
		// An admitted phase may finish, but Stop prevents admission of the next one.
		d.lifecycleMu.Lock()
		admitted := !d.stopping && attached()
		d.lifecycleMu.Unlock()
		if !admitted {
			return metadata.ErrMirrorConflict
		}
		var expected string
		switch before.Phase {
		case metadata.ReplacementPublicationQueued, metadata.ReplacementPublicationIntent, metadata.ReplacementPublicationArming, metadata.ReplacementPublicationArmed:
			expected = metadata.ReplacementPublicationCommitted
			if factory == nil || verifyBinding == nil {
				return metadata.ErrMirrorConflict
			}
			err = d.publishReplacementOwned(ctx, id, factory, func(binding metadata.CachePathBinding) error {
				if !attached() {
					return metadata.ErrMirrorConflict
				}
				return verifyBinding(binding)
			})
		case metadata.ReplacementPublicationCommitted:
			expected = metadata.ReplacementPublicationSettled
			err = h.settleReplacementOwned(ctx, id, d)
		case metadata.ReplacementPublicationSettled, metadata.ReplacementPublicationCleaning, metadata.ReplacementPublicationCleaned:
			expected = metadata.ReplacementPublicationCleaned
			err = h.cleanupReplacementOwned(ctx, id, d)
		default:
			return metadata.ErrMirrorConflict
		}
		if err != nil {
			return fmt.Errorf("replacement resume from %s: %w", before.Phase, err)
		}
		after, err := read()
		if err != nil {
			return err
		}
		if after.Phase != expected || after.OperationID != before.OperationID || after.SourceIdentity != before.SourceIdentity || after.WitnessSHA256 != before.WitnessSHA256 ||
			(before.Phase != metadata.ReplacementPublicationCleaned && after.Revision <= before.Revision) {
			return metadata.ErrMirrorConflict
		}
		if s.replacementResumePhase != nil {
			if err := s.replacementResumePhase(after.Phase); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if after.Phase == metadata.ReplacementPublicationCleaned {
			return nil
		}
	}
	return metadata.ErrMirrorConflict
}
