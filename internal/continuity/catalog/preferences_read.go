package catalog

import (
	"context"
	"database/sql"
	"errors"
	"io"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

func (d *DiscoveryService) preferenceSnapshotBinding(ctx context.Context, q queryer, subject SubjectID) (*preferenceBinding, error) {
	control, err := readPreferenceControl(ctx, q, d.library)
	if err != nil {
		return nil, err
	}
	now := d.store.now().Unix()
	if err := preferenceClock(now, control.floor); err != nil {
		return nil, err
	}
	s, err := readPreferenceSubject(ctx, q, d.library, subject)
	if err != nil {
		return nil, err
	}
	return &preferenceBinding{generation: s.generation, lower: now, floor: control.floor}, nil
}

func (d *DiscoveryService) readPreferenceView(ctx context.Context, q queryer, subject SubjectID) (map[continuity.AssetID]preferenceEntry, *preferenceBinding, error) {
	b, err := d.preferenceSnapshotBinding(ctx, q, subject)
	if err != nil {
		return nil, nil, err
	}
	view := make(map[continuity.AssetID]preferenceEntry)
	count := 0
	err = walkRows(ctx, q, `SELECT `+preferenceColumns+` FROM preferences INDEXED BY preferences_recent WHERE subject_id=? ORDER BY person_order DESC,asset_id LIMIT 257`, func(rows *sql.Rows) error {
		count++
		if count > 256 {
			return errDiscoveryWork
		}
		e, err := scanPreference(rows)
		if err != nil {
			return err
		}
		if e.order > b.generation {
			return corrupt("preference order exceeds generation")
		}
		if e.expires > b.lower {
			view[e.asset] = e
		}
		return nil
	}, subject)
	return view, b, err
}

func (d *DiscoveryService) PreferenceState(parent context.Context, bearer []byte, r PreferenceStateRequest, output io.Writer) (result DisclosureResult, resultErr error) {
	if d == nil || output == nil {
		return result, invalid("preference service or writer")
	}
	ctx, leave, err := d.enterPreferences(parent)
	if err != nil {
		return result, err
	}
	defer leave()
	if r.Version != 2 || r.LibraryID != d.library || r.AssetID.Validate() != nil {
		return result, invalid("preference state request")
	}
	tx, done, err := d.store.readSnapshot(ctx)
	if err != nil {
		return result, err
	}
	b, c, err := d.authorizePreference(ctx, tx, bearer, r.AssetID)
	if err != nil {
		return result, errors.Join(err, d.finishSnapshot(done))
	}
	result.SubjectID, result.Selector = c.subject, c.selector
	b.preference, err = d.preferenceSnapshotBinding(ctx, tx, c.subject)
	if err != nil {
		return result, errors.Join(err, d.finishSnapshot(done))
	}
	e, err := readPreference(ctx, tx, c.subject, r.AssetID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return result, errors.Join(err, d.finishSnapshot(done))
	}
	var token *string
	if err == nil {
		if e.order > b.preference.generation {
			return result, errors.Join(corrupt("preference status generation"), d.finishSnapshot(done))
		}
		if e.expires > b.preference.lower {
			token = &e.token
			b.preference.expiry = e.expires
		}
	}
	body, err := encodeDiscoveryValue(preferenceState{2, r.AssetID, token}, 1024)
	err = errors.Join(err, d.finishSnapshot(done))
	if err != nil {
		return result, err
	}
	if d.beforeFence != nil {
		if err := d.beforeFence(ctx); err != nil {
			return result, err
		}
	}
	if err := d.fence(ctx, b); err != nil {
		return result, err
	}
	now := d.store.now().Unix()
	if now < b.issued || now >= b.expires {
		return result, ErrUnauthenticated
	}
	if err := b.preference.checkTime(now); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	result.Started = true
	n, err := output.Write(body)
	if err != nil {
		return result, err
	}
	if n != len(body) {
		return result, io.ErrShortWrite
	}
	return result, nil
}
