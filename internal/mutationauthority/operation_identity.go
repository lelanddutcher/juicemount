package mutationauthority

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"
)

// OperationKind identifies the durable effect. Legacy records retain their
// original opaque meaning until a trusted startup classifier accepts them.
type OperationKind string

const (
	LegacyOperation       OperationKind = ""
	CustodyOperation      OperationKind = "custody-v1"
	MediaPublishOperation OperationKind = "media-publish-v1"
)

func validOperationIdentity(r Request) bool {
	switch r.Kind {
	case LegacyOperation, CustodyOperation:
		return r.FileID == ""
	case MediaPublishOperation:
		if r.FileID == "" || len(r.FileID) > 128 || r.ExpectedRevision == "" || len(r.ExpectedRevision) > 4096 || !utf8.ValidString(r.ExpectedRevision) || strings.ContainsRune(r.ExpectedRevision, 0) {
			return false
		}
		for _, c := range r.FileID {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func sameRequest(a, b Request) bool {
	return a.Volume == b.Volume && a.ID == b.ID && a.Session == b.Session && a.Epoch == b.Epoch &&
		a.Digest == b.Digest && a.ExpectedRevision == b.ExpectedRevision && a.Kind == b.Kind && a.FileID == b.FileID
}

// ClassifyLegacyCustody upgrades recognized custody requests only at startup,
// before claims, SDK startup, or callbacks. Every row and terminal state must
// validate before any classification commits. No operation is replayed or
// converted to a media publication receipt. accept must be pure and must not
// call the journal. Missing/corrupt artifacts remain uncertain for reconciliation.
func (j *Journal) ClassifyLegacyCustody(ctx context.Context, accept func(Request) bool) error {
	return j.ClassifyStartup(ctx, accept, nil)
}

// ClassifyStartup admits only explicitly preflighted media identities alongside
// custody. The media predicate grants no claim, receipt or effect; the SDK must
// independently fence and certify all retained stages before admission.
func (j *Journal) ClassifyStartup(ctx context.Context, accept, acceptMedia func(Request) bool) error {
	if accept == nil {
		return ErrInvalid
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return ErrClosed
	}
	if len(j.active) != 0 || len(j.deadlines) != 0 || j.backendStarted {
		return ErrInFlight
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var admitted bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM owners WHERE admission_closed=0)`).Scan(&admitted); err != nil {
		return err
	}
	if admitted {
		return ErrInFlight
	}
	rows, err := tx.QueryContext(ctx, `SELECT volume,id,session,epoch,digest,expected_revision,kind,file_id FROM operations`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var r Request
		if err = rows.Scan(&r.Volume, &r.ID, &r.Session, &r.Epoch, &r.Digest, &r.ExpectedRevision, &r.Kind, &r.FileID); err != nil {
			rows.Close()
			return err
		}
		custody := (r.Kind == LegacyOperation || r.Kind == CustodyOperation) && accept(r)
		media := r.Kind == MediaPublishOperation && acceptMedia != nil && acceptMedia(r)
		if !validRequest(r) || (!custody && !media) {
			rows.Close()
			return fmt.Errorf("%w: unsupported custody request", ErrInvalid)
		}
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if _, err = tx.ExecContext(ctx, `UPDATE operations SET kind=? WHERE kind=''`, CustodyOperation); err != nil {
		return err
	}
	return tx.Commit()
}
