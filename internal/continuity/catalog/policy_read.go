package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const subjectColumns = `library_id,subject_id,kind,CASE WHEN length(CAST(label AS BLOB))<=256 THEN label END,enabled,authorization_generation,created_sequence,updated_sequence`
const grantColumns = `library_id,grant_id,subject_id,scope_kind,COALESCE(asset_id,''),CASE WHEN length(actions)<=256 THEN actions END,created_sequence,updated_sequence,removed`

func scanSubject(row interface{ Scan(...any) error }, library continuity.LibraryID) (Subject, error) {
	var s Subject
	err := row.Scan(&s.LibraryID, &s.ID, &s.Value.Kind, &s.Value.Label, &s.Value.Enabled, &s.AuthorizationGeneration, &s.CreatedSequence, &s.UpdatedSequence)
	if errors.Is(err, sql.ErrNoRows) {
		return s, ErrNotFound
	}
	if err != nil {
		return s, formatError(err)
	}
	if s.LibraryID != library || s.ID.Validate() != nil || !validSubject(s.Value) || s.AuthorizationGeneration <= 0 || s.CreatedSequence <= 0 || s.UpdatedSequence < s.CreatedSequence {
		return Subject{}, corrupt("subject fields")
	}
	return s, nil
}
func readSubject(ctx context.Context, q queryer, library continuity.LibraryID, id SubjectID) (Subject, error) {
	return scanSubject(q.QueryRowContext(ctx, `SELECT `+subjectColumns+` FROM policy_subjects WHERE subject_id=?`, id), library)
}
func scanGrant(row interface{ Scan(...any) error }, library continuity.LibraryID) (Grant, error) {
	var g Grant
	var actions []byte
	err := row.Scan(&g.LibraryID, &g.ID, &g.Value.SubjectID, &g.Value.Scope.Kind, &g.Value.Scope.AssetID, &actions, &g.CreatedSequence, &g.UpdatedSequence, &g.Removed)
	if errors.Is(err, sql.ErrNoRows) {
		return g, ErrNotFound
	}
	if err != nil {
		return g, formatError(err)
	}
	if decodeCanonicalValue(actions, &g.Value.Actions) != nil || g.LibraryID != library || !validOperationID(g.ID) || !validGrant(g.Value) || g.CreatedSequence <= 0 || g.UpdatedSequence < g.CreatedSequence {
		return Grant{}, corrupt("grant fields")
	}
	return g, nil
}
func readGrant(ctx context.Context, q queryer, library continuity.LibraryID, id string) (Grant, error) {
	return scanGrant(q.QueryRowContext(ctx, `SELECT `+grantColumns+` FROM policy_grants WHERE grant_id=?`, id), library)
}

func readAuthority(ctx context.Context, q queryer, library continuity.LibraryID) (AuthorityStatus, error) {
	var a AuthorityStatus
	var lib continuity.LibraryID
	err := q.QueryRowContext(ctx, `SELECT library_id,incarnation,owner_subject_id FROM catalog_authority WHERE singleton=1`).Scan(&lib, &a.Incarnation, &a.OwnerSubjectID)
	if errors.Is(err, sql.ErrNoRows) {
		return a, nil
	}
	if err != nil {
		return a, err
	}
	if lib != library || !validRandomID(a.Incarnation, "auth_") || a.OwnerSubjectID.Validate() != nil {
		return a, corrupt("authority binding")
	}
	s, err := readSubject(ctx, q, library, a.OwnerSubjectID)
	if err != nil {
		return a, err
	}
	if s.Value.Kind != "person" || !s.Value.Enabled {
		return a, corrupt("authority owner")
	}
	return a, nil
}

func (o *LocalOwnerService) Status(ctx context.Context) (result AuthorityStatus, resultErr error) {
	if err := o.enter(); err != nil {
		return AuthorityStatus{}, err
	}
	defer o.store.leave()
	tx, done, err := o.store.readSnapshot(ctx)
	if err != nil {
		return AuthorityStatus{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, done()) }()
	return readAuthority(ctx, tx, o.library)
}
func (o *LocalOwnerService) ListSubjects(ctx context.Context, after SubjectID, limit int) ([]Subject, error) {
	if err := o.enter(); err != nil {
		return nil, err
	}
	defer o.store.leave()
	if limit < 1 || limit > MaxPageSize || after != "" && after.Validate() != nil {
		return nil, invalid("subject page")
	}
	result := make([]Subject, 0)
	err := walkRows(ctx, o.store.db, `SELECT `+subjectColumns+` FROM policy_subjects WHERE subject_id>? COLLATE BINARY ORDER BY subject_id COLLATE BINARY LIMIT ?`, func(rows *sql.Rows) error {
		s, err := scanSubject(rows, o.library)
		if err == nil {
			result = append(result, s)
		}
		return err
	}, after, limit)
	return result, err
}
func (o *LocalOwnerService) ListGrants(ctx context.Context, after string, limit int) ([]Grant, error) {
	if err := o.enter(); err != nil {
		return nil, err
	}
	defer o.store.leave()
	if limit < 1 || limit > MaxPageSize || after != "" && !validOperationID(after) {
		return nil, invalid("grant page")
	}
	result := make([]Grant, 0)
	err := walkRows(ctx, o.store.db, `SELECT `+grantColumns+` FROM policy_grants WHERE grant_id>? COLLATE BINARY ORDER BY grant_id COLLATE BINARY LIMIT ?`, func(rows *sql.Rows) error {
		g, err := scanGrant(rows, o.library)
		if err == nil {
			result = append(result, g)
		}
		return err
	}, after, limit)
	return result, err
}

func (e eventRow) decodePolicy(library continuity.LibraryID) (PolicyOperation, Receipt, error) {
	op, err := DecodePolicyRequest(e.request)
	if err != nil {
		return op, Receipt{}, corrupt("policy event request")
	}
	var receipt Receipt
	want := Receipt{1, library, e.id, e.sequence, e.digest, e.timestamp}
	if e.library != library || op.LibraryID != library || op.ID != e.id || e.sequence <= 0 || domainDigest("juicemount.catalog.policy.operation\x00\x01", e.request) != e.digest || decodeCanonicalValue(e.receipt, &receipt) != nil || receipt != want {
		return PolicyOperation{}, Receipt{}, corrupt("policy event identity")
	}
	return op, receipt, nil
}

func policyTotal(ctx context.Context, q queryer) (int64, error) {
	var count, last int64
	err := q.QueryRowContext(ctx, `SELECT count(*),COALESCE(max(sequence),0) FROM policy_events`).Scan(&count, &last)
	if err == nil && count != last {
		err = corrupt("policy sequence count")
	}
	return count, err
}
func walkPolicyEvents(ctx context.Context, q queryer, library continuity.LibraryID, count int64, fn func(eventRow) error) error {
	var sequence int64
	err := walkRows(ctx, q, `SELECT `+boundedEventColumns+` FROM policy_events ORDER BY sequence`, func(rows *sql.Rows) error {
		e, err := scanEvent(rows)
		if err != nil {
			return err
		}
		if e.sequence != sequence+1 || sequence >= count {
			return corrupt("policy sequence discontinuity")
		}
		if _, _, err := e.decodePolicy(library); err != nil {
			return err
		}
		if fn != nil {
			if err := fn(e); err != nil {
				return err
			}
		}
		sequence++
		return nil
	})
	if err == nil && sequence != count {
		return corrupt("missing policy events")
	}
	return err
}

func actionsBytes(actions []string) []byte { b, _ := json.Marshal(actions); return b }
