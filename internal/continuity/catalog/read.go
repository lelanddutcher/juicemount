package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

func readAsset(ctx context.Context, q queryer, library continuity.LibraryID, id continuity.AssetID) (Asset, error) {
	return scanAsset(q.QueryRowContext(ctx, `SELECT library_id,asset_id,label,COALESCE(head,''),created_sequence,updated_sequence FROM assets WHERE asset_id=?`, id), library)
}

func scanAsset(row interface{ Scan(...any) error }, library continuity.LibraryID) (Asset, error) {
	var a Asset
	err := row.Scan(&a.LibraryID, &a.ID, &a.Label, &a.Head, &a.CreatedSequence, &a.UpdatedSequence)
	if errors.Is(err, sql.ErrNoRows) {
		return Asset{}, ErrNotFound
	}
	if err != nil {
		return Asset{}, formatError(err)
	}
	if a.LibraryID != library || a.ID.Validate() != nil || !validText(a.Label, 4096, false) || a.CreatedSequence <= 0 || a.UpdatedSequence < a.CreatedSequence || (a.Head != "" && a.Head.Validate() != nil) {
		return Asset{}, corrupt("asset fields")
	}
	return a, nil
}

func readRevision(ctx context.Context, q queryer, library continuity.LibraryID, id continuity.RevisionID) (Revision, error) {
	var r Revision
	var owner continuity.LibraryID
	var asset continuity.AssetID
	var manifestJSON []byte
	err := q.QueryRowContext(ctx, `SELECT revision_id,library_id,asset_id,CASE WHEN length(canonical) BETWEEN 95 AND 2143 THEN canonical END,CASE WHEN length(manifest_json) BETWEEN 1 AND 8192 THEN manifest_json END,created_sequence FROM revisions WHERE revision_id=?`, id).
		Scan(&r.ID, &owner, &asset, &r.Canonical, &manifestJSON, &r.CreatedSequence)
	if errors.Is(err, sql.ErrNoRows) {
		return Revision{}, ErrNotFound
	}
	if err != nil {
		return Revision{}, formatError(err)
	}
	if owner != library || r.ID != id || r.ID.Validate() != nil || r.CreatedSequence <= 0 || len(r.Canonical) < 95 || len(r.Canonical) > 2143 {
		return Revision{}, corrupt("revision fields")
	}
	r.Manifest, err = decodeManifest(manifestJSON)
	if err != nil {
		return Revision{}, err
	}
	canonical, err := r.Manifest.Canonical()
	if err != nil {
		return Revision{}, formatError(err)
	}
	computed, err := r.Manifest.RevisionID()
	if err != nil {
		return Revision{}, formatError(err)
	}
	if computed != r.ID || !bytes.Equal(canonical, r.Canonical) || r.Manifest.LibraryID != owner || r.Manifest.AssetID != asset {
		return Revision{}, corrupt("revision canonical identity mismatch")
	}
	a, err := readAsset(ctx, q, library, asset)
	if err != nil {
		return Revision{}, fmt.Errorf("%w: revision asset: %v", ErrFormat, err)
	}
	if a.CreatedSequence > r.CreatedSequence {
		return Revision{}, corrupt("revision precedes its asset")
	}
	rows, err := q.QueryContext(ctx, `SELECT p.parent_id,p.asset_id,r.asset_id,r.library_id,r.created_sequence FROM revision_parents p JOIN revisions r ON r.revision_id=p.parent_id WHERE p.revision_id=? ORDER BY p.parent_id LIMIT 65`, id)
	if err != nil {
		return Revision{}, formatError(err)
	}
	index := 0
	for rows.Next() {
		var parent continuity.RevisionID
		var edgeAsset, parentAsset continuity.AssetID
		var parentLibrary continuity.LibraryID
		var parentSequence int64
		if err := rows.Scan(&parent, &edgeAsset, &parentAsset, &parentLibrary, &parentSequence); err != nil {
			rows.Close()
			return Revision{}, formatError(err)
		}
		if index >= len(r.Manifest.Parents) || parent != r.Manifest.Parents[index] || edgeAsset != asset || parentAsset != asset || parentLibrary != library || parentSequence <= 0 || parentSequence > r.CreatedSequence {
			rows.Close()
			return Revision{}, corrupt("revision parent membership mismatch")
		}
		index++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return Revision{}, formatError(err)
	}
	if index != len(r.Manifest.Parents) {
		return Revision{}, corrupt("revision parent set mismatch")
	}
	r.Canonical = append([]byte(nil), canonical...)
	return r, nil
}

type eventRow struct {
	sequence  int64
	library   continuity.LibraryID
	id        string
	request   []byte
	digest    string
	timestamp int64
	receipt   []byte
}

const eventColumns = `sequence,library_id,operation_id,request,payload_sha256,recorded_unix_nano,receipt`

func scanEvent(row interface{ Scan(...any) error }) (eventRow, error) {
	var e eventRow
	err := row.Scan(&e.sequence, &e.library, &e.id, &e.request, &e.digest, &e.timestamp, &e.receipt)
	return e, err
}

func (e eventRow) decode(library continuity.LibraryID) (Event, error) {
	if e.sequence <= 0 || e.library != library || !validOperationID(e.id) || len(e.receipt) == 0 || len(e.receipt) > 1024 {
		return Event{}, corrupt("event fields")
	}
	op, err := decodeOperation(e.request)
	if err != nil {
		return Event{}, err
	}
	if op.LibraryID != library || op.ID != e.id || payloadDigest(e.request) != e.digest {
		return Event{}, corrupt("event payload identity")
	}
	var receipt Receipt
	if err := strictJSON(e.receipt, &receipt); err != nil {
		return Event{}, formatError(err)
	}
	want := Receipt{OperationVersion, library, e.id, e.sequence, e.digest, e.timestamp}
	canonical, err := json.Marshal(receipt)
	if err != nil {
		return Event{}, formatError(err)
	}
	if receipt != want || !bytes.Equal(e.receipt, canonical) {
		return Event{}, corrupt("event receipt mismatch")
	}
	return Event{Receipt: receipt, Operation: op}, nil
}

func readEvent(ctx context.Context, q queryer, library continuity.LibraryID, id string) (Event, []byte, error) {
	raw, err := scanEvent(q.QueryRowContext(ctx, `SELECT `+eventColumns+` FROM events WHERE operation_id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Event{}, nil, ErrNotFound
	}
	if err != nil {
		return Event{}, nil, formatError(err)
	}
	event, err := raw.decode(library)
	return event, raw.request, err
}

func (s *Store) GetAsset(ctx context.Context, library continuity.LibraryID, id continuity.AssetID) (Asset, error) {
	if err := s.enter(); err != nil {
		return Asset{}, err
	}
	defer s.leave()
	if err := s.requireLibrary(library); err != nil {
		return Asset{}, err
	}
	if id.Validate() != nil {
		return Asset{}, invalid("asset identity")
	}
	return readAsset(ctx, s.db, library, id)
}

func (s *Store) GetRevision(ctx context.Context, library continuity.LibraryID, id continuity.RevisionID) (Revision, error) {
	if err := s.enter(); err != nil {
		return Revision{}, err
	}
	defer s.leave()
	if err := s.requireLibrary(library); err != nil {
		return Revision{}, err
	}
	if id.Validate() != nil {
		return Revision{}, invalid("revision identity")
	}
	return readRevision(ctx, s.db, library, id)
}

func (s *Store) GetEvent(ctx context.Context, library continuity.LibraryID, operationID string) (Event, error) {
	if err := s.enter(); err != nil {
		return Event{}, err
	}
	defer s.leave()
	if err := s.requireLibrary(library); err != nil {
		return Event{}, err
	}
	if !validOperationID(operationID) {
		return Event{}, invalid("operation identity")
	}
	event, _, err := readEvent(ctx, s.db, library, operationID)
	return event, err
}

func validPage(limit int) bool { return limit >= 1 && limit <= MaxPageSize }

// ListAssets uses an exclusive asset-ID cursor; empty starts at the beginning.
func (s *Store) ListAssets(ctx context.Context, library continuity.LibraryID, after continuity.AssetID, limit int) ([]Asset, error) {
	if err := s.enter(); err != nil {
		return nil, err
	}
	defer s.leave()
	if err := s.requireLibrary(library); err != nil {
		return nil, err
	}
	if !validPage(limit) || (after != "" && after.Validate() != nil) {
		return nil, invalid("asset page")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT library_id,asset_id,label,COALESCE(head,''),created_sequence,updated_sequence FROM assets WHERE asset_id>? ORDER BY asset_id LIMIT ?`, after, limit)
	if err != nil {
		return nil, formatError(err)
	}
	defer rows.Close()
	assets := make([]Asset, 0, limit)
	for rows.Next() {
		a, err := scanAsset(rows, library)
		if err != nil {
			return nil, err
		}
		assets = append(assets, a)
	}
	if err := rows.Err(); err != nil {
		return nil, formatError(err)
	}
	return assets, nil
}

// ListRevisions uses an exclusive revision-ID cursor within one asset.
func (s *Store) ListRevisions(ctx context.Context, library continuity.LibraryID, asset continuity.AssetID, after continuity.RevisionID, limit int) ([]Revision, error) {
	if err := s.enter(); err != nil {
		return nil, err
	}
	defer s.leave()
	if err := s.requireLibrary(library); err != nil {
		return nil, err
	}
	if asset.Validate() != nil || !validPage(limit) || (after != "" && after.Validate() != nil) {
		return nil, invalid("revision page")
	}
	if _, err := readAsset(ctx, s.db, library, asset); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT revision_id FROM revisions WHERE asset_id=? AND revision_id>? ORDER BY revision_id LIMIT ?`, asset, after, limit)
	if err != nil {
		return nil, formatError(err)
	}
	ids := make([]continuity.RevisionID, 0, limit)
	for rows.Next() {
		var id continuity.RevisionID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, formatError(err)
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, formatError(err)
	}
	revisions := make([]Revision, 0, len(ids))
	for _, id := range ids {
		r, err := readRevision(ctx, s.db, library, id)
		if err != nil {
			return nil, err
		}
		revisions = append(revisions, r)
	}
	return revisions, nil
}

// Events uses an exclusive sequence cursor; zero starts at the beginning.
func (s *Store) Events(ctx context.Context, library continuity.LibraryID, after int64, limit int) ([]Event, error) {
	if err := s.enter(); err != nil {
		return nil, err
	}
	defer s.leave()
	if err := s.requireLibrary(library); err != nil {
		return nil, err
	}
	if after < 0 || !validPage(limit) {
		return nil, invalid("event page")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+eventColumns+` FROM events WHERE sequence>? ORDER BY sequence LIMIT ?`, after, limit)
	if err != nil {
		return nil, formatError(err)
	}
	defer rows.Close()
	events := make([]Event, 0, limit)
	for rows.Next() {
		raw, err := scanEvent(rows)
		if err != nil {
			return nil, formatError(err)
		}
		event, err := raw.decode(library)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, formatError(err)
	}
	return events, nil
}
