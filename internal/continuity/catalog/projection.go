package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

type libraryProjection struct {
	LibraryID continuity.LibraryID `json:"library_id"`
}
type assetProjection struct {
	LibraryID       continuity.LibraryID  `json:"library_id"`
	AssetID         continuity.AssetID    `json:"asset_id"`
	Label           string                `json:"label"`
	Head            continuity.RevisionID `json:"head"`
	CreatedSequence int64                 `json:"created_sequence"`
	UpdatedSequence int64                 `json:"updated_sequence"`
}
type revisionProjection struct {
	LibraryID       continuity.LibraryID  `json:"library_id"`
	RevisionID      continuity.RevisionID `json:"revision_id"`
	AssetID         continuity.AssetID    `json:"asset_id"`
	CanonicalHex    string                `json:"canonical_hex"`
	Manifest        manifestJSON          `json:"manifest"`
	CreatedSequence int64                 `json:"created_sequence"`
}
type parentProjection struct {
	LibraryID  continuity.LibraryID  `json:"library_id"`
	RevisionID continuity.RevisionID `json:"revision_id"`
	ParentID   continuity.RevisionID `json:"parent_id"`
	AssetID    continuity.AssetID    `json:"asset_id"`
}
type relationshipProjection struct {
	LibraryID       continuity.LibraryID `json:"library_id"`
	ID              string               `json:"id"`
	Value           RelationshipValue    `json:"value"`
	CreatedSequence int64                `json:"created_sequence"`
	UpdatedSequence int64                `json:"updated_sequence"`
	Removed         bool                 `json:"removed"`
}
type locationProjection struct {
	LibraryID       continuity.LibraryID `json:"library_id"`
	ID              string               `json:"id"`
	Value           LocationValue        `json:"value"`
	CreatedSequence int64                `json:"created_sequence"`
	UpdatedSequence int64                `json:"updated_sequence"`
	Removed         bool                 `json:"removed"`
}

func walkRows(ctx context.Context, q queryer, query string, fn func(*sql.Rows) error, args ...any) (resultErr error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

func projectionTotal(ctx context.Context, q queryer) (int64, error) {
	var count int64
	err := q.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM catalog)+(SELECT count(*) FROM assets)+(SELECT count(*) FROM revisions)+(SELECT count(*) FROM revision_parents)+(SELECT count(*) FROM relationships)+(SELECT count(*) FROM locations)`).Scan(&count)
	return count, err
}

// projectionDigest scans all logical state, including tombstones, without
// accumulating records. The caller owns one consistent transaction snapshot.
func projectionDigest(ctx context.Context, q queryer, library continuity.LibraryID, maxRows int64) (string, int64, error) {
	h := sha256.New()
	_, _ = h.Write([]byte("juicemount.catalog.projection\x00\x01"))
	var count int64
	emit := func(kind byte, value any) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if count >= maxRows {
			return ErrLimit
		}
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if len(data) > 64*1024 {
			return corrupt("projection frame length")
		}
		header := frameHeader(kind, len(data))
		_, _ = h.Write(header[:])
		_, _ = h.Write(data)
		count++
		return nil
	}
	if err := emit(0x11, libraryProjection{library}); err != nil {
		return "", 0, err
	}
	err := walkRows(ctx, q, `SELECT library_id,asset_id,CASE WHEN length(CAST(label AS BLOB))<=4096 THEN label END,COALESCE(head,''),created_sequence,updated_sequence FROM assets ORDER BY asset_id COLLATE BINARY`, func(rows *sql.Rows) error {
		a, err := scanAsset(rows, library)
		if err != nil {
			return err
		}
		return emit(0x12, assetProjection{a.LibraryID, a.ID, a.Label, a.Head, a.CreatedSequence, a.UpdatedSequence})
	})
	if err != nil {
		return "", 0, err
	}
	// Close each bounded ID page before readRevision performs nested queries.
	var after continuity.RevisionID
	for {
		ids := make([]continuity.RevisionID, 0, 128)
		err := walkRows(ctx, q, `SELECT CASE WHEN length(revision_id)=69 THEN revision_id END FROM revisions WHERE revision_id>? COLLATE BINARY ORDER BY revision_id COLLATE BINARY LIMIT 128`, func(rows *sql.Rows) error {
			var id continuity.RevisionID
			if err := rows.Scan(&id); err != nil {
				return formatError(err)
			}
			if id.Validate() != nil {
				return corrupt("projection revision identity")
			}
			ids = append(ids, id)
			return nil
		}, after)
		if err != nil {
			return "", 0, err
		}
		for _, id := range ids {
			r, err := readRevision(ctx, q, library, id)
			if err != nil {
				return "", 0, err
			}
			if err := emit(0x13, revisionProjection{library, r.ID, r.Manifest.AssetID, hex.EncodeToString(r.Canonical), manifestWire(r.Manifest), r.CreatedSequence}); err != nil {
				return "", 0, err
			}
		}
		if len(ids) < 128 {
			break
		}
		after = ids[len(ids)-1]
	}
	err = walkRows(ctx, q, `SELECT e.revision_id,e.parent_id,e.asset_id,c.library_id,p.library_id,c.asset_id,p.asset_id,c.created_sequence,p.created_sequence FROM revision_parents e LEFT JOIN revisions c ON c.revision_id=e.revision_id LEFT JOIN revisions p ON p.revision_id=e.parent_id ORDER BY e.revision_id COLLATE BINARY,e.parent_id COLLATE BINARY`, func(rows *sql.Rows) error {
		var edge parentProjection
		var childLibrary, parentLibrary continuity.LibraryID
		var childAsset, parentAsset continuity.AssetID
		var childSequence, parentSequence int64
		if err := rows.Scan(&edge.RevisionID, &edge.ParentID, &edge.AssetID, &childLibrary, &parentLibrary, &childAsset, &parentAsset, &childSequence, &parentSequence); err != nil {
			return formatError(err)
		}
		if edge.RevisionID.Validate() != nil || edge.ParentID.Validate() != nil || edge.RevisionID == edge.ParentID || edge.AssetID.Validate() != nil || childLibrary != library || parentLibrary != library || childAsset != edge.AssetID || parentAsset != edge.AssetID || parentSequence <= 0 || childSequence < parentSequence {
			return corrupt("projection parent membership")
		}
		edge.LibraryID = library
		return emit(0x14, edge)
	})
	if err != nil {
		return "", 0, err
	}
	err = walkRows(ctx, q, relationshipSelect+` ORDER BY r.relationship_id COLLATE BINARY`, func(rows *sql.Rows) error {
		r, err := scanRelationship(rows, library)
		if err != nil {
			return err
		}
		return emit(0x15, relationshipProjection{r.LibraryID, r.ID, r.Value, r.CreatedSequence, r.UpdatedSequence, r.Removed})
	})
	if err != nil {
		return "", 0, err
	}
	err = walkRows(ctx, q, locationSelect+` ORDER BY l.location_id COLLATE BINARY`, func(rows *sql.Rows) error {
		l, err := scanLocation(rows, library)
		if err != nil {
			return err
		}
		return emit(0x16, locationProjection{l.LibraryID, l.ID, l.Value, l.CreatedSequence, l.UpdatedSequence, l.Removed})
	})
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), count, nil
}
