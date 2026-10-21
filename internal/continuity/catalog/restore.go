package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// Restore admits a complete metadata stream into a newly reserved, incomplete
// target only. It never resumes, merges, overwrites or removes a failed target.
func Restore(ctx context.Context, path string, library continuity.LibraryID, reader io.Reader, limits Limits) error {
	return restoreCatalog(ctx, path, library, reader, limits, syncParent)
}

func restoreCatalog(ctx context.Context, path string, library continuity.LibraryID, reader io.Reader, limits Limits, directoryBarrier func(string) error) (resultErr error) {
	if reader == nil || library.Validate() != nil {
		return invalid("restore reader or library")
	}
	if err := limits.validate(); err != nil {
		return err
	}
	input := streamInput{ctx: ctx, r: reader, max: limits.MaxBytes}
	magic := make([]byte, len(streamMagic))
	if err := input.exact(magic); err != nil {
		return err
	}
	if string(magic) == streamMagic4 {
		return restoreCollectionFormat(ctx, path, library, &input, limits, directoryBarrier, 4)
	}
	if string(magic) == streamMagic3 {
		return restoreCollections(ctx, path, library, &input, limits, directoryBarrier)
	}
	format := 1
	if string(magic) == streamMagic2 {
		format = 2
	} else if string(magic) != streamMagic {
		return corrupt("export magic")
	}
	h := streamHashFor(format)
	_, _ = h.Write(magic)
	headerData, err := input.frame(1, h)
	if err != nil {
		return err
	}
	header, err := decodeHeader(headerData, format, library)
	if err != nil {
		return err
	}
	if header.EventCount > limits.MaxEvents || header.PolicyEventCount > limits.MaxEvents-header.EventCount {
		return ErrLimit
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s, err := openStoreVersionContext(ctx, path, library, true, false, header.SchemaVersion)
	if err != nil {
		return err
	}
	defer func() {
		if s != nil {
			resultErr = errors.Join(resultErr, s.Close())
		}
	}()
	projectionRows := int64(1)
	for sequence := int64(1); sequence <= header.EventCount; sequence++ {
		payload, err := input.frame(2, h)
		if err != nil {
			return protocolError("restore event", err)
		}
		event, op, err := decodeStreamEvent(payload, library, sequence)
		if err != nil {
			return err
		}
		nextRows, err := s.replayEvent(ctx, event, op, projectionRows, limits.MaxProjectionRows)
		if err != nil {
			return protocolError("restore replay", err)
		}
		projectionRows = nextRows
	}
	for sequence := int64(1); sequence <= header.PolicyEventCount; sequence++ {
		payload, err := input.frame(4, h)
		if err != nil {
			return protocolError("restore policy event", err)
		}
		e, op, err := decodeStreamPolicyEvent(payload, library, sequence)
		if err != nil {
			return err
		}
		projectionRows, err = s.replayPolicyEvent(ctx, e, op, projectionRows, limits.MaxProjectionRows)
		if err != nil {
			return protocolError("restore policy replay", err)
		}
	}
	footerData, err := input.frame(3, nil)
	if err != nil {
		return err
	}
	footer, err := decodeFooter(footerData, format)
	if err != nil {
		return err
	}
	if footer.EventCount != header.EventCount || footer.PolicyEventCount != header.PolicyEventCount || footer.ProjectionRows < 1 {
		return corrupt("footer counts")
	}
	if footer.ProjectionRows > limits.MaxProjectionRows {
		return ErrLimit
	}
	digest, err := finishStreamHashFor(format, h, header.EventCount, header.PolicyEventCount, footer.ProjectionRows, footer.ProjectionSHA256)
	if err != nil {
		return err
	}
	if (continuity.Content{SHA256: footer.StreamSHA256}).Validate() != nil || digest != footer.StreamSHA256 {
		return corrupt("stream digest mismatch")
	}
	if err := input.end(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	actual, err := validateSchemaState(ctx, tx, true)
	if err != nil {
		return err
	}
	if actual != library {
		return corrupt("restore catalog identity")
	}
	if err := requireIncomplete(ctx, tx); err != nil {
		return err
	}
	if err := checkSettings(ctx, tx); err != nil {
		return err
	}
	if err := checkIntegrity(ctx, tx); err != nil {
		return err
	}
	if err := walkEvents(ctx, tx, library, header.EventCount, nil); err != nil {
		return err
	}
	if format == 2 {
		if err := walkPolicyEvents(ctx, tx, library, header.PolicyEventCount, nil); err != nil {
			return err
		}
	}
	projection, rows, err := projectionDigest(ctx, tx, library, limits.MaxProjectionRows)
	if err != nil {
		return err
	}
	if projection != footer.ProjectionSHA256 || rows != footer.ProjectionRows || rows != projectionRows {
		return corrupt("restored projection does not match source snapshot")
	}
	if err := s.checkPaths(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE catalog SET ready=1 WHERE singleton=1 AND ready=0`)
	if err := changedOne(result, err); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog restore commit outcome uncertain: %w", err)
	}
	canonicalTarget := s.path
	closeErr := s.Close()
	s = nil
	if closeErr != nil {
		return fmt.Errorf("catalog restore close outcome uncertain: %w", closeErr)
	}
	if err := directoryBarrier(canonicalTarget); err != nil {
		return fmt.Errorf("catalog restore directory barrier uncertain: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("catalog restore acknowledgment uncertain: %w", err)
	}
	return nil
}

func requireIncomplete(ctx context.Context, q queryer) error {
	var ready int
	if err := q.QueryRowContext(ctx, `SELECT ready FROM catalog WHERE singleton=1`).Scan(&ready); err != nil {
		return err
	}
	if ready != 0 {
		return corrupt("restore target is no longer incomplete")
	}
	return nil
}

func (s *Store) replayEvent(ctx context.Context, event eventRow, op Operation, rows, maxRows int64) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if err := s.checkVersion(ctx, tx); err != nil {
		return 0, err
	}
	if supportsCollections(s.version) {
		if event.library != s.library || op.LibraryID != s.library {
			return 0, corrupt("metadata replay library")
		}
		if err := s.prepareCollectionReplay(ctx, tx); err != nil {
			return 0, err
		}
	}
	if err := requireIncomplete(ctx, tx); err != nil {
		return 0, err
	}
	if err := s.checkPaths(); err != nil {
		return 0, err
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE operation_id=?)`, event.id).Scan(&exists); err != nil {
		return 0, err
	}
	if exists {
		return 0, corrupt("duplicate restore operation ID")
	}
	for _, change := range op.Changes {
		cost, err := projectionInsertCost(ctx, tx, change)
		if err != nil {
			return 0, err
		}
		if cost > maxRows-rows {
			return 0, ErrLimit
		}
		if err := s.applyChange(ctx, tx, change, event.sequence); err != nil {
			return 0, err
		}
		rows += cost
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(sequence,library_id,operation_id,request,payload_sha256,recorded_unix_nano,receipt) VALUES(?,?,?,?,?,?,?)`,
		event.sequence, event.library, event.id, event.request, event.digest, event.timestamp, event.receipt); err != nil {
		return 0, formatError(err)
	}
	if err := s.checkPaths(); err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("incomplete restore batch commit uncertain: %w", err)
	}
	return rows, nil
}

// Observe identities under the same write lock immediately before applying each
// change. A successful apply fixes this exact insertion delta; duplicates and
// tombstone edits cost zero. No growing-table rescans or optimistic estimates.
func projectionInsertCost(ctx context.Context, tx *sql.Tx, change Change) (int64, error) {
	table, column, id := "", "", ""
	var cost int64 = 1
	switch {
	case change.CreateAsset != nil:
		return 1, nil // Success necessarily inserts; existing IDs are conflicts.
	case change.RecordRevision != nil:
		revision, err := change.RecordRevision.RevisionID()
		if err != nil {
			return 0, err
		}
		table, column, id = "revisions", "revision_id", string(revision)
		cost += int64(len(change.RecordRevision.Parents))
	case change.PutRelationship != nil:
		table, column, id = "relationships", "relationship_id", change.PutRelationship.ID
	case change.PutLocation != nil:
		table, column, id = "locations", "location_id", change.PutLocation.ID
	default:
		return 0, nil
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM "+table+" WHERE "+column+"=?)", id).Scan(&exists); err != nil {
		return 0, err
	}
	if exists {
		return 0, nil
	}
	return cost, nil
}
