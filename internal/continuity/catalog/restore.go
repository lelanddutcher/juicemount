package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// Replayed events commit in batches: the target stays ready=0 until the final
// verification transaction, so a batch boundary only changes how much
// incomplete progress a failed restore leaves behind, not what Open admits.
var (
	restoreBatchEvents = 500
	restoreBatchBytes  = 16 << 20
	// restoreBatchCommitted observes replay batch commits in tests.
	restoreBatchCommitted func()
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
	if string(magic) != streamMagic {
		return corrupt("export magic")
	}
	h := streamHash()
	_, _ = h.Write(magic)
	headerData, err := input.frame(1, h)
	if err != nil {
		return err
	}
	var header streamHeader
	if err := canonicalJSON(headerData, &header); err != nil {
		return err
	}
	if header.Version != 1 || header.SchemaVersion < 2 || header.SchemaVersion > schemaVersion || header.LibraryID != library || header.EventCount < 0 {
		return corrupt("export header identity or version")
	}
	if header.EventCount > limits.MaxEvents {
		return ErrLimit
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// The restored catalog is at the format the stream names: format 3
	// unless the stream holds queries.
	format := MinOperableFormat
	if header.SchemaVersion > format {
		format = header.SchemaVersion
	}
	s, err := openStoreContext(ctx, path, library, true, false, format)
	if err != nil {
		return err
	}
	defer func() {
		if s != nil {
			resultErr = errors.Join(resultErr, s.Close())
		}
	}()
	var batch *sql.Tx
	defer func() {
		if batch != nil {
			batch.Rollback()
		}
	}()
	batchEvents, batchBytes := 0, 0
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
		for _, change := range op.Changes {
			if header.SchemaVersion < 3 && isCollectionChange(change) {
				return corrupt("collection change in a format 2 stream")
			}
			if header.SchemaVersion < 4 && isQueryChange(change) {
				return corrupt("query change in a format 2 or 3 stream")
			}
		}
		if batch == nil {
			if batch, err = s.beginReplayBatch(ctx); err != nil {
				return protocolError("restore replay", err)
			}
		}
		// Row accounting includes the open batch's uncommitted inserts; a
		// failure rolls the whole batch back and the restore stops.
		nextRows, err := s.replayEvent(ctx, batch, event, op, projectionRows, limits.MaxProjectionRows)
		if err != nil {
			return protocolError("restore replay", err)
		}
		projectionRows = nextRows
		batchEvents++
		batchBytes += len(event.request)
		if batchEvents >= restoreBatchEvents || batchBytes >= restoreBatchBytes || sequence == header.EventCount {
			err := commitReplayBatch(ctx, batch)
			batch = nil
			if err != nil {
				return protocolError("restore replay", err)
			}
			batchEvents, batchBytes = 0, 0
		}
	}
	footerData, err := input.frame(3, nil)
	if err != nil {
		return err
	}
	var footer streamFooter
	if err := canonicalJSON(footerData, &footer); err != nil {
		return err
	}
	if footer.EventCount != header.EventCount || footer.ProjectionRows < 1 {
		return corrupt("footer counts")
	}
	if footer.ProjectionRows > limits.MaxProjectionRows {
		return ErrLimit
	}
	digest, err := finishStreamHash(h, header.EventCount, footer.ProjectionRows, footer.ProjectionSHA256)
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
	// The exporter names the oldest format that holds the snapshot.
	if format, err := streamFormat(ctx, tx); err != nil {
		return err
	} else if format != header.SchemaVersion {
		return corrupt("stream format does not match its contents")
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

func (s *Store) beginReplayBatch(ctx context.Context) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if err := requireIncomplete(ctx, tx); err != nil {
		tx.Rollback()
		return nil, err
	}
	if err := s.checkPaths(); err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}

func commitReplayBatch(ctx context.Context, tx *sql.Tx) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("incomplete restore batch commit uncertain: %w", err)
	}
	if restoreBatchCommitted != nil {
		restoreBatchCommitted()
	}
	return nil
}

// replayEvent applies one event inside the open batch transaction with the same
// per-change invariants as Apply, returning the updated projection-row count.
func (s *Store) replayEvent(ctx context.Context, tx *sql.Tx, event eventRow, op Operation, rows, maxRows int64) (int64, error) {
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
	if err := checkCollectionLimits(ctx, tx, op); err != nil {
		return 0, err
	}
	if err := checkQueryLimits(ctx, tx, op); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(sequence,library_id,operation_id,request,payload_sha256,recorded_unix_nano,receipt) VALUES(?,?,?,?,?,?,?)`,
		event.sequence, event.library, event.id, event.request, event.digest, event.timestamp, event.receipt); err != nil {
		return 0, formatError(err)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
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
	case change.CreateAsset != nil, change.CreateCollection != nil, change.CreateQuery != nil:
		return 1, nil // Success necessarily inserts; existing IDs are conflicts.
	case change.AddCollectionMember != nil:
		var exists bool
		v := change.AddCollectionMember
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM collection_members WHERE collection_id=? AND asset_id=?)`, v.CollectionID, v.AssetID).Scan(&exists); err != nil {
			return 0, err
		}
		if exists {
			return 0, nil // Reactivation or no change.
		}
		return 1, nil
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
