package catalog

import (
	"context"
	"database/sql"
	"errors"
	"io"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const boundedEventColumns = `sequence,library_id,operation_id,CASE WHEN length(request) BETWEEN 1 AND 1048576 THEN request END,payload_sha256,recorded_unix_nano,CASE WHEN length(receipt) BETWEEN 1 AND 1024 THEN receipt END`

func eventTotal(ctx context.Context, q queryer) (int64, error) {
	var count, last int64
	err := q.QueryRowContext(ctx, `SELECT count(*),COALESCE(max(sequence),0) FROM events`).Scan(&count, &last)
	if err != nil {
		return 0, err
	}
	if count < 0 || last != count {
		return 0, corrupt("event sequence count")
	}
	return count, nil
}

func walkEvents(ctx context.Context, q queryer, library continuity.LibraryID, expected int64, fn func(eventRow) error) error {
	var sequence int64
	err := walkRows(ctx, q, `SELECT `+boundedEventColumns+` FROM events ORDER BY sequence`, func(rows *sql.Rows) error {
		e, err := scanEvent(rows)
		if err != nil {
			return formatError(err)
		}
		if sequence >= expected || e.sequence != sequence+1 {
			return corrupt("event sequence discontinuity")
		}
		if _, err := e.decode(library); err != nil {
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
	if err != nil {
		return err
	}
	if sequence != expected {
		return corrupt("missing event records")
	}
	return nil
}

// Export captures one metadata snapshot. It owns only its read pool and SQL
// resources; the caller owns writer lifecycle, deadlines and durable storage.
func (s *Store) Export(ctx context.Context, library continuity.LibraryID, writer io.Writer, limits Limits) (resultErr error) {
	if err := s.enter(); err != nil {
		return err
	}
	defer s.leave()
	if writer == nil {
		return invalid("nil export writer")
	}
	if err := limits.validate(); err != nil {
		return err
	}
	if err := s.requireLibrary(library); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := openDB(s.path, true)
	if err != nil {
		return err
	}
	defer func() {
		if db != nil {
			resultErr = errors.Join(resultErr, db.Close())
		}
	}()
	tx, err := db.BeginTx(ctx, nil) // mode=ro, default deferred; never s.db.
	if err != nil {
		return err
	}
	defer tx.Rollback()
	actual, err := validateSchema(ctx, tx)
	if err != nil {
		return err
	}
	if actual != library {
		return corrupt("export library identity")
	}
	if err := checkSettings(ctx, tx); err != nil {
		return err
	}
	if err := checkIntegrity(ctx, tx); err != nil {
		return err
	}
	if err := s.checkPaths(); err != nil {
		return err
	}
	count, err := eventTotal(ctx, tx)
	if err != nil {
		return err
	}
	version, err := catalogVersion(ctx, tx)
	if err != nil {
		return err
	}
	if err := s.checkVersion(ctx, tx); err != nil {
		return err
	}
	if supportsCollections(version) {
		output := streamOutput{ctx: ctx, w: writer, max: limits.MaxBytes}
		footer, err := exportCollectionSnapshot(ctx, tx, library, &output, limits)
		if err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		closeErr := db.Close()
		db = nil
		if closeErr != nil {
			return closeErr
		}
		if err := s.checkPaths(); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := output.frame(3, footer, nil); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return s.checkPaths()
	}
	format := 1
	var policy int64
	if portableSchemaVersion(version) == authoritySchemaVersion {
		format = 2
		policy, err = policyTotal(ctx, tx)
		if err != nil {
			return err
		}
	}
	rows, err := projectionTotal(ctx, tx)
	if err != nil {
		return err
	}
	if count > limits.MaxEvents || policy > limits.MaxEvents-count || rows > limits.MaxProjectionRows {
		return ErrLimit
	}
	output := streamOutput{ctx: ctx, w: writer, max: limits.MaxBytes}
	h := streamHashFor(format)
	magic := streamMagic
	if format == 2 {
		magic = streamMagic2
	}
	if err := output.write([]byte(magic), h); err != nil {
		return err
	}
	header := encodeHeader(format, library, count, policy)
	if err := output.frame(1, header, h); err != nil {
		return err
	}
	if err := walkEvents(ctx, tx, library, count, func(e eventRow) error { return output.frame(2, encodeEvent(e), h) }); err != nil {
		return err
	}
	if format == 2 {
		if err := walkPolicyEvents(ctx, tx, library, policy, func(e eventRow) error { return output.frame(4, encodeEvent(e), h) }); err != nil {
			return err
		}
	}
	projection, observedRows, err := projectionDigest(ctx, tx, library, limits.MaxProjectionRows)
	if err != nil {
		return err
	}
	if rows != observedRows {
		return corrupt("projection row count mismatch")
	}
	digest, err := finishStreamHashFor(format, h, count, policy, rows, projection)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	closeErr := db.Close()
	db = nil
	if closeErr != nil {
		return closeErr
	}
	if err := s.checkPaths(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	footer := encodeFooter(format, streamFooter2{count, policy, rows, projection, digest})
	if err := output.frame(3, footer, nil); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.checkPaths()
}
