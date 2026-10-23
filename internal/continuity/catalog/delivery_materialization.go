package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
)

type MaterializationAttempt struct {
	ID          string             `json:"attempt_id"`
	SelectionID string             `json:"selection_id"`
	OperationID string             `json:"operation_id"`
	State       string             `json:"state"`
	Content     continuity.Content `json:"content"`
}

type materializationRow struct {
	Attempt                               MaterializationAttempt
	Root, Directory, Lock, Partial, Ready []byte
}

func readMaterialization(ctx context.Context, q queryer, id string) (materializationRow, error) {
	var row materializationRow
	a := &row.Attempt
	err := q.QueryRowContext(ctx, `SELECT attempt_id,selection_id,operation_id,state,content_size,content_sha256,root_identity,attempt_identity,lock_identity,partial_identity,ready_identity FROM delivery_materializations WHERE attempt_id=?`, id).Scan(&a.ID, &a.SelectionID, &a.OperationID, &a.State, &a.Content.Size, &a.Content.SHA256, &row.Root, &row.Directory, &row.Lock, &row.Partial, &row.Ready)
	if errors.Is(err, sql.ErrNoRows) {
		return materializationRow{}, ErrNotFound
	}
	if err != nil {
		return materializationRow{}, err
	}
	if !validRandomID(a.ID, "dmat_") || !validRandomID(a.SelectionID, "dsel_") || !validOperationID(a.OperationID) || a.Content.Validate() != nil {
		return materializationRow{}, corrupt("materialization fields")
	}
	return row, nil
}
func (s *Store) ReserveMaterialization(ctx context.Context, bearer []byte, selectionID, operationID string) (result MaterializationAttempt, resultErr error) {
	if err := s.enter(); err != nil {
		return MaterializationAttempt{}, err
	}
	defer s.leave()
	if !supportsDelivery(s.version) {
		return MaterializationAttempt{}, ErrUpgradeRequired
	}
	if !validRandomID(selectionID, "dsel_") || !validOperationID(operationID) {
		return MaterializationAttempt{}, invalid("materialization identity")
	}
	f, err := s.beginDeliveryClockTxn(ctx)
	if err != nil {
		return MaterializationAttempt{}, err
	}
	defer f.finish(&resultErr)
	tx := f.tx
	if err := s.checkVersion(ctx, tx); err != nil {
		return MaterializationAttempt{}, err
	}
	binding, err := s.selectionFence(ctx, f, bearer, selectionID)
	if err != nil {
		return MaterializationAttempt{}, err
	}
	var oldID string
	err = tx.QueryRowContext(ctx, `SELECT attempt_id FROM delivery_materializations WHERE selection_id=?`, selectionID).Scan(&oldID)
	if err == nil {
		priorRow, err := readMaterialization(ctx, tx, oldID)
		if err != nil {
			return MaterializationAttempt{}, err
		}
		prior := priorRow.Attempt
		if prior.OperationID != operationID {
			return MaterializationAttempt{}, ErrConflict
		}
		if err := ctx.Err(); err != nil {
			return MaterializationAttempt{}, err
		}
		if err := s.checkPaths(); err != nil {
			return MaterializationAttempt{}, err
		}
		if err := f.commit(); err != nil {
			return MaterializationAttempt{}, fmt.Errorf("materialization retry commit uncertain (operation %s): %w", operationID, err)
		}
		return prior, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return MaterializationAttempt{}, err
	}
	var attempts, canonical, reserved int64
	if err := tx.QueryRowContext(ctx, `SELECT attempts,canonical_bytes,reserved_logical_bytes FROM delivery_quota WHERE singleton=1`).Scan(&attempts, &canonical, &reserved); err != nil {
		return MaterializationAttempt{}, corrupt("delivery quota")
	}
	if attempts >= 4096 || canonical+512 > 64<<20 || binding.Selection.Content.Size > 4<<30-reserved {
		return MaterializationAttempt{}, ErrLimit
	}
	id, err := randomIdentity("dmat_")
	if err != nil {
		return MaterializationAttempt{}, err
	}
	timestamp := s.now().UnixNano()
	a := MaterializationAttempt{id, selectionID, operationID, "RESERVED", binding.Selection.Content}
	if _, err := tx.ExecContext(ctx, `INSERT INTO delivery_materializations(attempt_id,selection_id,operation_id,state,content_size,content_sha256,created_unix_nano,updated_unix_nano) VALUES(?,?,?,'RESERVED',?,?,?,?)`, id, selectionID, operationID, a.Content.Size, a.Content.SHA256, timestamp, timestamp); err != nil {
		return MaterializationAttempt{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE delivery_quota SET attempts=attempts+1,canonical_bytes=canonical_bytes+512,reserved_logical_bytes=reserved_logical_bytes+? WHERE singleton=1`, a.Content.Size); err != nil {
		return MaterializationAttempt{}, err
	}
	if err := ctx.Err(); err != nil {
		return MaterializationAttempt{}, err
	}
	if err := s.checkPaths(); err != nil {
		return MaterializationAttempt{}, err
	}
	if err := f.commit(); err != nil {
		return MaterializationAttempt{}, fmt.Errorf("materialization reservation uncertain (operation %s): %w", operationID, err)
	}
	return a, nil
}
func (s *Store) RecordPreparedMaterialization(ctx context.Context, bearer []byte, p *privatefile.PreparedFile) (result MaterializationAttempt, resultErr error) {
	if p == nil || !validRandomID(p.ID(), "dmat_") {
		return MaterializationAttempt{}, invalid("owned prepared file")
	}
	if err := p.Revalidate(); err != nil {
		return MaterializationAttempt{}, err
	}
	if err := s.enter(); err != nil {
		return MaterializationAttempt{}, err
	}
	defer s.leave()
	if !supportsDelivery(s.version) {
		return MaterializationAttempt{}, ErrUpgradeRequired
	}
	// Whole-file verification occurs outside the SQL fence and independently
	// from the chunk receipts reported by the image client.
	if err := ctx.Err(); err != nil {
		return MaterializationAttempt{}, err
	}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return MaterializationAttempt{}, err
	}
	aRow, err := readMaterialization(ctx, tx, p.ID())
	if rollbackErr := tx.Rollback(); rollbackErr != nil {
		return MaterializationAttempt{}, rollbackErr
	}
	if err != nil {
		return MaterializationAttempt{}, err
	}
	a := aRow.Attempt
	if a.State != "RESERVED" {
		return MaterializationAttempt{}, ErrConflict
	}
	readFile := p.ReadOnlyFile()
	if readFile == nil {
		return MaterializationAttempt{}, invalid("prepared file closed")
	}
	if _, err := readFile.Seek(0, io.SeekStart); err != nil {
		return MaterializationAttempt{}, err
	}
	if err := continuity.VerifyContent(readFile, a.Content); err != nil {
		return MaterializationAttempt{}, err
	}
	if _, err := readFile.Seek(0, io.SeekStart); err != nil {
		return MaterializationAttempt{}, err
	}
	if err := p.Revalidate(); err != nil {
		return MaterializationAttempt{}, err
	}
	rootID, _ := json.Marshal(p.RootIdentity())
	dirID, _ := json.Marshal(p.AttemptIdentity())
	lockID, _ := json.Marshal(p.LockIdentity())
	fileID, _ := json.Marshal(p.FileIdentity())
	f, err := s.beginDeliveryClockTxn(ctx)
	if err != nil {
		return MaterializationAttempt{}, err
	}
	defer f.finish(&resultErr)
	tx = f.tx
	if err := s.checkVersion(ctx, tx); err != nil {
		return MaterializationAttempt{}, err
	}
	binding, err := s.selectionFence(ctx, f, bearer, a.SelectionID)
	if err != nil {
		return MaterializationAttempt{}, err
	}
	if binding.Selection.Content != a.Content || p.FileIdentity().Size != a.Content.Size {
		return MaterializationAttempt{}, ErrConflict
	}
	currentRow, err := readMaterialization(ctx, tx, p.ID())
	if err != nil {
		return MaterializationAttempt{}, err
	}
	if currentRow.Attempt.State != "RESERVED" || currentRow.Attempt.Content != a.Content {
		return MaterializationAttempt{}, ErrConflict
	}
	if err := p.Revalidate(); err != nil {
		return MaterializationAttempt{}, err
	}
	timestamp := s.now().UnixNano()
	if err := changedOne(tx.ExecContext(ctx, `UPDATE delivery_materializations SET state='PREPARED',root_identity=?,attempt_identity=?,lock_identity=?,partial_identity=?,updated_unix_nano=? WHERE attempt_id=? AND state='RESERVED'`, rootID, dirID, lockID, fileID, timestamp, p.ID())); err != nil {
		return MaterializationAttempt{}, err
	}
	if err := ctx.Err(); err != nil {
		return MaterializationAttempt{}, err
	}
	if err := s.checkPaths(); err != nil {
		return MaterializationAttempt{}, err
	}
	if err := f.commit(); err != nil {
		return MaterializationAttempt{}, fmt.Errorf("prepared record uncertain (attempt %s): %w", p.ID(), err)
	}
	a.State = "PREPARED"
	return a, nil
}

// PublishMaterialization owns its writer transaction until the exact original
// no-replace exposure and both directory Sync calls have returned. The request
// context is checked manually but cannot roll the fence back during file I/O.
func (s *Store) PublishMaterialization(ctx context.Context, bearer []byte, p *privatefile.PreparedFile) (result MaterializationAttempt, resultErr error) {
	if p == nil || !validRandomID(p.ID(), "dmat_") {
		return MaterializationAttempt{}, invalid("owned prepared file")
	}
	if err := ctx.Err(); err != nil {
		return MaterializationAttempt{}, err
	}
	if err := s.enter(); err != nil {
		return MaterializationAttempt{}, err
	}
	defer s.leave()
	if !supportsDelivery(s.version) {
		return MaterializationAttempt{}, ErrUpgradeRequired
	}
	fenceCtx := context.Background()
	f, err := s.beginDeliveryClockTxn(ctx)
	if err != nil {
		return MaterializationAttempt{}, err
	}
	defer f.finish(&resultErr)
	tx := f.tx
	if err := s.checkVersion(fenceCtx, tx); err != nil {
		return MaterializationAttempt{}, err
	}
	aRow, err := readMaterialization(fenceCtx, tx, p.ID())
	if err != nil {
		return MaterializationAttempt{}, err
	}
	a := aRow.Attempt
	if a.State != "PREPARED" {
		return MaterializationAttempt{}, ErrConflict
	}
	binding, err := s.selectionFence(fenceCtx, f, bearer, a.SelectionID)
	if err != nil {
		return MaterializationAttempt{}, err
	}
	if binding.Selection.Content != a.Content {
		return MaterializationAttempt{}, ErrConflict
	}
	rootNow, _ := json.Marshal(p.RootIdentity())
	dirNow, _ := json.Marshal(p.AttemptIdentity())
	lockNow, _ := json.Marshal(p.LockIdentity())
	fileNow, _ := json.Marshal(p.FileIdentity())
	if !bytes.Equal(aRow.Root, rootNow) || !bytes.Equal(aRow.Directory, dirNow) || !bytes.Equal(aRow.Lock, lockNow) || !bytes.Equal(aRow.Partial, fileNow) || p.FileIdentity().Size != a.Content.Size {
		return MaterializationAttempt{}, ErrConflict
	}
	if err := p.Revalidate(); err != nil {
		return MaterializationAttempt{}, err
	}
	// The original writer fence remains owned while the preparatory path
	// checks finish. Reobserve expiry and current authority immediately before
	// entering the synchronous no-replace exposure operation.
	if _, err := s.selectionFence(fenceCtx, f, bearer, a.SelectionID); err != nil {
		return MaterializationAttempt{}, err
	}
	if err := ctx.Err(); err != nil {
		return MaterializationAttempt{}, err
	}
	exposed, exposureErr := p.ExposeNoReplace()
	if !exposed {
		return MaterializationAttempt{}, exposureErr
	}
	state := "COMPLETED"
	if exposureErr != nil {
		state = "UNCERTAIN"
	}
	timestamp := s.now().UnixNano()
	if err := changedOne(tx.ExecContext(fenceCtx, `UPDATE delivery_materializations SET state=?,ready_identity=?,updated_unix_nano=? WHERE attempt_id=? AND state='PREPARED'`, state, fileNow, timestamp, a.ID)); err != nil {
		return MaterializationAttempt{}, errors.Join(exposureErr, fmt.Errorf("publication state uncertain: %w", err))
	}
	if err := s.checkPaths(); err != nil {
		return MaterializationAttempt{}, errors.Join(exposureErr, fmt.Errorf("publication catalog path uncertain: %w", err))
	}
	if err := f.commit(); err != nil {
		return MaterializationAttempt{}, errors.Join(exposureErr, fmt.Errorf("publication commit uncertain (attempt %s): %w", a.ID, err))
	}
	a.State = state
	if exposureErr != nil {
		return a, fmt.Errorf("publication directory barrier uncertain: %w", exposureErr)
	}
	return a, nil
}
