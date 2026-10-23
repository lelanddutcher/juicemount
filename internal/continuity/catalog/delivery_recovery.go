package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/lelanddutcher/juicemount/internal/mutationclient"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
)

var ErrReconciliationRequired = errors.New("materialization requires explicit existing-final reconciliation")

// MaterializationCleanup is issued with a successful exact reservation. Its
// fields are private and MarkUncertain accepts no caller-supplied attempt ID or
// bearer, so revocation cannot prevent conservative fault bookkeeping.
type MaterializationCleanup struct {
	store   *Store
	attempt MaterializationAttempt
}

func (s *Store) ReserveOwnedMaterialization(ctx context.Context, bearer []byte, selectionID, operationID string) (MaterializationAttempt, *MaterializationCleanup, error) {
	a, err := s.ReserveMaterialization(ctx, bearer, selectionID, operationID)
	if err != nil {
		return a, nil, err
	}
	return a, &MaterializationCleanup{store: s, attempt: a}, nil
}

// ExistingOwnedMaterialization is an exact authorized lookup. Explicit
// reconciliation cannot allocate an attempt or consume admission quota.
func (s *Store) ExistingOwnedMaterialization(ctx context.Context, bearer []byte, selectionID, operationID string) (result MaterializationAttempt, cleanup *MaterializationCleanup, resultErr error) {
	if !validRandomID(selectionID, "dsel_") || !validOperationID(operationID) {
		return result, nil, invalid("materialization identity")
	}
	if err := s.enter(); err != nil {
		return result, nil, err
	}
	defer s.leave()
	if !supportsDelivery(s.version) {
		return result, nil, ErrUpgradeRequired
	}
	f, err := s.beginDeliveryClockTxn(ctx)
	if err != nil {
		return result, nil, err
	}
	defer f.finish(&resultErr)
	tx := f.tx
	if err := s.checkVersion(ctx, tx); err != nil {
		return result, nil, err
	}
	if _, err := s.selectionFence(ctx, f, bearer, selectionID); err != nil {
		return result, nil, err
	}
	var attemptID string
	err = tx.QueryRowContext(ctx, `SELECT attempt_id FROM delivery_materializations WHERE selection_id=?`, selectionID).Scan(&attemptID)
	if errors.Is(err, sql.ErrNoRows) {
		return result, nil, ErrNotFound
	}
	if err != nil {
		return result, nil, err
	}
	row, err := readMaterialization(ctx, tx, attemptID)
	if err != nil {
		return result, nil, err
	}
	if row.Attempt.OperationID != operationID {
		return result, nil, ErrConflict
	}
	if err := ctx.Err(); err != nil {
		return result, nil, err
	}
	if err := s.checkPaths(); err != nil {
		return result, nil, err
	}
	if err := f.commit(); err != nil {
		return result, nil, fmt.Errorf("materialization existing lookup commit uncertain (operation %s): %w", operationID, err)
	}
	result = row.Attempt
	cleanup = &MaterializationCleanup{store: s, attempt: result}
	return result, cleanup, nil
}

func (c *MaterializationCleanup) MarkUncertain() (resultErr error) {
	if c == nil || c.store == nil || !validRandomID(c.attempt.ID, "dmat_") {
		return invalid("materialization cleanup capability")
	}
	s := c.store
	if err := s.enter(); err != nil {
		return err
	}
	defer s.leave()
	if !supportsDelivery(s.version) {
		return ErrUpgradeRequired
	}
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("materialization state uncertain: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, fmt.Errorf("materialization cleanup rollback: %w", err))
		}
	}()
	if err := s.checkVersion(ctx, tx); err != nil {
		return err
	}
	row, err := readMaterialization(ctx, tx, c.attempt.ID)
	if err != nil {
		return err
	}
	a := row.Attempt
	if a.ID != c.attempt.ID || a.SelectionID != c.attempt.SelectionID || a.OperationID != c.attempt.OperationID || a.Content != c.attempt.Content {
		return ErrConflict
	}
	if a.State == "RESERVED" || len(row.Root) == 0 || len(row.Directory) == 0 || len(row.Lock) == 0 || len(row.Partial) == 0 {
		return fmt.Errorf("materialization state uncertain: %w", ErrReconciliationRequired)
	}
	if a.State != "UNCERTAIN" {
		stamp := s.now().UnixNano()
		if err := changedOne(tx.ExecContext(ctx, `UPDATE delivery_materializations SET state='UNCERTAIN',updated_unix_nano=max(updated_unix_nano,?) WHERE attempt_id=? AND selection_id=? AND operation_id=? AND state IN('PREPARED','COMPLETED')`, stamp, a.ID, a.SelectionID, a.OperationID)); err != nil {
			return fmt.Errorf("materialization state uncertain: %w", err)
		}
	}
	if err := s.checkPaths(); err != nil {
		return fmt.Errorf("materialization state uncertain: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("materialization state uncertain after commit: %w", err)
	}
	return nil
}

// RecoveryBinding is read-only and contains only the exact previously recorded
// descriptor identities. An older attempt without a lock/directory binding is
// never repaired by this operation.
type RecoveryBinding struct {
	Attempt                     MaterializationAttempt
	Root, Directory, Lock, File privatefile.Identity
}

func decodeBoundIdentity(raw []byte) (privatefile.Identity, error) {
	var id privatefile.Identity
	if len(raw) == 0 || json.Unmarshal(raw, &id) != nil {
		return id, corrupt("missing managed identity binding")
	}
	encoded, err := json.Marshal(id)
	if err != nil || !bytes.Equal(encoded, raw) {
		return privatefile.Identity{}, corrupt("noncanonical managed identity binding")
	}
	return id, nil
}

func (s *Store) MaterializationRecoveryBinding(ctx context.Context, bearer []byte, attemptID string) (result RecoveryBinding, resultErr error) {
	if !validRandomID(attemptID, "dmat_") {
		return result, invalid("materialization identity")
	}
	if err := s.enter(); err != nil {
		return result, err
	}
	defer s.leave()
	if !supportsDelivery(s.version) {
		return result, ErrUpgradeRequired
	}
	f, err := s.beginDeliveryClockTxn(ctx)
	if err != nil {
		return result, err
	}
	defer f.finish(&resultErr)
	tx := f.tx
	if err := s.checkVersion(ctx, tx); err != nil {
		return result, err
	}
	row, err := readMaterialization(ctx, tx, attemptID)
	if err != nil {
		return result, err
	}
	if _, err := s.selectionFence(ctx, f, bearer, row.Attempt.SelectionID); err != nil {
		return result, err
	}
	if row.Attempt.State != "PREPARED" && row.Attempt.State != "COMPLETED" && row.Attempt.State != "UNCERTAIN" {
		return result, ErrReconciliationRequired
	}
	if row.Attempt.State == "COMPLETED" && !bytes.Equal(row.Ready, row.Partial) {
		return result, corrupt("completed ready identity")
	}
	result.Attempt = row.Attempt
	if result.Root, err = decodeBoundIdentity(row.Root); err != nil {
		return RecoveryBinding{}, err
	}
	if result.Directory, err = decodeBoundIdentity(row.Directory); err != nil {
		return RecoveryBinding{}, err
	}
	if result.Lock, err = decodeBoundIdentity(row.Lock); err != nil {
		return RecoveryBinding{}, err
	}
	if result.File, err = decodeBoundIdentity(row.Partial); err != nil {
		return RecoveryBinding{}, err
	}
	if result.File.Size != row.Attempt.Content.Size {
		return RecoveryBinding{}, corrupt("managed content length binding")
	}
	if err := ctx.Err(); err != nil {
		return RecoveryBinding{}, err
	}
	if err := s.checkPaths(); err != nil {
		return RecoveryBinding{}, err
	}
	if err := f.commit(); err != nil {
		return RecoveryBinding{}, fmt.Errorf("materialization binding commit uncertain: %w", err)
	}
	return result, nil
}

// RecoverMaterialization verifies the complete original final file before it
// begins the independently owned writer fence. Remote current-view status is
// sampled after the hash, then the fence rechecks all local authority and row
// bindings. No partial file is promoted here.
func (s *Store) RecoverMaterialization(ctx context.Context, bearer []byte, owned *privatefile.OwnedReady, client *mutationclient.Client, pinned mutationauthority.ImageView) (result MaterializationAttempt, resultErr error) {
	if owned == nil || client == nil || !validRandomID(owned.ID(), "dmat_") {
		return result, invalid("owned recovery input")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	// The first read retrieves only the content claim. The later writer fence
	// compares it again; it never trusts this snapshot for authorization.
	if err := s.enter(); err != nil {
		return result, err
	}
	defer s.leave()
	if !supportsDelivery(s.version) {
		return result, ErrUpgradeRequired
	}
	initial, end, err := s.readSnapshot(ctx)
	if err != nil {
		return result, err
	}
	initialRow, readErr := readMaterialization(ctx, initial, owned.ID())
	closeErr := end()
	if readErr != nil || closeErr != nil {
		return result, errors.Join(readErr, closeErr)
	}
	if err := owned.VerifyContent(initialRow.Attempt.Content); err != nil {
		return result, err
	}
	current, err := client.ImageStatus(ctx, mutationauthority.ImageRef{Version: 1, StoreID: pinned.StoreID, LibraryID: pinned.LibraryID, ImageID: pinned.ImageID})
	if err != nil {
		return result, err
	}
	pinnedBytes, _ := json.Marshal(pinned)
	currentBytes, _ := json.Marshal(current)
	if !bytes.Equal(pinnedBytes, currentBytes) {
		return result, mutationclient.ErrIntegrity
	}
	f, err := s.beginDeliveryClockTxn(ctx)
	if err != nil {
		return result, err
	}
	defer f.finish(&resultErr)
	tx := f.tx
	fenceCtx := context.Background()
	if err := s.checkVersion(fenceCtx, tx); err != nil {
		return result, err
	}
	row, err := readMaterialization(fenceCtx, tx, owned.ID())
	if err != nil {
		return result, err
	}
	if row.Attempt.Content != initialRow.Attempt.Content || row.Attempt.SelectionID != initialRow.Attempt.SelectionID || row.Attempt.OperationID != initialRow.Attempt.OperationID {
		return result, ErrConflict
	}
	binding, err := s.selectionFence(fenceCtx, f, bearer, row.Attempt.SelectionID)
	if err != nil {
		return result, err
	}
	if binding.Selection.Content != row.Attempt.Content {
		return result, ErrConflict
	}
	viewBytes, _ := json.Marshal(binding.View)
	if !bytes.Equal(viewBytes, pinnedBytes) {
		return result, mutationclient.ErrIntegrity
	}
	root, _ := json.Marshal(owned.RootIdentity())
	dir, _ := json.Marshal(owned.AttemptIdentity())
	lock, _ := json.Marshal(owned.LockIdentity())
	file, _ := json.Marshal(owned.FileIdentity())
	if !bytes.Equal(row.Root, root) || !bytes.Equal(row.Directory, dir) || !bytes.Equal(row.Lock, lock) || !bytes.Equal(row.Partial, file) || len(row.Ready) != 0 && !bytes.Equal(row.Ready, file) {
		return result, ErrConflict
	}
	if row.Attempt.State != "PREPARED" && row.Attempt.State != "UNCERTAIN" && row.Attempt.State != "COMPLETED" {
		return result, ErrReconciliationRequired
	}
	if row.Attempt.State == "COMPLETED" && !bytes.Equal(row.Ready, file) {
		return result, corrupt("completed ready binding")
	}
	if err := owned.Revalidate(); err != nil {
		return result, err
	}
	if _, err := s.selectionFence(fenceCtx, f, bearer, row.Attempt.SelectionID); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	// Recovery must repeat both original rename durability barriers under this
	// writer fence; readable bytes and a SQL row alone are insufficient.
	if barrierErr := owned.SyncExistingFinal(); barrierErr != nil {
		if row.Attempt.State != "UNCERTAIN" {
			stamp := s.now().UnixNano()
			if err := changedOne(tx.ExecContext(fenceCtx, `UPDATE delivery_materializations SET state='UNCERTAIN',updated_unix_nano=max(updated_unix_nano,?) WHERE attempt_id=? AND state IN('PREPARED','COMPLETED')`, stamp, row.Attempt.ID)); err != nil {
				return result, errors.Join(barrierErr, fmt.Errorf("recovery barrier state uncertain: %w", err))
			}
		}
		if err := s.checkPaths(); err != nil {
			return result, errors.Join(barrierErr, fmt.Errorf("recovery barrier catalog path uncertain: %w", err))
		}
		if err := f.commit(); err != nil {
			return result, errors.Join(barrierErr, fmt.Errorf("recovery barrier commit uncertain: %w", err))
		}
		result = row.Attempt
		result.State = "UNCERTAIN"
		return result, fmt.Errorf("recovery directory barrier uncertain: %w", barrierErr)
	}
	if row.Attempt.State != "COMPLETED" {
		stamp := s.now().UnixNano()
		if err := changedOne(tx.ExecContext(fenceCtx, `UPDATE delivery_materializations SET state='COMPLETED',ready_identity=?,updated_unix_nano=max(updated_unix_nano,?) WHERE attempt_id=? AND state IN('PREPARED','UNCERTAIN')`, file, stamp, row.Attempt.ID)); err != nil {
			return result, err
		}
	}
	if err := s.checkPaths(); err != nil {
		return result, err
	}
	if err := f.commit(); err != nil {
		return result, fmt.Errorf("materialization recovery commit uncertain: %w", err)
	}
	result = row.Attempt
	result.State = "COMPLETED"
	return result, nil
}
