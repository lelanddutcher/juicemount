package catalog

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestMaterializationBoundCleanupSurvivesRevocation(t *testing.T) {
	f := newDeliveryClockFixture(t, 3600)
	_, selection := f.selectLatest(t, "cleanup-selection", deliveryClockBase+600)
	prepared, _, initial := clockPrepared(t, f, selection.SelectionID)
	attempt, cleanup, err := f.store.ReserveOwnedMaterialization(context.Background(), f.bearer, selection.SelectionID, initial.OperationID)
	if err != nil || cleanup == nil || attempt.ID != initial.ID || attempt.State != "PREPARED" {
		t.Fatalf("bound exact retry: %+v cleanup=%v err=%v", attempt, cleanup != nil, err)
	}
	completed, err := f.store.PublishMaterialization(context.Background(), f.bearer, prepared)
	if err != nil || completed.State != "COMPLETED" {
		t.Fatalf("original publication: %+v %v", completed, err)
	}
	owner, err := f.store.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := readAuthority(context.Background(), f.store.db, testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	selector := "cred_" + string(f.bearer[5:37])
	if _, err := owner.RevokeCredential(context.Background(), adminTestRequest("cleanup-revoke", authority.Incarnation, "revoke_credential", RevokeCredentialRequest{Selector: selector, ExpectedGeneration: 1})); err != nil {
		t.Fatal(err)
	}
	if err := cleanup.MarkUncertain(); err != nil {
		t.Fatalf("bound cleanup after bearer revoke: %v", err)
	}
	if err := cleanup.MarkUncertain(); err != nil {
		t.Fatalf("repeat bound cleanup: %v", err)
	}
	row, err := readMaterialization(context.Background(), f.store.db, attempt.ID)
	if err != nil || row.Attempt.State != "UNCERTAIN" || len(row.Ready) == 0 {
		t.Fatalf("durable correction after revoke: %+v %v", row.Attempt, err)
	}
	if _, err := f.store.MaterializationRecoveryBinding(context.Background(), f.bearer, attempt.ID); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("revoked bearer recovered rights: %v", err)
	}
}

func TestMaterializationCleanupWriteFaultDoesNotClaimDowngrade(t *testing.T) {
	f := newDeliveryClockFixture(t, 3600)
	_, selection := f.selectLatest(t, "cleanup-fault-selection", deliveryClockBase+600)
	prepared, _, initial := clockPrepared(t, f, selection.SelectionID)
	attempt, cleanup, err := f.store.ReserveOwnedMaterialization(context.Background(), f.bearer, selection.SelectionID, initial.OperationID)
	if err != nil || cleanup == nil {
		t.Fatalf("cleanup capability: %+v %v", attempt, err)
	}
	if completed, err := f.store.PublishMaterialization(context.Background(), f.bearer, prepared); err != nil || completed.State != "COMPLETED" {
		t.Fatalf("original publication: %+v %v", completed, err)
	}
	if _, err := f.store.db.Exec(`CREATE TEMP TRIGGER cleanup_write_fault BEFORE UPDATE OF state ON main.delivery_materializations WHEN NEW.state='UNCERTAIN' BEGIN SELECT RAISE(ABORT,'injected cleanup write fault'); END`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := f.store.db.Exec(`DROP TRIGGER cleanup_write_fault`); err != nil {
			t.Error(err)
		}
	}()
	if err := cleanup.MarkUncertain(); err == nil || !strings.Contains(err.Error(), "injected cleanup write fault") {
		t.Fatalf("cleanup write fault omitted: %v", err)
	}
	row, err := readMaterialization(context.Background(), f.store.db, attempt.ID)
	if err != nil || row.Attempt.State != "COMPLETED" || len(row.Ready) == 0 {
		t.Fatalf("failed write claimed correction: %+v %v", row.Attempt, err)
	}
}

func TestExistingOwnedMaterializationDoesNotReserveOnMissOrAtLimit(t *testing.T) {
	f := newDeliveryClockFixture(t, 3600)
	_, selection := f.selectLatest(t, "existing-lookup-selection", deliveryClockBase+600)
	var attempts, canonical, reserved, rows int64
	if err := f.store.db.QueryRow(`SELECT attempts,canonical_bytes,reserved_logical_bytes FROM delivery_quota WHERE singleton=1`).Scan(&attempts, &canonical, &reserved); err != nil {
		t.Fatal(err)
	}
	if err := f.store.db.QueryRow(`SELECT count(*) FROM delivery_materializations`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.store.ExistingOwnedMaterialization(context.Background(), f.bearer, selection.SelectionID, "existing-miss"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing existing attempt: %v", err)
	}
	var afterAttempts, afterCanonical, afterReserved, afterRows int64
	if err := f.store.db.QueryRow(`SELECT attempts,canonical_bytes,reserved_logical_bytes FROM delivery_quota WHERE singleton=1`).Scan(&afterAttempts, &afterCanonical, &afterReserved); err != nil {
		t.Fatal(err)
	}
	if err := f.store.db.QueryRow(`SELECT count(*) FROM delivery_materializations`).Scan(&afterRows); err != nil {
		t.Fatal(err)
	}
	if attempts != afterAttempts || canonical != afterCanonical || reserved != afterReserved || rows != afterRows {
		t.Fatalf("existing lookup miss allocated attempt or quota: before=%d/%d/%d/%d after=%d/%d/%d/%d", attempts, canonical, reserved, rows, afterAttempts, afterCanonical, afterReserved, afterRows)
	}
	prepared, _, original := clockPrepared(t, f, selection.SelectionID)
	_ = prepared
	if _, err := f.store.db.Exec(`UPDATE delivery_quota SET attempts=4096 WHERE singleton=1`); err != nil {
		t.Fatal(err)
	}
	got, cleanup, err := f.store.ExistingOwnedMaterialization(context.Background(), f.bearer, selection.SelectionID, original.OperationID)
	if err != nil || cleanup == nil || got.ID != original.ID || got.State != "PREPARED" {
		t.Fatalf("existing attempt at admission limit: %+v cleanup=%v err=%v", got, cleanup != nil, err)
	}
	if _, _, err := f.store.ExistingOwnedMaterialization(context.Background(), f.bearer, selection.SelectionID, "different-operation"); !errors.Is(err, ErrConflict) {
		t.Fatalf("existing attempt rebound operation: %v", err)
	}
}
