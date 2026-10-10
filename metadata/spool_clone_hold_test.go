package metadata

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func openCloneHoldStore(t *testing.T) *SpoolStore {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "spool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := InitSpoolSchema(db); err != nil {
		t.Fatal(err)
	}
	return NewSpoolStore(db)
}

// A held row is never listed for dispatch or claimed, survives every state
// reset and a rename requeue, and still counts as pending.
func TestSpoolCloneHoldBlocksDispatchAndCountsPending(t *testing.T) {
	s := openCloneHoldStore(t)
	id, err := s.Insert("/proj/a.mov", "/spool/a")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MarkReady(id, 5, []byte{1}); err != nil {
		t.Fatal(err)
	}
	if n, err := s.HoldRowsCopiedFromOtherMac(); err != nil || n != 1 {
		t.Fatalf("hold = %d, %v", n, err)
	}
	for _, include := range []bool{false, true} {
		rows, err := s.listReady(10, include)
		if err != nil || len(rows) != 0 {
			t.Fatalf("held row listed for dispatch (includeWaiting=%v): %d %v", include, len(rows), err)
		}
	}
	if ok, err := s.MarkDraining(id); err != nil || ok {
		t.Fatalf("held row claimed: %v %v", ok, err)
	}
	if files, bytes, _ := s.PendingStats(); files != 1 || bytes != 5 {
		t.Fatalf("held row not pending: %d files %d bytes", files, bytes)
	}
	// Automatic failed->ready retry keeps the hold.
	if _, err := s.MarkFailed(id, "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResetForRetry(id); err != nil {
		t.Fatal(err)
	}
	if h, _ := s.CloneHold(id); h != SpoolCloneHoldReason {
		t.Fatalf("retry cleared the hold: %q", h)
	}
	// A draining row moved by a folder rename is requeued under a new ID;
	// the hold follows it.
	if _, err := s.db.Exec(`UPDATE spool_entries SET drain_state='draining' WHERE id=?`, id); err != nil {
		t.Fatal(err)
	}
	m, err := s.MigrateActivePaths("/proj", "/proj2")
	if err != nil || len(m) != 1 || !m[0].Requeued {
		t.Fatalf("migrate = %+v, %v", m, err)
	}
	if h, _ := s.CloneHold(m[0].NewID); h != SpoolCloneHoldReason {
		t.Fatalf("rename requeue dropped the hold: %q", h)
	}
	if rows, _ := s.ListReady(10); len(rows) != 0 {
		t.Fatal("requeued held row listed for dispatch")
	}
	// Release: dispatchable again.
	if n, err := s.ReleaseCloneHeld(); err != nil || n != 1 {
		t.Fatalf("release = %d, %v", n, err)
	}
	if rows, _ := s.ListReady(10); len(rows) != 1 {
		t.Fatalf("released row not dispatchable: %d", len(rows))
	}
}

// Done rows (already published by the other Mac) are not held.
func TestSpoolCloneHoldSkipsDoneRows(t *testing.T) {
	s := openCloneHoldStore(t)
	id, _ := s.Insert("/proj/done.mov", "/spool/done")
	if _, err := s.MarkDone(id); err != nil {
		t.Fatal(err)
	}
	if n, err := s.HoldRowsCopiedFromOtherMac(); err != nil || n != 0 {
		t.Fatalf("done row held: %d %v", n, err)
	}
}

// The punched/streaming custody path is the other way a row reaches
// draining. A row held as copied from another Mac is never claimed or
// published by it, and never begins a new custody.
func TestSpoolCloneHoldBlocksPunchedCustodyClaim(t *testing.T) {
	s, sp, row, v, c := custodyFixture(t, 4096)
	var err error
	c, err = s.BindSpoolStage(c, c.SourceIdentity, "stage-identity")
	if err != nil {
		t.Fatal(err)
	}
	c, err = s.PrepareSpoolCheckpoint(c, c.SourceIdentity, c.StageIdentity, 4096, 4096, 4096, 2048)
	if err != nil {
		t.Fatal(err)
	}
	if err := sp.SetPunchedEnd(row.ID, 2048); err != nil {
		t.Fatal(err)
	}
	c, err = s.ConfirmSpoolCheckpoint(c, c.SourceIdentity, c.StageIdentity)
	if err != nil {
		t.Fatal(err)
	}
	// Boot recovery returns a copied draining row to ready, then it is held.
	if err := sp.ResetToReady(row.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := sp.HoldRowsCopiedFromOtherMac(); err != nil || n != 1 {
		t.Fatalf("hold = %d, %v", n, err)
	}
	if _, err := s.ClaimPunchedSpool(c, v); err == nil {
		t.Fatal("H8: punched custody claimed a row copied from another Mac")
	}
	if got, _ := sp.Get(row.ID); got == nil || got.DrainState != DrainReady {
		t.Fatalf("held row changed state: %+v", got)
	}
	if _, err := s.BeginCustodyPublish(c, v, c.SourceIdentity, c.StageIdentity); err == nil {
		t.Fatal("H8: custody publish accepted a held row")
	}
	// After the user's upload decision the same claim succeeds.
	if _, err := sp.ReleaseCloneHeld(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimPunchedSpool(c, v); err != nil {
		t.Fatalf("released row not claimable: %v", err)
	}
}

func TestSpoolCloneHoldBlocksNewCustody(t *testing.T) {
	s, sp, row, v := boundSpoolFixture(t, 4096)
	if _, err := sp.HoldRowsCopiedFromOtherMac(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginSpoolCustody(row, v, custodyOperationID, custodySourceID,
		"source-dev=2,inode=3", filepath.Join(t.TempDir(), "stage-"+custodyOperationID)); err == nil {
		t.Fatal("H8: custody began on a row copied from another Mac")
	}
}
