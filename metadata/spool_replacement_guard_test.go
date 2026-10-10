package metadata

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func acceptedReplacementGuardFixture(t *testing.T) *replacementFixture {
	t.Helper()
	f := newReplacementFixture(t, false)
	if _, err := f.store.AcceptSpoolReplacement(f.w, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestAcceptedReplacementGuardsGenericSpoolMutations(t *testing.T) {
	f := acceptedReplacementGuardFixture(t)
	path := f.w.Destination.Path
	before, err := f.spool.Get(f.w.SourceRowID)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		call func() error
	}{
		{"exact-path", func() error { return f.spool.CheckReplacementUnder(path) }},
		{"ancestor-path", func() error { return f.spool.CheckReplacementUnder("a") }},
		{"mark-ready", func() error { return f.spool.MarkReady(before.ID, before.Size, before.SHA256) }},
		{"reopen-ready", func() error { _, err := f.spool.ReopenReady(before.ID); return err }},
		{"mark-draining", func() error { _, err := f.spool.MarkDraining(before.ID); return err }},
		{"mark-done", func() error { _, err := f.spool.MarkDone(before.ID); return err }},
		{"punch", func() error { return f.spool.SetPunchedEnd(before.ID, 1) }},
		{"cancel", func() error { _, err := f.spool.DeleteActiveByPath(path); return err }},
		{"cancel-ancestor", func() error { _, err := f.spool.DeleteActiveByPath("a"); return err }},
		{"move-exact", func() error { _, err := f.spool.MigrateActivePathsWithBase(path, "other.mov", nil); return err }},
		{"move-ancestor", func() error { _, err := f.spool.MigrateActivePathsWithBase("a", "other", nil); return err }},
		{"delete-id", func() error { return f.spool.Delete(before.ID) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, ErrMirrorConflict) {
				t.Fatalf("generic carrier mutation err=%v, want ErrMirrorConflict", err)
			}
			after, err := f.spool.Get(before.ID)
			if err != nil || !reflect.DeepEqual(after, before) {
				t.Fatalf("guard changed carrier before=%+v after=%+v err=%v", before, after, err)
			}
		})
	}
	for _, list := range []func(int) ([]*SpoolRow, error){f.spool.ListReady, f.spool.ListReadyIncludingWaiting} {
		rows, err := list(100)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if row.ID == before.ID {
				t.Fatalf("accepted carrier %d entered generic drain queue", before.ID)
			}
		}
	}
	if err := f.spool.CheckReplacementUnder("unrelated"); err != nil {
		t.Fatalf("unrelated path refused: %v", err)
	}
}

func TestAcceptedReplacementGuardBlocksMalformedWitnessAndDestinationMove(t *testing.T) {
	f := acceptedReplacementGuardFixture(t)
	ordinary, err := f.spool.Insert("work/ordinary.mov", filepath.Join(filepath.Dir(f.file), "ordinary.spool"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.spool.MigrateActivePathsWithBase("work/ordinary.mov", f.w.Destination.Path, nil); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("move into carrier destination err=%v", err)
	}
	ordinaryRow, err := f.spool.Get(ordinary)
	if err != nil || ordinaryRow.NFSPath != "work/ordinary.mov" {
		t.Fatalf("destination guard moved ordinary row %+v: %v", ordinaryRow, err)
	}
	if _, err := f.store.DB().Exec("UPDATE spool_entries SET replacement_witness=X'' WHERE id=?", f.w.SourceRowID); err != nil {
		t.Fatal(err)
	}
	if err := f.spool.CheckReplacementUnder("a"); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("malformed non-NULL carrier witness bypassed guard: %v", err)
	}
	if _, err := f.spool.DeleteActiveByPath(f.w.Destination.Path); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("malformed carrier was cancelled: %v", err)
	}
}

func TestReplacementGuardLeavesOrdinarySpoolPathsUsable(t *testing.T) {
	f := acceptedReplacementGuardFixture(t)
	plain, err := f.spool.Insert("work/plain.mov", filepath.Join(filepath.Dir(f.file), "plain.spool"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.spool.MarkReady(plain, 3, nil); err != nil {
		t.Fatal(err)
	}
	if err := f.spool.CheckReplacementUnder("work/plain.mov"); err != nil {
		t.Fatal(err)
	}
	if reopened, err := f.spool.ReopenReady(plain); err != nil || !reopened {
		t.Fatalf("ordinary reopen=%v err=%v", reopened, err)
	}
	if err := f.spool.SetPunchedEnd(plain, 1); err != nil {
		t.Fatal(err)
	}
	if rows, err := f.spool.DeleteActiveByPath("work/plain.mov"); err != nil || len(rows) != 1 || rows[0].ID != plain {
		t.Fatalf("ordinary cancellation rows=%+v err=%v", rows, err)
	}
	move, err := f.spool.Insert("work/move.mov", filepath.Join(filepath.Dir(f.file), "move.spool"))
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := f.spool.MigrateActivePathsWithBase("work/move.mov", "work/moved.mov", nil); err != nil || len(rows) != 1 || rows[0].OldID != move {
		t.Fatalf("ordinary migration rows=%+v err=%v", rows, err)
	}
	if err := f.spool.Delete(move); err != nil {
		t.Fatalf("ordinary row deletion: %v", err)
	}
	drain, err := f.spool.Insert("work/drain.mov", filepath.Join(filepath.Dir(f.file), "drain.spool"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.spool.MarkReady(drain, 4, nil); err != nil {
		t.Fatal(err)
	}
	ready, err := f.spool.ListReady(100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range ready {
		if row.ID == drain {
			found = true
		}
	}
	if !found {
		t.Fatal("ordinary ready row excluded from drain queue")
	}
	if claimed, err := f.spool.MarkDraining(drain); err != nil || !claimed {
		t.Fatalf("ordinary drain claim=%v err=%v", claimed, err)
	}
}

func TestDeleteDoneRetainsUnretiredReplacementWitness(t *testing.T) {
	f := acceptedReplacementGuardFixture(t)
	carrier := f.w.SourceRowID
	ordinary, err := f.spool.Insert("work/done.mov", filepath.Join(filepath.Dir(f.file), "done.spool"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.DB().Exec("UPDATE spool_entries SET drain_state='done' WHERE id IN (?,?)", carrier, ordinary); err != nil {
		t.Fatal(err)
	}
	if _, err := f.spool.DeleteDone(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.spool.Get(carrier); err != nil {
		t.Fatalf("unretired carrier witness was garbage-collected: %v", err)
	}
	if _, err := f.spool.Get(ordinary); err == nil {
		t.Fatal("ordinary done row was retained")
	}
}
