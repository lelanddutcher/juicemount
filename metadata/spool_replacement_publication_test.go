package metadata

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func acceptedPublicationFixture(t *testing.T) (*replacementFixture, *SpoolRow, *SpoolReplacementPublication) {
	t.Helper()
	f := newReplacementFixture(t, false)
	moved, err := f.store.AcceptSpoolReplacement(f.w, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	row, err := f.spool.Get(f.w.SourceRowID)
	if err != nil {
		t.Fatal(err)
	}
	p, err := f.spool.ReplacementPublication(row)
	if err != nil || p == nil || p.Expected != moved.Version || p.Phase != ReplacementPublicationQueued ||
		p.StagePath != "" || p.Revision != 1 || p.OperationID != f.w.OperationID {
		t.Fatalf("accepted publication=%+v err=%v", p, err)
	}
	return f, row, p
}

func TestReplacementPublicationAcceptedJournalSurvivesReopen(t *testing.T) {
	f, row, before := acceptedPublicationFixture(t)
	var required int
	if err := f.store.DB().QueryRow(`SELECT replacement_publication_required FROM spool_entries WHERE id=?`, row.ID).Scan(&required); err != nil || required != 1 {
		t.Fatalf("required marker=%d err=%v", required, err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := InitSpoolSchema(reopened.DB()); err != nil {
		t.Fatal(err)
	}
	spool := NewSpoolStore(reopened.DB())
	reopenedRow, err := spool.Get(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	after, err := spool.ReplacementPublication(reopenedRow)
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("reopened journal=%+v want=%+v err=%v", after, before, err)
	}
}

func TestReplacementPublicationSuccessorSupersedesQueuedAtomically(t *testing.T) {
	f := newReplacementFixture(t, false)
	w := successorReplacement(t, f)
	prior, err := f.spool.Get(w.PredecessorRowID)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := f.spool.ReplacementPublication(prior)
	if err != nil || queued == nil || queued.Phase != ReplacementPublicationQueued {
		t.Fatalf("predecessor queued=%+v err=%v", queued, err)
	}
	if _, err := f.store.AcceptSpoolReplacement(w, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	prior, err = f.spool.Get(w.PredecessorRowID)
	if err != nil || prior.RetiredBy != w.SourceRowID {
		t.Fatalf("prior retirement=%+v err=%v", prior, err)
	}
	terminal, err := f.spool.ReplacementPublication(prior)
	if err != nil || terminal == nil || terminal.Phase != ReplacementPublicationSuperseded ||
		terminal.Revision != 2 || terminal.StagePath != "" || terminal.OperationID != queued.OperationID {
		t.Fatalf("terminal journal=%+v err=%v", terminal, err)
	}
	newRow, err := f.spool.Get(w.SourceRowID)
	if err != nil {
		t.Fatal(err)
	}
	current, err := f.spool.ReplacementPublication(newRow)
	if err != nil || current == nil || current.Phase != ReplacementPublicationQueued || current.OperationID != w.OperationID {
		t.Fatalf("successor journal=%+v err=%v", current, err)
	}
}

func TestReplacementPublicationIntentBlocksSuccessorAndRawRetirement(t *testing.T) {
	f := newReplacementFixture(t, false)
	w := successorReplacement(t, f)
	prior, err := f.spool.Get(w.PredecessorRowID)
	if err != nil {
		t.Fatal(err)
	}
	checks := 0
	intent, err := f.store.BeginSpoolReplacementPublication(prior.ID, w.CurrentDestination, func() error { checks++; return nil })
	if err != nil || intent == nil || intent.Phase != ReplacementPublicationIntent || intent.Revision != 2 ||
		intent.StagePath != replacementStagePath(w.Destination.Path, intent.OperationID) || checks != 2 {
		t.Fatalf("begin intent=%+v checks=%d err=%v", intent, checks, err)
	}
	if same, err := f.store.BeginSpoolReplacementPublication(prior.ID, w.CurrentDestination, func() error { return nil }); err != nil || !reflect.DeepEqual(same, intent) {
		t.Fatalf("idempotent intent=%+v err=%v", same, err)
	}
	if moved, err := f.store.AcceptSpoolReplacement(w, func() error { return nil }); !errors.Is(err, ErrMirrorConflict) || moved != nil {
		t.Fatalf("successor retired intent: moved=%+v err=%v", moved, err)
	}
	for _, sql := range []string{`UPDATE spool_entries SET retired_by=999 WHERE id=?`, `DELETE FROM spool_entries WHERE id=?`} {
		if _, err := f.store.DB().Exec(sql, prior.ID); err == nil {
			t.Fatalf("raw SQL bypassed intent guard: %s", sql)
		}
	}
	retained, err := f.spool.Get(prior.ID)
	if err != nil || retained.RetiredBy != 0 {
		t.Fatalf("intent carrier changed=%+v err=%v", retained, err)
	}
	got, err := f.spool.ReplacementPublication(retained)
	if err != nil || !reflect.DeepEqual(got, intent) {
		t.Fatalf("intent journal changed=%+v err=%v", got, err)
	}
}

func TestReplacementPublicationReaderRejectsCorruption(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"missing-required", `DELETE FROM spool_replacement_publications WHERE spool_id=?`},
		{"payload", `UPDATE spool_replacement_publications SET payload=X'7B626164' WHERE spool_id=?`},
		{"phase-column", `UPDATE spool_replacement_publications SET phase='intent' WHERE spool_id=?`},
		{"marker", `UPDATE spool_entries SET replacement_publication_required=0 WHERE id=?`},
		{"noncanonical", `UPDATE spool_replacement_publications SET payload=CAST(payload AS TEXT)||' ' WHERE spool_id=?`},
		{"operation-column", `UPDATE spool_replacement_publications SET operation_id='other' WHERE spool_id=?`},
		{"witness", `UPDATE spool_entries SET replacement_witness=X'00' WHERE id=?`},
		{"row-size", `UPDATE spool_entries SET size=size+1 WHERE id=?`},
		{"row-epoch", `UPDATE spool_entries SET accepted_epoch=accepted_epoch+1 WHERE id=?`},
		{"mirror-revision", `UPDATE entries SET revision=revision+1 WHERE path=(SELECT nfs_path FROM spool_entries WHERE id=?)`},
		{"mirror-size", `UPDATE entries SET size=size+1 WHERE path=(SELECT nfs_path FROM spool_entries WHERE id=?)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, row, _ := acceptedPublicationFixture(t)
			if _, err := f.store.DB().Exec(tc.sql, row.ID); err != nil {
				t.Fatal(err)
			}
			fresh, err := f.spool.Get(row.ID)
			if err != nil {
				t.Fatal(err)
			}
			if p, err := f.spool.ReplacementPublication(fresh); !errors.Is(err, ErrMirrorConflict) || p != nil {
				t.Fatalf("corrupt journal accepted=%+v err=%v", p, err)
			}
		})
	}
}

func TestReplacementPublicationInsertAbortOrIgnoreRollsBackAcceptance(t *testing.T) {
	for _, action := range []string{"ABORT", "IGNORE"} {
		t.Run(action, func(t *testing.T) {
			f := newReplacementFixture(t, false)
			before := f.sqlState(t)
			sourceRAM, destRAM := f.store.LookupByPath(f.w.Source.Path), f.store.LookupByPath(f.w.Destination.Path)
			raise := "RAISE(IGNORE)"
			if action == "ABORT" {
				raise = "RAISE(ABORT,'journal')"
			}
			if _, err := f.store.DB().Exec(`CREATE TRIGGER reject_publication BEFORE INSERT ON spool_replacement_publications
 BEGIN SELECT ` + raise + `; END`); err != nil {
				t.Fatal(err)
			}
			if moved, err := f.store.AcceptSpoolReplacement(f.w, func() error { return nil }); err == nil || moved != nil {
				t.Fatalf("journal insertion fault accepted moved=%+v err=%v", moved, err)
			}
			f.assertUntouched(t, before, sourceRAM, destRAM)
			var count int
			if err := f.store.DB().QueryRow(`SELECT count(*) FROM spool_replacement_publications`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("journal partial insert count=%d err=%v", count, err)
			}
		})
	}
}

func TestReplacementPublicationIntentCASAbortIgnoreAndRewind(t *testing.T) {
	for _, action := range []string{"ABORT", "IGNORE", "REWIND"} {
		t.Run(action, func(t *testing.T) {
			f, row, before := acceptedPublicationFixture(t)
			var trigger string
			if action == "REWIND" {
				trigger = `CREATE TRIGGER rewind_publication AFTER UPDATE OF phase ON spool_replacement_publications
 WHEN NEW.phase='intent' BEGIN UPDATE spool_replacement_publications SET phase='queued',payload=OLD.payload WHERE spool_id=OLD.spool_id; END`
			} else {
				raise := "RAISE(IGNORE)"
				if action == "ABORT" {
					raise = "RAISE(ABORT,'intent')"
				}
				trigger = `CREATE TRIGGER reject_publication BEFORE UPDATE OF phase ON spool_replacement_publications
 BEGIN SELECT ` + raise + `; END`
			}
			if _, err := f.store.DB().Exec(trigger); err != nil {
				t.Fatal(err)
			}
			if intent, err := f.store.BeginSpoolReplacementPublication(row.ID, before.Expected, func() error { return nil }); err == nil || intent != nil {
				t.Fatalf("CAS fault accepted intent=%+v err=%v", intent, err)
			}
			after, err := f.spool.ReplacementPublication(row)
			if err != nil || !reflect.DeepEqual(after, before) {
				t.Fatalf("failed CAS changed journal=%+v want=%+v err=%v", after, before, err)
			}
		})
	}
}

func TestReplacementPublicationLegacySuccessorUpgradesAndSupersedes(t *testing.T) {
	f := newReplacementFixture(t, false)
	w := successorReplacement(t, f)
	if _, err := f.store.DB().Exec(`DELETE FROM spool_replacement_publications WHERE spool_id=?`, w.PredecessorRowID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.DB().Exec(`UPDATE spool_entries SET replacement_publication_required=0 WHERE id=?`, w.PredecessorRowID); err != nil {
		t.Fatal(err)
	}
	prior, err := f.spool.Get(w.PredecessorRowID)
	if err != nil {
		t.Fatal(err)
	}
	if p, err := f.spool.ReplacementPublication(prior); err != nil || p != nil {
		t.Fatalf("legacy prior journal=%+v err=%v", p, err)
	}
	if _, err := f.store.AcceptSpoolReplacement(w, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	prior, err = f.spool.Get(w.PredecessorRowID)
	if err != nil {
		t.Fatal(err)
	}
	p, err := f.spool.ReplacementPublication(prior)
	if err != nil || p == nil || p.Phase != ReplacementPublicationSuperseded || prior.RetiredBy != w.SourceRowID {
		t.Fatalf("upgraded legacy terminal=%+v row=%+v err=%v", p, prior, err)
	}
}

func TestReplacementPublicationCanonicalPayload(t *testing.T) {
	f, row, before := acceptedPublicationFixture(t)
	var payload []byte
	if err := f.store.DB().QueryRow(`SELECT payload FROM spool_replacement_publications WHERE spool_id=?`, row.ID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if len(payload) == 0 || !bytes.Equal(bytes.TrimSpace(payload), payload) {
		t.Fatal("noncanonical publication payload")
	}
	if before.StagePath != "" || before.Phase != ReplacementPublicationQueued {
		t.Fatal("accepted publication started a remote stage")
	}
}
