package metadata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestReplacementDispositionRowUpdateFailureRollsBackReceipt(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "first-insert"
		if existing {
			name = "waiting-to-sticky"
		}
		t.Run(name, func(t *testing.T) {
			f, row, queued := acceptedPublicationFixture(t)
			intent, err := f.store.BeginSpoolReplacementPublication(row.ID, queued.Expected, func() error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			if existing {
				if err := f.spool.RecordReplacementDisposition(*intent, ReplacementWaitingWriter, "mac-peer@peer-host", 20, "a"); err != nil {
					t.Fatal(err)
				}
			}
			beforeRow, err := f.spool.Get(row.ID)
			if err != nil {
				t.Fatal(err)
			}
			beforeJournal, err := f.spool.ReplacementPublication(beforeRow)
			if err != nil {
				t.Fatal(err)
			}
			beforeDisposition, err := f.spool.ReplacementDisposition(row.ID)
			if err != nil {
				t.Fatal(err)
			}
			beforeWait, err := f.spool.WaitProject(row.ID)
			if err != nil {
				t.Fatal(err)
			}
			beforeImage, err := os.ReadFile(f.file)
			if err != nil {
				t.Fatal(err)
			}
			// The INSERT/UPSERT of the disposition has succeeded within the
			// transaction when this later row UPDATE invokes the test trigger.
			trigger := fmt.Sprintf(`CREATE TRIGGER disposition_row_update_fault
				AFTER UPDATE OF wait_project,last_error ON spool_entries
				WHEN NEW.id=%d BEGIN SELECT RAISE(ABORT,'disposition row update fault'); END`, row.ID)
			if _, err := f.store.DB().Exec(trigger); err != nil {
				t.Fatal(err)
			}
			state, epoch, project := ReplacementWaitingWriter, uint64(21), "a"
			if existing {
				state, epoch, project = ReplacementWriterChanged, 22, ""
			}
			if err := f.spool.RecordReplacementDisposition(*intent, state, "mac-peer@peer-host", epoch, project); err == nil || !strings.Contains(err.Error(), "disposition row update fault") {
				t.Fatalf("did not reach faulted row update: %v", err)
			}
			afterDisposition, err := f.spool.ReplacementDisposition(row.ID)
			if err != nil || !reflect.DeepEqual(afterDisposition, beforeDisposition) {
				t.Fatalf("disposition was not rolled back: %+v -> %+v, %v", beforeDisposition, afterDisposition, err)
			}
			afterRow, err := f.spool.Get(row.ID)
			if err != nil || !reflect.DeepEqual(afterRow, beforeRow) {
				t.Fatalf("spool row changed on rollback: %+v -> %+v, %v", beforeRow, afterRow, err)
			}
			afterWait, err := f.spool.WaitProject(row.ID)
			if err != nil || afterWait != beforeWait {
				t.Fatalf("schedule wait changed: %q -> %q, %v", beforeWait, afterWait, err)
			}
			afterJournal, err := f.spool.ReplacementPublication(afterRow)
			if err != nil || !reflect.DeepEqual(afterJournal, beforeJournal) {
				t.Fatalf("journal changed on rollback: %+v -> %+v, %v", beforeJournal, afterJournal, err)
			}
			afterImage, err := os.ReadFile(f.file)
			if err != nil || !bytes.Equal(afterImage, beforeImage) {
				t.Fatalf("accepted image changed: %v", err)
			}
		})
	}
}

func TestReplacementDispositionRejectsBlankPrincipal(t *testing.T) {
	f, row, queued := acceptedPublicationFixture(t)
	intent, err := f.store.BeginSpoolReplacementPublication(row.ID, queued.Expected, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	for _, principal := range []string{"", " ", "\t", " \n "} {
		if err := f.spool.RecordReplacementDisposition(*intent, ReplacementWaitingWriter, principal, 20, "a"); !errors.Is(err, ErrMirrorConflict) {
			t.Fatalf("accepted blank principal %q: %v", principal, err)
		}
		if d, err := f.spool.ReplacementDisposition(row.ID); err != nil || d != nil {
			t.Fatalf("blank principal persisted %q: %+v %v", principal, d, err)
		}
	}
}

func TestReplacementDispositionCASAndRestart(t *testing.T) {
	f, row, queued := acceptedPublicationFixture(t)
	if err := f.spool.RecordReplacementDisposition(*queued, ReplacementWaitingWriter, "mac-peer@peer-host", 20, "a"); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("unstarted journal marked refused: %v", err)
	}
	intent, err := f.store.BeginSpoolReplacementPublication(row.ID, queued.Expected, func() error { return nil })
	if err != nil || intent == nil || intent.Phase != ReplacementPublicationIntent {
		t.Fatalf("intent: %+v %v", intent, err)
	}
	before, err := f.spool.Get(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.spool.RecordReplacementDisposition(*intent, ReplacementWaitingWriter, "mac-peer@peer-host", 20, "a"); err != nil {
		t.Fatal(err)
	}
	waiting, err := f.spool.ReplacementDisposition(row.ID)
	if err != nil || waiting == nil || waiting.State != ReplacementWaitingWriter || waiting.ObservedEpoch != 20 || waiting.OperationID != intent.OperationID || waiting.JournalRevision != intent.Revision {
		t.Fatalf("waiting receipt: %+v %v", waiting, err)
	}
	if got, err := f.spool.ReplacementDispatchCandidate(context.Background(), row.ID); got != nil || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("waiting dispatched: %+v %v", got, err)
	}
	after, err := f.spool.Get(row.ID)
	if err != nil || after.DrainState != DrainReady || after.Size != before.Size || after.SpoolFile != before.SpoolFile || !reflect.DeepEqual(after.SHA256, before.SHA256) || after.DrainAttempts != before.DrainAttempts || after.LastError == "" {
		t.Fatalf("accepted image changed: %+v %v", after, err)
	}
	for _, stale := range []SpoolReplacementPublication{
		func() SpoolReplacementPublication { p := *intent; p.OperationID = "wrong-operation"; return p }(),
		func() SpoolReplacementPublication { p := *intent; p.WitnessSHA256 = "wrong-witness"; return p }(),
		func() SpoolReplacementPublication { p := *intent; p.Revision++; return p }(),
	} {
		if err := f.spool.RecordReplacementDisposition(stale, ReplacementWriterChanged, "", 22, ""); !errors.Is(err, ErrMirrorConflict) {
			t.Fatalf("stale CAS accepted: %v", err)
		}
	}
	if d, err := f.spool.ReplacementDisposition(row.ID); err != nil || !reflect.DeepEqual(d, waiting) {
		t.Fatalf("stale CAS changed receipt: %+v %v", d, err)
	}
	if err := f.spool.RecordReplacementDisposition(*intent, ReplacementWriterChanged, "mac-peer@peer-host", 19, ""); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("accepted-epoch conflict accepted: %v", err)
	}
	if n, err := f.spool.ReleaseProjectHold("a"); err != nil || n != 1 {
		t.Fatalf("release scheduling wait: %d %v", n, err)
	}
	if got, err := f.spool.ReplacementDispatchCandidate(context.Background(), row.ID); err != nil || got == nil || *got != *intent {
		t.Fatalf("waiting reevaluation: %+v %v", got, err)
	}
	if err := f.spool.RecordReplacementDisposition(*intent, ReplacementWriterChanged, "mac-peer@peer-host", 22, ""); err != nil {
		t.Fatal(err)
	}
	sticky, err := f.spool.ReplacementDisposition(row.ID)
	if err != nil || sticky == nil || sticky.State != ReplacementWriterChanged || sticky.ObservedEpoch != 22 {
		t.Fatalf("sticky receipt: %+v %v", sticky, err)
	}
	if n, err := f.spool.ReleaseProjectHold("a"); err != nil || n != 0 {
		t.Fatalf("sticky schedule release: %d %v", n, err)
	}
	if got, err := f.spool.ReplacementDispatchCandidate(context.Background(), row.ID); got != nil || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("sticky dispatched: %+v %v", got, err)
	}
	if err := f.spool.RecordReplacementDisposition(*intent, ReplacementWaitingWriter, "mac-peer@peer-host", 23, "a"); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("sticky downgraded: %v", err)
	}
	for _, attempt := range []func() error{
		func() error { _, e := f.spool.MarkFailed(row.ID, "generic failure"); return e },
		func() error { _, e := f.spool.ResetForRetry(row.ID); return e },
		func() error { return f.spool.IncrementAttempts(row.ID, "retry") },
		func() error { return f.spool.ResetToReady(row.ID) },
		func() error {
			_, e := f.spool.HoldUnqualified(row.ID, row.NFSPath, row.SpoolFile, "unqualified")
			return e
		},
	} {
		if err := attempt(); !errors.Is(err, ErrMirrorConflict) {
			t.Fatalf("generic path changed held replacement: %v", err)
		}
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if err := InitSpoolSchema(reopened.DB()); err != nil {
		t.Fatal(err)
	}
	again := NewSpoolStore(reopened.DB())
	reopened.SetSpoolStore(again)
	if d, err := again.ReplacementDisposition(row.ID); err != nil || !reflect.DeepEqual(d, sticky) {
		t.Fatalf("restart lost sticky refusal: %+v %v", d, err)
	}
	if got, err := again.ReplacementDispatchCandidate(context.Background(), row.ID); got != nil || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("restart dispatched sticky: %+v %v", got, err)
	}
	statusRows, err := again.ListForStatus(time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	statuses, err := again.ReplacementDispositionsForStatus(statusRows)
	if err != nil || statuses[row.ID] == nil || statuses[row.ID].State != ReplacementWriterChanged {
		t.Fatalf("restart status: %+v %v", statuses, err)
	}
}

func TestReplacementDispositionGenericGuardsLeaveOrdinaryRowsAlone(t *testing.T) {
	f, row, _ := acceptedPublicationFixture(t)
	for _, attempt := range []func() error{
		func() error { _, e := f.spool.MarkFailed(row.ID, "generic failure"); return e },
		func() error { _, e := f.spool.ResetForRetry(row.ID); return e },
		func() error { _, e := f.spool.ResumeFailedPaused(row.ID); return e },
		func() error { return f.spool.IncrementAttempts(row.ID, "generic retry") },
		func() error { return f.spool.ResetToReady(row.ID) },
		func() error {
			_, e := f.spool.HoldUnqualified(row.ID, row.NFSPath, row.SpoolFile, "generic hold")
			return e
		},
	} {
		if err := attempt(); !errors.Is(err, ErrMirrorConflict) {
			t.Fatalf("accepted witness crossed generic path: %v", err)
		}
	}
	id, err := f.spool.Insert("other/file", filepath.Join(t.TempDir(), "ordinary"))
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := f.spool.MarkFailed(id, "ordinary failure"); err != nil || !ok {
		t.Fatalf("ordinary failure: %t %v", ok, err)
	}
	if ok, err := f.spool.ResetForRetry(id); err != nil || !ok {
		t.Fatalf("ordinary retry: %t %v", ok, err)
	}
	if err := f.spool.IncrementAttempts(id, "ordinary retry"); err != nil {
		t.Fatal(err)
	}
	if err := f.spool.ResetToReady(id); err != nil {
		t.Fatal(err)
	}
	if ok, err := f.spool.HoldUnqualified(id, "other/file", filepath.Join(t.TempDir(), "wrong-file"), "bad bytes"); err != nil || ok {
		t.Fatalf("ordinary nonmatch hold: %t %v", ok, err)
	}
}
