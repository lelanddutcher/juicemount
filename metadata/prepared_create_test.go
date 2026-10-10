package metadata

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func openPreparedCreateTestStore(t *testing.T) (*Store, string, func()) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "prepared-create.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = store.Close()
		}
	})
	closeStore := func() {
		t.Helper()
		if closed {
			return
		}
		if err := store.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		closed = true
	}
	return store, dbPath, closeStore
}

func prepareAbsentTestPath(t *testing.T, store *Store, name string) PreparedCreate {
	t.Helper()
	token, err := store.CaptureMirror(name)
	if err != nil {
		t.Fatalf("CaptureMirror(%q): %v", name, err)
	}
	prepared, err := store.PrepareCreate(token, "test-mount-generation")
	if err != nil {
		t.Fatalf("PrepareCreate(%q): %v", name, err)
	}
	return prepared
}

func TestPreparedCreatePendingIntentRefusesReopen(t *testing.T) {
	store, dbPath, closeStore := openPreparedCreateTestStore(t)
	prepared := prepareAbsentTestPath(t, store, "new.mov")
	closeStore()
	if reopened, err := Open(dbPath); reopened != nil || err == nil || !strings.Contains(err.Error(), prepared.Path) {
		if reopened != nil {
			_ = reopened.Close()
		}
		t.Fatalf("reopen with prepared intent = (%v, %v), want path-specific refusal", reopened, err)
	}
}

func TestPreparedCreateReservationRejectsOrdinaryWriters(t *testing.T) {
	store, _, _ := openPreparedCreateTestStore(t)
	prepareAbsentTestPath(t, store, "reserved.mov")

	e := MakeEntry("reserved.mov", false, 7, time.Now(), 301)
	if err := store.Insert(e); err == nil {
		t.Fatal("ordinary Insert crossed a prepared CREATE reservation")
	}
	store.InsertToCache(e)
	if got := store.LookupByPath(e.Path); got != nil {
		t.Fatalf("InsertToCache exposed reserved name: %+v", got)
	}
	if got := store.lookupByPathSQLite(e.Path); got != nil {
		t.Fatalf("failed ordinary Insert persisted reserved name: %+v", got)
	}
	pending, err := store.InspectPreparedCreates()
	if err != nil || len(pending) != 1 || pending[0].Path != e.Path {
		t.Fatalf("reservation after rejected writers = (%+v, %v), want one pending intent", pending, err)
	}
}

func TestPreparedCreateIgnoredInsertDoesNotPublishReservation(t *testing.T) {
	store, _, _ := openPreparedCreateTestStore(t)
	if _, err := store.DB().Exec(`CREATE TRIGGER ignore_prepared_create BEFORE INSERT ON prepared_creates
BEGIN SELECT RAISE(IGNORE); END`); err != nil {
		t.Fatal(err)
	}
	token, err := store.CaptureMirror("ignored.mov")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PrepareCreate(token, "test-mount-generation"); err == nil {
		t.Fatal("PrepareCreate accepted trigger-ignored INSERT")
	}
	if pending, err := store.InspectPreparedCreates(); err != nil || len(pending) != 0 {
		t.Fatalf("ignored INSERT persisted an intent: (%+v, %v)", pending, err)
	}
	e := MakeEntry("ignored.mov", false, 1, time.Now(), 310)
	if err := store.Insert(e); err != nil {
		t.Fatalf("ignored intent left a phantom RAM/SQL reservation: %v", err)
	}
}

func TestPreparedCreateFailedCompleteRetainsIntentAndRefusesReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "prepared-create.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = store.Close()
		}
	})
	prepared := prepareAbsentTestPath(t, store, "new.mov")
	if _, err := store.DB().Exec(`CREATE TRIGGER fail_prepared_create_complete BEFORE INSERT ON entries
BEGIN SELECT RAISE(FAIL, 'injected prepared-create completion failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompletePreparedCreate(prepared, MakeEntry(prepared.Path, false, 0, time.Now(), 302)); err == nil {
		t.Fatal("CompletePreparedCreate acknowledged injected SQLite failure")
	}
	if got := store.LookupByPath(prepared.Path); got != nil {
		t.Fatalf("failed completion published RAM entry: %+v", got)
	}
	pending, err := store.InspectPreparedCreates()
	if err != nil || len(pending) != 1 || pending[0] != prepared {
		t.Fatalf("failed completion intent = (%+v, %v), want original intent", pending, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	if reopened, err := Open(dbPath); reopened != nil || err == nil || !strings.Contains(err.Error(), prepared.Path) {
		if reopened != nil {
			_ = reopened.Close()
		}
		t.Fatalf("reopen after failed completion = (%v, %v), want path-specific refusal", reopened, err)
	}
}

func TestPreparedCreateAbortRequiresMatchingOperation(t *testing.T) {
	store, _, _ := openPreparedCreateTestStore(t)
	prepared := prepareAbsentTestPath(t, store, "owned.mov")
	wrong := prepared
	wrong.OpID = "00000000000000000000000000000000"
	if err := store.AbortPreparedCreate(wrong); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("AbortPreparedCreate with wrong operation = %v, want mirror conflict", err)
	}
	pending, err := store.InspectPreparedCreates()
	if err != nil || len(pending) != 1 || pending[0] != prepared {
		t.Fatalf("wrong-operation abort changed reservation: (%+v, %v)", pending, err)
	}
	if err := store.AbortPreparedCreate(prepared); err != nil {
		t.Fatalf("AbortPreparedCreate matching operation: %v", err)
	}
	if pending, err := store.InspectPreparedCreates(); err != nil || len(pending) != 0 {
		t.Fatalf("matching abort left pending intents: (%+v, %v)", pending, err)
	}
}

func TestPreparedCreateCompleteRetiresIntentAndSurvivesReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "prepared-create.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = store.Close()
		}
	})
	prepared := prepareAbsentTestPath(t, store, "new.mov")
	completed, err := store.CompletePreparedCreate(prepared, MakeEntry(prepared.Path, false, 0, time.Now(), 303))
	if err != nil {
		t.Fatalf("CompletePreparedCreate: %v", err)
	}
	if completed == nil || completed.Version.IsZero() {
		t.Fatalf("completed row has no durable mirror version: %+v", completed)
	}
	if pending, err := store.InspectPreparedCreates(); err != nil || len(pending) != 0 {
		t.Fatalf("successful completion left intent: (%+v, %v)", pending, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true

	reopened, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen after completed CREATE: %v", err)
	}
	defer reopened.Close()
	if got := reopened.LookupByPath(prepared.Path); got == nil || got.Version != completed.Version {
		t.Fatalf("reopened entry = %+v, want completed version %+v", got, completed.Version)
	}
	if pending, err := reopened.InspectPreparedCreates(); err != nil || len(pending) != 0 {
		t.Fatalf("reopen has pending intents: (%+v, %v)", pending, err)
	}
}

func TestPreparedCreateDisjointCompletionsCanRace(t *testing.T) {
	store, _, _ := openPreparedCreateTestStore(t)
	left := prepareAbsentTestPath(t, store, "left.mov")
	right := prepareAbsentTestPath(t, store, "right.mov")
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i, prepared := range []PreparedCreate{left, right} {
		wg.Add(1)
		go func(inode uint64, prepared PreparedCreate) {
			defer wg.Done()
			<-start
			_, err := store.CompletePreparedCreate(prepared,
				MakeEntry(prepared.Path, false, 0, time.Now(), inode))
			errs <- err
		}(uint64(320+i), prepared)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("disjoint completion conflicted: %v", err)
		}
	}
	for _, name := range []string{left.Path, right.Path} {
		if store.LookupByPath(name) == nil {
			t.Errorf("successful disjoint completion missing %q", name)
		}
	}
}
