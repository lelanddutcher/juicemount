package metadata

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func boundSpoolFixture(t *testing.T, size int64) (*Store, *SpoolStore, *SpoolRow, MirrorToken) {
	t.Helper()
	s, sp := openStoreWithSpool(t)
	if err := s.Insert(MakeEntry("reel.mov", false, 128, time.Unix(1700000000, 0), 901)); err != nil {
		t.Fatal(err)
	}
	v, err := s.CaptureMirror("reel.mov")
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.InsertBoundSpool(v, filepath.Join(t.TempDir(), "spool"))
	if err != nil {
		t.Fatal(err)
	}
	if err := sp.MarkReady(id, size, nil); err != nil {
		t.Fatal(err)
	}
	if ok, err := sp.MarkDraining(id); err != nil || !ok {
		t.Fatalf("claim %v %v", ok, err)
	}
	r, err := sp.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	return s, sp, r, v
}

func TestSpoolBindingCompleteZeroAndLegacyFence(t *testing.T) {
	s, sp, row, v := boundSpoolFixture(t, 0)
	if ok, err := sp.MarkDone(row.ID); ok || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("legacy done accepted: %v %v", ok, err)
	}
	if _, err := s.BatchDrainComplete([]DrainCommitItem{{SpoolID: row.ID, NFSPath: row.NFSPath, Size: 999, Mtime: time.Now()}}); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("legacy batch accepted: %v", err)
	}
	if e := s.LookupByPath(row.NFSPath); e.Size != 128 {
		t.Fatalf("legacy batch mutated mirror: %+v", e)
	}
	got, err := s.CaptureSpool(row)
	if err != nil || got != v {
		t.Fatalf("capture %v %v", got, err)
	}
	if err := s.BeginSpoolPublish(row, v, "/owned/stage"); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteSpool(row, v, time.Unix(1700000010, 0)); err != nil {
		t.Fatal(err)
	}
	if e := s.LookupByPath(row.NFSPath); e.Size != 0 || e.Version.Incarnation != v.Incarnation {
		t.Fatalf("bad completion %+v", e)
	}
	if got, err := sp.Get(row.ID); err != nil || got.DrainState != DrainDone {
		t.Fatalf("row %+v %v", got, err)
	}
	if err := s.CheckSpoolRecovery(); err != nil {
		t.Fatal(err)
	}
}

func TestSpoolBindingCanceledReplacementPreserved(t *testing.T) {
	s, sp, row, v := boundSpoolFixture(t, 4096)
	if err := s.BeginSpoolPublish(row, v, "/owned/stage"); err != nil {
		t.Fatal(err)
	}
	if _, err := sp.DeleteActiveByPath(row.NFSPath); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(row.NFSPath); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(MakeEntry(row.NFSPath, false, 64, time.Now(), 902)); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteSpool(row, v, time.Now()); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("accepted stale row: %v", err)
	}
	if got := s.LookupByPath(row.NFSPath); got.Size != 64 {
		t.Fatalf("replacement clobbered: %+v", got)
	}
	var pending *PendingSpoolPublication
	if err := s.CheckSpoolRecovery(); !errors.As(err, &pending) || len(pending.Paths) != 1 || pending.Paths[0] != row.NFSPath {
		t.Fatalf("lost marker: %v", err)
	}
}

func TestSpoolBindingSQLFailurePreservesBothRowsAndRAM(t *testing.T) {
	for _, action := range []string{"FAIL", "IGNORE"} {
		t.Run(action, func(t *testing.T) {
			s, sp, row, v := boundSpoolFixture(t, 4096)
			if err := s.BeginSpoolPublish(row, v, "/owned/stage"); err != nil {
				t.Fatal(err)
			}
			body := "RAISE(IGNORE)"
			if action == "FAIL" {
				body = "RAISE(FAIL,'injected')"
			}
			if _, err := s.DB().Exec(`CREATE TRIGGER fail_spool_size BEFORE UPDATE OF size ON entries BEGIN SELECT ` + body + `; END`); err != nil {
				t.Fatal(err)
			}
			if err := s.CompleteSpool(row, v, time.Now()); err == nil {
				t.Fatal("SQL failure accepted")
			}
			if e := s.LookupByPath(row.NFSPath); e.Size != 128 || e.Version != v {
				t.Fatalf("RAM changed: %+v", e)
			}
			if r, err := sp.Get(row.ID); err != nil || r.DrainState != DrainDraining {
				t.Fatalf("spool retired: %+v %v", r, err)
			}
			var phase string
			if err := s.DB().QueryRow(`SELECT phase FROM spool_bindings WHERE spool_id=?`, row.ID).Scan(&phase); err != nil || phase != "publishing" {
				t.Fatalf("marker %q %v", phase, err)
			}
		})
	}
}

func TestSpoolBindingRenameTransfersBoundCustody(t *testing.T) {
	s, sp, row, _ := boundSpoolFixture(t, 4096)
	source, dest, err := s.CaptureRename(row.NFSPath, "moved.mov")
	if err != nil {
		t.Fatal(err)
	}
	moved, err := sp.MigrateActivePaths(row.NFSPath, "moved.mov")
	if err != nil || len(moved) != 1 {
		t.Fatalf("migrate %v %v", moved, err)
	}
	if _, err := s.RenameExpected(source, dest); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CaptureSpool(row); err == nil {
		t.Fatal("old claim survived rename")
	}
	if ok, err := sp.MarkDraining(moved[0].NewID); err != nil || !ok {
		t.Fatalf("claim %v %v", ok, err)
	}
	r, err := sp.Get(moved[0].NewID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.CaptureSpool(r)
	if err != nil || v.Incarnation != source.Incarnation {
		t.Fatalf("binding lost %v %v", v, err)
	}
	if err := s.BeginSpoolPublish(r, v, "/owned/stage"); err != nil {
		t.Fatal(err)
	}
	if _, err := sp.MigrateActivePaths(r.NFSPath, "again.mov"); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("moved pending publication: %v", err)
	}
}

func TestSpoolBindingRestartRefusalAndTransientRetry(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "meta.db")
	s, err := Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	sp := NewSpoolStore(s.DB())
	s.SetSpoolStore(sp)
	if err := s.Insert(MakeEntry("reel.mov", false, 0, time.Now(), 1)); err != nil {
		t.Fatal(err)
	}
	v, err := s.CaptureMirror("reel.mov")
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.InsertBoundSpool(v, "/owned/spool")
	if err != nil {
		t.Fatal(err)
	}
	if err := sp.MarkReady(id, 4096, nil); err != nil {
		t.Fatal(err)
	}
	if ok, err := sp.MarkDraining(id); err != nil || !ok {
		t.Fatalf("claim %v %v", ok, err)
	}
	row, err := sp.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	sp = NewSpoolStore(s.DB())
	s.SetSpoolStore(sp)
	v, err = s.CaptureSpool(row)
	if err != nil {
		t.Fatalf("restart lost binding: %v", err)
	}
	if err := s.BeginSpoolPublish(row, v, "/owned/stage"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// A closed database error must not be classified as proven ambiguity.
	var pending *PendingSpoolPublication
	if err := s.CheckSpoolRecovery(); err == nil || errors.As(err, &pending) {
		t.Fatalf("query error misclassified: %v", err)
	}
	s, err = Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.CheckSpoolRecovery(); !errors.As(err, &pending) || len(pending.Paths) != 1 {
		t.Fatalf("restart accepted pending publication: %v", err)
	}
	if _, err := s.CaptureSpool(row); err == nil {
		t.Fatal("pending publication may auto-retry")
	}
}

func TestSpoolBindingLegacyUnboundNotAdopted(t *testing.T) {
	s, sp := openStoreWithSpool(t)
	id := seedReadyRow(t, s, sp, "reel.mov", 4096)
	row, err := sp.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CaptureSpool(row); err == nil {
		t.Fatal("unbound row adopted")
	}
	v, err := s.CaptureMirror(row.NFSPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.InsertBoundSpool(v, "/another/spool"); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("new writer accepted over an active legacy row: %v", err)
	}
}

func TestSpoolBindingFreshInsertRollsBackOnBindingFailure(t *testing.T) {
	for _, fault := range []string{"RAISE(FAIL,'fault')", "RAISE(IGNORE)"} {
		t.Run(fault, func(t *testing.T) {
			s, sp := openStoreWithSpool(t)
			if err := s.Insert(MakeEntry("reel.mov", false, 0, time.Now(), 901)); err != nil {
				t.Fatal(err)
			}
			v, err := s.CaptureMirror("reel.mov")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.DB().Exec(`CREATE TRIGGER reject_binding BEFORE INSERT ON spool_bindings BEGIN SELECT ` + fault + `; END`); err != nil {
				t.Fatal(err)
			}
			if _, err := s.InsertBoundSpool(v, "/owned/spool"); err == nil {
				t.Fatal("injected error ignored")
			}
			rows, err := sp.ListAll()
			if err != nil || len(rows) != 0 {
				t.Fatalf("unbound row escaped rollback: %+v %v", rows, err)
			}
			if _, err := s.DB().Exec(`DROP TRIGGER reject_binding`); err != nil {
				t.Fatal(err)
			}
			if _, err := s.InsertBoundSpool(v, "/owned/spool"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSpoolBindingRetiredClaimCannotPublishReplacement(t *testing.T) {
	s, sp, row, _ := boundSpoolFixture(t, 4096)
	source, dest, err := s.CaptureRename(row.NFSPath, "moved.mov")
	if err != nil {
		t.Fatal(err)
	}
	moved, err := sp.MigrateActivePaths(row.NFSPath, "moved.mov")
	if err != nil || len(moved) != 1 {
		t.Fatalf("migrate %v %v", moved, err)
	}
	if _, err := s.RenameExpected(source, dest); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(MakeEntry(row.NFSPath, false, 64, time.Unix(1700000010, 0), 902)); err != nil {
		t.Fatal(err)
	}
	replacement, err := s.CaptureMirror(row.NFSPath)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := sp.MarkDone(row.ID); ok || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("retired mark done accepted: %v %v", ok, err)
	}
	if _, err := s.BatchDrainComplete([]DrainCommitItem{{SpoolID: row.ID, NFSPath: row.NFSPath, Size: row.Size, Mtime: time.Now()}}); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("retired batch accepted: %v", err)
	}
	if e := s.LookupByPath(row.NFSPath); e.Size != 64 || e.Version != replacement {
		t.Fatalf("replacement RAM changed: %+v", e)
	}
	var size int64
	if err := s.DB().QueryRow(`SELECT size FROM entries WHERE path=?`, row.NFSPath).Scan(&size); err != nil || size != 64 {
		t.Fatalf("replacement durable size %d %v", size, err)
	}
	var phase string
	if err := s.DB().QueryRow(`SELECT phase FROM spool_bindings WHERE spool_id=?`, row.ID).Scan(&phase); err != nil || phase != "retired" {
		t.Fatalf("old tombstone %q %v", phase, err)
	}
	// Retiring the old attempt must not strand the transferred operation.
	if ok, err := sp.MarkDraining(moved[0].NewID); err != nil || !ok {
		t.Fatalf("claim %v %v", ok, err)
	}
	newRow, err := sp.Get(moved[0].NewID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.CaptureSpool(newRow)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BeginSpoolPublish(newRow, v, "/owned/stage"); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteSpool(newRow, v, time.Now()); err != nil {
		t.Fatal(err)
	}
}

func TestSpoolBindingMigrationIgnoreRollsBackCustody(t *testing.T) {
	for name, trigger := range map[string]string{
		"binding-insert": `BEFORE INSERT ON spool_bindings`,
		"binding-retire": `BEFORE UPDATE OF phase ON spool_bindings`,
		"spool-delete":   `BEFORE DELETE ON spool_entries`,
		"spool-requeue":  `BEFORE INSERT ON spool_entries`,
	} {
		t.Run(name, func(t *testing.T) {
			s, sp, row, v := boundSpoolFixture(t, 4096)
			if _, err := s.DB().Exec(`CREATE TRIGGER ignore_transfer ` + trigger + ` BEGIN SELECT RAISE(IGNORE); END`); err != nil {
				t.Fatal(err)
			}
			if _, err := sp.MigrateActivePaths(row.NFSPath, "moved.mov"); !errors.Is(err, ErrMirrorConflict) {
				t.Fatalf("ignored transfer accepted: %v", err)
			}
			rows, err := sp.ListAll()
			if err != nil || len(rows) != 1 || rows[0].ID != row.ID || rows[0].NFSPath != row.NFSPath || rows[0].SpoolFile != row.SpoolFile || rows[0].DrainState != DrainDraining {
				t.Fatalf("custody changed: %+v %v", rows, err)
			}
			var count int
			if err := s.DB().QueryRow(`SELECT count(*) FROM spool_bindings`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("binding count %d %v", count, err)
			}
			got, err := s.CaptureSpool(row)
			if err != nil || got != v {
				t.Fatalf("original binding lost: %v %v", got, err)
			}
		})
	}
}
