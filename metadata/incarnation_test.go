package metadata

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestMirrorIncarnationMigrationRestart(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "mirror.db")
	db, err := sql.Open("sqlite", filename)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO entries VALUES ('reel.mov','reel.mov','.',0,128,1700000000,901,420,0)`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	s, err := Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	before := s.LookupByPath("reel.mov")
	if before == nil || before.Version.IsZero() || before.Size != 128 {
		t.Fatalf("migration lost row or token: %+v", before)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	after := s.LookupByPath("reel.mov")
	if after == nil || after.Version != before.Version {
		t.Fatalf("restart changed incarnation: before=%+v after=%+v", before, after)
	}
	updated, err := s.SetSizeExactExpected(before.Version, 0, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if updated.Size != 0 || updated.Version.Incarnation != before.Version.Incarnation || updated.Version.Revision <= before.Version.Revision {
		t.Fatalf("bad CAS publication: %+v", updated)
	}
}

func TestMirrorIncarnationMigrationRollback(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(schema + metaSchema + `CREATE TRIGGER migration_failure BEFORE INSERT ON store_meta BEGIN SELECT RAISE(FAIL,'fault'); END;`); err != nil {
		t.Fatal(err)
	}
	if err := initIncarnationSchema(db); err == nil {
		t.Fatal("migration unexpectedly succeeded")
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM pragma_table_info('entries') WHERE name IN ('incarnation','revision')`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial migration escaped rollback: count=%d err=%v", count, err)
	}
	if _, err := db.Exec(`DROP TRIGGER migration_failure`); err != nil {
		t.Fatal(err)
	}
	if err := initIncarnationSchema(db); err != nil {
		t.Fatal(err)
	}
}

func TestMirrorExpectedZeroRowAndFailurePreserveRAM(t *testing.T) {
	s := newTestStore(t)
	e := MakeEntry("reel.mov", false, 128, time.Now(), 901)
	if err := s.Insert(e); err != nil {
		t.Fatal(err)
	}
	before := s.LookupByPath(e.Path)
	if _, err := s.DB().Exec(`CREATE TRIGGER ignore_size BEFORE UPDATE OF size ON entries BEGIN SELECT RAISE(IGNORE); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetSizeExactExpected(before.Version, 0, time.Now()); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("zero-row CAS error=%v", err)
	}
	if got := s.LookupByPath(e.Path); got.Size != 128 || got.Version != before.Version {
		t.Fatalf("zero-row CAS changed RAM: %+v", got)
	}
	if _, err := s.DB().Exec(`DROP TRIGGER ignore_size; CREATE TRIGGER fail_size BEFORE UPDATE OF size ON entries BEGIN SELECT RAISE(FAIL,'fault'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetSizeExactExpected(before.Version, 0, time.Now()); err == nil {
		t.Fatal("failed SQLite publication acknowledged")
	}
	if got := s.LookupByPath(e.Path); got.Size != 128 || got.Version != before.Version {
		t.Fatalf("failed CAS changed RAM: %+v", got)
	}
}

func TestMirrorAbsentTokenRejectsCreateDelete(t *testing.T) {
	s := newTestStore(t)
	missing, err := s.CaptureMirror("reel.mov")
	if err != nil {
		t.Fatal(err)
	}
	e := MakeEntry("reel.mov", false, 128, time.Now(), 901)
	if err := s.Insert(e); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(e.Path); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PublishExpected(missing, e); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("absent ABA observation accepted: %v", err)
	}
	fresh, err := s.CaptureMirror(e.Path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PublishExpected(fresh, e); err != nil {
		t.Fatal(err)
	}
}

func TestMirrorRenamePreservesIncarnationAndRejectsOldPath(t *testing.T) {
	s := newTestStore(t)
	for i, p := range []string{"scene", "scene/reel.mov"} {
		if err := s.Insert(MakeEntry(p, i == 0, 128, time.Now(), uint64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	oldChild := s.LookupByPath("scene/reel.mov")
	source, err := s.CaptureMirror("scene")
	if err != nil {
		t.Fatal(err)
	}
	destination, err := s.CaptureMirror("moved")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.RenameExpected(source, destination); err != nil || n != 2 {
		t.Fatalf("rename=(%d,%v)", n, err)
	}
	moved := s.LookupByPath("moved/reel.mov")
	if moved == nil || moved.Version.Incarnation != oldChild.Version.Incarnation || moved.Version.Revision <= oldChild.Version.Revision {
		t.Fatalf("rename lost incarnation: old=%+v new=%+v", oldChild, moved)
	}
	if err := s.Insert(oldChild); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("old-path producer accepted after ancestor rename: %v", err)
	}
	if s.LookupByPath(oldChild.Path) != nil {
		t.Fatal("old path resurrected")
	}
	if got := s.lookupByPathSQLite(moved.Path); got == nil || got.Version != moved.Version {
		t.Fatalf("SQL/RAM token mismatch: %+v versus %+v", got, moved)
	}
}

func TestMirrorRenameInvalidatesOnlyActualSubtrees(t *testing.T) {
	for _, directory := range []bool{false, true} {
		name := "file"
		if directory {
			name = "directory"
		}
		t.Run(name, func(t *testing.T) {
			s := newTestStore(t)
			if err := s.Insert(MakeEntry("source", directory, 128, time.Now(), 77)); err != nil {
				t.Fatal(err)
			}
			calls := 0
			s.SetOnSubtreeRenamed(func(old, new string) {
				if old != "source" || new != "destination" {
					t.Errorf("unexpected callback %s -> %s", old, new)
				}
				calls++
			})
			source, destination, err := s.CaptureRename("source", "destination")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.RenameExpected(source, destination); err != nil {
				t.Fatal(err)
			}
			want := 0
			if directory {
				want = 1
			}
			if calls != want {
				t.Fatalf("subtree callbacks=%d, want %d", calls, want)
			}
		})
	}
}

func TestMirrorRenameFailureLeavesBothNamespaces(t *testing.T) {
	s := newTestStore(t)
	for i, p := range []string{"scene", "scene/reel.mov"} {
		if err := s.Insert(MakeEntry(p, i == 0, 128, time.Now(), uint64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	source, _ := s.CaptureMirror("scene")
	destination, _ := s.CaptureMirror("moved")
	if _, err := s.DB().Exec(`CREATE TRIGGER fail_move BEFORE INSERT ON entries WHEN NEW.path='moved/reel.mov' BEGIN SELECT RAISE(FAIL,'fault'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RenameExpected(source, destination); err == nil {
		t.Fatal("rename publication fault acknowledged")
	}
	for _, p := range []string{"scene", "scene/reel.mov"} {
		if s.LookupByPath(p) == nil || s.lookupByPathSQLite(p) == nil {
			t.Fatalf("failed transaction removed %s", p)
		}
	}
	if s.LookupByPath("moved") != nil || s.lookupByPathSQLite("moved") != nil {
		t.Fatal("failed transaction exposed destination")
	}
}

func TestMirrorLegacyMutationVersionsStayAligned(t *testing.T) {
	s := newTestStore(t)
	e := MakeEntry("reel.mov", false, 128, time.Now(), 901)
	e.LocalOnly = true
	if err := s.Insert(e); err != nil {
		t.Fatal(err)
	}
	checks := []func() error{
		func() error { return s.UpdateSize(e.Path, 256, time.Now()) },
		func() error { return s.SetSizeExact(e.Path, 0, time.Now()) },
		func() error { return s.UpdateMode(e.Path, 0600) },
		func() error { return s.ClearLocalOnly(e.Path) },
		func() error { return s.BulkClearLocalOnly([]string{e.Path}) },
	}
	for i, update := range checks {
		if err := update(); err != nil {
			t.Fatal(err)
		}
		if _, err := s.CaptureMirror(e.Path); err != nil {
			t.Fatalf("mutation %d left incompatible token: %v", i, err)
		}
	}
}

func TestMirrorExpectedDeleteRecreateSameInode(t *testing.T) {
	s := newTestStore(t)
	e := MakeEntry("reel.mov", false, 128, time.Unix(1700000000, 0), 901)
	if err := s.Insert(e); err != nil {
		t.Fatal(err)
	}
	before := s.LookupByPath(e.Path)
	absent, err := s.DeleteExpected(before.Version)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := s.PublishExpected(absent, e)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.Version.Incarnation == before.Version.Incarnation {
		t.Fatal("delete/recreate reused logical incarnation")
	}
	if _, err := s.DeleteExpected(before.Version); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("stale delete accepted: %v", err)
	}
	if _, err := s.SetSizeExactExpected(before.Version, 0, time.Now()); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("stale CAS accepted: %v", err)
	}
}

func TestMirrorIncarnationAbruptRestart(t *testing.T) {
	const childEnv = "JM_TEST_MIRROR_CRASH_DB"
	if filename := os.Getenv(childEnv); filename != "" {
		s, err := Open(filename)
		if err != nil {
			t.Fatal(err)
		}
		e := s.LookupByPath("reel.mov")
		if e == nil {
			t.Fatal("migrated row missing")
		}
		if err := json.NewEncoder(os.Stdout).Encode(e.Version); err != nil {
			t.Fatal(err)
		}
		// No Store.Close, checkpoint, or deferred cleanup runs in the child.
		process, err := os.FindProcess(os.Getpid())
		if err != nil {
			t.Fatal(err)
		}
		if err := process.Kill(); err != nil {
			t.Fatal(err)
		}
		select {}
	}
	filename := filepath.Join(t.TempDir(), "mirror.db")
	db, err := sql.Open("sqlite", filename)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(schema + `INSERT INTO entries VALUES ('reel.mov','reel.mov','.',0,128,1700000000,901,420,0)`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestMirrorIncarnationAbruptRestart$")
	command.Env = append(os.Environ(), childEnv+"="+filename)
	output, err := command.Output()
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) {
		t.Fatalf("child was not terminated: err=%v output=%s", err, output)
	}
	var before MirrorToken
	if err := json.Unmarshal(output, &before); err != nil {
		t.Fatalf("child token: %v; stdout=%s stderr=%s", err, output, exitError.Stderr)
	}
	s, err := Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got := s.LookupByPath("reel.mov")
	if got == nil || got.Version != before {
		t.Fatalf("committed migration token changed after process death: before=%+v after=%+v", before, got)
	}
}

func TestMirrorRenameUnicodeAncestor(t *testing.T) {
	s := newTestStore(t)
	for i, p := range []string{"scène", "scène/réel.mov", "scène/nested", "scène/nested/取材.mov"} {
		if err := s.Insert(MakeEntry(p, i == 0 || i == 2, 128, time.Now(), uint64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	source, destination, err := s.CaptureRename("scène", "編集")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.RenameExpected(source, destination); err != nil || n != 4 {
		t.Fatalf("Unicode ancestor rename=(%d,%v), want 4,nil", n, err)
	}
	for _, suffix := range []string{"", "/réel.mov", "/nested", "/nested/取材.mov"} {
		if s.LookupByPath("scène"+suffix) != nil || s.lookupByPathSQLite("scène"+suffix) != nil {
			t.Fatalf("old Unicode path remains: %s", suffix)
		}
		if s.LookupByPath("編集"+suffix) == nil || s.lookupByPathSQLite("編集"+suffix) == nil {
			t.Fatalf("moved Unicode path missing: %s", suffix)
		}
	}
}

func TestMirrorAbsentInsertRejectsStaleSnapshot(t *testing.T) {
	oldThreshold := FTSFullRebuildThreshold
	FTSFullRebuildThreshold = 1
	t.Cleanup(func() { FTSFullRebuildThreshold = oldThreshold })
	for _, incremental := range []bool{false, true} {
		name := "bulk"
		if incremental {
			name = "incremental"
		}
		t.Run(name, func(t *testing.T) {
			s := newTestStore(t)
			if err := s.Insert(MakeEntry("reel.mov", false, 4096, time.Now(), 901)); err != nil {
				t.Fatal(err)
			}
			stale := s.LookupByPath("reel.mov")
			if err := s.Delete(stale.Path); err != nil {
				t.Fatal(err)
			}
			s.ftsInitialized.Store(incremental)
			if err := s.BulkInsertAbsent([]*Entry{stale}, 500); !errors.Is(err, ErrMirrorConflict) {
				t.Fatalf("stale absent insert accepted: %v", err)
			}
			if s.LookupByPath(stale.Path) != nil || s.lookupByPathSQLite(stale.Path) != nil {
				t.Fatal("stale snapshot resurrected a deleted row")
			}
		})
	}
}

func TestMirrorRenameRefusesRAMOnlyDescendant(t *testing.T) {
	s := newTestStore(t)
	if err := s.Insert(MakeEntry("scene", true, 0, time.Now(), 1)); err != nil {
		t.Fatal(err)
	}
	source, destination, err := s.CaptureRename("scene", "moved")
	if err != nil {
		t.Fatal(err)
	}
	child := MakeEntry("scene/reel.mov", false, 128, time.Now(), 2)
	s.InsertToCache(child)
	called := false
	s.SetOnSubtreeRenamed(func(string, string) { called = true })
	if _, _, err := s.CaptureRename("scene", "moved"); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("preflight accepted RAM-only child: %v", err)
	}
	if _, err := s.RenameExpected(source, destination); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("publication accepted new RAM-only child: %v", err)
	}
	if called {
		t.Fatal("failed rename fired committed-subtree callback")
	}
	if s.LookupByPath(child.Path) == nil || s.LookupByPath("scene") == nil || s.LookupByPath("moved") != nil {
		t.Fatal("refused rename changed the cached namespace")
	}
}

func TestMirrorAbsentBatchTokenRejectsCreateDelete(t *testing.T) {
	oldThreshold := FTSFullRebuildThreshold
	FTSFullRebuildThreshold = 1
	t.Cleanup(func() { FTSFullRebuildThreshold = oldThreshold })
	for _, incremental := range []bool{false, true} {
		name := "bulk"
		if incremental {
			name = "incremental"
		}
		t.Run(name, func(t *testing.T) {
			s := newTestStore(t)
			token, err := s.CaptureMirror("reel.mov")
			if err != nil {
				t.Fatal(err)
			}
			stale := MakeEntry("reel.mov", false, 4096, time.Now(), 901)
			stale.Version = token
			if err := s.Insert(MakeEntry(stale.Path, false, 128, time.Now(), 902)); err != nil {
				t.Fatal(err)
			}
			if err := s.Delete(stale.Path); err != nil {
				t.Fatal(err)
			}
			s.ftsInitialized.Store(incremental)
			if err := s.BulkInsertAbsent([]*Entry{stale}, 500); !errors.Is(err, ErrMirrorConflict) {
				t.Fatalf("absent ABA token accepted by batch: %v", err)
			}
			if s.LookupByPath(stale.Path) != nil || s.lookupByPathSQLite(stale.Path) != nil {
				t.Fatal("stale absent observation resurrected deleted row")
			}
		})
	}
}
