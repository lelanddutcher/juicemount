package catalog

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func editFixture(t *testing.T, path string, statements ...string) {
	t.Helper()
	db, err := openDB(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("fixture statement %s: %v", statement, err)
		}
	}
}

func triggerDDL(name string) string {
	for _, object := range expectedSchema() {
		if object.Name == name {
			return object.SQL
		}
	}
	panic("missing fixture trigger")
}

func TestCatalogRefusesUnrecognizedAndChangedFormats(t *testing.T) {
	for name, statements := range map[string][]string{
		"foreign app":                  {`PRAGMA application_id=42`},
		"newer schema":                 {`PRAGMA user_version=2`},
		"missing table":                {`DROP TABLE revision_parents`},
		"missing immutable trigger":    {`DROP TRIGGER events_no_update`},
		"extra table":                  {`CREATE TABLE unrelated(value TEXT)`},
		"wildcard autoindex lookalike": {`CREATE TABLE sqliteXautoindexYextra(value TEXT)`},
		"extra column":                 {`ALTER TABLE assets ADD COLUMN unexpected TEXT`},
		"ledger mismatch":              {`DROP TRIGGER schema_migrations_no_update`, `UPDATE schema_migrations SET fingerprint='0000000000000000000000000000000000000000000000000000000000000000'`, triggerDDL("schema_migrations_no_update")},
	} {
		t.Run(name, func(t *testing.T) {
			s, path := newTestStore(t)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			editFixture(t, path, statements...)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			opened, err := Open(path)
			if opened != nil {
				opened.Close()
			}
			if !errors.Is(err, ErrFormat) {
				t.Fatalf("Open = %v, want format error", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("refused target changed: %v", err)
			}
		})
	}
	for name, data := range map[string][]byte{"empty": {}, "corrupt": []byte("not a SQLite catalog")} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "catalog.sqlite")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if s, err := Open(path); err == nil {
				s.Close()
				t.Fatal("unrecognized file opened")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(data, after) {
				t.Fatalf("unrecognized target replaced: %v", err)
			}
		})
	}
	path := filepath.Join(t.TempDir(), "incomplete.sqlite")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	db, err := openDB(path, false)
	if err != nil {
		t.Fatal(err)
	}
	err = initialize(context.Background(), db, testLibrary) // ready=0, never completed.
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("incomplete fixture: %v, %v", err, closeErr)
	}
	if s, err := Open(path); !errors.Is(err, ErrFormat) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("incomplete database opened: %v", err)
	}
}

func TestCatalogCreatePreservesExistingTargetsAndOrphanSidecars(t *testing.T) {
	s, path := newTestStore(t)
	applyTest(t, s, initialOperation())
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if created, err := Create(path, otherLibrary); !errors.Is(err, ErrConflict) {
		if created != nil {
			created.Close()
		}
		t.Fatalf("duplicate Create: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("duplicate Create changed target: %v", err)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		t.Run(suffix, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "missing.sqlite")
			const data = "unrelated sidecar bytes"
			if err := os.WriteFile(path+suffix, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			if s, err := Create(path, testLibrary); err == nil {
				s.Close()
				t.Fatal("adopted orphan sidecar")
			}
			got, err := os.ReadFile(path + suffix)
			if err != nil || string(got) != data {
				t.Fatalf("orphan sidecar changed: %q, %v", got, err)
			}
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				t.Fatalf("main destination was reserved: %v", err)
			}
		})
	}
	if s, err := Open(filepath.Join(t.TempDir(), "missing")); !errors.Is(err, ErrNotFound) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("missing Open: %v", err)
	}
	for _, path := range []string{"", ":memory:", "file:catalog.sqlite?mode=memory"} {
		if s, err := Open(path); !errors.Is(err, ErrInvalid) {
			if s != nil {
				s.Close()
			}
			t.Fatalf("accepted URI/memory input %q: %v", path, err)
		}
	}
}

func TestCatalogCanonicalParentAndEscapedPath(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "real")
	if err := os.Mkdir(real, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(dir, "alias")
	if err := os.Symlink(real, alias); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(alias, "catalog ?#& café.sqlite")
	s, err := Create(path, testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	if s.path != filepath.Join(real, filepath.Base(path)) {
		t.Fatalf("parent not canonical: %s", s.path)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogRefusesSymlinksHardlinksAndNonregularFiles(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink", "directory"} {
		t.Run(kind, func(t *testing.T) {
			s, path := newTestStore(t)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(filepath.Dir(path), "alias.sqlite")
			var err error
			switch kind {
			case "symlink":
				err = os.Symlink(path, target)
			case "hardlink":
				err = os.Link(path, target)
			case "directory":
				err = os.Mkdir(target, 0700)
			}
			if err != nil {
				t.Fatal(err)
			}
			if opened, err := Open(target); err == nil {
				opened.Close()
				t.Fatal("unsafe target opened")
			}
			if kind == "hardlink" {
				if opened, err := Open(path); err == nil {
					opened.Close()
					t.Fatal("multiply-linked original opened")
				}
			}
		})
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		for _, kind := range []string{"symlink", "hardlink", "directory"} {
			t.Run(suffix+kind, func(t *testing.T) {
				s, path := newTestStore(t)
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				source := filepath.Join(filepath.Dir(path), "sidecar-source")
				if err := os.WriteFile(source, []byte("preserve"), 0600); err != nil {
					t.Fatal(err)
				}
				var err error
				switch kind {
				case "symlink":
					err = os.Symlink(source, path+suffix)
				case "hardlink":
					err = os.Link(source, path+suffix)
				case "directory":
					err = os.Mkdir(path+suffix, 0700)
				}
				if err != nil {
					t.Fatal(err)
				}
				if opened, err := Open(path); err == nil {
					opened.Close()
					t.Fatal("unsafe sidecar admitted")
				}
				got, err := os.ReadFile(source)
				if err != nil || string(got) != "preserve" {
					t.Fatalf("sidecar source changed: %q, %v", got, err)
				}
			})
		}
	}
}

func TestCatalogConnectionSettingsSurviveConnectionReplacement(t *testing.T) {
	s, _ := newTestStore(t)
	// Dropping every idle connection forces the DSN setup onto new connections.
	s.db.SetMaxIdleConns(0)
	for range 2 {
		if err := checkSettings(context.Background(), s.db); err != nil {
			t.Fatal(err)
		}
		var mode string
		if err := s.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
			t.Fatalf("journal_mode: %s, %v", mode, err)
		}
	}
	applyTest(t, s, initialOperation())
	// All prior standalone connections have been released, including transactions.
	if s.db.Stats().InUse != 0 {
		t.Fatal("connection leaked")
	}
}

func TestCatalogCloseDrainsAndRefusesNewCalls(t *testing.T) {
	s, _ := newTestStore(t)
	if err := s.enter(); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	closed := make(chan error, 1)
	go func() { close(started); closed <- s.Close() }()
	<-started
	var early bool
	select {
	case <-closed:
		early = true
	default:
	}
	s.leave()
	if early {
		t.Fatal("Close returned while an admitted call held lifecycle protection")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close failed to drain")
	}
	if err := s.Close(); err != nil {
		t.Fatal("repeated Close:", err)
	}
	for name, call := range map[string]func() error{
		"library":   func() error { _, e := s.LibraryID(); return e },
		"apply":     func() error { _, e := s.Apply(context.Background(), initialOperation()); return e },
		"asset":     func() error { _, e := s.GetAsset(context.Background(), testLibrary, testAsset); return e },
		"revision":  func() error { _, e := s.GetRevision(context.Background(), testLibrary, rootRevision); return e },
		"event":     func() error { _, e := s.GetEvent(context.Background(), testLibrary, "initial"); return e },
		"assets":    func() error { _, e := s.ListAssets(context.Background(), testLibrary, "", 10); return e },
		"revisions": func() error { _, e := s.ListRevisions(context.Background(), testLibrary, testAsset, "", 10); return e },
		"events":    func() error { _, e := s.Events(context.Background(), testLibrary, 0, 10); return e },
	} {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, ErrClosed) {
				t.Fatalf("closed call: %v", err)
			}
		})
	}
}
