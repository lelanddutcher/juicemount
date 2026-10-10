package metadata

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// legacySpoolDB recreates a PRE-punched_end database — what a live install looks
// like before this build first boots — and migrates it.
func legacySpoolDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "old.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`
CREATE TABLE spool_entries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    nfs_path        TEXT NOT NULL,
    spool_file      TEXT NOT NULL UNIQUE,
    size            INTEGER NOT NULL DEFAULT 0,
    sha256          BLOB,
    drain_state     TEXT NOT NULL CHECK(drain_state IN ('writing','ready','draining','done','failed')),
    drain_attempts  INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);`); err != nil {
		t.Fatalf("create legacy table: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO spool_entries (nfs_path, spool_file, size, drain_state, drain_attempts, created_at, updated_at)
		 VALUES ('/pre/existing.mov', '/spool/files/legacy-1', 42, 'failed', 0, 1, 1)`,
	); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}
	return db
}

// A row that predates streaming must migrate to punched_end = 0.
//
// This is the case that matters most for safety: every existing spool file on
// every existing install is un-punched, and recovery must keep treating those
// exactly as it always has. A non-zero default would make boot recovery preserve
// (and refuse to resume) every ordinary interrupted copy.
func TestPunchedEndMigratesLegacyRowsToZero(t *testing.T) {
	db := legacySpoolDB(t)
	if err := InitSpoolSchema(db); err != nil {
		t.Fatalf("InitSpoolSchema (migration): %v", err)
	}
	// Idempotent — it runs on every boot.
	if err := InitSpoolSchema(db); err != nil {
		t.Fatalf("InitSpoolSchema (rerun, duplicate-column path): %v", err)
	}

	s := NewSpoolStore(db)
	got, err := s.PunchedEnd(1)
	if err != nil {
		t.Fatalf("PunchedEnd on a migrated legacy row: %v", err)
	}
	if got != 0 {
		t.Errorf("legacy row migrated to punched_end = %d, want 0 — a non-zero "+
			"default would make recovery refuse to resume every pre-existing "+
			"interrupted copy", got)
	}
}

// The boundary must be MONOTONIC.
//
// Punching cannot be undone, so a lower value must never overwrite a higher one.
// A retry that recomputed a smaller boundary would tell recovery the file is
// MORE intact than it is — and recovery would then resume a spool file whose
// prefix is holes, uploading zeros over good backend bytes.
func TestSetPunchedEndIsMonotonic(t *testing.T) {
	db := legacySpoolDB(t)
	if err := InitSpoolSchema(db); err != nil {
		t.Fatal(err)
	}
	s := NewSpoolStore(db)

	if err := s.SetPunchedEnd(1, 64<<20); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.PunchedEnd(1); got != 64<<20 {
		t.Fatalf("after first set: %d, want %d", got, 64<<20)
	}

	if err := s.SetPunchedEnd(1, 1<<20); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.PunchedEnd(1); got != 64<<20 {
		t.Errorf("punched_end RETREATED to %d — those bytes are already holes, so "+
			"lowering the boundary tells recovery the file is more intact than it is",
			got)
	}

	if err := s.SetPunchedEnd(1, 128<<20); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.PunchedEnd(1); got != 128<<20 {
		t.Errorf("punched_end = %d after a legitimate advance, want %d", got, 128<<20)
	}
}

// A negative boundary is a programming error, not a value to store.
func TestSetPunchedEndRejectsNegative(t *testing.T) {
	db := legacySpoolDB(t)
	if err := InitSpoolSchema(db); err != nil {
		t.Fatal(err)
	}
	s := NewSpoolStore(db)
	if err := s.SetPunchedEnd(1, -1); err == nil {
		t.Error("SetPunchedEnd accepted a negative boundary")
	}
	if got, _ := s.PunchedEnd(1); got != 0 {
		t.Errorf("punched_end = %d after a rejected negative set", got)
	}
}
