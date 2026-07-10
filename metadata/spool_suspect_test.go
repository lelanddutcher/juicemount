package metadata

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// TestSpoolSchemaSuspectZeroTailMigration proves the #104 column lands on a
// PRE-EXISTING database created before the column existed (the ALTER TABLE
// migration path), and that InitSpoolSchema stays idempotent (a second run
// hits "duplicate column" and must not error).
func TestSpoolSchemaSuspectZeroTailMigration(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// Recreate the PRE-#104 table shape (no suspect_zero_tail column) —
	// what a live install's DB looks like before this build first boots.
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
		 VALUES ('/pre/existing.mov', '/spool/files/legacy-1', 42, 'ready', 0, 1, 1)`,
	); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}

	// First InitSpoolSchema on the legacy DB migrates; second proves idempotency.
	if err := InitSpoolSchema(db); err != nil {
		t.Fatalf("InitSpoolSchema (migration): %v", err)
	}
	if err := InitSpoolSchema(db); err != nil {
		t.Fatalf("InitSpoolSchema (rerun, duplicate-column path): %v", err)
	}

	// The pre-existing row must scan cleanly with the new column defaulting
	// to clean (NULL → "").
	s := NewSpoolStore(db)
	rows, err := s.ListAll()
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%d, want 1", len(rows))
	}
	if rows[0].SuspectZeroTail != "" {
		t.Errorf("legacy row suspect=%q, want empty", rows[0].SuspectZeroTail)
	}
}

// TestMarkSuspectZeroTailRoundTrip covers the marker CRUD: set on a writing
// row, read back via Get/ListForStatus, survive MarkReady/MarkDone untouched,
// and stay state-independent (no drain_state / updated_at side effects).
func TestMarkSuspectZeroTailRoundTrip(t *testing.T) {
	db := openTestDB(t)
	s := NewSpoolStore(db)

	id, err := s.Insert("/dl/interrupted.mp4", "/spool/files/zt-1")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	pre, err := s.Get(id)
	if err != nil {
		t.Fatalf("get pre: %v", err)
	}
	if pre.SuspectZeroTail != "" {
		t.Fatalf("fresh row suspect=%q, want empty", pre.SuspectZeroTail)
	}

	detail := `{"detected_at":"2026-07-10T00:00:00Z","size":65536,"contiguous":16384,"holes":49152}`
	if err := s.MarkSuspectZeroTail(id, detail); err != nil {
		t.Fatalf("MarkSuspectZeroTail: %v", err)
	}
	row, err := s.Get(id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if row.SuspectZeroTail != detail {
		t.Errorf("suspect=%q, want %q", row.SuspectZeroTail, detail)
	}
	if row.DrainState != DrainWriting {
		t.Errorf("drain_state=%q, want writing (marking must be state-independent)", row.DrainState)
	}
	if !row.UpdatedAt.Equal(pre.UpdatedAt) {
		t.Errorf("updated_at moved %v -> %v; marking must not reset age signals", pre.UpdatedAt, row.UpdatedAt)
	}

	// The marker must ride through the normal lifecycle untouched.
	if err := s.MarkReady(id, 65536, nil); err != nil {
		t.Fatalf("MarkReady: %v", err)
	}
	if ok, err := s.MarkDraining(id); err != nil || !ok {
		t.Fatalf("MarkDraining: ok=%v err=%v", ok, err)
	}
	if ok, err := s.MarkDone(id); err != nil || !ok {
		t.Fatalf("MarkDone: ok=%v err=%v", ok, err)
	}
	row, err = s.Get(id)
	if err != nil {
		t.Fatalf("get done: %v", err)
	}
	if row.SuspectZeroTail != detail {
		t.Errorf("suspect after lifecycle=%q, want %q", row.SuspectZeroTail, detail)
	}

	// Marking a nonexistent id is a no-op, not an error (detection-only).
	if err := s.MarkSuspectZeroTail(999999, detail); err != nil {
		t.Errorf("MarkSuspectZeroTail(missing id) = %v, want nil", err)
	}
}

// TestMigrateActivePathsCarriesSuspectMarker proves a rename-requeue
// (draining row DELETE+INSERT sharing the same spool file) carries the #104
// marker — the suspicion describes the BYTES, which are unchanged.
func TestMigrateActivePathsCarriesSuspectMarker(t *testing.T) {
	db := openTestDB(t)
	s := NewSpoolStore(db)

	id, err := s.Insert("/old/name.mp4", "/spool/files/zt-2")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	detail := `{"detected_at":"2026-07-10T00:00:00Z","size":100,"contiguous":10,"holes":90}`
	if err := s.MarkSuspectZeroTail(id, detail); err != nil {
		t.Fatalf("mark suspect: %v", err)
	}
	if err := s.MarkReady(id, 100, nil); err != nil {
		t.Fatalf("mark ready: %v", err)
	}
	if ok, err := s.MarkDraining(id); err != nil || !ok {
		t.Fatalf("mark draining: ok=%v err=%v", ok, err)
	}

	migs, err := s.MigrateActivePaths("/old/name.mp4", "/new/name.mp4")
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if len(migs) != 1 || !migs[0].Requeued {
		t.Fatalf("migrations=%+v, want 1 requeued", migs)
	}
	row, err := s.Get(migs[0].NewID)
	if err != nil {
		t.Fatalf("get requeued: %v", err)
	}
	if row.SuspectZeroTail != detail {
		t.Errorf("requeued suspect=%q, want %q (marker must follow the spool file)", row.SuspectZeroTail, detail)
	}
}
