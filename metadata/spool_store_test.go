package metadata

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "spool.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec(`PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;`); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if err := InitSpoolSchema(db); err != nil {
		t.Fatalf("schema: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestSpoolStoreInsertAndGet(t *testing.T) {
	db := openTestDB(t)
	s := NewSpoolStore(db)

	id, err := s.Insert("/foo/bar.mov", "/spool/files/abc-123")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if id == 0 {
		t.Fatalf("expected non-zero id")
	}

	row, err := s.Get(id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if row.NFSPath != "/foo/bar.mov" {
		t.Errorf("got nfs path %q, want %q", row.NFSPath, "/foo/bar.mov")
	}
	if row.SpoolFile != "/spool/files/abc-123" {
		t.Errorf("got spool file %q", row.SpoolFile)
	}
	if row.DrainState != DrainWriting {
		t.Errorf("got drain state %q, want %q", row.DrainState, DrainWriting)
	}
	if row.Size != 0 {
		t.Errorf("got size %d, want 0", row.Size)
	}
}

func TestSpoolStoreLifecycle(t *testing.T) {
	db := openTestDB(t)
	s := NewSpoolStore(db)

	id, err := s.Insert("/movie.mov", "/spool/files/x")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	sha := []byte("deadbeef-fake-sha")
	if err := s.MarkReady(id, 1024, sha); err != nil {
		t.Fatalf("mark ready: %v", err)
	}
	row, _ := s.Get(id)
	if row.DrainState != DrainReady {
		t.Fatalf("after MarkReady got state %q, want ready", row.DrainState)
	}
	if row.Size != 1024 {
		t.Errorf("got size %d, want 1024", row.Size)
	}
	if string(row.SHA256) != string(sha) {
		t.Errorf("SHA round-trip mismatch")
	}

	claimed, err := s.MarkDraining(id)
	if err != nil {
		t.Fatalf("mark draining: %v", err)
	}
	if !claimed {
		t.Fatalf("expected claim to succeed on ready row")
	}

	claimed2, _ := s.MarkDraining(id)
	if claimed2 {
		t.Fatalf("second MarkDraining should not re-claim a draining row")
	}

	if _, err := s.MarkDone(id); err != nil {
		t.Fatalf("mark done: %v", err)
	}
	row, _ = s.Get(id)
	if row.DrainState != DrainDone {
		t.Fatalf("after MarkDone got %q", row.DrainState)
	}
}

func TestSpoolStoreMarkReadyRefusesNonWriting(t *testing.T) {
	db := openTestDB(t)
	s := NewSpoolStore(db)

	id, _ := s.Insert("/a", "/spool/files/a")
	_ = s.MarkReady(id, 100, nil)
	// Second MarkReady should fail because state is no longer writing.
	if err := s.MarkReady(id, 200, nil); err == nil {
		t.Fatalf("expected MarkReady on already-ready row to error")
	}
}

func TestSpoolStoreLookupByPathReturnsLatest(t *testing.T) {
	db := openTestDB(t)
	s := NewSpoolStore(db)

	id1, _ := s.Insert("/dup.mov", "/spool/files/a")
	_ = s.MarkReady(id1, 100, nil)
	_, _ = s.MarkDraining(id1)
	_, _ = s.MarkDone(id1) // done rows are excluded from LookupByPath

	id2, _ := s.Insert("/dup.mov", "/spool/files/b")

	row, err := s.LookupByPath("/dup.mov")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if row.ID != id2 {
		t.Errorf("expected latest non-done id %d, got %d", id2, row.ID)
	}
}

func TestSpoolStoreLookupByPathMiss(t *testing.T) {
	db := openTestDB(t)
	s := NewSpoolStore(db)

	if _, err := s.LookupByPath("/nope"); err != sql.ErrNoRows {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestSpoolStoreListReady(t *testing.T) {
	db := openTestDB(t)
	s := NewSpoolStore(db)

	ids := make([]int64, 0, 5)
	for i := 0; i < 5; i++ {
		id, err := s.Insert("/f"+string(rune('a'+i)), "/spool/files/"+string(rune('a'+i)))
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
		if err := s.MarkReady(id, int64(i+1)*100, nil); err != nil {
			t.Fatalf("mark ready %d: %v", i, err)
		}
		ids = append(ids, id)
	}

	rows, err := s.ListReady(3)
	if err != nil {
		t.Fatalf("list ready: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3, got %d", len(rows))
	}
	// Should be ordered by created_at ASC.
	if rows[0].ID != ids[0] {
		t.Errorf("expected first row id=%d, got %d", ids[0], rows[0].ID)
	}
}

func TestSpoolStorePendingStats(t *testing.T) {
	db := openTestDB(t)
	s := NewSpoolStore(db)

	id1, _ := s.Insert("/a", "/spool/files/a")
	_ = s.MarkReady(id1, 100, nil)
	id2, _ := s.Insert("/b", "/spool/files/b")
	_ = s.MarkReady(id2, 200, nil)
	id3, _ := s.Insert("/c", "/spool/files/c")
	_ = s.MarkReady(id3, 300, nil)
	_, _ = s.MarkDraining(id3)
	_, _ = s.MarkDone(id3) // excluded from pending

	pending, bytes, err := s.PendingStats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if pending != 2 {
		t.Errorf("pending=%d, want 2", pending)
	}
	if bytes != 300 {
		t.Errorf("bytes=%d, want 300", bytes)
	}
}

func TestSpoolStoreIncrementAttempts(t *testing.T) {
	db := openTestDB(t)
	s := NewSpoolStore(db)

	id, _ := s.Insert("/retry.bin", "/spool/files/r")
	_ = s.MarkReady(id, 50, nil)
	_, _ = s.MarkDraining(id) // currently in draining state

	if err := s.IncrementAttempts(id, "transient EAGAIN"); err != nil {
		t.Fatalf("incr: %v", err)
	}
	row, _ := s.Get(id)
	if row.DrainAttempts != 1 {
		t.Errorf("attempts=%d, want 1", row.DrainAttempts)
	}
	if row.LastError != "transient EAGAIN" {
		t.Errorf("last_error=%q", row.LastError)
	}
	// Critically: state must NOT have changed.
	if row.DrainState != DrainDraining {
		t.Errorf("state changed by IncrementAttempts: got %q, want draining", row.DrainState)
	}

	// Second increment composes.
	_ = s.IncrementAttempts(id, "second EAGAIN")
	row, _ = s.Get(id)
	if row.DrainAttempts != 2 {
		t.Errorf("attempts=%d, want 2", row.DrainAttempts)
	}
	if row.LastError != "second EAGAIN" {
		t.Errorf("last_error=%q", row.LastError)
	}
}

func TestSpoolStoreDeleteDone(t *testing.T) {
	db := openTestDB(t)
	s := NewSpoolStore(db)

	// Three rows: one done & old, one done & recent, one ready & old.
	idOldDone, _ := s.Insert("/old-done", "/spool/files/o1")
	_ = s.MarkReady(idOldDone, 10, nil)
	_, _ = s.MarkDraining(idOldDone)
	_, _ = s.MarkDone(idOldDone)
	// Force its updated_at to the past.
	_, _ = db.Exec(`UPDATE spool_entries SET updated_at=0 WHERE id=?`, idOldDone)

	idNewDone, _ := s.Insert("/new-done", "/spool/files/o2")
	_ = s.MarkReady(idNewDone, 10, nil)
	_, _ = s.MarkDraining(idNewDone)
	_, _ = s.MarkDone(idNewDone)

	idOldReady, _ := s.Insert("/old-ready", "/spool/files/o3")
	_ = s.MarkReady(idOldReady, 10, nil)
	_, _ = db.Exec(`UPDATE spool_entries SET updated_at=0 WHERE id=?`, idOldReady)

	n, err := s.DeleteDone(time.Now())
	if err != nil {
		t.Fatalf("delete done: %v", err)
	}
	if n != 1 {
		t.Errorf("deleted=%d, want 1 (only the OLD done row)", n)
	}

	// Old done is gone; new done and old ready remain.
	if _, err := s.Get(idOldDone); err != sql.ErrNoRows {
		t.Errorf("expected old-done deleted, got %v", err)
	}
	if _, err := s.Get(idNewDone); err != nil {
		t.Errorf("new-done was wrongly deleted: %v", err)
	}
	row, err := s.Get(idOldReady)
	if err != nil {
		t.Errorf("old-ready was wrongly deleted: %v", err)
	}
	if err == nil && row.DrainState != DrainReady {
		t.Errorf("old-ready state was modified: %q", row.DrainState)
	}
}

func TestSpoolStoreMarkFailedBumpsAttempts(t *testing.T) {
	db := openTestDB(t)
	s := NewSpoolStore(db)

	id, _ := s.Insert("/x", "/spool/files/x")
	_ = s.MarkReady(id, 100, nil)

	if err := s.MarkFailed(id, "first try"); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	row, _ := s.Get(id)
	if row.DrainState != DrainFailed {
		t.Errorf("state=%q, want failed", row.DrainState)
	}
	if row.DrainAttempts != 1 {
		t.Errorf("attempts=%d, want 1", row.DrainAttempts)
	}
	if row.LastError != "first try" {
		t.Errorf("last_error=%q", row.LastError)
	}

	_ = s.ResetToReady(id)
	_ = s.MarkFailed(id, "second try")
	row, _ = s.Get(id)
	if row.DrainAttempts != 2 {
		t.Errorf("attempts after second fail=%d, want 2", row.DrainAttempts)
	}
}
