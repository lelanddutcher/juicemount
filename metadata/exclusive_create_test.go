package metadata

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func insertRow(t *testing.T, s *Store, p string, inode uint64) {
	t.Helper()
	if err := s.Insert(MakeEntry(p, false, 0, time.Now(), inode)); err != nil {
		t.Fatal(err)
	}
}

func reopenStore(t *testing.T, s *Store, dbPath string) *Store {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// An EXCLUSIVE CREATE's record survives a reopen, answers only for the file
// it was made for, and goes when cleared or too old.
func TestExclusiveCreateRecord(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	verf := [8]byte{1, 2, 3, 4}
	other := [8]byte{9}
	const synthetic, real = 1<<63 | 7, 4242
	for i, p := range []string{"a.mov", "b.mov", "c.mov"} {
		insertRow(t, s, p, synthetic+uint64(i))
	}
	s.RecordExclusiveCreate("a.mov", ExclusiveCreate{Verifier: verf, Inode: synthetic, BackendInode: real})
	s.RecordExclusiveCreate("b.mov", ExclusiveCreate{Verifier: verf, Inode: synthetic + 1})
	s.RecordExclusiveCreate("c.mov", ExclusiveCreate{Verifier: verf, Inode: synthetic + 2})
	s = reopenStore(t, s, dbPath)
	if !s.ExclusiveCreateMatches("a.mov", verf, synthetic) || s.ExclusiveCreateMatches("a.mov", other, synthetic) {
		t.Fatal("reopened store lost or misread the verifier")
	}
	// The mirror row's synthetic inode swapped for the backend one: the same file.
	if !s.ExclusiveCreateMatches("a.mov", verf, real) {
		t.Fatal("the reconciled inode is not the same file")
	}
	// Another file at the name (another Mac's save upserted over it).
	if s.ExclusiveCreateMatches("b.mov", verf, 9999) || s.ExclusiveCreateMatches("b.mov", verf, synthetic+1) {
		t.Fatal("a record answered for another file, or outlived it")
	}
	s.ClearExclusiveCreate("c.mov")
	if s.ExclusiveCreateMatches("c.mov", verf, synthetic+2) {
		t.Fatal("cleared record still matches")
	}
	s.exclusive.now = func() time.Time { return time.Now().Add(ExclusiveCreateMaxAge) }
	if s.ExclusiveCreateMatches("a.mov", verf, synthetic) {
		t.Fatal("record answered past its maximum age")
	}
	if n := s.exclusive.count.Load(); n != 0 {
		t.Fatalf("%d records left after every one was cleared or dropped", n)
	}
}

// Synthetic inodes restart at 1 with every process. A record whose row went
// while its clear was lost must not answer for a new file that is given the
// same synthetic inode after the restart.
func TestExclusiveCreateLoadDropsRecordsWithoutTheirRow(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	verf := [8]byte{4}
	const synthetic = 1<<63 | 1
	insertRow(t, s, "a.mov", synthetic)
	s.RecordExclusiveCreate("a.mov", ExclusiveCreate{Verifier: verf, Inode: synthetic})
	if err := s.Delete("a.mov"); err != nil { // its clear never reached SQLite
		t.Fatal(err)
	}
	s = reopenStore(t, s, dbPath)
	insertRow(t, s, "a.mov", synthetic) // a new file, the same first synthetic inode
	if s.ExclusiveCreateMatches("a.mov", verf, synthetic) {
		t.Fatal("a record without its row answered for a new file")
	}
}

// The clear runs on every WRITE open. It must not wait for SQLite while
// another connection holds the write lock (a keyspace bulk insert); the
// record reaches the table behind it.
func TestExclusiveCreateClearDoesNotWaitForSQLite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	verf := [8]byte{3}
	insertRow(t, s, "a.mov", 11)
	insertRow(t, s, "b.mov", 12)
	s.RecordExclusiveCreate("a.mov", ExclusiveCreate{Verifier: verf, Inode: 11})
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT OR REPLACE INTO store_meta(key, value) VALUES('exclusive-create-test', 'x')`); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		s.RecordExclusiveCreate("b.mov", ExclusiveCreate{Verifier: verf, Inode: 12})
		s.ClearExclusiveCreate("a.mov")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		_ = tx.Rollback()
		t.Fatal("record and clear waited for the SQLite write lock")
	}
	if s.ExclusiveCreateMatches("a.mov", verf, 11) || !s.ExclusiveCreateMatches("b.mov", verf, 12) {
		t.Fatal("in-memory records wrong while SQLite was locked")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	s = reopenStore(t, s, dbPath)
	if s.ExclusiveCreateMatches("a.mov", verf, 11) || !s.ExclusiveCreateMatches("b.mov", verf, 12) {
		t.Fatal("records written behind the lock did not persist in order")
	}
}

// A record and its clear a moment later (the client's first WRITE) cancel
// in memory; what is left is written in one transaction.
func TestExclusiveCreateWritesBehindInOneBatch(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	insertRow(t, s, "lock", 5)
	s.RecordExclusiveCreate("lock", ExclusiveCreate{Verifier: [8]byte{1}, Inode: 5})
	s.ClearExclusiveCreate("lock")
	first := &s.exclusive
	s = reopenStore(t, s, dbPath)
	if c, w := first.commits.Load(), first.writes.Load(); c != 0 || w != 0 {
		t.Fatalf("record then clear cost %d commits, %d writes; want none", c, w)
	}
	const n = 20
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("f%02d.mov", i)
		insertRow(t, s, p, uint64(100+i))
		s.RecordExclusiveCreate(p, ExclusiveCreate{Verifier: [8]byte{2}, Inode: uint64(100 + i)})
	}
	second := &s.exclusive
	s = reopenStore(t, s, dbPath)
	if c, w := second.commits.Load(), second.writes.Load(); c != 1 || w != n {
		t.Fatalf("%d records cost %d commits, %d writes; want 1, %d", n, c, w, n)
	}
	if !s.ExclusiveCreateMatches("f07.mov", [8]byte{2}, 107) {
		t.Fatal("batched record did not persist")
	}
}

// A DB opened by 31c71746 has an exclusive_creates table and two triggers
// on entries. Open drops them, triggers first, and deletes keep working.
func TestExclusiveCreateDropsTheEarlierShape(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`
CREATE TABLE exclusive_creates (path TEXT PRIMARY KEY, verifier BLOB NOT NULL);
CREATE TRIGGER exclusive_create_entry_delete AFTER DELETE ON entries
BEGIN DELETE FROM exclusive_creates WHERE path=OLD.path; END;
CREATE TRIGGER exclusive_create_entry_move AFTER UPDATE OF path ON entries
WHEN OLD.path <> NEW.path
BEGIN DELETE FROM exclusive_creates WHERE path=OLD.path OR path=NEW.path; END;`); err != nil {
		t.Fatal(err)
	}
	insertRow(t, s, "a.mov", 9)
	s = reopenStore(t, s, dbPath)
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name IN
		('exclusive_creates', 'exclusive_create_entry_delete', 'exclusive_create_entry_move')`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("earlier shape left %d objects (%v)", n, err)
	}
	if err := s.Delete("a.mov"); err != nil {
		t.Fatalf("entries delete after the drop: %v", err)
	}
}
