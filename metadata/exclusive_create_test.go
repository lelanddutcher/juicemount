package metadata

import (
	"path/filepath"
	"testing"
	"time"
)

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
	s.RecordExclusiveCreate("a.mov", ExclusiveCreate{Verifier: verf, Inode: synthetic, BackendInode: real})
	s.RecordExclusiveCreate("b.mov", ExclusiveCreate{Verifier: verf, Inode: synthetic + 1})
	s.RecordExclusiveCreate("c.mov", ExclusiveCreate{Verifier: verf, Inode: synthetic + 2})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err = Open(dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
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

// The clear runs on every WRITE open. It must not wait for SQLite while
// another connection holds the write lock (a keyspace bulk insert); the
// record reaches the table behind it.
func TestExclusiveCreateClearDoesNotWaitForSQLite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	verf := [8]byte{3}
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
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err = Open(dbPath); err != nil {
		t.Fatal(err)
	}
	if s.ExclusiveCreateMatches("a.mov", verf, 11) || !s.ExclusiveCreateMatches("b.mov", verf, 12) {
		t.Fatal("records written behind the lock did not persist in order")
	}
}
