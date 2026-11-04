package metadata

import (
	"path/filepath"
	"testing"
	"time"
)

// An EXCLUSIVE CREATE's verifier survives a reopen and goes when the name
// does, or when a mutation clears it.
func TestExclusiveCreateRecord(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	verf := [8]byte{1, 2, 3, 4}
	other := [8]byte{9}
	for _, p := range []string{"a.mov", "b.mov", "c.mov"} {
		if err := s.Insert(MakeEntry(p, false, 0, time.Now(), 100+uint64(len(p)))); err != nil {
			t.Fatal(err)
		}
		if err := s.RecordExclusiveCreate(p, verf); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err = Open(dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	match := func(p string, v [8]byte) bool {
		t.Helper()
		ok, err := s.ExclusiveCreateMatches(p, v)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !match("a.mov", verf) || match("a.mov", other) {
		t.Fatal("reopened store lost or misread the verifier")
	}
	// A same-path upsert (a size update) keeps it.
	if err := s.Insert(MakeEntry("a.mov", false, 42, time.Now(), 101)); err != nil {
		t.Fatal(err)
	}
	if !match("a.mov", verf) {
		t.Fatal("an upsert of the same row cleared the verifier")
	}
	if err := s.ClearExclusiveCreate("a.mov"); err != nil {
		t.Fatal(err)
	}
	if match("a.mov", verf) {
		t.Fatal("cleared verifier still matches")
	}
	if err := s.Delete("b.mov"); err != nil {
		t.Fatal(err)
	}
	if match("b.mov", verf) {
		t.Fatal("verifier outlived the deleted row")
	}
	if _, err := s.db.Exec(`UPDATE entries SET path='d.mov' WHERE path='c.mov'`); err != nil {
		t.Fatal(err)
	}
	if match("c.mov", verf) || match("d.mov", verf) {
		t.Fatal("verifier outlived the moved row")
	}
}
