//go:build h8aacceptance

package metadata

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestH8APruneRejectsLateCacheOnlyChild(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	mt := time.Unix(1700000000, 0)
	for _, e := range []*Entry{
		MakeEntry("proj", true, 0, mt, 10),
		MakeEntry("proj/shoot", true, 0, mt, 20),
		MakeEntry("proj/shoot/old.mov", false, 12, mt, 21),
	} {
		if err := s.Insert(e); err != nil {
			t.Fatal(err)
		}
	}
	expected := []*Entry{s.LookupByPath("proj/shoot"), s.LookupByPath("proj/shoot/old.mov")}
	s.deletePathsAfterCommitHook = func() {
		s.InsertToCache(MakeEntry("proj/shoot/new.mov", false, 34, mt, 22))
	}
	if err := s.DeletePathsConditional(expected); err != nil {
		t.Fatal(err)
	}
	if child := s.LookupByPath("proj/shoot/new.mov"); child != nil && s.LookupByPath("proj/shoot") == nil {
		t.Fatalf("cache-only child orphaned under pruned directory: %+v", child)
	}
}

func TestH8APruneRejectsLateDurableChild(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "store.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	mt := time.Unix(1700000000, 0)
	for _, e := range []*Entry{
		MakeEntry("proj", true, 0, mt, 10),
		MakeEntry("proj/shoot", true, 0, mt, 20),
		MakeEntry("proj/shoot/old.mov", false, 12, mt, 21),
	} {
		if err := s.Insert(e); err != nil {
			t.Fatal(err)
		}
	}
	expected := []*Entry{s.LookupByPath("proj/shoot"), s.LookupByPath("proj/shoot/old.mov")}
	if err := s.DeletePathsConditional(expected); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(MakeEntry("proj/shoot/new.mov", false, 34, mt, 22)); !errors.Is(err, ErrMirrorConflict) {
		t.Errorf("late durable child insert = %v, want mirror conflict", err)
	}
	if child := s.LookupByPath("proj/shoot/new.mov"); child != nil && s.LookupByPath("proj/shoot") == nil {
		t.Errorf("durable child visible under pruned directory: %+v", child)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if child := reopened.LookupByPath("proj/shoot/new.mov"); child != nil && reopened.LookupByPath("proj/shoot") == nil {
		t.Errorf("durable child survived reopen under pruned directory: %+v", child)
	}
}
