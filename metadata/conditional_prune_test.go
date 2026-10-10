package metadata

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestDeletePathsConditionalRejectsNewChild(t *testing.T) {
	for _, cacheOnly := range []bool{false, true} {
		name := "durable"
		if cacheOnly {
			name = "cache_only"
		}
		t.Run(name, func(t *testing.T) {
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
			newChild := MakeEntry("proj/shoot/new.mov", false, 34, mt, 22)
			if cacheOnly {
				s.InsertToCache(newChild)
			} else if err := s.Insert(newChild); err != nil {
				t.Fatal(err)
			}
			if err := s.DeletePathsConditional(expected); !errors.Is(err, ErrMirrorConflict) {
				t.Fatalf("conditional prune = %v, want mirror conflict", err)
			}
			for _, p := range []string{"proj/shoot", "proj/shoot/old.mov", "proj/shoot/new.mov"} {
				if s.LookupByPath(p) == nil {
					t.Errorf("RAM %q disappeared after conflict", p)
				}
			}
			for _, p := range []string{"proj/shoot", "proj/shoot/old.mov"} {
				var count int
				if err := s.DB().QueryRow(`SELECT COUNT(*) FROM entries WHERE path=?`, p).Scan(&count); err != nil || count != 1 {
					t.Errorf("SQL %q after conflict: count=%d err=%v", p, count, err)
				}
			}
		})
	}
}

func TestScopedPruneKeepsProtectedDescendantAncestor(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	mt := time.Unix(1700000000, 0)
	for _, e := range []*Entry{
		MakeEntry("proj", true, 0, mt, 10),
		MakeEntry("proj/shoot", true, 0, mt, 20),
		MakeEntry("proj/shoot/keep.mov", false, 12, mt, 21),
		MakeEntry("proj/shoot/gone.mov", false, 12, mt, 22),
	} {
		if err := s.Insert(e); err != nil {
			t.Fatal(err)
		}
	}
	rc := &RedisClient{store: s}
	markPruneHealthy(rc)
	rc.SetSpoolGuard(func(p string) bool { return p == "proj/shoot/keep.mov" })
	token := rc.scanToken()
	if err := rc.scopedPruneWithToken("proj", map[string]struct{}{}, &token); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"proj/shoot", "proj/shoot/keep.mov"} {
		if s.LookupByPath(p) == nil {
			t.Errorf("protected path %q disappeared", p)
		}
	}
	if s.LookupByPath("proj/shoot/gone.mov") != nil {
		t.Error("gone sibling was not pruned")
	}
}

func TestDeletePathsConditionalRunsHookAfterParentCleanup(t *testing.T) {
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
	newChild := MakeEntry("proj/shoot/new.mov", false, 34, mt, 22)
	s.deletePathsAfterCommitHook = func() {
		if s.LookupByPath("proj/shoot") != nil {
			s.InsertToCache(newChild)
		}
	}
	if err := s.DeletePathsConditional(expected); err != nil {
		t.Fatal(err)
	}
	if s.LookupByPath("proj/shoot/new.mov") != nil {
		t.Fatal("cache-only child was published between SQL commit and parent cleanup")
	}
}
