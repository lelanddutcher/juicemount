package metadata

import (
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func dirListingRecords(t *testing.T, s *Store) string {
	t.Helper()
	rows, err := s.db.Query(`SELECT path FROM dir_listing_incomplete`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// TestDirListingIncompleteTreeOps: rename and delete act on a directory and
// its descendants only (never a sibling sharing the name as a prefix), and a
// rename drops the records of the tree it replaces.
func TestDirListingIncompleteTreeOps(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, d := range []string{"a", "a/b", "a/b/é", "ab", "a0", "dst", "dst/old"} {
		if err := s.MarkDirListingIncomplete(d); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.RenameDirListingIncomplete("a", "dst"); err != nil {
		t.Fatal(err)
	}
	if got, want := dirListingRecords(t, s), "a0,ab,dst,dst/b,dst/b/é"; got != want {
		t.Fatalf("after rename = %s, want %s", got, want)
	}
	if err := s.ClearDirListingIncompleteTree("dst/b"); err != nil {
		t.Fatal(err)
	}
	if got, want := dirListingRecords(t, s), "a0,ab,dst"; got != want {
		t.Fatalf("after tree clear = %s, want %s", got, want)
	}
	if has, err := s.HasDirListingIncomplete("dst"); err != nil || !has {
		t.Fatalf("HasDirListingIncomplete(dst) = %v, %v", has, err)
	}
}

// TestDirListingIncompleteLandsOnExistingDB: a mirror DB written before the
// table existed opens, keeps its rows and gains the table; records persist
// across a reopen, and the load drops those whose folder has no rows.
func TestDirListingIncompleteLandsOnExistingDB(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(MakeEntry("d/a.txt", false, 1, time.Now(), 11)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`DROP TABLE dir_listing_incomplete`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(dbPath)
	if err != nil {
		t.Fatalf("Open of a DB without the table: %v", err)
	}
	if s.LookupByPath("d/a.txt") == nil {
		t.Fatal("existing row lost")
	}
	for _, d := range []string{"d", "empty"} {
		if err := s.MarkDirListingIncomplete(d); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	dirs, err := s.LoadDirListingIncomplete()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(dirs, ",") != "d" {
		t.Fatalf("loaded records = %v, want [d] (a folder without rows is dropped)", dirs)
	}
	if got := dirListingRecords(t, s); got != "d" {
		t.Fatalf("durable records after load = %s, want d", got)
	}
}
