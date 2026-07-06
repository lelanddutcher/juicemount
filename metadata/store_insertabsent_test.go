package metadata

import (
	"testing"
	"time"
)

// Batch-3 adversarial review #1 (U7 TOCTOU): the async FUSE warmers
// (refreshUnmirroredDir, prefetchChildren) filter via LookupByPath OUTSIDE
// any store lock, then batch-insert. A fresher row landing between the
// filter and the insert (a keyspace-push event finishing a remote write)
// must survive in BOTH the serving caches and SQLite — BulkInsertAbsent
// enforces the presence check inside the store's own critical sections.
// This test reproduces the exact interleaving: filter (row absent) → fresh
// insert → stale batch insert.
func TestBulkInsertAbsentNeverOverwritesFresherRow(t *testing.T) {
	s := newTestStore(t)

	// The warmer's FUSE snapshot: mid-write size (40MB) for d/a.mov plus a
	// genuinely-new sibling.
	stale := MakeEntry("d/a.mov", false, 40<<20, time.Now().Add(-time.Minute), 111)
	sibling := MakeEntry("d/b.mov", false, 7, time.Now(), 112)
	batch := []*Entry{stale, sibling}

	// Warmer filter step: LookupByPath == nil for both…
	for _, e := range batch {
		if s.LookupByPath(e.Path) != nil {
			t.Fatalf("precondition: %q unexpectedly present", e.Path)
		}
	}
	// …THEN the push event lands with the final size (53MB) before the batch.
	freshSize := int64(53 << 20)
	if err := s.Insert(MakeEntry("d/a.mov", false, freshSize, time.Now(), 111)); err != nil {
		t.Fatalf("fresh Insert: %v", err)
	}

	if err := s.BulkInsertAbsent(batch, 500); err != nil {
		t.Fatalf("BulkInsertAbsent: %v", err)
	}
	// Drain any deferred FTS backlog so the FTS-hit assertion below holds under
	// JM_FTS_DEFER=1 (no-op when the flag is off — fts_pending is empty).
	if _, err := s.drainFTSPendingForTest(); err != nil {
		t.Fatalf("drainFTSPendingForTest: %v", err)
	}

	// Fresh size survives in the serving cache…
	got := s.LookupByPath("d/a.mov")
	if got == nil {
		t.Fatal("d/a.mov missing after BulkInsertAbsent")
	}
	if got.Size != freshSize {
		t.Fatalf("cache size = %d, want fresh %d (stale FUSE snapshot clobbered the push row)", got.Size, freshSize)
	}
	// …AND in SQLite (interleavings can leave the two stores independently
	// stale — check both).
	var dbSize int64
	if err := s.DB().QueryRow(`SELECT size FROM entries WHERE path = ?`, "d/a.mov").Scan(&dbSize); err != nil {
		t.Fatalf("sqlite read: %v", err)
	}
	if dbSize != freshSize {
		t.Fatalf("sqlite size = %d, want fresh %d", dbSize, freshSize)
	}
	var rows int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM entries WHERE path = ?`, "d/a.mov").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("rows for d/a.mov = %d, want 1", rows)
	}

	// The genuinely-new sibling landed normally, in both stores.
	if s.LookupByPath("d/b.mov") == nil {
		t.Fatal("genuinely-new row d/b.mov not in the cache")
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM entries WHERE path = ?`, "d/b.mov").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("sqlite rows for d/b.mov = %d, want 1", rows)
	}

	// FTS stayed consistent: exactly one hit, serving the fresh row (the
	// skip path must issue NO FTS delete/insert for the existing row).
	res, err := s.Search("a.mov", 10, "")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	hits := 0
	for _, r := range res {
		if r.Entry.Path == "d/a.mov" {
			hits++
			if r.Entry.Size != freshSize {
				t.Fatalf("FTS-served size = %d, want %d", r.Entry.Size, freshSize)
			}
		}
	}
	if hits != 1 {
		t.Fatalf("FTS hits for d/a.mov = %d, want exactly 1", hits)
	}
}

// The rename-race variant of the same TOCTOU: the warmer's snapshot carries
// the OLD path of a file a fresher push-event row has since renamed (same
// inode, new path). Inserting the stale old-path entry must not displace the
// fresh owner in the inode map (QA-25 stale-handle class) — BulkInsertAbsent
// skips an entry whose inode is already owned by a live cached path.
func TestBulkInsertAbsentRespectsInodeOwner(t *testing.T) {
	s := newTestStore(t)

	fresh := MakeEntry("d/renamed.mov", false, 10, time.Now(), 222)
	if err := s.Insert(fresh); err != nil {
		t.Fatal(err)
	}
	stale := MakeEntry("d/old.mov", false, 10, time.Now().Add(-time.Minute), 222)
	if err := s.BulkInsertAbsent([]*Entry{stale}, 500); err != nil {
		t.Fatal(err)
	}

	got := s.LookupByInode(222)
	if got == nil || got.Path != "d/renamed.mov" {
		t.Fatalf("inode 222 owner = %+v, want d/renamed.mov (stale old-path entry displaced the fresh rename)", got)
	}
	if s.LookupByPath("d/old.mov") != nil {
		t.Fatal("stale old-path entry served from the cache despite its inode being owned by the fresh rename")
	}
}

// Plain-absent semantics: with no race at all, BulkInsertAbsent behaves like
// BulkInsert for brand-new rows (the U7/prefetch steady state).
func TestBulkInsertAbsentInsertsNewRows(t *testing.T) {
	s := newTestStore(t)
	batch := []*Entry{
		MakeEntry("w/a.mov", false, 1, time.Now(), 301),
		MakeEntry("w/b", true, 0, time.Now(), 302),
	}
	if err := s.BulkInsertAbsent(batch, 500); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"w/a.mov", "w/b"} {
		if s.LookupByPath(p) == nil {
			t.Fatalf("row %q not inserted", p)
		}
	}
	kids, _ := s.ListChildren("w")
	if len(kids) != 2 {
		t.Fatalf("ListChildren(w) = %d, want 2 (childrenIdx must be maintained)", len(kids))
	}
}
