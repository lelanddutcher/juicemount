package metadata

import (
	"testing"
	"time"
)

// buildRenameTree populates: root/A/{f1,f2,B/{f3,C/{f4}}} plus a sibling
// root/Z/f5 that must be untouched. Returns the store.
func buildRenameTree(t *testing.T) *Store {
	s := openTestStore(t)
	now := time.Now()
	mk := func(p string, dir bool, size int64, ino uint64) {
		e := MakeEntry(p, dir, size, now, ino)
		if err := s.Insert(e); err != nil {
			t.Fatalf("insert %s: %v", p, err)
		}
	}
	mk("root", true, 0, 1)
	mk("root/A", true, 0, 2)
	mk("root/A/f1", false, 100, 3)
	mk("root/A/f2", false, 200, 4)
	mk("root/A/B", true, 0, 5)
	mk("root/A/B/f3", false, 300, 6)
	mk("root/A/B/C", true, 0, 7)
	mk("root/A/B/C/f4", false, 400, 8)
	mk("root/Z", true, 0, 9)
	mk("root/Z/f5", false, 500, 10)
	return s
}

// TestRenameSubtreeRekeysDescendants pins #8: after RenameSubtree, every
// descendant is reachable at the NEW path (pathCache + childrenIdx + inode
// entry fields), the old paths are gone, and the moved dir LISTS its children
// — the exact "folder shows empty after move" failure.
func TestRenameSubtreeRekeysDescendants(t *testing.T) {
	s := buildRenameTree(t)

	// Simulate the handler's own-entry move (its existing flow), then subtree.
	old := s.LookupByPath("root/A")
	s.DeleteFromCache("root/A")
	s.InsertToCache(MakeEntry("root/MOVED", true, 0, old.Mtime, old.Inode))
	n := s.RenameSubtree("root/A", "root/MOVED")
	if n != 6 {
		t.Fatalf("descendants re-keyed = %d, want 6", n)
	}

	// The moved dir must LIST (the bug's symptom was an empty listing).
	kids, err := s.ListChildren("root/MOVED")
	if err != nil || len(kids) != 3 {
		t.Fatalf("root/MOVED lists %d children (err=%v), want 3 (f1,f2,B)", len(kids), err)
	}
	// Deep descendant reachable at the new path with rewritten fields.
	e := s.LookupByPath("root/MOVED/B/C/f4")
	if e == nil || e.ParentPath != "root/MOVED/B/C" || e.Size != 400 {
		t.Fatalf("deep descendant broken after rename: %+v", e)
	}
	// Old paths must be gone.
	for _, p := range []string{"root/A/f1", "root/A/B", "root/A/B/C/f4"} {
		if s.LookupByPath(p) != nil {
			t.Fatalf("old path %s still resolves", p)
		}
	}
	if kids, _ := s.ListChildren("root/A"); len(kids) != 0 {
		t.Fatalf("old dir still lists %d children", len(kids))
	}
	// Sibling untouched.
	if s.LookupByPath("root/Z/f5") == nil {
		t.Fatal("sibling outside the subtree was disturbed")
	}
	// Inode lookup follows the move (pointer fields rewritten in place).
	if e := s.LookupByInode(8); e == nil || e.Path != "root/MOVED/B/C/f4" {
		t.Fatalf("inode 8 resolves to %+v, want the NEW path", e)
	}
}

// TestRenameSubtreeMovesAggregates pins the #2 integration: the subtree's
// bytes/files leave the old ancestor chain and arrive on the new one, and the
// per-dir keys are re-keyed.
func TestRenameSubtreeMovesAggregates(t *testing.T) {
	s := buildRenameTree(t)
	if b, f, ok := s.SubtreeSize("root/A"); !ok || b != 1000 || f != 4 {
		t.Fatalf("precondition: root/A = (%d,%d,%v), want (1000,4,true)", b, f, ok)
	}

	old := s.LookupByPath("root/A")
	s.DeleteFromCache("root/A")
	s.InsertToCache(MakeEntry("root/MOVED", true, 0, old.Mtime, old.Inode))
	s.RenameSubtree("root/A", "root/MOVED")

	if b, f, _ := s.SubtreeSize("root/MOVED"); b != 1000 || f != 4 {
		t.Fatalf("root/MOVED aggregate = (%d,%d), want (1000,4)", b, f)
	}
	if b, f, _ := s.SubtreeSize("root/MOVED/B"); b != 700 || f != 2 {
		t.Fatalf("root/MOVED/B aggregate = (%d,%d), want (700,2)", b, f)
	}
	if b, _, _ := s.SubtreeSize("root/A"); b != 0 {
		t.Fatalf("root/A still carries %d bytes after the move", b)
	}
	// The shared ancestor keeps the total (500 from Z + 1000 from MOVED).
	if b, f, _ := s.SubtreeSize("root"); b != 1500 || f != 5 {
		t.Fatalf("root aggregate = (%d,%d), want (1500,5)", b, f)
	}
}

// TestRenameSubtreeDurablePersists: the async phase lands in SQLite — a store
// reopened from the same file resolves the NEW paths only.
func TestRenameSubtreeDurablePersists(t *testing.T) {
	s := buildRenameTree(t)
	old := s.LookupByPath("root/A")
	s.DeleteFromCache("root/A")
	newDir := MakeEntry("root/MOVED", true, 0, old.Mtime, old.Inode)
	s.InsertToCache(newDir)
	if err := s.Delete("root/A"); err != nil {
		t.Fatalf("durable dir delete: %v", err)
	}
	if err := s.Insert(newDir); err != nil {
		t.Fatalf("durable dir insert: %v", err)
	}
	s.RenameSubtree("root/A", "root/MOVED")

	// The durable phase is async — poll SQLite (via a fresh query path)
	// until the deep new path exists, bounded.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rows, err := s.db.Query(`SELECT path FROM entries WHERE path = ?`, "root/MOVED/B/C/f4"); err == nil {
			found := rows.Next()
			rows.Close()
			if found {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM entries WHERE path LIKE 'root/A/%'`).Scan(&n); err != nil {
		t.Fatalf("count old rows: %v", err)
	}
	if n != 0 {
		t.Fatalf("%d SQLite rows still under root/A/ after durable phase", n)
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM entries WHERE path LIKE 'root/MOVED/%'`).Scan(&n); err != nil {
		t.Fatalf("count new rows: %v", err)
	}
	if n != 6 {
		t.Fatalf("%d SQLite rows under root/MOVED/, want 6", n)
	}
}
