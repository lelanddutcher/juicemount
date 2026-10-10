package metadata

import (
	"path/filepath"
	"testing"
	"time"
)

func TestScopedPrunePreservesDirectoryReplacement(t *testing.T) {
	mt := time.Unix(1700000000, 0)
	seed := func(t *testing.T) *Store {
		t.Helper()
		s, err := Open(filepath.Join(t.TempDir(), "store.db"))
		if err != nil {
			t.Fatalf("Open store: %v", err)
		}
		t.Cleanup(func() { s.Close() })
		for _, e := range []*Entry{
			MakeEntry("proj", true, 0, mt, 10),
			MakeEntry("proj/shoot", true, 0, mt, 20),
			MakeEntry("proj/shoot/old.mov", false, 12, mt, 21),
		} {
			if err := s.Insert(e); err != nil {
				t.Fatalf("Insert %q: %v", e.Path, err)
			}
		}
		return s
	}

	t.Run("replacement_between_probe_and_delete_survives", func(t *testing.T) {
		s := seed(t)
		rc := &RedisClient{store: s}
		markPruneHealthy(rc)
		finalPass := false
		replaced := false
		rc.SetSpoolGuard(func(p string) bool {
			if p == "proj/shoot" {
				if finalPass && !replaced {
					replaced = true
					for _, e := range []*Entry{
						MakeEntry("proj/shoot", true, 0, mt.Add(time.Second), 30),
						MakeEntry("proj/shoot/new.mov", false, 34, mt.Add(time.Second), 31),
					} {
						if err := s.Insert(e); err != nil {
							t.Errorf("Insert replacement %q: %v", e.Path, err)
						}
					}
				}
				finalPass = true
			}
			return false
		})

		rc.scopedPrune("proj", map[string]struct{}{})
		if !replaced {
			t.Fatal("replacement hook did not run on the final toDelete filter")
		}
		for path, inode := range map[string]uint64{"proj/shoot": 30, "proj/shoot/new.mov": 31} {
			if got := s.LookupByPath(path); got == nil || got.Inode != inode {
				t.Errorf("RAM %q = %#v, want inode %d to survive prune", path, got, inode)
			}
			var gotInode int64
			if err := s.DB().QueryRow(`SELECT inode FROM entries WHERE path = ?`, path).Scan(&gotInode); err != nil {
				t.Errorf("SQL %q missing after prune: %v", path, err)
			} else if uint64(gotInode) != inode {
				t.Errorf("SQL %q inode = %d, want %d", path, gotInode, inode)
			}
		}
	})

	t.Run("stable_absence_prunes_old_subtree", func(t *testing.T) {
		s := seed(t)
		rc := &RedisClient{store: s}
		markPruneHealthy(rc)
		rc.scopedPrune("proj", map[string]struct{}{})
		for _, path := range []string{"proj/shoot", "proj/shoot/old.mov"} {
			if got := s.LookupByPath(path); got != nil {
				t.Errorf("RAM %q remains after stable-absence prune: %#v", path, got)
			}
			var count int
			if err := s.DB().QueryRow(`SELECT COUNT(*) FROM entries WHERE path = ?`, path).Scan(&count); err != nil {
				t.Errorf("SQL count %q: %v", path, err)
			} else if count != 0 {
				t.Errorf("SQL %q remains after stable-absence prune", path)
			}
		}
	})
}
