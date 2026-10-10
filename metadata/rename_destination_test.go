package metadata

import (
	"path/filepath"
	"testing"
	"time"
)

func TestKnownRenameDestinationChildrenUseLocalSources(t *testing.T) {
	for _, scenario := range []string{"ram-only", "sql-only", "sql-error"} {
		t.Run(scenario, func(t *testing.T) {
			s, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					_ = s.Close()
				}
			})
			child := MakeEntry("destination/child.mov", false, 17, time.Now(), 9001)
			switch scenario {
			case "ram-only":
				s.InsertToCache(child)
				var rows int
				if err := s.db.QueryRow(`SELECT COUNT(*) FROM entries WHERE path=?`, child.Path).Scan(&rows); err != nil || rows != 0 {
					t.Fatalf("RAM-only fixture unexpectedly durable: rows=%d err=%v", rows, err)
				}
			case "sql-only":
				if err := s.Insert(child); err != nil {
					t.Fatal(err)
				}
				s.mu.Lock()
				delete(s.childrenIdx, "destination")
				delete(s.pathCache, child.Path)
				delete(s.inodeCache, child.Inode)
				s.mu.Unlock()
			case "sql-error":
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				closed = true
			}
			present, err := s.HasKnownChildren("destination")
			if scenario == "sql-error" {
				if err == nil || present {
					t.Fatalf("SQL failure claimed empty or present directory: present=%v err=%v", present, err)
				}
				return
			}
			if err != nil || !present {
				t.Fatalf("known child missed: present=%v err=%v", present, err)
			}
			present, err = s.HasKnownChildren("different-destination")
			if err != nil || present {
				t.Fatalf("unrelated child changed destination admission: present=%v err=%v", present, err)
			}
		})
	}
}
