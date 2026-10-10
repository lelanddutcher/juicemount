package metadata

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRenameSQLWaitKeepsCachedNavigationResponsive(t *testing.T) {
	for _, operation := range []string{"capture", "publish"} {
		t.Run(operation, func(t *testing.T) {
			t.Setenv("JM_FTS_DEFER", "0")
			t.Setenv("JM_SERVE_FROM_SQLITE", "0")
			t.Setenv("JM_READDIR_PAGINATED", "0")
			s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			stamp := time.Unix(1700000000, 0)
			if err := s.BulkInsert([]*Entry{
				MakeEntry("known", true, 0, stamp, 10),
				MakeEntry("known/clip.mov", false, 4096, stamp, 11),
				MakeEntry("source", true, 0, stamp, 100),
				MakeEntry("source/clip.mov", false, 42, stamp, 101),
			}, 500); err != nil {
				t.Fatal(err)
			}
			source, destination, err := s.CaptureRename("source", "moved")
			if err != nil {
				t.Fatal(err)
			}
			release, baseline := holdPublicationSQLPool(t, s)
			type outcome struct {
				source, destination MirrorToken
				moved               int
				err                 error
			}
			done := make(chan outcome, 1)
			settled := false
			go func() {
				var result outcome
				if operation == "capture" {
					result.source, result.destination, result.err = s.CaptureRename("source", "moved")
				} else {
					result.moved, result.err = s.RenameExpected(source, destination)
				}
				done <- result
			}()
			defer func() {
				release()
				if !settled {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("rename operation did not settle")
					}
				}
			}()
			waitForPublicationSQLWait(t, s, baseline)
			// A retained RLock also matters: a queued cache writer makes
			// later readers wait behind it even though this read might pass.
			if s.mu.TryLock() {
				s.mu.Unlock()
			} else {
				t.Error("rename operation retained cache lock during SQL wait")
			}
			type navigation struct {
				path, inode *Entry
				children    []*Entry
				err         error
				elapsed     time.Duration
			}
			navDone := make(chan navigation, 1)
			go func() {
				start := time.Now()
				p := s.LookupByPath("known/clip.mov")
				i := s.LookupByInode(11)
				children, err := s.ListChildrenForReadDir("known")
				navDone <- navigation{p, i, children, err, time.Since(start)}
			}()
			var nav navigation
			select {
			case nav = <-navDone:
				t.Logf("cached navigation before SQL release: %s", nav.elapsed)
			case <-time.After(250 * time.Millisecond):
				t.Error("cached navigation waited >250ms behind rename SQL")
				release()
				select {
				case nav = <-navDone:
				case <-time.After(5 * time.Second):
					t.Fatal("cached reader did not settle after SQL release")
				}
			}
			if nav.err != nil || nav.path == nil || nav.path.Size != 4096 || nav.inode == nil || nav.inode.Path != "known/clip.mov" || len(nav.children) != 1 || nav.children[0].Inode != 11 {
				t.Fatalf("incorrect cached navigation: %+v", nav)
			}
			release()
			select {
			case result := <-done:
				settled = true
				if result.err != nil || (operation == "capture" && (result.source != source || result.destination != destination)) || (operation == "publish" && result.moved != 2) {
					t.Fatalf("rename result: %+v", result)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("rename operation did not finish after SQL release")
			}
			if operation == "publish" {
				moved := s.LookupByPath("moved/clip.mov")
				if moved == nil || moved.Inode != 101 || moved.Size != 42 || moved.Version.IsZero() || s.LookupByPath("source/clip.mov") != nil {
					t.Fatalf("incorrect renamed child: %+v", moved)
				}
			}
		})
	}
}
