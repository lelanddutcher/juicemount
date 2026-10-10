package metadata

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMirrorSQLWaitKeepsCachedNavigationResponsive(t *testing.T) {
	for _, operation := range []string{"capture-mirror", "publish-expected", "capture-directory", "confirm-directory"} {
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
			}, 500); err != nil {
				t.Fatal(err)
			}
			input := MakeEntry("background/target.mov", false, 42, stamp, 100)
			input.LocalOnly = true
			var expected MirrorToken
			var observed StoreEpoch
			if strings.HasSuffix(operation, "directory") {
				// The descriptor caller proposes a new inode, while an existing
				// synthetic directory handle must survive confirmation.
				input = MakeEntry("background/dir", true, 0, stamp, 999)
				s.InsertToCache(MakeEntry(input.Path, true, 0, stamp, 200))
				expected, observed, err = s.CaptureDirectoryForFUSE(input.Path)
			} else {
				if operation == "capture-mirror" {
					if err := s.Insert(input); err != nil {
						t.Fatal(err)
					}
				}
				expected, err = s.CaptureMirror(input.Path)
			}
			if err != nil {
				t.Fatal(err)
			}
			release, baseline := holdPublicationSQLPool(t, s)
			type outcome struct {
				token MirrorToken
				epoch StoreEpoch
				entry *Entry
				err   error
			}
			done := make(chan outcome, 1)
			settled := false
			go func() {
				var result outcome
				switch operation {
				case "capture-mirror":
					result.token, result.err = s.CaptureMirror(input.Path)
				case "capture-directory":
					result.token, result.epoch, result.err = s.CaptureDirectoryForFUSE(input.Path)
				case "publish-expected":
					result.entry, result.err = s.PublishExpected(expected, input)
				case "confirm-directory":
					result.entry, result.err = s.ConfirmAbsentDirectoryFromFUSE(expected, observed, input)
				}
				done <- result
			}()
			defer func() {
				release()
				if !settled {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("mirror operation did not settle")
					}
				}
			}()
			waitForPublicationSQLWait(t, s, baseline)
			// A held RLock can block new readers behind a queued cache writer.
			if s.mu.TryLock() {
				s.mu.Unlock()
			} else {
				t.Error("mirror operation retained cache lock during SQL wait")
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
				t.Error("cached navigation waited >250ms behind foreground mirror SQL")
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
			var result outcome
			select {
			case result = <-done:
				settled = true
			case <-time.After(5 * time.Second):
				t.Fatal("mirror operation did not finish after SQL release")
			}
			if result.err != nil {
				t.Fatal(result.err)
			}
			if strings.HasPrefix(operation, "capture-") {
				if result.token != expected || result.epoch != observed {
					t.Fatalf("incoherent capture: %+v, want token=%+v epoch=%+v", result, expected, observed)
				}
				return
			}
			wantInode := uint64(100)
			if operation == "confirm-directory" {
				wantInode = 200
				if input.Inode != 999 {
					t.Fatal("directory confirmation mutated caller input")
				}
			}
			for _, entry := range []*Entry{result.entry, s.lookupByPathRAM(input.Path), mirrorDurableEntry(t, s, input.Path)} {
				if entry == nil || entry.Inode != wantInode || entry.Path != input.Path || entry.IsDir != input.IsDir || entry.Size != input.Size || entry.LocalOnly != input.LocalOnly || entry.Version.IsZero() || entry.Version != result.entry.Version {
					t.Fatalf("incorrect committed mirror result: %+v (returned %+v)", entry, result.entry)
				}
			}
		})
	}
}
