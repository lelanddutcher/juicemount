package metadata

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestAbsentPublicationLoadsDurableWinnerNotStaleInput(t *testing.T) {
	t.Setenv("JM_SUBTREE_SIZES", "1")
	s := newTestStore(t)
	stamp := time.Unix(1700000000, 0)
	if err := s.Insert(MakeEntry("d", true, 0, stamp, 10)); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(MakeEntry("d/target.mov", false, 42, stamp, 100)); err != nil {
		t.Fatal(err)
	}
	s.DeleteFromCache("d/target.mov")
	if err := s.publishAbsentEntries([]*Entry{MakeEntry("d/target.mov", false, 1, stamp, 999)}); err != nil {
		t.Fatal(err)
	}
	got := s.lookupByPathRAM("d/target.mov")
	if got == nil || got.Inode != 100 || got.Size != 42 || got.Version.IsZero() {
		t.Fatalf("published row = %+v, want committed inode 100, size 42, version", got)
	}
	if byInode := s.lookupByInodeRAM(100); byInode == nil || byInode.Path != got.Path || s.lookupByInodeRAM(999) != nil {
		t.Fatalf("inode maps lost committed owner: committed=%+v stale=%+v", byInode, s.lookupByInodeRAM(999))
	}
	children, err := s.listChildrenRAM("d")
	if err != nil || len(children) != 1 || children[0].Path != got.Path {
		t.Fatalf("children after publication = %+v, %v", children, err)
	}
	if bytes, files, ok := s.SubtreeSize("d"); !ok || bytes != 42 || files != 1 {
		t.Fatalf("subtree after publication = %d bytes, %d files, ok=%v", bytes, files, ok)
	}
}

func TestAbsentPublicationQueuedCacheOnlySuccessorWins(t *testing.T) {
	for _, successor := range []string{"unlink", "replacement"} {
		t.Run(successor, func(t *testing.T) {
			t.Setenv("JM_FTS_DEFER", "0")
			t.Setenv("JM_SERVE_FROM_SQLITE", "0")
			t.Setenv("JM_SUBTREE_SIZES", "1")
			s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			stamp := time.Unix(1700000000, 0)
			if err := s.BulkInsert([]*Entry{
				MakeEntry("d", true, 0, stamp, 10),
				MakeEntry("d/target.mov", false, 42, stamp, 100),
			}, 500); err != nil {
				t.Fatal(err)
			}
			s.DeleteFromCache("d/target.mov")
			release, baseline := holdPublicationSQLPool(t, s)
			published := make(chan error, 1)
			pubSettled := false
			started := make(chan struct{})
			successorDone := make(chan struct{}, 1)
			successorStarted := false
			successorSettled := false
			go func() {
				published <- s.publishAbsentEntries([]*Entry{MakeEntry("d/target.mov", false, 1, stamp, 999)})
			}()
			defer func() {
				release()
				if !pubSettled {
					select {
					case <-published:
					case <-time.After(5 * time.Second):
						t.Error("publication did not settle after SQL release")
					}
				}
				if successorStarted && !successorSettled {
					select {
					case <-successorDone:
					case <-time.After(5 * time.Second):
						t.Error("queued successor did not settle after SQL release")
					}
				}
			}()
			waitForPublicationSQLWait(t, s, baseline)
			if s.mu.TryLock() {
				s.mu.Unlock()
			} else {
				release()
				t.Fatal("final SELECT retained cache lock")
			}
			if s.cachePublicationMu.TryLock() {
				s.cachePublicationMu.Unlock()
				release()
				t.Fatal("final SELECT did not retain publication gate")
			}
			go func() {
				close(started)
				if successor == "unlink" {
					s.DeleteFromCache("d/target.mov")
				} else {
					s.InsertToCache(MakeEntry("d/target.mov", false, 77, stamp.Add(time.Second), 101))
				}
				successorDone <- struct{}{}
			}()
			successorStarted = true
			<-started
			select {
			case <-successorDone:
				successorSettled = true
				t.Fatal("cache-only successor crossed held final SELECT")
			case <-time.After(25 * time.Millisecond):
			}
			release()
			select {
			case err := <-published:
				pubSettled = true
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("publication did not finish")
			}
			select {
			case <-successorDone:
				successorSettled = true
			case <-time.After(5 * time.Second):
				t.Fatal("cache-only successor did not finish")
			}
			got := s.lookupByPathRAM("d/target.mov")
			children, err := s.listChildrenRAM("d")
			if err != nil {
				t.Fatal(err)
			}
			if successor == "unlink" {
				if got != nil || s.lookupByInodeRAM(100) != nil || len(children) != 0 {
					t.Fatalf("unlink successor lost: path=%+v inode=%+v children=%+v", got, s.lookupByInodeRAM(100), children)
				}
				if bytes, files, ok := s.SubtreeSize("d"); !ok || bytes != 0 || files != 0 {
					t.Fatalf("unlink subtree = %d bytes, %d files, ok=%v", bytes, files, ok)
				}
			} else {
				oldHandle, newHandle := s.lookupByInodeRAM(100), s.lookupByInodeRAM(101)
				if got == nil || got.Inode != 101 || got.Size != 77 || len(children) != 1 || children[0].Inode != 101 {
					t.Fatalf("replacement successor or handle alias lost: path=%+v old=%+v new=%+v children=%+v", got, oldHandle, newHandle, children)
				}
				// Lookups return independent snapshots; compare the handle's
				// replacement identity and payload rather than Go pointers.
				for _, handle := range []*Entry{oldHandle, newHandle} {
					if handle == nil || handle.Path != got.Path || handle.Inode != got.Inode || handle.Size != got.Size || handle.Version != got.Version {
						t.Fatalf("replacement handle mismatch: path=%+v handle=%+v", got, handle)
					}
				}
				if bytes, files, ok := s.SubtreeSize("d"); !ok || bytes != 77 || files != 1 {
					t.Fatalf("replacement subtree = %d bytes, %d files, ok=%v", bytes, files, ok)
				}
			}
			var durableInode int64
			var durableSize int64
			if err := s.db.QueryRow(`SELECT inode, size FROM entries WHERE path='d/target.mov'`).Scan(&durableInode, &durableSize); err != nil || durableInode != 100 || durableSize != 42 {
				t.Fatalf("durable winner inode=%d size=%d err=%v", durableInode, durableSize, err)
			}
		})
	}
}

func TestAbsentPublicationCachedPathSkipsSQLEvenWithoutDurableRow(t *testing.T) {
	t.Setenv("JM_FTS_DEFER", "0")
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	stamp := time.Unix(1700000000, 0)
	s.InsertToCache(MakeEntry("cache-only.mov", false, 77, stamp, 77))
	release, _ := holdPublicationSQLPool(t, s)
	done := make(chan error, 1)
	settled := false
	go func() {
		done <- s.publishAbsentEntries([]*Entry{MakeEntry("cache-only.mov", false, 1, stamp, 999)})
	}()
	defer func() {
		release()
		if !settled {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("cache-hit publication did not settle")
			}
		}
	}()
	select {
	case err := <-done:
		settled = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(250 * time.Millisecond):
		release()
		t.Fatal("cached path queried held SQL despite present winner")
	}
	if got := s.lookupByPathRAM("cache-only.mov"); got == nil || got.Inode != 77 || got.Size != 77 || s.lookupByInodeRAM(999) != nil {
		t.Fatalf("cached winner changed: %+v", got)
	}
}

func TestAbsentPublicationMissingRowKeepsPublishedPrefixAndUnlocks(t *testing.T) {
	t.Setenv("JM_SUBTREE_SIZES", "1")
	s := newTestStore(t)
	stamp := time.Unix(1700000000, 0)
	for _, e := range []*Entry{
		MakeEntry("d", true, 0, stamp, 10),
		MakeEntry("d/first.mov", false, 42, stamp, 100),
		MakeEntry("d/tail.mov", false, 7, stamp, 102),
	} {
		if err := s.Insert(e); err != nil {
			t.Fatal(err)
		}
	}
	s.DeleteFromCache("d/first.mov")
	s.DeleteFromCache("d/tail.mov")
	err := s.publishAbsentEntries([]*Entry{
		MakeEntry("d/first.mov", false, 1, stamp, 999),
		MakeEntry("d/missing.mov", false, 1, stamp, 998),
		MakeEntry("d/tail.mov", false, 1, stamp, 997),
	})
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing committed row = %v, want sql.ErrNoRows", err)
	}
	if !s.writeMu.TryLock() {
		t.Fatal("writeMu retained after SELECT failure")
	}
	s.writeMu.Unlock()
	if !s.cachePublicationMu.TryLock() {
		t.Fatal("publication gate retained after SELECT failure")
	}
	s.cachePublicationMu.Unlock()
	if !s.mu.TryLock() {
		t.Fatal("cache lock retained after SELECT failure")
	}
	s.mu.Unlock()
	first := s.lookupByPathRAM("d/first.mov")
	if first == nil || first.Inode != 100 || first.Size != 42 || first.Version.IsZero() {
		t.Fatalf("valid prefix missing or stale: %+v", first)
	}
	for _, path := range []string{"d/missing.mov", "d/tail.mov"} {
		if got := s.lookupByPathRAM(path); got != nil {
			t.Fatalf("failed row or tail fabricated at %s: %+v", path, got)
		}
	}
	if s.lookupByInodeRAM(999) != nil || s.lookupByInodeRAM(998) != nil || s.lookupByInodeRAM(997) != nil || s.lookupByInodeRAM(102) != nil {
		t.Fatal("failed row or tail fabricated in inode map")
	}
	children, err := s.listChildrenRAM("d")
	if err != nil || len(children) != 1 || children[0].Path != "d/first.mov" {
		t.Fatalf("prefix children = %+v, %v", children, err)
	}
	if bytes, files, ok := s.SubtreeSize("d"); !ok || bytes != 42 || files != 1 {
		t.Fatalf("prefix subtree = %d bytes, %d files, ok=%v", bytes, files, ok)
	}
	for _, item := range []struct {
		path  string
		inode int64
	}{
		{"d/first.mov", 100}, {"d/tail.mov", 102},
	} {
		var inode int64
		if err := s.db.QueryRow(`SELECT inode FROM entries WHERE path=?`, item.path).Scan(&inode); err != nil || inode != item.inode {
			t.Fatalf("durable %s inode=%d err=%v, want %d", item.path, inode, err, item.inode)
		}
	}
	if err := s.publishAbsentEntries([]*Entry{MakeEntry("d/tail.mov", false, 1, stamp, 997)}); err != nil {
		t.Fatalf("later publication after error: %v", err)
	}
	if tail := s.lookupByPathRAM("d/tail.mov"); tail == nil || tail.Inode != 102 || tail.Size != 7 {
		t.Fatalf("locks not reusable or tail not committed: %+v", tail)
	}
}
