package metadata

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestRenameLegacyExcludesZeroTokenRoots(t *testing.T) {
	s := newTestStore(t)
	stamp := time.Unix(1700000000, 0)
	for _, e := range []*Entry{
		MakeEntry("source", true, 0, stamp, 10),
		MakeEntry("source/clip.mov", false, 42, stamp, 11),
	} {
		if err := s.Insert(e); err != nil {
			t.Fatal(err)
		}
	}
	s.DeleteFromCache("source")
	s.InsertToCache(MakeEntry("source", true, 0, stamp, 10))
	s.InsertToCache(MakeEntry("destination", true, 0, stamp, 20))
	if s.lookupByPathRAM("source").Version.IsZero() == false || s.lookupByPathRAM("destination").Version.IsZero() == false {
		t.Fatal("fixture roots unexpectedly carry durable tokens")
	}
	calls := 0
	s.SetOnSubtreeRenamed(func(old, new string) {
		if old != "source" || new != "destination" {
			t.Errorf("callback paths %q -> %q", old, new)
		}
		calls++
	})
	n, err := s.RenameSubtreeSync("source", "destination")
	if err != nil || n != 1 || calls != 1 {
		t.Fatalf("legacy descendants-only rename = %d, %v, callbacks=%d", n, err, calls)
	}
	if got := s.lookupByPathRAM("destination/clip.mov"); got == nil || got.Inode != 11 || got.Size != 42 {
		t.Fatalf("legacy moved child = %+v", got)
	}
	if s.lookupByPathRAM("source/clip.mov") != nil || s.lookupByPathRAM("destination") == nil || s.lookupByPathRAM("source") == nil {
		t.Fatal("legacy rename disturbed an exact zero-token root or retained old child")
	}
}

func TestRenameRejectsStaleRootFactsWithoutCallback(t *testing.T) {
	for _, stale := range []string{"source-durable", "destination-cache"} {
		t.Run(stale, func(t *testing.T) {
			s := newTestStore(t)
			stamp := time.Unix(1700000000, 0)
			for _, e := range []*Entry{
				MakeEntry("source", true, 0, stamp, 10),
				MakeEntry("source/clip.mov", false, 42, stamp, 11),
			} {
				if err := s.Insert(e); err != nil {
					t.Fatal(err)
				}
			}
			source, destination, err := s.CaptureRename("source", "destination")
			if err != nil {
				t.Fatal(err)
			}
			if stale == "source-durable" {
				if err := s.Insert(MakeEntry("source", true, 0, stamp.Add(time.Second), 10)); err != nil {
					t.Fatal(err)
				}
			} else {
				s.InsertToCache(MakeEntry("destination", true, 0, stamp, 20))
			}
			calls := 0
			s.SetOnSubtreeRenamed(func(string, string) { calls++ })
			if _, err := s.RenameExpected(source, destination); !errors.Is(err, ErrMirrorConflict) || calls != 0 {
				t.Fatalf("stale %s rename = %v, callbacks=%d", stale, err, calls)
			}
			if got := s.lookupByPathRAM("source/clip.mov"); got == nil || got.Inode != 11 {
				t.Fatalf("stale %s changed source child: %+v", stale, got)
			}
			if got := s.lookupByPathRAM("destination/clip.mov"); got != nil {
				t.Fatalf("stale %s exposed destination child: %+v", stale, got)
			}
			var durableSource int64
			if err := s.db.QueryRow(`SELECT inode FROM entries WHERE path='source/clip.mov'`).Scan(&durableSource); err != nil || durableSource != 11 {
				t.Fatalf("stale %s durable source inode=%d err=%v", stale, durableSource, err)
			}
		})
	}
}

func TestRenameQueuedCacheOnlyDestinationSuccessorWins(t *testing.T) {
	for _, successor := range []string{"replacement", "unlink"} {
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
				MakeEntry("source", true, 0, stamp, 10),
				MakeEntry("source/clip.mov", false, 42, stamp, 11),
			}, 500); err != nil {
				t.Fatal(err)
			}
			source, destination, err := s.CaptureRename("source", "destination")
			if err != nil {
				t.Fatal(err)
			}
			release, baseline := holdPublicationSQLPool(t, s)
			type result struct {
				n   int
				err error
			}
			renameDone := make(chan result, 1)
			renameSettled := false
			successorDone := make(chan struct{}, 1)
			successorStarted := false
			successorSettled := false
			go func() {
				n, err := s.RenameExpected(source, destination)
				renameDone <- result{n, err}
			}()
			defer func() {
				release()
				if !renameSettled {
					select {
					case <-renameDone:
					case <-time.After(5 * time.Second):
						t.Error("rename did not settle after SQL release")
					}
				}
				if successorStarted && !successorSettled {
					select {
					case <-successorDone:
					case <-time.After(5 * time.Second):
						t.Error("destination successor did not settle")
					}
				}
			}()
			waitForPublicationSQLWait(t, s, baseline)
			if s.cachePublicationMu.TryLock() {
				s.cachePublicationMu.Unlock()
				release()
				t.Fatal("rename did not retain publication gate during SQL")
			}
			started := make(chan struct{})
			go func() {
				close(started)
				if successor == "replacement" {
					s.InsertToCache(MakeEntry("destination/clip.mov", false, 77, stamp.Add(time.Second), 77))
				} else {
					s.DeleteFromCache("destination/clip.mov")
				}
				successorDone <- struct{}{}
			}()
			successorStarted = true
			<-started
			select {
			case <-successorDone:
				successorSettled = true
				t.Fatal("destination successor crossed held rename SQL")
			case <-time.After(25 * time.Millisecond):
			}
			release()
			select {
			case result := <-renameDone:
				renameSettled = true
				if result.err != nil || result.n != 2 {
					t.Fatalf("rename = %+v, want 2 moved", result)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("rename did not finish")
			}
			select {
			case <-successorDone:
				successorSettled = true
			case <-time.After(5 * time.Second):
				t.Fatal("queued destination successor did not finish")
			}
			got := s.lookupByPathRAM("destination/clip.mov")
			if successor == "replacement" {
				if got == nil || got.Inode != 77 || got.Size != 77 {
					t.Fatalf("queued replacement lost: current=%+v", got)
				}
				// Existing handles follow the replacement; lookups return
				// snapshots, so compare identity and values rather than pointers.
				for _, inode := range []uint64{11, 77} {
					handle := s.lookupByInodeRAM(inode)
					if handle == nil || handle.Path != got.Path || handle.Inode != got.Inode || handle.Size != got.Size || handle.Version != got.Version {
						t.Fatalf("replacement handle %d mismatch: path=%+v handle=%+v", inode, got, handle)
					}
				}
				if bytes, files, ok := s.SubtreeSize("destination"); !ok || bytes != 77 || files != 1 {
					t.Fatalf("replacement subtree %d bytes %d files ok=%v", bytes, files, ok)
				}
			} else {
				if got != nil || s.lookupByInodeRAM(11) != nil {
					t.Fatalf("queued unlink lost: path=%+v inode=%+v", got, s.lookupByInodeRAM(11))
				}
				if bytes, files, ok := s.SubtreeSize("destination"); !ok || bytes != 0 || files != 0 {
					t.Fatalf("unlink subtree %d bytes %d files ok=%v", bytes, files, ok)
				}
			}
			if s.lookupByPathRAM("source/clip.mov") != nil {
				t.Fatal("old source child retained after rename")
			}
			var durableInode int64
			if err := s.db.QueryRow(`SELECT inode FROM entries WHERE path='destination/clip.mov'`).Scan(&durableInode); err != nil || durableInode != 11 {
				t.Fatalf("durable rename child inode=%d err=%v", durableInode, err)
			}
		})
	}
}

func TestRenameSQLFailureRollsBackAndCallbackReentersAfterUnlock(t *testing.T) {
	s := newTestStore(t)
	stamp := time.Unix(1700000000, 0)
	for _, e := range []*Entry{
		MakeEntry("source", true, 0, stamp, 10),
		MakeEntry("source/clip.mov", false, 42, stamp, 11),
	} {
		if err := s.Insert(e); err != nil {
			t.Fatal(err)
		}
	}
	source, destination, err := s.CaptureRename("source", "destination")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`CREATE TRIGGER fail_child_move BEFORE INSERT ON entries WHEN NEW.path='destination/clip.mov' BEGIN SELECT RAISE(FAIL,'fault'); END`); err != nil {
		t.Fatal(err)
	}
	callbackCount := 0
	var callbackErr error
	s.SetOnSubtreeRenamed(func(old, new string) {
		callbackCount++
		if !s.writeMu.TryLock() {
			callbackErr = errors.New("callback ran with writeMu held")
			return
		}
		s.writeMu.Unlock()
		if !s.cachePublicationMu.TryLock() {
			callbackErr = errors.New("callback ran with publication gate held")
			return
		}
		s.cachePublicationMu.Unlock()
		if !s.mu.TryLock() {
			callbackErr = errors.New("callback ran with cache mu held")
			return
		}
		s.mu.Unlock()
		if _, err := s.CaptureStoreEpoch(); err != nil {
			callbackErr = err
			return
		}
		if got := s.lookupByPathRAM("destination/clip.mov"); got == nil || got.Inode != 11 {
			callbackErr = errors.New("callback reentry missed committed destination child")
		}
	})
	if _, err := s.RenameExpected(source, destination); err == nil || callbackCount != 0 {
		t.Fatalf("triggered rename = %v, callbacks=%d; want failure/no callback", err, callbackCount)
	}
	for _, lock := range []struct {
		name   string
		try    func() bool
		unlock func()
	}{
		{"writer", s.writeMu.TryLock, s.writeMu.Unlock},
		{"publication", s.cachePublicationMu.TryLock, s.cachePublicationMu.Unlock},
		{"cache", s.mu.TryLock, s.mu.Unlock},
	} {
		if !lock.try() {
			t.Fatalf("%s lock retained after trigger failure", lock.name)
		}
		lock.unlock()
	}
	for _, path := range []string{"source", "source/clip.mov"} {
		if s.lookupByPathRAM(path) == nil || s.lookupByPathSQLite(path) == nil {
			t.Fatalf("trigger failure lost source %q", path)
		}
	}
	if s.lookupByPathRAM("destination") != nil || s.lookupByPathSQLite("destination") != nil {
		t.Fatal("trigger failure exposed destination")
	}
	if _, err := s.db.Exec(`DROP TRIGGER fail_child_move`); err != nil {
		t.Fatal(err)
	}
	n, err := s.RenameExpected(source, destination)
	if err != nil || n != 2 || callbackCount != 1 || callbackErr != nil {
		t.Fatalf("retry rename = %d, %v, callbacks=%d, callback error=%v", n, err, callbackCount, callbackErr)
	}
	var inode int64
	if err := s.db.QueryRow(`SELECT inode FROM entries WHERE path='destination/clip.mov'`).Scan(&inode); err != nil || inode != 11 {
		t.Fatalf("durable retry destination inode=%d err=%v", inode, err)
	}
	if err := s.db.QueryRow(`SELECT inode FROM entries WHERE path='source/clip.mov'`).Scan(&inode); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("retry old source still durable: inode=%d err=%v", inode, err)
	}
}
