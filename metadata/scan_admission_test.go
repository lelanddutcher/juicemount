package metadata

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestScanDeleteBatchRejectsWriterBetweenChunks(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	mt := time.Unix(1700000000, 0)
	paths := make([]string, scanMutationChunk+1)
	for i := range paths {
		paths[i] = fmt.Sprintf("old-%03d.mov", i)
		if err := s.Insert(MakeEntry(paths[i], false, 12, mt, uint64(i+10))); err != nil {
			t.Fatal(err)
		}
	}
	epoch, err := s.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	epoch, err = s.ScanDeletePathsBatch(paths[:scanMutationChunk], epoch)
	if err != nil {
		t.Fatal(err)
	}
	replacement := MakeEntry(paths[scanMutationChunk], false, 8192, mt.Add(time.Second), 9000)
	if err := s.Insert(replacement); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ScanDeletePathsBatch(paths[scanMutationChunk:], epoch); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("second prune chunk = %v, want mirror conflict", err)
	}
	if got := s.LookupByPath(replacement.Path); got == nil || got.Inode != replacement.Inode {
		t.Fatalf("replacement after conflict = %+v", got)
	}
	var inode int64
	if err := s.DB().QueryRow(`SELECT inode FROM entries WHERE path=?`, replacement.Path).Scan(&inode); err != nil || uint64(inode) != replacement.Inode {
		t.Fatalf("durable replacement inode=%d err=%v", inode, err)
	}
}

func TestScanClearLocalOnlyBatchRejectsUnchangedRow(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := MakeEntry("media.mov", false, 12, time.Unix(1700000000, 0), 10)
	if err := s.Insert(e); err != nil {
		t.Fatal(err)
	}
	before := s.LookupByPath(e.Path)
	if before == nil {
		t.Fatal("inserted row missing")
	}
	beforeValue := *before
	epoch, err := s.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ScanClearLocalOnlyBatch([]string{e.Path}, epoch); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("clear non-local row = %v, want mirror conflict", err)
	}
	if got := s.LookupByPath(e.Path); got == nil || got.Version != beforeValue.Version || got.LocalOnly != beforeValue.LocalOnly {
		t.Fatalf("unchanged row after rejected clear = %+v", got)
	}
}

func TestScanDeleteBatchRefusesNonemptyDirectory(t *testing.T) {
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
			if err := s.Insert(MakeEntry("dir", true, 0, mt, 10)); err != nil {
				t.Fatal(err)
			}
			epoch, err := s.CaptureStoreEpoch()
			if err != nil {
				t.Fatal(err)
			}
			child := MakeEntry("dir/child.mov", false, 12, mt, 11)
			if cacheOnly {
				s.InsertToCache(child)
				epoch, err = s.CaptureStoreEpoch()
				if err != nil {
					t.Fatal(err)
				}
			} else if err := s.Insert(child); err != nil {
				t.Fatal(err)
			} else {
				epoch, err = s.CaptureStoreEpoch()
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.ScanDeletePathsBatch([]string{"dir"}, epoch); !errors.Is(err, ErrMirrorConflict) {
				t.Fatalf("nonempty prune = %v, want mirror conflict", err)
			}
			if s.LookupByPath("dir") == nil || s.LookupByPath(child.Path) == nil {
				t.Fatal("directory or child disappeared after refused prune")
			}
		})
	}
}
