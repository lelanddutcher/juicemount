//go:build h8aacceptance

package metadata

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestH8AScopedPruneConflictDoesNotInvalidateSurvivingWinner(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	mt := time.Unix(1700000000, 0)
	for _, e := range []*Entry{
		MakeEntry("proj", true, 0, mt, 10),
		MakeEntry("proj/shoot", true, 0, mt, 20),
		MakeEntry("proj/shoot/old.mov", false, 12, mt, 21),
	} {
		if err := store.Insert(e); err != nil {
			t.Fatal(err)
		}
	}
	var callbacks []uint64
	store.SetOnContentInvalidated(func(path string, isDir bool) {
		if path == "proj/shoot/old.mov" && !isDir {
			if current := store.LookupByPath(path); current != nil {
				callbacks = append(callbacks, current.Inode)
			}
		}
	})
	store.SetOnScanContentInvalidated(func(path string, oldInode uint64) {
		if path == "proj/shoot/old.mov" {
			callbacks = append(callbacks, oldInode)
		}
	})
	rc := &RedisClient{store: store}
	markPruneHealthy(rc)
	replaced := false
	rc.SetSpoolGuard(func(path string) bool {
		if path == "proj/shoot/old.mov" && !replaced {
			replaced = true
			if err := store.Insert(MakeEntry(path, false, 34, mt.Add(time.Second), 31)); err != nil {
				t.Errorf("insert foreground winner: %v", err)
			}
		}
		return false
	})
	token := rc.scanToken()
	err = rc.scopedPruneWithToken("proj", map[string]struct{}{}, &token)
	if !replaced {
		t.Fatal("replacement hook did not run after scoped-prune observation")
	}
	if !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("scoped prune = %v, want ErrMirrorConflict", err)
	}
	if len(callbacks) != 0 {
		t.Fatalf("rejected scoped prune invalidated content for observed inodes %v; want no callback", callbacks)
	}
	if got := store.LookupByPath("proj/shoot/old.mov"); got == nil || got.Inode != 31 || got.Size != 34 {
		t.Fatalf("conflicted scoped prune lost foreground winner: %+v", got)
	}
}

func TestH8AAdmittedScopedPruneInvalidatesExactOldInode(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	mt := time.Unix(1700000000, 0)
	for _, e := range []*Entry{
		MakeEntry("proj", true, 0, mt, 10),
		MakeEntry("proj/shoot", true, 0, mt, 20),
		MakeEntry("proj/shoot/old.mov", false, 12, mt, 21),
	} {
		if err := store.Insert(e); err != nil {
			t.Fatal(err)
		}
	}
	var observed []uint64
	store.SetOnContentInvalidated(func(path string, isDir bool) {
		if path == "proj/shoot/old.mov" && !isDir {
			if old := store.LookupByPath(path); old != nil {
				observed = append(observed, old.Inode)
			}
		}
	})
	store.SetOnScanContentInvalidated(func(path string, oldInode uint64) {
		if path == "proj/shoot/old.mov" {
			observed = append(observed, oldInode)
		}
	})
	rc := &RedisClient{store: store}
	markPruneHealthy(rc)
	token := rc.scanToken()
	if err := rc.scopedPruneWithToken("proj", map[string]struct{}{}, &token); err != nil {
		t.Fatalf("scopedPruneWithToken: %v", err)
	}
	if len(observed) != 1 || observed[0] != 21 {
		t.Fatalf("admitted prune callback observed inodes %v; want exactly [21]", observed)
	}
	if got := store.LookupByPath("proj/shoot/old.mov"); got != nil {
		t.Fatalf("admitted prune retained file: %+v", got)
	}
}
