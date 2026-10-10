package metadata

import (
	"path/filepath"
	"testing"
	"time"
)

// BulkInsertAbsent commits its durable batches before this production helper
// runs. Hold the final SELECT itself; holding SQL before the public call only
// exercises the earlier transaction, which already leaves the cache lock free.
func TestAbsentPublicationSQLWaitKeepsCachedNavigationResponsive(t *testing.T) {
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
		MakeEntry("cold/target.mov", false, 42, stamp, 100),
	}, 500); err != nil {
		t.Fatal(err)
	}
	s.DeleteFromCache("cold/target.mov")
	release, baseline := holdPublicationSQLPool(t, s)
	publicationDone := make(chan error, 1)
	settled := false
	go func() {
		// A stale warmer snapshot must never replace the actual durable row.
		publicationDone <- s.publishAbsentEntries([]*Entry{MakeEntry("cold/target.mov", false, 1, stamp, 999)})
	}()
	defer func() {
		release()
		if !settled {
			select {
			case err := <-publicationDone:
				if err != nil {
					t.Errorf("final publication: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("publication did not settle after SQL release")
			}
		}
	}()
	waitForPublicationSQLWait(t, s, baseline)
	if s.mu.TryLock() {
		s.mu.Unlock()
	} else {
		t.Error("absent publication retains the serving lock while waiting for SQL")
	}
	type navigation struct {
		path, inode *Entry
		children    []*Entry
		err         error
		elapsed     time.Duration
	}
	navDone := make(chan navigation, 1)
	go func() {
		started := time.Now()
		path := s.LookupByPath("known/clip.mov")
		inode := s.LookupByInode(11)
		children, err := s.ListChildrenForReadDir("known")
		navDone <- navigation{path, inode, children, err, time.Since(started)}
	}()
	var got navigation
	select {
	case got = <-navDone:
		t.Logf("cached path/inode/list completed before SQL release: %s", got.elapsed)
	case <-time.After(250 * time.Millisecond):
		t.Error("cached navigation waited >250ms behind absent-publication SQL")
		release()
		select {
		case got = <-navDone:
		case <-time.After(5 * time.Second):
			t.Fatal("cached navigation did not settle after SQL release")
		}
	}
	if got.err != nil || got.path == nil || got.path.Size != 4096 || got.inode == nil || got.inode.Path != "known/clip.mov" || len(got.children) != 1 || got.children[0].Inode != 11 {
		t.Fatalf("incorrect cached navigation: %+v", got)
	}
	release()
	select {
	case err := <-publicationDone:
		settled = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("final publication did not complete")
	}
	if e := s.lookupByPathRAM("cold/target.mov"); e == nil || e.Inode != 100 || e.Size != 42 || e.Version.IsZero() {
		t.Fatalf("published stale input instead of committed row: %+v", e)
	}
}
