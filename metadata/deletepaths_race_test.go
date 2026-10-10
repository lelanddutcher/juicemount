package metadata

import (
	"path/filepath"
	"testing"
	"time"
)

func TestDeletePathsDoesNotEvictReplacementInsertedAfterCommit(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "metadata.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Second)
	if err := s.Insert(MakeEntry("same/path", false, 42, now, 42)); err != nil {
		t.Fatal(err)
	}

	atGap := make(chan struct{})
	release := make(chan struct{})
	s.deletePathsAfterCommitHook = func() {
		close(atGap)
		<-release
	}
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- s.DeletePaths([]string{"same/path"}) }()

	<-atGap
	insertStarted := make(chan struct{})
	insertDone := make(chan error, 1)
	go func() {
		close(insertStarted)
		insertDone <- s.Insert(MakeEntry("same/path", false, 43, now, 43))
	}()
	<-insertStarted
	select {
	case err := <-insertDone:
		close(release)
		t.Fatalf("Insert completed before DeletePaths cache cleanup: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-deleteDone; err != nil {
		t.Fatal(err)
	}
	if err := <-insertDone; err != nil {
		t.Fatal(err)
	}

	got := s.lookupByPathRAM("same/path")
	if got == nil || got.Inode != 43 || got.Size != 43 {
		t.Fatalf("cache replacement = %+v, want inode=43 size=43", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got = reopened.LookupByPath("same/path")
	if got == nil || got.Inode != 43 || got.Size != 43 {
		t.Fatalf("reopened replacement = %+v, want inode=43 size=43", got)
	}
}

func TestDeletePathsSerializesBulkInsertAbsentThroughCacheCleanup(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "metadata.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now().Truncate(time.Second)
	if err := s.Insert(MakeEntry("same/path", false, 42, now, 42)); err != nil {
		t.Fatal(err)
	}

	atGap := make(chan struct{})
	release := make(chan struct{})
	s.deletePathsAfterCommitHook = func() {
		close(atGap)
		<-release
	}
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- s.DeletePaths([]string{"same/path"}) }()
	<-atGap

	bulkStarted := make(chan struct{})
	bulkDone := make(chan error, 1)
	go func() {
		close(bulkStarted)
		bulkDone <- s.BulkInsertAbsent([]*Entry{MakeEntry("same/path", false, 43, now, 43)}, 1)
	}()
	<-bulkStarted
	select {
	case err := <-bulkDone:
		close(release)
		t.Fatalf("BulkInsertAbsent completed before DeletePaths cache cleanup: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-deleteDone; err != nil {
		t.Fatal(err)
	}
	if err := <-bulkDone; err != nil {
		t.Fatal(err)
	}

	got := s.lookupByPathRAM("same/path")
	if got == nil || got.Inode != 43 || got.Size != 43 {
		t.Fatalf("cache replacement = %+v, want inode=43 size=43", got)
	}
	var inode, size int64
	if err := s.db.QueryRow(`SELECT inode, size FROM entries WHERE path = ?`, "same/path").Scan(&inode, &size); err != nil {
		t.Fatal(err)
	}
	if inode != 43 || size != 43 {
		t.Fatalf("durable replacement = inode %d size %d, want inode=43 size=43", inode, size)
	}
}

func TestDeletePathsRemovesUnreplacedCacheEntry(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Insert(MakeEntry("same/path", false, 42, time.Now().Truncate(time.Second), 42)); err != nil {
		t.Fatal(err)
	}
	if err := s.DeletePaths([]string{"same/path"}); err != nil {
		t.Fatal(err)
	}
	if got := s.lookupByPathRAM("same/path"); got != nil {
		t.Fatalf("deleted path remains in cache: %+v", got)
	}
}

func TestDeletePathsPreservesCacheOnlyReplacementAndNewVisibility(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Now().Truncate(time.Second)
	if err := s.Insert(MakeEntry("same/path", false, 42, now, 42)); err != nil {
		t.Fatal(err)
	}
	atGap := make(chan struct{})
	release := make(chan struct{})
	s.deletePathsAfterCommitHook = func() {
		close(atGap)
		<-release
	}
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- s.DeletePaths([]string{"same/path"}) }()
	<-atGap

	s.NoteDirectoryChanged("same/path")
	replacement := MakeEntry("same/path", false, 43, now, 43)
	s.InsertToCache(replacement)
	visible := s.DirectoryVisibilityMtime("same/path", time.Time{})
	close(release)
	if err := <-deleteDone; err != nil {
		t.Fatal(err)
	}

	got := s.lookupByPathRAM("same/path")
	if got == nil || got.Inode != 43 || got.Size != 43 {
		t.Fatalf("cache-only replacement = %+v, want inode=43 size=43", got)
	}
	if got := s.DirectoryVisibilityMtime("same/path", time.Time{}); !got.Equal(visible) {
		t.Fatalf("directory visibility = %v, want preserved %v", got, visible)
	}
}
