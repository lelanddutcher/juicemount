package metadata

import (
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestOpenClose(t *testing.T) {
	s := newTestStore(t)
	count, err := s.Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expected 0 entries, got %d", count)
	}
}

func TestInsertAndLookup(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	e := MakeEntry("project/video.mov", false, 1024*1024*50, now, 100)
	if err := s.Insert(e); err != nil {
		t.Fatal(err)
	}

	// Lookup by inode
	got := s.LookupByInode(100)
	if got == nil {
		t.Fatal("LookupByInode returned nil")
	}
	if got.Path != "project/video.mov" {
		t.Fatalf("path = %q, want %q", got.Path, "project/video.mov")
	}
	if got.Size != 1024*1024*50 {
		t.Fatalf("size = %d, want %d", got.Size, 1024*1024*50)
	}
	if got.IsDir {
		t.Fatal("expected file, got dir")
	}

	// Lookup by path
	got2 := s.LookupByPath("project/video.mov")
	if got2 == nil {
		t.Fatal("LookupByPath returned nil")
	}
	if got2.Inode != 100 {
		t.Fatalf("inode = %d, want 100", got2.Inode)
	}
}

func TestInsertDir(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	dir := MakeEntry("project", true, 0, now, 50)
	if err := s.Insert(dir); err != nil {
		t.Fatal(err)
	}

	got := s.LookupByPath("project")
	if got == nil || !got.IsDir {
		t.Fatal("expected dir")
	}
	if !got.Mode.IsDir() {
		t.Fatal("mode should indicate directory")
	}
}

func TestUpdate(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	e := MakeEntry("file.txt", false, 100, now, 1)
	if err := s.Insert(e); err != nil {
		t.Fatal(err)
	}

	later := now.Add(5 * time.Minute)
	if err := s.UpdateSize("file.txt", 200, later); err != nil {
		t.Fatal(err)
	}

	got := s.LookupByPath("file.txt")
	if got.Size != 200 {
		t.Fatalf("size = %d, want 200", got.Size)
	}
	if !got.Mtime.Equal(later) {
		t.Fatalf("mtime = %v, want %v", got.Mtime, later)
	}
}

func TestDelete(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	e := MakeEntry("temp.txt", false, 100, now, 1)
	if err := s.Insert(e); err != nil {
		t.Fatal(err)
	}

	if err := s.Delete("temp.txt"); err != nil {
		t.Fatal(err)
	}

	if got := s.LookupByPath("temp.txt"); got != nil {
		t.Fatal("expected nil after delete")
	}
	if got := s.LookupByInode(1); got != nil {
		t.Fatal("expected nil inode after delete")
	}
}

func TestListChildren(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	// Parent dir
	s.Insert(MakeEntry("project", true, 0, now, 10))

	// Children
	for i := 0; i < 5; i++ {
		s.Insert(MakeEntry(fmt.Sprintf("project/file_%d.mov", i), false, int64(i*1000), now, uint64(100+i)))
	}

	// Another dir's children (should not appear)
	s.Insert(MakeEntry("other", true, 0, now, 20))
	s.Insert(MakeEntry("other/nope.txt", false, 0, now, 200))

	children, err := s.ListChildren("project")
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 5 {
		t.Fatalf("expected 5 children, got %d", len(children))
	}
}

func TestBulkInsert(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	entries := make([]*Entry, 5000)
	for i := range entries {
		entries[i] = MakeEntry(
			fmt.Sprintf("bulk/file_%05d.dat", i),
			false, int64(i*100), now, uint64(1000+i),
		)
	}

	start := time.Now()
	if err := s.BulkInsert(entries, 5000); err != nil {
		t.Fatal(err)
	}
	dur := time.Since(start)

	count, _ := s.Count()
	if count != 5000 {
		t.Fatalf("count = %d, want 5000", count)
	}

	// Verify random lookups
	got := s.LookupByInode(1000 + 2500)
	if got == nil || got.Path != "bulk/file_02500.dat" {
		t.Fatalf("lookup inode 3500 failed: %v", got)
	}

	t.Logf("BulkInsert 5000 entries: %v", dur)
}

func TestBulkInsertBatched(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	entries := make([]*Entry, 12000)
	for i := range entries {
		entries[i] = MakeEntry(
			fmt.Sprintf("batched/file_%05d.dat", i),
			false, int64(i), now, uint64(10000+i),
		)
	}

	if err := s.BulkInsert(entries, 5000); err != nil {
		t.Fatal(err)
	}

	count, _ := s.Count()
	if count != 12000 {
		t.Fatalf("count = %d, want 12000", count)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	// Insert seed data
	for i := 0; i < 100; i++ {
		s.Insert(MakeEntry(fmt.Sprintf("concurrent/%d.txt", i), false, int64(i), now, uint64(i+1)))
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 200)

	// 10 concurrent readers
	for r := 0; r < 10; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				e := s.LookupByInode(uint64(i%100) + 1)
				if e == nil {
					errCh <- fmt.Errorf("inode %d not found", i%100+1)
					return
				}
			}
		}()
	}

	// 2 concurrent writers
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				e := MakeEntry(
					fmt.Sprintf("concurrent/writer_%d_%d.txt", id, i),
					false, int64(i), now, uint64(1000+id*100+i),
				)
				if err := s.Insert(e); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatal(err)
	}
}

func TestLocalOnlyFlag(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	e := MakeEntry("local/new_file.mov", false, 1024, now, 500)
	e.LocalOnly = true
	if err := s.Insert(e); err != nil {
		t.Fatal(err)
	}

	// Should appear in local-only list
	locals, err := s.LocalOnlyEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(locals) != 1 {
		t.Fatalf("expected 1 local_only entry, got %d", len(locals))
	}
	if !locals[0].LocalOnly {
		t.Fatal("expected LocalOnly=true")
	}

	// Clear the flag
	if err := s.ClearLocalOnly("local/new_file.mov"); err != nil {
		t.Fatal(err)
	}

	locals2, _ := s.LocalOnlyEntries()
	if len(locals2) != 0 {
		t.Fatalf("expected 0 local_only entries after clear, got %d", len(locals2))
	}

	// Entry still exists
	got := s.LookupByPath("local/new_file.mov")
	if got == nil {
		t.Fatal("entry should still exist after clearing local_only")
	}
	if got.LocalOnly {
		t.Fatal("LocalOnly should be false")
	}
}

func TestAllPathsAndDeletePaths(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	// Insert some regular and some local-only entries
	s.Insert(MakeEntry("a.txt", false, 10, now, 1))
	s.Insert(MakeEntry("b.txt", false, 20, now, 2))
	local := MakeEntry("c.txt", false, 30, now, 3)
	local.LocalOnly = true
	s.Insert(local)

	// AllPaths should only return non-local entries
	paths, err := s.AllPaths()
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 {
		t.Fatalf("expected 2 paths, got %d", len(paths))
	}
	if _, ok := paths["c.txt"]; ok {
		t.Fatal("local_only entry should not be in AllPaths")
	}

	// Delete multiple paths
	if err := s.DeletePaths([]string{"a.txt", "b.txt"}); err != nil {
		t.Fatal(err)
	}

	count, _ := s.Count()
	if count != 1 { // only the local_only entry remains
		t.Fatalf("expected 1 entry remaining, got %d", count)
	}
}

func TestFileInfo(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	e := MakeEntry("project/clip.mp4", false, 50*1024*1024, now, 42)
	fi := e.FileInfo()

	if fi.Name() != "clip.mp4" {
		t.Fatalf("Name = %q", fi.Name())
	}
	if fi.Size() != 50*1024*1024 {
		t.Fatalf("Size = %d", fi.Size())
	}
	if fi.IsDir() {
		t.Fatal("should not be dir")
	}
	if !fi.ModTime().Equal(now) {
		t.Fatalf("ModTime mismatch")
	}
	// Sys() returns a *syscall.Stat_t with the entry's inode and current user's UID/GID
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("Sys should return *syscall.Stat_t")
	}
	if st.Ino != e.Inode {
		t.Fatalf("Sys().Ino = %d, want %d", st.Ino, e.Inode)
	}
}

// TestConcurrentBulkInsertWithRename reproduces the SQLITE_BUSY bug:
// BulkInsert holds the write lock in large batches while a concurrent
// rename (Insert+Delete) tries to write. The in-memory cache must always
// reflect the rename immediately, even if SQLite is temporarily locked.
func TestConcurrentBulkInsertWithRename(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	// Seed a directory with entries (simulates existing metadata)
	for i := 0; i < 100; i++ {
		s.Insert(MakeEntry(fmt.Sprintf("project/file_%03d.mov", i), false, int64(i*1000), now, uint64(100+i)))
	}

	// Create the "untitled folder" that Finder will rename
	untitled := MakeEntry("project/untitled folder", true, 0, now, 999)
	s.Insert(untitled)

	var wg sync.WaitGroup
	errCh := make(chan error, 10)

	// Goroutine 1: BulkInsert simulating reconciliation (large batch)
	wg.Add(1)
	go func() {
		defer wg.Done()
		bulk := make([]*Entry, 5000)
		for i := range bulk {
			bulk[i] = MakeEntry(
				fmt.Sprintf("bulk/reconcile_%05d.dat", i),
				false, int64(i), now, uint64(10000+i),
			)
		}
		if err := s.BulkInsert(bulk, 500); err != nil {
			errCh <- fmt.Errorf("BulkInsert: %w", err)
		}
	}()

	// Goroutine 2: Rename "untitled folder" → "My Project" (simulates Finder rename)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Update in-memory cache first (the fix)
		s.DeleteFromCache("project/untitled folder")
		renamed := MakeEntry("project/My Project", true, 0, now, 999)
		s.InsertToCache(renamed)

		// SQLite writes (may be blocked by BulkInsert)
		s.Delete("project/untitled folder")
		if err := s.Insert(renamed); err != nil {
			errCh <- fmt.Errorf("rename Insert: %w", err)
		}
	}()

	// Goroutine 3: Concurrent lookups (simulates NFS GETATTR/LOOKUP)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// The renamed entry must be visible in the in-memory cache
		// even while BulkInsert holds the SQLite lock
		for i := 0; i < 50; i++ {
			// Old name should not be found
			if old := s.LookupByPath("project/untitled folder"); old != nil {
				// It's OK if we see it briefly before the rename goroutine runs
				continue
			}
			// New name should be found after rename
			if renamed := s.LookupByPath("project/My Project"); renamed != nil {
				if renamed.Inode != 999 {
					errCh <- fmt.Errorf("renamed entry has wrong inode: %d", renamed.Inode)
					return
				}
				return // success
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatal(err)
	}

	// Verify final state: renamed entry exists, old one doesn't
	if got := s.LookupByPath("project/untitled folder"); got != nil {
		t.Fatal("old name should not exist after rename")
	}
	got := s.LookupByPath("project/My Project")
	if got == nil {
		t.Fatal("renamed entry should exist")
	}
	if got.Inode != 999 {
		t.Fatalf("inode = %d, want 999", got.Inode)
	}
}

// TestListChildrenFromCache verifies ListChildren uses in-memory cache,
// not SQLite, so it's never blocked by concurrent BulkInsert.
func TestListChildrenFromCache(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	// Insert parent + children
	s.Insert(MakeEntry("project", true, 0, now, 10))
	for i := 0; i < 5; i++ {
		s.Insert(MakeEntry(fmt.Sprintf("project/file_%d.mov", i), false, int64(i*1000), now, uint64(100+i)))
	}

	// Add an entry only to cache (not SQLite) — simulates a just-created folder
	cacheOnly := MakeEntry("project/new_folder", true, 0, now, 999)
	s.InsertToCache(cacheOnly)

	children, err := s.ListChildren("project")
	if err != nil {
		t.Fatal(err)
	}
	// Should include the cache-only entry
	found := false
	for _, c := range children {
		if c.Path == "project/new_folder" {
			found = true
		}
	}
	if !found {
		t.Fatal("ListChildren should include cache-only entries")
	}
	if len(children) != 6 {
		t.Fatalf("expected 6 children (5 files + 1 cache-only), got %d", len(children))
	}
}

func TestInsertOrReplace(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	e1 := MakeEntry("dup.txt", false, 100, now, 1)
	s.Insert(e1)

	// Re-insert with different size (same path = replace)
	e2 := MakeEntry("dup.txt", false, 200, now.Add(time.Minute), 1)
	s.Insert(e2)

	count, _ := s.Count()
	if count != 1 {
		t.Fatalf("count = %d, want 1 (replace, not duplicate)", count)
	}

	got := s.LookupByPath("dup.txt")
	if got.Size != 200 {
		t.Fatalf("size = %d, want 200 after replace", got.Size)
	}
}
