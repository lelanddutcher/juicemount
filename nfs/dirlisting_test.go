package nfs

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/metadata"
)

// The listing record (dirlisting.go) follows its folder: a rename carries it,
// a delete drops it, a re-list that finds the folder gone drops it, the
// in-memory bound never drops a member, and a restart keeps it.

// mirrorBacking inserts the mirror row of an existing FUSE path, with its
// real inode, as a reconcile would have.
func mirrorBacking(t *testing.T, store *metadata.Store, fuseRoot, name string) {
	t.Helper()
	info, err := os.Lstat(filepath.Join(fuseRoot, name))
	if err != nil {
		t.Fatal(err)
	}
	e := metadata.MakeEntry(name, info.IsDir(), info.Size(), info.ModTime(), info.Sys().(*syscall.Stat_t).Ino)
	if err := store.Insert(e); err != nil {
		t.Fatal(err)
	}
}

// recordByStat leaves dir with one mirror row and a record: a Stat cache
// miss of child into a folder the mirror had no rows for.
func recordByStat(t *testing.T, jfs *juiceFS, store *metadata.Store, dir, child string) {
	t.Helper()
	if _, err := jfs.Stat(dir + "/" + child); err != nil {
		t.Fatalf("Stat(%s/%s): %v", dir, child, err)
	}
	if !pollUntil(t, 5*time.Second, mirrorHasNChildren(store, dir, 1)) {
		t.Fatalf("the cache miss did not insert its row into %s (test inconclusive)", dir)
	}
	if !dirRecorded(jfs.handler, dir) {
		t.Fatalf("%s not recorded (test inconclusive)", dir)
	}
}

func durablyRecorded(t *testing.T, store *metadata.Store, dir string) bool {
	t.Helper()
	has, err := store.HasDirListingIncomplete(dir)
	if err != nil {
		t.Fatal(err)
	}
	return has
}

// relistsTo: a readdir of dir re-lists it (a slow link serves the rows it has
// and refreshes in the background) and the next one serves want.
func relistsTo(t *testing.T, jfs *juiceFS, store *metadata.Store, dir string, want ...string) {
	t.Helper()
	if _, err := jfs.ReadDir(dir); err != nil {
		t.Fatal(err)
	}
	if !pollUntil(t, 5*time.Second, mirrorHasNChildren(store, dir, len(want))) {
		kids, _ := store.ListChildren(dir)
		t.Fatalf("%s mirror children = %d, want %d (served its partial rows as the whole folder)", dir, len(kids), len(want))
	}
	if !pollUntil(t, 5*time.Second, inFlightEmpty(jfs.handler)) {
		t.Fatal("listing never finished")
	}
	infos, err := jfs.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	wantNames(t, "ReadDir of "+dir+" after the re-list", infos, want...)
}

var clips = []string{"clip1.mov", "clip2.mov", "clip3.mov"}

// TestRenameCarriesDirListingRecord: renaming a recorded folder, or one of
// its ancestors, moves the record to the new path (so the new path re-lists
// instead of serving one row as the folder) and off the old one.
func TestRenameCarriesDirListingRecord(t *testing.T) {
	for _, tc := range []struct {
		name, from, to, newDir string
	}{
		{"folder", "proj/media", "proj/footage", "proj/footage"},
		{"ancestor", "proj", "moved", "moved/media"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			forceLinkClass(t, netprofile.ClassSlow)
			jfs, store, fuseRoot := newDirRefreshHarness(t)
			mkFUSEDir(t, fuseRoot, "proj/media", clips...)
			mirrorBacking(t, store, fuseRoot, "proj")
			mirrorBacking(t, store, fuseRoot, "proj/media")
			recordByStat(t, jfs, store, "proj/media", "clip1.mov")

			if err := jfs.Rename(tc.from, tc.to); err != nil {
				t.Fatalf("Rename(%s, %s): %v", tc.from, tc.to, err)
			}
			if dirRecorded(jfs.handler, "proj/media") || durablyRecorded(t, store, "proj/media") {
				t.Fatal("the old path kept the record")
			}
			if !dirRecorded(jfs.handler, tc.newDir) || !durablyRecorded(t, store, tc.newDir) {
				t.Fatalf("the record did not move to %s", tc.newDir)
			}
			if kids, _ := store.ListChildren(tc.newDir); len(kids) != 1 {
				t.Fatalf("%s mirror children after rename = %d, want 1 (test inconclusive)", tc.newDir, len(kids))
			}
			relistsTo(t, jfs, store, tc.newDir, clips...)
		})
	}
}

// TestRemoveDropsDirListingRecord: deleting a recorded folder drops its
// record, so it neither leaks nor marks a folder later made at that name.
func TestRemoveDropsDirListingRecord(t *testing.T) {
	forceLinkClass(t, netprofile.ClassSlow)
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	mkFUSEDir(t, fuseRoot, "d", "a.txt")
	mirrorBacking(t, store, fuseRoot, "d")
	recordByStat(t, jfs, store, "d", "a.txt")

	if err := jfs.Remove("d/a.txt"); err != nil {
		t.Fatal(err)
	}
	if err := jfs.Remove("d"); err != nil {
		t.Fatal(err)
	}
	if dirRecorded(jfs.handler, "d") || durablyRecorded(t, store, "d") {
		t.Fatal("the deleted folder kept its record")
	}
}

// TestRelistOfGoneFolderDropsRecord: a recorded folder removed on the server
// (another Mac) fails its re-list with ENOENT. The record must go, or every
// readdir of the leftover rows re-lists it again, forever.
func TestRelistOfGoneFolderDropsRecord(t *testing.T) {
	forceLinkClass(t, netprofile.ClassSlow)
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	mkFUSEDir(t, fuseRoot, "d", "a.txt", "b.txt")
	recordByStat(t, jfs, store, "d", "a.txt")
	if err := os.RemoveAll(filepath.Join(fuseRoot, "d")); err != nil {
		t.Fatal(err)
	}

	if _, err := jfs.ReadDir("d"); err != nil {
		t.Fatal(err)
	}
	if !pollUntil(t, 5*time.Second, func() bool { return !dirRecorded(jfs.handler, "d") }) {
		t.Fatal("a re-list that found the folder gone kept the record")
	}
	if !pollUntil(t, 5*time.Second, inFlightEmpty(jfs.handler)) {
		t.Fatal("re-list never finished")
	}
	if durablyRecorded(t, store, "d") {
		t.Fatal("the gone folder kept its durable record")
	}
}

// TestDirListingBoundKeepsMembers: a record that leaves memory at the bound
// stays a member, so its folder still re-lists instead of serving its one row
// as the whole folder.
func TestDirListingBoundKeepsMembers(t *testing.T) {
	forceLinkClass(t, netprofile.ClassSlow)
	prev := dirListingMaxTracked
	dirListingMaxTracked = 2
	t.Cleanup(func() { dirListingMaxTracked = prev })
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	dirs := []string{"f0", "f1", "f2"}
	for _, d := range dirs {
		mkFUSEDir(t, fuseRoot, d, clips...)
		if _, err := jfs.Stat(d + "/clip1.mov"); err != nil {
			t.Fatal(err)
		}
		if !pollUntil(t, 5*time.Second, mirrorHasNChildren(store, d, 1)) {
			t.Fatalf("the cache miss did not insert its row into %s (test inconclusive)", d)
		}
	}
	jfs.handler.dirRefreshMu.Lock()
	inMemory := len(jfs.handler.dirIncomplete)
	jfs.handler.dirRefreshMu.Unlock()
	if inMemory != 2 {
		t.Fatalf("records in memory = %d, want 2 (the bound; test inconclusive)", inMemory)
	}
	for _, d := range dirs {
		relistsTo(t, jfs, store, d, clips...)
	}
}

// TestDirListingRecordSurvivesRestart: after a restart, a recorded folder
// still re-lists instead of serving its partial rows as the whole folder.
func TestDirListingRecordSurvivesRestart(t *testing.T) {
	forceLinkClass(t, netprofile.ClassSlow)
	// The harness supplies clean global state and the FUSE root; the test
	// runs its own store at a path it can reopen.
	_, _, fuseRoot := newDirRefreshHarness(t)
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	open := func() (*juiceFS, *metadata.Store) {
		store, err := metadata.Open(dbPath)
		if err != nil {
			t.Fatalf("metadata.Open: %v", err)
		}
		return &juiceFS{handler: NewHandler(store, fuseRoot)}, store
	}
	mkFUSEDir(t, fuseRoot, "proj/media", clips...)

	first, store := open()
	recordByStat(t, first, store, "proj/media", "clip1.mov")
	first.handler.StopHandler()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	second, store := open()
	t.Cleanup(func() {
		second.handler.StopHandler()
		_ = store.Close()
	})
	if !dirRecorded(second.handler, "proj/media") {
		t.Fatal("the record did not survive the restart")
	}
	relistsTo(t, second, store, "proj/media", clips...)
	if durablyRecorded(t, store, "proj/media") {
		t.Fatal("the complete re-list did not clear the durable record")
	}
}

// TestIncompletePassRecordedBeforeRows: an incomplete pass writes its record
// before it publishes the rows it reached. Observed the instant the rows are
// committed (where a kill would freeze the DB), the record must already be
// durable, or a restart serves those rows as the whole folder.
func TestIncompletePassRecordedBeforeRows(t *testing.T) {
	for _, async := range []string{"1", "0"} {
		t.Run("JM_ASYNC_DIR_REFRESH="+async, func(t *testing.T) {
			forceLinkClass(t, netprofile.ClassSlow)
			fastRetry(t)
			wedged, _ := wedgeChild(t, "b.txt", 1)
			jfs, store, fuseRoot := newDirRefreshHarness(t)
			t.Setenv("JM_ASYNC_DIR_REFRESH", async)
			mkFUSEDir(t, fuseRoot, "d", "a.txt", "b.txt", "c.txt")
			seen := make(chan bool, 1)
			coldListingPublishedHook = func(dir string) {
				if dir != "d" {
					return
				}
				kids, _ := store.ListChildren("d")
				has, _ := store.HasDirListingIncomplete("d")
				select {
				case seen <- len(kids) == 2 && has:
				default:
				}
			}
			t.Cleanup(func() { coldListingPublishedHook = nil })

			if _, err := jfs.ReadDir("d"); err != nil {
				t.Fatal(err)
			}
			select {
			case ok := <-seen:
				if !ok {
					t.Fatal("the partial rows were committed before the folder's record")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the listing pass never published")
			}
			if wedged() != 1 {
				t.Fatal("the pass never listed the wedged child (test inconclusive)")
			}
			if !pollUntil(t, 5*time.Second, inFlightEmpty(jfs.handler)) {
				t.Fatal("refresh never finished")
			}
		})
	}
}

// TestDirListingBoundKeepsParkedRecord: at the bound, the record of a folder
// whose earlier stat is still hung must stay in memory. Dropped to the DB
// alone, its parked call is forgotten and every readdir re-lists it, each
// pass pinning another of prefetchGate's two slots behind the same child.
func TestDirListingBoundKeepsParkedRecord(t *testing.T) {
	forceLinkClass(t, netprofile.ClassSlow)
	fastRetry(t)
	prev := dirListingMaxTracked
	dirListingMaxTracked = 1
	t.Cleanup(func() { dirListingMaxTracked = prev })
	_, _ = wedgeChild(t, "b.txt", 1<<30) // hung for the whole test
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	h := jfs.handler
	mkFUSEDir(t, fuseRoot, "hung", "a.txt", "b.txt")
	mkFUSEDir(t, fuseRoot, "other", clips...)

	if _, err := jfs.ReadDir("hung"); err != nil {
		t.Fatal(err)
	}
	if !pollUntil(t, 5*time.Second, inFlightEmpty(h)) {
		t.Fatal("refresh of the hung folder never returned")
	}
	if !dirRecorded(h, "hung") || len(prefetchGate) != 1 {
		t.Fatalf("hung folder recorded=%v, prefetchGate=%d (test inconclusive)", dirRecorded(h, "hung"), len(prefetchGate))
	}
	recordByStat(t, jfs, store, "other", "clip1.mov") // the second record meets the bound

	for i := 0; i < 3; i++ {
		time.Sleep(3 * dirListingRetryBase)
		if _, err := jfs.ReadDir("hung"); err != nil {
			t.Fatal(err)
		}
		if !pollUntil(t, 5*time.Second, inFlightEmpty(h)) {
			t.Fatal("refresh of the hung folder never returned")
		}
	}
	if n := len(prefetchGate); n != 1 {
		t.Fatalf("prefetchGate occupancy = %d, want 1 (the hung folder was re-listed past its parked stat)", n)
	}
}

// TestDirListingSpillResets: once the DB holds fewer records than the bound,
// they are all back in memory and READDIR stops consulting SQLite.
func TestDirListingSpillResets(t *testing.T) {
	prev := dirListingMaxTracked
	dirListingMaxTracked = 2
	t.Cleanup(func() { dirListingMaxTracked = prev })
	jfs, _, _ := newDirRefreshHarness(t)
	h := jfs.handler
	for _, d := range []string{"f0", "f1", "f2"} {
		h.markDirListingUnknown(d)
	}
	if !h.dirIncompleteSpilled.Load() {
		t.Fatal("three records under a bound of two did not spill (test inconclusive)")
	}
	var kept string
	for _, d := range []string{"f0", "f1", "f2"} {
		if dirRecorded(h, d) {
			kept = d
			break
		}
	}
	h.clearDirListingIncomplete(kept)
	h.clearDirListingIncomplete(map[string]string{"f0": "f1", "f1": "f2", "f2": "f0"}[kept])
	if h.dirIncompleteSpilled.Load() {
		t.Fatal("spilled stayed set with one durable record under a bound of two")
	}
	n := 0
	for _, d := range []string{"f0", "f1", "f2"} {
		if dirRecorded(h, d) {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("records in memory after the reset = %d, want 1 (the remaining durable record)", n)
	}
}
