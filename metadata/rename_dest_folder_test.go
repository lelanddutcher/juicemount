package metadata

import (
	"path/filepath"
	"testing"
)

// Moving files into a subfolder abroad: while the Mac's backing rename runs,
// a background feed rewrites the destination folder's row (its mtime moved
// when the previous file landed in it). The publication must still succeed;
// failing after the backing rename blocks both names with EIO (review H1 of
// the second #100 review). A folder replaced by another row still fails.
func TestRenameIntoFolderSurvivesItsRefresh(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	for _, e := range []*Entry{MakeEntry("A", true, 0, testTime(0), 2), MakeEntry("A/x.tmp", false, 5, testTime(0), 4)} {
		if err := store.Insert(e); err != nil {
			t.Fatal(err)
		}
	}
	store.SetForegroundOwner(func(p string) bool { return p == "A/x.tmp" || p == "A/x" })
	parent, err := store.CaptureMirror("A")
	if err != nil {
		t.Fatal(err)
	}
	src, dst, err := store.CaptureRename("A/x.tmp", "A/x")
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := store.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ReconcileUpsertBatchOwned([]*Entry{MakeEntry("A", true, 0, testTime(7), 2)}, epoch); err != nil {
		t.Fatal(err)
	}
	if after, _ := store.CaptureMirror("A"); after == parent {
		t.Fatal("test setup: the refresh did not move the folder's revision")
	}
	if _, err := store.RenameExpectedWithParent(src, dst, parent); err != nil {
		t.Fatalf("rename into a refreshed folder: %v", err)
	}

	// The destination folder replaced by a different row (a teammate's
	// rename-over) must still refuse.
	if err := store.Insert(MakeEntry("A/y.tmp", false, 5, testTime(0), 6)); err != nil {
		t.Fatal(err)
	}
	store.SetForegroundOwner(func(p string) bool { return p == "A/y.tmp" || p == "A/y" })
	parent2, err := store.CaptureMirror("A")
	if err != nil {
		t.Fatal(err)
	}
	src2, dst2, err := store.CaptureRename("A/y.tmp", "A/y")
	if err != nil {
		t.Fatal(err)
	}
	other := parent2
	other.Incarnation = parent2.Incarnation + "-replaced"
	if _, err := store.RenameExpectedWithParent(src2, dst2, other); err == nil {
		t.Fatal("rename into a replaced folder: no conflict")
	}
}
