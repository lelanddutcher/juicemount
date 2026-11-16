package nfs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// Laptop bug B1 (2026-11-14): abroad, an in-place save's backing write and
// sync take seconds. Meanwhile the background feeds publish the write's own
// echo (the attr push's upsert, the pub/sub echo of the previous WRITE, the
// d-key prune). The foreground completion CAS then failed after the bytes had
// changed, the namespace operation stayed failed, and its "." overlap made
// every GETATTR of the volume root answer EIO for the rest of the session.

// echoDuringMutation simulates each background feed publishing name while
// the foreground mutation is between its durable intent and its completion.
// It returns what each feed did.
func echoDuringMutation(t *testing.T, store *metadata.Store, name string, prune bool) (feedApplied bool, reconcileOwned []string, pruneErr error) {
	t.Helper()
	cur := store.LookupByPath(name)
	if cur == nil {
		t.Fatalf("%s not mirrored", name)
	}
	echo := &metadata.Entry{
		Path: cur.Path, Name: cur.Name, ParentPath: cur.ParentPath, Size: cur.Size + 4096,
		Mtime: cur.Mtime.Add(1), MtimeNsecKnown: true, Inode: cur.Inode, Mode: cur.Mode,
	}
	var err error
	// The pub/sub echo of a previous WRITE (unversioned feed).
	feed := *echo
	feed.MtimeNsecKnown = false
	if feedApplied, err = store.InsertFromFeed(&feed); err != nil {
		t.Fatalf("InsertFromFeed: %v", err)
	}
	// The i-key attr push / reconcileDir upsert.
	epoch, err := store.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	if _, reconcileOwned, err = store.ReconcileUpsertBatchOwned([]*metadata.Entry{echo}, epoch); err != nil {
		t.Fatalf("ReconcileUpsertBatchOwned: %v", err)
	}
	// The d-key scoped prune (absent from a stale HGETALL).
	if row := store.LookupByPath(name); prune && row != nil {
		pruneErr = store.DeletePathsConditional([]*metadata.Entry{row})
	}
	return feedApplied, reconcileOwned, pruneErr
}

func TestOwnSaveBackgroundEchoWaitsForInPlaceWrite(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	const name = "w/proj/b0.prproj"
	seedFile(t, jfs, store, root, name, patternedPayload(8192, 61))
	mirror(t, store, root, "w", true)
	mirror(t, store, root, "w/proj", true)
	before := store.LookupByPath(name).Version

	f, err := jfs.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var feedApplied bool
	var owned []string
	var pruneErr error
	jfs.handler.testBeforeInPlaceMutation = func(_, op string) error {
		if op == "write" {
			feedApplied, owned, pruneErr = echoDuringMutation(t, store, name, true)
			// The folders above the file keep answering during the save; the
			// file itself waits (JUKEBOX).
			for _, dir := range []string{".", "w", "w/proj"} {
				if _, ok := jfs.StatCacheOnly(dir); !ok && dir != "." { // the root is not a mirror row
					t.Errorf("LOOKUP of %q during the save answered NOENT", dir)
				}
				if _, err := jfs.Stat(dir); err != nil {
					t.Errorf("Stat(%q) during the save = %v", dir, err)
				}
			}
			if _, err := jfs.Stat(name); !errors.Is(err, errFUSETimeout) {
				t.Errorf("Stat of the file during its save = %v, want retry", err)
			}
		}
		return nil
	}
	save := patternedPayload(4096, 63)
	if _, err := f.(io.WriterAt).WriteAt(save, 4096); err != nil {
		t.Fatalf("in-place save with background echoes = %v, want success", err)
	}
	if feedApplied {
		t.Fatal("pub/sub echo was applied under the running WRITE")
	}
	if len(owned) != 1 || owned[0] != name {
		t.Fatalf("reconcile upsert skipped %v, want [%s]", owned, name)
	}
	if !metadata.IsForegroundOwned(pruneErr) || !errors.Is(pruneErr, metadata.ErrMirrorConflict) {
		t.Fatalf("prune under the WRITE = %v, want ErrForegroundOwned", pruneErr)
	}
	e := store.LookupByPath(name)
	if e == nil || e.Size != 8192 || e.Version.Revision != before.Revision+1 || e.Version.Incarnation != before.Incarnation {
		t.Fatalf("after the save: %+v (before %+v), want only the save's own publication", e, before)
	}
	if pending, err := store.InspectOnlineMutations(); err != nil || len(pending) != 0 {
		t.Fatalf("intent left after a successful save: %+v, %v", pending, err)
	}
	if got := onDisk(t, root, name); !bytes.Equal(got[4096:], save) {
		t.Fatal("save bytes not on the backing file")
	}

	// After the owner finished, the deferred echo applies normally.
	jfs.handler.testBeforeInPlaceMutation = nil
	_, owned, pruneErr = echoDuringMutation(t, store, name, true)
	if len(owned) != 0 {
		t.Fatalf("after the save, reconcile still skipped %v", owned)
	}
	if pruneErr != nil {
		t.Fatalf("after the save, prune = %v", pruneErr)
	}
	if _, err := jfs.Stat("."); err != nil {
		t.Fatalf("volume root after the save = %v", err)
	}
}

// Without the ownership guard (the old behaviour), the same echo fails the
// save. The failure must stay with the file: the volume root, the folder and
// a sibling keep answering.
func TestOwnSaveConflictFailsOnlyThatFile(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	t.Cleanup(func() { discardFailedNamespaceForTest(jfs.handler) })
	const name = "w/proj/b0.prproj"
	seedFile(t, jfs, store, root, name, patternedPayload(8192, 71))
	seedFile(t, jfs, store, root, "w/proj/a0.prproj", patternedPayload(4096, 73))
	store.SetForegroundOwner(nil)

	f, err := jfs.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	jfs.handler.testBeforeInPlaceMutation = func(_, op string) error {
		if op == "write" {
			_, _, _ = echoDuringMutation(t, store, name, false)
		}
		return nil
	}
	if _, err := f.(io.WriterAt).WriteAt(patternedPayload(4096, 75), 4096); !errors.Is(err, metadata.ErrMirrorConflict) {
		t.Fatalf("unguarded save = %v, want the mirror conflict the laptop logged", err)
	}
	jfs.handler.testBeforeInPlaceMutation = nil
	if _, err := jfs.Stat(name); !errors.Is(err, syscall.EIO) {
		t.Fatalf("the failed file = %v, want EIO", err)
	}
	for _, other := range []string{".", "w", "w/proj", "w/proj/a0.prproj"} {
		if _, err := jfs.Stat(other); err != nil {
			t.Fatalf("Stat(%q) after one file's failed save = %v, want success", other, err)
		}
	}
	// Listing the volume root and the folder above it still works. Only the
	// failed file's own folder refuses its listing, which would carry the
	// file's attributes (TestWriteBindingPoisonBlocksRetainedReadsAndCommit).
	for _, dir := range []string{".", "w"} {
		if _, err := jfs.ReadDir(dir); err != nil {
			t.Fatalf("ReadDir(%q) after one file's failed save = %v, want success", dir, err)
		}
	}
	if _, err := jfs.ReadDir("w/proj"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("ReadDir of the failed file's folder = %v, want EIO", err)
	}
}

func TestNamespaceFailedOperationScope(t *testing.T) {
	var c namespaceCoordinator
	op := c.begin([]string{"w/proj/b0.prproj"})
	if op == nil {
		t.Fatal("begin refused")
	}
	if !c.ownsForBackground("w/proj/b0.prproj") || c.ownsForBackground("w/proj") ||
		c.ownsForBackground("w/proj/b0.prproj.tmp") || c.ownsForBackground(".") {
		t.Fatal("live ownership must cover exactly the held name and what lies beneath it")
	}
	op.mutated = true
	op.finish(metadata.ErrMirrorConflict) // a failed publication after a mutation
	if c.ownsForBackground("w/proj/b0.prproj") {
		t.Fatal("a failed operation must not block the background feeds")
	}
	for _, name := range []string{".", "", "w", "w/proj", "w/proj/a0.prproj", "w/proj/b0.prproj.tmp"} {
		if err := c.servingError(name); err != nil {
			t.Fatalf("servingError(%q) = %v, want nil: a failed file must not fail its ancestors", name, err)
		}
	}
	if err := c.servingError("w/proj/b0.prproj"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("servingError(failed name) = %v, want EIO", err)
	}

	dir := c.begin([]string{"w/cut"})
	dir.mutated = true
	dir.finish(syscall.EIO)
	if err := c.servingError("w/cut/inside.mov"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("a path under a failed folder = %v, want EIO", err)
	}
}

// The rename form of B1: abroad, the d-key push sees the backend rename (the
// source gone, the destination present) before the RENAME worker publishes
// it, prunes the source row and upserts the destination, and the worker's
// RenameExpected CAS fails. While the RENAME owns both names the push must
// leave them to it.
func TestOwnSaveRenameEchoWaitsForRename(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	const from, to = "w/proj/r0.prproj", "w/proj/r0-renamed.prproj"
	seedFile(t, jfs, store, root, from, patternedPayload(4096, 81))
	var owned []string
	var pruneErr error
	jfs.handler.namespace.phase = func(phase, name string) {
		if phase != "publish" || name != from {
			return
		}
		src := store.LookupByPath(from)
		dst := &metadata.Entry{Path: to, Name: "r0-renamed.prproj", ParentPath: "w/proj", Size: src.Size,
			Mtime: src.Mtime, MtimeNsecKnown: true, Inode: src.Inode, Mode: src.Mode}
		epoch, err := store.CaptureStoreEpoch()
		if err != nil {
			t.Error(err)
			return
		}
		if _, owned, err = store.ReconcileUpsertBatchOwned([]*metadata.Entry{dst}, epoch); err != nil {
			t.Error(err)
		}
		pruneErr = store.DeletePathsConditional([]*metadata.Entry{src})
	}
	if err := jfs.Rename(from, to); err != nil {
		t.Fatalf("rename with a racing d-key push = %v, want success", err)
	}
	if len(owned) != 1 || owned[0] != to || !metadata.IsForegroundOwned(pruneErr) {
		t.Fatalf("push during the rename: upsert skipped %v, prune %v", owned, pruneErr)
	}
	if store.LookupByPath(from) != nil || store.LookupByPath(to) == nil {
		t.Fatal("mirror does not show the rename")
	}
	if _, err := jfs.Stat("."); err != nil {
		t.Fatalf("volume root after the rename = %v", err)
	}
}

// A rename to a new name: the destination's token is the store-wide epoch.
// Abroad the backing rename takes about a second, and in that second another
// file's write (here the teammate's file the push refreshes) moves the epoch.
// That must not fail the rename: the RENAME owns the destination name.
func TestOwnSaveRenameSurvivesUnrelatedMirrorWrite(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	const from, to = "w/proj/r0.prproj", "w/proj/r0-renamed.prproj"
	seedFile(t, jfs, store, root, from, patternedPayload(4096, 91))
	seedFile(t, jfs, store, root, "w/other/load.bin", patternedPayload(1024, 93))
	jfs.handler.namespace.phase = func(phase, name string) {
		if phase != "publish" || name != from {
			return
		}
		other := store.LookupByPath("w/other/load.bin")
		bumped := metadata.MakeEntry(other.Path, false, other.Size+1, other.Mtime.Add(1), other.Inode)
		epoch, err := store.CaptureStoreEpoch()
		if err != nil {
			t.Error(err)
			return
		}
		if _, owned, err := store.ReconcileUpsertBatchOwned([]*metadata.Entry{bumped}, epoch); err != nil || len(owned) != 0 {
			t.Errorf("unrelated upsert: owned %v, err %v", owned, err)
		}
	}
	if err := jfs.Rename(from, to); err != nil {
		t.Fatalf("rename while another file's row changed = %v, want success", err)
	}
	if store.LookupByPath(from) != nil || store.LookupByPath(to) == nil {
		t.Fatal("mirror does not show the rename")
	}
}

// The relaxed check still refuses a destination that appeared meanwhile.
func TestOwnSaveRenameRefusesDestinationCreatedMeanwhile(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	t.Cleanup(func() { discardFailedNamespaceForTest(jfs.handler) })
	const from, to = "w/proj/r0.prproj", "w/proj/r0-renamed.prproj"
	seedFile(t, jfs, store, root, from, patternedPayload(4096, 95))
	jfs.handler.namespace.phase = func(phase, name string) {
		if phase != "publish" || name != from {
			return
		}
		// A writer outside the namespace coordinator (none should exist).
		if err := store.Insert(metadata.MakeEntry(to, false, 1, time.Now(), 777)); err != nil {
			t.Error(err)
		}
	}
	if err := jfs.Rename(from, to); !errors.Is(err, metadata.ErrMirrorConflict) {
		t.Fatalf("rename onto a destination created meanwhile = %v, want ErrMirrorConflict", err)
	}
}

// Review L1 of #100: failed operations keep blocking their own names but do
// not take one of the 24 live slots.
func TestNamespaceFailedOperationsFreeTheirSlot(t *testing.T) {
	var c namespaceCoordinator
	for i := 0; i < 30; i++ {
		op := c.begin([]string{"w/failed/f" + string(rune('a'+i))})
		if op == nil {
			t.Fatalf("begin %d refused after %d failed operations", i, i)
		}
		op.mutated = true
		op.finish(syscall.EIO)
	}
	if c.begin([]string{"w/ok.prproj"}) == nil {
		t.Fatal("a new save refused because failed operations filled the cap")
	}
	if c.begin([]string{"w/failed/fa"}) != nil {
		t.Fatal("a failed name is no longer blocked")
	}
}
