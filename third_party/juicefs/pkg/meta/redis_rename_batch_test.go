/*
 * JuiceMount-owned test (patch 0022). Not part of upstream JuiceFS.
 *
 * Needs JM_WATCH_BATCH_TEST_REDIS_URL, as redis_watch_batch_test.go.
 */

package meta

import (
	"syscall"
	"testing"
)

func renameFixture(t *testing.T, m *redisMeta, names ...string) map[string]Ino {
	t.Helper()
	inos := map[string]Ino{}
	for i, name := range names {
		ino := Ino(2000 + i)
		attr := &Attr{Typ: TypeFile, Mode: 0644, Nlink: 1, Parent: RootInode, Full: true}
		if st := m.doMknod(Background(), RootInode, name, TypeFile, 0644, 022, "", &ino, attr); st != 0 {
			t.Fatalf("mknod %s: %v", name, st)
		}
		inos[name] = ino
	}
	return inos
}

// The drainer publishes every new file with a no-replace rename of its stage
// in the same folder. With the batched reads that rename takes 3 round trips
// (upstream with patch 0019: 5): the source entry, then the guard, the
// destination entry and the attributes together, then EXEC.
func TestJMRenameBatchRoundTrips(t *testing.T) {
	m, wc := countedMeta(t)
	setWatchBatch(t, true)
	ctx := Background()
	inos := renameFixture(t, m, "stage", "old", "victim")

	var ino Ino
	var attr Attr
	wc.n.Store(0)
	if st := m.doRename(ctx, RootInode, "stage", RootInode, "final", RenameNoReplace, &ino, nil, &attr, nil); st != 0 {
		t.Fatalf("rename: %v", st)
	}
	if got := wc.n.Load(); got != 3 {
		t.Errorf("new-name rename took %d round trips, want 3", got)
	}
	if ino != inos["stage"] {
		t.Fatalf("renamed inode %d, want %d", ino, inos["stage"])
	}
	assertNoWatches(t, m)

	// Replacing an existing file reads the victim's attribute after its WATCH:
	// one more round trip.
	var tino Ino
	var tattr Attr
	wc.n.Store(0)
	if st := m.doRename(ctx, RootInode, "old", RootInode, "victim", 0, &ino, &tino, &attr, &tattr); st != 0 {
		t.Fatalf("replace: %v", st)
	}
	if got := wc.n.Load(); got != 4 {
		t.Errorf("replacing rename took %d round trips, want 4", got)
	}
	if tino != inos["victim"] {
		t.Fatalf("replaced inode %d, want %d", tino, inos["victim"])
	}
	assertNoWatches(t, m)

	// No-replace onto an existing name is refused with nothing written.
	if st := m.doRename(ctx, RootInode, "final", RootInode, "victim", RenameNoReplace, &ino, nil, &attr, nil); st != syscall.EEXIST {
		t.Fatalf("no-replace onto existing: %v, want EEXIST", st)
	}
	assertNoWatches(t, m)

	var got Ino
	var a Attr
	if st := m.doLookup(ctx, RootInode, "final", &got, &a); st != 0 || got != inos["stage"] {
		t.Fatalf("lookup final: %d %v", got, st)
	}
	if st := m.doLookup(ctx, RootInode, "victim", &got, &a); st != 0 || got != inos["old"] {
		t.Fatalf("lookup victim: %d %v", got, st)
	}
	for _, gone := range []string{"stage", "old"} {
		if st := m.doLookup(ctx, RootInode, gone, &got, &a); st != syscall.ENOENT {
			t.Fatalf("%s still present: %v", gone, st)
		}
	}
}
