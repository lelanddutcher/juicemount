package nfs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

// mirrorHas publishes a mirror entry for name, as LOOKUP/readdir would after
// this Mac saw the backend file.
func mirrorHas(t *testing.T, spool *SpoolStore, name string, size int64) {
	t.Helper()
	store := spool.Meta().Owner()
	if store == nil {
		t.Fatal("spool store not wired to a metadata store")
	}
	if err := store.Insert(metadata.MakeEntry(name, false, size, time.Unix(1700000000, 0), 4242)); err != nil {
		t.Fatal(err)
	}
}

func backendIdentity(t *testing.T, p string) metadata.SpoolBase {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Lstat(p, &st); err != nil {
		t.Fatal(err)
	}
	return metadata.SpoolBase{Kind: metadata.SpoolBasePresent, Ino: uint64(st.Ino),
		MtimeNs: st.Mtim.Nano(), CtimeNs: st.Ctim.Nano(), Len: st.Size}
}

// H-8B L4 RED: a production shadow-open records what this Mac saw at the name:
// the exact backend identity of the file it replaces, or "absent" for a new
// name. rowDrainBase must stop returning baseUnknown for production rows.
func TestH8BProductionRowRecordsBaseIdentity(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	const existing = "Film/ProjA/edit.prproj"
	dest := filepath.Join(d.fuseRoot, existing)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dest, []byte("version 1"), 0o644); err != nil {
		t.Fatal(err)
	}
	mirrorHas(t, spool, existing, 9)
	want := backendIdentity(t, dest)

	e := writeSpoolEntry(t, spool, existing, []byte("version 2"))
	row, err := spool.Meta().Get(e.ID())
	if err != nil {
		t.Fatal(err)
	}
	got, _ := d.rowDrainBase(row)
	if got != want {
		t.Errorf("H8B: save over an existing file recorded base %+v, want %+v", got, want)
	}

	e2 := writeSpoolEntry(t, spool, "Film/ProjA/new.mov", []byte("new"))
	row2, _ := spool.Meta().Get(e2.ID())
	if got, _ := d.rowDrainBase(row2); got.Kind != metadata.SpoolBaseAbsent {
		t.Errorf("H8B: new name recorded base %v, want absent", got.Kind)
	}

	// Premiere saves temp-then-rename: the rename onto the existing name must
	// re-derive the base for the destination it replaces.
	if _, err := spool.Meta().MarkDone(e.ID()); err != nil {
		t.Fatal(err)
	}
	spool.EvictIndex(existing, e.ID())
	tmp := writeSpoolEntry(t, spool, "Film/ProjA/edit.prproj.tmp", []byte("version 3"))
	if _, _, err := spool.MigrateForRename("Film/ProjA/edit.prproj.tmp", existing); err != nil {
		t.Fatal(err)
	}
	row3, _ := spool.Meta().Get(tmp.ID())
	// F1: an exact destination base becomes "replace" (same inode, mtime
	// and length; ctime is not carried), so the drain stages and replaces
	// instead of truncating the file the save replaces.
	wantReplace := metadata.SpoolBase{Kind: metadata.SpoolBaseReplace, Ino: want.Ino, MtimeNs: want.MtimeNs, Len: want.Len}
	if got, _ := d.rowDrainBase(row3); got != wantReplace {
		t.Errorf("H8B: rename onto an existing name recorded base %+v, want %+v", got, wantReplace)
	}
}

// H-8B through the real handler: an atomic save (CREATE temp in the spool,
// RENAME it over an existing backend file) must record the replaced file's
// base, and that base must be what the drain checks. Before F1 the handler
// removed the destination at the rename, so the recorded base was never
// compared with anything (the drain always found an absent name).
func TestH8BAtomicSaveThroughHandlerChecksRecordedBase(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandlerNoDrain(t)
	const name = "edit.prproj"
	dest := filepath.Join(fuseRoot, name)
	if err := os.WriteFile(dest, []byte("version 1 on the server"), 0o644); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Lstat(dest)
	if err := jfs.handler.store.Insert(metadata.MakeEntry(name, false, fi.Size(), fi.ModTime(), 4242)); err != nil {
		t.Fatal(err)
	}
	old := backendIdentity(t, dest)
	simulateNFSCreateThenWrite(t, jfs, name+".tmp", []byte("version 2 saved on this Mac"), 8)
	if err := jfs.Rename(name+".tmp", name); err != nil {
		t.Fatalf("rename: %v", err)
	}
	row, err := spool.Meta().LookupByPath(name)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := drainer.rowDrainBase(row)
	if got.Kind != metadata.SpoolBaseReplace || got.Ino != old.Ino || got.MtimeNs != old.MtimeNs || got.Len != old.Len {
		t.Fatalf("H8B: handler rename recorded base %+v, want replace of %+v", got, old)
	}
	if cur := backendIdentity(t, dest); cur.Ino != old.Ino {
		t.Fatalf("H8B/F1: the rename must keep the replaced server file until the drain installs (inode %d -> %d)", old.Ino, cur.Ino)
	}

	// Another writer changes the server file after the save: the recorded
	// base no longer matches, so the drain must not replace it.
	if err := os.WriteFile(dest, []byte("version 1b written by another Mac"), 0o644); err != nil {
		t.Fatal(err)
	}
	spool.sweepOnce(0)
	drainAllForTest(t, drainer)
	b, _ := os.ReadFile(dest)
	if string(b) != "version 1b written by another Mac" {
		t.Fatalf("H8B: drain replaced a file that changed after the save: %q", b)
	}
	r, _ := spool.Meta().Get(row.ID)
	if r.DrainState != metadata.DrainFailed || !isConflictCopyFailure(r.LastError) {
		t.Fatalf("H8B: expected a conflict copy, got state=%s last_error=%q", r.DrainState, r.LastError)
	}
}

// H-8B L4 RED: a drain into a project another Mac holds is parked locally
// (waiting_project), the menu bar learns who holds it, and the row drains when
// the project frees. No attempt is spent and nothing is written.
func TestH8BDrainRefusalHoldsRowNotifies(t *testing.T) {
	const proj = "Film/ProjA"
	t.Run("held at acquire", func(t *testing.T) {
		spool, d := newTestDrainer(t, DrainerConfig{})
		f := newFakeLeaseServer(t)
		c := newTestLeaseClient(t, f, spool, nil)
		d.SetProjectLeases(c)
		f.setHolder(proj, "editor@Edit-Bay-2")
		const name = proj + "/card/A001.mov"
		ours := patternedPayload(8192, 11)
		e := writeSpoolEntry(t, spool, name, ours)

		drainOnce(t, d)

		row, err := spool.Meta().Get(e.ID())
		if err != nil {
			t.Fatal(err)
		}
		wait, _ := spool.Meta().WaitProject(e.ID())
		if row.DrainState != metadata.DrainReady || wait != proj || row.DrainAttempts != 0 {
			t.Fatalf("H8B: refused drain not held: state=%s wait=%q attempts=%d", row.DrainState, wait, row.DrainAttempts)
		}
		if _, err := os.Stat(e.SpoolFilePath()); err != nil {
			t.Fatalf("H8B: held row lost its spool source: %v", err)
		}
		if _, err := os.Lstat(filepath.Join(d.fuseRoot, name)); !os.IsNotExist(err) {
			t.Fatalf("H8B: held row wrote the backend: %v", err)
		}
		snap := c.Snapshot()
		if len(snap.Projects) != 1 || snap.Projects[0].State != "waiting" ||
			snap.Projects[0].HolderHost != "Edit-Bay-2" || snap.Projects[0].WaitingRows != 1 {
			t.Fatalf("H8B: menu bar not told who holds the project: %+v", snap)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if n := d.DrainOnceForTest(ctx); n != 0 {
			t.Fatalf("H8B: held row dispatched again while the project is held (%d)", n)
		}

		f.free(proj)
		c.Tick(context.Background())
		drainOnce(t, d)
		if got, _ := os.ReadFile(filepath.Join(d.fuseRoot, name)); !bytes.Equal(got, ours) {
			t.Fatalf("H8B: freed project did not drain the held row (%d bytes)", len(got))
		}
		if row, _ := spool.Meta().Get(e.ID()); row == nil || row.DrainState != metadata.DrainDone {
			t.Fatalf("H8B: freed row not done: %+v", row)
		}
	})
	t.Run("EROFS from the mount", func(t *testing.T) {
		spool, d := newTestDrainer(t, DrainerConfig{})
		f := newFakeLeaseServer(t)
		c := newTestLeaseClient(t, f, spool, nil)
		d.SetProjectLeases(c)
		const name = proj + "/new.mov"
		e := writeSpoolEntry(t, spool, name, []byte("refused at install"))
		// The proxy refused the commit (JMDENY -> EROFS from bundled juicefs).
		drainRenameNoReplace = func(string, string) error {
			return &os.LinkError{Op: "rename", Err: syscall.EROFS}
		}
		t.Cleanup(func() { drainRenameNoReplace = renameStreamNoReplace })

		drainOnce(t, d)

		row, _ := spool.Meta().Get(e.ID())
		wait, _ := spool.Meta().WaitProject(e.ID())
		if row == nil || row.DrainState != metadata.DrainReady || wait != proj || row.DrainAttempts != 0 {
			t.Fatalf("H8B: EROFS not held as waiting_project: %+v wait=%q", row, wait)
		}
		if _, err := os.Stat(e.SpoolFilePath()); err != nil {
			t.Fatalf("H8B: EROFS lost the spool source: %v", err)
		}
		assertNoDrainStages(t, filepath.Dir(filepath.Join(d.fuseRoot, name)))
		if s := c.Snapshot(); len(s.Projects) != 1 || s.Projects[0].State != "waiting" {
			t.Fatalf("H8B: EROFS refusal not surfaced: %+v", s)
		}
	})
}

// H-8B L4 RED: a save accepted under an earlier epoch, whose base another Mac
// edited while it held the project, publishes as a conflict copy. The other
// Mac's file is never overwritten.
func TestH8BForeignEpochEditBecomesConflictCopy(t *testing.T) {
	const proj = "Film/ProjA"
	const name = proj + "/edit.prproj"
	setup := func(t *testing.T) (*SpoolStore, *Drainer, *fakeLeaseServer, *ProjectLeaseClient, *time.Time, string) {
		fixConflictClock(t)
		spool, d := newTestDrainer(t, DrainerConfig{})
		f := newFakeLeaseServer(t)
		now := time.Unix(1_800_000_000, 0)
		c := newTestLeaseClient(t, f, spool, func() time.Time { return now })
		d.SetProjectLeases(c)
		dest := filepath.Join(d.fuseRoot, name)
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dest, []byte("v1 the file both Macs opened"), 0o644); err != nil {
			t.Fatal(err)
		}
		mirrorHas(t, spool, name, 28)
		if dec, err := c.EnsureForDrain(proj); err != nil || !dec.held {
			t.Fatalf("initial lease %+v %v", dec, err)
		}
		return spool, d, f, c, &now, dest
	}
	foreignEdit := func(t *testing.T, f *fakeLeaseServer, now *time.Time, dest string, theirs []byte) {
		// Our lease lapses (10 min offline); Edit Bay 2 takes the project,
		// edits the file in place, and frees it.
		f.setHolder(proj, "editor@Edit-Bay-2")
		time.Sleep(10 * time.Millisecond) // a distinct ctime
		if err := os.WriteFile(dest, theirs, 0o644); err != nil {
			t.Fatal(err)
		}
		f.free(proj)
		*now = now.Add(time.Hour)
	}
	conflictName := filepath.Join(proj, "edit (conflict mac2 2026-10-06 140309).prproj")

	t.Run("edited base", func(t *testing.T) {
		spool, d, f, _, now, dest := setup(t)
		ours := []byte("v2 our offline save")
		e := writeSpoolEntry(t, spool, name, ours)
		if _, epoch, _ := spool.Meta().DrainBase(e.ID()); epoch != 1 {
			t.Fatalf("row accepted under epoch %d, want 1", epoch)
		}
		theirs := []byte("v2 EDIT BAY 2's save")
		foreignEdit(t, f, now, dest, theirs)

		drainOnce(t, d)

		if got, _ := os.ReadFile(dest); !bytes.Equal(got, theirs) {
			t.Fatalf("H8B: foreign-epoch edit overwritten in place: %q", got)
		}
		if got, err := os.ReadFile(filepath.Join(d.fuseRoot, conflictName)); err != nil || !bytes.Equal(got, ours) {
			t.Fatalf("H8B: no conflict copy of our save: %q %v", got, err)
		}
	})
	t.Run("legacy row under a foreign epoch", func(t *testing.T) {
		spool, d, f, _, now, dest := setup(t)
		ours := []byte("v2 legacy row")
		e := writeSpoolEntry(t, spool, name, ours)
		// A row from before the base columns existed: unknown base, epoch 0.
		if _, err := spool.Meta().Owner().DB().Exec(`UPDATE spool_entries SET base_ino=0, base_mtime_ns=0,
		 base_ctime_ns=0, base_len=0, accepted_epoch=0 WHERE id=?`, e.ID()); err != nil {
			t.Fatal(err)
		}
		theirs := []byte("v2 EDIT BAY 2")
		foreignEdit(t, f, now, dest, theirs)

		drainOnce(t, d)

		if got, _ := os.ReadFile(dest); !bytes.Equal(got, theirs) {
			t.Fatalf("H8B: legacy row overwrote a foreign-epoch file: %q", got)
		}
		if _, err := os.Lstat(filepath.Join(d.fuseRoot, conflictName)); err != nil {
			t.Fatalf("H8B: legacy row under a foreign epoch made no conflict copy: %v", err)
		}
	})
	t.Run("unchanged base under the same lease writes in place", func(t *testing.T) {
		spool, d, _, _, _, dest := setup(t)
		var before unix.Stat_t
		_ = unix.Lstat(dest, &before)
		ours := []byte("v2 ordinary save")
		writeSpoolEntry(t, spool, name, ours)
		drainOnce(t, d)
		var after unix.Stat_t
		_ = unix.Lstat(dest, &after)
		if got, _ := os.ReadFile(dest); !bytes.Equal(got, ours) || after.Ino != before.Ino {
			t.Fatalf("H8B: ordinary save not written in place: %q ino %d->%d", got, before.Ino, after.Ino)
		}
		if _, err := os.Lstat(filepath.Join(d.fuseRoot, conflictName)); err == nil {
			t.Fatal("H8B: ordinary save produced a conflict copy")
		}
	})
}
