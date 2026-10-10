package nfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

func failedConflictSave(t *testing.T) (*juiceFS, *SpoolStore, string, []byte, int64) {
	t.Helper()
	jfs, spool, drainer, dest, _, saved := atomicSaveFixture(t, 2<<20)
	if n := spool.sweepOnce(0); n != 1 {
		t.Fatalf("finalized %d rows", n)
	}
	row, err := spool.Meta().LookupByPath("edit.prproj")
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := spool.Meta().MarkDraining(row.ID); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	drainer.failPermanent(row, "test: published as conflict copy edit (conflict).prproj")
	return jfs, spool, dest, saved, row.ID
}

func TestConflictSaveLocalViewSurvivesRemoteReconcile(t *testing.T) {
	jfs, spool, dest, saved, id := failedConflictSave(t)
	if spool.HasPending("edit.prproj") {
		t.Fatal("failed row was placed in active index")
	}
	row, err := spool.Meta().Get(id)
	if err != nil || row.DrainState != metadata.DrainFailed {
		t.Fatalf("failed status: %+v %v", row, err)
	}
	view := spool.conflictRead("edit.prproj")
	if view == nil {
		t.Fatal("qualified local view absent")
	}
	if got := jfs.handler.ToHandle(jfs, []string{"edit.prproj"}); !bytes.Equal(got, jfs.handler.export.handle(view.inode)) {
		t.Fatal("LOOKUP handle did not use local inode")
	}
	fs, parts, err := jfs.handler.FromHandle(jfs.handler.export.handle(view.inode))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := fs.(*conflictHandleFS); !ok {
		t.Fatalf("held handle routed to %T", fs)
	}
	if info, err := fs.Stat(fs.Join(parts...)); err != nil || info.Size() != int64(len(saved)) {
		t.Fatalf("GETATTR local size: %v %v", info, err)
	}
	if info, ok := jfs.StatCacheOnly("edit.prproj"); !ok || info.Size() != int64(len(saved)) {
		t.Fatalf("LOOKUP local size: %v %v", info, ok)
	}
	infos, err := jfs.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, info := range infos {
		if info.Name() == "edit.prproj" {
			found = true
			if info.Size() != int64(len(saved)) {
				t.Fatalf("READDIR size %d", info.Size())
			}
		}
	}
	if !found {
		t.Fatal("READDIR omitted local view")
	}
	// A full backend refresh with a different inode and size is not a local
	// successor. The accepted bytes and inode remain authoritative here.
	foreign := []byte("foreign backend version")
	if err := os.WriteFile(dest, foreign, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := jfs.handler.store.Insert(metadata.MakeEntry("edit.prproj", false, int64(len(foreign)), time.Now(), 987654)); err != nil {
		t.Fatal(err)
	}
	read, err := fs.Open("edit.prproj")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(read)
	read.Close()
	if err != nil || !bytes.Equal(got, saved) {
		t.Fatalf("served backend instead of accepted bytes: %d %v", len(got), err)
	}
	held, err := fs.Open("edit.prproj")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if info, err := jfs.Stat("edit.prproj"); err != nil || info.Size() != int64(len(saved)) {
		t.Fatalf("post-reconcile stat: %v %v", info, err)
	}
	if _, cleared, _, err := spool.ClearFailed(true); err != nil || cleared != 1 {
		t.Fatalf("clear: %d %v", cleared, err)
	}
	if n, err := held.ReadAt(make([]byte, 16), 0); n != 0 || err != syscall.EIO {
		t.Fatalf("held read after clear: %d %v", n, err)
	}
	if cErr := fs.(*conflictHandleFS).CommitFile("edit.prproj"); cErr != syscall.EROFS {
		t.Fatalf("held COMMIT reached successor: %v", cErr)
	}
	if _, _, err := jfs.handler.FromHandle(jfs.handler.export.handle(view.inode)); err == nil {
		t.Fatal("old held handle survived clear")
	}
}

func TestConflictSaveBootProofAndNoFallback(t *testing.T) {
	for _, damage := range []string{"intact", "same-size-corrupt", "missing", "punched", "clone-held"} {
		t.Run(damage, func(t *testing.T) {
			jfs, spool, dest, saved, id := failedConflictSave(t)
			row, err := spool.Meta().Get(id)
			if err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "same-size-corrupt":
				if err := os.WriteFile(row.SpoolFile, bytes.Repeat([]byte{0x77}, len(saved)), 0o600); err != nil {
					t.Fatal(err)
				}
			case "missing":
				if err := os.Remove(row.SpoolFile); err != nil {
					t.Fatal(err)
				}
			case "punched":
				if err := spool.Meta().SetPunchedEnd(id, 4096); err != nil {
					t.Fatal(err)
				}
			case "clone-held":
				if n, err := spool.Meta().HoldRowsCopiedFromOtherMac(); err != nil || n == 0 {
					t.Fatalf("clone hold: %d %v", n, err)
				}
			}
			// Reopen the persisted store before serving, exactly as startup does.
			var seq int
			var name, dbPath string
			if err := jfs.handler.store.DB().QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &dbPath); err != nil {
				t.Fatal(err)
			}
			jfs.handler.StopHandler()
			if err := jfs.handler.store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err := metadata.Open(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			meta := metadata.NewSpoolStore(store.DB())
			store.SetSpoolStore(meta)
			recovered, err := NewSpoolStore(spool.root, 0, meta)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := recovered.RecoverOnBoot(context.Background()); damage == "punched" {
				var pending *metadata.PendingPunchedSpool
				if !errors.As(err, &pending) || recovered.conflictRead("edit.prproj") != nil {
					t.Fatalf("punched boot must refuse NFS serving: %v view=%v", err, recovered.conflictRead("edit.prproj"))
				}
				return
			} else if err != nil {
				t.Fatal(err)
			}
			if damage == "clone-held" && recovered.conflictRead("edit.prproj") != nil {
				t.Fatal("copied failed row acquired local read authority")
			}
			h := NewHandler(store, filepath.Dir(dest))
			defer h.StopHandler()
			h.SetSpool(recovered, nil)
			reader := &juiceFS{handler: h}
			file, err := reader.Open("edit.prproj")
			if damage == "intact" {
				if err != nil {
					t.Fatal(err)
				}
				got, readErr := io.ReadAll(file)
				file.Close()
				if readErr != nil || !bytes.Equal(got, saved) {
					t.Fatalf("boot local read: %d %v", len(got), readErr)
				}
				return
			}
			if file != nil {
				file.Close()
			}
			if err != syscall.EIO {
				t.Fatalf("%s used backend fallback: %v", damage, err)
			}
		})
	}
}

func TestConflictReadGenericFailureAndChangedLocalSource(t *testing.T) {
	t.Run("generic failure", func(t *testing.T) {
		jfs, spool, drainer, _, _, _ := atomicSaveFixture(t, 64<<10)
		spool.sweepOnce(0)
		row, err := spool.Meta().LookupByPath("edit.prproj")
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := spool.Meta().MarkDraining(row.ID); err != nil || !ok {
			t.Fatalf("claim: %v %v", ok, err)
		}
		drainer.failPermanent(row, "generic terminal failure")
		if _, err := jfs.Open("edit.prproj"); err != syscall.EIO {
			t.Fatalf("generic failure read: %v", err)
		}
		if spool.conflictRead("edit.prproj") != nil {
			t.Fatal("generic failure acquired local view")
		}
	})
	t.Run("same size and restored mtime", func(t *testing.T) {
		jfs, spool, _, _, id := failedConflictSave(t)
		row, err := spool.Meta().Get(id)
		if err != nil {
			t.Fatal(err)
		}
		fi, err := os.Stat(row.SpoolFile)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.OpenFile(row.SpoolFile, os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt([]byte("changed!"), 0); err != nil {
			t.Fatal(err)
		}
		f.Close()
		if err := os.Chtimes(row.SpoolFile, fi.ModTime(), fi.ModTime()); err != nil {
			t.Fatal(err)
		}
		if _, err := jfs.Open("edit.prproj"); err != syscall.EIO {
			t.Fatalf("changed source read: %v", err)
		}
	})
}

func TestConflictReadSuccessorAndInodeIsolation(t *testing.T) {
	jfs, spool, _, saved, _ := failedConflictSave(t)
	view := spool.conflictRead("edit.prproj")
	if view == nil {
		t.Fatal("precondition: no conflict view")
	}
	// A failed successor admission rolls its SQL row back and retains the
	// original accepted image. Only the new spool root is made invalid.
	root := spool.root
	spool.root = filepath.Join(t.TempDir(), "missing")
	if e, err := spool.OpenWrite("edit.prproj"); err == nil {
		e.Close()
		t.Fatal("expected spool-file open failure")
	}
	spool.root = root
	if spool.conflictRead("edit.prproj") != view {
		t.Fatal("failed successor revoked accepted view")
	}
	read, err := jfs.Open("edit.prproj")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(read)
	read.Close()
	if err != nil || !bytes.Equal(got, saved) {
		t.Fatalf("rollback read: %d %v", len(got), err)
	}
	// A second conflict generation with an accidentally reused live inode
	// must not hijack the first path's already-issued NFS handle.
	e := writeSpoolEntry(t, spool, "other.prproj", []byte("other accepted bytes"))
	e.SetInode(view.inode)
	row, err := spool.Meta().Get(e.ID())
	if err != nil {
		t.Fatal(err)
	}
	if row.DrainState != metadata.DrainReady {
		t.Fatalf("closed other row state: %s, want ready", row.DrainState)
	}
	if ok, err := spool.Meta().MarkDraining(row.ID); err != nil || !ok {
		t.Fatalf("claim other: %v %v", ok, err)
	}
	d, err := NewDrainer(spool, DrainerConfig{FuseRoot: filepath.Dir(jfs.fullPath("edit.prproj"))})
	if err != nil {
		t.Fatal(err)
	}
	d.failPermanent(row, "test: published as conflict copy other (conflict).prproj")
	other := spool.conflictRead("other.prproj")
	if other == nil || other.inode == view.inode {
		t.Fatalf("duplicate inode aliased: other=%+v first=%d", other, view.inode)
	}
	if mapped, _ := spool.conflictHandle(view.inode); mapped != view {
		t.Fatal("first handle redirected to second path")
	}
	// Successful local admission revokes the original generation before
	// exposing its new row through the active spool index.
	successor, err := spool.OpenWrite("edit.prproj")
	if err != nil {
		t.Fatal(err)
	}
	defer successor.Close()
	if spool.conflictRead("edit.prproj") != nil {
		t.Fatal("accepted successor did not revoke old view")
	}
	if _, _, err := jfs.handler.FromHandle(jfs.handler.export.handle(view.inode)); err == nil {
		t.Fatal("old handle survived accepted successor")
	}
}
