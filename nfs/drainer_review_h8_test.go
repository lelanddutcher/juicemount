package nfs

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// Reviewer probes from the H-8 drainer review (BLOCK verdict on replace-by-
// rename). R1 and R2 are copied verbatim. R3 is adapted: the drain no longer
// uses a replacing rename for new names, and its REMOVE now arrives through
// the production namespace admission, as a real NFS REMOVE does.

func reviewIno(t *testing.T, p string) uint64 {
	t.Helper()
	fi, err := os.Lstat(p)
	if err != nil {
		t.Fatal(err)
	}
	return uint64(fi.Sys().(*syscall.Stat_t).Ino)
}

// R1: a save of an EXISTING file keeps the backend inode (NFS fileid/handle).
func TestReviewOverwriteKeepsInode(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "/proj/export.mov"
	dest := filepath.Join(d.fuseRoot, name)
	os.MkdirAll(filepath.Dir(dest), 0o755)
	if err := os.WriteFile(dest, []byte("version-1"), 0o644); err != nil {
		t.Fatal(err)
	}
	before := reviewIno(t, dest)
	mirrorSeesBackend(t, spool, name, dest)
	writeSpoolEntry(t, spool, name, []byte("version-2-longer"))
	drainOnce(t, d)
	if after := reviewIno(t, dest); after != before {
		t.Fatalf("REVIEW: overwrite changed backend inode %d -> %d", before, after)
	}
}

// R2: a pooled read fd taken before the save must serve the saved bytes after it.
func TestReviewPooledFDSeesSavedBytes(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "/proj/export2.mov"
	dest := filepath.Join(d.fuseRoot, name)
	os.MkdirAll(filepath.Dir(dest), 0o755)
	if err := os.WriteFile(dest, []byte("OLD-OLD-OLD-OLD!"), 0o644); err != nil {
		t.Fatal(err)
	}
	mirrorSeesBackend(t, spool, name, dest)
	pool := NewFDPool()
	defer pool.Stop()
	if _, err := pool.Get(dest); err != nil {
		t.Fatal(err)
	}
	pool.Release(dest)
	want := []byte("NEW-NEW-NEW-NEW!")
	writeSpoolEntry(t, spool, name, want)
	drainOnce(t, d)
	fd, err := pool.Get(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Release(dest)
	got := make([]byte, len(want))
	if _, err := fd.ReadAt(got, 0); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("REVIEW: pooled fd serves pre-save bytes %q after drain, want %q", got, want)
	}
}

// R3: a REMOVE that arrives between the claim recheck and the install rename.
// With the install inside the namespace lease, the REMOVE is refused (the
// client retries later) and the drain publishes; the retried REMOVE then
// deletes it. Without the lease the REMOVE deletes first and the rename
// resurrects the file.
func TestReviewRemoveRacingInstallDoesNotResurrect(t *testing.T) {
	jfs, spool, d, fuseRoot := newSpoolWiredHandlerNoDrain(t)
	const name = "proj/deleted.mov"
	dest := filepath.Join(fuseRoot, name)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatal(err)
	}
	simulateNFSCreateThenWrite(t, jfs, name, []byte("user deleted this"), 4)
	spool.sweepOnce(0)
	removedBeforeInstall, hookRan := false, false
	orig := drainRenameNoReplace
	drainRenameNoReplace = func(a, b string) error {
		if !removedBeforeInstall && filepath.Base(b) == filepath.Base(dest) {
			hookRan = true
			if err := jfs.Remove(name); err == nil {
				removedBeforeInstall = true
			}
		}
		return orig(a, b)
	}
	defer func() { drainRenameNoReplace = orig }()
	drainAllForTest(t, d)
	if !hookRan {
		t.Fatal("install rename never ran; the probe exercised nothing")
	}
	if removedBeforeInstall {
		if _, err := os.Lstat(dest); err == nil {
			t.Fatalf("REVIEW: file deleted during install window was resurrected by the drain")
		}
	}
	if !removedBeforeInstall {
		// The REMOVE was refused during install; the client's retry runs now.
		if err := jfs.Remove(name); err != nil {
			t.Fatalf("retried remove: %v", err)
		}
		if _, err := os.Lstat(dest); err == nil {
			t.Fatal("retried REMOVE did not delete the published file")
		}
	}
}

// Re-review probe P1 (verbatim intent): our own sidecar, chmod'd 0444 by the
// post-drain mode apply, is rewritten by Finder and must reach done.
func TestProbeOwnReadOnlySidecarRewrite(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{MaxAttempts: 3, BackoffBase: time.Millisecond})
	const name = "/perms/._readonly.dat"
	dest := filepath.Join(d.fuseRoot, name)
	os.MkdirAll(filepath.Dir(dest), 0o755)
	os.WriteFile(dest, []byte("first-sidecar"), 0o444)
	mirrorSeesBackend(t, spool, name, dest)
	e := writeSpoolEntry(t, spool, name, []byte("second-sidecar-authoritative"))
	probeDrainLoop(d)
	row, _ := spool.Meta().Get(e.ID())
	if row.DrainState != metadata.DrainDone {
		t.Errorf("P1 REGRESSION: own read-only sidecar rewrite never lands: state=%s err=%q", row.DrainState, row.LastError)
	}
}

func probeDrainLoop(d *Drainer) {
	for i := 0; i < 40; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		d.DrainOnceForTest(ctx)
		cancel()
		time.Sleep(20 * time.Millisecond)
	}
}
