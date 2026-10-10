package nfs

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

// atomicSaveFixture puts an old file on the backend, makes the mirror see
// it, then saves new bytes the way Premiere and NSDocument apps do: CREATE a
// temp (spool only), WRITE it, RENAME it over the original.
func atomicSaveFixture(t *testing.T, size int) (jfs *juiceFS, spool *SpoolStore, d *Drainer, dest string, oldIno uint64, newBytes []byte) {
	t.Helper()
	jfs, spool, d, fuseRoot := newSpoolWiredHandlerNoDrain(t)
	const name = "edit.prproj"
	dest = filepath.Join(fuseRoot, name)
	if err := os.WriteFile(dest, patternedPayload(size, 1), 0o644); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Lstat(dest)
	if err := jfs.handler.store.Insert(metadata.MakeEntry(name, false, fi.Size(), fi.ModTime(), 4242)); err != nil {
		t.Fatal(err)
	}
	oldIno = inodeOfPath(t, dest)
	newBytes = patternedPayload(size, 2)
	simulateNFSCreateThenWrite(t, jfs, name+".tmp", newBytes, 64<<10)
	if err := jfs.Rename(name+".tmp", name); err != nil {
		t.Fatalf("rename: %v", err)
	}
	return jfs, spool, d, dest, oldIno, newBytes
}

func inodeOfPath(t *testing.T, p string) uint64 {
	t.Helper()
	fi, err := os.Lstat(p)
	if err != nil {
		return 0
	}
	return uint64(fi.Sys().(*syscall.Stat_t).Ino)
}

// replaceObservation is what a second Mac sees at the name while this Mac
// drains: one Lstat per sample, classified by inode.
type replaceObservation struct {
	samples, absent, old, new, other int64
}

func observeReplace(dest string, oldIno uint64, stop <-chan struct{}) *replaceObservation {
	r := &replaceObservation{}
	for {
		select {
		case <-stop:
			return r
		default:
		}
		r.samples++
		fi, err := os.Lstat(dest)
		switch {
		case err != nil:
			r.absent++
		case uint64(fi.Sys().(*syscall.Stat_t).Ino) == oldIno:
			r.old++
		default:
			r.new++
		}
	}
}

// F1: during an atomic save the name is never missing on the server. A second
// reader sees the old file, then the new one, and nothing in between.
func TestF1AtomicSaveNeverLeavesNameMissing(t *testing.T) {
	_, spool, d, dest, oldIno, newBytes := atomicSaveFixture(t, 4<<20)
	if got := inodeOfPath(t, dest); got != oldIno {
		t.Fatalf("F1: the rename removed or replaced the server file (inode %d -> %d)", oldIno, got)
	}
	stop := make(chan struct{})
	var obs *replaceObservation
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); obs = observeReplace(dest, oldIno, stop) }()
	time.Sleep(2 * time.Millisecond)
	if k := spool.sweepOnce(0); k < 1 {
		t.Fatalf("sweep finalized %d", k)
	}
	drainAllForTest(t, d)
	time.Sleep(2 * time.Millisecond)
	close(stop)
	wg.Wait()
	// Validity gate: the observer must have seen both sides of the swap.
	if obs.samples < 10 || obs.old == 0 || obs.new == 0 {
		t.Fatalf("VALIDITY: observer saw samples=%d old=%d new=%d; refusing to judge", obs.samples, obs.old, obs.new)
	}
	if obs.absent != 0 {
		t.Fatalf("F1: second reader saw the name MISSING in %d of %d samples during an atomic save", obs.absent, obs.samples)
	}
	got, err := os.ReadFile(dest)
	if err != nil || sha256.Sum256(got) != sha256.Sum256(newBytes) {
		t.Fatalf("F1: final server content is not the saved bytes (err %v)", err)
	}
	if inodeOfPath(t, dest) == oldIno {
		t.Fatal("F1: the save was written in place (old inode kept); expected a replacing install")
	}
	assertNoDrainStages(t, filepath.Dir(dest))
	if _, err := spool.Meta().LookupByPath("edit.prproj"); err == nil {
		t.Fatal("F1: the replace row is still pending; completion did not accept the replacing install")
	}
	if spool.HasPending("edit.prproj") {
		t.Fatal("F1: spool shadow not evicted after the replacing install completed")
	}
}

// F1: the replacing rename re-checks the destination under the install
// lease. A change that lands after the copy started becomes a conflict copy;
// the other writer's file keeps the name.
func TestF1ReplaceRechecksDestinationAtInstall(t *testing.T) {
	_, spool, d, dest, _, newBytes := atomicSaveFixture(t, 64<<10)
	other := []byte("written by another Mac while this save uploaded")
	old := drainPreReplaceHook
	drainPreReplaceHook = func(p string) {
		if p == dest {
			if err := os.WriteFile(dest, other, 0o644); err != nil {
				t.Error(err)
			}
		}
	}
	t.Cleanup(func() { drainPreReplaceHook = old })
	spool.sweepOnce(0)
	drainAllForTest(t, d)
	if b, _ := os.ReadFile(dest); !bytes.Equal(b, other) {
		t.Fatalf("F1: the install replaced a file that changed during the upload: %q", b[:min(len(b), 40)])
	}
	ents, _ := os.ReadDir(filepath.Dir(dest))
	var copies int
	for _, e := range ents {
		if strings.Contains(e.Name(), "(conflict ") {
			copies++
			b, _ := os.ReadFile(filepath.Join(filepath.Dir(dest), e.Name()))
			if !bytes.Equal(b, newBytes) {
				t.Fatal("F1: conflict copy does not hold the saved bytes")
			}
		}
	}
	if copies != 1 {
		t.Fatalf("F1: want exactly one conflict copy, got %d", copies)
	}
}

// F1 + F2: a replace whose upload keeps failing never fails on its attempt
// budget. The server keeps the previous version, this Mac keeps serving the
// saved bytes from the spool, and the status says why.
func TestF1ReplaceFailurePausesAndThisMacServesSavedBytes(t *testing.T) {
	setDrainPauseForTest(t, nil)
	_, spool, d, dest, oldIno, newBytes := atomicSaveFixture(t, 256<<10)
	d.maxAttempts = 3
	spool.sweepOnce(0)
	row, err := spool.Meta().LookupByPath("edit.prproj")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(row.SpoolFile, int64(len(newBytes)/2)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		drainAllForTest(t, d)
	}
	r, _ := spool.Meta().Get(row.ID)
	if r.DrainState == metadata.DrainFailed || !strings.Contains(r.LastError, serverPreviousMarker) {
		t.Fatalf("F1: a failing replace must pause, not fail: state=%s attempts=%d last_error=%q", r.DrainState, r.DrainAttempts, r.LastError)
	}
	if inodeOfPath(t, dest) != oldIno {
		t.Fatal("F1: a failed replace changed the server copy")
	}
	if !spool.HasPending("edit.prproj") {
		t.Fatal("F1: this Mac lost its spool shadow; it would serve the previous version as current (4b25448)")
	}
	if err := os.WriteFile(row.SpoolFile, newBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	drainAllForTest(t, d)
	got, _ := os.ReadFile(dest)
	if sha256.Sum256(got) != sha256.Sum256(newBytes) {
		t.Fatal("F1: retry after the cause cleared did not install the saved bytes")
	}
}

// A weak destination observation cannot authorize removal or an in-place
// rewrite. The complete old copy stays at the name and saved bytes become a
// verified conflict copy after the drain.
func TestF1WithoutExactBasePreservesOldAndSavedBytes(t *testing.T) {
	jfs, spool, d, fuseRoot := newSpoolWiredHandlerNoDrain(t)
	dest := filepath.Join(fuseRoot, "clip.mov")
	oldBytes, newBytes := []byte("complete old copy"), []byte("complete saved copy")
	if err := os.WriteFile(dest, oldBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Lstat(dest)
	if err := jfs.handler.store.Insert(metadata.MakeEntry("clip.mov", false, fi.Size(), fi.ModTime(), 4242)); err != nil {
		t.Fatal(err)
	}
	old := drainBaseLstat
	drainBaseLstat = func(string, time.Duration) (unix.Stat_t, error, bool) { return unix.Stat_t{}, nil, false }
	t.Cleanup(func() { drainBaseLstat = old })
	simulateNFSCreateThenWrite(t, jfs, "clip.mov.tmp", newBytes, 8)
	if err := jfs.Rename("clip.mov.tmp", "clip.mov"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if got, err := os.ReadFile(dest); err != nil || sha256.Sum256(got) != sha256.Sum256(oldBytes) {
		t.Fatalf("old destination changed at rename: %v", err)
	}
	spool.sweepOnce(0)
	drainAllForTest(t, d)
	if got, err := os.ReadFile(dest); err != nil || sha256.Sum256(got) != sha256.Sum256(oldBytes) {
		t.Fatalf("old destination changed during uncertain publication: %v", err)
	}
	entries, err := os.ReadDir(fuseRoot)
	if err != nil {
		t.Fatal(err)
	}
	var copies int
	for _, entry := range entries {
		if !strings.Contains(entry.Name(), "(conflict ") {
			continue
		}
		copies++
		got, err := os.ReadFile(filepath.Join(fuseRoot, entry.Name()))
		if err != nil || sha256.Sum256(got) != sha256.Sum256(newBytes) {
			t.Fatalf("conflict copy lacks saved bytes: %v", err)
		}
	}
	if copies != 1 {
		t.Fatalf("want one saved-byte conflict copy, got %d", copies)
	}
}
