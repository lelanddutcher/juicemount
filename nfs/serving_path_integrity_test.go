package nfs

// Serving-path data integrity (2026-07-28).
//
// These are the END-TO-END repros for four SILENT-corruption bugs found in the
// shipped 0.4.0 serving path. Every one of them returns wrong bytes (or writes
// bytes into the wrong file) with NO error at any layer — the worst possible
// outcome for editorial media, strictly worse than a hard failure.
//
//	C1  read-after-rename       — the FDPool read slot serves the PRE-rename inode
//	C2  write-after-rename      — the FDPool write slot DESTROYS the renamed file
//	C3  write/read-after-delete — the FDPool serves an fd to the UNLINKED inode
//	C4  writeSizes never cleared on Remove — stale-HIGH size → mmap zero-fill
//
// Root cause for C1-C3: FDPool is keyed by {path, write} — no inode, no
// generation — and NOTHING invalidated it on rename or delete. FlushStale (a
// watchdog FUSE remount) was the pool's only invalidator anywhere in the tree.
// The 2-minute idle bound does not contain it: Get bumps lastUsed on every hit
// and onRead re-opens per READ RPC, so a file under sustained playback pins its
// stale fd indefinitely.
//
// Each test drives juiceFS — the billy.Filesystem layer the go-nfs fork calls
// for RENAME/REMOVE/READ/WRITE/GETATTR — against a temp dir standing in for the
// FUSE mount, the same stand-in the spool and dir-refresh tests use.

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

// newIntegrityHarness builds a juiceFS over a temp FUSE root + temp SQLite
// mirror with NO spool wired, so writes take the LEGACY in-place fdPool path —
// which is exactly the path C2/C3 corrupt (an in-place modify of a file that
// already lives on FUSE never routes through the spool; see OpenFile).
func newIntegrityHarness(t *testing.T) (*juiceFS, *metadata.Store, string) {
	t.Helper()

	// Process-global state another test could have left dirty.
	pin.SetOffline(false)
	pin.ResetFUSEIdentityForTest()
	t.Cleanup(func() {
		pin.SetOffline(false)
		pin.ResetFUSEIdentityForTest()
	})

	fuseRoot := t.TempDir()
	store, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	h := NewHandler(store, fuseRoot)
	t.Cleanup(func() {
		h.StopHandler()
		_ = store.Close()
	})
	return &juiceFS{handler: h}, store, fuseRoot
}

// seedFile writes `content` at fuseRoot/rel and mirrors it, so the read path
// (which requires a mirror entry to reach the FDPool) can serve it.
func seedFile(t *testing.T, jfs *juiceFS, store *metadata.Store, fuseRoot, rel string, content []byte) {
	t.Helper()
	full := filepath.Join(fuseRoot, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", rel, err)
	}
	if err := os.WriteFile(full, content, 0o644); err != nil {
		t.Fatalf("seed %s: %v", rel, err)
	}
	mirror(t, store, fuseRoot, rel, false)
}

// mirror inserts (or refreshes) the metadata entry for rel with the REAL
// on-disk size, mimicking what a scan/reconcile would record. A fresh unique
// inode is minted per call: the mirror's inode is deliberately NOT load-bearing
// here — the FDPool is keyed by PATH ALONE, which is the entire bug — and
// minting a new one per recreation matches what a real re-create produces.
func mirror(t *testing.T, store *metadata.Store, fuseRoot, rel string, isDir bool) {
	t.Helper()
	full := filepath.Join(fuseRoot, rel)
	fi, err := os.Stat(full)
	if err != nil {
		t.Fatalf("stat %s: %v", full, err)
	}
	testInodeCounter++
	store.InsertToCache(metadata.MakeEntry(rel, isDir, fi.Size(), fi.ModTime(), testInodeCounter))
}

var testInodeCounter uint64 = 1 << 40

// readVia reads the whole file through the RPC-shaped read path
// (OpenFile → ReadAt → Close), which is what onRead does per READ RPC.
func readVia(t *testing.T, jfs *juiceFS, rel string, n int) []byte {
	t.Helper()
	f, err := jfs.OpenFile(rel, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile(read %s): %v", rel, err)
	}
	defer f.Close()
	buf := make([]byte, n)
	got, err := f.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt(%s): %v", rel, err)
	}
	return buf[:got]
}

// writeVia writes through the RPC-shaped write path
// (OpenFile(O_RDWR) → WriteAt → Close), which is what onWrite does per WRITE RPC.
func writeVia(t *testing.T, jfs *juiceFS, rel string, off int64, p []byte) {
	t.Helper()
	f, err := jfs.OpenFile(rel, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile(write %s): %v", rel, err)
	}
	wa, ok := f.(io.WriterAt)
	if !ok {
		f.Close()
		t.Fatalf("write handle for %s is not an io.WriterAt (%T)", rel, f)
	}
	if _, err := wa.WriteAt(p, off); err != nil {
		f.Close()
		t.Fatalf("WriteAt(%s): %v", rel, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close(%s): %v", rel, err)
	}
}

func onDisk(t *testing.T, fuseRoot, rel string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(fuseRoot, rel))
	if err != nil {
		t.Fatalf("read on-disk %s: %v", rel, err)
	}
	return b
}

// TestRenameThenRecreateServesNewBytes is C1.
//
// Repro: read reel_A.mov (the pool caches fd→inode A) → mv reel_A.mov
// reel_A_OLD.mov → create a NEW reel_A.mov → read reel_A.mov. Pre-fix the pool
// hands back inode A's fd and the reader gets the MOVED file's bytes. The
// payloads are deliberately the SAME LENGTH: that is the silent case — right
// size, wrong content, no short read, no error.
func TestRenameThenRecreateServesNewBytes(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)

	old := []byte("AAAAAAAA")
	seedFile(t, jfs, store, fuseRoot, "reel_A.mov", old)

	if got := readVia(t, jfs, "reel_A.mov", len(old)); !bytes.Equal(got, old) {
		t.Fatalf("pre-rename read = %q, want %q", got, old)
	}

	if err := jfs.Rename("reel_A.mov", "reel_A_OLD.mov"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	fresh := []byte("BBBBBBBB") // same length as `old` — the SILENT case
	seedFile(t, jfs, store, fuseRoot, "reel_A.mov", fresh)

	got := readVia(t, jfs, "reel_A.mov", len(fresh))
	if !bytes.Equal(got, fresh) {
		t.Fatalf("C1 SILENT CORRUPTION: read of the recreated reel_A.mov returned %q, want %q "+
			"(the pooled fd is still serving the renamed-away file's bytes)", got, fresh)
	}
	// The renamed-away file must be untouched.
	if got := onDisk(t, fuseRoot, "reel_A_OLD.mov"); !bytes.Equal(got, old) {
		t.Fatalf("archived file = %q, want %q", got, old)
	}
}

// TestWriteAfterRenameLandsInNewFile is C2 — the worst of the four.
//
// Repro: mv project.prproj project_v1.prproj → an app writes a new
// project.prproj in place (the legacy non-spool write path: an in-place modify
// of a file already on FUSE never routes through the spool) → pre-fix
// GetWrite returns the PRE-rename fd, so every WriteAt lands INSIDE
// project_v1.prproj, destroying the archived version, and the new file is
// never written. This is the atomic-save / "keep a dated copy" workflow every
// NLE and Office-style app performs.
func TestWriteAfterRenameLandsInNewFile(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)

	archive := []byte("ARCHIVE1")
	seedFile(t, jfs, store, fuseRoot, "project.prproj", archive)

	// Populate the pool's WRITE slot for project.prproj, exactly as any prior
	// WRITE RPC on this path would (go-nfs runs OpenFile→Write→Close per RPC).
	writeVia(t, jfs, "project.prproj", 0, archive)

	if err := jfs.Rename("project.prproj", "project_v1.prproj"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// The app now writes a brand-new original at the ORIGINAL name.
	seedFile(t, jfs, store, fuseRoot, "project.prproj", make([]byte, len(archive)))
	fresh := []byte("NEWDATA!")
	writeVia(t, jfs, "project.prproj", 0, fresh)

	if got := onDisk(t, fuseRoot, "project.prproj"); !bytes.Equal(got, fresh) {
		t.Fatalf("C2 DATA LOSS: new project.prproj = %q, want %q "+
			"(the write went through the pre-rename fd)", got, fresh)
	}
	if got := onDisk(t, fuseRoot, "project_v1.prproj"); !bytes.Equal(got, archive) {
		t.Fatalf("C2 DATA LOSS: the ARCHIVED project_v1.prproj was overwritten — got %q, want %q",
			got, archive)
	}
}

// TestDeleteThenRecreateServesNewBytes is C3, read direction: rm take_07.mov →
// recreate → read. Pre-fix the pool serves the fd to the deleted generation.
func TestDeleteThenRecreateServesNewBytes(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)

	old := []byte("OLDOLD77")
	seedFile(t, jfs, store, fuseRoot, "take_07.mov", old)
	if got := readVia(t, jfs, "take_07.mov", len(old)); !bytes.Equal(got, old) {
		t.Fatalf("pre-delete read = %q, want %q", got, old)
	}

	if err := jfs.Remove("take_07.mov"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	fresh := []byte("NEWNEW77") // same length — the silent case
	seedFile(t, jfs, store, fuseRoot, "take_07.mov", fresh)

	if got := readVia(t, jfs, "take_07.mov", len(fresh)); !bytes.Equal(got, fresh) {
		t.Fatalf("C3 SILENT CORRUPTION: read of the recreated take_07.mov returned %q, want %q "+
			"(the pooled fd still points at the deleted inode)", got, fresh)
	}
}

// TestWriteAfterDeleteLandsInNewFile is C3, write direction — the silent
// data-LOSS half: rm take_08.mov → recreate → write. Pre-fix GetWrite returns
// the fd to the UNLINKED inode, so every byte goes to a ghost the kernel
// reclaims on close. The recreated file stays empty and no layer errors.
func TestWriteAfterDeleteLandsInNewFile(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)

	seedFile(t, jfs, store, fuseRoot, "take_08.mov", []byte("ORIGINAL"))
	writeVia(t, jfs, "take_08.mov", 0, []byte("ORIGINAL")) // pools the write fd

	if err := jfs.Remove("take_08.mov"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	seedFile(t, jfs, store, fuseRoot, "take_08.mov", make([]byte, 8))
	fresh := []byte("RECOVER!")
	writeVia(t, jfs, "take_08.mov", 0, fresh)

	if got := onDisk(t, fuseRoot, "take_08.mov"); !bytes.Equal(got, fresh) {
		t.Fatalf("C3 DATA LOSS: recreated take_08.mov = %q, want %q "+
			"(the write went to the unlinked inode)", got, fresh)
	}
}

// TestWriteSizesClearedOnRemove is C4 — the #104 Premiere black-frame bug,
// reachable today.
//
// h.writeSizes is MAX-only and Stat/Lstat report it whenever it exceeds the
// mirror's size. juiceFS.Remove never deleted from it and there was no TTL and
// no sweep (the claim in writeFile.Close that it is "cleaned up lazily by the
// next Stat()" was false — Stat and Lstat only READ the map). So: write a big
// file to X.mov → rm X.mov → create a SMALL X.mov → stat reports the BIG size →
// an mmap reader (any NLE) faults past real EOF and the kernel ZERO-FILLS the
// gap. Black frames, no error.
func TestWriteSizesClearedOnRemove(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)

	big := bytes.Repeat([]byte("B"), 4096)
	seedFile(t, jfs, store, fuseRoot, "X.mov", big)
	writeVia(t, jfs, "X.mov", 0, big) // trackWriteSize → writeSizes["X.mov"] = 4096

	jfs.handler.writeSizeMu.Lock()
	sz, ok := jfs.handler.writeSizes["X.mov"]
	jfs.handler.writeSizeMu.Unlock()
	if !ok || sz != int64(len(big)) {
		t.Fatalf("precondition: writeSizes[X.mov] = (%d, %v), want (%d, true)", sz, ok, len(big))
	}

	if err := jfs.Remove("X.mov"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	jfs.handler.writeSizeMu.Lock()
	_, stillThere := jfs.handler.writeSizes["X.mov"]
	jfs.handler.writeSizeMu.Unlock()
	if stillThere {
		t.Fatalf("C4: writeSizes entry survived Remove")
	}

	small := []byte("small!") // 6 bytes
	seedFile(t, jfs, store, fuseRoot, "X.mov", small)

	fi, err := jfs.Stat("X.mov")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() != int64(len(small)) {
		t.Fatalf("C4 SILENT CORRUPTION: Stat reports %d for a %d-byte file — an mmap reader "+
			"faults past real EOF and the kernel zero-fills the gap (#104)", fi.Size(), len(small))
	}
	li, err := jfs.Lstat("X.mov")
	if err != nil {
		t.Fatalf("Lstat: %v", err)
	}
	if li.Size() != int64(len(small)) {
		t.Fatalf("C4: Lstat reports %d for a %d-byte file (same stale-HIGH map, second reader)",
			li.Size(), len(small))
	}
}

// TestEvictStaleWriteSizesAgesOutAndRespectsGuards covers the backstop sweep
// for the paths that vanish WITHOUT a Remove RPC (remote delete, reconcile
// prune, crashed writer) — plus the two guards that keep the sweep from ever
// under-reporting a live file's size.
func TestEvictStaleWriteSizesAgesOutAndRespectsGuards(t *testing.T) {
	jfs, _, _ := newIntegrityHarness(t)
	h := jfs.handler

	h.trackWriteSize("gone.mov", 1<<30)
	h.trackWriteSize("still-writing.mov", 1<<30)
	h.incActiveWriter("still-writing.mov")
	defer h.decActiveWriter("still-writing.mov")

	// Nothing is old enough yet.
	if n := h.evictStaleWriteSizes(time.Hour); n != 0 {
		t.Fatalf("evicted %d fresh entries, want 0", n)
	}

	// Backdate both marks past the TTL.
	h.writeSizeMu.Lock()
	old := time.Now().Add(-2 * time.Hour)
	h.writeSizeAt["gone.mov"] = old
	h.writeSizeAt["still-writing.mov"] = old
	h.writeSizeMu.Unlock()

	if n := h.evictStaleWriteSizes(time.Hour); n != 1 {
		t.Fatalf("evicted %d, want exactly 1 (the abandoned mark)", n)
	}
	h.writeSizeMu.Lock()
	_, goneStillThere := h.writeSizes["gone.mov"]
	_, activeStillThere := h.writeSizes["still-writing.mov"]
	h.writeSizeMu.Unlock()
	if goneStillThere {
		t.Fatal("abandoned writeSizes mark survived the sweep")
	}
	if !activeStillThere {
		t.Fatal("sweep evicted a mark with an ACTIVE WRITER — would under-report a live file mid-write")
	}
}

// TestSubtreeRenameServesNewDescendantBytes is the DIRECTORY-rename face of C1:
// one syscall re-parents every descendant, so every descendant's pooled fd goes
// stale at once. Renaming a shoot folder aside and dropping a fresh one in its
// place is routine ("REEL_0065" → "REEL_0065_OLD", re-ingest).
func TestSubtreeRenameServesNewDescendantBytes(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)

	old := []byte("OLDCLIP1")
	seedFile(t, jfs, store, fuseRoot, "dir/clip.mov", old)
	mirror(t, store, fuseRoot, "dir", true)

	if got := readVia(t, jfs, "dir/clip.mov", len(old)); !bytes.Equal(got, old) {
		t.Fatalf("pre-rename descendant read = %q, want %q", got, old)
	}

	if err := jfs.Rename("dir", "dir_old"); err != nil {
		t.Fatalf("Rename(dir): %v", err)
	}

	fresh := []byte("NEWCLIP1") // same length — the silent case
	seedFile(t, jfs, store, fuseRoot, "dir/clip.mov", fresh)
	mirror(t, store, fuseRoot, "dir", true)

	if got := readVia(t, jfs, "dir/clip.mov", len(fresh)); !bytes.Equal(got, fresh) {
		t.Fatalf("C1/subtree SILENT CORRUPTION: dir/clip.mov = %q, want %q "+
			"(a descendant's pooled fd still points into the renamed-away directory)", got, fresh)
	}
	if got := onDisk(t, fuseRoot, "dir_old/clip.mov"); !bytes.Equal(got, old) {
		t.Fatalf("renamed-away descendant = %q, want %q", got, old)
	}
}

// TestSubtreeRenameHookInvalidatesPooledFDs isolates the metadata→nfs seam:
// metadata/ owns the "a subtree moved" signal but cannot import nfs/, so
// RenameSubtree publishes it via Store.SetOnSubtreeRenamed and NewHandler
// translates it into an FDPool invalidation. Driving store.RenameSubtree
// DIRECTLY (not through juiceFS.Rename, which also invalidates on its own)
// proves the hook itself is wired.
func TestSubtreeRenameHookInvalidatesPooledFDs(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)
	h := jfs.handler

	seedFile(t, jfs, store, fuseRoot, "shoot/a.mov", []byte("x"))
	mirror(t, store, fuseRoot, "shoot", true)

	full := filepath.Join(fuseRoot, "shoot/a.mov")
	if _, err := h.fdPool.Get(full); err != nil {
		t.Fatalf("pool Get: %v", err)
	}
	h.fdPool.Release(full)
	if open, _ := h.fdPool.Stats(); open != 1 {
		t.Fatalf("precondition: pool has %d entries, want 1", open)
	}

	store.RenameSubtree("shoot", "shoot_old")

	if open, _ := h.fdPool.Stats(); open != 0 {
		t.Fatalf("RenameSubtree did not invalidate the descendant's pooled fd (pool still has %d entries) "+
			"— Store.SetOnSubtreeRenamed is not wired", open)
	}
}

// plantSubtreeTripwire inserts a pool entry keyed UNDER `parent` and returns a
// checker for whether it survived.
//
// The key is synthetic on purpose: a regular file has no children, so no real
// open can ever produce it, and the ONLY thing that can reach it is
// InvalidateTree's `strings.HasPrefix(k.path, root+"/")` scan. That makes it an
// exact detector for WHICH invalidator a rename dispatched to — a question with
// no other observable answer, because for a file both invalidators drop the same
// two real slots.
func plantSubtreeTripwire(t *testing.T, p *FDPool, parent string) (key fdKey, survived func() bool) {
	t.Helper()
	scratch := filepath.Join(t.TempDir(), "tripwire")
	if err := os.WriteFile(scratch, []byte("x"), 0o644); err != nil {
		t.Fatalf("tripwire seed: %v", err)
	}
	fd, err := os.Open(scratch)
	if err != nil {
		t.Fatalf("tripwire open: %v", err)
	}
	t.Cleanup(func() { fd.Close() })
	k := fdKey{path: parent + "/tripwire", write: false}
	p.mu.Lock()
	p.entries[k] = &poolEntry{fd: fd, lastUsed: time.Now()}
	p.mu.Unlock()
	return k, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		_, ok := p.entries[k]
		return ok
	}
}

func poolHasKey(p *FDPool, k fdKey) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.entries[k]
	return ok
}

// TestFileRenameInvalidatesExactKeyNotTheSubtree is S3.
//
// juiceFS.Rename invalidated BOTH ends with the TREE scan unconditionally, even
// though the mirror entry it already reads tells it the source is a plain file.
// InvalidateTree is an O(len(p.entries)) walk held under the SINGLE pool mutex,
// and a .app / ditto bundle copy is a rename PER FILE while p.entries holds
// thousands of keys from that same copy's parallel WRITE RPCs — so it put two
// full scans per renamed file on the exact lock that produced the 2026-06-14
// convoy (93 goroutines wedged in GetWrite behind this mutex → Finder "error
// 100060"). A file rename must take the exact-key path; the identity guarantee
// is unchanged because POSIX rename cannot change an entry's type.
func TestFileRenameInvalidatesExactKeyNotTheSubtree(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)
	h := jfs.handler

	seedFile(t, jfs, store, fuseRoot, "reel.mov", []byte("AAAA"))
	full := filepath.Join(fuseRoot, "reel.mov")

	if _, err := h.fdPool.Get(full); err != nil {
		t.Fatalf("pool Get: %v", err)
	}
	h.fdPool.Release(full)
	if _, err := h.fdPool.GetWrite(full, os.O_RDWR, 0o644); err != nil {
		t.Fatalf("pool GetWrite: %v", err)
	}
	h.fdPool.ReleaseWrite(full)

	_, tripwireSurvived := plantSubtreeTripwire(t, h.fdPool, full)

	if err := jfs.Rename("reel.mov", "reel_v1.mov"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// Identity guarantee unchanged: the file's own two slots are gone.
	if poolHasKey(h.fdPool, fdKey{path: full, write: false}) {
		t.Error("the renamed file's READ slot survived — C1 is back")
	}
	if poolHasKey(h.fdPool, fdKey{path: full, write: true}) {
		t.Error("the renamed file's WRITE slot survived — C2 is back")
	}
	// ...and the subtree namespace was never walked.
	if !tripwireSurvived() {
		t.Fatal("S3: a FILE rename took InvalidateTree — an O(len(entries)) scan under the pool mutex, " +
			"twice per renamed file, for the whole duration of a bundle copy (the 2026-06-14 convoy lock)")
	}
}

// TestDirectoryRenameStillScansTheSubtree is S3's other half: the dispatch must
// still choose the tree scan when the thing being renamed actually HAS a subtree,
// or every descendant keeps serving its pre-rename inode.
func TestDirectoryRenameStillScansTheSubtree(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)
	h := jfs.handler

	seedFile(t, jfs, store, fuseRoot, "shoot/clip.mov", []byte("AAAA"))
	mirror(t, store, fuseRoot, "shoot", true)

	child := filepath.Join(fuseRoot, "shoot/clip.mov")
	if _, err := h.fdPool.Get(child); err != nil {
		t.Fatalf("pool Get: %v", err)
	}
	h.fdPool.Release(child)

	if err := jfs.Rename("shoot", "shoot_old"); err != nil {
		t.Fatalf("Rename(dir): %v", err)
	}
	if poolHasKey(h.fdPool, fdKey{path: child, write: false}) {
		t.Fatal("a DIRECTORY rename left a descendant's pooled fd behind — every file under the " +
			"moved folder still serves its pre-rename inode (C1/subtree)")
	}
}

// TestUnmirroredRenameFallsBackToTheSubtreeScan pins the conservative direction
// of the S3 dispatch: when the mirror has no entry for the source, the type is
// UNKNOWN, and guessing "file" would silently skip a real subtree. Fall back to
// the scan — a wasted scan costs latency, a missed one costs correctness.
func TestUnmirroredRenameFallsBackToTheSubtreeScan(t *testing.T) {
	jfs, _, fuseRoot := newIntegrityHarness(t)
	h := jfs.handler

	// On FUSE but deliberately NOT mirrored.
	full := filepath.Join(fuseRoot, "unknown.mov")
	if err := os.WriteFile(full, []byte("AAAA"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_, tripwireSurvived := plantSubtreeTripwire(t, h.fdPool, full)

	if err := jfs.Rename("unknown.mov", "unknown_v1.mov"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if tripwireSurvived() {
		t.Fatal("an UNMIRRORED source was treated as a plain file — a directory the mirror has " +
			"evicted would keep every descendant's pooled fd alive")
	}
}

// TestRemotePathInvalidatedHookDropsPooledFDs is R1: the metadata→nfs seam for
// mutations made by ANOTHER writer.
//
// applyEvent (the Redis pub/sub apply path) handled remote deletes and renames
// by updating the MIRROR and nothing else, so a pooled fd outlived a delete or
// rename it never saw. In a product with a farm, ClipLogger and a second Mac all
// writing the same volume, that is C1/C3 with a remote actor: recreate the path
// and fdPool.Get hands back the fd to the previous inode, no error anywhere.
//
// Driving Store.NotifyPathInvalidated DIRECTLY (the public trigger applyEvent
// fires) proves the hook is wired at handler construction, the same way
// TestSubtreeRenameHookInvalidatesPooledFDs does for RenameSubtree.
func TestRemotePathInvalidatedHookDropsPooledFDs(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)
	h := jfs.handler

	seedFile(t, jfs, store, fuseRoot, "peer/clip.mov", []byte("AAAA"))
	mirror(t, store, fuseRoot, "peer", true)
	child := filepath.Join(fuseRoot, "peer/clip.mov")

	pool := func() {
		t.Helper()
		if _, err := h.fdPool.Get(child); err != nil {
			t.Fatalf("pool Get: %v", err)
		}
		h.fdPool.Release(child)
		if open, _ := h.fdPool.Stats(); open != 1 {
			t.Fatalf("precondition: pool has %d entries, want 1", open)
		}
	}

	// A peer DELETED the file (or renamed it away): the file's own slot goes.
	pool()
	store.NotifyPathInvalidated("peer/clip.mov", false)
	if open, _ := h.fdPool.Stats(); open != 0 {
		t.Fatalf("a REMOTE delete/rename left %d pooled fd(s) — the recreated file will be served the "+
			"deleted inode's bytes (Store.SetOnPathInvalidated is not wired)", open)
	}

	// A peer renamed the DIRECTORY: every descendant's slot goes.
	pool()
	store.NotifyPathInvalidated("peer", true)
	if open, _ := h.fdPool.Stats(); open != 0 {
		t.Fatalf("a REMOTE directory rename left %d descendant fd(s) pooled", open)
	}

	// A remote mutation ELSEWHERE must not be collateral damage.
	pool()
	store.NotifyPathInvalidated("peerX", true)
	store.NotifyPathInvalidated("peer/other.mov", false)
	if open, _ := h.fdPool.Stats(); open != 1 {
		t.Fatalf("unrelated remote mutations dropped a live pooled fd (pool has %d entries, want 1)", open)
	}
}

func TestContentInvalidationHookDropsOnlyAffectedMemoryBytes(t *testing.T) {
	jfs, store, _ := newIntegrityHarness(t)
	h := jfs.handler

	seed := func(p string) {
		t.Helper()
		h.memBuf.mu.Lock()
		h.memBuf.entries[p] = &memBufEntry{data: []byte("old"), size: 3}
		h.memBuf.totalSize += 3
		h.memBuf.mu.Unlock()
	}
	has := func(p string) bool {
		h.memBuf.mu.Lock()
		defer h.memBuf.mu.Unlock()
		_, ok := h.memBuf.entries[p]
		return ok
	}

	seed("peer/clip.mov")
	seed("peer/sub/clip.mov")
	seed("peer-old/keep.mov")
	store.NotifyContentInvalidated("peer/clip.mov", false)
	if has("peer/clip.mov") || !has("peer/sub/clip.mov") || !has("peer-old/keep.mov") {
		t.Fatal("file content invalidation did not stay exact-path scoped")
	}

	store.NotifyContentInvalidated("peer", true)
	if has("peer/sub/clip.mov") || !has("peer-old/keep.mov") {
		t.Fatal("directory content invalidation crossed a sibling prefix or missed a descendant")
	}

	store.NotifyContentReset()
	if has("peer-old/keep.mov") {
		t.Fatal("content reset left buffered bytes behind")
	}
}

func TestReadOpenDefersFUSEUntilDirectCacheMiss(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)
	h := jfs.handler
	cr := cache.NewReader(t.TempDir(), cache.DefaultBlockSize, nil)
	h.SetCacheReader(cr)

	seedFile(t, jfs, store, fuseRoot, "media/clip.mov", []byte("coherent-fallback"))
	f, err := jfs.OpenFile("media/clip.mov", os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("lazy OpenFile: %v", err)
	}
	cf, ok := f.(*cachedFile)
	if !ok {
		t.Fatalf("OpenFile returned %T, want *cachedFile", f)
	}
	if cf.fuseFD != nil {
		t.Fatal("read-only OpenFile eagerly acquired FUSE despite an attached direct-cache reader")
	}
	buf := make([]byte, len("coherent-fallback"))
	if n, err := cf.ReadAt(buf, 0); err != nil || n != len(buf) {
		t.Fatalf("fallback ReadAt = %d, %v", n, err)
	}
	if string(buf) != "coherent-fallback" {
		t.Fatalf("fallback bytes = %q", buf)
	}
	if cf.fuseFD == nil {
		t.Fatal("direct-cache miss did not lazily acquire the coherent FUSE descriptor")
	}
	if err := cf.Close(); err != nil {
		t.Fatal(err)
	}
}
