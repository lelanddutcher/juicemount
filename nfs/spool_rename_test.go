package nfs

// Phase-1 BUG 1 (docs/LAUNCH_PLAN.md): juiceFS.Rename was spool-blind. A
// rename of a path with an active spool entry left the entry keyed at the
// OLD path — the old path kept "existing" via the LookupActive shadow, the
// FUSE rename either failed (file not drained yet) or moved a partial file,
// and the drainer later materialized the OLD path on FUSE (the QA-37
// "resurrection" class, unfixed for rename).
//
// These tests drive the rename through the same billy surface the go-nfs
// fork's onRename calls (fs.Rename) and assert the spool entry's full
// identity migrates: in-memory index key, entry state, SQLite row, and the
// drain TARGET. They are hermetic — tmpdir spool + tmpdir FUSE root.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// newSpoolWiredHandlerNoDrain builds the same stack as newSpoolWiredHandler
// but does NOT attach/start the drainer's dispatch loop, so tests can hold
// entries in `writing`/`ready`/`draining` deterministically and drive drains
// explicitly via DrainOnceForTest.
//
// SetSpool is called with a nil drainer (so StopHandler doesn't try to Stop
// a never-started dispatch loop); the drain-complete metadata sync the
// production wiring installs is replicated via SetOnDrainComplete.
func newSpoolWiredHandlerNoDrain(t *testing.T) (*juiceFS, *SpoolStore, *Drainer, string) {
	t.Helper()
	fuseRoot := t.TempDir()
	spoolDir := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "meta.db")

	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}

	handler := NewHandler(store, fuseRoot)

	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatalf("init spool schema: %v", err)
	}
	meta := metadata.NewSpoolStore(store.DB())
	spool, err := NewSpoolStore(spoolDir, 0, meta)
	if err != nil {
		t.Fatalf("new spool store: %v", err)
	}
	drainer, err := NewDrainer(spool, DrainerConfig{
		FuseRoot:     fuseRoot,
		BackoffBase:  5 * time.Millisecond,
		PollFallback: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new drainer: %v", err)
	}
	handler.SetSpool(spool, nil)
	drainer.SetOnDrainComplete(handler.onSpoolDrained)
	t.Cleanup(func() {
		handler.StopHandler()
		_ = store.Close()
	})

	return &juiceFS{handler: handler}, spool, drainer, fuseRoot
}

func patternedPayload(n int, seed byte) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i)*7 + seed
	}
	return p
}

func drainAllForTest(t *testing.T, d *Drainer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d.DrainOnceForTest(ctx)
}

// TestRenameMigratesWritingSpoolEntry: rename of a file whose spool entry is
// still in `writing` state (mid Finder copy, pre-finalize). The entry's whole
// identity must follow the rename, and the eventual drain must land at the
// NEW path only.
func TestRenameMigratesWritingSpoolEntry(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandlerNoDrain(t)

	payload := patternedPayload(256<<10, 3)
	want := sha256.Sum256(payload)
	simulateNFSCreateThenWrite(t, jfs, "src.mov", payload, 64<<10)

	if _, ok := spool.LookupActive("src.mov"); !ok {
		t.Fatal("precondition: src.mov should have an active spool entry")
	}

	if err := jfs.Rename("src.mov", "dst.mov"); err != nil {
		t.Fatalf("Rename of spooled (writing) file must succeed, got: %v", err)
	}

	if _, ok := spool.LookupActive("src.mov"); ok {
		t.Error("old path still shadowed by the spool index after rename")
	}
	e, ok := spool.LookupActive("dst.mov")
	if !ok {
		t.Fatal("new path has no spool index entry after rename")
	}
	if got := e.NFSPath(); got != "dst.mov" {
		t.Errorf("entry.NFSPath()=%q, want dst.mov", got)
	}

	row, err := spool.Meta().LookupByPath("dst.mov")
	if err != nil {
		t.Fatalf("SQL row for dst.mov missing after rename: %v", err)
	}
	if row.DrainState != metadata.DrainWriting {
		t.Errorf("row state=%s, want writing", row.DrainState)
	}
	if _, err := spool.Meta().LookupByPath("src.mov"); err == nil {
		t.Error("SQL still has an active row for src.mov after rename")
	}

	if n := spool.sweepOnce(0); n < 1 {
		t.Fatalf("sweepOnce finalized %d entries, want >=1", n)
	}
	drainAllForTest(t, drainer)

	got, err := os.ReadFile(filepath.Join(fuseRoot, "dst.mov"))
	if err != nil {
		t.Fatalf("drained file missing at NEW path: %v", err)
	}
	if gotSHA := sha256.Sum256(got); gotSHA != want {
		t.Errorf("drained content mismatch at new path: len=%d want=%d", len(got), len(payload))
	}
	if _, err := os.Lstat(filepath.Join(fuseRoot, "src.mov")); !os.IsNotExist(err) {
		t.Errorf("OLD path resurrected on FUSE after drain (err=%v)", err)
	}
	if used, _ := spool.Capacity(); used != 0 {
		t.Errorf("spool capacity not released after drain: used=%d", used)
	}
	if _, ok := spool.LookupActive("dst.mov"); ok {
		t.Error("index entry not evicted after drain completed at new path")
	}
}

// TestRenameMigratesReadySpoolEntry: rename after finalize but before the
// drainer claims the row. The queued drain's target must follow the rename.
func TestRenameMigratesReadySpoolEntry(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandlerNoDrain(t)

	payload := patternedPayload(128<<10, 9)
	want := sha256.Sum256(payload)
	simulateNFSCreateThenWrite(t, jfs, "ready.bin", payload, 32<<10)

	if n := spool.sweepOnce(0); n < 1 {
		t.Fatalf("sweepOnce finalized %d entries, want >=1", n)
	}
	row, err := spool.Meta().LookupByPath("ready.bin")
	if err != nil || row.DrainState != metadata.DrainReady {
		t.Fatalf("precondition: want ready row, got %+v err=%v", row, err)
	}

	if err := jfs.Rename("ready.bin", "renamed.bin"); err != nil {
		t.Fatalf("Rename of spooled (ready) file must succeed, got: %v", err)
	}

	rowNew, err := spool.Meta().LookupByPath("renamed.bin")
	if err != nil {
		t.Fatalf("SQL row for renamed.bin missing: %v", err)
	}
	if rowNew.DrainState != metadata.DrainReady {
		t.Errorf("row state=%s, want ready", rowNew.DrainState)
	}
	if rowNew.SpoolFile != row.SpoolFile {
		t.Errorf("spool file changed across rename: %q → %q", row.SpoolFile, rowNew.SpoolFile)
	}

	drainAllForTest(t, drainer)

	got, err := os.ReadFile(filepath.Join(fuseRoot, "renamed.bin"))
	if err != nil {
		t.Fatalf("drained file missing at NEW path: %v", err)
	}
	if gotSHA := sha256.Sum256(got); gotSHA != want {
		t.Error("drained content mismatch at new path")
	}
	if _, err := os.Lstat(filepath.Join(fuseRoot, "ready.bin")); !os.IsNotExist(err) {
		t.Errorf("OLD path resurrected on FUSE after drain (err=%v)", err)
	}
	if used, _ := spool.Capacity(); used != 0 {
		t.Errorf("spool capacity not released: used=%d", used)
	}
}

// TestRenameDuringActiveDrainRequeuesToNewPath: the drainer has CLAIMED the
// row (state=draining) and is mid-copy to the old FUSE target when the
// rename lands. The in-flight drain must be cancelled via the QA-37
// MarkDone=false contract (so its FUSE write is undone) and the entry
// requeued as `ready` at the NEW path.
func TestRenameDuringActiveDrainRequeuesToNewPath(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandlerNoDrain(t)

	payload := patternedPayload(96<<10, 5)
	want := sha256.Sum256(payload)
	simulateNFSCreateThenWrite(t, jfs, "mid.bin", payload, 32<<10)
	if n := spool.sweepOnce(0); n < 1 {
		t.Fatalf("sweepOnce finalized %d entries, want >=1", n)
	}
	row, err := spool.Meta().LookupByPath("mid.bin")
	if err != nil {
		t.Fatalf("ready row: %v", err)
	}

	// Simulate the worker claim + partial FUSE write, exactly as
	// dispatchRow/drainOne would.
	claimed, err := spool.Meta().MarkDraining(row.ID)
	if err != nil || !claimed {
		t.Fatalf("claim: claimed=%v err=%v", claimed, err)
	}
	partial := payload[:16<<10]
	if err := os.WriteFile(filepath.Join(fuseRoot, "mid.bin"), partial, 0o644); err != nil {
		t.Fatalf("seed partial dest: %v", err)
	}

	if err := jfs.Rename("mid.bin", "moved.bin"); err != nil {
		t.Fatalf("Rename during active drain must succeed, got: %v", err)
	}

	// The old row must be cancelled and a fresh ready row queued at the
	// new path, pointing at the SAME spool file.
	rowNew, err := spool.Meta().LookupByPath("moved.bin")
	if err != nil {
		t.Fatalf("requeued row at new path missing: %v", err)
	}
	if rowNew.DrainState != metadata.DrainReady {
		t.Errorf("requeued row state=%s, want ready", rowNew.DrainState)
	}
	if rowNew.SpoolFile != row.SpoolFile {
		t.Errorf("requeued row spool file %q, want %q", rowNew.SpoolFile, row.SpoolFile)
	}
	if rowNew.ID == row.ID {
		t.Errorf("draining row must be requeued under a NEW id (old in-flight drain must observe cancellation)")
	}

	// The stale in-flight worker finishes its copy and calls
	// MarkDrainComplete with ITS claimed identity — it must be told the row
	// was cancelled (done=false → it undoes its FUSE write, QA-37 contract),
	// and the shared spool file must survive for the requeued drain.
	done, err := spool.MarkDrainComplete(row.ID, "mid.bin", row.SpoolFile, row.Size)
	if err != nil {
		t.Fatalf("stale MarkDrainComplete: %v", err)
	}
	if done {
		t.Fatal("stale drain was allowed to complete at the OLD path — rename did not cancel it")
	}
	_ = os.Remove(filepath.Join(fuseRoot, "mid.bin")) // the worker's undo step
	if _, err := os.Lstat(row.SpoolFile); err != nil {
		t.Fatalf("spool file must survive the cancelled drain (requeued row needs it): %v", err)
	}

	// The index entry must have adopted the requeued identity so the drain
	// completion at the new path evicts it.
	e, ok := spool.LookupActive("moved.bin")
	if !ok {
		t.Fatal("no index entry at new path")
	}
	if e.ID() != rowNew.ID {
		t.Errorf("index entry id=%d, want requeued id=%d (drain-complete eviction would miss)", e.ID(), rowNew.ID)
	}

	drainAllForTest(t, drainer)

	got, err := os.ReadFile(filepath.Join(fuseRoot, "moved.bin"))
	if err != nil {
		t.Fatalf("requeued drain did not land at new path: %v", err)
	}
	if gotSHA := sha256.Sum256(got); gotSHA != want {
		t.Error("requeued drain content mismatch")
	}
	if _, err := os.Lstat(filepath.Join(fuseRoot, "mid.bin")); !os.IsNotExist(err) {
		t.Errorf("OLD path present on FUSE after requeued drain (err=%v)", err)
	}
	if used, _ := spool.Capacity(); used != 0 {
		t.Errorf("spool capacity not released: used=%d", used)
	}
	if _, ok := spool.LookupActive("moved.bin"); ok {
		t.Error("index entry not evicted after requeued drain completed")
	}
}

// TestRenameDirectoryMigratesSpooledChildren: cp -R a folder then rename the
// folder immediately (a real Finder pattern). Spooled entries UNDER the
// renamed directory must be prefix-migrated or their drains re-create the
// old directory on FUSE.
func TestRenameDirectoryMigratesSpooledChildren(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandlerNoDrain(t)

	if err := jfs.MkdirAll("proj", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	payloadA := patternedPayload(64<<10, 11)
	payloadB := patternedPayload(48<<10, 13)
	simulateNFSCreateThenWrite(t, jfs, "proj/a.bin", payloadA, 32<<10)
	simulateNFSCreateThenWrite(t, jfs, "proj/b.bin", payloadB, 32<<10)

	if err := jfs.Rename("proj", "final"); err != nil {
		t.Fatalf("directory rename must succeed: %v", err)
	}

	if _, ok := spool.LookupActive("proj/a.bin"); ok {
		t.Error("child a.bin still indexed under OLD directory prefix")
	}
	if _, ok := spool.LookupActive("final/a.bin"); !ok {
		t.Error("child a.bin not indexed under NEW directory prefix")
	}
	if _, ok := spool.LookupActive("final/b.bin"); !ok {
		t.Error("child b.bin not indexed under NEW directory prefix")
	}

	if n := spool.sweepOnce(0); n < 2 {
		t.Fatalf("sweepOnce finalized %d entries, want >=2", n)
	}
	drainAllForTest(t, drainer)

	gotA, errA := os.ReadFile(filepath.Join(fuseRoot, "final", "a.bin"))
	if errA != nil {
		t.Fatalf("a.bin missing under new dir: %v", errA)
	}
	if !bytes.Equal(gotA, payloadA) {
		t.Error("a.bin content mismatch under new dir")
	}
	if got, err := os.ReadFile(filepath.Join(fuseRoot, "final", "b.bin")); err != nil || !bytes.Equal(got, payloadB) {
		t.Errorf("b.bin under new dir: err=%v match=%v", err, err == nil && bytes.Equal(got, payloadB))
	}
	if _, err := os.Lstat(filepath.Join(fuseRoot, "proj")); !os.IsNotExist(err) {
		t.Errorf("OLD directory present on FUSE after drains (err=%v) — drain target did not follow dir rename", err)
	}
	if used, _ := spool.Capacity(); used != 0 {
		t.Errorf("spool capacity not released: used=%d", used)
	}
}

// TestRenameOntoActiveSpoolPathCancelsDestination: POSIX rename replaces the
// destination. If the DESTINATION has its own active spool entry, that entry
// must be cancelled (QA-37 semantics) — otherwise its stale drain later
// overwrites the renamed file with the destination's OLD bytes.
func TestRenameOntoActiveSpoolPathCancelsDestination(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandlerNoDrain(t)

	payloadA := patternedPayload(64<<10, 17)
	payloadB := patternedPayload(80<<10, 23)
	wantA := sha256.Sum256(payloadA)
	simulateNFSCreateThenWrite(t, jfs, "a.bin", payloadA, 32<<10)
	simulateNFSCreateThenWrite(t, jfs, "b.bin", payloadB, 32<<10)

	rowB, err := spool.Meta().LookupByPath("b.bin")
	if err != nil {
		t.Fatalf("b row: %v", err)
	}

	if err := jfs.Rename("a.bin", "b.bin"); err != nil {
		t.Fatalf("rename onto spooled destination must succeed: %v", err)
	}

	// b.bin's surviving row must be A's migrated entry, not B's old one.
	row, err := spool.Meta().LookupByPath("b.bin")
	if err != nil {
		t.Fatalf("row at destination missing: %v", err)
	}
	if row.SpoolFile == rowB.SpoolFile {
		t.Fatal("destination still backed by the OLD destination entry — replaced file would resurrect")
	}
	e, ok := spool.LookupActive("b.bin")
	if !ok {
		t.Fatal("no index entry at destination after rename")
	}
	if got := e.WrittenEnd(); got != int64(len(payloadA)) {
		t.Errorf("destination entry WrittenEnd=%d, want %d (source's bytes)", got, len(payloadA))
	}
	if _, ok := spool.LookupActive("a.bin"); ok {
		t.Error("source path still indexed after rename")
	}

	if n := spool.sweepOnce(0); n < 1 {
		t.Fatalf("sweepOnce finalized %d entries, want >=1", n)
	}
	drainAllForTest(t, drainer)

	got, err := os.ReadFile(filepath.Join(fuseRoot, "b.bin"))
	if err != nil {
		t.Fatalf("destination missing on FUSE after drain: %v", err)
	}
	if gotSHA := sha256.Sum256(got); gotSHA != wantA {
		t.Error("destination content is not the renamed source's bytes")
	}
	if _, err := os.Lstat(filepath.Join(fuseRoot, "a.bin")); !os.IsNotExist(err) {
		t.Errorf("source path present on FUSE (err=%v)", err)
	}
	if used, _ := spool.Capacity(); used != 0 {
		t.Errorf("capacity not fully released (dest entry's reservation leaked?): used=%d", used)
	}
}

// TestRenameNonSpooledFileKeepsLegacyBehavior: a file that lives only on
// FUSE (no spool entry) renames exactly as before.
func TestRenameNonSpooledFileKeepsLegacyBehavior(t *testing.T) {
	jfs, spool, _, fuseRoot := newSpoolWiredHandlerNoDrain(t)

	data := []byte("legacy bytes")
	if err := os.WriteFile(filepath.Join(fuseRoot, "old.bin"), data, 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := jfs.Rename("old.bin", "new.bin"); err != nil {
		t.Fatalf("legacy rename: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(fuseRoot, "new.bin"))
	if err != nil || !bytes.Equal(got, data) {
		t.Errorf("legacy rename result: err=%v", err)
	}
	if _, err := os.Lstat(filepath.Join(fuseRoot, "old.bin")); !os.IsNotExist(err) {
		t.Errorf("old path still on FUSE: %v", err)
	}
	if n := spool.Index().Len(); n != 0 {
		t.Errorf("legacy rename touched the spool index (len=%d)", n)
	}

	// A rename of a path that exists NOWHERE must still error (clients
	// depend on ENOENT here).
	if err := jfs.Rename("ghost.bin", "whatever.bin"); err == nil {
		t.Error("rename of nonexistent path must fail")
	}
}

// TestRenameThenContinuedWritesViaNewPath: NFSv3 clients keep writing through
// the file HANDLE after a rename; the handle re-resolves to the new path.
// Writes arriving at the new path must land in the SAME spool entry.
func TestRenameThenContinuedWritesViaNewPath(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandlerNoDrain(t)

	payload := patternedPayload(128<<10, 29)
	want := sha256.Sum256(payload)
	half := len(payload) / 2

	simulateNFSCreateThenWrite(t, jfs, "tmp.part", payload[:half], 32<<10)

	if err := jfs.Rename("tmp.part", "movie.mov"); err != nil {
		t.Fatalf("rename mid-write: %v", err)
	}

	// Continue the write at the NEW path (the shape onWrite produces after
	// FromHandle re-resolution).
	wf, err := jfs.OpenFile("movie.mov", os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile after rename: %v", err)
	}
	if _, err := wf.(io.WriterAt).WriteAt(payload[half:], int64(half)); err != nil {
		t.Fatalf("WriteAt after rename: %v", err)
	}
	if err := wf.Close(); err != nil {
		t.Fatalf("Close after rename: %v", err)
	}

	if n := spool.sweepOnce(0); n < 1 {
		t.Fatalf("sweepOnce finalized %d entries, want >=1", n)
	}
	drainAllForTest(t, drainer)

	got, err := os.ReadFile(filepath.Join(fuseRoot, "movie.mov"))
	if err != nil {
		t.Fatalf("drained file missing: %v", err)
	}
	if gotSHA := sha256.Sum256(got); gotSHA != want {
		t.Errorf("content mismatch: got len=%d want len=%d", len(got), len(payload))
	}
	if _, err := os.Lstat(filepath.Join(fuseRoot, "tmp.part")); !os.IsNotExist(err) {
		t.Errorf("old temp path resurrected (err=%v)", err)
	}
}

// TestDrainerStaleSnapshotClaimDrainsToFreshPath: adversarial-review BUG A.
// The dispatcher's ListReady snapshot can be minutes stale under a worker
// backlog. A rename in that window updates a ready row's nfs_path IN PLACE
// (same id, same state), so the (id, state)-keyed claim still succeeds — and
// draining from the snapshot's path would write to the OLD path, silently
// undoing the rename and leaving a dead index entry shadowing the NEW path
// forever. dispatchRow must drain from a post-claim re-read of the row,
// never from the snapshot it was handed.
func TestDrainerStaleSnapshotClaimDrainsToFreshPath(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandlerNoDrain(t)

	payload := patternedPayload(96<<10, 17)
	simulateNFSCreateThenWrite(t, jfs, "stale-old.bin", payload, 32<<10)
	if n := spool.sweepOnce(0); n != 1 {
		t.Fatalf("sweepOnce finalized %d entries, want 1", n)
	}

	// The dispatcher's stale snapshot: captured BEFORE the rename, exactly
	// what a slot-blocked dispatchRow holds.
	snap, err := spool.Meta().LookupByPath("stale-old.bin")
	if err != nil {
		t.Fatalf("snapshot row: %v", err)
	}

	// User renames while the row sits listed-but-unclaimed.
	if err := jfs.Rename("stale-old.bin", "stale-new.bin"); err != nil {
		t.Fatalf("rename: %v", err)
	}

	// Dispatch with the STALE snapshot (its NFSPath is still the old path).
	if !drainer.dispatchRow(snap) {
		t.Fatalf("dispatchRow reported stop signal")
	}

	// The drain runs in a worker goroutine — wait for the row to terminal.
	deadline := time.Now().Add(10 * time.Second)
	for {
		r, gerr := spool.Meta().Get(snap.ID)
		if gerr == nil && r.DrainState == metadata.DrainDone {
			break
		}
		if time.Now().After(deadline) {
			state := "<unreadable>"
			if gerr == nil {
				state = string(r.DrainState)
			}
			t.Fatalf("drain never reached done: state=%s err=%v", state, gerr)
		}
		time.Sleep(10 * time.Millisecond)
	}

	got, err := os.ReadFile(filepath.Join(fuseRoot, "stale-new.bin"))
	if err != nil {
		t.Fatalf("renamed file missing on FUSE after drain: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("renamed file content mismatch after drain")
	}
	if _, err := os.Lstat(filepath.Join(fuseRoot, "stale-old.bin")); !os.IsNotExist(err) {
		t.Errorf("OLD path materialized on FUSE (err=%v) — drain used the stale snapshot path", err)
	}
	if _, ok := spool.LookupActive("stale-new.bin"); ok {
		t.Error("index entry not evicted after drain at new path")
	}
	if used, _ := spool.Capacity(); used != 0 {
		t.Errorf("spool capacity not released: used=%d", used)
	}
}

// TestRenameUnicodeDirectoryMigratesSpooledChildren: adversarial-review
// BUG B. SQLite substr() counts CHARACTERS while Go len() counts BYTES, so
// a multi-byte directory prefix ("vidéos/" = 8 bytes, 7 chars) never
// matched and unicode directory renames migrated zero children — their
// drains then resurrected the old directory. Accented/CJK names are routine
// for this product's video-editor users.
func TestRenameUnicodeDirectoryMigratesSpooledChildren(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandlerNoDrain(t)

	const oldDir = "vidéos 映像"
	const newDir = "final ✅"
	if err := jfs.MkdirAll(oldDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	payloadA := patternedPayload(64<<10, 21)
	payloadB := patternedPayload(48<<10, 23)
	simulateNFSCreateThenWrite(t, jfs, oldDir+"/clip à.mov", payloadA, 32<<10)
	simulateNFSCreateThenWrite(t, jfs, oldDir+"/クリップ b.mov", payloadB, 32<<10)

	if err := jfs.Rename(oldDir, newDir); err != nil {
		t.Fatalf("unicode directory rename must succeed: %v", err)
	}

	if _, ok := spool.LookupActive(oldDir + "/clip à.mov"); ok {
		t.Error("child still indexed under OLD unicode directory prefix")
	}
	if _, ok := spool.LookupActive(newDir + "/clip à.mov"); !ok {
		t.Error("child not indexed under NEW directory prefix (substr byte/char mismatch?)")
	}
	if _, ok := spool.LookupActive(newDir + "/クリップ b.mov"); !ok {
		t.Error("CJK child not indexed under NEW directory prefix")
	}

	if n := spool.sweepOnce(0); n < 2 {
		t.Fatalf("sweepOnce finalized %d entries, want >=2", n)
	}
	drainAllForTest(t, drainer)

	if got, err := os.ReadFile(filepath.Join(fuseRoot, newDir, "clip à.mov")); err != nil || !bytes.Equal(got, payloadA) {
		t.Errorf("clip à.mov under new dir: err=%v match=%v", err, err == nil && bytes.Equal(got, payloadA))
	}
	if got, err := os.ReadFile(filepath.Join(fuseRoot, newDir, "クリップ b.mov")); err != nil || !bytes.Equal(got, payloadB) {
		t.Errorf("クリップ b.mov under new dir: err=%v match=%v", err, err == nil && bytes.Equal(got, payloadB))
	}
	if _, err := os.Lstat(filepath.Join(fuseRoot, oldDir)); !os.IsNotExist(err) {
		t.Errorf("OLD unicode directory present on FUSE after drains (err=%v)", err)
	}
	if used, _ := spool.Capacity(); used != 0 {
		t.Errorf("spool capacity not released: used=%d", used)
	}
}
