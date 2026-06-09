package nfs

// Phase-1 BUGs 3+4 (docs/LAUNCH_PLAN.md): SETATTR against a spool-shadowed
// path failed. The size arm of SetFileAttributes.Apply opens the path for
// write (routing into the spool) and calls Truncate — which slice C rejected
// for anything but truncate-to-0-on-fresh. The raw (non-NFSStatusError)
// error then (a) leaked the opened handle's spool refcount (BUG 2's primary
// leak), (b) failed cp with rc=1 despite correct bytes (BUG 3), and (c) fell
// through the error formatter to an RPC-level SYSTEM_ERR reply that macOS
// surfaces as EBADRPC "RPC struct is bad" (BUG 4, fio rc=72).
//
// These tests drive the EXACT call shapes internal/nfs/nfs_onsetattr.go and
// nfs_oncommit.go make against the billy layer (SetFileAttributes.Apply,
// Lstat) plus unit coverage for the new spool truncate support.

import (
	"bytes"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
)

// TestSpoolWriteFileTruncateSupportsResize covers the ftruncate shapes NFS
// SETATTR produces against an in-flight spool entry:
//   - extend a fresh entry (fio preallocates with ftruncate before writing)
//   - truncate to the current writtenEnd (cp/copyfile's final ftruncate —
//     must be a no-op that PRESERVES the streaming hash)
//   - shrink below writtenEnd (releases capacity)
func TestSpoolWriteFileTruncateSupportsResize(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	handler := minimalHandlerForTest()
	defer handler.StopHandler()

	e, err := s.OpenWrite("/resize.bin")
	if err != nil {
		t.Fatalf("open write: %v", err)
	}
	wf := &spoolWriteFile{name: "/resize.bin", entry: e, handler: handler}

	// Extend a fresh entry (fio shape).
	if err := wf.Truncate(4096); err != nil {
		t.Fatalf("Truncate(4096) on fresh entry must succeed (fio preallocate): %v", err)
	}
	if got := e.WrittenEnd(); got != 4096 {
		t.Errorf("WrittenEnd after extend = %d, want 4096", got)
	}
	if fi, err := os.Stat(e.SpoolFilePath()); err != nil || fi.Size() != 4096 {
		t.Errorf("on-disk spool file size after extend: fi=%v err=%v", fi, err)
	}
	if used, _ := s.Capacity(); used != 4096 {
		t.Errorf("capacity after extend = %d, want 4096", used)
	}

	// Shrink (releases the difference).
	if err := wf.Truncate(2048); err != nil {
		t.Fatalf("Truncate(2048) shrink must succeed: %v", err)
	}
	if got := e.WrittenEnd(); got != 2048 {
		t.Errorf("WrittenEnd after shrink = %d, want 2048", got)
	}
	if used, _ := s.Capacity(); used != 2048 {
		t.Errorf("capacity after shrink = %d, want 2048", used)
	}

	// Same-size truncate is a no-op success.
	if err := wf.Truncate(2048); err != nil {
		t.Errorf("Truncate(samesize): %v", err)
	}

	if err := wf.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestSpoolWriteFileTruncateNoopPreservesStreamingHash: cp's final
// ftruncate(dst, size) where size == bytes already written must not
// invalidate the streaming SHA — the drainer's end-to-end verification is a
// core integrity property for the dominant copy workload.
func TestSpoolWriteFileTruncateNoopPreservesStreamingHash(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	handler := minimalHandlerForTest()
	defer handler.StopHandler()

	e, err := s.OpenWrite("/cphash.bin")
	if err != nil {
		t.Fatalf("open write: %v", err)
	}
	wf := &spoolWriteFile{name: "/cphash.bin", entry: e, handler: handler}

	payload := []byte("sequential payload whose streaming hash must survive")
	if _, err := wf.WriteAt(payload, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := wf.Truncate(int64(len(payload))); err != nil {
		t.Fatalf("no-op truncate (cp shape): %v", err)
	}
	if !e.StreamingHashValid() {
		t.Error("no-op truncate invalidated the streaming hash — drainer would skip SHA verification on every cp")
	}
	if err := wf.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Per-RPC Close releases the handle without finalizing; the idle
	// sweeper finalizes (and seals the streaming SHA).
	if n := s.sweepOnce(0); n != 1 {
		t.Fatalf("sweepOnce finalized %d, want 1", n)
	}
	want := sha256.Sum256(payload)
	if got := e.SHA256(); !bytes.Equal(got, want[:]) {
		t.Error("streaming SHA mismatch after no-op truncate")
	}
}

// TestSpoolWriteFileTruncateHonorsCapacity: extending past the capacity cap
// must fail with ErrSpoolFull and reserve nothing.
func TestSpoolWriteFileTruncateHonorsCapacity(t *testing.T) {
	s := newTestSpoolStore(t, 1024)
	handler := minimalHandlerForTest()
	defer handler.StopHandler()

	e, err := s.OpenWrite("/cap.bin")
	if err != nil {
		t.Fatalf("open write: %v", err)
	}
	wf := &spoolWriteFile{name: "/cap.bin", entry: e, handler: handler}
	defer wf.Close()

	if err := wf.Truncate(4096); err != ErrSpoolFull {
		t.Fatalf("Truncate past cap: got %v, want ErrSpoolFull", err)
	}
	if used, _ := s.Capacity(); used != 0 {
		t.Errorf("failed truncate leaked a reservation: used=%d", used)
	}
}

// TestSetattrTruncateOnActiveSpoolEntryFioShape emulates fio's startup
// against a spooled path: CREATE → ftruncate(file_size) → positioned writes.
// Pre-fix the SETATTR size errored (and the reply was RPC-level garbage —
// "RPC struct is bad"). Post-fix the file drains at the truncated size with
// the written prefix intact and zeros beyond.
func TestSetattrTruncateOnActiveSpoolEntryFioShape(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandlerNoDrain(t)
	h := jfs.handler

	// CREATE RPC shape.
	cf, err := jfs.Create("job.0.0")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := cf.Close(); err != nil {
		t.Fatalf("create close: %v", err)
	}

	// SETATTR {size} — exactly what onSetAttr does after parsing sattr3.
	size := uint64(1 << 20)
	attrs := &nfslib.SetFileAttributes{SetSize: &size}
	if err := attrs.Apply(h.Change(jfs), jfs, "job.0.0"); err != nil {
		t.Fatalf("SETATTR size on active spool entry must succeed (fio ftruncate): %v", err)
	}

	// WRITE RPCs for the first 256 KiB.
	data := patternedPayload(256<<10, 41)
	wf, err := jfs.OpenFile("job.0.0", os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open for write: %v", err)
	}
	if _, err := wf.(io.WriterAt).WriteAt(data, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := wf.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if n := spool.sweepOnce(0); n < 1 {
		t.Fatalf("sweepOnce finalized %d, want >=1 (leaked SETATTR handle blocks finalize)", n)
	}
	drainAllForTest(t, drainer)

	got, err := os.ReadFile(filepath.Join(fuseRoot, "job.0.0"))
	if err != nil {
		t.Fatalf("drained file missing: %v", err)
	}
	if len(got) != int(size) {
		t.Fatalf("drained size=%d, want %d (ftruncate-extended length)", len(got), size)
	}
	if !bytes.Equal(got[:len(data)], data) {
		t.Error("written prefix corrupted")
	}
	for i := len(data); i < len(got); i++ {
		if got[i] != 0 {
			t.Fatalf("byte %d past written prefix is %#x, want 0 (sparse tail)", i, got[i])
		}
	}
}

// TestCpSequenceOnSpooledPathSucceedsWithoutLeak emulates the full cp/
// copyfile RPC sequence against a spooled path: CREATE → WRITEs →
// SETATTR{mode} → SETATTR{atime,mtime} → SETATTR{size=writtenEnd} → COMMIT-
// shape stat. Every step must succeed (BUG 3: cp exited rc=1), no write
// handle may leak (BUG 2: 43 stuck `writing` entries), and the bytes must
// drain intact.
func TestCpSequenceOnSpooledPathSucceedsWithoutLeak(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandlerNoDrain(t)
	h := jfs.handler

	payload := patternedPayload(192<<10, 47)
	want := sha256.Sum256(payload)
	simulateNFSCreateThenWrite(t, jfs, "doc.bin", payload, 64<<10)

	entry, ok := spool.LookupActive("doc.bin")
	if !ok {
		t.Fatal("precondition: doc.bin must be spooled")
	}
	changer := h.Change(jfs)

	// fchmod → SETATTR {mode}.
	mode := uint32(0o755)
	if err := (&nfslib.SetFileAttributes{SetMode: &mode}).Apply(changer, jfs, "doc.bin"); err != nil {
		t.Fatalf("SETATTR mode on spooled path: %v", err)
	}

	// futimes → SETATTR {atime, mtime}.
	at := time.Now().Add(-time.Hour)
	mt := time.Now().Add(-30 * time.Minute)
	if err := (&nfslib.SetFileAttributes{SetAtime: &at, SetMtime: &mt}).Apply(changer, jfs, "doc.bin"); err != nil {
		t.Fatalf("SETATTR times on spooled path: %v", err)
	}

	// copyfile's ftruncate(dst, final_size) → SETATTR {size == writtenEnd}.
	size := uint64(len(payload))
	if err := (&nfslib.SetFileAttributes{SetSize: &size}).Apply(changer, jfs, "doc.bin"); err != nil {
		t.Fatalf("SETATTR size==writtenEnd on spooled path (cp's ftruncate): %v", err)
	}

	// COMMIT's server-side shape: post-op stat of the handle path.
	if _, err := jfs.Lstat("doc.bin"); err != nil {
		t.Fatalf("COMMIT-shape Lstat on spooled path: %v", err)
	}

	// BUG 2's primary leak: the SETATTR size arm opened a write handle and
	// dropped it without Close on the (pre-fix) truncate error. The
	// refcount must be back to zero or the sweeper can never finalize.
	if rc := spoolRefcountForTest(entry); rc != 0 {
		t.Fatalf("write-handle refcount leaked by SETATTR sequence: refcount=%d, want 0", rc)
	}

	if n := spool.sweepOnce(0); n < 1 {
		t.Fatalf("sweepOnce finalized %d, want >=1 — entry stuck in writing (the 43-entry leak class)", n)
	}
	drainAllForTest(t, drainer)

	got, err := os.ReadFile(filepath.Join(fuseRoot, "doc.bin"))
	if err != nil {
		t.Fatalf("drained file missing: %v", err)
	}
	if gotSHA := sha256.Sum256(got); gotSHA != want {
		t.Errorf("drained content mismatch: len=%d want=%d", len(got), len(payload))
	}
	if used, _ := spool.Capacity(); used != 0 {
		t.Errorf("capacity not released: used=%d", used)
	}
}

// TestSetattrShrinkReflectsInStatAfterDrain: after a truncate-down, Stat
// must report the SHRUNKEN size — including after the drain, when the sticky
// writeSizes high-water map would otherwise override the metadata size with
// the stale pre-truncate value (green-when-broken stat lie).
func TestSetattrShrinkReflectsInStatAfterDrain(t *testing.T) {
	jfs, spool, drainer, _ := newSpoolWiredHandlerNoDrain(t)
	h := jfs.handler

	payload := patternedPayload(4096, 53)
	simulateNFSCreateThenWrite(t, jfs, "shrunk.bin", payload, 4096)

	size := uint64(2048)
	if err := (&nfslib.SetFileAttributes{SetSize: &size}).Apply(h.Change(jfs), jfs, "shrunk.bin"); err != nil {
		t.Fatalf("SETATTR shrink: %v", err)
	}

	// While spooled: the shadow must report the truncated size.
	if fi, err := jfs.Stat("shrunk.bin"); err != nil || fi.Size() != 2048 {
		t.Fatalf("Stat while spooled: size=%v err=%v, want 2048", fiSize(fi), err)
	}

	if n := spool.sweepOnce(0); n < 1 {
		t.Fatalf("sweepOnce finalized %d, want >=1", n)
	}
	drainAllForTest(t, drainer)

	// After drain: the metadata path must NOT resurrect the stale 4096
	// high-water mark from the sticky writeSizes map.
	fi, err := jfs.Stat("shrunk.bin")
	if err != nil {
		t.Fatalf("Stat after drain: %v", err)
	}
	if fi.Size() != 2048 {
		t.Errorf("Stat after drain reports %d, want 2048 (stale writeSizes high-water leaked through)", fi.Size())
	}
}

func fiSize(fi os.FileInfo) int64 {
	if fi == nil {
		return -1
	}
	return fi.Size()
}
