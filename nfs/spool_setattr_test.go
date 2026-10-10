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
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
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

// TestOfflineSetattrZeroReplacesExistingViaSpool reproduces Finder/cp's
// existing-destination sequence during a true Link route outage. SETATTR zero
// must not open FUSE; it establishes a durable replacement spool entry using
// the existing inode, subsequent WRITE RPCs stay local, and recovery drains an
// exact (possibly smaller) replacement rather than preserving the stale size.
func TestOfflineSetattrZeroReplacesExistingViaSpool(t *testing.T) {
	wasOffline := pin.IsOffline()
	pin.SetOffline(true)
	t.Cleanup(func() { pin.SetOffline(wasOffline) })

	jfs, spool, drainer, fuseRoot := newSpoolWiredHandlerNoDrain(t)
	original := patternedPayload(4096, 61)
	fullPath := filepath.Join(fuseRoot, "existing.bin")
	if err := os.WriteFile(fullPath, original, 0o644); err != nil {
		t.Fatalf("seed FUSE file: %v", err)
	}
	const inode = uint64(424242)
	entry := metadata.MakeEntry("existing.bin", false, int64(len(original)), time.Now(), inode)
	if err := jfs.handler.store.Insert(entry); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}
	if err := os.Chmod(fullPath, 0o644); err != nil {
		t.Fatalf("seed FUSE mode: %v", err)
	}
	if err := jfs.handler.Change(jfs).Chmod("existing.bin", 0o600); err != nil {
		t.Fatalf("offline metadata-only chmod: %v", err)
	}
	fiBeforeDrain, err := os.Stat(fullPath)
	if err != nil || fiBeforeDrain.Mode().Perm() != 0o644 {
		t.Fatalf("offline chmod touched FUSE: mode=%v err=%v, want 0644 until drain", fiBeforeDrain.Mode().Perm(), err)
	}
	if gotMode := jfs.handler.store.LookupByPath("existing.bin").Mode.Perm(); gotMode != 0o600 {
		t.Fatalf("offline chmod metadata mode=%#o, want 0600", gotMode)
	}

	zero := uint64(0)
	start := time.Now()
	if err := (&nfslib.SetFileAttributes{SetSize: &zero}).Apply(jfs.handler.Change(jfs), jfs, "existing.bin"); err != nil {
		t.Fatalf("offline SETATTR zero: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("offline SETATTR blocked for %s, want local completion under 500ms", elapsed)
	}
	sentry, ok := spool.LookupActive("existing.bin")
	if !ok {
		t.Fatal("offline replacement did not create a spool shadow")
	}
	if got := sentry.Inode(); got != inode {
		t.Fatalf("replacement inode=%d, want existing inode %d", got, inode)
	}
	if got := jfs.handler.store.LookupByPath("existing.bin").Size; got != 0 {
		t.Fatalf("metadata size after authoritative truncate=%d, want 0", got)
	}
	if got, err := os.ReadFile(fullPath); err != nil || !bytes.Equal(got, original) {
		t.Fatalf("offline SETATTR touched backend bytes: len=%d err=%v", len(got), err)
	}

	replacement := patternedPayload(513, 67)
	wf, err := jfs.OpenFile("existing.bin", os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open replacement spool: %v", err)
	}
	if _, err := wf.(io.WriterAt).WriteAt(replacement, 0); err != nil {
		t.Fatalf("write replacement: %v", err)
	}
	if err := wf.Close(); err != nil {
		t.Fatalf("close replacement: %v", err)
	}

	pin.SetOffline(false)
	if n := spool.sweepOnce(0); n != 1 {
		t.Fatalf("finalized %d replacement entries, want 1", n)
	}
	drainAllForTest(t, drainer)
	got, err := os.ReadFile(fullPath)
	if err != nil {
		t.Fatalf("read drained replacement: %v", err)
	}
	if !bytes.Equal(got, replacement) {
		t.Fatalf("drained replacement mismatch: got len=%d want len=%d", len(got), len(replacement))
	}
	if gotSize := jfs.handler.store.LookupByPath("existing.bin").Size; gotSize != int64(len(replacement)) {
		t.Fatalf("post-drain metadata size=%d, want %d", gotSize, len(replacement))
	}
	fiAfterDrain, err := os.Stat(fullPath)
	if err != nil || fiAfterDrain.Mode().Perm() != 0o600 {
		t.Fatalf("post-drain FUSE mode=%v err=%v, want offline metadata mode 0600", fiAfterDrain.Mode().Perm(), err)
	}
}

// TestOfflinePartialExistingWriteFailsFast proves an outage cannot silently
// manufacture a zero-filled replacement for an operation that depends on old
// backend bytes. Both non-zero truncate and a direct in-place write must return
// promptly, leave no spool entry, and preserve the backend file.
func TestOfflinePartialExistingWriteFailsFast(t *testing.T) {
	wasOffline := pin.IsOffline()
	pin.SetOffline(true)
	t.Cleanup(func() { pin.SetOffline(wasOffline) })

	jfs, spool, _, fuseRoot := newSpoolWiredHandlerNoDrain(t)
	original := []byte("preserve every byte of this existing file")
	fullPath := filepath.Join(fuseRoot, "partial.bin")
	if err := os.WriteFile(fullPath, original, 0o644); err != nil {
		t.Fatalf("seed FUSE file: %v", err)
	}
	entry := metadata.MakeEntry("partial.bin", false, int64(len(original)), time.Now(), 9898)
	if err := jfs.handler.store.Insert(entry); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	size := uint64(7)
	start := time.Now()
	err := (&nfslib.SetFileAttributes{SetSize: &size}).Apply(jfs.handler.Change(jfs), jfs, "partial.bin")
	if err == nil {
		t.Fatal("offline non-zero truncate unexpectedly succeeded")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("offline partial truncate blocked for %s, want under 500ms", elapsed)
	}

	start = time.Now()
	if f, err := jfs.OpenFile("partial.bin", os.O_RDWR, 0o644); err == nil {
		_ = f.Close()
		t.Fatal("offline direct in-place write unexpectedly opened")
	} else if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("offline direct write blocked for %s, want under 500ms", elapsed)
	}
	if spool.HasPending("partial.bin") {
		t.Fatal("unsafe partial operation created a destructive spool entry")
	}
	got, err := os.ReadFile(fullPath)
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("unsafe partial operation changed backend bytes: got=%q err=%v", got, err)
	}
}

// TestOfflineCachedMetadataRewriteSeedsWholeSpoolImage reproduces the Finder
// sequence that aborted the release battery: a directory `._` sidecar drains
// online, the route flips offline, then Finder revisits only a byte range of the
// existing sidecar. A complete cached body makes that partial rewrite safe; the
// untouched bytes must survive the new spool drain exactly.
func TestOfflineCachedMetadataRewriteSeedsWholeSpoolImage(t *testing.T) {
	wasOffline := pin.IsOffline()
	pin.SetOffline(false)
	t.Cleanup(func() { pin.SetOffline(wasOffline) })

	jfs, spool, drainer, fuseRoot := newSpoolWiredHandlerNoDrain(t)
	const name = "tree/._dir2"
	if err := os.MkdirAll(filepath.Dir(filepath.Join(fuseRoot, name)), 0o755); err != nil {
		t.Fatalf("mkdir backend parent: %v", err)
	}
	original := patternedPayload(4096, 79)
	if err := os.WriteFile(filepath.Join(fuseRoot, name), original, 0o644); err != nil {
		t.Fatalf("seed backend sidecar: %v", err)
	}
	mtime := time.Now().Add(-time.Minute)
	// The mirror reflects the backend file it observed (H-8B: the offline
	// rewrite records this length and mtime as its base and the drain checks
	// the backend still matches).
	if err := os.Chtimes(filepath.Join(fuseRoot, name), mtime, mtime); err != nil {
		t.Fatalf("stamp backend sidecar mtime: %v", err)
	}
	const inode = uint64(717171)
	meta := metadata.MakeEntry(name, false, int64(len(original)), mtime, inode)
	if err := jfs.handler.store.Insert(meta); err != nil {
		t.Fatalf("seed sidecar metadata: %v", err)
	}
	jfs.handler.sidecar.put(name, original, meta.Mtime.Unix(), int64(len(original)))

	pin.SetOffline(true)
	wf, err := jfs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("cached offline sidecar rewrite was rejected: %v", err)
	}
	patch := []byte("offline-finder-metadata-continuation")
	const off = int64(152)
	if _, err := wf.(io.WriterAt).WriteAt(patch, off); err != nil {
		t.Fatalf("offline sidecar continuation write: %v", err)
	}
	if err := wf.Close(); err != nil {
		t.Fatalf("close offline sidecar continuation: %v", err)
	}
	sentry, ok := spool.LookupActive(name)
	if !ok {
		t.Fatal("offline sidecar continuation has no spool shadow")
	}
	if got := sentry.Inode(); got != inode {
		t.Fatalf("seeded sidecar inode=%d, want existing inode %d", got, inode)
	}

	pin.SetOffline(false)
	if n := spool.sweepOnce(0); n != 1 {
		t.Fatalf("finalized %d seeded sidecar entries, want 1", n)
	}
	drainAllForTest(t, drainer)

	want := append([]byte(nil), original...)
	copy(want[off:], patch)
	got, err := os.ReadFile(filepath.Join(fuseRoot, name))
	if err != nil {
		t.Fatalf("read rewritten backend sidecar: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("offline sidecar rewrite did not preserve untouched cached ranges")
	}
}

// Finder can encounter an AppleDouble file that was materialized directly by
// macOS/FUSE alongside a newly-created directory, rather than by an NFS CREATE
// routed through the write spool. During a forced-offline copy Finder first
// READs that complete 4 KiB body, using an rsize buffer larger than the file
// (so ReadAt returns n=size, io.EOF), then sends a positioned WRITE. The
// successful bounded offline read is authoritative local evidence and must
// populate the complete-body cache so the rewrite can seed a safe spool image.
func TestOfflineCompleteMetadataReadWithEOFSeedsFollowingRewrite(t *testing.T) {
	wasOffline := pin.IsOffline()
	pin.SetOffline(true)
	t.Cleanup(func() { pin.SetOffline(wasOffline) })

	jfs, spool, _, fuseRoot := newSpoolWiredHandlerNoDrain(t)
	const name = "tree/._dir-fuse-created"
	if err := os.MkdirAll(filepath.Dir(filepath.Join(fuseRoot, name)), 0o755); err != nil {
		t.Fatalf("mkdir backend parent: %v", err)
	}
	original := patternedPayload(4096, 83)
	if err := os.WriteFile(filepath.Join(fuseRoot, name), original, 0o644); err != nil {
		t.Fatalf("seed FUSE-created sidecar: %v", err)
	}
	meta := metadata.MakeEntry(name, false, int64(len(original)), time.Now().Add(-time.Minute), 727272)
	if err := jfs.handler.store.Insert(meta); err != nil {
		t.Fatalf("seed sidecar metadata: %v", err)
	}

	rf, err := jfs.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("offline metadata read open: %v", err)
	}
	buf := make([]byte, 64<<10) // NFS rsize is larger than this 4 KiB file.
	n, readErr := rf.(io.ReaderAt).ReadAt(buf, 0)
	if closeErr := rf.Close(); closeErr != nil {
		t.Fatalf("offline metadata read close: %v", closeErr)
	}
	if n != len(original) || !errors.Is(readErr, io.EOF) || !bytes.Equal(buf[:n], original) {
		t.Fatalf("offline metadata read = n=%d err=%v bytes_match=%v", n, readErr, bytes.Equal(buf[:n], original))
	}
	if _, ok := jfs.handler.offlineMetadataSeed(name); !ok {
		t.Fatal("complete offline metadata read did not populate a safe rewrite seed")
	}

	wf, err := jfs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("offline rewrite after complete read was rejected: %v", err)
	}
	defer wf.Close()
	if _, err := wf.(io.WriterAt).WriteAt([]byte("finder-continuation"), 152); err != nil {
		t.Fatalf("offline positioned rewrite: %v", err)
	}
	if _, ok := spool.LookupActive(name); !ok {
		t.Fatal("offline positioned rewrite did not create a seeded spool shadow")
	}
}

// An online in-place metadata rewrite invalidates the prior complete body at
// write-open. The last close must replace it with the new whole image; without
// that close-side snapshot, a route flip before Finder's next range WRITE
// reproduces the live NFS3ERR_ACCES release blocker.
func TestOfflineMetadataContinuationAfterOnlineInPlaceRewrite(t *testing.T) {
	wasOffline := pin.IsOffline()
	pin.SetOffline(false)
	t.Cleanup(func() { pin.SetOffline(wasOffline) })

	jfs, spool, _, fuseRoot := newSpoolWiredHandlerNoDrain(t)
	const name = "tree/._dir-online-rewrite"
	if err := os.MkdirAll(filepath.Dir(filepath.Join(fuseRoot, name)), 0o755); err != nil {
		t.Fatalf("mkdir backend parent: %v", err)
	}
	original := patternedPayload(4096, 101)
	if err := os.WriteFile(filepath.Join(fuseRoot, name), original, 0o644); err != nil {
		t.Fatalf("seed backend metadata: %v", err)
	}
	meta := metadata.MakeEntry(name, false, int64(len(original)), time.Now().Add(-time.Minute), 737373)
	if err := jfs.handler.store.Insert(meta); err != nil {
		t.Fatalf("seed metadata row: %v", err)
	}
	jfs.handler.sidecar.put(name, original, meta.Mtime.Unix(), int64(len(original)))

	onlinePatch := []byte("online-prefix-rewrite")
	wf, err := jfs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("online open: %v", err)
	}
	if _, err := wf.(io.WriterAt).WriteAt(onlinePatch, 0); err != nil {
		t.Fatalf("online range write: %v", err)
	}
	if err := wf.Close(); err != nil {
		t.Fatalf("online close: %v", err)
	}

	pin.SetOffline(true)
	wf, err = jfs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("offline continuation lacked refreshed complete image: %v", err)
	}
	defer wf.Close()
	seeded, ok := spool.LookupActive(name)
	if !ok {
		t.Fatal("offline continuation did not create a seeded spool image")
	}
	rf, err := seeded.OpenForRead()
	if err != nil {
		t.Fatalf("open seeded image: %v", err)
	}
	got, err := io.ReadAll(rf)
	_ = rf.Close()
	if err != nil {
		t.Fatalf("read seeded image: %v", err)
	}
	want := append([]byte(nil), original...)
	copy(want, onlinePatch)
	if !bytes.Equal(got, want) {
		t.Fatal("offline seed did not contain the complete post-rewrite body")
	}
}

// Preserve a complete local-spool body at the per-WRITE close boundary. This
// is the exact 152-byte-header → later offset-152 Finder shape from the live
// failure; cancelling the first generation simulates its absent shadow and
// proves the retained body is independently sufficient for safe copy-up.
func TestOfflineMetadataContinuationAfterSpoolGenerationDisappears(t *testing.T) {
	wasOffline := pin.IsOffline()
	pin.SetOffline(false)
	t.Cleanup(func() { pin.SetOffline(wasOffline) })

	jfs, spool, _, _ := newSpoolWiredHandlerNoDrain(t)
	const name = "tree/._dir-spool-generation"
	cf, err := jfs.Create(name)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = cf.Close()
	header := patternedPayload(152, 103)
	wf, err := jfs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open first range: %v", err)
	}
	if _, err := wf.(io.WriterAt).WriteAt(header, 0); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if err := wf.Close(); err != nil {
		t.Fatalf("close header: %v", err)
	}
	spool.CancelForDelete(name)
	if _, ok := spool.LookupActive(name); ok {
		t.Fatal("test precondition: first spool generation still active")
	}

	pin.SetOffline(true)
	wf, err = jfs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("offline offset-152 continuation lacked retained header: %v", err)
	}
	defer wf.Close()
	seeded, ok := spool.LookupActive(name)
	if !ok {
		t.Fatal("offline continuation did not create a replacement spool image")
	}
	rf, err := seeded.OpenForRead()
	if err != nil {
		t.Fatalf("open replacement image: %v", err)
	}
	got := make([]byte, len(header))
	n, readErr := rf.ReadAt(got, 0)
	_ = rf.Close()
	if n != len(header) || (readErr != nil && readErr != io.EOF) || !bytes.Equal(got, header) {
		t.Fatalf("replacement seed lost header: n=%d err=%v", n, readErr)
	}
}

// The real drainer invokes cacheDrainedMetadata before spool cleanup and then
// onSpoolDrained after it. The saved body must carry the callback's final mirror
// tag or an offline Finder continuation is incorrectly refused despite the
// exact complete image being local.
func TestPathOnlyDrainCallbackCannotRetagCachedMetadata(t *testing.T) {
	jfs, _, _, _ := newSpoolWiredHandlerNoDrain(t)
	const name = "tree/._dir-retag"
	body := patternedPayload(4096, 97)
	oldMtime := time.Now().Add(-time.Hour)
	meta := metadata.MakeEntry(name, false, int64(len(body)), oldMtime, 727272)
	if err := jfs.handler.store.Insert(meta); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}
	jfs.handler.sidecar.put(name, body, oldMtime.Unix(), int64(len(body)))

	before := jfs.handler.store.LookupByPath(name)
	jfs.handler.onSpoolDrained(name, int64(len(body)), time.Now())
	after := jfs.handler.store.LookupByPath(name)
	if after == nil || after.Version != before.Version || !after.Mtime.Equal(before.Mtime) {
		t.Fatal("path-only callback changed mirror")
	}
	seed, ok := jfs.handler.offlineMetadataSeed(name)
	if !ok {
		t.Fatal("path-only callback changed a complete cached metadata body")
	}
	if !bytes.Equal(seed, body) {
		t.Fatal("retagged offline metadata seed changed bytes")
	}
}

// TestDrainCachesMetadataBeforeSpoolCleanup proves the prior whole image is
// captured from the spool before MarkDrainComplete unlinks it. This is what
// makes a later offline Finder continuation eligible for the safe seeded path.
func TestDrainCachesMetadataBeforeSpoolCleanup(t *testing.T) {
	wasOffline := pin.IsOffline()
	pin.SetOffline(false)
	t.Cleanup(func() { pin.SetOffline(wasOffline) })

	jfs, spool, drainer, _ := newSpoolWiredHandlerNoDrain(t)
	drainer.SetOnSizeReady(jfs.handler.publishDrainedSize)
	drainer.SetOnDrainMetadataReady(jfs.handler.cacheDrainedMetadata)

	const name = "tree/._dir3"
	payload := patternedPayload(4096, 83)
	simulateNFSCreateThenWrite(t, jfs, name, payload, 1024)
	if n := spool.sweepOnce(0); n != 1 {
		t.Fatalf("finalized %d metadata entries, want 1", n)
	}
	drainAllForTest(t, drainer)
	if _, ok := spool.LookupActive(name); ok {
		t.Fatal("drained metadata entry still active; precondition for offline reopen not met")
	}

	pin.SetOffline(true)
	wf, err := jfs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("drained metadata body was not retained for offline rewrite: %v", err)
	}
	defer wf.Close()
	seeded, ok := spool.LookupActive(name)
	if !ok {
		t.Fatal("offline reopen did not create a seeded spool entry")
	}
	rf, err := seeded.OpenForRead()
	if err != nil {
		t.Fatalf("open seeded spool body: %v", err)
	}
	got, err := io.ReadAll(rf)
	_ = rf.Close()
	if err != nil {
		t.Fatalf("read seeded spool body: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("drain-time cached metadata did not seed the exact prior image")
	}
}

// Finder commonly preallocates an AppleDouble file to 4 KiB, writes only its
// 152-byte header in the first NFS WRITE, and relies on the zero-filled tail.
// The close-side snapshot correctly refuses that still-sparse generation, so
// the drainer is the only place that can retain the authoritative complete
// 4 KiB image. Exercise both completion modes because production enables the
// metadata drain batcher.
func TestDrainCachesPreallocatedAppleDoubleAfterHeaderOnlyWrite(t *testing.T) {
	testDrainCachesPreallocatedAppleDoubleAfterHeaderOnlyWrite(t, false)
}

func TestDrainCachesPreallocatedAppleDoubleAfterHeaderOnlyWriteBatched(t *testing.T) {
	testDrainCachesPreallocatedAppleDoubleAfterHeaderOnlyWrite(t, true)
}

func testDrainCachesPreallocatedAppleDoubleAfterHeaderOnlyWrite(t *testing.T, batched bool) {
	wasOffline := pin.IsOffline()
	pin.SetOffline(false)
	t.Cleanup(func() { pin.SetOffline(wasOffline) })

	var jfs *juiceFS
	var spool *SpoolStore
	var drainer *Drainer
	if batched {
		jfs, spool, drainer, _ = newBatchWiredHandlerNoDispatch(t, 1)
	} else {
		jfs, spool, drainer, _ = newSpoolWiredHandlerNoDrain(t)
		drainer.SetOnSizeReady(jfs.handler.publishDrainedSize)
		drainer.SetOnDrainMetadataReady(jfs.handler.cacheDrainedMetadata)
		drainer.SetOnDrainComplete(jfs.handler.onSpoolDrained)
	}

	const name = "tree/._dir-preallocated"
	cf, err := jfs.Create(name)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := cf.Close(); err != nil {
		t.Fatalf("close create handle: %v", err)
	}

	preallocated := uint64(4096)
	if err := (&nfslib.SetFileAttributes{SetSize: &preallocated}).Apply(jfs.handler.Change(jfs), jfs, name); err != nil {
		t.Fatalf("preallocate sidecar: %v", err)
	}
	header := patternedPayload(152, 127)
	wf, err := jfs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open header write: %v", err)
	}
	if _, err := wf.(io.WriterAt).WriteAt(header, 0); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if err := wf.Close(); err != nil {
		t.Fatalf("close header write: %v", err)
	}

	if n := spool.sweepOnce(0); n != 1 {
		t.Fatalf("finalized %d metadata entries, want 1", n)
	}
	drainAllForTest(t, drainer)
	if _, ok := spool.LookupActive(name); ok {
		t.Fatal("drained metadata entry still active")
	}

	pin.SetOffline(true)
	wf, err = jfs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("offline continuation lacked drained 4 KiB image: %v", err)
	}
	defer wf.Close()
	seeded, ok := spool.LookupActive(name)
	if !ok {
		t.Fatal("offline continuation did not create a seeded spool image")
	}
	rf, err := seeded.OpenForRead()
	if err != nil {
		t.Fatalf("open seeded image: %v", err)
	}
	got, err := io.ReadAll(rf)
	_ = rf.Close()
	if err != nil {
		t.Fatalf("read seeded image: %v", err)
	}
	if len(got) != int(preallocated) || !bytes.Equal(got[:len(header)], header) || !bytes.Equal(got[len(header):], make([]byte, int(preallocated)-len(header))) {
		t.Fatalf("seeded image did not preserve the header plus zero-filled tail (len=%d)", len(got))
	}
}

// A successful drain restores the spool file's client-visible mtime onto the
// backend inode. The metadata mirror must publish that same timestamp: the
// next reconcile reads it back from FUSE, and a different drain-time tag would
// make the complete cached AppleDouble body look stale. Finder can then flip
// offline and continue the same sidecar at offset 152; that safe continuation
// must still seed from the retained whole image instead of returning ACCES.
func TestOfflineMetadataContinuationSurvivesBackendMtimeReconcile(t *testing.T) {
	testOfflineMetadataContinuationSurvivesBackendMtimeReconcile(t, false)
}

func TestOfflineMetadataContinuationSurvivesBackendMtimeReconcileBatched(t *testing.T) {
	testOfflineMetadataContinuationSurvivesBackendMtimeReconcile(t, true)
}

// Finder can reopen a just-drained AppleDouble sidecar while the route is
// still online, then issue the next range WRITE after the route flips offline.
// The online reopen must keep the complete body in the spool instead of
// invalidating the only safe copy and switching back to an in-place FUSE fd.
func TestOnlineMetadataReopenKeepsShadowAcrossOfflineFlip(t *testing.T) {
	wasOffline := pin.IsOffline()
	pin.SetOffline(false)
	t.Cleanup(func() { pin.SetOffline(wasOffline) })

	jfs, spool, drainer, _ := newSpoolWiredHandlerNoDrain(t)
	drainer.SetOnSizeReady(jfs.handler.publishDrainedSize)
	drainer.SetOnDrainMetadataReady(jfs.handler.cacheDrainedMetadata)
	drainer.SetOnDrainComplete(jfs.handler.onSpoolDrained)

	const name = "tree/._dir-online-overlap"
	body := patternedPayload(4096, 113)
	simulateNFSCreateThenWrite(t, jfs, name, body, 1024)
	if n := spool.sweepOnce(0); n != 1 {
		t.Fatalf("finalized %d metadata entries, want 1", n)
	}
	drainAllForTest(t, drainer)
	if _, ok := spool.LookupActive(name); ok {
		t.Fatal("drained metadata entry still active")
	}

	first, err := jfs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("online continuation open: %v", err)
	}
	defer first.Close()
	if _, err := first.(io.WriterAt).WriteAt([]byte("online-prefix"), 0); err != nil {
		t.Fatalf("online continuation write: %v", err)
	}

	// Keep the first per-RPC handle open while the route changes. A direct
	// backend writer has already invalidated the saved image at this point;
	// a spool-backed writer remains the authoritative complete shadow.
	pin.SetOffline(true)
	second, err := jfs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("offline continuation lost the online writer shadow: %v", err)
	}
	defer second.Close()
	if _, err := second.(io.WriterAt).WriteAt([]byte("offline-tail"), 152); err != nil {
		t.Fatalf("offline continuation write: %v", err)
	}
}

func testOfflineMetadataContinuationSurvivesBackendMtimeReconcile(t *testing.T, batched bool) {
	wasOffline := pin.IsOffline()
	pin.SetOffline(false)
	t.Cleanup(func() { pin.SetOffline(wasOffline) })

	var jfs *juiceFS
	var spool *SpoolStore
	var drainer *Drainer
	var fuseRoot string
	if batched {
		jfs, spool, drainer, fuseRoot = newBatchWiredHandlerNoDispatch(t, 1)
	} else {
		jfs, spool, drainer, fuseRoot = newSpoolWiredHandlerNoDrain(t)
		drainer.SetOnSizeReady(jfs.handler.publishDrainedSize)
		drainer.SetOnDrainMetadataReady(jfs.handler.cacheDrainedMetadata)
	}

	const name = "tree/._dir-reconcile"
	body := patternedPayload(152, 109)
	simulateNFSCreateThenWrite(t, jfs, name, body, len(body))
	entry, ok := spool.LookupActive(name)
	if !ok {
		t.Fatal("metadata spool generation disappeared before drain")
	}
	clientMtime := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	if err := os.Chtimes(entry.SpoolFilePath(), clientMtime, clientMtime); err != nil {
		t.Fatalf("set deterministic spool mtime: %v", err)
	}
	if n := spool.sweepOnce(0); n != 1 {
		t.Fatalf("finalized %d metadata entries, want 1", n)
	}
	drainAllForTest(t, drainer)

	backendInfo, err := os.Stat(filepath.Join(fuseRoot, name))
	if err != nil {
		t.Fatalf("stat drained metadata: %v", err)
	}
	if backendInfo.ModTime().Unix() != clientMtime.Unix() {
		t.Fatalf("backend mtime=%v, want restored client mtime=%v", backendInfo.ModTime(), clientMtime)
	}
	current := jfs.handler.store.LookupByPath(name)
	if current == nil {
		t.Fatal("drained metadata missing from mirror")
	}
	if current.Mtime.Unix() != backendInfo.ModTime().Unix() {
		t.Fatalf("drain published mirror mtime=%v, backend mtime=%v", current.Mtime, backendInfo.ModTime())
	}
	// Model the keyspace/reconcile observation of the authoritative backend
	// inode. This is the live race: it can arrive between the first sidecar row
	// draining and Finder's later offset-152 write after the route went offline.
	reconciled := metadata.MakeEntry(name, false, int64(len(body)), backendInfo.ModTime(), current.Inode)
	reconciled.Mode = current.Mode
	if err := jfs.handler.store.BulkInsert([]*metadata.Entry{reconciled}, 1); err != nil {
		t.Fatalf("simulate backend reconcile: %v", err)
	}

	pin.SetOffline(true)
	wf, err := jfs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("offline continuation rejected after backend mtime reconcile: %v", err)
	}
	defer wf.Close()
	seeded, ok := spool.LookupActive(name)
	if !ok {
		t.Fatal("offline continuation did not create a seeded spool generation")
	}
	rf, err := seeded.OpenForRead()
	if err != nil {
		t.Fatalf("open reconciled seed: %v", err)
	}
	got, err := io.ReadAll(rf)
	_ = rf.Close()
	if err != nil {
		t.Fatalf("read reconciled seed: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("reconciled offline seed did not preserve the complete prior image")
	}
}

func fiSize(fi os.FileInfo) int64 {
	if fi == nil {
		return -1
	}
	return fi.Size()
}
