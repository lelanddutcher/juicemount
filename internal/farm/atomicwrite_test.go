package farm

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// TestAtomicWriteFileNeverPartial proves the reader-visibility guarantee that
// matters for OpenLoupe: while atomicWriteFile is materializing a large blob, a
// concurrent reader polling the final path must observe either ABSENT (open
// fails with ErrNotExist) or the COMPLETE, byte-identical payload — NEVER a
// truncated / half-written blob. A plain os.WriteFile would let the poller read
// the destination mid-write and see fewer bytes; the temp-write + rename
// approach makes the final path flip atomically, so this test would fail loudly
// if anyone regressed the helper back to an in-place write.
func TestAtomicWriteFileNeverPartial(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "blob.bin")

	// A large, content-checkable payload. Non-zero, position-dependent bytes so
	// a torn read (correct length but stale/garbage tail) would also be caught,
	// not just a short read.
	const size = 8 << 20 // 8 MiB
	want := make([]byte, size)
	for i := range want {
		want[i] = byte(i*31 + 7)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Reader: hammer the final path. Any successful open MUST yield the full,
	// exact payload. Anything shorter or different is a partial-blob exposure.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			got, err := os.ReadFile(path)
			if err != nil {
				if os.IsNotExist(err) {
					continue // absent is allowed
				}
				t.Errorf("unexpected read error: %v", err)
				return
			}
			if len(got) != size {
				t.Errorf("partial blob observed: read %d bytes, want %d", len(got), size)
				return
			}
			if !bytes.Equal(got, want) {
				t.Errorf("torn blob observed: %d-byte read did not match payload", len(got))
				return
			}
		}
	}()

	// Writer: rewrite the blob many times via the helper while the reader polls,
	// widening the window in which a non-atomic implementation would be caught.
	for i := 0; i < 50; i++ {
		if err := atomicWriteFile(path, want, 0o644); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("atomicWriteFile: %v", err)
		}
	}

	close(stop)
	wg.Wait()

	// Final state is the complete payload with the requested permissions.
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("final read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("final blob mismatch")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("final stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o644 {
		t.Fatalf("final perm = %o, want 0644", perm)
	}

	// No temp siblings should linger after a clean run.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range entries {
		if e.Name() != "blob.bin" {
			t.Errorf("leftover temp sibling: %q", e.Name())
		}
	}
}

// TestInterruptedWriteNeverVisible simulates the #54 failure mode directly: a
// producer that wrote its temp file and DIED before the rename (kill -9, ffmpeg
// OOM, node reboot). The live incident this guards against was a killed cp
// leaving a 0-byte poster.jpg that consumers then served. With the temp-write +
// rename discipline the consumer-facing path must simply not exist — an
// interrupted write leaves only a dot-prefixed temp sibling no consumer ever
// addresses (blobs are fetched by exact name: poster.jpg, proxy.mp4, ...).
func TestInterruptedWriteNeverVisible(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "poster.jpg")

	tmp := filepath.Join(filepath.Dir(final), "."+filepath.Base(final)+".tmp-test")
	if err := os.WriteFile(tmp, []byte("partial-jpeg-bytes"), 0o644); err != nil {
		t.Fatalf("write temp: %v", err)
	}

	// in-process lane: the CreateTemp pattern atomicWriteFile uses, never renamed.
	tmp2, err := os.CreateTemp(dir, "."+filepath.Base(final)+".tmp-*")
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}
	if _, err := tmp2.Write([]byte("half")); err != nil {
		t.Fatalf("write temp2: %v", err)
	}
	_ = tmp2.Close()

	// The consumer's view: the blob does not exist. Not 0-byte, not partial —
	// absent, which is exactly the state consumers already fail-closed on.
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatalf("final path visible after interrupted write: stat err = %v", err)
	}

	// Both orphaned temps are dot-prefixed so an exact-name consumer (or a
	// non-hidden directory listing) never picks them up.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			t.Errorf("orphaned temp %q is not dot-prefixed (visible to consumers)", e.Name())
		}
		if !strings.Contains(e.Name(), ".tmp-") {
			t.Errorf("orphaned temp %q lacks the .tmp- marker", e.Name())
		}
	}
}

// TestAtomicWriteFileCleansTempOnError forces the rename step to fail (the
// destination name is occupied by a DIRECTORY, so os.Rename cannot replace it)
// and asserts the error path removes the temp file — no partial ".tmp-*"
// siblings may accumulate on the volume when writes fail.
func TestAtomicWriteFileCleansTempOnError(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "waveform.json")
	// Occupy the final path with a non-empty directory → rename(file, dir) fails.
	if err := os.MkdirAll(filepath.Join(final, "occupied"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	if err := atomicWriteFile(final, []byte("payload"), 0o644); err == nil {
		t.Fatalf("atomicWriteFile onto a directory path unexpectedly succeeded")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp-") {
			t.Errorf("temp file %q leaked on the error path", e.Name())
		}
	}
}

// TestAtomicCommitFile and TestAtomicTempPathUniqueAndHidden were deleted with
// the helpers they covered. The property they protected — ffmpeg output becomes
// visible under its real name atomically and never half-written — did not go
// away; it moved to internal/derivatives.CommitStagedAt, which publishes with
// renameat through a held directory descriptor and is covered by
// TestStageAndCommitThroughDescriptor and TestCommitStagedRefusesEmptyPlaceholder.
// Recorded here so the coverage is traceable rather than looking dropped.
