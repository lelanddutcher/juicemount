package farm

import (
	"bytes"
	"os"
	"path/filepath"
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
