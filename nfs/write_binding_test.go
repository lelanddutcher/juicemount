package nfs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// Tests may discard failed in-memory reservations after asserting refusal, so
// their cleanup can stop the handler. This is not a production recovery API.
func discardFailedNamespaceForTest(h *JuiceMountHandler) {
	h.namespace.mu.Lock()
	defer h.namespace.mu.Unlock()
	for op := range h.namespace.active {
		if op.failed {
			delete(h.namespace.active, op)
		}
	}
}

func TestWriteBindingRejectsSameSizeBackingReplacement(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	const name = "take.mov"
	original, replacement := patternedPayload(8192, 13), patternedPayload(8192, 17)
	seedFile(t, jfs, store, root, name, original)
	f, err := jfs.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := os.Rename(filepath.Join(root, name), filepath.Join(root, "retired.mov")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, name), replacement, 0644); err != nil {
		t.Fatal(err)
	}
	if n, err := f.(io.WriterAt).WriteAt([]byte("bad"), 0); n != 0 || err == nil {
		t.Fatalf("stale write = (%d,%v)", n, err)
	}
	if err := f.Truncate(0); err == nil {
		t.Fatal("stale truncate succeeded")
	}
	if !bytes.Equal(onDisk(t, root, "retired.mov"), original) {
		t.Fatal("retired descriptor was mutated")
	}
	if !bytes.Equal(onDisk(t, root, name), replacement) {
		t.Fatal("replacement was mutated")
	}
	if err := f.Close(); err == nil {
		t.Fatal("stale close succeeded")
	}
}

func TestWriteBindingPoisonBlocksRetainedReadsAndCommit(t *testing.T) {
	for _, operation := range []string{"write", "truncate"} {
		t.Run(operation, func(t *testing.T) {
			jfs, store, root := newIntegrityHarness(t)
			t.Cleanup(func() { discardFailedNamespaceForTest(jfs.handler) })
			const name = "take.mov"
			seedFile(t, jfs, store, root, name, patternedPayload(8192, 19))
			reader, err := jfs.OpenFile(name, os.O_RDONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			writer, err := jfs.OpenFile(name, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			if _, err := store.DB().Exec(`CREATE TRIGGER fail_bound_size BEFORE UPDATE OF size ON entries BEGIN SELECT RAISE(FAIL, 'bound publication failure'); END`); err != nil {
				t.Fatal(err)
			}
			if operation == "write" {
				_, err = writer.(io.WriterAt).WriteAt([]byte("growth"), 8192)
			} else {
				err = writer.Truncate(0)
			}
			if err == nil {
				t.Fatal("faulted publication succeeded")
			}
			if _, err := reader.ReadAt(make([]byte, 8), 0); !errors.Is(err, syscall.EIO) {
				t.Fatalf("retained read = %v, want EIO", err)
			}
			if _, err := writer.ReadAt(make([]byte, 8), 0); !errors.Is(err, syscall.EIO) {
				t.Fatalf("direct read = %v, want EIO", err)
			}
			if _, err := jfs.Lstat(name); !errors.Is(err, syscall.EIO) {
				t.Fatalf("GETATTR path = %v, want EIO", err)
			}
			if _, err := jfs.ReadDir("."); !errors.Is(err, syscall.EIO) {
				t.Fatalf("directory attributes = %v, want EIO", err)
			}
			if _, ok := jfs.StatCacheOnly(name); ok {
				t.Fatal("cache-only stat served poisoned attributes")
			}
			if err := jfs.CommitFile(name); !errors.Is(err, syscall.EIO) {
				t.Fatalf("COMMIT = %v, want EIO", err)
			}
			if jfs.handler.StopHandler() {
				t.Fatal("shutdown discarded unresolved mutation")
			}
		})
	}
}
