package nfs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

func requirePathIntent(t *testing.T, store *metadata.Store, kind string, oldEOF, targetEOF int64) metadata.OnlineMutation {
	t.Helper()
	pending, err := store.InspectOnlineMutations()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending mutations = %+v, want one", pending)
	}
	m := pending[0]
	if m.Kind != kind || m.Path != "reel.mov" || m.OldEOF != oldEOF || m.TargetEOF != targetEOF ||
		m.OpID == "" || m.BackingIdentity == "" || m.MountGeneration == "" {
		t.Fatalf("unexpected pending intent: %+v", m)
	}
	return m
}

func TestOnlineMutationIntentPrecedesBackingSyscall(t *testing.T) {
	for _, kind := range []string{"truncate", "write"} {
		t.Run(kind, func(t *testing.T) {
			jfs, store, root := newIntegrityHarness(t)
			original := patternedPayload(4096, 181)
			seedFile(t, jfs, store, root, "reel.mov", original)
			f, err := jfs.OpenFile("reel.mov", os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			seen := false
			jfs.handler.testBeforeInPlaceMutation = func(_ string, operation string) error {
				if operation != kind {
					t.Fatalf("operation = %q, want %q", operation, kind)
				}
				end := int64(0)
				if kind == "write" {
					end = 8192
				}
				requirePathIntent(t, store, kind, 4096, end)
				if got := onDisk(t, root, "reel.mov"); !bytes.Equal(got, original) {
					t.Fatal("backing bytes changed before durable intent was inspected")
				}
				if _, err := jfs.Stat("reel.mov"); !errors.Is(err, errFUSETimeout) {
					t.Fatalf("in-process read fence = %v, want retry", err)
				}
				seen = true
				return nil
			}
			if kind == "write" {
				_, err = f.(io.WriterAt).WriteAt(patternedPayload(4096, 183), 4096)
			} else {
				err = f.Truncate(0)
			}
			if err != nil || !seen {
				t.Fatalf("mutation = %v, saw pre-syscall intent = %v", err, seen)
			}
			if pending, err := store.InspectOnlineMutations(); err != nil || len(pending) != 0 {
				t.Fatalf("completed mutation left intent: %+v, %v", pending, err)
			}
		})
	}
}

func TestOnlineMutationSyncFailureRetainsIntent(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	t.Cleanup(func() { discardFailedNamespaceForTest(jfs.handler) })
	seedFile(t, jfs, store, root, "reel.mov", patternedPayload(4096, 187))
	failed := errors.New("injected backing sync failure")
	jfs.handler.testSyncOnlineMutation = func(*os.File) error { return failed }
	f, err := jfs.OpenFile("reel.mov", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(0); !errors.Is(err, failed) {
		t.Fatalf("truncate = %v, want sync error", err)
	}
	requirePathIntent(t, store, "truncate", 4096, 0)
	if _, err := jfs.Stat("reel.mov"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("serving after failed sync = %v, want EIO", err)
	}
}

func TestOnlineMutationPreSyscallErrorRetainsIntent(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	t.Cleanup(func() { discardFailedNamespaceForTest(jfs.handler) })
	original := patternedPayload(4096, 189)
	seedFile(t, jfs, store, root, "reel.mov", original)
	failed := errors.New("injected pre-syscall failure")
	jfs.handler.testBeforeInPlaceMutation = func(_, _ string) error { return failed }
	f, err := jfs.OpenFile("reel.mov", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.(io.WriterAt).WriteAt([]byte("growth"), 4096); !errors.Is(err, failed) {
		t.Fatalf("write = %v, want pre-syscall error", err)
	}
	requirePathIntent(t, store, "write", 4096, 4102)
	if got := onDisk(t, root, "reel.mov"); !bytes.Equal(got, original) {
		t.Fatal("backing bytes changed despite pre-syscall error")
	}
}

func TestOnlineMutationCompletionFailureRetainsIntent(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	t.Cleanup(func() { discardFailedNamespaceForTest(jfs.handler) })
	seedFile(t, jfs, store, root, "reel.mov", patternedPayload(4096, 191))
	if _, err := store.DB().Exec(`CREATE TRIGGER fail_online_publish BEFORE UPDATE OF size ON entries BEGIN SELECT RAISE(FAIL, 'injected'); END`); err != nil {
		t.Fatal(err)
	}
	f, err := jfs.OpenFile("reel.mov", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.(io.WriterAt).WriteAt([]byte("growth"), 4096); err == nil {
		t.Fatal("write acknowledged failed durable completion")
	}
	requirePathIntent(t, store, "write", 4096, 4102)
	if got := store.LookupByPath("reel.mov").Size; got != 4096 {
		t.Fatalf("failed completion published mirror size %d", got)
	}
}

func TestOnlineMutationReplacementBeforeCompletionRetainsIntent(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	t.Cleanup(func() { discardFailedNamespaceForTest(jfs.handler) })
	original := patternedPayload(4096, 193)
	seedFile(t, jfs, store, root, "reel.mov", original)
	jfs.handler.testAfterInPlaceMutation = func(_ string, operation string) {
		if operation != "truncate" {
			t.Fatalf("operation = %q", operation)
		}
		if err := os.Rename(filepath.Join(root, "reel.mov"), filepath.Join(root, "retired.mov")); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "reel.mov"), original, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	f, err := jfs.OpenFile("reel.mov", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(0); !errors.Is(err, metadata.ErrMirrorConflict) {
		t.Fatalf("replacement race = %v, want identity conflict", err)
	}
	requirePathIntent(t, store, "truncate", 4096, 0)
	if got := onDisk(t, root, "reel.mov"); !bytes.Equal(got, original) {
		t.Fatal("replacement bytes were changed")
	}
}

func TestOnlineMutationLegacyCreateUsesIntentForExistingName(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	seedFile(t, jfs, store, root, "reel.mov", patternedPayload(4096, 197))
	seen := false
	jfs.handler.testBeforeInPlaceMutation = func(_, operation string) error {
		if operation != "truncate" {
			t.Fatalf("legacy CREATE operation = %q, want truncate", operation)
		}
		requirePathIntent(t, store, "truncate", 4096, 0)
		if got := len(onDisk(t, root, "reel.mov")); got != 4096 {
			t.Fatalf("existing file truncated before intent: %d", got)
		}
		seen = true
		return nil
	}
	f, err := jfs.Create("reel.mov")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if !seen || store.LookupByPath("reel.mov").Size != 0 || len(onDisk(t, root, "reel.mov")) != 0 {
		t.Fatal("existing legacy CREATE did not publish journaled zero EOF")
	}
	if err := os.WriteFile(filepath.Join(root, "unmirrored.mov"), []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if f, err := jfs.Create("unmirrored.mov"); !errors.Is(err, os.ErrExist) {
		if f != nil {
			_ = f.Close()
		}
		t.Fatalf("unmirrored existing CREATE = %v, want exclusive conflict", err)
	}
	if got := string(onDisk(t, root, "unmirrored.mov")); got != "keep" {
		t.Fatalf("unmirrored backing was overwritten: %q", got)
	}
}
