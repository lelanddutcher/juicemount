//go:build h8_acceptance

package nfs

// Release-blocking assertions. Run explicitly with:
// go test -tags h8_acceptance ./nfs -run '^TestH8Acceptance' -count=1
// These tests must pass before H8 can be closed; the ordinary suite excludes
// the tag while the durable mutation protocol remains unimplemented.

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

func TestH8AcceptanceRestartAfterPublicationFailure(t *testing.T) {
	fuseRoot := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	jfs := &juiceFS{handler: NewHandler(store, fuseRoot)}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			jfs.handler.StopHandler()
		}
		_ = store.Close()
	})
	const path = "reel.mov"
	seedFile(t, jfs, store, fuseRoot, path, patternedPayload(8192, 79))
	if err := store.Insert(store.LookupByPath(path)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`CREATE TRIGGER fail_h8_size BEFORE UPDATE OF size ON entries BEGIN SELECT RAISE(FAIL, 'H8 publication failure'); END`); err != nil {
		t.Fatal(err)
	}
	f, err := jfs.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(0); err == nil {
		_ = f.Close()
		t.Fatal("fault injection did not reject publication")
	}
	_ = f.Close()
	// A crash loses the in-memory poison reservation. Discard it only in this
	// test so reopening checks durable recovery rather than clean-quit refusal.
	discardFailedNamespaceForTest(jfs.handler)
	if !jfs.handler.StopHandler() {
		t.Fatal("handler did not stop")
	}
	stopped = true
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := metadata.Open(dbPath)
	if reopened != nil {
		defer reopened.Close()
	}
	var refused *metadata.PendingOnlineMutation
	if !errors.As(err, &refused) {
		t.Fatalf("reopen = (%v, %v), want typed pending-online-mutation refusal", reopened, err)
	}
	pending, err := metadata.InspectOnlineMutationsAt(dbPath)
	if err != nil {
		t.Fatalf("offline inspect: %v", err)
	}
	if len(pending) != 1 || len(refused.Mutations) != 1 || pending[0] != refused.Mutations[0] ||
		pending[0].Path != path || pending[0].Kind != "truncate" ||
		pending[0].OldEOF != 8192 || pending[0].TargetEOF != 0 || pending[0].OpID == "" {
		t.Fatalf("restart recovery evidence = %+v, refusal = %+v", pending, refused.Mutations)
	}
	backing, err := os.Stat(filepath.Join(fuseRoot, path))
	if err != nil {
		t.Fatal(err)
	}
	if backing.Size() != 0 {
		t.Fatalf("backing EOF after failed publication = %d, want 0", backing.Size())
	}
}

func TestH8AcceptanceTruncateCannotPublishIntoReplacement(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)
	const path = "reel.mov"
	seedFile(t, jfs, store, fuseRoot, path, patternedPayload(8192, 83))
	if err := store.Insert(store.LookupByPath(path)); err != nil {
		t.Fatal(err)
	}
	f, err := jfs.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// An already-open NFS handle survives replacement of its pathname. Its
	// later truncate may change the unlinked object, never the new mirror row.
	if err := jfs.Remove(path); err != nil {
		t.Fatal(err)
	}
	seedFile(t, jfs, store, fuseRoot, path, patternedPayload(4096, 89))
	if err := store.Insert(store.LookupByPath(path)); err != nil {
		t.Fatal(err)
	}
	truncateErr := f.Truncate(0)
	if got := len(onDisk(t, fuseRoot, path)); got != 4096 {
		t.Fatalf("replacement backing bytes changed: size %d", got)
	}
	if got := store.LookupByPath(path).Size; got != 4096 {
		t.Fatalf("H8: truncate published old file EOF into replacement: mirror size %d, backing 4096, error %v", got, truncateErr)
	}
	if truncateErr == nil {
		t.Fatal("H8: retained handle must reject truncate after its namespace incarnation is replaced")
	}
}

func TestH8AcceptanceRetainedWriterCannotPublishIntoReplacement(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)
	const path = "reel.mov"
	seedFile(t, jfs, store, fuseRoot, path, patternedPayload(8192, 97))
	if err := store.Insert(store.LookupByPath(path)); err != nil {
		t.Fatal(err)
	}
	f, err := jfs.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
	}()
	if err := jfs.Remove(path); err != nil {
		t.Fatal(err)
	}
	seedFile(t, jfs, store, fuseRoot, path, patternedPayload(4096, 101))
	if err := store.Insert(store.LookupByPath(path)); err != nil {
		t.Fatal(err)
	}
	n, writeErr := f.(io.WriterAt).WriteAt([]byte("stale"), 16384)
	closeErr := f.Close()
	closed = true
	if n != 0 || writeErr == nil {
		t.Errorf("H8: retained writer mutated the retired object: bytes=%d error=%v", n, writeErr)
	}
	if closeErr == nil {
		t.Error("H8: retained writer close acknowledged publication after replacement")
	}
	if got := store.LookupByPath(path).Size; got != 4096 {
		t.Errorf("H8: retained writer published into replacement mirror: size=%d", got)
	}
	if got := len(onDisk(t, fuseRoot, path)); got != 4096 {
		t.Errorf("replacement backing bytes changed: size=%d", got)
	}
}

func TestH8AcceptanceRetainedTruncateAfterRename(t *testing.T) {
	for _, ancestor := range []bool{false, true} {
		name := "file"
		if ancestor {
			name = "ancestor"
		}
		t.Run(name, func(t *testing.T) {
			jfs, store, fuseRoot := newIntegrityHarness(t)
			oldPath, newPath := "old/reel.mov", "new/reel.mov"
			seedFile(t, jfs, store, fuseRoot, oldPath, patternedPayload(8192, 103))
			if err := store.Insert(store.LookupByPath(oldPath)); err != nil {
				t.Fatal(err)
			}
			if ancestor {
				mirror(t, store, fuseRoot, "old", true)
				if err := store.Insert(store.LookupByPath("old")); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Mkdir(filepath.Join(fuseRoot, "new"), 0755); err != nil {
				t.Fatal(err)
			}
			f, err := jfs.OpenFile(oldPath, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			from, to := oldPath, newPath
			if ancestor {
				from, to = "old", "new"
			}
			if err := jfs.Rename(from, to); err != nil {
				t.Fatal(err)
			}
			// A handle still bound to the old path must fail before changing the
			// moved object. Supporting writable handles across rename requires
			// rebinding both backing identity and durable mirror generation.
			truncateErr := f.Truncate(0)
			if got := len(onDisk(t, fuseRoot, newPath)); got != 8192 {
				t.Errorf("H8: retained truncate mutated renamed backing object: size=%d error=%v", got, truncateErr)
			}
			if truncateErr == nil {
				t.Error("H8: retained truncate acknowledged an obsolete namespace binding")
			}
			if e := store.LookupByPath(newPath); e == nil || e.Size != 8192 {
				t.Errorf("H8: renamed mirror diverged from expected object size: %+v", e)
			}
		})
	}
}

func TestH8AcceptanceOpenTruncatePublishesExactSize(t *testing.T) {
	for _, pooled := range []bool{false, true} {
		name := "cold"
		if pooled {
			name = "pooled"
		}
		t.Run(name, func(t *testing.T) {
			jfs, store, fuseRoot := newIntegrityHarness(t)
			const path = "reel.mov"
			seedFile(t, jfs, store, fuseRoot, path, patternedPayload(8192, 107))
			if err := store.Insert(store.LookupByPath(path)); err != nil {
				t.Fatal(err)
			}
			if pooled {
				f, err := jfs.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			}
			f, err := jfs.OpenFile(path, os.O_RDWR|os.O_TRUNC, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			if got := len(onDisk(t, fuseRoot, path)); got != 0 {
				t.Errorf("H8: O_TRUNC acknowledged without truncating backing: size=%d", got)
			}
			if got := store.LookupByPath(path).Size; got != 0 {
				t.Errorf("H8: O_TRUNC acknowledged without exact mirror publication: size=%d", got)
			}
			var durableSize int64
			if err := store.DB().QueryRow(`SELECT size FROM entries WHERE path = ?`, path).Scan(&durableSize); err != nil {
				t.Fatal(err)
			}
			if durableSize != 0 {
				t.Errorf("H8: O_TRUNC acknowledged with stale durable size: size=%d", durableSize)
			}
		})
	}
}
