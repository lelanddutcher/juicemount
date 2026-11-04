package nfs

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// phaseHolds parks the first namespace worker that reaches each registered
// (phase, name) checkpoint until the test releases it. Install before any
// operation starts; the coordinator's phase hook is not synchronized.
type phaseHolds struct {
	mu    sync.Mutex
	holds map[string]*phaseHold
}

type phaseHold struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func installPhaseHolds(jfs *juiceFS) *phaseHolds {
	p := &phaseHolds{holds: make(map[string]*phaseHold)}
	jfs.handler.namespace.phase = func(phase, name string) {
		p.mu.Lock()
		h := p.holds[phase+"\x00"+name]
		p.mu.Unlock()
		if h != nil {
			h.once.Do(func() { close(h.entered); <-h.release })
		}
	}
	return p
}

func (p *phaseHolds) hold(phase, name string) *phaseHold {
	h := &phaseHold{entered: make(chan struct{}), release: make(chan struct{})}
	p.mu.Lock()
	p.holds[phase+"\x00"+name] = h
	p.mu.Unlock()
	return h
}

// jukeboxed runs call past the RPC mutation bound: the worker is parked at
// (phase, name) until the call has returned errFUSETimeout, which the RPC layer
// answers NFS3ERR_JUKEBOX. The worker then finishes the mutation on its own.
// The worker may reach the checkpoint before or after the bound expires; held
// there, it cannot finish before the call has timed out either way. The bound
// only has to outlast admission, so it is not a race against a slow worker.
func jukeboxed(t *testing.T, jfs *juiceFS, holds *phaseHolds, phase, name string, call func() error) {
	t.Helper()
	h := holds.hold(phase, name)
	t.Setenv("JM_MUTATION_OP_TIMEOUT_MS", "250")
	done := make(chan error, 1)
	go func() { done <- call() }()
	if err := <-done; !errors.Is(err, errFUSETimeout) {
		t.Fatalf("first transmission = %v, want the JUKEBOX timeout", err)
	}
	select {
	case <-h.entered:
	case <-time.After(30 * time.Second):
		t.Fatalf("worker never reached %s %s", phase, name)
	}
	close(h.release)
	awaitNamespaceIdle(t, &jfs.handler.namespace)
	// Resends get a generous bound: they must not become a latency test.
	t.Setenv("JM_MUTATION_OP_TIMEOUT_MS", spoolFixtureMutationBoundMS)
}

func renameCall(jfs *juiceFS, oldpath, newpath string) func() error {
	return func() error { return jfs.Rename(oldpath, newpath) }
}

func wantNoEnt(t *testing.T, what string, err error) {
	t.Helper()
	if !os.IsNotExist(err) {
		t.Fatalf("%s = %v, want ENOENT", what, err)
	}
}

func newRenameRetryHarness(t *testing.T) (*juiceFS, *phaseHolds, string) {
	t.Helper()
	jfs, store, root := newIntegrityHarness(t)
	seedFile(t, jfs, store, root, "dir/a.mov", []byte("original"))
	mirror(t, store, root, "dir", true)
	return jfs, installPhaseHolds(jfs), root
}

// The reported bug: Finder renames a freshly copied file while the copy keeps
// the spool's SQLite busy. The rename outlives the bound, is answered JUKEBOX
// and finishes anyway. macOS resends it seconds later with a new XID. That
// resend used to cancel the destination's spool entry (the renamed file's
// only copy of its bytes), poison both names, and fail ENOENT, which macOS
// reports as success: the file was silently gone.
func TestRenameResentAfterJukeboxKeepsSpooledFile(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandlerNoDrain(t)
	holds := installPhaseHolds(jfs)
	payload := patternedPayload(256<<10, 5)
	simulateNFSCreateThenWrite(t, jfs, "src.mov", payload, 64<<10)

	jukeboxed(t, jfs, holds, "publish", "src.mov", renameCall(jfs, "src.mov", "dst.mov"))
	if err := jfs.Rename("src.mov", "dst.mov"); err != nil {
		t.Fatalf("resent rename = %v, want success", err)
	}
	if _, ok := spool.LookupActive("dst.mov"); !ok {
		t.Fatal("resent rename cancelled the renamed file's spool entry")
	}
	awaitNamespaceIdle(t, &jfs.handler.namespace)
	if _, err := jfs.Stat("dst.mov"); err != nil {
		t.Fatalf("Stat(dst.mov) after resend = %v", err)
	}

	// The answer is given once. Another resend runs for real, and finding no
	// source it must still leave the destination alone.
	wantNoEnt(t, "second resend", jfs.Rename("src.mov", "dst.mov"))
	if _, ok := spool.LookupActive("dst.mov"); !ok {
		t.Fatal("second resend cancelled the renamed file's spool entry")
	}
	awaitNamespaceIdle(t, &jfs.handler.namespace)

	if n := spool.sweepOnce(0); n < 1 {
		t.Fatalf("sweepOnce finalized %d entries, want >=1", n)
	}
	drainAllForTest(t, drainer)
	got, err := os.ReadFile(filepath.Join(fuseRoot, "dst.mov"))
	if err != nil || sha256.Sum256(got) != sha256.Sum256(payload) {
		t.Fatalf("drained dst.mov: %d bytes, %v; want the written payload", len(got), err)
	}
}

func TestRenameResentAfterJukeboxBackendFile(t *testing.T) {
	jfs, holds, root := newRenameRetryHarness(t)
	jukeboxed(t, jfs, holds, "publish", "dir/a.mov", renameCall(jfs, "dir/a.mov", "dir/b.mov"))
	if err := jfs.Rename("dir/a.mov", "dir/b.mov"); err != nil {
		t.Fatalf("resent rename = %v, want success", err)
	}
	wantNoEnt(t, "second resend", jfs.Rename("dir/a.mov", "dir/b.mov"))
	if got, err := os.ReadFile(filepath.Join(root, "dir/b.mov")); err != nil || string(got) != "original" {
		t.Fatalf("dir/b.mov = %q, %v", got, err)
	}
}

// A request that only resembles the late one is never answered from memory,
// and a remembered one is answered only once, within the TTL.
func TestRenameReplayRequiresTheSameRequestAndState(t *testing.T) {
	t.Run("other target", func(t *testing.T) {
		jfs, holds, _ := newRenameRetryHarness(t)
		jukeboxed(t, jfs, holds, "publish", "dir/a.mov", renameCall(jfs, "dir/a.mov", "dir/b.mov"))
		wantNoEnt(t, "rename to another target", jfs.Rename("dir/a.mov", "dir/c.mov"))
		// It neither consumed nor disturbed the entry for the real resend.
		if err := jfs.Rename("dir/a.mov", "dir/b.mov"); err != nil {
			t.Fatalf("resent rename = %v, want success", err)
		}
	})
	t.Run("other principal", func(t *testing.T) {
		jfs, holds, _ := newRenameRetryHarness(t)
		jfs.SetMutationPrincipal("1:uid-501")
		jukeboxed(t, jfs, holds, "publish", "dir/a.mov", renameCall(jfs, "dir/a.mov", "dir/b.mov"))
		other := &juiceFS{handler: jfs.handler}
		other.SetMutationPrincipal("1:uid-502")
		wantNoEnt(t, "same rename from another principal", other.Rename("dir/a.mov", "dir/b.mov"))
		if err := jfs.Rename("dir/a.mov", "dir/b.mov"); err != nil {
			t.Fatalf("resent rename = %v, want success", err)
		}
	})
	t.Run("last resend inside the retry cap", func(t *testing.T) {
		// macOS resends at most 30 s after the late completion.
		jfs, holds, _ := newRenameRetryHarness(t)
		jukeboxed(t, jfs, holds, "publish", "dir/a.mov", renameCall(jfs, "dir/a.mov", "dir/b.mov"))
		jfs.handler.completedMutations.now = func() time.Time { return time.Now().Add(32 * time.Second) }
		if err := jfs.Rename("dir/a.mov", "dir/b.mov"); err != nil {
			t.Fatalf("resend 32 s after completion = %v, want success", err)
		}
	})
	t.Run("expired", func(t *testing.T) {
		jfs, holds, _ := newRenameRetryHarness(t)
		jukeboxed(t, jfs, holds, "publish", "dir/a.mov", renameCall(jfs, "dir/a.mov", "dir/b.mov"))
		jfs.handler.completedMutations.now = func() time.Time { return time.Now().Add(40 * time.Second) }
		wantNoEnt(t, "resend 40 s after completion", jfs.Rename("dir/a.mov", "dir/b.mov"))
	})
	t.Run("answered rename", func(t *testing.T) {
		jfs, _, _ := newRenameRetryHarness(t)
		// Nothing is remembered for a rename whose reply was delivered.
		if err := jfs.Rename("dir/a.mov", "dir/b.mov"); err != nil {
			t.Fatal(err)
		}
		wantNoEnt(t, "repeat of an answered rename", jfs.Rename("dir/a.mov", "dir/b.mov"))
	})
}

// The resend's own reply can miss the bound too. The client resends again and
// still needs the answer.
func TestRenameReplayLostToTheBoundIsRememberedAgain(t *testing.T) {
	jfs, holds, _ := newRenameRetryHarness(t)
	jukeboxed(t, jfs, holds, "publish", "dir/a.mov", renameCall(jfs, "dir/a.mov", "dir/b.mov"))
	jukeboxed(t, jfs, holds, "replay", "dir/a.mov", renameCall(jfs, "dir/a.mov", "dir/b.mov"))
	if err := jfs.Rename("dir/a.mov", "dir/b.mov"); err != nil {
		t.Fatalf("third transmission = %v, want success", err)
	}
	wantNoEnt(t, "fourth transmission", jfs.Rename("dir/a.mov", "dir/b.mov"))
}

// Independently of any remembered outcome, a rename of a missing source must
// not cancel the destination's spool entry on its way to ENOENT.
func TestRenameMissingSourceKeepsSpooledDestination(t *testing.T) {
	jfs, spool, _, _ := newSpoolWiredHandlerNoDrain(t)
	simulateNFSCreateThenWrite(t, jfs, "dst.mov", patternedPayload(64<<10, 2), 64<<10)
	wantNoEnt(t, "rename of a missing source", jfs.Rename("absent.mov", "dst.mov"))
	if _, ok := spool.LookupActive("dst.mov"); !ok {
		t.Fatal("rename of a missing source cancelled the destination's spool entry")
	}
	awaitNamespaceIdle(t, &jfs.handler.namespace)
}

func TestCompletedMutationsBounded(t *testing.T) {
	var c completedMutations
	base := time.Now()
	now := base
	c.now = func() time.Time { return now }
	key := func(i int) completedMutationKey { return completedMutationKey{op: "rename", from: fmt.Sprint(i)} }
	for i := 0; i <= completedMutationCap; i++ {
		now = base.Add(time.Duration(i) * time.Millisecond)
		c.record(key(i))
	}
	if len(c.entries) != completedMutationCap {
		t.Fatalf("entries = %d, want cap %d", len(c.entries), completedMutationCap)
	}
	if c.take(key(0)) {
		t.Fatal("oldest entry survived past the cap")
	}
	if !c.take(key(completedMutationCap)) {
		t.Fatal("newest entry missing")
	}
	if c.take(key(completedMutationCap)) {
		t.Fatal("entry answered twice")
	}
}

// Another actor changes the names between the late completion and the
// resend. Running the resend could only act on that actor's files.
func TestRenameResendAfterAnotherActorKeepsTheirFiles(t *testing.T) {
	t.Run("source recreated", func(t *testing.T) {
		jfs, holds, root := newRenameRetryHarness(t)
		jukeboxed(t, jfs, holds, "publish", "dir/a.mov", renameCall(jfs, "dir/a.mov", "dir/b.mov"))
		seedFile(t, jfs, jfs.handler.store, root, "dir/a.mov", []byte("new take"))
		if err := jfs.Rename("dir/a.mov", "dir/b.mov"); err != nil {
			t.Fatalf("resent rename = %v, want success", err)
		}
		if got, err := os.ReadFile(filepath.Join(root, "dir/b.mov")); err != nil || string(got) != "original" {
			t.Fatalf("resend replaced the renamed file: dir/b.mov = %q, %v", got, err)
		}
		if got, err := os.ReadFile(filepath.Join(root, "dir/a.mov")); err != nil || string(got) != "new take" {
			t.Fatalf("dir/a.mov = %q, %v", got, err)
		}
		// The entry is spent: renaming the new file is a new request and runs.
		if err := jfs.Rename("dir/a.mov", "dir/b.mov"); err != nil {
			t.Fatal(err)
		}
		if got, err := os.ReadFile(filepath.Join(root, "dir/b.mov")); err != nil || string(got) != "new take" {
			t.Fatalf("dir/b.mov after a new rename = %q, %v", got, err)
		}
	})
	t.Run("destination replaced", func(t *testing.T) {
		jfs, holds, root := newRenameRetryHarness(t)
		jukeboxed(t, jfs, holds, "publish", "dir/a.mov", renameCall(jfs, "dir/a.mov", "dir/b.mov"))
		if err := jfs.Remove("dir/b.mov"); err != nil {
			t.Fatal(err)
		}
		seedFile(t, jfs, jfs.handler.store, root, "dir/b.mov", []byte("someone else's"))
		if err := jfs.Rename("dir/a.mov", "dir/b.mov"); err != nil {
			t.Fatalf("resent rename = %v, want success", err)
		}
		if got, err := os.ReadFile(filepath.Join(root, "dir/b.mov")); err != nil || string(got) != "someone else's" {
			t.Fatalf("dir/b.mov = %q, %v", got, err)
		}
	})
}

// REMOVE resent after another actor recreated the name: the resend used to
// cancel the new file's spool entry and delete it.
func TestRemoveResentAfterJukeboxKeepsRecreatedFile(t *testing.T) {
	jfs, spool, _, _ := newSpoolWiredHandlerNoDrain(t)
	holds := installPhaseHolds(jfs)
	simulateNFSCreateThenWrite(t, jfs, "take.mov", patternedPayload(64<<10, 1), 64<<10)
	jukeboxed(t, jfs, holds, "publish", "take.mov", func() error { return jfs.Remove("take.mov") })
	if _, ok := spool.LookupActive("take.mov"); ok {
		t.Fatal("precondition: the late remove did not finish")
	}
	payload := patternedPayload(128<<10, 7)
	simulateNFSCreateThenWrite(t, jfs, "take.mov", payload, 64<<10)

	if err := jfs.Remove("take.mov"); err != nil {
		t.Fatalf("resent remove = %v, want success", err)
	}
	if _, ok := spool.LookupActive("take.mov"); !ok {
		t.Fatal("resent remove deleted the recreated file")
	}
	if fi, err := jfs.Stat("take.mov"); err != nil || fi.Size() != int64(len(payload)) {
		t.Fatalf("recreated take.mov = %v, %v", fi, err)
	}
	// Spent: a later remove is a new request and runs.
	if err := jfs.Remove("take.mov"); err != nil {
		t.Fatal(err)
	}
	if _, err := jfs.Stat("take.mov"); !os.IsNotExist(err) {
		t.Fatalf("Stat after a new remove = %v, want ENOENT", err)
	}
}

func writeAtVia(t *testing.T, jfs *juiceFS, name string, payload []byte) {
	t.Helper()
	f, err := jfs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.(io.WriterAt).WriteAt(payload, 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// CREATE resent after another actor wrote to the file it created: an unchecked
// create of an existing file truncates (macOS EXCLUSIVE is served unchecked).
func TestCreateResentAfterJukeboxKeepsWrittenBytes(t *testing.T) {
	for _, mode := range []string{"spool", "backend"} {
		t.Run(mode, func(t *testing.T) {
			var jfs *juiceFS
			name := "dir/c.mov"
			if mode == "spool" {
				jfs, _, _, _ = newSpoolWiredHandlerNoDrain(t)
				name = "c.mov"
			} else {
				jfs, _, _ = newRenameRetryHarness(t)
			}
			holds := installPhaseHolds(jfs)
			jukeboxed(t, jfs, holds, "publish", name, func() error {
				f, err := jfs.Create(name)
				_ = closeNamespaceFile(f)
				return err
			})
			payload := patternedPayload(64<<10, 4)
			writeAtVia(t, jfs, name, payload)
			before := jfs.handler.store.LookupByPath(name)
			if before == nil {
				t.Fatal("precondition: created file is not mirrored")
			}

			f, err := jfs.Create(name)
			if err != nil {
				t.Fatalf("resent create = %v, want success", err)
			}
			if f.Name() != name {
				t.Fatalf("resent create named %q", f.Name())
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			if fi, err := jfs.Stat(name); err != nil || fi.Size() != int64(len(payload)) {
				t.Fatalf("after resent create: %v, %v; want %d bytes kept", fi, err, len(payload))
			}
			// The spool path never truncated; there a resend re-minted the
			// synthetic inode, staling the handle the writer already holds.
			if after := jfs.handler.store.LookupByPath(name); after == nil || after.Inode != before.Inode {
				t.Fatalf("resent create changed the file's inode: %d -> %+v", before.Inode, after)
			}
		})
	}
}

// A replayed CREATE must not hand back a name that no longer exists.
func TestCreateResentAfterRemovalCreatesAgain(t *testing.T) {
	jfs, holds, _ := newRenameRetryHarness(t)
	createC := func() error {
		f, err := jfs.Create("dir/c.mov")
		_ = closeNamespaceFile(f)
		return err
	}
	jukeboxed(t, jfs, holds, "publish", "dir/c.mov", createC)
	if err := jfs.Remove("dir/c.mov"); err != nil {
		t.Fatal(err)
	}
	if err := createC(); err != nil {
		t.Fatalf("resent create = %v", err)
	}
	if _, err := jfs.Stat("dir/c.mov"); err != nil {
		t.Fatalf("resent create left no file: %v", err)
	}
}
