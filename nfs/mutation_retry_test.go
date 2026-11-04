package nfs

import (
	"crypto/sha256"
	"errors"
	"fmt"
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

// A request that only resembles the late one is never answered from memory.
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
	t.Run("destination replaced", func(t *testing.T) {
		jfs, holds, root := newRenameRetryHarness(t)
		jukeboxed(t, jfs, holds, "publish", "dir/a.mov", renameCall(jfs, "dir/a.mov", "dir/b.mov"))
		if err := jfs.Remove("dir/b.mov"); err != nil {
			t.Fatal(err)
		}
		seedFile(t, jfs, jfs.handler.store, root, "dir/b.mov", []byte("someone else's"))
		wantNoEnt(t, "resend after the destination changed", jfs.Rename("dir/a.mov", "dir/b.mov"))
		if got, err := os.ReadFile(filepath.Join(root, "dir/b.mov")); err != nil || string(got) != "someone else's" {
			t.Fatalf("dir/b.mov = %q, %v", got, err)
		}
	})
	t.Run("source recreated", func(t *testing.T) {
		jfs, holds, root := newRenameRetryHarness(t)
		jukeboxed(t, jfs, holds, "publish", "dir/a.mov", renameCall(jfs, "dir/a.mov", "dir/b.mov"))
		seedFile(t, jfs, jfs.handler.store, root, "dir/a.mov", []byte("new take"))
		// A source exists again, so this is a new rename and must run.
		if err := jfs.Rename("dir/a.mov", "dir/b.mov"); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(filepath.Join(root, "dir/a.mov")); !os.IsNotExist(err) {
			t.Fatalf("new source was answered from memory instead of renamed: %v", err)
		}
		if got, err := os.ReadFile(filepath.Join(root, "dir/b.mov")); err != nil || string(got) != "new take" {
			t.Fatalf("dir/b.mov = %q, %v", got, err)
		}
	})
	t.Run("expired", func(t *testing.T) {
		jfs, holds, _ := newRenameRetryHarness(t)
		jukeboxed(t, jfs, holds, "publish", "dir/a.mov", renameCall(jfs, "dir/a.mov", "dir/b.mov"))
		jfs.handler.completedMutations.now = func() time.Time { return time.Now().Add(completedMutationTTL) }
		wantNoEnt(t, "resend after the TTL", jfs.Rename("dir/a.mov", "dir/b.mov"))
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
		c.record(key(i), mutationIdentity{inode: uint64(i)})
	}
	if len(c.entries) != completedMutationCap {
		t.Fatalf("entries = %d, want cap %d", len(c.entries), completedMutationCap)
	}
	if _, ok := c.take(key(0)); ok {
		t.Fatal("oldest entry survived past the cap")
	}
	if id, ok := c.take(key(completedMutationCap)); !ok || id.inode != completedMutationCap {
		t.Fatalf("newest entry = %+v, %v", id, ok)
	}
	if _, ok := c.take(key(completedMutationCap)); ok {
		t.Fatal("entry answered twice")
	}
}
