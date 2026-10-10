package nfs

// Regression cover for the xattr-loss bug (2026-07-28).
//
// R1 wired the metadata pub/sub into FDPool.Invalidate on the belief that those
// events describe REMOTE mutations. `juicemount:metadata` also replays THIS
// process's own writes and MetadataEvent has no origin field, so our own write
// invalidated its own in-flight pooled WRITE fd. macOS rewrites a ._ AppleDouble
// sidecar once per xattr (observed: 6 rewrites in ~70 ms), each drain published
// an event back at us, and quarantine/FinderTags/whereFrom/FinderInfo were
// silently lost on copy — qa-battery 01-file-types, 2/2 LOST with R1 present and
// 2/2 PASS with it absent, on both 10GbE and WiFi.
//
// The fix splits the invalidation by slot: second-hand (pub/sub) mutations drop
// READ fds only; the local Rename/Remove RPC path still drops BOTH, because
// there we performed the mutation ourselves and C2/C3 require it.
//
// NOTE the unit tests written alongside R1 did NOT catch this: they invoked the
// hook directly and asserted the read fd went stale, never replaying a
// self-originated write. Assert the WRITE slot SURVIVES, or this reopens.

import (
	"os"
	"path/filepath"
	"testing"
)

// TestInvalidateReadsSparesWriteSlot is the direct regression assertion: a
// second-hand invalidation must drop the pooled read fd and leave the pooled
// write fd usable, so an in-flight local write is never torn down by an event
// this process itself generated.
func TestInvalidateReadsSparesWriteSlot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "._sidecar.bin")
	if err := os.WriteFile(path, []byte("seed"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p := NewFDPool()
	defer p.Stop()

	rfd, err := p.Get(path)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	wfd, err := p.GetWrite(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("GetWrite: %v", err)
	}
	p.Release(path)
	p.ReleaseWrite(path)

	p.InvalidateReads(path)

	// The write slot must be the SAME fd — untouched, still pooled, still usable.
	wfd2, err := p.GetWrite(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("GetWrite after InvalidateReads: %v", err)
	}
	if wfd2 != wfd {
		t.Fatalf("write slot was invalidated by InvalidateReads — this is the xattr-loss bug")
	}
	if _, err := wfd2.WriteAt([]byte("xattrpayload"), 0); err != nil {
		t.Fatalf("write fd unusable after InvalidateReads: %v", err)
	}
	p.ReleaseWrite(path)

	// The read slot must have been dropped: a fresh Get returns a NEW fd.
	rfd2, err := p.Get(path)
	if err != nil {
		t.Fatalf("Get after InvalidateReads: %v", err)
	}
	if rfd2 == rfd {
		t.Fatalf("read slot survived InvalidateReads — C1/C3 with a remote actor is reopened")
	}
	p.Release(path)
}

// TestInvalidateStillDropsBothSlots pins the LOCAL path's contract, so the
// read-only split above cannot be over-applied to Rename/Remove and silently
// reopen C2/C3.
func TestInvalidateStillDropsBothSlots(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "clip.mov")
	if err := os.WriteFile(path, []byte("seed"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p := NewFDPool()
	defer p.Stop()

	rfd, _ := p.Get(path)
	wfd, err := p.GetWrite(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("GetWrite: %v", err)
	}
	p.Release(path)
	p.ReleaseWrite(path)

	p.Invalidate(path)

	if rfd2, _ := p.Get(path); rfd2 == rfd {
		t.Fatalf("Invalidate left the READ slot pooled — C1 reopened")
	}
	p.Release(path)
	wfd2, err := p.GetWrite(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("GetWrite: %v", err)
	}
	if wfd2 == wfd {
		t.Fatalf("Invalidate left the WRITE slot pooled — C2/C3 reopened")
	}
	p.ReleaseWrite(path)
}

// TestInvalidateReadsTreeSparesWriteSlots is the directory-rename analogue: a
// second-hand subtree mutation must not tear down descendants' write fds.
func TestInvalidateReadsTreeSparesWriteSlots(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "reel")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	child := filepath.Join(sub, "._a.mov")
	if err := os.WriteFile(child, []byte("seed"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p := NewFDPool()
	defer p.Stop()

	rfd, _ := p.Get(child)
	wfd, err := p.GetWrite(child, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("GetWrite: %v", err)
	}
	p.Release(child)
	p.ReleaseWrite(child)

	p.InvalidateReadsTree(sub)

	wfd2, err := p.GetWrite(child, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("GetWrite after InvalidateReadsTree: %v", err)
	}
	if wfd2 != wfd {
		t.Fatalf("descendant write slot invalidated by InvalidateReadsTree — xattr-loss bug at subtree scope")
	}
	p.ReleaseWrite(child)

	if rfd2, _ := p.Get(child); rfd2 == rfd {
		t.Fatalf("descendant read slot survived InvalidateReadsTree")
	}
	p.Release(child)
}
