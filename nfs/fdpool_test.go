package nfs

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestFDPoolReadWriteKeyspaceSplit covers the QA-37 fix where a previously
// opened read-only fd (e.g. from a Stat → Get) would be silently returned
// to a writer calling GetWrite, causing EBADF on WriteAt and Finder -36.
//
// Contract after the fix:
//   - Get(path) and GetWrite(path, ...) live in independent slots.
//   - Release(path) drops the read slot; ReleaseWrite(path) drops the write slot.
//   - HasOpenRefs(path) reports true if EITHER slot has outstanding refs.
func TestFDPoolReadWriteKeyspaceSplit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	if err := os.WriteFile(path, []byte("hello"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p := NewFDPool()
	defer p.Stop()

	rfd, err := p.Get(path)
	if err != nil {
		t.Fatalf("Get(read): %v", err)
	}

	wfd, err := p.GetWrite(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("GetWrite: %v", err)
	}

	if rfd == wfd {
		t.Fatalf("read fd and write fd must be different — keyspace split broken")
	}

	// Writer should be able to write — would EBADF if the pool had handed
	// back the RDONLY fd from the read slot.
	if _, err := wfd.WriteAt([]byte("HELLO"), 0); err != nil {
		t.Fatalf("WriteAt on write-slot fd: %v", err)
	}

	open, active := p.Stats()
	if open != 2 || active != 2 {
		t.Fatalf("expected 2 open + 2 active (one per slot), got open=%d active=%d", open, active)
	}

	if !p.HasOpenRefs(path) {
		t.Fatalf("HasOpenRefs should be true while reader holds the read slot")
	}

	// Drop the reader; writer still active.
	p.Release(path)
	if !p.HasOpenRefs(path) {
		t.Fatalf("HasOpenRefs should still be true — writer slot still holds a ref")
	}

	// Drop the writer; now both slots drained.
	p.ReleaseWrite(path)
	if p.HasOpenRefs(path) {
		t.Fatalf("HasOpenRefs should be false once both slots are released")
	}
}

// TestFDPoolReleaseWrongSlotIsNoop guards against silently corrupting the
// wrong slot's refcount when callers mis-route a Close.
func TestFDPoolReleaseWrongSlotIsNoop(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "g.bin")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p := NewFDPool()
	defer p.Stop()

	if _, err := p.GetWrite(path, os.O_RDWR, 0); err != nil {
		t.Fatalf("GetWrite: %v", err)
	}

	// Calling read-side Release for a path that has no read slot must not
	// panic and must not decrement the write slot.
	p.Release(path)

	if !p.HasOpenRefs(path) {
		t.Fatalf("Release on absent read slot must not drain the write slot")
	}
}

// TestFDPoolConcurrentGetWrite exercises the double-check-under-lock path
// where two goroutines race to insert the same write-slot entry. Under
// Finder multi-stream copies, GetWrite is called per WRITE RPC and can
// easily fire concurrently for the same path. The loser fd is closed
// inside GetWrite; both callers must end up with the same VALID fd
// (writable, not yanked closed under them).
func TestFDPoolConcurrentGetWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "race.bin")
	if err := os.WriteFile(path, []byte(""), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p := NewFDPool()
	defer p.Stop()

	const N = 16
	fds := make([]*os.File, N)
	errs := make([]error, N)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			fd, err := p.GetWrite(path, os.O_RDWR, 0)
			fds[i] = fd
			errs[i] = err
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < N; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: GetWrite: %v", i, errs[i])
		}
		if fds[i] != fds[0] {
			t.Fatalf("goroutine %d returned a different fd (got %p, want %p) — keyspace winner-takes-all broken", i, fds[i], fds[0])
		}
	}

	// All goroutines got the winner fd. Writing through it must succeed
	// — i.e. the pool did not close the winner thinking it was a loser.
	if _, err := fds[0].WriteAt([]byte("ok"), 0); err != nil {
		t.Fatalf("WriteAt on shared write fd: %v", err)
	}

	// Drain all refs.
	for i := 0; i < N; i++ {
		p.ReleaseWrite(path)
	}
	if p.HasOpenRefs(path) {
		t.Fatalf("expected all refs drained after N=%d ReleaseWrite", N)
	}
}
