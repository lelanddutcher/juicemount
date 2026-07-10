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

// TestFlushStaleClosesIdleAndReopens pins #12's main population: idle pooled
// fds (cached for reuse) close immediately on FlushStale, and the next Get
// opens a FRESH fd instead of re-serving the dead one.
func TestFlushStaleClosesIdleAndReopens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	os.WriteFile(path, []byte("hello"), 0o644)

	p := NewFDPool()
	defer p.Stop()

	fd1, err := p.Get(path)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	p.Release(path) // idle now (refCount 0, still pooled)

	closed, marked := p.FlushStale()
	if closed != 1 || marked != 0 {
		t.Fatalf("FlushStale = (%d,%d), want (1,0)", closed, marked)
	}
	// The old fd must actually be closed.
	if _, err := fd1.ReadAt(make([]byte, 1), 0); err == nil {
		t.Fatal("stale idle fd still readable after FlushStale")
	}
	// Next Get reopens fresh and works.
	fd2, err := p.Get(path)
	if err != nil {
		t.Fatalf("Get after flush: %v", err)
	}
	defer p.Release(path)
	if _, err := fd2.ReadAt(make([]byte, 1), 0); err != nil {
		t.Fatalf("fresh fd unreadable: %v", err)
	}
	if fd2 == fd1 {
		t.Fatal("Get re-served the flushed fd object")
	}
}

// TestFlushStaleHeldDisplaced pins the held-entry path: a HELD fd is marked
// (not closed under its holder), the next Get displaces it with a fresh fd,
// the displaced fd lands in the orphan list, and the old holder's key-based
// Release cannot drive the new entry negative (clamp).
func TestFlushStaleHeldDisplaced(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	os.WriteFile(path, []byte("hello"), 0o644)

	p := NewFDPool()
	defer p.Stop()

	held, err := p.Get(path) // holder A keeps this across the "remount"
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	closed, marked := p.FlushStale()
	if closed != 0 || marked != 1 {
		t.Fatalf("FlushStale = (%d,%d), want (0,1)", closed, marked)
	}
	// Held fd must NOT be closed under its holder yet.
	if _, err := held.ReadAt(make([]byte, 1), 0); err != nil {
		t.Fatalf("held fd closed under its holder: %v", err)
	}

	// Holder B: must get a FRESH fd, never the stale one.
	fresh, err := p.Get(path)
	if err != nil {
		t.Fatalf("Get after flush: %v", err)
	}
	if fresh == held {
		t.Fatal("Get re-served the stale held fd")
	}
	p.mu.Lock()
	orphans := len(p.orphans)
	entry := p.entries[fdKey{path: path, write: false}]
	p.mu.Unlock()
	if orphans != 1 {
		t.Fatalf("orphans = %d, want 1 (displaced held fd)", orphans)
	}
	if entry == nil || entry.stale || entry.refCount != 1 {
		t.Fatalf("fresh entry state wrong: %+v", entry)
	}

	// Old holder A releases: key-based, lands on the NEW entry — clamp
	// means it can take it to 0 but the later B release must not underflow.
	p.Release(path) // A (mis-landed, tolerated)
	p.Release(path) // B
	p.mu.Lock()
	rc := p.entries[fdKey{path: path, write: false}].refCount
	p.mu.Unlock()
	if rc < 0 {
		t.Fatalf("refCount underflow: %d", rc)
	}
}

// TestFlushStaleWriteSide: the write keyspace gets the same treatment.
func TestFlushStaleWriteSide(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "w.bin")
	os.WriteFile(path, []byte("hello"), 0o644)

	p := NewFDPool()
	defer p.Stop()

	wfd, err := p.GetWrite(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("GetWrite: %v", err)
	}
	p.ReleaseWrite(path)
	if closed, _ := p.FlushStale(); closed != 1 {
		t.Fatalf("write-side idle fd not closed")
	}
	fresh, err := p.GetWrite(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("GetWrite after flush: %v", err)
	}
	defer p.ReleaseWrite(path)
	if fresh == wfd {
		t.Fatal("GetWrite re-served the flushed fd")
	}
	if _, err := fresh.WriteAt([]byte("x"), 0); err != nil {
		t.Fatalf("fresh write fd broken: %v", err)
	}
}
