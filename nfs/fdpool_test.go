package nfs

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
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
	// refCount 2, not 1: B's own ref PLUS the CARRY for holder A, whose
	// key-based Release will land on this entry. Carrying it is what keeps
	// the new entry from reaching 0 while B is still reading (see
	// displaceStaleLocked).
	if entry == nil || entry.stale || entry.refCount != 2 {
		t.Fatalf("fresh entry state wrong: %+v (want refCount 2 = B + carry for A)", entry)
	}

	// Both holders release; the entry drains to exactly 0, never below.
	p.Release(path) // A (carried)
	p.Release(path) // B
	p.mu.Lock()
	rc := p.entries[fdKey{path: path, write: false}].refCount
	p.mu.Unlock()
	if rc != 0 {
		t.Fatalf("refCount = %d after both holders released, want 0", rc)
	}
}

// TestFDPoolPendingRefsKeepDisplacedEntryAlive is the direct regression for the
// accounting bug the Invalidate race test exposed: without the refcount DEBT
// parked by displaceStaleLocked (pendingRefs), holder A's key-based Release
// drops holder B's ref on the NEW entry, refCount hits 0 while B is mid-read,
// and the next Invalidate (which closes at refCount<=0) yanks B's fd — "file
// already closed" in the middle of playback. Trading silent corruption for a
// spurious hard error is not a fix, so this pins the accounting.
func TestFDPoolPendingRefsKeepDisplacedEntryAlive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "playback.mov")
	os.WriteFile(path, []byte("hello"), 0o644)

	p := NewFDPool()
	defer p.Stop()

	if _, err := p.Get(path); err != nil { // holder A
		t.Fatalf("Get(A): %v", err)
	}
	p.Invalidate(path)      // A is held → marked stale
	fdB, err := p.Get(path) // displaces; B gets a fresh fd
	if err != nil {
		t.Fatalf("Get(B): %v", err)
	}
	p.Release(path) // A's release, key-based → lands on B's entry

	// B is STILL reading. A second Invalidate must not close B's fd.
	p.Invalidate(path)
	if _, err := fdB.ReadAt(make([]byte, 1), 0); err != nil {
		t.Fatalf("holder B's fd closed under it after a mis-landed Release + Invalidate: %v", err)
	}
	p.Release(path) // B done
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

// ---------------------------------------------------------------------------
// Serving-path data integrity (C1/C2/C3, 2026-07-28).
//
// The pool is keyed by {path, write} — no inode, no generation — and until
// Invalidate existed NOTHING dropped a slot on rename or delete (FlushStale,
// which only fires on a watchdog FUSE remount, was the pool's ONLY invalidator
// anywhere in the tree). These tests pin the invalidation contract at the pool
// layer; the end-to-end RPC-level repros live in serving_path_integrity_test.go.
// ---------------------------------------------------------------------------

// TestFDPoolInvalidateDropsBothSlotsAndReopens is the core C1/C2/C3 unit: an
// IDLE pooled fd (the overwhelmingly common state — onRead does
// Get→ReadAt→Release per READ RPC) must be closed and unmapped by Invalidate,
// and the next Get/GetWrite must open a FRESH fd rather than re-serve the old
// identity.
func TestFDPoolInvalidateDropsBothSlotsAndReopens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "reel_A.mov")
	if err := os.WriteFile(path, []byte("AAAA"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p := NewFDPool()
	defer p.Stop()

	rfd, err := p.Get(path)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	wfd, err := p.GetWrite(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("GetWrite: %v", err)
	}
	// Idle both slots — the state the pool sits in between RPCs.
	p.Release(path)
	p.ReleaseWrite(path)

	closed, marked := p.Invalidate(path)
	if closed != 2 || marked != 0 {
		t.Fatalf("Invalidate = (closed=%d, marked=%d), want (2,0) — both slots must drop", closed, marked)
	}
	if open, active := p.Stats(); open != 0 || active != 0 {
		t.Fatalf("pool not empty after Invalidate: open=%d active=%d", open, active)
	}

	// Both old fds must be genuinely closed, not just unmapped.
	if _, err := rfd.ReadAt(make([]byte, 1), 0); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("read fd still usable after Invalidate: err=%v", err)
	}
	if _, err := wfd.WriteAt([]byte("x"), 0); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("write fd still usable after Invalidate: err=%v", err)
	}

	// Simulate the rename/recreate: a DIFFERENT file now lives at this path.
	if err := os.WriteFile(path, []byte("BBBB"), 0o644); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	fresh, err := p.Get(path)
	if err != nil {
		t.Fatalf("Get after Invalidate: %v", err)
	}
	defer p.Release(path)
	if fresh == rfd {
		t.Fatal("Get re-served the invalidated fd object")
	}
	buf := make([]byte, 4)
	if _, err := fresh.ReadAt(buf, 0); err != nil {
		t.Fatalf("fresh fd unreadable: %v", err)
	}
	if !bytes.Equal(buf, []byte("BBBB")) {
		t.Fatalf("fresh fd served %q, want %q — stale identity", buf, "BBBB")
	}
}

// TestFDPoolInvalidateHeldFDNotClosedUnderHolder pins the safety half of the
// contract: Invalidate must NEVER close an fd another goroutine is mid-ReadAt
// on. A HELD entry is marked stale (holder keeps reading), the next Get
// displaces it with a fresh open, and the displaced fd lands in the orphan
// list for the evict loop's grace close — identical discipline to FlushStale.
func TestFDPoolInvalidateHeldFDNotClosedUnderHolder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "held.mov")
	os.WriteFile(path, []byte("hello"), 0o644)

	p := NewFDPool()
	defer p.Stop()

	held, err := p.Get(path) // holder A is mid-read across the rename
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	closed, marked := p.Invalidate(path)
	if closed != 0 || marked != 1 {
		t.Fatalf("Invalidate = (closed=%d, marked=%d), want (0,1) — held fd must be marked, not closed", closed, marked)
	}
	if _, err := held.ReadAt(make([]byte, 1), 0); err != nil {
		t.Fatalf("held fd closed under its holder: %v", err)
	}

	fresh, err := p.Get(path)
	if err != nil {
		t.Fatalf("Get after Invalidate: %v", err)
	}
	if fresh == held {
		t.Fatal("Get re-served the invalidated held fd")
	}
	p.mu.Lock()
	orphans := len(p.orphans)
	entry := p.entries[fdKey{path: path, write: false}]
	p.mu.Unlock()
	if orphans != 1 {
		t.Fatalf("orphans = %d, want 1 (displaced held fd)", orphans)
	}
	// refCount 2 = the new holder's own ref PLUS the debt absorbed for the
	// displaced holder, whose key-based Release will land here.
	if entry == nil || entry.stale || entry.refCount != 2 {
		t.Fatalf("fresh entry state wrong: %+v (want refCount 2 = new holder + absorbed debt)", entry)
	}
	// Both holders release; the entry drains to exactly 0, never below.
	p.Release(path)
	p.Release(path)
	p.mu.Lock()
	rc := p.entries[fdKey{path: path, write: false}].refCount
	p.mu.Unlock()
	if rc != 0 {
		t.Fatalf("refCount = %d after both holders released, want 0", rc)
	}
}

// TestFDPoolInvalidateTreeCoversDescendantsOnly pins the DIRECTORY-rename case:
// one syscall re-parents every descendant, so every descendant's pooled fd goes
// stale at once. Siblings whose path merely shares a string prefix ("dirX") must
// NOT be collateral damage.
func TestFDPoolInvalidateTreeCoversDescendantsOnly(t *testing.T) {
	root := t.TempDir()
	mk := func(rel string) string {
		full := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", full, err)
		}
		if err := os.WriteFile(full, []byte("x"), 0o644); err != nil {
			t.Fatalf("seed %s: %v", full, err)
		}
		return full
	}
	dirSelf := filepath.Join(root, "dir")
	child := mk("dir/clip.mov")
	deep := mk("dir/sub/deep.mov")
	sibling := mk("dirX/other.mov") // shares the "dir" string prefix — must survive
	elsewhere := mk("other/c.mov")

	p := NewFDPool()
	defer p.Stop()

	for _, f := range []string{child, deep, sibling, elsewhere} {
		if _, err := p.Get(f); err != nil {
			t.Fatalf("Get(%s): %v", f, err)
		}
		p.Release(f)
	}
	if _, err := p.GetWrite(child, os.O_RDWR, 0o644); err != nil {
		t.Fatalf("GetWrite: %v", err)
	}
	p.ReleaseWrite(child)

	closed, marked := p.InvalidateTree(dirSelf)
	if closed != 3 || marked != 0 {
		t.Fatalf("InvalidateTree = (closed=%d, marked=%d), want (3,0): child read+write and deep read", closed, marked)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, k := range []fdKey{
		{path: child, write: false},
		{path: child, write: true},
		{path: deep, write: false},
	} {
		if _, ok := p.entries[k]; ok {
			t.Fatalf("descendant slot %+v survived InvalidateTree", k)
		}
	}
	for _, k := range []fdKey{
		{path: sibling, write: false},
		{path: elsewhere, write: false},
	} {
		if _, ok := p.entries[k]; !ok {
			t.Fatalf("non-descendant slot %+v was wrongly invalidated", k)
		}
	}
}

// TestFDPoolInvalidateTreeMatchesExactPath: a plain FILE rename must be covered
// by the same tree call juiceFS.Rename uses (its subtree is simply empty).
func TestFDPoolInvalidateTreeMatchesExactPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	os.WriteFile(path, []byte("x"), 0o644)

	p := NewFDPool()
	defer p.Stop()

	if _, err := p.Get(path); err != nil {
		t.Fatalf("Get: %v", err)
	}
	p.Release(path)
	if closed, _ := p.InvalidateTree(path); closed != 1 {
		t.Fatalf("InvalidateTree(exact file) closed=%d, want 1", closed)
	}
	if open, _ := p.Stats(); open != 0 {
		t.Fatalf("pool still has %d entries", open)
	}
}

// TestFDPoolInvalidateNilAndEmptySafe guards the degenerate inputs the wiring
// can hand us (a nil pool on a struct-literal handler; an empty path).
func TestFDPoolInvalidateNilAndEmptySafe(t *testing.T) {
	var nilPool *FDPool
	nilPool.Invalidate("/x")
	nilPool.InvalidateTree("/x")

	p := NewFDPool()
	defer p.Stop()
	p.Invalidate("")
	p.InvalidateTree("")
	p.InvalidateTree("/") // must not nuke the whole pool
}

// TestFDPoolInvalidateRacesInFlightRead is the -race guard for the whole point
// of the mark-stale-when-held design: a reader looping Get→ReadAt→Release must
// never observe a closed fd or a short/corrupt read while another goroutine
// hammers Invalidate on the same path. A regression that closes held fds shows
// up here as fs.ErrClosed / bad bytes, and any lock misuse shows up under -race.
func TestFDPoolInvalidateRacesInFlightRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "playback.mov")
	want := bytes.Repeat([]byte("MEDIA123"), 512) // 4 KiB
	if err := os.WriteFile(path, want, 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p := NewFDPool()
	defer p.Stop()

	stop := make(chan struct{})
	errCh := make(chan error, 8)
	var wg sync.WaitGroup

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, len(want))
			for {
				select {
				case <-stop:
					return
				default:
				}
				fd, err := p.Get(path)
				if err != nil {
					errCh <- err
					return
				}
				n, err := fd.ReadAt(buf, 0)
				p.Release(path)
				if err != nil {
					errCh <- err
					return
				}
				if n != len(want) || !bytes.Equal(buf, want) {
					errCh <- errors.New("short or corrupt read through pooled fd")
					return
				}
			}
		}()
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				p.Invalidate(path)
				p.InvalidateTree(dir)
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("in-flight read broken by concurrent Invalidate: %v", err)
	}
}
