package nfs

// Serving-path data integrity, round 2 (2026-07-28): the INVALIDATION-DURING-OPEN
// hole and the refcount debt it double-counts.
//
// Get/GetWrite deliberately drop p.mu across os.Open/os.OpenFile (a JuiceFS FUSE
// open costs ~60 ms; holding the single pool mutex across it convoys every other
// caller — the 2026-06-14 "error 100060" convoy). Everything below lives in that
// window:
//
//	S1  a RENAME/REMOVE that lands mid-open found NO ENTRY at the key and no-op'd,
//	    and the open then pooled an fd for the PRE-rename inode. C1/C2/C3
//	    verbatim, narrowed from deterministic to racy.
//	S2  a Release that lands mid-open found no entry either, and was SILENTLY
//	    DISCARDED while the same ref stayed parked as pendingRefs debt — so the
//	    entry the open created settled at refCount 1 with zero real holders:
//	    un-evictable forever, fd pinned for the process lifetime, HasOpenRefs
//	    permanently true (which disables the phantom-purge Lstat gate for the path).
//
// The window is opened DETERMINISTICALLY with a FIFO: a POSIX open of a FIFO for
// reading blocks until a writer opens it (and vice versa), so the test controls
// exactly when the open inside Get/GetWrite returns. No production seam, no sleeps
// as synchronization. The content-level proof (which needs real files) is the
// rename/recreate race at the bottom.

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
)

// mkfifoT creates a FIFO whose open() blocks until the opposite end arrives.
func mkfifoT(t *testing.T, dir, name string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := syscall.Mkfifo(p, 0o600); err != nil {
		t.Skipf("mkfifo unsupported here (%v) — this test needs a blocking open", err)
	}
	return p
}

// waitOpensInFlight blocks until exactly n opens are registered as in flight.
// This is the synchronization point that proves a Get/GetWrite really is INSIDE
// the open syscall with p.mu released — the window every bug here lives in.
func waitOpensInFlight(t *testing.T, p *FDPool, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		p.mu.Lock()
		got := len(p.openInFlight)
		p.mu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d in-flight open(s), have %d", n, got)
		}
		time.Sleep(200 * time.Microsecond)
	}
}

type getResult struct {
	fd  *os.File
	err error
}

// TestFDPoolInvalidateDuringInFlightReadOpenIsNeverPooled is S1, read side.
//
// Sequence (the one the pre-fix code cannot see): READ calls Get, misses,
// releases p.mu, enters the open; RENAME runs os.Rename then Invalidate, which
// finds NO ENTRY at the key and returns (0,0); the open returns an fd on the
// PRE-rename inode and Get POOLS it. Every later read of that path is then served
// another file's bytes — C1, with no error at any layer.
func TestFDPoolInvalidateDuringInFlightReadOpenIsNeverPooled(t *testing.T) {
	dir := t.TempDir()
	fifo := mkfifoT(t, dir, "slow-open.mov")

	p := NewFDPool()
	defer p.Stop()

	first := make(chan getResult, 1)
	go func() {
		fd, err := p.Get(fifo)
		first <- getResult{fd, err}
	}()
	waitOpensInFlight(t, p, 1)

	// The RENAME/REMOVE RPC lands INSIDE the open window. Pre-fix this is a pure
	// no-op — there is no entry to invalidate yet.
	if closed, marked := p.Invalidate(fifo); closed != 0 || marked != 0 {
		t.Fatalf("Invalidate = (%d,%d); the key has no ENTRY yet — the in-flight open is the whole problem", closed, marked)
	}

	// Let the open complete.
	w, err := os.OpenFile(fifo, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open write end: %v", err)
	}
	r := <-first
	if r.err != nil {
		t.Fatalf("Get: %v", r.err)
	}
	w.Close()

	// The caller keeps its fd (it was valid when it was opened, and the RPC that
	// asked for it raced the rename) — but the POOL must never hand it out again.
	p.mu.Lock()
	e := p.entries[fdKey{path: fifo, write: false}]
	p.mu.Unlock()
	if e == nil {
		t.Fatal("no entry at all after the racing Get")
	}
	if !e.stale {
		t.Fatal("S1: the fd opened ACROSS an Invalidate was pooled as SERVABLE — " +
			"every later read/write of this path gets the pre-rename inode (C1/C2/C3, racy)")
	}
	p.Release(fifo)

	// Behavioral proof, not just the flag: a LATER caller must get a fresh open.
	second := make(chan getResult, 1)
	go func() {
		fd, err := p.Get(fifo)
		second <- getResult{fd, err}
	}()
	waitOpensInFlight(t, p, 1)
	w2, err := os.OpenFile(fifo, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open write end (2): %v", err)
	}
	r2 := <-second
	if r2.err != nil {
		t.Fatalf("Get (2): %v", r2.err)
	}
	defer p.Release(fifo)
	w2.Close()
	if r2.fd == r.fd {
		t.Fatal("S1: Get re-served the fd that was opened across an Invalidate")
	}
	// The displaced fd was unheld by then, so it must have been closed outright.
	if _, err := r.fd.Read(make([]byte, 1)); !errors.Is(err, fs.ErrClosed) {
		t.Errorf("the invalidated fd was not closed on displacement: err = %v", err)
	}
}

// TestFDPoolInvalidateDuringInFlightWriteOpenIsNeverPooled is S1, WRITE side —
// the gap the auditor named explicitly, and the more destructive half: a pooled
// write fd on a pre-rename inode sends every later WriteAt INSIDE the file that
// was renamed away (C2 — the archived version is destroyed and the new file is
// never written).
func TestFDPoolInvalidateDuringInFlightWriteOpenIsNeverPooled(t *testing.T) {
	dir := t.TempDir()
	fifo := mkfifoT(t, dir, "slow-open.prproj")

	p := NewFDPool()
	defer p.Stop()

	first := make(chan getResult, 1)
	go func() {
		// O_WRONLY on a FIFO blocks until a READER arrives.
		fd, err := p.GetWrite(fifo, os.O_WRONLY, 0o644)
		first <- getResult{fd, err}
	}()
	waitOpensInFlight(t, p, 1)

	p.Invalidate(fifo)

	rd, err := os.OpenFile(fifo, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open read end: %v", err)
	}
	r := <-first
	if r.err != nil {
		t.Fatalf("GetWrite: %v", r.err)
	}
	rd.Close()

	p.mu.Lock()
	e := p.entries[fdKey{path: fifo, write: true}]
	p.mu.Unlock()
	if e == nil {
		t.Fatal("no write-slot entry after the racing GetWrite")
	}
	if !e.stale {
		t.Fatal("S1/write: the fd opened ACROSS an Invalidate was pooled as SERVABLE — " +
			"later WRITE RPCs land inside the renamed-away file (C2)")
	}
	p.ReleaseWrite(fifo)
}

// TestFDPoolInvalidateTreeDuringInFlightOpenIsNeverPooled covers the DIRECTORY
// face of S1: a folder rename must poison in-flight opens of its DESCENDANTS
// (one syscall re-parents every one of them), while a sibling that merely shares
// a string prefix must be left alone.
func TestFDPoolInvalidateTreeDuringInFlightOpenIsNeverPooled(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "shoot"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "shootX"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	deep := mkfifoT(t, filepath.Join(root, "shoot"), "clip.mov")
	sibling := mkfifoT(t, filepath.Join(root, "shootX"), "clip.mov")

	p := NewFDPool()
	defer p.Stop()

	res := make(chan getResult, 2)
	for _, f := range []string{deep, sibling} {
		go func(f string) {
			fd, err := p.Get(f)
			res <- getResult{fd, err}
		}(f)
	}
	waitOpensInFlight(t, p, 2)

	p.InvalidateTree(filepath.Join(root, "shoot"))

	for _, f := range []string{deep, sibling} {
		w, err := os.OpenFile(f, os.O_WRONLY, 0)
		if err != nil {
			t.Fatalf("open write end %s: %v", f, err)
		}
		w.Close()
	}
	<-res
	<-res

	p.mu.Lock()
	de := p.entries[fdKey{path: deep, write: false}]
	se := p.entries[fdKey{path: sibling, write: false}]
	p.mu.Unlock()
	if de == nil || !de.stale {
		t.Fatalf("descendant open across InvalidateTree was pooled servable: %+v", de)
	}
	if se == nil || se.stale {
		t.Fatalf("a NON-descendant that merely shares the string prefix was poisoned: %+v", se)
	}
	p.Release(deep)
	p.Release(sibling)
}

// TestFDPoolReleaseIntoTheHoleIsNotDoubleCounted is S2, driven through the exact
// interleaving the auditor described.
//
//	A holds the read slot (refCount 1)
//	Invalidate            → A's entry is marked stale (A is mid-I/O)
//	B calls Get           → displaceStaleLocked deletes the entry and PARKS A's
//	                        ref as pendingRefs{refs:1}; B is now inside the open
//	                        with p.mu released
//	A calls Release       → p.entries[k] is ABSENT → pre-fix the decrement was
//	                        silently DROPPED while the debt stayed parked
//	B's open returns      → refCount = 1 + takePending(1) = 2, with ONE real holder
//	B releases            → refCount stays 1 FOREVER
//
// From there evictLoop (which needs refCount<=0) can never close the fd, and
// HasOpenRefs is permanently true so asyncConfirmPhantomPurge skips the path for
// the process lifetime.
func TestFDPoolReleaseIntoTheHoleIsNotDoubleCounted(t *testing.T) {
	dir := t.TempDir()
	fifo := mkfifoT(t, dir, "playback.mov")

	p := NewFDPool()
	defer p.Stop()

	// --- holder A takes the read slot -------------------------------------
	aCh := make(chan getResult, 1)
	go func() {
		fd, err := p.Get(fifo)
		aCh <- getResult{fd, err}
	}()
	waitOpensInFlight(t, p, 1)
	wa, err := os.OpenFile(fifo, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open write end (A): %v", err)
	}
	if a := <-aCh; a.err != nil {
		t.Fatalf("Get(A): %v", a.err)
	}
	wa.Close()

	// --- the rename marks it stale under its live holder -------------------
	if closed, marked := p.Invalidate(fifo); closed != 0 || marked != 1 {
		t.Fatalf("Invalidate = (%d,%d), want (0,1) — A is held, so it must be MARKED not closed", closed, marked)
	}

	// --- B displaces it and parks A's ref as debt, then blocks in the open --
	bCh := make(chan getResult, 1)
	go func() {
		fd, err := p.Get(fifo)
		bCh <- getResult{fd, err}
	}()
	waitOpensInFlight(t, p, 1)

	p.mu.Lock()
	debt := p.pendingRefs[fdKey{path: fifo, write: false}].refs
	_, entryPresent := p.entries[fdKey{path: fifo, write: false}]
	p.mu.Unlock()
	if debt != 1 || entryPresent {
		t.Fatalf("precondition: debt=%d entryPresent=%v, want debt=1 and NO entry (the hole)", debt, entryPresent)
	}

	// --- A's Release lands IN THE HOLE ------------------------------------
	p.Release(fifo)

	// --- B's open completes -----------------------------------------------
	wb, err := os.OpenFile(fifo, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open write end (B): %v", err)
	}
	if b := <-bCh; b.err != nil {
		t.Fatalf("Get(B): %v", b.err)
	}
	wb.Close()

	p.mu.Lock()
	rc := p.entries[fdKey{path: fifo, write: false}].refCount
	p.mu.Unlock()
	if rc != 1 {
		t.Fatalf("S2: entry refCount = %d after B's open, want 1 (B is the only real holder) — "+
			"A's Release was dropped into the hole AND still counted as parked debt", rc)
	}

	// --- B, the only real holder, releases --------------------------------
	p.Release(fifo)

	p.mu.Lock()
	rc = p.entries[fdKey{path: fifo, write: false}].refCount
	p.mu.Unlock()
	if rc != 0 {
		t.Fatalf("S2: refCount = %d with every holder released, want 0 — the entry is now UN-EVICTABLE "+
			"(evictLoop requires refCount<=0), its fd pinned open for the process lifetime", rc)
	}
	if p.HasOpenRefs(fifo) {
		t.Fatal("S2: HasOpenRefs is permanently true with no holders — asyncConfirmPhantomPurge " +
			"skips this path for the rest of the process's life")
	}
}

// TestFDPoolReleaseIntoTheHoleFloorsAtZero guards the direction the S2 fix opens
// up: paying down debt must never go negative, and an unpaired Release against a
// key with neither an entry nor debt must stay a no-op (it would otherwise
// under-count the NEXT entry created there and let Invalidate close an fd
// out from under a live reader).
func TestFDPoolReleaseIntoTheHoleFloorsAtZero(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p := NewFDPool()
	defer p.Stop()

	k := fdKey{path: path, write: false}

	// Debt of 1, released twice.
	p.mu.Lock()
	p.pendingRefs[k] = pendingRef{refs: 1, since: time.Now()}
	p.mu.Unlock()
	p.Release(path)
	p.Release(path)
	p.Release(path)
	p.mu.Lock()
	_, stillThere := p.pendingRefs[k]
	p.mu.Unlock()
	if stillThere {
		t.Fatal("debt entry survived being paid down to zero")
	}

	// The next real Get must start at exactly 1 — no negative carry.
	if _, err := p.Get(path); err != nil {
		t.Fatalf("Get: %v", err)
	}
	p.mu.Lock()
	rc := p.entries[k].refCount
	p.mu.Unlock()
	if rc != 1 {
		t.Fatalf("fresh entry refCount = %d, want 1 (over-released debt must not carry negative)", rc)
	}
	p.Release(path)
}

// TestFDPoolRenameRecreateUnderConcurrentReadsNeverServesOldBytes is the CONTENT
// proof for S1 — the assertion the pre-existing race test structurally cannot
// make, because it hammers one unchanging file.
//
// Here the file's IDENTITY changes constantly: every round replaces the path with
// a new same-length generation payload (rename-into-place, exactly what an
// atomic-save / re-ingest does) and then Invalidates, publishing the generation
// only AFTER the Invalidate returns. That ordering makes one invariant checkable
// on every single read:
//
//	a reader that observed generation G before calling Get can NEVER be served
//	bytes older than G
//
// because after Invalidate(G) no entry pooled before G survives as servable, and
// any later open resolves the NAME at a point where it already means G or newer.
// The pre-fix code breaks it: an open in flight across Invalidate(G) pools the
// G-1 inode's fd as SERVABLE, and the next reader is handed G-1's bytes —
// right length, wrong content, no error.
func TestFDPoolRenameRecreateUnderConcurrentReadsNeverServesOldBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "reel.mov")

	// Fixed-width payload: a wrong-identity read has the RIGHT LENGTH, which is
	// the silent case (no short read, no error, just another file's bytes).
	const width = 8
	payload := func(g int) []byte { return []byte(fmt.Sprintf("G%07d", g)) }
	parse := func(b []byte) (int, error) {
		if len(b) != width || b[0] != 'G' {
			return 0, fmt.Errorf("unparseable payload %q", b)
		}
		return strconv.Atoi(string(b[1:]))
	}

	if err := os.WriteFile(path, payload(0), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p := NewFDPool()
	defer p.Stop()

	var genMu sync.Mutex
	curGen := 0 // the newest generation whose Invalidate has already completed

	stop := make(chan struct{})
	errCh := make(chan error, 16)
	var wg sync.WaitGroup

	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, width)
			for {
				select {
				case <-stop:
					return
				default:
				}
				genMu.Lock()
				snap := curGen
				genMu.Unlock()

				fd, err := p.Get(path)
				if err != nil {
					errCh <- fmt.Errorf("Get: %w", err)
					return
				}
				n, err := fd.ReadAt(buf, 0)
				p.Release(path)
				if err != nil && n != width {
					errCh <- fmt.Errorf("ReadAt: %w", err)
					return
				}
				got, perr := parse(buf[:n])
				if perr != nil {
					errCh <- perr
					return
				}
				if got < snap {
					errCh <- fmt.Errorf("SILENT CORRUPTION: served generation %d to a reader that had "+
						"already observed generation %d — a pooled fd from before the rename is being "+
						"re-served (C1 via the in-flight-open hole)", got, snap)
					return
				}
			}
		}()
	}

	deadline := time.Now().Add(2 * time.Second)
	for g := 1; g <= 4000 && time.Now().Before(deadline); g++ {
		tmp := filepath.Join(dir, fmt.Sprintf(".stage%d", g))
		if err := os.WriteFile(tmp, payload(g), 0o644); err != nil {
			t.Fatalf("stage gen %d: %v", g, err)
		}
		// Atomic replace: `path` always resolves to SOME complete generation.
		if err := os.Rename(tmp, path); err != nil {
			t.Fatalf("rename gen %d: %v", g, err)
		}
		p.Invalidate(path)
		// Publish only AFTER the invalidation has returned, so a reader that
		// observes g is guaranteed no pre-g entry is still servable.
		genMu.Lock()
		curGen = g
		genMu.Unlock()

		select {
		case err := <-errCh:
			close(stop)
			wg.Wait()
			t.Fatal(err)
		default:
		}
	}
	close(stop)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// Quiesced: the pool must serve the FINAL generation, not something a racing
	// open cached on its way past the last Invalidate.
	genMu.Lock()
	final := curGen
	genMu.Unlock()
	fd, err := p.Get(path)
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}
	defer p.Release(path)
	buf := make([]byte, width)
	if _, err := fd.ReadAt(buf, 0); err != nil {
		t.Fatalf("final ReadAt: %v", err)
	}
	if !bytes.Equal(buf, payload(final)) {
		t.Fatalf("quiesced read = %q, want %q — the pool retained a stale-identity fd", buf, payload(final))
	}
}
