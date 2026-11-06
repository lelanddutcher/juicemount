package nfs

// U7 (V2.3) — async unmirrored-dir refresh. An ONLINE readdir of a directory
// with ZERO mirror rows must return the mirror's (empty) answer immediately
// and refresh the mirror from FUSE in the BACKGROUND, so the READDIR RPC
// never blocks on FUSE. These tests drive juiceFS.ReadDir (the layer the
// go-nfs fork calls) against a temp dir standing in for the FUSE mount, the
// same stand-in the spool integration tests use.

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/metadata"
)

// newDirRefreshHarness builds a minimal handler over a temp FUSE root and a
// temp SQLite mirror, with known-clean global state (online, identity gate
// inert, async refresh enabled).
func newDirRefreshHarness(t *testing.T) (*juiceFS, *metadata.Store, string) {
	t.Helper()

	// Known-clean global state. Both are process globals another test could
	// have left dirty; reset going in AND on cleanup.
	pin.SetOffline(false)
	pin.ResetFUSEIdentityForTest()
	t.Cleanup(func() {
		pin.SetOffline(false)
		pin.ResetFUSEIdentityForTest()
	})
	t.Setenv("JM_ASYNC_DIR_REFRESH", "1")
	// A zero-row ReadDir takes the synchronous populate on a Fast link and
	// the async empty-then-pop-in path on any other class. The measured
	// class is process-wide and moves with real reads in earlier tests, so
	// pin the class a lone run sees unless this test already chose one.
	pinUnforcedLinkClass(t, netprofile.ClassMedium)

	// The refresh draws its readdir and child stats from these process-wide
	// gates. A slot still held by an earlier test's background goroutine
	// (it releases when its FUSE call returns) would turn this test's stats
	// into gate timeouts, so start only once they are free.
	if !pollUntil(t, 5*time.Second, gatesQuiescent) {
		t.Fatalf("FUSE gates not quiescent: prefetchGate=%d nfsLstatGate=%d", len(prefetchGate), len(nfsLstatGate))
	}

	fuseRoot := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	h := NewHandler(store, fuseRoot)
	t.Cleanup(func() {
		h.StopHandler()
		_ = store.Close()
	})
	return &juiceFS{handler: h}, store, fuseRoot
}

// mkFUSEDir creates fuseRoot/dir with the given file names so the FUSE
// stand-in has children the mirror doesn't know about.
func mkFUSEDir(t *testing.T, fuseRoot, dir string, names ...string) {
	t.Helper()
	p := filepath.Join(fuseRoot, dir)
	if err := os.MkdirAll(p, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", p, err)
	}
	for _, n := range names {
		if err := os.WriteFile(filepath.Join(p, n), []byte("x"), 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", n, err)
		}
	}
}

// waitFor polls cond until it returns true or the deadline passes.
func pollUntil(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

func mirrorHasChildren(store *metadata.Store, dir string) func() bool {
	return func() bool {
		kids, _ := store.ListChildren(dir)
		return len(kids) > 0
	}
}

// mirrorHasNChildren waits for a finished publication. The refresh publishes
// rows to the RAM mirror one at a time, so "some children" can be observed
// before the last one lands.
func mirrorHasNChildren(store *metadata.Store, dir string, n int) func() bool {
	return func() bool {
		kids, _ := store.ListChildren(dir)
		return len(kids) == n
	}
}

func gatesQuiescent() bool {
	return len(prefetchGate) == 0 && len(nfsLstatGate) == 0
}

func inFlightEmpty(h *JuiceMountHandler) func() bool {
	return func() bool {
		h.dirRefreshMu.Lock()
		defer h.dirRefreshMu.Unlock()
		return len(h.dirRefreshInFlight) == 0
	}
}

// TestAsyncDirRefreshPopulatesStore: the readdir returns empty immediately,
// and the async refresh lands the FUSE children in the mirror so the NEXT
// readdir serves them.
func TestAsyncDirRefreshPopulatesStore(t *testing.T) {
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	mkFUSEDir(t, fuseRoot, "d", "a.txt", "b.txt")

	infos, err := jfs.ReadDir("d")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(infos) != 0 {
		t.Fatalf("first ReadDir of unmirrored dir = %d entries, want 0 (mirror answer served immediately)", len(infos))
	}

	if !pollUntil(t, 3*time.Second, mirrorHasNChildren(store, "d", 2)) {
		kids, _ := store.ListChildren("d")
		t.Fatalf("mirror children = %d, want 2", len(kids))
	}
	if !pollUntil(t, 3*time.Second, inFlightEmpty(jfs.handler)) {
		t.Fatal("refresh worker never finished")
	}

	// The NEXT readdir serves from the mirror.
	infos, err = jfs.ReadDir("d")
	if err != nil {
		t.Fatalf("second ReadDir: %v", err)
	}
	if len(infos) != 2 {
		t.Fatalf("second ReadDir = %d entries, want 2", len(infos))
	}
	names := map[string]bool{}
	for _, fi := range infos {
		names[fi.Name()] = true
	}
	if !names["a.txt"] || !names["b.txt"] {
		t.Fatalf("second ReadDir names = %v, want a.txt+b.txt", names)
	}
}

// TestAsyncDirRefreshSingleFlight: while a refresh for a directory is in
// flight, a readdir storm on that directory must NOT spawn more goroutines —
// it coalesces (the phantomPurgeInFlight pattern). Simulated by holding the
// singleflight key, which is exactly the state an in-flight refresh holds.
func TestAsyncDirRefreshSingleFlight(t *testing.T) {
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	h := jfs.handler
	mkFUSEDir(t, fuseRoot, "d", "a.txt")

	// Simulate an in-flight refresh for "d".
	h.dirRefreshMu.Lock()
	h.dirRefreshInFlight["d"] = struct{}{}
	h.dirRefreshMu.Unlock()

	for i := 0; i < 5; i++ {
		infos, err := jfs.ReadDir("d")
		if err != nil {
			t.Fatalf("ReadDir #%d: %v", i, err)
		}
		if len(infos) != 0 {
			t.Fatalf("ReadDir #%d = %d entries, want 0", i, len(infos))
		}
	}

	// No worker may have been dispatched: the sem stays empty and the mirror
	// stays unpopulated.
	time.Sleep(300 * time.Millisecond)
	if n := len(h.dirRefreshSem); n != 0 {
		t.Fatalf("dirRefreshSem occupancy = %d, want 0 (storm must coalesce)", n)
	}
	if kids, _ := store.ListChildren("d"); len(kids) != 0 {
		t.Fatalf("mirror populated (%d children) despite in-flight singleflight key", len(kids))
	}

	// Release the key: the next readdir re-fires the refresh and it lands.
	h.dirRefreshMu.Lock()
	delete(h.dirRefreshInFlight, "d")
	h.dirRefreshMu.Unlock()

	if _, err := jfs.ReadDir("d"); err != nil {
		t.Fatalf("ReadDir after release: %v", err)
	}
	if !pollUntil(t, 3*time.Second, mirrorHasChildren(store, "d")) {
		t.Fatal("refresh did not re-fire after singleflight key released")
	}
}

// TestAsyncDirRefreshShedWhenSaturated: with all refresh workers busy
// (dirRefreshSem full), dispatch sheds — no goroutine, singleflight key
// cleared immediately so a later readdir can retry.
func TestAsyncDirRefreshShedWhenSaturated(t *testing.T) {
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	h := jfs.handler
	mkFUSEDir(t, fuseRoot, "d", "a.txt")

	// Saturate the worker semaphore.
	for i := 0; i < cap(h.dirRefreshSem); i++ {
		h.dirRefreshSem <- struct{}{}
	}

	infos, err := jfs.ReadDir("d")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(infos) != 0 {
		t.Fatalf("ReadDir = %d entries, want 0", len(infos))
	}
	// Shed path clears the key synchronously on the dispatch path.
	if !inFlightEmpty(h)() {
		t.Fatal("singleflight key not cleared on shed")
	}
	time.Sleep(200 * time.Millisecond)
	if kids, _ := store.ListChildren("d"); len(kids) != 0 {
		t.Fatalf("mirror populated (%d children) despite saturated sem", len(kids))
	}

	// Drain the semaphore: the next readdir dispatches normally.
	for i := 0; i < cap(h.dirRefreshSem); i++ {
		<-h.dirRefreshSem
	}
	if _, err := jfs.ReadDir("d"); err != nil {
		t.Fatalf("ReadDir after drain: %v", err)
	}
	if !pollUntil(t, 3*time.Second, mirrorHasChildren(store, "d")) {
		t.Fatal("refresh did not fire after semaphore drained")
	}
}

// TestAsyncDirRefreshKillSwitch: JM_ASYNC_DIR_REFRESH=0 restores the prior
// FOREGROUND fallback — the readdir itself returns the FUSE children and the
// mirror is populated synchronously; nothing is dispatched async.
func TestAsyncDirRefreshKillSwitch(t *testing.T) {
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	h := jfs.handler
	t.Setenv("JM_ASYNC_DIR_REFRESH", "0")
	mkFUSEDir(t, fuseRoot, "d", "a.txt")

	infos, err := jfs.ReadDir("d")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(infos) != 1 || infos[0].Name() != "a.txt" {
		t.Fatalf("foreground fallback ReadDir = %v, want [a.txt]", infos)
	}
	// Synchronous: the mirror row exists the moment ReadDir returns.
	if kids, _ := store.ListChildren("d"); len(kids) != 1 {
		t.Fatalf("mirror children after foreground fallback = %d, want 1", len(kids))
	}
	if !inFlightEmpty(h)() {
		t.Fatal("kill switch must not touch the async singleflight state")
	}
}

// TestAsyncDirRefreshOfflineUntouched: offline keeps its existing empty-fast
// semantics — no async dispatch, no FUSE traffic, no mirror writes.
func TestAsyncDirRefreshOfflineUntouched(t *testing.T) {
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	h := jfs.handler
	mkFUSEDir(t, fuseRoot, "d", "a.txt")

	pin.SetOffline(true)
	defer pin.SetOffline(false)

	infos, err := jfs.ReadDir("d")
	if err != nil {
		t.Fatalf("ReadDir offline: %v", err)
	}
	if len(infos) != 0 {
		t.Fatalf("offline ReadDir = %d entries, want 0", len(infos))
	}
	// The offline branch returns BEFORE the async dispatch: no singleflight
	// key was ever created, and the mirror stays empty.
	if !inFlightEmpty(h)() {
		t.Fatal("offline readdir must not create a refresh singleflight key")
	}
	time.Sleep(200 * time.Millisecond)
	if kids, _ := store.ListChildren("d"); len(kids) != 0 {
		t.Fatalf("mirror populated (%d children) while offline", len(kids))
	}
}

// TestAsyncDirRefreshIdentityGateRefusal: with the G0 FUSE-identity gate
// reporting NOT-OK (the temp fuseRoot is a plain local dir — same fsid as
// its parent), the async worker must refuse to readdir/insert. The RPC path
// itself still answers immediately (it never consults the gate — QA-35).
func TestAsyncDirRefreshIdentityGateRefusal(t *testing.T) {
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	h := jfs.handler
	mkFUSEDir(t, fuseRoot, "d", "a.txt")

	// Arm the gate on the plain-dir stand-in: statfs(fuseRoot) shares its
	// parent's fsid, so the gate reports NOT-OK — exactly the plain-dir
	// mountpoint condition G0 exists for.
	pin.SetFUSEIdentityPath(fuseRoot)
	t.Cleanup(pin.ResetFUSEIdentityForTest)
	if pin.FUSEIdentityOK() {
		t.Skip("temp dir unexpectedly reports as its own filesystem; cannot arm a failing identity gate here")
	}

	infos, err := jfs.ReadDir("d")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(infos) != 0 {
		t.Fatalf("ReadDir = %d entries, want 0", len(infos))
	}

	// The worker runs, refuses at the identity gate, and clears its key.
	if !pollUntil(t, 3*time.Second, inFlightEmpty(h)) {
		t.Fatal("refresh worker never finished")
	}
	if kids, _ := store.ListChildren("d"); len(kids) != 0 {
		t.Fatalf("mirror populated (%d children) despite failing identity gate", len(kids))
	}
}

// TestAsyncDirRefreshNeverOverwrites: an async result must be INSERT-only.
// A mirror row that appeared between dispatch and completion (here:
// pre-inserted) keeps its size/inode; only genuinely-new children land.
func TestAsyncDirRefreshNeverOverwrites(t *testing.T) {
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	mkFUSEDir(t, fuseRoot, "d", "a.txt", "b.txt")

	// Pre-existing mirror row for d/a.txt with a size and inode that differ
	// from what FUSE reports (1-byte file, real inode).
	pre := metadata.MakeEntry("d/a.txt", false, 999, time.Now().Add(-time.Hour), 424242)
	if err := store.Insert(pre); err != nil {
		t.Fatalf("pre-insert: %v", err)
	}

	// Drive the worker synchronously (same function the dispatch spawns).
	jfs.refreshUnmirroredDir("d", filepath.Join(fuseRoot, "d"))

	got := store.LookupByPath("d/a.txt")
	if got == nil {
		t.Fatal("pre-existing row vanished")
	}
	if got.Size != 999 || got.Inode != 424242 {
		t.Fatalf("pre-existing row overwritten by async result: size=%d inode=%d, want 999/424242", got.Size, got.Inode)
	}
	if store.LookupByPath("d/b.txt") == nil {
		t.Fatal("new child d/b.txt not inserted by async refresh")
	}
}

// ============================================================================
// Batch-3 adversarial-review coverage: bounded per-child stats (#0), the
// atomic insert-if-absent race (#1), and the scan-filtered-namespace
// unification (#2/#4).
// ============================================================================

// blockingDirEntry fakes a DirEntry whose Info() — a LAZY lstat(2) on darwin
// — wedges until released: the macFUSE watchdog-thrash mode review #0
// targets.
type blockingDirEntry struct {
	name    string
	release chan struct{}
}

func (b *blockingDirEntry) Name() string      { return b.name }
func (b *blockingDirEntry) IsDir() bool       { return false }
func (b *blockingDirEntry) Type() os.FileMode { return 0 }
func (b *blockingDirEntry) Info() (os.FileInfo, error) {
	<-b.release
	return nil, os.ErrNotExist
}

// TestInfoWithTimeoutWedgedStat: a wedged Info() must return ok=false within
// the budget, keep holding its gate slot while parked (bounded leak), and
// release the slot once the underlying stat finally returns.
func TestInfoWithTimeoutWedgedStat(t *testing.T) {
	gate := make(chan struct{}, 1)
	de := &blockingDirEntry{name: "wedged.mov", release: make(chan struct{})}

	_, _, ok, parked := infoWithTimeout(metrics.FUSESrcDirRefresh, de, 50*time.Millisecond, 50*time.Millisecond, gate)
	if ok {
		t.Fatal("wedged Info() must report ok=false")
	}
	if parked == nil {
		t.Fatal("a timed-out stat must return its parked channel")
	}
	// The parked worker holds the gate slot until the stat returns…
	select {
	case gate <- struct{}{}:
		<-gate
		t.Fatal("gate slot free while the wedged Info() is still parked — the slot must be held (bounded leak)")
	default:
	}
	// …and releases it once the wedge clears.
	close(de.release)
	if !pollUntil(t, 2*time.Second, func() bool {
		select {
		case gate <- struct{}{}:
			<-gate
			return true
		default:
			return false
		}
	}) {
		t.Fatal("gate slot never released after the wedged Info() returned")
	}
	select {
	case <-parked:
	case <-time.After(2 * time.Second):
		t.Fatal("parked channel not closed after the wedged Info() returned")
	}
}

// TestInfoWithTimeoutCompletes: the healthy path returns the FileInfo.
func TestInfoWithTimeoutCompletes(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(dir)
	if err != nil || len(ents) != 1 {
		t.Fatalf("ReadDir: %v (%d entries)", err, len(ents))
	}
	gate := make(chan struct{}, 1)
	fi, ierr, ok, parked := infoWithTimeout(metrics.FUSESrcDirRefresh, ents[0], 2*time.Second, 2*time.Second, gate)
	if !ok || ierr != nil || fi == nil || fi.Name() != "a.txt" || parked != nil {
		t.Fatalf("infoWithTimeout = (%v, %v, ok=%v), want a.txt info", fi, ierr, ok)
	}
}

// TestColdDirListingBreaksOnWedgedStat: with the S4 fan-out DISABLED
// (JM_COLD_LIST_FANOUT=0 ⇒ K=1, serial), one wedged child must ABANDON the
// listing pass — entries behind the wedge are never stat'd — so a U7 worker
// always returns promptly and its deferred dirRefreshSem/singleflight release
// runs. The pass reports itself incomplete, with the parked stat, so callers
// record the folder for a later re-list. This is the serial kill-switch contract; the parallel contract is
// TestColdDirListingParallelWedgeCompletes.
func TestColdDirListingBreaksOnWedgedStat(t *testing.T) {
	jfs, _, fuseRoot := newDirRefreshHarness(t)
	t.Setenv("JM_COLD_LIST_FANOUT", "0") // force serial break semantics

	oldTimeout := fuseStatTimeout
	fuseStatTimeout = 50 * time.Millisecond
	t.Cleanup(func() { fuseStatTimeout = oldTimeout })

	// Two REAL healthy entries with a wedged fake between them; the entry
	// after the wedge must never be reached.
	mkFUSEDir(t, fuseRoot, "d", "a.txt", "c.txt")
	real, err := os.ReadDir(filepath.Join(fuseRoot, "d"))
	if err != nil || len(real) != 2 {
		t.Fatalf("ReadDir: %v (%d entries)", err, len(real))
	}
	wedged := &blockingDirEntry{name: "b-wedged.mov", release: make(chan struct{})}
	t.Cleanup(func() { close(wedged.release) })
	entries := []os.DirEntry{real[0], wedged, real[1]}

	gate := make(chan struct{}, 4)
	l := jfs.coldDirListing(metrics.FUSESrcDirRefresh, "d", entries, gate)
	infos, toInsert := l.infos, l.toInsert
	if l.complete || len(l.parked) != 1 {
		t.Fatalf("pass over a wedged stat: complete=%v parked=%d, want false/1", l.complete, len(l.parked))
	}
	if len(infos) != 1 || infos[0].Name() != "a.txt" {
		t.Fatalf("infos = %d entries, want only a.txt (break on wedge — no stats issued past it)", len(infos))
	}
	if len(toInsert) != 1 || toInsert[0].Path != "d/a.txt" {
		t.Fatalf("toInsert = %d entries, want only d/a.txt", len(toInsert))
	}
}

// TestColdDirListingParallelWedgeCompletes: with the S4 fan-out ENABLED, a
// single wedged child must NOT hang the pass — the pool completes promptly, the
// wedged child is DROPPED (bounded, its worker parks holding only its own gate
// slot until the stat returns), and healthy siblings — even ones ordered AFTER
// the wedge — still resolve (a strictly MORE complete listing than serial). The
// safety property preserved from the serial version is "always returns
// promptly," not "never stats past the wedge."
func TestColdDirListingParallelWedgeCompletes(t *testing.T) {
	jfs, _, fuseRoot := newDirRefreshHarness(t)
	// Default class (medium) → fan-out 24; be explicit for clarity.
	t.Setenv("JM_COLD_LIST_FANOUT", "24")

	oldTimeout := fuseStatTimeout
	fuseStatTimeout = 50 * time.Millisecond
	t.Cleanup(func() { fuseStatTimeout = oldTimeout })

	mkFUSEDir(t, fuseRoot, "d", "a.txt", "c.txt")
	real, err := os.ReadDir(filepath.Join(fuseRoot, "d"))
	if err != nil || len(real) != 2 {
		t.Fatalf("ReadDir: %v (%d entries)", err, len(real))
	}
	wedged := &blockingDirEntry{name: "b-wedged.mov", release: make(chan struct{})}
	t.Cleanup(func() { close(wedged.release) })
	entries := []os.DirEntry{real[0], wedged, real[1]}

	gate := make(chan struct{}, 8)
	start := time.Now()
	l := jfs.coldDirListing(metrics.FUSESrcDirRefresh, "d", entries, gate)
	infos, toInsert := l.infos, l.toInsert
	elapsed := time.Since(start)
	if l.complete {
		t.Fatal("a pass with a dropped (wedged) child reported complete")
	}

	// Must not hang: bounded by ~fuseStatTimeout (50ms) plus scheduling slack.
	if elapsed > 2*time.Second {
		t.Fatalf("parallel cold listing took %v — must complete promptly under one wedged child", elapsed)
	}
	names := map[string]bool{}
	for _, fi := range infos {
		names[fi.Name()] = true
	}
	// Both healthy siblings resolve; the wedged one is dropped.
	if !names["a.txt"] || !names["c.txt"] {
		t.Fatalf("infos names = %v, want both a.txt AND c.txt (fan-out resolves siblings past the wedge)", names)
	}
	if names["b-wedged.mov"] {
		t.Fatalf("wedged child leaked into the listing: %v", names)
	}
	if len(toInsert) != 2 {
		t.Fatalf("toInsert = %d entries, want 2 (both healthy children)", len(toInsert))
	}
}

// wedgeChild makes the first `passes` cold listing passes that list the named
// child see it as a DirEntry whose lstat does not return until release, so
// those passes end incomplete at the fuseStatTimeout bound. Call it BEFORE
// newDirRefreshHarness so its cleanup runs after the handler has stopped. It
// returns the number of passes that saw the wedge, and a release for every
// parked stat.
func wedgeChild(t *testing.T, name string, passes int32) (wedged func() int32, release func()) {
	t.Helper()
	var used atomic.Int32
	gate := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	prev := coldListingEntriesHook
	coldListingEntriesHook = func(dir string, entries []os.DirEntry) []os.DirEntry {
		if prev != nil {
			entries = prev(dir, entries)
		}
		out := make([]os.DirEntry, len(entries))
		copy(out, entries)
		for i, e := range entries {
			if e.Name() != name {
				continue
			}
			if n := used.Add(1); n > passes {
				used.Add(-1)
				break
			}
			out[i] = &blockingDirEntry{name: name, release: gate}
		}
		return out
	}
	t.Cleanup(func() {
		release()
		coldListingEntriesHook = prev
		if !pollUntil(t, 5*time.Second, gatesQuiescent) {
			t.Error("wedged stat never released its gate slot")
		}
	})
	return used.Load, release
}

// slowDirEntry answers its lstat after a fixed delay: a high-latency link.
type slowDirEntry struct {
	os.DirEntry
	delay time.Duration
}

func (s slowDirEntry) Info() (os.FileInfo, error) {
	time.Sleep(s.delay)
	return s.DirEntry.Info()
}

// errDirEntry fails its lstat with err.
type errDirEntry struct {
	name string
	err  error
}

func (e errDirEntry) Name() string               { return e.name }
func (e errDirEntry) IsDir() bool                { return false }
func (e errDirEntry) Type() os.FileMode          { return 0 }
func (e errDirEntry) Info() (os.FileInfo, error) { return nil, e.err }

// forceLinkClass pins the process-wide link class for this test and restores
// the override that was in place before it (often the harness pin).
func forceLinkClass(t *testing.T, c netprofile.LinkClass) {
	t.Helper()
	p := netprofile.Default()
	prev, had := p.ForcedClass()
	p.ForceClass(&c)
	t.Cleanup(func() {
		if had {
			p.ForceClass(&prev)
		} else {
			p.ForceClass(nil)
		}
	})
}

// pinUnforcedLinkClass pins c only when no class is forced yet, so a test that
// forced one before building its harness keeps it.
func pinUnforcedLinkClass(t *testing.T, c netprofile.LinkClass) {
	t.Helper()
	if _, forced := netprofile.Default().ForcedClass(); forced {
		return
	}
	forceLinkClass(t, c)
}

// fastRetry shortens the incomplete-listing backoff. Call before the harness.
func fastRetry(t *testing.T) {
	t.Helper()
	prev := dirListingRetryBase
	dirListingRetryBase = 20 * time.Millisecond
	t.Cleanup(func() { dirListingRetryBase = prev })
}

func sortedNames(infos []os.FileInfo) []string {
	names := make([]string, 0, len(infos))
	for _, fi := range infos {
		names = append(names, fi.Name())
	}
	sort.Strings(names)
	return names
}

func wantNames(t *testing.T, what string, infos []os.FileInfo, want ...string) {
	t.Helper()
	got := sortedNames(infos)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

func dirRecorded(h *JuiceMountHandler, dir string) bool {
	h.dirRefreshMu.Lock()
	defer h.dirRefreshMu.Unlock()
	return h.dirIncomplete[dir] != nil
}

// TestAsyncDirRefreshIncompletePassRelists: a refresh that ends incomplete
// on a hung child publishes what it reached and records the folder. Its next
// readdirs serve those rows but do not re-list while the hung stat still
// holds a gate slot; once it returns, a readdir re-lists the folder on a
// slow link (which has no speculative re-read), the full listing lands, the
// record clears and the folder's mtime moves so the client re-reads it.
func TestAsyncDirRefreshIncompletePassRelists(t *testing.T) {
	forceLinkClass(t, netprofile.ClassSlow)
	fastRetry(t)
	wedged, release := wedgeChild(t, "b.txt", 1)
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	h := jfs.handler
	mkFUSEDir(t, fuseRoot, "d", "a.txt", "b.txt", "c.txt")

	if infos, err := jfs.ReadDir("d"); err != nil || len(infos) != 0 {
		t.Fatalf("cold ReadDir = %d entries, %v; want 0 (async)", len(infos), err)
	}
	if !pollUntil(t, 5*time.Second, inFlightEmpty(h)) {
		t.Fatal("incomplete refresh never released its singleflight key")
	}
	if wedged() != 1 {
		t.Fatal("the refresh never listed the wedged child (test inconclusive)")
	}
	if kids, _ := store.ListChildren("d"); len(kids) != 2 {
		t.Fatalf("mirror children after incomplete pass = %d, want 2 (a.txt, c.txt published)", len(kids))
	}
	if !dirRecorded(h, "d") {
		t.Fatal("incomplete pass did not record the folder")
	}

	// The hung stat still holds its slot: readdirs serve the rows and must not
	// start another pass, however long Finder keeps asking.
	for i := 0; i < 3; i++ {
		time.Sleep(3 * dirListingRetryBase)
		infos, err := jfs.ReadDir("d")
		if err != nil {
			t.Fatal(err)
		}
		wantNames(t, "ReadDir while the stat is parked", infos, "a.txt", "c.txt")
		if !inFlightEmpty(h)() {
			t.Fatal("re-listed the folder while its earlier stat still holds a gate slot")
		}
	}
	if n := len(prefetchGate); n != 1 {
		t.Fatalf("prefetchGate occupancy = %d, want 1 (one slot per hung child)", n)
	}

	before := store.DirectoryVisibilityMtime("d", time.Time{})
	release()
	if !pollUntil(t, 5*time.Second, gatesQuiescent) {
		t.Fatal("released stat never freed its slot")
	}
	time.Sleep(3 * dirListingRetryBase)
	if _, err := jfs.ReadDir("d"); err != nil {
		t.Fatal(err)
	}
	if !pollUntil(t, 5*time.Second, mirrorHasNChildren(store, "d", 3)) {
		kids, _ := store.ListChildren("d")
		t.Fatalf("mirror children = %d, want 3 (folder not re-listed)", len(kids))
	}
	if !pollUntil(t, 5*time.Second, inFlightEmpty(h)) {
		t.Fatal("re-list never finished")
	}
	if dirRecorded(h, "d") {
		t.Fatal("complete pass did not clear the record")
	}
	if after := store.DirectoryVisibilityMtime("d", time.Time{}); !after.After(before) {
		t.Fatalf("directory mtime not advanced by the completing pass (%v -> %v)", before, after)
	}
	infos, err := jfs.ReadDir("d")
	if err != nil {
		t.Fatal(err)
	}
	wantNames(t, "ReadDir after re-list", infos, "a.txt", "b.txt", "c.txt")
}

// TestHungChildDoesNotStarveOtherFolders: a child whose stat never returns
// costs one prefetchGate slot. Re-reading its folder must not park another
// slot per readdir, or the 2-slot background gate fills and every other
// unloaded folder stays empty.
func TestHungChildDoesNotStarveOtherFolders(t *testing.T) {
	forceLinkClass(t, netprofile.ClassSlow)
	fastRetry(t)
	_, _ = wedgeChild(t, "b.txt", 1<<30) // hung for the whole test
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	h := jfs.handler
	mkFUSEDir(t, fuseRoot, "hung", "a.txt", "b.txt")
	mkFUSEDir(t, fuseRoot, "other", "x.txt", "y.txt")

	for i := 0; i < 4; i++ {
		if _, err := jfs.ReadDir("hung"); err != nil {
			t.Fatal(err)
		}
		if !pollUntil(t, 5*time.Second, inFlightEmpty(h)) {
			t.Fatal("refresh of the hung folder never returned")
		}
		time.Sleep(3 * dirListingRetryBase)
	}
	if n := len(prefetchGate); n != 1 {
		t.Fatalf("prefetchGate occupancy after 4 readdirs of the hung folder = %d, want 1", n)
	}

	if _, err := jfs.ReadDir("other"); err != nil {
		t.Fatal(err)
	}
	if !pollUntil(t, 5*time.Second, mirrorHasNChildren(store, "other", 2)) {
		kids, _ := store.ListChildren("other")
		t.Fatalf("other folder mirrored %d of 2 children: the hung child starved it", len(kids))
	}
	if !pollUntil(t, 5*time.Second, inFlightEmpty(h)) {
		t.Fatal("refresh of the other folder never finished")
	}
}

// TestSlowLinkLargeFolderCompletes: at 250 ms per stat on a slow link, a
// 30-child folder must complete in one pass. The per-stat budget used to
// include the wait for one of prefetchGate's 2 slots behind the pass's own
// 8-wide fan-out, so stats past the first few always timed out.
func TestSlowLinkLargeFolderCompletes(t *testing.T) {
	forceLinkClass(t, netprofile.ClassSlow)
	prev := coldListingEntriesHook
	coldListingEntriesHook = func(_ string, entries []os.DirEntry) []os.DirEntry {
		out := make([]os.DirEntry, len(entries))
		for i, e := range entries {
			out[i] = slowDirEntry{DirEntry: e, delay: 250 * time.Millisecond}
		}
		return out
	}
	t.Cleanup(func() { coldListingEntriesHook = prev })
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	h := jfs.handler
	names := make([]string, 30)
	for i := range names {
		names[i] = fmt.Sprintf("clip%02d.mov", i)
	}
	mkFUSEDir(t, fuseRoot, "big", names...)

	if _, err := jfs.ReadDir("big"); err != nil {
		t.Fatal(err)
	}
	if !pollUntil(t, 20*time.Second, inFlightEmpty(h)) {
		t.Fatal("refresh never finished")
	}
	if kids, _ := store.ListChildren("big"); len(kids) != 30 {
		t.Fatalf("mirror children after one slow-link pass = %d, want 30", len(kids))
	}
	if dirRecorded(h, "big") {
		t.Fatal("a complete slow-link pass was recorded as incomplete")
	}
}

// TestColdDirListingOnlyGoneIsBenign: a child deleted between the readdir and
// its lstat (ENOENT, ENOTDIR) is simply gone; any other stat error leaves an
// existing child out, so the pass is incomplete.
func TestColdDirListingOnlyGoneIsBenign(t *testing.T) {
	jfs, _, fuseRoot := newDirRefreshHarness(t)
	mkFUSEDir(t, fuseRoot, "d", "a.txt")
	real, err := os.ReadDir(filepath.Join(fuseRoot, "d"))
	if err != nil || len(real) != 1 {
		t.Fatalf("ReadDir: %v (%d entries)", err, len(real))
	}
	for _, tc := range []struct {
		err      error
		complete bool
	}{
		{&fs.PathError{Op: "lstat", Path: "x", Err: syscall.ENOENT}, true},
		{&fs.PathError{Op: "lstat", Path: "x", Err: syscall.ENOTDIR}, true},
		{&fs.PathError{Op: "lstat", Path: "x", Err: syscall.EIO}, false},
		{&fs.PathError{Op: "lstat", Path: "x", Err: syscall.EACCES}, false},
	} {
		entries := []os.DirEntry{real[0], errDirEntry{name: "x", err: tc.err}}
		l := jfs.coldDirListing(metrics.FUSESrcDirRefresh, "d", entries, make(chan struct{}, 2))
		if l.complete != tc.complete {
			t.Errorf("stat error %v: complete = %v, want %v", tc.err, l.complete, tc.complete)
		}
		wantNames(t, fmt.Sprintf("infos with %v", tc.err), l.infos, "a.txt")
	}
}

// TestSyncColdPopulateIncompletePass: on a fast link the first readdir waits
// for the refresh. An incomplete pass answers empty (not a partial listing)
// but publishes what it reached; once the hung stat returns, the next readdir
// re-lists within its wait and serves the full folder.
func TestSyncColdPopulateIncompletePass(t *testing.T) {
	forceLinkClass(t, netprofile.ClassFast)
	fastRetry(t)
	wedged, release := wedgeChild(t, "b.txt", 1)
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	t.Setenv("JM_SYNC_COLD_POPULATE", "1")
	// Not a latency test: wait for the whole pass, wedge included.
	prevTimeout := syncColdPopulateTimeout
	syncColdPopulateTimeout = 30 * time.Second
	t.Cleanup(func() { syncColdPopulateTimeout = prevTimeout })
	mkFUSEDir(t, fuseRoot, "d", "a.txt", "b.txt", "c.txt")

	infos, err := jfs.ReadDir("d")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if wedged() != 1 {
		t.Fatal("the refresh never listed the wedged child (test inconclusive)")
	}
	if len(infos) != 0 {
		t.Fatalf("fast-link ReadDir over an incomplete pass = %v, want empty", sortedNames(infos))
	}
	if !pollUntil(t, 5*time.Second, inFlightEmpty(jfs.handler)) {
		t.Fatal("incomplete refresh never released its singleflight key")
	}
	if kids, _ := store.ListChildren("d"); len(kids) != 2 {
		t.Fatalf("mirror children after incomplete pass = %d, want 2", len(kids))
	}

	release()
	if !pollUntil(t, 5*time.Second, gatesQuiescent) {
		t.Fatal("released stat never freed its slot")
	}
	time.Sleep(3 * dirListingRetryBase)
	infos, err = jfs.ReadDir("d")
	if err != nil {
		t.Fatalf("ReadDir after recovery: %v", err)
	}
	wantNames(t, "fast-link ReadDir after recovery", infos, "a.txt", "b.txt", "c.txt")
}

// TestForegroundColdListingIncompletePass: with the async refresh switched
// off, the foreground fallback returns what an incomplete pass reached
// (failing the RPC would make a hard mount retry it forever) and records the
// folder, so a later readdir re-lists it.
func TestForegroundColdListingIncompletePass(t *testing.T) {
	forceLinkClass(t, netprofile.ClassSlow)
	fastRetry(t)
	wedged, release := wedgeChild(t, "b.txt", 1)
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	t.Setenv("JM_ASYNC_DIR_REFRESH", "0")
	mkFUSEDir(t, fuseRoot, "d", "a.txt", "b.txt", "c.txt")

	infos, err := jfs.ReadDir("d")
	if wedged() != 1 {
		t.Fatal("the foreground listing never saw the wedged child (test inconclusive)")
	}
	if err != nil {
		t.Fatalf("foreground ReadDir over an incomplete pass: %v, want the partial listing", err)
	}
	wantNames(t, "foreground ReadDir over an incomplete pass", infos, "a.txt", "c.txt")
	if !dirRecorded(jfs.handler, "d") {
		t.Fatal("incomplete foreground pass did not record the folder")
	}

	release()
	if !pollUntil(t, 5*time.Second, gatesQuiescent) {
		t.Fatal("released stat never freed its slot")
	}
	time.Sleep(3 * dirListingRetryBase)
	if _, err := jfs.ReadDir("d"); err != nil {
		t.Fatal(err)
	}
	if !pollUntil(t, 5*time.Second, mirrorHasNChildren(store, "d", 3)) {
		kids, _ := store.ListChildren("d")
		t.Fatalf("mirror children = %d, want 3 (folder not re-listed)", len(kids))
	}
	if !pollUntil(t, 5*time.Second, inFlightEmpty(jfs.handler)) {
		t.Fatal("re-list never finished")
	}
	infos, err = jfs.ReadDir("d")
	if err != nil {
		t.Fatal(err)
	}
	wantNames(t, "ReadDir after re-list", infos, "a.txt", "b.txt", "c.txt")
}

// TestSingleRowInsertRelistsFolder: a Stat or ToHandle cache miss inserts
// one row. Into a folder with no rows, READDIR would serve that row as the
// whole folder (Premiere resolving a project's media). The folder is
// recorded, so on a slow link its next readdir re-lists it.
func TestSingleRowInsertRelistsFolder(t *testing.T) {
	for _, tc := range []struct {
		name   string
		insert func(jfs *juiceFS, p string)
	}{
		{"Stat", func(jfs *juiceFS, p string) {
			if _, err := jfs.Stat(p); err != nil {
				t.Fatalf("Stat(%s): %v", p, err)
			}
		}},
		{"ToHandle", func(jfs *juiceFS, p string) {
			if jfs.handler.ToHandle(jfs, strings.Split(p, "/")) == nil {
				t.Fatalf("ToHandle(%s) = nil", p)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			forceLinkClass(t, netprofile.ClassSlow)
			jfs, store, fuseRoot := newDirRefreshHarness(t)
			mkFUSEDir(t, fuseRoot, "proj/media", "clip1.mov", "clip2.mov", "clip3.mov")

			tc.insert(jfs, "proj/media/clip1.mov")
			if !pollUntil(t, 5*time.Second, mirrorHasNChildren(store, "proj/media", 1)) {
				t.Fatal("the cache miss did not insert its row (test inconclusive)")
			}
			if _, err := jfs.ReadDir("proj/media"); err != nil {
				t.Fatal(err)
			}
			if !pollUntil(t, 5*time.Second, mirrorHasNChildren(store, "proj/media", 3)) {
				kids, _ := store.ListChildren("proj/media")
				t.Fatalf("mirror children = %d, want 3 (the one-row folder was never listed)", len(kids))
			}
			if !pollUntil(t, 5*time.Second, inFlightEmpty(jfs.handler)) {
				t.Fatal("listing never finished")
			}
			infos, err := jfs.ReadDir("proj/media")
			if err != nil {
				t.Fatal(err)
			}
			wantNames(t, "ReadDir after the listing", infos, "clip1.mov", "clip2.mov", "clip3.mov")
		})
	}
}

// TestSingleRowInsertIntoListedFolderNotRecorded: a cache-miss insert into a
// folder that already has rows records nothing (no extra FUSE listing).
func TestSingleRowInsertIntoListedFolderNotRecorded(t *testing.T) {
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	mkFUSEDir(t, fuseRoot, "d", "a.txt", "b.txt")
	if err := store.Insert(metadata.MakeEntry("d/a.txt", false, 1, time.Now(), 4242)); err != nil {
		t.Fatal(err)
	}
	if _, err := jfs.Stat("d/b.txt"); err != nil {
		t.Fatal(err)
	}
	if dirRecorded(jfs.handler, "d") {
		t.Fatal("insert into a folder with rows recorded it as unlisted")
	}
}

// TestColdListFanout: the class-scaled fan-out width + kill-switch.
func TestColdListFanout(t *testing.T) {
	// Kill-switch forces serial regardless of class.
	t.Setenv("JM_COLD_LIST_FANOUT", "0")
	for _, c := range []netprofile.LinkClass{
		netprofile.ClassMetered, netprofile.ClassSlow,
		netprofile.ClassMedium, netprofile.ClassFast,
	} {
		if got := coldListFanout(c); got != 1 {
			t.Fatalf("coldListFanout(%v) with kill-switch = %d, want 1", c, got)
		}
	}

	// Without the kill-switch: WAN classes gentle (8), LAN classes wide (24).
	t.Setenv("JM_COLD_LIST_FANOUT", "")
	cases := []struct {
		class netprofile.LinkClass
		want  int
	}{
		{netprofile.ClassMetered, 8},
		{netprofile.ClassSlow, 8},
		{netprofile.ClassMedium, 24},
		{netprofile.ClassFast, 24},
	}
	for _, tc := range cases {
		if got := coldListFanout(tc.class); got != tc.want {
			t.Fatalf("coldListFanout(%v) = %d, want %d", tc.class, got, tc.want)
		}
	}
}

// TestSyncColdPopulateFastLink (S3): on a FAST link with the kill-switch on, a
// zero-mirror-row readdir returns the FUSE children POPULATED from THIS readdir
// (not empty-then-pop-in). On a non-fast link OR with the kill-switch off it
// FALLS THROUGH to the async empty-then-pop-in path (returns empty; the mirror
// fills in the background).
func TestSyncColdPopulateFastLink(t *testing.T) {
	forceClass := func(t *testing.T, c netprofile.LinkClass) {
		t.Helper()
		netprofile.Default().ForceClass(&c)
		t.Cleanup(func() { netprofile.Default().ForceClass(nil) })
	}

	t.Run("fast link populates synchronously", func(t *testing.T) {
		jfs, store, fuseRoot := newDirRefreshHarness(t)
		t.Setenv("JM_SYNC_COLD_POPULATE", "1")
		forceClass(t, netprofile.ClassFast)
		// This case checks that a fast link takes the synchronous path, not how
		// fast the populate runs: the 50 ms production budget is missed under
		// -race on a loaded host, and the fallback then returns empty.
		prevTimeout := syncColdPopulateTimeout
		syncColdPopulateTimeout = 30 * time.Second
		t.Cleanup(func() { syncColdPopulateTimeout = prevTimeout })
		mkFUSEDir(t, fuseRoot, "d", "a.txt", "b.txt")

		infos, err := jfs.ReadDir("d")
		if err != nil {
			t.Fatalf("ReadDir: %v", err)
		}
		if len(infos) != 2 {
			t.Fatalf("fast-link cold ReadDir = %d entries, want 2 (populated synchronously)", len(infos))
		}
		// Mirror populated synchronously — the row exists the moment ReadDir returns.
		if kids, _ := store.ListChildren("d"); len(kids) != 2 {
			t.Fatalf("mirror children after sync populate = %d, want 2", len(kids))
		}
	})

	t.Run("kill-switch falls through to async empty", func(t *testing.T) {
		jfs, _, fuseRoot := newDirRefreshHarness(t)
		t.Setenv("JM_SYNC_COLD_POPULATE", "0")
		forceClass(t, netprofile.ClassFast)
		mkFUSEDir(t, fuseRoot, "d", "a.txt", "b.txt")

		infos, err := jfs.ReadDir("d")
		if err != nil {
			t.Fatalf("ReadDir: %v", err)
		}
		if len(infos) != 0 {
			t.Fatalf("kill-switch ReadDir = %d entries, want 0 (async empty-then-pop-in)", len(infos))
		}
	})

	t.Run("non-fast link falls through to async empty", func(t *testing.T) {
		jfs, _, fuseRoot := newDirRefreshHarness(t)
		t.Setenv("JM_SYNC_COLD_POPULATE", "1")
		forceClass(t, netprofile.ClassSlow)
		mkFUSEDir(t, fuseRoot, "d", "a.txt", "b.txt")

		infos, err := jfs.ReadDir("d")
		if err != nil {
			t.Fatalf("ReadDir: %v", err)
		}
		if len(infos) != 0 {
			t.Fatalf("slow-link ReadDir = %d entries, want 0 (stays async on a non-fast link)", len(infos))
		}
	})
}

// Fast-link waits must share the existing background budget, even when the
// backend cannot start. Excess requests shed without creating parked jobs.
func TestFastColdDirectoryRefreshUsesBoundedBackgroundWorkers(t *testing.T) {
	class := netprofile.ClassFast
	netprofile.Default().ForceClass(&class)
	t.Cleanup(func() { netprofile.Default().ForceClass(nil) })
	jfs, store, root := newDirRefreshHarness(t)
	t.Setenv("JM_SYNC_COLD_POPULATE", "1")
	if len(prefetchGate) != 0 || len(nfsLstatGate) != 0 {
		t.Fatal("fixture backend gates are not quiescent")
	}
	for i := 0; i < cap(prefetchGate); i++ {
		prefetchGate <- struct{}{}
	}
	released := false
	release := func() {
		if !released {
			for i := 0; i < cap(prefetchGate); i++ {
				<-prefetchGate
			}
			released = true
		}
	}
	defer func() {
		release()
		if !pollUntil(t, 3*time.Second, inFlightEmpty(jfs.handler)) {
			t.Error("background refreshes did not join")
		}
	}()
	names := []string{"one", "two", "three", "four", "shed"}
	for _, name := range names {
		mkFUSEDir(t, root, name, "a.txt")
	}
	for _, name := range names {
		start := time.Now()
		infos, err := jfs.ReadDir(name)
		if err != nil || len(infos) != 0 {
			t.Fatalf("blocked backend result: %d, %v", len(infos), err)
		}
		if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
			t.Errorf("backend budget exceeded for %s: %s", name, elapsed)
		}
	}
	jfs.handler.dirRefreshMu.Lock()
	active, keys := jfs.handler.dirRefreshActive, len(jfs.handler.dirRefreshInFlight)
	_, shedPresent := jfs.handler.dirRefreshInFlight["shed"]
	jfs.handler.dirRefreshMu.Unlock()
	if active != 4 || keys != 4 || shedPresent || len(jfs.handler.dirRefreshSem) != 4 {
		t.Fatalf("refresh bound: active=%d keys=%d shed=%v", active, keys, shedPresent)
	}
	if len(nfsLstatGate) != 0 {
		t.Fatal("cold background work occupied foreground FUSE admission")
	}
	release()
	if !pollUntil(t, 3*time.Second, inFlightEmpty(jfs.handler)) {
		t.Fatal("released refreshes did not finish")
	}
	for _, name := range names[:4] {
		if kids, _ := store.ListChildren(name); len(kids) != 1 {
			t.Errorf("%s children=%d", name, len(kids))
		}
	}
	if kids, _ := store.ListChildren("shed"); len(kids) != 0 {
		t.Fatal("shed request ran backend work")
	}
}

// TestReadDirScanFilteredShortCircuit (#2/#4): a zero-row ONLINE readdir of
// a scan-filtered namespace must return empty immediately — no async
// dispatch, no foreground FUSE readdir, no mirror inserts. Otherwise every
// walker entering .trash/.juicemount (ls -a, rsync, Spotlight, OpenLoupe)
// re-mirrors what the #78 open-GC removed, every session.
func TestReadDirScanFilteredShortCircuit(t *testing.T) {
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	h := jfs.handler
	mkFUSEDir(t, fuseRoot, ".juicemount/derivatives", "p.mp4")
	mkFUSEDir(t, fuseRoot, ".trash/2026-07-02-08", "1-2-x.mov")

	for _, dir := range []string{".juicemount", ".juicemount/derivatives", ".trash", ".trash/2026-07-02-08"} {
		infos, err := jfs.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir(%s): %v", dir, err)
		}
		if len(infos) != 0 {
			t.Fatalf("ReadDir(%s) = %d entries, want 0 (short-circuit)", dir, len(infos))
		}
	}
	if !inFlightEmpty(h)() {
		t.Fatal("scan-filtered readdir dispatched an async refresh")
	}
	time.Sleep(200 * time.Millisecond)
	for _, p := range []string{
		".juicemount/derivatives", ".juicemount/derivatives/p.mp4",
		".trash/2026-07-02-08", ".trash/2026-07-02-08/1-2-x.mov",
	} {
		if store.LookupByPath(p) != nil {
			t.Fatalf("mirror row %q re-created for a scan-filtered namespace", p)
		}
	}

	// The JM_ASYNC_DIR_REFRESH=0 foreground fallback short-circuits
	// identically (the guard sits above BOTH paths).
	t.Setenv("JM_ASYNC_DIR_REFRESH", "0")
	infos, err := jfs.ReadDir(".juicemount")
	if err != nil || len(infos) != 0 {
		t.Fatalf("foreground ReadDir(.juicemount) = (%d entries, %v), want (0, nil)", len(infos), err)
	}
	if kids, _ := store.ListChildren(".juicemount"); len(kids) != 0 {
		t.Fatalf("foreground fallback mirrored %d filtered children", len(kids))
	}
}

// TestColdListingListsButNeverMirrorsFiltered (#2): the cold listing keeps
// scan-filtered children in the RETURNED listing (client-visible behavior
// unchanged — the bare dirs still appear at the volume root) but never
// mirrors them.
func TestColdListingListsButNeverMirrorsFiltered(t *testing.T) {
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	t.Setenv("JM_ASYNC_DIR_REFRESH", "0") // foreground path → synchronous asserts
	mkFUSEDir(t, fuseRoot, ".juicemount", "d.jpg")
	mkFUSEDir(t, fuseRoot, "movies", "clip.mov")

	infos, err := jfs.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir(.): %v", err)
	}
	names := map[string]bool{}
	for _, fi := range infos {
		names[fi.Name()] = true
	}
	if !names[".juicemount"] || !names["movies"] {
		t.Fatalf("root listing = %v, want .juicemount AND movies visible", names)
	}
	if store.LookupByPath("movies") == nil {
		t.Fatal("regular child not mirrored by the cold listing")
	}
	if store.LookupByPath(".juicemount") != nil {
		t.Fatal("bare .juicemount mirrored by a FUSE-sourced listing — the #78 filter must apply")
	}
}

// TestPrefetchChildrenSkipsFilteredNamespace (#2): the prefetcher must never
// warm a filtered namespace — neither when asked directly (a walker
// descended into it) nor via the fast-link root fan-out (which was
// re-mirroring the .juicemount derivative tree one level per navigation).
func TestPrefetchChildrenSkipsFilteredNamespace(t *testing.T) {
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	h := jfs.handler
	mkFUSEDir(t, fuseRoot, ".juicemount/derivatives", "p.mp4")
	mkFUSEDir(t, fuseRoot, "movies", "clip.mov")

	// Direct.
	h.prefetchChildren(".juicemount")
	if kids, _ := store.ListChildren(".juicemount"); len(kids) != 0 {
		t.Fatalf("prefetchChildren(.juicemount) mirrored %d children", len(kids))
	}

	// Root fan-out (tmpfs readdir is instant → fast-link path): movies is
	// warmed; .juicemount is skipped from toInsert AND from the subdir
	// fan-out.
	h.prefetchChildren(".")
	if !pollUntil(t, 3*time.Second, mirrorHasChildren(store, "movies")) {
		t.Fatal("fan-out never warmed the regular sibling (test inconclusive)")
	}
	time.Sleep(200 * time.Millisecond) // let any stray fan-out land
	if store.LookupByPath(".juicemount") != nil {
		t.Fatal("root prefetch mirrored the bare .juicemount row")
	}
	if kids, _ := store.ListChildren(".juicemount"); len(kids) != 0 {
		t.Fatalf("root fan-out descended into .juicemount (%d children mirrored)", len(kids))
	}
}

// A prefetch whose rows take more than one batch publishes them a batch (and
// a cache chunk) at a time. The folder is recorded before the first batch,
// so a READDIR in between re-lists it instead of serving part of it as the
// whole folder, and the record this pass made is cleared once all landed.
func TestPrefetchChildrenRecordsMultiBatchPublication(t *testing.T) {
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	h := jfs.handler
	names := make([]string, coldListingInsertBatch+1)
	for i := range names {
		names[i] = fmt.Sprintf("f%04d.mov", i)
	}
	mkFUSEDir(t, fuseRoot, "big", names...)
	recorded := false
	called := false
	prefetchPublishHook = func(dir string) {
		if dir != "big" {
			return
		}
		called = true
		recorded, _ = store.HasDirListingIncomplete("big")
	}
	t.Cleanup(func() { prefetchPublishHook = nil })

	h.prefetchChildren("big")
	if !called {
		t.Fatal("the prefetch never published (test inconclusive)")
	}
	if !recorded {
		t.Fatal("a multi-batch prefetch published without recording the folder first")
	}
	if kids, _ := store.ListChildren("big"); len(kids) != len(names) {
		t.Fatalf("prefetch mirrored %d of %d children", len(kids), len(names))
	}
	if has, _ := store.HasDirListingIncomplete("big"); has {
		t.Fatal("the prefetch left its own record after every row landed")
	}
}

// bigPrefetchFolder makes a FUSE folder whose prefetch takes more than one
// batch, so the pass records it.
func bigPrefetchFolder(t *testing.T, fuseRoot string) int {
	t.Helper()
	names := make([]string, coldListingInsertBatch+1)
	for i := range names {
		names[i] = fmt.Sprintf("f%04d.mov", i)
	}
	mkFUSEDir(t, fuseRoot, "big", names...)
	return len(names)
}

// The incomplete record is shared by every listing path. A mark another
// pass makes while a multi-batch prefetch publishes (here a cold listing
// that left a stat parked) must survive the prefetch's own clear.
func TestPrefetchChildrenKeepsAConcurrentMark(t *testing.T) {
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	h := jfs.handler
	n := bigPrefetchFolder(t, fuseRoot)
	prefetchPublishHook = func(dir string) {
		if dir == "big" {
			h.markDirListingIncomplete("big", nil)
		}
	}
	t.Cleanup(func() { prefetchPublishHook = nil })

	h.prefetchChildren("big")
	if kids, _ := store.ListChildren("big"); len(kids) != n {
		t.Fatalf("prefetch mirrored %d of %d children (test inconclusive)", len(kids), n)
	}
	if has, _ := store.HasDirListingIncomplete("big"); !has {
		t.Fatal("the prefetch cleared a record another pass made after its own mark")
	}
	h.dirRefreshMu.Lock()
	st := h.dirIncomplete["big"]
	h.dirRefreshMu.Unlock()
	if st == nil {
		t.Fatal("the record left memory, so READDIR would not re-list the folder")
	}
}

// A prefetch that skipped a child (its stat failed) is a partial pass: it
// must not clear the record it made before publishing.
func TestPrefetchChildrenWithFailedStatKeepsItsRecord(t *testing.T) {
	jfs, store, fuseRoot := newDirRefreshHarness(t)
	h := jfs.handler
	n := bigPrefetchFolder(t, fuseRoot)
	prefetchEntriesHook = func(dir string, entries []os.DirEntry) []os.DirEntry {
		if dir != "big" {
			return entries
		}
		return append(entries, errDirEntry{name: "lost.mov", err: syscall.EIO})
	}
	t.Cleanup(func() { prefetchEntriesHook = nil })

	h.prefetchChildren("big")
	if kids, _ := store.ListChildren("big"); len(kids) != n {
		t.Fatalf("prefetch mirrored %d of %d children (test inconclusive)", len(kids), n)
	}
	if has, _ := store.HasDirListingIncomplete("big"); !has {
		t.Fatal("a prefetch that skipped a child cleared the folder's record")
	}
}
