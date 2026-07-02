package nfs

// U7 (V2.3) — async unmirrored-dir refresh. An ONLINE readdir of a directory
// with ZERO mirror rows must return the mirror's (empty) answer immediately
// and refresh the mirror from FUSE in the BACKGROUND, so the READDIR RPC
// never blocks on FUSE. These tests drive juiceFS.ReadDir (the layer the
// go-nfs fork calls) against a temp dir standing in for the FUSE mount, the
// same stand-in the spool integration tests use.

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
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

	if !pollUntil(t, 3*time.Second, mirrorHasChildren(store, "d")) {
		t.Fatal("async refresh never landed children in the mirror")
	}
	kids, _ := store.ListChildren("d")
	if len(kids) != 2 {
		t.Fatalf("mirror children = %d, want 2", len(kids))
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
