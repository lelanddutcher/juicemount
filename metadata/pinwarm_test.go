package metadata

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// openTestStore builds a fresh file-backed Store (not :memory:, which shares a
// single DB process-wide via cache=shared) for pin-warm tests.
func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func insertDir(t *testing.T, s *Store, p string, inode uint64) {
	t.Helper()
	e := MakeEntry(p, true, 0, time.Unix(1700000000, 0), inode)
	s.InsertToCache(e)
	if err := s.Insert(e); err != nil {
		t.Fatalf("Insert %q: %v", p, err)
	}
}

// ---------------------------------------------------------------------------
// Kill switch: JM_PIN_WARM_METADATA default ON; =0 OFF; anything else ON.
// ---------------------------------------------------------------------------

func TestPinWarmMetadataEnabledKillSwitch(t *testing.T) {
	t.Setenv("JM_PIN_WARM_METADATA", "")
	if !PinWarmMetadataEnabled() {
		t.Error("unset env must be ENABLED (default on)")
	}
	t.Setenv("JM_PIN_WARM_METADATA", "1")
	if !PinWarmMetadataEnabled() {
		t.Error("=1 must be ENABLED")
	}
	t.Setenv("JM_PIN_WARM_METADATA", "yes")
	if !PinWarmMetadataEnabled() {
		t.Error("non-'0' value must be ENABLED")
	}
	t.Setenv("JM_PIN_WARM_METADATA", "0")
	if PinWarmMetadataEnabled() {
		t.Error("=0 must be DISABLED (today's behavior)")
	}
}

// ---------------------------------------------------------------------------
// ReconcileAncestors walks the parent chain root-first (root inode 1, then each
// resolved ancestor dir). Uses the testReconcileDir seam so no Redis is needed.
// ---------------------------------------------------------------------------

func TestReconcileAncestorsWalksChain(t *testing.T) {
	t.Setenv("JM_PIN_WARM_METADATA", "")
	pin.SetOffline(false)

	s := openTestStore(t)
	// Pre-populate the ancestor dir chain so LookupByPath resolves each inode
	// (the fake reconcileDir doesn't mutate the store).
	insertDir(t, s, "movies", 10)
	insertDir(t, s, "movies/2026", 20)
	insertDir(t, s, "movies/2026/reel", 30)

	var got []uint64
	rc := &RedisClient{store: s}
	rc.testReconcileDir = func(inode uint64) error {
		got = append(got, inode)
		return nil
	}

	if err := rc.ReconcileAncestors("movies/2026/reel/clip.mov"); err != nil {
		t.Fatalf("ReconcileAncestors: %v", err)
	}

	// Root (1) first, then movies(10), movies/2026(20), movies/2026/reel(30).
	want := []uint64{1, 10, 20, 30}
	if len(got) != len(want) {
		t.Fatalf("reconciled inodes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("reconcile order = %v, want %v (root-first, chain)", got, want)
		}
	}
}

// A top-level pinned file has only the root as its ancestor.
func TestReconcileAncestorsTopLevel(t *testing.T) {
	t.Setenv("JM_PIN_WARM_METADATA", "")
	pin.SetOffline(false)

	s := openTestStore(t)
	var got []uint64
	rc := &RedisClient{store: s}
	rc.testReconcileDir = func(inode uint64) error { got = append(got, inode); return nil }

	if err := rc.ReconcileAncestors("notes.txt"); err != nil {
		t.Fatalf("ReconcileAncestors: %v", err)
	}
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("top-level ancestors = %v, want [1] (root only)", got)
	}
}

// ---------------------------------------------------------------------------
// ScanFilteredPath guard: a .trash / .juicemount pin root is refused (would be
// silently #78-no-op'd by reconcileDir). Both entry points reject.
// ---------------------------------------------------------------------------

func TestReconcileAncestorsScanFilteredGuard(t *testing.T) {
	t.Setenv("JM_PIN_WARM_METADATA", "")
	pin.SetOffline(false)

	s := openTestStore(t)
	var called int
	rc := &RedisClient{store: s}
	rc.testReconcileDir = func(uint64) error { called++; return nil }

	for _, p := range []string{".trash", ".trash/x", ".juicemount", ".juicemount/derivatives/y"} {
		err := rc.ReconcileAncestors(p)
		if !errors.Is(err, errPinInternalNamespace) {
			t.Errorf("ReconcileAncestors(%q) err = %v, want errPinInternalNamespace", p, err)
		}
	}
	if called != 0 {
		t.Errorf("scan-filtered ancestors must not reconcile any dir, got %d calls", called)
	}
}

func TestReconcileSubtreeScanFilteredGuard(t *testing.T) {
	t.Setenv("JM_PIN_WARM_METADATA", "")
	pin.SetOffline(false)

	s := openTestStore(t)
	// The pinned root inode resolves to a .trash path — must be refused.
	insertDir(t, s, ".trash", 99)

	var called int
	rc := &RedisClient{store: s}
	rc.testReconcileDir = func(uint64) error { called++; return nil }
	rc.testChildDirInodes = func(uint64) ([]uint64, error) { return nil, nil }

	err := rc.ReconcileSubtree(99, 0)
	if !errors.Is(err, errPinInternalNamespace) {
		t.Errorf("ReconcileSubtree(scan-filtered root) err = %v, want errPinInternalNamespace", err)
	}
	if called != 0 {
		t.Errorf("scan-filtered subtree must not reconcile any dir, got %d calls", called)
	}
}

// ---------------------------------------------------------------------------
// Offline gate: when pin.IsOffline(), both entry points skip entirely (no
// reconcile) — an offline reconcileDir would burn a 30s Redis timeout per dir.
// ---------------------------------------------------------------------------

func TestPinWarmOfflineGateSkips(t *testing.T) {
	t.Setenv("JM_PIN_WARM_METADATA", "")
	pin.SetOffline(true)
	t.Cleanup(func() { pin.SetOffline(false) })

	s := openTestStore(t)
	insertDir(t, s, "movies", 10)

	var called int
	rc := &RedisClient{store: s}
	rc.testReconcileDir = func(uint64) error { called++; return nil }
	rc.testChildDirInodes = func(uint64) ([]uint64, error) { called++; return nil, nil }

	if err := rc.ReconcileAncestors("movies/clip.mov"); err != nil {
		t.Fatalf("ReconcileAncestors(offline): %v", err)
	}
	if err := rc.ReconcileSubtree(10, 0); err != nil {
		t.Fatalf("ReconcileSubtree(offline): %v", err)
	}
	if called != 0 {
		t.Errorf("offline gate must skip all reconcile work, got %d calls", called)
	}
}

// ---------------------------------------------------------------------------
// Kill switch OFF: JM_PIN_WARM_METADATA=0 -> both entry points no-op.
// ---------------------------------------------------------------------------

func TestPinWarmKillSwitchOffSkips(t *testing.T) {
	t.Setenv("JM_PIN_WARM_METADATA", "0")
	pin.SetOffline(false)

	s := openTestStore(t)
	insertDir(t, s, "movies", 10)

	var called int
	rc := &RedisClient{store: s}
	rc.testReconcileDir = func(uint64) error { called++; return nil }
	rc.testChildDirInodes = func(uint64) ([]uint64, error) { called++; return nil, nil }

	if err := rc.ReconcileAncestors("movies/clip.mov"); err != nil {
		t.Fatalf("ReconcileAncestors(killswitch off): %v", err)
	}
	if err := rc.ReconcileSubtree(10, 0); err != nil {
		t.Fatalf("ReconcileSubtree(killswitch off): %v", err)
	}
	if called != 0 {
		t.Errorf("kill switch off must skip all reconcile work, got %d calls", called)
	}
}

// ---------------------------------------------------------------------------
// ReconcileSubtree walk: BFS over a fake dir tree via testChildDirInodes; each
// discovered dir is reconciled exactly once (dedup), and the cap truncates.
// ---------------------------------------------------------------------------

func TestReconcileSubtreeWalksAndDedups(t *testing.T) {
	t.Setenv("JM_PIN_WARM_METADATA", "")
	pin.SetOffline(false)

	// Fake tree (inode -> child dir inodes):
	//   1 -> {2, 3}
	//   2 -> {4}
	//   3 -> {4}          (4 is reachable via TWO parents — must reconcile once)
	//   4 -> {}
	children := map[uint64][]uint64{
		1: {2, 3},
		2: {4},
		3: {4},
		4: {},
	}
	reconciled := map[uint64]int{}

	rc := &RedisClient{store: openTestStore(t)}
	rc.testReconcileDir = func(inode uint64) error { reconciled[inode]++; return nil }
	rc.testChildDirInodes = func(inode uint64) ([]uint64, error) { return children[inode], nil }

	if err := rc.ReconcileSubtree(1, 0); err != nil {
		t.Fatalf("ReconcileSubtree: %v", err)
	}

	for _, in := range []uint64{1, 2, 3, 4} {
		if reconciled[in] != 1 {
			t.Errorf("dir %d reconciled %d times, want exactly 1 (dedup)", in, reconciled[in])
		}
	}
	if len(reconciled) != 4 {
		t.Errorf("reconciled %d distinct dirs, want 4", len(reconciled))
	}
}

func TestReconcileSubtreeBoundedByCap(t *testing.T) {
	t.Setenv("JM_PIN_WARM_METADATA", "")
	pin.SetOffline(false)

	// A long chain 1 -> 2 -> 3 -> ... where every dir has exactly one child dir.
	children := func(inode uint64) ([]uint64, error) {
		return []uint64{inode + 1}, nil // unbounded chain
	}
	var count int
	rc := &RedisClient{store: openTestStore(t)}
	rc.testReconcileDir = func(uint64) error { count++; return nil }
	rc.testChildDirInodes = children

	const maxDirs = 5
	if err := rc.ReconcileSubtree(1, maxDirs); err != nil {
		t.Fatalf("ReconcileSubtree: %v", err)
	}
	if count != maxDirs {
		t.Errorf("reconcile count = %d, want exactly maxDirs=%d (bounded walk)", count, maxDirs)
	}
}
