package metadata

import (
	"path/filepath"
	"testing"
	"time"
)

// Task #78: the keyspace-push insert paths must mirror the full SCAN's
// namespace exclusions (scanFilteredPath is the single source of truth), the
// pruneAbsent ladder must never track scan-filtered paths, and the one-time
// open-GC must remove pre-fix .trash/.juicemount mirror rows while sparing
// `._` sidecars and regular rows.

func TestScanFilteredPathTruthTable(t *testing.T) {
	cases := []struct {
		p    string
		want bool
	}{
		// JuiceFS built-in trash — filtered, root and descendants.
		{".trash", true},
		{".trash/2026-07-01-12", true},
		{".trash/2026-07-01-12/1-2-clip.mov", true},
		// Server-side derivative namespace — filtered.
		{".juicemount", true},
		{".juicemount/derivatives", true},
		{".juicemount/derivatives/12345/proxy.mp4", true},
		// Namespace match must be EXACT, not a raw prefix.
		{".trashcan/clip.mov", false},
		{".trash2", false},
		{".juicemount2/x", false},
		{".juicemountain", false},
		// Only VOLUME-ROOT-LEVEL namespaces: nested lookalikes are user paths.
		{"movies/.trash/clip.mov", false},
		{"a/.juicemount/x", false},
		// `._` AppleDouble sidecars are deliberately NOT push-filtered (they
		// are SCAN-visible once drained; see scanfilter.go).
		{"._sidecar", false},
		{"movies/._proxy.mov", false},
		// Regular paths.
		{"movies/clip.mov", false},
		{"", false},
		{".", false},
		{".Spotlight-V100", false}, // handler-hidden, but NOT scan-filtered
	}
	for _, tc := range cases {
		if got := scanFilteredPath(tc.p); got != tc.want {
			t.Errorf("scanFilteredPath(%q) = %v, want %v", tc.p, got, tc.want)
		}
	}
}

func TestApplyEventSkipsScanFilteredNamespaces(t *testing.T) {
	s := newTestStore(t)
	rc := &RedisClient{store: s}
	now := time.Now().Unix()

	// create into .trash → NOT mirrored.
	rc.applyEvent(MetadataEvent{Op: "create", Path: ".trash/2026-07-01-12/1-2-gone.mov", Inode: 42, Mtime: now})
	if s.LookupByPath(".trash/2026-07-01-12/1-2-gone.mov") != nil {
		t.Error("push-created .trash row was mirrored — SCAN can never diff it out (task #78 bug class)")
	}

	// create into .juicemount → NOT mirrored.
	rc.applyEvent(MetadataEvent{Op: "create", Path: ".juicemount/derivatives/9/proxy.mp4", Inode: 43, Mtime: now})
	if s.LookupByPath(".juicemount/derivatives/9/proxy.mp4") != nil {
		t.Error("push-created .juicemount row was mirrored — SCAN can never diff it out (task #78 bug class)")
	}

	// `._` sidecar create → STILL mirrored (push handling deliberately unchanged).
	rc.applyEvent(MetadataEvent{Op: "create", Path: "movies/._clip.mov", Inode: 44, Mtime: now})
	if s.LookupByPath("movies/._clip.mov") == nil {
		t.Error("._ sidecar push-create was filtered — ._ must NOT be in the push filter (served Mac-side)")
	}

	// regular create → mirrored.
	rc.applyEvent(MetadataEvent{Op: "create", Path: "movies/clip.mov", Inode: 45, Mtime: now})
	if s.LookupByPath("movies/clip.mov") == nil {
		t.Fatal("regular push-create was not mirrored")
	}

	// rename INTO .trash (JuiceFS delete-to-trash): old row dropped, no new row.
	rc.applyEvent(MetadataEvent{Op: "rename", OldPath: "movies/clip.mov", Path: ".trash/2026-07-01-12/1-45-clip.mov", Inode: 45, Mtime: now})
	if s.LookupByPath("movies/clip.mov") != nil {
		t.Error("rename-into-.trash left the source row behind — delete-to-trash must still drop it")
	}
	if s.LookupByPath(".trash/2026-07-01-12/1-45-clip.mov") != nil {
		t.Error("rename-into-.trash mirrored the trash destination row")
	}

	// rename OUT of .trash (restore): destination is mirrored normally.
	rc.applyEvent(MetadataEvent{Op: "rename", OldPath: ".trash/2026-07-01-12/1-45-clip.mov", Path: "movies/restored.mov", Inode: 45, Mtime: now})
	if s.LookupByPath("movies/restored.mov") == nil {
		t.Error("trash-restore rename destination was not mirrored")
	}

	// delete of an internal-namespace path is convergent (no error, no row).
	rc.applyEvent(MetadataEvent{Op: "delete", Path: ".trash/2026-07-01-12/1-2-gone.mov"})
	if s.LookupByPath(".trash/2026-07-01-12/1-2-gone.mov") != nil {
		t.Error("delete of internal-namespace path left a row")
	}
}

// reconcileDir must refuse to reconcile a directory inside a scan-filtered
// namespace BEFORE any Redis round-trip. The harness client has NO Redis
// connection: if the filter fails, reconcileDir dereferences a nil
// *redis.Client and the test fails loudly.
func TestReconcileDirSkipsFilteredNamespaceDirs(t *testing.T) {
	s := newTestStore(t)
	rc := &RedisClient{store: s}

	// Seed the dir rows a pre-fix build would have mirrored.
	for inode, p := range map[uint64]string{
		777: ".trash/2026-07-01-12",
		778: ".juicemount/derivatives",
	} {
		if err := s.Insert(MakeEntry(p, true, 0, time.Now(), inode)); err != nil {
			t.Fatal(err)
		}
	}

	if err := rc.reconcileDir(777); err != nil {
		t.Fatalf("reconcileDir(.trash dir) = %v, want nil skip", err)
	}
	if err := rc.reconcileDir(778); err != nil {
		t.Fatalf("reconcileDir(.juicemount dir) = %v, want nil skip", err)
	}
}

func TestTrackAbsentPathsNeverTracksFilteredNamespaces(t *testing.T) {
	set := func(paths ...string) map[string]struct{} {
		m := make(map[string]struct{}, len(paths))
		for _, p := range paths {
			m[p] = struct{}{}
		}
		return m
	}

	rc := &RedisClient{pruneAbsent: map[string]int{
		// Stale counter for a filtered path (as a pre-fix build accumulated) —
		// must be actively dropped even though the row still exists in SQLite.
		".trash/stale-counter": 7,
	}}
	existing := set(
		".trash/x", ".trash/stale-counter",
		".juicemount/derivatives/1/proxy.mp4",
		"movies/gone.mov", "movies/here.mov",
		"movies/._gone-sidecar", // ._ tracking unchanged: ladder guard handles it later
	)
	inRedis := set("movies/here.mov")

	rc.trackAbsentPaths(existing, inRedis, false)

	for _, p := range []string{".trash/x", ".juicemount/derivatives/1/proxy.mp4", ".trash/stale-counter"} {
		if _, ok := rc.pruneAbsent[p]; ok {
			t.Errorf("filtered-namespace path %q tracked in pruneAbsent — permanent pending_prune floor (task #78)", p)
		}
	}
	if rc.pruneAbsent["movies/gone.mov"] != 1 {
		t.Errorf("genuinely-absent path not tracked: count = %d, want 1", rc.pruneAbsent["movies/gone.mov"])
	}
	if rc.pruneAbsent["movies/._gone-sidecar"] != 1 {
		t.Errorf("._ sidecar tracking changed: count = %d, want 1 (guard lives at qualification, not tracking)", rc.pruneAbsent["movies/._gone-sidecar"])
	}
	if _, ok := rc.pruneAbsent["movies/here.mov"]; ok {
		t.Error("Redis-present path left on the ladder")
	}

	// skipIncrement (RecentlyDegraded) cycles must not advance ANY counter.
	rc2 := &RedisClient{pruneAbsent: map[string]int{}}
	rc2.trackAbsentPaths(existing, inRedis, true)
	if len(rc2.pruneAbsent) != 0 {
		t.Errorf("degraded cycle advanced counters: %v", rc2.pruneAbsent)
	}
}

// seedGCFixture writes one row per class into a file-backed store and closes
// it, returning the db path for re-Open (the GC runs at Open).
func seedGCFixture(t *testing.T) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "metadata.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open(seed): %v", err)
	}
	now := time.Now()
	for inode, row := range map[uint64]struct {
		path  string
		isDir bool
	}{
		101: {".trash", true},                            // bare namespace dir — GC spares (children only)
		102: {".trash/2026-07-01-12/1-2-a.mov", false},   // GC target
		103: {".juicemount", true},                       // bare namespace dir — spared
		104: {".juicemount/derivatives/9/p.mp4", false},  // GC target
		105: {"movies/._sidecar", false},                 // ._ row — MUST survive
		106: {"._rootsidecar", false},                    // root-level ._ row — MUST survive
		107: {"movies/clip.mov", false},                  // regular row — MUST survive
		108: {"movies/.trash-lookalike/keep.mov", false}, // nested lookalike — MUST survive
	} {
		if err := s.Insert(MakeEntry(row.path, row.isDir, 1, now, inode)); err != nil {
			t.Fatalf("seed insert %q: %v", row.path, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close(seed): %v", err)
	}
	return dbPath
}

func TestMirrorNamespaceGC(t *testing.T) {
	dbPath := seedGCFixture(t)

	s, err := Open(dbPath) // GC fires here
	if err != nil {
		t.Fatalf("Open(gc): %v", err)
	}
	defer s.Close()

	for _, gone := range []string{
		".trash/2026-07-01-12/1-2-a.mov",
		".juicemount/derivatives/9/p.mp4",
	} {
		if s.LookupByPath(gone) != nil {
			t.Errorf("GC left internal-namespace row %q", gone)
		}
	}
	for _, kept := range []string{
		".trash",      // bare dir rows spared by design
		".juicemount", // bare dir rows spared by design
		"movies/._sidecar",
		"._rootsidecar",
		"movies/clip.mov",
		"movies/.trash-lookalike/keep.mov",
	} {
		if s.LookupByPath(kept) == nil {
			t.Errorf("GC removed row %q it must spare", kept)
		}
	}
	count, err := s.Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != 6 {
		t.Errorf("post-GC Count = %d, want 6", count)
	}
}

func TestMirrorNamespaceGCKillSwitch(t *testing.T) {
	dbPath := seedGCFixture(t)
	t.Setenv("JM_MIRROR_NS_GC", "0")

	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open(kill switch): %v", err)
	}
	defer s.Close()

	for _, p := range []string{
		".trash/2026-07-01-12/1-2-a.mov",
		".juicemount/derivatives/9/p.mp4",
	} {
		if s.LookupByPath(p) == nil {
			t.Errorf("JM_MIRROR_NS_GC=0 did not disable the GC: row %q removed", p)
		}
	}
}
