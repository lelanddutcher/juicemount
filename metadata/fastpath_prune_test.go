package metadata

import (
	"os"
	"path/filepath"
	"testing"
)

// V2.3 G5 (task #73): the mass-delete fast-path must converge a deleted
// subtree in ONE cycle — double-confirmed (absent-in-Redis via pruneAbsent +
// FUSE-Lstat-ENOENT at the subtree root) — while sparing FUSE-present paths
// and `._` sidecars, and staying inert on degraded cycles / kill switch.

// harness: a RedisClient with a real temp-dir fuseRoot. Paths NOT created on
// disk read as Lstat-ENOENT (deleted); created ones read as present.
func fastPathHarness(t *testing.T, pruneAbsent map[string]int) *RedisClient {
	t.Helper()
	fuseRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(fuseRoot, "Assets", "Keep"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fuseRoot, "Assets", "Keep", "still.mov"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	return &RedisClient{fuseRoot: fuseRoot, pruneAbsent: pruneAbsent}
}

func TestFastPathPruneConvergesDeletedSubtree(t *testing.T) {
	rc := fastPathHarness(t, map[string]int{
		// Deleted subtree — counts far below PruneThreshold(10): the ladder
		// would need 10 clean cycles; the fast-path must confirm them NOW.
		"Assets/Gone":           1,
		"Assets/Gone/a.wav":     3,
		"Assets/Gone/sub/b.wav": 1,
		"Assets/Gone/._c.wav":   2, // AppleDouble — must be left alone
		// FUSE-present path (exists on disk) — must be left to the ladder.
		"Assets/Keep/still.mov": 2,
	})

	confirmed, capped := rc.collectFastPathPrunes(false)

	if capped {
		t.Fatal("tiny set must not hit any cap")
	}
	for _, want := range []string{"Assets/Gone", "Assets/Gone/a.wav", "Assets/Gone/sub/b.wav"} {
		if _, ok := confirmed[want]; !ok {
			t.Errorf("deleted path %q not fast-confirmed — ghost tree persists (the 125k pending_prune bug)", want)
		}
		if _, still := rc.pruneAbsent[want]; still {
			t.Errorf("confirmed path %q not removed from pruneAbsent", want)
		}
	}
	if _, ok := confirmed["Assets/Gone/._c.wav"]; ok {
		t.Error("._ AppleDouble sidecar fast-pruned — violates the scan-filter guard (had_shadow STALE class)")
	}
	if _, ok := confirmed["Assets/Keep/still.mov"]; ok {
		t.Error("FUSE-PRESENT path fast-pruned — this deletes live media from the mirror")
	}
	if _, still := rc.pruneAbsent["Assets/Keep/still.mov"]; !still {
		t.Error("FUSE-present path must stay on the ladder for future cycles")
	}
}

// Review fix: spool-pending paths must be excluded at COLLECTION time — a
// file mid-drain is absent from Redis AND absent from FUSE (spool-only), so
// without the guard the fast-path would confirm-and-prune it, Forgetting its
// live NFS handle (ESTALE mid-copy).
func TestFastPathPruneSparesSpoolPending(t *testing.T) {
	rc := fastPathHarness(t, map[string]int{
		"Assets/Gone":         1,
		"Assets/Gone/a.wav":   1,
		"Assets/draining.mov": 1, // spool-pending root — must not be probed/pruned
	})
	rc.SetSpoolGuard(func(p string) bool { return p == "Assets/draining.mov" })

	confirmed, _ := rc.collectFastPathPrunes(false)
	if _, ok := confirmed["Assets/draining.mov"]; ok {
		t.Fatal("spool-pending path fast-pruned — ESTALE/100070 mid-copy bug class")
	}
	if _, still := rc.pruneAbsent["Assets/draining.mov"]; !still {
		t.Fatal("spool-pending path must remain tracked for later cycles")
	}
	if _, ok := confirmed["Assets/Gone"]; !ok {
		t.Fatal("guard must not over-spare: genuinely deleted subtree still prunes")
	}
}

func TestFastPathPruneInertOnDegradedCycle(t *testing.T) {
	rc := fastPathHarness(t, map[string]int{"Assets/Gone": 1})
	confirmed, capped := rc.collectFastPathPrunes(true /* skipIncrement: RecentlyDegraded */)
	if len(confirmed) != 0 || capped {
		t.Fatalf("degraded cycle must not confirm deletions (got %d) — a partial Redis view is not a delete signal", len(confirmed))
	}
	if _, still := rc.pruneAbsent["Assets/Gone"]; !still {
		t.Fatal("degraded cycle must leave pruneAbsent untouched")
	}
}

func TestFastPathPruneKillSwitch(t *testing.T) {
	t.Setenv("JM_PRUNE_FASTPATH", "0")
	rc := fastPathHarness(t, map[string]int{"Assets/Gone": 1})
	if confirmed, _ := rc.collectFastPathPrunes(false); len(confirmed) != 0 {
		t.Fatalf("JM_PRUNE_FASTPATH=0 must disable the fast-path (got %d)", len(confirmed))
	}
}

func TestFastPathPruneInertWithoutFuseRoot(t *testing.T) {
	rc := &RedisClient{pruneAbsent: map[string]int{"Assets/Gone": 1}}
	if confirmed, _ := rc.collectFastPathPrunes(false); len(confirmed) != 0 {
		t.Fatalf("no fuseRoot → no FUSE authority → must not confirm (got %d)", len(confirmed))
	}
}
