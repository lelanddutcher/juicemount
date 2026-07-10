package metadata

import (
	"fmt"
	"path"
	"path/filepath"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// INSTANT-NAV #2: subtree-size aggregation tests.
//
// The invariant under test: for every dir P, subtreeBytes[P]/subtreeFiles[P]
// equal the sum/count over all non-dir entries currently in pathCache at any
// depth under P; dirs contribute 0; keys with both aggregates at exactly 0 are
// absent. expectedSubtreeAggregates recomputes that ground truth from
// pathCache with an independent walk so every test can assert full-map
// equality after arbitrary mutation sequences.
// ---------------------------------------------------------------------------

// subtreeTestStore opens a store with the gate explicitly ON (the default),
// pinned via t.Setenv so an inherited JM_SUBTREE_SIZES=0 can't skew the run.
func subtreeTestStore(t *testing.T) *Store {
	t.Helper()
	t.Setenv("JM_SUBTREE_SIZES", "1")
	return newTestStore(t)
}

// expectedSubtreeAggregates independently recomputes the ground-truth
// aggregates from the CURRENT pathCache (same semantics, independent code
// path from applySubtreeDeltaLocked's incremental maintenance).
func expectedSubtreeAggregates(s *Store) (map[string]int64, map[string]int64) {
	wantBytes := map[string]int64{}
	wantFiles := map[string]int64{}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, e := range s.pathCache {
		if e.IsDir {
			continue
		}
		p := e.ParentPath
		if p == "" {
			p = "."
		}
		for {
			wantBytes[p] += e.Size
			wantFiles[p]++
			if p == "." || p == "/" {
				break
			}
			np := path.Dir(p)
			if np == p {
				break
			}
			p = np
		}
	}
	return wantBytes, wantFiles
}

// assertSubtreeConsistent compares the live incrementally-maintained maps
// against the independent recompute, key by key in both directions.
func assertSubtreeConsistent(t *testing.T, s *Store) {
	t.Helper()
	wantBytes, wantFiles := expectedSubtreeAggregates(s)
	s.mu.RLock()
	defer s.mu.RUnlock()
	for p, wb := range wantBytes {
		if gb := s.subtreeBytes[p]; gb != wb {
			t.Errorf("subtreeBytes[%q] = %d, want %d", p, gb, wb)
		}
		if gf := s.subtreeFiles[p]; gf != wantFiles[p] {
			t.Errorf("subtreeFiles[%q] = %d, want %d", p, gf, wantFiles[p])
		}
	}
	for p := range s.subtreeBytes {
		if _, ok := wantBytes[p]; !ok {
			t.Errorf("subtreeBytes has stale key %q = %d (want absent)", p, s.subtreeBytes[p])
		}
	}
	for p := range s.subtreeFiles {
		if _, ok := wantFiles[p]; !ok {
			t.Errorf("subtreeFiles has stale key %q = %d (want absent)", p, s.subtreeFiles[p])
		}
	}
}

// mustSubtree asserts SubtreeSize(dir) returns exactly (bytes, files) with ok.
func mustSubtree(t *testing.T, s *Store, dir string, bytes, files int64) {
	t.Helper()
	gb, gf, ok := s.SubtreeSize(dir)
	if !ok {
		t.Fatalf("SubtreeSize(%q): ok=false, want true", dir)
	}
	if gb != bytes || gf != files {
		t.Fatalf("SubtreeSize(%q) = (%d bytes, %d files), want (%d, %d)", dir, gb, gf, bytes, files)
	}
}

func TestSubtreeInsertUpdatesAncestors(t *testing.T) {
	s := subtreeTestStore(t)
	now := time.Now()

	// Dirs first (contribute nothing themselves).
	for i, d := range []string{"a", "a/b", "a/b/c"} {
		if err := s.Insert(MakeEntry(d, true, 0, now, uint64(1+i))); err != nil {
			t.Fatal(err)
		}
	}
	mustSubtree(t, s, "a", 0, 0)

	if err := s.Insert(MakeEntry("a/b/c/clip.mov", false, 1000, now, 10)); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(MakeEntry("a/b/other.wav", false, 50, now, 11)); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(MakeEntry("rootfile.txt", false, 7, now, 12)); err != nil {
		t.Fatal(err)
	}

	mustSubtree(t, s, "a/b/c", 1000, 1)
	mustSubtree(t, s, "a/b", 1050, 2)
	mustSubtree(t, s, "a", 1050, 2)
	mustSubtree(t, s, ".", 1057, 3) // root sees everything incl. the top-level file
	mustSubtree(t, s, "", 1057, 3)  // "" normalizes to root
	mustSubtree(t, s, "a/b/c/unknown", 0, 0)
	assertSubtreeConsistent(t, s)

	// InsertToCache (RAM-only path) must maintain the aggregates identically.
	s.InsertToCache(MakeEntry("a/b/c/cache-only.bin", false, 200, now, 13))
	mustSubtree(t, s, "a/b/c", 1200, 2)
	mustSubtree(t, s, ".", 1257, 4)
	assertSubtreeConsistent(t, s)
}

func TestSubtreeSizeChangeUpsertAppliesDelta(t *testing.T) {
	s := subtreeTestStore(t)
	now := time.Now()

	if err := s.Insert(MakeEntry("proj", true, 0, now, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(MakeEntry("proj/export.mp4", false, 100, now, 2)); err != nil {
		t.Fatal(err)
	}
	mustSubtree(t, s, "proj", 100, 1)

	// Re-upsert the same path with a bigger size: (new−old), NOT double-count.
	if err := s.Insert(MakeEntry("proj/export.mp4", false, 175, now, 2)); err != nil {
		t.Fatal(err)
	}
	mustSubtree(t, s, "proj", 175, 1)
	mustSubtree(t, s, ".", 175, 1)

	// And a shrink upsert (reconcile publishing a corrected size) subtracts.
	if err := s.Insert(MakeEntry("proj/export.mp4", false, 60, now, 2)); err != nil {
		t.Fatal(err)
	}
	mustSubtree(t, s, "proj", 60, 1)

	// File replaced by a DIRECTORY at the same path: contribution fully undone.
	if err := s.Insert(MakeEntry("proj/export.mp4", true, 0, now, 3)); err != nil {
		t.Fatal(err)
	}
	mustSubtree(t, s, "proj", 0, 0)
	mustSubtree(t, s, ".", 0, 0)
	assertSubtreeConsistent(t, s)
}

func TestSubtreeUpdateSizeAppliesDelta(t *testing.T) {
	s := subtreeTestStore(t)
	now := time.Now()

	if err := s.Insert(MakeEntry("cam", true, 0, now, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(MakeEntry("cam/A001.mov", false, 1_000, now, 2)); err != nil {
		t.Fatal(err)
	}

	// Grow (the NFS write-close high-water publish).
	if err := s.UpdateSize("cam/A001.mov", 5_000, now); err != nil {
		t.Fatal(err)
	}
	mustSubtree(t, s, "cam", 5_000, 1)

	// Shrink attempt: UpdateSize has MAX semantics — no cache change, no delta.
	if err := s.UpdateSize("cam/A001.mov", 3_000, now); err != nil {
		t.Fatal(err)
	}
	mustSubtree(t, s, "cam", 5_000, 1)

	// Unknown path: no-op, no aggregate churn.
	if err := s.UpdateSize("cam/nope.mov", 9_999, now); err != nil {
		t.Fatal(err)
	}
	mustSubtree(t, s, "cam", 5_000, 1)
	assertSubtreeConsistent(t, s)
}

func TestSubtreeDeleteAndPruneSubtract(t *testing.T) {
	s := subtreeTestStore(t)
	now := time.Now()

	for i, d := range []string{"x", "x/y"} {
		if err := s.Insert(MakeEntry(d, true, 0, now, uint64(1+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Insert(MakeEntry("x/y/a.bin", false, 10, now, 10)); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(MakeEntry("x/y/b.bin", false, 20, now, 11)); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(MakeEntry("x/c.bin", false, 40, now, 12)); err != nil {
		t.Fatal(err)
	}
	mustSubtree(t, s, "x", 70, 3)

	// Delete (SQLite + cache).
	if err := s.Delete("x/y/a.bin"); err != nil {
		t.Fatal(err)
	}
	mustSubtree(t, s, "x/y", 20, 1)
	mustSubtree(t, s, "x", 60, 2)

	// DeleteFromCache (RAM-only).
	s.DeleteFromCache("x/y/b.bin")
	mustSubtree(t, s, "x/y", 0, 0)
	mustSubtree(t, s, "x", 40, 1)

	// The emptied dir's aggregate keys must be GONE (bounded maps), while the
	// dir entry itself still exists.
	s.mu.RLock()
	if _, ok := s.subtreeBytes["x/y"]; ok {
		t.Error("subtreeBytes[\"x/y\"] should be deleted at exact zero")
	}
	if _, ok := s.subtreeFiles["x/y"]; ok {
		t.Error("subtreeFiles[\"x/y\"] should be deleted at exact zero")
	}
	s.mu.RUnlock()
	if e := s.LookupByPath("x/y"); e == nil || !e.IsDir {
		t.Fatal("dir entry x/y should still exist")
	}

	// DeletePaths (the reconcile/keyspace prune path).
	if err := s.DeletePaths([]string{"x/c.bin", "x/never-existed.bin"}); err != nil {
		t.Fatal(err)
	}
	mustSubtree(t, s, "x", 0, 0)
	mustSubtree(t, s, ".", 0, 0)
	assertSubtreeConsistent(t, s)
}

func TestSubtreeDeepChain(t *testing.T) {
	s := subtreeTestStore(t)
	now := time.Now()

	// 6-level directory chain.
	chain := "l1/l2/l3/l4/l5/l6"
	parts := []string{"l1", "l1/l2", "l1/l2/l3", "l1/l2/l3/l4", "l1/l2/l3/l4/l5", chain}
	for i, d := range parts {
		if err := s.Insert(MakeEntry(d, true, 0, now, uint64(1+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Insert(MakeEntry(chain+"/deep.raw", false, 12345, now, 100)); err != nil {
		t.Fatal(err)
	}

	// EVERY ancestor on the chain (and the root) sees the file exactly once.
	for _, d := range append([]string{"."}, parts...) {
		mustSubtree(t, s, d, 12345, 1)
	}

	if err := s.Delete(chain + "/deep.raw"); err != nil {
		t.Fatal(err)
	}
	for _, d := range append([]string{"."}, parts...) {
		mustSubtree(t, s, d, 0, 0)
	}
	assertSubtreeConsistent(t, s)
}

func TestSubtreeBulkInsertPaths(t *testing.T) {
	s := subtreeTestStore(t)
	now := time.Now()

	batch := []*Entry{
		MakeEntry("bulk", true, 0, now, 1),
		MakeEntry("bulk/sub", true, 0, now, 2),
		MakeEntry("bulk/sub/f1.mov", false, 100, now, 10),
		MakeEntry("bulk/sub/f2.mov", false, 200, now, 11),
		MakeEntry("bulk/f3.mov", false, 400, now, 12),
	}
	if err := s.BulkInsert(batch, 2); err != nil { // batchSize 2 forces multi-batch
		t.Fatal(err)
	}
	mustSubtree(t, s, "bulk/sub", 300, 2)
	mustSubtree(t, s, "bulk", 700, 3)
	mustSubtree(t, s, ".", 700, 3)

	// Bulk RE-upsert with changed sizes: per-entry (new−old), no double-count.
	rebatch := []*Entry{
		MakeEntry("bulk/sub/f1.mov", false, 150, now, 10),
		MakeEntry("bulk/f3.mov", false, 50, now, 12),
	}
	if err := s.BulkInsert(rebatch, 500); err != nil {
		t.Fatal(err)
	}
	mustSubtree(t, s, "bulk/sub", 350, 2)
	mustSubtree(t, s, "bulk", 400, 3)

	// BulkInsertAbsent: present rows are SKIPPED (no aggregate change),
	// genuinely-new rows are added.
	absent := []*Entry{
		MakeEntry("bulk/sub/f1.mov", false, 999_999, now, 10), // present — must be ignored
		MakeEntry("bulk/sub/f4.mov", false, 25, now, 13),      // new
	}
	if err := s.BulkInsertAbsent(absent, 500); err != nil {
		t.Fatal(err)
	}
	mustSubtree(t, s, "bulk/sub", 375, 3)
	mustSubtree(t, s, "bulk", 425, 4)
	assertSubtreeConsistent(t, s)
}

// TestSubtreeRenameOrphanSweep covers evictInodeOrphanLocked: juicefs
// preserves inode across rename, so inserting the new path with the same
// inode sweeps the stale old path out of pathCache — the aggregates must
// move the contribution, not duplicate it.
func TestSubtreeRenameOrphanSweep(t *testing.T) {
	s := subtreeTestStore(t)
	now := time.Now()

	for i, d := range []string{"src", "dst"} {
		if err := s.Insert(MakeEntry(d, true, 0, now, uint64(1+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Insert(MakeEntry("src/take1.mov", false, 500, now, 42)); err != nil {
		t.Fatal(err)
	}
	mustSubtree(t, s, "src", 500, 1)

	// Rename lands as an insert of the NEW path with the SAME inode.
	if err := s.Insert(MakeEntry("dst/take1.mov", false, 500, now, 42)); err != nil {
		t.Fatal(err)
	}
	mustSubtree(t, s, "src", 0, 0)
	mustSubtree(t, s, "dst", 500, 1)
	mustSubtree(t, s, ".", 500, 1)
	if e := s.LookupByPath("src/take1.mov"); e != nil {
		t.Fatal("stale old path should have been swept")
	}
	assertSubtreeConsistent(t, s)
}

// TestSubtreeRebuildPass wipes the live aggregates and recomputes them from
// pathCache in one pass — the boot/full-rebuild code path — then compares
// against both the pre-wipe values and the independent ground truth.
func TestSubtreeRebuildPass(t *testing.T) {
	s := subtreeTestStore(t)
	now := time.Now()

	ents := []*Entry{
		MakeEntry("r", true, 0, now, 1),
		MakeEntry("r/s", true, 0, now, 2),
		MakeEntry("r/s/t", true, 0, now, 3),
		MakeEntry("r/s/t/a.bin", false, 11, now, 10),
		MakeEntry("r/s/b.bin", false, 22, now, 11),
		MakeEntry("r/c.bin", false, 44, now, 12),
		MakeEntry("top.bin", false, 88, now, 13),
	}
	if err := s.BulkInsert(ents, 500); err != nil {
		t.Fatal(err)
	}

	// Snapshot live values, wipe, recompute, compare.
	beforeB, beforeF := expectedSubtreeAggregates(s)
	s.mu.Lock()
	s.subtreeBytes = map[string]int64{"poison": 1} // wipe + poison: rebuild must fully replace
	s.subtreeFiles = map[string]int64{"poison": 1}
	s.recomputeSubtreeAggregatesLocked()
	s.mu.Unlock()

	assertSubtreeConsistent(t, s)
	for p, wb := range beforeB {
		mustSubtree(t, s, p, wb, beforeF[p])
	}
	if _, _, ok := s.SubtreeSize("poison"); !ok {
		t.Fatal("gate should still be on")
	}
	if b, f, _ := s.SubtreeSize("poison"); b != 0 || f != 0 {
		t.Fatal("poison key should have been dropped by the rebuild")
	}
}

// TestSubtreeBootRebuild proves a fresh Open over an existing mirror DB
// recomputes the aggregates from the SQLite load (rebuildCaches path).
func TestSubtreeBootRebuild(t *testing.T) {
	t.Setenv("JM_SUBTREE_SIZES", "1")
	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	now := time.Now().Truncate(time.Second)

	s1, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i, d := range []string{"boot", "boot/deep"} {
		if err := s1.Insert(MakeEntry(d, true, 0, now, uint64(1+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := s1.Insert(MakeEntry("boot/deep/x.mov", false, 640, now, 10)); err != nil {
		t.Fatal(err)
	}
	if err := s1.Insert(MakeEntry("boot/y.mov", false, 60, now, 11)); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { s2.Close() })
	mustSubtree(t, s2, "boot/deep", 640, 1)
	mustSubtree(t, s2, "boot", 700, 2)
	mustSubtree(t, s2, ".", 700, 2)
	assertSubtreeConsistent(t, s2)
}

// TestSubtreeGateOff: JM_SUBTREE_SIZES=0 must skip ALL maintenance — the maps
// stay nil (zero map writes on any mutation path) and SubtreeSize reports
// ok=false. This is the byte-identical-off-path guarantee.
func TestSubtreeGateOff(t *testing.T) {
	t.Setenv("JM_SUBTREE_SIZES", "0")
	s := newTestStore(t)
	now := time.Now()

	if err := s.Insert(MakeEntry("off", true, 0, now, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(MakeEntry("off/f.bin", false, 123, now, 2)); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateSize("off/f.bin", 456, now); err != nil {
		t.Fatal(err)
	}
	if err := s.BulkInsert([]*Entry{MakeEntry("off/g.bin", false, 9, now, 3)}, 500); err != nil {
		t.Fatal(err)
	}
	s.InsertToCache(MakeEntry("off/h.bin", false, 4, now, 4))
	s.DeleteFromCache("off/h.bin")
	if err := s.Delete("off/g.bin"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeletePaths([]string{"off/f.bin"}); err != nil {
		t.Fatal(err)
	}

	if _, _, ok := s.SubtreeSize("off"); ok {
		t.Fatal("SubtreeSize ok=true with gate off")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.subtreeBytes != nil || s.subtreeFiles != nil {
		t.Fatalf("aggregate maps must stay nil with gate off (got %v / %v)",
			s.subtreeBytes, s.subtreeFiles)
	}
}

// TestSubtreeEvictionSubtracts: evictOldest drops LRU files under memory
// pressure (never in production at ~300k vs 500k); the aggregates track the
// RAM mirror, so dropped entries must be subtracted and a later re-insert
// re-adds exactly once.
func TestSubtreeEvictionSubtracts(t *testing.T) {
	t.Setenv("JM_SUBTREE_SIZES", "1")
	s, err := OpenWithMaxCacheSize(":memory:", 4) // tiny budget forces eviction
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	base := time.Now().Add(-time.Hour)

	ents := []*Entry{MakeEntry("ev", true, 0, base, 1)}
	for i := 0; i < 8; i++ {
		ents = append(ents, MakeEntry(fmt.Sprintf("ev/f%02d.bin", i), false, 100, base.Add(time.Duration(i)*time.Minute), uint64(10+i)))
	}
	if err := s.BulkInsert(ents, 500); err != nil {
		t.Fatal(err)
	}

	// Budget 4 = 1 dir (always kept) + 3 newest files.
	pcs, _ := s.CacheStats()
	if pcs != 4 {
		t.Fatalf("pathCache size = %d, want 4 (eviction should have fired)", pcs)
	}
	mustSubtree(t, s, "ev", 300, 3)
	assertSubtreeConsistent(t, s)
}

// ---------------------------------------------------------------------------
// Boot-rebuild pass timing at a ~300k-entry synthetic tree (INSTANT-NAV #2
// bound: the one O(N×depth) recompute pass must stay <1s).
// ---------------------------------------------------------------------------

// buildSubtreeBenchTree populates the store's RAM caches with a ~304k-entry
// tree: 8×5×5×5×3 = 3000 leaf dirs (chain depth 5 + root) × 100 files each =
// 300,000 files + 4,248 dirs. InsertToCache only (no SQLite) — this shapes
// pathCache for timing the recompute pass itself.
func buildSubtreeBenchTree(tb testing.TB, s *Store) int {
	tb.Helper()
	now := time.Now()
	inode := uint64(1)
	total := 0
	insert := func(p string, isDir bool, size int64) {
		s.InsertToCache(MakeEntry(p, isDir, size, now, inode))
		inode++
		total++
	}
	for a := 0; a < 8; a++ {
		pa := fmt.Sprintf("v%d", a)
		insert(pa, true, 0)
		for b := 0; b < 5; b++ {
			pb := fmt.Sprintf("%s/proj%d", pa, b)
			insert(pb, true, 0)
			for c := 0; c < 5; c++ {
				pc := fmt.Sprintf("%s/scene%d", pb, c)
				insert(pc, true, 0)
				for d := 0; d < 5; d++ {
					pd := fmt.Sprintf("%s/day%d", pc, d)
					insert(pd, true, 0)
					for e := 0; e < 3; e++ {
						pe := fmt.Sprintf("%s/cam%d", pd, e)
						insert(pe, true, 0)
						for f := 0; f < 100; f++ {
							insert(fmt.Sprintf("%s/clip%03d.mov", pe, f), false, int64(1024*(f+1)))
						}
					}
				}
			}
		}
	}
	return total
}

// BenchmarkSubtreeRecompute_300k times ONLY the boot/full-rebuild aggregate
// pass (recomputeSubtreeAggregatesLocked) over the ~304k-entry tree — the
// exact work rebuildCaches adds at Open. Bound: <1s per pass.
func BenchmarkSubtreeRecompute_300k(b *testing.B) {
	b.Setenv("JM_SUBTREE_SIZES", "1")
	s, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer s.Close()
	n := buildSubtreeBenchTree(b, s)
	b.Logf("tree built: %d entries", n)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.mu.Lock()
		s.recomputeSubtreeAggregatesLocked()
		s.mu.Unlock()
	}
	b.StopTimer()

	// Sanity: root totals match the analytic sum — 3000 leaf dirs × Σ 1..100 KiB.
	wantFiles := int64(300_000)
	wantBytes := int64(3000) * (100 * 101 / 2) * 1024
	gb, gf, ok := s.SubtreeSize(".")
	if !ok || gb != wantBytes || gf != wantFiles {
		b.Fatalf("root aggregate = (%d, %d, %v), want (%d, %d, true)", gb, gf, ok, wantBytes, wantFiles)
	}
}
