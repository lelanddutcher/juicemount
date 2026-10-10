package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

// These tests benchmark the metadata operations that determine how "snappy"
// Finder feels. The key operations are:
//
// 1. READDIR — listing a directory (Finder opens a folder)
// 2. STAT — getting file info (Finder shows size, date, icon)
// 3. LOOKUP — resolving a path component (Finder navigates into a subfolder)
// 4. READDIRPLUS — readdir + stat in one NFS call (the NFS advantage over FUSE)
//
// For each, we compare NFS (our server) vs direct FUSE (JuiceFS mount).

// fuseMountPath is resolved at run time; see fuseInternalPath below.
var fuseMountPath = fuseInternalPath()

// fuseInternalPath resolves the JuiceFS FUSE mount for the CURRENT user.
//
// This was the literal string "/Users/USER/.juicemount/fuse-internal" — a
// placeholder that no machine has. Every NFS-vs-FUSE comparison in this file
// therefore compared our server against a directory that does not exist, at 20
// call sites, and reported the result as a measurement. That is the exact
// failure the "harnesses must refuse to report" rule exists for: a number
// produced from no data is worse than silence.
//
// Resolving from os.UserHomeDir also means the comparison arm now works in a
// worktree and on any machine, instead of only on the one whose path was
// hardcoded.
func fuseInternalPath() string {
	if path := os.Getenv("JM_TEST_FUSE_PATH"); path != "" {
		return filepath.Clean(path)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".juicemount", "fuse-internal")
}

// requireFUSEMount skips (never fails) when the FUSE mount is absent, and
// REFUSES to let a caller benchmark a path that isn't there.
func requireFUSEMount(t *testing.T) string {
	t.Helper()
	p := fuseInternalPath()
	if p == "" {
		t.Skip("cannot resolve home dir; FUSE comparison arm unavailable")
	}
	st, err := os.Stat(p)
	if err != nil || !st.IsDir() {
		t.Skipf("FUSE mount %s not present — skipping the FUSE comparison arm "+
			"rather than timing a directory that does not exist", p)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := mounttable.Output(ctx)
	if err != nil || !strings.Contains(string(out), " on "+p+" (") {
		t.Skipf("FUSE path %s is only a directory, not an active mount — skipping "+
			"rather than reading or writing the hidden local mountpoint", p)
	}
	return p
}

// finderFixture is a bounded namespace created through the NFS arm and then
// observed through the underlying FUSE arm.  Earlier versions selected paths
// by walking the live volume.  Besides comparing unrelated corpora when the
// test Redis and the running app used different volumes, that could spend the
// package's entire ten-minute timeout enumerating a founder's media archive.
// A deterministic common fixture measures the serving paths, not user data.
type finderFixture struct {
	nfsRoot, fuseRoot string
	nfsMedium         string
	fuseMedium        string
	nfsLarge          string
	fuseLarge         string
	nfsDeep           string
	fuseDeep          string
	deepRel           string
}

func setupFinderFixture(t *testing.T, env *e2eEnv) finderFixture {
	t.Helper()
	requireFUSEMount(t)

	name := fmt.Sprintf("__jm_finder_fixture_%d", time.Now().UnixNano())
	nfsRoot := filepath.Join(env.mount, name)
	fuseRoot := filepath.Join(fuseInternalPath(), name)
	fx := finderFixture{
		nfsRoot: nfsRoot, fuseRoot: fuseRoot,
		nfsMedium:  filepath.Join(nfsRoot, "medium"),
		fuseMedium: filepath.Join(fuseRoot, "medium"),
		nfsLarge:   filepath.Join(nfsRoot, "large"),
		fuseLarge:  filepath.Join(fuseRoot, "large"),
		deepRel:    filepath.Join("Project", "Footage", "Day1", "Camera A"),
	}
	fx.nfsDeep = filepath.Join(nfsRoot, fx.deepRel)
	fx.fuseDeep = filepath.Join(fuseRoot, fx.deepRel)

	for _, dir := range []string{fx.nfsMedium, fx.nfsLarge, fx.nfsDeep} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("create Finder fixture %s: %v", dir, err)
		}
	}
	for i := 0; i < 20; i++ {
		if err := os.Mkdir(filepath.Join(fx.nfsMedium, fmt.Sprintf("item-%03d", i)), 0o755); err != nil {
			t.Fatalf("create medium Finder fixture: %v", err)
		}
	}
	for i := 0; i < 100; i++ {
		if err := os.Mkdir(filepath.Join(fx.nfsLarge, fmt.Sprintf("item-%03d", i)), 0o755); err != nil {
			t.Fatalf("create large Finder fixture: %v", err)
		}
	}
	for i := 0; i < 10; i++ {
		if err := os.Mkdir(filepath.Join(fx.nfsDeep, fmt.Sprintf("take-%02d", i)), 0o755); err != nil {
			t.Fatalf("create deep Finder fixture: %v", err)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		entries, err := os.ReadDir(fx.fuseLarge)
		logical := 0
		for _, entry := range entries {
			// macOS may create one AppleDouble `._item-*` sidecar for every
			// directory made through NFS.  Those are legitimate Finder traffic,
			// but they must not make the fixture's convergence check expect an
			// impossible exact raw count of 100.
			if !strings.HasPrefix(entry.Name(), "._") {
				logical++
			}
		}
		if err == nil && logical == 100 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Finder fixture did not converge into FUSE view: raw=%d logical=%d err=%v", len(entries), logical, err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Cleanup(func() {
		// Both paths name the exact same unique test subtree.  Try NFS first so
		// handler metadata stays coherent, then remove the same exact FUSE path
		// only if an interrupted test left it behind.
		_ = os.RemoveAll(fx.nfsRoot)
		_ = os.RemoveAll(fx.fuseRoot)
	})
	return fx
}

// benchmarkReadDir measures directory listing performance.
func benchmarkReadDir(t *testing.T, label, dirPath string, iterations int) time.Duration {
	t.Helper()

	// Warm up (first access may involve NFS LOOKUP chain)
	os.ReadDir(dirPath)

	var totalDur time.Duration
	var entryCount int

	for i := 0; i < iterations; i++ {
		start := time.Now()
		entries, err := os.ReadDir(dirPath)
		dur := time.Since(start)
		if err != nil {
			t.Fatalf("%s ReadDir: %v", label, err)
		}
		totalDur += dur
		entryCount = len(entries)
	}

	// VALIDITY GATE. A ReadDir over an empty (or nonexistent-but-readable)
	// directory returns instantly, and without this the harness printed that
	// instant as a great result. No entries walked = no measurement.
	if entryCount == 0 {
		t.Fatalf("%s: ReadDir(%s) returned 0 entries — refusing to report a "+
			"latency for a walk that covered nothing", label, dirPath)
	}

	avg := totalDur / time.Duration(iterations)
	perEntry := avg / time.Duration(entryCount)
	t.Logf("  %s: %d entries, avg %v total, %v/entry (%d iterations)",
		label, entryCount, avg, perEntry, iterations)
	return avg
}

// benchmarkStat measures stat() latency for files in a directory.
func benchmarkStat(t *testing.T, label, dirPath string, iterations int) time.Duration {
	t.Helper()

	entries, err := os.ReadDir(dirPath)
	if err != nil || len(entries) == 0 {
		t.Skipf("%s: no entries to stat", label)
		return 0
	}

	// Pick up to 50 files to stat
	limit := 50
	if len(entries) < limit {
		limit = len(entries)
	}

	// Warm up
	for _, e := range entries[:limit] {
		os.Stat(filepath.Join(dirPath, e.Name()))
	}

	var totalDur time.Duration
	statCount := 0

	for iter := 0; iter < iterations; iter++ {
		for _, e := range entries[:limit] {
			path := filepath.Join(dirPath, e.Name())
			start := time.Now()
			_, err := os.Stat(path)
			dur := time.Since(start)
			if err != nil {
				continue
			}
			totalDur += dur
			statCount++
		}
	}

	// VALIDITY GATE. This was `totalDur / time.Duration(statCount)` with
	// statCount possibly 0 — a divide-by-zero PANIC when every stat errored
	// (each error hits `continue`). The empty-dir guard above does not cover
	// it: the directory can be non-empty and every stat still fail, which is
	// exactly what a wedged mount looks like.
	if statCount == 0 {
		t.Fatalf("%s: all %d stats failed in %s — refusing to report an average "+
			"over zero samples", label, limit*iterations, dirPath)
	}

	avg := totalDur / time.Duration(statCount)
	t.Logf("  %s: avg %v/stat (%d stats)", label, avg, statCount)
	return avg
}

// benchmarkDeepLookup measures time to resolve a deep path (Finder clicking through folders).
func benchmarkDeepLookup(t *testing.T, label, deepPath string, iterations int) time.Duration {
	t.Helper()

	// Warm up
	os.Stat(deepPath)

	var totalDur time.Duration
	for i := 0; i < iterations; i++ {
		start := time.Now()
		_, err := os.Stat(deepPath)
		dur := time.Since(start)
		if err != nil {
			t.Fatalf("%s Stat(%s): %v", label, filepath.Base(deepPath), err)
		}
		totalDur += dur
	}

	avg := totalDur / time.Duration(iterations)
	depth := strings.Count(deepPath, "/")
	t.Logf("  %s: avg %v for %d-deep path (%d iterations)",
		label, avg, depth, iterations)
	return avg
}

// TestFinderPerf_ReadDir compares NFS vs FUSE readdir for directories of varying sizes.
func TestFinderPerf_ReadDir(t *testing.T) {
	env := setupE2E(t)
	fx := setupFinderFixture(t, env)

	t.Log("=== READDIR PERFORMANCE: NFS vs FUSE ===")
	t.Log("(Simulates: Finder opens a folder and lists contents)")
	t.Log("")

	type dirTest struct {
		label   string
		nfsDir  string
		fuseDir string
	}
	tests := []dirTest{
		{label: "Fixture root (3 entries)", nfsDir: fx.nfsRoot, fuseDir: fx.fuseRoot},
		{label: "Medium directory (20 entries)", nfsDir: fx.nfsMedium, fuseDir: fx.fuseMedium},
		{label: "Large directory (100 entries)", nfsDir: fx.nfsLarge, fuseDir: fx.fuseLarge},
	}

	for _, tt := range tests {
		t.Logf("\n--- %s ---", tt.label)
		nfsDur := benchmarkReadDir(t, "NFS ", tt.nfsDir, 20)
		fuseDur := benchmarkReadDir(t, "FUSE", tt.fuseDir, 20)

		if fuseDur > 0 {
			speedup := float64(fuseDur) / float64(nfsDur)
			t.Logf("  Speedup: NFS is %.1fx faster than FUSE", speedup)
		}
	}
}

// TestFinderPerf_Stat compares individual file stat latency.
func TestFinderPerf_Stat(t *testing.T) {
	env := setupE2E(t)
	fx := setupFinderFixture(t, env)

	t.Log("=== STAT PERFORMANCE: NFS vs FUSE ===")
	t.Log("(Simulates: Finder displaying file size, date, type for each item)")
	t.Log("")

	t.Log("--- Large common fixture ---")
	nfsStat := benchmarkStat(t, "NFS ", fx.nfsLarge, 10)
	fuseStat := benchmarkStat(t, "FUSE", fx.fuseLarge, 10)
	if fuseStat > 0 {
		t.Logf("  Speedup: NFS is %.1fx faster than FUSE", float64(fuseStat)/float64(nfsStat))
	}
}

// TestFinderPerf_DeepNavigation simulates clicking through nested folders in Finder.
func TestFinderPerf_DeepNavigation(t *testing.T) {
	env := setupE2E(t)
	fx := setupFinderFixture(t, env)

	t.Log("=== DEEP NAVIGATION: NFS vs FUSE ===")
	t.Log("(Simulates: Finder clicking through Project > Footage > Day1 > Camera A)")
	t.Log("")

	maxDepth := strings.Count(fx.deepRel, string(filepath.Separator)) + 1
	t.Logf("Fixture path: %s (depth=%d)", fx.deepRel, maxDepth)
	t.Log("")
	benchmarkDeepLookup(t, "NFS ", fx.nfsDeep, 50)
	benchmarkDeepLookup(t, "FUSE", fx.fuseDeep, 50)

	// Also test incremental navigation (stat each path component)
	t.Log("")
	t.Log("--- Incremental navigation (stat each level) ---")
	parts := strings.Split(fx.deepRel, string(filepath.Separator))

	nfsTotal := time.Duration(0)
	fuseTotal := time.Duration(0)

	for i := 1; i <= len(parts); i++ {
		partial := filepath.Join(parts[:i]...)

		nfsPath := filepath.Join(fx.nfsRoot, partial)
		fusePath := filepath.Join(fx.fuseRoot, partial)

		start := time.Now()
		os.Stat(nfsPath)
		nfsDur := time.Since(start)
		nfsTotal += nfsDur

		start = time.Now()
		os.Stat(fusePath)
		fuseDur := time.Since(start)
		fuseTotal += fuseDur
	}

	t.Logf("  NFS  total navigation (%d levels): %v (avg %v/level)",
		len(parts), nfsTotal, nfsTotal/time.Duration(len(parts)))
	t.Logf("  FUSE total navigation (%d levels): %v (avg %v/level)",
		len(parts), fuseTotal, fuseTotal/time.Duration(len(parts)))
	if fuseTotal > 0 {
		t.Logf("  Speedup: NFS is %.1fx faster", float64(fuseTotal)/float64(nfsTotal))
	}
}

// TestFinderPerf_ColdDirectoryOpen simulates opening a directory for the first time
// (cold NFS attribute cache, cold FUSE stat cache).
func TestFinderPerf_ColdDirectoryOpen(t *testing.T) {
	env := setupE2E(t)
	fx := setupFinderFixture(t, env)

	t.Log("=== COLD DIRECTORY OPEN: NFS vs FUSE ===")
	t.Log("(Simulates: First time opening a folder in Finder)")
	t.Log("")

	testDirs := []string{
		".",
		"medium",
		"large",
		fx.deepRel,
	}

	for _, rel := range testDirs {
		nfsDir := filepath.Join(fx.nfsRoot, rel)
		fuseDir := filepath.Join(fx.fuseRoot, rel)

		// Cold open: readdir + stat every entry (what Finder does)
		nfsDur := benchColdOpen(nfsDir)
		fuseDur := benchColdOpen(fuseDir)

		entries, _ := os.ReadDir(fuseDir)
		speedup := float64(fuseDur) / float64(nfsDur)
		t.Logf("  %s (%d entries): NFS=%v  FUSE=%v  (NFS %.1fx faster)",
			rel, len(entries), nfsDur.Round(time.Microsecond), fuseDur.Round(time.Microsecond), speedup)
	}
}

func benchColdOpen(dirPath string) time.Duration {
	start := time.Now()
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return 0
	}
	// Stat every entry (Finder does this to show file info)
	for _, e := range entries {
		os.Stat(filepath.Join(dirPath, e.Name()))
	}
	return time.Since(start)
}

// TestFinderPerf_ConcurrentBrowse simulates multiple Finder windows open simultaneously.
func TestFinderPerf_ConcurrentBrowse(t *testing.T) {
	env := setupE2E(t)
	fx := setupFinderFixture(t, env)

	t.Log("=== CONCURRENT BROWSING: NFS vs FUSE ===")
	t.Log("(Simulates: Multiple Finder windows/tabs open)")
	t.Log("")

	dirs := []string{
		".",
		"medium",
		"large",
		fx.deepRel,
	}

	// Concurrent NFS browse
	nfsDur := benchConcurrentBrowse(fx.nfsRoot, dirs)
	fuseDur := benchConcurrentBrowse(fx.fuseRoot, dirs)

	t.Logf("  NFS  concurrent (%d dirs): %v", len(dirs), nfsDur)
	t.Logf("  FUSE concurrent (%d dirs): %v", len(dirs), fuseDur)
	if fuseDur > 0 {
		t.Logf("  Speedup: NFS is %.1fx faster", float64(fuseDur)/float64(nfsDur))
	}
}

func benchConcurrentBrowse(root string, dirs []string) time.Duration {
	var wg sync.WaitGroup
	start := time.Now()

	for _, rel := range dirs {
		wg.Add(1)
		go func(dirRel string) {
			defer wg.Done()
			dirPath := filepath.Join(root, dirRel)
			entries, _ := os.ReadDir(dirPath)
			for _, e := range entries {
				os.Stat(filepath.Join(dirPath, e.Name()))
			}
		}(rel)
	}
	wg.Wait()
	return time.Since(start)
}

// TestFinderPerf_TreeWalk simulates Finder's "Calculate Size" or Spotlight indexing.
func TestFinderPerf_TreeWalk(t *testing.T) {
	env := setupE2E(t)
	fx := setupFinderFixture(t, env)

	t.Log("=== FULL TREE WALK: NFS vs FUSE ===")
	t.Log("(Simulates: Finder 'Get Info' → Calculate Size, or Spotlight indexing)")
	t.Log("")

	nfsStart := time.Now()
	var nfsFiles, nfsDirs int
	filepath.Walk(fx.nfsRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			nfsDirs++
		} else {
			nfsFiles++
		}
		return nil
	})
	nfsDur := time.Since(nfsStart)
	t.Logf("  NFS:  %d dirs, %d files in %v (%.0f entries/sec)",
		nfsDirs, nfsFiles, nfsDur, float64(nfsFiles+nfsDirs)/nfsDur.Seconds())

	// FUSE walk of same subtree (may be slow on WiFi)
	fuseStart := time.Now()
	var fuseFiles, fuseDirs int
	filepath.Walk(fx.fuseRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			fuseDirs++
		} else {
			fuseFiles++
		}
		return nil
	})
	fuseDur := time.Since(fuseStart)
	t.Logf("  FUSE: %d dirs, %d files in %v (%.0f entries/sec)",
		fuseDirs, fuseFiles, fuseDur, float64(fuseFiles+fuseDirs)/fuseDur.Seconds())
	if fuseDur > 0 {
		t.Logf("  Speedup: NFS is %.1fx faster", float64(fuseDur)/float64(nfsDur))
	}
}

// TestFinderPerf_SMBReference provides reference numbers for SMB comparison.
func TestFinderPerf_SMBReference(t *testing.T) {
	t.Log("=== SMB REFERENCE NUMBERS (from industry benchmarks) ===")
	t.Log("")
	t.Log("Typical macOS SMB3 performance on LAN:")
	t.Log("  readdir 100 entries:  50-200ms (SMB2/3 compound, depends on server)")
	t.Log("  stat per file:        2-10ms (individual SMB QUERY_INFO)")
	t.Log("  deep path lookup:     10-50ms per level (sequential SMB CREATE/CLOSE)")
	t.Log("  tree walk 1000 files: 5-30s (no READDIRPLUS equivalent)")
	t.Log("")
	t.Log("macOS SMB client issues (known):")
	t.Log("  - No READDIRPLUS: lists dir, then stats each file individually")
	t.Log("  - .DS_Store queries: extra QUERY_INFO for every directory")
	t.Log("  - Resource fork queries: ._file checks add 1 extra roundtrip/file")
	t.Log("  - Finder 'spinning beach ball' on dirs with 500+ files over WiFi")
	t.Log("")
	t.Log("NFS advantages over SMB for Finder:")
	t.Log("  - READDIRPLUS returns dir listing + all file attrs in one call")
	t.Log("  - Kernel attribute cache (actimeo) eliminates repeated stats")
	t.Log("  - No .DS_Store or resource fork overhead")
	t.Log("  - macOS NFS client is kernel-native, SMB client is kext-based")
}

// TestFinderPerf_Summary aggregates all results.
func TestFinderPerf_Summary(t *testing.T) {
	env := setupE2E(t)
	fx := setupFinderFixture(t, env)

	t.Log("=== PERFORMANCE SUMMARY ===")
	t.Log("")

	type result struct {
		test    string
		nfs     time.Duration
		fuse    time.Duration
		speedup float64
	}

	var results []result

	// ReadDir a 100-entry directory shared by both arms.
	nfsRD := benchmarkReadDir(t, "NFS", fx.nfsLarge, 20)
	fuseRD := benchmarkReadDir(t, "FUSE", fx.fuseLarge, 20)
	results = append(results, result{"ReadDir (100 entries)", nfsRD, fuseRD, float64(fuseRD) / float64(nfsRD)})

	// Stat
	nfsST := benchmarkStat(t, "NFS", fx.nfsLarge, 10)
	fuseST := benchmarkStat(t, "FUSE", fx.fuseLarge, 10)
	results = append(results, result{"Stat (per file)", nfsST, fuseST, float64(fuseST) / float64(nfsST)})

	deepDepth := strings.Count(fx.deepRel, string(filepath.Separator)) + 1
	nfsDL := benchmarkDeepLookup(t, "NFS", fx.nfsDeep, 50)
	fuseDL := benchmarkDeepLookup(t, "FUSE", fx.fuseDeep, 50)
	results = append(results, result{fmt.Sprintf("Deep lookup (%d levels)", deepDepth), nfsDL, fuseDL, float64(fuseDL) / float64(nfsDL)})

	nfsTW := time.Now()
	filepath.Walk(fx.nfsRoot, func(path string, info os.FileInfo, err error) error { return nil })
	nfsTWDur := time.Since(nfsTW)
	fuseTW := time.Now()
	filepath.Walk(fx.fuseRoot, func(path string, info os.FileInfo, err error) error { return nil })
	fuseTWDur := time.Since(fuseTW)
	results = append(results, result{"Tree walk (fixture)", nfsTWDur, fuseTWDur, float64(fuseTWDur) / float64(nfsTWDur)})

	// Print summary table
	t.Log("")
	t.Log("┌────────────────────────────┬────────────┬────────────┬──────────┐")
	t.Log("│ Operation                  │ NFS        │ FUSE       │ Speedup  │")
	t.Log("├────────────────────────────┼────────────┼────────────┼──────────┤")

	sort.Slice(results, func(i, j int) bool { return results[i].speedup > results[j].speedup })

	for _, r := range results {
		t.Logf("│ %-26s │ %10s │ %10s │ %6.1fx  │",
			r.test,
			r.nfs.Round(time.Microsecond),
			r.fuse.Round(time.Microsecond),
			r.speedup)
	}
	t.Log("└────────────────────────────┴────────────┴────────────┴──────────┘")
}
