package test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
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

const fuseMountPath = "/Users/USER/.juicemount/fuse-internal"

// findDirWithNEntries finds a directory with approximately n entries.
func findDirWithNEntries(root string, target int, tolerance float64) (string, int) {
	best := ""
	bestCount := 0
	bestDiff := target

	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || !info.IsDir() {
			return nil
		}
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil
		}
		diff := len(entries) - target
		if diff < 0 {
			diff = -diff
		}
		if diff < bestDiff || (diff == bestDiff && len(entries) > bestCount) {
			best = path
			bestCount = len(entries)
			bestDiff = diff
		}
		if float64(diff)/float64(target) < tolerance {
			return filepath.SkipAll
		}
		return nil
	})
	return best, bestCount
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

	avg := totalDur / time.Duration(iterations)
	perEntry := time.Duration(0)
	if entryCount > 0 {
		perEntry = avg / time.Duration(entryCount)
	}
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
	nfsMount := env.mount

	t.Log("=== READDIR PERFORMANCE: NFS vs FUSE ===")
	t.Log("(Simulates: Finder opens a folder and lists contents)")
	t.Log("")

	// Find directories with different entry counts
	type dirTest struct {
		label   string
		nfsDir  string
		fuseDir string
		count   int
	}

	var tests []dirTest

	// Root directory
	nfsEntries, _ := os.ReadDir(nfsMount)
	tests = append(tests, dirTest{
		label:   fmt.Sprintf("Root (%d entries)", len(nfsEntries)),
		nfsDir:  nfsMount,
		fuseDir: fuseMountPath,
		count:   len(nfsEntries),
	})

	// Find a medium directory (10-30 entries)
	medDir, medCount := findDirWithNEntries(fuseMountPath, 20, 0.5)
	if medDir != "" {
		rel, _ := filepath.Rel(fuseMountPath, medDir)
		tests = append(tests, dirTest{
			label:   fmt.Sprintf("Medium dir (%d entries): %s", medCount, rel),
			nfsDir:  filepath.Join(nfsMount, rel),
			fuseDir: medDir,
			count:   medCount,
		})
	}

	// Find a large directory (100+ entries)
	lgDir, lgCount := findDirWithNEntries(fuseMountPath, 100, 0.5)
	if lgDir != "" {
		rel, _ := filepath.Rel(fuseMountPath, lgDir)
		tests = append(tests, dirTest{
			label:   fmt.Sprintf("Large dir (%d entries): %s", lgCount, rel),
			nfsDir:  filepath.Join(nfsMount, rel),
			fuseDir: lgDir,
			count:   lgCount,
		})
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
	nfsMount := env.mount

	t.Log("=== STAT PERFORMANCE: NFS vs FUSE ===")
	t.Log("(Simulates: Finder displaying file size, date, type for each item)")
	t.Log("")

	// Test stat on root entries
	t.Log("--- Root directory files ---")
	nfsStat := benchmarkStat(t, "NFS ", nfsMount, 10)
	fuseStat := benchmarkStat(t, "FUSE", fuseMountPath, 10)
	if fuseStat > 0 {
		t.Logf("  Speedup: NFS is %.1fx faster than FUSE", float64(fuseStat)/float64(nfsStat))
	}

	// Test stat on a subdirectory with many files
	t.Log("")
	t.Log("--- Subdirectory files ---")
	subDir, count := findDirWithNEntries(fuseMountPath, 50, 0.5)
	if subDir != "" {
		rel, _ := filepath.Rel(fuseMountPath, subDir)
		t.Logf("Using: %s (%d entries)", rel, count)
		nfsSub := benchmarkStat(t, "NFS ", filepath.Join(nfsMount, rel), 10)
		fuseSub := benchmarkStat(t, "FUSE", subDir, 10)
		if fuseSub > 0 {
			t.Logf("  Speedup: NFS is %.1fx faster than FUSE", float64(fuseSub)/float64(nfsSub))
		}
	}
}

// TestFinderPerf_DeepNavigation simulates clicking through nested folders in Finder.
func TestFinderPerf_DeepNavigation(t *testing.T) {
	env := setupE2E(t)
	nfsMount := env.mount

	t.Log("=== DEEP NAVIGATION: NFS vs FUSE ===")
	t.Log("(Simulates: Finder clicking through Project > Footage > Day1 > Camera A)")
	t.Log("")

	// Use a known deep path instead of walking the entire FUSE tree
	// (walking 146K entries through FUSE takes too long over WiFi)
	rel := "Film Projects/GMTM/All Sports Combines/7v7 Elite SPARQ St Pete/Footage/Friday"
	maxDepth := strings.Count(rel, "/") + 1

	// Verify it exists
	if _, err := os.Stat(filepath.Join(fuseMountPath, rel)); err != nil {
		t.Skipf("Deep path not found: %v", err)
	}
	t.Logf("Deepest path: %s (depth=%d)", rel, maxDepth)

	nfsDeep := filepath.Join(nfsMount, rel)
	t.Log("")
	benchmarkDeepLookup(t, "NFS ", nfsDeep, 50)
	benchmarkDeepLookup(t, "FUSE", filepath.Join(fuseMountPath, rel), 50)

	// Also test incremental navigation (stat each path component)
	t.Log("")
	t.Log("--- Incremental navigation (stat each level) ---")
	parts := strings.Split(rel, "/")

	nfsTotal := time.Duration(0)
	fuseTotal := time.Duration(0)

	for i := 1; i <= len(parts); i++ {
		partial := filepath.Join(parts[:i]...)

		nfsPath := filepath.Join(nfsMount, partial)
		fusePath := filepath.Join(fuseMountPath, partial)

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
	nfsMount := env.mount

	t.Log("=== COLD DIRECTORY OPEN: NFS vs FUSE ===")
	t.Log("(Simulates: First time opening a folder in Finder)")
	t.Log("")

	// Use known directories instead of walking FUSE tree
	testDirs := []string{
		".",
		"Film Projects",
		"Film Projects/GMTM",
		"Film Projects/GMTM/All Sports Combines",
		"Soap Regular",
	}
	// Filter to ones that exist
	var validDirs []string
	for _, d := range testDirs {
		if _, err := os.Stat(filepath.Join(fuseMountPath, d)); err == nil {
			validDirs = append(validDirs, d)
		}
	}
	testDirs = validDirs

	for _, rel := range testDirs {
		nfsDir := filepath.Join(nfsMount, rel)
		fuseDir := filepath.Join(fuseMountPath, rel)

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
	nfsMount := env.mount

	t.Log("=== CONCURRENT BROWSING: NFS vs FUSE ===")
	t.Log("(Simulates: Multiple Finder windows/tabs open)")
	t.Log("")

	dirs := []string{
		".",
		"Film Projects",
		"Film Projects/GMTM",
		"Soap Regular",
	}

	// Concurrent NFS browse
	nfsDur := benchConcurrentBrowse(nfsMount, dirs)
	fuseDur := benchConcurrentBrowse(fuseMountPath, dirs)

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
	nfsMount := env.mount

	t.Log("=== FULL TREE WALK: NFS vs FUSE ===")
	t.Log("(Simulates: Finder 'Get Info' → Calculate Size, or Spotlight indexing)")
	t.Log("")

	// Walk Film Projects/GMTM (subset — full FUSE walk takes minutes on WiFi)
	testDir := "Film Projects/GMTM"

	nfsStart := time.Now()
	var nfsFiles, nfsDirs int
	filepath.Walk(filepath.Join(nfsMount, testDir), func(path string, info os.FileInfo, err error) error {
		if err != nil { return nil }
		if info.IsDir() { nfsDirs++ } else { nfsFiles++ }
		return nil
	})
	nfsDur := time.Since(nfsStart)
	t.Logf("  NFS:  %d dirs, %d files in %v (%.0f entries/sec)",
		nfsDirs, nfsFiles, nfsDur, float64(nfsFiles+nfsDirs)/nfsDur.Seconds())

	// FUSE walk of same subtree (may be slow on WiFi)
	fuseStart := time.Now()
	var fuseFiles, fuseDirs int
	filepath.Walk(filepath.Join(fuseMountPath, testDir), func(path string, info os.FileInfo, err error) error {
		if err != nil { return nil }
		if info.IsDir() { fuseDirs++ } else { fuseFiles++ }
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
	nfsMount := env.mount

	t.Log("=== PERFORMANCE SUMMARY ===")
	t.Log("")

	type result struct {
		test    string
		nfs     time.Duration
		fuse    time.Duration
		speedup float64
	}

	var results []result

	// ReadDir root
	nfsRD := benchmarkReadDir(t, "NFS", nfsMount, 20)
	fuseRD := benchmarkReadDir(t, "FUSE", fuseMountPath, 20)
	results = append(results, result{"ReadDir root", nfsRD, fuseRD, float64(fuseRD) / float64(nfsRD)})

	// Stat
	nfsST := benchmarkStat(t, "NFS", nfsMount, 10)
	fuseST := benchmarkStat(t, "FUSE", fuseMountPath, 10)
	results = append(results, result{"Stat (per file)", nfsST, fuseST, float64(fuseST) / float64(nfsST)})

	// Deep lookup — use known path
	deepRel := "Film Projects/GMTM/All Sports Combines/7v7 Elite SPARQ St Pete/Footage/Friday"
	deepDepth := strings.Count(deepRel, "/") + 1
	nfsDL := benchmarkDeepLookup(t, "NFS", filepath.Join(nfsMount, deepRel), 50)
	fuseDL := benchmarkDeepLookup(t, "FUSE", filepath.Join(fuseMountPath, deepRel), 50)
	results = append(results, result{fmt.Sprintf("Deep lookup (%d levels)", deepDepth), nfsDL, fuseDL, float64(fuseDL) / float64(nfsDL)})

	// Tree walk
	nfsTW := time.Now()
	filepath.Walk(filepath.Join(nfsMount, "Film Projects"), func(path string, info os.FileInfo, err error) error { return nil })
	nfsTWDur := time.Since(nfsTW)
	fuseTW := time.Now()
	filepath.Walk(filepath.Join(fuseMountPath, "Film Projects"), func(path string, info os.FileInfo, err error) error { return nil })
	fuseTWDur := time.Since(fuseTW)
	results = append(results, result{"Tree walk (Film Projects)", nfsTWDur, fuseTWDur, float64(fuseTWDur) / float64(nfsTWDur)})

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
