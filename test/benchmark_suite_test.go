package test

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const (
	nfsMount  = "/Volumes/zpool"
	fuseMount = "/Users/LelandDutcher/.juicemount/fuse-internal"
	smbMount  = "/Volumes/zSSD" // optional

	// Known directories of various sizes on the NFS mount
	dirSmall = "Film Projects/GMTM/Coaches and FO3 Ads"                // ~9 entries
	dirLarge = "Video Editing Assets/SFX Organized/Impacts & Hits"     // ~1788 entries

	// Known large file for sequential / random read benchmarks (~136 MB)
	largeFile = "Film Projects/GMTM/Athlete Success Stories final/lauren.mov"

	// Deep path for tree walk
	deepTreeRoot = "Film Projects/GMTM"

	// Regression threshold: flag if >20% slower than baseline
	regressionThreshold = 1.20

	benchmarkBaselinesFile = "benchmark_baselines.json"
)

// ---------------------------------------------------------------------------
// Baselines
// ---------------------------------------------------------------------------

type baselines struct {
	FinderDirOpen10    float64 `json:"finder_dir_open_10"`
	FinderDirOpen100   float64 `json:"finder_dir_open_100"`
	FinderDirOpen1000  float64 `json:"finder_dir_open_1000"`
	StatP50            float64 `json:"stat_p50_ms"`
	StatP95            float64 `json:"stat_p95_ms"`
	StatP99            float64 `json:"stat_p99_ms"`
	SeqReadMBPS        float64 `json:"sequential_read_mbps"`
	RandomRead64kMS    float64 `json:"random_read_64k_ms"`
	WriteSmall50x1kMS  float64 `json:"write_small_50x1k_ms"`
	WriteLarge10mMS    float64 `json:"write_large_10m_ms"`
	CreateRenameMS     float64 `json:"create_rename_ms"`
	DeepTreeWalkMS     float64 `json:"deep_tree_walk_ms"`
	ConcurrentReads4MS float64 `json:"concurrent_reads_4x_ms"`
	ColdRunMS          float64 `json:"cold_run_ms"`
	WarmRunMS          float64 `json:"warm_run_ms"`
}

func loadBaselines(t *testing.T) baselines {
	t.Helper()
	path := filepath.Join(".", benchmarkBaselinesFile)
	// Also try from test directory
	if _, err := os.Stat(path); err != nil {
		path = filepath.Join(filepath.Dir(os.Args[0]), benchmarkBaselinesFile)
	}
	if _, err := os.Stat(path); err != nil {
		// Try relative to test source
		path = filepath.Join("testdata", benchmarkBaselinesFile)
	}
	if _, err := os.Stat(path); err != nil {
		// Last resort: hardcoded path
		path = "/Users/LelandDutcher/Developer/JuiceMount5/test/benchmark_baselines.json"
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Logf("WARNING: could not load baselines from %s: %v (using defaults)", path, err)
		return baselines{
			FinderDirOpen10:    15,
			FinderDirOpen100:   50,
			FinderDirOpen1000:  300,
			StatP50:            0.5,
			StatP95:            2.0,
			StatP99:            5.0,
			SeqReadMBPS:        80,
			RandomRead64kMS:    5.0,
			WriteSmall50x1kMS:  500,
			WriteLarge10mMS:    2000,
			CreateRenameMS:     50,
			DeepTreeWalkMS:     1000,
			ConcurrentReads4MS: 500,
			ColdRunMS:          200,
			WarmRunMS:          50,
		}
	}
	var b baselines
	if err := json.Unmarshal(data, &b); err != nil {
		t.Fatalf("parse baselines: %v", err)
	}
	return b
}

// ---------------------------------------------------------------------------
// Result tracking
// ---------------------------------------------------------------------------

type benchResult struct {
	Name       string
	Value      float64 // measured value (ms, MB/s, etc.)
	Unit       string
	Baseline   float64
	Regressed  bool
	Ratio      float64 // measured / baseline (>1 = slower for latency)
	LowerBetter bool   // true for latency (ms), false for throughput (MB/s)
}

type benchSuite struct {
	t       *testing.T
	bl      baselines
	results []benchResult
}

func newBenchSuite(t *testing.T) *benchSuite {
	return &benchSuite{
		t:  t,
		bl: loadBaselines(t),
	}
}

// record adds a result and checks for regression.
// For latency metrics (lowerBetter=true), regression means measured > baseline * threshold.
// For throughput metrics (lowerBetter=false), regression means measured < baseline / threshold.
func (s *benchSuite) record(name string, measured, baseline float64, unit string, lowerBetter bool) {
	var ratio float64
	var regressed bool

	if lowerBetter {
		// Latency: higher is worse
		if baseline > 0 {
			ratio = measured / baseline
			regressed = ratio > regressionThreshold
		}
	} else {
		// Throughput: lower is worse
		if baseline > 0 {
			ratio = baseline / measured // >1 means regression
			regressed = ratio > regressionThreshold
		}
	}

	r := benchResult{
		Name:        name,
		Value:       measured,
		Unit:        unit,
		Baseline:    baseline,
		Regressed:   regressed,
		Ratio:       ratio,
		LowerBetter: lowerBetter,
	}
	s.results = append(s.results, r)

	status := "OK"
	if regressed {
		status = "REGRESSION"
	}
	s.t.Logf("  %-35s %8.2f %-6s (baseline: %8.2f, ratio: %.2f) [%s]",
		name, measured, unit, baseline, ratio, status)
}

func (s *benchSuite) printSummary() {
	s.t.Log("")
	s.t.Log("================================================================================")
	s.t.Log("  BENCHMARK SUITE RESULTS")
	s.t.Log("================================================================================")
	s.t.Log("")
	s.t.Logf("  %-35s %10s %6s %10s %8s %s", "Benchmark", "Measured", "Unit", "Baseline", "Ratio", "Status")
	s.t.Log("  " + strings.Repeat("-", 90))

	regressions := 0
	for _, r := range s.results {
		status := "  OK"
		if r.Regressed {
			status = "  ** REGRESSION **"
			regressions++
		}
		direction := ""
		if r.LowerBetter {
			if r.Ratio > 1 {
				direction = " (slower)"
			} else {
				direction = " (faster)"
			}
		} else {
			if r.Ratio > 1 {
				direction = " (slower)"
			} else {
				direction = " (faster)"
			}
		}
		s.t.Logf("  %-35s %10.2f %6s %10.2f %7.2fx%s %s",
			r.Name, r.Value, r.Unit, r.Baseline, r.Ratio, direction, status)
	}

	s.t.Log("  " + strings.Repeat("-", 90))
	s.t.Logf("  Total benchmarks: %d | Regressions: %d", len(s.results), regressions)
	s.t.Log("================================================================================")

	if regressions > 0 {
		s.t.Errorf("FAILED: %d regression(s) detected (>%.0f%% slower than baseline)",
			regressions, (regressionThreshold-1)*100)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func requireMount(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Skipf("mount not available: %s (%v)", path, err)
	}
}

func requireFile(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Skipf("file not available: %s (%v)", path, err)
	}
	if info.IsDir() {
		t.Skipf("expected file, got directory: %s", path)
	}
}

func requireDir(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Skipf("directory not available: %s (%v)", path, err)
	}
	if !info.IsDir() {
		t.Skipf("expected directory, got file: %s", path)
	}
}

// findDirNearSize finds a directory under root with approximately n entries.
func findDirNearSize(root string, target int) (string, int) {
	best := ""
	bestCount := 0
	bestDiff := math.MaxInt32

	// Walk only a few levels to avoid slow FUSE traversal
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || !info.IsDir() {
			return nil
		}
		// Limit depth
		rel, _ := filepath.Rel(root, path)
		if strings.Count(rel, string(filepath.Separator)) > 3 {
			return filepath.SkipDir
		}
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil
		}
		diff := len(entries) - target
		if diff < 0 {
			diff = -diff
		}
		if diff < bestDiff {
			best = path
			bestCount = len(entries)
			bestDiff = diff
		}
		// Good enough
		if float64(diff)/float64(target+1) < 0.3 {
			return filepath.SkipAll
		}
		return nil
	})
	return best, bestCount
}

// percentile computes the p-th percentile from a sorted slice of durations.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func msFromDur(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

// ---------------------------------------------------------------------------
// TestBenchmarkSuite — the main entry point
// ---------------------------------------------------------------------------

func TestBenchmarkSuite(t *testing.T) {
	requireMount(t, nfsMount)

	suite := newBenchSuite(t)

	t.Log("NFS mount:", nfsMount)
	t.Log("Regression threshold:", regressionThreshold)
	t.Log("")

	t.Run("1_FinderDirectoryOpen", func(t *testing.T) { benchFinderDirOpen(t, suite) })
	t.Run("2_StatLatency", func(t *testing.T) { benchStatLatency(t, suite) })
	t.Run("3_SequentialRead", func(t *testing.T) { benchSequentialRead(t, suite) })
	t.Run("4_RandomReadScrubbing", func(t *testing.T) { benchRandomRead(t, suite) })
	t.Run("5_WriteSmallFiles", func(t *testing.T) { benchWriteSmall(t, suite) })
	t.Run("6_WriteLargeFile", func(t *testing.T) { benchWriteLarge(t, suite) })
	t.Run("7_CreateRename", func(t *testing.T) { benchCreateRename(t, suite) })
	t.Run("8_DeepTreeWalk", func(t *testing.T) { benchDeepTreeWalk(t, suite) })
	t.Run("9_ConcurrentReads", func(t *testing.T) { benchConcurrentReads(t, suite) })
	t.Run("10_ColdVsWarm", func(t *testing.T) { benchColdVsWarm(t, suite) })

	suite.printSummary()
}

// ---------------------------------------------------------------------------
// 1. Finder Directory Open — scandir + stat all entries
// ---------------------------------------------------------------------------

func benchFinderDirOpen(t *testing.T, suite *benchSuite) {
	t.Log("=== Finder Directory Open (scandir + stat all entries) ===")

	type dirCase struct {
		label    string
		relPath  string
		baseline float64
		fallback int // target entry count if relPath not found
	}

	cases := []dirCase{
		{"dir_open_10", dirSmall, suite.bl.FinderDirOpen10, 10},
		{"dir_open_1000+", dirLarge, suite.bl.FinderDirOpen1000, 1000},
	}

	// Try to find a ~100 entry directory
	medDir, medCount := findDirNearSize(nfsMount, 100)
	if medDir != "" && medCount >= 50 {
		rel, _ := filepath.Rel(nfsMount, medDir)
		cases = append(cases[:1], append([]dirCase{
			{"dir_open_~100", rel, suite.bl.FinderDirOpen100, 100},
		}, cases[1:]...)...)
	}

	for _, c := range cases {
		dirPath := filepath.Join(nfsMount, c.relPath)
		if _, err := os.Stat(dirPath); err != nil {
			// Try to find a substitute
			sub, cnt := findDirNearSize(nfsMount, c.fallback)
			if sub == "" {
				t.Logf("  SKIP %s: directory not found", c.label)
				continue
			}
			dirPath = sub
			t.Logf("  Using substitute for %s: %s (%d entries)", c.label, dirPath, cnt)
		}

		const iterations = 5
		var totalMS float64

		for i := 0; i < iterations; i++ {
			start := time.Now()
			entries, err := os.ReadDir(dirPath)
			if err != nil {
				t.Logf("  SKIP %s: %v", c.label, err)
				continue
			}
			// Stat every entry (mimics Finder showing file metadata)
			for _, e := range entries {
				os.Stat(filepath.Join(dirPath, e.Name()))
			}
			elapsed := time.Since(start)
			totalMS += msFromDur(elapsed)
		}

		avg := totalMS / float64(iterations)
		suite.record(c.label, avg, c.baseline, "ms", true)
	}
}

// ---------------------------------------------------------------------------
// 2. Stat Latency — p50/p95/p99
// ---------------------------------------------------------------------------

func benchStatLatency(t *testing.T, suite *benchSuite) {
	t.Log("=== Stat Latency (p50/p95/p99) ===")

	// Use the large directory so we have plenty of files to stat
	dirPath := filepath.Join(nfsMount, dirLarge)
	requireDir(t, dirPath)

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		t.Skipf("cannot read %s: %v", dirPath, err)
	}

	// Limit to 200 files for reasonable test time
	limit := 200
	if len(entries) < limit {
		limit = len(entries)
	}

	// Warm up — stat each file once
	for _, e := range entries[:limit] {
		os.Stat(filepath.Join(dirPath, e.Name()))
	}

	// Measure
	var durations []time.Duration
	for _, e := range entries[:limit] {
		path := filepath.Join(dirPath, e.Name())
		start := time.Now()
		_, err := os.Stat(path)
		elapsed := time.Since(start)
		if err == nil {
			durations = append(durations, elapsed)
		}
	}

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	p50 := percentile(durations, 50)
	p95 := percentile(durations, 95)
	p99 := percentile(durations, 99)

	t.Logf("  Samples: %d stats", len(durations))
	t.Logf("  p50: %v | p95: %v | p99: %v", p50, p95, p99)

	suite.record("stat_p50", msFromDur(p50), suite.bl.StatP50, "ms", true)
	suite.record("stat_p95", msFromDur(p95), suite.bl.StatP95, "ms", true)
	suite.record("stat_p99", msFromDur(p99), suite.bl.StatP99, "ms", true)
}

// ---------------------------------------------------------------------------
// 3. Sequential Read — read a large file start to finish
// ---------------------------------------------------------------------------

func benchSequentialRead(t *testing.T, suite *benchSuite) {
	t.Log("=== Sequential Read ===")

	// Create a 10MB test file to avoid stale NFS file handle issues with existing files
	const testSize = 10 * 1024 * 1024
	filePath := filepath.Join(nfsMount, fmt.Sprintf("__bench_seqr_%d.dat", time.Now().UnixNano()))

	t.Logf("  Creating 10 MB test file for sequential read...")
	chunk := make([]byte, 256*1024)
	for i := range chunk {
		chunk[i] = byte(i % 251) // prime modulus for variety
	}
	wf, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("create test file: %v", err)
	}
	written := 0
	for written < testSize {
		n, err := wf.Write(chunk)
		if err != nil {
			wf.Close()
			os.Remove(filePath)
			t.Fatalf("write test file: %v", err)
		}
		written += n
	}
	wf.Sync()
	wf.Close()
	defer os.Remove(filePath)

	sizeMB := float64(testSize) / (1024 * 1024)
	t.Logf("  File: %s (%.1f MB)", filepath.Base(filePath), sizeMB)

	f, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	buf := make([]byte, 256*1024) // 256KB read buffer
	start := time.Now()
	var bytesRead int64
	for {
		n, err := f.Read(buf)
		bytesRead += int64(n)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	elapsed := time.Since(start)

	mbps := float64(bytesRead) / (1024 * 1024) / elapsed.Seconds()
	t.Logf("  Read %.1f MB in %v = %.1f MB/s", float64(bytesRead)/(1024*1024), elapsed, mbps)

	suite.record("seq_read_throughput", mbps, suite.bl.SeqReadMBPS, "MB/s", false)
}

// ---------------------------------------------------------------------------
// 4. Random Read / Scrubbing — seek to random offsets, read 64KB
// ---------------------------------------------------------------------------

func benchRandomRead(t *testing.T, suite *benchSuite) {
	t.Log("=== Random Read / Scrubbing (64KB chunks at random offsets) ===")

	// Create a 10MB test file to avoid stale NFS file handle issues
	const testSize = 10 * 1024 * 1024
	filePath := filepath.Join(nfsMount, fmt.Sprintf("__bench_randr_%d.dat", time.Now().UnixNano()))

	t.Logf("  Creating 10 MB test file for random reads...")
	chunk := make([]byte, 256*1024)
	for i := range chunk {
		chunk[i] = byte(i % 251)
	}
	wf, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("create test file: %v", err)
	}
	written := 0
	for written < testSize {
		n, err := wf.Write(chunk)
		if err != nil {
			wf.Close()
			os.Remove(filePath)
			t.Fatalf("write test file: %v", err)
		}
		written += n
	}
	wf.Sync()
	wf.Close()
	defer os.Remove(filePath)

	fileSize := int64(testSize)

	f, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	const (
		chunkSize = 64 * 1024 // 64KB — typical video scrub read
		numReads  = 50
	)

	rng := rand.New(rand.NewSource(42)) // deterministic for reproducibility
	buf := make([]byte, chunkSize)

	var totalDur time.Duration
	successCount := 0
	for i := 0; i < numReads; i++ {
		offset := rng.Int63n(fileSize - chunkSize)
		start := time.Now()
		_, err := f.ReadAt(buf, offset)
		elapsed := time.Since(start)
		if err != nil && err != io.EOF {
			t.Logf("  ReadAt offset %d: %v", offset, err)
			continue
		}
		totalDur += elapsed
		successCount++
	}

	if successCount == 0 {
		t.Skip("no successful random reads")
	}

	avgMS := msFromDur(totalDur / time.Duration(successCount))
	t.Logf("  %d random 64KB reads, avg %v per read", successCount, totalDur/time.Duration(successCount))

	suite.record("random_read_64k", avgMS, suite.bl.RandomRead64kMS, "ms", true)
}

// ---------------------------------------------------------------------------
// 5. Write Small Files — 50 x 1KB (project file save pattern)
// ---------------------------------------------------------------------------

func benchWriteSmall(t *testing.T, suite *benchSuite) {
	t.Log("=== Write Small Files (50 x 1KB) ===")

	tmpDir := filepath.Join(nfsMount, fmt.Sprintf("__bench_w50_%d", time.Now().UnixNano()))
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	data := make([]byte, 1024) // 1KB
	for i := range data {
		data[i] = byte(i % 256)
	}

	const fileCount = 50
	start := time.Now()
	for i := 0; i < fileCount; i++ {
		name := filepath.Join(tmpDir, fmt.Sprintf("file_%04d.dat", i))
		if err := os.WriteFile(name, data, 0644); err != nil {
			t.Fatalf("write file %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	totalMS := msFromDur(elapsed)

	t.Logf("  Wrote %d x 1KB files in %v (%.1f ms/file)", fileCount, elapsed, totalMS/fileCount)

	suite.record("write_50x1k", totalMS, suite.bl.WriteSmall50x1kMS, "ms", true)
}

// ---------------------------------------------------------------------------
// 6. Write Large File — single 10MB+ file
// ---------------------------------------------------------------------------

func benchWriteLarge(t *testing.T, suite *benchSuite) {
	t.Log("=== Write Large File (10 MB) ===")

	tmpFile := filepath.Join(nfsMount, fmt.Sprintf("__bench_w10m_%d.dat", time.Now().UnixNano()))
	defer os.Remove(tmpFile)

	const totalSize = 10 * 1024 * 1024 // 10 MB
	const chunkSize = 256 * 1024        // 256KB writes

	chunk := make([]byte, chunkSize)
	for i := range chunk {
		chunk[i] = byte(i % 256)
	}

	f, err := os.Create(tmpFile)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	start := time.Now()
	written := 0
	for written < totalSize {
		n, err := f.Write(chunk)
		if err != nil {
			f.Close()
			t.Fatalf("write: %v", err)
		}
		written += n
	}
	if err := f.Sync(); err != nil {
		f.Close()
		t.Fatalf("sync: %v", err)
	}
	f.Close()
	elapsed := time.Since(start)

	totalMS := msFromDur(elapsed)
	mbps := float64(written) / (1024 * 1024) / elapsed.Seconds()
	t.Logf("  Wrote %.1f MB in %v (%.1f MB/s)", float64(written)/(1024*1024), elapsed, mbps)

	suite.record("write_large_10m", totalMS, suite.bl.WriteLarge10mMS, "ms", true)
}

// ---------------------------------------------------------------------------
// 7. Create + Rename — Finder "new folder" workflow
//    Run with concurrent goroutine hammering the FS to catch SQLITE_BUSY
// ---------------------------------------------------------------------------

func benchCreateRename(t *testing.T, suite *benchSuite) {
	t.Log("=== Create + Rename (with concurrent FS activity) ===")

	baseDir := filepath.Join(nfsMount, fmt.Sprintf("__bench_cr_%d", time.Now().UnixNano()))
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		t.Fatalf("mkdir base: %v", err)
	}
	defer os.RemoveAll(baseDir)

	// Start a background goroutine that creates/removes files to simulate
	// the reconcile loop and other FS activity (triggers SQLITE_BUSY if locking is wrong)
	stopCh := make(chan struct{})
	var bgWg sync.WaitGroup
	bgWg.Add(1)
	go func() {
		defer bgWg.Done()
		idx := 0
		for {
			select {
			case <-stopCh:
				return
			default:
			}
			name := filepath.Join(baseDir, fmt.Sprintf("__bg_%d", idx))
			os.WriteFile(name, []byte("bg"), 0644)
			os.Remove(name)
			idx++
			time.Sleep(time.Millisecond)
		}
	}()

	const iterations = 20
	var totalDur time.Duration

	for i := 0; i < iterations; i++ {
		untitled := filepath.Join(baseDir, "untitled folder")
		renamed := filepath.Join(baseDir, fmt.Sprintf("My Project %d", i))

		start := time.Now()
		if err := os.Mkdir(untitled, 0755); err != nil {
			t.Fatalf("mkdir untitled (iter %d): %v", i, err)
		}
		if err := os.Rename(untitled, renamed); err != nil {
			t.Fatalf("rename (iter %d): %v", i, err)
		}
		elapsed := time.Since(start)
		totalDur += elapsed

		// Clean up for next iteration — wait for remove to propagate
		// through NFS → FUSE → JuiceFS before creating next "untitled folder"
		os.RemoveAll(renamed)
		// Wait for the old "untitled folder" path to be fully clear
		for j := 0; j < 50; j++ {
			if _, err := os.Stat(filepath.Join(baseDir, "untitled folder")); os.IsNotExist(err) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	close(stopCh)
	bgWg.Wait()

	avgMS := msFromDur(totalDur / time.Duration(iterations))
	t.Logf("  %d create+rename cycles, avg %v", iterations, totalDur/time.Duration(iterations))

	suite.record("create_rename", avgMS, suite.bl.CreateRenameMS, "ms", true)
}

// ---------------------------------------------------------------------------
// 8. Deep Tree Walk — os.Walk to depth 3-4
// ---------------------------------------------------------------------------

func benchDeepTreeWalk(t *testing.T, suite *benchSuite) {
	t.Log("=== Deep Tree Walk (os.Walk, depth 3-4) ===")

	root := filepath.Join(nfsMount, deepTreeRoot)
	requireDir(t, root)

	const maxDepth = 4
	var fileCount, dirCount int

	start := time.Now()
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		depth := strings.Count(rel, string(filepath.Separator))
		if depth > maxDepth {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if info.IsDir() {
			dirCount++
		} else {
			fileCount++
		}
		return nil
	})
	elapsed := time.Since(start)

	totalMS := msFromDur(elapsed)
	total := fileCount + dirCount
	rate := float64(total) / elapsed.Seconds()
	t.Logf("  Walked %d dirs + %d files = %d entries in %v (%.0f entries/sec)",
		dirCount, fileCount, total, elapsed, rate)

	suite.record("deep_tree_walk", totalMS, suite.bl.DeepTreeWalkMS, "ms", true)
}

// ---------------------------------------------------------------------------
// 9. Concurrent Reads — 4 goroutines reading different files
// ---------------------------------------------------------------------------

func benchConcurrentReads(t *testing.T, suite *benchSuite) {
	t.Log("=== Concurrent Reads (4 goroutines, different files) ===")

	// Create 4 x 1MB test files to avoid stale NFS file handle issues
	const fileSize = 1024 * 1024 // 1MB each
	const fileCount = 4

	tmpDir := filepath.Join(nfsMount, fmt.Sprintf("__bench_conc_%d", time.Now().UnixNano()))
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	data := make([]byte, fileSize)
	for i := range data {
		data[i] = byte(i % 251)
	}

	var files []string
	for i := 0; i < fileCount; i++ {
		fpath := filepath.Join(tmpDir, fmt.Sprintf("track_%d.dat", i))
		if err := os.WriteFile(fpath, data, 0644); err != nil {
			t.Fatalf("create file %d: %v", i, err)
		}
		files = append(files, fpath)
	}

	t.Logf("  Reading %d x 1MB files concurrently (simulates multi-track timeline)", len(files))

	var wg sync.WaitGroup
	start := time.Now()
	for _, fpath := range files {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			f, err := os.Open(p)
			if err != nil {
				return
			}
			defer f.Close()
			buf := make([]byte, fileSize)
			io.ReadFull(f, buf)
		}(fpath)
	}
	wg.Wait()
	elapsed := time.Since(start)

	totalMS := msFromDur(elapsed)
	t.Logf("  4 concurrent 1MB reads completed in %v", elapsed)

	suite.record("concurrent_reads_4x", totalMS, suite.bl.ConcurrentReads4MS, "ms", true)
}

// ---------------------------------------------------------------------------
// 10. Cold vs Warm — measure same operation twice
// ---------------------------------------------------------------------------

func benchColdVsWarm(t *testing.T, suite *benchSuite) {
	t.Log("=== Cold vs Warm (readdir + stat, same directory twice) ===")

	// Use a directory that is likely NOT in cache
	// We pick a deep subdirectory that hasn't been touched yet
	coldDir := filepath.Join(nfsMount, "Video Editing Assets/SFX Organized/Ambience")
	if _, err := os.Stat(coldDir); err != nil {
		coldDir = filepath.Join(nfsMount, dirSmall)
	}
	requireDir(t, coldDir)

	// Cold run — first access
	coldStart := time.Now()
	entries, err := os.ReadDir(coldDir)
	if err != nil {
		t.Fatalf("cold readdir: %v", err)
	}
	for _, e := range entries {
		os.Stat(filepath.Join(coldDir, e.Name()))
	}
	coldDur := time.Since(coldStart)

	// Warm run — second access (kernel cache should be warm)
	warmStart := time.Now()
	entries, err = os.ReadDir(coldDir)
	if err != nil {
		t.Fatalf("warm readdir: %v", err)
	}
	for _, e := range entries {
		os.Stat(filepath.Join(coldDir, e.Name()))
	}
	warmDur := time.Since(warmStart)

	coldMS := msFromDur(coldDur)
	warmMS := msFromDur(warmDur)

	t.Logf("  Directory: %s (%d entries)", filepath.Base(coldDir), len(entries))
	t.Logf("  Cold: %v | Warm: %v | Speedup: %.1fx",
		coldDur, warmDur, float64(coldDur)/float64(warmDur))

	suite.record("cold_run", coldMS, suite.bl.ColdRunMS, "ms", true)
	suite.record("warm_run", warmMS, suite.bl.WarmRunMS, "ms", true)
}

// ---------------------------------------------------------------------------
// TestBenchmarkSuite_UpdateBaselines — run to capture new baselines
// ---------------------------------------------------------------------------

func TestBenchmarkSuite_UpdateBaselines(t *testing.T) {
	if os.Getenv("UPDATE_BASELINES") != "1" {
		t.Skip("Set UPDATE_BASELINES=1 to update baselines")
	}

	t.Log("Running benchmark suite to capture new baselines...")
	t.Log("This will overwrite benchmark_baselines.json")

	// Run the full suite and capture results
	suite := newBenchSuite(t)

	// Override baselines to very generous values so nothing "regresses"
	suite.bl = baselines{
		FinderDirOpen10:    99999,
		FinderDirOpen100:   99999,
		FinderDirOpen1000:  99999,
		StatP50:            99999,
		StatP95:            99999,
		StatP99:            99999,
		SeqReadMBPS:        0.001,
		RandomRead64kMS:    99999,
		WriteSmall50x1kMS:  99999,
		WriteLarge10mMS:    99999,
		CreateRenameMS:     99999,
		DeepTreeWalkMS:     99999,
		ConcurrentReads4MS: 99999,
		ColdRunMS:          99999,
		WarmRunMS:           99999,
	}

	t.Run("1_FinderDirectoryOpen", func(t *testing.T) { benchFinderDirOpen(t, suite) })
	t.Run("2_StatLatency", func(t *testing.T) { benchStatLatency(t, suite) })
	t.Run("3_SequentialRead", func(t *testing.T) { benchSequentialRead(t, suite) })
	t.Run("4_RandomReadScrubbing", func(t *testing.T) { benchRandomRead(t, suite) })
	t.Run("5_WriteSmallFiles", func(t *testing.T) { benchWriteSmall(t, suite) })
	t.Run("6_WriteLargeFile", func(t *testing.T) { benchWriteLarge(t, suite) })
	t.Run("7_CreateRename", func(t *testing.T) { benchCreateRename(t, suite) })
	t.Run("8_DeepTreeWalk", func(t *testing.T) { benchDeepTreeWalk(t, suite) })
	t.Run("9_ConcurrentReads", func(t *testing.T) { benchConcurrentReads(t, suite) })
	t.Run("10_ColdVsWarm", func(t *testing.T) { benchColdVsWarm(t, suite) })

	// Build new baselines from measured values
	newBL := map[string]interface{}{
		"_comment":              "Baseline timings. Updated by TestBenchmarkSuite_UpdateBaselines.",
		"_updated":              time.Now().Format("2006-01-02"),
	}

	for _, r := range suite.results {
		switch r.Name {
		case "dir_open_10":
			newBL["finder_dir_open_10"] = math.Round(r.Value*100) / 100
		case "dir_open_~100":
			newBL["finder_dir_open_100"] = math.Round(r.Value*100) / 100
		case "dir_open_1000+":
			newBL["finder_dir_open_1000"] = math.Round(r.Value*100) / 100
		case "stat_p50":
			newBL["stat_p50_ms"] = math.Round(r.Value*1000) / 1000
		case "stat_p95":
			newBL["stat_p95_ms"] = math.Round(r.Value*1000) / 1000
		case "stat_p99":
			newBL["stat_p99_ms"] = math.Round(r.Value*1000) / 1000
		case "seq_read_throughput":
			newBL["sequential_read_mbps"] = math.Round(r.Value*10) / 10
		case "random_read_64k":
			newBL["random_read_64k_ms"] = math.Round(r.Value*100) / 100
		case "write_50x1k":
			newBL["write_small_50x1k_ms"] = math.Round(r.Value*10) / 10
		case "write_large_10m":
			newBL["write_large_10m_ms"] = math.Round(r.Value*10) / 10
		case "create_rename":
			newBL["create_rename_ms"] = math.Round(r.Value*100) / 100
		case "deep_tree_walk":
			newBL["deep_tree_walk_ms"] = math.Round(r.Value*10) / 10
		case "concurrent_reads_4x":
			newBL["concurrent_reads_4x_ms"] = math.Round(r.Value*10) / 10
		case "cold_run":
			newBL["cold_run_ms"] = math.Round(r.Value*10) / 10
		case "warm_run":
			newBL["warm_run_ms"] = math.Round(r.Value*10) / 10
		}
	}

	data, err := json.MarshalIndent(newBL, "", "  ")
	if err != nil {
		t.Fatalf("marshal baselines: %v", err)
	}

	outPath := "/Users/LelandDutcher/Developer/JuiceMount5/test/benchmark_baselines.json"
	if err := os.WriteFile(outPath, data, 0644); err != nil {
		t.Fatalf("write baselines: %v", err)
	}
	t.Logf("Updated baselines written to %s", outPath)
}
