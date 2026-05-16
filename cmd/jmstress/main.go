// jmstress drives synthetic load against a mounted JuiceMount NFS path
// and reports latency / error metrics. The acceptance test for tier-1
// in VISION.md requires 24h of synthetic load without leaks or wedges
// when no real users are available; this harness is what generates that
// load. It is NOT a unit test — it talks to a real mount and assumes a
// real JuiceMount server is running.
//
// Three workload types model the realistic mix:
//
//   - finder:  rapid Stat / Readdir on random paths. Mimics a user
//              browsing in Finder.
//   - nle:     sequential Read of a randomly-chosen large file (read
//              the entire file, then pick another). Mimics Premiere /
//              DaVinci / FCP scrubbing through dailies.
//   - backup:  recursive directory walk reading every file's metadata.
//              Mimics Time Machine or rsync.
//
// Usage:
//
//	jmstress --mount /Volumes/zpool-dev --duration 1h \
//	         --finder-workers 4 --nle-workers 2 --backup-workers 1
//
// On completion, prints per-worker latency distributions and any errors
// encountered, plus a /metrics delta if --metrics-url is reachable.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	var (
		mount          = flag.String("mount", "/Volumes/zpool-dev", "mounted NFS path to drive")
		duration       = flag.Duration("duration", 1*time.Minute, "how long to run; default 1m for smoke, 24h for real validation")
		finderWorkers  = flag.Int("finder-workers", 4, "parallel Finder-shaped goroutines (Stat/Readdir)")
		nleWorkers     = flag.Int("nle-workers", 2, "parallel NLE-shaped goroutines (sequential Read of large files)")
		backupWorkers  = flag.Int("backup-workers", 1, "parallel backup-shaped goroutines (recursive walks)")
		discoveryDepth = flag.Int("discovery-depth", 6, "how many directory levels to pre-walk for the path pool")
		largeFileMin   = flag.Int64("large-file-min-mb", 50, "minimum size (MiB) for NLE worker to pick a file")
		metricsURL     = flag.String("metrics-url", "http://127.0.0.1:11050/metrics", "JuiceMount metrics endpoint for before/after delta")
		seed           = flag.Int64("seed", time.Now().UnixNano(), "RNG seed for reproducibility")
	)
	flag.Parse()

	rng := rand.New(rand.NewSource(*seed))
	fmt.Printf("jmstress: mount=%s duration=%s finder=%d nle=%d backup=%d seed=%d\n",
		*mount, *duration, *finderWorkers, *nleWorkers, *backupWorkers, *seed)

	// Sanity: confirm mount is reachable.
	if _, err := os.Stat(*mount); err != nil {
		fmt.Fprintf(os.Stderr, "mount unreachable: %v\n", err)
		os.Exit(2)
	}

	// Discover the path pool once. Bounded depth so a huge tree doesn't
	// stall startup. We also collect "large files" for the NLE worker.
	fmt.Printf("discovering paths (depth %d)...\n", *discoveryDepth)
	t0 := time.Now()
	pool, largeFiles, derr := discoverPool(*mount, *discoveryDepth, *largeFileMin*1024*1024)
	if derr != nil {
		fmt.Fprintf(os.Stderr, "discovery error: %v\n", derr)
	}
	fmt.Printf("discovery: %d dirs/files, %d large files (>%dMiB), took %s\n",
		len(pool), len(largeFiles), *largeFileMin, time.Since(t0).Round(time.Millisecond))

	if len(pool) == 0 {
		fmt.Fprintln(os.Stderr, "no paths discovered; aborting")
		os.Exit(2)
	}
	if *nleWorkers > 0 && len(largeFiles) == 0 {
		fmt.Fprintf(os.Stderr, "warning: no files >%dMiB found; NLE workers will idle\n", *largeFileMin)
	}

	// Snapshot metrics before.
	beforeMetrics := fetchMetrics(*metricsURL)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	// Honor SIGINT/SIGTERM for graceful early exit.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nsignal received — winding down")
		cancel()
	}()

	var wg sync.WaitGroup
	finderStats := newWorkerStats("finder")
	nleStats := newWorkerStats("nle")
	backupStats := newWorkerStats("backup")

	for i := 0; i < *finderWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			finderWorker(ctx, id, pool, rng, finderStats)
		}(i)
	}
	for i := 0; i < *nleWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			nleWorker(ctx, id, largeFiles, rng, nleStats)
		}(i)
	}
	for i := 0; i < *backupWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			backupWorker(ctx, id, *mount, backupStats)
		}(i)
	}

	wg.Wait()

	fmt.Println("\n=== results ===")
	finderStats.report()
	nleStats.report()
	backupStats.report()

	// Metrics delta.
	afterMetrics := fetchMetrics(*metricsURL)
	reportMetricsDelta(beforeMetrics, afterMetrics)
}

// ---------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------

func discoverPool(root string, maxDepth int, largeFileBytes int64) (allPaths, largeFiles []string, err error) {
	rootDepth := -1
	for _, c := range root {
		if c == '/' {
			rootDepth++
		}
	}
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			// Skip but don't fail the whole walk.
			return nil
		}
		depth := 0
		for _, c := range p {
			if c == '/' {
				depth++
			}
		}
		if depth-rootDepth > maxDepth {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		allPaths = append(allPaths, p)
		if !d.IsDir() {
			info, ierr := d.Info()
			if ierr == nil && info.Size() >= largeFileBytes {
				largeFiles = append(largeFiles, p)
			}
		}
		return nil
	})
	return
}

// ---------------------------------------------------------------------
// Workers
// ---------------------------------------------------------------------

func finderWorker(ctx context.Context, id int, pool []string, rng *rand.Rand, stats *workerStats) {
	// Each worker gets its own RNG to avoid lock contention on the
	// shared rand.Source.
	localRNG := rand.New(rand.NewSource(rng.Int63() + int64(id)))
	for {
		if ctx.Err() != nil {
			return
		}
		p := pool[localRNG.Intn(len(pool))]
		// Mix: 70% Stat, 30% Readdir on directories.
		op := "stat"
		if localRNG.Intn(10) < 3 {
			op = "readdir"
		}
		start := time.Now()
		var err error
		switch op {
		case "stat":
			_, err = os.Stat(p)
		case "readdir":
			info, ierr := os.Stat(p)
			if ierr == nil && info.IsDir() {
				_, err = os.ReadDir(p)
			} else {
				_, err = os.Stat(p)
				op = "stat"
			}
		}
		stats.record(op, time.Since(start), err)
		// Small jitter so workers don't lockstep.
		time.Sleep(time.Duration(localRNG.Intn(20)) * time.Millisecond)
	}
}

func nleWorker(ctx context.Context, id int, largeFiles []string, rng *rand.Rand, stats *workerStats) {
	if len(largeFiles) == 0 {
		return
	}
	localRNG := rand.New(rand.NewSource(rng.Int63() + int64(id)*7919))
	buf := make([]byte, 1<<20) // 1 MiB buffer matches NFS rsize
	for {
		if ctx.Err() != nil {
			return
		}
		p := largeFiles[localRNG.Intn(len(largeFiles))]
		start := time.Now()
		err := readWhole(ctx, p, buf, stats)
		stats.record("read", time.Since(start), err)
	}
}

func readWhole(ctx context.Context, path string, buf []byte, stats *workerStats) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_, err := f.Read(buf)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func backupWorker(ctx context.Context, id int, root string, stats *workerStats) {
	for {
		if ctx.Err() != nil {
			return
		}
		start := time.Now()
		var fileCount int64
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if walkErr != nil {
				return nil
			}
			fileCount++
			return nil
		})
		stats.record("walk", time.Since(start), err)
		_ = fileCount
		// Pause between walks so we don't pin a single goroutine on it.
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// ---------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------

type workerStats struct {
	name string
	mu   sync.Mutex
	// op → samples (latency nanoseconds)
	samples map[string][]int64
	errors  atomic.Int64
}

func newWorkerStats(name string) *workerStats {
	return &workerStats{
		name:    name,
		samples: make(map[string][]int64),
	}
}

func (w *workerStats) record(op string, elapsed time.Duration, err error) {
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		w.errors.Add(1)
		return
	}
	w.mu.Lock()
	w.samples[op] = append(w.samples[op], elapsed.Nanoseconds())
	w.mu.Unlock()
}

func (w *workerStats) report() {
	w.mu.Lock()
	defer w.mu.Unlock()
	errs := w.errors.Load()
	fmt.Printf("\n[%s] errors=%d\n", w.name, errs)
	ops := make([]string, 0, len(w.samples))
	for op := range w.samples {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	for _, op := range ops {
		s := append([]int64(nil), w.samples[op]...)
		sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
		if len(s) == 0 {
			continue
		}
		p := func(q float64) time.Duration {
			idx := int(float64(len(s)) * q)
			if idx >= len(s) {
				idx = len(s) - 1
			}
			return time.Duration(s[idx])
		}
		fmt.Printf("  %-8s n=%6d p50=%-9s p95=%-9s p99=%-9s max=%-9s\n",
			op, len(s),
			p(0.50).Round(time.Microsecond),
			p(0.95).Round(time.Microsecond),
			p(0.99).Round(time.Microsecond),
			p(1.0).Round(time.Microsecond))
	}
}

// ---------------------------------------------------------------------
// Metrics endpoint
// ---------------------------------------------------------------------

func fetchMetrics(url string) map[string]any {
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var m map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil
	}
	return m
}

func reportMetricsDelta(before, after map[string]any) {
	if before == nil || after == nil {
		fmt.Println("\n[metrics] endpoint unreachable; no delta")
		return
	}
	fmt.Println("\n[metrics] (after - before)")
	getInt := func(m map[string]any, k string) int64 {
		v, ok := m[k]
		if !ok {
			return 0
		}
		switch x := v.(type) {
		case float64:
			return int64(x)
		case int64:
			return x
		}
		return 0
	}
	rpcDelta := getInt(after, "rpc_total") - getInt(before, "rpc_total")
	errDelta := getInt(after, "rpc_errors") - getInt(before, "rpc_errors")
	byteDelta := getInt(after, "bytes_read") - getInt(before, "bytes_read")
	fmt.Printf("  rpc_total: +%d\n", rpcDelta)
	fmt.Printf("  rpc_errors: +%d\n", errDelta)
	fmt.Printf("  bytes_read: +%d (%.1f MiB)\n", byteDelta, float64(byteDelta)/(1<<20))
}
