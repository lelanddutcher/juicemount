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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
		jsonOut        = flag.Bool("json", false, "emit a single JSON summary on completion (stdout). Human-readable output goes to stderr in this mode.")
		periodicJSON   = flag.Duration("periodic-json", 0, "if >0, emit a JSON snapshot of running stats every N (default 0 = only at end). Useful for soak runs where you want a timeseries to graph degradation over hours.")
		// Goroutine-leak watchdog (tier-1 iter D, hardens 1.6). Polls the
		// JuiceMount /debug/pprof/goroutine endpoint and asserts the count
		// stays within a multiple of the first-tick baseline. Catches the
		// failure class where latency + errors look healthy but goroutines
		// ramp unbounded — a soak-test signal that pure latency tracking
		// would miss.
		goroutineCheck   = flag.Bool("goroutine-check", true, "poll /debug/pprof/goroutine and flag unbounded growth (default true; disable with --goroutine-check=false)")
		// Default 3.0 (not 1.5) because empirical smoke runs show natural
		// NLE-worker variance of ~3x baseline as files are opened, drained,
		// and closed in sequence — each cycle spawns transient goroutines.
		// 1.5x would false-positive on healthy workloads. 3.0x still catches
		// the leak class we care about: sustained monotonic growth over
		// hours. Users tracking stricter thresholds can tighten via flag.
		goroutineMult    = flag.Float64("goroutine-multiplier", 3.0, "max allowed goroutine count as a multiple of the post-warmup baseline; breaches surface in JSON snapshots and as a non-zero exit")
		goroutineTick    = flag.Duration("goroutine-tick", 30*time.Second, "how often to sample the goroutine endpoint after warmup")
		goroutineWarmup  = flag.Duration("goroutine-warmup", 5*time.Minute, "delay before the watchdog starts ticking; lets workers reach steady state so baseline isn't biased by ramp-up. Set to 0 to start immediately (useful for smoke tests; not for 24h soaks).")
	)
	flag.Parse()

	rng := rand.New(rand.NewSource(*seed))

	// When --json is set, stdout is reserved for machine-parseable
	// output and human-readable progress goes to stderr. Otherwise both
	// go to stdout. This lets `jmstress --json | jq` work cleanly.
	humanOut := os.Stdout
	if *jsonOut {
		humanOut = os.Stderr
	}
	fmt.Fprintf(humanOut, "jmstress: mount=%s duration=%s finder=%d nle=%d backup=%d seed=%d\n",
		*mount, *duration, *finderWorkers, *nleWorkers, *backupWorkers, *seed)

	// Sanity: confirm mount is reachable.
	if _, err := os.Stat(*mount); err != nil {
		fmt.Fprintf(os.Stderr, "mount unreachable: %v\n", err)
		os.Exit(2)
	}

	// Discover the path pool once. Bounded depth so a huge tree doesn't
	// stall startup. We also collect "large files" for the NLE worker.
	fmt.Fprintf(humanOut, "discovering paths (depth %d)...\n", *discoveryDepth)
	t0 := time.Now()
	pool, largeFiles, derr := discoverPool(*mount, *discoveryDepth, *largeFileMin*1024*1024)
	if derr != nil {
		fmt.Fprintf(os.Stderr, "discovery error: %v\n", derr)
	}
	fmt.Fprintf(humanOut, "discovery: %d dirs/files, %d large files (>%dMiB), took %s\n",
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

	// Goroutine watchdog. Derives the pprof URL from --metrics-url so
	// users don't need a separate flag. Probes once now to fail fast
	// if pprof isn't wired up; the actual baseline is taken later by
	// the ticker (see worker loop below) so it reflects steady-state
	// goroutine count, not the spike during discovery.
	gwd := newGoroutineWatchdog(*metricsURL, *goroutineMult)
	if *goroutineCheck {
		if err := gwd.probe(); err != nil {
			fmt.Fprintf(humanOut, "goroutine watchdog disabled: %v\n", err)
		}
	}

	// Honor SIGINT/SIGTERM for graceful early exit.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintln(humanOut, "\nsignal received — winding down")
		cancel()
	}()

	var wg sync.WaitGroup
	finderStats := newWorkerStats("finder")
	nleStats := newWorkerStats("nle")
	backupStats := newWorkerStats("backup")

	// Periodic JSON snapshots while the run is in flight. Stops when
	// the context cancels. Snapshots are append-only on stdout when
	// --json is set, so a 24h soak produces a parseable timeseries.
	startTime := time.Now()
	if *jsonOut && *periodicJSON > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(*periodicJSON)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case now := <-t.C:
					snap := snapshot{
						T:          now.Format(time.RFC3339Nano),
						ElapsedS:   now.Sub(startTime).Seconds(),
						Type:       "tick",
						Finder:     finderStats.exportSnapshot(),
						NLE:        nleStats.exportSnapshot(),
						Backup:     backupStats.exportSnapshot(),
						Goroutines: gwd.exportSnapshot(),
					}
					if err := json.NewEncoder(os.Stdout).Encode(snap); err != nil {
						fmt.Fprintf(os.Stderr, "json encode error: %v\n", err)
					}
				}
			}
		}()
	}

	// Goroutine watchdog ticker. Independent of the periodic-JSON ticker
	// so users can run the watchdog at 30s and JSON snapshots at 60s (or
	// not at all). Sleeps for --goroutine-warmup before first tick so
	// the baseline reflects steady-state worker activity, not the
	// goroutine spike during NLE/backup worker ramp-up.
	if *goroutineCheck && gwd.enabled() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if *goroutineWarmup > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(*goroutineWarmup):
				}
			}
			t := time.NewTicker(*goroutineTick)
			defer t.Stop()
			// Take the first tick immediately after warmup elapses,
			// rather than waiting an additional --goroutine-tick.
			if breach := gwd.tick(); breach != nil {
				fmt.Fprintf(humanOut, "[watchdog] goroutine breach: count=%d baseline=%d ratio=%.2fx (threshold %.2fx)\n",
					breach.count, breach.baseline, breach.ratio, *goroutineMult)
			}
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if breach := gwd.tick(); breach != nil {
						fmt.Fprintf(humanOut, "[watchdog] goroutine breach: count=%d baseline=%d ratio=%.2fx (threshold %.2fx)\n",
							breach.count, breach.baseline, breach.ratio, *goroutineMult)
					}
				}
			}
		}()
	}

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

	// Human-readable summary always renders (to humanOut: stdout normally,
	// stderr in --json mode so it doesn't corrupt the JSON channel).
	fmt.Fprintln(humanOut, "\n=== results ===")
	finderStats.reportTo(humanOut)
	nleStats.reportTo(humanOut)
	backupStats.reportTo(humanOut)

	// Metrics delta.
	afterMetrics := fetchMetrics(*metricsURL)
	reportMetricsDeltaTo(humanOut, beforeMetrics, afterMetrics)

	// Final JSON summary on stdout when --json is set. This is the
	// authoritative result for soak runs — periodic ticks are useful for
	// graphing, but this is the row that says "did the run pass."
	if *jsonOut {
		summary := snapshot{
			T:          time.Now().Format(time.RFC3339Nano),
			ElapsedS:   time.Since(startTime).Seconds(),
			Type:       "final",
			Finder:     finderStats.exportSnapshot(),
			NLE:        nleStats.exportSnapshot(),
			Backup:     backupStats.exportSnapshot(),
			Goroutines: gwd.exportSnapshot(),
			Metrics: &metricsDelta{
				RPCTotalDelta:  fieldDelta(beforeMetrics, afterMetrics, "rpc_total"),
				RPCErrorsDelta: fieldDelta(beforeMetrics, afterMetrics, "rpc_errors"),
				BytesReadDelta: fieldDelta(beforeMetrics, afterMetrics, "bytes_read"),
			},
		}
		if err := json.NewEncoder(os.Stdout).Encode(summary); err != nil {
			fmt.Fprintf(os.Stderr, "json encode error: %v\n", err)
			os.Exit(1)
		}
	}

	// If the watchdog detected unbounded growth at any tick, the soak
	// "passed" by latency/error tests but failed by leak detection.
	// Exit non-zero so CI gates surface the breach.
	if gwd.breached() {
		fmt.Fprintln(humanOut, "[watchdog] goroutine watchdog recorded one or more breaches — exiting non-zero")
		os.Exit(1)
	}
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
	w.reportTo(os.Stdout)
}

func (w *workerStats) reportTo(out *os.File) {
	w.mu.Lock()
	defer w.mu.Unlock()
	errs := w.errors.Load()
	fmt.Fprintf(out, "\n[%s] errors=%d\n", w.name, errs)
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
		fmt.Fprintf(out, "  %-8s n=%6d p50=%-9s p95=%-9s p99=%-9s max=%-9s\n",
			op, len(s),
			p(0.50).Round(time.Microsecond),
			p(0.95).Round(time.Microsecond),
			p(0.99).Round(time.Microsecond),
			p(1.0).Round(time.Microsecond))
	}
}

// exportSnapshot returns a serializable view of the worker's current
// counters. Per-op samples are summarized as the same percentile shape
// as the human report. Safe to call concurrently with record() — takes
// w.mu and copies the samples slice before computing.
func (w *workerStats) exportSnapshot() workerSnapshot {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := workerSnapshot{
		Name:   w.name,
		Errors: w.errors.Load(),
		Ops:    make(map[string]opSnapshot, len(w.samples)),
	}
	for op, raw := range w.samples {
		if len(raw) == 0 {
			continue
		}
		s := append([]int64(nil), raw...)
		sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
		p := func(q float64) int64 {
			idx := int(float64(len(s)) * q)
			if idx >= len(s) {
				idx = len(s) - 1
			}
			return s[idx]
		}
		out.Ops[op] = opSnapshot{
			N:        int64(len(s)),
			P50Ns:    p(0.50),
			P95Ns:    p(0.95),
			P99Ns:    p(0.99),
			MaxNs:    p(1.0),
			MeanNs:   meanInt64(s),
		}
	}
	return out
}

func meanInt64(s []int64) int64 {
	if len(s) == 0 {
		return 0
	}
	var sum int64
	for _, v := range s {
		sum += v
	}
	return sum / int64(len(s))
}

// snapshot is the JSON shape emitted on tick and at end of run. The
// "type" field is "tick" for periodic snapshots and "final" for the
// terminating summary. Soaks that need a timeseries will see many
// ticks followed by one final. The schema is stable — additions are
// additive; deletions require a new field.
type snapshot struct {
	T          string                `json:"t"`         // RFC3339Nano timestamp
	ElapsedS   float64               `json:"elapsed_s"` // seconds since run start
	Type       string                `json:"type"`      // "tick" | "final"
	Finder     workerSnapshot        `json:"finder"`
	NLE        workerSnapshot        `json:"nle"`
	Backup     workerSnapshot        `json:"backup"`
	Goroutines *goroutineSnapshot    `json:"goroutines,omitempty"`
	Metrics    *metricsDelta         `json:"metrics,omitempty"`
}

type workerSnapshot struct {
	Name   string                  `json:"name"`
	Errors int64                   `json:"errors"`
	Ops    map[string]opSnapshot   `json:"ops"`
}

type opSnapshot struct {
	N      int64 `json:"n"`
	P50Ns  int64 `json:"p50_ns"`
	P95Ns  int64 `json:"p95_ns"`
	P99Ns  int64 `json:"p99_ns"`
	MaxNs  int64 `json:"max_ns"`
	MeanNs int64 `json:"mean_ns"`
}

type metricsDelta struct {
	RPCTotalDelta  int64 `json:"rpc_total_delta"`
	RPCErrorsDelta int64 `json:"rpc_errors_delta"`
	BytesReadDelta int64 `json:"bytes_read_delta"`
}

func fieldDelta(before, after map[string]any, key string) int64 {
	get := func(m map[string]any) int64 {
		if m == nil {
			return 0
		}
		v, ok := m[key]
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
	return get(after) - get(before)
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
	reportMetricsDeltaTo(os.Stdout, before, after)
}

func reportMetricsDeltaTo(out *os.File, before, after map[string]any) {
	if before == nil || after == nil {
		fmt.Fprintln(out, "\n[metrics] endpoint unreachable; no delta")
		return
	}
	fmt.Fprintln(out, "\n[metrics] (after - before)")
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
	fmt.Fprintf(out, "  rpc_total: +%d\n", rpcDelta)
	fmt.Fprintf(out, "  rpc_errors: +%d\n", errDelta)
	fmt.Fprintf(out, "  bytes_read: +%d (%.1f MiB)\n", byteDelta, float64(byteDelta)/(1<<20))
}

// ---------------------------------------------------------------------
// Goroutine watchdog (tier-1 iter D, hardens 1.6)
// ---------------------------------------------------------------------

// goroutineWatchdog polls /debug/pprof/goroutine?debug=1 on a schedule
// and detects unbounded goroutine growth. The failure class it catches:
// a soak where latency and errors stay clean but the server leaks a
// goroutine per RPC, sliding into RSS-pressure death over hours. Pure
// latency tracking misses this until it cascades; this catches the
// linear growth at every tick.
//
// Lifecycle: caller constructs with newGoroutineWatchdog(metricsURL,
// multiplier), then calls prime() once to record the baseline. tick()
// fires from a ticker goroutine and returns a non-nil breach when the
// current count exceeds baseline*multiplier. The state is all atomic
// so exportSnapshot() is safe to call from the periodic-JSON goroutine
// without coordination.
type goroutineWatchdog struct {
	pprofURL   string
	multiplier float64
	client     http.Client

	armed    atomic.Bool   // true once probe() succeeds; ticker only runs when armed
	baseline atomic.Int64  // 0 until first successful tick() captures it
	current  atomic.Int64
	max      atomic.Int64
	ticks    atomic.Int64
	breaches atomic.Int64
}

type goroutineBreach struct {
	count    int64
	baseline int64
	ratio    float64
}

type goroutineSnapshot struct {
	Baseline int64   `json:"baseline"`
	Current  int64   `json:"current"`
	Max      int64   `json:"max"`
	Ticks    int64   `json:"ticks"`
	Breaches int64   `json:"breaches"`
	Ratio    float64 `json:"ratio"`         // current / baseline
	MaxRatio float64 `json:"max_ratio"`     // max / baseline
}

func newGoroutineWatchdog(metricsURL string, multiplier float64) *goroutineWatchdog {
	pprofURL := derivePprofURL(metricsURL)
	return &goroutineWatchdog{
		pprofURL:   pprofURL,
		multiplier: multiplier,
		client:     http.Client{Timeout: 2 * time.Second},
	}
}

// derivePprofURL returns the goroutine pprof endpoint URL by replacing
// the path component of metricsURL. Returns "" on parse failure — the
// watchdog will then report disabled.
func derivePprofURL(metricsURL string) string {
	u, err := url.Parse(metricsURL)
	if err != nil || u.Host == "" {
		return ""
	}
	u.Path = "/debug/pprof/goroutine"
	u.RawQuery = "debug=1"
	return u.String()
}

// probe is a fail-fast reachability check for the pprof endpoint.
// Caller runs it before starting the watchdog ticker; if probe fails,
// log and disable rather than aborting the whole run. Probe does NOT
// set the baseline — that's tick()'s job on its first call, so the
// baseline reflects steady-state goroutine count (after workers warm
// up) rather than the spike during discovery.
func (g *goroutineWatchdog) probe() error {
	if g.pprofURL == "" {
		return fmt.Errorf("invalid metrics URL — cannot derive pprof endpoint")
	}
	count, err := g.fetch()
	if err != nil {
		return err
	}
	// Mark enabled by setting an "armed" sentinel — first tick replaces
	// this with the real baseline. Initialize max to the probe count so
	// any snapshot emitted before the first tick fires shows
	// max >= current (avoids confusing JSON readers in the warmup window).
	g.armed.Store(true)
	g.current.Store(count)
	g.max.Store(count)
	return nil
}

// enabled returns true if the watchdog was successfully probed and
// should be polled. False if probe() failed.
func (g *goroutineWatchdog) enabled() bool {
	return g.armed.Load()
}

// tick samples the endpoint, updates state, and returns a non-nil
// breach when the current count exceeds baseline*multiplier. A tick
// that hits a transient HTTP error is silently skipped (don't count
// as a breach — we have no current observation).
//
// The FIRST successful tick stores the baseline. This is deliberately
// AFTER the ticker's first delay (default 30s) so the baseline reflects
// steady-state worker activity rather than the goroutine spike during
// discoverPool's filesystem walk.
func (g *goroutineWatchdog) tick() *goroutineBreach {
	count, err := g.fetch()
	if err != nil {
		return nil
	}
	// Defend against a malformed pprof body that parses as "total 0".
	// Storing 0 as baseline would cause every subsequent tick to divide
	// through zero in exportSnapshot and (with the current logic)
	// register as a breach. Skip the tick instead.
	if count <= 0 {
		return nil
	}
	g.ticks.Add(1)
	g.current.Store(count)
	for {
		old := g.max.Load()
		if count <= old {
			break
		}
		if g.max.CompareAndSwap(old, count) {
			break
		}
	}
	// First tick after probing: capture the baseline. CompareAndSwap so
	// we only set it once even if multiple tickers raced (they don't
	// today, but cheap insurance).
	if g.baseline.Load() == 0 {
		g.baseline.CompareAndSwap(0, count)
		return nil
	}
	baseline := g.baseline.Load()
	threshold := float64(baseline) * g.multiplier
	if float64(count) > threshold {
		g.breaches.Add(1)
		return &goroutineBreach{
			count:    count,
			baseline: baseline,
			ratio:    float64(count) / float64(baseline),
		}
	}
	return nil
}

// breached reports whether any tick has exceeded the threshold during
// the run. Used by the main goroutine to set exit code.
func (g *goroutineWatchdog) breached() bool {
	return g.breaches.Load() > 0
}

// exportSnapshot returns a JSON-serializable view of the watchdog
// state. Returns nil if the watchdog was never probed (so JSON
// snapshots omit the field cleanly via omitempty). If probed but no
// baseline yet (i.e. we're between probe and the first ticker fire),
// returns a snapshot with current/max populated but baseline=0 and
// the ratios omitted.
func (g *goroutineWatchdog) exportSnapshot() *goroutineSnapshot {
	if !g.armed.Load() {
		return nil
	}
	current := g.current.Load()
	max := g.max.Load()
	baseline := g.baseline.Load()
	snap := &goroutineSnapshot{
		Baseline: baseline,
		Current:  current,
		Max:      max,
		Ticks:    g.ticks.Load(),
		Breaches: g.breaches.Load(),
	}
	if baseline > 0 {
		snap.Ratio = float64(current) / float64(baseline)
		snap.MaxRatio = float64(max) / float64(baseline)
	}
	return snap
}

// fetch reads the goroutine count from the pprof endpoint. The
// endpoint's first non-empty line has the form:
//
//	goroutine profile: total 40
//
// We parse the trailing integer. Anything else is treated as an error.
func (g *goroutineWatchdog) fetch() (int64, error) {
	resp, err := g.client.Get(g.pprofURL)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("pprof endpoint returned status %d", resp.StatusCode)
	}
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		// Expected: "goroutine profile: total NN"
		const prefix = "goroutine profile: total "
		if !strings.HasPrefix(line, prefix) {
			return 0, fmt.Errorf("unexpected pprof header: %q", line)
		}
		count, err := strconv.ParseInt(strings.TrimSpace(line[len(prefix):]), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse goroutine count: %w", err)
		}
		return count, nil
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("empty pprof response")
}
