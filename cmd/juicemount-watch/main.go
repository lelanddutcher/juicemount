// juicemount-watch polls JuiceMount's admin endpoints at a configurable
// interval and emits a structured JSONL log of all observed values,
// computed deltas, and anomaly flags. It is the OBSERVE half of the
// closed-loop autonomous test infrastructure (jmstress DRIVES load,
// juicemount-watch observes).
//
// Designed to survive:
//
//   - Endpoint outages: a single failed poll emits an "endpoint_down"
//     tick but doesn't terminate the watcher. The next tick retries.
//   - Server restarts: when /metrics comes back after being down,
//     uptime_sec resets to a small value; the watcher treats that as
//     a restart signal and resets its delta baseline rather than
//     emitting nonsense "rpc_total went backwards" deltas.
//   - Partial degradation: each component (/health, /metrics, /offline,
//     /cache-status) is polled and reported independently; one being
//     unreachable doesn't suppress the others.
//
// Anomaly flags (a non-exhaustive list — additive over time):
//
//   - "endpoint_down":          one or more endpoints failed to respond
//   - "auto_offline_engaged":   auto_offline flipped false → true
//   - "auto_offline_recovered": auto_offline flipped true → false
//   - "health_degraded":        /health.healthy flipped true → false
//   - "rpc_error_burst":        rpc_errors_delta > 10 in one tick
//   - "p99_spike":              any RPC's max_us exceeds 2x its
//     previous-tick max (sustained, not
//     single-sample)
//   - "pin_progress_stalled":   pin store has work queued but
//     bytes_pinned hasn't moved in 60s
//
// Usage:
//
//	juicemount-watch \
//	    [--metrics URL]   \  default http://127.0.0.1:11050
//	    [--interval D]    \  default 10s
//	    [--duration D]    \  default 0 (unbounded; SIGINT to stop)
//	    [--out PATH]      \  default stdout
//	    [--alert PATH]    \  optional; touched on first anomaly
//
// Output: JSONL on the chosen sink. One line per tick. Stable schema —
// additions are additive, deletions require a new field.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	var (
		metricsURL = flag.String("metrics", "http://127.0.0.1:11050", "JuiceMount admin endpoint base URL (no trailing slash)")
		interval   = flag.Duration("interval", 10*time.Second, "polling cadence")
		duration   = flag.Duration("duration", 0, "total run duration; 0 = unbounded (SIGINT/SIGTERM to stop)")
		outPath    = flag.String("out", "", "JSONL output path; empty = stdout")
		alertPath  = flag.String("alert", "", "optional: touched once on the first anomaly tick (for cron-style polling)")
	)
	flag.Parse()

	*metricsURL = strings.TrimRight(*metricsURL, "/")

	out := os.Stdout
	if *outPath != "" {
		f, err := os.OpenFile(*outPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open output: %v\n", err)
			os.Exit(2)
		}
		defer f.Close()
		out = f
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if *duration > 0 {
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\nsignal received — shutting down watcher")
		cancel()
	}()

	w := newWatcher(*metricsURL, *alertPath)
	enc := json.NewEncoder(out)

	fmt.Fprintf(os.Stderr, "juicemount-watch: metrics=%s interval=%s duration=%s\n",
		*metricsURL, *interval, *duration)

	// First tick fires immediately so the user sees output without
	// waiting one full interval.
	w.tick(ctx, enc)

	t := time.NewTicker(*interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.tick(ctx, enc)
		}
	}
}

// ---------------------------------------------------------------------
// Watcher
// ---------------------------------------------------------------------

type watcher struct {
	metricsURL string
	alertPath  string
	client     http.Client

	// Previous-tick state for delta computation.
	prev            *snapshot
	alertFired      atomic.Bool
	pinStalledSince time.Time // wall time when bytes_pinned last moved
}

func newWatcher(metricsURL, alertPath string) *watcher {
	return &watcher{
		metricsURL: metricsURL,
		alertPath:  alertPath,
		client:     http.Client{Timeout: 3 * time.Second},
	}
}

// tick runs one full poll across all endpoints, computes anomalies
// against the prev tick's snapshot, and emits one JSONL line.
//
// The four endpoint fetches run in parallel under a shared 3s client
// timeout. Worst-case tick duration is bounded by the slowest single
// endpoint, not the sum of all four (which would have been ~12s and
// piled up against the default 10s polling interval).
func (w *watcher) tick(ctx context.Context, enc *json.Encoder) {
	now := time.Now()
	snap := &snapshot{T: now.Format(time.RFC3339Nano)}

	var wg sync.WaitGroup
	fetch := func(path string, dst *map[string]any) {
		defer wg.Done()
		*dst = w.fetchJSON(ctx, path)
	}
	wg.Add(4)
	go fetch("/metrics", &snap.Metrics)
	go fetch("/health", &snap.Health)
	go fetch("/offline", &snap.Offline)
	go fetch("/cache-status", &snap.Cache)
	wg.Wait()

	// Down-detection: a fetch that returned nil means the endpoint
	// didn't respond. Group these for the anomaly flag.
	down := []string{}
	if snap.Metrics == nil {
		down = append(down, "/metrics")
	}
	if snap.Health == nil {
		down = append(down, "/health")
	}
	if snap.Offline == nil {
		down = append(down, "/offline")
	}
	if snap.Cache == nil {
		down = append(down, "/cache-status")
	}

	// Server-restart detection: uptime_sec going backwards (or
	// dramatically dropping) signals a Start after a Stop. Reset our
	// delta baseline to avoid emitting bogus "rpc_total went down by 1M"
	// deltas.
	if w.prev != nil && snap.Metrics != nil && w.prev.Metrics != nil {
		prevUp := getNumber(w.prev.Metrics, "uptime_sec")
		curUp := getNumber(snap.Metrics, "uptime_sec")
		if curUp < prevUp || (prevUp > 60 && curUp < 10) {
			w.prev = nil // baseline reset
		}
	}

	// Anomaly detection. Each detector returns "" if no anomaly,
	// otherwise a short flag name.
	anomalies := []string{}
	if len(down) > 0 {
		anomalies = append(anomalies, "endpoint_down")
	}
	if a := w.detectAutoOffline(snap); a != "" {
		anomalies = append(anomalies, a)
	}
	if a := w.detectHealthDegraded(snap); a != "" {
		anomalies = append(anomalies, a)
	}
	if a := w.detectRPCErrorBurst(snap); a != "" {
		anomalies = append(anomalies, a)
	}
	if a := w.detectP99Spike(snap); a != "" {
		anomalies = append(anomalies, a)
	}
	if a := w.detectPinStalled(snap, now); a != "" {
		anomalies = append(anomalies, a)
	}

	// Deltas for the eyeballable fields.
	var delta *snapshotDelta
	if w.prev != nil {
		delta = computeDelta(w.prev, snap)
	}

	tickOut := tickRecord{
		T:         snap.T,
		Down:      down,
		Anomalies: anomalies,
		Metrics:   snap.Metrics,
		Health:    snap.Health,
		Offline:   snap.Offline,
		Cache:     snap.Cache,
		Delta:     delta,
	}

	if err := enc.Encode(tickOut); err != nil {
		fmt.Fprintf(os.Stderr, "encode error: %v\n", err)
	}

	// Write the alert file on the first anomaly tick. Hybrid sentinel
	// + first-incident log: cron-style watchers can test for file
	// existence as the "this run hit an anomaly" signal, AND can cat
	// the file to learn WHAT the first anomaly was. We deliberately
	// don't re-touch on subsequent anomalies — the JSONL log captures
	// the full timeseries; the alert file is the canonical "first
	// notification" artifact.
	if len(anomalies) > 0 && w.alertPath != "" {
		if w.alertFired.CompareAndSwap(false, true) {
			if f, err := os.Create(w.alertPath); err == nil {
				fmt.Fprintf(f, "%s: %s\n", snap.T, strings.Join(anomalies, ","))
				f.Close()
			}
		}
	}

	w.prev = snap
}

// fetchJSON pulls a single endpoint and returns the decoded body, or
// nil on any failure. Errors are silent at this layer — the caller
// detects nil and folds it into the "endpoint_down" anomaly.
func (w *watcher) fetchJSON(ctx context.Context, path string) map[string]any {
	req, err := http.NewRequestWithContext(ctx, "GET", w.metricsURL+path, nil)
	if err != nil {
		return nil
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 && resp.StatusCode != 503 {
		// 503 is acceptable (the health endpoint returns it on
		// degraded state — we still want to read the body to know
		// WHY it's degraded).
		return nil
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil
	}
	return m
}

// ---------------------------------------------------------------------
// Anomaly detectors
// ---------------------------------------------------------------------

func (w *watcher) detectAutoOffline(snap *snapshot) string {
	if w.prev == nil || snap.Offline == nil || w.prev.Offline == nil {
		return ""
	}
	prevAuto, _ := w.prev.Offline["auto_offline"].(bool)
	curAuto, _ := snap.Offline["auto_offline"].(bool)
	if !prevAuto && curAuto {
		return "auto_offline_engaged"
	}
	if prevAuto && !curAuto {
		return "auto_offline_recovered"
	}
	return ""
}

func (w *watcher) detectHealthDegraded(snap *snapshot) string {
	if w.prev == nil || snap.Health == nil || w.prev.Health == nil {
		return ""
	}
	prevHealthy, _ := w.prev.Health["healthy"].(bool)
	curHealthy, _ := snap.Health["healthy"].(bool)
	if prevHealthy && !curHealthy {
		return "health_degraded"
	}
	if !prevHealthy && curHealthy {
		return "health_recovered"
	}
	return ""
}

// detectRPCErrorBurst returns a non-empty flag when rpc_errors grew
// by more than 10 in a single tick. Distinguishes "occasional
// transient" from "we're suddenly seeing lots of errors."
func (w *watcher) detectRPCErrorBurst(snap *snapshot) string {
	if w.prev == nil || snap.Metrics == nil || w.prev.Metrics == nil {
		return ""
	}
	cur := getNumber(snap.Metrics, "rpc_errors")
	prev := getNumber(w.prev.Metrics, "rpc_errors")
	if cur-prev > 10 {
		return "rpc_error_burst"
	}
	return ""
}

// detectP99Spike returns a non-empty flag when ANY RPC's p99_us in
// this tick exceeds 2x its p99_us in the prev tick. A single slow
// outlier is normal noise; "this RPC's 99th percentile just doubled"
// is a leading indicator of degradation.
//
// IMPORTANT: we use p99_us, NOT max_us. max_us is a running max
// maintained server-side that only ever grows, so a tick-vs-tick
// comparison of max_us only ever fires on the first occurrence of a
// new record — and then never again, even if the RPC is sustained-
// degraded at the new level. p99_us is a percentile over the metric
// window (resets each measurement cycle) so it tracks current state
// rather than all-time worst.
func (w *watcher) detectP99Spike(snap *snapshot) string {
	if w.prev == nil || snap.Metrics == nil || w.prev.Metrics == nil {
		return ""
	}
	prevRPCs, ok1 := w.prev.Metrics["rpcs"].(map[string]any)
	curRPCs, ok2 := snap.Metrics["rpcs"].(map[string]any)
	if !ok1 || !ok2 {
		return ""
	}
	for name, raw := range curRPCs {
		cur, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		prev, ok := prevRPCs[name].(map[string]any)
		if !ok {
			continue
		}
		curP99 := getFloat(cur, "p99_us")
		prevP99 := getFloat(prev, "p99_us")
		// Require both a meaningful previous baseline (>1ms) AND
		// non-trivial absolute current (>1ms) to filter out noise at
		// near-zero latencies where 2x means microseconds.
		if prevP99 > 1000 && curP99 > 1000 && curP99 > 2*prevP99 {
			return "p99_spike:" + name
		}
	}
	return ""
}

// detectPinStalled flags when /cache-status shows pinned work
// outstanding but CachedBytes hasn't moved in 60s. Direct repro
// signal for QA-1 and QA-5 (pin enqueue not draining).
//
// Schema: /cache-status returns NFSServerCacheStatus → has an
// `aggregate` nested object with PascalCase fields (no json tags on
// pin.AggregateStats). We read aggregate.PendingFiles as the "is
// there work?" signal and aggregate.CachedBytes as the progress
// counter. Falls through to "" when fields are absent (e.g. /cache-
// status was unreachable) — the endpoint_down flag handles outages.
//
// Also resets pinStalledSince when snap.Cache is nil so an endpoint
// outage doesn't accumulate apparent-stall time that fires a
// spurious flag on recovery.
func (w *watcher) detectPinStalled(snap *snapshot, now time.Time) string {
	if snap.Cache == nil {
		// Endpoint down — can't observe pin progress. Reset clock so
		// the outage window doesn't count toward the stall threshold.
		w.pinStalledSince = time.Time{}
		return ""
	}
	agg, ok := snap.Cache["aggregate"].(map[string]any)
	if !ok {
		// Schema mismatch — fail-quiet rather than firing.
		w.pinStalledSince = time.Time{}
		return ""
	}
	queue := getNumber(agg, "pending_files")
	if queue <= 0 {
		// Nothing to pin → not stalled.
		w.pinStalledSince = time.Time{}
		return ""
	}
	curBytes := getNumber(agg, "cached_bytes")
	if w.prev == nil || w.prev.Cache == nil {
		w.pinStalledSince = now
		return ""
	}
	prevAgg, ok := w.prev.Cache["aggregate"].(map[string]any)
	if !ok {
		w.pinStalledSince = now
		return ""
	}
	prevBytes := getNumber(prevAgg, "cached_bytes")
	if curBytes > prevBytes {
		// Pin store made progress — reset the stall clock.
		w.pinStalledSince = now
		return ""
	}
	if w.pinStalledSince.IsZero() {
		w.pinStalledSince = now
		return ""
	}
	if now.Sub(w.pinStalledSince) > 60*time.Second {
		return "pin_progress_stalled"
	}
	return ""
}

// ---------------------------------------------------------------------
// Snapshot + delta types
// ---------------------------------------------------------------------

type snapshot struct {
	T       string         `json:"t"`
	Metrics map[string]any `json:"metrics,omitempty"`
	Health  map[string]any `json:"health,omitempty"`
	Offline map[string]any `json:"offline,omitempty"`
	Cache   map[string]any `json:"cache,omitempty"`
}

type tickRecord struct {
	T         string         `json:"t"`
	Down      []string       `json:"down,omitempty"`
	Anomalies []string       `json:"anomalies,omitempty"`
	Metrics   map[string]any `json:"metrics,omitempty"`
	Health    map[string]any `json:"health,omitempty"`
	Offline   map[string]any `json:"offline,omitempty"`
	Cache     map[string]any `json:"cache,omitempty"`
	Delta     *snapshotDelta `json:"delta,omitempty"`
}

// snapshotDelta surfaces the human-eyeballable deltas between ticks.
// Long enough to be useful for tail -f juicemount-watch.jsonl | jq;
// short enough to not clutter the per-tick output.
type snapshotDelta struct {
	IntervalSec       float64 `json:"interval_sec"`
	RPCTotalDelta     int64   `json:"rpc_total_delta,omitempty"`
	RPCErrorsDelta    int64   `json:"rpc_errors_delta,omitempty"`
	BytesReadDelta    int64   `json:"bytes_read_delta,omitempty"`
	BytesWrittenDelta int64   `json:"bytes_written_delta,omitempty"`
	BytesPinnedDelta  int64   `json:"bytes_pinned_delta,omitempty"`
}

func computeDelta(prev, cur *snapshot) *snapshotDelta {
	if prev == nil || cur == nil {
		return nil
	}
	prevT, errP := time.Parse(time.RFC3339Nano, prev.T)
	curT, errC := time.Parse(time.RFC3339Nano, cur.T)
	if errP != nil || errC != nil {
		return nil
	}
	d := &snapshotDelta{
		IntervalSec: curT.Sub(prevT).Seconds(),
	}
	if prev.Metrics != nil && cur.Metrics != nil {
		d.RPCTotalDelta = getNumber(cur.Metrics, "rpc_total") - getNumber(prev.Metrics, "rpc_total")
		d.RPCErrorsDelta = getNumber(cur.Metrics, "rpc_errors") - getNumber(prev.Metrics, "rpc_errors")
		d.BytesReadDelta = getNumber(cur.Metrics, "bytes_read") - getNumber(prev.Metrics, "bytes_read")
		d.BytesWrittenDelta = getNumber(cur.Metrics, "bytes_written") - getNumber(prev.Metrics, "bytes_written")
	}
	if prev.Cache != nil && cur.Cache != nil {
		curAgg, ok1 := cur.Cache["aggregate"].(map[string]any)
		prevAgg, ok2 := prev.Cache["aggregate"].(map[string]any)
		if ok1 && ok2 {
			d.BytesPinnedDelta = getNumber(curAgg, "CachedBytes") - getNumber(prevAgg, "cached_bytes")
		}
	}
	return d
}

// getNumber extracts a numeric field from a JSON map, returning 0 if
// missing or wrong type. JSON numbers decode as float64.
func getNumber(m map[string]any, key string) int64 {
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
	case int:
		return int64(x)
	}
	return 0
}

// getFloat preserves float64 precision (vs getNumber's int64 cast).
// Useful for percentile fields like p99_us where sub-microsecond
// precision can matter in the 2x-spike comparison.
func getFloat(m map[string]any, key string) float64 {
	if m == nil {
		return 0
	}
	v, ok := m[key]
	if !ok {
		return 0
	}
	switch x := v.(type) {
	case float64:
		return x
	case int64:
		return float64(x)
	case int:
		return float64(x)
	}
	return 0
}
