// jmcompare reads two `jmstress --json` output files (before.jsonl and
// after.jsonl) and reports the per-worker, per-operation deltas in
// latency percentiles, throughput, and error counts. It's the
// analytical companion to jmstress: stress generates the data; this
// turns "did we improve?" into a yes/no with numbers.
//
// Each input is expected to be a JSON-lines file produced by
// `jmstress --json` (and optionally `--periodic-json`). Only the
// "type":"final" entry is used for comparison; ticks are ignored.
//
// Usage:
//
//	jmcompare before.jsonl after.jsonl
//	jmcompare --json before.jsonl after.jsonl > diff.json
//	jmcompare --threshold-p99-regression-pct 10 before.jsonl after.jsonl
//
// Exit codes:
//
//	0  before and after both parse; no regression beyond threshold
//	1  regression detected (any p99 worse by more than threshold, OR
//	   new errors appeared, OR a worker that was healthy now errors)
//	2  parse error / missing file / no "final" entry
//
// The threshold flag is intentionally simple: a single p99 regression
// cap that applies to every (worker, op) pair. The non-zero exit lets
// CI gate merges on "the new code doesn't make Finder worse."
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"time"
)

// Schema mirrors cmd/jmstress's emit. Kept here as a local copy because
// jmcompare is meant to be runnable against historical JSON files,
// including ones produced by future jmstress versions that may evolve
// the struct. Local copy → forward-compat via missing fields = zero.

type snapshot struct {
	T        string         `json:"t"`
	ElapsedS float64        `json:"elapsed_s"`
	Type     string         `json:"type"`
	Finder   workerSnapshot `json:"finder"`
	NLE      workerSnapshot `json:"nle"`
	Backup   workerSnapshot `json:"backup"`
	Metrics  *metricsDelta  `json:"metrics,omitempty"`
}

type workerSnapshot struct {
	Name   string                `json:"name"`
	Errors int64                 `json:"errors"`
	Ops    map[string]opSnapshot `json:"ops"`
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

func main() {
	var (
		jsonOut          = flag.Bool("json", false, "emit the diff as JSON instead of human-readable")
		p99RegressionPct = flag.Float64("threshold-p99-regression-pct", 0, "exit 1 if any (worker, op) p99 regressed by more than this percent. 0 = warn only.")
	)
	flag.Parse()

	args := flag.Args()
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: jmcompare [flags] before.jsonl after.jsonl")
		flag.PrintDefaults()
		os.Exit(2)
	}

	beforePath := args[0]
	afterPath := args[1]

	before, err := loadFinal(beforePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading %s: %v\n", beforePath, err)
		os.Exit(2)
	}
	after, err := loadFinal(afterPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading %s: %v\n", afterPath, err)
		os.Exit(2)
	}

	diff := diffSnapshots(before, after)

	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(diff); err != nil {
			fmt.Fprintf(os.Stderr, "json encode error: %v\n", err)
			os.Exit(2)
		}
	} else {
		printHumanDiff(os.Stdout, diff, beforePath, afterPath)
	}

	// Threshold gate. Returns non-zero if any p99 regressed past the
	// threshold OR any new error appeared in a worker that was clean.
	if regressionExceedsThreshold(diff, *p99RegressionPct) {
		os.Exit(1)
	}
}

// loadFinal reads a JSON-lines file and returns the last entry whose
// "type" is "final". Tick entries are ignored. Caller error if no
// final entry is present.
func loadFinal(path string) (snapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return snapshot{}, err
	}
	defer f.Close()

	var final snapshot
	var foundFinal bool

	dec := json.NewDecoder(f)
	for dec.More() {
		var s snapshot
		if err := dec.Decode(&s); err != nil {
			return snapshot{}, fmt.Errorf("decode: %w", err)
		}
		if s.Type == "final" {
			final = s
			foundFinal = true
		}
	}
	if !foundFinal {
		return snapshot{}, fmt.Errorf("no entry with type=final in %s", path)
	}
	return final, nil
}

// ---------------------------------------------------------------------
// Diff types
// ---------------------------------------------------------------------

type comparisonDiff struct {
	Before  snapshotMeta  `json:"before"`
	After   snapshotMeta  `json:"after"`
	Workers []workerDiff  `json:"workers"`
	Metrics *metricsDiff  `json:"metrics,omitempty"`
}

type snapshotMeta struct {
	Timestamp string  `json:"t"`
	ElapsedS  float64 `json:"elapsed_s"`
}

type workerDiff struct {
	Name        string    `json:"name"`
	ErrorsDelta int64     `json:"errors_delta"`
	Ops         []opDiff  `json:"ops"`
}

type opDiff struct {
	Op          string  `json:"op"`
	NBefore     int64   `json:"n_before"`
	NAfter      int64   `json:"n_after"`
	P50Pct      float64 `json:"p50_pct"`     // % change; positive = worse
	P95Pct      float64 `json:"p95_pct"`
	P99Pct      float64 `json:"p99_pct"`
	MaxPct      float64 `json:"max_pct"`
	P50BeforeNs int64   `json:"p50_before_ns"`
	P50AfterNs  int64   `json:"p50_after_ns"`
	P99BeforeNs int64   `json:"p99_before_ns"`
	P99AfterNs  int64   `json:"p99_after_ns"`
}

type metricsDiff struct {
	RPCTotalBefore  int64 `json:"rpc_total_before"`
	RPCTotalAfter   int64 `json:"rpc_total_after"`
	RPCErrorsBefore int64 `json:"rpc_errors_before"`
	RPCErrorsAfter  int64 `json:"rpc_errors_after"`
	BytesReadBefore int64 `json:"bytes_read_before"`
	BytesReadAfter  int64 `json:"bytes_read_after"`
}

// ---------------------------------------------------------------------
// Diff computation
// ---------------------------------------------------------------------

func diffSnapshots(before, after snapshot) comparisonDiff {
	d := comparisonDiff{
		Before: snapshotMeta{Timestamp: before.T, ElapsedS: before.ElapsedS},
		After:  snapshotMeta{Timestamp: after.T, ElapsedS: after.ElapsedS},
	}

	for _, name := range []string{"finder", "nle", "backup"} {
		var b, a workerSnapshot
		switch name {
		case "finder":
			b, a = before.Finder, after.Finder
		case "nle":
			b, a = before.NLE, after.NLE
		case "backup":
			b, a = before.Backup, after.Backup
		}
		d.Workers = append(d.Workers, workerDiff{
			Name:        name,
			ErrorsDelta: a.Errors - b.Errors,
			Ops:         diffOps(b.Ops, a.Ops),
		})
	}

	if before.Metrics != nil || after.Metrics != nil {
		bd := metricsDelta{}
		ad := metricsDelta{}
		if before.Metrics != nil {
			bd = *before.Metrics
		}
		if after.Metrics != nil {
			ad = *after.Metrics
		}
		d.Metrics = &metricsDiff{
			RPCTotalBefore:  bd.RPCTotalDelta,
			RPCTotalAfter:   ad.RPCTotalDelta,
			RPCErrorsBefore: bd.RPCErrorsDelta,
			RPCErrorsAfter:  ad.RPCErrorsDelta,
			BytesReadBefore: bd.BytesReadDelta,
			BytesReadAfter:  ad.BytesReadDelta,
		}
	}

	return d
}

func diffOps(before, after map[string]opSnapshot) []opDiff {
	allOps := map[string]bool{}
	for op := range before {
		allOps[op] = true
	}
	for op := range after {
		allOps[op] = true
	}
	out := make([]opDiff, 0, len(allOps))
	for op := range allOps {
		b := before[op]
		a := after[op]
		out = append(out, opDiff{
			Op:          op,
			NBefore:     b.N,
			NAfter:      a.N,
			P50Pct:      pctChange(b.P50Ns, a.P50Ns),
			P95Pct:      pctChange(b.P95Ns, a.P95Ns),
			P99Pct:      pctChange(b.P99Ns, a.P99Ns),
			MaxPct:      pctChange(b.MaxNs, a.MaxNs),
			P50BeforeNs: b.P50Ns,
			P50AfterNs:  a.P50Ns,
			P99BeforeNs: b.P99Ns,
			P99AfterNs:  a.P99Ns,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Op < out[j].Op })
	return out
}

// pctChange returns the percent change from b to a. Positive means
// got worse (latency increased). Special cases:
//   - both 0 → 0%
//   - b == 0, a > 0 → +inf (treated as 1000% so the diff is visible)
//   - b > 0, a == 0 → -100%
func pctChange(b, a int64) float64 {
	if b == 0 && a == 0 {
		return 0
	}
	if b == 0 {
		return 1000
	}
	return (float64(a-b) / float64(b)) * 100
}

// ---------------------------------------------------------------------
// Human output
// ---------------------------------------------------------------------

func printHumanDiff(out *os.File, d comparisonDiff, beforePath, afterPath string) {
	fmt.Fprintf(out, "before: %s  (%s, %.1fs)\n", beforePath, d.Before.Timestamp, d.Before.ElapsedS)
	fmt.Fprintf(out, "after:  %s  (%s, %.1fs)\n", afterPath, d.After.Timestamp, d.After.ElapsedS)
	fmt.Fprintln(out)

	for _, w := range d.Workers {
		header := fmt.Sprintf("[%s]", w.Name)
		if w.ErrorsDelta != 0 {
			header += fmt.Sprintf(" errors %+d", w.ErrorsDelta)
		}
		fmt.Fprintln(out, header)
		if len(w.Ops) == 0 {
			fmt.Fprintln(out, "  (no ops)")
			continue
		}
		// Pad columns for alignment.
		fmt.Fprintf(out, "  %-8s %-12s %-12s %s\n", "op", "n", "p50", "p99")
		for _, o := range w.Ops {
			p50 := fmt.Sprintf("%s → %s  (%s)",
				humanDur(o.P50BeforeNs), humanDur(o.P50AfterNs), humanPct(o.P50Pct))
			p99 := fmt.Sprintf("%s → %s  (%s)",
				humanDur(o.P99BeforeNs), humanDur(o.P99AfterNs), humanPct(o.P99Pct))
			ncol := fmt.Sprintf("%d → %d", o.NBefore, o.NAfter)
			fmt.Fprintf(out, "  %-8s %-12s %-25s %s\n", o.Op, ncol, p50, p99)
		}
		fmt.Fprintln(out)
	}

	if d.Metrics != nil {
		fmt.Fprintln(out, "[server metrics]")
		fmt.Fprintf(out, "  rpc_total:  %d → %d\n", d.Metrics.RPCTotalBefore, d.Metrics.RPCTotalAfter)
		fmt.Fprintf(out, "  rpc_errors: %d → %d\n", d.Metrics.RPCErrorsBefore, d.Metrics.RPCErrorsAfter)
		fmt.Fprintf(out, "  bytes_read: %d MiB → %d MiB\n",
			d.Metrics.BytesReadBefore/(1<<20), d.Metrics.BytesReadAfter/(1<<20))
	}
}

func humanDur(ns int64) string {
	return time.Duration(ns).Round(time.Microsecond).String()
}

// humanPct prints a percent-change with explicit sign. Positive =
// worse. Color codes via ANSI when stdout is a terminal would be
// nice but we leave that out for now; pipes-to-jq compat matters more.
func humanPct(pct float64) string {
	if math.Abs(pct) < 0.1 {
		return "  ±0%"
	}
	sign := "+"
	if pct < 0 {
		sign = ""
	}
	return fmt.Sprintf("%s%.1f%%", sign, pct)
}

// ---------------------------------------------------------------------
// Threshold gate
// ---------------------------------------------------------------------

func regressionExceedsThreshold(d comparisonDiff, p99Pct float64) bool {
	hit := false
	for _, w := range d.Workers {
		if w.ErrorsDelta > 0 {
			fmt.Fprintf(os.Stderr, "REGRESSION: worker %s grew errors by %d\n",
				w.Name, w.ErrorsDelta)
			hit = true
		}
		for _, o := range w.Ops {
			if p99Pct > 0 && o.P99Pct > p99Pct {
				fmt.Fprintf(os.Stderr, "REGRESSION: %s.%s p99 +%.1f%% (cap %.1f%%): %s → %s\n",
					w.Name, o.Op, o.P99Pct, p99Pct,
					humanDur(o.P99BeforeNs), humanDur(o.P99AfterNs))
				hit = true
			}
		}
	}
	if hit && p99Pct <= 0 {
		// Errors regressed but threshold was 0 → still report exit 1
		return true
	}
	return hit
}

