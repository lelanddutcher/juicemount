# JuiceMount performance-testing methodology

**Status:** Phase II perf-hardening (2026-05-25 onward), born out of the
QA-31 post-mortem (see README "Recent: NFS read throughput restored").

## Why this exists

The QA-31 post-mortem proved a category of failure the old QA suite
couldn't catch:

> A bug is invisible until a workload sustains the RPC rate that exposes
> it. The per-RPC FUSE Stat amplification was present for months. It
> only became user-visible when DaVinci scrubbed cached 4K media at
> 60-240 READ RPCs/sec.

The QA suite tested correctness *per op type*. It never sustained an
RPC rate that mirrors a real production workflow. The methodology below
fixes that.

## Workload taxonomy

Each workload models a specific NLE / Finder behavior and sustains a
specific RPC rate. Together they cover the surface area that breaks in
production.

| Workload                | Script                                                | RPC mix                                                 | Sustained rate                  | Exposes                                                       |
|-------------------------|-------------------------------------------------------|---------------------------------------------------------|----------------------------------|---------------------------------------------------------------|
| `resolve-scrub`         | `scripts/qa-suite/11-workloads/resolve-scrub.sh`      | random READ on 1-4 MB ranges                            | ~60-240 READ/sec                 | per-RPC overhead, cache hit latency tail, FUSE pool contention |
| `finder-copy-deep`      | `scripts/qa-suite/11-workloads/finder-copy-deep.sh`   | LOOKUP + CREATE + WRITE cascades + AppleDouble sidecars | ~1k mixed RPC/sec                | sync writeMu contention, cache symmetry, pin/unpin races       |
| `bin-browse`            | `scripts/qa-suite/11-workloads/bin-browse.sh`         | LOOKUP + GETATTR + READDIRPLUS tree walks               | ~500 LOOKUP/sec                  | phantom-purge gate cost, children-index integrity              |
| `cold-playback`         | `scripts/qa-suite/11-workloads/cold-playback.sh`      | sequential READ from fully uncached file                | backend-bound                    | timeo budget, prefetcher behavior, writeback queue             |
| `pin-coverage-verify`   | `scripts/qa-suite/11-workloads/pin-coverage-verify.sh`| re-prefetch entire pin root                             | bounded by disk + backend        | LRU/eviction pressure, juicefs cache fill behavior             |

## Metrics contract

Every workload script:

1. Snapshots `/metrics` JSON before the workload (`baseline.json`).
2. Runs the workload for a fixed wall-clock duration (typical: 30-60 s).
3. Snapshots `/metrics` again after (`final.json`).
4. Emits a `summary.json` with per-RPC deltas:
   ```json
   {
     "workload": "resolve-scrub",
     "build_sha": "88ccee8",
     "duration_sec": 60,
     "started_at": "2026-05-25T21:30:00Z",
     "bytes_read_delta": 12884901888,
     "throughput_MBps": 204.8,
     "rpcs": {
       "READ":   {"count_delta": 12238, "mean_us": 245.1, "p50_us": 312.4, "p95_us": 481.0, "p99_us": 1240.5, "max_us": 370000},
       "LOOKUP": {"count_delta": 18,    "mean_us":   1.2, "p50_us":   0.1, "p95_us":   5.0, "p99_us":   12.0, "max_us":   850}
     },
     "rpc_errors_delta": 0,
     "from_handle_stale_events": 0
   }
   ```
5. Tags the artifact with the JM build SHA at run time
   (`scripts/qa-suite/baselines/<workload>-<sha>.json`).

The `metric-counters` and `get-metric` jmctl subcommands make this
scripting straightforward — no JSON-mangling in shell.

## Regression thresholds

The `12-perf-regression.sh` orchestrator runs the battery and compares
to the stored baseline. Any of the following fails the run:

| Signal                                         | Threshold          |
|------------------------------------------------|--------------------|
| Any RPC type's `p95_us` vs baseline            | > 2.0×             |
| Any RPC type's `max_us` vs baseline            | > 5.0×             |
| Throughput (bytes/sec) vs baseline             | < 50% of baseline  |
| `rpc_errors_delta`                             | > 0 (any non-zero) |
| `from_handle_stale_events` during the workload | > 0                |
| Workload didn't complete in 1.5× wall-clock    | FAIL               |

These are FAIL thresholds. WARN thresholds (one column tighter) fire
informationally without exit-code-failing the run.

**Rationale:** the QA-31 bug looked like a 4-5× p95 regression and a
14× throughput drop. 2× / 50% catches anything in that class with
headroom. The 5× max threshold catches single-outlier bugs that don't
show up in averages.

## Baseline lifecycle

`scripts/qa-suite/baselines/healthy-<sha>.json` is the source of truth
for "this build at this commit ran the workload battery with these
numbers." Updating the baseline is a deliberate act — not automatic —
because automatic-update masks regressions:

1. Run the battery on the new build: `scripts/qa-suite/12-perf-regression.sh`.
2. If FAILED: regression. Investigate, fix, re-run. Do NOT update
   baseline until green.
3. If PASSED with improvement (e.g., 30% better throughput): manually
   copy the new run's `<workload>-<sha>.json` into `baselines/healthy-<sha>.json`
   and commit with a one-line note explaining the speedup.
4. Old baselines accumulate by SHA so we can attribute regressions to
   specific commits.

## Chaos overlay

The fixes from QA-26..QA-31 reduced the symptom but didn't replace the
underlying chaos triggers (wifi flapping, Redis hiccups, MinIO
bandwidth dips). Workloads are re-run under each of these to surface
the *next* latent bug class:

| Chaos                                       | Implementation                                                                   |
|---------------------------------------------|----------------------------------------------------------------------------------|
| `redis-flap`                                | pfctl block to `${REDIS_HOST}:${REDIS_PORT}` for 5s every 30s during the workload |
| `minio-throttle`                            | dnctl pipe limiting `${BACKEND_HOST}:${BACKEND_PORT}` to 10 Mbit/s for the workload |
| `forced-sync`                               | `jmctl sync` fired every 10s during the workload (drives metadata.Store writeMu)  |

The orchestrator runs each workload first clean, then under each
chaos. The regression thresholds apply to the chaos runs too, with a
WARN-then-FAIL ladder rather than instant FAIL (a 2× p95 under chaos
might be the correct system behavior, not a bug).

## Code-reviewer subagent contract for perf

For any perf-relevant change (anything in `nfs/`, `internal/nfs/`,
`metadata/`, `bridge/cbridge.go` mount opts, or the prefetcher), the
code-review prompt must explicitly ask:

> "Will this change increase per-RPC overhead for any RPC type
> (READ/WRITE/LOOKUP/GETATTR/CREATE)? If so, by how much, and is the
> increase justified by a correctness or safety improvement? Run the
> change against `scripts/qa-suite/12-perf-regression.sh`. Report any
> RPC type whose p95 went up more than 20% as a finding."

The 20% threshold for code review is tighter than the 2× FAIL gate —
catching the slow-creep regressions that would otherwise accumulate
into the next QA-31.

## CLI: jmctl

`jmctl` (built from `cmd/jmctl/`) is the single entry point for
control-plane operations from scripts. It wraps the HTTP endpoints on
`127.0.0.1:11050` with JSON-aware exit codes and a `get-metric` /
`metric-counters` shortcut for perf scraping. See `jmctl -h` for the
command list.

Convention: workload scripts use jmctl exclusively. No raw curl
invocations in the perf suite — keeps the contract narrow and the
diagnostics consistent.

## Adding a new workload

1. Mirror an existing pattern in `scripts/qa-suite/11-workloads/`.
2. Use `jmctl` for any control-plane operation.
3. Snapshot metrics before + after; write `summary.json` to the
   per-run artifact dir.
4. Add the workload to the orchestrator's battery.
5. Run once to generate the baseline; commit `baselines/<workload>-<sha>.json`.
6. Document the workload in the table at the top of this file.

## Future work

- **Unified pin-aware cache truth** — the three-layer guard in QA-30
  treats the symptom of pin store and metadata store both holding
  truth about "what's cached" without a shared invariant. The right
  long-term fix is a single source of truth. Tracked as the next
  architectural step.
- **Continuous perf-regression in CI** — once baselines are stable
  across several SHAs, wire `12-perf-regression.sh` into a pre-merge
  CI gate. The numbers stop drifting only when they're checked
  automatically.
