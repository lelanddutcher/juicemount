# Tier 1 — Stability (table-stakes)

The active blocking tier of the JuiceMount development plan. Tier-1
must close before any other tier can be declared production-ready
(`VISION.md` non-negotiable). "Closed" means: every acceptance test
below passes for **7 consecutive days of real user load**, or **24h
of synthetic stress-harness load** when no real users exist yet.

The product promise tier-1 backs:

> "JuiceMount feels like a local SSD. It never freezes Finder. It
> never wedges a mount. When it can't recover, it tells the user
> exactly what to do."

Without that, every tier above is built on sand.

## Acceptance tests

Each row has a numeric ID for STATE.md tracking and a measurable
threshold. "✓ validated" requires either real-user load over 7 days
OR the automated harness (`scripts/test-offline-resilience.sh`,
`cmd/jmstress`) reproducing pass against the live mount.

| # | Test | Pass criterion | Status reference |
|---|---|---|---|
| 1.1 | Concurrent per-connection NFS dispatch | `cat /Volumes/zpool/<200MB-file> > /dev/null &` running, Finder browses adjacent folders <1s p99 | docs/STATE.md |
| 1.2 | No Finder freeze on any wedged backend | Kill MinIO mid-read; Finder reports error within 5s, no beachball | docs/STATE.md |
| 1.3 | Clean unmount in every state | Stop never leaves a wedged kernel mount, even mid-read / mid-write / mid-sync | docs/STATE.md |
| 1.4 | Crash-safe metadata (`kill -9` → mountable in <5s) | scripts/crash-recover-test.sh `--real` exits 0 | docs/STATE.md |
| 1.5 | Recovery diagnostics (Export Diagnostics zip) | One-click zip in popover; contains logs + mount table + JuiceFS state + MinIO health | done (Phase B) |
| 1.6 | Stress test harness (24h CI run) | `cmd/jmstress --duration 24h --json` completes with 0 server errors + p99 stat <500ms | docs/STATE.md |
| 1.7 | Walk-out: pinned files keep working when network drops | pfctl harness: un-pinned stat refused in <2s, pinned stat works | ✓ validated via `scripts/test-offline-resilience.sh` |
| 1.8 | Auto-engage offline mode within 5s of route loss | pfctl harness: auto_offline=true within 5s of block engage | ✓ validated |
| 1.9 | Auto-recover offline mode within 30s of route return | pfctl harness: auto_offline=false within 30s of block release | ✓ validated |
| 1.10 | Network errors classified as network (not "Redis degraded") | Log lines for `no route to host` errors emit `kind:network_path` | ✓ validated via production log entries |

## Architecture summary (post-iter)

The Go-side architecture that backs tier-1:

```
macOS Finder / NLE / any app
    ↓  POSIX I/O on /Volumes/<name>
macOS NFS client (kernel)            ← rsize=256K, soft, timeo=10
    ↓  TCP loopback (127.0.0.1:11049)
internal/nfs.(*conn).serve           ← concurrent dispatch (one goroutine
    ↓                                   per RPC; XID demux on response)
nfs/handler.go juiceFS               ← Stat/Lookup/Read paths,
    ├─ metadata.Store (SQLite)         offline-mode gates
    ├─ cache.Reader (JuiceFS chunks)
    ├─ MemoryBuffer (hot small files)
    ├─ FDPool (open file LRU)
    ├─ pin.Store (offline-pinned)
    └─ pin.IsOffline (gate)          ← driven by reachability monitor
                                       (health/reachability.go) +
                                       user-toggle
    ↓ FUSE
JuiceFS daemon at ~/.juicemount/fuse-internal
    ↓
Redis (metadata) + MinIO (S3)  ← on the user's LAN/server
```

## Iteration plan

Tier-1 work is mostly shipped. Remaining items are:

### Iteration A — Run the 24h stress soak (closes 1.6)

`cmd/jmstress` harness landed. Acceptance is the actual 24h run.

Slice:
1. Pick a window when network won't be touched for 24h. (~5min setup)
2. Launch the soak in background with `--json --periodic-json 60s`:
   ```bash
   /tmp/jmstress-bin --mount /Volumes/zpool-dev --duration 24h \
     --finder-workers 4 --nle-workers 2 --backup-workers 1 \
     --discovery-depth 6 --large-file-min-mb 200 \
     --json --periodic-json 60s > /tmp/jm-soak-24h.jsonl &
   ```
3. Periodic check (every ~2h): `tail -1 /tmp/jm-soak-24h.jsonl | jq` —
   verify per-op p50/p99/max have not regressed and errors are stable.
4. At T+24h: parse final entry, assert:
   - `finder.errors < 10`
   - `nle.errors < 10`
   - `finder.stat p99 < 500ms`
   - `nle.read p99 < 2s`
   - no goroutine leak (RSS at T+24h within 2× RSS at T+1h)
5. If pass: flip 1.6 to ✓ in STATE.md. If fail: triage which threshold
   broke and address per the offline-resilience pattern (one commit
   per concern, code-reviewer pass, real-binary restart).

### Iteration B — Real-mount-wedge matrix (closes 1.2, 1.3)

The pfctl harness covers Redis-path failures. The remaining failure
classes are:

- **MinIO down mid-read**: pfctl block `<minio-host>:9000` during an
  active `cat <large-file>`. Expect: read returns ENXIO within 2s
  (handler chunked-loop deadline), no Finder beachball.
- **FUSE-daemon hang**: `kill -STOP <juicefs-pid>` mid-operation,
  then `kill -CONT`. Expect: ops during the stop block on the
  Lstat-timeout (2s) and fail, system recovers cleanly.
- **NFS-loopback mid-shutdown**: trigger Stop while a 5GB read is
  in flight. Expect: read errors cleanly (NFS server drains and
  closes), unmount succeeds, no kernel mount-table residue.

Each is a 30-minute script in `scripts/wedge-tests/<scenario>.sh`,
each emits a single-line pass/fail. Together they form the manual-
matrix entry for STATE.md.

### Iteration C — Real-run the crash-safety harness (closes 1.4)

`scripts/crash-recover-test.sh` exists with `--real` flag. Currently
dry-run only. Slice:
1. With mount alive: `./scripts/crash-recover-test.sh --real`
2. Confirm `kill -9` → relaunch → mount up within 5s
3. If timing budget exceeded, profile the bottleneck (likely SQLite
   journal replay on cold-cache) and either bump budget OR optimize
4. Flip 1.4 to ✓ when 3 consecutive runs all <5s

### Iteration D — Goroutine-leak watchdog (optional, hardens 1.6)

Add to `cmd/jmstress`: every periodic tick, GET `/debug/pprof/goroutine?debug=1`
and assert goroutine count is within 1.5× the first-tick baseline.
If it ramps unbounded over 24h, surface as a soak-test failure
even when latency/error tests pass. ~3 hours.

## Signals to watch

Telemetry signals confirming each iteration's behavior IS what we
think. Future-self iterations should grep for these before declaring
"works."

| Acceptance test | Signal (in juicemount.log / `/metrics` / harness output) |
|---|---|
| 1.1 | metrics `RPCs in flight > 1` during stress run |
| 1.2 | log `purging phantom file` does NOT fire during Redis-down windows (gate from `bbc6bff`) |
| 1.3 | log `nfs unmounted` on every Stop; no `nfs unmount FAILED — mount is wedged` |
| 1.4 | `scripts/crash-recover-test.sh --real` exit 0 with each interval <5000ms |
| 1.5 | `Export Diagnostics` button produces a zip containing >= juicemount.log + mount table + a `/debug/pprof/goroutine` snapshot |
| 1.6 | `jmstress` JSON: `finder.stat.p99_ns < 500_000_000` at T+24h, `nle.errors < 10`, RSS within 2× baseline |
| 1.7 | `pfctl harness`: `[PASS] 1.7a: stat failed in <Xs (budget 2s)` |
| 1.8 | `pfctl harness`: `[PASS] 1.8: auto_offline=true within <Xs (budget 5s)` |
| 1.9 | `pfctl harness`: `[PASS] 1.9: auto_offline=false within <Xs (budget 30s)` |
| 1.10 | log `network path to backend lost` with `kind:network_path` (NOT `reconciliation failed`) |

## Non-negotiables (per VISION.md, repeated here for the in-tier reader)

- No FileProviderExtension, ever (`docs/no-fileprovider.md`)
- No telemetry without opt-in
- No proprietary deps for self-hosters
- No bundled-PR scope creep
- Risky work behind default-off flag
- Open-source-first

## Dependencies

- **Blocks** tier-2 onwards. No tier-2 polish ships before all tier-1
  acceptance tests pass.
- **Depends on** none — tier-1 is the foundation.

## Bottom line

Tier-1 is ~90% done. Remaining work is mostly running the harnesses
that already exist (24h soak, crash-recover real run, wedge-matrix
scripts), not new code. A focused 1-2 day push should close it.
