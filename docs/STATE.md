# JuiceMount development state

The canonical "what's done / what's next" file for the autonomous
development loop. Driven by `docs/VISION.md` tier acceptance tests.

Format: one entry per iteration. Each entry declares which tier it
touched, what shipped, what's still broken, and what the next
unblocked item is.

---

## Active tier: Tier 1 — Stability

Acceptance tests (from `docs/VISION.md`):

| # | Test | Status |
|---|---|---|
| 1.1 | Concurrent per-connection NFS dispatch (Finder browses while a long Read is in flight) | ✗ open — designed in OVERNIGHT-AUDIT iteration 7, not landed |
| 1.2 | No Finder freeze on any wedged backend | ⚠ partial — many vectors closed, full validation TBD |
| 1.3 | Clean unmount in every state | ✓ likely (ordered shutdown + Force Eject landed) — needs real validation |
| 1.4 | Crash-safe metadata (kill -9 → mountable in <5s) | ⚠ not validated |
| 1.5 | Recovery diagnostics (Export Diagnostics zip) | ✓ landed in Phase B |
| 1.6 | Stress test harness (24h CI run) | ✗ not built |

**Tier-1 cannot be declared "production-ready" until all six pass.**
Active iteration count toward the 7-day-real-load / 24h-stress-harness
clock starts only after every box is checked.

---

## Tier-1 backlog (unblocked)

1. **Concurrent per-connection NFS dispatch** — designed in
   `OVERNIGHT-AUDIT.md` iteration 7 (now gone from working tree but in
   git history at `04a6dd7`). Largest single hang-vector remaining.
   Estimated 2-4 hours.

2. **Stress test harness** — synthetic Finder-like workload that runs
   for 24h, exercising the mount lifecycle, concurrent reads, cache
   eviction, and offline-mode transitions. No existing implementation.

3. **Crash-safety validation** — needs a reproducible test that does
   `kill -9` on the running app + relaunches and measures recovery
   time. Could be a script under `scripts/`.

4. **Full unmount validation** — manual matrix: stop mid-read,
   mid-write, mid-sync, with offline gate flipped, with prefetcher
   active. Document outcomes; fix any wedge.

---

## Reference: overnight stability sprint (2026-05-13)

A 9-iteration autonomous loop landed 11 commits closing independent
hang vectors. Summary preserved from `OVERNIGHT-AUDIT.md` (now removed
from the working tree):

| # | Commit | Fix |
|---|---|---|
| 1 | `1121bae` | NFS timeo 300→10, retrans 5→2, Force Eject, ordered shutdown |
| 2 | `1d73c7d` | FUSE-direct self-test (no NFS loopback wedge) |
| 3 | `a12bd8c` | All `health/` shell-outs bounded with CommandContext |
| 4 | `a5a42e5` | `globalMu` snapshot-then-release in every slow cgo export |
| 5 | `adf70b8` | Chunked `BulkInsert` + pin store `busy_timeout` |
| 6 | `0316096` | `tailJuiceFSLog` stopCh + `FUSEManager.Stop` bounded |
| 7 | `21db111` | Code-review followups: 3 HIGH bugs in #2–#5 closed |
| 8 | `0a7f767` | `rc.mu` dropped around `pruneAbsent` iteration |
| 9 | `e603eab` | Swift popover: cacheStatus + setOffline off MainActor |
| 10 | `b1e9c6a` | Phantom-file Lstat bounded with 2 s timeout |
| 11 | `93e9d8d` | membuf cascade-freeze bounded + cache Redis timeouts |

Six independent failure modes closed: kernel-NFS loopback wedge,
monitor-loop syscall parking, `globalMu` cgo serialization, metadata
`writeMu` serialization, Swift MainActor cgo blocking, redis
pruneAbsent iteration.

The single remaining architectural lever (concurrent dispatch in
`internal/nfs/conn.go`) was deferred to supervised landing — which is
this loop's iteration 1.

---

## Loop log

(Iterations append here.)
