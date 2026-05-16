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
| 1.1 | Concurrent per-connection NFS dispatch (Finder browses while a long Read is in flight) | ⚠ landed in `691f550`; **prior validation invalidated by `f944a82` build-staleness bug; needs re-validation against the fresh binary** |
| 1.2 | No Finder freeze on any wedged backend | ⚠ partial — many vectors closed, full validation TBD |
| 1.3 | Clean unmount in every state | ✓ likely (ordered shutdown + Force Eject landed) — needs real validation |
| 1.4 | Crash-safe metadata (kill -9 → mountable in <5s) | ⚠ test tooling shipped in `5ec1a33`; real run pending |
| 1.5 | Recovery diagnostics (Export Diagnostics zip) | ✓ landed in Phase B |
| 1.6 | Stress test harness (24h CI run) | ⚠ scaffold landed in `74a9739`; 24h soak run pending |

**Tier-1 cannot be declared "production-ready" until all six pass.**
Active iteration count toward the 7-day-real-load / 24h-stress-harness
clock starts only after every box is checked.

---

## Tier-1 backlog (unblocked)

1. ~~**Concurrent per-connection NFS dispatch**~~ — landed in `691f550`
   (iteration 1). Awaiting real-Finder validation before checking the
   tier-1.1 box. Validation script: `cat /Volumes/zpool/<200MB-file> > /dev/null &`
   running, then navigate Finder around adjacent folders; expected
   <1s response on every Lookup.

2. ~~**Stress test harness (tier-1.6)**~~ — scaffold landed in
   `74a9739` as `cmd/jmstress`. Three workload mixes (finder/nle/backup),
   per-worker latency reporting, metrics endpoint delta. Next step on
   this item: run a 24h soak against the dev mount and check for
   leaks, wedges, or error accumulation. The 24h run itself is the
   acceptance test — once it passes cleanly, tier-1.6 is checked.

3. ~~**Crash-safety validation (tier-1.4)**~~ — `scripts/crash-recover-test.sh`
   landed in `5ec1a33`. Dry-run default; `--real` actually does the
   kill+relaunch with 5s budget assertion. Next step: user runs
   `--real` against the dev mount when ready.

4. **Full unmount validation (tier-1.3)** — manual matrix: stop
   mid-read, mid-write, mid-sync, with offline gate flipped, with
   prefetcher active. Document outcomes; fix any wedge. Requires
   real-Finder session — likely user-driven not loop-driven.

5. **Wedged-backend behavior matrix (tier-1.2)** — exercise the
   "kill MinIO mid-read" / "blackhole the network" / "Redis OOM"
   failure modes; assert Finder errors within 5s in each case. Some
   already covered (Lstat timeout, membuf timeout, Redis timeouts);
   the remaining ones need explicit testing.

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

### Iteration 1 — 2026-05-16

**Tier:** 1 (Stability).
**Picked:** tier-1.1 — concurrent per-connection NFS dispatch.

**Shipped (`691f550`):**
- `internal/nfs/conn.go`: request frame now buffered in
  `readRequestHeader` so the bufio.Reader advances past it before
  dispatch. `drain()` learns about `*bytes.Reader` (no-op). `serve()`
  acquires `rpcSem` then dispatches `c.handle` in a goroutine with
  panic recovery and idempotent close-on-write-error.
- Bonus fixes from code-review pass: 2 MiB frame-size ceiling
  (prevents malformed-header remote OOM), goroutine-level `recover()`
  (panic in one RPC no longer crashes the daemon), `finish()` routes
  buffer return through capacity-guarded `putResponseBuffer`.

**Validated:** `go vet` clean, race-detector tests pass on
`internal/nfs`, production build succeeds. Code-reviewer sub-agent
report: structurally correct, all flagged issues addressed.

**Deferred:** real-Finder validation belongs to the user's next
hands-on session (unit tests give false positives on the stack per
CLAUDE.md). Until that validation, tier-1.1 stays in the "landed but
not verified" state.

**Broken:** nothing introduced.

**Next:** iteration 2 should pick the stress test harness (tier-1.6)
— it's the prerequisite for declaring tier-1 production-ready since
acceptance requires 24h of synthetic load when no real users exist
yet. Estimated 4-6 hour slice; may be split across multiple
iterations.

### Iteration 2 — 2026-05-16

**Tier:** 1 (Stability).
**Picked:** tier-1.6 — stress test harness scaffold.

**Shipped (`74a9739`):**
- `cmd/jmstress/main.go`: external Go load generator that drives a
  mounted JuiceMount path with three workload mixes (finder/nle/backup),
  per-worker p50/p95/p99/max latency distributions, error counts, and
  a `/metrics` endpoint before/after delta. Graceful shutdown on
  SIGINT.
- Smoke-tested for 60s against live dev mount: 50K RPCs flowed, 0
  errors, latencies match what manual testing showed (finder p99
  ~250ms, occasional max-spikes to 1-2s on cold metadata).

**Validated:** `go vet` clean, smoke run succeeds, no panics or
deadlocks observed.

**Deferred:** the actual 24h soak run is the next-up acceptance test
for tier-1.6 — it's not in this iteration because a 24h soak isn't a
"2–6 hour slice." Future iteration kicks it off in background;
results land in a follow-up STATE.md entry.

**Broken:** nothing.

**Observations worth follow-up (not this iteration):**
- Finder-stat p99 ~250ms and max-spikes to 1-2s are consistent across
  manual tests and stress smoke. Concurrent dispatch fix eliminated
  the multi-second freezes, but there's residual latency worth
  understanding. Candidate causes: cold metadata Redis fetch,
  `os.Stat` path-canonicalization overhead, macOS NFS client ATTR
  refresh storms. Belongs to tier-1.2 (Finder responsiveness under
  load) once tier-1.6 closes.

**Next:** iteration 3 picks tier-1.4 (crash-safety validation) as a
small slice — script that does `kill -9` + relaunch + measures
recovery. ~1-2 hours. After that, iteration 4 kicks off the 24h soak
in background and parallel-works on tier-1.2 latency analysis.

### Iteration 3 — 2026-05-16

**Tier:** 1 (Stability).
**Picked:** tier-1.4 — crash-safety acceptance script.

**Shipped (`5ec1a33`):**
- `scripts/crash-recover-test.sh`: measures kill→reap, open→proc,
  open→metrics, open→mount intervals against a configurable budget
  (default 5s). Dry-run default to protect the live mount from
  accidental kills.

**Validated:** dry-run against live dev mount confirms preconditions,
PID detection, and plan output. Real-run validation deferred — the
user runs `--real` when they've got a non-critical mount window.

**Deferred:** the real kill+relaunch run is the actual acceptance
test. Tooling is the iteration deliverable; the data is the user's
next action.

**Broken:** nothing introduced. The script DID surface a real UX gap
that wasn't in any acceptance test before: JuiceMount doesn't
auto-Start the NFS server on app launch. The script warns about it
explicitly rather than hanging silently. This belongs to tier-2 (app
polish) as a "first-launch defaults" item.

**Next:** iteration 4 picks tier-1.2 (Finder responsiveness under
load) since it now has data to chase — the 200-400ms stat p99 the
stress harness surfaced. Likely involves enabling Go runtime traces
or pprof during a stress run to identify where the latency is
coming from. ~3-4 hour slice; may split if the root cause is deep.

### Iteration 4 — 2026-05-16

**Tier:** 1 (Stability).
**Picked:** tier-1.2 — Finder responsiveness investigation under load.
**Outcome:** uncovered a build-infrastructure bug that invalidates
prior tier-1 validation. Shipped a fix to the build script; the
intended latency investigation must be re-done in iteration 5 against
a fresh binary.

**What happened:**
1. Ran stress harness for 60s against the live mount, captured CPU
   pprof + goroutine dump.
2. Found stat p50=505ms, p99=3.6s, max=6.3s, 33 client-side errors.
3. CPU profile said os.Lstat dominated (75% cum). Goroutine dump
   showed exactly one goroutine in conn.serve at the snapshot.
4. Source has lstatNotExistWithTimeout (from b1e9c6a, 2026-05-13) AND
   concurrent dispatch (from 691f550, this session). Neither was in
   the binary's symbol table (`nm -a`).
5. Root cause: SPM's incremental build doesn't detect content changes
   in `libnfsd.a` passed via -L/-l. The Swift binary was re-linked
   from a stale cache.
6. The build script (`scripts/build-app.sh`) didn't `rm -rf
   .build/<config>/JuiceMount` before re-running `swift build`. This
   was a known issue from project memory but had never been added to
   the script.

**Shipped (`f944a82`):**
- `scripts/build-app.sh`: removes `build/libnfsd.{a,h}` before the
  Go build, and removes `.build/<config>/JuiceMount` before the Swift
  build. Subsequent rebuild verified the new binary contains
  lstatNotExist symbols (count 4 via `nm -a`).

**Tier-1 acceptance tests affected:**
- 1.1 (concurrent dispatch): the "<1s adjacent-Finder-op during 5GB
  Read" result from iteration 1 was measured against the OLD binary,
  which was running sequential dispatch. The fact that latencies
  were still <1s suggests sequential dispatch is less catastrophic
  than the overnight audit suspected (or that macOS NFS client
  pipelined more than we thought). NEEDS RE-VALIDATION against the
  fresh build.
- 1.2 (Finder responsiveness): the 200-400ms stat numbers from
  iteration 2 stress smoke were the OLD binary too. The 500ms p50 /
  3.6s p99 from iteration 4's stress run was also the OLD binary.
  Expected to drop substantially against the fresh build because
  (a) concurrent dispatch is now actually live and (b) the Lstat
  timeout caps individual stat blocking at 2s.

**Validation pending — needs the user to:**
1. Stop the current JuiceMount instance (PID 41860).
2. `open build/JuiceMount.app` (fresh build, signed at 03:35-ish).
3. Click Start in the menu bar.
4. Re-run `cmd/jmstress` for 60s and report numbers.

**Broken:** discovered that ALL builds in this session and the
overnight loop may have shipped stale code to the user. Tier-1
"shipped" markers should be treated as "code committed AND a freshly
re-validated binary" only — not "code committed."

**Next:** iteration 5 re-validates 1.1 and 1.2 against the fresh
binary. Latency investigation is gated on knowing the actual current
baseline.
