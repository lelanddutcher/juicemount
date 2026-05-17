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
| 1.2 | No Finder freeze on any wedged backend | ⚠ 2 of 3 iter-B wedge harnesses shipped (`minio-down-mid-read` iter 9, `fuse-hang-mid-op` iter 10); both pass with adjacent-stat max <40ms during wedge. NFS-loopback-mid-shutdown harness still TBD |
| 1.3 | Clean unmount in every state | ✓ likely (ordered shutdown + Force Eject landed) — needs real validation |
| 1.4 | Crash-safe metadata (kill -9 → mountable in <5s) | ⚠ test tooling shipped in `5ec1a33`; real run pending |
| 1.5 | Recovery diagnostics (Export Diagnostics zip) | ✓ landed in Phase B |
| 1.6 | Stress test harness (24h CI run) | ⚠ scaffold landed in `74a9739`; 24h soak run pending |
| 1.7 | Walk-out: pinned files keep working when network drops | ✓ validated 2026-05-16 23:21 via pfctl harness: un-pinned stat refused in 0.02s (budget 2s) |
| 1.8 | Auto-engage offline mode within 5s of route loss | ✓ validated 2026-05-16 23:21 via pfctl harness: auto_offline=true in 3.28s (budget 5s) |
| 1.9 | Auto-recover offline mode within 30s of route return | ✓ validated 2026-05-16 23:21 via pfctl harness: auto_offline=false in 0.77s (budget 30s) |
| 1.10 | Network errors classified as network (not "Redis degraded") | ✓ landed in `e8aa5cb`; validated 2026-05-16 15:27-15:30 — three real "no route to host" events all logged with new `network path to backend lost` / `kind: network_path` shape |

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

### Iteration 5 — 2026-05-16

**Tier:** 1 (Stability).
**Picked:** pivot — user hasn't restarted the mount with the fresh
binary yet (PID 41860 still running from 02:09:48). Re-validation
deferred. Used the iteration on a non-restart-dependent tier-1.6
extension: machine-readable output for jmstress.

**Shipped (`386ac52`):**
- `cmd/jmstress`: added `--json` (emit a single JSON summary on
  stdout, human output to stderr) and `--periodic-json N` (emit
  "type":"tick" snapshots every N during the run, with a final
  "type":"final" entry). Stable schema with mean/p50/p95/p99/max
  per op, errors per worker, and a metrics delta on the final.

**Validated:** 10s smoke with `--json --periodic-json 3s` produced 4
valid JSON lines (3 ticks + 1 final). stat p50 = 357µs, p99 = 1.6ms
on the shallow-discovery smoke — the iteration-4 outliers (505ms p50)
were from deeper cold-backend traversal, not a regression.

**Deferred:** the 24h soak that produces the actual tier-1.6
acceptance data. Now actionable since the harness produces
analyzable timeseries instead of a single summary blob.

**Broken:** nothing.

**Next:** iteration 6 status depends on what the user does between
now and the next wake. If they restart the mount, iter 6 re-runs
the harness against the fresh binary and updates tier-1.1/1.2 with
real numbers. If they don't, iter 6 picks tier-1.3 manual-unmount
matrix or starts a long-duration `--json` background soak against
the current binary as a baseline before the swap.

### Iteration 6 — 2026-05-16

**Tier:** 1 (Stability).
**Picked:** pivot again — PID 41860 (old binary) still running. Built
the analytical companion to jmstress: a soak-result differ.

**Shipped (`b5f75bb`):**
- `cmd/jmcompare/main.go`: reads two `jmstress --json` output files
  (before.jsonl, after.jsonl), reports per-worker, per-op latency
  percentile deltas with explicit +/- percent changes. Threshold
  gate via `--threshold-p99-regression-pct N` exits non-zero on
  regression — suitable for CI gating "the new code doesn't make
  Finder worse." Optional `--json` for machine-readable diff.

**Validated:** smoke test on two 8s runs (same workload, different
seeds) produced expected 4-column human report and correct
threshold gate (exit 1 at 0.1% threshold; exit 0 in warn-only mode).

**Why this matters:** the tier-1 acceptance workflow is now
end-to-end actionable when the binary swap happens:
  1. jmstress --json --duration 1h > old.jsonl  (current binary)
  2. swap to fresh binary
  3. jmstress --json --duration 1h > new.jsonl
  4. jmcompare old.jsonl new.jsonl

Before this iteration, step 4 was "eyeball two text reports."

**Broken:** nothing.

**Observation:** three iterations now (4, 5, 6) where the user fired
/loop without restarting the mount. The pattern suggests they're
letting the autonomous loop iterate while they work elsewhere, OR
the dev mount swap requires a context window they haven't had.
Iterations have been pivoting to non-restart-dependent work — which
is finite. Eventually we run out of harness-extension ideas and
must either (a) get the restart and proceed with tier-1.1/1.2
re-validation, or (b) drop down to tier-1.3 manual matrix that
requires the user's hands.

**Next:** iteration 7 either re-runs validation against fresh binary
(if user restarts) or kicks off a long `jmstress --duration 4h --json
--periodic-json 60s` baseline against the current binary as a
backstop datapoint, then queues tier-1.3.

### Iteration 7 — 2026-05-16

**Tier:** 1 (Stability).
**Picked:** still no restart. Built a small forward-looking dev tool
that prevents recurrence of the iter-4 build-staleness incident.

**Shipped (`3566bb7`):**
- `scripts/verify-build.sh`: symbol-table inspector for a built
  JuiceMount.app. Confirms every known fix in a sampling manifest
  is present in the binary; `--running` also confirms the live
  process is using that binary (inode-via-lsof, mtime fallback).
  Each manifest entry pairs a non-inlinable symbol pattern with a
  human description; consts and small inlined helpers can't be
  detected this way and are documented as a known limitation.

**Validated:** on-disk binary passes 3 fixes (lstatNotExistWithTimeout
+ closure + concurrent-dispatch gowrap1), exit 0. `--running`
correctly flags PID 41860 as stale (start time predates fresh
binary's mtime), exit 2. Matches the iter-4 failure mode exactly.

**Why this matters:** iter 4 burned an entire iteration discovering
the staleness bug after running pprof against the wrong binary. This
script catches it in seconds. Future iterations that depend on a
specific fix being live can prefix with `verify-build.sh --running`
and abort cleanly if the fix isn't there.

**Broken:** nothing.

**Observation:** four iterations now (4–7) without a restart. PID
41860 has been running since 02:09:48 today. The autonomous loop is
now generating dev-tooling at a steady rate but actual tier-1
acceptance numbers are still gated on the binary swap. Iteration 8
should either re-validate (post-restart) or genuinely run out of
tier-1.6-extension work — at which point the loop should stop
rather than manufacturing make-work.

**Next:** iteration 8 checks the restart state. If restarted: full
re-validation. If not: I'll stop the loop after this iteration with
a PushNotification — eight iterations of tooling on a stale binary
is the point where continued autonomous work has negative marginal
value.

### Iteration 10 — 2026-05-17

**Tier:** 1 (Stability).
**Picked:** tier-1.2 iter-B sub-slice 2 — FUSE-hang-mid-op wedge harness.

**Shipped (commit pending):**
- `scripts/wedge-tests/fuse-hang-mid-op.sh`: SIGSTOP's all juicefs
  processes matching `juicefs mount.*fuse-internal` (the JuiceMount-
  managed FUSE backend), then in parallel measures:
  - Fresh-path stat (forces FUSE traversal — handler can't satisfy
    from metadata.Store cache, must go through wedged FUSE) → must
    return within `--fuse-timeout` (default 4s).
  - Cached-path stats (mount root, served from metadata.Store) →
    must stay under `--stat-budget-ms` (default 500ms). The "Finder
    doesn't beachball even while FUSE is wedged" proxy.
  - Post-SIGCONT recovery stat → must succeed within `--recover-budget`
    (default 5s).

  Trap-EXIT/INT/TERM SIGCONTs unconditionally; only SIGKILL of the
  script itself leaves a wedged mount (documented with manual recovery).

**Validated:** 4 consecutive runs against the live mount:
  - run 1: fresh=0.70s, cached_max=27ms (8 probes), recovery=0.02s — PASS
  - run 2: fresh=0.70s, cached_max=26ms, recovery=0.17s — PASS
  - run 3: fresh=0.94s, cached_max=27ms, recovery=0.02s — PASS
  - run 4 (post-fix): fresh=0.68s, cached_max=30ms, recovery=0.02s — PASS

  Fresh-stat consistently returns in 0.7-1.0s, well under the
  handler's internal 2s Lstat timeout. Reason: the NFS client mounts
  `soft` with `timeo=1s`, so the kernel surrenders before the handler
  deadline fires. Same user-facing outcome, different cause —
  documented inline so future readers don't conclude the handler
  timeout is broken.

**Code-reviewer pass:** 2 HIGH + 1 MEDIUM addressed:
  - HIGH-1: TOCTOU between PID discovery and SIGSTOP. Fix: re-discover
    PIDs immediately before SIGSTOP, abort if set changed.
  - HIGH-2: `set -e` would abort mid-wedge if any SIGSTOP target died
    between cross-check and kill, producing uninterpretable exit
    code. Fix: guard each kill with `|| { log "WARN"; }` and continue.
  - MEDIUM: NFS-soft-mount-timeout-vs-handler-timeout explanation
    documented inline at the fresh-stat measurement.

  1 MEDIUM deferred (multi-instance PID-pattern false positive —
  not relevant single-instance dev setup, would matter for CI hosts
  running multiple JuiceMount profiles simultaneously).

**Tier-1.2 status:** advances from "MinIO-wedge shipped; FUSE-hang +
NFS-mid-shutdown harnesses still TBD" to "2 of 3 wedge harnesses
shipped; NFS-loopback-mid-shutdown still TBD." One scenario left
before 1.2 ✓ validated.

**Broken:** nothing.

**Next:** iteration 11 picks the third and final wedge scenario —
`scripts/wedge-tests/nfs-loopback-mid-shutdown.sh` (trigger Stop
while a 5GB read is in flight, expect read errors cleanly and
unmount succeeds without kernel mount-table residue). This one
touches mount lifecycle directly so the test requires triggering
JuiceMount Stop programmatically — likely via the admin API or by
killing the JuiceMount process and observing macOS's mount table.

---

### Iteration 9 — 2026-05-17

**Tier:** 1 (Stability).
**Picked:** tier-1.2 iter-B sub-slice 1 — first wedge-test script (MinIO
down mid-read). The active loop resumed against a fresh-binary mount
(PID 42644, verified via `scripts/verify-build.sh --running`), unblocking
real wedge-injection testing.

**Shipped (commit pending):**
- `scripts/wedge-tests/minio-down-mid-read.sh`: pfctl-based harness that
  starts a streaming `cat` of an auto-rotated 1GB+ probe file, mid-read
  engages a pf block on the MinIO endpoint (default `192.168.0.212:9000`)
  via a dedicated sub-anchor (`com.apple/251.JuiceMountWedge`, distinct
  from the offline-resilience harness's 250 anchor), then in parallel
  measures (a) how long until the streaming read errors, (b) how long
  adjacent stats on the mount root take during the wedge.

  Verdict logic:
    HARD FAIL if cat wedges past `--max-wait` (default 10s) OR adjacent
      stat max exceeds `--stat-budget-ms` (default 500ms) — the real
      "Finder would beachball" signal.
    WARN if read-to-EOF-error exceeds `--read-budget` (default 5s) but
      the test otherwise passes — cat drains the JuiceFS prefetch buffer
      before erroring, so it's strictly conservative vs the Finder
      experience (which issues small reads, sees the first error sooner).
    INCONCLUSIVE if cat exits 0 (probe was cached); rotation cache
      `/tmp/jmwedge-last-probe` reduces but doesn't eliminate this.

  Trap-EXIT cleanup releases the pf anchor on every termination path
  except external SIGKILL (documented in help text with manual recovery).

**Validated:** 4 consecutive runs against the live mount:
  - run 1 (2.3GB cold): read-error 4.25s, stat max 27ms over 15 probes — PASS
  - run 2 (same probe, partial cache): read-error 1.16s, stat max 32ms — PASS
  - run 3 (rotated probe): read-error 1.73s, stat max 28ms — PASS
  - run 4 (rotated probe): read-error 2.04s, stat max 37ms — PASS
  Stat-side proxy (the canonical "no beachball" check) sat at 27-37ms
  across all runs — order of magnitude below the 500ms budget.

**Code-reviewer pass:** spawned, 1 MEDIUM addressed (wedge/clean-exit
verdict-ordering race guard), 1 LOW addressed (max-wait > read-budget
precondition guard), 1 LOW addressed (manual pf-recovery documented in
help text). 1 MEDIUM deferred (python3/bc subprocess overhead inside the
stat-probe loop — measurements show stat_max stays at 27-37ms, well under
500ms budget, so the overhead isn't biasing the signal yet).

**Tier-1.2 status:** advances from "many vectors closed, full validation
TBD" to "MinIO-wedge harness shipped, FUSE-hang + NFS-mid-shutdown
harnesses still TBD." Two more wedge scenarios needed before 1.2 ✓.

**Broken:** nothing.

**Observation about the handler:** cat-to-EOF-error times varied from
1.16s to 4.25s post-block depending on cache state. The handler appears
to NOT enforce a tight chunked-loop deadline — it lets the JuiceFS
prefetch buffer drain before failing. That's user-acceptable (Finder
doesn't beachball, mount stays responsive) but it's a real signal:
adding an explicit per-chunk-fetch deadline (~1s) would tighten this
to a consistent <2s exit and free worker goroutines faster under
sustained backend failure. Belongs to a future tier-1.2 follow-up, not
this slice.

**Next:** iteration 10 picks the second wedge harness — `scripts/wedge-tests/fuse-hang-mid-op.sh`
(SIGSTOP the JuiceFS daemon mid-op, expect Lstat-timeout-bounded
recovery). Pattern reuses this iteration's structure: probe + wedge-
inject + concurrent-stat probe + verdict.

---

### Iteration 8 — 2026-05-16 — LOOP TERMINATED

**Tier:** 1 (Stability).
**Picked:** terminate. PID 41860 still running stale binary (verified
via `scripts/verify-build.sh --running` from iter 7). Per the
commitment in iter 7's STATE.md note, this iteration stops the loop
rather than producing another round of tooling on a binary that
hasn't been swapped.

**Final action before stopping:** kicked off a 1h baseline soak in
the background. When the user resumes the loop (presumably after
restarting with the fresh binary), the comparison data is partially
collected:

  PID:    33513 (jmstress, detached via nohup + disown)
  Output: /tmp/baseline-old-pid41860.jsonl
  Stderr: /tmp/baseline-old-pid41860.stderr
  Config: 3 finder + 1 nle workers, depth 5, 200 MiB min for NLE
  Discovered: 10796 paths, 26 large files
  Duration: 1h (terminates ~05:54 if uninterrupted)

After the swap, run another jmstress 1h soak with the same flags,
then `jmcompare /tmp/baseline-old-pid41860.jsonl new-soak.jsonl`.
That diff is the canonical answer to "did the tier-1 fixes
actually improve real-world behavior?"

**Loop summary across iterations 1-8:**

| Iter | Outcome | Commits |
|------|---------|---------|
| 1 | Concurrent NFS dispatch (tier-1.1) — code landed, NOT validated due to staleness | `691f550`, `99e6a12` |
| 2 | Stress harness scaffold (tier-1.6) | `74a9739`, `3f32005` |
| 3 | Crash-safety acceptance script (tier-1.4) | `5ec1a33`, `ab83389` |
| 4 | Discovered build-staleness bug; fixed build script | `f944a82`, `11986e6` |
| 5 | jmstress --json output (tier-1.6 extension) | `386ac52`, `efc117a` |
| 6 | jmcompare soak-result differ (tier-1.6 extension) | `b5f75bb`, `13d0477` |
| 7 | verify-build symbol checker (dev infra) | `3566bb7`, `4cfcb39` |
| 8 | Loop terminated; 1h baseline soak running | this commit |

**Real tier-1 status as of stop:**
- 1.1: code landed, validation invalidated by staleness, needs re-run
- 1.2: partial; full acceptance pending wedge-injection tooling
- 1.3: depends on user manual testing
- 1.4: tooling shipped; user-driven real run pending
- 1.5: done
- 1.6: scaffold + JSON + comparer + soak running; 24h acceptance run still pending

**To resume:**
1. Confirm fresh build is alive: `bash scripts/verify-build.sh --running`
2. If stale: quit JuiceMount, `open build/JuiceMount.app`, click Start, re-verify
3. Re-fire `/loop`. Iteration 9 will pick up wherever STATE.md points.
4. Optional: check on the baseline soak — `tail -1 /tmp/baseline-old-pid41860.jsonl | jq` for the latest tick, or `pgrep -lf jmstress-bin`.
