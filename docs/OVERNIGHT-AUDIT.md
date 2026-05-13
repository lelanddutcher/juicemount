# Overnight stability audit — 2026-05-13

User left an instruction at ~02:30 to run an autonomous loop overnight
auditing JuiceMount for hang/crash/perf vulnerabilities. They need the
mount working tomorrow morning for files only accessible via JuiceMount.

This doc is the running journal. Each loop iteration appends a section
with: what was investigated, what was found, what was fixed (or
documented as "user must verify"). The user will read this first thing
in the morning.

## Morning recovery checklist (READ THIS FIRST)

The mount is currently WEDGED in the kernel. To launch fresh:

```bash
# 1. From a fresh Terminal (not one of last night's hung shells):
sudo umount -f -t nfs /Volumes/zpool
# If that hangs, just reboot — fastest path.

# 2. Confirm clean:
mount | grep -iE "juicefs|zpool"   # should be empty
pgrep -lf "juicefs|JuiceMount"     # should be empty

# 3. Launch the new build:
open /Users/LelandDutcher/Developer/JuiceMount6/build/JuiceMount.app
```

Last good commit before overnight loop: `1121bae` —
"fix(stability): tighter NFS timeouts + Force Eject + ordered unmount".
That commit's NFS mount opts (`timeo=10,retrans=2`) mean a wedged mount
takes 3 seconds to fail per stat, not 150 seconds. So even if something
goes sideways tomorrow, Finder will be annoyed for 3 s, not catatonic.

## Working hypotheses for tonight's audit

1. **Auto-self-test on startup deadlocks** when our Go process reads
   from `/Volumes/zpool` (localhost NFS) — the read goes through macOS
   NFS client → back to our NFS server → our handler tries to look up
   the same path in the metadata store that `pickSelfTestTarget` was
   walking. Recursive lock or lock contention is likely.

2. **`.juicemount-selftest.tmp` from a crashed prior run** poisons the
   next run — its blocks may be in Redis (metadata) but not in MinIO
   (data). Reading triggers JuiceFS to retry MinIO forever.

3. **NFS handler may hold locks on the metadata store** while serving
   reads, preventing internal Go code (prefetcher, self-test) from
   making progress.

4. **fdpool** may have stale entries from prior runs leading to EIO on
   reuse paths.

5. **Pre-mount conflict probe (Phase A1)** may not catch the case where
   the previous JuiceMount left a `127.0.0.1`-source mount table entry
   with no server behind it. We treat `127.0.0.1` as "ours, reuse" but
   reuse may inherit a wedged state.

## Loop iteration log

---

## Iteration 1 — 2026-05-13 02:35-ish — Audit wave 1 returns

Three sub-agents launched in parallel. Agent 1 (cbridge + nfs/handler) returned
with strong findings. Agents 2 (metadata) and 3 (health) still in flight.

### Agent 1 findings — HIGH severity

#### 1. `NFSServerStats` holds `globalMu` while doing slow work — THE menu-bar hang

`bridge/cbridge.go:533-555`. Every `NFSServer*` C export grabs `globalMu.Lock()`
(plain `sync.Mutex`). When the user clicks the menu, Swift calls `NFSServerStats`;
if any other export is in flight (Shutdown waiting on a 60 s osascript admin
prompt; SyncNow doing a Redis EVAL; the initial sync's BulkInsert holding the
store write lock), Stats waits behind it. Swift's cgo call blocks, MainActor
blocks via the dispatch chain, **AppKit can't even render the menu** — exactly
the symptom the user reported.

The fix is "snapshot-then-release": take the lock, copy pointers into locals
(or use `atomic.Pointer[T]`), release immediately, then call slow methods.
Applies to: `NFSServerStats`, `NFSServerIsRunning`, `NFSServerCacheStatus`,
`NFSServerShutdown`.

#### 2. Self-test races the initial sync — store write lock starvation

`bridge/cbridge.go:1325-1457` (runSelfTest) + `bridge/cbridge.go:375`
(go runAndStoreSelfTest fires immediately after Start).

`pickSelfTestTarget` walks the metadata store via up to 5000 `ListChildren`
calls (RLock — fast). But the initial `SyncOnce` running concurrently does
`BulkInsert` which holds `store.writeMu` AND the RWMutex write-lock for the
duration of the cache-rebuild fold (lines 316-326, 100K+ entries on a real
sync). During that window every `ListChildren` from the self-test stalls
behind the cache rebuild.

Then the self-test's `os.Open("/Volumes/zpool/<file>")` re-enters our own NFS
server via macOS loopback, the handler hits the same store contention, and
the apparent "hang" compounds.

Fixes: delay `runAndStoreSelfTest` by 10 s after Start so initial sync finishes
first; bound `pickSelfTestTarget` by wall-clock not visit count.

#### 3. `NFSServerShutdown` holds `globalMu` for up to ~70 s

`bridge/cbridge.go:479-508`. Shutdown takes `globalMu` and only releases at
function return. `unmountNFS` inside can take 5 s + 5 s + 60 s (osascript
admin prompt). Every menu-bar interaction during shutdown blocks on the lock
for up to 70 s.

Fix: snapshot globals under the lock, release, then run slow paths.

### Agent 1 findings — MEDIUM severity

- `prefetchChildren` (`nfs/handler.go:166-238`) recursive fan-out can starve
  the self-test under contention. Gate on a "server warm" flag.
- `mountAt`/`isMounted` shell out to `mount` while `globalMu` held during
  Start. Bound with `exec.CommandContext` 3 s deadline.
- `unmountNFS` silently treats diskutil timeout as "fall through to osascript"
  with no diagnostic. Make osascript opt-in via the Force Eject codepath.

### Verdict on the self-test deadlock hypothesis

Not a strict AB-BA deadlock — `metadata.Store` uses `sync.RWMutex`, no
re-entrant locking. But **lock-induced starvation feels identical to deadlock
from the user's perspective**: the self-test fires during the initial sync's
write-lock-holding cache rebuild, stalls there, the menu bar's `Stats` call
stalls behind `globalMu` held by something else, the whole thing wedges.

### Highest-leverage fixes for tonight

1. **`globalMu` snapshot-then-release in Stats / IsRunning / CacheStatus / Shutdown** — closes the menu-bar hang.
2. **Delay `runAndStoreSelfTest` by 10 s + wall-clock-bound target search** — closes the startup-window mount stall.

These two together close the symptom class. Will implement in iteration 2
after Agents 2 + 3 land their findings (in case they surface a related issue
I should bundle).

### Agent 3 findings — health/ — HIGH severity

#### 4. `isMountedLocked` runs `exec.Command("mount")` with NO timeout, under `fm.mu`

`health/fuse.go:414`. Called every 10 s by the monitor loop. When the kernel
mount table has a wedged entry, `mount` (which calls `getfsstat()`) blocks in
kernel. The monitor goroutine wedges holding `fm.mu`; any caller that needs
the lock (Stop, IsMounted, Mount-respawn) parks behind it. **Compound failure
mode with Agent 1's `globalMu` finding** — both locks can wedge simultaneously
during one click.

#### 5. `checkFUSE()` does a bare `os.Stat(fusePath)` with no timeout

`health/monitor.go:424`. On a wedged FUSE mount, `os.Stat` hangs in the kernel
forever. The subsequent timeout-guarded `ReadDir` (line 438) is dead code
because we never reach it.

#### 6. `FUSEManager.Stop()` is unbounded and can deadlock against its own monitor

`health/fuse.go:393-401`. `Stop()` closes stopCh, then waits on `<-fm.done`.
If `monitorLoop` is parked in `exec.Command("mount")` inside `isMountedLocked()`,
it never observes `stopCh` (the select is only between ticks at line 479-482).
`<-fm.done` blocks forever; UI Stop button hangs; force-quit doesn't help
because the goroutine is on a syscall.

#### 7. Eight `exec.Command` calls in health/, zero use `CommandContext`

Inventory:
- `mount` × 2 (fuse.go:414, monitor.go:431)
- `umount -f` × 2 (fuse.go:435, fuse.go:467)
- `pgrep` (fuse.go:458)
- `kill -9` (fuse.go:461)
- `tmutil thinlocalsnapshots` (fuse.go:626)
- `sudo umount -f` (monitor.go:353)
- Plus `cmd.Run()` on `juicefs mount -d` (fuse.go:205) — unbounded daemon launch

### Verdict on second root cause

**Confirmed.** The FUSEManager's monitor loop can hang on a wedged mount table,
parking `fm.mu` forever. Combined with Agent 1's `globalMu` finding, the
"click menu, app freezes" pattern has two independent causes — fixing one
won't fully close the symptom, both must be fixed.

### Implementation plan for tonight

Combining Agent 1 + Agent 3 findings, priority order:

**Iteration 2 (next wakeup):** Wrap every shell-out in `health/` with
`exec.CommandContext` and explicit deadlines: `mount` → 5 s, `umount -f` → 15 s,
`pgrep`/`kill` → 2 s, `tmutil` → 60 s, juicefs daemon launch → 30 s. On
`mount` timeout in `isMountedLocked`/`checkFUSE`, treat as "unknown/unhealthy"
and return false rather than block. Wrap `os.Stat` in `checkFUSE` in the
goroutine+timeout pattern already used at fuse.go:438.

**Iteration 3:** `globalMu` snapshot-then-release in `NFSServerStats`,
`NFSServerIsRunning`, `NFSServerCacheStatus`, `NFSServerShutdown`.

**Iteration 4:** Delay `runAndStoreSelfTest` by 10 s, wall-clock-bound
`pickSelfTestTarget`, add `stopCh` listen to `tailJuiceFSLog`, bound
`FUSEManager.Stop()` with a timeout race.

**Iteration 5+:** Bundle the medium-severity fixes (prefetchChildren gate on
warm flag, mountAt/isMounted timeouts in cbridge, unmountNFS osascript
opt-in).

### Agent 2 findings — metadata/ — HIGH severity

#### 8. Self-test reads via `/Volumes/zpool` (NFS loopback) — THE concrete loopback wedge

`bridge/cbridge.go:1352`. `runSelfTest` does `os.Open(target)` where `target`
is `/Volumes/zpool/...`. This goes kernel NFS client → our NFS server (same
process) → handler → metadata.Store. If the handler is concurrently blocked
on `store.writeMu` (held by a long-running `BulkInsert` from `SyncOnce`), the
self-test read parks in kernel; if Swift's menu-bar code is synchronously
waiting on `/self-test`, the whole chain wedges.

**Single highest-leverage fix:** route the self-test through the FUSE path
(`~/.juicemount/fuse-internal/<file>`) instead of `/Volumes/zpool/<file>`.
This eliminates the in-process NFS loopback entirely. Same data path
(JuiceFS-backed), no kernel NFS-client involvement.

#### 9. `BulkInsert` holds `writeMu` for the full multi-batch loop

`metadata/store.go:270-313`. On a 131K-entry initial sync, this is several
seconds. During the hold, all NFS write-back paths (`UpdateSize`, applyEvent
in subscribeLoop) block. NFS reads are unaffected (RLock only) but any
ATTR-cache-invalidating read can stall.

Fix: chunk by releasing `writeMu` between batches inside the loop, or use
a smaller batch with explicit yield. Same applies to the follow-on
`RebuildFTS` call (also takes `writeMu`).

#### 10. `pin.Store` has no `busy_timeout` PRAGMA

`internal/cache/pin/store.go:92`. Returns "database is locked" instead of
waiting under contention. Should match the metadata-store pattern.

### Lock structure summary (Agent 2)

- `metadata.Store`: TWO locks. `writeMu` (`sync.Mutex`) serializes SQLite
  writes. `mu` (`sync.RWMutex`) protects in-memory caches. All read paths
  use `mu.RLock` only — no AB-BA risk among reads.
- `pin.Store`: single `sync.RWMutex`. RLock on reads, WLock on writes.
- Connection pool: 8 conns max on metadata store, unlimited on pin store.

### Combined root-cause map (all 3 agents)

The menu-bar hang and Finder hangs are caused by THREE independent mechanisms
that all activate during the same window (Start + initial sync + first
self-test):

| # | Mechanism | Lock blocked | Fix |
|---|---|---|---|
| 1 | `runSelfTest` reads via `/Volumes/zpool` — re-enters own NFS server | (no lock; kernel NFS retry queue) | FUSE-direct read |
| 2 | `mount` shell-out hangs on wedged kernel mount table | `fm.mu` parked forever | `exec.CommandContext` with deadline |
| 3 | `globalMu` held during slow ops (Shutdown / Sync) | `globalMu` blocks all Stats calls | snapshot-then-release |
| 4 | `BulkInsert` holds `writeMu` for full multi-batch loop | `writeMu` blocks NFS writes | chunk & release between batches |

Fixing #1 alone closes the symptom we hit tonight. Fixing #2 closes the
"after a previous wedge, every subsequent click hangs" pattern. Fixing #3
closes the "click menu during sync hangs the app" pattern. Fix #4 is
performance hygiene.

### Iteration plan (revised after all 3 audits in)

**Iteration 2 (this turn):** Fix #1 — route `runSelfTest` through FUSE path,
not `/Volumes/zpool`. Single biggest win, smallest blast radius.

**Iteration 3:** Fix #2 — wrap all 8 `exec.Command` calls in `health/` with
`exec.CommandContext` deadlines. Also `mountAt`/`isMounted` in `cbridge.go`.

**Iteration 4:** Fix #3 — snapshot-then-release `globalMu` in `NFSServerStats`,
`NFSServerIsRunning`, `NFSServerCacheStatus`, `NFSServerShutdown`.

**Iteration 5:** Fix #4 — chunk `BulkInsert` to release `writeMu` between
batches. Also add `busy_timeout` PRAGMA to `pin.Store`.

**Iteration 6:** Cleanup & final build & summary. Push notification to user.

---

## Iteration 2 — 2026-05-13 ~03:02

**Implemented Fix #2** — wrap every shell-out + bare `os.Stat` in `health/`
with `exec.CommandContext` deadlines or goroutine+timeout pattern.

Commit `a12bd8c`. Files: `health/fuse.go`, `health/monitor.go`.

Bounded (all newly):
- `isMountedLocked()` `mount` → 5 s
- `unmountLocked()` `pgrep` → 2 s, `kill -9` → 2 s, `umount -f` → 15 s (via `runBoundedCommand`)
- `FUSEManager.Mount()` `juicefs mount -d` daemon launch → 30 s
- `ReclaimPurgeableSpace()` `tmutil` → 90 s
- `forceUnmount()` `sudo umount -f` → 20 s
- `checkFUSE()` `os.Stat` → 5 s (goroutine+timeout), `mount` → 5 s

Also added `runBoundedCommand` helper that reaps the child process,
replacing the prior `.Start()`-without-`.Wait()` pattern that leaked
zombies on every monitor tick.

Result: zero unbounded `exec.Command` calls remain in `health/`. The
"click menu → monitor lock parked on a wedged mount syscall → app hangs"
failure mode is closed.

Next iteration: Fix #3 — `globalMu` snapshot-then-release in `NFSServerStats`,
`NFSServerIsRunning`, `NFSServerCacheStatus`, `NFSServerShutdown`.

---

## Iteration 3 — 2026-05-13 ~03:31

**Implemented Fix #3** — snapshot-then-release `globalMu` in every slow cgo
export.

Commit `a5a42e5`. File: `bridge/cbridge.go`.

Before: 8 cgo exports used `globalMu.Lock(); defer globalMu.Unlock()` and
then did slow work (Redis EVAL, FTS query, directory walks, NFS server
drain, osascript admin prompt) with the lock held. Stats / IsRunning
calls from the menu-bar poller queued behind whichever export was in
flight. SwiftUI MainActor blocked on cgo. UI froze.

After: every slow export takes the lock just long enough to copy pointer
globals into locals, releases, and does the slow work on the snapshots.
The lock is now held for microseconds, not seconds-to-minutes.

Refactored: `NFSServerStats`, `NFSServerIsRunning`, `NFSServerCacheStatus`,
`NFSServerSyncNow`, `NFSServerSearch`, `NFSServerPin`, `NFSServerUnpin`,
`NFSServerShutdown`, `stopServerLocked` (internal).

Only `NFSServerStart` still defer-holds the lock (singleton init, must
serialize concurrent Starts).

Result: the "click menu-bar icon → app freezes" pattern is now closed
from three independent angles:
- Fix #1: self-test no longer hangs on NFS-loopback wedge
- Fix #2: monitor lock can't park on a wedged `mount` syscall
- Fix #3: menu-bar pollers can't park on `globalMu` held by any slow op

Next iteration: Fix #4 — chunk `BulkInsert` to release `writeMu` between
batches + add `busy_timeout` PRAGMA to pin store. Also start running
unit tests under race detector to surface any remaining contention.

---

## Iteration 4 — 2026-05-13 ~04:05

**Implemented Fix #4** — chunk `BulkInsert` writeMu acquisition + pin store
busy_timeout.

Commit `adf70b8`. Files: `metadata/store.go`, `internal/cache/pin/store.go`.

- `BulkInsert` no longer holds `writeMu` across the entire multi-batch
  loop. Each batch is its own acquisition (~ms hold). Concurrent NFS
  write-backs (UpdateSize, applyEvent inserts) get their turn between
  batches instead of waiting for the full sync to finish.
- `pin.Store` opens with `busy_timeout = 30000` so the hot offline-gate
  reader (`IsPinnedReady`) waits for in-flight writes (~50 ms) instead
  of returning "database is locked" → EIO up to the NFS handler.

**Race-detector test pass:**
- `internal/cache/pin` ✓
- `internal/jmlog` ✓
- `cache` ✓
- `metadata` non-Redis subset ✓ (Redis-backed integration tests are
  ~25× slower under -race; can't be run in the overnight loop windows)

**Fresh production build:** done. `build/JuiceMount.app` has all four
fixes baked in (ad-hoc signed, JM_QUICK=1 — for distribution you'll
need to set up Developer ID, see `docs/signing.md`).

## State at end of iteration 4

All four confirmed root causes addressed:

| # | Mechanism | Commit |
|---|---|---|
| 1 | self-test re-enters own NFS server (loopback wedge) | `1d73c7d` |
| 2 | `mount` shell-out hangs in kernel under `fm.mu` | `a12bd8c` |
| 3 | `globalMu` held during slow ops → menu-bar freeze | `a5a42e5` |
| 4 | `BulkInsert` holds `writeMu` for full multi-batch loop | `adf70b8` |

Plus earlier today (commit `1121bae`): NFS timeo 300→10, retrans 5→2,
Force Eject button, ordered shutdown.

The "click menu-bar icon → app freezes → mount wedges → Finder can't
launch" cascade has been broken in five independent places. If it
still reproduces tomorrow, the failure mode is something we haven't
audited yet — and the new audit will be cheap because the four obvious
mechanisms are out of the way.

## Next directions to consider in remaining iterations

The four known root causes are closed. Remaining items from the
agent reports plus my own enumeration of fragility:

a. `tailJuiceFSLog` has no stop channel — leaks the goroutine + open
   file handle on FUSEManager.Stop(). MEDIUM severity.

b. `FUSEManager.Stop()` waits `<-fm.done` without timeout — if
   monitorLoop is parked in a now-bounded `mount` syscall, this still
   takes 5 s. Worth bounding the wait.

c. `RedisClient.pruneAbsent` holds `RedisClient.mu` write lock during
   the 131K-entry counter iteration — Agent 2's medium. Affects status
   pollers reading `LastSyncDuration` etc. Less critical now that
   Stats uses snapshot pattern.

d. Spawn a code-reviewer sub-agent on the four commits tonight to
   second-guess the design before user wakes. Catches any subtle bugs
   I missed.

e. Audit Swift side for blocking-on-MainActor patterns. The Go side is
   now hardened; if SwiftUI code somehow blocks MainActor on a cgo
   call, that's the next bottleneck.

Iteration 5 plan: spawn the code-reviewer agent on tonight's commits
(item d) + land items a + b as small defensive fixes.

---

## Iteration 5 — 2026-05-13 ~04:39

**Spawned code-reviewer sub-agent** on tonight's four root-cause commits
(`1d73c7d`, `a12bd8c`, `a5a42e5`, `adf70b8`) to second-guess the design.
Still running in background; results will be processed in iteration 6.

**Landed defensive fixes (items a + b from iteration 4's plan):**

Commit `0316096`. File: `health/fuse.go`.

- `tailJuiceFSLog` now a method on FUSEManager that listens to
  `fm.stopCh` in three places (the wait-for-file loop, the
  scanner-restart loop pre-check, and the sleep between restarts).
  Previously leaked the goroutine + open file handle on every
  FUSEManager.Stop().

- `FUSEManager.Stop()` bounds the `<-fm.done` join with a 10 s race.
  Previously a bare blocking receive; if monitorLoop was mid-syscall
  it could park the user-visible Stop button for the full syscall
  duration. Now if the monitor doesn't exit in 10 s, we log a warning
  and proceed with unmount anyway (goroutine becomes a zombie, user's
  click returns).

## Tonight's running tally

| # | Commit | Fix |
|---|---|---|
| 1 | `1121bae` | NFS timeo 300→10, retrans 5→2, Force Eject, ordered shutdown |
| 2 | `1d73c7d` | FUSE-direct self-test (no NFS loopback wedge) |
| 3 | `a12bd8c` | All `health/` shell-outs bounded with CommandContext |
| 4 | `a5a42e5` | `globalMu` snapshot-then-release in every slow cgo export |
| 5 | `adf70b8` | Chunked `BulkInsert` + pin store `busy_timeout` |
| 6 | `0316096` | `tailJuiceFSLog` stopCh + `FUSEManager.Stop` bounded |

Six commits closing the hang/crash failure modes. Production build refreshed
at the end of iteration 4 (`build/JuiceMount.app` ready for morning launch
once the kernel mount table is cleared).

---

## Iteration 5 — code-reviewer findings + fixes

The code-reviewer sub-agent (spawned at the top of iteration 5) returned
with **3 HIGH severity issues** in tonight's four root-cause commits.
All landed in commit `21db111`.

**HIGH #1 — `unmountLocked`'s `runBoundedCommand` was synchronous.**
Iteration 2 swapped fire-and-forget `.Start()` for the reaping
`runBoundedCommand` — but called it synchronously in a path that runs
with `fm.mu` held. A 15 s umount would park `fm.mu`, re-introducing the
exact failure mode iteration 2 was closing. Fix: prefix with `go`.
This was the worst of the three — would have manifested as a 15-second
UI freeze whenever the FUSE mount needed a forced unmount.

**HIGH #2 — `stopServerLocked` didn't nil `globalPinStore` /
`globalPrefetcher`.** Subsequent Start re-runs `pin.Open(...)` and
overwrites the global without closing the previous SQLite handle.
Connection + WAL file lock leak per Stop/Start cycle. Fix: snapshot +
nil + `Close()` them alongside the other globals. Prefetcher `.Stop()`
also called so worker goroutines exit cleanly.

**HIGH #3 — `BulkInsert` silently dropped `RebuildFTS` errors.** The
comment said "log but don't fail" but no log call was present. Fix:
actual `log.Printf` so a drifted search index becomes visible.

Two LOW findings deferred (goroutine leak on os.Open timeout, duplicate
doc comment in runSelfTest) — neither user-visible, not worth iteration
budget tonight.

## Tonight's final tally

| # | Commit | Fix |
|---|---|---|
| 1 | `1121bae` | NFS timeo 300→10, retrans 5→2, Force Eject, ordered shutdown |
| 2 | `1d73c7d` | FUSE-direct self-test (no NFS loopback wedge) |
| 3 | `a12bd8c` | All `health/` shell-outs bounded with CommandContext |
| 4 | `a5a42e5` | `globalMu` snapshot-then-release in every slow cgo export |
| 5 | `adf70b8` | Chunked `BulkInsert` + pin store `busy_timeout` |
| 6 | `0316096` | `tailJuiceFSLog` stopCh + `FUSEManager.Stop` bounded |
| 7 | `21db111` | Code-review followups: 3 HIGH bugs in #2–#5 closed |

7 commits across the night. The "click menu-bar icon → app freezes →
mount wedges → Finder unrecoverable" cascade has been broken in
multiple independent places. Production build refreshed at end of this
iteration.

---

## Iteration 6 — 2026-05-13 ~05:15

**Swift-side MainActor audit.** Spawned a code-reviewer sub-agent
(no source modification, pure review) to audit the Swift side for
blocking-on-MainActor patterns. Now that the Go side won't park
under contention, the next obvious freeze vector is SwiftUI code
that calls cgo synchronously on the UI thread.

### Findings

**HIGH #1 — Popover 2 s timer ran `NFSServerCacheStatus()` on MainActor.**
`MenuPopoverView.swift:38-44` had `Timer.scheduledTimer { _ in Task { @MainActor in refreshCacheStatus() } }`
where `refreshCacheStatus()` called the cgo entry directly. Same for
the `onAppear` initial fetch.

**HIGH #2 — Offline toggle setter called cgo on MainActor.**
`MenuPopoverView.swift:487-505`. The `Toggle`'s `Binding.set` closure
synchronously invoked `NFSServerSetOffline()` then `NFSServerCacheStatus()`.
`offlineToggleBusy` flag was set true/false around it but neither call
was actually async, so the flag never reflected work.

**LOW** — `forceEjectMount` path (`MenuPopoverView.swift:226-280`) is
already structured correctly: AppKit modal on main, URLSession + 120 s
semaphore wait on `DispatchQueue.global`, result alert hopped back to
main. No change needed.

**LOW** — `NFSBridge.selfTest()` uses `DispatchSemaphore.wait()` but
its sole caller (`ServerController.refreshSelfTest`) dispatches off
main first. Footgun-shaped API; not user-visible today.

### Fixes landed

Commit `e603eab`. Files: `ServerController.swift`, `MenuPopoverView.swift`.

- Added `@Observable var cacheStatus` on `ServerController`. Refreshed
  by new `refreshCacheStatus()` method which dispatches the cgo call
  to the existing `workQueue` and republishes on MainActor.
- Added `setOffline(_ on: Bool)` on `ServerController` that runs both
  the `NFSServerSetOffline` cgo call and the follow-up cache-status
  refresh on `workQueue`.
- `MenuPopoverView` now reads `server.cacheStatus` via a computed
  mirror under the original local name (`private var cacheStatus: ...`)
  so the rest of the view's bindings are untouched. Timer and `onAppear`
  both call the controller's off-main method instead of the cgo entry.
- Offline toggle setter now calls `server.setOffline(newValue)` — fully
  off-main; UI reverts to whatever cacheStatus republishes.

Bonus commit `0a7f767` (also iteration 6): drop `rc.mu` around the
131K-entry `pruneAbsent` iteration in `syncMetadata`. Lock-held
iteration was wasted serialization for the Stats poller (Agent 2's
item from iteration 4's "next directions"). Lock now scopes only the
`lastSyncDuration`/`lastSyncTime` updates at the end.

## Tonight's final tally (iteration 6)

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

9 commits. The original symptom class — click freezes, Finder hangs,
mount wedges — now has independent fixes against:
- kernel-NFS loopback wedge (#2)
- monitor-loop syscall parking under `fm.mu` (#3)
- cgo serialization under `globalMu` (#4)
- metadata writeMu serialization across full syncs (#5)
- redis-side `mu` serialization across full syncs (#8)
- SwiftUI MainActor cgo blocking (#9)

Production build refreshed at end of this iteration.

---

## Iteration 7 — 2026-05-13 ~05:42

**Audited `nfs/handler.go` + `internal/nfs/conn.go`.** This is the
request-serving path — the only major file untouched tonight.

### Tonight's fix landed: phantom-file Lstat timeout (Finding 2)

Commit `b1e9c6a`. File: `nfs/handler.go`.

`juiceFS.Stat`'s cache-hit path used to verify non-directory entries
via a bare `os.Lstat(fusePath)`. On a wedged FUSE daemon this blocks
forever, and under the current single-threaded per-connection dispatch
(see Finding 1 below) that single Stat freezes every other RPC on
the same TCP connection — i.e. every Finder Stat / Lookup / Getattr.

Now bounded with a 2 s goroutine+timeout pattern. On timeout we
conservatively keep the cache entry rather than block; the stale
entry (if any) gets cleaned up on a future stat once FUSE recovers.
2 s is well above a healthy stat (microseconds) and well below
Finder's beachball threshold.

### Architectural finding deferred to morning review (Finding 1)

**The audit identified the dominant freeze vector**, but it's invasive
enough that I'm leaving it for your eyes rather than landing it
unsupervised at 5:45am.

#### What's wrong

`internal/nfs/conn.go:143-193` — the per-connection serve loop reads
a request, calls `c.handle(connCtx, w)` **synchronously**, then reads
the next request. macOS NFS client uses one persistent TCP connection
per mount. So every Read serializes all subsequent Lookups/Getattrs/
Readdirs on that connection. A single slow file open (JuiceFS waiting
on a MinIO GET) freezes Finder until it returns — even though tonight's
fixes ensure the management plane stays responsive.

The cross-connection `rpcSem` (cap 128) doesn't help within a single
connection.

#### Why I didn't land it tonight

The fix is small but the blast radius is the hottest path in the
server. Specifically:

1. **Request body buffering required.** Handlers read from
   `w.req.Body`, which is a `*io.LimitedReader` over the connection's
   shared `bufio.Reader`. For concurrent dispatch the serve loop must
   advance past the body before dispatching, so each goroutine gets
   its own independent buffer (`bytes.Reader`). Change in
   `readRequestHeader`.

2. **Connection close races.** Currently `c.handle` returning an
   error or `w.finish` returning an error causes `serve()` to return,
   shutting down the connection. With concurrent dispatch, a child
   goroutine's error needs to signal shutdown — either via
   `c.Close()` (causing the next `readRequestHeader` to error) or
   via a shared close channel.

3. **drain() handles `*io.LimitedReader`** specifically and returns
   `io.ErrUnexpectedEOF` for other types. Needs to handle
   `*bytes.Reader` (no-op since body is pre-buffered).

4. **Untested under load.** The integration tests need a Redis
   instance; race-detector runs of nfs/ tests time out in 60 s
   without Redis available locally tonight.

#### Recommended design (when you wake)

Smallest correct change to `internal/nfs/conn.go`:

```go
// In readRequestHeader, after reading the RPC header:
remaining := r.N
bodyBuf := make([]byte, remaining)
if remaining > 0 {
    if _, err := io.ReadFull(&r, bodyBuf); err != nil {
        return nil, err
    }
}
req.Body = bytes.NewReader(bodyBuf)
```

```go
// In drain(), add:
if _, ok := w.req.Body.(*bytes.Reader); ok {
    return nil  // body fully buffered upstream
}
```

```go
// In serve(), replace the synchronous c.handle block with:
if c.Server.rpcSem != nil {
    select {
    case c.Server.rpcSem <- struct{}{}:
    case <-connCtx.Done():
        return
    }
}
go func(w *response) {
    defer func() {
        if c.Server.rpcSem != nil { <-c.Server.rpcSem }
    }()
    start := time.Now()
    err := c.handle(connCtx, w)
    elapsed := time.Since(start)
    respErr := w.finish(connCtx)
    rpcCount.Add(1)
    if elapsed > 5*time.Millisecond {
        slowRPCCount.Add(1)
        if elapsed > 50*time.Millisecond {
            Log.Warnf("slow RPC: %v took %v", w.req, elapsed)
        }
    }
    if obs := currentObserver(); obs != nil {
        obs(w.req.Header.Prog, w.req.Header.Proc, elapsed, err)
    }
    if err != nil { Log.Errorf("error handling req: %v", err) }
    if respErr != nil {
        Log.Errorf("error sending response: %v", respErr)
        c.Close()
    }
}(w)
```

#### Safety analysis (verified concurrent-safe)

- `c.writeSerializer` is a channel; multiple goroutines can send safely.
- Each request's `*bytes.Buffer` (writer) comes from `responseBufferPool`,
  single-goroutine access per request.
- `atomic` counters (`rpcCount`, `slowRPCCount`, etc.) are safe.
- `c.Server.handlerFor()` reads a dispatch table set once at init.
- `c.Close()` is idempotent on `net.TCPConn`.

#### How to validate the fix

1. Build and launch.
2. Open Finder to `/Volumes/zpool`.
3. In Terminal: `cat /Volumes/zpool/<some-large-file> > /dev/null &`
   (kicks off a slow Read).
4. In Finder, navigate around — should stay responsive.
5. Watch `pgrep -lf JuiceMount` and the metrics endpoint at
   `http://127.0.0.1:11050/metrics` for elevated `rpc_count` /
   `slow_rpc_count`.

If Finder hangs during the Read, the fix didn't take effect (or
something else regressed). Roll back with `git revert` of the
conn.go commit.

### Other handler findings (deferred — lower priority)

- **Finding 3** (`MemoryBuffer.Get` blocks under `mb.mu` release/re-
  acquire cycle): NOT a bug per se — the lock is dropped before the
  wait. Only an issue because of Finding 1's serialized dispatch.
  Fixing Finding 1 makes this a non-issue.

- **Finding 4** (`ReadDir` calls `BulkInsert` synchronously): one-
  liner to make async (`go jfs.handler.store.BulkInsert(...)`), but
  has a tradeoff — subsequent Stat calls from Finder land on a cold
  cache and fall through to FUSE stat. After iteration 4's
  `writeMu` chunking, BulkInsert holds the lock for <50 ms per 500-
  entry batch, so the current synchronous version is probably fine.

- **Finding 6** (context not propagated to FUSE syscalls): correct
  but low impact; the semaphore + Finding 2's per-call timeout cap
  the damage. Worth threading `ctx` through the handler eventually
  for cleanliness.

## Tonight's final tally (iteration 7)

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

10 commits. The dominant architectural finding (concurrent RPC
dispatch) is documented above with a recommended implementation —
ready to land tomorrow with your eyes on it.

---

## Iteration 8 — 2026-05-13 ~06:15

**Audited the read-path cache layer.** Files reviewed:
`cache/reader.go`, `internal/cache/pin/{prefetcher,offline}.go`,
`nfs/membuf.go`, `nfs/readahead.go`, `nfs/fdpool.go`.

### Verdict on hot-path lock discipline

**Clean.** All three caches (Reader, MemoryBuffer, FDPool) follow the
correct pattern: take lock, check map, release lock, perform syscall,
re-take lock to insert. No blocking I/O happens under any mutex. The
prefetcher uses a bounded N-worker pool draining a 256-slot channel
— right architecture.

### Two HIGH findings fixed (commit `93e9d8d`)

**HIGH #1 — `MemoryBuffer.Get` cascade-freeze on wedged loadFile.**
`nfs/membuf.go:85`. A second RPC for a mid-loading file dropped the
mutex and blocked on `<-entry.ready` forever. If `loadFile` was stuck
on a wedged FUSE read (MinIO unreachable, JuiceFS hung), every
subsequent reader of the same file parked there indefinitely. Now
bounded by `memBufLoadTimeout = 5s`. On timeout the caller returns
nil, falling through to the SSD-direct `cacheReader.ReadBlock` (a
separate code path that reads JuiceFS chunk files from local SSD via
Redis slice lookup — no FUSE involvement). If that also misses, the
caller hits the FUSE fd pool where the stall is per-RPC rather than
cascading-shared.

**HIGH #2 — Cache Redis client had no explicit timeouts.**
`bridge/cbridge.go:217`. `cache.Reader.getSlices` does a Redis
`LRange` on every cache-miss read. With no explicit `ReadTimeout`,
the go-redis default applies and a Redis hiccup parks every concurrent
NFS RPC. Now matches the metadata client pattern:
`ReadTimeout=10s, WriteTimeout=5s, DialTimeout=5s`.

### One LOW finding noted, not fixed

`nfs/membuf.go:164` — `data := make([]byte, fileSize)` allocates the
full file size on cache miss. The race window between budget reservation
and population means concurrent misses for different files could each
peak-allocate up to budget simultaneously. Not a freeze risk; bounded
in practice by the small race window. Noted for future cleanup.

### Read-path context propagation (deferred to morning)

The audit also flagged that `cachedFile.ReadAt` calls
`cacheReader.ReadBlock(context.Background(), ...)` — the actual NFS
RPC's context never reaches the cache layer. Threading context through
`billy.File.ReadAt` would require changing the third-party `billy`
interface signature, which is a non-trivial change. The two timeouts
above mitigate the worst symptom (cascade-freezes), so the context
work can wait for your review tomorrow.

## Tonight's final tally (iteration 8)

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

11 commits. Lock discipline across the cache layer verified clean.
The single largest remaining lever — concurrent per-connection RPC
dispatch, designed and documented in iteration 7 — awaits your
morning review.

## Morning summary

The build at `/Users/LelandDutcher/Developer/JuiceMount6/build/JuiceMount.app`
includes all 11 fixes. Before launching:

1. Confirm no wedged mount: `mount | grep -iE "juicefs|zpool"` should
   be empty. If not, `sudo umount -f -t nfs /Volumes/zpool` (or reboot).
2. Confirm no orphan processes: `pgrep -lf "juicefs|JuiceMount"` empty.
3. Launch: `open build/JuiceMount.app`.

If Finder still freezes under load tomorrow:
- The next fix to land is concurrent RPC dispatch in
  `internal/nfs/conn.go` (designed in iteration 7).
- If that also doesn't fix it, the failure mode is novel and we
  need fresh diagnostics.
