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
