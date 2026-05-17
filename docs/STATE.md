# JuiceMount development state

The canonical "what's done / what's next" file for the autonomous
development loop. Driven by `docs/VISION.md` tier acceptance tests.

Format: one entry per iteration. Each entry declares which tier it
touched, what shipped, what's still broken, and what the next
unblocked item is.

---

## QA findings (pending investigation)

User-reported issues from live use, not yet diagnosed or assigned to
an iteration. These should be triaged before tier-1 advances —
they're real-world correctness signals that synthetic harnesses miss.

### QA-9 (2026-05-17) — pin progress feels stuck at scale — ⚠ landed-needs-validation 2026-05-17

**Fix (Loop A.9, iter 21, 2026-05-17 ~03:30):**

Added a live MB/s rate readout to the popover cache row, displayed
between "N pinned" and "X MB / Y MB" while PendingFiles > 0 and
the measured rate is >= 0.5 MB/s. Computes the rate from successive
CachedBytes samples at the existing 2s polling cadence — faster
polling would just produce noisier numbers; the perception bug is
solved by SHOWING activity, not sampling more often.

State: `prevCachedBytes`, `prevCachedAt`, `pinRateMBps`. Reset to
0 when nothing pending so a stale rate doesn't linger after a pin
completes. Pure UI change — no bridge involvement.

This is the perception-bug fix that the original QA-1 report
actually wanted: at scale (whole-root pin), the popover used to
look stuck at "0 B / 50 GB" for minutes while the prefetcher
drained the queue. Now it shows "120 MB/s · 5 GB / 50 GB" so the
user sees progress.

Validation pending binary swap: user pins a 1+ GB folder, observes
the blue "X MB/s" appear next to the byte counter while it climbs.

---

### QA-1 (2026-05-17) — pinned folders not downloading — ✓ CLOSED 2026-05-17 (could not reproduce)

**Original observation:** marking a folder as pinned does not cause
its bytes to download.

**Investigation outcome (Loop A iter 15-16, 2026-05-17 ~02:55):**

Could not reproduce after the clean reset documented in QA-7a.
Tested at three scales against fresh binary ba47621+:
  - 41 MB (Bolts, pre-cached): pin returned in 34ms with ReadyFiles=7
    instant — bytes were already in JuiceFS chunk cache.
  - 502 MB (VOs): cold-cache, 0 → 100% within 3s. CachedBytes climbed
    +526 MB in one watcher tick.
  - 6.6 GB (GRAINS & DUST): 0 → 100% within 6s (1.8 GB/s observed,
    indicating warm chunk cache); CachedBytes counter climbed
    cleanly across ticks.

The /pin HTTP endpoint and the cgo NFSServerPin (which the Swift
UI calls via NFSBridge.pin) route to the SAME pinStore.PinMany
code, so the UI path can't behave differently from the HTTP path.

**Hypothesis for the original report:** the symptom was the
combination of (a) the degraded mount environment (Redis "no route
to host", FUSE daemon dead, auto-offline engaged) that we found at
loop resume earlier in this session, plus (b) the user accidentally
pinned the entire mount root, which floods the prefetcher with
thousands of files and the popover's CachedBytes/TotalBytes counter
appears stuck near zero for minutes while the queue drains. Not a
"doesn't work" — a "doesn't work fast enough at runaway scale."

QA-9 (added below) tracks the UX side of this: pin progress should
SHOW activity at scale even when fewer percent of the bytes are
cached, so users don't see a flat 0 KB readout. Goal A.9 in the
current loop will fix that via the popover rate readout.

### QA-2 (2026-05-17) — offline toggle blocks cached files — ✓ CLOSED 2026-05-17 (works correctly)

**Investigation outcome (Loop A iter 18, 2026-05-17 ~03:10):**

Could not reproduce. The handler's read path at nfs/handler.go:994
(`cachedFile.ReadAt`) services reads in priority order:
  1. MemoryBuffer (memBuf) — hits return immediately
  2. SSD cache reader (cacheReader) — hits return immediately
  3. FUSE fallthrough — ONLY at this point does the
     `pin.IsOffline() && !f.pinned` gate at line 1044 fire

So cached content is ALREADY served before the offline gate is even
consulted. This is exactly what QA-2 asked for.

Tested with offline toggled true:
  - Pinned file (VOs): read worked, 172ms (served from pin cache)
  - Cached-but-unpinned file (Bolts, 10 MB full read): worked, 16ms
  - Fresh-untouched file (OVERLAYS): **failed fast with exit=1 in 7ms**
    — the gate fires correctly only for genuinely uncached content

The OpenFile gate at line 745 enforces the same logic at open-time
(refuses unpinned files when offline) — but only after the cache
readers' attempts have completed. So the user's hypothesis ("gate
fires too eagerly even when bytes are in cache") doesn't match the
code: the gate only refuses when both cache layers MISS.

Like QA-1 and QA-5, the original report was likely environmental
(degraded FUSE/Redis state) rather than a code bug.

KEEPING ORIGINAL ENTRY BELOW for archeology — but the symptom is
not currently reproducing in a clean environment.

---

### QA-2 (original report, 2026-05-17) — offline toggle blocks cached files

**Observed:** toggling offline in the app lets navigation continue
(good — directory listings work), but files the user JUST opened or
copied to the JuiceMount become unreadable in offline mode. Those
files should still be in the local JuiceFS chunk cache and therefore
readable even when the network path to MinIO is blocked.

**Reporter:** user, manual test.
**Hypothesis (user):** the offline gate may be returning fail-fast
on EVERY non-pinned read, not just reads that would miss the cache
— effectively treating the cache as if it didn't exist when offline.
**Where to investigate:** the offline gate in
`internal/nfs/nfs_on_read.go` and `nfs_on_lookup.go`, plus how it
interacts with `cache.Reader`'s in-flight + recently-cached chunk
state. The fail-fast sentinel `pin.ErrOfflineNotAvailable` was added
in `54b744b` for the un-pinned-when-offline-disconnected case; it
may be firing too eagerly, ignoring that the data is actually
present in JuiceFS's chunk cache or in JuiceMount's MemoryBuffer.
**Repro hint:** mount up online, open a small file (gets cached),
toggle offline via the menu bar, try to open the same file again.
Expected: opens from cache. Observed: fails.

**Why this matters:** if confirmed, offline mode is defeating its
own value proposition — the whole point of having a local cache is
that recently-touched files survive a network drop. The tier-1.7
acceptance test ("pinned files keep working when network drops") is
✓ validated for PINNED files, but does NOT exercise the cached-but-
unpinned case. The acceptance test may need to be tightened, AND
the gate logic may need to be fixed.

### QA-3 (2026-05-17) — no way to clear the cache — ⚠ landed-needs-validation 2026-05-17

**Fix (Loop A.7, iter 20, 2026-05-17 ~03:30):**

Backend: new POST /cache-clear admin endpoint in bridge/cbridge.go.
Walks ~/.juicefs/cache/{uuid}/raw/chunks/ and removes every chunk
file. Optional ?keep-pinned=true triggers an internal
globalPrefetcher.VerifyAndRepair afterward so pinned content
immediately starts re-downloading. atomic.Bool gate prevents
concurrent calls (matches /stop pattern).

UI: "Clear Cache" button added next to Reclaim in the disk-space
row. Calls /cache-clear?keep-pinned=true so pinned content
recovers automatically. Extracted as @ViewBuilder cacheClearButton
to avoid code duplication across the high-/normal-pressure UI
branches.

Code-reviewer pass: 2 HIGH + 2 MEDIUM all addressed:
  - HIGH-1: filepath.Walk now distinguishes ENOENT (benign — dir
    doesn't exist) from real walk failures (logged via jmlog.Warn);
    per-chunk Remove errors that aren't ENOENT also logged.
  - HIGH-2: VerifyAndRepair goroutine now uses globalPinCtx (cancels
    on shutdown) instead of context.Background — 30s timeout kept
    as safety belt with comment clarifying it only bounds the
    DB-mark phase, not the actual re-download work in PullPending.
  - MED-1: atomic.Bool concurrency gate matching /stop pattern.
  - MED-2: button extracted to @ViewBuilder.
  - 2 LOW deferred (URLSession semaphore pattern is project-wide
    convention).

Validation pending binary swap: user reopen, hit "Clear Cache",
observe cache GB drop in disk-space row, then climb back up as
pinned content re-downloads.

---

### QA-3 (original report, 2026-05-17) — no way to clear the cache

**Observed:** no surface (UI button, /admin endpoint, CLI command)
to evict cached files. Users who pin too aggressively or want to
reset state have to `rm -rf ~/.juicefs/cache` manually.

**Where to investigate:** add `/cache-clear` to the admin route
table in `bridge/cbridge.go` (alongside `/reclaim` which already
clears APFS purgeable space). Two layers to potentially target:
JuiceMount's own `MemoryBuffer` + `cache.Reader` state, and the
JuiceFS chunk cache on disk. A "clear all" should hit both;
"clear unpinned" should preserve pin-store contents. The UI hook
goes in `app/JuiceMount/Sources/JuiceMount/UI/MenuPopoverView.swift`.

### QA-4 (2026-05-17) — no un-pin UI in the popover — ⚠ landed-needs-validation 2026-05-17

**Fix (Loop A.5, iter 19, 2026-05-17 ~03:15):** added a per-row
"−" button to the pinned-folders list in MenuPopoverView. Calls
NFSBridge.unpin off the main thread (matches existing
triggerVerifyPins pattern), refreshes cache-status afterward so
the row vanishes. No confirm dialog — un-pin is non-destructive
(cache stays until eviction or future /cache-clear endpoint).

Pin store's internal Unpin already cancels in-flight prefetch for
the path (per pin.Store.Unpin implementation), so the "cancel
prefetch" requirement is implicit.

Button uses `.buttonStyle(.borderless)` to preserve keyboard focus
traversal (vs .plain which suppresses it). Code-reviewer pass
flagged this and one threading concern (global concurrent queue vs
workQueue serial); the threading concern is deferred — matches
existing pattern, and Go pin store is internally serialized via
SQLite mutex so concurrent callers are safe.

Validation pending: requires user to (a) quit + reopen JuiceMount
on fresh binary, (b) verify the new "−" button appears next to
each pinned root, (c) click it to confirm the row disappears + the
ReadyFiles aggregate drops.

---

### QA-4 (original report, 2026-05-17) — no un-pin UI in the popover

**Observed:** the pinned-folders list in the menu-bar popover
shows what's pinned but has no way to un-pin. The `/unpin` HTTP
endpoint exists (`bridge/cbridge.go`, handleUnpinHTTP) but isn't
wired to any UI control.

**Where to investigate:** add a per-row action (swipe-to-delete,
right-click menu, or a trailing "−" button) on the pinned-folders
list in `MenuPopoverView.swift` that calls POST /unpin. Same shape
as the existing pin enqueue. Should also stop any in-flight
prefetch for that path and free its cache footprint (the prefetcher
needs a cancellation path — verify it has one before wiring this up).

### QA-5 (2026-05-17) — "Sync Now" doesn't re-trigger pinned downloads — ✓ CLOSED 2026-05-17 (works correctly)

**Original observation:** Sync Now doesn't restart pin downloads.

**Investigation outcome (Loop A iter 17, 2026-05-17 ~03:00):**

Validated end-to-end. The Sync Now button at MenuPopoverView.swift:781
calls BOTH `server.syncNow()` and `triggerVerifyPins()` (line 786);
triggerVerifyPins POSTs to /verify-pins which returns ok=true with
the re-enqueue count.

Test: with 27 files / 7.65 GB pinned, deleted a JuiceFS cache chunk
to simulate eviction, then POST /verify-pins → returned
`reenqueued:27, total_pinned:27, ok:true`. Within 2 seconds of the
call, all 27 files were back to ReadyFiles=27, CachedBytes=7.65 GB
(prefetcher picked up the re-enqueued work, served most from chunk
cache, re-pulled the evicted one).

Like QA-1, the original report was likely tied to the same broken-
environment scenario. The fix that landed in 9a1f229 ("Sync now also
re-verifies pinned coverage") is doing the right thing.

### QA-6 (2026-05-17) — Pin folder dialog blocks directory navigation — ⚠ landed-needs-validation 2026-05-17

**Root cause (Loop A.6, iter 20, 2026-05-17 ~03:10):** JuiceMount
is an LSUIElement accessory app (no Dock icon, menu-bar only). When
NSOpenPanel.runModal() runs from inside the popover, macOS does NOT
auto-promote the app to foreground. The panel APPEARS but focus and
click events fall through to whatever full-app was previously
foreground — textbook "panel visible, clicks don't register"
symptom.

**Fix:** added `NSApp.activate()` immediately before runModal so
the panel becomes the keyWindow with focus. Also set:
  - `treatsFilePackagesAsDirectories = true` so video-production
    packages (.photoslibrary, .fcpbundle, .drp) can be descended
    into (side-effect: .app bundles also traversable — harmless).
  - `canCreateDirectories = false` (we're pinning existing folders).
  - `showsHiddenFiles = false`.

Code-reviewer pass: 2 MEDIUM both addressed — switched to the
non-parameterized `NSApp.activate()` form (macOS 14+ API; old form
deprecated in Sonoma), and added a comment noting the .app descent
side-effect.

Validation pending binary swap: user quit + reopen, click Pin
Folder for Offline, verify panel opens AND clicks into
subdirectories navigate normally.

---

### QA-6 (original report, 2026-05-17) — Pin folder dialog blocks directory navigation

**Observed:** clicking "Pin folder for offline" opens a Finder-
style picker, but clicking into subdirectories doesn't work. The
picker is functionally unable to select anything but the top-level
mount root, so pinning anything specific is impossible from the UI.

**Where to investigate:** the picker code is likely an NSOpenPanel
in `MenuPopoverView.swift` or a sibling. Things to check:
(a) `canChooseDirectories=true` but no `canChooseFiles=false`
override might be blocking double-click navigation; (b) the panel's
allowed file types might be filtering out directory entries on the
JuiceMount NFS volume; (c) the panel might need
`resolvesAliases=false` for the JuiceFS-backed paths; (d) the panel
might be running on the wrong queue and double-clicks aren't being
delivered. Quick local repro: use any NSOpenPanel-based file picker
in another app against /Volumes/zpool-dev — if those work and
JuiceMount's doesn't, the bug is in our panel config, not the mount.

---

### QA-7a (2026-05-17) — pin store fd leak after Stop — ⚠ landed-needs-validation 2026-05-17

**Root cause (Loop A.11, iter 22, 2026-05-17 ~03:50):**

`Prefetcher.Stop()` does `close(stopCh); close(jobs); p.wg.Wait()`.
But `p.wg` only tracked the worker-loop goroutines (registered in
`NewPrefetcher`). The two LONG-RUNNING daemons launched from
cbridge.go — `PullPending` and `ReWarmupLoop` — were spawned via
plain `go globalPrefetcher.X(...)` with no wg tracking. So
`wg.Wait()` returned while those loops were mid-query on the pin
store, and `pinStore.Close()` (immediately after, in
stopServerLocked) raced with SQLite connections still checked out
→ leaked file descriptors on every Stop cycle.

**Fix:** new `Prefetcher.Go(fn)` helper that does `p.wg.Add(1)`
synchronously then `go func(){ defer wg.Done(); fn() }()`. The Add
MUST happen synchronously with the launch (not inside the goroutine
body) — otherwise `Stop().Wait()` could complete before the
goroutine got a chance to register itself, and a subsequent `Add`
on a zero-counter wg with active Wait is undefined per Go docs.

cbridge.go now spawns the two daemons via `pf.Go(func() { pf.X() })`,
with `pf` snapshotted into a local so the closures don't TOCTOU
against a future re-Start's globalPrefetcher.

Bonus cleanup along the way: replaced the historical structural-
interface `contextLike` type alias with `context.Context` directly
for `globalPinCtx`. Removes the runtime type-assertion in the
launch sites.

Code-reviewer pass: 2 MEDIUM addressed (closure capture, contextLike
removal), 1 LOW addressed defensively (`Go()` returns silently if
called after Stop closed the stopCh, vs panicking).

Validation pending binary swap: user reopens, pins a folder, clicks
Stop, verifies `lsof ~/Library/Application\ Support/JuiceMount/pin.db`
shows zero fds (vs the 9 + 2 we saw on the broken binary).

---

### QA-7a (original report, 2026-05-17) — pin store fd leak after Stop

**Observed during QA-7 triage:** after clicking Stop, the
JuiceMount app process held the pin.db file open with 9 file
descriptors and the pin.db-wal with 2, even though stopServerLocked
documents that pinStore.Close() runs as part of the soft-stop
sequence. The leak prevents a clean pin-store reset from outside —
e.g. moving pin.db aside requires killing the app first.

**Where to investigate:** stopServerLocked at bridge/cbridge.go line
494 nils globalPinStore under globalMu then calls pinStore.Close()
outside the lock. Either Close() isn't fully releasing the fds, OR
some OTHER component (Swift UI showing the pinned list?) is also
holding pin.db open via a different code path. Inspect with
`lsof <path-to-pin.db>` mid-Stop to confirm.

**Recovery (used 2026-05-17 to clear runaway pins from QA-1 spiral):**
1. Quit JuiceMount fully (kill app PID)
2. Kill all juicefs daemons matching fuse-internal
3. Move pin.db/pin.db-shm/pin.db-wal to backups/
4. Re-open JuiceMount.app — pin store recreates empty

### QA-7 (2026-05-17) — Stop button doesn't fully stop JuiceMount — ⚠ landed-needs-validation 2026-05-17

**Fix (Loop A.10, iter 21, 2026-05-17 ~03:30):** user-approved
two-button design (AskUserQuestion → user picked "rename + add"):

- "Stop mount and finish sync" (new orange pause-icon button):
  unmounts NFS so /Volumes/<name> disappears, tears down NFS server
  + metadata + caches + metrics, but DELIBERATELY leaves FUSE +
  JuiceFS daemon alive. Next Start reuses the existing mount — no
  admin-password re-prompt.

- "Stop everything" (red stop-icon button, with confirmation
  dialog): full teardown via NFSServerShutdown — also unmounts FUSE
  and kills JuiceFS daemons.

Implementation: new cgo export NFSServerStopMount in bridge/cbridge.go,
NFSBridge.stopMount() Swift wrapper, ServerController.stopMount() —
all mirror existing stop()/NFSServerShutdown plumbing for
consistency. Confirm dialog on "Stop everything" prevents mis-taps
mid-ingest. Defensive fix added to NFSServerStart: if globalFUSE is
non-nil but fuseLooksHealthy rejects it (the post-stopMount edge
case), stop the old FUSEManager before overwriting — prevents
monitor-goroutine leak.

Verify-build manifest gained main.NFSServerStopMount + main.handleCacheClearHTTP
so future staleness on these landing fixes is detectable.

Code-reviewer pass: 1 HIGH (FUSEManager double-mount risk on Start
after stopMount) fixed defensively; 1 HIGH (double-click start race)
deferred as it matches existing stop() behavior, not a regression;
1 MEDIUM (no confirm on destructive button) addressed with
SwiftUI .confirmationDialog; 1 MEDIUM (FUSEPath stale on config
change) deferred — config changes are rare and would need a fuller
Start refactor.

Validation pending binary swap: user reopens, sees TWO stop
buttons, exercises both, verifies the orange one keeps FUSE alive
for fast restart and the red one fully tears down with confirm
dialog.

---

### QA-7 (original report, 2026-05-17) — Stop button doesn't fully stop JuiceMount

**Observed:** clicking Stop in the menu bar ends the NFS share but
leaves JuiceFS and the JuiceMount background processes running.
User-visible symptom: "Stop" doesn't feel like stop — things are
still happening.

**Root cause (already in code, not a bug per se but a UX mismatch):**
the menu-bar Stop button calls the cgo `NFSServerStop` export, which
is a deliberate SOFT stop — tears down NFS server + metadata + caches
+ metrics listener, but intentionally leaves FUSE/NFS mounts in
place so subsequent Start cycles don't require admin password
re-prompt for re-mount (per the comment in `bridge/cbridge.go`
line 463-474). `NFSServerShutdown` is the HARD stop that also
unmounts. The Swift app never wires the hard-stop into a UI control.

**Two possible fixes (pick at investigation time, not bundled with
the user-reported symptom):**

  1. Rename the menu-bar action: "Stop" → "Stop NFS Server" to set
     user expectation. Add a separate "Quit JuiceMount" or "Stop
     Completely" that calls NFSServerShutdown. Preserves the
     fast-cycle property for power users.

  2. Change Stop to call NFSServerShutdown unconditionally. Matches
     naive user expectation. Costs the fast Start-Stop cycle (admin
     re-prompt on each remount) unless we also wire the
     passwordless-sudo path the offline-resilience harness uses.

**Where to investigate:** `app/JuiceMount/Sources/JuiceMount/UI/
MenuBarController.swift` for the Stop action handler. The Stop
button likely calls `nfsBridge.stop()` (which maps to NFSServerStop);
swapping to `nfsBridge.shutdown()` (or similar — confirm the
NFSBridge.swift naming) plus a confirm-dialog would land fix #2 in
~30 min. Fix #1 is more like 1-2 hours (new menu items, two code
paths, copy revision).

**Why this matters for autonomous testing:** the testing loop needs
a reliable "tear it all down and re-init" path. Right now
"click Stop → click Start" leaves stale FUSE state behind. The
/stop endpoint shipped in iter 12 is the soft-stop too — for a true
clean-slate cycle we either need a /shutdown endpoint or to fix the
underlying UX confusion here.

---

## Pattern: pin/offline subsystem needs a dedicated investigation

QA-1, QA-2, QA-5, QA-6 all point at the pin/offline subsystem.
Together they suggest pinning is end-to-end broken right now:
can't pick a folder (QA-6), pinning doesn't download (QA-1),
Sync Now doesn't recover (QA-5), and even cached content fails
offline (QA-2). Recommend a dedicated investigation iteration —
not piecemeal fixes — that traces a single end-to-end pin
operation through the UI, the bridge, the pin store, the
prefetcher, JuiceFS, and MinIO, with a logger at every hop. The
fixes likely cascade from one root cause.

---

## Tier 2 — App polish (in progress)

| # | Slice | Status |
|---|---|---|
| B.2 | Self-test dashboard in popover | ⚠ landed-needs-validation 2026-05-17: healthDotsRow shows 4 colored dots (R/M/F/N) + rolling MB/s, click-to-copy diagnostic |

---

## Tier 3 — Server packaging (iter 1+2, partial)

| # | Test | Status |
|---|---|---|
| 3.1 | Cold-deploy on Ubuntu Server 24.04 LTS | ⚠ iter 1+2 of N landed: minio + redis + juicefs-init + **juicemount-server** all in `server/docker-compose.yml`. Cold-deploy timing pending Docker daemon start; compose config validates cleanly. |
| 3.4 | Healthchecks | ⚠ healthchecks defined on all 4 services + Dockerfile HEALTHCHECK against /health |
| 3.2/3.3/3.5/3.6/3.7 | Synology / Configuration / doctor / Backup / Upgrade | not started; depend on iter 3+ (Caddy, doctor command) |

---

## Active tier: Tier 1 — Stability

Acceptance tests (from `docs/VISION.md`):

| # | Test | Status |
|---|---|---|
| 1.1 | Concurrent per-connection NFS dispatch (Finder browses while a long Read is in flight) | ⚠ landed in `691f550`; **prior validation invalidated by `f944a82` build-staleness bug; needs re-validation against the fresh binary** |
| 1.2 | No Finder freeze on any wedged backend | ⚠ all 3 iter-B wedge harnesses shipped (`minio-down-mid-read` iter 9, `fuse-hang-mid-op` iter 10, `nfs-loopback-mid-shutdown` iter 13); first two PASS validated against live mount, third awaits binary swap (needs iter-12's `/stop` endpoint) |
| 1.3 | Clean unmount in every state | ✓ likely (ordered shutdown + Force Eject landed) — needs real validation |
| 1.4 | Crash-safe metadata (kill -9 → mountable in <5s) | ⚠ test tooling shipped in `5ec1a33`; real run pending |
| 1.5 | Recovery diagnostics (Export Diagnostics zip) | ✓ landed in Phase B |
| 1.6 | Stress test harness (24h CI run) | ⚠ scaffold landed in `74a9739`; goroutine-leak watchdog (iter D) landed in iter 11; 24h soak run pending |
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

### Iteration 14b — 2026-05-17 — LOOP TERMINATED (binary-swap blocked)

**Tier:** 1 (Stability).
**Outcome:** stopped after iter 14 verify-build update. Continuing
would stack more code on the same validation debt — 5 iterations
(11-14) have shipped commits that need the user's binary swap to
runtime-validate. The iter-8 termination pattern repeats: at some
point the cost of more unvalidated code outweighs its benefit.

**Pending runtime validation (in order of effort to validate):**

1. **Restart JuiceMount on fresh binary**:
   - Quit JuiceMount (menu bar → Quit, or `kill 42644`)
   - `open build/JuiceMount.app`
   - Click Start
   - `bash scripts/verify-build.sh --running` should show 11 ✓ green
     fixes and "process started after binary was built — likely current"

2. **Validate iter-12 /stop endpoint**:
   - `curl -s -X GET http://127.0.0.1:11050/stop -o /dev/null -w '%{http_code}\n'`
   - Expected: `405` (was `200` on stale binary)

3. **Validate iter-13 NFS-shutdown harness** (DESTRUCTIVE):
   - `scripts/wedge-tests/nfs-loopback-mid-shutdown.sh`
   - Leaves JuiceMount in stopped state; restart via Start button
     when done

4. **Validate iter-11 goroutine watchdog** (long-running):
   - `cmd/jmstress --mount /Volumes/zpool-dev --duration 1h --json
     --periodic-json 60s --goroutine-tick 60s --goroutine-warmup 5m
     > /tmp/soak-1h.jsonl`
   - Look for `goroutines` block in each tick; `breaches: 0` at the
     end means no leak in this window.

5. **Tier-1.6 24h soak** (the real acceptance gate):
   - Same command, `--duration 24h`. Run when network won't be
     touched for that long.

**Commits this session (iter 9-14):**

| Iter | Commit  | Theme                                         |
|------|---------|-----------------------------------------------|
| 9    | cb04f56 | MinIO-down-mid-read wedge harness (tier-1.2)  |
| 10   | 76695da | FUSE-hang-mid-op wedge harness (tier-1.2)     |
| 11   | 09906a6 | Goroutine-leak watchdog in jmstress (tier-1.6)|
| 12   | ba47621 | POST /stop admin endpoint                     |
| 13   | 84af5a5 | NFS-loopback-mid-shutdown wedge harness       |
| 14   | d07de1d | verify-build manifest entry for /stop         |

Iter 9 and 10 PASS-validated against live mount in-session. Iter 11
smoke-validated; needs 1h+ run to be confidence-tested. Iter 12-14
all need the binary swap.

**Tier-1 state at stop:**
- 1.1: code landed, validation invalidated by old binary-staleness,
  needs re-run with current binary
- 1.2: ALL 3 iter-B wedge harnesses shipped (MinIO/FUSE/NFS); MinIO
  and FUSE validated; NFS needs binary swap
- 1.3: depends on user manual unmount-matrix
- 1.4: tooling shipped; user-driven real run pending
- 1.5: ✓ done
- 1.6: scaffold + JSON + comparer + watchdog all in; 24h soak
  acceptance run still pending
- 1.7-1.10: ✓ all validated

**Live mount state at stop:**
- PID 42644 still on May-16 stale binary
- FUSE daemon died at some point overnight
- Redis ping fails ("no route to host")
- Auto-offline correctly engaged at 00:41:18
- Mount needs restart to be usable

**To resume:** after restart, re-fire `/loop`. Iteration 15 will
pick up wherever STATE.md points.

---

### Iteration 14 — 2026-05-17

**Tier:** 1 (Stability) — dev-infra slice that hardens future
staleness detection.
**Picked:** add iter-12 /stop endpoint to verify-build manifest.

**Shipped (commit pending):**
- `scripts/verify-build.sh`: added two new entries to the symbol-
  table FIXES manifest:
  - `main.handleStopHTTP` — the POST /stop handler function
  - `main.stopInProgress` — the atomic.Bool gating concurrent /stops
  Both symbols verified present in the current on-disk
  build/JuiceMount.app via nm -a. Without these entries, future
  iterations could ship a build that silently lost the /stop endpoint
  (the same class of bug iter 4 discovered with the Lstat timeout).

**Validated:** `bash scripts/verify-build.sh` reports 11 ✓ entries
(up from 9), all green. `bash scripts/verify-build.sh --running`
still correctly flags the live PID 42644 as stale (it predates the
iter-12 build at mtime May 17 00:44:53).

**Tier-1 status:** no acceptance-test changes; this is
dev-infrastructure that makes future code-staleness incidents
catchable in seconds, the same way iter-7 saved iter-4-class bugs.

**Live mount status (unchanged from iter 13):** still stale,
auto-offline still engaged, FUSE daemon still down. User restart
needed; iter 13's NFS-shutdown harness + iter 12's /stop endpoint
all wait on that.

**Broken:** nothing.

**Next:** iteration 15 picks one of:
  - Wait for user binary swap + validate iter-13's NFS-shutdown
    harness end-to-end (closes the last tier-1.2 gap).
  - /shutdown endpoint (full teardown — stop + unmount FUSE + NFS).
  - Tier-2 work (since tier-1 acceptance tests are almost all
    landed-needs-validation rather than blocked-on-code).
  - Tier-1.4 crash-recover-test --real (needs user-driven destructive
    operation against the live mount).

Several iterations now have been "ship code, blocked on user
restart for runtime validation." Pivoting to tier-2 (UI/UX) work
that doesn't depend on the mount state would unblock continued
autonomous progress. Picking that for iter 15 unless the binary
swaps before the next wake.

---

### Iteration 13 — 2026-05-17

**Tier:** 1 (Stability).
**Picked:** tier-1.2 iter B sub-slice 3 — NFS-loopback-mid-shutdown
wedge harness, the last of the three wedge scenarios in iter B.

**Context discovered at start of iteration:** running binary (PID
42644) is STILL stale — no /stop endpoint. Additionally, the live
mount is in a degraded state — FUSE daemon died at some point during
the night, Redis ping fails ("no route to host"), auto-offline
engaged at 00:41:18. Auto-offline behavior is working correctly
(tier-1.7-1.10 still validated), but the mount is not currently
usable for runtime validation of this iteration's harness.

Decision: ship the harness anyway. The precondition guards make it
inert on a stale binary (exits 2 before any destructive action), so
the script can sit ready until the user restarts JuiceMount on a
fresh binary.

**Shipped (commit pending):**
- `scripts/wedge-tests/nfs-loopback-mid-shutdown.sh`: starts a
  streaming `cat` of an auto-rotated >=1GB probe file, mid-read
  POSTs /stop to the metrics listener, then in sequence measures:
  - Time until cat exits (server should drain reads — kernel NFS
    soft-mount sees connection close, propagates error to read()).
  - Time until /health stops responding (metrics server is the
    first thing torn down in stopServerLocked).
  - Optional /force-eject phase with passwordless-sudo umount
    fallback (/force-eject's listener is dead post-/stop — falls
    back to direct umount on the same sudoers allowlist).
  - Residue check: mount table should not show $MOUNT after eject.

  Precondition probe: GET /stop expects 405. On a stale binary the
  catch-all index handler returns 200 with text/plain — script bails
  with exit 2 and a clear error pointing at iter 12 + the rebuild
  instruction. Verified end-to-end against the stale running binary:
  precondition fires, no destructive action, exit 2.

  Single-shot semantics: the harness intentionally leaves JuiceMount
  in the stopped state. Restart via the Swift app's Start button.
  Documented loudly in the help text and in the final post-test log.

**Code-reviewer pass:** 1 HIGH + 2 MEDIUM addressed:
  - HIGH: missing inconclusive guard. Sibling MinIO harness has a
    `cat_exit == 0 && !cat_wedged → WARN` branch to catch the case
    where a fully-cached probe streams to EOF before /stop drains —
    the NFS harness was missing it. Added.
  - MED-2: mount-path normalization. Stripped trailing slash from
    --mount input so the `mount | grep -q " $MOUNT "` pattern
    matches correctly (without this, --mount /Volumes/zpool-dev/
    would silently false-negative on both precondition and residue
    checks). Sibling harnesses have the same bug but lower stakes;
    fixing here since residue false-negative would mask a real
    tier-1.2 failure.
  - MED-3: single curl call for status+ctype probe (no TOCTOU window).
  - 3 LOW deferred (defensive 503 branch is fine, bc sub-process
    fork is sibling-style, trap scope is correctly narrow).

**Tier-1.2 status:** all 3 wedge harnesses now shipped. Two
validated end-to-end (MinIO-wedge and FUSE-hang); the NFS-shutdown
harness is ready to validate the next time the user restarts on a
fresh binary.

**Mount state warning (separate from iter scope):** the live mount
needs user intervention — Quit JuiceMount, re-open from
build/JuiceMount.app, click Start. The fresh binary then has the
/stop endpoint AND the iter-11 goroutine watchdog support.

**Broken:** nothing introduced.

**Next:** iteration 14 picks one of:
  - Wait for user binary swap + validate the NFS-shutdown harness
    end-to-end (closes the last tier-1.2 gap).
  - Add verify-build.sh manifest entry for handleStopHTTP so future
    builds catch staleness on this fix too.
  - Tier-1.6: kick off the 24h soak with watchdog enabled (once the
    user has the fresh binary running).
  - /shutdown endpoint (full teardown — stop + unmount FUSE + NFS).

The verify-build manifest update is the cheapest unblocker for
future iterations and doesn't depend on the mount state. Pick it.

---

### Iteration 12 — 2026-05-17

**Tier:** 1 (Stability) — product-feature slice that unblocks
tier-1.2's third wedge harness.
**Picked:** ship POST /stop admin endpoint.

**Why this slice:** iter-11's STATE.md note called it out as the next
unblocker for `scripts/wedge-tests/nfs-loopback-mid-shutdown.sh`. The
wedge harness needs to trigger JuiceMount Stop programmatically while
a large read is in flight; today the only entry points are the Swift
menu-bar Stop button (manual click) and the cgo NFSServerStop export
(not callable from a shell script). `/stop` closes that gap and is
independently useful for headless server deployments and automated
upgrade flows (stop, swap binary, start).

**Shipped (commit pending):**
- `bridge/cbridge.go`: `handleStopHTTP` registered at POST /stop on
  the existing localhost-only metrics listener (127.0.0.1:11050,
  alongside /pin, /offline, /force-eject, etc). Returns
  {"ok":true,"stopping":true} immediately, flushes, then spawns a
  goroutine that sleeps 100ms (grace for the kernel to flush the
  response onto the socket) and calls stopServerLocked() — the same
  soft-stop sequence the cgo NFSServerStop entry point uses.
- `stopInProgress` atomic.Bool gates the teardown spawn so concurrent
  /stop POSTs within the 100ms flush window don't each spawn their
  own teardown goroutine (stopServerLocked is already serialized by
  globalMu so the second goroutine would no-op, but it's wasteful
  and pollutes the goroutine count the iter-11 watchdog observes).
- NFSServerStart resets stopInProgress so subsequent Start+Stop+Start
  cycles work as expected (a process-wide sync.Once would be
  permanently spent after the first /stop).

**Validated:** go vet clean, go build clean, scripts/build-app.sh
produces a fresh app bundle, `nm -a` confirms `_main.handleStopHTTP`
and `_main.stopInProgress` symbols are linked, `strings` confirms
the "/stop" route literal and the "handleStopHTTP: soft-stop
requested via HTTP" log line are in the binary. Runtime validation
(POST /stop against a running mount, verify drain semantics)
requires a binary swap — the live mount is still on iter-10's
binary (PID 42644 from May 16 17:29).

**Code-reviewer pass:** 1 HIGH addressed, 1 MEDIUM documented,
2 LOW addressed:
  - HIGH-1: concurrent-POST double-teardown race. Initial fix used
    sync.Once but that would permanently break /stop after one use.
    Switched to atomic.Bool gating + reset on NFSServerStart so the
    Start+Stop+Start cycle works correctly.
  - MEDIUM-1: 100ms flush grace is a heuristic; under sustained
    NFS I/O load the loopback TCP buffer might not drain. Known
    limitation documented inline; if wedge harness sees flaky
    empty-body results, switch to ResponseWriter.Hijack().
  - LOW-1: added jmlog.Info("handleStopHTTP: soft-stop requested",
    "remote", r.RemoteAddr) so /stop calls are correlatable with
    "server stopped responding" reports.
  - LOW-2: subsumed by the HIGH fix (atomic.Bool + reset means no
    orphaned long-lived goroutines).

**Tier-1 status:** no acceptance-test row changes; this is
infrastructure unlocking iter B sub-slice 3 (NFS-shutdown wedge
harness, future iteration).

**Broken:** nothing. /force-eject still available as the existing
unmount path; /stop is purely additive.

**Next:** iteration 13 picks one of:
  - Ship `scripts/wedge-tests/nfs-loopback-mid-shutdown.sh` against
    POST /stop (closes tier-1.2 once user swaps binaries).
  - Add a `verify-build.sh` manifest entry for handleStopHTTP so
    future builds catch staleness on this fix too.
  - Tier-1.6: kick off the 24h soak with the watchdog enabled.

The wedge harness is the most-leveraged: it both validates /stop's
shutdown semantics AND closes a tier-1.2 acceptance gap.

---

### Iteration 11 — 2026-05-17

**Tier:** 1 (Stability).
**Picked:** tier-1 iter D — goroutine-leak watchdog in `cmd/jmstress`.

**Why this slice (not the next wedge harness):** the iter-10 STATE.md
note pointed at `nfs-loopback-mid-shutdown.sh` as iter B sub-slice 3.
Investigation showed that scenario needs JuiceMount Stop triggered
programmatically — but there is no `/stop` admin endpoint today, only
`/force-eject` (unmounts but doesn't drain the NFS server). Building
that endpoint is a real product feature deserving its own slice
(per "no bundled-PR scope creep"). Pivoting to iter D — a clean
self-contained code addition that hardens tier-1.6's 24h soak gate.

**Shipped (commit pending):**
- `cmd/jmstress`: goroutine watchdog component that polls
  `/debug/pprof/goroutine?debug=1` on a configurable ticker, captures
  the post-warmup baseline, flags subsequent ticks where count >
  baseline × multiplier as breaches. Surfaces a `goroutines` block
  in periodic JSON snapshots and the final summary. Any breach exits
  jmstress non-zero, so a CI gate parsing the final JSON line OR
  the exit code catches the leak class where latency and errors look
  healthy but goroutines ramp unbounded.

  Flags: `--goroutine-check` (default true), `--goroutine-multiplier`
  (default 3.0 — empirical NLE-worker variance is ~3x; 1.5x would
  false-positive on healthy workloads), `--goroutine-tick` (default
  30s), `--goroutine-warmup` (default 5min — delays first tick so
  baseline reflects steady-state worker activity, not the spike
  during NLE/backup worker ramp-up).

**Validated:** 3 smoke runs and 1 forced-breach run:
  - normal 50s run with --goroutine-warmup 15s: baseline=241,
    current 136-275, max_ratio=1.14, 0 breaches, exit 0
  - forced --goroutine-multiplier 0.5: 1 breach at tick 2, exit 1,
    JSON summary shows breaches=1
  - bad metrics URL: "goroutine watchdog disabled" logged,
    run completes normally, exit 0
  - go vet ./cmd/jmstress/ clean; go build OK

**Code-reviewer pass:** 2 MEDIUM addressed:
  - MED-1: tick() now rejects count <= 0 from fetch() before storing
    baseline (defends against malformed pprof response producing
    "total 0" which would divide-by-zero subsequent ratio calcs).
  - MED-2: probe() now initializes max to the probe count, so any
    JSON snapshot emitted in the warmup window shows max >= current
    (avoids confusing readers expecting that invariant).
  - 2 LOW deferred (fetch-error logging in tick() — would generate
    per-tick noise on a flapping network; could surface as an error
    counter in a follow-up).

**Tier-1.6 status:** advances from "scaffold landed" to "scaffold +
goroutine watchdog landed; 24h soak run pending." The remaining
gate is the actual 24h soak run, which needs a quiet window.

**NFS-shutdown wedge harness:** deferred pending a `/stop` admin
endpoint (which would be a tier-1 follow-up product slice). The
existing two wedge harnesses (`minio-down-mid-read.sh`,
`fuse-hang-mid-op.sh`) already validate the most common backend-
failure modes for tier-1.2.

**Broken:** nothing.

**Next:** iteration 12 picks one of:
  - Ship `/stop` admin endpoint (unblocks the third wedge harness;
    also useful for headless deployments and automated upgrades).
  - Kick off the actual 24h soak run with the watchdog enabled
    (closes tier-1.6 if it passes).
  - Tier-1.4 crash-recover-test --real run (needs user buy-in for
    a destructive operation against the live mount).

The `/stop` endpoint is the most-leveraged code work — pick that.

---

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
