# Tuning Revert Log

Per the cellular-revert-safety discipline: every link-class-gated or
WAN/cellular-affecting tuning change is logged here with its kill switch and
the exact baseline it reverts to, so a regression can be backed out **without
a rebuild**.

---

## 2026-07-02 — Boot fast-path C1 (skip the boot SCAN when mirror fresh + push engaged)

Env-revertable without a rebuild. Motivation: a deployed build over a cellular
relay ran a 174s background "Rebuilding index…" boot SCAN that is redundant
when keyspace push is engaged (the PSUBSCRIBE gap-fill + periodic backstop
already guarantee convergence).

**C1 — skip the boot SCAN when the mirror is fresh + push engaged.**
`RedisClient.ShouldSkipBootSync()` returns true only when BOTH
`JM_METADATA_KEYSPACE_PUSH=1` AND a durably-persisted `last_sync_time` (new
`store_meta` key/value table in `metadata.db`, written on every successful
`syncMetadata`) is within a freshness window. When true, `cbridge`'s boot-sync
block skips the one-shot `SyncOnce` entirely — the PSUBSCRIBE gap-fill + the
periodic backstop SCAN carry any deltas since. `rc.Start()` (reconcile loop +
keyspace loop) launches unchanged. `IsSyncing()` stays false for a skipped boot
(no `lastSyncStartedAt` stamp), so G7's `/activity` never shows "Rebuilding
index…".

- **`JM_BOOT_SYNC_SKIP=0`** — kill switch. Forces the current always-sync
  behavior (`ShouldSkipBootSync` returns false unconditionally). Baseline =
  every boot runs the SCAN as before.
- **`JM_BOOT_SYNC_MAX_AGE_SEC=<n>`** — freshness window in seconds (default
  86400 = 24h). `0` (or negative) = **never skip**. A malformed value fails
  safe (no skip). Reverting to the always-SCAN behavior needs only
  `JM_BOOT_SYNC_SKIP=0` (or push off).

**Fail-safe cases (all fall through to today's blocking/background boot SCAN):**
push disabled/unset; no persisted `last_sync_time` (first run OR the app's
"Reset local metadata cache" — `store_meta` lives in the same DB the reset
wipes, so a wiped mirror is never treated as fresh); stale timestamp;
corrupt/future timestamp (clock skew); read error. The empty-mirror blocking
`SyncOnce` (U1) and `JM_BOOT_SYNC_FIRST=1` are unreachable behind a skip because
`ShouldSkipBootSync` returns false for an empty/first-run mirror.

**Validated:** unit — `TestStoreMetaRoundTrip`, `TestShouldSkipBootSync` (full
truth table incl. env overrides + kill switch + corrupt/future timestamps).
`gofmt` clean; `go build ./...` green; `go test ./metadata/ ./bridge/ -count=1`
green. Live boot-timing (a fresh restart over a cellular relay skips the SCAN
and shows no "Rebuilding index…") is the real proof — pending.

---

## 2026-06-28 — Slow-link false-flap fix (drain-liveness probe override + degrade-gated prunes)

*(Salvaged onto feat/v2.3 on 2026-07-01 — the original landed on the rolled-back
NFSv3-perfection sprint branch as task #66 and was lost in the rollback.)*

**What:** A Finder copy over saturated WiFi (network proven fine: 30/30 Redis
dials, 0% ping loss, 3 ms; the drain KEEPS SUCCEEDING during the flap) was
erroring because of two compounding defects:

1. **Probe false-flap (`health/reachability.go`).** The reachability monitor
   probes only the Redis host:port with a cold TCP dial. On healthy WiFi RTT
   ~3 ms keeps the adaptive `effectiveDialTimeout` clamped to the 1 s floor (it
   folds RTT only from *successful* dials). When the drainer saturates the
   uplink, a cold SYN queues behind the bulk PUT traffic and exceeds 1 s → 2
   failures → a FALSE "unreachable" that arms the 18 s offline-engage deferral.
   The adaptive timeout cannot grow from the very congestion causing the failure.
2. **Prune-during-degrade (`metadata/keyspace.go` + `metadata/redis.go`).** The
   keyspace-push `scopedPrune` had NO degrade gate (only the periodic full-SCAN
   prune did), and `RecentlyDegraded()` was FALSE during the flap anyway —
   `rc.connected` is written only by doReconcile / Reconnect / the keyspace sub
   (which keep succeeding), never by the reachability monitor. A single push
   `hdel` observation then removed real files mid-copy (257 scoped-prune removals
   → 210 STALE → copy error).

**Fix (a) — drain-liveness probe override (load-bearing).** A completed drain
(a MinIO PUT that landed) is positive proof the backend is reachable over the
SAME congested link. The drainer stamps `LastDrainSuccess` (an `atomic.Int64`
UnixNano) ONLY at the `DrainsSucceeded.Add(1)` success site —
never on an attempted/in-flight drain. `health.WithLivenessHook` injects
"time-since-last-proven-backend-IO" into the monitor; on a probe FAILURE, if a
drain succeeded within the liveness window (`~2*baseInterval`, ~4 s), the failure
is suppressed — the fail streak is neither advanced nor reset, no transition
fires. A genuine outage stops drains within seconds → the window lapses → the
normal 2-failure flip proceeds. Wired in `bridge/cbridge.go` capturing
`globalDrainer` (read under `globalMu`); nil drainer → MaxInt64 sentinel → the
override never fires → behavior identical to today.

**Fix (b) — degrade-gate both prune paths.** `RecentlyDegraded()` is now
reachability-aware: it also returns true when the injected reachability signal
(`reachableNow()`, wired via `metadata.SetClassSignals` under
`JM_METADATA_KEYSPACE_PUSH=1`) reports unreachable — checked before taking
`rc.mu.RLock` so the separate `keyspaceSignalMu` never nests under `rc.mu`.
`scopedPrune` gains a top gate (`if rc.RecentlyDegraded(60s) { return }`, after
the `ListChildren` early-out), symmetric to the SCAN path's existing gate,
covering the subtree-delete path too. This DEFERS the authoritative push-delete
one cycle (replayed on the next keyspace event / healed by the backstop SCAN) —
**never drops it**; a removed file lingers at most one degraded window.
**Upserts stay ungated**, so navigation/convergence keeps working while pruning
is paused.

**Kill switch (env, no rebuild):**

| `JM_REACH_DRAIN_LIVENESS` | Effect |
|---|---|
| unset / non-`0` (default) | **ENABLED** — drain-liveness override active. |
| `0` | **DISABLED** — the liveness hook is never consulted; a probe failure flips after the normal 2-failure threshold regardless of recent drains. **Byte-identical to the pre-fix monitor.** |

Fix (b) has no separate env switch: it is correctness, not a tuning band, and it
only ever DEFERS a delete (never drops one). It is implicitly neutralized
wherever reachability is — a nil `reachableFn` makes `reachableNow()` return
true, so `RecentlyDegraded()` reverts to its pure `rc.connected`/cooldown logic
and `scopedPrune` gates exactly as the pre-fix SCAN path already did. (On this
base the reachability signal is only wired when `JM_METADATA_KEYSPACE_PUSH=1` —
the same env gate the original fix had.)

**Baseline to revert to:** `JM_REACH_DRAIN_LIVENESS=0` restores the exact
pre-fix probe behavior (no liveness consultation). Fix (b)'s gate matches the
periodic SCAN prune's long-standing `RecentlyDegraded` gate, so on a healthy LAN
(reachable + connected) `RecentlyDegraded()` is false and pruning runs exactly as
before — **10GbE behavior is byte-identical**.

**Validated:** Go tests only (false-positive-prone per testing discipline) —
`TestReachability_DrainLivenessSuppressesFalseFlap` (recent drain → no flip;
stale drain → flips; absent hook → flips) +
`TestReachability_DrainLivenessKillSwitchByteIdentical` (`=0` → hook never called,
normal flip); `TestDrainerLastDrainSuccessStamp` (zero before any drain, stamped
inside the drain window after success); `TestScopedPruneDeferredWhileDegraded`
(unreachable → defer, candidate spared; reachable → pruned) and
`TestRecentlyDegradedReachabilityAware` (true when reachable==false despite
connected==true; false when nil-signal). **Pending:** REAL-app live gate — re-run
the saturating Finder copy and expect ~0 flap-induced prunes / STALE; the
orchestrator owns that verdict.

---

## 2026-06-27 — Metadata Redis keyspace-notification push

**What:** Demote the full-tree Lua metadata SCAN from a fixed 30s cadence to a
rare, class-gated backstop, driven by Redis keyspace notifications
(`PSUBSCRIBE __keyspace@<db>__:d*`) so only **changed** directories are
incrementally reconciled. Motivation: the 30s full SCAN takes 87–178s over
cellular and finds zero changes ~93% of the time, saturating the link and
killing navigation.

**Kill switch (env, no rebuild):**

| `JM_METADATA_KEYSPACE_PUSH` | Effect |
|---|---|
| unset / `0` (default) | **DISABLED** — the keyspace loop never starts; `backstopNanos` stays at `DefaultReconcileInterval` (30s); `reconcileLoop` runs the exact production Lua SCAN. **Byte-identical to pre-change behavior.** |
| `1` | Engage the push subsystem (still self-disables if Redis lacks `notify-keyspace-events` — see auto-detect below). |

**Auto-detect (second safety net):** even with `=1`, the client runs
`CONFIG GET notify-keyspace-events` on every (re)connect. If the result is
insufficient (`sufficient := contains('K') && (contains('A') || (contains('g')
&& contains('h')))`) it stays DISABLED and runs the 30s SCAN. The live NAS
returns **empty** today, so the DISABLED path is what actually runs until the
durable NAS compose change lands (see `server/NOTIFY-KEYSPACE-EVENTS.md`).

**Baseline to revert to:** `DefaultReconcileInterval = 30 * time.Second` full
Lua SCAN (the proven production path). Set `JM_METADATA_KEYSPACE_PUSH=0` (or
unset) — no rebuild, no redeploy.

**Class-gated values (only active when ENABLED + reachable):**

| Link class | Active interface band | Rare-backstop SCAN interval | Coalescer debounce / max-wait |
|---|---|---|---|
| LAN / Ethernet | `en*` (≠ `en0`), `eth*` | 10 min | 200 ms / 2 s |
| WiFi | `en0` | 15 min | 400 ms / 3 s |
| Tunnel / cellular / WAN | `utun*`, `tailscale0`, or `JM_WAN_MODE=1` | 45 min | 1.5 s / 5 s |
| **DISABLED / DEGRADED / unreachable** | (any) | **30 s** (baseline) | n/a |

Coalescer burst ceiling: **200 distinct dirs** in one window → promote to a
single full SCAN (`TriggerSync`) instead of thousands of `HGETALL`s.

**Interaction with backoff:** the reconcile loop's failure backoff is computed
off the 30s base (a failure streak drives the loop *faster* to detect
recovery, never slower); on recovery it resets to the live `backstopNanos`, so
backoff and the long backstop don't fight.

**Convergence guarantees (push is fire-and-forget, never sole source of truth):**
(A) startup blocking `SyncOnce()` baseline; (B) every (re)connect runs
PSUBSCRIBE-then-full-SCAN gap-fill; (C) the rare class-gated periodic SCAN
ceiling above. Pure in-place `i{inode}` size/mtime edits fire no `d`-event and
are caught only by (C) — an accepted, documented tradeoff.

**Validated:** PARITY Go test (incremental store == fresh full-SCAN store,
byte-identical path/inode/size/mtime/isDir) over a seeded `d`/`i` fixture, plus
foreign create/delete/rename/move/rmdir mutations; QA-30 pin-safety on the
scoped prune path; coalescer collapse + burst-promotion; wrong-db channel
rejection. **Pending:** REAL Finder/VLC validation on a live mount with the NAS
reconfigured to `Kghx` (unit tests give false positives on this codebase per
testing-feedback discipline).

## 2026-06-27 — cellular backstop capped 45m → 5m + LSEnvironment flag

- `backstopForClass(classTunnel)` 45m → **5m** (metadata/keyspace.go). QA flagged that a 45m
  backstop stretched the two backstop-bounded staleness windows (foreign in-place attr edits;
  a delete missed during a reconnect gap, healed via PruneThreshold×backstop) to hours on
  cellular. 5m bounds attr drift to ≤5m and a missed-delete ghost to PruneThreshold×5m (~50m)
  while still a 6×+ reduction from the old constant 30s cadence. REVERT: restore 45m if the
  5m SCAN re-introduces link contention on cellular (push remains the freshness mechanism;
  the SCAN is only a backstop, so a longer interval trades staleness for link headroom).
- `JM_METADATA_KEYSPACE_PUSH=1` set via `LSEnvironment` in app/JuiceMount/Resources/Info.plist
  (timing-safe: in environ at process launch, before the Go runtime caches it). REVERT/kill:
  remove the LSEnvironment key (or set notify-keyspace-events empty on the NAS → auto-fallback
  to the classic 30s SCAN). The core auto-detects via CONFIG GET, so the flag is dormant unless
  the NAS Redis has notify-keyspace-events sufficient (K && (A || (g && h))).

---

## 2026-06-30 — Demoted-SCAN backstop lengthened to ~15-30m (keyspace-push reliance)

**What:** With keyspace-push proven engaged on the live NAS (subscribed + per-dir
reconcile observed live), lengthen the DEMOTED periodic full-SCAN backstop from
10/15/5m (LAN/WiFi/tunnel) to 15/20/15m. Field testing (remote/WAN) showed the
~5m WAN SCAN "rebuilding the index every ~5 min" was too eager and churned the
438MB mirror (177MB WAL) into user-visible periodic sluggishness. The push
(real-time per-dir d-key reconcile) is the live path; the SCAN is only a
missed-event safety net.

**Safety (unchanged):** when push DROPS, `keyspaceLoop` calls
`setEngagement(keyspaceDegraded)` which resets the cadence to
`DefaultReconcileInterval` (30s) until push re-engages, so the long backstop only
ever applies WHILE PUSH IS HEALTHY + REACHABLE. tunnel stays <= lan/wifi.

**Kill switch (env, no rebuild):**

| `JM_RECONCILE_BACKSTOP_SEC` | Effect |
|---|---|
| unset / `0` (default) | class-gated 15/20/15m (LAN/WiFi/tunnel) |
| positive integer N | force N-second backstop for ALL classes (`300` restores the prior 5m tunnel cap; `900` = 15m everywhere; `1800` = 30m) |

**Full revert:** `JM_METADATA_KEYSPACE_PUSH=0` (disables push → 30s SCAN,
byte-identical to pre-keyspace-push) OR `JM_RECONCILE_BACKSTOP_SEC=300`. Baseline:
keyspace.go backstopForClass 10/15/5m.

---

## 2026-07-02 — G6: Layer-A prune verification budgeted + class-gated (task #79)

**What:** `syncMetadata`'s prune-ladder "Layer A: per-path FUSE Lstat
verification" (metadata/redis.go, extracted to `verifyPruneCandidates`) was
UNBOUNDED — every ladder candidate got a 1s-capped Lstat with no total budget.
Proven live 2026-07-02: a SCAN-coverage bug left ~112k permanent candidates,
so a threshold-crossing cycle ran **86s on LAN** and **~90 min over a cellular
relay** (~50ms/Lstat), saturating the link and pinning "Rebuilding index…"
forever. Two changes, both fail-safe (deferral, never an unverified prune):

1. **Budget** — same bounding pattern as `collectFastPathPrunes`:
   `layerAProbeCap = 2048` probes / `layerAProbeBudget = 3s` wall per cycle.
   Once either trips, remaining unprobed ladder candidates are deferred and
   re-inserted into `pruneAbsent` at their PRIOR counter (captured before the
   ladder collection erases it), so they keep their ladder position and
   re-qualify on the very next cycle — no 10-cycle restart, no unverified
   prune. One log line per bounded cycle: probed/verified/deferred + elapsed.
2. **Class gate** — on the tunnel/cellular band (`currentLinkClass() ==
   classTunnel`: `utun*`/`tailscale0`/`JM_WAN_MODE=1` — same accessor as the
   backstop + coalescer gating) ladder candidates are never probed at all
   (each Lstat is a metered round-trip); ALL are deferred at prior count.
   G5 `fastConfirmed` entries are exempt from both gates — root-verified this
   same cycle, they still prune (a deleted tree costs a handful of root
   Lstats, which is exactly the cheap path the gate preserves).

**Kill switch (env, no rebuild, read per cycle like `JM_PRUNE_FASTPATH`):**

| `JM_LAYERA_BUDGET` | Effect |
|---|---|
| unset / anything else (default) | cap 2048 / 3s budget + tunnel-class deferral |
| `0` | restores the pre-G6 behavior EXACTLY: unbounded per-candidate probing, no class gate |

**Revert:** `JM_LAYERA_BUDGET=0`, or restore the inline Layer-A loop from the
parent of this commit. Baseline behavior on 10GbE/LAN is unchanged for any
cycle with fewer than 2048 candidates finishing under 3s (the normal case).

**Validated:** unit (`TestLayerABudgetDefersUnprobedCandidates`,
`TestLayerAKillSwitchRestoresUnbounded`,
`TestLayerAMeteredClassDefersLadderButNotFastPath`, + full metadata suite
incl. -race on the prune tests). **Pending:** live cellular-relay validation
that a threshold-crossing cycle stays <5s and "Rebuilding index…" clears
(unit tests give false positives on this codebase per testing discipline).

## 2026-07-02 — U1 serve-first boot (V2.3)

| `JM_BOOT_SYNC_FIRST` | Effect |
|---|---|
| unset (default) | NFS + control plane start immediately; initial full-tree SyncOnce runs in the background (syncMu single-flighted). Blocking retained automatically when the mirror is EMPTY (fresh install). |
| `1` | Restores the pre-U1 blocking order (SyncOnce completes before the NFS server starts). |

**Why:** field "5+ min to first items" — measured 136s of a 141s cellular cold start was the blocking SyncOnce; mirror was already RAM-hydrated. jm5's initial-sync Fatalf also demoted to Warn (K4).
**Revert:** `JM_BOOT_SYNC_FIRST=1`, or revert the commit.

## 2026-07-02 — U5 mountVerifyTimeout + U2 boot-defer RTT (V2.3)

| Switch | Effect |
|---|---|
| `JM_FUSE_WATCHDOG_LINKAWARE` off | U5 inert: launch mount verify stays fixed 15s (as before). On: 45s slow / 90s metered, 15s LAN unchanged. |
| `JM_BOOT_DEFER_RTT_MS` (default 500) | Boot dial RTT above this → start-while-offline path instead of synchronous online boot. `0` disables U2 entirely. |

**Revert:** flip the switches; both paths byte-identical to pre-V2.3 when disabled.

## 2026-07-02 — #78 push-path namespace filter + one-time internal-namespace mirror GC (V2.3)

**What:** the SQLite mirror held ~112k rows under backend-internal namespaces
(`.trash/…` JuiceFS built-in trash, `.juicemount/…` server-side derivatives)
plus un-drained `._` sidecars. The full SCAN can never return the internal
namespaces (the trash tree is structurally unreachable from root inode 1 in
syncMetadata's path reconstruction; `.juicemount` live-proven SCAN-absent), but
the keyspace-push insert paths (`applyEvent`, `reconcileDir`) had no matching
filter — so pushed rows were mirrored, then cycled forever in the `pruneAbsent`
ladder (permanent pending_prune floor, inflated diffs, pre-G6 Layer-A storms).

**Fix:** `metadata/scanfilter.go: scanFilteredPath` is the single source of
truth mirroring the SCAN's effective exclusions (`.trash/`, `.juicemount/` —
root-level only; `._` deliberately NOT included: it is SCAN-visible once
drained and served Mac-side). Applied in `applyEvent` (create/update + rename
destination; deletes and rename-source removal stay ungated), `reconcileDir`
(dir-level skip before any Redis round-trip + root-child skip; `freshNames`
stays faithful to Redis), `scopedPrune` candidates, and the `pruneAbsent`
tracking loop (`trackAbsentPaths` — filtered paths never tracked, stale
counters dropped). One-time GC at Store open (before cache/FTS rebuild)
deletes `path LIKE '.trash/%' OR '.juicemount/%'` in 5k-row batches — bare
namespace dir rows and ALL `._` rows are spared.

| `JM_MIRROR_NS_GC` | Effect |
|---|---|
| unset (default) | one-time GC runs at every Store open (idempotent; ~112k rows first run, then 0) |
| `0` | GC disabled — pre-fix mirror rows are left in place (they are inert: push-filtered + never ladder-tracked) |

**Revert:** `JM_MIRROR_NS_GC=0` stops the GC without a rebuild (the push
filter itself has no switch — it only skips writes the SCAN would never
confirm; full revert = revert the commit). Deleted rows are NOT restored by
the revert, but Finder/NFS never depended on them (the NFS handler serves
`._` via its own write path, and `.trash`/`.juicemount` are server-internal;
listings simply stop showing the internal trees).

**Validated:** unit (`TestScanFilteredPathTruthTable`,
`TestApplyEventSkipsScanFilteredNamespaces`,
`TestReconcileDirSkipsFilteredNamespaceDirs`,
`TestTrackAbsentPathsNeverTracksFilteredNamespaces`, `TestMirrorNamespaceGC`,
`TestMirrorNamespaceGCKillSwitch` + full metadata suite). **Pending:** live
validation that pending_prune drops from the ~112k floor to ~0 after one
restart + first SCAN cycle.

## 2026-07-02 — U7 async unmirrored-dir refresh: never block READDIR on FUSE (V2.3)

**What:** a directory with ZERO rows in the SQLite mirror used to fall through
to a FOREGROUND bounded FUSE readdir while online (`nfs/handler.go` ReadDir
fallback). Over a slow link — or any FUSE slowness; root readdir was measured
hanging >25s over a cellular relay — Finder sat on "loading" for up to a
minute on such directories. Now the online zero-row readdir returns the
mirror's answer (empty) IMMEDIATELY and dispatches an ASYNC bounded FUSE
readdir (`refreshUnmirroredDir`) that INSERT-only upserts the discovered
children into the mirror (SQLite + cache), so the next readdir (Finder
retries/refreshes on its own) serves them from the mirror hot path.

**Bounds:** singleflight per directory (`dirRefreshInFlight`, the
phantomPurgeInFlight pattern) so a Finder storm on one dir spawns ONE
goroutine; `dirRefreshSem` (4) caps concurrently-refreshing dirs, excess is
shed and re-fired by the next readdir; the FUSE readdir itself uses
`readDirWithTimeout` with `fuseStatTimeout` (the WAN-aware budget the old
foreground fallback used) and draws from `prefetchGate` (the background
readdir budget), never `nfsLstatGate` (QA-35: background work must not
consume the foreground hot-path FUSE budget). Worker re-checks
`pin.IsOffline()` and the G0 `pin.FUSEIdentityOK()` gate before touching
FUSE; a plain-dir mountpoint is never scanned or mirrored. Offline readdir
semantics untouched (empty-fast path returns before the dispatch). Entries
are built by `coldDirListing`, the SAME extracted code path the foreground
fallback uses.

| `JM_ASYNC_DIR_REFRESH` | Effect |
|---|---|
| unset / `1` (default) | Online zero-row readdir returns empty immediately + async mirror refresh; next readdir serves the children. |
| `0` | Restores the prior FOREGROUND bounded-FUSE fallback byte-identically (old code path kept intact behind the switch). |

**Known trade (by design):** the first listing of an unmirrored dir shows
empty until Finder re-lists (macOS may cache the empty answer up to
acdirmax); the refresh typically lands within one `fuseStatTimeout`.

**Revert:** `JM_ASYNC_DIR_REFRESH=0` (read per call — a launchctl setenv +
app restart suffices), or revert the commit.

**Validated:** unit (`TestAsyncDirRefreshPopulatesStore`,
`TestAsyncDirRefreshSingleFlight`, `TestAsyncDirRefreshShedWhenSaturated`,
`TestAsyncDirRefreshKillSwitch`, `TestAsyncDirRefreshOfflineUntouched`,
`TestAsyncDirRefreshIdentityGateRefusal`,
`TestAsyncDirRefreshNeverOverwrites` + full nfs suite incl. -race on the new
tests; only the known environmental TestMemBuf* failures remain). **Pending:**
live slow-link validation that an unmirrored dir populates on Finder's own
refresh cadence.

## 2026-07-02 — Batch-3 adversarial-review fixes: U3 mounts FUSE when persisted-offline (HIGH), U7 bounded stats + insert-if-absent, #78 filter unification (V2.3)

**What changed (behavior notes for the three entries above):**

- **U3 (HIGH):** a persisted-offline boot now PROBES the backend and — when
  reachable — MOUNTS FUSE, exactly like every other boot. Offline pinned reads
  REQUIRE the mount (OpenFile reads pinned bytes through the FUSE fd from the
  JuiceFS local block cache); the old skip-probe/skip-mount start made every
  pinned-and-ready file unreadable for the whole relaunched offline session,
  with the U4 watchdog stand-down (correctly) refusing to recover it. The
  split that holds now: FUSE mount + Redis connect budget key on `backendUp`
  (true probed reachability); boot-sync suppression, offline gates, and the
  started_offline banner key on `startedOffline`. The U2 RTT-defer is skipped
  on a U3 boot (it works by fabricating `backendUp=false`, which would
  re-open the same hole on a slow-but-alive link). The "touches no network"
  U3 claim was wrong and is retired: juicefs needs Redis to mount, so the
  boot Redis connect stays (the review suggestion to skip it was
  rejected/subsumed for the same reason). Added traffic on a metered link:
  the 1.5s TCP probe + mount metadata handshake — no data reads.
  Also: marker path now via `os.UserHomeDir()` (warn-and-disable on failure);
  per-cause `started_offline:` reasons (R-4 unreachable / U2 slow / U3 "your
  setting from last session"); a marker that outlives a missing/empty
  metadata mirror DB (app-data reset) is treated as stale — cleared, normal
  online boot with the U1 empty-mirror blocking sync
  (`pin.DropStaleOfflineIntent`).
- **U7:** the per-child `de.Info()` lstats in `coldDirListing` are now
  bounded (`infoWithTimeout`, gate-parameterized: foreground fallback →
  `nfsLstatGate`, async worker → `prefetchGate`) and a timeout ABANDONS the
  listing pass, so a wedged FUSE can no longer park refresh workers forever
  and silently exhaust `dirRefreshSem`. The async warmers
  (`refreshUnmirroredDir`, `prefetchChildren`) insert via the new
  `Store.BulkInsertAbsent` (INSERT OR IGNORE + skip-if-present cache mutation
  inside the store's critical sections), closing the TOCTOU where a fresher
  push-event row landing mid-warm was clobbered by the stale FUSE snapshot
  (stale-GETATTR-size / phantom-re-insert class). Foreground writers keep
  replace semantics.
- **#78:** `metadata.ScanFilteredPath` is now exported and applied on ALL
  FUSE-sourced mirror-insert paths — ReadDir short-circuits scan-filtered
  namespaces (empty answer, no FUSE readdir, no insert), `coldDirListing`
  lists-but-never-mirrors filtered children, `prefetchChildren` skips them
  and never fans out into them — so `.trash`/`.juicemount` can no longer be
  re-mirrored mid-session, closing the GC-delete/re-add cycle. The open-GC
  predicate is now case-sensitive GLOB (`.trash/*`, `.juicemount/*`): SQLite
  LIKE was ASCII-case-insensitive and deleted case-variant USER trees
  (`.Trash/…` from a home-dir backup) at every open. The prune-tracking
  exclusion narrowed to a new `scanFilteredDescendant` (descendants only):
  the bare `.trash`/`.juicemount` dir rows re-enter the FUSE-verified prune
  paths, so they are spared while real (2 bounded Layer-A Lstats worst case
  per cycle) but prunable if the backend namespace is ever genuinely removed
  (no more permanent ghost dir).

**Switches:** unchanged — `JM_MIRROR_NS_GC=0`, `JM_ASYNC_DIR_REFRESH=0`,
`JM_FUSE_OFFLINE_REMOUNT=1`, `JM_BOOT_DEFER_RTT_MS` all still apply; no new
env vars. Full revert = revert the commit.

**Validated:** unit — `TestInfoWithTimeoutWedgedStat`/`Completes`,
`TestColdDirListingBreaksOnWedgedStat`, `TestReadDirScanFilteredShortCircuit`,
`TestColdListingListsButNeverMirrorsFiltered`,
`TestPrefetchChildrenSkipsFilteredNamespace`,
`TestBulkInsertAbsentNeverOverwritesFresherRow`/`RespectsInodeOwner`/
`InsertsNewRows`, `TestScanFilteredDescendantTruthTable`,
`TestMirrorNamespaceGC` (case-variant rows), `TestDropStaleOfflineIntent` +
full metadata/nfs/pin/bridge suites (only the known environmental TestMemBuf*
failures with the live app). **Pending:** live persisted-offline relaunch
proving pinned files open (the HIGH's headline flow).

## 2026-07-02 — G7: class-gated SCAN budget + deferred-not-failed slow-link syncs (task #80)

**What:** `syncMetadata`'s full-SCAN batch loop ran under a FIXED
`context.WithTimeout(…, 120s)`. On a degraded cellular relay the full SCAN
needs ~200-300s, so EVERY attempt died with "redis SCAN batch: context
deadline exceeded" (live 2026-07-01: **31 consecutive failures over 3h**).
Worse, `doReconcile`'s failure backoff retries FASTER than the class backstop
(deliberate for outage recovery, wrong here) — each retry saturated the
metered link for the full 120s and then died — and `IsSyncing()` keyed only on
`lastSyncStartedAt > lastSyncTime`, so /activity rendered an eternal
"Rebuilding index…". The keyspace push was ENGAGED and healthy the whole time
(deltas flowing): the failing backstop SCAN was redundant convergence work.
Three changes (metadata/redis.go, metadata/keyspace.go, bridge/cbridge.go):

1. **Class-gated SCAN budget** — `scanContextTimeout()` (keyspace.go, same
   `currentLinkClass()` accessor as the backstop/coalescer/G6 gating):
   LAN/WiFi → 120s (byte-identical), tunnel/cellular (`utun*`/`tailscale0`/
   `JM_WAN_MODE=1`) → 300s so the observed slow-link SCAN can finish.
2. **Deferred-not-failed classification** — a sync error that wraps
   `context.DeadlineExceeded` (syncMetadata now guarantees the sentinel
   whenever its OWN budget context expired, however go-redis dressed it)
   WHILE the push is engaged (`currentBackstop() > DefaultReconcileInterval`,
   the established push-carrying test) is DEFERRED: no `consecutiveFailures`
   bump / fast-retry backoff (next attempt waits for the normal backstop
   tick; a prior failure-streak's short ticker is reset to the backstop), no
   `connected=false` flip (push-healthy means the backend IS reachable —
   flipping would poison the `RecentlyDegraded` prune/phantom-purge gates),
   Warn logged ONCE per streak. If the push drops, `setEngagement` snaps the
   backstop to 30s and every deadline error is a genuine failure again —
   real outages keep the classic fail-fast backoff.
3. **/activity truthfulness** — new `lastSyncEndedAt` stamped on EVERY
   syncMetadata exit; `IsSyncing()` additionally requires
   `lastSyncStartedAt.After(lastSyncEndedAt)`, so a failed/deferred attempt
   stops reporting "syncing" immediately. `lastSyncStartedAt` is NEVER
   zeroed (the reconcileLoop flap debounce reads it and must keep
   suppressing flap-triggered SCANs right after the link was saturated).
   `SyncDeferredReason()` drives a truthful /activity line: "Index sync
   deferred — link too slow for a full rebuild; live updates continue via
   push" (Active: false). The U6 N/~M progress rendering is unchanged for
   genuinely-active syncs.

**Switches (env, no rebuild, read per call):**

| Switch | Effect |
|---|---|
| `JM_SCAN_TIMEOUT_SEC=<n>` (positive) | forces the SCAN budget to n seconds for ALL classes; `0`/unset = class logic |
| `JM_SYNC_DEFERRAL=0` | disables the deferred classification entirely — every sync error drives the pre-G7 failure backoff |

**Revert:** `JM_SYNC_DEFERRAL=0` + `JM_SCAN_TIMEOUT_SEC=120` restores the
pre-G7 timing/backoff behavior without a rebuild (the only residual is the
truthful-IsSyncing bookkeeping, which is presentation-only); full revert =
revert the commit. LAN/WiFi baseline is byte-identical: 120s budget, and the
deferral can only engage when the push holds a long backstop AND the budget
expires — never in the deployed 10GbE steady state where SCANs take 2-10s.

**Validated:** unit — `TestScanContextTimeoutClassTable`,
`TestScanContextTimeoutEnvOverride`, `TestIsDeferredSyncErrClassification`,
`TestDoReconcileDeferredSkipsBackoff` (real doReconcile→syncMetadata against
a hung local listener: counter stays 0, connected stays true, IsSyncing
false, reason set), `TestDoReconcileFailurePathUnchanged` (push not engaged →
byte-identical failure backoff), `TestIsSyncingFalseAfterDeferredOrFailed`,
`TestDeferredStreakCounterResetsOnSuccess`; full metadata+bridge suites
green, `go vet` clean, new tests race-clean. **Pending:** live
cellular-relay validation that a 200-300s SCAN now completes within the 300s
budget (or defers quietly), the link stays usable between backstop ticks,
and /activity shows the deferred line instead of "Rebuilding index…" (unit
tests give false positives on this codebase per testing discipline).

## 2026-07-02 — G8: classify link by BACKEND route, not default route (task #81)

**Proven live (2026-07-02 11:05, iPhone-hotspot + Tailscale):** the NAS route
was `utun6` but `currentLinkClass()` said WIFI, because the interface signal
(`health.NetWatcher.ActiveInterface`, wired via `metadata.SetClassSignals` in
`bridge/cbridge.go`) reported the DEFAULT-ROUTE interface (`en0`). Measured
consequences: G7's `scanContextTimeout()` used the 120s WiFi budget instead of
the 300s cellular budget → the SCAN could never complete → keyspace-push
gap-fill never finished → engagement never ENABLED → G7's deferral never armed
→ a 60s/backoff retry loop burned the metered link. The same wrong class also
mis-gated G6's tunnel deferral, the 15m-vs-20m backstop, and the coalescer.

**Change (health/netwatch.go, bridge/cbridge.go):** the keyspace-push
NetWatcher is now constructed `WithBackendTarget(redisAddr)` (host:port from
`metadata.ParseRedisURL(cfg.RedisURL)`), which switches it from default-route
detection to backend-route detection: resolve *the interface the kernel routes
to the backend*.

**Resolver chosen — connected-UDP trick (no exec, no probe traffic):**
`net.Dial("udp", backend)` performs `connect(2)` on a datagram socket, which
sends NO packet — the kernel only does the routing-table lookup and binds the
local source IP of the chosen route; that IP is mapped back to its owning
interface via `net.Interfaces`. LAN backend (10.x over en21) → binds en21's
address → `en21`; Tailscale backend (100.64/10 or subnet-router route) → binds
the Mac's OWN utunN address (the utun has its own local IP) → `utunN`
(live-validated on the hotspot+Tailscale rig). Resolution is cached ~10s
(success AND failure) — the 1s poll is too hot for a possibly-DNS-involving
dial; route changes are rare, ≤10s detection lag is fine.

**Failure modes (all degrade to the pre-G8 default-route behavior, never
worse):** DNS-named backend with DNS down → bounded 2s dial timeout, failure
cached; fully offline (no route) → connect fails → fallback; loopback backend
(dev localhost Redis) → resolves `lo0` → unknown-name conservative WiFi band;
link-local IPv6 zone mismatch → no interface match → fallback. The connected
socket proves ROUTING only, not reachability (liveness stays with
`health.Reachability`).

**Override precedence untouched:** `JM_WAN_MODE=1` is still consulted FIRST in
`metadata.currentLinkClass` (forces tunnel regardless of any interface
signal); `JM_NET_FORCE_CLASS`/`JM_NET_ADAPTIVE` (netprofile) are a separate
classifier and unchanged. jm5 does not wire `SetClassSignals` — left as is.

**Revert:** no new env switch — the backend-route mode only engages under
`JM_METADATA_KEYSPACE_PUSH=1` (same gate as the NetWatcher itself), and
`JM_WAN_MODE=1` already pins the tunnel band without a rebuild if the resolver
ever misclassifies. Full revert = revert the commit (the `WithBackendTarget`
option is additive; dropping it restores default-route detection
byte-identically).

**Validated:** unit — `TestCurrentLinkClassBands` (+`utun6` case, via the
injected `SetClassSignals` signal), `TestInterfaceForIPRoundTrip` (real
`net.Interfaces` data: lo0/en0/...; utun can't be fabricated in CI —
live-validated), `TestInterfaceForIPUnownedIP`,
`TestResolveRouteInterfaceLoopback` (full connected-UDP path over the loopback
route), `TestResolveRouteInterfaceBadTarget`,
`TestNetWatcherBackendRouteSuccess/Cached/FallbackOnError/Recovery`,
`TestNetWatcherNoTargetUnchanged`. `go vet` clean; metadata + bridge suites
green; health green except the known environmental failures
(`TestRedisHealthCheck`/`TestMinIOHealthCheck`/`TestStatusReturnsCorrectState`
need a live local Redis/MinIO). **Pending:** live hotspot+Tailscale re-test
that `currentLinkClass()` now reports tunnel and the G7 300s budget engages.
