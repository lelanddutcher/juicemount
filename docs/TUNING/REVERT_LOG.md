# Tuning Revert Log

Per the cellular-revert-safety discipline: every link-class-gated or
WAN/cellular-affecting tuning change is logged here with its kill switch and
the exact baseline it reverts to, so a regression can be backed out **without
a rebuild**.

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
