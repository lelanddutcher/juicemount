# Tuning Revert Log

Per the cellular-revert-safety discipline: every link-class-gated or
WAN/cellular-affecting tuning change is logged here with its kill switch and
the exact baseline it reverts to, so a regression can be backed out **without
a rebuild**.

---

## 2026-07-03 — Perf lever: defer FTS5 trigram indexing off the write path (`JM_FTS_DEFER`)

Env-revertable without a rebuild. Motivation: under write storms (a large Finder
copy / SD-card offload / reconcile delta) the synchronous FTS5 **trigram** merge
was ~30% of metadata-write CPU. The `entries_fts` external-content index
(trigram tokenizer over name+path) is maintained synchronously in Go — every
`Insert` / incremental `BulkInsert` row does the expensive `_fts5*` trigram
INSERT inside the write transaction, on the hot path that also serves NFS
CREATEs. `#86/#88` moves that trigram INSERT OFF the write path.

**Lever — defer the NEW-rowid trigram INSERT to a background compactor.** When
ON (`JM_FTS_DEFER=1`), the write path keeps the cheap work synchronous and
records the new rowid in a durable `fts_pending(rowid)` marker table instead of
doing the trigram merge. A background goroutine (`ftsCompactorLoop`, started in
`Open` only when the flag is on, stopped in `Close`) drains `fts_pending` in
**bounded, coalesced batches** (`ftsCompactBatch`≈256 rows per `writeMu`-held tx,
`ftsCompactIdleSleep`≈2ms yield between batches, `ftsCompactTick`≈200ms idle
ticker + an explicit wake after each deferred write), so it never holds `writeMu`
long enough to starve foreground NFS serving or reconcile. Search stays
eventually-consistent: a just-created file is not searchable until the compactor
indexes it (sub-second), which is acceptable and expected.

- **`JM_FTS_DEFER`** — the A/B flag. **Default OFF** (unset / any value other
  than `1`): the write path is **byte-identical** to today's synchronous FTS —
  `ftsExternalUpsert` does the trigram INSERT inline, `fts_pending` stays empty,
  no compactor goroutine is started, and search is immediately correct. Set
  `JM_FTS_DEFER=1` to defer. Read once at `Open` (`ftsDeferEnabled()` →
  `s.ftsDefer`), so a revert is: unset the env and restart (no rebuild).

**INTEGRITY — the rowid-lifecycle landmine (preserved).** External-content FTS5
has NO triggers, so the OLD rowid's tokens must be removed synchronously before
that rowid can be freed and reused by a later `INSERT OR REPLACE`; otherwise a
stale token resolves to an unrelated file (a WRONG search hit) and violates FTS5
integrity. This removal stays SYNCHRONOUS in the write path via the new
`ftsDeleteOldRowid` helper, which handles BOTH index states the old row can be
in: if the old row was already INDEXED it issues the external-content
`entries_fts('delete', rowid, name, path)`; if the old row was STILL PENDING
(deferred, never indexed) it must NOT issue that `'delete'` (subtracting
never-inserted tokens corrupts the index — "database disk image is malformed")
and instead only clears the pending marker. Only the NEW-rowid trigram INSERT is
ever deferred. `Delete` / `DeletePaths` route through the same helper.

**Crash safety.** `fts_pending` is durable (same WAL DB as `entries`, written in
the SAME tx as the entries row). After a crash+restart, `Open`'s existing
`RebuildFTS` re-derives the FULL index from `entries` (the external-content
backstop — always rebuildable) and clears `fts_pending`; any surviving markers
are also drained by the compactor. No FTS data is ever lost, only deferred. The
spool `RecoverOnBoot` path is untouched.

**Revert.** Unset `JM_FTS_DEFER` (or set `≠1`) and restart. Flag-off is the
pre-lever synchronous behavior; `fts_pending` simply stays empty (the table is
created unconditionally — additive `CREATE IF NOT EXISTS` — so toggling the flag
never needs a schema migration). No rebuild required. Tuning knobs
(`ftsCompactBatch`, `ftsCompactIdleSleep`, `ftsCompactTick`) are package vars,
adjustable only via a rebuild, but the on/off switch is env-only.

---

## 2026-07-02 — Perf Lever 0: DSN-pin `synchronous=NORMAL` across all pooled connections (`JM_SQLITE_SYNC_NORMAL_ALL`)

Env-revertable without a rebuild. Motivation: the same CPU profile that
motivated Lever 1 showed ~22% in `database/sql.(*Tx).Commit`. A reducible
contributor is a per-connection pragma footgun in `metadata/store.go`. The
metadata DB sets `PRAGMA synchronous = NORMAL` inside the `pragmas` string and
applies it via `db.Exec(pragmas)` — but `synchronous` is a **per-connection**
setting, so `db.Exec` only takes on the ONE pooled connection that ran it. With
`db.SetMaxOpenConns(8)` the other 7 connections fall back to SQLite's default
`synchronous = FULL` (an extra fsync + directory sync per commit). Under write
load, commits fan out across all 8 connections, so most commits pay FULL-mode's
double-fsync — a large slice of the profiled 22% Commit cost. (This is the SAME
class of bug already fixed for `busy_timeout`, which is correctly set as a DSN
`_pragma` so modernc applies it to every connection. `journal_mode = WAL` is a
DB-header setting, so `Exec`-once is fine — only `synchronous` is
per-connection.)

**Lever 0 — pin `synchronous=NORMAL` on EVERY pooled connection via a DSN
`_pragma`.** When ON, `OpenWithMaxCacheSize` appends `&_pragma=synchronous(1)`
(1 == NORMAL) to the DSN, right after the `busy_timeout(30000)` DSN pragma, so
modernc applies NORMAL to all 8 connections. The existing `db.Exec(pragmas)` is
kept as-is (harmless; the DSN pragma is what makes NORMAL apply everywhere).

- **`JM_SQLITE_SYNC_NORMAL_ALL`** — the A/B flag. **Default OFF** (unset / any
  value other than `1`): behavior is byte-identical to today — NORMAL on the one
  connection that runs `db.Exec(pragmas)`, FULL on the other 7. Set
  `JM_SQLITE_SYNC_NORMAL_ALL=1` to pin NORMAL on all 8. Read once at `Open`
  (`syncNormalAllEnabled()`), so a revert is: unset the env and restart (no
  rebuild).

**Correctness — does NOT weaken durability below intent.** `synchronous=NORMAL`
under WAL is ALREADY the app's declared durability level (it is in `pragmas`);
this change only makes the other 7 connections MATCH that intent instead of
silently running STRICTER `FULL`. `journal_mode` is untouched (stays WAL). Under
WAL, NORMAL is crash-safe (a checkpoint fsyncs the WAL); a power loss can lose
only the last un-checkpointed transactions, which is the durability envelope the
app already accepted on connection #1. Verified: `TestSQLiteSyncNormalAllPins
EveryConnection` forces 16 concurrent pinned connections (2× the pool) and
asserts every one reports `synchronous == 1`; `TestSQLiteSyncNormalAllCrash
Recovery` round-trips a committed row across a Close/reopen with the flag on and
asserts `journal_mode` stays `wal`.

**Baseline it reverts to:** `JM_SQLITE_SYNC_NORMAL_ALL` unset → the
NORMAL-on-1 / FULL-on-7 behavior that shipped before this change.

---

## 2026-07-02 — Perf Lever 1: batch drain metadata SQLite writes (`JM_DRAIN_BATCH_INSERT`)

Env-revertable without a rebuild. Motivation: a CPU profile under a concurrent
write storm showed the app SYSCALL + SCHEDULER-bound (47% raw syscalls, 22%
netpoll, 16% thread park/wake), NOT lock-bound. A reducible contributor is the
volume of BACKGROUND drain metadata SQLite writes: the drainer commits a
SEPARATE transaction PER drained file — the size publish (`onSizeReady` →
`Store.UpdateSize`) and the mark-done (`MarkDrainComplete` → `SpoolStore.MarkDone`)
are two transactions, each its own WAL append + fsync. Under an N-file copy
storm that is ~2N transactions = ~2N fsync/syscall cascades competing with the
foreground NFS handlers on the same DB.

**Lever 1 — coalesce the drainer's per-file metadata writes into BATCHED
transactions.** When ON, a copied+SHA-verified drain is enqueued into a bounded
write-coalescer instead of committing inline; the coalescer flushes on the FIRST
of two thresholds — **256 ops** or **50 ms** since the batch opened — committing
every pending file's (size publish + mark-done) in ONE cross-table SQLite
transaction (`metadata.Store.BatchDrainComplete`). N drains → 1 transaction → 1
fsync cascade. The per-file post-commit cleanup (index evict, capacity release,
spool-file removal, manifest, metrics, `onDrainComplete`) runs unchanged, only
after the batch commits.

- **`JM_DRAIN_BATCH_INSERT`** — the A/B flag. **Default OFF** (unset / any value
  other than `1`): the drainer takes the per-file `onSizeReady →
  MarkDrainComplete` path exactly as today — byte-identical, the coalescer is
  never constructed (`drainer.batch == nil`). Set `JM_DRAIN_BATCH_INSERT=1` to
  enable the coalescer. Read once in `NewDrainer`, so a revert is: unset the env
  and restart (no rebuild).

**Correctness — task #65 (size-publish-before-eviction) preserved by
construction, NOT by timing.** For every file, `BatchDrainComplete` runs
`UPDATE entries SET size=MAX(...)` BEFORE `UPDATE spool_entries SET
drain_state=done` and both commit atomically in the one transaction; the
eviction side-effects (index evict, spool-file removal) run ONLY after that
`tx.Commit()`. So a file's authoritative size is durable no later than its
mark-done, and its spool shadow is not evicted until after the commit — a fresh
read can never resolve a post-eviction-but-pre-size 0/partial size. This is the
same guarantee the per-file path gives, with fewer fsyncs. entries.size stays
MAX-only (idempotent, matching `UpdateSize`).

> ⚠️ **SUPERSEDED (see the 2026-07-02 QA-37 addendum below).** The original
> claim here — that "the QA-37 cancel contract holds via `_txlock=immediate`"
> because the batch tx takes SQLite's write lock at `Begin` — was **WRONG**. In
> WAL mode a concurrent `DeleteActiveByPath` runs its decision-making SELECT as
> a *snapshot reader* that holds **no** write lock, so it could observe a
> still-`draining` row and collect it for deletion while the batch committed
> `drain_state=done` first (`Done=true`); the cancel's later DELETE then matched
> 0 rows and the user-deleted file was **resurrected** as a committed
> backend/Redis entry. The batch marked `spool_entries` done under
> `Store.writeMu`, a **different** mutex than the `SpoolStore.writeMu` the cancel
> serializes on, so `_txlock=immediate` alone could not order them. Fixed by the
> addendum: the batched mark-done now runs under `SpoolStore.writeMu`.

**No stranded writes / no offline-boundary spanning:** the batch flushes on
drain-idle (ready queue empty), before the dispatcher parks (offline / FUSE
identity loss), and on `Stop` (after in-flight workers drain). On any commit
error the whole tx rolls back and every file in the batch is retried transiently
(`failTransient`) with its FUSE write removed — no partial commit.

**Baseline to revert to = `JM_DRAIN_BATCH_INSERT` unset/`0`** (no rebuild). Not
link-class gated (it's a background-drain fsync optimization, link-independent),
but logged here per discipline because it changes durable write batching on the
metadata store that the offline promise depends on.

---

## 2026-07-02 — QA-37 fix: batched mark-done must serialize on `SpoolStore.writeMu` (cancel↔drain race)

Correctness fix to Perf Lever 1 above (`JM_DRAIN_BATCH_INSERT`). **Not
env-revertable within the lever** — the fix is required whenever the lever is
ON, so the only revert is turning the lever OFF (`JM_DRAIN_BATCH_INSERT` unset).
The per-file path was never affected and stays byte-identical.

**The defect (confirmed, adversarial review).** `metadata.Store.BatchDrainComplete`
marked `spool_entries` done under `Store.writeMu`, while the NFS delete path's
`metadata.SpoolStore.DeleteActiveByPath` (SELECT-then-DELETE) and per-file
`MarkDone` serialize on `SpoolStore.writeMu` — a **different** mutex. So a
mid-coalescer file in `draining` could be deleted by the user (NFS Remove →
`CancelForDelete` → `DeleteActiveByPath`) and BOTH sides "win": the cancel's
SELECT (a WAL snapshot reader, no write lock) sees the row `draining` and
collects it; the batch commits `drain_state=done` (`Done=true`); the cancel's
DELETE (filtered to writing/ready/draining) then matches 0 rows. `Done=true`
drove `BatchCompleteDrainCleanup` + `onDrainComplete` → the user-deleted file was
**resurrected** as a committed backend/Redis create + `entries.size`.
`_txlock=immediate` did **not** save it (the cancel's decision-SELECT holds no
write lock). The per-file path is immune because `MarkDone` and
`DeleteActiveByPath` share `SpoolStore.writeMu`.

**The fix (approach A — restores the exact per-file invariant).** The batched
mark-done moved into `SpoolStore.batchMarkDoneTx(tx, items)`, which
`BatchDrainComplete` calls **while holding `SpoolStore.writeMu`**, passing its
own `*sql.Tx` so the `entries.size` publish and the `spool_entries` mark-done
stay in ONE atomic transaction (task #65 preserved). `BatchDrainComplete` now
holds BOTH `Store.writeMu` and `SpoolStore.writeMu`. A cancel and a batch
mark-done can no longer interleave — one runs entirely before the other, exactly
as `MarkDone` vs `DeleteActiveByPath` do. `Store` gets the sibling `SpoolStore`
via `SetSpoolStore`, wired in `handler.SetSpool` (independent of the drainer);
`BatchDrainComplete` **fails closed** (errors, whole batch retried transiently)
if flushed unwired, rather than run the mark-done unserialized.

**Deadlock audit.** Global lock order established: **`Store.writeMu` →
`SpoolStore.writeMu` → SQLite-write-lock (`tx.Begin`, `_txlock=immediate`)**.
Audited every acquisition site of both mutexes across `metadata/` + `nfs/`:
`Store` holds no reference to `SpoolStore` and no `SpoolStore` method calls back
into any `Store` method, so before this change the two lock domains were
disjoint and `BatchDrainComplete` is the ONLY site that ever holds both — no
inversion possible. Critically, `SpoolStore.writeMu` is acquired **before**
`tx.Begin`, so no `SpoolStore` writer can hold `SpoolStore.writeMu` while blocked
on the WAL write lock this tx holds (the reverse pairing would 30s-stall on
`busy_timeout`, not hang, but is avoided outright).

**Regression test (the one that was missing).**
`TestBatchDrainCompleteConcurrentCancelDoesNotResurrect` (`metadata/`, runs under
`-race`) drives the real concurrent interleaving via a test-only sync hook fired
inside `DeleteActiveByPath` between its SELECT and DELETE: the cancel parks there
holding `SpoolStore.writeMu`, the batch flush is launched and asserted to BLOCK
(not complete) until the cancel releases, then the cancel's DELETE wins the row
and the batch's mark-done sees 0 rows → `Done=false` → no resurrection; a
sibling keep-row still commits `Done=true`. Verified to FAIL on the pre-fix code
(batch completed unserialized while the cancel was parked) and PASS after.
`TestBatchDrainCompleteCancelledRowReportsNotDone` (the delete-before-batch case)
is retained.

**Baseline to revert to = `JM_DRAIN_BATCH_INSERT` unset/`0`** (no rebuild); the
per-file drain path is unchanged and carries no resurrection risk.

---

## 2026-07-02 — Item 0: pinned-subtree metadata warming (pinned dirs list offline)

Env-revertable without a rebuild. Motivation: pinning a directory cached its
file BLOCKS but never wrote the directory's METADATA subtree into the mirror.
Online a not-yet-mirrored pinned dir fell through to the FUSE fallback and
listed; OFFLINE the handler correctly refuses FUSE and returned EMPTY — so a
pinned dir listed empty offline, breaking the core offline promise. Root cause:
`pin.CountFilesUnder` returns FILES ONLY, so `NFSServerPin`/`PinMany` warmed
blocks but never reconciled subtree rows into the metadata Store.

**Item 0 — warm the pinned subtree's metadata into the mirror at pin time and
at boot.** New exported `metadata.RedisClient.ReconcileSubtree(rootInode, maxDirs)`
and `ReconcileAncestors(internalPath)` LOOP the existing `reconcileDir`, which
durably writes SQLite `entries` (via `Insert` → `ftsExternalUpsert`'s
`INSERT OR REPLACE INTO entries`) AND the `childrenIdx` — so the rows survive a
reboot (the load-bearing property). `NFSServerPin`'s existing detached goroutine
calls both after `PinMany`; a boot-time background pass enumerates
`pinStore.PinRoots()` and warms each root, healing dirs pinned in a prior
session. No parallel Redis fetcher — reuses `reconcileDir` verbatim.

- **`JM_PIN_WARM_METADATA=0`** — kill switch. Restores today's behavior
  (blocks cached, metadata NOT warmed at pin time — a pinned dir stays empty
  offline until a full SCAN happens to cover it). Default (unset / any non-`0`
  value) = warming ON. Checked inside `PinWarmMetadataEnabled()`, gating both
  `ReconcileSubtree` and `ReconcileAncestors` and both bridge call sites.

**Gates / guards (all fail toward today's behavior, never toward data loss):**
- **Offline gate:** the whole warm pass is skipped when `pin.IsOffline()` — an
  offline `reconcileDir` burns a full 30s Redis timeout per dir. The pin BLOCKS
  are already cached; metadata warming resumes on the next pin / next boot when
  online. Checked at the top of both functions AND at both bridge call sites
  (and re-checked between roots in the boot pass).
- **ScanFilteredPath guard:** a pin root matching `metadata.ScanFilteredPath`
  (`.trash` / `.juicemount`) is REJECTED with a clear "cannot pin internal
  namespace" — `reconcileDir`'s #78 filter would silently no-op it, so warming
  is impossible. `NFSServerPin` rejects at entry; the metadata functions
  self-reject with `errPinInternalNamespace` as defense-in-depth. The filter is
  NOT weakened.
- **Bounded walk:** `ReconcileSubtree` BFS is capped at `maxDirs` (default
  50,000); on truncation it logs and stops, leaving the remainder to the
  authoritative full SCAN.

**Substrate note:** this is independent of the SQLite-direct serving decision
(serving-layer-decision.md §Item 0) — the metadata must be warmed at pin time
regardless of whether nav serves from the RAM shadow or SQLite-WAL. Baseline to
revert to = `JM_PIN_WARM_METADATA=0` (no rebuild).

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

## 2026-07-02 — Boot fast-path S1 (parallelize mount ‖ store-open ‖ connect)

Env-revertable without a rebuild. Motivation: `NFSServerStart` booted strictly
serial — `fm.Mount()` (~1.4-7s) → `metadata.Open()`+hydrate (~6s) →
`connectRedisWithRetry` (~1-2s) → `srv.Start()` — ~16s before Finder showed
anything.

**S1 — run the independent boot steps concurrently.** `fm.Mount()`,
`metadata.Open()`, and `connectRedisWithRetry` are mutually independent (the
mount needs only `cfg`+`backendUp`; connect needs only the opened store).
The mount is extracted into a `mountFUSE` closure and run in a goroutine; the
main flow opens the store then connects (serial — connect needs the store) so
the mount overlaps both. **No semantic change** — identical work, identical
causal order (store→connect unchanged), only the mount now overlaps instead of
stacking. Collapses ~16s serial → ~max(mount, open+connect).

**Ordering / invariants preserved:**
- The mount is JOINED (`WaitGroup.Wait`) **before** `srv.Start()` — NFS never
  serves before the mount lane registered `globalFUSE` + armed G0
  `pin.SetFUSEIdentityPath` (preserves pinned-files-instant; backgrounding the
  mount past NFS-serve is the separate, out-of-scope **S9**).
- `srv.Start()` still needs the store OPEN+hydrated; it does NOT need Redis
  connected (`SetRedisClient` is post-Start already). Connect is joined in the
  main flow before its result feeds `rc.SetPathConfig`/`SetReconcileInterval`/
  `rc.Start()`.
- A fatal `metadata.Open` error still returns (after joining the mount
  goroutine so we don't abandon an in-flight global write); a mount failure
  still `StartMonitor`+registers `globalFUSE` UNCONDITIONALLY inside `mountFUSE`
  (QA-7 monitor-leak guard intact); a malformed-URL connect still takes the
  deferred-client path. `bootSyncWired`, the R-4 offline path
  (`backendUp==false` skips `fm.Mount`), `globalMu` discipline, and the
  reachability liveness hook (reads `globalDrainerAtomic`, never `globalMu`)
  are untouched. The mount lane and the store lane write disjoint globals
  (`globalFUSE*` vs `globalStore`) and the parent blocks on the join before any
  other code reads them, so no new race (`-race` clean).

- **`JM_BOOT_PARALLEL=0`** — kill switch. Runs the three steps strictly serially
  in the original order (`mountFUSE()` → `openStore()` → `connect`),
  byte-identical to the pre-S1 flow. Baseline for a bisect if the goroutine
  orchestration is ever suspected.

**Validated:** the orchestration is not unit-testable in isolation (needs a
live FUSE mount + Redis); both the parallel and serial paths reuse the SAME
`mountFUSE`/`openStore`/`connect` calls, covered by the bridge suite. `gofmt`
clean; `go build ./...` green; `go test -race ./metadata/ ./bridge/ -count=1`
green. Live boot-timing (the ~16s-to-first-items shrinking to ~max serial over
a cellular relay) is the real proof — pending.

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

---

## 2026-07-02 — Item 1: paginated READDIR via ListChildrenPage (big-dir veto mitigation)

Env-revertable without a rebuild. Motivation: the serving-layer decision
(VISION/serving-layer-decision.md §Item 1) moves NFS nav toward serving from
SQLite-WAL, but a naive whole-dir `SELECT` on the 10,774-child DCIM dir was the
one number that vetoed the port: 12.7ms p99 / 6.26MB / 215,517 allocs as a
single full scan+copy. READDIR must be PAGINATED first.

**Item 1 — `Store.ListChildrenPage(parentPath, cursor, limit)`** runs a prepared
seek-cursor query on the mirror — `WHERE parent_path=? AND name > ? ORDER BY
name LIMIT ?` — returning one READDIR page + a next-cursor (the last name). A
`sync.Pool` recycles the scalar scan scratch to cut per-row allocs. The NFS
`ReadDir` path (nfs/handler.go) builds its whole-dir listing by iterating
`ListChildrenPage` in bounded pages instead of one giant `ListChildren` map
copy. (The NFS PROTOCOL layer — nfs_onreaddir.go — still hashes+caches the full
listing and paginates by index into it; Item 1 changes only how that listing is
BUILT, not the protocol contract. See "uncertainty" below.)

**Load-bearing schema addition — `idx_parent_name ON entries(parent_path,
name)`:** without a composite (parent_path, name) index the planner uses
`idx_parent` for the equality but then builds a `USE TEMP B-TREE FOR ORDER BY`
that sorts the ENTIRE child set by name on EVERY page — making each page
O(all-children), ~2.2ms on the DCIM dir REGARDLESS of LIMIT (measured, and it is
the same floor at page size 64 or 512). The composite index satisfies both the
equality seek AND the name ordering from the index (no temp b-tree), turning
each page into a true O(limit) seek: a realistic 256-row page drops to ~0.30ms
(first page ~0.30ms, middle page ~0.33ms) — a ~7x win and well under the
sub-1ms/page RPC budget. The index is additive (`CREATE INDEX IF NOT EXISTS`),
BINARY collation (matches the plain `ORDER BY name`; idx_name's NOCASE variant
does NOT satisfy a BINARY ordering), and lands on existing mirror DBs at open.
It costs one extra b-tree on writes — negligible vs the READDIR win.

- **`JM_READDIR_PAGINATED=1`** — enables the paginated whole-dir build in NFS
  ReadDir. **Default OFF (unset / != "1")** = today's behavior: the RAM
  `childrenIdx` whole-dir map copy, byte-identical. Checked in
  `readdirPaginatedEnabled()` (metadata/serve_sqlite.go), consulted only inside
  `Store.ListChildrenForReadDir`; no handler.go call-site behavior changes when
  off. `JM_SERVE_FROM_SQLITE=1` (Item 2) implies the paged build too.

**Revert:** unset `JM_READDIR_PAGINATED` → instant return to the RAM whole-dir
copy, no restart-of-the-world. The idx_parent_name index remains (inert when
unused; harmless extra index). `ListChildren` (the RAM whole-dir accessor) is
untouched and still used when the flag is off.

**Validated:** unit — `TestListChildrenPage_MultiPage` (25 items/10-limit → 3
pages, strict name order, no dupes/gaps), `_ExactMultiple` (20/10 → empty tail
page terminates), `_EmptyDir`, `_OverLimit` (100/7, no dupes),
`TestListChildrenPage_BigDirUnderBudget` (10,774-child, 256-row page < 2ms gate
— catches idx_parent_name loss). Benchmarks (Apple M2 Pro, on-disk WAL + shared
`:memory:` both):
`BenchmarkListChildrenPage_BigDir` (256-row middle page) = ~333µs / 3,867
allocs; `_FirstPage` = ~302µs; `_FullPage` (1000-row) = ~1.17ms. `go vet` clean;
metadata suite green. **Pending:** real Finder open of the DCIM dir (no
beachball) — unit benches are the veto-mitigation proof, not the field test.

---

## 2026-07-02 — Item 2: SQLite-direct serve accessors behind JM_SERVE_FROM_SQLITE (live A/B)

Env-revertable without a rebuild. Motivation: felt problem (c) — 10GbE nav
sluggish under a concurrent writer — is a reader stall, not idle latency. Every
serve accessor (`LookupByPath`/`LookupByInode`/`ListChildren`) takes only
`s.mu.RLock` and Go's RWMutex is write-preferring, so a queued writer's 2048-row
`Lock` chunks block new readers mid-burst. Benchmark-proven: under a contending
writer, small-dir nav p99 = SQLite-WAL 206µs vs RAM RWMutex 0.93-1.6ms. WAL
readers snapshot around the writer and remove `s.mu` from the read path.

**Item 2 — SQLite-backed serve accessors with IDENTICAL signatures/semantics**
to the RAM accessors, gated INSIDE each accessor (metadata/store.go):
`if serveFromSQLite() { <sqlite> } else { <existing RAM map read> }`. Both paths
are compiled in; the flag picks per call, so NO handler.go call site changes and
rollback is one env var. The substrate is logged ONCE per boot
(`metadata: serve substrate = …`) so an A/B run is attributable.

**Concurrency model (the crux):** every SQLite serve query runs against a
lazily-prepared `*sql.Stmt` owned by the Store and prepared against the
`*sql.DB` POOL (not a single conn). modernc.org/sqlite's `*sql.DB` is
concurrency-safe and pools connections (`SetMaxOpenConns(8)`); a `*sql.Stmt`
from `db.Prepare` is safe for concurrent use by many goroutines —
`database/sql` re-prepares it per pooled connection on demand and caches that
per-conn, so N concurrent NFS goroutines each take a pooled conn and run the
point/page query with NO shared Go-level lock in the hot path and NO per-RPC
Prepare. The only sync is a one-time `sync.Once` for lazy stmt init. Point
lookups use `QueryRow` on the PRIMARY-KEY (path) / `idx_inode`; ReadDir uses the
Item 1 paged `idx_parent_name` seek. This satisfies `feedback_perf_hot_path`: a
warm prepared point/index query against the WAL page cache is not a cold
FUSE/Redis syscall-per-RPC. WAL + `synchronous=NORMAL` already set in `pragmas`.

- **`JM_SERVE_FROM_SQLITE=1`** — serve `LookupByPath`/`LookupByInode`/
  `ListChildren` from SQLite-WAL. **Default OFF (unset / != "1")** = RAM maps,
  byte-identical to prior behavior (the A/B baseline and the rollback). Read
  once and cached in `serveFromSQLite()` (metadata/serve_sqlite.go).

**LEFT UNCHANGED (correctness gates, not perf):** the offline-empty branch
(handler.go ~1922) and the online FUSE-fallback branch (handler.go ~1924+).
They call `ListChildren`, which may now be SQLite-backed — fine: a SQLite
`ListChildren` returning 0 for a genuinely-empty/unmirrored dir behaves
identically to the RAM map returning 0. **NOT deleted (Item 3, a later commit
gated on QA-battery-green):** `rebuildCaches`, the 3 maps, and all writer-side
cache maintenance — both paths coexist behind the flag.

**Parity guarantees (verified):** the SQLite scan column list matches
`scanEntry` exactly (9 cols incl. `local_only`) with the SAME semantics — high-bit
inode reinterpret (`uint64(int64)`), `time.Unix(mtime,0)`, `fs.FileMode(mode) |
ModeDir` — so a row here equals the RAM map's Entry for the same row.

**Known, bounded divergences (flagged, not papered over):**
1. `InsertToCache` (symlink seed, handler.go ~1831) writes the RAM maps
   synchronously then `go Insert` commits SQLite async. In that brief window a
   SQLite LOOKUP misses where RAM would hit; it self-heals when the async Insert
   commits (and the FromHandle/Stat fallback re-seeds). Bounded to the
   symlink-mint path; correctness gates are untouched.
2. `PreSerializedGetAttr` (a lazy per-entry XDR cache on the RAM `*Entry`) is
   nil on SQLite-served entries, so GETATTR recomputes the 88-byte fattr3 each
   call — a small perf regression, not a correctness one. (Item 3 / a future
   pass can add a serve-side XDR cache if it shows up.)
3. RAM `ListChildren` returns children in map-iteration (nondeterministic)
   order; the SQLite path returns them NAME-SORTED. Every serve caller that
   cares already sorts (ReadDir, getDirListingWithVerifier), so this is a
   strict superset of the RAM contract — same SET, a defined order — never a
   divergence in the child set. Verified: RAM vs SQLite child SETS are
   identical across 1,234-child, unicode, `._` sidecar, dot-file, case, and
   root-parent cases.

**Revert ladder:** Item 2 misbehaves → `JM_SERVE_FROM_SQLITE=0` (instant, no
restart; the shadow is still maintained so it's warm). Item 1 misbehaves →
`JM_READDIR_PAGINATED=0`. Both default off, so a stock build is unchanged.

**Validated:** unit — `TestServeParity_LookupByPathAndInode` (500 rows + high-bit
inode + local_only + a dir; RAM path == SQLite path for a sample of path/inode
lookups incl. a not-found → nil on both), `TestServeParity_ListChildren` (1,234
children: identical set, SQLite name-sorted), `TestServeParity_EmptyDir` (both
return nil, not empty slice), `TestServeFlagOff_UsesRAM` (flag off → public
accessor returns the RAM cache entry). Benchmarks: `BenchmarkLookupByPath_RAM`
~16ns vs `_SQLite` ~3.1-5µs (matches the design's 85ns→~7µs idle claim — both
sub-RPC-budget). `go vet` clean; metadata suite green (nfs suite: only the
known-environmental `TestMemBuf*` failures, unrelated). **Pending (the real
gate):** live 10GbE A/B under a background edit storm (`=1` vs `=0`) — SQLite-on
p99 must beat RAM-on p99; and the torn-read re-run of the v0.2.0 HOLD repro on
the SQLite path (zero silent truncation). Do NOT flip default-on or proceed to
Item 3 (shadow removal) until those pass.

---

## 2026-07-02 — Data-integrity fix: GETATTR reports full written size, not contiguous prefix (#85/#65/#38)

**Not a tuning lever — no env kill switch, no link-class gate.** This is a
correctness fix logged here for revert traceability. It has no A/B flag because
the pre-fix behavior is a bug (a truncated stat after a large write), not a
tunable trade-off.

**What changed.** `nfs/spool_readfile.go` `spoolFileInfoForEntry` now reports the
spool entry's **`WrittenEnd()`** (the written high-water) as the FileInfo
`Size()`, instead of `ContiguousEnd()` (the contiguously-written prefix from
offset 0). This is the single site that synthesizes a spool-shadow size for
GETATTR/Stat/Lstat (routed from `handler.go` Stat/Lstat short-circuits, and from
the WRITE/COMMIT reply post-op attrs via `tryStat → Lstat`).

**Why.** Under macOS out-of-order async WRITE dispatch, `contiguousEnd` pins at a
JuiceFS block boundary (~4 MiB) while the true high-water races to full, so
GETATTR returned a truncated size after a large write until the macOS attr cache
refreshed — and the stale size also seeded the client attr cache via WRITE/COMMIT
post-op attrs. Decoupling "reported size" from "readable prefix" is safe: the
read path (`spoolReadFile.ReadAt`/`ReadableBounds`/`IncompleteAt`) independently
clamps to `contiguousEnd` and JUKEBOX-holds an in-flight hole rather than serving
zeros, so a read into `[contiguousEnd, writtenEnd)` never fabricates data even
though the reported size now covers it.

**Shrink safety.** An authoritative NFS SETATTR{size}/Truncate shrink lowers
`writtenEnd` (`nfs/spool.go` `Truncate` sets `writtenEnd = size` on both grow and
shrink), so a client-commanded shrink is honored, never masked by a stale
high-water. Invariant preserved: task #65 size-publish-before-eviction is
untouched (finalize/drain still publish the finalize-time full size).

**Residual (accepted):** a preallocate-then-fill writer (fio ftruncate up-front)
over-reports `writtenEnd` vs bytes actually filled. Acceptable: reads into the
unfilled region hold/JUKEBOX or read zeros correctly — no truncation. The
dominant cp/Finder/OpenLoupe pattern is write-then-ftruncate where
`writtenEnd == real bytes`.

**Revert:** one line — restore `size: e.ContiguousEnd()` in
`spoolFileInfoForEntry`. Requires a rebuild (this is compiled behavior, not an
env flag). Reverting reintroduces the truncated-stat-after-write bug.

**Validated:** `nfs` — rescoped `TestSpoolInFlightReadNeverServesHoleAsZeros`
(now asserts `Size()==writtenEnd` AND that a hole read still returns
`ErrSpoolIncomplete` despite the larger size), new `TestSpoolShadowReportsFullSizeImmediately`,
`TestSpoolShadowSizeMonotonicOutOfOrder` (the discriminator — red pre-fix),
`TestSpoolShadowTruncateShrinkHonored`; `internal/nfs` —
`TestGetAttrFullSizeDuringInflightWrite` (onWrite RPCs then onGetAttr →
`Filesize==totalWritten`, plus WRITE and COMMIT reply post-op attrs carry the
full high-water). `gofmt`/`go vet` clean; `nfs`/`internal/nfs`/`metadata` green
under `-race` (only the known-environmental `TestMemBuf*` async-load failures,
unrelated).

---

## 2026-07-02 — #87: un-gate Go-core start from the network preflight (first-launch hang)

Compiled behavior (Swift), NOT an env flag — reverting requires a rebuild.

**What.** On the ALREADY-ONBOARDED launch path
(`app/JuiceMount/Sources/JuiceMount/App.swift`, the `else` branch of
`applicationDidFinishLaunching`), `server.start()` is now called IMMEDIATELY and
synchronously — no `await` on the Redis TCP dial. The `OnboardingPreflight` now
runs only as a NON-BLOCKING diagnostic in a `Task.detached` that can never hold
back start(): it re-opens the setup assistant ONLY for LOCAL hard-stops
(`report.juicefsPath == nil || !report.macFUSEInstalled`), never merely for an
unreachable backend (the Go core boots offline via R-4 and self-recovers). The
genuinely-first-run onboarding `if` branch is UNCHANGED — a brand-new user still
onboards before the core starts.

**Why.** First launch after install/update hung ~150 s because `server.start()`
was gated on `report.criticalOK`, which requires `backendReachable` — a cold
`tcpReachable` NWConnection dial to Redis. On a cold first-connect that dial
stalls, and its 3 s watchdog was scheduled on the SAME serial queue as the
connection so it could not fire. The Go core needs nothing from the preflight
(`start()` reads only `preferences.toServerConfig()`) and already supports
start-while-offline, so gating the mount on the dial was pure, removable latency.

**Two supporting changes (same commit):**
- `app/JuiceMount/Sources/JuiceMount/UI/MenuBarController.swift`:
  `SPUStandardUpdaterController(startingUpdater: false, …)` + a deferred
  `updaterController.startUpdater()` fired `DispatchQueue.main.asyncAfter(+12 s)`
  — no Sparkle feed fetch in the launch window. (Sparkle 2.9.3; `startUpdater()`
  is the documented public API for the `startingUpdater:false` path.)
- `app/JuiceMount/Sources/JuiceMount/UI/OnboardingWindowView.swift`
  `tcpReachable`: the 3 s watchdog `asyncAfter` now runs on an INDEPENDENT
  `com.juicemount.preflight.timeout` queue (not the connection's
  `com.juicemount.preflight.dial` queue), so a wedged NWConnection setup can no
  longer starve its own timeout. `finish()` is guarded by an `NSLock` so the
  continuation still resumes exactly once regardless of which queue fires first.

**Revert:** restore the `else`-branch body to `Task { … if report.criticalOK {
server.start() } else { openOnboardingWindow() } }`, set Sparkle back to
`startingUpdater: true` (drop the deferred `startUpdater()` call), and move the
`tcpReachable` timeout back onto the dial `queue` (dropping the lock). Requires a
rebuild. Reverting reintroduces the ~150 s first-launch hang.

**Build:** `swift build -c release` of `app/JuiceMount` (with the build script's
`-L build -lnfsd` + framework link flags) compiles clean — Swift-only change;
no Go/c-archive change.
