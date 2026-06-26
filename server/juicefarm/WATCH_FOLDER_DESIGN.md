# Watch-folder — auto-pre-generate farm derivatives on file arrival

> **STATUS: EARMARKED — design only, no build. (2026-06-25)**
> Leland: "hold off for now." This is the architecture + phased path for a new-file
> watcher that auto-enqueues farm derivatives. Nothing here is built beyond the
> components explicitly marked BUILT. Do not start the watcher until its blocking
> dependencies (below) land.

## Intent
You drop files onto a JuiceMount volume — a Finder copy, an NLE export, or (the headline
case) a **photographer/videographer SD-card offload** — and the derivatives (proxy,
filmstrip, poster, waveform, tech, transcript) **just get pre-generated on the farm** and
surface in OpenLoupe with no manual "Generate" click. The watcher is not a new subsystem:
it is a **third producer** against the already-built P4 Redis queue
([`FARM_QUEUE_PROTOCOL.md`](../../../../juicemount-contract/FARM_QUEUE_PROTOCOL.md)),
alongside the Manager (`POST /api/farm/sweep`) and OpenLoupe
([`FARM_CONTROL_SURFACE.md`](../../../../juicemount-contract/FARM_CONTROL_SURFACE.md)).
Every flavor below differs ONLY in **what triggers the enqueue**; the sink, the worker,
and the result-return path are identical and already proven.

---

## The core tension (read this first)
This feature is in direct tension with the **locked decision: pre-generation is OFF
volume-wide by default**, opt-IN per directory ([FARM_TAB_SPRINT.md](FARM_TAB_SPRINT.md)
"Decisions locked"). The load-bearing rationale: NLEs (DaVinci/Premiere) write their OWN
proxies into the project tree — **we must not make proxies of proxies** — and a giant
folder landing on the volume must **never trigger a surprise full-volume sweep**.

**The watcher must be default-DENY by construction, not by heuristic:**
- Scope is resolved by an **upward walk** from the new file's directory toward the volume
  root, looking for a `.juicefarm` opt-in marker at each ancestor. **First marker found =
  the governing opt-in root**; **no marker before root ⇒ NOT opted in ⇒ enqueue nothing.**
- A 2 TB card dump into a non-opted directory finds no marker → zero jobs → the queue never
  sees the paths → the worker never walks them. There is **no code path that turns "a lot of
  new files" into work**; work exists only where a marker was deliberately placed.
- **Fail CLOSED:** unreadable marker, walk error, or unparseable policy ⇒ treat as NOT
  opted-in (skip). Mirrors the contract-platform fail-closed hash-gate discipline.
- **Defense in depth:** the gate is checked **producer-side** (watcher, pre-enqueue) AND
  re-checked **worker-side** (pre-drain) using the *same* upward-marker walk, so a
  direct-Redis producer (OpenLoupe) can't bypass it and the two sides can't disagree.

> **Hard prerequisite:** the `.juicefarm` opt-in marker is **Phase 5, NOT BUILT** (zero code
> references confirm). Until it exists there is no in-code notion of "opted-in subtree," so
> any watcher built now would fire volume-wide — violating the locked decision. The marker is
> a prerequisite, not an enhancement. The worker's `collectTargets` (`cmd/jmfarm/main.go`)
> today does a **blind `filepath.Walk(job.Path)` with zero opt-in check** — it must gain the
> same re-check before any direct-Redis producer is opened.

---

## Detection: how do we know a file arrived?

### Primary — Mac core write-completion event → Redis `juicemount:metadata` (PUSH)
The signal **already exists and already fires.** Both durable-landing points publish an
`{Op:"create", Path, Size, Mtime, Inode}` event to the Redis pub/sub channel
`juicemount:metadata`:
- **Direct (non-spooled) write:** `writeFile.Close()` (`nfs/handler.go:~2613`) publishes on
  close.
- **Spooled write (the offload/ingest path):** `onSpoolDrained` (`nfs/handler.go:~501-521`,
  wired via `drainer.SetOnDrainComplete`, `nfs/handler.go:~468`) fires from
  `nfs/drainer.go:~536` **AFTER `MarkDrainComplete`** — i.e. after the bytes are SHA-verified
  and durably in the JuiceFS backend.

A farm watcher SUBSCRIBEs to that channel, filters `Op=="create"` to paths under an opted-in
`.juicefarm` subtree, and calls `farmqueue.Enqueue`. Near-zero new detection plumbing,
sub-100ms latency, and **the spool variant fires only post-drain, so it never trips on a
torn/in-flight file** — it respects the hard-won "don't act on incomplete files" lesson for
free.

### Fallback — server-side cursored poll (BACKSTOP, not the trigger)
Redis pub/sub is **fire-and-forget with no replay** — any event published while the watcher,
its subscriber, or the worker is down is **lost**. So pair the event stream with a
slow-cadence reconcile poll (the same belt-and-suspenders the metadata mirror already uses,
`metadata/redis.go:~62`):
- Scope ONLY to opted-in `.juicefarm` subtrees (never a full-volume scan).
- Carry an **mtime/cursor watermark** so each poll diffs only new-since-last entries.
- Diff candidates against the derivatives index — `derivatives.Known(inode)`
  (`internal/derivatives/store.go:~151`) and `ListChangedSince(since,limit)` (`:~371`) — and
  enqueue only the gaps.
- A poll that walks the FUSE tree directly must NOT assume `presence == verified`: the drainer
  writes dest then `MarkDrainComplete`, so a mid-drain dest exists transiently. Cross-check
  against the manifest's `drain_done` record, not raw stat.

### Why filesystem-watching (inotify / fsnotify / NFS) is the WRONG tool here
- **No watcher dependency exists** anywhere in the repo (`grep -rE 'fsnotify|inotify|fanotify'`
  = zero hits; `go.mod` has none) — confirming this was never relied on, by design.
- **The Mac is the NFS *server*.** Client writes arrive as WRITE/COMMIT RPCs, **not** as
  create events on a watched tree — there's nothing to watch. NFS does not deliver reliable
  create notifications to a watcher on the export.
- **WRITE RPCs are not the completion point.** macOS sends UNSTABLE writes deferred to a later
  COMMIT (`nfs/nfs_onwrite.go` forces durability only on DATA_SYNC/FILE_SYNC). A watcher keying
  off raw RPCs would fire on a file not yet durable.
- **Watching the NAS-side FUSE mount is doubly wrong.** A Mac write lands in the **spool first**
  (`nfs/spool_writefile.go`) and is **not visible in JuiceFS** until the async drainer copies it
  in (`nfs/drainer.go`); FUSE create-event delivery to inotify on JuiceFS is partial/unreliable.
  You'd watch the wrong layer and still race the drain.

**Conclusion:** key off the **published `create` event** (which both write paths emit at their
*true* completion point), with the cursored poll as the reconcile net. Never off raw FS events.

---

## The hard parts (and the recommended answer to each)

### 1. Settle / half-written files
**Risk:** processing a torn or still-draining file. **Answer:** the offload/spool path solves
this for free — `onSpoolDrained` fires **only on the success path, after** streaming-SHA match,
at-rest SHA re-verify, and `MarkDrainComplete`. Transient-fail and SHA-mismatch quarantine paths
(`drainer.go` failTransient/quarantine; manifest `drain_failed`/`quarantine`) **never** reach the
callback. So subscribing to the `create` event inherently excludes corrupt/incomplete files. For
direct (non-spool) writes, gate on `writeFile.Close()` + a short debounce; a poll-discovered file
must additionally confirm via the manifest `drain_done` record, never raw presence.

### 2. Backpressure / batching (the card-dump case)
**Risk:** a 5,000-file SD-card offload becomes 5,000 single-file enqueues, and the watcher
competes with the offload for the 10GbE / MinIO while bytes are still draining. **Answer:**
- A card dump is **N spool entries draining asynchronously** (bounded by `JM_DRAIN_WORKERS`,
  default 4) — `create` events arrive **spread across the whole drain window**, not in a burst.
- **Debounce + directory-coalesce:** buffer arrivals and emit **one folder-scoped job** per
  settled directory, not N file jobs (the worker's `collectTargets` already walks a dir
  recursively — folder-scoped is cheaper *and* natural).
- Consider **holding enqueues while the spool is still draining** and resuming on a
  quiescent-spool signal, so the watcher doesn't compete with ingest for bandwidth (the drainer
  already has capacity backpressure to subscribe to).
- **Never enqueue a job whose `path` is a high ancestor** (e.g. the opt-in root). That would make
  `runJob → collectTargets → filepath.Walk(root)` re-walk the WHOLE tree on **every** new file.
  Scope each enqueue to the specific arrived leaf file/dir.

### 3. Dedup / skip-already-done
**Risk:** `GenerateProxy` re-encodes **unconditionally** today (FARM_QUEUE_PROTOCOL v1 "No
enqueue-time dedup"). A poll re-runs the whole volume every sweep; a push re-encodes on every
NLE relink/overwrite. **Answer:** key dedup on **`(inode, kind, source_hash)`** against
`derivatives.Known()` / `ListChangedSince` + the per-source content hash from
`derivatives.PutSource` (`store.go:~236`). Critically, an **overwrite-in-place** (re-export /
NLE relink over the same name) routes through the spool and re-fires `onSpoolDrained` with the
**same path** — the watcher must treat a repeat `create` as **"source changed, re-derive"**
(keyed off inode + content-hash, NOT first-seen), or it will skip regenerating a stale proxy
after a relink. Idempotent overwrite means missing dedup is *wasteful, not incorrect* — but it
makes scheduled/auto sweeps unaffordable. **Dedup is BUILT? No — Phase 4 "dedup", unbuilt.**

### 4. Crash-safety / at-least-once
**Risk:** pub/sub drops events while the subscriber or worker is down (no replay/backlog).
**Answer:** the **cursored reconcile poll is the at-least-once backstop** — the event stream is
the fast path, the poll guarantees completeness, exactly mirroring the metadata-mirror reconcile
pattern. Do not rely on the event stream alone. The P4 queue itself persists jobs in Redis
(survives a worker restart), and the worker's `MarkDone`/`MarkFailed` make a re-drained job
re-runnable; dedup (above) makes a replayed enqueue a no-op rather than a re-encode storm.

---

## Recommended phased path

> **Phase 0 (BLOCKING precondition — not the watcher itself, but the watcher is invisible
> without it): wire the Mac auto-reconcile bridge into the running app.**
> `farm.ReconcileSidecars` EXISTS but only as the `jmfarm -reconcile` one-shot CLI
> (`cmd/jmfarm/main.go:~295`). The long-running juicemount app does **not** call it on a timer
> or on a `/derivatives` miss. Without this bridge running IN the app, watched→processed work
> lands on the volume but **never surfaces to OpenLoupe automatically** — the watcher's entire
> payoff is invisible. JM15_DESIGN flags this as "the one remaining piece." Wire it as (a) a
> low-frequency background sweep of `<mount>/.juicemount/derivatives/` and (b) stat-on-miss at
> `/derivatives?inode=N`. **NOT built.**

| Phase | Trigger | What it adds | Key new pieces |
|---|---|---|---|
| **1 — Poll-sweep** *(cheapest / safest)* | Scheduled cron sweep enumerates opted-in dirs, dedups against coverage, enqueues the gaps (Jellyfin-style "scan now + scheduled"). | A reconcile net + scheduled coverage backfill. | **NEW:** `.juicefarm` opt-in marker; dedup/skip-already-done; a **`farm-sweep` schedule ACTION** in the manager cron engine (`internal/manager/schedules.go` today fires only **backup** `Submit/rsync` tuples — needs a new action branch, NOT a drop-in reuse). |
| **2 — Offload-push** *(HIGHEST VALUE)* | Hang an `Enqueue` off `Drainer.SetOnDrainComplete` (alongside `onSpoolDrained`): a SHA-verified drained file under an opted-in dir → debounced, dir-coalesced enqueue. | The card→auto-process magic moment. | **NEW:** opt-in gate (shared w/ P1); dedup (shared); a debounce/dir-coalesce buffer. **BUILT & reused:** the drain callback (`drainer.go:~212`, wired `handler.go:~468`); drain SHA/settle/quarantine completion semantics. |
| **3 — Core-push** *(most general)* | Any backend-confirmed write under an opted-in dir → debounce → folder-scoped enqueue. Covers non-card ingest (Finder copy, NLE export, network drop). | Broadest coverage (catches Finder drops that bypass any producer). | **NEW:** a settle/debounce gate for **direct** FUSE writes (`onSpoolDrained` only covers spool-routed writes; direct writes need an equivalent settle gate — partial). Most surface area, most footguns (proxies-of-proxies, relink churn, partial dirs). |

**What already exists (reused, low risk):** the P4 queue `farmqueue.Enqueue` + `jmfarm -queue`
standing worker (BUILT, smoke-verified, needs production rollout); `Drainer.SetOnDrainComplete`
(BUILT, wired); drain SHA/settle/quarantine semantics (BUILT); `farm.WriteManifestSidecar` +
the `since=` feed (BUILT); the manager already holds a `*farmqueue.Client` in-process and the
NFS handler holds the drainer.

**Biggest unlock = Phase 2.** The single hardest problem a generic file-watcher faces — *"is
this file actually finished, or am I about to process a torn write?"* — is **already solved at
the `SetOnDrainComplete` callback.** The watcher inherits bulletproof completion semantics for
free and just adds an opt-in check + a debounced enqueue. Card-in → proxies/transcripts
auto-generated on the NAS → shows up in OpenLoupe, zero polling, zero torn-file risk. **Its
payoff is gated entirely on Phase 0** — without the in-app reconcile bridge, the generated
derivatives never reach OpenLoupe.

---

## Blocking dependencies (shared)
1. **Mac auto-reconcile bridge** wired INTO the running app (Phase 0). Exists only as the
   `jmfarm -reconcile` CLI. Without it, watched→processed work is invisible to OpenLoupe.
2. **`.juicefarm` opt-in marker** + whitelistable subfolders (Phase 5, NOT built). Load-bearing
   default-deny gate; no watcher flavor is safe without it. **Plus** the worker-side re-check in
   `collectTargets`/`runJob` (today a blind `filepath.Walk`).
3. **Dedup / skip-already-done** keyed on `(inode, kind, source_hash)` (Phase 4, NOT built).
   Makes scheduled/auto sweeps affordable and makes overwrite-relink re-derive correctly.
4. **Marker-resolution cache** (marker-root → policy, invalidated on `.juicefarm` mtime or a
   coarse TTL). A card dump fires thousands of `create` events; per-event upward-stat-walks would
   hammer the FUSE/NFS metadata path — the exact "never FUSE-syscall per RPC" hot-path lesson.
   **Resolve once per directory, not once per file.**

---

## Risks + open questions (Leland's calls)
- **Marker semantics — `.juicefarm` (on-volume) vs manager-stored whitelist?** The locked
  decision is opt-IN `.juicefarm`, but the only Phase-5 item actually stubbed is the **inverse**
  `.nojuicefarm` opt-OUT + a manager `-exclude`/`JM_FARM_EXCLUDE` glob set. On-volume marker works
  from Finder and survives the worker having no DB; a manager-stored whitelist is central but the
  worker/OpenLoupe must query the manager (a network resolution path the worker lacks today). The
  upward-walk resolver assumes an **on-volume marker** — confirm that's canonical.
- **`.juicefarm` content schema is unspecified.** Empty-marker = opt-in-whole-subtree, vs a parsed
  body carrying the subfolder whitelist/exclude globs + per-folder proxy skip-threshold override.
  Producer-side and worker-side resolvers must parse it **identically** — define the schema in the
  contract repo before either side reads it.
- **Worker re-check a hard prerequisite for direct-Redis enqueue?** (Recommended: **yes** — until
  the worker independently re-resolves the marker, the queue is the only gate and a direct-Redis
  producer can enqueue any path.)
- **Settle/debounce window** after `drain_done` before enqueue, and how to coalesce a card-dump's
  thousands of records into a few scoped jobs without thrashing the marker resolver per-file *or*
  waiting so long the farm feels idle. Ties to the nav-sluggishness/flap-debounce discipline.
- **Interaction with offload backpressure** — should the watcher hold enqueues while the spool is
  still draining (don't compete for 10GbE/MinIO during ingest) and resume on a quiescent-spool
  signal?
- **Compressed/NLE-proxy avoidance** is structural (marker scoping = a watcher never looks inside
  NLE proxy folders) reinforced by the existing `mediaExts` allowlist (`cmd/jmfarm/main.go` —
  excludes still-image/RAW `.jpg/.cr3/.nef/.arw/.dng/.heic`, so a RAW-stills SD dump yields zero
  proxy targets even inside an opted-in folder) and the `JM_FARM_PROXY_MIN_SAVING` skip-threshold
  (Phase 5, needs the Phase-3 estimator to calibrate before it's trusted). The watcher reuses
  these; it does not re-implement them.

---

## Cross-links
- [`FARM_TAB_SPRINT.md`](FARM_TAB_SPRINT.md) — Phase 4 (queue trigger, BUILT) + Phase 5
  (scope governance, the `.juicefarm` opt-in, and the Watch-folder line item this doc expands).
- [`FARM_QUEUE_PROTOCOL.md`](../../../../juicemount-contract/FARM_QUEUE_PROTOCOL.md) — the P4
  queue wire contract the watcher enqueues to (the same `farmqueue.Enqueue`).
- [`FARM_CONTROL_SURFACE.md`](../../../../juicemount-contract/FARM_CONTROL_SURFACE.md) — the
  shared producer/worker model; the watcher is a third producer alongside Manager + OpenLoupe.
- `JM15_DESIGN.md` — the sidecar→reconcile→`/derivatives`→`since=` result loop (Phase 0 wires
  the missing Mac-side reconcile into the app).
