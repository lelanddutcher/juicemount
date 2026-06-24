# Control-plane spec (contract v1)

JuiceMount exposes one loopback HTTP server, default **`http://127.0.0.1:11050`** (the `metricsAddr`
preference / `-metrics-addr` flag), **no auth** (loopback-trust). The NFS data volume is a separate port
(`11049`, the `nfsListenAddr`); it is not part of this contract.

Discover the control-plane base URL via `defaults read com.juicemount.app metricsAddr` (default
`127.0.0.1:11050`) — or, preferred, via `GET /whoami.control_plane` once that endpoint exists.

> **Trust + safety:** the control plane is unauthenticated loopback. OpenLoupe inherits that trust model
> (any local process can already poke JuiceMount); it does not widen it. **Every** call OpenLoupe makes —
> HTTP or DB — MUST run inside a deadline-bounded probe (OpenLoupe's `Deadline.bounded` /
> `AccessStore.resolveGrantedBounded`) so a wedged mount can never freeze the app, and a timeout fails closed
> to "unknown/offline" while *keeping* the asset (it may resolve later). See
> [`honest-state.md`](honest-state.md).

All shapes below are specified by the JSON Schemas in [`schema/`](schema/) and exemplified by
[`../fixtures/`](../fixtures/). Where v1 specifies a shape that differs from what JuiceMount6 ships **today**,
it is called out inline and tracked in [`../BACKLOG.md`](../BACKLOG.md).

---

## Endpoint summary

| Endpoint | Method | Exists on `origin/main`? | Schema | Consumer use |
|---|---|---|---|---|
| `/whoami` | GET | **NO — JM-1** | [`whoami.schema.json`](schema/whoami.schema.json) | identity, version, capabilities, paths |
| `/health` | GET | yes | [`health.schema.json`](schema/health.schema.json) | confirm a detected mount is live JuiceMount |
| `/residency?path=` | GET | **NO — JM-2** | [`residency.schema.json`](schema/residency.schema.json) | honest per-asset resident/streaming/offline badge |
| `/lookup?path=` | GET | **NO — JM-4** | [`lookup.schema.json`](schema/lookup.schema.json) | inode + nas_rel_path for durable identity |
| `/cache-status` | GET | yes¹ | [`cache-status.schema.json`](schema/cache-status.schema.json) | warming progress, aggregate residency, capacity |
| `/offline` | GET / POST | yes | [`offline-state.schema.json`](schema/offline-state.schema.json) | online/offline truth + toggle |
| `/spool` | GET | yes | [`spool-status.schema.json`](schema/spool-status.schema.json) | per-file upload (drain) state |
| `/activity` | GET | yes | [`activity.schema.json`](schema/activity.schema.json) | "what's happening now" inspector line (reconcile/drain/prefetch) |
| `/pin?path=` | POST | yes | [`pin-result.schema.json`](schema/pin-result.schema.json) | warm-before-scrub (Phase 2) |
| `/unpin?path=` | POST | yes | [`pin-result.schema.json`](schema/pin-result.schema.json) | release a warmed set |

¹ `/cache-status` exists but v1 specifies **normalized snake_case keys** for the inner aggregate/roots/live
structs — requires the JM-3 struct-tag change (see below). Its `capacity` + `scanning` keys are already
snake_case on main.

**Deployment skew:** the **GUI app** (cbridge) serves the full set above. The **`jm5` CLI** serves only
`/health /metrics /spool` (+ `/whoami` under this contract) — it does **not** serve `/cache-status /offline
/activity /pin`. Never assume an endpoint exists — feature-detect via `whoami.capabilities`. See
[`capabilities.md`](capabilities.md).

---

## New endpoints (JuiceMount must add)

### `GET /whoami` — JM-1 (high)

The keystone. Removes the `defaults read` dance, gives a real version + `contract_version`, and replaces
version-floor guessing with a capabilities list. Schema: [`whoami.schema.json`](schema/whoami.schema.json).
Fixture: [`../fixtures/whoami/gui.json`](../fixtures/whoami/gui.json),
[`../fixtures/whoami/cli.json`](../fixtures/whoami/cli.json).

```json
{
  "app": "JuiceMount",
  "version": "0.1.0",
  "contract_version": 1,
  "instance_id": "5C7E0E2A-…",
  "volume_name": "zpool",
  "mount_point": "/Volumes/zpool",
  "nas_root": "/Volumes/zpool",
  "control_plane": "http://127.0.0.1:11050",
  "metadata_db_path": "/Users/leland/Library/Application Support/JuiceMount/metadata.db",
  "deployment": "gui",
  "capabilities": ["health","whoami","residency","lookup","cache-status","offline","spool","pin","unpin","self-test","verify-pins","metrics"]
}
```

- `instance_id`: a **stable per-install UUID** JuiceMount mints once and persists (e.g. in Preferences).
  It is the `jm_instance` half of OpenLoupe's durable `(instance, inode)` identity — so an asset survives a
  remount and is shareable across machines that mount the same volume. See [`identity.md`](identity.md).
- `version`: the **public release version** string (currently `0.1.0` — the notarized release + only git
  tag). Source it from a single version-of-record (a Go const set by the release process); reconcile the
  stale Info.plist marketing string (`2.0.0`) down to match. Distinct from `contract_version`.
- `contract_version`: integer from this repo's `VERSION`.
- `nas_root`: today equal to `mount_point` (paths are mount-anchored); a distinct field so it can diverge.
- `capabilities`: the **only** correct way to feature-detect. **Derived, never hardcoded** (see
  [`capabilities.md`](capabilities.md) for the exact derivation rule + vocabulary). The GUI fixture lists the
  full set; the CLI (`jm5`) fixture lists `["health","spool","metrics","whoami"]` (jm5 also gains a `/whoami`
  handler under this contract, so `whoami` is necessarily present).

### `GET /residency?path=<abs>` — JM-2 (high)

The single endpoint that lets OpenLoupe paint an **honest** resident/streaming badge. Today residency is only
*inferred* from `pin.db` aggregates — you cannot truthfully say "all bytes are on SSD right now" for an
arbitrary path. This endpoint answers it directly. Schema:
[`residency.schema.json`](schema/residency.schema.json). Fixtures:
[`../fixtures/residency/`](../fixtures/residency/).

```json
{
  "path": "/Volumes/zpool/Project_Foo/clip_031.mov",
  "exists": true,
  "inode": 1180417,
  "resident": true,
  "pinned": true,
  "bytes_cached": 880000000,
  "total": 880000000,
  "streaming": false,
  "upload_state": "done",
  "checked_at": 1750000000
}
```

- `resident` MUST mean **all bytes are cached on SSD right now**, NOT "is pinned".
  **Implementation rule (important):** JuiceMount has per-byte cache accounting **only** for files with a
  `pinned_files` row (`bytes_cached` lives solely in `pin.db`; the pin layer does not inspect JuiceFS's own
  cache). So for any path with **no `pinned_files` row**, JuiceMount MUST report
  `resident:false, streaming:true, bytes_cached:0` — the honest under-claim. `resident:true` is emitted
  **only** when a `pinned_files` row shows `bytes_cached >= total`. This keeps the badge truthful without the
  provider inventing data (and matches OpenLoupe's under-claim-never-over-claim discipline).
- `streaming` = `exists && !resident` (the file is reachable but served block-by-block from the backend).
- `upload_state`: mirror the file's `spool_entries.drain_state` if it is mid-upload, else `null`/`done`.
  Lets OpenLoupe show "uploading to NAS".
- `inode`: echo `entries.inode` so a single residency probe also yields the durable id.
- If `exists=false`: `resident/pinned/streaming=false`, `bytes_cached/total=0`, `inode` omitted.

### `GET /lookup?path=<abs>` — JM-4 (medium)

So OpenLoupe never has to open JuiceMount's SQLite for identity. Schema:
[`lookup.schema.json`](schema/lookup.schema.json). Fixture:
[`../fixtures/lookup/file.json`](../fixtures/lookup/file.json).

```json
{ "path": "/Volumes/zpool/Project_Foo/clip_031.mov", "exists": true, "inode": 1180417,
  "nas_rel_path": "Project_Foo/clip_031.mov", "is_dir": false, "size": 880000000, "mtime": 1700000000 }
```

- `nas_rel_path`: **defined by this contract** as `path` with the `mount_point` prefix removed (no column for
  it exists in JuiceMount today — it must be computed). Human-debuggable, survives a metadata rebuild.

---

## Existing endpoints (grounded in `origin/main`)

### `GET /health`

`{ "healthy": bool, "components": { "redis": "ok", "fuse": "ok", "nfs": "ok" }, "reason": "" }`
HTTP **503** when `healthy=false`, else 200. On main `components` is **always present** (normalized to `{}`,
not omitempty); `reason` is omitted when empty. Served by **both** the GUI and `jm5`. Use it to confirm a
mount detected by signature is actually live JuiceMount. Source: `internal/metrics/metrics.go:226` (struct),
`:435` (route), `:465` (handler). Schema: [`health.schema.json`](schema/health.schema.json).

### `POST /pin?path=<abs>` / `POST /unpin?path=<abs>`

`PinResult { "ok": bool, "files_pinned": int, "bytes_total": int64, "error"?: string, "scanning": bool }`
(`bridge/cbridge.go:1942`). Pins/prefetches a **whole** file or tree (no partial/range warm exists — see
JM-5). **`scanning` is always present** (deliberately not omitempty): a fresh `/pin` returns
`{ok:true, scanning:true, files_pinned:0, bytes_total:0}` and walks asynchronously — **real counts arrive via
`/cache-status`**, not the `/pin` response. On `/unpin`, `files_pinned` is the count **removed**,
`bytes_total` is 0, `scanning` is false. HTTP 400 `missing ?path` if absent.
**Warm policy:** OpenLoupe expresses *intent* via `/pin`; JuiceMount owns eviction. Never `/unpin` what the
user or JuiceMount pinned. Schema: [`pin-result.schema.json`](schema/pin-result.schema.json).

### `GET /cache-status`

`CacheStatus { aggregate, roots[], live, offline_mode, capacity, scanning[] }` (`bridge/cbridge.go:2046`).
Per-root + aggregate warm progress, the live prefetcher state, a `capacity` verdict, and any in-flight pin
scans. **v1 specifies snake_case keys** for `aggregate`/`roots`/`live` (`total_files`, `cached_bytes`,
`bytes_prefetched`, `current_file`, …) — see the JM-3 normalization note below. `capacity`
(`pin.CapacityVerdict`) and `scanning` (`pin.ScanningRoot[]`) are **already** snake_case on main and always
present (`scanning` normalized to `[]`). Schema: [`cache-status.schema.json`](schema/cache-status.schema.json).

### `GET /offline` (read) · `POST /offline?on=true|false` (toggle)

Read (no `?on`) → `OfflineState { offline, user_offline, auto_offline, reason?, since?, since_sec }`
(`internal/cache/pin/offline.go:127`). Toggle (`?on` present) → `{ "ok": true, "offline_mode": bool }`
(`NFSServerSetOffline`, `bridge/cbridge.go:2099`). Schema:
[`offline-state.schema.json`](schema/offline-state.schema.json).

### `GET /spool`

`SpoolStatusResponse { enabled, error?, pending_files, pending_bytes, in_progress, succeeded, failed,
quarantined, capacity_used, capacity_total, stalled_files, failed_files, oldest_pending_age_sec,
stall_waiters, offline, offline_buffer_full, entries[] }` where each
`SpoolEntryView { path, size, drain_state, drain_attempts, last_error?, updated_at_unix, age_sec, stalled }`
(`nfs/spool_status.go:17`). HTTP **503** with `enabled:false` when the spool is not enabled
(`JM_SPOOL_ENABLE=1`). `entries` is capped at 200, newest-first active rows + a short recently-done tail.
`drain_state ∈ {writing, ready, draining, done, failed}`. Backs the "uploading to NAS" badge. Schema:
[`spool-status.schema.json`](schema/spool-status.schema.json).

### `GET /activity`

`{ "busy": bool, "summary": string, "operations": [ {kind, active, detail, files?, bytes?} ] }`
(`handleActivityHTTP`, `bridge/cbridge.go:2256`; `activityOperation` struct `:2242`). `kind ∈ {reconcile,
drain, prefetch}`; absent subsystems are omitted. Always HTTP 200. This is the **cleanest single source** for
OpenLoupe's "what's happening now" inspector line (preferred over stitching `/cache-status.live` + `/spool`).
GUI-only (not served by `jm5`). Schema: [`activity.schema.json`](schema/activity.schema.json).

---

## JM-3 — `/cache-status` key normalization (required for conformance)

**Today:** the inner structs `pin.AggregateStats`, `pin.RootSummary`, `pin.LiveStats` carry **no json tags**,
so `/cache-status` marshals them with **capitalized Go field names**: `TotalFiles`, `ReadyFiles`,
`CachedBytes`, `BytesPrefetched`, `CurrentFile`, `Workers`, etc. Every other endpoint uses snake_case.

**v1 contract:** snake_case everywhere. JuiceMount adds `json:"total_files"` (etc.) tags to those three
structs and updates the one in-tree consumer (the `juicemount` CLI parser, `cmd/juicemount/main.go:173-215`)
to read the new keys. A few dozen lines; makes the whole API consistent. The fixtures in
[`../fixtures/cache-status/`](../fixtures/cache-status/) are the **target** (snake_case) form.

(If you would rather not touch the existing keys, the alternative is to specify the contract in capitalized
form — but since both apps are first-party and the CLI is the only other consumer, normalizing now is the
clean choice and is filed as JM-3 high.)

> **Consumer gate:** the `cache-status` capability token only asserts the *route exists* — pre-JM-3
> JuiceMount still emits **capitalized** keys. OpenLoupe must gate snake_case parsing on
> `whoami.contract_version >= 1` (i.e. JM-3 landed), **not** on the capability token alone.

---

## Grounding notes (re-verified against `origin/main`)

> **Provenance — read this.** v1 was first drafted from a `JuiceMount6` working tree that happened to be
> checked out on a divergent `production-hardening` branch (≈350 commits behind `main`). That first draft
> "corrected" the prose handoff doc on several points where the **doc was actually right about `main`** — so
> those corrections were themselves the drift. v1 has since been **re-grounded against `origin/main`**
> (`a0ffc6a`). The facts below are the corrected, main-accurate ground truth; every schema `source_ref` is a
> `main` line.

- **`/activity` exists** on main (`bridge/cbridge.go:768` → `handleActivityHTTP:2256`), returning exactly
  `{busy, summary, operations[]}`. It's a real, useful endpoint — see its entry above; the contract consumes
  it for the "what's happening now" line.
- **`PinResult` has a `scanning` field** (always present, not omitempty). A fresh `/pin` returns
  `scanning:true` with zero counts and walks async; counts land via `/cache-status`.
- **`/cache-status` carries `capacity` + `scanning`** top-level keys (both already snake_case), in addition
  to the capitalized `aggregate`/`roots`/`live` that JM-3 normalizes.
- **`/spool` carries extra fields** (`stalled_files`, `failed_files`, `oldest_pending_age_sec`,
  `stall_waiters`, `offline`, `offline_buffer_full`; entries add `age_sec`, `stalled`).
- **`/pin` `/unpin` `/offline` are POST** (the shipped `juicemount` CLI calls them with POST; Go handlers are
  method-agnostic, but the contract standardizes on POST for mutating/toggle).
- **`/health` lives in the metrics package** (`internal/metrics/metrics.go:434-435`), not `cbridge` — noted
  so nobody greps the wrong file. Served by **both** the GUI and `jm5`.
- **The cbridge control-plane route table is at `bridge/cbridge.go:733-789`** on main (the new `/whoami`,
  `/residency`, `/lookup` handlers register there).
- **No version field exists** in any response and there is no Go version constant — exactly why JM-1
  `/whoami` matters. (Info.plist marketing version is `2.0.0`; the public/notarized release tag is `v0.1.0`
  — see [`../HANDOFF_JUICEMOUNT.md`](../HANDOFF_JUICEMOUNT.md) for the version-of-record decision.)
- **No `nas_rel_path` column exists.** Paths are mount-anchored; `nas_rel_path` is *defined* by this contract
  as `path − mount_point` and must be computed.
- Detection signature `isOurNFSMount` (`bridge/cbridge.go:1803`) is source prefix `127.0.0.1` / `localhost`
  AND fstype contains `nfs`. See [`identity.md`](identity.md) for the probe.
