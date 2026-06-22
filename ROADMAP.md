# JuiceMount Roadmap

**Goal:** Native macOS menu bar app for video editors. Self-hosted JuiceFS-backed shared storage that feels like a local SSD to Premiere, Resolve, FCPX, and Finder. Cellular-capable via offline pinning.

**Last updated:** 2026-05-28
**Active branch:** `production-hardening` (mirrored to `main`)

---

## Current state (2026-05-08)

What ships today (`production-hardening` branch, 8 commits ahead of `prototype/offline-pin`):

- Native SwiftUI menu-bar app with status icon, popover, search window, preferences window
- Go c-archive backend in-process (no IPC) via `bridge/cbridge.go`
- Full read path: memory buffer → SSD cache reader → JuiceFS FUSE LRU → S3 backend
- NFS v3 server at `127.0.0.1:11049`, exposed via `/Volumes/zpool` on the user's Mac
- 147K-entry SQLite metadata cache with FTS5 trigram search (~29 ms typical)
- Auto-mount JuiceFS FUSE with `--cache-size` auto-expanded to 85% of disk and `--free-space-ratio 0.01`
- Auto-reclaim of APFS purgeable space (Time Machine local snapshots) at mount time
- HTTP control plane on `127.0.0.1:11050`: `/metrics`, `/cache-status`, `/pin`, `/unpin`, `/offline`, `/reclaim`, `/verify-pins`
- Offline pinning with open-time + read-time gates, pinned reads served from local cache at 200+ MB/s
- Menu bar Pin Folder button, macOS Services right-click integration ("JuiceMount: Pin for Offline")
- Sync Now triggers metadata reconciliation AND pin coverage verify-and-repair
- Disk-pressure banner in popover with three real states (free < 1%, free < 3%, pinned > total disk)
- Structured JSON logging at `~/Library/Logs/JuiceMount/juicemount.log` with size rotation (16 MB × 5)
- JuiceFS daemon log auto-tailed into our log with WARNING aggregation
- Cold-start retry: bounded exponential-backoff Redis connect (1+2+4+8+16 s) for wifi/cell handoffs at launch

What's verified live:

- Pinned offline read of a 350 MB media file: 215+ MB/s sustained
- Random-seek reads on cached media: 16–481 MB/s depending on hit rate
- 200 MiB sequential read on a fully-cached file: 431 MB/s, only 4.6 MB of network traffic
- Unpinned offline read fail-fast: 4–67 ms (was 14 s before open-time gate)
- 0 RPC errors over 20 s of active Resolve playback during regression session

---

## Phase 1 — Stability & polish ✅ complete

### 1.1 SQLite single-writer goroutine ✅
- `writeMu` mutex serializes SQLite writes; SQLITE_BUSY retry logic removed.

### 1.2 Incremental Redis sync ✅
- BulkClearLocalOnly only processes `local_only=1` entries.
- Sync time: 6 s → 2.6 s steady-state, 0 upserts when no changes.

### 1.3 Stale entry detection ✅
- `Stat()` verifies non-directory cache hits against FUSE.
- `OpenFile()` purges stale entries on FUSE ENOENT.

### 1.4 Codesign the binary ✅
- `scripts/build-cli.sh` builds + ad-hoc-signs with `com.apple.security.network.{client,server}` entitlements.
- Eliminates the `ssh localhost` workaround for 10GbE access.

### 1.5 Test infrastructure ⚠️ partial
- 28/28 metadata unit tests pass including search.
- Real Redis sync tests (131 K entries) pass.
- TODO: Finder simulation tests (LOOKUP → GETATTR → READDIR → READ sequences); FUSE crash/remount integration tests; pin/offline path tests in `test/` (unit-level coverage exists in `nfs/` and `internal/cache/pin/`).

### 1.6 FTS5 search ✅
- SQLite FTS5 virtual table with trigram tokenizer.
- Manual rebuild after BulkInsert (no per-row trigger overhead).
- ~29 ms search across 100 K entries.

---

## Phase 2 — Swift menu-bar app ✅ shipped 2026-05-07

### 2.1 Architecture: Swift shell + Go core
```
+---------------------------+
|  Menu Bar App (Swift)     |
|  - SwiftUI popover        |
|  - Search window          |
|  - Preferences window     |
|  - Login item (LaunchAgent)|
+------------+--------------+
             |
         C bridge (cbridge.go)
         via c-archive .a library
             |
+------------+--------------+
|  JuiceMount Go Core       |
|  (NFS server, FUSE        |
|   manager, pin store,     |
|   prefetcher, control     |
|   plane HTTP)             |
+---------------------------+
```

### 2.2 Menu bar icon states
| State | Icon | Meaning |
|-------|------|---------|
| Connected | Green dot | All healthy, volume mounted |
| Syncing | Spinning arrows | Metadata sync in progress |
| Degraded | Yellow dot | Redis or MinIO unreachable, serving from cache |
| Disconnected | Red dot | FUSE down or NFS unmounted |
| Idle | Gray dot | Server stopped |

### 2.3 Popover sections (current)
- Header: status, volume info, last sync, search button
- Health: Redis / MinIO / FUSE individual indicators
- Cache section:
  - Offline mode toggle
  - Pin Folder button
  - Aggregate cache counts (ready / pending / failed / total bytes / cached bytes)
  - Per-pinned-root status list with progress
  - Live prefetch row (current file + bytes)
  - Disk-space row (free GB · reclaimable GB) with Reclaim button
  - Pressure banner (3 states)
- Action buttons: Start / Stop / Sync Now / Preferences / Quit

### 2.5 LaunchAgent ✅
- `~/Library/LaunchAgents/com.juicemount.agent.plist`
- Starts on login, communicates with Go core via the C bridge (in-process, not IPC).

---

## Phase 3 — Production hardening ✅ shipped 2026-05-08

### 3.1 Observability ✅
- Structured JSON logging via `internal/jmlog` (slog-based).
- Per-RPC-type latency histograms (GETATTR, LOOKUP, READ, WRITE, READDIR, READDIRPLUS, ACCESS, FSSTAT, SETATTR).
- Metrics exposed at `127.0.0.1:11050/metrics` (Prometheus-style JSON).
- File logs at `~/Library/Logs/JuiceMount/juicemount.log`, 16 MB × 5 generations rotation.
- JuiceFS daemon log auto-tailed and promoted into our log.

### 3.2 Cache resilience ✅
- `--cache-size` auto-expansion to 85% of disk.
- `--free-space-ratio 0.01` (was hostile 0.1 default).
- Up-front free-space-volume health check.
- APFS purgeable-space auto-reclaim before mount.
- Pin-coverage verify on Sync.

### 3.3 Bandwidth-aware mode ✅ (offline form)
- Offline pinning + open-time / read-time gates fail fast on un-pinned reads.
- Operates as the bandwidth-aware fallback for cellular: user pre-caches what they need, toggles offline, reads succeed locally, anything else fails in <100 ms.
- Future: automatic detection (not yet — currently user-toggled).

### 3.4 Connection resilience ✅
- `connectRedisWithRetry` exponential backoff (1+2+4+8+16 s) at startup.
- `health.FUSEManager.StartMonitor` auto-remounts FUSE on death.
- Existing reconcileLoop reconnects Redis on signal.

### 3.5 Error recovery ✅
- Stale FUSE: kill juicefs process clears kernel mount; FUSEManager handles this on Stop / re-Start.
- Redis unreachable >5 min: surfaced via popover health row + structured-log warnings.
- Verify-and-repair recovery for files marked Failed during a transient FUSE outage.

---

## Write spool (Option 2) — ✅ shipped 2026-05-28 (opt-in, default off)

A foundational write-path change landed after Phase 3: a JuiceMount-owned **write spool on local SSD, interposed between the NFS handler and JuiceFS's raw staging**. Writes ack the moment the data is durable on the user's local SSD (`fsync()`), and a background drainer copies into JuiceFS at MinIO's pace. This is the Dropbox / LucidLink / Suite write model — a local-durability boundary with async upload — achieved without forking JuiceFS's `--buffer-size`-gated flow control.

**Why:** live 2 GB / 600-file Finder copies over Tailscale hit ~2-hour ETAs (~280 KB/s) because JuiceFS back-pressures on a RAM-tracked budget even with 700+ GB of disk headroom in `rawstaging/`. Bumping `--buffer-size` steals RAM from DaVinci/Premiere project caches. The spool decouples Finder's "write complete" from the MinIO upload entirely.

**Shipped — all 8 slices CI-green:**
- **A** Spool primitives + SQLite `spool_entries` index (capacity cap, streaming SHA-256, refcounted entries)
- **B** Drainer goroutine (single dispatcher + bounded worker pool, exponential-backoff retry, SHA-mismatch quarantine)
- **C** Write-path integration (`O_CREATE` routes through `spoolWriteFile`; ack at local SSD speed)
- **D** Read-path 3-tier lookup (`spoolReadFile` + Stat/Lstat shadow; QA-35 perf-gated, empty-spool lookup ~8 ns)
- **E** Runtime wiring + `GET /spool` control-plane endpoint (port 11050)
- **F** Crash-recovery boot scrubber (`RecoverOnBoot`: orphan cleanup, `writing`→failed, `draining`→ready, capacity re-accounting)
- **G** Integrity audit log (append-only `manifest.log`; drain-done + quarantine events with SHA-256)
- **H** WAN-mode polish (`JM_WAN_MODE` / `JM_MAX_UPLOADS` raise JuiceFS `--max-uploads` 20 → 64)

**Integrity discipline:** SHA-256 computed streaming on write, re-verified when the drainer reads the spool file, and re-verified at-rest through the FUSE mount after copy. Mismatch → quarantine (file moved aside, never deleted), surfaced via the manifest log.

**Rollout:** opt-in, default off. In the app the switch is **Preferences → Cache & Storage → Enable write spool** (the `JM_SPOOL_ENABLE` env var only works for the standalone `jm5` CLI, not the embedded app). Disabled = the original direct-to-FUSE `writeFile` path, unchanged. Flip to default-on after a clean soak.

**Update (2026-06-10, launch-hardening Phases 2–3b):** the previously deferred UI surfaces shipped — Swift menu-bar "Pending uploads" popover section with stalled/failed badges + Retry/Recover actions (`/spool-recover`), the icon upload badge, graceful-quit drain guard, and the Preferences spool controls. Still open: Manager web-UI tile and the 24-hour live soak test. The `/spool` JSON data contract is stable.

Full design + slice-by-slice plan: `docs/ROADMAP/option-2-spool.md`. Architecture: `ARCHITECTURE_juicemount.md` § 15.

---

## Phase 4 — Workflow features (next)

This is what turns the production-hardened tool into the "winning product" tracked in `VISION/`. Numbered against `VISION/feature-roadmap-ranked.md`.

### 4.1 Codec-aware Quick Look proxies (R3D / ARRI / BRAW / ProRes RAW) — score 14
**Status:** Prototype 01 in `prototype/codec-aware-quicklook` branch. Architecture proven, not feature-complete. **Estimated 2–3 weeks** to production-ready.

The wedge: macOS Quick Look (spacebar in Finder) on Resolve raw codecs is broken everywhere else. We can transcode-on-demand into Quick Look's preview cache directory and make spacebar Just Work.

### 4.2 Content-hash backup verification — score 13
**Status:** Prototype 02 in `prototype/backup-verification` branch. Working core engine, no UI shell yet. **Estimated 3–4 weeks** including the menu-bar Backups tab.

The wedge: every editor has a Toy Story 2 trauma. Show traffic-light status on every project: green (3+ verified copies), yellow (1–2), red (single source of truth). One click to see exactly which files are at risk.

### 4.3 Bandwidth-aware streaming fallback — score 12
**Status:** Manual offline-mode toggle is shipped (this branch). Auto-detect mode is next.

Detect cell vs. wifi vs. 10 GbE; auto-flip to offline mode on cellular handoff; auto-flip back when wifi quality is sufficient.

### 4.4 Project version history / snapshot layer — score 10
**Status:** Not started. JuiceFS supports snapshots natively; this is mostly a UI surface.

### 4.5 NLE bin sharing / cooperative locking — score 10
**Status:** Not started. Requires per-project lock-file convention + UI surface in popover.

### 4.6 Pre-cache heuristic — score 9
**Status:** Not started. Beyond manual pinning, watch active NLE bin to prefetch on user behavior signals.

### 4.7+ Hosted backend, Windows client, Linux client, AI search, NLE panels, mobile

See `VISION/feature-roadmap-ranked.md` for full scoring and rationale.

---

## Pre-launch UX & reliability needs (from field testing, 2026-06-16)

Surfaced while hammer-testing the mount across cellular / wifi / 10GbE. These are polish/
trust items, not big features — but they shape the "feels trustworthy" first impression.

### 4.8 "Clear failed files" action
**Status:** Not started. Small.
The drainer already auto-RetryFailed on reconnect (#17), but a file that's *permanently*
failed (e.g. its source was deleted, or a quarantine) lingers in `/spool` `failed_files`
forever with no user way to dismiss it. Add a manual **Clear failed files** button (menu-bar /
Manager) backed by a control-plane action (e.g. `POST /spool/clear-failed`, sibling to the
existing retry path) so the count can be acknowledged/reset. Must distinguish "retry" (try the
drain again) from "clear" (give up + remove from the failed set) and never silently drop
un-drained user data without confirmation.

### 4.9 Confirm cache/pins survive restart without a full re-warm
**Status:** Under code dissection (workflow `wf_ecd85331-f62`, 2026-06-16).
Verify — at the code level, not just empirically — that a system/app restart does NOT force a
full re-download of the JuiceFS block cache or pinned content. Empirically the on-disk cache
(`~/.juicefs/cache`) survived a recent reboot (49 GB still populated), but we need certainty on
(a) no startup wipe path and (b) the pin **prefetcher** skipping already-cached blocks rather
than re-reading every pinned file on boot. (Separate concern already known: a big unrelated
ingest can LRU-evict a user's working set — argues for pin cache-priority, see Deferred.)

### 4.10 Background-operation activity surface ("why is Finder slow right now")
**Status:** Not started. Extends #37 (post-remount "rebuilding index" indicator).
When reconcile / drain / pin-prefetch / warmup are running, Finder can feel sluggish and the
user has no idea why. Surface a plain-language **activity indicator** (menu-bar + Manager):
e.g. "Rebuilding index 62%", "Uploading 412 files (38 GB) to backend", "Warming pinned project
(120/180 GB)", "Catching up after reconnect" — so a slow moment reads as *a known background
task in progress*, not *the product is broken*. Include an ETA/throughput where cheap.
**Status: SHIPPED 2026-06-17** (4.8 clear-failed + 4.10 /activity backend + popover UI).

---

## Release punch list (2026-06-17, blocking public launch)

Working through as a loop. Code bugs first, then features/planning, then UI/audits.

Progress (2026-06-22): R-1, R-2, R-9 **SHIPPED** (built, deployed, backend-validated — verdict
math confirmed against live REEL_0065 pins). R-3 noted, R-5 ideated. **#43** (offline gate trusted
the stale pin "Ready" flag → tarpit/empty offline read; the *real* root cause of the original
"offline broke" report — R-1 proved it was never a capacity problem) coded + committed, awaiting a
deploy + real offline validation. Remaining: R-4, R-6, R-7, R-8.

### R-1 Pinned dir larger than disk → prefetch loops forever — ✅ SHIPPED 2026-06-22
Pinning a directory whose bytes exceed the usable cache capacity never converges: JuiceFS
LRU-evicts as the prefetcher warms, so files never all stay Ready and the re-warm loop runs
forever. FIX: at pin time compute pinned-bytes vs available cache capacity; if it can't fit,
surface a clear "pinned set (X) exceeds available cache (Y) — free space or pin less" state and
stop the infinite re-warm. (Root-caused 2026-06-17: 180 GB pinned, 139 GB free → never converges.)

### R-2 Pinning a folder doesn't take effect on first attempt / tens-of-seconds delay — ✅ SHIPPED 2026-06-22
After selecting a dir in the Finder pin picker and clicking Pin, nothing changes in the pinned
UI for tens of seconds. Likely the PullPending 5s poll + slow UI refresh. FIX: signal the
prefetcher immediately on pin (don't wait for the poll), and refresh the pin UI promptly so the
user sees "Pending/Warming" instantly.

### R-3 (QA note only) Removing files from a pinned dir shows them as "failed to download/pin"
When files are deleted from a pinned directory, that count surfaces as failed pins. User says
no guard needed yet — **note for QA** so it isn't mistaken for a real failure.

### R-4 Start-while-offline
Boot the app + serve the NFS share with ZERO network / no server connection, exposing cached
content in its last-known state. Needs the startup path to not hard-depend on a reachable
backend, plus a clear "started offline — showing cached state" modal/banner.

### R-5 Multi-user collision & sync-back (ideation)
Plan/ideate concurrent multi-user access: what collision/sync semantics do we have today
(JuiceFS + Redis), what breaks with two writers, and the path to safe cooperative editing.

### R-6 Run the marketing website locally for feedback
Serve the website version(s) on localhost for another review pass.

### R-7 Manager / dashboard scope audit — Trash + Destinations inoperative
Audit JuiceMount Manager against what was scoped; Trash and Destinations pages reported inop.
(See tasks #31–34.)

### R-8 Menu-bar mount-state reporting edge cases — ✅ FIXED 2026-06-22 (awaiting deploy)
Internet disconnect/change makes the mount go offline → reconnect → responsive, but the menu
bar still reports DISCONNECTED. Audit the color/text state machine. (Task #30.) ROOT CAUSE (adversarial
multi-agent investigation): criterion asymmetry — `.disconnected` is ENTERED on FUSE-only, but both
recovery backstops gated on `/health` Overall (which ANDs in NFS); the loopback NFS mount recovers
~180s slower than the backend, so the red latch stuck while reads worked. FIX: HealthProbe.coreHealthy
(fuse/redis/minio, ignoring NFS) gates both backstops; glanceState reads the reliable in-process
cacheStatus.offline_mode (not the HTTP /offline fetch that's kept stale on nil) so the blue "Offline
files" icon also clears promptly. NFS-mount health stays surfaced via volumeMounted (amber, "Mount Now").

### R-9 Settings clarity + design sprint — ✅ SHIPPED 2026-06-22
Settings text fields aren't obviously editable (users miss that they can change the volume
name); move the volume-name field down to the "mounts at" row; run a design sprint to make the
whole settings pane cleaner/clearer. DONE: composed `/Volumes/[name]` Mounts-at row with a bordered
editable volume-name field, custom mount point moved to a disclosure, `.roundedBorder` on all
address fields.

### #43 Offline gate trusted the stale pin "Ready" flag — ✅ FIXED 2026-06-22 (awaiting deploy)
The read-time offline gate exempted pinned files, trusting the pin "Ready" flag as proof the bytes
were resident. After LRU eviction that flag is stale → offline reads tarpitted on an unreachable
backend GET / returned empties. The original "offline access to a pinned reel broke" report. FIX:
pinned files now use the same bounded-read-then-refuse path as un-pinned (4s bound vs 1.5s) — a
resident block is served, an evicted one refuses cleanly with ErrOfflineNotAvailable. Safe by
construction (never returns wrong bytes; offline-only cost).

- **NFS v4.1**: server-initiated callbacks (delegations) for instant invalidation. Major protocol change.
- **Redis Streams**: replace SUBSCRIBE + Lua SCAN with a Redis Stream for change tracking. Requires JuiceFS cooperation or a sidecar.
- **Notarization + DMG installer**: ad-hoc-signed dev build is sufficient for the user's own machine; required for distribution.
- **FinderSync extension** (`.appex`): would put the offline-pin badge directly on Finder icons. Deferred because macOS Services right-click already gives the same UX path with much less complexity (no separate bundle, no App Group entitlements).

---

## See also

- `ARCHITECTURE_juicemount.md` — system architecture, data flows, performance optimizations
- `MENU_BAR_APP.md` — current state of the SwiftUI app, popover layout, build instructions
- `CHANGELOG.md` — release notes; latest entry covers the production-hardening branch
- `VISION/` — strategic positioning, persona, competitive analysis, feature roadmap, prototype writeups
- `z-quarantine/README.md` — files set aside for review or removal
