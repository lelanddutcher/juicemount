# JuiceMount6 Changelog

## 0.5.0 — release candidate — 2026-08-30

### App and Link

- The write spool is on by default, persists an explicit opt-out, protects
  acknowledged writes with local durability and SHA-256-verified drain, and
  exposes recovery controls for stalled or failed uploads.
- A late Finder continuation write now atomically reopens the same unclaimed
  spool image instead of waiting behind the drain queue and surfacing repeated
  retry errors. Rows already claimed by the drainer or partially streamed stay
  on the corruption-safe defer path.
- A verified bounded Finder-metadata image is retained across spool drain so an
  already-drained `._` sidecar or `.DS_Store` can be rewritten safely after an
  offline transition; ordinary partial file edits still fail closed.
- Large Finder directories keep each AppleDouble sidecar beside its principal
  at the final NFS protocol ordering layer with a linear merge, eliminating the
  redundant lookup storm and second full sort seen with 5,000-file folders.
  Bounded metadata mutations no longer share navigation's RPC admission pool,
  so a burst of slow sidecar removes cannot block READDIR/GETATTR. Late metadata
  updates on an already-unlinked sidecar retain handle semantics without
  resurrecting the deleted name or returning a stale-handle error. A directory
  removed while macOS still holds its NFS handle now does the same for late
  metadata requests, preventing a successful Finder tree deletion from ending
  in a stale-handle warning. Renames of new spool-only files no longer issue a
  remote FUSE rename solely to discover that the source is absent. The NFS
  response writer also flushes continuously refilled Finder traffic in bounded
  batches, preventing a ready navigation reply from waiting indefinitely for
  the response queue to become empty.
- JuiceMount Link persists pairing authorization, applies and tests remote
  backend routes, and fails closed when coordination or saved credentials are
  incomplete.
- Update checks are manual-only. JuiceMount makes no background Sparkle network
  request; choosing **Check for Updates…** starts the signed updater and contacts
  the documented GitHub appcast.

### JuiceFarm and Manager

- Automatic recursive discovery has a durable catch-up cursor, bounded server
  planning shards, renewable processing claims, measured GPU admission,
  HEVC-first proxy routing, explicit hardware H.264 and CPU H.264 fallback, and
  per-file incompatible-source isolation.
- Durable proxy commit receipts and codec-agnostic automatic freshness prevent
  retries or a returning HEVC worker from replacing valid proxies and amplifying
  JuiceFS trash. Storage permits and an atomic safety pause stop new render
  claims below physical-pool headroom.
- Proxy and Quick Look MP4 muxing now completes on node-local scratch before a
  single sequential publish into JuiceFS. This prevents FFmpeg append/seek and
  `+faststart` rewrites from creating tens of gigabytes of retained object slices
  for only a few gigabytes of visible output.
- Embedded audio cover artwork is excluded from motion-video classification, so
  audio-only assets cannot be routed into the MP4 proxy encoder.
- Farm mission control now shows every queue lane and each node's current
  `{job, kind, path, stage, percent}`; supports per-node pause, drain, resume and
  verified restart; returns a bounded 200-line runtime log; cooperatively
  cancels queued/running work; and requeues a failed job exactly once from its
  stored immutable payload while preserving the original terminal record.
- The TrueNAS release compose requires digest-pinned first-party artifacts, the
  existing Manager and MinIO credentials, and exact physical zpool capacity at
  render time. Installation guidance explicitly excludes the MinIO object
  dataset from recursive ZFS snapshots.

### Release integrity

- Go dependency and reachable-vulnerability gates include the `go-billy` path.
  Linux CI runs Redis-backed Farm durability tests; macOS CI builds and verifies
  a signed-equivalent app and rejects stale source identity.
- The Finder release battery scales drain deadlines by both bytes and object
  count and avoids `pipefail`/early-exit false negatives when reading live
  offline and keyspace-push status. Its directory and Stat latency probes now
  start their clocks inside the process performing the syscall, so process
  launch scheduling under a hot copy cannot masquerade as filesystem latency.
- Container builds embed and execute exact version/commit identity, publish SBOM
  and provenance, and production deployment references accepted manifest
  digests rather than mutable tags. Distribution Mac builds fail closed unless
  Developer ID signing, notarization, stapling, Sparkle signing, and a matching
  appcast all succeed.

### Explicitly gated from 0.5

- The incomplete Teams/seats authorization surface remains fully feature-gated.
  Camera catalog and public website work are outside this release.

## 0.4.0 — 2026-07-15 — Resilience & recovery, faster navigation on slow links

### Added
- **The app tells you when macOS is blocking it.** If macOS's Local Network
  permission is denied, JuiceMount now detects it and says so with a clear,
  actionable alert — instead of silently appearing "offline" while the network
  is actually fine.
- **"Why is it slow?" self-diagnosis.** A new diagnostic surfaces *why* the mount
  feels slow at a given moment (warming up, on a metered link, backend
  unreachable) rather than leaving you guessing.
- **QuickLook thumbnails in Finder.** When the server has pre-rendered a poster
  for a clip, Finder shows it from a local cache instead of streaming and
  decoding the remote video just to draw an icon; on any miss it falls back to
  Apple's generator. (Thumbnail generation is a server-side feature.)
- **Instant folder sizes.** Folder sizes are aggregated incrementally, so a
  directory reports its total size immediately instead of after a full walk.

### Fixed
- **Automatic recovery when the mount drops.** If the volume disappears while the
  backend is still alive, the app now remounts it automatically instead of
  getting stuck showing the drive as unavailable.
- **Survives a backend restart mid-operation.** In-flight reads and writes now
  park and retry across a brief backend (Redis) restart instead of failing the
  copy or read outright.
- **Moving a folder keeps its contents.** Renaming or moving a folder now carries
  all of its children with it — no more items appearing to vanish after a move.
- **Copying bundles and packages is reliable.** Fixed the remaining cases where
  copying `.app`/`.framework`/NLE bundles could fail with a stale-file error,
  by keeping symlink identity and inode numbers stable.
- **Faster, more reliable startup.** Removed a startup deadlock and added a
  boot fast-path that skips redundant index work when the local mirror is
  already fresh — the mount becomes responsive sooner.
- **No more repeated disconnects on machines with a large local cache.** The
  mount health-check now allows enough time for a big on-disk cache to warm up
  at launch, so the app no longer mistakes a busy-but-healthy mount for a failed
  one and cycles it — which previously showed up as the drive dropping and
  reconnecting every couple of minutes.
- **Interrupted downloads are flagged, not served as good.** A file left
  zero-tailed by an interrupted download (e.g. a browser download that dropped)
  is detected at finalize so it can't masquerade as complete.

### Changed
- **Much faster navigation on cellular and other slow links.** Directory listings
  and first-visit folder opens are dramatically quicker over metered/high-latency
  connections: AppleDouble sidecar bodies are served from RAM (and persist across
  restarts), reads use a two-lane gate so a big transfer can't starve browsing,
  drains are serialized so uploads don't saturate the uplink, and the metadata
  push channel self-heals so the app no longer periodically drops into a long
  "rebuilding index" over a slow link.
- **Newly created content shows up in seconds, not up to an hour.** Tightened the
  directory attribute cache so files created on another machine become visible
  almost immediately.
- **A warm-up phase card with progress.** After launch or a reconnect, the popover
  now shows the warm-up phase and its progress, with a note that the mount may be
  slower until it reaches steady state.
- **Cache bar shows how much is pinned.** The menu-bar cache bar now overlays the
  portion of your cache budget consumed by pinned items.
- **Changing the SSD cache size prompts a restart.** The cache size is set when the
  daemon launches, so the app now offers Restart Now / Later when you change it —
  rather than silently not taking effect until the next launch.
- **Pinned items can't be evicted.** Hardened the guarantee that pinned files stay
  resident even under cache pressure, with eviction-triggered repair.

### Server & Manager (control-plane container)
- **Security — trash-restore and scheduled backups are now confined to the volume.**
  The manager runs as root on the NAS; two path-confinement holes are closed:
  trash-restore could move a caller-controlled file to any host path (a root
  code-execution path), and a scheduled backup could read outside its permitted
  source roots (arbitrary-file exfiltration). Both are now gated exactly like
  interactive migrations.
- **Trash retention is readable and settable in embedded mode.** Embedded
  deployments now surface and change the JuiceFS `--trash-days` setting (set
  `JM_OVERVIEW_META`); the UI distinguishes "disabled (0)" from "unknown".
- **Scheduled maintenance (GC / FSCK / compact).** Each maintenance lever can now
  run on a schedule — off by default, each pre-filled with a conservative
  recommended cadence and inline guidance. JuiceFS already auto-handles the
  routine cleanup (trash expiry + slice compaction), so these are opt-in hygiene.
- **Farm tab — "Clear finished" + running sweep to the top.** Clear terminal jobs
  from the recent-jobs list; the currently-running sweep floats to the top.
- **Under the hood.** Fixed a jobs-API data race and an unbounded Redis
  job-index leak.

## 0.3.0 — 2026-07-06 — Reliability: no more "connection interrupted", instant saves & exports

### Fixed
- **"The server connection was interrupted" is gone.** Finder no longer flashes a
  spurious connection-interrupted error during copies, at the moment a Premiere
  export finishes, or when working with unicode / emoji filenames. Root cause: the
  server could briefly report a just-written file as unavailable while it was still
  settling — it now stays consistently visible.
- **Saving and exporting feel instant.** Closing a file — a Premiere project save,
  a video export — now finalizes in a few seconds instead of up to ~30, and no
  longer stalls on "the last 1%". Large exports finalize as soon as the app signals
  it's done, not after a fixed wait.
- **No more black frames for images in Premiere.** A stale in-memory copy could
  serve a truncated image (fixed only by restarting the app); images now read from
  the always-fresh path and stay correct.
- **Copies no longer stall ~a minute per file.** A Finder copy of files carrying
  Finder tags or a quarantine flag no longer hangs ~73 s per file.
- **No data-integrity issues under heavy concurrent copies.** Hardened the write
  spool so a continuation write during a drain can never truncate an already-written
  file, and out-of-order writes are tracked correctly.

### Changed (app)
- **Clear Cache updates the number immediately.** The menu-bar "X cached" figure now
  drops as soon as the cache is cleared.
- **Clearer Cache & Storage settings.** Numeric fields use the standard bordered
  input styling so it's obvious where to type.
- **More complete diagnostics export.** The exported bundle now includes the full
  rotated logs (write-spool + NFS activity) and a live spool snapshot.
- **Removed the obsolete "Reconcile interval" setting.** Metadata now syncs in real
  time via change notifications; there's no interval to configure.

## 0.2.0 — 2026-06-29 — Bundles & packages, offline hardening, auto-updates, server-side generation

### Added
- **Automatic updates (Sparkle).** JuiceMount now checks for and installs signed
  updates in the background (Developer-ID + notarized builds; EdDSA-signed appcast).
- **App bundles & packages copy correctly.** `.app`, `.framework`, `.fcpbundle`, and
  NLE project folders — which contain internal symlinks — now copy without the
  "you don't have permission to access some of the items" error. NFS SYMLINK /
  READLINK are implemented.
- **JuiceFarm (advanced, optional).** Server-side generation of playback proxies,
  poster frames, filmstrips, audio waveforms, technical metadata, and transcripts, so
  clients don't transcode on-device. See [`docs/JUICEFARM.md`](docs/JUICEFARM.md).
- **OpenLoupe integration.** A versioned wire contract for portable derivatives and
  human assertions (ratings / picks / person names), shared with the OpenLoupe DAM.

### Changed
- **Faster, less-disruptive metadata sync.** The index updates from live Redis
  keyspace notifications (push) instead of a constant full re-scan, so a background
  index refresh no longer stalls Finder; the full scan is demoted to a rare backstop.
- **Snappier folder opening under load.** Opening a directory while a large copy
  drains returns instantly from the local index instead of showing a spinner.
- **Correct cache-size reporting.** "Cache used" reflects the JuiceFS block cache,
  not just pinned files.

### Fixed
- **Offline bundle copies.** Copying a package with internal symlinks while offline
  no longer fails — the symlinks are deferred and materialized on reconnect.
- **No spurious "connection interrupted" when toggling offline mid-copy.** The health
  watchdog is now offline-aware and won't remount a busy-but-healthy mount.
- **AppleDouble (`._`) sidecars** no longer trip a transient stale-handle warning
  during a metadata sync.
- Additional data-integrity, concurrency, and offline-ingest hardening.

## 2026-06-10 — Launch hardening, Phase 4 (OSS publication hygiene)

- **Apache-2.0.** `LICENSE` + `NOTICE` added (JuiceFS and go-nfs/go-nfs-client
  attribution); README license section matches.
- **Publication README** replaces the developer-oriented one (which moved to
  `docs/dev-setup.md`): verified feature/requirement claims, honest
  comparisons, author-attributed performance numbers.
- **`scripts/uninstall.sh` (new):** stops the app, unmounts NFS/FUSE with a
  refuses-to-rm-through-a-live-mount guard, inventories everything with sizes
  before one confirmation, requires a separate explicit confirmation (or
  `--delete-pending-uploads`) before touching a spool with un-uploaded files,
  shows the JuiceFS chunk-cache size before offering to remove it, and
  supports `--dry-run`/`--yes`. Refuses to run as root.
- **LaunchAgent fixed:** `scripts/com.juicemount.agent.plist` now launches via
  `open -a` (LaunchServices single-instance — can no longer double-launch
  against the in-app "Start at login" toggle) and logs to
  `~/Library/Logs/JuiceMount/agent.log` instead of world-readable /tmp.
- Personal LAN IPs/paths scrubbed from CLI help strings, QA scripts (NAS IP is
  now the `JM_QA_NAS_IP` env var), and example text; docs drift corrected
  (spool opt-in is the Preferences toggle, sudoers examples include
  `/sbin/umount`, Gatekeeper guidance modernized, MENU_BAR_APP reflects the
  current UI, OPEN_BUGS closed out against the hardening commits).

## 2026-06-08/10 — Launch hardening, Phases 1–3b (write-path correctness, spool durability + UX, identity/onboarding, preferences redesign)

Four serialized hardening batches preparing the open-source launch
(ledger: `docs/LAUNCH_PLAN.md`; commits `c29a19f`, `95882fc`, `848dd9e`,
`b47834e`). User-visible summary:

### Phase 1 — write-path correctness (`c29a19f`)
- **Finder mv/rename is now spool-aware.** Renaming a file or folder whose
  contents were still queued for upload previously broke the upload silently
  (data landed at the old path or not at all); renames now migrate queued
  spool entries, including a post-claim re-read that closes the race against
  an in-flight drain.
- **`cp` to the volume no longer exits 1** and truncate over NFS no longer
  fails with "RPC struct is bad" — the SETATTR{size} path is implemented for
  spooled files instead of stubbed.
- Fixed a spool file-handle refcount leak that left entries permanently
  "writing" after interrupted copies (the boot scrubber now also clears them).
- SQLite write contention under rsync-style load fixed (`_txlock=immediate`);
  stress run went from 2/5 failures to 0/10.

### Phase 2 — spool durability + UX (`95882fc`)
- **No more stranded uploads:** disabling the spool, quitting the app, or
  Stop-Everything now waits for (or explicitly hands off) pending uploads
  instead of silently orphaning them; "everything failed" no longer
  auto-quits past the problem.
- **Pending uploads UI:** the popover shows pending/in-flight counts, per-file
  age and last error, stalled/failed badges, and Retry failed / Recover
  stalled buttons (`GET /spool-recover?action=…` on the control plane).
- `/spool` reporting corrected (live size for in-progress writes; done rows
  no longer listed as pending).
- A full spool now reports "disk full" (NFS3ERR_NOSPC) to Finder instead of
  a generic I/O error.

### Phase 3 — identity, onboarding, mount honesty (`848dd9e`, assets `e557a3d`)
- **State-tinted menu-bar icon:** the citrus logo replaces the SF-Symbol
  drive — green healthy, amber degraded, blue offline-files mode, red fault,
  dimmed when idle — plus an upload-activity badge while drains run. Proper
  app icon (AppIcon.icns) from the same mark.
- **First-run Setup Assistant** with preflight checks (juicefs binary,
  macFUSE, backend reachability) and guided errors; reachable later via
  menu → Setup Assistant…. Existing configured installs are migrated as
  already-onboarded (no surprise first-run window).
- **Mount honesty:** NFS-mount state is part of the health model; the popover
  shows "Volume not mounted" with a Mount Now remedy row (single-flight
  `/mount-now` endpoint) instead of pretending all is well.
- At-a-glance popover header: one-word health, cache-vs-free bar, uploads row.

### Phase 3b — preferences redesign (`b47834e`)
- Preferences rebuilt as four grouped tabs (General / Connection /
  Cache & Storage / Maintenance) with sane sizing, clamped numeric fields,
  and whitespace-stripped URL fields.
- **Placebo settings eliminated:** memory-buffer budget, buffer file-size
  limit, and reconcile interval are now actually wired end-to-end (defaults
  byte-identical to before).
- Volume name now derives the mount point; hardcoded `127.0.0.1:11050` /
  mount-point strings removed from the UI (everything respects Preferences).
- Reset Local Metadata Cache flow (soft-stop → delete → Start Now/Later;
  pin database preserved).

## 2026-05-28 — Write spool (Option 2): local-SSD write intermediary

A foundational write-path change, behind `JM_SPOOL_ENABLE` (default off). Interposes
a JuiceMount-owned write spool on local SSD **between the NFS handler and JuiceFS's
raw staging**, so Finder writes ack the moment they're durable on local SSD
(`fsync()`) and a background drainer copies them into JuiceFS at MinIO's pace. Fixes
the WAN write cliff — 2 GB / 600-file Finder copies over Tailscale had hit ~2-hour
ETAs (~280 KB/s) because JuiceFS back-pressures on a RAM-tracked `--buffer-size`
budget even with 700+ GB of disk headroom. This is the Dropbox / LucidLink / Suite
write model, without forking JuiceFS. All 8 slices CI-green on `production-hardening`
(mirrored to `main`).

### What landed (slices A–H)

- **Spool primitives + SQLite index** (`nfs/spool.go`, `nfs/spool_index.go`,
  `metadata/spool_schema.go`, `metadata/spool_store.go`). `spool_entries` table in the
  existing metadata DB (shares its WAL + `writeMu`); capacity cap with atomic CAS
  reservation (default 50 GiB); refcounted `SpoolEntry` with streaming SHA-256.
- **Drainer** (`nfs/drainer.go`). Single dispatcher + bounded worker pool (default 4);
  `ListReady → MarkDraining → copy-to-FUSE → SHA-verify → MarkDrainComplete`.
  Exponential-backoff retry (out-of-worker timer so a slot isn't held during backoff),
  `MaxAttempts` 5 → `failed`; SHA mismatch → quarantine (never deleted).
- **Write-path integration** (`nfs/handler.go`, `nfs/spool_writefile.go`). `O_CREATE`
  routes through `spoolWriteFile`; acks at local SSD speed. nil-spool = unchanged
  `writeFile` path.
- **Read-path 3-tier lookup** (`nfs/handler.go`, `nfs/spool_readfile.go`). `spoolReadFile`
  + Stat/Lstat shadow serve not-yet-drained files from the spool. QA-35 perf-gated:
  empty-spool lookup benchmarked ~8 ns.
- **Runtime wiring + `GET /spool`** (`bridge/cbridge.go`, `cmd/jm5/main.go`,
  `nfs/spool_status.go`). Live pending/in-progress/failed/quarantined + per-entry list
  on the control plane (port 11050); 503 when disabled.
- **Crash recovery** (`SpoolStore.RecoverOnBoot`, `nfs/spool_recover_test.go`). Boot
  scrubber reconciles on-disk files vs SQL rows: orphan cleanup, `writing`→failed,
  `draining`→ready, capacity re-accounting. Runs before `drainer.Start`.
- **Integrity audit log** (`nfs/spool_manifest.go`). Append-only `manifest.log` with
  SHA-256 + timestamp for every drain-done and quarantine event. SHA-256 verified at
  three hops: streaming on write, on drain-read, and at-rest through FUSE after copy.
- **WAN-mode polish** (`health/fuse.go`). `JM_WAN_MODE=1` raises JuiceFS `--max-uploads`
  20 → 64; `JM_MAX_UPLOADS=<n>` overrides directly. `--buffer-size` stays at the QA-33
  value of 4096 — the spool, not a bigger RAM buffer, absorbs the write burst.

### Configuration

`JM_SPOOL_ENABLE=1` to opt in. `JM_SPOOL_DIR` (default
`~/Library/Application Support/JuiceMount/spool/`), `JM_SPOOL_SIZE_GB` (default 50),
`JM_WAN_MODE`, `JM_MAX_UPLOADS`. Rollback: set `JM_SPOOL_ENABLE=0` and restart.

### Deferred (not in this change)

Swift menu-bar "Pending uploads" section + icon badge, Manager web-UI tile,
`App.swift` graceful-quit dialog, Preferences "Sync & Upload" pane, 24-hour live soak.
The `/spool` JSON contract is stable, so these can land independently.

Full design + architecture: `docs/ROADMAP/option-2-spool.md`, `ARCHITECTURE_juicemount.md` § 15.

## 2026-05-13 — Phase A+B+C: production safeguards landed in parallel

Three agents working in parallel landed the next batch of production-readiness
work the day after the FileProvider ghost incident. Diff: 622 lines across
8 files; no unit-test regressions.

### Phase A — self-defense against environmental conflicts

- **Pre-mount conflict probe** (`bridge/cbridge.go:128`, `preMountConflictCheck` + `mountAt` helpers). Before the juicefs and NFS mounts run, walk the kernel mount table; if a foreign mount already owns the FUSE or NFS path, refuse to mount and surface a clear error string to Swift (including the source, type, and a `diskutil unmount` hint). Our own mounts (`JuiceFS:*` at FUSE path, `127.0.0.1:*` at NFS path) pass through and the existing soft-Stop reuse logic kicks in. Catches the "another FileProvider claimed /Volumes/zpool" failure mode at launch instead of in production.
- **Post-mount self-test** (`bridge/cbridge.go` `runSelfTest`/`SelfTestResult`, HTTP `GET /self-test` + `POST /self-test`). 10 MB read against the live NFS mount, classified green (≥200 MB/s) / yellow (50–200 MB/s) / red (<50 MB/s). Target selection: walks SQLite for any file ≥10 MB; falls back to a tmp file in the mount if none. Auto-runs once at server start, rerunnable via POST. Swift integration: `NFSBridge.SelfTestResult` + `selfTest(force:)` API; `ServerController.refreshSelfTest`; `MenuBarController` overlays a 5-pt colored dot in the icon's lower-right when self-test is yellow/red/error; `MenuPopoverView` `selfTestRow` shows "Self-test: 247 MB/s ✓" in the cache section.

### Phase B — observability

- **`/debug/pprof/*` endpoints** on the existing metrics server (port 11050). Five routes: `/`, `/cmdline`, `/profile`, `/symbol`, `/trace`. Live goroutine/heap/CPU/trace inspection without a separate listener. Would have saved hours of stack-archaeology during the 2026-05-12 incident.
- **Export Diagnostics button** in the menu-bar popover (`app/JuiceMount/Sources/JuiceMount/Core/DiagnosticsExporter.swift`, new). Bundles a 10-file zip onto the user's Desktop: tail of `juicemount.log` (5 MiB) and `juicefs.log` (1 MiB); current `/metrics` and `/cache-status` snapshots; `pluginkit -m`, `fileproviderctl dump` (first 200 lines), `df -h`, `mount`, `nfsstat -m`, plus a `system.txt` with macOS version, app version, anonymized hostname, and pid map. Cross-volume safe (copy+remove fallback), timeout-bounded subprocesses, deadlock-safe pipe draining on dispatch queues. `errors.txt` only when something fails — never crashes the export.

### Phase C1 — Developer ID signing + notarization

- **`scripts/build-app.sh` codesigning rewrite**. Auto-detects a Developer ID Application cert via `security find-identity`; falls back to ad-hoc with a yellow WARNING on the dev path. Env vars: `JM_SIGN_IDENTITY` (override cert), `JM_NOTARY_PROFILE` (default `JuiceMount`), `JM_QUICK=1` (skip notarization). Builds with `--timestamp --options runtime` when signing real, omits `--timestamp` on ad-hoc (Apple rejects it). Build footer now prints `Identity / Notarized / Staple` lines.
- **Notarization gated**. Only fires when: `JM_QUICK` unset, identity is not ad-hoc, AND a notary keychain profile is reachable. Uses `xcrun notarytool submit --wait` → `stapler staple` → `stapler validate`. Failures degrade to a WARNING (dev iteration never breaks).
- **`docs/signing.md`** (new) — first-time-setup walkthrough: how to create the Developer ID Application cert in Xcode, how to store the notarization credential profile (`xcrun notarytool store-credentials`), what each env var controls, verification commands (`spctl`, `stapler validate`), common errors, and a back-reference to `docs/no-fileprovider.md`.

### Coordination notes

- All three agents were briefed to work on non-overlapping file sections; merge was conflict-free.
- `bridge/cbridge.go` grew 391 lines (mostly Phase A's self-test + conflict probe logic).
- `MenuPopoverView.swift` grew 66 lines (Phase A self-test row + Phase B Export button).
- Pre-existing test failures in `nfs/` E2E tests confirmed environmental (need a live FUSE+Redis backend); unit packages `internal/jmlog`, `internal/cache/pin`, `cache`, `metadata` all green.

## 2026-05-07 / 2026-05-08 — Production-Hardening Branch (Phase 3 ship)

Eight commits on the `production-hardening` branch turning the prototype-quality menu-bar app into something a video editor can put real cellular sessions through. The headline features were already in earlier prototypes; this work is what made them survive contact with a 97%-full disk, an unreachable NAS at launch, and a pinned-set bigger than the configured cache.

### Offline mode + pin store (prototype 03 → production)

- `internal/cache/pin/` SQLite-backed pin registry with statuses Pending / Prefetching / Ready / Failed / Unpinned, indexed primary-key lookup via `IsPinnedReady(path)` (hot path on every NFS OpenFile when offline mode is on).
- 4-worker bounded prefetcher pool reading through FUSE in 1 MB chunks. JuiceFS LRU caches each block as it streams.
- `Prefetcher.PullPending(ctx, batchSize)` long-running daemon that drains Pending rows into the worker queue every 5s. Avoids the queue-overflow problem (256-slot ring) when the user pins large sets at once.
- `Prefetcher.ReWarmupLoop(ctx, ttl, batchSize)` periodic re-warmer (every 15 min, files older than 6 hours).
- `Prefetcher.VerifyAndRepair(ctx)` walks every Ready / Prefetching / Failed entry and marks them Pending so PullPending re-feeds the worker pool. The recovery path for "FUSE was momentarily unmounted at restart, every open() returned ENOENT, all 329 entries got marked Failed."
- Atomic `pin.IsOffline()` flag, set via `pin.SetOffline(bool)`. Read on every OpenFile and ReadAt; lock-free.
- **Open-time gate** in `nfs/handler.go` `OpenFile`: refuses un-pinned reads in offline mode in ~6 ms (vs. the 14-second NFS-retry timeout the read-time-only gate produced earlier).
- **Read-time gate** in `cachedFile.ReadAt`: only refuses reads on un-pinned files; pinned files fall through to FUSE/JuiceFS LRU at multi-GB/s.
- `Prefetcher.stripMountPrefix(path)` translates canonical pin keys back to FUSE-relative paths using the configured `mountPoint`, replacing the hardcoded `/Volumes/zpool/` constant.
- `JuiceMountHandler.canonicalize(filename)` translates in-mount filenames to canonical pin-store keys, handling plain-relative, leading-slash-relative, already-absolute, and trailing-slash-on-mount-point cases. Replaces the hardcoded `/Volumes/zpool/` + filename concat.

### macOS Services right-click + Pin Folder UI

- `app/JuiceMount/Sources/JuiceMount/Core/FinderService.swift` `@MainActor` class implementing `pinForOffline` and `toggleOfflineMode` via `@objc` methods, registered as `NSApp.servicesProvider`.
- `Info.plist` `NSServices` array declares both services with `public.file-url` send types. After enabling once in System Settings → Keyboard → Keyboard Shortcuts → Services, "JuiceMount: Pin for Offline" appears in Finder right-click → Services.
- `MenuPopoverView` Pin Folder button with native `NSOpenPanel` rooted at the mount point. Multiple-folder selection. Pin work runs on a background queue.

### JuiceFS cache config (the hardest-fought regression fix)

- **`--cache-size` plumbing.** GUI shim (`bridge/cbridge.go`) was constructing `FUSEManager` without forwarding `cfg.CacheSize`, so the user's `ssdCacheGB` preference was silently ignored. CLI (`cmd/jm5/main.go`) had been passing `100000` (100 GiB) all along.
- **`--free-space-ratio 0.01`.** JuiceFS default is 0.1 (refuse to cache when disk drops below 10% free). On a 97%-full video-editor's laptop, that meant every read went straight to S3. Lowered to 1% — the disk IS the cache.
- **`--cache-size` auto-expansion.** `FUSEManager.Mount()` now sizes the cache to 85% of total disk (with the user's configured value as a *floor*, not a ceiling). On a 926 GiB MacBook this expands a 100 GiB configured cache to 787 GiB, headroom enough for any realistic pinned set. Free-space-ratio still enforces the upper bound at write time.
- **`tailJuiceFSLog()`** goroutine watches `~/.juicefs/juicefs.log` and promotes WARNING/ERROR lines into `jmlog`. The chatty "space not enough on device" pattern is aggregated (one summary per 60 s with a count) so a flooded daemon log doesn't drown our log, but the user CAN see when the JuiceFS daemon is unhappy.
- **`checkCacheVolumeHealth()`** runs at mount time, emits a `jmlog.Warn` if the volume's free ratio is below the configured threshold.

### APFS purgeable-space reclamation

- `health.ReclaimPurgeableSpace(volume, targetBytes)` shells out to `tmutil thinlocalsnapshots <vol> <amount> 4` (urgency 4 = thin as much as possible). Measures freed bytes via statfs before/after.
- **Auto-reclaim before mount.** `FUSEManager.Mount()` checks free space; if below 50 GB it calls `ReclaimPurgeableSpace`. On the user's machine this freed 210.5 GB (49 GB → 260 GB free) by thinning a single Time Machine local snapshot.
- HTTP `POST /reclaim` endpoint on the control plane returns `{ok, freed_bytes, freed_gb}`.
- Swift popover shows a one-line "X GB free · Y GB reclaimable" with a Reclaim button when there's ≥ 5 GB reclaimable. Disk numbers are read via Foundation's `URLResourceKey.volumeAvailableCapacityForImportantUsageKey` — the only public macOS API that reports purgeable-aware capacity correctly.
- Disk-pressure banner in popover with three real states:
  - free < 1% of total → red, "JuiceFS has stopped caching"
  - free < 3% of total → orange, "JuiceFS will stop caching at 1% free"
  - pinned set > total disk capacity → red, "Pinned set exceeds disk capacity"

### Logging + control plane

- `internal/jmlog/jmlog.go` `rotatingFile` wrapper: 16 MB × 5 generations = 80 MB cap. Per-instance rotation flag (was package-level — re-Init bug). Auto-mkdir.
- `Preferences.defaultLogPath()` → `~/Library/Logs/JuiceMount/juicemount.log`. Swift app forwards `logFile` and `logLevel` to cbridge.
- HTTP control plane on the metrics port (11050):
  - `GET /metrics` — Prometheus-style counters
  - `GET /cache-status` — pin aggregate, per-root totals, live prefetch stats, offline mode flag
  - `POST /pin?path=...` — register a path/folder for offline pinning
  - `POST /unpin?path=...` — remove from registry
  - `GET /offline?on=1|0` — toggle offline mode
  - `POST /reclaim` — thin Time Machine local snapshots
  - `POST /verify-pins` — re-verify all pinned coverage by marking everything Pending

### Resilience

- `connectRedisWithRetry()` wraps `metadata.NewRedisClient` with exponential backoff (1 s, 2 s, 4 s, 8 s, 16 s = 5 attempts ≈ 31 s worst case). Survives wifi/cellular handoffs at launch. Logs each retry; emits "redis connect recovered" on success.
- Sync Now button now triggers BOTH metadata sync AND `verify-pins`. The user's mental model — "press Sync to make sure everything's current" — actually does what they expect.

### NLE project parsers (deferred, not wired into menu UI yet)

- `internal/nle/parser.go`, `premiere.go`, `resolve.go`, `fcpx.go` — extract media references from `.prproj` (gzipped XML), `.drp` (zip + SQLite), `.fcpxml`. 13/13 unit tests pass. Smoke-tested on a real Tampa Restaurants Premiere project. CLI exposed as `juicemount prefetch-project <file>` but no UI button yet.

### Tests added

- `internal/jmlog/TestRotation` — locks in 16 MB × 5 generations cap.
- `nfs/canonicalize_test.go` — 7 cases: plain, leading slash, absolute under mount, trailing slash on mount, non-default mount, empty fallback, root file.
- `nfs/billyfile_offline_test.go` — 4 states: online unpinned, offline unpinned, offline pinned, ReadAt and Read variants.
- `internal/cache/pin/TestIsPinnedReadyWindow` — 6 transition states including the late-Ready window where status is still Prefetching but bytes_cached >= size.
- `internal/cache/pin/TestStripMountPrefix` — 6 cases for the prefetcher's path translation.

### Bug fixes worth flagging

- **`OpenFile` open-time gate added.** Previously offline mode only refused reads at `cachedFile.ReadAt` time; small reads (<= 1 MB) succeeded silently because they'd hit the kernel page cache or memory buffer first. A 137 MB file took 14 seconds to fail. Open-time gate now fires in ~6 ms regardless of read size.
- **`cachedFile.pinned` snapshot** captured at OpenFile time. Without this, the read-time offline gate refused pinned files when the SSD cache reader missed (a separate cache layer from JuiceFS LRU), even though FUSE could have served the bytes locally.
- **`billyFile` offline-guard gap** (caught by independent code review). billyFile is the read path for files not yet in the SQLite metadata cache (fresh directory traversal). It had no read-time offline gate. Fixed: `billyFile.Read` and `billyFile.ReadAt` now both check `pin.IsOffline() && !f.pinned`.

### Verified live (snapshot at end of session)

- Pinned set: 331 files, 159 GB. All Ready after Sync triggers a verify-and-repair cycle.
- 200 MiB sequential read on a pinned file: **431 MB/s, 4.6 MB of network traffic** (Redis chunk-mapping lookups only — no bulk S3 fetches).
- Random-seek reads on pinned media: 50 MB blocks at 16–481 MB/s depending on cache hit rate.
- Pinned-offline reads: 215+ MB/s sustained (FUSE → JuiceFS LRU).
- Unpinned-offline reads: 4–67 ms fail-fast EIO (was 14 s pre-fix on a 137 MB file).

### Branch state

8 commits on `production-hardening`, parented on `prototype/offline-pin`. Squash-merge candidate to `main` once cellular soak test confirms no regressions.

```
9a1f229 feat(verify): Sync now also re-verifies pinned coverage; surface cache pressure
983b795 fix(cache): auto-expand --cache-size; user's 100 GiB cap was evicting all reads
56c2f6c feat(cache): reclaim APFS purgeable space for JuiceFS cache
21da8ee fix(cache): restore JuiceFS local caching — biggest GUI regression
f96ba16 production-hardening: address review findings + cold-start retry
a6db0c3 production-hardening: logging, path canonicalization, offline gate fixes
0c72399 prototype: baseline offline-pin + production hardening branch
c177cab prototype 03: offline-pin (power-user pre-cache + offline mode)
```

---

## 2026-05-07 — Phase 2 Menu Bar App + Phase 3 Observability

### Phase 2 — Native macOS Menu Bar App

JuiceMount now ships as a proper macOS menu bar application built in SwiftUI 6.2 with a Go c-archive backend. No more CLI-only operation.

**Architecture:**
- `app/JuiceMount/` — Swift Package with executable target
- `app/JuiceMount/Sources/JuiceMountCore/` — C interop layer that imports the Go-generated `libnfsd.h`
- `bridge/cbridge.go` — Go c-archive exports: `NFSServerStart`, `NFSServerStop`, `NFSServerIsRunning`, `NFSServerStats`, `NFSServerSyncNow`, `NFSServerSearch`, `NFSServerFreeString`
- All NFSBridge calls dispatched to a background queue so the UI never blocks

**Menu bar UI** (`MenuBarController` + `MenuPopoverView`):
- SF Symbol-based status icon with state-driven badge: idle, starting, running, syncing, degraded, disconnected, error
- Polished popover with header, volume info (entries count, last sync), per-backend health (Redis/MinIO/FUSE), and action buttons
- AppKit `NSStatusItem` for native feel; popover content is pure SwiftUI

**Search window** (`SearchWindowView`):
- Type-as-you-search with 150ms debounce and parent-path scope picker
- Native `Table` view with name/path/size columns and color-coded file-type icons
- **Spacebar opens `QLPreviewPanel`** for native Quick Look preview — the workflow you wanted for finding sound effects/footage by name and previewing instantly
- Enter / double-click reveals selection in Finder
- Drag to NLE (Premiere/Resolve/FCPX) supported via standard NSURL drag
- Result limit configurable: 50/100/250/1000

**Preferences window** (`PreferencesWindowView`):
- 4 tabs: General, Server, Cache, Advanced
- Persists to `UserDefaults`; volume name, mount point, Redis URL, NFS listen addr, SSD cache size, memory buffer budget, reconcile interval
- "Start at Login" via `SMAppService.mainApp` (macOS 13+)
- "Reset Local Metadata Cache" maintenance action

**Global hotkey** (`HotkeyManager`):
- ⌘⇧F opens search window from any app
- Carbon Event Manager (no Accessibility permission required)
- Toggleable in Preferences

**Build & deploy:**
- `scripts/build-app.sh` — full pipeline (Go c-archive → Swift build → `.app` bundle assembly → ad-hoc codesign)
- `scripts/build-cli.sh` — CLI-only build with entitlement codesigning
- `scripts/install.sh` — install to `/Applications`, optionally enable LaunchAgent
- `scripts/com.juicemount.agent.plist` — LaunchAgent for auto-start with crash-only relaunch

### Phase 1.4 — Codesigning

CLI binary is now codesigned with `com.apple.security.network.client` + `com.apple.security.network.server` entitlements via `scripts/build-cli.sh`. Eliminates the `ssh localhost` workaround needed to bypass macOS's 10GbE network filter.

### Phase 3 — Production Hardening (in progress)

Started during the same session by a background agent:

- **Structured logging** (`internal/jmlog`): replaces `log.Printf` with `log/slog` JSON handler. Levels: Debug/Info/Warn/Error. Optional file output via `--log-file`.
- **Per-RPC latency histograms** (`internal/metrics`): GETATTR, LOOKUP, READ, WRITE, READDIR, READDIRPLUS, CREATE, REMOVE, RENAME timed individually. Counters for total RPCs, errors, bytes.
- **/metrics endpoint**: small zero-deps HTTP server bound to `127.0.0.1:11050`. JSON output with p50/p95/p99 percentiles.
- **NFS auto-remount on stale mount**: Health monitor detects stale FUSE/NFS mounts via 5s `Stat()` timeout, gates remount on 3 consecutive failures + 60s cooldown.

### Files Added

| Path | Purpose |
|------|---------|
| `app/JuiceMount/Package.swift` | Swift Package definition |
| `app/JuiceMount/Sources/JuiceMountCore/include/JuiceMountCore.h` | C bridge header |
| `app/JuiceMount/Sources/JuiceMount/App.swift` | App entry point |
| `app/JuiceMount/Sources/JuiceMount/Core/NFSBridge.swift` | Idiomatic Swift wrapper around c-archive |
| `app/JuiceMount/Sources/JuiceMount/Core/ServerController.swift` | `@Observable` lifecycle controller |
| `app/JuiceMount/Sources/JuiceMount/Core/Preferences.swift` | UserDefaults-backed prefs |
| `app/JuiceMount/Sources/JuiceMount/Core/HotkeyManager.swift` | Global ⌘⇧F hotkey |
| `app/JuiceMount/Sources/JuiceMount/Core/LoginItemManager.swift` | SMAppService start-at-login |
| `app/JuiceMount/Sources/JuiceMount/UI/MenuBarController.swift` | NSStatusItem + popover host |
| `app/JuiceMount/Sources/JuiceMount/UI/MenuPopoverView.swift` | Menu popover SwiftUI view |
| `app/JuiceMount/Sources/JuiceMount/UI/SearchWindowView.swift` | Search window with QuickLook |
| `app/JuiceMount/Sources/JuiceMount/UI/QuickLookCoordinator.swift` | QLPreviewPanel bridge |
| `app/JuiceMount/Sources/JuiceMount/UI/PreferencesWindowView.swift` | Preferences tabs |
| `app/JuiceMount/Resources/Info.plist` | Bundle Info.plist (LSUIElement, sandbox keys) |
| `scripts/build-app.sh` | Full build pipeline |
| `scripts/build-cli.sh` | CLI-only build with codesigning |
| `scripts/install.sh` | Install to /Applications + LaunchAgent |
| `scripts/com.juicemount.agent.plist` | LaunchAgent definition |
| `internal/jmlog/` | Structured logging package (Phase 3) |
| `internal/metrics/` | Latency histograms + /metrics HTTP server (Phase 3) |

---

## 2026-03-31 — Code Audit & Full-Stack Overhaul

### Summary
Comprehensive code audit identified 8 correctness bugs, 8 performance issues, and 8 architectural gaps. All critical items addressed. JuiceFS FUSE mount management integrated into the JM5 process — JM5 is now a complete self-contained stack.

---

### Correctness Fixes

1. **Race condition in inode counter** (`nfs/handler.go`)
   - `inodeCounter` was bare `uint64` incremented without synchronization across goroutines
   - Fixed: changed to `atomic.Uint64`

2. **MemoryBuffer never invalidated on writes** (`nfs/handler.go`)
   - A file cached in MemoryBuffer would serve stale data after NFS writes, renames, or deletes
   - Fixed: `writeFile.Close()`, `Rename()`, and `Remove()` now call `memBuf.Invalidate()`

3. **writeSizes map leaked memory** (`nfs/handler.go`)
   - `trackWriteSize()` added entries but nothing ever removed them
   - Fixed: `writeFile.Close()` deletes the entry after use

4. **Remove() deleted from FUSE in background** (`nfs/handler.go`)
   - Returned success to NFS client before the file was actually removed on disk
   - Fixed: FUSE `os.Remove()` is now synchronous

5. **FDPool double-check race** (`nfs/fdpool.go`)
   - `Get()` and `GetWrite()` accessed entry fields outside the lock after the double-check
   - Fixed: all entry access is now under the lock

6. **UID/GID=0 on metadata-cached entries** (`metadata/types.go`, `nfs/handler.go`)
   - `metadata.FileInfo.Sys()` returned `*metadata.Entry` which the NFS attribute builder didn't recognize, falling back to UID=0/GID=0 (root:wheel)
   - macOS Finder showed red "no access" minus signs on every folder
   - Fixed: `Sys()` now returns `*syscall.Stat_t` with the current user's UID/GID
   - Same fix applied to `rootDirInfo`

### Performance Improvements

1. **O(1) ListChildren** (`metadata/store.go`)
   - Added `childrenIdx map[string]map[string]*Entry` (parent -> children)
   - `ListChildren()` was O(total entries) scanning all 147K entries per ReadDir
   - Now O(children count) — direct map lookup
   - All 8 mutation points (Insert, Delete, BulkInsert, etc.) maintain the index

2. **READ/WRITE buffer pools** (`internal/nfs/nfs_onread.go`, `nfs_onwrite.go`)
   - Added `sync.Pool` for 1MB read data buffers (eliminates per-RPC allocation)
   - Response writers use the existing `responseBufferPool` instead of `bytes.NewBuffer([]byte{})`

3. **Pre-serialized GETATTR XDR** (`internal/nfs/nfs_ongetattr.go`)
   - Already implemented in codebase; now works correctly with the UID/GID fix
   - 88-byte cached response body, zero XDR marshaling on cache hit

4. **BulkClearLocalOnly** (`metadata/store.go`, `metadata/redis.go`)
   - Discovered during testing: `syncMetadata()` called `ClearLocalOnly()` individually for 147K entries (147K SQLite UPDATE transactions)
   - Added `BulkClearLocalOnly()` that batches 500 per transaction
   - Redis sync time on cell hotspot: >60s timeout -> 85s pass; on 10GbE: ~7s

5. **Readahead uses FDPool** (`nfs/readahead.go`)
   - Prefetch goroutines now reuse pooled file descriptors instead of opening new ones

### Architecture Changes

1. **Removed double handle caching** (`nfs/server.go`)
   - `helpers.NewCachingHandler` wrapper was adding UUID-based LRU handles on top of our deterministic inode-based handles
   - Removed: NFS handler now served directly, eliminating UUID allocation, LRU eviction, and mutex contention

2. **Bounded prefetch goroutines** (`nfs/handler.go`)
   - Recursive `prefetchChildren` could spawn unbounded goroutines
   - Fixed: non-blocking semaphore acquisition before spawning sub-prefetches

3. **TCP keepalive** (`nfs/server.go`)
   - Added `SetKeepAlive(true)` + `SetKeepAlivePeriod(30s)` on NFS connections
   - Stale client connections (laptop sleep, WiFi drop) are now detected and cleaned up

4. **Graceful shutdown ordering** (`cmd/jm5/main.go`)
   - Replaced defer-based shutdown with explicit ordered sequence:
     unmount NFS -> stop server -> stop handler -> stop health -> stop Redis -> close SQLite -> unmount FUSE

5. **Stale prefetch map cleanup** (`nfs/handler.go`)
   - `prefetched` map entries now evicted after 2 minutes by the existing cleanup loop

6. **JuiceFS FUSE mount management** (`health/fuse.go`, `cmd/jm5/main.go`)
   - JM5 now mounts JuiceFS FUSE automatically on startup (no manual `juicefs mount` needed)
   - Background monitor checks mount health every 10s, auto-remounts on crash
   - Kills stale JuiceFS processes before remount
   - FUSE mounted at hidden `~/.juicemount/fuse-internal` (invisible to user)
   - Users only see the NFS volume at `/Volumes/zpool`
   - `--no-fuse` flag for testing/development

7. **Real FUSE health check** (`health/monitor.go`)
   - Old check: `os.Stat(fusePath)` — passes even when FUSE is unmounted (directory still exists)
   - New check: verifies mount table entry exists AND `ReadDir` responds within 5s

### Dead Code Cleanup
- Removed unused `cacheWarm` field from ReadaheadManager
- Removed dead `rdb` creation in main.go (was printing a pointer address)
- Removed unused `context` import in cbridge.go

### Test Fixes
- `TestReadCachedBlock`: added 30s context timeout and reduced scan iterations (was hanging forever over slow networks)
- `TestFileInfo`: updated to expect `*syscall.Stat_t` from `Sys()` instead of `*metadata.Entry`
- `ReadaheadManager` test calls updated for new constructor signature and `Stats()` return values

---

### Files Modified (16 files)

| File | Changes |
|------|---------|
| `cmd/jm5/main.go` | FUSE manager integration, graceful shutdown, dead code, flags |
| `nfs/handler.go` | atomic inode, membuf invalidation, writeSizes cleanup, sync Remove, bounded prefetch, prefetched cleanup, rootDirInfo UID/GID |
| `nfs/server.go` | Removed CachingHandler wrapper, TCP keepalive |
| `nfs/fdpool.go` | Double-check race fix |
| `nfs/readahead.go` | FDPool integration, removed unused field |
| `nfs/readahead_test.go` | Updated constructor and Stats() calls |
| `metadata/store.go` | Children index, BulkClearLocalOnly |
| `metadata/types.go` | Sys() returns *syscall.Stat_t with correct UID/GID |
| `metadata/redis.go` | Uses BulkClearLocalOnly |
| `metadata/store_test.go` | Updated TestFileInfo for new Sys() |
| `cache/reader_test.go` | Context timeout on TestReadCachedBlock |
| `internal/nfs/nfs_onread.go` | Buffer pool for read data + response |
| `internal/nfs/nfs_onwrite.go` | Buffer pool for response |
| `health/fuse.go` | NEW — JuiceFS FUSE mount lifecycle manager |
| `health/monitor.go` | Real FUSE health check (mount table + readdir) |
| `bridge/cbridge.go` | Dead code cleanup |
