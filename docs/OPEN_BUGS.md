# Open bugs — triage queue

**Purpose.** `docs/STATE.md` is the chronological closure log of QA findings (good for "what did we ship and when"). This doc is the forward-looking triage list — what's still open, ranked by user-impact, with the smallest-viable next step for each. Maintained as we find bugs and pruned as we close them.

**Snapshot:** 2026-05-26 after commit `bd7eb8d` (TrueNAS-app loop closeout shipped); QA-36 logged.

---

## P0 — blocking the user vision

### ~~QA-34 — FUSE mount disappears under sustained write~~
**Status:** ✓ CLOSED 2026-05-25. Root cause was a hidden side-effect in `health/fuse.go isMountedLocked()`: when a 5s ReadDir probe timed out, it fired `umount -f` as a fire-and-forget goroutine. That umount races juicefs's in-flight fsync (which can run 15-30s under sustained writes), reliably killing the daemon mid-write. QA-33's consecutive-failure tolerance was bypassed because the kill came from the leaf health check, not from `monitorLoop`. Fix (commit pending): `isMountedLocked()` is now pure-read; only `monitorLoop` triggers remounts; `unmountLocked()` now waits synchronously (60s bound) for umount to complete before returning, eliminating the umount/remount race. Validation: 5 GB write test completed all 5 × 1 GiB copies end-to-end, single juicefs daemon survived two slow-fsync wedges by self-recovering, zero bounded-command timeouts.

Plus earlier sub-fix: Lua SCAN MATCH tightened from `d*` to `d[0-9]*` to exclude juicefs `delfiles` / `delSlices` LIST keys (was producing WRONGTYPE errors in `metadata.RedisClient` sync). Both fixes were necessary; the WRONGTYPE was a secondary symptom of the same load condition.

**Symptom:** sustained `dd`-style write to `/Volumes/zpool` past ~1.4 GB cumulative bytes results in the FUSE mount disappearing from the mount table mid-write. dd reports "Permission denied" on subsequent file opens. JM logs show:

- `component degraded: FUSE — not mounted (directory exists but no FUSE)`
- `reconciliation failed: redis EVAL: WRONGTYPE Operation against a key holding the wrong kind of value script: …`
- `FromHandle STALE` storms on real inodes for files that just lost their FUSE backing

**Why it blocks the vision:** the user-requested "Dropbox-style write model" (write at local-disk speed up to cache size, upload in background) cannot ship while sustained writes risk killing the mount mid-flight. QA-32 + QA-33 reduced the blast radius (no more pinned-file destruction, no more juicefs murder on slow flush) but the underlying instability remains.

**Hypotheses to test, in order of cheapness:**
1. **Redis DB collision** — both JuiceFS and JM's `metadata.RedisClient` use `redis://…/1`. JuiceFS uses Lua scripts that assume specific Redis key shapes. If JM's reconcile/applyEvent paths are writing keys juicefs reads (or vice versa), you'd get exactly `WRONGTYPE` errors. **Cheap test:** point JM at `/2` instead of `/1`, repeat the 5 GB write test, see if WRONGTYPE goes away. Move JuiceFS first if needed (juicefs config is per-volume, ours lives in Redis at `/1`).
2. **JuiceFS daemon bailing out** under buffer pressure — `--buffer-size 4096` may be hitting a memory limit on macOS, or the upload-pipeline state machine has a bug surfacing only under sustained chunky writes. Run the test with `--verbose` juicefs logging captured to a file; look at the last 50 lines before FUSE disappears.
3. **macOS-FUSE kernel-side unmount** — less likely but possible. `dmesg | grep -i fuse` or `log show --predicate 'process == "kernel"' --last 5m` immediately after the failure.

**Smallest viable next step:** swap JM to a separate Redis DB (`/2`), re-run the 5 GB test. Single config-line change in `Preferences.swift` / bridge config. If WRONGTYPE goes away, hypothesis 1 is confirmed and the fix is "use separate DBs by default."

---

## P1 — known bug under specific workload

### ~~QA-37 — Delete during spool drain resurrects the file (writes feel local, deletes don't stick)~~

**Status:** ✓ FIXED 2026-06-01. The NFS REMOVE path now cancels any in-flight spool entry before the drainer can resurrect it: `nfs/handler.go Remove` calls `SpoolStore.CancelForDelete` (evict index entry + `metadata.DeleteActiveByPath` deletes the writing/ready/draining row(s) under `writeMu` + remove spool file + release capacity). `metadata.MarkDone` now returns rows-affected and `nfs.MarkDrainComplete` returns `(done bool, _)`; a drain that finds its row cancelled (`done==false`) undoes its FUSE write (`os.Remove(dest)`) instead of completing. The DELETE and the drainer's MarkDone are serialized by `writeMu`, so exactly one of {complete, cancel} wins and the loser observes the resolved state. Regression tests in `nfs/spool_delete_race_test.go` (RED→GREEN: before-drain cancel + in-flight `draining`-state cancel), green under `-race`. **Live-verified** on the real mount: write 20 files + immediate delete mid-drain → 0 resurrected (was: all 20 reappeared).

**Original report —** found + reproduced 2026-06-01 (this Mac, 10GbE LAN, build with write-spool enabled).

**Symptom:** Deleting a file while its spooled copy is still draining to JuiceFS/MinIO returns success, but the drainer then writes the file back — it reappears. Reproduced deterministically: write 20×256 KB files, immediately delete all 20 (NFS REMOVE returns 0 errors); as the spool drains `pending → 0`, all 20 files reappear. **Control:** deleting the same files *after* the drain settles holds correctly (0 reappear) — so it is specifically a delete↔drain race, not a delete bug. Also observed incidentally: an `os.rmdir` "succeeded" yet the directory + files were back moments later.

**Why it matters:** A real editing workflow — copy a folder then remove some files, or write-then-delete render temp files — can leave "deleted" data on the vault, and storage isn't reclaimed. Silent: the client believes the delete succeeded. This is a direct gap in the Option-2 write-spool coordination.

**Root cause (likely):** the NFS REMOVE path (`nfs/handler.go`) deletes the JuiceFS/metadata entry but does not cancel/remove the pending spool entry, so `nfs/drainer.go` later drains the spooled copy back into JuiceFS, re-materializing the file (and its parent dir).

**Smallest viable next step:** on delete, look up and remove/tombstone any active or pending spool entry for that path, and have the drainer re-check existence (or honor a tombstone) before writing. Add a regression test using the repro above (write → immediate delete → assert absent after drain settles).

### ~~QA-32 follow-up — 1,577 STALE events during pin-coverage-verify~~
**Status:** ✓ CLOSED 2026-05-25 by commit 6fa28cf. Re-ran the workload post-QA-32 (build 6fa28cf vs original 88ccee8): **0 STALE events (was 1577)**. The pin-guard added to the OpenFile phantom-purge path in QA-32 was the fix — that path was bypassing Layer C, allowing the cascade.

### QA-32 follow-up — (entry preserved for history)
**Status:** ✓ CLOSED (see above).

**Symptom:** running `scripts/qa-suite/11-workloads/pin-coverage-verify.sh` on the post-QA-31 build produces 1,577 `FromHandle STALE` events in 60 s, all on real (non-synthetic) inodes. Layer B recovered the MP4 inode `0x1aa63` once but STALE re-fired 25 more times for the same inode. `pathCache` and `inodeCache` size delta grew from baseline 5 → 147 during the run (alias-inode accumulation past Layer B).

**Why it matters:** when a user hits "Sync Now" with pinned files, the verify path triggers what looks like a normal-ish surface — except for ~26 STALE events/sec, which DaVinci Resolve sees as "file flickered offline" 26 times per second during a scrub.

**Hypotheses:**
1. The verify path re-prefetches via FUSE while metadata sync is concurrent. The two paths race on the same inodes. evictOldest may be re-rebuilding cache from pathCache and dropping the QA-28 redirect aliases.
2. Spotlight + Finder are issuing background NFS lookups during the verify, hitting handles that were just transiently invalidated by the concurrent re-sync.

**Smallest viable next step:** instrument the QA-28 redirect path with a counter (`inode_redirects_total`, `alias_inode_drops_total`). Re-run pin-coverage-verify with the counter visible. If the drop count climbs in lockstep with STALE events, we've found the leak. Fix is to extend Layer B's shadow map to also park aliases on eviction.

### QA-36 — Mac client stuck after long network outage (≥1h); won't reconnect on its own

**Status:** OPEN, logged 2026-05-26. Reproduced live (this Mac, build da46708, between 18:43 and 20:30 EDT). Workaround verified.

**Symptom:** When the LAN path from the Mac to the TrueNAS box is broken for an extended period (~1.5h in this incident), JuiceMount enters a state it cannot recover from automatically even after the network comes back. UI reports "Server is in error." Log shows:

- 32 consecutive `redis EVAL: dial tcp 192.168.0.197:30179: connect: no route to host` errors with 5-minute backoff
- After the network restored: `juicefs FUSE mount failed: juicefs mount: timed out after 30s (backend unreachable?)`
- Each manual Start (menu bar) attempts a fresh mount; each times out at 30s with the same backend-unreachable error
- Meanwhile, raw TCP probes from the SAME Mac succeed within milliseconds: `nc -z 192.168.0.197 30179` connects, `redis-cli -h 192.168.0.197 -p 30179 ping` returns PONG, MinIO health endpoint returns 200 in <20ms
- Server side is fully healthy; `juicefs status redis://192.168.0.197:30179/1` from the CLI works and returns the formatted volume

**Recovery (verified):** killing the JuiceMount.app process entirely (`pkill -TERM`, NOT SIGQUIT) and relaunching produces a clean startup. New process mounts FUSE + NFS within ~30s and reports `system recovered — all components healthy`. Stop+Start from the menu bar was insufficient — the cgo bridge's juicefs subprocess kept reusing the broken connection pool.

**Why it matters:** every Mac client that lives through a wifi-flip, VPN-toggle, or genuine network outage longer than the JM reconnect backoff window has to be manually restarted by the user. For a "always-on" desktop service this is exactly the kind of jank that erodes trust. The auto-recovery path that QA-26 added (ephemeral URLSession, stuck-state backstop) isn't reaching this case because it's the juicefs daemon's connection state, not JM's HTTP polling, that's stuck.

**Hypotheses:**

1. **Go net.Dialer cached resolution / routing.** Go's net package on macOS uses cgo getaddrinfo, but the `net.Dialer` keeps an internal pool that may not invalidate cleanly on a "network unreachable" → "reachable" transition. Test: capture netstat / routing table before-and-after a network flip, look for stale TCP CLOSE_WAIT / FIN_WAIT to the Redis port.

2. **juicefs daemon's redis client backoff.** The juicefs binary spawns its own go-redis client with its own backoff schedule. Even after the parent JM clears its state and re-mounts, the juicefs subprocess inherits the broken connection logic. Test: instrument the FUSE mount path with `JM_FUSE_VERBOSE=1` (already wired), check the juicefs.log around the timeout for evidence of stuck Redis client.

3. **Cached negative DNS / route entries in the kernel.** The Mac's routing table picked up a "no route" decision and is reusing it. Test: `route flush` or `dscacheutil -flushcache; sudo killall -HUP mDNSResponder` between the network outage and the next mount attempt.

4. **FUSE mount handle is stale.** The `~/.juicemount/fuse-internal` directory may have an orphaned macFUSE attachment that prevents fresh mounts. Test: `diskutil unmount force ~/.juicemount/fuse-internal` before each mount retry.

**Smallest viable next step:** wire a recovery-from-long-network-outage detector into JM's `ServerController` or `health/fuse.go monitorLoop`. When the network has been unreachable for >5 min AND becomes reachable, FORCE a full process-internal teardown of the juicefs subprocess (kill + wait + respawn) instead of relying on whatever soft-reconnect the existing reconnect goroutine is doing. The signal: `network_path` warnings stopping after 5+ minutes of firing → fire a "purge connection pools" callback that walks `globalFUSE.Stop()` → `globalFUSE.Mount()` cleanly, in addition to whatever the user clicks.

**Validation criteria for fix:** simulate a network outage by `sudo pfctl -a com.apple/airdrop -F all -f -` or by toggling wifi off → wait 2 min → wifi on. Without fix: JM stays in stuck-state until user kills + relaunches. With fix: JM recovers within 60s of network return with no user action.

**Workaround for users today:** if the menu bar says "Server is in error" after a network outage, quit the app entirely (Cmd+Q from the menu) and relaunch from /Applications or `open build/JuiceMount.app`. Stop+Start from the menu is not enough.

---

### QA-37 — Finder write errors -36 / 100060 + non-local throughput on small files

**Status:** PARTIAL, logged 2026-05-28 from live testing.
- Slice B (FDPool keyspace split) — COMMITTED 2026-05-28 (929909f), CI in flight.
  Fixes the -36/EBADF error class. See commit message for full diff context.
- Slice A (async UpdateSize + debounced publishEvent) — DESIGNED, awaiting user
  ack before implementation. See "Slice A design" subsection below.

**Symptom:**
- Writing or copying to `/Volumes/zpool` from Finder occasionally errors with
  **Error code -36** (`ioErr`, macOS POSIX write failure surfaced through the
  Finder File Manager).
- Some operations error with **100060** (likely a Cocoa
  `NSFileWriteUnknownError` or related Finder file-coordination code; not a
  standard POSIX errno — need to trace the exact origin in JuiceMount.app
  logs or `console.app` at the time of failure).
- For files small enough to be fully covered by the local SSD cache
  (`< 1 GB`, well under the configured `--cache-size 100000` = 100 GB), write
  throughput is NOT at "local SSD" speed — the user observes network-bound
  behavior even when the buffer/cache should absorb the entire payload.

**Why it matters:** the Dropbox-style write model (see P2) is contingent on
exactly this case working — small/medium writes must land in the local
buffer/cache and ack to Finder at SSD speed, with the upload happening
asynchronously in the background. If -36 / 100060 errors fire mid-write,
the cache is being bypassed or the write path is going synchronous to the
S3 backend. Two distinct symptoms (errors AND non-local throughput) make me
suspect they share a root cause around how juicefs's `--writeback` /
`--buffer-size` interact with the NFS gateway's `WRITE` RPCs.

**Hypotheses:**

1. **NFS write commit semantics** — NFS clients on macOS issue
   `NFSPROC_WRITE` with `stable=DATA_SYNC` for some patterns (Finder
   metadata writes, Resolve project saves). The handler may be honoring
   `DATA_SYNC` synchronously, forcing a flush all the way to MinIO before
   ACK, defeating the cache. Check `nfs/handler.go` write path for the
   `commit` flag handling.

2. **Buffer overflow under burst** — `juicefs mount --buffer-size 4096`
   (4 GB) might be filling under a sustained burst, blocking new writes
   until pages drain. Look for `juicefs_staging_block_bytes` saturation
   in `/cache-status`.

3. **POSIX → NFS error translation** — `-36 ioErr` in NFS land typically
   surfaces from a downstream EIO. The mapping in `nfs/handler.go` might
   be returning `NFS3ERR_IO` (= EIO) for transient backend errors that
   should retry transparently.

4. **100060 is JuiceMount.app's own log code, not POSIX** — search
   `app/JuiceMount/Sources/.../Logging.swift` for the literal `100060`
   to see which path emits it. Probably a specific error category for
   "write rejected by NFS server" or similar.

**Smallest viable next step:** add structured logging to `nfs/handler.go`
WRITE path that records `stable`, `count`, `time-on-cache-vs-time-on-network`,
and the resulting NFS reply code. Reproduce by copying a 500 MB file from
the Mac into `/Volumes/zpool` and noting (a) wall-clock time vs the same
copy to a local SSD, (b) any errors, (c) whether the file is fully
cache-resident afterward via `jmctl pin-status`.

**Validation criteria for fix:** 500 MB write to `/Volumes/zpool` finishes
in roughly the time of a local-SSD copy (same order of magnitude, allowing
for cache-write overhead), with zero -36 / 100060 errors. The data shows up
fully in JuiceFS afterward and reads back at local speed.

#### Slice B — fdpool keyspace split (LANDED 2026-05-28, 929909f)

**Root cause:** `nfs/fdpool.go` keyed by bare `path`. `Stat()` would do
`fdPool.Get(path)` and cache an RDONLY fd. A subsequent WRITE RPC calling
`fdPool.GetWrite(path, O_RDWR|O_CREATE, perm)` found the cached RDONLY fd
and returned it; the next `WriteAt` EBADF'd → Finder -36 / Cocoa 100060.

**Fix:** keyed by `fdKey{path, write}` so read and write fds live in
independent slots. `HasOpenRefs` now checks both slots (QA-35 active-holder
gate covers writers too).

**Pre-merge testing:** read-path regression tests green
(TestNFSReadFile, TestNFSReadLargeFile, TestReadahead*, TestNFSFDPoolStats,
TestNFSCreateAndReadLarger). Three new FDPool tests cover slot isolation,
wrong-slot noop, and N=16 concurrent GetWrite race under `-race`. Code
reviewer signed off after 2 HIGH items addressed (stale comment + missing
race test).

**Live validation still needed (post-deploy):**
- Repro the original -36 error from a Finder copy on the deployed image.
  Slice B should eliminate the EBADF class entirely.
- Confirm no read-cache regression: DaVinci playback of a fully-cached
  4 K MP4 with a parallel write in progress.

#### Slice A — async UpdateSize + debounced publishEvent (DESIGN, AWAITING ACK)

**Suspected throughput root cause (H2 from original triage):** every WRITE
RPC's `writeFile.Close` at `nfs/handler.go:~1690` does a synchronous
`store.UpdateSize` (SQLite UPDATE under `writeMu`) + spawns a `publishEvent`
goroutine that PUBLISH'es to Redis. On a 2000+ file Finder copy, this
serializes all concurrent writers on `writeMu` and floods Redis subscribers
with one event per RPC (potentially thousands per file at 64 KB RPCs).

**Proposed design:**

1. **Batched size flusher.** New `nfs/size_flusher.go`. Adds
   `sizeFlushPending map[path]{size, mtime, inode}` + a 500 ms ticker
   goroutine. `writeFile.Close` enqueues into this map (MAX merge on
   concurrent updates per path) instead of calling `store.UpdateSize`
   directly. Flusher snapshots the map under lock, clears it, releases the
   lock, then issues SQLite UPDATEs in batch. Final flush on
   `StopHandler`.

2. **publishEvent debounce.** New
   `lastPublished map[path]time.Time` + 1 s window. `writeFile.Close` skips
   publish if within window. ONLY for the per-Close create/update event;
   rename/delete/other publishers untouched.

**Cache-correctness invariants preserved:**
- `writeSizes` map is the truth source during writes (handler.go:990-998
  already prefers it over SQLite size on Lstat; same for Stat). Stat/Lstat
  during the debounce window see the new size from `writeSizes`, not
  from stale SQLite. **No read-path code touched.**
- `pathCache` update inside `UpdateSize` still happens during the flusher
  pass — just delayed by up to 500 ms. Acceptable because writeSizes
  shadows it for fresh writes.

**Trade-off accepted:**
- Crash-window durability: if the process crashes between `writeFile.Close`
  and the next flusher tick, the SQLite size for in-flight writes is lost.
  On restart, `writeSizes` is gone and SQLite returns the stale size.
  Next write to the file fixes it. Risk: low (rare crash, recoverable),
  worth the ~10x-100x write-throughput gain on small-file bursts.

**Tests to add pre-merge:**
- Read-after-write sees the new size within the debounce window
  (writeSizes invariant).
- Concurrent writes to the same path coalesce into a single SQLite UPDATE.
- `StopHandler` flushes all pending writes.
- DaVinci playback regression check on fully-cached 4 K MP4 with parallel
  500 MB write in progress (user's specific concern).

**ASK FOR USER:** before implementing Slice A, please confirm:
(a) the crash-window trade-off is acceptable, and
(b) the 500 ms / 1 s windows are reasonable defaults (configurable later
via env if needed).

---

### QA-38 — Menu bar shows persistent "offline / disconnected" while mount is online

**Status:** OPEN, logged 2026-05-28 from live testing.

**Symptom:** JuiceMount menu-bar app's status indicator reports
"offline" / "disconnected" persistently, even though `/Volumes/zpool` is
mounted and operational (writes/reads round-trip through the NFS gateway
successfully).

**Why it matters:** the status indicator is the user's primary signal of
JM's health. If it lies, the user either ignores the status (eroding all
future warnings — boy-who-cried-wolf) or assumes the mount is broken and
forces a stop/start, causing churn the system didn't need.

**Hypotheses:**

1. **`ServerController.status` reads a stale value** — the menu bar polls
   either a Combine publisher or a direct `health/fuse.go` query that
   isn't getting invalidated when the mount transitions back to healthy.

2. **Health monitor consecutive-failure threshold sticky** — `health/fuse.go`
   may be holding a "degraded" state past the point where the underlying
   check is reporting healthy (QA-33 raised this threshold to 3 consecutive
   failures; the reverse — clearing degraded state — may need its own
   threshold or it sticks).

3. **Online detector is keyed on the wrong signal** — if "online" is
   defined as "Redis reachable AND MinIO reachable AND FUSE mounted" and
   any of those three reports stale data from a cached HTTP probe, the
   aggregate stays "offline" even after the mount itself is fine.

4. **macOS reachability framework lag** — `Network.framework` /
   `SCNetworkReachability` can hold stale `notSatisfied` state through
   a brief network blip; the JM app may be trusting that signal as
   gospel.

**Smallest viable next step:** read
`app/JuiceMount/Sources/JuiceMount/UI/MenuBarController.swift` and
`Core/ServerController.swift` to find which Combine publisher / `@Published`
property the status icon binds to. Add structured-log entries on every
transition (`"status: \(old) -> \(new), trigger: \(reason)"`) and reproduce
by checking the menu bar against `mount | grep fuse-internal` over a
10-minute window.

**Validation criteria for fix:** menu bar status follows the actual mount
state with at most a 10-second lag in either direction. No "phantom
disconnected" states when the mount is provably up via `mount` + a
round-trip write/read test.

---

### Watchdog tolerance edge case
**Status:** open follow-up to QA-33.

**Symptom:** QA-33 requires 3 consecutive 10-second-interval unhealthy checks before remounting (30 s threshold). For a juicefs daemon that's truly hung but with the PID still alive (process exists, but FUSE handle is dead), this means 30 s of dead mount before recovery. The fast-path "is PID alive" only catches PID-gone cases.

**Smallest viable next step:** add a second fast-path: if `isMountedLocked()` reports unhealthy AND `mount | grep fuse-internal` returns nothing (i.e., kernel says no FUSE here), remount immediately regardless of consecutive-failure count. PID-alive-but-mount-gone is the macOS-FUSE-kernel-killed case.

---

## P1.5 — migrator tier-3 / tier-4 features (deferred)

The migrator's tier-1 toggles + tier-2 advanced opts shipped in
commit `0e58106`. The remaining tiers from the design discussion are
parked here so they're not lost. Each item is a self-contained slice.

**Tier 3 — polish + advanced (next iterations):**

- **Job persistence to SQLite.** Currently job state lives in
  in-process memory; a juicemount-server restart loses everything.
  Add a `migrator_jobs` table to the existing pin store DB or a
  sibling DB; persist Submit / state transitions / final Last
  ProgressEvent. On startup, load incomplete jobs and either
  resume (if --resume-on-startup) or surface them as "interrupted."
  Smallest viable: add to internal/migrator/state.go, wire into
  JobManager.Submit + run() transitions.

- **Scheduled migrations.** Cron-style picker in the UI ("Now",
  "Tonight at 2 AM", "Custom cron expression"). New JobState
  `scheduled` + a goroutine in JobManager that promotes
  scheduled→pending at the right time. Requires #1 (persistence).

- **Continuous mirror mode.** Toggle in the options form. After the
  initial sync completes, queue a follow-up sync N hours later.
  Implementation: when a job finishes with mirror=true, JobManager
  schedules a fresh job with the same params at now+interval.

- **Migration profiles.** Save the current options form (source set
  + destination + all toggles) by name; re-run by clicking the
  profile. Backend: small CRUD endpoint on /api/profiles. Storage:
  same SQLite as job persistence.

- **Live throughput sparkline.** SSE already emits BPS per tick;
  the UI just needs to keep a rolling window and render an SVG
  sparkline in the job card. Pure frontend work.

- **CSV migration report.** Per-job "what got copied" log:
  path, size, mtime, status. juicefs sync has --metrics-prefix
  and stderr lines already include this; capture into the job's
  job.csv during run, expose as GET /api/jobs/{id}/report.csv.

- **Multi-job concurrency knob.** JobManager today is strictly
  single-worker. Add MaxConcurrent (default 1) so prosumers with
  fast NAS can run 2-3 migrations in parallel. Watch out for the
  per-job Redis lock semantics of juicefs sync at higher counts.

**Tier 4 — moonshot (someday):**

- **Source thumbnail previews** for media files in the source browser
  (ffmpeg/imagemagick sidecar, cached at /tmp/thumb-<sha>.jpg).
- **AI-assisted destination structure** — scan source for camera /
  date metadata, suggest folder organization. Out of scope unless
  there's clear user demand.
- **Delete source after verified migration.** Dangerous, gated
  behind double confirmation. Useful only for "decommissioning the
  old NAS" workflows.

**Smallest viable next step:** ship tier-3 job persistence (item #1).
Everything else in tier 3 depends on durable job state.

---

## P2 — user-vision deliverables (depend on P0)

### Dropbox-style write model
**Status:** designed but not deployed. Blocked on QA-34.

**Scope** (3 separable commits):
1. **JuiceFS config tuning** — once QA-34 is closed, evaluate whether `--buffer-size = cache-size` is feasible, or pick a sane "buffer = N × typical-render-size" middle ground based on real measurement.
2. **Metrics surface** — wire JuiceFS prometheus `juicefs_staging_block_bytes` through JM's `/cache-status`. Already partially in place (jmctl can scrape).
3. **Popover UI + quit-protection** — pending-upload section ("Background upload: 4.2 GB pending · ~3 min remaining"), menu-bar indicator state for "upload in progress," confirm-on-Quit when dirty bytes > 0.

**Smallest viable next step (post-QA-34):** ship JuiceFS config change + measure. UI work waits until the underlying numbers are stable enough to render.

---

## P3 — architectural / future

### Unified pin-aware cache truth
**Status:** future ADR.

The QA-30 three-layer guard (Layer C pin guard, Layer A FUSE Lstat verify, Layer B recently-evicted shadow) treats a symptom: pin store and metadata store both hold truth about "what's cached" without a shared invariant. Every new bug class in this area (QA-32, QA-34, the pin-coverage-verify STALE storm) traces back to that asymmetry. Right long-term fix is one source of truth.

**Trigger:** when adding a fourth layer to the guard would be required, that's the signal we've over-patched and need the redesign.

---

## Spool (Option 2) — wired live 2026-05-28; backend code review follow-ups

A backend code review found the spool was **inert on the live write path**: the
write branch was gated on `os.O_CREATE`, but NFS CREATE calls `fs.Create` and
NFS WRITE calls `OpenFile(O_RDWR)` — neither sets `O_CREATE`, so with
`JM_SPOOL_ENABLE=1` real Finder writes still bypassed the spool entirely
(Finding 1). Fixed this cycle: `juiceFS.Create` routes new files to the spool;
`juiceFS.OpenFile` routes WRITE RPCs to the spool for any path with an active
entry; per-RPC `Close` no longer finalizes (NFS closes after every RPC) —
finalize is driven by an idle sweeper, mirroring `FDPool.evictLoop`. Certified
by `TestSpoolInterceptsNFSCreateWriteSequence` (drives the real CREATE+WRITE
call shapes; red pre-fix, green post-fix). Also fixed in the same pass:
reopen-of-finalized-entry write error (Finding 2, via OpenWrite block-wait) and
`MarkDrainComplete` un-index-before-remove ordering (Finding 3).

### P1 — Server-side ZFS volume growing ~1 TB/month with no activity
**Symptom (user, 2026-05-28):** the TrueNAS box hosting JuiceMount's MinIO/JuiceFS
backend grows up to ~1 TB/month even when idle — a "unique write-size problem."
**Likely causes (cheapest first):** (1) JuiceFS slice/garbage accumulation —
chunks not reclaimed after rewrites/deletes; (2) writeback rewriting whole
chunks on small edits (write amplification); (3) ZFS snapshots retaining
churned blocks; (4) orphaned `delSlices`/`delfiles` not GC'd (cf. QA-34's Lua
key class). **Smallest viable next step (server-side):** compare `juicefs info`
logical size vs MinIO bucket size vs `zfs list -o used,logicalused`; run
`juicefs gc` (dry-run) and `zfs list -t snapshot`; correlate growth windows
with write activity. Separate from the client-side spool fix — file under its
own investigation.

### P2 — Spool: post-crash-recovery can still dup-drain to one dest (Finding 4 residual)
`RecoverOnBoot` re-accounts `ready`/`draining` rows but does NOT repopulate the
in-memory index. So a same-path rewrite AFTER a crash (before the recovered
entry drains) misses `LookupActive`, creates a second spool entry, and both
drain to the same FUSE dest. The normal (non-recovery) path is safe — OpenWrite
block-waits on a still-draining entry. **Next step:** repopulate the index for
`ready`/`draining` rows in `RecoverOnBoot` so OpenWrite's block-wait sees them,
or serialize drains per `nfs_path` in the drainer. Rare (needs crash + immediate
same-path rewrite).

### P2 — Spool: in-flight truncate hits narrow `spoolWriteFile.Truncate` (Finding 6-adjacent)
Now that the spool is live, a SETATTR size-change (truncate) on a file still in
the spool routes to `spoolWriteFile.Truncate`, which only supports
truncate-to-0-on-fresh. Arbitrary truncate of an in-flight spooled file errors.
Common Finder copy flow is unaffected (no mid-write truncate). **Next step:**
support `ftruncate` on the on-disk spool file (adjust `writtenEnd` +
invalidate the streaming hash), or finalize-and-fall-back.

### P2 — Spool: error→NFS-status mapping + atomic-save (temp+rename)
`ErrSpoolFull`/`ErrSpoolBusy` currently surface as generic `OpenFile` errors
(onWrite maps them to NFS3ERR_ACCESS) rather than NOSPC / JUKEBOX(DELAY).
Separately, an atomic-save app that writes a temp file then `rename()`s it will
hit `juiceFS.Rename`, which `os.Rename`s on FUSE — but an undrained spool temp
isn't in FUSE yet, so the rename fails. **Next step:** map the spool errors to
proper NFS statuses; make `juiceFS.Rename` finalize+drain (or wait for) a
spool entry on the old path before renaming.

### P3 — Spool: `writeSizes` map grows unbounded (Finding 5, pre-existing)
`handler.writeSizes` is written per WRITE and never pruned (sticky by QA-16
design). A long-uptime, heavy-write session leaks one entry per distinct path
ever written. Not spool-specific but worsened by spool workloads. **Next step:**
lazily prune a path's entry once its size is confirmed in SQLite and no active
writer holds it (bounded sweep).

### ✓ FIXED 2026-05-29 — `busy_timeout` not applied to all pooled connections
Surfaced by the spool concurrent-drain integration test. `metadata.Open` set
`PRAGMA busy_timeout=30000` via `db.Exec`, which configures only ONE of the 8
pooled connections. The entries store hid this via its single `writeMu`; the
spool store's independent `writeMu` did not, so a spool write + an entries
write on two different pooled connections → `SQLITE_BUSY` immediately (no
wait) under the drainer's 4 concurrent workers. Fix: `busy_timeout` moved to
the DSN (`?_pragma=busy_timeout(30000)`), which modernc applies to EVERY
connection. App-wide improvement; metadata + spool tests green under `-race`.

### P3 — `metadata.Store.UpdateSize` mutates the cached `*Entry` in place (data race)
`-race` caught it via the spool test: `UpdateSize` writes `e.Size`/`e.Mtime`
under `s.mu` (store.go:731), but `juiceFS.Stat` reads `e.Size` and does
`clone := *e` WITHOUT `s.mu` (handler.go:976). Write-under-lock + read-without-
lock is a race. **Pre-existing** — the legacy `writeFile.Close` → `UpdateSize`
path hits it too; my `onSpoolDrained` calling `UpdateSize` from the drainer's
worker pool (concurrent with NFS Stats) just makes it reliably reachable.
**Effectively benign in production** (aligned int64 reads are atomic on
arm64/amd64, so no torn value — worst case a momentarily-inconsistent FileInfo
that self-corrects), but it's real and `-race`-flagged. **Fix options:** (a)
copy-on-write in `UpdateSize` — build a new `*Entry`, mutate the copy, swap it
into pathCache/inodeCache/childrenIdx under `s.mu` so lock-free readers holding
the old pointer never observe a mutation; or (b) a `LookupByPathSnapshot` that
returns a value copy under `s.mu.RLock`, used by `Stat`. The integration test
serializes around it (reads only when the drainer is idle) so it stays green;
this entry tracks the proper fix.

### Validation owed — enable `JM_SPOOL_ENABLE=1` end-to-end
The fix is unit-certified at the handler↔spool seam. Still owed: the deferred
live soak — real NFS mount + FUSE + MinIO over Tailscale/LAN, confirming the
2 GB Finder copy acks fast, the menu-bar/Manager surfaces pending count, and
the drainer empties at MinIO pace. This is the original Slice acceptance test
(`scripts/qa-suite/30-spool-soak.sh`).

---

## How to use this doc

- When closing a bug, **move the entry to `STATE.md`** (with closure date + commit SHA) and delete it from here.
- When finding a new bug, add it here first (P-tier + "smallest viable next step"), then start work.
- When the doc accumulates more than ~10 open items, that's the signal we're falling behind on triage and a focused loop is warranted.
