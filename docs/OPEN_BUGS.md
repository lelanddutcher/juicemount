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

## How to use this doc

- When closing a bug, **move the entry to `STATE.md`** (with closure date + commit SHA) and delete it from here.
- When finding a new bug, add it here first (P-tier + "smallest viable next step"), then start work.
- When the doc accumulates more than ~10 open items, that's the signal we're falling behind on triage and a focused loop is warranted.
