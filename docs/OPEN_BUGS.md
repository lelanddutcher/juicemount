# Open bugs — triage queue

**Purpose.** `docs/STATE.md` is the chronological closure log of QA findings (good for "what did we ship and when"). This doc is the forward-looking triage list — what's still open, ranked by user-impact, with the smallest-viable next step for each. Maintained as we find bugs and pruned as we close them.

**Snapshot:** 2026-05-25 after commit `399a56f` (QA-32 + QA-33 shipped).

---

## P0 — blocking the user vision

### QA-34 — FUSE mount disappears under sustained write
**Status:** open. Reproduced cleanly.

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

### QA-32 follow-up — 1,577 `FromHandle STALE` events during `pin-coverage-verify`
**Status:** open. Diagnosed but not fully traced.

**Symptom:** running `scripts/qa-suite/11-workloads/pin-coverage-verify.sh` on the post-QA-31 build produces 1,577 `FromHandle STALE` events in 60 s, all on real (non-synthetic) inodes. Layer B recovered the MP4 inode `0x1aa63` once but STALE re-fired 25 more times for the same inode. `pathCache` and `inodeCache` size delta grew from baseline 5 → 147 during the run (alias-inode accumulation past Layer B).

**Why it matters:** when a user hits "Sync Now" with pinned files, the verify path triggers what looks like a normal-ish surface — except for ~26 STALE events/sec, which DaVinci Resolve sees as "file flickered offline" 26 times per second during a scrub.

**Hypotheses:**
1. The verify path re-prefetches via FUSE while metadata sync is concurrent. The two paths race on the same inodes. evictOldest may be re-rebuilding cache from pathCache and dropping the QA-28 redirect aliases.
2. Spotlight + Finder are issuing background NFS lookups during the verify, hitting handles that were just transiently invalidated by the concurrent re-sync.

**Smallest viable next step:** instrument the QA-28 redirect path with a counter (`inode_redirects_total`, `alias_inode_drops_total`). Re-run pin-coverage-verify with the counter visible. If the drop count climbs in lockstep with STALE events, we've found the leak. Fix is to extend Layer B's shadow map to also park aliases on eviction.

### Watchdog tolerance edge case
**Status:** open follow-up to QA-33.

**Symptom:** QA-33 requires 3 consecutive 10-second-interval unhealthy checks before remounting (30 s threshold). For a juicefs daemon that's truly hung but with the PID still alive (process exists, but FUSE handle is dead), this means 30 s of dead mount before recovery. The fast-path "is PID alive" only catches PID-gone cases.

**Smallest viable next step:** add a second fast-path: if `isMountedLocked()` reports unhealthy AND `mount | grep fuse-internal` returns nothing (i.e., kernel says no FUSE here), remount immediately regardless of consecutive-failure count. PID-alive-but-mount-gone is the macOS-FUSE-kernel-killed case.

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
