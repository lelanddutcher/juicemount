# REVERT LOG — cellular/WAN tuning changes

Every behavioral change made while tuning for the flaky, metered **cellular** link, with
exactly how to undo it before/after returning to 10GbE. These changes are tuned for a slow,
flaky, metered link; the same change can behave differently (or badly) on a fast LAN. This
log exists so we can revert cleanly if any of it causes havoc on 10GbE.

## TL;DR — the escape hatches (no rebuild needed for #1)

1. **`JM_NET_ADAPTIVE=0`** (env, restart the app) — MASTER KILL-SWITCH. Pins the link class
   to `medium`, which makes EVERY adaptive consumer fall back to the **historical hard-coded
   defaults** (server readahead 8 blocks / seq 3 / 4 workers; juicefs `--buffer-size 4096
   --prefetch 3`; NFS-client `readahead=16`). One variable restores byte-for-byte original
   behavior. Set it in the app's launch environment.
   - `JM_NET_FORCE_CLASS=medium` does the same (pins class=medium).
2. **Full revert to pre-tuning baseline:** `git checkout main` (commit **63bae1c**) — the
   clean fallback with none of the #16 / nav-tuning work. Or `git revert` the named commits
   below on `claude/cache-tuning`.

## Baseline

- **`main` = 63bae1c** — clean fallback, none of the adaptive-link or nav-sluggishness work.
- All tuning lives on branch **`claude/cache-tuning`**.

## ⚠️ The 10GbE risk to know about

The adaptive system classifies the link and on a **fast** link (10GbE) engages policies that
are MORE aggressive than the historical defaults AND are **NOT YET VALIDATED on 10GbE**:
- server `ReadaheadManager`: **16 blocks (64 MB) / 8 workers** (vs historical 8/4)
- juicefs mount: **`--prefetch 8`** (vs historical 3)
- NFS-client readahead: **unchanged at 16** on fast (no risk there)

So **returning to 10GbE does NOT auto-restore historical behavior** — it engages the untested
`fast` policies (this was the intended 10GbE "dial-up" experiment for the ~3-of-10 Gbit/s
starvation). If they cause trouble on 10GbE, set **`JM_NET_ADAPTIVE=0`** to get historical
behavior, then validate the fast policies deliberately.

---

## Changes (chronological, branch `claude/cache-tuning` on top of 63bae1c)

### C1 — netprofile link estimator + adaptive server ReadaheadManager
- **Commits:** `b32a9f5` (feat), `b32a9f5`..; master switch added in this change set.
- **Files:** `internal/netprofile/profile.go` (new), `nfs/readahead.go`, `nfs/handler.go`,
  `health/reachability.go` (RTT observer), `internal/metrics/metrics.go` (/metrics network{}),
  `bridge/cbridge.go` (wiring).
- **What:** server-side prefetch (depth/width/enable) now scales with measured link class.
- **Gating:** class-gated. `medium` == historical (8 blocks / seq 3 / 4 workers). `slow` →
  2 blocks/2 workers; `metered` → DISABLED; `fast` → 16 blocks / 8 workers.
- **10GbE risk:** `fast` policy is MORE aggressive than historical — UNVALIDATED on 10GbE.
- **Revert:** `JM_NET_ADAPTIVE=0` (→ medium/historical), or revert the commit.

### C2 — adaptive juicefs mount flags + read-path bandwidth sampling
- **Commit:** `0b1243f`.
- **Files:** `internal/netprofile/profile.go` (JuiceFS policy), `health/fuse.go` (one-shot RTT
  probe at mount → picks `--buffer-size`/`--prefetch`), `nfs/handler.go` (ObserveThroughput).
- **What:** juicefs `--buffer-size`/`--prefetch` chosen from link class at MOUNT time.
- **Gating:** class-gated. `medium` == historical (`4096`/`3`). `slow` → `1024`/`1`;
  `metered` → `512`/`0`; `fast` → `4096`/`8`.
- **10GbE risk:** `fast` uses `--prefetch 8` (vs 3) — UNVALIDATED on 10GbE. Read-path sampling
  is passive (no behavior change).
- **Revert:** `JM_NET_ADAPTIVE=0` (→ medium/`4096`/`3`), or revert the commit. NOTE: mount
  flags are set at mount time, so a class/env change needs an app restart (re-mount) to apply.

### C3 — link-aware NFS-client readahead
- **Commit:** `b26908b`.
- **Files:** `internal/netprofile/profile.go` (NFSReadahead), `bridge/cbridge.go`
  (`nfsMountOpts` uses it).
- **What:** NFS mount `readahead=` chosen from link class.
- **Gating:** class-gated, only ever LOWERS from 16. `slow` → 4; `metered` → 2;
  `medium`+`fast` → **16 (unchanged)**.
- **10GbE risk:** NONE — fast/medium keep the validated 16. (Also: 16 is the concurrent-read
  truncation mitigation #18/#19; we never raise it.)
- **Revert:** `JM_NET_ADAPTIVE=0` (→ 16), or revert the commit. Set at mount time → restart to apply.

### C4 — bandwidth estimate = aggregate bytes / wall-time (windowed)
- **Commit:** `0c3ac72`.
- **Files:** `internal/netprofile/profile.go`.
- **What:** how the BW number is COMPUTED (windowed aggregate vs per-read). Affects only the
  measured value that feeds classification + /metrics.
- **10GbE risk:** low — it's a measurement refinement; could shift classification thresholds
  but `medium`==historical is unaffected.
- **Revert:** `JM_NET_ADAPTIVE=0` makes it irrelevant (class pinned), or revert the commit.

### C5 — master kill-switch `JM_NET_ADAPTIVE=0`
- **Files:** `internal/netprofile/profile.go` (`Default()`).
- **What:** the single env var that pins class=medium everywhere. Pure safety; no behavior
  change unless set.

---

## How to verify a revert worked

- `curl -s http://127.0.0.1:11050/metrics | python3 -c 'import sys,json;print(json.load(sys.stdin)["network"])'`
  → with `JM_NET_ADAPTIVE=0` the readahead policy fields should read the historical
  `blocks=8, seq=3, workers=4` regardless of link.
- juicefs mount log line should show `--buffer-size 4096 --prefetch 3`.
- `nfsstat -m /Volumes/zpool-dev | grep -o 'readahead=[0-9]*'` → `readahead=16`.

---

## Nav-sluggishness fixes (root-caused via workflow wf_80bcef8b-876, 2026-06-16)

Root cause (verified against juicemount.log): the DOMINANT driver of "loading bar that never
cures" is NOT the reconcile SCAN — it is the **FUSE-stale watchdog SIGKILLing a live-but-slow
juicefs** (~469 escalations / day on 06-15, ~6% remount success, FUSE dead 30-45s per cycle).
The reconcile SCAN storm is a secondary, co-symptom (metered-data burn + evening contention).
Both fixed below, class-gated.

### R1 — watchdog: don't SIGKILL a slow-but-alive juicefs on a slow link
- **Files:** `health/fuse.go` — `staleEscalateTicks()`, `confirmProbeTimeout()` helpers +
  `fuseWatchdogLinkAware` var; call sites in `monitorLoop` (the escalate-tick check + the
  confirm-probe). Test: `health/fuse_linkaware_test.go`.
- **What:** the escalation-to-remount threshold and the "is it wedged or just slow" confirm
  probe now scale with link class. metered: 27 ticks (~270s) + 90s confirm; slow: 18 ticks
  (~180s) + 45s confirm; **medium/fast: 9 ticks (~90s) + 25s confirm — UNCHANGED.**
- **Why:** on cellular the 5s readdir stale-probe times out tick after tick (legit-slow, not
  wedged) and the 25s confirm probe also false-fails, so the watchdog killed a live juicefs.
- **10GbE risk:** NONE — medium/fast are byte-for-byte the historical 9/25s (locked by the
  test). The only metered/slow trade-off is that a GENUINELY-wedged mount on cellular recovers
  later (270s vs 90s); acceptable since false-kills were ~469/day and true wedges are rare.
- **Revert:** `JM_FUSE_WATCHDOG_LINKAWARE=0` (fixed 9/25s), or `JM_NET_ADAPTIVE=0`
  (class=medium → historical), or revert the commit. NOTE: the menu-bar "stale" indicator
  still shows during cellular slowness (honest) — we only changed the destructive escalation.

### R2 — reconcile: debounce flap-triggered full SCANs
- **Files:** `metadata/redis.go` — `flapDebounceInterval()` + `reconcileFlapDebounce` var +
  `lastSyncStartedAt` field; gate in the `reconcileLoop` `syncNowCh` case.
- **What:** a NETWORK-CHANGE-triggered reconcile is suppressed if a sync started/finished
  within the class-aware window (metered 60s, slow 30s, **medium/fast 0 = no debounce**). The
  periodic ticker is untouched, so any real drift is still caught within the cadence.
- **Why:** the flaky cellular link flaps constantly and each recovery fired a full 87-178s
  SCAN that ~93% of the time found zero changes — back-to-back, burning metered data.
- **10GbE risk:** NONE — window is 0 on medium/fast, so LAN fires on every flap exactly as
  before. Correctness: only delays a redundant SCAN; never skips reconciliation (the ticker
  still runs); cannot show stale data beyond the existing cadence latency.
- **Revert:** `JM_RECONCILE_FLAP_DEBOUNCE=0`, or `JM_NET_ADAPTIVE=0` (window→0), or revert the commit.

### Deferred (P1, not yet implemented — larger/riskier, see workflow plan)
- Skip-if-fresh / incremental reconcile (avoid the ~93% zero-change full SCANs). HIGH
  correctness-attention (prune ladder) — needs a periodic full-SCAN safety floor.
- Cold-readdir / per-file phantom-GETATTR decoupling on degraded/metered (faster mirror-miss).
- Pace the SCAN between batches on metered (defense-in-depth).
