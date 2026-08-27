# JuiceMount REAL-FINDER Release Testing Battery — Methodology

This document is the source of truth for the JuiceMount release battery. It is
written so a **future session (or a fresh agent) can run the full battery
correctly from this doc alone**, even after a context compaction. If anything
below conflicts with a vague memory of "just run a `cp` loop" — this doc wins.

Location: `test/qa-battery/` (rc-keyspace worktree).
Shared library: `lib.sh`. Orchestrator: `run-all.sh`.

---

## Why this battery exists (read this first)

A prior agent repeatedly collapsed this battery to a synthetic `cp`/`dd`/`ditto`
loop after a context compaction — and **missed real bugs every time**. Synthetic
copies false-green: they never exercise the Finder→NFS path users actually hit
(LOOKUP / CREATE / SETATTR / WRITE / READ + `._AppleDouble` sidecars + xattr /
resource forks + symlink/bundle semantics). The whole point of this battery is
that **every write is driven by a REAL macOS Finder operation via `osascript`**
(`tell application "Finder" to duplicate …`). `md5` is used **only** to verify
integrity after the fact — never to drive a copy.

---

## The 6 principles (the bar every category is held to)

1. **REAL Finder ops, not synthetic copies.** Every write is a real Finder
   duplicate/move/delete via `osascript` (`qa_finder_copy` / `qa_finder_move` /
   `qa_finder_delete` / `qa_finder_cancel_copy`). Synthetic `cp`/`dd`/`ditto`
   are banned as copy drivers — they skip the NFS path and the `._AppleDouble` /
   xattr-fork traffic that surfaces the real bugs. Synthetic generation is
   confined to **staging source bytes off the mount** (`qa_stage_file` /
   `qa_stage_tree`).

2. **Full chain of custody, end to end.** For every file the chain is:
   Finder write → NFS handler (CREATE/WRITE/SETATTR) → write spool →
   drainer (bounded durable checkpoints + streaming SHA verification) → JuiceFS backend →
   **readback** (`md5` == source). The drain gate is **mandatory before
   verifying**: `qa_wait_drain` blocks until `pending_files == 0 AND
   in_progress == 0` so custody reads the at-rest backend copy, never the spool
   or cache. `qa_verify_custody` passes **only** when `MISSING == 0 AND
   WRONG == 0` (zero data loss, zero corruption).

3. **Zero random Finder errors gate.** No user-visible failure may appear in the
   JuiceMount log during the run. Gated signatures (grounded in `nfs/handler.go`):
   `FromHandle STALE`, `purging phantom`, `100070`/`NFS3ERR_STALE`, `100060`,
   `-48`/`already an item`, `-36`/`ioErr`, `-5000`/`afpAccessDenied`, and any
   permission denial. Each test brackets its window with `qa_log_mark` …
   `qa_error_scan`; the orchestrator additionally re-scans the **entire run
   window** for the authoritative cross-cutting tally. Every occurrence names the
   offending path (pulled from the dumped `errscan-*.txt` artifact). Known-open
   edges (see below) are asserted explicitly (`qa_warn`, never silently masked)
   and driven to zero — they are NOT whitelisted in the release gate.

4. **Snappiness bar (the user must NEVER see the Finder spinner).** Opening any
   directory on the mount must return its listing from the **local metadata DB**
   in `< $QA_SNAPPY_MS` (default **200ms**), regardless of network state, and
   must never require a backend round-trip when the DB already has the entries.
   Measured with `qa_dir_listing_ms` (`ls -1f`, sort/stat disabled, so it times
   the readdir path, not a stat-storm). A listing `>= QA_SNAPPY_MS` is a hard
   FAIL that names the directory.

5. **Offline / spool-disconnect hardening.** The write spool must keep full
   chain of custody when the backend is unavailable — the field scenario (NAS
   asleep, Wi-Fi drops, laptop carried out of range mid-copy). The control plane
   `GET /offline?on=1|0` forces offline / back-online; the spool buffers writes
   with NO drain while offline, then drains losslessly on reconnect. Cached
   directory listings must stay snappy while offline (principle #4 holds with no
   network).

6. **Non-destructive, always.** Every script copies into a **unique timestamped
   dest** (`qa_unique_tag` → `$MOUNT/JM_RELEASE_BATTERY/QA_<epoch>_<pid>_<rand>/…`)
   and cleans up **only** its own subtree (`qa_cleanup` guard refuses to `rm`
   anything outside `$MOUNT/JM_RELEASE_BATTERY` or `$QA_STAGE`). The battery
   never writes to or deletes the user's real folders. Safe to run repeatedly.

---

## Engineering constraints (do not violate)

- **bash 3.2-safe** (macOS default `/bin/bash` is 3.2.57): no associative
  arrays, no `mapfile`, no `${var^^}`, no `declare -A`. Iterate
  newline-delimited streams.
- **No GNU `timeout`.** Bounded probes use `qa_timeout` (perl `alarm`).
- The only place the battery sleeps is bounded copy-cancel grace and drain
  polling — both via perl, both tiny.
- **Mount safety:** do NOT cache-clear / redeploy-churn a live mount mid-battery;
  that can wedge macFUSE into watchdog-thrash needing a reboot. Run the battery
  against a settled, healthy mount.

---

## Category scripts — what each covers + pass criteria

| # | Script | Covers | Pass criteria |
|---|--------|--------|---------------|
| 01 | `01-file-types.sh` | One real Finder copy of a folder holding **every realistic item type**: media (`.mov/.mp4/.wav/.aac`), docs (`.pdf/.jpg/.png`), archives (`.zip/.dmg`), a `.app` bundle, a `*.framework` with `Versions/A` + `Versions/Current` symlinks, a relative symlink, a dangling symlink, files with xattrs/resource forks, `uchg`/ACL items. | Per-item custody md5 == source for regular files; `Versions/Current -> A` stays a symlink and resolves; top-level `Sample` shim resolves through it to the real binary; relative-link target string preserved; dangling link stays dangling; perms/`uchg`/ACL/xattr-sidecar preserved. Zero gated errors. |
| 02 | `02-file-sizes.sh` | **Size spectrum** through the full spool/drain chain: `zero` (0B), `onebyte`, `kb` (4KiB), `mb5`, `mb100`, `gb1` (>1GB). | Real Finder duplicate drives every copy; full custody per size; **no torn/partial-size read** during the drain tail (landed size only ever equals the final source size, polled during drain); landed size == source size exactly. Zero gated errors. |
| 03 | `03-deep-wide-trees.sh` | Tree shapes that stress LOOKUP/CREATE/readdir at the edges: **DEEP** (14-level chain), **WIDE** (1200 files in one dir), **MIXED** (3200 files over 64 dirs, with mid-copy navigation probes). | Whole-folder real Finder duplicate; full drain; total file count + full md5 custody; intermediate dirs stay navigable during the in-flight MIXED copy (worst observed listing latency recorded). Zero gated errors. |
| 04 | `04-unicode-names.sh` | **Pathological filenames**: emoji, NFC **and** NFD accents, CJK, RTL/bidi marks, 255-byte max leaf, spaces/`&`/`;`/parens, leading dot, trailing space. | Every name lands, is findable, drains, reads back byte-identical. No normalization-induced collision/loss; no `-36/-43/-1407/STALE/phantom`. The NFC-vs-NFD lookup gap is asserted as its own explicit `qa_warn` (driven to zero, not masked). |
| 05 | `05-concurrent-copies.sh` | **Genuine concurrency**: 3–4 simultaneous real Finder duplicates (each own dest+manifest) + 1 concurrent large-file readback loop + concurrent dir-nav sampling, all overlapping. | One combined drain, per-dest custody (`MISSING==0 && WRONG==0` each), single error scan over the whole concurrency window. No `100060` retry-storm, no shared-handle STALE, listings stay `< QA_SNAPPY_MS` and never beachball while the spool is hot. |
| 06 | `06-cancel-retry.sh` | **Interruption resilience**: (a) cancel mid-flight ×N then a clean retry of the same source; (b) back-to-back copy to the SAME dest name after `rm -rf` (directory delete-lag `-48`). | A cancelled copy strands **no** handle / phantom / STALE; mount stays healthy; the clean retry succeeds with full custody. The back-to-back same-dest case completes; the single `-48` delete-lag is the named known-open edge (driven to zero). |
| 07 | `07-offline-spool.sh` | **Write-spool hardening**: offline toggle buffers writes with no drain; a real mid-copy disconnect; stall/backpressure when the offline buffer fills. | Full custody after reconnect (lossless drain); spool fields (`pending/in_progress/failed_files/quarantined/stall_waiters/offline_buffer_full`) behave per `nfs/spool_status.go`; cached listings stay snappy while offline; nothing quarantined that shouldn't be. Zero gated errors. |
| 08 | `08-chain-of-custody.sh` | **Dedicated end-to-end custody stress** (long-tail backend readback) for a large mixed corpus incl. a >1GB camera-original-scale file. | REAL Finder copy → spool transit observed → mandatory full drain → two independent backend readback sweeps md5 == source (MISSING==0 && WRONG==0) → zero gated Finder errors. |
| 09 | `09-finder-snappiness.sh` | **Directory-open snappiness** across dir sizes (10/100/1000/5000 files, with real `._AppleDouble` sidecars): COLD (first listing), WARM (re-list from DB), OFFLINE (DB-served with control plane offline), DRAINING (unrelated cached dir while a copy drains). | Every local-DB-resident listing `< QA_SNAPPY_MS`. Key metric = worst observed `MS` vs budget (from `latency-table.tsv`). Any listing `>=` budget is a hard FAIL naming the dir. |
| 10 | `10-index-stability.sh` | **Stability during a metadata index reconcile** (the Chrome file-picker-froze-all-of-Finder symptom). Also verifies the build uses **keyspace-PUSH** (incremental Redis deltas) and is NOT doing a ~30s full SCAN re-pull when it shouldn't. | Listings stay snappy and a parallel Finder copy keeps progressing while a reconcile runs (`GET /activity` `busy`/`reconcile`); log shows keyspace-push events, no spurious "promoting to full SCAN". Full custody on the parallel copy; zero gated errors. |
| 11 | `11-drain-latency.sh` | **Directory-open + Stat latency UNDER sustained spool-drain load** (the "Finder spinner while a copy drains" regression). Pre-caches an unrelated tree, starts a sustained multi-GB Finder copy to keep the drainer's durable checkpoints hot, then loops measuring dir-open (`qa_dir_listing_ms`) AND per-file Stat/GETATTR on the cached tree for the whole drain window, recording p50/p99. | Across the entire drain window, **p99 of both dir-open and Stat `< QA_SNAPPY_MS`** (any single sample over budget is a hard FAIL naming it). Proves the async phantom-purge + isolated prefetch gate keep the synchronous Stat hot path off FUSE so a hot drain can't head-of-line-block Finder's NFS connection. LOAD-payload custody intact (zero data loss under load); zero gated errors. Key metric in `drain-latency-table.tsv`. |

Category labels in the table match the `VERDICT:` line each script emits
(`VERDICT: PASS <label>` / `VERDICT: FAIL <label>: <reason>`). Scripts always
`exit 0`; the VERDICT line + per-category `.summary` carry the real result.

---

## Known-open edges being driven to zero (assert-on, do NOT mask)

These are real, narrow, currently-nonzero edges. They are asserted explicitly
(as `qa_warn` inside their category) so they stay visible, but they are **NOT**
whitelisted in the release gate — the gate's cross-cutting tally counts them.
The release is GREEN only when they are **actually zero on the run**.

1. **Residual ~1 `FromHandle STALE` per ~960 `._`-heavy files.** Surfaces on the
   deep/wide `._AppleDouble`-dense trees (cat 03/05). Drive to zero; if one
   appears, the gate goes RED and the offending path is in
   `errscan-run-aggregate.txt`.
2. **Back-to-back same-dest `-48` ("already an item") delete-lag.** Copy → drain
   → `rm -rf` the landed dir → immediately re-copy the SAME leaf name; the
   directory delete hasn't fully settled, so the re-copy can see one `-48`
   (cat 06). Drive to zero.
3. **NFC-vs-NFD lookup gap.** The macOS NFS path NFD-normalizes leaf names at
   rest; an explicit NFC-spelled lookup of an NFD-stored leaf can miss unless the
   caller normalizes first (cat 04). Asserted as its own `qa_warn`. Drive to zero.

---

## How to run

All commands run from `test/qa-battery/`. Defaults autodetect the live mount and
control plane (`lib.sh`): `MOUNT` falls back to the first `127.0.0.1:/` NFS
mount, `CP_ADDR=127.0.0.1:11050`, `JM_LOG=~/Library/Logs/JuiceMount/juicemount.log`,
`QA_SNAPPY_MS=200`.

```sh
# Full battery, every category in numeric order:
./run-all.sh

# A subset (by number, zero-padded or not, or by full name):
./run-all.sh 04 07
./run-all.sh 9 10
./run-all.sh 01-file-types 09-finder-snappiness

# Tighten the snappiness budget for the run:
QA_SNAPPY_MS=150 ./run-all.sh

# Point at a non-default mount / control plane:
MOUNT=/Volumes/zpool CP_ADDR=127.0.0.1:11050 ./run-all.sh
```

Run a **single category script directly** (it sources `lib.sh`, runs its own
preflight, and prints its own `VERDICT:` line):

```sh
./03-deep-wide-trees.sh
```

### What `run-all.sh` does (gate logic)

1. **Precheck** (fail fast, clear message): control plane reachable +
   `healthy:true`, `MOUNT` is an active NFS mount, **spool idle at start**
   (waits up to `QA_DRAIN_TIMEOUT` for it to settle so custody attribution is
   clean). If precheck fails it writes `RELEASE GATE: RED (precheck failed)` and
   exits 1 without running anything.
2. **Marks the JuiceMount log once** (`qa_log_mark`) so the end-of-run scan
   covers the **entire** battery window.
3. Runs each selected `NN-*.sh` **in order**, tees its console to
   `<artifacts>/NN.console.log`, and captures its last `VERDICT:` line. A missing
   selected script is recorded as `SKIP(missing)` and keeps the gate RED.
4. **Cross-cutting error tally**: re-scans the JuiceMount log from the start mark
   over the whole run (`qa_error_scan`) for all gated signatures — this is the
   authoritative "zero random Finder errors" number, independent of how each
   category chose to `warn` vs `fail` its own edges.
5. **Snappiness gate**: scans every category console for a snappiness `[FAIL]`
   marker and cross-checks cat 09's `latency-table.tsv` for any `MS >= budget`.
6. Prints the **RESULT TABLE** (category | PASS/FAIL | key metric), the
   cross-cutting tally, and the overall **RELEASE GATE** verdict to stdout and to
   `<artifacts>/RESULT.txt`.

**RELEASE GATE is GREEN only if ALL of:**
- every category that ran **PASSed**, AND
- the cross-cutting error tally across the whole run is **0**
  (STALE + phantom + 100070 + 100060 + -48 + -36 + -5000 + permission), AND
- **snappiness held** (no listing `>= QA_SNAPPY_MS`), AND
- no selected category was **missing / not-run**.

Otherwise the gate is **RED** with an itemized reason list, and `run-all.sh`
exits non-zero (so CI can gate on it directly).

### Artifacts

Everything lands under one run dir (`$QA_ARTIFACTS`, default
`/tmp/jm-battery-run-<timestamp>-<pid>/`):
- `RESULT.txt` — the final result table + gate verdict.
- `NN.console.log` — full console of each category.
- `NN-<label>/.summary` — per-category pass/fail/warn counts + elapsed.
- `NN-<label>/errscan-*.txt` — matched error lines **with paths** for attribution.
- `09-finder-snappiness/latency-table.tsv` — per-phase listing latencies.
- `_run_aggregate/errscan-run-aggregate.txt` — the whole-run gated-error dump
  (the file to open first when the gate is RED on errors).

---

## BEFORE EVERY RELEASE, RUN THIS

1. Confirm the build under test is the intended RC (keyspace-push enabled; check
   cat 10's push-vs-SCAN assertion). Mount is up, settled, healthy.
2. Make sure the spool is idle and nothing else is hammering the mount (the
   precheck enforces this, but don't fight it with a background copy).
3. From `test/qa-battery/`, run the **full** battery:
   ```sh
   ./run-all.sh
   ```
4. Open `RESULT.txt`. The release is shippable **only** when:
   - `RELEASE GATE: GREEN`, AND
   - the cross-cutting tally line reads all zeros
     (`TOTAL gated errors = 0`), AND
   - `SNAPPINESS: held`, AND
   - no category shows `SKIP(missing)` (notably **08 is authored and must PASS, and
     present** before a green release that includes it).
5. If RED: open `_run_aggregate/errscan-run-aggregate.txt` for the offending
   paths, fix the root cause (do **not** whitelist a known-open edge), and re-run.
   Re-runs are safe (unique dests, self-cleaning).
6. Do NOT cache-clear or redeploy-churn the live mount between re-runs — let it
   settle; a wedged macFUSE needs a reboot, not another run.

---

## Maintenance notes for a future agent

- If you are tempted to "simplify" any category to a `cp`/`dd`/`ditto` loop:
  **don't.** That is the exact regression this battery exists to prevent. Real
  Finder ops or it doesn't count.
- Keep everything bash 3.2-safe and `qa_timeout`-bounded.
- All shared behavior (staging, Finder ops, drain/custody/error/snappiness
  gates, preflight, cleanup) lives in `lib.sh`. Add helpers there, not inline.
- The orchestrator's gate is intentionally strict: a missing script, a single
  gated error, or one slow listing turns it RED. That strictness is the point.
