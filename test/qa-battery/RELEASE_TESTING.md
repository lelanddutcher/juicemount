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
   in_progress == 0`. `qa_verify_custody` passes **only** when `MISSING == 0
   AND WRONG == 0` for mounted readback. Drain plus a mounted hash match is
   necessary, but resident caching can still serve that read; it does not by
   itself prove backend persistence. Compare the retained source manifest to
   an independent backend readback before claiming full custody.

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
   `POST /offline?on=1|0` forces offline / back-online; the spool buffers writes
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
| 03 | `03-deep-wide-trees.sh` | Tree shapes that stress LOOKUP/CREATE/readdir at the edges: **DEEP** (14-level chain), **WIDE** (1200 files in one dir), **MIXED** (3200 files over 64 dirs, with mid-copy navigation probes). | Whole-folder real Finder duplicate; full drain; total file count + full md5 custody; intermediate dirs stay navigable during the in-flight MIXED copy. A persistent probe process times only opendir/readdir/closedir: p99 must remain `< QA_SNAPPY_MS`, no sample may reach `QA_HARD_STALL_MS`, and gated errors remain zero. |
| 04 | `04-unicode-names.sh` | **Pathological filenames**: emoji, NFC **and** NFD accents, CJK, RTL/bidi marks, 255-byte max leaf, spaces/`&`/`;`/parens, leading dot, trailing space. | Every name lands, is findable, drains, reads back byte-identical. No normalization-induced collision/loss; no `-36/-43/-1407/STALE/phantom`. The NFC-vs-NFD lookup gap is asserted as its own explicit `qa_warn` (driven to zero, not masked). |
| 05 | `05-concurrent-copies.sh` | **Genuine concurrency**: 3–4 simultaneous real Finder duplicates (each own dest+manifest) + 1 concurrent large-file readback loop + concurrent dir-nav sampling, all overlapping. | One combined drain, per-dest custody (`MISSING==0 && WRONG==0` each), single error scan over the whole concurrency window. No `100060` retry-storm, no shared-handle STALE, listings stay `< QA_SNAPPY_MS` and never beachball while the spool is hot. |
| 06 | `06-cancel-retry.sh` | **Interruption resilience**: (a) cancel mid-flight ×N then a clean retry of the same source; (b) back-to-back copy to the SAME dest name after `rm -rf` (directory delete-lag `-48`). | A cancelled copy strands **no** handle / phantom / STALE; mount stays healthy; the clean retry succeeds with full custody. The back-to-back same-dest case completes; the single `-48` delete-lag is the named known-open edge (driven to zero). |
| 07 | `07-offline-spool.sh` | **Write-spool hardening**: offline toggle buffers writes with no drain; a real mid-copy disconnect; stall/backpressure when the offline buffer fills. | Full custody after reconnect (lossless drain); spool fields (`pending/in_progress/failed_files/quarantined/stall_waiters/offline_buffer_full`) behave per `nfs/spool_status.go`; cached listings stay snappy while offline; nothing quarantined that shouldn't be. Zero gated errors. |
| 08 | `08-chain-of-custody.sh` | **Dedicated end-to-end custody stress** (long-tail backend readback) for a large mixed corpus incl. a >1GB camera-original-scale file. | REAL Finder copy → spool transit observed → mandatory full drain → two independent backend readback sweeps md5 == source (MISSING==0 && WRONG==0) → zero gated Finder errors. |
| 09 | `09-finder-snappiness.sh` | Finder-populated **directory enumeration** for 10/100/1000/5000 primary files, with separately inventoried AppleDouble/system entries. First-timed (not cold), warm, effective forced-offline, and active-drain-progress brackets. Original Finder replies and worker joins are retained; unknown completion holds the fixture. | Exact primary names/counts, typed monotonic NFS counters, positive drain progress and every valid enumeration `< QA_SNAPPY_MS`. Per-interval JSON and console output label client-cache-only intervals as server-path unproven; `latency-table.tsv` retains the five-column timing format. This is not Finder paint timing or proof of no network; independent backend verification remains a controller gate. |
| 10 | `10-index-stability.sh` | One supported metadata sync with sampled reconcile-active boundaries. Defaults remain20×15×262144B plus2×5×131072B; primary bytes79,953,920. | At least12 valid listings spanning both owned namespaces, all below200ms; actual parallel Finder reply below45s and a covered interval inside its original lifetime. Real keyspace delivery and published-mutation deltas required separately from intentional sync. Unknown or uncovered is RED/HOLD, never warning-based PASS. Backend proof remains an independent controller gate. |
| 11 | `11-drain-latency.sh` | Finder-populated probe (200×4096B) and sustained load (8×40×8MiB), with original copy intent/reply/join custody. Paired enumeration and regular-file stat are retained from load dispatch until positive original reply plus typed settled spool. | At least40 valid pairs active at both spool acquisition boundaries and positive succeeded growth across the covered window. Both whole-window and active-only nearest-rank p99 must be `< QA_SNAPPY_MS` (200ms), every valid sample below `QA_HARD_STALL_MS` (1000ms). No post-drain padding. Deadlines, errors, invalid schemas or missing coverage are RED. Global READDIR/READDIRPLUS and GETATTR brackets distinguish client-cache-only observations; they do not establish exclusive probe attribution, Finder paint or a no-network path. Original operations remain owned through failure; `QA_KEEP_DATA=1` retains evidence. Mounted-byte custody is checked; independent backend readback remains the controller gate. |

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

An explicitly supplied `MOUNT` is used exactly as given, including
`/Volumes/zpool`; an empty explicit value fails before any fallback. Only an
unset `MOUNT` may autodetect a loopback NFS mount. An explicit `QA_POOL` is
honored; otherwise its lazily generated 64 MiB file lives under `QA_STAGE`.
For an isolated persistent staging root, set `QA_STAGE` and `QA_ARTIFACTS` to
owned paths before running the battery.

`QA_KEEP_DATA=1` is an opt-in inspection aid: it skips `lib.sh`'s `qa_cleanup`
and category 04's EXIT-trap offline reset and deletion. The default cleanup
behavior is unchanged. This retention applies only to the library cleanup and
category 04 trap; other categories may have their own cleanup actions. It does
not establish Finder timeout completion, ownership of pending operations, or
custody of a retained mount. Inspect those separately before using retained
data as release evidence.

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
- **snappiness held** (every category-specific latency contract passed: strict
  per-sample where specified, otherwise p99 plus hard-stall ceiling), AND
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

### Category10 owned sync adapter contract

Category10 requires `QA_INDEX_SYNC_DIR`, a precreated literal0700 directory on the same primary device as real HOME and the product metadata directory, under the controller's exact isolated root. It must never be a CACHE/external directory. The controller binds this root to its retained original service process; `/whoami.instance_id` is durable identity, not a process-unique token. QA checks live `/whoami` before any Finder operation and again before its one request, and rechecks identity after completion. No executable command is accepted through this interface.

`arm.json` contains `clock_domain:"posix-clock-monotonic-ns"`, `schema:1`, `method:"sync"`, a32–64lowercase-hex `request_nonce`, `service_instance`, exact `metadata_db_path`, `mount_point`, and `isolation_root`. QA exclusively creates and fsyncs `request.json` once, containing `schema`, `method`, `request_nonce`, `service_instance`, `clock_domain`, plus integer `requested_ns` from `time.clock_gettime_ns(time.CLOCK_MONOTONIC)`. Every adapter/helper cross-process timestamp must use this exact POSIX clock domain. Python `time.monotonic_ns()` is not interchangeable on macOS Python3.9 because its origin can be process-local; a different or absent domain is rejected. The owner must latch that exact request before invoking one supported service `sync`, retain its original socket/thread through uncertainty, and atomically publish `reply.json` only after the original joins. The reply repeats all request fields and adds integer `dispatched_ns`, `completed_ns`, `joined:true`, and exact `text:"ok"`. Timestamps must be ordered. Files are bounded regular no-follow files; no partial file or absence authorizes another request. The directory must be private control storage; independent backend/media payloads do not belong there.

The QA process latches request uncertainty before publication and never resends, deletes, cancels, toggles offline or shuts down on uncertainty. Its terminal receipt is published only after every issued Finder operation and the original sync are known joined. The unchanged custody helper preserves original worker reply timestamps; actual worker `reply_at`, not later parent observation, bounds copy overlap. Wall/monotonic acquisition anchors must have a nonempty common offset interval no wider than1s; copy overlap uses the conservative inner bounds after conversion, and inconsistent clocks reject coverage. `QA_KEEP_DATA=1` retains the fixture. Known failure also retains data; unknown completion keeps the original category process in HOLD.

Measurements are directory-enumeration syscalls with typed reconcile-active observations at both acquisition boundaries and monotonic metric chains including inter-sample gaps. They do not establish continuous/exclusive scan attribution, local SQL contention, WAN behavior, Finder paint or server coverage when the client cache answered. No arbitrary sleep pads coverage and no second sync retries an uncovered window. Root qualification of an existing bounded client-side latency fixture is separate from this source contract; singleton renewal must bypass any simulated delay.


## Strict disposable-fixture execution

For a complete single-device acceptance run, use `QA_STRICT_RELEASE=1` and
`QA_KEEP_DATA=1`. Supply explicit `MOUNT`, `CP_ADDR`, `JM_LOG`, `QA_STAGE` and
`QA_ARTIFACTS` from the already admitted isolated fixture owner. Strict mode
never selects a mount automatically. The owner admits capacity for all retained
sources, destinations and SDK readbacks, and no unrelated Finder copy may be in
progress when the category06 Cmd-. action runs. Keep the original controller
and category handles; do not use an outer timeout or kill a Finder driver.

Also supply `QA_OWNER_HOOK` (an absolute executable path) and
`QA_OWNER_HOOK_SHA256`. This small, independently reviewed fixture-owner adapter
is invoked directly, with no shell evaluation. It must retain typed receipts
and return zero only after these operations have completed:

- `preflight 07`: prove the exact owned transport, unchanged upstream Redis
  lifetime and metadata identity, source/tool binding, no production/foreign
  route control, and a spool cap that the bounded workload can actually reach.
- `transport off` / `transport verify-off`: disconnect only that fixture's
  transport, including existing sessions; retain ordinary connection-failure
  proof. A forced-offline flag alone is insufficient. Do not use pf, routes,
  host-wide networking or SIGSTOP of a process holding singleton renewal.
- `transport on` / `transport verify-on`: restore the same admitted endpoint
  under its owner, retain the new transport lifetime and authenticated readiness,
  and preserve the original upstream metadata. Unknown restoration is HOLD.
- `backend-readback MANIFEST DEST`: admit the exact regular source manifest and
  destination below this fixture's `JM_RELEASE_BATTERY`; use the pinned bundled
  JuiceFS SDK CLI and explicit metadata identity to read the corresponding
  backend path into a fresh private internal directory. Verify every manifest
  entry against the independent bytes, and retain command, original child join,
  full output and source/readback hashes. A second mounted read is insufficient.

The fixture hook is a required execution input, not supplied by a successful
shell exit or invented by the battery. Freeze its bytes with the controller
before starting. A missing or changed hook cannot earn acceptance. The existing
qualified SDK readback and transport-owner mechanisms should be reused.

The strict path requires all declared real cancellations (original Finder -128
reply following destination growth), actual buffered writes during transport
loss, an observed spool-cap stall, and verification of all originally submitted
stall inputs. Required alias/ACL/NFC/navigation observations cannot be warnings.
Category06/07/08 reuse `snappiness_custody.sh` original-worker receipts and waits.
An outstanding worker or unknown observation retains custody; a known RED with
no registered worker stops the category. The outer owner must independently
prove quiescence before retirement. No failure trap removes source or destination
bytes or silently restores a transport of unknown state.

Example after the fixture and hook are frozen (all angle-bracket values must be
replaced with their exact admitted paths/identities):

```sh
QA_STRICT_RELEASE=1 QA_KEEP_DATA=1 \
MOUNT=<owned-mount> CP_ADDR=<owned-loopback> JM_LOG=<owned-log> \
QA_STAGE=<owned-stage> QA_ARTIFACTS=<new-owned-artifacts> \
QA_OWNER_HOOK=<reviewed-owner-hook> QA_OWNER_HOOK_SHA256=<exact-sha256> \
QA_INDEX_SYNC_DIR=<armed-owned-sync-dir> /bin/bash test/qa-battery/run-all.sh
```

A subset, first-RED stop, missing hook, unobserved required case, or outstanding
original operation never constitutes a complete release battery PASS. All data
are retained until the owner has independently reconciled custody and retired
the fixture. Existing category09/10/11 latency and coverage bars are unchanged.
