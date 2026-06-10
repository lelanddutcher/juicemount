# Launch Plan — Hardening & Validation Ledger

Goal: harden and validate all code in the repo for open-source launch. Every
change batch passes the **standard gate** before merge; phases are serialized
so a regression is attributable to exactly one batch. Sub-agents implement;
the orchestrator (Claude session) deploys, gates, and commits.

## Standard gate (run after every phase, before its commit)
1. `go build ./...` + `go vet ./...` clean; `gofmt -l` empty on touched files.
2. `go test ./...` — no NEW failures vs the Phase-0 baseline catalog
   (`/tmp/jm-launch/baseline-go-test.txt`). Known env-dependent failures
   (need local Redis :6379 / MinIO :9000): TestMinIOHealthCheck,
   TestStatusReturnsCorrectState, TestBillyFileOfflineGate, TestMemBuf*
   (×4). Flagged-unreliable in the Phase-0 capture (ran during the app
   restart window; re-verify at the Phase-1 gate before counting either
   way): TestBenchmarkSuite, TestFUSEMountCheck, TestRedisHealthCheck.
3. Swift bundle builds: `JM_QUICK=1 ./scripts/build-app.sh --release`.
4. Clean relaunch (quit → clear mounts → open → healthy ≤90s).
5. **Byte-integrity (QA Rule 1)**: ≥10 MiB random file written through
   /Volumes/<mount>, read back, sha256 exact match. Non-negotiable.
6. Targeted QA phases for the change surface (see per-phase rows) via
   `PHASES="…" bash scripts/qa-suite/run-all.sh` — zero FAILs in targeted
   phases (02-finder mv/rename tests MUST pass from Phase 1 onward).
7. Adversarial diff review by a fresh sub-agent (correctness + QA-bug
   regression scan: QA-19/27/29/30/32/35/36/37 guards intact).
8. Commit locally (no push) with phase tag in message.

## Phases

| # | Scope (bugs) | Files (owner) | Targeted QA | Status |
|---|---|---|---|---|
| 0 | Baseline: commit session work, catalog go-test baseline, restart app (clears 43 stuck spool entries via RecoverOnBoot), purge QA leftovers | — | 00-precheck | ☐ |
| 1 | **Write-path correctness**: rename spool-blind (handler.go:1540) → silent mv breakage; spool handle-refcount leak + sweeper silent skip (spool.go:962); cp rc=1; ftruncate "RPC struct is bad" (SETATTR path) | nfs/*, internal/nfs/*, metadata/spool_store.go | 01-smoke, 02-finder, 06-concurrency + wedge write-integrity | ☐ |
| 2 | **Spool durability+UX**: stranded writes on spool-disable/quit (LB-3); stuck-spool UI affordance + last_error render + stalled detection + clear/retry endpoint (LB-5); /spool size overlay for writing rows | nfs/spool_status.go, nfs/drainer.go, bridge/cbridge.go, App.swift, PreferencesWindowView.swift, MenuPopoverView.swift | 01-smoke, 10-control-plane + manual spool scenario | ☐ |
| 3 | **First-run + state honesty + identity** (expanded 2026-06-10 per user): onboarding/welcome flow for initial setup with preflight (juicefs/macFUSE/backend) + guided errors (LB-1); NFS-mounted in state machine + Mount Now retry, wire EnableNFSRemount (LB-2); menu-bar icon = Logos/ mark, state-tinted (green healthy / amber degraded / blue offline-files mode / red fault) + upload-activity badge, replacing SF-Symbol icons (S-1 folds in); app icon from Logos/color; simplified at-a-glance popover status (health · cache-vs-free · uploads); de-hardcode 127.0.0.1:11050 ×6 + mount-point strings (S-2); Reset-DB flow (S-6) | Swift UI/Core, Logos/, scripts/build-app.sh (icns), bridge/cbridge.go (status), health/monitor.go | 01-smoke, 10-control-plane + manual first-run sims | ☐ |
| 3b | **Preferences redesign sprint** (user 2026-06-10): de-clutter + fix scaling/sizing; logical grouping; placebo settings wired-or-deleted here (LB-4 moves from ph.4) | PreferencesWindowView.swift, Preferences.swift | 01-smoke + manual | ☐ |
| 4 | **OSS hygiene + README**: publication README (draft from web-track agent, merged + verified against actual behavior); scrub personal IPs/paths from UI/CLI/scripts (S-5); docs honesty pass — prereqs, spool docs, sudoers incl. umount, Gatekeeper (S-4); uninstall.sh (S-3); LaunchAgent consistency (P-2) | docs, README, scripts/, cmd/ | 00-precheck, 01-smoke | ☐ |
| W | **Web presence track** (parallel, non-app): juicemount.com plan + content architecture + interactive tool concept; positioning vs Suite/Shade/Iconik/Aspect (BYO-storage, no contracts) and vs Nextcloud/MountainDuck/Seafile (partial-file streaming, ~7Gbit on 10GbE); cost story; JuiceFS foundation; JuiceMount Manager tie-in. NEW FILES ONLY (docs/web/, docs/README_DRAFT.md) | docs/web/ (new) | n/a (docs) | ✅ 2026-06-10: README_DRAFT.md + web/SITE_PLAN.md + web/INTERACTIVE_TOOL.md; ~17 VERIFY tags for the Phase-4 merge; **LICENSE FILE MISSING — founder decision needed (MIT vs Apache-2.0, deps are Apache-2.0)**; competitor pricing researched + dated |
| 5 | **Full-suite final validation**: run-all (00–07, 09 short, 10–12; 08-netshape if sudo granted), wedge-tests ×4, test-offline-resilience.sh, crash-recover-test.sh; update OPEN_BUGS/CHANGELOG/STATE; final readiness report | — | everything | ☐ |

Pipelining: while phase N is in gate/QA, phase N+1's agent may implement in
an isolated worktree and rebase after N's commit. Merges stay serialized.

## Approved icon/state spec (user-approved 2026-06-10, via mockup)
- Menu-bar icon = the Logos/ citrus mark, state-tinted (isTemplate=false —
  color IS the signal): **green = healthy** (original palette),
  **yellow/amber #EF9F27 = degraded/recovering**, **blue #378ADD =
  offline-files-only mode**, **red #E24B4A = unreachable/fault**.
- Upload-activity badge: small circular badge (blue, up-arrow) bottom-right
  of the mark while drains are active; pending count lives in the popover.
- Assets: Logos/state-{healthy,degraded,offline-files,fault}.svg (generated
  from color.svg by swapping the 5-green palette; verified 5 fills each).
- Render pipeline (proven, zero deps): NSImage loads SVG on modern macOS —
  /tmp/svg2png.swift pattern renders crisp 36px and 512px PNGs; fold into
  scripts/build-app.sh for menu-bar @1x/@2x and the AppIcon.icns iconset.
- App icon: Logos/color (healthy green) via the existing iconutil path.

## Ledger (append per phase)
<!-- phase / agent / changes / gate results / commit -->

- **Phase 0** (2026-06-08): baseline commit e3aff46 (11 files, session
  hardening work). go-test baseline captured (7 stable env-dependent
  failures + 3 restart-contaminated, see gate note). App restarted:
  RecoverOnBoot cleared all 43 stuck `writing` spool rows (pending 43→0,
  capacity 66.6MB→0) — validates boot-recovery path. Backend QA leftovers
  (.jmqa-finder-34595) purged via FUSE. Observation for Phase 2: /spool
  still lists 46 historical entries with pending=0 (presentation). ✅
- **Phase 1** (2026-06-08/09): write-path correctness. Agent fixed all 4
  bugs (shared epicenter: SETATTR{size} → spool truncate stub → cp rc=1 +
  EBADRPC XDR + the 43-handle leak; plus spool-blind Rename). 20 new
  failing-first tests. Gate: build/vet/gofmt ✅; full sweep zero new
  failures ✅ (the 3 restart-contaminated baseline entries now pass);
  byte-integrity sha256 ✅ cp rc=0 ✅; live mv acid test FUSE-verified ✅;
  QA 01-smoke 10/10 ✅, 02-finder 11/1 ✅ (the 1 = pre-existing burst-create
  ETIMEDOUT, logged below), 06-concurrency 6/0 ✅. Adversarial review:
  BLOCK → 3 fixes applied + 1 hardening: (A) drainer stale-snapshot claim
  → post-claim row re-read (CRITICAL: rename racing a backlogged drain
  silently undone); (B) MigrateActivePaths substr rune-vs-byte count
  (unicode dir renames migrated zero children); (C) unconditional dst
  CancelForDelete (post-restart rows invisible to the index gate);
  (D) nfsStatusErrorFrom unwraps to concrete *NFSStatusError. A+B
  regression tests added and NEGATIVE-CHECKED (each test proven to fail
  with its fix reverted). Reviewer follow-ups deferred as non-blocking:
  D-rollback-on-FUSE-rename-fail, late-SETATTR zero-clobber shape (E),
  writeSizes dir-children carry (F), stale QuarantineDrain hygiene (G),
  ErrSpoolFull→NFS3ERR_NOSPC mapping (H), escalation-test flake margin.
  Gate finale: rsync-under-load failure root-caused live (deferred-tx
  SQLITE_BUSY upgrade in MigrateForRename racing drain-completion writes;
  busy_timeout doesn't apply to snapshot-stale upgrades) → DSN
  _txlock=immediate; before/after stress 2/5 fails → 0/10. ✅ COMMITTED
  c29a19f.

- **Phase 2** (2026-06-10): spool durability + UX. Agent delivered all 4
  items (stranded-write guards on spool-disable/quit/stop-everything;
  stuck-spool UI with last_error/age/stalled badges + retry/recover buttons;
  /spool-recover endpoint; /spool reporting correctness incl. WrittenEnd
  overlay + relevance-filtered entries; ErrSpoolFull→NFS3ERR_NOSPC via
  syscall.ENOSPC wrap). Adversarial review: SHIP-WITH-FIXES → all applied:
  (1) failPermanent/QuarantineDrain ownership-guarded capacity release
  (MarkFailed returns rows-affected) + two-reservation exact-capacity tests,
  negative-checked; (2) drain-wait views stop on failedFiles>0 (no
  auto-quit/disable on "everything failed") + unreachable-server handling;
  (3) RetryFailed HasNewerRowForPath staleness guard + test; (4) retry
  reserve-before-reset ordering; (5) quit re-entrancy guards. Review
  follow-ups deferred (non-blocking): RecoverStalled scope vs the 3
  persistent stalled states; RecoverOnBoot deletes failed rows' files
  (restart kills retryability — design decision); spool_entries GC timer;
  WriteAt NOENT mapping note. Gate: build/vet/gofmt ✅; suite = only the 5
  env-dependent fails ✅; -race clean ✅; swift 0 errors ✅; live: integrity
  ✅, /spool fields ✅, /spool-recover contract ✅, QA 01-smoke 10/10 +
  10-control-plane 16/16 ✅.

- **Phase 3** (2026-06-10): UI sprint. Implementer died at session limit
  AFTER finishing (no report; diff was the truth): state-tinted logo
  menu-bar icon (approved spec incl. precedence + idle 0.5α + upload
  badge, cached NSImages, SF-Symbol fallback) + AppIcon.icns pipeline in
  build-app.sh (svg2png, all-or-nothing menubar dir, legacy fallback);
  at-a-glance popover header (health word / cache-vs-free bar / uploads
  row); onboarding assistant (431-line window, preflight juicefs+macFUSE+
  backend dial, launch gate, Setup Assistant menu item); /mount-now
  endpoint (5 tests) + NFS in /health + glance state machine.
  Adversarial review: SHIP-WITH-FIXES → applied: (1) P1-A Mount Now UI
  was never wired (review's "classic session-limit death") — remedy row +
  spinner + honest "Volume not mounted" subtitle added; (2) P1-B
  auto-remount disarmed BEFORE unmount in NFSServerShutdown/StopMount
  (stop-window remount race → orphaned kernel mount, QA-27 class);
  (3) P2-A assistant Continue from .error now drives stop→start instead
  of silently no-opping; (4) P2-B upgrade migration: absent
  hasCompletedOnboarding key + existing metadata.db ⇒ onboarded (no
  first-run window on configured installs). Non-blocking follow-ups →
  Phase 3b/4: preflight auto-retry while gate-blocked assistant open;
  /mount-now server-side single-flight; health-details auto-expand
  initial state; juicefs PATH-fallback drift; item-5 cleanups (6×
  hardcoded 11050, Reset-DB flow). Gate: build/vet/gofmt ✅, bridge tests
  ✅, full sweep = 8 known env-dependent only ✅, swift 0 errors ✅,
  bundle pipeline produced 8 menubar PNGs + 498KB icns ✅.

- **Phase 3b** (2026-06-10): preferences redesign sprint — all 5 items, no
  stop-shorts. 4 grouped-Form tabs (General/Connection/Cache&Storage/
  Maintenance), 600pt fixed width + per-tab heights, clamped numerics,
  whitespace-stripped URLs, accurate effect-timing footers, personal-IP
  example scrubbed. LB-4 placebo kills: membuf budget + file-limit +
  reconcile interval wired end-to-end (variadic NewHandler option /
  SetReconcileInterval pre-Start; byte-identical defaults, back-compat
  pinned by tests incl. exact JSON keys); volumeName derives mountPoint
  UI-side. S-2: zero hardcoded 11050/zpool strings left in popover (all
  via prefs, MainActor-captured). S-6: Reset-DB = soft-stop → delete →
  Start Now/Later, pin.db preserved. /mount-now single-flight (CAS, 409,
  race-tested); health-details onAppear; preflight PATH fallback.
  Adversarial review: SHIP-WITH-FIXES (cleanest phase — no criticals) →
  applied: (1) name→mount derivation anchor survives clear-then-retype;
  (2) DiagnosticsExporter routed through configured metricsAddr
  (MainActor-captured at both call sites); (3) "."/".." volume names
  rejected (privileged mount over /Volumes or /); (4) metricsAddr footer
  notes stale-readout window; (5) fallback doc-comment corrected.
  Review follow-ups deferred non-blocking: legacy out-of-range stored
  values render unclamped until edited (Go maps ≤0→defaults, cosmetic);
  reconcile base >5min inverts backoff (validation range caps at 3600s —
  document); reset success alert on failed delete; cbridge→jmnfs field-
  mapping pin test. Gate: build/vet ✅, all new tests ✅, swift 0 errors
  0 warnings ✅, zero hardcoded addrs ✅, live: integrity OK, 01-smoke
  10/10, 10-control-plane 16/16 ✅.

- **Phase 4** (2026-06-10): OSS publication hygiene — all 6 items.
  LICENSE (Apache-2.0, founder-decided) + NOTICE (go-nfs v0.0.3 Apache-2.0,
  go-nfs-client BSD-2 VMware, JuiceFS external-binary — provenance
  verified); publication README from the draft with all 17 VERIFY tags
  resolved (dev content → docs/dev-setup.md); personal IP/path scrub of
  shipping surfaces (JM_QA_NAS_IP env for netshape; secrets sweep CLEAN);
  docs honesty pass (JM_SPOOL_ENABLE truth ×6 docs, sudoers+umount 4-way
  agreement, MENU_BAR_APP rewritten to current UI, Gatekeeper modernized,
  CHANGELOG phases 1-4, OPEN_BUGS closed against commits); uninstall.sh
  (mountpoint-guarded, spool double-confirmation, --dry-run; live dry-run
  caught the du-through-FUSE 15TB trap); LaunchAgent via open -a +
  user-log path. Adversarial review: SHIP-WITH-FIXES → applied: README
  exit-story corrected (bucket holds JuiceFS chunk format — "aws s3 sync,
  done" was FALSE; now copy-from-volume/juicefs sync), "plain objects" +
  "JuiceFS gateway" wording fixed, ARCHITECTURE "(private)" dropped,
  STATE.md µs/ms unit labels fixed (p95 481µs), Gatekeeper line scoped to
  macOS 15+, uninstall hardening (space-safe mount parsing, root +
  empty-HOME guards, --help range), Phase-4 CHANGELOG entry. Review
  verified: README ~55 claims clean otherwise, uninstall.sh NO data-loss
  path, NOTICE accurate, sudoers 4-way match, CHANGELOG SHAs real.
  Pre-publish checklist items left for the founder: decide whether
  z-quarantine/ ships (internal junk, no secrets); capture the 7Gbit/s
  test command for the methodology doc someday.

### New findings logged during Phase 1 gate (not regressions)
- Burst-create ETIMEDOUT ~1/1000 under load (QA-29 stall class, newly
  visible now that 02-finder runs past rsync) — candidate Phase 2/3.
- cp -R exits 1 copying dir xattrs (NFSv3 no-xattr; data lands fine) —
  launch-list item: AppleDouble/xattr story for dirs.
- QA suite fragility: a phase script dying mid-run (set -e) writes no
  .summary → run-all reports synthetic fail=99 and masks real results —
  fix in Phase 5 prep.
- /spool lists historical done rows with pending=0 (presentation) — Phase 2.
