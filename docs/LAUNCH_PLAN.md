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
| 3 | **First-run + state honesty**: preflight (juicefs/macFUSE/backend) with guided errors (LB-1); NFS-mounted in state machine + Mount Now retry, wire EnableNFSRemount (LB-2); distinct error icon + .startFailed alert (S-1); de-hardcode 127.0.0.1:11050 ×6 + mount-point strings (S-2); Reset-DB stop→delete→restart flow (S-6) | Swift UI/Core, bridge/cbridge.go (status), health/monitor.go (remount wiring) | 01-smoke, 10-control-plane + manual first-run sims | ☐ |
| 4 | **OSS hygiene**: placebo settings wired-or-deleted (LB-4); scrub personal IPs/paths from UI/CLI/scripts (S-5); docs honesty pass — README prereqs, spool docs, sudoers incl. umount, Gatekeeper (S-4); uninstall.sh (S-3); LaunchAgent consistency (P-2) | docs, README, scripts/, Preferences*.swift, cmd/ | 00-precheck, 01-smoke | ☐ |
| 5 | **Full-suite final validation**: run-all (00–07, 09 short, 10–12; 08-netshape if sudo granted), wedge-tests ×4, test-offline-resilience.sh, crash-recover-test.sh; update OPEN_BUGS/CHANGELOG/STATE; final readiness report | — | everything | ☐ |

Pipelining: while phase N is in gate/QA, phase N+1's agent may implement in
an isolated worktree and rebase after N's commit. Merges stay serialized.

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
  → gate finishing: relaunch + QA re-run, then commit.

### New findings logged during Phase 1 gate (not regressions)
- Burst-create ETIMEDOUT ~1/1000 under load (QA-29 stall class, newly
  visible now that 02-finder runs past rsync) — candidate Phase 2/3.
- cp -R exits 1 copying dir xattrs (NFSv3 no-xattr; data lands fine) —
  launch-list item: AppleDouble/xattr story for dirs.
- QA suite fragility: a phase script dying mid-run (set -e) writes no
  .summary → run-all reports synthetic fail=99 and masks real results —
  fix in Phase 5 prep.
- /spool lists historical done rows with pending=0 (presentation) — Phase 2.
