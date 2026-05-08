# z-quarantine/

Files moved here are not actively used by the current build, but they're kept around for one of these reasons:

1. **Historical context** — early diagnostic scripts, prototype docs from before the production-hardening branch, etc.
2. **Possible reuse** — code paths we might want to revisit, but don't want noise in the active tree.
3. **User review** — Claude moved this here because it looks unused; user (Leland) should confirm before deletion.

**Decision rule:** if it's been here for more than one development sprint and nobody's asked about it, it's safe to `git rm`.

---

## What's here

### `scripts-test-bridge.swift` (was `scripts/test-bridge.swift`)
**Quarantined:** 2026-05-08
**Reason:** Early diagnostic script from when we were debugging whether `ServerConfig` JSON survived the Swift→Go bridge transit. Not referenced from any build script. Replaced by the structured logging we now have on both sides — `~/Library/Logs/JuiceMount/juicemount.log` includes a `cbridge received config` line on every Start() call that shows the full config Go received.

### `juicemount-f1.ai` (was `juicemount f1.ai` at repo root)
**Quarantined:** 2026-05-08
**Reason:** A 1.2 MB Adobe Illustrator file (PDF wrapper, 5 pages) at the repo root, dated 2026-03-31. Looks like an early brand-iteration drop that never moved into `logos/`. The active brand assets are in `logos/` (black/white/color × .png/.svg). Renamed to remove the space in the filename.

**If keeping:** move into `logos/` once the user confirms it's still wanted.
**If not:** safe to `git rm`.

---

## What's NOT here, but you might think should be

### `cmd/jm5/` (headless server CLI)
**Status:** **kept active.** Looks duplicative of `cmd/juicemount/`, but they serve different purposes:
- `cmd/jm5/` — long-running headless NFS server CLI. Use this for TrueNAS / Linux server deployment where you don't want the menu-bar app. Built by `scripts/build-cli.sh`.
- `cmd/juicemount/` — short-lived control client. Talks to a running JuiceMount app via the HTTP control plane on port 11050 to send `pin`, `unpin`, `cache-status`, `prefetch-project`, `verify-pins`.

### `test/` (integration tests, dated 2026-03-31)
**Status:** **kept active.** The unit-test packages (`benchmark_suite_test.go`, `e2e_test.go`, `workflow_test.go`, `finder_perf_test.go`) predate the production-hardening branch and don't exercise pin/offline/verify paths. They still pass and exercise the metadata + read paths. Candidate for refresh, not removal.

### VISION/ documents
**Status:** **kept active and being updated.** The strategic research from the VISION loop is the source of truth for positioning, persona, competitive analysis, and feature roadmap. See `VISION/STATE.md` for the implementation status of each prototype.

### `credentials.md`
**Status:** **kept active and gitignored.** Sensitive infrastructure config; do not move.
