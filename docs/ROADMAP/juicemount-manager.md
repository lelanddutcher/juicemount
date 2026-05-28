# JuiceMount Manager — implementation plan

**Status:** Foundation shipped (migrator works end-to-end). Manager
expansion = NOT STARTED.
**Branch:** `production-hardening` (no separate feature branch — slices
land directly via small PRs / commits)
**Filed:** 2026-05-27 from user request to broaden the migrator into a
control plane.
**Defers:** Section 7 (ZFS snapshot integration) — scoped out
explicitly; revisit after main vision lands.

---

## 1. Vision

The current `juicemount-migrator` becomes `juicemount-manager` — a
single-page control plane for the JuiceFS volume's lifecycle.
Every capability the user can think of for the volume's daily
operation lives in this app. **NOT** a file browser; **NOT** a WebDAV
replacement. It's the dashboard with the system's knobs and levers:
sync, trash, backups (scheduled remote sync), storage maintenance,
read-only system status, settings.

Strict design rule: if WebDAV (`:30180`) already serves a use case
(file viewing, downloading individual files), the manager does NOT
duplicate it.

## 2. Scope

### In scope (this plan)

| Section | What |
|---|---|
| 0. Rename + tab nav | Foundation refactor — image, binary, URL prefix, sidebar |
| 1. Overview | Read-only at-a-glance dashboard |
| 2. Migrations | Today's migrator + bidirectional (Out of / Between JuiceFS) |
| 3. Trash | Browse `.trash/`, bulk/per-item restore, retention knob |
| 4. Destinations | Named remote-endpoint profiles (s3, b2, sftp, webdav, jfs, file); encrypted at rest |
| 5. Backups | Cron-scheduled jobs reusing the sync engine |
| 6. Maintenance | GC, FSCK, warmup, cache flush, compact metadata |
| 8. Settings | Defaults, admin key rotation, log retention, theme |

### Out of scope (this plan)

- **§7 ZFS snapshots / TrueNAS integration** — deferred per user
  request. Snapshot lifecycle is inherently filesystem-side; the
  TrueNAS UI already handles configuration. Revisit after sections 0-8
  land.
- **File browser, preview, inline edit** — WebDAV already covers this.
- **User / RBAC management** — single admin-key model.
- **Anything requiring a database beyond the JSON state file** — keep
  install profile minimal.

## 3. Cross-cutting architecture

These decisions affect ≥2 slices and must be locked before any slice
that depends on them starts.

### 3.1 State schema versioning

The persistent JSON state file (`/var/lib/migrator/jobs.json` today)
grows new top-level sections. Add a version field and a `migrate()`
function that upgrades old shapes on load.

```json
{
  "schema_version": 2,
  "jobs":         { ... },              // unchanged from v1
  "order":        [ ... ],              // unchanged
  "destinations": [ ... ],              // §4
  "schedules":    [ ... ],              // §5
  "settings":     { ... },              // §8
  "trash_audit":  [ ... ]               // §3 (recent restore/delete log)
}
```

v1 (today) has no `schema_version`; loading code defaults to 1 and
runs no migration (existing fields are still valid). v2 introduces
the new sections as empty arrays.

State file location renames: `/var/lib/migrator/jobs.json` →
`/var/lib/manager/state.json`. Loader checks the old path as a
fallback and writes a one-shot migration line to the log.

### 3.2 Credential encryption at rest

Destinations (§4) store cloud credentials. Plaintext-on-disk is
disqualifying for any deployment beyond LAN-only.

**Scheme:**
- KDF: HKDF-SHA256 over `JM_ADMIN_KEY` with a fixed info string
  `"juicemount-manager v1 cred-key"` → 32 bytes
- Cipher: AES-256-GCM
- Per-secret nonce: 12 random bytes prepended to the ciphertext
- Wire format (base64 in JSON): `<12-byte nonce><ciphertext><16-byte
  GCM tag>`
- Plaintext never touches disk; encryption happens on POST to
  `/api/destinations` and decryption on subprocess exec
  (`os.Environ()` only at the moment of `RunSync`)

**Admin-key rotation story:** rotating `JM_ADMIN_KEY` invalidates all
stored creds. Settings (§8) provides "Rotate admin key" which prompts
for the OLD key, decrypts everything in memory, re-encrypts under the
NEW key, then writes the new state file atomically. Failure mode: if
the old key is forgotten, destinations are orphaned and must be
re-entered. Document explicitly.

**Security-reviewer gate:** §4 commit MUST be reviewed by
`everything-claude-code:security-reviewer` before merge.

### 3.3 Frontend routing

Single `index.html` with hash-routes (`#/migrations`, `#/trash`,
`#/destinations`, `#/backups`, `#/maintenance`, `#/settings`).
Sidebar nav swaps the `<main>` content via JS. Preserves the
`/migrator/` (and the future `/manager/`) prefix-agnostic deployment
that already works.

Trade-off: hash-routes vs. real History API. Hash-routes win because
the static `handleStatic` Go handler doesn't have to know about every
route. No backend changes needed for nav.

### 3.4 Backend route prefix

API stays under `/api/...` (unchanged). New endpoints:
- `/api/destinations` (CRUD)
- `/api/destinations/{name}/test` (validate connectivity)
- `/api/schedules` (CRUD)
- `/api/schedules/{name}/run` (trigger one-shot)
- `/api/trash/list`, `/api/trash/restore`, `/api/trash/delete`,
  `/api/trash/empty`
- `/api/maintenance/{gc,fsck,warmup,cache-flush,compact-meta}` (POST
  triggers, GET status)
- `/api/overview` (aggregate read-only)
- `/api/settings` (GET/PUT)

`/api/migrate`, `/api/jobs/*`, `/api/sources`, `/api/browse`,
`/api/preview`, `/api/resolve-destination` are kept unchanged for
backwards compat — they're the migration tab's API.

### 3.5 Rename: migrator → manager

Naming changes:
- Binary: `cmd/juicemount-migrator/main.go` → `cmd/juicemount-manager/main.go`
- Image: `ghcr.io/lelanddutcher/juicemount-migrator` →
  `ghcr.io/lelanddutcher/juicemount-manager`. Keep the migrator tag as
  an alias for one release (build the same Dockerfile under both
  names in the matrix workflow for a transition period of ≥ 2 weeks).
- Env vars: `JM_*` prefix stays; nothing to rename in env. State file
  path env: `JM_STATE_FILE` (unchanged).
- Internal package: `internal/migrator` → `internal/manager`. Sub-
  packages added per slice (see slices below).
- HTTP prefix: `/migrator/` → `/manager/` for the embedded mode in
  juicemount-server. Add a redirect from `/migrator/*` to
  `/manager/*` for one release.

### 3.6 Backwards compatibility

- Image tag aliases for two releases.
- State file path: load old path first, write to new path going forward.
- HTTP prefix redirect `/migrator/*` → `/manager/*`.
- Existing `/api/migrate` etc. paths unchanged.

After two releases, drop the aliases with a CHANGELOG note.

## 4. Slice-by-slice plan

Each slice = one focused commit (or small commit series) on
`production-hardening`. Slices have explicit dependencies. Many can
land in parallel — see §5 for the dependency graph.

Every slice MUST pass:
- `go vet ./...`
- `go test -race -count=1 ./internal/manager/...`
- `go build ./cmd/...`
- `node --check internal/manager/static/app.js` (if JS touched)
- Code-reviewer agent (general) before merge
- For §4 and §8: also `everything-claude-code:security-reviewer`

---

### SLICE 0 — Rename + tab nav scaffold

**ID:** `slice-0-rename`
**Status:** COMPLETE (commit db2b531, deployed 2026-05-28, container `ix-juicemount-juicemount-manager-1` healthy, sidebar + 7 tabs serving at port 30190 root)
**Depends on:** none
**Blocks:** every other slice (foundation)

**Goal:** Rename migrator → manager across the codebase; introduce
the sidebar + tab-routing scaffold; ship with the current Migrations
tab still working end-to-end. Zero feature change.

**Files to touch:**
- `cmd/juicemount-migrator/` → `cmd/juicemount-manager/` (NEW dir, move main.go)
- `internal/migrator/` → `internal/manager/` (rename package)
- `internal/manager/static/index.html` (MOD — add sidebar, move
  existing UI into a `<section data-tab="migrations">` block)
- `internal/manager/static/style.css` (MOD — sidebar layout)
- `internal/manager/static/app.js` (MOD — hash-route handler that
  shows the right `[data-tab]` section, defaults to migrations; wraps
  existing init so it only runs when that tab activates)
- `server/juicemount-migrator/Dockerfile` →
  `server/juicemount-manager/Dockerfile` (rename + update build path)
- `.github/workflows/docker-publish.yml` (MOD — add second matrix
  entry that publishes the same image under the old tag too for the
  compat period)
- `server/docker-compose.yml` (MOD — change service name and image)
- `server/INSTALL-TrueNAS.md`, `server/README.md` (MOD — rename
  references)
- `cmd/jm5/main.go` (MOD — update import path + URL prefix
  `/migrator/` → `/manager/`, ALSO add a 301 redirect handler at
  `/migrator/*` → `/manager/*`)
- `internal/manager/api.go` (MOD — add the `/migrator/*` →
  `/manager/*` redirect handler)

**New types/functions:**
- `func (a *API) redirectMigrator(w, r)` — 301 to /manager/ preserving path/query
- Frontend: `route()`, `showTab(name)`, hash-change listener

**Acceptance:**
1. `docker compose up` with the new compose brings up
   `juicemount-manager` (not migrator) and the existing migration UI
   works at both `/migrator/` and `/manager/` URLs.
2. Sidebar appears with 7 entries (Overview, Migrations, Trash,
   Destinations, Backups, Maintenance, Settings) — only Migrations is
   functional; rest show "Coming soon" placeholders.
3. Existing migration flow (browse, preview, start, live progress,
   resume) unaffected.
4. `go test -race ./internal/manager/...` matches the prior migrator
   coverage.
5. Old image tag `juicemount-migrator:production-hardening` continues
   to publish from CI alongside the new tag.

**Test plan:**
- Manual: open `/manager/`, confirm sidebar; click Trash → "coming
  soon" placeholder.
- Manual: visit `/migrator/` → 301 to `/manager/`, page renders.
- `curl -fsS http://<host>:30190/api/sources` still works (no API
  path change).
- `curl -fsS -L http://<host>:30190/migrator/` returns the index html.

**Risks:**
- Package rename will trigger a wide mechanical change; double-check
  imports in `cmd/jm5/main.go` and any test files.
- Compose service rename means existing TrueNAS apps need redeploy.
  The image-tag alias absorbs this — users with `pull_policy: always`
  on the migrator tag keep working.

**Sub-agent prompt** (paste as the Agent prompt):

> Implement SLICE 0 of docs/ROADMAP/juicemount-manager.md on the
> production-hardening branch. The slice renames every "migrator"
> reference to "manager" (binary, package, image, HTTP prefix) and
> introduces a sidebar + hash-route scaffold in the static UI. The
> existing Migrations tab continues to work exactly as today.
>
> Constraints:
> - Preserve every API endpoint path under /api/ unchanged.
> - Add a 301 redirect from /migrator/* → /manager/* (one-release
>   compatibility shim).
> - Keep the old `juicemount-migrator:production-hardening` image
>   tag publishing from CI for the transition.
>
> Before commit:
> - go vet + go test -race ./internal/manager/... pass.
> - node --check static/app.js passes.
> - Run everything-claude-code:code-reviewer on the diff.
>
> Read docs/ROADMAP/juicemount-manager.md §3 and the SLICE 0 entry
> in §4 for the file-by-file list, types, acceptance criteria, and
> test plan. Don't add scope beyond what's listed.

---

### SLICE 1 — Bidirectional migrations

**ID:** `slice-1-bidirectional`
**Status:** COMPLETE (commit d84e495, deployed 2026-05-28, Direction picker live in Migrations tab; browser smoke pending user confirmation)
**Depends on:** slice-0
**Blocks:** slice-4 (destinations layout reuses Direction picker)

**Goal:** Expand the Migrations tab to support three directions:
Into JuiceFS (today), Out of JuiceFS, JuiceFS-to-JuiceFS.

**Files to touch:**
- `internal/manager/sync.go` — generalize `normalizeSourceURI` to
  accept any of: `/sources/*`, `/jfs/*`, `jfs://*`. Add
  `normalizeAnyURI(s, preserveStructure)` that routes to the right
  helper.
- `internal/manager/api.go` — `migrateRequest` grows
  `Direction` field (enum: `in`, `out`, `between`); routing in
  `handleMigrate` validates source/destination pair per direction.
- `internal/manager/static/app.js` — Direction radio above source
  picker; toggles which root the browser starts from.
- `internal/manager/static/index.html` — Direction UI block.

**New types/functions:**
- `type Direction string` with constants `DirectionIn`, `DirectionOut`,
  `DirectionBetween`.
- `validateDirectionPair(dir, src, dst) error` — gates that, e.g.,
  Out-direction destination cannot be the JuiceFS volume.
- `func (a *API) handleBrowseJFS(w, r)` — browse the FUSE mount tree
  for the Out / Between cases (re-uses pathAllowed against `/jfs`).

**Acceptance:**
1. Direction=In behavior matches current migrator exactly.
2. Direction=Out picker browses `/jfs/` (FUSE mount), submits a sync
   with `file:///jfs/...` source and `file:///external/...` dest.
3. Direction=Between requires two JuiceFS volumes configured (out of
   scope here — surface a "Configure a second JuiceFS destination
   first (Destinations tab)" message; this lights up once slice-4
   ships).
4. Path-traversal guards apply equally to both sides.
5. Live progress, cancel, resume all work in the new directions.

**Test plan:**
- `TestNormalizeAnyURI` unit table for all three direction types.
- `TestValidateDirectionPair` cases incl. forbidden combinations.
- Integration: kick off a small Out-direction copy to a bind-mounted
  `/external/test`; verify files land at the host path.

**Sub-agent prompt:**

> Implement SLICE 1 of docs/ROADMAP/juicemount-manager.md. Add
> Direction (in/out/between) to the migration request and UI. The
> Between case is stubbed until slice-4 lands — show a helpful
> message pointing the user to Destinations.
>
> Reuse normalizeSourceURI / normalizeDestURIEmbedded / matchSlash
> patterns from sync.go; add normalizeAnyURI as the dispatcher.
> Preserve the strict trailing-slash agreement guarantee that
> matchSlash enforces — juicefs sync FATALs on a mismatch.
>
> Run code-reviewer before commit. Tests required for both new
> validation functions.

---

### SLICE 2 — Overview dashboard

**ID:** `slice-2-overview`
**Status:** COMPLETE with caveats (commit 32d2d29, deployed 2026-05-28; /api/overview live, redis+jobs cards green, MinIO card shows "minioURL not configured" — JM_MINIO_URL env not applied to live compose during in-place patch, follow-up patch needed; volume card shows "juicefs status failed exit 1" — likely the standalone container's juicefs version doesn't support --json on status, needs version detection)
**Depends on:** slice-0
**Blocks:** none

**Goal:** Read-only landing tab with the volume's at-a-glance state.
No knobs; all signal.

**Files to touch:**
- `internal/manager/overview.go` (NEW) — aggregate handler that
  fans out to: `juicefs status`, `juicefs stats`, Redis `INFO`,
  MinIO admin liveness, recent job summary (from JobManager).
- `internal/manager/api.go` — register `/api/overview` GET.
- `internal/manager/static/app.js` — Overview tab; polls
  `/api/overview` every 10s while the tab is visible.
- `internal/manager/static/index.html` — Overview section markup.
- `internal/manager/static/style.css` — stat-card grid.

**New types/functions:**
- `type OverviewSnapshot struct { ... }` — JSON shape with: volume
  bytes used/total, file count, redis latency ms, minio reachable,
  cache hit rate, recent jobs (last 10), background ops status.
- `func collectOverview(ctx) OverviewSnapshot`

**Acceptance:**
1. Loads in <500 ms on a warm cache.
2. Degrades gracefully (per-source `error: string` field) if any
   backend is unreachable — never returns 5xx.
3. Stat-cards rerender without flicker when polling.

**Test plan:**
- Unit-test `collectOverview` with all backends mocked.
- Unit-test the degraded path (Redis down → snapshot shows
  `redis.error`).

**Sub-agent prompt:**

> Implement SLICE 2 of docs/ROADMAP/juicemount-manager.md. Build the
> Overview dashboard tab. Backend is a fan-out aggregator that NEVER
> returns 5xx — each source contributes its own error field on
> failure. Frontend polls every 10s while the tab is visible (pause
> when hidden via document.visibilityState).
>
> Reuse the existing styles and bytes formatter helpers. Run
> code-reviewer before commit.

---

### SLICE 3 — Trash

**ID:** `slice-3-trash`
**Status:** COMPLETE (commit eba9b7a, deployed 2026-05-28, /api/trash/config returns days=-1 recommended=7 — UI can prompt user to enable retention)
**Depends on:** slice-0
**Blocks:** none

**Goal:** Surface JuiceFS's `.trash/` subtree as a browsable +
restorable interface. Re-enable trash retention by default.

**Files to touch:**
- `internal/manager/trash.go` (NEW) — list, restore, delete, empty.
- `internal/manager/api.go` — register `/api/trash/*` endpoints.
- `internal/manager/static/app.js` — Trash tab: date-grouped list,
  per-row Restore/Delete buttons, bulk-select, Empty Trash button.
- `internal/manager/static/index.html` + style.css.
- `server/docker-compose.yml` — flip `--trash-days 0` to
  `--trash-days 7` in the juicefs-init format command (new installs
  only — existing volumes need `juicefs config --trash-days 7` run
  once).
- `server/INSTALL-TrueNAS.md` — document the change + the one-time
  config command for upgraders.

**New types/functions:**
- `type TrashEntry struct { Path, OriginalPath, DeletedAt, Size }`
- `func listTrash(ctx, fuseMount) ([]TrashEntry, error)` — reads
  `<fuseMount>/.trash/`.
- `func restoreTrash(entry TrashEntry, targetPath string) error`
- `func deleteTrash(path string) error`
- `func emptyTrash(ctx) (freed int64, err error)`
- `func getTrashConfig(metaURL) (days int, err error)` — `juicefs
  config <metaURL>` parser
- `func setTrashConfig(metaURL string, days int) error` — `juicefs
  config <metaURL> --trash-days N`

**Acceptance:**
1. Trash tab lists at most 1000 newest items with a "load more"
   pagination (bounded to keep the page responsive).
2. Restore preserves the original path — if it already exists,
   append `(restored YYYY-MM-DD HH-MM-SS)` to the basename.
3. Empty Trash requires a typed confirmation ("delete N items, X GB")
   to fire.
4. Retention knob in the tab shows current value, lets the user pick
   from {0=disabled, 1, 7, 30, 90, 365}, calls
   `juicefs config --trash-days N` and reloads.
5. Trash-audit log (last 20 ops) shown at bottom of the tab.

**Test plan:**
- Unit tests for `restoreTrash` collision-rename logic.
- Integration: mark a file as deleted via FUSE rm, confirm it appears
  in the trash list, restore it, verify the original path is
  restored.

**Risks:**
- Existing volumes have `--trash-days 0`. Document that switching
  retention on does NOT recover already-deleted-permanently files.
- Trash lives inside the JuiceFS volume — large trash counts against
  volume usage. Make this clear in the Trash tab header.

**Sub-agent prompt:**

> Implement SLICE 3 of docs/ROADMAP/juicemount-manager.md. Build the
> Trash tab. Trash is JuiceFS's built-in feature exposed at
> /<fuseMount>/.trash/. Restore preserves original paths with
> collision-rename. Empty Trash requires typed confirmation. The
> retention knob writes via `juicefs config --trash-days N`.
>
> Also flip the default in server/docker-compose.yml's juicefs-init
> from --trash-days 0 → --trash-days 7 (new installs). Document in
> INSTALL-TrueNAS.md the one-time upgrade command for existing
> volumes.
>
> Run code-reviewer before commit. Integration test required.

---

### SLICE 4 — Destinations

**ID:** `slice-4-destinations`
**Status:** NOT STARTED
**Depends on:** slice-0; encryption design from §3.2 finalized
**Blocks:** slice-5 (schedules require destinations)
**Reviewer gate:** `everything-claude-code:security-reviewer`
MANDATORY before merge — credential storage + encryption code.

**Goal:** Saved profiles for remote endpoints (s3, b2, sftp, webdav,
jfs, file). Stored encrypted at rest. UI for add/edit/test/delete.
Migration tab destination picker can choose a profile by name.

**Files to touch:**
- `internal/manager/destinations.go` (NEW) — CRUD + test connection.
- `internal/manager/crypto.go` (NEW) — HKDF + AES-GCM
  encrypt/decrypt helpers; takes JM_ADMIN_KEY-derived key.
- `internal/manager/state.go` (NEW or MOD jobs.go) — generalize
  persistence to include destinations (§3.1 schema v2).
- `internal/manager/api.go` — `/api/destinations` GET/POST/PUT/DELETE,
  `/api/destinations/{name}/test`.
- `internal/manager/sync.go` — generalize `RunSync` to accept a
  Destination profile instead of (or alongside) the raw URI. Pass
  decrypted env vars only at exec time.
- `internal/manager/static/app.js` — Destinations tab; migration
  destination picker grows a "Saved destinations" dropdown.
- `internal/manager/static/index.html` + style.css.

**New types/functions:**
- `type Destination struct { Name, Kind string; Config map[string]string; CreatedAt int64 }`
  (`Config` holds plaintext at the API boundary; encryption happens
  before persist)
- `type encryptedDestination struct { ... }` — wire shape on disk
  (base64 ciphertext)
- `func deriveCredKey(adminKey []byte) []byte` — HKDF
- `func encryptDestination(d Destination, key []byte) (encryptedDestination, error)`
- `func decryptDestination(e encryptedDestination, key []byte) (Destination, error)`
- `func (d Destination) ToSyncURI(role string) (uri string, env []string, error)`
  — converts to juicefs-sync arg form + env var list (creds via env,
  NOT command-line, so they don't leak to `ps aux`)
- `func testConnection(ctx, d Destination) error` — kind-specific
  light probe (s3: HeadBucket, sftp: SSH connect, webdav: PROPFIND, etc.)

**Acceptance:**
1. POST /api/destinations with plaintext creds → 201; on-disk state
   file has only ciphertext.
2. PUT updates existing; GET lists (with cred fields REDACTED in the
   response, never re-emitted plaintext).
3. DELETE removes from state. Idempotent.
4. POST /api/destinations/{name}/test returns 200 on success, 5xx
   with diagnostic message on failure.
5. Admin-key rotation flow in Settings (§8) re-encrypts all
   destinations.
6. juicefs sync subprocess receives creds via `cmd.Env` only —
   `ps aux` shows no secrets.
7. Migration tab can pick a saved destination instead of typing a path.

**Test plan:**
- Unit-test crypto.go: round-trip encrypt/decrypt; wrong-key fails
  with AEAD verification error; nonce uniqueness over N=10000
  encryptions.
- Unit-test testConnection per kind with mocked subprocess.
- Integration: add an S3 destination pointing at the local MinIO,
  test connection, run a small migration to it.

**Security-reviewer focus areas:**
- HKDF info string + salt
- Nonce generation (must be random per encryption, never reused)
- GCM tag handling — full 16 bytes appended, verified on decrypt
- Memory hygiene: plaintext creds zeroed after use where possible
- Wire-format leak: API responses never echo plaintext creds back

**Sub-agent prompt:**

> Implement SLICE 4 of docs/ROADMAP/juicemount-manager.md. Build the
> Destinations tab with encrypted-at-rest credential storage.
>
> Encryption scheme is fixed in §3.2: HKDF-SHA256 from JM_ADMIN_KEY,
> AES-256-GCM, 12-byte random nonce per secret. Plaintext credentials
> NEVER touch disk and NEVER appear in API responses. juicefs sync
> subprocess receives credentials via cmd.Env only.
>
> Kinds to support: file, s3, b2, webdav, sftp, jfs.
>
> MANDATORY before commit:
> 1. go vet, go test -race, go build all green
> 2. everything-claude-code:security-reviewer on the diff
> 3. everything-claude-code:code-reviewer on the diff
>
> Both reviewer passes must complete without CRITICAL or HIGH issues.
> MEDIUM issues must be addressed before commit.

---

### SLICE 5 — Backups (schedules)

**ID:** `slice-5-schedules`
**Status:** NOT STARTED
**Depends on:** slice-4 (destinations)
**Blocks:** none

**Goal:** Recurring sync jobs. Cron expression OR friendly preset.
Each schedule fires a normal Job through the existing pipeline.

**Files to touch:**
- `internal/manager/schedules.go` (NEW) — schedule store + scheduler
  goroutine.
- `internal/manager/state.go` — add `schedules` array to v2 schema.
- `internal/manager/api.go` — `/api/schedules` CRUD + `/run` trigger.
- `internal/manager/static/app.js` — Backups tab.
- `internal/manager/static/index.html` + style.css.
- `go.mod` — add `github.com/robfig/cron/v3` (battle-tested, single dep).

**New types/functions:**
- `type Schedule struct { Name string; Source SourceSpec; Destination DestinationRef; Options SyncOptions; Cron string; Paused bool; RetainHistory int; LastRun int64; NextRun int64 }`
- `type Scheduler struct { mu sync.Mutex; cron *cron.Cron; entries map[string]cron.EntryID; mgr *JobManager; ... }`
- `func (s *Scheduler) Start(ctx)` — fires scheduled jobs by calling
  `mgr.Submit(...)` at each tick.
- `func (s *Scheduler) Update(sched Schedule)` — re-registers the
  cron entry.
- Friendly presets: a small map of name → cron string for the UI to
  suggest (nightly 2am, weekly Sun 3am, hourly, every-6-hours).

**Acceptance:**
1. CRUD + run-now work via API.
2. Schedule fires create a Job that appears in the Migrations tab job
   list with `schedule_name` annotation.
3. Pausing a schedule prevents further firings without deleting it.
4. Cron parser rejects malformed expressions with a clear message.
5. Friendly preset dropdown populates the cron field.
6. State file persists schedules; scheduler reloads them on startup.
7. Per-schedule history view shows last N runs (limited by
   `RetainHistory`).

**Test plan:**
- Unit-test the cron parser wrapper.
- Unit-test scheduler.Update idempotency.
- Integration: create a schedule with cron `* * * * *` (every minute);
  verify it fires twice; pause; verify no further fires.

**Sub-agent prompt:**

> Implement SLICE 5 of docs/ROADMAP/juicemount-manager.md. Build the
> Backups tab with cron-scheduled jobs.
>
> Use github.com/robfig/cron/v3 (add to go.mod). Scheduler is a
> goroutine in the JobManager that submits one-shot Jobs at each
> fire. Each Job carries a schedule_name field so the UI can group
> them.
>
> Friendly preset map: nightly-2am, weekly-sun-3am, hourly,
> every-6-hours. UI surfaces both the preset dropdown and the raw
> cron field.
>
> Run code-reviewer before commit. Integration test required.

---

### SLICE 6 — Storage maintenance

**ID:** `slice-6-maintenance`
**Status:** COMPLETE (commit afa885d, deployed 2026-05-28 after GHCR juicemount-manager package visibility flipped to Public; 5 levers wired, SSE streams ready, per-kind mutex live)
**Depends on:** slice-0
**Blocks:** none

**Goal:** Levers for `juicefs gc`, `juicefs fsck`, `juicefs warmup`,
local cache flush, Redis `BGREWRITEAOF`. On-demand or scheduled (via
slice-5 when that lands).

**Files to touch:**
- `internal/manager/maintenance.go` (NEW) — subprocess wrappers + status.
- `internal/manager/api.go` — `/api/maintenance/{gc,fsck,warmup,cache-flush,compact-meta}`.
- `internal/manager/static/app.js` — Maintenance tab.
- `internal/manager/static/index.html` + style.css.

**New types/functions:**
- `type MaintenanceOp struct { Kind string; State string; StartedAt, FinishedAt int64; Output []string; Error string }`
- `func runGC(ctx, dryRun bool) (*MaintenanceOp, error)`
- `func runFSCK(ctx) (*MaintenanceOp, error)`
- `func runWarmup(ctx, paths []string) (*MaintenanceOp, error)`
- `func runCacheFlush(ctx) (*MaintenanceOp, error)`
- `func runCompactMeta(ctx) (*MaintenanceOp, error)`
- These mirror the JobManager pattern: one op at a time per kind, SSE
  for live output.

**Acceptance:**
1. Each lever has a button + a "running" / "last run" display.
2. GC supports `--dry-run` toggle; output shows bytes that WOULD be
   reclaimed.
3. FSCK output streams live; errors collected into a summary.
4. Warmup takes a path input; defaults to volume root.
5. Output capped at 1000 lines per op; older lines truncated with a
   "[truncated]" marker.
6. Concurrent ops of the SAME kind rejected with 409; different
   kinds can run concurrently (gc + fsck simultaneously is fine).

**Test plan:**
- Unit-test the op-per-kind-mutex.
- Mock subprocess output and verify SSE streaming.
- Manual: run GC --dry-run on the existing volume, confirm output is
  non-empty and 0 bytes-to-reclaim (clean state).

**Sub-agent prompt:**

> Implement SLICE 6 of docs/ROADMAP/juicemount-manager.md. Build the
> Maintenance tab with five levers: GC, FSCK, Warmup, Cache flush,
> Compact metadata. Each is a juicefs CLI subprocess wrapped with
> SSE-streamed output.
>
> One op per kind at a time (409 if a same-kind op is running);
> different kinds run concurrently. Output capped at 1000 lines.
>
> Run code-reviewer before commit.

---

### SLICE 7 — DEFERRED (snapshots)

Per user instruction, NOT building. Documented here so future readers
know it was intentional. Revisit after slices 0-6 + 8 land.

---

### SLICE 8 — Settings

**ID:** `slice-8-settings`
**Status:** NOT STARTED
**Depends on:** slice-4 (admin-key rotation needs encryption)
**Reviewer gate:** `everything-claude-code:security-reviewer`
recommended for the admin-key rotation flow.

**Goal:** Per-job defaults, admin-key view/rotate, log retention,
theme.

**Files to touch:**
- `internal/manager/settings.go` (NEW) — settings struct +
  load/save.
- `internal/manager/state.go` — add `settings` to v2 schema.
- `internal/manager/api.go` — `/api/settings` GET/PUT,
  `/api/settings/rotate-admin-key`.
- `internal/manager/static/app.js` — Settings tab.
- `internal/manager/static/index.html` + style.css.

**New types/functions:**
- `type Settings struct { JobDefaults SyncOptions; Theme string; LogRetentionLines int; DestinationsRedacted bool }`
- `func rotateAdminKey(oldKey, newKey []byte) error` — decrypts all
  destinations under old, re-encrypts under new, atomic state write,
  then the operator updates `JM_ADMIN_KEY` on the container.

**Acceptance:**
1. Defaults set in Settings are pre-filled in the Migrations tab
   options.
2. Theme toggle works (system / dark / light) and persists via
   localStorage AND state file (server defaults win on fresh load).
3. Admin-key rotation requires the CURRENT key; verifies it by
   decrypting at least one destination; on success re-encrypts all
   and writes new state file atomically; instructs the operator to
   update the container env.
4. Log retention knob caps the trash-audit + recent-jobs arrays.

**Test plan:**
- Unit-test rotateAdminKey: success path, wrong-old-key rejection.
- Integration: rotate admin key with 3 destinations configured;
  verify all decryptable under new key.

**Sub-agent prompt:**

> Implement SLICE 8 of docs/ROADMAP/juicemount-manager.md. Build the
> Settings tab. Admin-key rotation is the high-risk feature — it
> must verify the old key before rewriting state, write the new
> state file atomically, and clearly instruct the operator to update
> the container env var afterward.
>
> Run code-reviewer AND security-reviewer before commit.

---

## 5. Dependency graph + parallelism

```
slice-0  ──┬──> slice-1 ──┐
           │              │
           ├──> slice-2   │
           │              │
           ├──> slice-3   │
           │              │
           ├──> slice-4 ──┼──> slice-5
           │              │
           ├──> slice-6   │
           │              │
           └──> slice-8 <─┘
```

**Critical path:** slice-0 → slice-4 → slice-5
(also slice-0 → slice-4 → slice-8 in parallel).

**Parallelizable after slice-0 lands:** 1, 2, 3, 6 can all run
concurrently. 4 is the long pole because of the crypto + reviewer
gates.

**Suggested order if running sequentially:** 0, 2, 3, 1, 6, 4, 5, 8
(quick wins first; the high-risk crypto work in slice-4 lands once
the surrounding surface is stable).

## 6. Branch + commit policy

- All slices land on `production-hardening` via small commits.
- Each slice = 1-3 commits. Each commit: green on `go vet + go test
  -race + go build + node --check`.
- Code-reviewer agent on EVERY commit before push.
- security-reviewer on slice-4 and slice-8 specifically.
- CI matrix builds publish `juicemount-manager` AND
  `juicemount-migrator` tags for the compat period (drop migrator
  tag two releases after slice-0 ships).
- After each slice merges to production-hardening, the loop operator
  redeploys the TrueNAS app and validates the slice's acceptance
  criteria live.

## 7. Rollout

Per-slice deployment via the existing pattern:
1. Commit + push.
2. CI builds image (~6 min).
3. `midclt app.update juicemount` with patched compose (or just
   trigger a redeploy if compose unchanged).
4. Smoke test the new functionality against the running volume.
5. Update task tracking; close slice.

User-facing changelog: maintain `CHANGELOG.md` entries per slice with
the breaking-change note for the rename (slice-0).

## 8. Loop-operator workflow

When operator wants to start a slice:
1. Read this doc → find the slice → copy the sub-agent prompt.
2. Spawn a sub-agent (typically `general-purpose` for implementation,
   then `code-reviewer` for review).
3. The sub-agent reads its slice in this file for specifics.
4. On completion, operator runs the acceptance tests, commits,
   pushes, redeploys.
5. Mark slice STATUS: COMPLETE in this doc as part of the merge
   commit.

Slices block strictly per the graph in §5. The operator should not
start slice-N until slice-N's blockers all show STATUS: COMPLETE.

## 9. Risks / open questions

1. **Cron library choice**: robfig/cron/v3 is the obvious pick. If
   the dependency adds appreciable image size or maintenance worry,
   fallback is to ship a 200-line manual scheduler. Decide in
   slice-5.
2. **State file size**: settings + schedules + destinations + trash
   audit could grow into hundreds of KB on a heavily-used install.
   Acceptable for a JSON file written once per state change. Revisit
   only if startup load latency exceeds 100 ms.
3. **Admin-key rotation atomicity**: if the operator forgets to
   update `JM_ADMIN_KEY` after rotation, the next container restart
   fails to decrypt. Mitigation: rotation flow writes the new admin
   key to a clearly-named file (`/var/lib/manager/PENDING_ADMIN_KEY`)
   that the operator can copy into the compose; manager refuses to
   start if the env doesn't match. Implement in slice-8.
4. **Trash storage accounting**: the `.trash/` subtree counts against
   JuiceFS volume capacity. Make this LOUD in the Trash tab header
   so users don't get surprise out-of-space.
5. **Sub-agent context**: each slice's sub-agent prompt should
   include a pointer to this doc and the specific slice section.
   Sub-agents read the relevant section before implementing.
