# JuiceMount — Vision

## What this is

JuiceMount is an open-source, self-hostable file system that makes a remote
S3-compatible object store feel like a local SSD on macOS. Editors and
filmmakers mount the same shared namespace, get byte-range streaming reads
(no full downloads), and reference project media at identical absolute paths
across every machine — so `.fcpx` / `.prproj` / `.drp` projects open on a
new editor's machine **without remapping media**.

The architectural inspiration is LucidLink. The business model is the exact
opposite: **no SaaS, no per-seat pricing, no per-TB pricing, no proprietary
cloud, no enterprise gating, no web platform.** Users bring their own S3 (or
self-hosted MinIO) and run the whole stack themselves with one Docker
command.

The market we serve is the 1–10 person creative team that owns a NAS or a
Hetzner box and refuses to pay $50/seat/month to share footage. LucidLink
won't sell to them; Suite Studio won't host them; Dropbox/Drive can't
stream byte-ranges. Nobody is building this; we are.

## Core architectural wins (inherited from JuiceFS + the LucidLink approach)

1. **Byte-range S3 streaming.** A 12 GB R3D opens instantly. JuiceFS fetches
   only the bytes the NLE actually reads, not the whole file. Tier-1 cache
   lives on local SSD; tier-2 is the S3 backend.

2. **Shared identical mount paths.** Every machine mounts at `/Volumes/<name>`.
   Project files reference media at the same absolute path on every editor's
   machine. New editor opens the project — no relink, no "media offline."

3. **Multi-user concurrent access.** Two editors can open the same media
   simultaneously. Writes go through a single metadata authority (Redis);
   concurrent reads scale linearly with backend bandwidth.

4. **Hot bytes reused across editors.** When editor A scrubs through dailies,
   those JuiceFS blocks land in MinIO's edge cache. Editor B on the same LAN
   gets those bytes from MinIO (LAN speed) instead of re-fetching from cold
   storage (WAN speed). A small team scrubbing the same footage all day
   pays the WAN cost once.

5. **Pinned offline use.** Mark a folder as pinned — its bytes are downloaded
   to the local SSD up front. The team flies somewhere with bad Wi-Fi; the
   editor keeps cutting. JuiceMount falls back to the SSD cache and never
   tries the network for pinned bytes.

## What we explicitly are NOT building

- **A SaaS.** No "JuiceMount cloud," no managed offering, no hosted servers,
  no billing system. Self-host or don't use it.
- **A web platform.** No browser-based file viewer, no Frame.io-style review
  tool, no shared comment threads, no project dashboards. Those are
  LucidLink's / Suite's businesses.
- **A team-management UI.** No accounts, no role hierarchy, no permission
  matrix. Whoever has the shared key has the mount. Per-folder ACLs may
  arrive in tier-7 if anyone asks; until then, MinIO bucket policies are
  the answer.
- **A creative-AI layer.** No auto-tagging, no smart collections, no caption
  generation. That's Shade's space. Maybe tier-6 if a contributor wants
  to build it; not on the critical path.
- **A FileProviderExtension, ever.** See `docs/no-fileprovider.md` for the
  postmortem. Finder integration happens via FinderSync (badge overlays)
  and `NSServices` (right-click menu items) instead. The ghost-domain
  failure class is permanently disqualifying.

## The user-friendliness deltas (what we do that LucidLink doesn't)

These are the places where LucidLink's enterprise/SaaS DNA trades against
their UX, and where a tighter open-source product can dominate.

| Area | LucidLink | JuiceMount target |
|---|---|---|
| Pricing | Per-TB + per-seat SaaS | Free; self-host whatever you can afford to run |
| Onboarding | Account → filespace → invite users → IAM | Open app → paste shared key → click Mount |
| First mount | Heavy initial sync, no bandwidth detection | Bandwidth probe → user picks budget → resumable warmup |
| Server setup | Their cloud, period | `docker compose up` on any Linux box |
| Errors | "Contact support" | "Run this command:" + Force Eject button + diagnostic export |
| Project portability | Filespace IDs | `.juiceproject` YAML file you commit to git |
| Auth | OIDC, SSO, IAM policies | Shared key or MinIO credential; nothing more |
| Vocabulary | "Filespace," "block manifest," "snapshot identifier" | "Project," "shared folder," "pinned" |
| Configuration surface | Dozens of toggles in an admin UI | Sane defaults; advanced settings hidden by default |
| Update channel | Mandatory rolling updates from their cloud | Sparkle, two channels (stable/beta), user controls cadence |

## Tiered development priorities

The loop statement in this repo's session memory (and pasted at the bottom
of this doc) advances tiers sequentially. A tier can't be declared "done"
until every acceptance test in it passes for **7 consecutive days of real
user load**, or **24 hours of synthetic stress-harness load** when no real
users exist yet.

Per-tier detail lives under `docs/ROADMAP/`. Each linked doc has the
concrete acceptance tests, architecture notes, anti-patterns,
dependency map, **iteration plan** (work decomposed into 2-6 hour
slices the autonomous loop can consume), and **signals to watch**
(telemetry/log markers confirming each step works).

- `docs/ROADMAP/tier-1-stability.md` — table-stakes; never freeze
  Finder, never wedge mounts, crash-safe metadata, 24h soak pass.
  **Active blocking tier.**
- `docs/ROADMAP/tier-2-app-polish.md` — onboarding wizard, popover
  redesign, self-explaining errors, Sparkle, a11y. ~6 days of work.
- `docs/ROADMAP/tier-3-server-packaging.md` — `docker compose up` stack,
  `juicemount doctor`, backup sidecar, admin UI behind Caddy. ~5 days.
- `docs/ROADMAP/tier-4-network-resilience.md` — the **tier-1-blocking
  offline-resilience portion is already shipped and validated** (iter
  1-5, commits in docs/STATE.md). Remaining tier-4-proper work:
  bandwidth probing, per-project budgets, deferred indexing, write
  queues. ~8 days.
- `docs/ROADMAP/tier-5-finder-ux.md` — FinderSync badges, Services menu,
  QuickLook, mdimport (NEVER FileProviderExtension). ~5 days.
- `docs/ROADMAP/tier-6-search.md` — CoreML content tags, embeddings,
  `.juiceproject` bundles. Optional. ~5 days.
- `docs/ROADMAP/tier-7-collaboration.md` — live presence, soft locks,
  activity feed, per-folder ACLs. Optional, community-driven. ~5 days.

Total from current state to full vision: ~34 working days for the
required tiers (1-5) + ~10 if tiers 6 and 7 ship too.

### Tier 1 — Stability (the table-stakes)

| Feature | Acceptance test |
|---|---|
| Concurrent per-connection NFS dispatch | `cat /Volumes/zpool/<200MB-file> > /dev/null &` running, Finder browses adjacent folders <1s |
| No Finder freeze on any wedged backend | Kill MinIO mid-read; Finder reports error within 5s, doesn't beachball |
| Clean unmount in every state | Stop never leaves a wedged kernel mount, even mid-read / mid-write / mid-sync |
| Crash-safe metadata | `kill -9` JuiceMount; relaunch; metadata opens cleanly within 5s |
| Recovery diagnostics | "Export Diagnostics" produces a zip with logs, mount table, JuiceFS stats, MinIO health |
| Stress test harness | Synthetic Finder-like workload runs 24h in CI; no leaks, no wedges |

### Tier 2 — macOS app polish (the "iconic" tier)

| Feature | Reference |
|---|---|
| One-screen onboarding wizard | Tailscale's first-launch flow |
| Menu-bar popover: pinned-project tree with per-project progress bars | LucidLink filespace view, but better-designed |
| In-app errors with copy-pasteable remediation steps | Differentiator — nobody does this well today |
| Self-test dashboard (read speed, RTT, cache hit rate, backend health) | Tailscale's status page aesthetic |
| Notifications (pin done, sync done, error) | macOS Notification Center, opt-in |
| Sparkle auto-update with stable/beta channels | Standard OSS macOS pattern |
| Dark/light pass + accessibility audit | macOS HIG |

### Tier 3 — Server-side packaging

Target: `git clone <repo> && cd server && docker compose up` boots a
working JuiceMount server on a fresh Ubuntu / Synology / TrueNAS box.

```yaml
# docker-compose.yml shape
services:
  minio:        # object store
  redis:        # JuiceFS metadata authority
  juicefs-init: # one-shot format on first run
  juicemount:  # NFS-loopback + admin API
  caddy:        # TLS termination
volumes:
  data:         # bind-mount to host SSD/HDD pool
  metadata:
  certs:
```

Requirements:
- Healthchecks on every service
- Configurable via a single `.env` (data path, ports, admin key)
- `juicemount doctor` validates the full stack from inside the container
- Backup job: `mc mirror` + `juicefs dump` on a schedule
- Optional `--dev` mode runs everything on localhost without TLS

### Tier 4 — Network resilience

| Feature | Behavior |
|---|---|
| Bandwidth probe on launch | Measures real RTT + throughput, stores baseline per network |
| Per-project warm budget | "Warm this project to 50 GB on cellular, 500 GB on Wi-Fi" |
| Resumable warmup | Killed mid-warm → relaunch picks up where it left off |
| Cellular-aware offline gate | Auto-flip to offline mode on cellular; user confirms |
| Deferred indexing | First mount returns instantly; FTS index builds in background |
| Sparse first-mount | Don't pull file list for archived projects; lazy-load on navigate |

LucidLink probes bandwidth but doesn't expose budgets. Suite handles
projects but doesn't stream. Nobody does deferred indexing well — that's
a clean differentiator.

### Tier 5 — Finder-adjacent UX (no FileProvider)

| Feature | Mechanism |
|---|---|
| Right-click "Pin for offline" / "Unpin" | `NSServices` plist — works in any app |
| Status overlay badges (pinned / syncing / error) | **Finder Sync Extension (FinderSync)** — NOT FileProvider |
| Quick Look preview for proxies | `QLPreviewExtension` |
| Spotlight metadata for project assets | `mdimport` plugin reading our metadata store |
| Search Window: Cmd-Shift-F system-wide | Already exists; polish keyboard nav and previews |
| Drag-drop pinning from Finder onto popover | `NSDraggingDestination` on popover root |

**Hard rule:** FinderSync is the lightweight cousin of FileProvider —
badge overlays + context menu items only, no ghost-domain failure mode.
We ship FinderSync; we never ship FileProvider.

### Tier 6 — Search and metadata (Shade-class, optional)

| Feature | Approach |
|---|---|
| Content-based image/video search | CoreML CLIP embeddings stored in metadata DB |
| Auto-tagging by content | Vision framework face/object detection, opt-in |
| EXIF / video codec indexing | `mediainfo` sidecar runs on file ingest |
| Project bundles (`.juiceproject`) | YAML manifest of paths + bandwidth budget, commit to git |
| External LLM hooks | OpenAI / local Ollama for captioning, opt-in |

### Tier 7 — Collaboration

| Feature | Approach |
|---|---|
| Live presence (who has file X open) | Redis pub/sub keyed by file handle |
| Soft-locking for write conflicts | Advisory locks in Redis; Premiere/Avid lock files honored |
| Activity feed in popover | Append-only log of opens/writes |
| Per-folder ACLs | Metadata store + NFS-layer enforcement |

## Success metrics (production-ready definition)

| Metric | Target |
|---|---|
| Mount → scrubbing 4K in Premiere | <5s |
| Cold-read first-byte on warm cache | <100ms |
| Concurrent users on same backend | 10 editors without degradation |
| Recovery after `kill -9` → mountable | <10s |
| Onboarding: credentials → editing | <2 min |
| Server deploy: fresh box → working server | <10 min |
| Mean uptime between mount wedges | >30 days |
| Finder responsiveness during 4K scrub | <100ms for adjacent ops |

## Non-negotiables (block any change that violates)

- **No FileProviderExtension, ever.**
- **No telemetry without opt-in.** No analytics calls home. Period.
- **No proprietary dependencies for self-hosters.** Every component must
  be runnable on a Linux box with open-source software only.
- **No bundled-PR scope creep.** One theme per merge. Stability fixes
  never share a commit with feature work.
- **Reliability beats novelty.** Risky changes go behind a default-off
  flag until validated on real workloads.
- **Open-source-first.** Every release is reproducible, signed, notarized,
  with a public changelog explaining rationale.
- **No SaaS bait-and-switch.** If a future contributor proposes a hosted
  offering, it lives in a separate repo with a separate license. Core
  JuiceMount stays free, self-hostable, and unencumbered.

## The loop statement

Pasted here for easy copy:

```
/loop Drive JuiceMount toward production-ready per docs/VISION.md.
The vision is enumerated as seven tiered roadmap docs under
docs/ROADMAP/tier-N-<theme>.md. Each tier doc contains: goal,
acceptance tests, architecture, feature backlog, iteration plan
(slices with hour estimates), and signals to watch.

The active tier is the lowest-numbered tier whose acceptance tests
are not all ✓ validated in docs/STATE.md. Tier-N blocks tier-N+1.
At session start: read docs/STATE.md to identify the active tier,
then read docs/ROADMAP/tier-N-*.md to load its iteration plan.

PER-ITERATION CHECKLIST:
1. Read docs/STATE.md → confirm active tier + last-shipped slice
2. Read docs/ROADMAP/tier-N-*.md → pick the next unshipped slice
   from its "Iteration plan" table (smallest unshipped row)
3. Scope to that single slice — never bundle slices, never expand
   into adjacent tiers' work
4. If item touches competitor territory, consult
   docs/COMPETITIVE/<area>.md
5. Implement. Build (scripts/build-app.sh for app changes,
   scripts/build-cli.sh for CLI/handler). Run touched-package
   tests: `go vet ./...` + `go test -race ./<pkg>/...`
6. Spawn code-reviewer sub-agent on any commit touching request
   path, mount lifecycle, metadata store, or cache invariants
7. Validate against the relevant "Signals to watch" row in the
   tier doc — real Finder/VLC/NLE testing where applicable, not
   synthetic-only
8. Update docs/STATE.md: mark acceptance test ⚠ landed-needs-
   validation or ✓ validated; append the slice ID + commit hash
9. Commit rationale-first (why before what)
10. Self-pace next iteration

ADVANCEMENT RULE:
Active tier advances to tier N+1 when every acceptance test in
docs/ROADMAP/tier-N-*.md is ✓ validated in docs/STATE.md AND the
tier has held green for 7 consecutive days of real user load
(or 24h of stress-harness load when there is no real user load
yet). Tier-6 and tier-7 are optional — only enter them if the user
explicitly greenlights.

NON-NEGOTIABLES (per docs/VISION.md):
- No FileProviderExtension, ever. The build-time guard in
  scripts/build-app.sh enforces this; do not bypass it.
- No telemetry without explicit opt-in.
- No proprietary deps for self-hosters; every component runnable
  on Linux with OSS only.
- No bundled-PR scope creep; one theme per commit.
- Risky work behind a default-off flag until validated on real
  workloads.
- Open-source-first; reproducible signed notarized releases with
  rationale-first changelogs.

STOP when docs/STATE.md reports tier-1 through tier-5 all ✓
validated (the "required tiers" set per docs/VISION.md) OR when
the user explicitly halts. PushNotification on stop with a one-line
summary of the final tier state.
```
