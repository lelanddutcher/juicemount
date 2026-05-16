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

Per-tier detail lives under `docs/ROADMAP/`. The summaries below are the
canonical *what* per tier; each linked doc has the concrete acceptance
tests, architecture notes, anti-patterns, and dependency map.

- `docs/ROADMAP/tier-2-app-polish.md` — onboarding wizard, popover
  redesign, self-explaining errors, Sparkle, a11y
- `docs/ROADMAP/tier-3-server-packaging.md` — `docker compose up` stack,
  `juicemount doctor`, backup sidecar, admin UI behind Caddy
- `docs/ROADMAP/tier-4-network-resilience.md` — **includes the offline
  resilience plan that partially blocks tier-1 advancement**; bandwidth
  probing, per-project budgets, deferred indexing
- `docs/ROADMAP/tier-5-finder-ux.md` — FinderSync badges, Services menu,
  QuickLook, mdimport (NEVER FileProviderExtension)
- `docs/ROADMAP/tier-6-search.md` — CoreML content tags, embeddings,
  `.juiceproject` bundles, optional
- `docs/ROADMAP/tier-7-collaboration.md` — live presence, soft locks,
  activity feed, per-folder ACLs, optional

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
/loop Drive JuiceMount toward production-ready per docs/VISION.md. Each
iteration: open docs/STATE.md, pick the next unblocked item from the
current active tier (tier-N blocks tier-N+1), scope to a 2–6 hour slice,
ship it, append a STATE.md entry. Do not bundle themes.

Active tier advances when every acceptance test in docs/VISION.md for
the current tier passes for 7 consecutive days of real user load, or
24h of synthetic stress-harness load when no real users yet.

PER-ITERATION CHECKLIST:
1. Read docs/STATE.md → pick item
2. If item touches competitor territory, consult docs/COMPETITIVE/<area>.md
3. Decompose, build, validate with real Finder/VLC where applicable
4. Spawn code-reviewer sub-agent on any commit touching request path,
   mount lifecycle, or metadata store
5. Update STATE.md: shipped, deferred, broken, next
6. Commit rationale-first
7. Self-pace next iteration

NON-NEGOTIABLES (per docs/VISION.md): no FileProviderExtension; no opt-out
telemetry; no proprietary deps for self-hosters; no scope creep; risky
work behind default-off flag; open-source-first.

STOP when docs/STATE.md reports tier-3 production-ready AND user
greenlights tier-4 entry. PushNotification on stop.
```
