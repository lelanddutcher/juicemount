# JuiceMount

A native macOS menu-bar app that makes JuiceFS-backed shared storage feel like a local SSD to Premiere, DaVinci Resolve, Final Cut, and Finder. Self-hosted, cellular-capable, no recurring fees.

**Last updated:** 2026-05-25
**Active branch:** `production-hardening`

---

## What it does

Mounts a Redis + S3 (MinIO / B2 / R2) backed JuiceFS volume at `/Volumes/zpool` over a loopback NFS v3 server tuned for macOS Finder, with a SwiftUI menu-bar app for everything else.

- **Browse 100 K+ entries instantly** via SQLite metadata cache + FTS5 trigram search.
- **Read pinned media at LAN / SSD speed** (200+ MB/s sustained, 4.6 MB total network traffic on a 200 MiB read of a cached file).
- **Toggle offline mode** for cellular: pinned files keep working, un-pinned reads fail in <100 ms instead of beachballing.
- **Pin folders for offline** via popover button or Finder right-click → Services.
- **Auto-expand JuiceFS cache** to 85% of disk, with `--free-space-ratio 0.01` so the disk is genuinely the cache.
- **Reclaim APFS purgeable space** (Time Machine local snapshots) at mount time and on-demand.
- **Verify-and-repair** pinned coverage on Sync — re-prefetches anything JuiceFS evicted.

---

## Quick start

```bash
# Build (Go c-archive + Swift app + .app bundle + ad-hoc codesign)
./scripts/build-app.sh

# Install
./scripts/install.sh                  # to /Applications
./scripts/install.sh --launchd        # also enable login auto-start

# Launch
open /Applications/JuiceMount.app

# Or run from the build dir without installing
open ./build/JuiceMount.app
```

The menu-bar drive icon appears. Click it for the popover.

For headless / TrueNAS / Linux deployment without the menu bar app:

```bash
./scripts/build-cli.sh                # builds /tmp/jm5
/tmp/jm5 --redis redis://127.0.0.1:6379/1 \
         --mount /Volumes/zpool \
         --listen 127.0.0.1:11049 \
         --db /tmp/jm5.db \
         --cache-size 100000
```

---

## Repo layout

```
JuiceMount6/
├── README.md                  ← you are here
├── ROADMAP.md                 — phased status + Phase 4 next steps
├── ARCHITECTURE_juicemount.md — system architecture, data flows
├── MENU_BAR_APP.md            — popover features, keyboard shortcuts, troubleshooting
├── CHANGELOG.md               — release notes
├── credentials.md             — sensitive infra config (gitignored)
│
├── app/JuiceMount/            — Swift Package: menu-bar app
│   └── Sources/
│       ├── JuiceMountCore/    — C interop layer over libnfsd.h
│       └── JuiceMount/        — App.swift, UI, ServerController, NFSBridge
│
├── bridge/cbridge.go          — Go c-archive exports (Start/Stop/Stats/Pin/Unpin/...)
├── cmd/jm5/                   — headless server CLI (long-running NFS server)
├── cmd/juicemount/            — control client CLI (talks to running app via HTTP)
│
├── nfs/                       — NFS handler, read/write paths, fd pool, readahead, membuf
├── metadata/                  — SQLite store + Redis sync + FTS5 search
├── cache/                     — Direct SSD cache reader (Priority 2 read path)
├── health/                    — FUSEManager, monitor loop, network watcher
│
├── internal/
│   ├── cache/pin/             — Pin store + prefetcher + verify-and-repair
│   ├── jmlog/                 — Structured JSON logging with rotation
│   ├── metrics/               — RPC counters, /metrics HTTP endpoint
│   ├── nfs/                   — Vendored go-nfs fork
│   └── nle/                   — Premiere/Resolve/FCPX project parsers
│
├── test/                      — Integration / e2e / workflow / benchmark tests (2026-03-31)
├── scripts/                   — Build, install, codesign helpers
├── logos/                     — Brand assets
│
├── VISION/                    — Strategic positioning, competitive, persona, roadmap, prototypes
│   ├── STATE.md               — vision-loop status + implementation status
│   ├── feature-roadmap-ranked.md
│   ├── positioning.md, personas.md, brand-identity.md, gtm-strategy.md, ...
│   ├── competitive/           — Suite, Shade, Iconik, Frame.io, Jellyfish, NAS vendors
│   └── prototypes/            — Per-prototype writeups (01 codec, 02 backup, 03 offline-pin)
│
└── z-quarantine/              — Files set aside for review or removal
    └── README.md              — what's there and why
```

---

## How a read happens

```
Premiere / Resolve / Finder reads /Volumes/zpool/Project/clip.mov
        ↓
NFS RPC → 127.0.0.1:11049 → nfs/server.go → handler.OpenFile
        ↓
[offline mode + un-pinned?  → return EIO in ~6 ms]
        ↓
cachedFile.ReadAt(buf, off):
        ├─ Priority 1: memBuf (small files: prproj, LUTs)
        ├─ Priority 2: cache.Reader (direct pread on JuiceFS chunks/ — bypasses FUSE)
        └─ Priority 3: fuseFD.ReadAt (FUSE → JuiceFS LRU → S3 backend on miss)
```

When you pin a folder, the prefetcher walks it, opens each file via the FUSE mount, reads it in 1 MB chunks, and discards the bytes. The side effect is the bytes flowing through JuiceFS's download + cache pipeline. Subsequent reads hit Priority 2 or Priority 3 with no backend round-trip.

For full detail see `ARCHITECTURE_juicemount.md` § 11 (pin/prefetcher/offline-mode) and § 4 (data flows).

---

## Configuration

Edit Preferences in the app, or set defaults via:
- **Redis URL:** `redis://127.0.0.1:6379/1`
- **Mount point:** `/Volumes/zpool`
- **NFS listen:** `127.0.0.1:11049`
- **Metrics / control plane:** `127.0.0.1:11050`
- **SSD cache:** auto-expanded to 85% of disk (user pref is a floor)
- **Memory buffer:** 2 GiB default, files <128 MiB

Logs:
- App: `~/Library/Logs/JuiceMount/juicemount.log` (JSON, 16 MB × 5 rotation)
- JuiceFS daemon: `~/.juicefs/juicefs.log` (auto-tailed into the above with WARN aggregation)

---

## Recent: NFS read throughput restored after a wifi-triggered regression (2026-05-25)

A user-reported regression — **DaVinci Resolve playback at <2 fps on
cached 4K media** — led to discovering two compounding issues, one
long-latent and one triggered by wifi instability, that together had
collapsed NFS read throughput to **9.5 MB/s** against a healthy-system
target of 200+ MB/s. Closed as QA-26 through QA-31.

### What looked broken

- DaVinci played fully-cached media at <2 fps.
- Finder occasionally showed media as offline (red minus / ESTALE).
- Big-file copies into the mount failed mid-transfer with Finder error
  100060 (ETIMEDOUT) or 100070 (ESTALE).
- "Sync Now" appeared to re-prefetch files that were already 100%
  cached.

### What was actually broken (three independent bugs interleaving)

**1. Long-latent: per-NFS-RPC Stat amplification.** Every NFS READ RPC
made two FUSE Stat round-trips (size-clamp in `nfs_onread.go:101` plus
post-op attrs in `nfs_onread.go:203`), each going through the
2-second-budgeted phantom-purge gate in `juiceFS.Stat`. The bug had
been present for months and was latent because earlier workloads
(small-file copy, Finder browsing) didn't sustain the high RPC rate
that exposes the per-RPC cost. **DaVinci's scrub pattern was the
first workload that stressed it.**

**2. Wifi-triggered: metadata-sync cycles taking 17 s** under
intermittent Redis reachability. That made all FUSE-side work slower,
which made #1 worse. Also caused the prune logic (`PruneThreshold=2`
cycles, ≤60 s window) to incorrectly mark still-present files for
deletion when Redis SCAN returned incomplete results during a network
blip.

**3. Cache-correctness consequence of #2:** pruning a still-valid file
removed `inodeCache[inode]`. Kernel-cached NFS handles then returned
ESTALE → DaVinci treated fully-cached media as offline.

### Why wifi was the trigger, not the cause

- ~4-day session on a flaky AP with multiple reconnects.
- Each wifi flip briefly broke Redis reachability (`no route to host`).
- JuiceFS daemon crashed on Redis loss and restarted, re-syncing cold.
- The metadata sync queue backed up, holding `writeMu` for seconds at
  a time.
- All NFS ops slowed; the per-RPC Stat amplification went from
  "tolerable" to "lethal".

**"We were stable" reflected lower stress, not better code.** The
per-RPC Stat amplification had been there since the phantom-purge gate
was added; previous workloads never sustained the RPC rate that made
it visible.

### Fixes shipped in this branch

| ID    | Layer | Change |
|-------|-------|--------|
| QA-26 | Swift state machine | Ephemeral URLSession defeats post-wifi-switch connection rot; stuck-state backstop force-transitions popover from `.disconnected → .running` when `/health` is healthy. |
| QA-27/28 | metadata cache | `evictPathOrphan` keeps `inodeCache` consistent under inode-reassignment WITHOUT breaking still-valid kernel handles (redirect, not delete). |
| QA-29 | mount option | `timeo=10 → timeo=200` (10× per-RPC budget) so CREATE/WRITE under metadata-sync `writeMu` contention don't hit the kernel timeout. |
| QA-30 Layer C | metadata + pin store | Pinned files are NEVER pruned or evicted, period. |
| QA-30 Layer A | metadata sync | Per-path FUSE `Lstat` verifies prune candidates before deletion. Bounded 8-goroutine gate; >25% timeouts → bail cycle. `PruneThreshold` 2 → 10 (5-min buffer). |
| QA-30 Layer B | nfs handler | Recently-evicted shadow map + singleflight recovery in `FromHandle` — if an inode goes missing and FUSE confirms the path still exists, the entry is restored on demand. |
| QA-31 | nfs read path | `rsize` 256 KB → 1 MB (cuts RPC count 4×); `cachedFile.CachedInfo()` value-snapshot exposes Open-time metadata so `onRead` skips two FUSE Stat round-trips per RPC. EOF-underflow in size-clamp fixed. |

### Measured impact — same cached MP4 via `dd 200M`

| Stage                                            | Throughput     | READ p95    | READ max    |
|--------------------------------------------------|----------------|-------------|-------------|
| Pre-fix (rsize=256K + per-RPC Stat)              | 9.5 MB/s       | 3.4 s       | 3.45 s      |
| After QA-31 Slice 2 (rsize=1MB only)             | 50 MB/s        | —           | —           |
| After QA-31 Slice 3 (Stat fast-path + EOF fix)   | **226-571 MB/s** | **481 ms** | **370 ms**  |

**Zero `FromHandle STALE` events** since QA-30 Layer C+A deploy.
`01-smoke.sh` 10 PASS / 0 FAIL. All metadata/internal-nfs tests pass
under `-race`.

Five HIGH findings from two code-reviewer audit gates (QA-30 Layer
C+A) and one audit gate (QA-30 Layer B + QA-31) were all resolved
before deploy. Audit trail in `docs/STATE.md`.

### Lessons

- Wifi instability is a useful chaos-test for backend dependencies
  but it can mask latent bugs as "environmental" — until a workload
  arrives that exposes them.
- Pin store and metadata store both holding truth about "what's
  cached" without a single shared invariant is the root design issue
  these three layers treat at the symptom level. A unified
  pin-aware cache truth is on the followup list.
- A 2-second-budgeted `Lstat` inside the per-RPC critical path is
  always a latent throughput cliff. Per-RPC overhead must be
  guarded with explicit per-handle caching (the QA-31 pattern).

---

## Status

Phase 1 ✅ stability + polish · Phase 2 ✅ menu-bar app · Phase 3 ✅ production hardening (this branch)

**Phase 4 — workflow features for video creatives** — see `ROADMAP.md` and `VISION/feature-roadmap-ranked.md`. Top three:
1. Codec-aware Quick Look proxies (R3D / ARRI / BRAW / ProRes RAW) — 2–3 weeks to ship
2. Content-hash backup verification — 3–4 weeks including UI shell
3. Auto-detect bandwidth-aware mode (manual offline-toggle is shipped)
