# JuiceMount

A native macOS menu-bar app that makes JuiceFS-backed shared storage feel like a local SSD to Premiere, DaVinci Resolve, Final Cut, and Finder. Self-hosted, cellular-capable, no recurring fees.

**Last updated:** 2026-05-08
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

## Status

Phase 1 ✅ stability + polish · Phase 2 ✅ menu-bar app · Phase 3 ✅ production hardening (this branch)

**Phase 4 — workflow features for video creatives** — see `ROADMAP.md` and `VISION/feature-roadmap-ranked.md`. Top three:
1. Codec-aware Quick Look proxies (R3D / ARRI / BRAW / ProRes RAW) — 2–3 weeks to ship
2. Content-hash backup verification — 3–4 weeks including UI shell
3. Auto-detect bandwidth-aware mode (manual offline-toggle is shipped)
