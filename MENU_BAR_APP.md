# JuiceMount Menu Bar App

A native macOS menu bar app for managing your JuiceMount NFS server, with built-in instant-search and Quick Look preview. No more CLI.

## Quick Start

```bash
# Build the app (Go c-archive + Swift app, all in one)
./scripts/build-app.sh

# Install to /Applications
./scripts/install.sh

# Or with LaunchAgent for auto-start on login:
./scripts/install.sh --launchd

# Launch
open /Applications/JuiceMount.app
```

You'll see a small drive icon appear in your menu bar. Click it to open the popover.

## Architecture

```
┌─────────────────────────────────────┐
│  JuiceMount.app (Swift, AppKit)     │
│  ┌─────────────────────────────┐    │
│  │  MenuBarController          │    │
│  │  ├─ MenuPopoverView         │    │
│  │  ├─ SearchWindowView        │    │
│  │  └─ PreferencesWindowView   │    │
│  └──────────────┬──────────────┘    │
│                 │                    │
│  ┌──────────────▼──────────────┐    │
│  │  ServerController (@Observable) │
│  │  └─ NFSBridge (Swift)         │    │
│  └──────────────┬──────────────┘    │
│                 │ C calls            │
│  ┌──────────────▼──────────────┐    │
│  │  libnfsd.a (Go c-archive)    │    │
│  │  Full JM5 stack:             │    │
│  │  • NFS server                │    │
│  │  • SQLite metadata + FTS5    │    │
│  │  • Redis sync                │    │
│  │  • JuiceFS FUSE manager      │    │
│  │  • SSD cache reader          │    │
│  │  • Memory buffer             │    │
│  │  • Write spool + drainer     │    │
│  └──────────────────────────────┘    │
└─────────────────────────────────────┘
```

The menu bar app and the Go core run **in the same process**. There's no IPC overhead — function calls cross language via the C ABI.

The Go stack also runs the **write spool + drainer** (Option 2) when `JM_SPOOL_ENABLE=1` — see `ARCHITECTURE_juicemount.md` § 15. Its menu-bar surface (a *Pending uploads* section + an icon badge) is not yet wired; pending-upload status is currently exposed on the control plane at `127.0.0.1:11050/spool`.

## Features

### Menu Bar Popover

Click the menu bar icon to see:

- **Status header** with green/yellow/red dot indicator
- **Volume info**: mount point, total entries indexed, last sync time
- **Health indicators** for Redis, MinIO, and FUSE
- **Cache section** (added 2026-05-08):
  - Offline mode toggle — flip to refuse un-pinned reads in <100 ms instead of waiting on a 30 s NFS-retry timeout. Useful on cellular, in airplane mode, or when the NAS is unreachable.
  - **Pin Folder for Offline…** button — opens a native folder picker rooted at the mount; selected folders are recursively scanned and queued for pre-cache.
  - Aggregate cache counts (Ready / Pending / Failed / total bytes / cached bytes)
  - Per-pinned-root list with Ready/Pending counts and progress
  - Live prefetch row (current file being prefetched + bytes prefetched this session)
  - Disk-space row: `38 GB free · 283 GB reclaimable [Reclaim]` — Reclaim button thins Time Machine local snapshots and surfaces APFS purgeable space for JuiceFS to use.
  - Pressure banner (only shown when actually under pressure):
    - 🔴 free < 1% of total disk → JuiceFS has stopped caching
    - 🟠 free < 3% of total disk → JuiceFS will stop caching at 1% free
    - 🔴 pinned set > total disk capacity
- **Action buttons**:
  - Start / Stop JuiceMount
  - Search Files... (⌘⇧F from anywhere)
  - Sync Now — runs metadata reconciliation AND verify-and-repair on the pin set (re-prefetches anything whose disk coverage might be incomplete)
  - Preferences...
  - Quit

### Status States

| State | Icon | Meaning |
|-------|------|---------|
| Idle | externaldrive | Server not started |
| Starting | externaldrive.badge.timemachine | Initial sync in progress |
| Running | externaldrive.fill.badge.checkmark | All systems healthy |
| Syncing | externaldrive.badge.timemachine | Periodic reconciliation |
| Degraded | externaldrive.fill.badge.exclamationmark | Redis or MinIO unreachable |
| Disconnected | externaldrive.fill.badge.xmark | FUSE down or NFS unmounted |

### Search Window (⌘⇧F)

The killer feature. Press the global hotkey or click "Search Files..." to open.

- **Type to search**: 150ms debounce, results from SQLite FTS5 trigram index (sub-50ms even at 100K+ entries)
- **Scope picker**: Whole library, SFX, LUTs, Footage, Film Projects, Music
- **Results table**: Name (color-coded by type), Path, Size
- **Spacebar → QuickLook**: instant preview of the selected file. Video plays inline, audio plays, images show. This is what you can't get from Finder search on NFS.
- **Enter / Double-click → Reveal in Finder**: opens Finder window with the file selected
- **Drag & drop**: drag results directly into Premiere/Resolve/FCPX timelines
- **Right-click**: Open in Finder, Quick Look, Copy Path

### Preferences Window

Four tabs:

- **General**: Volume name, mount point, start at login, search hotkey toggle
- **Server**: Redis URL, NFS listen address, live health status
- **Cache**: SSD cache size (10GB–2TB), memory buffer budget (128MB–16GB), buffer file size limit
- **Advanced**: Reconcile interval, database path, reset metadata cache, restart server

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| ⌘⇧F | Open search window (global, from any app) |
| Spacebar | Quick Look preview (in search window) |
| ↩ | Reveal selected in Finder |
| ⌘, | Open Preferences |
| ⌘Q | Quit JuiceMount |

## Build Scripts

| Script | Purpose |
|--------|---------|
| `scripts/build-app.sh` | Full build: Go c-archive + Swift app + .app bundle + codesign |
| `scripts/build-app.sh --debug` | Debug build (faster compile, slower runtime) |
| `scripts/build-cli.sh` | Standalone CLI binary (`/tmp/jm5`) with codesigning |
| `scripts/install.sh` | Install to /Applications |
| `scripts/install.sh --launchd` | Install + enable LaunchAgent for auto-start |
| `build-lib.sh` | Just the Go c-archive (legacy) |

## File Structure

```
JuiceMount6/
├── app/JuiceMount/
│   ├── Package.swift                       # SPM manifest
│   ├── Resources/
│   │   └── Info.plist                      # Bundle Info.plist
│   └── Sources/
│       ├── JuiceMountCore/
│       │   ├── JuiceMountCore.c           # Stub for SPM C target
│       │   └── include/JuiceMountCore.h   # Mirrors libnfsd.h exports
│       └── JuiceMount/
│           ├── App.swift                   # @main + AppDelegate
│           ├── Core/
│           │   ├── NFSBridge.swift         # Swift wrapper over c-archive
│           │   ├── ServerController.swift  # @Observable lifecycle
│           │   ├── Preferences.swift       # UserDefaults model
│           │   ├── HotkeyManager.swift     # Carbon global hotkey
│           │   └── LoginItemManager.swift  # SMAppService
│           └── UI/
│               ├── MenuBarController.swift # NSStatusItem + popover
│               ├── MenuPopoverView.swift   # Popover SwiftUI
│               ├── SearchWindowView.swift  # Search + Table
│               ├── QuickLookCoordinator.swift
│               └── PreferencesWindowView.swift
├── bridge/
│   └── cbridge.go                          # Go c-archive exports
└── scripts/
    ├── build-app.sh
    ├── build-cli.sh
    ├── install.sh
    └── com.juicemount.agent.plist
```

## Troubleshooting

**Menu bar icon doesn't appear**
- The app uses `LSUIElement=true` (no Dock icon). The icon is in the right side of your menu bar.
- Check Activity Monitor for "JuiceMount" process. If running but no icon, run `killall SystemUIServer` to refresh the menu bar.

**"Damaged or unsigned" warning on launch**
- Right-click the app and choose Open the first time. macOS Gatekeeper blocks ad-hoc-signed apps by default; this whitelists it.
- For long-term, sign with a Developer ID: `codesign --force --sign "Developer ID Application: <your name>" --entitlements entitlements.plist --options runtime build/JuiceMount.app`

**Server starts but Finder can't access /Volumes/zpool**
- Check that the path isn't already mounted by something else: `mount | grep zpool`
- Check the Preferences → Server tab for health status; Redis or MinIO may be down.

**Search returns no results**
- Click "Sync Now" in the popover. The FTS5 index rebuilds at the end of every sync.
- Verify the metadata cache has entries: popover should show "Entries: N" where N > 0.

**Hotkey ⌘⇧F doesn't work**
- Some apps capture this shortcut globally (e.g., browser "Find in Page"). Toggle it off in Preferences if needed; you can still launch search from the menu.

## Finder Right-Click ("Services")

JuiceMount registers two macOS Services on launch (declared in `Info.plist` `NSServices`, dispatched via `NSApp.servicesProvider = finderService`):

- **JuiceMount: Pin for Offline** — select one or more folders in Finder, right-click → Services → "JuiceMount: Pin for Offline". Same effect as the popover Pin Folder button, but inline in your existing workflow.
- **JuiceMount: Toggle Offline Mode** — keyboard-shortcut-able toggle.

First-time setup: macOS hides Services items by default. Enable them once in System Settings → Keyboard → Keyboard Shortcuts → Services → Files and Folders → check "JuiceMount: Pin for Offline". May require a `killall Finder` to refresh.

## HTTP Control Plane

The popover talks to the running app via in-process C calls, but the same operations are exposed as HTTP routes on the metrics port (`127.0.0.1:11050`) so external scripts and the `cmd/juicemount` CLI can drive them too:

| Route | Method | Purpose |
|-------|--------|---------|
| `/metrics` | GET | Prometheus-style RPC counters + latencies |
| `/cache-status` | GET | Pin aggregate, per-root, live prefetch, offline mode |
| `/pin?path=...` | POST | Register a folder for offline pinning |
| `/unpin?path=...` | POST | Remove from pin registry |
| `/offline?on=1\|0` | GET | Toggle offline mode |
| `/reclaim` | POST | Thin Time Machine local snapshots; returns freed bytes |
| `/verify-pins` | POST | Mark every pinned-Ready/Failed entry Pending so prefetcher re-verifies coverage |

## Logging

Go-side structured JSON logs are written to `~/Library/Logs/JuiceMount/juicemount.log`. Size-rotated (16 MB × 5 generations = 80 MB cap). The JuiceFS daemon's own log (`~/.juicefs/juicefs.log`) is auto-tailed and its WARNING/ERROR records are promoted into our log with the chatty "space not enough on device" pattern aggregated into a 60 s summary (so a flooded daemon log doesn't drown ours).

For live debugging:
```bash
tail -f ~/Library/Logs/JuiceMount/juicemount.log | jq .
```

## Future Work

See ROADMAP.md Phase 4 — codec-aware Quick Look proxies, content-hash backup verification, automatic bandwidth-aware mode, project version history, NLE bin sharing.
