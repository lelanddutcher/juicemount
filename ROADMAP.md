# JuiceMount Roadmap

**Goal:** Native macOS menu bar app with one-click mount, transparent operation, and zero CLI interaction.

---

## Current State

JM5 is a Go CLI binary that manages the full stack:
- Mounts JuiceFS FUSE (hidden from user)
- Syncs 147K metadata entries from Redis to local SQLite in ~7s
- Serves NFS v3 at localhost with sub-millisecond metadata
- Three-tier read path: memory buffer -> SSD cache -> FUSE
- Health monitoring with auto-recovery (FUSE remount, Redis reconnect)
- Launched via `ssh localhost` workaround for macOS 10GbE network filter

**What works well:**
- Directory browsing is near-instant (15-120ms vs 3-10s raw FUSE)
- Large file reads at full 10GbE throughput
- Write-back through JuiceFS with size tracking
- Multi-machine sync via Redis pub/sub

**What needs work:**
- CLI-only (no GUI)
- Requires `ssh localhost` launch for 10GbE routing
- No user-facing error reporting
- No connection status indicator
- No preferences UI

---

## Phase 1 — Stability & Polish
### 1.1 SQLite Single-Writer Goroutine
- All SQLite writes go through a channel-serialized writer
- Eliminates SQLITE_BUSY entirely — no retry logic needed
- Unblocks concurrent NFS operations during reconciliation

### 1.2 Incremental Redis Sync
- Track a high-water-mark mtime; Lua script only returns entries modified since last sync
- Full scan fallback if drift detected
- Reduces 147K-entry sync from 7s to <500ms for steady-state

### 1.3 Stale Entry Detection
- When NFS OPEN fails with ENOENT but metadata says file exists, remove the stale entry
- Prevents "phantom files" that appear in Finder but can't be opened
- Log and track stale entry rate for monitoring

### 1.4 Codesign the Binary
- Ad-hoc or Developer ID signing so macOS allows 10GbE network access without SSH workaround
- Required for Phase 2 (menu bar app can't launch via SSH)

### 1.5 Test Infrastructure
- TestMain-based shared server setup (avoid 147K sync per test)
- Finder simulation tests (LOOKUP -> GETATTR -> READDIR -> READ sequences)
- Stale entry detection tests
- FUSE crash/remount integration tests

---

## Phase 2 — Swift Menu Bar App (May 2026)

### 2.1 Architecture: Swift Shell + Go Core
```
+---------------------------+
|  Menu Bar App (Swift)     |
|  - SwiftUI status icon    |
|  - Preferences window     |
|  - Login item (LaunchAgent)|
+------------+--------------+
             |
         C bridge (cbridge.go)
         via c-archive .a library
             |
+------------+--------------+
|  JM5 Go Core              |
|  (existing code, compiled |
|   as c-archive)           |
+---------------------------+
```

The `bridge/cbridge.go` already exports C functions:
- `NFSServerStart(configJSON)` -> address string
- `NFSServerStop()`
- `NFSServerIsRunning()` -> bool
- `NFSServerStats()` -> JSON
- `NFSServerSyncNow()` -> status

### 2.2 Menu Bar Icon States
| State | Icon | Meaning |
|-------|------|---------|
| Connected | Green dot | All healthy, volume mounted |
| Syncing | Spinning arrows | Metadata sync in progress |
| Degraded | Yellow dot | Redis or MinIO unreachable, serving from cache |
| Disconnected | Red dot | FUSE down or NFS unmounted |
| Idle | Gray dot | Server stopped |

### 2.3 Menu Items
```
 [icon] JuiceMount
 ─────────────────
 Volume: /Volumes/zpool         (mounted)
 Entries: 146,758               (synced 3s ago)
 Health: Redis ✓  MinIO ✓  FUSE ✓
 ─────────────────
 Cache: 2.1 GB / 100 GB SSD
 Memory: 847 MB heap
 FD Pool: 12 open, 4 active
 ─────────────────
 Sync Now
 ─────────────────
 Preferences...
 Quit JuiceMount
```

### 2.4 Preferences Window
```
 [General]
   Volume Name:    [ zpool          ]
   Mount Point:    [ /Volumes/zpool ]
   Start at Login: [x]

 [Server]
   Redis URL:      [ redis://192.168.0.210:6379/1 ]
   MinIO URL:      [ http://192.168.0.212:9000     ]
   NFS Port:       [ 11049 ]

 [Cache]
   SSD Cache Size: [ 100 ] GB
   Memory Buffer:  [ 2   ] GB
   Buffer Files <  [ 128 ] MB

 [Advanced]
   Reconcile Interval: [ 30 ] seconds
   FD Pool Timeout:    [ 120 ] seconds
   Readahead Blocks:   [ 8  ] (32 MB)
```

### 2.5 LaunchAgent
- `~/Library/LaunchAgents/com.juicemount.agent.plist`
- Starts on login, keeps running in background
- Menu bar app communicates with Go core via the C bridge (in-process, not IPC)

---

## Phase 3 — Production Hardening 
### 3.1 Observability
- Structured JSON logging (replace `log.Printf`)
- Per-RPC-type latency histograms (GETATTR, LOOKUP, READ, WRITE, READDIR)
- Expose metrics via local HTTP endpoint for debugging
- Optional Prometheus/Grafana export


### 3.3 Bandwidth-Aware Mode
- Detect network quality (10GbE vs WiFi vs cell)
- On slow networks: disable readahead, increase cache TTLs, reduce sync frequency
- On fast networks: aggressive prefetch, short TTLs


### 3.5 Error Recovery
- If NFS mount goes stale (Finder shows spinning beach ball): auto-unmount and remount
- If Redis is unreachable for >5 min: surface notification to user via menu bar
- If FUSE crashes: auto-remount (already implemented) + notify user

---


---

## Deferred / Future

- **NFS v4.1**: Would allow server-initiated callbacks (delegations) for instant invalidation. Major protocol change.
- **Redis Streams**: Replace SUBSCRIBE + Lua SCAN with a Redis Stream for change tracking. Requires JuiceFS cooperation or a sidecar.

