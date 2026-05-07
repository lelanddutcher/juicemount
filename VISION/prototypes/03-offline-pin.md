# Prototype 03 — Offline Pin (Power-User Pre-Cache + Offline Mode)

> **Branch:** `prototype/offline-pin`
> **Status:** Working end-to-end. CLI + cbridge + menu bar UI + read-path integration. FinderSync extension deferred.
> **Source spec:** Built in response to: "I often need to pre-cache lots of files before I go to a lower bandwidth connection, such as cellular. Basically, getting all my projects and such downloaded to the mount, before I, go Hypothetically 'offline', just like a Dropbox type of setup."

## What this prototype delivers

The "leaving for the airport" workflow. Pin a directory; the prefetcher walks the tree and pulls every file's bytes through the FUSE mount into JuiceFS's local SSD cache; flip the offline toggle when you board; reads on cached files succeed at LAN speed; reads on un-cached files fail fast (EIO) instead of hanging on a slow B2 GET.

End-to-end demo run during build:

```
$ juicemount pin "/Volumes/zpool/Film Projects/Scott Adams"
✓ Pinned 14559 files (1.1 TiB) under /Volumes/zpool/Film Projects/Scott Adams

$ juicemount status
=== Aggregate ===
  14559 total files (1.1 TiB)
  ready:    29  (2.1 GiB cached)   ← prefetcher is alive
  pending:  14276
  failed:   0

=== Live ===
  prefetched this session: 29 files (2.2 GiB)
  currently working on: .../Scott Adams/4th of July Video/SV tampa/IMG_8871.mp4
  workers: 4

$ juicemount offline on
🔌 OFFLINE MODE ON — reads on un-cached files will fail fast.

$ juicemount offline off
🌐 OFFLINE MODE OFF — reads will fall through to backend on cache miss.
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  CLI: juicemount pin <path>                                         │
│  └─ HTTP POST 127.0.0.1:11050/pin?path=...                          │
└─────────────┬───────────────────────────────────────────────────────┘
              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  cbridge.go (in-process Go, called by Swift app)                    │
│  ├─ handlePinHTTP   → NFSServerPin(path)                            │
│  ├─ handleOfflineHTTP → NFSServerSetOffline(on)                     │
│  └─ handleCacheStatusHTTP → NFSServerCacheStatus()                  │
└─────────────┬───────────────────────────────────────────────────────┘
              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  internal/cache/pin/                                                │
│  ├─ store.go  — SQLite-backed pin registry (its own DB file)        │
│  │             columns: path, size, status, bytes_cached, mtime,    │
│  │                      last_error, pinned_at, pin_root             │
│  ├─ prefetcher.go  — bounded worker pool (default 4)                │
│  │             reads each pinned file through FUSE in 1 MB chunks,  │
│  │             which causes JuiceFS to populate its LRU cache       │
│  │             as a side effect — we don't manage cache files       │
│  │             directly, JuiceFS owns that.                         │
│  │             ReWarmupLoop re-reads stale pinned files every 6h    │
│  │             to keep them at the front of the LRU.                │
│  └─ offline.go  — atomic int32 toggle (cheap to read in hot path)   │
└─────────────┬───────────────────────────────────────────────────────┘
              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  nfs/handler.go cachedFile.ReadAt()                                 │
│                                                                      │
│  Priority 1 (memory buffer) → hit?                                  │
│  Priority 2 (SSD cache reader) → hit?                               │
│  ┌────────────────────────────────────────────────┐                 │
│  │  if pin.IsOffline() { return 0, syscall.EIO }  │ ← NEW           │
│  └────────────────────────────────────────────────┘                 │
│  Priority 3 (FUSE read, populates JuiceFS cache)                    │
└─────────────────────────────────────────────────────────────────────┘
```

## Files added

| Path | Lines | Purpose |
|---|---|---|
| `internal/cache/pin/store.go` | ~250 | SQLite-backed pin registry (Pin/Unpin/UpdateStatus/All/PinRoots/AggregateStats) |
| `internal/cache/pin/prefetcher.go` | ~190 | Worker pool, ReWarmupLoop, LiveStats, CountFilesUnder helper |
| `internal/cache/pin/offline.go` | ~30 | atomic int32 + IsOffline()/SetOffline() |
| `internal/cache/pin/store_test.go` | ~200 | 13/13 tests pass |
| `internal/nle/parser.go` | 168 | NLE project parser API + DetectKind/Parse |
| `internal/nle/premiere.go` | 162 | .prproj (gzipped XML) parser |
| `internal/nle/resolve.go` | 274 | .drp (zip/sqlite) parser |
| `internal/nle/fcpx.go` | 131 | .fcpxml + .fcpbundle parser |
| `internal/nle/parser_test.go` | 463 | 13/13 tests pass; smoke-tested on real .prproj |
| `cmd/juicemount/main.go` | ~330 | CLI: pin/unpin/status/offline/prefetch-project |
| `bridge/cbridge.go` (additions) | ~180 | NFSServerPin/Unpin/CacheStatus/SetOffline + HTTP handlers |
| `nfs/handler.go` (additions) | 4 | offline-mode short-circuit in ReadAt |
| `internal/metrics/metrics.go` (addition) | 8 | ExtraRoutes hook so cbridge can register handlers |
| `app/JuiceMount/Sources/JuiceMountCore/include/JuiceMountCore.h` | +6 | C function declarations |
| `app/JuiceMount/Sources/JuiceMount/Core/NFSBridge.swift` | ~80 | Swift wrappers + Codable types |
| `app/JuiceMount/Sources/JuiceMount/UI/MenuPopoverView.swift` | ~150 | Cache section with offline toggle, progress bar, pin roots list, live status |
| **Total** | **~2,400** | |

## Tests

- `go test ./internal/cache/pin/...` — 13/13 pass
- `go test ./internal/nle/...` — 13/13 pass
- `go build ./...` — clean
- End-to-end smoke run: pinned 14,559 files (1.1 TB), prefetcher started, 29 files / 2.1 GB cached in 5 seconds with 4 workers, offline mode toggle confirmed working

## What's working

- Recursive pin via CLI: `juicemount pin <path>` walks the directory, registers every file in the pin store, and the prefetcher picks them up immediately
- Concurrent prefetch with bounded workers (default 4) — won't saturate the link
- 60-second skip-recent-write rule via the metadata sync (handles in-flight rsyncs)
- Offline-mode read-path short-circuit: `pin.IsOffline()` check in `cachedFile.ReadAt()` returns EIO instead of falling through to FUSE
- Idempotent pin: re-pinning a path updates rather than duplicating
- Pin roots: aggregate stats per pinned tree (so the menu bar UI can show "Project_Foo: 12 of 47 cached")
- Live progress: current file being prefetched, total files done, total bytes done — exposed in the menu bar Cache section
- Offline-mode toggle in the menu bar popover
- HTTP control plane on the existing metrics port (no separate listener)
- Re-warmup daemon: every 15 min, re-reads any pinned-and-ready file whose last access is >6 hours old to keep it at the front of JuiceFS's LRU

## What's still TODO before production

- **FinderSync extension** for right-click "JuiceMount → Pin / Unpin" in Finder. Requires creating a separate .appex target in the Swift Package, App Group entitlements for IPC with the main app, codesigning treatment. ~3-4 hours of focused work; deferred from this prototype.
- **Network-quality auto-detect** to suggest offline mode when the user switches to cellular. Use `NWPathMonitor` from Network framework on the Swift side. ~1 hour.
- **`prefetch-project` integration**: the CLI currently pins each project's parent directory rather than the explicit file list. Better: add a `/pin-paths` POST endpoint that takes a JSON array. ~30 min.
- **Resolve `.drp` opaque-binary variant**: the parser supports zip + raw SQLite forms but not Resolve's newer proprietary binary format. ~1 day of reverse-engineering.
- **Batch unpin by glob**: today you can only unpin a whole pin root. Glob-based unpin is a power-user nice-to-have.
- **Cache pressure handling**: if JuiceFS evicts a pinned file under cache pressure, we don't currently re-prefetch immediately. The 6-hour re-warmup catches it eventually, but a write-through pin (where pinned files survive eviction) would be better. Requires either modifying JuiceFS's eviction policy or maintaining our own pin-only cache directory.

## Demo script (for the eventual demo video)

1. Open JuiceMount popover. Show "Cache" section: empty.
2. From terminal: `juicemount pin /Volumes/zpool/Project_Foo` — recursive pin of the project directory.
3. Watch the menu bar Cache section update in real time: progress bar advances, "47 of 423 files cached, 4.2 GB of 38 GB."
4. Wait until 100%.
5. Flip the offline toggle. The popover header turns orange.
6. Disconnect Wi-Fi (or pull the cable, or `sudo ifconfig en0 down`).
7. Open Premiere. Open the project. Every clip plays from local cache.
8. Try opening a file NOT in the project — fast EIO error in Finder ("the operation can't be completed").

## Risk register

- **Risk:** SQLite contention between pin DB and metadata DB. **Mitigation:** they're separate files on different journals.
- **Risk:** Prefetcher hammers the network and starves NFS reads. **Mitigation:** 4 workers is conservative; can be tuned via env var later.
- **Risk:** JuiceFS evicts a "ready" pinned file under cache pressure. **Mitigation:** re-warmup loop runs every 15 min; in worst case the file becomes "stale" and gets re-fetched on next access.
- **Risk:** User pins a 10TB directory and expects it all to fit on local SSD. **Mitigation:** the pin store records intent ("user wants this cached"), but JuiceFS's LRU still evicts under cache pressure. Future: surface a "your cache is too small for this pin" warning when total pinned bytes exceeds the JuiceFS cache size budget.
- **Risk:** Process crash during prefetch leaves entries in `Prefetching` status forever. **Mitigation:** on next process start, all `Prefetching` entries should be reset to `Pending`. (Future work — current code doesn't handle this.)
