# Tier 4 — Network resilience

The user-friendliness differentiator from LucidLink. LucidLink treats
every connection the same; we adapt. This is also where "open-source
LucidLink for creators on flaky home/coffee-shop Wi-Fi" stops being
marketing and becomes architecture.

## Why this matters (and why parts of it block tier 1)

2026-05-16 incident, post-mortem after the user pushed back on our
"Redis is fragile" framing:

```
redis EVAL: dial tcp 192.168.0.210:6379: connect: no route to host
```

This is a **client-side network state error**, not a backend
fault. The Mac's kernel didn't have a route to the backend IP at
the moment of the call. Causes include Wi-Fi re-association, sleep/
wake transitions tearing down the IP stack, route-table reset on
network change, VPN reconnection, or simply leaving the building.
Corroborating evidence in the same log window:

- Zero MinIO errors. If the backend host had gone down, both Redis
  AND MinIO calls would fail. They didn't.
- 21 consecutive Redis "no route to host" failures from 04:29 to
  06:10 (~2 hours), then nothing for 7 hours, then sporadic. That
  pattern matches client-side wake/sleep cycles, not backend
  instability.

The product impact today: when the Mac's path to the backend drops
mid-session, the mount becomes user-visibly broken even for files
the user has pinned for offline use. Finder waits for kernel-NFS
timeouts (~7s) on every operation instead of failing fast with
"offline." That contradicts the LucidLink-class positioning in
`VISION.md` (open-source self-hostable streaming + offline pinning).

## Splitting offline resilience: tier-1 vs tier-4

| Capability | Tier | Rationale |
|---|---|---|
| Detect network loss to metadata host | **1** | Stability — without this, every operation pays full kernel-NFS timeout |
| Auto-engage offline mode (read pinned + cached, fail-fast un-pinned) | **1** | Stability — "no Finder freeze on wedged backend" (acceptance test 1.2) covers this |
| Surface "offline" state clearly in UI | **2** | Polish, but cheap to do alongside tier-1 work |
| Bandwidth probe + per-project budgets | **4** | Real tier-4 — only matters once the basic offline story works |
| Resumable warmup with persistent progress | **4** | Tier-4 |
| Write queue with local journal + replay on reconnect | **4 / 7** | Bigger feature; some of it overlaps with collaboration's conflict resolution |
| Deferred indexing (mount usable immediately, FTS in background) | **4** | First-mount UX |

Read this doc top-to-bottom for the offline-resilience plan. Tier-1
work is the first section; tier-4 work is the rest.

---

## Tier-1 component: Graceful degradation on network loss

### Acceptance tests (added to tier 1)

| # | Test | Pass criterion |
|---|---|---|
| 1.7 | Walk-out scenario: disconnect Wi-Fi while mount is active. Pinned files remain readable. Un-pinned reads fail-fast with `media not available offline`, never hang past 2 s. | Stat on pinned file: <100 ms. Stat on un-pinned cold file: error in <2 s, NOT EIO from kernel timeout. |
| 1.8 | Auto-engage: from the moment the Mac kernel's route to the metadata host disappears, the mount must be in offline state within 5 s. | Menu bar icon updates; subsequent ops follow offline semantics; log message clearly says "offline mode auto-engaged: route to backend lost." |
| 1.9 | Auto-recover: when the route returns, the mount must be back in online state within one reconciliation cycle (~30 s) without user action. | Log message "reconciliation recovered" within 30 s; subsequent un-pinned reads work again. |
| 1.10 | Error classification: any "no route to host" / "i/o timeout" error must NOT be logged as "Redis is degraded" or imply backend trouble. It's a network-path issue. | Log message says "network path to backend lost" or similar; UI distinguishes "your network is degraded" from "the server is down." |

### Architecture additions

```
                  Network state monitor
                  ┌────────────────────┐
                  │  health/network.go │ (new)
                  │                    │
                  │ - route-change kqueue (PF_ROUTE)
                  │ - active probe to metadata host (~10 s cadence)
                  │ - exposes IsReachable(host) bool
                  └────────────────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │   offline manager  │ (refactor pin.offline.go)
                  │                    │
                  │ - manual user toggle (today)
                  │ - automatic from network monitor (new)
                  │ - one bool: IsOffline() — true if either fired
                  └────────────────────┘
                            │
                            ▼
   ┌─────────────────┐   ┌──────────────────┐   ┌─────────────────┐
   │ NFS handler     │   │ metadata.RedisCli│   │ Self-test       │
   │                 │   │                  │   │                 │
   │ on Read miss:   │   │ on offline:      │   │ if offline:     │
   │  - serve from   │   │  skip reconcile  │   │  test pinned    │
   │    cache iff    │   │  attempts        │   │  data only      │
   │    pinned       │   │                  │   │                 │
   │  - else EOFFLINE│   │                  │   │                 │
   └─────────────────┘   └──────────────────┘   └─────────────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │ Menu bar icon      │
                  │  online → ●green   │
                  │  offline → ●blue   │ (new state, not yellow)
                  │  degraded → ●yellow│
                  └────────────────────┘
```

### Tier-1 fix list (ordered)

**1. Classify network errors correctly** (~2 hours)

In `metadata/redis.go` `doReconcile`, classify the error. If it's
`net.OpError` with `syscall.EHOSTUNREACH` or `i/o timeout`, log as
"network path to backend lost" and don't increment the "Redis
unhealthy" component counter. Internal: introduce a typed error
distinguishing `errNetworkPath` from `errBackendError`.

Acceptance: re-walk the log lines from 2026-05-16 — they should now
say "network path lost" not "Redis degraded."

**2. Network reachability monitor** (~1 day)

New `health/network.go`:

- Subscribe to PF_ROUTE kqueue events (macOS-native; same mechanism
  the existing network-change detector already uses internally).
  When the default route or relevant subnet route changes, mark
  state stale.
- Active probe: every 10 s, attempt a 1 s TCP dial to the configured
  metadata host:port. Track last successful reach.
- Exposes `func ReachableToBackend(timeout time.Duration) bool` and
  `func TimeSinceLastReachable() time.Duration`.

Acceptance: disconnect Wi-Fi; within 5 s, `ReachableToBackend` returns
false. Reconnect; within 10 s, returns true.

**3. Auto-engage offline mode** (~3 hours)

`internal/cache/pin/offline.go`:

- Existing `SetOffline(bool)` becomes user-only manual override.
- New `setAutoOffline(reason string)` driven by the network monitor.
- `IsOffline()` returns `userOffline || autoOffline`.
- Reason field exposed via metrics endpoint so UI can show "offline:
  network unreachable" vs. "offline: user toggle."

Acceptance: disconnect Wi-Fi; within 5 s, `IsOffline()` returns true
with reason "auto: network unreachable." Re-connect; reverts within
30 s.

**4. Handler refuses un-pinned reads when offline, fast** (~4 hours)

`nfs/handler.go` `cachedFile.ReadAt`:

- Already has `pin.IsOffline() && !f.pinned` → return EIO (today).
- Change error to a distinct `syscall.ENXIO` ("media not available
  offline") so macOS shows a better Finder error message.
- Add: gate `Stat` / `Lookup` paths to also fail-fast for un-pinned
  un-cached paths during offline. Currently they fall through to
  FUSE which then takes seconds to fail.
- Pinned files keep working via the cache path (memBuf / cacheReader
  / FUSE if locally cached) — unchanged.

Acceptance: while offline, opening a pinned `.prproj` works in
<200 ms. Stat'ing an un-pinned cold file returns "not available
offline" in <2 s, no kernel timeout.

**5. UI surfaces offline state distinctly** (~2 hours)

`app/JuiceMount/Sources/JuiceMount/UI/`:

- Menu bar icon gains a blue dot for "offline by intent or network."
- Popover header shows "Offline: 5 m 20 s • 14 pinned available."
- Notification on auto-engage and recovery (opt-in via Preferences).

Acceptance: visual verification during walk-out scenario.

### Tier-1 commit plan

| # | Commit | Scope |
|---|---|---|
| 1 | `fix(metadata): classify network errors vs backend errors` | Step 1 above |
| 2 | `feat(health): network reachability monitor` | Step 2 above |
| 3 | `feat(pin): auto-engage offline mode on network loss` | Step 3 above |
| 4 | `fix(handler): fail-fast for un-pinned ops when offline` | Step 4 above |
| 5 | `feat(ui): offline-state indicator in menu bar` | Step 5 above |

Each commit independent. Each gets a code-reviewer pass per the loop
checklist (handler + metadata + mount-lifecycle changes all qualify).

---

## Tier-4 component: Full offline workflow

These are the features that turn "graceful degradation" into "full
LucidLink-class offline support." Build only after tier-1 closes.

### Features

**Bandwidth probe on launch** (~1 day)

Measure RTT + throughput to the metadata host and a sentinel object
in MinIO on app start. Store baseline per network (SSID hash or
default-gateway MAC). Detect cellular by interface type (`en` with
LTE/5G modem). Use baseline to:

- Set initial warmup chunk size.
- Refuse high-bandwidth ops on cellular by default (user override).
- Display "Wi-Fi (180 MB/s)" or "Cellular (8 MB/s)" in popover.

Reference: LucidLink does the bandwidth probe; doesn't expose
budget knobs. We do both.

**Per-project warm budget** (~2 days)

`.juiceproject` YAML (also tier 6) declares:

```yaml
budget:
  cellular_mb: 50000      # don't warm beyond 50 GB on cellular
  wifi_mb: 500000         # 500 GB on Wi-Fi
prefetch_paths:
  - "Footage/2024/A001"
  - "Audio/dialog/scene12"
```

When the project is opened (or `.juiceproject` is double-clicked),
the prefetcher warms the listed paths up to the active-network's
budget. Track per-project bytes-cached so a second open of the same
project is fast.

**Resumable warmup with persistent progress** (~3 days)

Persist warmup progress to disk so killing the app mid-warm doesn't
restart from zero. Per-file: stored in pin store. Per-project:
manifest in `~/Library/Application Support/JuiceMount/projects/`.

On launch, if the active project has a partial warmup, resume from
the last completed file boundary.

**Cellular-aware offline gate** (~half day)

When `health/network.go` detects cellular: prompt user once via
notification ("On cellular — flip to offline mode? Pinned files
still work."). User can save preference per SSID.

**Deferred indexing** (~2 days)

First mount returns instantly without waiting for the full
`SyncOnce` to populate the FTS5 index. Search returns "indexing,
N/M files complete" until done. Background goroutine indexes;
periodic checkpoint to SQLite. Survives restart.

**Sparse first-mount** (~3 days)

Don't pull the full Redis path list on cold start. Lazy-load
directory contents on first `ListChildren` per directory. For users
with 500K+ files in their bucket, this changes first-mount time
from minutes to seconds.

### Acceptance tests (tier 4)

| # | Test | Pass criterion |
|---|---|---|
| 4.1 | First-mount on fresh box, 100K-file bucket | Mount usable in <5 s; FTS background-indexes within 5 min |
| 4.2 | `.juiceproject` warm-up | `git clone + open .juiceproject` → ready to edit in <2 min |
| 4.3 | Resumable warm | `kill -9` mid-warm; relaunch picks up at same byte boundary |
| 4.4 | Bandwidth-aware throttle | On cellular, fetches respect declared budget; banner shows clearly |
| 4.5 | Cellular-aware offline auto-prompt | One notification, dismissible, per-SSID memory |

---

## Anti-patterns to avoid

- **Don't auto-engage offline on a single failed RPC.** Use the
  network monitor's signal, which has its own debounce / probe
  cadence. A single timeout in flight when the network is fine
  shouldn't flip the whole mount.
- **Don't queue writes silently.** If the user writes while offline
  and we queue, they expect to see it persisted. If we can't promise
  conflict-free replay (tier 7), refuse the write upfront.
- **Don't conflate "offline-by-intent" with "offline-by-network."**
  These are different states with different recovery paths. The
  `pin.IsOffline()` API should expose both.
- **Don't probe the backend with a "real" RPC.** The probe should
  be a cheap TCP connect, not a Redis EVAL or a MinIO HEAD. A real
  RPC eats RTT and adds load to a flaky network.

## Dependencies on / blockers from other tiers

- Tier 1 advancement waits on items 1.7–1.10 in the section above.
- Tier 2 (UI polish) overlaps step 5 (offline state surface) but
  the cheap version belongs here; the polished version belongs to
  tier 2.
- Tier 6's `.juiceproject` schema is consumed by tier 4's per-project
  warmup. Tier 4 can ship a partial version (manual pin) before
  tier 6 lands.

## Iteration plan — tier-4-proper (after tier-1 closes)

The tier-1-blocking section above shipped 2026-05-16 (commits in
docs/STATE.md). What remains is the actual tier-4 work — the polished
offline workflow built on the foundation.

| # | Slice | Hours | Files |
|---|---|---|---|
| 4.A.1 | Bandwidth probe on launch: measure RTT + throughput to metadata host, store baseline per SSID | 5 | new `health/bandwidth.go` |
| 4.A.2 | Cellular detection (interface type + CTCellularData), inform `pin.SetAutoOffline` rules | 3 | extend `health/network.go` |
| 4.A.3 | Bandwidth display in popover ("Wi-Fi (180 MB/s)") | 2 | `MenuPopoverView.swift` |
| 4.B.1 | `.juiceproject` YAML schema parser + per-project warm-budget store | 5 | new `internal/project/` |
| 4.B.2 | Prefetcher consumes per-project budget (replaces global cache-size knob) | 4 | extend `internal/cache/pin/prefetcher.go` |
| 4.B.3 | Project-open hook (double-click `.juiceproject` → set active project → trigger warmup) | 3 | Swift app + `URL scheme` registration |
| 4.C.1 | Resumable warmup: persist per-file progress to pin store | 4 | extend `pin.Store` schema |
| 4.C.2 | Per-project manifest in `~/Library/Application Support/JuiceMount/projects/` | 2 | new project-state package |
| 4.C.3 | Resume-on-launch path | 2 | App.swift + ServerController |
| 4.D.1 | Cellular-aware prompt-once-per-SSID | 3 | extend Preferences + notify path |
| 4.E.1 | Deferred FTS indexing: mount returns instantly, FTS builds in background with progress | 6 | `metadata/store.go` + UI surface |
| 4.E.2 | Indexing-progress percentage in popover | 2 | popover row |
| 4.F.1 | Sparse first-mount: lazy `ListChildren` per directory (skip archived projects) | 5 | extend `nfs/handler.go` ReadDir |
| 4.G.1 | Write queue local journal | 8 | new `internal/writequeue/` package |
| 4.G.2 | Reconnect-replay with conflict-detection | 6 | extend `pin` + write-queue |
| 4.G.3 | Merge-UI for conflicts (Swift sheet) | 4 | new `ConflictResolutionSheet.swift` |

Total: ~64 hours = ~8 working days of tier-4-proper work.

## Signals to watch — tier-4-proper

| Item | Signal |
|---|---|
| 4.A | Popover shows network name + measured throughput; first-mount on cellular asks user before warmup |
| 4.B | `.juiceproject` in a `git clone` → double-click → app pins listed paths, surfaces project in "Active Projects" tree within 10s |
| 4.C | `kill -9` mid-warm; relaunch resumes at byte-boundary (verify via pin store row count) |
| 4.D | `defaults read com.juicemount.app cellularPromptedSSIDs` lists each Wi-Fi the user has answered for |
| 4.E | First mount on a 500K-file bucket: mount usable <5s; FTS background-build completes within 5min; search returns "indexing N/M" until ready |
| 4.F | Mount on a backup-bucket with 50K archived files: `mount` returns <2s; `ls /Volumes/<name>` of unvisited dirs lazy-loads on first access |
| 4.G | While offline: write to a pinned project, kill -9, reconnect, relaunch → write appears in upstream within 60s. With conflict: merge UI surfaces, no silent loss. |
