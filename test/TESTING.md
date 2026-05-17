# JuiceMount5 Testing Guide

## What We're Proving

JuiceMount5's value proposition is a single sentence: **a video editor on a remote network should not be able to tell the difference between browsing local 10GbE storage and browsing NAS storage through JuiceMount5.**

The caching stack has three tiers, each covering a different axis of that claim:

| Tier | What it covers | Expected result |
|------|---------------|----------------|
| SQLite metadata | Stat, LOOKUP, READDIR | Identical latency on any network |
| JuiceFS SSD cache | Reads of previously-accessed blocks | Identical throughput on any network |
| JuiceFS → MinIO (cold) | First-ever read of uncached data | Limited by network bandwidth — expected to be slower |

The only acceptable degradation is cold reads. Everything else must feel local.

---

## What "Snappy" Means in Practice

**Do not** test by brute-force walking a directory tree (`find`, `os.walk`, Finder "Get Info on volume"). These operations populate results artificially and don't reflect editor workflow. Test the way editors actually use the system:

### Finder / file browser navigation
- Open a folder: triggers `READDIRPLUS` → should complete in <5ms from SQLite
- Click into a subfolder: same, should feel instant
- Hover a file to preview: triggers `GETATTR` → <1ms from SQLite
- **Do not** trigger Finder indexing or Spotlight during a benchmark run — both issue thousands of stats that warm caches artificially

### App open dialogs (Premiere, DaVinci, FCP)
- Use the app's actual File > Open or "Link Media" dialog to navigate the mount
- File chooser navigation = repeated READDIR + GETATTR; the editor must not see a spinner
- **Measure**: time from click to folder contents appearing (human-perceptible, not µs)
- Target: indistinguishable from a local drive at the Open dialog

### Media engine reads (playing/scrubbing footage)
- Sequential read of a video file triggers JuiceFS readahead (32MB, 4 workers)
- After first play-through, all blocks land in SSD cache → subsequent scrubs are SSD-speed
- Cold play (first time touching a clip): limited by WiFi/VPN bandwidth — this is acceptable
- **Measure**: dropped frames during playback, buffer-wheel duration on scrub

### `ditto` copy (Finder copy, export)
- Creates files via NFS → JuiceFS FUSE → MinIO
- 100% success required, no silent data corruption
- `ditto -V src/ /Volumes/zpool/dest/` and verify checksums

---

## Is This Test Telling Me Anything About My Code Change?

Before running a benchmark to evaluate a code change, answer these questions:

**"What NFS operation does my change affect?"**
Map the change to specific RPC ops. If you changed readahead, measure read throughput — not stat latency. If you changed SQLite query strategy, measure readdir and stat. Matching the test to the change is the only way to get signal.

**"Am I measuring the right layer?"**
- NFS protocol layer: use JM5 log `[WARN] slow RPC` rate and distribution
- SQLite layer: measure stat/readdir latency directly via NFS mount (not FUSE)
- SSD cache layer: measure warm read NFS vs FUSE ratio (should be >80%)
- Network layer: only matters for cold reads; warm reads should be network-independent

**"Is my baseline actually cold?"**
macOS kernel page cache, JuiceFS in-memory block cache, and SQLite in-memory path/inode caches all mask performance. A "cold" benchmark that isn't truly cold is meaningless. The full cold-state reset procedure is in the Pre-Test Checklist below.

**"Am I measuring variance, not just one run?"**
Single-shot numbers lie. Run each measurement at least 10 iterations and report p50/p95. A change that improves p50 but blows up p99 is not an improvement.

---

## Tests That Actually Catch Regressions

The current automated test suite (`go test ./...`) checks correctness but not performance regressions. Add these before production:

### 1. NFS RPC Slow-Log Rate
Parse `[WARN] slow RPC` lines from JM5 log after a standardized workload. The slow-log threshold is 100ms; any RPC above that counts.

```bash
# Run standardized workload (stat 200 paths + readdir 10 dirs + read 64MB)
# then:
grep "slow RPC" /tmp/jm5-bench.log | wc -l   # should be 0 on 10GbE, <5 on WiFi
grep "slow RPC" /tmp/jm5-bench.log | grep -v "nfs.Read"  # metadata slow RPCs = always a bug
```

A metadata RPC (Stat, Lookup, ReadDir) should **never** appear in the slow log on any network, because it serves from SQLite. If it does, there's a lock contention or fallthrough-to-FUSE bug.

### 2. Readahead Hit Rate
After playing a video file from start to finish, the readahead manager should have prefetched most blocks before they were needed. JM5 exposes this via `NFSServerStats()`:

```bash
curl -s http://127.0.0.1:11049/stats 2>/dev/null || \
  grep -o "readahead.*" /tmp/jm5-bench.log | tail -5
```

Target: readahead hit rate >70% on sequential reads.

### 3. FD Pool Utilization
The FD pool (max 256 open FUSE file descriptors) should not reach capacity during normal operation. If it does, reads block waiting for a slot.

```go
// nfs/fdpool.go: FDPool.Stats() returns (open, active int)
// Log this after each benchmark run
```

### 4. Concurrent Client Stress
Single-client benchmarks miss semaphore starvation and lock contention. Run N parallel readers simultaneously:

```python
# Launch 8 goroutines each reading a different 64MB file concurrently
# Measure: wall time for all to complete, p99 of individual reads
# Regression: any read taking >10x the single-client p50
```

### 5. Long-running Session Stability
Start JM5, run a steady workload (stat 10 paths/sec + read 1MB/sec) for 1 hour, watch for:
- Memory growth (Go heap should stay flat after GC stabilizes)
- SQLite cache drift (path/inode cache size should stabilize, not grow unbounded)
- SUBSCRIBE reconnect storms (if Redis hiccups, reconnect should not thrash)

### 6. SUBSCRIBE Propagation End-to-End
When a file is created/renamed/deleted on the NAS, it should appear in the NFS mount within 200ms. The existing `TestRedisSubscribePublish` covers this at the library level, but it should also be tested end-to-end:

```bash
# On NAS (or via juicefs CLI):
# touch /mnt/juicefs/test_subscribe_probe.txt
# Then immediately on Mac:
stat /Volumes/zpool/test_subscribe_probe.txt
# Should succeed within 200ms without waiting for 30s batch sync
```

---

## Network Scenarios to Test

Each scenario must be tested in a **cold state** (fresh FUSE mount, fresh JM5 DB, no kernel page cache hot). The cold state ensures we're testing the infrastructure, not macOS memory.

### Scenario 1: Local 10GbE
- Mac physically connected to NAS switch
- Expected NAS RTT: <1ms
- This is the baseline everything else is measured against
- All metadata RPCs should be <500µs; any slower is a code bug, not a network issue

### Scenario 2: WiFi on home network (same subnet)
- Mac on WiFi, NAS on 10GbE — same router
- Expected NAS RTT: 2–5ms
- Metadata should still be sub-ms (SQLite); if it's not, something is falling through to FUSE
- Warm reads should match 10GbE within 10%

### Scenario 3: Remote (hotspot / off-site) via Tailscale
- Mac on phone hotspot or external network, NAS reachable via Tailscale WireGuard
- Expected NAS RTT: 40–100ms
- **This is the killer scenario the whole stack is built for**
- Metadata: SQLite gives 300–1000x speedup over raw NAS RTT
- Reads warm: SSD cache makes it feel local
- Reads cold: limited by Tailscale + WiFi bandwidth — acceptable, but should show readahead helping

### Scenario 4: Network transition (sleep/wake, WiFi switch)
- Put Mac to sleep, switch networks (home WiFi → hotspot or vice versa)
- Wake Mac, try to open a file immediately
- Expected: health monitor detects FUSE death within 10s, remounts, NFS recovers
- **Do not manually remount** — the health monitor must handle it autonomously
- Check JM5 log: should see `[health] FUSE: healthy → unhealthy` then `recovered` within 15s

### Scenario 5: Redis/MinIO degradation
- On the NAS, temporarily firewall Redis port (6379) for 30s, then re-open
- JM5 should: continue serving metadata from SQLite, attempt SUBSCRIBE reconnect, resume sync after reconnect
- The health monitor should log `Redis: healthy → unhealthy` and `recovered`
- No NFS client errors should propagate during the 30s window (SQLite covers the gap)

---

## Pre-Test Checklist (Cold State)

Before any benchmark run, especially remote scenarios:

```bash
# 1. Verify juicefs-minio resolves (required for MinIO access off home network)
host juicefs-minio
# If NXDOMAIN: sudo bash -c 'echo "127.0.0.1   juicefs-minio" >> /etc/hosts'

# 2. Force-unmount stale FUSE (if it was up before network switch)
sudo umount -f ~/.juicemount/fuse-internal

# 3. Remount FUSE fresh
juicefs mount --background \
  --cache-dir ~/.juicefs/cache --cache-size 102400 \
  --attr-cache 3600 --entry-cache 3600 --dir-entry-cache 3600 \
  redis://127.0.0.1:6379/1 ~/.juicemount/fuse-internal

# 4. Kill existing JM5, start with fresh DB
pkill -f '/tmp/jm5'
rm -f /tmp/jm5-bench.db
ssh localhost "export PATH=/usr/local/go/bin:/opt/homebrew/bin:\$PATH && \
  /tmp/jm5 --redis redis://127.0.0.1:6379/1 \
  --fuse-path ~/.juicemount/fuse-internal \
  --mount /Volumes/zpool \
  --listen 127.0.0.1:11049 \
  --db /tmp/jm5-bench.db > /tmp/jm5-bench.log 2>&1" &

# 5. Wait for first metadata sync (~10s)
sleep 15 && grep "metadata sync" /tmp/jm5-bench.log | tail -1

# 6. Purge kernel page cache — MUST be done in your Terminal (requires sudo)
sudo purge
```

---

## What to Measure

### Metadata (should be network-independent on all scenarios)

```python
# Stat latency — 200 iterations on unique paths, report p50/p95/p99
# ReadDir — root, subdir with >20 items, report p50/min
# 50 concurrent stats via threading — report wall time and per-stat p50
```

Passing thresholds (any network scenario):
- Stat p50 < 1ms
- Stat p95 < 5ms
- ReadDir p50 < 10ms
- No metadata ops in `[WARN] slow RPC` log

### Read throughput (cold → warm cycle)

```python
# 1. Pick a pre-existing file committed to MinIO (not written by the benchmark — slow commits)
# 2. Read 64MB at an offset never accessed this session (guarantees cold)
# 3. Record cold throughput and elapsed time
# 4. Read same 64MB again (JuiceFS SSD cache should have it now)
# 5. Record warm throughput
# 6. Read same 64MB via FUSE directly (reference — bypasses NFS loopback)
# Report: cold_bw, warm_bw, fuse_bw, warm/cold ratio, NFS/FUSE ratio
```

Expected pattern:
- Cold: limited by network bandwidth (varies by scenario)
- Warm (NFS): should match FUSE warm within 20% overhead
- NFS/FUSE ratio on warm reads: >80%
- Warm/cold ratio: >5x (cache must provide measurable benefit)

### Mount resilience (health monitor)

```bash
# Force FUSE death: sudo umount -f ~/.juicemount/fuse-internal (with NFS still mounted)
# Measure: seconds until JM5 detects and remounts FUSE automatically
# Check: NFS client recovers without ESTALE errors after remount
# Target: <15s detection + remount, zero manual intervention
```

### Write path (when on local network)
Large NFS writes time out on slow WiFi (MinIO COMMIT). Only test writes on 10GbE or home WiFi:

```bash
# Create a 50MB file via NFS, verify it appears via FUSE and in Redis metadata
ditto /path/to/local/50mb_file.mov /Volumes/zpool/__bench_write_test.mov
# Verify:
md5 /path/to/local/50mb_file.mov
md5 /Volumes/zpool/__bench_write_test.mov  # must match
stat ~/.juicemount/fuse-internal/__bench_write_test.mov  # must exist and correct size
```

---

## Known Infrastructure Constraints

| Issue | Impact | Status |
|-------|--------|--------|
| `juicefs-minio` hostname only resolves on home LAN | Data reads/writes fail off home network | Fix: `sudo bash -c 'echo "127.0.0.1 juicefs-minio" >> /etc/hosts'` |
| NFS COMMIT on slow WiFi | Large writes (>50MB) time out | Known; writes work on local network only |
| Synthetic inodes (1<<63) | SQLite insert error on SUBSCRIBE echo-back | Needs fix before production |
| `go-nfs` exclusive create unsupported | Finder logs errors on some creates | Benign, falls back gracefully |
| READDIRPLUS root slow path | Dirs not in SQLite fall through to FUSE (slow on WiFi) | Needs fix: all accessed dirs should be cached in SQLite |
| `sudo purge` needed for true cold reads | Can't run from Claude Code (no sudo) | Always run manually in Terminal before read benchmarks |
| ESTALE after JM5 restart | Kernel vnode cache holds stale handles | Fixed by NFS remount; health monitor should automate this |

---

## Benchmark Baselines (measured values)

These live in `test/benchmark_baselines.json`. Update them after each significant infrastructure change. A result more than 2x worse than baseline on the same hardware = regression.

### 10GbE (local, reference)
- Stat p50: ~71µs
- ReadDir 111 entries: ~1.4ms
- Warm read (NFS): ~858–2054 MB/s
- Warm read (kernel cached): ~5700–12500 MB/s
- Cold read (SSD→NFS): ~1368–2054 MB/s
- Slow RPC count on standardized workload: 0

### WiFi + Tailscale (remote)
- NAS RTT: 42–60ms
- Stat p50: ~108–162µs (300–1100x faster than NAS RTT)
- ReadDir root (70 items): ~3.4–4.8ms
- ReadDir subdir (26 items): ~0.1–0.2ms
- FUSE stat (direct, no SQLite): ~120ms (one network round trip per stat)
- Cold/warm reads: pending `juicefs-minio` DNS fix; update here once measured
- Slow RPC count on metadata-only workload: 0 (reads may produce some)

---

## Test Gaps (not yet covered)

These are known blind spots. Fixing them would give much higher confidence in code changes:

- **Concurrent readers**: 8 simultaneous file reads — does semaphore pool saturate?
- **DaVinci / Premiere integration**: actual app open dialog and playback test, not synthetic
- **1-hour soak**: memory leak detection, SQLite cache drift, SUBSCRIBE reconnect behavior
- **Redis outage simulation**: 30s Redis firewall, verify SQLite serves the gap
- **QuickLook / Spotlight**: do macOS system services work correctly on the mount?
- **Large directory**: a directory with 10,000+ files — does READDIRPLUS stay <10ms?
- **JM5 metrics endpoint**: expose RPC histograms, cache hit rates, active connections so
  performance trends are visible over time without parsing logs
