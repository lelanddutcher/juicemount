# JuiceMount tuning lab

Living notes from the network/cache/metadata-mirror tuning loop. Goal: understand
and tune (1) the local JuiceFS block cache, (2) the local file-tree mirror (the
app's SQLite metadata Store), and (3) how we fetch both — especially **cold start
from an empty cache with no local metadata** and behavior under **low bandwidth**.

Branch: `claude/cache-tuning` (forked from `main` @ 63bae1c, the stable baseline
with the graceful-stall + adaptive-reachability work). Keep experiments here so
`main` stays a clean fallback.

## The pieces (what we're tuning)

| Layer | What it is | Where | Tunables |
|---|---|---|---|
| **Block cache** | JuiceFS on-disk chunk cache (read data) | `--cache-dir`, `--cache-size 92160` (90 GB) | cache-size, prefetch, buffer-size, free-space-ratio |
| **Tree mirror** | App's SQLite mirror of Redis metadata (fast NFS stat/readdir) | `~/Library/Application Support/JuiceMount/metadata.db` | reconcile cadence (adaptive), FTS rebuild threshold, prefetch fan-out |
| **Fetch path** | Cold read → JuiceFS → MinIO; cold stat → reconcile/FUSE → Redis | NFS handler + drainer | readahead=16, subreadDeadline, retry budget, prefetch |

## Network profiles measured this session

| Profile | Iface | RTT | Jitter | Notes |
|---|---|---|---|---|
| 10GbE LAN | en21 | 0.37 ms | ~0 | baseline |
| Wi-Fi (near) | en0 | 8 ms | <1 ms | |
| Wi-Fi (far) | en0 | 50 ms | jittery | |
| Cellular+VPN (good) | utun7 | 40–64 ms | low | |
| Cellular+VPN (bad) | utun7 | 496 ms | 94–898 ms | the stress case |

## Topic logs

1. [Bandwidth & network constraints](01-bandwidth-network-constraints.md)
2. [Environmental constraints](02-environmental-constraints.md)
3. [Finder hangs](03-finder-hangs.md)
4. [Usability issues](04-usability-issues.md)
5. [Time to return full file trees](05-file-tree-return-times.md)
6. [Cold start (empty cache + empty mirror)](cold-start.md)

## Method

- Measure with `perl -e 'alarm N; exec @ARGV'` (macOS has no `timeout`).
- `/metrics` (`:11050`) for bytes/rpc/read counters; `/spool` for offline/cap/pending.
- App log: `~/Library/Logs/JuiceMount/juicemount.log` (JSON).
- The app log's internal NFS-lib logger (keep-awake etc.) goes to stderr, not the JSON log.
- Distinguish **cached/warm** (mirror or block cache hit → local-fast) from **cold**
  (falls through to Redis/MinIO over the wire → latency/bandwidth-bound).

## Headline findings so far

- **Writes feel local at every latency** — 1101 MB/s (Wi-Fi), 206 MB/s (500 ms cellular).
  The spool absorbs them; the wire never gates ingest. This is the core win, validated.
- **Cached metadata nav is instant** at any latency (mirror-served).
- **Cold metadata nav is the weak point:** first `ls -la` of a 692-entry dir was 65 s at
  50 ms Wi-Fi (then instant). Per-file cold GETATTR over the wire is the cost.
- **Cold reads** are byte-perfect but slow + retry-heavy over WAN (4.1 MB/s, 100s of retries
  on cellular). Remote read UX = pin/prefetch first.
- **Reconcile** scales with RTT: ~3 s (LAN) → 10 s (50 ms) → much longer on cellular; adaptive
  cadence stretches to a 300 s cap so it doesn't contend.
