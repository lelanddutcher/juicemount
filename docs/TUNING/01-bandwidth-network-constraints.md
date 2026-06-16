# Bandwidth & network constraints

How the system behaves as latency rises and bandwidth falls. Key principle:
**the wire is between the SPOOL/cache and the BACKEND, never between Finder and
the spool.** So writes and cached reads are latency-immune; cold reads and
metadata reconcile are latency/bandwidth-bound.

## Measured (this session)

| Op | 10GbE 0.37ms | Wi-Fi 8–50ms | Cellular 40–64ms | Cellular 500ms |
|---|---|---|---|---|
| Write (local-feel) | 445 MB/s | 1101 MB/s | 206 MB/s | 206 MB/s |
| Drain to backend | 400+ MB/s | 66–131 MB/s | ~0.8 MB/s (up) | — |
| Cold read from MinIO | — | 108 MB/s | 4.1 MB/s | very slow |
| Cached read / nav | instant | instant | instant | instant |
| Cold `ls -la` 692 files | — | **65 s** then instant | — | minutes (extrapolated) |
| Full reconcile (261k) | ~3 s | 10 s | tens of s | 10s+ → 300 s cap |
| Reachability (pre-fix) | stable | stable | stable | **flapped 80×/5min** |

## Findings

- **Writes are latency-immune** (spool-absorbed). Confirmed 0.37 ms → 500 ms with no
  throughput change. THE core property.
- **Drain is bandwidth-bound.** Cellular *upload* measured ~0.8 MB/s — a 500 GB copy
  would take days to fully drain over cellular (but feels instant locally and the
  graceful stall paces it). Implication: surface "N GB still uploading" clearly.
- **Cold reads are bandwidth+loss-bound + retry-heavy.** 4.1 MB/s over cellular with
  hundreds of `read_retries` to grind through packet loss — byte-perfect, but slow.
  Per-RPC READ latency over 56 ms cellular: **mean ~1 s, p99 9.4 s, max 9.4 s.**
- **The hidden read amplifier: xattr/AppleDouble probing.** `ls -la`/Finder issue a
  per-file extended-attribute probe; NFSv3 has no xattr RPC, so macOS emulates it with a
  cold first-block READ per file. A warm-mirror `ls -la` of 692 files still fired **240
  cold READs in 18 s and timed out** — the reads, not the metadata, are the wall. These
  reads serialize on the single macOS NFS TCP connection, so they also starve the
  reachability heartbeat (→ false offline → "connection interrupted", H1/#39). Full
  analysis: [03-finder-hangs.md](03-finder-hangs.md) §H2.
- **Reachability probe was LAN-tuned** (1 s dial, 2 fails). Fixed: adaptive timeout
  (`health/reachability.go`, commit 96b25bc) — clamps to 1 s on LAN, grows to ~1.8 s at
  500 ms so jitter stops flapping. **Validated: 0 flaps post-fix.**

## ⭐ Read amplification — the WAN data-cost surprise (measured 2026-06-15)

**A single 4 KB read of a cold file pulled the WHOLE 53 MB file from MinIO** (`dd
bs=4096 count=1` → 8 s, cache grew +52,984 KB). Touching one byte fetches the entire
file. The cascade (all three confirmed in code/config):

1. **NFS client `readahead=16`** (× `rsize=1 MB`) turns one small read into up to **16 MB**
   of speculative sequential readahead RPCs.
2. Those land server-side as sequential reads → after `sequentialThreshold=3` our
   **`ReadaheadManager` fires** and prefetches `readaheadBlocks=8 × 4 MB = 32 MB` ahead
   (`nfs/readahead.go`).
3. JuiceFS's own **`--prefetch 3`** (3 × 4 MB concurrent block prefetch) + the **4 MB
   BlockSize** (the *minimum* fetch unit is 4 MB even for a 4 KB read) top it off → the
   whole file lands in cache.

**Three independent prefetchers stack** — NFS client `readahead=16`, juicefs `--prefetch 3`,
and our `ReadaheadManager` — none aware of the others or of link cost. That's the knob set
to tune together for #16.

**Why it matters on a metered/cellular link:**
- **Browsing** a folder: Finder/`ls -la` xattr-probe every file (§[H2](03-finder-hangs.md)),
  each probe = a cold first-block read = ≥4 MB (one block) fetched. A 692-file dir ≈
  **2.7 GB just to look at it**, and that's if readahead *doesn't* escalate to whole files.
- **Previewing/opening** one file (Quick Look, double-click): the sequential read trips the
  full cascade → the entire file (here 53 MB) is pulled in one burst → **8–12 s beach-ball**
  and saturates the single TCP connection. This IS the user's "preview a 60 MB file →
  connection interrupted" symptom ([H1](03-finder-hangs.md)).

**The tension:** `readahead=16` was deliberately set to fix concurrent-read truncation
(#18/#19) and aggressive prefetch makes LAN copy-out/playback fast. It is exactly wrong on
a metered WAN. → **Adaptive readahead by measured link (#16):** aggressive on fast/cheap
LAN, minimal on cellular/metered. Levers: lower client `readahead` on slow links, raise the
server `sequentialThreshold` / shrink `readaheadBlocks`, and don't let an xattr/Quick-Look
probe escalate to a whole-file prefetch. **No code changed yet — needs a rebuild+deploy
(mount churn), so hold for a deliberate session.**

## Open tuning questions

- Can prefetch/readahead be tuned per measured bandwidth? (#16 adaptive timeouts)
- Should drain workers / concurrency scale down on a metered/cellular link to avoid
  saturating it (which trips reachability — see [Finder hangs](03-finder-hangs.md))?
- Block `--cache-size` vs free-space behavior on a laptop SSD over WAN.
