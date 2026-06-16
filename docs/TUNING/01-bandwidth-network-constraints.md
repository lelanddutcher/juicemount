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
- **Reachability probe was LAN-tuned** (1 s dial, 2 fails). Fixed: adaptive timeout
  (`health/reachability.go`, commit 96b25bc) — clamps to 1 s on LAN, grows to ~1.8 s at
  500 ms so jitter stops flapping. **Validated: 0 flaps post-fix.**

## Open tuning questions

- Can prefetch/readahead be tuned per measured bandwidth? (#16 adaptive timeouts)
- Should drain workers / concurrency scale down on a metered/cellular link to avoid
  saturating it (which trips reachability — see [Finder hangs](03-finder-hangs.md))?
- Block `--cache-size` vs free-space behavior on a laptop SSD over WAN.
