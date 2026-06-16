# Cold start — empty cache + empty local metadata mirror

The "new device / fresh install" experience: a client with no local tree mirror and
no block cache, connecting to a backend (Redis+MinIO) that already holds the data.
How long until the full tree is browsable, and until reads are local-fast?

## The two cold-start costs

1. **Metadata mirror rebuild** (tree-browse): the SQLite mirror (`metadata.db`,
   ~317 MB / 261k entries here) is empty → the first reconcile SCANs all of Redis and
   BulkInserts the tree. RTT-bound (each Redis SCAN batch is a round-trip). **This is
   what gates "open a folder and see everything."**
2. **Block cache rebuild** (read): `~/.juicefs/cache` (7.9 GB here) is empty → every read
   fetches chunks from MinIO over the wire. Bandwidth-bound. Characterized separately —
   cold reads are slow + retry-heavy over WAN (see 01-bandwidth).

Before the mirror is rebuilt, navigation still WORKS — it falls through to FUSE→Redis
per lookup (one round-trip each). So cold-start isn't "broken," it's "slow until warm."

## Baseline (this 64 ms cellular link, mirror NOT wiped)

- Steady-state full reconcile (diff): **103.5 s** for 261,302 entries.
- That's the RTT-bound floor. A from-empty rebuild = the same SCAN + a full BulkInsert +
  FTS index build (local, faster) on top.
- Estimate before measuring: ~2 min over 64 ms cellular; ~8× (~13 min) over a 500 ms link.

## Method (metadata-mirror cold start)

1. `/metrics` + log snapshot; note entry count.
2. Clean teardown (kill app + juicefs, unmount) via `/tmp/jm_deploy.sh` teardown.
3. **Back up + remove** `metadata.db*` (mirror) — keep `~/.juicefs/cache` so reads stay
   warm and we isolate the METADATA cost.
4. Relaunch; start a stopwatch.
5. Capture: t(app healthy), t(first "metadata sync complete" with full entry count),
   and navigation latency DURING the rebuild (cold FUSE-fallback) vs AFTER (mirror).
6. The rebuilt mirror is correct (from Redis) — no restore needed; backup is safety only.

## Results — measured 2026-06-15, 64 ms cellular, mirror wiped (block cache kept)

| Metric | Value |
|---|---|
| relaunch → app healthy + tree fully fast | **~121 s** |
| full mirror rebuild from empty (261,302 entries) | **110.8 s** (`dur=110831ms`) |
| nav post-rebuild (`ls` 692-file dir) | **instant** (mirror-served) |
| rebuilt mirror size | 288 MB |

Extrapolated: ~8× over a 500 ms link → **~15 min first-launch**. This is the single
biggest WAN-UX gap, and it's entirely the metadata mirror.

### Where the 110 s goes

The reconcile runs a Lua `SCAN cursor MATCH 'd[0-9]*' COUNT 500` + server-side `HGETALL`
per batch (`metadata/redis.go:890,919`). Decomposition over the 64 ms link:
- **~33 s round-trips:** 261k ÷ 500 = ~522 SCAN batches × 64 ms RTT.
- **~70 s payload transfer:** the full ~288 MB of metadata down the cellular pipe (~4 MB/s).

So it's **both RTT- and throughput-bound.** And the full SCAN runs on *every* startup (it's
also how prune/diff works) — so a pre-seeded/snapshot mirror does NOT avoid the cost unless
the app stops *blocking* startup on the full SCAN.

### Key insight

Navigation already works *during* the rebuild via FUSE→Redis fallback (one round-trip per
lookup) — it's slow, not broken. So the win is **not** "rebuild faster" but **"don't block
on the rebuild":** make the tree usable immediately + build the mirror in the background +
show progress (#37). The full eager mirror exists for offline-full-tree browsing (#3), so
the right shape is a **hybrid: lazy/on-navigate warming for instant start + background full
sync for offline coverage.**

## Tuning experiments (ranked by leverage)

1. **Non-blocking startup + background reconcile** (biggest UX win, no WAN-cost change):
   tree usable from FUSE-fallback immediately; mirror builds behind a progress indicator.
2. **Lazy mirror + background full-sync hybrid:** populate navigated dirs first (instant
   first-browse), full SCAN continues in the background for offline coverage.
3. **Bigger `scanCount` (500 → 1–2k):** cuts the ~33 s RTT portion ~2–4× — BUT a longer Lua
   per batch risks the Redis-BUSY monopolization the chunked-SCAN fix (5ee6dcb / #15)
   solved. Bounded win, test carefully.
4. **Compress the reconcile payload:** the ~70 s is structured metadata (highly
   compressible) over a slow pipe; gzip in the Lua + decompress client-side could cut it
   materially. Bigger change.
5. **Skip the SCAN when the mirror is recent + trust pub-sub:** do the full reconcile lazily
   (e.g., first idle window) instead of at startup, relying on the <100 ms SUBSCRIBE channel
   for live changes. Risky for prune-correctness; needs care.

## Tuning ideas to test

- **Ship a seed mirror** with the app, or snapshot/restore the mirror, so a fresh client
  doesn't pay the full SCAN. (The mirror is just a cache of Redis — could be exported.)
- **Stream the reconcile** so the tree becomes progressively browsable (top levels first)
  rather than all-or-nothing, and show progress (#37).
- **Parallelize / larger Redis SCAN batches** to cut the round-trip count (RTT-bound).
- **Lazy mirror:** don't full-SCAN on first launch — populate on-navigate (warm the dirs
  the user actually opens) + a background full sync. Trades first-browse speed for not
  blocking on a 103 s scan.
