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

## Results

> (filled by the measurement run — see the run log)

## Tuning ideas to test

- **Ship a seed mirror** with the app, or snapshot/restore the mirror, so a fresh client
  doesn't pay the full SCAN. (The mirror is just a cache of Redis — could be exported.)
- **Stream the reconcile** so the tree becomes progressively browsable (top levels first)
  rather than all-or-nothing, and show progress (#37).
- **Parallelize / larger Redis SCAN batches** to cut the round-trip count (RTT-bound).
- **Lazy mirror:** don't full-SCAN on first launch — populate on-navigate (warm the dirs
  the user actually opens) + a background full sync. Trades first-browse speed for not
  blocking on a 103 s scan.
