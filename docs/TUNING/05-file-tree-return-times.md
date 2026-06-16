# Time to return full file trees

How long until a directory / the whole tree is listable, broken down by warm vs cold
and by latency. This is the metric users feel most when they open a folder.

## Two regimes

- **Mirror hit (warm):** the path's children are already in the SQLite mirror →
  `readdir`/`getattr` served locally → **instant** regardless of latency.
- **Mirror miss (cold):** falls through to FUSE → Redis over the wire, one round-trip per
  lookup/stat. Cost ≈ (entries) × (RTT + per-op overhead).

## Measured

| Scenario | Latency | Time |
|---|---|---|
| Top-level `ls` (warm) | any | 0.01 s |
| `ls -la` 692-file dir, **cold** | 50 ms Wi-Fi | **65 s** (~94 ms/file) |
| `ls -la` 692-file dir, warm | any | 0.02–0.18 s |
| `find` traversal (warm) | 50 ms | 0.02 s |
| `find` deep dir (cold) | 64 ms cellular | timed out / very slow |

## Cost model

cold per-file ≈ ~2× RTT (lookup + getattr, or a READDIRPLUS round-trip). At 50 ms that's
~94 ms/file measured; at 500 ms it'd be ~1 s/file → a 692-file dir ≈ 11 min cold. **This
is the #1 thing to fix for WAN navigation.**

## Levers to test (the tuning loop)

1. **READDIRPLUS / batch getattr:** fetch all child attrs in one round-trip instead of
   N. JuiceFS readdir already returns attrs — make sure the mirror-populate uses them in
   ONE pass (it does via `prefetchChildren` → BulkInsert) and that the NFS readdir path
   doesn't re-stat per entry.
2. **Warm-on-navigate:** when a dir is opened cold, kick a background mirror-populate so
   the SECOND access (and Finder's re-stat storm) is instant. Already partially done
   (prefetch fan-out is gated off on slow links — revisit: gate the *recursive* fan-out
   but keep the *one-level* warm).
3. **Persist the mirror across restarts** so cold-browse only happens on a truly fresh
   client (see [cold-start.md](cold-start.md)).
4. **Negative-entry caching** for `._*` AppleDouble probe storms (Finder generates ~18k
   ENOENT lookups) — already handled; verify cost under WAN.
