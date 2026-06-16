# Time to return full file trees

How long until a directory / the whole tree is listable, broken down by warm vs cold
and by latency. This is the metric users feel most when they open a folder.

## Two regimes

- **Mirror hit (warm):** the path's children are already in the SQLite mirror →
  `readdir`/`getattr` served locally → **instant** regardless of latency.
- **Mirror miss (cold):** falls through to FUSE → Redis over the wire, one round-trip per
  lookup/stat. Cost ≈ (entries) × (RTT + per-op overhead).

## ⚠️ The dominant cost is NOT metadata — it's the xattr/AppleDouble READ storm

Measured 2026-06-15 (56 ms cellular, **warm** mirror): with the mirror warm, both
regimes above are instant. `readdir` (names) and a 692-file `stat` loop both complete in
≤1 s with **zero READ RPCs**. But `ls -la` / Finder add a per-file **extended-attribute
probe**, and over NFSv3 (no xattr RPC) macOS emulates that with `LOOKUP ._<name>` + a
small cold **READ of the first block** — ~1 s each cold (READ p99 9.4 s), serialized on
the single NFS TCP connection. A 692-file `ls -la` thus issued **240 cold READs in 18 s
and timed out**, while the metadata-only stat-storm of the *same dir* finished in ~1 s.
**So "time to return a full tree" over WAN is gated by the xattr read storm, not our
metadata.** Full root-cause + levers: [03-finder-hangs.md](03-finder-hangs.md) §H2.

## Measured

| Scenario | Latency | Time |
|---|---|---|
| Top-level `ls` (warm) | any | 0.01 s |
| `ls -la` 692-file dir, **cold** | 50 ms Wi-Fi | **65 s** (~94 ms/file) |
| `ls -la` 692-file dir, warm | any | 0.02–0.18 s |
| `find` traversal (warm) | 50 ms | 0.02 s |
| `find` deep dir (cold) | 64 ms cellular | timed out / very slow |

## Cost model

Two distinct costs, often conflated:

1. **Cold metadata** (mirror empty): ~2× RTT per entry (lookup + getattr / a
   READDIRPLUS round-trip). 94 ms/file measured at 50 ms. **Paid once**, then the mirror
   serves it forever → instant. Largely solved by the mirror + cold-start work.
2. **Per-file xattr/AppleDouble READ** (paid on EVERY `ls -la`/Finder browse, even with a
   warm mirror): ~1 cold first-block READ per file, p99 9.4 s cold over cellular,
   serialized on one TCP connection. **This is the steady-state WAN-browse bottleneck**
   and the one to fix next — see [03-finder-hangs.md](03-finder-hangs.md) §H2.

## Levers to test (the tuning loop)

1. **Kill the xattr READ storm (NEW #1):** serve absent `._`/empty-xattr from the mirror
   without a backend READ, and/or warm each child's first 4 KB block on readdir, and/or
   read-admission isolation so the storm can't starve metadata/heartbeat. This is now the
   top WAN-nav lever (§H2).
2. **READDIRPLUS / batch getattr:** fetch all child attrs in one round-trip instead of
   N. JuiceFS readdir already returns attrs — the mirror-populate uses them in ONE pass
   (`prefetchChildren` → BulkInsert) and the NFS readdir path doesn't re-stat per entry.
   (Confirmed not the bottleneck once warm, but matters for the cold first-touch.)
3. **Warm-on-navigate:** when a dir is opened cold, kick a background mirror-populate so
   the SECOND access (and Finder's re-stat storm) is instant. Already partially done
   (prefetch fan-out is gated off on slow links — revisit: gate the *recursive* fan-out
   but keep the *one-level* warm).
4. **Persist the mirror across restarts** so cold-browse only happens on a truly fresh
   client (see [cold-start.md](cold-start.md)).
5. **Negative-entry caching** for `._*` AppleDouble probe storms (Finder generates ~18k
   ENOENT lookups) — already handled for the LOOKUP; the gap is the follow-on READ (§H2).
