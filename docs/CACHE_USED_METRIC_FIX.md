# "Cache used" is wrong — it counts pinned files only, not the real JuiceFS cache

> **STATUS: EARMARKED — bug, design only, not built. (2026-06-25)**
> Reported by Leland: the "cache used" figure is wrong **globally in the app AND OpenLoupe** — it shows ~0 KB
> cached unless the file is part of the **pinned** set, hiding all normally-cached (read-but-not-pinned) data.

## Root cause (grounded)
The "X cached" number is `cacheStatus.aggregate.CachedBytes`
(`app/.../MenuPopoverView.swift:1269,1491`; OpenLoupe relays the same `/cache-status` aggregate). That field is
computed in the **pin store** as:

```
internal/cache/pin/store.go:530  AggregateStats()
   SELECT … SUM(bytes_cached) … FROM pinned_files     (:521-543)
```

So `CachedBytes = the sum of bytes_cached across the pinned_files table` — **pinned files only.** JuiceFS's normal
**block cache** (the LRU of every block you've read but not pinned, living in the `--cache-size`-capped cache dir)
is never counted. Pin nothing → "cache used" reads 0, even with gigabytes of warm blocks on disk.

**The metric conflates two different things:**
1. **Pinned-resident bytes** — how much of the *pinned* set is downloaded (pin store `SUM(bytes_cached)`). A
   legitimate number for the pin/offline-readiness view.
2. **Total cache used** — actual disk the JuiceFS block cache occupies. This is what "X cached" *should* show, and
   it has **nothing to do with pinning.**

The UI/OL show (1) labeled as if it were (2).

## The fix
Source "total cache used" from the **JuiceFS block cache**, not the pin store:
- **Preferred:** scrape `juicefs_blockcache_bytes` (a gauge) from the juicefs Prometheus metrics endpoint — the
  core already runs juicefs with `--metrics 127.0.0.1:9567` (the manager pins this addr, `internal/manager/sync.go:24`).
  That gauge is exactly "bytes currently in the on-disk block cache."
- **Fallback:** `du` the JuiceFS cache dir (`cfg.CacheDir`, the `--cache-size` location) — heavier, but works if
  the metric is unavailable.
- Expose it as a **new, distinct** field in `/cache-status` (e.g. `cache_used_bytes` / `blockcache_bytes`) next to
  the existing pinned aggregate — don't overwrite the pinned number (the pin-capacity guard legitimately uses
  `PinnedBytes = agg.TotalBytes`, `internal/cache/pin/capacity.go:124` — leave it alone).
- Point the app's "X cached" (`MenuPopoverView:1491`) + the cache progress bar (`:1269,1274`) at the new field.
  Keep "pinned resident" as its own clearly-labeled line.

## OpenLoupe (OL) angle
OL relays the `/cache-status` aggregate, so it shows the same wrong number. **Fix is app/core-side** — add the
real `cache_used_bytes` to `/cache-status`; OL then reads the new field. Since this adds a field to the shared
`/cache-status` shape, **note it in the contract repo (`CONSUMER_STATUS.md`)** so OL switches its "cache used"
display to the new field (and stops treating the pinned aggregate as total).

## Don't-break
- Pin-capacity / over-capacity logic (`capacity.go`) keys off pinned bytes — correct as-is; the new metric is
  purely additive.
- The cache hit-rate/read-write counters were previously dropped from the manager Overview for being error-prone
  (that was the rate counters); `juicefs_blockcache_bytes` is a simple gauge, not those counters.
- A `du` fallback on a huge cache dir is not a hot path — compute on the `/cache-status` poll cadence, cache it.

## Cross-links
- `internal/cache/pin/store.go` (the pinned-only aggregate), `internal/cache/pin/capacity.go` (legit pinned use),
  `internal/manager/sync.go:24` (the juicefs `:9567` metrics addr), `CONSUMER_STATUS.md` (the OL `/cache-status`
  contract). See also [project_cache_architecture] in memory.
