# Durable identity (the crux)

This is the single most consequential design decision in the integration, and the contract's job is to make
it possible. **Decide it at Phase 0, not Phase 4.**

## The problem

OpenLoupe keys an asset — and *every* media cache — on the **local path string**:

- `assets.id` = `AssetID(url.path)` (`AssetScanner.swift:82`, `MediaAsset.swift:28`), the SQLite
  `assets.id TEXT PRIMARY KEY` (`Migrations.swift:17`).
- Thumbnail/filmstrip/proxy cache key = **FNV-1a 64 of `url.standardizedFileURL.path`**
  (`ThumbnailGenerator.swift:63-67,169`).
- The browse↔offload bridge joins on `ingest_manifest.destination_path = url.path`
  (`Database+OffloadStatus.swift:38`).

A JuiceMount mount path is **not stable**: rename the volume, mount at a different point, or reach the same
NAS from a second Mac, and every asset becomes "new" — its DB row, thumbnails, deep metadata, tags, and
transcript all orphan. For a network volume this is unacceptable, and it's the whole multi-editor story
JuiceMount is built for.

## The fix (elegant because we own both sides)

JuiceMount already has a **stable JuiceFS `inode`** per path — `entries.inode INTEGER NOT NULL`, indexed by
`idx_inode`, **preserved across rename** (juicefs guarantees it). The contract surfaces it two ways so
OpenLoupe never opens JuiceMount's SQLite:

- `GET /whoami.instance_id` — a stable per-install UUID = the `jm_instance` half.
- `GET /residency.inode` and `GET /lookup.inode` — the `inode` half (residency already probes the path, so
  it returns the inode for free).

**Durable identity = `(jm_instance, inode)`**, with `nas_rel_path` (= `path − mount_point`, defined by this
contract) as a human-debuggable secondary that survives a metadata rebuild.

## Why Phase 0, not Phase 4

The map of OpenLoupe confirmed that retrofitting identity after path-keyed rows and caches exist is **not an
additive migration** — it touches five sites, including a PRIMARY KEY (which a plain `ALTER` can't change):

1. `AssetScanner.swift:82` — id construction must resolve `(instance, inode)` for JM assets, not `url.path`.
2. `MediaAsset.swift:28` — `AssetID` semantics / an indirection field.
3. `assets` PRIMARY KEY + `idx_assets_url` + the upsert (`Migrations.swift:17,34`, `Database.swift:177-196`,
   upsert keys `ON CONFLICT(id)` at `Database.swift:182`).
4. `ThumbnailGenerator.cachedPath` / `stableHash` (`:63-67,169-178`) — or every cached thumbnail re-keys.
5. The manifest join key `destination_path` (`Database+IngestManifest.swift:24`,
   `Database+OffloadStatus.swift:38`).

So introduce a **stable-id → cache-key indirection at Phase 0** (for JuiceMount assets only). You don't have
to ship cross-machine sharing yet — but the indirection layer must exist *before the first JM cache is
written*, or Phase 4 becomes a painful re-key + orphaned-blob cleanup instead of a feature flip.

Precedent already in the tree: `WipeManifest.volumeID` (`WipeManifest.swift:57-59`) is a stable, non-path
identity built as a first-non-empty fallback chain (`mount-root | UUID`). The same indirection shape applies
to assets.

## Detection probe (how OpenLoupe knows a mount is JuiceMount)

The signature is logically exact as the old doc described (only its line refs were stale). Real source:
`isOurNFSMount` (`bridge/cbridge.go:1421-1430`):

```
mount source HasPrefix "127.0.0.1" || "localhost"   AND   fstype contains "nfs"
```

- In Swift, enumerate volumes (`FileManager.mountedVolumeURLs` / `getmntinfo`), read `f_mntfromname` +
  `f_fstypename`, apply the prefix+fstype test, then **confirm with a bounded `GET /health`** on the
  control-plane port (from `defaults read com.juicemount.app metricsAddr`, default `11050`).
- The NFS **port (11049)** lives in the mount `-o` options, **not** in the source string — match the host
  prefix, not the tail.
- Ignore the internal FUSE mount (`JuiceFS:<vol>`, type `macfuse`, at `~/.juicemount/fuse-internal`) — target
  the user-facing NFS volume.
- The volume that actually trips OpenLoupe's macFUSE "looks local" hazard (`SpacePreflight.swift:20-23`,
  `isLocal = volumeIsLocal || volumeIsInternal`) is the **internal** macFUSE mount — which is exactly why the
  consumer ignores it and targets the NFS volume. The user-facing **NFS** volume reports
  `volumeIsLocal == false`, so a JuiceMount *destination* already lands in preflight's network/FUSE WARN
  branch (not strict BLOCK). (Detecting a JuiceMount *source* doesn't change destination preflight — these
  are separate; the win here is correctly classifying the source as network-resident, not local card media.)
