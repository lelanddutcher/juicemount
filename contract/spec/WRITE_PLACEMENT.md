# Write placement — who may write a derivative blob, where, and when it counts as real

**Status: PROVIDER-PUBLISHED, 2026-08-04. Closes `WRITE-PLACEMENT` in [`LOOP.md`](../LOOP.md).**
Answers Ask 2.1 and 2.2 of `EDGE_CONTRIBUTE_AND_PORTABLE_DERIVATIVES.md`.

Every filename below was **VERIFIED** by reading what the farm writes today
(`internal/farm/farm.go`, `proxy.go`, `internal/derivatives/aiblob.go`) — this documents existing
behaviour so a consumer-written blob lands beside a farm-written one, not next to it.

Depends on [`DERIVATIVE_KEY.md`](DERIVATIVE_KEY.md) for `<key>`.

---

## 1 · The directory

```
<nas_root>/.juicemount/derivatives/<key>/
```

`<key>` is the `jm1:<hash>:<size>` derivative key, `_`-substituted on filesystems that reject `:`.

**Migration note, stated plainly:** the provider's live tree is still **inode**-keyed
(`/.juicemount/derivatives/<inode>/`). The content-keyed layout exists on a provider branch
(`feat/derivative-content-key`: `asset_key TEXT PRIMARY KEY`, inode demoted to a column) and is
**not merged**. So today, **a consumer must not guess the directory** — resolve it from the
`/derivatives` manifest for the asset rather than constructing it. When the content-keyed store
lands, the provider will publish the cutover here and both spellings will resolve for a window.
This is the one part of this spec that is not yet true in production, and it is why §5 makes the
manifest the discovery mechanism rather than the path.

## 2 · Reserved filenames

One artifact per kind per key. These names are **reserved**: a consumer writing a `proxy` writes
exactly `proxy.mp4`, so the next reader — farm, web UI, another client — finds it without
negotiation.

| kind | file | sidecar | notes |
|---|---|---|---|
| `poster` | `poster.jpg` | — | single frame |
| `filmstrip` | `strip.jpg` | `strip.json` | geometry is **JM-16, still open** — see §6 |
| `waveform` | `waveform.json` | — | JM-18 shape |
| `proxy` | `proxy.mp4` | `proxy.json` | `PROXY_CODEC_SPEC.md`; H.264/AAC faststart floor |
| `ai` | `ai.logger.json` | — | legacy `ai.loupe.json` still READ; see the 08-03 cutover |

Rules:
- **Never invent a sixth name.** A kind not in this table is not writable by a consumer yet; propose
  it in `CONSUMER_STATUS.md` first.
- **Never write a second file for the same kind** (`proxy_720.mp4`, `poster@2x.jpg`). Rung/variant
  support needs a manifest shape that does not exist; it is not a naming decision to make locally.
- **No subdirectories** under `<key>/`.
- Anything else in `<key>/` is provider-owned scratch. Do not read it, do not clean it up.

## 3 · Atomic write — the discipline is temp-in-same-dir, fsync, rename, fsync-parent

This is exactly what the farm does (`internal/farm/atomicwrite.go`, VERIFIED):

1. `CreateTemp` in the **same directory** as the target, named `.<target>.tmp-*` — same dir so the
   rename is intra-filesystem and therefore atomic.
2. `Chmod` to the final mode, then **`Sync` the file**.
3. `Rename` temp → final name.
4. **`fsync` the parent directory** so the rename itself is durable, not just the bytes.
5. On any error, remove the temp.

A reader must never see a partial artifact. The rename is what publishes it.

## 4 · The manifest row is the commit point

**The bytes landing is not the commit.** Order is: write the blob per §3, THEN
`POST /derivatives/register`.

- A blob present in `<key>/` with **no manifest row is by definition incomplete** and MUST be
  ignored — not read, not repaired, not deleted.
- This makes a half-written artifact *unreachable* rather than merely unlikely, which is stronger
  than atomic-rename alone: it also covers a process that dies between rename and register, and a
  blob whose bytes are fine but whose provenance was never recorded.
- The consumer accepted this verbatim (CONSUMER_STATUS 2026-08-03 C) and it is now binding on both
  sides. The farm follows the same order.

Corollary: **do not delete a blob because it has no row.** An unregistered blob may be another
writer mid-flight. Garbage collection of orphans is provider-owned and not scheduled.

## 5 · Discovery: read the manifest, don't construct the path

Resolve an artifact by asking `/derivatives?inode=N` and using the row's `blob_rel_path`, rather than
building `<key>/proxy.mp4` yourself. Reasons, in order of how much they will bite:

1. The provider tree is mid-migration from inode-keyed to content-keyed (§1). A constructed path is
   wrong today.
2. `blob_rel_path` is the field both sides already agree on and is what `register` records.
3. It keeps the key format an implementation detail of the two ends that compute it.

Construct a path only when there is no server at all — the Tier 2 portable case — which is exactly
where the key format earns its keep.

## 6 · What this spec does NOT yet settle

- **JM-16 filmstrip geometry.** `strip.json` is reserved above, but its CONTENTS are still
  unspecified. A consumer must not write `strip.jpg`/`strip.json` until JM-16 lands: an edge-written
  strip whose tile geometry nobody can express is unusable, which is the same blocker from the other
  direction. **`filmstrip` is reserved-but-not-writable.**
- **Quota / eviction.** Consumer-written blobs consume volume space the consumer does not manage.
  Retention/LRU is the deferred phase 2 of the content-key work. Until it lands, the free-space
  precondition in §7 is the only backstop.
- **Trust.** Ruled in `PROVIDER_STATUS` 2026-08-03 C: honest provenance (`producer:"on-device"`,
  never laundered) plus lazy farm re-verification and promotion.

## 7 · Free space is a precondition, not a courtesy

A contribution that competes with the user's own copy is worse than no contribution. Before writing,
check free space and **skip** if the write would push the volume near its floor. Skipping is always
correct — the artifact is rebuildable by definition.

Provider-side status: the spool's admission budget was clamped to LIVE free disk on 2026-08-04
(previously a startup snapshot). The founder's low-disk copy-stall incident is the reason this
sentence exists.
