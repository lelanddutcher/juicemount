# JuiceFarm — server-side derivative & AI generation (advanced / optional)

> **You do not need this to use JuiceMount.** Plain JuiceMount — the macOS menu-bar
> app that mounts your JuiceFS volume as a local NFS share — is fully functional on
> its own. JuiceFarm is an **optional** server-side companion for power users and for
> the [OpenLoupe](https://github.com/lelanddutcher) DAM integration. If you just want
> to mount your media and work, skip this document.

---

## What it is

JuiceFarm is a headless worker that runs **next to your storage** (on the NAS, or any
host that can reach the JuiceFS metadata + object store) and **pre-generates
derivatives** of your media so that client apps never have to compute them on-device:

| Derivative | What it is | Generator |
|---|---|---|
| **tech** | container/codec/resolution/bitrate/duration/EXIF (the "deep" metadata) | `ffprobe` |
| **poster** | a representative still frame | `ffmpeg` |
| **filmstrip** | a contact-sheet of frames for scrubbing | `ffmpeg` |
| **waveform** | an audio waveform image (per-channel) | `ffmpeg` |
| **proxy** | a small, seek-friendly H.264/MP4 playback proxy of a heavy original | `ffmpeg` (`libx264`) |
| **transcript** | a spoken-word transcript (`ai.loupe.json`) | `whisper.cpp` |

The point: a 4K ProRes camera original is expensive to scrub, preview, or transcode.
The farm transcodes a lightweight proxy **once, server-side**, and **every** client that
opens that file reads the ready-made result instead of burning local CPU. The same is
true for thumbnails, waveforms, and AI transcripts.

A client that has no farm (or whose farm hasn't produced a given derivative yet) simply
falls back to generating locally — the farm is **always an optimization, never a gate**.

---

## Why it exists

JuiceMount's vision is "open-source LucidLink for creators." Creators work with huge
camera originals over a shared volume. Two problems the farm solves:

1. **Don't make every laptop transcode.** A studio of 10 editors shouldn't each spend a
   core generating the same proxy. Generate it once at the source; everyone reads it.
2. **The metadata-authority story.** Derivatives (and human assertions like ratings,
   picks, person names) live as **portable sidecars on the volume**, content-hash-keyed
   to the source. Any client — JuiceMount, OpenLoupe, a future web UI — discovers and
   trusts them via a versioned contract, never a proprietary silo.

---

## Architecture

```
        ┌──────────────────────── NAS / storage host ───────────────────────┐
        │                                                                     │
        │   Redis (JuiceFS metadata)        MinIO/S3 (JuiceFS data)           │
        │        ▲                                ▲                           │
        │        │ juicefs mount (its OWN client) │                           │
        │   ┌────┴────────────────────────────────┴────┐                      │
        │   │  juicefarm-worker  (container, image      │                      │
        │   │  juicefarm:local)                         │                      │
        │   │   • BRPOP juicefarm:queue  (job intake)   │                      │
        │   │   • ffprobe / ffmpeg / whisper.cpp        │                      │
        │   │   • writes blobs + a JM-15 manifest       │                      │
        │   │     sidecar to the VOLUME, + a server     │                      │
        │   │     derivatives.db (Tier-B index)         │                      │
        │   └───────────────────────────────────────────┘                     │
        └─────────────────────────────────────────────────────────────────────┘
                                   │  derivatives land on the shared volume at
                                   ▼  /<vol>/.juicemount/derivatives/<inode>/
        ┌──────────────── Mac (JuiceMount) ────────────────┐
        │  GET /derivatives?inode=N → on-miss reconcile     │
        │    reads the volume sidecar → local Tier-B index  │
        │  GET /blob?inode=N&kind=proxy → byte-range serve  │
        └───────────────────────────────────────────────────┘
```

Key properties:

- **Own FUSE mount, additive + safe.** The farm runs its *own* `juicefs mount`
  (JuiceFS supports many concurrent clients). It never disturbs the Mac client or the
  server's primary juicefs container. Always give it a **dedicated** cache dir.
- **Blobs live on the volume, indexed in a DB.** A generated proxy is written to
  `<mount>/.juicemount/derivatives/<inode>/proxy.mp4`, alongside a `manifest.json`
  sidecar (the JM-15 discovery record) and a row in the server-side `derivatives.db`.
- **Content-hash provenance.** Every derivative carries a sampled `xxh3` source hash.
  It is **byte-stable across macOS and Linux**, so a farm-generated blob passes a
  consumer's hash-freshness gate directly — a client trusts a derivative only when
  `hash == live source_hash`.
- **Two passes, decoupled.** Cheap derivatives (tech/poster/filmstrip/waveform) commit
  atomically and fast; the slow `-preset slow` proxy transcode and the whisper
  transcript run as **separate** passes so a long encode never withholds the fast
  results.

---

## How a job flows (queue mode)

The worker runs `jmfarm -queue` and `BRPOP`s the Redis key `juicefarm:queue` (on the
same metadata Redis, db 1). A job is a small JSON document:

```json
{
  "id": "…",
  "path": "/jfs/<relative path to the source>",
  "kinds": ["proxy"],
  "producer": "manager",
  "enqueued_at": "<RFC-3339>",
  "crf": 21, "preset": "slow", "vcodec": "libx264",
  "model": "", "workers": 0, "proxy_workers": 0
}
```

Enqueue from the manager UI, from OpenLoupe, or by hand
(`redis-cli -n 1 LPUSH juicefarm:queue '<json>'`). The worker:

1. stats the source on its `/jfs` mount → inode + sampled hash,
2. runs the requested generators,
3. writes the blob(s) + the `manifest.json` sidecar + the `derivatives.db` row,
4. (a permanently-failed job publishes a `failed` row so the consumer regenerates
   locally rather than waiting forever).

---

## Discovery (JM-15) — how the Mac learns about server-generated blobs

The farm writes to the volume; the Mac's Tier-B index learns about it **lazily**:

- **On-miss reconcile.** When a client asks `GET /derivatives?inode=N` for an asset the
  local index hasn't seen, the control plane reads
  `<mount>/.juicemount/derivatives/N/manifest.json` and ingests it on the spot — no
  restart, no full sweep.
- **Full sweep.** `jmfarm -reconcile -mount /Volumes/<vol> -db <local derivatives.db>`
  walks every sidecar and ingests them (idempotent upsert) — use after a bulk
  generation run.
- *Known edge:* the on-miss path is **discovery-only**, so *re-generating* an already-
  known asset's derivative won't refresh its row until a full reconcile.

Once ingested, `GET /derivatives?inode=N` returns the manifest and
`GET /blob?inode=N&kind=proxy` serves the blob with HTTP `Range`/`206` byte-range
support, so a remote client streams a proxy without downloading it whole.

---

## Deployment (TrueNAS / Docker)

Build natively on the storage host (no cross-compile):

```bash
# from a checkout on the host:
docker build -f server/juicefarm/Dockerfile -t juicefarm:local .
```

Run the standing **queue worker** as a side container on the JuiceFS compose network —
**never** by re-applying the whole stack YAML (that restarts redis/minio, which the
Mac client depends on):

```bash
docker run -d --name juicefarm-worker \
  --network <juicefs_compose_network> \
  --cap-add SYS_ADMIN --device /dev/fuse --security-opt apparmor:unconfined \
  --restart unless-stopped \
  -e JM_META=redis://redis:6379/1 \
  -e JM_FARM_QUEUE=1 \
  -e JM_FARM_PRODUCER=linux-farm \
  -e JM_FARM_MODEL=/models/ggml-medium.en.bin \
  -v /path/to/juicefarm-cache:/jfs-cache \
  -v /path/to/juicefarm-state:/state \
  juicefarm:local
```

Build gotchas (baked into the Dockerfile):

- The JuiceFS `ce-v1.3.x` runtime is Debian **bullseye / glibc 2.31**; statically link
  `jmfarm` and build `whisper.cpp` on bullseye so the symbols match.
- Use a **dedicated** farm cache dir, never the primary juicefs container's cache.

Modes: `JM_FARM_QUEUE=1` (standing queue drain) or `JM_FARM_MODE=all|transcript|…`
with `JM_FARM_ONCE=1` (one-shot sweep). Throttling knobs: `JM_FARM_PROXY_WORKERS`
(proxy `-preset slow` pins a core/clip, so it gets fewer workers than the cheap
passes), `JM_FARM_CRF`/`JM_FARM_PRESET`, `JM_FARM_NICE`/`JM_FARM_IONICE`.

---

## The proxy contract (interoperability)

So a farm-made proxy and a client's local fallback are **byte-interchangeable**, the
proxy profile is locked: MP4 (ISO BMFF), single file, `+faststart` (moov first);
H.264/AVC High, 8-bit `yuv420p` 4:2:0, BT.709 SDR, CFR, closed 2 s GOP; AAC-LC stereo
128 kbps / 48 kHz; type string `video/mp4; codecs="avc1.640028, mp4a.40.2"`. The
`proxy` derivative row carries `codec` / `codec_string` / `blob_size` so a consumer can
gate playback (and skip the proxy if it isn't actually smaller) without re-probing.
CRF/preset are a quality knob only; HEVC/AV1 are additive rungs where the farm has
hardware encode — H.264 remains the guaranteed-decodable floor.

---

## Manager integration

The `juicemount-manager` web UI exposes a **Farm tab** (read-only Phase 1): coverage by
derivative kind, last-sweep summary, provenance (tech→ffprobe, proxy→libx264,
ai→whisper.cpp), and the resolved throttling governor. The manager is CGO-free, so the
farm pre-aggregates status to `/state/farm-status.json` and the manager relays it via
`GET /api/farm`. Job control, scheduling, and per-directory opt-in are on the roadmap.

---

## Status & roadmap

- **Live + proven:** tech / poster / filmstrip / waveform / **proxy** / whisper
  **transcript** generation; the queue worker; on-miss + full JM-15 discovery; the
  proxy-codec + `/blob` byte-range contract; portable assertion sidecars; the Manager
  Farm tab (read-only).
- **Earmarked:** atomic blob writes deploy; manager-driven job control + scheduling +
  per-directory opt-in (don't proxy an NLE's own proxies); GPU/ML workers for richer AI
  (faces / OCR / framing) writing `ai.loupe.json`; locality/residency hints.

For the wire contract OpenLoupe and JuiceMount share, see the private
`juicemount-contract` repository (spec + golden fixtures + `PROVIDER_STATUS` /
`CONSUMER_STATUS`).
