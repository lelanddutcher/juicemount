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
| **proxy** | a small, seek-friendly MP4 playback proxy of a heavy original | `ffmpeg` (verified hardware HEVC/H.264, or CPU H.264) |
| **transcript** | a spoken-word transcript (`ai.logger.json`) | `whisper.cpp` |

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
        │   ┌────┴───────────────────────────────┐  ┌──────────────────────┐ │
        │   │ server worker                      │  │ render worker        │ │
        │   │ • tech + waveform metadata lane    │  │ • verified GPU only  │ │
        │   │ • discovery leader + backstop      │  │ • HEVC/H.264 proxy   │ │
        │   │ • visible CPU fallback lane        │  │ • video previews/AI  │ │
        │   └─────────────────────────────────────┘  └──────────────────────┘ │
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
- **Compute-aware derivative passes.** The public `derivatives` kind is planned into
  server metadata/audio children and one-file video preview children. Poster and
  filmstrip decoding uses a verified hardware decoder when available; unsupported
  sources enter an explicit `cpu-decode` lane instead of demoting the rest of a batch.
  Proxy transcode and Whisper transcript remain separate passes.
- **Observed capability, not advertised hardware.** A render worker is admitted only
  after it completes real encode and hardware-decode probes. H.264 Constrained
  Baseline/Main/High and HEVC Main/Main10 are probed independently; plain H.264
  Baseline remains distinct and is not inferred from Constrained Baseline. A worker may
  claim only the source profiles it actually decoded in hardware. Its measured
  encode, decode, transcript, and mount-read rates are published in the Manager.
- **Durable claims.** Claiming a job atomically moves it to a per-worker processing
  list with a renewable lease. If the worker disappears, another worker requeues the
  receipt instead of losing work between a Redis pop and execution.
- **Server-planned render batches.** Fresh derivatives, proxy, and transcript requests first enter
  a metadata-only server lane. The server recursively discovers targets and publishes
  deterministic bounded children (one file for transcripts and video previews, small
  groups for proxy work); only those children enter measured GPU/CPU execution lanes. A render node
  therefore never burns accelerator time walking an entire directory, and one
  incompatible source cannot demote or block a directory-sized batch.

---

## How a job flows (queue mode)

The worker runs `jmfarm -queue` against the same metadata Redis (db 1). Producers
create one job per requested kind. Fresh derivatives/proxy/transcript requests first
become server planning jobs; after recursive discovery, each bounded child is routed
from the live, verified worker profiles to a server-metadata, render, or explicit
CPU-fallback lane. A job is a small JSON document; scheduler
annotations are additive. The planning parent looks like:

```json
{
  "id": "…",
  "path": "/jfs/<relative path to the source>",
  "kinds": ["proxy"],
  "producer": "manager",
  "enqueued_at": "<RFC-3339>",
  "crf": 21, "preset": "slow",
  "plan_only": true,
  "queue_class": "server",
  "required_capabilities": ["metadata"],
  "selected_backend": "server-dispatch",
  "model": "", "workers": 0, "proxy_workers": 0
}
```

Its bounded children carry `parent_id`, `shard_index`, `shard_count`, exact
`retry_targets`, and the measured execution admission such as
`selected_backend: "hevc_vaapi"` or `selected_backend: "h264_vaapi"` for a preview
decoder. Derivative children also carry `derivative_pass: "metadata"|"previews"`;
`source_video_codec`, `source_video_profile`, `source_bit_depth`,
`source_pixel_format`, `source_video_width`, and `source_video_height` preserve the server's live
ffprobe result through retry/recovery. Proxy children are homogeneous bounded
batches: a render child requires its hardware encoder plus both
`decoder:h264_vaapi` and (for example)
`decoder:h264_vaapi:profile:high`. Worker heartbeats publish the largest frame
geometry and pixel formats each decoder actually completed during startup.
Measured AV1 Main/Main10 and HEVC Rext 4:2:2/4:4:4 decoding can therefore feed
the preferred hardware HEVC encoder on capable nodes. Sources exceeding the
measured envelope, truly unverified codecs such as ProRes, and unverified pixel
formats enter the explicit CPU/H.264 lane directly; they do not consume a GPU
lease first. Intentional software decode is visible as `cpu-decode`. Planner
admission is never copied to a child.

Jobs arrive automatically from the recursive discovery watcher and its cursored
backstop. The Manager's advanced path sweep and OpenLoupe remain repair/manual
producers. Do not push raw JSON directly to a class queue: using the Manager API or
`farmqueue.Enqueue` is what applies routing, status, dedupe, and recovery metadata.
The execution worker:

1. stats the source on its `/jfs` mount → inode + sampled hash,
2. runs the requested generators,
3. writes the blob(s) + the `manifest.json` sidecar + the `derivatives.db` row,
4. acknowledges the durable claim only after the terminal status is stored,
5. (a permanently-failed job publishes a `failed` row so the consumer regenerates
   locally rather than waiting forever).

The whole transport is controlled through `GET|PUT /api/farm/control`. Pause lets an
in-flight atomic job finish, then prevents new claims. Automatic discovery has its
own enable switch; dirty events remain pending while the farm is paused. One worker
holds the short discovery-leader lease, and a 15-minute recursive modified-directory
scan repairs events missed while Redis pub/sub or a worker was offline.

Manager also exposes explicit per-node lifecycle controls. **Pause** stops that node
from taking another claim while its current atomic work finishes. **Drain** does the
same but reports `draining` immediately so planned maintenance is distinguishable
from a general pause; **Resume** clears both flags. Heartbeats retain the legacy
`current_job` ID and add `current_activity {id,kind,path,started_at,stage,pct}`.
`GET /api/farm/queues` reports each physical execution lane, and
`GET /api/farm/node/{name}/log` returns the live runtime's newest 200 bounded log
lines. These routes, like every Manager API route, require the admin key.

Queued or running work can be cooperatively canceled with
`POST /api/farm/job/{id}/cancel`. The durable `juicefarm:cancel:{id}` flag is checked
before work starts and once per second during execution; the worker commits terminal
`canceled` before acknowledging its claim. A failed job can be retried with
`POST /api/farm/job/{id}/requeue`. That creates exactly one new ID from the failed
job's stored payload—including narrowed targets, backend, and explicit fallback
lock—while the original terminal record remains immutable and links to the retry.

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

Run a standing **server worker** as a side container on the JuiceFS compose network —
**never** by re-applying the whole stack YAML (that restarts redis/minio, which the
Mac client depends on):

```bash
docker run -d --name juicefarm-worker \
  --network <juicefs_compose_network> \
  --cap-add SYS_ADMIN --device /dev/fuse --security-opt apparmor:unconfined \
  --restart unless-stopped \
  -e JM_META=redis://redis:6379/1 \
  -e JM_FARM_QUEUE=1 \
  -e JM_WORKER_NAME=nas-server-01 \
  -e JM_WORKER_ROLE=server \
  -e JM_FARM_PRODUCER=linux-farm \
  -e JM_FARM_MODEL=/models/ggml-medium.en.bin \
  -v /path/to/juicefarm-cache:/jfs-cache \
  -v /path/to/juicefarm-state:/state \
juicefarm:local
```

Every standing worker should set a unique lowercase `JM_WORKER_NAME` and use a
persistent `/state`, persistent `/jfs-cache`, and `--restart unless-stopped`. Those
are the contracts behind Manager's pause/drain/resume/restart controls. The older
emergency disable flag remains readable for rolling-upgrade compatibility, but the
0.5 UI does not use it for planned maintenance. Restart is one-shot and is reported
as successful only after the command is acknowledged and a replacement runtime ID
heartbeats.

Run render workers separately with `JM_WORKER_ROLE=render` and the GPU device(s)
passed into the container. Render admission fails closed if the configured
accelerator cannot complete its live probe. `auto` chooses render only when the
probe succeeds; otherwise it becomes a server worker.

For the Intel Arc/Vulkan image, expose the render node and the host's matching
userspace drivers as read-only files. This matters for a GPU whose PCI ID is newer
than the Mesa release in the image (for example, Intel Battlemage `8086:e223`). The
Intel image pins `VK_ICD_FILENAMES` to the hardware ICD so Mesa's CPU-only llvmpipe
device cannot satisfy admission:

```bash
docker run -d --name juicefarm-gpu-worker \
  --restart unless-stopped \
  --network <juicefs_compose_network> \
  --cap-add SYS_ADMIN --security-opt apparmor=unconfined \
  --device /dev/fuse --device /dev/dri \
  -v /usr/lib/x86_64-linux-gnu/dri/iHD_drv_video.so:/usr/lib/x86_64-linux-gnu/dri/iHD_drv_video.so:ro \
  -v /usr/lib/x86_64-linux-gnu/libvulkan_intel.so:/usr/lib/x86_64-linux-gnu/libvulkan_intel.so:ro \
  -v /path/to/juicefarm-cache:/jfs-cache \
  -v /path/to/juicefarm-state:/state \
  -e JM_META=redis://redis:6379/1 \
  -e JM_FARM_QUEUE=1 \
  -e JM_WORKER_NAME=studio-render-01 \
  -e JM_WORKER_ROLE=render \
  -e JM_FARM_TRANSCRIPT_DEVICE=vulkan \
  -e JM_FARM_VCODEC=hevc_vaapi \
  juicefarm-gpu:local
```

The host driver bind is a recorded deployment dependency, not evidence of GPU
readiness by itself. On every start, `jmfarm` runs a real hardware encode and
decode plus a one-second Whisper inference. The worker advertises
`transcript:vulkan` only after that inference succeeds. Check the Manager worker
profile for a non-zero transcript real-time ratio and no Vulkan probe error before
allowing it to claim accelerated AI jobs.

Build gotchas (baked into the Dockerfile):

- Source the JuiceFS binary and FoundationDB client from the pinned
  `ce-v1.3.x` image, but run the worker on Debian **trixie**. Its FFmpeg 7.1+
  decodes modern camera audio such as Sony MOV/MP4 `ipcm`; inheriting the
  upstream Debian 11 userland strands those tracks on FFmpeg 4.3. `jmfarm`
  remains statically linked, and the bullseye-built static whisper binary runs
  on the newer glibc runtime.
- Use a **dedicated** farm cache dir, never the primary juicefs container's cache.
- Bind that cache from persistent local storage; do not leave `/jfs-cache` in the
  container writable layer. `JM_FARM_CACHE_DIR` selects the in-container path and
  `JM_FARM_CACHE_SIZE` sets its MiB budget (default `20000`). Keep the budget below
  the node's actual free space.

Modes: `JM_FARM_QUEUE=1` (standing queue drain) or `JM_FARM_MODE=all|transcript|…`
with `JM_FARM_ONCE=1` (one-shot sweep). Throttling knobs: `JM_FARM_PROXY_WORKERS`
(proxy `-preset slow` pins a core/clip, so it gets fewer workers than the cheap
passes), `JM_FARM_CRF`/`JM_FARM_PRESET`, `JM_FARM_NICE`/`JM_FARM_IONICE`.

---

## The proxy contract (interoperability)

Every proxy is a single MP4 (ISO BMFF) with `+faststart`; AAC-LC stereo 128 kbps /
48 kHz; a closed 2 s GOP; and recorded `codec`, `codec_string`, and `blob_size` fields
so a consumer can gate playback without re-probing. A verified hardware worker uses
HEVC/H.265 first (`hvc1` tag), then hardware H.264 if HEVC is unavailable. CPU H.264
(`libx264`) is the compatibility and outage fallback. Hardware proxy execution sets
an explicit matching hardware decoder and never retries with software decode inside
the render node; a failed/offline render node produces a visible reroute instead.

---

## Manager integration

The `juicemount-manager` web UI exposes a **Farm tab** with play/pause, automatic
discovery control, per-lane queue depth and history, cooperative job cancel and
exact-payload failed retry, live worker roles/capabilities/current stage, bounded
worker logs, measured throughput, selected backend, attempts, explicit node
pause/drain/resume/restart, commit-pinned node enrollment, and advanced manual
repair sweeps. Set `JM_FARM_NODE_META_URL` to the Redis URL that
new nodes can reach; Manager deliberately does not reuse a docker-internal Redis
hostname for enrollment. Optional `JM_FARM_SERVER_IMAGE` and
`JM_FARM_RENDER_IMAGE` values select registry images; otherwise the Manager derives
the exact `rc-0.5-<commit>` tag from its own build. Remote enrollment additionally
requires JuiceMount Link and returns a one-time Headscale command in an authenticated,
`no-store` response.

The manager is CGO-free: farm coverage is relayed from
`/state/farm-status.json`, while queue, control, and worker state are read from Redis
through bounded probes. Live sweep progress is freshness-checked: if a killed worker
leaves an old `in_progress` record behind, Manager presents it as interrupted work
instead of claiming that it is still running. Stable coverage and last-sweep history
remain available.

The same read-only state mount is also the Manager's physical-pool headroom probe.
Before admitting a manual sweep or farm-wide resume, Manager requires the larger of
64 GiB and 1% of the configured physical pool capacity to remain available. Set
`JM_FARM_STORAGE_CAPACITY_BYTES` to `zpool list -Hp -o size <pool>` on ZFS: `statfs`
available bytes reflect shared pool headroom, but its total is dataset-scoped and
must not be presented as the zpool capacity. The server worker performs the
same check before claiming output work. Each safe server-side probe refreshes a
20-second Redis storage permit. Render workers expose that they require the permit and
verify it in the same Redis transaction that claims a job. If the Manager/server loses
its pool view, Redis stops accepting permit refreshes, or the permit expires, render
workers remain visible as `waiting-storage-permit` but cannot claim more write-heavy
work. An unprobeable or undersized backend revokes the permit and engages an atomic
Redis safety pause for every worker; it never auto-resumes, and the Manager shows the
reason until an operator restores space and explicitly presses Play.
`JM_FARM_STORAGE_PATH` may name a more representative server-side path and
`JM_FARM_MIN_FREE_BYTES` may override the worker reserve for a deliberately sized
deployment.

Proxy publication uses a durable, byte-bound receipt next to `proxy.mp4`. The worker
writes and fsyncs that receipt before the final blob rename. If the blob commits but
the worker loses its private index or the shared manifest write fails, any later
worker validates the exact blob, repairs registration, and skips re-encoding. This
keeps retries idempotent across nodes and prevents overwritten JuiceFS slices from
multiplying physical storage during an outage.

JuiceFS intentionally retains both deleted files and stale slices from overwrites for
the configured trash period. Automatic farm sweeps therefore preserve any current,
source-matched, byte-validated portable proxy regardless of whether it is H.264,
HEVC, or AV1; codec migration requires explicit regeneration. JuiceMount treats
derivative idempotency and physical headroom as its own responsibilities. Do not put
the MinIO object dataset under a high-retention recursive ZFS snapshot policy: those
snapshots retain blocks after JuiceFS GC deletes the objects and make logical cleanup
appear ineffective. Selective `chattr +s` trash bypass is
not used by this RC: it was added upstream in JuiceFS v1.4.0, while the deployed stack
is pinned to v1.3.1. A coordinated all-client JuiceFS upgrade can add that optional
defense later; it is not required for the receipt and storage-permit fixes above.

---

## Status & roadmap

- **RC:** automatic recursive discovery + persistent catch-up; durable queue claims;
  farm-wide pause/play; server/render lane separation; benchmarked GPU admission;
  hardware-decoded poster/filmstrip routing with per-file CPU fallback; HEVC-first
  proxy routing with explicit H.264 fallback; a two-heartbeat render-outage grace
  that prevents a brief node restart from dumping the ready backlog onto CPU;
  portable-proxy preservation across worker codecs with explicit-only codec migration;
  backend-headroom safety pause; cross-worker proxy commit recovery;
  live Manager control, per-node drain/pause, cooperative cancellation,
  exact-payload failed retry, and bounded node telemetry; tech/poster/filmstrip/waveform/proxy/
  transcript generation; JM-15 discovery; `/blob` byte ranges; and portable
  assertion sidecars.
- **Not in this RC:** richer AI (faces/OCR/framing), historical benchmark models,
  cross-volume locality scheduling, and the incomplete Teams/seats authorization UI.

For the wire contract OpenLoupe and JuiceMount share, see the private
`juicemount-contract` repository (spec + golden fixtures + `PROVIDER_STATUS` /
`CONSUMER_STATUS`).
