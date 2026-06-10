# JuiceMount

**A LucidLink-style mounted volume for video editors — running entirely on hardware you own.**

JuiceMount is a macOS menu-bar app that mounts shared storage at `/Volumes/<name>` and makes it behave the way editors need it to: scrub a 100 GB file and only the blocks you touch come over the wire, pin a project and keep cutting on a plane, drop a render on the volume and it lands on local SSD instantly while uploading in the background. The backend is a box you already have — a TrueNAS, a Synology, any Linux machine running Docker — not a storage contract.

Built on [JuiceFS](https://github.com/juicedata/juicefs) (metadata in Redis, file data in JuiceFS's open, documented chunk format in MinIO or any S3-compatible store), re-exported locally through a Finder-tuned NFS server with an SQLite metadata cache, an SSD block cache, offline pinning, and a write spool.

> **Status:** pre-1.0, macOS-only, self-hosted. If you want a managed service with support contracts, this is not that — see [What JuiceMount is not](#what-juicemount-is-not).

---

## Why this exists

Storage for small video teams currently forces a three-way trade:

1. **Block-streaming SaaS** (LucidLink, Suite, Shade, Aspect) gets you the magic mounted-drive workflow — and a per-seat, per-TB bill that scales with exactly the thing video generates most of. Your library lives inside their filesystem.
2. **Self-hosted sync** (Nextcloud, Seafile, Mountain Duck) gets you ownership — but syncs whole files. Opening a 100 GB clip to check one shot means moving 100 GB. And in the author's testing these tools plateau around 0.8–1 Gbit/s even on a 10 GbE LAN.
3. **Plain NFS/SMB to a NAS** gets you speed and ownership — and nothing else. No offline files, no cache, no WAN story, and Finder grinds on a 100 K-file library.

JuiceMount is the missing combination: **partial-file streaming and Dropbox-style cache/offline semantics, at near-line-speed on your own LAN, against storage you own.** On the author's 10 GbE setup it sustains roughly 7 Gbit/s in both directions — the same hardware where Mountain Duck/Seafile-class tools capped near 1 Gbit/s in the same tests. (All performance claims here are the author's own measurements, not independent benchmarks — see [Performance](#performance).)

There is no penalty for choosing the hybrid. That's the whole product.

S3 and cloud collaboration platforms have their place — this exists because at video scale, cloud-storage pricing is brutal, and a small team shouldn't have to accept inferior infrastructure to afford speed.

---

## What you get

All of the below is shipped and exercised in the current codebase (see [`ROADMAP.md`](ROADMAP.md) for validation status):

- **Block-level partial reads.** Files are stored as chunked objects (JuiceFS). Scrubbing 3 seconds of a 100 GB OCF streams only those blocks — no full-file download, ever.
- **Local SSD cache that respects your disk.** Set a cache size; JuiceMount grows it only as far as needed to keep your pinned content fully cached, and never squeezes the boot disk below a hard 10 GiB free floor. It also reclaims APFS purgeable space (Time Machine local snapshots) at mount time and on demand, so "disk full" usually isn't.
- **Pin folders for offline.** From the menu-bar popover or Finder right-click → Services → *JuiceMount: Pin for Offline*. A prefetcher pulls every byte to local SSD; *Sync Now* runs verify-and-repair to re-fetch anything the cache evicted.
- **Offline-files mode.** Flip one toggle (cellular, plane, NAS down): pinned files keep working at SSD speed; un-pinned reads fail in milliseconds instead of beachballing Finder for 30+ seconds.
- **Write spool** *(opt-in: Preferences → Cache & Storage → Enable write spool)*. Writes ack the moment they're durable on local SSD, then trickle-upload to the server in the background at whatever the network allows — SHA-256-verified at every hop. A 2 GB Finder copy over a WAN feels like a local copy, and the popover shows pending uploads until they drain.
- **Instant search across the whole library.** ⌘⇧F from any app (global hotkey, toggleable): SQLite FTS5 trigram index, results in tens of milliseconds across 100 K+ entries. Spacebar Quick Look, Enter to reveal in Finder, drag results straight into a Premiere/Resolve/FCPX timeline.
- **Same absolute path on every machine.** Every editor mounts `/Volumes/<name>`, and metadata syncs through the shared Redis instance — project files reference media at identical paths, so a teammate's `.prproj`/`.drp`/`.fcpx` opens without relinking. This is the designed multi-machine workflow; note that heavy *simultaneous* multi-editor use hasn't been soak-tested yet (most QA to date is single-editor).
- **A menu-bar app, not a daemon you babysit.** A state-tinted menu-bar icon (green healthy / amber degraded / blue offline-files / red fault) with an upload-activity badge, health detail for Redis/MinIO/FUSE/NFS, cache and pin progress, disk-pressure banners, structured JSON logs, and an HTTP control plane (`/metrics`, `/health`, `/pin`, `/offline`, `/spool`, `/mount-now`, …) for scripting.
- **A server stack that's one `docker compose up`.** Redis + MinIO + a JuiceFS mount with WebDAV access + **JuiceMount Manager**, a web UI for migrating existing data into the volume plus trash, backups, maintenance, and settings tabs. Migration is the production-tested path (live progress, junk-file filtering, sequential job queue); the other tabs are newer. Production-tested install path for TrueNAS SCALE (paste-the-YAML), works on any Docker host.

---

## How it fits together

```
 YOUR MAC                                          YOUR SERVER (any Docker host)
┌────────────────────────────────────────┐        ┌──────────────────────────────┐
│ Premiere / Resolve / FCPX / Finder     │        │  docker compose up -d        │
│            │                           │        │                              │
│            ▼  NFS v3 (localhost only)  │        │  ┌─────────┐  ┌───────────┐  │
│ ┌──────────────────────────────┐       │        │  │  Redis  │  │   MinIO   │  │
│ │ JuiceMount.app               │       │  LAN / │  │ (file   │  │ (file     │  │
│ │  • NFS server, Finder-tuned  │◄──────┼──WAN──►│  │  meta-  │  │  data as  │  │
│ │  • SQLite metadata + search  │       │        │  │  data)  │  │  plain S3 │  │
│ │  • SSD block cache + pins    │       │        │  └─────────┘  │  objects) │  │
│ │  • write spool (opt-in)      │       │        │               └───────────┘  │
│ │  • JuiceFS client (FUSE)     │       │        │  ┌────────────────────────┐  │
│ └──────────────────────────────┘       │        │  │ JuiceMount Manager     │  │
│            │                           │        │  │ web UI :30190          │  │
│            ▼                           │        │  │ (migrate / maintain)   │  │
│   /Volumes/<name>  ← editors work here │        │  └────────────────────────┘  │
└────────────────────────────────────────┘        └──────────────────────────────┘
```

Reads are served in priority order: memory buffer (small files) → direct SSD-cache reads that bypass FUSE entirely → JuiceFS → your object store. Metadata (the thing Finder hammers hardest) never leaves local SQLite on the hot path — directory opens that take 3–10 s through raw FUSE complete in 15–120 ms. Full detail in [`ARCHITECTURE_juicemount.md`](ARCHITECTURE_juicemount.md).

The object store can also be a cloud bucket (Backblaze B2, Cloudflare R2, Wasabi — anything S3-compatible that JuiceFS supports); Redis still runs on your box. Be honest with yourself about what's been proven, though: the self-hosted MinIO/TrueNAS path is the one this project's QA exercises; cloud buckets ride on JuiceFS's S3 support and haven't been part of JuiceMount's own test matrix.

---

## Requirements

Honest list — this is a self-hosted system and there is a server side.

**On the Mac (client):**

- macOS 14 (Sonoma) or later (`Package.swift` targets `.macOS(.v14)`). Developed and tested on Apple Silicon; Intel Macs are untested (the build scripts produce host-architecture binaries, so an Intel build *should* work from source, but no one has verified it).
- [macFUSE](https://macfuse.github.io/) — required by the JuiceFS client. The first-run Setup Assistant preflight checks that it's installed and walks you through it if not.
- The `juicefs` binary (`brew install juicefs`; auto-detected from `/opt/homebrew/bin`, `/usr/local/bin`, `/usr/bin`, then `$PATH`).
- **An admin password prompt, once per session**, the first time JuiceMount mounts: macOS restricts `mount_nfs`/`umount` to root, so the app escalates through the standard macOS auth dialog (and macOS caches the authorization for the session). Optionally set up a [scoped passwordless-sudo rule](docs/dev-setup.md) (exactly `/sbin/mount_nfs`, `/sbin/umount`, `/bin/mkdir` — no shell, no wildcards) to remove the prompt entirely.
- To build from source (currently the only way to get the app — no prebuilt/notarized DMG yet): Go 1.26+ and Xcode command-line tools (Swift 5.9+).

**On the server (any of these):**

- TrueNAS SCALE (the production-tested path — paste-the-YAML install, see [`server/INSTALL-TrueNAS.md`](server/INSTALL-TrueNAS.md)), or
- Any Linux box / Synology DSM 7+ / laptop with Docker + Docker Compose.
- Disk for two things: the MinIO object bucket (your actual media) and a small Redis dataset (metadata; AOF-persisted).
- A LAN you trust — the stack's Redis and MinIO ports are LAN-exposed by default; firewall them if untrusted clients share the network (see [`server/README.md` § Security notes](server/README.md)).

**Network:** anything from hotel Wi-Fi (with pinned files + the write spool) up to 10 GbE (where the throughput ceiling becomes your disks). For WAN use the author runs Tailscale; any VPN that gives the Mac a route to the Redis + MinIO ports works.

---

## Quick start

### 1. Server (≈10 minutes)

```sh
git clone https://github.com/lelanddutcher/juicemount
cd juicemount/server

# Edit docker-compose.yml: set the bind-mount paths for your disks
# and a strong MINIO_ROOT_PASSWORD (openssl rand -base64 24).

docker compose up -d
docker compose ps                    # wait for all services healthy
docker compose logs juicefs-init    # confirms first-time volume format
```

TrueNAS SCALE users: **Apps → Discover → ⋮ → Install via YAML**, paste the compose. Full walkthrough in [`server/INSTALL-TrueNAS.md`](server/INSTALL-TrueNAS.md).

Already have terabytes on the NAS? Open the **JuiceMount Manager** web UI at `http://<server>:30190` and use the Migrations tab — live progress, junk-file filtering (`.DS_Store`, `._*`, `Thumbs.db`), sequential job queue.

### 2. Mac

```sh
brew install juicefs
brew install --cask macfuse          # approve the system extension if macOS asks

git clone https://github.com/lelanddutcher/juicemount
cd juicemount
./scripts/build-app.sh               # Go c-archive + Swift app + codesign
./scripts/install.sh                 # → /Applications  (add --launchd for login start)
open /Applications/JuiceMount.app
```

A locally built app is not quarantined, so Gatekeeper won't object. If you instead obtained a pre-built `JuiceMount.app` from someone else (it's unsigned/ad-hoc-signed), macOS will block it: either remove the quarantine flag with `xattr -d com.apple.quarantine /Applications/JuiceMount.app`, or launch once and approve it under **System Settings → Privacy & Security → Open Anyway** (the right-click-Open trick no longer bypasses Gatekeeper on macOS 15 and later).

On first launch the **Setup Assistant** opens automatically: it preflight-checks `juicefs`, macFUSE, and backend reachability, and walks you through pointing the app at your box (also reachable later via menu-bar icon → Setup Assistant…, or **Preferences → Connection**):

- **Redis URL:** `redis://<server>:30179/1`
- **S3 endpoint override:** `http://<server>:30151/<bucket>` (only needed if the volume was formatted with a docker-internal hostname)

(MinIO credentials live in the JuiceFS volume's format metadata in Redis — they don't need to be re-entered on the Mac.)

Hit **Start**. Enter your admin password at the mount prompt (once per session). `/Volumes/<name>` appears in Finder; point your NLE's media browser at it and edit.

---

## Performance

All numbers below were **measured by the author on his own setup** (Apple-Silicon Mac ↔ TrueNAS SCALE over 10 GbE; methodology, workload scripts, and regression harness in [`docs/PERFORMANCE_METHODOLOGY.md`](docs/PERFORMANCE_METHODOLOGY.md)). They are honest measurements, not marketing benchmarks — your hardware will differ.

| What | Measured |
|---|---|
| Sustained network throughput, 10 GbE LAN | ~7 Gbit/s up and down (author-measured) |
| Cached read through the full NFS path (`dd`, 200 MiB) | **226–571 MB/s**, READ p95 481 µs |
| Fully-cached 200 MiB sequential read | 431 MB/s with **4.6 MB** total network traffic |
| Pinned 350 MB file read, network off | 215+ MB/s sustained |
| Directory open, 100 K+-entry volume | 15–120 ms (3–10 s through raw FUSE) |
| Filename search across ~131 K entries | ~29 ms |
| Un-pinned read refusal in offline mode | 4–67 ms (vs. a 30 s NFS retry hang) |

For comparison on the same 10 GbE link, Mountain Duck/Seafile-class mounted-bucket tools measured ~0.8–1 Gbit/s in the author's testing — they're bound by single-stream sync engines, not the network.

---

## How it compares

Two different categories claim to solve this; JuiceMount sits deliberately between them. Pricing below was checked **June 2026** from public pricing pages — verify before relying on it.

**vs. storage SaaS for editors:**

| | **JuiceMount** | LucidLink | Suite | Shade | Aspect |
|---|---|---|---|---|---|
| Your files live | your hardware / your bucket | their cloud (AWS) | their cloud ($75/TB/mo) or BYO ($40/TB/mo) | their cloud | their cloud |
| Partial-file streaming | ✅ block-level | ✅ | ✅ | ✅ (ShadeFS) | ✅ |
| Offline pinned files | ✅ | ✅ | not clearly documented | not clearly documented | not clearly documented |
| Pricing model | **$0 — bring your own hardware** | $7–27+/user/mo + $8/100 GB extra | $75/TB/mo, +$10/user after 5 | $29.75/seat/mo (500 GB active/seat) | free tier + custom enterprise |
| Exhaustive metadata/AI search | roadmap (filename search today) | — | — | ✅ | ✅ |
| Open source | ✅ | — | — | — | — |
| Leave with your bytes intact | your hardware — copy from the mounted volume or `juicefs sync`; the bucket stays under your control | export required | export required | export required | export required |

These products are good at things JuiceMount doesn't do: managed convenience, review/approval tools, AI semantic search, Windows clients, someone to call. If you want those and the bill works for you, use them.

**vs. self-hosted sync** (category behavior as of June 2026 — check current client docs, these tools evolve):

| | **JuiceMount** | Nextcloud | Seafile | Mountain Duck |
|---|---|---|---|---|
| Open a 100 GB file to check one shot | streams the blocks you touch | syncs the file | fetches the file | fetches the file |
| 10 GbE LAN throughput | ~7 Gbit/s (author-measured) | ~1 Gbit/s class | ~1 Gbit/s class | ~0.8–1 Gbit/s (author-measured) |
| Offline files + fail-fast offline mode | ✅ | sync model | sync model | cache, no pin semantics |
| Finder-native NLE workflow (identical paths, scrub-in-place) | ✅ | — | — | partial |
| Cost | $0, OSS | $0, OSS | $0 community | $49 one-time |

---

## What JuiceMount is not

Stating this up front saves everyone time:

- **Not a SaaS.** No hosted offering, no accounts, no billing. You run the server.
- **Not a review platform.** No browser viewer, comments, or approvals — Frame.io and friends own that lane and pair fine with this.
- **Not AI media search.** Filename search is instant today; content-aware search (the thing Shade/Aspect/Iconik do well) is acknowledged roadmap, not a current feature.
- **Not multi-OS.** macOS only today. The server side runs anywhere Docker does.
- **Not a backup.** It's primary storage with a cache. Run real backups of the MinIO bucket and Redis — the Manager has backup-scheduling tooling, but the 3-2-1 discipline is yours.
- **Not zero-ops.** A failed disk on your NAS is your failed disk. That's the deal that makes it free.

---

## Roadmap

Next up (full ranked list in [`ROADMAP.md`](ROADMAP.md) and `VISION/feature-roadmap-ranked.md`):

1. Codec-aware Quick Look proxies (R3D / ARRI / BRAW / ProRes RAW)
2. Content-hash backup verification with a traffic-light inventory
3. Bandwidth-aware automatic offline/streaming mode

---

## Built on JuiceFS — credit where due

JuiceMount exists because [JuiceFS](https://github.com/juicedata/juicefs) (Apache-2.0, by [Juicedata](https://juicefs.com)) solved the hard distributed-filesystem problems — chunked object layout, Redis metadata engine, cache management — and proved them in production for years. JuiceMount is a macOS-native experience layer on top: the Finder-tuned NFS re-export, metadata caching, pinning, offline gates, the write spool, the menu-bar app, and the server packaging. The NFS server is a fork of [`willscott/go-nfs`](https://github.com/willscott/go-nfs) (Apache-2.0), vendored at `internal/nfs` and attributed in [`NOTICE`](NOTICE).

If JuiceFS itself fits your (non-video, non-macOS) problem, use it directly — it's excellent.

## License

[Apache License 2.0](LICENSE). Third-party attributions (JuiceFS, go-nfs, go-nfs-client) are in [`NOTICE`](NOTICE); JuiceFS and go-nfs are likewise Apache-2.0, go-nfs-client is BSD-2-Clause.

## Contributing

- **Bugs:** open an issue with the diagnostic zip (menu-bar → Export Diagnostics) — it bundles logs, mount table, and backend health.
- **Code:** one theme per PR — stability fixes never share a commit with features. Run `go vet ./...` and `go test -race` on touched packages; request-path changes get an adversarial review pass (see `docs/QA-procedure.md`).
- **Testing on real hardware** is the most valuable contribution: different NAS vendors, network shapes, and NLE versions. Real Finder/Resolve/Premiere testing beats synthetic checks — that rule is earned, see the QA history in `docs/`.
- **Developer setup** (passwordless mount for fast test cycles, headless CLI, config reference): [`docs/dev-setup.md`](docs/dev-setup.md).

*Non-negotiables, so you don't waste a PR:* no telemetry without opt-in, no proprietary dependencies for self-hosters, no FileProviderExtension (ever — `docs/no-fileprovider.md` is the postmortem), reliability beats novelty.
