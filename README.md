<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/readme/banner-dark.svg">
  <img src="assets/readme/banner-light.svg" alt="JuiceMount — the mounted-drive workflow editors get from LucidLink-class SaaS, at 10GbE-direct-attached speed, with Dropbox-style offline resilience, on hardware you already own" width="100%">
</picture>

# JuiceMount

[![License: Apache-2.0](assets/readme/badges/license-apache-2.0.svg)](LICENSE)
![Platform: macOS 14+](assets/readme/badges/platform-macos-14plus.svg)
[![Built on JuiceFS](assets/readme/badges/built-on-juicefs.svg)](https://github.com/juicedata/juicefs)
![Status: beta](assets/readme/badges/status-beta.svg)

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

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/readme/arch-diagram-dark.svg">
  <img src="assets/readme/arch-diagram-light.svg" alt="Architecture: Finder, Resolve, and Premiere open files on /Volumes/(name) through the macOS NFS client, served by the JuiceMount app's localhost-only NFS server. An SQLite mirror, an SSD cache with pins, and an opt-in write spool keep the hot path local. Behind the app, a hidden JuiceFS FUSE mount crosses your LAN or WAN to your server, where MinIO or any S3-compatible store holds file data in JuiceFS's open, documented chunk format and Redis holds file metadata" width="100%">
</picture>

The server side is one `docker compose up -d`: Redis, MinIO, and **JuiceMount Manager** (a web UI on `:30190` for migrating data in and maintaining the stack).

Reads are served in priority order: memory buffer (small files) → direct SSD-cache reads that bypass FUSE entirely → JuiceFS → your object store. Metadata (the thing Finder hammers hardest) never leaves local SQLite on the hot path — directory opens that take 3–10 s through raw FUSE complete in 15–120 ms. Full detail in [`ARCHITECTURE_juicemount.md`](ARCHITECTURE_juicemount.md).

The object store can also be a cloud bucket (Backblaze B2, Cloudflare R2, Wasabi — anything S3-compatible that JuiceFS supports); Redis still runs on your box. Be honest with yourself about what's been proven, though: the self-hosted MinIO/TrueNAS path is the one this project's QA exercises; cloud buckets ride on JuiceFS's S3 support and haven't been part of JuiceMount's own test matrix.

<!-- SCREENSHOTS: uncomment when docs/screenshots/*.png land (capture steps per shot: docs/screenshots/CAPTURE.md)

## A look at it

<p>
  <img src="docs/screenshots/menubar-states.png" alt="JuiceMount menu-bar icon states: green healthy, amber degraded, blue offline-files, red fault" width="70%">
</p>
<p>
  <img src="docs/screenshots/popover-glance.png" alt="Menu-bar popover: at-a-glance health, backend rows, cache and pinned folders, pending uploads" width="49%">
  <img src="docs/screenshots/onboarding.png" alt="Setup Assistant preflight checks: juicefs, macFUSE, backend reachability" width="49%">
</p>
<p>
  <img src="docs/screenshots/preferences-connection.png" alt="Preferences, Connection tab: volume name, Redis URL, S3 endpoint override" width="49%">
  <img src="docs/screenshots/calculator-web.png" alt="The rent-vs-own calculator from the JuiceMount site" width="49%">
</p>

-->

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

| | **JuiceMount** | LucidLink | Suite | Shade |
|---|---|---|---|---|
| Your files live | your hardware / your bucket | their cloud (AWS) | their cloud ($75/TB/mo) or BYO ($40/TB/mo) | their cloud |
| Partial-file streaming | ✅ block-level | ✅ | ✅ | ✅ (ShadeFS) |
| Offline pinned files | ✅ | ✅ | not clearly documented | ✅ (documented) |
| Pricing model | **$0 — bring your own hardware** | $7–27+/user/mo + $8/100 GB extra ($27 is promo off $32 list) | $75/TB/mo, +$10/user after 5 | $29.75/seat/mo annual ($35 monthly), 500 GB active/seat |
| Exhaustive metadata/AI search | roadmap (filename search today) | no | no | ✅ |
| Open source | ✅ | no | no | no |
| Leave with your bytes intact | your hardware — copy from the mounted volume or `juicefs sync`; the bucket stays under your control | export required | export required | export required |

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

## FAQ

Straight answers, sourced from the docs and code in this repo. Where something hasn't been verified, it says so.

**Why does JuiceMount ask for an admin password?**

macOS restricts `mount_nfs` and `umount` to root, so the app escalates through the standard macOS authorization dialog the first time it mounts — once per session; macOS caches the authorization. If you restart the app often (or just dislike the prompt), [`docs/dev-setup.md`](docs/dev-setup.md) sets up a scoped passwordless-sudo rule — exactly `/sbin/mount_nfs`, `/sbin/umount`, `/bin/mkdir`, no shell, no wildcards — and the app probes for it and uses it automatically, falling back to the prompt on machines without it.

**What happens when the NAS is off, or I'm on a plane?**

Pin what you need first (popover → *Pin Folder for Offline…*, or Finder right-click → Services → *JuiceMount: Pin for Offline*): a prefetcher pulls every byte to local SSD and shows per-folder progress. Then flip on offline-files mode (popover toggle): pinned files keep reading at SSD speed, and un-pinned reads refuse in 4–67 ms instead of hanging Finder on a ~30 s NFS retry. Back online, *Sync Now* runs verify-and-repair on the pin set, re-fetching anything the cache evicted.

**What happens to my writes if the network drops or the server dies mid-copy?**

With the write spool enabled (Preferences → Cache & Storage), a write is acknowledged the moment it's durable on local SSD; a background drainer uploads it once the server is reachable, SHA-256-verified at every hop. The popover shows pending / in-flight / stalled / failed uploads with per-entry age and last error, offers *Retry failed* and *Recover stalled*, and the app guards quit and spool-disable while uploads are pending so spooled data isn't stranded. With the spool off (the default), writes go through to the server synchronously — if the backend is unreachable, the write fails the way it would on any network drive. Note that offline-files mode gates *reads*; it doesn't make un-spooled writes safe. <!-- sources: docs/dev-setup.md (write path), MENU_BAR_APP.md (spool UI), docs/OPEN_BUGS.md launch-hardening closures (quit/disable drain guards); the offline open gate in nfs/handler.go applies to reads only -->

**Can two Macs mount the same volume?**

Yes — every machine mounts the same `/Volumes/<name>`, metadata syncs through the shared Redis instance, and project files reference media at identical paths, so a teammate's `.prproj`/`.drp`/`.fcpx` opens without relinking. That's the designed multi-machine workflow. The caveat from above bears repeating: heavy *simultaneous* multi-editor use hasn't been soak-tested yet — most QA to date is single-editor.

**Is my data locked in?**

No. Your bytes are on your hardware — copy them from the mounted volume, or `juicefs sync` them out; the bucket stays under your control. In the bucket, file data lives in JuiceFS's open, documented chunk format, and the volume is a standard JuiceFS volume (the server stack formats it with stock `juicefs format`), so the stock `juicefs` client can mount it with no JuiceMount involved. <!-- exit story verbatim from the comparison table above; standard-volume claim: server/INSTALL-TrueNAS.md runs stock juicefs format -->

**How much disk does the cache use?**

You set the cache size in Preferences → Cache & Storage; JuiceMount grows the cache only as far as needed to keep your pinned content fully cached, and clamps it so the boot disk always keeps at least 10 GiB free (the JuiceFS free-space ratio is raised dynamically to enforce the floor). It also reclaims APFS purgeable space — mostly Time Machine local snapshots — at mount time and on demand via the popover's *Reclaim* button. When you leave, `scripts/uninstall.sh` shows the size of everything it would delete before touching anything; the JuiceFS chunk cache is usually the big one (it can be hundreds of GB).

<details>
<summary><strong>Why an NFS loopback server instead of using FUSE directly, or a File Provider extension?</strong></summary>

Finder performance, mostly. Metadata is the thing Finder hammers hardest, and JuiceMount answers it from a local SQLite mirror behind a Finder-tuned NFS server — directory opens that take 3–10 s through the raw FUSE mount complete in 15–120 ms. The FUSE mount still exists underneath (JuiceFS needs it), but it's hidden and apps never touch it. As for File Provider: never. An orphaned File Provider registration once pinned two system daemons above 100% CPU and collapsed this very NFS path to 13 MB/s — and the registration outlived the app, the source project, and a reboot. [`docs/no-fileprovider.md`](docs/no-fileprovider.md) is the postmortem; the build script fails if a plugin ever sneaks into the bundle.

</details>

<details>
<summary><strong>What exactly is the relationship to JuiceFS?</strong></summary>

JuiceMount is built on JuiceFS and says so loudly (see the credit section below and [`NOTICE`](NOTICE)). JuiceFS solved the distributed-filesystem problems — chunked object layout, the Redis metadata engine, cache management — and JuiceMount adds the macOS experience layer: the Finder-tuned NFS re-export, the SQLite metadata cache, pinning and offline gates, the write spool, the menu-bar app, and the server packaging. The app drives the separately installed `juicefs` binary (`brew install juicefs`); it isn't bundled. If your problem isn't video-on-macOS, use JuiceFS directly — it's excellent.

</details>

<details>
<summary><strong>Does it phone home?</strong></summary>

No. The app's network connections are the Redis and S3 endpoints you configure, plus a loopback control plane on `127.0.0.1`. JuiceFS's own anonymous usage reporting is explicitly disabled — the app passes `--no-usage-report` when mounting. There's no crash reporting, no update check, no analytics; "no telemetry without opt-in" is a stated non-negotiable (see [Contributing](#contributing)). Diagnostics exist only as a local zip you create yourself with Export Diagnostics and choose to share. <!-- verified: health/fuse.go passes the no-usage-report flag to juicefs mount; the app's only URLSession targets are loopback control-plane routes; non-negotiables in Contributing + docs/VISION.md -->

</details>

<details>
<summary><strong>Why is the write spool off by default?</strong></summary>

It's the newest piece of the write path, and a change to where your data's durability boundary sits should earn default-on status. The spool's integrity story is strong — per-hop SHA-256, boot-time crash recovery, drain guards on quit and disable, exercised through the launch-hardening QA gates — but a planned 24-hour live soak is still on the books ([`docs/OPEN_BUGS.md`](docs/OPEN_BUGS.md)). With the spool off, writes use the unchanged direct path. Flip it on in Preferences → Cache & Storage when background-upload writes are worth it to you; over a WAN they're transformative.

</details>

<details>
<summary><strong>Why does it need macFUSE?</strong></summary>

The JuiceFS client mounts its filesystem through FUSE, and FUSE on macOS means macFUSE. That mount is internal — hidden where Finder and your NLE never browse it; everything user-facing goes through the kernel's native NFS client at `/Volumes/<name>`. The Setup Assistant's preflight checks for macFUSE and walks you through installing it if it's missing.

</details>

<details>
<summary><strong>Does it run on Intel Macs?</strong></summary>

Untested, honestly. Development and testing are on Apple Silicon; the build scripts produce host-architecture binaries, so building from source on an Intel Mac *should* work, but no one has verified it. macOS 14 (Sonoma) or later applies either way. If you try it, a report — success or failure — is a genuinely useful contribution.

</details>

<details>
<summary><strong>Can the object store be a cloud bucket instead of MinIO?</strong></summary>

Yes — anything S3-compatible that JuiceFS supports (Backblaze B2, Cloudflare R2, Wasabi, …), with Redis still running on your box. Honesty check, same as above: the self-hosted MinIO/TrueNAS path is what this project's QA exercises; cloud buckets ride on JuiceFS's S3 support and haven't been part of JuiceMount's own test matrix.

</details>

---

## Troubleshooting

The popover's health rows (Redis / MinIO / FUSE / NFS mount) are the first thing to check — most fixes start there. Deeper app-side detail lives in [`MENU_BAR_APP.md`](MENU_BAR_APP.md).

**The volume doesn't appear in Finder.** Open the popover: if the NFS row says "Volume not mounted", click **Mount Now** — a privileged re-mount that may show the admin prompt once. (Scriptable as `/mount-now` on the control plane; it's single-flighted and returns 409 while a mount is already in progress.) If the prompt itself is the obstacle — headless Mac, automated restarts — set up the [scoped sudoers rule](docs/dev-setup.md). Also check that something else doesn't already own the path: `mount | grep <volume-name>`.

**Finder says "not responding", or the icon turns amber.** Amber means degraded: running, but a backend (Redis / MinIO / FUSE / NFS) is unhealthy or recovering — the popover names which one and why. Give it a moment: the health monitor force-remounts a wedged FUSE daemon once the backend is reachable again (in the controlled long-outage repro this took about 15 s after the network returned), and an independent watchdog keeps the menu-bar state converging on reality instead of sticking. If the kernel mount itself is wedged — server died, every Finder access hangs — **Force Eject** in the popover is the last resort: a privileged kernel-level unmount behind a confirmation dialog, after which in-flight operations on the volume fail with I/O errors rather than hanging. <!-- self-heal story: QA-36 and QA-38 closure notes in docs/OPEN_BUGS.md; Force Eject: MenuPopoverView.swift -->

**Uploads look stuck.** With the spool enabled, the popover's *Pending uploads* section shows pending / in-flight / stalled / failed counts with per-entry age and last error; **Retry failed** and **Recover stalled** act on them directly (scriptable as `/spool` and `/spool-recover` on the control plane). A full spool surfaces to Finder as "disk full" rather than a mystery error.

**You want to re-run first-time setup.** Menu-bar icon → **Setup Assistant…** reopens onboarding any time: it preflight-checks `juicefs`, macFUSE, and backend reachability, and re-points the app at your server (same fields as Preferences → Connection).

**Where logs live.** Structured JSON at `~/Library/Logs/JuiceMount/juicemount.log` (16 MB × 5 rotation); the JuiceFS daemon's own log is auto-tailed into it with warnings promoted. `tail -f ~/Library/Logs/JuiceMount/juicemount.log | jq .` for live debugging. For a bug report, use **Export Diagnostics…** (in the popover and in Preferences → Maintenance): it bundles logs, the mount table, and backend health into a local zip — nothing is sent anywhere.

**Uninstalling.** `./scripts/uninstall.sh` stops the app, unmounts, shows exactly what it will remove with sizes, and asks once. One warning worth repeating from the script itself: if the write spool still holds files, those are uploads that never reached the server — deleting them loses data, so that step requires its own explicit confirmation (`--delete-pending-uploads` for unattended runs). It deliberately leaves the app bundle, the `juicefs` binary, macFUSE, and everything on your server alone; `--dry-run` previews the whole plan.

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
