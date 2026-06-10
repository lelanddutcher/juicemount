# Dev setup — passwordless mount/unmount for automated testing

JuiceMount's mount and unmount paths require root privileges
because macOS's `mount_nfs(8)` and `umount(8)` are restricted system
commands. Out of the box, every restart pops a macOS admin password
prompt — fine for end-users (one prompt per session), painful for
the dev workflow where every test cycle restarts the app.

This document sets up **passwordless sudo for mount_nfs and umount**
on your local machine. With it configured:

- JuiceMount starts and mounts non-interactively
- Restarting for testing is free
- End-user behavior on machines WITHOUT this config is unchanged
  (the AppleScript admin prompt path still works as fallback)

## Why this is safe

The sudoers entry grants password-free execution of exactly four
binaries with restricted argument shapes:

- `/sbin/mount_nfs` — mount an NFS volume
- `/sbin/umount` — unmount a volume
- `/bin/mkdir` — create the mount point directory

These commands are individually used by the bridge with arguments
that are constructed in-process from typed config — not from
user-controllable strings. There's no shell expansion, no
indirection through `sh -c`, and no broad `ALL` rule.

A malicious local process could still call `sudo mount_nfs` on its
own — but if a malicious process is running locally, it already has
your user privileges and far worse options.

## One-time setup

Run this in a terminal — it will prompt for your admin password
**once** to create the sudoers file, then never again:

```bash
sudo tee /etc/sudoers.d/juicemount-mount >/dev/null <<EOF
# JuiceMount passwordless mount/unmount.
# See docs/dev-setup.md in the repo for rationale and scope.
%admin ALL=(ALL) NOPASSWD: /sbin/mount_nfs, /sbin/umount, /bin/mkdir
EOF
sudo chmod 0440 /etc/sudoers.d/juicemount-mount
sudo visudo -c -f /etc/sudoers.d/juicemount-mount
```

The final `visudo -c` verifies syntax and refuses to apply a broken
file. If it prints `parsed OK`, you're set.

To scope the rule to your specific user instead of all admins,
replace `%admin` with your username (the output of `whoami`).

## Verifying it works

```bash
# Probe sudo non-interactivity:
sudo -n /sbin/mount_nfs -h >/dev/null 2>&1 && echo "OK" || echo "FAIL"
```

If `OK`, restart JuiceMount and confirm in
`~/Library/Logs/JuiceMount/juicemount.log` that you see:

```
"nfs mounted via passwordless sudo"
```

instead of the previous AppleScript-with-admin prompt path.

## Undoing it

```bash
sudo rm /etc/sudoers.d/juicemount-mount
```

The bridge automatically reverts to the AppleScript admin-prompt
path when passwordless sudo is unavailable. No code change needed
to revert.

## Behavior on machines WITHOUT this config

The bridge probes `sudo -n` before trying. If passwordless sudo
isn't available, it falls through to the AppleScript admin-prompt
path that's been there since v1. End users on shipped JuiceMount
builds will see the password prompt exactly once per session, as
expected.

## What's covered, what's not

**Covered:** the in-app mount, the in-app unmount (clean shutdown),
and the privileged force-unmount escalations.

**Not covered:** anything else that escalates privileges. The
`Reclaim` button uses `tmutil` which doesn't need root. The
`Export Diagnostics` button runs entirely in user space. No other
paths use admin-elevation.

---

# Developer reference

Operational details that used to live in the top-level README and are
developer- rather than user-facing.

## Build & run from the repo

```bash
# Build (Go c-archive + Swift app + .app bundle + ad-hoc codesign)
./scripts/build-app.sh

# Install
./scripts/install.sh                  # to /Applications
./scripts/install.sh --launchd        # also enable login auto-start

# Or run from the build dir without installing
open ./build/JuiceMount.app
```

## Headless CLI (no menu-bar app)

For headless testing or non-app deployment, `cmd/jm5` runs the same NFS
server as a standalone process:

```bash
./scripts/build-cli.sh                # builds /tmp/jm5
/tmp/jm5 --redis redis://127.0.0.1:6379/1 \
         --mount /Volumes/zpool \
         --listen 127.0.0.1:11049 \
         --db /tmp/jm5.db \
         --cache-size 100000
```

## Configuration reference

End users configure everything through the app's Preferences window
(General / Connection / Cache & Storage / Maintenance). Defaults:

- **Redis URL:** `redis://127.0.0.1:6379/1`
- **Volume name:** `zpool` → mount point `/Volumes/zpool`
- **NFS listen:** `127.0.0.1:11049`
- **Metrics / control plane:** `127.0.0.1:11050` (`/metrics`, `/health`,
  `/pin`, `/offline`, `/spool`, `/spool-recover`, `/reclaim`, `/mount-now`)
- **SSD cache:** the configured size is respected; it grows only as far
  as needed to keep the pinned set fully cached, and is clamped so the
  boot disk always keeps ≥10 GiB free (`--free-space-ratio` is raised to
  enforce the floor dynamically).
- **Memory buffer:** 2 GiB budget, files <128 MiB (tunable in
  Preferences since Phase 3b).
- **Write spool:** enabled via Preferences → Cache & Storage in the app.
  The `JM_SPOOL_ENABLE=1` env var works **only for the `jm5` CLI** — the
  embedded c-archive snapshots its environment before Swift could set
  it, so the app passes the flag through its config JSON instead.
  Spool knobs (CLI: env; app: Preferences/config): `JM_SPOOL_DIR`
  (default `~/Library/Application Support/JuiceMount/spool/`),
  `JM_SPOOL_SIZE_GB` (default 50). Live status: `127.0.0.1:11050/spool`.
- **WAN tuning (env, read by the JuiceFS mount layer at start):**
  `JM_WAN_MODE=1` raises JuiceFS `--max-uploads` 20 → 64;
  `JM_MAX_UPLOADS=<n>` overrides directly.

## Logs

- App: `~/Library/Logs/JuiceMount/juicemount.log` (JSON, 16 MB × 5
  rotation)
- JuiceFS daemon: `~/.juicefs/juicefs.log` (auto-tailed into the above
  with WARN aggregation)

## Repo layout

```
JuiceMount6/
├── README.md                  — public-facing overview
├── ROADMAP.md                 — phased status
├── ARCHITECTURE_juicemount.md — system architecture, data flows
├── MENU_BAR_APP.md            — popover features, shortcuts, troubleshooting
├── CHANGELOG.md               — release notes
├── credentials.md             — sensitive infra config (gitignored)
│
├── app/JuiceMount/            — Swift Package: menu-bar app
│   └── Sources/
│       ├── JuiceMountCore/    — C interop layer over libnfsd.h
│       └── JuiceMount/        — App.swift, UI, ServerController, NFSBridge
│
├── bridge/cbridge.go          — Go c-archive exports (Start/Stop/Stats/Pin/...)
├── cmd/jm5/                   — headless server CLI
├── cmd/juicemount/            — control client CLI (talks to the app via HTTP)
├── cmd/juicemount-manager/    — server-side Manager web UI binary
│
├── nfs/                       — NFS handler, read/write paths, fd pool,
│                                readahead, membuf, write spool + drainer
├── metadata/                  — SQLite store + Redis sync + FTS5 search
├── cache/                     — direct SSD cache reader (Priority-2 reads)
├── health/                    — FUSEManager, monitor loop, network watcher
│
├── internal/
│   ├── cache/pin/             — pin store + prefetcher + verify-and-repair
│   ├── jmlog/                 — structured JSON logging with rotation
│   ├── manager/               — JuiceMount Manager server implementation
│   ├── metrics/               — RPC counters, /metrics HTTP endpoint
│   ├── nfs/                   — vendored go-nfs fork (see NOTICE)
│   └── nle/                   — Premiere/Resolve/FCPX project parsers
│
├── server/                    — docker-compose stack + TrueNAS install docs
├── scripts/                   — build, install, uninstall, QA suite
├── test/                      — integration / e2e / workflow / benchmarks
└── docs/                      — engineering docs, QA procedure, state
```

## How a read happens

```
Premiere / Resolve / Finder reads /Volumes/<name>/Project/clip.mov
        ↓
NFS RPC → 127.0.0.1:11049 → nfs/server.go → handler.OpenFile
        ↓
[offline mode + un-pinned?  → fail fast in ms]
        ↓
cachedFile.ReadAt(buf, off):
        ├─ Priority 1: memBuf (small files: prproj, LUTs)
        ├─ Priority 2: cache.Reader (direct pread on JuiceFS chunks/ — bypasses FUSE)
        └─ Priority 3: fuseFD.ReadAt (FUSE → JuiceFS LRU → S3 backend on miss)
```

## How a write happens (spool enabled)

```
Premiere / Finder writes /Volumes/<name>/Project/render.mov
        ↓
NFS WRITE RPC → handler.OpenFile(O_CREATE) → spool.OpenWrite
        ↓
write bytes to a file on local SSD  ← streaming SHA-256
        ↓
Close → fsync → mark "ready" → ACK to Finder   (durable + fast; no wait on MinIO)
        ↓
[background] drainer: copy spool file → JuiceFS FUSE → rawstaging → MinIO
             re-verify SHA-256 at the FUSE hop, then delete the spool file
```

Reads of a just-written file are served from the spool until the drainer
copies it through (3-tier read lookup: spool → cache/readahead/memBuf →
FUSE). With the spool disabled, writes use the direct-to-FUSE path
unchanged. Full detail: `ARCHITECTURE_juicemount.md` § 15 and
`docs/ROADMAP/option-2-spool.md`.
