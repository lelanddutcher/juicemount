# JuiceMount server-side stack

`docker compose up` boots a working JuiceMount backend on any host
with Docker installed: TrueNAS Scale Apps, Synology DSM 7+ via
Container Manager, Ubuntu, any laptop with Docker Desktop.

## Production install (TrueNAS Scale)

The proven path is **TrueNAS Apps → Discover → ⋮ menu → Install via
YAML**, pasting the production-hardened compose. Full step-by-step
in [INSTALL-TrueNAS.md](INSTALL-TrueNAS.md).

The stack:

| Service               | What it does                                  | Port  |
|-----------------------|-----------------------------------------------|-------|
| `redis`               | JuiceFS metadata authority (AOF persistence)  | 30179 |
| `minio`               | S3-compatible object store for JuiceFS chunks | 30151 (S3), 30152 (console) |
| `juicefs-init`        | One-shot pre-flight + first-time volume format | —    |
| `juicefs`             | Live FUSE mount + WebDAV (browse / smoke test) | 30180 |
| `juicemount-manager`  | Control-plane web UI (migrations, trash, backups, maintenance, settings) | 30190 |

The Mac JuiceMount.app drives this stack via:
- Redis URL: `redis://<host>:30179/1`
- S3 Endpoint Override: `http://<host>:30151/zpool`

(The MinIO + Redis credentials don't need to live on the Mac side —
they're embedded in the JuiceFS volume's format header. The override
fields are documented in the app's Preferences → Connection pane.)

## Plain Docker host (Ubuntu / Synology / laptop)

Same compose file works. Edit the bind-mount paths at the top of
`docker-compose.yml` (the defaults are TrueNAS-flavored `/mnt/zSSD/...`)
to wherever you want the data to live. Generate a Manager key, export it in
the deployment shell, then render the compose before changing containers:

```sh
cd server
export JM_ADMIN_KEY="$(openssl rand -hex 32)"
docker compose config --quiet
docker compose up -d
docker compose ps          # all services healthy
docker compose logs juicefs-init   # confirms first-time format
```

## Migrating existing data

The included `juicemount-manager` service exposes a web UI at port
30190 with a Migrations tab that copies data from any bind-mounted
source directory into the JuiceFS volume. (SLICE 0 renamed the
service from `juicemount-migrator` — the legacy image tag still
publishes for one release as a compat alias and `/migrator/*` URLs
301-redirect to `/manager/*`.)

1. Bind-mount each existing dataset read-only at `/sources/<name>`
   in the `juicemount-manager` service.
2. Open `http://<host>:30190/` in a browser (Migrations tab is the
   default route).
3. Browse the source root, pick a folder, hit Start.

Features:

- **Live progress bar** with real percentage (driven by juicefs's
  Prometheus metrics, scraped every 2s).
- **Smart default destination** that strips the source-root bind-mount
  name so `/jfs/oldzpool/...` doesn't clutter the structure.
- **Destination preview** showing the resolved URLs + 3 example file
  destinations before you commit.
- **Junk-filter** for `.DS_Store`, `._*`, `Thumbs.db`, `.sync.ffs_db`,
  Spotlight/Trash metadata.
- **Permissions OFF by default** — destination files land with sensible
  modes the Mac user can open. Tick the option only if you need
  archival fidelity (matters for POSIX-to-POSIX with uid-preserving
  intent).
- Jobs queue and run sequentially.

## Persistence & upgrade

Data lives in the host paths you set in the `volumes:` lines. To
upgrade:

```sh
cd server
git pull
docker compose pull       # pull newer service images
docker compose config --quiet  # fails before recreation if JM_ADMIN_KEY is absent
docker compose up -d      # restart with no data loss
```

The bind-mount strategy means container churn never touches your data.

## Reset (DESTROYS DATA)

```sh
docker compose down
sudo rm -rf /mnt/.../juicemount/{redis,bucket,cache}/*
docker compose up -d
```

Take a backup first.

## Security notes

- **Redis port (30179) is exposed on the LAN.** Anyone who can reach
  the host on that port can read/write the entire JuiceFS metadata —
  equivalent to total volume loss. Confine via host firewall (TrueNAS
  Networks → firewall rules) if untrusted clients are on the same LAN.
- **MinIO console (30152) is HTTP-only.** Don't expose to public IPs.
- **`MINIO_ROOT_PASSWORD` is the master key.** Generate via
  `openssl rand -base64 24`. Anyone with this password can read every
  byte in the volume.
- **Manager admin key (`JM_ADMIN_KEY`)** gates write access to the
  manager's HTTP API. The production compose requires a 32+ character value
  in the deployment environment and refuses to render without it. Generate
  one via `openssl rand -hex 32`; preserve the same value across upgrades and
  never commit it to the repository. The binary still permits an empty key for
  explicit local-development runs while JuiceMount Link is disabled.

## Troubleshooting

| Symptom                               | Likely cause / fix |
|---------------------------------------|--------------------|
| `juicefs-init` exited 2               | MinIO unreachable from inside the container network |
| `juicefs-init` exited 4               | MinIO credentials empty, whitespace, or placeholder |
| `juicefs-init` exited 5               | `JM_BUCKET_URL` missing `http://` |
| `juicefs-init` exited 6               | `juicefs format` errored — read the log for the JuiceFS-side reason |
| Manager refuses to start with an admin-key error | Restore the prior durable `JM_ADMIN_KEY`; the persisted value is absent, short, or still a placeholder. |
| Manager copy reports "0 files / 0 B" | Stale image — pull `ghcr.io/lelanddutcher/juicemount-manager:latest` and redeploy |
| Mac can't open copied files           | Source had restrictive perms; either un-tick Preserve permissions before migrating, or `chmod -R u+rwX,g+rwX,o+rX` on the destination |

Use `docker compose logs <service>` for the full output of any
container's startup. Each pre-flight log line starts with
`[precheck-N]` for easy grepping.
