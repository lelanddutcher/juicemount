# Tier 3 — Server-side packaging

Goal: `git clone <repo> && cd server && docker compose up` boots a
working JuiceMount backend on any Ubuntu / Synology / TrueNAS box
in under 10 minutes from a fresh OS.

The self-host commitment from `VISION.md` only becomes real when this
ships. Until then, JuiceMount is "interesting OSS code" not "thing
you can deploy this weekend."

## Acceptance tests

| # | Test | Pass criterion |
|---|---|---|
| 3.1 | Cold-deploy on Ubuntu Server 24.04 LTS | From `git clone` to a mountable backend: <10 min, no manual config steps |
| 3.2 | Cold-deploy on Synology DSM 7.x via Container Manager | Same end state; docs lay out the Synology-specific paths |
| 3.3 | Configuration via single `.env` | One file controls all knobs: data path, ports, admin key, retention |
| 3.4 | Healthchecks | `docker compose ps` shows all services healthy; failed service is obvious from `docker logs` |
| 3.5 | `juicemount doctor` | Single command inside the container validates the full stack and prints findings |
| 3.6 | Backup job | Scheduled `mc mirror` to a remote bucket; restorable to a fresh box |
| 3.7 | Upgrade path | `git pull && docker compose up -d` produces a working upgraded stack; data path preserved |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                  server/docker-compose.yml                      │
│                                                                 │
│  ┌────────────────┐  ┌────────────────┐  ┌──────────────────┐ │
│  │  minio         │  │  redis         │  │  juicefs-init    │ │
│  │  : object      │  │  : metadata    │  │  : one-shot      │ │
│  │  : 9000/9001   │  │  : 6379        │  │  : formats vol   │ │
│  │  : data:/data  │  │  : data:/data  │  │  : exits clean   │ │
│  └────────────────┘  └────────────────┘  └──────────────────┘ │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  juicemount-server  (new container, current cmd/jm5)    │   │
│  │  : NFS export on 2049                                   │   │
│  │  : admin API on 11050                                   │   │
│  │  : healthcheck endpoint /healthz                        │   │
│  │  : depends_on: minio, redis, juicefs-init               │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌────────────────┐  ┌────────────────┐                       │
│  │  caddy         │  │  backup        │                       │
│  │  : 443         │  │  : cron sidecar│                       │
│  │  : TLS+admin UI│  │  : mc mirror   │                       │
│  └────────────────┘  └────────────────┘                       │
└─────────────────────────────────────────────────────────────────┘

  Bind mounts (from .env):
    DATA_PATH   → minio data, juicefs blocks
    META_PATH   → redis AOF + RDB
    CERTS_PATH  → caddy state
    BACKUP_PATH → outbound backup target
```

## Concrete deliverables

### 3.A — `server/docker-compose.yml` and `.env.example`

Single canonical file. No profiles, no per-platform variants. The
same compose file boots on any Linux box.

`.env.example` covers:

```bash
# Where to store object data (MinIO + JuiceFS blocks).
DATA_PATH=/srv/juicemount/data

# Where to store metadata (Redis AOF + RDB).
META_PATH=/srv/juicemount/meta

# Admin key — single credential for both the NFS server and the
# admin UI. Rotate via re-deploy.
JM_ADMIN_KEY=change-me-please

# Optional public hostname for TLS via Caddy + Let's Encrypt.
# Leave empty for self-signed.
JM_HOSTNAME=

# Backup target. Empty disables backup. Format: s3://bucket/prefix
# or absolute path for local backup.
JM_BACKUP_TARGET=
```

### 3.B — `juicemount doctor` command

Single binary, in `cmd/jmdoctor`. Run inside the container:

```
$ docker compose exec juicemount-server jmdoctor
[OK]   MinIO reachable at minio:9000
[OK]   Redis reachable at redis:6379, 47 MB allocated
[OK]   JuiceFS daemon healthy, mount at /mnt/jfs OK
[OK]   NFS server listening on 0.0.0.0:2049
[WARN] No TLS configured (JM_HOSTNAME empty). Use a reverse proxy
       for remote access.
[OK]   Backup job last ran 2 h 14 m ago, succeeded.
```

Exits non-zero if any [FAIL] or [ERROR]. Suitable for cron-driven
health gates.

### 3.C — Backup sidecar

A small container that runs `mc mirror` (MinIO's CLI) on a schedule.
Configurable via `JM_BACKUP_TARGET`. Emits Prometheus-friendly
metrics on a local Unix socket so an external monitoring stack can
ingest.

Includes `juicefs dump` periodic for the metadata side so a
restore-from-backup is possible without Redis state.

### 3.D — Admin UI behind Caddy

Lightweight HTML/JS hosted by the `juicemount-server` container,
served by Caddy. Read-only by default. Shows:

- Connected clients (filtered to LAN IPs)
- Bytes in/out per client
- Top files by access count (last hour)
- Cache fill, RPC latency p50/p99/max
- Recent errors

Auth via `JM_ADMIN_KEY` as a Bearer token. No accounts, no IAM. This
is the "admin sees what's happening" surface, not a SaaS dashboard.

### 3.E — Signed installer for non-Docker users

For people who don't run Docker on their NAS, a single signed binary
that includes the same orchestration logic. Bundled MinIO + Redis +
JuiceFS via internal supervisor (single process, multiple
goroutines). Less recommended path but real users will want it.

## Anti-patterns

- **No Kubernetes.** Target audience runs `docker compose`. Adding
  K8s manifests bifurcates the docs and grows the testing surface
  without serving the target user.
- **No managed cloud offering.** Per the non-negotiables in
  `VISION.md`. Even if a community member proposes one, it lives in
  a separate repo.
- **No auto-update of the docker-compose file.** Users review and
  pull on their schedule. Watchtower-style auto-update breaks
  production deploys.
- **No OIDC/SSO in the admin UI.** Shared admin key is the
  credential. Anything else is enterprise scope creep.
- **No phone-home telemetry.** Per `VISION.md`. The admin UI is
  local-only.

## Reference implementations to study

- **Tailscale's headscale** (`juanfont/headscale`) — closest
  open-source analog to our positioning. Single binary, docker
  deploy, no managed offering. Read their compose file.
- **Mastodon's docker setup** — multi-service, healthcheck patterns,
  clean .env, decent docs.
- **Caddy's reverse-proxy stories** — for the TLS termination piece.

## Dependencies

- Independent of tier 1 advancement; can be developed in parallel.
- Tier 2's wizard step "paste server URL" should accept the same
  share-string format the server emits on first boot, so the loop
  is end-to-end testable.
- Tier 4's bandwidth probe needs an endpoint to probe — `juicemount-
  server` should expose `/healthz` reachable from clients.

## Iteration plan

Tier-3 work is mostly fresh creation under a new `server/` directory.
Each iteration delivers something deployable end-to-end so we don't
ship half a stack.

| # | Slice | Hours | Files |
|---|---|---|---|
| 3.A.1 | Minimal `server/docker-compose.yml` — MinIO + Redis + JuiceFS-init bind-mounted to a single `DATA_PATH` from `.env` | 4 | `server/docker-compose.yml`, `server/.env.example`, `server/README.md` |
| 3.A.2 | Add the `juicemount-server` container — runs `cmd/juicemount` in -daemon-mode, exposes NFS on 2049 + admin API on 11050 | 5 | new `cmd/juicemount-server/main.go`, Dockerfile, compose entry |
| 3.A.3 | Healthchecks on every service (compose `healthcheck:` blocks) — fail fast on bad config | 2 | docker-compose.yml |
| 3.A.4 | Caddy front + admin-key auth header on every admin route | 3 | new `server/Caddyfile`, admin-key middleware in juicemount-server |
| 3.B.1 | `cmd/jmdoctor` — single binary, prints OK/WARN/FAIL per component | 4 | new `cmd/jmdoctor/main.go` |
| 3.B.2 | Wire jmdoctor into `docker compose exec juicemount-server jmdoctor` flow + cron-friendly exit code | 1 | Dockerfile + docs |
| 3.C.1 | Backup sidecar: `mc mirror` to `JM_BACKUP_TARGET` on a cron schedule | 4 | new `server/backup/` dir, `backup-sidecar/Dockerfile` |
| 3.C.2 | `juicefs dump` periodic for metadata-side restore | 2 | extend the sidecar |
| 3.D.1 | Admin UI HTML/JS (static — connected clients, bytes I/O, cache fill, recent errors) | 6 | `server/admin-ui/` static assets, served by juicemount-server |
| 3.D.2 | Admin UI talks to admin API via Bearer auth — works behind Caddy TLS | 2 | wire it through |
| 3.E.1 | Signed single binary for non-Docker installs (Linux, supervisor pattern) | 6 | new `cmd/juicemount-allinone/main.go` |
| 3.E.2 | Cross-compile + release pipeline (GoReleaser config) | 3 | `.goreleaser.yml` |

Total: ~42 hours = ~5 working days.

**First-iteration target**: cold-deploy on a fresh Ubuntu 24.04 VM
from `git clone` to a Mac-mountable backend in under 10 minutes. If
that works, every subsequent iteration builds on a proven foundation.

## Signals to watch

| Item | Signal proving it works |
|---|---|
| 3.A.1-2 | `docker compose up` exits with all services healthy within 30s on fresh box |
| 3.A.3 | `docker compose ps` shows all services in `healthy` state; introducing bad config in `.env` produces a clear failure not a hang |
| 3.A.4 | Curl to admin route without bearer → 401; with valid bearer → 200 |
| 3.B | `docker compose exec ... jmdoctor` exits 0 in healthy state; exits non-zero when any component is down + names the failure |
| 3.C | Backup target file appears at the configured `JM_BACKUP_TARGET` after a scheduled run; manual `juicefs load` from dump produces an identical metadata state |
| 3.D | Admin UI loads at the `JM_HOSTNAME` URL behind TLS, shows current per-client byte counters |
| 3.E | Single binary executes on a fresh Linux box with only `--config` flag, produces same component states as the docker stack |
