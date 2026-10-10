# JuiceMount server-side stack

The 0.5 server stack needs release image digests, persistent credentials,
storage paths, and a rendered Compose file before it can start. Use the
[TrueNAS install guide](INSTALL-TrueNAS.md) or follow the same render process
on a Docker host. Obtain the release image digests from its published manifest;
do not replace them with mutable tags.

## Production install (TrueNAS Scale)

Use **TrueNAS Apps → Discover → ⋮ → Install via YAML** with the rendered,
digest-pinned release compose. The setup and upgrade steps are in
[INSTALL-TrueNAS.md](INSTALL-TrueNAS.md).

The stack:

| Service               | What it does                                  | Port  |
|-----------------------|-----------------------------------------------|-------|
| `redis`               | JuiceFS metadata authority (AOF persistence), internal only | — |
| `respproxy`           | Authenticated Redis gateway for named principals | 30179 |
| `minio`               | S3-compatible object store for JuiceFS chunks | 30151 (S3), 30152 (console) |
| `minio-keys`          | Provisions separate client and GC object keys | — |
| `juicefs-init`        | One-shot pre-flight + first-time volume format | —    |
| `juicefs`             | Live FUSE mount + loopback-only WebDAV | 127.0.0.1:30180 |
| `juicemount-manager`  | Control-plane web UI (migrations, trash, backups, maintenance, settings) | 30190 |
| `juicefarm`           | NAS CPU derivative worker | — |

The Mac JuiceMount.app uses the server through:
- an enrolled Mac principal at the RESP proxy on port 30179;
- an S3 endpoint at port 30151 with a provisioned client object key;
- the Manager on port 30190 for enrollment and control.

The Manager delivers the Mac's credentials during enrollment. Do not put a
password in a volume URL or assume the old unauthenticated Redis URL works.
Keep these endpoints on a trusted LAN or private route.

## Plain Docker host (Ubuntu / Synology / laptop)

Use the canonical `docker-compose.yml`, adjusting its TrueNAS-flavored bind
paths for your host. Copy `release.env.example` outside the repository, fill
every required credential and digest from the accepted release manifest, and
keep that file private. Render and inspect it before starting containers:

```sh
docker compose --env-file /secure/path/juicemount-release.env \
  -f server/docker-compose.yml config > /secure/path/juicemount-rendered.yml
docker compose --env-file /secure/path/juicemount-release.env \
  -f server/docker-compose.yml up -d
docker compose --env-file /secure/path/juicemount-release.env \
  -f server/docker-compose.yml ps
docker compose --env-file /secure/path/juicemount-release.env \
  -f server/docker-compose.yml logs juicefs-init
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

Data lives in the host paths set in `volumes:`, but a full-stack
`docker compose up -d` can restart Redis, MinIO, and JuiceFS and interrupt
live Mac clients. For a canonical Compose project owned by this checkout,
use the service-scoped [Compose rollout procedure](COMPOSE-ROLLOUT.md).
It verifies the previous configuration and digest-pinned images, updates only
Manager and farm, and restores previous images on failed verification.

TrueNAS Apps-generated Compose projects are **not** covered by that script.
Do not run the standalone `update-server.sh` against Compose-owned services
or use a full-stack reapply as a substitute for a rehearsed TrueNAS rollout.

## Security notes

- **The RESP proxy, not Redis, owns port 30179.** It authenticates named
  principals; keep the port on a trusted LAN or private route because RESP
  authentication is plaintext without a separate TLS layer.
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
- **The default 0.5 compose leaves embedded Link off.** Keep Redis, MinIO,
  WebDAV, and Manager on a trusted LAN or private route. The full Link route,
  reconnect, and revoke workflow has not passed acceptance.

## Troubleshooting

| Symptom                               | Likely cause / fix |
|---------------------------------------|--------------------|
| `juicefs-init` exited 2               | MinIO unreachable from inside the container network |
| `juicefs-init` exited 4               | MinIO credentials empty, whitespace, or placeholder |
| `juicefs-init` exited 5               | `JM_BUCKET_URL` missing `http://` |
| `juicefs-init` exited 6               | `juicefs format` errored — read the log for the JuiceFS-side reason |
| Manager refuses to start with an admin-key error | Restore the prior durable `JM_ADMIN_KEY`; the persisted value is absent, short, or still a placeholder. |
| Manager copy reports "0 files / 0 B" | Check the job and accepted image digest; do not switch to a mutable `latest` tag. |
| Mac can't open copied files           | Source had restrictive perms; either un-tick Preserve permissions before migrating, or `chmod -R u+rwX,g+rwX,o+rX` on the destination |

Use `docker compose logs <service>` for the full output of any
container's startup. Each pre-flight log line starts with
`[precheck-N]` for easy grepping.
