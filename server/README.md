# JuiceMount server-side stack

`docker compose up` boots a working JuiceMount backend on any host
with Docker installed: Ubuntu, Synology DSM 7+ via Container Manager,
TrueNAS SCALE Apps, any laptop with Docker Desktop. From `git clone`
to a mountable backend in under 10 minutes.

This directory implements tier 3 of the JuiceMount roadmap. See
`docs/ROADMAP/tier-3-server-packaging.md` for the full spec.

## What's in this iteration (tier-3 iter 1)

The foundation: the three services every JuiceMount deployment needs.

| Service        | What it does                                    | Port  |
|----------------|-------------------------------------------------|-------|
| `minio`        | S3-compatible object store                      | 9000  |
| `redis`        | JuiceFS metadata authority                      | 6379  |
| `juicefs-init` | One-shot — formats the JuiceFS volume on first run | —  |

A web console for MinIO is on port 9001 — point a browser at
`http://<host>:9001` and log in with `MINIO_ROOT_USER` /
`MINIO_ROOT_PASSWORD` from your `.env`.

**Not yet in this iteration** (tier-3 iter 2+):

- `juicemount-server` container exporting NFS on 2049
- Caddy reverse-proxy with TLS
- `juicemount doctor` healthcheck command
- Backup job (`mc mirror` to remote bucket)
- Admin UI behind the Caddy layer

For now, your macOS JuiceMount.app talks directly to the MinIO + Redis
above, doing the FUSE / NFS-loopback work locally.

## Quick start

```sh
cd server
cp .env.example .env
$EDITOR .env                          # set MINIO_ROOT_PASSWORD (required)
docker compose up -d
docker compose ps                     # minio + redis should be healthy
docker compose logs juicefs-init      # confirms first-time format
```

Point JuiceMount.app at:
- Redis URL: `redis://<host>:6379/1`
- Bucket URL: `http://<host>:9000/zpool`
- Access key: value of `MINIO_ROOT_USER` (default `juicemount`)
- Secret key: value of `MINIO_ROOT_PASSWORD`

## Persistence & upgrade

Data lives in the host paths you set via `MINIO_DATA_DIR` and
`REDIS_DATA_DIR` (defaults to `./data/{minio,redis}` next to the
compose file). To upgrade:

```sh
cd server
git pull
docker compose pull       # pull newer service images
docker compose up -d      # restart with no data loss
```

The bind-mount strategy means container churn never touches your data.

## Troubleshooting

| Symptom                               | Likely cause / fix |
|---------------------------------------|--------------------|
| `juicefs-init` exited non-zero        | Check `.env` — MINIO_ROOT_PASSWORD probably empty |
| Redis container unhealthy             | Port 6379 already in use; set REDIS_PORT in `.env` |
| MinIO web console unreachable         | Port 9001 collides; set MINIO_CONSOLE_PORT |
| Can't connect from JuiceMount.app     | Firewall blocking 6379/9000; or wrong host in app preferences |

Use `docker compose logs <service>` for the full output of any
container's startup.

## Resetting the stack (DESTROYS DATA)

If you need to start fresh — wipes the MinIO bucket and the JuiceFS
metadata:

```sh
docker compose down
rm -rf data/   # or whatever you set MINIO_DATA_DIR / REDIS_DATA_DIR to
docker compose up -d
```

This is destructive. Take a backup first if there's anything you want
to keep — there's no in-place migration story between formats.

## What this stack does NOT include

- TLS termination (defer to tier-3 iter 2's Caddy)
- Authentication beyond MinIO's root password (defer to admin-key
  middleware in iter 2)
- The NFS export (currently your macOS JuiceMount.app provides this
  locally — the server-side `juicemount-server` container lands in
  iter 2)
- Notarized release / signed images (defer to tier-3 iter 5 polish)
