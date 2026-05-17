# JuiceMount server-side stack

`docker compose up` boots a working JuiceMount backend on any host
with Docker installed: Ubuntu, Synology DSM 7+ via Container Manager,
TrueNAS SCALE Apps, any laptop with Docker Desktop. From `git clone`
to a mountable backend in under 10 minutes.

This directory implements tier 3 of the JuiceMount roadmap. See
`docs/ROADMAP/tier-3-server-packaging.md` for the full spec.

## What's in this iteration (tier-3 iter 3)

The full headless stack with TLS termination and admin-key auth.
After this iteration, JuiceMount serves an NFS export from a Linux
container, and admin endpoints are behind https.

| Service             | What it does                                | Port  |
|---------------------|---------------------------------------------|-------|
| `minio`             | S3-compatible object store                  | 9000  |
| `redis`             | JuiceFS metadata authority                  | 6379  |
| `juicefs-init`      | One-shot — formats the JuiceFS volume       | —     |
| `juicemount-server` | NFS export + admin API                      | 2049, 11050 |
| `caddy`             | TLS termination + admin-key auth gate       | 443, 80 |

MinIO web console at `https://<host>/` (proxied by Caddy; log in
with `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` from your `.env`).

**Mount the volume from any NFS client:**

```sh
# macOS
sudo mkdir -p /Volumes/zpool
sudo mount -t nfs -o vers=3,soft,timeo=300,resvport <host>:/ /Volumes/zpool

# Linux
sudo mkdir -p /mnt/zpool
sudo mount -t nfs -o vers=3,soft,timeo=300 <host>:/ /mnt/zpool
```

The JuiceMount.app on macOS can also drive this container as its
backend by pointing its profile at `<host>:11050` for admin and using
the same `<host>:2049` for the NFS path (in the Custom mount path
preference).

**Not yet in this iteration** (tier-3 iter 3+):

- Caddy reverse-proxy with TLS + admin-key auth (iter 3)
- `juicemount doctor` healthcheck command (iter 4)
- Backup job (`mc mirror` to remote bucket) (iter 5)
- Admin UI behind the Caddy layer (iter 6)

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

## What this stack does NOT include yet

- `juicemount doctor` healthcheck CLI (iter 4)
- Scheduled backups (`mc mirror` to remote bucket, iter 5)
- Admin UI (iter 6)
- Notarized / signed container images (iter 7)

## TLS + admin-key auth (iter 3)

Caddy fronts MinIO console + the JuiceMount admin API behind a
single TLS port. Generate an admin key and add to `.env`:

```sh
echo "ADMIN_KEY=$(openssl rand -hex 32)" >> .env
```

After `docker compose up -d`:

- `https://<host>/` → MinIO web console (self-signed cert by default;
  set `JM_HOSTNAME` to a real DNS name and swap `tls internal` →
  `tls your@email` in the Caddyfile to enable Let's Encrypt).
- `https://<host>/api/*` → JuiceMount admin endpoints, but requires:
  ```
  X-JuiceMount-Admin-Key: <your ADMIN_KEY>
  ```
  Example:
  ```sh
  curl -sk -H "X-JuiceMount-Admin-Key: $(grep ADMIN_KEY .env | cut -d= -f2)" \
       https://localhost/api/health
  ```

NFS (2049) is exposed directly, not through Caddy. NFS is a stateful
TCP protocol and HTTP-layer routing doesn't help. On-LAN deployments
should put NFS behind a host firewall; over-internet exposure would
need stunnel or wireguard wrapping (future iteration).

## Security notes for production deployments

- **Redis port (6379) is exposed.** Set `REDIS_PORT=127.0.0.1:6379`
  in `.env` if only the local host needs metadata access. Without
  TLS+auth, anyone who can reach the port can read/write/delete
  the entire JuiceFS metadata store — equivalent to total volume
  loss even if the MinIO data survives. iter 3 will put Redis
  behind Caddy with admin-key auth.
- **MinIO console (9001) is HTTP-only.** Don't expose to public
  IPs without first putting it behind Caddy+TLS (iter 3).
- **`MINIO_ROOT_PASSWORD` is the master key.** Anyone with this
  password can read every byte in the volume. Rotate before going
  to production; `juicefs format` writes the secret-key into the
  Redis metadata, so re-format is the only way to rotate cleanly
  today.
