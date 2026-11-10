# Install the JuiceMount server (TrueNAS SCALE or any Docker host)

One command installs the JuiceMount 0.5 server stack. You type no password,
key, image digest or path. The command is `server/scripts/juicemount-setup.sh`
in this repository.

## Before you start

- **TrueNAS SCALE 24.10 or later** (Electric Eel, Fangtooth, Goldeye) with
  Apps set up: **Apps → Configuration → Choose Pool**. This starts Docker.
  Or **any Linux host** with Docker Engine and Docker Compose v2
  (`docker compose version` answers).
- These host ports are free on every address: TCP 30179 (Redis proxy), 30151
  and 30152 (object store), 30190 (Manager) and 30180 (WebDAV, published on
  127.0.0.1 only). With Link also TCP 30193. The command checks each
  published port, TCP and UDP, with `ss` when the host has it. Link's node
  container also binds UDP 41641 on the host (host networking, not a
  published port, so not checked); if another program holds it, see
  `JM_NET_NAS_UDP_PORT` in `server/docker-compose.link.yml`.
- Run it as root (on TrueNAS: the System → Shell, through `sudo`). Root is
  needed for datasets and for the private install folder. On a plain Docker
  host, a user with Docker access may install into a folder it can write
  (`--dir`).
- The host can reach `github.com`, `raw.githubusercontent.com`, `ghcr.io`
  and the registries named in `server/docker-compose.yml`.

**Known limit (release).** The command installs a release made after it was
added to this repository. The latest release today, 0.5.0, predates it, so
the command refuses it with a message and changes nothing. **The setup command
works from the next release on.** For 0.5.0, use the manual install in the
appendix.

**Known limit (object store image).** The MinIO image pinned in
`server/docker-compose.yml` cannot be downloaded anonymously any more: Docker
Hub and Quay answer 401. Until a release pins a replacement image, the
setup command stops at "could not download an image" and starts nothing. It
keeps the generated settings, so running it again later (with
`--release vX.Y.Z` for the release that fixes it) continues from there.
An operator who already has a qualified image can apply it with
`--override FILE` (a compose file that sets the `image:` of `minio`,
`minio-keys` and `minio-keyd`, pinned by `@sha256:` digest).

## Install

On TrueNAS, open **System → Shell**. On another host, open a terminal. Paste:

```sh
curl -fsSL https://raw.githubusercontent.com/lelanddutcher/juicemount/main/server/scripts/juicemount-setup.sh | sudo bash
```

The command shows a plan and asks once: type `y`. A first install takes a
few minutes, mostly image downloads. At the end it prints two lines:

```
Manager URL:  http://192.168.1.20:30190
Sign-in key:  <32 letters and digits>
```

Keep the sign-in key. You need it to open the Manager and to approve each Mac.
It is also stored in the settings file (below); running the command again
prints it again.

### What the command does

1. **Finds the storage.**
   - On TrueNAS it creates the dataset `<pool>/juicemount` with one child
     dataset per data folder, on the pool with the most free space (never the
     boot pool). `--pool NAME` chooses the pool. The datasets get ACL type
     POSIX and ACL mode Discard (the "Generic" preset), so an SMB parent with
     NFSv4 ACLs passes no access entries down. An existing dataset with NFSv4
     ACLs is refused. A locked or unmounted dataset is refused, not taken for
     an empty one.
   - On another host it uses the folder `/srv/juicemount`. `--dir PATH`
     chooses another folder (on TrueNAS too: then it creates no dataset).
2. **Reads the release** (the latest one, or `--release vX.Y.Z`). It
   downloads the release's `release-manifest.json` and `server-images.env`.
   Their hashes must match the release's `SHA256SUMS`, the manifest's version
   must match `--release`, and each image digest must be the one the manifest
   gives for the same image. These checks catch a damaged or mismatched
   download; they do not prove who made the release (see "What the command
   trusts").
3. **Takes the compose file** `server/docker-compose.yml` and the files it
   mounts (`redis.conf`, `redis.acl`, `minio/`) from the commit the release
   manifest names, so the files and the images always belong to the same
   release. It replaces the sample host paths with the chosen storage. Every
   other image (Redis, the object store, JuiceFS) comes from that compose
   file.
4. **Checks everything before it writes anything:** the storage (an
   unmounted or locked dataset, or a folder with content where a new dataset
   would go, is refused), other JuiceMount installs on the host (a TrueNAS
   app such as `ix-juicemount`, a Compose project or loose containers), the
   rendered compose (`docker compose config` must render: every required value
   has a `${VAR:?}` guard; every image must be pinned by `@sha256:` digest;
   every mounted path must exist) and free ports.
5. **Shows the plan and asks once.** `--yes` skips the question (needed when
   no terminal is attached). `--dry-run` runs every check, shows the plan and
   changes nothing.
6. **Generates every secret** from `/dev/urandom`: the Manager sign-in key,
   the MinIO root password, the six Redis proxy passwords and the two object
   keys. It writes them, with the digests and the storage size, to the
   settings file. Values reach Docker Compose only from that file: the command
   clears its environment, except what reaches the Docker daemon.
7. **Pulls the images and starts the stack.** If the one-shot `juicefs-init`
   fails, the command prints its exit code and meaning (table below).
8. **Waits until the Manager answers** with the new key, then prints the two
   lines.

The object store is SILO (`pgsty/silo`), a maintained community fork of
MinIO with the same S3 API, settings and on-disk format. Releases up to 0.5.0
pinned `minio/minio`
(`sha256:ed9be66eb5f2636c18289c34c3b725ddf57815f2777c77b5938543b78a44f144`),
which can no longer be pulled anonymously: Docker Hub and Quay return 401.
An existing stack keeps its bucket data when the `minio` service switches to
SILO.

### What the command trusts

The command runs what the GitHub repository `lelanddutcher/juicemount` holds:
the script itself from its `main` branch, and the release's assets
(`release-manifest.json`, `server-images.env`, `SHA256SUMS`) with the files of
the commit the release names. `SHA256SUMS` is not signed and arrives over the
same HTTPS connection as the files it lists, so it detects a damaged download,
not a tampered one. Anyone who can write to the repository's `main` branch or
its releases decides what is installed. The images are pulled by `@sha256:`
digest, so a registry cannot swap them, but the digests come from that same
release. Releases are not signed yet.

### Where everything is

`<data>` is `/mnt/<pool>/juicemount` on TrueNAS, `/srv/juicemount` otherwise.
`<data>` itself is mode `0700`: no other user of the host can enter it. The
command sets that when it creates `<data>`, and sets it again on a re-run if
the mode was loosened. The containers reach their folders through bind mounts
and are not affected.

| Path | Holds | Mode |
|---|---|---|
| `<data>/compose/` | Compose project folder: `docker-compose.yml`, `redis.conf`, `redis.acl`, `minio/`, optional `docker-compose.override.yml` | `0700` |
| `<data>/compose/overlays/` | Extra compose files you add (such as Link); always applied, in name order | |
| `<data>/compose/jm-compose` | Runs `docker compose` for this install with exactly the files and settings setup uses | |
| `<data>/compose/.env` | **Every secret of this server**, the image digests, the storage size, and the release, commit and Compose version used at install | `0600` |
| `<data>/bucket` | Object store: every byte of your files | |
| `<data>/redis` | JuiceFS metadata (Redis AOF/RDB) | |
| `<data>/cache` | JuiceFS chunk cache | |
| `<data>/manager-state`, `respproxy`, `objkeys`, `headscale`, `juicefarm-state` | Manager state, proxy principals, per-Mac key exchange, Link state, farm index | |
| `<data>/import` | Empty, mounted read-only in the Manager at `/sources/import` | |

**Back up `<data>/compose/.env` with the data.** Without it the data cannot
be opened: the command never generates new secrets for existing data.

On TrueNAS, exclude `<pool>/juicemount/bucket` from recursive periodic
snapshots. JuiceFS already keeps file and trash history in its metadata. ZFS
snapshots of the object bucket keep deleted blocks after garbage collection,
which can fill the pool without any visible JuiceFS usage. Back up JuiceFS
with a coordinated metadata-and-object procedure, not an isolated bucket
snapshot.

The Compose project is named `juicemount` (`--project NAME` changes it). It
runs as a plain Docker Compose project, not as a TrueNAS catalog app: the
TrueNAS Apps page may list it as an external app. To see it, or to run any
other `docker compose` command for it, use the wrapper:

```sh
sudo <data>/compose/jm-compose ps
sudo <data>/compose/jm-compose logs juicemount-manager
```

### Running it again

Running the same command again is safe:

- It never changes a secret, and never changes or deletes data.
- On a started install it changes no file and recreates no service. It makes
  `<data>` private (0700) again if its mode was loosened. It runs
  `docker compose up -d`, which starts any stopped service and runs the
  one-shot services again (`minio-keys` and `juicefs-init`; both change
  nothing when everything is already set up). It waits for the Manager and
  prints the same two lines. It always applies the files in
  `<data>/compose/overlays/`.
- It never starts a stopped Redis that would replay its append-only file
  under the 0.5.0 ACL. 0.5.0's `server/redis.acl` made Redis drop every
  transaction when it replayed that file at a start, so each restart lost
  recent metadata. If `<data>/compose/redis.acl`, or the ACL a stopped Redis
  was last started with, still has the 0.5.0 line, the command refuses and
  prints the steps from CHANGELOG.md (0.5.1, "Upgrading the Redis proxy and
  Manager"). A running Redis is left alone, with a warning. The command never
  replaces that file on a started install. A new install refuses a release
  whose `redis.acl` has the 0.5.0 line.
- It refuses, and changes nothing, when:
  - a running service does not match what the install's files would start.
    Setup never recreates a started service. There are two possible causes:
    the files changed after the services started (an overlay applied by hand
    but not kept in `overlays/`, or an edited `.env` or compose file), or
    Docker Compose was upgraded and computes its configuration hash
    differently. The message names the Compose version recorded at install
    when it differs. Check whether a file in `<data>/compose` (`overlays/`,
    `.env`, `docker-compose*.yml`) changed since the services started
    (`docker compose up --dry-run` does not help: it reports every service
    with `pull_policy: always` as changed). If the files are as intended,
    `<data>/compose/jm-compose up -d` recreates those services from them and
    keeps the data. If Redis is among them, do not run a bare `up -d`:
    recreate the others with `up -d --no-deps <services>`, and restart Redis
    only in the order of CHANGELOG.md (0.5.1, "Upgrading the Redis proxy and
    Manager");
  - a started install is given options other than the ones it was installed
    with (`--source`, `--images-env`, `--override`, `--import`, or a
    `--release` other than the one it runs). Run it with no options, or with
    exactly the same ones;
  - the Compose project name already runs from another folder: setup never
    takes over another install;
  - a data folder already holds data but `.env` is missing, or `.env` lacks a
    secret.
- It finds the install through the running Compose project or, on TrueNAS,
  through `<pool>/juicemount`.
- An install that has never started (no containers, empty data folders, for
  example after an image could not be downloaded) takes new options: another
  `--release` (new compose files and digests), `--override` or `--import`.
  Its secrets stay as they are.

**Upgrading a setup-made install is not supported yet.** The command never
upgrades a started install. Do not use `server/scripts/rollout-compose.sh` or
`update-server.sh` on it: they expect a git checkout and would recreate
services from the unrendered compose, with its sample paths. Keep the install
on its release until an upgrade path for setup-made installs exists.

### All options

| Option | Effect |
|---|---|
| `--yes` | Do not ask for confirmation. |
| `--dry-run` | Run every check and show the plan; change nothing. |
| `--pool NAME` | ZFS: create the datasets on this pool. |
| `--dir PATH` | Keep all data in this folder; create no dataset. |
| `--release vX.Y.Z` | Install this release (compose files and image digests) instead of the latest. |
| `--source DIR` | Developer: take the compose and its files from this `server/` folder; digests still come from the release. |
| `--images-env FILE` | Developer, needs `--source`: take the digests from this `server-images.env`. |
| `--override FILE` | Apply this compose file on top (kept as `docker-compose.override.yml`). |
| `--import PATH` | Mount this existing folder read-only for migration (repeatable). |
| `--project NAME` | Docker Compose project name (default `juicemount`). |
| `--host ADDR` | Address printed in the Manager URL (default: this host's LAN address). |

To pass options through the pipe: `... | sudo bash -s -- --pool tank`.
`... | bash -s -- --help` lists them.

### Turning on JuiceMount Link

Link (embedded Headscale) is off by default. On a setup-made install:

1. Add `JM_NET_SERVER_URL=https://<public-name>:30193` (and any other
   `JM_NET_*` value from the header of `server/docker-compose.link.yml`) to
   `<data>/compose/.env`.
2. Copy `server/docker-compose.link.yml` from the same release into
   `<data>/compose/overlays/`.
3. Recreate the Manager and start the Link node container:
   `sudo <data>/compose/jm-compose up -d --no-deps juicemount-manager juicemount-link`.

The Link node container (`juicemount-link`) uses host networking, so it is on
the NAS's LAN; the overlay's header says why and what it may do.

Re-running the setup command keeps Link on, because it always applies
`overlays/`. To turn Link off, stop and remove the node container
(`sudo <data>/compose/jm-compose rm -sf juicemount-link`), remove the file
from `overlays/`, and recreate the Manager:
`sudo <data>/compose/jm-compose up -d --no-deps juicemount-manager`.

### Faster downloads to remote Macs (TCP BBR)

A Mac far from the server, on a link that loses some packets, downloads much
faster when the server sends with BBR congestion control instead of the
kernel default, cubic. Cubic slows down at every lost packet; BBR does not.
On a LAN it makes no difference. It is off by default.

The overlay `server/docker-compose.tcp-bbr.yml` asks Docker to set BBR
inside the object store (`minio`) and the proxy (`respproxy`) containers.
The host and the other containers keep cubic, and no host setting changes.

**Check the kernel first.** On the host, one of these must succeed:

- `modinfo tcp_bbr` (BBR is a kernel module; it does not need to be
  loaded, because Docker loads it when it starts the containers), or
- `grep -w bbr /proc/sys/net/ipv4/tcp_available_congestion_control`
  (BBR is already loaded or built in).

If neither succeeds, do not use the overlay: on a kernel without BBR,
Docker refuses to start `minio` and `respproxy`, and the server is down.
The setup command checks this and refuses such an overlay.

On a setup-made install:

1. Copy `server/docker-compose.tcp-bbr.yml` from the same release into
   `<data>/compose/overlays/`.
2. Restart the two services: `sudo <data>/compose/jm-compose up -d --no-deps
   minio respproxy`. This interrupts the Macs' open connections, so do it
   when no Mac is reading or saving.
3. Check: `sudo <data>/compose/jm-compose exec minio cat
   /proc/sys/net/ipv4/tcp_congestion_control` prints `bbr`.

Optional: to have the module loaded before Docker starts, add `modprobe
tcp_bbr` as a **Pre Init** command in **System > Advanced > Init/Shutdown
Scripts**. Docker loads it on demand, so this is not required.

To turn it off, remove the file from `overlays/` and repeat step 2.

Overlays apply to setup-made installs, through `jm-compose` only.
`rollout-compose.sh` and `update-server.sh` do not apply them: after
`rollout-compose.sh --cutover-proxy`, `--proxy-rollback` or
`--proxy-restore`, the proxy sends with the kernel default again.

It speeds up downloads to Macs (reads). Uploads (saves) are sent by the Mac,
and macOS has no BBR.

## Connect a Mac

1. Open the **Manager URL** in a browser and enter the **sign-in key**.
2. In the JuiceMount Mac app, open **Preferences → Connection** and enter:

   ```
   Redis URL:            redis://<server-address>:30179/1
   S3 Endpoint Override: http://<server-address>:30151/zpool
   ```

   Click **Save**, then **Request access** in **Settings → Server access**.
   The app shows a short fingerprint such as `A006-3BB2`.
3. In the Manager, open **Principals** (`/principals.html`). Find the request
   with the same fingerprint and click **Approve**.

The app then stores its own proxy principal and object key in the Keychain.
Confirm access before you start the mount at `/Volumes/zpool`. The plain Redis
URL only gives the proxy's address; it is not a login.

There is no pairing code that a Mac can type. Each Mac is approved by a person
signed in to the Manager. The optional auto-approve window, for moving many
Macs at once, is described in `server/COMPOSE-ROLLOUT.md`.

## Keep it private

- **Redis proxy (30179).** It authenticates every client as a named
  principal, but RESP sends `AUTH` in cleartext, and enrollment is plain
  HTTP. Keep 30179 and the Manager on a trusted LAN, or for remote Macs a
  system private route (Tailscale subnet router, WireGuard). Never publish
  them to the Internet. For encryption on the LAN, put a TLS terminator in
  front of the proxy (for example stunnel or a Caddy layer-4 listener to
  `respproxy:6379`) and point Macs at `rediss://<host>:<tls-port>/1`.
- **MinIO console (30152)** is HTTP only. Do not expose it.
- **WebDAV (30180)** has no authentication in JuiceFS 1.3. It is published on
  127.0.0.1 only. Do not re-expose it without an authenticating proxy.
- **Embedded Link** (Headscale) stays off. To enable it, apply
  `server/docker-compose.link.yml` on top of the compose; its header lists
  what it publishes and what its node container (host networking, no
  capabilities) may do. Do not enable `JM_NET_ALLOW_INSECURE` or
  `JM_NET_ALLOW_LAN_ONLY` in a release deployment.
- **Redis administration** uses the `jm-admin` user, whose password is minted
  inside the Redis container at every start and never leaves it:
  `docker exec -it <redis> sh -c 'REDISCLI_AUTH="$(cat /run/jm-redis/admin.pass)" redis-cli --user jm-admin'`.
- The proxy starts in `passthrough`. Switch to `observe` and then `enforce`
  on the Principals page. The runbook and rollback are in
  `server/COMPOSE-ROLLOUT.md`.
- Remote render nodes use the `farm` principal, user@host only
  (`JM_FARM_NODE_META_URL`, which the setup sets to
  `redis://farm@<host>:30179/1`). The enrollment command passes
  `-e JM_META_PASSWORD` by name; set it in the node's shell first with
  `read -rs JM_META_PASSWORD && export JM_META_PASSWORD`.

The Mac runs the NFS gateway locally. The standalone `juicemount-server`
container is a legacy deployment and is not part of 0.5. The NAS CPU farm
worker is in the compose. A GPU render worker needs a separate host; GPU
admission and deployed queue recovery have not passed 0.5 acceptance.

## Diagnostic endpoints

- Manager web UI: `http://<server>:30190`
- MinIO console: `http://<server>:30152` (user `juicemount`, password
  `MINIO_ROOT_PASSWORD` in `.env`)
- WebDAV: `http://127.0.0.1:30180` on the server only

## Migrating existing data

Install with `--import /mnt/tank/media` (repeat for each folder). Each folder
appears read-only in the Manager as `/sources/<folder name>`, so a copy cannot
damage it. Then, in the Manager's **Migrations** tab:

1. Click a source root (for example `/sources/media`) and open the folder to
   migrate.
2. The destination fills in with a mirror path under `/jfs/`, without the
   source-root prefix.
3. The default options suit Mac access: structure kept 1:1, junk files
   (`.DS_Store`, `._*`, `Thumbs.db`, `.sync.ffs_db`) excluded, file
   permissions not preserved.
4. Progress shows percent, files and bytes copied, and errors. Jobs queue and
   run one at a time.

Tick **Preserve file permissions** before Start only when you need the source
uid/gid/mode (for example archival to another POSIX system).

For an install that has already started, add an import folder by hand: add a
read-only line under the `juicemount-manager` service's `volumes:` in
`<data>/compose/docker-compose.yml` and recreate only that service
(`docker compose ... up -d --no-deps juicemount-manager`).

## If the init container exits non-zero

Each exit code of `juicefs-init` names the failure; the setup command prints
it:

| Code | Meaning |
|---|---|
| 2 | MinIO unreachable from inside the container network |
| 4 | Credentials empty, whitespace, or placeholder pattern |
| 5 | `JM_BUCKET_URL` missing the `http://` scheme |
| 6 | `juicefs format` itself errored (read the log line above for the JuiceFS-side reason) |

Each precheck logs `[precheck-N]` prefix lines you can grep for in the
container log. Code 3 (Redis PING failed) belonged to composes before the
Redis proxy; today an unreachable proxy shows as code 6.

If the Manager refuses to start with an admin-key error, the persisted
`JM_ADMIN_KEY` is empty, shorter than 32 characters, or still a placeholder.
Restore the same durable key used by the prior deployment before recreating the
service.

## Upgrading installs made by hand

The sections below apply to installs made by hand (appendix) or before the
setup command existed. They do not upgrade a setup-made install; upgrading
those is not supported yet (see "Running it again").

## Upgrading from --trash-days 0 (SLICE 3 trash retention)

JuiceMount installs that formatted before SLICE 3 used
`--trash-days 0`, which disabled JuiceFS's built-in trash retention
entirely (deletes were immediate, permanent, and unrecoverable). SLICE
3 ships with `--trash-days 7` for new installs and introduces the
**Trash tab** in the Manager UI for browse/restore/delete operations.

`juicefs format` skips an existing volume, so changing the compose value does
not change an existing volume's retention. Use the Manager **Trash → Retention**
control to set and verify the value. The Manager performs the authenticated
admin operation; avoid putting the admin secret in a URL or shell command.

**What this does NOT recover:** files deleted before the retention
knob was turned on are already gone. The change applies forward only.

**Capacity heads-up:** the `.trash/` subtree lives inside the JuiceFS
volume itself. A high-churn workflow with a long retention window
will see real space sitting in trash. The Trash tab header banner
makes this LOUD; operators should size the JuiceFS volume with a
margin for retention.

## Upgrading from juicemount-migrator (SLICE 0 rename)

If you previously installed JuiceMount with the `juicemount-migrator`
image tag, SLICE 0 of the manager roadmap renamed the binary, image,
HTTP prefix, and state-file path. The transition is one-release
backward-compatible:

- Older installs may still use mutable `juicemount-migrator` image tags.
  Move to the accepted digest-pinned Manager image for 0.5 after following
  the service-scoped [Compose rollout procedure](COMPOSE-ROLLOUT.md).
- HTTP requests to `/migrator/*` are 301-redirected to `/manager/*`.
- The container reads `/var/lib/migrator/jobs.json` as a fallback if
  `/var/lib/manager/state.json` doesn't exist yet, then writes going
  forward to the new path. A one-shot log line announces the
  migration. To carry job history forward, additionally bind-mount the
  old `migrator-state` dataset at `/var/lib/migrator` (read-only is
  fine).

An existing pre-proxy installation is moved with
`server/scripts/rollout-compose.sh --cutover-proxy`, not by re-applying the
YAML or running the setup command. See `server/COMPOSE-ROLLOUT.md`.

## Appendix: manual install (TrueNAS "Install via YAML")

Use this path only when the setup command cannot run, or to install the stack
as a TrueNAS custom app. TrueNAS 24.10+ removed "Add Catalog"; its replacement
is **Apps → Discover → ⋮ → Install via YAML**, which takes a Docker Compose
YAML and runs it as a native TrueNAS app.

### Datasets

Create these datasets (Datasets → Add Dataset) and note their full paths
(for example `/mnt/tank/juicemount/bucket`):

| Dataset | What it holds | Sized for |
|---|---|---|
| `bucket` | MinIO chunk storage — every byte of your media | Large (HDD pool fine) |
| `redis` | Redis AOF/RDB — JuiceFS metadata | Small (a few GB) on SSD |
| `cache` | JuiceFS local chunk cache | Fast SSD/NVMe, sized to your active project working set |
| `manager-state` | JSON file of job history, settings, destinations, schedules, Mac enrollments | <1 MB; any pool fine |
| `respproxy` | Proxy principals file (password hashes, mode 0600), rollback fragment, reload handshake. Shared by `respproxy` and the Manager | <1 MB; any pool fine |
| `objkeys`, `headscale`, `juicefarm-state` | Per-Mac key exchange, Link state, farm index | Small |

To migrate existing data, also note the path of each source dataset. The
Manager mounts it read-only.

### Settings

Copy `server/release.env.example` outside the repository and replace every
placeholder. It lists each value and how to generate it:

```
openssl rand -hex 32      # JM_ADMIN_KEY and each JM_*_PASSWORD (separately)
openssl rand -hex 20      # MINIO_ROOT_PASSWORD, JM_OBJ_CLIENT_SECRET_KEY, JM_OBJ_GC_SECRET_KEY
zpool list -Hp -o size <pool>   # JM_FARM_STORAGE_CAPACITY_BYTES
```

`zpool list` reports the raw pool size, before RAID-Z parity, so on RAID-Z
this value is larger than the usable space. The farm uses it only to report
total capacity and to scale its default 1% reserve; the setup command uses the
same value.

Take the four image digests from the release's `server-images.env` (also in
its `release-manifest.json`); never replace them with mutable tags. The MinIO
root password stays in the `minio` and `minio-keys` services. The volume is
formatted with no object key in its Redis `setting`; each JuiceFS client
brings its own key. Macs and the farm get `jm-client`, which cannot delete
objects; only the server's `bg` mount and the Manager get `jm-gc`. See
COMPOSE-ROLLOUT.md, "Object keys".

Treat the Manager key and every proxy password as durable deployment state.
Reuse the same values on every upgrade. Before applying an edited or rendered
compose, verify that its `JM_ADMIN_KEY` is neither empty nor a `CHANGEME_*`
placeholder. A live container may have received a key at runtime even when
the persisted TrueNAS custom-app YAML is empty; recreating from that drifted
YAML drops the key and the Link-enabled Manager correctly refuses to start. Do
not use the live container's healthy state as proof that the stored app
configuration is complete.

### Render and install

1. For remote use, establish a trusted system private route from the Mac to
   the NAS (see "Keep it private"). Verify the Redis and MinIO endpoints from
   the remote network before testing JuiceMount.
2. Edit only the host bind paths in the canonical compose: replace
   `/mnt/zSSD/juicemount/` with your dataset parent and
   `/mnt/storage0/zpool` with your source dataset. Confirm the bucket dataset
   is excluded from recursive ZFS snapshots.
3. Render without touching the live app:

   ```sh
   docker compose --env-file /secure/path/juicemount-release.env \
     -f server/docker-compose.yml config > /secure/path/juicemount-rendered.yml
   ```

4. Inspect the rendered file: first-party images must use `@sha256:…`, WebDAV
   must remain bound to `127.0.0.1`, credentials must be non-placeholder, and
   both Manager and the NAS worker must carry the same numeric physical
   capacity. Embedded Link must be `off`, and neither `JM_NET_ALLOW_INSECURE`
   nor `JM_NET_ALLOW_LAN_ONLY` may be enabled.
5. Copy `server/redis.conf`, `server/redis.acl`, and the `server/minio`
   directory to stable NAS paths, then replace their relative mount sources
   in the rendered YAML with those exact paths. Paste the result into
   **Apps → Discover → ⋮ → Install via YAML**.

The rendered YAML contains every secret. Keep it as private as the settings
file, and delete any copy you do not need.
