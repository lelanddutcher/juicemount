# Install the JuiceMount server (TrueNAS SCALE or any Docker host)

One command installs the JuiceMount 0.5 server stack. You type no password,
key, image digest or path. The command is `server/scripts/juicemount-setup.sh`
in this repository.

## Before you start

- **TrueNAS SCALE 24.10 or later** (Electric Eel, Fangtooth, Goldeye) with
  Apps set up: **Apps → Configuration → Choose Pool**. This starts Docker.
  Or **any Linux host** with Docker Engine and Docker Compose v2
  (`docker compose version` answers).
- These host ports are free: 30179 (Redis proxy), 30151 and 30152 (object
  store), 30190 (Manager), and 30180 on 127.0.0.1 (WebDAV).
- The host can reach `github.com`, `raw.githubusercontent.com`, `ghcr.io`
  and the registries named in `server/docker-compose.yml`.

**Known limit (object store image).** The MinIO image pinned in
`server/docker-compose.yml` cannot be downloaded anonymously any more: Docker
Hub and Quay answer 401. Until a replacement image is chosen and pinned, the
setup command stops at "could not download an image" and starts nothing. It
keeps the generated settings, so running it again later continues from there.
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
     boot pool). `--pool NAME` chooses the pool.
   - On another host it uses the folder `/srv/juicemount`. `--dir PATH`
     chooses another folder (on TrueNAS too: then it creates no dataset).
2. **Reads the image digests** of the latest release from its published
   `server-images.env`, and checks each one against the release's
   `release-manifest.json`. `--release vX.Y.Z` chooses a release.
3. **Takes the compose file** `server/docker-compose.yml` and the files it
   mounts (`redis.conf`, `redis.acl`, `minio/`) from this repository's `main`
   branch, and replaces the sample host paths with the chosen storage. Every
   other image (Redis, the object store, JuiceFS) comes from that compose
   file. `--source DIR` uses a local checkout's `server/` folder instead.
4. **Shows the plan and asks once.** `--yes` skips the question (needed when
   no terminal is attached). `--dry-run` shows the plan and changes nothing.
5. **Generates every secret** from `/dev/urandom`: the Manager sign-in key,
   the MinIO root password, the six Redis proxy passwords and the two object
   keys. It writes them, with the digests and the storage size, to the
   settings file. Values reach Docker Compose only from that file.
6. **Validates before starting.** `docker compose config` must render (every
   required value has a `${VAR:?}` guard), every image must be pinned by
   `@sha256:` digest, every mounted path must exist, and no port may be in use.
7. **Pulls the images and starts the stack.** If the one-shot `juicefs-init`
   fails, the command prints its exit code and meaning (table below).
8. **Waits until the Manager answers** with the new key, then prints the two
   lines.

### Where everything is

`<data>` is `/mnt/<pool>/juicemount` on TrueNAS, `/srv/juicemount` otherwise.

| Path | Holds | Mode |
|---|---|---|
| `<data>/compose/` | Compose project folder: `docker-compose.yml`, `redis.conf`, `redis.acl`, `minio/`, optional `docker-compose.override.yml` | `0700` |
| `<data>/compose/.env` | **Every secret of this server**, the image digests, the storage size | `0600` |
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
TrueNAS Apps page may list it as an external app. To see it:

```sh
sudo docker compose --project-name juicemount --project-directory <data>/compose --env-file <data>/compose/.env ps
```

### Running it again

Running the same command again is safe:

- It never rewrites `<data>/compose/.env`, so no secret changes.
- It never changes or deletes data. It starts any stopped service
  (`docker compose up -d`), waits for the Manager and prints the same two lines.
- It finds the install through the running Compose project or, on TrueNAS,
  through `<pool>/juicemount`.
- It refuses, and changes nothing, if a data folder already holds data but
  `.env` is missing, if `.env` lacks a secret, or if a folder with content is
  where a new dataset would go.
- Options that would change a started install (`--source`, `--override`,
  `--release`, `--import`) are ignored with a note. An install that has never
  started takes them.

The command does not upgrade a running server. To upgrade, follow
[COMPOSE-ROLLOUT.md](COMPOSE-ROLLOUT.md).

### All options

| Option | Effect |
|---|---|
| `--yes` | Do not ask for confirmation. |
| `--dry-run` | Show the plan; change nothing. |
| `--pool NAME` | ZFS: create the datasets on this pool. |
| `--dir PATH` | Keep all data in this folder; create no dataset. |
| `--release vX.Y.Z` | Read image digests from this release instead of the latest. |
| `--images-env FILE` | Read image digests from a local `server-images.env`. |
| `--source DIR` | Take the compose and its files from this `server/` folder. |
| `--override FILE` | Apply this compose file on top (kept as `docker-compose.override.yml`). |
| `--import PATH` | Mount this existing folder read-only for migration (repeatable). |
| `--project NAME` | Docker Compose project name (default `juicemount`). |
| `--host ADDR` | Address printed in the Manager URL (default: this host's LAN address). |

To pass options through the pipe: `... | sudo bash -s -- --pool tank`.

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
  what it publishes. Do not enable `JM_NET_ALLOW_INSECURE` or
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
| 3 | Redis PING failed |
| 4 | Credentials empty, whitespace, or placeholder pattern |
| 5 | `JM_BUCKET_URL` missing the `http://` scheme |
| 6 | `juicefs format` itself errored (read the log line above for the JuiceFS-side reason) |

Each precheck logs `[precheck-N]` prefix lines you can grep for in the
container log.

If the Manager refuses to start with an admin-key error, the persisted
`JM_ADMIN_KEY` is empty, shorter than 32 characters, or still a placeholder.
Restore the same durable key used by the prior deployment before recreating the
service.

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
