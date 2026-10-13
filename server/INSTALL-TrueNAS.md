# Install JuiceMount on TrueNAS Scale via "Install via YAML"

This is the 0.5 release install path for TrueNAS Scale
24.10+ (Electric Eel, Fangtooth, Goldeye). The "Add Catalog" feature
was removed in 24.10; you can't sideload third-party catalogs. The
replacement is **Apps → Discover → ⋮ menu → Install via YAML**, which
takes a Docker Compose YAML and runs it as a native TrueNAS app.

## Before you start

Fresh 0.5 server installation has not passed qualification. The legacy MinIO
release pinned by the Compose file is affected by published
[object-write authentication bypass](https://github.com/minio/minio/security/advisories/GHSA-hv4r-mvr4-25vw)
and [Snowball authentication bypass](https://github.com/minio/minio/security/advisories/GHSA-9c4q-hq6p-c237)
advisories. Digest pinning, authenticated pulls, and cached images do not fix
these vulnerabilities. Keep MinIO on a trusted private route; do not expose
it to the public Internet.

The proposed storage replacement has passed bounded compatibility and stopped
migration checks, but full security and deployment qualification remain open.
It is not shipped or approved for production installation. Preserve working
deployments and their data; image access alone does not qualify a fresh install
or a storage replacement.

If this installation uses ZFS periodic snapshots, exclude the MinIO bucket dataset
from recursive snapshots of the parent pool (for the default layout,
`zSSD/juicemount/bucket`). JuiceFS already owns file/trash history in metadata; ZFS
snapshots of the object bucket retain deleted slice blocks after GC and can consume
the pool without increasing visible JuiceFS usage. Back up JuiceFS using a coordinated
metadata-and-object procedure instead of treating an isolated bucket snapshot as a
filesystem rollback.

Set `JM_FARM_STORAGE_CAPACITY_BYTES` to the numeric result of
`zpool list -Hp -o size <pool>` for exact Farm total-capacity reporting. ZFS `statfs`
is dataset-scoped, although its available-byte value still reflects shared pool
headroom.

Create three datasets in TrueNAS (Datasets → Add Dataset):

| Dataset | What it holds | Sized for |
|---|---|---|
| `bucket` | MinIO chunk storage — every byte of your media | Large (HDD pool fine) |
| `redis` | Redis AOF/RDB — JuiceFS metadata | Small (a few GB) on SSD |
| `cache` | JuiceFS local chunk cache | Fast SSD/NVMe, sized to your active project working set |

Note their full paths (e.g., `/mnt/tank/juicemount/bucket`).

**Optional — for migrating existing data**: if you have an existing
dataset full of media you want to copy into JuiceMount (so the JuiceFS
volume becomes your single source of truth), note its full path too.
The manager service exposes it read-only inside the container so the
copy can't damage the source. You can add multiple.

Also create one small dataset for manager state (job history JSON
plus future control-plane state), and one for the RESP proxy's principals:

| Dataset | What it holds | Sized for |
|---|---|---|
| `manager-state` | JSON file of job history, settings, destinations, schedules, Mac enrollments | <1 MB; any pool fine |
| `respproxy` | Proxy principals file (password hashes, mode 0600), rollback fragment, reload handshake. Shared by `respproxy` and the Manager | <1 MB; any pool fine |

Generate a strong MinIO password and JuiceMount admin key:

```
openssl rand -base64 24   # for MINIO_ROOT_PASSWORD
openssl rand -hex 32      # for JM_ADMIN_KEY (used to gate /manager UI)
openssl rand -hex 20      # for JM_OBJ_CLIENT_SECRET_KEY (MinIO user jm-client)
openssl rand -hex 20      # for JM_OBJ_GC_SECRET_KEY (MinIO user jm-gc)
```

The MinIO root password stays in the `minio` and `minio-keys` services. The
volume is formatted with no object key in its Redis `setting`; each JuiceFS
client brings its own key instead. Macs and the farm get `jm-client`, which
cannot delete objects. Only the server's `bg` mount and the Manager get
`jm-gc`. See COMPOSE-ROLLOUT.md, "Object keys".

Note your TrueNAS LAN IP — your Mac will need to reach this address
on the published ports below.

Treat the Manager key as durable deployment state. Reuse the same value on
every upgrade. Before applying an edited or rendered compose, verify that its
`JM_ADMIN_KEY` is neither empty nor a `CHANGEME_*` placeholder. A live
container may have received a key at runtime even when the persisted TrueNAS
custom-app YAML is empty; recreating from that drifted YAML drops the key and
the Link-enabled Manager correctly refuses to start. Do not use the live
container's healthy state as proof that the stored app configuration is
complete.

## Render the exact release YAML

The 0.5 install input is the repository's canonical
`server/docker-compose.yml` rendered with values from
`server/release.env.example`. It digest-pins every image and refuses to render
without the existing Manager key, MinIO password, four release image digests,
proxy and object credentials, and the physical zpool capacity. Obtain the
digests from the published release manifest. This render step prevents an
edited TrueNAS form from silently dropping a credential or reverting to a
mutable image.

Fresh 0.5 stack installation has not been qualified. During the release check,
an anonymous pull of the compose-pinned MinIO image
(`sha256:ed9be66eb5f2636c18289c34c3b725ddf57815f2777c77b5938543b78a44f144`)
was unavailable: Docker Hub returned 401 for its manifest after issuing a token
without pull scope, and Quay's anonymous request also returned 401. This does
not establish whether authenticated accounts can pull it or whether an existing
image cache works. Before a fresh install or replacing a working stack, verify
that the deployment host can access that exact MinIO image and every matching
server and base image digest. Image access and a valid rendered configuration
are prerequisites, not security or installation qualification. Preserve the
working stack and its data while the qualification gaps above remain open.

1. Copy `server/release.env.example` outside the repository and replace every
   placeholder. Obtain `JM_FARM_STORAGE_CAPACITY_BYTES` from
   `zpool list -Hp -o size <pool>`.
2. For remote use, establish a trusted system private route from the Mac to
   the NAS. The 0.5 client uses the Mac's normal route to its configured Redis
   and MinIO endpoints. Verify both endpoints from the remote network before
   testing JuiceMount. A Tailscale subnet router or an existing WireGuard,
   QNAP, or UniFi route can supply this path. Keep Redis, MinIO, WebDAV, and
   Manager off the public Internet.
3. Edit only the host bind paths in the canonical compose for the target NAS.
   Confirm the MinIO bucket dataset is
   excluded from recursive ZFS snapshots before starting the farm.
4. Render without touching the live app:

   ```sh
   docker compose --env-file /secure/path/juicemount-release.env \
     -f server/docker-compose.yml config > /secure/path/juicemount-rendered.yml
   ```

5. Inspect the rendered file: first-party images must use `@sha256:…`, WebDAV
   must remain bound to `127.0.0.1`, credentials must be non-placeholder, and
   both Manager and the NAS worker must carry the same numeric physical capacity.
   Embedded Link must be `off`, and neither `JM_NET_ALLOW_INSECURE` nor
   `JM_NET_ALLOW_LAN_ONLY` may be enabled. The release's remote acceptance run
   must prove the client reaches storage over its system private route.
6. Copy `server/redis.conf`, `server/redis.acl`, and the `server/minio`
   directory to stable NAS paths, then replace their relative mount sources
   in the rendered YAML with those exact paths. Paste the result into
   **Apps → Discover → ⋮ → Install via YAML**.

The Mac runs the NFS gateway locally. The standalone `juicemount-server`
container is a legacy/optional deployment and is not part of this 0.5
installation. The NAS CPU farm worker is in the compose above. A GPU render
worker requires a separate host and deployment procedure; GPU admission and
deployed queue recovery have not passed 0.5 acceptance, so do not count that
lane as ready.

Embedded Headscale remains an advanced, explicit option for operators who run
its public HTTPS control plane. Preserve any existing Headscale state directory
when moving to the system private route; disabling the embedded service does
not require deleting paired identities. The default 0.5 release manifest does
not publish a Headscale listener or require a Headscale URL. Do not enable
`JM_NET_ALLOW_INSECURE` or `JM_NET_ALLOW_LAN_ONLY` in a release deployment.

### The RESP proxy (H-8B)

Redis has no published port. `respproxy` owns host port 30179, and every
client of the volume logs in to it as a named principal. Connections without
a valid principal are refused.

- **Server principals** come from the render env: `JM_REDIS_PROXY_PASSWORD`
  (the proxy's own Redis login) and `JM_PROXY_FARM_PASSWORD`,
  `JM_PROXY_MANAGER_PASSWORD`, `JM_PROXY_BG_PASSWORD`,
  `JM_PROXY_WEBDAV_PASSWORD`, `JM_PROXY_ADMIN_PASSWORD`. Generate each one
  separately with `openssl rand -hex 32`. Render refuses to start without
  them. Treat them like `JM_ADMIN_KEY`: reuse the same values on every
  upgrade.
- **No secret on any command line.** Every volume URL in the compose is
  `user@host` only. The Manager and the farm read their secrets from
  `JM_META_PASSWORD` (and the Manager from `JM_META_BG_PASSWORD` for
  gc/compact/fsck and `JM_META_ADMIN_PASSWORD` for lease break, the mode
  switch and `juicefs config`); juicefs and jmfarm receive them as
  `META_PASSWORD`. The Manager image refuses to start with a password inline
  in `JM_META`.
- **Macs** are not in the env file. The 0.5 app enrolls with the Manager
  (`POST /api/principals/enroll`, using its instance ID). The secret is sent
  once; the reply carries a poll token the app uses afterwards. An admin
  approves the Mac on the Manager's Principals page
  (`http://<nas>:30190/principals.html`), which the Manager allows only once
  the proxy is live (it has acknowledged the current principals file). The
  optional cutover auto-approve
  window is capped (default 20 Macs, at most 2 per address) and everything it
  approves stays on a review list; see `server/COMPOSE-ROLLOUT.md` for when it
  may be opened.
- **Transport security.** The proxy speaks plain RESP: `AUTH` crosses the
  network in cleartext on every connection, and enrollment is plain HTTP.
  Keep port 30179 and the Manager on a trusted path: the LAN, or for remote
  Macs the 0.5 system private route (Tailscale subnet router, WireGuard).
  Never publish them to the Internet. If you need encryption on the LAN,
  terminate TLS in front of the proxy (for example a stunnel or Caddy layer-4
  listener forwarding to `respproxy:6379`) and point Macs at
  `rediss://<host>:<tls-port>/1`. jm-respproxy itself has no TLS listener.
- **Redis administration** uses the `jm-admin` user, whose password is minted
  inside the Redis container at every start and never leaves it:
  `docker exec -it <redis> sh -c 'REDISCLI_AUTH="$(cat /run/jm-redis/admin.pass)" redis-cli --user jm-admin'`.
- The proxy starts in `passthrough`. Switch to `observe` and then `enforce`
  on the Principals page. The full runbook and the rollback commands are in
  `server/COMPOSE-ROLLOUT.md`.
- Remote render nodes (`JM_FARM_NODE_META_URL`) use the `farm` principal,
  user@host only: `redis://farm@<host>:30179/1`. The Manager refuses a
  password in it. The enrollment command passes `-e JM_META_PASSWORD` by
  name; set it in the node's shell first with
  `read -rs JM_META_PASSWORD && export JM_META_PASSWORD`.
- The compose mounts `./redis.conf` and `./redis.acl`. Rendering from the
  repository checkout keeps them; if you paste the YAML into TrueNAS,
  replace those two relative paths with absolute paths to the same files on
  the NAS.

An existing pre-proxy installation is moved with
`server/scripts/rollout-compose.sh --cutover-proxy`, not by re-applying the
YAML. See `server/COMPOSE-ROLLOUT.md`.

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

## After install

In the JuiceMount Mac app → Preferences → Connection:

```
Redis URL:            redis://<your-truenas-ip>:30179/1
S3 Endpoint Override: http://<your-truenas-ip>:30151/zpool
```

Then click Save and use **Request access** in Settings → Server access. The
plain Redis URL supplies the proxy host; it is not an authenticated login by
itself. An admin approves this Mac on Manager's Principals page, after which
the app stores its own principal and object key in Keychain. Confirm access
before starting the mount at `/Volumes/zpool`.

## Diagnostic endpoints

- MinIO Console: `http://<truenas-ip>:30152`
- WebDAV: `http://127.0.0.1:30180` on the NAS host only (loopback-published by default)
- Manager web UI: `http://<truenas-ip>:30190` (if you included the service)

## Migrating existing data

The manager UI at `:30190` → Migrations tab lets you copy from any
bind-mounted source root into the JuiceFS volume:

1. Click a source root (e.g. `/sources/oldzpool`), drill into the folder
   to migrate.
2. The destination input auto-fills with a sensible mirror path under
   `/jfs/` — strips the source-root prefix so you don't get
   `/jfs/oldzpool/...` cluttering the structure.
3. Default options are tuned for Mac access: structure preserved 1:1,
   junk files (`.DS_Store`, `._*`, `Thumbs.db`, `.sync.ffs_db`) excluded,
   file permissions NOT preserved (so destination files land with
   sensible defaults the Mac user can read).
4. Live progress shows real %, files copied, bytes copied, errors.
   Multiple jobs queue and run sequentially.

If you do want to preserve source uid/gid/mode (e.g. archival to
another POSIX system), tick **Preserve file permissions** before
hitting Start.

## If the init container exits non-zero

Each exit code names the failure:

| Code | Meaning |
|---|---|
| 2 | MinIO unreachable from inside the container network |
| 3 | Redis PING failed |
| 4 | Credentials empty, whitespace, or placeholder pattern |
| 5 | `JM_BUCKET_URL` missing the `http://` scheme |
| 6 | `juicefs format` itself errored (read the log line above for the JuiceFS-side reason) |

Each precheck logs `[precheck-N]` prefix lines you can grep for in
the TrueNAS UI Apps → Logs view.

If Manager refuses to start with an admin-key error, the persisted
`JM_ADMIN_KEY` is empty, shorter than 32 characters, or still a placeholder.
Restore the same durable key used by the prior deployment before recreating the
service.
