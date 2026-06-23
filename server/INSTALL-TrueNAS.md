# Install JuiceMount on TrueNAS Scale via "Install via YAML"

This is the **current shippable install path** for TrueNAS Scale
24.10+ (Electric Eel, Fangtooth, Goldeye). The "Add Catalog" feature
was removed in 24.10; you can't sideload third-party catalogs. The
replacement is **Apps → Discover → ⋮ menu → Install via YAML**, which
takes a Docker Compose YAML and runs it as a native TrueNAS app.

## Before you start

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
plus future control-plane state):

| Dataset | What it holds | Sized for |
|---|---|---|
| `manager-state` | JSON file of job history, settings, destinations, schedules | <1 MB; any pool fine |

Generate a strong MinIO password and JuiceMount admin key:

```
openssl rand -base64 24   # for MINIO_ROOT_PASSWORD
openssl rand -hex 32      # for JM_ADMIN_KEY (used to gate /manager UI)
```

Note your TrueNAS LAN IP — your Mac will need to reach this address
on the published ports below.

## Paste this YAML

Apps → Discover → kebab menu → **Install via YAML** → paste this and
edit the four `CHANGEME_*` placeholders before submitting.

```yaml
services:

  # ─── Redis: JuiceFS metadata authority ─────────────────────────
  redis:
    image: redis:7.4-alpine
    restart: unless-stopped
    ports:
      - "30179:6379"                  # Mac client connects here
    command:
      - "redis-server"
      - "--appendonly"
      - "yes"
      - "--appendfsync"
      - "everysec"
      - "--maxmemory-policy"
      - "noeviction"
      - "--save"
      - "900"
      - "1"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
    volumes:
      - CHANGEME_REDIS_PATH:/data     # !!! EDIT — your Redis dataset path

  # ─── MinIO: S3 object store for chunks ─────────────────────────
  minio:
    image: minio/minio:RELEASE.2025-01-20T14-49-07Z
    restart: unless-stopped
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: juicemount
      MINIO_ROOT_PASSWORD: CHANGEME_MINIO_PASSWORD    # !!! EDIT — strong, no whitespace
      MINIO_API_REQUESTS_DEADLINE: 10m
    ports:
      - "30151:9000"                  # Mac client connects here
      - "30152:9001"                  # MinIO console — web UI
    healthcheck:
      test: ["CMD", "curl", "-fsS", "http://localhost:9000/minio/health/live"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 10s
    volumes:
      - CHANGEME_BUCKET_PATH:/data    # !!! EDIT — your MinIO bucket dataset path

  # ─── juicefs-init: one-shot with pre-flight checks ─────────────
  juicefs-init:
    image: juicedata/mount:ce-v1.3.1
    restart: "no"
    depends_on:
      minio:
        condition: service_healthy
      redis:
        condition: service_healthy
    environment:
      MINIO_ROOT_USER: juicemount
      MINIO_ROOT_PASSWORD: CHANGEME_MINIO_PASSWORD    # !!! EDIT — same as above
      JM_BUCKET_URL: "http://minio:9000/zpool"
      JM_META_URL: "redis://redis:6379/1"
      JM_VOL_NAME: "zpool"
    entrypoint: ["/bin/sh", "-c"]
    command:
      - |
        set -e
        echo "[precheck-0] sleeping 2s for minio admin api warmup..."
        sleep 2
        echo "[precheck-1] minio reachable at $$JM_BUCKET_URL"
        if ! curl -fsS -m 5 http://minio:9000/minio/health/live >/dev/null; then
          echo "[precheck-1] FAIL: minio not reachable. Check MinIO container logs." >&2; exit 2
        fi
        echo "[precheck-2] redis ping"
        if ! redis-cli -h redis ping 2>/dev/null | grep -q PONG; then
          echo "[precheck-2] FAIL: redis not reachable. Check Redis container logs." >&2; exit 3
        fi
        echo "[precheck-3] credentials non-empty + no placeholder + no whitespace"
        if [ -z "$$MINIO_ROOT_USER" ] || [ -z "$$MINIO_ROOT_PASSWORD" ]; then
          echo "[precheck-3] FAIL: MINIO_ROOT_USER/PASSWORD must be non-empty" >&2; exit 4
        fi
        case "$$MINIO_ROOT_PASSWORD" in CHANGEME*|CHANGE*|REPLACE*|replaceme*)
          echo "[precheck-3] FAIL: MinIO password looks like a placeholder. Edit the YAML." >&2; exit 4 ;;
        esac
        case "$$MINIO_ROOT_USER" in *' '*) echo "[precheck-3] FAIL: MINIO_ROOT_USER contains whitespace" >&2; exit 4 ;; esac
        case "$$MINIO_ROOT_PASSWORD" in *' '*) echo "[precheck-3] FAIL: MINIO_ROOT_PASSWORD contains whitespace" >&2; exit 4 ;; esac
        echo "[precheck-4] bucket URL well-formed"
        case "$$JM_BUCKET_URL" in http://*|https://*) ;; *)
          echo "[precheck-4] FAIL: bucket URL must start with http:// or https://" >&2; exit 5 ;;
        esac
        echo "[precheck-5] juicefs already formatted?"
        if juicefs status "$$JM_META_URL" >/dev/null 2>&1; then
          echo "[precheck-5] OK: volume already formatted; skipping format."
          exit 0
        fi
        echo "[format] running juicefs format with bucket=$$JM_BUCKET_URL"
        # SLICE 3 default: --trash-days 7. New installs format with
        # JuiceFS's built-in trash retention enabled so the Manager
        # UI's Trash tab has a useful default window. The "Upgrading
        # from --trash-days 0" section below covers existing installs.
        juicefs format --storage minio --bucket "$$JM_BUCKET_URL" \
          --access-key "$$MINIO_ROOT_USER" --secret-key "$$MINIO_ROOT_PASSWORD" \
          --trash-days 7 "$$JM_META_URL" "$$JM_VOL_NAME" || { echo "[format] FAIL: juicefs format errored" >&2; exit 6; }
        echo "[format] complete"

  # ─── juicefs: live FUSE mount + WebDAV (for browse / smoke test) ─
  juicefs:
    image: juicedata/mount:ce-v1.3.1
    restart: unless-stopped
    depends_on:
      juicefs-init:
        condition: service_completed_successfully
    cap_add:
      - SYS_ADMIN
    devices:
      - /dev/fuse
    security_opt:
      - apparmor:unconfined
    ports:
      - "30180:80"                    # WebDAV — Finder Cmd+K
    healthcheck:
      test: ["CMD", "sh", "-c", "mountpoint -q /jfs && curl -sf http://localhost:80/ || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s
    volumes:
      - CHANGEME_CACHE_PATH:/jfs-cache    # !!! EDIT — your fast-SSD cache dataset
    entrypoint: ["/bin/sh", "-c"]
    command:
      - |
        set -e
        echo "Mounting JuiceFS at /jfs..."
        juicefs mount \
          --cache-dir /jfs-cache \
          --cache-size 100000 \
          --buffer-size 4096 \
          --prefetch 3 \
          --backup-meta 3600 \
          redis://redis:6379/1 \
          /jfs &
        sleep 5
        until mountpoint -q /jfs; do sleep 1; done
        echo "Mount ready. Starting WebDAV on :80..."
        mkdir -p /jfs/data /jfs/shared
        chmod 777 /jfs/data /jfs/shared
        juicefs webdav --cache-dir /jfs-cache --cache-size 100000 \
          redis://redis:6379/1 0.0.0.0:80 &
        wait

  # ─── juicemount-manager: control-plane web UI for the JuiceFS volume ─
  # (Renamed from juicemount-migrator in SLICE 0 of the manager
  # roadmap. The legacy juicemount-migrator image tag is still
  # published for one release as a compat alias — but new installs
  # should use juicemount-manager.)
  #
  # Browse host paths under /sources, pick a destination under /jfs,
  # watch live progress with real % bar (driven by juicefs's Prometheus
  # metrics). Bind-mount as many existing datasets read-only as you
  # want. Omit this service entirely if you have no existing data.
  juicemount-manager:
    image: ghcr.io/lelanddutcher/juicemount-manager:latest
    pull_policy: always
    restart: unless-stopped
    depends_on:
      juicefs-init:
        condition: service_completed_successfully
    environment:
      JM_META: "redis://redis:6379/1"
      JM_VOL_NAME: "zpool"
      JM_SOURCE_ROOTS: "/sources"
      JM_ADMIN_KEY: CHANGEME_ADMIN_KEY        # !!! EDIT — 32+ random chars; empty = LAN-only
      JM_STATE_FILE: "/var/lib/manager/state.json"   # state persists across restart
    ports:
      - "30190:8080"                          # web UI
    volumes:
      # One bind-mount per existing dataset you want to migrate from.
      # Each becomes a browsable source root in the UI.
      - CHANGEME_SOURCE_PATH:/sources/<your-source-name>:ro
      # Small writable mount for the JSON state file. Without this,
      # state vanishes on container restart (no Resume button
      # available for canceled jobs after a redeploy).
      - CHANGEME_STATE_PATH:/var/lib/manager
```

## Upgrading from --trash-days 0 (SLICE 3 trash retention)

JuiceMount installs that formatted before SLICE 3 used
`--trash-days 0`, which disabled JuiceFS's built-in trash retention
entirely (deletes were immediate, permanent, and unrecoverable). SLICE
3 ships with `--trash-days 7` for new installs and introduces the
**Trash tab** in the Manager UI for browse/restore/delete operations.

`juicefs format` is idempotent-skipped on every boot after the first
(the `precheck-5` step in `juicefs-init` short-circuits when the
metadata store already exists), so flipping the YAML value above does
**NOT** auto-migrate existing volumes. To turn on 7-day retention on
an already-formatted volume, exec into the running init container
once:

```sh
docker exec ix-juicemount-juicefs-1 juicefs config <metaURL> --trash-days 7
```

Replace `<metaURL>` with the same Redis URL the compose YAML sets in
`JM_META_URL` (typically `redis://redis:6379/1`). Verify the change:

```sh
docker exec ix-juicemount-juicefs-1 juicefs config <metaURL> | grep TrashDays
```

Once the value is set, the Manager UI's Trash tab → Retention drop-
down shows the current value live and can update it from there
without further `docker exec` calls.

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

- Both `juicemount-manager:latest` and the legacy
  `juicemount-migrator:latest` image tags publish from CI (alongside the
  pinned `:X.Y.Z` / `:X.Y` version tags cut from each `vX.Y.Z` git tag).
  Existing apps with `pull_policy: always` on the migrator tag keep working.
- HTTP requests to `/migrator/*` are 301-redirected to `/manager/*`.
- The container reads `/var/lib/migrator/jobs.json` as a fallback if
  `/var/lib/manager/state.json` doesn't exist yet, then writes going
  forward to the new path. A one-shot log line announces the
  migration. To carry job history forward, additionally bind-mount the
  old `migrator-state` dataset at `/var/lib/migrator` (read-only is
  fine).

The migrator alias is dropped two releases after slice-0 ships.

## After install

In the JuiceMount Mac app → Preferences → Connection:

```
Redis URL:            redis://<your-truenas-ip>:30179/1
S3 Endpoint Override: http://<your-truenas-ip>:30151/zpool
```

Then click Save. The Mac NFS volume mounts at `/Volumes/zpool`.

## Diagnostic endpoints

- MinIO Console: `http://<truenas-ip>:30152`
- WebDAV (Finder Cmd+K): `http://<truenas-ip>:30180`
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
