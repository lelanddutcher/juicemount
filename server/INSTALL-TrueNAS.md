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

Generate a strong MinIO password and JuiceMount admin key:

```
openssl rand -base64 24   # for MINIO_ROOT_PASSWORD
openssl rand -hex 32      # for JM_ADMIN_KEY
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
        juicefs format --storage minio --bucket "$$JM_BUCKET_URL" \
          --access-key "$$MINIO_ROOT_USER" --secret-key "$$MINIO_ROOT_PASSWORD" \
          --trash-days 0 "$$JM_META_URL" "$$JM_VOL_NAME" || { echo "[format] FAIL: juicefs format errored" >&2; exit 6; }
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
```

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
