# Standalone JuiceMount Manager container.
#
# This image is for users who run the manager WITHOUT juicemount-server
# on the same host (e.g. TrueNAS users whose NFS gateway lives on the
# Mac client). Users running juicemount-server already get the manager
# embedded at /manager/ on the existing metrics port — no separate
# container needed.
#
# Standalone mode talks to Redis + MinIO directly via jfs://<vol> URLs.
# Network reachability to MinIO is required from inside this container
# — typically by attaching it to the juicemount stack's docker network
# (see docs/INSTALL-TrueNAS.md for the YAML snippet).

# ----- Stage 1: builder -----
ARG JM_VERSION=0.5.0
ARG JM_COMMIT=unknown
FROM golang:1.26.6-alpine AS builder
ARG JM_VERSION
ARG JM_COMMIT

WORKDIR /src

RUN apk add --no-cache git build-base

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO not required (no sqlite, no FUSE in this binary). Fully static.
# No cache mounts — stale Go build cache served old binaries even with
# --no-cache (BuildKit cache mounts persist independently of layer cache).
RUN CGO_ENABLED=0 GOOS=linux \
    go build -ldflags="-s -w -X github.com/lelanddutcher/juicemount/internal/version.Version=${JM_VERSION} -X github.com/lelanddutcher/juicemount/internal/version.Commit=${JM_COMMIT}" \
      -o /out/juicemount-manager ./cmd/juicemount-manager

# ----- Stage 2: runtime -----
# juicedata/mount gives us /usr/local/bin/juicefs for the sync subprocess.
FROM juicedata/mount:ce-v1.3.1
ARG JM_VERSION
ARG JM_COMMIT
LABEL org.opencontainers.image.version="${JM_VERSION}" \
      org.opencontainers.image.revision="${JM_COMMIT}"

# Headscale (Tier-2 Link): fetched at build time so the manager can
# supervise it as a child process — one container, one deploy. Pinned
# version; v0.29.x speaks the 2026-era tsnet client protocol (0.26 rejects
# noise handshakes from tailscale v1.102 clients — live-verified).
RUN curl -fsSL https://github.com/juanfont/headscale/releases/download/v0.29.3/headscale_0.29.3_linux_amd64 \
      -o /usr/local/bin/headscale && chmod +x /usr/local/bin/headscale

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/juicemount-manager /usr/local/bin/juicemount-manager

RUN mkdir -p /sources /data/headscale

ENV JM_LISTEN=0.0.0.0:8080 \
    JM_DEST_MOUNT=/jfs \
    JM_SOURCE_ROOTS=/sources \
    JM_VOL_NAME=zpool

EXPOSE 8080

# Manager liveness alone is not release readiness when Link is enabled. The
# prior check stayed green after the supervised Headscale child missed its
# startup deadline, leaving paired Macs with listening loopback proxies that
# could not forward Redis or object-store traffic. Keep Link optional, but
# require its complete control/route/data plane in opted-in deployments.
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD printf 'header = "X-JuiceMount-Admin-Key: %s"\n' "$JM_ADMIN_KEY" \
          | curl --config - -fsS http://localhost:8080/api/sources >/dev/null \
      && { [ "$JM_NET_HEADSCALE" != "on" ] \
        || printf 'header = "X-JuiceMount-Admin-Key: %s"\n' "$JM_ADMIN_KEY" \
          | curl --config - -fsS http://localhost:8080/api/net/link \
          | grep -Eq '"data_plane_ready"[[:space:]]*:[[:space:]]*true'; } \
      || exit 1

# Default invocation: standalone mode. JM_META is REQUIRED at runtime
# (e.g. JM_META=redis://<server-ip>:30179/1). To switch to embedded
# mode, override with `command: ["--fuse-mount=/jfs"]` in compose AND
# pre-mount the JuiceFS volume at /jfs (cap_add SYS_ADMIN, /dev/fuse).
#
# EMBEDDED MODE also needs JM_OVERVIEW_META=redis://redis:6379/1 — in
# embedded mode --meta must stay empty (mutually exclusive with
# --fuse-mount), so the Overview tab, Trash retention (juicefs config
# --trash-days), and the GC/FSCK/compact Maintenance levers all read the
# volume through --overview-meta. Without it the Overview cards read
# "metaURL not configured", the Trash retention shows "unknown", and the
# metadata-side Maintenance buttons 501. Standalone mode leaves it empty
# (--meta already serves both).
ENTRYPOINT ["/bin/sh", "-c", "exec /usr/local/bin/juicemount-manager \
    --listen=\"$JM_LISTEN\" \
    --dest-mount=\"$JM_DEST_MOUNT\" \
    --source-roots=\"$JM_SOURCE_ROOTS\" \
    --vol-name=\"$JM_VOL_NAME\" \
    --meta=\"${JM_META:-}\" \
    --overview-meta=\"${JM_OVERVIEW_META:-}\" \
    \"$@\"", "--"]
