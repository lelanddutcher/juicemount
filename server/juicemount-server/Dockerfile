# JuiceMount server-side container (tier-3 iter 2).
#
# Two-stage build:
#   1. Builder — compiles cmd/jm5 for linux/amd64. Module-cache mount
#      keeps the build fast on rebuild.
#   2. Runtime — built on juicedata/mount which already ships the
#      juicefs binary the FUSEManager spawns at startup. Copying the
#      jm5 binary on top is all we need.
#
# Build context is the repo root (server/docker-compose.yml sets
# `context: ..` for this service). That lets the COPY pull from
# cmd/jm5 + every package jm5 imports.
#
# Build standalone for debugging:
#   docker build -f server/juicemount-server/Dockerfile -t jm-server:dev .
#
# Run standalone (won't be useful without redis + minio; use the
# compose stack for the real path):
#   docker run --rm --cap-add=SYS_ADMIN --device=/dev/fuse \
#     --security-opt apparmor:unconfined \
#     -p 2049:2049 -p 11050:11050 jm-server:dev

# ----- Stage 1: builder -----
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Tini for clean signal forwarding in stage 2 — easier to install in
# the builder where apk works freely than to add it later.
RUN apk add --no-cache git build-base

# Module cache first for layer reuse on source-only changes.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# CGO_ENABLED=1 because the metadata store and pin store both use
# mattn/go-sqlite3 which is a cgo binding. Static linking would skip
# the need for libc in the runtime image, but juicedata/mount is
# already alpine-based with libc available, so we accept the dynamic
# link.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /out/jm5 ./cmd/jm5

# ----- Stage 2: runtime -----
# Pin to a specific JuiceFS release for CI reproducibility. JuiceFS
# changed its tag scheme in mid-2025: the old `v1.2.3` form is gone,
# replaced by `ce-v1.3.1` (Community Edition prefix). Bump intentionally
# on each release rather than tracking `latest`.
FROM juicedata/mount:ce-v1.3.1

# Tools the entrypoint needs: curl for healthcheck, ca-certificates
# for any future S3 over HTTPS. The base image is alpine-derived.
RUN apk add --no-cache curl ca-certificates

# Drop the jm5 binary in. The base image already has /usr/local/bin
# on PATH; juicefs lives there too so PATH lookups inside the
# FUSEManager just work.
COPY --from=builder /out/jm5 /usr/local/bin/jm5

# Persistent state goes under /data. The host bind-mounts this in
# docker-compose. SQLite WAL files + pin.db all live here.
#
# /mnt/juicefs is the JuiceFS FUSE mount point (JM_FUSE_PATH).
# /mnt/nfs-export is the JuiceMount NFS-side mount point (JM_MOUNT).
# jm5 expects both to exist before launch.
RUN mkdir -p /data /mnt/juicefs /mnt/nfs-export

# Container-internal default paths. Overridable via CMD/ENV in
# docker-compose for power users.
ENV JM_REDIS=redis://redis:6379/1 \
    JM_FUSE_PATH=/mnt/juicefs \
    JM_MOUNT=/mnt/nfs-export \
    JM_LISTEN=0.0.0.0:2049 \
    JM_METRICS=0.0.0.0:11050 \
    JM_DB=/data/metadata.db \
    JM_CACHE_SIZE_MB=100000

EXPOSE 2049 11050

# Healthcheck hits the JuiceMount /health endpoint inside the container.
# The endpoint returns 200 when healthy / 503 when degraded; either
# means jm5 is at least responsive. Docker treats 503 as unhealthy
# automatically (any non-200), which is correct — degraded state
# should trigger restart policy or alerts.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://localhost:11050/health || exit 1

# jm5 needs to skip the macOS-specific mount_nfs path with --no-mount.
# Linux clients mount via TCP to the container's exposed 2049 port
# from outside. Inside the container, FUSE mounts at JM_FUSE_PATH via
# the existing FUSEManager → juicefs subprocess.
#
# /bin/sh -c so the ENV vars expand. The shell exec replaces itself
# with jm5 so signals (SIGTERM from docker stop) reach the process
# directly without a tini-style wrapper.
ENTRYPOINT ["/bin/sh", "-c", "exec /usr/local/bin/jm5 \
    --redis=\"$JM_REDIS\" \
    --fuse-path=\"$JM_FUSE_PATH\" \
    --mount=\"$JM_MOUNT\" \
    --listen=\"$JM_LISTEN\" \
    --metrics-addr=\"$JM_METRICS\" \
    --db=\"$JM_DB\" \
    --cache-size=\"$JM_CACHE_SIZE_MB\" \
    --no-mount \
    --log-level=info"]
