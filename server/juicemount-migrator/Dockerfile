# JuiceMount Migrator container.
#
# Wraps `juicefs sync` in a tiny HTTP service + web UI for one-click
# data migration from existing datasets into a JuiceFS volume.
#
# Designed to run as a sidecar of the JuiceMount stack, with:
#   - The user's existing media datasets bind-mounted read-only under
#     /sources (or any path passed to --source-roots)
#   - The JuiceFS volume mounted read-write at /jfs (via the
#     juicemount-server container or a standalone juicefs mount)
#
# Build context is the repo root (server/docker-compose.yml or the
# TrueNAS-app Jinja template sets `context: ..` for this service).
#
# Standalone debug build:
#   docker build -f server/juicemount-migrator/Dockerfile -t jm-migrator:dev .
#
# Standalone run:
#   docker run --rm -p 8080:8080 \
#     -v /mnt/source:/sources/source:ro \
#     -v /mnt/jfs:/jfs \
#     -e JM_ADMIN_KEY=replace-me \
#     jm-migrator:dev \
#     --meta redis://redis:6379/1 --source-roots /sources/source

# ----- Stage 1: builder -----
FROM golang:1.26-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git build-base

# Module cache for layer reuse on source-only changes.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# CGO not required for the migrator (no sqlite, no FUSE) so the
# resulting binary is fully static. amd64 build by default; CI's
# buildx handles the multi-arch matrix.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build -tags migrator_wip -ldflags="-s -w" \
      -o /out/juicemount-migrator ./cmd/juicemount-migrator

# ----- Stage 2: runtime -----
# juicedata/mount gives us the `juicefs` CLI at /usr/bin/juicefs which
# the migrator shells out to for `juicefs sync`. Without this we'd
# need to download or vendor juicefs separately.
FROM juicedata/mount:ce-v1.3.1

# curl for the healthcheck. ca-certificates for any future HTTPS-S3
# sync targets the user might add.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/juicemount-migrator /usr/local/bin/juicemount-migrator

# Stable paths used by docker-compose and the TrueNAS-app Jinja
# template. Users bind-mount their data here.
RUN mkdir -p /sources /jfs

# Container-internal defaults. Each is overridable via CLI flag or
# environment.
ENV JM_LISTEN=0.0.0.0:8080 \
    JM_DEST_MOUNT=/jfs \
    JM_SOURCE_ROOTS=/sources

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:8080/api/sources || exit 1

# Defaults are overridable via the command in docker-compose:
#   command: ["--meta", "redis://redis:6379/1", "--source-roots", "/sources/media"]
# Note: --meta is REQUIRED (no default) — fail-fast if not supplied.
ENTRYPOINT ["/bin/sh", "-c", "exec /usr/local/bin/juicemount-migrator \
    --listen=\"$JM_LISTEN\" \
    --dest-mount=\"$JM_DEST_MOUNT\" \
    --source-roots=\"$JM_SOURCE_ROOTS\" \
    --juicefs=/usr/bin/juicefs \
    --meta=\"${JM_META:-}\" \
    --admin-key=\"${JM_ADMIN_KEY:-}\" \
    \"$@\"", "--"]
