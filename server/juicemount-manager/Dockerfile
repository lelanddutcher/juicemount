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
FROM golang:1.26-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git build-base

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# CGO not required (no sqlite, no FUSE in this binary). Fully static.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build -ldflags="-s -w" \
      -o /out/juicemount-manager ./cmd/juicemount-manager

# ----- Stage 2: runtime -----
# juicedata/mount gives us /usr/local/bin/juicefs for the sync subprocess.
FROM juicedata/mount:ce-v1.3.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/juicemount-manager /usr/local/bin/juicemount-manager

RUN mkdir -p /sources

ENV JM_LISTEN=0.0.0.0:8080 \
    JM_DEST_MOUNT=/jfs \
    JM_SOURCE_ROOTS=/sources \
    JM_VOL_NAME=zpool

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:8080/api/sources || exit 1

# Default invocation: standalone mode. JM_META is REQUIRED at runtime
# (e.g. JM_META=redis://192.168.0.197:30179/1). To switch to embedded
# mode, override with `command: ["--fuse-mount=/jfs"]` in compose AND
# pre-mount the JuiceFS volume at /jfs (cap_add SYS_ADMIN, /dev/fuse).
ENTRYPOINT ["/bin/sh", "-c", "exec /usr/local/bin/juicemount-manager \
    --listen=\"$JM_LISTEN\" \
    --dest-mount=\"$JM_DEST_MOUNT\" \
    --source-roots=\"$JM_SOURCE_ROOTS\" \
    --vol-name=\"$JM_VOL_NAME\" \
    --meta=\"${JM_META:-}\" \
    --admin-key=\"${JM_ADMIN_KEY:-}\" \
    \"$@\"", "--"]
