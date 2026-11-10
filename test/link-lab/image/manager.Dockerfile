# Link lab Manager image: the real juicemount-manager binary from this
# checkout (its Link code: embedded Headscale supervisor, ACL policy, NAS
# tailnet node) plus the pinned Headscale release, on a small runtime.
#
#   docker build -f test/link-lab/image/manager.Dockerfile -t jmlinklab-manager:local .
#
# It differs from server/juicemount-manager/Dockerfile only where the lab
# does not need the product image's parts: no patched juicefs (the lab
# formats no volume and runs no sync), no juicedata/mount base. The manager
# binary is built with the same tags, and Headscale is the same release
# (v0.29.3), here also checked against its published sha256.

FROM golang:1.26.10-alpine AS builder
ARG JM_COMMIT=0000000000000000000000000000000000000000
WORKDIR /src
COPY go.mod go.sum ./
COPY third_party/juicefs ./third_party/juicefs
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -tags nosqlite,nomysql,nopg,notikv,nobadger,noetcd \
      -ldflags="-s -w -X github.com/lelanddutcher/juicemount/internal/version.Version=0.0.0-linklab -X github.com/lelanddutcher/juicemount/internal/version.Commit=${JM_COMMIT}" \
      -o /out/juicemount-manager ./cmd/juicemount-manager

FROM alpine:3.22@sha256:49340b346798875b5592e5d420241f2f9cfba5f47e62ebb1446a1b34734e898f
ARG TARGETARCH
RUN apk add --no-cache ca-certificates curl
RUN case "$TARGETARCH" in \
      amd64) sum=8dc183758024ed7095cf610fedea0790233613c71353bc8be2715d82ba29b92c ;; \
      arm64) sum=ecf0099f9aa1efb56e7c74718342a493f7d44a840626a2877ca526e675040f4e ;; \
      *) echo "unsupported Headscale architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && curl -fsSL -o /usr/local/bin/headscale \
      "https://github.com/juanfont/headscale/releases/download/v0.29.3/headscale_0.29.3_linux_${TARGETARCH}" \
    && echo "$sum  /usr/local/bin/headscale" | sha256sum -c - \
    && chmod +x /usr/local/bin/headscale
COPY --from=builder /out/juicemount-manager /usr/local/bin/juicemount-manager
RUN mkdir -p /sources /data/headscale
ENTRYPOINT ["/usr/local/bin/juicemount-manager"]
