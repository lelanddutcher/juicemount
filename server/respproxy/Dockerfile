# jm-respproxy: the H-8 option B RESP proxy (cmd/jm-respproxy) in front of
# the JuiceFS metadata Redis. Built from the repository root:
#
#   docker build -f server/respproxy/Dockerfile \
#     --build-arg JM_VERSION=<x.y.z> --build-arg JM_COMMIT=<40-hex> .
#
# The runtime base is the same digest-pinned redis:7.4-alpine image the
# compose runs, for busybox, jq and redis-cli (operator debugging only).

ARG JM_VERSION
ARG JM_COMMIT
FROM golang:1.26.6-alpine AS builder
ARG JM_VERSION
ARG JM_COMMIT
WORKDIR /src
RUN apk add --no-cache git
RUN test -n "$JM_VERSION" \
    && echo "$JM_COMMIT" | grep -Eq '^[0-9a-f]{40}$'
COPY go.mod go.sum ./
COPY third_party/juicefs ./third_party/juicefs
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w -X github.com/lelanddutcher/juicemount/internal/version.Version=${JM_VERSION} -X github.com/lelanddutcher/juicemount/internal/version.Commit=${JM_COMMIT}" \
      -o /out/jm-respproxy ./cmd/jm-respproxy

FROM redis:7.4-alpine@sha256:ff02b58f971e7d7d156a1267e283fcbbeee91773b6aa36c49dac28ecfe28eadf
ARG JM_VERSION
ARG JM_COMMIT
LABEL org.opencontainers.image.version="${JM_VERSION}" \
      org.opencontainers.image.revision="${JM_COMMIT}"
RUN apk add --no-cache jq flock
COPY --from=builder /out/jm-respproxy /usr/local/bin/jm-respproxy
COPY server/respproxy/entrypoint.sh /usr/local/bin/jm-respproxy-entrypoint
RUN chmod 0755 /usr/local/bin/jm-respproxy-entrypoint \
    && mkdir -p /etc/jm-respproxy /run/jm-respproxy
EXPOSE 6379
# The stats listener only comes up after the principals file loaded and the
# client listener bound (cmd/jm-respproxy/main.go), so it is a readiness probe.
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:9180/stats || exit 1
ENTRYPOINT ["/usr/local/bin/jm-respproxy-entrypoint"]
