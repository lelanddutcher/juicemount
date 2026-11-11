# Remote lab image: the vendored juicefs client plus the network tools the
# lab needs (tc, ethtool, iptables, ping, iperf3, and userspace WireGuard for
# the macOS VM gateway) and python3 for the probe.
# The wan router, the remote client and the iperf3 server all run this one
# image. Built from the repository root:
#
#   docker build -f test/remote-lab/image/Dockerfile -t jmlab:local .
#
# juicefs is built exactly as .github/workflows/ci.yml builds it: the pinned
# ldflags from third_party/juicefs/SOURCE.json, the same build tags, and the
# same version check.

FROM golang:1.26.11-alpine AS juicefs
# zstd, lz4 and gspt need cgo; the runtime image is alpine (musl) too.
RUN apk add --no-cache gcc musl-dev
WORKDIR /src
COPY third_party/juicefs ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    ldflags="$(sed -n 's/^ *"ldflags": "\(.*\)",*$/\1/p' SOURCE.json)" \
    && CGO_ENABLED=1 go build -trimpath \
         -tags nocos,nobos,nohdfs,noibmcos,noobs,nooss,noqingstor,noswift,noazure,nogs,noufile,nodragonfly,nosqlite,nomysql,nopg,notikv,nobadger,noetcd \
         -ldflags="$ldflags" -o /out/juicefs . \
    && test "$(/out/juicefs --version)" = "$(sed -n 's/^ *"version_output": "\(.*\)",*$/\1/p' SOURCE.json)"

FROM alpine:3.22@sha256:49340b346798875b5592e5d420241f2f9cfba5f47e62ebb1446a1b34734e898f
RUN apk add --no-cache iproute2 iproute2-tc ethtool iptables iputils iperf3 python3 fuse3 coreutils \
      wireguard-go wireguard-tools-wg
COPY --from=juicefs /out/juicefs /usr/local/bin/juicefs
