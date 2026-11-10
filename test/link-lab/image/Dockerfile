# Link lab tools image: one image for every lab part that is not the NAS
# stack. The routers, the WAN, the lab DNS, the relay and both Mac stand-ins
# run it. Built from the repository root:
#
#   docker build -f test/link-lab/image/Dockerfile -t jmlinklab:local .
#
# It carries:
#   - the stock Tailscale client (tailscale, tailscaled), copied from the
#     official image at the same version as tailscale.com in go.mod;
#   - derper, Tailscale's DERP relay, built from the same module version;
#   - the router tools (iptables, conntrack, dnsmasq, tc) and the probes'
#     tools (curl, redis-cli, python3, openssl).

FROM golang:1.26.10-alpine AS derper
# Same version as tailscale.com in go.mod. go install resolves its own
# dependencies from the module's go.sum.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOBIN=/out go install -trimpath tailscale.com/cmd/derper@v1.102.3 \
    && /out/derper --version 2>/dev/null | head -1 || true

FROM tailscale/tailscale:v1.102.3@sha256:8c42c4574ab066384fcb72f69e086a2ff1dd3652eb6f56856cee34bcf0d2f680 AS tailscale

FROM alpine:3.22@sha256:49340b346798875b5592e5d420241f2f9cfba5f47e62ebb1446a1b34734e898f
RUN apk add --no-cache iproute2 iproute2-tc ethtool iptables conntrack-tools iputils \
      dnsmasq bind-tools curl openssl redis python3 jq ca-certificates coreutils
COPY --from=tailscale /usr/local/bin/tailscale /usr/local/bin/tailscaled /usr/local/bin/
COPY --from=derper /out/derper /usr/local/bin/derper
RUN tailscale version | head -1 && test -x /usr/local/bin/derper
