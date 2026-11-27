#!/usr/bin/env bash
# Link-class lab: how the app classifies the lab link (class, read class,
# high-latency flag, RTT and bandwidth estimates, juicefs live timeouts), and
# what one sequential read gets on it.
#
#   link-class-lab.sh build [BIN]       build linkclassprobe into $STATE/BIN
#                                       (default bin; another name keeps an
#                                       older build for a before/after pair)
#   link-class-lab.sh tunnel up|down    route the client to the NAS through a
#                                       WireGuard interface named tailscale0
#   link-class-lab.sh fixture           write the clips (on lan, no tunnel needed)
#   link-class-lab.sh run PROFILE [LABEL] [PROBE_FLAG...]
#
# The probe (linkclassprobe) runs the app's own code inside the client: the
# backend route lookup and tunnel ceiling, the mount's link sample, the
# reachability monitor with its RTT observer, and the NFS layer's read-ahead
# with timed 1 MiB reads feeding the bandwidth estimate. It samples the
# profile every second: LCP_IDLE seconds idle (default 20), then a
# sequential read of the cold clip for LCP_READ seconds (default 25).
#
# The tunnel stands in for Tailscale or another VPN on a Mac (utunN): the
# route to the NAS goes out of an interface the app treats as a tunnel, so
# its route ceiling fires. WireGuard runs in userspace (wireguard-go) in two
# helper containers that share the network namespaces of the wan and the
# client; nothing on the host changes. The tunnel's UDP packets cross the
# wan, so they get the profile's delay, loss and rate. MTU 1280, as
# Tailscale uses.
#
# One run: shape the link, remount the client cold, start the probe. Output
# goes to results/link-class-<time>/<label>.json (LCP_OUT overrides).
#
#   ./lab.sh up && experiments/link-class-lab.sh build && \
#     experiments/link-class-lab.sh fixture && \
#     experiments/link-class-lab.sh tunnel up && \
#     experiments/link-class-lab.sh run fast-remote
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
LAB="$HERE/lab.sh"
STATE_NAME="${JMLAB_STATE_DIR:-.state}"
STATE="$HERE/$STATE_NAME"
PROXY_IP="${JMLAB_PROXY_IP:-10.231.10.20}"
NAS_SUBNET="${JMLAB_NAS_SUBNET:-10.231.10.0/24}"
WAN_REMOTE_IP="${JMLAB_WAN_REMOTE_IP:-10.231.20.2}"
TUN_SUBNET="${JMLAB_LCP_TUN_SUBNET:-10.231.31.0/24}"
TUN_WAN_IP="${TUN_SUBNET%.*/*}.1"
TUN_CLIENT_IP="${TUN_SUBNET%.*/*}.10"
CLIP="${LCP_CLIP:-linkclass/clip-1g.mov}"
OUT="${LCP_OUT:-$HERE/results/link-class-$(date +%Y%m%d-%H%M%S)}"

log() { printf '[link-class] %s\n' "$*" >&2; }
die() { log "$*"; exit 2; }
cid() { "$LAB" _cid "$1"; }
helper() { printf '%s-lcp-%s' "$JMLAB_PROJECT" "$1"; }
export JMLAB_PROJECT="${JMLAB_PROJECT:-jmlab}"

build() {
  local bin="${1:-bin}"
  mkdir -p "$STATE/$bin"
  chmod 755 "$STATE" "$STATE/$bin" # the client's root reads it through /lab
  (cd "$ROOT" && CGO_ENABLED=0 go build -trimpath -tags nosqlite,nomysql,nopg,notikv,nobadger,noetcd \
    -o "$STATE/$bin/linkclassprobe" ./test/remote-lab/linkclassprobe)
  log "built linkclassprobe into $STATE_NAME/$bin from $(git -C "$ROOT" rev-parse --short HEAD)"
}

wg_tool() { docker run --rm -i --network none --entrypoint wg jmlab:local "$@"; }

tunnel_up() {
  local wan client wpriv wpub cpriv cpub
  wan="$(cid wan)"; client="$(cid client)"
  [ -n "$wan" ] && [ -n "$client" ] || die "the lab is not up"
  tunnel_down quiet
  wpriv="$(wg_tool genkey)"; wpub="$(printf '%s' "$wpriv" | wg_tool pubkey)"
  cpriv="$(wg_tool genkey)"; cpub="$(printf '%s' "$cpriv" | wg_tool pubkey)"
  (
    umask 077
    printf '[Interface]\nPrivateKey = %s\nListenPort = 51820\n\n[Peer]\nPublicKey = %s\nAllowedIPs = %s/32\n' \
      "$wpriv" "$cpub" "$TUN_CLIENT_IP" > "$STATE/lcp-wan.conf"
    printf '[Interface]\nPrivateKey = %s\n\n[Peer]\nPublicKey = %s\nEndpoint = %s:51820\nAllowedIPs = %s\nPersistentKeepalive = 25\n' \
      "$cpriv" "$wpub" "$WAN_REMOTE_IP" "$NAS_SUBNET" > "$STATE/lcp-client.conf"
  )
  # The wan end: wg0, its tunnel traffic masqueraded toward the NAS like the
  # remote network's (compose.yml).
  # shellcheck disable=SC2016
  docker run -d --name "$(helper wan)" --network "container:$wan" --cap-add NET_ADMIN --device /dev/net/tun \
    -v "$STATE/lcp-wan.conf:/wg.conf:ro" -e TUN_SUBNET="$TUN_SUBNET" -e TUN_IP="$TUN_WAN_IP" \
    --entrypoint /bin/sh jmlab:local -ec '
      wireguard-go -f wg0 & pid=$!
      for i in $(seq 50); do ip link show wg0 >/dev/null 2>&1 && break; sleep 0.1; done
      wg setconf wg0 /wg.conf
      ip addr add "$TUN_IP/${TUN_SUBNET#*/}" dev wg0
      ip link set wg0 mtu 1280 up
      ethtool -K wg0 tso off gso off gro off 2>/dev/null || true
      iptables -t nat -C POSTROUTING -s "$TUN_SUBNET" -j MASQUERADE 2>/dev/null ||
        iptables -t nat -A POSTROUTING -s "$TUN_SUBNET" -j MASQUERADE
      iptables -t mangle -C FORWARD -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --clamp-mss-to-pmtu 2>/dev/null ||
        iptables -t mangle -A FORWARD -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --clamp-mss-to-pmtu
      touch /run/lcp.ready
      wait "$pid"' >/dev/null
  # The client end: tailscale0, carrying the whole nas subnet.
  # shellcheck disable=SC2016
  docker run -d --name "$(helper client)" --network "container:$client" --cap-add NET_ADMIN --device /dev/net/tun \
    -v "$STATE/lcp-client.conf:/wg.conf:ro" -e NAS_SUBNET="$NAS_SUBNET" -e TUN_SUBNET="$TUN_SUBNET" \
    -e TUN_IP="$TUN_CLIENT_IP" --entrypoint /bin/sh jmlab:local -ec '
      wireguard-go -f tailscale0 & pid=$!
      for i in $(seq 50); do ip link show tailscale0 >/dev/null 2>&1 && break; sleep 0.1; done
      wg setconf tailscale0 /wg.conf
      ip addr add "$TUN_IP/${TUN_SUBNET#*/}" dev tailscale0
      ip link set tailscale0 mtu 1280 up
      ethtool -K tailscale0 tso off gso off gro off 2>/dev/null || true
      ip route replace "$NAS_SUBNET" dev tailscale0 src "$TUN_IP"
      touch /run/lcp.ready
      wait "$pid"' >/dev/null
  for _ in $(seq 50); do
    if docker exec "$(helper client)" test -f /run/lcp.ready 2>/dev/null &&
       docker exec "$(helper wan)" test -f /run/lcp.ready 2>/dev/null; then break; fi
    sleep 0.2
  done
  "$LAB" exec ping -c 3 -q "$PROXY_IP" >&2 || die "no answer through the tunnel"
  log "tunnel up: client reaches $NAS_SUBNET through tailscale0 ($TUN_CLIENT_IP)"
}

tunnel_down() {
  docker rm -f "$(helper client)" "$(helper wan)" >/dev/null 2>&1 || true
  local client
  client="$(cid client 2>/dev/null || true)"
  if [ -n "$client" ]; then
    "$LAB" exec ip route replace "$NAS_SUBNET" via "$WAN_REMOTE_IP" 2>/dev/null || true
  fi
  rm -f "$STATE/lcp-wan.conf" "$STATE/lcp-client.conf"
  [ "${1:-}" = quiet ] || log "tunnel down: client routes $NAS_SUBNET through the wan directly"
}

# fixture: the clip, plus two more 1 GiB clips and a 256 MiB clip in the
# same folder for the probe's -scrub-file, -read2-file and -ttfb-file
# (scrub-1g.mov, next-1g.mov, ttfb-256m.mov).
fixture() {
  local dir="${CLIP%/*}"
  "$LAB" shape lan >&2
  "$LAB" exec sh -ec "mkdir -p /jfs/$dir
    [ -s /jfs/$CLIP ] || head -c 1073741824 /dev/urandom > /jfs/$CLIP
    [ -s /jfs/$dir/scrub-1g.mov ] || head -c 1073741824 /dev/urandom > /jfs/$dir/scrub-1g.mov
    [ -s /jfs/$dir/next-1g.mov ] || head -c 1073741824 /dev/urandom > /jfs/$dir/next-1g.mov
    [ -s /jfs/$dir/ttfb-256m.mov ] || head -c 268435456 /dev/urandom > /jfs/$dir/ttfb-256m.mov
    ls -l /jfs/$dir" >&2
}

run() {
  local profile="$1" label="${2:-$1}"
  shift; [ $# -eq 0 ] || shift
  local bin="${LCP_BIN:-bin}"
  [ -x "$STATE/$bin/linkclassprobe" ] || die "run link-class-lab.sh build first"
  mkdir -p "$OUT"
  "$LAB" shape "$profile" >&2
  "$LAB" mount --cold >&2
  log "run $label on $profile ($bin)"
  "$LAB" exec "/lab/$STATE_NAME/$bin/linkclassprobe" -meta "$PROXY_IP:6379" -file "$CLIP" \
    -idle "${LCP_IDLE:-20}s" -read "${LCP_READ:-25}s" "$@" > "$OUT/$label.json"
  python3 - "$OUT/$label.json" <<'PY' >&2
import json, sys
r = json.load(open(sys.argv[1])); s = r["summary"]
print(f'{sys.argv[1].rsplit("/",1)[-1]}: route {r["route_interface"]} ceiling={r["route_ceiling"]} '
      f'class {s["classes"]} read {s["read_classes"]} high_latency {s["high_latency_samples"]}/{s["samples"]} '
      f'rtt_med {s["rtt_median_ms"]:.1f} ms bw_med {s["bw_median_mbit"]:.0f} last {s["bw_last_mbit"]:.0f} Mbit/s '
      f'seq {r["read_mbit"]:.0f} Mbit/s final {s["final_class"]}/{s["final_read_class"]} jfs {s["final_jfs_flags"]}')
PY
}

cmd="${1:-}"; shift || true
case "$cmd" in
  build) build "$@" ;;
  tunnel)
    case "${1:-}" in up) tunnel_up ;; down) tunnel_down ;; *) die "usage: link-class-lab.sh tunnel up|down" ;; esac ;;
  fixture) fixture ;;
  run) [ $# -ge 1 ] || die "usage: link-class-lab.sh run PROFILE [LABEL] [PROBE_FLAG...]"; run "$@" ;;
  *) sed -n '2,36p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
