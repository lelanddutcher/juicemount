#!/usr/bin/env bash
# mac-lab.sh — put a macOS VM behind the remote lab's emulated WAN.
#
#   mac-lab.sh up               start the VM, start the lab with the WireGuard
#                               gateway, and connect the VM to it (lan profile)
#   mac-lab.sh profile NAME     switch the WAN profile (profiles.tsv); no restart
#   mac-lab.sh smoke [NAME...]  link smoke test from the VM (default: lan abroad)
#   mac-lab.sh guest CMD...     run a command in the VM
#   mac-lab.sh status           lab containers, wan qdiscs, tunnel state
#   mac-lab.sh down             disconnect the VM, remove the lab, stop the VM
#
# The VM reaches the server stack only through a WireGuard tunnel to the
# gateway container, which sits on the lab's remote network next to the Linux
# client. Every packet to and from the stack crosses the wan container, so it
# gets the current profile in both directions. See README.md, "macOS VM".
#
# Required environment (no defaults; nothing about your network is in this repo):
#   JMLAB_MAC_HOST      SSH destination of the VM host, for example user@host.
#                       Its SSH command must accept start | stop | status and
#                       allow jumping (-W) to the VM's SSH port. `start` prints
#                       the VM's IP address on its last line.
#   JMLAB_MAC_KEY       SSH private key for the VM host and the VM.
#   JMLAB_MAC_USER      user in the VM; needs sudo without a password and Homebrew
#                       (up installs wireguard-tools and iperf3 with it).
#   JMLAB_MAC_ENDPOINT  address of THIS Docker host as the VM can reach it.
# Optional:
#   JMLAB_MAC_WG_PORT   UDP port published for WireGuard (default 51820)
#   JMLAB_MAC_BIND      host address the port is published on (default 0.0.0.0)
#   JMLAB_MAC_KEEP_VM=1 leave the VM running after down
#   JMLAB_MAC_SMOKE_MB  size of the smoke download (default 100)
#   JMLAB_MAC_SMOKE_MAX time limit per smoke transfer in seconds (default 600)
#   JMLAB_BBR=1         at up: the server's opt-in BBR overlay, as in lab.sh
# The lab uses its own Docker project (jmmac), state directory (.state-mac) and
# subnets (10.231.110-130.0/24), so it can run next to a lab.sh/measure.py lab.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export JMLAB_PROJECT="${JMLAB_PROJECT:-jmmac}"
export JMLAB_STATE_DIR="${JMLAB_STATE_DIR:-.state-mac}"
export JMLAB_NAS_SUBNET="${JMLAB_NAS_SUBNET:-10.231.110.0/24}" JMLAB_NAS_DYNAMIC="${JMLAB_NAS_DYNAMIC:-10.231.110.128/25}"
export JMLAB_REMOTE_SUBNET="${JMLAB_REMOTE_SUBNET:-10.231.120.0/24}" JMLAB_REMOTE_DYNAMIC="${JMLAB_REMOTE_DYNAMIC:-10.231.120.128/25}"
export JMLAB_PROXY_IP="${JMLAB_PROXY_IP:-10.231.110.20}" JMLAB_SILO_IP="${JMLAB_SILO_IP:-10.231.110.30}"
export JMLAB_IPERF_IP="${JMLAB_IPERF_IP:-10.231.110.40}" JMLAB_WAN_NAS_IP="${JMLAB_WAN_NAS_IP:-10.231.110.2}"
export JMLAB_WAN_REMOTE_IP="${JMLAB_WAN_REMOTE_IP:-10.231.120.2}" JMLAB_CLIENT_IP="${JMLAB_CLIENT_IP:-10.231.120.10}"
export JMLAB_GATEWAY_IP="${JMLAB_GATEWAY_IP:-10.231.120.20}"
export JMLAB_TUNNEL_SUBNET="${JMLAB_TUNNEL_SUBNET:-10.231.130.0/24}" JMLAB_TUNNEL_GW_IP="${JMLAB_TUNNEL_GW_IP:-10.231.130.1}"
export JMLAB_MAC_WG_PORT="${JMLAB_MAC_WG_PORT:-51820}" JMLAB_MAC_BIND="${JMLAB_MAC_BIND:-0.0.0.0}"
GUEST_TUN_IP="${JMLAB_TUNNEL_GUEST_IP:-10.231.130.2}"
STATE="$HERE/$JMLAB_STATE_DIR"
LAB="$HERE/lab.sh"
WG_NAME=jmlab   # the VM's wg-quick config name (jmlab.conf)

log() { printf '[mac-lab] %s\n' "$*" >&2; }
die() { log "$*"; exit 2; }

need_env() {
  local v
  for v in JMLAB_MAC_HOST JMLAB_MAC_KEY JMLAB_MAC_USER JMLAB_MAC_ENDPOINT; do
    [ -n "${!v:-}" ] || die "set $v (see the top of mac-lab.sh)"
  done
  [ -r "$JMLAB_MAC_KEY" ] || die "JMLAB_MAC_KEY is not readable"
}

host_ssh() { ssh -i "$JMLAB_MAC_KEY" -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=15 "$JMLAB_MAC_HOST" "$@"; }

guest_ip() {
  [ -s "$STATE/guest.ip" ] || die "no VM address recorded: run mac-lab.sh up"
  cat "$STATE/guest.ip"
}

guest() {
  local ip proxy
  ip="$(guest_ip)"
  proxy="ssh -i $(printf %q "$JMLAB_MAC_KEY") -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=15 $(printf %q "$JMLAB_MAC_HOST") -W %h:%p"
  ssh -i "$JMLAB_MAC_KEY" -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=20 \
    -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$STATE/known_hosts" \
    -o ProxyCommand="$proxy" "$JMLAB_MAC_USER@$ip" "$@"
}

# The guest's PATH over SSH lacks Homebrew; wg-quick needs its bash 4+.
# The guest scripts below are single-quoted on purpose: they expand in the VM.
# shellcheck disable=SC2016
BREW_ENV='eval "$(/opt/homebrew/bin/brew shellenv 2>/dev/null || /usr/local/bin/brew shellenv)"'

wg_tool() { docker run --rm -i --network none --entrypoint wg jmlab:local "$@"; }

write_keys() {
  local gw_priv gw_pub vm_priv vm_pub
  # Keep the keys of a running lab: the gateway read them at its start.
  [ ! -s "$STATE/gateway.conf" ] || [ ! -s "$STATE/guest.conf" ] || return 0
  log "generating throwaway WireGuard keys"
  gw_priv="$(wg_tool genkey)"; gw_pub="$(printf '%s' "$gw_priv" | wg_tool pubkey)"
  vm_priv="$(wg_tool genkey)"; vm_pub="$(printf '%s' "$vm_priv" | wg_tool pubkey)"
  (
    umask 077
    # wg setconf format (no Address lines): read by the gateway container.
    printf '[Interface]\nPrivateKey = %s\nListenPort = 51820\n\n[Peer]\nPublicKey = %s\nAllowedIPs = %s/32\n' \
      "$gw_priv" "$vm_pub" "$GUEST_TUN_IP" > "$STATE/gateway.conf"
    # wg-quick format: installed in the VM. Only the nas subnet goes into
    # the tunnel; the VM's other traffic keeps its own route.
    printf '[Interface]\nPrivateKey = %s\nAddress = %s/32\nMTU = 1420\n\n[Peer]\nPublicKey = %s\nEndpoint = %s:%s\nAllowedIPs = %s\nPersistentKeepalive = 25\n' \
      "$vm_priv" "$GUEST_TUN_IP" "$gw_pub" "$JMLAB_MAC_ENDPOINT" "$JMLAB_MAC_WG_PORT" "$JMLAB_NAS_SUBNET" > "$STATE/guest.conf"
  )
  # Read by the gateway's root through the read-only /lab mount.
  chmod 644 "$STATE/gateway.conf"
}

# shellcheck disable=SC2016
guest_tunnel_up() {
  log "installing the tunnel in the VM"
  guest "$BREW_ENV"'; set -e
    for f in wireguard-tools iperf3; do
      brew list --formula "$f" >/dev/null 2>&1 || HOMEBREW_NO_AUTO_UPDATE=1 brew install -q "$f" >/dev/null
    done
    d="$(brew --prefix)/etc/wireguard"; sudo install -d -m 700 "$d"
    sudo env PATH="$PATH" wg-quick down "$d/'"$WG_NAME"'.conf" >/dev/null 2>&1 || true
    sudo sh -c '\''umask 077; cat > "$1"'\'' _ "$d/'"$WG_NAME"'.conf"
    sudo env PATH="$PATH" wg-quick up "$d/'"$WG_NAME"'.conf" 2>&1 | grep -v "^\[#\]" || true' < "$STATE/guest.conf"
  for _ in $(seq 20); do
    if guest ping -c 1 -t 2 "$JMLAB_SILO_IP" >/dev/null 2>&1; then
      log "VM reaches the object store at $JMLAB_SILO_IP through the tunnel"
      return 0
    fi
    sleep 1
  done
  guest "$BREW_ENV; sudo wg show" >&2 || true
  die "the VM cannot reach $JMLAB_SILO_IP through the tunnel"
}

# shellcheck disable=SC2016
guest_tunnel_down() {
  guest "$BREW_ENV"'; d="$(brew --prefix)/etc/wireguard"
    if [ -f "$d/'"$WG_NAME"'.conf" ]; then
      sudo env PATH="$PATH" wg-quick down "$d/'"$WG_NAME"'.conf" >/dev/null 2>&1 || true
      sudo rm -f "$d/'"$WG_NAME"'.conf"
    fi' || log "could not reach the VM to remove the tunnel"
}

# A public-read/write bucket with one random object of JMLAB_MAC_SMOKE_MB MB.
# Lab only: the bucket and the whole object store go away at down.
smoke_bucket() {
  local mb="${JMLAB_MAC_SMOKE_MB:-100}" cid user pass
  cid="$(JMLAB_STATE_DIR="$JMLAB_STATE_DIR" "$LAB" _cid silo)"
  user="$(sed -n 's/^JMLAB_S3_USER=//p' "$STATE/lab.env")"
  pass="$(sed -n 's/^JMLAB_S3_PASSWORD=//p' "$STATE/lab.env")"
  docker exec -e U="$user" -e P="$pass" -e MB="$mb" "$cid" sh -ec '
    mc alias set lab http://127.0.0.1:9000 "$U" "$P" >/dev/null
    mc mb -p lab/jmlab-smoke >/dev/null
    mc anonymous set public lab/jmlab-smoke >/dev/null
    if ! mc stat "lab/jmlab-smoke/blob-${MB}M" >/dev/null 2>&1; then
      head -c "${MB}M" /dev/urandom | mc pipe "lab/jmlab-smoke/blob-${MB}M" >/dev/null
    fi'
}

cmd="${1:-}"; shift || true
case "$cmd" in
  up)
    need_env
    command -v docker >/dev/null || die "docker is required"
    mkdir -p "$STATE"; chmod 700 "$STATE"
    log "starting the VM"
    ip="$(host_ssh start | tail -n 1 | tr -d '[:space:]')"
    [[ "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "the VM host's start printed no IPv4 address"
    printf '%s\n' "$ip" > "$STATE/guest.ip"
    for _ in $(seq 60); do guest true 2>/dev/null && break; sleep 2; done
    guest true || die "cannot SSH into the VM"
    log "building the lab image"
    docker build -q -f "$HERE/image/Dockerfile" -t jmlab:local "$HERE/../.." >/dev/null
    write_keys
    JMLAB_MAC=1 "$LAB" up
    guest_tunnel_up
    log "up: the VM reaches the nas subnet $JMLAB_NAS_SUBNET only through the wan (profile lan)"
    log "RESP proxy $JMLAB_PROXY_IP:6379, object store http://$JMLAB_SILO_IP:9000"
    ;;
  profile)
    [ $# -eq 1 ] || die "usage: mac-lab.sh profile NAME"
    "$LAB" shape "$1"
    ;;
  smoke)
    need_env
    [ $# -gt 0 ] || set -- lan abroad
    for p in "$@"; do awk -v p="$p" '$1 == p { f = 1 } END { exit !f }' "$HERE/profiles.tsv" || die "unknown profile $p"; done
    smoke_bucket
    out="$HERE/results/mac-$(date -u +%Y%m%dT%H%M%SZ)"
    mkdir -p "$out"
    for p in "$@"; do
      "$LAB" shape "$p" >&2
      sleep 1
      log "smoke on $p"
      # shellcheck disable=SC2046 # the profile's two rate caps, as two words
      guest /usr/bin/python3 - "$p" "$JMLAB_PROXY_IP" "$JMLAB_SILO_IP" "$JMLAB_IPERF_IP" \
        $(awk -v p="$p" '$1 == p { print $5, $6; exit }' "$HERE/profiles.tsv") \
        "${JMLAB_MAC_SMOKE_MB:-100}" "${JMLAB_MAC_SMOKE_MAX:-600}" < "$HERE/lib/mac_smoke.py" | tee "$out/$p.json"
    done
    "$LAB" shape lan >&2
    python3 "$HERE/lib/mac_smoke.py" --table "$out"/*.json | tee "$out/smoke.md"
    log "results in $out"
    ;;
  guest)
    need_env
    guest "$@"
    ;;
  status)
    "$LAB" status
    docker exec "$("$LAB" _cid gateway)" wg show wg0 | sed -E 's/(public key: ).*/\1(hidden)/; s/^(peer: ).*/\1(hidden)/'
    ;;
  down)
    need_env
    if [ -s "$STATE/guest.ip" ] && [ "$(host_ssh status 2>/dev/null)" != stopped ]; then guest_tunnel_down; fi
    if [ -f "$STATE/lab.env" ]; then "$LAB" down; fi
    rm -rf "$STATE"
    if [ "${JMLAB_MAC_KEEP_VM:-0}" != 1 ]; then
      host_ssh stop >/dev/null || log "could not stop the VM"
      log "VM: $(host_ssh status 2>/dev/null || echo unknown)"
    fi
    ;;
  *) sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
