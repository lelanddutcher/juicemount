#!/bin/sh
# router.sh run|apply — a Linux NAT router, INSIDE a lab router container.
#
#   run     apply the saved switches (/etc/lab/router.conf, kept across a
#           container restart), then stay up (the container's command)
#   apply   re-apply the switches now (lab.sh writes router.conf first)
#   uplink-down   take the internet-side link down, as when the internet
#           provider's line is down; "apply" brings it back
#
# The container's role ($ROUTER) fixes its addresses. router.conf sets the
# switches a case changes:
#
#   NAT=easy|hard     easy: MASQUERADE (keeps the source port where it can:
#                     endpoint-independent mapping, like most home routers).
#                     hard: MASQUERADE --random-fully (a new random port per
#                     destination: endpoint-dependent, "symmetric" NAT).
#   FWD_TCP, FWD_UDP  port forwards, "PORT>IP:PORT ..." (empty: none)
#   HAIRPIN=on|off    on: a LAN device that dials the router's public
#                     address is looped back to the forward's target
#   SPLIT_DNS         "NAME/IP": the LAN resolver answers NAME with IP (a
#                     router's "local DNS" entry). Empty: public answers.
#   UPLINK=direct|carrier   away-router only: carrier sends everything
#                     through cgn (carrier-grade NAT, a second NAT)
#   PORTMAP=on|off    a NAT-PMP responder on the LAN address (natpmp.py)
#   NAS_NODE=host|bridge  nas-host only: where the NAS node runs. bridge:
#                     41641/udp is published to the Manager. host: the node
#                     container listens on nas-host's own address, so
#                     nothing is published for it.
#
# nas-host also listens on 127.0.0.1 for each published port and relays to
# the container behind it, as Docker's userland proxy does on a NAS host.
# The node container (host networking) reaches the NAS services there.
#
# Inbound from the internet side reaches the LAN only as a reply or through
# a forward, as on a real router. Only this container's namespace changes.
set -eu

CONF=/etc/lab/router.conf
log() { echo "router[$ROUTER]: $*" >&2; }

case "$ROUTER" in
  nas-host)    LAN_IP=10.232.11.2  WAN_IP=10.232.10.5  GW=10.232.10.2 DNS=off ;;
  home-router) LAN_IP=10.232.10.2  WAN_IP=10.232.1.10  GW=10.232.1.2  DNS=on ;;
  away-router) LAN_IP=10.232.20.2  WAN_IP=10.232.2.10  GW=10.232.2.2  DNS=on ;;
  cgn)         LAN_IP=10.232.21.2  WAN_IP=10.232.2.20  GW=10.232.2.2  DNS=off ;;
  *) log "unknown ROUTER"; exit 2 ;;
esac

# Defaults: what the role does with no case applied.
NAT=easy FWD_TCP="" FWD_UDP="" HAIRPIN=off SPLIT_DNS="" UPLINK=direct PORTMAP=off NAS_NODE=host
# shellcheck disable=SC1090
[ ! -f "$CONF" ] || . "$CONF"
if [ "$ROUTER" = nas-host ]; then
  # Docker's published ports for the Link overlay, with Docker's hairpin.
  FWD_TCP="30193>10.232.11.10:8091" FWD_UDP="" HAIRPIN=on
  [ "$NAS_NODE" = host ] || FWD_UDP="41641>10.232.11.10:41641"
fi

# published_loopback: nas-host's 127.0.0.1 listeners for the published ports
# (docker-proxy), "PORT>IP:PORT".
PUBLISHED="30193>10.232.11.10:8091 30190>10.232.11.10:8080 30179>10.232.11.20:6379 30151>10.232.11.30:9000"
# Started once: they never change, and a restart would cut the node's
# connections, which a router reconfiguration on the LAN does not.
published_loopback() {
  pgrep -x socat >/dev/null && return 0
  for p in $PUBLISHED; do
    socat "TCP-LISTEN:${p%%>*},bind=127.0.0.1,reuseaddr,fork" "TCP:${p#*>}" </dev/null >/dev/null 2>&1 &
  done
}

iface_for() {
  ip -o -4 addr show | awk -v ip="$1" '{ split($4, a, "/"); if (a[1] == ip) { sub(/@.*/, "", $2); print $2 } }'
}

apply() {
  lan_if="$(iface_for "$LAN_IP")"
  wan_if="$(iface_for "$WAN_IP")"
  [ -n "$lan_if" ] && [ -n "$wan_if" ] || { log "interfaces not found"; exit 2; }
  out_ip=$WAN_IP gw=$GW
  if [ "$ROUTER" = away-router ] && [ "$UPLINK" = carrier ]; then
    out_ip=10.232.21.10 gw=10.232.21.2
    ip link set "$wan_if" down
  else
    ip link set "$wan_if" up
  fi
  out_if="$(iface_for "$out_ip")"
  for dev in /sys/class/net/*; do dev=${dev##*/}
    [ "$dev" = lo ] || ethtool -K "$dev" tso off gso off gro off >/dev/null 2>&1 || true
  done
  ip route replace default via "$gw" dev "$out_if"

  iptables -t nat -F
  iptables -F FORWARD
  iptables -P FORWARD ACCEPT
  masq="-j MASQUERADE"
  [ "$NAT" = easy ] || masq="-j MASQUERADE --random-fully"
  # shellcheck disable=SC2086
  iptables -t nat -A POSTROUTING -o "$out_if" $masq

  fwd() { # fwd PROTO "PORT>IP:PORT"
    port=${2%%>*} to=${2#*>}
    to_ip=${to%:*} to_port=${to##*:}
    if [ "$HAIRPIN" = on ]; then
      iptables -t nat -A PREROUTING -d "$out_ip" -p "$1" --dport "$port" -j DNAT --to-destination "$to"
      # The looped-back connection must come back through the router.
      iptables -t nat -A POSTROUTING -o "$lan_if" -d "$to_ip" -p "$1" --dport "$to_port" -j MASQUERADE
    else
      iptables -t nat -A PREROUTING -i "$out_if" -d "$out_ip" -p "$1" --dport "$port" -j DNAT --to-destination "$to"
    fi
  }
  for f in $FWD_TCP; do fwd tcp "$f"; done
  for f in $FWD_UDP; do fwd udp "$f"; done

  iptables -A FORWARD -i "$out_if" -o "$lan_if" -m conntrack --ctstate ESTABLISHED,RELATED,DNAT -j ACCEPT
  iptables -A FORWARD -i "$out_if" -o "$lan_if" -j DROP
  # Forget every mapping, as a router reboot or reconfiguration does.
  conntrack -F >/dev/null 2>&1 || true

  if [ "$DNS" = on ]; then
    pkill -x dnsmasq 2>/dev/null || true
    set -- --no-resolv --no-hosts --listen-address="$LAN_IP" --bind-interfaces \
      --server=10.232.3.53 --cache-size=0 --pid-file=/run/dnsmasq.pid
    [ -z "$SPLIT_DNS" ] || set -- "$@" --address="/${SPLIT_DNS%%/*}/${SPLIT_DNS#*/}"
    dnsmasq "$@"
  fi

  pkill -f natpmp.py 2>/dev/null || true
  if [ "$PORTMAP" = on ]; then
    python3 /lab/lib/natpmp.py --lan-ip "$LAN_IP" --public-ip "$out_ip" --out-if "$out_if" \
      >>/var/log/natpmp.log 2>&1 &
  fi
  [ "$ROUTER" != nas-host ] || published_loopback
  log "applied: NAT=$NAT FWD_TCP='$FWD_TCP' FWD_UDP='$FWD_UDP' HAIRPIN=$HAIRPIN SPLIT_DNS='$SPLIT_DNS' UPLINK=$UPLINK PORTMAP=$PORTMAP NAS_NODE=$NAS_NODE"
}

case "${1:-}" in
  run)
    apply
    touch /run/router.ready
    exec sleep infinity ;;
  apply) apply ;;
  uplink-down)
    wan_if="$(iface_for "$WAN_IP")"
    [ -n "$wan_if" ] || { log "uplink interface not found"; exit 2; }
    ip link set "$wan_if" down
    conntrack -F >/dev/null 2>&1 || true
    log "uplink down ($wan_if)" ;;
  *) echo "usage: router.sh run|apply|uplink-down" >&2; exit 2 ;;
esac
