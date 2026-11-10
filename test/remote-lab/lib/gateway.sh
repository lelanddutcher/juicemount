#!/bin/sh
# gateway.sh — runs INSIDE the gateway container (compose.mac.yml).
#
# Brings up a userspace WireGuard interface (wireguard-go) for the macOS VM
# and routes it to the nas network through the wan container, like the Linux
# client. Traffic from the tunnel leaves masqueraded as the gateway's own
# remote-network address, so the wan and the NAS side need no route back.
# Only the gateway's own network namespace changes.
set -eu
: "${NAS_SUBNET:?}" "${WAN_REMOTE_IP:?}" "${TUNNEL_SUBNET:?}" "${TUNNEL_GW_IP:?}" "${WG_CONF:?}"
[ -f "$WG_CONF" ] || { echo "gateway: $WG_CONF is missing (run mac-lab.sh up)" >&2; exit 2; }

# Real 1500-byte packets through the wan, as in the client (see lab.sh).
ethtool -K eth0 tso off gso off gro off 2>/dev/null || true
ip route replace "$NAS_SUBNET" via "$WAN_REMOTE_IP"

wireguard-go -f wg0 &
wg_pid=$!
for _ in $(seq 50); do ip link show wg0 >/dev/null 2>&1 && break; sleep 0.1; done
ip link show wg0 >/dev/null 2>&1 || { echo "gateway: wireguard-go did not create wg0" >&2; exit 2; }
wg setconf wg0 "$WG_CONF"
ip addr add "$TUNNEL_GW_IP/${TUNNEL_SUBNET#*/}" dev wg0
ip link set wg0 mtu 1420 up
ethtool -K wg0 tso off gso off gro off 2>/dev/null || true
iptables -t nat -A POSTROUTING -s "$TUNNEL_SUBNET" -o eth0 -j MASQUERADE
# Keep TCP segments within the tunnel MTU whatever the VM announces.
iptables -t mangle -A FORWARD -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --clamp-mss-to-pmtu

touch /run/gateway.ready
echo "gateway: wg0 up on $TUNNEL_GW_IP, routing $NAS_SUBNET via $WAN_REMOTE_IP"
trap 'kill "$wg_pid" 2>/dev/null; exit 0' TERM INT
wait "$wg_pid"
