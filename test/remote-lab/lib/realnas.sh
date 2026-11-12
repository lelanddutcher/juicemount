#!/bin/sh
# realnas.sh — runs INSIDE the realnas container (compose.realnas.yml).
#
# Makes this container an alias of a real JuiceMount server on the lab's nas
# network. A TCP connection to REALNAS_IP:PORT, for each PORT in
# REALNAS_PORTS, is forwarded (DNAT) to REALNAS_HOST:PORT. The source is
# rewritten to this container's own address (MASQUERADE), so the replies come
# back through it, and so through the wan, which shapes both directions.
# TCP stays end to end between the lab client and the real server.
#
# Only those ports, only to REALNAS_HOST, only new connections from the nas
# subnet: everything else this container would forward is dropped. Only the
# container's own network namespace changes.
set -eu

die() { echo "realnas: $*" >&2; exit 2; }
: "${REALNAS_HOST:?set JMLAB_REALNAS_HOST}" "${REALNAS_IP:?}" "${REALNAS_PORTS:?}" "${NAS_SUBNET:?}"

case "$REALNAS_HOST" in
  *[!0-9.]*|'') die "JMLAB_REALNAS_HOST must be an IPv4 address" ;;
esac
for p in $REALNAS_PORTS; do
  case "$p" in *[!0-9]*|'') die "JMLAB_REALNAS_PORTS must be TCP port numbers" ;; esac
  [ "$p" -ge 1 ] && [ "$p" -le 65535 ] || die "bad port $p"
done

# The real server sends large segments that the host's network card merged on
# receive (GRO). With segmentation offloads off here, the kernel cuts them into
# 1500-byte packets on the way out of this container, so the wan drops and
# delays real packets, not 64 KB super-packets.
for path in /sys/class/net/*; do
  dev="${path##*/}"
  [ "$dev" = lo ] || ethtool -K "$dev" tso off gso off gro off 2>/dev/null || true
done

iptables -P FORWARD DROP
iptables -A FORWARD -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
for p in $REALNAS_PORTS; do
  iptables -t nat -A PREROUTING -s "$NAS_SUBNET" -d "$REALNAS_IP" -p tcp --dport "$p" \
    -j DNAT --to-destination "$REALNAS_HOST:$p"
  iptables -A FORWARD -s "$NAS_SUBNET" -d "$REALNAS_HOST" -p tcp --dport "$p" \
    -m conntrack --ctstate NEW -j ACCEPT
done
iptables -t nat -A POSTROUTING -d "$REALNAS_HOST" -j MASQUERADE

echo "realnas: $REALNAS_IP forwards TCP ports $REALNAS_PORTS to the real server"
touch /run/realnas.ready
exec sleep infinity
