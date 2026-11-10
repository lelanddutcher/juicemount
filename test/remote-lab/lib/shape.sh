#!/bin/sh
# shape.sh PROFILE — runs INSIDE the wan container (lab.sh calls it).
#
# Applies one line of profiles.tsv to the wan container's two interfaces.
# Each interface shapes only the traffic that LEAVES through it:
#
#   interface on the remote network  ->  traffic toward the client
#                                        (the NAS uplink: down_mbit)
#   interface on the nas network     ->  traffic toward the NAS
#                                        (the client uplink: up_mbit)
#
# Per direction: an htb root caps the rate, and a netem leaf adds half the
# round-trip delay, half the jitter, and the loss. netem also gets a rate far
# above the link rate: that makes netem keep packets in order even with
# jitter (it schedules each packet after the previous one), which is how a
# real jittery link behaves. Without it netem reorders packets and TCP
# collapses for a reason no real link has.
#
# Only the wan container's own network namespace changes.
set -eu

die() { echo "shape: $*" >&2; exit 2; }
[ $# -eq 1 ] || die "usage: shape.sh PROFILE"
PROFILES="${JMLAB_PROFILES:-/lab/profiles.tsv}"
: "${WAN_NAS_IP:?}" "${WAN_REMOTE_IP:?}"

line="$(awk -v p="$1" '$1 == p { print; exit }' "$PROFILES")"
[ -n "$line" ] || die "unknown profile '$1' (see profiles.tsv)"
# shellcheck disable=SC2086
set -- $line
rtt=$2 jitter=$3 loss=$4 down=$5 up=$6

iface_for() {
  ip -o -4 addr show | awk -v ip="$1" '{ split($4, a, "/"); if (a[1] == ip) { sub(/@.*/, "", $2); print $2 } }'
}
to_client="$(iface_for "$WAN_REMOTE_IP")"
to_nas="$(iface_for "$WAN_NAS_IP")"
[ -n "$to_client" ] && [ -n "$to_nas" ] || die "wan interfaces not found"

# Half of a millisecond value, in microseconds (so 1 ms splits exactly).
half_us() { echo $(( $1 * 1000 / 2 )); }

apply() { # apply DEV RATE_MBIT
  dev=$1 rate=$2
  tc qdisc del dev "$dev" root 2>/dev/null || true
  if [ "$rtt" = 0 ] && [ "$jitter" = 0 ] && [ "$loss" = 0 ] && [ "$rate" = 0 ]; then
    return 0
  fi
  netem="delay $(half_us "$rtt")us"
  [ "$jitter" = 0 ] || netem="$netem $(half_us "$jitter")us"
  [ "$loss" = 0 ] || netem="$netem loss ${loss}%"
  # Queue: three bandwidth-delay products (at least 1000 packets), so the
  # emulator never drops packets a real path would have carried.
  eff_rate=$rate; [ "$eff_rate" != 0 ] || eff_rate=10000
  limit=$(( eff_rate * 1000000 * (rtt + jitter + 10) * 3 / 8 / 1000 / 1500 ))
  [ "$limit" -ge 1000 ] || limit=1000
  netem="$netem limit $limit rate 100gbit"
  if [ "$rate" = 0 ]; then
    # shellcheck disable=SC2086
    tc qdisc add dev "$dev" root handle 1: netem $netem
  else
    # Burst: 1 ms of traffic, at least 16 KB.
    burst=$(( rate * 1000 / 8 )); [ "$burst" -ge 16000 ] || burst=16000
    tc qdisc add dev "$dev" root handle 1: htb default 10 r2q 10000
    tc class add dev "$dev" parent 1: classid 1:10 htb rate "${rate}mbit" ceil "${rate}mbit" quantum 1514 burst "$burst" cburst "$burst"
    # shellcheck disable=SC2086
    tc qdisc add dev "$dev" parent 1:10 handle 10: netem $netem
  fi
}

apply "$to_client" "$down"
apply "$to_nas" "$up"
echo "shape: $1 applied (rtt ${rtt} ms, jitter ${jitter} ms, loss ${loss}%/direction, down ${down} Mbit/s, up ${up} Mbit/s)"
