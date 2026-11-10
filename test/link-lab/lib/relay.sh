#!/bin/sh
# relay.sh — the relay, INSIDE the relay container: Tailscale's derper with
# DERP over TLS on 443 and STUN on 3478/udp, using the lab-CA certificate
# for derp.lab.example (certmode manual: <name>.crt and <name>.key).
set -eu
ip route replace default via 10.232.3.2
for dev in /sys/class/net/*; do dev=${dev##*/}
  [ "$dev" = lo ] || ethtool -K "$dev" tso off gso off gro off >/dev/null 2>&1 || true
done
exec derper --hostname=derp.lab.example --certmode=manual --certdir=/lab-pki \
  -a :443 --http-port=-1 --stun --stun-port=3478 --verify-clients=false
