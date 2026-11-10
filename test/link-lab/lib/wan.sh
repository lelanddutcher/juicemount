#!/bin/sh
# wan.sh — the emulated internet, INSIDE the wan container. It routes
# between the home link, the away link and the core (relay, DNS); it does
# no NAT and knows no private LAN. lab.sh shapes it with the remote lab's
# profiles (/remote-lab/lib/shape.sh); it starts on "lan" (no shaping).
set -eu
for dev in /sys/class/net/*; do dev=${dev##*/}
  [ "$dev" = lo ] || ethtool -K "$dev" tso off gso off gro off >/dev/null 2>&1 || true
done
/remote-lab/lib/shape.sh lan
touch /run/wan.ready
exec sleep infinity
