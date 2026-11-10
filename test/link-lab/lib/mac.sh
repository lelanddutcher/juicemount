#!/bin/sh
# mac.sh — a Mac stand-in, INSIDE home-mac or away-mac: the stock Tailscale
# client on Linux in userspace mode (the same netstack the app's tsnet uses;
# no TUN device, so no kernel module), with a SOCKS5 port for the probes.
#
#   run     route via the site's router ($MAC_GW), use it as the resolver,
#           trust the lab CA, then stay up (the container's command)
#   start   (re)start tailscaled with empty in-memory state (a new node)
#   stop    stop tailscaled
set -eu
SOCK=/run/tailscale/tailscaled.sock

case "${1:-}" in
  run)
    ip route replace default via "$MAC_GW"
    ethtool -K eth0 tso off gso off gro off >/dev/null 2>&1 || true
    echo "nameserver $MAC_GW" > /etc/resolv.conf
    cp /lab/.state/pki/ca.crt /usr/local/share/ca-certificates/jm-link-lab.crt
    update-ca-certificates >/dev/null 2>&1
    exec sleep infinity
    ;;
  start)
    pkill -x tailscaled 2>/dev/null || true
    for _ in $(seq 50); do pgrep -x tailscaled >/dev/null || break; sleep 0.1; done
    mkdir -p /run/tailscale
    tailscaled --tun=userspace-networking --state=mem: --socket="$SOCK" --port=0 \
      --socks5-server=127.0.0.1:1055 --no-logs-no-support >/var/log/tailscaled.log 2>&1 &
    for _ in $(seq 100); do [ -S "$SOCK" ] && exit 0; sleep 0.1; done
    echo "tailscaled did not start" >&2; tail -n 20 /var/log/tailscaled.log >&2; exit 1
    ;;
  stop)
    pkill -x tailscaled 2>/dev/null || true
    ;;
  *) echo "usage: mac.sh run|start|stop" >&2; exit 2 ;;
esac
