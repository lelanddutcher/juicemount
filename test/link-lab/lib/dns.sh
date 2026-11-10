#!/bin/sh
# dns.sh — the lab DNS, INSIDE the dns container: the public answers for
# the lab's two names, and nothing else (no upstream, no internet names).
#   link.lab.example  the home's public address (home-router, internet side)
#   derp.lab.example  the relay
set -eu
ip route replace default via 10.232.3.2
exec dnsmasq --keep-in-foreground --no-resolv --no-hosts --cache-size=0 \
  --listen-address=10.232.3.53 --bind-interfaces \
  --address=/link.lab.example/10.232.1.10 \
  --address=/derp.lab.example/10.232.3.10 \
  --log-facility=-
