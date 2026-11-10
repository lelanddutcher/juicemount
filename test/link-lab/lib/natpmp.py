#!/usr/bin/env python3
"""natpmp.py: a small NAT-PMP responder (RFC 6886) for a lab router.

Runs INSIDE a router container (router.sh starts it when PORTMAP=on). It
answers on the router's LAN address, UDP 5351:

  opcode 0         the public address
  opcode 1 / 2     map a UDP / TCP port: adds a DNAT forward from the public
                   address to the asking device (lifetime 0 removes it)

It stands in for a home router's UPnP / NAT-PMP / PCP service. The lab
does not run miniupnpd: Alpine's build drives the legacy iptables
interface, which needs kernel table modules a stock Docker host may not
have loaded. This responder uses the router's own (nf_tables) iptables.
PCP and UPnP requests get no answer; Tailscale's port mapper then uses
NAT-PMP. Mappings do not expire; a case change (router.sh apply) clears them.
"""
import argparse
import socket
import struct
import subprocess
import sys
import time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lan-ip", required=True)
    ap.add_argument("--public-ip", required=True)
    ap.add_argument("--out-if", required=True)
    a = ap.parse_args()
    epoch0 = time.monotonic()
    maps = {}  # (proto, external port) -> (client ip, internal port)
    pub = socket.inet_aton(a.public_ip)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((a.lan_ip, 5351))
    print(f"natpmp: listening on {a.lan_ip}:5351, public {a.public_ip}", flush=True)

    def rule(op, proto, ext, ip, port):
        subprocess.run(["iptables", "-t", "nat", op, "PREROUTING", "-i", a.out_if, "-d", a.public_ip,
                        "-p", proto, "--dport", str(ext), "-j", "DNAT",
                        "--to-destination", f"{ip}:{port}"], check=False)

    while True:
        data, (ip, port) = s.recvfrom(1100)
        epoch = int(time.monotonic() - epoch0)
        if len(data) < 2 or data[0] != 0:
            continue  # PCP (version 2) and anything else: no answer
        op = data[1]
        if op == 0:
            s.sendto(struct.pack(">BBHI", 0, 128, 0, epoch) + pub, (ip, port))
            continue
        if op not in (1, 2) or len(data) < 12:
            s.sendto(struct.pack(">BBHI", 0, 128 + op, 5, epoch), (ip, port))  # unsupported opcode
            continue
        proto = "udp" if op == 1 else "tcp"
        internal, suggested, lifetime = struct.unpack(">HHI", data[4:12])
        mine = [k for k, v in maps.items() if k[0] == proto and v == (ip, internal)]
        if lifetime == 0:
            for k in mine:
                rule("-D", proto, k[1], ip, internal)
                del maps[k]
            s.sendto(struct.pack(">BBHIHHI", 0, 128 + op, 0, epoch, internal, 0, 0), (ip, port))
            continue
        if mine:
            ext = mine[0][1]
        else:
            ext = suggested or internal
            while (proto, ext) in maps:
                ext = 1024 + (ext * 7919) % 64000
            maps[(proto, ext)] = (ip, internal)
            rule("-I", proto, ext, ip, internal)
            print(f"natpmp: {proto} {a.public_ip}:{ext} -> {ip}:{internal} ({lifetime}s)", flush=True)
        s.sendto(struct.pack(">BBHIHHI", 0, 128 + op, 0, epoch, internal, ext, lifetime), (ip, port))


if __name__ == "__main__":
    sys.exit(main())
