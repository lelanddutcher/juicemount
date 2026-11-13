#!/usr/bin/env python3
"""Link-policy probe for the remote lab (runs inside the client container).

It speaks the bundled juicefs's JMLinkPolicy control message (patch 0027)
the way the app's link follower does (nfs/jfs_link_policy.go):

  policy               the settings in force
  policy lan|abroad    set the policy the app sends on that link class
  ttfb FILE            open FILE and read its first 128 KiB: first-byte time

Every command prints one JSON line.
"""
import json
import os
import struct
import sys
import time

MOUNT = "/jfs"
MSG = 1106
VERSION = 1
# The app's policies (nfs/jfs_link_policy.go jfsLinkPolicyFor): lan = the
# upstream defaults, abroad = a far link (ClassSlow) mounted before any
# bandwidth sample, as health/fuse.go mounts it.
POLICIES = {
    "lan": (0, 60_000, 60_000, 10),
    "abroad": (1, 120_000, 180_000, 3),
}


def control(body):
    msg = struct.pack(">II", MSG, len(body)) + body
    fd = os.open(os.path.join(MOUNT, ".control"), os.O_RDWR)
    try:
        os.write(fd, msg)
        out = b""
        deadline = time.time() + 30
        while len(out) < 11:
            chunk = os.read(fd, 11 - len(out))
            out += chunk
            if out[:1] not in (b"", b"\0"):
                break
            if not chunk:
                if time.time() > deadline:
                    raise TimeoutError("no reply")
                time.sleep(0.002)
        return out
    finally:
        os.close(fd)


def cmd_policy(args):
    body = bytes([VERSION])
    if args:
        flags, get_ms, put_ms, retries = POLICIES[args[0]]
        body += struct.pack(">BIIB", flags, get_ms, put_ms, retries)
    s = time.time()
    r = control(body)
    ms = round((time.time() - s) * 1000, 1)
    if r[0] != 0 or len(r) < 11:
        print(json.dumps({"cmd": "policy", "errno": r[0], "ms": ms}))
        return
    flags, get_ms, put_ms, retries = struct.unpack(">BIIB", r[1:11])
    print(json.dumps({"cmd": "policy", "set": args[0] if args else None, "demand_first": bool(flags & 1),
                      "get_timeout_s": get_ms / 1000, "put_timeout_s": put_ms / 1000, "io_retries": retries,
                      "ms": ms}))


def cmd_ttfb(f):
    s = time.time()
    fd = os.open(f, os.O_RDONLY)
    try:
        opened = time.time()
        b = os.read(fd, 128 << 10)
        done = time.time()
    finally:
        os.close(fd)
    print(json.dumps({"cmd": "ttfb", "file": f, "open_ms": round((opened - s) * 1000, 1),
                      "first_byte_ms": round((done - s) * 1000, 1), "bytes": len(b)}))


def main():
    c, args = sys.argv[1], sys.argv[2:]
    if c == "policy":
        cmd_policy(args)
    elif c == "ttfb":
        cmd_ttfb(args[0])
    else:
        sys.exit("usage: linkprobe.py policy [lan|abroad] | ttfb FILE")


if __name__ == "__main__":
    main()
