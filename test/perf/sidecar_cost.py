#!/usr/bin/env python3
"""sidecar_cost.py — what the ._ AppleDouble doubling actually costs.

WHAT A SIDECAR IS. macOS stores extended attributes and resource forks in a
companion file named `._Foo` beside `Foo`. NFSv3 has no extended-attribute RPC,
so the macOS NFS client emulates them this way. The client writes them; the
server is not asked and cannot decline. Nothing in this codebase creates one —
`grep` finds only read-side classification (nfs/sidecar.go, handler.go).

WHAT THIS MEASURES, and why it is not obvious:
  1. Is the doubling CONDITIONAL on the source carrying xattrs? If it were,
     most programmatic copies would cost nothing and the original finding (451
     CREATEs for 200 files + 51 dirs) would have been about Finder's own
     metadata rather than a general tax. Measured 2026-08-16: files written with
     a plain open() and NO xattrs still got a sidecar each, plus one per
     DIRECTORY (310 sidecars for 300 files + 10 dirs). It is unconditional.
  2. What share of WORK vs BYTES do they take? This is the number that matters,
     because the drainer bounds concurrency by FILE COUNT, not bytes.

THE CONCLUSION THIS SUPPORTS. Sidecars are ~50% of write operations and a
rounding error in bytes (~4 KiB against a 72 MiB camera master = 0.005%). Every
one of them takes a whole drain worker slot and blocks on a real MinIO PUT,
exactly like a media file. So half the drain capacity serves a thousandth of a
percent of the data. If drain concurrency is bounded at all it should be bounded
by BYTES IN FLIGHT — which is what the incident behind the worker count of 4 was
actually about (2026-06-14: 16 concurrent 66 MB copies saturated juicefs's
buffer, the mount went readdir-unresponsive, the watchdog SIGKILLed it) — rather
than by file count, which a 4 KiB sidecar consumes as greedily as a 66 MB file.

The doubling itself is STRUCTURAL and not fixable server-side. The cost it
imposes on the drain is not.
"""

import argparse
import json
import os
import shutil
import sys
import time
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib"))
import harness  # noqa: E402

CONTROL = os.environ.get("JM_CONTROL", "http://127.0.0.1:11050")
OPS = ("CREATE", "WRITE", "COMMIT", "SETATTR", "REMOVE")


def rpcs():
    try:
        with urllib.request.urlopen(CONTROL + "/metrics", timeout=8) as r:
            m = json.load(r)
    except Exception:
        return None
    q = m.get("rpcs") or {}
    return {k: (q.get(k) or {}).get("count", 0) for k in OPS}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mount", default="/Volumes/zpool")
    ap.add_argument("--count", type=int, default=100)
    ap.add_argument("--size-kb", type=int, default=32)
    ap.add_argument("--fuse-root", default=os.path.expanduser("~/.juicemount/fuse-internal"),
                    help="stat sizes HERE, not through the NFS mount: the mount "
                         "carries acregmin=acregmax=3600, so a stat over NFS "
                         "returns the size cached at creation for a FULL HOUR")
    ap.add_argument("--settle", type=int, default=25,
                    help="seconds to wait for the drain to publish real sizes; "
                         "sizes read 0 before it and a byte share from zeros is "
                         "not a measurement")
    ap.add_argument("--out", default="")
    a = ap.parse_args()

    dest = os.path.join(a.mount, ".jm-sidecar-cost")
    if rpcs() is None:
        print("control plane unreachable at %s" % CONTROL)
        return 3

    shutil.rmtree(dest, ignore_errors=True)
    os.makedirs(dest, exist_ok=True)
    time.sleep(2)
    before = rpcs()
    blob = b"y" * (a.size_kb * 1024)
    for i in range(a.count):
        with open(os.path.join(dest, "f%04d.bin" % i), "wb") as f:
            f.write(blob)
    time.sleep(4)
    after = rpcs()
    d = {k: after[k] - before[k] for k in OPS}

    real = [f for f in os.listdir(dest) if not f.startswith("._")]
    side = [f for f in os.listdir(dest) if f.startswith("._")]

    if not real:
        print("INVALID — no files landed; nothing was measured")
        shutil.rmtree(dest, ignore_errors=True)
        return 3

    # Sizes are published by the DRAIN, not at close. Reading them immediately
    # returns 0 for everything and yields a byte share computed from zeros —
    # which is how the first version of this died with ZeroDivisionError rather
    # than silently printing 0.00%.
    print("  waiting %ds for the drain to publish sizes…" % a.settle)
    time.sleep(a.settle)

    # Stat through FUSE, never through the NFS mount. acregmin=acregmax=3600
    # means the macOS client answers getattr from a cache populated when the
    # file was created — so sizes read over /Volumes/zpool stay at their
    # creation-time value for an hour. Measured: 100 files of 32 KiB totalled
    # 32 KiB over NFS after 90s of settling. That is the client cache, not the
    # drain, and no amount of waiting fixes it.
    fdir = os.path.join(a.fuse_root, os.path.relpath(dest, a.mount))
    def total(names):
        t = 0
        for f in names:
            try:
                t += os.path.getsize(os.path.join(fdir, f))
            except OSError:
                pass
        return t
    if not os.path.isdir(fdir):
        print("  INVALID — FUSE path %s absent; cannot read uncached sizes" % fdir)
        shutil.rmtree(dest, ignore_errors=True)
        return 3
    rb, sb = total(real), total(side)

    print("  wrote %d files of %d KiB, NO xattrs set on any of them" % (a.count, a.size_kb))
    print("  RPC deltas: %s" % d)
    print("  per real file: %.2f CREATE, %.2f WRITE, %.2f COMMIT"
          % (d["CREATE"] / a.count, d["WRITE"] / a.count, d["COMMIT"] / a.count))
    print("  on disk: %d real (%.1f KiB) + %d sidecars (%.1f KiB)"
          % (len(real), rb / 1024.0, len(side), sb / 1024.0))

    # "Some size is non-zero" is NOT enough, and letting that stand produced a
    # confidently wrong number: 100 files of 32 KiB reported 128 KiB total —
    # a handful published, the rest still 0 — and the gate printed an 8.57%
    # byte share from it. The real bytes written are KNOWN (count x size), so
    # check against that and refuse anything short of it.
    expect = a.count * a.size_kb * 1024
    if rb < 0.95 * expect:
        print("  INVALID — real files total %.1f KiB but %d x %d KiB were written "
              "(%.1f KiB expected). The drain has not finished publishing sizes, so "
              "any byte share here is computed from zeros. Raise --settle."
              % (rb / 1024.0, a.count, a.size_kb, expect / 1024.0))
        shutil.rmtree(dest, ignore_errors=True)
        return 3

    obj_share = 100.0 * len(side) / (len(real) + len(side))
    byte_share = 100.0 * sb / (rb + sb)
    per_side = sb / max(len(side), 1)
    print("  sidecar share of OBJECTS: %.0f%%    of BYTES: %.2f%%" % (obj_share, byte_share))
    print("  one sidecar (%.1f KiB) beside a 72 MiB camera master = %.4f%% of bytes"
          % (per_side / 1024.0, 100.0 * per_side / (72 * 1024 * 1024)))
    print()
    print("  => sidecars take ~%.0f%% of drain slots for %.2f%% of the bytes at this "
          "file size, and far less at media sizes. Bound the drain by BYTES, not "
          "file count." % (obj_share, byte_share))

    doc = harness.result("sidecar_cost", [d["CREATE"] / a.count], "creates_per_real_file",
                         corpus="%dx%dKiB" % (a.count, a.size_kb),
                         extra={"rpc_deltas": d, "objects": {"real": len(real),
                                                             "sidecars": len(side)},
                                "bytes": {"real": rb, "sidecars": sb},
                                "object_share_pct": obj_share,
                                "byte_share_pct": byte_share})
    if a.out:
        json.dump(doc, open(a.out, "w"), indent=2)
        print("  wrote %s" % a.out)
    shutil.rmtree(dest, ignore_errors=True)
    # Fail if the doubling is NOT present: that would mean this gate has stopped
    # measuring what it claims to (e.g. the mount changed), not that a fix landed.
    return 0 if d["CREATE"] >= 1.5 * a.count else 1


if __name__ == "__main__":
    sys.exit(main())
