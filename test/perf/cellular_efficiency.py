#!/usr/bin/env python3
"""cellular_efficiency.py — cost per USEFUL OPERATION, not raw totals.

THE ASK: "ensure we are being the MOST efficient with rebuilding index and
bandwidth when on a cellular connection."

WHY PER-OPERATION. Raw totals are not comparable between runs — a longer run
moves more bytes and that says nothing. What is comparable, and what a
regression actually looks like, is the cost of ONE directory opened or ONE file
previewed. That number should be near-zero for warm content on any link.

THE UNIT IS ROUND TRIPS. On a high-RTT link the cost model is round trips x RTT,
not bytes. The evidence: a 2026-07-29 session at ~500 ms RTT spent 67 minutes of
cumulative metadata wait against only 77 object GETs. The bytes were trivial.
So the headline number here is redis.round_trips per operation, with backend
bytes reported alongside for the data half.

WHAT EACH INSTRUMENT ACTUALLY COVERS — established by cross-validation while
building these, not assumed:
  redis.round_trips     metadata chatter. EXACT (go-redis hook).
  backend.object_get_bytes  object-store traffic. Authoritative.
  wirebytes.py          NFS loopback volume — NOT a cellular link measure.
                        The object fetch is invisible to it; see its header.

INDEX EFFICIENCY is folded in because "rebuilding the index" was half the ask:
full SCANs per hour and the round trips each one costs. Post-93321e4 the routine
count should be at the backstop cadence, which test/perf/invariants.py asserts
separately.
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib"))
import harness  # noqa: E402

CONTROL = os.environ.get("JM_CONTROL", "http://127.0.0.1:11050")


def snap():
    try:
        with urllib.request.urlopen(CONTROL + "/metrics", timeout=8) as r:
            m = json.load(r)
    except Exception:
        return None
    r_ = m.get("redis") or {}
    b = m.get("backend") or {}
    rpc = m.get("rpcs") or {}
    return {
        "round_trips": r_.get("round_trips"),
        "commands": r_.get("commands"),
        "pipelines": r_.get("pipelines"),
        "dials": r_.get("dials"),
        "meta_ops": b.get("meta_ops"),
        "object_get_bytes": b.get("object_get_bytes"),
        "cache_miss": b.get("cache_miss"),
        "readdir_ops": ((rpc.get("READDIRPLUS") or {}).get("count", 0)
                        + (rpc.get("READDIR") or {}).get("count", 0)),
        "read_ops": (rpc.get("READ") or {}).get("count", 0),
    }


def delta(a, b):
    return {k: (None if a.get(k) is None or b.get(k) is None else b[k] - a[k])
            for k in a}


# The mount carries acdirmin=3,acdirmax=15, so a repeat walk inside that window
# is answered by the macOS client and never reaches us. Every measurement here
# ages past it first, otherwise the run reports a beautiful zero having tested
# the client's cache rather than JuiceMount.
CLIENT_DIR_CACHE_S = 16

# FILES are different, and it changes what is measurable. The same mount uses
# acregmin=acregmax=3600 — a full HOUR of client-side attribute caching for
# regular files. So a warm file re-read is answered entirely by macOS and never
# reaches JuiceMount at all. That is not a harness limitation and it is not a
# problem: it means a warm re-read costs the product literally nothing.
#
# It does mean "warm file preview" is UNMEASURABLE server-side without waiting
# an hour, so measuring it would only ever produce a zero from no coverage. The
# meaningful file number is the COLD open — the first touch, which is where the
# 66x cellular penalty lives (1,226-5,660 ms first open vs 24-36 ms second, all
# of it per-FILE rather than per-byte). That is what this measures instead.


def op_dirs(root, n):
    dirs = []
    for cur, subs, _ in os.walk(root):
        subs[:] = [s for s in subs if not s.startswith(".")]
        dirs.append(cur)
        if len(dirs) >= n:
            break
    return dirs


def op_files(root, n):
    out = []
    for cur, subs, names in os.walk(root):
        subs[:] = [s for s in subs if not s.startswith(".")]
        for nm in names:
            if not nm.startswith("."):
                out.append(os.path.join(cur, nm))
        if len(out) >= n:
            break
    return out[:n]


def measure(label, do_op, count, age_out=True):
    if age_out:
        time.sleep(CLIENT_DIR_CACHE_S)
    a = snap()
    if a is None:
        return {"op": label, "valid": False, "reason": "control plane unreachable"}
    t0 = time.perf_counter()
    do_op()
    wall = (time.perf_counter() - t0) * 1000.0
    time.sleep(2)   # let async counters settle
    b = snap()
    d = delta(a, b)

    # VALIDITY: the server must actually have served the operation. Without
    # this the run reports "0 round trips per dir" when the client cache
    # answered everything — a perfect score for having measured nothing.
    served = (d.get("readdir_ops") or 0) + (d.get("read_ops") or 0)
    if served <= 0:
        return {"op": label, "valid": False,
                "reason": ("0 server-side ops — the macOS client cache answered "
                           "everything, so any per-op cost below would be a "
                           "number from no coverage")}
    if d.get("round_trips") is None:
        return {"op": label, "valid": False,
                "reason": ("redis.round_trips absent from /metrics — the running "
                           "build predates the hook, so metadata cost is "
                           "unmeasurable. NOT a pass.")}
    return {
        "op": label, "valid": True, "count": count, "wall_ms": wall,
        "server_ops": served,
        "round_trips_total": d["round_trips"],
        "round_trips_per_op": d["round_trips"] / count,
        "object_get_bytes_total": d.get("object_get_bytes"),
        "object_get_bytes_per_op": (d["object_get_bytes"] / count
                                    if d.get("object_get_bytes") is not None else None),
        "meta_ops_total": d.get("meta_ops"),
        "cache_miss": d.get("cache_miss"),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="/Volumes/zpool/oldzpool/ARCHIVE")
    ap.add_argument("--dirs", type=int, default=40)
    ap.add_argument("--files", type=int, default=25)
    ap.add_argument("--read-bytes", type=int, default=65536,
                    help="bytes requested per file; the denominator of amplification")
    ap.add_argument("--max-amplification", type=float, default=0,
                    help="fail if pulled/requested exceeds this (0 = report only)")
    ap.add_argument("--out", default="")
    a = ap.parse_args()

    if snap() is None:
        print("control plane unreachable at %s" % CONTROL)
        return 3

    dirs = op_dirs(a.root, a.dirs)
    files = op_files(a.root, a.files)
    print("corpus: %d dirs, %d files under %s" % (len(dirs), len(files), a.root))

    def walk():
        for d in dirs:
            try:
                os.listdir(d)
            except Exception:
                pass

    def preview():
        for f in files:
            try:
                with open(f, "rb") as fh:
                    fh.read(65536)
            except Exception:
                pass

    print("warming…")
    walk(); preview(); walk(); preview()

    # Cold files: never touched this session, so the open genuinely reaches us.
    cold = [f for f in op_files(a.root, a.files * 12) if f not in set(files)][-a.files:]

    def preview_cold():
        for f in cold:
            try:
                with open(f, "rb") as fh:
                    fh.read(a.read_bytes)
            except Exception:
                pass

    results = [
        measure("warm_dir_open", walk, len(dirs)),
        measure("cold_file_open", preview_cold, len(cold)) if cold else
        {"op": "cold_file_open", "valid": False, "reason": "no untouched files found"},
    ]

    print()
    for r in results:
        if not r.get("valid"):
            print("  %-18s INVALID — %s" % (r["op"], r["reason"]))
            continue
        ob = r["object_get_bytes_per_op"]
        print("  %-18s %6.2f round_trips/op   %8s bytes/op   (%d ops served, %.0f ms)"
              % (r["op"], r["round_trips_per_op"],
                 ("%.0f" % ob) if ob is not None else "n/a",
                 r["server_ops"], r["wall_ms"]))

    # --- READ AMPLIFICATION: bytes pulled per byte actually asked for.
    # This is the cellular headline. Measured on class=fast: 1.56 MiB requested
    # pulled 785 MiB, 503x. Readahead is class-gated (blocks_ahead/workers narrow
    # on a metered link), so this number is the LAN figure — re-run under a
    # metered class to learn the cellular one.
    print("\n  READ AMPLIFICATION — bytes pulled per byte requested")
    amp_bad = []
    for r in results:
        if not r.get("valid") or r["op"] != "cold_file_open":
            continue
        pulled = r.get("object_get_bytes_per_op")
        if pulled is None:
            print("    object_get_bytes unavailable — cannot judge")
            continue
        amp = pulled / a.read_bytes if a.read_bytes else 0
        print("    requested %.2f MiB, pulled %.1f MiB  ->  %.0fx"
              % (r["count"] * a.read_bytes / 1048576.0,
                 r["count"] * pulled / 1048576.0, amp))
        if a.max_amplification and amp > a.max_amplification:
            amp_bad.append(amp)
            print("    EXCEEDS the %.0fx budget" % a.max_amplification)

    print("\n  PASS BAR — warm operations cost ~0 metadata round trips")
    bad = []
    for r in results:
        if not r.get("valid"):
            print("    %-18s SKIP (invalid arm)" % r["op"])
            continue
        rt = r["round_trips_per_op"]
        if rt > 1.0:
            bad.append(r["op"])
            print("    %-18s %.2f round_trips/op  <-- metadata chatter on warm content"
                  % (r["op"], rt))
        else:
            print("    %-18s %.2f round_trips/op" % (r["op"], rt))

    doc = harness.result("cellular_efficiency",
                         [r["round_trips_per_op"] for r in results if r.get("valid")],
                         "redis_round_trips_per_op",
                         corpus="%dd/%df" % (len(dirs), len(files)),
                         extra={"ops": results})
    if a.out:
        json.dump(doc, open(a.out, "w"), indent=2)
        print("  wrote %s" % a.out)
    return 1 if (bad or amp_bad) else 0


if __name__ == "__main__":
    sys.exit(main())
