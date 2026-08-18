#!/usr/bin/env python3
"""cached_read_class_ab.py — does a LOCALLY CACHED read get slower just because
the link is bad?

THE CLAIM UNDER TEST. A file whose bytes are already on this Mac needs no
network to serve. So reading it should take the same time on cellular as on
gigabit ethernet. The founder reports it does not, and two gates in our NFS
server explain why:

  nfs/fusedatagate.go effectiveFUSEDataWidth() narrows the concurrent-read
    ceiling from 16 to 4 (slow) or 2 (metered).
  nfs/readqos.go readQoS.active() shapes reads into waiting lanes.

Both switch on netprofile's LINK CLASS alone. Neither asks whether the read will
touch the backend. Both were built for a real problem -- concurrent backend GETs
starving each other, which on 2026-06-14 saturated juicefs's buffer and got the
mount SIGKILLed -- but a cached read issues no GET, so for cached reads the
throttle buys nothing and costs latency.

HOW THIS IS MEASURED. Two arms, each a full app run with JM_NET_FORCE_CLASS
pinned (the class is read once under a sync.Once, so it cannot be changed in a
live process). Same file, primed into the local cache, read through
/Volumes/zpool so OUR gates are in the path. Page cache bypassed with F_NOCACHE
so every read is really served, not answered from RAM.

THE VALIDITY GATE THAT MATTERS. Backend GET bytes must be ~0 in BOTH arms. If
the metered arm pulled from the network, it was not a cached read and the whole
comparison is void -- it would be measuring the NAS, which is exactly the
confound this test exists to exclude. A slower metered arm only means something
if both arms were served locally.

Run one arm at a time with --arm; then --compare the two files.
"""

import argparse
import fcntl
import json
import os
import statistics
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib"))
import harness  # noqa: E402

NFS = "/Volumes/zpool"
JFS_METRICS = os.environ.get("JM_JFS_METRICS", "http://127.0.0.1:9568/metrics")
F_NOCACHE = 48
WARM_TOLERANCE = 8 * 1024 * 1024


def backend_get_bytes():
    try:
        raw = urllib.request.urlopen(JFS_METRICS, timeout=6).read().decode()
    except Exception:
        return None
    for line in raw.splitlines():
        if line.startswith("juicefs_object_request_data_bytes") and 'method="GET"' in line:
            return float(line.rsplit(None, 1)[1])
    return 0.0


def observed_class():
    try:
        m = json.load(urllib.request.urlopen("http://127.0.0.1:11050/metrics", timeout=6))
        return (m.get("network") or {}).get("class")
    except Exception:
        return None


def read_once(path, buf=1024 * 1024):
    fd = os.open(path, os.O_RDONLY)
    try:
        fcntl.fcntl(fd, F_NOCACHE, 1)
        total = 0
        t0 = time.perf_counter()
        while True:
            b = os.read(fd, buf)
            if not b:
                break
            total += len(b)
        return total, time.perf_counter() - t0
    finally:
        os.close(fd)


def run_arm(a):
    target = os.path.join(NFS, a.file)
    if not os.path.exists(target):
        print("INVALID — target missing: %s" % target)
        return 2
    size = os.path.getsize(target)
    cls = observed_class()
    print("arm=%s  observed class=%s  target=%.1f MiB" % (a.arm, cls, size / 1048576))
    if cls != a.arm:
        print("INVALID — asked for class %r but the server reports %r. The arm did "
              "not take effect; JM_NET_FORCE_CLASS is read once at startup, so the "
              "app must be relaunched WITH it set." % (a.arm, cls))
        return 2

    read_once(target)          # prime the local cache
    g0 = backend_get_bytes()
    samples = []
    if a.concurrency <= 1:
        for i in range(a.n):
            nb, dt = read_once(target)
            if nb != size:
                print("INVALID — short read (%d of %d)" % (nb, size))
                return 2
            samples.append(dt * 1000.0)
            print("  read %d: %.1f ms" % (i + 1, dt * 1000.0))
    else:
        # Each round fires `concurrency` readers at once and records the WALL
        # time for the whole round. Wall, not per-reader mean: a narrowed gate
        # does not make any single read slower, it makes readers queue, and
        # queueing only shows up in how long the batch takes to finish.
        import threading
        for rnd in range(a.n):
            errs, t0 = [], time.perf_counter()
            def worker():
                try:
                    nb, _ = read_once(target)
                    if nb != size:
                        errs.append("short read %d" % nb)
                except Exception as e:
                    errs.append(str(e))
            ts = [threading.Thread(target=worker) for _ in range(a.concurrency)]
            for t in ts:
                t.start()
            for t in ts:
                t.join()
            wall = (time.perf_counter() - t0) * 1000.0
            if errs:
                print("INVALID — %d reader errors: %s" % (len(errs), errs[:3]))
                return 2
            samples.append(wall)
            print("  round %d: %.1f ms wall for %d concurrent readers"
                  % (rnd + 1, wall, a.concurrency))
    g1 = backend_get_bytes()
    d_get = (g1 - g0) if (g0 is not None and g1 is not None) else None

    print("backend GET delta: %s"
          % ("%.1f MiB" % (d_get / 1048576) if d_get is not None else "UNKNOWN"))
    if d_get is None:
        print("INVALID — juicefs metrics unreachable; cannot prove the read was cached")
        return 2
    if d_get > WARM_TOLERANCE:
        print("INVALID — pulled %.1f MiB from the backend, so this arm was NOT a "
              "cached read. A slow cached read is the claim; a slow COLD read is "
              "just the network." % (d_get / 1048576))
        return 2

    doc = harness.result("cached_read_class", samples, "ms_per_full_read",
                         corpus="%.0fMiB cached, via NFS, concurrency=%d" % (size / 1048576, a.concurrency),
                         note="class forced to %s; F_NOCACHE; backend GET delta "
                              "%.2f MiB (cached)" % (a.arm, d_get / 1048576),
                         extra={"forced_class": a.arm, "observed_class": cls,
                                "backend_get_bytes_delta": d_get,
                                "size_bytes": size, "file": a.file,
                                "concurrency": a.concurrency})
    s = doc["stats"]
    print("\nmedian %.1f ms   range %.1f-%.1f   n=%d"
          % (s["median"], s["min"], s["max"], s["n"]))
    json.dump(doc, open(a.out, "w"), indent=2)
    print("wrote %s" % a.out)
    return 0


def do_compare(a):
    fast = json.load(open(a.compare[0]))
    metered = json.load(open(a.compare[1]))
    for d, label in ((fast, "fast"), (metered, "metered")):
        e = d.get("extra") or {}
        print("%-8s median %7.1f ms  n=%d  backend_get=%.2f MiB"
              % (label, d["stats"]["median"], d["stats"]["n"],
                 (e.get("backend_get_bytes_delta") or 0) / 1048576))
    # The class IS the independent variable here, so the class-equality guard is
    # waived deliberately — never inferred. n>=6 and class_measured still apply.
    v = harness.compare(fast, metered, lower_is_better=True, treatment="link_class")
    print("\nVERDICT: %s" % v["verdict"])
    print("  %s" % v["reason"])
    if v["verdict"] == harness.FAIL:
        print("\n  A cached read got slower purely because the link class changed.")
        print("  The bytes were already local in BOTH arms (backend GET ~0), so the")
        print("  extra time is our own throttle, not the network.")
    elif v["verdict"] == harness.PASS:
        print("\n  No separable difference — the class-keyed throttles do NOT")
        print("  measurably slow a cached read. The N1 finding is REFUTED as stated.")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--arm", choices=["fast", "slow", "metered", "medium"])
    ap.add_argument("--file", help="path relative to /Volumes/zpool")
    ap.add_argument("--n", type=int, default=8)
    ap.add_argument("--concurrency", type=int, default=1,
                    help="simultaneous readers. THE POINT: both gates bound "
                         "CONCURRENCY, not per-read speed, so a single reader "
                         "never reaches a ceiling of 2 and feels nothing. Finder "
                         "and an NLE read many files at once.")
    ap.add_argument("--out")
    ap.add_argument("--compare", nargs=2, metavar=("FAST", "METERED"))
    a = ap.parse_args()
    if a.compare:
        return do_compare(a)
    if not (a.arm and a.file and a.out):
        ap.error("--arm, --file and --out are required unless --compare is used")
    return run_arm(a)


if __name__ == "__main__":
    sys.exit(main())
