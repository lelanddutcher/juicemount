#!/usr/bin/env python3
"""playback_ttfb.py — how long from open() to the first byte, and what it costs.

THE REPORT: "feels like cached files are checking with Redis if there is a newer
version before serving, because there is a bit of lag between pressing play and
playback starting."

WHY THE EXISTING GATE DOES NOT ANSWER THIS. nav_parity.py proved warm DIRECTORY
listings cost 0 Redis round trips on LAN, shaped and offline. That is the
listing path. Pressing play is a different sequence — LOOKUP, ACCESS, GETATTR,
OPEN, then the first READ — and the felt latency is time-to-FIRST-BYTE, not
throughput. Nothing measured that.

WHAT IS MEASURED, per file:
  ttfb_ms          open() through the first byte returned. This is the number
                   the user feels.
  redis round trips attributed to that open. If a warm file re-validates
                   against Redis before serving, this is where it shows.
  backend bytes    whether the "cached" file actually came from cache.

TWO ARMS on the SAME files:
  first   the file has not been touched this session
  repeat  immediately after, so any per-open revalidation still happens but the
          bytes are certainly local

VALIDITY. The mount carries acregmin=acregmax=3600, so a repeat open inside an
hour can be answered entirely by the macOS client and never reach us — which
would report a beautiful 0 having measured nothing. The repeat arm therefore
reports how many server-side ops it actually saw, and says so when that is zero
rather than claiming a win.

NOTE ON THIS LINK: the Mac is on WiFi, and netprofile's own RTT swings 15-163 ms.
Any MB/s figure here is unreliable for that reason; TTFB and round-trip COUNTS
are not, which is why those are the headline.
"""

import argparse
import json
import os
import statistics
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
    g = m.get("fuse_data_gate") or {}
    n = m.get("network") or {}
    return {
        "round_trips": r_.get("round_trips"),
        "object_get_bytes": b.get("object_get_bytes"),
        "cache_miss": b.get("cache_miss"),
        "meta_ops": b.get("meta_ops"),
        "read_ops": (rpc.get("READ") or {}).get("count", 0),
        "lookup": (rpc.get("LOOKUP") or {}).get("count", 0),
        "getattr": (rpc.get("GETATTR") or {}).get("count", 0),
        "access": (rpc.get("ACCESS") or {}).get("count", 0),
        "gate_refusals": g.get("refusals"),
        "class": n.get("class"),
    }


def delta(a, b):
    return {k: (None if a.get(k) is None or b.get(k) is None
                or not isinstance(a.get(k), (int, float)) else b[k] - a[k])
            for k in a}


def ttfb(path, nbytes):
    """open() through first byte. Returns ms, or None if it failed."""
    t0 = time.perf_counter()
    try:
        fh = open(path, "rb")
    except OSError:
        return None
    try:
        data = fh.read(nbytes)
    except OSError:
        fh.close()
        return None
    fh.close()
    if not data:
        return None
    return (time.perf_counter() - t0) * 1000.0


def arm(label, files, nbytes):
    a = snap()
    if a is None:
        return {"arm": label, "valid": False, "reason": "control plane unreachable"}
    times = [t for t in (ttfb(f, nbytes) for f in files) if t is not None]
    time.sleep(2)
    b = snap()
    d = delta(a, b)
    if not times:
        return {"arm": label, "valid": False, "reason": "no file opened successfully"}

    served = (d.get("lookup") or 0) + (d.get("getattr") or 0) + (d.get("read_ops") or 0)
    st = harness.stats(times)
    return {
        "arm": label, "valid": True, "n": len(times),
        "ttfb_ms": st,
        "server_ops": served,
        "round_trips_total": d.get("round_trips"),
        "round_trips_per_open": ((d["round_trips"] / len(times))
                                 if d.get("round_trips") is not None else None),
        "object_get_bytes": d.get("object_get_bytes"),
        "cache_miss": d.get("cache_miss"),
        "gate_refusals": d.get("gate_refusals"),
        "rpc": {k: d.get(k) for k in ("lookup", "getattr", "access", "read_ops")},
        "class": b.get("class"),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="/Volumes/zpool/oldzpool/ARCHIVE")
    ap.add_argument("--files", type=int, default=20)
    ap.add_argument("--min-mb", type=int, default=8)
    ap.add_argument("--bytes", type=int, default=65536,
                    help="bytes read to count as 'playback started'")
    ap.add_argument("--skip", type=int, default=0)
    ap.add_argument("--out", default="")
    a = ap.parse_args()

    if snap() is None:
        print("control plane unreachable at %s" % CONTROL)
        return 3

    picks = []
    for cur, subs, names in os.walk(a.root):
        subs[:] = [s for s in subs if not s.startswith(".")]
        for nm in names:
            if nm.startswith("."):
                continue
            p = os.path.join(cur, nm)
            try:
                if os.path.getsize(p) >= a.min_mb * 1024 * 1024:
                    picks.append(p)
            except OSError:
                pass
        if len(picks) > a.skip + a.files * 3:
            break
    picks = sorted(picks)[a.skip:a.skip + a.files]
    if len(picks) < 3:
        print("only %d files found — not a measurement" % len(picks))
        return 3
    print("corpus: %d files >=%dMiB under %s" % (len(picks), a.min_mb, a.root))

    results = [arm("first", picks, a.bytes), arm("repeat", picks, a.bytes)]

    print()
    for r in results:
        if not r.get("valid"):
            print("  %-7s INVALID — %s" % (r["arm"], r["reason"]))
            continue
        s = r["ttfb_ms"]
        rt = r["round_trips_per_open"]
        print("  %-7s TTFB p50 %7.1f ms  (range %.1f-%.1f, n=%d)   "
              "redis %s/open   backend %s B   gate_refusals +%s   class=%s"
              % (r["arm"], s["median"], s["min"], s["max"], s["n"],
                 ("%.2f" % rt) if rt is not None else "n/a",
                 r["object_get_bytes"], r["gate_refusals"], r["class"]))
        print("          RPCs: %s   server_ops=%d" % (r["rpc"], r["server_ops"]))

    rep = results[1]
    print("\n  THE QUESTION: does a warm open re-validate against Redis?")
    # ATTRIBUTION IS THE WHOLE DIFFICULTY HERE. redis.round_trips is a
    # PROCESS-WIDE counter: keyspace events, reconcile and any background sweep
    # land in the same window as the opens. Dividing it by the open count is
    # only honest when the opens dominate that window. They do not when the
    # client cache served them.
    #
    # Measured 2026-08-17: a repeat arm with read_ops=0 and 18 GETATTRs reported
    # "14.75 round trips per warm open" and would have CONFIRMED a revalidation
    # that never happened — the traffic was a farm migration churning Redis. A
    # LOW number is still meaningful (nothing was revalidating); a HIGH number
    # with no reads is background noise, and saying so is the only correct
    # answer.
    if not rep.get("valid"):
        print("    unanswerable — repeat arm invalid")
    elif (rep["rpc"].get("read_ops") or 0) == 0 and (rep["round_trips_per_open"] or 0) > 1.0:
        print("    INCONCLUSIVE — %.2f round trips per open, but read_ops=0: the "
              "client cache served every open, so the server did no per-open work "
              "and these round trips are BACKGROUND traffic in the same window, "
              "not revalidation. Re-run when the process is otherwise idle."
              % rep["round_trips_per_open"])
    elif rep["server_ops"] == 0:
        print("    UNMEASURABLE this session: the repeat arm reached the server 0 "
              "times. acregmin=acregmax=3600 means macOS answered every repeat "
              "open from its own cache, so JuiceMount was not consulted at all — "
              "which also means it cannot be the source of the felt lag on a "
              "re-open. A 0 here is NOT proof the warm path is cheap.")
    elif rep["round_trips_per_open"] and rep["round_trips_per_open"] > 1.0:
        print("    YES — %.2f Redis round trips per warm open. That is the "
              "re-validation the founder suspected."
              % rep["round_trips_per_open"])
    else:
        print("    NO — %.2f Redis round trips per warm open across %d server-side "
              "ops. The warm open is not consulting Redis; the felt lag is "
              "elsewhere (see gate_refusals and the class above)."
              % (rep["round_trips_per_open"] or 0.0, rep["server_ops"]))

    if a.out:
        json.dump(harness.result("playback_ttfb",
                                 [r["ttfb_ms"]["median"] for r in results if r.get("valid")],
                                 "ttfb_ms_p50", corpus="%d files" % len(picks),
                                 extra={"arms": results}),
                  open(a.out, "w"), indent=2)
        print("  wrote %s" % a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
