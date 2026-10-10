#!/usr/bin/env python3
"""nav_parity.py — directory navigation must cost the same on any link.

THE PRODUCT REQUIREMENT, in the founder's words: "offline and online performance
of the NFS share are both superb in terms of Finder navigation ... no matter
bandwidth (because we have an offline mirror of where every file is)".

THE SUSPECTED BUG, also theirs: "juicemount checks back with Redis to see if the
cached file is most recent before serving it to the user". And the insight that
makes this worth gating rather than eyeballing — "this cascades probably to
latency and network calls when a user is on 10GbE too, just not noticeable".

So the assertion that matters is NOT a stopwatch. It is:

    NAVIGATING ALREADY-WARM CONTENT MUST COST ~0 BACKEND OPERATIONS.

meta_ops is the honest counter for that: every metadata round trip to Redis
increments it. If we re-validate against Redis before serving something we
already hold, meta_ops climbs during a warm walk — on 10GbE that is invisible
in wall-clock and fatal on cellular. Wall time alone would pass on LAN and hide
it, which is exactly why the bug survived this long.

SAFE ARMS, same corpus, same walk:
  lan      — as-is
  offline  — control-plane offline toggle

The old ``shaped`` arm is retired and fails closed. It globally reloaded PF and
flushed every dummynet pipe, so even its cleanup could disrupt a firewall, VPN,
or unrelated experiment. Use ``test/perf/cellular_synthetic_acceptance.py`` for
bounded user-space cellular acceptance, or explicitly operate the scoped
``scripts/netshape-dev.sh`` helper when wire-level TCP behavior is required.

Pass bars:
  1. warm-walk backend cost ~0 on EVERY arm (the real test)
  2. dir-open p50/p95 indistinguishable across arms (all safe arms should be
     served from the local mirror, so divergence IS the finding)

SAFETY: this harness never changes PF or dummynet state. LAN and offline remain
available because neither requires host-global network mutation.
"""

import argparse
import atexit
import json
import os
import statistics
import sys
import time
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib"))
import harness  # noqa: E402

CONTROL = os.environ.get("JM_CONTROL", "http://127.0.0.1:11050")
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SAFE_SHAPER = os.path.join(REPO_ROOT, "scripts", "netshape-dev.sh")
USERSPACE_RUNNER = os.path.join(
    REPO_ROOT, "test", "perf", "cellular_synthetic_acceptance.py")


def _get(path, timeout=8):
    try:
        with urllib.request.urlopen(CONTROL + path, timeout=timeout) as r:
            return json.load(r)
    except Exception:
        return {}


def backend_counters():
    m = _get("/metrics")
    b = m.get("backend") or {}
    r = m.get("rpcs") or {}
    return {
        "meta_ops": b.get("meta_ops"),
        "object_get_bytes": b.get("object_get_bytes"),
        "cache_miss": b.get("cache_miss"),
        # The validity denominator. NOT rpc_total: an unrelated FSSTAT ticking
        # over makes rpc_total move while every listing was served from the
        # macOS client's attr cache, and the run then reports a confident
        # "meta_ops +0" having measured nothing at all. Observed exactly that:
        # rpc_total +1 with READDIRPLUS/READDIR/LOOKUP/GETATTR/ACCESS all zero.
        "readdir_ops": ((r.get("READDIRPLUS") or {}).get("count", 0)
                        + (r.get("READDIR") or {}).get("count", 0)),
        "lookup": (r.get("LOOKUP") or {}).get("count", 0),
    }


# The mount is made with acdirmin=3,acdirmax=15 (see cmd/jm5/main.go), so the
# client holds a directory listing for up to 15s. A second walk inside that
# window never reaches our server. Anything measuring SERVER behaviour has to
# age past it first.
CLIENT_DIR_CACHE_S = 16


def delta(a, b):
    out = {}
    for k in a:
        if a.get(k) is None or b.get(k) is None:
            out[k] = None
        else:
            out[k] = b[k] - a[k]
    return out


def walk(dirs):
    """Time one listdir per directory. Returns per-dir milliseconds."""
    ms = []
    for d in dirs:
        t0 = time.perf_counter()
        try:
            os.listdir(d)
        except Exception:
            continue
        ms.append((time.perf_counter() - t0) * 1000.0)
    return ms


def pick_dirs(root, want):
    dirs = []
    for cur, subs, _ in os.walk(root):
        subs[:] = [s for s in subs if not s.startswith(".")]
        dirs.append(cur)
        if len(dirs) >= want:
            break
    return dirs


# ------------------------------------------------------------ safe arm state

_offline = False


def print_retired_shape_guidance():
    """Explain the safe replacements without executing either one."""
    print("  shaped: REFUSED — legacy arm would mutate global PF/dummynet state")
    print("  bounded user-space runner: python3 %s --help" % USERSPACE_RUNNER)
    print("  explicit scoped shaper:   %s self-test" % SAFE_SHAPER)


def set_offline(on):
    global _offline
    try:
        # The control-plane contract is query-based (`?on=on|off`). The old
        # harness POSTed a JSON body that the handler intentionally ignores;
        # it still received HTTP 200 from the status response and falsely
        # labelled an online walk "offline". Register cleanup before engaging
        # so SIGINT/exception cannot strand the user's persisted intent.
        if on and not _offline:
            atexit.register(lambda: set_offline(False))
        state = "on" if on else "off"
        req = urllib.request.Request(
            CONTROL + "/offline?on=" + state, method="POST", data=b"",
            headers={"X-JM-Control": "1"})
        with urllib.request.urlopen(req, timeout=10) as r:
            ok = r.status == 200
            if ok:
                _offline = bool(on)
            return ok
    except Exception as e:
        print("  offline toggle failed: %s" % e)
        return False


# --------------------------------------------------------------------- arms

def run_arm(name, dirs, runs, age_out=True):
    per_run, costs = [], []
    for _ in range(runs):
        if age_out:
            # Let the client's directory cache expire so the walk actually
            # reaches our server.
            time.sleep(CLIENT_DIR_CACHE_S)
        b0 = backend_counters()
        ms = walk(dirs)
        b1 = backend_counters()
        if not ms:
            continue
        per_run.append(statistics.median(ms))
        costs.append(delta(b0, b1))
    if not per_run:
        return None
    agg = {}
    for k in (costs[0] if costs else {}):
        vals = [c[k] for c in costs if c.get(k) is not None]
        agg[k] = statistics.median(vals) if vals else None
    served = sum(c.get("readdir_ops") or 0 for c in costs)
    return {"arm": name, "dir_open_ms": harness.stats(per_run),
            "backend_cost_median": agg, "runs": len(per_run),
            "readdir_ops_total": served,
            # Without server-side readdir activity the arm measured the macOS
            # client cache, not JuiceMount.
            "valid": served > 0}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="/Volumes/zpool/oldzpool/ARCHIVE")
    ap.add_argument("--dirs", type=int, default=40)
    ap.add_argument("--runs", type=int, default=6)
    ap.add_argument("--arms", default="lan",
                    help="comma list of lan,offline (shaped is retired and refused)")
    # Parse the old knobs so automation gets a deliberate refusal instead of
    # an argparse mystery, but they can no longer reach a mutation path.
    ap.add_argument("--mbit", default="5", help=argparse.SUPPRESS)
    ap.add_argument("--delay-ms", default="200", help=argparse.SUPPRESS)
    ap.add_argument("--out", default="")
    a = ap.parse_args()

    requested_arms = [x.strip() for x in a.arms.split(",") if x.strip()]
    unknown = [x for x in requested_arms if x not in {"lan", "offline", "shaped"}]
    if unknown:
        print("unknown arm(s): %s; allowed safe arms are lan,offline" %
              ",".join(unknown))
        return 2
    unsafe_requested = "shaped" in requested_arms
    if unsafe_requested:
        print_retired_shape_guidance()
    arms = [x for x in requested_arms if x != "shaped"]
    if not arms:
        return 2

    dirs = pick_dirs(a.root, a.dirs)
    if len(dirs) < 5:
        print("only %d dirs under %s — not a walk" % (len(dirs), a.root))
        return 3
    print("corpus: %d directories under %s" % (len(dirs), a.root))

    print("warming (the point is to measure the WARM cost)…")
    walk(dirs)
    walk(dirs)

    results = []
    for arm in arms:
        if arm == "offline":
            if not set_offline(True):
                print("  offline: SKIP (toggle unavailable)")
                continue
            time.sleep(3)

        r = run_arm(arm, dirs, a.runs)
        if r and not r["valid"]:
            print("  %-8s INVALID — 0 server-side readdir ops; the macOS client "
                  "attr cache served every listing, so nothing about JuiceMount "
                  "was measured" % arm)
        if r:
            results.append(r)
            s = r["dir_open_ms"]
            c = r["backend_cost_median"]
            print("  %-8s dir-open p50 %.2f ms (range %.2f-%.2f, n=%d)  "
                  "meta_ops+%s  object_get+%s  cache_miss+%s  [readdir_ops=%d]"
                  % (arm, s["median"], s["min"], s["max"], s["n"],
                     c.get("meta_ops"), c.get("object_get_bytes"), c.get("cache_miss"),
                     r.get("readdir_ops_total", 0)))

        if arm == "offline":
            set_offline(False)
            time.sleep(2)

    doc = harness.result("nav_parity", [r["dir_open_ms"]["median"] for r in results],
                         "ms_per_dir_open", corpus="%d dirs" % len(dirs),
                         extra={"arms": results})
    if a.out:
        json.dump(doc, open(a.out, "w"), indent=2)
        print("  wrote %s" % a.out)

    # --- pass bar 1: warm navigation must not cost backend operations
    print("\n  PASS BAR 1 — warm navigation costs ~0 backend ops")
    bad = []
    for r in results:
        if not r.get("valid"):
            print("    %-8s SKIP — arm was invalid (no server-side readdir)" % r["arm"])
            continue
        mo = r["backend_cost_median"].get("meta_ops")
        if mo is None:
            print("    %-8s meta_ops UNAVAILABLE — cannot judge" % r["arm"])
        elif mo > 0:
            bad.append((r["arm"], mo))
            print("    %-8s meta_ops +%s  <-- re-validating against Redis on a warm walk"
                  % (r["arm"], mo))
        else:
            print("    %-8s meta_ops +%s" % (r["arm"], mo))

    # --- pass bar 2: arms indistinguishable
    if len(results) > 1:
        print("\n  PASS BAR 2 — arms indistinguishable")
        base = results[0]
        for r in results[1:]:
            v = harness.compare({"stats": base["dir_open_ms"],
                                 "envelope": {"link": {"class": "x", "class_measured": True}}},
                                {"stats": r["dir_open_ms"],
                                 "envelope": {"link": {"class": "x", "class_measured": True}}})
            print("    %-8s vs %-8s %s — %s" % (r["arm"], base["arm"],
                                                v["verdict"], v["reason"]))
    if bad:
        return 1
    # Safe arms may still have produced useful output, but a caller that asked
    # for the retired shaped arm must see a non-success status rather than
    # silently believing the requested experiment ran.
    return 2 if unsafe_requested else 0


if __name__ == "__main__":
    sys.exit(main())
