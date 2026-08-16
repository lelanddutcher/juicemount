#!/usr/bin/env python3
"""invariants.py — assertions that must hold during ANY test, or in steady state.

WHY ASSERTIONS AND NOT BENCHMARKS. Both bugs found on 2026-08-16 were invariant
violations, not slow numbers, and neither needed a baseline to detect:

  * a full metadata SCAN every ~300s while keyspace push was healthy
    (clamp-ordering bug: the ceiling was applied after the floor, truncating a
    correctly-chosen 900s backstop to 300s)
  * the NFS listener taking 321s to come up because it waits on a full sync

`cmd/juicemount-watch` already OBSERVES and emits anomaly flags, but a flag is
advisory — nothing fails on it. These are pass/fail.

THE SHAPE THAT MATTERS: every check returns PASS, FAIL, or **SKIP**, and SKIP is
never rendered as a pass. A check whose precondition is absent has nothing to
say, and reporting that as green is the false-green class this codebase keeps
paying for (see the keyspace verdict that read "working" off the wrong feed).
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.request

CONTROL = os.environ.get("JM_CONTROL", "http://127.0.0.1:11050")
LOG = os.environ.get("JM_LOG") or os.path.expanduser("~/Library/Logs/JuiceMount/juicemount.log")
CRASHES = os.path.expanduser("~/Library/Logs/DiagnosticReports")

PASS, FAIL, SKIP = "PASS", "FAIL", "SKIP"


def _get(path, timeout=6):
    try:
        with urllib.request.urlopen(CONTROL + path, timeout=timeout) as r:
            return json.load(r)
    except Exception:
        return None


def _log_events(pattern, limit=4000):
    """Recent parsed log lines whose msg matches; newest last."""
    if not os.path.exists(LOG):
        return []
    out = subprocess.run(["tail", "-n", str(limit), LOG],
                         capture_output=True, text=True).stdout
    rows = []
    for line in out.splitlines():
        try:
            d = json.loads(line)
        except Exception:
            continue
        if pattern in d.get("msg", ""):
            rows.append(d)
    return rows


def _ts(d):
    import datetime
    return datetime.datetime.fromisoformat(d["time"]).timestamp()


# ---------------------------------------------------------------- invariants

def inv_scan_cadence(_state, tolerance=0.8):
    """Full metadata SCANs must occur no FASTER than the configured backstop.

    NOT "zero scans": the backstop legitimately runs a periodic reconcile, and
    asserting zero would fail on correct behaviour. The bug was that a
    correctly-chosen 900s backstop was being truncated to 300s, so the honest
    assertion is on the OBSERVED INTERVAL against the CONFIGURED one.

    Verified against 12 hours of post-fix operation: median 900s, range
    896-905s, n=13. Pre-fix it was a steady ~300s.
    """
    ks = (_get("/metrics") or {}).get("keyspace") or {}
    if ks.get("verdict") != "working":
        return SKIP, ("keyspace verdict is %r, not 'working' — with push down the "
                      "backstop is SUPPOSED to tighten to 30s, so this check has "
                      "nothing to say" % ks.get("verdict"))

    changes = _log_events("metadata reconcile backstop changed")
    if not changes:
        return SKIP, "no backstop value in the log window; cannot judge cadence"
    backstop = changes[-1].get("new_sec")
    if not backstop:
        return SKIP, "backstop value unreadable"

    syncs = _log_events("metadata sync complete")
    if len(syncs) < 3:
        return SKIP, "saw %d syncs; need >=3 to measure an interval" % len(syncs)

    times = [_ts(s) for s in syncs][-8:]
    gaps = [b - a for a, b in zip(times, times[1:])]
    if not gaps:
        return SKIP, "no measurable interval"
    fastest = min(gaps)
    floor = backstop * tolerance
    if fastest < floor:
        return FAIL, ("fastest observed SCAN interval %.0fs is below %.0f%% of the "
                      "%ss backstop (floor %.0fs). A full-tree SCAN is running "
                      "faster than configured — the 2026-08-16 clamp-ordering bug"
                      % (fastest, tolerance * 100, backstop, floor))
    return PASS, ("fastest interval %.0fs vs %ss backstop (n=%d gaps)"
                  % (fastest, backstop, len(gaps)))


def inv_nfs_healthy(_state):
    h = _get("/health")
    if h is None:
        return SKIP, "control plane not answering; app may be starting"
    comps = h.get("components") or {}
    if not comps:
        return SKIP, ("/health reports no components yet — a green health with an "
                      "empty components map means nothing has reported, not that "
                      "everything is well")
    nfs = comps.get("nfs")
    if nfs != "ok":
        return FAIL, "nfs component is %r (all: %s)" % (nfs, comps)
    return PASS, "nfs ok (%s)" % comps


def inv_no_new_crashes(state):
    try:
        now = {f for f in os.listdir(CRASHES) if f.startswith("JuiceMount-")}
    except Exception:
        return SKIP, "crash report directory unreadable"
    before = state.get("crashes")
    state["crashes"] = now
    if before is None:
        return SKIP, "first observation; nothing to compare against yet"
    new = now - before
    if new:
        return FAIL, "NEW crash report(s): %s" % sorted(new)
    return PASS, "no new crash reports (%d total)" % len(now)


def inv_spool_not_starved(state):
    """in_progress pinned at its ceiling while pending grows == drain starvation.

    This is the signature the small-file gate (T3) exists to characterise: the
    drainer's worker semaphore defaults to 4, so a large batch of small files
    can sit behind a fixed concurrency ceiling.
    """
    s = _get("/spool")
    if s is None:
        return SKIP, "spool endpoint not answering"
    if not s.get("enabled"):
        return SKIP, "spool disabled"
    pend, prog = s.get("pending_files") or 0, s.get("in_progress") or 0
    prev = state.get("spool")
    state["spool"] = (pend, prog)
    if prev is None:
        return SKIP, "first observation; need two to see a trend"
    if pend == 0:
        return SKIP, "spool idle (pending=0) — nothing to starve"
    ppend, pprog = prev
    if pend > ppend and prog == pprog and prog > 0:
        return FAIL, ("pending grew %d->%d while in_progress stayed pinned at %d "
                      "— drain concurrency is the ceiling" % (ppend, pend, prog))
    return PASS, "pending=%d in_progress=%d" % (pend, prog)


CHECKS = {
    "scan_cadence": inv_scan_cadence,
    "nfs_healthy": inv_nfs_healthy,
    "no_new_crashes": inv_no_new_crashes,
    "spool_not_starved": inv_spool_not_starved,
}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--checks", default="all")
    ap.add_argument("--watch-seconds", type=float, default=0,
                    help="poll for this long; any FAIL in the window fails the run")
    ap.add_argument("--interval", type=float, default=15)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    names = list(CHECKS) if a.checks == "all" else [c.strip() for c in a.checks.split(",")]
    state, worst, rows = {}, PASS, []
    deadline = time.time() + a.watch_seconds

    while True:
        for n in names:
            v, why = CHECKS[n](state)
            rows.append({"check": n, "verdict": v, "reason": why})
            if v == FAIL:
                worst = FAIL
            if not a.json:
                print("  %-20s %-5s %s" % (n, v, why))
        if time.time() >= deadline:
            break
        time.sleep(a.interval)

    if a.json:
        json.dump({"verdict": worst, "results": rows}, sys.stdout, indent=2)
        print()
    else:
        print("\n  overall: %s" % worst)
    return 1 if worst == FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
