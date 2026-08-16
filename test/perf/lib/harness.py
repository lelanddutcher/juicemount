#!/usr/bin/env python3
"""harness.py — the discipline layer every performance gate stamps its results with.

WHY THIS EXISTS. The gates in this tree could not discriminate, so adding more
benchmarks on top would only have added noise:

  * TestBenchmarkSuite fails at 1.20x while varying 2.2-4.4x run to run. A gate
    that noisy trains everyone to ignore it.
  * test/benchmark_baselines.json says "_updated": "2026-03-25" — months stale,
    LAN-only.
  * scripts/qa-suite/baselines/* are pinned to build 88ccee8.
  * NO baseline records the LINK CLASS, so a LAN baseline can be silently
    compared against a cellular run and the verdict is meaningless.
  * scripts/qa-suite/11-workloads/_common.sh keeps rpcs + bytes_* but DISCARDS
    fuse_attrib and network — throwing away both the per-source attribution and
    the link class.

Three rules, encoded here so no individual harness has to remember them:

 1. STAMP EVERYTHING. Build SHA, link class, whether the class was actually
    measured, corpus id, host, timestamp. A number without its conditions is
    not a measurement.

 2. COMPARE RANGES, NOT RATIOS. n>=6, report median and min/max, and call a
    regression only when the ranges DO NOT OVERLAP. A 1.20x threshold against
    2.2-4.4x natural variance is how the existing gate earned its reputation.

 3. REFUSE TO REPORT. Already proven the right shape in
    test/perf/metrics_delta.py, which exits 3 when rpc_total did not move. A
    number produced from no coverage is worse than silence, because it looks
    like a pass.

The verdict vocabulary is deliberately four-valued. PASS/FAIL alone forces a
cross-class or no-coverage run into one of two wrong answers; INCOMPARABLE and
INVALID say what actually happened.
"""

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import time
import urllib.request

CONTROL_PLANE = os.environ.get("JM_CONTROL", "http://127.0.0.1:11050")


def _sh(cmd, default=""):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                              timeout=15).stdout.strip() or default
    except Exception:
        return default


def _metrics(timeout=6):
    try:
        with urllib.request.urlopen(CONTROL_PLANE + "/metrics", timeout=timeout) as r:
            return json.load(r)
    except Exception:
        return {}


def envelope(corpus="", note=""):
    """The conditions a result was produced under.

    `class_measured` is the honest half. netprofile reports ClassMedium before
    it has any sample, so a class alone cannot be trusted — have_rtt AND
    have_bandwidth must both be true for the class to mean anything. A result
    whose class was assumed rather than measured is not comparable to one whose
    class was measured, even if the strings match.
    """
    m = _metrics()
    net = m.get("network") or {}
    have_rtt = bool(net.get("have_rtt"))
    have_bw = bool(net.get("have_bandwidth"))
    return {
        "schema": 1,
        "ts": int(time.time()),
        "iso": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "build_sha": _sh("git rev-parse --short HEAD", "unknown"),
        "branch": _sh("git rev-parse --abbrev-ref HEAD", "unknown"),
        "dirty": bool(_sh("git status --porcelain")),
        "host": platform.node(),
        "corpus": corpus,
        "note": note,
        "link": {
            "class": net.get("class", "unknown"),
            "rtt_ms": net.get("rtt_ms"),
            "bandwidth_mbps": net.get("bandwidth_mbps"),
            "high_latency": net.get("high_latency"),
            "have_rtt": have_rtt,
            "have_bandwidth": have_bw,
            # False => the class is a default, not an observation.
            "class_measured": have_rtt and have_bw,
        },
        "control_plane_reachable": bool(m),
    }


def stats(samples):
    """Median plus the full range. The range is what the comparison keys on."""
    xs = sorted(float(x) for x in samples)
    if not xs:
        return {"n": 0, "median": None, "min": None, "max": None}
    return {
        "n": len(xs),
        "median": statistics.median(xs),
        "min": xs[0],
        "max": xs[-1],
        "mean": statistics.fmean(xs),
    }


# Verdicts
PASS = "PASS"
FAIL = "FAIL"
INCOMPARABLE = "INCOMPARABLE"
INVALID = "INVALID"

MIN_N = 6


def compare(baseline, current, lower_is_better=True, min_n=MIN_N, treatment=None):
    """Verdict for `current` against `baseline`.

    `treatment` names the independent variable when the experiment DELIBERATELY
    changes something this function otherwise refuses to compare across. Pass
    treatment="link_class" for an A/B whose whole point is the link class — e.g.
    measuring what the class-gated cellular mitigation buys, by pinning
    JM_NET_FORCE_CLASS on one arm. Without it such a run is reported
    INCOMPARABLE, which is right for an ACCIDENTAL class difference (a baseline
    taken on 10GbE and a current taken on cellular really is meaningless) and
    wrong for a declared one.

    It must be DECLARED, never inferred. Auto-detecting "the classes differ so
    they must have meant it" would silently reclassify the exact mistake the
    guard exists to catch as an intentional experiment.

    INVALID   — not enough samples on either side to say anything. The gate did
                not measure enough to have an opinion; that is not a pass.
    INCOMPARABLE — the two runs were produced under different link conditions,
                or under a class that was never actually measured. Comparing a
                cellular run to a LAN baseline produces a number, and the number
                is meaningless; saying so is the only correct answer.
    FAIL      — ranges do NOT overlap and current is on the wrong side.
    PASS      — everything else, including overlapping ranges. Overlap means the
                difference is not separable from run-to-run noise, and calling
                that a regression is exactly the mistake the 1.20x gate makes.
    """
    bs, cs = baseline.get("stats", {}), current.get("stats", {})
    if (bs.get("n") or 0) < min_n or (cs.get("n") or 0) < min_n:
        return {"verdict": INVALID,
                "reason": "need n>=%d on both sides; baseline n=%s current n=%s"
                          % (min_n, bs.get("n"), cs.get("n"))}

    bl = (baseline.get("envelope") or {}).get("link") or {}
    cl = (current.get("envelope") or {}).get("link") or {}
    if not bl.get("class_measured") or not cl.get("class_measured"):
        return {"verdict": INCOMPARABLE,
                "reason": "link class was not MEASURED on one side (baseline=%s/%s "
                          "current=%s/%s); netprofile reports a default class "
                          "before it has samples, so the class is an assumption"
                          % (bl.get("class"), bl.get("class_measured"),
                             cl.get("class"), cl.get("class_measured"))}
    if bl.get("class") != cl.get("class") and treatment != "link_class":
        return {"verdict": INCOMPARABLE,
                "reason": "baseline was taken on '%s', current on '%s' — a "
                          "cross-class comparison yields a number with no meaning "
                          "(pass treatment=\"link_class\" if the class IS the "
                          "experiment)"
                          % (bl.get("class"), cl.get("class"))}

    overlap = not (cs["min"] > bs["max"] or bs["min"] > cs["max"])
    if overlap:
        return {"verdict": PASS,
                "reason": "ranges overlap (baseline %.4g-%.4g, current %.4g-%.4g) — "
                          "not separable from run-to-run noise"
                          % (bs["min"], bs["max"], cs["min"], cs["max"])}

    worse = (cs["median"] > bs["median"]) if lower_is_better else (cs["median"] < bs["median"])
    ratio = (cs["median"] / bs["median"]) if bs["median"] else float("inf")
    return {"verdict": FAIL if worse else PASS,
            "reason": "ranges disjoint (baseline %.4g-%.4g, current %.4g-%.4g); "
                      "median %.4g -> %.4g (%.2fx)"
                      % (bs["min"], bs["max"], cs["min"], cs["max"],
                         bs["median"], cs["median"], ratio)}


def result(name, samples, unit, corpus="", note="", extra=None):
    """A complete, self-describing result document."""
    doc = {"name": name, "unit": unit,
           "envelope": envelope(corpus, note),
           "stats": stats(samples),
           "samples": list(samples)}
    if extra:
        doc["extra"] = extra
    return doc


def main():
    ap = argparse.ArgumentParser(description="harness discipline helpers")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("envelope")
    p = sub.add_parser("stats"); p.add_argument("values", nargs="+", type=float)
    p = sub.add_parser("compare")
    p.add_argument("baseline"); p.add_argument("current")
    p.add_argument("--higher-is-better", action="store_true")
    a = ap.parse_args()

    if a.cmd == "envelope":
        json.dump(envelope(), sys.stdout, indent=2); print(); return 0
    if a.cmd == "stats":
        json.dump(stats(a.values), sys.stdout, indent=2); print(); return 0

    v = compare(json.load(open(a.baseline)), json.load(open(a.current)),
                lower_is_better=not a.higher_is_better)
    json.dump(v, sys.stdout, indent=2); print()
    return {PASS: 0, FAIL: 1, INCOMPARABLE: 2, INVALID: 3}[v["verdict"]]


if __name__ == "__main__":
    sys.exit(main())
