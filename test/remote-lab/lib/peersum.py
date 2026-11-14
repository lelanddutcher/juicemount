#!/usr/bin/env python3
"""Summarize one peer-edit-lab.sh session: visibility delays per case, and the
reader juicefs's metadata operations each case cost (from /jfs/.stats
snapshots taken before and after it).

    peersum.py OUTDIR LABEL
"""
import json
import os
import re
import statistics
import sys
from collections import defaultdict

LINE = re.compile(r'^(juicefs_[A-Za-z_]+)\s+([0-9.e+]+)$')


def counters(path):
    """The reader juicefs's counters: meta:<Method> operations, txn
    (metadata write transactions) and obj:GET (object downloads)."""
    out = {}
    try:
        with open(path) as f:
            for line in f:
                m = LINE.match(line.strip())
                if not m:
                    continue
                name, val = m.groups()
                if name.startswith("juicefs_meta_ops_total_"):
                    key = "meta:" + name[len("juicefs_meta_ops_total_"):]
                elif name == "juicefs_transaction_durations_histogram_seconds_total":
                    key = "txn"
                elif name == "juicefs_object_request_durations_histogram_seconds_GET_total":
                    key = "obj:GET"
                else:
                    continue
                out[key] = float(val)
    except OSError:
        pass
    return out


def fmt(xs):
    seen = [x for x in xs if x is not None and x >= 0]
    never = sum(1 for x in xs if x is not None and x < 0)
    if not xs:
        return "-"
    s = "n=%d" % len(xs)
    if seen:
        s += " median %.2f s, max %.2f s" % (statistics.median(seen), max(seen))
    if never:
        s += ", NEVER %d" % never
    return s


def main():
    out, label = sys.argv[1], sys.argv[2]
    rows = []
    with open(os.path.join(out, label + ".jsonl")) as f:
        for line in f:
            line = line.strip()
            if line.startswith("{"):
                rows.append(json.loads(line))
    by = defaultdict(list)
    for r in rows:
        if "error" in r or (r["case"] == "steady" and "mean_read_s" not in r):
            print("  error in %s %s: %s" % (r["case"], r["file"], r.get("error", "no result")))
            continue
        by[r["case"]].append(r)
    for kase in ("steady", "held", "fresh", "append", "append-held"):
        rs = by.get(kase, [])
        if not rs:
            continue
        print("== %s (%d)" % (kase, len(rs)))
        if kase == "steady":
            print("  mean read %.4f s, worst %.4f s" % (
                statistics.mean(r["mean_read_s"] for r in rs), max(r["worst_read_s"] for r in rs)))
        for k in ("data_visible_s", "mtime_visible_s", "size_visible_s"):
            xs = [r[k] for r in rs if k in r]
            if xs:
                print("  %-16s %s" % (k, fmt(xs)))
        firsts = [r["data_first_new_read_s"] for r in rs if r.get("data_visible_s", -1) >= 0 and "data_first_new_read_s" in r]
        if firsts:
            print("  first new READ took median %.3f s" % statistics.median(firsts))
        reopen = [r.get("reopen_new") for r in rs if "reopen_new" in r]
        if reopen:
            print("  after NEVER, a new lookup+read saw new: %s" % reopen)
        # Metadata operations the reader's juicefs ran during the case.
        delta = defaultdict(float)
        n = 0
        for f in sorted(os.listdir(out)):
            m = re.match(re.escape(label) + r"\.%s\.(\d+)\.before\.txt$" % re.escape(kase), f)
            if not m:
                continue
            a = counters(os.path.join(out, f))
            b = counters(os.path.join(out, f.replace(".before.", ".after.")))
            for k in set(a) | set(b):
                d = b.get(k, 0) - a.get(k, 0)
                if d:
                    delta[k] += d
            n += 1
        if n:
            ops = ", ".join("%s %.1f" % (k, v / n) for k, v in sorted(delta.items()) if k != "txn")
            print("  reader juicefs per trial: %s; txn %.1f" % (ops or "none", delta.get("txn", 0) / n))


if __name__ == "__main__":
    main()
