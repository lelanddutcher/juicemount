#!/usr/bin/env python3
"""Summarise a heldread.py run: heldsum.py RUN (RUN.events, RUN.held.jsonl)."""
import json
import sys

run = sys.argv[1]
ev = {e["event"]: e["t"] for e in map(json.loads, open(run + ".events"))}
reads = [json.loads(l) for l in open(run + ".held.jsonl")]
cut, back = ev["cut"], ev["restore"]
after = [r for r in reads if r["t"] >= back]
first_ok = next((r for r in after if r["ok"]), None)
errs = sorted({r["err"] for r in reads if not r["ok"]})
fail_during = [r for r in reads if not r["ok"] and cut <= r["t"] < back]
fail_after = [r for r in after if not r["ok"]]
print(json.dumps({
    "run": run.rsplit("/", 1)[-1],
    "outage_s": round((back - cut) / 1000, 1),
    "reads": len(reads),
    "first_error_after_cut_s": round((fail_during[0]["t"] - cut) / 1000, 1) if fail_during else None,
    "errors_during": len(fail_during),
    "errors_after_restore": len(fail_after),
    "reads_back_after_restore_s": round((first_ok["t"] + first_ok["ms"] - back) / 1000, 2) if first_ok else None,
    "err_kinds": errs,
    "mib_read": sum(1 for r in reads if r["ok"]),
}))
