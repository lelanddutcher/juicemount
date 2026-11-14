#!/usr/bin/env python3
"""Summarize roam-lab runs: time to recover per worker.

  roamsum.py RESULTS/LABEL-CASE [...]      (or the .events files)

For each run it reads <run>.events (the script's marks), <run>.jsonl (the
probe's timeline) and <run>.verify.json, and prints one JSON line and one
Markdown table row. Times are seconds.

  recover   from the last "restore" mark to the end of the first successful
            operation after which the worker never again went 10 s without
            a success until the stop (an operation that was waiting through
            the outage and then completed counts: it resumed); "never" when
            it was still stalled at the stop
  gap       the longest time between two successful operations of a worker
            in the whole run
  errors    failed operations between "cut" and "stop" ("invalid argument"
            is NFS3ERR_JUKEBOX, the server's "try later": a Mac retries it)
  stuck     an operation still running when the probe stopped (seconds)
"""
import json
import os
import sys

WORKERS = ["read", "write", "meta", "fuse"]
STALL_MS = 10_000


def load(path):
    out = []
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        out.append(json.loads(line))
                    except ValueError:
                        pass
    except OSError:
        pass
    return out


def summarize(run):
    ev = {}
    for e in load(run + ".events"):
        ev.setdefault(e["event"], []).append(e)
    cut = ev["cut"][0]["t"]
    restores = [e["t"] for e in ev.get("restore", []) + ev.get("restore-back", [])]
    restore = max(restores)
    first_restore = min(e["t"] for e in ev.get("restore", []))
    stop = ev.get("stop", [{"t": None}])[0]["t"]
    tl = load(run + ".jsonl")
    res = {"run": os.path.basename(run), "outage_s": round((first_restore - cut) / 1000, 1)}
    for w in WORKERS:
        rows = [r for r in tl if r.get("w") == w and "ok" in r]
        # A read recovers when bytes flow again, not when the file reopens.
        oks = [r["t"] for r in rows if r["ok"] and (w != "read" or r.get("op") == "read")]
        # Recovered for good: the first success after the restore with no
        # gap over STALL seconds between successes from there to the stop.
        after = [t for t in oks if t > restore]
        steady = None
        end = stop or (after[-1] if after else restore)
        done = [r["t"] for r in rows if r.get("op") == "done"]
        if done:  # a read that reached the end of its file stops there
            end = min(end, done[0])
        for i, t in enumerate(after):
            rest = after[i:] + [end]
            if all(b - a <= STALL_MS for a, b in zip(rest, rest[1:])):
                steady = t
                break
        after = [steady] if steady else []
        errs = [r for r in rows if not r["ok"] and r["t"] >= cut and (stop is None or r["t"] <= stop)]
        gap = max((b - a for a, b in zip(oks, oks[1:])), default=0)
        stuck = [r for r in tl if r.get("w") == w and r.get("stuck")]
        d = {
            "recover": round((after[0] - restore) / 1000, 1) if after else None,
            "gap": round(gap / 1000, 1),
            "errors": len(errs),
            "err_kinds": sorted({(r.get("err") or "")[:70] for r in errs})[:4],
        }
        if stuck:
            d["stuck"] = round(stuck[0]["ms"] / 1000, 1)
            d["stuck_op"] = stuck[0].get("op")
        if w == "read":
            offs = [r for r in rows if r["ok"] and "off" in r]
            d["mib_at_cut"] = max([r["off"] for r in offs if r["t"] <= cut] or [0]) >> 20
            d["mib_end"] = max([r["off"] for r in offs] or [0]) >> 20
            d["reopens"] = sum(1 for r in rows if r.get("op") == "open" and r["ok"]) - 1
        if w == "write":
            d["acked"] = sum(1 for r in rows if r["ok"])
            d["retried"] = sum(1 for r in rows if r["ok"] and r.get("try", 0) > 0)
        res[w] = d
    reach = [r for r in tl if r.get("w") == "reach"]
    res["reach"] = [[round((r["t"] - cut) / 1000, 1), r["up"]] for r in reach]
    downs = [r["t"] for r in reach if not r["up"]]
    ups = [r["t"] for r in reach if r["up"] and r["t"] > restore - 1]
    res["reach_down_after_s"] = round((downs[0] - cut) / 1000, 1) if downs else None
    res["reach_up_after_restore_s"] = round((ups[0] - restore) / 1000, 1) if ups else None
    spool = [r for r in tl if r.get("w") == "spool" and isinstance(r.get("spool"), dict)]
    pend = [(r["spool"].get("pending_files") or 0) for r in spool]
    res["spool_max_pending"] = max(pend or [0])
    # The proxy's lease records (lease_poll): the longest time without a
    # heartbeat as the proxy saw it, and every epoch the project went through.
    hb_gap, epochs, states = 0, set(), set()
    for r in load(run + ".leases.jsonl"):
        for rec in r.get("leases") or []:
            if not isinstance(rec, list):
                continue
            d = dict(zip(rec[0::2], rec[1::2]))
            if d.get("label", "").startswith("/roam"):
                epochs.add(d.get("epoch"))
                states.add(d.get("state"))
                if d.get("last_hb_ms"):
                    hb_gap = max(hb_gap, r["t"] - d["last_hb_ms"])
    res["lease_hb_gap_s"] = round(hb_gap / 1000, 1)
    res["lease_epochs"] = sorted(e for e in epochs if e is not None)
    res["lease_states"] = sorted(s for s in states if s)
    drained = ev.get("drained", [{}])[0]
    res["drained_pending"] = drained.get("pending")
    res["drain_after_stop_s"] = drained.get("after_s")
    v = load(run + ".verify.json")
    if v:
        v = v[0]
        res["verify"] = {"ok": v.get("ok"), "acked": v.get("acknowledged"), "on_mount": v.get("on_mount"),
                         "missing": len(v.get("missing") or []), "wrong": len(v.get("wrong") or []),
                         "extra": len(v.get("extra") or [])}
    return res


def row(r):
    def cell(w):
        d = r[w]
        if d.get("stuck") is not None and d.get("recover") is None:
            return "STUCK %ss (%s)" % (d["stuck"], d.get("stuck_op"))
        rec = "never" if d["recover"] is None else "%.1f" % d["recover"]
        return "%s (gap %.0f, err %d)" % (rec, d["gap"], d["errors"])
    v = r.get("verify") or {}
    ver = "ok" if v.get("ok") else "FAIL m%s w%s x%s" % (v.get("missing"), v.get("wrong"), v.get("extra"))
    return "| %s | %s | %s | %s | %s | %s | %s / %s | %s (%s) | %s | %s |" % (
        r["run"], r["outage_s"], cell("read"), cell("write"), cell("meta"), cell("fuse"),
        r["reach_down_after_s"], r["reach_up_after_restore_s"], r["lease_hb_gap_s"],
        ",".join(str(e) for e in r["lease_epochs"]), r["drained_pending"], ver)


def main():
    runs = [a[:-len(".events")] if a.endswith(".events") else a for a in sys.argv[1:]]
    out = [summarize(r) for r in runs]
    for r in out:
        print(json.dumps(r))
    print("| run | outage s | read | write | meta (NFS) | fuse | reach down / up s | lease hb gap s (epochs) | left in spool | files |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for r in out:
        print(row(r))


if __name__ == "__main__":
    main()
