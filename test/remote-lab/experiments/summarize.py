#!/usr/bin/env python3
"""summarize.py FILE.jsonl...: one markdown row per (profile, config, cache)."""
import json, sys

PROF = ["lan", "same-city", "abroad", "abroad-noloss", "hotel", "cellular"]


def s(ms):
    if ms is None:
        return "-"
    return f"{ms / 1000:.2f} s" if ms >= 1000 else f"{ms:.0f} ms"


recs = [json.loads(l) for f in sys.argv[1:] for l in open(f) if l.strip()]
recs.sort(key=lambda r: (PROF.index(r["profile"]), r["config"]))
print("| Profile | Config | Cache | iperf down/up | TTFB @0 | TTFB ranged @4K | Seq (after 1st byte) | Scrub median / p90 (done) | 32 MiB upload | 64 KiB file write median |")
print("|---|---|---|---|---|---|---|---|---|---|")
for r in recs:
    if "error" in r:
        print(f"| {r['profile']} | {r['config']} | - | error: {r['error'].strip().splitlines()[-1][:80]} |")
    for c in ("cold", "warm"):
        if c not in r or "scrub" not in r[c]:
            continue
        d = r[c]
        ip = f"{r.get('iperf_down', '-')}/{r.get('iperf_up', '-')}" if c == "cold" else ""
        q = d["seq"]
        seq = f"{q['mbit_s_after_first']:.1f} Mbit/s" + ("" if q["complete"] else f" ({q['bytes'] >> 20} MiB)")
        sc = d["scrub"]
        scr = f"{s(sc['read']['median_ms'])} / {s(sc['read']['p90_ms'])} ({sc['done']})"
        up = f"{r['bigwrite']['mbit_s']:.1f} Mbit/s" if c == "cold" and "bigwrite" in r else ""
        sw = s(r["smallwrite"]["per_file"]["median_ms"]) if c == "cold" and "smallwrite" in r else ""
        print(f"| {r['profile']} | {r['config']} | {c} | {ip} | {s(d['ttfb0']['ttfb_ms'])} | "
              f"{s(d.get('ttfb_ranged', {}).get('ttfb_ms'))} | {seq} | {scr} | {up} | {sw} |")
