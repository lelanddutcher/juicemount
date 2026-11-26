#!/usr/bin/env python3
"""enginesum.py FILE.jsonl... — the T1-T5 table from engineprobe result lines.

Groups lines by profile (the label up to "-r<N>", plus any suffix after the
round) and arm, pooling the rounds. Prints one Markdown table per profile.

T1  serial metadata round trips before the first byte (relay rt_serial over
    open + first read), median of the later clips; the first clip in brackets
T2  open -> first MiB, ms: median of the later clips; the first clip of each
    round in brackets
T3  two 1 MiB reads at once / one read (ratio of the medians)
T4  peer edit -> new bytes on the held handle, ms (median); refresh: after the
    JMRefreshInode message; none: no message ("never" = not within the timeout)
T5  files per second, 200 small files into one folder (create, write, flush,
    rename), per worker count; metadata round trips per file
"""
import json
import re
import statistics
import sys
from collections import defaultdict


def med(xs):
    xs = [x for x in xs if isinstance(x, (int, float))]
    return statistics.median(xs) if xs else None


def fmt(x, nd=0):
    if x is None:
        return "–"
    return f"{x:.{nd}f}"


def group(label):
    m = re.match(r"^(.*?)-r(\d+)(-.*)?$", label or "")
    if not m:
        return label or "?", ""
    return m.group(1), (m.group(3) or "").lstrip("-")


def main(paths):
    rows = defaultdict(lambda: defaultdict(list))
    for p in paths:
        with open(p) as f:
            for line in f:
                line = line.strip()
                if not line.startswith("{"):
                    continue
                r = json.loads(line)
                prof, variant = group(r.get("label"))
                arm = r.get("arm", "?") + (f" ({variant})" if variant else "")
                rows[prof][arm].append(r)
    for prof in sorted(rows):
        print(f"\n### {prof}\n")
        print("| Arm | T1 serial RT before first byte, later [first] | T1 RT total | T2 first MiB ms, later [first] "
              "| open ms (later) | T3 two/one | T4 refresh → new bytes ms | T4 refresh call ms | T4 play read max ms | T4 no message "
              "| T5 files/s w4 / w8 | T5 RT per file |")
        print("|---|---|---|---|---|---|---|---|---|---|---|---|")
        for arm in sorted(rows[prof]):
            rs = rows[prof][arm]
            t12 = [r for r in rs if r.get("test") == "t1t2"]
            later = [r for r in t12 if r.get("i", 0) > 0]
            first = [r for r in t12 if r.get("i", 0) == 0]
            ser = lambda xs: [(r.get("rt_before_data") or {}).get("rt_serial") for r in xs]
            tot = lambda xs: [(r.get("rt_before_data") or {}).get("rt") for r in xs]
            t1 = f"{fmt(med(ser(later)))} [{fmt(med(ser(first)))}]"
            t1t = f"{fmt(med(tot(later)))}"
            t2 = f"{fmt(med([r.get('first_mib_ms') for r in later]))} [{', '.join(fmt(r.get('first_mib_ms')) for r in first)}]"
            op = fmt(med([r.get('open_ms') for r in later]))
            t3s = [r for r in rs if r.get("test") == "t3"]
            one, two = med([r["one_ms"] for r in t3s]), med([r["two_ms"] for r in t3s])
            t3 = f"{two / one:.2f} ({fmt(two)}/{fmt(one)})" if one and two else "–"
            t4r = [r for r in rs if r.get("test") == "t4" and r.get("mode") == "refresh"]
            t4n = [r for r in rs if r.get("test") == "t4" and r.get("mode") == "none"]
            vis = med([r.get("visible_ms") for r in t4r])
            nev = sum(1 for r in t4r if r.get("visible_ms") == "never")
            t4 = fmt(vis) + (f" ({nev} never)" if nev else "") + f" n={len(t4r)}"
            t4call = fmt(med([r.get("refresh_ms") for r in t4r]))
            plays = [r.get("play_read_max_ms") for r in t4r + t4n if isinstance(r.get("play_read_max_ms"), (int, float))]
            t4play = fmt(max(plays)) if plays else "–"
            nvis = [r.get("visible_ms") for r in t4n]
            nnev = sum(1 for v in nvis if v == "never")
            t4none = (f"{nnev}/{len(nvis)} never" if nnev else "") + (
                f" med {fmt(med(nvis))}" if med(nvis) is not None else "")
            t5 = defaultdict(list)
            rtpf = []
            for r in rs:
                if r.get("test") == "t5":
                    t5[r["workers"]].append(r["files_per_s"])
                    if r.get("rt_per_file") is not None:
                        rtpf.append(r["rt_per_file"])
            t5s = " / ".join(f"{fmt(med(t5[w]), 1)}" for w in sorted(t5)) or "–"
            print(f"| {arm} | {t1} | {t1t} | {t2} | {op} | {t3} | {t4} | {t4call} | {t4play} | {t4none.strip() or '–'} | {t5s} | {fmt(med(rtpf), 1)} |")


if __name__ == "__main__":
    main(sys.argv[1:])
