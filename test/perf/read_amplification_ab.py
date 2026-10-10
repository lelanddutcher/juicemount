#!/usr/bin/env python3
"""read_amplification_ab.py — bytes pulled per byte requested, and what it costs.

THE QUESTION F1 ACTUALLY ASKS. A 64 KiB preview-shaped read of a cold file made
JuiceMount fetch 32.9 MB from the object store — 503x amplification measured on
class=fast. The instinct is "cut the readahead". The record says do not: lever A
(NFS-client readahead 16 -> 4) was tested live on 10GbE on 2026-07-07 and bought
a 3.1x byte cut for only a 1.25x browse win, because smaller per-file transfers
re-expose per-file backend latency. 16 is also the floor validated against
concurrent-read truncation (#18/#19, which appeared at 128). So the LAN number
is deliberate, not a defect.

WHAT IS GENUINELY UNMEASURED is the other half: the product ships a class-gated
mitigation for exactly this problem — on a metered link juicefs drops to
--prefetch 0, the client readahead drops to 2, and --max-readahead is capped at
1M. That policy IS the answer to "be the most efficient with bandwidth on a
cellular connection", and nobody has ever put a number on it off a real cellular
link. This harness does, because JM_NET_FORCE_CLASS pins the class at startup and
the whole mount-time policy stack follows.

TWO ARMS, BOTH REQUIRED — a byte cut that halves playback is not a win:
  amplification   bytes pulled per byte requested on a preview-shaped read
  sequential      MB/s and bytes pulled reading a large file straight through

COLDNESS IS THE WHOLE MEASUREMENT, so replicates must be DISJOINT. Reading the
same 25 files six times measures the cache five times and the product once. Each
replicate here takes its own slice of a stable, sorted, never-touched file list,
and the run refuses to report if cache_miss did not move — a warm arm reporting
"1x amplification" would be a number from no coverage.
"""

import argparse
import json
import os
import sys
import time
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib"))
import harness  # noqa: E402

CONTROL = os.environ.get("JM_CONTROL", "http://127.0.0.1:11050")


def snap():
    try:
        with urllib.request.urlopen(CONTROL + "/metrics", timeout=10) as r:
            m = json.load(r)
    except Exception:
        return None
    b = m.get("backend") or {}
    rpc = m.get("rpcs") or {}
    n = m.get("network") or {}
    return {
        "object_get_bytes": b.get("object_get_bytes"),
        "cache_miss": b.get("cache_miss"),
        "meta_ops": b.get("meta_ops"),
        "read_ops": (rpc.get("READ") or {}).get("count", 0),
        "class": n.get("class"),
    }


def big_files(root, min_mb):
    """Stable, sorted, size-filtered. Sorted so replicate slices are reproducible
    across arms — arm A replicate 3 and arm B replicate 3 must be the SAME files
    or the comparison is between corpora, not between policies."""
    out = []
    for cur, subs, names in os.walk(root):
        subs[:] = [s for s in subs if not s.startswith(".")]
        for n in names:
            if n.startswith("."):
                continue
            p = os.path.join(cur, n)
            try:
                sz = os.path.getsize(p)
            except OSError:
                continue
            if sz >= min_mb * 1024 * 1024:
                out.append(p)
        if len(out) > 600:
            break
    return sorted(out)


# THE HEADLINE METRIC IS BYTES PULLED PER FILE TOUCHED. Two earlier candidates
# were both corpus-dependent garbage, and the measurements that killed them are
# worth keeping because both looked completely reasonable first:
#
#   "Nx vs bytes requested" — if the policy pulled whole files this is just
#   filesize/requested, so a slice of 4 GB camera masters scores 1421x and a
#   slice of 5 MB files scores 80x under the IDENTICAL policy. Three arms came
#   back 466x / 80x / 773x and the spread was mostly file sizes.
#
#   "fraction of touched files pulled" — assumed a whole-file pull, and the
#   measurement says otherwise: across replicates whose corpora ranged 1.9 GB to
#   16.3 GB, the pull stayed ~44-88 MiB PER FILE. So the fraction just tracked
#   the denominator (0.062 on the 16 GB slice, 0.428 on a 2.5 GB one) and said
#   nothing about policy.
#
# What is actually invariant is the per-file pull: readahead drags a bounded
# chunk of each file it touches, and THAT is the quantity the class-gated
# mitigation changes. Reported alongside median file size, because the metric is
# implicitly capped by it — if a slice's files are smaller than the pull, the
# number reads low for a reason that has nothing to do with the policy, and the
# reader has to be able to see that.
#
# Cold-ness cannot be repeated on the same files (the first read warms them), so
# arms are FORCED onto different slices and a slice-robust metric is not a nicety.
def measure(files, read_bytes, whole=False):
    a = snap()
    if a is None:
        return {"valid": False, "reason": "control plane unreachable"}
    got = 0
    corpus_bytes = 0
    for f in files:
        try:
            corpus_bytes += os.path.getsize(f)
        except OSError:
            pass
    t0 = time.perf_counter()
    for f in files:
        try:
            with open(f, "rb") as fh:
                if whole:
                    while True:
                        chunk = fh.read(8 << 20)
                        if not chunk:
                            break
                        got += len(chunk)
                else:
                    got += len(fh.read(read_bytes))
        except OSError:
            pass
    wall = time.perf_counter() - t0
    time.sleep(3)  # async backend counters settle
    b = snap()
    if b is None:
        return {"valid": False, "reason": "control plane vanished mid-run"}
    d = {k: (None if a.get(k) is None or b.get(k) is None or not isinstance(a.get(k), (int, float))
             else b[k] - a[k]) for k in a}

    if (d.get("read_ops") or 0) <= 0:
        return {"valid": False,
                "reason": "0 READ RPCs — the macOS page cache served it; nothing measured"}
    if d.get("object_get_bytes") is None:
        return {"valid": False, "reason": "backend.object_get_bytes absent from /metrics"}
    if (d.get("cache_miss") or 0) <= 0:
        return {"valid": False,
                "reason": ("cache_miss +0 — these files were already in the block cache, "
                           "so this is a WARM read. Amplification from a warm arm is "
                           "meaningless; use an untouched replicate slice.")}
    pulled = d["object_get_bytes"]
    return {
        "valid": True, "requested": got, "pulled": pulled,
        "corpus_bytes": corpus_bytes,
        # THE headline: what fraction of the touched files did we drag over the
        # link to satisfy a small read? ~1.0 = whole-file pull.
        "pull_fraction": (pulled / corpus_bytes) if corpus_bytes else None,
        "pulled_per_file": (pulled / len(files)) if files else None,
        "median_file_bytes": (sorted(os.path.getsize(f) for f in files)[len(files) // 2]
                              if files else None),
        "amplification": (pulled / got) if got else None,
        "wall_s": wall,
        "mb_per_s": (got / 1048576.0) / wall if wall else 0,
        "read_ops": d["read_ops"], "cache_miss": d["cache_miss"],
        "class": b.get("class"),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="/Volumes/zpool/oldzpool/ARCHIVE")
    ap.add_argument("--min-mb", type=int, default=8)
    ap.add_argument("--per-rep", type=int, default=12,
                    help="files per amplification replicate (disjoint across replicates)")
    ap.add_argument("--runs", type=int, default=6, help="n>=6; see harness.MIN_N")
    ap.add_argument("--read-bytes", type=int, default=65536)
    ap.add_argument("--label", default="arm", help="tag for this arm, e.g. fast / metered")
    ap.add_argument("--skip", type=int, default=0,
                    help="offset into the file list; use a fresh offset per ARM so the "
                         "second arm is not reading what the first one warmed")
    ap.add_argument("--seq", action="store_true", help="also run the sequential-throughput arm")
    ap.add_argument("--out", default="")
    a = ap.parse_args()

    s0 = snap()
    if s0 is None:
        print("control plane unreachable at %s" % CONTROL)
        return 3
    print("arm=%s  live class=%s" % (a.label, s0.get("class")))

    files = big_files(a.root, a.min_mb)
    need = a.skip + a.runs * a.per_rep + (a.runs if a.seq else 0)
    print("corpus: %d files >=%dMiB  (need %d)" % (len(files), a.min_mb, need))
    if len(files) < need:
        print("NOT ENOUGH untouched files for %d disjoint replicates — refusing to "
              "reuse warm files" % a.runs)
        return 3

    amps, seqs, reps = [], [], []
    cur = a.skip
    for i in range(a.runs):
        sl = files[cur:cur + a.per_rep]
        cur += a.per_rep
        r = measure(sl, a.read_bytes)
        reps.append(r)
        if not r.get("valid"):
            print("  rep %d/%d  INVALID — %s" % (i + 1, a.runs, r["reason"]))
            continue
        amps.append(r["pulled_per_file"] / 1048576.0)
        print("  rep %d/%d  median file %7.1f MiB   pulled %7.1f MiB/file   "
              "(%.3f of corpus, %.0fx vs requested)"
              % (i + 1, a.runs, r["median_file_bytes"] / 1048576.0,
                 r["pulled_per_file"] / 1048576.0,
                 r["pull_fraction"], r["amplification"]))

    if a.seq:
        print("\n  sequential arm (must-not-regress side)")
        for i in range(a.runs):
            f = files[cur]
            cur += 1
            r = measure([f], 0, whole=True)
            if not r.get("valid"):
                print("    seq %d/%d INVALID — %s" % (i + 1, a.runs, r["reason"]))
                continue
            seqs.append(r["mb_per_s"])
            print("    seq %d/%d  %8.1f MiB in %5.2fs = %6.1f MB/s  (pulled %.1f MiB)"
                  % (i + 1, a.runs, r["requested"] / 1048576.0, r["wall_s"],
                     r["mb_per_s"], r["pulled"] / 1048576.0))

    if not amps:
        print("\nEVERY replicate was invalid — no amplification number is reportable.")
        return 3

    doc = harness.result("read_amplification", amps, "MiB_pulled_per_file_touched",
                         corpus="%dx%d files >=%dMiB" % (a.runs, a.per_rep, a.min_mb),
                         note="arm=%s" % a.label,
                         extra={"replicates": reps,
                                "sequential_mb_per_s": harness.stats(seqs) if seqs else None})
    st = doc["stats"]
    med_file = sorted(r["median_file_bytes"] for r in reps if r.get("valid"))
    print("\n  PULLED PER FILE  n=%d  median %.1f MiB  range %.1f-%.1f MiB"
          % (st["n"], st["median"], st["min"], st["max"]))
    print("  (for a %d KiB read of each file; slice median file size %.1f MiB — the "
          "metric is capped by this, so a low number on a small-file slice is not a win)"
          % (a.read_bytes // 1024, med_file[len(med_file) // 2] / 1048576.0))
    if seqs:
        ss = harness.stats(seqs)
        print("  SEQUENTIAL     n=%d  median %.1f MB/s  range %.1f-%.1f"
              % (ss["n"], ss["median"], ss["min"], ss["max"]))

    if a.out:
        json.dump(doc, open(a.out, "w"), indent=2)
        print("  wrote %s" % a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
