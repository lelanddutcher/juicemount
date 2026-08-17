#!/usr/bin/env python3
"""mixed_drain_bench.py — do media files wait behind 4 KiB sidecars?

THE POINT. The drain semaphore counts FILES. A 4 KiB `._` AppleDouble sidecar
therefore takes a whole slot and blocks on a real MinIO PUT exactly like a 72 MiB
camera master, and sidecars are 50% of all write operations (2.00 CREATE/WRITE/
COMMIT per real file, measured) for 0.0054% of the bytes. With 4 slots, half the
drain capacity serves a rounding error.

WHY THE PURE SMALL-FILE BENCH CANNOT SHOW THIS. smallfile_write_bench.py writes
a uniform 32 KiB corpus — every row is "small", so there is nothing to be unfair
TO. The lane's effect only appears when large and small rows compete, which is
the real shape of every Premiere export and every card offload: media plus a
sidecar per file.

WHAT IS MEASURED: wall time to fully drain a MIXED corpus, and the per-file
completion split BY SIZE CLASS. The number that should move is media-file
latency; total throughput should not, because the ~50 files/s ceiling is
serialized per-file work in the write path, not drain width (F2).

VALIDITY: the run refuses to report unless the spool actually showed depth —
if everything landed before the drainer ever queued, no contention existed and
a "no difference" result would be from no coverage.
"""

import argparse
import json
import os
import shutil
import statistics
import sys
import threading
import time
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib"))
import harness  # noqa: E402

CONTROL = os.environ.get("JM_CONTROL", "http://127.0.0.1:11050")


def spool():
    try:
        with urllib.request.urlopen(CONTROL + "/metrics", timeout=5) as r:
            return (json.load(r).get("spool") or {})
    except Exception:
        return {}


def drain_idle(timeout=600):
    """Wait until the spool reports no pending/in-progress work."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        s = spool()
        pend = s.get("pending_files") or 0
        prog = s.get("in_progress") or 0
        if pend == 0 and prog == 0:
            return time.time() - t0
        time.sleep(0.5)
    return None


def build_corpus(path, n_media, media_mb, n_small, small_kb):
    marker = os.path.join(path, ".mix-%d-%d-%d-%d" % (n_media, media_mb, n_small, small_kb))
    if os.path.exists(marker):
        return False
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)
    big = os.urandom(media_mb * 1024 * 1024)
    small = os.urandom(small_kb * 1024)
    for i in range(n_media):
        with open(os.path.join(path, "media%03d.bin" % i), "wb") as f:
            f.write(big)
    for i in range(n_small):
        with open(os.path.join(path, "small%04d.bin" % i), "wb") as f:
            f.write(small)
    open(marker, "w").close()
    return True


def one_run(corpus, dest, threads):
    shutil.rmtree(dest, ignore_errors=True)
    os.makedirs(dest, exist_ok=True)
    files = [f for f in sorted(os.listdir(corpus)) if not f.startswith(".mix-")]

    media_ms, small_ms, failures = [], [], []
    lock, idx = threading.Lock(), [0]

    def worker():
        while True:
            with lock:
                i = idx[0]
                idx[0] += 1
            if i >= len(files):
                return
            name = files[i]
            t0 = time.perf_counter()
            try:
                shutil.copyfile(os.path.join(corpus, name), os.path.join(dest, name))
            except OSError as e:
                with lock:
                    failures.append("%s: %s" % (name, e))
                continue
            dt = (time.perf_counter() - t0) * 1000.0
            with lock:
                (media_ms if name.startswith("media") else small_ms).append(dt)

    t0 = time.perf_counter()
    ws = [threading.Thread(target=worker, daemon=True) for _ in range(threads)]
    for w in ws:
        w.start()
    for w in ws:
        w.join()
    copy_wall = time.perf_counter() - t0
    return media_ms, small_ms, copy_wall, failures


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mount", default="/Volumes/zpool")
    ap.add_argument("--media", type=int, default=12)
    ap.add_argument("--media-mb", type=int, default=16)
    ap.add_argument("--small", type=int, default=300)
    ap.add_argument("--small-kb", type=int, default=4, help="sidecar-sized")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--runs", type=int, default=6)
    ap.add_argument("--corpus", default=os.path.expanduser("~/.jm-mixed-corpus"))
    ap.add_argument("--label", default="arm")
    ap.add_argument("--out", default="")
    a = ap.parse_args()

    if not spool() and spool() != {}:
        print("control plane unreachable")
        return 3

    if build_corpus(a.corpus, a.media, a.media_mb, a.small, a.small_kb):
        print("corpus built: %d x %dMiB media + %d x %dKiB small"
              % (a.media, a.media_mb, a.small, a.small_kb))
    else:
        print("corpus reused: %d x %dMiB media + %d x %dKiB small"
              % (a.media, a.media_mb, a.small, a.small_kb))

    dest = os.path.join(a.mount, ".jm-mixed-bench")
    media_p50, drain_s, depths = [], [], []

    for r in range(a.runs):
        drain_idle(120)  # start from a quiet spool or the timing is someone else's
        mm, sm, wall, failures = one_run(a.corpus, dest, a.threads)
        depth = (spool().get("pending_files") or 0)
        d = drain_idle()
        if failures:
            print("  run %d/%d  INVALID — %d copies failed (first: %s)"
                  % (r + 1, a.runs, len(failures), failures[0][:80]))
            continue
        if d is None:
            print("  run %d/%d  INVALID — spool never drained within the timeout"
                  % (r + 1, a.runs))
            continue
        if not mm:
            print("  run %d/%d  INVALID — no media files completed" % (r + 1, a.runs))
            continue
        media_p50.append(statistics.median(mm))
        drain_s.append(wall + d)
        depths.append(depth)
        print("  run %d/%d  media p50 %7.1f ms   small p50 %6.1f ms   copy %5.1fs "
              "+ drain %5.1fs   spool depth at copy-end %d"
              % (r + 1, a.runs, statistics.median(mm),
                 statistics.median(sm) if sm else 0, wall, d, depth))

    shutil.rmtree(dest, ignore_errors=True)

    if not media_p50:
        print("\nEVERY run invalid — nothing reportable.")
        return 3
    if max(depths) == 0:
        print("\n  INVALID — the spool never showed depth, so media and small rows "
              "never competed for a drain slot. A 'no difference' result here "
              "would be from no coverage, not from fairness.")
        return 3

    doc = harness.result("mixed_drain", media_p50, "media_file_p50_ms",
                         corpus="%dx%dMiB+%dx%dKiB" % (a.media, a.media_mb,
                                                       a.small, a.small_kb),
                         note="arm=%s threads=%d" % (a.label, a.threads),
                         extra={"total_seconds": harness.stats(drain_s),
                                "spool_depth_peak": max(depths)})
    st = doc["stats"]
    ts = doc["extra"]["total_seconds"]
    print("\n  MEDIA p50   n=%d  median %.1f ms  range %.1f-%.1f"
          % (st["n"], st["median"], st["min"], st["max"]))
    print("  TOTAL       n=%d  median %.1f s   range %.1f-%.1f"
          % (ts["n"], ts["median"], ts["min"], ts["max"]))
    if a.out:
        json.dump(doc, open(a.out, "w"), indent=2)
        print("  wrote %s" % a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
