#!/usr/bin/env python3
"""smallfile_write_bench.py — small-file concurrent write throughput.

THE REPORT THIS ANSWERS: "concurrent uploads and copies which contain many
small files has gotten slower recently (write spool concurrency probably)".

WHY THE EXISTING SUITES DO NOT COVER IT:
  * test/qa-battery/05-concurrent-copies.sh tests INTEGRITY under concurrency
    and records the worst directory-listing time — not write throughput.
  * test/qa-battery/02-file-sizes.sh walks a size spectrum (0, 1B, KB, 5MB,
    100MB, GB) ONE FILE AT A TIME.
Neither measures many-small-files-at-concurrency, which is the reported case.

THE SUSPICION IS SPECIFIC AND CHECKABLE: nfs/drainer.go documents "Worker
concurrency is bounded by a semaphore (default 4)". Four concurrent drains
against 10GbE with thousands of small files. There is also a standing earmark
to parallelise the spool->backend drain.

THE DIAGNOSTIC: if `in_progress` pins at the semaphore width while
`pending_files` climbs, drain concurrency is the ceiling — that is the
signature, and it is sampled here throughout the run rather than inferred.

MEASURES WHAT THE USER FEELS:
  * files/sec sustained — NOT aggregate MB/s, which one large file dominates
  * p50/p99 per-file completion
  * spool depth over time (pending / in_progress)
  * app CPU%, because the CPU penalty was called out explicitly

Corpus is generated ONCE off-mount and reused. A corpus that changes per run
makes the number incomparable, which is half of why the existing baselines are
untrustworthy.
"""

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import threading
import time
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib"))
import harness  # noqa: E402

CONTROL = os.environ.get("JM_CONTROL", "http://127.0.0.1:11050")


def _spool():
    try:
        with urllib.request.urlopen(CONTROL + "/spool", timeout=5) as r:
            return json.load(r)
    except Exception:
        return {}


def _app_pid():
    out = subprocess.run(["pgrep", "-f", "JuiceMount.app/Contents/MacOS/JuiceMount"],
                         capture_output=True, text=True).stdout.split()
    return int(out[0]) if out else 0


def _cpu(pid):
    if not pid:
        return None
    out = subprocess.run(["ps", "-p", str(pid), "-o", "%cpu="],
                         capture_output=True, text=True).stdout.strip()
    try:
        return float(out)
    except Exception:
        return None


def build_corpus(path, count, size_kb):
    """Generate once, reuse forever. Returns True if it created it."""
    marker = os.path.join(path, ".corpus-%d-%dk" % (count, size_kb))
    if os.path.exists(marker):
        return False
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)
    blob = os.urandom(size_kb * 1024)
    per_dir = max(1, count // 50)
    for i in range(count):
        d = os.path.join(path, "d%02d" % (i // per_dir))
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "f%05d.bin" % i), "wb") as f:
            f.write(blob)
    open(marker, "w").close()
    return True


class SpoolSampler(threading.Thread):
    """Samples spool depth + CPU during the run. This is where the semaphore
    signature shows up, so it must be observed live rather than reconstructed.

    ALSO times a foreground directory listing on every sample, because raising
    drain concurrency trades against exactly that. The drain shares FUSE with
    the user's Finder window, and the reason the worker count is 4 is an
    incident: a 16-way Finder copy on 2026-06-14 saturated JuiceFS's buffer, the
    mount went readdir-unresponsive, and the watchdog SIGKILLed it. So "faster
    files/sec" is only half a result — if nav latency degrades WHILE the drain
    runs, the change is a regression no matter what the throughput says. Measured
    during the drain, which is the only time it can be observed."""

    def __init__(self, pid, nav_dir=None, interval=0.5):
        super().__init__(daemon=True)
        self.pid, self.interval, self.nav_dir = pid, interval, nav_dir
        self.stop_flag = threading.Event()
        self.samples = []
        self.nav_ms = []

    def run(self):
        while not self.stop_flag.is_set():
            s = _spool()
            if s:
                self.samples.append({
                    "t": time.time(),
                    "pending": s.get("pending_files") or 0,
                    "in_progress": s.get("in_progress") or 0,
                    "cpu": _cpu(self.pid),
                })
            if self.nav_dir:
                t0 = time.perf_counter()
                try:
                    os.listdir(self.nav_dir)
                    self.nav_ms.append((time.perf_counter() - t0) * 1000.0)
                except OSError:
                    pass
            self.stop_flag.wait(self.interval)

    def summary(self):
        if not self.samples:
            return {"valid": False, "reason": "no spool samples"}
        prog = [s["in_progress"] for s in self.samples]
        pend = [s["pending"] for s in self.samples]
        cpus = [s["cpu"] for s in self.samples if s["cpu"] is not None]
        peak = max(prog)
        # The starvation signature: in_progress sitting at its peak while
        # pending is still climbing. Only meaningful when the spool actually
        # showed depth — with peak==0 "pinned at peak" would count every idle
        # sample and manufacture a signature out of nothing.
        pinned = 0 if peak == 0 else sum(
            1 for a, b in zip(self.samples, self.samples[1:])
            if a["in_progress"] == peak and b["pending"] > a["pending"])
        return {
            "valid": True,
            "samples": len(self.samples),
            "in_progress_peak": peak,
            "in_progress_median": statistics.median(prog),
            "pending_peak": max(pend),
            "pinned_while_pending_grew": pinned,
            "cpu_peak": max(cpus) if cpus else None,
            "cpu_median": statistics.median(cpus) if cpus else None,
            # Foreground nav DURING the drain — the must-not-regress side.
            "nav_ms_median": statistics.median(self.nav_ms) if self.nav_ms else None,
            "nav_ms_p95": (sorted(self.nav_ms)[int(len(self.nav_ms) * 0.95)]
                           if len(self.nav_ms) > 3 else None),
            "nav_samples": len(self.nav_ms),
        }


def one_run(corpus, dest, threads):
    """Copy the corpus onto the mount with N threads; return per-file times."""
    shutil.rmtree(dest, ignore_errors=True)
    os.makedirs(dest, exist_ok=True)
    files = []
    for root, _, names in os.walk(corpus):
        for n in names:
            if n.startswith(".corpus-"):
                continue
            src = os.path.join(root, n)
            rel = os.path.relpath(src, corpus)
            files.append((src, os.path.join(dest, rel)))
    for _, d in files:
        os.makedirs(os.path.dirname(d), exist_ok=True)

    times, lock = [], threading.Lock()
    idx = [0]
    failures = []

    def worker():
        while True:
            with lock:
                i = idx[0]
                idx[0] += 1
            if i >= len(files):
                return
            s, d = files[i]
            t0 = time.perf_counter()
            # A worker that dies takes its share of the work with it while the
            # file COUNT stays 2000, so the rate is computed from work that
            # never happened. Observed live: one run reported 281 files/s in
            # 7.1s against a 46 files/s baseline because a thread threw
            # FileNotFoundError and the remaining files were silently skipped.
            # Failures are collected and invalidate the run.
            try:
                shutil.copyfile(s, d)
            except OSError as e:
                with lock:
                    failures.append("%s: %s" % (d, e))
                continue
            dt = time.perf_counter() - t0
            with lock:
                times.append(dt)

    t0 = time.perf_counter()
    ws = [threading.Thread(target=worker, daemon=True) for _ in range(threads)]
    for w in ws:
        w.start()
    for w in ws:
        w.join()
    wall = time.perf_counter() - t0
    # len(times), not len(files): the denominator must be work that COMPLETED.
    return times, wall, len(times), failures


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mount", default="/Volumes/zpool")
    ap.add_argument("--count", type=int, default=2000)
    ap.add_argument("--size-kb", type=int, default=32)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--runs", type=int, default=6, help="n>=6; see harness.MIN_N")
    ap.add_argument("--corpus", default=os.path.expanduser("~/.jm-smallfile-corpus"))
    ap.add_argument("--out", default="")
    ap.add_argument("--keep", action="store_true", help="do not delete the written files")
    ap.add_argument("--nav-dir", default="/Volumes/zpool/oldzpool/ARCHIVE",
                    help="directory listed repeatedly during the drain to measure "
                         "foreground nav latency under load")
    a = ap.parse_args()

    if build_corpus(a.corpus, a.count, a.size_kb):
        print("corpus generated: %d x %dKB at %s" % (a.count, a.size_kb, a.corpus))
    else:
        print("corpus reused: %d x %dKB at %s" % (a.count, a.size_kb, a.corpus))

    pid = _app_pid()
    dest_root = os.path.join(a.mount, ".jm-smallfile-bench")
    rates, spools = [], []

    for r in range(a.runs):
        samp = SpoolSampler(pid, nav_dir=a.nav_dir)
        samp.start()
        times, wall, n, failures = one_run(a.corpus, dest_root, a.threads)
        samp.stop_flag.set()
        samp.join(timeout=5)
        rate = n / wall if wall else 0
        if failures:
            print("  run %d/%d  INVALID — %d of %d copies failed (first: %s). A rate "
                  "from a partial copy is a number from work that did not happen."
                  % (r + 1, a.runs, len(failures), len(failures) + n, failures[0]))
            spools.append(samp.summary())
            # Let the spool drain before the next run: tearing down the dest
            # tree while the drainer still holds the previous run's rows is the
            # most likely source of these failures.
            time.sleep(20)
            continue
        rates.append(rate)
        spools.append(samp.summary())
        ts = sorted(times)
        p50 = statistics.median(ts) if ts else 0
        p99 = ts[int(len(ts) * 0.99)] if len(ts) > 2 else 0
        sp = spools[-1]
        nm = sp.get("nav_ms_median")
        print("  run %d/%d  %6.1f files/s  wall %5.1fs  p50 %5.1fms p99 %6.1fms  "
              "in_progress peak=%s pinned=%s  cpu_peak=%s  nav_p50=%s"
              % (r + 1, a.runs, rate, wall, p50 * 1000, p99 * 1000,
                 sp.get("in_progress_peak"), sp.get("pinned_while_pending_grew"),
                 sp.get("cpu_peak"),
                 ("%.1fms" % nm) if nm is not None else "n/a"))

    if not a.keep:
        shutil.rmtree(dest_root, ignore_errors=True)

    doc = harness.result(
        "smallfile_write", rates, "files_per_sec",
        corpus="%dx%dKB" % (a.count, a.size_kb),
        note="threads=%d" % a.threads,
        extra={"spool": spools},
    )
    st = doc["stats"]
    print("\n  n=%d  median %.1f files/s  range %.1f-%.1f"
          % (st["n"], st["median"], st["min"], st["max"]))

    peaks = [s.get("in_progress_peak") for s in spools if s.get("valid")]
    pinned = sum(s.get("pinned_while_pending_grew") or 0 for s in spools if s.get("valid"))
    if peaks:
        print("  spool in_progress peak across runs: %s" % sorted(set(peaks)))
        print("  samples where in_progress was pinned while pending grew: %d" % pinned)
        if max(peaks) == 0:
            print("  NOTE: spool never showed depth — writes may not be routing "
                  "through the spool at all. That is a finding, not a pass.")

    navs = [s["nav_ms_median"] for s in spools
            if s.get("valid") and s.get("nav_ms_median") is not None]
    if navs:
        ns = harness.stats(navs)
        print("  nav-during-drain p50: median %.1f ms  range %.1f-%.1f  (n=%d)"
              % (ns["median"], ns["min"], ns["max"], ns["n"]))
        doc["extra"]["nav_during_drain_ms"] = ns
    else:
        print("  nav-during-drain: NO SAMPLES — the must-not-regress side was not "
              "measured, so a throughput win here is unproven, not proven.")

    if a.out:
        json.dump(doc, open(a.out, "w"), indent=2)
        print("  wrote %s" % a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
