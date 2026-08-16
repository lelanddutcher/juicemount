#!/usr/bin/env python3
"""wirebytes.py — per-process bytes that actually crossed the network interface.

WHY THIS EXISTS. Nothing in this tree measured wire bytes. Our own /metrics
`bytes_read` counts bytes SERVED TO THE NFS CLIENT, which cannot distinguish a
byte off the local SSD from a byte dragged over a cellular uplink, and the
juicefs daemon's `object_get_bytes` covers only object-store traffic — not the
Redis metadata chatter that dominates a high-RTT link. So "is this efficient on
cellular?" and "does navigating cached content touch the network?" were both
unfalsifiable. This is the missing instrument.

HOW, and the two traps that cost real time to find:

 1. `nettop -x` reports CUMULATIVE per-process counters, NOT per-interval
    deltas. Measured directly: 585,814,655 -> 585,844,048 -> 585,868,065 across
    three one-second samples of an idle-ish app. Summing rows — the obvious
    reading of the man page — over-counts by orders of magnitude. The window
    cost is last MINUS first.

 2. `-l 0` (stream forever) produces NOTHING when stdout is not a TTY. A
    finite `-l N` is the only non-interactive mode that emits. A background
    `nettop -l 0 > file` yields an empty file and, with a naive accumulator,
    a confident report of zero bytes.

WHAT THIS ACTUALLY MEASURES — corrected by cross-validation, not assumed.

The first reading of the data was wrong and worth recording. The app PID showed
a large cumulative byte count, so it looked like all backend traffic was charged
to it. Running the tool against the juicefs daemon's own counters on the SAME
150 MiB uncached read settled it:

    wirebytes (app PID):   in 0.4 MiB    out 219.2 MiB
    backend (juicefs):     object_get    236.6 MiB     (cache miss +76)

236.6 MiB genuinely came off the object store, and this tool saw essentially
none of it inbound. What it saw was 219.2 MiB OUTBOUND: the NFS loopback traffic
the app served to the reader. The `juicefs mount` PIDs report 0/0 throughout, so
the backend fetch is attributed to neither.

So:
  * BACKEND wire bytes  -> `backend.object_get_bytes` / `object_put_bytes` on
    /metrics. Authoritative, already exposed, and the right input for any
    cellular-efficiency gate.
  * NFS LOOPBACK volume -> this tool. Useful for read amplification (how much
    the client pulled versus what the app served) but it is NOT a measure of
    what crossed a cellular link.
  * REDIS/metadata bytes -> STILL UNMEASURED. Nothing counts them. On a
    high-RTT link this is the traffic that dominates, and closing the gap needs
    a go-redis AddHook on the three clients in metadata/redis.go.

Do not build a "bytes over the cellular link" number on this tool alone. That
was the mistake this comment exists to prevent.

REFUSES TO REPORT. A zero-byte reading is the single most flattering result this
tool can produce — it reads as "perfectly efficient" — and it is exactly what a
broken sampler emits. So a run is INVALID unless it actually observed the
process: at least `--min-samples` rows attributable to a live PID. Callers must
check `valid` before believing `bytes_in`/`bytes_out`.
"""

import argparse
import json
import re
import shutil
import signal
import subprocess
import sys
import time

# A nettop process row looks like:
#   13:54:21.135763 juicefs.25438   <cols...>  bytes_in  bytes_out  ...
# The process cell is "<name>.<pid>"; the two counters we want are the first two
# numeric columns after it. Interface/socket sub-rows carry a leading blank
# process cell and are skipped — summing them as well would double-count.
_PROC = re.compile(r"^\s*[\d:.]+\s+(\S+)\.(\d+)\s+(.*)$")


def _numbers(rest):
    return [int(x) for x in re.findall(r"(?<![\w.])(\d+)(?![\w.])", rest)]


class Sampler:
    """Window cost per PID = last cumulative reading minus the first."""

    def __init__(self, pids, interval=1):
        self.pids = {int(p) for p in pids}
        self.interval = interval
        self.first = {}   # pid -> (in, out) at window open
        self.last = {}    # pid -> (in, out) at window close
        self.rows = 0     # attributable rows seen — the validity denominator

    def _sample(self, into):
        """One finite nettop snapshot; record cumulative counters per PID.

        Finite `-l 2` rather than `-l 1`: the first emission of a fresh nettop
        can carry zeros before it has attributed sockets, so we take the LAST
        row per PID from a two-sample run.
        """
        if not shutil.which("nettop"):
            raise RuntimeError("nettop not found; wire bytes are unmeasurable on this host")
        cmd = ["nettop", "-P", "-x", "-l", "2", "-s", str(self.interval),
               "-j", "bytes_in,bytes_out"]
        for p in sorted(self.pids):
            cmd += ["-p", str(p)]
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=60).stdout
        for line in out.splitlines():
            m = _PROC.match(line)
            if not m:
                continue
            pid = int(m.group(2))
            if pid not in self.pids:
                continue
            nums = _numbers(m.group(3))
            if len(nums) < 2:
                continue
            into[pid] = (nums[0], nums[1])
            self.rows += 1

    def open_window(self):
        self._sample(self.first)
        return self

    def close_window(self):
        self._sample(self.last)

    def result(self, min_samples):
        per_pid = {}
        tin = tout = 0
        negative = []
        for pid in sorted(self.pids):
            f = self.first.get(pid)
            l = self.last.get(pid)
            if f is None or l is None:
                continue
            din, dout = l[0] - f[0], l[1] - f[1]
            # A NEGATIVE delta means the counter reset under us (the process
            # restarted mid-window). Reporting it would subtract traffic that
            # happened; refusing is the only honest option.
            if din < 0 or dout < 0:
                negative.append(pid)
                continue
            per_pid[str(pid)] = {"bytes_in": din, "bytes_out": dout}
            tin += din
            tout += dout

        valid = self.rows >= min_samples and bool(per_pid) and not negative
        if negative:
            reason = ("counter went BACKWARDS for pid(s) %s — the process restarted "
                      "mid-window, so the delta is not a measurement" % negative)
        elif not per_pid:
            reason = ("no watched PID was observed at BOTH ends of the window; a "
                      "zero-byte reading from a sampler that saw nothing reads as "
                      "'perfectly efficient', which is the most flattering way to be wrong")
        elif self.rows < min_samples:
            reason = "observed %d attributable rows, need >= %d" % (self.rows, min_samples)
        else:
            reason = ""

        return {
            "valid": valid,
            "reason": reason,
            "samples": self.rows,
            "per_pid": per_pid,
            "bytes_in": tin,
            "bytes_out": tout,
            "bytes_total": tin + tout,
        }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pid", action="append", default=[], type=int, required=True,
                    help="process to watch; repeatable")
    ap.add_argument("--seconds", type=float, default=10.0,
                    help="measurement window (ignored with --wait-for-file)")
    ap.add_argument("--interval", type=int, default=1, help="nettop sample interval")
    ap.add_argument("--min-samples", type=int, default=2,
                    help="validity floor: rows attributable to a watched PID")
    ap.add_argument("--wait-for-file", default="",
                    help="run until this path disappears, instead of a fixed window")
    args = ap.parse_args()

    s = Sampler(args.pid, args.interval).open_window()
    try:
        if args.wait_for_file:
            import os
            while os.path.exists(args.wait_for_file):
                time.sleep(0.25)
        else:
            time.sleep(args.seconds)
    finally:
        s.close_window()

    out = s.result(args.min_samples)
    json.dump(out, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0 if out["valid"] else 3


if __name__ == "__main__":
    sys.exit(main())
