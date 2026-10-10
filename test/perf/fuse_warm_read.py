#!/usr/bin/env python3
"""fuse_warm_read.py — how fast the FUSE CHANNEL itself carries cached bytes.

WHAT THE FUSE CHANNEL IS. Our volume is served in two layers. `juicefs mount`
is a user-space program that presents the storage as a normal folder at
~/.juicemount/fuse-internal. It cannot do that alone: macOS only lets the kernel
own a filesystem, so a kernel extension called macFUSE sits between them and
ferries every read and write between the kernel and juicefs. That ferry is the
FUSE channel. Every byte any application reads from the volume crosses it.

WHY THIS GATE EXISTS. macFUSE 5.3.0 introduced a "zero-copy" channel and claims
reads up to 15x faster. We upgraded 5.1.3 -> 5.3.3 on 2026-08-18. That claim is
about the CHANNEL and nothing else, so it cannot be tested through our NFS
server, through Finder, or over the network -- each of those adds a larger cost
on top that would hide it. This gate reads straight from the FUSE folder with
the data already cached locally, so the channel is the only thing being timed.

WHAT IT DELIBERATELY EXCLUDES:
  - Our NFS server. Reads here never touch it, so OUR code is not a variable.
    That matters because our binary changed since the 5.1.3 baselines were taken;
    a gate running through /Volumes/zpool would measure our changes AND macFUSE
    together and could not separate them.
  - The network. The file is read once to prime juicefs's local disk cache, and
    that priming read is discarded. A run that still pulls from the backend is
    measuring the NAS, not the channel, and is reported INVALID.
  - The kernel page cache. macOS would otherwise serve a re-read from RAM
    without consulting the filesystem at all -- the reads would never reach FUSE
    and the gate would report a large, meaningless number. Every measured read
    sets F_NOCACHE, so the page cache is bypassed and each byte genuinely
    crosses the channel.

VALIDITY GATES. This gate REFUSES to print a number unless it can prove it
measured something. Five conditions, each producing INVALID rather than a
result:
  1. The mount is a REAL macFUSE mount. If juicefs is not running, the folder is
     an ordinary empty local directory that reads fine and means nothing -- the
     failure that put 174 GB into a plain directory on 2026-07-25.
  2. Every read returned the whole file.
  3. juicefs's FUSE op counter actually advanced -- proof the reads reached the
     channel rather than being served above it.
  4. Backend GET bytes did NOT materially grow -- proof the reads were warm.
  5. n >= 6 measured samples after discarding the priming read.
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib"))
import harness  # noqa: E402

# Overridable so the validity gates below can be exercised against a path
# that is deliberately NOT a FUSE mount — a gate that has never been
# watched to refuse is not known to refuse.
MOUNT = os.environ.get("JM_FUSE_MOUNT",
                       os.path.expanduser("~/.juicemount/fuse-internal"))
JFS_METRICS = os.environ.get("JM_JFS_METRICS", "http://127.0.0.1:9568/metrics")

# macOS fcntl: tell the kernel not to keep this file's pages in the unified
# buffer cache, so each read goes to the filesystem instead of to RAM.
F_NOCACHE = 48

# Backend growth we tolerate across the measured reads. Not zero: other activity
# on the volume (the farm, ClipLogger, a background scan) shares these
# process-wide counters. A few MiB is unrelated noise; more than this and we
# cannot claim our own reads were served from cache.
WARM_TOLERANCE_BYTES = 8 * 1024 * 1024


def jfs_counters():
    """FUSE op count and backend GET bytes, from juicefs's own metrics."""
    try:
        raw = urllib.request.urlopen(JFS_METRICS, timeout=6).read().decode()
    except Exception:
        return None
    ops = get_bytes = None
    for line in raw.splitlines():
        if line.startswith("#"):
            continue
        if line.startswith("juicefs_fuse_ops_durations_histogram_seconds_count"):
            ops = float(line.rsplit(None, 1)[1])
        elif line.startswith("juicefs_object_request_data_bytes") and 'method="GET"' in line:
            get_bytes = float(line.rsplit(None, 1)[1])
    if ops is None:
        return None
    return {"fuse_ops": ops, "get_bytes": get_bytes or 0.0}


def is_real_fuse_mount(path):
    """G0: the path must be a macFUSE mount, not a same-named local directory."""
    try:
        out = subprocess.run(["/sbin/mount"], capture_output=True, text=True,
                             timeout=10).stdout
    except Exception:
        return False, "could not read the mount table"
    for line in out.splitlines():
        if os.path.realpath(path) in line or path in line:
            if "macfuse" in line:
                return True, line.strip()
            return False, "mounted, but NOT macfuse: %s" % line.strip()
    return False, ("nothing is mounted at %s — it is a plain local directory, so "
                   "any number from it would describe the local SSD" % path)


def macfuse_version():
    try:
        return subprocess.run(
            ["/usr/libexec/PlistBuddy", "-c", "print CFBundleShortVersionString",
             "/Library/Filesystems/macfuse.fs/Contents/Info.plist"],
            capture_output=True, text=True, timeout=10).stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def pick_target(min_bytes, max_bytes):
    """First file in the size BAND, stopping as soon as one is found.

    Not "the largest file": the volume holds camera masters in the hundreds of
    GB, and priming one of those would pull it over the network for minutes to
    time a read that takes a second. The band wants a file big enough that the
    per-read timing is not dominated by open/close, and small enough that the
    prime is cheap. Walking stops at the first match — on a 420k-file volume,
    walking to completion to find a marginally larger file costs far more than
    it is worth.
    """
    scanned = 0
    for root, dirs, files in os.walk(MOUNT):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for fn in files:
            if fn.startswith("."):
                continue
            p = os.path.join(root, fn)
            try:
                sz = os.path.getsize(p)
            except OSError:
                continue
            scanned += 1
            if min_bytes <= sz <= max_bytes:
                return (p, sz)
            if scanned > 30000:
                return None
    return None


def read_once(path, bufsize):
    """Full read with the page cache bypassed. Returns (bytes, seconds)."""
    import fcntl
    fd = os.open(path, os.O_RDONLY)
    try:
        fcntl.fcntl(fd, F_NOCACHE, 1)
        total = 0
        t0 = time.perf_counter()
        while True:
            b = os.read(fd, bufsize)
            if not b:
                break
            total += len(b)
        return total, time.perf_counter() - t0
    finally:
        os.close(fd)


def invalid(reason, extra=None):
    doc = {"name": "fuse_warm_read", "verdict": harness.INVALID, "reason": reason,
           "envelope": harness.envelope(note="macfuse=%s" % macfuse_version())}
    if extra:
        doc["extra"] = extra
    print("INVALID — %s" % reason)
    return doc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=8, help="measured reads (prime is extra)")
    ap.add_argument("--min-mb", type=int, default=32, help="minimum target file size")
    ap.add_argument("--max-mb", type=int, default=256,
                    help="maximum target — priming a 100GB master to time a "
                         "cached read would take minutes and measure the NAS")
    ap.add_argument("--buf-kb", type=int, default=1024)
    ap.add_argument("--file", help="explicit target, relative to the mount")
    ap.add_argument("--out")
    a = ap.parse_args()

    ok, detail = is_real_fuse_mount(MOUNT)
    print("macFUSE %s" % macfuse_version())
    print("mount: %s" % detail)
    if not ok:
        doc = invalid("not a real macFUSE mount — %s" % detail)
        if a.out:
            json.dump(doc, open(a.out, "w"), indent=2)
        return 2

    if a.file:
        target = os.path.join(MOUNT, a.file)
        size = os.path.getsize(target)
    else:
        found = pick_target(a.min_mb * 1024 * 1024, a.max_mb * 1024 * 1024)
        if not found:
            doc = invalid("no file between %d and %d MiB found under the mount — "
                          "nothing suitable to measure" % (a.min_mb, a.max_mb))
            if a.out:
                json.dump(doc, open(a.out, "w"), indent=2)
            return 2
        target, size = found
    print("target: %s (%.1f MiB)" % (os.path.relpath(target, MOUNT), size / 1048576))

    # Prime: pull the file into juicefs's local disk cache. Discarded.
    pb, pt = read_once(target, a.buf_kb * 1024)
    print("prime:  %.1f MiB in %.2fs (%.0f MB/s) — discarded"
          % (pb / 1048576, pt, (pb / 1048576) / pt if pt else 0))

    c0 = jfs_counters()
    if c0 is None:
        doc = invalid("juicefs metrics unreachable at %s — cannot prove the reads "
                      "reached the FUSE channel or were served warm" % JFS_METRICS)
        if a.out:
            json.dump(doc, open(a.out, "w"), indent=2)
        return 2

    samples, short = [], 0
    for i in range(a.n):
        nb, dt = read_once(target, a.buf_kb * 1024)
        if nb != size:
            short += 1
            continue
        mbps = (nb / 1048576) / dt if dt else 0
        samples.append(mbps)
        print("  read %d: %.0f MB/s (%.3fs)" % (i + 1, mbps, dt))

    c1 = jfs_counters()
    d_ops = (c1["fuse_ops"] - c0["fuse_ops"]) if c1 else 0
    d_get = (c1["get_bytes"] - c0["get_bytes"]) if c1 else 0
    print("fuse ops delta: %.0f    backend GET delta: %.1f MiB"
          % (d_ops, d_get / 1048576))

    extra = {"target": os.path.relpath(target, MOUNT), "size_bytes": size,
             "macfuse": macfuse_version(), "fuse_ops_delta": d_ops,
             "backend_get_bytes_delta": d_get, "short_reads": short,
             "buf_kb": a.buf_kb}

    if short:
        doc = invalid("%d of %d reads returned less than the whole file — the "
                      "throughput of a truncated read is not a throughput"
                      % (short, a.n), extra)
    elif d_ops <= 0:
        doc = invalid("juicefs's FUSE op counter did not move, so these reads "
                      "never crossed the channel this gate exists to measure — "
                      "F_NOCACHE is not holding, or the metrics are from a "
                      "different mount", extra)
    elif d_get > WARM_TOLERANCE_BYTES:
        doc = invalid("backend GET grew %.1f MiB during the measured reads — the "
                      "cache was not warm, so this timed the NAS and not the "
                      "channel" % (d_get / 1048576), extra)
    elif len(samples) < harness.MIN_N:
        doc = invalid("only %d valid samples; need >= %d"
                      % (len(samples), harness.MIN_N), extra)
    else:
        doc = harness.result("fuse_warm_read", samples, "MB_per_s",
                             corpus="%.0fMiB single file" % (size / 1048576),
                             note="macfuse=%s; page cache bypassed (F_NOCACHE); "
                                  "read direct from FUSE, NFS server not involved"
                                  % macfuse_version(),
                             extra=extra)
        s = doc["stats"]
        print("\nmedian %.0f MB/s   range %.0f-%.0f   n=%d"
              % (s["median"], s["min"], s["max"], s["n"]))

    if a.out:
        json.dump(doc, open(a.out, "w"), indent=2)
        print("wrote %s" % a.out)
    return 0 if doc.get("stats") else 2


if __name__ == "__main__":
    sys.exit(main())
