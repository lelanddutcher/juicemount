#!/usr/bin/env python3
"""probe.py: the timed operations of the remote lab. Runs INSIDE the client
container (measure.py calls it through `docker compose exec`), so no Docker
overhead lands inside a timed region.

Each subcommand prints one JSON object: its timings plus the change in the
JuiceFS client's own counters (/jfs/.stats) across the operation.

  probe.py seed  --size small|full         write the fixture (idempotent)
  probe.py list                            list one big folder; walk the tree
  probe.py open  --count N                 first stat, then first open
  probe.py seq   --budget S                time to first byte, sequential read
  probe.py scrub --reads N --budget S      random 1-4 MiB reads
  probe.py write --files N --kib K         write N new files, close each

The fixture is deterministic: every name, size and byte comes from a seeded
generator, so two runs (and two machines) read the same tree.
"""
import argparse
import json
import os
import random
import shutil
import statistics
import sys
import time

ROOT = "/jfs"
FIX = ROOT + "/fixture"
MIB = 1 << 20

# Fixture shapes. "small" is the CI fixture; "full" is the local matrix.
SIZES = {
    "small": {"dirs": 6, "files_per_dir": 50, "big_dir": 400,
              "videos": [64 * MIB, 96 * MIB]},
    "full": {"dirs": 20, "files_per_dir": 150, "big_dir": 1000,
             "videos": [1024 * MIB, 512 * MIB, 256 * MIB]},
}
SEED = 20261109


def stats():
    """The JuiceFS client counters, as {name: float}."""
    out = {}
    try:
        with open(ROOT + "/.stats") as f:
            for line in f:
                parts = line.split()
                if len(parts) == 2:
                    try:
                        out[parts[0]] = float(parts[1])
                    except ValueError:
                        pass
    except OSError:
        pass
    return out


KEEP = ("juicefs_meta_ops_total_", "juicefs_fuse_ops_total_",
        "juicefs_object_request_durations_histogram_seconds_total",
        "juicefs_blockcache_hits", "juicefs_blockcache_miss",
        "juicefs_blockcache_hit_bytes", "juicefs_blockcache_miss_bytes",
        "juicefs_object_request_data_bytes", "juicefs_transaction_durations_histogram_seconds_total",
        "juicefs_meta_ops_durations_histogram_seconds_total")


def delta(before, after):
    d = {}
    for k, v in after.items():
        if not k.startswith(KEEP):
            continue
        x = v - before.get(k, 0.0)
        if x:
            d[k.replace("juicefs_", "")] = round(x, 6)
    return d


def summary(xs):
    if not xs:
        return None
    s = sorted(xs)
    return {"n": len(s), "median_ms": round(statistics.median(s) * 1000, 2),
            "p90_ms": round(s[min(len(s) - 1, int(len(s) * 0.9))] * 1000, 2),
            "p95_ms": round(s[min(len(s) - 1, int(len(s) * 0.95))] * 1000, 2),
            "min_ms": round(s[0] * 1000, 2), "max_ms": round(s[-1] * 1000, 2)}


def manifest():
    with open(FIX + "/MANIFEST.json") as f:
        return json.load(f)


def small_files(m):
    """Every small file of the tree, in a fixed order."""
    out = []
    for d in range(m["dirs"]):
        for i in range(m["files_per_dir"]):
            out.append(f"{FIX}/tree/d{d:03d}/clip_{i:04d}.xml")
    return out


# --- seed ---------------------------------------------------------------

def cmd_seed(a):
    want = dict(SIZES[a.size], size=a.size, seed=SEED)
    try:
        if manifest() == want:
            return {"op": "seed", "skipped": True, "fixture": want}
    except (OSError, ValueError):
        pass
    if os.path.exists(FIX):
        shutil.rmtree(FIX)
    rng = random.Random(SEED)
    t0 = time.monotonic()
    n = 0
    os.makedirs(FIX + "/tree")
    for d in range(want["dirs"]):
        os.makedirs(f"{FIX}/tree/d{d:03d}")
        for i in range(want["files_per_dir"]):
            with open(f"{FIX}/tree/d{d:03d}/clip_{i:04d}.xml", "wb") as f:
                f.write(rng.randbytes(rng.randint(1024, 64 * 1024)))
            n += 1
    os.makedirs(FIX + "/bigdir")
    for i in range(want["big_dir"]):
        with open(f"{FIX}/bigdir/frame_{i:05d}.dpx.meta", "wb") as f:
            f.write(rng.randbytes(rng.randint(256, 4096)))
        n += 1
    os.makedirs(FIX + "/media")
    for i, size in enumerate(want["videos"]):
        vr = random.Random(SEED + 1 + i)
        with open(f"{FIX}/media/video_{i}.mov", "wb") as f:
            left = size
            while left:
                chunk = min(left, 8 * MIB)
                f.write(vr.randbytes(chunk))
                left -= chunk
        n += 1
    with open(FIX + "/MANIFEST.json", "w") as f:
        json.dump(want, f)
    os.sync()
    return {"op": "seed", "skipped": False, "files": n, "seconds": round(time.monotonic() - t0, 2),
            "fixture": want}


# --- list ---------------------------------------------------------------

def cmd_list(a):
    m = manifest()
    s0 = stats()
    t0 = time.monotonic()
    # One big folder, as a file browser shows it: names plus attributes.
    entries = 0
    with os.scandir(FIX + "/bigdir") as it:
        for e in it:
            e.stat(follow_symlinks=False)
            entries += 1
    t1 = time.monotonic()
    s1 = stats()
    # The whole tree, folder by folder, with every attribute.
    walked = 0
    for dirpath, dirnames, filenames in os.walk(FIX + "/tree"):
        for name in filenames:
            os.lstat(os.path.join(dirpath, name))
            walked += 1
    t2 = time.monotonic()
    s2 = stats()
    if entries != m["big_dir"] or walked != m["dirs"] * m["files_per_dir"]:
        raise SystemExit(f"list: counted {entries}/{walked} entries, fixture says otherwise")
    return {"op": "list",
            "bigdir": {"entries": entries, "ms": round((t1 - t0) * 1000, 2), "client": delta(s0, s1)},
            "tree_walk": {"dirs": m["dirs"], "files": walked, "ms": round((t2 - t1) * 1000, 2),
                          "client": delta(s1, s2)}}


# --- open ---------------------------------------------------------------

def cmd_open(a):
    files = small_files(manifest())
    # A fixed sample spread over the tree; the same files cold and warm.
    rng = random.Random(SEED + 100)
    pick = rng.sample(files, min(a.count, len(files)))
    s0 = stats()
    st, op, rd = [], [], []
    for p in pick:
        t0 = time.monotonic()
        os.stat(p)
        t1 = time.monotonic()
        fd = os.open(p, os.O_RDONLY)
        t2 = time.monotonic()
        os.read(fd, 4096)
        t3 = time.monotonic()
        os.close(fd)
        st.append(t1 - t0)
        op.append(t2 - t1)
        rd.append(t3 - t2)
    return {"op": "open", "files": len(pick), "stat": summary(st), "open": summary(op),
            "first_read_4k": summary(rd), "client": delta(s0, stats())}


# --- seq ----------------------------------------------------------------

def cmd_seq(a):
    path = FIX + "/media/video_0.mov"
    size = os.path.getsize(path)
    s0 = stats()
    t0 = time.monotonic()
    fd = os.open(path, os.O_RDONLY)
    first = os.read(fd, 64 * 1024)
    ttfb = time.monotonic() - t0
    got = len(first)
    complete = False
    while True:
        b = os.read(fd, 4 * MIB)
        if not b:
            complete = True
            break
        got += len(b)
        if time.monotonic() - t0 > a.budget:
            break
    el = time.monotonic() - t0
    os.close(fd)
    return {"op": "seq", "file_bytes": size, "ttfb_ms": round(ttfb * 1000, 2),
            "bytes_read": got, "seconds": round(el, 3), "complete": complete,
            "mbit_s": round(got * 8 / el / 1e6, 2), "client": delta(s0, stats())}


# --- ttfb ---------------------------------------------------------------

def cmd_ttfb(a):
    """Open a large file and read 64 KiB at offset 0: the time to the first
    byte, alone (seq also measures it, then keeps reading)."""
    path = FIX + "/media/" + a.file
    s0 = stats()
    t0 = time.monotonic()
    fd = os.open(path, os.O_RDONLY)
    first = os.read(fd, 64 * 1024)
    ttfb = time.monotonic() - t0
    os.close(fd)
    return {"op": "ttfb", "file": a.file, "ttfb_ms": round(ttfb * 1000, 2), "bytes": len(first),
            "client": delta(s0, stats())}


# --- scrub --------------------------------------------------------------

def cmd_scrub(a):
    path = FIX + "/media/video_1.mov"
    size = os.path.getsize(path)
    rng = random.Random(SEED + 200)
    plan = []
    for _ in range(a.reads):
        n = rng.randint(a.min_kib * 1024, a.max_kib * 1024)
        plan.append((rng.randrange(0, size - n), n))
    s0 = stats()
    lat, done, nbytes = [], 0, 0
    t0 = time.monotonic()
    fd = os.open(path, os.O_RDONLY)
    for off, n in plan:
        r0 = time.monotonic()
        b = os.pread(fd, n, off)
        lat.append(time.monotonic() - r0)
        if len(b) != n:
            raise SystemExit(f"scrub: short read {len(b)} of {n} at {off}")
        done += 1
        nbytes += n
        if time.monotonic() - t0 > a.budget:
            break
    os.close(fd)
    return {"op": "scrub", "planned": len(plan), "done": done, "bytes": nbytes,
            "seconds": round(time.monotonic() - t0, 3), "read": summary(lat),
            "client": delta(s0, stats())}


# --- write --------------------------------------------------------------

def cmd_write(a):
    d = f"{ROOT}/writes/{a.tag}"
    if os.path.exists(d):
        shutil.rmtree(d)
    os.makedirs(d)
    rng = random.Random(SEED + 300)
    payload = rng.randbytes(a.kib * 1024)
    s0 = stats()
    per = []
    t0 = time.monotonic()
    for i in range(a.files):
        f0 = time.monotonic()
        # close() returns after juicefs uploaded the data and committed the
        # metadata (no --writeback): the file is on the server.
        with open(f"{d}/new_{i:04d}.bin", "wb") as f:
            f.write(payload)
        per.append(time.monotonic() - f0)
        if time.monotonic() - t0 > a.budget:
            break
    el = time.monotonic() - t0
    return {"op": "write", "files": len(per), "planned": a.files, "kib_each": a.kib,
            "seconds": round(el, 3), "per_file": summary(per), "client": delta(s0, stats())}


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("seed"); s.add_argument("--size", choices=sorted(SIZES), required=True)
    sub.add_parser("list")
    s = sub.add_parser("open"); s.add_argument("--count", type=int, default=30)
    s = sub.add_parser("seq"); s.add_argument("--budget", type=float, default=60)
    s = sub.add_parser("scrub"); s.add_argument("--reads", type=int, default=50); s.add_argument("--budget", type=float, default=90)
    s.add_argument("--min-kib", type=int, default=1024); s.add_argument("--max-kib", type=int, default=4096)
    s = sub.add_parser("ttfb"); s.add_argument("--file", default="video_0.mov")
    s = sub.add_parser("write"); s.add_argument("--files", type=int, default=50); s.add_argument("--kib", type=int, default=64)
    s.add_argument("--tag", required=True); s.add_argument("--budget", type=float, default=90)
    a = p.parse_args()
    out = globals()["cmd_" + a.cmd](a)
    json.dump(out, sys.stdout)
    print()


if __name__ == "__main__":
    main()
