#!/usr/bin/env python3
"""xprobe.py: extra probes for the remote-lab tuning experiments. Runs INSIDE
the client container, like lib/probe.py. Prints one JSON object.

  xprobe.py seed                 write /jfs/exp (two 256 MiB files, one 64 MiB)
  xprobe.py ttfb --file F [--offset O]   open + read 64 KiB at O
  xprobe.py seq --file F --budget S      sequential read rate (4 MiB reads)
  xprobe.py scrub --file F --reads N --budget S   random 1-4 MiB preads
  xprobe.py bigwrite --mib M --tag T     write one M MiB file, close (upload)
  xprobe.py smallwrite --files N --tag T write N x 64 KiB files
  xprobe.py walk                         walk /jfs/fixture/tree twice (2nd pass = in-mount caches)
"""
import argparse, json, os, random, shutil, statistics, sys, time

MIB = 1 << 20
EXP = "/jfs/exp"
SEED = 7331


def summary(xs):
    if not xs:
        return None
    s = sorted(xs)
    return {"n": len(s), "median_ms": round(statistics.median(s) * 1000, 1),
            "p90_ms": round(s[min(len(s) - 1, int(len(s) * 0.9))] * 1000, 1)}


def seed(a):
    os.makedirs(EXP, exist_ok=True)
    out = {}
    for name, size in (("seq.bin", 256 * MIB), ("scrub.bin", 256 * MIB), ("ttfb.bin", 64 * MIB),
                       ("ttfb2.bin", 64 * MIB)):
        p = f"{EXP}/{name}"
        if os.path.exists(p) and os.path.getsize(p) == size:
            continue
        r = random.Random(f"{SEED}{name}")
        with open(p, "wb") as f:
            left = size
            while left:
                n = min(left, 8 * MIB)
                f.write(r.randbytes(n))
                left -= n
        out[name] = size
    os.sync()
    return {"op": "seed", "wrote": out}


def ttfb(a):
    t0 = time.monotonic()
    fd = os.open(a.file, os.O_RDONLY)
    t1 = time.monotonic()
    b = os.pread(fd, 64 * 1024, a.offset)
    t2 = time.monotonic()
    os.close(fd)
    return {"op": "ttfb", "offset": a.offset, "open_ms": round((t1 - t0) * 1000, 1),
            "ttfb_ms": round((t2 - t0) * 1000, 1), "got": len(b)}


def seq(a):
    t0 = time.monotonic()
    fd = os.open(a.file, os.O_RDONLY)
    first = os.read(fd, 64 * 1024)
    tt = time.monotonic() - t0
    got, done = len(first), False
    while True:
        b = os.read(fd, 4 * MIB)
        if not b:
            done = True
            break
        got += len(b)
        if time.monotonic() - t0 > a.budget:
            break
    el = time.monotonic() - t0
    os.close(fd)
    # rate after the first byte, so TTFB does not dominate short runs
    return {"op": "seq", "ttfb_ms": round(tt * 1000, 1), "bytes": got, "seconds": round(el, 2),
            "complete": done, "mbit_s": round(got * 8 / el / 1e6, 2),
            "mbit_s_after_first": round((got - len(first)) * 8 / max(el - tt, 1e-3) / 1e6, 2)}


def scrub(a):
    size = os.path.getsize(a.file)
    rng = random.Random(SEED + 1)
    plan = [(rng.randrange(0, size - 4 * MIB), rng.randint(MIB, 4 * MIB)) for _ in range(a.reads)]
    lat = []
    t0 = time.monotonic()
    fd = os.open(a.file, os.O_RDONLY)
    for off, n in plan:
        r0 = time.monotonic()
        b = os.pread(fd, n, off)
        lat.append(time.monotonic() - r0)
        assert len(b) == n
        if time.monotonic() - t0 > a.budget:
            break
    os.close(fd)
    return {"op": "scrub", "done": len(lat), "planned": a.reads, "read": summary(lat),
            "seconds": round(time.monotonic() - t0, 2)}


def bigwrite(a):
    d = f"/jfs/xwrites/{a.tag}"
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    data = random.Random(SEED + 2).randbytes(4 * MIB)
    t0 = time.monotonic()
    with open(f"{d}/big.bin", "wb") as f:
        for _ in range(a.mib // 4):
            f.write(data)
    el = time.monotonic() - t0
    return {"op": "bigwrite", "mib": a.mib, "seconds": round(el, 2), "mbit_s": round(a.mib * MIB * 8 / el / 1e6, 2)}


def smallwrite(a):
    d = f"/jfs/xwrites/{a.tag}"
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    payload = random.Random(SEED + 3).randbytes(64 * 1024)
    per = []
    for i in range(a.files):
        f0 = time.monotonic()
        with open(f"{d}/f{i:03d}.bin", "wb") as f:
            f.write(payload)
        per.append(time.monotonic() - f0)
    return {"op": "smallwrite", "files": a.files, "per_file": summary(per), "total_s": round(sum(per), 2)}


def walk(a):
    res = []
    for _ in range(2):
        t0 = time.monotonic()
        n = 0
        for dp, dn, fn in os.walk("/jfs/fixture/tree"):
            for x in fn:
                os.lstat(os.path.join(dp, x))
                n += 1
        res.append(round((time.monotonic() - t0) * 1000, 1))
    return {"op": "walk", "files": n, "pass_ms": res}


p = argparse.ArgumentParser()
p.add_argument("cmd")
p.add_argument("--file")
p.add_argument("--offset", type=int, default=0)
p.add_argument("--budget", type=float, default=40)
p.add_argument("--reads", type=int, default=30)
p.add_argument("--mib", type=int, default=32)
p.add_argument("--files", type=int, default=10)
p.add_argument("--tag", default="x")
a = p.parse_args()
json.dump(globals()[a.cmd](a), sys.stdout)
print()
