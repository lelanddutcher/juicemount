#!/usr/bin/env python3
"""Pin probe for the remote lab (runs inside the client container).

It speaks the bundled juicefs's JMPinCache control message (patch 0025) the
way the app's pin workers do, and measures what an editor would feel:

  pin FILE...          fill and pin with 4 workers; progress every 5 s
  check FILE...        pinned bytes missing from disk (local)
  sweep FILE...        keep only these files pinned
  readthrough FILE...  the old pin: 4 workers read 128 KiB direct reads
  presence FILE...     bytes of each file in the block cache (juicefs warmup --check)
  seq FILE             sequential read in 1 MiB reads: Mbit/s
  scrub FILE N         N random 1 MiB reads: latency p50/p95/max
  metrics              object-store GETs and bytes so far

Every command prints one JSON line last.
"""
import json
import os
import random
import re
import struct
import subprocess
import sys
import threading
import time
import urllib.request

MOUNT = "/jfs"
MSG = 1104
START, STATUS, CHECK, SWEEP = 1, 2, 3, 4
LOCK = threading.Lock()


def control(op, body, reply_len):
    msg = struct.pack(">IIB", MSG, 1 + len(body), op) + body
    with LOCK:
        fd = os.open(os.path.join(MOUNT, ".control"), os.O_RDWR)
        try:
            os.write(fd, msg)
            out = b""
            deadline = time.time() + 30
            while len(out) < reply_len:
                chunk = os.read(fd, reply_len - len(out))
                out += chunk
                if out[:1] not in (b"", b"\0"):
                    return out[:1]
                if not chunk:
                    if time.time() > deadline:
                        raise TimeoutError("no reply")
                    time.sleep(0.002)
            return out
        finally:
            os.close(fd)


def ino(path):
    return os.stat(path).st_ino


def status(i):
    r = control(STATUS, struct.pack(">Q", i), 27)
    if r[0]:
        return {"errno": r[0]}
    state, eno, total, done, fetched = struct.unpack(">BBQQQ", r[1:27])
    return {"state": state, "fill_errno": eno, "total": total, "done": done, "fetched": fetched}


def cmd_pin(files):
    jobs = list(files)
    results, done_bytes = {}, {}
    total_all = sum(os.stat(f).st_size for f in files)
    t0 = time.time()

    def worker():
        while True:
            with LOCK:
                if not jobs:
                    return
                f = jobs.pop(0)
            i = ino(f)
            s = time.time()
            r = control(START, struct.pack(">Q", i), 1)
            if r[0]:
                results[f] = {"start_errno": r[0]}
                continue
            while True:
                st = status(i)
                done_bytes[f] = st.get("done", 0)
                if st.get("state") != 1:
                    st["secs"] = round(time.time() - s, 2)
                    results[f] = st
                    break
                time.sleep(0.2)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    last = 0
    while any(t.is_alive() for t in threads):
        time.sleep(0.5)
        if time.time() - last >= 5:
            last = time.time()
            d = sum(done_bytes.values())
            el = time.time() - t0
            rate = d / el if el > 0 else 0
            eta = (total_all - d) / rate if rate > 0 else -1
            print(json.dumps({"progress_s": round(el, 1), "done": d, "total": total_all,
                              "rate_MBps": round(rate / 1e6, 1), "eta_s": round(eta, 1)}), flush=True)
    for t in threads:
        t.join()
    ok = sum(1 for r in results.values() if r.get("state") == 2)
    print(json.dumps({"cmd": "pin", "secs": round(time.time() - t0, 2), "files": len(files), "ready": ok,
                      "failed": len(files) - ok, "fetched": sum(r.get("fetched", 0) for r in results.values()),
                      "total": total_all,
                      "errors": {f: r for f, r in results.items() if r.get("state") != 2}}))


def cmd_check(files):
    out, missing, unknown = {}, 0, 0
    for f in files:
        r = control(CHECK, struct.pack(">Q", ino(f)), 18)
        if r[0]:
            out[f] = {"errno": r[0]}
            continue
        known, total, miss = struct.unpack(">BQQ", r[1:18])
        out[f] = {"known": known, "total": total, "missing": miss}
        missing += miss
        unknown += 0 if known else 1
    print(json.dumps({"cmd": "check", "files": len(files), "unknown": unknown, "missing": missing,
                      "detail": {k: v for k, v in out.items() if v.get("missing") or not v.get("known")}}))


def cmd_sweep(files):
    inos = [ino(f) for f in files]
    body = struct.pack(">BI", 3, len(inos)) + b"".join(struct.pack(">Q", i) for i in inos)
    r = control(SWEEP, body, 21)
    if r[0]:
        print(json.dumps({"cmd": "sweep", "errno": r[0]}))
        return
    dropped, pinned, limit = struct.unpack(">IQQ", r[1:21])
    print(json.dumps({"cmd": "sweep", "dropped": dropped, "pinned": pinned, "limit": limit}))


def cmd_readthrough(files):
    jobs = list(files)
    t0 = time.time()

    def worker():
        buf = bytearray(128 << 10)
        while True:
            with LOCK:
                if not jobs:
                    return
                f = jobs.pop(0)
            fd = os.open(f, os.O_RDONLY | os.O_DIRECT)
            try:
                with os.fdopen(fd, "rb", buffering=0) as fh:
                    while fh.readinto(buf):
                        pass
            except OSError:
                pass

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(json.dumps({"cmd": "readthrough", "files": len(files), "secs": round(time.time() - t0, 2)}))


def cmd_presence(files):
    out, cached, total = {}, 0, 0
    for f in files:
        p = subprocess.run(["juicefs", "warmup", "--check", f], capture_output=True, text=True)
        text = p.stdout + p.stderr
        m = re.search(r"check cache: .*\(([\d.]+)%\) cached", text)
        size = os.stat(f).st_size
        c = int(float(m.group(1)) / 100 * size) if m else -1
        out[os.path.basename(f)] = c
        cached += max(c, 0)
        total += size
    print(json.dumps({"cmd": "presence", "cached": cached, "total": total, "files": out}))


def cmd_seq(f):
    t0 = time.time()
    n = 0
    with open(f, "rb", buffering=0) as fh:
        while True:
            b = fh.read(1 << 20)
            if not b:
                break
            n += len(b)
    el = time.time() - t0
    print(json.dumps({"cmd": "seq", "file": f, "bytes": n, "secs": round(el, 2), "mbit_s": round(n * 8 / el / 1e6, 1)}))


def cmd_scrub(f, count):
    size = os.stat(f).st_size
    rnd = random.Random(f + str(count))
    lat = []
    with open(f, "rb", buffering=0) as fh:
        for _ in range(count):
            off = rnd.randrange(0, max(1, size - (1 << 20)))
            s = time.time()
            fh.seek(off)
            fh.read(1 << 20)
            lat.append((time.time() - s) * 1000)
    lat.sort()
    p = lambda q: round(lat[min(len(lat) - 1, int(q * len(lat)))], 1)
    print(json.dumps({"cmd": "scrub", "file": f, "n": count, "p50_ms": p(0.5), "p95_ms": p(0.95), "max_ms": round(lat[-1], 1)}))


def cmd_metrics():
    body = urllib.request.urlopen("http://127.0.0.1:9567/metrics", timeout=5).read().decode()
    gets = gbytes = 0.0
    for line in body.splitlines():
        if line.startswith("juicefs_object_request_durations_histogram_seconds_count") and 'method="GET"' in line:
            gets += float(line.split()[-1])
        if line.startswith("juicefs_object_request_data_bytes") and 'method="GET"' in line:
            gbytes += float(line.split()[-1])
        if line.startswith("juicefs_blockcache_bytes"):
            cache = float(line.split()[-1])
    print(json.dumps({"cmd": "metrics", "gets": int(gets), "get_bytes": int(gbytes), "blockcache_bytes": int(cache)}))


def main():
    c, args = sys.argv[1], sys.argv[2:]
    if c == "pin":
        cmd_pin(args)
    elif c == "check":
        cmd_check(args)
    elif c == "sweep":
        cmd_sweep(args)
    elif c == "readthrough":
        cmd_readthrough(args)
    elif c == "presence":
        cmd_presence(args)
    elif c == "seq":
        cmd_seq(args[0])
    elif c == "scrub":
        cmd_scrub(args[0], int(args[1]))
    elif c == "metrics":
        cmd_metrics()
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
