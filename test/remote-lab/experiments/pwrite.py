#!/usr/bin/env python3
"""pwrite.py WORKERS FILES_EACH SAMEDIR(0|1) TAG: parallel 64 KiB writers,
like the Mac drainer's small-file lane. Runs inside the client container."""
import json, os, sys, threading, time

w, n, same, tag = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3] == "1", sys.argv[4]
base = f"/jfs/pw/{tag}"
os.makedirs(base, exist_ok=True)
payload = os.urandom(64 * 1024)
lat = []


def work(k):
    d = base if same else f"{base}/w{k}"
    os.makedirs(d, exist_ok=True)
    for i in range(n):
        t = time.monotonic()
        with open(f"{d}/f{k}_{i}.bin", "wb") as f:
            f.write(payload)
        lat.append(time.monotonic() - t)


t0 = time.monotonic()
ts = [threading.Thread(target=work, args=(k,)) for k in range(w)]
[t.start() for t in ts]
[t.join() for t in ts]
el = time.monotonic() - t0
lat.sort()
print(json.dumps({"workers": w, "files": w * n, "same_dir": same, "seconds": round(el, 2),
                  "files_per_s": round(w * n / el, 2), "median_s": round(lat[len(lat) // 2], 2)}))
