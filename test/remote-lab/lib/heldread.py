#!/usr/bin/env python3
"""Read one file through ONE descriptor kept open, as a player does.

    heldread.py FILE OUT.jsonl [MiB/s]

Reads 1 MiB at a time, paced (default 2 MiB/s). After an error it retries
the same offset every 0.5 s on the same descriptor and never reopens: on
macOS every open of a file shares one FUSE handle (macFUSE), so a read error
juicefs keeps on that handle is what the app sees until it restarts. One JSON
line per read: t (ms), off, ok, ms, err. Stops on SIGTERM.
"""
import json
import os
import signal
import sys
import time

path, out = sys.argv[1], sys.argv[2]
rate = float(sys.argv[3]) if len(sys.argv) > 3 else 2.0
stop = False


def on_term(*_):
    global stop
    stop = True


signal.signal(signal.SIGTERM, on_term)
fd = os.open(path, os.O_RDONLY)
size = os.fstat(fd).st_size
off, step = 0, 1 << 20
with open(out, "a", buffering=1) as log:
    nxt = time.monotonic()
    while not stop and off < size:
        t0 = time.time()
        try:
            n = len(os.pread(fd, step, off))
            ok, err = n > 0, ""
        except OSError as e:
            ok, err = False, os.strerror(e.errno)
        log.write(json.dumps({"t": int(t0 * 1000), "off": off, "ok": ok,
                              "ms": int((time.time() - t0) * 1000), "err": err}) + "\n")
        if ok:
            off += step
            nxt += 1.0 / rate
        else:
            nxt = time.monotonic() + 0.5
        time.sleep(max(0.0, nxt - time.monotonic()))
os.close(fd)
