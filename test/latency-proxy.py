#!/usr/bin/env python3
"""latency-proxy.py — bounded TCP impairment proxy for cellular simulation.

It adds propagation latency and optional jitter, applies aggregate directional
bandwidth ceilings, and can abort at a hard forwarded-byte budget. The default
remains latency-only for backward compatibility.

WHY THIS EXISTS
    The reported navigation bug only appears on cellular: browsing a directory
    is instant OFFLINE and awful ONLINE, for the same directories. Measured on
    WiFi (4.8ms RTT) online nav is ALREADY at parity with offline (3ms/dir vs
    6ms/dir), so the bug is not reproducible where the fix is being written —
    and a fix "validated" on a LAN proves nothing about cellular.

    It fronts REDIS specifically. Profiling the real cellular link showed
    navigation is metadata-bound, not data-bound: 67 minutes of cumulative
    metadata wait against only 77 object GETs, Lookup averaging 266ms and
    GetAttr 221ms. Slowing the metadata round trip reproduces the actual
    bottleneck; slowing object storage would reproduce a different one.

WHY PYTHON AND NOT GO
    A Go build of this same tool could not connect at all:
        dial 192.168.0.197:30179: connect: no route to host
    while a Python socket reached that identical host:port in 40ms seconds
    earlier. macOS Local Network Privacy is granted PER BINARY, and a
    freshly-built binary is unapproved — the same wall the app hits after every
    rebuild. Python inherits the already-approved interpreter, so the tool works
    immediately with no grant, no signing, and no build step. See
    project_local_network_permission_offline in memory.

USAGE
    ./test/latency-proxy.py --listen 127.0.0.1:16379 \
                           --target 192.168.0.197:30179 --delay-ms 90 \
                           --jitter-ms 20 --down-mbps 8 --up-mbps 2 \
                           --max-forward-bytes 16777216 --stats-file /tmp/jm-link.json

    then start JuiceMount with JM_DEBUG_META_ADDR=127.0.0.1:16379 so juicefs
    mounts against the proxy (health/fuse.go metaURLForMount).

The delay is applied per burst, not per byte: that models PROPAGATION delay (an
RTT), which is what cellular imposes on a request/response protocol like Redis.
Bandwidth is paced separately across all connections in each direction. This
is a TCP-stream simulator, so it deliberately does not discard stream bytes to
fake packet loss; use the scoped dummynet harness for TCP retransmission tests.
"""

import argparse
import json
import os
import pathlib
import random
import signal
import socket
import sys
import threading
import time

_stats_lock = threading.Lock()
_conns = 0
_active = 0
_bytes_up = 0
_bytes_down = 0
_bursts_up = 0
_bursts_down = 0
_budget_exceeded = False
_started = time.monotonic()
_stop = threading.Event()
_profile = {}


class Pacer:
    """Aggregate directional byte-rate limiter shared by every connection."""

    def __init__(self, megabits_per_sec: float) -> None:
        self.bytes_per_sec = max(0.0, megabits_per_sec) * 1_000_000 / 8
        self._next = 0.0
        self._lock = threading.Lock()

    def wait(self, size: int) -> None:
        if self.bytes_per_sec <= 0 or size <= 0:
            return
        with self._lock:
            now = time.monotonic()
            finish = max(now, self._next) + size / self.bytes_per_sec
            self._next = finish
            delay = finish - now
        if delay > 0:
            _stop.wait(delay)


def _record(direction: str, size: int, max_bytes: int) -> bool:
    """Account one forwarded burst; false trips the hard global byte budget."""
    global _bytes_up, _bytes_down, _bursts_up, _bursts_down, _budget_exceeded
    with _stats_lock:
        total = _bytes_up + _bytes_down
        if max_bytes > 0 and total + size > max_bytes:
            _budget_exceeded = True
            _stop.set()
            return False
        if direction == "up":
            _bytes_up += size
            _bursts_up += 1
        else:
            _bytes_down += size
            _bursts_down += 1
    return True


def _snapshot() -> dict:
    with _stats_lock:
        return {
            "schema": 1,
            "simulated": True,
            "connections_total": _conns,
            "connections_active": _active,
            "bytes_up": _bytes_up,
            "bytes_down": _bytes_down,
            "bytes_total": _bytes_up + _bytes_down,
            "bursts_up": _bursts_up,
            "bursts_down": _bursts_down,
            "budget_exceeded": _budget_exceeded,
            "elapsed_sec": round(time.monotonic() - _started, 3),
            # Keep artifacts self-describing without recording endpoint names,
            # addresses, credentials, or any source-media information.
            "profile": dict(_profile),
        }


def _write_stats(path: str) -> None:
    if not path:
        return
    dest = pathlib.Path(path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    tmp.write_text(json.dumps(_snapshot(), sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, dest)


def _stats_loop(path: str) -> None:
    while not _stop.wait(0.25):
        _write_stats(path)
    _write_stats(path)


def _pump(src: socket.socket, dst: socket.socket, delay: float, jitter: float,
          pacer: Pacer, direction: str, max_bytes: int) -> None:
    """Copy src -> dst, sleeping `delay` before forwarding each burst.

    Half-closes propagate so the peer sees EOF instead of hanging until a
    timeout — a stuck half-open connection would look exactly like the mount
    stall we are trying to measure.
    """
    try:
        while not _stop.is_set():
            buf = src.recv(65536)
            if not buf:
                break
            shaped_delay = max(0.0, delay + random.uniform(-jitter, jitter))
            if shaped_delay > 0 and _stop.wait(shaped_delay):
                break
            pacer.wait(len(buf))
            if _stop.is_set() or not _record(direction, len(buf), max_bytes):
                break
            dst.sendall(buf)
    except OSError:
        pass
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)
        except OSError:
            pass


def _handle(client: socket.socket, target: tuple, delay: float, jitter: float,
            upstream: Pacer, downstream: Pacer, max_bytes: int,
            quiet: bool) -> None:
    global _active
    backend = None
    try:
        backend = socket.create_connection(target, timeout=10)
        backend.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        t1 = threading.Thread(target=_pump,
                              args=(client, backend, delay, jitter, upstream,
                                    "up", max_bytes), daemon=True)
        t2 = threading.Thread(target=_pump,
                              args=(backend, client, delay, jitter, downstream,
                                    "down", max_bytes), daemon=True)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
    except OSError as e:
        if not quiet:
            print(f"dial {target[0]}:{target[1]}: {e}", file=sys.stderr, flush=True)
    finally:
        for s in (client, backend):
            if s is not None:
                try:
                    s.close()
                except OSError:
                    pass
        with _stats_lock:
            _active -= 1


def main() -> int:
    global _profile
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--listen", default="127.0.0.1:16379")
    ap.add_argument("--target", required=True, help="backend host:port")
    ap.add_argument("--delay-ms", type=float, default=250.0,
                    help="latency added per direction (round trip is 2x this)")
    ap.add_argument("--jitter-ms", type=float, default=0.0,
                    help="uniform ± jitter added independently to each burst")
    ap.add_argument("--down-mbps", type=float, default=0.0,
                    help="aggregate backend-to-client ceiling; zero is unlimited")
    ap.add_argument("--up-mbps", type=float, default=0.0,
                    help="aggregate client-to-backend ceiling; zero is unlimited")
    ap.add_argument("--max-forward-bytes", type=int, default=0,
                    help="hard aggregate safety budget; zero disables the budget")
    ap.add_argument("--stats-file", default="",
                    help="atomically updated JSON counters for acceptance harnesses")
    ap.add_argument("--seed", type=int, default=1,
                    help="deterministic jitter seed")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    if min(args.delay_ms, args.jitter_ms, args.down_mbps, args.up_mbps,
           args.max_forward_bytes) < 0:
        ap.error("impairment values and byte budget must be nonnegative")
    random.seed(args.seed)

    lh, lp = args.listen.rsplit(":", 1)
    th, tp = args.target.rsplit(":", 1)
    target = (th, int(tp))
    delay = args.delay_ms / 1000.0
    jitter = args.jitter_ms / 1000.0
    upstream = Pacer(args.up_mbps)
    downstream = Pacer(args.down_mbps)
    _profile = {
        "delay_ms_each_direction": args.delay_ms,
        "jitter_ms": args.jitter_ms,
        "down_mbps": args.down_mbps,
        "up_mbps": args.up_mbps,
        "max_forward_bytes": args.max_forward_bytes,
        "seed": args.seed,
    }

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((lh, int(lp)))
    srv.listen(128)
    srv.settimeout(0.5)

    print(f"latency-proxy: {args.listen} -> {args.target}, "
          f"+{args.delay_ms:.0f}ms±{args.jitter_ms:.0f}ms per direction "
          f"(round trip ~+{2*args.delay_ms:.0f}ms), "
          f"down={args.down_mbps or 'unlimited'}Mbps "
          f"up={args.up_mbps or 'unlimited'}Mbps "
          f"budget={args.max_forward_bytes or 'unlimited'}B",
          flush=True)

    def stop(_signum=None, _frame=None):
        _stop.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    stats_thread = threading.Thread(target=_stats_loop,
                                    args=(args.stats_file,), daemon=True)
    stats_thread.start()

    global _conns, _active
    try:
        while not _stop.is_set():
            try:
                client, _ = srv.accept()
            except socket.timeout:
                continue
            with _stats_lock:
                _conns += 1
                _active += 1
            threading.Thread(target=_handle,
                             args=(client, target, delay, jitter, upstream,
                                   downstream, args.max_forward_bytes, args.quiet),
                             daemon=True).start()
    except KeyboardInterrupt:
        print(f"latency-proxy: stopping ({_conns} connections handled)", flush=True)
    finally:
        _stop.set()
        srv.close()
        stats_thread.join(timeout=1)
        _write_stats(args.stats_file)
    snap = _snapshot()
    print("latency-proxy: stopping " + json.dumps(snap, sort_keys=True), flush=True)
    return 3 if snap["budget_exceeded"] else 0


if __name__ == "__main__":
    sys.exit(main())
