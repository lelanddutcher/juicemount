#!/usr/bin/env python3
"""latency-proxy.py — TCP proxy that adds a fixed delay to every round trip,
so a high-RTT link (cellular) can be reproduced on a LAN.

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
                           --target 192.168.0.197:30179 --delay-ms 250

    then start JuiceMount with JM_DEBUG_META_ADDR=127.0.0.1:16379 so juicefs
    mounts against the proxy (health/fuse.go metaURLForMount).

The delay is applied per burst, not per byte: that models PROPAGATION delay (an
RTT), which is what cellular imposes on a request/response protocol like Redis.
A per-byte delay would model a bandwidth cap instead, and this workload is
RTT-bound, not throughput-bound.
"""

import argparse
import socket
import sys
import threading
import time

_stats_lock = threading.Lock()
_conns = 0
_active = 0


def _pump(src: socket.socket, dst: socket.socket, delay: float) -> None:
    """Copy src -> dst, sleeping `delay` before forwarding each burst.

    Half-closes propagate so the peer sees EOF instead of hanging until a
    timeout — a stuck half-open connection would look exactly like the mount
    stall we are trying to measure.
    """
    try:
        while True:
            buf = src.recv(65536)
            if not buf:
                break
            if delay > 0:
                time.sleep(delay)
            dst.sendall(buf)
    except OSError:
        pass
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)
        except OSError:
            pass


def _handle(client: socket.socket, target: tuple, delay: float, quiet: bool) -> None:
    global _active
    backend = None
    try:
        backend = socket.create_connection(target, timeout=10)
        backend.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        t1 = threading.Thread(target=_pump, args=(client, backend, delay), daemon=True)
        t2 = threading.Thread(target=_pump, args=(backend, client, delay), daemon=True)
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
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--listen", default="127.0.0.1:16379")
    ap.add_argument("--target", required=True, help="backend host:port")
    ap.add_argument("--delay-ms", type=float, default=250.0,
                    help="latency added per direction (round trip is 2x this)")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    lh, lp = args.listen.rsplit(":", 1)
    th, tp = args.target.rsplit(":", 1)
    target = (th, int(tp))
    delay = args.delay_ms / 1000.0

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((lh, int(lp)))
    srv.listen(128)

    print(f"latency-proxy: {args.listen} -> {args.target}, "
          f"+{args.delay_ms:.0f}ms per direction (round trip ~+{2*args.delay_ms:.0f}ms)",
          flush=True)

    global _conns, _active
    try:
        while True:
            client, _ = srv.accept()
            with _stats_lock:
                _conns += 1
                _active += 1
            threading.Thread(target=_handle,
                             args=(client, target, delay, args.quiet),
                             daemon=True).start()
    except KeyboardInterrupt:
        print(f"latency-proxy: stopping ({_conns} connections handled)", flush=True)
    finally:
        srv.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
