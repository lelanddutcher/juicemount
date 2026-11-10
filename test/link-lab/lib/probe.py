#!/usr/bin/env python3
"""probe.py: the timed operations of the Link lab. Runs INSIDE a Mac
stand-in (home-mac or away-mac); measure.py calls it with `docker exec`, so
no Docker overhead lands inside a timed region. Each subcommand prints one
JSON object.

  probe.py connect --server URL --hostname H [--direct-wait S]
      start a new node (mac.sh start), join with the pairing code in
      $LINK_CODE, then time: control registration, the first Redis PING
      through Link, and (within S seconds) the first direct path
  probe.py path                 the NAS peer's path now (status + tailscale ping)
  probe.py bench --get-object K --put-mib N --pings N
      Redis PING round trips, a pipelined PING burst, an object GET and PUT,
      all through Link to the NAS node's service ports
  probe.py wait-ping --timeout S
      time until a Redis PING through Link answers again (recovery)

Credentials come from the environment (LINK_REDIS_USER/PASS, LINK_S3_USER/
PASS), never from argv. Traffic to the NAS goes through tailscaled's SOCKS5
port, so it rides the tunnel exactly like the app's loopback proxies.
"""
import argparse
import json
import os
import socket
import statistics
import struct
import subprocess
import sys
import time

SOCKS = ("127.0.0.1", 1055)
SOCK = "/run/tailscale/tailscaled.sock"
REDIS_PORT = 30179
S3_PORT = 30151
BUCKET = "linklab"


def ts(*args, timeout=30):
    return subprocess.run(["tailscale", "--socket=" + SOCK, *args], capture_output=True,
                          text=True, timeout=timeout)


def status():
    r = ts("status", "--json", timeout=10)
    if r.returncode != 0:
        return None
    try:
        return json.loads(r.stdout)
    except ValueError:
        return None


def nas_peer(st):
    """The NAS node: the peer tagged tag:jm-nas (or named juicemount-nas)."""
    for p in (st or {}).get("Peer", {}).values():
        if "tag:jm-nas" in (p.get("Tags") or []) or p.get("HostName") == "juicemount-nas":
            return p
    return None


def nas_ip(st):
    p = nas_peer(st)
    if not p:
        return None
    for ip in p.get("TailscaleIPs") or []:
        if "." in ip:
            return ip
    return None


def path_of(p):
    """direct / derp / peer-relay / none, from a status peer entry."""
    if not p:
        return {"path": "none"}
    out = {"cur_addr": p.get("CurAddr") or "", "relay": p.get("Relay") or "",
           "peer_relay": p.get("PeerRelay") or ""}
    if out["cur_addr"]:
        out["path"] = "direct"
    elif out["peer_relay"]:
        out["path"] = "peer-relay"
    elif p.get("InMagicSock") or out["relay"]:
        out["path"] = "derp"
    else:
        out["path"] = "none"
    return out


def socks_connect(host, port, timeout):
    s = socket.create_connection(SOCKS, timeout=timeout)
    s.settimeout(timeout)
    s.sendall(b"\x05\x01\x00")
    if s.recv(2) != b"\x05\x00":
        raise OSError("socks: no auth method")
    addr = socket.inet_aton(host)
    s.sendall(b"\x05\x01\x00\x01" + addr + struct.pack(">H", port))
    rep = recvn(s, 10)
    if rep[1] != 0:
        raise OSError(f"socks: connect failed ({rep[1]})")
    return s


def recvn(s, n):
    b = b""
    while len(b) < n:
        c = s.recv(n - len(b))
        if not c:
            raise OSError("connection closed")
        b += c
    return b


def resp(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a.encode() if isinstance(a, str) else a
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


def read_line(s, buf):
    while b"\r\n" not in buf[0]:
        c = s.recv(65536)
        if not c:
            raise OSError("connection closed")
        buf[0] += c
    line, buf[0] = buf[0].split(b"\r\n", 1)
    return line


def redis_session(ip, timeout=5):
    s = socks_connect(ip, REDIS_PORT, timeout)
    buf = [b""]
    s.sendall(resp("AUTH", os.environ["LINK_REDIS_USER"], os.environ["LINK_REDIS_PASS"]))
    line = read_line(s, buf)
    if not line.startswith(b"+OK"):
        raise OSError("redis AUTH: " + line.decode(errors="replace")[:80])
    return s, buf


def redis_ping_once(ip, timeout=3):
    s, buf = redis_session(ip, timeout)
    try:
        s.sendall(resp("PING"))
        if read_line(s, buf) != b"+PONG":
            raise OSError("no PONG")
    finally:
        s.close()


def tailscale_ping(ip, count=5):
    r = ts("ping", "--c", str(count), "--timeout", "3s", "--until-direct=false", ip, timeout=60)
    vias, lat = [], []
    for line in r.stdout.splitlines():
        if line.startswith("pong from"):
            v = line.split(" via ", 1)[1].split(" in ")[0] if " via " in line else ""
            vias.append(v)
            try:
                lat.append(float(line.rsplit(" in ", 1)[1].rstrip("ms")))
            except (IndexError, ValueError):
                pass
    return {"via": vias[-1] if vias else "", "pongs": len(vias),
            "median_ms": round(statistics.median(lat), 2) if lat else None}


# --- subcommands --------------------------------------------------------

def cmd_connect(a):
    subprocess.run(["/lab/lib/mac.sh", "start"], check=True)
    t0 = time.monotonic()
    out = {"op": "connect", "hostname": a.hostname}
    r = ts("up", "--login-server", a.server, "--auth-key", os.environ["LINK_CODE"],
           "--hostname", a.hostname, "--accept-dns=false", "--accept-routes=false",
           "--timeout", f"{a.up_timeout}s", timeout=a.up_timeout + 10)
    out["control_ms"] = round((time.monotonic() - t0) * 1000)
    if r.returncode != 0:
        out["error"] = "tailscale up failed: " + (r.stderr or r.stdout).strip()[-300:]
        return out
    # First Redis PING through Link (the NAS peer may appear a moment later).
    deadline = t0 + a.ping_timeout
    ip, last = None, ""
    while time.monotonic() < deadline:
        if not ip:
            ip = nas_ip(status())
            if ip:
                out["peer_seen_ms"] = round((time.monotonic() - t0) * 1000)
        if ip:
            out["ping_attempts"] = out.get("ping_attempts", 0) + 1
            try:
                redis_ping_once(ip)
                out["first_ping_ms"] = round((time.monotonic() - t0) * 1000)
                break
            except OSError as e:
                last = str(e) or type(e).__name__
                errs = out.setdefault("ping_errors", [])
                if len(errs) < 5:
                    errs.append(f"{round((time.monotonic() - t0) * 1000)}ms: {last}")
        time.sleep(0.2)
    out["nas_ip"] = ip
    if "first_ping_ms" not in out:
        out["error"] = f"no Redis PING through Link within {a.ping_timeout}s ({last or 'no NAS peer'})"
        return out
    # Direct path: poll while a little traffic flows (disco needs some).
    end = time.monotonic() + a.direct_wait
    while time.monotonic() < end:
        p = path_of(nas_peer(status()))
        if p["path"] == "direct":
            out["direct_ms"] = round((time.monotonic() - t0) * 1000)
            break
        try:
            redis_ping_once(ip)
        except OSError:
            pass
        time.sleep(0.5)
    out.update(path_of(nas_peer(status())))
    return out


def cmd_path(a):
    st = status()
    ip = nas_ip(st)
    out = {"op": "path", **path_of(nas_peer(st))}
    if ip:
        out["tailscale_ping"] = tailscale_ping(ip, a.count)
        out.update(path_of(nas_peer(status())))
    return out


def curl_s3(method, ip, key, upload=None):
    u, p = os.environ["LINK_S3_USER"], os.environ["LINK_S3_PASS"]
    cmd = ["curl", "-sS", "--fail", "-o", "/dev/null", "--max-time", "300",
           "--socks5-hostname", "%s:%d" % SOCKS, "--aws-sigv4", "aws:amz:us-east-1:s3",
           "--user", u + ":" + p,
           "-w", "%{http_code} %{size_download} %{size_upload} %{time_total} %{time_starttransfer}"]
    if upload:
        cmd += ["-T", upload]
    r = subprocess.run(cmd + [f"http://{ip}:{S3_PORT}/{BUCKET}/{key}"], capture_output=True, text=True)
    if r.returncode != 0:
        return {"error": (r.stderr or r.stdout).strip()[-200:]}
    code, down, up, total, ttfb = r.stdout.split()
    n = float(up if upload else down)
    return {"http": int(code), "bytes": int(n), "seconds": round(float(total), 3),
            "ttfb_ms": round(float(ttfb) * 1000, 1),
            "mbit_s": round(n * 8 / 1e6 / float(total), 1) if float(total) > 0 else None}


def cmd_bench(a):
    ip = nas_ip(status())
    if not ip:
        return {"op": "bench", "error": "no NAS peer"}
    out = {"op": "bench", "nas_ip": ip}
    # Redis: sequential PINGs on one connection, then a pipelined burst.
    s, buf = redis_session(ip, 10)
    try:
        rtts = []
        for _ in range(a.pings):
            t = time.monotonic()
            s.sendall(resp("PING"))
            if read_line(s, buf) != b"+PONG":
                raise OSError("no PONG")
            rtts.append(time.monotonic() - t)
        n = 2000
        t = time.monotonic()
        s.sendall(resp("PING") * n)
        for _ in range(n):
            read_line(s, buf)
        burst = time.monotonic() - t
    finally:
        s.close()
    out["redis_ping_ms"] = {"median": round(statistics.median(rtts) * 1000, 2),
                            "p90": round(sorted(rtts)[int(len(rtts) * 0.9) - 1] * 1000, 2)}
    out["redis_pipelined_ops_s"] = round(n / burst)
    # Object store: GET the seeded object, PUT a new one.
    out["s3_get"] = curl_s3("GET", ip, a.get_object)
    if a.put_mib:
        path = "/var/lib/probe-put.bin"
        with open(path, "wb") as f:
            f.write(os.urandom(a.put_mib << 20))
        out["s3_put"] = curl_s3("PUT", ip, f"put-{int(time.time())}.bin", upload=path)
        os.remove(path)
    return out


def cmd_wait_ping(a):
    t0 = time.monotonic()
    last = ""
    while time.monotonic() - t0 < a.timeout:
        ip = nas_ip(status())
        if ip:
            try:
                redis_ping_once(ip, timeout=2)
                return {"op": "wait-ping", "ms": round((time.monotonic() - t0) * 1000)}
            except OSError as e:
                last = str(e)
        time.sleep(0.25)
    return {"op": "wait-ping", "error": f"no Redis PING through Link within {a.timeout}s ({last})"}


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("connect")
    c.add_argument("--server", required=True)
    c.add_argument("--hostname", required=True)
    c.add_argument("--up-timeout", type=int, default=60)
    c.add_argument("--ping-timeout", type=int, default=90)
    c.add_argument("--direct-wait", type=int, default=20)
    p = sub.add_parser("path")
    p.add_argument("--count", type=int, default=5)
    b = sub.add_parser("bench")
    b.add_argument("--get-object", default="get-64m.bin")
    b.add_argument("--put-mib", type=int, default=16)
    b.add_argument("--pings", type=int, default=30)
    w = sub.add_parser("wait-ping")
    w.add_argument("--timeout", type=int, default=120)
    a = ap.parse_args()
    fn = {"connect": cmd_connect, "path": cmd_path, "bench": cmd_bench, "wait-ping": cmd_wait_ping}[a.cmd]
    try:
        out = fn(a)
    except (OSError, subprocess.SubprocessError) as e:
        out = {"op": a.cmd, "error": str(e)}
    print(json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
