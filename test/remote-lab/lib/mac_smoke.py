#!/usr/bin/env python3
"""mac_smoke.py — link smoke test, run INSIDE the macOS VM by mac-lab.sh.

    python3 - PROFILE PROXY_IP SILO_IP IPERF_IP DOWN_MBIT UP_MBIT MB MAX_S   (stdin)
    python3 mac_smoke.py --table RESULT.json...    (on the host: a table)

Standard library and system tools only (ping, curl), so the VM needs
nothing installed. Prints one JSON object.

- rtt: 20 pings to the object store.
- redis: TCP connect to the RESP proxy and one PING; the first reply line
  (an auth error is expected: no password is sent). Median of 5.
- s3: GET /minio/health/live; connect time and time to first byte. Median of 5.
- download: GET of the MB-MB object (bucket jmlab-smoke), time and Mbit/s.
- upload: PUT of a 20 MB random body into the same bucket, time and Mbit/s.
- link: if iperf3 is installed (mac-lab.sh up installs it), the link rate in
  each direction: UDP at the profile's cap (what arrives, and the loss), or
  TCP for 10 s on a profile without a cap. This checks the path; the curl
  transfers above show what one TCP stream from the object store gets.
"""
import json
import os
import re
import socket
import statistics
import subprocess
import sys
import tempfile
import time


def ping(host):
    out = subprocess.run(["ping", "-c", "20", "-i", "0.2", host], capture_output=True, text=True).stdout
    m = re.search(r"= ([\d.]+)/([\d.]+)/([\d.]+)/([\d.]+) ms", out)
    loss = re.search(r"([\d.]+)% packet loss", out)
    if not m:
        return {"error": out.strip()[-300:]}
    return {"min_ms": float(m[1]), "avg_ms": float(m[2]), "max_ms": float(m[3]),
            "loss_pct": float(loss[1]) if loss else None}


def redis(host):
    conn, reply, first = [], [], ""
    for _ in range(5):
        t0 = time.monotonic()
        s = socket.create_connection((host, 6379), timeout=30)
        t1 = time.monotonic()
        s.sendall(b"PING\r\n")
        data = s.recv(256)
        t2 = time.monotonic()
        s.close()
        conn.append((t1 - t0) * 1000)
        reply.append((t2 - t1) * 1000)
        first = data.split(b"\r\n")[0].decode(errors="replace")[:60]
    return {"connect_ms": round(statistics.median(conn), 1), "reply_ms": round(statistics.median(reply), 1),
            "reply": first}


def curl(args):
    fmt = "%{http_code} %{time_connect} %{time_starttransfer} %{time_total} %{size_download} %{size_upload}"
    r = subprocess.run(["curl", "-sS", "-o", "/dev/null", "-w", fmt] + args, capture_output=True, text=True)
    f = r.stdout.split()
    if len(f) != 6:
        return {"error": (r.stderr or r.stdout).strip()[-300:]}
    return {"http": int(f[0]), "connect_ms": float(f[1]) * 1000, "ttfb_ms": float(f[2]) * 1000,
            "total_s": float(f[3]), "down_bytes": int(float(f[4])), "up_bytes": int(float(f[5])),
            "exit": r.returncode}


def s3(silo):
    runs = [curl(["--max-time", "30", f"http://{silo}:9000/minio/health/live"]) for _ in range(5)]
    ok = [r for r in runs if "http" in r]
    if not ok:
        return runs[-1]
    return {"http": ok[-1]["http"], "connect_ms": round(statistics.median(r["connect_ms"] for r in ok), 1),
            "ttfb_ms": round(statistics.median(r["ttfb_ms"] for r in ok), 1)}


def iperf3_bin():
    for p in ("/opt/homebrew/bin/iperf3", "/usr/local/bin/iperf3"):
        if os.access(p, os.X_OK):
            return p
    return None


def link(host, down, up):
    exe = iperf3_bin()
    if not exe:
        return {"skipped": "iperf3 not installed"}
    res = {}
    for direction, cap in (("down", down), ("up", up)):
        args = [exe, "-c", host, "-J", "-t", "10"] + (["-R"] if direction == "down" else [])
        if cap > 0:
            args += ["-u", "-b", f"{cap}M"]
        r = subprocess.run(args, capture_output=True, text=True)
        try:
            end = json.loads(r.stdout)["end"]
            if cap > 0:
                # sum_received (iperf3 3.17+) is what arrived; plain sum is what was sent.
                rec = end.get("sum_received") or end["sum"]
                res[direction] = {"udp_mbit_s": round(rec["bits_per_second"] / 1e6, 1),
                                  "loss_pct": round(rec["lost_percent"], 2), "offered_mbit_s": cap}
            else:
                res[direction] = {"tcp_mbit_s": round(end["sum_received"]["bits_per_second"] / 1e6, 1)}
        except (ValueError, KeyError):
            res[direction] = {"error": (r.stdout + r.stderr).strip()[-200:]}
    return res


def rate(r, key):
    if "total_s" in r and r["total_s"] > 0:
        r["mbit_s"] = round(r[key] * 8 / r["total_s"] / 1e6, 2)
        r["total_s"] = round(r["total_s"], 2)
    return r


def main():
    profile, proxy, silo, iperf, down, up, mb, max_s = sys.argv[1:9]
    res = {"profile": profile, "rtt": ping(silo), "redis": redis(proxy), "s3": s3(silo),
           "link": link(iperf, int(down), int(up))}
    res["download"] = rate(curl(["--max-time", max_s, f"http://{silo}:9000/jmlab-smoke/blob-{mb}M"]), "down_bytes")
    with tempfile.NamedTemporaryFile() as f:
        f.write(os.urandom(20 * 1024 * 1024))
        f.flush()
        res["upload"] = rate(curl(["--max-time", max_s, "-T", f.name,
                                   f"http://{silo}:9000/jmlab-smoke/upload-{profile}"]), "up_bytes")
    print(json.dumps(res))


def table(paths):
    rows = ["| Profile | RTT avg (ping loss) | Redis connect / PING reply | S3 connect / TTFB "
            "| Link down / up (iperf3) | Download (1 stream) | Upload 20 MB (1 stream) |",
            "|---|---|---|---|---|---|---|"]
    order = {"lan": 0}
    for p in sorted(paths, key=lambda p: (order.get(json.load(open(p))["profile"], 1), p)):
        r = json.load(open(p))
        rt, rd, s, d, u = r["rtt"], r["redis"], r["s3"], r["download"], r["upload"]

        def xfer(x):
            if "mbit_s" not in x:
                return x.get("error", "?")
            part = "" if x.get("exit", 0) == 0 else " (time limit)"
            return f"{x['down_bytes'] or x['up_bytes']:,} B in {x['total_s']} s = {x['mbit_s']} Mbit/s{part}"
        def lk(x):
            if "udp_mbit_s" in x:
                return f"{x['udp_mbit_s']} Mbit/s UDP ({x['loss_pct']}% lost)"
            return f"{x['tcp_mbit_s']} Mbit/s TCP" if "tcp_mbit_s" in x else x.get("error", "?")
        ln = r.get("link", {})
        link_cell = ln["skipped"] if "skipped" in ln else f"{lk(ln.get('down', {}))} / {lk(ln.get('up', {}))}"
        rows.append(f"| {r['profile']} | {rt.get('avg_ms')} ms ({rt.get('loss_pct')}%) | "
                    f"{rd['connect_ms']} / {rd['reply_ms']} ms (`{rd['reply']}`) | "
                    f"{s.get('connect_ms')} / {s.get('ttfb_ms')} ms (HTTP {s.get('http')}) | {link_cell} | "
                    f"{xfer(d)} | {xfer(u)} |")
    print("\n".join(rows))


if __name__ == "__main__":
    if sys.argv[1:2] == ["--table"]:
        table(sys.argv[2:])
    else:
        main()
