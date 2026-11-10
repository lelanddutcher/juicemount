#!/usr/bin/env python3
"""measure.py: run the remote lab matrix and write JSON plus a scoreboard.

  ./measure.py                                   full matrix, full fixture
  ./measure.py --profiles lan,abroad --fixture small
  ./measure.py --profiles hotel --keep           leave the lab running after

For each profile it (1) shapes the wan, (2) checks the link with ping and
iperf3 and stops if the link is not what the profile says, then (3) for a
cold and a warm client cache mounts the volume and runs every probe
(lib/probe.py) inside the client container.

  cold  the client is remounted with an EMPTY block cache.
  warm  the client is remounted and KEEPS the block cache the cold pass
        filled. Metadata caches start empty again (a remount clears them),
        as after the app restarts with its cache on disk.

Around each probe it reads the RESP proxy's counters: `commands` (Redis
commands the client sent) and `flushes` (reply batches the proxy wrote back,
about one per client round trip; pipelined commands share one).

Output: results/<run id>/<profile>.json, results/<run id>/run.json and
results/<run id>/scoreboard.md. Exit 0 when the lab worked, whatever the
numbers; non-zero when the lab broke (a profile failed its link check, a
probe crashed, a mount failed).
"""
import argparse
import datetime
import json
import os
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
LAB = os.path.join(HERE, "lab.sh")
IPERF = os.environ.get("JMLAB_IPERF_IP", "10.231.10.40")
PROJECT = os.environ.get("JMLAB_PROJECT", "jmlab")


class LabError(Exception):
    pass


def run(cmd, timeout=900, check=True):
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and p.returncode != 0:
        raise LabError(f"{' '.join(cmd[:4])}... exited {p.returncode}:\n{p.stderr[-2000:]}")
    return p


def lab(*args, timeout=900):
    return run([LAB, *args], timeout=timeout).stdout


def in_client(*args, timeout=900):
    return lab("exec", *args, timeout=timeout)


def profiles():
    out = {}
    with open(os.path.join(HERE, "profiles.tsv")) as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            name, rtt, jit, loss, down, up = line.split()
            out[name] = {"rtt_ms": float(rtt), "jitter_ms": float(jit), "loss_pct": float(loss),
                         "down_mbit": float(down), "up_mbit": float(up)}
    return out


def counter():
    return json.loads(lab("proxy-counter"))


def probe(*args, timeout=900):
    before = counter()
    out = json.loads(in_client("python3", "/lab/lib/probe.py", *args, timeout=timeout))
    after = counter()
    out["proxy"] = {k: after[k] - before[k] for k in ("commands", "flushes")}
    return out


# --- link check ---------------------------------------------------------

def iperf(*args, timeout=60):
    """iperf3 client against the NAS-side server; retries while the server
    is still finishing the previous test."""
    failures, last = 0, ""
    for _ in range(20):
        p = run(["docker", "compose", "-p", PROJECT, "exec", "-T", "client",
                 "iperf3", "-c", IPERF, "-J", *args], timeout=timeout, check=False)
        try:
            d = json.loads(p.stdout)
        except ValueError:
            d = {}
        if "busy" in d.get("error", "") or "busy" in p.stderr:
            time.sleep(2)
            continue
        if "error" in d or p.returncode != 0:
            # iperf3's control connection can time out on a lossy link
            # ("Resource temporarily unavailable"): retry it a few times.
            failures += 1
            last = d.get("error") or p.stderr[-500:]
            if failures >= 3:
                raise LabError(f"iperf3 {' '.join(args)} failed 3 times: {last}")
            time.sleep(3)
            continue
        return d
    raise LabError(f"iperf3 server stayed busy {last}")


def check_link(name, prof):
    """Ping and a throughput probe inside the lab. Raises LabError when the
    link is not what the profile says."""
    out = run(["docker", "compose", "-p", PROJECT, "exec", "-T", "client",
               "ping", "-q", "-c", "60", "-i", "0.1", IPERF], timeout=120, check=False).stdout
    # 60 pings; parse the summary lines.
    sent = recv = 0
    rtt = {}
    for line in out.splitlines():
        if "packets transmitted" in line:
            sent = int(line.split()[0]); recv = int(line.split()[3])
        if line.startswith("rtt"):
            vals = line.split("=")[1].split()[0].split("/")
            rtt = dict(zip(("min", "avg", "max", "mdev"), map(float, vals)))
    if not rtt:
        raise LabError(f"{name}: ping got no replies")
    res = {"ping": {"sent": sent, "received": recv, "loss_pct": round(100 * (sent - recv) / sent, 1),
                    "rtt_ms": rtt}}
    target = prof["rtt_ms"]
    problems = []
    # avg, not median: ping -q prints no per-packet times. Jitter is uniform
    # around the delay, so the average stays at the target.
    lo = target * 0.9 - 0.5 - prof["jitter_ms"] * 0.2
    hi = target * 1.1 + 2 + prof["jitter_ms"] * 0.2
    if not lo <= rtt["avg"] <= hi:
        problems.append(f"ping avg {rtt['avg']:.1f} ms outside {lo:.1f}-{hi:.1f} ms")
    for direction, cap, extra in (("down", prof["down_mbit"], ["-R"]), ("up", prof["up_mbit"], [])):
        if cap:
            d = iperf("-u", "-b", f"{cap * 1.25:g}M", "-t", "4", *extra)
            got = d["end"]["sum_received"]["bits_per_second"] / 1e6
            want_lo = cap * 0.8 * (1 - prof["loss_pct"] / 100)
            res[f"udp_{direction}_mbit"] = round(got, 2)
            if not want_lo <= got <= cap * 1.05:
                problems.append(f"udp {direction} {got:.1f} Mbit/s outside {want_lo:.1f}-{cap * 1.05:.1f}")
    # One TCP stream toward the client: information, not a pass/fail.
    d = iperf("-t", "8", "-R", timeout=90)
    res["tcp_down_1stream_mbit"] = round(d["end"]["sum_received"]["bits_per_second"] / 1e6, 2)
    res["ok"] = not problems
    res["problems"] = problems
    return res


# --- matrix -------------------------------------------------------------

def one_profile(name, prof, a, outdir):
    print(f"== {name}", file=sys.stderr, flush=True)
    lab("shape", name)
    rec = {"profile": name, "spec": prof, "started": now()}
    rec["link"] = check_link(name, prof)
    if not rec["link"]["ok"]:
        raise LabError(f"{name}: link check failed: {'; '.join(rec['link']['problems'])}")
    rec["passes"] = {}
    for cache in ("cold", "warm"):
        lab("umount")
        before = counter()
        out = lab("mount", "--cold" if cache == "cold" else "--warm")
        after = counter()
        ms = [int(x.split("=")[1]) for x in out.split() if x.startswith("mount_ms=")]
        if not ms:
            raise LabError(f"{name}: lab.sh mount printed no mount_ms")
        ps = {"mount": {"ms": ms[0], "proxy": {k: after[k] - before[k] for k in ("commands", "flushes")}}}
        # open runs first: the list probe fills the metadata caches, and a
        # stat after it would read the cache, not the server.
        for op, args in (
                ("open", ["open", "--count", str(a.open_count)]),
                ("list", ["list"]),
                ("seq", ["seq", "--budget", str(a.seq_budget)]),
                ("scrub", ["scrub", "--reads", "50", "--budget", str(a.scrub_budget)]),
                ("write", ["write", "--files", str(a.write_files), "--kib", str(a.write_kib),
                           "--tag", f"{name}-{cache}", "--budget", str(a.write_budget)])):
            print(f"   {cache} {op}", file=sys.stderr, flush=True)
            ps[op] = probe(*args)
        rec["passes"][cache] = ps
    rec["finished"] = now()
    with open(os.path.join(outdir, f"{name}.json"), "w") as f:
        json.dump(rec, f, indent=2)
    return rec


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def fmt_ms(v):
    if v is None:
        return "-"
    return f"{v / 1000:.2f} s" if v >= 1000 else f"{v:.0f} ms" if v >= 10 else f"{v:.1f} ms"


def scoreboard(recs, meta):
    rows = [
        "| Profile | Cache | Mount | List 1 folder | Walk tree | Stat (median) | Open (median) | TTFB | Seq read | Scrub read (median / p90) | Write files | Round trips: mount / list+walk / per file stat+open+read / per file written |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in recs:
        for cache, p in r["passes"].items():
            L, O, S, C, W = p["list"], p["open"], p["seq"], p["scrub"], p["write"]
            seq = f"{S['mbit_s']:.1f} Mbit/s" + ("" if S["complete"] else f" ({S['bytes_read'] >> 20} MiB in {S['seconds']:.0f} s)")
            scrub = f"{fmt_ms(C['read']['median_ms'])} / {fmt_ms(C['read']['p90_ms'])}"
            if C["done"] < C["planned"]:
                scrub += f" ({C['done']}/{C['planned']} reads)"
            write = f"{W['files']} × {W['kib_each']} KiB in {W['seconds']:.1f} s"
            nfile = max(O["files"], 1)
            rows.append(
                f"| {r['profile']} | {cache} | {fmt_ms(p['mount']['ms'])} | {fmt_ms(L['bigdir']['ms'])} "
                f"({L['bigdir']['entries']}) | {fmt_ms(L['tree_walk']['ms'])} ({L['tree_walk']['files']}) | "
                f"{fmt_ms(O['stat']['median_ms'])} | {fmt_ms(O['open']['median_ms'])} | {fmt_ms(S['ttfb_ms'])} | {seq} | "
                f"{scrub} | {write} | {p['mount']['proxy']['flushes']} / {L['proxy']['flushes']} / {O['proxy']['flushes'] / nfile:.1f} / "
                f"{W['proxy']['flushes'] / max(W['files'], 1):.1f} |")
    links = ["| Profile | Spec | Ping avg (min-max) | Ping loss | UDP down / up | 1 TCP stream down |", "|---|---|---|---|---|---|"]
    for r in recs:
        s, l = r["spec"], r["link"]
        spec = f"{s['rtt_ms']:g} ms" + (f" ± {s['jitter_ms']:g}" if s["jitter_ms"] else "") + \
               (f", {s['loss_pct']:g}% loss" if s["loss_pct"] else "") + \
               (f", {s['down_mbit']:g}/{s['up_mbit']:g} Mbit/s" if s["down_mbit"] else ", no cap")
        pr = l["ping"]["rtt_ms"]
        udp = f"{l.get('udp_down_mbit', '-')} / {l.get('udp_up_mbit', '-')}"
        links.append(f"| {r['profile']} | {spec} | {pr['avg']:.1f} ms ({pr['min']:.1f}-{pr['max']:.1f}) | "
                     f"{l['ping']['loss_pct']}% | {udp} | {l['tcp_down_1stream_mbit']} Mbit/s |")
    head = (f"# Remote lab scoreboard\n\nRun {meta['run_id']}, commit {meta['commit'][:12]}, "
            f"fixture `{meta['fixture']}`, juicefs `{meta['juicefs']}`.\n")
    cc = meta.get("congestion_control")
    if cc:
        head += "TCP congestion control: " + ", ".join(f"{k} {v}" for k, v in cc.items()) + ".\n"
    return "\n".join([head, "## Links (checked before measuring)", "", *links, "",
                      "## Operations", "", *rows, ""])


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profiles", default="lan,same-city,abroad,abroad-noloss,hotel,cellular")
    ap.add_argument("--fixture", choices=("small", "full"), default="full")
    ap.add_argument("--out", default=os.path.join(HERE, "results"))
    ap.add_argument("--open-count", type=int, default=30)
    ap.add_argument("--seq-budget", type=float, default=60)
    ap.add_argument("--scrub-budget", type=float, default=90)
    ap.add_argument("--write-files", type=int, default=50)
    ap.add_argument("--write-kib", type=int, default=64)
    ap.add_argument("--write-budget", type=float, default=90)
    ap.add_argument("--no-up", action="store_true", help="the lab is already up")
    ap.add_argument("--keep", action="store_true", help="leave the lab running")
    ap.add_argument("--bbr", action="store_true",
                    help="the server sends with BBR (server/docker-compose.tcp-bbr.yml; applied at lab up)")
    a = ap.parse_args()
    if a.bbr:
        os.environ["JMLAB_BBR"] = "1"

    allp = profiles()
    names = [n.strip() for n in a.profiles.split(",") if n.strip()]
    for n in names:
        if n not in allp:
            ap.error(f"unknown profile {n}; known: {', '.join(allp)}")
    run_id = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    outdir = os.path.join(a.out, run_id)
    os.makedirs(outdir, exist_ok=True)
    commit = run(["git", "-C", HERE, "rev-parse", "HEAD"], check=False).stdout.strip() or "unknown"
    meta = {"run_id": run_id, "commit": commit, "fixture": a.fixture, "profiles": names,
            "started": now(), "ok": False}
    recs = []
    code = 0
    try:
        if not a.no_up:
            print("== lab up", file=sys.stderr, flush=True)
            lab("up", timeout=1800)
        meta["juicefs"] = in_client("juicefs", "--version").strip()
        meta["congestion_control"] = dict(l.split() for l in lab("cc").splitlines() if l.strip())
        lab("shape", "lan")
        lab("mount")
        print(f"== seed ({a.fixture}) on lan", file=sys.stderr, flush=True)
        meta["seed"] = json.loads(in_client("python3", "/lab/lib/probe.py", "seed", "--size", a.fixture,
                                            timeout=3600))
        for n in names:
            recs.append(one_profile(n, allp[n], a, outdir))
        meta["ok"] = True
    except (LabError, subprocess.TimeoutExpired, ValueError, KeyError) as e:
        meta["error"] = str(e)
        print(f"LAB BROKE: {e}", file=sys.stderr)
        code = 1
    finally:
        meta["finished"] = now()
        with open(os.path.join(outdir, "run.json"), "w") as f:
            json.dump(meta, f, indent=2)
        if recs:
            md = scoreboard(recs, {**meta, "juicefs": meta.get("juicefs", "?")})
            with open(os.path.join(outdir, "scoreboard.md"), "w") as f:
                f.write(md)
            print(md)
        if not a.keep:
            print("== lab down", file=sys.stderr, flush=True)
            run([LAB, "down"], check=False, timeout=300)
    print(f"results: {outdir}", file=sys.stderr)
    return code


if __name__ == "__main__":
    sys.exit(main())
