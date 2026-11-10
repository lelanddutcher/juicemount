#!/usr/bin/env python3
"""measure.py: run Link lab cases and write JSON plus a Markdown scoreboard.

  ./measure.py                                  every case in cases.tsv
  ./measure.py --cases direct-forward,hard-nat-relay --profile same-city

For each case it applies the router switches (lab.sh case), joins a new
Mac node with a fresh pairing code, then measures:

  connect   control registration, first Redis PING through Link, first
            direct path (if one forms within --direct-wait seconds)
  path      direct / derp, from `tailscale status --json` and `tailscale ping`
  bench     Redis PING round trip and pipelined rate, object GET and PUT
            through the NAS node's service ports; relay CPU meanwhile
  recovery  time until a Redis PING answers again after the home router
            restarts, then after the Manager restarts (Headscale, and the
            NAS node in bridge mode), then, in host mode, after the node
            container restarts

wan-down-boot runs a sequence instead (run_wan_down_boot): home internet
down, Manager restart, the Mac at home connects, the Mac away cannot,
internet back, the Mac away connects.

It exits 2 only when the lab breaks: the lab does not come up, a case
cannot be applied, or a case that should connect does not. A path other
than the expected one is reported, never a failure.
"""
import argparse
import datetime
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
LAB = os.path.join(HERE, "lab.sh")
SERVER = "https://link.lab.example:30193"


def log(msg):
    print(f"[measure] {msg}", file=sys.stderr, flush=True)


def lab(*args, check=True, timeout=600):
    r = subprocess.run([LAB, *args], capture_output=True, text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise RuntimeError(f"lab.sh {' '.join(args)} failed: {r.stderr.strip()[-600:]}")
    return r


def lab_json(*args, timeout=600):
    r = lab(*args, timeout=timeout)
    lines = [ln for ln in r.stdout.splitlines() if ln.startswith("{")]
    if not lines:
        raise RuntimeError(f"lab.sh {' '.join(args)}: no JSON in output: {r.stdout[-300:]} {r.stderr[-300:]}")
    return json.loads(lines[-1])


def read_cases():
    out = []
    with open(os.path.join(HERE, "cases.tsv")) as f:
        for line in f:
            if line.strip() and not line.startswith("#"):
                out.append(line.split()[0])
    return out


def relay_cpu_usec():
    return int(lab("relay-cpu").stdout.strip())


def recover(case, site, service):
    """Restart SERVICE, then time until a Redis PING through Link answers."""
    t0 = time.monotonic()
    lab("restart", service)
    restarted = time.monotonic()
    w = lab_json("probe", site, "wait-ping", "--timeout", "150", timeout=200)
    out = {"service": service, "restart_ms": round((restarted - t0) * 1000)}
    if "error" in w:
        out["error"] = w["error"]
    else:
        out["ms"] = round((time.monotonic() - t0) * 1000)
        p = lab_json("probe", site, "path", "--count", "3", timeout=120)
        out["path_after"] = p.get("path")
    return out


def run_case(name, a):
    if name == "wan-down-boot":
        return run_wan_down_boot(name, a)
    res = {"case": name, "started": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")}
    res["config"] = lab_json("case", name)
    site, expect = res["config"]["site"], res["config"]["expect"]
    host = f"{site}-mac-{name}"[:60]
    up_timeout = "25" if expect == "fail" else "60"
    c = lab_json("probe", site, "connect", "--server", SERVER, "--hostname", host,
                 "--up-timeout", up_timeout, "--direct-wait", str(a.direct_wait), timeout=300)
    res["connect"] = c
    if "error" in c:
        res["path"] = "none"
        res["outcome"] = "expected" if expect == "fail" else "broken"
        lab("exec", f"{site}-mac", "/lab/lib/mac.sh", "stop", check=False)
        return res
    p = lab_json("probe", site, "path", timeout=120)
    res["path_probe"] = p
    res["path"] = p.get("path")
    u0, t0 = relay_cpu_usec(), time.monotonic()
    b = lab_json("probe", site, "bench", "--put-mib", str(a.put_mib), timeout=900)
    u1, t1 = relay_cpu_usec(), time.monotonic()
    b["relay_cpu_pct"] = round((u1 - u0) / ((t1 - t0) * 1e6) * 100, 1)
    b["relay_cpu_s"] = round((u1 - u0) / 1e6, 2)
    res["bench"] = b
    if a.recovery:
        res["recovery"] = [recover(name, site, "home-router"), recover(name, site, "manager")]
        if res["config"].get("nas_net") == "host":
            res["recovery"].append(recover(name, site, "link-node"))
    lab("exec", f"{site}-mac", "/lab/lib/mac.sh", "stop", check=False)
    broken = "error" in b or any("error" in r for r in res.get("recovery", []))
    if broken:
        res["outcome"] = "broken"
    elif expect == "fail":
        res["outcome"] = "unexpected (connected)"
    else:
        res["outcome"] = "expected" if res["path"] == expect else f"unexpected ({res['path']})"
    return res


def connect(site, name, up_timeout, direct_wait):
    host = f"{site}-mac-{name}"[:60]
    c = lab_json("probe", site, "connect", "--server", SERVER, "--hostname", host,
                 "--up-timeout", str(up_timeout), "--direct-wait", str(direct_wait),
                 timeout=up_timeout + 240)
    if "error" not in c:
        c["path_probe"] = lab_json("probe", site, "path", "--count", "3", timeout=120)
    lab("exec", f"{site}-mac", "/lab/lib/mac.sh", "stop", check=False)
    return c


def run_wan_down_boot(name, a):
    """The NAS boots while the home has no internet, then the internet returns.

    Before the Manager kept its own DERP map file, Headscale exited when it
    could not fetch the map URL at start, so Link (and LAN access through
    Link) stayed down until the internet came back.
    """
    res = {"case": name, "started": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")}
    res["config"] = lab_json("case", name)
    steps = res["wan_down_boot"] = {}
    lab("uplink", "home", "down")
    try:
        t0 = time.monotonic()
        lab("restart", "manager")
        if res["config"].get("nas_net") == "host":
            lab("restart", "link-node")
        w = lab("wait-link", check=False, timeout=200)
        steps["link_ready"] = w.returncode == 0
        steps["link_ready_ms"] = round((time.monotonic() - t0) * 1000) if w.returncode == 0 else None
        steps["home_while_down"] = connect("home", name + "-down", 60, a.direct_wait)
        steps["away_while_down"] = connect("away", name + "-down", 20, 0)
    finally:
        t1 = time.monotonic()
        lab("uplink", "home", "up")
    c = connect("away", name + "-up", 90, a.direct_wait)
    c["since_wan_up_ms"] = round((time.monotonic() - t1) * 1000)
    steps["away_after_up"] = c
    res["connect"] = c
    res["path"] = c.get("path_probe", {}).get("path", "none") if "error" not in c else "none"
    # The lab fails the case only when Link itself breaks: it does not start
    # without internet, the away Mac connects while the home has no internet
    # (the uplink switch did not work), or the away Mac does not connect once
    # it is back. The home Mac's result is reported. Without a relay it
    # connects only if the NAS node advertises an address the LAN can reach:
    # in bridge mode the NAS node knows only its container address; the node
    # container (host) has the NAS's LAN address, so there it must connect.
    ok = (steps["link_ready"]
          and "error" in steps["away_while_down"]
          and "error" not in c)
    host = res["config"].get("nas_net") == "host"
    if not ok:
        res["outcome"] = "broken"
    elif "error" in steps["home_while_down"]:
        res["outcome"] = ("unexpected; home Mac: no LAN path" if host
                          else "expected; home Mac: no LAN path without a relay")
    else:
        res["outcome"] = "expected"
    return res


def fmt_s(ms):
    return "–" if ms is None else f"{ms / 1000:.1f}"


def scoreboard(run, results):
    lines = [f"# Link lab scoreboard ({run['id']})", "",
             f"Commit `{run['commit'][:12]}`, profile `{run['profile']}`. "
             "Times in seconds. Throughput in Mbit/s through the tunnel to the NAS node's ports "
             "(GET: 64 MiB object store read toward the Mac; PUT: upload). Relay CPU: the relay "
             "container's CPU during the bench, % of one core.", "",
             "| Case | Site | NAS node | NAT home / away | Forward ctrl / UDP | Expect | Path | Outcome | Control | First PING | Direct at | Redis PING ms | GET | PUT | Relay CPU | Router restart → PING | Manager restart → PING | Node container restart → PING |",
             "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in results:
        c = r.get("config", {})
        cn = r.get("connect", {})
        b = r.get("bench", {})
        rec = {x["service"]: x for x in r.get("recovery", [])}
        nat = f"{c.get('home_nat', '?')} / {c.get('away_nat', '?')}"
        if c.get("cgnat", "off") != "off":
            nat += f" + CGNAT ({c['cgnat']})"
        if c.get("portmap") == "on":
            nat += ", NAT-PMP"
        if c.get("site") == "home":
            nat = f"LAN; hairpin {c.get('hairpin')}, local DNS {c.get('split_dns')}"

        def rc(svc):
            x = rec.get(svc)
            if not x:
                return "–"
            if "error" in x:
                return "failed"
            return f"{fmt_s(x['ms'])} ({x.get('path_after', '?')})"

        lines.append("| " + " | ".join([
            r["case"], c.get("site", "?"),
            {"host": "own container, host network", "bridge": "in Manager, bridge"}.get(c.get("nas_net"), "?"), nat, f"{c.get('fwd_ctrl')} / {c.get('fwd_udp')}",
            c.get("expect", "?"), r.get("path", "?"), r.get("outcome", "?"),
            fmt_s(cn.get("control_ms")), fmt_s(cn.get("first_ping_ms")), fmt_s(cn.get("direct_ms")),
            str(b.get("redis_ping_ms", {}).get("median", "–")),
            str(b.get("s3_get", {}).get("mbit_s", "–")), str(b.get("s3_put", {}).get("mbit_s", "–")),
            "–" if "relay_cpu_pct" not in b else f"{b['relay_cpu_pct']}%",
            rc("home-router"), rc("manager"), rc("link-node")]) + " |")
    for r in results:
        s = r.get("wan_down_boot")
        if not s:
            continue
        hd, ad, au = s.get("home_while_down", {}), s.get("away_while_down", {}), s.get("away_after_up", {})
        lines += ["", f"`{r['case']}`: home internet down while the Manager (and the node container) restart, then back.", "",
                  "| Step | Result |", "|---|---|",
                  f"| Link ready after Manager restart, internet down | "
                  + ("yes, " + fmt_s(s.get("link_ready_ms")) + " s" if s.get("link_ready") else "no") + " |",
                  "| Mac at home connects, internet down | "
                  + (f"yes: first PING {fmt_s(hd.get('first_ping_ms'))} s, path {hd.get('path_probe', {}).get('path', '?')}"
                     if "error" not in hd else f"no: {hd['error']}") + " |",
                  "| Mac away connects, internet down (should not) | "
                  + ("no (as expected)" if "error" in ad else "yes (unexpected)") + " |",
                  "| Mac away connects after the internet returns | "
                  + (f"yes: first PING {fmt_s(au.get('first_ping_ms'))} s after its start, path {au.get('path_probe', {}).get('path', '?')}"
                     if "error" not in au else f"no: {au['error']}") + " |"]
    errs = [(r["case"], r.get("connect", {}).get("error") or r.get("error")) for r in results
            if r.get("connect", {}).get("error") or r.get("error")]
    if errs:
        lines += ["", "Errors:", ""] + [f"- `{n}`: {e}" for n, e in errs]
    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cases", default="", help="comma-separated case names (default: all)")
    ap.add_argument("--profile", default="same-city", help="remote-lab profile for the wan")
    ap.add_argument("--direct-wait", type=int, default=20)
    ap.add_argument("--put-mib", type=int, default=16)
    ap.add_argument("--no-recovery", dest="recovery", action="store_false")
    ap.add_argument("--keep", action="store_true", help="leave the lab running")
    ap.add_argument("--no-up", action="store_true", help="reuse a running lab")
    a = ap.parse_args()

    cases = [c for c in a.cases.split(",") if c] or read_cases()
    commit = subprocess.run(["git", "-C", HERE, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    run = {"id": datetime.datetime.now().strftime("%Y%m%d-%H%M%S"), "commit": commit or "unknown",
           "profile": a.profile, "cases": cases}
    outdir = os.path.join(HERE, "results", run["id"])
    os.makedirs(outdir, exist_ok=True)
    results, status = [], 0
    try:
        if not a.no_up:
            log("starting the lab (lab.sh up)")
            t = time.monotonic()
            r = lab("up", timeout=1800, check=False)
            if r.returncode != 0:
                sys.stderr.write(r.stderr[-3000:])
                raise RuntimeError("lab.sh up failed")
            run["up_s"] = round(time.monotonic() - t, 1)
        run["versions"] = {
            "tailscale_client": lab("exec", "away-mac", "tailscale", "version").stdout.split("\n")[0],
            "headscale": lab("exec", "manager", "headscale", "version").stdout.split("\n")[0],
        }
        log(lab("shape", a.profile).stdout.strip())
        for name in cases:
            log(f"case {name}")
            t = time.monotonic()
            try:
                res = run_case(name, a)
            except (RuntimeError, subprocess.TimeoutExpired, ValueError) as e:
                res = {"case": name, "outcome": "broken", "error": str(e)}
            res["seconds"] = round(time.monotonic() - t, 1)
            results.append(res)
            with open(os.path.join(outdir, f"{name}.json"), "w") as f:
                json.dump(res, f, indent=2)
            log(f"  {res.get('outcome')} path={res.get('path')} ({res['seconds']} s)")
            if res.get("outcome") == "broken":
                status = 2
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        run["error"] = str(e)
        log(f"lab broke: {e}")
        status = 2
    finally:
        run["status"] = "ok" if status == 0 else "broken"
        with open(os.path.join(outdir, "run.json"), "w") as f:
            json.dump(run, f, indent=2)
        with open(os.path.join(outdir, "results.json"), "w") as f:
            json.dump({"run": run, "results": results}, f, indent=2)
        sb = scoreboard(run, results)
        with open(os.path.join(outdir, "scoreboard.md"), "w") as f:
            f.write(sb)
        print(sb)
        if not a.keep:
            lab("down", check=False)
    return status


if __name__ == "__main__":
    sys.exit(main())
