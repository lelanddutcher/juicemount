#!/usr/bin/env python3
"""sweep.py: tuning experiments on a running remote lab (lab.sh up first).

  ./experiments/sweep.py --profiles abroad,hotel --configs base,bbr,ra32 --out results/x.jsonl

Per profile and config it sets TCP congestion control in the lab's own
container network namespaces (never the host's), remounts with extra
juicefs flags, and runs experiments/xprobe.py cold and warm. One JSON line
per (profile, config) goes to --out.
"""
import argparse, json, os, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
LAB = os.path.join(HERE, "..", "lab.sh")
PROJECT = os.environ.get("JMLAB_PROJECT", "jmlab")
IPERF = "10.231.10.40"

# name: (extra mount flags, congestion control on the NAS side, on the client)
CONFIGS = {
    "base":        ("", "cubic", "cubic"),
    "bbr":         ("", "bbr", "bbr"),
    "ra8":         ("--max-readahead 8M", "cubic", "cubic"),
    "ra32":        ("--max-readahead 32M", "cubic", "cubic"),
    "ra128":       ("--max-readahead 128M", "cubic", "cubic"),
    "ra32-bbr":    ("--max-readahead 32M", "bbr", "bbr"),
    "ra128-bbr":   ("--max-readahead 128M", "bbr", "bbr"),
    "ra32-pf4":    ("--max-readahead 32M --prefetch 4", "cubic", "cubic"),
    "ra32-bbr-pf4": ("--max-readahead 32M --prefetch 4", "bbr", "bbr"),
}


def sh(cmd, timeout=900, check=True, env=None):
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
    if check and p.returncode != 0:
        raise RuntimeError(f"{cmd[:4]} -> {p.returncode}: {p.stderr[-1500:]}")
    return p.stdout


def cid(svc):
    return sh(["docker", "compose", "-p", PROJECT, "ps", "-q", svc]).strip()


def set_cc(svc, cc):
    # Privileged helper joins ONLY the service's network namespace; the
    # sysctl is per-netns. fq gives BBR proper pacing.
    qd = "tc qdisc replace dev eth0 root fq" if cc == "bbr" else "{ tc qdisc del dev eth0 root 2>/dev/null || true; }"
    sh(["docker", "run", "--rm", "--privileged", "--network", f"container:{cid(svc)}", "--entrypoint", "sh",
        "jmlab:local", "-c", f"sysctl -qw net.ipv4.tcp_congestion_control={cc} && {qd}"])


def counter():
    return json.loads(sh([LAB, "proxy-counter"]))


def x(*args, timeout=900):
    b = counter()
    out = json.loads(sh([LAB, "exec", "python3", "/lab/experiments/xprobe.py", *args], timeout=timeout))
    a = counter()
    out["rt"] = a["flushes"] - b["flushes"]
    out["cmds"] = a["commands"] - b["commands"]
    return out


def mount(opts, cold):
    env = dict(os.environ, JMLAB_MOUNT_OPTS=opts)
    b = counter()
    out = sh([LAB, "mount", "--cold" if cold else "--warm"], env=env)
    a = counter()
    return {"ms": int(out.split("mount_ms=")[1].split()[0]), "rt": a["flushes"] - b["flushes"]}


def iperf(args):
    for _ in range(5):
        p = subprocess.run(["docker", "compose", "-p", PROJECT, "exec", "-T", "client", "iperf3", "-c", IPERF,
                            "-J", "-t", "10", *args], capture_output=True, text=True, timeout=90)
        try:
            d = json.loads(p.stdout)
            if "error" not in d:
                return round(d["end"]["sum_received"]["bits_per_second"] / 1e6, 2)
        except ValueError:
            pass
        time.sleep(3)
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profiles", required=True)
    ap.add_argument("--configs", required=True)
    ap.add_argument("--budget", type=float, default=40)
    ap.add_argument("--out", required=True)
    ap.add_argument("--writes", action="store_true", help="also bigwrite 32 MiB and 10 small files")
    ap.add_argument("--iperf", action="store_true")
    a = ap.parse_args()
    B = str(a.budget)
    for prof in a.profiles.split(","):
        sh([LAB, "shape", prof])
        for name in a.configs.split(","):
            opts, nas_cc, cli_cc = CONFIGS[name]
            for s in ("silo", "respproxy", "iperf"):
                set_cc(s, nas_cc)
            set_cc("client", cli_cc)
            rec = {"profile": prof, "config": name, "opts": opts, "nas_cc": nas_cc, "client_cc": cli_cc}
            print(f"== {prof} {name}", file=sys.stderr, flush=True)
            try:
                if a.iperf:
                    rec["iperf_down"] = iperf(["-R"])
                    rec["iperf_up"] = iperf([])
                for cache in ("cold", "warm"):
                    r = rec[cache] = {"mount": mount(opts, cache == "cold")}
                    r["ttfb0"] = x("ttfb", "--file", "/jfs/exp/ttfb.bin", "--offset", "0")
                    if cache == "cold":
                        r["ttfb_ranged"] = x("ttfb", "--file", "/jfs/exp/ttfb2.bin", "--offset", "4096")
                    r["seq"] = x("seq", "--file", "/jfs/exp/seq.bin", "--budget", B)
                    r["scrub"] = x("scrub", "--file", "/jfs/exp/scrub.bin", "--reads", "30", "--budget", B)
                if a.writes:
                    rec["bigwrite"] = x("bigwrite", "--mib", "32", "--tag", f"{prof}-{name}")
                    rec["smallwrite"] = x("smallwrite", "--files", "10", "--tag", f"{prof}-{name}")
            except Exception as e:  # keep going; record the failure
                rec["error"] = str(e)[-800:]
            with open(a.out, "a") as f:
                f.write(json.dumps(rec) + "\n")
            print(json.dumps(rec), flush=True)
    for s in ("silo", "respproxy", "iperf", "client"):
        set_cc(s, "cubic")
    sh([LAB, "shape", "lan"])


if __name__ == "__main__":
    main()
