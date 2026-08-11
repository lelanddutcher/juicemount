#!/usr/bin/env python3
"""cell_scoreboard.py — the INSTANT-NAV sprint's condition-matrix runner.

Runs the budget measurements from INSTANT_NAV_SPRINT.md against the RUNNING
build and appends one JSON row per run to scoreboard.jsonl (+ prints a table).
Designed for unattended cellular/10GbE runs; every probe is time-bounded so a
wedged mount can never hang the rig.

Budgets covered per run:
  B1  warm nav populate (mirrored dir, list view, warm pass)
  B2  cold LIST populate of a mirrored dir (client cache expired, mirror serves)
  B4' server-created-content visibility: mkdir+files server-side via SSH →
      poll the NFS mount until all N entries list (the OpenLoupe/farm
      "new content appears on the client" UX; the honest cellular first-touch)
  B5/B6 cold COLUMN previews (45-item viewport) on a fresh block-cold slice
  B8  sequential read throughput (one bounded file, --bytes-heavy only)
  env RTT to backend, route interface, running build label

Usage:
  cell_scoreboard.py --label 0.3.0-baseline \
      --warm-dir "/Volumes/zpool/Film Projects/GMTM" \
      --preview-dir "/Volumes/zpool/.../Good Videos" --preview-slice 90 \
      [--bytes-heavy] [--skip-preview] [--thru-file PATH]

State: coldpool usage is the caller's job (pass fresh --preview-slice offsets).
"""
import argparse, json, os, subprocess, sys, time, urllib.request

CP = "http://127.0.0.1:11050"
NAS = "192.168.0.197"
SSH = ["ssh", "-i", os.path.expanduser("~/.ssh/codex_truenas_tmp"),
       "-o", "ConnectTimeout=10", f"root@{NAS}"]
HERE = os.path.dirname(os.path.abspath(__file__))
SCOREBOARD = os.path.join(HERE, "scoreboard.jsonl")


def bounded(cmd, timeout, **kw):
    try:
        return subprocess.run(cmd, capture_output=True, text=True,
                              timeout=timeout, **kw)
    except subprocess.TimeoutExpired:
        return None


def metrics():
    try:
        with urllib.request.urlopen(CP + "/metrics", timeout=5) as r:
            return json.load(r)
    except Exception:
        return {}


def env_capture():
    out = {"ts": time.strftime("%Y-%m-%dT%H:%M:%S")}
    r = bounded(["route", "get", NAS], 6)
    out["route_if"] = ""
    if r and r.returncode == 0:
        for ln in r.stdout.splitlines():
            if "interface:" in ln:
                out["route_if"] = ln.split(":")[1].strip()
    p = bounded(["ping", "-c", "3", "-t", "5", NAS], 20)
    out["rtt_ms"] = None
    if p and "min/avg/max" in p.stdout:
        try:
            out["rtt_ms"] = float(p.stdout.split("=")[1].split("/")[1])
        except Exception:
            pass
    h = bounded(["curl", "-s", "-m5", "-o", "/dev/null", "-w", "%{http_code}",
                 CP + "/health"], 8)
    out["health"] = h.stdout.strip() if h else "timeout"
    return out


def warm_and_cold_list(warm_dir):
    """B1 warm + B2 cold-list on a mirrored dir via finder_populate_clean."""
    res = {}
    r = bounded([sys.executable, "-u", os.path.join(HERE, "finder_populate_clean.py"),
                 warm_dir, "--view", "list", "--cool-s", "7", "--json"], 240)
    if r and r.returncode == 0 and r.stdout.strip():
        try:
            d = json.loads(r.stdout.strip().splitlines()[-1])
            res["b2_cold_list_ms"] = round(d["cold"]["populate_ms"], 1)
            res["b1_warm_list_ms"] = round(d["warm"]["populate_ms"], 1)
        except Exception as e:
            res["list_err"] = str(e)[:120]
    else:
        res["list_err"] = "runner timeout/failed"
    return res


def server_content_visibility(n_files=20):
    """B4': create a fresh dir + N files SERVER-SIDE, then poll the NFS mount
    until all N list. Measures push→mirror→serve latency end-to-end — the
    'new server content appears on the client' UX."""
    stamp = time.strftime("%H%M%S")
    rel = f"JM_SPRINT_TEST/coldpool/{stamp}"
    mk = " && ".join([
        f"docker exec ix-juicemount-juicefs-1 sh -c 'mkdir -p /jfs/{rel} && "
        f"for i in $(seq 1 {n_files}); do echo x > /jfs/{rel}/f$i.txt; done && "
        f"chown -R 501:20 /jfs/{rel.split('/')[0]}'"
    ])
    t0 = time.time()
    r = bounded(SSH + [mk], 40)
    if not r or r.returncode != 0:
        return {"b4_visibility_err": (r.stderr[:120] if r else "ssh timeout")}
    created = time.time()
    nfs_dir = f"/Volumes/zpool/{rel}"
    deadline = time.time() + 120
    seen = -1
    while time.time() < deadline:
        try:
            seen = len([e for e in os.listdir(nfs_dir) if e.startswith("f")])
        except OSError:
            seen = -1
        if seen >= n_files:
            return {"b4_visibility_s": round(time.time() - created, 2),
                    "b4_create_ssh_s": round(created - t0, 2)}
        time.sleep(0.5)
    return {"b4_visibility_s": None, "b4_seen": seen,
            "b4_note": f"timeout: {seen}/{n_files} visible after 120s"}


def cold_previews(preview_dir, slice_start):
    """B5/B6: cold column-view previews on a fresh 45-item slice; also reports
    server bytes pulled (the amplification)."""
    m0 = metrics().get("bytes_read", 0)
    t0 = time.time()
    # in-process concurrent 16-wide viewport fetch (the real Finder shape)
    code = f"""
import os,time,concurrent.futures as cf
D={preview_dir!r}; S={slice_start}
fs=[os.path.join(D,e) for e in sorted(os.listdir(D)) if not e.startswith('._') and e!='.DS_Store']
fs=[p for p in fs if os.path.isfile(p)][S:S+45]
def rd(p):
    try:
        with open(p,'rb',buffering=0) as f: return len(f.read(262144))
    except OSError: return 0
t=time.time()
with cf.ThreadPoolExecutor(max_workers=16) as ex: list(ex.map(rd, fs))
print('%.0f %d' % ((time.time()-t)*1000, len(fs)))
"""
    r = bounded([sys.executable, "-c", code], 900)
    out = {}
    if r and r.returncode == 0 and r.stdout.strip():
        ms, n = r.stdout.split()
        out["b56_cold_preview_ms"] = float(ms)
        out["b56_items"] = int(n)
    else:
        out["b56_err"] = "preview probe timeout/failed"
    out["b56_server_mib"] = round((metrics().get("bytes_read", 0) - m0) / 1048576, 1)
    out["b56_wall_s"] = round(time.time() - t0, 1)
    return out


def throughput(path):
    """B8: one bounded sequential read (~costs its size in tunnel data)."""
    try:
        sz = os.path.getsize(path)
    except OSError:
        return {"b8_err": "file missing"}
    t0 = time.time()
    r = bounded(["dd", f"if={path}", "of=/dev/null", "bs=1m"], 900)
    el = time.time() - t0
    if not r:
        return {"b8_err": "dd timeout"}
    return {"b8_mb": round(sz / 1e6), "b8_mbps": round(sz / 1e6 / el, 1),
            "b8_s": round(el, 1)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--warm-dir", required=True)
    ap.add_argument("--preview-dir")
    ap.add_argument("--preview-slice", type=int, default=0)
    ap.add_argument("--skip-preview", action="store_true")
    ap.add_argument("--skip-visibility", action="store_true")
    ap.add_argument("--bytes-heavy", action="store_true")
    ap.add_argument("--thru-file")
    a = ap.parse_args()

    row = {"label": a.label}
    row.update(env_capture())
    print(f"[env] route={row['route_if']} rtt={row['rtt_ms']}ms health={row['health']}", flush=True)

    row.update(warm_and_cold_list(a.warm_dir))
    print(f"[B1/B2] warm={row.get('b1_warm_list_ms')}ms cold-list={row.get('b2_cold_list_ms')}ms", flush=True)

    if not a.skip_visibility:
        row.update(server_content_visibility())
        print(f"[B4'] server-content visibility={row.get('b4_visibility_s')}s", flush=True)

    if a.preview_dir and not a.skip_preview:
        row.update(cold_previews(a.preview_dir, a.preview_slice))
        print(f"[B5/6] cold previews={row.get('b56_cold_preview_ms')}ms "
              f"server={row.get('b56_server_mib')}MiB", flush=True)

    if a.bytes_heavy and a.thru_file:
        row.update(throughput(a.thru_file))
        print(f"[B8] {row.get('b8_mbps')} MB/s over {row.get('b8_mb')}MB", flush=True)

    with open(SCOREBOARD, "a") as f:
        f.write(json.dumps(row) + "\n")
    print("[scoreboard] row appended:", json.dumps(row), flush=True)


if __name__ == "__main__":
    main()
