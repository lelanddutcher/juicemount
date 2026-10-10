#!/usr/bin/env python3
"""latency_spread.py — METADATA-latency SPREAD harness for JuiceMount nav.

Hunts the wide-spread SLOW metadata OUTLIERS — the "really really slow
LOOKUP/GETATTR/READDIR/ACCESS/READLINK/GETXATTR" ops whose spread is wide even
on 10GbE (route en21 direct, backend RTT ~0.4ms, so anything >5ms is a real
outlier worth explaining). The seconds-class byte-read cost (Finder column-view
preview reads of SOURCE bytes) is already root-caused (NAV_LATENCY_DOSSIER.md
RC-1) and is OUT OF SCOPE here.

METADATA-ONLY. Every timed op is one of:
  os.lstat / os.stat        -> LOOKUP + GETATTR
  os.scandir (+ optional e.stat) -> READDIR / READDIRPLUS
  os.readlink               -> READLINK
  mac_getxattr / mac_listxattr -> GETXATTR / LISTXATTR  (ctypes, in-process)
NEVER open()s a file, NEVER reads file bytes.

WHY IN-PROCESS TIMING (the measurement-artifact decomposition)
--------------------------------------------------------------
A metadata op that hits the RAM mirror is ~10-90 microseconds. If you time it by
shelling out `stat`/`perl` per op, the fork+exec of the child process costs
~1-5 MILLISECONDS on macOS — 50-500x the thing you are trying to measure. Any
per-op-subprocess harness therefore reports mostly SPAWN TAX, not op latency.
This harness times every op IN-PROCESS with time.perf_counter_ns(), so the tax
is subtracted BY CONSTRUCTION. It also measures the tax explicitly (the
`spawn_tax` pseudo-scenario) so you can see exactly how much a shell-based
harness would have added.

COLD vs WARM
------------
COLD: sleep --cold-sleep (default 7s, > the JuiceFS FUSE 5s attr/entry TTL) then
      time the op against a target NOT touched this pass. For per-file cases we
      rotate through a POOL of distinct targets so the macOS NFS client attr
      cache is cold per-file too. For negative lookups we use fresh random names
      each rep, which the client never caches -> always a genuine server LOOKUP.
WARM: prime the op once, then time N reps against the same (now cached) target.

ATTRIBUTION (mirror vs backend/FUSE)
------------------------------------
Two independent signals, either sufficient:
  1. /metrics RPC-count delta around the scenario (needs the control plane up).
     A rise in READ/GETATTR/LOOKUP beyond the sample count, and the server's own
     p99/max, say whether the op fell through to the backend.
  2. per-sample latency: any single sample >= --cold-threshold-ms (default 3ms)
     is flagged a backend/FUSE fallback (RAM-mirror serve is tens of us). This
     works even with the control plane DOWN, so the harness still attributes
     outliers when only the mount is reachable.
Also tails ~/.juicemount/*.accesslog for ops >= 3ms in the scenario window when
present.

OUTPUT
------
Per-scenario p50/p99/MAX/mean (microseconds), N, cold/warm, RPC delta, and a
final RANKED table of the slowest metadata ops (widest-spread outliers first)
with their attribution. --json writes the full structured result.

READ-ONLY. Creates no file, reads no file bytes. The optional concurrent-writer
scenario (-i) is OFF unless you pass --writer-scratch DIR to a writable scratch
path; it only ever writes small temp files there, never user content.

Usage:
  latency_spread.py [--root /Volumes/zpool] [--n 200] [--cold-n 6]
                    [--cold-sleep 7] [--cp http://127.0.0.1:11050]
                    [--cold-threshold-ms 3] [--json out.json]
                    [--writer-scratch DIR] [--max-visit 60000] [--quiet]
"""
import os, sys, json, time, random, string, argparse, subprocess, ctypes, ctypes.util
import urllib.request

# ----------------------------------------------------------------------------
# ctypes macOS getxattr/listxattr (os.getxattr is Linux-only in CPython).
# In-process => no spawn tax. Signature (macOS):
#   ssize_t getxattr(path, name, void*value, size_t size, u_int32_t position, int options)
#   ssize_t listxattr(path, char*namebuf, size_t size, int options)
# ----------------------------------------------------------------------------
_libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
_HAVE_MAC_XATTR = hasattr(_libc, "getxattr")
if _HAVE_MAC_XATTR:
    _libc.getxattr.restype = ctypes.c_ssize_t
    _libc.getxattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p,
                               ctypes.c_size_t, ctypes.c_uint32, ctypes.c_int]
    _libc.listxattr.restype = ctypes.c_ssize_t
    _libc.listxattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p,
                                ctypes.c_size_t, ctypes.c_int]

def mac_listxattr(path):
    if not _HAVE_MAC_XATTR:
        raise OSError("no libc listxattr")
    p = path.encode()
    n = _libc.listxattr(p, None, 0, 0)
    if n < 0:
        raise OSError(ctypes.get_errno(), "listxattr")
    if n == 0:
        return []
    buf = ctypes.create_string_buffer(n)
    n = _libc.listxattr(p, buf, n, 0)
    if n < 0:
        raise OSError(ctypes.get_errno(), "listxattr")
    return [x.decode(errors="replace") for x in buf.raw[:n].split(b"\x00") if x]

def mac_getxattr(path, name):
    if not _HAVE_MAC_XATTR:
        raise OSError("no libc getxattr")
    p, nm = path.encode(), name.encode()
    n = _libc.getxattr(p, nm, None, 0, 0, 0)
    if n < 0:
        raise OSError(ctypes.get_errno(), "getxattr")  # ENOATTR is a fast, valid answer
    if n == 0:
        return b""
    buf = ctypes.create_string_buffer(n)
    n = _libc.getxattr(p, nm, buf, n, 0, 0)
    if n < 0:
        raise OSError(ctypes.get_errno(), "getxattr")
    return buf.raw[:n]

# ----------------------------------------------------------------------------
# stats
# ----------------------------------------------------------------------------
def pct(sorted_us, q):
    if not sorted_us:
        return 0.0
    if len(sorted_us) == 1:
        return sorted_us[0]
    idx = q * (len(sorted_us) - 1)
    lo = int(idx)
    frac = idx - lo
    if lo + 1 < len(sorted_us):
        return sorted_us[lo] * (1 - frac) + sorted_us[lo + 1] * frac
    return sorted_us[lo]

def summarize(samples_ns):
    us = sorted(s / 1000.0 for s in samples_ns)
    return {
        "n": len(us),
        "min_us": round(us[0], 2) if us else 0.0,
        "p50_us": round(pct(us, 0.50), 2),
        "p99_us": round(pct(us, 0.99), 2),
        "max_us": round(us[-1], 2) if us else 0.0,
        "mean_us": round(sum(us) / len(us), 2) if us else 0.0,
    }

# ----------------------------------------------------------------------------
# control plane
# ----------------------------------------------------------------------------
def cp_snap(cp):
    try:
        with urllib.request.urlopen(cp + "/metrics", timeout=6) as r:
            return json.load(r)
    except Exception:
        return None

RPCS = ["LOOKUP", "GETATTR", "ACCESS", "READDIR", "READDIRPLUS", "READ",
        "READLINK", "FSSTAT"]

def cp_delta(a, b):
    if not a or not b:
        return None
    ra, rb = a.get("rpcs", {}), b.get("rpcs", {})
    out = {}
    for k in RPCS:
        d = rb.get(k, {}).get("count", 0) - ra.get(k, {}).get("count", 0)
        if d:
            out[k] = d
    out["_bytes_read"] = b.get("bytes_read", 0) - a.get("bytes_read", 0)
    return out

# ----------------------------------------------------------------------------
# discovery — bounded, metadata-only scandir walk
# ----------------------------------------------------------------------------
BUNDLE_SUFFIXES = (".app", ".rtfd", ".key", ".photoslibrary", ".bundle",
                   ".framework", ".fcpbundle", ".imovielibrary", ".pkg",
                   ".numbers", ".pages")
SKIP = {".DS_Store"}

def discover(root, max_visit, budget_s):
    """One bounded walk; returns a dict of representative targets."""
    t0 = time.perf_counter()
    visited = 0
    big_dir = (None, -1)          # (path, direct_child_count)
    real_files = []               # pool of plain files (distinct, for cold rotation)
    dirs_pool = []                # pool of dirs (for cold-unmirrored)
    symlink = None
    bundle = None                 # (bundle_path, inner_file_path)
    deepest = (None, -1)          # (path, depth)
    root_depth = root.rstrip("/").count("/")

    stack = [root]
    while stack:
        if visited >= max_visit or (time.perf_counter() - t0) > budget_s:
            break
        d = stack.pop()
        depth = d.rstrip("/").count("/") - root_depth
        try:
            entries = list(os.scandir(d))
        except OSError:
            continue
        # direct child count (excluding dot-junk) for big-flat-dir pick
        nkids = sum(1 for e in entries if e.name not in SKIP and not e.name.startswith("._"))
        if nkids > big_dir[1]:
            big_dir = (d, nkids)
        for e in entries:
            visited += 1
            nm = e.name
            if nm in SKIP:
                continue
            try:
                is_link = e.is_symlink()
            except OSError:
                is_link = False
            if is_link and symlink is None:
                symlink = e.path
                continue
            try:
                is_dir = e.is_dir(follow_symlinks=False)
            except OSError:
                is_dir = False
            if is_dir:
                edepth = depth + 1
                if edepth > deepest[1]:
                    deepest = (e.path, edepth)
                if bundle is None and nm.endswith(BUNDLE_SUFFIXES):
                    inner = _first_inner_file(e.path)
                    if inner:
                        bundle = (e.path, inner)
                if len(dirs_pool) < 4000:
                    dirs_pool.append(e.path)
                # recurse (DFS, bounded)
                if not nm.startswith("._"):
                    stack.append(e.path)
            else:
                if not nm.startswith("._") and len(real_files) < 8000:
                    real_files.append(e.path)
    return {
        "big_dir": big_dir[0], "big_dir_n": big_dir[1],
        "real_files": real_files,
        "dirs_pool": dirs_pool,
        "symlink": symlink,
        "bundle": bundle,
        "deep_dir": deepest[0], "deep_depth": deepest[1],
        "visited": visited,
        "walk_s": round(time.perf_counter() - t0, 2),
    }

def _first_inner_file(bundle_path):
    for base, _dirs, files in os.walk(bundle_path):
        for f in files:
            if f not in SKIP and not f.startswith("._"):
                return os.path.join(base, f)
    return None

def path_depth_of(root, p):
    return p.rstrip("/").count("/") - root.rstrip("/").count("/")

# ----------------------------------------------------------------------------
# timing core
# ----------------------------------------------------------------------------
def rand_name():
    return ".__nx_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=10)) + "_probe"

def time_reps(fn, n):
    """Time fn() n times in-process; returns list of ns durations. fn may raise
    (e.g. FileNotFoundError for negative lookups) — that IS the measured op."""
    out = []
    for _ in range(n):
        t0 = time.perf_counter_ns()
        try:
            fn()
        except (FileNotFoundError, OSError):
            pass
        out.append(time.perf_counter_ns() - t0)
    return out

def time_cold(make_fn, targets, cold_n, cold_sleep):
    """Cold reps: sleep past TTL, then one timed op against a DISTINCT target."""
    out = []
    for i in range(cold_n):
        tgt = targets[i % len(targets)] if targets else None
        time.sleep(cold_sleep)
        fn = make_fn(tgt)
        t0 = time.perf_counter_ns()
        try:
            fn()
        except (FileNotFoundError, OSError):
            pass
        out.append(time.perf_counter_ns() - t0)
    return out

# ----------------------------------------------------------------------------
# scenario ops (each returns a zero-arg callable = one metadata op)
# ----------------------------------------------------------------------------
def op_neg_lookup(dirpath):
    return lambda: os.lstat(os.path.join(dirpath, rand_name()))

def op_appledouble(filepath):
    d, b = os.path.split(filepath)
    return lambda: os.lstat(os.path.join(d, "._" + b))

def op_stat(path):
    return lambda: os.stat(path)

def op_lstat(path):
    return lambda: os.lstat(path)

def op_readlink(path):
    return lambda: os.readlink(path)

def op_getxattr(path):
    def go():
        names = mac_listxattr(path)
        for nm in (names[:1] or ["com.apple.FinderInfo"]):
            try:
                mac_getxattr(path, nm)
            except OSError:
                pass
    return go

def op_readdir_names(dirpath):
    return lambda: [e.name for e in os.scandir(dirpath)]

def op_readdirplus(dirpath):
    def go():
        with os.scandir(dirpath) as it:
            for e in it:
                try:
                    e.stat(follow_symlinks=False)
                except OSError:
                    pass
    return go

# ----------------------------------------------------------------------------
# scenario runner
# ----------------------------------------------------------------------------
def run_scenario(key, label, cp, cold_thresh_ms, quiet,
                 warm_fn=None, warm_n=0,
                 cold_make=None, cold_targets=None, cold_n=0, cold_sleep=7,
                 absent=None):
    res = {"key": key, "label": label}
    if absent:
        res["absent"] = absent
        if not quiet:
            print(f"  [{key}] {label}: ABSENT — {absent}")
        return res

    for phase, do in (("cold", cold_make is not None and cold_n > 0),
                      ("warm", warm_fn is not None and warm_n > 0)):
        if not do:
            continue
        before = cp_snap(cp)
        if phase == "warm":
            try:  # prime
                warm_fn()
            except (FileNotFoundError, OSError):
                pass
            samples = time_reps(warm_fn, warm_n)
        else:
            samples = time_cold(cold_make, cold_targets, cold_n, cold_sleep)
        after = cp_snap(cp)
        s = summarize(samples)
        delta = cp_delta(before, after)
        # attribution: any sample >= threshold => backend/FUSE fallback touched
        backend_hits = sum(1 for x in samples if x / 1_000_000.0 >= cold_thresh_ms)
        s["backend_hits"] = backend_hits
        s["attribution"] = _attribute(s, delta, backend_hits, cold_thresh_ms)
        s["rpc_delta"] = delta
        res[phase] = s
        if not quiet:
            dstr = ""
            if delta:
                dstr = " rpc[" + ",".join(f"{k}+{v}" for k, v in delta.items()
                                          if k != "_bytes_read" and v) + "]"
            print(f"  [{key}:{phase:4s}] p50={s['p50_us']:>9.1f}us "
                  f"p99={s['p99_us']:>10.1f}us MAX={s['max_us']:>11.1f}us "
                  f"n={s['n']:>3d} {s['attribution']}{dstr}")
    return res

def _attribute(s, delta, backend_hits, thresh_ms):
    # metrics-driven first (authoritative when control plane up)
    if delta is not None:
        if delta.get("READ", 0) > 0:
            return "BACKEND (READ RPCs — bytes crossed)"
        if s["max_us"] >= thresh_ms * 1000:
            return "backend/FUSE (slow sample, mirror-cheap p50)"
        if s["p50_us"] < 200:
            return "mirror/RAM (us-class)"
        return "client/FUSE (sub-ms, no backend READ)"
    # no control plane: latency-only classification
    if backend_hits > 0:
        return f"backend/FUSE ({backend_hits} sample(s) >= {thresh_ms}ms) [no /metrics]"
    if s["p50_us"] < 200:
        return "mirror-or-clientcache (us-class) [no /metrics]"
    return "sub-ms (no /metrics)"

# ----------------------------------------------------------------------------
# spawn-tax measurement (the artifact we subtract by construction)
# ----------------------------------------------------------------------------
def measure_spawn_tax(warm_file, reps=25):
    out = {}
    # in-process os.stat baseline (what the real scenarios use)
    if warm_file:
        try:
            os.stat(warm_file)
        except OSError:
            pass
        ip = time_reps(op_stat(warm_file), reps)
        out["inproc_os_stat_us"] = summarize(ip)["p50_us"]
    # subprocess `stat` (what a shell harness pays PER OP)
    def sub_stat():
        subprocess.run(["stat", "-f", "%z", warm_file], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
    st = time_reps(sub_stat, reps)
    out["subproc_stat_us"] = summarize(st)["p50_us"]
    # bare fork/exec floor: /usr/bin/true
    def sub_true():
        subprocess.run(["true"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    tr = time_reps(sub_true, reps)
    out["subproc_true_us"] = summarize(tr)["p50_us"]
    # perl alarm-wrapper floor (the bounding idiom the harness notes mac needs)
    def sub_perl():
        subprocess.run(["perl", "-e", "1"], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
    pl = time_reps(sub_perl, reps)
    out["subproc_perl_us"] = summarize(pl)["p50_us"]
    if "inproc_os_stat_us" in out:
        out["tax_per_op_us"] = round(out["subproc_stat_us"] - out["inproc_os_stat_us"], 1)
    return out

# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="metadata-latency spread harness")
    ap.add_argument("--root", default="/Volumes/zpool")
    ap.add_argument("--n", type=int, default=200, help="warm reps per scenario")
    ap.add_argument("--cold-n", type=int, default=6, help="cold reps per scenario")
    ap.add_argument("--cold-sleep", type=float, default=7.0,
                    help="seconds to sleep before each cold rep (>5s FUSE TTL)")
    ap.add_argument("--cp", default="http://127.0.0.1:11050")
    ap.add_argument("--cold-threshold-ms", type=float, default=3.0)
    ap.add_argument("--max-visit", type=int, default=60000)
    ap.add_argument("--walk-budget-s", type=float, default=25.0)
    ap.add_argument("--writer-scratch", default=None,
                    help="writable scratch dir enables scenario (i); OFF if unset")
    ap.add_argument("--json", default=None)
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--skip-cold", action="store_true",
                    help="skip cold phases (fast dry-run of the harness)")
    args = ap.parse_args()

    cp = args.cp
    cp_up = cp_snap(cp) is not None
    print("=== metadata-latency SPREAD harness ===")
    print(f"root={args.root}  warm_n={args.n}  cold_n={args.cold_n}  "
          f"cold_sleep={args.cold_sleep}s  cold_thresh={args.cold_threshold_ms}ms")
    # route check (informational)
    try:
        rc = subprocess.run(["route", "get", "192.168.0.197"],
                            capture_output=True, text=True, timeout=4)
        iface = next((l.split(":")[1].strip() for l in rc.stdout.splitlines()
                      if "interface:" in l), "?")
    except Exception:
        iface = "?"
    dfout = ""
    try:
        dfout = subprocess.run(["df", "-h", args.root], capture_output=True,
                               text=True, timeout=6).stdout.splitlines()[-1].split()[0]
    except Exception:
        pass
    print(f"route->NAS iface={iface}   mount_fs={dfout or '?'}   "
          f"control_plane={'UP' if cp_up else 'DOWN'}")
    if not cp_up:
        print("  !! control plane DOWN — attribution falls back to per-sample "
              f">= {args.cold_threshold_ms}ms latency threshold (no RPC deltas).")
    if dfout.startswith("//"):
        print("  !! WARNING: mount looks like SMB (//...), NOT the JuiceMount NFS "
              "loopback. Numbers are SMB-transport, NOT a JuiceMount answer.")

    print(f"\n-- discovering targets under {args.root} (bounded) --")
    disc = discover(args.root, args.max_visit, args.walk_budget_s)
    print(f"   visited={disc['visited']} in {disc['walk_s']}s   "
          f"big_dir={disc['big_dir_n']} kids   files_pool={len(disc['real_files'])}   "
          f"dirs_pool={len(disc['dirs_pool'])}")
    print(f"   big_dir  = {disc['big_dir']}")
    print(f"   symlink  = {disc['symlink']}")
    print(f"   bundle   = {disc['bundle'][0] if disc['bundle'] else None}")
    print(f"   deep_dir = {disc['deep_dir']} (depth {disc['deep_depth']})")

    files = disc["real_files"]
    dirs = disc["dirs_pool"]
    big = disc["big_dir"]
    warm_file = files[0] if files else None
    warm_dir = big
    cold_n = 0 if args.skip_cold else args.cold_n

    print("\n-- spawn-tax (measurement artifact; subtracted by construction) --")
    tax = measure_spawn_tax(warm_file) if warm_file else {}
    for k, v in tax.items():
        print(f"   {k} = {v}")
    if "tax_per_op_us" in tax:
        print(f"   => a shell-per-op harness adds ~{tax['tax_per_op_us']}us PER OP "
              f"(vs in-process os.stat p50 {tax.get('inproc_os_stat_us')}us). "
              f"All scenario numbers below are in-process and pay ZERO of this.")

    print("\n-- scenarios (cold sweep first when enabled, then warm) --")
    results = []

    # (a) negative lookup in a big dir
    results.append(run_scenario(
        "a", "neg-lookup nonexistent name in BIG dir", cp, args.cold_threshold_ms, args.quiet,
        warm_fn=op_neg_lookup(big) if big else None, warm_n=args.n,
        cold_make=(lambda _t: op_neg_lookup(big)) if big else None,
        cold_targets=[big], cold_n=cold_n, cold_sleep=args.cold_sleep,
        absent=None if big else "no directory discovered"))

    # (b) ._<name> AppleDouble probe of a real file
    results.append(run_scenario(
        "b", "._AppleDouble sidecar probe of a real file", cp, args.cold_threshold_ms, args.quiet,
        warm_fn=op_appledouble(warm_file) if warm_file else None, warm_n=args.n,
        cold_make=(lambda t: op_appledouble(t)) if files else None,
        cold_targets=files[:cold_n] or files, cold_n=cold_n, cold_sleep=args.cold_sleep,
        absent=None if warm_file else "no regular file discovered"))

    # (c) stat of a file inside a PACKAGE/bundle
    bundle_inner = disc["bundle"][1] if disc["bundle"] else None
    results.append(run_scenario(
        "c", "stat a file INSIDE a package/bundle", cp, args.cold_threshold_ms, args.quiet,
        warm_fn=op_stat(bundle_inner) if bundle_inner else None, warm_n=args.n,
        cold_make=(lambda _t: op_stat(bundle_inner)) if bundle_inner else None,
        cold_targets=[bundle_inner], cold_n=cold_n, cold_sleep=args.cold_sleep,
        absent=None if bundle_inner else "no .app/.rtfd/.key/.photoslibrary/bundle found under root"))

    # (d) symlink readlink
    sym = disc["symlink"]
    results.append(run_scenario(
        "d", "readlink a symlink", cp, args.cold_threshold_ms, args.quiet,
        warm_fn=op_readlink(sym) if sym else None, warm_n=args.n,
        cold_make=(lambda _t: op_readlink(sym)) if sym else None,
        cold_targets=[sym], cold_n=cold_n, cold_sleep=args.cold_sleep,
        absent=None if sym else "no symlink found under root"))

    # (e) getxattr com.apple.* on a real file
    xattr_absent = None
    if not warm_file:
        xattr_absent = "no regular file discovered"
    elif not _HAVE_MAC_XATTR:
        xattr_absent = "libc getxattr unavailable"
    results.append(run_scenario(
        "e", "getxattr com.apple.* on a real file", cp, args.cold_threshold_ms, args.quiet,
        warm_fn=op_getxattr(warm_file) if not xattr_absent else None, warm_n=args.n,
        cold_make=(lambda t: op_getxattr(t)) if not xattr_absent else None,
        cold_targets=files[:cold_n] or files, cold_n=cold_n, cold_sleep=args.cold_sleep,
        absent=xattr_absent))

    # (f) deep-path stat (10+ levels)
    deep_ok = disc["deep_dir"] and disc["deep_depth"] >= 10
    deep_target = disc["deep_dir"]
    if not deep_ok:
        # fall back to the deepest file we saw
        deep_files = [f for f in files if path_depth_of(args.root, f) >= 10]
        if deep_files:
            deep_target, deep_ok = deep_files[0], True
    results.append(run_scenario(
        "f", f"stat a DEEP path (>=10 levels)", cp, args.cold_threshold_ms, args.quiet,
        warm_fn=op_stat(deep_target) if deep_ok else None, warm_n=args.n,
        cold_make=(lambda _t: op_stat(deep_target)) if deep_ok else None,
        cold_targets=[deep_target], cold_n=cold_n, cold_sleep=args.cold_sleep,
        absent=None if deep_ok else f"no path >=10 levels deep (deepest={disc['deep_depth']})"))

    # (g) readdir of a HUGE flat dir  (names-only = READDIR ; +stat = READDIRPLUS)
    results.append(run_scenario(
        "g1", f"READDIR (names) of biggest flat dir ({disc['big_dir_n']} kids)",
        cp, args.cold_threshold_ms, args.quiet,
        warm_fn=op_readdir_names(big) if big else None, warm_n=min(args.n, 40),
        cold_make=(lambda _t: op_readdir_names(big)) if big else None,
        cold_targets=[big], cold_n=cold_n, cold_sleep=args.cold_sleep,
        absent=None if big else "no directory discovered"))
    results.append(run_scenario(
        "g2", f"READDIRPLUS (names+stat) of biggest flat dir",
        cp, args.cold_threshold_ms, args.quiet,
        warm_fn=op_readdirplus(big) if big else None, warm_n=min(args.n, 20),
        cold_make=(lambda _t: op_readdirplus(big)) if big else None,
        cold_targets=[big], cold_n=cold_n, cold_sleep=args.cold_sleep,
        absent=None if big else "no directory discovered"))

    # (h) unmirrored / never-browsed dir stat (cold-populate) — cold ONLY is meaningful
    #     rotate over distinct dirs so each is a genuine first-touch scandir.
    results.append(run_scenario(
        "h", "cold-populate a never-browsed dir (scandir)", cp, args.cold_threshold_ms, args.quiet,
        warm_fn=None, warm_n=0,
        cold_make=(lambda t: op_readdirplus(t)) if dirs else None,
        cold_targets=(dirs[-cold_n:] if dirs else []), cold_n=cold_n, cold_sleep=args.cold_sleep,
        absent=None if dirs else "no directory pool discovered"))

    # (i) stat during a concurrent light writer — OFF unless a scratch dir is given
    if args.writer_scratch and os.path.isdir(args.writer_scratch) and warm_file:
        results.append(_run_with_writer(args.writer_scratch, warm_file, cp,
                                        args.cold_threshold_ms, args.n, args.quiet))
    else:
        results.append(run_scenario(
            "i", "stat during a concurrent light writer", cp, args.cold_threshold_ms, args.quiet,
            absent="OFF (read-only): pass --writer-scratch <writable dir> to enable"))

    # (j) plain warm stat baseline (the fast floor)
    results.append(run_scenario(
        "j", "plain WARM stat baseline (fast floor)", cp, args.cold_threshold_ms, args.quiet,
        warm_fn=op_stat(warm_file) if warm_file else None, warm_n=args.n,
        cold_make=None, cold_targets=None, cold_n=0,
        absent=None if warm_file else "no regular file discovered"))

    # ---- ranked outlier table ----
    print("\n=== RANKED metadata OUTLIERS (widest-spread / slowest first) ===")
    print(f"  {'scenario':40s} {'phase':5s} {'p50_us':>10s} {'p99_us':>11s} "
          f"{'MAX_us':>12s}  attribution")
    rankable = []
    for r in results:
        for phase in ("cold", "warm"):
            if phase in r:
                rankable.append((r["key"], r["label"], phase, r[phase]))
    rankable.sort(key=lambda x: x[3]["max_us"], reverse=True)
    for key, label, phase, s in rankable:
        print(f"  {(key+' '+label)[:40]:40s} {phase:5s} {s['p50_us']:>10.1f} "
              f"{s['p99_us']:>11.1f} {s['max_us']:>12.1f}  {s['attribution']}")

    absent = [r for r in results if r.get("absent")]
    if absent:
        print("\n  absent scenarios:")
        for r in absent:
            print(f"    [{r['key']}] {r['label']}: {r['absent']}")

    out = {"root": args.root, "iface": iface, "mount_fs": dfout,
           "control_plane_up": cp_up, "discovery": {k: disc[k] for k in
           ("big_dir", "big_dir_n", "symlink", "deep_dir", "deep_depth",
            "visited", "walk_s")},
           "spawn_tax": tax, "scenarios": results}
    if args.json:
        with open(args.json, "w") as f:
            json.dump(out, f, indent=2, default=str)
        print(f"\n  wrote {args.json}")

def _run_with_writer(scratch, warm_file, cp, thresh, n, quiet):
    """Scenario (i): time warm stat while a light background writer churns a
    small temp file in the caller-provided scratch dir. Never touches user data."""
    import threading
    stop = threading.Event()
    scratch_file = os.path.join(scratch, f".latspread_writer_{os.getpid()}.tmp")
    def churn():
        buf = b"x" * 4096
        while not stop.is_set():
            try:
                with open(scratch_file, "wb") as fh:
                    fh.write(buf)
                    fh.flush()
                    os.fsync(fh.fileno())
            except OSError:
                break
            time.sleep(0.05)
    t = threading.Thread(target=churn, daemon=True)
    before = cp_snap(cp)
    t.start()
    time.sleep(0.3)
    try:
        os.stat(warm_file)
    except OSError:
        pass
    samples = time_reps(op_stat(warm_file), n)
    stop.set()
    t.join(timeout=2)
    try:
        os.unlink(scratch_file)
    except OSError:
        pass
    after = cp_snap(cp)
    s = summarize(samples)
    delta = cp_delta(before, after)
    backend_hits = sum(1 for x in samples if x / 1_000_000.0 >= thresh)
    s["backend_hits"] = backend_hits
    s["attribution"] = _attribute(s, delta, backend_hits, thresh) + " (under writer)"
    s["rpc_delta"] = delta
    if not quiet:
        print(f"  [i:warm] p50={s['p50_us']:.1f}us p99={s['p99_us']:.1f}us "
              f"MAX={s['max_us']:.1f}us n={s['n']} {s['attribution']}")
    return {"key": "i", "label": "stat during concurrent light writer", "warm": s}

if __name__ == "__main__":
    main()
