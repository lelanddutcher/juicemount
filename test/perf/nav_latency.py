#!/usr/bin/env python3
"""nav_latency.py — precise per-op nav latency for JuiceMount's NFS mount.

Times individual os.lstat() calls IN-PROCESS (no per-op subprocess spawn, so
the number is the real NFS-round-trip-to-RAM-mirror latency, not shell tax) and
reports percentiles. Replays Finder's fan-out: for each real file it lstats the
entry and its ._<name> AppleDouble sidecar; once per dir it lstats .DS_Store and
does an os.scandir (readdir). READ-ONLY, metadata-only — never opens/reads bytes.

Two passes (cold, then warm) over the SAME entry set so cold-vs-warm isolates
cache leverage. Prints a per-op histogram bucketed against the backend RTT.

Usage: nav_latency.py <subtree> [rtt_ms]
"""
import os, sys, time

sub = sys.argv[1] if len(sys.argv) > 1 else "/Volumes/zpool/Film Projects/College Sports Co"
rtt = float(sys.argv[2]) if len(sys.argv) > 2 else 44.0

# Enumerate once (this walk is itself mirror-served).
files, dirs = [], []
for root, dnames, fnames in os.walk(sub):
    dirs.append(root)
    for fn in fnames:
        if fn.startswith("._"):
            continue
        files.append(os.path.join(root, fn))
print(f"subtree={sub}\n  dirs={len(dirs)} files={len(files)}  backend_rtt≈{rtt:.1f}ms")

def walk(label):
    lat = []
    t0 = time.perf_counter()
    for d in dirs:
        s = time.perf_counter()
        try: list(os.scandir(d))
        except OSError: pass
        lat.append((time.perf_counter()-s)*1000)
        s = time.perf_counter()
        try: os.lstat(os.path.join(d, ".DS_Store"))
        except OSError: pass
        lat.append((time.perf_counter()-s)*1000)
    for f in files:
        s = time.perf_counter()
        try: os.lstat(f)
        except OSError: pass
        lat.append((time.perf_counter()-s)*1000)
        d, b = os.path.split(f)
        s = time.perf_counter()
        try: os.lstat(os.path.join(d, "._"+b))
        except OSError: pass
        lat.append((time.perf_counter()-s)*1000)
    wall = time.perf_counter()-t0
    lat.sort()
    n = len(lat)
    def pct(p): return lat[min(n-1, int(round(p/100*(n-1))))] if n else 0.0
    mean = sum(lat)/n if n else 0
    print(f"\n[{label}] {n} ops in {wall:.3f}s  ({n/wall:.0f} ops/sec)")
    print(f"  per-op ms: p50={pct(50):.3f}  p90={pct(90):.3f}  p95={pct(95):.3f} "
          f"p99={pct(99):.3f}  max={lat[-1]:.2f}  mean={mean:.3f}")
    # how many ops were at/above the backend RTT (i.e. plausibly leaked)?
    ge_rtt = sum(1 for x in lat if x >= rtt*0.5)
    print(f"  ops ≥ 0.5×RTT ({rtt*0.5:.1f}ms): {ge_rtt} ({100*ge_rtt/n:.2f}%)  "
          f"← candidates that may have touched the backend")
    return pct(50), pct(99), n/wall

print("\n=== COLD PASS ===");  c50,c99,cops = walk("COLD")
print("\n=== WARM PASS ==="); w50,w99,wops = walk("WARM")
print("\n=== VERDICT ===")
print(f"  warm p50={w50:.3f}ms vs backend RTT {rtt:.1f}ms  → {rtt/w50:.0f}x faster than one backend round-trip" if w50>0 else "")
print(f"  cold p50={c50:.3f}ms / warm p50={w50:.3f}ms  ratio={c50/w50:.2f}x" if w50>0 else "")
print(f"  throughput cold={cops:.0f} warm={wops:.0f} ops/sec")
