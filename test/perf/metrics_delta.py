#!/usr/bin/env python3
"""metrics_delta.py — snapshot the JuiceMount NFS server /metrics before an
action and diff after, reporting per-RPC count deltas plus the current
percentiles. Used to see how many of Finder's ops hit the mirror (fast p50 in
µs) vs fell back to a slow backend path (p99 in the hundreds of ms).

Two modes:
  metrics_delta.py snap                     -> prints a JSON snapshot to stdout
  metrics_delta.py diff <before.json> <after.json>  -> prints the delta table

READ-ONLY (control-plane HTTP GET only).
"""
import sys, json, urllib.request

CP = "http://127.0.0.1:11050"
RPCS = ["LOOKUP","GETATTR","ACCESS","READDIR","READDIRPLUS","READ","FSSTAT","OPEN","CREATE","WRITE"]

def snap():
    with urllib.request.urlopen(CP + "/metrics", timeout=8) as r:
        return json.load(r)

if len(sys.argv) >= 2 and sys.argv[1] == "snap":
    json.dump(snap(), sys.stdout)
    sys.exit(0)

if len(sys.argv) >= 4 and sys.argv[1] == "diff":
    a = json.load(open(sys.argv[2]))
    b = json.load(open(sys.argv[3]))
    ra, rb = a.get("rpcs", {}), b.get("rpcs", {})
    print("  rpc_total: %d -> %d  (delta %d)" % (
        a.get("rpc_total", 0), b.get("rpc_total", 0),
        b.get("rpc_total", 0) - a.get("rpc_total", 0)))
    print("  bytes_read delta: %d" % (b.get("bytes_read",0) - a.get("bytes_read",0)))
    print("  %-12s %8s %8s   %10s %10s %10s" % ("RPC","d_count","cnt_aft","p50_us","p95_us","p99_us"))
    for k in RPCS:
        x, y = ra.get(k, {}), rb.get(k, {})
        d = y.get("count", 0) - x.get("count", 0)
        if d == 0 and y.get("count",0) == 0:
            continue
        print("  %-12s %8d %8d   %10.1f %10.1f %10.1f" % (
            k, d, y.get("count", 0), y.get("p50_us", 0), y.get("p95_us", 0), y.get("p99_us", 0)))
    sys.exit(0)

print(__doc__)
sys.exit(2)
