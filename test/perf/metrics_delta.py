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
    rpc_delta = b.get("rpc_total", 0) - a.get("rpc_total", 0)

    # VALIDITY GATE — refuse to report a latency table from no data.
    #
    # This harness is from the night five invalid measurements were produced in
    # a row, one of them a "listing benchmark" the macOS NFS client served
    # entirely from its own attr cache: rpc_total never moved, the server did
    # nothing, and the number looked superb. Without this gate the table below
    # still prints p50/p95/p99 for every op, and a stale reading is
    # indistinguishable from a fast one.
    if rpc_delta <= 0:
        print("  INVALID: rpc_total did not move (%d -> %d). The server did no "
              "work in this window, so there is nothing to report." % (
              a.get("rpc_total", 0), b.get("rpc_total", 0)))
        print("  Most likely the macOS NFS client served the operation from its "
              "own attr cache. Use server_lookup_latency.sh (fresh unique names) "
              "to force genuine server LOOKUPs, or remount to drop the client cache.")
        sys.exit(3)

    print("  rpc_total: %d -> %d  (delta %d)" % (
        a.get("rpc_total", 0), b.get("rpc_total", 0), rpc_delta))
    print("  bytes_read delta: %d" % (b.get("bytes_read",0) - a.get("bytes_read",0)))

    # Backend vs cache — the only place you learn whether bytes crossed the
    # link. Absent means the juicefs daemon scrape failed; it is reported as
    # absent rather than as zero, because "0 bytes from the backend" reads as a
    # perfect cache-hit session.
    ba, bb = a.get("backend"), b.get("backend")
    if not ba or not bb:
        print("  backend: UNAVAILABLE (juicefs daemon not scraped) — cannot say "
              "whether reads came from the local cache or over the link")
    else:
        hits = bb.get("cache_hits",0) - ba.get("cache_hits",0)
        miss = bb.get("cache_miss",0) - ba.get("cache_miss",0)
        got  = bb.get("object_get_bytes",0) - ba.get("object_get_bytes",0)
        mb   = bb.get("cache_miss_bytes",0) - ba.get("cache_miss_bytes",0)
        meta = bb.get("meta_ops",0) - ba.get("meta_ops",0)
        if hits + miss == 0:
            print("  backend: no block reads in this window (metadata-only "
                  "operation, or served above the block layer)")
        else:
            print("  backend: %d block hits / %d miss = %.1f%% hit; "
                  "%.1f MB pulled from the object store for %.1f MB missed"
                  " (amplification %.2fx); %d meta ops (Redis round trips)" % (
                  hits, miss, 100.0*hits/(hits+miss),
                  got/1e6, mb/1e6, (got/mb if mb else 0.0), meta))
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
