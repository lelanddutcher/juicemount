#!/bin/bash
# cellular_nav_bench.sh — Finder-representative NFS tree-walk benchmark for
# JuiceMount, purpose-built to measure whether file-tree NAVIGATION is served
# from the LOCAL SQLite/RAM metadata mirror (fast, backend-independent) or
# LEAKS to the slow backend (Redis/MinIO over the tunnel/cellular link).
#
# WHY THIS EXISTS (task #90, v0.4): the product promise is that Finder nav stays
# fast on cellular because it is served from the local mirror, not the slow NAS.
# That was never properly tested — only ever on 10GbE where nav is trivially
# RAM-fast. This orchestrates three measurements against a chosen cached subtree:
#
#   1. Network condition       : egress interface + backend ICMP RTT (the
#                                latency the mirror must hide).
#   2. nav_latency.py          : Finder fan-out (per-file entry + ._sidecar +
#                                per-dir .DS_Store + readdir), COLD then WARM,
#                                timed IN-PROCESS (no per-op subprocess tax),
#                                with per-op p50/p99 vs the RTT.
#   3. server /metrics delta   : the server's own per-RPC LOOKUP/GETATTR/READDIR*
#                                percentiles across the walk — cross-checks the
#                                client-side timing against the server's view.
#
# Companion scripts (run them for the crux evidence):
#   - server_lookup_latency.sh : forces FRESH server LOOKUPs (defeats the macOS
#                                NFS client attr cache) → the server's true
#                                per-LOOKUP serve latency, immune to caching.
#   - backend_leak_probe.sh    : A/B/C — warm mirror stat vs cold fuse-internal
#                                backend stat vs raw RTT. The direct RTT-hiding
#                                proof.
#
# READ-ONLY, metadata-only — never reads file bytes, never writes/deletes.
#
# Usage: cellular_nav_bench.sh [SUBTREE] [CONTROL_PLANE_URL]
set -u
SUBTREE="${1:-/Volumes/zpool/Film Projects/College Sports Co}"
CP="${2:-http://127.0.0.1:11050}"
BACKEND_HOST="${JM_BACKEND_HOST:-192.168.0.197}"
HERE="$(cd "$(dirname "$0")" && pwd)"
OUTDIR="${JM_BENCH_OUTDIR:-${TMPDIR:-/tmp}/jm_cellular_nav_bench}"
mkdir -p "$OUTDIR"; STAMP="$(date +%Y%m%d_%H%M%S)"

echo "==================================================================="
echo " JuiceMount cellular nav-leverage benchmark   ($STAMP)"
echo " subtree : $SUBTREE"
echo " control : $CP    backend: $BACKEND_HOST"
echo "==================================================================="

echo; echo "--- mount identity (must be 127.0.0.1:/ NFS, not local disk) ---"
df "$SUBTREE" 2>/dev/null | tail -2

echo; echo "--- 1. NETWORK CONDITION ---"
echo "egress to backend:"; route get "$BACKEND_HOST" 2>/dev/null | grep -E "interface:"
echo "default route    :"; route get default 2>/dev/null | grep -E "interface:"
echo "app net-profile class (from log):"
grep -oE '"class":"[a-z]+"' ~/Library/Logs/JuiceMount/juicemount.log 2>/dev/null | tail -1
echo "backend ICMP RTT :"
RTTLINE=$(ping -c 8 -i 0.3 "$BACKEND_HOST" 2>/dev/null | tail -1)
echo "  $RTTLINE"
RTT_MS=$(echo "$RTTLINE" | sed -n 's#.*= [0-9.]*/\([0-9.]*\)/.*#\1#p'); [ -z "$RTT_MS" ] && RTT_MS=44

echo; echo "--- /activity BEFORE (watch for reconcile 'Rebuilding index') ---"
curl -s --max-time 5 "$CP/activity" 2>/dev/null; echo

# server metrics BEFORE
curl -s --max-time 5 "$CP/metrics" > "$OUTDIR/metrics_before_$STAMP.json" 2>/dev/null

echo; echo "--- 2. FINDER FAN-OUT WALK (in-process, cold then warm; RTT=${RTT_MS}ms) ---"
python3 "$HERE/nav_latency.py" "$SUBTREE" "$RTT_MS"

# server metrics AFTER
curl -s --max-time 5 "$CP/metrics" > "$OUTDIR/metrics_after_$STAMP.json" 2>/dev/null

echo; echo "--- /activity AFTER ---"
curl -s --max-time 5 "$CP/activity" 2>/dev/null; echo

echo; echo "--- 3. SERVER-SIDE per-RPC DELTA (cross-check vs client timing) ---"
python3 - "$OUTDIR/metrics_before_$STAMP.json" "$OUTDIR/metrics_after_$STAMP.json" <<'PY'
import json,sys
def L(p):
    try: return json.load(open(p))
    except Exception: return {}
a,b=L(sys.argv[1]),L(sys.argv[2]); ra,rb=a.get("rpcs",{}),b.get("rpcs",{})
delta=b.get("rpc_total",0)-a.get("rpc_total",0)
# VALIDITY GATE — see metrics_delta.py. A per-RPC table printed from a window
# in which the server did nothing is a fabricated measurement, not a fast one.
if delta<=0:
    print("  INVALID: rpc_total did not move (%s -> %s) — the server did no work"
          %(a.get("rpc_total",0),b.get("rpc_total",0)))
    print("  The client almost certainly served this from its own attr cache.")
    print("  Re-run via server_lookup_latency.sh to force genuine server LOOKUPs.")
    sys.exit(3)
print("  rpc_total: %s -> %s (delta %s)"%(a.get("rpc_total",0),b.get("rpc_total",0),delta))
print("  %-12s %8s %8s   %10s %10s"%("RPC","cnt_bef","cnt_aft","p50_us_aft","p99_us_aft"))
for k in ["LOOKUP","GETATTR","ACCESS","READDIR","READDIRPLUS","FSSTAT"]:
    x,y=ra.get(k,{}),rb.get(k,{})
    print("  %-12s %8d %8d   %10.1f %10.1f"%(k,x.get("count",0),y.get("count",0),
          y.get("p50_us",0),y.get("p99_us",0)))
print("\n  NOTE: the macOS NFS client attr-cache absorbs repeat GETATTRs on a")
print("  subtree you have already walked, so the client-visible RPC delta here")
print("  UNDER-counts server work. Use server_lookup_latency.sh (fresh unique")
print("  names) to force — and measure — genuine server LOOKUPs.")
PY

echo; echo "Done. Raw metrics snapshots under $OUTDIR (stamp $STAMP)."
echo "Run companion crux probes:"
echo "  $HERE/server_lookup_latency.sh \"$SUBTREE\""
echo "  $HERE/backend_leak_probe.sh    \"$SUBTREE\""
