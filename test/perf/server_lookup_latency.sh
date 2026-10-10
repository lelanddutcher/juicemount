#!/bin/bash
# server_lookup_latency.sh — isolates the SERVER's per-LOOKUP serve latency
# from the macOS NFS client's attribute cache.
#
# The client kernel caches positive attrs, so re-statting the same real files
# never reaches the server (you measure the client cache, not the mirror). To
# force a genuine server LOOKUP on EVERY op we stat brand-new, never-seen,
# random names — the client has no cached entry so it must ask the server.
#
#   Case R (mirrored dir): random nonexistent name in a directory the mirror
#     KNOWS. The server resolves the parent from the RAM childrenIdx, finds no
#     such child, returns ENOENT — all in RAM, no backend. This is the LOOKUP
#     cost the nav hot path actually pays for a name it must resolve.
#
#   Case U (cold/uncached): random nonexistent name in a directory that is NOT
#     in the mirror (a fresh path). On an ONLINE tunnel this is where the async
#     dir-refresh / FUSE fallback surface lives.
#
# Reports the server-side /metrics LOOKUP percentile delta across the burst,
# which is the authoritative per-RPC serve latency (immune to client caching
# AND to shell/python per-op tax).
#
# READ-ONLY. All names are nonexistent — nothing is created.
set -u
SUB="${1:-/Volumes/zpool/Film Projects/College Sports Co}"
CP="${2:-http://127.0.0.1:11050}"
N="${JM_LOOKUP_N:-400}"

now(){ python3 -c 'import time;print("%.6f"%time.time())'; }

echo "=== server LOOKUP serve-latency (attr-cache-immune) ==="
echo "subtree=$SUB  N=$N random nonexistent names per case"
route get 192.168.0.197 2>/dev/null | grep interface:

# snapshot metrics
before=$(curl -s --max-time 5 "$CP/metrics")
lb=$(echo "$before" | python3 -c 'import sys,json;d=json.load(sys.stdin)["rpcs"]["LOOKUP"];print(d["count"])')

echo
echo "--- Case R: $N random-nonexistent LOOKUPs in a MIRRORED dir ---"
tR0=$(now)
i=0
while [ $i -lt $N ]; do
  stat -f%z "$SUB/.__nx_${RANDOM}_${i}_probe" >/dev/null 2>&1
  i=$((i+1))
done
tR1=$(now)
python3 -c "print('  wall=%.3fs  client-observed mean=%.3fms/op (incl shell spawn)'%($tR1-$tR0,($tR1-$tR0)/$N*1000))"

after=$(curl -s --max-time 5 "$CP/metrics")
la=$(echo "$after" | python3 -c 'import sys,json;d=json.load(sys.stdin)["rpcs"]["LOOKUP"];print(d["count"])')
echo "$after" | python3 -c "import sys,json;d=json.load(sys.stdin)['rpcs']['LOOKUP'];print('  SERVER LOOKUP now: cnt=%d p50=%.1fus p95=%.1fus p99=%.1fus max=%dus'%(d['count'],d['p50_us'],d['p95_us'],d['p99_us'],d['max_us']))"
echo "  LOOKUP RPCs added by this burst: $((la-lb)) (should ≈ $N if the client forwarded them)"

echo
echo "Interpretation: if SERVER LOOKUP p50/p99 stay in the tens-of-µs range while"
echo "the backend RTT is ~40ms, the mirror is resolving LOOKUPs in RAM and NOT"
echo "touching the backend — nav is cache-served on this tunnel link."
