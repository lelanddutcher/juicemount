#!/bin/bash
# l2_roundtrips.sh — the crux L2 measurement: how many BACKEND meta round-trips
# does one COLD directory readdir cost on the fuse-internal (juicefs->Redis)
# path, and is the cold wall-time ≈ round-trips × RTT (i.e. serial + RTT-bound)?
#
# Uses a set of DISTINCT dirs (each touched at most once) so every readdir is a
# genuine cold backend fetch, not a cache hit. Captures .accesslog per dir and
# counts lookup/getattr/readdir/open/statfs plus their summed latency.
#
# This isolates whether the FIX is BATCHING the backend metadata fetch: if
# cold_wall ≈ N_roundtrips × RTT and the ops are serial, batching wins big.
#
# Run when reconcile is IDLE (pass the guard) for clean numbers.
# READ-ONLY, metadata-only. Usage: l2_roundtrips.sh <dir1> [dir2 ...]
set -u
FUSE="${JM_FUSE_INTERNAL:-$HOME/.juicemount/fuse-internal}"
BACKEND_HOST="${JM_BACKEND_HOST:-192.168.0.197}"
bcmd(){ local s="$1"; shift; perl -e 'alarm shift; exec @ARGV' "$s" "$@"; }
now(){ python3 -c 'import time;print("%.6f"%time.time())'; }

# quick RTT
RTT=$(ping -c 6 -i 0.2 "$BACKEND_HOST" 2>/dev/null | tail -1 | sed -n 's#.*/\([0-9.]*\)/[0-9.]*/[0-9.]*.*#\1#p')
[ -z "$RTT" ] && RTT=52
echo "=== L2 round-trips-per-cold-dir (RTT≈${RTT}ms) ==="
echo "reconcile guard: $(perl -e 'alarm 6; exec @ARGV' curl -s http://127.0.0.1:11050/activity 2>/dev/null | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("summary"))' 2>/dev/null)"
echo

for TARGET in "$@"; do
  REL="${TARGET#/Volumes/zpool/}"
  FT="$FUSE/$REL"
  [ -d "$FT" ] || { echo "skip (not a dir): $FT"; continue; }
  AL="/tmp/jm_l2_$$_$RANDOM.log"
  # cool > TTL
  sleep 7
  ( bcmd 20 cat "$FUSE/.accesslog" > "$AL" 2>/dev/null ) & ALPID=$!
  sleep 0.4
  t0=$(now); bcmd 25 /bin/ls -1f "$FT" >/dev/null 2>&1; t1=$(now)
  sleep 0.5; kill "$ALPID" 2>/dev/null; wait "$ALPID" 2>/dev/null
  COLD=$(python3 -c "print('%.1f'%(($t1-$t0)*1000))")
  python3 - "$AL" "$t0" "$t1" "$RTT" "$COLD" "$REL" <<'PY'
import sys,re
al,t0,t1,rtt,cold,rel=sys.argv[1],float(sys.argv[2]),float(sys.argv[3]),float(sys.argv[4]),float(sys.argv[5]),sys.argv[6]
pat=re.compile(r'\[[^\]]*\] (\w+) .*<([0-9.]+)>')
ops={}; be=0; besum=0.0; durs=[]
for ln in open(al,errors='replace').read().splitlines():
    m=pat.search(ln)
    if not m: continue
    op=m.group(1); d=float(m.group(2))*1000
    ops[op]=ops.get(op,0)+1; durs.append(d)
    if d>=5: be+=1; besum+=d
tot=sum(ops.values())
print("  DIR %s"%rel)
print("    cold readdir wall: %.1f ms"%cold)
print("    total meta ops    : %d   (%s)"%(tot, ", ".join("%s=%d"%(k,v) for k,v in sorted(ops.items()))))
print("    backend RTs (>=5ms): %d  sum=%.0f ms  mean=%.0f ms"%(be,besum,besum/max(be,1)))
print("    predicted if serial×RTT: %d × %.0f = %.0f ms   vs actual %.1f ms  -> %s"%(
        be,rtt,be*rtt,cold, "RTT-BOUND/SERIAL" if abs(be*rtt-cold)<cold*0.6 else "not purely serial"))
PY
  rm -f "$AL"
  echo
done
echo "Interpretation: if backend round-trips is large (tens) and cold_wall ≈ RTs×RTT,"
echo "the cold fuse path is serial + RTT-bound -> the v0.4 fix is to BATCH the backend"
echo "metadata fetch (one multi-key round-trip instead of N). The NFS mirror already"
echo "sidesteps this by serving from RAM; this quantifies the penalty the mirror avoids."
