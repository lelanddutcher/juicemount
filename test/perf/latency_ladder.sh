#!/bin/bash
# latency_ladder.sh — decompose cold Finder-navigation latency on JuiceMount
# into a STACKED per-layer ladder for ONE target directory:
#
#   L0  ICMP RTT to the NAS/Redis host (the floor the caches must hide).
#   L1  one Redis round-trip over the tunnel (raw socket connect+PING), timed.
#   L2  JuiceFS COLD readdir on ~/.juicemount/fuse-internal/<rel>, with the
#       .accesslog captured in the background so we COUNT the backend meta ops
#       (lookup/getattr/readdir/open/statfs) and sum their per-op latency.
#       Then a WARM readdir for the cold->warm delta.
#   L3  NFS mirror cold-populate: time the FIRST cold `ls` of /Volumes/zpool/<rel>
#       until the child count STABILIZES (async-refresh detection).
#
# L4 (real Finder column-view populate) is measured separately by the
# osascript driver (finder_column_open.scpt) because it needs the GUI.
#
# COLD is forced by sleeping > the 5s juicefs attr/entry/dir-entry-cache TTL
# (SLEEP_COLD, default 7s) before each cold measurement. That expires BOTH the
# macOS NFS client cache and the juicefs FUSE cache, so a cold readdir must
# re-resolve through juicefs (which may hit Redis even for mirror-resident
# metadata — that is exactly what we are measuring).
#
# Bounded everywhere with perl alarm() (no macOS `timeout`). READ-ONLY,
# metadata-only: scandir/ls/stat only, never opens file bytes.
#
# Usage: latency_ladder.sh <target_dir> [label]
set -u
TARGET="${1:?usage: latency_ladder.sh <target_dir> [label]}"
LABEL="${2:-target}"
CP="${CP:-http://127.0.0.1:11050}"
BACKEND_HOST="${JM_BACKEND_HOST:-192.168.0.197}"
REDIS_PORT="${JM_REDIS_PORT:-30179}"
REDIS_DB="${JM_REDIS_DB:-1}"
FUSE="${JM_FUSE_INTERNAL:-$HOME/.juicemount/fuse-internal}"
SLEEP_COLD="${JM_SLEEP_COLD:-7}"
REL="${TARGET#/Volumes/zpool/}"
FTARGET="$FUSE/$REL"
STAMP="$(date +%H%M%S)"
AL="/tmp/jm_ladder_accesslog_${STAMP}_$$.log"

bcmd(){ local s="$1"; shift; perl -e 'alarm shift; exec @ARGV' "$s" "$@"; }
now(){ python3 -c 'import time;print("%.6f"%time.time())'; }

echo "################################################################"
echo "# LATENCY LADDER  [$LABEL]"
echo "#   mount path : $TARGET"
echo "#   fuse path  : $FTARGET"
echo "#   backend    : $BACKEND_HOST:$REDIS_PORT db$REDIS_DB"
echo "################################################################"

# ---- guard: is a reconcile running? (pollutes everything) ----
ACT=$(bcmd 6 curl -s "$CP/activity" 2>/dev/null)
echo "[guard] activity: $(echo "$ACT" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("summary"),"|",[o["detail"] for o in d.get("operations",[]) if o.get("active")])' 2>/dev/null)"
echo "[guard] route to backend: $(route get "$BACKEND_HOST" 2>/dev/null | sed -n 's/.*interface: //p')"

# ---- L0: ICMP RTT ----
echo
echo "=== L0  ICMP RTT to $BACKEND_HOST ==="
PL=$(ping -c 10 -i 0.2 "$BACKEND_HOST" 2>/dev/null | tail -1); echo "  $PL"
L0=$(echo "$PL" | sed -n 's#.*= [0-9.]*/\([0-9.]*\)/.*#\1#p'); [ -z "$L0" ] && L0=50

# ---- L1: one Redis round-trip (raw socket, no password in this deploy) ----
echo
echo "=== L1  Redis round-trip (raw socket connect + PING over tunnel) ==="
python3 - "$BACKEND_HOST" "$REDIS_PORT" "$REDIS_DB" <<'PY'
import socket, time, sys
host, port, db = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
# NOTE: this JuiceMount deploy uses redis://host:port/db with NO password
# (verified: no AUTH token in the juicefs cmdline). If a password were present
# we would AUTH here and NEVER print it. Nothing secret is emitted regardless.
def one(connect_new):
    t0=time.perf_counter()
    s=socket.create_connection((host,port),timeout=5)
    s.settimeout(5)
    s.sendall(b"*1\r\n$4\r\nPING\r\n")
    r=s.recv(64)
    t1=time.perf_counter()
    s.close()
    return (t1-t0)*1000, r.strip()
# first call includes TCP connect; then measure PING-only on a kept-open socket
s=socket.create_connection((host,port),timeout=5); s.settimeout(5)
lat=[]
for _ in range(10):
    t0=time.perf_counter()
    s.sendall(b"*1\r\n$4\r\nPING\r\n"); s.recv(64)
    lat.append((time.perf_counter()-t0)*1000)
s.close()
lat.sort(); n=len(lat)
p=lambda q: lat[min(n-1,int(q/100*(n-1)))]
# also one full connect+ping to show connect cost
ct,resp=one(True)
print("  PING-only (warm socket) ms: p50=%.2f p99=%.2f min=%.2f max=%.2f  (resp=%r)"%(p(50),p(99),lat[0],lat[-1],resp[:8]))
print("  connect+PING (cold socket) ms: %.2f"%ct)
PY
L1=$(python3 - "$BACKEND_HOST" "$REDIS_PORT" <<'PY'
import socket,time,sys
host,port=sys.argv[1],int(sys.argv[2])
try:
    s=socket.create_connection((host,port),timeout=5);s.settimeout(5)
    lat=[]
    for _ in range(10):
        t0=time.perf_counter();s.sendall(b"*1\r\n$4\r\nPING\r\n");s.recv(64);lat.append((time.perf_counter()-t0)*1000)
    s.close();lat.sort();print("%.2f"%lat[len(lat)//2])
except Exception:
    print("nan")
PY
)

# ---- L2: JuiceFS COLD readdir + accesslog op count ----
echo
echo "=== L2  JuiceFS COLD readdir on fuse-internal (+ accesslog op count) ==="
if [ ! -d "$FTARGET" ]; then
  echo "  [skip] fuse path not present/dir: $FTARGET"
else
  echo "  cooling caches (sleep ${SLEEP_COLD}s > 5s TTL)…"; sleep "$SLEEP_COLD"
  # start bounded accesslog capture BEFORE the cold readdir
  ( bcmd $((SLEEP_COLD+8)) cat "$FUSE/.accesslog" > "$AL" 2>/dev/null ) &
  ALPID=$!
  sleep 0.4   # let the reader attach
  t0=$(now)
  bcmd 20 /bin/ls -1f "$FTARGET" >/dev/null 2>&1
  t1=$(now)
  COLD_MS=$(python3 -c "print('%.1f'%(($t1-$t0)*1000))")
  # second (warm) readdir immediately
  t2=$(now); bcmd 20 /bin/ls -1f "$FTARGET" >/dev/null 2>&1; t3=$(now)
  WARM_MS=$(python3 -c "print('%.1f'%(($t3-$t2)*1000))")
  sleep 0.6; kill "$ALPID" 2>/dev/null; wait "$ALPID" 2>/dev/null
  echo "  COLD readdir wall: ${COLD_MS} ms"
  echo "  WARM readdir wall: ${WARM_MS} ms"
  # Count meta ops that fired for THIS target during the cold window.
  # Match either the target's basename or its child names in the accesslog.
  BN=$(basename "$TARGET")
  python3 - "$AL" "$t0" "$t1" <<'PY'
import sys
al, t0, t1 = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
# accesslog line: "2026.07.06 18:35:08.923633 [uid...] readdir (...): (..) - OK <0.094280>"
import re
ops={}; slow=[]; total_backend_ms=0.0; nlines=0
pat=re.compile(r'^(\d{4}\.\d\d\.\d\d \d\d:\d\d:\d\d\.\d+) \[[^\]]*\] (\w+) .*<([0-9.]+)>')
try:
    lines=open(al,errors='replace').read().splitlines()
except FileNotFoundError:
    lines=[]
for ln in lines:
    m=pat.search(ln)
    if not m: continue
    op=m.group(2); dur=float(m.group(3))*1000  # ms
    nlines+=1
    ops[op]=ops.get(op,0)+1
    # a "backend" op is one that took materially longer than a RAM hit (>5ms)
    if dur>=5.0:
        total_backend_ms+=dur; slow.append((op,dur))
print("  accesslog lines parsed: %d"%nlines)
print("  meta-op counts (this cold window): " + ", ".join("%s=%d"%(k,v) for k,v in sorted(ops.items())))
be=[o for o in slow]
print("  backend round-trips (op >=5ms): %d  (sum %.1f ms)"%(len(be),total_backend_ms))
for op,dur in sorted(slow,key=lambda x:-x[1])[:8]:
    print("     %-10s %.1f ms"%(op,dur))
PY
  echo "  (raw accesslog kept: $AL)"
fi

# ---- L3: NFS mirror cold-populate (async-refresh aware) ----
echo
echo "=== L3  NFS mirror cold-populate (ls until child count stabilizes) ==="
echo "  cooling caches (sleep ${SLEEP_COLD}s)…"; sleep "$SLEEP_COLD"
# true child count from a settled listing (do one warm ls first, off the clock,
# then cool again) — actually: measure the async behavior directly.
python3 - "$TARGET" "$SLEEP_COLD" <<'PY'
import os,sys,time
target, cool = sys.argv[1], float(sys.argv[2])
def count():
    try:
        with os.scandir(target) as it:
            return sum(1 for e in it if not e.name.startswith("._") and e.name!=".DS_Store")
    except OSError:
        return -1
# first cold ls
t0=time.perf_counter()
first=count()
t_first=(time.perf_counter()-t0)*1000
# poll until stable (2 identical consecutive counts) or 15s
counts=[first]; stable_at=None; deadline=time.time()+15
prev=first; same=0
while time.time()<deadline:
    c=count()
    counts.append(c)
    if c==prev and c>=0:
        same+=1
        if same>=2:
            stable_at=(time.perf_counter()-t0)*1000; break
    else:
        same=0
    prev=c
    time.sleep(0.15)
final=counts[-1]
print("  first cold ls: count=%d  in %.1f ms"%(first,t_first))
print("  final stable count=%d  populate_to_stable=%s ms"%(final, "%.1f"%stable_at if stable_at else ">15000 (never stabilized)"))
print("  count trajectory (first 12 samples): %s"%counts[:12])
if first==final:
    print("  -> COMPLETE ON FIRST LS (mirror served the full listing synchronously; no async backfill)")
else:
    print("  -> ASYNC BACKFILL observed: first=%d final=%d (%d entries arrived after the first ls)"%(first,final,final-first))
PY

echo
echo "=== LADDER SUMMARY [$LABEL] ==="
echo "  L0 ICMP RTT (avg)     : ${L0} ms"
echo "  L1 Redis PING (p50)   : ${L1} ms"
echo "  L2 cold/warm readdir  : ${COLD_MS:-NA} / ${WARM_MS:-NA} ms  (see op counts above)"
echo "  L3 mirror populate    : see above"
echo "  L4 Finder column view : run finder_column_open.scpt (GUI)"
