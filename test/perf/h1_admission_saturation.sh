#!/bin/bash
# h1_admission_saturation.sh — decide whether the H1 read-admission head-of-line
# block is REAL on this Mac, or whether macOS nfsiod self-limits reads below the
# 128-slot rpcSem (in which case P-H1 readSem surgery is NOT worth landing).
#
# Method: drive a cold-media READ STORM (many parallel cold reads funnel through
# the single per-mount TCP connection → many concurrent server READ RPCs), then
# (a) snapshot a goroutine dump and count how many goroutines are actually IN the
# read path (peak in-flight reads — does it reach ~128?), and (b) time a WARM
# metadata stat/readdir DURING the storm — if it spikes, metadata is stalling at
# admission behind the reads (H1 confirmed); if it stays µs, admission is fine.
#
# READ-ONLY (of=/dev/null, no writes). Bounded read volume (~4MB x N cold files).
set -u
CP=http://127.0.0.1:11050
FI="$HOME/.juicemount/fuse-internal"
N="${1:-150}"          # parallel cold readers (aim above the 128 rpcSem cap)
DUMP=/tmp/h1_gd_$$.txt
bcmd(){ local s="$1"; shift; perl -e 'alarm shift; exec @ARGV' "$s" "$@"; }

gdump(){ bcmd 6 curl -s "$CP/debug/pprof/goroutine?debug=1" 2>/dev/null; }
# count goroutines whose stack is in the READ path vs blocked acquiring rpcSem
count_reads(){ grep -cE 'ReadAt|nfs_onread|onRead|\(\*cachedFile\)\.ReadAt|fuseFD' "$1" 2>/dev/null; }
count_admit(){ grep -cE 'rpcSem|\(\*conn\)\.serve|chan send' "$1" 2>/dev/null; }

echo "=== H1 admission-saturation probe (N=$N parallel cold readers) ==="
echo "route: $(route get 192.168.0.197 2>/dev/null | awk '/interface:/{print $2}')  health: $(bcmd 4 curl -s -o /dev/null -w '%{http_code}' $CP/health)"

# pick N distinct COLD media files (large, so each read stays in-flight)
mapfile_compat(){ while IFS= read -r l; do printf '%s\n' "$l"; done; }
FILES=$(find "$FI" -type f \( -iname '*.mov' -o -iname '*.mp4' -o -iname '*.wav' -o -iname '*.dng' -o -iname '*.mxf' \) ! -name '._*' 2>/dev/null | head -"$N")
NF=$(printf '%s\n' "$FILES" | grep -c . )
echo "cold media files selected: $NF"
[ "$NF" -lt 10 ] && { echo "  too few media files — aborting"; exit 1; }

echo "--- baseline goroutine dump (idle) ---"
gdump > "$DUMP.base"
echo "  read-path goroutines: $(count_reads "$DUMP.base")   total goroutines: $(grep -c '^goroutine ' "$DUMP.base")"

echo "--- WARM metadata baseline (no storm) ---"
for i in 1 2 3; do python3 -c "import os,time;t=time.time();os.listdir('/Volumes/zpool');print('  readdir / : %.3f ms'%((time.time()-t)*1000))"; done

echo "--- launching $NF parallel cold reads (bs=1m count=4 each, of=/dev/null) ---"
i=0
printf '%s\n' "$FILES" | while IFS= read -r f; do
  [ -n "$f" ] && { dd if="$f" of=/dev/null bs=1m count=4 2>/dev/null & }
  i=$((i+1))
done
STORMPGID=$!
sleep 2   # let the storm ramp + fill in-flight reads

echo "--- DURING storm: goroutine dump + warm metadata latency ---"
gdump > "$DUMP.storm"
RP=$(count_reads "$DUMP.storm"); TOT=$(grep -c '^goroutine ' "$DUMP.storm")
echo "  PEAK read-path goroutines: $RP   (rpcSem cap=128 — if RP approaches/exceeds 128, reads CAN saturate admission)"
echo "  admission/chan-send-blocked goroutines: $(count_admit "$DUMP.storm")"
echo "  total goroutines: $TOT"
echo "  warm metadata DURING storm (compare to the µs baseline above):"
for i in 1 2 3 4 5; do python3 -c "import os,time;t=time.time();os.listdir('/Volumes/zpool');print('    readdir / : %.3f ms'%((time.time()-t)*1000))"; done
for i in 1 2 3; do python3 -c "import os,time;t=time.time();os.stat('/Volumes/zpool');print('    stat / : %.3f ms'%((time.time()-t)*1000))"; done

echo "--- draining the storm ---"
pkill -f 'dd if=.*fuse-internal' 2>/dev/null; pkill -x dd 2>/dev/null
wait 2>/dev/null
sleep 1

echo "=== VERDICT INPUTS ==="
echo "  peak in-flight reads = $RP (vs 128 cap). warm metadata during storm above."
echo "  -> H1 REAL if peak reads ~>= 128 AND warm metadata spiked to ms during the storm."
echo "  -> H1 NOT worth fixing if reads self-limited (peak << 128) OR metadata stayed µs."
rm -f "$DUMP.base" "$DUMP.storm"
