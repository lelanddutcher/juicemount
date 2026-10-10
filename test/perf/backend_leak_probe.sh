#!/bin/bash
# backend_leak_probe.sh — the A/B control that isolates "cache-served" from
# "leaked to backend". It measures three latencies on the SAME cellular link:
#
#   (A) WARM cached stat   — a path already in the SQLite/RAM mirror (nav).
#   (B) COLD backend stat  — the SAME metadata resolved through the FUSE-internal
#                            JuiceFS mount (~/.juicemount/fuse-internal), which
#                            goes Redis-over-tunnel. This is what nav latency
#                            WOULD be if it leaked to the backend.
#   (C) raw backend RTT    — ICMP ping to the Redis/MinIO host.
#
# If (A) << (C) ≈ (B): the mirror is hiding the backend — nav is cache-served.
# If (A) ≈ (C) ≈ (B): nav leaked to the slow backend.
#
# READ-ONLY, metadata-only. Uses `stat`, never reads bytes.
#
# Usage: backend_leak_probe.sh [SUBTREE]
set -u
SUBTREE="${1:-/Volumes/zpool/Film Projects/College Sports Co}"
FUSE="${JM_FUSE_INTERNAL:-$HOME/.juicemount/fuse-internal}"
BACKEND_HOST="${JM_BACKEND_HOST:-192.168.0.197}"
N="${JM_PROBE_N:-40}"
OUTDIR_B="${TMPDIR:-/tmp}"

now(){ python3 -c 'import time;print("%.6f"%time.time())'; }

# Portable bounded command: macOS has no coreutils `timeout`/`gtimeout` by
# default, and an unbounded stat against a wedged fuse-internal hangs forever.
# Use perl's alarm() (always present on macOS) to hard-bound each call.
# bcmd <secs> <cmd...>  → exit 142 (128+SIGALRM) on timeout.
bcmd(){ local s="$1"; shift; perl -e 'alarm shift; exec @ARGV' "$s" "$@"; }

# Map a mount path to its fuse-internal equivalent by swapping the /Volumes/zpool
# prefix for the fuse-internal root.
to_fuse(){ echo "$FUSE/${1#/Volumes/zpool/}"; }

echo "=== backend-leak A/B probe (cellular) ==="
echo "subtree=$SUBTREE"
echo "fuse-internal=$FUSE"
route get "$BACKEND_HOST" 2>/dev/null | grep -E "interface:"
echo

# Pick N real files from the subtree (non-sidecar).
FILES=$(find "$SUBTREE" -type f 2>/dev/null | grep -v '/\._' | head -"$N")
CNT=$(echo "$FILES" | grep -c . )
echo "sampling $CNT files"
echo

# (A) WARM cached stats via the NFS mount (served from RAM mirror)
tA=$(now)
echo "$FILES" | while IFS= read -r f; do stat -f%z "$f" >/dev/null 2>&1; done
tA2=$(now)
python3 -c "print('(A) WARM mirror stat  : total=%.3fs  mean_per_op=%.2fms'%($tA2-$tA,($tA2-$tA)/$CNT*1000))"

# (B) COLD backend stats via fuse-internal (Redis-over-tunnel). We stat the
# SAME files' fuse-internal path. To force a genuine backend round-trip (not a
# FUSE kernel attr-cache hit), we prepend a fresh nonexistent-sibling LOOKUP in
# the same dir — a guaranteed cache-miss that must resolve against Redis.
#
# CRITICAL: bound EACH fuse-internal stat with `timeout` — on cellular the
# FUSE-internal mount frequently WEDGES (redis_probe_ms in the hundreds-of-ms
# to seconds; see the app log "fuse wedge diagnostics"), and an unbounded stat
# there hangs the whole probe. A timeout is itself a valid data point: it means
# the backend path is unresponsive on this link (which is EXACTLY the condition
# the RAM mirror exists to hide). We count timeouts separately.
BTMO="${JM_BACKEND_STAT_TIMEOUT:-4}"
BTFILE="$OUTDIR_B/jm_btimeouts.$$"; : > "$BTFILE"
tB=$(now)
echo "$FILES" | while IFS= read -r f; do
  ff=$(to_fuse "$f")
  dd=$(dirname "$ff")
  # a name that cannot be attr-cached: forces JuiceFS->Redis lookup(miss).
  # exit 142 == 128+SIGALRM == perl alarm fired == wedged backend.
  bcmd "$BTMO" stat -f%z "$dd/.__leakprobe_$$_$RANDOM" >/dev/null 2>&1
  [ $? -eq 142 ] && echo timeout >> "$BTFILE"
done
tB2=$(now)
btimeouts=$(wc -l < "$BTFILE" 2>/dev/null | tr -d ' '); rm -f "$BTFILE"
python3 -c "print('(B) COLD backend miss : total=%.3fs  mean_per_op=%.2fms  timeouts=$btimeouts/$CNT  (FUSE->Redis over tunnel; timeout=wedged backend)'%($tB2-$tB,($tB2-$tB)/$CNT*1000))"

# (C) raw ICMP RTT
echo
echo "(C) raw backend RTT:"
ping -c 6 -i 0.3 "$BACKEND_HOST" 2>/dev/null | tail -1

echo
echo "Verdict rule: if (A) is far below (B) and (C), nav is served from the"
echo "local mirror and is backend-independent. If (A) tracks (B)/(C), it leaked."
