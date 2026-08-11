#!/bin/bash
# finder_open_instrumented.sh — Step 3 wrapper: snapshot server /metrics + app
# log around a REAL Finder column-view open (L4), so we see which RPCs Finder
# issued and whether any fell back to the slow backend path.
#
#   before: /metrics snapshot + note log tail position
#   action: finder_column_open.sh (opens target in column view, times populate)
#   after : /metrics delta (per-RPC count + p50/p95/p99) + grep the log slice
#           for FUSE fallbacks / async dir-refresh / "Rebuilding index".
#
# Usage: finder_open_instrumented.sh <target_dir> [label] [cool_s]
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
TARGET="${1:?usage: finder_open_instrumented.sh <dir> [label] [cool_s]}"
LABEL="${2:-target}"
COOL="${3:-7}"
LOG="$HOME/Library/Logs/JuiceMount/juicemount.log"
TMP="${TMPDIR:-/tmp}"
BEF="$TMP/jm_mx_before_$$.json"; AFT="$TMP/jm_mx_after_$$.json"

echo "==================================================================="
echo "INSTRUMENTED FINDER OPEN  [$LABEL]  target=$TARGET"
echo "==================================================================="

# reconcile guard
GUARD=$(perl -e 'alarm 6; exec @ARGV' curl -s http://127.0.0.1:11050/activity 2>/dev/null | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("summary"),"|", [o["detail"] for o in d.get("operations",[]) if o.get("active")])' 2>/dev/null)
echo "[guard] $GUARD"

python3 "$HERE/metrics_delta.py" snap > "$BEF" 2>/dev/null
LOGLINES_BEFORE=$(wc -l < "$LOG" 2>/dev/null | tr -d ' ')

# The Finder open (L4)
"$HERE/finder_column_open.sh" "$TARGET" "$LABEL" "$COOL"

python3 "$HERE/metrics_delta.py" snap > "$AFT" 2>/dev/null
echo
echo "--- server /metrics delta across the open ---"
python3 "$HERE/metrics_delta.py" diff "$BEF" "$AFT"

echo
echo "--- app-log slice during the open (fallbacks / async-refresh / reconcile) ---"
if [ -n "$LOGLINES_BEFORE" ]; then
  tail -n +"$LOGLINES_BEFORE" "$LOG" 2>/dev/null | \
    grep -iE 'fallback|async|refresh|rebuild|reconcile|backend|wedge|stale|fuse|slow|noent|jukebox' | \
    grep -vi 'deferred to keyspace' | tail -20 | cut -c1-200
  echo "(log grew $(( $(wc -l < "$LOG") - LOGLINES_BEFORE )) lines during open)"
fi
rm -f "$BEF" "$AFT"
