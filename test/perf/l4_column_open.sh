#!/bin/bash
# l4_column_open.sh — L4 done right: measure a REAL Finder column-view window
# open on the target, WITHOUT the pathological `count of items` (which forces a
# deep recursive enumeration + byte reads). Instead we:
#   1. snapshot server /metrics + start an .accesslog capture,
#   2. time the `make new Finder window` + `set current view to column view`
#      round-trip (Finder returns control once the column is drawn/populated),
#   3. hold the window ~2s so Finder renders icons/thumbnails for the visible
#      column, then snapshot /metrics again + stop the accesslog,
#   4. report open-time + the server RPC delta (LOOKUP/GETATTR/READDIRPLUS/READ)
#      and bytes_read — so we see whether a plain column open reads FILE BYTES
#      (icons/previews/.DS_Store) and how much of that leaks to the backend.
#
# Run with reconcile IDLE for clean numbers (guard printed). Opens a visible
# Finder window (expected). READ-ONLY nav; a media column WILL read some bytes
# for thumbnails (intentional, reported as bytes_read delta).
#
# Usage: l4_column_open.sh <target_dir> [label] [cool_s]
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
TARGET="${1:?usage: l4_column_open.sh <dir> [label] [cool_s]}"
LABEL="${2:-target}"
COOL="${3:-7}"
FUSE="$HOME/.juicemount/fuse-internal"
BEF=/tmp/jm_l4_bef_$$.json; AFT=/tmp/jm_l4_aft_$$.json
AL=/tmp/jm_l4_al_$$.log
bcmd(){ local s="$1"; shift; perl -e 'alarm shift; exec @ARGV' "$s" "$@"; }

echo "=== L4 column-view OPEN [$LABEL] : $TARGET ==="
echo "guard: $(bcmd 6 curl -s http://127.0.0.1:11050/activity 2>/dev/null | python3 -c 'import sys,json;print(json.load(sys.stdin).get("summary"))' 2>/dev/null)"

# close any window already on target + cool
osascript -e "tell application \"Finder\"
  try
    repeat with w in (every Finder window)
      try
        if (target of w as alias) is (folder ((POSIX file \"$TARGET\") as text) as alias) then close w
      end try
    end repeat
  end try
end tell" >/dev/null 2>&1
sleep "$COOL"

python3 "$HERE/metrics_delta.py" snap > "$BEF" 2>/dev/null
( bcmd 30 cat "$FUSE/.accesslog" > "$AL" 2>/dev/null ) & ALPID=$!
sleep 0.4

# Time the open+view-set. Finder returns once the column is drawn.
RES=$(bcmd 30 osascript -e "
set t0 to (do shell script \"python3 -c 'import time;print(time.time())'\")
tell application \"Finder\"
  activate
  set w to make new Finder window to (folder ((POSIX file \"$TARGET\") as text))
  set current view of w to column view
  set bounds of w to {60, 60, 1300, 950}
end tell
set t1 to (do shell script \"python3 -c 'import time;print(time.time())'\")
return (t1 - t0) as text
" 2>&1)
# hold to let icons/thumbnails render for the visible column
sleep 2.0
python3 "$HERE/metrics_delta.py" snap > "$AFT" 2>/dev/null
sleep 0.3; kill $ALPID 2>/dev/null; wait $ALPID 2>/dev/null
# close window
osascript -e "tell application \"Finder\"
  try
    close (every Finder window whose target as alias is (folder ((POSIX file \"$TARGET\") as text) as alias))
  end try
end tell" >/dev/null 2>&1

echo "  open+column-view round-trip: $(python3 -c "print('%.0f ms'%(float('$RES')*1000))" 2>/dev/null || echo "raw=$RES")"
echo
echo "  --- server /metrics delta (open + 2s render) ---"
python3 "$HERE/metrics_delta.py" diff "$BEF" "$AFT"
echo
echo "  --- accesslog: did the plain open READ BYTES? ---"
python3 - "$AL" <<'PY'
import sys,re
ops={}; rd=0; rdbytes_hint=0; slow=0; slowsum=0
pat=re.compile(r'\[[^\]]*\] (\w+) \(([^)]*)\).*<([0-9.]+)>')
for ln in open(sys.argv[1],errors='replace').read().splitlines():
    m=pat.search(ln)
    if not m: continue
    op,args,d=m.group(1),m.group(2),float(m.group(3))*1000
    ops[op]=ops.get(op,0)+1
    if d>=5: slow+=1; slowsum+=d
print("  op counts:", ", ".join("%s=%d"%(k,v) for k,v in sorted(ops.items())) or "(none captured)")
print("  read ops=%d  total ops=%d  backend(>=5ms)=%d sum=%.0fms"%(ops.get("read",0),sum(ops.values()),slow,slowsum))
PY
rm -f "$BEF" "$AFT" "$AL"
