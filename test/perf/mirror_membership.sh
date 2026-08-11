#!/bin/bash
# mirror_membership.sh — for each target dir (and a sample of its children),
# ask the control-plane /lookup whether the metadata is resident in the NFS
# MIRROR (SQLite/RAM). exists:true + an inode == mirror-resident (→ nav is
# RAM-served, backend-independent). exists:false == NOT in the mirror (→ a cold
# nav there must populate from the backend over the tunnel).
#
# This is the crux disambiguation for the latency ladder: it decides whether
# the sluggishness is COLD-BACKEND metadata populate or FINDER-side cost.
#
# READ-ONLY (pure control-plane HTTP GETs, never touches the mount).
#
# Usage: mirror_membership.sh <dir> [dir2 ...]
set -u
CP="${CP:-http://127.0.0.1:11050}"

enc(){ python3 -c 'import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))' "$1"; }
lookup(){ perl -e 'alarm 8; exec @ARGV' curl -s "$CP/lookup?path=$(enc "$1")" 2>/dev/null; }

verdict(){ # json -> RESIDENT/absent
  echo "$1" | python3 -c 'import sys,json
try: d=json.load(sys.stdin)
except Exception: print("ERR"); sys.exit()
print(("RESIDENT inode=%s dir=%s"%(d.get("inode"),d.get("is_dir"))) if d.get("exists") else "ABSENT")'
}

for DIR in "$@"; do
  echo "==================================================================="
  echo "TARGET: $DIR"
  j=$(lookup "$DIR")
  echo "  dir itself : $(verdict "$j")"
  # sample up to 8 children (mix dirs+files), probe each
  n=0; res=0; abs=0
  while IFS= read -r child; do
    [ -z "$child" ] && continue
    cj=$(lookup "$DIR/$child")
    v=$(verdict "$cj")
    case "$v" in RESIDENT*) res=$((res+1));; ABSENT) abs=$((abs+1));; esac
    printf "    child[%d] %-45.45s %s\n" "$n" "$child" "$v"
    n=$((n+1)); [ $n -ge 8 ] && break
  done < <(ls "$DIR" 2>/dev/null | grep -v '^\._' | grep -v '^\.DS_Store$' | head -8)
  echo "  -> children sampled=$n resident=$res absent=$abs"
done
