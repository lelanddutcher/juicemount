#!/bin/bash
# Reliable synthetic repro of the "copies take forever" bug (task #100).
#
# ROOT CAUSE: cp/Finder copy the file's extended attributes after writing the data.
# On an NFS mount with no native xattr support, macOS writes them as a `._` AppleDouble
# sidecar via copyfile's fsetxattr. For files carrying a LARGE xattr (e.g. a camera's
# com.blackmagicdesign.thumbnail, ~20KB), that fsetxattr HANGS ~73s per file — while the
# JuiceMount NFS server sits IDLE (goroutine in conn.readRequestHeader; the macOS NFS
# client is stuck in-kernel, sending us nothing). The file DATA lands in ~2s; the ~73s is
# purely the ._-sidecar/xattr write. `cp -X` (skip xattrs) is instant, proving it.
#
# USE IN THE FIX LOOP: run this; after a fix the WITH-xattr copy should be ~as fast as -X.
#   PASS = with-xattr copy completes in < FAIL_SEC seconds.
set -u
MNT="${1:-/Volumes/zpool}"
FAIL_SEC="${FAIL_SEC:-10}"
SRC="${SRC:-$(find "$HOME/Desktop/proxy" -type f ! -name '._*' 2>/dev/null | head -1)}"
[ -n "$SRC" ] && [ -f "$SRC" ] || { echo "no source file (set SRC=…)"; exit 2; }
echo "src: $(basename "$SRC")  $(stat -f%z "$SRC") bytes  xattrs: $(xattr "$SRC" 2>/dev/null | tr '\n' ',')"
D="$MNT/repro_fsetxattr_$$"; mkdir -p "$D" 2>/dev/null || { echo "cannot mkdir on mount"; exit 2; }

timed(){ # timed <label> <cmd...> ; prints elapsed, kills at 2*FAIL_SEC
  local lbl="$1"; shift; "$@" 2>/dev/null & local p=$!; local t=$SECONDS
  while kill -0 $p 2>/dev/null; do
    [ $((SECONDS-t)) -ge $((FAIL_SEC*2)) ] && { echo "  $lbl: HANG (>$((FAIL_SEC*2))s) — kill"; kill -9 $p 2>/dev/null; return 1; }
    sleep 1
  done; echo "  $lbl: ${_x:-$((SECONDS-t))}s"; return 0
}
echo "=== baseline: cp -X (skip xattrs → no fsetxattr) — must be fast ==="
timed "cp -X" cp -X "$SRC" "$D/a_noxattr.mov"
echo "=== repro: cp WITH xattrs (the ._-sidecar/fsetxattr path) ==="
if timed "cp    " cp "$SRC" "$D/b_xattr.mov"; then el=$?; echo "RESULT: PASS (with-xattr copy fast)"; else echo "RESULT: FAIL — with-xattr copy hangs (bug present)"; fi
rm -rf "$D" 2>/dev/null
