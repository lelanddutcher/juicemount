#!/usr/bin/env bash
# 02-finder.sh — Finder-equivalent real-world operations.
# Target: ~20 min.
#
# Covers the patterns macOS Finder produces under user actions:
#   - cp -p (copy with metadata)
#   - cp -R (recursive directory copy)
#   - mv across paths
#   - Spotlight stat-storm (recursive find + stat every entry)
#   - Quick Look-equivalent (open + read first ~256 KiB + close fast)
#   - Trash-equivalent (move to .Trash dir then rm)
#   - rsync (incremental cp pattern)

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

phase_init "02-finder"

TROOT="$MOUNT/.jmqa-finder-$$"
mkdir -p "$TROOT"

# ---------------------------------------------------------------------------
section "cp -p — preserve mode + xattrs (Finder normal copy)"
mkdir -p "$TROOT/cp-p"
for sz_mib in 1 10 50; do
    src="$TMPDIR_LOCAL/finder-cp-src-$$-${sz_mib}"
    dst="$TROOT/cp-p/file-${sz_mib}MiB"
    pool_slice "$src" "$sz_mib"
    xattr -w com.apple.FinderInfo \
        "00000000000000000000000000000000000000000000000000000000000000000000000000000000" \
        "$src" 2>/dev/null || true
    src_md5=$(md5q "$src")
    set +e
    cp -p "$src" "$dst" 2>/dev/null
    set -e
    dst_md5=$(md5q "$dst")
    if [[ "$src_md5" == "$dst_md5" && -n "$src_md5" ]]; then
        pass "cp -p ${sz_mib} MiB md5 round-trip"
    else
        fail "cp -p ${sz_mib} MiB md5 mismatch (src=$src_md5 dst=$dst_md5)"
    fi
    rm -f "$src"
done

# ---------------------------------------------------------------------------
section "cp -R — recursive directory tree (dragging a folder in Finder)"
TREE_SRC="$TMPDIR_LOCAL/finder-tree-src-$$"
mkdir -p "$TREE_SRC"/{a,b,c,d}/{sub1,sub2}
for d in "$TREE_SRC"/*/sub*; do
    for i in 1 2 3 4 5; do
        head -c 65536 /dev/urandom > "$d/file-$i.dat"
    done
done
SRC_FILES=$(find "$TREE_SRC" -type f | wc -l | tr -d ' ')
SRC_TOTAL=$(du -sk "$TREE_SRC" | awk '{print $1}')
TREE_DST="$TROOT/tree-copy"
START=$(date +%s)
set +e
cp -R "$TREE_SRC" "$TREE_DST" 2>"${PHASE_DIR}/cp-R.err"
cp_exit=$?
set -e
ELAPSED=$(( $(date +%s) - START ))
DST_FILES=$(find "$TREE_DST" -type f 2>/dev/null | wc -l | tr -d ' ')
DST_TOTAL=$(du -sk "$TREE_DST" 2>/dev/null | awk '{print $1}')
log "cp -R: $SRC_FILES files / ${SRC_TOTAL}K → $DST_FILES files / ${DST_TOTAL}K in ${ELAPSED}s, exit=$cp_exit"
if [[ "$SRC_FILES" -eq "$DST_FILES" && "$SRC_TOTAL" -eq "$DST_TOTAL" ]]; then
    pass "cp -R file count + total size match"
else
    fail "cp -R divergence: src=$SRC_FILES/${SRC_TOTAL}K vs dst=$DST_FILES/${DST_TOTAL}K"
fi

# Spot-check md5 on a handful of nested files
section "spot-check md5 on nested files after cp -R"
fail_seen=0
for rel in a/sub1/file-1.dat b/sub2/file-3.dat c/sub1/file-5.dat d/sub2/file-2.dat; do
    s_md5=$(md5q "$TREE_SRC/$rel")
    d_md5=$(md5q "$TREE_DST/$rel")
    if [[ "$s_md5" != "$d_md5" || -z "$s_md5" ]]; then
        fail "cp -R nested mismatch at $rel"
        fail_seen=1
    fi
done
[[ $fail_seen -eq 0 ]] && pass "cp -R nested files all md5-match"
rm -rf "$TREE_SRC"

# ---------------------------------------------------------------------------
section "mv across paths (Finder's drag-with-Cmd)"
mkdir -p "$TROOT/mv-from" "$TROOT/mv-to"
pool_slice "$TROOT/mv-from/movable.dat" 20
PRE_MD5=$(md5q "$TROOT/mv-from/movable.dat")
mv "$TROOT/mv-from/movable.dat" "$TROOT/mv-to/movable.dat" 2>/dev/null
POST_MD5=$(md5q "$TROOT/mv-to/movable.dat")
if [[ "$PRE_MD5" == "$POST_MD5" && -n "$PRE_MD5" ]] && [[ ! -f "$TROOT/mv-from/movable.dat" ]]; then
    pass "mv preserves bytes AND removes source"
else
    fail "mv broken: pre=$PRE_MD5 post=$POST_MD5 src_still_exists=$([[ -f "$TROOT/mv-from/movable.dat" ]] && echo yes || echo no)"
fi

# ---------------------------------------------------------------------------
section "Spotlight stat-storm (recursive find + stat every entry)"
TOTAL_STAT=0
START=$(date +%s)
while IFS= read -r f; do
    stat "$f" >/dev/null 2>&1
    TOTAL_STAT=$((TOTAL_STAT+1))
done < <(find "$MOUNT" -maxdepth 3 -type f 2>/dev/null | head -500)
ELAPSED=$(( $(date +%s) - START ))
if (( TOTAL_STAT > 0 && ELAPSED < 60 )); then
    pass "stat-storm: $TOTAL_STAT entries in ${ELAPSED}s ($(( TOTAL_STAT / (ELAPSED + 1) )) stat/s)"
else
    warn "stat-storm: $TOTAL_STAT in ${ELAPSED}s (slow or no files found)"
fi

# ---------------------------------------------------------------------------
section "Quick Look-equivalent (open + read first 256 KiB + close fast)"
QL_HITS=0
QL_MISSES=0
while IFS= read -r f; do
    if head -c 262144 "$f" >/dev/null 2>&1; then
        QL_HITS=$((QL_HITS+1))
    else
        QL_MISSES=$((QL_MISSES+1))
    fi
done < <(find "$MOUNT" -maxdepth 3 -type f 2>/dev/null | head -50)
if (( QL_HITS > 0 && QL_MISSES == 0 )); then
    pass "Quick Look-equivalent: $QL_HITS/50 succeeded"
elif (( QL_HITS > 0 )); then
    warn "Quick Look-equivalent: $QL_HITS succeeded, $QL_MISSES failed"
else
    fail "Quick Look-equivalent: 0 succeeded"
fi

# ---------------------------------------------------------------------------
section "rsync (incremental cp pattern many editors use)"
if has_cmd rsync; then
    RSRC="$TMPDIR_LOCAL/finder-rsync-src-$$"
    RDST="$TROOT/rsync-dst"
    mkdir -p "$RSRC"
    for i in $(seq 1 20); do
        head -c $((128 * 1024)) /dev/urandom > "$RSRC/file-$i.bin"
    done
    # First sync
    rsync -a "$RSRC/" "$RDST/" >"${PHASE_DIR}/rsync.log" 2>&1
    [[ $? -eq 0 ]] && pass "rsync initial sync"
    # Add files locally, sync again — incremental path
    head -c $((128 * 1024)) /dev/urandom > "$RSRC/file-NEW.bin"
    rsync -a "$RSRC/" "$RDST/" >>"${PHASE_DIR}/rsync.log" 2>&1
    [[ -f "$RDST/file-NEW.bin" ]] && pass "rsync incremental sync (new file)" || fail "rsync didn't pick up new file"
    rm -rf "$RSRC"
else
    warn "rsync not available — skipping"
fi

# ---------------------------------------------------------------------------
section "many small files (1000 × 1 KiB) — Finder's 'open with assets'"
SMALL_DIR="$TROOT/small-files"
mkdir -p "$SMALL_DIR"
START=$(date +%s)
for i in $(seq 1 1000); do
    head -c 1024 /dev/urandom > "$SMALL_DIR/file-${i}.txt"
done
ELAPSED=$(( $(date +%s) - START ))
CREATED=$(find "$SMALL_DIR" -type f | wc -l | tr -d ' ')
if [[ "$CREATED" -eq 1000 ]]; then
    pass "1000 small files created in ${ELAPSED}s ($(( 1000 * 1024 / (ELAPSED + 1) )) B/s)"
else
    fail "expected 1000 small files, got $CREATED"
fi

# Re-stat them all
START=$(date +%s)
ls -la "$SMALL_DIR" > "${PHASE_DIR}/ls-1k-files.txt" 2>&1
ELAPSED=$(( $(date +%s) - START ))
log "ls -la on 1000 entries: ${ELAPSED}s"
(( ELAPSED < 30 )) && pass "ls 1000 entries under 30s" || warn "ls 1000 entries took ${ELAPSED}s"

snapshot_metrics "post-finder"
snapshot_rss "post-finder"

# Cleanup
rm -rf "$TROOT" 2>/dev/null
phase_report
