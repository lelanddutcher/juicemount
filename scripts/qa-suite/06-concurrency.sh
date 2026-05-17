#!/usr/bin/env bash
# 06-concurrency.sh — concurrent R/W stress.
# Target: ~25 min.
#
# Stress the dispatch path with worker fan-outs that mimic what happens when
# a user drops a folder of media on the mount: many parallel writers, then
# many parallel readers, then a mixed phase.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

phase_init "06-concurrency"

TROOT="$MOUNT/.jmqa-concurrency-$$"
mkdir -p "$TROOT"

# ---------------------------------------------------------------------------
section "16 parallel writers, each 20 MiB → distinct file"
declare -a EMD5
declare -a EDST
PIDS=()
for w in $(seq 1 16); do
    src="$TMPDIR_LOCAL/cc-write-src-$$-$w"
    dst="$TROOT/writer-$w.bin"
    pool_slice "$src" 20
    EMD5[$w]=$(md5q "$src")
    EDST[$w]=$dst
    cp "$src" "$dst" &
    PIDS+=($!)
done
START=$(date +%s)
for pid in "${PIDS[@]}"; do wait "$pid"; done
ELAPSED=$(( $(date +%s) - START + 1 ))
log "16 parallel writers (20 MiB each = 320 MiB total) in ${ELAPSED}s = $(( 320 / ELAPSED )) MiB/s"
OK=0; BAD=0
for w in $(seq 1 16); do
    if [[ "$(md5q "${EDST[$w]}")" == "${EMD5[$w]}" && -n "${EMD5[$w]}" ]]; then
        OK=$((OK+1))
    else
        BAD=$((BAD+1))
    fi
    rm -f "$TMPDIR_LOCAL/cc-write-src-$$-$w" 2>/dev/null
done
if (( BAD == 0 )); then pass "16 parallel writers: all md5 match"; else fail "$BAD/16 corrupted"; fi

# ---------------------------------------------------------------------------
section "32 parallel readers across the 16 files just written"
PIDS=()
declare -a READ_MD5
for r in $(seq 1 32); do
    target_idx=$(( (r - 1) % 16 + 1 ))
    target="${EDST[$target_idx]}"
    (
        md5q "$target" > "${PHASE_DIR}/parread-${r}.md5"
    ) &
    PIDS+=($!)
done
START=$(date +%s)
for pid in "${PIDS[@]}"; do wait "$pid"; done
ELAPSED=$(( $(date +%s) - START + 1 ))
log "32 parallel readers across 16 files in ${ELAPSED}s"
OK=0; BAD=0
for r in $(seq 1 32); do
    target_idx=$(( (r - 1) % 16 + 1 ))
    expected="${EMD5[$target_idx]}"
    actual=$(cat "${PHASE_DIR}/parread-${r}.md5" 2>/dev/null)
    if [[ "$expected" == "$actual" && -n "$expected" ]]; then OK=$((OK+1)); else BAD=$((BAD+1)); fi
done
if (( BAD == 0 )); then pass "32 parallel readers: all md5 match"; else fail "$BAD/32 reader-side mismatches"; fi

# ---------------------------------------------------------------------------
section "8 writers + 8 readers mixed (parallel R+W on same dir)"
MIX_DIR="$TROOT/mix"
mkdir -p "$MIX_DIR"
PIDS_W=()
declare -a MEMD5
declare -a MDST
for w in $(seq 1 8); do
    src="$TMPDIR_LOCAL/cc-mix-w-$$-$w"
    dst="$MIX_DIR/mixwrite-$w.bin"
    pool_slice "$src" 30
    MEMD5[$w]=$(md5q "$src")
    MDST[$w]=$dst
    cp "$src" "$dst" &
    PIDS_W+=($!)
done
# Concurrent readers off the 16 first-phase files
PIDS_R=()
for r in $(seq 1 8); do
    target="${EDST[$r]}"
    (
        md5q "$target" > "${PHASE_DIR}/mixread-${r}.md5"
    ) &
    PIDS_R+=($!)
done
for pid in "${PIDS_W[@]}" "${PIDS_R[@]}"; do wait "$pid"; done
# Verify writers
OK_W=0; BAD_W=0
for w in $(seq 1 8); do
    if [[ "$(md5q "${MDST[$w]}")" == "${MEMD5[$w]}" && -n "${MEMD5[$w]}" ]]; then OK_W=$((OK_W+1)); else BAD_W=$((BAD_W+1)); fi
    rm -f "$TMPDIR_LOCAL/cc-mix-w-$$-$w" 2>/dev/null
done
OK_R=0; BAD_R=0
for r in $(seq 1 8); do
    if [[ "$(cat "${PHASE_DIR}/mixread-${r}.md5")" == "${EMD5[$r]}" ]]; then OK_R=$((OK_R+1)); else BAD_R=$((BAD_R+1)); fi
done
if (( BAD_W == 0 && BAD_R == 0 )); then
    pass "mixed 8R+8W: all writes + all reads md5 match"
else
    fail "mixed: writes $BAD_W/8 wrong, reads $BAD_R/8 wrong"
fi

# ---------------------------------------------------------------------------
section "rapid create/delete churn (file lifecycle race)"
CHURN_DIR="$TROOT/churn"
mkdir -p "$CHURN_DIR"
CREATED=0
DELETED=0
end=$(( $(date +%s) + 20 ))
while (( $(date +%s) < end )); do
    f="$CHURN_DIR/churn-$RANDOM-$RANDOM.tmp"
    head -c 4096 /dev/urandom > "$f" 2>/dev/null && CREATED=$((CREATED+1))
    if (( RANDOM % 3 == 0 )) && [[ -f "$f" ]]; then
        rm -f "$f" 2>/dev/null && DELETED=$((DELETED+1))
    fi
done
log "churn: created $CREATED, deleted-during-churn $DELETED in 20s"
(( CREATED > 100 )) && pass "churn loop sustained > 100 creates" || warn "churn only $CREATED creates"

# ---------------------------------------------------------------------------
section "tarball create on mount (multi-file cohesion check)"
if has_cmd tar; then
    TARGET="$TROOT/snapshot.tar"
    START=$(date +%s)
    tar -cf "$TARGET" -C "$TROOT" mix >/dev/null 2>&1
    rc=$?
    ELAPSED=$(( $(date +%s) - START + 1 ))
    TAR_SZ=$(sz "$TARGET")
    log "tar -cf on mix dir: ${TAR_SZ} bytes in ${ELAPSED}s, exit=$rc"
    [[ $rc -eq 0 && $TAR_SZ -gt 0 ]] && pass "tar archive built" || fail "tar failed (rc=$rc size=$TAR_SZ)"
    # Verify by extracting + comparing
    if [[ $rc -eq 0 ]]; then
        EXTRACT="$TMPDIR_LOCAL/jmqa-extract-$$"
        mkdir -p "$EXTRACT"
        tar -xf "$TARGET" -C "$EXTRACT" >/dev/null 2>&1
        EXTRACT_FILES=$(find "$EXTRACT" -type f | wc -l | tr -d ' ')
        if [[ "$EXTRACT_FILES" -eq 8 ]]; then
            pass "tar extract round-trip: 8 files restored"
        else
            fail "tar extract: expected 8 files, got $EXTRACT_FILES"
        fi
        rm -rf "$EXTRACT"
    fi
else
    warn "tar not available — skipping"
fi

snapshot_metrics "post-concurrency"
snapshot_lsof "post-concurrency"
snapshot_rss "post-concurrency"
rm -rf "$TROOT" 2>/dev/null
phase_report
