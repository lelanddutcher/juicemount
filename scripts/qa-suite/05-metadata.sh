#!/usr/bin/env bash
# 05-metadata.sh — metadata storm tests (stat/getattr/readdir heavy).
# Target: ~15 min.
#
# Stress the metadata path:
#   - SQLite store hot path
#   - readdir / getattr RPC throughput
#   - tier-1.2 (Finder doesn't freeze under directory iteration)

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

phase_init "05-metadata"

TROOT="$MOUNT/.jmqa-meta-$$"
mkdir -p "$TROOT"

# ---------------------------------------------------------------------------
section "create 5000 small files in flat directory"
START=$(date +%s)
for i in $(seq 1 5000); do
    echo "$i" > "$TROOT/f-$i.txt"
done
ELAPSED=$(( $(date +%s) - START + 1 ))
CREATE_RATE=$(( 5000 / ELAPSED ))
log "creation: 5000 files in ${ELAPSED}s = ${CREATE_RATE} files/s"
(( CREATE_RATE > 50 )) && pass "creation rate > 50 files/s" || warn "creation rate only ${CREATE_RATE}/s"

# ---------------------------------------------------------------------------
section "readdir storm (ls -la on 5000 entries × 10 iterations)"
START=$(date +%s)
for i in 1 2 3 4 5 6 7 8 9 10; do
    ls -la "$TROOT" >/dev/null 2>&1
done
ELAPSED=$(( $(date +%s) - START + 1 ))
log "10 × readdir on 5000 entries: ${ELAPSED}s total"
(( ELAPSED < 30 )) && pass "10×readdir under 30s" || warn "10×readdir took ${ELAPSED}s"

# ---------------------------------------------------------------------------
section "stat-every-entry storm"
START=$(date +%s)
STATTED=0
while IFS= read -r f; do
    stat "$f" >/dev/null 2>&1
    STATTED=$((STATTED+1))
done < <(ls -1 "$TROOT")
ELAPSED=$(( $(date +%s) - START + 1 ))
RATE=$(( STATTED / ELAPSED ))
log "stat-storm: $STATTED stat calls in ${ELAPSED}s = ${RATE} stat/s"
(( RATE > 100 )) && pass "stat rate > 100/s" || warn "stat rate only ${RATE}/s"

# ---------------------------------------------------------------------------
section "deep directory tree (5 levels × 10 wide)"
DEEP="$TROOT/deep"
mkdir -p "$DEEP"
START=$(date +%s)
for a in $(seq 1 5); do
    for b in $(seq 1 5); do
        for c in $(seq 1 5); do
            mkdir -p "$DEEP/$a/$b/$c"
            for f in 1 2 3; do
                echo "$a-$b-$c-$f" > "$DEEP/$a/$b/$c/file-$f"
            done
        done
    done
done
ELAPSED=$(( $(date +%s) - START + 1 ))
FILES=$(find "$DEEP" -type f | wc -l | tr -d ' ')
log "deep tree: $FILES files across 5 levels in ${ELAPSED}s"
[[ "$FILES" -gt 100 ]] && pass "deep tree built" || fail "deep tree incomplete"

# ---------------------------------------------------------------------------
section "find -name (Spotlight-equivalent recursive scan)"
START=$(date +%s)
HITS=$(find "$DEEP" -name 'file-2' 2>/dev/null | wc -l | tr -d ' ')
ELAPSED=$(( $(date +%s) - START + 1 ))
log "find -name across deep tree: $HITS hits in ${ELAPSED}s"
(( HITS > 0 )) && pass "find returned results" || fail "find found nothing"

# ---------------------------------------------------------------------------
section "concurrent stat from 8 parallel workers"
PIDS=()
for w in 1 2 3 4 5 6 7 8; do
    (
        for i in $(seq 1 200); do
            stat "$TROOT/f-$((RANDOM % 5000 + 1)).txt" >/dev/null 2>&1
        done
    ) &
    PIDS+=($!)
done
START=$(date +%s)
for pid in "${PIDS[@]}"; do wait "$pid"; done
ELAPSED=$(( $(date +%s) - START + 1 ))
log "8 workers × 200 stats = 1600 total in ${ELAPSED}s = $(( 1600 / ELAPSED )) stat/s"
(( ELAPSED < 30 )) && pass "concurrent stat-storm under 30s" || warn "concurrent stat-storm ${ELAPSED}s"

# ---------------------------------------------------------------------------
section "directory listing during write (Finder-while-copying)"
WRITE_PID=""
(
    end=$(( $(date +%s) + 15 ))
    while (( $(date +%s) < end )); do
        echo "burst" > "$TROOT/burst-$RANDOM.txt"
    done
) &
WRITE_PID=$!
LS_HITS=0
LS_FAILS=0
end=$(( $(date +%s) + 15 ))
while (( $(date +%s) < end )); do
    if ls "$TROOT" >/dev/null 2>&1; then
        LS_HITS=$((LS_HITS+1))
    else
        LS_FAILS=$((LS_FAILS+1))
    fi
done
wait "$WRITE_PID" 2>/dev/null
log "ls while writing: $LS_HITS successful, $LS_FAILS failed"
(( LS_FAILS == 0 )) && pass "ls never failed during concurrent writes" || fail "$LS_FAILS ls failures"

snapshot_metrics "post-meta"
rm -rf "$TROOT" 2>/dev/null
phase_report
