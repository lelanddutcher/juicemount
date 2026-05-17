#!/usr/bin/env bash
# 03-media.sh — Media-editing workflow patterns.
# Target: ~25 min.
#
# Replays patterns that Premiere Pro / DaVinci Resolve / Final Cut produce:
#   - Sequential whole-file read (playback)
#   - Random ±1 MiB seeks (scrubbing a timeline)
#   - Bulk import of a project (many random-sized media files at once)
#   - Mixed read+write (write a render output while reading source media)
#   - ffmpeg probe (the metadata-extraction storm Resolve does on import)

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

phase_init "03-media"

TROOT="$MOUNT/.jmqa-media-$$"
mkdir -p "$TROOT"

# ---------------------------------------------------------------------------
section "stage media: 5 × 100 MiB files (simulated clips)"
declare -a CLIPS
for i in 1 2 3 4 5; do
    clip="$TROOT/clip-${i}.bin"
    pool_slice "$TMPDIR_LOCAL/staging-$$-$i" 100
    cp "$TMPDIR_LOCAL/staging-$$-$i" "$clip"
    rm -f "$TMPDIR_LOCAL/staging-$$-$i"
    CLIPS+=("$clip")
done
pass "staged ${#CLIPS[@]} × 100 MiB clips"

# ---------------------------------------------------------------------------
section "sequential whole-file read (playback)"
TOTAL_BYTES=0
START=$(date +%s)
for clip in "${CLIPS[@]}"; do
    bytes=$(dd if="$clip" of=/dev/null bs=1M 2>&1 | awk '/bytes/ {print $1}' | head -1)
    TOTAL_BYTES=$((TOTAL_BYTES + bytes))
done
ELAPSED=$(( $(date +%s) - START + 1 ))
MBPS=$(( TOTAL_BYTES / ELAPSED / 1048576 ))
log "sequential read of ${#CLIPS[@]} × 100 MiB: ${TOTAL_BYTES} bytes in ${ELAPSED}s = ${MBPS} MiB/s"
(( MBPS > 50 )) && pass "sequential read > 50 MiB/s" || warn "sequential read only ${MBPS} MiB/s"

# ---------------------------------------------------------------------------
section "scrubbing simulation (100 random ±1 MiB reads across clip-1)"
CLIP="$TROOT/clip-1.bin"
CLIP_SZ=$(sz "$CLIP")
FAIL_SEEK=0
START=$(date +%s)
for i in $(seq 1 100); do
    offset=$(( RANDOM * RANDOM % (CLIP_SZ - 1048576) ))
    if ! dd if="$CLIP" of=/dev/null bs=1M count=1 iseek=$((offset / 1048576)) 2>/dev/null; then
        FAIL_SEEK=$((FAIL_SEEK+1))
    fi
done
ELAPSED=$(( $(date +%s) - START + 1 ))
SEEKS_PER_S=$(( 100 / ELAPSED ))
log "100 random seek+1MiB reads in ${ELAPSED}s = ${SEEKS_PER_S} seeks/s, $FAIL_SEEK failures"
if (( FAIL_SEEK == 0 )); then pass "all 100 random seeks completed"; else fail "$FAIL_SEEK seeks failed"; fi

# ---------------------------------------------------------------------------
section "bulk import (20 mixed-size files copied in parallel — Resolve project open)"
BULK_DIR="$TROOT/bulk-import"
mkdir -p "$BULK_DIR"
PIDS=()
declare -a EXPECTED_MD5
declare -a BULK_SRCS
for i in $(seq 1 20); do
    size=$(( (RANDOM % 50) + 1 ))  # 1–50 MiB
    src="$TMPDIR_LOCAL/bulk-$$-$i"
    pool_slice "$src" "$size"
    EXPECTED_MD5[$i]=$(md5q "$src")
    BULK_SRCS+=("$src")
    cp "$src" "$BULK_DIR/file-$i.bin" &
    PIDS+=($!)
done
for pid in "${PIDS[@]}"; do wait "$pid" 2>/dev/null; done
# Clean up sources AFTER all cps are done — backgrounding rm in the loop
# above produced a race where the source vanished before cp could open it.
for src in "${BULK_SRCS[@]}"; do rm -f "$src" 2>/dev/null; done
# Verify all
BULK_OK=0
BULK_BAD=0
for i in $(seq 1 20); do
    dst_md5=$(md5q "$BULK_DIR/file-$i.bin")
    if [[ "$dst_md5" == "${EXPECTED_MD5[$i]}" && -n "$dst_md5" ]]; then
        BULK_OK=$((BULK_OK+1))
    else
        BULK_BAD=$((BULK_BAD+1))
    fi
done
if (( BULK_OK == 20 )); then pass "20-file bulk import: all md5 match"; else fail "$BULK_BAD/20 bulk imports corrupted"; fi

# ---------------------------------------------------------------------------
section "mixed read + write (render to mount while reading from mount)"
RENDER_OUT="$TROOT/render-output.bin"
READ_PID=""
WRITE_PID=""
(
    # Read clips sequentially in a loop for 20s
    end=$(( $(date +%s) + 20 ))
    while (( $(date +%s) < end )); do
        for clip in "${CLIPS[@]}"; do
            dd if="$clip" of=/dev/null bs=1M 2>/dev/null
        done
    done
) >/dev/null 2>&1 &
READ_PID=$!
(
    # Write a 50 MiB output file
    pool_slice "$TMPDIR_LOCAL/render-src-$$" 50
    cp "$TMPDIR_LOCAL/render-src-$$" "$RENDER_OUT"
    rm -f "$TMPDIR_LOCAL/render-src-$$"
) >/dev/null 2>&1 &
WRITE_PID=$!
wait "$WRITE_PID" 2>/dev/null
W_EXIT=$?
WRITE_SZ=$(sz "$RENDER_OUT")
kill "$READ_PID" 2>/dev/null || true
wait "$READ_PID" 2>/dev/null || true
if [[ $W_EXIT -eq 0 && $WRITE_SZ -eq $((50 * 1048576)) ]]; then
    pass "render-while-reading: write completed at correct size ($WRITE_SZ)"
else
    fail "render-while-reading: write exit=$W_EXIT size=$WRITE_SZ"
fi

# ---------------------------------------------------------------------------
section "ffmpeg probe (metadata extraction across all clips)"
if has_cmd ffmpeg && has_cmd ffprobe; then
    PROBE_OK=0
    PROBE_BAD=0
    START=$(date +%s)
    for clip in "${CLIPS[@]}"; do
        # ffprobe will fail (these are random bytes not real video) but should
        # respond quickly. We're testing the I/O path's responsiveness to
        # ffprobe's read-then-seek pattern, not container validity.
        timeout 10 ffprobe -v quiet -i "$clip" -show_format -show_streams \
            >/dev/null 2>&1
        # exit code doesn't matter — we just want it to NOT hang
        if [[ $? -ne 124 ]]; then
            PROBE_OK=$((PROBE_OK+1))
        else
            PROBE_BAD=$((PROBE_BAD+1))
        fi
    done
    ELAPSED=$(( $(date +%s) - START ))
    log "ffprobe across ${#CLIPS[@]} clips in ${ELAPSED}s — $PROBE_OK responded, $PROBE_BAD timed out"
    (( PROBE_BAD == 0 )) && pass "no ffprobe hangs" || fail "$PROBE_BAD ffprobe calls hung past 10s"
else
    warn "ffmpeg/ffprobe not installed — skipping"
fi

snapshot_metrics "post-media"
snapshot_rss "post-media"
rm -rf "$TROOT" 2>/dev/null
phase_report
