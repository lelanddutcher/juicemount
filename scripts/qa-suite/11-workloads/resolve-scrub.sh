#!/usr/bin/env bash
# resolve-scrub: simulate a Resolve scrub pattern on a cached MP4.
# Sustained random READ at ~60-240 READ-RPCs/sec on 1-4 MiB ranges.
#
# Exposes: per-RPC overhead, cache hit latency tail, FUSE pool
# contention. This is the workload that caught QA-31 in production.
#
# Env:
#   TARGET_FILE  required — path to a cached file >= 500 MiB
#   DURATION     workload wall-clock; default 30s
#   ART_DIR      artifact dir; default /tmp/jm-perf/resolve-scrub-<ts>

set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/_common.sh"

TARGET_FILE="${TARGET_FILE:-}"
DURATION="${DURATION:-30}"
ART_DIR="${ART_DIR:-/tmp/jm-perf/resolve-scrub-$(date +%s)}"
mkdir -p "$ART_DIR"

if [ -z "$TARGET_FILE" ] || [ ! -f "$TARGET_FILE" ]; then
    echo "resolve-scrub: TARGET_FILE not set or missing — aborting" >&2
    echo "Set TARGET_FILE=/Volumes/zpool/path/to/cached-big-file.mp4" >&2
    exit 2
fi

SIZE=$(stat -f%z "$TARGET_FILE" 2>/dev/null || echo 0)
if [ "$SIZE" -lt $((500 * 1024 * 1024)) ]; then
    echo "resolve-scrub: TARGET_FILE smaller than 500 MiB ($SIZE) — pattern is meaningless" >&2
    exit 2
fi

echo "resolve-scrub: target=$TARGET_FILE size=$SIZE duration=${DURATION}s artifacts=$ART_DIR"
START_LINE=$(log_line_count)
snapshot_metrics_to "$ART_DIR/before.json"

END=$(($(date +%s) + DURATION))
ITERS=0
# Random offsets aligned to 4 KiB; 1-4 MiB read length.
while [ "$(date +%s)" -lt "$END" ]; do
    # Random offset in [0, SIZE - 4MiB), 4 KiB aligned.
    MAX_OFF=$(( (SIZE - 4 * 1024 * 1024) / 4096 ))
    OFF_BLK=$(( RANDOM * 32768 + RANDOM % MAX_OFF ))
    OFF_BLK=$(( OFF_BLK % (MAX_OFF + 1) ))
    OFF=$(( OFF_BLK * 4096 ))
    LEN_MB=$(( (RANDOM % 4) + 1 ))
    dd if="$TARGET_FILE" of=/dev/null bs=$((1024 * 1024)) count="$LEN_MB" \
        iseek="$((OFF / (1024 * 1024)))" >/dev/null 2>&1
    ITERS=$((ITERS + 1))
done
ACTUAL_DUR=$(( $(date +%s) - END + DURATION ))
ACTUAL_DUR=$(( ACTUAL_DUR < 1 ? 1 : ACTUAL_DUR ))

snapshot_metrics_to "$ART_DIR/after.json"
END_LINE=$(log_line_count)
STALE=$(stale_events_in_range "$START_LINE" "$END_LINE")

build_summary "$ART_DIR/before.json" "$ART_DIR/after.json" "$ART_DIR/summary.json" \
    "resolve-scrub" "$ACTUAL_DUR" "$STALE"

echo "=== resolve-scrub summary ==="
cat "$ART_DIR/summary.json"
