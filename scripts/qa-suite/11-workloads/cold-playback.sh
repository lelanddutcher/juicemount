#!/usr/bin/env bash
# cold-playback: sequential READ from a file whose chunks are NOT in
# the JuiceFS local cache. Backend-bound throughput.
#
# Exposes: timeo budget, prefetcher behavior, writeback queue
# pressure, MinIO bandwidth ceiling.
#
# To force "cold": jmctl cache-clear before the workload, then read.
# This evicts JuiceFS chunk cache; the read goes to MinIO.
#
# Env:
#   TARGET_FILE  required — file >= 200 MiB
#   DURATION     wall-clock budget (workload may finish earlier); default 60s
#   COLD         "1" to call cache-clear first; default "1"
#   ART_DIR      artifact dir

set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/_common.sh"

TARGET_FILE="${TARGET_FILE:-}"
DURATION="${DURATION:-60}"
COLD="${COLD:-1}"
ART_DIR="${ART_DIR:-/tmp/jm-perf/cold-playback-$(date +%s)}"
mkdir -p "$ART_DIR"

if [ -z "$TARGET_FILE" ] || [ ! -f "$TARGET_FILE" ]; then
    echo "cold-playback: TARGET_FILE not set or missing" >&2
    exit 2
fi
SIZE=$(stat -f%z "$TARGET_FILE" 2>/dev/null || echo 0)
if [ "$SIZE" -lt $((200 * 1024 * 1024)) ]; then
    echo "cold-playback: TARGET_FILE too small ($SIZE)" >&2
    exit 2
fi

echo "cold-playback: target=$TARGET_FILE size=$SIZE cold=$COLD"
if [ "$COLD" = "1" ]; then
    "$JMCTL" cache-clear >/dev/null 2>&1 || echo "(cache-clear failed; proceeding anyway)" >&2
    sleep 2
fi
START_LINE=$(log_line_count)
snapshot_metrics_to "$ART_DIR/before.json"

# Read sequentially with a wall-clock cap, up to 500 MiB.
WALL_START=$(date +%s)
COUNT=$(( SIZE / (1024 * 1024) ))
if [ "$COUNT" -gt 500 ]; then COUNT=500; fi
( dd if="$TARGET_FILE" of=/dev/null bs=$((1024 * 1024)) count="$COUNT" status=progress 2>&1 ) &
DD_PID=$!
while kill -0 "$DD_PID" 2>/dev/null; do
    NOW=$(date +%s)
    if [ $((NOW - WALL_START)) -ge "$DURATION" ]; then
        kill "$DD_PID" 2>/dev/null
        break
    fi
    sleep 1
done
wait "$DD_PID" 2>/dev/null
ACTUAL_DUR=$(( $(date +%s) - WALL_START ))
ACTUAL_DUR=$(( ACTUAL_DUR < 1 ? 1 : ACTUAL_DUR ))

snapshot_metrics_to "$ART_DIR/after.json"
END_LINE=$(log_line_count)
STALE=$(stale_events_in_range "$START_LINE" "$END_LINE")

build_summary "$ART_DIR/before.json" "$ART_DIR/after.json" "$ART_DIR/summary.json" \
    "cold-playback" "$ACTUAL_DUR" "$STALE"
echo "=== cold-playback summary ==="
cat "$ART_DIR/summary.json"
