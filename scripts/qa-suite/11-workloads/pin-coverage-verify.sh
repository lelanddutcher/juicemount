#!/usr/bin/env bash
# pin-coverage-verify: drive the pin-coverage verify path (re-prefetch
# every pinned file). Bounded by disk + backend.
#
# Exposes: LRU/eviction pressure during sustained read, juicefs cache
# fill behavior, the prefetcher's per-file state-machine.
#
# Env:
#   WAIT_SEC     how long to let the worker pool drain after the
#                verify call; default 60s
#   ART_DIR      artifact dir

set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/_common.sh"

WAIT_SEC="${WAIT_SEC:-60}"
ART_DIR="${ART_DIR:-/tmp/jm-perf/pin-coverage-verify-$(date +%s)}"
mkdir -p "$ART_DIR"

# Snapshot before; need pinned-file count to know if the workload was
# actually meaningful (zero pins = no-op).
PIN_COUNT=$("$JMCTL" cache-status 2>/dev/null \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['aggregate']['TotalFiles'])" 2>/dev/null \
    || echo 0)
if [ "$PIN_COUNT" = "0" ]; then
    echo "pin-coverage-verify: zero pinned files — skipping (no-op)" >&2
    exit 0
fi
echo "pin-coverage-verify: pinned_files=$PIN_COUNT wait=${WAIT_SEC}s artifacts=$ART_DIR"
START_LINE=$(log_line_count)
snapshot_metrics_to "$ART_DIR/before.json"

WALL_START=$(date +%s)
"$JMCTL" verify-pins >/dev/null 2>&1
# Let the worker pool drain.
sleep "$WAIT_SEC"
ACTUAL_DUR=$(( $(date +%s) - WALL_START ))

snapshot_metrics_to "$ART_DIR/after.json"
END_LINE=$(log_line_count)
STALE=$(stale_events_in_range "$START_LINE" "$END_LINE")

build_summary "$ART_DIR/before.json" "$ART_DIR/after.json" "$ART_DIR/summary.json" \
    "pin-coverage-verify" "$ACTUAL_DUR" "$STALE"
echo "=== pin-coverage-verify summary ==="
cat "$ART_DIR/summary.json"
