#!/usr/bin/env bash
# bin-browse: simulate an NLE bin-browse / Finder window-open pattern.
# Sustained LOOKUP + GETATTR + READDIRPLUS tree walks.
#
# Exposes: phantom-purge gate cost, children-index integrity, the
# 2-second-budgeted Lstat in juiceFS.Stat that fires on every cache-
# entry stat.
#
# Env:
#   TARGET_DIR   required — directory with at least ~100 entries
#   DURATION     workload wall-clock; default 30s
#   ART_DIR      artifact dir

set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/_common.sh"

TARGET_DIR="${TARGET_DIR:-}"
DURATION="${DURATION:-30}"
ART_DIR="${ART_DIR:-/tmp/jm-perf/bin-browse-$(date +%s)}"
mkdir -p "$ART_DIR"

if [ -z "$TARGET_DIR" ] || [ ! -d "$TARGET_DIR" ]; then
    echo "bin-browse: TARGET_DIR not set or missing" >&2
    exit 2
fi

echo "bin-browse: target=$TARGET_DIR duration=${DURATION}s artifacts=$ART_DIR"
START_LINE=$(log_line_count)
snapshot_metrics_to "$ART_DIR/before.json"

END=$(($(date +%s) + DURATION))
ITERS=0
while [ "$(date +%s)" -lt "$END" ]; do
    # Recursive ls -la triggers LOOKUP + GETATTR + READDIRPLUS at every
    # directory level. Cap depth so the iteration completes within the
    # workload window even on big trees.
    find "$TARGET_DIR" -maxdepth 4 -print >/dev/null 2>&1
    # ls -la at the top adds a READDIRPLUS pass.
    ls -la "$TARGET_DIR" >/dev/null 2>&1
    ITERS=$((ITERS + 1))
done
ACTUAL_DUR=$DURATION

snapshot_metrics_to "$ART_DIR/after.json"
END_LINE=$(log_line_count)
STALE=$(stale_events_in_range "$START_LINE" "$END_LINE")

build_summary "$ART_DIR/before.json" "$ART_DIR/after.json" "$ART_DIR/summary.json" \
    "bin-browse" "$ACTUAL_DUR" "$STALE"
echo "=== bin-browse summary ==="
cat "$ART_DIR/summary.json"
