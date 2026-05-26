#!/usr/bin/env bash
# finder-copy-deep: simulate a Finder-style folder copy with many
# small files + AppleDouble sidecars. Sustained LOOKUP + CREATE +
# WRITE cascades.
#
# Exposes: sync writeMu contention, cache symmetry bugs (QA-27/28),
# pin/unpin races, AppleDouble inode-recycling edge cases.
#
# Env:
#   STAGING_DIR  required — writable dir under the mount; will be
#                cleaned up at the end
#   DURATION     workload wall-clock; default 30s
#   ART_DIR      artifact dir

set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/_common.sh"

STAGING_DIR="${STAGING_DIR:-}"
DURATION="${DURATION:-30}"
ART_DIR="${ART_DIR:-/tmp/jm-perf/finder-copy-deep-$(date +%s)}"
mkdir -p "$ART_DIR"

if [ -z "$STAGING_DIR" ]; then
    echo "finder-copy-deep: STAGING_DIR not set" >&2
    exit 2
fi
WORK="$STAGING_DIR/.jm-perf-finder-copy-$$"
mkdir -p "$WORK" || { echo "cannot create $WORK" >&2; exit 2; }
trap 'rm -rf "$WORK"' EXIT INT TERM

echo "finder-copy-deep: staging=$WORK duration=${DURATION}s artifacts=$ART_DIR"
START_LINE=$(log_line_count)
snapshot_metrics_to "$ART_DIR/before.json"

END=$(($(date +%s) + DURATION))
ITERS=0
# 4 KB content per file, batches of 50 per iteration, mixed with the
# `._xxx` sidecars Finder creates during a real copy.
PAYLOAD=$(head -c 4096 /dev/urandom | base64 | head -c 4096)
while [ "$(date +%s)" -lt "$END" ]; do
    BATCH="$WORK/b$ITERS"
    mkdir -p "$BATCH"
    for i in $(seq 1 50); do
        printf '%s' "$PAYLOAD" > "$BATCH/file-$i.bin"
        # AppleDouble sidecar — the inode-recycling pattern that
        # surfaced QA-27/28.
        printf '%s' "$PAYLOAD" > "$BATCH/._file-$i.bin"
    done
    ITERS=$((ITERS + 1))
done
ACTUAL_DUR=$DURATION

snapshot_metrics_to "$ART_DIR/after.json"
END_LINE=$(log_line_count)
STALE=$(stale_events_in_range "$START_LINE" "$END_LINE")

build_summary "$ART_DIR/before.json" "$ART_DIR/after.json" "$ART_DIR/summary.json" \
    "finder-copy-deep" "$ACTUAL_DUR" "$STALE"
echo "=== finder-copy-deep summary ==="
cat "$ART_DIR/summary.json"
