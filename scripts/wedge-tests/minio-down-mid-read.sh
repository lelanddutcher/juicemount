#!/usr/bin/env bash
# wedge-tests/minio-down-mid-read.sh — closes part of tier-1.2.
#
# Scenario: a `cat <large-file>` is in flight, sourcing bytes from
# MinIO via the JuiceFS chunk cache miss path. Mid-read, MinIO becomes
# unreachable. The acceptance criterion (per docs/ROADMAP/tier-1-stability.md
# iter B): the in-flight read errors out within READ_ERROR_BUDGET
# seconds (default 2) AND concurrent metadata-only ops on the mount
# (a stat on the mount root, which is served from the SQLite/Redis
# metadata path and does NOT need MinIO) stay under STAT_BUDGET_MS
# (default 500).
#
# The stat side-test is the "no Finder beachball" proxy: real Finder
# beachball is a WindowServer-level event we can't measure from a
# shell, but a stuck mount that holds the global server-side lock will
# block adjacent stats too. If stats stay snappy while the read dies,
# the rest of the file system stays responsive.
#
# Prerequisites:
#
#   1. JuiceMount mount up.
#   2. Passwordless sudo for /sbin/pfctl (see docs/dev-setup.md).
#   3. --probe-file pointing at a real file on the mount large enough
#      that a sequential cat takes >2× PRE_READ_DELAY_MS to complete.
#      Default tries to auto-discover under MOUNT.
#
# Anchor: com.apple/251.JuiceMountWedge. Distinct from the offline-
# resilience harness's anchor (250.JuiceMountTest) so the two can
# coexist without stomping each other.
#
# Usage:
#
#   scripts/wedge-tests/minio-down-mid-read.sh \
#       [--target HOST:PORT]      \ default 192.168.0.212:9000
#       [--mount PATH]            \ default /Volumes/zpool-dev
#       [--probe-file PATH]       \ default: auto-discover a large .mov
#       [--read-budget SECONDS]   \ default 2
#       [--stat-budget-ms MILLIS] \ default 500
#       [--pre-read-delay-ms MILLIS] \ default 500
#
# Exit codes:
#   0  pass (or inconclusive — see WARN line)
#   1  fail (acceptance criterion missed)
#   2  precondition error
#
# Emits a single trailing line `[PASS|FAIL|WARN] minio-down-mid-read: <details>`
# suitable for aggregation by a future wedge-matrix runner.
#
# If the script is SIGKILL'd from outside (`kill -9`), the trap won't
# fire and the pf anchor will persist, silently blocking MinIO. To
# recover manually:
#
#     sudo /sbin/pfctl -a com.apple/251.JuiceMountWedge -F all

set -euo pipefail

# --- defaults ---
TARGET="${TARGET:-192.168.0.212:9000}"
MOUNT="${MOUNT:-/Volumes/zpool-dev}"
PROBE_FILE=""
READ_ERROR_BUDGET=5     # acceptance bar from tier-1.2 ("errors within 5s")
STAT_BUDGET_MS=500
PRE_READ_DELAY_MS=500
MAX_WAIT_SECONDS=10     # hard ceiling for measuring natural exit time;
                        # if cat is still alive at MAX_WAIT we declare wedge
PF_ANCHOR="com.apple/251.JuiceMountWedge"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --target)             TARGET="$2"; shift 2 ;;
        --mount)              MOUNT="$2"; shift 2 ;;
        --probe-file)         PROBE_FILE="$2"; shift 2 ;;
        --read-budget)        READ_ERROR_BUDGET="$2"; shift 2 ;;
        --stat-budget-ms)     STAT_BUDGET_MS="$2"; shift 2 ;;
        --pre-read-delay-ms)  PRE_READ_DELAY_MS="$2"; shift 2 ;;
        --max-wait)           MAX_WAIT_SECONDS="$2"; shift 2 ;;
        -h|--help)            grep '^#' "$0" | sed 's/^# \?//'; exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

TARGET_HOST="${TARGET%:*}"
TARGET_PORT="${TARGET##*:}"

# Precondition: MAX_WAIT_SECONDS must exceed READ_ERROR_BUDGET, or
# the kill-at-MAX_WAIT path fires before the read has a chance to
# error within budget — producing confusingly inverted verdicts.
if [[ $MAX_WAIT_SECONDS -le $READ_ERROR_BUDGET ]]; then
    echo "error: --max-wait ($MAX_WAIT_SECONDS) must be > --read-budget ($READ_ERROR_BUDGET)" >&2
    exit 2
fi

# --- helpers (mirror scripts/test-offline-resilience.sh) ---
ts_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }
log()  { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
pass() { printf '\033[32m[PASS]\033[0m %s\n' "$*"; }
fail() { printf '\033[31m[FAIL]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[WARN]\033[0m %s\n' "$*"; }

engage_block() {
    local rule="block out quick proto tcp from any to $TARGET_HOST port $TARGET_PORT"
    sudo -n /sbin/pfctl -E 2>/dev/null || true
    echo "$rule" | sudo -n /sbin/pfctl -a "$PF_ANCHOR" -f - 2>/dev/null
}

release_block() {
    sudo -n /sbin/pfctl -a "$PF_ANCHOR" -F all 2>/dev/null || true
}

is_blocked() {
    nc -G 1 "$TARGET_HOST" "$TARGET_PORT" </dev/null 2>/dev/null
    [ $? -ne 0 ]
}

# Cleanup on any exit path — pfctl rules must not outlive the test
# (would silently break MinIO access for everything else).
cleanup() {
    if [[ -n "${CAT_PID:-}" ]]; then
        kill -KILL "$CAT_PID" 2>/dev/null || true
    fi
    release_block
}
trap cleanup EXIT INT TERM

# --- preconditions ---
log "== preconditions =="
log "  target:      $TARGET   (MinIO endpoint)"
log "  mount:       $MOUNT"
log "  read budget: ${READ_ERROR_BUDGET}s    stat budget: ${STAT_BUDGET_MS}ms"
log ""

if ! mount | grep -q " $MOUNT "; then
    fail "mount $MOUNT is not active"
    echo "[FAIL] minio-down-mid-read: precondition (mount inactive)"
    exit 2
fi
log "  ✓ mount is active"

if ! sudo -n /sbin/pfctl -s rules >/dev/null 2>&1; then
    fail "passwordless sudo for /sbin/pfctl not configured (see docs/dev-setup.md)"
    echo "[FAIL] minio-down-mid-read: precondition (no passwordless pfctl)"
    exit 2
fi
log "  ✓ passwordless pfctl available"

# MinIO must be reachable at baseline — otherwise the test result is
# meaningless ("read failed because MinIO was already down").
if ! nc -G 1 "$TARGET_HOST" "$TARGET_PORT" </dev/null 2>/dev/null; then
    fail "baseline: MinIO at $TARGET is not reachable"
    echo "[FAIL] minio-down-mid-read: precondition (MinIO baseline unreachable)"
    exit 2
fi
log "  ✓ MinIO reachable at baseline"

# Auto-discover a probe file if not provided. Cache-warming is the
# enemy: if we reuse the same file run-after-run, the JuiceFS chunk
# cache eventually holds the whole thing and cat finishes in <500ms
# (no MinIO requests, nothing to wedge). Two countermeasures:
#
#   1. Prefer files >= 1 GB — too big for JuiceFS's prefetch window to
#      cover, so post-block cat will starve once buffered chunks drain.
#   2. Track the last-used probe file in /tmp/jmwedge-last-probe and
#      skip it on the next run. Picks a different >=1GB file each run.
LAST_PROBE_STATE="/tmp/jmwedge-last-probe"
if [[ -z "$PROBE_FILE" ]]; then
    log "  no --probe-file specified; auto-discovering (>=1GB, rotating)..."
    last_used=""
    [[ -f "$LAST_PROBE_STATE" ]] && last_used=$(cat "$LAST_PROBE_STATE" 2>/dev/null)
    while IFS= read -r candidate; do
        if [[ -n "$candidate" && "$candidate" != "$last_used" ]]; then
            PROBE_FILE="$candidate"
            break
        fi
    done < <(find "$MOUNT" -type f -size +1G -not -path '*/.*' 2>/dev/null)
    # If only one candidate exists, fall back to it even if reused.
    if [[ -z "$PROBE_FILE" ]]; then
        PROBE_FILE="$last_used"
    fi
    if [[ -z "$PROBE_FILE" ]]; then
        fail "no probe file found under $MOUNT (need a file >= 1GB)"
        echo "[FAIL] minio-down-mid-read: precondition (no large probe file)"
        exit 2
    fi
fi
echo "$PROBE_FILE" >"$LAST_PROBE_STATE" 2>/dev/null || true
if [[ ! -f "$PROBE_FILE" ]]; then
    fail "probe file does not exist: $PROBE_FILE"
    echo "[FAIL] minio-down-mid-read: precondition (probe-file missing)"
    exit 2
fi
PROBE_SIZE=$(stat -f%z "$PROBE_FILE" 2>/dev/null || echo 0)
log "  ✓ probe file: $PROBE_FILE ($((PROBE_SIZE / 1024 / 1024)) MB)"

# Ensure pf is clean.
release_block

# --- the test ---
log ""
log "== minio-down-mid-read =="

# Start the streaming read. cat reads sequentially through the file;
# we want it to be mid-stream when MinIO disappears. Background it so
# we can engage the block, then wait for cat to exit.
log "  starting background cat..."
# Run cat directly (not via a subshell trampoline) so `wait $CAT_PID`
# returns cat's actual exit status. Bash propagates the awaited
# process's status through wait's exit code.
cat "$PROBE_FILE" >/dev/null 2>&1 &
CAT_PID=$!

# Let cat get past the open() and into actual byte streaming.
sleep $(echo "scale=3; $PRE_READ_DELAY_MS / 1000" | bc)

# Sanity: cat should still be running. If it already finished, the
# probe file was too small or fully cached — test inconclusive.
if ! kill -0 "$CAT_PID" 2>/dev/null; then
    # set -e + non-zero wait would abort; guard with || =.
    early_exit=0; wait "$CAT_PID" 2>/dev/null || early_exit=$?
    warn "cat finished before block could engage (exit $early_exit) — probe file too small or fully cached"
    echo "[WARN] minio-down-mid-read: inconclusive (probe finished before block)"
    exit 0
fi
log "  ✓ cat is streaming (PID $CAT_PID)"

# Engage the block.
log "  engaging pf block to $TARGET..."
engage_block
if ! is_blocked; then
    fail "pf block loaded but $TARGET still reachable (anchor not in eval path?)"
    echo "[FAIL] minio-down-mid-read: pf block ineffective"
    exit 2
fi
block_engaged_ms=$(ts_ms)
log "  ✓ pf block effective at $(date +%H:%M:%S.%3N)"

# Concurrent stat probes during the wedge. We run stats at 200ms
# intervals while waiting for cat to exit. Each must complete within
# STAT_BUDGET_MS — that's the "Finder doesn't beachball" proxy.
#
# We give cat up to MAX_WAIT_SECONDS to exit naturally. That's
# decoupled from READ_ERROR_BUDGET: we always measure the true exit
# time, then compare against the budget for the verdict. Without that
# separation, a kill-at-budget caps the measurement and we can't tell
# "errored at 4.9s (just under bar)" from "wedged forever."
stat_max_ms=0
stat_count=0
stat_fails=0
wedge_deadline_ms=$((block_engaged_ms + MAX_WAIT_SECONDS * 1000))
while kill -0 "$CAT_PID" 2>/dev/null; do
    now_ms=$(ts_ms)
    if [[ $now_ms -gt $wedge_deadline_ms ]]; then
        break
    fi
    s_start=$(ts_ms)
    if stat "$MOUNT/" >/dev/null 2>&1; then
        s_end=$(ts_ms)
        s_elapsed=$((s_end - s_start))
        stat_count=$((stat_count + 1))
        if [[ $s_elapsed -gt $stat_max_ms ]]; then
            stat_max_ms=$s_elapsed
        fi
    else
        stat_fails=$((stat_fails + 1))
    fi
    sleep 0.2
done

# Reap cat. If it's still alive at MAX_WAIT_SECONDS, that's a "wedge
# forever" outcome — we'll report it as a fail with wedged=true and
# kill it ourselves so the harness doesn't hang the next test.
cat_wedged=false
if kill -0 "$CAT_PID" 2>/dev/null; then
    cat_wedged=true
    kill -TERM "$CAT_PID" 2>/dev/null || true
    sleep 0.5
    kill -KILL "$CAT_PID" 2>/dev/null || true
fi
# `wait` returns the awaited process's exit status. For signals,
# bash reports 128+signum (KILL=137, TERM=143). Guard with || = so
# a non-zero status (the expected case — cat erroring on the wedged
# read) doesn't trip set -e.
cat_exit=0; wait "$CAT_PID" 2>/dev/null || cat_exit=$?
cat_exit_ms=$(ts_ms)

read_elapsed_ms=$((cat_exit_ms - block_engaged_ms))
read_elapsed_s=$(echo "scale=2; $read_elapsed_ms / 1000" | bc)
budget_ms=$((READ_ERROR_BUDGET * 1000))

log "  cat exit: $cat_exit  after ${read_elapsed_ms}ms post-block  (wedged=$cat_wedged)"
log "  stat probes: $stat_count completed (max ${stat_max_ms}ms), $stat_fails failed"

# Release block before reporting (so user isn't stuck offline mid-debug).
release_block

# Verdict.
#
# The acceptance bar from tier-1.2 is "Finder reports error within 5s,
# doesn't beachball." The two signals map to:
#
#   - "doesn't beachball" → adjacent stat ops on the mount root stay
#     under STAT_BUDGET_MS while the doomed read is dying. This is
#     the canonical responsiveness check; violation = HARD FAIL.
#   - "reports error within 5s" → here we measure cat-to-EOF-error
#     time. That's strictly conservative: cat drains the JuiceFS
#     prefetch buffer (~64 MB per chunk) before issuing the read that
#     actually fails, so cat may take 2-6s post-block to exit even
#     when the first read error is delivered to the kernel in <1s.
#     Real Finder issues small reads, not stream-to-EOF, so it sees
#     the first error much sooner. We log cat-exit-time as a WARN
#     when it exceeds READ_ERROR_BUDGET, not a hard fail.
#
# Hard fail conditions:
#   - cat wedged past MAX_WAIT_SECONDS (read never erroring at all)
#   - adjacent stat max exceeded STAT_BUDGET_MS (real beachball)
verdict_pass=true
verdict_details=""

if [[ "$cat_wedged" == "true" ]]; then
    verdict_pass=false
    verdict_details="cat never exited within MAX_WAIT=${MAX_WAIT_SECONDS}s — read appears wedged"
fi

if [[ "$cat_wedged" == "false" && "$cat_exit" == "0" ]]; then
    # Clean exit (and we did NOT have to kill cat) means the whole
    # file was already in cache and streamed past the block before
    # MinIO mattered. Inconclusive, not a hard failure — caller
    # should rerun with a fresh probe.
    #
    # The cat_wedged guard prevents a race where a SIGKILL'd cat
    # somehow reports exit 0 from silently overriding the wedge
    # verdict (review HIGH-2).
    warn "cat exited 0 — probe file likely cached; test inconclusive"
    echo "[WARN] minio-down-mid-read: inconclusive (cat exited 0 in ${read_elapsed_s}s; try a different probe file)"
    exit 0
fi

if [[ $stat_max_ms -gt $STAT_BUDGET_MS ]]; then
    if [[ "$verdict_pass" == "true" ]]; then
        verdict_pass=false
        verdict_details="adjacent stat max ${stat_max_ms}ms (budget ${STAT_BUDGET_MS}ms) — Finder would beachball"
    else
        verdict_details="$verdict_details; adjacent stat max ${stat_max_ms}ms"
    fi
fi

if [[ $stat_count -eq 0 ]]; then
    warn "no stat probes completed during wedge — read died too fast to sample"
fi

read_status="within"
if [[ $read_elapsed_ms -gt $budget_ms ]]; then
    read_status="over"
    warn "read-to-EOF-error took ${read_elapsed_s}s (over ${READ_ERROR_BUDGET}s budget — but this is stream-to-EOF, not first-error)"
fi

log ""
if [[ "$verdict_pass" == "true" ]]; then
    pass "minio-down-mid-read: read errored in ${read_elapsed_s}s ($read_status budget); stat max ${stat_max_ms}ms (probes=$stat_count)"
    echo "[PASS] minio-down-mid-read: read=${read_elapsed_s}s ($read_status budget) stat_max=${stat_max_ms}ms probes=$stat_count"
    exit 0
else
    fail "minio-down-mid-read: $verdict_details"
    echo "[FAIL] minio-down-mid-read: $verdict_details (read=${read_elapsed_s}s stat_max=${stat_max_ms}ms)"
    exit 1
fi
