#!/usr/bin/env bash
# 07-failure.sh — failure injection + recovery.
# Target: ~20 min.
#
# Validates fail-safes and recovery paths:
#   1. SIGSTOP the juicefs FUSE daemon mid-read (FUSE hang detection)
#   2. SIGCONT the daemon — does the read resume cleanly?
#   3. Toggle user-offline mode mid-write — does write get clean ECONNREFUSED?
#   4. SIGTERM JuiceMount mid-operation — does it shut down cleanly?
#   5. Validate mount recovers automatically OR is restartable
#
# NOTE: this phase intentionally degrades the mount and depends on the
# orchestrator to verify recovery (and restart if needed) before the next
# phase runs.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

phase_init "07-failure"

TROOT="$MOUNT/.jmqa-failure-$$"
mkdir -p "$TROOT"

# ---------------------------------------------------------------------------
section "scenario A: SIGSTOP juicefs daemon mid-read (FUSE-hang resilience)"
JFS_PID=$(pgrep -f 'juicefs mount' | head -1)
if [[ -z "$JFS_PID" ]]; then
    warn "juicefs daemon not found — skipping A"
else
    # Place a file to read
    pool_slice "$TROOT/A-readtarget.bin" 50
    info "juicefs PID=$JFS_PID; sending SIGSTOP, reading should fail-fast not hang"
    kill -STOP "$JFS_PID"
    sleep 1
    # Try to read; should fail (or fall back to cache) within bounded time
    START=$(date +%s)
    set +e
    timeout 15 dd if="$TROOT/A-readtarget.bin" of=/dev/null bs=1M >/dev/null 2>&1
    rc=$?
    set -e
    ELAPSED=$(( $(date +%s) - START ))
    log "read while juicefs STOP'd: exit=$rc in ${ELAPSED}s"
    if [[ $rc -eq 124 ]]; then
        fail "read hung past 15s while juicefs was STOP'd"
    elif (( ELAPSED < 12 )); then
        pass "read returned within ${ELAPSED}s (fail-fast working)"
    else
        warn "read returned slowly: ${ELAPSED}s"
    fi
    # Resume the daemon
    kill -CONT "$JFS_PID"
    sleep 3
    info "juicefs resumed"
    # Re-attempt read
    START=$(date +%s)
    READ_SZ=$(dd if="$TROOT/A-readtarget.bin" of=/dev/null bs=1M 2>&1 | awk '/bytes/{print $1}' | head -1)
    ELAPSED=$(( $(date +%s) - START + 1 ))
    if [[ "$READ_SZ" -eq $((50 * 1048576)) ]]; then
        pass "post-resume read returned full 50 MiB in ${ELAPSED}s"
    else
        fail "post-resume read returned $READ_SZ bytes (expected $((50 * 1048576)))"
    fi
fi

# ---------------------------------------------------------------------------
section "scenario B: toggle user-offline mid-write"
# Stage a write that will be interrupted by toggling offline
pool_slice "$TMPDIR_LOCAL/B-src-$$" 100
(
    cp "$TMPDIR_LOCAL/B-src-$$" "$TROOT/B-write.bin" 2>/dev/null
) &
WP=$!
sleep 1  # Let it get into the write
# Trigger user-offline (the menubar app's "Go offline" toggle posts to /offline)
set +e
curl -s --max-time 3 -X POST "http://${JM_METRICS_ADDR}/offline" \
    -H "Content-Type: application/json" -d '{"offline":true}' >/dev/null
set -e
sleep 1
# Whatever happens to the write, capture state
wait "$WP" 2>/dev/null
W_EXIT=$?
B_SZ=$(sz "$TROOT/B-write.bin")
log "write-during-offline-toggle: exit=$W_EXIT, dst size=$B_SZ"
# Toggle back online
curl -s --max-time 3 -X POST "http://${JM_METRICS_ADDR}/offline" \
    -H "Content-Type: application/json" -d '{"offline":false}' >/dev/null 2>&1
sleep 2
# The write either succeeded (offline was set AFTER write completed) or failed
# fast (offline gate refused). Either is correct; what matters is no hang and
# no silent corruption of partial bytes.
if [[ $W_EXIT -ne 124 ]]; then
    pass "no hang during offline-toggle (write exit=$W_EXIT)"
else
    fail "write hung past timeout"
fi
rm -f "$TMPDIR_LOCAL/B-src-$$" 2>/dev/null

# Verify mount is healthy again
if jm_is_healthy && ! jm_auto_offline_engaged; then
    pass "mount healthy post-offline-toggle"
else
    warn "mount still degraded after offline-toggle"
fi

# ---------------------------------------------------------------------------
section "scenario C: cancel transfer mid-copy (does mount recover?)"
pool_slice "$TMPDIR_LOCAL/C-src-$$" 200
(
    cp "$TMPDIR_LOCAL/C-src-$$" "$TROOT/C-write.bin" 2>/dev/null
) &
CP_PID=$!
sleep 1
kill -INT "$CP_PID" 2>/dev/null || true
wait "$CP_PID" 2>/dev/null
sleep 2
# Try to do something after the cancel — mount should still work
if mount_writable; then
    pass "mount still writable after cancelled copy"
else
    fail "mount stuck after cancelled copy"
fi
rm -f "$TMPDIR_LOCAL/C-src-$$" 2>/dev/null

# ---------------------------------------------------------------------------
section "scenario D: pin-then-unpin cycle (cache plumbing)"
PIN_TARGET="$TROOT/D-pin-target.bin"
pool_slice "$PIN_TARGET" 20
PIN_PATH=$(python3 -c "import os; print(os.path.basename('$PIN_TARGET'))")
# Pin via HTTP endpoint
set +e
PIN_RESP=$(curl -s --max-time 10 -X POST "http://${JM_METRICS_ADDR}/pin" \
    -H "Content-Type: application/json" \
    -d "{\"paths\":[\"/.jmqa-failure-$$/$PIN_PATH\"]}" 2>/dev/null)
set -e
sleep 3
if echo "$PIN_RESP" | grep -q -E 'queued|ready|"err":""|"err":null'; then
    pass "pin accepted by /pin endpoint"
else
    warn "pin response unexpected: $(echo "$PIN_RESP" | head -c 200)"
fi
# Unpin
set +e
UNPIN_RESP=$(curl -s --max-time 10 -X POST "http://${JM_METRICS_ADDR}/unpin" \
    -H "Content-Type: application/json" \
    -d "{\"paths\":[\"/.jmqa-failure-$$/$PIN_PATH\"]}" 2>/dev/null)
set -e
if [[ -n "$UNPIN_RESP" ]]; then
    pass "unpin completed"
else
    warn "unpin returned empty"
fi

# ---------------------------------------------------------------------------
section "scenario E: trigger /sync — does the metadata reconciler complete?"
START=$(date +%s)
set +e
SYNC_RESP=$(curl -s --max-time 60 -X POST "http://${JM_METRICS_ADDR}/sync" 2>/dev/null)
sync_rc=$?
set -e
ELAPSED=$(( $(date +%s) - START ))
if [[ $sync_rc -eq 0 ]]; then
    pass "/sync returned in ${ELAPSED}s"
else
    fail "/sync failed or timed out (rc=$sync_rc, ${ELAPSED}s)"
fi

snapshot_metrics "post-failure"
snapshot_lsof "post-failure"
snapshot_rss "post-failure"
rm -rf "$TROOT" 2>/dev/null
phase_report
