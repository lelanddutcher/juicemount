#!/usr/bin/env bash
# crash-recover-test.sh — tier-1.4 acceptance test for docs/VISION.md.
#
# Acceptance criterion: "kill -9 JuiceMount; relaunch; metadata store
# opens cleanly within 5 s."
#
# This script automates that test, measuring three intervals:
#   T_kill:    time from SIGKILL to the process actually being reaped
#   T_launch:  time from `open` to the new process showing up in pgrep
#   T_ready:   time from new process up to /metrics endpoint responding
#   T_mount:   time from new process up to the NFS mount being reachable
#
# DEFAULTS TO --dry-run. This script kills the running JuiceMount app;
# without --dry-run it will tear down whatever's currently mounted at the
# user's mount path. Don't run it against a mount you're using.
#
# Usage:
#   scripts/crash-recover-test.sh                     # dry-run, prints plan
#   scripts/crash-recover-test.sh --real              # actually does it
#   scripts/crash-recover-test.sh --real --mount /Volumes/zpool-dev
#   scripts/crash-recover-test.sh --real --metrics http://127.0.0.1:11050/metrics
#
# Exit codes:
#   0  pass — recovery within budget
#   1  fail — recovery exceeded budget or something didn't come back
#   2  precondition error (nothing running, missing app bundle, etc.)

set -euo pipefail

# --- defaults ---
DRY_RUN=1
APP_PATH="${APP_PATH:-$(cd "$(dirname "$0")/.." && pwd)/build/JuiceMount.app}"
MOUNT_PATH="${MOUNT_PATH:-/Volumes/zpool-dev}"
METRICS_URL="${METRICS_URL:-http://127.0.0.1:11050/metrics}"
BUDGET_SECONDS="${BUDGET_SECONDS:-5}"
POLL_INTERVAL_MS=100

# --- arg parsing ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --real)        DRY_RUN=0; shift ;;
        --dry-run)     DRY_RUN=1; shift ;;
        --mount)       MOUNT_PATH="$2"; shift 2 ;;
        --metrics)     METRICS_URL="$2"; shift 2 ;;
        --app)         APP_PATH="$2"; shift 2 ;;
        --budget)      BUDGET_SECONDS="$2"; shift 2 ;;
        -h|--help)
            grep '^#' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

# --- helpers ---
log()   { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
warn()  { printf '\033[33m[%s] WARN %s\033[0m\n' "$(date +%H:%M:%S)" "$*"; }
err()   { printf '\033[31m[%s] ERR  %s\033[0m\n' "$(date +%H:%M:%S)" "$*"; }
pass()  { printf '\033[32m[%s] PASS %s\033[0m\n' "$(date +%H:%M:%S)" "$*"; }
fail()  { printf '\033[31m[%s] FAIL %s\033[0m\n' "$(date +%H:%M:%S)" "$*"; }

# Returns the current JuiceMount PID or empty.
jm_pid() { pgrep -f "$APP_PATH/Contents/MacOS/JuiceMount" || true; }

# Returns nonzero if the metrics endpoint isn't reachable.
metrics_up() { curl -fsS --max-time 1 "$METRICS_URL" >/dev/null 2>&1; }

# Returns nonzero if the mount isn't responsive.
mount_up() { /bin/test -d "$MOUNT_PATH" && /bin/ls -1 "$MOUNT_PATH" >/dev/null 2>&1; }

# Polls a check function until it succeeds or deadline elapses.
# Returns wall-clock milliseconds when it succeeded, or -1 on timeout.
wait_until() {
    local check_fn="$1"
    local deadline_ms="$2"
    local start_ms; start_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
    while true; do
        local now_ms; now_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
        if "$check_fn"; then
            echo $((now_ms - start_ms))
            return 0
        fi
        if [[ $((now_ms - start_ms)) -gt $deadline_ms ]]; then
            echo "-1"
            return 1
        fi
        sleep 0.$(printf '%03d' $POLL_INTERVAL_MS)
    done
}

# --- preconditions ---
log "crash-recover-test"
log "  app:       $APP_PATH"
log "  mount:     $MOUNT_PATH"
log "  metrics:   $METRICS_URL"
log "  budget:    ${BUDGET_SECONDS}s"
log "  dry_run:   $DRY_RUN"

if [[ ! -d "$APP_PATH" ]]; then
    err "app bundle not found: $APP_PATH"; exit 2
fi

current_pid="$(jm_pid)"
if [[ -z "$current_pid" ]]; then
    err "no JuiceMount process running. Launch it first, then start the mount."
    exit 2
fi
log "  current PID: $current_pid"

if ! metrics_up; then
    warn "metrics endpoint not responding at start — was the mount Start'ed?"
fi
if ! mount_up; then
    warn "mount path not responsive at start — was the mount Start'ed?"
fi

# --- dry-run ---
if [[ "$DRY_RUN" -eq 1 ]]; then
    log "DRY RUN — would do:"
    log "  1. kill -9 $current_pid"
    log "  2. wait for process to reap (deadline ${BUDGET_SECONDS}s)"
    log "  3. open $APP_PATH"
    log "  4. wait for new PID to appear"
    log "  5. wait for metrics endpoint to respond"
    log "  6. wait for mount to be reachable"
    log "  7. assert total recovery <= ${BUDGET_SECONDS}s"
    log "  8. report intervals"
    log ""
    log "to run for real: $0 --real"
    exit 0
fi

# --- real run ---
warn "REAL RUN — killing PID $current_pid"
sleep 1

t_kill_start_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
kill -9 "$current_pid" 2>/dev/null || true

t_reap_ms=$(wait_until 'test -z "$(jm_pid)"' $((BUDGET_SECONDS * 1000))) || true
if [[ "$t_reap_ms" == "-1" ]]; then
    fail "process did not reap within ${BUDGET_SECONDS}s after SIGKILL"
    exit 1
fi
log "  kill→reap: ${t_reap_ms}ms"

t_launch_start_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
open "$APP_PATH"

t_proc_ms=$(wait_until 'test -n "$(jm_pid)"' $((BUDGET_SECONDS * 1000))) || true
if [[ "$t_proc_ms" == "-1" ]]; then
    fail "no new JuiceMount process within ${BUDGET_SECONDS}s after open"
    exit 1
fi
log "  open→proc: ${t_proc_ms}ms"

# The new process needs to be told to Start the mount. JuiceMount doesn't
# auto-start unless the preference is set. For an unattended recovery
# test we accept "metrics endpoint responding" as the success criterion —
# the user must have set startAtLogin or equivalent OR manually clicked
# Start. If neither is true, this will timeout, which is a valid
# documentation of the current UX: kill-9 recovery requires manual Start.
t_metrics_ms=$(wait_until 'metrics_up' $((BUDGET_SECONDS * 1000))) || true
if [[ "$t_metrics_ms" == "-1" ]]; then
    warn "metrics endpoint not up within ${BUDGET_SECONDS}s — likely because"
    warn "JuiceMount doesn't auto-start the server. Manually click Start in"
    warn "the menu bar, then re-run. This is a tier-2 UX gap (auto-restart"
    warn "of the server on app launch, off by default)."
    fail "metrics endpoint did not come up in budget"
    exit 1
fi
log "  open→metrics: ${t_metrics_ms}ms"

t_mount_ms=$(wait_until 'mount_up' $((BUDGET_SECONDS * 1000))) || true
if [[ "$t_mount_ms" == "-1" ]]; then
    fail "mount path not reachable within ${BUDGET_SECONDS}s"
    exit 1
fi
log "  open→mount: ${t_mount_ms}ms"

total_ms=$((t_reap_ms + t_mount_ms))
log ""
log "  total recovery: ${total_ms}ms (budget ${BUDGET_SECONDS}s = $((BUDGET_SECONDS * 1000))ms)"

if [[ $total_ms -gt $((BUDGET_SECONDS * 1000)) ]]; then
    fail "total recovery ${total_ms}ms exceeded budget ${BUDGET_SECONDS}s"
    exit 1
fi

pass "crash-recover-test"
exit 0
