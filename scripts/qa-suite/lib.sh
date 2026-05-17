#!/usr/bin/env bash
# lib.sh — shared helpers for the JuiceMount QA suite.
#
# Sourced by every phase script and the orchestrator. Provides:
#   - common env defaults (MOUNT, ARTIFACTS, etc.)
#   - structured logging (log/pass/fail/warn/section)
#   - health probes (mount_is_nfs, mount_writable, jm_health)
#   - artifact capture (snapshot_metrics, snapshot_lsof)
#   - cleanup helpers
#
# Conventions:
#   - PASS_COUNT and FAIL_COUNT are global counters maintained per phase.
#   - Each phase script sources this, sets PHASE_NAME, then calls
#     phase_init / phase_report.
#   - Exit code: phase scripts ALWAYS exit 0 (so the orchestrator continues);
#     pass/fail is communicated via the .summary file in the phase artifact dir.

# ---------------------------------------------------------------------------
# Env defaults

MOUNT="${MOUNT:-/Volumes/zpool-dev}"
JM_METRICS_ADDR="${JM_METRICS_ADDR:-127.0.0.1:11050}"
FUSE_INTERNAL="${FUSE_INTERNAL:-$HOME/.juicemount/fuse-internal}"
ARTIFACTS_ROOT="${ARTIFACTS_ROOT:-/tmp/jm-qa-artifacts}"
RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
RUN_DIR="${ARTIFACTS_ROOT}/${RUN_ID}"
TMPDIR_LOCAL="${TMPDIR_LOCAL:-/tmp}"

mkdir -p "$RUN_DIR" 2>/dev/null

# ---------------------------------------------------------------------------
# Logging

ts() { date +%H:%M:%S; }

log()     { printf '[%s] %s\n' "$(ts)" "$*"; }
section() { printf '\n[%s] \033[36m== %s ==\033[0m\n' "$(ts)" "$*"; }
pass()    { printf '\033[32m[PASS]\033[0m %s\n' "$*"; PASS_COUNT=$((PASS_COUNT+1)); }
fail()    { printf '\033[31m[FAIL]\033[0m %s\n' "$*"; FAIL_COUNT=$((FAIL_COUNT+1)); }
warn()    { printf '\033[33m[WARN]\033[0m %s\n' "$*"; WARN_COUNT=$((WARN_COUNT+1)); }
info()    { printf '\033[34m[INFO]\033[0m %s\n' "$*"; }

# ---------------------------------------------------------------------------
# Phase boilerplate

phase_init() {
    PHASE_NAME="${1:?phase_init needs PHASE_NAME}"
    PHASE_DIR="${RUN_DIR}/${PHASE_NAME}"
    mkdir -p "$PHASE_DIR"
    PASS_COUNT=0
    FAIL_COUNT=0
    WARN_COUNT=0
    PHASE_START_TS=$(date +%s)
    log "==> phase $PHASE_NAME starting (artifacts: $PHASE_DIR)"
}

phase_report() {
    local elapsed=$(( $(date +%s) - PHASE_START_TS ))
    local summary_file="${PHASE_DIR}/.summary"
    cat >"$summary_file" <<EOF
phase=$PHASE_NAME
pass=$PASS_COUNT
fail=$FAIL_COUNT
warn=$WARN_COUNT
elapsed_sec=$elapsed
finished_at=$(date -Iseconds)
EOF
    log "==> phase $PHASE_NAME done (pass=$PASS_COUNT fail=$FAIL_COUNT warn=$WARN_COUNT in ${elapsed}s)"
    return 0  # phases NEVER fail the orchestrator; summary file communicates status
}

# ---------------------------------------------------------------------------
# Health probes

mount_is_nfs() {
    mount | awk -v m="$MOUNT" '$0 ~ " "m" " { print }' | grep -q '(nfs'
}

mount_writable() {
    local probe="$MOUNT/.jmqa-probe-$$-$RANDOM"
    : >"$probe" 2>/dev/null || return 1
    rm -f "$probe" 2>/dev/null
    return 0
}

jm_health() {
    curl -s --max-time 3 "http://${JM_METRICS_ADDR}/health" 2>/dev/null
}

jm_is_healthy() {
    local h
    h="$(jm_health)"
    [[ -n "$h" ]] && echo "$h" | grep -q '"healthy": *true'
}

jm_offline_state() {
    curl -s --max-time 3 "http://${JM_METRICS_ADDR}/offline" 2>/dev/null
}

jm_auto_offline_engaged() {
    jm_offline_state | grep -q '"auto_offline":true'
}

# wait_for_mount_back: polls every 2s up to timeout for mount + write to return.
# Used after failure-injection phases to confirm recovery before the next phase.
wait_for_mount_back() {
    local timeout="${1:-60}"
    local waited=0
    while (( waited < timeout )); do
        if mount_is_nfs && mount_writable && jm_is_healthy && ! jm_auto_offline_engaged; then
            return 0
        fi
        sleep 2
        waited=$((waited+2))
    done
    return 1
}

# ---------------------------------------------------------------------------
# Artifact capture (snapshots of system state at key moments)

snapshot_metrics() {
    local label="$1"
    local out="${PHASE_DIR}/metrics-${label}.json"
    {
        echo "{"
        echo '  "ts": "'"$(date -Iseconds)"'",'
        echo '  "health": '"$(jm_health | tr -d '\n' || echo null)"','
        echo '  "offline": '"$(jm_offline_state | tr -d '\n' || echo null)"','
        echo '  "metrics": '"$(curl -s --max-time 3 "http://${JM_METRICS_ADDR}/metrics" | tr -d '\n' || echo null)"
        echo "}"
    } >"$out" 2>/dev/null
}

snapshot_lsof() {
    local label="$1"
    local pid
    pid=$(pgrep -f 'JuiceMount.app/Contents/MacOS/JuiceMount' | head -1)
    if [[ -n "$pid" ]]; then
        lsof -nP -p "$pid" 2>/dev/null > "${PHASE_DIR}/lsof-${label}.txt"
    fi
}

snapshot_rss() {
    local label="$1"
    local pid
    pid=$(pgrep -f 'JuiceMount.app/Contents/MacOS/JuiceMount' | head -1)
    if [[ -n "$pid" ]]; then
        ps -o pid,rss,vsz,%cpu,etime,command -p "$pid" 2>/dev/null > "${PHASE_DIR}/rss-${label}.txt"
    fi
}

# Generate-once + reuse pattern: a 256 MiB random pool used as the source for
# many tests. Generating /dev/urandom is slow (~1 GiB/s on M1+), so we share.
SHARED_POOL="${TMPDIR_LOCAL}/jmqa-pool-256MiB"

ensure_shared_pool() {
    if [[ ! -f "$SHARED_POOL" ]] || (( $(stat -f%z "$SHARED_POOL" 2>/dev/null) < 268435456 )); then
        dd if=/dev/urandom of="$SHARED_POOL" bs=1M count=256 2>/dev/null
    fi
}

# Take a slice of the shared pool of arbitrary size (multiples of 1 MiB).
# pool_slice <out_path> <size_mib>
pool_slice() {
    local out="$1" mib="$2"
    ensure_shared_pool
    dd if="$SHARED_POOL" of="$out" bs=1M count="$mib" 2>/dev/null
}

# md5q: md5 -q wrapper that returns "" on missing files instead of erroring
md5q() { md5 -q "$1" 2>/dev/null || echo ""; }

# Path helpers
sz()  { stat -f%z "$1" 2>/dev/null || echo 0; }

# Restart JuiceMount (used by orchestrator after failure-injection phases)
restart_juicemount() {
    info "restarting JuiceMount…"
    pkill -TERM -f 'JuiceMount.app/Contents/MacOS/JuiceMount' 2>/dev/null
    sleep 3
    open "/Users/LelandDutcher/Developer/JuiceMount6/build/JuiceMount.app"
    sleep 5
    if wait_for_mount_back 60; then
        info "JuiceMount restarted and healthy"
        return 0
    fi
    warn "JuiceMount restart did not return to healthy within 60s"
    return 1
}

# ---------------------------------------------------------------------------
# Tool availability checks

has_cmd() { command -v "$1" >/dev/null 2>&1; }
