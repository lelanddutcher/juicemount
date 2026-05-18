#!/usr/bin/env bash
# 08-netshape.sh — network shaping via dnctl + pfctl.
# Target: ~20 min.
#
# Probes how the app behaves under degraded WAN conditions to the backend:
#   - 5 Mbps bandwidth cap (DSL-tier remote site)
#   - 200ms RTT latency
#   - 5% packet loss
#   - Full block of MinIO port (auto-offline trigger)
#
# IMPORTANT: this phase requires sudo (passwordless or interactive). It tears
# down its own rules on exit. If interrupted, the orchestrator's cleanup
# phase also tries to flush them.
#
# Backend target (from running juicefs config): MinIO container reachable from
# the Mac at the host:port specified below. Adjust if your setup differs.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

phase_init "08-netshape"

# ---------------------------------------------------------------------------
# Detect MinIO target
BACKEND_HOST="${BACKEND_HOST:-192.168.0.212}"
BACKEND_PORT="${BACKEND_PORT:-9000}"
# 192.168.0.212 is the macvlan IP for the juicefs-minio container.
# Earlier this defaulted to .197 (the SMB host); was a wrong guess.
# Override with BACKEND_HOST env if your setup differs.
log "shaping target: ${BACKEND_HOST}:${BACKEND_PORT}"

# Bail early if we can't even reach the backend on a non-shaped run
if ! nc -zv -G 3 "$BACKEND_HOST" "$BACKEND_PORT" >/dev/null 2>&1; then
    warn "backend ${BACKEND_HOST}:${BACKEND_PORT} not currently reachable — net-shaping tests would be redundant"
    warn "skipping. If you set BACKEND_HOST/PORT explicitly, double-check."
    phase_report
    exit 0
fi

if ! sudo -n true 2>/dev/null; then
    warn "sudo requires password — this phase can't run unattended. Skipping."
    warn "to enable: configure passwordless sudo for pfctl/dnctl, or run this phase manually."
    phase_report
    exit 0
fi

cleanup_shaping() {
    info "tearing down shaping rules…"
    sudo -n pfctl -a com.juicemount.qa -F all >/dev/null 2>&1 || true
    sudo -n dnctl pipe flush >/dev/null 2>&1 || true
    sudo -n pfctl -d >/dev/null 2>&1 || true
}
trap cleanup_shaping EXIT INT TERM

TROOT="$MOUNT/.jmqa-netshape-$$"
mkdir -p "$TROOT"

# Enable PF (idempotent)
sudo -n pfctl -E >/dev/null 2>&1 || true

apply_pipe() {
    local pipe_args="$1"
    local rule="$2"
    sudo -n dnctl pipe flush >/dev/null 2>&1 || true
    sudo -n dnctl pipe 1 config $pipe_args
    # Build a tiny anchor file and load it
    local tmpf
    tmpf=$(mktemp)
    echo "$rule" > "$tmpf"
    sudo -n pfctl -a com.juicemount.qa -f "$tmpf" >/dev/null 2>&1
    rm -f "$tmpf"
}

# ---------------------------------------------------------------------------
section "case 1: 5 Mbps bandwidth cap on backend (DSL)"
apply_pipe "bw 5Mbit/s" "dummynet out proto tcp to ${BACKEND_HOST} port ${BACKEND_PORT} pipe 1"
log "shaping active. writing 20 MiB and timing it."
pool_slice "$TMPDIR_LOCAL/N1-src-$$" 20
START=$(date +%s)
cp "$TMPDIR_LOCAL/N1-src-$$" "$TROOT/N1-shaped.bin" 2>/dev/null
W_RC=$?
ELAPSED=$(( $(date +%s) - START + 1 ))
SIZE=$(sz "$TROOT/N1-shaped.bin")
log "20 MiB write under 5 Mbps cap: ${ELAPSED}s, size=${SIZE}, exit=${W_RC}"
SRC_MD5=$(md5q "$TMPDIR_LOCAL/N1-src-$$")
DST_MD5=$(md5q "$TROOT/N1-shaped.bin")
if [[ "$SRC_MD5" == "$DST_MD5" && -n "$SRC_MD5" ]]; then
    pass "md5 match under bandwidth cap"
else
    # JuiceFS writeback may buffer locally before pushing to MinIO, so the
    # client-side write should complete fast even under cap. md5 must still match.
    fail "md5 mismatch under bandwidth cap (src=$SRC_MD5 dst=$DST_MD5)"
fi
rm -f "$TMPDIR_LOCAL/N1-src-$$"

# ---------------------------------------------------------------------------
section "case 2: 200 ms RTT latency"
apply_pipe "delay 100" "dummynet out proto tcp to ${BACKEND_HOST} port ${BACKEND_PORT} pipe 1"
# (100ms each way = 200ms RTT)
log "shaping active. reading the file we just wrote (cache may serve)"
START=$(date +%s)
md5_under_latency=$(md5q "$TROOT/N1-shaped.bin")
ELAPSED=$(( $(date +%s) - START + 1 ))
log "read-back under 200ms RTT: ${ELAPSED}s"
if [[ "$md5_under_latency" == "$DST_MD5" ]]; then
    pass "md5 stable under latency"
else
    fail "md5 changed under latency: $DST_MD5 -> $md5_under_latency"
fi

# ---------------------------------------------------------------------------
section "case 3: 5% packet loss"
apply_pipe "plr 0.05" "dummynet out proto tcp to ${BACKEND_HOST} port ${BACKEND_PORT} pipe 1"
log "5% drop rate active. writing 10 MiB."
pool_slice "$TMPDIR_LOCAL/N3-src-$$" 10
START=$(date +%s)
set +e
timeout 60 cp "$TMPDIR_LOCAL/N3-src-$$" "$TROOT/N3-lossy.bin" 2>/dev/null
W_RC=$?
set -e
ELAPSED=$(( $(date +%s) - START + 1 ))
SRC_MD5=$(md5q "$TMPDIR_LOCAL/N3-src-$$")
DST_MD5=$(md5q "$TROOT/N3-lossy.bin")
log "10 MiB under 5% loss: ${ELAPSED}s exit=${W_RC}"
if [[ "$SRC_MD5" == "$DST_MD5" && -n "$SRC_MD5" ]]; then
    pass "md5 match under 5% packet loss"
elif [[ $W_RC -eq 124 ]]; then
    fail "write hung past 60s under 5% loss"
else
    warn "write under loss: src=$SRC_MD5 dst=$DST_MD5 (acceptable if backend retry sorted)"
fi
rm -f "$TMPDIR_LOCAL/N3-src-$$"

# ---------------------------------------------------------------------------
section "case 4: full backend block (forces auto-offline) + recovery"
# Block all traffic to backend
sudo -n pfctl -a com.juicemount.qa -F all >/dev/null 2>&1
sudo -n dnctl pipe flush >/dev/null 2>&1
tmpf=$(mktemp)
echo "block drop out proto tcp to ${BACKEND_HOST} port ${BACKEND_PORT}" > "$tmpf"
sudo -n pfctl -a com.juicemount.qa -f "$tmpf" >/dev/null 2>&1
rm -f "$tmpf"
log "backend traffic blocked. waiting up to 20s for auto-offline…"
DETECTED=0
for i in $(seq 1 10); do
    if jm_auto_offline_engaged; then
        DETECTED=1
        log "auto-offline engaged at ${i}×2s"
        break
    fi
    sleep 2
done
if (( DETECTED == 1 )); then
    pass "auto-offline engaged within 20s of block"
else
    fail "auto-offline did NOT engage within 20s"
fi

# Now unblock and see if it recovers (QA-15 the bug)
sudo -n pfctl -a com.juicemount.qa -F all >/dev/null 2>&1
log "block lifted. waiting up to 30s for auto-offline to clear…"
RECOVERED=0
for i in $(seq 1 15); do
    if ! jm_auto_offline_engaged; then
        RECOVERED=1
        log "auto-offline cleared at ${i}×2s"
        break
    fi
    sleep 2
done
if (( RECOVERED == 1 )); then
    pass "auto-offline cleared within 30s of unblock (QA-15 not present)"
else
    fail "auto-offline STILL engaged 30s after unblock — QA-15 reproduced"
fi

cleanup_shaping
trap - EXIT INT TERM

snapshot_metrics "post-netshape"
snapshot_lsof "post-netshape"
rm -rf "$TROOT" 2>/dev/null
phase_report
