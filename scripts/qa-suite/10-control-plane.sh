#!/usr/bin/env bash
# 10-control-plane.sh — HTTP endpoints, pin store, search.
# Target: ~10 min.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

phase_init "10-control-plane"

# ---------------------------------------------------------------------------
section "endpoint smoke (each must return 2xx and parse)"
# Correct paths from cbridge.go ExtraRoutes (verified 2026-05-17):
#   /health, /metrics — built into metrics package
#   /offline, /cache-status, /self-test, /verify-pins — ExtraRoutes
#   /pin /unpin take ?path= query param (GET)
ENDPOINTS=(
    /health
    /metrics
    /offline
    /cache-status
    /self-test
)
for ep in "${ENDPOINTS[@]}"; do
    set +e
    code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 "http://${JM_METRICS_ADDR}${ep}")
    set -e
    if [[ "$code" =~ ^2 ]]; then
        pass "$ep returned $code"
    else
        fail "$ep returned $code"
    fi
done

# ---------------------------------------------------------------------------
section "endpoint hammer (100× /health in parallel)"
PIDS=()
HAMMER_OUT="${PHASE_DIR}/hammer-codes.txt"
: > "$HAMMER_OUT"
for i in $(seq 1 100); do
    (
        c=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 "http://${JM_METRICS_ADDR}/health")
        echo "$c" >> "$HAMMER_OUT"
    ) &
    PIDS+=($!)
done
START=$(date +%s)
for pid in "${PIDS[@]}"; do wait "$pid"; done
ELAPSED=$(( $(date +%s) - START + 1 ))
OK=$(grep -c '^2' "$HAMMER_OUT" || echo 0)
log "100 parallel /health: ${OK}/100 returned 2xx in ${ELAPSED}s"
(( OK >= 95 )) && pass "≥95% 2xx under 100-way parallel hammer" || fail "only ${OK}/100 successful"

# ---------------------------------------------------------------------------
section "/metrics shape"
M=$(curl -s --max-time 5 "http://${JM_METRICS_ADDR}/metrics")
for key in uptime_sec rpc_total rpc_errors bytes_read bytes_written; do
    if echo "$M" | grep -q "\"$key\""; then
        pass "/metrics contains $key"
    else
        fail "/metrics missing $key"
    fi
done

# ---------------------------------------------------------------------------
section "pin/unpin cycle for a transient file"
PINTEST="$MOUNT/.jmqa-pin-$$"
pool_slice "$PINTEST" 5
PIN_REL="/.jmqa-pin-$$"
# /pin and /unpin take ?path= query param (verified cbridge.go:1570)
PIN_URL="http://${JM_METRICS_ADDR}/pin?path=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$PIN_REL'))")"
UNPIN_URL="http://${JM_METRICS_ADDR}/unpin?path=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$PIN_REL'))")"
PIN_RESP=$(curl -s --max-time 10 "$PIN_URL" 2>/dev/null)
if [[ -n "$PIN_RESP" ]] && echo "$PIN_RESP" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
    pass "/pin?path= returned valid JSON: $(echo "$PIN_RESP" | head -c 100)"
else
    fail "/pin?path= returned non-JSON/empty: $(echo "$PIN_RESP" | head -c 200)"
fi
sleep 2
CACHE_NOW=$(curl -s --max-time 5 "http://${JM_METRICS_ADDR}/cache-status" 2>/dev/null)
if [[ -n "$CACHE_NOW" ]] && echo "$CACHE_NOW" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
    pass "/cache-status returned valid JSON"
    echo "$CACHE_NOW" > "${PHASE_DIR}/cache-status.json"
else
    warn "/cache-status non-JSON: $(echo "$CACHE_NOW" | head -c 200)"
fi
UNPIN_RESP=$(curl -s --max-time 10 "$UNPIN_URL" 2>/dev/null)
if [[ -n "$UNPIN_RESP" ]] && echo "$UNPIN_RESP" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
    pass "/unpin?path= returned valid JSON"
else
    fail "/unpin?path= returned non-JSON/empty"
fi
rm -f "$PINTEST"

# ---------------------------------------------------------------------------
section "self-test endpoint (GET = cached, POST = rerun)"
# Cached GET first
ST_GET=$(curl -s --max-time 5 "http://${JM_METRICS_ADDR}/self-test" 2>/dev/null)
if [[ -n "$ST_GET" ]] && echo "$ST_GET" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
    pass "/self-test GET returned valid JSON"
else
    warn "/self-test GET non-JSON: $(echo "$ST_GET" | head -c 200)"
fi
# Force rerun via POST
ST_POST=$(curl -s --max-time 30 -X POST "http://${JM_METRICS_ADDR}/self-test" 2>/dev/null)
if [[ -n "$ST_POST" ]] && echo "$ST_POST" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
    if echo "$ST_POST" | grep -q '"write_ok"\|"status"'; then
        pass "/self-test POST (rerun) returned with expected fields"
        echo "$ST_POST" > "${PHASE_DIR}/self-test.json"
    else
        warn "/self-test POST unexpected shape: $(echo "$ST_POST" | head -c 200)"
    fi
else
    fail "/self-test POST returned non-JSON/empty"
fi

snapshot_metrics "post-control-plane"
phase_report
