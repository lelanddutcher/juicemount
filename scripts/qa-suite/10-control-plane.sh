#!/usr/bin/env bash
# 10-control-plane.sh — HTTP endpoints, pin store, search.
# Target: ~10 min.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

phase_init "10-control-plane"

# ---------------------------------------------------------------------------
section "endpoint smoke (each must return 2xx and parse)"
ENDPOINTS=(
    /health
    /metrics
    /offline
    /cache/status
    /selftest
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
section "search endpoint"
if has_cmd python3; then
    RESP=$(curl -s --max-time 10 -G \
        --data-urlencode "q=." \
        --data-urlencode "limit=10" \
        "http://${JM_METRICS_ADDR}/search" 2>/dev/null)
    if [[ -n "$RESP" ]] && echo "$RESP" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
        N=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('results',[])) if isinstance(d, dict) else len(d))" 2>/dev/null || echo 0)
        pass "/search returned valid JSON ($N results for '.')"
    else
        warn "/search returned non-JSON or empty: $(echo "$RESP" | head -c 200)"
    fi
fi

# ---------------------------------------------------------------------------
section "pin/unpin cycle for a transient file"
PINTEST="$MOUNT/.jmqa-pin-$$"
pool_slice "$PINTEST" 5
PIN_REL="/.jmqa-pin-$$"
# Pin
PIN_RESP=$(curl -s --max-time 10 -X POST "http://${JM_METRICS_ADDR}/pin" \
    -H "Content-Type: application/json" \
    -d "{\"paths\":[\"$PIN_REL\"]}" 2>/dev/null)
if [[ -n "$PIN_RESP" ]]; then
    pass "/pin accepted request: $(echo "$PIN_RESP" | head -c 100)"
else
    fail "/pin returned empty"
fi
sleep 2
# Cache status should reflect pinned bytes
CACHE_BEFORE=$(curl -s --max-time 5 "http://${JM_METRICS_ADDR}/cache/status" 2>/dev/null)
log "cache/status: $(echo "$CACHE_BEFORE" | head -c 200)"
# Unpin
UNPIN_RESP=$(curl -s --max-time 10 -X POST "http://${JM_METRICS_ADDR}/unpin" \
    -H "Content-Type: application/json" \
    -d "{\"paths\":[\"$PIN_REL\"]}" 2>/dev/null)
if [[ -n "$UNPIN_RESP" ]]; then
    pass "/unpin accepted"
else
    fail "/unpin returned empty"
fi
rm -f "$PINTEST"

# ---------------------------------------------------------------------------
section "self-test endpoint"
ST=$(curl -s --max-time 30 -X POST "http://${JM_METRICS_ADDR}/selftest?force=true" 2>/dev/null)
if [[ -n "$ST" ]]; then
    if echo "$ST" | grep -q '"write_ok"'; then
        pass "/selftest force returned with write_ok present"
        echo "$ST" > "${PHASE_DIR}/selftest.json"
    else
        warn "/selftest response unexpected: $(echo "$ST" | head -c 200)"
    fi
else
    fail "/selftest returned empty"
fi

snapshot_metrics "post-control-plane"
phase_report
