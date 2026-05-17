#!/usr/bin/env bash
# 09-endurance.sh — sustained load + leak detection.
# Target: ~25 min.
#
# Runs a steady write+read mix for 20 minutes while sampling:
#   - RSS / VSZ (memory leak detection)
#   - file descriptor count
#   - JuiceMount /metrics counters (per-RPC latencies)
#
# Fails if RSS grows > 50% from baseline, or fd count grows monotonically.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

phase_init "09-endurance"

TROOT="$MOUNT/.jmqa-endurance-$$"
mkdir -p "$TROOT"

PID=$(pgrep -f 'JuiceMount.app/Contents/MacOS/JuiceMount' | head -1)
if [[ -z "$PID" ]]; then
    fail "JuiceMount not running — can't run endurance phase"
    phase_report
    exit 0
fi

SAMPLES="${PHASE_DIR}/samples.tsv"
echo -e "ts\trss_kb\tvsz_kb\tfd_count\trpc_total\trpc_errors" > "$SAMPLES"

# Background sampler — every 10s
SAMPLER_PID=""
(
    while true; do
        ts=$(date +%s)
        ps_line=$(ps -o rss=,vsz= -p "$PID" 2>/dev/null)
        rss=$(echo "$ps_line" | awk '{print $1}')
        vsz=$(echo "$ps_line" | awk '{print $2}')
        fd_count=$(lsof -nP -p "$PID" 2>/dev/null | wc -l | tr -d ' ')
        metrics=$(curl -s --max-time 2 "http://${JM_METRICS_ADDR}/metrics" 2>/dev/null)
        rpc_total=$(echo "$metrics" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('rpc_total','0'))" 2>/dev/null || echo 0)
        rpc_errors=$(echo "$metrics" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('rpc_errors','0'))" 2>/dev/null || echo 0)
        echo -e "$ts\t${rss:-0}\t${vsz:-0}\t${fd_count:-0}\t${rpc_total:-0}\t${rpc_errors:-0}" >> "$SAMPLES"
        sleep 10
    done
) &
SAMPLER_PID=$!

# 4 background writers each writing 10 MiB on a loop for ~20 min
WORKER_PIDS=()
DURATION="${ENDURANCE_DURATION:-1200}"  # 20 min default; override for shorter run
log "starting 4 writer workers + 4 reader workers for ${DURATION}s"

# Pre-stage 4 read targets
for i in 1 2 3 4; do
    pool_slice "$TROOT/read-${i}.bin" 50
done

START=$(date +%s)
END=$(( START + DURATION ))

# Writer workers
for w in 1 2 3 4; do
    (
        local_src="$TMPDIR_LOCAL/end-w-$$-$w"
        pool_slice "$local_src" 10
        i=0
        while (( $(date +%s) < END )); do
            cp "$local_src" "$TROOT/writer-${w}-${i}.bin" 2>/dev/null
            sleep 1  # don't pin the CPU; this is endurance, not throughput
            # Delete some old ones to keep disk usage bounded
            if (( i > 20 )); then
                rm -f "$TROOT/writer-${w}-$((i-20)).bin" 2>/dev/null
            fi
            i=$((i+1))
        done
        rm -f "$local_src" 2>/dev/null
    ) &
    WORKER_PIDS+=($!)
done

# Reader workers
for r in 1 2 3 4; do
    (
        target="$TROOT/read-${r}.bin"
        while (( $(date +%s) < END )); do
            dd if="$target" of=/dev/null bs=1M 2>/dev/null
            sleep 1
        done
    ) &
    WORKER_PIDS+=($!)
done

log "endurance loop running — will run for ${DURATION}s"
log "tailing first samples; full log at $SAMPLES"
# Print first few samples as they land
SHOW=0
while (( $(date +%s) < END )); do
    sleep 60
    SHOW=$((SHOW+1))
    elapsed=$(( $(date +%s) - START ))
    last=$(tail -1 "$SAMPLES")
    log "endurance ${elapsed}/${DURATION}s — last sample: $last"
done

# Cleanup workers
for pid in "${WORKER_PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
sleep 2
for pid in "${WORKER_PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
kill "$SAMPLER_PID" 2>/dev/null || true
wait "$SAMPLER_PID" 2>/dev/null || true

# ---------------------------------------------------------------------------
section "analyze: RSS drift"
FIRST_RSS=$(awk 'NR==2{print $2}' "$SAMPLES")
LAST_RSS=$(awk 'END{print $2}' "$SAMPLES")
MAX_RSS=$(awk 'NR>1{if($2+0>m+0) m=$2} END{print m}' "$SAMPLES")
log "RSS: first=${FIRST_RSS}KB last=${LAST_RSS}KB max=${MAX_RSS}KB"
if [[ -n "$FIRST_RSS" && -n "$LAST_RSS" ]]; then
    DELTA_PCT=$(( (LAST_RSS - FIRST_RSS) * 100 / FIRST_RSS ))
    log "RSS growth: ${DELTA_PCT}%"
    if (( DELTA_PCT < 50 )); then
        pass "RSS growth ${DELTA_PCT}% under 50% threshold"
    else
        fail "RSS growth ${DELTA_PCT}% — potential memory leak"
    fi
fi

section "analyze: fd count drift"
FIRST_FD=$(awk 'NR==2{print $4}' "$SAMPLES")
LAST_FD=$(awk 'END{print $4}' "$SAMPLES")
MAX_FD=$(awk 'NR>1{if($4+0>m+0) m=$4} END{print m}' "$SAMPLES")
log "fd: first=${FIRST_FD} last=${LAST_FD} max=${MAX_FD}"
if [[ -n "$FIRST_FD" && -n "$LAST_FD" ]]; then
    FD_DELTA=$((LAST_FD - FIRST_FD))
    if (( FD_DELTA < 50 )); then
        pass "fd count drift ${FD_DELTA} — no leak"
    else
        fail "fd count grew by ${FD_DELTA} — fd leak likely"
    fi
fi

section "analyze: RPC errors"
FIRST_ERR=$(awk 'NR==2{print $6}' "$SAMPLES")
LAST_ERR=$(awk 'END{print $6}' "$SAMPLES")
ERR_DELTA=$((LAST_ERR - FIRST_ERR))
log "rpc_errors: first=${FIRST_ERR} last=${LAST_ERR} delta=${ERR_DELTA}"
if (( ERR_DELTA == 0 )); then
    pass "no RPC errors during endurance"
elif (( ERR_DELTA < 10 )); then
    warn "$ERR_DELTA RPC errors during endurance (acceptable transient)"
else
    fail "$ERR_DELTA RPC errors — investigate"
fi

snapshot_metrics "post-endurance"
snapshot_lsof "post-endurance"
snapshot_rss "post-endurance"
rm -rf "$TROOT" 2>/dev/null
phase_report
