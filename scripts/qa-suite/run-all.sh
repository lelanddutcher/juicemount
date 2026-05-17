#!/usr/bin/env bash
# run-all.sh — orchestrator for the JuiceMount QA suite.
#
# Runs phases 00 through 10 in order, with health-recovery between phases.
# Total wall-clock target: ~3 hours. The agent monitors via a separate
# /loop with intervals ~15 minutes.
#
# Usage:
#   bash scripts/qa-suite/run-all.sh                    # full 3-hour run
#   QUICK=1 bash scripts/qa-suite/run-all.sh            # short run (skip 04-fio + 09-endurance long parts)
#   ENDURANCE_DURATION=600 bash scripts/qa-suite/run-all.sh   # shorter endurance
#   PHASES="01-smoke 06-concurrency" bash scripts/qa-suite/run-all.sh  # specific phases only
#   STOP_ON_FAIL=1 bash scripts/qa-suite/run-all.sh     # halt at first phase with FAIL>0
#
# Artifacts: /tmp/jm-qa-artifacts/<run-id>/
#   - run.log             (this orchestrator's output)
#   - run.summary         (final pass/fail counts per phase)
#   - <phase>/.summary    (per-phase counts)
#   - <phase>/*.{log,json,txt,md5,tsv}  (per-test artifacts)

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

ORCHESTRATOR_LOG="${RUN_DIR}/run.log"
exec > >(tee -a "$ORCHESTRATOR_LOG") 2>&1

log "============================================================"
log "JuiceMount QA Suite — run id: $RUN_ID"
log "artifacts: $RUN_DIR"
log "mount: $MOUNT"
log "============================================================"

ALL_PHASES=(
    00-precheck
    01-smoke
    02-finder
    03-media
    04-fio
    05-metadata
    06-concurrency
    07-failure
    08-netshape
    09-endurance
    10-control-plane
)

# Adjust phases via PHASES env or QUICK mode
if [[ -n "${PHASES:-}" ]]; then
    read -ra PHASES_TO_RUN <<< "$PHASES"
elif [[ -n "${QUICK:-}" ]]; then
    PHASES_TO_RUN=(00-precheck 01-smoke 02-finder 05-metadata 06-concurrency 10-control-plane)
    export ENDURANCE_DURATION="${ENDURANCE_DURATION:-120}"
    log "QUICK mode — running ${PHASES_TO_RUN[*]} only"
else
    PHASES_TO_RUN=("${ALL_PHASES[@]}")
fi

SUITE_START=$(date +%s)
# Bash 3.2 compatible: per-phase counters in parallel indexed arrays.
PHASE_NAMES=()
PHASE_PASS_LIST=()
PHASE_FAIL_LIST=()
PHASE_WARN_LIST=()
PHASE_ELAPSED_LIST=()

# Run each phase, recovering between if mount unhealthy
for phase in "${PHASES_TO_RUN[@]}"; do
    log ""
    log "############################################################"
    log "## PHASE $phase starting at $(date -Iseconds)"
    log "############################################################"

    # Pre-phase health check (skip for 00-precheck — it does its own)
    if [[ "$phase" != "00-precheck" ]]; then
        if ! mount_is_nfs || ! jm_is_healthy || jm_auto_offline_engaged; then
            warn "mount unhealthy before phase $phase — attempting restart"
            restart_juicemount || true
            sleep 5
            if ! mount_is_nfs || ! jm_is_healthy; then
                warn "mount STILL unhealthy after restart — phase $phase may degrade or skip itself"
            fi
        fi
    fi

    PHASE_SCRIPT="$SCRIPT_DIR/${phase}.sh"
    if [[ ! -x "$PHASE_SCRIPT" ]]; then
        chmod +x "$PHASE_SCRIPT" 2>/dev/null || true
    fi
    if [[ ! -f "$PHASE_SCRIPT" ]]; then
        warn "phase script $PHASE_SCRIPT missing — skipping"
        continue
    fi

    # Run the phase (it always exits 0; pass/fail is in its .summary file)
    bash "$PHASE_SCRIPT" 2>&1 | tee -a "${RUN_DIR}/${phase}.stdout"

    # Read summary
    SUM_FILE="${RUN_DIR}/${phase}/.summary"
    if [[ -f "$SUM_FILE" ]]; then
        p_pass=$(grep '^pass=' "$SUM_FILE" | cut -d= -f2)
        p_fail=$(grep '^fail=' "$SUM_FILE" | cut -d= -f2)
        p_warn=$(grep '^warn=' "$SUM_FILE" | cut -d= -f2)
        p_elapsed=$(grep '^elapsed_sec=' "$SUM_FILE" | cut -d= -f2)
    else
        p_pass=0
        p_fail=99
        p_warn=0
        p_elapsed=0
        warn "phase $phase produced no summary"
    fi
    PHASE_NAMES+=("$phase")
    PHASE_PASS_LIST+=("${p_pass:-0}")
    PHASE_FAIL_LIST+=("${p_fail:-0}")
    PHASE_WARN_LIST+=("${p_warn:-0}")
    PHASE_ELAPSED_LIST+=("${p_elapsed:-0}")

    log "## PHASE $phase: pass=${p_pass:-0} fail=${p_fail:-0} warn=${p_warn:-0} elapsed=${p_elapsed:-0}s"

    if [[ -n "${STOP_ON_FAIL:-}" && "${p_fail:-0}" -gt 0 ]]; then
        log "STOP_ON_FAIL set and phase $phase had failures — halting suite"
        break
    fi
done

# ---------------------------------------------------------------------------
SUITE_ELAPSED=$(( $(date +%s) - SUITE_START ))
SUMMARY_FILE="${RUN_DIR}/run.summary"

{
    echo "JuiceMount QA Suite — final report"
    echo "==================================="
    echo "Run ID: $RUN_ID"
    echo "Started: $(date -r $SUITE_START -Iseconds 2>/dev/null || date -Iseconds)"
    echo "Finished: $(date -Iseconds)"
    echo "Wall-clock: ${SUITE_ELAPSED}s ($(( SUITE_ELAPSED / 60 ))min)"
    echo ""
    echo "Per-phase:"
    printf "%-22s %6s %6s %6s %8s\n" "phase" "pass" "fail" "warn" "elapsed"
    TOTAL_PASS=0; TOTAL_FAIL=0; TOTAL_WARN=0
    for i in "${!PHASE_NAMES[@]}"; do
        phase="${PHASE_NAMES[$i]}"
        p="${PHASE_PASS_LIST[$i]:-0}"
        f="${PHASE_FAIL_LIST[$i]:-0}"
        w="${PHASE_WARN_LIST[$i]:-0}"
        e="${PHASE_ELAPSED_LIST[$i]:-0}"
        printf "%-22s %6s %6s %6s %7ss\n" "$phase" "$p" "$f" "$w" "$e"
        TOTAL_PASS=$((TOTAL_PASS + p))
        TOTAL_FAIL=$((TOTAL_FAIL + f))
        TOTAL_WARN=$((TOTAL_WARN + w))
    done
    echo ""
    printf "%-22s %6s %6s %6s\n" "TOTAL" "$TOTAL_PASS" "$TOTAL_FAIL" "$TOTAL_WARN"
    echo ""
    if (( TOTAL_FAIL == 0 )); then
        echo "VERDICT: ✓ ALL PHASES PASS"
    else
        echo "VERDICT: ✗ $TOTAL_FAIL failures across phases"
    fi
    echo ""
    echo "Artifacts: $RUN_DIR"
} | tee "$SUMMARY_FILE"

# Final exit code: 0 if no failures, 1 otherwise
if (( TOTAL_FAIL == 0 )); then
    exit 0
else
    exit 1
fi
