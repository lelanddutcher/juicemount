#!/usr/bin/env bash
# Category11: paired enumeration/stat throughout an original Finder load and
# spool drain. Measures syscalls, not Finder paint or exclusive server work.
# Unknown Finder completion retains this original owner and data; no kill/retry.
set -uo pipefail
DRAIN_HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DRAIN_HERE/lib.sh"
source "$DRAIN_HERE/snappiness_custody.sh"
QA_CAT=11-drain-latency
DRAIN_DIRS="${DRAIN_DIRS:-8}"
DRAIN_FILES_PER_DIR="${DRAIN_FILES_PER_DIR:-40}"
DRAIN_FILE_BYTES="${DRAIN_FILE_BYTES:-8388608}"
if [ "${QUICK:-0}" = 1 ]; then
    DRAIN_DIRS="${DRAIN_DIRS_QUICK:-4}"
    DRAIN_FILES_PER_DIR="${DRAIN_FILES_PER_DIR_QUICK:-20}"
    DRAIN_FILE_BYTES="${DRAIN_FILE_BYTES_QUICK:-2097152}"
fi
CACHE_PROBE_FILES="${CACHE_PROBE_FILES:-200}"
CACHE_PROBE_BYTES="${CACHE_PROBE_BYTES:-4096}"
DRAIN_SAMPLE_PAUSE_MS="${DRAIN_SAMPLE_PAUSE_MS:-100}"
DRAIN_WINDOW_MAX_S="${DRAIN_WINDOW_MAX_S:-$QA_DRAIN_TIMEOUT}"
DRAIN_MIN_SAMPLES="${DRAIN_MIN_SAMPLES:-40}"
SNAP_FINDER_TIMEOUT=$((DRAIN_WINDOW_MAX_S+600))
SNAP_PENDING=0 SNAP_HOLD=0 SNAP_SOFT_RED=0 SNAP_SIGNALLED=0
SNAP_SOFT_REASON="" SNAP_COPY_PIDS="" SNAP_FORCED_OFFLINE=0
SNAP_TMP="$QA_STAGE/drain-ipc-$$"
trap snap_on_exit EXIT
trap snap_signal INT TERM

drain_evidence() { /usr/bin/python3 -I -B "$DRAIN_HERE/drain_latency_evidence.py" "$@"; }
drain_get() { qa_timeout 5 curl -fsS "$CP_BASE/$1" > "$2"; }

drain_spool() {
    local path="$1"
    drain_get spool "$path" || return 1
    printf '%s\n' "$path" >> "$QA_CAT_DIR/spool-observations.txt" || return 1
    drain_evidence spool "$path" "${DRAIN_LAST_SPOOL:-}" || return 1
    DRAIN_LAST_SPOOL="$path"
}

drain_settle() {
    local prefix="$1" bound="$2" until=$(( $(date +%s)+$2 )) n=0
    while :; do
        n=$((n+1))
        if ! drain_spool "$prefix-$n.json"; then
            qa_fail 'typed settle observation failed'; return 1
        fi
        drain_evidence settled "$prefix-$n.json" 2>/dev/null && return 0
        [ "$(date +%s)" -lt "$until" ] || { qa_fail 'typed settle deadline exceeded'; return 1; }
        sleep 1
    done
}

drain_custody() {
    local man="$1" dest="$2" dirs="$3" files="$4" label="$5"
    drain_evidence manifest "$man" "$dirs" "$files" > "$QA_CAT_DIR/$label.expected.json" || return 1
    qa_verify_custody "$man" "$dest" > "$QA_CAT_DIR/$label.custody.log" 2>&1
}

drain_pair() {
    local prefix="$1" rc=0
    # All original synchronous observers are joined; failure remains sticky and
    # never abandons the already-issued Finder load. Preserve partial evidence.
    drain_spool "$prefix.spool-before.json" || rc=1
    drain_get metrics "$prefix.metrics-before.json" || rc=1
    drain_evidence observe list "$PROBE_DIR" > "$prefix.list.json" 2> "$prefix.list.error" || rc=1
    drain_get metrics "$prefix.metrics-middle.json" || rc=1
    drain_evidence observe stat "$PROBE_FILE" > "$prefix.stat.json" 2> "$prefix.stat.error" || rc=1
    drain_get metrics "$prefix.metrics-after.json" || rc=1
    drain_spool "$prefix.spool-after.json" || rc=1
    if [ "$rc" = 0 ]; then
        drain_evidence pair "$prefix" "$QA_CAT_DIR/probe.expected.json" "$CACHE_PROBE_BYTES" > "$prefix.validation.log" 2>&1 || rc=1
    fi
    return "$rc"
}

drain_window() {
    local pid="$1" deadline="$2" n=0 rc=0 ended=window-cap
    : > "$QA_CAT_DIR/pairs.tsv" || { snap_soft_deadline 'pair index publication failed'; rc=1; }
    while :; do
        # Completion never pads a sample floor. The only valid endpoint is an
        # actual positive reply AND a typed settled spool, not worker liveness.
        if [ -f "$SNAP_TMP/load.rc" ]; then
            snap_reply_positive "$(cat "$SNAP_TMP/load.rc")" "$(cat "$SNAP_TMP/load.out" 2>/dev/null)" ||
                snap_hold_unknown 'load lacks positive original reply'
            if ! drain_spool "$QA_CAT_DIR/end-$n.json"; then
                qa_fail 'end-of-window spool request failed or invalid'; rc=1
            elif drain_evidence settled "$QA_CAT_DIR/end-$n.json" 2>/dev/null; then
                ended=copy-and-spool-complete; break
            fi
            # Malformed observations are RED, not equivalent to a pending spool.
            if ! drain_evidence spool "$QA_CAT_DIR/end-$n.json" 2>/dev/null; then
                qa_fail 'invalid end-of-window spool'; rc=1
            fi
        fi
        if [ "$(date +%s)" -ge "$deadline" ]; then
            snap_soft_deadline 'drain measurement window cap'; rc=1; break
        fi
        n=$((n+1))
        if drain_pair "$QA_CAT_DIR/pair-$n"; then
            printf '%s\tvalid\n' "$n" >> "$QA_CAT_DIR/pairs.tsv" || rc=1
        else
            printf '%s\tinvalid\n' "$n" >> "$QA_CAT_DIR/pairs.tsv" || true
            qa_fail "paired observer failed at $n; retaining evidence and original load"; rc=1
        fi
        perl -e 'select undef,undef,undef,shift()/1000' "$DRAIN_SAMPLE_PAUSE_MS"
    done
    # Before any fallible summary/custody reporting, join this SAME original.
    snap_join_positive load "$pid" "$deadline"
    snap_flush_soft_reason
    printf '%s\n' "$ended" > "$QA_CAT_DIR/window-end" || rc=1
    drain_settle "$QA_CAT_DIR/load-settle" "$LOAD_CEIL" || { SNAP_HOLD=1; rc=1; }
    drain_get metrics "$QA_CAT_DIR/window-after.json" || { qa_fail 'window-end metrics failed'; rc=1; }
    return "$rc"
}

drain_run() {
    local value probe_stage probe_manifest load_stage load_manifest pid deadline mark rc=0
    qa_preflight || return 1
    for value in "$DRAIN_DIRS" "$DRAIN_FILES_PER_DIR" "$DRAIN_FILE_BYTES" "$CACHE_PROBE_FILES" "$CACHE_PROBE_BYTES" "$DRAIN_SAMPLE_PAUSE_MS" "$DRAIN_WINDOW_MAX_S" "$DRAIN_MIN_SAMPLES" "$QA_SNAPPY_MS" "$QA_HARD_STALL_MS"; do
        case "$value" in ''|*[!0-9]*) return 1 ;; esac
        [ "$value" -gt 0 ] || return 1
    done
    [ "$DRAIN_MIN_SAMPLES" -ge 40 ] || return 1
    mkdir -p "$SNAP_TMP" || return 1
    printf '%s\n' "$SNAP_TMP" > "$QA_CAT_DIR/custody.path" || return 1
    DRAIN_DEST="$QA_DEST_ROOT/$(qa_unique_tag QA)"
    mkdir -p "$DRAIN_DEST" || return 1
    mark=$(qa_log_mark)
    probe_stage="$QA_STAGE/drain_probe_$(qa_unique_tag s)"
    probe_manifest="$(qa_stage_tree "$probe_stage" 1 "$CACHE_PROBE_FILES" "$CACHE_PROBE_BYTES" 1)" || return 1
    drain_evidence manifest "$probe_manifest" 1 "$CACHE_PROBE_FILES" > "$QA_CAT_DIR/probe.expected.json" || return 1
    snap_admit_next || return 1
    deadline=$(( $(date +%s)+DRAIN_WINDOW_MAX_S ))
    snap_start_copy probe "$probe_stage" "$DRAIN_DEST" "$probe_manifest" || return 1
    snap_join_positive probe "$SNAP_LAST_PID" "$deadline"
    snap_flush_soft_reason
    snap_admit_next || return 1
    drain_settle "$QA_CAT_DIR/probe-settle" "$QA_DRAIN_TIMEOUT" || { SNAP_HOLD=1; return 1; }
    drain_custody "$probe_manifest" "$DRAIN_DEST/$(basename "$probe_stage")" 1 "$CACHE_PROBE_FILES" probe || return 1
    PROBE_DIR="$DRAIN_DEST/$(basename "$probe_stage")/dir1"; PROBE_FILE="$PROBE_DIR/file1.dat"
    # Warm-up evidence is retained but never added to the drain sample floor.
    drain_pair "$QA_CAT_DIR/warmup" || return 1
    load_stage="$QA_STAGE/drain_load_$(qa_unique_tag s)"
    load_manifest="$(qa_stage_tree "$load_stage" "$DRAIN_DIRS" "$DRAIN_FILES_PER_DIR" "$DRAIN_FILE_BYTES" 1)" || return 1
    drain_evidence manifest "$load_manifest" "$DRAIN_DIRS" "$DRAIN_FILES_PER_DIR" > "$QA_CAT_DIR/load.expected.json" || return 1
    LOAD_CEIL="$(qa_drain_ceiling $((DRAIN_DIRS*DRAIN_FILES_PER_DIR*DRAIN_FILE_BYTES)) $((DRAIN_DIRS*DRAIN_FILES_PER_DIR*2)))"
    case "$LOAD_CEIL" in ''|*[!0-9]*) return 1 ;; esac
    [ "$LOAD_CEIL" -gt 0 ] || return 1
    snap_admit_next || return 1
    deadline=$(( $(date +%s)+DRAIN_WINDOW_MAX_S ))
    drain_get metrics "$QA_CAT_DIR/window-before.json" || return 1
    snap_start_copy load "$load_stage" "$DRAIN_DEST" "$load_manifest" || return 1
    pid="$SNAP_LAST_PID"
    drain_window "$pid" "$deadline" || rc=1
    drain_evidence summary "$QA_CAT_DIR" "$DRAIN_MIN_SAMPLES" "$QA_SNAPPY_MS" "$QA_HARD_STALL_MS" > "$QA_CAT_DIR/summary-validation.log" 2>&1 || rc=1
    drain_custody "$load_manifest" "$DRAIN_DEST/$(basename "$load_stage")" "$DRAIN_DIRS" "$DRAIN_FILES_PER_DIR" load || rc=1
    qa_error_scan "$mark" "$QA_CAT" > "$QA_CAT_DIR/error-scan.log" 2>&1 || rc=1
    if [ "$rc" = 0 ] && snap_admit_next; then
        qa_pass 'whole-window and active-only paired latency and mounted custody passed; backend proof separate'
    else
        qa_fail 'required drain phase invalid, uncovered, slow or incomplete'; return 1
    fi
}

drain_publish_terminal() {
    printf 'pending\thold\tforced_offline\tsoft_red\tfail\n%s\t%s\t%s\t%s\t%s\n' \
        "$SNAP_PENDING" "$SNAP_HOLD" "$SNAP_FORCED_OFFLINE" "$SNAP_SOFT_RED" "$QA_FAIL" \
        > "$QA_CAT_DIR/terminal.tsv.tmp" && mv "$QA_CAT_DIR/terminal.tsv.tmp" "$QA_CAT_DIR/terminal.tsv"
}

drain_finish() {
    [ "$SNAP_PENDING" = 0 ] || snap_hold_unknown 'category11 exits with original work pending'
    if [ "$SNAP_HOLD" = 1 ] || [ "$QA_FAIL" -gt 0 ]; then QA_KEEP_DATA=1; fi
    while ! drain_publish_terminal; do
        snap_soft_deadline 'category11 terminal publication failed'; QA_KEEP_DATA=1; sleep 1
    done
    qa_end
    if [ "$QA_FAIL" = 0 ]; then echo "VERDICT: PASS $QA_CAT (syscall latency; inspect coverage)"
    else echo "VERDICT: FAIL $QA_CAT; evidence retained at $QA_CAT_DIR"; fi
}

qa_begin "$QA_CAT"
drain_run || qa_fail 'category11 required phase failed; no replacement operation'
drain_finish
exit 0
