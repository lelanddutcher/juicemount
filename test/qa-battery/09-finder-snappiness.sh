#!/usr/bin/env bash
# Category09: Finder-populated directory enumeration, not Finder paint timing.
# Default workload remains 10/100/1000/5000 primary files plus AppleDouble extras.
# First-timed / warm / forced-offline / under-drain phases retain exact names,
# original Finder replies/joins, metrics brackets and mounted-byte custody.
# Independent backend readback is the acceptance controller's separate gate.
# A soft deadline never cancels Finder. Unknown completion keeps this ORIGINAL
# category process and data in HOLD; no redispatch, offline toggle or cleanup.
set -uo pipefail
SNAP_HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SNAP_HERE/lib.sh"
source "$SNAP_HERE/snappiness_custody.sh"
QA_CAT=09-finder-snappiness
SNAP_SIZES="${SNAP_SIZES:-10 100 1000 5000}"
[ "${QUICK:-0}" = 1 ] && SNAP_SIZES="${SNAP_SIZES_QUICK:-10 100 1000}"
SNAP_FILE_BYTES="${SNAP_FILE_BYTES:-4096}"
SNAP_DRAIN_PROBE_FILES="${SNAP_DRAIN_PROBE_FILES:-100}"
SNAP_TIMEOUT="${SNAP_TIMEOUT:-600}"
SNAP_FINDER_TIMEOUT=$((SNAP_TIMEOUT+600))
SNAP_PENDING=0 SNAP_HOLD=0 SNAP_SOFT_RED=0 SNAP_SIGNALLED=0
SNAP_SOFT_REASON="" SNAP_COPY_PIDS="" SNAP_FORCED_OFFLINE=0 SNAP_DEST=""
SNAP_SEQ=0
SNAP_TMP="$QA_STAGE/snap-ipc-$$"
trap snap_on_exit EXIT
trap snap_signal INT TERM

snap_evidence() {
    /usr/bin/python3 -I -B "$SNAP_HERE/snappiness_evidence.py" "$@"
}

snap_get() {
    # Original synchronous control request is joined before returning.
    qa_timeout 5 curl -fsS "$CP_BASE/$1" > "$2"
}

snap_row() {
    printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" >> "$QA_CAT_DIR/snap_dirs.tsv"
}

snap_validate_rows() {
    # Deliberate word splitting only for the validated numeric sizes list.
    snap_evidence rows "$QA_CAT_DIR/snap_dirs.tsv" $SNAP_SIZES
}

snap_copy() {
    local id="$1" src="$2" dest="$3" man="$4" deadline
    snap_admit_next || return 1
    deadline=$(( $(date +%s) + SNAP_TIMEOUT ))
    snap_start_copy "$id" "$src" "$dest" "$man" || return 1
    snap_join_positive "$id" "$SNAP_LAST_PID" "$deadline"
    snap_flush_soft_reason
    snap_admit_next
}

snap_custody() {
    local man="$1" dest="$2" count="$3" receipt="$4"
    # Validate before shared helper: missing/empty manifest must never pass as 0/0.
    snap_evidence manifest "$man" "$count" > "$receipt.names.json" || return 1
    qa_verify_custody "$man" "$dest" > "$receipt.custody.log" 2>&1
}

snap_offline() {
    local mode="$1" prefix="$QA_CAT_DIR/offline-$1"
    snap_admit_next || return 1
    # Set uncertainty BEFORE dispatch; failure cannot erase restoration custody.
    SNAP_FORCED_OFFLINE=1
    if ! qa_offline "$mode" > "$prefix.reply.json" ||
       ! snap_get offline "$prefix.state.json" ||
       ! snap_evidence offline "$prefix.reply.json" "$prefix.state.json" "$mode"; then
        SNAP_HOLD=1
        qa_fail "effective $mode transition unknown; retain fixture/control receipts"
        return 1
    fi
    [ "$mode" != off ] || SNAP_FORCED_OFFLINE=0
}

snap_settled() {
    local prefix="$1"
    qa_wait_drain > "$prefix.drain.log" 2>&1 &&
        snap_get spool "$prefix.spool.json" &&
        snap_evidence settled "$prefix.spool.json"
}

snap_measure() {
    local phase="$1" label="$2" dir="$3" expected="$4" prefix observation rc
    SNAP_SEQ=$((SNAP_SEQ+1))
    prefix="$QA_CAT_DIR/interval-$SNAP_SEQ-$phase"
    snap_get metrics "$prefix.before.json" || return 1
    if [ "$phase" = under-drain ]; then
        snap_get spool "$prefix.spool-before.json" || return 1
    fi
    snap_evidence list "$dir" > "$prefix.listing.json" || return 1
    if [ "$phase" = under-drain ]; then
        snap_get spool "$prefix.spool-after.json" || return 1
    fi
    snap_get metrics "$prefix.after.json" || return 1
    observation="$(snap_evidence interval "$prefix" "$expected" "$QA_SNAPPY_MS" "$phase")"
    rc=$?
    # Invalid schemas/names/overlap emit no timing. Valid but slow/error samples
    # remain in result.json and the table as FAIL, never a false PASS.
    if [ "$rc" != 0 ]; then
        qa_fail "enumeration invalid/slow/error [$phase/$label]; see $prefix"
        [ -z "$observation" ] || printf '%s\t%s\t%s\tFAIL\n' "$phase" "$label" "$(printf '%s' "$observation" | cut -f1,2)" >> "$QA_CAT_DIR/latency-table.tsv"
        return 1
    fi
    printf '%s\t%s\t%s\tPASS\n' "$phase" "$label" "$(printf '%s' "$observation" | cut -f1,2)" >> "$QA_CAT_DIR/latency-table.tsv" || return 1
    qa_pass "enumeration [$phase/$label] $observation (bracket counters, not exclusive RPC attribution)"
}

snap_drain_phase() {
    local stage="$QA_STAGE/snap_drainprobe_$(qa_unique_tag s)" man base pid deadline i hot=0 measured=1
    snap_admit_next || return 1
    man="$(qa_stage_tree "$stage" 1 "$SNAP_DRAIN_PROBE_FILES" "$SNAP_FILE_BYTES" 0)" || return 1
    snap_evidence manifest "$man" "$SNAP_DRAIN_PROBE_FILES" > "$QA_CAT_DIR/drain.names.json" || return 1
    base="$(basename "$stage")"
    deadline=$(( $(date +%s) + SNAP_TIMEOUT ))
    snap_start_copy drain "$stage" "$SNAP_DEST" "$man" || return 1
    pid="$SNAP_LAST_PID"
    for i in $(seq 1 20); do
        [ "$SNAP_HOLD" = 0 ] || break
        if snap_get spool "$QA_CAT_DIR/drain-active-$i.json" &&
           snap_evidence spool "$QA_CAT_DIR/drain-active-$i.json" 2>/dev/null; then
            hot=1; break
        fi
        sleep 0.5
    done
    if [ "$hot" = 1 ]; then
        snap_measure under-drain "flat_$SNAP_BIG_N" "$SNAP_BIG_LEAF" "$SNAP_BIG_NAMES"
        measured=$?
    else
        qa_fail 'under-drain phase uncovered: no active drain; no retry or larger copy'
    fi
    # Even an invalid observation must join the SAME original Finder worker.
    snap_join_positive drain "$pid" "$deadline"
    snap_flush_soft_reason
    snap_admit_next || return 1
    snap_settled "$QA_CAT_DIR/drain-final" || return 1
    snap_custody "$man" "$SNAP_DEST/$base" "$SNAP_DRAIN_PROBE_FILES" "$QA_CAT_DIR/drain-final" || return 1
    return "$measured"
}

snap_run() {
    local n stage man base leaf dest phase mark
    qa_preflight || return 1
    # Reject malformed tunables before creating mounted data or Finder work.
    for n in $SNAP_SIZES "$SNAP_FILE_BYTES" "$SNAP_DRAIN_PROBE_FILES" "$SNAP_TIMEOUT"; do
        case "$n" in ''|*[!0-9]*) return 1 ;; esac
        [ "$n" -gt 0 ] || return 1
    done
    [ -n "$SNAP_SIZES" ] || return 1
    mkdir -p "$SNAP_TMP" || return 1
    printf '%s\n' "$SNAP_TMP" > "$QA_CAT_DIR/custody.path" || return 1
    SNAP_DEST="$QA_DEST_ROOT/$(qa_unique_tag QA)"
    mkdir -p "$SNAP_DEST" || return 1
    : > "$QA_CAT_DIR/snap_dirs.tsv" || return 1
    printf 'phase\tlabel\tcount\tms\tverdict\n' > "$QA_CAT_DIR/latency-table.tsv" || return 1
    mark=$(qa_log_mark)
    for n in $SNAP_SIZES; do
        snap_admit_next || return 1
        stage="$QA_STAGE/snap_n${n}_$(qa_unique_tag s)"
        man="$(qa_stage_tree "$stage" 1 "$n" "$SNAP_FILE_BYTES" 1)" || return 1
        snap_evidence manifest "$man" "$n" > "$QA_CAT_DIR/expected-$n.json" || return 1
        base="$(basename "$stage")"; dest="$SNAP_DEST/$base"; leaf="$dest/dir1"
        snap_copy "populate-$n" "$stage" "$SNAP_DEST" "$man" || return 1
        snap_row "$n" "$leaf" "$man" "$dest" || return 1
    done
    snap_validate_rows || return 1
    snap_settled "$QA_CAT_DIR/population" || return 1
    while IFS="$(printf '\t')" read -r n leaf man dest; do
        snap_custody "$man" "$dest" "$n" "$QA_CAT_DIR/custody-$n" || return 1
        SNAP_BIG_N="$n"; SNAP_BIG_LEAF="$leaf"; SNAP_BIG_NAMES="$QA_CAT_DIR/expected-$n.json"
    done < "$QA_CAT_DIR/snap_dirs.tsv"
    for phase in first-timed warm; do
        while IFS="$(printf '\t')" read -r n leaf man dest; do
            snap_admit_next || return 1
            snap_measure "$phase" "flat_$n" "$leaf" "$QA_CAT_DIR/expected-$n.json" || return 1
        done < "$QA_CAT_DIR/snap_dirs.tsv"
    done
    snap_offline on || return 1
    # On a measurement failure, restore known offline state before returning.
    local offline_rc=0
    snap_measure offline "flat_$SNAP_BIG_N" "$SNAP_BIG_LEAF" "$SNAP_BIG_NAMES" || offline_rc=1
    snap_evidence top "$QA_CAT_DIR/snap_dirs.tsv" > "$QA_CAT_DIR/top.names.json" || offline_rc=1
    if [ "$offline_rc" = 0 ]; then
        snap_measure offline dest_top "$SNAP_DEST" "$QA_CAT_DIR/top.names.json" || offline_rc=1
    fi
    snap_offline off || return 1
    [ "$offline_rc" = 0 ] || return 1
    snap_drain_phase || return 1
    qa_error_scan "$mark" "$QA_CAT" > "$QA_CAT_DIR/error-scan.log" 2>&1 || return 1
}

snap_publish_terminal() {
    printf 'pending\thold\tforced_offline\tsoft_red\tfail\n%s\t%s\t%s\t%s\t%s\n' \
        "$SNAP_PENDING" "$SNAP_HOLD" "$SNAP_FORCED_OFFLINE" "$SNAP_SOFT_RED" "$QA_FAIL" \
        > "$QA_CAT_DIR/terminal.tsv.tmp" && mv "$QA_CAT_DIR/terminal.tsv.tmp" "$QA_CAT_DIR/terminal.tsv"
}

snap_finish() {
    [ "$SNAP_PENDING" = 0 ] || snap_hold_unknown 'category exited with original Finder work pending'
    # qa_end calls shared cleanup; guard that path as well as the EXIT trap.
    if [ "$SNAP_HOLD" = 1 ] || [ "$SNAP_FORCED_OFFLINE" = 1 ] || [ "$QA_FAIL" -gt 0 ]; then
        QA_KEEP_DATA=1
    fi
    while ! snap_publish_terminal; do
        snap_soft_deadline 'terminal receipt publication failed'
        QA_KEEP_DATA=1
        sleep 1
    done
    qa_end
    if [ "$QA_FAIL" = 0 ]; then
        echo "VERDICT: PASS $QA_CAT (enumeration only; inspect coverage; backend proof separate)"
    else
        echo "VERDICT: FAIL $QA_CAT; retained evidence at $QA_CAT_DIR"
    fi
}

qa_begin "$QA_CAT"
snap_run || qa_fail 'category09 required phase failed or uncovered; no replacement operation'
snap_finish
exit 0
