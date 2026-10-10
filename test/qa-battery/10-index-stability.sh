#!/usr/bin/env bash
# One supported metadata sync: sampled native enumeration/copy overlap, not paint.
set -uo pipefail
INDEX_HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$INDEX_HERE/lib.sh"
source "$INDEX_HERE/snappiness_custody.sh"
QA_CAT=10-index-stability
INDEX_DIRS="${INDEX_DIRS:-20}" INDEX_FILES="${INDEX_FILES:-15}" INDEX_BYTES="${INDEX_BYTES:-262144}"
INDEX_SMALL_DIRS=2 INDEX_SMALL_FILES=5 INDEX_SMALL_BYTES=131072
INDEX_WINDOW_S="${INDEX_WINDOW_S:-240}"
SNAP_FINDER_TIMEOUT=900
SNAP_PENDING=0 SNAP_HOLD=0 SNAP_SOFT_RED=0 SNAP_SIGNALLED=0 SNAP_FORCED_OFFLINE=0
SNAP_SOFT_REASON="" SNAP_COPY_PIDS="" INDEX_SYNC_PENDING=0
SNAP_TMP="$QA_STAGE/index-ipc-$$"
trap index_exit EXIT
trap snap_signal INT TERM
index_evidence() { /usr/bin/python3 -I -B "$INDEX_HERE/index_stability_evidence.py" "$@"; }
index_get() { qa_timeout 5 curl -fsS "$CP_BASE/$1" > "$2"; }
index_exit() {
    [ "$INDEX_SYNC_PENDING" = 0 ] || return 0
    snap_on_exit
}
index_sync_join() {
    local deadline="$1"
    while [ ! -f "$QA_INDEX_SYNC_DIR/reply.json" ]; do
        [ "$(date +%s)" -lt "$deadline" ] || snap_soft_deadline 'original sync observation deadline'
        sleep 1
    done
    index_evidence reply "$QA_INDEX_SYNC_DIR" > "$QA_CAT_DIR/sync-joined.json" || snap_hold_unknown 'original sync reply unresolved or failed'
    INDEX_SYNC_PENDING=0
}
index_settle() {
    local label="$1" deadline=$(( $(date +%s)+QA_DRAIN_TIMEOUT )) n=0
    while :; do
        n=$((n+1))
        index_get spool "$QA_CAT_DIR/$label-$n.json" || return 1
        index_evidence spool "$QA_CAT_DIR/$label-$n.json" || return 1
        index_evidence settled "$QA_CAT_DIR/$label-$n.json" 2>/dev/null && return 0
        [ "$(date +%s)" -lt "$deadline" ] || return 1
        sleep 1
    done
}
index_sample() {
    local n="$1" namespace="$2" path="$3" files="$4" prefix="$QA_CAT_DIR/sample-$1" a b wa wb rc=0
    wa=$(index_evidence wallclock) || rc=1
    a=$(index_evidence clock) || rc=1
    index_get activity "$prefix.activity-before.json" || rc=1
    index_get metrics "$prefix.metrics-before.json" || rc=1
    index_evidence observe "$path" > "$prefix.listing.json" || rc=1
    index_get metrics "$prefix.metrics-after.json" || rc=1
    index_get activity "$prefix.activity-after.json" || rc=1
    b=$(index_evidence clock) || rc=1
    wb=$(index_evidence wallclock) || rc=1
    index_evidence clockpair "$a" "$b" "$wa" "$wb" > "$prefix.clock.json" || rc=1
    if [ "$rc" = 0 ]; then index_evidence sample "$prefix" "$files" "$namespace" > "$prefix.result.json" || rc=1; fi
    if [ "$rc" = 0 ]; then printf '%s\tvalid\t%s\t%s\n' "$n" "$namespace" "$files" >> "$QA_CAT_DIR/samples.tsv" || return 1
    else printf '%s\tinvalid\t%s\t%s\n' "$n" "$namespace" "$files" >> "$QA_CAT_DIR/samples.tsv"; return 1; fi
}
index_window() {
    local deadline="$1" n=0 rc=0 small_pid='' namespace path files activity_ok
    : > "$QA_CAT_DIR/samples.tsv" || rc=1
    while [ ! -f "$QA_INDEX_SYNC_DIR/reply.json" ]; do
        if [ "$(date +%s)" -ge "$deadline" ]; then snap_soft_deadline 'sync measurement window capped';rc=1;break;fi
        activity_ok=1
        index_get activity "$QA_CAT_DIR/dispatch-activity.json" || { qa_fail 'activity request failed';rc=1;activity_ok=0; }
        if [ "$rc" = 0 ] && [ "$activity_ok" = 1 ] && [ -z "$small_pid" ] && snap_admit_next && index_evidence active "$QA_CAT_DIR/dispatch-activity.json" 2>/dev/null; then
            index_evidence clock > "$QA_CAT_DIR/small-start-ns" || { rc=1;break; }
            index_evidence wallclock > "$QA_CAT_DIR/small-start-wall-ns" || { rc=1;break; }
            snap_start_copy small "$SMALL_STAGE" "$INDEX_DEST" "$SMALL_MANIFEST" || { rc=1;break; }
            small_pid="$SNAP_LAST_PID"
        fi
        namespace=driver;path="$DRIVER_DIR";files="$INDEX_FILES"
        if [ -n "$small_pid" ] && [ -f "$SNAP_TMP/small.rc" ]; then
            snap_reply_positive "$(cat "$SNAP_TMP/small.rc")" "$(cat "$SNAP_TMP/small.out" 2>/dev/null)" || snap_hold_unknown 'small original reply failed'
            [ -f "$QA_CAT_DIR/small-reply-observed-ns" ] || index_evidence clock > "$QA_CAT_DIR/small-reply-observed-ns" || rc=1
            if [ $((n%2)) = 1 ]; then namespace=small;path="$SMALL_DIR";files="$INDEX_SMALL_FILES";fi
        fi
        n=$((n+1));index_sample "$n" "$namespace" "$path" "$files" || { qa_fail 'listing observation failed';rc=1; }
    done
    # Always settle the same originals before any summary or backend handoff.
    if [ -n "$small_pid" ]; then
        snap_join_positive small "$small_pid" "$deadline"
        [ -f "$QA_CAT_DIR/small-reply-observed-ns" ] || index_evidence clock > "$QA_CAT_DIR/small-reply-observed-ns" || rc=1
    else qa_fail 'no small copy admitted during reconcile';rc=1;fi
    index_sync_join "$deadline"
    snap_flush_soft_reason
    index_settle final-settle || { SNAP_HOLD=1;rc=1; }
    index_get metrics "$QA_CAT_DIR/window-after.json" || rc=1
    index_get whoami "$QA_CAT_DIR/whoami-after.json" || rc=1
    index_evidence identity "$QA_INDEX_SYNC_DIR/arm.json" "$QA_CAT_DIR/whoami-after.json" || rc=1
    return "$rc"
}
index_run() {
    local driver_stage driver_manifest mark deadline pid rc=0 value
    qa_preflight || return 1
    for value in "$INDEX_DIRS" "$INDEX_FILES" "$INDEX_BYTES" "$INDEX_WINDOW_S"; do case "$value" in ''|*[!0-9]*) return 1;;esac;[ "$value" -gt 0 ] || return 1;done
    # Fail before ANY Finder operation unless controller armed the one original.
    [ -n "${QA_INDEX_SYNC_DIR:-}" ] || return 1
    index_get whoami "$QA_CAT_DIR/whoami-before.json" || return 1
    index_evidence arm "$QA_INDEX_SYNC_DIR" "$QA_CAT_DIR/whoami-before.json" || return 1
    mkdir -p "$SNAP_TMP" || return 1
    printf '%s\n' "$SNAP_TMP" > "$QA_CAT_DIR/custody.path" || return 1
    INDEX_DEST="$QA_DEST_ROOT/$(qa_unique_tag QA)";mkdir -p "$INDEX_DEST" || return 1
    driver_stage="$QA_STAGE/index_driver_$(qa_unique_tag s)";SMALL_STAGE="$QA_STAGE/index_small_$(qa_unique_tag s)"
    driver_manifest=$(qa_stage_tree "$driver_stage" "$INDEX_DIRS" "$INDEX_FILES" "$INDEX_BYTES" 1) || return 1
    SMALL_MANIFEST=$(qa_stage_tree "$SMALL_STAGE" "$INDEX_SMALL_DIRS" "$INDEX_SMALL_FILES" "$INDEX_SMALL_BYTES" 1) || return 1
    index_evidence manifest "$driver_manifest" "$INDEX_DIRS" "$INDEX_FILES" > "$QA_CAT_DIR/driver.expected.json" || return 1
    index_evidence manifest "$SMALL_MANIFEST" "$INDEX_SMALL_DIRS" "$INDEX_SMALL_FILES" > "$QA_CAT_DIR/small.expected.json" || return 1
    DRIVER_DIR="$INDEX_DEST/$(basename "$driver_stage")/dir1";SMALL_DIR="$INDEX_DEST/$(basename "$SMALL_STAGE")/dir1"
    mark=$(qa_log_mark);index_get metrics "$QA_CAT_DIR/push-before.json" || return 1
    snap_admit_next || return 1;deadline=$(( $(date +%s)+INDEX_WINDOW_S ))
    snap_start_copy driver "$driver_stage" "$INDEX_DEST" "$driver_manifest" || return 1
    snap_join_positive driver "$SNAP_LAST_PID" "$deadline";snap_flush_soft_reason
    index_settle driver-settle || { SNAP_HOLD=1;return 1; }
    qa_verify_custody "$driver_manifest" "$INDEX_DEST/$(basename "$driver_stage")" > "$QA_CAT_DIR/driver.custody.log" 2>&1 || return 1
    index_get metrics "$QA_CAT_DIR/push-after.json" || return 1
    index_evidence push "$QA_CAT_DIR/push-before.json" "$QA_CAT_DIR/push-after.json" > "$QA_CAT_DIR/push.json" || return 1
    snap_admit_next || return 1
    index_get metrics "$QA_CAT_DIR/window-before.json" || return 1
    index_evidence metrics "$QA_CAT_DIR/push-after.json" "$QA_CAT_DIR/window-before.json" || return 1
    index_get whoami "$QA_CAT_DIR/whoami-request.json" || return 1
    INDEX_SYNC_PENDING=1 # latch BEFORE exclusive publication; never infer absent == unissued.
    index_evidence request "$QA_INDEX_SYNC_DIR" "$QA_CAT_DIR/whoami-request.json" > "$QA_CAT_DIR/sync-request.json" || snap_hold_unknown 'sync request publication uncertain'
    deadline=$(( $(date +%s)+INDEX_WINDOW_S ))
    index_window "$deadline" || rc=1
    index_evidence summary "$QA_CAT_DIR" "$QA_INDEX_SYNC_DIR" "$QA_SNAPPY_MS" > "$QA_CAT_DIR/summary-validation.log" 2>&1 || rc=1
    qa_verify_custody "$SMALL_MANIFEST" "$INDEX_DEST/$(basename "$SMALL_STAGE")" > "$QA_CAT_DIR/small.custody.log" 2>&1 || rc=1
    qa_error_scan "$mark" "$QA_CAT" > "$QA_CAT_DIR/error-scan.log" 2>&1 || rc=1
    index_evidence errors "$QA_CAT_DIR/error-scan.log" || rc=1
    if [ "$rc" = 0 ] && snap_admit_next;then qa_pass 'sampled reconcile overlap, push delivery and mounted custody; backend proof separate'
    else qa_fail 'required reconcile coverage invalid or unproven';return 1;fi
}
index_finish() {
    [ "$SNAP_PENDING" = 0 ] && [ "$INDEX_SYNC_PENDING" = 0 ] || snap_hold_unknown 'category10 original operation pending'
    if [ "$SNAP_HOLD" = 1 ] || [ "$QA_FAIL" -gt 0 ];then QA_KEEP_DATA=1;fi
    while ! { printf 'pending\thold\tforced_offline\tsoft_red\tfail\n%s\t%s\t%s\t%s\t%s\n' "$SNAP_PENDING" "$SNAP_HOLD" "$SNAP_FORCED_OFFLINE" "$SNAP_SOFT_RED" "$QA_FAIL" > "$QA_CAT_DIR/terminal.tsv.tmp" && mv "$QA_CAT_DIR/terminal.tsv.tmp" "$QA_CAT_DIR/terminal.tsv"; };do snap_soft_deadline 'terminal publication failed';QA_KEEP_DATA=1;sleep 1;done
    qa_end
    if [ "$QA_FAIL" = 0 ];then echo "VERDICT: PASS $QA_CAT (sampled reconcile overlap)";else echo "VERDICT: FAIL $QA_CAT; retained $QA_CAT_DIR";fi
}
qa_begin "$QA_CAT"
index_run || qa_fail 'category10 required phase failed; no replacement operation'
index_finish
exit 0
