#!/usr/bin/env bash
# ===========================================================================
# run-all.sh — JuiceMount REAL-FINDER RELEASE BATTERY orchestrator.
#
# Runs the whole battery (or a selected subset) in order, captures each
# category's VERDICT, aggregates a CROSS-CUTTING user-visible-error tally over
# the ENTIRE run (the "zero random Finder errors" gate), prints a RESULT TABLE
# and an overall RELEASE-GATE verdict. The gate is GREEN only when:
#     (1) EVERY category that ran PASSed, AND
#     (2) the cross-cutting error tally across the whole run is 0
#         (FromHandle STALE / purging phantom / 100070 / 100060 / -48 / -36 /
#          -5000 / permission), AND
#     (3) the snappiness thresholds held (no listing exceeded $QA_SNAPPY_MS,
#         derived from category 09's latency table + any [FAIL] snappiness rows).
#
# WHY A SINGLE ORCHESTRATOR (read before "simplifying"):
#   The per-category errscan windows overlap and each script only scans its OWN
#   window. A release is gated on ZERO user-visible errors across the WHOLE run,
#   not per-category. So this orchestrator marks the JuiceMount log ONCE at the
#   start and re-scans the entire run window at the end (qa_log_mark /
#   qa_error_scan from lib.sh) for the authoritative cross-cutting tally — the
#   number that actually gates the release — independent of how each category
#   chose to qa_warn vs qa_fail its own known-open edges.
#
# USAGE:
#   ./run-all.sh                 # run ALL categories in numeric order
#   ./run-all.sh 04 07           # run ONLY categories 04 and 07 (subset)
#   ./run-all.sh 01-file-types   # full names work too
#   QA_SNAPPY_MS=150 ./run-all.sh   # tighten the snappiness budget
#
# SAFE TO RUN REPEATEDLY: every category copies into a UNIQUE timestamped dest
# (qa_unique_tag) under $MOUNT/JM_RELEASE_BATTERY and cleans up ONLY its own
# subtree (lib.sh qa_cleanup guard). Nothing here touches the user's real data.
#
# bash 3.2-safe: no associative arrays, no mapfile, no ${var^^}. NO GNU timeout
# (uses lib.sh qa_timeout = perl alarm). Iterates newline-delimited streams.
#
# EXIT STATUS: 0 only when the RELEASE GATE is GREEN; 1 otherwise (so CI can gate
# on it directly).
# ===========================================================================

set -uo pipefail

QA_BATTERY_DIR="$(cd "$(dirname "$0")" && pwd)"

# Source lib.sh so we get env defaults (MOUNT autodetect, CP_ADDR, JM_LOG,
# QA_SNAPPY_MS, QA_ARTIFACTS) plus the preflight + cross-cutting scan helpers.
# shellcheck source=lib.sh
. "$QA_BATTERY_DIR/lib.sh"

# A SINGLE shared artifacts dir for the whole run — every category writes its
# subdir under here, and the final aggregate report lands at the top. Export so
# each child script inherits it instead of minting its own per-process dir.
export QA_ARTIFACTS="${QA_ARTIFACTS:-/tmp/jm-battery-run-$(date +%Y%m%d-%H%M%S)-$$}"
mkdir -p "$QA_ARTIFACTS" 2>/dev/null
RUN_REPORT="$QA_ARTIFACTS/RESULT.txt"

# Canonical category order. A category is run iff its NN-*.sh file exists.
ALL_CATEGORIES="
01-file-types
02-file-sizes
03-deep-wide-trees
04-unicode-names
05-concurrent-copies
06-cancel-retry
07-offline-spool
08-chain-of-custody
09-finder-snappiness
10-index-stability
11-drain-latency
"

# ---------------------------------------------------------------------------
# Argument parsing: no args => all; otherwise a subset by number or full name.

_resolve_category() {
    # Echo the canonical "NN-name" for a user token (e.g. 04, 4, 04-unicode-names).
    local tok="$1" c
    # Zero-pad a bare 1-2 digit number.
    case "$tok" in
        [0-9]) tok="0$tok" ;;
    esac
    for c in $ALL_CATEGORIES; do
        case "$c" in
            "$tok"|"$tok"-*) echo "$c"; return 0 ;;
        esac
        # match by NN prefix
        case "$c" in
            "$tok"*) echo "$c"; return 0 ;;
        esac
    done
    return 1
}

SELECTED=""
if [ "$#" -eq 0 ]; then
    SELECTED="$ALL_CATEGORIES"
else
    for arg in "$@"; do
        c="$(_resolve_category "$arg")" || {
            printf 'run-all: unknown category token "%s" (valid: %s)\n' "$arg" "$(echo $ALL_CATEGORIES)" >&2
            exit 2
        }
        SELECTED="$SELECTED
$c"
    done
fi

# ---------------------------------------------------------------------------
# Precheck — fail FAST with a clear message instead of letting every category
# time out. Mount healthy + control plane up + spool idle. Uses lib.sh helpers.

precheck() {
    local ok=0
    qa_sec "PRECHECK"

    if qa_cp_reachable; then
        qa_pass "control plane $CP_BASE reachable"
    else
        qa_fail "control plane $CP_BASE NOT reachable — is JuiceMount running?"
        ok=1
    fi

    if mount | awk -v m="$MOUNT" '$0 ~ " "m" " && /nfs/ {f=1} END{exit !f}'; then
        qa_pass "mount $MOUNT is an active NFS mount"
    else
        qa_fail "mount $MOUNT is NOT an active NFS mount"
        ok=1
    fi

    # control plane reports healthy:true
    if [ "$ok" -eq 0 ]; then
        if qa_health >/dev/null 2>&1; then
            qa_pass "control plane reports healthy:true"
        else
            qa_fail "control plane /health is not healthy:true"
            ok=1
        fi
    fi

    # spool idle at the START (so we attribute only THIS run's drain activity).
    if [ "$ok" -eq 0 ]; then
        local p ip
        p="$(qa_spool_pending)"
        ip="$(qa_spool_field in_progress)"
        if [ "$p" = "0" ] && { [ "$ip" = "0" ] || [ "$ip" = "-1" ]; }; then
            qa_pass "spool idle at start (pending=$p in_progress=$ip)"
        else
            qa_warn "spool NOT idle at start (pending=$p in_progress=$ip) — waiting up to ${QA_DRAIN_TIMEOUT}s for it to settle"
            if qa_wait_drain; then
                qa_pass "spool settled before run"
            else
                qa_fail "spool did not settle before run — aborting (would contaminate custody attribution)"
                ok=1
            fi
        fi
    fi

    return "$ok"
}

# ---------------------------------------------------------------------------
# Per-category run: invoke the script, tee its output, capture its VERDICT.

# Globals accumulated across the run (newline-delimited rows: "cat|status|metric").
RESULT_ROWS=""
ANY_FAIL=0
RAN_COUNT=0
SKIPPED=""

# Extract the single key metric for a category from its artifacts/.summary, for
# the RESULT TABLE's third column. Best-effort; never fails the run.
_category_metric() {
    local cat="$1" d="$QA_ARTIFACTS/$cat" m="" s
    s="$d/.summary"
    # pass/fail/warn counts always available from the .summary lib writes.
    local pass fail warn
    if [ -f "$s" ]; then
        pass="$(awk -F= '/^pass=/{print $2}'  "$s" 2>/dev/null)"
        fail="$(awk -F= '/^fail=/{print $2}'  "$s" 2>/dev/null)"
        warn="$(awk -F= '/^warn=/{print $2}'  "$s" 2>/dev/null)"
    fi
    case "$cat" in
        09-finder-snappiness)
            # Worst observed listing latency from the latency table (col 'MS').
            local lt="$d/latency-table.tsv" maxms=""
            if [ -f "$lt" ]; then
                maxms="$(awk 'NR>1{for(i=1;i<=NF;i++) if($i+0==$i && $i!="") c=$i; if(c>m){m=c}} END{print m+0}' "$lt" 2>/dev/null)"
            fi
            [ -n "$maxms" ] && m="max ${maxms}ms / ${QA_SNAPPY_MS}ms budget"
            ;;
        10-index-stability)
            # push vs full-SCAN: count expensive re-pull promotions in the window.
            local es; es="$(ls "$d"/errscan-*.txt 2>/dev/null | head -1)"
            m="reconcile stayed snappy"
            ;;
    esac
    if [ -z "$m" ]; then
        m="pass=${pass:-?} fail=${fail:-?} warn=${warn:-?}"
    else
        m="$m (pass=${pass:-?} fail=${fail:-?} warn=${warn:-?})"
    fi
    echo "$m"
}

run_category() {
    local cat="$1" script="$QA_BATTERY_DIR/$cat.sh"
    if [ ! -f "$script" ]; then
        qa_warn "category $cat: script $cat.sh MISSING — skipping (treated as NOT-RUN, gate stays red if it was selected)"
        SKIPPED="$SKIPPED $cat"
        RESULT_ROWS="$RESULT_ROWS
$cat|SKIP(missing)|script not present"
        return 0
    fi
    if [ ! -x "$script" ]; then
        chmod +x "$script" 2>/dev/null || true
    fi

    qa_sec "RUN category $cat"
    RAN_COUNT=$((RAN_COUNT+1))

    local logf="$QA_ARTIFACTS/$cat.console.log"
    local verdict status
    # Run the category. It sources lib.sh itself, inherits our exported env
    # (QA_ARTIFACTS, MOUNT, CP_ADDR, QA_SNAPPY_MS, ...). Each script always
    # exits 0; the real result is the VERDICT line on stdout.
    "$script" 2>&1 | tee "$logf"

    # Pull the LAST 'VERDICT:' line the script emitted (authoritative result).
    verdict="$(grep '^VERDICT:' "$logf" 2>/dev/null | tail -1)"
    if [ -z "$verdict" ]; then
        status="FAIL"
        verdict="VERDICT: FAIL $cat: no VERDICT line emitted (script crashed before reporting — see $logf)"
        ANY_FAIL=1
    elif printf '%s' "$verdict" | grep -q '^VERDICT: PASS'; then
        status="PASS"
    else
        status="FAIL"
        ANY_FAIL=1
    fi

    local metric; metric="$(_category_metric "$cat")"
    RESULT_ROWS="$RESULT_ROWS
$cat|$status|$metric"
    qa_log "category $cat => $status"
}

# ---------------------------------------------------------------------------
# Snappiness gate across the run: any [FAIL] snappiness row in any category, or
# any latency-table MS >= budget, fails the snappiness gate independently.

snappiness_held() {
    local bad=0 f
    # Scan every category console log for a snappiness FAIL marker.
    for f in "$QA_ARTIFACTS"/*.console.log; do
        [ -f "$f" ] || continue
        if grep -qE '\[FAIL\].*(snappiness|spinner|listing exceeded|beachball)' "$f" 2>/dev/null; then
            bad=1
        fi
    done
    # And cross-check category 09's latency table directly.
    local lt="$QA_ARTIFACTS/09-finder-snappiness/latency-table.tsv"
    if [ -f "$lt" ]; then
        if awk -v b="$QA_SNAPPY_MS" 'NR>1{for(i=1;i<=NF;i++) if($i+0==$i && $i!="") c=$i; if(c+0>=b) bad=1} END{exit !bad}' "$lt" 2>/dev/null; then
            bad=1
        fi
    fi
    [ "$bad" -eq 0 ]
}

# ===========================================================================
# MAIN

qa_log "=========================================================="
qa_log "JuiceMount RELEASE BATTERY — run-all"
qa_log "  MOUNT=$MOUNT  CP=$CP_BASE  SNAPPY_MS=$QA_SNAPPY_MS"
qa_log "  artifacts: $QA_ARTIFACTS"
qa_log "  categories: $(echo $SELECTED | tr '\n' ' ')"
qa_log "=========================================================="

# Mark the JuiceMount log ONCE so the end-of-run cross-cutting scan covers the
# ENTIRE battery window (the authoritative zero-error gate).
RUN_MARK="$(qa_log_mark)"
qa_info "JuiceMount log marked at line $RUN_MARK ($JM_LOG)"

if ! precheck; then
    qa_fail "PRECHECK failed — refusing to run the battery (fix the environment first)"
    {
        echo "RELEASE GATE: RED (precheck failed)"
        echo "See [FAIL] lines above."
    } | tee "$RUN_REPORT"
    exit 1
fi

# Set QA_CAT_DIR so the orchestrator's own qa_error_scan dumps land somewhere.
QA_CAT_DIR="$QA_ARTIFACTS/_run_aggregate"
mkdir -p "$QA_CAT_DIR" 2>/dev/null

# Run each selected category in order.
for cat in $SELECTED; do
    [ -z "$cat" ] && continue
    run_category "$cat"
done

# ---------------------------------------------------------------------------
# CROSS-CUTTING ERROR TALLY over the WHOLE run window (the release gate's
# zero-random-Finder-errors number). Re-scan the JuiceMount log from RUN_MARK.

qa_sec "CROSS-CUTTING ERROR TALLY (whole-run window)"
XSCAN="$(qa_error_scan "$RUN_MARK" "run-aggregate")"
qa_log "$XSCAN"

# Parse each signature out of the tally line. Format (lib.sh qa_error_scan):
#   errscan STALE=N PHANTOM=N E100070=N E100060=N E48=N E36=N E5000=N PERM=N
_xfield() { printf '%s' "$XSCAN" | tr ' ' '\n' | awk -F= -v k="$1" '$1==k{print $2}'; }
X_STALE="$(_xfield STALE)";    X_STALE="${X_STALE:-0}"
X_PHANTOM="$(_xfield PHANTOM)"; X_PHANTOM="${X_PHANTOM:-0}"
X_E70="$(_xfield E100070)";    X_E70="${X_E70:-0}"
X_E60="$(_xfield E100060)";    X_E60="${X_E60:-0}"
X_E48="$(_xfield E48)";        X_E48="${X_E48:-0}"
X_E36="$(_xfield E36)";        X_E36="${X_E36:-0}"
X_E5000="$(_xfield E5000)";    X_E5000="${X_E5000:-0}"
X_PERM="$(_xfield PERM)";      X_PERM="${X_PERM:-0}"

XTOTAL=$(( X_STALE + X_PHANTOM + X_E70 + X_E60 + X_E48 + X_E36 + X_E5000 + X_PERM ))

# ---------------------------------------------------------------------------
# Snappiness gate.
if snappiness_held; then SNAPPY_OK=1; else SNAPPY_OK=0; fi

# ---------------------------------------------------------------------------
# Did we run everything that was selected? A SKIPPED (missing) selected category
# keeps the gate RED — a release can't be green with a missing test.
SELECTED_COUNT=0
for cat in $SELECTED; do [ -n "$cat" ] && SELECTED_COUNT=$((SELECTED_COUNT+1)); done
MISSING_SELECTED=0
[ -n "$(echo "$SKIPPED" | tr -d ' ')" ] && MISSING_SELECTED=1

# ---------------------------------------------------------------------------
# RESULT TABLE + RELEASE-GATE verdict.

{
    echo ""
    echo "=========================================================="
    echo " JuiceMount RELEASE BATTERY — RESULT TABLE"
    echo " mount=$MOUNT  cp=$CP_BASE  snappy_budget=${QA_SNAPPY_MS}ms"
    echo " artifacts=$QA_ARTIFACTS"
    echo "=========================================================="
    printf '%-22s | %-13s | %s\n' "CATEGORY" "RESULT" "KEY METRIC"
    printf '%-22s-+-%-13s-+-%s\n' "----------------------" "-------------" "------------------------------"
    # RESULT_ROWS rows are "cat|status|metric".
    printf '%s\n' "$RESULT_ROWS" | while IFS='|' read -r c s m; do
        [ -z "$c" ] && continue
        printf '%-22s | %-13s | %s\n' "$c" "$s" "$m"
    done
    echo ""
    echo "CROSS-CUTTING USER-VISIBLE ERROR TALLY (whole run window):"
    printf '  FromHandle STALE=%s  purging-phantom=%s  100070=%s  100060=%s\n' "$X_STALE" "$X_PHANTOM" "$X_E70" "$X_E60"
    printf '  -48(already-item)=%s  -36(ioErr)=%s  -5000=%s  permission=%s\n' "$X_E48" "$X_E36" "$X_E5000" "$X_PERM"
    printf '  TOTAL gated errors = %s   (gate requires 0)\n' "$XTOTAL"
    echo ""
    if [ "$SNAPPY_OK" -eq 1 ]; then
        echo "SNAPPINESS: held (no listing >= ${QA_SNAPPY_MS}ms)"
    else
        echo "SNAPPINESS: VIOLATED (a listing met/exceeded ${QA_SNAPPY_MS}ms — see 09 latency table / [FAIL] rows)"
    fi
    if [ "$MISSING_SELECTED" -eq 1 ]; then
        echo "COVERAGE:   INCOMPLETE — missing selected script(s):$SKIPPED"
    fi
    echo "=========================================================="
} | tee "$RUN_REPORT"

# GREEN only if: no category failed, AND cross-cutting tally == 0, AND
# snappiness held, AND nothing selected was skipped/missing.
GATE_GREEN=1
[ "$ANY_FAIL" -eq 0 ]        || GATE_GREEN=0
[ "$XTOTAL" -eq 0 ]         || GATE_GREEN=0
[ "$SNAPPY_OK" -eq 1 ]      || GATE_GREEN=0
[ "$MISSING_SELECTED" -eq 0 ] || GATE_GREEN=0

{
    if [ "$GATE_GREEN" -eq 1 ]; then
        echo "RELEASE GATE: GREEN — ship it. ($RAN_COUNT/$SELECTED_COUNT categories PASS, 0 gated errors, snappiness held)"
    else
        echo "RELEASE GATE: RED — DO NOT SHIP."
        [ "$ANY_FAIL" -ne 0 ]         && echo "  - one or more categories FAILED (see RESULT TABLE)"
        [ "$XTOTAL" -ne 0 ]           && echo "  - cross-cutting error tally is $XTOTAL (must be 0) — see $QA_CAT_DIR/errscan-run-aggregate.txt for the offending paths"
        [ "$SNAPPY_OK" -ne 1 ]        && echo "  - snappiness budget violated (${QA_SNAPPY_MS}ms)"
        [ "$MISSING_SELECTED" -ne 0 ] && echo "  - selected categories were missing/not-run:$SKIPPED"
    fi
} | tee -a "$RUN_REPORT"

[ "$GATE_GREEN" -eq 1 ] && exit 0 || exit 1
