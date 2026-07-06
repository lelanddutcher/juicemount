#!/usr/bin/env bash
# ===========================================================================
# 10-index-stability.sh — JuiceMount RELEASE BATTERY, category "10-index-stability"
#
# SCOPE: System-wide stability DURING a metadata index reconcile.
#
# The reported symptom this category exists to catch: opening a Chrome
# file-picker while the metadata index was rebuilding froze ALL of Finder,
# system-wide. The bar here is that a metadata reconcile (index rebuild) must
# NEVER stall the user's foreground work — directory listings must stay snappy
# and a parallel Finder copy must keep making progress while a reconcile runs.
#
# This category also verifies the BUILD is using keyspace-PUSH (incremental
# deltas from Redis keyspace notifications) and is NOT doing a full ~30s
# re-pull (SCAN) when it doesn't need to. Grounded in the real source:
#
#   * Control plane http://127.0.0.1:11050 (lib.sh CP_BASE):
#       GET /activity  → JSON {"busy":bool,"summary":...,"operations":[
#                          {"kind":"reconcile","active":bool,"detail":"Rebuilding index…"},
#                          {"kind":"drain",...},{"kind":"prefetch",...}]}
#                        (bridge/cbridge.go:2901,2928 — handleActivityHTTP)
#       GET /health    → "healthy":true
#       GET /spool     → pending_files / in_progress (drain gate)
#   * Log ~/Library/Logs/JuiceMount/juicemount.log (lib.sh JM_LOG). The
#     metadata layer logs distinguish push vs full re-pull (verbatim strings):
#       - PUSH ENABLED : "metadata keyspace push: loop starting"
#                        "metadata keyspace push: subscribed"
#                        (metadata/keyspace.go:292,400)
#       - PUSH EVENT   : "metadata keyspace push: reconcileDir"
#                        (incremental, per-dir HGETALL — the good path)
#                        (metadata/keyspace.go:844)
#       - FULL RE-PULL : "metadata keyspace push: burst over ceiling, promoting to full SCAN"
#                        (metadata/keyspace.go:558) and the periodic backstop
#                        "metadata sync complete" (metadata/redis.go:1558).
#                        These are the EXPENSIVE ~30s re-pulls; over a quiet
#                        window with light churn they must be RARE relative to
#                        push events — the whole point of the keyspace-push work.
#
# There is NO HTTP endpoint to force a reconcile (NFSServerSyncNow is cgo-only,
# cbridge.go:1397). So we OBSERVE a reconcile two ways and accept either:
#   (a) a REAL Finder copy of a fresh tree drives keyspace-push reconcileDir
#       events + a spool drain → /activity reports reconcile/drain busy;
#   (b) the periodic backstop SCAN fires on its own cadence.
# We watch /activity (kind=reconcile active=true, or busy=true) and the log
# window for reconcile markers, and ASSERT that WHILE that activity is in
# flight, a parallel `ls` stays < QA_SNAPPY_MS and a parallel small Finder copy
# completes within a tight bound (it must not freeze like the Chrome picker did).
#
# CASES (echoed per-case so a human can watch progress):
#   C1  build-uses-keyspace-push     — log shows push loop subscribed; over the
#                                       observation window, full-SCAN re-pull
#                                       events are NOT more frequent than push
#                                       events (no needless 30s re-pull).
#   C2  observe-or-trigger-reconcile — Finder-copy a fresh tree; confirm a
#                                       reconcile/drain becomes observable via
#                                       /activity (busy) and/or the log window.
#   C3  parallel-listing-no-stall    — while the reconcile/drain is in flight,
#                                       sample dir listings repeatedly; EVERY
#                                       sample must be < QA_SNAPPY_MS (the
#                                       "no Finder spinner" / no system freeze
#                                       guarantee). Includes an unrelated dir
#                                       (system-wide, not just the copied one).
#   C4  parallel-finder-copy-no-stall— while the reconcile is in flight, run a
#                                       SMALL real Finder copy into a separate
#                                       dest, bounded tight; it must COMPLETE
#                                       (not freeze), then drain + full custody.
#
# After every write op: qa_wait_drain → qa_verify_custody → qa_error_scan,
# per battery principles #2/#3 (chain of custody + zero-Finder-error gate).
#
# Non-destructive: all dests are $QA_DEST_ROOT/QA_<epoch>_<pid>_* (qa_cleanup
# guard). Restores offline state on exit. Bounded probes via qa_timeout/perl.
# bash 3.2-safe. AUTHORED here; the orchestrator runs it later. NEVER touches
# the user's real folders.
#
# Final line: VERDICT: 'PASS 10-index-stability' or
#             'FAIL 10-index-stability: <reason with offending path>'.
# ===========================================================================

set -uo pipefail

source "$(dirname "$0")/lib.sh"

QA_CAT_LABEL="10-index-stability"

# --- EXIT trap: restore online state, clean our own data only -------------
# qa_cleanup (called by qa_end) already removes our dest subtree + staging
# under its guard. We additionally force the mount back ONLINE in case a case
# left it offline (none here toggle offline, but be defensive) and best-effort
# clean our staging dir directly in case we exit before qa_end.
_index_stability_cleanup() {
    # Restore online (idempotent; harmless if already online).
    qa_offline off >/dev/null 2>&1 || true
    # qa_cleanup is guarded to ONLY our $MOUNT/JM_RELEASE_BATTERY/QA_*_$$_* +
    # /tmp/jm-battery-stage-$$ — safe to call again on the exit path.
    qa_cleanup >/dev/null 2>&1 || true
}
trap _index_stability_cleanup EXIT

qa_begin "$QA_CAT_LABEL"

# Preflight gate — self-skip cleanly if the environment isn't safe.
if ! qa_preflight; then
    qa_warn "preflight failed — skipping $QA_CAT_LABEL (environment not ready)"
    qa_end
    echo "VERDICT: FAIL $QA_CAT_LABEL: preflight failed (control plane / mount not ready)"
    exit 0
fi

# Tunable knobs (env-overridable so the orchestrator can scale QUICK/full).
: "${IDX_TREE_DIRS:=20}"          # reconcile-driver tree: subdirs
: "${IDX_TREE_FPD:=15}"           # files per subdir (20*15 = 300 dirents to index)
: "${IDX_TREE_SIZE:=262144}"      # 256 KiB each — enough drain to keep reconcile busy
: "${IDX_SMALL_DIRS:=2}"          # parallel small copy: subdirs
: "${IDX_SMALL_FPD:=5}"           # parallel small copy: files per subdir
: "${IDX_SMALL_SIZE:=131072}"     # 128 KiB each — small, must NOT stall
: "${IDX_LISTING_SAMPLES:=12}"    # how many listing samples to take during reconcile
: "${IDX_SMALL_COPY_BOUND:=45}"   # bound (s) for the parallel small Finder copy —
                                  # generous, but a freeze (the Chrome-picker bug)
                                  # would blow past it and FAIL.
: "${IDX_OBSERVE_SECS:=40}"       # max seconds to watch /activity for a reconcile

DEST="$QA_DEST_ROOT/$(qa_unique_tag QA)"
mkdir -p "$DEST" 2>/dev/null
qa_info "dest=$DEST  mount=$MOUNT  cp=$CP_BASE"

# An unrelated, pre-existing dir on the mount to prove SYSTEM-WIDE non-stall
# (the symptom froze ALL Finder, not just the copy's folder). We use the dest
# root itself (always present, never the user's real folders).
UNRELATED_DIR="$QA_DEST_ROOT"

# Helper: is a reconcile (or any background work) currently observable?
# Echoes "busy" if /activity reports busy=true OR a reconcile op active=true;
# echoes "idle" otherwise; echoes "err" if /activity is unreachable.
_idx_activity_state() {
    local body
    body="$(qa_timeout 5 curl -s "$CP_BASE/activity" 2>/dev/null)"
    if [ -z "$body" ]; then echo "err"; return; fi
    # Reconcile op active?  ...{"kind":"reconcile","active":true,...}
    if printf '%s' "$body" \
        | tr '{}' '\n\n' \
        | grep -q '"kind":"reconcile"' \
        && printf '%s' "$body" | grep -Eq '"kind":"reconcile","active":true|"reconcile".*"active":true'; then
        echo "busy"; return
    fi
    # Or any background work (drain/prefetch) — the system-wide load condition.
    if printf '%s' "$body" | grep -Eq '"busy"[[:space:]]*:[[:space:]]*true'; then
        echo "busy"; return
    fi
    echo "idle"
}

# ===========================================================================
# C1 — build uses keyspace-PUSH, not needless full SCAN
# ===========================================================================
qa_sec "C1 build-uses-keyspace-push"
c1_mark=$(qa_log_mark)
echo "[CASE] C1 build-uses-keyspace-push — inspecting log for push subscription + SCAN frequency"

# Look back over the existing log (push loop logs once at startup) for the
# enable markers, then we re-check the live window after driving churn in C2.
push_enabled=0
if [ -f "$JM_LOG" ]; then
    if grep -qE 'metadata keyspace push: (loop starting|subscribed)' "$JM_LOG" 2>/dev/null; then
        push_enabled=1
    fi
fi
if [ "$push_enabled" -eq 1 ]; then
    qa_pass "C1: build has metadata keyspace-PUSH enabled (loop starting/subscribed in log)"
else
    # Not fatal to the stability gate, but it IS the configuration that prevents
    # the 30s re-pulls. Warn loudly so the release owner notices a non-push build.
    qa_warn "C1: no 'metadata keyspace push: loop starting/subscribed' in $JM_LOG — build may NOT be using keyspace-push (JM_METADATA_KEYSPACE_PUSH unset?). Stability cases still run."
fi

# Frequency check is done AFTER C2 drives churn (so we have push events to
# compare against); the counts use inline `grep -c ... | tr -d ' '` at that point.

# ===========================================================================
# C2 — observe / trigger a reconcile via a real Finder copy of a fresh tree
# ===========================================================================
qa_sec "C2 observe-or-trigger-reconcile"
echo "[CASE] C2 observe-or-trigger-reconcile — staging tree + real Finder copy to drive a reconcile"

SRC_TREE="$QA_STAGE/idxtree"
md_manifest="$(qa_stage_tree "$SRC_TREE" "$IDX_TREE_DIRS" "$IDX_TREE_FPD" "$IDX_TREE_SIZE")"
qa_info "staged reconcile-driver tree: $SRC_TREE (manifest $md_manifest)"

c2_mark=$(qa_log_mark)

# Launch the real Finder copy in the BACKGROUND so we can observe /activity and
# run the parallel-listing/parallel-copy cases WHILE it (and the resulting
# reconcile) are in flight. We capture its result/rc via temp files.
COPY_OUT="$QA_CAT_DIR/c2-finder-copy.out"
COPY_RC="$QA_CAT_DIR/c2-finder-copy.rc"
: > "$COPY_OUT"; : > "$COPY_RC"
(
    out="$(qa_finder_copy "$SRC_TREE" "$DEST")"
    rc=$?
    printf '%s\n' "$out" > "$COPY_OUT"
    printf '%s\n' "$rc"  > "$COPY_RC"
) &
COPY_PID=$!

# Watch /activity (and the log) until we SEE a reconcile/drain busy, or the
# observation window elapses. We don't fail C2 if we never catch the activity
# transition (the copy can be quick); we just record whether we observed it,
# and the log window will still tell us if a reconcile ran.
observed_busy=0
waited=0
while [ "$waited" -lt "$IDX_OBSERVE_SECS" ]; do
    st="$(_idx_activity_state)"
    if [ "$st" = "busy" ]; then
        observed_busy=1
        break
    fi
    # If the copy already finished AND we never saw busy, stop waiting.
    if ! kill -0 "$COPY_PID" 2>/dev/null && [ "$st" = "idle" ]; then
        break
    fi
    perl -e 'select undef,undef,undef,0.5'
    waited=$((waited+1))
done

# Did the log window show reconcile activity (push reconcileDir OR full SCAN)?
log_recon=0
if [ -f "$JM_LOG" ]; then
    tail -n +$(( c2_mark + 1 )) "$JM_LOG" 2>/dev/null \
        | grep -qE 'metadata keyspace push: reconcileDir|metadata sync complete|metadata keyspace push: burst over ceiling' \
        && log_recon=1
fi

if [ "$observed_busy" -eq 1 ] || [ "$log_recon" -eq 1 ]; then
    qa_pass "C2: reconcile/drain activity observed during the copy (activity_busy=$observed_busy log_reconcile=$log_recon)"
else
    qa_warn "C2: did not catch a reconcile transition in the ${IDX_OBSERVE_SECS}s window (copy may have been too quick); stability cases below still exercise the in-flight path"
fi

# IMPORTANT: while the background copy/reconcile is STILL in flight, run C3 + C4.
# Only after those do we join the copy + drain + verify custody.

# ===========================================================================
# C3 — parallel directory listings must NOT stall during the reconcile
# ===========================================================================
qa_sec "C3 parallel-listing-no-stall"
echo "[CASE] C3 parallel-listing-no-stall — sampling dir listings WHILE reconcile/drain runs"

slow_listing=0
failed_listing=0
worst_ms=0
samples_taken=0
i=1
while [ "$i" -le "$IDX_LISTING_SAMPLES" ]; do
    # Alternate between the unrelated (system-wide) dir and the actively-copied
    # dest, so we prove BOTH a foreground-unrelated listing and the busy dir
    # stay snappy — the Chrome-picker freeze was system-wide.
    if [ $(( i % 2 )) -eq 0 ]; then
        target="$UNRELATED_DIR"; tname="unrelated($UNRELATED_DIR)"
    else
        target="$DEST"; tname="copydest($DEST)"
    fi
    ms="$(qa_dir_listing_ms "$target")"
    samples_taken=$((samples_taken+1))
    if [ "$ms" = "-1" ]; then
        qa_fail "C3: dir listing FAILED (readdir error) on $tname during reconcile"
        failed_listing=$((failed_listing+1))
    else
        [ "$ms" -gt "$worst_ms" ] && worst_ms="$ms"
        if [ "$ms" -ge "$QA_SNAPPY_MS" ]; then
            qa_fail "C3: dir listing STALLED ${ms}ms (budget ${QA_SNAPPY_MS}ms) on $tname during reconcile — index rebuild is blocking foreground listings"
            slow_listing=$((slow_listing+1))
        else
            qa_info "C3 sample $i: ${ms}ms on $tname (ok < ${QA_SNAPPY_MS}ms)"
        fi
    fi
    # Small spacing so we sample across the reconcile/drain, not all at once.
    perl -e 'select undef,undef,undef,0.4'
    i=$((i+1))
done

if [ "$slow_listing" -eq 0 ] && [ "$failed_listing" -eq 0 ]; then
    qa_pass "C3: all $samples_taken parallel listings stayed snappy during reconcile (worst=${worst_ms}ms < ${QA_SNAPPY_MS}ms)"
fi

# ===========================================================================
# C4 — a parallel small Finder copy must COMPLETE (not freeze) during reconcile
# ===========================================================================
qa_sec "C4 parallel-finder-copy-no-stall"
echo "[CASE] C4 parallel-finder-copy-no-stall — small real Finder copy WHILE reconcile/drain runs"

SMALL_SRC="$QA_STAGE/idxsmall"
small_manifest="$(qa_stage_tree "$SMALL_SRC" "$IDX_SMALL_DIRS" "$IDX_SMALL_FPD" "$IDX_SMALL_SIZE")"
SMALL_DEST="$QA_DEST_ROOT/$(qa_unique_tag QA)"
mkdir -p "$SMALL_DEST" 2>/dev/null

# Confirm we're still under load when we launch the small copy; if the big copy
# already finished, the case is still valid (we assert non-stall regardless),
# but log the state so a human can interpret.
pre_state="$(_idx_activity_state)"
qa_info "C4: activity state at small-copy launch = $pre_state"

c4_mark=$(qa_log_mark)
# Time the small copy ourselves with a TIGHT outer bound. A freeze (the reported
# Chrome-picker symptom) would block past IDX_SMALL_COPY_BOUND; qa_finder_copy's
# own bound also caps it. We measure wall time to surface sluggishness short of a
# hard freeze.
c4_t0="$(_qa_now_ms)"
small_out="$(qa_finder_copy "$SMALL_SRC" "$SMALL_DEST" "$IDX_SMALL_COPY_BOUND")"
small_rc=$?
c4_t1="$(_qa_now_ms)"
c4_elapsed_ms=$(( c4_t1 - c4_t0 ))
printf '%s\n' "$small_out" > "$QA_CAT_DIR/c4-finder-copy.out"

# rc 142 == perl alarm timeout (128+SIGALRM) == the copy did NOT complete in
# the bound == a stall/freeze. That's the headline failure for this category.
if [ "$small_rc" -eq 142 ]; then
    qa_fail "C4: parallel small Finder copy DID NOT COMPLETE within ${IDX_SMALL_COPY_BOUND}s during reconcile (FREEZE) — dest $SMALL_DEST"
elif [ "$small_rc" -ne 0 ]; then
    qa_fail "C4: parallel small Finder copy errored (rc=$small_rc) during reconcile: $small_out — dest $SMALL_DEST"
else
    if qa_finder_result_clean "$small_out"; then
        qa_pass "C4: parallel small Finder copy completed in ${c4_elapsed_ms}ms during reconcile (no freeze) — dest $SMALL_DEST"
    else
        qa_fail "C4: parallel small Finder copy returned a gated error code during reconcile: $small_out — dest $SMALL_DEST"
    fi
fi

# ===========================================================================
# Join the background big copy, then drain + verify custody for BOTH copies.
# ===========================================================================
qa_sec "join + chain-of-custody"

# Wait (bounded) for the background big copy to finish writing into the spool.
join_waited=0
while kill -0 "$COPY_PID" 2>/dev/null && [ "$join_waited" -lt 600 ]; do
    perl -e 'select undef,undef,undef,1'
    join_waited=$((join_waited+1))
done
wait "$COPY_PID" 2>/dev/null

big_out="$(cat "$COPY_OUT" 2>/dev/null)"
big_rc="$(cat "$COPY_RC" 2>/dev/null)"
[ -z "$big_rc" ] && big_rc=1

if [ "$big_rc" = "0" ] && qa_finder_result_clean "$big_out"; then
    qa_pass "C2: reconcile-driver Finder copy completed clean ($DEST)"
else
    qa_fail "C2: reconcile-driver Finder copy failed (rc=$big_rc) result=$big_out — dest $DEST"
fi

# MANDATORY drain gate before any custody verify (principle #2).
echo "[CASE] drain — waiting for spool to fully drain before custody verify"
if qa_wait_drain; then
    qa_pass "spool fully drained (pending_files==0 && in_progress==0)"
else
    qa_fail "spool did NOT drain within ${QA_DRAIN_TIMEOUT}s after index-stability copies — dest $DEST / $SMALL_DEST"
fi

# Full chain of custody for the big reconcile-driver tree.
echo "[CASE] custody — verifying reconcile-driver tree md5 end-to-end"
if qa_verify_custody "$md_manifest" "$DEST/$(basename "$SRC_TREE")"; then
    qa_pass "custody OK: reconcile-driver tree round-tripped byte-identical ($DEST/$(basename "$SRC_TREE"))"
else
    qa_fail "custody FAILED on reconcile-driver tree — see named paths above ($DEST/$(basename "$SRC_TREE"))"
fi

# Full chain of custody for the parallel small copy (only if it landed at all).
echo "[CASE] custody — verifying parallel small copy md5 end-to-end"
if [ "$small_rc" = "0" ]; then
    if qa_verify_custody "$small_manifest" "$SMALL_DEST/$(basename "$SMALL_SRC")"; then
        qa_pass "custody OK: parallel small copy round-tripped byte-identical ($SMALL_DEST/$(basename "$SMALL_SRC"))"
    else
        qa_fail "custody FAILED on parallel small copy — see named paths above ($SMALL_DEST/$(basename "$SMALL_SRC"))"
    fi
else
    qa_warn "skipping custody on parallel small copy (it did not complete; failure already recorded for $SMALL_DEST)"
fi

# ===========================================================================
# C1 (continued) — SCAN-frequency assertion now that C2 drove real churn.
# ===========================================================================
qa_sec "C1 SCAN-vs-push frequency"
echo "[CASE] C1 SCAN-frequency — comparing full re-pull events vs push events over the window"

if [ -f "$JM_LOG" ] && [ "$push_enabled" -eq 1 ]; then
    win="$QA_CAT_DIR/c1-keyspace.window"
    tail -n +$(( c1_mark + 1 )) "$JM_LOG" 2>/dev/null > "$win"
    push_events=$(grep -c 'metadata keyspace push: reconcileDir' "$win" 2>/dev/null | tr -d ' ')
    [ -n "$push_events" ] || push_events=0
    promote_scans=$(grep -c 'metadata keyspace push: burst over ceiling, promoting to full SCAN' "$win" 2>/dev/null | tr -d ' ')
    [ -n "$promote_scans" ] || promote_scans=0
    full_scans=$(grep -c 'metadata sync complete' "$win" 2>/dev/null | tr -d ' ')
    [ -n "$full_scans" ] || full_scans=0
    qa_info "C1 window: push_reconcileDir=$push_events promote_to_SCAN=$promote_scans full_SCAN_complete=$full_scans"

    # A light-churn window (one 300-dirent tree + one 10-file tree) must be
    # served INCREMENTALLY. A needless full 30s re-pull shows up as a
    # promote-to-SCAN or a periodic "metadata sync complete" out of proportion
    # to push events. We FAIL if full re-pulls OUTNUMBER push events (i.e. the
    # build is re-pulling instead of pushing), or if a burst promotion fired on
    # this small batch (the batch is far below any sane burst ceiling).
    total_fullpull=$(( promote_scans + full_scans ))
    if [ "$promote_scans" -gt 0 ]; then
        qa_fail "C1: keyspace push PROMOTED to a full SCAN ($promote_scans×) on a tiny ${IDX_TREE_DIRS}-dir batch — burst ceiling mis-tuned, causing needless 30s re-pulls"
    elif [ "$push_events" -gt 0 ] && [ "$total_fullpull" -gt "$push_events" ]; then
        qa_fail "C1: full re-pulls ($total_fullpull) outnumber push events ($push_events) over the window — build is re-pulling the whole index instead of applying incremental keyspace deltas"
    elif [ "$push_events" -eq 0 ] && [ "$full_scans" -gt 1 ]; then
        qa_fail "C1: zero push events but $full_scans full SCAN reconciles in-window — index updates went via expensive re-pull, not keyspace push"
    else
        qa_pass "C1: incremental keyspace-push dominated (push=$push_events, full_repull=$total_fullpull) — no needless 30s re-pull"
    fi
else
    qa_warn "C1: skipping SCAN-frequency assertion (push not enabled or log missing) — non-push build cannot be measured for push/SCAN ratio"
fi

# ===========================================================================
# Zero-random-Finder-error gate across the WHOLE category window (principle #3)
# ===========================================================================
qa_sec "error scan"
echo "[CASE] error-scan — scanning log window for STALE/phantom/100070/100060/-48/-36/-5000/perm"
scan_line="$(qa_error_scan "$c2_mark" "$QA_CAT_LABEL")"
qa_info "$scan_line"
# Parse the tally; any nonzero = FAIL with the offending path pulled from the
# dumped errscan artifact. No known-open edges are whitelisted for THIS category
# (the ._-heavy STALE edge lives in 07-dotunderscore-heavy.sh; here a STALE
# during a reconcile is a real regression).
es_stale=$(printf '%s' "$scan_line"   | sed -n 's/.*STALE=\([0-9]*\).*/\1/p')
es_phantom=$(printf '%s' "$scan_line" | sed -n 's/.*PHANTOM=\([0-9]*\).*/\1/p')
es_e70=$(printf '%s' "$scan_line"     | sed -n 's/.*E100070=\([0-9]*\).*/\1/p')
es_e60=$(printf '%s' "$scan_line"     | sed -n 's/.*E100060=\([0-9]*\).*/\1/p')
es_e48=$(printf '%s' "$scan_line"     | sed -n 's/.*E48=\([0-9]*\).*/\1/p')
es_e36=$(printf '%s' "$scan_line"     | sed -n 's/.*E36=\([0-9]*\).*/\1/p')
es_e5000=$(printf '%s' "$scan_line"   | sed -n 's/.*E5000=\([0-9]*\).*/\1/p')
es_perm=$(printf '%s' "$scan_line"    | sed -n 's/.*PERM=\([0-9]*\).*/\1/p')
errscan_dump="$QA_CAT_DIR/errscan-${QA_CAT_LABEL}.txt"
for pair in "STALE=$es_stale" "PHANTOM=$es_phantom" "E100070=$es_e70" \
            "E100060=$es_e60" "E48=$es_e48" "E36=$es_e36" \
            "E5000=$es_e5000" "PERM=$es_perm"; do
    name="${pair%%=*}"; cnt="${pair#*=}"
    [ -z "$cnt" ] && cnt=0
    if [ "$cnt" -gt 0 ]; then
        off="$(head -n1 "$errscan_dump" 2>/dev/null)"
        qa_fail "error-scan: $name=$cnt in reconcile window (first offending: ${off:-see $errscan_dump})"
    fi
done
if [ "${es_stale:-0}" -eq 0 ] && [ "${es_phantom:-0}" -eq 0 ] && [ "${es_e70:-0}" -eq 0 ] \
   && [ "${es_e60:-0}" -eq 0 ] && [ "${es_e48:-0}" -eq 0 ] && [ "${es_e36:-0}" -eq 0 ] \
   && [ "${es_e5000:-0}" -eq 0 ] && [ "${es_perm:-0}" -eq 0 ]; then
    qa_pass "error-scan: zero Finder/NFS error signatures during the reconcile window"
fi

# Mount must still be healthy after all the in-flight load.
if qa_health >/dev/null 2>&1; then
    qa_pass "mount still healthy after index-stability load"
else
    qa_fail "mount NOT healthy after index-stability load (control plane /health != healthy) — dest $DEST"
fi

# ===========================================================================
# Verdict
# ===========================================================================
qa_end

if [ "${QA_FAIL:-0}" -eq 0 ]; then
    echo "VERDICT: PASS $QA_CAT_LABEL"
else
    # Surface the most actionable reason: prefer a stall/freeze, then custody,
    # then SCAN-frequency, then error-scan. The detailed per-FAIL lines (with
    # offending paths) are already printed above.
    reason="see [FAIL] lines above"
    if [ "${slow_listing:-0}" -gt 0 ] || [ "${failed_listing:-0}" -gt 0 ]; then
        reason="parallel dir listing stalled/failed during reconcile (>=${QA_SNAPPY_MS}ms) — dest $DEST"
    elif [ "${small_rc:-1}" = "142" ]; then
        reason="parallel small Finder copy froze during reconcile (>${IDX_SMALL_COPY_BOUND}s) — dest $SMALL_DEST"
    fi
    echo "VERDICT: FAIL $QA_CAT_LABEL: $reason"
fi

exit 0
