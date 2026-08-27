#!/usr/bin/env bash
# 02-file-sizes.sh — JuiceMount RELEASE BATTERY: SIZE SPECTRUM through the full
# spool/drain chain of custody.
#
# WHY THIS CATEGORY EXISTS
#   A camera/creator workflow throws every size at the mount: empty sidecars,
#   1-byte markers, KB JSON/XMP, ~5MB stills, ~100MB ProRes proxies, and
#   multi-GB camera-original clips. Each size class stresses a different part of
#   the chain: zero/1-byte exercise CREATE+SETATTR with no WRITE body; KB lands
#   in a single block; ~5MB/~100MB span several durable checkpoints; >1GB forces
#   the long-tail drain where a torn/partial-size read is most likely to surface.
#
# WHAT IT PROVES (per the spec for this category)
#   1. REAL Finder duplicate drives every copy (osascript), NOT cp/dd/ditto.
#      Synthetic ops false-green; Finder exercises the true NFS path
#      (LOOKUP/CREATE/SETATTR/WRITE/READ + ._AppleDouble + xattr forks).
#   2. FULL CHAIN OF CUSTODY per file: Finder write -> NFS handler -> write spool
#      -> drainer (durable checkpoints + SHA verify) -> JuiceFS backend ->
#      readback -> md5 == source. We qa_wait_drain (pending_files==0 &&
#      in_progress==0) BEFORE verifying so we read the at-rest backend copy, not
#      the cache.
#   3. NO TORN/PARTIAL-SIZE READS during the drain tail: for the >1GB file we
#      poll stat size during the drain and assert the landed size only ever
#      equals the FINAL source size once drain completes — never an intermediate
#      partial size that a reader could mistake for "done". (A partial size that
#      equals final, or a size that exceeds final, is a torn-read / over-write
#      bug.)
#   4. ZERO random Finder errors: qa_error_scan over each test window must be
#      clean (FromHandle STALE / purging phantom / 100070 / 100060 / -48 / -36 /
#      -5000 / permission all zero). Any hit = FAIL naming the offending path.
#
# NON-DESTRUCTIVE: stages bytes under $QA_STAGE (off-mount), copies into a UNIQUE
# $QA_DEST_ROOT/QA_<epoch>_<pid>_<rand> dest, and EXIT-traps cleanup of both.
# bash 3.2-safe; bounded probes via qa_timeout (perl alarm), no GNU timeout.
#
# Scripts ALWAYS exit 0; pass/fail is the .summary + the final VERDICT line.
# ===========================================================================

set -uo pipefail

source "$(dirname "$0")/lib.sh"

QA_CAT_NAME="02-file-sizes"

# Size knobs (env-overridable so the orchestrator can run a QUICK pass). Defaults
# hit the full spectrum the spec demands: 0, 1, KB, ~5MB, ~100MB, >1GB.
: "${SZ_ZERO:=0}"
: "${SZ_ONE:=1}"
: "${SZ_KB:=4096}"                 # 4 KiB
: "${SZ_5MB:=5242880}"             # 5 MiB
: "${SZ_100MB:=104857600}"         # 100 MiB
: "${SZ_GB:=1181116006}"           # ~1.1 GB (>1 GiB; camera-original scale)

# The >1GB file gets a longer drain ceiling than the default 300s.
: "${BIG_DRAIN_TIMEOUT:=900}"

# Where we'll copy. Built in main() after preflight so MOUNT is final.
DEST=""
RESTORE_OFFLINE=0   # set to 1 if we ever flip offline and must restore

# ---------------------------------------------------------------------------
# EXIT trap: restore any toggled state, then tear down ONLY our own dest + stage.
# qa_cleanup() already guards to $MOUNT/JM_RELEASE_BATTERY/QA_*_$$_* + $QA_STAGE.
_on_exit() {
    # If anything left us offline, force back online so we never strand the mount.
    if [ "$RESTORE_OFFLINE" = "1" ]; then
        qa_offline off >/dev/null 2>&1 || true
    fi
    # Explicitly remove this run's dest leaf (belt-and-suspenders with qa_cleanup,
    # which qa_end also calls).
    if [ -n "$DEST" ]; then
        case "$DEST" in
            "$MOUNT"/JM_RELEASE_BATTERY/QA_*) rm -rf "$DEST" 2>/dev/null || true ;;
        esac
    fi
}
trap _on_exit EXIT

# ---------------------------------------------------------------------------
# Per-size case runner.
#   run_size_case LABEL SIZE_BYTES DRAIN_TIMEOUT [TORN_CHECK]
# Stages one file of SIZE_BYTES off-mount (with an xattr so ._AppleDouble/forks
# are exercised), real-Finder-copies it into a per-case subdir of $DEST, waits
# for the spool to fully drain, then md5-verifies custody and scans the log
# window for error signatures. If TORN_CHECK=1, monitors landed stat-size during
# the drain tail and asserts it only ever equals the final source size.
run_size_case() {
    local label="$1" size="$2" dtimeout="$3" torn="${4:-0}"
    qa_sec "size case: $label (${size} bytes)"

    local casedir="$DEST/$label"
    mkdir -p "$casedir" 2>/dev/null

    # Stage off-mount. Give every file an xattr so the NFS ._/xattr-fork path is
    # exercised even for the empty/1-byte files (where the body is trivial but
    # CREATE+SETATTR still must round-trip).
    local srcname="clip_${label}.dat"
    local src="$QA_STAGE/$srcname"
    local want_md5 want_size
    want_md5="$(qa_stage_file "$src" "$size" "com.apple.metadata:_kMDItemUserTags=515141")"
    want_size="$(stat -f%z "$src" 2>/dev/null || echo -1)"

    if [ ! -f "$src" ] || [ "$want_size" != "$size" ]; then
        qa_fail "stage failed for $label: $src (want size=$size got=$want_size)"
        return 1
    fi
    qa_info "$label staged: $src size=$want_size md5=$want_md5"

    # Build a one-line manifest (relpath<TAB>md5) so we reuse qa_verify_custody.
    # The landed file will be at $casedir/$srcname (Finder duplicate keeps the
    # source leaf name).
    local manifest="$QA_CAT_DIR/${label}.manifest"
    printf '%s\t%s\n' "$srcname" "$want_md5" > "$manifest"

    # Mark the log window, then drive the REAL Finder copy.
    local mark out rc
    mark=$(qa_log_mark)
    out="$(qa_finder_copy "$src" "$casedir")"
    rc=$?

    if [ "$rc" -ne 0 ]; then
        qa_fail "Finder copy rc=$rc for $label -> $casedir/$srcname : $out"
    else
        qa_pass "Finder copy started/returned for $label -> $casedir/$srcname"
    fi
    if ! qa_finder_result_clean "$out"; then
        qa_fail "Finder result carried a gated error for $label ($casedir/$srcname): $out"
    fi

    # ----- Torn/partial-size guard (the drain-tail assertion) -----------------
    # For the big file we poll the landed file's stat size WHILE the spool is
    # still draining. The invariant: the on-mount size must NEVER report a
    # partial value that a reader could mistake for the finished file. It is
    # acceptable for the file to be absent or 0 early; it is NOT acceptable for
    # it to report an intermediate size in (0, final) and certainly not > final.
    # We sample until drain completes or the timeout fires.
    if [ "$torn" = "1" ]; then
        _torn_watch "$casedir/$srcname" "$want_size" "$dtimeout" "$label"
    fi

    # ----- MANDATORY drain gate before any custody verify ---------------------
    # Use a payload-SCALED ceiling so a multi-GB drain is never cut short. We take
    # the GREATER of the caller-supplied dtimeout and the size-derived ceiling
    # (ceil(size/5MiB), floor 600s) — the static value stays a lower bound.
    local dceil; dceil="$(qa_drain_ceiling "$size")"
    [ "$dtimeout" -gt "$dceil" ] 2>/dev/null && dceil="$dtimeout"
    if ! qa_wait_drain "$dceil"; then
        qa_fail "spool did not drain within ${dceil}s for $label ($casedir/$srcname)"
    else
        qa_pass "spool drained for $label"
    fi

    # No quarantine / SHA-mismatch allowed (drainer at-rest verify). qa_spool_field
    # echoes -1 on CP error — treat <=0 as "no quarantine".
    local q f
    q="$(qa_spool_field quarantined)"
    f="$(qa_spool_actionable_failed)"
    if [ "${q:-0}" -gt 0 ] 2>/dev/null; then
        qa_fail "quarantined=$q after drain for $label ($casedir/$srcname) — SHA at-rest mismatch?"
    fi
    if [ "${f:-0}" -gt 0 ] 2>/dev/null; then
        qa_fail "failed_files=$f after drain for $label ($casedir/$srcname)"
    fi

    # ----- Custody (the END of the chain: at-rest readback md5 == source) -----
    if qa_verify_custody "$manifest" "$casedir"; then
        qa_pass "custody OK for $label ($casedir/$srcname)"
    else
        qa_fail "custody FAILED for $label ($casedir/$srcname)"
    fi

    # ----- Final landed-size sanity (must equal final source size exactly) ----
    local landed_size
    landed_size="$(stat -f%z "$casedir/$srcname" 2>/dev/null || echo -1)"
    if [ "$landed_size" = "$want_size" ]; then
        qa_pass "landed size == source size ($want_size) for $label"
    else
        qa_fail "landed size mismatch for $label ($casedir/$srcname): want=$want_size got=$landed_size"
    fi

    # ----- Zero-random-Finder-error gate over this case's window --------------
    local scan
    scan="$(qa_error_scan "$mark" "$label")"
    qa_info "$scan"
    if printf '%s' "$scan" | grep -q 'STALE=0 PHANTOM=0 E100070=0 E100060=0 E48=0 E36=0 E5000=0 PERM=0'; then
        qa_pass "no Finder error signatures in $label window"
    else
        # Name the offending path from the dumped errscan file when possible.
        local errfile="$QA_CAT_DIR/errscan-${label}.txt" firstpath
        firstpath="$(head -n1 "$errfile" 2>/dev/null)"
        qa_fail "Finder error signature in $label window ($scan) ${firstpath:+first=$firstpath}"
    fi

    # Stage cleanup of this size's source as we go (keep /tmp small for the GB file).
    rm -f "$src" 2>/dev/null || true
}

# _torn_watch LANDED_PATH FINAL_SIZE TIMEOUT_S LABEL
#   Poll the landed file's stat size during the drain tail. Assert it NEVER
#   reports a partial size in (0, FINAL_SIZE) and NEVER exceeds FINAL_SIZE.
#   Absent or 0 is fine (not yet drained). Stops once the spool has drained
#   (pending_files==0 && in_progress==0) or the timeout elapses. Polls fast
#   (~250ms) to actually catch a torn intermediate.
_torn_watch() {
    local path="$1" final="$2" timeout="$3" label="$4"
    local waited_ms=0 step_ms=250 budget_ms=$(( timeout * 1000 ))
    local saw_partial=0 saw_over=0 p ip sz
    qa_info "torn-watch arming for $label (final=$final bytes)"
    while [ "$waited_ms" -lt "$budget_ms" ]; do
        if [ -e "$path" ]; then
            sz="$(stat -f%z "$path" 2>/dev/null || echo -1)"
            if [ "$sz" -gt "$final" ] 2>/dev/null; then
                saw_over=1
                qa_fail "TORN/OVER read: $path reported size=$sz > final=$final for $label (drain tail)"
                break
            elif [ "$sz" -gt 0 ] 2>/dev/null && [ "$sz" -lt "$final" ] 2>/dev/null; then
                # Intermediate partial visible on the mount during drain. This is
                # the exact bug class we gate: a reader stat-ing now would see a
                # short file. Record once (don't spam) and keep watching to see if
                # it ever "settles wrong".
                if [ "$saw_partial" = "0" ]; then
                    qa_fail "TORN/PARTIAL size visible during drain: $path size=$sz < final=$final for $label"
                fi
                saw_partial=1
            fi
        fi
        # Stop watching once the spool is drained — past this point any size we
        # read is the settled value, checked by the post-drain landed-size assert.
        p="$(qa_spool_pending)"
        ip="$(qa_spool_field in_progress)"
        if [ "$p" = "0" ] && { [ "$ip" = "0" ] || [ "$ip" = "-1" ]; }; then
            break
        fi
        perl -e 'select undef,undef,undef,0.25'
        waited_ms=$(( waited_ms + step_ms ))
    done
    if [ "$saw_partial" = "0" ] && [ "$saw_over" = "0" ]; then
        qa_pass "no torn/partial-size read observed during $label drain tail"
    fi
}

# ---------------------------------------------------------------------------
main() {
    qa_begin "$QA_CAT_NAME"

    if ! qa_preflight; then
        qa_warn "preflight failed — skipping $QA_CAT_NAME (battery may be aborting)"
        qa_end
        echo "VERDICT: FAIL $QA_CAT_NAME: preflight failed (control plane / mount / dest-root guard)"
        exit 0
    fi

    DEST="$QA_DEST_ROOT/$(qa_unique_tag QA)"
    if ! mkdir -p "$DEST" 2>/dev/null; then
        qa_fail "could not create dest $DEST"
        qa_end
        echo "VERDICT: FAIL $QA_CAT_NAME: could not create dest $DEST"
        exit 0
    fi
    qa_info "dest for this run: $DEST"

    # Snappiness sanity: the dest dir listing must be instant from the local DB.
    local list_ms
    list_ms="$(qa_dir_listing_ms "$DEST")"
    if [ "$list_ms" = "-1" ]; then
        qa_fail "dest listing failed (readdir) for $DEST"
    elif [ "$list_ms" -le "$QA_SNAPPY_MS" ] 2>/dev/null; then
        qa_pass "dest listing snappy (${list_ms}ms <= ${QA_SNAPPY_MS}ms) for $DEST"
    else
        qa_fail "dest listing SLOW (${list_ms}ms > ${QA_SNAPPY_MS}ms) for $DEST"
    fi

    # The full size spectrum. Small/medium use the default drain ceiling; the
    # >1GB file uses BIG_DRAIN_TIMEOUT and turns on the torn-read watcher.
    run_size_case "zero"   "$SZ_ZERO"   "$QA_DRAIN_TIMEOUT" 0
    run_size_case "onebyte" "$SZ_ONE"   "$QA_DRAIN_TIMEOUT" 0
    run_size_case "kb"     "$SZ_KB"     "$QA_DRAIN_TIMEOUT" 0
    run_size_case "mb5"    "$SZ_5MB"    "$QA_DRAIN_TIMEOUT" 0
    run_size_case "mb100"  "$SZ_100MB"  "$QA_DRAIN_TIMEOUT" 0
    run_size_case "gb1"    "$SZ_GB"     "$BIG_DRAIN_TIMEOUT" 1

    # Mount must still be healthy after the GB drain.
    if qa_health >/dev/null 2>&1; then
        qa_pass "control plane healthy after size spectrum"
    else
        qa_fail "control plane NOT healthy after size spectrum (post-GB)"
    fi

    qa_end
    local fails=$?

    if [ "$fails" -eq 0 ]; then
        echo "VERDICT: PASS $QA_CAT_NAME"
    else
        # Surface the first FAIL reason (with its path) from this category's
        # artifacts/transcript so the one-line verdict is actionable.
        echo "VERDICT: FAIL $QA_CAT_NAME: $fails failing assertion(s) — see $QA_CAT_DIR/.summary and errscan-*.txt (size spectrum: zero/onebyte/kb/mb5/mb100/gb1; check custody/torn/errscan for the offending dest under $DEST)"
    fi
    exit 0
}

main "$@"
