#!/usr/bin/env bash
# 08-chain-of-custody.sh — JuiceMount RELEASE BATTERY: the INTEGRITY SPINE.
#
# WHY THIS CATEGORY EXISTS
#   This is the test the whole release hinges on. Other categories prove a
#   particular SHAPE lands (sizes, trees, unicode...). THIS one is the dedicated
#   end-to-end CUSTODY STRESS: a large mixed corpus — including a >1GB
#   camera-original-scale file — copied by Finder must traverse the full chain
#   (NFS handler -> write spool -> drainer/at-rest SHA verify -> JuiceFS backend)
#   and read back byte-identical to the source, with the spool observably
#   carrying then fully draining it, and ZERO gated Finder errors anywhere.
#
# WHAT IT PROVES (RELEASE_TESTING.md row 08)
#   1. REAL Finder duplicate drives the copy (osascript via qa_finder_copy), never
#      cp/dd/ditto. Synthetic ops false-green; Finder exercises the true NFS path.
#   2. The payload genuinely TRANSITS THE SPOOL: pending_files is observed to RISE
#      during the copy (proof the writes were spooled), then qa_wait_drain confirms
#      it returns to 0 / in_progress 0 — the drainer pushed every byte to the
#      backend AT REST (the >1GB file forces the long-tail drain + SHA-at-rest path).
#   3. PER-FILE CUSTODY after the MANDATORY drain: md5(landed) == md5(source) for
#      every manifest entry, MISSING==0 && WRONG==0. Verified by a SECOND
#      independent readback sweep too (cache-vs-backend agreement; a divergence is
#      a torn/stale read bug).
#   4. ZERO random Finder errors across the whole window (qa_error_scan clean:
#      FromHandle STALE / purging phantom / 100070 / 100060 / -48 / -36 / -5000 /
#      permission). Any hit = FAIL naming the offending path.
#
# NON-DESTRUCTIVE: stages bytes under $QA_STAGE (off-mount), copies into a UNIQUE
# QA_<epoch>_<pid>_<rand> dest under $QA_DEST_ROOT ($MOUNT/JM_RELEASE_BATTERY); the
# EXIT/INT/TERM trap restores online state and removes ONLY our own dest + stage.
# AUTHORED here; the orchestrator runs it live later — do NOT execute it against
# the live mount from here (mount-safety: never churn a live mount).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$HERE/lib.sh"

# --- tunables (overridable by env; the >1GB file + a longer drain ceiling) ------
: "${COC_SZ_GB:=1181116006}"      # ~1.1 GB (> 1 GiB; camera-original scale)
: "${COC_DRAIN_CEIL:=1800}"       # drain timeout for the GB-class long tail (s)
: "${COC_TREE_DIRS:=8}"           # corpus tree breadth
: "${COC_TREE_FPD:=20}"           # files per dir
: "${COC_TREE_SZ:=49152}"         # 48KB small-file size

# --- state the EXIT trap must always restore/clean -----------------------------
COC_SRC=""
COC_DEST=""
_coc_cleanup() {
    # Always leave the box ONLINE (08 never toggles offline, but the convention +
    # spec require restoring online state defensively even on interrupt).
    qa_offline off >/dev/null 2>&1 || true
    [ -n "$COC_DEST" ] && rm -rf "$COC_DEST" 2>/dev/null
    [ -n "$COC_SRC" ]  && rm -rf "$COC_SRC"  2>/dev/null
    qa_cleanup 2>/dev/null || true
}
trap _coc_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# _coc_first_bad MANIFEST DEST — echo the first MISSING/WRONG file (path + detail)
# for the VERDICT reason. Called BEFORE qa_end cleans the dest, only on failure.
_coc_first_bad() {
    local man="$1" dest="$2" rel want landed got tab
    tab="$(printf '\t')"
    while IFS="$tab" read -r rel want; do
        [ -z "$rel" ] && continue
        landed="$dest/$rel"
        if [ ! -f "$landed" ]; then printf '%s' "$landed (MISSING)"; return; fi
        got="$(md5 -q "$landed" 2>/dev/null)"
        [ "$got" = "$want" ] && [ -n "$got" ] || { printf '%s' "$landed (md5 want=$want got=$got)"; return; }
    done < "$man"
}

main() {
    if ! qa_preflight; then
        echo "VERDICT: FAIL 08-chain-of-custody: preflight failed (mount/control-plane not ready) — cannot certify custody"
        return 0
    fi
    qa_begin "08-chain-of-custody"

    local TAG SRC MAN DEST
    TAG="$(qa_unique_tag QA)"          # QA_<epoch>_<pid>_<rand> — matches qa_cleanup's guard
    SRC="$QA_STAGE/$TAG";   COC_SRC="$SRC"
    DEST="$QA_DEST_ROOT/$TAG"; COC_DEST="$DEST"

    # --- stage a large mixed corpus (tree w/ ._ companions + size singletons + >1GB) ---
    qa_sec "staging large mixed corpus under $SRC (incl. a >1GB file)"
    MAN="$(qa_stage_tree "$SRC" "$COC_TREE_DIRS" "$COC_TREE_FPD" "$COC_TREE_SZ" 1)"
    local spec rel size md
    for spec in "empty.dat:0" "onebyte.dat:1" "kb.xmp:4096" "mb5.still:5242880" "mb100.proxy:104857600" "gig.mov:$COC_SZ_GB"; do
        rel="${spec%%:*}"; size="${spec##*:}"
        md="$(qa_stage_file "$SRC/$rel" "$size")"
        printf '%s\t%s\n' "$rel" "$md" >> "$MAN"
    done
    local NF; NF="$(wc -l < "$MAN" | tr -d ' ')"
    qa_info "staged $NF manifest files ($(du -sh "$SRC" 2>/dev/null | awk '{print $1}'); largest ~$(( COC_SZ_GB / 1048576 ))MB)"

    # Payload-scaled drain ceiling: derive from the ACTUAL staged corpus byte size
    # (du -sk, KiB) so the >1GB long tail is never cut short, and keep the static
    # COC_DRAIN_CEIL as a generous lower bound. A 2-3GB corpus thus gets the full
    # qa_drain_ceiling band (floor 600s, scaled at the 5MB/s drain floor).
    local _coc_bytes; _coc_bytes="$(du -sk "$SRC" 2>/dev/null | awk '{print $1*1024; f=1} END{if(!f) print 0}')"
    local _coc_scaled; _coc_scaled="$(qa_drain_ceiling "$_coc_bytes" $(( NF * 2 )))"
    [ "$_coc_scaled" -gt "$COC_DRAIN_CEIL" ] 2>/dev/null && COC_DRAIN_CEIL="$_coc_scaled"
    qa_info "drain ceiling = ${COC_DRAIN_CEIL}s (corpus ~$(( _coc_bytes / 1048576 ))MB, payload-scaled)"
    qa_assert $([ "$NF" -gt 0 ] && echo 0 || echo 1) \
        "corpus staged ($NF files incl. the >1GB camera-original)" \
        "corpus staging produced an empty manifest"

    # --- clean start: spool idle before the chain begins ---
    qa_wait_drain 120 >/dev/null 2>&1

    # --- REAL Finder copy + observe the spool RISE (proof of transit) ---
    qa_sec "real Finder copy -> $DEST (observing spool transit)"
    local MARK; MARK="$(qa_log_mark)"
    ( qa_finder_copy "$SRC" "$QA_DEST_ROOT" "$COC_DRAIN_CEIL" > "$QA_CAT_DIR/$TAG.finder" 2>&1 ) &
    local cppid=$! peak=0 s pend
    for s in $(seq 1 300); do
        pend="$(qa_spool_pending)"
        [ "$pend" -gt "$peak" ] 2>/dev/null && peak="$pend"
        kill -0 "$cppid" 2>/dev/null || break
        perl -e 'select undef,undef,undef,1'
    done
    wait "$cppid" 2>/dev/null
    local fres; fres="$(cat "$QA_CAT_DIR/$TAG.finder" 2>/dev/null)"
    qa_info "Finder result: $(printf '%s' "$fres" | head -c 120)"
    qa_assert $(qa_finder_result_clean "$fres"; echo $?) \
        "Finder copy returned clean (no -48/-36/-5000/permission)" \
        "Finder copy returned an error: $(printf '%s' "$fres" | head -c 160) (src=$SRC)"
    qa_assert $([ "$peak" -gt 0 ] && echo 0 || echo 1) \
        "payload transited the write spool (peak pending=$peak)" \
        "spool never showed pending writes (peak=$peak) for a >1GB corpus — writes may have bypassed the spool (src=$SRC)"

    # --- MANDATORY full drain to the backend before any custody check ---
    qa_sec "draining to JuiceFS backend (long-tail GB ceiling ${COC_DRAIN_CEIL}s)"
    local drc; qa_wait_drain "$COC_DRAIN_CEIL"; drc=$?
    qa_assert "$drc" \
        "spool fully drained (pending=0, in_progress=0) — every byte is at-rest on the backend" \
        "spool did NOT fully drain within ${COC_DRAIN_CEIL}s (residual stuck) — chain of custody uncertifiable (dest=$DEST)"

    # --- READBACK sweep #1 + #2 (md5 == source; exercises at-rest SHA verify) ---
    qa_sec "custody readback sweep #1 (md5 vs source)"
    local rc1; qa_verify_custody "$MAN" "$DEST"; rc1=$?
    qa_assert "$rc1" \
        "custody sweep #1: every file present + md5-identical to source" \
        "custody sweep #1 found MISSING/WRONG files (offending paths in [FAIL] lines above)"

    qa_sec "custody readback sweep #2 (independent re-read; must agree)"
    local rc2; qa_verify_custody "$MAN" "$DEST"; rc2=$?
    qa_assert "$rc2" \
        "custody sweep #2: at-rest readback stable + identical to source" \
        "custody sweep #2 diverged from source/#1 — inconsistent read (cache vs backend)"

    # Capture the offending path NOW, before qa_end cleans the dest.
    local COC_BAD=""
    if [ "$rc1" -ne 0 ] || [ "$rc2" -ne 0 ]; then
        COC_BAD="$(_coc_first_bad "$MAN" "$DEST")"
    fi

    # --- ZERO random Finder errors across the whole chain window ---
    qa_sec "error-scan over the full Finder->spool->drain->readback window"
    local scan; scan="$(qa_error_scan "$MARK" "08-chain-of-custody")"
    qa_info "$scan"
    qa_assert $(qa_error_scan "$MARK" "08-chain-of-custody" >/dev/null; echo $?) \
        "ZERO random Finder errors during the full custody chain" \
        "random Finder errors during the custody window ($scan) — see errscan-08-chain-of-custody.txt for offending paths"

    qa_end

    # --- single-line authoritative VERDICT (always exit 0; run-all greps this) ---
    if [ "$QA_FAIL" -eq 0 ]; then
        echo "VERDICT: PASS 08-chain-of-custody"
    else
        local reason="$COC_BAD"
        if [ -z "$reason" ]; then
            reason="drain/spool-transit/error-scan failure (dest=$DEST) — see [FAIL] lines + errscan-08-chain-of-custody.txt"
        fi
        echo "VERDICT: FAIL 08-chain-of-custody: $reason"
    fi
}

main "$@"
exit 0
