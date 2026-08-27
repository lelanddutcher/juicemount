#!/usr/bin/env bash
# ===========================================================================
# 09-finder-snappiness.sh — JuiceMount RELEASE BATTERY: directory-open SNAPPINESS
#
# THE GATE (hard requirement, principle #4):
#   Opening any directory on the mount must return its listing near-instantly
#   from the LOCAL metadata DB — the user must NEVER see Finder's loading
#   spinner, regardless of network state. A cached/known dir must return its
#   readdir in < $QA_SNAPPY_MS (200ms) and must NEVER require a network
#   round-trip when the metadata DB already has the entries.
#
# WHAT THIS SCRIPT DOES:
#   Pre-populates real directories of varying sizes (10, 100, 1000, 5000 files)
#   under a UNIQUE timestamped dest on the mount (via REAL Finder copies — NOT
#   cp/dd, per principle #1: synthetic ops false-green; Finder exercises the
#   real NFS LOOKUP/READDIR path users hit, with ._AppleDouble sidecars), then
#   measures directory-listing (readdir) latency:
#
#     * COLD  — first listing right after population (worst case for the DB)
#     * WARM  — immediate re-listing (must be served from the local metadata DB)
#     * OFFLINE — listing while the control plane is forced offline (proves the
#                 listing is served from the DB, NOT a backend round-trip)
#     * DRAINING — listing of an UNRELATED cached dir while a copy drains
#                 (network/spool activity must not stall cached listings)
#
#   It asserts EVERY local-DB-resident listing is < $QA_SNAPPY_MS and reports a
#   latency table. FAIL if any local-DB-resident dir exceeds the threshold, or
#   if any readdir fails (-1).
#
# CHAIN OF CUSTODY: the populated dirs are staged off-mount with an md5 manifest
#   and copied via real Finder; we qa_wait_drain then qa_verify_custody so the
#   snappiness numbers are taken against REAL at-rest data, not phantom entries.
#
# NON-DESTRUCTIVE: all sources are staged under $QA_STAGE (/tmp); the single
#   dest is $QA_DEST_ROOT/$(qa_unique_tag QA) (matches the QA_*_$$_* cleanup
#   guard). The EXIT trap restores online state and runs qa_cleanup. We NEVER
#   touch the user's real folders.
#
# Scripts ALWAYS exit 0; pass/fail is in the .summary + the final VERDICT line.
# ===========================================================================

set -uo pipefail

source "$(dirname "$0")/lib.sh"

QA_CAT="09-finder-snappiness"

# ---------------------------------------------------------------------------
# Tunables (override via env). Sizes here are FILE COUNTS per dir — the task's
# 10/100/1000/5000 snappiness fixture. QUICK mode shrinks the 5000-file dir so a
# smoke run is fast; the budget assertion is identical in either mode.
SNAP_SIZES="${SNAP_SIZES:-10 100 1000 5000}"
[ "${QUICK:-0}" = "1" ] && SNAP_SIZES="${SNAP_SIZES_QUICK:-10 100 1000}"
# Tiny files keep population fast; snappiness is about readdir/metadata, not bytes.
SNAP_FILE_BYTES="${SNAP_FILE_BYTES:-4096}"
# A separate small "control" dir we list while a large copy drains — proves the
# cached listing is unaffected by concurrent network/spool work.
SNAP_DRAIN_PROBE_FILES="${SNAP_DRAIN_PROBE_FILES:-100}"

# ---------------------------------------------------------------------------
# State for the EXIT trap (restore offline + cleanup).
SNAP_FORCED_OFFLINE=0
SNAP_DEST=""

snap_on_exit() {
    # Always restore ONLINE if we forced offline, so we never leave the mount
    # wedged in spool-only mode for the next category / the user.
    if [ "$SNAP_FORCED_OFFLINE" = "1" ]; then
        qa_offline off >/dev/null 2>&1 || true
        SNAP_FORCED_OFFLINE=0
    fi
    # qa_cleanup removes ONLY $MOUNT/JM_RELEASE_BATTERY/QA_*_$$_* and $QA_STAGE
    # (its internal guard refuses anything else). Belt-and-suspenders: also drop
    # our specific dest if it still exists and is inside the guarded root.
    if [ -n "$SNAP_DEST" ]; then
        case "$SNAP_DEST" in
            "$QA_DEST_ROOT"/QA_*_$$_*) rm -rf "$SNAP_DEST" 2>/dev/null || true ;;
        esac
    fi
    qa_cleanup 2>/dev/null || true
}
trap snap_on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# ---------------------------------------------------------------------------
# Latency table accumulation (bash 3.2: no assoc arrays — append to a string).
# Each row:  <phase>\t<label>\t<file_count>\t<ms>\t<verdict>
SNAP_TABLE=""
snap_row() {
    # snap_row PHASE LABEL COUNT MS VERDICT
    SNAP_TABLE="${SNAP_TABLE}$(printf '%s\t%s\t%s\t%s\t%s' "$1" "$2" "$3" "$4" "$5")
"
}

# snap_measure PHASE LABEL DIR COUNT
#   Time one readdir of DIR, record a table row, and assert the budget.
#   A listing that FAILS (ms == -1) is a hard FAIL (broken readdir, not slow).
#   A listing >= $QA_SNAPPY_MS is a hard FAIL naming the dir (spinner gate).
snap_measure() {
    local phase="$1" label="$2" dir="$3" count="$4"
    local ms verdict
    ms="$(qa_dir_listing_ms "$dir")"
    if [ "$ms" = "-1" ]; then
        verdict="ERR"
        snap_row "$phase" "$label" "$count" "$ms" "$verdict"
        qa_fail "snappiness READDIR FAILED [$phase/$label] dir=$dir (count=$count)"
        printf '[CASE] %-9s %-14s count=%-5s -> READDIR FAILED (FAIL): %s\n' "$phase" "$label" "$count" "$dir"
        return 1
    fi
    if [ "$ms" -lt "$QA_SNAPPY_MS" ]; then
        verdict="OK"
        snap_row "$phase" "$label" "$count" "$ms" "$verdict"
        qa_pass "snappiness OK [$phase/$label] ${ms}ms < ${QA_SNAPPY_MS}ms (count=$count) dir=$dir"
        printf '[CASE] %-9s %-14s count=%-5s -> %4sms  < %sms  PASS\n' "$phase" "$label" "$count" "$ms" "$QA_SNAPPY_MS"
        return 0
    fi
    verdict="SLOW"
    snap_row "$phase" "$label" "$count" "$ms" "$verdict"
    qa_fail "snappiness SLOW [$phase/$label] ${ms}ms >= ${QA_SNAPPY_MS}ms (spinner gate) dir=$dir (count=$count)"
    printf '[CASE] %-9s %-14s count=%-5s -> %4sms >= %sms  FAIL (spinner)\n' "$phase" "$label" "$count" "$ms" "$QA_SNAPPY_MS"
    return 1
}

# ---------------------------------------------------------------------------
# MAIN
qa_begin "$QA_CAT"

if ! qa_preflight; then
    qa_log "preflight failed — skipping $QA_CAT (battery should have aborted in run-all's precheck)"
    qa_end
    echo "VERDICT: FAIL $QA_CAT: preflight failed (control plane/mount/dest guard) — see [FAIL] lines above"
    exit 0
fi

SNAP_DEST="$QA_DEST_ROOT/$(qa_unique_tag QA)"
mkdir -p "$SNAP_DEST" 2>/dev/null
qa_info "snappiness dest: $SNAP_DEST"
qa_info "sizes (file counts): $SNAP_SIZES   file bytes: $SNAP_FILE_BYTES   budget: ${QA_SNAPPY_MS}ms"

mark=$(qa_log_mark)

# ---------------------------------------------------------------------------
# PHASE 0 — POPULATE each size dir via REAL Finder copy, then prove custody.
# We stage a flat dir of N files off-mount (qa_stage_tree with DIRS=1,
# FILES_PER_DIR=N, WITH_DOTUNDERSCORE=1 so the real ._AppleDouble path is
# exercised), Finder-duplicate the staged dir onto the mount, drain, verify.
# The landed leaf is "$SNAP_DEST/<staged-basename>/dir1" — a flat dir of N files
# whose listing we then time. We track these in three parallel newline lists
# (bash 3.2: no arrays of structs).
qa_sec "PHASE 0: populate size dirs via REAL Finder (10/100/1000/5000)"

SNAP_DIRS=""     # one "count<TAB>listing_dir" per line
for n in $SNAP_SIZES; do
    stage_root="$QA_STAGE/snap_n${n}_$(qa_unique_tag s)"
    # DIRS=1, FILES_PER_DIR=n, WITH_DOTUNDERSCORE=1 -> flat dir1/ of n files + ._ sidecars
    manifest="$(qa_stage_tree "$stage_root" 1 "$n" "$SNAP_FILE_BYTES" 1)"
    src_base="$(basename "$stage_root")"

    printf '[CASE] populate     count=%-5s -> Finder duplicate %s\n' "$n" "$src_base"
    out="$(qa_finder_copy "$stage_root" "$SNAP_DEST")"
    rc=$?
    if [ "$rc" -ne 0 ] || ! qa_finder_result_clean "$out"; then
        qa_fail "snappiness populate: Finder copy failed (count=$n) src=$stage_root dest=$SNAP_DEST result=$out"
        continue
    fi
    landed_leaf="$SNAP_DEST/$src_base/dir1"
    # Record the (count, listing_dir) pair AND the manifest for custody.
    SNAP_DIRS="${SNAP_DIRS}${n}	${landed_leaf}	${manifest}	${SNAP_DEST}/${src_base}
"
done

# Mandatory pre-verify gate (principle #2): wait for the spool to fully drain so
# custody reads the at-rest backend copy, and so the metadata DB is settled.
qa_sec "PHASE 0b: drain + custody (snappiness is measured against REAL data)"
if ! qa_wait_drain; then
    qa_fail "snappiness: spool did not drain before listing (dest=$SNAP_DEST)"
fi

# Custody per populated tree. We must iterate WITHOUT a pipe so that
# qa_verify_custody's qa_fail increments survive into the gate counters (a piped
# `while` runs in a subshell and would lose them). Persist the (count, leaf,
# manifest, destdir) rows to a temp TSV and read from it via redirection.
_custody_tmp="$QA_CAT_DIR/snap_dirs.tsv"
printf '%s' "$SNAP_DIRS" > "$_custody_tmp"
while IFS="$(printf '\t')" read -r n leaf manifest destdir; do
    [ -z "$n" ] && continue
    if qa_verify_custody "$manifest" "$destdir" >/dev/null 2>&1; then
        printf '[CASE] custody      count=%-5s -> OK\n' "$n"
    else
        qa_fail "snappiness custody (count=$n) leaf=$leaf"
        printf '[CASE] custody      count=%-5s -> MISSING/WRONG (FAIL)\n' "$n"
    fi
done < "$_custody_tmp"

# ---------------------------------------------------------------------------
# PHASE 1 — COLD listing (first readdir after population/drain) per size.
qa_sec "PHASE 1: COLD directory-listing latency per size"
while IFS="$(printf '\t')" read -r n leaf manifest destdir; do
    [ -z "$n" ] && continue
    snap_measure "cold" "flat_${n}" "$leaf" "$n"
done < "$_custody_tmp"

# ---------------------------------------------------------------------------
# PHASE 2 — WARM re-listing (must be served from the local metadata DB).
qa_sec "PHASE 2: WARM re-listing latency per size (local metadata DB)"
while IFS="$(printf '\t')" read -r n leaf manifest destdir; do
    [ -z "$n" ] && continue
    snap_measure "warm" "flat_${n}" "$leaf" "$n"
done < "$_custody_tmp"

# Capture the 5000-file (largest) leaf for the offline/draining probes — that's
# the stress dir for the spinner gate.
SNAP_BIG_LEAF=""
SNAP_BIG_N=""
while IFS="$(printf '\t')" read -r n leaf manifest destdir; do
    [ -z "$n" ] && continue
    SNAP_BIG_LEAF="$leaf"; SNAP_BIG_N="$n"
done < "$_custody_tmp"

# Top-level dest dir (holds one subdir per size) — another known-to-the-DB dir.
SNAP_TOP="$SNAP_DEST"

# ---------------------------------------------------------------------------
# PHASE 3 — OFFLINE listing: force the control plane offline and prove cached
# listings still meet the budget (NO network round-trip — served from the DB).
qa_sec "PHASE 3: listing latency WHILE OFFLINE (must be DB-served, no network)"
off_body="$(qa_offline on)"
if [ -n "$off_body" ]; then
    SNAP_FORCED_OFFLINE=1
    qa_info "forced offline: $off_body"
    # Largest dir + the top dest dir, while offline.
    if [ -n "$SNAP_BIG_LEAF" ]; then
        snap_measure "offline" "flat_${SNAP_BIG_N}" "$SNAP_BIG_LEAF" "$SNAP_BIG_N"
    fi
    snap_measure "offline" "dest_top" "$SNAP_TOP" "$(printf '%s' "$SNAP_SIZES" | wc -w | tr -d ' ')"
    # Back online for the drain probe + teardown.
    on_body="$(qa_offline off)"
    SNAP_FORCED_OFFLINE=0
    qa_info "restored online: $on_body"
else
    qa_warn "snappiness: could not toggle offline (control plane) — skipping OFFLINE phase"
fi

# ---------------------------------------------------------------------------
# PHASE 4 — DRAINING listing: kick off a fresh Finder copy of a probe dir so the
# spool is actively draining, then time the listing of an ALREADY-CACHED dir
# (the big leaf). Network/spool activity must NOT slow a cached readdir.
qa_sec "PHASE 4: listing latency WHILE a copy is DRAINING (cached dir unaffected)"
drain_stage="$QA_STAGE/snap_drainprobe_$(qa_unique_tag s)"
drain_manifest="$(qa_stage_tree "$drain_stage" 1 "$SNAP_DRAIN_PROBE_FILES" "$SNAP_FILE_BYTES" 0)"
drain_base="$(basename "$drain_stage")"

# Start the copy in the background so we list DURING active drain.
( qa_finder_copy "$drain_stage" "$SNAP_DEST" >/dev/null 2>&1 ) &
drain_copy_pid=$!
# Give Finder a beat to begin pushing to the spool (bounded, tiny — the only
# sleep here, via perl, per the no-GNU-sleep discipline).
perl -e 'select undef,undef,undef,0.5'

# Time the cached big-dir listing while the probe copy is in flight.
if [ -n "$SNAP_BIG_LEAF" ]; then
    pend="$(qa_spool_pending)"
    qa_info "during-drain probe: spool pending_files=$pend (copy pid=$drain_copy_pid)"
    snap_measure "draining" "flat_${SNAP_BIG_N}" "$SNAP_BIG_LEAF" "$SNAP_BIG_N"
fi

# Let the background copy finish and the spool settle so teardown is clean and
# custody of the probe is real.
wait "$drain_copy_pid" 2>/dev/null || true
if ! qa_wait_drain; then
    qa_warn "snappiness: drain-probe spool did not fully settle (dest=$SNAP_DEST)"
fi
qa_verify_custody "$drain_manifest" "$SNAP_DEST/$drain_base" >/dev/null 2>&1 \
    || qa_fail "snappiness drain-probe custody (dest=$SNAP_DEST/$drain_base)"

# ---------------------------------------------------------------------------
# Zero-random-Finder-error gate over the whole window (principle #3).
qa_sec "error-signature scan over the snappiness window"
scan="$(qa_error_scan "$mark" "$QA_CAT")"
qa_info "$scan"
if ! qa_error_scan "$mark" "$QA_CAT" >/dev/null 2>&1; then
    qa_fail "snappiness: Finder error signature in window ($scan) — see $QA_CAT_DIR/errscan-$QA_CAT.txt"
fi

# ---------------------------------------------------------------------------
# Latency table report.
qa_sec "LATENCY TABLE (budget ${QA_SNAPPY_MS}ms)"
printf 'PHASE      LABEL          COUNT   MS     VERDICT\n'
printf '%s' "$SNAP_TABLE" | while IFS="$(printf '\t')" read -r ph lb ct ms vd; do
    [ -z "$ph" ] && continue
    printf '%-10s %-14s %-7s %-6s %s\n' "$ph" "$lb" "$ct" "$ms" "$vd"
done
# Persist the table as an artifact.
{
    printf 'phase\tlabel\tcount\tms\tverdict\n'
    printf '%s' "$SNAP_TABLE"
} > "$QA_CAT_DIR/latency-table.tsv" 2>/dev/null

# ---------------------------------------------------------------------------
qa_end

# ---------------------------------------------------------------------------
# Final single-line VERDICT. Reconstruct the reason (with the offending dir) from
# the table on FAIL: first SLOW/ERR row names the path that broke the gate.
if [ "$QA_FAIL" -eq 0 ]; then
    echo "VERDICT: PASS $QA_CAT"
else
    bad="$(printf '%s' "$SNAP_TABLE" | awk -F'\t' '$5=="SLOW" || $5=="ERR" {print $1"/"$2" count="$3" "$4"ms ("$5")"; exit}')"
    if [ -n "$bad" ]; then
        echo "VERDICT: FAIL $QA_CAT: listing exceeded ${QA_SNAPPY_MS}ms budget or failed: $bad (dest=$SNAP_DEST) — see latency-table.tsv + [FAIL] lines"
    else
        echo "VERDICT: FAIL $QA_CAT: custody/drain/error-scan failure (dest=$SNAP_DEST) — see [FAIL] lines above"
    fi
fi

exit 0
