#!/usr/bin/env bash
# ===========================================================================
# 05-concurrent-copies.sh — JuiceMount RELEASE TESTING BATTERY
#
# CATEGORY: Genuine concurrency under real Finder load.
#
# This is the "what a real editor actually does" test: several Finder copies
# land at once while another large file is being read back and while the user
# clicks around the volume in Finder. A single-threaded `cp` test (the trap a
# prior agent kept reverting to after compaction) NEVER exercises the FUSE/NFS
# concurrency that surfaces 100060 retry-storms, FromHandle STALE on shared
# handles, beachballs, and partial drains. So here we deliberately stack:
#
#   * 3-4 SIMULTANEOUS real Finder duplicates, each into its OWN unique dest,
#     each from its OWN staged tree+manifest (backgrounded osascript).
#   * 1 CONCURRENT readback (md5) of an already-landed large file, looped for
#     the whole duration of the copies — proves reads aren't starved/corrupted
#     while the spool is hot with writes.
#   * CONCURRENT directory navigation (qa_dir_listing_ms over the local
#     metadata DB) sampled repeatedly during the copies — proves listings stay
#     snappy (< QA_SNAPPY_MS) and never beachball while writes are in flight.
#
# After the storm: ONE combined qa_wait_drain for the shared spool, then
# per-dest qa_verify_custody (MISSING==0 && WRONG==0 each), then a single
# qa_error_scan across the WHOLE concurrency window. Any 100060 (FUSE wedge
# under concurrency), any STALE/phantom, any custody miss/corruption, any
# stuck spool, or any listing >= budget = FAIL naming the offending path.
#
# DRIVER: REAL Finder via osascript (qa_finder_copy). md5 is verification only.
# NON-DESTRUCTIVE: all dests are $QA_DEST_ROOT/QA_<epoch>_<pid>_* (cleaned by
# qa_cleanup's $$ guard); sources live off-mount under $QA_STAGE.
# BOUNDED: every Finder op is bounded by qa_finder_copy's perl-alarm; no GNU
# timeout, no unbounded hang.
#
# Scripts ALWAYS exit 0; the VERDICT line + the .summary carry pass/fail.
# ===========================================================================

set -uo pipefail

source "$(dirname "$0")/lib.sh"

QA_CAT="05-concurrent-copies"

# ---- knobs (env-overridable so the orchestrator can scale up/down) ----------
# Number of simultaneous Finder copies (scope: 3-4).
: "${CC_PARALLEL:=4}"
# Per-copy staged tree shape (kept modest so 4x concurrent stays bounded yet
# the copies overlap in time long enough to actually contend).
: "${CC_DIRS:=4}"
: "${CC_FILES_PER_DIR:=15}"
: "${CC_FILE_SIZE:=1048576}"          # 1 MiB data files
# Size of the already-landed file we read back concurrently (large enough that
# a single readback spans the copy window).
: "${CC_READBACK_SIZE:=268435456}"    # 256 MiB
# Hard ceiling on how long we let the concurrent storm run before we stop the
# read/nav background loops (the copies themselves are bounded individually).
: "${CC_STORM_TIMEOUT:=600}"

# ---------------------------------------------------------------------------
# Background-process bookkeeping + teardown.
# We track every PID we spawn (copies, readback loop, nav loop) so the EXIT
# trap can guarantee nothing is left running, no offline state is left set,
# and our staged/dest data is removed even on an early failure/interrupt.

CC_COPY_PIDS=""        # backgrounded qa_finder_copy wrappers
CC_BG_PIDS=""          # readback + nav background loops
CC_TMP="$QA_STAGE/cc-ipc-$$"   # per-copy result/rc files written by bg copies

_cc_kill_pids() {
    local p
    for p in $*; do
        [ -n "$p" ] || continue
        kill -0 "$p" 2>/dev/null && kill -TERM "$p" 2>/dev/null
    done
}

_cc_teardown() {
    # Stop background loops first (they spin), then any lingering copies.
    _cc_kill_pids "$CC_BG_PIDS"
    _cc_kill_pids "$CC_COPY_PIDS"
    # Restore network state in case we failed between qa_offline on/off
    # elsewhere — this category never goes offline, but be defensive so a
    # crash here can't strand the mount offline for the next category.
    qa_offline off >/dev/null 2>&1 || true
    rm -rf "$CC_TMP" 2>/dev/null || true
    # qa_end runs qa_cleanup (removes our QA_*_$$_* dests + $QA_STAGE).
}
trap '_cc_teardown' EXIT INT TERM

# ---------------------------------------------------------------------------
# Background workers.

# _cc_bg_copy SRC DESTPARENT IDX — run ONE real Finder duplicate in the
# background, capturing its result string + rc into per-idx files so the
# foreground can attribute success/failure per copy after join.
_cc_bg_copy() {
    local src="$1" destp="$2" idx="$3"
    local out rc
    out="$(qa_finder_copy "$src" "$destp")"
    rc=$?
    printf '%s' "$out" > "$CC_TMP/copy${idx}.out"
    printf '%s' "$rc"  > "$CC_TMP/copy${idx}.rc"
}

# _cc_readback_loop FILE FLAGFILE — repeatedly md5 FILE until FLAGFILE vanishes.
# Records the LAST md5 seen + a count + a "changed" marker if the digest ever
# differs between reads (which would mean a concurrent read saw torn/changing
# bytes — a corruption signal). Writes results into $CC_TMP.
_cc_readback_loop() {
    local file="$1" flag="$2"
    local first="" cur n=0 changed=0
    while [ -e "$flag" ]; do
        cur="$(md5 -q "$file" 2>/dev/null)"
        if [ -z "$cur" ]; then
            # A failed readback during the storm is itself a finding.
            printf '%s' "READFAIL" > "$CC_TMP/readback.err"
        else
            [ -z "$first" ] && first="$cur"
            [ "$cur" != "$first" ] && changed=1
        fi
        n=$((n+1))
        perl -e 'select undef,undef,undef,0.2'
    done
    printf '%s' "$first"   > "$CC_TMP/readback.md5"
    printf '%s' "$n"       > "$CC_TMP/readback.count"
    printf '%s' "$changed" > "$CC_TMP/readback.changed"
}

# _cc_nav_loop FLAGFILE PATHS... — repeatedly time-list each PATH (over the
# local metadata DB) until FLAGFILE vanishes. Records the WORST listing ms and
# the dir that produced it, plus a count and any -1 (failed readdir / beachball).
_cc_nav_loop() {
    local flag="$1"; shift
    local worst=0 worstdir="" faildir="" ms n=0
    while [ -e "$flag" ]; do
        local p
        for p in "$@"; do
            [ -d "$p" ] || continue
            ms="$(qa_dir_listing_ms "$p")"
            n=$((n+1))
            if [ "$ms" = "-1" ]; then
                faildir="$p"
            elif [ "$ms" -gt "$worst" ]; then
                worst="$ms"; worstdir="$p"
            fi
        done
        perl -e 'select undef,undef,undef,0.25'
    done
    printf '%s' "$worst"    > "$CC_TMP/nav.worst_ms"
    printf '%s' "$worstdir" > "$CC_TMP/nav.worst_dir"
    printf '%s' "$faildir"  > "$CC_TMP/nav.fail_dir"
    printf '%s' "$n"        > "$CC_TMP/nav.count"
}

# ===========================================================================
# MAIN
# ===========================================================================

qa_begin "$QA_CAT"

if ! qa_preflight; then
    qa_log "preflight failed — skipping $QA_CAT"
    qa_end
    echo "VERDICT: FAIL $QA_CAT: preflight (control plane / mount / dest guard) — see log"
    exit 0
fi

mkdir -p "$CC_TMP" 2>/dev/null

# ---- baseline log mark for the WHOLE concurrency window --------------------
mark=$(qa_log_mark)

# ---------------------------------------------------------------------------
# CASE A — stage N independent source trees (off-mount), one dest each.
# ---------------------------------------------------------------------------
qa_sec "CASE A: stage $CC_PARALLEL independent trees + dests"

# Parallel arrays kept as newline lists / indexed files (bash 3.2: no arrays of
# substance needed; we drive by integer idx and well-known paths).
i=1
stage_ok=1
while [ "$i" -le "$CC_PARALLEL" ]; do
    src_root="$QA_STAGE/tree$i"
    # qa_stage_tree writes its manifest to $QA_CAT_DIR/<basename root>.manifest.
    man="$(qa_stage_tree "$src_root" "$CC_DIRS" "$CC_FILES_PER_DIR" "$CC_FILE_SIZE")"
    if [ ! -s "$man" ]; then
        qa_fail "CASE A: staging tree$i produced empty manifest ($man)"
        stage_ok=0
    fi
    # Record the manifest path + the source leaf name for later custody.
    printf '%s' "$man"               > "$CC_TMP/man${i}.path"
    printf '%s' "$(basename "$src_root")" > "$CC_TMP/man${i}.leaf"
    dest="$QA_DEST_ROOT/$(qa_unique_tag QA)"
    mkdir -p "$dest" 2>/dev/null
    printf '%s' "$dest" > "$CC_TMP/dest${i}.path"
    qa_log "  tree$i: src=$src_root dest=$dest manifest=$man"
    i=$((i+1))
done
qa_assert "$([ "$stage_ok" = 1 ] && echo 0 || echo 1)" \
    "CASE A: all $CC_PARALLEL source trees staged with manifests" \
    "CASE A: one or more source trees failed to stage"

# ---------------------------------------------------------------------------
# CASE B — land a large file FIRST, so we have something real to read back
# concurrently during the storm. This file is copied via REAL Finder (it must
# itself round-trip), drained, and custody-checked before the storm starts.
# ---------------------------------------------------------------------------
qa_sec "CASE B: pre-land a large file for concurrent readback"

rb_src="$QA_STAGE/readback_src.dat"
rb_md5="$(qa_stage_file "$rb_src" "$CC_READBACK_SIZE")"
rb_dest="$QA_DEST_ROOT/$(qa_unique_tag QA)"
mkdir -p "$rb_dest" 2>/dev/null

rb_out="$(qa_finder_copy "$rb_src" "$rb_dest")"
rb_rc=$?
rb_landed="$rb_dest/$(basename "$rb_src")"

if [ "$rb_rc" -ne 0 ] || ! qa_finder_result_clean "$rb_out"; then
    qa_fail "CASE B: Finder copy of readback file failed/dirty -> $rb_landed : $rb_out"
else
    qa_pass "CASE B: Finder copy of readback file returned clean"
fi

# Drain + verify the readback file is truly at-rest before we read it in a loop.
if qa_wait_drain; then
    qa_pass "CASE B: spool drained for readback pre-land"
else
    qa_fail "CASE B: spool did not drain for readback pre-land ($rb_landed)"
fi
if [ -f "$rb_landed" ]; then
    rb_got="$(md5 -q "$rb_landed" 2>/dev/null)"
    if [ "$rb_got" = "$rb_md5" ] && [ -n "$rb_got" ]; then
        qa_pass "CASE B: readback file landed byte-identical ($rb_landed)"
    else
        qa_fail "CASE B: readback file corrupt before storm (want=$rb_md5 got=$rb_got) $rb_landed"
    fi
else
    qa_fail "CASE B: readback file missing before storm ($rb_landed)"
fi

# ---------------------------------------------------------------------------
# CASE C — THE STORM: launch the concurrent readback + nav loops, then fire
# all N Finder copies SIMULTANEOUSLY, then join.
# ---------------------------------------------------------------------------
qa_sec "CASE C: $CC_PARALLEL simultaneous Finder copies + live readback + live nav"

# Flag file: the background loops run while it exists; we remove it to stop them.
storm_flag="$CC_TMP/storm.flag"
: > "$storm_flag"

# Collect a few paths to navigate during the storm: the dest root, each copy's
# dest, and one already-populated dir (the readback dest). These should all list
# from the local metadata DB regardless of write pressure.
nav_paths="$QA_DEST_ROOT $rb_dest"
i=1
while [ "$i" -le "$CC_PARALLEL" ]; do
    nav_paths="$nav_paths $(cat "$CC_TMP/dest${i}.path")"
    i=$((i+1))
done

# Start the concurrent READBACK loop (only if the file is present — otherwise
# the loop would just spin on a missing file; we still want nav running).
if [ -f "$rb_landed" ]; then
    _cc_readback_loop "$rb_landed" "$storm_flag" &
    rb_loop_pid=$!
    CC_BG_PIDS="$CC_BG_PIDS $rb_loop_pid"
    qa_log "  readback loop started (pid=$rb_loop_pid) on $rb_landed"
else
    rb_loop_pid=""
    qa_warn "CASE C: no landed readback file; skipping concurrent readback loop"
fi

# Start the concurrent NAV loop.
_cc_nav_loop "$storm_flag" $nav_paths &
nav_loop_pid=$!
CC_BG_PIDS="$CC_BG_PIDS $nav_loop_pid"
qa_log "  nav loop started (pid=$nav_loop_pid) over: $nav_paths"

# Fire ALL copies at once. Each backgrounds its own bounded qa_finder_copy.
storm_t0=$(date +%s)
i=1
while [ "$i" -le "$CC_PARALLEL" ]; do
    src_root="$QA_STAGE/tree$i"
    dest="$(cat "$CC_TMP/dest${i}.path")"
    _cc_bg_copy "$src_root" "$dest" "$i" &
    cpid=$!
    CC_COPY_PIDS="$CC_COPY_PIDS $cpid"
    qa_log "  copy$i launched (pid=$cpid): $src_root -> $dest"
    i=$((i+1))
done

# Join all copies. Each qa_finder_copy is itself bounded (perl alarm, 600s) so
# `wait` cannot hang indefinitely; CC_STORM_TIMEOUT is an additional belt — if
# the copies somehow outlive it (a true wedge), we stop the loops and FAIL.
storm_join_ok=1
join_deadline=$(( storm_t0 + CC_STORM_TIMEOUT ))
for cpid in $CC_COPY_PIDS; do
    # Bounded wait per pid using a poll loop (bash 3.2 `wait` has no timeout).
    while kill -0 "$cpid" 2>/dev/null; do
        if [ "$(date +%s)" -ge "$join_deadline" ]; then
            storm_join_ok=0
            break
        fi
        perl -e 'select undef,undef,undef,0.5'
    done
    wait "$cpid" 2>/dev/null || true
done
storm_elapsed=$(( $(date +%s) - storm_t0 ))

# Stop the background loops by clearing the flag, then let them flush results.
rm -f "$storm_flag" 2>/dev/null
for bpid in $CC_BG_PIDS; do
    # Bounded join for the loops too.
    n=0
    while kill -0 "$bpid" 2>/dev/null && [ "$n" -lt 20 ]; do
        perl -e 'select undef,undef,undef,0.25'
        n=$((n+1))
    done
    kill -0 "$bpid" 2>/dev/null && kill -TERM "$bpid" 2>/dev/null
    wait "$bpid" 2>/dev/null || true
done

if [ "$storm_join_ok" = 1 ]; then
    qa_pass "CASE C: all $CC_PARALLEL copies joined within storm budget (${storm_elapsed}s)"
else
    qa_fail "CASE C: storm exceeded CC_STORM_TIMEOUT=${CC_STORM_TIMEOUT}s (possible wedge/deadlock) — dests under $QA_DEST_ROOT"
fi

# ---- per-copy Finder result attribution -----------------------------------
i=1
copies_clean=1
while [ "$i" -le "$CC_PARALLEL" ]; do
    dest="$(cat "$CC_TMP/dest${i}.path")"
    out=""; rc="1"
    [ -f "$CC_TMP/copy${i}.out" ] && out="$(cat "$CC_TMP/copy${i}.out")"
    [ -f "$CC_TMP/copy${i}.rc" ]  && rc="$(cat "$CC_TMP/copy${i}.rc")"
    if [ "$rc" != "0" ]; then
        qa_fail "CASE C: copy$i Finder rc=$rc (dest=$dest) result=$out"
        copies_clean=0
    elif ! qa_finder_result_clean "$out"; then
        qa_fail "CASE C: copy$i Finder result dirty (dest=$dest): $out"
        copies_clean=0
    else
        qa_pass "CASE C: copy$i Finder result clean (dest=$dest)"
    fi
    i=$((i+1))
done
[ "$copies_clean" = 1 ] || qa_log "CASE C: at least one concurrent copy returned a Finder error"

# ---- concurrent readback verdict ------------------------------------------
if [ -n "$rb_loop_pid" ]; then
    rb_loop_md5="$(cat "$CC_TMP/readback.md5" 2>/dev/null || echo '')"
    rb_loop_n="$(cat "$CC_TMP/readback.count" 2>/dev/null || echo 0)"
    rb_loop_chg="$(cat "$CC_TMP/readback.changed" 2>/dev/null || echo 0)"
    if [ -f "$CC_TMP/readback.err" ]; then
        qa_fail "CASE C: concurrent readback FAILED to read $rb_landed during the storm (read starvation/EIO)"
    elif [ "$rb_loop_chg" = "1" ]; then
        qa_fail "CASE C: concurrent readback saw CHANGING bytes on $rb_landed (torn/corrupt read under write load)"
    elif [ -n "$rb_loop_md5" ] && [ "$rb_loop_md5" = "$rb_md5" ]; then
        qa_pass "CASE C: concurrent readback stable + correct over $rb_loop_n reads during storm"
    elif [ -n "$rb_loop_md5" ]; then
        qa_fail "CASE C: concurrent readback md5 wrong (want=$rb_md5 got=$rb_loop_md5) $rb_landed"
    else
        qa_warn "CASE C: concurrent readback produced no md5 (loop may not have run; n=$rb_loop_n)"
    fi
fi

# ---- concurrent navigation / snappiness verdict ---------------------------
nav_worst="$(cat "$CC_TMP/nav.worst_ms" 2>/dev/null || echo 0)"
nav_worst_dir="$(cat "$CC_TMP/nav.worst_dir" 2>/dev/null || echo '')"
nav_fail_dir="$(cat "$CC_TMP/nav.fail_dir" 2>/dev/null || echo '')"
nav_n="$(cat "$CC_TMP/nav.count" 2>/dev/null || echo 0)"
if [ -n "$nav_fail_dir" ]; then
    qa_fail "CASE C: directory listing FAILED (readdir -1 / beachball) during storm: $nav_fail_dir"
elif [ "$nav_n" = "0" ]; then
    qa_warn "CASE C: nav loop recorded 0 listings (storm too short to sample)"
elif [ "$nav_worst" -ge "$QA_SNAPPY_MS" ]; then
    qa_fail "CASE C: directory listing $nav_worst ms >= ${QA_SNAPPY_MS}ms budget DURING storm: $nav_worst_dir"
else
    qa_pass "CASE C: all $nav_n live listings snappy during storm (worst=${nav_worst}ms < ${QA_SNAPPY_MS}ms; $nav_worst_dir)"
fi

# ---------------------------------------------------------------------------
# CASE D — combined drain of the shared spool, then per-dest custody.
# (MANDATORY: drain BEFORE custody so we read the at-rest backend copy.)
# ---------------------------------------------------------------------------
qa_sec "CASE D: combined drain + per-dest chain of custody"

if qa_wait_drain; then
    qa_pass "CASE D: combined spool drained (pending_files==0 && in_progress==0)"
else
    qa_fail "CASE D: combined spool did NOT drain after concurrent storm (dests under $QA_DEST_ROOT)"
fi

# Surface any failed/quarantined rows explicitly — quarantine == SHA mismatch
# at-rest, which is a hard data-integrity failure.
sp_failed="$(qa_spool_actionable_failed)"
sp_quar="$(qa_spool_field quarantined)"
if [ "$sp_quar" != "0" ] && [ "$sp_quar" != "-1" ]; then
    qa_fail "CASE D: spool reports quarantined=$sp_quar (SHA mismatch at-rest under concurrency)"
fi
if [ "$sp_failed" != "0" ] && [ "$sp_failed" != "-1" ]; then
    qa_warn "CASE D: spool reports failed_files=$sp_failed (inspect drain rows)"
fi

# Per-dest custody. Each copy duplicated tree$i INTO dest, so the landed leaf
# is dest/tree$i/<relpath> (Finder duplicate preserves the source folder name).
i=1
all_custody=1
while [ "$i" -le "$CC_PARALLEL" ]; do
    man="$(cat "$CC_TMP/man${i}.path")"
    leaf="$(cat "$CC_TMP/man${i}.leaf")"
    dest="$(cat "$CC_TMP/dest${i}.path")"
    landed_dir="$dest/$leaf"
    if qa_verify_custody "$man" "$landed_dir"; then
        qa_pass "CASE D: copy$i full custody (MISSING==0 && WRONG==0) -> $landed_dir"
    else
        qa_fail "CASE D: copy$i custody FAILED -> $landed_dir"
        all_custody=0
    fi
    i=$((i+1))
done

# ---------------------------------------------------------------------------
# CASE E — single error scan across the ENTIRE concurrency window.
# This is where 100060 (FUSE-wedge retry-storm under concurrency), FromHandle
# STALE, and phantom purges would show up. Under concurrency these MUST be zero
# (no known-open edge applies here — this isn't the ._-heavy ratio case).
# ---------------------------------------------------------------------------
qa_sec "CASE E: error-signature scan over the whole storm window"

scan_line="$(qa_error_scan "$mark" "$QA_CAT")"
scan_rc=$?
qa_log "$scan_line"
errfile="$QA_CAT_DIR/errscan-$QA_CAT.txt"

if [ "$scan_rc" -eq 0 ]; then
    qa_pass "CASE E: zero Finder error signatures across concurrency window"
    fail_reason=""
else
    # Name the offending path(s) from the dumped errscan file so the FAIL is
    # actionable. 100060 under concurrency is the headline failure mode.
    off_path=""
    if [ -f "$errfile" ]; then
        off_path="$(grep -m1 -E '100060|FromHandle STALE|purging phantom|100070' "$errfile" 2>/dev/null)"
    fi
    qa_fail "CASE E: error signature(s) during concurrency [$scan_line] first=${off_path:-see $errfile}"
    fail_reason="error signature under concurrency: ${off_path:-see $errfile}"
fi

# ---------------------------------------------------------------------------
# CASE F — mount still healthy after the storm (no wedge/beachball left behind).
# ---------------------------------------------------------------------------
qa_sec "CASE F: post-storm health"

if qa_health >/dev/null 2>&1; then
    qa_pass "CASE F: control plane reports healthy after concurrent storm"
else
    qa_fail "CASE F: control plane NOT healthy after concurrent storm (possible wedge)"
fi

# ---------------------------------------------------------------------------
# Verdict + summary.
# ---------------------------------------------------------------------------
qa_end

if [ "${QA_FAIL:-0}" -eq 0 ]; then
    echo "VERDICT: PASS $QA_CAT"
else
    # Build a concise reason naming the most actionable offending detail.
    reason=""
    if [ "${storm_join_ok:-1}" != 1 ]; then
        reason="storm wedge/deadlock (>${CC_STORM_TIMEOUT}s) under $QA_DEST_ROOT"
    elif [ "${copies_clean:-1}" != 1 ]; then
        reason="a concurrent Finder copy returned an error under $QA_DEST_ROOT"
    elif [ "${all_custody:-1}" != 1 ]; then
        reason="custody loss/corruption under $QA_DEST_ROOT (see per-copy FAIL lines)"
    elif [ -n "${fail_reason:-}" ]; then
        reason="$fail_reason"
    else
        reason="see [FAIL] lines above (artifacts: ${QA_CAT_DIR:-$QA_ARTIFACTS})"
    fi
    echo "VERDICT: FAIL $QA_CAT: $reason"
fi

exit 0
