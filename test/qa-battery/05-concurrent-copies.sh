#!/usr/bin/env bash
# ===========================================================================
# 05-concurrent-copies.sh — JuiceMount RELEASE TESTING BATTERY
#
# CATEGORY: Real Finder copy storm with measured overlap.
#
# This is the "what a real editor actually does" test: several Finder copies
# land at once while another large file is being read back and while the user
# clicks around the volume in Finder. A single-threaded `cp` test (the trap a
# prior agent kept reverting to after compaction) NEVER exercises the FUSE/NFS
# concurrency that surfaces 100060 retry-storms, FromHandle STALE on shared
# handles, beachballs, and partial drains. So here we deliberately stack:
#
#   * 3-4 real Finder duplicates launched toward unique destinations. Launch
#     overlap alone does not prove overlapping Finder writes.
#   * 1 CONCURRENT readback (md5) of an already-landed large file, looped for
#     the whole duration of the copies — proves reads aren't starved/corrupted
#     while the spool is hot with writes.
#   * Mounted directory listing (qa_dir_listing_ms), sampled repeatedly during
#     the copies. This measures readdir, not Finder UI navigation.
#
# After the storm: ONE combined qa_wait_drain for the shared spool, then
# per-dest qa_verify_custody (MISSING==0 && WRONG==0 each), then a single
# qa_error_scan across the WHOLE concurrency window. Any 100060 (FUSE wedge
# under concurrency), any STALE/phantom, any custody miss/corruption, any
# stuck spool, or any listing >= budget = FAIL naming the offending path.
#
# DRIVER: REAL Finder via retained osascript workers. md5 checks mounted bytes.
# NON-DESTRUCTIVE: all dests are $QA_DEST_ROOT/QA_<epoch>_<pid>_* (cleaned by
# qa_cleanup's $$ guard); sources live off-mount under $QA_STAGE. With
# QA_KEEP_DATA=1, the original operation receipts and data remain for the
# acceptance controller. A timeout means unknown Finder completion, not cancel.
#
# Scripts ALWAYS exit 0; the VERDICT line + the .summary carry pass/fail.
# ===========================================================================

set -uo pipefail

source "$(dirname "$0")/lib.sh"

QA_CAT="05-concurrent-copies"

# ---- knobs (env-overridable so the orchestrator can scale up/down) ----------
# Number of Finder copies launched into one storm (scope: 3-4).
: "${CC_PARALLEL:=4}"
# Per-copy staged tree shape (kept modest so 4x concurrent stays bounded yet
# the copies overlap in time long enough to actually contend).
: "${CC_DIRS:=4}"
: "${CC_FILES_PER_DIR:=15}"
: "${CC_FILE_SIZE:=1048576}"          # 1 MiB data files
# Size of the already-landed file we read back concurrently (large enough that
# a single readback spans the copy window).
: "${CC_READBACK_SIZE:=268435456}"    # 256 MiB
# Soft observation deadline. Exceeding it retains the original Finder worker
# and read/nav observers in HOLD; it never cancels a Finder operation.
: "${CC_STORM_TIMEOUT:=600}"
# AppleEvent remains alive past the soft observation deadline so a late real
# reply can be joined on the SAME original worker. Its own terminal error is
# still an unknown Finder completion, never a synthetic cancellation.
CC_FINDER_TIMEOUT=$((CC_STORM_TIMEOUT + 600))

# ---------------------------------------------------------------------------
# Background-process bookkeeping + teardown. Pending Finder operations must
# retain all data and handles on every exit path.

CC_COPY_PIDS=""        # backgrounded, original osascript workers
CC_BG_PIDS=""          # readback + nav background loops
CC_TMP="$QA_STAGE/cc-ipc-$$"   # per-copy result/rc files written by bg copies
CC_PENDING=0
CC_HOLD=0
CC_SOFT_RED=0
CC_SOFT_REASON=""
CC_OBSERVERS=0
CC_SIGNALLED=0

_cc_teardown() {
    # Neither retention nor an unknown Finder operation permits a destructive
    # EXIT/INT/TERM cleanup. An osascript exit alone is not Finder completion.
    [ "${QA_KEEP_DATA:-0}" = 1 ] && return
    [ "$CC_PENDING" -gt 0 ] && return
    [ "$CC_OBSERVERS" -gt 0 ] && return
    [ "$CC_HOLD" = 1 ] && return
    # Every original child has already been joined. Saved numeric PIDs are
    # never authority to signal a process after they may have been reused.
    # Restore network state in case we failed between qa_offline on/off
    # elsewhere — this category never goes offline, but be defensive so a
    # crash here can't strand the mount offline for the next category.
    qa_offline off >/dev/null 2>&1 || true
    rm -rf "$CC_TMP" 2>/dev/null || true
    # qa_end runs qa_cleanup (removes our QA_*_$$_* dests + $QA_STAGE).
}
trap '_cc_teardown' EXIT
trap 'CC_SIGNALLED=1; { [ "$CC_PENDING" -gt 0 ] || [ "$CC_OBSERVERS" -gt 0 ] || [ "$CC_HOLD" = 1 ]; } && qa_log "CASE C: INT during original custody — retaining data" || exit 130' INT
trap 'CC_SIGNALLED=1; { [ "$CC_PENDING" -gt 0 ] || [ "$CC_OBSERVERS" -gt 0 ] || [ "$CC_HOLD" = 1 ]; } && qa_log "CASE C: TERM during original custody — retaining data" || exit 143' TERM

# ---------------------------------------------------------------------------
# Background workers.

# _cc_intent records the bound operation before launching a worker. The final
# marker is published last; incomplete records cannot become positive replies.
_cc_intent() {
    local id="$1" src="$2" dest="$3" man="$4"
    printf '%s' "$src" > "$CC_TMP/$id.src" &&
        printf '%s' "$dest" > "$CC_TMP/$id.dest" &&
        printf '%s' "$man" > "$CC_TMP/$id.manifest" &&
        printf '%s\n' "intent $(date +%s) $id" > "$CC_TMP/$id.intent.tmp" &&
        mv "$CC_TMP/$id.intent.tmp" "$CC_TMP/$id.intent"
}

# Direct osascript, with no Perl alarm or driver kill. Its own 600-second
# AppleScript timeout may return an error, which is still unknown Finder state.
_cc_bg_copy() {
    local src="$1" destp="$2" id="$3" bound="$4"
    local rc
    osascript \
        -e 'on run argv' \
        -e '  set s to POSIX file (item 1 of argv) as alias' \
        -e '  set d to POSIX file (item 2 of argv)' \
        -e '  with timeout of '"$bound"' seconds' \
        -e '    tell application "Finder" to set r to duplicate s to (d as alias)' \
        -e '  end timeout' \
        -e '  return (r as text)' \
        -e 'end run' \
        "$src" "$destp" > "$CC_TMP/$id.out.tmp" 2>&1
    rc=$?
    local replied_at
    replied_at="$(perl -MTime::HiRes=time -e 'printf "%.6f", time()')"
    # A reporting failure retains this SAME worker's original result in
    # memory. Retry only publication; never issue another AppleEvent.
    while ! mv "$CC_TMP/$id.out.tmp" "$CC_TMP/$id.out" 2>/dev/null; do sleep 1; done
    while ! { printf '%s\n' "$replied_at" > "$CC_TMP/$id.reply_at.tmp" &&
              mv "$CC_TMP/$id.reply_at.tmp" "$CC_TMP/$id.reply_at"; }; do sleep 1; done
    while ! { printf '%s' "$rc" > "$CC_TMP/$id.rc.tmp" &&
              mv "$CC_TMP/$id.rc.tmp" "$CC_TMP/$id.rc"; }; do sleep 1; done
}

_cc_start_copy() {
    local id="$1" src="$2" dest="$3" man="$4" pid launched_at
    _cc_intent "$id" "$src" "$dest" "$man" || return 1
    CC_PENDING=$((CC_PENDING+1))
    _cc_bg_copy "$src" "$dest" "$id" "$CC_FINDER_TIMEOUT" &
    pid=$!
    launched_at="$(perl -MTime::HiRes=time -e 'printf "%.6f", time()')"
    CC_COPY_PIDS="$CC_COPY_PIDS $pid"
    while ! { printf '%s %s\n' "$pid" "$launched_at" > "$CC_TMP/$id.worker.tmp" &&
              mv "$CC_TMP/$id.worker.tmp" "$CC_TMP/$id.worker"; }; do
        _cc_soft_deadline "$id original worker $pid custody publication failed"
        sleep 1
    done
    qa_log "  $id launched (pid=$pid): $src -> $dest"
}

_cc_hold_unknown() {
    local why="$1"
    CC_HOLD=1
    printf '%s\n' "$why" > "$CC_TMP/HOLD.tmp"
    mv "$CC_TMP/HOLD.tmp" "$CC_TMP/HOLD"
    qa_fail "Finder completion unknown: $why; retaining original operations and data"
    # The acceptance controller retains this original category process. Do not
    # exit into run-all cleanup or send TERM as a substitute for cancellation.
    while :; do sleep 5; done
}

_cc_reply_positive() {
    [ "$1" = 0 ] && [[ "$2" =~ [^[:space:]] ]] && qa_finder_result_clean "$2"
}

_cc_soft_deadline() {
    local why="$1"
    [ "$CC_SOFT_RED" = 1 ] && return 0
    CC_SOFT_RED=1
    CC_SOFT_REASON="$why"
    CC_HOLD=1
    qa_fail "$why; acceptance RED, continuing original observation without redispatch"
    printf '%s\n' "$why" > "$CC_TMP/soft-deadline.tmp" &&
        mv "$CC_TMP/soft-deadline.tmp" "$CC_TMP/soft-deadline" || true
}

_cc_flush_soft_reason() {
    [ "$CC_SOFT_RED" = 1 ] || return 0
    while ! { printf '%s\n' "$CC_SOFT_REASON" > "$CC_TMP/soft-deadline.tmp" &&
              mv "$CC_TMP/soft-deadline.tmp" "$CC_TMP/soft-deadline"; }; do
        # Retry publication only, with all original workers still retained.
        sleep 1
    done
}

_cc_wait_original() {
    local pid="$1" rc
    while :; do
        [ "$CC_SIGNALLED" = 1 ] &&
            _cc_soft_deadline "original wait interrupted by signal"
        CC_SIGNALLED=0
        wait "$pid"
        rc=$?
        if [ "$CC_SIGNALLED" = 1 ]; then
            _cc_soft_deadline "original wait interrupted by signal"
            CC_SIGNALLED=0
            # A completed wait consumed the child's status; only a
            # signal-interrupted wait (>128) may rejoin the same PID.
            [ "$rc" -gt 128 ] && continue
        fi
        return "$rc"
    done
}

_cc_publish_joined() {
    local id="$1" rc="$2"
    printf '%s %s\n' "$rc" "$(date +%s)" > "$CC_TMP/$id.joined.tmp" &&
        mv "$CC_TMP/$id.joined.tmp" "$CC_TMP/$id.joined"
}

# A successful, nonempty original reply followed by the original worker join
# is the sole resolution path. The deadline is a soft observation bound.
_cc_join_positive() {
    local id="$1" pid="$2" deadline="$3" rc out joined_rc
    while [ ! -f "$CC_TMP/$id.rc" ]; do
        [ "$(date +%s)" -ge "$deadline" ] &&
            _cc_soft_deadline "$id exceeded observation deadline"
        perl -e 'select undef,undef,undef,0.25'
    done
    rc="$(cat "$CC_TMP/$id.rc")"
    out="$(cat "$CC_TMP/$id.out" 2>/dev/null || true)"
    _cc_reply_positive "$rc" "$out" ||
        _cc_hold_unknown "$id lacks clean nonempty original Finder reply (rc=$rc)"
    _cc_wait_original "$pid"
    joined_rc=$?
    [ "$joined_rc" = 0 ] || _cc_hold_unknown "$id original worker did not join cleanly"
    while ! _cc_publish_joined "$id" "$joined_rc"; do
        _cc_soft_deadline "$id joined-record publication failed"
        perl -e 'select undef,undef,undef,1'
    done
    CC_PENDING=$((CC_PENDING-1))
}

_cc_join_observer() {
    local id="$1" pid="$2" deadline="$3" rc
    while kill -0 "$pid" 2>/dev/null; do
        [ "$(date +%s)" -ge "$deadline" ] &&
            _cc_soft_deadline "$id observer exceeded observation deadline"
        perl -e 'select undef,undef,undef,0.25'
    done
    _cc_wait_original "$pid"
    rc=$?
    while ! _cc_publish_joined "$id" "$rc"; do
        _cc_soft_deadline "$id observer joined-record publication failed"
        perl -e 'select undef,undef,undef,1'
    done
    CC_OBSERVERS=$((CC_OBSERVERS-1))
    [ "$rc" = 0 ] || _cc_hold_unknown "$id observer exited $rc"
}

_cc_seed_gate() {
    [ "$seed_ready" = 1 ] && return 0
    export QA_KEEP_DATA=1 # unproven seed/drain: preserve source and destination
    qa_log "CASE B: read seed was not verified; storm cannot make a concurrency claim"
    qa_end
    echo "VERDICT: FAIL $QA_CAT: read seed not verified"
    return 1
}

_cc_finish_category() {
    [ "$CC_DRAIN_UNPROVEN" = 1 ] && export QA_KEEP_DATA=1
    qa_end
}

# _cc_readback_loop FILE FLAGFILE — repeatedly md5 FILE until FLAGFILE vanishes.
# Records the LAST md5 seen + a count + a "changed" marker if the digest ever
# differs between reads (which would mean a concurrent read saw torn/changing
# bytes — a corruption signal). Writes results into $CC_TMP.
_cc_readback_loop() {
    local file="$1" flag="$2"
    local first="" cur n=0 changed=0 op_start op_end
    while [ -e "$flag" ]; do
        op_start="$(perl -MTime::HiRes=time -e 'printf "%.6f", time()')"
        cur="$(md5 -q "$file" 2>/dev/null)"
        op_end="$(perl -MTime::HiRes=time -e 'printf "%.6f", time()')"
        if [ -z "$cur" ]; then
            # A failed readback during the storm is itself a finding.
            printf '%s' "READFAIL" > "$CC_TMP/readback.err" || return 91
        else
            [ -z "$first" ] && first="$cur"
            [ "$cur" != "$first" ] && changed=1
            if [ "$n" -lt 3000 ]; then
                printf '%s %s %s\n' "$op_start" "$op_end" "$cur" >> "$CC_TMP/readback.samples" ||
                    return 91
            fi
        fi
        n=$((n+1))
        perl -e 'select undef,undef,undef,0.2'
    done
    printf '%s' "$first"   > "$CC_TMP/readback.md5" &&
        printf '%s' "$n"       > "$CC_TMP/readback.count" &&
        printf '%s' "$changed" > "$CC_TMP/readback.changed"
}

# _cc_nav_loop FLAGFILE PATHS... — repeatedly time-list each PATH (over the
# local metadata DB) until FLAGFILE vanishes. Records the WORST listing ms and
# the dir that produced it, plus a count and any -1 (failed readdir / beachball).
_cc_nav_loop() {
    local flag="$1"; shift
    local worst=0 worstdir="" faildir="" ms n=0 op_start op_end
    while [ -e "$flag" ]; do
        local p
        for p in "$@"; do
            [ -d "$p" ] || continue
            op_start="$(perl -MTime::HiRes=time -e 'printf "%.6f", time()')"
            ms="$(qa_dir_listing_ms "$p")"
            op_end="$(perl -MTime::HiRes=time -e 'printf "%.6f", time()')"
            n=$((n+1))
            if [ "$n" -lt 3000 ]; then
                printf '%s %s %s %s\n' "$op_start" "$op_end" "$p" "$ms" >> "$CC_TMP/nav.samples" ||
                    return 91
            fi
            if [ "$ms" = "-1" ]; then
                faildir="$p"
            elif [ "$ms" -gt "$worst" ]; then
                worst="$ms"; worstdir="$p"
            fi
        done
        perl -e 'select undef,undef,undef,0.25'
    done
    printf '%s' "$worst"    > "$CC_TMP/nav.worst_ms" &&
        printf '%s' "$worstdir" > "$CC_TMP/nav.worst_dir" &&
        printf '%s' "$faildir"  > "$CC_TMP/nav.fail_dir" &&
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
printf '%s' "$MOUNT" > "$CC_TMP/mount.path" ||
    _cc_hold_unknown "mount binding could not be retained"
printf '%s' "$CC_PARALLEL" > "$CC_TMP/parallel.count" ||
    _cc_hold_unknown "copy count could not be retained"
qa_log "  category05 custody=$CC_TMP"

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
if [ "$stage_ok" != 1 ]; then
    qa_end
    echo "VERDICT: FAIL $QA_CAT: source staging incomplete"
    exit 0
fi

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

_cc_start_copy seed "$rb_src" "$rb_dest" "$rb_md5" ||
    _cc_hold_unknown "seed intent or worker launch failed"
_cc_join_positive seed "$(cut -d' ' -f1 "$CC_TMP/seed.worker")" "$(( $(date +%s) + 600 ))"
if [ "$CC_SOFT_RED" = 1 ]; then
    _cc_flush_soft_reason
    CC_HOLD=0 # original seed worker positively replied and joined
    export QA_KEEP_DATA=1 # no source/dependency cleanup before external quiescence
    qa_end
    echo "VERDICT: FAIL $QA_CAT: read seed exceeded observation deadline"
    exit 0
fi
rb_out="$(cat "$CC_TMP/seed.out")"
rb_rc="$(cat "$CC_TMP/seed.rc")"
rb_landed="$rb_dest/$(basename "$rb_src")"
seed_ready=1

if [ "$rb_rc" -ne 0 ] || ! qa_finder_result_clean "$rb_out"; then
    qa_fail "CASE B: Finder copy of readback file failed/dirty -> $rb_landed : $rb_out"
    seed_ready=0
else
    qa_pass "CASE B: Finder copy of readback file returned clean"
fi

# Drain + mounted readback verify before we read it in a loop. These checks do
# not independently establish backend persistence; the adapter checks that.
if qa_wait_drain; then
    qa_pass "CASE B: spool drained for readback pre-land"
else
    qa_fail "CASE B: spool did not drain for readback pre-land ($rb_landed)"
    seed_ready=0
fi
if [ -f "$rb_landed" ]; then
    rb_got="$(md5 -q "$rb_landed" 2>/dev/null)"
    if [ "$rb_got" = "$rb_md5" ] && [ -n "$rb_got" ]; then
        qa_pass "CASE B: readback file landed byte-identical ($rb_landed)"
    else
        qa_fail "CASE B: readback file corrupt before storm (want=$rb_md5 got=$rb_got) $rb_landed"
        seed_ready=0
    fi
else
    qa_fail "CASE B: readback file missing before storm ($rb_landed)"
    seed_ready=0
fi

if ! _cc_seed_gate; then
    exit 0
fi

# ---------------------------------------------------------------------------
# CASE C — launch readback + mounted listing loops, then Finder copies. The
# category requires measured destination-specific write overlap below.
# ---------------------------------------------------------------------------
qa_sec "CASE C: $CC_PARALLEL Finder copies + sampled mounted readback/listing"

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
    CC_OBSERVERS=$((CC_OBSERVERS+1))
    _cc_readback_loop "$rb_landed" "$storm_flag" &
    rb_loop_pid=$!
    CC_BG_PIDS="$CC_BG_PIDS $rb_loop_pid"
    qa_log "  readback loop started (pid=$rb_loop_pid) on $rb_landed"
else
    rb_loop_pid=""
    qa_fail "CASE C: no landed readback file; no concurrent read coverage"
fi

# Start the concurrent NAV loop.
CC_OBSERVERS=$((CC_OBSERVERS+1))
_cc_nav_loop "$storm_flag" $nav_paths &
nav_loop_pid=$!
CC_BG_PIDS="$CC_BG_PIDS $nav_loop_pid"
qa_log "  nav loop started (pid=$nav_loop_pid) over: $nav_paths"

# Bound /spool snapshots to this category's exact destinations. The helper
# retains only positive writing-size observations, never treats omitted rows
# (the endpoint is capped) as proof of completion or serialization.
CC_OBSERVERS=$((CC_OBSERVERS+1))
/usr/bin/python3 "$(dirname "$0")/concurrent_evidence.py" capture "$CP_BASE/spool" "$CC_TMP" &
spool_pid=$!
CC_BG_PIDS="$CC_BG_PIDS $spool_pid"
qa_log "  bounded spool observer started (pid=$spool_pid)"

# Launch all copies promptly. This does not itself establish Finder overlap.
storm_t0=$(date +%s)
i=1
launched=0
while [ "$i" -le "$CC_PARALLEL" ]; do
    if [ "$CC_SIGNALLED" = 1 ] || [ "$CC_SOFT_RED" = 1 ]; then
        _cc_soft_deadline "signal or evidence failure interrupted Finder dispatch"
        break
    fi
    src_root="$QA_STAGE/tree$i"
    dest="$(cat "$CC_TMP/dest${i}.path")"
    _cc_start_copy "copy$i" "$src_root" "$dest" "$(cat "$CC_TMP/man${i}.path")" ||
        _cc_hold_unknown "copy$i intent or worker launch failed"
    launched=$((launched+1))
    i=$((i+1))
done

# Join only after each original operation has a positive, nonempty reply.
# The storm limit bounds observation, not Finder execution or cleanup.
storm_join_ok=1
join_deadline=$(( storm_t0 + CC_STORM_TIMEOUT ))
i=1
while [ "$i" -le "$launched" ]; do
    _cc_join_positive "copy$i" "$(cut -d' ' -f1 "$CC_TMP/copy${i}.worker")" "$join_deadline"
    i=$((i+1))
done
storm_elapsed=$(( $(date +%s) - storm_t0 ))

# Stop the background loops by clearing the flag, then let them flush results.
rm -f "$storm_flag" 2>/dev/null || _cc_hold_unknown "storm observer stop flag could not be removed"
[ -z "$rb_loop_pid" ] || _cc_join_observer readback "$rb_loop_pid" "$(( $(date +%s) + 45 ))"
_cc_join_observer nav "$nav_loop_pid" "$(( $(date +%s) + 45 ))"
_cc_join_observer spool "$spool_pid" "$(( $(date +%s) + 45 ))"

if [ "$launched" -ne "$CC_PARALLEL" ]; then
    _cc_flush_soft_reason
    CC_HOLD=0 # all launched operations and observers positively joined
    export QA_KEEP_DATA=1
    qa_end
    echo "VERDICT: FAIL $QA_CAT: signal aborted remaining Finder dispatch"
    exit 0
fi

if /usr/bin/python3 "$(dirname "$0")/concurrent_evidence.py" verify "$CC_TMP" > "$CC_TMP/overlap.verdict.tmp" 2>&1; then
    mv "$CC_TMP/overlap.verdict.tmp" "$CC_TMP/overlap.verdict" ||
        _cc_hold_unknown "overlap evidence publication failed"
    qa_pass "CASE C: measured interleaved writes in at least two destination families with mounted read/listing samples: $(cat "$CC_TMP/overlap.verdict")"
else
    mv "$CC_TMP/overlap.verdict.tmp" "$CC_TMP/overlap.verdict" ||
        _cc_hold_unknown "overlap refusal publication failed"
    qa_fail "CASE C: no verified two-family interleaved progress: $(cat "$CC_TMP/overlap.verdict")"
fi
CC_HOLD=0 # all original workers and observers joined with durable receipts

if [ "$CC_SOFT_RED" = 1 ]; then
    _cc_flush_soft_reason
    export QA_KEEP_DATA=1
    qa_fail "CASE C: original observation exceeded its soft deadline (${storm_elapsed}s); late reply retained, acceptance RED"
else
    qa_pass "CASE C: all $CC_PARALLEL original Finder workers replied cleanly and joined within storm budget (${storm_elapsed}s)"
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
    elif [ "$rb_loop_n" -le 0 ] 2>/dev/null; then
        qa_fail "CASE C: concurrent readback recorded no samples (n=$rb_loop_n)"
    elif [ -n "$rb_loop_md5" ] && [ "$rb_loop_md5" = "$rb_md5" ]; then
        qa_pass "CASE C: concurrent readback stable + correct over $rb_loop_n reads during storm"
    elif [ -n "$rb_loop_md5" ]; then
        qa_fail "CASE C: concurrent readback md5 wrong (want=$rb_md5 got=$rb_loop_md5) $rb_landed"
    else
        qa_fail "CASE C: concurrent readback produced no md5 (n=$rb_loop_n)"
    fi
else
    qa_fail "CASE C: concurrent readback never started"
fi

# ---- concurrent navigation / snappiness verdict ---------------------------
nav_worst="$(cat "$CC_TMP/nav.worst_ms" 2>/dev/null || echo 0)"
nav_worst_dir="$(cat "$CC_TMP/nav.worst_dir" 2>/dev/null || echo '')"
nav_fail_dir="$(cat "$CC_TMP/nav.fail_dir" 2>/dev/null || echo '')"
nav_n="$(cat "$CC_TMP/nav.count" 2>/dev/null || echo 0)"
if [ -n "$nav_fail_dir" ]; then
    qa_fail "CASE C: directory listing FAILED (readdir -1 / beachball) during storm: $nav_fail_dir"
elif [ "$nav_n" = "0" ]; then
    qa_fail "CASE C: nav loop recorded 0 listings (no measured coverage)"
elif [ "$nav_worst" -ge "$QA_SNAPPY_MS" ]; then
    qa_fail "CASE C: directory listing $nav_worst ms >= ${QA_SNAPPY_MS}ms budget DURING storm: $nav_worst_dir"
else
    qa_pass "CASE C: all $nav_n live listings snappy during storm (worst=${nav_worst}ms < ${QA_SNAPPY_MS}ms; $nav_worst_dir)"
fi

# ---------------------------------------------------------------------------
# CASE D — combined drain and mounted per-dest custody. Independent backend
# readback remains a separate acceptance requirement.
# ---------------------------------------------------------------------------
qa_sec "CASE D: combined drain + per-dest chain of custody"
CC_DRAIN_UNPROVEN=0

if qa_wait_drain; then
    qa_pass "CASE D: combined spool drained (pending_files==0 && in_progress==0)"
else
    qa_fail "CASE D: combined spool did NOT drain after concurrent storm (dests under $QA_DEST_ROOT)"
    CC_DRAIN_UNPROVEN=1
fi

# Surface any failed/quarantined rows explicitly — quarantine == SHA mismatch
# at-rest, which is a hard data-integrity failure.
sp_failed="$(qa_spool_actionable_failed)"
sp_quar="$(qa_spool_field quarantined)"
if [ "$sp_quar" != "0" ]; then
    qa_fail "CASE D: spool quarantined=$sp_quar (nonzero or unavailable)"
    CC_DRAIN_UNPROVEN=1
fi
if [ "$sp_failed" != "0" ]; then
    qa_fail "CASE D: spool actionable failed_files=$sp_failed (nonzero or unavailable)"
    CC_DRAIN_UNPROVEN=1
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
_cc_finish_category

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
