# Category09 original Finder operation custody. Source only; no main.
snap_intent() {
    local id="$1" src="$2" dest="$3" man="$4"
    printf '%s' "$src" > "$SNAP_TMP/$id.src" &&
        printf '%s' "$dest" > "$SNAP_TMP/$id.dest" &&
        printf '%s' "$man" > "$SNAP_TMP/$id.manifest" &&
        printf '%s\n' "intent $(date +%s) $id" > "$SNAP_TMP/$id.intent.tmp" &&
        mv "$SNAP_TMP/$id.intent.tmp" "$SNAP_TMP/$id.intent"
}

snap_bg_copy() {
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
        "$src" "$destp" > "$SNAP_TMP/$id.out.tmp" 2>&1
    rc=$?
    local replied_at
    replied_at="$(perl -MTime::HiRes=time -e 'printf "%.6f", time()')"
    # A reporting failure retains this SAME worker's original result in
    # memory. Retry only publication; never issue another AppleEvent.
    while ! mv "$SNAP_TMP/$id.out.tmp" "$SNAP_TMP/$id.out" 2>/dev/null; do sleep 1; done
    while ! { printf '%s\n' "$replied_at" > "$SNAP_TMP/$id.reply_at.tmp" &&
              mv "$SNAP_TMP/$id.reply_at.tmp" "$SNAP_TMP/$id.reply_at"; }; do sleep 1; done
    while ! { printf '%s' "$rc" > "$SNAP_TMP/$id.rc.tmp" &&
              mv "$SNAP_TMP/$id.rc.tmp" "$SNAP_TMP/$id.rc"; }; do sleep 1; done
}

snap_start_copy() {
    local id="$1" src="$2" dest="$3" man="$4" pid launched_at
    snap_intent "$id" "$src" "$dest" "$man" || return 1
    SNAP_PENDING=$((SNAP_PENDING+1))
    snap_bg_copy "$src" "$dest" "$id" "$SNAP_FINDER_TIMEOUT" &
    pid=$!
    SNAP_LAST_PID="$pid"
    launched_at="$(perl -MTime::HiRes=time -e 'printf "%.6f", time()')"
    SNAP_COPY_PIDS="$SNAP_COPY_PIDS $pid"
    while ! { printf '%s %s\n' "$pid" "$launched_at" > "$SNAP_TMP/$id.worker.tmp" &&
              mv "$SNAP_TMP/$id.worker.tmp" "$SNAP_TMP/$id.worker"; }; do
        snap_soft_deadline "$id original worker $pid custody publication failed"
        sleep 1
    done
    qa_log "  $id launched (pid=$pid): $src -> $dest"
}

snap_hold_unknown() {
    local why="$1"
    SNAP_HOLD=1
    printf '%s\n' "$why" > "$SNAP_TMP/HOLD.tmp"
    mv "$SNAP_TMP/HOLD.tmp" "$SNAP_TMP/HOLD"
    qa_fail "Finder completion unknown: $why; retaining original operations and data"
    # The acceptance controller retains this original category process. Do not
    # exit into run-all cleanup or send TERM as a substitute for cancellation.
    while :; do sleep 5; done
}

snap_reply_positive() {
    [ "$1" = 0 ] && [[ "$2" =~ [^[:space:]] ]] && qa_finder_result_clean "$2"
}

snap_soft_deadline() {
    local why="$1"
    [ "$SNAP_SOFT_RED" = 1 ] && return 0
    SNAP_SOFT_RED=1
    SNAP_SOFT_REASON="$why"
    SNAP_HOLD=1
    qa_fail "$why; acceptance RED, continuing original observation without redispatch"
    printf '%s\n' "$why" > "$SNAP_TMP/soft-deadline.tmp" &&
        mv "$SNAP_TMP/soft-deadline.tmp" "$SNAP_TMP/soft-deadline" || true
}

snap_flush_soft_reason() {
    [ "$SNAP_SOFT_RED" = 1 ] || return 0
    while ! { printf '%s\n' "$SNAP_SOFT_REASON" > "$SNAP_TMP/soft-deadline.tmp" &&
              mv "$SNAP_TMP/soft-deadline.tmp" "$SNAP_TMP/soft-deadline"; }; do
        # Retry publication only, with all original workers still retained.
        sleep 1
    done
}

snap_wait_original() {
    local pid="$1" rc
    while :; do
        [ "$SNAP_SIGNALLED" = 1 ] &&
            snap_soft_deadline "original wait interrupted by signal"
        SNAP_SIGNALLED=0
        wait "$pid"
        rc=$?
        if [ "$SNAP_SIGNALLED" = 1 ]; then
            snap_soft_deadline "original wait interrupted by signal"
            SNAP_SIGNALLED=0
            # A completed wait consumed the child's status; only a
            # signal-interrupted wait (>128) may rejoin the same PID.
            [ "$rc" -gt 128 ] && continue
        fi
        return "$rc"
    done
}

snap_publish_joined() {
    local id="$1" rc="$2"
    printf '%s %s\n' "$rc" "$(date +%s)" > "$SNAP_TMP/$id.joined.tmp" &&
        mv "$SNAP_TMP/$id.joined.tmp" "$SNAP_TMP/$id.joined"
}

snap_join_positive() {
    local id="$1" pid="$2" deadline="$3" rc out joined_rc
    while [ ! -f "$SNAP_TMP/$id.rc" ]; do
        [ "$(date +%s)" -ge "$deadline" ] &&
            snap_soft_deadline "$id exceeded observation deadline"
        perl -e 'select undef,undef,undef,0.25'
    done
    rc="$(cat "$SNAP_TMP/$id.rc")"
    out="$(cat "$SNAP_TMP/$id.out" 2>/dev/null || true)"
    snap_reply_positive "$rc" "$out" ||
        snap_hold_unknown "$id lacks clean nonempty original Finder reply (rc=$rc)"
    snap_wait_original "$pid"
    joined_rc=$?
    [ "$joined_rc" = 0 ] || snap_hold_unknown "$id original worker did not join cleanly"
    while ! snap_publish_joined "$id" "$joined_rc"; do
        snap_soft_deadline "$id joined-record publication failed"
        perl -e 'select undef,undef,undef,1'
    done
    SNAP_PENDING=$((SNAP_PENDING-1))
}

# A signal is an observation failure, never permission to abandon a child.
snap_signal() {
    SNAP_SIGNALLED=1
    QA_KEEP_DATA=1
    snap_soft_deadline "signal during category09 custody"
}

snap_admit_next() {
    [ "$SNAP_PENDING" = 0 ] && [ "$SNAP_HOLD" = 0 ] && [ "$SNAP_SOFT_RED" = 0 ]
}

snap_on_exit() {
    [ "$SNAP_PENDING" = 0 ] || return 0
    [ "$SNAP_HOLD" = 0 ] || return 0
    [ "$SNAP_FORCED_OFFLINE" = 0 ] || return 0
    [ "${QA_KEEP_DATA:-0}" != 1 ] || return 0
    qa_cleanup
}
