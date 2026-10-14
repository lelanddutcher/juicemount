# Strict release adapters reuse the existing original Finder worker custody.
# No main, no mounts, no transport implementation and no automatic cleanup.
# The external owner hook is a hash-bound executable, never an eval string.
qa_release_hook() {
    local actual
    case "$QA_OWNER_HOOK" in /*) ;; *) return 2 ;; esac
    [ -f "$QA_OWNER_HOOK" ] && [ ! -L "$QA_OWNER_HOOK" ] && [ -x "$QA_OWNER_HOOK" ] || return 2
    actual="$(shasum -a 256 "$QA_OWNER_HOOK")" || return 2
    [ "${actual%% *}" = "$QA_OWNER_HOOK_SHA256" ] || return 2
    "$QA_OWNER_HOOK" "$@"
}

qa_release_hold() {
    export QA_KEEP_DATA=1
    printf '[HOLD] %s; original operations and source data retained\n' "$*" >&2
    printf '%s\n' "$*" >> "$QA_ARTIFACTS/RELEASE-HOLD.txt"
    # With no registered original worker, stop this category and let the
    # outer fixture owner retain the mount/data. Never dispatch another case.
    [ "${SNAP_PENDING:-0}" != 0 ] || exit 1
    trap ':' INT TERM
    while :; do sleep 5; done
}

# A strict failure cannot continue into another mutating case or EXIT cleanup.
qa_fail() {
    printf '[FAIL] %s\n' "$*"
    QA_FAIL=$((QA_FAIL+1))
    qa_release_hold "$*"
}

qa_release_custody_init() {
    . "$QA_BATTERY_LIB_DIR/snappiness_custody.sh"
    SNAP_TMP="$(mktemp -d "$QA_CAT_DIR/original-finder.XXXXXXXX")" || qa_release_hold 'cannot create original Finder receipts'
    SNAP_PENDING=0; SNAP_HOLD=0; SNAP_SOFT_RED=0; SNAP_SIGNALLED=0
    SNAP_FORCED_OFFLINE=0; SNAP_COPY_PIDS=''; SNAP_SOFT_REASON=''
    SNAP_FINDER_TIMEOUT=86400
    trap snap_signal INT TERM
}

# Caller invokes directly in its owning shell, never through $(...).
qa_release_start_copy() {
    local id="$1" src="$2" dest="$3" manifest="$4"
    snap_admit_next || qa_release_hold 'previous Finder operation remains unsettled'
    snap_start_copy "$id" "$src" "$dest" "$manifest" || qa_release_hold 'Finder intent not retained'
}

qa_release_join_copy() {
    snap_join_positive "$1" "$2" "$(( $(date +%s) + ${3:-600} ))"
    snap_admit_next || qa_release_hold 'original Finder observation exceeded its bound'
}

# Synchronous callers (including $(...)) retain the worker in that same
# still-live shell. No alarm kills the AppleEvent driver on a soft deadline.
qa_release_operation_worker() {
    local operation="$1" src="$2" dest="$3" id="$4" action rc replied_at
    case "$operation" in
        copy) action='duplicate s to (d as alias)' ;;
        replacing) action='duplicate s to (d as alias) with replacing' ;;
        move) action='move s to (d as alias)' ;;
        delete) action='delete s' ;;
        *) return 2 ;;
    esac
    osascript -e 'on run argv' \
        -e 'set s to POSIX file (item 1 of argv) as alias' \
        -e 'set d to POSIX file (item 2 of argv)' \
        -e 'with timeout of 86400 seconds' \
        -e "tell application \"Finder\" to set r to $action" \
        -e 'end timeout' -e 'return (r as text)' -e 'end run' \
        "$src" "$dest" > "$SNAP_TMP/$id.out.tmp" 2>&1
    rc=$?
    replied_at="$(perl -MTime::HiRes=time -e 'printf "%.6f", time()')"
    # Publication alone may retry; the original AppleEvent never does.
    while ! mv "$SNAP_TMP/$id.out.tmp" "$SNAP_TMP/$id.out"; do sleep 1; done
    while ! { printf '%s\n' "$replied_at" > "$SNAP_TMP/$id.reply_at.tmp" &&
              mv "$SNAP_TMP/$id.reply_at.tmp" "$SNAP_TMP/$id.reply_at"; }; do sleep 1; done
    while ! { printf '%s\n' "$rc" > "$SNAP_TMP/$id.rc.tmp" &&
              mv "$SNAP_TMP/$id.rc.tmp" "$SNAP_TMP/$id.rc"; }; do sleep 1; done
}

qa_release_finder_operation() {
    local operation="$1" src="$2" dest="$3" bound="$4" id=operation pid deadline rc out joined_rc
    [ "${SNAP_PENDING:-0}" = 0 ] || qa_release_hold 'prior original Finder worker remains registered'
    qa_release_custody_init
    snap_intent "$id" "$src" "$dest" '' || qa_release_hold 'Finder intent not retained'
    printf '%s\n' "$operation" > "$SNAP_TMP/$id.kind" || qa_release_hold 'Finder kind not retained'
    SNAP_PENDING=1
    qa_release_operation_worker "$operation" "$src" "$dest" "$id" &
    pid=$!; SNAP_COPY_PIDS="$pid"
    while ! { printf '%s %s\n' "$pid" "$(date +%s)" > "$SNAP_TMP/$id.worker.tmp" &&
              mv "$SNAP_TMP/$id.worker.tmp" "$SNAP_TMP/$id.worker"; }; do sleep 1; done
    deadline=$(( $(date +%s) + bound ))
    while [ ! -f "$SNAP_TMP/$id.rc" ]; do
        [ "$(date +%s)" -lt "$deadline" ] || qa_release_hold 'original Finder reply remains unknown after observation deadline'
        sleep 0.25
    done
    rc="$(cat "$SNAP_TMP/$id.rc")"; out="$(cat "$SNAP_TMP/$id.out")"
    snap_wait_original "$pid"; joined_rc=$?
    while ! snap_publish_joined "$id" "$joined_rc"; do sleep 1; done
    [ "$joined_rc" = 0 ] || qa_release_hold 'original Finder worker did not join cleanly'
    # Even an observed error retains custody; a timeout reply does not prove
    # Finder stopped its out-of-process work. Never start a later mutation.
    snap_reply_positive "$rc" "$out" || qa_release_hold 'original Finder reply was not clean and positive'
    SNAP_PENDING=0
    snap_admit_next || qa_release_hold 'original Finder observation interrupted'
    printf '%s\n' "$out"
}

qa_finder_copy() { qa_release_finder_operation copy "$1" "$2" "${3:-600}"; }
qa_finder_copy_replacing() { qa_release_finder_operation replacing "$1" "$2" "${3:-600}"; }
qa_finder_move() { qa_release_finder_operation move "$1" "$2" "${3:-600}"; }
qa_finder_delete() { qa_release_finder_operation delete "$1" / "${2:-300}"; }

# Pure classification of ONE retained /spool snapshot: 0=typed all-zero,
# 1=typed outstanding work, 2=unknown/error. Cumulative .failed is not work.
qa_release_drain_state() {
    python3 -I -B - "$1" <<'PYDRAIN'
import json, sys
try:
    with open(sys.argv[1], "rb") as f:
        raw = f.read(2097153)
    if len(raw) > 2097152:
        raise ValueError("snapshot exceeds bound")
    def unique(pairs):
        d = {}
        for k, v in pairs:
            if k in d:
                raise ValueError("duplicate field")
            d[k] = v
        return d
    d = json.loads(raw, object_pairs_hook=unique)
    fields = "pending_files pending_symlinks pending_bytes in_progress capacity_used failed_files declined_files quarantined replacement_conflicts replacement_waiting stall_waiters stalled_files suspect_zero_tail".split()
    if not isinstance(d, dict) or d.get("enabled") is not True or d.get("offline") is not False or d.get("error", "") != "":
        raise ValueError("spool unavailable/offline/error")
    if any(type(d.get(k)) is not int or not 0 <= d[k] <= 9223372036854775807 for k in fields):
        raise ValueError("missing or invalid work counter")
    print(d["pending_bytes"], d["pending_files"] + d["in_progress"])
    status = 0 if all(d[k] == 0 for k in fields) else 1
except (OSError, ValueError, TypeError, RecursionError):
    status = 2
sys.exit(status)
PYDRAIN
}

qa_wait_drain() {
    local timeout="${1:-}" dir sample=0 started now rc values pb pf
    dir="$(mktemp -d "${QA_CAT_DIR:-$QA_ARTIFACTS}/drain.XXXXXXXX")" || qa_release_hold 'cannot retain drain observations'
    started="$(date +%s)"
    while :; do
        sample=$((sample+1))
        curl --fail --silent --show-error --max-time 5 --max-filesize 2097152 \
            "$CP_BASE/spool" > "$dir/$sample.json" 2> "$dir/$sample.stderr"
        rc=$?
        printf '%s\n' "$rc" > "$dir/$sample.curl-rc" || qa_release_hold 'cannot retain drain transport result'
        [ "$rc" = 0 ] || qa_release_hold 'drain snapshot transport failed; retained exact response'
        values="$(qa_release_drain_state "$dir/$sample.json")"; rc=$?
        printf '%s\n' "$rc" > "$dir/$sample.state" || qa_release_hold 'cannot retain drain classification'
        [ "$rc" != 2 ] || qa_release_hold 'drain snapshot has unknown, invalid or unavailable fields'
        [ "$rc" = 0 ] && { qa_info 'spool has typed zero work/error counters'; return 0; }
        [ "$rc" = 1 ] || qa_release_hold 'drain classifier failed'
        if [ -z "$timeout" ]; then
            read -r pb pf <<< "$values"
            timeout="$(qa_drain_ceiling "$pb" "$pf")"
        fi
        now="$(date +%s)"
        [ "$((now-started))" -lt "$timeout" ] || qa_release_hold 'drain observation deadline reached with outstanding work/error counters'
        sleep 2
    done
}

# Pure receipt predicate, shared with focused host-free controls.
qa_release_cancel_reply() {
    [ "$1" != 0 ] && printf '%s\n' "$2" | grep -E 'execution error:.*\(-128\)$' >/dev/null
}

# Real cancellation requires prior destination growth AND the exact original
# Finder cancellation reply (-128). A completed copy is not a cancellation.
qa_finder_cancel_copy() {
    local src="$1" dest="$2" grace="${3:-1500}" id=cancel pid leaf b0 b1 elapsed=0 rc out joined_rc
    qa_release_custody_init
    qa_release_start_copy "$id" "$src" "$dest" ''
    pid="$SNAP_LAST_PID"; leaf="$dest/$(basename "$src")"
    b0="$(_qa_dest_bytes "$leaf")"
    while [ ! -f "$SNAP_TMP/$id.rc" ]; do
        sleep 0.25; elapsed=$((elapsed+250)); b1="$(_qa_dest_bytes "$leaf")"
        if [ "$elapsed" -ge "$grace" ] && [ "$b1" -gt "$b0" ] 2>/dev/null; then break; fi
        b0="$b1"
        [ "$elapsed" -lt 15000 ] || qa_release_hold 'no proved active Finder byte growth before cancel'
    done
    if [ -f "$SNAP_TMP/$id.rc" ]; then
        qa_release_join_copy "$id" "$pid" 15
        return 1
    fi
    printf '%s\t%s\t%s\n' "$pid" "$b0" "$b1" > "$SNAP_TMP/growth-before-cancel.tsv" || qa_release_hold 'growth receipt failed'
    # The fixture owner must admit no unrelated Finder copy before this case.
    # The action is not sufficient proof: the original -128 reply below is mandatory.
    osascript -e 'with timeout of 10 seconds' \
        -e 'tell application "Finder" to activate' \
        -e 'tell application "System Events" to keystroke "." using {command down}' \
        -e 'end timeout' > "$SNAP_TMP/cancel-action.out" 2>&1 || qa_release_hold 'Finder cancel action failed or unknown'
    local deadline=$(( $(date +%s) + 30 ))
    while [ ! -f "$SNAP_TMP/$id.rc" ]; do
        [ "$(date +%s)" -lt "$deadline" ] || qa_release_hold 'original Finder cancel reply remains unknown'
        sleep 0.25
    done
    rc="$(cat "$SNAP_TMP/$id.rc")"; out="$(cat "$SNAP_TMP/$id.out")"
    snap_wait_original "$pid"; joined_rc=$?
    [ "$joined_rc" = 0 ] || qa_release_hold 'original cancellation worker did not join'
    while ! snap_publish_joined "$id" "$joined_rc"; do sleep 1; done
    SNAP_PENDING=$((SNAP_PENDING-1))
    qa_release_cancel_reply "$rc" "$out" || qa_release_hold 'original Finder reply did not prove user cancellation'
    [ "$SNAP_SIGNALLED" = 0 ] && [ "$SNAP_SOFT_RED" = 0 ] || qa_release_hold 'cancellation observation interrupted'
    printf '%s\n' "$pid"
}
