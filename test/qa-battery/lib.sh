#!/usr/bin/env bash
# lib.sh — shared helpers for the JuiceMount REAL-FINDER RELEASE TESTING BATTERY.
#
# This battery COMPLEMENTS scripts/qa-suite/ (which drives synthetic cp/dd/fio).
# Here, every write is driven by a REAL macOS Finder operation via osascript so
# the actual NFS path (LOOKUP/CREATE/SETATTR/WRITE/READ + ._AppleDouble sidecars
# + xattr forks) is exercised — the path users hit, the one synthetic cp false-
# greens. md5 is used ONLY to VERIFY integrity after the fact, never as the
# driver of a copy.
#
# Sourced by every category script (01-finder-copy.sh ... NN-*.sh). Provides:
#   - env defaults (MOUNT, control plane addr, log path)
#   - PASS/FAIL/assert reporting + counters
#   - qa_finder_copy / qa_finder_move / qa_finder_delete  (REAL Finder ops)
#   - qa_wait_drain / qa_spool_pending                    (spool chain gates)
#   - qa_verify_custody                                   (md5 chain of custody)
#   - qa_error_scan / qa_log_mark                         (zero-Finder-error gate)
#   - qa_dir_listing_ms                                   (snappiness gate)
#   - qa_offline                                          (offline-spool control)
#   - qa_stage_file / qa_stage_tree / qa_unique_tag       (non-destructive staging)
#   - qa_cleanup                                          (tear down only OUR dests)
#
# Hard rules baked in:
#   * bash 3.2-safe (macOS default /bin/bash). NO associative arrays, NO `mapfile`,
#     NO `${var^^}`, NO `declare -A`. Loops over newline-delimited streams.
#   * NO GNU `timeout`. Bounded probes use: perl -e 'alarm N; exec @ARGV'.
#   * NON-DESTRUCTIVE: every script copies into a UNIQUE timestamped dest
#     ($QA_DEST_ROOT/QA_<epoch>_<pid>/...) and cleans up ONLY that subtree.
#     We NEVER write to or delete the user's real folders.
#
# Control plane: http://127.0.0.1:11050  →  /health  /spool  /offline?on=1|0
# Spool drained  ⇔  GET /spool .pending_files == 0  AND .in_progress == 0.
# Log:  ~/Library/Logs/JuiceMount/juicemount.log
# ===========================================================================

# ---------------------------------------------------------------------------
# Env defaults (override by exporting before sourcing)

: "${MOUNT:=/Volumes/zpool}"
# If the literal default isn't an active NFS mount, fall back to autodetecting
# the first 127.0.0.1:/ NFS mount (matches the sibling suite's behavior so the
# battery runs against whatever mount JuiceMount actually brought up).
if ! mount | awk -v m="$MOUNT" '$0 ~ " "m" " && /nfs/ {f=1} END{exit !f}'; then
    _auto="$(mount | awk '$1 == "127.0.0.1:/" && $3 != "" && /nfs/ {print $3; exit}')"
    [ -n "$_auto" ] && MOUNT="$_auto"
fi
export MOUNT

export CP_ADDR="${CP_ADDR:-127.0.0.1:11050}"
export CP_BASE="http://${CP_ADDR}"
export JM_LOG="${JM_LOG:-$HOME/Library/Logs/JuiceMount/juicemount.log}"

# All battery-created data lives UNDER this root on the mount. A single shared
# root per run keeps cleanup trivial and keeps us provably away from real data.
export QA_DEST_ROOT="${QA_DEST_ROOT:-$MOUNT/JM_RELEASE_BATTERY}"

# Local staging area (source-of-truth bytes live here, off the mount).
export QA_STAGE="${QA_STAGE:-/tmp/jm-battery-stage-$$}"

# Artifacts (manifests, scan dumps, listings, summaries).
export QA_ARTIFACTS="${QA_ARTIFACTS:-/tmp/jm-battery-artifacts/$(date +%Y%m%d-%H%M%S)}"

mkdir -p "$QA_STAGE" "$QA_ARTIFACTS" 2>/dev/null

# Snappiness budget: a cached dir listing served from the local metadata DB
# must return in under this many ms (principle #4 — user must NEVER see the
# Finder spinner). 200ms hard ceiling.
: "${QA_SNAPPY_MS:=200}"

# Drain wait default ceiling (seconds).
: "${QA_DRAIN_TIMEOUT:=300}"

# ---------------------------------------------------------------------------
# Reporting + counters
#
# Each script sets QA_CAT (category label) then calls qa_begin / qa_end.
# Counters are global; qa_begin re-zeros them. Scripts ALWAYS exit 0 so a
# harness can run the whole battery; pass/fail is read from the .summary file
# and from the nonzero $QA_FAIL count echoed in qa_end.

: "${QA_PASS:=0}"
: "${QA_FAIL:=0}"
: "${QA_WARN:=0}"

_ts() { date +%H:%M:%S; }

qa_log()  { printf '[%s] %s\n' "$(_ts)" "$*"; }
qa_sec()  { printf '\n[%s] == %s ==\n' "$(_ts)" "$*"; }
# qa_pass MSG — record a passing assertion.
qa_pass() { printf '[PASS] %s\n' "$*"; QA_PASS=$((QA_PASS+1)); }
# qa_fail MSG — record a FAILING assertion. ALWAYS name the offending path in MSG.
qa_fail() { printf '[FAIL] %s\n' "$*"; QA_FAIL=$((QA_FAIL+1)); }
# qa_warn MSG — known-edge / informational; does not fail the gate by itself.
qa_warn() { printf '[WARN] %s\n' "$*"; QA_WARN=$((QA_WARN+1)); }
qa_info() { printf '[INFO] %s\n' "$*"; }

# qa_assert COND_RC PASS_MSG FAIL_MSG
#   COND_RC is the exit status of a test command ( `cmd; qa_assert $? ...` ).
#   0 → qa_pass PASS_MSG ; nonzero → qa_fail FAIL_MSG. Returns COND_RC.
qa_assert() {
    local rc="$1" pmsg="$2" fmsg="$3"
    if [ "$rc" -eq 0 ]; then qa_pass "$pmsg"; else qa_fail "$fmsg"; fi
    return "$rc"
}

# qa_begin CATEGORY_LABEL — start a category; zero counters; record start time.
qa_begin() {
    QA_CAT="${1:?qa_begin needs a category label}"
    QA_PASS=0; QA_FAIL=0; QA_WARN=0
    QA_T0=$(date +%s)
    QA_CAT_DIR="$QA_ARTIFACTS/$QA_CAT"
    mkdir -p "$QA_CAT_DIR" 2>/dev/null
    qa_log "==> BATTERY category $QA_CAT starting (artifacts: $QA_CAT_DIR)"
}

# qa_end — write .summary, print totals, run qa_cleanup. Returns QA_FAIL count.
qa_end() {
    local elapsed=$(( $(date +%s) - ${QA_T0:-$(date +%s)} ))
    {
        echo "category=$QA_CAT"
        echo "pass=$QA_PASS"
        echo "fail=$QA_FAIL"
        echo "warn=$QA_WARN"
        echo "elapsed_sec=$elapsed"
        echo "finished_at=$(date -Iseconds)"
    } > "$QA_CAT_DIR/.summary" 2>/dev/null
    qa_log "==> $QA_CAT done: pass=$QA_PASS fail=$QA_FAIL warn=$QA_WARN in ${elapsed}s"
    qa_cleanup
    return "$QA_FAIL"
}

# ---------------------------------------------------------------------------
# Bounded execution (NO GNU timeout on macOS)

# qa_timeout SECONDS CMD [ARGS...] — run CMD bounded by SECONDS using perl's
# alarm. Returns CMD's exit status; 142 (128+SIGALRM) on timeout.
qa_timeout() {
    local secs="$1"; shift
    perl -e 'alarm shift; exec @ARGV' "$secs" "$@"
}

# ---------------------------------------------------------------------------
# Staging (non-destructive; bytes live OFF the mount)

# qa_unique_tag [PREFIX] — echo a collision-proof tag: <PREFIX>_<epoch>_<pid>_<rand>.
# Use as the leaf of a dest dir so back-to-back runs never collide.
qa_unique_tag() {
    local prefix="${1:-QA}"
    printf '%s_%s_%s_%s' "$prefix" "$(date +%s)" "$$" "$RANDOM"
}

# qa_stage_file PATH SIZE_BYTES [XATTR_SPEC]
#   Create a random file of SIZE_BYTES at PATH (under $QA_STAGE — caller passes a
#   path beneath it). XATTR_SPEC is an optional ';'-separated list of
#   NAME=VALUE xattrs written with `xattr -w` to exercise resource forks /
#   ._AppleDouble sidecars on the NFS side. Echoes the file's md5 on success.
#   Example: qa_stage_file "$QA_STAGE/clip.mov" 10485760 \
#              "com.apple.metadata:_kMDItemUserTags=hex; com.apple.quarantine=0083"
qa_stage_file() {
    local path="$1" size="$2" xspec="${3:-}"
    mkdir -p "$(dirname "$path")" 2>/dev/null
    # Reuse a 64MiB random pool to avoid hammering /dev/urandom per file.
    _qa_ensure_pool
    if [ "$size" -le "$QA_POOL_BYTES" ]; then
        dd if="$QA_POOL" of="$path" bs=1m count=$(( (size + 1048575) / 1048576 )) 2>/dev/null
        # Truncate to exact byte length.
        if command -v truncate >/dev/null 2>&1; then
            truncate -s "$size" "$path" 2>/dev/null
        else
            # macOS lacks coreutils truncate by default; use dd seek to set size.
            dd if=/dev/null of="$path" bs=1 seek="$size" count=0 2>/dev/null
        fi
    else
        dd if=/dev/urandom of="$path" bs=1m count=$(( (size + 1048575) / 1048576 )) 2>/dev/null
        # The MB-rounded dd above overshoots to a whole-MiB boundary (e.g. a
        # 1181116006-byte gb1 lands at 1181745152). Truncate to the EXACT byte
        # length, same as the <=64MiB branch, so size assertions are byte-exact.
        if command -v truncate >/dev/null 2>&1; then
            truncate -s "$size" "$path" 2>/dev/null
        else
            dd if=/dev/null of="$path" bs=1 seek="$size" count=0 2>/dev/null
        fi
    fi
    if [ -n "$xspec" ]; then
        # Split on ';' (bash 3.2-safe: swap IFS, iterate positional params).
        local oldifs="$IFS"; IFS=';'; set -- $xspec; IFS="$oldifs"
        local kv name val
        for kv in "$@"; do
            kv="$(printf '%s' "$kv" | sed 's/^ *//; s/ *$//')"
            [ -z "$kv" ] && continue
            name="${kv%%=*}"; val="${kv#*=}"
            xattr -w "$name" "$val" "$path" 2>/dev/null || true
        done
    fi
    md5 -q "$path" 2>/dev/null
}

QA_POOL="/tmp/jm-battery-pool-64MiB"
QA_POOL_BYTES=67108864
_qa_ensure_pool() {
    if [ ! -f "$QA_POOL" ] || [ "$(stat -f%z "$QA_POOL" 2>/dev/null || echo 0)" -lt "$QA_POOL_BYTES" ]; then
        dd if=/dev/urandom of="$QA_POOL" bs=1m count=64 2>/dev/null
    fi
}

# qa_stage_tree ROOT DIRS FILES_PER_DIR SIZE_BYTES [WITH_DOTUNDERSCORE]
#   Build a staged directory tree under ROOT: DIRS subdirectories each holding
#   FILES_PER_DIR files of SIZE_BYTES. If WITH_DOTUNDERSCORE=1, also drop a
#   `._sidecar` companion next to each file (drives the ._-heavy FromHandle
#   STALE edge — principle #3 known edge: ~1 STALE per ~960 ._ files).
#   Writes a MANIFEST to $QA_CAT_DIR/<basename ROOT>.manifest (TSV: relpath<TAB>md5)
#   and echoes the manifest path.
qa_stage_tree() {
    local root="$1" dirs="$2" fpd="$3" size="$4" du="${5:-0}"
    local manifest="$QA_CAT_DIR/$(basename "$root").manifest"
    : > "$manifest"
    local d f path md rel
    d=1
    while [ "$d" -le "$dirs" ]; do
        f=1
        while [ "$f" -le "$fpd" ]; do
            rel="dir$d/file$f.dat"
            path="$root/$rel"
            md="$(qa_stage_file "$path" "$size")"
            printf '%s\t%s\n' "$rel" "$md" >> "$manifest"
            if [ "$du" = "1" ]; then
                # ._companion: tiny AppleDouble-style sidecar. Not in manifest
                # (it's metadata, not user data) but present on disk to exercise
                # the NFS ._ path during the Finder copy.
                printf '\x00\x05\x16\x07AppleDouble' > "$root/dir$d/._file$f.dat" 2>/dev/null
            fi
            f=$((f+1))
        done
        d=$((d+1))
    done
    echo "$manifest"
}

# ---------------------------------------------------------------------------
# REAL Finder operations (the whole point — osascript, not cp)

# qa_finder_copy SRC DEST_PARENT
#   Drive a REAL Finder copy: `tell application "Finder" to duplicate <SRC> to <DEST_PARENT>`.
#   SRC may be a file or a folder (Finder recurses folders natively, creating
#   ._AppleDouble sidecars + xattr forks exactly as a user drag does).
#   DEST_PARENT must already exist on the mount. Echoes the Finder result string
#   (the AppleScript reference to the duplicated item, e.g.
#   "document file clip.mov of folder ... of disk ...") and returns:
#     0  on success (result captured)
#     N  nonzero on AppleScript error (the osascript error text is echoed to stderr
#        AND to stdout so the caller can grep it for "-48", "already an item", etc.)
#   The whole op is bounded (default 600s) so a wedged copy can't hang the battery.
qa_finder_copy() {
    local src="$1" destp="$2" bound="${3:-600}"
    local sp dp out rc
    sp="$(_qa_posix2hfs "$src")"
    dp="$(_qa_posix2hfs "$destp")"
    # `with replacing` mirrors a user choosing "Replace" on a name clash; we
    # WANT to surface -48 'already an item' as a real result in the dedicated
    # back-to-back-copy case, so callers that test that edge pass bound only and
    # rely on the default (no replacing). Here default behavior = no replacing.
    out="$(qa_timeout "$bound" osascript \
        -e 'on run argv' \
        -e '  set s to POSIX file (item 1 of argv) as alias' \
        -e '  set d to POSIX file (item 2 of argv)' \
        -e '  with timeout of '"$bound"' seconds' \
        -e '    tell application "Finder" to set r to duplicate s to (d as alias)' \
        -e '  end timeout' \
        -e '  return (r as text)' \
        -e 'end run' \
        "$src" "$destp" 2>&1)"
    rc=$?
    printf '%s\n' "$out"
    return $rc
}

# qa_finder_copy_replacing SRC DEST_PARENT — same as qa_finder_copy but passes
# `with replacing` (user clicked "Replace"). Used to confirm replace semantics
# don't corrupt or strand handles.
qa_finder_copy_replacing() {
    local src="$1" destp="$2" bound="${3:-600}"
    local out rc
    out="$(qa_timeout "$bound" osascript \
        -e 'on run argv' \
        -e '  set s to POSIX file (item 1 of argv) as alias' \
        -e '  set d to POSIX file (item 2 of argv)' \
        -e '  with timeout of '"$bound"' seconds' \
        -e '    tell application "Finder" to set r to duplicate s to (d as alias) with replacing' \
        -e '  end timeout' \
        -e '  return (r as text)' \
        -e 'end run' \
        "$src" "$destp" 2>&1)"
    rc=$?
    printf '%s\n' "$out"
    return $rc
}

# qa_finder_move SRC DEST_PARENT — REAL Finder move (drag without Option).
# Echoes result string, returns AppleScript rc.
qa_finder_move() {
    local src="$1" destp="$2" bound="${3:-600}"
    local out rc
    out="$(qa_timeout "$bound" osascript \
        -e 'on run argv' \
        -e '  set s to POSIX file (item 1 of argv) as alias' \
        -e '  set d to POSIX file (item 2 of argv)' \
        -e '  with timeout of '"$bound"' seconds' \
        -e '    tell application "Finder" to set r to move s to (d as alias)' \
        -e '  end timeout' \
        -e '  return (r as text)' \
        -e 'end run' \
        "$src" "$destp" 2>&1)"
    rc=$?
    printf '%s\n' "$out"
    return $rc
}

# qa_finder_delete TARGET — REAL Finder "Move to Trash" (delete) of a file/folder
# on the mount. Echoes result string, returns rc. Used by the delete-then-recopy
# (-48 directory-delete-lag) case.
qa_finder_delete() {
    local tgt="$1" bound="${2:-300}"
    local out rc
    out="$(qa_timeout "$bound" osascript \
        -e 'on run argv' \
        -e '  set t to POSIX file (item 1 of argv) as alias' \
        -e '  with timeout of '"$bound"' seconds' \
        -e '    tell application "Finder" to set r to delete t' \
        -e '  end timeout' \
        -e '  return (r as text)' \
        -e 'end run' \
        "$tgt" 2>&1)"
    rc=$?
    printf '%s\n' "$out"
    return $rc
}

# _qa_dest_bytes DIR — echo the current on-disk byte size of DIR's subtree (0 if
# absent). Used to detect an IN-FLIGHT copy by watching partial bytes grow.
_qa_dest_bytes() {
    local d="$1"
    [ -e "$d" ] || { echo 0; return; }
    qa_timeout 20 du -sk "$d" 2>/dev/null | awk '{print $1*1024; f=1} END{if(!f) print 0}'
}

# qa_finder_cancel_copy SRC DEST_PARENT GRACE_MS
#   REAL mid-flight cancel of a Finder copy (NOT a fake "kill the osascript
#   driver"). A `tell application "Finder" to duplicate` runs the copy
#   OUT-OF-PROCESS inside Finder via an AppleEvent; the osascript process merely
#   BLOCKS on the reply, so killing osascript does NOT stop the copy — Finder
#   keeps writing to completion. To cancel for real we must hit Finder's copy
#   progress sheet the way a user does: Cmd-. (and Escape) via System Events.
#
#   Sequence:
#     1. Launch the duplicate in the background (real NFS CREATE/WRITE path).
#     2. Poll the dest subtree until partial bytes are GROWING — proof the copy
#        is genuinely IN-FLIGHT (not yet finished, not still spinning up).
#     3. Bring Finder frontmost and send Cmd-. + Escape via System Events to
#        cancel the in-flight copy at its progress sheet.
#     4. Confirm the copy stopped (the osascript driver returns / dest stops
#        growing).
#   Echoes the bg osascript PID. RETURN CONTRACT (the caller MUST branch on it
#   and must NOT claim "cancel" on anything but 0):
#     0  REAL in-flight cancel issued (dest was observed growing, Cmd-. sent).
#     1  copy finished before we could catch it in-flight — payload too small;
#        caller should GROW and retry (do not count as a cancel).
#     2  DEGRADED: copy was in-flight but System Events could not drive the
#        cancel (no Accessibility grant / no progress UI). We abandoned the
#        osascript driver instead — this is an interrupted/abandoned copy, NOT a
#        user-style cancel; the caller must relabel its assertion accordingly.
qa_finder_cancel_copy() {
    local src="$1" destp="$2" grace_ms="${3:-1500}"
    osascript \
        -e 'on run argv' \
        -e '  set s to POSIX file (item 1 of argv) as alias' \
        -e '  set d to POSIX file (item 2 of argv)' \
        -e '  tell application "Finder" to duplicate s to (d as alias)' \
        -e 'end run' \
        "$src" "$destp" >/dev/null 2>&1 &
    local pid=$! leaf b0 b1 inflight=0 waited_ms=0 step_ms=250
    # Where the duplicated tree is landing (Finder names the leaf = basename SRC).
    leaf="$destp/$(basename "$src")"
    # Watch for genuine in-flight growth, bounded by ~ max(grace_ms, 8000)ms.
    local cap_ms="$grace_ms"; [ "$cap_ms" -lt 8000 ] && cap_ms=8000
    b0="$(_qa_dest_bytes "$leaf")"
    while [ "$waited_ms" -lt "$cap_ms" ]; do
        kill -0 "$pid" 2>/dev/null || break       # driver already returned
        perl -e 'select undef,undef,undef, shift' "$(awk "BEGIN{print $step_ms/1000}")"
        waited_ms=$((waited_ms+step_ms))
        b1="$(_qa_dest_bytes "$leaf")"
        if [ "$b1" -gt "$b0" ] 2>/dev/null && [ "$waited_ms" -ge "$grace_ms" ]; then
            inflight=1; break                       # bytes grew AND past the grace
        fi
        b0="$b1"
    done

    if ! kill -0 "$pid" 2>/dev/null; then
        printf '%s\n' "$pid"
        return 1   # finished before we could catch it mid-flight — grow payload
    fi
    if [ "$inflight" -ne 1 ]; then
        # Still running but we never saw growth (stuck spinning up, or du blocked).
        # Treat as too-fast/indeterminate so the caller grows rather than over-claims.
        kill -TERM "$pid" 2>/dev/null
        printf '%s\n' "$pid"
        return 1
    fi

    # --- REAL cancel: drive Finder's copy progress sheet via System Events. ---
    # Cmd-. is the universal "cancel current operation" in Finder copy sheets;
    # Escape is a belt-and-suspenders second nudge. Bounded so a missing
    # Accessibility grant can't hang the battery.
    local se_rc
    qa_timeout 10 osascript \
        -e 'tell application "Finder" to activate' \
        -e 'tell application "System Events"' \
        -e '  keystroke "." using {command down}' \
        -e '  key code 53' \
        -e 'end tell' >/dev/null 2>&1
    se_rc=$?

    # Give Finder a beat to tear down the copy, then confirm it stopped.
    perl -e 'select undef,undef,undef,1'
    local stopped=0 s
    for s in 1 2 3 4 5 6; do
        kill -0 "$pid" 2>/dev/null || { stopped=1; break; }
        perl -e 'select undef,undef,undef,0.5'
    done

    if [ "$se_rc" -eq 0 ] && [ "$stopped" -eq 1 ]; then
        printf '%s\n' "$pid"
        return 0   # REAL cancel: Cmd-. accepted and the copy stopped.
    fi

    # System Events failed (likely no Accessibility grant) OR the copy didn't
    # observably stop. Abandon the driver and report DEGRADED so the caller makes
    # an HONEST interrupted-copy claim instead of a cancel claim.
    kill -TERM "$pid" 2>/dev/null
    printf '%s\n' "$pid"
    return 2
}

# _qa_posix2hfs POSIXPATH — echo an HFS-colon path (legacy AppleScript form).
# Kept for diagnostics/logging; the copy fns above use `POSIX file` directly.
_qa_posix2hfs() {
    osascript -e 'on run a' -e 'return POSIX file (item 1 of a) as text' -e 'end run' "$1" 2>/dev/null
}

# ---------------------------------------------------------------------------
# Spool chain-of-custody gates

# qa_spool_pending — echo the current pending_files count from GET /spool.
# Echoes -1 if the control plane is unreachable / malformed (caller treats <0
# as an error, never as "drained").
qa_spool_pending() {
    local body
    body="$(qa_timeout 5 curl -s "$CP_BASE/spool" 2>/dev/null)"
    [ -z "$body" ] && { echo -1; return; }
    # Extract "pending_files":N without jq (bash 3.2-safe).
    printf '%s' "$body" \
        | tr ',{}' '\n\n\n' \
        | awk -F: '/"pending_files"/ {gsub(/[^0-9-]/,"",$2); print $2; found=1} END{if(!found) print -1}'
}

# qa_spool_field FIELD — echo any integer field from GET /spool (e.g. in_progress,
# failed_files, quarantined, succeeded). -1 on error/missing.
qa_spool_field() {
    local field="$1" body
    body="$(qa_timeout 5 curl -s "$CP_BASE/spool" 2>/dev/null)"
    [ -z "$body" ] && { echo -1; return; }
    printf '%s' "$body" \
        | tr ',{}' '\n\n\n' \
        | awk -F: -v f="\"$field\"" '$0 ~ f {gsub(/[^0-9-]/,"",$2); print $2; found=1} END{if(!found) print -1}'
}

# qa_spool_actionable_failed — current rows that genuinely require operator or
# retry action. Do NOT gate on `.failed`: that field is the process-lifetime
# cumulative DrainsFailed metric and remains non-zero after an old failure has
# been retried, cleared, or classified as a deliberate policy decline.
qa_spool_actionable_failed() {
    qa_spool_field failed_files
}

# qa_spool_pending_bytes — echo the total bytes still in flight (pending_bytes)
# from GET /spool, used to scale the drain timeout to the real
# payload. Echoes 0 when the field is absent or the control plane is unreachable
# (callers then fall back to the static $QA_DRAIN_TIMEOUT / $QA_DRAIN_MIN floor).
qa_spool_pending_bytes() {
    local pb
    pb="$(qa_spool_field pending_bytes)"
    case "$pb" in ''|*[!0-9-]*) pb=0 ;; esac
    [ "$pb" -lt 0 ] 2>/dev/null && pb=0
    printf '%s' "$pb"
}

# qa_drain_ceiling PAYLOAD_BYTES [PAYLOAD_FILES]
#   Echo a payload-SCALED drain ceiling (seconds) for a known in-flight payload.
#   A multi-GB Finder copy drains single-threaded at a conservatively-assumed
#   floor of ~5 MB/s to the backend (proven: 2.3GB drains in ~5min ≈ 7.8MB/s, so
#   5MB/s is a safe under-estimate that never cuts a real drain short). We derive
#   BOTH ceil(bytes / 5MiB) and ceil(files / 5 files/s), take the larger, then
#   clamp to a generous [min,max] band. The file floor matters for tiny-file
#   trees: each object has backend create/mtime/metadata costs even when its byte
#   count is negligible, and a 10,000-entry Finder corpus legitimately takes far
#   longer than the old bytes-only 600s minimum.
#     floor:  $QA_DRAIN_MIN  (default 600s)   — never wait LESS than this
#     cap:    $QA_DRAIN_MAX  (default 3600s)  — never wait MORE than this
: "${QA_DRAIN_FLOOR_BPS:=5242880}"   # 5 MiB/s assumed drain floor
: "${QA_DRAIN_FLOOR_FILES_PER_SEC:=5}"
: "${QA_DRAIN_MIN:=600}"
: "${QA_DRAIN_MAX:=3600}"
qa_drain_ceiling() {
    local bytes="${1:-0}" files="${2:-0}" byte_secs file_secs secs
    case "$bytes" in ''|*[!0-9]*) bytes=0 ;; esac
    case "$files" in ''|*[!0-9]*) files=0 ;; esac
    byte_secs=$(( (bytes + QA_DRAIN_FLOOR_BPS - 1) / QA_DRAIN_FLOOR_BPS ))
    file_secs=$(( (files + QA_DRAIN_FLOOR_FILES_PER_SEC - 1) / QA_DRAIN_FLOOR_FILES_PER_SEC ))
    secs="$byte_secs"
    [ "$file_secs" -gt "$secs" ] && secs="$file_secs"
    [ "$secs" -lt "$QA_DRAIN_MIN" ] && secs="$QA_DRAIN_MIN"
    [ "$secs" -gt "$QA_DRAIN_MAX" ] && secs="$QA_DRAIN_MAX"
    printf '%s' "$secs"
}

# qa_wait_drain [TIMEOUT_S]
#   Block until the spool is FULLY drained — pending_files==0 AND in_progress==0 —
#   or TIMEOUT_S elapses. This is the MANDATORY gate before qa_verify_custody
#   (principle #2): verifying before the drainer has pushed bytes to the JuiceFS
#   backend would read from the spool/cache, not the true at-rest copy. Returns 0
#   if drained, 1 on timeout (and logs the residual counts so the author can see
#   WHAT was stuck). Polls every 2s.
#
#   DEFAULT timeout: when no TIMEOUT_S is passed we do NOT use a flat 300s — a
#   multi-GB drain needs far longer. Instead we SCALE the default from the spool's
#   own pending byte count (qa_spool_pending_bytes) via qa_drain_ceiling, falling
#   back to $QA_DRAIN_TIMEOUT only if the byte count is unavailable, and never
#   waiting LESS than $QA_DRAIN_MIN (600s). Callers that already know their staged
#   payload size SHOULD pass an explicit qa_drain_ceiling <bytes> ceiling.
qa_wait_drain() {
    local timeout p ip
    if [ -n "${1:-}" ]; then
        timeout="$1"
    else
        local pb pf; pb="$(qa_spool_pending_bytes)"
        p="$(qa_spool_pending)"
        ip="$(qa_spool_field in_progress)"
        case "$p" in ''|*[!0-9-]*) p=0 ;; esac
        case "$ip" in ''|*[!0-9-]*) ip=0 ;; esac
        [ "$p" -lt 0 ] 2>/dev/null && p=0
        [ "$ip" -lt 0 ] 2>/dev/null && ip=0
        pf=$(( p + ip ))
        if [ "$pb" -gt 0 ] 2>/dev/null || [ "$pf" -gt 0 ] 2>/dev/null; then
            timeout="$(qa_drain_ceiling "$pb" "$pf")"
        else
            timeout="$QA_DRAIN_TIMEOUT"
            [ "$timeout" -lt "$QA_DRAIN_MIN" ] 2>/dev/null && timeout="$QA_DRAIN_MIN"
        fi
    fi
    local waited=0
    while [ "$waited" -lt "$timeout" ]; do
        p="$(qa_spool_pending)"
        ip="$(qa_spool_field in_progress)"
        if [ "$p" = "0" ] && { [ "$ip" = "0" ] || [ "$ip" = "-1" ]; }; then
            qa_info "spool drained after ${waited}s"
            return 0
        fi
        if [ "$p" -lt 0 ] 2>/dev/null; then
            qa_warn "qa_wait_drain: /spool unreachable at ${waited}s"
        fi
        perl -e 'select undef,undef,undef,2'
        waited=$((waited+2))
    done
    qa_warn "qa_wait_drain TIMEOUT after ${timeout}s (pending=$(qa_spool_pending) in_progress=$(qa_spool_field in_progress) failed_files=$(qa_spool_actionable_failed) quarantined=$(qa_spool_field quarantined))"
    return 1
}

# ---------------------------------------------------------------------------
# Integrity / chain of custody

# qa_verify_custody MANIFEST DEST_DIR
#   For every "relpath<TAB>md5" line in MANIFEST, md5 the corresponding landed
#   file at DEST_DIR/relpath and compare. This is the END of the chain:
#   Finder write → NFS handler → write spool → drainer → JuiceFS backend →
#   READBACK (this md5). Echoes a summary line:
#     "custody OK=<n> MISSING=<n> WRONG=<n> total=<n>"
#   and returns 0 ONLY when MISSING==0 AND WRONG==0 (zero data loss, zero
#   corruption — the bar). Each MISSING/WRONG is logged with its full DEST path
#   so the failure names the offending file (principle #3).
#   NOTE: caller MUST qa_wait_drain BEFORE calling this. To force a true at-rest
#   readback (not a cached spool read), set QA_DROP_CACHE=1 and provide a
#   /sync or pin-drop hook if available; by default we read straight through the
#   mount, which is what the user experiences.
qa_verify_custody() {
    local manifest="$1" dest="$2"
    local ok=0 missing=0 wrong=0 total=0
    local rel want got landed
    # bash 3.2-safe line read; tab-delimited.
    while IFS="$(printf '\t')" read -r rel want; do
        [ -z "$rel" ] && continue
        total=$((total+1))
        landed="$dest/$rel"
        if [ ! -f "$landed" ]; then
            qa_fail "custody MISSING: $landed (expected md5=$want)"
            missing=$((missing+1))
            continue
        fi
        got="$(md5 -q "$landed" 2>/dev/null)"
        if [ "$got" = "$want" ] && [ -n "$got" ]; then
            ok=$((ok+1))
        else
            qa_fail "custody WRONG: $landed (want=$want got=$got)"
            wrong=$((wrong+1))
        fi
    done < "$manifest"
    qa_log "custody OK=$ok MISSING=$missing WRONG=$wrong total=$total"
    [ "$missing" -eq 0 ] && [ "$wrong" -eq 0 ]
}

# ---------------------------------------------------------------------------
# Zero-random-Finder-error gate (principle #3)

# _qa_grep_count FILE FLAGS PATTERN [PATTERN...]
#   Count matching LINES in FILE as exactly ONE clean integer (always 0 on no
#   match / missing file). FLAGS is an extra grep flag string ("" or "-i").
#   WHY this exists: the naive idiom `grep -c PAT FILE || echo 0` is BROKEN on
#   BSD grep — `grep -c` PRINTS "0" to stdout AND exits nonzero when nothing
#   matches, so the `|| echo 0` appends a SECOND "0" and the captured value
#   becomes the multi-line string "0\n0". That string then blows up every
#   downstream `[ "$n" -eq 0 ]` ("integer expression expected") and silently
#   flips a CLEAN log window into a false RED. We instead drop the `|| echo 0`
#   entirely (grep's own "0" is the right answer) and collapse the output to a
#   single integer with `tr -d '\n'`, defaulting empty/garbage to 0.
_qa_grep_count() {
    local file="$1" flags="$2"; shift 2
    local p n
    # After `shift 2`, "$@" holds ONLY the patterns. Rebuild "$@" in place as
    # "-e PAT -e PAT ..." (each pattern as a literal -e arg so a leading '-' in a
    # pattern like '-5000' is never mistaken for a grep flag). We splice each new
    # "-e PAT" pair to the FRONT of the still-unconsumed tail via a shift+append
    # rotation so we never clobber the patterns before reading them.
    local count=$#
    while [ "$count" -gt 0 ]; do
        p="$1"; shift
        set -- "$@" -e "$p"
        count=$((count-1))
    done
    if [ -n "$flags" ]; then
        n=$(grep -c "$flags" "$@" "$file" 2>/dev/null | tr -d '\n')
    else
        n=$(grep -c "$@" "$file" 2>/dev/null | tr -d '\n')
    fi
    [ -z "$n" ] && n=0
    case "$n" in *[!0-9]*) n=0 ;; esac
    printf '%s' "$n"
}

# qa_log_mark — echo the current line count of the JuiceMount log. Capture this
# BEFORE a test; pass it to qa_error_scan AFTER so only the test's own window is
# scanned. Echoes 0 if the log doesn't exist yet.
qa_log_mark() {
    [ -f "$JM_LOG" ] || { echo 0; return; }
    wc -l < "$JM_LOG" | tr -d ' '
}

# qa_error_scan SINCE_LINE [LABEL]
#   Scan the JuiceMount log window from SINCE_LINE..EOF for the user-visible
#   failure signatures (grounded in nfs/handler.go):
#     FromHandle STALE | purging phantom | 100070 | 100060 | -48 'already an item'
#     | -36 | -5000 | "you don't have permission" | "Operation not permitted"
#   Writes the matching lines to $QA_CAT_DIR/errscan-<LABEL>.txt and echoes a
#   tally line:  "errscan STALE=<n> PHANTOM=<n> E100070=<n> E100060=<n> E48=<n> E36=<n> E5000=<n> PERM=<n>"
#   Returns 0 if ALL counts are zero (clean), 1 otherwise.
#
#   KNOWN-OPEN EDGES (assert-on, drive to zero — do NOT mask): the caller may
#   choose to qa_warn (not qa_fail) on:
#     * residual ~1 "FromHandle STALE" per ~960 ._-heavy files
#     * a single -48 'already an item' on a back-to-back copy to the SAME dest
#       right after a delete (directory delete-lag).
#   Every OTHER occurrence = qa_fail with the offending path named. Pull the
#   path from the dumped errscan file (each STALE line logs path=...).
qa_error_scan() {
    local since="${1:-0}" label="${2:-scan}"
    local out="$QA_CAT_DIR/errscan-${label}.txt"
    [ -f "$JM_LOG" ] || { echo "errscan STALE=0 PHANTOM=0 E100070=0 E100060=0 E48=0 E36=0 E5000=0 PERM=0"; return 0; }
    local total
    total="$(wc -l < "$JM_LOG" | tr -d ' ')"
    # Window = lines (since+1)..end. tail -n +K is 1-based.
    tail -n +$(( since + 1 )) "$JM_LOG" > "$out.window" 2>/dev/null

    local stale phantom e70 e60 e48 e36 e5000 perm
    stale=$(_qa_grep_count   "$out.window" ""   'FromHandle STALE')
    phantom=$(_qa_grep_count "$out.window" ""   'purging phantom')
    e70=$(_qa_grep_count     "$out.window" ""   '100070' 'NFS3ERR_STALE')
    e60=$(_qa_grep_count     "$out.window" ""   '100060')
    e48=$(_qa_grep_count     "$out.window" "-i" 'already an item' 'error -48' 'errAEItemAlreadyExists')
    e36=$(_qa_grep_count     "$out.window" ""   'error -36' 'ioErr')
    e5000=$(_qa_grep_count   "$out.window" ""   '-5000' 'afpAccessDenied')
    perm=$(_qa_grep_count    "$out.window" "-i" "you don't have permission" 'Operation not permitted' 'permission denied')

    # Keep only the matching lines in the artifact for path attribution.
    grep -nE 'FromHandle STALE|purging phantom|100070|100060|already an item|error -48|error -36|ioErr|-5000|permission' \
        "$out.window" > "$out" 2>/dev/null
    rm -f "$out.window" 2>/dev/null

    echo "errscan STALE=$stale PHANTOM=$phantom E100070=$e70 E100060=$e60 E48=$e48 E36=$e36 E5000=$e5000 PERM=$perm"
    [ "$stale" -eq 0 ] && [ "$phantom" -eq 0 ] && [ "$e70" -eq 0 ] && [ "$e60" -eq 0 ] \
        && [ "$e48" -eq 0 ] && [ "$e36" -eq 0 ] && [ "$e5000" -eq 0 ] && [ "$perm" -eq 0 ]
}

# qa_finder_result_clean RESULT_STRING — inspect a captured Finder result/error
# string (from qa_finder_copy/move/delete) for the same user-visible error codes.
# Returns 0 if clean, 1 if it contains an AppleScript error code we gate on.
# Use right after a Finder op: `out=$(qa_finder_copy ...); qa_finder_result_clean "$out"`.
qa_finder_result_clean() {
    local s="$1"
    printf '%s' "$s" | grep -qiE 'error -48|already an item|error -36|-5000|error -5000|you don.t have permission|error -1407|error -43' && return 1
    return 0
}

# ---------------------------------------------------------------------------
# Snappiness gate (principle #4)

# qa_dir_listing_ms PATH — time a single readdir (`ls -1f`) of PATH and echo the
# elapsed wall time in integer milliseconds. `-f` disables sorting/stat so we
# measure the readdir/LOOKUP path served from the local metadata DB, not a
# stat-storm. A cached dir over the local DB must come back < $QA_SNAPPY_MS.
# Echoes -1 if the listing itself failed (caller treats as FAIL, not slow).
qa_dir_listing_ms() {
    local path="$1"
    local t0 t1
    t0=$(_qa_now_ms)
    if ! qa_timeout 30 ls -1f "$path" >/dev/null 2>&1; then
        echo -1; return
    fi
    t1=$(_qa_now_ms)
    echo $(( t1 - t0 ))
}

# _qa_now_ms — current time in ms. perl gives sub-second precision (date +%N is
# unavailable on macOS/BSD).
_qa_now_ms() {
    perl -MTime::HiRes=time -e 'printf "%d", time()*1000'
}

# ---------------------------------------------------------------------------
# Offline / control plane

# qa_offline on|off — POST/GET the offline toggle. Maps:
#   on  → GET /offline?on=1   (force offline: writes buffer to spool, no drain)
#   off → GET /offline?on=0   (back online: drainer resumes)
# Echoes the JSON response; returns 0 if the call succeeded (HTTP body non-empty).
qa_offline() {
    local mode="$1" v body
    case "$mode" in
        on|1|true)  v=1 ;;
        off|0|false) v=0 ;;
        *) qa_fail "qa_offline: bad mode '$mode' (want on|off)"; return 2 ;;
    esac
    body="$(qa_timeout 5 curl -s "$CP_BASE/offline?on=$v" 2>/dev/null)"
    printf '%s\n' "$body"
    [ -n "$body" ]
}

# qa_health — echo GET /health body; returns 0 if it parses as healthy:true.
qa_health() {
    local body
    body="$(qa_timeout 5 curl -s "$CP_BASE/health" 2>/dev/null)"
    printf '%s\n' "$body"
    printf '%s' "$body" | grep -q '"healthy"[[:space:]]*:[[:space:]]*true'
}

# qa_cp_reachable — 0 if the control plane answers /health at all (used by
# preflight to fail fast with a clear message instead of every case timing out).
qa_cp_reachable() {
    qa_timeout 5 curl -sf "$CP_BASE/health" >/dev/null 2>&1
}

# ---------------------------------------------------------------------------
# Preflight + mount safety

# qa_preflight — assert the environment is sane before any case runs:
#   * control plane reachable
#   * MOUNT is an active NFS mount
#   * MOUNT is writable
#   * QA_DEST_ROOT exists/creatable and is UNDER the mount (never the user's dirs)
# Returns 0 if safe to proceed, 1 otherwise (caller should qa_end + exit).
qa_preflight() {
    local ok=0
    if ! qa_cp_reachable; then
        qa_fail "preflight: control plane $CP_BASE not reachable"; ok=1
    fi
    if ! mount | awk -v m="$MOUNT" '$0 ~ " "m" " && /nfs/ {f=1} END{exit !f}'; then
        qa_fail "preflight: $MOUNT is not an active NFS mount"; ok=1
    fi
    case "$QA_DEST_ROOT" in
        "$MOUNT"/*) : ;;  # good — dest is under the mount
        *) qa_fail "preflight: QA_DEST_ROOT '$QA_DEST_ROOT' is NOT under MOUNT '$MOUNT' — refusing (non-destructive guard)"; ok=1 ;;
    esac
    if [ "$ok" -eq 0 ]; then
        mkdir -p "$QA_DEST_ROOT" 2>/dev/null
        local probe="$QA_DEST_ROOT/.qa-write-probe-$$"
        if : > "$probe" 2>/dev/null; then rm -f "$probe" 2>/dev/null; else
            qa_fail "preflight: $QA_DEST_ROOT not writable"; ok=1
        fi
    fi
    return "$ok"
}

# ---------------------------------------------------------------------------
# Cleanup (non-destructive: ONLY our own dests + local staging)

# qa_cleanup — remove this run's staged sources and the battery dest subtree.
# GUARD: refuses to rm anything not under $MOUNT/JM_RELEASE_BATTERY or $QA_STAGE,
# so a misconfigured QA_DEST_ROOT can never delete user data. Best-effort; never
# fails the gate. Leaves $QA_ARTIFACTS (manifests/scans) for post-run review.
qa_cleanup() {
    case "$QA_DEST_ROOT" in
        "$MOUNT"/JM_RELEASE_BATTERY*)
            # Delete only the THIS-RUN tagged subdirs the scripts created; the
            # scripts name their dests QA_<epoch>_<pid>_* under QA_DEST_ROOT.
            find "$QA_DEST_ROOT" -maxdepth 1 -type d -name "QA_*_$$_*" -prune -exec rm -rf {} + 2>/dev/null
            ;;
        *)
            qa_warn "qa_cleanup: QA_DEST_ROOT '$QA_DEST_ROOT' outside guard — NOT removing"
            ;;
    esac
    case "$QA_STAGE" in
        /tmp/jm-battery-stage-*) rm -rf "$QA_STAGE" 2>/dev/null ;;
    esac
    return 0
}
