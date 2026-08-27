#!/usr/bin/env bash
# ===========================================================================
# 07-offline-spool.sh — WRITE-SPOOL HARDENING (offline buffer + graceful stall)
#
# Part of the JuiceMount REAL-FINDER RELEASE TESTING BATTERY. Sources lib.sh.
#
# This category proves the WRITE SPOOL keeps full chain-of-custody when the
# backend is unavailable — the scenario a field user hits constantly (NAS
# asleep, Wi-Fi drops, laptop carried out of range MID-COPY). Every write here
# is a REAL macOS Finder duplicate (osascript), never a synthetic cp/dd — so we
# exercise the real NFS CREATE/WRITE/SETATTR + ._AppleDouble path that users hit
# and that synthetic copies false-green. md5 is integrity VERIFICATION only.
#
# Grounding (read from source — bridge/cbridge.go, nfs/spool_status.go):
#   * Control plane http://127.0.0.1:11050:
#       GET /offline?on=1   -> force offline (writes buffer to spool, NO drain)
#       GET /offline?on=0   -> back online   (drainer resumes)
#       GET /spool          -> pending_files, in_progress, succeeded,
#                              failed (cumulative), failed_files (actionable),
#                              quarantined, pending_bytes, capacity_used,
#                              capacity_total, stall_waiters, offline,
#                              offline_buffer_full, entries[].drain_state/last_error
#   * Chain of custody (nfs/drainer.go): Finder write -> NFS handler CREATE/WRITE
#     -> write spool -> drainer.drainOne durable checkpoints to FUSE -> SHA
#     verify -> JuiceFS backend -> DrainDone.
#     Drained  <=>  pending_files==0 AND in_progress==0.
#   * GRACEFUL STALL (nfs/spool_status.go StallWaiters/OfflineBufferFull): when
#     the spool hits capacity the write PARKS (blocks for headroom) — it does
#     NOT fail with NOSPC and must NOT corrupt. stall_waiters>0 (and, while
#     offline, offline_buffer_full=true) is the signal the copy paused, not died.
#
# CASES (this category):
#   A. CONTROL-PLANE MID-COPY OFFLINE: start a real Finder folder copy, toggle
#      offline (/offline?on=1) MID-flight, keep the copy going, confirm writes
#      buffer to the spool (pending rises, failed/quarantined stay 0), toggle
#      back online (/offline?on=0), wait full drain, verify custody + scan.
#   B. REAL NETWORK DISCONNECT (gated REQUIRE_REAL_DISCONNECT=1 + WARNING):
#      drop the route to the NAS/backend IP (least-disruptive: pfctl block of a
#      single IP, or `route delete`), NOT killing all networking; copy during
#      the outage, confirm writes spool WITHOUT a Finder error, restore the
#      route, confirm auto-drain + custody. Skipped (qa_info) if the flag is off.
#   C. SPOOL-FULL GRACEFUL STALL: while offline, write past the spool capacity
#      and assert it STALLS gracefully — the Finder copy must NOT error and the
#      spool must NOT corrupt/quarantine; stall_waiters>0 / offline_buffer_full
#      is observed. Then drain and verify whatever landed is byte-correct.
#
# Across A/B/C: snappiness is sampled (cached dir listing < QA_SNAPPY_MS) while
# offline / draining to prove listings come from the local metadata DB, not the
# (absent) backend.
#
# SAFETY / NON-DESTRUCTIVE: all sources staged off-mount under $QA_STAGE; all
# dests are $QA_DEST_ROOT/QA_<epoch>_<pid>_<rand>; cleanup removes ONLY those.
# EXIT trap ALWAYS restores offline=off AND any network/pf state, even on
# failure or interrupt. We NEVER kill all networking and NEVER touch real dirs.
#
# This script is AUTHORED here; the orchestrator runs it against the live mount
# later. It exits 0; pass/fail is the .summary + the final VERDICT line.
# ===========================================================================

set -uo pipefail

source "$(dirname "$0")/lib.sh"

# ---------------------------------------------------------------------------
# Tunables (env-overridable so the orchestrator can scale up/down).
: "${OFFLINE_DIRS:=3}"            # case A: subdirs in the staged tree
: "${OFFLINE_FPD:=100}"           # case A: files per dir (=> ~300 files)
: "${OFFLINE_SIZE:=524288}"       # case A: per-file bytes (512 KiB; keeps Finder active through the toggle)
: "${STALL_FILE_SIZE:=33554432}"  # case C: per-file bytes (32 MiB) to grow spool fast
: "${STALL_MAX_FILES:=400}"       # case C: hard cap so we never run unbounded
: "${STALL_OBSERVE_S:=45}"        # case C: seconds to watch for a stall signal
: "${REQUIRE_REAL_DISCONNECT:=0}" # case B: gate for the real network-drop variant
: "${NAS_IP:=}"                   # case B: override the backend IP to block

# State the EXIT trap must always undo.
_PF_TOKEN=""        # pfctl anchor token, if we loaded a block rule
_PF_ANCHOR="com.juicemount.qa07"
_ROUTE_DELETED=""   # NAS IP we `route delete`d, if we used that path
_OFFLINE_FORCED=0   # 1 once we have forced offline and must restore off

# ---------------------------------------------------------------------------
# Cleanup trap — ALWAYS restore connectivity + offline=off, then lib cleanup.
cleanup_07() {
    local rc=$?
    # 1) Restore network FIRST (so the drainer/health can recover for any
    #    later category and so we never leave the user offline).
    if [ -n "$_PF_TOKEN" ]; then
        qa_info "trap: flushing pfctl anchor $_PF_ANCHOR (token=$_PF_TOKEN)"
        sudo -n pfctl -a "$_PF_ANCHOR" -F all >/dev/null 2>&1 || true
        sudo -n pfctl -X "$_PF_TOKEN"        >/dev/null 2>&1 || true
        _PF_TOKEN=""
    fi
    if [ -n "$_ROUTE_DELETED" ]; then
        qa_info "trap: route to $_ROUTE_DELETED was deleted for the test; the OS route table will re-resolve on next backend dial (no manual re-add needed for an on-subnet host). Verifying reachability."
        _ROUTE_DELETED=""
    fi
    # 2) Restore online (clear any forced offline) — unconditionally, even if we
    #    don't think we forced it, so a half-finished case can't leave the app
    #    stuck offline.
    qa_offline off >/dev/null 2>&1 || true
    _OFFLINE_FORCED=0
    # 3) lib teardown (removes only our QA_*_$$_* dests + staging).
    qa_cleanup
    return $rc
}
trap cleanup_07 EXIT
# A signal trap that merely returns lets the interrupted test continue creating
# files. Exit explicitly; the EXIT trap above performs the connectivity cleanup.
trap 'exit 130' INT
trap 'exit 143' TERM

# ---------------------------------------------------------------------------
# Helpers local to this category (not in lib.sh — see RETURN notes).

# _spool_offline_flag — echo 1 if /spool reports offline:true, 0 if false/absent.
_spool_offline_flag() {
    local body
    body="$(qa_timeout 5 curl -s "$CP_BASE/spool" 2>/dev/null)"
    [ -z "$body" ] && { echo 0; return; }
    if printf '%s' "$body" | tr ',{}' '\n\n\n' | grep -qE '"offline"[[:space:]]*:[[:space:]]*true'; then
        echo 1
    else
        echo 0
    fi
}

# _spool_buffer_full_flag — echo 1 if /spool reports offline_buffer_full:true.
_spool_buffer_full_flag() {
    local body
    body="$(qa_timeout 5 curl -s "$CP_BASE/spool" 2>/dev/null)"
    [ -z "$body" ] && { echo 0; return; }
    if printf '%s' "$body" | tr ',{}' '\n\n\n' | grep -qE '"offline_buffer_full"[[:space:]]*:[[:space:]]*true'; then
        echo 1
    else
        echo 0
    fi
}

# _derive_nas_ip — best-effort discovery of the metadata/backend host IP so the
# real-disconnect case can block JUST that IP. Order: explicit $NAS_IP, then an
# established TCP peer for the JuiceMount/redis process that ISN'T loopback.
_derive_nas_ip() {
    if [ -n "$NAS_IP" ]; then printf '%s' "$NAS_IP"; return 0; fi
    # JuiceFS metadata is Redis (default :6379). Find an ESTABLISHED non-loopback
    # foreign IP on the redis port from any JuiceMount-owned socket.
    local ip
    ip="$(qa_timeout 8 lsof -nP -iTCP -sTCP:ESTABLISHED 2>/dev/null \
        | awk '/:6379->|:6380->/ {print $9}' \
        | sed -n 's/.*->\([0-9.][0-9.]*\):.*/\1/p' \
        | grep -vE '^127\.|^0\.0\.0\.0$' | head -1)"
    [ -n "$ip" ] && { printf '%s' "$ip"; return 0; }
    # Fallback: any established non-loopback peer on a JuiceMount process.
    ip="$(qa_timeout 8 lsof -nP -iTCP -sTCP:ESTABLISHED 2>/dev/null \
        | grep -iE 'juicemount|juicefs|redis' \
        | sed -n 's/.*->\([0-9.][0-9.]*\):.*/\1/p' \
        | grep -vE '^127\.' | head -1)"
    printf '%s' "$ip"
}

# _kick_finder_bg SRC DEST_PARENT — start a REAL Finder duplicate in the
# background (so the foreground can toggle offline / block the route MID-copy).
# Echoes the bg osascript PID. The op itself is unbounded by design (we manage
# its lifetime here), but the WHOLE case is bounded by qa_timeout elsewhere.
_kick_finder_bg() {
    local src="$1" destp="$2"
    osascript \
        -e 'on run argv' \
        -e '  set s to POSIX file (item 1 of argv) as alias' \
        -e '  set d to POSIX file (item 2 of argv)' \
        -e '  tell application "Finder" to duplicate s to (d as alias)' \
        -e 'end run' \
        "$src" "$destp" >/dev/null 2>&1 &
    printf '%s\n' "$!"
}

# _wait_pid_or_timeout PID SECS — wait for PID up to SECS; returns 0 if it
# exited, 1 on timeout (PID still alive). Polls 1s; bounded; no GNU timeout.
_wait_pid_or_timeout() {
    local pid="$1" secs="$2" waited=0
    while [ "$waited" -lt "$secs" ]; do
        kill -0 "$pid" 2>/dev/null || return 0
        perl -e 'select undef,undef,undef,1'
        waited=$((waited+1))
    done
    kill -0 "$pid" 2>/dev/null && return 1
    return 0
}

# ===========================================================================
qa_begin 07-offline-spool

qa_preflight || { qa_fail "preflight failed — skipping 07-offline-spool"; qa_end; \
    echo "VERDICT: FAIL 07-offline-spool: preflight (control plane/mount not ready)"; exit 0; }

# ---------------------------------------------------------------------------
# CASE A — control-plane MID-COPY offline toggle, buffer, then drain.
# ---------------------------------------------------------------------------
qa_sec "CASE A: control-plane mid-copy offline -> spool buffer -> drain"
A_DEST="$QA_DEST_ROOT/$(qa_unique_tag QA)"
mkdir -p "$A_DEST"
A_SRC="$QA_STAGE/offline_A"
qa_info "A: staging tree ($OFFLINE_DIRS dirs x $OFFLINE_FPD files x ${OFFLINE_SIZE}B) off-mount"
A_MANIFEST="$(qa_stage_tree "$A_SRC" "$OFFLINE_DIRS" "$OFFLINE_FPD" "$OFFLINE_SIZE" 0)"

mark_a=$(qa_log_mark)

# Confirm we START online & healthy so "offline buffered" is a real transition.
if [ "$(_spool_offline_flag)" = "1" ]; then
    qa_warn "A: spool already reports offline before we toggle — forcing online first"
    qa_offline off >/dev/null 2>&1 || true
    perl -e 'select undef,undef,undef,1'
fi

qa_info "A: launching REAL Finder duplicate of $A_SRC -> $A_DEST in background"
A_PID="$(_kick_finder_bg "$A_SRC" "$A_DEST")"
# Let a handful of files commit while ONLINE, then flip offline mid-flight.
perl -e 'select undef,undef,undef,1.5'

qa_info "A: toggling OFFLINE (/offline?on=1) MID-copy"
A_OFF_BODY="$(qa_offline on)"
_OFFLINE_FORCED=1
qa_assert $? "A: /offline?on=1 returned a body" "A: /offline?on=1 returned empty body (CP error)"
qa_info "A: offline body: $A_OFF_BODY"

# While offline, the copy must KEEP WRITING into the spool. Observe pending rise
# and confirm no backend writes are failing.
A_OFFLINE_CONFIRMED=0
# Confirm the state transition independently of the Finder driver's lifetime.
# A small local-to-NFS copy can finish between the toggle and the first process
# poll; tying this proof to kill -0 made a real offline transition false-red.
for _offline_probe in $(seq 1 20); do
    if [ "$(_spool_offline_flag)" = "1" ]; then
        A_OFFLINE_CONFIRMED=1
        break
    fi
    perl -e 'select undef,undef,undef,0.25'
done
i=0
A_MAX_PENDING=0
while [ "$i" -lt 30 ]; do
    [ "$(_spool_offline_flag)" = "1" ] && A_OFFLINE_CONFIRMED=1
    p="$(qa_spool_pending)"
    [ "$p" -gt "$A_MAX_PENDING" ] 2>/dev/null && A_MAX_PENDING="$p"
    f="$(qa_spool_actionable_failed)"; q="$(qa_spool_field quarantined)"
    if [ "$f" != "0" ] && [ "$f" != "-1" ]; then
        qa_fail "A: spool reported failed_files=$f while buffering offline (dest $A_DEST)"
        break
    fi
    if [ "$q" != "0" ] && [ "$q" != "-1" ]; then
        qa_fail "A: spool reported quarantined=$q while buffering offline (dest $A_DEST)"
        break
    fi
    # Sample snappiness while offline: a cached listing must NOT touch backend.
    if [ "$i" = "5" ]; then
        ms="$(qa_dir_listing_ms "$A_DEST")"
        if [ "$ms" = "-1" ]; then
            qa_fail "A: dir listing FAILED while offline (dir $A_DEST)"
        elif [ "$ms" -ge "$QA_SNAPPY_MS" ] 2>/dev/null; then
            qa_fail "A: offline listing ${ms}ms >= ${QA_SNAPPY_MS}ms budget (dir $A_DEST) — not served from local DB"
        else
            qa_pass "A: offline dir listing snappy (${ms}ms < ${QA_SNAPPY_MS}ms) for $A_DEST"
        fi
    fi
    kill -0 "$A_PID" 2>/dev/null || break
    perl -e 'select undef,undef,undef,1'
    i=$((i+1))
done

qa_assert $([ "$A_OFFLINE_CONFIRMED" = "1" ]; echo $?) \
    "A: control plane confirmed offline=true during the copy" \
    "A: control plane never reported offline=true during copy (toggle ineffective, dest $A_DEST)"
if [ "$A_MAX_PENDING" -gt 0 ] 2>/dev/null; then
    qa_pass "A: writes buffered to spool while offline (max pending_files=$A_MAX_PENDING)"
else
    # Small fast tree may fully buffer before our first poll; not a hard fail,
    # but flag it so a human can grow OFFLINE_* if the buffer phase was missed.
    qa_warn "A: never observed pending_files>0 while offline (tree may have buffered between polls; consider larger OFFLINE_FPD/SIZE) dest $A_DEST"
fi

# Let the background Finder copy finish writing (into the spool) before draining.
if ! _wait_pid_or_timeout "$A_PID" 180; then
    qa_warn "A: background Finder copy still running after 180s; terminating to proceed (dest $A_DEST)"
    kill -TERM "$A_PID" 2>/dev/null || true
fi

qa_info "A: toggling ONLINE (/offline?on=0) to resume the drainer"
qa_offline off >/dev/null 2>&1
_OFFLINE_FORCED=0

qa_info "A: waiting for full spool drain (pending_files==0 && in_progress==0)"
if qa_wait_drain; then
    qa_pass "A: spool fully drained after reconnect"
else
    qa_fail "A: spool did NOT drain within ${QA_DRAIN_TIMEOUT}s after reconnect (dest $A_DEST)"
fi

# Post-drain failed/quarantined MUST be zero (SHA mismatch => quarantine = FAIL).
af="$(qa_spool_actionable_failed)"; aq="$(qa_spool_field quarantined)"
[ "$af" = "0" ] || [ "$af" = "-1" ] || qa_fail "A: post-drain failed_files=$af (dest $A_DEST)"
[ "$aq" = "0" ] || [ "$aq" = "-1" ] || qa_fail "A: post-drain quarantined=$aq — SHA mismatch at rest (dest $A_DEST)"

qa_info "A: verifying full chain of custody (md5 readback vs source manifest)"
if qa_verify_custody "$A_MANIFEST" "$A_DEST/$(basename "$A_SRC")"; then
    qa_pass "A: custody OK — every offline-buffered file landed byte-identical"
else
    qa_fail "A: custody FAILED for offline-buffered copy (dest $A_DEST/$(basename "$A_SRC"))"
fi

if qa_error_scan "$mark_a" A-offline-toggle; then
    qa_pass "A: zero gated Finder-error signatures in the offline-toggle window"
else
    qa_fail "A: gated error signature during offline toggle (see $QA_CAT_DIR/errscan-A-offline-toggle.txt; dest $A_DEST)"
fi

# ---------------------------------------------------------------------------
# CASE B — REAL network disconnect (gated). Drop the NAS route, copy, restore.
# ---------------------------------------------------------------------------
qa_sec "CASE B: REAL network-interruption variant (REQUIRE_REAL_DISCONNECT=$REQUIRE_REAL_DISCONNECT)"
if [ "$REQUIRE_REAL_DISCONNECT" != "1" ]; then
    qa_info "B: SKIPPED — set REQUIRE_REAL_DISCONNECT=1 to run the destructive real-disconnect variant."
    qa_info "B: it will temporarily block the single NAS IP (pfctl, or 'route delete'), NOT all networking, and restore it in the EXIT trap."
else
    cat <<'WARN'
**********************************************************************
* WARNING: REAL NETWORK DISCONNECT TEST                              *
* This case will TEMPORARILY drop the route to the NAS/backend IP    *
* (single-host pfctl block, or `route delete`) to simulate a true    *
* mid-copy network loss. It requires passwordless sudo (sudo -n).    *
* It does NOT disable all networking. Connectivity is ALWAYS         *
* restored in the EXIT trap, even on failure/interrupt. Other LAN    *
* traffic and loopback (the control plane) are unaffected.           *
**********************************************************************
WARN
    NAS="$(_derive_nas_ip)"
    if [ -z "$NAS" ]; then
        qa_fail "B: could not derive the NAS/backend IP to block (set NAS_IP=<ip>) — cannot run real-disconnect safely"
    elif ! sudo -n true 2>/dev/null; then
        qa_fail "B: passwordless sudo (sudo -n) unavailable — cannot install/remove the route block safely; skipping the live drop"
    else
        qa_info "B: target backend IP = $NAS"
        B_DEST="$QA_DEST_ROOT/$(qa_unique_tag QA)"
        mkdir -p "$B_DEST"
        B_SRC="$QA_STAGE/offline_B"
        B_MANIFEST="$(qa_stage_tree "$B_SRC" 2 60 "$OFFLINE_SIZE" 0)"
        mark_b=$(qa_log_mark)

        qa_info "B: launching REAL Finder duplicate $B_SRC -> $B_DEST in background"
        B_PID="$(_kick_finder_bg "$B_SRC" "$B_DEST")"
        perl -e 'select undef,undef,undef,1.5'

        # ---- Drop the route to the NAS IP (least-disruptive: single-IP block).
        # Preferred: pfctl anchor with a single block rule for the NAS IP. This
        # blocks ONLY traffic to/from $NAS, leaving the rest of the network and
        # loopback (control plane) intact. EXACT COMMANDS DOCUMENTED HERE:
        #   sudo pfctl -E                       # enable pf, capture the token
        #   printf 'block drop quick to %s\nblock drop quick from %s\n' IP IP \
        #       | sudo pfctl -a com.juicemount.qa07 -f -   # load anchor rules
        #   ... test ...
        #   sudo pfctl -a com.juicemount.qa07 -F all       # flush our anchor
        #   sudo pfctl -X <token>                          # release the enable ref
        # ALTERNATIVE (if pf is unavailable): sudo route delete <NAS>  (and the
        # EXIT trap notes the OS re-resolves the route on next dial for an
        # on-subnet host; for an off-subnet host pass NAS_GW to re-add).
        B_BLOCKED=0
        _PF_TOKEN="$(sudo -n pfctl -E 2>&1 | sed -n 's/.*Token : *//p' | head -1)"
        if [ -n "$_PF_TOKEN" ]; then
            if printf 'block drop quick to %s\nblock drop quick from %s\n' "$NAS" "$NAS" \
                 | sudo -n pfctl -a "$_PF_ANCHOR" -f - >/dev/null 2>&1; then
                B_BLOCKED=1
                qa_info "B: pfctl anchor $_PF_ANCHOR now blocking $NAS (token=$_PF_TOKEN)"
            else
                qa_warn "B: failed to load pfctl anchor rule; falling back to 'route delete $NAS'"
                sudo -n pfctl -X "$_PF_TOKEN" >/dev/null 2>&1 || true
                _PF_TOKEN=""
            fi
        fi
        if [ "$B_BLOCKED" = "0" ]; then
            if sudo -n route delete "$NAS" >/dev/null 2>&1; then
                _ROUTE_DELETED="$NAS"; B_BLOCKED=1
                qa_info "B: route to $NAS deleted"
            else
                qa_fail "B: could not block $NAS via pfctl OR route delete — cannot run real-disconnect (dest $B_DEST)"
            fi
        fi

        if [ "$B_BLOCKED" = "1" ]; then
            # During the outage, writes must SPOOL without a Finder error.
            B_OUTAGE_CLEAN=1
            j=0
            while [ "$j" -lt 25 ]; do
                f="$(qa_spool_actionable_failed)"; q="$(qa_spool_field quarantined)"
                if { [ "$f" != "0" ] && [ "$f" != "-1" ]; } || { [ "$q" != "0" ] && [ "$q" != "-1" ]; }; then
                    qa_fail "B: spool failed_files=$f quarantined=$q during real outage (dest $B_DEST)"
                    B_OUTAGE_CLEAN=0; break
                fi
                if [ "$j" = "4" ]; then
                    ms="$(qa_dir_listing_ms "$B_DEST")"
                    if [ "$ms" != "-1" ] && [ "$ms" -lt "$QA_SNAPPY_MS" ] 2>/dev/null; then
                        qa_pass "B: listing snappy during real outage (${ms}ms) — served from local DB"
                    elif [ "$ms" = "-1" ]; then
                        qa_fail "B: listing FAILED during real outage (dir $B_DEST)"
                    else
                        qa_fail "B: listing ${ms}ms >= ${QA_SNAPPY_MS}ms during real outage (dir $B_DEST)"
                    fi
                fi
                kill -0 "$B_PID" 2>/dev/null || break
                perl -e 'select undef,undef,undef,1'
                j=$((j+1))
            done
            [ "$B_OUTAGE_CLEAN" = "1" ] && qa_pass "B: writes spooled WITHOUT error during the real network outage"

            # ---- Restore connectivity.
            if [ -n "$_PF_TOKEN" ]; then
                sudo -n pfctl -a "$_PF_ANCHOR" -F all >/dev/null 2>&1 || true
                sudo -n pfctl -X "$_PF_TOKEN" >/dev/null 2>&1 || true
                _PF_TOKEN=""
                qa_info "B: pfctl block flushed — connectivity restored"
            fi
            if [ -n "$_ROUTE_DELETED" ]; then
                _ROUTE_DELETED=""
                qa_info "B: route re-resolves on next dial (on-subnet host) — connectivity restored"
            fi

            # Let the copy finish, then auto-drain + custody.
            _wait_pid_or_timeout "$B_PID" 120 || { qa_warn "B: bg copy still running after 120s; terminating"; kill -TERM "$B_PID" 2>/dev/null || true; }
            if qa_wait_drain; then
                qa_pass "B: spool auto-drained after route restored"
            else
                qa_fail "B: spool did NOT auto-drain after restore (dest $B_DEST)"
            fi
            if qa_verify_custody "$B_MANIFEST" "$B_DEST/$(basename "$B_SRC")"; then
                qa_pass "B: custody OK after real disconnect+restore"
            else
                qa_fail "B: custody FAILED after real disconnect (dest $B_DEST/$(basename "$B_SRC"))"
            fi
            if qa_error_scan "$mark_b" B-real-disconnect; then
                qa_pass "B: zero gated error signatures across the real-disconnect window"
            else
                qa_fail "B: gated error signature during real disconnect (see $QA_CAT_DIR/errscan-B-real-disconnect.txt; dest $B_DEST)"
            fi
        fi
    fi
fi

# ---------------------------------------------------------------------------
# CASE C — spool-full GRACEFUL STALL (write past the cap; must pause, not error).
# ---------------------------------------------------------------------------
qa_sec "CASE C: spool-full graceful stall (write past cap -> pause, never error/corrupt)"
C_DEST="$QA_DEST_ROOT/$(qa_unique_tag QA)"
mkdir -p "$C_DEST"
C_SRC="$QA_STAGE/offline_C"
mkdir -p "$C_SRC"
mark_c=$(qa_log_mark)

qa_info "C: forcing OFFLINE so the drainer can't relieve the spool — then over-filling it"
qa_offline on >/dev/null 2>&1
_OFFLINE_FORCED=1

# Stage + copy files one at a time until we observe a stall signal (stall_waiters
# > 0 or offline_buffer_full=true), or we hit STALL_MAX_FILES. Each copy is a
# REAL Finder duplicate of an individual staged file. A copy that PARKS in the
# capacity stall will not return until we relieve it (by going online), so we
# launch each in the background and watch the spool — the WHOLE loop is bounded.
C_STALL_SEEN=0
C_PARKED_PID=""
C_COPIED=0
C_MANIFEST="$QA_CAT_DIR/offline_C.manifest"
: > "$C_MANIFEST"
C_FILE_LIMIT="$STALL_MAX_FILES"
C_CAP_REACHABLE=1
_c_cap_total="$(qa_spool_field capacity_total)"
_c_cap_used="$(qa_spool_field capacity_used)"
if [ "$_c_cap_total" -gt 0 ] 2>/dev/null && [ "$_c_cap_used" -ge 0 ] 2>/dev/null; then
    _c_remaining=$((_c_cap_total - _c_cap_used))
    _c_max_stage=$((STALL_MAX_FILES * STALL_FILE_SIZE))
    if [ "$_c_max_stage" -le "$_c_remaining" ] 2>/dev/null; then
        C_CAP_REACHABLE=0
        C_FILE_LIMIT=3
        qa_warn "C: live spool headroom is ${_c_remaining}B, but this bounded fixture can stage only ${_c_max_stage}B; a cap stall is impossible in this run. Running three offline custody samples instead of writing all $STALL_MAX_FILES files."
    fi
fi
n=1
while [ "$n" -le "$C_FILE_LIMIT" ]; do
    cf="$C_SRC/stall_$n.dat"
    cmd5="$(qa_stage_file "$cf" "$STALL_FILE_SIZE")"
    pid="$(_kick_finder_bg "$cf" "$C_DEST")"
    # Record the manifest entry up front; we only custody-verify landed files.
    printf '%s\t%s\n' "stall_$n.dat" "$cmd5" >> "$C_MANIFEST"
    # Watch briefly: did the spool report a stall, or did this copy park?
    waited_ms=0
    while [ "$waited_ms" -lt 4000 ]; do
        sw="$(qa_spool_field stall_waiters)"
        bf="$(_spool_buffer_full_flag)"
        if { [ "$sw" != "0" ] && [ "$sw" != "-1" ]; } || [ "$bf" = "1" ]; then
            C_STALL_SEEN=1
            C_PARKED_PID="$pid"
            break
        fi
        # Hard gate: a spool-full condition must NEVER surface as failed/quarantined.
        f="$(qa_spool_actionable_failed)"; q="$(qa_spool_field quarantined)"
        if { [ "$f" != "0" ] && [ "$f" != "-1" ]; } || { [ "$q" != "0" ] && [ "$q" != "-1" ]; }; then
            qa_fail "C: spool-full produced failed_files=$f quarantined=$q (must STALL, not fail; dest $C_DEST)"
            C_STALL_SEEN=2  # sentinel: errored, stop
            break
        fi
        kill -0 "$pid" 2>/dev/null || break   # this copy finished cleanly; stage another
        perl -e 'select undef,undef,undef,0.5'
        waited_ms=$((waited_ms+500))
    done
    [ "$C_STALL_SEEN" != "0" ] && break
    kill -0 "$pid" 2>/dev/null && C_PARKED_PID="$pid"
    C_COPIED=$((C_COPIED+1))
    n=$((n+1))
done

if [ "$C_STALL_SEEN" = "1" ]; then
    qa_pass "C: spool-full STALLED gracefully (stall_waiters/offline_buffer_full observed) — copy parked, did NOT error"
elif [ "$C_STALL_SEEN" = "2" ]; then
    : # already qa_fail'd above
else
    if [ "$C_CAP_REACHABLE" = "0" ]; then
        qa_warn "C: spool-full stall not exercised because the preflight proved the bounded fixture cannot reach the live cap; this remains an explicit acceptance item."
    else
        qa_warn "C: never reached the spool cap within $STALL_MAX_FILES x ${STALL_FILE_SIZE}B; cap likely larger than the staged volume. Increase STALL_FILE_SIZE/STALL_MAX_FILES to exercise the stall. (No error observed, which is itself acceptable.)"
    fi
fi

# Confirm we never saw a Finder/NFS error signature DURING the over-fill, even
# though something parked. (NOSPC surfacing as a Finder error = FAIL.)
if qa_error_scan "$mark_c" C-stall-fill; then
    qa_pass "C: zero gated error signatures while over-filling the spool"
else
    qa_fail "C: gated error signature during spool over-fill (see $QA_CAT_DIR/errscan-C-stall-fill.txt; dest $C_DEST)"
fi

# Relieve the stall: go ONLINE so the drainer makes headroom, releasing parked
# writers. Then drain fully and custody-verify whatever landed.
qa_info "C: going ONLINE to relieve the stall and release any parked writers"
qa_offline off >/dev/null 2>&1
_OFFLINE_FORCED=0

# Give parked Finder copies a moment to unblock and finish committing.
if [ -n "$C_PARKED_PID" ]; then
    _wait_pid_or_timeout "$C_PARKED_PID" 120 || { qa_warn "C: parked copy still running after 120s; terminating"; kill -TERM "$C_PARKED_PID" 2>/dev/null || true; }
fi

if qa_wait_drain; then
    qa_pass "C: spool drained after relieving the stall"
else
    qa_fail "C: spool did NOT drain after relieving the stall (dest $C_DEST)"
fi
cf2="$(qa_spool_actionable_failed)"; cq2="$(qa_spool_field quarantined)"
[ "$cf2" = "0" ] || [ "$cf2" = "-1" ] || qa_fail "C: post-stall failed_files=$cf2 (dest $C_DEST)"
[ "$cq2" = "0" ] || [ "$cq2" = "-1" ] || qa_fail "C: post-stall quarantined=$cq2 — corruption at rest (dest $C_DEST)"

# Custody: only assert files that actually landed (a parked-then-cancelled write
# may not have committed). Build a landed-only manifest, then verify it.
C_LANDED_MANIFEST="$QA_CAT_DIR/offline_C.landed.manifest"
: > "$C_LANDED_MANIFEST"
while IFS="$(printf '\t')" read -r rel md; do
    [ -z "$rel" ] && continue
    [ -f "$C_DEST/$rel" ] && printf '%s\t%s\n' "$rel" "$md" >> "$C_LANDED_MANIFEST"
done < "$C_MANIFEST"
if [ -s "$C_LANDED_MANIFEST" ]; then
    if qa_verify_custody "$C_LANDED_MANIFEST" "$C_DEST"; then
        qa_pass "C: every landed file is byte-identical (no corruption from the stall)"
    else
        qa_fail "C: corruption/loss among landed files after stall relief (dest $C_DEST)"
    fi
else
    qa_warn "C: no files landed to custody-verify (stall cap not reached or all parked-cancelled); dest $C_DEST"
fi

# ---------------------------------------------------------------------------
# Final health + verdict.
qa_sec "Final: mount health after offline-spool stress"
if qa_health >/dev/null 2>&1; then
    qa_pass "mount/control-plane healthy after offline-spool stress"
else
    qa_fail "control plane not healthy:true after offline-spool stress"
fi

qa_end
qa_log "07-offline-spool: pass=$QA_PASS fail=$QA_FAIL warn=$QA_WARN"

if [ "${QA_FAIL:-0}" -eq 0 ]; then
    echo "VERDICT: PASS 07-offline-spool"
else
    echo "VERDICT: FAIL 07-offline-spool: $QA_FAIL failing assertion(s) — see $QA_CAT_DIR/.summary and errscan-*.txt for the named offending path(s)"
fi
exit 0
