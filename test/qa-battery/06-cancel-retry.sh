#!/usr/bin/env bash
# ===========================================================================
# 06-cancel-retry.sh — INTERRUPTION RESILIENCE (cancel / retry / re-copy)
#
# Part of the JuiceMount REAL-FINDER RELEASE TESTING BATTERY. Every write is a
# REAL macOS Finder operation via osascript (NOT cp/dd/ditto — those false-green
# by skipping the NFS LOOKUP/CREATE/SETATTR/WRITE path + ._AppleDouble sidecars
# + xattr forks that users actually hit). md5 is used ONLY to verify integrity
# end-to-end after the fact, never to drive a copy.
#
# This category exercises what happens when a copy does NOT run cleanly to
# completion the first time, then is retried / repeated:
#
#   (a) CANCEL / ABANDON MID-FLIGHT + CLEAN RETRY:
#         Start a large folder Finder duplicate, wait until it is genuinely
#         IN-FLIGHT (dest partial bytes observed GROWING), then cancel it the way
#         a user does — Cmd-. / Escape sent to Finder's copy progress sheet via
#         System Events (qa_finder_cancel_copy). A `tell Finder to duplicate`
#         runs the copy OUT-OF-PROCESS inside Finder, so killing the osascript
#         driver does NOT stop it — only the progress-sheet cancel does. We
#         repeat a few times. Verify the volume stays CONSISTENT — no stranded
#         handle (FromHandle STALE), no purging phantom, no 100070/100060, mount
#         still healthy — then a CLEAN RETRY of the SAME source SUCCEEDS with
#         FULL chain of custody (every file md5 == source after the spool drains).
#         HONESTY NOTE: when System Events cannot drive the cancel (no
#         Accessibility grant / no progress UI), the helper reports DEGRADED and
#         we relabel that attempt as an interrupted/abandoned copy (the osascript
#         driver was abandoned) rather than overclaiming a user-style cancel; the
#         consistency + clean-retry gates still apply to that abandoned copy.
#
#   (b) BACK-TO-BACK COPY TO THE SAME DEST NAME after `rm -rf` (delete-lag -48):
#         Copy a tree to dest leaf NAME, wait it drains, `rm -rf` that landed
#         dir on the mount, then IMMEDIATELY re-copy the SAME source to the SAME
#         leaf NAME. This is the directory delete-lag scenario. Because the
#         delete here is a filesystem `rm -rf` (NOT a Finder "Move to Trash"),
#         the FIX-FORWARD TARGET is ZERO -48 / 'already an item': any -48 here is
#         a FAIL (named), and any STALE/phantom is a FAIL. (The known ~1 -48
#         delete-lag warn-edge belongs to the Finder-Trash recopy in 02, not to
#         this rm -rf path — here we drive it to 0.)
#
#   (c) COPY, DELETE HALF, RE-COPY:
#         Copy a tree, wait it drains, `rm -rf` HALF the landed files, then
#         re-copy the SAME source over the top (with replacing). The merged
#         result must contain every file with the correct md5 — the deleted half
#         restored, the surviving half intact — zero loss, zero corruption, and
#         no -48 / STALE / phantom.
#
# Chain of custody (every file): Finder write -> NFS handler (CREATE/WRITE) ->
# write spool -> drainer.drainOne io.CopyBuffer to FUSE -> SHA re-read at-rest
# verify -> JuiceFS backend -> readback md5 == source. We ALWAYS qa_wait_drain
# (pending_files==0 AND in_progress==0) BEFORE qa_verify_custody, because
# verifying a non-drained spool reads cache, not the at-rest backend copy.
#
# Error gate (principle #3): every test is bracketed qa_log_mark .. qa_error_scan;
# ANY FromHandle STALE / purging phantom / 100070 / 100060 / -48 / -36 / -5000 /
# permission signature in the window FAILS the gate, naming the offending path
# pulled from the dumped errscan-*.txt.
#
# NON-DESTRUCTIVE: all sources are staged off-mount under $QA_STAGE; all dests
# live under a UNIQUE timestamped leaf in $QA_DEST_ROOT/QA_<epoch>_<pid>_<rand>.
# The EXIT trap + qa_cleanup remove ONLY this run's staged /tmp data and this
# run's tagged dest subtree (guarded), and restore online state. We NEVER touch
# the user's real folders.
#
# bash 3.2-safe (macOS default). NO GNU timeout — bounded probes via qa_timeout
# (perl alarm). Control plane: http://127.0.0.1:11050 (/spool /health /offline).
#
# This script is AUTHORED here and run LATER by the orchestrator against the live
# mount — it is NOT executed during authoring. It exits 0 always; pass/fail is the
# .summary + the final single-line VERDICT.
# ===========================================================================

set -uo pipefail

source "$(dirname "$0")/lib.sh"

QA_CAT="06-cancel-retry"

# Size knobs (env-overridable). The cancel case needs a payload big enough that a
# ~1.5s grace can't finish it (else qa_finder_cancel_copy returns 1 = too fast,
# and we GROW the payload rather than pass trivially).
: "${C06_CANCEL_DIRS:=6}"
: "${C06_CANCEL_FPD:=8}"
: "${C06_CANCEL_SIZE:=33554432}"   # 32 MiB/file -> ~1.5 GiB tree by default
: "${C06_CANCEL_REPEATS:=5}"       # how many cancel attempts (spec: ~5x)
: "${C06_CANCEL_GRACE_MS:=1500}"   # kill ~1.5s in
: "${C06_CANCEL_GROW:=2}"          # payload multiplier when a cancel finished too fast
: "${C06_CANCEL_MAX_GROW:=3}"      # bound the grow retries

: "${C06_SMALL_DIRS:=4}"
: "${C06_SMALL_FPD:=10}"
: "${C06_SMALL_SIZE:=1048576}"     # 1 MiB/file for the b/c cases (fast, real ._ forks)

# Reserve a dedicated unique dest root for this run; the EXIT trap cleans it.
C06_DEST="$QA_DEST_ROOT/$(qa_unique_tag QA)"

# ---------------------------------------------------------------------------
# EXIT trap: restore online state + remove ONLY our own staged + dest data.
_c06_cleanup() {
    # Always try to leave the mount ONLINE regardless of where we failed.
    qa_offline off >/dev/null 2>&1 || true
    # Remove this run's dedicated dest leaf (guarded: must be under the battery root).
    case "$C06_DEST" in
        "$MOUNT"/JM_RELEASE_BATTERY/*) rm -rf "$C06_DEST" 2>/dev/null || true ;;
    esac
    # qa_cleanup removes $QA_STAGE + any QA_*_$$_* dests (guarded), best-effort.
    qa_cleanup
}
trap _c06_cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
qa_begin "$QA_CAT"

if ! qa_preflight; then
    qa_log "preflight failed (see [FAIL] lines above)"
    qa_end || true
    echo "VERDICT: FAIL $QA_CAT: preflight failed (control plane / mount / dest guard)"
    exit 0
fi

mkdir -p "$C06_DEST" 2>/dev/null

# Track the first offending path so the final VERDICT can name it.
C06_FIRST_FAIL=""
_note_fail() {
    # _note_fail MSG — qa_fail + remember the first offender for the VERDICT line.
    qa_fail "$1"
    [ -z "$C06_FIRST_FAIL" ] && C06_FIRST_FAIL="$1"
}

# Pull the first attributed path out of a dumped errscan file (each STALE/phantom
# line logs path=...). Echoes a short "<signature> @ <path-or-line>" for the VERDICT.
_first_errscan_path() {
    local f="$1"
    [ -f "$f" ] || { echo "(no errscan artifact)"; return; }
    local line
    line="$(grep -m1 -E 'FromHandle STALE|purging phantom|100070|100060|already an item|error -48|error -36|ioErr|-5000|permission' "$f" 2>/dev/null)"
    [ -z "$line" ] && { echo "(errscan artifact empty)"; return; }
    # Prefer the path= token if present; else echo the whole (trimmed) line.
    case "$line" in
        *path=*) echo "${line#*path=}" | awk '{print $1}' ;;
        *)       printf '%s' "$line" | sed 's/^[0-9]*://; s/^ *//' | cut -c1-160 ;;
    esac
}

# ===========================================================================
# CASE (a) — CANCEL MID-FLIGHT, then CLEAN RETRY with full custody.
# ===========================================================================
qa_sec "CASE (a): cancel/abandon mid-flight x${C06_CANCEL_REPEATS}, then clean retry"

A_SRC_ROOT="$QA_STAGE/cancel_src"
A_DEST="$C06_DEST/cancel"
mkdir -p "$A_DEST" 2>/dev/null

# Stage a large tree (manifest = relpath<TAB>md5). qa_stage_tree names its leaf
# dir = basename of the staged ROOT, so the landed copy will be at
# "$A_DEST/$(basename A_SRC_ROOT)".
a_dirs="$C06_CANCEL_DIRS"; a_fpd="$C06_CANCEL_FPD"; a_size="$C06_CANCEL_SIZE"
A_MANIFEST="$(qa_stage_tree "$A_SRC_ROOT" "$a_dirs" "$a_fpd" "$a_size" 1)"
A_LEAF="$(basename "$A_SRC_ROOT")"
qa_info "case-a staged tree: $A_SRC_ROOT (~$(( a_dirs * a_fpd )) files @ ${a_size}B, ._ sidecars on)"

a_cancelled_total=0     # attempts with a REAL Cmd-./Escape progress-sheet cancel
a_abandoned_total=0     # attempts that DEGRADED to an abandoned (driver-killed) copy
a_grow=0
i=1
while [ "$i" -le "$C06_CANCEL_REPEATS" ]; do
    # Each attempt goes into its OWN scratch dest so a cancelled/abandoned partial
    # can't collide with the next attempt's CREATE (we are testing interruption
    # hygiene, not name-clash here). The clean RETRY at the end uses the real A_DEST.
    a_scratch="$A_DEST/attempt$i"
    mkdir -p "$a_scratch" 2>/dev/null
    mark=$(qa_log_mark)

    qa_finder_cancel_copy "$A_SRC_ROOT" "$a_scratch" "$C06_CANCEL_GRACE_MS"
    a_rc=$?

    # a_rc contract (lib.sh qa_finder_cancel_copy):
    #   0 = REAL in-flight cancel (Cmd-. accepted, copy stopped)
    #   1 = finished before we caught it mid-flight (too fast) -> GROW + retry
    #   2 = DEGRADED: in-flight but System Events couldn't drive the cancel; the
    #       osascript driver was abandoned (interrupted copy, NOT a user cancel)
    a_how="cancel"
    if [ "$a_rc" -eq 1 ]; then
        # Payload too small to exercise mid-copy interruption. GROW and retry this
        # attempt instead of passing trivially.
        if [ "$a_grow" -lt "$C06_CANCEL_MAX_GROW" ]; then
            a_grow=$((a_grow+1))
            a_dirs=$(( a_dirs * C06_CANCEL_GROW ))
            qa_warn "case-a attempt $i finished before we could interrupt it mid-flight; growing payload (dirs -> $a_dirs) and retrying"
            rm -rf "$A_SRC_ROOT" "$a_scratch" 2>/dev/null
            A_MANIFEST="$(qa_stage_tree "$A_SRC_ROOT" "$a_dirs" "$a_fpd" "$a_size" 1)"
            A_LEAF="$(basename "$A_SRC_ROOT")"
            continue   # re-run THIS i with the bigger payload
        else
            qa_warn "case-a attempt $i still finished before we could interrupt it after $a_grow grows (mount is very fast); asserting hygiene on the completed copy only"
            a_how="completed"
        fi
    elif [ "$a_rc" -eq 2 ]; then
        a_abandoned_total=$((a_abandoned_total+1))
        a_how="abandoned"
        qa_warn "case-a attempt $i: System Events could not drive Finder's progress-sheet cancel (no Accessibility grant / no progress UI) — abandoned the in-flight copy instead (interrupted copy, NOT a user cancel)"
    else
        a_cancelled_total=$((a_cancelled_total+1))
        a_how="cancel"
        qa_info "case-a attempt $i: REAL mid-flight cancel (Cmd-./Escape) accepted; copy stopped"
    fi

    # Let the spool settle whatever the interrupted copy already pushed (a partial
    # is allowed; what is NOT allowed is a stranded handle / phantom / stale).
    qa_wait_drain || qa_warn "case-a attempt $i: spool did not fully drain post-$a_how (residual logged)"

    # Mount must still be HEALTHY right after a mid-flight interruption.
    if qa_health >/dev/null 2>&1; then
        qa_pass "case-a attempt $i: mount healthy after $a_how"
    else
        _note_fail "case-a attempt $i: mount UNHEALTHY after $a_how ($a_scratch)"
    fi

    # Scan the interruption window for stranded-handle signatures. ZERO tolerance
    # here: a cancel/abandon must NOT leak FromHandle STALE / purging phantom /
    # 100070 / 100060.
    scan="$(qa_error_scan "$mark" "cancel-a$i")"
    qa_info "case-a attempt $i ($a_how) $scan"
    if printf '%s' "$scan" | grep -q 'STALE=0 PHANTOM=0 E100070=0 E100060=0'; then
        qa_pass "case-a attempt $i: no stranded-handle signatures in $a_how window"
    else
        ep="$(_first_errscan_path "$QA_CAT_DIR/errscan-cancel-a$i.txt")"
        _note_fail "case-a attempt $i: stranded-handle signature after $a_how ($scan) offending=$ep"
    fi

    # Clean up the interrupted partial so it can't pollute custody / the next attempt.
    rm -rf "$a_scratch" 2>/dev/null
    i=$((i+1))
done

if [ "$a_cancelled_total" -ge 1 ]; then
    qa_pass "case-a: $a_cancelled_total/$C06_CANCEL_REPEATS copies REAL-cancelled mid-flight (Cmd-./Escape on Finder's progress sheet)"
elif [ "$a_abandoned_total" -ge 1 ]; then
    qa_warn "case-a: 0 real progress-sheet cancels; $a_abandoned_total/$C06_CANCEL_REPEATS were interrupted/abandoned mid-flight (System Events couldn't drive the cancel — grant Accessibility to exercise the true Cmd-. cancel). Interruption hygiene was still asserted on the abandoned copies."
else
    qa_warn "case-a: 0 copies were interrupted mid-flight (mount too fast even after grow) — interruption hygiene asserted on completed copies only"
fi

# --- CLEAN RETRY of the SAME source: must SUCCEED with FULL custody ---
qa_sec "CASE (a): clean retry after cancellations"
mark=$(qa_log_mark)
out="$(qa_finder_copy "$A_SRC_ROOT" "$A_DEST")"
rc=$?
if [ "$rc" -ne 0 ] || ! qa_finder_result_clean "$out"; then
    _note_fail "case-a retry: Finder duplicate error to $A_DEST: $out"
else
    qa_pass "case-a retry: Finder duplicate accepted ($A_DEST/$A_LEAF)"
fi

if qa_wait_drain; then
    qa_pass "case-a retry: spool fully drained"
else
    _note_fail "case-a retry: spool did not drain ($A_DEST/$A_LEAF)"
fi

if qa_verify_custody "$A_MANIFEST" "$A_DEST/$A_LEAF"; then
    qa_pass "case-a retry: full chain of custody (zero loss, zero corruption)"
else
    _note_fail "case-a retry: custody failure under $A_DEST/$A_LEAF"
fi

scan="$(qa_error_scan "$mark" "retry-a")"
qa_info "case-a retry $scan"
if printf '%s' "$scan" | grep -q 'STALE=0 PHANTOM=0 E100070=0 E100060=0 E48=0 E36=0 E5000=0 PERM=0'; then
    qa_pass "case-a retry: clean error window (all signatures zero)"
else
    ep="$(_first_errscan_path "$QA_CAT_DIR/errscan-retry-a.txt")"
    _note_fail "case-a retry: error signature in window ($scan) offending=$ep"
fi

# ===========================================================================
# CASE (b) — BACK-TO-BACK COPY TO THE SAME DEST NAME after rm -rf.
#            FIX-FORWARD TARGET: ZERO -48 (delete-lag). Any -48 here = FAIL.
# ===========================================================================
qa_sec "CASE (b): rm -rf then immediate re-copy to SAME dest name (-48 target=0)"

B_SRC_ROOT="$QA_STAGE/relay_src"
B_DEST="$C06_DEST/samedest"      # the FIXED dest-parent; leaf NAME is reused
mkdir -p "$B_DEST" 2>/dev/null
B_MANIFEST="$(qa_stage_tree "$B_SRC_ROOT" "$C06_SMALL_DIRS" "$C06_SMALL_FPD" "$C06_SMALL_SIZE" 1)"
B_LEAF="$(basename "$B_SRC_ROOT")"   # the dest leaf NAME we will re-create
B_LANDED="$B_DEST/$B_LEAF"

mark=$(qa_log_mark)

# First copy.
out="$(qa_finder_copy "$B_SRC_ROOT" "$B_DEST")"
rc=$?
if [ "$rc" -ne 0 ] || ! qa_finder_result_clean "$out"; then
    _note_fail "case-b copy#1: Finder duplicate error to $B_DEST: $out"
else
    qa_pass "case-b copy#1: Finder duplicate accepted ($B_LANDED)"
fi
if qa_wait_drain; then qa_pass "case-b copy#1: spool drained"; else _note_fail "case-b copy#1: spool did not drain ($B_LANDED)"; fi
if qa_verify_custody "$B_MANIFEST" "$B_LANDED"; then
    qa_pass "case-b copy#1: full custody"
else
    _note_fail "case-b copy#1: custody failure under $B_LANDED"
fi

# rm -rf the landed dir on the mount (filesystem delete, NOT Finder Trash), then
# IMMEDIATELY re-copy the SAME source to the SAME leaf name. This is the
# directory delete-lag scenario; with rm -rf the fix-forward target is ZERO -48.
if [ -e "$B_LANDED" ]; then
    rm -rf "$B_LANDED" 2>/dev/null
fi
if [ -e "$B_LANDED" ]; then
    _note_fail "case-b: rm -rf did not remove $B_LANDED (still present pre-recopy)"
else
    qa_pass "case-b: rm -rf removed landed dir ($B_LANDED)"
fi

# Immediate re-copy to the SAME dest leaf name.
out2="$(qa_finder_copy "$B_SRC_ROOT" "$B_DEST")"
rc2=$?
# Inspect the Finder RESULT string itself for -48 first (the synchronous edge).
if printf '%s' "$out2" | grep -qiE 'error -48|already an item'; then
    _note_fail "case-b recopy: -48 'already an item' on rm -rf back-to-back to SAME dest ($B_LANDED) — fix-forward target is 0; Finder result: $out2"
elif [ "$rc2" -ne 0 ] || ! qa_finder_result_clean "$out2"; then
    _note_fail "case-b recopy: Finder duplicate error to $B_DEST: $out2"
else
    qa_pass "case-b recopy: Finder duplicate accepted with no -48 ($B_LANDED)"
fi

if qa_wait_drain; then qa_pass "case-b recopy: spool drained"; else _note_fail "case-b recopy: spool did not drain ($B_LANDED)"; fi
if qa_verify_custody "$B_MANIFEST" "$B_LANDED"; then
    qa_pass "case-b recopy: full custody after rm -rf + re-copy"
else
    _note_fail "case-b recopy: custody failure under $B_LANDED"
fi

# Error window: -48 target is ZERO here (rm -rf, not Finder Trash). STALE/phantom
# /100070/100060 also zero. Any of them = FAIL with the named offender.
scan="$(qa_error_scan "$mark" "samedest-b")"
qa_info "case-b $scan"
if printf '%s' "$scan" | grep -q 'STALE=0 PHANTOM=0 E100070=0 E100060=0 E48=0 E36=0 E5000=0 PERM=0'; then
    qa_pass "case-b: clean error window (zero -48 / STALE / phantom — delete-lag driven to 0)"
else
    ep="$(_first_errscan_path "$QA_CAT_DIR/errscan-samedest-b.txt")"
    # Name the -48 specifically since it's the headline assertion for this case.
    if printf '%s' "$scan" | grep -qE 'E48=[1-9]'; then
        _note_fail "case-b: -48 'already an item' in log window ($scan) — directory delete-lag NOT fixed-forward; offending=$ep"
    else
        _note_fail "case-b: error signature in window ($scan) offending=$ep"
    fi
fi

# ===========================================================================
# CASE (c) — COPY, DELETE HALF, RE-COPY (merge restore).
# ===========================================================================
qa_sec "CASE (c): copy, delete half, re-copy (merge must restore + preserve)"

C_SRC_ROOT="$QA_STAGE/half_src"
C_DEST="$C06_DEST/half"
mkdir -p "$C_DEST" 2>/dev/null
C_MANIFEST="$(qa_stage_tree "$C_SRC_ROOT" "$C06_SMALL_DIRS" "$C06_SMALL_FPD" "$C06_SMALL_SIZE" 1)"
C_LEAF="$(basename "$C_SRC_ROOT")"
C_LANDED="$C_DEST/$C_LEAF"

mark=$(qa_log_mark)

# Initial full copy.
out="$(qa_finder_copy "$C_SRC_ROOT" "$C_DEST")"
rc=$?
if [ "$rc" -ne 0 ] || ! qa_finder_result_clean "$out"; then
    _note_fail "case-c copy#1: Finder duplicate error to $C_DEST: $out"
else
    qa_pass "case-c copy#1: Finder duplicate accepted ($C_LANDED)"
fi
if qa_wait_drain; then qa_pass "case-c copy#1: spool drained"; else _note_fail "case-c copy#1: spool did not drain ($C_LANDED)"; fi
if qa_verify_custody "$C_MANIFEST" "$C_LANDED"; then
    qa_pass "case-c copy#1: full custody"
else
    _note_fail "case-c copy#1: custody failure under $C_LANDED"
fi

# Delete HALF of the landed DATA files (rm -rf each). Walk the manifest, delete
# every other entry. Keep ._ sidecars alone — we measure DATA custody.
c_total=0; c_deleted=0
while IFS="$(printf '\t')" read -r rel _md; do
    [ -z "$rel" ] && continue
    c_total=$((c_total+1))
    if [ $(( c_total % 2 )) -eq 0 ]; then
        rm -f "$C_LANDED/$rel" 2>/dev/null
        c_deleted=$((c_deleted+1))
    fi
done < "$C_MANIFEST"
qa_info "case-c: deleted $c_deleted/$c_total landed data files (every other) before re-copy"

# Confirm at least one deletion actually took (else the test is a no-op).
if [ "$c_deleted" -ge 1 ]; then
    qa_pass "case-c: half-delete removed $c_deleted files under $C_LANDED"
else
    _note_fail "case-c: half-delete removed nothing under $C_LANDED (test would be a no-op)"
fi

# Re-copy the SAME source over the top. Use REPLACING so the surviving half is
# overwritten in place and the deleted half is restored (user re-drags + Replace).
out2="$(qa_finder_copy_replacing "$C_SRC_ROOT" "$C_DEST")"
rc2=$?
if [ "$rc2" -ne 0 ] || ! qa_finder_result_clean "$out2"; then
    _note_fail "case-c recopy: Finder duplicate(replacing) error to $C_DEST: $out2"
else
    qa_pass "case-c recopy: Finder duplicate(replacing) accepted ($C_LANDED)"
fi
if qa_wait_drain; then qa_pass "case-c recopy: spool drained"; else _note_fail "case-c recopy: spool did not drain ($C_LANDED)"; fi

# Full custody over the MERGED result: deleted half restored, surviving half intact.
if qa_verify_custody "$C_MANIFEST" "$C_LANDED"; then
    qa_pass "case-c recopy: full custody after delete-half + re-copy (restore complete)"
else
    _note_fail "case-c recopy: custody failure under $C_LANDED (deleted half not restored or surviving half corrupted)"
fi

scan="$(qa_error_scan "$mark" "half-c")"
qa_info "case-c $scan"
if printf '%s' "$scan" | grep -q 'STALE=0 PHANTOM=0 E100070=0 E100060=0 E48=0 E36=0 E5000=0 PERM=0'; then
    qa_pass "case-c: clean error window (all signatures zero)"
else
    ep="$(_first_errscan_path "$QA_CAT_DIR/errscan-half-c.txt")"
    _note_fail "case-c: error signature in window ($scan) offending=$ep"
fi

# ===========================================================================
# VERDICT
# ===========================================================================
qa_offline off >/dev/null 2>&1 || true   # belt-and-suspenders: ensure online before verdict

qa_log "==> $QA_CAT totals: pass=$QA_PASS fail=$QA_FAIL warn=$QA_WARN"

if [ "$QA_FAIL" -eq 0 ]; then
    echo "VERDICT: PASS $QA_CAT"
else
    if [ -n "$C06_FIRST_FAIL" ]; then
        echo "VERDICT: FAIL $QA_CAT: $C06_FIRST_FAIL"
    else
        echo "VERDICT: FAIL $QA_CAT: $QA_FAIL failing assertion(s) (see [FAIL] lines)"
    fi
fi

# qa_end writes .summary, prints totals, runs qa_cleanup; the EXIT trap also runs.
qa_end || true
exit 0
