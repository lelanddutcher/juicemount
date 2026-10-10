#!/usr/bin/env bash
# ===========================================================================
# 03-deep-wide-trees.sh — JuiceMount REAL-FINDER RELEASE BATTERY, category 03
#
# SCOPE: Expansive tree shapes that stress LOOKUP/CREATE/readdir at the edges:
#   (A) DEEP   — a single chain 14 levels deep (nested folders, file at each
#                level) — exercises deep recursive Finder duplicate + deep
#                readdir/LOOKUP.
#   (B) WIDE   — one folder holding 1200 files (1000+) — exercises a single
#                large readdir / the snappiness gate on a fat directory.
#   (C) MIXED  — 3200 files spread across 64 folders (3000+ across many dirs) —
#                the large-count omnibus; copied with mid-copy navigation probes.
#
# For each shape we drive a REAL Finder `duplicate` of the WHOLE top folder
# (osascript — NOT cp/dd/ditto), wait for the spool to FULLY drain, verify
# total file count + full md5 chain-of-custody, and scan the JuiceMount log for
# the gated user-visible error signatures. For the MIXED shape we additionally
# launch the copy in the background and probe intermediate directories WHILE the
# copy is in flight to prove they stay navigable, recording p99 and worst-case
# listing latency, plus final completeness (count + custody). p99 must stay
# below the snappiness budget and no individual sample may cross the separate
# catastrophic-stall ceiling.
#
# PRINCIPLES honored (see battery spec):
#   1. REAL Finder ops only (qa_finder_copy); synthetic generation is confined
#      to OFF-mount staging (qa_stage_file / qa_stage_tree).
#   2. Full chain of custody: qa_wait_drain (MANDATORY) BEFORE qa_verify_custody.
#   3. Zero-random-Finder-error gate: qa_log_mark … qa_error_scan $mark, FAIL on
#      any non-whitelisted signature, naming the offending path from the
#      dumped errscan-*.txt. (No ._ sidecars are staged here, so STALE has no
#      known-edge budget in this category — any STALE = FAIL.)
#   4. Snappiness: p99 must be < $QA_SNAPPY_MS and every individual listing
#      must be < $QA_HARD_STALL_MS.
#   5. Non-destructive: stage off-mount under $QA_STAGE; copy into a UNIQUE
#      timestamped dest under $QA_DEST_ROOT; EXIT-trap cleans up only ours.
#   6. Bounded probes via qa_timeout / perl alarm — no GNU timeout.
#
# Exits 0 always; pass/fail is the .summary + the final single-line VERDICT.
# Run later by the orchestrator, NOT during authoring.
# ===========================================================================

set -uo pipefail

source "$(dirname "$0")/lib.sh"

# --- size / shape knobs (env-overridable so the orchestrator can scale) -----
: "${QA_DEEP_LEVELS:=14}"          # nesting depth for the DEEP chain (>=12)
: "${QA_DEEP_FILE_SIZE:=65536}"    # bytes per file in the DEEP chain (64 KiB)
: "${QA_WIDE_FILES:=1200}"         # files in the single WIDE folder (>=1000)
: "${QA_WIDE_FILE_SIZE:=16384}"    # bytes per file in the WIDE folder (16 KiB)
: "${QA_MIXED_DIRS:=64}"           # folders in the MIXED tree
: "${QA_MIXED_FPD:=50}"            # files per folder (64*50 = 3200 >= 3000)
: "${QA_MIXED_FILE_SIZE:=8192}"    # bytes per file in the MIXED tree (8 KiB)
: "${QA_DEEPWIDE_BOUND:=600}"      # per-Finder-copy bound (s)

QA_CAT_LABEL="03-deep-wide-trees"

# --- unique dest + staging roots for THIS run -------------------------------
DEST=""        # set after preflight
STAGE_DIR="$QA_STAGE/deepwide-$$"

# --- EXIT trap: restore network state, remove our dest + our staging --------
# Runs on any exit path. Best-effort; never blocks. qa_end already calls
# qa_cleanup (which removes $MOUNT/JM_RELEASE_BATTERY/QA_*_$$_* and $QA_STAGE),
# but we add an explicit belt-and-suspenders teardown of THIS script's own dest
# and local staging, and we force the offline toggle back OFF in case a case
# left it on (this category never goes offline, but the guard is cheap).
_cleanup_03() {
    # Never leave the mount stuck offline.
    qa_offline off >/dev/null 2>&1 || true
    # Remove our unique dest subtree (guard: must be under the battery root).
    if [ -n "${DEST:-}" ]; then
        case "$DEST" in
            "$MOUNT"/JM_RELEASE_BATTERY/QA_*)
                qa_timeout 120 rm -rf "$DEST" 2>/dev/null || true
                ;;
        esac
    fi
    # Remove our local staging (guard: must be under $QA_STAGE).
    case "$STAGE_DIR" in
        "$QA_STAGE"/*) rm -rf "$STAGE_DIR" 2>/dev/null || true ;;
    esac
}
trap _cleanup_03 EXIT

# ---------------------------------------------------------------------------
# Helper (LOCAL to this script — NOT in lib.sh contract): stage a DEEP nested
# chain off-mount and emit a manifest in the exact TSV (relpath<TAB>md5) format
# qa_verify_custody consumes. lib.sh's qa_stage_tree only builds a FLAT
# dir1..N/file1..M shape, so a >=12-level nested chain has no contract stager;
# we build it here using qa_stage_file (which is contract, and echoes md5) so
# the bytes still flow through the sanctioned staging path. Echoes the manifest
# path. (Flagged in the return notes for possible promotion into lib.sh.)
# ---------------------------------------------------------------------------
_stage_deep_chain() {
    local root="$1" levels="$2" size="$3"
    local manifest="$QA_CAT_DIR/$(basename "$root").manifest"
    : > "$manifest"
    local i rel path md cur="$root"
    rel=""
    i=1
    while [ "$i" -le "$levels" ]; do
        # Build nested relpath level1/level2/.../levelI
        if [ -z "$rel" ]; then rel="level$i"; else rel="$rel/level$i"; fi
        # One data file at this level.
        local frel="$rel/depth$i.dat"
        path="$root/$frel"
        md="$(qa_stage_file "$path" "$size")"
        printf '%s\t%s\n' "$frel" "$md" >> "$manifest"
        i=$((i+1))
    done
    echo "$manifest"
}

# Count files in a manifest (lines).
_manifest_count() { wc -l < "$1" | tr -d ' '; }

# Count regular files actually landed under a dir (real on-mount file count).
_landed_count() {
    # bounded so a wedged readdir can't hang us
    qa_timeout 120 find "$1" -type f ! -name '._*' 2>/dev/null | wc -l | tr -d ' '
}

# ===========================================================================
# MAIN
# ===========================================================================
qa_begin "$QA_CAT_LABEL"

if ! qa_preflight; then
    qa_log "preflight failed — skipping $QA_CAT_LABEL"
    qa_end
    echo "VERDICT: FAIL $QA_CAT_LABEL: preflight failed (control plane / mount / dest guard) — see log above"
    exit 0
fi

DEST="$QA_DEST_ROOT/$(qa_unique_tag QA)"
mkdir -p "$DEST" 2>/dev/null
mkdir -p "$STAGE_DIR" 2>/dev/null
qa_info "dest=$DEST stage=$STAGE_DIR"
qa_info "knobs: DEEP_LEVELS=$QA_DEEP_LEVELS WIDE_FILES=$QA_WIDE_FILES MIXED=${QA_MIXED_DIRS}x${QA_MIXED_FPD}"

# track a VERDICT reason for the FIRST hard failure (so the final line names it)
FAIL_REASON=""
_note_fail() { [ -z "$FAIL_REASON" ] && FAIL_REASON="$1"; }

# ---------------------------------------------------------------------------
# CASE A — DEEP chain (>=12 levels), real Finder duplicate of the whole top dir
# ---------------------------------------------------------------------------
qa_sec "CASE A: DEEP chain ($QA_DEEP_LEVELS levels)"
A_SRC="$STAGE_DIR/deep"
A_MAN="$(_stage_deep_chain "$A_SRC" "$QA_DEEP_LEVELS" "$QA_DEEP_FILE_SIZE")"
A_EXPECT="$(_manifest_count "$A_MAN")"
qa_info "[A] staged $A_EXPECT files across $QA_DEEP_LEVELS nested levels: $A_SRC"

A_DESTP="$DEST/A_deep"; mkdir -p "$A_DESTP" 2>/dev/null
A_LEAF="$A_DESTP/$(basename "$A_SRC")"     # Finder duplicates SRC *into* DESTP
mark=$(qa_log_mark)
qa_log "[A] Finder duplicate (deep) -> $A_DESTP"
A_OUT="$(qa_finder_copy "$A_SRC" "$A_DESTP" "$QA_DEEPWIDE_BOUND")"; A_RC=$?
if [ "$A_RC" -ne 0 ]; then
    qa_fail "[A] Finder duplicate rc=$A_RC for $A_SRC -> $A_DESTP :: $A_OUT"
    _note_fail "deep Finder duplicate failed ($A_SRC -> $A_DESTP): $A_OUT"
else
    qa_pass "[A] Finder duplicate returned (rc=0)"
fi
if ! qa_finder_result_clean "$A_OUT"; then
    qa_fail "[A] Finder result carried a gated error for $A_LEAF :: $A_OUT"
    _note_fail "deep Finder result error at $A_LEAF: $A_OUT"
fi

qa_log "[A] waiting for spool to drain (MANDATORY pre-verify gate)"
# Payload-scaled drain ceiling: the full deep tree must drain before custody.
A_CEIL="$(qa_drain_ceiling $(( A_EXPECT * QA_DEEP_FILE_SIZE )) $(( A_EXPECT * 2 )))"
if qa_wait_drain "$A_CEIL"; then qa_pass "[A] spool drained"; else
    qa_fail "[A] spool did NOT drain for $A_LEAF"; _note_fail "deep spool drain timeout ($A_LEAF)"
fi

A_LANDED="$(_landed_count "$A_LEAF")"
if [ "$A_LANDED" = "$A_EXPECT" ]; then
    qa_pass "[A] file count complete: landed=$A_LANDED expected=$A_EXPECT"
else
    qa_fail "[A] file count INCOMPLETE under $A_LEAF: landed=$A_LANDED expected=$A_EXPECT"
    _note_fail "deep count mismatch ($A_LEAF): landed=$A_LANDED expected=$A_EXPECT"
fi

if qa_verify_custody "$A_MAN" "$A_LEAF"; then
    qa_pass "[A] custody clean (zero loss / zero corruption)"
else
    qa_fail "[A] custody FAILED under $A_LEAF"; _note_fail "deep custody failed ($A_LEAF)"
fi

# Snappiness on the deepest path (served from local metadata DB).
A_DEEPEST="$A_LEAF"
i=1; while [ "$i" -le "$QA_DEEP_LEVELS" ]; do A_DEEPEST="$A_DEEPEST/level$i"; i=$((i+1)); done
A_MS="$(qa_dir_listing_ms "$A_DEEPEST")"
if [ "$A_MS" = "-1" ]; then
    qa_fail "[A] deepest-dir listing FAILED (readdir error) for $A_DEEPEST"
    _note_fail "deep readdir failed ($A_DEEPEST)"
elif [ "$A_MS" -lt "$QA_SNAPPY_MS" ]; then
    qa_pass "[A] deepest-dir listing snappy: ${A_MS}ms (<${QA_SNAPPY_MS}ms) $A_DEEPEST"
else
    qa_fail "[A] deepest-dir listing SLOW: ${A_MS}ms (>=${QA_SNAPPY_MS}ms) $A_DEEPEST"
    _note_fail "deep listing slow ${A_MS}ms ($A_DEEPEST)"
fi

A_SCAN="$(qa_error_scan "$mark" A_deep)"
qa_log "[A] $A_SCAN"
if printf '%s' "$A_SCAN" | grep -q 'STALE=0 PHANTOM=0 E100070=0 E100060=0 E48=0 E36=0 E5000=0 PERM=0'; then
    qa_pass "[A] error-scan clean"
else
    qa_fail "[A] error signature(s) in window for $A_LEAF :: $A_SCAN (see $QA_CAT_DIR/errscan-A_deep.txt)"
    _note_fail "deep error signature ($A_SCAN); see $QA_CAT_DIR/errscan-A_deep.txt"
fi

# ---------------------------------------------------------------------------
# CASE B — WIDE folder (1000+ files in ONE dir), real Finder duplicate
# ---------------------------------------------------------------------------
qa_sec "CASE B: WIDE folder ($QA_WIDE_FILES files in one dir)"
B_ROOT="$STAGE_DIR/wide"
# qa_stage_tree builds DIRS x FILES_PER_DIR; use 1 dir x N files => N files in
# one folder (relpath = dir1/fileK.dat). Manifest is dir1/* — we copy the ROOT
# so the single fat dir is exercised as a child of the copied top folder.
B_MAN="$(qa_stage_tree "$B_ROOT" 1 "$QA_WIDE_FILES" "$QA_WIDE_FILE_SIZE")"
B_EXPECT="$(_manifest_count "$B_MAN")"
qa_info "[B] staged $B_EXPECT files in a single folder: $B_ROOT/dir1"

B_DESTP="$DEST/B_wide"; mkdir -p "$B_DESTP" 2>/dev/null
B_LEAF="$B_DESTP/$(basename "$B_ROOT")"
mark=$(qa_log_mark)
qa_log "[B] Finder duplicate (wide) -> $B_DESTP"
B_OUT="$(qa_finder_copy "$B_ROOT" "$B_DESTP" "$QA_DEEPWIDE_BOUND")"; B_RC=$?
if [ "$B_RC" -ne 0 ]; then
    qa_fail "[B] Finder duplicate rc=$B_RC for $B_ROOT -> $B_DESTP :: $B_OUT"
    _note_fail "wide Finder duplicate failed ($B_ROOT -> $B_DESTP): $B_OUT"
else
    qa_pass "[B] Finder duplicate returned (rc=0)"
fi
if ! qa_finder_result_clean "$B_OUT"; then
    qa_fail "[B] Finder result carried a gated error for $B_LEAF :: $B_OUT"
    _note_fail "wide Finder result error at $B_LEAF: $B_OUT"
fi

qa_log "[B] waiting for spool to drain"
# Payload-scaled drain ceiling for the full wide folder.
B_CEIL="$(qa_drain_ceiling $(( B_EXPECT * QA_WIDE_FILE_SIZE )) $(( B_EXPECT * 2 )))"
if qa_wait_drain "$B_CEIL"; then qa_pass "[B] spool drained"; else
    qa_fail "[B] spool did NOT drain for $B_LEAF"; _note_fail "wide spool drain timeout ($B_LEAF)"
fi

B_LANDED="$(_landed_count "$B_LEAF")"
if [ "$B_LANDED" = "$B_EXPECT" ]; then
    qa_pass "[B] file count complete: landed=$B_LANDED expected=$B_EXPECT"
else
    qa_fail "[B] file count INCOMPLETE under $B_LEAF: landed=$B_LANDED expected=$B_EXPECT"
    _note_fail "wide count mismatch ($B_LEAF): landed=$B_LANDED expected=$B_EXPECT"
fi

if qa_verify_custody "$B_MAN" "$B_LEAF"; then
    qa_pass "[B] custody clean (zero loss / zero corruption)"
else
    qa_fail "[B] custody FAILED under $B_LEAF"; _note_fail "wide custody failed ($B_LEAF)"
fi

# Snappiness on the fat single directory (the readdir that would spin).
B_FATDIR="$B_LEAF/dir1"
B_MS="$(qa_dir_listing_ms "$B_FATDIR")"
if [ "$B_MS" = "-1" ]; then
    qa_fail "[B] fat-dir listing FAILED (readdir error) for $B_FATDIR"
    _note_fail "wide readdir failed ($B_FATDIR)"
elif [ "$B_MS" -lt "$QA_SNAPPY_MS" ]; then
    qa_pass "[B] fat-dir ($B_EXPECT entries) listing snappy: ${B_MS}ms (<${QA_SNAPPY_MS}ms)"
else
    qa_fail "[B] fat-dir listing SLOW: ${B_MS}ms (>=${QA_SNAPPY_MS}ms) $B_FATDIR"
    _note_fail "wide listing slow ${B_MS}ms ($B_FATDIR)"
fi

B_SCAN="$(qa_error_scan "$mark" B_wide)"
qa_log "[B] $B_SCAN"
if printf '%s' "$B_SCAN" | grep -q 'STALE=0 PHANTOM=0 E100070=0 E100060=0 E48=0 E36=0 E5000=0 PERM=0'; then
    qa_pass "[B] error-scan clean"
else
    qa_fail "[B] error signature(s) in window for $B_LEAF :: $B_SCAN (see $QA_CAT_DIR/errscan-B_wide.txt)"
    _note_fail "wide error signature ($B_SCAN); see $QA_CAT_DIR/errscan-B_wide.txt"
fi

# ---------------------------------------------------------------------------
# CASE C — MIXED large-count tree (3000+ files / many folders) with MID-COPY
# navigability probes. Background the Finder copy; while it runs, repeatedly
# list intermediate dirs and record the WORST (max) listing latency observed =
# "min-visible-during-copy" responsiveness. Then drain + count + custody.
# ---------------------------------------------------------------------------
qa_sec "CASE C: MIXED tree (${QA_MIXED_DIRS} dirs x ${QA_MIXED_FPD} files) + mid-copy nav"
C_ROOT="$STAGE_DIR/mixed"
C_MAN="$(qa_stage_tree "$C_ROOT" "$QA_MIXED_DIRS" "$QA_MIXED_FPD" "$QA_MIXED_FILE_SIZE")"
C_EXPECT="$(_manifest_count "$C_MAN")"
qa_info "[C] staged $C_EXPECT files across $QA_MIXED_DIRS folders: $C_ROOT"

C_DESTP="$DEST/C_mixed"; mkdir -p "$C_DESTP" 2>/dev/null
C_LEAF="$C_DESTP/$(basename "$C_ROOT")"
mark=$(qa_log_mark)

# Launch the REAL Finder duplicate in the BACKGROUND so we can navigate during
# it. Capture rc/out to a file the foreground reads after the copy finishes.
C_OUTFILE="$QA_CAT_DIR/C_mixed.finder-out"
: > "$C_OUTFILE"
qa_log "[C] launching background Finder duplicate (mixed) -> $C_DESTP"
( qa_finder_copy "$C_ROOT" "$C_DESTP" "$QA_DEEPWIDE_BOUND" > "$C_OUTFILE" 2>&1; echo "rc=$?" >> "$C_OUTFILE" ) &
C_BGPID=$!

# Mid-copy navigation probes run inside ONE persistent process for the full copy
# window. Finder is persistent too; spawning a fresh low-priority interpreter
# for each sample caused scheduler-preemption tails that the server never saw
# (READDIR max <35ms while the per-sample process reported ~300ms). We still
# time each complete opendir/readdir/closedir syscall window independently.
C_SAMPLES="$QA_CAT_DIR/C-mid-copy-listing-ms.txt"
: > "$C_SAMPLES"
C_PROBE_RAW="$QA_CAT_DIR/C-mid-copy-probes.tsv"
perl -MTime::HiRes=time,sleep -e '
    my ($leaf, $mid, $last, $bound) = @ARGV;
    my $deadline = time() + $bound;
    my $stop = 0;
    $SIG{TERM} = sub { $stop = 1 };
    my @paths = ($leaf . "/dir1", $leaf . "/dir" . $mid,
                 $leaf . "/dir" . $last, $leaf);
    while (!$stop && time() < $deadline) {
        for my $path (@paths) {
            last if $stop;
            next unless -d $path;
            my $t0 = time();
            my $ok = eval {
                local $SIG{ALRM} = sub { die "timeout\n" };
                alarm 30;
                opendir(my $dh, $path) or die "opendir\n";
                1 while readdir($dh);
                closedir($dh) or die "closedir\n";
                alarm 0;
                1;
            };
            alarm 0;
            if ($ok) {
                printf "OK\t%d\n", (time() - $t0) * 1000;
            } else {
                print "ERR\t-1\n";
            }
        }
        sleep 0.25 unless $stop;
    }
' "$C_LEAF" "$(( (QA_MIXED_DIRS+1)/2 ))" "$QA_MIXED_DIRS" "$QA_DEEPWIDE_BOUND" > "$C_PROBE_RAW" &
C_PROBE_PID=$!
wait "$C_BGPID" 2>/dev/null
kill -TERM "$C_PROBE_PID" 2>/dev/null || true
wait "$C_PROBE_PID" 2>/dev/null || true
C_RC="$(awk -F= '/^rc=/{print $2}' "$C_OUTFILE" 2>/dev/null | tail -1)"
[ -z "$C_RC" ] && C_RC=0
C_OUT="$(grep -v '^rc=' "$C_OUTFILE" 2>/dev/null)"
awk -F'\t' '$1=="OK" { print $2 }' "$C_PROBE_RAW" > "$C_SAMPLES"
C_PROBES="$(wc -l < "$C_PROBE_RAW" | tr -d ' ')"
C_PROBE_FAILS="$(awk -F'\t' '$1=="ERR" { n++ } END { print n+0 }' "$C_PROBE_RAW")"
C_NAV_SEEN=0
[ -s "$C_SAMPLES" ] && C_NAV_SEEN=1
C_MAXMS="$(sort -nr "$C_SAMPLES" 2>/dev/null | head -1)"
[ -z "$C_MAXMS" ] && C_MAXMS=-1
C_P99="$(qa_pctile 99 < "$C_SAMPLES")"
C_OVER="$(awk -v b="$QA_SNAPPY_MS" '$1 >= b { n++ } END { print n+0 }' "$C_SAMPLES")"

qa_info "[C] mid-copy nav probes=$C_PROBES probe_fails=$C_PROBE_FAILS p99=${C_P99}ms max_listing=${C_MAXMS}ms over_budget=$C_OVER nav_seen=$C_NAV_SEEN"

# Report MID-COPY navigability as its own gate.
if [ "$C_PROBE_FAILS" -gt 0 ]; then
    qa_fail "[C] $C_PROBE_FAILS mid-copy directory listing(s) FAILED under $C_LEAF (dirs not navigable mid-copy)"
    _note_fail "mixed mid-copy listing failed ($C_PROBE_FAILS under $C_LEAF)"
elif [ "$C_NAV_SEEN" -eq 0 ]; then
    # Copy finished before any intermediate dir was observable — not a failure
    # of navigability, but we couldn't prove it. Warn (don't false-green).
    qa_warn "[C] no intermediate dest dir was observable mid-copy (copy too fast); navigability unproven — consider raising QA_MIXED_DIRS/QA_MIXED_FPD"
elif [ "$C_P99" = "-1" ] || [ "$C_MAXMS" = "-1" ]; then
    qa_warn "[C] mid-copy probes ran but recorded no valid timing"
elif [ "$C_MAXMS" -ge "$QA_HARD_STALL_MS" ]; then
    qa_fail "[C] mid-copy listing HARD STALL: worst ${C_MAXMS}ms (>=${QA_HARD_STALL_MS}ms) under $C_LEAF"
    _note_fail "mixed mid-copy hard stall ${C_MAXMS}ms ($C_LEAF)"
elif [ "$C_P99" -lt "$QA_SNAPPY_MS" ]; then
    qa_pass "[C] intermediate dirs navigable mid-copy; p99 ${C_P99}ms (<${QA_SNAPPY_MS}ms), worst ${C_MAXMS}ms (<${QA_HARD_STALL_MS}ms) over $C_PROBES probes"
else
    qa_fail "[C] mid-copy listing SLOW: p99 ${C_P99}ms (>=${QA_SNAPPY_MS}ms), worst ${C_MAXMS}ms under $C_LEAF"
    _note_fail "mixed mid-copy listing p99 slow ${C_P99}ms ($C_LEAF)"
fi

# Finder result of the (now-finished) bg copy.
if [ "$C_RC" -ne 0 ]; then
    qa_fail "[C] Finder duplicate rc=$C_RC for $C_ROOT -> $C_DESTP :: $C_OUT"
    _note_fail "mixed Finder duplicate failed ($C_ROOT -> $C_DESTP): $C_OUT"
else
    qa_pass "[C] Finder duplicate returned (rc=0)"
fi
if ! qa_finder_result_clean "$C_OUT"; then
    qa_fail "[C] Finder result carried a gated error for $C_LEAF :: $C_OUT"
    _note_fail "mixed Finder result error at $C_LEAF: $C_OUT"
fi

qa_log "[C] waiting for spool to drain (MANDATORY pre-verify gate)"
# Payload-scaled drain ceiling for the full mixed tree.
C_CEIL="$(qa_drain_ceiling $(( C_EXPECT * QA_MIXED_FILE_SIZE )) $(( C_EXPECT * 2 )))"
if qa_wait_drain "$C_CEIL"; then qa_pass "[C] spool drained"; else
    qa_fail "[C] spool did NOT drain for $C_LEAF"; _note_fail "mixed spool drain timeout ($C_LEAF)"
fi

# FINAL completeness: total count + full custody.
C_LANDED="$(_landed_count "$C_LEAF")"
if [ "$C_LANDED" = "$C_EXPECT" ]; then
    qa_pass "[C] final file count complete: landed=$C_LANDED expected=$C_EXPECT"
else
    qa_fail "[C] final file count INCOMPLETE under $C_LEAF: landed=$C_LANDED expected=$C_EXPECT"
    _note_fail "mixed count mismatch ($C_LEAF): landed=$C_LANDED expected=$C_EXPECT"
fi

if qa_verify_custody "$C_MAN" "$C_LEAF"; then
    qa_pass "[C] custody clean (zero loss / zero corruption)"
else
    qa_fail "[C] custody FAILED under $C_LEAF"; _note_fail "mixed custody failed ($C_LEAF)"
fi

C_SCAN="$(qa_error_scan "$mark" C_mixed)"
qa_log "[C] $C_SCAN"
if printf '%s' "$C_SCAN" | grep -q 'STALE=0 PHANTOM=0 E100070=0 E100060=0 E48=0 E36=0 E5000=0 PERM=0'; then
    qa_pass "[C] error-scan clean"
else
    qa_fail "[C] error signature(s) in window for $C_LEAF :: $C_SCAN (see $QA_CAT_DIR/errscan-C_mixed.txt)"
    _note_fail "mixed error signature ($C_SCAN); see $QA_CAT_DIR/errscan-C_mixed.txt"
fi

# ---------------------------------------------------------------------------
# Wrap up: write .summary, run lib cleanup, emit single-line VERDICT.
# ---------------------------------------------------------------------------
qa_end
if [ "${QA_FAIL:-0}" -eq 0 ]; then
    echo "VERDICT: PASS $QA_CAT_LABEL"
else
    [ -z "$FAIL_REASON" ] && FAIL_REASON="see [FAIL] lines above and $QA_CAT_DIR/errscan-*.txt"
    echo "VERDICT: FAIL $QA_CAT_LABEL: $FAIL_REASON"
fi
exit 0
