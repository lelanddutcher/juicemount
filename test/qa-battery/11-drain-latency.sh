#!/usr/bin/env bash
# ===========================================================================
# 11-drain-latency.sh — JuiceMount RELEASE BATTERY: directory-open + Stat
#                        latency UNDER SUSTAINED SPOOL-DRAIN LOAD
#
# THE GATE (hard requirement — the spinner-under-drain regression test):
#   While the drainer is actively pushing a SUSTAINED multi-GB copy to the
#   backend (io.CopyBuffer(1MiB) + dst.Sync() per file through the same
#   JuiceFS/FUSE daemon), opening an ALREADY-CACHED directory AND stat'ing an
#   already-cached file must STILL return near-instantly from the local
#   metadata DB. The user must NEVER see Finder's loading spinner because a
#   copy is draining in the background.
#
#   Concretely: across the ENTIRE drain window, the p99 of both
#     * directory-open  (qa_dir_listing_ms — readdir/LOOKUP)
#     * per-file Stat/GETATTR (stat(2) of a cached file)
#   must stay < $QA_SNAPPY_MS (200ms). ANY single sample over budget FAILs the
#   gate and names the offending probe + latency.
#
# WHY THIS TEST EXISTS (the bug it guards):
#   Pre-fix, a Stat of a cached NON-directory entry with no active writer / open
#   FD / recent Redis blip / ._ / spool-pending fell into the phantom-purge else
#   branch and issued a BLOCKING os.Lstat on FUSE on the SYNCHRONOUS Stat path.
#   Idle that Lstat is µs; under a hot drain it blocked up to its timeout because
#   the drainer saturates the SAME FUSE daemon. Each slow Stat held an
#   nfsLstatGate slot AND an rpcSem slot; once those drained the NFS reader
#   parked and EVERY subsequent READDIR on Finder's single TCP connection waited
#   -> the directory-open spinner. The fix serves the cached FileInfo
#   immediately and confirms the phantom in the background (deduped per path),
#   and isolates the background prefetcher onto its own FUSE-syscall budget so it
#   can't starve the foreground gate. THIS test is the live gate proving the
#   spinner is gone.
#
# WHAT THIS SCRIPT DOES:
#   1. Pre-stage an UNRELATED tree off-mount, Finder-copy it onto the mount, and
#      drain it so it is genuinely CACHED (in the local metadata DB, at rest).
#   2. Start a SUSTAINED multi-GB Finder copy (hundreds of files) in the
#      background — enough payload to keep the drainer's CopyBuffer+Sync hot for
#      the whole measurement window.
#   3. For the FULL drain duration, LOOP measuring directory-open latency AND a
#      per-file Stat/GETATTR latency on the CACHED tree. Record p50/p99 for each.
#   4. GATE: p99 < $QA_SNAPPY_MS across the entire window. FAIL on ANY sample
#      over budget, naming it.
#
# CHAIN OF CUSTODY: both trees are staged off-mount with md5 manifests and copied
#   via REAL Finder; we qa_wait_drain then qa_verify_custody so the latency
#   numbers are taken against REAL at-rest data and the drain payload is proven
#   intact (no data loss from the load itself).
#
# NON-DESTRUCTIVE: all sources are staged under $QA_STAGE (/tmp); the single dest
#   is $QA_DEST_ROOT/$(qa_unique_tag QA) (matches the QA_*_$$_* cleanup guard).
#   The EXIT/INT/TERM trap stops any in-flight copy, restores online state, and
#   runs qa_cleanup. We NEVER touch the user's real folders.
#
# Scripts ALWAYS exit 0; pass/fail is in the .summary + the final VERDICT line.
# ===========================================================================

set -uo pipefail

source "$(dirname "$0")/lib.sh"

QA_CAT="11-drain-latency"

# ---------------------------------------------------------------------------
# Tunables (override via env).
#   DRAIN payload: hundreds of files totalling multiple GB so the drainer stays
#   hot the whole window. QUICK mode shrinks it for a smoke run (gate identical).
DRAIN_DIRS="${DRAIN_DIRS:-8}"                # subdirs in the drain source tree
DRAIN_FILES_PER_DIR="${DRAIN_FILES_PER_DIR:-40}"  # files per subdir
DRAIN_FILE_BYTES="${DRAIN_FILE_BYTES:-8388608}"   # 8 MiB each -> 8*40*8MiB ~= 2.5 GB
if [ "${QUICK:-0}" = "1" ]; then
    DRAIN_DIRS="${DRAIN_DIRS_QUICK:-4}"
    DRAIN_FILES_PER_DIR="${DRAIN_FILES_PER_DIR_QUICK:-20}"
    DRAIN_FILE_BYTES="${DRAIN_FILE_BYTES_QUICK:-2097152}"  # 2 MiB -> ~160 MiB
fi

# CACHED probe tree (the thing whose dir-open + Stat we measure under load). Kept
# small + tiny-byte: it's about metadata latency, not bytes, and it must be fully
# cached + at rest BEFORE the drain starts.
CACHE_PROBE_FILES="${CACHE_PROBE_FILES:-200}"
CACHE_PROBE_BYTES="${CACHE_PROBE_BYTES:-4096}"

# Measurement loop: keep sampling until the drain finishes, but always bound the
# window so a stuck drain can't hang the battery. Each loop does one dir-open +
# one file-Stat. Tiny pause between iterations to avoid a busy spin.
DRAIN_SAMPLE_PAUSE_MS="${DRAIN_SAMPLE_PAUSE_MS:-100}"
DRAIN_WINDOW_MAX_S="${DRAIN_WINDOW_MAX_S:-$QA_DRAIN_TIMEOUT}"
# Floor on samples: even a fast drain must yield a statistically meaningful set
# so a transient stall can't slip between two samples. If the drain finishes
# before this many samples, keep sampling (mount still warm) up to the floor.
DRAIN_MIN_SAMPLES="${DRAIN_MIN_SAMPLES:-40}"

# ---------------------------------------------------------------------------
# State for the EXIT trap (stop copy, restore offline, cleanup).
DRAIN_DEST=""
DRAIN_COPY_PID=""

drain_on_exit() {
    # Stop any still-running background drain copy so we never leave Finder
    # pushing into the spool after we exit.
    if [ -n "$DRAIN_COPY_PID" ]; then
        kill -TERM "$DRAIN_COPY_PID" 2>/dev/null || true
        DRAIN_COPY_PID=""
    fi
    # Belt-and-suspenders dest removal inside the guarded root; qa_cleanup also
    # removes ONLY $MOUNT/JM_RELEASE_BATTERY/QA_*_$$_* and $QA_STAGE.
    if [ -n "$DRAIN_DEST" ]; then
        case "$DRAIN_DEST" in
            "$QA_DEST_ROOT"/QA_*_$$_*) rm -rf "$DRAIN_DEST" 2>/dev/null || true ;;
        esac
    fi
    qa_cleanup 2>/dev/null || true
}
trap drain_on_exit EXIT INT TERM

# ---------------------------------------------------------------------------
# Per-file Stat/GETATTR latency. stat(2) of a cached file forces a GETATTR over
# NFS — the exact RPC the pre-fix phantom-purge blocked under drain. Echoes
# integer ms, or -1 if the stat itself failed (caller treats -1 as FAIL).
drain_stat_ms() {
    local f="$1" t0 t1
    t0=$(_qa_now_ms)
    if ! qa_timeout 30 stat -f%z "$f" >/dev/null 2>&1; then
        echo -1; return
    fi
    t1=$(_qa_now_ms)
    echo $(( t1 - t0 ))
}

# Percentile of a newline-delimited integer-ms stream. drain_pctile PCT < file.
# bash 3.2-safe: sort numerically, pick the ceil(PCT/100 * N) element (1-based,
# nearest-rank). Ignores -1 (failed) samples for the percentile (failures are
# gated separately as hard FAILs). Echoes the ms value, or -1 if no valid sample.
drain_pctile() {
    local pct="$1"
    awk -v pct="$pct" '
        /^-?[0-9]+$/ && $1 >= 0 { a[n++]=$1 }
        END {
            if (n==0) { print -1; exit }
            # numeric sort (insertion — n is small: a few hundred samples)
            for (i=1;i<n;i++){ v=a[i]; j=i-1; while(j>=0 && a[j]>v){a[j+1]=a[j];j--}; a[j+1]=v }
            rank = int((pct/100.0)*n + 0.999999)   # ceil, nearest-rank
            if (rank < 1) rank = 1
            if (rank > n) rank = n
            print a[rank-1]
        }'
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

DRAIN_DEST="$QA_DEST_ROOT/$(qa_unique_tag QA)"
mkdir -p "$DRAIN_DEST" 2>/dev/null
qa_info "drain-latency dest: $DRAIN_DEST"
qa_info "drain payload: ${DRAIN_DIRS}x${DRAIN_FILES_PER_DIR} files @ ${DRAIN_FILE_BYTES}B  (~$(( DRAIN_DIRS * DRAIN_FILES_PER_DIR * DRAIN_FILE_BYTES / 1048576 )) MiB)"
qa_info "cache probe: ${CACHE_PROBE_FILES} files @ ${CACHE_PROBE_BYTES}B   budget(p99): ${QA_SNAPPY_MS}ms"

mark=$(qa_log_mark)

# ---------------------------------------------------------------------------
# PHASE 0 — Pre-stage + cache the UNRELATED probe tree, drain, verify, so its
# dir-open + Stat are served from the local DB (NOT a backend round-trip).
qa_sec "PHASE 0: stage + cache the UNRELATED probe tree (real Finder copy)"
probe_stage="$QA_STAGE/drain_probe_$(qa_unique_tag s)"
# DIRS=1, FILES_PER_DIR=N, WITH_DOTUNDERSCORE=1 -> flat dir1/ + ._ sidecars.
probe_manifest="$(qa_stage_tree "$probe_stage" 1 "$CACHE_PROBE_FILES" "$CACHE_PROBE_BYTES" 1)"
probe_base="$(basename "$probe_stage")"

out="$(qa_finder_copy "$probe_stage" "$DRAIN_DEST")"
rc=$?
if [ "$rc" -ne 0 ] || ! qa_finder_result_clean "$out"; then
    qa_fail "drain-latency: probe Finder copy failed src=$probe_stage dest=$DRAIN_DEST result=$out"
fi
PROBE_DIR="$DRAIN_DEST/$probe_base/dir1"     # the cached dir we will dir-open under load
# Pick a stable cached file inside it for the Stat probe.
PROBE_FILE="$PROBE_DIR/file1.dat"

qa_sec "PHASE 0b: drain + custody of the probe (so latency is vs REAL cached data)"
if ! qa_wait_drain; then
    qa_fail "drain-latency: probe spool did not drain before measuring (dest=$DRAIN_DEST)"
fi
if qa_verify_custody "$probe_manifest" "$DRAIN_DEST/$probe_base" >/dev/null 2>&1; then
    qa_pass "drain-latency: probe custody OK (cached tree intact before load)"
else
    qa_fail "drain-latency: probe custody MISSING/WRONG (dest=$DRAIN_DEST/$probe_base)"
fi

# Warm the probe once so the very first under-load sample isn't a legit cold miss
# (the gate is about CACHED snappiness under load, not first-touch cold latency).
qa_dir_listing_ms "$PROBE_DIR" >/dev/null 2>&1 || true
drain_stat_ms "$PROBE_FILE" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# PHASE 1 — Start the SUSTAINED multi-GB drain copy in the background.
qa_sec "PHASE 1: start SUSTAINED multi-GB Finder copy (keeps the drainer hot)"
drain_stage="$QA_STAGE/drain_load_$(qa_unique_tag s)"
drain_manifest="$(qa_stage_tree "$drain_stage" "$DRAIN_DIRS" "$DRAIN_FILES_PER_DIR" "$DRAIN_FILE_BYTES" 1)"
drain_base="$(basename "$drain_stage")"

( qa_finder_copy "$drain_stage" "$DRAIN_DEST" >/dev/null 2>&1 ) &
DRAIN_COPY_PID=$!
qa_info "drain copy started (pid=$DRAIN_COPY_PID) src=$drain_stage"

# Wait until the spool is actually PENDING (drainer is hot) before we trust the
# window — bounded so a no-op copy can't hang us. Poll up to ~10s.
_w=0
while [ "$_w" -lt 20 ]; do
    pend="$(qa_spool_pending)"
    [ "$pend" -gt 0 ] 2>/dev/null && break
    kill -0 "$DRAIN_COPY_PID" 2>/dev/null || break
    perl -e 'select undef,undef,undef,0.5'
    _w=$((_w+1))
done
qa_info "drain hot: spool pending_files=$(qa_spool_pending) in_progress=$(qa_spool_field in_progress)"

# ---------------------------------------------------------------------------
# PHASE 2 — LOOP measuring dir-open + Stat latency for the FULL drain window.
qa_sec "PHASE 2: measure dir-open + Stat latency for the FULL drain window"
DIR_SAMPLES="$QA_CAT_DIR/dir-open-ms.txt"
STAT_SAMPLES="$QA_CAT_DIR/stat-ms.txt"
: > "$DIR_SAMPLES"; : > "$STAT_SAMPLES"

DRAIN_WORST_DIR_MS=-1; DRAIN_WORST_DIR=""
DRAIN_WORST_STAT_MS=-1; DRAIN_WORST_STAT=""
DRAIN_DIR_OVER=0; DRAIN_STAT_OVER=0
DRAIN_DIR_ERR=0; DRAIN_STAT_ERR=0
samples=0
t_start=$(date +%s)
pause_s="$(awk "BEGIN{print $DRAIN_SAMPLE_PAUSE_MS/1000}")"

while :; do
    now=$(date +%s)
    elapsed=$(( now - t_start ))

    # Stop conditions: drain finished AND we have the minimum sample floor, OR
    # we hit the hard window cap (so a stuck drain can never hang the battery).
    drain_done=0
    if ! kill -0 "$DRAIN_COPY_PID" 2>/dev/null; then
        p="$(qa_spool_pending)"; ip="$(qa_spool_field in_progress)"
        if [ "$p" = "0" ] && { [ "$ip" = "0" ] || [ "$ip" = "-1" ]; }; then
            drain_done=1
        fi
    fi
    if [ "$drain_done" = "1" ] && [ "$samples" -ge "$DRAIN_MIN_SAMPLES" ]; then
        break
    fi
    if [ "$elapsed" -ge "$DRAIN_WINDOW_MAX_S" ]; then
        qa_warn "drain-latency: hit window cap ${DRAIN_WINDOW_MAX_S}s (samples=$samples) — stopping measurement"
        break
    fi

    # --- dir-open sample (readdir of the cached probe dir) ---
    dms="$(qa_dir_listing_ms "$PROBE_DIR")"
    echo "$dms" >> "$DIR_SAMPLES"
    if [ "$dms" = "-1" ]; then
        DRAIN_DIR_ERR=$((DRAIN_DIR_ERR+1))
        qa_fail "drain-latency: dir-open READDIR FAILED under load dir=$PROBE_DIR (sample #$samples, elapsed=${elapsed}s)"
    else
        if [ "$dms" -gt "$DRAIN_WORST_DIR_MS" ]; then DRAIN_WORST_DIR_MS="$dms"; DRAIN_WORST_DIR="$PROBE_DIR"; fi
        if [ "$dms" -ge "$QA_SNAPPY_MS" ]; then
            DRAIN_DIR_OVER=$((DRAIN_DIR_OVER+1))
            qa_fail "drain-latency: dir-open ${dms}ms >= ${QA_SNAPPY_MS}ms (spinner) under drain dir=$PROBE_DIR (sample #$samples, elapsed=${elapsed}s)"
        fi
    fi

    # --- Stat/GETATTR sample (cached file) ---
    sms="$(drain_stat_ms "$PROBE_FILE")"
    echo "$sms" >> "$STAT_SAMPLES"
    if [ "$sms" = "-1" ]; then
        DRAIN_STAT_ERR=$((DRAIN_STAT_ERR+1))
        qa_fail "drain-latency: Stat/GETATTR FAILED under load file=$PROBE_FILE (sample #$samples, elapsed=${elapsed}s)"
    else
        if [ "$sms" -gt "$DRAIN_WORST_STAT_MS" ]; then DRAIN_WORST_STAT_MS="$sms"; DRAIN_WORST_STAT="$PROBE_FILE"; fi
        if [ "$sms" -ge "$QA_SNAPPY_MS" ]; then
            DRAIN_STAT_OVER=$((DRAIN_STAT_OVER+1))
            qa_fail "drain-latency: Stat ${sms}ms >= ${QA_SNAPPY_MS}ms under drain file=$PROBE_FILE (sample #$samples, elapsed=${elapsed}s)"
        fi
    fi

    samples=$((samples+1))
    perl -e 'select undef,undef,undef, shift' "$pause_s"
done

qa_info "measurement done: samples=$samples elapsed=$(( $(date +%s) - t_start ))s dir_over=$DRAIN_DIR_OVER stat_over=$DRAIN_STAT_OVER dir_err=$DRAIN_DIR_ERR stat_err=$DRAIN_STAT_ERR"

if [ "$samples" -eq 0 ]; then
    qa_fail "drain-latency: zero latency samples taken (drain never went hot? dest=$DRAIN_DEST)"
fi

# ---------------------------------------------------------------------------
# PHASE 3 — p50/p99 across the whole window. GATE on p99 < budget.
qa_sec "PHASE 3: p50/p99 latency over the drain window (GATE: p99 < ${QA_SNAPPY_MS}ms)"
DIR_P50="$(drain_pctile 50 < "$DIR_SAMPLES")"
DIR_P99="$(drain_pctile 99 < "$DIR_SAMPLES")"
STAT_P50="$(drain_pctile 50 < "$STAT_SAMPLES")"
STAT_P99="$(drain_pctile 99 < "$STAT_SAMPLES")"

printf '[CASE] dir-open  under-drain  samples=%-4s p50=%-5sms p99=%-5sms worst=%sms  budget=%sms\n' \
    "$samples" "$DIR_P50" "$DIR_P99" "$DRAIN_WORST_DIR_MS" "$QA_SNAPPY_MS"
printf '[CASE] stat      under-drain  samples=%-4s p50=%-5sms p99=%-5sms worst=%sms  budget=%sms\n' \
    "$samples" "$STAT_P50" "$STAT_P99" "$DRAIN_WORST_STAT_MS" "$QA_SNAPPY_MS"

# p99 gate (each over-budget sample already qa_fail'd above; the p99 assertion is
# the headline gate and catches a window whose tail crept over budget overall).
if [ "$DIR_P99" != "-1" ] && [ "$DIR_P99" -lt "$QA_SNAPPY_MS" ]; then
    qa_pass "drain-latency: dir-open p99 ${DIR_P99}ms < ${QA_SNAPPY_MS}ms across drain window"
else
    qa_fail "drain-latency: dir-open p99 ${DIR_P99}ms >= ${QA_SNAPPY_MS}ms (spinner under drain) worst=${DRAIN_WORST_DIR_MS}ms dir=$DRAIN_WORST_DIR"
fi
if [ "$STAT_P99" != "-1" ] && [ "$STAT_P99" -lt "$QA_SNAPPY_MS" ]; then
    qa_pass "drain-latency: Stat p99 ${STAT_P99}ms < ${QA_SNAPPY_MS}ms across drain window"
else
    qa_fail "drain-latency: Stat p99 ${STAT_P99}ms >= ${QA_SNAPPY_MS}ms under drain worst=${DRAIN_WORST_STAT_MS}ms file=$DRAIN_WORST_STAT"
fi

# Persist a small latency table artifact.
{
    printf 'metric\tsamples\tp50_ms\tp99_ms\tworst_ms\tbudget_ms\tover_budget\terrors\n'
    printf 'dir-open\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$samples" "$DIR_P50" "$DIR_P99" "$DRAIN_WORST_DIR_MS" "$QA_SNAPPY_MS" "$DRAIN_DIR_OVER" "$DRAIN_DIR_ERR"
    printf 'stat\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$samples" "$STAT_P50" "$STAT_P99" "$DRAIN_WORST_STAT_MS" "$QA_SNAPPY_MS" "$DRAIN_STAT_OVER" "$DRAIN_STAT_ERR"
} > "$QA_CAT_DIR/drain-latency-table.tsv" 2>/dev/null

# ---------------------------------------------------------------------------
# PHASE 4 — let the drain finish + verify custody of the LOAD payload (the load
# itself must not have lost/corrupted data).
qa_sec "PHASE 4: settle drain + custody of the LOAD payload (zero data loss)"
wait "$DRAIN_COPY_PID" 2>/dev/null || true
DRAIN_COPY_PID=""
if ! qa_wait_drain; then
    qa_warn "drain-latency: load spool did not fully settle (dest=$DRAIN_DEST)"
fi
if qa_verify_custody "$drain_manifest" "$DRAIN_DEST/$drain_base" >/dev/null 2>&1; then
    qa_pass "drain-latency: LOAD payload custody OK (no data loss under measurement)"
else
    qa_fail "drain-latency: LOAD payload custody MISSING/WRONG (dest=$DRAIN_DEST/$drain_base)"
fi

# ---------------------------------------------------------------------------
# Zero-random-Finder-error gate over the whole window (principle #3).
qa_sec "error-signature scan over the drain-latency window"
scan="$(qa_error_scan "$mark" "$QA_CAT")"
qa_info "$scan"
if ! qa_error_scan "$mark" "$QA_CAT" >/dev/null 2>&1; then
    qa_fail "drain-latency: Finder error signature in window ($scan) — see $QA_CAT_DIR/errscan-$QA_CAT.txt"
fi

# ---------------------------------------------------------------------------
qa_end

# ---------------------------------------------------------------------------
# Final single-line VERDICT.
if [ "$QA_FAIL" -eq 0 ]; then
    echo "VERDICT: PASS $QA_CAT"
else
    reason=""
    if [ "$DRAIN_DIR_OVER" -gt 0 ] || [ "$DRAIN_STAT_OVER" -gt 0 ] || [ "$DRAIN_DIR_ERR" -gt 0 ] || [ "$DRAIN_STAT_ERR" -gt 0 ]; then
        reason="dir-open p99=${DIR_P99}ms (over=$DRAIN_DIR_OVER err=$DRAIN_DIR_ERR worst=${DRAIN_WORST_DIR_MS}ms), Stat p99=${STAT_P99}ms (over=$DRAIN_STAT_OVER err=$DRAIN_STAT_ERR worst=${DRAIN_WORST_STAT_MS}ms) under drain"
    else
        reason="custody/drain/error-scan failure"
    fi
    echo "VERDICT: FAIL $QA_CAT: $reason (budget ${QA_SNAPPY_MS}ms, dest=$DRAIN_DEST) — see drain-latency-table.tsv + [FAIL] lines above"
fi

exit 0
