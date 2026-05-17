#!/usr/bin/env bash
# 04-fio.sh — synthetic IO benchmarks via fio.
# Target: ~40 min.
#
# Profiles (each 4-8 min):
#   randread-4k    — 4 KiB random read, IOps measurement
#   randwrite-4k   — 4 KiB random write, IOps measurement
#   seqread-1m     — 1 MiB sequential read, throughput
#   seqwrite-1m    — 1 MiB sequential write, throughput
#   mixed-7030     — 70% read / 30% write 64 KiB random, real-world editing
#   many-small     — create + read 8 KiB files (small-file behavior)
#
# All profiles target the mount path. Output JSON to artifacts/.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

phase_init "04-fio"

if ! has_cmd fio; then
    warn "fio not installed — skipping entire phase. Install via: brew install fio"
    phase_report
    exit 0
fi

TROOT="$MOUNT/.jmqa-fio-$$"
mkdir -p "$TROOT"

run_fio() {
    local name="$1" size="$2" rw="$3" bs="$4" runtime="$5" iodepth="${6:-16}" numjobs="${7:-1}"
    local out="${PHASE_DIR}/fio-${name}.json"
    log "fio: ${name} (${rw} bs=${bs} size=${size} runtime=${runtime}s iodepth=${iodepth} jobs=${numjobs})"
    fio --name="$name" \
        --directory="$TROOT" \
        --size="$size" \
        --rw="$rw" \
        --bs="$bs" \
        --runtime="$runtime" \
        --time_based \
        --iodepth="$iodepth" \
        --numjobs="$numjobs" \
        --direct=0 \
        --ioengine=posixaio \
        --group_reporting \
        --output-format=json \
        --output="$out" \
        >/dev/null 2>&1
    local rc=$?
    # Extract throughput / IOps from the JSON
    if [[ $rc -eq 0 && -s "$out" ]]; then
        local r_bw r_iops w_bw w_iops
        r_bw=$(python3 -c "import json; d=json.load(open('$out')); print(int(d['jobs'][0]['read']['bw']))" 2>/dev/null)
        r_iops=$(python3 -c "import json; d=json.load(open('$out')); print(int(d['jobs'][0]['read']['iops']))" 2>/dev/null)
        w_bw=$(python3 -c "import json; d=json.load(open('$out')); print(int(d['jobs'][0]['write']['bw']))" 2>/dev/null)
        w_iops=$(python3 -c "import json; d=json.load(open('$out')); print(int(d['jobs'][0]['write']['iops']))" 2>/dev/null)
        printf '    read:  %s KiB/s, %s IOps\n' "${r_bw:-0}" "${r_iops:-0}"
        printf '    write: %s KiB/s, %s IOps\n' "${w_bw:-0}" "${w_iops:-0}"
        pass "fio $name completed"
    else
        fail "fio $name failed (rc=$rc) — see $out"
    fi
}

section "1) seqwrite-1m — 1 MiB sequential write throughput"
run_fio seqwrite-1m 512M write 1M 60 8 1

section "2) seqread-1m — 1 MiB sequential read throughput"
# Pre-populate by reusing what seqwrite created above; new file though.
run_fio seqread-1m 512M read 1M 60 8 1

section "3) randwrite-4k — 4 KiB random write IOps"
run_fio randwrite-4k 256M randwrite 4k 120 32 1

section "4) randread-4k — 4 KiB random read IOps"
run_fio randread-4k 256M randread 4k 120 32 1

section "5) mixed-7030 — 70/30 read/write 64 KiB random (editing-realistic)"
run_fio mixed-7030 512M randrw 64k 180 16 1

section "6) randread-1m — high-iodepth scrub simulation"
run_fio randread-1m 1G randread 1M 120 64 1

section "7) parallel-jobs — 4 concurrent writers (multi-stream)"
run_fio parallel-write 256M write 1M 60 8 4

section "8) parallel-randmix — 4 concurrent mixed workers"
run_fio parallel-randmix 256M randrw 64k 90 16 4

snapshot_metrics "post-fio"
snapshot_rss "post-fio"
rm -rf "$TROOT" 2>/dev/null
phase_report
