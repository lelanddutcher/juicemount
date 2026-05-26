#!/usr/bin/env bash
# 12-perf-regression: orchestrate the perf workload battery + compare
# each result against the stored healthy baseline. Exits non-zero on
# any threshold breach.
#
# See docs/PERFORMANCE_METHODOLOGY.md for the contract.
#
# Env / required external state:
#   * JM running on /Volumes/zpool (or wherever MOUNT auto-detects)
#   * jmctl built (this script builds it on demand)
#   * At least one pinned MP4 >= 500 MiB for resolve-scrub / cold-playback
#   * Read-write staging dir under the mount for finder-copy-deep
#
# Env (overridable):
#   TARGET_FILE       cached MP4 for resolve-scrub/cold-playback
#   BIN_BROWSE_DIR    dir for bin-browse
#   STAGING_DIR       writable dir for finder-copy-deep
#   BASELINE_DIR      where healthy baselines live;
#                     default scripts/qa-suite/baselines
#   ART_ROOT          where per-run artifacts go;
#                     default /tmp/jm-perf-run-<ts>
#   DURATION          per-workload duration; default 30s
#   FAIL_ON_REGRESSION if "1", exit 1 on any breach; default 1

set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
. "$HERE/lib.sh"
. "$HERE/11-workloads/_common.sh"

phase_init "12-perf-regression"

BASELINE_DIR="${BASELINE_DIR:-$REPO/scripts/qa-suite/baselines}"
ART_ROOT="${ART_ROOT:-/tmp/jm-perf-run-$(date +%s)}"
DURATION="${DURATION:-30}"
FAIL_ON_REGRESSION="${FAIL_ON_REGRESSION:-1}"

# Best-effort default targets. Override via env if the layout differs.
TARGET_FILE="${TARGET_FILE:-/Volumes/zpool/Film Projects/Leland Tiktoks/2026/09 beta premiere testing/A_0062C806H260427_142741EJ_LAD04.MP4}"
BIN_BROWSE_DIR="${BIN_BROWSE_DIR:-/Volumes/zpool/Film Projects/Leland Tiktoks/2026/09 beta premiere testing}"
STAGING_DIR="${STAGING_DIR:-/Volumes/zpool/testing}"

mkdir -p "$ART_ROOT"
log "perf-regression run → $ART_ROOT (baselines: $BASELINE_DIR)"

# Build jmctl into /tmp so workload scripts can find it at $JMCTL.
log "building jmctl…"
( cd "$REPO" && go build -o /tmp/jmctl ./cmd/jmctl ) || { fail "go build jmctl failed"; phase_report; exit 1; }
export JMCTL=/tmp/jmctl

# JM up?
if ! curl -sS --max-time 3 "http://${JM_METRICS_ADDR}/health" 2>/dev/null \
    | grep -q '"healthy":\s*true'; then
    warn "JM not healthy; running workloads anyway but expect noise"
fi

WORKLOADS=()
[ -n "$TARGET_FILE" ] && [ -f "$TARGET_FILE" ] && WORKLOADS+=("resolve-scrub")
[ -n "$BIN_BROWSE_DIR" ] && [ -d "$BIN_BROWSE_DIR" ] && WORKLOADS+=("bin-browse")
[ -n "$STAGING_DIR" ] && [ -d "$STAGING_DIR" ] && WORKLOADS+=("finder-copy-deep")
[ -n "$TARGET_FILE" ] && [ -f "$TARGET_FILE" ] && WORKLOADS+=("cold-playback")
WORKLOADS+=("pin-coverage-verify")

log "running ${#WORKLOADS[@]} workloads: ${WORKLOADS[*]}"

run_workload() {
    local name="$1"
    local art="$ART_ROOT/$name"
    mkdir -p "$art"
    ART_DIR="$art" \
        TARGET_FILE="$TARGET_FILE" \
        TARGET_DIR="$BIN_BROWSE_DIR" \
        STAGING_DIR="$STAGING_DIR" \
        DURATION="$DURATION" \
        bash "$HERE/11-workloads/$name.sh" > "$art/run.log" 2>&1
    local rc=$?
    if [ $rc -ne 0 ]; then
        warn "$name exited non-zero (rc=$rc); see $art/run.log"
    fi
    return $rc
}

# Run all workloads first, then compare.
for w in "${WORKLOADS[@]}"; do
    log "→ $w"
    run_workload "$w"
done

# Compare each summary to its baseline. Threshold semantics from
# docs/PERFORMANCE_METHODOLOGY.md.
BREACHES=0
compare_workload() {
    local name="$1"
    local cur="$ART_ROOT/$name/summary.json"
    local base="$BASELINE_DIR/${name}-healthy.json"
    if [ ! -f "$cur" ]; then
        warn "$name: no summary.json (workload didn't complete)"
        BREACHES=$((BREACHES + 1))
        return
    fi
    if [ ! -f "$base" ]; then
        warn "$name: no baseline at $base; recording but not comparing"
        return
    fi
    python3 - "$cur" "$base" "$name" <<'PY'
import json, sys
cur_p, base_p, name = sys.argv[1:]
with open(cur_p) as f:  cur  = json.load(f)
with open(base_p) as f: base = json.load(f)
problems = []
# Throughput
b_tput = base.get("throughput_MBps", 0.0)
c_tput = cur.get("throughput_MBps", 0.0)
if b_tput > 0 and c_tput < b_tput * 0.5:
    problems.append(f"throughput {c_tput:.1f} MB/s < 50% of baseline {b_tput:.1f}")
# rpc_errors
if cur.get("rpc_errors_delta", 0) > 0:
    problems.append(f"rpc_errors_delta={cur['rpc_errors_delta']}")
# FromHandle STALE
if cur.get("from_handle_stale_events", 0) > 0:
    problems.append(f"from_handle_stale_events={cur['from_handle_stale_events']}")
# Per-RPC p95 (2x) and max (5x)
for k, ra in (cur.get("rpcs") or {}).items():
    rb = (base.get("rpcs") or {}).get(k) or {}
    bp = (rb.get("p95_us") or 0)
    cp = (ra.get("p95_us") or 0)
    if bp > 0 and cp > 2.0 * bp:
        problems.append(f"{k}.p95_us {cp/1000:.1f}ms > 2× baseline {bp/1000:.1f}ms")
    bm = (rb.get("max_us") or 0)
    cm = (ra.get("max_us") or 0)
    if bm > 0 and cm > 5.0 * bm:
        problems.append(f"{k}.max_us {cm/1e6:.2f}s > 5× baseline {bm/1e6:.2f}s")
if not problems:
    print(f"  ✓ {name}: within thresholds")
    sys.exit(0)
print(f"  ✗ {name}: {len(problems)} breaches")
for p in problems:
    print(f"      - {p}")
sys.exit(1)
PY
    if [ $? -ne 0 ]; then
        BREACHES=$((BREACHES + 1))
    fi
}

echo ""
log "--- regression check ---"
for w in "${WORKLOADS[@]}"; do
    compare_workload "$w"
done

# Copy summaries into the artifact dir.
for w in "${WORKLOADS[@]}"; do
    [ -f "$ART_ROOT/$w/summary.json" ] && cp "$ART_ROOT/$w/summary.json" "$ART_DIR/${w}.json" 2>/dev/null || true
done

if [ "$BREACHES" -gt 0 ]; then
    fail "$BREACHES workload(s) breached thresholds"
    phase_report
    [ "$FAIL_ON_REGRESSION" = "1" ] && exit 1
else
    pass "all workloads within thresholds"
    phase_report
fi
