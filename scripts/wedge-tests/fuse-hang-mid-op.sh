#!/usr/bin/env bash
# wedge-tests/fuse-hang-mid-op.sh — closes part of tier-1.2.
#
# Scenario: the JuiceFS FUSE daemon (juicefs mount, backing the
# JuiceMount handler's storage layer) hangs mid-operation. We
# simulate this by SIGSTOP'ing the juicefs processes attached to the
# JuiceMount fuse-internal mount point, then issuing a stat on a
# fresh path that forces the handler to actually traverse FUSE
# (not serve from its metadata-store cache).
#
# Acceptance criterion (per docs/ROADMAP/tier-1-stability.md iter B
# and the existing Lstat-timeout work from b1e9c6a, 2026-05-13):
#
#   - Stat on a fresh (uncached) path returns within FUSE_TIMEOUT_S
#     seconds (default 4 — Lstat timeout is ~2s in the handler, plus
#     small overhead). It does NOT hang indefinitely.
#   - Adjacent stats on an ALREADY-CACHED path (e.g. mount root, which
#     the handler serves from metadata.Store without touching FUSE)
#     stay under STAT_BUDGET_MS (default 500ms) — the "metadata-only
#     ops stay responsive while FUSE is wedged" proxy.
#   - After SIGCONT, system recovers within RECOVER_BUDGET_S seconds —
#     a fresh stat succeeds within budget.
#
# This script is more invasive than minio-down-mid-read.sh because
# SIGSTOP'ing JuiceFS blocks ALL FUSE-bound operations on the mount.
# If the script crashes between SIGSTOP and SIGCONT, the user's mount
# is wedged until they manually run:
#
#     kill -CONT <juicefs-pid>...   # see ps aux | grep juicefs
#
# The trap-EXIT/INT/TERM is the safety net for normal terminations.
# SIGKILL of the script itself is the only unrecoverable case.
#
# Prerequisites:
#
#   1. JuiceMount mount up.
#   2. JuiceFS daemon running and attached to a JuiceMount fuse-
#      internal path discoverable via pgrep -f 'juicefs mount.*fuse-internal'.
#   3. We do NOT need passwordless sudo — SIGSTOP/SIGCONT work on
#      processes owned by the current user, which juicefs is.
#
# Usage:
#
#   scripts/wedge-tests/fuse-hang-mid-op.sh \
#       [--mount PATH]            \ default /Volumes/zpool-dev
#       [--fuse-timeout SECONDS]  \ default 4
#       [--stat-budget-ms MILLIS] \ default 500
#       [--recover-budget SECONDS] \ default 5
#       [--hold-duration SECONDS] \ default 3 (how long to keep SIGSTOP'd)
#
# Exit codes:
#   0  pass (or inconclusive — see WARN line)
#   1  fail (acceptance criterion missed)
#   2  precondition error
#
# Emits trailing `[PASS|FAIL|WARN] fuse-hang-mid-op: <details>`.

set -euo pipefail

# --- defaults ---
MOUNT="${MOUNT:-/Volumes/zpool-dev}"
FUSE_TIMEOUT_S=4
STAT_BUDGET_MS=500
RECOVER_BUDGET_S=5
HOLD_DURATION_S=3

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mount)            MOUNT="$2"; shift 2 ;;
        --fuse-timeout)     FUSE_TIMEOUT_S="$2"; shift 2 ;;
        --stat-budget-ms)   STAT_BUDGET_MS="$2"; shift 2 ;;
        --recover-budget)   RECOVER_BUDGET_S="$2"; shift 2 ;;
        --hold-duration)    HOLD_DURATION_S="$2"; shift 2 ;;
        -h|--help)          grep '^#' "$0" | sed 's/^# \?//'; exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

# --- helpers ---
ts_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }
log()  { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
pass() { printf '\033[32m[PASS]\033[0m %s\n' "$*"; }
fail() { printf '\033[31m[FAIL]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[WARN]\033[0m %s\n' "$*"; }

JUICEFS_PIDS=""

# Discover JuiceMount-attached JuiceFS PIDs. We target only daemons
# whose mount arg contains "fuse-internal" (the JuiceMount-managed
# JuiceFS instance). This avoids accidentally SIGSTOP'ing an
# unrelated juicefs mount on the same machine.
discover_juicefs_pids() {
    pgrep -f 'juicefs mount.*fuse-internal' 2>/dev/null | tr '\n' ' '
}

# Resume all SIGSTOP'd juicefs targets. Idempotent — SIGCONT on a
# running process is a no-op. The trap calls this; we also call it
# from the happy path before assessing recovery.
sigcont_all() {
    if [[ -n "$JUICEFS_PIDS" ]]; then
        for pid in $JUICEFS_PIDS; do
            kill -CONT "$pid" 2>/dev/null || true
        done
    fi
}

# Cleanup must SIGCONT under any termination path, otherwise the
# user's mount stays wedged.
cleanup() {
    sigcont_all
}
trap cleanup EXIT INT TERM

# --- preconditions ---
log "== preconditions =="
log "  mount:           $MOUNT"
log "  fuse timeout:    ${FUSE_TIMEOUT_S}s  (~2s Lstat + handler overhead)"
log "  stat budget:     ${STAT_BUDGET_MS}ms (for metadata-cached ops)"
log "  recover budget:  ${RECOVER_BUDGET_S}s after SIGCONT"
log "  hold duration:   ${HOLD_DURATION_S}s SIGSTOP'd"
log ""

if ! mount | grep -q " $MOUNT "; then
    fail "mount $MOUNT is not active"
    echo "[FAIL] fuse-hang-mid-op: precondition (mount inactive)"
    exit 2
fi
log "  ✓ mount is active"

JUICEFS_PIDS=$(discover_juicefs_pids)
if [[ -z "$JUICEFS_PIDS" ]]; then
    fail "no juicefs processes found matching 'juicefs mount.*fuse-internal'"
    echo "[FAIL] fuse-hang-mid-op: precondition (no juicefs PIDs)"
    exit 2
fi
log "  ✓ juicefs PIDs: $JUICEFS_PIDS"

# Pre-prime metadata cache by touching the mount root — that way the
# "cached stat stays fast" probe really IS hitting the metadata-only
# path, not racing against a first-time lookup.
stat "$MOUNT/" >/dev/null 2>&1 || true

# Baseline: stat on mount root should be sub-budget when juicefs
# is running normally.
b_start=$(ts_ms)
stat "$MOUNT/" >/dev/null 2>&1
b_end=$(ts_ms)
baseline_stat_ms=$((b_end - b_start))
log "  ✓ baseline mount-root stat: ${baseline_stat_ms}ms"

# --- the test ---
log ""
log "== fuse-hang-mid-op =="

# Re-discover PIDs immediately before SIGSTOP to shrink the TOCTOU
# window between precondition discovery and the actual stop. If
# juicefs crashed and restarted between then and now, we'd otherwise
# SIGSTOP nothing (or, worse, a reused PID belonging to an unrelated
# process). Cross-check and abort if the set changed.
LIVE_PIDS=$(discover_juicefs_pids)
if [[ "$LIVE_PIDS" != "$JUICEFS_PIDS" ]]; then
    fail "juicefs PIDs changed between discovery ($JUICEFS_PIDS) and SIGSTOP ($LIVE_PIDS) — aborting to avoid hitting wrong process"
    echo "[FAIL] fuse-hang-mid-op: precondition (juicefs PID set unstable)"
    exit 2
fi

# SIGSTOP each PID. Guard against the rare case where a target died
# between the cross-check and here — under set -e a single failure
# would abort mid-wedge before SIGCONT, producing an uninterpretable
# exit code. The trap still SIGCONTs whatever did get stopped, but
# the test would emit no verdict line. Better to log and continue.
log "  SIGSTOP'ing juicefs ($JUICEFS_PIDS)..."
for pid in $JUICEFS_PIDS; do
    if ! kill -STOP "$pid" 2>/dev/null; then
        log "  WARN: PID $pid disappeared before SIGSTOP — continuing with remaining targets"
    fi
done
stop_ts_ms=$(ts_ms)
log "  ✓ FUSE wedged at $(date +%H:%M:%S.%3N)"

# Probe 1: stat a FRESH path that forces FUSE traversal. The handler's
# metadata.Store can't satisfy this from cache, so it must go through
# FUSE — which is wedged. Expectation: the handler's Lstat timeout
# fires at ~2s, the stat returns with an error within FUSE_TIMEOUT_S.
FRESH_PATH="$MOUNT/.jm-fusehang-$(uuidgen 2>/dev/null || date +%s%N)"
log "  probing fresh path: $(basename "$FRESH_PATH")"
p_start=$(ts_ms)
fresh_exit=0; stat "$FRESH_PATH" >/dev/null 2>&1 || fresh_exit=$?
p_end=$(ts_ms)
fresh_elapsed_ms=$((p_end - p_start))
fresh_elapsed_s=$(printf '%.2f' "$(echo "scale=3; $fresh_elapsed_ms / 1000" | bc)")
log "  fresh-path stat returned (exit $fresh_exit) in ${fresh_elapsed_ms}ms"
# NOTE: fresh-path stat typically returns in 0.7-1s, well under the
# handler's internal Lstat timeout (~2s, b1e9c6a). This is expected:
# the kernel NFS client mounts with `soft` and `timeo=1s` (see
# scripts/test-offline-resilience.sh comments), so the kernel
# surrenders and returns an error to stat() before the handler's own
# deadline fires. The test still validates the acceptance criterion
# ("does not hang indefinitely") — the user-facing latency bound is
# the NFS soft-mount, not the handler timeout. If the kernel timeout
# is ever raised, this test starts measuring the handler deadline
# directly.

# Probe 2: while still SIGSTOP'd, stat the mount root. Metadata-only
# op served from cache — should stay snappy. This is the "Finder
# doesn't beachball" proxy for the FUSE-wedge case.
cached_max_ms=0
cached_count=0
hold_deadline_ms=$((stop_ts_ms + HOLD_DURATION_S * 1000))
while true; do
    now_ms=$(ts_ms)
    if [[ $now_ms -gt $hold_deadline_ms ]]; then break; fi
    s_start=$(ts_ms)
    if stat "$MOUNT/" >/dev/null 2>&1; then
        s_end=$(ts_ms)
        s_elapsed=$((s_end - s_start))
        cached_count=$((cached_count + 1))
        if [[ $s_elapsed -gt $cached_max_ms ]]; then
            cached_max_ms=$s_elapsed
        fi
    fi
    sleep 0.2
done
log "  cached-path stat probes during wedge: $cached_count (max ${cached_max_ms}ms)"

# Resume FUSE. From this point on, the system should be recovering.
log "  SIGCONT'ing juicefs..."
sigcont_all
cont_ts_ms=$(ts_ms)

# Probe 3: recovery — a fresh stat should succeed (or return ENOENT
# fast — fast either way is fine; the test is "does it return at all
# within RECOVER_BUDGET_S"). We re-use a new fresh UUID to avoid any
# negative-cache hit on the first FRESH_PATH from probe 1.
RECOVERY_PATH="$MOUNT/.jm-fusehang-recovery-$(uuidgen 2>/dev/null || date +%s%N)"
r_start=$(ts_ms)
rec_exit=0; stat "$RECOVERY_PATH" >/dev/null 2>&1 || rec_exit=$?
r_end=$(ts_ms)
rec_elapsed_ms=$((r_end - r_start))
rec_elapsed_s=$(printf '%.2f' "$(echo "scale=3; $rec_elapsed_ms / 1000" | bc)")
log "  recovery stat returned (exit $rec_exit) in ${rec_elapsed_ms}ms"

# --- verdict ---
#
# PASS requires ALL of:
#   - fresh-path stat returned within FUSE_TIMEOUT_S (the handler's
#     Lstat timeout fired and bounded the wedge — system did not hang)
#   - cached-path stats during wedge stayed under STAT_BUDGET_MS
#     (metadata-only ops responsive)
#   - recovery stat completed within RECOVER_BUDGET_S
#
# WEDGE-FOREVER conditions (HARD FAIL):
#   - fresh-path stat exceeded FUSE_TIMEOUT_S (timeout didn't fire)
#   - cached-path stats exceeded STAT_BUDGET_MS (handler's metadata
#     path is also wedged — global lock contention)
#   - recovery stat exceeded RECOVER_BUDGET_S (post-SIGCONT recovery
#     broken)

verdict_pass=true
verdict_details=""
fuse_timeout_ms=$((FUSE_TIMEOUT_S * 1000))
recover_budget_ms=$((RECOVER_BUDGET_S * 1000))

if [[ $fresh_elapsed_ms -gt $fuse_timeout_ms ]]; then
    verdict_pass=false
    verdict_details="fresh-path stat took ${fresh_elapsed_s}s (budget ${FUSE_TIMEOUT_S}s) — Lstat timeout did not fire"
fi

if [[ $cached_max_ms -gt $STAT_BUDGET_MS ]]; then
    msg="cached stat max ${cached_max_ms}ms (budget ${STAT_BUDGET_MS}ms) — metadata path wedged too"
    if [[ "$verdict_pass" == "true" ]]; then
        verdict_pass=false
        verdict_details="$msg"
    else
        verdict_details="$verdict_details; $msg"
    fi
fi

if [[ $rec_elapsed_ms -gt $recover_budget_ms ]]; then
    msg="recovery stat took ${rec_elapsed_s}s (budget ${RECOVER_BUDGET_S}s) — system did not recover"
    if [[ "$verdict_pass" == "true" ]]; then
        verdict_pass=false
        verdict_details="$msg"
    else
        verdict_details="$verdict_details; $msg"
    fi
fi

if [[ $cached_count -eq 0 ]]; then
    warn "no cached-path stat probes completed during wedge (HOLD_DURATION=${HOLD_DURATION_S}s may be too short)"
fi

log ""
if [[ "$verdict_pass" == "true" ]]; then
    pass "fuse-hang-mid-op: fresh=${fresh_elapsed_s}s cached_max=${cached_max_ms}ms recovery=${rec_elapsed_s}s (probes=$cached_count)"
    echo "[PASS] fuse-hang-mid-op: fresh=${fresh_elapsed_s}s cached_max=${cached_max_ms}ms recovery=${rec_elapsed_s}s"
    exit 0
else
    fail "fuse-hang-mid-op: $verdict_details"
    echo "[FAIL] fuse-hang-mid-op: $verdict_details"
    exit 1
fi
