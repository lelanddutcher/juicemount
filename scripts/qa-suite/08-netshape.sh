#!/usr/bin/env bash
# 08-netshape.sh — retired compatibility phase.
#
# This phase used to install a private PF anchor while globally flushing every
# dummynet pipe, reload/disable PF during cleanup, and therefore risk damaging
# an unrelated firewall, VPN, or developer experiment. It intentionally cannot
# shape traffic anymore.
#
# Safe replacements:
#   - test/perf/cellular_synthetic_acceptance.py
#       Bounded user-space synthetic cellular acceptance; no host firewall
#       mutation. Prefer this for routine policy/responsiveness testing.
#   - scripts/netshape-dev.sh
#       Explicit opt-in wire shaping with exact JuiceMount service/link ports,
#       a private evaluated anchor, reserved pipes, collision checks, and a PF
#       enable-reference token. Use only when TCP retransmission/wire behavior
#       itself is under test.
#
# The legacy phase keeps its read-only backend reachability and diagnostics so
# old run-all invocations remain useful, but it never invokes sudo, pfctl, or
# dnctl and never claims that shaping was exercised.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

phase_init "08-netshape"

SAFE_SHAPER="$JM_REPO_ROOT/scripts/netshape-dev.sh"
USERSPACE_RUNNER="$JM_REPO_ROOT/test/perf/cellular_synthetic_acceptance.py"

section "legacy global network shaping retired"
warn "08-netshape no longer mutates global PF/dummynet state; no shaped cases were run"
info "routine bounded simulation: python3 $USERSPACE_RUNNER --help"
info "explicit scoped wire shaping: $SAFE_SHAPER self-test"

if [[ -f "$USERSPACE_RUNNER" ]]; then
    pass "bounded user-space cellular acceptance runner is available"
else
    warn "bounded user-space runner is not present in this checkout: $USERSPACE_RUNNER"
fi
if [[ -x "$SAFE_SHAPER" ]]; then
    pass "scoped opt-in netshape helper is available"
else
    warn "scoped netshape helper is not executable: $SAFE_SHAPER"
fi

# Preserve the only useful no-shape part of the old phase: a read-only service
# reachability preflight. No default target is invented.
BACKEND_HOST="${BACKEND_HOST:-${JM_QA_NAS_IP:-}}"
BACKEND_PORT="${BACKEND_PORT:-9000}"
if [[ -z "$BACKEND_HOST" ]]; then
    warn "JM_QA_NAS_IP/BACKEND_HOST not set; read-only backend preflight skipped"
elif nc -zv -G 3 "$BACKEND_HOST" "$BACKEND_PORT" >/dev/null 2>&1; then
    pass "backend reachability preflight succeeded (${BACKEND_HOST}:${BACKEND_PORT})"
else
    warn "backend reachability preflight failed (${BACKEND_HOST}:${BACKEND_PORT})"
fi

snapshot_metrics "legacy-netshape-retired"
snapshot_lsof "legacy-netshape-retired"
phase_report
