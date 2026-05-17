#!/usr/bin/env bash
# 01-smoke.sh — quick sanity, ~5 min.
# Wraps the existing write-integrity harness and adds basic file ops.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

phase_init "01-smoke"

section "delegating to scripts/wedge-tests/write-integrity.sh"
WI_LOG="${PHASE_DIR}/write-integrity.log"
if MOUNT="$MOUNT" bash "$(dirname "$SCRIPT_DIR")/wedge-tests/write-integrity.sh" >"$WI_LOG" 2>&1; then
    pass "write-integrity 8/8"
else
    fail "write-integrity failed — see $WI_LOG"
fi
tail -3 "$WI_LOG" | sed 's/^/    /'

section "basic file ops (touch / mkdir / mv / rm / stat / readdir)"
T="$MOUNT/.jmqa-smoke-$$"
mkdir -p "$T" 2>/dev/null && pass "mkdir works" || fail "mkdir failed"
echo "hello" > "$T/a.txt" 2>/dev/null && pass "create file works" || fail "create failed"
[[ "$(cat "$T/a.txt" 2>/dev/null)" == "hello" ]] && pass "read-back works" || fail "read-back wrong"
mv "$T/a.txt" "$T/b.txt" 2>/dev/null && pass "mv works" || fail "mv failed"
[[ -f "$T/b.txt" ]] && pass "rename target exists" || fail "rename target missing"
ls -la "$T" > "${PHASE_DIR}/ls-after-ops.txt" 2>&1 && pass "readdir works" || fail "readdir failed"
stat "$T/b.txt" > "${PHASE_DIR}/stat-b.txt" 2>&1 && pass "stat works" || fail "stat failed"
rm -rf "$T" 2>/dev/null && pass "rm -rf works" || fail "rm -rf failed"
[[ ! -d "$T" ]] && pass "directory removed" || fail "directory still exists"

snapshot_metrics "post-smoke"
phase_report
