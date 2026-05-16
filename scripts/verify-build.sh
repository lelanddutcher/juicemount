#!/usr/bin/env bash
# verify-build.sh — sanity-check a JuiceMount build.
#
# Background: 2026-05-16 iter 4 of the autonomous loop discovered that
# SPM's incremental build doesn't notice content changes in libnfsd.a
# when it's passed via -L/-l. The result was a binary on disk that
# claimed to be "production" but was missing two tier-1 fixes (the
# Lstat timeout and the concurrent-dispatch fix). The running mount
# silently ran old code while we ran "validation" against it.
#
# This script makes the same class of bug visible. It verifies:
#
#   1. The .app binary on disk contains every fix this script knows
#      about (by symbol name). Each fix is identified by a symbol
#      that should exist in the final binary; if not, the build is
#      stale or the source no longer has that fix.
#
#   2. (Optional, --running) Any currently-running JuiceMount process
#      is using THIS binary, not a stale one. Compares process binary
#      inode to the .app's binary inode.
#
# Usage:
#   scripts/verify-build.sh
#   scripts/verify-build.sh --app /path/to/JuiceMount.app
#   scripts/verify-build.sh --running   # also check the live process
#
# Exit codes:
#   0  every known fix is present in the binary; running PID (if checked) matches
#   1  one or more fixes are missing from the binary
#   2  --running was set and the live PID is using a different binary
#   3  precondition error (app bundle missing, binary not executable, etc.)

set -euo pipefail

APP_PATH="${APP_PATH:-/Users/LelandDutcher/Developer/JuiceMount6/build/JuiceMount.app}"
CHECK_RUNNING=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --app)     APP_PATH="$2"; shift 2 ;;
        --running) CHECK_RUNNING=1; shift ;;
        -h|--help)
            grep '^#' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *) echo "unknown arg: $1" >&2; exit 3 ;;
    esac
done

YELLOW=$'\033[33m'
GREEN=$'\033[32m'
RED=$'\033[31m'
RESET=$'\033[0m'

pass() { printf '%s✓%s %s\n' "$GREEN" "$RESET" "$*"; }
fail() { printf '%s✗%s %s\n' "$RED" "$RESET" "$*"; }
info() { printf '  %s\n' "$*"; }

BINARY="$APP_PATH/Contents/MacOS/JuiceMount"
if [[ ! -f "$BINARY" ]]; then
    echo "ERROR: binary not found: $BINARY" >&2
    exit 3
fi

echo "verify-build"
info "binary:   $BINARY"
info "mtime:    $(stat -f '%Sm' "$BINARY")"
info "size:     $(du -h "$BINARY" | cut -f1)"
echo ""

# --- Fix manifest ---
# Each entry: SYMBOL_PATTERN | HUMAN_DESCRIPTION
# A fix is considered present if the binary's symbol table (`nm -a`)
# contains at least one match for the pattern. Add a new entry to
# this list whenever you ship a fix whose presence in the final
# binary should be verifiable. Removing an entry implies the fix is
# either rolled back or no longer needs verification.
#
# Only fixes with NON-INLINABLE symbols can be reliably detected:
# functions large enough to avoid the inliner, anonymous goroutine
# closures (always emitted as separate symbols), or named types. Go
# constants and small helpers vanish from the symbol table after
# inlining, so they can't be checked here. The verification is
# therefore a SAMPLING check, not exhaustive: if every entry passes,
# we're confident the build isn't stale; if one fails, the build is
# definitely stale.
declare -a FIXES=(
    "lstatNotExistWithTimeout|Lstat timeout in juiceFS.Stat (b1e9c6a, 2026-05-13)"
    "lstatNotExistWithTimeout.func1|Lstat timeout helper closure (same commit)"
    "internal/nfs.(\*conn).serve.gowrap1|Concurrent NFS dispatch goroutine in serve (691f550, 2026-05-16)"
    "RedisClient).RecentlyDegraded|Redis-health gate for phantom-purge + prune (bbc6bff, 2026-05-16)"
    "main.runWriteProbe|Write probe in self-test (8732f13, 2026-05-16)"
    "metadata.classifyConnErr|Network-vs-backend error classifier (e8aa5cb, 2026-05-16)"
    "health.(\*Reachability).probe|Reachability monitor (fd267b9, 2026-05-16)"
    "pin.SetAutoOffline|Auto-offline engage on network loss (10607ab, 2026-05-16)"
    "pin.ErrOfflineNotAvailable|Offline fail-fast sentinel for handler (54b744b, 2026-05-16)"
)

FAILURES=0
# nm -a output for a 15 MiB Go binary is multiple MiB of text — large
# enough that capturing it into a bash variable via $(...) can drop
# data on macOS. Dump to a temp file and grep it directly.
NM_TMP="$(mktemp -t verify-build.XXXXXX)"
trap 'rm -f "$NM_TMP"' EXIT
nm -a "$BINARY" > "$NM_TMP" 2>/dev/null || true

for entry in "${FIXES[@]}"; do
    IFS='|' read -r pattern desc <<< "$entry"
    if grep -q "$pattern" "$NM_TMP"; then
        pass "$desc"
    else
        fail "MISSING: $desc"
        fail "         expected symbol pattern: $pattern"
        FAILURES=$((FAILURES + 1))
    fi
done
echo ""

if [[ $FAILURES -gt 0 ]]; then
    fail "$FAILURES fix(es) missing from binary — likely a stale build."
    fail "Run: bash scripts/build-app.sh"
    fail "(That script forces SPM relink + libnfsd.a recreate. If a fix is"
    fail " STILL missing after a fresh build, the source may not actually"
    fail " contain it — grep the repo for the symbol name to confirm.)"
    exit 1
fi

# --- Optional running-process check ---
if [[ $CHECK_RUNNING -eq 1 ]]; then
    echo ""
    echo "checking running JuiceMount process..."
    PID=$(pgrep -f "$APP_PATH/Contents/MacOS/JuiceMount" 2>/dev/null | head -1 || true)
    if [[ -z "$PID" ]]; then
        info "${YELLOW}no JuiceMount process running — nothing to compare${RESET}"
        exit 0
    fi
    info "running PID: $PID"
    info "started:    $(ps -o lstart= -p "$PID" 2>/dev/null || echo unknown)"

    # macOS gives us /proc-equivalent info via lsof. The TXT line for
    # the process is the executable; comparing its inode to the .app
    # binary tells us if they're the same on-disk file (i.e., the
    # process is running THIS build vs. an older snapshot of the same
    # path that's been overwritten).
    DISK_INODE=$(stat -f '%i' "$BINARY")
    RUNNING_INODE=$(lsof -p "$PID" 2>/dev/null | awk '$4 == "txt" && $9 ~ /JuiceMount\/Contents\/MacOS\/JuiceMount/ {print $6; exit}' || true)

    if [[ -z "$RUNNING_INODE" ]]; then
        info "${YELLOW}could not determine running-binary inode via lsof${RESET}"
        info "fallback: comparing process start time vs binary mtime"
        PROC_START_EPOCH=$(date -j -f '%a %b %d %T %Y' "$(ps -o lstart= -p "$PID")" '+%s' 2>/dev/null || echo 0)
        BIN_MTIME_EPOCH=$(stat -f '%m' "$BINARY")
        if [[ "$PROC_START_EPOCH" -ge "$BIN_MTIME_EPOCH" ]]; then
            pass "process started after binary was built — likely current"
        else
            fail "process started BEFORE binary was last built — definitely stale"
            fail "kill the process and re-launch from the fresh .app"
            exit 2
        fi
    else
        if [[ "$DISK_INODE" == "$RUNNING_INODE" ]]; then
            pass "running process is using the on-disk binary"
        else
            fail "running process is using a DIFFERENT binary"
            fail "  disk inode:    $DISK_INODE"
            fail "  running inode: $RUNNING_INODE"
            fail "kill the process and re-launch from the fresh .app"
            exit 2
        fi
    fi
fi

echo ""
pass "build OK"
