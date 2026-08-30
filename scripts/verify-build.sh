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
#   1. The bundle and embedded Go binary carry the version and exact source
#      commit expected by the current checkout (or explicit arguments).
#
#   2. The .app binary on disk contains every fix this script knows
#      about (by symbol name). Each fix is identified by a symbol
#      that should exist in the final binary; if not, the build is
#      stale or the source no longer has that fix.
#
#   3. (Optional, --release) The bundle is distribution-ready: Sparkle is
#      embedded, all nested code is validly Developer-ID signed, Gatekeeper
#      accepts it as notarized, the ticket is stapled, and the signed update
#      archive/appcast contain the same version and commit.
#
#   4. (Optional, --running) Any currently-running JuiceMount process
#      is using THIS binary, not a stale one. Compares process binary
#      inode to the .app's binary inode.
#
# Usage:
#   scripts/verify-build.sh
#   scripts/verify-build.sh --app /path/to/JuiceMount.app
#   scripts/verify-build.sh --version 0.5.0 --commit <40-char-sha>
#   scripts/verify-build.sh --release --archive build/sparkle-release/JuiceMount.zip \
#       --appcast build/sparkle-release/appcast.xml
#   scripts/verify-build.sh --running   # also check the live process
#
# Exit codes:
#   0  every known fix is present in the binary; running PID (if checked) matches
#   1  one or more fixes are missing from the binary
#   2  --running was set and the live PID is using a different binary
#   3  precondition error (app bundle missing, binary not executable, etc.)

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
APP_PATH="${APP_PATH:-$PROJECT_ROOT/build/JuiceMount.app}"
CHECK_RUNNING=0
CHECK_RELEASE=0
EXPECTED_VERSION="${JM_VERSION:-}"
EXPECTED_COMMIT="${JM_COMMIT:-}"
ARCHIVE_PATH=""
APPCAST_PATH=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --app)      [[ $# -ge 2 ]] || { echo "--app requires a path" >&2; exit 3; }; APP_PATH="$2"; shift 2 ;;
        --version)  [[ $# -ge 2 ]] || { echo "--version requires a value" >&2; exit 3; }; EXPECTED_VERSION="$2"; shift 2 ;;
        --commit)   [[ $# -ge 2 ]] || { echo "--commit requires a value" >&2; exit 3; }; EXPECTED_COMMIT="$2"; shift 2 ;;
        --archive)  [[ $# -ge 2 ]] || { echo "--archive requires a path" >&2; exit 3; }; ARCHIVE_PATH="$2"; shift 2 ;;
        --appcast)  [[ $# -ge 2 ]] || { echo "--appcast requires a path" >&2; exit 3; }; APPCAST_PATH="$2"; shift 2 ;;
        --release)  CHECK_RELEASE=1; shift ;;
        --running)  CHECK_RUNNING=1; shift ;;
        -h|--help)
            grep '^#' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *) echo "unknown arg: $1" >&2; exit 3 ;;
    esac
done

# Source identity is the default expectation. This is deliberately stricter
# than the historical symbol-only verifier: running this script from a newer
# checkout against an older app must fail even when both binaries contain the
# same sampled functions.
if [[ -z "$EXPECTED_VERSION" ]]; then
    EXPECTED_VERSION="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleShortVersionString' \
        "$PROJECT_ROOT/app/JuiceMount/Resources/Info.plist" 2>/dev/null || true)"
fi
if [[ -z "$EXPECTED_COMMIT" ]] && git -C "$PROJECT_ROOT" rev-parse --verify HEAD >/dev/null 2>&1; then
    EXPECTED_COMMIT="$(git -C "$PROJECT_ROOT" rev-parse HEAD)"
fi

YELLOW=$'\033[33m'
GREEN=$'\033[32m'
RED=$'\033[31m'
RESET=$'\033[0m'

pass() { printf '%s✓%s %s\n' "$GREEN" "$RESET" "$*"; }
fail() { printf '%s✗%s %s\n' "$RED" "$RESET" "$*"; }
info() { printf '  %s\n' "$*"; }

BINARY="$APP_PATH/Contents/MacOS/JuiceMount"
PLIST="$APP_PATH/Contents/Info.plist"
APPEX="$APP_PATH/Contents/PlugIns/JuiceMountThumbnails.appex"
APPEX_PLIST="$APPEX/Contents/Info.plist"
if [[ ! -f "$BINARY" ]]; then
    echo "ERROR: binary not found: $BINARY" >&2
    exit 3
fi
if [[ ! -f "$PLIST" ]]; then
    echo "ERROR: bundle plist not found: $PLIST" >&2
    exit 3
fi

echo "verify-build"
info "binary:   $BINARY"
info "mtime:    $(stat -f '%Sm' "$BINARY")"
info "size:     $(du -h "$BINARY" | cut -f1)"
echo ""

FAILURES=0
check_equal() {
    local label="$1" actual="$2" expected="$3"
    if [[ -n "$expected" && "$actual" == "$expected" ]]; then
        pass "$label: $actual"
    elif [[ -z "$expected" ]]; then
        fail "$label: no expected value was available"
        FAILURES=$((FAILURES + 1))
    else
        fail "$label: got '${actual:-<missing>}', expected '$expected'"
        FAILURES=$((FAILURES + 1))
    fi
}

APP_VERSION="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleShortVersionString' "$PLIST" 2>/dev/null || true)"
APP_COMMIT="$(/usr/libexec/PlistBuddy -c 'Print :JMBuildCommit' "$PLIST" 2>/dev/null || true)"
check_equal "bundle version" "$APP_VERSION" "$EXPECTED_VERSION"
check_equal "bundle commit" "$APP_COMMIT" "$EXPECTED_COMMIT"

if [[ -f "$APPEX_PLIST" ]]; then
    APPEX_VERSION="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleShortVersionString' "$APPEX_PLIST" 2>/dev/null || true)"
    APPEX_COMMIT="$(/usr/libexec/PlistBuddy -c 'Print :JMBuildCommit' "$APPEX_PLIST" 2>/dev/null || true)"
    check_equal "QuickLook extension version" "$APPEX_VERSION" "$EXPECTED_VERSION"
    check_equal "QuickLook extension commit" "$APPEX_COMMIT" "$EXPECTED_COMMIT"
else
    fail "QuickLook extension plist is missing"
    FAILURES=$((FAILURES + 1))
fi

# The plist alone is insufficient: the exact commit must also be present in
# the linked Go archive inside the final Swift executable. This catches a
# correctly stamped plist wrapped around a stale SPM-linked libnfsd.a.
if [[ -n "$EXPECTED_COMMIT" ]] && LC_ALL=C grep -aFq "$EXPECTED_COMMIT" "$BINARY"; then
    pass "embedded Go commit: $EXPECTED_COMMIT"
else
    fail "embedded Go commit is missing from the final executable"
    FAILURES=$((FAILURES + 1))
fi
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
    "main.handleStopHTTP|POST /stop admin endpoint (ba47621, 2026-05-17)"
    "main.stopInProgress|/stop concurrent-POST gate (same commit)"
    "main.NFSServerStopMount|Middle-ground stop semantic for QA-7 (2026-05-17)"
    "main.handleCacheClearHTTP|POST /cache-clear admin endpoint for QA-3 (d09be09, 2026-05-17)"
    "main.prepareLinkStateDir|Pairing-specific Link identity migration for reliable re-pairing (RC 0.5)"
    "nfs.recoverLinkNoState|Saved-profile Link recovery without one-time-key replay (RC 0.5)"
)

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
    fail "$FAILURES build verification check(s) failed — artifact is stale or incomplete."
    fail "Run: bash scripts/build-app.sh"
    fail "(That script forces SPM relink + libnfsd.a recreate. If a fix is"
    fail " STILL missing after a fresh build, the source may not actually"
    fail " contain it — grep the repo for the symbol name to confirm.)"
    exit 1
fi

# --- Distribution release checks ---
if [[ $CHECK_RELEASE -eq 1 ]]; then
    echo ""
    echo "checking distribution release requirements..."

    if [[ ! "$EXPECTED_COMMIT" =~ ^[0-9a-f]{40}$ ]]; then
        fail "release commit must be a full lowercase 40-character git SHA"
        FAILURES=$((FAILURES + 1))
    fi

    SPARKLE_FW="$APP_PATH/Contents/Frameworks/Sparkle.framework"
    if [[ -d "$SPARKLE_FW" ]] && otool -L "$BINARY" | grep -Fq '@rpath/Sparkle.framework/'; then
        pass "Sparkle.framework is embedded and linked"
    else
        fail "Sparkle.framework is missing or not linked by the executable"
        FAILURES=$((FAILURES + 1))
    fi

    if codesign --verify --deep --strict --verbose=2 "$APP_PATH" >/dev/null 2>&1; then
        pass "strict nested code-signature verification"
    else
        fail "strict nested code-signature verification"
        FAILURES=$((FAILURES + 1))
    fi

    SIGN_DETAIL="$(codesign -d --verbose=4 "$APP_PATH" 2>&1 || true)"
    if grep -Fq 'Authority=Developer ID Application:' <<< "$SIGN_DETAIL" \
       && grep -Eq '^TeamIdentifier=[A-Z0-9]+$' <<< "$SIGN_DETAIL"; then
        pass "Developer ID Application identity and Team ID"
    else
        fail "bundle is not signed with a Developer ID Application identity"
        FAILURES=$((FAILURES + 1))
    fi

    SPCTL_DETAIL="$(spctl --assess --type execute --verbose=4 "$APP_PATH" 2>&1 || true)"
    if grep -Fq 'accepted' <<< "$SPCTL_DETAIL" && grep -Fq 'source=Notarized Developer ID' <<< "$SPCTL_DETAIL"; then
        pass "Gatekeeper accepts notarized Developer ID bundle"
    else
        fail "Gatekeeper did not accept the bundle as Notarized Developer ID"
        info "$SPCTL_DETAIL"
        FAILURES=$((FAILURES + 1))
    fi

    if xcrun stapler validate "$APP_PATH" >/dev/null 2>&1; then
        pass "notarization ticket is stapled"
    else
        fail "notarization ticket is not stapled or is invalid"
        FAILURES=$((FAILURES + 1))
    fi

    if [[ -z "$ARCHIVE_PATH" || ! -f "$ARCHIVE_PATH" ]]; then
        fail "--release requires an existing --archive"
        FAILURES=$((FAILURES + 1))
    else
        ARCHIVE_TMP="$(mktemp -d -t verify-build-archive.XXXXXX)"
        trap 'rm -f "$NM_TMP"; rm -rf "${ARCHIVE_TMP:-}"' EXIT
        if ditto -x -k "$ARCHIVE_PATH" "$ARCHIVE_TMP" >/dev/null 2>&1; then
            ARCHIVE_PLIST="$ARCHIVE_TMP/JuiceMount.app/Contents/Info.plist"
            ARCHIVE_VERSION="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleShortVersionString' "$ARCHIVE_PLIST" 2>/dev/null || true)"
            ARCHIVE_COMMIT="$(/usr/libexec/PlistBuddy -c 'Print :JMBuildCommit' "$ARCHIVE_PLIST" 2>/dev/null || true)"
            check_equal "update archive version" "$ARCHIVE_VERSION" "$EXPECTED_VERSION"
            check_equal "update archive commit" "$ARCHIVE_COMMIT" "$EXPECTED_COMMIT"
            pass "update archive SHA-256: $(shasum -a 256 "$ARCHIVE_PATH" | awk '{print $1}')"
        else
            fail "update archive could not be extracted"
            FAILURES=$((FAILURES + 1))
        fi
    fi

    if [[ -z "$APPCAST_PATH" || ! -f "$APPCAST_PATH" ]]; then
        fail "--release requires an existing --appcast"
        FAILURES=$((FAILURES + 1))
    elif grep -Fq 'sparkle:edSignature=' "$APPCAST_PATH" \
         && grep -Fq "$EXPECTED_VERSION" "$APPCAST_PATH"; then
        pass "Sparkle appcast carries an EdDSA signature and expected version"
        pass "appcast SHA-256: $(shasum -a 256 "$APPCAST_PATH" | awk '{print $1}')"
    else
        fail "Sparkle appcast is unsigned or does not contain the expected version"
        FAILURES=$((FAILURES + 1))
    fi

    if [[ $FAILURES -gt 0 ]]; then
        fail "$FAILURES distribution release check(s) failed"
        exit 1
    fi
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
