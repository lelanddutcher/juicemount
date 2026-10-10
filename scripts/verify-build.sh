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
#   2. The bundled service binary contains every engine fix this script
#      knows about (by symbol name), and its own Version/Commit symbols
#      match the expected source. If a sampled fix is absent, the build is
#      stale or the source no longer has that fix.
#
#   3. Both bundle plists and every slice of each first-party executable
#      set the minimum to exactly macOS 26.0, including the update archive on --release.
#
#   4. (Optional, --release) The bundle is distribution-ready: Sparkle is
#      embedded, all nested code is validly Developer-ID signed, Gatekeeper
#      accepts it as notarized, the ticket is stapled, and the signed update
#      archive/appcast contain the same version, build number, and commit.
#
#   5. (Optional, --running) Any currently-running JuiceMount process
#      is using THIS binary, not a stale one. Compares process binary
#      inode to the .app's binary inode.
#
# Usage:
#   scripts/verify-build.sh
#   scripts/verify-build.sh --app /path/to/JuiceMount.app
#   scripts/verify-build.sh --version 0.5.0 --build-number 14 --commit <40-char-sha>
#   scripts/verify-build.sh --release --archive build/sparkle-release/JuiceMount.zip \
#       --appcast build/sparkle-release/appcast.xml
#   scripts/verify-build.sh --running   # also check the live process
#
# Exit codes:
#   0  every known fix is present in the service; running PID (if checked) matches
#   1  one or more fixes or service identity checks failed
#   2  --running was set and the live PID is using a different binary
#   3  precondition error (app bundle missing, binary not executable, etc.)

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
APP_PATH="${APP_PATH:-$PROJECT_ROOT/build/JuiceMount.app}"
CHECK_RUNNING=0
CHECK_RELEASE=0
EXPECTED_VERSION="${JM_VERSION:-}"
EXPECTED_BUILD_NUMBER="${JM_BUILD_NUMBER:-}"
EXPECTED_COMMIT="${JM_COMMIT:-}"
ARCHIVE_PATH=""
APPCAST_PATH=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --app)      [[ $# -ge 2 ]] || { echo "--app requires a path" >&2; exit 3; }; APP_PATH="$2"; shift 2 ;;
        --version)  [[ $# -ge 2 ]] || { echo "--version requires a value" >&2; exit 3; }; EXPECTED_VERSION="$2"; shift 2 ;;
        --build-number) [[ $# -ge 2 ]] || { echo "--build-number requires a value" >&2; exit 3; }; EXPECTED_BUILD_NUMBER="$2"; shift 2 ;;
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

if [[ $CHECK_RELEASE -eq 1 ]] && {
    [[ ! "$EXPECTED_BUILD_NUMBER" =~ ^[1-9][0-9]*$ ]] ||
    (( ${#EXPECTED_BUILD_NUMBER} > 15 )) ||
    (( EXPECTED_BUILD_NUMBER <= 13 ));
}; then
    echo "ERROR: --release requires --build-number (or JM_BUILD_NUMBER) as a decimal integer greater than 13" >&2
    exit 3
fi

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
SERVICE_BINARY="$APP_PATH/Contents/MacOS/juicemount-service"
MOUNT_PROBE="$APP_PATH/Contents/MacOS/juicemount-mount-probe"
PLIST="$APP_PATH/Contents/Info.plist"
APPEX="$APP_PATH/Contents/PlugIns/JuiceMountThumbnails.appex"
APPEX_PLIST="$APPEX/Contents/Info.plist"
for executable in "$BINARY" "$SERVICE_BINARY" "$MOUNT_PROBE"; do
    if [[ ! -f "$executable" || ! -x "$executable" ]]; then
        echo "ERROR: required executable missing or not executable: $executable" >&2
        exit 3
    fi
done
if [[ ! -f "$PLIST" ]]; then
    echo "ERROR: bundle plist not found: $PLIST" >&2
    exit 3
fi

echo "verify-build"
info "binary:   $BINARY"
info "service:  $SERVICE_BINARY"
info "probe:    $MOUNT_PROBE"
info "mtime:    $(stat -f '%Sm' "$BINARY")"
info "size:     $(du -h "$BINARY" | cut -f1)"
echo ""

NM_TMP=""
ARCHIVE_TMP=""
trap 'rm -f "${NM_TMP:-}"; rm -rf "${ARCHIVE_TMP:-}"' EXIT

FAILURES=0
if python3 -I -B "$PROJECT_ROOT/scripts/verify-macos26-artifact.py" "$APP_PATH"; then
    pass "packaged first-party code has minimum macOS 26.0"
else
    fail "packaged minimum macOS 26.0 is missing, malformed, or wrong"
    FAILURES=$((FAILURES + 1))
fi
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
APP_BUILD_NUMBER="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleVersion' "$PLIST" 2>/dev/null || true)"
APP_COMMIT="$(/usr/libexec/PlistBuddy -c 'Print :JMBuildCommit' "$PLIST" 2>/dev/null || true)"
check_equal "bundle version" "$APP_VERSION" "$EXPECTED_VERSION"
if [[ -n "$EXPECTED_BUILD_NUMBER" ]]; then
    check_equal "bundle build number" "$APP_BUILD_NUMBER" "$EXPECTED_BUILD_NUMBER"
fi
check_equal "bundle commit" "$APP_COMMIT" "$EXPECTED_COMMIT"

if [[ -f "$APPEX_PLIST" ]]; then
    APPEX_VERSION="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleShortVersionString' "$APPEX_PLIST" 2>/dev/null || true)"
    APPEX_BUILD_NUMBER="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleVersion' "$APPEX_PLIST" 2>/dev/null || true)"
    APPEX_COMMIT="$(/usr/libexec/PlistBuddy -c 'Print :JMBuildCommit' "$APPEX_PLIST" 2>/dev/null || true)"
    check_equal "QuickLook extension version" "$APPEX_VERSION" "$EXPECTED_VERSION"
    check_equal "QuickLook extension build number" "$APPEX_BUILD_NUMBER" "$APP_BUILD_NUMBER"
    check_equal "QuickLook extension commit" "$APPEX_COMMIT" "$EXPECTED_COMMIT"
else
    fail "QuickLook extension plist is missing"
    FAILURES=$((FAILURES + 1))
fi

# Only build-app.sh --distribution stamps JMBuildChannel=distribution. A dev
# bundle can carry a Developer ID signature, a notarization ticket, and a build
# number above 13, so the channel is the artifact-level proof that it came
# through the gated shipping path.
APP_CHANNEL="$(/usr/libexec/PlistBuddy -c 'Print :JMBuildChannel' "$PLIST" 2>/dev/null || true)"
APPEX_CHANNEL="$(/usr/libexec/PlistBuddy -c 'Print :JMBuildChannel' "$APPEX_PLIST" 2>/dev/null || true)"
if [[ $CHECK_RELEASE -eq 1 ]]; then
    check_equal "bundle build channel" "$APP_CHANNEL" "distribution"
    check_equal "QuickLook extension build channel" "$APPEX_CHANNEL" "distribution"
else
    info "build channel: ${APP_CHANNEL:-<unstamped>}"
fi

# The update archive is what Sparkle actually installs. Its identity is checked
# here, beside the bundle identity, so it is evaluated even when a later
# symbol check fails.
if [[ $CHECK_RELEASE -eq 1 ]]; then
    if [[ -z "$ARCHIVE_PATH" || ! -f "$ARCHIVE_PATH" ]]; then
        fail "--release requires an existing --archive"
        FAILURES=$((FAILURES + 1))
    else
        ARCHIVE_TMP="$(mktemp -d -t verify-build-archive.XXXXXX)"
        if ditto -x -k "$ARCHIVE_PATH" "$ARCHIVE_TMP" >/dev/null 2>&1; then
            if python3 -I -B "$PROJECT_ROOT/scripts/verify-macos26-artifact.py" "$ARCHIVE_TMP/JuiceMount.app"; then
                pass "update archive first-party code has minimum macOS 26.0"
            else
                fail "update archive minimum macOS 26.0 is missing, malformed, or wrong"
                FAILURES=$((FAILURES + 1))
            fi
            ARCHIVE_PLIST="$ARCHIVE_TMP/JuiceMount.app/Contents/Info.plist"
            ARCHIVE_VERSION="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleShortVersionString' "$ARCHIVE_PLIST" 2>/dev/null || true)"
            ARCHIVE_BUILD_NUMBER="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleVersion' "$ARCHIVE_PLIST" 2>/dev/null || true)"
            ARCHIVE_COMMIT="$(/usr/libexec/PlistBuddy -c 'Print :JMBuildCommit' "$ARCHIVE_PLIST" 2>/dev/null || true)"
            check_equal "update archive version" "$ARCHIVE_VERSION" "$EXPECTED_VERSION"
            check_equal "update archive build number" "$ARCHIVE_BUILD_NUMBER" "$EXPECTED_BUILD_NUMBER"
            check_equal "update archive commit" "$ARCHIVE_COMMIT" "$EXPECTED_COMMIT"
            ARCHIVE_CHANNEL="$(/usr/libexec/PlistBuddy -c 'Print :JMBuildChannel' "$ARCHIVE_PLIST" 2>/dev/null || true)"
            check_equal "update archive build channel" "$ARCHIVE_CHANNEL" "distribution"
            ARCHIVE_APPEX_PLIST="$ARCHIVE_TMP/JuiceMount.app/Contents/PlugIns/JuiceMountThumbnails.appex/Contents/Info.plist"
            ARCHIVE_APPEX_BUILD_NUMBER="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleVersion' "$ARCHIVE_APPEX_PLIST" 2>/dev/null || true)"
            check_equal "update archive extension build number" "$ARCHIVE_APPEX_BUILD_NUMBER" "$EXPECTED_BUILD_NUMBER"
            ARCHIVE_APPEX_CHANNEL="$(/usr/libexec/PlistBuddy -c 'Print :JMBuildChannel' "$ARCHIVE_APPEX_PLIST" 2>/dev/null || true)"
            check_equal "update archive extension build channel" "$ARCHIVE_APPEX_CHANNEL" "distribution"
            pass "update archive SHA-256: $(shasum -a 256 "$ARCHIVE_PATH" | awk '{print $1}')"
        else
            fail "update archive could not be extracted"
            FAILURES=$((FAILURES + 1))
        fi
    fi
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
if ! command -v python3 >/dev/null 2>&1; then
    fail "Python 3 is required for read-only service Mach-O stamp inspection"
    FAILURES=$((FAILURES + 1))
elif python3 "$PROJECT_ROOT/scripts/verify-service-stamp.py" "$SERVICE_BINARY" "$EXPECTED_VERSION" "$EXPECTED_COMMIT"; then
    pass "service Version/Commit symbols match expected source"
else
    fail "service Version/Commit symbols do not match expected source"
    FAILURES=$((FAILURES + 1))
fi

# --- Bundled JuiceFS ---
# The runtime runs only Contents/MacOS/juicefs (never PATH or Homebrew). It must
# be the pinned, patched build from third_party/juicefs, signed by the same
# identity as the app's main executable (which carries the bundle signature),
# and report the version pinned in SOURCE.json.
BUNDLED_JUICEFS="$APP_PATH/Contents/MacOS/juicefs"
EXPECTED_JUICEFS_VERSION="$(python3 -I -B -c 'import json,sys; print(json.load(open(sys.argv[1]))["binary"]["version_output"])' \
    "$PROJECT_ROOT/third_party/juicefs/SOURCE.json" 2>/dev/null || true)"
signing_identity() {
    codesign -d --verbose=2 "$1" 2>&1 | grep -E '^(Authority|TeamIdentifier)=|^Signature=adhoc$' || true
}
if [[ ! -f "$BUNDLED_JUICEFS" || ! -x "$BUNDLED_JUICEFS" ]]; then
    fail "bundled juicefs is missing or not executable: $BUNDLED_JUICEFS"
    FAILURES=$((FAILURES + 1))
else
    if codesign --verify --strict "$BUNDLED_JUICEFS" >/dev/null 2>&1; then
        JUICEFS_SIGNER="$(signing_identity "$BUNDLED_JUICEFS")"
        APP_SIGNER="$(signing_identity "$BINARY")"
        if [[ -n "$JUICEFS_SIGNER" && "$JUICEFS_SIGNER" == "$APP_SIGNER" ]]; then
            pass "bundled juicefs is signed with the bundle identity"
        else
            fail "bundled juicefs signer differs from the bundle: '${JUICEFS_SIGNER:-<none>}' vs '${APP_SIGNER:-<none>}'"
            FAILURES=$((FAILURES + 1))
        fi
    else
        fail "bundled juicefs signature is invalid"
        FAILURES=$((FAILURES + 1))
    fi
    check_equal "bundled juicefs version" "$("$BUNDLED_JUICEFS" --version 2>/dev/null || true)" "$EXPECTED_JUICEFS_VERSION"
fi
# Onboarding preflight compares the engine's --version with this plist key.
check_equal "Info.plist bundled juicefs version" \
    "$(/usr/libexec/PlistBuddy -c 'Print :JMBundledJuiceFSVersion' "$PLIST" 2>/dev/null || true)" "$EXPECTED_JUICEFS_VERSION"
# A jmdevjuicefs build admits any configured juicefs; it must never ship.
if ! command -v go >/dev/null 2>&1; then
    fail "go is required to read the service build tags (go version -m)"
    FAILURES=$((FAILURES + 1))
elif SERVICE_BUILD_INFO="$(go version -m "$SERVICE_BINARY" 2>/dev/null)" && [[ -n "$SERVICE_BUILD_INFO" ]]; then
    if grep -Eq -- '-tags=([^[:space:]]*,)?jmdevjuicefs(,|[[:space:]]|$)' <<< "$SERVICE_BUILD_INFO"; then
        fail "service was built with the jmdevjuicefs dev tag"
        FAILURES=$((FAILURES + 1))
    else
        pass "service has no jmdevjuicefs dev override"
    fi
else
    fail "service build information is unreadable"
    FAILURES=$((FAILURES + 1))
fi

# --- Privileged unmount helper (H-3) ---
# SMAppService.daemon needs the plist in Contents/Library/LaunchDaemons and the
# program at its BundleProgram. The helper admits a peer only if libxpc finds
# it satisfies the requirement compiled into the helper, so the strings, the
# bundle's actual signing identifiers and the build channel are checked
# together. A release bundle's own service and app must satisfy them.
UNMOUNTD="$APP_PATH/Contents/MacOS/juicemount-unmountd"
UNMOUNTD_PLIST="$APP_PATH/Contents/Library/LaunchDaemons/com.juicemount.unmountd.plist"
helper_fail() { fail "$*"; FAILURES=$((FAILURES + 1)); }
signing_identifier() { codesign -d --verbose=2 "$1" 2>&1 | sed -n 's/^Identifier=//p' | head -1; }
if [[ ! -f "$UNMOUNTD_PLIST" ]]; then
    helper_fail "unmount helper LaunchDaemon plist is missing: $UNMOUNTD_PLIST"
elif ! plutil -lint -s "$UNMOUNTD_PLIST" >/dev/null 2>&1; then
    helper_fail "unmount helper LaunchDaemon plist does not parse"
else
    helper_plist() { /usr/libexec/PlistBuddy -c "Print :$1" "$UNMOUNTD_PLIST" 2>/dev/null || true; }
    check_equal "helper plist Label" "$(helper_plist Label)" "com.juicemount.unmountd"
    check_equal "helper plist BundleProgram" "$(helper_plist BundleProgram)" "Contents/MacOS/juicemount-unmountd"
    check_equal "helper plist MachServices com.juicemount.unmountd" "$(helper_plist MachServices:com.juicemount.unmountd)" "true"
    check_equal "helper plist MachServices com.juicemount.unmountd.status" "$(helper_plist MachServices:com.juicemount.unmountd.status)" "true"
    check_equal "helper plist AssociatedBundleIdentifiers" "$(helper_plist AssociatedBundleIdentifiers:0)" \
        "$(/usr/libexec/PlistBuddy -c 'Print :CFBundleIdentifier' "$PLIST" 2>/dev/null || true)"
    check_equal "helper plist RunAtLoad" "$(helper_plist RunAtLoad)" "true"
    if [[ -n "$(helper_plist KeepAlive)" ]]; then
        helper_fail "helper plist sets KeepAlive (launchd must start it on demand and at load only)"
    else
        pass "helper plist has no KeepAlive"
    fi
fi
if [[ ! -f "$UNMOUNTD" || ! -x "$UNMOUNTD" ]]; then
    helper_fail "unmount helper is missing or not executable: $UNMOUNTD"
else
    if codesign --verify --strict "$UNMOUNTD" >/dev/null 2>&1; then
        HELPER_SIGNER="$(signing_identity "$UNMOUNTD")"
        if [[ -n "$HELPER_SIGNER" && "$HELPER_SIGNER" == "$(signing_identity "$BINARY")" ]]; then
            pass "unmount helper is signed with the bundle identity"
        else
            helper_fail "unmount helper signer differs from the bundle: '${HELPER_SIGNER:-<none>}'"
        fi
        check_equal "unmount helper signing identifier" "$(signing_identifier "$UNMOUNTD")" "com.juicemount.unmountd"
        if grep -q '^Signature=adhoc$' <<< "$HELPER_SIGNER"; then
            info "unmount helper is ad-hoc signed: no hardened runtime (dev build)"
        elif /bin/bash "$PROJECT_ROOT/scripts/verify-codesign-runtime.sh" "$UNMOUNTD"; then
            pass "unmount helper has the hardened runtime"
        else
            helper_fail "unmount helper is Developer-ID signed without the hardened runtime"
        fi
    else
        helper_fail "unmount helper signature is invalid"
    fi

    HELPER_REQS="$("$UNMOUNTD" -print-requirements 2>&1 || true)"
    HELPER_CHANNEL="$(sed -n 's/^channel=//p' <<< "$HELPER_REQS")"
    HELPER_SERVICE_REQ="$(sed -n 's/^service=//p' <<< "$HELPER_REQS")"
    HELPER_STATUS_REQ="$(sed -n 's/^status=//p' <<< "$HELPER_REQS")"
    if [[ $CHECK_RELEASE -eq 1 ]]; then
        check_equal "unmount helper peer requirement channel" "$HELPER_CHANNEL" "release"
    else
        info "unmount helper peer requirement channel: ${HELPER_CHANNEL:-<unreadable>}"
    fi
    SERVICE_SIGNING_ID="$(signing_identifier "$SERVICE_BINARY")"
    APP_SIGNING_ID="$(signing_identifier "$APP_PATH")"
    for req_name in service status; do
        if [[ $req_name == service ]]; then req="$HELPER_SERVICE_REQ"; else req="$HELPER_STATUS_REQ"; fi
        if [[ -z "$req" ]]; then
            helper_fail "unmount helper $req_name requirement is unreadable"
            continue
        fi
        if grep -Fq 'certificate leaf[subject.OU] = "PKUE74YS72"' <<< "$req" \
           && grep -Fq 'anchor apple generic' <<< "$req" \
           && grep -Fq '!(entitlement["com.apple.security.get-task-allow"] exists)' <<< "$req"; then
            pass "unmount helper $req_name requirement pins team PKUE74YS72 and rejects get-task-allow"
        else
            helper_fail "unmount helper $req_name requirement lacks the team/anchor/get-task-allow clauses: $req"
        fi
        if [[ $CHECK_RELEASE -eq 1 ]] && ! grep -Eq '(^| )notarized( |$)' <<< "$req"; then
            helper_fail "unmount helper $req_name requirement does not require notarization"
        fi
        if grep -Fq "identifier \"$SERVICE_SIGNING_ID\"" <<< "$req"; then
            pass "unmount helper $req_name requirement names the service's signing identifier ($SERVICE_SIGNING_ID)"
        else
            helper_fail "unmount helper $req_name requirement does not name the service's signing identifier '${SERVICE_SIGNING_ID:-<none>}'"
        fi
    done
    if grep -Fq "identifier \"$APP_SIGNING_ID\"" <<< "$HELPER_STATUS_REQ"; then
        pass "unmount helper status requirement names the app's signing identifier ($APP_SIGNING_ID)"
    else
        helper_fail "unmount helper status requirement does not name the app's signing identifier '${APP_SIGNING_ID:-<none>}'"
    fi
    if grep -Fq "identifier \"$APP_SIGNING_ID\"" <<< "$HELPER_SERVICE_REQ"; then
        helper_fail "unmount helper service requirement admits the GUI app (it may only call status)"
    fi
    # A jmdevunmountd helper admits unnotarized peers; it must never ship.
    if HELPER_BUILD_INFO="$(go version -m "$UNMOUNTD" 2>/dev/null)" && [[ -n "$HELPER_BUILD_INFO" ]]; then
        if grep -Eq -- '-tags=([^[:space:]]*,)?jmdevunmountd(,|[[:space:]]|$)' <<< "$HELPER_BUILD_INFO"; then
            if [[ $CHECK_RELEASE -eq 1 ]]; then
                helper_fail "unmount helper was built with the jmdevunmountd dev tag"
            else
                info "${YELLOW}unmount helper carries the jmdevunmountd dev requirement (refused by --release)${RESET}"
            fi
        else
            pass "unmount helper has no jmdevunmountd dev requirement"
        fi
    else
        helper_fail "unmount helper build information is unreadable"
    fi
    # The bundle's own peers, checked statically with the helper's compiled-in
    # strings (SecStaticCodeCheckValidity, the engine libxpc uses).
    for peer in "service:$SERVICE_BINARY" "status:$APP_PATH"; do
        role="${peer%%:*}"; path="${peer#*:}"
        if CHECK_OUT="$("$UNMOUNTD" -check-peer "$path" -check-peer-role "$role" 2>&1)"; then
            pass "bundle $role peer satisfies the helper requirement"
        elif [[ $CHECK_RELEASE -eq 1 ]]; then
            helper_fail "bundle $role peer does NOT satisfy the helper requirement: $CHECK_OUT"
        else
            info "${YELLOW}WARNING: bundle $role peer does not satisfy the helper requirement; the helper will refuse it.${RESET}"
            info "${YELLOW}         Expected for ad-hoc or unnotarized builds; use a notarized build or JM_UNMOUNTD_DEV=1 with Developer ID.${RESET}"
        fi
    done
fi
echo ""

# --- Fix manifest ---
# Each entry: SYMBOL_PATTERN | HUMAN_DESCRIPTION
# A fix is considered present if the service's symbol table (`nm -a`)
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
nm -a "$SERVICE_BINARY" > "$NM_TMP" 2>/dev/null || true

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

    if [[ -z "$APPCAST_PATH" || ! -f "$APPCAST_PATH" ]]; then
        fail "--release requires an existing --appcast"
        FAILURES=$((FAILURES + 1))
    else
        APPCAST_ITEM="/*[local-name()='rss']/*[local-name()='channel']/*[local-name()='item'][1]"
        APPCAST_VERSION="$(xmllint --xpath "string($APPCAST_ITEM/*[local-name()='shortVersionString'])" "$APPCAST_PATH" 2>/dev/null || true)"
        APPCAST_BUILD_NUMBER="$(xmllint --xpath "string($APPCAST_ITEM/*[local-name()='version'])" "$APPCAST_PATH" 2>/dev/null || true)"
        APPCAST_SIGNATURE="$(xmllint --xpath "string($APPCAST_ITEM/*[local-name()='enclosure']/@*[local-name()='edSignature'])" "$APPCAST_PATH" 2>/dev/null || true)"
        check_equal "Sparkle appcast version" "$APPCAST_VERSION" "$EXPECTED_VERSION"
        check_equal "Sparkle appcast build number" "$APPCAST_BUILD_NUMBER" "$EXPECTED_BUILD_NUMBER"
        if [[ -n "$APPCAST_SIGNATURE" ]]; then
            pass "Sparkle appcast carries an EdDSA signature"
        else
            fail "Sparkle appcast is missing its EdDSA signature"
            FAILURES=$((FAILURES + 1))
        fi
        pass "appcast SHA-256: $(shasum -a 256 "$APPCAST_PATH" | awk '{print $1}')"
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
