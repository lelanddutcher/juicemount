#!/bin/bash
#
# build-app.sh — full build of the JuiceMount macOS menu bar app.
#
# Steps:
#   1. Rebuild the Go c-archive (libnfsd.a + libnfsd.h)
#   2. Build the Swift app via Swift Package Manager (release config)
#   3. Assemble the .app bundle with proper Info.plist and Resources
#   4. Ad-hoc codesign so macOS allows it to run
#
# Output: build/JuiceMount.app
#
# Usage: ./scripts/build-app.sh [--release|--debug]

set -e

CONFIG="${1:---release}"
SWIFT_CONFIG="release"
if [ "$CONFIG" = "--debug" ]; then
    SWIFT_CONFIG="debug"
fi

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

BUILD_DIR="$PROJECT_ROOT/build"
APP_DIR="$BUILD_DIR/JuiceMount.app"
APP_BIN_DIR="$APP_DIR/Contents/MacOS"
APP_RES_DIR="$APP_DIR/Contents/Resources"
SWIFT_PKG="$PROJECT_ROOT/app/JuiceMount"

echo "==> JuiceMount Build"
echo "    config:    $SWIFT_CONFIG"
echo "    project:   $PROJECT_ROOT"
echo ""

# 1. Build the Go c-archive
echo "==> [1/4] Building Go c-archive (libnfsd.a)..."
mkdir -p "$BUILD_DIR"
CGO_ENABLED=1 go build \
    -buildmode=c-archive \
    -o "$BUILD_DIR/libnfsd.a" \
    ./bridge/
echo "    Built: $BUILD_DIR/libnfsd.a ($(du -h "$BUILD_DIR/libnfsd.a" | cut -f1))"

# 2. Build the Swift app
echo ""
echo "==> [2/4] Building Swift app via SPM ($SWIFT_CONFIG)..."
cd "$SWIFT_PKG"
swift build -c "$SWIFT_CONFIG" \
    -Xlinker "-L$BUILD_DIR" \
    -Xlinker "-lnfsd" \
    -Xlinker "-framework" -Xlinker "CoreFoundation" \
    -Xlinker "-framework" -Xlinker "Security" \
    -Xlinker "-framework" -Xlinker "AppKit" \
    -Xlinker "-framework" -Xlinker "Quartz" \
    -Xlinker "-framework" -Xlinker "Carbon" \
    -Xlinker "-framework" -Xlinker "ServiceManagement"

SWIFT_BIN="$SWIFT_PKG/.build/$SWIFT_CONFIG/JuiceMount"
if [ ! -f "$SWIFT_BIN" ]; then
    echo "ERROR: Swift binary not found at $SWIFT_BIN"
    exit 1
fi
echo "    Built: $SWIFT_BIN"

# 3. Assemble the .app bundle
echo ""
echo "==> [3/4] Assembling JuiceMount.app bundle..."
cd "$PROJECT_ROOT"
rm -rf "$APP_DIR"
mkdir -p "$APP_BIN_DIR" "$APP_RES_DIR"

# Copy executable
cp "$SWIFT_BIN" "$APP_BIN_DIR/JuiceMount"

# Copy Info.plist
cp "$SWIFT_PKG/Resources/Info.plist" "$APP_DIR/Contents/Info.plist"

# Copy app icon (use color logo for the dock-icon equivalent)
if [ -f "$PROJECT_ROOT/logos/color.png" ]; then
    # Convert PNG to .icns if iconutil is available, otherwise just copy
    if command -v iconutil >/dev/null 2>&1 && command -v sips >/dev/null 2>&1; then
        ICONSET="$BUILD_DIR/AppIcon.iconset"
        rm -rf "$ICONSET"
        mkdir -p "$ICONSET"
        for size in 16 32 64 128 256 512; do
            sips -z $size $size "$PROJECT_ROOT/logos/color.png" --out "$ICONSET/icon_${size}x${size}.png" >/dev/null 2>&1 || true
            sips -z $((size*2)) $((size*2)) "$PROJECT_ROOT/logos/color.png" --out "$ICONSET/icon_${size}x${size}@2x.png" >/dev/null 2>&1 || true
        done
        iconutil -c icns "$ICONSET" -o "$APP_RES_DIR/AppIcon.icns" 2>/dev/null || \
            cp "$PROJECT_ROOT/logos/color.png" "$APP_RES_DIR/AppIcon.png"
    else
        cp "$PROJECT_ROOT/logos/color.png" "$APP_RES_DIR/AppIcon.png"
    fi
fi

# Copy any SPM-bundled resources
SPM_RES_BUNDLE="$SWIFT_PKG/.build/$SWIFT_CONFIG/JuiceMount_JuiceMount.bundle"
if [ -d "$SPM_RES_BUNDLE" ]; then
    cp -R "$SPM_RES_BUNDLE" "$APP_RES_DIR/"
fi

echo "    Bundle: $APP_DIR"

# 4. Codesign
echo ""
echo "==> [4/4] Codesigning..."
ENT_FILE="$PROJECT_ROOT/entitlements.plist"
[ -f "$ENT_FILE" ] || ENT_FILE=""

# Pick a signing identity. Order of preference:
#   1. JM_SIGN_IDENTITY env var (cert hash or full name in quotes)
#   2. Any "Developer ID Application" cert in the user's keychain
#   3. Fall back to ad-hoc with a warning (daily-dev path).
YELLOW=$'\033[33m'
RESET=$'\033[0m'

SIGN_IDENTITY=""
SIGN_IDENTITY_LABEL=""
if [ -n "${JM_SIGN_IDENTITY:-}" ]; then
    SIGN_IDENTITY="$JM_SIGN_IDENTITY"
    SIGN_IDENTITY_LABEL="$JM_SIGN_IDENTITY"
    echo "    Signing with JM_SIGN_IDENTITY: $SIGN_IDENTITY_LABEL"
elif command -v security >/dev/null 2>&1; then
    # Parse `security find-identity` output. Each match looks like:
    #   1) ABCDEF0123... "Developer ID Application: Jane Doe (TEAMID12)"
    # Pull the first matching cert's quoted human-readable name.
    DEVID_LINE="$(security find-identity -p codesigning -v 2>/dev/null | grep "Developer ID Application" | head -1 || true)"
    if [ -n "$DEVID_LINE" ]; then
        # Strip everything up to the first quote and the trailing quote.
        SIGN_IDENTITY="$(printf '%s\n' "$DEVID_LINE" | sed -E 's/^[^"]*"([^"]+)".*$/\1/')"
        SIGN_IDENTITY_LABEL="$SIGN_IDENTITY"
        DEVID_COUNT="$(security find-identity -p codesigning -v 2>/dev/null | grep -c "Developer ID Application" || true)"
        if [ "${DEVID_COUNT:-0}" -gt 1 ]; then
            echo "    ${YELLOW}NOTE: ${DEVID_COUNT} Developer ID Application certs found; using first match.${RESET}"
            echo "    ${YELLOW}      Set JM_SIGN_IDENTITY=\"<full cert name or SHA1>\" to pick a specific one.${RESET}"
        fi
        echo "    Signing with Developer ID: $SIGN_IDENTITY_LABEL"
    else
        SIGN_IDENTITY="-"
        SIGN_IDENTITY_LABEL="ad-hoc"
        echo "    ${YELLOW}WARNING: no Developer ID Application cert found; signing ad-hoc (not distributable)${RESET}"
    fi
else
    SIGN_IDENTITY="-"
    SIGN_IDENTITY_LABEL="ad-hoc"
    echo "    ${YELLOW}WARNING: 'security' command not found; signing ad-hoc (not distributable)${RESET}"
fi

# Build the codesign argv. `--timestamp` is required for notarization but only
# works with a real identity (ad-hoc rejects it).
CS_ARGS=(--force --deep --sign "$SIGN_IDENTITY" --options runtime)
if [ "$SIGN_IDENTITY" != "-" ]; then
    CS_ARGS+=(--timestamp)
fi
if [ -n "$ENT_FILE" ]; then
    CS_ARGS+=(--entitlements "$ENT_FILE")
fi

codesign "${CS_ARGS[@]}" "$APP_DIR"
codesign --verify --verbose=2 "$APP_DIR" 2>&1 | head -3 || true

# Notarization, gated. Skipped on:
#   - JM_QUICK=1  (dev iteration; notarization is slow, 1-5 minutes)
#   - ad-hoc signature (Apple rejects ad-hoc submissions)
#   - missing notary credential profile
NOTARIZED="no"
STAPLED="skipped"
if [ -n "${JM_QUICK:-}" ]; then
    echo "    INFO: quick build (JM_QUICK=1) — skipping notarization"
elif [ "$SIGN_IDENTITY" = "-" ]; then
    echo "    INFO: ad-hoc signature — skipping notarization (not eligible)"
else
    NOTARY_PROFILE="${JM_NOTARY_PROFILE:-JuiceMount}"
    if xcrun notarytool history --keychain-profile "$NOTARY_PROFILE" >/dev/null 2>&1; then
        echo ""
        echo "==> Notarizing with profile: $NOTARY_PROFILE"
        NOTARY_ZIP="$BUILD_DIR/JuiceMount-notarize.zip"
        ditto -c -k --keepParent "$APP_DIR" "$NOTARY_ZIP"
        if xcrun notarytool submit "$NOTARY_ZIP" --keychain-profile "$NOTARY_PROFILE" --wait; then
            NOTARIZED="yes"
            echo "==> Stapling notarization ticket"
            if xcrun stapler staple "$APP_DIR" && xcrun stapler validate "$APP_DIR"; then
                STAPLED="yes"
            else
                STAPLED="no"
                echo "    ${YELLOW}WARNING: stapling failed; the notarization ticket exists at Apple but is not embedded in the app.${RESET}"
            fi
        else
            echo "    ${YELLOW}WARNING: notarization failed (see output above). App will work locally but Gatekeeper will warn on other Macs.${RESET}"
        fi
        rm -f "$NOTARY_ZIP"
    else
        echo "    INFO: no notary profile '$NOTARY_PROFILE' found in keychain — skipping notarization."
        echo "          To set up: xcrun notarytool store-credentials JuiceMount \\"
        echo "                       --apple-id <email> --team-id <team> --password <app-specific-password>"
    fi
fi

# Guard: refuse to ship a build that contains a FileProvider extension.
#
# Why: a Xcode-Debug build from an older JuiceMount project once registered
# a FileProviderExtension domain with macOS. The registration persists in
# fileproviderd's database FOREVER (even after the project is deleted),
# silently routes /Volumes/zpool file access through file-coordination
# arbitration, and pins filecoordinationd at 100%+ CPU. Recovery required
# Finder's privileged XPC removeDomain to dislodge -- see
# docs/no-fileprovider.md for the postmortem.
#
# JuiceMount serves files via NFS and FUSE. It does not need a FileProvider
# extension. If somebody adds one (even an empty stub), this guard fires.
if [ -d "$APP_DIR/Contents/PlugIns" ]; then
    echo ""
    echo "ERROR: build output contains $APP_DIR/Contents/PlugIns"
    echo "  Bundled extensions (especially FileProviderExtension) silently"
    echo "  register with macOS on first launch and can persist as ghost"
    echo "  domains for the life of this Mac. See docs/no-fileprovider.md."
    echo "  If a future architectural change genuinely needs an app extension,"
    echo "  remove this guard intentionally and document the removeDomain"
    echo "  lifecycle plan."
    exit 1
fi

echo ""
echo "==> Build complete"
echo "    App:        $APP_DIR"
echo "    Identity:   ${SIGN_IDENTITY_LABEL:-unknown}"
echo "    Notarized:  $NOTARIZED"
echo "    Staple:     $STAPLED"
echo "    Run:        open $APP_DIR"
echo "    Or:         $APP_BIN_DIR/JuiceMount"

# Distribution-readiness hint.
if [ "$SIGN_IDENTITY" = "-" ]; then
    echo ""
    echo "    ${YELLOW}NOTE: this is an ad-hoc build. It runs locally but other Macs will reject it.${RESET}"
    echo "    ${YELLOW}      See docs/signing.md for how to set up a Developer ID + notary profile.${RESET}"
elif [ "$NOTARIZED" != "yes" ] && [ -z "${JM_QUICK:-}" ]; then
    echo ""
    echo "    ${YELLOW}NOTE: build is signed but not notarized. Gatekeeper on other Macs may warn.${RESET}"
    echo "    ${YELLOW}      See docs/signing.md for notary profile setup.${RESET}"
fi
