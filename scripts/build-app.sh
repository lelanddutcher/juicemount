#!/bin/bash
#
# build-app.sh — full build of the JuiceMount macOS menu bar app.
#
# Steps:
#   1. Rebuild the Go c-archive (libnfsd.a + libnfsd.h)
#   2. Build the Swift app via Swift Package Manager (release config)
#      2b. Build the QuickLook thumbnail appex (JuiceMountThumbnails)
#   3. Assemble the .app bundle with proper Info.plist and Resources
#      (including Contents/PlugIns/JuiceMountThumbnails.appex)
#   4. Codesign inside-out (Sparkle helpers, appex, then the app)
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
PLISTBUDDY="/usr/libexec/PlistBuddy"
JM_BUILD_VERSION="${JM_VERSION:-0.5.0}"
if [ -n "${JM_COMMIT:-}" ]; then
    JM_BUILD_COMMIT="$JM_COMMIT"
elif git rev-parse --verify HEAD >/dev/null 2>&1; then
    JM_BUILD_COMMIT="$(git rev-parse HEAD)"
else
    JM_BUILD_COMMIT="unknown"
fi
GO_VERSION_PACKAGE="github.com/lelanddutcher/juicemount/internal/version"
GO_BUILD_LDFLAGS="-X ${GO_VERSION_PACKAGE}.Version=${JM_BUILD_VERSION} -X ${GO_VERSION_PACKAGE}.Commit=${JM_BUILD_COMMIT}"

echo "==> JuiceMount Build"
echo "    config:    $SWIFT_CONFIG"
echo "    project:   $PROJECT_ROOT"
echo "    version:   $JM_BUILD_VERSION"
echo "    commit:    $JM_BUILD_COMMIT"
echo ""

# 1. Build the Go c-archive
echo "==> [1/4] Building Go c-archive (libnfsd.a)..."
mkdir -p "$BUILD_DIR"
# Force-rebuild the archive. Go's build cache is fine, but the output
# file path itself can confuse SPM (step 2) when only the .a contents
# changed under an unchanged mtime. Removing the .a/.h pair so the
# subsequent build creates fresh inodes.
rm -f "$BUILD_DIR/libnfsd.a" "$BUILD_DIR/libnfsd.h"
MACOSX_DEPLOYMENT_TARGET=14.0 \
CGO_CFLAGS="${CGO_CFLAGS:-} -mmacosx-version-min=14.0" \
CGO_LDFLAGS="${CGO_LDFLAGS:-} -mmacosx-version-min=14.0" \
CGO_ENABLED=1 go build \
    -ldflags "$GO_BUILD_LDFLAGS" \
    -buildmode=c-archive \
    -o "$BUILD_DIR/libnfsd.a" \
    ./bridge/
echo "    Built: $BUILD_DIR/libnfsd.a ($(du -h "$BUILD_DIR/libnfsd.a" | cut -f1))"

# 2. Build the Swift app
echo ""
echo "==> [2/4] Building Swift app via SPM ($SWIFT_CONFIG)..."
cd "$SWIFT_PKG"
# Wipe SPM's incremental build cache for this package. Otherwise SPM
# treats the previously-linked JuiceMount binary as up-to-date even
# when libnfsd.a (which provides ALL of the Go symbols Swift links
# against) has changed underneath it. Reproduced 2026-05-16: the
# overnight Lstat-timeout fix and the concurrent-dispatch fix both
# landed in libnfsd.a but were silently absent from the final binary
# until this rm forced a relink. Symptoms were "code review and tests
# pass but production behavior is stale." Painful to debug; cheap to
# prevent.
rm -rf "$SWIFT_PKG/.build/$SWIFT_CONFIG/JuiceMount"
# --product JuiceMount: the -Xlinker flags below (libnfsd.a, app frameworks,
# Sparkle rpath) belong to the app executable ONLY. The thumbnail appex is
# built in step 2b as a separate product so these flags never leak into its
# link (and its differing link flags never dirty the app's incremental build).
swift build -c "$SWIFT_CONFIG" --product JuiceMount \
    -Xlinker "-L$BUILD_DIR" \
    -Xlinker "-lnfsd" \
    -Xlinker "-framework" -Xlinker "CoreFoundation" \
    -Xlinker "-framework" -Xlinker "Security" \
    -Xlinker "-framework" -Xlinker "AppKit" \
    -Xlinker "-framework" -Xlinker "Quartz" \
    -Xlinker "-framework" -Xlinker "Carbon" \
    -Xlinker "-framework" -Xlinker "ServiceManagement" \
    -Xlinker "-rpath" -Xlinker "@executable_path/../Frameworks"
    # ^ rpath for the embedded Sparkle.framework. The binary references
    #   @rpath/Sparkle.framework/.../Sparkle, and the framework is copied to
    #   Contents/Frameworks/ in step 3. Without this rpath dyld only searches
    #   @loader_path (= Contents/MacOS) and the app crashes at launch with
    #   "Library not loaded: @rpath/Sparkle.framework". This is the standard
    #   bundled-framework rpath; SwiftPM does NOT add it for binary-artifact
    #   frameworks the way Xcode's "Embed Frameworks" phase would.

SWIFT_BIN="$SWIFT_PKG/.build/$SWIFT_CONFIG/JuiceMount"
if [ ! -f "$SWIFT_BIN" ]; then
    echo "ERROR: Swift binary not found at $SWIFT_BIN"
    exit 1
fi
echo "    Built: $SWIFT_BIN"

# 2b. QuickLook thumbnail appex executable. A tiny sandboxed XPC process that
# serves the farm-pre-rendered posters from the daemon's loopback endpoint
# (127.0.0.1:11050/thumb-local) so Finder never decodes remote video just to
# draw a thumbnail; on any miss/error it fails fast and macOS falls back to
# Apple's generator. Its link settings (QuickLookThumbnailing etc.) live in
# Package.swift — no -Xlinker flags here on purpose (see step 2 comment).
echo ""
echo "==> [2b] Building QuickLook thumbnail appex (JuiceMountThumbnails)..."
swift build -c "$SWIFT_CONFIG" --product JuiceMountThumbnails
APPEX_BIN="$SWIFT_PKG/.build/$SWIFT_CONFIG/JuiceMountThumbnails"
if [ ! -f "$APPEX_BIN" ]; then
    echo "ERROR: appex binary not found at $APPEX_BIN"
    exit 1
fi
echo "    Built: $APPEX_BIN"

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
"$PLISTBUDDY" -c "Set :CFBundleShortVersionString $JM_BUILD_VERSION" "$APP_DIR/Contents/Info.plist"
"$PLISTBUDDY" -c "Set :JMBuildCommit $JM_BUILD_COMMIT" "$APP_DIR/Contents/Info.plist"

# --- Icon rendering (Phase 3 identity) -------------------------------------
# Render the state-tinted citrus mark SVGs to menu-bar PNGs and the AppIcon
# iconset from logos/color.svg. Zero external deps: NSImage loads SVG on
# modern macOS, so scripts/svg2png.swift (compiled once here) does all the
# rasterizing. EVERY failure path falls back to the previous behavior
# (sips-resized logos/color.png) with a loud warning — the build must keep
# working on a machine without the SVGs, without swiftc, or with a rendering
# regression.
YELLOW=$'\033[33m'
RESET=$'\033[0m'

SVG2PNG_BIN=""
if [ -f "$PROJECT_ROOT/scripts/svg2png.swift" ] && command -v swiftc >/dev/null 2>&1; then
    if swiftc -O "$PROJECT_ROOT/scripts/svg2png.swift" -o "$BUILD_DIR/svg2png" 2>/dev/null; then
        SVG2PNG_BIN="$BUILD_DIR/svg2png"
    else
        echo "    ${YELLOW}WARNING: svg2png.swift failed to compile — falling back to PNG icon pipeline${RESET}"
    fi
else
    echo "    ${YELLOW}WARNING: svg2png.swift or swiftc unavailable — falling back to PNG icon pipeline${RESET}"
fi

render_png() {
    # render_png <in.svg> <out.png> <size>
    [ -n "$SVG2PNG_BIN" ] && "$SVG2PNG_BIN" "$1" "$2" "$3" >/dev/null 2>&1
}

# Menu-bar state icons: 18pt @1x + 36px @2x per state, loaded at runtime by
# MenuBarController (Contents/Resources/menubar/state-<state>[@2x].png).
# All-or-nothing: a partial set would mix logo states with the SF-Symbol
# fallback mid-session, so any failure removes the whole directory.
MENUBAR_OK=1
if [ -n "$SVG2PNG_BIN" ]; then
    mkdir -p "$APP_RES_DIR/menubar"
    for st in healthy degraded offline-files fault; do
        SVG="$PROJECT_ROOT/logos/state-$st.svg"
        if [ ! -f "$SVG" ] \
           || ! render_png "$SVG" "$APP_RES_DIR/menubar/state-$st.png" 18 \
           || ! render_png "$SVG" "$APP_RES_DIR/menubar/state-$st@2x.png" 36; then
            MENUBAR_OK=0
        fi
    done
else
    MENUBAR_OK=0
fi
if [ "$MENUBAR_OK" = 1 ]; then
    echo "    Menu-bar icons: 4 states rendered (18/36px) from logos/state-*.svg"
else
    rm -rf "$APP_RES_DIR/menubar"
    echo "    ${YELLOW}WARNING: menu-bar state icons NOT rendered — app falls back to SF-Symbol icons${RESET}"
fi

# App icon: full iconset (16..1024) rendered from logos/color.svg so every
# size is crisp vector output instead of a downsampled 512px PNG.
APPICON_OK=0
if [ -n "$SVG2PNG_BIN" ] && [ -f "$PROJECT_ROOT/logos/color.svg" ] && command -v iconutil >/dev/null 2>&1; then
    ICONSET="$BUILD_DIR/AppIcon.iconset"
    rm -rf "$ICONSET"
    mkdir -p "$ICONSET"
    APPICON_OK=1
    for size in 16 32 128 256 512; do
        render_png "$PROJECT_ROOT/logos/color.svg" "$ICONSET/icon_${size}x${size}.png" "$size" || APPICON_OK=0
        render_png "$PROJECT_ROOT/logos/color.svg" "$ICONSET/icon_${size}x${size}@2x.png" "$((size*2))" || APPICON_OK=0
    done
    if [ "$APPICON_OK" = 1 ] && iconutil -c icns "$ICONSET" -o "$APP_RES_DIR/AppIcon.icns" 2>/dev/null; then
        echo "    App icon: AppIcon.icns rendered from logos/color.svg (16..1024)"
    else
        APPICON_OK=0
    fi
fi
if [ "$APPICON_OK" != 1 ]; then
    echo "    ${YELLOW}WARNING: SVG app-icon render failed — using legacy logos/color.png pipeline${RESET}"
    # Legacy fallback: sips-resize logos/color.png into the iconset.
    if [ -f "$PROJECT_ROOT/logos/color.png" ]; then
        if command -v iconutil >/dev/null 2>&1 && command -v sips >/dev/null 2>&1; then
            ICONSET="$BUILD_DIR/AppIcon.iconset"
            rm -rf "$ICONSET"
            mkdir -p "$ICONSET"
            for size in 16 32 128 256 512; do
                sips -z $size $size "$PROJECT_ROOT/logos/color.png" --out "$ICONSET/icon_${size}x${size}.png" >/dev/null 2>&1 || true
                sips -z $((size*2)) $((size*2)) "$PROJECT_ROOT/logos/color.png" --out "$ICONSET/icon_${size}x${size}@2x.png" >/dev/null 2>&1 || true
            done
            iconutil -c icns "$ICONSET" -o "$APP_RES_DIR/AppIcon.icns" 2>/dev/null || \
                cp "$PROJECT_ROOT/logos/color.png" "$APP_RES_DIR/AppIcon.png"
        else
            cp "$PROJECT_ROOT/logos/color.png" "$APP_RES_DIR/AppIcon.png"
        fi
    fi
fi
# --- end icon rendering -----------------------------------------------------

# Copy any SPM-bundled resources
SPM_RES_BUNDLE="$SWIFT_PKG/.build/$SWIFT_CONFIG/JuiceMount_JuiceMount.bundle"
if [ -d "$SPM_RES_BUNDLE" ]; then
    cp -R "$SPM_RES_BUNDLE" "$APP_RES_DIR/"
fi

# --- Sparkle.framework embed ------------------------------------------------
# The Swift app links against Sparkle (auto-updater). SwiftPM ships it as a
# binary artifact (an .xcframework) rather than linking it statically, so the
# runtime framework must be copied into Contents/Frameworks/ or the app
# crashes at launch with a dyld "Library not loaded: @rpath/Sparkle.framework"
# error. The executable already carries an LC_RPATH of @executable_path/../
# Frameworks (SwiftPM adds it because the product depends on a framework
# artifact), so this is the canonical embed location.
#
# We pick the macOS slice out of the resolved .xcframework. The path is under
# .build/artifacts/sparkle/Sparkle/Sparkle.xcframework — found dynamically so a
# Sparkle version bump (which changes nothing here) doesn't break the build.
APP_FW_DIR="$APP_DIR/Contents/Frameworks"
SPARKLE_EMBEDDED=0
SPARKLE_XCFW="$(find "$SWIFT_PKG/.build/artifacts" -type d -name 'Sparkle.xcframework' 2>/dev/null | head -1)"
if [ -n "$SPARKLE_XCFW" ]; then
    # macOS slice dir name is e.g. macos-arm64_x86_64 — glob it so an arch
    # change in a future Sparkle build is tolerated.
    SPARKLE_FW_SRC="$(find "$SPARKLE_XCFW" -type d -name 'Sparkle.framework' -path '*macos*' 2>/dev/null | head -1)"
fi
if [ -n "${SPARKLE_FW_SRC:-}" ] && [ -d "$SPARKLE_FW_SRC" ]; then
    mkdir -p "$APP_FW_DIR"
    # -R preserves the Versions/Current symlink farm (required for a valid
    # versioned framework). Remove any prior copy first so a stale signature
    # never lingers across rebuilds.
    rm -rf "$APP_FW_DIR/Sparkle.framework"
    cp -R "$SPARKLE_FW_SRC" "$APP_FW_DIR/Sparkle.framework"
    SPARKLE_EMBEDDED=1
    echo "    Embedded: Sparkle.framework -> Contents/Frameworks/ (from $SPARKLE_FW_SRC)"
else
    echo "    ${YELLOW}WARNING: Sparkle.framework not found under .build/artifacts — auto-updater will crash at launch.${RESET}"
    echo "    ${YELLOW}         Run 'swift package resolve' in $SWIFT_PKG and rebuild.${RESET}"
fi
# --- end Sparkle.framework embed --------------------------------------------

# --- QuickLook thumbnail appex embed -----------------------------------------
# Contents/PlugIns/JuiceMountThumbnails.appex — the QuickLook thumbnail
# provider built in step 2b. This is the ONLY extension JuiceMount ships; the
# PlugIns guard at the end of this script enforces exactly that.
APPEX_DIR="$APP_DIR/Contents/PlugIns/JuiceMountThumbnails.appex"
mkdir -p "$APPEX_DIR/Contents/MacOS"
cp "$APPEX_BIN" "$APPEX_DIR/Contents/MacOS/JuiceMountThumbnails"
cp "$SWIFT_PKG/Resources/JuiceMountThumbnails-Info.plist" "$APPEX_DIR/Contents/Info.plist"
# Keep the appex version in lockstep with the app's (single source of truth:
# the app Info.plist already copied above) so a release bump can't drift the
# two apart — mismatched versions are a notarization/App-verify footgun.
APP_SHORT_VER="$($PLISTBUDDY -c 'Print :CFBundleShortVersionString' "$APP_DIR/Contents/Info.plist" 2>/dev/null || true)"
APP_BUNDLE_VER="$($PLISTBUDDY -c 'Print :CFBundleVersion' "$APP_DIR/Contents/Info.plist" 2>/dev/null || true)"
if [ -n "$APP_SHORT_VER" ]; then
    "$PLISTBUDDY" -c "Set :CFBundleShortVersionString $APP_SHORT_VER" "$APPEX_DIR/Contents/Info.plist"
fi
if [ -n "$APP_BUNDLE_VER" ]; then
    "$PLISTBUDDY" -c "Set :CFBundleVersion $APP_BUNDLE_VER" "$APPEX_DIR/Contents/Info.plist"
fi
"$PLISTBUDDY" -c "Add :JMBuildCommit string $JM_BUILD_COMMIT" "$APPEX_DIR/Contents/Info.plist"
echo "    Embedded: JuiceMountThumbnails.appex -> Contents/PlugIns/ (v${APP_SHORT_VER:-?})"
# --- end QuickLook thumbnail appex embed --------------------------------------

echo "    Bundle: $APP_DIR"

# 4. Codesign
echo ""
echo "==> [4/4] Codesigning..."
ENT_FILE="$PROJECT_ROOT/entitlements.plist"
[ -f "$ENT_FILE" ] || ENT_FILE=""

# Pick a signing identity. Order of preference:
#   1. JM_ADHOC=1 env var — force ad-hoc (fast, hang-free local dev path)
#   2. JM_SIGN_IDENTITY env var (cert hash or full name in quotes)
#   3. Any "Developer ID Application" cert in the user's keychain
#   4. Fall back to ad-hoc with a warning (daily-dev path).
#
# Why JM_ADHOC exists: an unnotarized "Developer ID Application" signature
# combined with the hardened runtime (--options runtime) makes macOS stall the
# FIRST launch of a freshly-built bundle for 30-60 s while syspolicyd performs
# an online notarization check that times out. During that window the process
# is parked in-kernel before main() runs (no logs, 0% CPU) — the "startup hang"
# that plagued the rebuild→launch dev loop. An ad-hoc signature skips that path
# entirely and launches instantly. Unnotarized Developer ID gives NO
# distribution benefit anyway (Gatekeeper rejects it on other Macs), so for
# local iteration ad-hoc is strictly better. Set JM_ADHOC=1 for dev builds;
# leave it unset (and provide a notary profile) for real release builds.
YELLOW=$'\033[33m'
RESET=$'\033[0m'

SIGN_IDENTITY=""
SIGN_IDENTITY_LABEL=""
if [ -n "${JM_ADHOC:-}" ]; then
    SIGN_IDENTITY="-"
    SIGN_IDENTITY_LABEL="ad-hoc (JM_ADHOC=1)"
    echo "    Signing ad-hoc (JM_ADHOC=1) — fast local-dev path, no Gatekeeper first-launch stall"
elif [ -n "${JM_SIGN_IDENTITY:-}" ]; then
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

# Hardened runtime (--options runtime) and --timestamp are for DISTRIBUTABLE
# (notarizable) builds and only work with a real identity. Ad-hoc dev builds
# skip both: hardened runtime gives no local benefit, --timestamp is rejected
# for ad-hoc, and (critically) hardened runtime is part of what makes an
# unnotarized bundle stall on first launch.
HARDEN_ARGS=()
if [ "$SIGN_IDENTITY" != "-" ]; then
    HARDEN_ARGS=(--options runtime --timestamp)
fi

# --- Inside-out codesign of the embedded Sparkle.framework ------------------
# We do NOT rely on `codesign --deep`: Apple deprecated it for signing and it
# does NOT correctly sign a framework that contains nested XPC services and a
# helper .app (Sparkle's Downloader.xpc / Installer.xpc / Updater.app /
# Autoupdate). Each nested code item must be signed FIRST, from the leaves up,
# THEN the framework, THEN the app. The framework's own bundles carry their own
# identifiers (org.sparkle-project.*) and embedded entitlements, so we sign
# them WITHOUT --entitlements (those are JuiceMount's app entitlements; forcing
# them onto Sparkle's helpers would be wrong and can break notarization).
# --force replaces Sparkle's shipped ad-hoc signatures with our Dev ID.
if [ "$SPARKLE_EMBEDDED" = 1 ]; then
    echo "    Signing embedded Sparkle.framework inside-out ($SIGN_IDENTITY_LABEL)..."
    FW="$APP_FW_DIR/Sparkle.framework"
    FW_VER="$FW/Versions/Current"   # symlink -> B (or A); codesign follows it
    # Leaves first: XPC services, then the Updater.app and its nested helper
    # executable, then the standalone Autoupdate helper. Order matters — a
    # parent bundle's seal includes its children's signatures.
    SPARKLE_NESTED=(
        "$FW_VER/XPCServices/Downloader.xpc"
        "$FW_VER/XPCServices/Installer.xpc"
        "$FW_VER/Updater.app/Contents/MacOS/Autoupdate"
        "$FW_VER/Updater.app"
        "$FW_VER/Autoupdate"
    )
    for item in "${SPARKLE_NESTED[@]}"; do
        if [ -e "$item" ]; then
            codesign --force "${HARDEN_ARGS[@]}" --sign "$SIGN_IDENTITY" "$item"
        fi
    done
    # Finally the framework bundle itself (seals the now-signed nested items).
    codesign --force "${HARDEN_ARGS[@]}" --sign "$SIGN_IDENTITY" "$FW"
fi
# --- end inside-out Sparkle signing -----------------------------------------

# --- QuickLook thumbnail appex signing ---------------------------------------
# Nested code signs BEFORE the app (inside-out), same rule as Sparkle above.
# Unlike Sparkle's helpers, the appex signs WITH its own entitlements file:
# app extensions MUST carry com.apple.security.app-sandbox (macOS refuses to
# host unsandboxed appexes) plus network.client for the loopback poster
# fetch. The app's entitlements.plist would be wrong here — do not reuse it.
APPEX_ENT="$SWIFT_PKG/Resources/JuiceMountThumbnails.entitlements"
if [ -d "$APPEX_DIR" ]; then
    echo "    Signing JuiceMountThumbnails.appex ($SIGN_IDENTITY_LABEL)..."
    codesign --force "${HARDEN_ARGS[@]}" --entitlements "$APPEX_ENT" --sign "$SIGN_IDENTITY" "$APPEX_DIR"
fi
# --- end appex signing --------------------------------------------------------

# Sign the app LAST (its seal now covers the already-signed framework and
# appex). No --deep: every nested code item is signed explicitly above, and
# --deep would re-sign Sparkle's helpers and the appex with the wrong (app)
# entitlements.
CS_ARGS=(--force "${HARDEN_ARGS[@]}" --sign "$SIGN_IDENTITY")
if [ -n "$ENT_FILE" ]; then
    CS_ARGS+=(--entitlements "$ENT_FILE")
fi

codesign "${CS_ARGS[@]}" "$APP_DIR"
codesign --verify --deep --strict --verbose=2 "$APP_DIR" 2>&1 | head -5 || true

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

# --- Sparkle appcast generation (release artifact step) ---------------------
# Produces build/sparkle-release/{JuiceMount.zip, appcast.xml}. These are the
# two files uploaded to the GitHub Release that SUFeedURL points at:
#   https://github.com/lelanddutcher/juicemount/releases/latest/download/appcast.xml
#
# generate_appcast signs the .zip with the EdDSA PRIVATE key from the login
# keychain (the same key whose PUBLIC half is in Info.plist) and writes the
# signature into appcast.xml. It MUST run after notarize+staple so the zipped
# app carries the stapled ticket (otherwise users updating get an un-notarized
# build). Gated behind JM_APPCAST=1 because it's a release-only step, and
# skipped unless the build was actually notarized.
if [ -n "${JM_APPCAST:-}" ]; then
    GEN_APPCAST="$(find "$SWIFT_PKG/.build/artifacts" -type f -name 'generate_appcast' 2>/dev/null | head -1)"
    if [ "$NOTARIZED" != "yes" ]; then
        echo "    ${YELLOW}WARNING: JM_APPCAST=1 but build was not notarized — refusing to publish an un-notarized update.${RESET}"
    elif [ -z "$GEN_APPCAST" ]; then
        echo "    ${YELLOW}WARNING: generate_appcast not found under .build/artifacts — run 'swift package resolve'.${RESET}"
    else
        echo ""
        echo "==> Generating Sparkle appcast"
        REL_DIR="$BUILD_DIR/sparkle-release"
        rm -rf "$REL_DIR"
        mkdir -p "$REL_DIR"
        # generate_appcast scans a directory of app archives; give it a clean
        # dir containing only this build's zip (stapled app).
        ditto -c -k --keepParent "$APP_DIR" "$REL_DIR/JuiceMount.zip"
        # --download-url-prefix makes the appcast's enclosure URL point at the
        # GitHub release asset path so clients fetch the zip from the right place.
        if "$GEN_APPCAST" \
              --download-url-prefix "https://github.com/lelanddutcher/juicemount/releases/latest/download/" \
              -o "$REL_DIR/appcast.xml" \
              "$REL_DIR"; then
            echo "    Appcast: $REL_DIR/appcast.xml"
            echo "    Archive: $REL_DIR/JuiceMount.zip"
            echo "    Upload BOTH to the GitHub Release tagged for this version."
        else
            echo "    ${YELLOW}WARNING: generate_appcast failed — see output above.${RESET}"
        fi
    fi
fi
# --- end appcast generation -------------------------------------------------

# Warn about the hang-prone combo: Developer ID signature + hardened runtime
# but NOT notarized. macOS stalls the first launch of such a bundle for
# 30-60 s (syspolicyd online notarization check that times out), which reads
# as a "startup hang" in the rebuild→launch dev loop. For local iteration,
# JM_ADHOC=1 sidesteps it entirely.
if [ "$SIGN_IDENTITY" != "-" ] && [ "$NOTARIZED" != "yes" ]; then
    echo "    ${YELLOW}WARNING: signed Developer ID but NOT notarized — this bundle may stall 30-60s on its"
    echo "             FIRST launch while Gatekeeper does an online check. For local dev rebuilds, set"
    echo "             JM_ADHOC=1 to sign ad-hoc and launch instantly.${RESET}"
fi

# Guard: Contents/PlugIns must contain EXACTLY JuiceMountThumbnails.appex —
# the QuickLook thumbnail provider — and nothing else.
#
# Why so strict: a Xcode-Debug build from an older JuiceMount project once
# registered a FileProviderExtension domain with macOS. The registration
# persists in fileproviderd's database FOREVER (even after the project is
# deleted), silently routes /Volumes/zpool file access through
# file-coordination arbitration, and pins filecoordinationd at 100%+ CPU.
# Recovery required Finder's privileged XPC removeDomain to dislodge -- see
# docs/no-fileprovider.md for the postmortem.
#
# JuiceMount serves files via NFS and FUSE and needs no FileProvider
# extension — ever. The ONE sanctioned extension is the QuickLook thumbnail
# appex (extension point com.apple.quicklook.thumbnail), whose registration
# is benign and user-visible in System Settings > Extensions. Anything else
# appearing here (even an empty stub) fails the build; adding a new extension
# means changing this guard intentionally and documenting its registration
# lifecycle.
PLUGINS_GUARD_FAIL=""
if [ ! -d "$APP_DIR/Contents/PlugIns/JuiceMountThumbnails.appex" ]; then
    PLUGINS_GUARD_FAIL="Contents/PlugIns/JuiceMountThumbnails.appex is missing (appex not assembled?)"
else
    UNEXPECTED_PLUGINS="$(find "$APP_DIR/Contents/PlugIns" -mindepth 1 -maxdepth 1 ! -name 'JuiceMountThumbnails.appex' -print 2>/dev/null)"
    if [ -n "$UNEXPECTED_PLUGINS" ]; then
        PLUGINS_GUARD_FAIL="unexpected item(s) in Contents/PlugIns: $UNEXPECTED_PLUGINS"
    else
        APPEX_POINT="$(/usr/libexec/PlistBuddy -c 'Print :NSExtension:NSExtensionPointIdentifier' \
            "$APP_DIR/Contents/PlugIns/JuiceMountThumbnails.appex/Contents/Info.plist" 2>/dev/null || true)"
        if [ "$APPEX_POINT" != "com.apple.quicklook.thumbnail" ]; then
            PLUGINS_GUARD_FAIL="JuiceMountThumbnails.appex extension point is '${APPEX_POINT:-<none>}' (want com.apple.quicklook.thumbnail)"
        fi
    fi
fi
if [ -n "$PLUGINS_GUARD_FAIL" ]; then
    echo ""
    echo "ERROR: PlugIns guard failed: $PLUGINS_GUARD_FAIL"
    echo "  Bundled extensions silently register with macOS on first launch and"
    echo "  can persist as ghost domains for the life of this Mac (a stray"
    echo "  FileProviderExtension once pinned filecoordinationd at 100% CPU --"
    echo "  see docs/no-fileprovider.md). Only the QuickLook thumbnail appex is"
    echo "  sanctioned; anything new needs an explicit guard change plus a"
    echo "  registration-lifecycle plan."
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
