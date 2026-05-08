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
if [ -f "$ENT_FILE" ]; then
    codesign --force --deep --sign - --entitlements "$ENT_FILE" --options runtime "$APP_DIR" 2>&1 | head -5 || \
        codesign --force --deep --sign - "$APP_DIR" 2>&1 | head -5
else
    codesign --force --deep --sign - "$APP_DIR"
fi
codesign --verify --verbose=2 "$APP_DIR" 2>&1 | head -3 || true

echo ""
echo "==> Build complete"
echo "    App:  $APP_DIR"
echo "    Run:  open $APP_DIR"
echo "    Or:   $APP_BIN_DIR/JuiceMount"
