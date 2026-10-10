#!/bin/bash
#
# build-cli.sh — build & codesign the standalone CLI binary `jm5`.
#
# Codesigning with the entitlements.plist (network client + server) lets the
# binary bypass macOS's 10GbE network filter, so it no longer needs the
# `ssh localhost` workaround to reach the NAS.
#
# Output: build/jm5, build/juicemount-mount-probe and build/juicefs (signed).
# jm5 runs only the juicefs beside it (the pinned, patched build; never PATH
# or Homebrew), so the bundled build is placed next to it.

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

OUT="${1:-$PROJECT_ROOT/build/jm5}"
mkdir -p "$(dirname "$OUT")"
PROBE="$(dirname "$OUT")/juicemount-mount-probe"
JUICEFS="$(dirname "$OUT")/juicefs"
ENT="$PROJECT_ROOT/entitlements.plist"

echo "==> Building CLI binary..."
CGO_CFLAGS="${CGO_CFLAGS:-} -mmacosx-version-min=26.0" \
CGO_LDFLAGS="${CGO_LDFLAGS:-} -mmacosx-version-min=26.0" \
MACOSX_DEPLOYMENT_TARGET=26.0 \
CGO_ENABLED=1 go build -trimpath -o "$OUT" ./cmd/jm5/
CGO_CFLAGS="${CGO_CFLAGS:-} -mmacosx-version-min=26.0" \
CGO_LDFLAGS="${CGO_LDFLAGS:-} -mmacosx-version-min=26.0" \
MACOSX_DEPLOYMENT_TARGET=26.0 \
CGO_ENABLED=1 go build -trimpath -o "$PROBE" ./cmd/juicemount-mount-probe/
echo "    Built: $OUT ($(du -h "$OUT" | cut -f1))"
"$PROJECT_ROOT/scripts/build-bundled-juicefs.sh" "$JUICEFS"

echo ""
echo "==> Codesigning with entitlements..."
codesign --force --sign - --options runtime "$PROBE"
codesign --verify --verbose=2 "$PROBE"
codesign --force --sign - --options runtime "$JUICEFS"
codesign --verify --verbose=2 "$JUICEFS"
codesign --force --sign - --entitlements "$ENT" --options runtime "$OUT"
codesign --verify --verbose=2 "$OUT" 2>&1 | head -3
echo ""
echo "==> Codesigned entitlements:"
codesign -d --entitlements - "$OUT" 2>&1 | grep -A1 "com.apple.security" | head -10

echo ""
echo "==> Done. Run directly (no SSH workaround needed):"
echo "    $OUT \\"
echo "      --redis redis://127.0.0.1:6379/1 \\"
echo "      --mount /Volumes/zpool \\"
echo "      --listen 127.0.0.1:11049 \\"
echo "      --db '$PROJECT_ROOT/build/jm5-new.db' \\"
echo "      --cache-size 100000"
