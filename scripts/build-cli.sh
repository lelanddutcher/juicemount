#!/bin/bash
#
# build-cli.sh — build & codesign the standalone CLI binary `jm5`.
#
# Codesigning with the entitlements.plist (network client + server) lets the
# binary bypass macOS's 10GbE network filter, so it no longer needs the
# `ssh localhost` workaround to reach the NAS.
#
# Output: /tmp/jm5 (signed)

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

OUT="${1:-/tmp/jm5}"
ENT="$PROJECT_ROOT/entitlements.plist"

echo "==> Building CLI binary..."
go build -o "$OUT" ./cmd/jm5/
echo "    Built: $OUT ($(du -h "$OUT" | cut -f1))"

echo ""
echo "==> Codesigning with entitlements..."
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
echo "      --db /tmp/jm5-new.db \\"
echo "      --cache-size 100000"
