#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

OUTPUT_DIR="build"
mkdir -p "$OUTPUT_DIR"

echo "Building libnfsd.a (c-archive)..."
CGO_ENABLED=1 go build \
    -buildmode=c-archive \
    -o "$OUTPUT_DIR/libnfsd.a" \
    ./bridge/

echo "Built: $OUTPUT_DIR/libnfsd.a"
echo "Header: $OUTPUT_DIR/libnfsd.h"
ls -la "$OUTPUT_DIR/libnfsd.a" "$OUTPUT_DIR/libnfsd.h"
echo ""
echo "Exported symbols:"
grep "^extern" "$OUTPUT_DIR/libnfsd.h" | sed 's/^extern /  /'
