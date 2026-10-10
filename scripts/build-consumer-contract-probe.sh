#!/usr/bin/env bash
# Build the acceptance probe against a named ClipLogger checkout, without
# editing it or using its SwiftPM build directory. No app or mount is launched.
set -euo pipefail
if [[ $# -ne 2 ]]; then
  printf 'usage: %s <cliplogger-checkout> <output-binary>\n' "$0" >&2
  exit 2
fi
consumer_root="$(cd "$1" && pwd)"
source_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
consumer_source="$consumer_root/app/Sources/ClipLoggerJuiceMount"
sources=()
for name in JuiceMountContract JuiceMountWire JuiceMountCapability JuiceMountTransport JuiceMountClient JuiceMountBlob; do
  sources+=("$consumer_source/$name.swift")
done
mkdir -p "$(dirname "$2")"
printf 'Consumer HEAD: %s\n' "$(git -C "$consumer_root" rev-parse HEAD)"
shasum -a 256 "${sources[@]}" "$source_root/test/consumer-contract-probe.swift"
compiler_options=(-swift-version 6 -parse-as-library)
if [[ -n "${CLANG_MODULE_CACHE_PATH:-}" ]]; then
  compiler_options+=(-module-cache-path "$CLANG_MODULE_CACHE_PATH")
fi
swiftc "${compiler_options[@]}" "${sources[@]}" \
  "$source_root/test/consumer-contract-probe.swift" -o "$2"
printf 'Probe: %s\n' "$2"
