#!/bin/bash
#
# build-bundled-juicefs.sh OUT — build the pinned, patched JuiceFS to OUT.
#
# The runtime runs only a juicefs beside its own executable (never PATH or
# Homebrew), and this build's signal handlers never unmount by path
# (third_party/juicefs/patches/0005). Used by build-app.sh and build-cli.sh.
#
# 1. scripts/verify-juicefs-sdk.py proves third_party/juicefs matches
#    SOURCE.json and its recorded patches (against the upstream module ZIP
#    when it is in the module cache).
# 2. Build from the nested module, so its reviewed go.mod/go.sum choose the
#    security-patched dependencies; tags and ldflags come from SOURCE.json "binary".
# 3. Refuse a binary whose --version is not SOURCE.json binary.version_output.
#
# Fails closed: there is no fallback binary. Signing is the caller's job.
# GO_BUILD_VCS_ARGS (e.g. -buildvcs=false) is honored when exported.

set -eo pipefail

OUT="${1:?usage: build-bundled-juicefs.sh OUT}"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JUICEFS_SRC="$PROJECT_ROOT/third_party/juicefs"

JUICEFS_ARCHIVE="$(go env GOMODCACHE)/cache/download/github.com/juicedata/juicefs/@v/v1.3.1.zip"
VERIFY_ARGS=()
if [ -f "$JUICEFS_ARCHIVE" ]; then
    VERIFY_ARGS=(--archive "$JUICEFS_ARCHIVE")
else
    echo "    WARNING: upstream JuiceFS archive not cached; verifying recorded hashes only" >&2
fi
/usr/bin/python3 -I -B "$PROJECT_ROOT/scripts/verify-juicefs-sdk.py" "${VERIFY_ARGS[@]}" >/dev/null || {
    echo "ERROR: third_party/juicefs does not match SOURCE.json and its patches (scripts/verify-juicefs-sdk.py)" >&2
    exit 1
}

pin() {
    /usr/bin/python3 -I -B -c 'import json,sys; print(json.load(open(sys.argv[1]))["binary"][sys.argv[2]])' \
        "$JUICEFS_SRC/SOURCE.json" "$1"
}
TAGS="$(pin tags)"
LDFLAGS="$(pin ldflags)"
EXPECTED="$(pin version_output)"
read -r -a VCS_ARGS <<< "${GO_BUILD_VCS_ARGS:-}"

echo "    Building bundled JuiceFS ($EXPECTED)..."
START=$SECONDS
mkdir -p "$(dirname "$OUT")"
(
    cd "$JUICEFS_SRC"
    MACOSX_DEPLOYMENT_TARGET=26.0 \
    CGO_CFLAGS="${CGO_CFLAGS:-} -mmacosx-version-min=26.0" \
    CGO_LDFLAGS="${CGO_LDFLAGS:-} -mmacosx-version-min=26.0" \
    CGO_ENABLED=1 GOWORK=off go build "${VCS_ARGS[@]}" -trimpath -tags "$TAGS" -ldflags "$LDFLAGS" -o "$OUT" .
) || {
    echo "ERROR: bundled JuiceFS build failed; there is no other JuiceFS to run." >&2
    echo "       Its Go modules must be in the module cache (cd third_party/juicefs && go mod download)." >&2
    exit 1
}
ACTUAL="$("$OUT" --version)"
if [ "$ACTUAL" != "$EXPECTED" ]; then
    echo "ERROR: bundled JuiceFS reports '$ACTUAL', expected '$EXPECTED'" >&2
    exit 1
fi
echo "    Built: $OUT ($(du -h "$OUT" | cut -f1), $((SECONDS - START))s)"
