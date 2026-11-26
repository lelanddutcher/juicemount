#!/usr/bin/env bash
# build.sh OUTDIR [GOOS/GOARCH...] — build engineprobe with CGO_ENABLED=0.
#
# The engine's compression packages (DataDog/zstd, hungys/go-lz4) need cgo.
# The lab and laptop volumes are not compressed, so the probe is built
# without cgo: the workspace file nocgo/go.work replaces those two modules
# with stubs (nocgo/zstd, nocgo/lz4) for this build only. Nothing in
# third_party/juicefs changes. The probe refuses a compressed volume.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
OUT="${1:?usage: build.sh OUTDIR [GOOS/GOARCH...]}"; shift
TARGETS=("$@"); [ ${#TARGETS[@]} -gt 0 ] || TARGETS=(linux/amd64 darwin/arm64)
mkdir -p "$OUT"
cd "$ROOT"
export GOWORK="$HERE/nocgo/go.work"
TAGS=nosqlite,nomysql,nopg,notikv,nobadger,noetcd,nocos,nobos,nohdfs,noibmcos,noobs,nooss,noqingstor,noswift,noazure,nogs,noufile,nodragonfly
for t in "${TARGETS[@]}"; do
  os="${t%/*}" arch="${t#*/}"
  CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -trimpath -tags "$TAGS" \
    -o "$OUT/engineprobe-$os-$arch" ./test/remote-lab/engineprobe
  echo "$OUT/engineprobe-$os-$arch"
done
