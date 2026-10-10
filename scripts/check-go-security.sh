#!/usr/bin/env bash
# Scan both shipped Go module graphs with the compiler used by the build.
# The bundled CLI has its own go.mod; scanning the root cannot cover it.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
export GOWORK=off GOTOOLCHAIN=local
python3 scripts/verify-juicefs-sdk.py
printf 'Security scan compiler: '
go version
go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...
tags="$(python3 -c 'import json; print(json.load(open("third_party/juicefs/SOURCE.json"))["binary"]["tags"])')"
cd third_party/juicefs
go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 -tags "$tags" .
