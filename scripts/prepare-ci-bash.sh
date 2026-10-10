#!/usr/bin/env bash
# Build a private Bash for Linux keyd tests. Bash 5.2.21 has an upstream
# trap/command-substitution parser defect; this does not establish the cause
# of any particular keyd TERM failure.
set -euo pipefail

: "${RUNNER_TEMP:?RUNNER_TEMP must be set by GitHub Actions}"
: "${GITHUB_ENV:?GITHUB_ENV must be set by GitHub Actions}"

version=5.3
expected_sha256=0d5cd86965f869a26cf64f4b71be7b96f90a3ba8b3d74e27e8e9d9d5550f31ba
work="$(mktemp -d "${RUNNER_TEMP}/jm-ci-bash-5.3.XXXXXXXX")"
chmod 700 "$work"
archive="$work/bash-${version}.tar.gz"

curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
  --max-time 180 "https://ftp.gnu.org/gnu/bash/bash-${version}.tar.gz" \
  --output "$archive"
printf '%s  %s\n' "$expected_sha256" "$archive" | sha256sum --check --status
tar -xzf "$archive" -C "$work"
rm -f "$archive"

source_dir="$work/bash-${version}"
(
  cd "$source_dir"
  ./configure --without-bash-malloc --disable-readline --disable-nls
  make -j2
)

binary="$source_dir/bash"
test -x "$binary"
selected_version="$("$binary" --version)"
selected_version="${selected_version%%$'\n'*}"
case "$selected_version" in
  'GNU bash, version 5.3.0('*')-release '*) ;;
  *) printf 'unexpected private Bash version: %s\n' "$selected_version" >&2; exit 1 ;;
esac
printf 'JM_TEST_KEYD_BASH=%s\n' "$binary" >> "$GITHUB_ENV"
printf 'keyd tests use %s at %s\n' "$selected_version" "$binary"
