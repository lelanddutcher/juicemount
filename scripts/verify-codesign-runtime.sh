#!/bin/bash
# Check the complete codesign result. A codesign | grep -q pipeline can fail
# under pipefail when grep closes early and codesign exits on SIGPIPE.
if [ "$#" -ne 1 ]; then
    echo "usage: verify-codesign-runtime.sh executable" >&2
    exit 2
fi
if ! DETAIL="$(codesign -d --verbose=2 "$1" 2>&1)"; then
    exit 1
fi
grep -Eq '^CodeDirectory .*flags=0x[0-9a-f]+\([^)]*runtime' <<< "$DETAIL"
