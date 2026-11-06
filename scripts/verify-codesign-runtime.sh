#!/bin/bash
# Check the complete codesign result. A codesign | grep -q pipeline can fail
# under pipefail when grep closes early and codesign exits on SIGPIPE.
# With IDENTIFIER, the signature must also carry exactly that identifier
# (read from the same codesign output).
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    echo "usage: verify-codesign-runtime.sh executable [identifier]" >&2
    exit 2
fi
if ! DETAIL="$(codesign -d --verbose=2 "$1" 2>&1)"; then
    exit 1
fi
grep -Eq '^CodeDirectory .*flags=0x[0-9a-f]+\([^)]*runtime' <<< "$DETAIL" || exit 1
if [ "$#" -eq 2 ]; then
    grep -Fxq "Identifier=$2" <<< "$DETAIL" || exit 1
fi
exit 0
