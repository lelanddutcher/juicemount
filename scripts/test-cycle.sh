#!/bin/bash
# Test 3 Stop/Start cycles against the running JuiceMount.app
# Verifies FUSE/NFS/Redis/MinIO all stay healthy through soft cycles.

set -e

if ! pgrep -x JuiceMount >/dev/null; then
    echo "JuiceMount.app not running. Start it first."
    exit 1
fi

check_health() {
    local label=$1
    local h=$(curl -s --max-time 2 http://127.0.0.1:11050/health 2>&1)
    echo "[$label] $h"
}

# This test can't easily click the popover buttons via System Events
# because they're SwiftUI inside an NSPopover. Instead, just monitor
# health while the user clicks Stop / Start a few times.

echo "=== Initial state ==="
check_health "before"
echo ""

echo "Click Stop and Start in the popover 3 times. I'll monitor for 90s."
echo ""

prev_uptime=""
for i in $(seq 1 30); do
    h=$(curl -s --max-time 2 http://127.0.0.1:11050/health 2>/dev/null || echo "unreachable")
    m=$(curl -s --max-time 2 http://127.0.0.1:11050/metrics 2>/dev/null)
    if [ -n "$m" ]; then
        uptime=$(echo "$m" | python3 -c "import json,sys;print(json.load(sys.stdin)['uptime_sec'])" 2>/dev/null)
        # If uptime decreased, we know a Stop/Start happened
        if [ -n "$prev_uptime" ] && [ "$uptime" -lt "$prev_uptime" ]; then
            echo "[t=${i}] *** RESTART DETECTED — uptime reset to ${uptime}s ***"
        fi
        prev_uptime=$uptime
    fi
    echo "[t=${i}] uptime=${uptime:-?}s   health: $(echo $h | python3 -c "import json,sys;d=json.load(sys.stdin);print(' '.join(f'{k}:{v}' for k,v in d.get('components',{}).items()))" 2>/dev/null || echo $h)"
    sleep 3
done
