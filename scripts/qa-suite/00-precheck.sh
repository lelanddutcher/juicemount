#!/usr/bin/env bash
# 00-precheck.sh — environment, tools, and baseline metrics.
# Target: ~3 minutes.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

phase_init "00-precheck"

section "tool availability"
# Bash 3.2 compatible: colon-delimited "cmd:purpose" pairs. The purpose
# string begins with "required" or "optional"; we key fail/warn off that.
TOOLS=(
    "fio:required (synthetic IO benchmarks)"
    "ffmpeg:optional (media tests)"
    "dnctl:required (network shaping; system tool)"
    "pfctl:required (packet filter; system tool)"
    "curl:required"
    "python3:required (JSON parsing)"
    "md5:required"
    "stat:required"
    "dd:required"
    "lsof:required"
)
for entry in "${TOOLS[@]}"; do
    cmd="${entry%%:*}"
    purpose="${entry#*:}"
    if has_cmd "$cmd"; then
        pass "$cmd available — $purpose"
    else
        case "$purpose" in
            required*) fail "$cmd MISSING — $purpose" ;;
            *)         warn "$cmd missing — $purpose (tests using it will be skipped)" ;;
        esac
    fi
done

section "mount sanity"
if mount_is_nfs; then pass "$MOUNT mounted as nfs"; else fail "$MOUNT not an nfs mount"; fi
if mount_writable; then pass "$MOUNT writable"; else fail "$MOUNT not writable"; fi

section "JuiceMount health"
H="$(jm_health)"
if echo "$H" | grep -q '"healthy": *true'; then
    pass "JuiceMount /health reports healthy"
else
    fail "JuiceMount /health not healthy: $H"
fi

OS="$(jm_offline_state)"
if echo "$OS" | grep -q '"auto_offline":false'; then
    pass "auto_offline=false"
else
    fail "auto_offline engaged: $OS"
fi

section "system baseline"
log "uname: $(uname -a)"
log "darwin version: $(sw_vers -productVersion 2>/dev/null)"
log "interfaces with IPs:"
ifconfig | awk '/^[a-z]/ {iface=$1} /inet / && iface !~ /^lo/ {print "  " iface " " $2}'
log "disk free on local: $(df -h /tmp | tail -1)"
log "disk free on mount: $(df -h "$MOUNT" | tail -1)"

snapshot_metrics "baseline"
snapshot_lsof "baseline"
snapshot_rss "baseline"

section "shared source pool"
log "ensuring 256 MiB random source pool at $SHARED_POOL"
ensure_shared_pool
log "pool size: $(sz "$SHARED_POOL") bytes"

phase_report
