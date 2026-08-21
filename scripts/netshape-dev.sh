#!/bin/bash
# netshape-dev.sh — scoped pseudo-cellular wire shaping via macOS dummynet.
#
#   netshape-dev.sh on  --rtt 300 --bw-down 2 --bw-up 1 --ip 192.168.0.197
#   netshape-dev.sh off
#   netshape-dev.sh status
#
# SAFETY CONTRACT (fixes the qa-suite H4 class of bug):
#   - Shapes ONLY traffic to/from --ip (the NAS). Nothing else on this Mac
#     is touched — no global pipes, no other destinations.
#   - ON saves the COMPLETE prior pf state (enabled-flag + full ruleset) to
#     a state file; OFF restores it byte-for-byte. We never blind-disable pf,
#     so host firewalls (LuLu/Little Snitch/corporate) survive the round trip.
#   - A marker file records active shaping so harnesses can refuse-to-report;
#     `status` prints it; stale markers older than the pf state are flagged.
#
# Requires passwordless sudo for pfctl/dnctl (repo-documented grant) or will
# prompt once per invocation group.

set -uo pipefail

STATE_DIR="${HOME}/.juicemount"
STATE_FILE="${STATE_DIR}/netshape-state.conf"
MARKER="${STATE_DIR}/netshape-ACTIVE.json"
ANCHOR_RULES="/etc/pf.anchors/jum-netshape.conf"

usage() { sed -n '2,10p' "$0"; exit 2; }

cmd="${1:-status}"; shift || true

RTT_MS=300; BW_DOWN=2; BW_UP=1; NAS_IP="192.168.0.197"
while [ $# -gt 0 ]; do
  case "$1" in
    --rtt) RTT_MS="$2"; shift 2;;
    --bw-down) BW_DOWN="$2"; shift 2;;
    --bw-up) BW_UP="$2"; shift 2;;
    --ip) NAS_IP="$2"; shift 2;;
    *) usage;;
  esac
done

sudo_ok() { sudo -n true 2>/dev/null; }

save_state() {
  mkdir -p "$STATE_DIR"
  {
    echo "# jum-netshape saved $(date)"
    echo "ENABLED=$(sudo -n pfctl -s info 2>/dev/null | head -1 | grep -c 'Enabled')"
    sudo -n pfctl -s rules 2>/dev/null
  } > "$STATE_FILE"
}

restore_state() {
  [ -f "$STATE_FILE" ] || { echo "no saved state — nothing to restore"; return 1; }
  local want_enabled
  want_enabled=$(grep -m1 '^ENABLED=' "$STATE_FILE" | cut -d= -f2)
  # Strip our bookkeeping line, keep the exact prior ruleset.
  grep -v '^#' "$STATE_FILE" | grep -v '^ENABLED=' > /tmp/jum-pf-restore.conf
  sudo -n pfctl -f /tmp/jum-pf-restore.conf
  if [ "${want_enabled:-1}" = "0" ]; then
    sudo -n pfctl -d   # pf was disabled before we touched it — put it back
  fi
  rm -f "$STATE_FILE" "$MARKER" /tmp/jum-pf-restore.conf
  echo "prior pf state restored; shaping removed"
}

case "$cmd" in
  on)
    sudo_ok || { echo "need sudo (will prompt)"; }
    save_state
    HALF=$(( RTT_MS / 2 ))
    sudo -n dnctl flush 2>/dev/null
    sudo -n dnctl pipe 1 config delay "$HALF" ms bandwidth "$((BW_DOWN * 1000000))" bit/s
    sudo -n dnctl pipe 2 config delay "$HALF" ms bandwidth "$((BW_UP * 1000000))" bit/s
    cat > /tmp/jum-netshape.rules <<EOF
# JuiceMount dev shaping -> ${NAS_IP} (rtt=${RTT_MS}ms down=${BW_DOWN}Mb/s up=${BW_UP}Mb/s)
dummynet in  proto { tcp udp } from ${NAS_IP} to any pipe 1
dummynet out proto { tcp udp } from any to ${NAS_IP} pipe 2
EOF
    # Merge: prior rules + our shaping lines at TOP (dummynet must see packets
    # before pass-all defaults).
    { cat /tmp/jum-netshape.rules; grep -v '^#' "$STATE_FILE" | grep -v '^ENABLED='; } | sudo -n pfctl -f -
    now=$(date +%s)
    printf '{"active":true,"ip":"%s","rtt_ms":%s,"bw_down_mb":%s,"bw_up_mb":%s,"since":%s}\n' \
      "$NAS_IP" "$RTT_MS" "$BW_DOWN" "$BW_UP" "$now" > "$MARKER"
    echo "SHAPING ACTIVE -> ${NAS_IP}: rtt≈${RTT_MS}ms down=${BW_DOWN}Mb/s up=${BW_UP}Mb/s"
    echo "marker: $MARKER  ('off' to remove; survives until then)"
    ;;
  off)
    sudo_ok || { echo "need sudo"; exit 1; }
    restore_state
    ;;
  status)
    if [ -f "$MARKER" ]; then cat "$MARKER"; else echo "not shaping"; fi
    sudo -n pfctl -s Anchors 2>/dev/null | grep -q . || true
    ;;
  *) usage;;
esac
