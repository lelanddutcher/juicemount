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

set -euo pipefail

STATE_DIR="${HOME}/.juicemount"
STATE_FILE="${STATE_DIR}/netshape-state.conf"
MARKER="${STATE_DIR}/netshape-ACTIVE.json"
ANCHOR_RULES="/etc/pf.anchors/jum-netshape.conf"
PIPE_IN=4241
PIPE_OUT=4242

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

# This workstation grants the two required binaries through sudoers without
# necessarily granting the unrelated `sudo true`. Probe the actual operations
# we need so an allowed, safe setup is not rejected (and a disallowed one never
# gets as far as mutating PF).
sudo_ok() {
  sudo -n pfctl -s info >/dev/null 2>&1 &&
    sudo -n dnctl pipe show >/dev/null 2>&1
}

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
  local restore_tmp
  want_enabled=$(grep -m1 '^ENABLED=' "$STATE_FILE" | cut -d= -f2)
  restore_tmp=$(mktemp -t jum-pf-restore.XXXXXX)
  # Strip our bookkeeping line, keep the exact prior ruleset.
  grep -v '^#' "$STATE_FILE" | grep -v '^ENABLED=' > "$restore_tmp"
  # Delete only JuiceMount's reserved pipes. Never flush another developer's
  # dummynet setup while removing this scoped shaper.
  sudo -n dnctl pipe delete "$PIPE_IN" "$PIPE_OUT" 2>/dev/null || true
  sudo -n pfctl -f "$restore_tmp"
  if [ "${want_enabled:-1}" = "0" ]; then
    # `pfctl -d` exits non-zero when PF is already disabled. That state is the
    # desired result, not a cleanup failure; do not strand the marker/state.
    sudo -n pfctl -d 2>/dev/null || true
  fi
  rm -f "$STATE_FILE" "$MARKER" "$restore_tmp"
  echo "prior pf state restored; shaping removed"
}

verify_shape() {
  local pipes
  local rules
  pipes=$(sudo -n dnctl pipe show 2>/dev/null)
  # macOS omits dummynet rules from `-sr` even though it includes ordinary
  # filter rules there; `-s all` is the only read-only view that renders the
  # active dummynet section.
  rules=$(sudo -n pfctl -s all 2>/dev/null)
  echo "$pipes" | grep -Eq "(^|[[:space:]])0*${PIPE_IN}:" &&
    echo "$pipes" | grep -Eq "(^|[[:space:]])0*${PIPE_OUT}:" &&
    sudo -n pfctl -s info 2>/dev/null | grep -q 'Status: Enabled' &&
    echo "$rules" | grep -Eq "dummynet in .*from ${NAS_IP} .*pipe 0*${PIPE_IN}" &&
    echo "$rules" | grep -Eq "dummynet out .*to ${NAS_IP} .*pipe 0*${PIPE_OUT}"
}

activate_shape() {
  local half="$1"
  local rules_tmp

  # Refuse to collide with an existing dummynet experiment. We reserve two
  # high-numbered pipes and delete only those on cleanup, but a pre-existing
  # setup still means the measured result would not have a trustworthy shape.
  if sudo -n dnctl pipe show 2>/dev/null | grep -q .; then
    echo "existing dummynet pipes detected — refusing to stack shapers" >&2
    return 1
  fi

  # macOS dnctl requires the unit to be attached to the value (`150ms`), and
  # uses `bw`; the previous split `delay 150 ms bandwidth ...` was rejected
  # while the script continued and falsely wrote an ACTIVE marker.
  sudo -n dnctl pipe "$PIPE_IN" config delay "${half}ms" bw "${BW_DOWN}Mbit/s"
  sudo -n dnctl pipe "$PIPE_OUT" config delay "${half}ms" bw "${BW_UP}Mbit/s"

  rules_tmp=$(mktemp -t jum-netshape.XXXXXX)
  {
    echo "# JuiceMount dev shaping -> ${NAS_IP} (rtt=${RTT_MS}ms down=${BW_DOWN}Mb/s up=${BW_UP}Mb/s)"
    echo "dummynet in  proto { tcp udp } from ${NAS_IP} to any pipe ${PIPE_IN}"
    echo "dummynet out proto { tcp udp } from any to ${NAS_IP} pipe ${PIPE_OUT}"
    grep -v '^#' "$STATE_FILE" | grep -v '^ENABLED='
  } > "$rules_tmp"
  sudo -n pfctl -f "$rules_tmp"
  rm -f "$rules_tmp"

  # Loading rules does not enable PF when the saved state was disabled. A
  # disabled PF means zero packets enter dummynet, so claiming ACTIVE then is a
  # false test. Enable it for the measurement; restore_state returns it to the
  # exact prior enabled flag.
  if ! sudo -n pfctl -s info 2>/dev/null | grep -q 'Status: Enabled'; then
    sudo -n pfctl -e
  fi

  # Do not advertise active unless both pipes, PF, and both scoped rules are
  # observably live. A pipe without a PF rule carries zero traffic and makes a
  # benchmark look green while testing the unshaped LAN.
  verify_shape
}

case "$cmd" in
  on)
    sudo_ok || { echo "need passwordless sudo for pfctl and dnctl"; exit 1; }
    [ ! -f "$STATE_FILE" ] || { echo "saved netshape state already exists — run 'off' first"; exit 1; }
    save_state
    HALF=$(( RTT_MS / 2 ))
    if ! activate_shape "$HALF"; then
      echo "failed to activate verified shaper — restoring prior PF state" >&2
      restore_state
      exit 1
    fi
    now=$(date +%s)
    printf '{"active":true,"ip":"%s","rtt_ms":%s,"bw_down_mb":%s,"bw_up_mb":%s,"since":%s}\n' \
      "$NAS_IP" "$RTT_MS" "$BW_DOWN" "$BW_UP" "$now" > "$MARKER"
    echo "SHAPING ACTIVE -> ${NAS_IP}: rtt≈${RTT_MS}ms down=${BW_DOWN}Mb/s up=${BW_UP}Mb/s"
    echo "marker: $MARKER  ('off' to remove; survives until then)"
    ;;
  off)
    sudo_ok || { echo "need passwordless sudo for pfctl and dnctl"; exit 1; }
    restore_state
    ;;
  status)
    if [ -f "$MARKER" ]; then
      cat "$MARKER"
      if ! sudo_ok || ! verify_shape; then
        echo "ERROR: marker exists but verified PF/dummynet shaping is not active" >&2
        exit 1
      fi
      echo "verified: PF enabled; inbound/outbound pipes and scoped rules present"
    else
      echo "not shaping"
    fi
    ;;
  *) usage;;
esac
