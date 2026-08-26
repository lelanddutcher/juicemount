#!/bin/bash
# netshape-dev.sh — scoped pseudo-cellular wire shaping via macOS dummynet.
#
#   netshape-dev.sh on  --rtt 300 --bw-down 2 --bw-up 1 --ip 192.168.0.197
#   netshape-dev.sh off
#   netshape-dev.sh status
#   netshape-dev.sh self-test
#
# SAFETY CONTRACT (fixes the qa-suite H4 class of bug):
#   - Shapes ONLY JuiceMount Redis/object traffic to/from --ip and the explicit
#     --redis-port/--object-port values. Nothing else on this Mac is touched.
#     SSH, SMB/Time Machine, and other NAS services cannot match these rules.
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
REDIS_PORT=30179; OBJECT_PORT=30151; PROBE_PORT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --rtt) RTT_MS="$2"; shift 2;;
    --bw-down) BW_DOWN="$2"; shift 2;;
    --bw-up) BW_UP="$2"; shift 2;;
    --ip) NAS_IP="$2"; shift 2;;
    --redis-port) REDIS_PORT="$2"; shift 2;;
    --object-port) OBJECT_PORT="$2"; shift 2;;
    --probe-port) PROBE_PORT="$2"; shift 2;;
    *) usage;;
  esac
done

# By default the reachability/RTT probe exercises the shaped Redis endpoint.
# Keep --probe-port as an escape hatch for non-default test deployments.
PROBE_PORT="${PROBE_PORT:-$REDIS_PORT}"

# These values are interpolated into a root-loaded PF ruleset. Reject anything
# except a literal address and numeric scalar values before generating it.
python3 - "$NAS_IP" "$REDIS_PORT" "$OBJECT_PORT" "$PROBE_PORT" "$RTT_MS" "$BW_DOWN" "$BW_UP" <<'PY'
import ipaddress
import sys

try:
    ipaddress.ip_address(sys.argv[1])
    ports = [int(value) for value in sys.argv[2:5]]
    rtt, down, up = [int(value) for value in sys.argv[5:8]]
except ValueError as exc:
    raise SystemExit(f"invalid netshape argument: {exc}")
if any(port < 1 or port > 65535 for port in ports):
    raise SystemExit("invalid netshape argument: port must be 1..65535")
if rtt < 0 or down < 1 or up < 1:
    raise SystemExit("invalid netshape argument: rtt must be nonnegative and bandwidth positive")
PY

render_shape_rules() {
  local port
  for port in "$REDIS_PORT" "$OBJECT_PORT"; do
    echo "dummynet in  quick proto tcp from ${NAS_IP} port ${port} to any pipe ${PIPE_IN}"
    echo "dummynet out quick proto tcp from any to ${NAS_IP} port ${port} pipe ${PIPE_OUT}"
  done
}

self_test() {
  local rules
  rules=$(render_shape_rules)
  [ "$(printf '%s\n' "$rules" | wc -l | tr -d ' ')" = "4" ]
  [ "$(printf '%s\n' "$rules" | grep -Ec '^dummynet (in|out) +quick proto tcp .* port [0-9][0-9]*.* pipe [0-9][0-9]*$')" = "4" ]
  ! printf '%s\n' "$rules" | grep -Eq 'port (22|137|138|139|445)([^0-9]|$)'
  ! printf '%s\n' "$rules" | grep -Eq 'from [^ ]+ to any pipe|from any to [^ ]+ pipe'
  echo "PASS: exactly four backend-port rules; no broad, SSH, or SMB/NetBIOS match"
}

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
  local probe_ms
  local min_probe_ms
  pipes=$(sudo -n dnctl pipe show 2>/dev/null)
  # macOS omits dummynet rules from `-sr` even though it includes ordinary
  # filter rules there; `-s all` is the only read-only view that renders the
  # active dummynet section.
  rules=$(sudo -n pfctl -s all 2>/dev/null)
  # Darwin's dnctl display drops the idle/outbound pipe header after the first
  # flow enters the inbound pipe, even though the outbound pipe is still
  # operational. Prove both half-delays on the wire instead of trusting that
  # misleading rendering: a TCP handshake traverses out + in and must therefore
  # take approximately the requested RTT. The default probe is JuiceMount's
  # Redis endpoint; --probe-port supports non-default test deployments.
  probe_ms=$(python3 - "$NAS_IP" "$PROBE_PORT" "$RTT_MS" <<'PY'
import socket, sys, time
host, port, rtt = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
s = socket.socket()
s.settimeout(max(3.0, rtt / 1000.0 * 5.0))
started = time.monotonic()
try:
    s.connect_ex((host, port))
finally:
    s.close()
print(int((time.monotonic() - started) * 1000))
PY
  )
  min_probe_ms=$(( RTT_MS * 3 / 4 ))
  echo "$pipes" | grep -Eq "(^|[[:space:]])0*${PIPE_IN}:" &&
    sudo -n pfctl -s info 2>/dev/null | grep -q 'Status: Enabled' &&
    echo "$rules" | grep -Eq "dummynet in .*from ${NAS_IP} port (= )?${REDIS_PORT} .*pipe 0*${PIPE_IN}" &&
    echo "$rules" | grep -Eq "dummynet out .*to ${NAS_IP} port (= )?${REDIS_PORT} .*pipe 0*${PIPE_OUT}" &&
    echo "$rules" | grep -Eq "dummynet in .*from ${NAS_IP} port (= )?${OBJECT_PORT} .*pipe 0*${PIPE_IN}" &&
    echo "$rules" | grep -Eq "dummynet out .*to ${NAS_IP} port (= )?${OBJECT_PORT} .*pipe 0*${PIPE_OUT}" &&
    ! echo "$rules" | grep -Eq "dummynet .*${NAS_IP} port (= )?(22|139|445)([^0-9]|$)" &&
    [ "$probe_ms" -ge "$min_probe_ms" ] &&
    echo "wire probe: ${probe_ms}ms (minimum ${min_probe_ms}ms)"
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
    echo "# JuiceMount dev shaping -> ${NAS_IP}:{${REDIS_PORT},${OBJECT_PORT}} (rtt=${RTT_MS}ms down=${BW_DOWN}Mb/s up=${BW_UP}Mb/s)"
    # Dummynet rules are queueing rules, so PF requires all of them before any
    # saved filtering rules. Exact service ports are safer than broad rules plus
    # exemptions: SMB/SSH cannot enter either pipe in the first place.
    render_shape_rules
    # PF is last-match-wins unless a rule is quick. The saved system ruleset
    # contains broad pass rules after ours; without quick, outbound packets
    # bypassed dummynet even though `pfctl -s all` still displayed the rule.
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
    printf '{"active":true,"ip":"%s","redis_port":%s,"object_port":%s,"probe_port":%s,"rtt_ms":%s,"bw_down_mb":%s,"bw_up_mb":%s,"since":%s}\n' \
      "$NAS_IP" "$REDIS_PORT" "$OBJECT_PORT" "$PROBE_PORT" "$RTT_MS" "$BW_DOWN" "$BW_UP" "$now" > "$MARKER"
    echo "SHAPING ACTIVE -> ${NAS_IP}:{${REDIS_PORT},${OBJECT_PORT}} rtt≈${RTT_MS}ms down=${BW_DOWN}Mb/s up=${BW_UP}Mb/s"
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
      echo "verified: PF enabled; inbound/outbound backend-port pipes present; SMB/SSH excluded by construction"
    else
      echo "not shaping"
    fi
    ;;
  self-test)
    self_test
    ;;
  *) usage;;
esac
