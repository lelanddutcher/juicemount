#!/bin/bash
# netshape-dev.sh — scoped pseudo-cellular wire shaping via macOS dummynet.
#
#   netshape-dev.sh on  --rtt 300 --bw-down 2 --bw-up 1 --ip 192.168.0.197 \
#     --link-udp-port 63296
#   netshape-dev.sh on  --loss 1 --ip 192.168.0.197 --link-udp-port 63296
#   netshape-dev.sh off
#   netshape-dev.sh status
#   netshape-dev.sh self-test
#
# SAFETY CONTRACT (fixes the qa-suite H4 class of bug):
#   - Shapes ONLY JuiceMount Redis/object traffic to/from --ip and the explicit
#     --redis-port/--object-port values. Nothing else on this Mac is touched.
#     An optional exact local --link-udp-port shapes tsnet/WireGuard traffic;
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

RTT_MS=300; BW_DOWN=2; BW_UP=1; LOSS_RATE=0; NAS_IP="192.168.0.197"
REDIS_PORT=30179; OBJECT_PORT=30151; PROBE_PORT=""; LINK_UDP_PORT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --rtt) RTT_MS="$2"; shift 2;;
    --bw-down) BW_DOWN="$2"; shift 2;;
    --bw-up) BW_UP="$2"; shift 2;;
    --loss) LOSS_RATE="$2"; shift 2;;
    --ip) NAS_IP="$2"; shift 2;;
    --redis-port) REDIS_PORT="$2"; shift 2;;
    --object-port) OBJECT_PORT="$2"; shift 2;;
    --probe-port) PROBE_PORT="$2"; shift 2;;
    --link-udp-port) LINK_UDP_PORT="$2"; shift 2;;
    *) usage;;
  esac
done

# By default the reachability/RTT probe exercises the shaped Redis endpoint.
# Keep --probe-port as an escape hatch for non-default test deployments.
PROBE_PORT="${PROBE_PORT:-$REDIS_PORT}"

# These values are interpolated into a root-loaded PF ruleset. Reject anything
# except a literal address and numeric scalar values before generating it.
python3 - "$NAS_IP" "$REDIS_PORT" "$OBJECT_PORT" "$PROBE_PORT" "${LINK_UDP_PORT:-0}" "$RTT_MS" "$BW_DOWN" "$BW_UP" "$LOSS_RATE" <<'PY'
import ipaddress
import sys

try:
    ipaddress.ip_address(sys.argv[1])
    ports = [int(value) for value in sys.argv[2:5]]
    link_port = int(sys.argv[5])
    rtt, down, up = [int(value) for value in sys.argv[6:9]]
    loss = float(sys.argv[9])
except ValueError as exc:
    raise SystemExit(f"invalid netshape argument: {exc}")
if any(port < 1 or port > 65535 for port in ports):
    raise SystemExit("invalid netshape argument: port must be 1..65535")
if link_port < 0 or link_port > 65535:
    raise SystemExit("invalid netshape argument: link UDP port must be 1..65535")
if any(port in {22, 137, 138, 139, 445} for port in ports + [link_port] if port):
    raise SystemExit("invalid netshape argument: refusing an SSH/SMB/NetBIOS port")
if rtt < 0 or down < 1 or up < 1:
    raise SystemExit("invalid netshape argument: rtt must be nonnegative and bandwidth positive")
if loss < 0 or loss > 1:
    raise SystemExit("invalid netshape argument: loss must be between 0 and 1")
PY

render_shape_rules() {
  local port
  for port in "$REDIS_PORT" "$OBJECT_PORT"; do
    echo "dummynet in  quick proto tcp from ${NAS_IP} port ${port} to any pipe ${PIPE_IN}"
    echo "dummynet out quick proto tcp from any to ${NAS_IP} port ${port} pipe ${PIPE_OUT}"
  done
  if [ -n "$LINK_UDP_PORT" ]; then
    # Match the app-owned local UDP socket in both directions. This covers the
    # encrypted tsnet/WireGuard data plane without shaping other NAS traffic.
    echo "dummynet in  quick proto udp from any to any port ${LINK_UDP_PORT} pipe ${PIPE_IN}"
    echo "dummynet out quick proto udp from any port ${LINK_UDP_PORT} to any pipe ${PIPE_OUT}"
  fi
}

self_test() {
  local rules
  local expected
  rules=$(render_shape_rules)
  expected=4
  [ -z "$LINK_UDP_PORT" ] || expected=6
  [ "$(printf '%s\n' "$rules" | wc -l | tr -d ' ')" = "$expected" ]
  [ "$(printf '%s\n' "$rules" | grep -Ec '^dummynet (in|out) +quick proto tcp .* port [0-9][0-9]*.* pipe [0-9][0-9]*$')" = "4" ]
  if [ -n "$LINK_UDP_PORT" ]; then
    [ "$(printf '%s\n' "$rules" | grep -Ec "^dummynet (in|out) +quick proto udp .* port ${LINK_UDP_PORT}.* pipe [0-9][0-9]*$")" = "2" ]
  fi
  ! printf '%s\n' "$rules" | grep -Eq 'port (22|137|138|139|445)([^0-9]|$)'
  ! printf '%s\n' "$rules" | grep -Eq 'from [^ ]+ to any pipe|from any to [^ ]+ pipe'
  echo "PASS: exact backend/link-port rules; no broad, SSH, or SMB/NetBIOS match"
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
  local probe_code
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
  read -r probe_code probe_ms <<< "$(python3 - "$NAS_IP" "$PROBE_PORT" "$RTT_MS" <<'PY'
import socket, sys, time
host, port, rtt = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
s = socket.socket()
s.settimeout(max(3.0, rtt / 1000.0 * 5.0))
started = time.monotonic()
try:
    code = s.connect_ex((host, port))
finally:
    s.close()
print(code, int((time.monotonic() - started) * 1000))
PY
  )"
  min_probe_ms=$(( RTT_MS * 3 / 4 ))
  echo "$pipes" | grep -Eq "(^|[[:space:]])0*${PIPE_IN}:" &&
    sudo -n pfctl -s info 2>/dev/null | grep -q 'Status: Enabled' &&
    echo "$rules" | grep -Eq "dummynet in .*from ${NAS_IP} port (= )?${REDIS_PORT} .*pipe 0*${PIPE_IN}" &&
    echo "$rules" | grep -Eq "dummynet out .*to ${NAS_IP} port (= )?${REDIS_PORT} .*pipe 0*${PIPE_OUT}" &&
    echo "$rules" | grep -Eq "dummynet in .*from ${NAS_IP} port (= )?${OBJECT_PORT} .*pipe 0*${PIPE_IN}" &&
    echo "$rules" | grep -Eq "dummynet out .*to ${NAS_IP} port (= )?${OBJECT_PORT} .*pipe 0*${PIPE_OUT}" &&
    { [ -z "$LINK_UDP_PORT" ] || {
      echo "$rules" | grep -Eq "dummynet in .*proto udp .*to any port (= )?${LINK_UDP_PORT} .*pipe 0*${PIPE_IN}" &&
      echo "$rules" | grep -Eq "dummynet out .*proto udp .*from any port (= )?${LINK_UDP_PORT} .*pipe 0*${PIPE_OUT}";
    }; } &&
    ! echo "$rules" | grep -Eq "dummynet .*${NAS_IP} port (= )?(22|139|445)([^0-9]|$)" &&
    { if [ "$LOSS_RATE" = "1" ] || [ "$LOSS_RATE" = "1.0" ]; then
        [ "$probe_code" -ne 0 ] && [ "$probe_ms" -ge 1000 ] &&
          echo "wire probe: blocked for ${probe_ms}ms (100% loss verified)"
      else
        [ "$probe_code" -eq 0 ] && [ "$probe_ms" -ge "$min_probe_ms" ] &&
          echo "wire probe: ${probe_ms}ms (minimum ${min_probe_ms}ms)"
      fi; }
}

load_marker_shape() {
  local marker_values
  marker_values=$(python3 - "$MARKER" <<'PY'
import ipaddress
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    marker = json.load(handle)
if marker.get("active") is not True:
    raise SystemExit("netshape marker does not describe an active shaper")
ipaddress.ip_address(marker["ip"])
ports = [int(marker[key]) for key in ("redis_port", "object_port", "probe_port")]
link_port = marker.get("link_udp_port")
if link_port is not None:
    link_port = int(link_port)
rtt = int(marker["rtt_ms"])
down = int(marker["bw_down_mb"])
up = int(marker["bw_up_mb"])
loss = float(marker["loss_rate"])
if any(port < 1 or port > 65535 for port in ports):
    raise SystemExit("invalid port in netshape marker")
if link_port is not None and (link_port < 1 or link_port > 65535):
    raise SystemExit("invalid Link UDP port in netshape marker")
if rtt < 0 or down < 1 or up < 1 or loss < 0 or loss > 1:
    raise SystemExit("invalid impairment value in netshape marker")
print("\t".join(str(value) for value in (
    marker["ip"], ports[0], ports[1], ports[2],
    link_port if link_port is not None else "-", rtt, down, up, loss,
)))
PY
  )
  IFS=$'\t' read -r NAS_IP REDIS_PORT OBJECT_PORT PROBE_PORT LINK_UDP_PORT RTT_MS BW_DOWN BW_UP LOSS_RATE <<< "$marker_values"
  [ "$LINK_UDP_PORT" != "-" ] || LINK_UDP_PORT=""
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
  sudo -n dnctl pipe "$PIPE_IN" config delay "${half}ms" bw "${BW_DOWN}Mbit/s" plr "$LOSS_RATE"
  sudo -n dnctl pipe "$PIPE_OUT" config delay "${half}ms" bw "${BW_UP}Mbit/s" plr "$LOSS_RATE"

  rules_tmp=$(mktemp -t jum-netshape.XXXXXX)
  {
    echo "# JuiceMount dev shaping -> ${NAS_IP}:{${REDIS_PORT},${OBJECT_PORT}} (rtt=${RTT_MS}ms down=${BW_DOWN}Mb/s up=${BW_UP}Mb/s loss=${LOSS_RATE})"
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
    link_json=null
    [ -z "$LINK_UDP_PORT" ] || link_json="$LINK_UDP_PORT"
    printf '{"active":true,"ip":"%s","redis_port":%s,"object_port":%s,"link_udp_port":%s,"probe_port":%s,"rtt_ms":%s,"bw_down_mb":%s,"bw_up_mb":%s,"loss_rate":%s,"since":%s}\n' \
      "$NAS_IP" "$REDIS_PORT" "$OBJECT_PORT" "$link_json" "$PROBE_PORT" "$RTT_MS" "$BW_DOWN" "$BW_UP" "$LOSS_RATE" "$now" > "$MARKER"
    echo "SHAPING ACTIVE -> backend ${NAS_IP}:{${REDIS_PORT},${OBJECT_PORT}} link_udp=${LINK_UDP_PORT:-off} rtt≈${RTT_MS}ms down=${BW_DOWN}Mb/s up=${BW_UP}Mb/s loss=${LOSS_RATE}"
    echo "marker: $MARKER  ('off' to remove; survives until then)"
    ;;
  off)
    sudo_ok || { echo "need passwordless sudo for pfctl and dnctl"; exit 1; }
    restore_state
    ;;
  status)
    if [ -f "$MARKER" ]; then
      cat "$MARKER"
      # Re-validate against the exact active profile. Using this invocation's
      # defaults made `status` demand a 225 ms probe after `on --rtt 80`, even
      # though activation had correctly proved the requested 80 ms wire delay.
      load_marker_shape
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
