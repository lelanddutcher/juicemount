#!/usr/bin/env bash
# Roam lab: change the network under a running session and time the
# recovery.
#
#   roam-lab.sh fixture             write the cold read files (on lan)
#   roam-lab.sh run CASE [LABEL]    one case; results/roam-<time>/<label>-<case>.*
#   roam-lab.sh all [LABEL]         every case in order (or ROAM_CASES)
#   roam-lab.sh summary DIR         the recovery table of one results folder
#
# A session is jm5 (the NFS server with the write spool, its drainer and the
# project-lease client) over the client's juicefs mount, as drain-lab.sh runs
# it. `drainprobe roam` drives it over loopback NFS: a sequential read of a
# cold file, one new file per second, a folder listing, a stat straight on the
# juicefs mount, and the app's reachability monitor (see drainprobe/roam.go).
# Then the case changes the network under it:
#
#   ipchange      the client gets a new address (Wi-Fi to another network):
#                 every open TCP connection is left with a dead address
#   drop5/30/180  the link goes down (interface gone) for N s, then returns
#                 with the same address
#   blackhole30   the link stays up but carries nothing for 30 s (the wan
#                 drops every packet: associated, no internet)
#   held-CASE     CASE without a session: one descriptor of a cold file held
#                 open on the juicefs mount across the change (run_held)
#   path          the route moves to the second router (the server sees a
#                 new address, as when a Mac moves from the LAN to a relay),
#                 stays there 60 s, then moves back
#
# After the event the probe runs ROAM_POST seconds (default 120; 240 after
# drop180), then the script waits for the spool to drain (ROAM_DRAIN_CAP,
# default 300 s) and checks every acknowledged file on the juicefs mount
# (drainprobe verify: same bytes, once each, nothing extra).
#
# Needs a lab set up with the second router and the binaries:
#   JMLAB_ROAM=1 JMLAB_BBR=1 ./lab.sh up
#   ./drain-lab.sh build && experiments/roam-lab.sh fixture
# ROAM_PROFILE (default same-city) shapes the wan; ROAM_ALT_PROFILE (default
# abroad-noloss) shapes the second router. ROAM_BIN names another folder of
# the state directory holding jm5 (a before/after pair, as DRAIN_BIN).
# ROAM_JM5_ENV adds environment to jm5. ROAM_READ_MIBS paces the read like a
# player (default 2 MiB/s, so a 1 GiB file lasts the longest case).
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
LAB="$HERE/lab.sh"
STATE_NAME="${JMLAB_STATE_DIR:-.state}"
STATE="$HERE/$STATE_NAME"
PROJECT="${JMLAB_PROJECT:-jmlab}"
NET="${PROJECT}_remote"
NAS_SUBNET="${JMLAB_NAS_SUBNET:-10.231.10.0/24}"
WAN_IP="${JMLAB_WAN_REMOTE_IP:-10.231.20.2}"
ALT_IP="${JMLAB_ALT_REMOTE_IP:-10.231.20.3}"
CLIENT_IP="${JMLAB_CLIENT_IP:-10.231.20.10}"
CLIENT_IP2="${JMLAB_CLIENT_IP2:-10.231.20.11}"
PROXY="${JMLAB_PROXY_IP:-10.231.10.20}:6379"
PROFILE="${ROAM_PROFILE:-same-city}"
ALT_PROFILE="${ROAM_ALT_PROFILE:-abroad-noloss}"
FILE_MB="${ROAM_FILE_MB:-1024}"
FILES="${ROAM_FILES:-16}"
PRE="${ROAM_PRE:-15}"
DRAIN_CAP="${ROAM_DRAIN_CAP:-300}"
OUT="${ROAM_OUT:-$HERE/results/roam-$(date +%Y%m%d-%H%M%S)}"
BIN="/lab/$STATE_NAME/${ROAM_BIN:-bin}"
CASES="ipchange drop5 drop30 drop180 blackhole30 path"

log() { printf '[roam] %s\n' "$*" >&2; }
die() { log "$*"; exit 2; }
now_ms() { date +%s%3N; }
cid() { "$LAB" _cid client; }
mark() { printf '{"t":%s,"event":"%s"}\n' "$(now_ms)" "$1" >> "$OUT/$RUN.events"; log "$RUN: $1"; }

# The client's address and route after a reconnect. Docker gives the new
# interface its own name; offloads go off as at start (compose.yml).
client_net_setup() {
  "$LAB" exec sh -c 'for d in $(ls /sys/class/net | grep -v "^lo$"); do ethtool -K "$d" tso off gso off gro off 2>/dev/null || true; done
    ip route replace '"$NAS_SUBNET"' via '"$1"
}
current_ip() { docker inspect -f "{{(index .NetworkSettings.Networks \"$NET\").IPAddress}}" "$(cid)"; }
reconnect() { # reconnect IP: the client is back on the remote network
  docker network connect --ip "$1" "$NET" "$(cid)"
  client_net_setup "$WAN_IP"
}

# The proxy's view of the project leases, every 2 s, from the nas side (the
# redis container), so the network events never touch the poll itself:
# last_hb_ms shows each renewal, a new epoch a lost and re-granted lease.
lease_poll() {
  local pass rcid
  pass="$(sed -n 's/^JM_PROXY_ADMIN_PASSWORD=//p' "$STATE/lab.env")"
  rcid="$("$LAB" _cid redis)"
  while :; do
    printf '{"t":%s,"leases":%s}\n' "$(now_ms)" "$(docker exec -e REDISCLI_AUTH="$pass" "$rcid" redis-cli \
      --no-auth-warning --json -h respproxy --user admin JM.LEASE STATUS '*' 2>/dev/null | tr -d '\n ' || echo null)"
    sleep 2
  done
}

stop_jm5() {
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'p=$(cat /var/jm/jm5.pid 2>/dev/null) || exit 0
    kill "$p" 2>/dev/null || exit 0
    for i in $(seq 50); do kill -0 "$p" 2>/dev/null || exit 0; sleep 0.2; done
    kill -9 "$p" 2>/dev/null; sleep 1'
}

start_jm5() {
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'rm -rf /var/jm && mkdir -p /var/jm/spool /jfs/roam/runs'
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'cd /var/jm && HOME=/var/jm JM_SPOOL_ENABLE=1 JM_SPOOL_DIR=/var/jm/spool JM_SPOOL_SIZE_GB=8 JM_NET_FORCE_CLASS=slow '"${ROAM_JM5_ENV:-}"' \
    nohup '"$BIN"'/jm5 --redis "redis://admin:$META_PASSWORD@'"$PROXY"'/1" --replacement-recovery \
      --fuse-path /jfs --no-fuse --no-mount --listen 127.0.0.1:21049 --metrics-addr 127.0.0.1:21050 \
      --db /var/jm/metadata.db >/var/jm/jm5.log 2>&1 & echo $! > /var/jm/jm5.pid'
  for _ in $(seq 120); do
    if "$LAB" exec sh -c 'wget -q -O /dev/null http://127.0.0.1:21050/health' 2>/dev/null; then break; fi
    sleep 1
  done
  sleep 3
  XPATH="$("$LAB" exec sed -n 's/.*"export":"\(\/jm-[0-9a-f]*\)".*/\1/p' /var/jm/jm5.log | head -n 1)"
  [ -n "$XPATH" ] || { "$LAB" exec tail -n 40 /var/jm/jm5.log >&2; die "jm5 logged no export path"; }
}

next_file() {
  local n
  n="$(cat "$STATE/roam-next" 2>/dev/null || echo 1)"
  [ "$n" -le "$FILES" ] || die "every fixture file was read once; run roam-lab.sh fixture again"
  echo $((n + 1)) > "$STATE/roam-next"
  printf 'roam/src/r%02d.bin' "$n"
}

event() { # event CASE: the network change, with its marks
  local ip
  case "$1" in
    ipchange)
      ip="$(current_ip)"
      local new="$CLIENT_IP2"; [ "$ip" != "$CLIENT_IP2" ] || new="$CLIENT_IP"
      mark cut
      docker network disconnect "$NET" "$(cid)"
      reconnect "$new"
      mark restore ;;
    drop*)
      ip="$(current_ip)"
      mark cut
      docker network disconnect "$NET" "$(cid)"
      sleep "${1#drop}"
      reconnect "$ip"
      mark restore ;;
    blackhole*)
      mark cut
      docker exec "$("$LAB" _cid wan)" sh -c 'iptables -I FORWARD -j DROP'
      sleep "${1#blackhole}"
      docker exec "$("$LAB" _cid wan)" sh -c 'iptables -D FORWARD -j DROP'
      mark restore ;;
    path)
      mark cut
      client_net_setup "$ALT_IP"
      mark restore
      sleep 60
      mark back
      client_net_setup "$WAN_IP"
      mark restore-back ;;
    *) die "unknown case $1 (one of: $CASES)" ;;
  esac
}

run_case() {
  local case="$1" label="${2:-run}"
  RUN="$label-$case"
  mkdir -p "$OUT"
  local file post
  file="$(next_file)"
  post="${ROAM_POST:-120}"; [ "$case" != drop180 ] || post="${ROAM_POST:-240}"
  stop_jm5
  "$LAB" shape lan >/dev/null
  [ "$(current_ip)" = "$CLIENT_IP" ] || { docker network disconnect "$NET" "$(cid)"; reconnect "$CLIENT_IP"; }
  client_net_setup "$WAN_IP"
  start_jm5
  local jfs_lines
  jfs_lines="$("$LAB" exec sh -c 'wc -l < /var/log/juicefs.log' 2>/dev/null | tr -dc 0-9)"
  jfs_lines="${jfs_lines:-0}"
  "$LAB" shape "$PROFILE" >/dev/null
  "$LAB" shape "$ALT_PROFILE" alt >/dev/null
  local dir
  dir="roam/runs/$RUN-$(date +%s)"
  : > "$OUT/$RUN.events"
  mark start
  "$LAB" exec sh -c "rm -f /var/jm/roam.jsonl*; nohup $BIN/drainprobe roam -export $XPATH -dir $dir -read $file -read-mibs ${ROAM_READ_MIBS:-2} \
    -reach $PROXY -out /var/jm/roam.jsonl -duration 30m >/var/jm/roam.err 2>&1 & echo \$! > /var/jm/roam.pid"
  lease_poll >> "$OUT/$RUN.leases.jsonl" &
  local poller=$!
  sleep "$PRE"
  event "$case"
  sleep "$post"
  mark stop
  kill "$poller" 2>/dev/null || true
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'kill -TERM "$(cat /var/jm/roam.pid)"; for i in $(seq 30); do grep -q "\"end\":true" /var/jm/roam.jsonl && exit 0; sleep 1; done; exit 0'
  log "$RUN: wait for the spool to drain (cap ${DRAIN_CAP}s)"
  local t0 pending="?"
  t0=$(date +%s)
  while [ $(( $(date +%s) - t0 )) -lt "$DRAIN_CAP" ]; do
    pending="$("$LAB" exec sh -c 'wget -q -O - http://127.0.0.1:21050/spool' 2>/dev/null \
      | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["pending_files"]+d["pending_folders"])' 2>/dev/null || echo "?")"
    [ "$pending" != 0 ] || break
    sleep 2
  done
  printf '{"t":%s,"event":"drained","pending":"%s","after_s":%s}\n' "$(now_ms)" "$pending" $(( $(date +%s) - t0 )) >> "$OUT/$RUN.events"
  "$LAB" exec cat /var/jm/roam.jsonl > "$OUT/$RUN.jsonl"
  "$LAB" exec cat /var/jm/roam.jsonl.manifest > "$OUT/$RUN.manifest"
  "$LAB" exec "$BIN/drainprobe" verify -manifest /var/jm/roam.jsonl.manifest -dir "/jfs/$dir" > "$OUT/$RUN.verify.json" || true
  "$LAB" exec sh -c 'wget -q -O - http://127.0.0.1:21050/spool' > "$OUT/$RUN.spool.json" 2>/dev/null || true
  "$LAB" exec cat /var/jm/jm5.log > "$OUT/$RUN.jm5.log" 2>/dev/null || true
  "$LAB" exec cat /var/jm/roam.err > "$OUT/$RUN.probe.err" 2>/dev/null || true
  "$LAB" exec tail -n 400 /var/log/juicefs.log > "$OUT/$RUN.juicefs.log" 2>/dev/null || true
  # Read errors juicefs kept on an open file, and its retries of them (patch
  # 0030), over the whole run (the tail above can miss them).
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'tail -n +'"$((jfs_lines + 1))"' /var/log/juicefs.log | grep -E "read file [0-9]+: |retrying after an error" | tail -n 200' \
    > "$OUT/$RUN.jfs-read-errors.log" 2>/dev/null || true
  stop_jm5
  "$LAB" shape lan >/dev/null
  python3 "$HERE/lib/roamsum.py" "$OUT/$RUN"
}

# held-CASE: no session. One reader keeps ONE descriptor of a cold file open
# on the juicefs mount across the network change (lib/heldread.py: 1 MiB
# reads at ROAM_READ_MIBS, the same offset again every 0.5 s after an
# error), as the Mac app does through macFUSE's shared handle. It shows
# whether juicefs keeps a read error on the open file after the network
# returns (patch 0030). After the event the reader runs ROAM_POST seconds
# (default 60).
run_held() {
  local case="${1#held-}" label="${2:-run}"
  RUN="$label-held-$case"
  mkdir -p "$OUT"
  local file post jfs_lines
  file="$(next_file)"
  post="${ROAM_POST:-60}"
  stop_jm5
  "$LAB" shape lan >/dev/null
  [ "$(current_ip)" = "$CLIENT_IP" ] || { docker network disconnect "$NET" "$(cid)"; reconnect "$CLIENT_IP"; }
  client_net_setup "$WAN_IP"
  jfs_lines="$("$LAB" exec sh -c 'wc -l < /var/log/juicefs.log' 2>/dev/null | tr -dc 0-9)"
  "$LAB" shape "$PROFILE" >/dev/null
  : > "$OUT/$RUN.events"
  mark start
  "$LAB" exec sh -c "rm -f /var/jm/held.jsonl; mkdir -p /var/jm; nohup python3 /lab/lib/heldread.py /jfs/$file /var/jm/held.jsonl ${ROAM_READ_MIBS:-2} \
    >/var/jm/held.err 2>&1 & echo \$! > /var/jm/held.pid"
  sleep "$PRE"
  event "$case"
  sleep "$post"
  mark stop
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'kill -TERM "$(cat /var/jm/held.pid)" 2>/dev/null; sleep 2; kill -9 "$(cat /var/jm/held.pid)" 2>/dev/null; true'
  "$LAB" exec cat /var/jm/held.jsonl > "$OUT/$RUN.held.jsonl"
  "$LAB" exec cat /var/jm/held.err > "$OUT/$RUN.probe.err" 2>/dev/null || true
  "$LAB" exec sh -c 'tail -n +'"$((${jfs_lines:-0} + 1))"' /var/log/juicefs.log | grep -E "read file [0-9]+: |retrying after an error" | tail -n 200' \
    > "$OUT/$RUN.jfs-read-errors.log" 2>/dev/null || true
  "$LAB" shape lan >/dev/null
  python3 "$HERE/lib/heldsum.py" "$OUT/$RUN"
}

case "${1:-}" in
  fixture)
    [ -x "$STATE/bin/drainprobe" ] || die "run ./drain-lab.sh build first"
    "$LAB" shape lan >/dev/null
    log "writing $FILES files of $FILE_MB MiB"
    # shellcheck disable=SC2016
    "$LAB" exec sh -c 'mkdir -p /jfs/roam/src && for i in $(seq -w 1 '"$FILES"'); do
      f=/jfs/roam/src/r$(printf %02d "${i#0}").bin
      [ -s "$f" ] || head -c '"$((FILE_MB << 20))"' /dev/urandom > "$f"; done; sync'
    "$LAB" mount --cold >/dev/null
    echo 1 > "$STATE/roam-next"
    log "fixture ready; the block cache is empty"
    ;;
  run) [ $# -ge 2 ] || die "usage: roam-lab.sh run CASE [LABEL]"
    case "$2" in held-*) run_held "$2" "${3:-run}" ;; *) run_case "$2" "${3:-run}" ;; esac ;;
  all) for c in ${ROAM_CASES:-$CASES}; do run_case "$c" "${2:-run}"; done ;;
  summary) [ $# -eq 2 ] || die "usage: roam-lab.sh summary DIR"; python3 "$HERE/lib/roamsum.py" "$2"/*.events ;;
  *) sed -n '2,38p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
