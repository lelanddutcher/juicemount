#!/usr/bin/env bash
# drain-lab.sh — measure the NFS write path (folder creation, the write spool
# and its drain) over a lab profile.
#
#   drain-lab.sh build            build jm5 and drainprobe into $STATE/bin
#   drain-lab.sh run PROFILE [LABEL]
#
# The remote lab alone runs JuiceFS. This script adds the JuiceMount NFS
# layer: it runs cmd/jm5 (the CLI server, with the write spool on) inside the
# client container, over the client's /jfs mount, and drives it over loopback
# NFSv3 with drainprobe. Every byte jm5 sends to the server still crosses the
# lab's wan.
#
# One run: shape the link, remount cold, start jm5 on an empty spool, then
#   1. drainprobe write: DRAIN_MKDIRS (default 3) timed mkdirs (a folder and
#      empty folders inside it), 200 files of 2–63 KiB in the first, one file
#      of DRAIN_BIG_MB (default 500) MB;
#   2. drainprobe wait: poll the spool database until nothing is left to send, at most
#      DRAIN_CAP seconds (default 600).
# With DRAIN_READ_S=N, the run first reads a DRAIN_SEED_MB (default 300) MB
# file over NFS for N seconds (scrub, then sequential), as an editor would
# before saving. The drain then shows what the read path leaves behind.
# DRAIN_FOLDERS=N spreads the small files over N new folders inside the run's
# folder (default 0: all in one folder). DRAIN_ACCESSLOG=1 keeps the JuiceFS
# access log (every FUSE operation with its duration) as <label>.access.txt.
# DRAIN_JM5_ENV adds environment to jm5 (an experiment, such as
# JM_DRAIN_SYNC_BYTES=2097152); JMLAB_MOUNT_OPTS adds juicefs mount flags.
# The RESP proxy counters are read before and after each step, and a Redis
# MONITOR trace of the whole run is kept (DRAIN_MONITOR=0 turns it off).
# Results go to results/drain-<time>/<label>.* (git-ignored).
#
# Needs a lab that is up (lab.sh up), with the same JMLAB_PROJECT and
# JMLAB_STATE_DIR as this script sees.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
STATE_NAME="${JMLAB_STATE_DIR:-.state}"
STATE="$HERE/$STATE_NAME"
LAB="$HERE/lab.sh"
PROXY_IP="${JMLAB_PROXY_IP:-10.231.10.20}"
CAP="${DRAIN_CAP:-600}"
BIG="${DRAIN_BIG_MB:-500}"
SMALL="${DRAIN_SMALL:-200}"
READ_S="${DRAIN_READ_S:-0}"
SEED_MB="${DRAIN_SEED_MB:-300}"
OUT="${DRAIN_OUT:-$HERE/results/drain-$(date +%Y%m%d-%H%M%S)}"

log() { printf '[drain] %s\n' "$*" >&2; }
die() { log "$*"; exit 2; }

build() {
  mkdir -p "$STATE/bin"
  chmod 755 "$STATE" "$STATE/bin" # the client's root reads it through /lab
  (cd "$ROOT" && CGO_ENABLED=0 go build -trimpath -tags nosqlite,nomysql,nopg,notikv,nobadger,noetcd \
    -o "$STATE/bin/jm5" ./cmd/jm5 && CGO_ENABLED=0 go build -trimpath -o "$STATE/bin/drainprobe" \
    ./test/remote-lab/drainprobe)
  log "built jm5 and drainprobe from $(git -C "$ROOT" rev-parse --short HEAD)"
}

counter() { "$LAB" proxy-counter; }

redis_cid() { "$LAB" _cid redis; }

stop_jm5() {
  # By pid file: pgrep -f reads /proc/<pid>/cmdline, which blocks while jm5
  # waits in a FUSE call. A stopped run leaves the spool to the next wipe.
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'p=$(cat /var/jm/jm5.pid 2>/dev/null) || exit 0
    kill "$p" 2>/dev/null || exit 0
    for i in $(seq 50); do kill -0 "$p" 2>/dev/null || exit 0; sleep 0.2; done
    kill -9 "$p" 2>/dev/null; sleep 1'
}

run() {
  local profile="$1" label="${2:-$1}"
  mkdir -p "$OUT"
  # DRAIN_BIN names another folder of $STATE holding a jm5 (an older build,
  # for a before/after pair).
  [ -x "$STATE/${DRAIN_BIN:-bin}/jm5" ] || die "run drain-lab.sh build first"
  stop_jm5
  "$LAB" shape lan >/dev/null
  "$LAB" mount --cold >/dev/null
  # Fresh spool and mirror; the parent folder exists on the server.
  "$LAB" exec sh -c 'rm -rf /var/jm && mkdir -p /var/jm/spool && mkdir -p /jfs/w'
  if [ "$READ_S" -gt 0 ]; then
    # A file to read before the writes, written at lan speed; the second
    # cold mount empties the block cache so the reads cross the link.
    # shellcheck disable=SC2016
    "$LAB" exec sh -c '[ -f /jfs/seed.bin ] || head -c '"$((SEED_MB << 20))"' /dev/urandom > /jfs/seed.bin'
    "$LAB" mount --cold >/dev/null
  fi
  "$LAB" shape "$profile" >/dev/null
  # The app classes every lab WAN profile "slow" (laptop lab); jm5 has no
  # link probe of its own here, so pin the class the drainer reads.
  local class=slow
  [ "$profile" != lan ] || class=fast
  # jm5 reaches Redis through the RESP proxy, across the wan, as the app does.
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'cd /var/jm && HOME=/var/jm JM_SPOOL_ENABLE=1 JM_SPOOL_DIR=/var/jm/spool JM_SPOOL_SIZE_GB=8 JM_NET_FORCE_CLASS='"$class"' JM_DRAIN_PHASE_LOG=1 '"${DRAIN_JM5_ENV:-}"' \
    nohup /lab/'"$STATE_NAME/${DRAIN_BIN:-bin}"'/jm5 --redis "redis://admin:$META_PASSWORD@'"$PROXY_IP"':6379/1" \
      --fuse-path /jfs --no-fuse --no-mount --listen 127.0.0.1:21049 --metrics-addr 127.0.0.1:21050 \
      --db /var/jm/metadata.db >/var/jm/jm5.log 2>&1 & echo $! > /var/jm/jm5.pid'
  local up=0
  for _ in $(seq 120); do
    if "$LAB" exec sh -c 'nc -z 127.0.0.1 21049 2>/dev/null || (echo > /dev/tcp/127.0.0.1/21049) 2>/dev/null || wget -q -O /dev/null http://127.0.0.1:21050/health' 2>/dev/null; then up=1; break; fi
    sleep 1
  done
  [ "$up" = 1 ] || { "$LAB" exec tail -n 40 /var/jm/jm5.log >&2; die "jm5 did not start"; }
  sleep 3
  local xpath
  xpath="$("$LAB" exec sed -n 's/.*"export":"\(\/jm-[0-9a-f]*\)".*/\1/p' /var/jm/jm5.log | head -n 1)"
  [ -n "$xpath" ] || die "jm5 logged no export path"

  local mon=""
  if [ "${DRAIN_MONITOR:-1}" = 1 ]; then
    # MONITOR as the server's loopback admin: the trace holds commands and
    # keys, never a password (AUTH is redacted by Redis).
    # shellcheck disable=SC2016
    docker exec -d "$(redis_cid)" sh -c 'REDISCLI_AUTH="$(cat /run/jm-redis/admin.pass)" redis-cli --no-auth-warning --user jm-admin monitor > /tmp/monitor.txt'
    mon=1
  fi
  if [ "$READ_S" -gt 0 ]; then
    log "$label: read for ${READ_S}s ($profile)"
    "$LAB" exec /lab/"$STATE_NAME"/bin/drainprobe read -export "$xpath" -nfs 127.0.0.1:21049 \
      -file seed.bin -budget "${READ_S}s" > "$OUT/$label.read.json"
  fi
  if [ "${DRAIN_ACCESSLOG:-0}" = 1 ]; then
    # shellcheck disable=SC2016
    "$LAB" exec sh -c 'nohup cat /jfs/.accesslog > /var/jm/access.txt 2>/dev/null & echo $! > /var/jm/access.pid'
  fi
  local dir
  dir="w/$label-$(date +%s)"
  local c0 c1 c2
  c0="$(counter)"
  log "$label: write ($profile)"
  "$LAB" exec /lab/"$STATE_NAME"/bin/drainprobe write -export "$xpath" -nfs 127.0.0.1:21049 -dir "$dir" \
    -mkdirs "${DRAIN_MKDIRS:-3}" -folders "${DRAIN_FOLDERS:-0}" -small "$SMALL" -big-mb "$BIG" > "$OUT/$label.write.json"
  c1="$(counter)"
  log "$label: wait for the drain (cap ${CAP}s)"
  "$LAB" exec /lab/"$STATE_NAME"/bin/drainprobe wait -db /var/jm/metadata.db -fuse /jfs -dir "$dir" \
    -metrics http://127.0.0.1:21050/metrics -jfsstats /jfs/.stats \
    -count $((SMALL + (BIG > 0 ? 1 : 0))) -timeout "${CAP}s" > "$OUT/$label.wait.json"
  c2="$(counter)"
  printf '{"before":%s,"after_write":%s,"after_drain":%s}\n' "$c0" "$c1" "$c2" > "$OUT/$label.counter.json"
  if [ -n "$mon" ]; then
    docker exec "$(redis_cid)" sh -c 'pkill -f "[r]edis-cli.*monitor" || true'
    docker cp "$(redis_cid):/tmp/monitor.txt" "$OUT/$label.monitor.txt" >/dev/null
    docker exec "$(redis_cid)" rm -f /tmp/monitor.txt
  fi
  "$LAB" exec cat /var/jm/jm5.log > "$OUT/$label.jm5.log" 2>/dev/null || true
  if [ "${DRAIN_ACCESSLOG:-0}" = 1 ]; then
    # shellcheck disable=SC2016
    "$LAB" exec sh -c 'kill "$(cat /var/jm/access.pid)" 2>/dev/null; true'
    "$LAB" exec cat /var/jm/access.txt > "$OUT/$label.access.txt" 2>/dev/null || true
  fi
  stop_jm5
  "$LAB" shape lan >/dev/null
  cat "$OUT/$label.write.json" "$OUT/$label.wait.json"
  log "$label: results in $OUT"
}

cmd="${1:-}"; shift || true
case "$cmd" in
  build) build ;;
  run) [ $# -ge 1 ] || die "usage: drain-lab.sh run PROFILE [LABEL]"; run "$@" ;;
  *) sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
