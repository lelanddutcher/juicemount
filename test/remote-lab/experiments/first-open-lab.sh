#!/usr/bin/env bash
# first-open-lab.sh — measure and trace a cold open-to-first-byte through the
# JuiceMount NFS layer.
#
#   first-open-lab.sh build              build jm5, drainprobe and the client's
#                                        juicefs into $STATE/bin (DRAIN_BIN)
#   first-open-lab.sh fixture [N] [MB]   write N clips of MB MiB (default 6 x 32)
#                                        to /jfs/fo on the lan profile
#   first-open-lab.sh run PROFILE LABEL  one session: cold remount, fresh jm5,
#                                        then open the clips one by one
#
# A session starts like a relaunched app on a new day: an empty block cache,
# fresh JuiceFS metadata caches, a fresh jm5 (its mirror and cache mappings
# empty). drainprobe firstbyte then opens every clip in turn: LOOKUP, READ the
# first 128 KiB (first byte) and the rest of the first MiB. Clip 1 pays the
# cold folder lookups; the later clips show the per-file cost.
#
# jm5 runs as drain-lab.sh runs it, with the direct SSD cache reader on
# (JM_CACHE_DIR and --external-juicefs-is-bundled), so the path qualification
# and the mapping lookup the app pays on a cold read happen here too.
#
# Per session, results/first-open-<time>/<label>.* (git-ignored) holds the probe
# output, the jm5 log (its "slow cached read" lines split each read into
# qualification, mapping, FUSE open and FUSE read), a Redis MONITOR trace, the
# object store's request trace and the JuiceFS access log. FO_TRACE=0 skips
# the three traces. FO_JM5_ENV adds environment to jm5. DRAIN_BIN as in
# drain-lab.sh picks another build folder of $STATE.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
STATE_NAME="${JMLAB_STATE_DIR:-.state}"
STATE="$HERE/$STATE_NAME"
LAB="$HERE/lab.sh"
PROXY_IP="${JMLAB_PROXY_IP:-10.231.10.20}"
OUT="${FO_OUT:-$HERE/results/first-open-$(date +%Y%m%d-%H%M%S)}"
BIN="${DRAIN_BIN:-bin}"

log() { printf '[first-open] %s\n' "$*" >&2; }
die() { log "$*"; exit 2; }

build() {
  mkdir -p "$STATE/$BIN"
  chmod 755 "$STATE" "$STATE/$BIN"
  (cd "$ROOT" && CGO_ENABLED=0 go build -trimpath -tags nosqlite,nomysql,nopg,notikv,nobadger,noetcd \
    -o "$STATE/$BIN/jm5" ./cmd/jm5 && CGO_ENABLED=0 go build -trimpath -o "$STATE/$BIN/drainprobe" \
    ./test/remote-lab/drainprobe)
  # The client's juicefs, built from this checkout as the lab image builds
  # it. Each session mounts /jfs with the juicefs of its build folder, and
  # the cache reader checks the version of that same file beside jm5.
  local tag="jmlab-juicefs:$BIN-$$" cid
  docker build -q --target juicefs -t "$tag" -f "$HERE/image/Dockerfile" "$ROOT" >/dev/null
  cid="$(docker create "$tag")"
  docker cp "$cid:/out/juicefs" "$STATE/$BIN/juicefs" >/dev/null
  docker rm "$cid" >/dev/null
  docker rmi "$tag" >/dev/null
  chmod 755 "$STATE/$BIN/juicefs"
  log "juicefs: $("$LAB" exec "/lab/$STATE_NAME/$BIN/juicefs" --version)"
  log "built jm5 and drainprobe from $(git -C "$ROOT" rev-parse --short HEAD) into $STATE/$BIN"
}

fixture() {
  local n="${1:-6}" mb="${2:-32}" i
  "$LAB" shape lan >/dev/null
  for i in $(seq -f '%02g' 1 "$n"); do
    "$LAB" exec sh -c "mkdir -p /jfs/fo && [ -f /jfs/fo/c$i.mov ] || head -c $((mb << 20)) /dev/urandom > /jfs/fo/c$i.mov"
  done
  log "fixture: $n clips of $mb MiB in /jfs/fo"
}

stop_jm5() {
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'p=$(cat /var/jm/jm5.pid 2>/dev/null) || exit 0
    kill "$p" 2>/dev/null || exit 0
    for i in $(seq 50); do kill -0 "$p" 2>/dev/null || exit 0; sleep 0.2; done
    kill -9 "$p" 2>/dev/null; sleep 1'
}

silo_cid() { "$LAB" _cid silo; }
redis_cid() { "$LAB" _cid redis; }

trace_start() {
  # MONITOR as the server's loopback admin (AUTH is redacted by Redis).
  # shellcheck disable=SC2016
  docker exec -d "$(redis_cid)" sh -c 'REDISCLI_AUTH="$(cat /run/jm-redis/admin.pass)" redis-cli --no-auth-warning --user jm-admin monitor > /tmp/monitor.txt'
  # Every object request with its server-side time; the credentials stay in
  # the container's environment.
  # shellcheck disable=SC2016
  docker exec -d "$(silo_cid)" sh -c 'MC_HOST_l="http://$MINIO_ROOT_USER:$MINIO_ROOT_PASSWORD@127.0.0.1:9000" mc admin trace --json l > /tmp/s3trace.json 2>/dev/null'
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'nohup cat /jfs/.accesslog > /var/jm/access.txt 2>/dev/null & echo $! > /var/jm/access.pid'
  sleep 1
}

trace_stop() {
  local label="$1"
  docker exec "$(redis_cid)" sh -c 'pkill -f "[r]edis-cli.*monitor" || true'
  docker cp "$(redis_cid):/tmp/monitor.txt" "$OUT/$label.monitor.txt" >/dev/null
  docker exec "$(redis_cid)" rm -f /tmp/monitor.txt
  docker exec "$(silo_cid)" sh -c 'pkill -f "[m]c admin trace" || true'
  docker cp "$(silo_cid):/tmp/s3trace.json" "$OUT/$label.s3trace.json" >/dev/null || true
  docker exec "$(silo_cid)" rm -f /tmp/s3trace.json
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'kill "$(cat /var/jm/access.pid)" 2>/dev/null; true'
  "$LAB" exec cat /var/jm/access.txt > "$OUT/$label.access.txt" 2>/dev/null || true
}

run() {
  local profile="$1" label="$2"
  mkdir -p "$OUT"
  [ -x "$STATE/$BIN/jm5" ] || die "run first-open-lab.sh build first"
  stop_jm5
  "$LAB" shape lan >/dev/null
  "$LAB" umount
  # A rename, not a copy over the file: an exiting juicefs may still hold it.
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'cp "$1" /usr/local/bin/juicefs.new && mv -f /usr/local/bin/juicefs.new /usr/local/bin/juicefs' \
    sh "/lab/$STATE_NAME/$BIN/juicefs"
  "$LAB" mount --cold >/dev/null
  "$LAB" exec sh -c 'rm -rf /var/jm && mkdir -p /var/jm'
  local uuid files
  uuid="$("$LAB" exec sh -c 'juicefs status "redis://admin@'"$PROXY_IP"':6379/1" 2>/dev/null' | sed -n 's/.*"UUID": "\([^"]*\)".*/\1/p' | head -n 1)"
  [ -n "$uuid" ] || die "no volume UUID"
  files="$("$LAB" exec sh -c 'cd /jfs && ls fo/*.mov' | paste -sd, -)"
  [ -n "$files" ] || die "no fixture: run first-open-lab.sh fixture"
  "$LAB" shape "$profile" >/dev/null
  local class=slow
  [ "$profile" != lan ] || class=fast
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'cd /var/jm && HOME=/var/jm JM_CACHE_DIR=/var/jfscache/'"$uuid"' JM_NET_FORCE_CLASS='"$class"' '"${FO_JM5_ENV:-}"' \
    nohup /lab/'"$STATE_NAME/$BIN"'/jm5 --redis "redis://admin:$META_PASSWORD@'"$PROXY_IP"':6379/1" \
      --fuse-path /jfs --no-fuse --external-juicefs-is-bundled --no-mount --listen 127.0.0.1:21049 \
      --metrics-addr 127.0.0.1:21050 --log-level debug --db /var/jm/metadata.db >/var/jm/jm5.log 2>&1 & echo $! > /var/jm/jm5.pid'
  local up=0
  for _ in $(seq 120); do
    if "$LAB" exec sh -c 'nc -z 127.0.0.1 21049' 2>/dev/null; then up=1; break; fi
    sleep 1
  done
  [ "$up" = 1 ] || { "$LAB" exec tail -n 40 /var/jm/jm5.log >&2; die "jm5 did not start"; }
  # Let start-up work (mirror sync, cache qualification) finish, as a user
  # opens a clip some seconds after the volume appears.
  sleep "${FO_SETTLE_S:-8}"
  local xpath
  xpath="$("$LAB" exec sed -n 's/.*"export":"\(\/jm-[0-9a-f]*\)".*/\1/p' /var/jm/jm5.log | head -n 1)"
  [ -n "$xpath" ] || die "jm5 logged no export path"
  [ "${FO_TRACE:-1}" = 0 ] || trace_start
  local c0 c1
  c0="$("$LAB" proxy-counter)"
  log "$label: open $files ($profile)"
  "$LAB" exec /lab/"$STATE_NAME/$BIN"/drainprobe firstbyte -export "$xpath" -nfs 127.0.0.1:21049 \
    -files "$files" > "$OUT/$label.firstbyte.jsonl"
  c1="$("$LAB" proxy-counter)"
  printf '{"before":%s,"after":%s}\n' "$c0" "$c1" > "$OUT/$label.counter.json"
  [ "${FO_TRACE:-1}" = 0 ] || trace_stop "$label"
  "$LAB" exec cat /var/jm/jm5.log > "$OUT/$label.jm5.log" 2>/dev/null || true
  stop_jm5
  "$LAB" shape lan >/dev/null
  cat "$OUT/$label.firstbyte.jsonl"
  log "$label: results in $OUT"
}

cmd="${1:-}"; shift || true
case "$cmd" in
  build) build ;;
  fixture) fixture "$@" ;;
  run) [ $# -eq 2 ] || die "usage: first-open-lab.sh run PROFILE LABEL"; run "$@" ;;
  *) sed -n '2,27p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
