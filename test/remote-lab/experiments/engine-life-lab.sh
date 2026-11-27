#!/usr/bin/env bash
# engine-life-lab.sh — the engine helper's life cycle (engine-direct step 1,
# checks E1–E6): `juicefs jm-engine` started, watched, killed, restarted and
# stopped by the app's supervisor (internal/enginehost), beside the lab's
# unchanged FUSE mount at /jfs.
#
#   engine-life-lab.sh build              build enginelife and jm5 into
#                                         $STATE/bin; copy the client's juicefs
#                                         (it must have jm-engine) beside jm5
#   engine-life-lab.sh probe PROFILE LABEL  E1, E2, E3, E4 and E6 (SIGSTOP)
#                                         with enginelife; E5 on lan
#   engine-life-lab.sh link LABEL         E6 link: a ready helper while the
#                                         link is down for 60 s (profile down)
#   engine-life-lab.sh app PROFILE LABEL  through jm5: unset JM_ENGINE starts
#                                         no helper; JM_ENGINE=shadow: E1, E2
#                                         (the NFS server and /jfs stay up),
#                                         E3 on quit and on kill -9 of jm5
#   engine-life-lab.sh all                probe lan, probe abroad, link, app
#                                         lan, app abroad
#
# Results: JSON lines in results/engine-life-<time>/LABEL.jsonl. EL_TRIALS
# sets trials per check (default 5), EL_PINGS the E4 pings (default 10000).
# jm5 and the probe log in as the kind-mac principal (peer-edit-lab.sh
# principal), never as admin.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
STATE_NAME="${JMLAB_STATE_DIR:-.state}"
STATE="$HERE/$STATE_NAME"
LAB="$HERE/lab.sh"
PROXY_IP="${JMLAB_PROXY_IP:-10.231.10.20}"
OUT="${EL_OUT:-$HERE/results/engine-life-$(date +%Y%m%d-%H%M%S)}"
TRIALS="${EL_TRIALS:-5}"
MAC_NAME=jmlab-mac
META="redis://$MAC_NAME@$PROXY_IP:6379/1"
BIN="/lab/$STATE_NAME/bin"

log() { printf '[engine-life] %s\n' "$*" >&2; }
die() { log "$*"; exit 2; }

build() {
  mkdir -p "$STATE/bin"
  chmod 755 "$STATE" "$STATE/bin" # the client's root and nobody read it through /lab
  (cd "$ROOT" && CGO_ENABLED=0 go build -trimpath -tags nosqlite,nomysql,nopg,notikv,nobadger,noetcd \
    -o "$STATE/bin/jm5" ./cmd/jm5 && CGO_ENABLED=0 go build -trimpath -o "$STATE/bin/enginelife" \
    ./test/remote-lab/enginelife)
  docker cp "$("$LAB" _cid client):/usr/local/bin/juicefs" "$STATE/bin/juicefs"
  "$LAB" exec "$BIN/juicefs" jm-engine --help >/dev/null 2>&1 || die "the client's juicefs has no jm-engine (rebuild the lab image)"
  log "built jm5 and enginelife from $(git -C "$ROOT" rev-parse --short HEAD); $("$LAB" exec "$BIN/juicefs" --version)"
}

setup() {
  [ -x "$STATE/bin/enginelife" ] && [ -x "$STATE/bin/jm5" ] || die "run engine-life-lab.sh build first"
  mkdir -p "$OUT"
  stop_jm5
  "$LAB" shape lan >/dev/null
  "$LAB" exec sh -c 'mkdir -p /var/jm'
  "$HERE/experiments/peer-edit-lab.sh" principal
}

probe() { "$LAB" exec "$BIN/enginelife" "$@" -meta "$META" -pass-file /var/jm/mac.pass -n "$TRIALS"; }

run_probe() {
  local profile="$1" label="$2"
  setup
  "$LAB" shape "$profile" >/dev/null
  log "$label: enginelife life on $profile, $TRIALS trials (E2 and E6 wait 61 s between trials)"
  probe life -label "$label" -pings "${EL_PINGS:-10000}" >> "$OUT/$label.jsonl" 2>> "$OUT/$label.stderr" \
    || log "$label: life failed"
  if [ "$profile" = lan ]; then
    probe auth -label "$label" >> "$OUT/$label.jsonl" 2>> "$OUT/$label.stderr" || log "$label: auth failed"
  fi
  "$LAB" shape lan >/dev/null
  "$LAB" exec sh -c 'cat /var/jm/enginelife-helper.log 2>/dev/null' > "$OUT/$label.helper.log" || true
  log "$label: results in $OUT/$label.jsonl"
}

run_link() {
  local label="$1"
  setup
  log "$label: a ready helper; link down 60 s from +15 s"
  probe hold -label "$label" -seconds 100 >> "$OUT/$label.jsonl" 2>> "$OUT/$label.stderr" &
  local p=$!
  for _ in $(seq 120); do grep -q 'hold: ready' "$OUT/$label.stderr" 2>/dev/null && break; sleep 0.5; done
  sleep 15
  "$LAB" shape down >/dev/null
  log "$label: link down"
  sleep 60
  "$LAB" shape lan >/dev/null
  log "$label: link up"
  wait "$p" || log "$label: hold failed"
  tail -n 1 "$OUT/$label.jsonl"
}

stop_jm5() {
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'p=$(cat /var/jm/jm5.pid 2>/dev/null) || exit 0
    kill "$p" 2>/dev/null || exit 0
    for i in $(seq 150); do kill -0 "$p" 2>/dev/null || exit 0; sleep 0.2; done
    kill -9 "$p" 2>/dev/null; sleep 1'
}

# start_jm5 ENGINE_LEVEL: jm5 over the lab's /jfs, as the mac principal.
start_jm5() {
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'rm -rf /var/jm/db && mkdir -p /var/jm/db && cd /var/jm && : > /var/jm/jm5.log &&
    HOME=/var/jm JM_ENGINE="$1" nohup '"$BIN"'/jm5 --redis "redis://'"$MAC_NAME"':$(cat /var/jm/mac.pass)@'"$PROXY_IP"':6379/1" \
      --fuse-path /jfs --no-fuse --external-juicefs-is-bundled --no-mount --listen 127.0.0.1:21049 \
      --metrics-addr 127.0.0.1:21050 --log-level debug --db /var/jm/db/metadata.db >/var/jm/jm5.log 2>&1 &
    echo $! > /var/jm/jm5.pid' sh "$1"
  for _ in $(seq 120); do
    "$LAB" exec sh -c 'nc -z 127.0.0.1 21049' 2>/dev/null && return 0
    sleep 1
  done
  "$LAB" exec tail -n 40 /var/jm/jm5.log >&2
  die "jm5 did not start"
}

# In the client: wait for the Nth "engine helper ready" line; print its
# start_ms and the wall time since $T0 (ns).
wait_ready_line() {
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'n=$1; t0=$2
    for i in $(seq 1200); do
      c=$(grep -c "engine helper ready" /var/jm/jm5.log || true)
      if [ "$c" -ge "$n" ]; then
        s=$(grep "engine helper ready" /var/jm/jm5.log | sed -n "${n}p" | sed "s/.*\"start_ms\":\([0-9]*\).*/\1/")
        echo "$s $(( ($(date +%s%N) - t0) / 1000000 ))"; exit 0
      fi
      sleep 0.05
    done; echo "none none"' sh "$1" "$2"
}

now_ns() { "$LAB" exec date +%s%N | tr -d '\r\n'; }
helper_pid() { "$LAB" exec sh -c 'pgrep -f "juicefs [j]m-engine" | head -n 1' | tr -d '\r\n'; }

# In the client: ms until PID is gone (or "none" after 60 s).
gone_ms() {
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'p=$1; t0=$2
    for i in $(seq 6000); do kill -0 "$p" 2>/dev/null || { echo $(( ($(date +%s%N) - t0) / 1000000 )); exit 0; }; sleep 0.01; done
    echo none' sh "$1" "$2"
}

emit() { printf '%s\n' "$1" >> "$OUT/$2.jsonl"; }

run_app() {
  local profile="$1" label="$2" t0 line pid g up
  setup
  "$LAB" shape "$profile" >/dev/null

  # Default off: no JM_ENGINE, no helper, no engine log line.
  start_jm5 ""
  sleep 10
  pid="$(helper_pid)"
  line=$("$LAB" exec sh -c 'grep -c "engine helper" /var/jm/jm5.log || true' | tr -d '\r\n')
  emit "{\"check\":\"default-off\",\"label\":\"$label\",\"helper_process\":\"${pid:-none}\",\"engine_log_lines\":$line}" "$label"
  stop_jm5

  for i in $(seq 0 $((TRIALS - 1))); do
    t0="$(now_ns)"
    start_jm5 shadow
    read -r s w <<<"$(wait_ready_line 1 "$t0")"
    emit "{\"check\":\"E1-app\",\"label\":\"$label\",\"trial\":$i,\"start_ms\":\"$s\",\"since_jm5_start_ms\":\"$w\"}" "$label"
    if [ "$i" = 0 ]; then
      # E2 through the app: kill -9 the helper; the NFS server and /jfs stay.
      for k in 2 3 4; do
        pid="$(helper_pid)"
        t0="$(now_ns)"
        "$LAB" exec kill -9 "$pid"
        read -r s w <<<"$(wait_ready_line "$k" "$t0")"
        up=$("$LAB" exec sh -c 'nc -z 127.0.0.1 21049 && grep -q " /jfs fuse" /proc/mounts && ls /jfs >/dev/null && echo true || echo false' | tr -d '\r\n')
        emit "{\"check\":\"E2-app\",\"label\":\"$label\",\"kill\":$((k - 1)),\"ready_ms\":\"$w\",\"start_ms\":\"$s\",\"nfs_and_volume_up\":$up}" "$label"
        [ "$k" = 4 ] || sleep 61 # back-off reset
      done
    fi
    # E3 through the app: quit (SIGTERM) on even trials, kill -9 on odd.
    pid="$(helper_pid)"
    t0="$(now_ns)"
    if [ $((i % 2)) = 0 ]; then
      # shellcheck disable=SC2016
      "$LAB" exec sh -c 'kill "$(cat /var/jm/jm5.pid)"'
      g="$(gone_ms "$pid" "$t0" | tr -d '\r\n')"
      emit "{\"check\":\"E3-app-quit\",\"label\":\"$label\",\"trial\":$i,\"helper_gone_ms\":\"$g\"}" "$label"
      stop_jm5
    else
      # shellcheck disable=SC2016
      "$LAB" exec sh -c 'kill -9 "$(cat /var/jm/jm5.pid)"'
      g="$(gone_ms "$pid" "$t0" | tr -d '\r\n')"
      emit "{\"check\":\"E3-app-killed\",\"label\":\"$label\",\"trial\":$i,\"helper_gone_ms\":\"$g\"}" "$label"
    fi
    "$LAB" exec sh -c 'cat /var/jm/jm5.log' > "$OUT/$label.jm5-$i.log" 2>/dev/null || true
  done
  "$LAB" exec sh -c 'cat /var/jm/db/juicefs-engine.log 2>/dev/null' > "$OUT/$label.helper.log" || true
  "$LAB" shape lan >/dev/null
  log "$label: results in $OUT/$label.jsonl"
}

cmd="${1:-}"; shift || true
case "$cmd" in
  build) build ;;
  probe) [ $# -eq 2 ] || die "usage: engine-life-lab.sh probe PROFILE LABEL"; run_probe "$@" ;;
  link) [ $# -eq 1 ] || die "usage: engine-life-lab.sh link LABEL"; run_link "$@" ;;
  app) [ $# -eq 2 ] || die "usage: engine-life-lab.sh app PROFILE LABEL"; run_app "$@" ;;
  all)
    run_probe lan probe-lan
    run_probe abroad probe-abroad
    run_link link-lan
    run_app lan app-lan
    run_app abroad app-abroad
    ;;
  *) sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
