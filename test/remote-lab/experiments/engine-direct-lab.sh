#!/usr/bin/env bash
# engine-direct-lab.sh — Step 0 of the macFUSE report, on Linux: the JuiceFS
# engine called directly (no mount) against a FUSE mount of the same volume,
# same lab, same hour. engineprobe (../engineprobe) measures both arms.
#
#   engine-direct-lab.sh build          engineprobe (linux/amd64, darwin/arm64)
#                                       and the client's juicefs, from this
#                                       checkout, into $STATE/bin
#   engine-direct-lab.sh up             start the lab (own project and
#                                       subnets), install that juicefs, enroll
#                                       the kind-mac principal, start the relay
#   engine-direct-lab.sh fixture        write the fixture through /jfs (lan)
#   engine-direct-lab.sh run PROFILE N  round N on PROFILE: both arms, cold,
#                                       odd rounds fuse first, even rounds
#                                       direct first; JSON lines in
#                                       results/engine-direct/PROFILE-rN-ARM.jsonl
#   engine-direct-lab.sh run-arm PROFILE N ARM   one arm of round N (the
#                                       link stays shaped; ENG_SUFFIX labels a
#                                       variant such as ENG_FUSE_ENTRY=60)
#   engine-direct-lab.sh relay          (re)start the relay
#   engine-direct-lab.sh down           remove the lab
#
# The arms:
#   fuse    juicefs mount at /jfs-probe, the app's flags (lab.sh MOUNT_FLAGS)
#           with --entry-cache 0 --dir-entry-cache 0 so Linux looks up every
#           path part on each open, as macFUSE does with novncache (bug M1).
#           ENG_FUSE_ENTRY=60 keeps Linux's own name cache instead.
#   direct  engineprobe opens the engine itself with the same flags and its
#           own cache folder; nothing is mounted. ENG_RESOLVE=walk times the
#           name lookups; ino (default) knows the inode first, as the app's
#           mirror does.
# Both log in to the RESP proxy as the kind-mac principal jmlab-mac (as an
# enrolled Mac, never admin) through engineprobe's relay on 127.0.0.1:16379,
# which counts round trips. The peer for T4 is lab.sh's own /jfs mount (admin,
# a separate juicefs process: the teammate).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
export JMLAB_PROJECT="${JMLAB_PROJECT:-jmengine}"
export JMLAB_STATE_DIR="${JMLAB_STATE_DIR:-.state-eng}"
export JMLAB_NAS_SUBNET="${JMLAB_NAS_SUBNET:-10.231.210.0/24}"
export JMLAB_NAS_DYNAMIC="${JMLAB_NAS_DYNAMIC:-10.231.210.128/25}"
export JMLAB_REMOTE_SUBNET="${JMLAB_REMOTE_SUBNET:-10.231.220.0/24}"
export JMLAB_REMOTE_DYNAMIC="${JMLAB_REMOTE_DYNAMIC:-10.231.220.128/25}"
export JMLAB_PROXY_IP="${JMLAB_PROXY_IP:-10.231.210.20}"
export JMLAB_SILO_IP="${JMLAB_SILO_IP:-10.231.210.30}"
export JMLAB_IPERF_IP="${JMLAB_IPERF_IP:-10.231.210.40}"
export JMLAB_WAN_NAS_IP="${JMLAB_WAN_NAS_IP:-10.231.210.2}"
export JMLAB_WAN_REMOTE_IP="${JMLAB_WAN_REMOTE_IP:-10.231.220.2}"
export JMLAB_CLIENT_IP="${JMLAB_CLIENT_IP:-10.231.220.10}"
STATE="$HERE/$JMLAB_STATE_DIR"
LAB="$HERE/lab.sh"
PROBE="/lab/$JMLAB_STATE_DIR/bin/engineprobe-linux-amd64"
MAC_NAME=jmlab-mac
RELAY_META="redis://$MAC_NAME@127.0.0.1:16379/1"
OUT="$HERE/results/engine-direct"
FIX=(-dir engineprobe-fx -clips "${ENG_CLIPS:-12}" -pairs "${ENG_PAIRS:-8}" -edits "${ENG_EDITS:-6}")

log() { printf '[engine] %s\n' "$*" >&2; }
die() { log "$*"; exit 2; }
in_client() { "$LAB" exec "$@"; }

# The app's mount flags, as lab.sh mounts /jfs.
MOUNT_FLAGS=(
  --no-usage-report --no-bgjob --max-deletes 0
  --heartbeat 60s --max-uploads 4 --max-readahead 1M
  --buffer-size 512 --prefetch 1
  --get-timeout 2m --put-timeout 3m --io-retries 3
  --attr-cache 10 --entry-cache "${ENG_FUSE_ENTRY:-0}" --dir-entry-cache "${ENG_FUSE_ENTRY:-0}" --negative-entry-cache 5
  --cache-size 20480 --demand-first
)

build() {
  mkdir -p "$STATE/bin"
  chmod 755 "$STATE" "$STATE/bin"
  "$HERE/engineprobe/build.sh" "$STATE/bin" linux/amd64 darwin/arm64 >/dev/null
  local tag="jmengine-juicefs:$$" cid
  docker build -q --target juicefs -t "$tag" -f "$HERE/image/Dockerfile" "$ROOT" >/dev/null
  cid="$(docker create "$tag")"
  docker cp "$cid:/out/juicefs" "$STATE/bin/juicefs" >/dev/null
  docker rm "$cid" >/dev/null
  docker rmi "$tag" >/dev/null
  chmod 755 "$STATE/bin/juicefs"
  log "built engineprobe and juicefs from $(git -C "$ROOT" rev-parse --short HEAD) into $STATE/bin"
}

start_relay() {
  # shellcheck disable=SC2016
  in_client sh -c 'if wget -q -O - http://127.0.0.1:16380/total >/dev/null 2>&1; then exit 0; fi
    nohup "$1" relay -listen 127.0.0.1:16379 -to "$2:6379" -http 127.0.0.1:16380 >/var/log/relay.log 2>&1 &
    echo $! > /var/run/engineprobe-relay.pid
    for i in $(seq 50); do wget -q -O - http://127.0.0.1:16380/total >/dev/null 2>&1 && exit 0; sleep 0.1; done
    echo "the relay did not start" >&2; exit 1' sh "$PROBE" "$JMLAB_PROXY_IP"
}

up() {
  [ -x "$STATE/bin/engineprobe-linux-amd64" ] && [ -x "$STATE/bin/juicefs" ] || die "run build first"
  "$LAB" up
  "$LAB" umount
  # shellcheck disable=SC2016
  in_client sh -c 'cp "$1" /usr/local/bin/juicefs.new && mv -f /usr/local/bin/juicefs.new /usr/local/bin/juicefs' \
    sh "/lab/$JMLAB_STATE_DIR/bin/juicefs"
  "$LAB" mount --cold >/dev/null
  log "client juicefs: $(in_client juicefs --version)"
  JMLAB_STATE_DIR="$JMLAB_STATE_DIR" "$HERE/experiments/peer-edit-lab.sh" principal
  start_relay
}

# probe_mount: the fuse arm's mount, cold, as the mac principal via the relay.
probe_mount() {
  # shellcheck disable=SC2016
  in_client sh -c '
    if grep -q " /jfs-probe fuse" /proc/mounts; then umount /jfs-probe 2>/dev/null || umount -l /jfs-probe; fi
    sleep 1; rm -rf /var/jfscache-probe; mkdir -p /jfs-probe /var/jfscache-probe
    META_PASSWORD="$(cat /var/jm/mac.pass)" juicefs mount -d "$@" --cache-dir /var/jfscache-probe \
      --log /var/log/juicefs-probe.log >/dev/null
    grep -q " /jfs-probe fuse" /proc/mounts' sh "$RELAY_META" /jfs-probe "${MOUNT_FLAGS[@]}"
}

probe_umount() {
  # shellcheck disable=SC2016
  in_client sh -c 'if grep -q " /jfs-probe fuse" /proc/mounts; then umount /jfs-probe 2>/dev/null || umount -l /jfs-probe; fi' || true
}

COMMON=(-relay http://127.0.0.1:16380 -peer /jfs -allow-write -tests "${ENG_TESTS:-t1t2,t3,t4,t5}"
        -t4-play "${ENG_PLAY:-2}" -t5-n "${ENG_T5_N:-200}" -t5-workers "${ENG_T5_WORKERS:-4,8}")

run_arm() { # run_arm ARM LABEL FILE
  local arm="$1" label="$2" file="$3"
  if [ "$arm" = fuse ]; then
    probe_mount
    in_client "$PROBE" run -arm fuse -mnt /jfs-probe -label "$label" "${FIX[@]}" "${COMMON[@]}" > "$file"
    probe_umount
  else
    # shellcheck disable=SC2016
    in_client sh -c 'rm -rf /var/jfscache-direct && mkdir -p /var/jfscache-direct
      META_PASSWORD="$(cat /var/jm/mac.pass)" exec "$@"' sh \
      "$PROBE" run -arm direct -meta "$RELAY_META" -cache-dir /var/jfscache-direct -resolve "${ENG_RESOLVE:-ino}" \
      -label "$label" "${FIX[@]}" "${COMMON[@]}" > "$file"
  fi
  log "$label $arm: $(wc -l < "$file") lines"
}

run() {
  local profile="$1" round="$2" a b
  mkdir -p "$OUT"
  start_relay
  "$LAB" shape "$profile" >/dev/null
  if [ $((round % 2)) = 1 ]; then a=fuse b=direct; else a=direct b=fuse; fi
  run_arm "$a" "$profile-r$round" "$OUT/$profile-r$round-$a.jsonl"
  run_arm "$b" "$profile-r$round" "$OUT/$profile-r$round-$b.jsonl"
  "$LAB" shape lan >/dev/null
}

# one_arm PROFILE ROUND ARM: one arm only (a long profile in shorter steps).
# ENG_SUFFIX names a variant (for example e60 with ENG_FUSE_ENTRY=60).
one_arm() {
  local profile="$1" round="$2" arm="$3" sfx="${ENG_SUFFIX:+-$ENG_SUFFIX}"
  mkdir -p "$OUT"
  start_relay
  "$LAB" shape "$profile" >/dev/null
  run_arm "$arm" "$profile-r$round$sfx" "$OUT/$profile-r$round-$arm$sfx.jsonl"
}

cmd="${1:-}"; shift || true
case "$cmd" in
  build) build ;;
  up) up ;;
  relay) start_relay ;;
  fixture)
    "$LAB" shape lan >/dev/null
    in_client "$PROBE" fixture -mnt /jfs -allow-write "${FIX[@]}" ;;
  run) [ $# -eq 2 ] || die "usage: run PROFILE ROUND"; run "$1" "$2" ;;
  run-arm) [ $# -eq 3 ] || die "usage: run-arm PROFILE ROUND fuse|direct"; one_arm "$1" "$2" "$3" ;;
  down) probe_umount; "$LAB" down ;;
  *) sed -n '2,30p' "$0" >&2; exit 2 ;;
esac
