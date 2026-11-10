#!/usr/bin/env bash
# lab.sh — start, shape, mount and stop the JuiceMount remote lab.
#
#   lab.sh up                 build images, start the lab, format the volume,
#                             mount the client (on the lan profile)
#   lab.sh shape PROFILE      apply a profile from profiles.tsv to the wan
#   lab.sh mount [--cold|--warm]  (re)mount the client; --cold empties its
#                             block cache first, --warm (default) keeps it
#   lab.sh umount             unmount the client
#   lab.sh exec CMD...        run a command in the client container
#   lab.sh proxy-counter      the RESP proxy's command/flush counters (JSON)
#   lab.sh status             containers and the current wan qdiscs
#   lab.sh down               stop and remove every container, network and
#                             volume of the lab, and its .state directory
#
# Everything lives in Docker project "${JMLAB_PROJECT:-jmlab}". The secrets
# are throwaway, generated into .state/lab.env (git-ignored, mode 600).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE="$HERE/.state"
ENV_FILE="$STATE/lab.env"
export JMLAB_PROJECT="${JMLAB_PROJECT:-jmlab}"
PROXY_IP="${JMLAB_PROXY_IP:-10.231.10.20}"
SILO_IP="${JMLAB_SILO_IP:-10.231.10.30}"
META_URL="redis://admin@$PROXY_IP:6379/1"

log() { printf '[lab] %s\n' "$*" >&2; }
die() { log "$*"; exit 2; }

dc() {
  [ -f "$ENV_FILE" ] || die "the lab is not set up: run lab.sh up"
  JMLAB_COMMIT="$(git -C "$HERE" rev-parse HEAD 2>/dev/null || printf '%040d' 0)" \
    docker compose -f "$HERE/compose.yml" --env-file "$ENV_FILE" "$@"
}
in_client() { dc exec -T client "$@"; }

gen_env() {
  [ -f "$ENV_FILE" ] && return 0
  mkdir -p "$STATE"
  chmod 700 "$STATE"
  local f k
  f="$(mktemp "$STATE/lab.env.XXXXXX")"
  {
    for k in JM_REDIS_PROXY_PASSWORD JM_PROXY_FARM_PASSWORD JM_PROXY_MANAGER_PASSWORD \
             JM_PROXY_BG_PASSWORD JM_PROXY_WEBDAV_PASSWORD JM_PROXY_ADMIN_PASSWORD JMLAB_S3_PASSWORD; do
      printf '%s=%s\n' "$k" "$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
    done
    printf 'JMLAB_S3_USER=jmlab\n'
  } > "$f"
  chmod 600 "$f"
  mv "$f" "$ENV_FILE"
}

env_get() { sed -n "s/^$1=//p" "$ENV_FILE"; }

# The NAS-side containers send to the client through the wan. Turn off their
# segmentation offloads (in their own network namespaces) so the wan shapes
# and drops real 1500-byte packets, not 64 KB super-packets: loss applied to
# a super-packet would drop 40 packets at once.
nas_offloads_off() {
  local svc cid
  for svc in respproxy silo iperf; do
    cid="$(dc ps -q "$svc")"
    [ -n "$cid" ] || die "service $svc is not running"
    docker run --rm --network "container:$cid" --cap-add NET_ADMIN --entrypoint /bin/sh jmlab:local \
      -c 'ethtool -K eth0 tso off gso off gro off >/dev/null 2>&1 || true'
  done
}

wait_proxy() {
  local cid
  cid="$(dc ps -q respproxy)"
  for _ in $(seq 60); do
    docker exec "$cid" wget -q -O /dev/null http://127.0.0.1:9180/stats 2>/dev/null && return 0
    sleep 1
  done
  dc logs --tail 40 respproxy >&2
  die "the RESP proxy did not come up"
}

format_volume() {
  if in_client juicefs status "$META_URL" >/dev/null 2>&1; then
    log "volume already formatted"
    return 0
  fi
  local _ enc=()
  # JMLAB_ENCRYPT=1: an encrypted volume (aes256gcm-rsa) with a throwaway
  # RSA key in .state, which the client sees at /lab/.state.
  if [ "${JMLAB_ENCRYPT:-0}" = 1 ]; then
    command -v openssl >/dev/null || die "JMLAB_ENCRYPT=1 needs openssl on this host"
    [ -f "$STATE/encrypt.pem" ] || (umask 077 && openssl genrsa -out "$STATE/encrypt.pem" 2048 2>/dev/null) \
      || die "openssl genrsa failed"
    chmod 644 "$STATE/encrypt.pem" # read by the client container's root; .state itself is 700
    enc=(--encrypt-rsa-key /lab/.state/encrypt.pem)
    log "formatting an encrypted volume"
  fi
  for _ in $(seq 30); do
    if dc exec -T -e ACCESS_KEY="$(env_get JMLAB_S3_USER)" -e SECRET_KEY="$(env_get JMLAB_S3_PASSWORD)" client \
         juicefs format --storage minio --bucket "http://$SILO_IP:9000/jmlab" --trash-days 0 \
         "${enc[@]}" --no-update "$META_URL" jmlab >/dev/null 2>"$STATE/format.err"; then
      log "volume formatted"
      return 0
    fi
    sleep 2
  done
  cat "$STATE/format.err" >&2
  die "juicefs format failed"
}

# The client mount: the flags the Mac app gives its own juicefs child
# (health/fuse.go: desktopJuiceFSMountArgs, session, readahead and metadata
# cache TTLs), with the slow-link class policy for buffer, prefetch and
# object timeouts (internal/netprofile, desktopJuiceFSObjectIOArgs).
# JMLAB_MOUNT_OPTS appends to them.
MOUNT_FLAGS=(
  --no-usage-report --no-bgjob --max-deletes 0
  --heartbeat 60s --max-uploads 4 --max-readahead 1M
  --buffer-size 512 --prefetch 1
  --get-timeout 2m --put-timeout 3m --io-retries 3
  --attr-cache 10 --entry-cache 60 --dir-entry-cache 60 --negative-entry-cache 5
  --cache-dir /var/jfscache --cache-size 20480
)

do_umount() {
  # shellcheck disable=SC2016
  in_client sh -c '
    if grep -q " /jfs fuse" /proc/mounts; then umount /jfs 2>/dev/null || umount -l /jfs; fi
    for i in $(seq 100); do pgrep -x juicefs >/dev/null || exit 0; sleep 0.1; done
    pkill -9 -x juicefs; sleep 1' || true
}

do_mount() {
  local cold=0
  [ "${1:-}" != --cold ] || cold=1
  do_umount
  if [ "$cold" = 1 ]; then in_client sh -c 'rm -rf /var/jfscache && mkdir -p /var/jfscache'; fi
  # Timed inside the container; prints mount_ms=N. `juicefs mount -d`
  # returns once the mount answers.
  # shellcheck disable=SC2016,SC2086
  in_client sh -c 's=$(date +%s%N); juicefs mount -d "$@" >/dev/null || exit 1; e=$(date +%s%N)
    grep -q " /jfs fuse" /proc/mounts || { echo "/jfs is not mounted" >&2; exit 1; }
    echo "mount_ms=$(( (e - s) / 1000000 ))"' mount \
    "$META_URL" /jfs "${MOUNT_FLAGS[@]}" ${JMLAB_MOUNT_OPTS:-} --log /var/log/juicefs.log \
    2>"$STATE/mount.err" || {
      cat "$STATE/mount.err" >&2; in_client tail -n 30 /var/log/juicefs.log >&2 || true
      die "mount failed"; }
}

cmd="${1:-}"; shift || true
case "$cmd" in
  up)
    command -v docker >/dev/null || die "docker is required"
    gen_env
    log "building images and starting project $JMLAB_PROJECT"
    dc up -d --build --wait --quiet-pull >&2
    nas_offloads_off
    wait_proxy
    format_volume
    do_mount
    log "up: client mounted /jfs on the lan profile"
    ;;
  shape)
    [ $# -eq 1 ] || die "usage: lab.sh shape PROFILE"
    dc exec -T wan /lab/lib/shape.sh "$1"
    ;;
  mount)
    case "${1:-}" in ''|--cold|--warm) ;; *) die "usage: lab.sh mount [--cold|--warm]" ;; esac
    do_mount "${1:-}" ;;
  umount) do_umount ;;
  exec) in_client "$@" ;;
  proxy-counter)
    docker exec "$(dc ps -q respproxy)" wget -q -O - http://127.0.0.1:9180/counter
    ;;
  status)
    dc ps
    dc exec -T wan tc qdisc show
    ;;
  down)
    if [ -f "$ENV_FILE" ]; then
      do_umount 2>/dev/null || true
      dc down --volumes --remove-orphans --timeout 5 >&2
    fi
    rm -rf "$STATE"
    log "down: project $JMLAB_PROJECT removed"
    ;;
  *) sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
