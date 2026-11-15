#!/usr/bin/env bash
# own-save-lab.sh — the Mac's own saves through the JuiceMount NFS layer
# (laptop bug B1, 2026-11-14: abroad, a save made the whole volume answer EIO).
#
#   own-save-lab.sh build               build jm5, peerprobe and the client's
#                                       juicefs into $STATE/$DRAIN_BIN
#   own-save-lab.sh run PROFILE LABEL   one session; JSON lines in
#                                       results/own-save-<time>/LABEL.jsonl
#
# A session remounts the client cold, starts a fresh jm5 over /jfs with the
# write spool on (as the app runs), logged in to the proxy as a kind-mac
# principal (as peer-edit-lab.sh does), shapes the link and runs
# `peerprobe ownsave` (see peerprobe/ownsave.go): atomic temp-file saves,
# in-place saves, chmod and rename, OS_TRIALS times (default 3), with a
# 256 KiB file saved every second in another folder meanwhile. The summary
# line says whether the volume root ever answered an error (volume_eio) and
# whether it still did at the end (root_ok_at_end). OS_SEED_LAN=0 writes the
# in-place, chmod and rename targets through jm5 instead of seeding them on
# lan. OS_CASES picks the operations (default atomic,inplace,chmod,rename).
# OS_JM5_ENV adds jm5 environment. DRAIN_BIN as in drain-lab.sh.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_NAME="${JMLAB_STATE_DIR:-.state}"
STATE="$HERE/$STATE_NAME"
LAB="$HERE/lab.sh"
PROXY_IP="${JMLAB_PROXY_IP:-10.231.10.20}"
OUT="${OS_OUT:-$HERE/results/own-save-$(date +%Y%m%d-%H%M%S)}"
BIN="${DRAIN_BIN:-bin}"
MAC_NAME=jmlab-mac

log() { printf '[own-save] %s\n' "$*" >&2; }
die() { log "$*"; exit 2; }

stop_jm5() {
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'p=$(cat /var/jm/jm5.pid 2>/dev/null) || exit 0
    kill "$p" 2>/dev/null || exit 0
    for i in $(seq 50); do kill -0 "$p" 2>/dev/null || exit 0; sleep 0.2; done
    kill -9 "$p" 2>/dev/null; sleep 1'
}

run() {
  local profile="$1" label="$2"
  mkdir -p "$OUT"
  [ -x "$STATE/$BIN/jm5" ] && [ -x "$STATE/$BIN/peerprobe" ] || die "run own-save-lab.sh build first"
  stop_jm5
  "$LAB" shape lan >/dev/null
  "$LAB" umount
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'cp "$1" /usr/local/bin/juicefs.new && mv -f /usr/local/bin/juicefs.new /usr/local/bin/juicefs' \
    sh "/lab/$STATE_NAME/$BIN/juicefs"
  "$LAB" mount --cold >/dev/null
  "$LAB" exec sh -c 'rm -rf /var/jm && mkdir -p /var/jm/spool && mkdir -p /jfs/w && rm -rf /jfs/w/os-'"$label"' /jfs/w/os-'"$label"'-load'
  if [ "${OS_SEED_LAN:-1}" = 1 ]; then
    # The in-place, chmod and rename targets, written on lan straight into
    # /jfs (setup through jm5 abroad takes minutes); jm5's start-up SCAN
    # mirrors them.
    # shellcheck disable=SC2016
    "$LAB" exec sh -c 'd=/jfs/w/os-'"$label"'; mkdir -p "$d"; for i in $(seq 0 $(('"${OS_TRIALS:-3}"' - 1))); do
      for k in b c r; do head -c 2097152 /dev/urandom > "$d/$k$i.prproj"; done; done'
  fi
  "$HERE/experiments/peer-edit-lab.sh" principal
  local uuid class=slow
  uuid="$("$LAB" exec sh -c 'juicefs status "redis://admin@'"$PROXY_IP"':6379/1" 2>/dev/null' | sed -n 's/.*"UUID": "\([^"]*\)".*/\1/p' | head -n 1)"
  [ -n "$uuid" ] || die "no volume UUID"
  "$LAB" shape "$profile" >/dev/null
  [ "$profile" != lan ] || class=fast
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'cd /var/jm && HOME=/var/jm JM_CACHE_DIR=/var/jfscache/'"$uuid"' JM_NET_FORCE_CLASS='"$class"' \
    JM_SPOOL_ENABLE=1 JM_SPOOL_DIR=/var/jm/spool JM_SPOOL_SIZE_GB=8 '"${OS_JM5_ENV:-}"' \
    nohup /lab/'"$STATE_NAME/$BIN"'/jm5 --redis "redis://'"$MAC_NAME"':$(cat /var/jm/mac.pass)@'"$PROXY_IP"':6379/1" \
      --fuse-path /jfs --no-fuse --external-juicefs-is-bundled --no-mount --listen 127.0.0.1:21049 \
      --metrics-addr 127.0.0.1:21050 --log-level debug --db /var/jm/metadata.db >/var/jm/jm5.log 2>&1 & echo $! > /var/jm/jm5.pid'
  local up=0
  for _ in $(seq 120); do
    if "$LAB" exec sh -c 'nc -z 127.0.0.1 21049' 2>/dev/null; then up=1; break; fi
    sleep 1
  done
  [ "$up" = 1 ] || { "$LAB" exec tail -n 40 /var/jm/jm5.log >&2; die "jm5 did not start"; }
  sleep "${OS_SETTLE_S:-10}"
  local xpath
  xpath="$("$LAB" exec sed -n 's/.*"export":"\(\/jm-[0-9a-f]*\)".*/\1/p' /var/jm/jm5.log | head -n 1)"
  [ -n "$xpath" ] || die "jm5 logged no export path"
  log "$label: $profile, ${OS_TRIALS:-3} trials"
  "$LAB" exec "/lab/$STATE_NAME/$BIN/peerprobe" ownsave -export "$xpath" -dir "w/os-$label" \
    -trials "${OS_TRIALS:-3}" -watch "${OS_WATCH:-20s}" -cases "${OS_CASES:-atomic,inplace,chmod,rename}" > "$OUT/$label.jsonl" || log "$label: probe failed"
  "$LAB" exec cat /var/jm/jm5.log > "$OUT/$label.jm5.log" 2>/dev/null || true
  stop_jm5
  "$LAB" shape lan >/dev/null
  grep -c '"level":"ERROR"' "$OUT/$label.jm5.log" > "$OUT/$label.errors.txt" || true
  grep -hoE 'mirror observation no longer current|err loading attrs for [^"]*' "$OUT/$label.jm5.log" | sort | uniq -c >> "$OUT/$label.errors.txt" || true
  tail -n 1 "$OUT/$label.jsonl"
  log "$label: results in $OUT"
}

cmd="${1:-}"; shift || true
case "$cmd" in
  build) "$HERE/experiments/peer-edit-lab.sh" build ;;
  run) [ $# -eq 2 ] || die "usage: own-save-lab.sh run PROFILE LABEL"; run "$@" ;;
  *) sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
