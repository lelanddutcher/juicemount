#!/usr/bin/env bash
# peer-edit-lab.sh — how fast a peer's edit becomes visible through the
# JuiceMount NFS layer.
#
#   peer-edit-lab.sh build               build jm5, peerprobe and the client's
#                                        juicefs into $STATE/$DRAIN_BIN
#   peer-edit-lab.sh run PROFILE LABEL   one session (see below); JSON lines
#                                        in results/peer-edit-<time>/LABEL.*
#
# A session remounts the client cold, mounts a SECOND juicefs client of the
# same volume at /jfs-peer (the peer: its own session, open-file cache and
# block cache, as a teammate's Mac or the farm), rewrites the fixture through
# the peer, starts a fresh jm5 over /jfs and shapes the link. Then peerprobe
# (an NFS client of jm5, as the Mac is) runs each case PE_TRIALS times:
#
#   held         jm5 has read the file (its descriptor is pooled), then the
#                peer writes 16 B at 0 and 32 MiB and closes. Poll READ at
#                both offsets until both show the peer's bytes (bug B1), and
#                GETATTR until the mtime changes (B2).
#   fresh        the same edit on a file jm5 never read (B2: mtime only).
#   append       the peer appends 1 MiB to an 8 MiB file jm5 never read.
#                Poll GETATTR until the size grows, READ until the new
#                bytes arrive (B2).
#   append-held  the same append on a file jm5 has open.
#   steady       200 reads of a held file with no edit: the per-read cost.
#
# Around every case the jm5 juicefs's own metadata operation counters
# (/jfs/.stats; the peer has its own) and jm5's /metrics are saved, so the
# summary (lib/peersum.py) gives the metadata round trips each case cost the
# reader. PE_TIMEOUT (default 30s) ends a poll; PE_JM5_ENV adds environment
# to jm5 (for example JM_METADATA_ATTR_PUSH=0). DRAIN_BIN as in drain-lab.sh.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
STATE_NAME="${JMLAB_STATE_DIR:-.state}"
STATE="$HERE/$STATE_NAME"
LAB="$HERE/lab.sh"
PROXY_IP="${JMLAB_PROXY_IP:-10.231.10.20}"
OUT="${PE_OUT:-$HERE/results/peer-edit-$(date +%Y%m%d-%H%M%S)}"
BIN="${DRAIN_BIN:-bin}"
TRIALS="${PE_TRIALS:-3}"
TIMEOUT="${PE_TIMEOUT:-30s}"

log() { printf '[peer-edit] %s\n' "$*" >&2; }
die() { log "$*"; exit 2; }

build() {
  DRAIN_BIN="$BIN" "$HERE/experiments/first-open-lab.sh" build
  (cd "$ROOT" && CGO_ENABLED=0 go build -trimpath -o "$STATE/$BIN/peerprobe" ./test/remote-lab/peerprobe)
  log "built peerprobe into $STATE/$BIN"
}

stop_jm5() {
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'p=$(cat /var/jm/jm5.pid 2>/dev/null) || exit 0
    kill "$p" 2>/dev/null || exit 0
    for i in $(seq 50); do kill -0 "$p" 2>/dev/null || exit 0; sleep 0.2; done
    kill -9 "$p" 2>/dev/null; sleep 1'
}

peer_mount() {
  # A plain juicefs client: no JuiceMount flags, its own cache. It reaches
  # the server through the same wan, so on a far profile its own writes are
  # slow too; every delay is measured from its close().
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'mkdir -p /jfs-peer /var/jfscache-peer
    juicefs mount -d "redis://admin@'"$PROXY_IP"':6379/1" /jfs-peer --cache-dir /var/jfscache-peer \
      --no-usage-report --log /var/log/juicefs-peer.log >/dev/null
    grep -q " /jfs-peer fuse" /proc/mounts'
}

stats() { # stats FILE: the reader juicefs's metadata and object counters
  "$LAB" exec sh -c "grep -E '^juicefs_(meta_ops_total_|transaction_durations_histogram_seconds_total|object_request_durations_histogram_seconds_GET_total)' /jfs/.stats; echo '#jm5'; wget -q -O - http://127.0.0.1:21050/metrics 2>/dev/null | tr -d '\n' | head -c 200000; echo" > "$1" 2>/dev/null || true
}

probe() { "$LAB" exec "/lab/$STATE_NAME/$BIN/peerprobe" "$@"; }

run() {
  local profile="$1" label="$2"
  mkdir -p "$OUT"
  [ -x "$STATE/$BIN/jm5" ] && [ -x "$STATE/$BIN/peerprobe" ] || die "run peer-edit-lab.sh build first"
  stop_jm5
  "$LAB" shape lan >/dev/null
  "$LAB" umount
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'cp "$1" /usr/local/bin/juicefs.new && mv -f /usr/local/bin/juicefs.new /usr/local/bin/juicefs' \
    sh "/lab/$STATE_NAME/$BIN/juicefs"
  "$LAB" mount --cold >/dev/null
  "$LAB" exec sh -c 'rm -rf /var/jfscache-peer /var/jm && mkdir -p /var/jm'
  peer_mount
  probe fixture -peer /jfs-peer -dir pe -n "$TRIALS" > "$OUT/$label.fixture.json"
  local uuid class=slow
  uuid="$("$LAB" exec sh -c 'juicefs status "redis://admin@'"$PROXY_IP"':6379/1" 2>/dev/null' | sed -n 's/.*"UUID": "\([^"]*\)".*/\1/p' | head -n 1)"
  [ -n "$uuid" ] || die "no volume UUID"
  "$LAB" shape "$profile" >/dev/null
  [ "$profile" != lan ] || class=fast
  # shellcheck disable=SC2016
  "$LAB" exec sh -c 'cd /var/jm && HOME=/var/jm JM_CACHE_DIR=/var/jfscache/'"$uuid"' JM_NET_FORCE_CLASS='"$class"' '"${PE_JM5_ENV:-}"' \
    nohup /lab/'"$STATE_NAME/$BIN"'/jm5 --redis "redis://admin:$META_PASSWORD@'"$PROXY_IP"':6379/1" \
      --fuse-path /jfs --no-fuse --external-juicefs-is-bundled --no-mount --listen 127.0.0.1:21049 \
      --metrics-addr 127.0.0.1:21050 --log-level debug --db /var/jm/metadata.db >/var/jm/jm5.log 2>&1 & echo $! > /var/jm/jm5.pid'
  local up=0
  for _ in $(seq 120); do
    if "$LAB" exec sh -c 'nc -z 127.0.0.1 21049' 2>/dev/null; then up=1; break; fi
    sleep 1
  done
  [ "$up" = 1 ] || { "$LAB" exec tail -n 40 /var/jm/jm5.log >&2; die "jm5 did not start"; }
  sleep "${PE_SETTLE_S:-10}"
  local xpath
  xpath="$("$LAB" exec sed -n 's/.*"export":"\(\/jm-[0-9a-f]*\)".*/\1/p' /var/jm/jm5.log | head -n 1)"
  [ -n "$xpath" ] || die "jm5 logged no export path"
  log "$label: $profile, $TRIALS trials per case, timeout $TIMEOUT"
  local i n kase file
  for i in $(seq 1 "$TRIALS"); do
    n="$(printf '%02d' "$i")"
    for kase in steady held fresh append append-held; do
      case "$kase" in
        steady) file="pe/f$n.bin" ;;
        held) file="pe/f$n.bin" ;;
        # fresh needs a file jm5 never read: the next trial's f file is
        # read by its own held case only later.
        fresh) file="pe/f$(printf '%02d' $(( (i % TRIALS) + 1 ))).bin" ;;
        append) file="pe/a$n.bin" ;;
        append-held) file="pe/a$n.bin" ;;
      esac
      # fresh on the last trial would reuse f01, already read: skip it.
      if [ "$kase" = fresh ] && [ "$i" = "$TRIALS" ]; then continue; fi
      stats "$OUT/$label.$kase.$n.before.txt"
      probe run -export "$xpath" -case "$kase" -file "$file" -timeout "$TIMEOUT" >> "$OUT/$label.jsonl" \
        || log "$label: $kase $file failed"
      sleep 1
      stats "$OUT/$label.$kase.$n.after.txt"
    done
  done
  "$LAB" exec cat /var/jm/jm5.log > "$OUT/$label.jm5.log" 2>/dev/null || true
  stop_jm5
  "$LAB" exec sh -c 'umount /jfs-peer 2>/dev/null || umount -l /jfs-peer; true'
  "$LAB" shape lan >/dev/null
  python3 "$HERE/lib/peersum.py" "$OUT" "$label" | tee "$OUT/$label.summary.txt"
  log "$label: results in $OUT"
}

cmd="${1:-}"; shift || true
case "$cmd" in
  build) build ;;
  run) [ $# -eq 2 ] || die "usage: peer-edit-lab.sh run PROFILE LABEL"; run "$@" ;;
  *) sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
