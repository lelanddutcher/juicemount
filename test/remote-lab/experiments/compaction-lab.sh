#!/usr/bin/env bash
# compaction-lab.sh — what a read of a teammate's fragmented file costs the
# reader's link in compaction (juicefs patch 0036, --no-read-compaction).
#
#   compaction-lab.sh run PROFILE LABEL   one session; results in
#                                         results/compaction-<time>/LABEL.*
#   compaction-lab.sh gc LABEL            compact the volume as the server
#                                         does (juicefs gc --compact, on the
#                                         lan profile) and record each lab
#                                         file's slice count
#
# Build first with peer-edit-lab.sh build (jm5, peerprobe and the client's
# juicefs into $STATE/$DRAIN_BIN).
#
# A session remounts the client cold with the juicefs of DRAIN_BIN, mounts a
# second juicefs client (the peer: a teammate's Mac) at /jfs-peer, and the
# peer writes CW_FILES files per case of CW_MIB MiB each, 1 MiB per open and
# close, so each chunk holds CW_MIB slices (the laptop's 48 MiB file had 48).
# Then jm5 starts over /jfs, as on a Mac: its link follower sends juicefs the
# live link policy (control message 1106), with read compaction off away from
# the backend after this patch. The link is shaped to PROFILE. Per file:
#
#   race   the client reads the first CW_PRIME_KIB KiB (as the laptop's
#          prime read), and CW_PEER_DELAY s later the peer writes 16 B at 0
#          and closes (the teammate keeps editing). A compaction the read
#          started is then wasted.
#   quiet  the same read with no peer write: a compaction commits.
#   contend  the same read; CW_PEER_DELAY s later a teammate NEAR the NAS (a
#          juicefs in its own container on the NAS network, upstream
#          defaults) writes 16 B at 0 and reads the first 64 KiB, as the
#          laptop run's teammate did (perw.py checks the new bytes). Its read
#          compacts the chunk over the fast path and commits first; a
#          compaction the far client started loses the head check
#          (redis.go doCompactChunk) and is wasted. A plain write never
#          wastes it: the check reads only the head of the slice list, and a
#          write appends.
#
# After each read the script waits until the client's object traffic has been
# still for 8 s (at most CW_WAIT s). It records, from the client juicefs's
# .stats, the object GET and PUT bytes and the compactions started
# (compact_size_histogram), from its log the wasted compactions, and from
# Redis the chunk's slice count before and after. Then the client reads the
# whole file and its md5 must equal that of a local copy of what the peer
# wrote (read correctness). The peer itself never reads: its plain juicefs
# would compact the chunk on the read.
#
# JM_NET_FORCE_CLASS of jm5 is fast on lan and slow otherwise, as in
# peer-edit-lab.sh; the near/far decision of the follower also uses the RTT
# jm5 measures. CW_JM5=0 runs no jm5 (the mount flags alone decide; use
# JMLAB_MOUNT_OPTS=--no-read-compaction for the mount flag).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_NAME="${JMLAB_STATE_DIR:-.state}"
STATE="$HERE/$STATE_NAME"
LAB="$HERE/lab.sh"
PE="$HERE/experiments/peer-edit-lab.sh"
PROXY_IP="${JMLAB_PROXY_IP:-10.231.10.20}"
OUT="${CW_OUT:-$HERE/results/compaction-$(date +%Y%m%d-%H%M%S)}"
BIN="${DRAIN_BIN:-bin}"
FILES="${CW_FILES:-2}"
MIB="${CW_MIB:-48}"
CASES="${CW_CASES:-race quiet}"
WAIT="${CW_WAIT:-900}"
MAC_NAME=jmlab-mac

log() { printf '[compaction] %s\n' "$*" >&2; }
die() { log "$*"; exit 2; }

cx() { "$LAB" exec "$@"; }

NEAR="${JMLAB_PROJECT:-jmlab}-near"
near() { docker exec "$NEAR" "$@"; }

near_up() { # a teammate's juicefs on the NAS network (contend case)
  docker rm -f "$NEAR" >/dev/null 2>&1 || true
  local envf="$STATE/near.env"
  (umask 077 && sed -n 's/^JM_PROXY_ADMIN_PASSWORD=/META_PASSWORD=/p' "$STATE/lab.env" > "$envf")
  docker run -d --name "$NEAR" --network "${JMLAB_PROJECT:-jmlab}_nas" --device /dev/fuse --cap-add SYS_ADMIN \
    --security-opt apparmor:unconfined --env-file "$envf" --entrypoint /bin/sh jmlab:local -ec \
    'mkdir -p /jfs-near /var/jfscache-near
     juicefs mount -d "redis://admin@'"$PROXY_IP"':6379/1" /jfs-near --cache-dir /var/jfscache-near \
       --no-usage-report --log /var/log/juicefs-near.log >/dev/null
     exec sleep infinity' >/dev/null
  rm -f "$envf"
  for _ in $(seq 30); do near grep -q " /jfs-near fuse" /proc/mounts 2>/dev/null && return 0; sleep 1; done
  die "the near teammate did not mount"
}

redis_admin() {
  # shellcheck disable=SC2016
  docker exec "$("$LAB" _cid redis)" sh -c 'REDISCLI_AUTH="$(cat /run/jm-redis/admin.pass)" redis-cli --no-auth-warning --user jm-admin -n 1 "$@"' sh "$@"
}

stop_jm5() {
  # shellcheck disable=SC2016
  cx sh -c 'p=$(cat /var/jm/jm5.pid 2>/dev/null) || exit 0
    kill "$p" 2>/dev/null || exit 0
    for i in $(seq 50); do kill -0 "$p" 2>/dev/null || exit 0; sleep 0.2; done
    kill -9 "$p" 2>/dev/null; sleep 1'
}

# counters: "get put compactions compacted_bytes wasted" of the client juicefs.
counters() {
  # shellcheck disable=SC2016
  cx sh -c '
    s=$(cat /jfs/.stats)
    v() { printf "%s\n" "$s" | awk -v k="$1" "\$1==k {print \$2; f=1} END {if (!f) print 0}"; }
    w=$(grep -c "compaction for .* is wasted" /var/log/juicefs.log 2>/dev/null || true)
    echo "$(v juicefs_object_request_data_bytes_GET) $(v juicefs_object_request_data_bytes_PUT) $(v juicefs_compact_size_histogram_bytes_total) $(v juicefs_compact_size_histogram_bytes_sum) ${w:-0}"'
}

slices() { # slices INODE: the slice count of chunk 0
  redis_admin llen "c${1}_0" | tr -d '\r'
}

# settle BEFORE INODE CASE: wait, at most WAIT s, until a compaction the read
# started has ended (the chunk went to 2 slices or fewer, except in the
# contend case where the near teammate's compaction does that, a wasted line was
# logged, or the client logged a failed compaction) or none started, and then
# until GET+PUT bytes have not moved for 8 s. A far upload of one compaction
# takes minutes, with pauses longer than 8 s between 4 MiB PUTs.
settle() {
  local before="$1" ino="$2" kase="$3" last="" same=0 t=0 c now ended=0
  local started0 wasted0 fails0
  started0="$(echo "$before" | awk '{print $3}')"
  wasted0="$(echo "$before" | awk '{print $5}')"
  fails0="$(cx sh -c 'grep -c "<WARNING>: compact [0-9]" /var/log/juicefs.log || true' | tr -d '\r')"
  while [ "$t" -lt "$WAIT" ]; do
    now="$(counters)"
    c="$(echo "$now" | awk '{print $1 + $2}')"
    if [ "$c" = "$last" ]; then same=$((same + 1)); else same=0; last="$c"; fi
    if [ "$ended" = 0 ]; then
      if [ "$(echo "$now" | awk '{print $3}')" = "$started0" ] && [ "$same" -ge 4 ]; then ended=1
      elif { [ "$kase" != contend ] && [ "$(slices "$ino")" -le 2 ]; } || [ "$(echo "$now" | awk '{print $5}')" != "$wasted0" ] \
        || [ "$(cx sh -c 'grep -c "<WARNING>: compact [0-9]" /var/log/juicefs.log || true' | tr -d '\r')" != "$fails0" ]; then ended=1
      fi
    fi
    [ "$ended" = 0 ] || [ "$same" -lt 4 ] || return 0
    sleep 2; t=$((t + 2))
  done
  log "the compaction or the traffic did not end in ${WAIT}s"
}

peer_mount() {
  # shellcheck disable=SC2016
  cx sh -c 'mkdir -p /jfs-peer /var/jfscache-peer
    juicefs mount -d "redis://admin@'"$PROXY_IP"':6379/1" /jfs-peer --cache-dir /var/jfscache-peer \
      --no-usage-report --log /var/log/juicefs-peer.log >/dev/null
    grep -q " /jfs-peer fuse" /proc/mounts'
}

run() {
  local profile="$1" label="$2"
  mkdir -p "$OUT"
  [ -x "$STATE/$BIN/jm5" ] && [ -x "$STATE/$BIN/juicefs" ] || die "run peer-edit-lab.sh build first"
  stop_jm5
  "$LAB" shape lan >/dev/null
  cx sh -c 'umount /jfs-peer 2>/dev/null || umount -l /jfs-peer 2>/dev/null; true'
  "$LAB" umount
  # shellcheck disable=SC2016
  cx sh -c 'cp "$1" /usr/local/bin/juicefs.new && mv -f /usr/local/bin/juicefs.new /usr/local/bin/juicefs' \
    sh "/lab/$STATE_NAME/$BIN/juicefs"
  cx sh -c ': > /var/log/juicefs.log'
  "$LAB" mount --cold >/dev/null
  cx sh -c 'rm -rf /var/jfscache-peer /var/jm && mkdir -p /var/jm'
  "$PE" principal
  peer_mount
  local version
  version="$(cx juicefs --version | tr -d '\r')"
  log "$label: $version, $profile, $FILES files x {$CASES}, $MIB slices each"

  # The fixture, on lan: each file written by the peer 1 MiB per open/close.
  local kase i f
  cx sh -c "rm -rf /jfs-peer/cw-$label /var/cw && mkdir -p /jfs-peer/cw-$label /var/cw"
  for kase in $CASES; do
    for i in $(seq 1 "$FILES"); do
      f="/jfs-peer/cw-$label/$kase-$i.bin"
      # shellcheck disable=SC2016
      cx sh -c 'mkdir -p /var/cw; src="/var/cw/$3"
        head -c $(($2 * 1048576)) /dev/urandom > "$src"
        for j in $(seq 0 $(($2 - 1))); do
          dd if="$src" of="$1" bs=1M count=1 skip=$j seek=$j conv=notrunc,fsync status=none || exit 1
        done' sh "$f" "$MIB" "$label-$kase-$i"
    done
  done

  if [ "${CW_JM5:-1}" = 1 ]; then
    local uuid class=slow
    [ "$profile" != lan ] || class=fast
    uuid="$(cx sh -c 'juicefs status "redis://admin@'"$PROXY_IP"':6379/1" 2>/dev/null' | sed -n 's/.*"UUID": "\([^"]*\)".*/\1/p' | head -n 1)"
    "$LAB" shape "$profile" >/dev/null
    # shellcheck disable=SC2016
    cx sh -c 'cd /var/jm && HOME=/var/jm JM_CACHE_DIR=/var/jfscache/'"$uuid"' JM_NET_FORCE_CLASS='"$class"' '"${CW_JM5_ENV:-}"' \
      nohup /lab/'"$STATE_NAME/$BIN"'/jm5 --redis "redis://'"$MAC_NAME"':$(cat /var/jm/mac.pass)@'"$PROXY_IP"':6379/1" \
        --fuse-path /jfs --no-fuse --external-juicefs-is-bundled --no-mount --listen 127.0.0.1:21049 \
        --metrics-addr 127.0.0.1:21050 --log-level debug --db /var/jm/metadata.db >/var/jm/jm5.log 2>&1 & echo $! > /var/jm/jm5.pid'
    local up=0
    for _ in $(seq 120); do
      if cx sh -c 'nc -z 127.0.0.1 21049' 2>/dev/null; then up=1; break; fi
      sleep 1
    done
    [ "$up" = 1 ] || { cx tail -n 40 /var/jm/jm5.log >&2; die "jm5 did not start"; }
  else
    "$LAB" shape "$profile" >/dev/null
  fi
  case " $CASES " in *" contend "*) near_up ;; esac
  sleep "${CW_SETTLE_S:-15}"
  # The policy in force: the follower's last switch, if any.
  cx sh -c 'grep -h "juicefs link policy\|read compaction" /var/jm/jm5.log /var/log/juicefs.log 2>/dev/null | tail -n 5' > "$OUT/$label.policy.txt" || true

  local ino before after s0 s1 t0 t1
  for kase in $CASES; do
    for i in $(seq 1 "$FILES"); do
      f="cw-$label/$kase-$i.bin"
      ino="$(cx stat -c %i "/jfs-peer/$f" | tr -d '\r')"
      s0="$(slices "$ino")"
      before="$(counters)"
      t0="$(date +%s.%N)"
      if [ "$kase" = race ]; then
        # shellcheck disable=SC2016
        cx sh -c '(sleep "$3"; printf "PEERTAG-%08d" "$4" | dd of="$2" bs=16 count=1 conv=notrunc,fsync status=none
            printf "PEERTAG-%08d" "$4" | dd of="$6" bs=16 count=1 conv=notrunc status=none) &
          dd if="$1" of=/dev/null bs=1k count="$5" status=none; wait' \
          sh "/jfs/$f" "/jfs-peer/$f" "${CW_PEER_DELAY:-2}" "$i" "${CW_PRIME_KIB:-64}" "/var/cw/$label-$kase-$i"
      elif [ "$kase" = contend ]; then
        # shellcheck disable=SC2016
        near sh -c 'sleep "$2"; printf "PEERTAG-%08d" "$3" | dd of="$1" bs=16 count=1 conv=notrunc,fsync status=none
          dd if="$1" of=/dev/null bs=1k count=64 status=none' sh "/jfs-near/$f" "${CW_PEER_DELAY:-2}" "$i" &
        cx dd if="/jfs/$f" of=/dev/null bs=1k count="${CW_PRIME_KIB:-64}" status=none
        wait
        # shellcheck disable=SC2016
        cx sh -c 'printf "PEERTAG-%08d" "$1" | dd of="$2" bs=16 count=1 conv=notrunc status=none' sh "$i" "/var/cw/$label-$kase-$i"
      else
        cx dd if="/jfs/$f" of=/dev/null bs=1k count="${CW_PRIME_KIB:-64}" status=none
      fi
      settle "$before" "$ino" "$kase"
      t1="$(date +%s.%N)"
      after="$(counters)"
      s1="$(slices "$ino")"
      python3 - "$label" "$profile" "$kase" "$f" "$s0" "$s1" "$before" "$after" "$t0" "$t1" <<'PY' >> "$OUT/$label.jsonl"
import json, sys
label, profile, kase, f, s0, s1, b, a, t0, t1 = sys.argv[1:]
b = [float(x) for x in b.split()]; a = [float(x) for x in a.split()]
d = [y - x for x, y in zip(b, a)]
print(json.dumps({"label": label, "profile": profile, "case": kase, "file": f,
    "slices_before": int(s0), "slices_after": int(s1),
    "get_bytes": int(d[0]), "put_bytes": int(d[1]),
    "compactions_started": int(d[2]), "compacted_bytes": int(d[3]), "wasted": int(d[4]),
    "committed": int(s1) <= 2,
    "settle_s": round(float(t1) - float(t0), 1)}))
PY
      tail -n 1 "$OUT/$label.jsonl" >&2
    done
  done
  # Read correctness, after every file was measured: the client's whole-file
  # md5 against the local copy. On a build that compacts, these reads start
  # compactions of their own; they come after the numbers above.
  sleep 11 # the client's attribute cache (10 s) after the last peer write
  local sum_peer sum_client
  for kase in $CASES; do
    for i in $(seq 1 "$FILES"); do
      f="cw-$label/$kase-$i.bin"
      sum_peer="$(cx md5sum "/var/cw/$label-$kase-$i" | cut -d' ' -f1)"
      sum_client="$(cx md5sum "/jfs/$f" | cut -d' ' -f1)"
      printf '{"label": "%s", "file": "%s", "md5_ok": %s}\n' "$label" "$f" \
        "$([ "$sum_peer" = "$sum_client" ] && echo true || echo false)" >> "$OUT/$label.md5.jsonl"
    done
  done
  cat "$OUT/$label.md5.jsonl" >&2
  cx cat /var/jm/jm5.log > "$OUT/$label.jm5.log" 2>/dev/null || true
  cx grep -E "compact|wasted|read compaction" /var/log/juicefs.log > "$OUT/$label.juicefs-compaction.log" 2>/dev/null || true
  stop_jm5
  docker rm -f "$NEAR" >/dev/null 2>&1 || true
  cx sh -c 'umount /jfs-peer 2>/dev/null || umount -l /jfs-peer; true'
  "$LAB" shape lan >/dev/null
  log "$label: results in $OUT/$label.jsonl"
}

# gc LABEL: compact the whole volume as the server's scheduled maintenance
# does (juicefs gc --compact), from the lan side, and record the slice count
# of every lab file before and after.
gc() {
  local label="$1" f ino
  mkdir -p "$OUT"
  "$LAB" shape lan >/dev/null
  peer_mount
  : > "$OUT/$label.gc.txt"
  local files
  files="$(cx sh -c 'ls -1 /jfs-peer/cw-*/*.bin 2>/dev/null' | tr -d '\r')"
  for f in $files; do
    ino="$(cx stat -c %i "$f" | tr -d '\r')"
    echo "before $f $(slices "$ino")" >> "$OUT/$label.gc.txt"
  done
  cx sh -c 'juicefs gc --compact "redis://admin@'"$PROXY_IP"':6379/1" 2>&1 | tail -n 5' >> "$OUT/$label.gc.txt"
  for f in $files; do
    ino="$(cx stat -c %i "$f" | tr -d '\r')"
    echo "after $f $(slices "$ino")" >> "$OUT/$label.gc.txt"
  done
  cx sh -c 'umount /jfs-peer 2>/dev/null || umount -l /jfs-peer; true'
  cat "$OUT/$label.gc.txt"
}

cmd="${1:-}"; shift || true
case "$cmd" in
  run) [ $# -eq 2 ] || die "usage: compaction-lab.sh run PROFILE LABEL"; run "$@" ;;
  gc) [ $# -eq 1 ] || die "usage: compaction-lab.sh gc LABEL"; gc "$@" ;;
  *) sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
