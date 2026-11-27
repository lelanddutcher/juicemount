#!/usr/bin/env bash
# engine-reads-lab.sh — engine-direct step 2 (JM_ENGINE=reads) through the
# app (jm5) against the same app with the level off, same lab, same hour.
#
#   engine-reads-lab.sh build             jm5, drainprobe, enginelife into
#                                         $STATE/bin; the client's juicefs (it
#                                         must serve the socket: -jm23 or later)
#   engine-reads-lab.sh fixture           clips and files through /jfs (lan)
#   engine-reads-lab.sh run PROFILE ARM LABEL
#                                         one session: ARM off or reads
#   engine-reads-lab.sh door LABEL        E4 (read) and both doors: enginelife
#                                         starts its own mount with the socket
#   engine-reads-lab.sh argv FILE         write the argv of lab.sh's own /jfs
#                                         mount (in the client) for
#                                         peer-edit-lab.sh / own-save-lab.sh:
#                                         ER_ENGINE_ARGV=FILE makes them unmount
#                                         /jfs and let jm5 mount it; add
#                                         JM_ENGINE=reads to PE_/OS_JM5_ENV for
#                                         the reads arm
#
# Both arms are the same: jm5 starts `juicefs mount` at /jfs-app itself (as
# the app does), logged in as the kind-mac principal (never admin), with the
# app's flags and --entry-cache 0 --dir-entry-cache 0 (macFUSE's novncache:
# the kernel looks up every path part). Arm reads adds JM_ENGINE=reads, so the
# same mount also serves the engine socket and jm5 reads through it. The
# lab's own /jfs mount (admin) is the teammate.
#
# A session (results/engine-reads-<time>/LABEL.*): cold cache, fresh jm5,
# shape PROFILE, then
#   firstbyte  6 clips, first 128 KiB then up to 1 MiB (T2, T2L; T1 from
#              jm5's "slow cached read" open time over the RTT)
#   pair       T3 (lan only): two 1 MiB READs at once over one
#   seq        S1: first clip and next clip, 2 READs in flight, ER_SEQ_S each
#   c1         C1: every fixture file read with SHA-256 while the teammate
#              rewrites three edit files and the Mac saves (drainprobe write);
#              then the edit files after a settle
#   warm       W1 (lan only): the first clip again, warm
#   long       F: a clip read while the teammate saves it in pieces (abroad)
#   offline    the link down, auto offline, a warm file read again
# ER_STEPS picks steps (default all that apply to the profile).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
STATE_NAME="${JMLAB_STATE_DIR:-.state}"
STATE="$HERE/$STATE_NAME"
LAB="$HERE/lab.sh"
PROXY_IP="${JMLAB_PROXY_IP:-10.231.10.20}"
OUT="${ER_OUT:-$HERE/results/engine-reads-$(date +%Y%m%d-%H%M%S)}"
BIN="/lab/$STATE_NAME/bin"
MAC_NAME=jmlab-mac
MNT=/jfs-app
log() { printf '[engine-reads] %s\n' "$*" >&2; }
die() { log "$*"; exit 2; }
in_client() { "$LAB" exec "$@"; }

APP_FLAGS=(
  --no-usage-report --no-bgjob --max-deletes 0
  --heartbeat 60s --max-uploads 4 --max-readahead 1M
  --buffer-size 512 --prefetch 1
  --get-timeout 2m --put-timeout 3m --io-retries 3
  --attr-cache 10 --entry-cache 0 --dir-entry-cache 0 --negative-entry-cache 5
  --cache-dir /var/jfscache-app --cache-size 20480 --demand-first
)

build() {
  mkdir -p "$STATE/bin"
  chmod 755 "$STATE" "$STATE/bin"
  (cd "$ROOT" && CGO_ENABLED=0 go build -trimpath -tags nosqlite,nomysql,nopg,notikv,nobadger,noetcd \
    -o "$STATE/bin/jm5" ./cmd/jm5 &&
    CGO_ENABLED=0 go build -trimpath -o "$STATE/bin/drainprobe" ./test/remote-lab/drainprobe &&
    CGO_ENABLED=0 go build -trimpath -o "$STATE/bin/enginelife" ./test/remote-lab/enginelife)
  docker cp "$("$LAB" _cid client):/usr/local/bin/juicefs" "$STATE/bin/juicefs"
  grep -q jm-door-listen-fd <<<"$(in_client "$BIN/juicefs" mount --help 2>/dev/null)" ||
    die "the client's juicefs has no engine socket in mount (rebuild the lab image)"
  log "built from $(git -C "$ROOT" rev-parse --short HEAD); $(in_client "$BIN/juicefs" --version)"
}

fixture() {
  "$LAB" shape lan >/dev/null
  # shellcheck disable=SC2016
  in_client sh -c 'mkdir -p /jfs/er/clips /jfs/er/set /jfs/er/edit /jfs/er/big
    for i in 01 02 03 04 05 06; do [ -f /jfs/er/clips/c$i.mov ] || head -c $((32 << 20)) /dev/urandom > /jfs/er/clips/c$i.mov; done
    for i in 1 2; do [ -f /jfs/er/big/s$i.mov ] || head -c $((256 << 20)) /dev/urandom > /jfs/er/big/s$i.mov; done
    [ -f /jfs/er/big/pair.mov ] || head -c $((160 << 20)) /dev/urandom > /jfs/er/big/pair.mov
    for i in $(seq 1 24); do n=$(printf %02d "$i"); [ -f /jfs/er/set/f$n.bin ] || head -c $(( (i * 997 % 9000 + 50) << 10 )) /dev/urandom > /jfs/er/set/f$n.bin; done
    for i in 1 2 3; do head -c $((4 << 20)) /dev/urandom > /jfs/er/edit/e$i.bin; done
    for i in 1 2 3 4 5 6 7 8; do [ -f /jfs/er/clips/long$i.mov ] || head -c $((96 << 20)) /dev/urandom > /jfs/er/clips/long$i.mov; done
    sync'
  "$HERE/experiments/peer-edit-lab.sh" principal
  log "fixture in /jfs/er"
}

stop_jm5() {
  # shellcheck disable=SC2016
  in_client sh -c 'p=$(cat /var/jm/jm5.pid 2>/dev/null) || exit 0
    kill "$p" 2>/dev/null || exit 0
    for i in $(seq 250); do kill -0 "$p" 2>/dev/null || exit 0; sleep 0.2; done
    kill -9 "$p" 2>/dev/null; sleep 1' || true
  # shellcheck disable=SC2016
  in_client sh -c 'if grep -q " '"$MNT"' fuse" /proc/mounts; then umount '"$MNT"' 2>/dev/null || umount -l '"$MNT"'; fi' || true
}

# write_argv FILE META_USER PASSFILE MOUNTPOINT FLAG...: a foreground mount
# with the password from a file (never argv).
write_argv() {
  local file="$1" user="$2" pass="$3" mp="$4"; shift 4
  python3 -c 'import json,sys; print(json.dumps(sys.argv[1:]))' sh -c \
    "META_PASSWORD=\"\$(cat $pass)\" exec juicefs mount \"redis://$user@$PROXY_IP:6379/1\" $mp $* \"\$@\"" sh \
    | in_client sh -c "mkdir -p \"\$(dirname $file)\" && cat > $file"
}

start_jm5() { # start_jm5 ARM PROFILE
  local arm="$1" profile="$2" level="" class=slow uuid
  [ "$arm" = reads ] && level=reads
  [ "$profile" != lan ] || class=fast
  [ "$profile" != fast-remote ] || class=medium
  # The principal's password survives the wipe of jm5's state.
  in_client sh -c "mkdir -p /var/jm-engine && { [ ! -s /var/jm/mac.pass ] || cp /var/jm/mac.pass /var/jm-engine/mac.pass; } &&
    [ -s /var/jm-engine/mac.pass ] && chmod 600 /var/jm-engine/mac.pass &&
    rm -rf /var/jm /var/jfscache-app && mkdir -p /var/jm/db /var/jfscache-app $MNT && cp /var/jm-engine/mac.pass /var/jm/mac.pass" ||
    die "no principal password: run engine-reads-lab.sh fixture (peer-edit-lab.sh principal)"
  write_argv /var/jm-engine/app.json "$MAC_NAME" /var/jm/mac.pass "$MNT" "${APP_FLAGS[@]}"
  uuid="$(in_client sh -c 'juicefs status "redis://admin@'"$PROXY_IP"':6379/1" 2>/dev/null' | sed -n 's/.*"UUID": "\([^"]*\)".*/\1/p' | head -n 1)"
  [ -n "$uuid" ] || die "no volume UUID"
  # shellcheck disable=SC2016
  in_client sh -c 'cd /var/jm && HOME=/var/jm JM_ENGINE='"$level"' JM_ENGINE_MOUNT_ARGV=/var/jm-engine/app.json \
    JM_CACHE_DIR=/var/jfscache-app/'"$uuid"' JM_NET_FORCE_CLASS='"$class"' JM_SPOOL_ENABLE=1 JM_SPOOL_DIR=/var/jm/spool JM_SPOOL_SIZE_GB=4 '"${ER_JM5_ENV:-}"' \
    nohup '"$BIN"'/jm5 --redis "redis://'"$MAC_NAME"':$(cat /var/jm/mac.pass)@'"$PROXY_IP"':6379/1" \
      --fuse-path '"$MNT"' --no-fuse --external-juicefs-is-bundled --no-mount --listen 127.0.0.1:21049 \
      --metrics-addr 127.0.0.1:21050 --log-level debug --db /var/jm/db/metadata.db >/var/jm/jm5.log 2>&1 &
    echo $! > /var/jm/jm5.pid'
  for _ in $(seq 150); do
    in_client sh -c 'nc -z 127.0.0.1 21049' 2>/dev/null && break
    sleep 1
  done
  in_client sh -c 'nc -z 127.0.0.1 21049' || { in_client tail -n 40 /var/jm/jm5.log >&2; die "jm5 did not start"; }
  if [ "$arm" = reads ]; then
    for _ in $(seq 60); do in_client grep -q 'engine socket ready' /var/jm/jm5.log && break; sleep 1; done
    in_client grep -q 'engine socket ready' /var/jm/jm5.log || die "arm reads: no engine socket"
  fi
  sleep "${ER_SETTLE_S:-8}"
  XPATH="$(in_client sed -n 's/.*"export":"\(\/jm-[0-9a-f]*\)".*/\1/p' /var/jm/jm5.log | head -n 1)"
  [ -n "$XPATH" ] || die "jm5 logged no export path"
}

probe() { in_client "$BIN/drainprobe" "$@" -export "$XPATH" -nfs 127.0.0.1:21049; }

expected_sums() { # the teammate's view of the fixture
  # shellcheck disable=SC2016
  in_client sh -c 'cd /jfs && sha256sum er/set/*.bin er/clips/c0*.mov er/edit/*.bin' > "$OUT/$1.expected.sha"
}

run() {
  local profile="$1" arm="$2" label="$3" steps
  mkdir -p "$OUT"
  [ -x "$STATE/bin/jm5" ] || die "run build first"
  steps="${ER_STEPS:-firstbyte pair seq c1 warm long offline}"
  stop_jm5
  "$LAB" shape lan >/dev/null
  start_jm5 "$arm" "$profile"
  "$LAB" shape "$profile" >/dev/null
  log "$label: $arm on $profile: $steps"
  for s in $steps; do
    case "$s" in
      firstbyte)
        probe firstbyte -files er/clips/c01.mov,er/clips/c02.mov,er/clips/c03.mov,er/clips/c04.mov,er/clips/c05.mov,er/clips/c06.mov \
          >> "$OUT/$label.jsonl" ;;
      pair)
        [ "$profile" = lan ] || continue
        probe pair -file er/big/pair.mov -reps 12 >> "$OUT/$label.jsonl" ;;
      seq)
        probe seqread -files er/big/s1.mov,er/big/s2.mov -inflight 2 -budget "${ER_SEQ_S:-25}s" >> "$OUT/$label.jsonl" ;;
      warm)
        [ "$profile" = lan ] || continue
        probe seqread -files er/clips/c01.mov -inflight 2 >> "$OUT/$label.jsonl"
        probe seqread -files er/clips/c01.mov,er/clips/c01.mov,er/clips/c01.mov -inflight 2 >> "$OUT/$label.jsonl" ;;
      c1)
        c1 "$profile" "$label" ;;
      long)
        [ "$profile" = abroad ] || continue
        long_save "$label" ;;
      offline)
        offline "$profile" "$label" ;;
      metrics)
        in_client sh -c 'wget -q -O - http://127.0.0.1:21050/metrics' | grep -iE 'ssd|cold_subread|warm_subread|bytes_read|read_fail' \
          > "$OUT/$label.metrics.txt" || true ;;
    esac
  done
  stop_jm5 # first: jm5 logs its final engine counters on exit
  in_client cat /var/jm/jm5.log > "$OUT/$label.jm5.log" 2>/dev/null || true
  in_client cat /var/jm/juicefs-mount.log > "$OUT/$label.juicefs.log" 2>/dev/null || true
  "$LAB" shape lan >/dev/null
  log "$label: results in $OUT/$label.jsonl"
}

# C1: reads with checksums while the teammate edits and the Mac saves.
c1() {
  local profile="$1" label="$2" files
  "$LAB" shape lan >/dev/null
  in_client sh -c 'for i in 1 2 3; do head -c $((4 << 20)) /dev/urandom > /jfs/er/edit/e$i.bin; done; sync'
  expected_sums "$label.c1-before"
  "$LAB" shape "$profile" >/dev/null
  # The teammate rewrites the edit files every 3 s for 45 s.
  # shellcheck disable=SC2016
  in_client sh -c 'nohup sh -c "for r in \$(seq 15); do for i in 1 2 3; do head -c \$((4 << 20)) /dev/urandom > /jfs/er/edit/e\$i.bin.t && mv /jfs/er/edit/e\$i.bin.t /jfs/er/edit/e\$i.bin; done; sleep 3; done" >/var/jm/teammate.log 2>&1 & echo $! > /var/jm/teammate.pid'
  # The Mac saves meanwhile.
  in_client sh -c "nohup $BIN/drainprobe write -export $XPATH -nfs 127.0.0.1:21049 -dir er/saves-$label -small 40 -big-mb 0 >/var/jm/saves.jsonl 2>&1 &" || true
  files="$(in_client sh -c 'cd /jfs && ls er/set/*.bin er/clips/c0*.mov' | paste -sd, -)"
  probe seqread -files "$files" -inflight 2 -gap 0s | sed 's/"step":"seqread"/"step":"c1-read"/' >> "$OUT/$label.jsonl"
  # shellcheck disable=SC2016
  in_client sh -c 'p=$(cat /var/jm/teammate.pid); while kill -0 $p 2>/dev/null; do sleep 1; done'
  "$LAB" shape lan >/dev/null
  expected_sums "$label.c1-after"
  sleep "${ER_RECONCILE_S:-40}" # a reconcile interval: the mirror learns the last edit
  "$LAB" shape "$profile" >/dev/null
  probe seqread -files er/edit/e1.bin,er/edit/e2.bin,er/edit/e3.bin -inflight 2 -gap 0s |
    sed 's/"step":"seqread"/"step":"c1-edit"/' >> "$OUT/$label.jsonl"
  in_client cat /var/jm/saves.jsonl > "$OUT/$label.saves.jsonl" 2>/dev/null || true
}

# F: a clip played while the teammate saves it in pieces (a long save).
long_save() {
  local label="$1" n
  n="$(( $(grep -c '"step":"long"' "$OUT/$label.jsonl" 2>/dev/null || true) + 1 ))"
  # shellcheck disable=SC2016
  in_client sh -c 'nohup sh -c "for p in \$(seq 0 11); do dd if=/dev/urandom of=/jfs/er/clips/long'"$n"'.mov bs=1M count=8 seek=\$((p * 8)) conv=notrunc 2>/dev/null; sync; sleep 4; done" >/dev/null 2>&1 & echo $! > /var/jm/long.pid'
  in_client sh -c "nohup $BIN/drainprobe write -export $XPATH -nfs 127.0.0.1:21049 -dir er/longsaves-$label-$n -small 20 -big-mb 0 >/dev/null 2>&1 &" || true
  probe seqread -files "er/clips/long$n.mov" -inflight 2 -budget 60s | sed 's/"step":"seqread"/"step":"long"/' >> "$OUT/$label.jsonl"
  # shellcheck disable=SC2016
  in_client sh -c 'p=$(cat /var/jm/long.pid); while kill -0 $p 2>/dev/null; do sleep 1; done'
}

# Offline: warm a file, cut the link, wait for auto offline, read it again.
offline() {
  local profile="$1" label="$2"
  probe seqread -files er/set/f01.bin,er/set/f02.bin -inflight 2 | sed 's/"step":"seqread"/"step":"offline-warm"/' >> "$OUT/$label.jsonl"
  "$LAB" shape down >/dev/null
  for _ in $(seq 60); do in_client grep -q 'cache backend unqualified\|auto-offline\|offline' /var/jm/jm5.log && break; sleep 1; done
  sleep 10
  probe seqread -files er/set/f01.bin,er/set/f02.bin -inflight 2 | sed 's/"step":"seqread"/"step":"offline-read"/' >> "$OUT/$label.jsonl" || true
  "$LAB" shape "$profile" >/dev/null
}

door() {
  local label="$1"
  mkdir -p "$OUT"
  "$LAB" shape lan >/dev/null
  in_client sh -c 'rm -rf /var/jfscache-door; mkdir -p /var/jfscache-door /jfs-door'
  in_client "$BIN/enginelife" door -bin "$BIN/juicefs" -meta "redis://$MAC_NAME@$PROXY_IP:6379/1" \
    -pass-file /var/jm/mac.pass -door-file er/clips/c06.mov -door-mount /jfs-door -label "$label" \
    -helper-log /var/jm-engine/door-mount.log -volume "" >> "$OUT/$label.jsonl"
  cat "$OUT/$label.jsonl"
}

cmd="${1:-}"; shift || true
case "$cmd" in
  build) build ;;
  fixture) fixture ;;
  run) [ $# -eq 3 ] || die "usage: run PROFILE off|reads LABEL"; run "$@" ;;
  door) door "${1:-door}" ;;
  argv)
    # lab.sh's /jfs mount (admin, its flags) for the other labs' reads arm.
    write_argv "${1:?file}" admin /var/jm-engine/admin.pass /jfs \
      --no-usage-report --no-bgjob --max-deletes 0 --heartbeat 60s --max-uploads 4 --max-readahead 1M \
      --buffer-size 512 --prefetch 1 --get-timeout 2m --put-timeout 3m --io-retries 3 \
      --attr-cache 10 --entry-cache 60 --dir-entry-cache 60 --negative-entry-cache 5 \
      --cache-dir /var/jfscache --cache-size 20480 --demand-first
    # shellcheck disable=SC2016
    in_client sh -c 'umask 077; printf %s "$META_PASSWORD" > /var/jm-engine/admin.pass'
    ;;
  *) die "usage: build|fixture|run|door|argv" ;;
esac
