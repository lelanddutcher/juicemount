#!/usr/bin/env bash
# Link-switch lab: the link class changes under a running mount (bundled
# juicefs patch 0027, the app's link follower in nfs/jfs_link_policy.go).
#
#   link-switch-lab.sh fixture        write the clips (on lan)
#   link-switch-lab.sh down [ROUND]   mount on lan, link turns abroad
#   link-switch-lab.sh up [ROUND]     mount abroad, link turns lan
#
# Each direction measures, on a cold clip each time:
#   before   the link changed, the mount still runs its mount-time policy
#            (what every build before patch 0027 does until a relaunch);
#   after    the policy the app's follower sends for the new class;
#   control  a fresh mount on the new link (what a relaunch gives).
# A step prints JSON lines: ttfb (open + first 128 KiB), scrub (20 random
# 1 MiB reads), seq (sequential read, up only), metrics (object GETs so far).
#
# Set the lab up with server BBR, as the laptop runs used:
#   JMLAB_BBR=1 ./lab.sh up && experiments/link-switch-lab.sh fixture
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
LAB="$HERE/lab.sh"
DIR=/jfs/linkswitch
probe() { "$LAB" exec python3 /lab/lib/pinprobe.py "$@"; }
link() { "$LAB" exec python3 /lab/lib/linkprobe.py "$@"; }
mark() { printf '{"step":"%s","at":"%s"}\n' "$1" "$(date -u +%H:%M:%S)"; }

# The app's mount flags on each link (health/fuse.go). lab.sh's default
# flags are the slow-link mount; a lan mount has a 4 GiB buffer, upstream
# object deadlines and no --demand-first.
mount_lan() {
  JMLAB_DEMAND_FIRST=0 JMLAB_MOUNT_OPTS="--buffer-size 4096 --get-timeout 60s --put-timeout 60s --io-retries 10" \
    "$LAB" mount --cold >&2
}
mount_abroad() { JMLAB_DEMAND_FIRST=1 JMLAB_MOUNT_OPTS="" "$LAB" mount --cold >&2; }

measure() { # measure STEP CLIP [seq]
  mark "$1"
  link policy
  probe metrics
  link ttfb "$DIR/$2.mov"
  probe scrub "$DIR/$2.mov" 20
  if [ "${3:-}" = seq ]; then probe seq "$DIR/s$2.mov"; fi
  probe metrics
}

round="${2:-1}"
case "${1:-}" in
  fixture)
    "$LAB" shape lan >&2
    # Per round: three clips for down, three (and their seq twins) for up.
    for r in 1 2; do
      for c in d1 d2 d3 u1 u2 u3; do
        "$LAB" exec sh -c "mkdir -p $DIR && head -c 134217728 /dev/urandom > $DIR/r$r$c.mov"
      done
      for c in u1 u2 u3; do
        "$LAB" exec sh -c "head -c 134217728 /dev/urandom > $DIR/sr$r$c.mov"
      done
    done
    mark fixture-written
    ;;
  down)
    "$LAB" shape lan >&2; mount_lan
    mark mounted-lan; link policy
    "$LAB" shape abroad >&2
    measure before "r${round}d1"
    mark switch; link policy abroad
    measure after "r${round}d2"
    mount_abroad
    measure control "r${round}d3"
    ;;
  up)
    "$LAB" shape abroad >&2; mount_abroad
    mark mounted-abroad; link policy
    "$LAB" shape lan >&2
    measure before "r${round}u1" seq
    mark switch; link policy lan
    measure after "r${round}u2" seq
    mount_lan
    measure control "r${round}u3" seq
    ;;
  *) sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
