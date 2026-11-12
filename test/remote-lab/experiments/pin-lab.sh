#!/usr/bin/env bash
# Pin-before-travel lab session (bundled juicefs patch 0026).
#
#   pin-lab.sh before|after|cap|offline    one session; JSON lines on stdout
#
# The lab must be up with a 3 GiB block cache (JMLAB_MOUNT_OPTS="--cache-size
# 3072") and the fixture written: /jfs/pack (4 x 512 MiB clips + 40 x 16 KiB
# project files, 2.15 GB) and /jfs/media/fill-1.mov, fill-2.mov (1.5 GiB each).
# lib/pinprobe.py must be at /usr/local/bin/pinprobe.py in the client.
#
#   before  the old pin: four workers read the project in 128 KiB direct reads
#           (what macFUSE sends), then a 1.5 GiB unpinned read, then abroad.
#   after   the new pin (the JMPinCache message, as the app's workers send it),
#           the same pressure and abroad reads, then a relaunch: the app's
#           guard pins again; a second 1.5 GiB unpinned read after it.
#   cap     with the project pinned, pinning 3 GiB more must fail with ENOSPC.
#   offline the travel case: pin, cut the link, kill juicefs (an app restart),
#           fill the cache disk below its free-space floor, start juicefs (it
#           needs the server only to mount), cut the link again, and check
#           the pins with no server while more disk pressure evicts. The cache
#           is a 4 GiB tmpfs so the disk can be filled.
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
LAB="$HERE/lab.sh"
P=(/jfs/pack/clip1.mov /jfs/pack/clip2.mov /jfs/pack/clip3.mov /jfs/pack/clip4.mov)
probe() { "$LAB" exec python3 /usr/local/bin/pinprobe.py "$@"; }
pack() { "$LAB" exec sh -c 'ls /jfs/pack/*.mov /jfs/pack/proj/*' | tr '\n' ' '; }
mark() { printf '{"step":"%s","at":"%s"}\n' "$1" "$(date -u +%H:%M:%S)"; }
abroad_reads() {
  "$LAB" shape abroad >&2
  mark abroad
  probe metrics
  for f in "${P[@]}"; do probe scrub "$f" 20; done
  probe seq /jfs/pack/clip3.mov
  probe metrics
}

case "${1:-}" in
  before)
    "$LAB" shape lan >&2; "$LAB" mount --cold >&2
    mark readthrough; probe metrics
    # shellcheck disable=SC2046
    probe readthrough $(pack)
    probe metrics
    # shellcheck disable=SC2046
    probe presence $(pack)
    mark filler; probe seq /jfs/media/fill-1.mov
    # shellcheck disable=SC2046
    probe presence $(pack)
    abroad_reads
    ;;
  after)
    "$LAB" shape lan >&2; "$LAB" mount --cold >&2
    mark pin; probe metrics
    # shellcheck disable=SC2046
    probe pin $(pack)
    probe metrics
    # shellcheck disable=SC2046
    probe check $(pack)
    mark hashread; probe seq /jfs/pack/clip1.mov; probe metrics
    mark filler; probe seq /jfs/media/fill-1.mov
    # shellcheck disable=SC2046
    probe check $(pack)
    # shellcheck disable=SC2046
    probe presence $(pack)
    probe metrics
    abroad_reads
    mark relaunch; "$LAB" mount >&2
    # shellcheck disable=SC2046
    probe check $(pack)
    probe metrics
    # shellcheck disable=SC2046
    probe pin $(pack)
    # shellcheck disable=SC2046
    probe sweep $(pack)
    probe metrics
    for f in /jfs/pack/clip1.mov /jfs/pack/clip4.mov; do probe scrub "$f" 20; done
    probe metrics
    mark filler2; "$LAB" shape lan >&2; probe seq /jfs/media/fill-2.mov
    # shellcheck disable=SC2046
    probe check $(pack)
    probe metrics
    ;;
  cap)
    "$LAB" shape lan >&2
    # shellcheck disable=SC2046
    probe pin $(pack)
    mark cap; probe pin /jfs/media/fill-1.mov /jfs/media/fill-2.mov
    # shellcheck disable=SC2046
    probe sweep $(pack)
    ;;
  offline)
    NAS="${JMLAB_NAS_SUBNET:-10.231.10.0/24}"
    cut() { "$LAB" exec iptables -I OUTPUT -d "$NAS" -j DROP; mark "link-cut"; }
    heal() { "$LAB" exec sh -c "while iptables -D OUTPUT -d $NAS -j DROP 2>/dev/null; do :; done"; mark "link-restored"; }
    junk() { "$LAB" exec sh -c "head -c $1 /dev/zero >> /var/jfscache/junk" || true; mark "junk+$1"; }
    "$LAB" shape lan >&2; "$LAB" umount >&2
    "$LAB" exec sh -c 'umount /var/jfscache 2>/dev/null; rm -rf /var/jfscache; mkdir -p /var/jfscache && mount -t tmpfs -o size=4g tmpfs /var/jfscache'
    "$LAB" mount >&2
    mark pin
    # shellcheck disable=SC2046
    probe pin $(pack)
    probe seq /jfs/media/fill-1.mov
    # shellcheck disable=SC2046
    probe check $(pack)
    probe blocks
    # Inode numbers now: with no server, nothing under /jfs can be listed.
    # shellcheck disable=SC2046
    INOS="$(probe inodes $(pack))"
    CLIP1="$(probe inodes /jfs/pack/clip1.mov)"
    cut
    mark kill; "$LAB" exec sh -c 'umount -l /jfs; pkill -9 -x juicefs; sleep 1'
    probe blocks
    mark offline-mount
    if "$LAB" exec timeout 30 juicefs --version >/dev/null && ! timeout 60 "$LAB" mount >&2; then
      printf '{"step":"offline-mount","mounted":false}\n'
    else
      printf '{"step":"offline-mount","mounted":true}\n'
    fi
    "$LAB" umount >&2 || true
    # The disk goes below the floor while juicefs is down.
    junk 700000000
    probe blocks
    heal; "$LAB" mount >&2; cut
    sleep 5
    mark check-offline
    # shellcheck disable=SC2086
    probe check $INOS
    probe blocks
    junk 300000000
    sleep 5
    # shellcheck disable=SC2086
    probe check $INOS
    probe blocks
    # A re-pin needs the server: it waits, and pinned blocks stay meanwhile.
    probe start "$CLIP1"
    heal
    # Below the floor, a new pin is refused at once, with nothing downloaded.
    probe metrics
    probe pin /jfs/media/fill-2.mov
    probe metrics
    "$LAB" umount >&2; "$LAB" exec sh -c 'umount /var/jfscache; mkdir -p /var/jfscache'
    ;;
  *) printf 'usage: %s before|after|cap|offline\n' "$0" >&2; exit 2 ;;
esac
