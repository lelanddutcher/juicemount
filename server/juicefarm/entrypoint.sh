#!/bin/sh
# juicefarm entrypoint: mount the JuiceFS volume read-only-ish (we only WRITE
# under .juicemount/derivatives), then run the generators over a target path.
#
# Env:
#   JM_META           redis URL for the volume metadata   (required)
#   JM_FARM_TARGET     path UNDER the volume to process    (default: whole volume)
#   JM_FARM_DB         server-side derivatives index path  (default: /state/derivatives.db)
#   JM_FARM_PRODUCER   producer tag stamped on rows        (default: linux-farm)
#   JM_FARM_MODEL      ggml whisper model                  (baked: /models/ggml-base.en.bin)
#   JM_FARM_MODE       "transcript" | "derivatives" | "all"(default: all)
#   JM_FARM_WORKERS    concurrency                          (default: 4)
#   JM_FARM_ONCE       "1" → process once and exit; else loop every JM_FARM_INTERVAL
#   JM_FARM_INTERVAL   seconds between sweeps               (default: 900)
set -eu

: "${JM_META:?JM_META (redis://redis:6379/1) is required}"
MNT=/jfs
TARGET="${JM_FARM_TARGET:-$MNT}"
DB="${JM_FARM_DB:-/state/derivatives.db}"
PRODUCER="${JM_FARM_PRODUCER:-linux-farm}"
MODEL="${JM_FARM_MODEL:-/models/ggml-base.en.bin}"
MODE="${JM_FARM_MODE:-all}"
WORKERS="${JM_FARM_WORKERS:-4}"
INTERVAL="${JM_FARM_INTERVAL:-900}"
mkdir -p "$(dirname "$DB")"

# Own FUSE mount of the same volume — JuiceFS supports many concurrent mounts,
# so this does NOT disturb the Mac client or the server's juicefs container.
echo "[juicefarm] mounting $JM_META → $MNT"
juicefs mount --cache-dir /jfs-cache --cache-size 20000 --backup-meta 0 "$JM_META" "$MNT" &
i=0; until mountpoint -q "$MNT"; do i=$((i+1)); [ "$i" -gt 60 ] && { echo "[juicefarm] mount timeout" >&2; exit 1; }; sleep 1; done
echo "[juicefarm] mounted; target=$TARGET mode=$MODE producer=$PRODUCER"

run_sweep() {
  case "$MODE" in
    transcript) FLAGS="-transcript -whisper-model $MODEL" ;;
    derivatives) FLAGS="-blobs -filmstrip -waveform" ;;
    all|*) FLAGS="-blobs -filmstrip -waveform" ;;  # basic derivatives pass...
  esac
  echo "[juicefarm] sweep: jmfarm $FLAGS -root $TARGET"
  # shellcheck disable=SC2086
  jmfarm -mount "$MNT" -db "$DB" -producer "$PRODUCER" -concurrency "$WORKERS" $FLAGS -root "$TARGET" || true
  if [ "$MODE" = "all" ]; then
    echo "[juicefarm] sweep: jmfarm -transcript (AI pass)"
    jmfarm -mount "$MNT" -db "$DB" -producer "$PRODUCER" -concurrency "$WORKERS" \
      -transcript -whisper-model "$MODEL" -root "$TARGET" || true
  fi
}

if [ "${JM_FARM_ONCE:-0}" = "1" ]; then
  run_sweep
  echo "[juicefarm] once-mode done; unmounting"
  juicefs umount "$MNT" || true
  exit 0
fi

while true; do
  run_sweep
  echo "[juicefarm] sleeping ${INTERVAL}s"
  sleep "$INTERVAL"
done
