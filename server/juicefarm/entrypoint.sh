#!/bin/sh
# juicefarm entrypoint: mount the JuiceFS volume read-only-ish (we only WRITE
# under .juicemount/derivatives), then run the generators over a target path.
#
# Env:
#   JM_META           redis URL for the volume metadata   (required)
#   JM_FARM_TARGET     path UNDER the volume to process    (default: whole volume)
#   JM_FARM_DB         server-side derivatives index path  (default: /state/derivatives.db)
#   JM_FARM_PRODUCER   producer tag stamped on rows        (default: linux-farm)
#   JM_FARM_MODEL      ggml whisper model: a PATH (baked default) or a bare model
#                      NAME (e.g. "large-v3") fetched into /state/models on first
#                      use — so the manager can switch models without a rebuild.
#                      (baked default: /models/ggml-medium.en.bin)
#   JM_FARM_VCODEC     proxy video encoder (default libx264; GPU: h264_nvenc /
#                      h264_qsv / h264_vaapi when the host has an APU/GPU)
#   JM_FARM_MODE       "transcript" | "derivatives" | "proxy" | "all" (default: all)
#                      OR "queue" → standing queue-drain worker (see JM_FARM_QUEUE)
#   JM_FARM_WORKERS    concurrency                          (default: 4)
#   JM_FARM_ONCE       "1" → process once and exit; else loop every JM_FARM_INTERVAL
#   JM_FARM_INTERVAL   seconds between sweeps               (default: 900)
#   JM_FARM_QUEUE      "1" (or JM_FARM_MODE=queue) → run as a STANDING worker that
#                      drains the shared juicefarm: queue on $JM_META instead of
#                      doing one-shot/interval -root sweeps. The container stays up;
#                      it heartbeats + BRPOPs jobs forever (exit on SIGTERM). All the
#                      one-shot env above still supplies the per-job run defaults a
#                      job's options override (CRF/preset/model/vcodec/workers).
set -eu

: "${JM_META:?JM_META (redis://redis:6379/1) is required}"
MNT=/jfs
TARGET="${JM_FARM_TARGET:-$MNT}"
DB="${JM_FARM_DB:-/state/derivatives.db}"
PRODUCER="${JM_FARM_PRODUCER:-linux-farm}"
MODE="${JM_FARM_MODE:-all}"
WORKERS="${JM_FARM_WORKERS:-4}"
INTERVAL="${JM_FARM_INTERVAL:-900}"
VCODEC="${JM_FARM_VCODEC:-libx264}"
CRF="${JM_FARM_CRF:-21}"
PRESET="${JM_FARM_PRESET:-slow}"
# Proxy transcode pins a core per clip — default it to a LOWER worker count than
# the cheap passes so a full sweep can't saturate the NAS (manager governor sets it).
PROXY_WORKERS="${JM_FARM_PROXY_WORKERS:-2}"
STATUS="${JM_FARM_STATUS:-/state/farm-status.json}"
# Yield CPU + IO to interactive load so a sweep never starves the live mount.
# JM_FARM_NICE: niceness 0-19 (higher = nicer). JM_FARM_IONICE: best-effort IO
# class 3=idle; set empty to disable. Both are guarded by command -v.
NICE="${JM_FARM_NICE:-10}"
IONICE="${JM_FARM_IONICE:-3}"
mkdir -p "$(dirname "$DB")"

# resolve_model: accept a path (use as-is) OR a bare model name to fetch into
# /state/models (persistent), so the manager can pick large-v3 etc. without a
# rebuild. Falls back to the baked default on a fetch failure.
DEFAULT_MODEL=/models/ggml-medium.en.bin
resolve_model() {
  m="${JM_FARM_MODEL:-$DEFAULT_MODEL}"
  if [ -f "$m" ]; then echo "$m"; return; fi
  name=$(printf '%s' "$m" | sed -e 's#.*/##' -e 's#^ggml-##' -e 's#\.bin$##')
  dest="/state/models/ggml-${name}.bin"
  if [ -f "$dest" ]; then echo "$dest"; return; fi
  echo "[juicefarm] fetching whisper model '$name' → $dest" >&2
  mkdir -p /state/models
  if bash /usr/local/bin/download-ggml-model.sh "$name" /state/models >&2 2>&1; then
    echo "$dest"
  else
    echo "[juicefarm] model fetch failed; using baked $DEFAULT_MODEL" >&2
    echo "$DEFAULT_MODEL"
  fi
}
MODEL=$(resolve_model)

# Own FUSE mount of the same volume — JuiceFS supports many concurrent mounts,
# so this does NOT disturb the Mac client or the server's juicefs container.
echo "[juicefarm] mounting $JM_META → $MNT"
juicefs mount --cache-dir /jfs-cache --cache-size 20000 --backup-meta 0 "$JM_META" "$MNT" &
i=0; until mountpoint -q "$MNT"; do i=$((i+1)); [ "$i" -gt 60 ] && { echo "[juicefarm] mount timeout" >&2; exit 1; }; sleep 1; done
echo "[juicefarm] mounted; target=$TARGET mode=$MODE producer=$PRODUCER"

# do_pass runs jmfarm once with an explicit mode's flags. proxy + transcript are
# their OWN modes (each ignores the basic-derivative flags), so "all" runs three
# separate passes: fast derivatives first (publish immediately), then the slow
# proxy transcode, then the AI transcript.
do_pass() {
  echo "[juicefarm] sweep: jmfarm $* -root $TARGET (nice=$NICE ionice=${IONICE:-off})"
  wrap=""
  command -v nice >/dev/null 2>&1 && wrap="nice -n $NICE"
  if [ -n "$IONICE" ] && command -v ionice >/dev/null 2>&1; then wrap="ionice -c $IONICE $wrap"; fi
  # Pass the governor knobs informationally so jmfarm stamps them in the status
  # file (the manager's read-only inspector). IONICE may be empty (disabled) →
  # record 0. These are recorded for display; the wrap above actually applies them.
  ion="${IONICE:-0}"
  # shellcheck disable=SC2086
  $wrap jmfarm -mount "$MNT" -db "$DB" -producer "$PRODUCER" -concurrency "$WORKERS" -status "$STATUS" \
    -nice "$NICE" -ionice "$ion" -interval "$INTERVAL" "$@" -root "$TARGET" || true
}

run_sweep() {
  case "$MODE" in
    transcript)  do_pass -transcript -whisper-model "$MODEL" ;;
    proxy)       do_pass -proxy -vcodec "$VCODEC" -crf "$CRF" -preset "$PRESET" -proxy-concurrency "$PROXY_WORKERS" ;;
    derivatives) do_pass -blobs -filmstrip -waveform ;;
    all|*)
      do_pass -blobs -filmstrip -waveform
      do_pass -proxy -vcodec "$VCODEC" -crf "$CRF" -preset "$PRESET" -proxy-concurrency "$PROXY_WORKERS"
      do_pass -transcript -whisper-model "$MODEL" ;;
  esac
}

# Standing queue-drain worker: JM_FARM_QUEUE=1 (or JM_FARM_MODE=queue). Instead of
# one-shot/interval -root sweeps, exec the worker — it heartbeats + drains the
# shared juicefarm: queue on $JM_META and runs each job's passes scoped to the
# job's path (reusing the same generators do_pass uses). Wrapped in the SAME
# nice/ionice as do_pass so a queued sweep never starves the live mount. This is a
# standing container: exec replaces the shell so SIGTERM reaches jmfarm directly.
if [ "${JM_FARM_QUEUE:-0}" = "1" ] || [ "$MODE" = "queue" ]; then
  ion="${IONICE:-0}"
  wrap=""
  command -v nice >/dev/null 2>&1 && wrap="nice -n $NICE"
  if [ -n "$IONICE" ] && command -v ionice >/dev/null 2>&1; then wrap="ionice -c $IONICE $wrap"; fi
  echo "[juicefarm] queue mode: draining $JM_META (nice=$NICE ionice=${IONICE:-off})"
  # shellcheck disable=SC2086
  exec $wrap jmfarm -queue -meta "$JM_META" -mount "$MNT" -db "$DB" -producer "$PRODUCER" \
    -status "$STATUS" -nice "$NICE" -ionice "$ion" \
    -concurrency "$WORKERS" -proxy-concurrency "$PROXY_WORKERS" \
    -vcodec "$VCODEC" -crf "$CRF" -preset "$PRESET" -whisper-model "$MODEL"
fi

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
