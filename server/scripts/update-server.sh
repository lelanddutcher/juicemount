#!/usr/bin/env bash
#
# update-server.sh — one-command JuiceMount server-side update push.
#
# From the Mac, in one shot:
#   1. rsync the repo worktree to the TrueNAS build dir
#      (CONTENTS sync: "<repo>/ → <host>:<dest>/" — note the trailing slash on
#      the SOURCE; without it rsync nests the repo a directory deeper and the
#      remote docker build context breaks)
#   2. docker-build the farm image  (server/juicefarm/Dockerfile   → juicefarm:local)
#      and the manager image        (server/juicemount-manager/Dockerfile
#                                                    → juicemount-manager:local)
#      ON the host (native x86_64 — no cross-compile, no registry)
#   3. recreate ONLY the farm worker + manager containers, re-running each with
#      the run-args it ALREADY has (introspected live via `docker inspect` —
#      env/mounts/network/ports/restart/caps/devices are reconstructed, never
#      hardcoded), pointed at the freshly-built :local images
#   4. verify: farm worker container is up + manager answers /api/farm with 200
#
# ############################################################################
# ##  NEVER TOUCHES redis / minio / juicefs CONTAINERS.                     ##
# ##  Those hold the volume's metadata, objects, and the gateway mount —    ##
# ##  restarting ANY of them breaks the live Mac client mid-session.        ##
# ##  A hard guard below refuses to run if a target container name even     ##
# ##  looks like one of them.                                               ##
# ############################################################################
#
# Idempotent: rsync -a --delete converges the tree, docker build is cached,
# and recreation always starts from the container's current (or just-captured)
# config. Re-running after success is a no-op deploy of the same bits.
#
# Dry-run: --dry-run prints the full plan — the exact rsync command and the
# complete remote script — and executes NOTHING (no ssh, no rsync, no docker).
#
# Usage:
#   server/scripts/update-server.sh [--repo <path>] [--host root@192.168.0.197]
#       [--dest /root/juicefarm-build] [--farm-container juicefarm-worker]
#       [--manager-container juicemount-manager] [--dry-run]
#
# Self-test (PATH-shimmed, no network): server/scripts/update-server.test.sh

set -euo pipefail

# ---- defaults --------------------------------------------------------------
HOST="root@192.168.0.197"
DEST="/root/juicefarm-build"
FARM_CONTAINER="juicefarm-worker"
MANAGER_CONTAINER="juicemount-manager"
SKIP_MANAGER=0
FARM_IMAGE="juicefarm:local"
MANAGER_IMAGE="juicemount-manager:local"
DRY_RUN=0
REPO=""

usage() { sed -n '2,40p' "$0"; exit "${1:-0}"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --repo)              REPO="$2"; shift 2 ;;
    --host)              HOST="$2"; shift 2 ;;
    --dest)              DEST="$2"; shift 2 ;;
    --farm-container)    FARM_CONTAINER="$2"; shift 2 ;;
    --manager-container) MANAGER_CONTAINER="$2"; shift 2 ;;
    --skip-manager)      SKIP_MANAGER=1; shift ;;
    --dry-run)           DRY_RUN=1; shift ;;
    -h|--help)           usage 0 ;;
    *) echo "update-server: unknown arg: $1" >&2; usage 1 ;;
  esac
done

# ---- locate + sanity-check the repo root -----------------------------------
if [ -z "$REPO" ]; then
  # script lives at <repo>/server/scripts/update-server.sh
  REPO="$(cd "$(dirname "$0")/../.." && pwd)"
fi
for f in go.mod server/juicefarm/Dockerfile server/juicemount-manager/Dockerfile; do
  if [ ! -f "$REPO/$f" ]; then
    echo "update-server: $REPO does not look like the JuiceMount repo (missing $f)" >&2
    exit 1
  fi
done

# ---- guard: the two target names must never be infra containers ------------
# (defense in depth: the remote script re-checks on the host too)
for name in "$FARM_CONTAINER" "$MANAGER_CONTAINER"; do
  case "$name" in
    *redis*|*minio*|*juicefs*)
      echo "update-server: REFUSING — target container '$name' matches a protected infra name (redis/minio/juicefs)." >&2
      echo "Restarting those breaks the live Mac client. Pick the farm-worker/manager container names." >&2
      exit 1 ;;
  esac
done

# ---- the remote program -----------------------------------------------------
# Generated locally, executed on the host via `ssh ... bash -s`. Variables are
# injected through a printf preamble; the body is a QUOTED heredoc so nothing
# expands locally. All container introspection happens ON THE HOST at runtime.
build_remote_script() {
  printf 'DEST=%q\n'              "$DEST"
  printf 'FARM_CONTAINER=%q\n'    "$FARM_CONTAINER"
  printf 'MANAGER_CONTAINER=%q\n' "$MANAGER_CONTAINER"
  printf 'SKIP_MANAGER=%q\n' "$SKIP_MANAGER"
  printf 'FARM_IMAGE=%q\n'        "$FARM_IMAGE"
  printf 'MANAGER_IMAGE=%q\n'     "$MANAGER_IMAGE"
  cat <<'REMOTE_EOF'
set -euo pipefail

log() { printf '[update-server:remote] %s\n' "$*"; }

# ############################################################################
# ## HARD GUARD — NEVER touch redis / minio / juicefs containers.           ##
# ## They carry live volume state; restarting them breaks the Mac client.  ##
# ############################################################################
for name in "$FARM_CONTAINER" "$MANAGER_CONTAINER"; do
  case "$name" in
    *redis*|*minio*|*juicefs*)
      log "REFUSING: '$name' matches a protected infra container name"; exit 1 ;;
  esac
done

cd "$DEST"

# A compose/app recreation can silently discard a credential that exists only
# in the live container's runtime environment. Refuse the entire update before
# touching either workload unless the Manager we are about to preserve has a
# non-placeholder 32+ character key. The value stays in shell memory only: it
# is never logged, written to a backup helper file, or placed in argv.
preflight_manager_auth() {
  [ "$SKIP_MANAGER" = "1" ] && return 0
  if ! docker inspect "$MANAGER_CONTAINER" >/dev/null 2>&1; then
    log "ERROR: manager '$MANAGER_CONTAINER' not found — cannot preserve its runtime authentication"
    exit 1
  fi
  local key
  key="$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$MANAGER_CONTAINER" |
    sed -n 's/^JM_ADMIN_KEY=//p' | head -1)"
  case "$key" in
    CHANGEME*|CHANGE*|REPLACE*|replaceme*)
      log "ERROR: manager runtime JM_ADMIN_KEY is still a placeholder; refusing recreation"
      exit 1 ;;
  esac
  if [ "${#key}" -lt 32 ]; then
    log "ERROR: manager runtime JM_ADMIN_KEY is missing or shorter than 32 characters; refusing recreation"
    exit 1
  fi
  unset key
  log "manager authentication preflight passed"
}

preflight_manager_auth

# ---- build both images on the host (BuildKit for the cache mounts) --------
log "building $FARM_IMAGE"
DOCKER_BUILDKIT=1 docker build -f server/juicefarm/Dockerfile -t "$FARM_IMAGE" .
log "building $MANAGER_IMAGE"
DOCKER_BUILDKIT=1 docker build -f server/juicemount-manager/Dockerfile -t "$MANAGER_IMAGE" .

# ---- helpers: reconstruct `docker run` args from a live container ----------

# env entries the RUNTIME supplied (container env minus the OLD image's baked
# env) — re-injecting baked defaults would pin stale values over the NEW
# image's own defaults.
runtime_env() { # $1=container
  local cimg
  cimg="$(docker inspect -f '{{.Config.Image}}' "$1")"
  comm -23 \
    <(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$1" | sed '/^$/d' | LC_ALL=C sort) \
    <(docker image inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$cimg" 2>/dev/null | sed '/^$/d' | LC_ALL=C sort)
}

# container Cmd only when it differs from the old image's default (i.e. it was
# an explicit compose/run override, not inherited).
runtime_cmd() { # $1=container
  local cimg ccmd icmd
  cimg="$(docker inspect -f '{{.Config.Image}}' "$1")"
  ccmd="$(docker inspect -f '{{json .Config.Cmd}}' "$1")"
  icmd="$(docker image inspect -f '{{json .Config.Cmd}}' "$cimg" 2>/dev/null || echo null)"
  if [ "$ccmd" != "null" ] && [ "$ccmd" != "$icmd" ]; then
    docker inspect -f '{{range .Config.Cmd}}{{println .}}{{end}}' "$1"
  fi
}

# recreate <container> <new-image>: capture config → backup → stop/rm → run.
recreate() {
  local name="$1" image="$2"
  case "$name" in *redis*|*minio*|*juicefs*)
    log "REFUSING recreate of protected container '$name'"; exit 1 ;; esac

  if ! docker inspect "$name" >/dev/null 2>&1; then
    log "ERROR: container '$name' not found — cannot introspect its run args."
    log "First-time launches are a compose/install task, not an update push."
    exit 1
  fi

  mkdir -p "$DEST/.update-backups"
  local backup="$DEST/.update-backups/${name}-$(date +%Y%m%d-%H%M%S).json"
  if ! command -v jq >/dev/null 2>&1; then
    log "ERROR: jq is required to create a credential-redacted config backup"
    exit 1
  fi
  # Preserve container topology for incident recovery, but retain only
  # environment variable names. Raw docker-inspect backups persist admin keys,
  # object-store credentials, and authenticated URLs in plaintext.
  docker inspect "$name" |
    jq 'map(.Config.Env = ((.Config.Env // []) | map((split("=")[0]) + "=<redacted>")))' > "$backup"
  chmod 600 "$backup"
  log "captured credential-redacted $name config → $backup"

  # -------- reconstruct run args (never hardcoded) --------
  local -a args=(-d --name "$name")
  local -a env_assignments=()

  # restart policy
  local rp rc
  rp="$(docker inspect -f '{{.HostConfig.RestartPolicy.Name}}' "$name")"
  rc="$(docker inspect -f '{{.HostConfig.RestartPolicy.MaximumRetryCount}}' "$name")"
  if [ -n "$rp" ] && [ "$rp" != "no" ]; then
    if [ "$rp" = "on-failure" ] && [ "$rc" -gt 0 ]; then args+=(--restart "on-failure:$rc")
    else args+=(--restart "$rp"); fi
  fi

  # primary network (extra networks reconnected after run)
  local netmode
  netmode="$(docker inspect -f '{{.HostConfig.NetworkMode}}' "$name")"
  [ -n "$netmode" ] && [ "$netmode" != "default" ] && args+=(--network "$netmode")

  # env (runtime-provided only). Pass only each variable NAME in docker's
  # argv; export its captured value inside the short-lived launch subshell so
  # credentials never appear in `ps`, shell history, or deployment logs.
  local e key
  while IFS= read -r e; do
    [ -z "$e" ] && continue
    key="${e%%=*}"
    if [[ ! "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
      log "ERROR: refusing malformed runtime environment name while recreating $name"
      exit 1
    fi
    case "$key" in
      HOME|PATH|SHELL|USER|LOGNAME)
        log "ERROR: refusing to repurpose host process variable '$key' while recreating $name"
        exit 1 ;;
    esac
    env_assignments+=("$e")
    args+=(-e "$key")
  done < <(runtime_env "$name")

  # mounts: named volumes + binds, preserving ro
  while IFS='|' read -r mtype msrc mdst mrw; do
    [ -z "$mdst" ] && continue
    local suffix=""
    [ "$mrw" = "false" ] && suffix=":ro"
    case "$mtype" in
      volume|bind) args+=(-v "${msrc}:${mdst}${suffix}") ;;
    esac
  done < <(docker inspect -f '{{range .Mounts}}{{.Type}}|{{if eq .Type "volume"}}{{.Name}}{{else}}{{.Source}}{{end}}|{{.Destination}}|{{.RW}}{{println}}{{end}}' "$name" | sed '/^$/d')

  # published ports
  while IFS= read -r p; do [ -n "$p" ] && args+=(-p "$p"); done < <(
    docker inspect -f '{{range $port, $binds := .HostConfig.PortBindings}}{{range $binds}}{{if .HostIp}}{{.HostIp}}:{{end}}{{.HostPort}}:{{$port}}{{println}}{{end}}{{end}}' "$name" | sed '/^$/d')

  # privileges / caps / devices / security opts (the farm needs SYS_ADMIN +
  # /dev/fuse for its in-container JuiceFS mount)
  [ "$(docker inspect -f '{{.HostConfig.Privileged}}' "$name")" = "true" ] && args+=(--privileged)
  while IFS= read -r c; do [ -n "$c" ] && args+=(--cap-add "$c"); done < <(
    docker inspect -f '{{range .HostConfig.CapAdd}}{{println .}}{{end}}' "$name" | sed '/^$/d')
  while IFS= read -r d; do [ -n "$d" ] && args+=(--device "$d"); done < <(
    docker inspect -f '{{range .HostConfig.Devices}}{{.PathOnHost}}:{{.PathInContainer}}:{{.CgroupPermissions}}{{println}}{{end}}' "$name" | sed '/^$/d')
  while IFS= read -r s; do [ -n "$s" ] && args+=(--security-opt "$s"); done < <(
    docker inspect -f '{{range .HostConfig.SecurityOpt}}{{println .}}{{end}}' "$name" | sed '/^$/d')
  while IFS= read -r h; do [ -n "$h" ] && args+=(--add-host "$h"); done < <(
    docker inspect -f '{{range .HostConfig.ExtraHosts}}{{println .}}{{end}}' "$name" | sed '/^$/d')

  # resource limits
  local nano mem
  nano="$(docker inspect -f '{{.HostConfig.NanoCpus}}' "$name")"
  mem="$(docker inspect -f '{{.HostConfig.Memory}}' "$name")"
  [ "$nano" -gt 0 ] 2>/dev/null && args+=(--cpus "$(awk "BEGIN{printf \"%g\", $nano/1000000000}")")
  [ "$mem" -gt 0 ] 2>/dev/null && args+=(-m "$mem")

  local user
  user="$(docker inspect -f '{{.Config.User}}' "$name")"
  [ -n "$user" ] && args+=(--user "$user")

  # explicit command override (only if it wasn't inherited from the old image)
  local -a cmd=()
  while IFS= read -r c; do [ -n "$c" ] && cmd+=("$c"); done < <(runtime_cmd "$name")

  # extra networks beyond the primary
  local -a extra_nets=()
  while IFS= read -r n; do
    [ -n "$n" ] && [ "$n" != "$netmode" ] && extra_nets+=("$n")
  done < <(docker inspect -f '{{range $n, $_ := .NetworkSettings.Networks}}{{println $n}}{{end}}' "$name" | sed '/^$/d')

  # -------- stop/rm/run --------
  log "recreating $name → $image"
  docker stop "$name" >/dev/null
  docker rm "$name" >/dev/null
  (
    for e in "${env_assignments[@]}"; do export "$e"; done
    docker run "${args[@]}" "$image" "${cmd[@]}"
  )
  for n in "${extra_nets[@]}"; do docker network connect "$n" "$name" || true; done
}

recreate "$FARM_CONTAINER" "$FARM_IMAGE"
if [ "$SKIP_MANAGER" = "1" ]; then
  log "skipping manager recreate (--skip-manager). Before any separate compose/app recreation, supply the existing JM_ADMIN_KEY through the deployment environment and require 'docker compose config --quiet' to pass; never recreate from persisted YAML with an empty key."
else
  recreate "$MANAGER_CONTAINER" "$MANAGER_IMAGE"
fi

# ---- verify -----------------------------------------------------------------
log "verifying farm worker stays up"
sleep 5
if [ "$(docker inspect -f '{{.State.Running}}' "$FARM_CONTAINER")" != "true" ]; then
  log "FAIL: $FARM_CONTAINER is not running after recreate; last logs:"
  docker logs --tail 40 "$FARM_CONTAINER" || true
  exit 1
fi
log "farm worker up"

if [ "$SKIP_MANAGER" = "1" ]; then
  log "manager verify skipped (--skip-manager)"
else
  log "verifying manager /api/farm returns 200"
  verify_mgr() {
    # Expand JM_ADMIN_KEY only inside the container and stream curl's header
    # config over stdin. The credential is absent from host/container argv.
    docker exec "$MANAGER_CONTAINER" sh -ec '
      [ "${#JM_ADMIN_KEY}" -ge 32 ] || exit 22
      printf '\''header = "X-JuiceMount-Admin-Key: %s"\n'\'' "$JM_ADMIN_KEY" |
        curl --config - -fsS -o /dev/null -w '\''%{http_code}'\'' http://127.0.0.1:8080/api/farm
    '
  }
  ok=0
  for i in $(seq 1 12); do
    code="$(verify_mgr || true)"
    if [ "$code" = "200" ]; then ok=1; break; fi
    sleep 5
  done
  if [ "$ok" != "1" ]; then
    log "FAIL: manager /api/farm did not return 200; last logs:"
    docker logs --tail 40 "$MANAGER_CONTAINER" || true
    exit 1
  fi
  log "manager /api/farm → 200"
fi
log "DONE: farm updated. redis/minio/juicefs untouched."
REMOTE_EOF
}

# ---- plan -------------------------------------------------------------------
RSYNC_CMD=(rsync -a --delete --exclude .git --exclude .claude "$REPO/" "$HOST:$DEST/")

if [ "$DRY_RUN" = "1" ]; then
  echo "== update-server DRY RUN — nothing will be executed =="
  echo
  echo "-- step 1: sync worktree CONTENTS (note the trailing slash on the source) --"
  printf '  %q' "${RSYNC_CMD[@]}"; echo
  echo
  echo "-- step 2+3+4: remote build/recreate/verify script (via: ssh $HOST bash -s) --"
  build_remote_script | sed 's/^/  | /'
  echo
  echo "== END DRY RUN — redis/minio/juicefs are never touched =="
  exit 0
fi

echo "[update-server] syncing $REPO/ → $HOST:$DEST/"
"${RSYNC_CMD[@]}"

echo "[update-server] building + recreating on $HOST (farm=$FARM_CONTAINER manager=$MANAGER_CONTAINER)"
build_remote_script | ssh "$HOST" bash -s

echo "[update-server] complete."
