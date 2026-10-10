#!/usr/bin/env bash
# Roll out the canonical Compose stack on the NAS. Never use this for a
# TrueNAS-generated Compose project or a full-stack up.
#
# Modes (exactly one):
#   (default) release   recreate only juicemount-manager and juicefarm with
#                       new digests; every other service is protected.
#   --cutover-proxy     H-8B port swap from the pre-proxy compose (Redis on
#                       host port 30179) to the proxy compose (Redis internal,
#                       respproxy on 30179), together with new Manager/farm
#                       digests. Staged: Redis, proxy, juicefs-init, juicefs,
#                       Manager+farm. Any failure rolls the whole stack back
#                       to the pre-proxy compose (--previous-ref) and the
#                       previous digests.
#   --proxy-rollback    plan section 3 rollback step 3: proxy out of the path,
#                       Redis back on 30179 with every principal mirrored as a
#                       plain ACL user. Images unchanged.
#   --proxy-restore     undo --proxy-rollback: the proxy takes 30179 again.
set -euo pipefail

usage() {
  echo 'usage: rollout-compose.sh --project NAME --manager-digest sha256:HEX --farm-digest sha256:HEX --render-digest sha256:HEX --ack-worker-quiesced [--repo PATH] [--dry-run]' >&2
  echo '       rollout-compose.sh --project NAME --cutover-proxy --previous-ref GIT_REF --manager-digest ... --farm-digest ... --render-digest ... --ack-worker-quiesced --ack-macs-drained [--repo PATH] [--dry-run]' >&2
  echo '       rollout-compose.sh --project NAME (--proxy-rollback | --proxy-restore) --ack-worker-quiesced --ack-macs-drained [--repo PATH] [--dry-run]' >&2
  exit "${1:-2}"
}

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
PROJECT=""
MANAGER_DIGEST=""
FARM_DIGEST=""
RENDER_DIGEST=""
ACK_QUIESCED=0
ACK_MACS=0
DRY_RUN=0
MODE=release
PREVIOUS_REF=""
set_mode() { [ "$MODE" = release ] || usage 2; MODE="$1"; }
while [ "$#" -gt 0 ]; do
  case "$1" in
    --repo) REPO="$2"; shift 2 ;;
    --cutover-proxy) set_mode cutover; shift ;;
    --proxy-rollback) set_mode proxy-rollback; shift ;;
    --proxy-restore) set_mode proxy-restore; shift ;;
    --previous-ref) PREVIOUS_REF="$2"; shift 2 ;;
    --project) PROJECT="$2"; shift 2 ;;
    --manager-digest) MANAGER_DIGEST="$2"; shift 2 ;;
    --farm-digest) FARM_DIGEST="$2"; shift 2 ;;
    --render-digest) RENDER_DIGEST="$2"; shift 2 ;;
    --ack-worker-quiesced) ACK_QUIESCED=1; shift ;;
    --ack-macs-drained) ACK_MACS=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage 0 ;;
    *) usage 2 ;;
  esac
done

REPO="$(cd "$REPO" && pwd)"
COMPOSE_FILE="$REPO/server/docker-compose.yml"
ROLLBACK_OVERRIDE="$REPO/server/respproxy/docker-compose.rollback.yml"
# The pre-proxy compose is kept here (gitignored by the untracked check
# below) so containers rolled back to it keep a readable config file.
PRE_DIR="$REPO/.h8b-rollback"
PRE_COMPOSE="$PRE_DIR/docker-compose.pre-proxy.yml"
ENV_FILE="$REPO/server/.env"
LOCK="$REPO/.update-lock"
MANAGER_REPO='ghcr.io/lelanddutcher/juicemount-manager'
FARM_REPO='ghcr.io/lelanddutcher/juicefarm'
RENDER_REPO='ghcr.io/lelanddutcher/juicefarm-gpu'
RESPPROXY_REPO='ghcr.io/lelanddutcher/jm-respproxy'
# The one place naming the oldest acceptable proxy source commit (tracked,
# so the clean-tree check covers it). See require_proxy_gate.
GATE_FILE="$REPO/server/respproxy/min-gate-commit"
# Principals the proxy compose defines from server/.env (name:VAR suffix).
SERVER_PRINCIPALS='farm:FARM manager:MANAGER bg:BG webdav:WEBDAV admin:ADMIN'
# Shared principals directory inside the Manager container.
MANAGER_PROXY_DIR='/var/lib/jm-respproxy'
case "$MODE" in
  release|cutover)
    for digest in "$MANAGER_DIGEST" "$FARM_DIGEST" "$RENDER_DIGEST"; do
      [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo 'rollout-compose: all three release digests must be full lowercase sha256 values' >&2; exit 2; }
    done ;;
  *)
    [ -z "$MANAGER_DIGEST$FARM_DIGEST$RENDER_DIGEST" ] || { echo "rollout-compose: --$MODE keeps the running images; do not pass digests" >&2; exit 2; } ;;
esac
if [ "$MODE" != release ] && [ "$ACK_MACS" != 1 ]; then
  # Redis restarts: a Mac still writing loses its session and, after a swap,
  # owns no session record at the proxy (BG_ONLY/ADMIN_ONLY refusals).
  echo "rollout-compose: --$MODE restarts Redis; every Mac must first drain its spool to zero and quit JuiceMount (--ack-macs-drained)" >&2; exit 2
fi
if [ "$MODE" = cutover ]; then
  [[ "$PREVIOUS_REF" =~ ^[0-9A-Za-z._/-]+$ ]] || { echo 'rollout-compose: --cutover-proxy needs --previous-ref, the git ref of the running pre-proxy compose' >&2; exit 2; }
else
  [ -z "$PREVIOUS_REF" ] || usage 2
fi
[[ "$PROJECT" =~ ^[a-z][a-z0-9_-]*$ ]] || { echo 'rollout-compose: explicit Compose project name is required' >&2; exit 2; }
[ "$ACK_QUIESCED" = 1 ] || { echo 'rollout-compose: an operator must confirm the farm queue is quiesced' >&2; exit 2; }
[ -f "$COMPOSE_FILE" ] && [ -f "$ENV_FILE" ] || { echo 'rollout-compose: canonical Compose file and server/.env are required' >&2; exit 2; }
[ -f "$REPO/internal/version/version.go" ] || { echo 'rollout-compose: source version missing' >&2; exit 2; }
git -C "$REPO" rev-parse --verify HEAD >/dev/null
if ! git -C "$REPO" diff --quiet --ignore-submodules -- || ! git -C "$REPO" diff --cached --quiet --ignore-submodules --; then
  echo 'rollout-compose: refusing dirty tracked source' >&2; exit 2
fi
# Untracked source can change a release even when HEAD is clean. Local build
# output and the private server/.env are excluded from this check.
while IFS= read -r -d '' path; do
  case "$path" in
    build/*|app/JuiceMount/.build/*|.codex/*|.update-lock/*|.h8b-rollback/*|server/.env) ;;
    *) echo "rollout-compose: refusing untracked source: $path" >&2; exit 2 ;;
  esac
done < <(git -C "$REPO" ls-files --others -z)
SOURCE_COMMIT="$(git -C "$REPO" rev-parse HEAD)"
SOURCE_VERSION="$(awk -F'"' '/^var Version = / { print $2; exit }' "$REPO/internal/version/version.go")"
[[ "$SOURCE_COMMIT" =~ ^[0-9a-f]{40}$ && "$SOURCE_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]] || {
  echo 'rollout-compose: source identity invalid' >&2; exit 2;
}

NEW_MANAGER="$MANAGER_REPO@$MANAGER_DIGEST"
NEW_FARM="$FARM_REPO@$FARM_DIGEST"
NEW_RENDER="$RENDER_REPO@$RENDER_DIGEST"
if [ "$MODE" = cutover ]; then
  git -C "$REPO" rev-parse --verify "$PREVIOUS_REF^{commit}" >/dev/null 2>&1 || { echo 'rollout-compose: --previous-ref is not a commit in this checkout' >&2; exit 2; }
  prev_src="$(git -C "$REPO" show "$PREVIOUS_REF:server/docker-compose.yml")" || { echo 'rollout-compose: --previous-ref has no server/docker-compose.yml' >&2; exit 2; }
  # The fallback target must be the topology being replaced: Redis itself
  # publishing 30179 and no proxy service.
  if ! printf '%s\n' "$prev_src" | grep -q '"30179:6379"' || printf '%s\n' "$prev_src" | grep -q 'respproxy'; then
    echo 'rollout-compose: --previous-ref is not a pre-proxy compose (Redis must publish 30179, no respproxy)' >&2; exit 2
  fi
  unset prev_src
fi
for key in JM_MANAGER_DIGEST JM_FARM_DIGEST JM_FARM_RENDER_DIGEST; do
  if [ "${!key+x}" = x ]; then
    echo "rollout-compose: unset $key in the shell; the private .env must be the persistent digest authority" >&2
    exit 2
  fi
done
if [ "$DRY_RUN" = 1 ]; then
  printf 'rollout-compose dry run: mode=%s project=%s source=%s (%s)\n' "$MODE" "$PROJECT" "$SOURCE_VERSION" "$SOURCE_COMMIT"
  case "$MODE" in
    release)
      printf 'pull and verify: %s %s %s\n' "$NEW_MANAGER" "$NEW_FARM" "$NEW_RENDER"
      echo 'preflight live Compose labels, config hashes, credentials, old local images, and protected container IDs'
      echo 'recreate only juicemount-manager and juicefarm with --no-deps --no-build --pull never'
      echo 'atomically persist digest assignments in server/.env before service recreation'
      echo 'verify protected health and workload identity; on failure restore .env and previous images' ;;
    cutover)
      printf 'pull and verify: %s %s %s and the respproxy digest from server/.env\n' "$NEW_MANAGER" "$NEW_FARM" "$NEW_RENDER"
      printf 'preflight against the pre-proxy compose from %s; Redis must publish 30179, respproxy must not run\n' "$PREVIOUS_REF"
      echo 'refuse a respproxy image whose org.opencontainers.image.revision does not descend from server/respproxy/min-gate-commit'
      echo 'require JM_REDIS_PROXY_PASSWORD and JM_PROXY_{FARM,MANAGER,BG,WEBDAV,ADMIN}_PASSWORD in server/.env'
      echo 'operator: every Mac drained its spool to zero and quit JuiceMount (--ack-macs-drained)'
      echo 'stage 0: stop farm, Manager, juicefs (quiesce; Redis gets a 120 s SIGTERM grace for its RDB save)'
      echo 'stage 1: recreate redis (no host port, ACL default off); force jmlease:v1:cfg mode=passthrough; stage 2: start respproxy on 30179'
      echo 'stage 3: rerun juicefs-init through the proxy; stage 4: juicefs; stage 5: Manager + farm on new digests'
      echo 'any failure: remove respproxy, recreate every stage from the pre-proxy compose with the previous digests' ;;
    proxy-rollback)
      echo 'build the rollback ACL: server principals from server/.env + enrolled Macs from the Manager (sha256 only)'
      echo 'write it to the Redis data dir; remove respproxy; recreate redis on 30179 with the rollback ACL'
      echo 'recreate juicefs-init, juicefs, Manager and farm pointing straight at Redis; on failure restore the proxy' ;;
    proxy-restore)
      echo 'stop farm, Manager, juicefs; recreate redis without a host port; force mode=passthrough; start respproxy on 30179'
      echo 'recreate juicefs-init, juicefs, Manager, farm'
      echo 'on failure return to the rollback topology' ;;
  esac
  exit 0
fi

command -v docker >/dev/null && command -v jq >/dev/null || { echo 'rollout-compose: docker and jq are required' >&2; exit 2; }
[ ! -L "$ENV_FILE" ] && [ -O "$ENV_FILE" ] && [ -w "$ENV_FILE" ] || {
  echo 'rollout-compose: server/.env must be a writable regular file owned by the rollout user' >&2; exit 2;
}
umask 077
# This directory lock is intentionally persistent after a process crash. Never
# auto-steal it: inspect the deployed project before removing it manually.
if ! mkdir "$LOCK" 2>/dev/null; then
  echo "rollout-compose: lock exists at $LOCK; inspect before retrying" >&2; exit 2
fi
printf '%s %s\n' "$$" "$SOURCE_COMMIT" > "$LOCK/owner"
LOCK_HELD=1
ROLLBACK_ARMED=0
FALLBACK_TOPO=""
ENV_STAGED=0
ENV_TEMP=""
OLD_MANAGER=""
OLD_FARM=""
OLD_RENDER_DIGEST=""
OLD_MANAGER_ID=""
OLD_FARM_ID=""
OLD_MANAGER_IDENTITY=""
OLD_FARM_IDENTITY=""

log() { printf '[rollout-compose] %s\n' "$*"; }
compose_topo() { # topology (proxy|pre|direct), compose args...
  local topo="$1"
  shift
  local -a files
  case "$topo" in
    proxy) files=(-f "$COMPOSE_FILE") ;;
    pre) files=(-f "$PRE_COMPOSE") ;;
    direct) files=(-f "$COMPOSE_FILE" -f "$ROLLBACK_OVERRIDE") ;;
    *) return 1 ;;
  esac
  docker compose --project-name "$PROJECT" --project-directory "$REPO/server" --env-file "$ENV_FILE" "${files[@]}" "$@"
}
compose() { compose_topo proxy "$@"; }
with_digests() { # manager digest, farm digest, render digest, compose args...
  local manager="$1" farm="$2" render="$3"
  shift 3
  JM_MANAGER_DIGEST="$manager" JM_FARM_DIGEST="$farm" JM_FARM_RENDER_DIGEST="$render" compose "$@"
}
with_digests_topo() { # topology, manager, farm, render digests, compose args...
  local topo="$1" manager="$2" farm="$3" render="$4"
  shift 4
  JM_MANAGER_DIGEST="$manager" JM_FARM_DIGEST="$farm" JM_FARM_RENDER_DIGEST="$render" compose_topo "$topo" "$@"
}
release_lock() {
  if [ "$LOCK_HELD" = 1 ]; then
    rm -f "$LOCK/protected-ids" "$LOCK/env.backup"
    rm "$LOCK/owner"
    rmdir "$LOCK"
    LOCK_HELD=0
  fi
}
restore_env() {
  local temp
  temp="$(mktemp "$ENV_FILE.restore.XXXXXX")" || return 1
  if ! cp "$LOCK/env.backup" "$temp" || ! chmod 600 "$temp" || ! mv -f "$temp" "$ENV_FILE"; then
    rm -f "$temp"
    return 1
  fi
  ENV_STAGED=0
}
on_exit() {
  local status="$?" rollback_failed=0
  trap - EXIT
  trap '' HUP INT TERM
  if [ -n "$ENV_TEMP" ] && ! rm -f "$ENV_TEMP"; then
    rollback_failed=1
    log 'CRITICAL: private temporary .env could not be removed'
  fi
  if [ "$status" -ne 0 ] && [ "$ENV_STAGED" = 1 ]; then
    if ! restore_env; then
      rollback_failed=1
      log 'CRITICAL: private .env could not be restored; backup retained under lock'
    fi
  fi
  if [ "$status" -ne 0 ] && [ "$ROLLBACK_ARMED" = 1 ] && [ "$MODE" != release ]; then
    log "topology change failed; restoring the $FALLBACK_TOPO topology with the previous images"
    if ! apply_topology "$FALLBACK_TOPO" "${OLD_MANAGER##*@}" "${OLD_FARM##*@}" "$OLD_RENDER_DIGEST"; then
      rollback_failed=1
      log "CRITICAL: $FALLBACK_TOPO topology rollback failed"
    fi
    if ! verify_running "$OLD_MANAGER" "$OLD_FARM" "$OLD_MANAGER_IDENTITY" "$OLD_FARM_IDENTITY"; then
      rollback_failed=1
      log 'CRITICAL: prior service health/identity did not recover'
    fi
  elif [ "$status" -ne 0 ] && [ "$ROLLBACK_ARMED" = 1 ]; then
    log 'rollout failed; restoring previous digest-pinned services'
    if ! with_digests "${OLD_MANAGER##*@}" "${OLD_FARM##*@}" "$OLD_RENDER_DIGEST" \
      up -d --no-deps --no-build --pull never --force-recreate juicemount-manager juicefarm; then
      rollback_failed=1
      log 'CRITICAL: previous-image rollback failed'
    fi
    if ! verify_running "$OLD_MANAGER" "$OLD_FARM" "$OLD_MANAGER_IDENTITY" "$OLD_FARM_IDENTITY"; then
      rollback_failed=1
      log 'CRITICAL: prior service health/identity did not recover'
    fi
  fi
  if [ "$rollback_failed" = 0 ]; then
    release_lock
  else
    log "CRITICAL: lock retained at $LOCK for operator recovery"
  fi
  exit "$status"
}
trap on_exit EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
cp -p "$ENV_FILE" "$LOCK/env.backup"
chmod 600 "$LOCK/env.backup"
cmp -s "$ENV_FILE" "$LOCK/env.backup" || { log 'private .env changed while taking the locked snapshot'; exit 1; }

# Every topology transition can enter the proxy topology, including its
# automatic rollback. That path forces the stored lease mode to passthrough,
# so an opted-in stage-birth proxy would refuse startup. Read Compose's
# resolved environment: shell overrides and quoted .env values count.
if [ "$MODE" != release ]; then
  stage_birth="$(compose config --format json | jq -er '
    .services.respproxy.environment as $env
    | if ($env | type) != "object" then error("respproxy environment missing")
      else $env.JM_RESPPROXY_STAGE_BIRTH as $value
      | if $value == null or $value == "" or $value == "false" then "false"
        elif $value == "true" then "true"
        else error("resolved JM_RESPPROXY_STAGE_BIRTH must be true or false") end
      end
  ')" || { log 'cannot resolve canonical respproxy stage-birth configuration'; exit 1; }
  case "$stage_birth" in
    false) ;;
    true)
      log 'topology transition refused: disable JM_RESPPROXY_STAGE_BIRTH before changing topology, then deliberately reactivate enforce mode and stage births after the transition'
      exit 1 ;;
  esac
  unset stage_birth
fi

service_id() {
  local ids
  ids="$(compose ps --all -q "$1")"
  [ "$(printf '%s\n' "$ids" | sed '/^$/d' | wc -l | tr -d ' ')" = 1 ] || return 1
  printf '%s\n' "$ids"
}
label() { docker inspect -f "{{ index .Config.Labels \"$2\" }}" "$1"; }
require_ownership() { # service, expected container id
  local service="$1" id="$2"
  [ "$(label "$id" com.docker.compose.project)" = "$PROJECT" ] &&
    [ "$(label "$id" com.docker.compose.service)" = "$service" ] &&
    [ "$(label "$id" com.docker.compose.project.working_dir)" = "$REPO/server" ] &&
    config_files_ok "$(label "$id" com.docker.compose.project.config_files)"
}
# Release rollouts accept only the canonical file. Topology modes also accept
# the two files their own rollbacks create containers from.
config_files_ok() {
  [ "$1" = "$COMPOSE_FILE" ] && return 0
  [ "$MODE" != release ] || return 1
  [ "$1" = "$PRE_COMPOSE" ] || [ "$1" = "$COMPOSE_FILE,$ROLLBACK_OVERRIDE" ]
}
require_service() {
  require_ownership "$1" "$2" && [ "$(docker inspect -f '{{.State.Running}}' "$2")" = true ]
}
require_init() {
  require_ownership juicefs-init "$1" &&
    [ "$(docker inspect -f '{{.State.Status}}' "$1")" = exited ] &&
    [ "$(docker inspect -f '{{.State.ExitCode}}' "$1")" = 0 ]
}
hash_for() { # manager, farm, render digests, service, [topology]
  local digest_manager="$1" digest_farm="$2" digest_render="$3" service="$4" topo="${5:-proxy}"
  with_digests_topo "$topo" "$digest_manager" "$digest_farm" "$digest_render" config --hash "$service" | awk '{print $NF}'
}
require_image() {
  [[ "$1" == "$2"@sha256:* && "${1##*@}" =~ ^sha256:[0-9a-f]{64}$ ]]
}

OLD_MANAGER_ID="$(service_id juicemount-manager)" || { log 'Manager must be one existing Compose service'; exit 1; }
OLD_FARM_ID="$(service_id juicefarm)" || { log 'farm must be one existing Compose service'; exit 1; }
require_service juicemount-manager "$OLD_MANAGER_ID" && require_service juicefarm "$OLD_FARM_ID" || {
  log 'live service ownership, working directory, config file, or running state differs from canonical Compose'; exit 1;
}
OLD_MANAGER="$(docker inspect -f '{{.Config.Image}}' "$OLD_MANAGER_ID")"
OLD_FARM="$(docker inspect -f '{{.Config.Image}}' "$OLD_FARM_ID")"
require_image "$OLD_MANAGER" "$MANAGER_REPO" && require_image "$OLD_FARM" "$FARM_REPO" || {
  log 'previous images are not immutable canonical digest references'; exit 1;
}
OLD_RENDER="$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$OLD_MANAGER_ID" | sed -n 's/^JM_FARM_RENDER_IMAGE=//p')"
require_image "$OLD_RENDER" "$RENDER_REPO" || { log 'previous render image is not digest-pinned'; exit 1; }
OLD_RENDER_DIGEST="${OLD_RENDER##*@}"
docker image inspect "$OLD_MANAGER" "$OLD_FARM" >/dev/null || { log 'previous images are unavailable locally for rollback'; exit 1; }
OLD_MANAGER_IDENTITY="$(docker exec "$OLD_MANAGER_ID" /usr/local/bin/juicemount-manager --build-info)" || exit 1
OLD_FARM_IDENTITY="$(docker exec "$OLD_FARM_ID" /usr/local/bin/jmfarm --build-info)" || exit 1
[[ "$OLD_MANAGER_IDENTITY" =~ ^juicemount-manager\  && "$OLD_FARM_IDENTITY" =~ ^jmfarm\  ]] || {
  log 'previous binary identity is unavailable'; exit 1;
}

# Protected services must be healthy now and unchanged afterwards. The
# topology modes recreate redis, respproxy, juicefs-init and juicefs on
# purpose, so only MinIO stays protected there.
if [ "$MODE" = release ]; then
  PROTECTED='redis minio respproxy juicefs juicefs-init'
else
  PROTECTED='minio'
fi
for service in $PROTECTED; do
  id="$(service_id "$service")" || {
    if [ "$service" = respproxy ]; then
      log 'respproxy is missing: this stack predates the H-8B proxy; run --cutover-proxy first'
    else
      log "protected $service container missing or ambiguous"
    fi
    exit 1
  }
  if [ "$service" = juicefs-init ]; then
    require_init "$id" || { log 'juicefs-init must be the successful canonical one-shot container'; exit 1; }
  else
    require_service "$service" "$id" &&
      [ "$(docker inspect -f '{{.State.Health.Status}}' "$id")" = healthy ] || {
        log "protected $service must be running and healthy"; exit 1;
      }
  fi
  printf '%s %s\n' "$service" "$id" >> "$LOCK/protected-ids"
done

# Topology of the running stack, from who publishes 30179.
publishes_30179() { docker inspect -f '{{json .HostConfig.PortBindings}}' "$1" | grep -q '"HostPort":"30179"'; }
proxy_running() {
  local id
  id="$(service_id respproxy 2>/dev/null)" || return 1
  [ "$(docker inspect -f '{{.State.Running}}' "$id")" = true ]
}
if [ "$MODE" != release ]; then
  REDIS_ID="$(service_id redis)" || { log 'redis must be one existing Compose service'; exit 1; }
  require_service redis "$REDIS_ID" && [ "$(docker inspect -f '{{.State.Health.Status}}' "$REDIS_ID")" = healthy ] || {
    log 'redis must be running and healthy before a topology change'; exit 1;
  }
  case "$MODE" in
    cutover|proxy-restore)
      publishes_30179 "$REDIS_ID" && ! proxy_running || {
        log 'Redis does not publish 30179 (or respproxy is running): this stack is already behind the proxy'; exit 1;
      } ;;
    proxy-rollback)
      ! publishes_30179 "$REDIS_ID" || { log 'Redis already publishes 30179: the proxy is not in the path'; exit 1; } ;;
  esac
fi

# The compose topology the live Manager and farm were created from.
case "$MODE" in
  cutover)
    mkdir -p "$PRE_DIR"
    git -C "$REPO" show "$PREVIOUS_REF:server/docker-compose.yml" > "$PRE_COMPOSE.tmp"
    mv -f "$PRE_COMPOSE.tmp" "$PRE_COMPOSE"
    LIVE_TOPO=pre ;;
  proxy-restore) LIVE_TOPO=direct ;;
  *) LIVE_TOPO=proxy ;;
esac

old_m_digest="${OLD_MANAGER##*@}"
old_f_digest="${OLD_FARM##*@}"
for service in juicemount-manager juicefarm; do
  id="$OLD_MANAGER_ID"
  [ "$service" = juicefarm ] && id="$OLD_FARM_ID"
  expected="$(hash_for "$old_m_digest" "$old_f_digest" "$OLD_RENDER_DIGEST" "$service" "$LIVE_TOPO")"
  [[ "$expected" =~ ^[0-9a-f]{64}$ ]] && [ "$(label "$id" com.docker.compose.config-hash)" = "$expected" ] || {
    log "$service live config hash differs from canonical Compose with previous digests"; exit 1;
  }
done

if [ "$MODE" = release ] || [ "$MODE" = cutover ]; then
  with_digests "$MANAGER_DIGEST" "$FARM_DIGEST" "$RENDER_DIGEST" config --quiet
  # `config` resolves secrets from .env. Compare without printing or persisting
  # the credential; this release rollout must not rotate it as a side effect.
  current_key="$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$OLD_MANAGER_ID" | sed -n 's/^JM_ADMIN_KEY=//p')"
  new_key="$(with_digests "$MANAGER_DIGEST" "$FARM_DIGEST" "$RENDER_DIGEST" config --format json | jq -er '.services["juicemount-manager"].environment.JM_ADMIN_KEY')"
  [ "${#current_key}" -ge 32 ] && [ "$current_key" = "$new_key" ] || { log 'Manager credential is missing, short, or would change'; exit 1; }
  unset current_key new_key
fi

env_has_digest() { # key, expected digest: reject duplicates, quoting, and ambiguous syntax
  local key="$1" expected="$2" assignments
  assignments="$(awk -v key="$key" '
    $0 ~ "^[[:space:]]*(export[[:space:]]+)?" key "[[:space:]]*=" { print }
  ' "$ENV_FILE")"
  [ "$assignments" = "$key=$expected" ]
}
env_has_digest JM_MANAGER_DIGEST "$old_m_digest" &&
  env_has_digest JM_FARM_DIGEST "$old_f_digest" &&
  env_has_digest JM_FARM_RENDER_DIGEST "$OLD_RENDER_DIGEST" || {
    log 'private .env digests must exactly match live images, one plain assignment each'; exit 1;
  }

persisted_config_matches() {
  compose config --format json | jq -e \
    --arg manager "$NEW_MANAGER" --arg farm "$NEW_FARM" --arg render "$NEW_RENDER" \
    '.services["juicemount-manager"].image == $manager and
     .services.juicefarm.image == $farm and
     .services["juicemount-manager"].environment.JM_FARM_RENDER_IMAGE == $render' >/dev/null
}
stage_env() {
  cmp -s "$ENV_FILE" "$LOCK/env.backup" || return 1
  ENV_TEMP="$(mktemp "$ENV_FILE.rollout.XXXXXX")" || return 1
  if ! chmod 600 "$ENV_TEMP"; then return 1; fi
  if ! awk -v manager="$MANAGER_DIGEST" -v farm="$FARM_DIGEST" -v render="$RENDER_DIGEST" '
    /^JM_MANAGER_DIGEST=/ { print "JM_MANAGER_DIGEST=" manager; next }
    /^JM_FARM_DIGEST=/ { print "JM_FARM_DIGEST=" farm; next }
    /^JM_FARM_RENDER_DIGEST=/ { print "JM_FARM_RENDER_DIGEST=" render; next }
    { print }
  ' "$LOCK/env.backup" > "$ENV_TEMP"; then
    return 1
  fi
  # Recheck after preparing the replacement: an operator edit during the image
  # pulls or render must not be silently overwritten by this rollout.
  cmp -s "$ENV_FILE" "$LOCK/env.backup" || return 1
  ENV_STAGED=1
  if ! mv -f "$ENV_TEMP" "$ENV_FILE"; then return 1; fi
  ENV_TEMP=""
  env_has_digest JM_MANAGER_DIGEST "$MANAGER_DIGEST" &&
    env_has_digest JM_FARM_DIGEST "$FARM_DIGEST" &&
    env_has_digest JM_FARM_RENDER_DIGEST "$RENDER_DIGEST" &&
    persisted_config_matches
}

protected_unchanged() {
  local service previous now
  while read -r service previous; do
    now="$(service_id "$service")" || return 1
    [ "$now" = "$previous" ] || return 1
    if [ "$service" = juicefs-init ]; then
      require_init "$now" || return 1
    else
      require_service "$service" "$now" || return 1
      [ "$(docker inspect -f '{{.State.Health.Status}}' "$now")" = healthy ] || return 1
    fi
  done < "$LOCK/protected-ids"
}
verify_running() { # expected image refs and embedded identities
  local manager_ref="$1" farm_ref="$2" manager_identity="$3" farm_identity="$4" manager_id farm_id code identity
  protected_unchanged || { log 'CRITICAL: protected Compose container identity changed'; return 1; }
  manager_id="$(service_id juicemount-manager)" || return 1
  farm_id="$(service_id juicefarm)" || return 1
  require_service juicemount-manager "$manager_id" && require_service juicefarm "$farm_id" || return 1
  [ "$(docker inspect -f '{{.Config.Image}}' "$manager_id")" = "$manager_ref" ] || return 1
  [ "$(docker inspect -f '{{.Config.Image}}' "$farm_id")" = "$farm_ref" ] || return 1
  identity="$(docker exec "$farm_id" /usr/local/bin/jmfarm --build-info)" || return 1
  [ "$identity" = "$farm_identity" ] || return 1
  identity="$(docker exec "$manager_id" /usr/local/bin/juicemount-manager --build-info)" || return 1
  [ "$identity" = "$manager_identity" ] || return 1
  for _ in $(seq 1 12); do
    code="$(docker exec "$manager_id" sh -ec '
      [ "${#JM_ADMIN_KEY}" -ge 32 ] || exit 22
      printf '\''header = "X-JuiceMount-Admin-Key: %s"\n'\'' "$JM_ADMIN_KEY" |
        curl --config - -fsS -o /dev/null -w '\''%{http_code}'\'' http://127.0.0.1:8080/api/farm
    ' 2>/dev/null)" || code=""
    [ "$code" = 200 ] && return 0
    sleep 5
  done
  return 1
}

# ---- H-8B topology helpers -------------------------------------------------
wait_healthy() { # service
  local id i
  for i in $(seq 1 24); do
    if id="$(service_id "$1")" && [ "$(docker inspect -f '{{.State.Running}}' "$id")" = true ] &&
      [ "$(docker inspect -f '{{.State.Health.Status}}' "$id")" = healthy ]; then
      return 0
    fi
    sleep 5
  done
  log "$1 did not become healthy"
  return 1
}
wait_init() {
  local id i
  for i in $(seq 1 24); do
    if id="$(service_id juicefs-init)" && [ "$(docker inspect -f '{{.State.Status}}' "$id")" = exited ]; then
      [ "$(docker inspect -f '{{.State.ExitCode}}' "$id")" = 0 ] && return 0
      log 'juicefs-init exited non-zero (see its log: credentials or proxy reachability)'
      return 1
    fi
    sleep 5
  done
  log 'juicefs-init did not finish'
  return 1
}
recreate_topo() { # topology, manager, farm, render digests, services...
  local topo="$1" m="$2" f="$3" r="$4"
  shift 4
  with_digests_topo "$topo" "$m" "$f" "$r" up -d --no-deps --no-build --pull never --force-recreate "$@"
}
# apply_topology moves the stack to one topology. The order is the port
# handover: exactly one service holds host port 30179 at any time.
apply_topology() { # topology, manager, farm, render digests
  local topo="$1" m="$2" f="$3" r="$4" id
  # Quiesce: stop every server-side volume client first, so nothing writes
  # while Redis restarts and its SIGTERM RDB save (120 s grace) completes.
  log 'stage: quiesce server volume clients (farm, Manager, juicefs)'
  compose_topo "$topo" stop -t 60 juicefarm juicemount-manager juicefs || return 1
  if [ "$topo" = proxy ]; then
    log 'stage: redis without a host port, ACL default off'
    recreate_topo proxy "$m" "$f" "$r" redis || return 1
    wait_healthy redis || return 1
    id="$(service_id redis)" && ! publishes_30179 "$id" || { log 'redis still publishes 30179'; return 1; }
    # The stored mode survives every rollback. Entering the proxy topology
    # always starts in passthrough: resuming a stale observe/enforce over
    # sessions the new proxy does not own would refuse every client.
    log 'stage: force lease mode passthrough (jmlease:v1:cfg)'
    force_passthrough "$id" || { log 'could not reset jmlease:v1:cfg mode to passthrough'; return 1; }
    log 'stage: respproxy takes host port 30179'
    recreate_topo proxy "$m" "$f" "$r" respproxy || return 1
    wait_healthy respproxy || return 1
    id="$(service_id respproxy)" && publishes_30179 "$id" || { log 'respproxy does not publish 30179'; return 1; }
  else
    log 'stage: remove respproxy (frees host port 30179)'
    compose_topo proxy rm -sf respproxy || return 1
    log "stage: redis publishes 30179 ($topo topology)"
    recreate_topo "$topo" "$m" "$f" "$r" redis || return 1
    wait_healthy redis || return 1
    id="$(service_id redis)" && publishes_30179 "$id" || { log 'redis does not publish 30179'; return 1; }
  fi
  log 'stage: juicefs-init (volume reachable with the admin principal)'
  recreate_topo "$topo" "$m" "$f" "$r" juicefs-init || return 1
  wait_init || return 1
  log 'stage: juicefs (bg + webdav)'
  recreate_topo "$topo" "$m" "$f" "$r" juicefs || return 1
  wait_healthy juicefs || return 1
  log 'stage: Manager and farm'
  recreate_topo "$topo" "$m" "$f" "$r" juicemount-manager juicefarm || return 1
}
force_passthrough() { # redis container id: HSET through the loopback-only jm-admin
  local out
  out="$(docker exec "$1" sh -ec 'REDISCLI_AUTH="$(cat /run/jm-redis/admin.pass)" redis-cli --no-auth-warning --user jm-admin -n 1 HSET jmlease:v1:cfg mode passthrough')" || return 1
  [[ "$out" =~ ^[01]$ ]]
}
# The respproxy image digest from .env (exactly one assignment).
respproxy_digest() {
  local d
  d="$(awk '/^JM_RESPPROXY_DIGEST=/ { n++; v=substr($0, 21) } END { if (n == 1) print v }' "$ENV_FILE")"
  [[ "$d" =~ ^sha256:[0-9a-f]{64}$ ]] || return 1
  printf '%s' "$d"
}
# require_proxy_gate refuses a proxy image whose source predates the
# every-mode command/key-class gate: before it, passthrough and observe let
# any principal FLUSHDB or rewrite jmlease:v1:cfg. The image's revision
# label must descend from the commit in server/respproxy/min-gate-commit.
require_proxy_gate() { # image ref (present locally)
  local min rev
  min="$(grep -Ev '^[[:space:]]*(#|$)' "$GATE_FILE" 2>/dev/null | head -1 | tr -d '[:space:]')"
  [[ "$min" =~ ^[0-9a-f]{40}$ ]] || { log "server/respproxy/min-gate-commit must name one full commit"; return 1; }
  rev="$(docker image inspect -f '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "$1")" || return 1
  [[ "$rev" =~ ^[0-9a-f]{40}$ ]] || { log "respproxy image has no org.opencontainers.image.revision label"; return 1; }
  git -C "$REPO" cat-file -e "$min^{commit}" 2>/dev/null || { log "required proxy commit $min is not in this checkout"; return 1; }
  git -C "$REPO" cat-file -e "$rev^{commit}" 2>/dev/null || { log "respproxy image revision $rev is not in this checkout"; return 1; }
  git -C "$REPO" merge-base --is-ancestor "$min" "$rev" || {
    log "respproxy image revision $rev predates the every-mode gate $min; refusing"; return 1;
  }
  log "respproxy image revision $rev contains the every-mode gate $min"
}
sha256_hex() {
  if command -v sha256sum >/dev/null 2>&1; then printf '%s' "$1" | sha256sum | cut -d' ' -f1
  else printf '%s' "$1" | shasum -a 256 | cut -d' ' -f1; fi
}
env_secret() { # key: print the single plain assignment's value, refuse unsafe ones
  local key="$1" assignments value
  assignments="$(awk -v key="$key" '$0 ~ "^[[:space:]]*(export[[:space:]]+)?" key "[[:space:]]*=" { n++ } END { print n+0 }' "$ENV_FILE")"
  [ "$assignments" = 1 ] || return 1
  value="$(sed -n "s/^$key=//p" "$ENV_FILE")"
  [[ "$value" =~ ^[A-Za-z0-9._~-]{32,}$ ]] || return 1
  printf '%s' "$value"
}
require_proxy_env() {
  local key spec
  for key in JM_REDIS_PROXY_PASSWORD; do env_secret "$key" >/dev/null || { log "server/.env needs one URL-safe 32+ character $key"; return 1; }; done
  for spec in $SERVER_PRINCIPALS; do
    key="JM_PROXY_${spec#*:}_PASSWORD"
    env_secret "$key" >/dev/null || { log "server/.env needs one URL-safe 32+ character $key"; return 1; }
  done
}
# The rollback ACL: one plain Redis user per proxy principal, same name and
# secret, as sha256 (plan section 3). Server principals come from .env, Macs
# from the Manager's rollback-macs.acl fragment.
build_rollback_acl() {
  local spec name key secret line macs
  for spec in $SERVER_PRINCIPALS; do
    name="${spec%%:*}"; key="JM_PROXY_${spec#*:}_PASSWORD"
    secret="$(env_secret "$key")" || return 1
    printf 'user %s on #%s ~* &* +@all\n' "$name" "$(sha256_hex "$secret")"
  done
  # Strict: an unreadable fragment must not silently drop every enrolled
  # Mac from the rollback ACL (they would all be locked out).
  macs="$(docker exec "$OLD_MANAGER_ID" cat "$MANAGER_PROXY_DIR/rollback-macs.acl")" || {
    log "cannot read $MANAGER_PROXY_DIR/rollback-macs.acl from the Manager"; return 1;
  }
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    [[ "$line" == \#* ]] && continue
    [[ "$line" =~ ^user\ mac-[a-z0-9-]+\ on\ \#[0-9a-f]{64}\ ~\*\ \&\*\ \+@all$ ]] || {
      log 'Manager rollback-macs.acl has a line that is not a sha256-authenticated Mac user'; return 1;
    }
    printf '%s\n' "$line"
  done <<< "$macs"
}

verify_running "$OLD_MANAGER" "$OLD_FARM" "$OLD_MANAGER_IDENTITY" "$OLD_FARM_IDENTITY" || {
  log 'previous services are not healthy; refusing an update without a working rollback target'; exit 1;
}

case "$MODE" in
  release|cutover)
    if [ "$MODE" = cutover ]; then
      require_proxy_env || exit 1
      rp_digest="$(respproxy_digest)" || { log 'server/.env needs exactly one JM_RESPPROXY_DIGEST=sha256:... assignment'; exit 1; }
      log 'pulling the immutable respproxy image'
      docker pull "$RESPPROXY_REPO@$rp_digest" >/dev/null
      require_proxy_gate "$RESPPROXY_REPO@$rp_digest" || exit 1
    fi
    log 'pulling immutable Manager, farm, and GPU enrollment images before service mutation'
    docker pull "$NEW_MANAGER" >/dev/null
    docker pull "$NEW_FARM" >/dev/null
    docker pull "$NEW_RENDER" >/dev/null
    for spec in "$NEW_MANAGER|juicemount-manager" "$NEW_FARM|jmfarm" "$NEW_RENDER|jmfarm"; do
      image="${spec%%|*}"; binary="${spec#*|}"
      actual="$(docker run --rm --entrypoint "/usr/local/bin/$binary" "$image" --build-info)"
      [ "$actual" = "$binary $SOURCE_VERSION ($SOURCE_COMMIT)" ] || { log "$image embedded source identity mismatch"; exit 1; }
    done

    log 'atomically persisting release digests in private server/.env'
    stage_env || { log 'persistent digest update failed; restoring the previous .env'; exit 1; }
    ROLLBACK_ARMED=1
    if [ "$MODE" = release ]; then
      log 'recreating only Manager and farm (no dependencies, no build, no pull)'
      with_digests "$MANAGER_DIGEST" "$FARM_DIGEST" "$RENDER_DIGEST" \
        up -d --no-deps --no-build --pull never --force-recreate juicemount-manager juicefarm
    else
      FALLBACK_TOPO=pre
      log 'H-8B cutover: moving host port 30179 from Redis to respproxy (passthrough)'
      apply_topology proxy "$MANAGER_DIGEST" "$FARM_DIGEST" "$RENDER_DIGEST" || { log 'cutover stage failed'; exit 1; }
    fi
    verify_running "$NEW_MANAGER" "$NEW_FARM" "juicemount-manager $SOURCE_VERSION ($SOURCE_COMMIT)" "jmfarm $SOURCE_VERSION ($SOURCE_COMMIT)" || {
      log 'new service verification failed'; exit 1;
    }
    persisted_config_matches || { log 'persisted Compose configuration changed during rollout'; exit 1; }
    if [ "$MODE" = cutover ]; then
      log "complete: respproxy owns 30179 in passthrough; source $SOURCE_VERSION ($SOURCE_COMMIT)"
      log "pre-proxy compose kept at $PRE_COMPOSE"
    else
      log "complete: source $SOURCE_VERSION ($SOURCE_COMMIT); protected containers healthy and unchanged"
    fi ;;

  proxy-rollback|proxy-restore)
    env_has_digest JM_MANAGER_DIGEST "$old_m_digest" &&
      env_has_digest JM_FARM_DIGEST "$old_f_digest" &&
      env_has_digest JM_FARM_RENDER_DIGEST "$OLD_RENDER_DIGEST" || {
        log 'private .env digests must exactly match live images, one plain assignment each'; exit 1;
      }
    require_proxy_env || exit 1
    if [ "$MODE" = proxy-restore ]; then
      rp_digest="$(respproxy_digest)" || { log 'server/.env needs exactly one JM_RESPPROXY_DIGEST=sha256:... assignment'; exit 1; }
      docker image inspect "$RESPPROXY_REPO@$rp_digest" >/dev/null 2>&1 || docker pull "$RESPPROXY_REPO@$rp_digest" >/dev/null
      require_proxy_gate "$RESPPROXY_REPO@$rp_digest" || exit 1
    fi
    if [ "$MODE" = proxy-rollback ]; then
      acl="$(build_rollback_acl)" || { log 'refusing: the rollback ACL could not be built safely'; exit 1; }
      log "writing the rollback ACL ($(printf '%s\n' "$acl" | grep -c '^user ') users, sha256 only) to the Redis data directory"
      printf '%s\n' "$acl" | docker exec -i "$REDIS_ID" sh -ec \
        'umask 077; cat > /data/jm-rollback-users.acl.tmp; mv -f /data/jm-rollback-users.acl.tmp /data/jm-rollback-users.acl'
      unset acl
      FALLBACK_TOPO=proxy
      TARGET_TOPO=direct
    else
      FALLBACK_TOPO=direct
      TARGET_TOPO=proxy
    fi
    ROLLBACK_ARMED=1
    apply_topology "$TARGET_TOPO" "$old_m_digest" "$old_f_digest" "$OLD_RENDER_DIGEST" || { log "$MODE stage failed"; exit 1; }
    verify_running "$OLD_MANAGER" "$OLD_FARM" "$OLD_MANAGER_IDENTITY" "$OLD_FARM_IDENTITY" || {
      log 'service verification failed'; exit 1;
    }
    if [ "$MODE" = proxy-rollback ]; then
      log 'complete: respproxy removed; Redis publishes 30179 with the mirrored principal ACL. NO lease enforcement.'
    else
      log 'complete: respproxy owns 30179 again'
    fi ;;
esac
