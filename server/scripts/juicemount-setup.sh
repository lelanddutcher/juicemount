#!/usr/bin/env bash
# juicemount-setup: install a JuiceMount server with one command.
#
#   curl -fsSL https://raw.githubusercontent.com/lelanddutcher/juicemount/main/server/scripts/juicemount-setup.sh | sudo bash
#
# Runs in the TrueNAS SCALE shell or on any Linux host with Docker Compose v2.
# Needs only bash, coreutils, curl and docker. The person running it types no
# secret, digest or path:
#
#   1. Finds the storage. TrueNAS (midclt present) or --pool: a dataset
#      <pool>/juicemount with one child dataset per data directory, on the pool
#      with the most free space. Otherwise a directory, /srv/juicemount.
#   2. Reads the image digests of the latest release from its published
#      server-images.env and checks each one against release-manifest.json.
#   3. Takes server/docker-compose.yml and the files it mounts from this
#      repository (branch main, or --source DIR), with the host paths replaced
#      by the chosen data directory. Every other image (Redis, the object
#      store, JuiceFS) comes from that compose file, or from an --override
#      compose file; this script names no image.
#   4. Shows the plan and asks once. Then it generates every secret, writes
#      <data>/compose/.env (mode 0600, directory 0700), validates the compose
#      (its ${VAR:?} guards, digest-pinned images, existing bind sources),
#      pulls, starts the stack, and waits until the Manager answers.
#   5. Prints two lines on stdout: the Manager URL and the Manager sign-in key.
#
# Re-running is safe. An existing <data>/compose/.env is never rewritten, so
# no secret is ever regenerated over an install. A re-run only checks the
# configuration, starts what is stopped (`docker compose up -d`) and prints the
# same two lines. Data directories that already hold data without that .env
# are refused: new secrets would not open them. Nothing is ever deleted.
#
# Options (all optional):
#   --yes             do not ask for confirmation (needed without a terminal)
#   --pool NAME       ZFS: use this pool (creates <pool>/juicemount datasets)
#   --dir PATH        use this directory for all data (no datasets created)
#   --source DIR      take docker-compose.yml and its files from DIR (a
#                     checkout's server/ directory) instead of GitHub main
#   --release TAG     read image digests from release TAG (default: latest)
#   --images-env FILE read image digests from FILE (server-images.env format)
#   --override FILE   extra compose file applied on top (e.g. another object
#                     store image); kept as compose/docker-compose.override.yml
#   --import PATH     existing folder the Manager may copy from, read-only
#                     (repeatable; default: <data>/import)
#   --project NAME    Compose project name (default juicemount)
#   --host ADDR       address printed in the Manager URL (default: LAN IP)
#   --dry-run         show the plan and exit; change nothing
#
# Exit codes: 0 running; 1 refused or failed (the message says why); 2 bad
# usage. juicefs-init failures name its exit code (see INSTALL-TrueNAS.md).
set -euo pipefail

REPO_SLUG="lelanddutcher/juicemount"
RAW_BASE="https://raw.githubusercontent.com/$REPO_SLUG/main/server"
RELEASES="https://github.com/$REPO_SLUG/releases"
DEFAULT_DIR="/srv/juicemount"
# The host paths the canonical compose ships with, replaced at render time.
COMPOSE_DATA_PREFIX="/mnt/zSSD/juicemount/"
# Files the compose mounts relative to its own directory.
SUPPORT_FILES=(
  docker-compose.yml redis.conf redis.acl
  minio/provision-object-keys.sh minio/keyd.sh
  minio/jm-client-policy.json minio/jm-gc-policy.json minio/jm-keyadmin-policy.json
)
# name:random bytes. Hex encoding doubles the length. JM_ADMIN_KEY is the
# shortest the Manager accepts (32); MinIO secrets are at most 40; proxy
# passwords are URL-safe and at least 32.
SECRETS=(
  JM_ADMIN_KEY:16 MINIO_ROOT_PASSWORD:20
  JM_REDIS_PROXY_PASSWORD:32 JM_PROXY_FARM_PASSWORD:32 JM_PROXY_MANAGER_PASSWORD:32
  JM_PROXY_BG_PASSWORD:32 JM_PROXY_WEBDAV_PASSWORD:32 JM_PROXY_ADMIN_PASSWORD:32
  JM_OBJ_CLIENT_SECRET_KEY:20 JM_OBJ_GC_SECRET_KEY:20
)
DIGESTS=(JM_MANAGER_DIGEST JM_FARM_DIGEST JM_FARM_RENDER_DIGEST JM_RESPPROXY_DIGEST)
OTHER_KEYS=(JM_FARM_STORAGE_CAPACITY_BYTES JM_FARM_NODE_META_URL)
SAFE_PATH_RE='^/[A-Za-z0-9._/-]*$'

say() { printf '%s\n' "$*" >&2; }
die() { printf 'juicemount-setup: %s\n' "$*" >&2; exit 1; }
usage() { sed -n '/^# Options/,/^# Exit codes/p' "${BASH_SOURCE[0]}" 2>/dev/null | sed '$d; s/^# \{0,1\}//' >&2 || true; exit 2; }

YES=0 DRY=0 POOL="" DIR="" SOURCE="" RELEASE="latest" IMAGES_ENV="" OVERRIDE="" PROJECT="juicemount" HOST=""
IMPORTS=()
STAGE="" LOCK=""

cleanup() {
  [ -n "$STAGE" ] && rm -rf "$STAGE"
  [ -n "$LOCK" ] && rmdir "$LOCK" 2>/dev/null
  return 0
}

parse_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --yes|-y) YES=1; shift ;;
      --dry-run) DRY=1; shift ;;
      --pool|--dir|--source|--release|--images-env|--override|--import|--project|--host)
        [ $# -ge 2 ] && [ -n "$2" ] || { say "$1 needs a value"; usage; }
        case "$1" in
          --pool) POOL="$2" ;; --dir) DIR="$2" ;; --source) SOURCE="$2" ;;
          --release) RELEASE="$2" ;; --images-env) IMAGES_ENV="$2" ;;
          --override) OVERRIDE="$2" ;; --import) IMPORTS+=("$2") ;;
          --project) PROJECT="$2" ;; --host) HOST="$2" ;;
        esac
        shift 2 ;;
      -h|--help) usage ;;
      *) say "unknown option: $1"; usage ;;
    esac
  done
  [[ "$PROJECT" =~ ^[a-z][a-z0-9_-]*$ ]] || die "--project must be lowercase letters, digits, - or _"
  [[ "$RELEASE" == latest || "$RELEASE" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][A-Za-z0-9.]+)?$ ]] || die "--release must be latest or a tag like v0.5.0"
  [ -z "$POOL" ] || [[ "$POOL" =~ ^[A-Za-z0-9][A-Za-z0-9_.:-]*$ ]] || die "--pool is not a pool name"
  [ -z "$POOL" ] || [ -z "$DIR" ] || die "use --pool or --dir, not both"
  [ -z "$HOST" ] || [[ "$HOST" =~ ^[A-Za-z0-9.:-]+$ ]] || die "--host must be a host name or address"
  if [ -n "$DIR" ]; then
    DIR="$(abspath "$DIR")"
    [[ "$DIR" =~ $SAFE_PATH_RE ]] || die "--dir may contain only letters, digits and . _ - /"
    [ "$DIR" != / ] || die "--dir must not be /"
  fi
  if [ -n "$SOURCE" ]; then
    SOURCE="$(abspath "$SOURCE")"
    [ -f "$SOURCE/docker-compose.yml" ] || die "--source $SOURCE has no docker-compose.yml"
  fi
  [ -z "$IMAGES_ENV" ] || { [ -f "$IMAGES_ENV" ] || die "--images-env $IMAGES_ENV is not a file"; IMAGES_ENV="$(abspath "$IMAGES_ENV")"; }
  [ -z "$OVERRIDE" ] || { [ -f "$OVERRIDE" ] || die "--override $OVERRIDE is not a file"; OVERRIDE="$(abspath "$OVERRIDE")"; }
  return 0
}

abspath() {
  case "$1" in
    /*) printf '%s' "${1%/}" ;;
    *) printf '%s/%s' "$(pwd)" "${1%/}" ;;
  esac
}

need() { command -v "$1" >/dev/null 2>&1 || die "$2"; }

preflight() {
  need docker "Docker is not installed. On TrueNAS, open Apps and choose a pool for apps first."
  need curl "curl is not installed."
  need od "od (coreutils) is not installed."
  docker info >/dev/null 2>&1 || die "Docker is not running, or this user may not use it. Run with sudo. On TrueNAS, open Apps and choose a pool for apps first."
  docker compose version >/dev/null 2>&1 || die "Docker Compose v2 (docker compose) is not installed."
  [ -r /dev/urandom ] || die "/dev/urandom is not readable."
}

fetch() { # url dest
  curl -fsSL --proto '=https' --tlsv1.2 --retry 3 -o "$2" "$1" || die "could not download $1"
}

rand_hex() {
  local out
  out="$(od -An -vtx1 -N"$1" /dev/urandom | tr -d ' \n')"
  [[ "$out" =~ ^[0-9a-f]+$ ]] && [ ${#out} -eq $(($1 * 2)) ] || die "could not read random bytes"
  printf '%s' "$out"
}

env_get() { # file key -> last plain assignment
  sed -n "s/^$2=//p" "$1" | tail -n 1
}

# ---- where the install lives ------------------------------------------------

is_truenas() { command -v midclt >/dev/null 2>&1; }

zfs_mountpoint() { zfs get -H -o value mountpoint "$1" 2>/dev/null; }
zfs_exists() { zfs list -H -o name "$1" >/dev/null 2>&1; }

pick_pool() {
  # The pool with the most free space, never the boot pool.
  zpool list -Hp -o name,free 2>/dev/null |
    awk '$1 != "boot-pool" && $1 != "freenas-boot" { if ($2 + 0 > best) { best = $2 + 0; name = $1 } } END { if (name != "") print name }'
}

# Sets MODE (zfs|dir), ROOT, ROOT_DS, POOL.
locate_install() {
  local wd id
  ROOT_DS=""
  # An install of this project already known to Docker wins over defaults.
  if [ -z "$DIR" ] && [ -z "$POOL" ]; then
    id="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT" | sed -n 1p)"
    if [ -n "$id" ]; then
      wd="$(docker inspect -f '{{index .Config.Labels "com.docker.compose.project.working_dir"}}' "$id")"
      case "$wd" in
        */compose) [ -f "$wd/.env" ] && DIR="${wd%/compose}" ;;
      esac
      [ -n "$DIR" ] || die "Compose project '$PROJECT' already exists ($wd) and was not made by juicemount-setup. Use --project with another name, or install it by hand (server/INSTALL-TrueNAS.md)."
    fi
  fi
  if [ -n "$DIR" ]; then
    MODE=dir ROOT="$DIR"
    return
  fi
  if [ -n "$POOL" ] || is_truenas; then
    need zpool "zpool is not installed; use --dir instead of --pool."
    need zfs "zfs is not installed; use --dir instead of --pool."
    if [ -z "$POOL" ]; then
      # An earlier install on any pool.
      local p mp
      while read -r p; do
        case "$p" in boot-pool|freenas-boot) continue ;; esac
        mp="$(zfs_mountpoint "$p/juicemount")" || continue
        [ -f "$mp/compose/.env" ] && { POOL="$p"; break; }
      done < <(zpool list -H -o name 2>/dev/null)
    fi
    [ -n "$POOL" ] || POOL="$(pick_pool)"
    [ -n "$POOL" ] || die "No ZFS storage pool found. Create one in Storage first, or pass --dir."
    zpool list -H -o name "$POOL" >/dev/null 2>&1 || die "ZFS pool $POOL does not exist."
    MODE=zfs ROOT_DS="$POOL/juicemount"
    if zfs_exists "$ROOT_DS"; then
      ROOT="$(zfs_mountpoint "$ROOT_DS")"
    else
      ROOT="$(zfs_mountpoint "$POOL")/juicemount"
    fi
    [[ "$ROOT" =~ $SAFE_PATH_RE ]] && [ "$ROOT" != /juicemount ] || die "Pool $POOL has no usable mountpoint ($ROOT)."
    return
  fi
  MODE=dir ROOT="$DEFAULT_DIR"
}

# ---- files ------------------------------------------------------------------

# Copies or downloads the compose and the files it mounts into $STAGE/src.
stage_compose() {
  local f
  mkdir -p "$STAGE/src/minio"
  for f in "${SUPPORT_FILES[@]}"; do
    if [ -n "$SOURCE" ]; then
      [ -f "$SOURCE/$f" ] || die "--source $SOURCE lacks $f"
      cp "$SOURCE/$f" "$STAGE/src/$f"
    else
      fetch "$RAW_BASE/$f" "$STAGE/src/$f"
    fi
  done
  [ -z "$OVERRIDE" ] || cp "$OVERRIDE" "$STAGE/src/docker-compose.override.yml"
  grep -Eq "^[[:space:]]*[^#[:space:]][^#]*$COMPOSE_DATA_PREFIX" "$STAGE/src/docker-compose.yml" ||
    die "docker-compose.yml no longer uses $COMPOSE_DATA_PREFIX host paths; this setup script does not know its layout."
}

# Data directory names (first path element below the data root) the compose
# mounts, from its non-comment lines.
data_subdirs() { # compose file, prefix
  grep -v '^[[:space:]]*#' "$1" | grep -o "$2[A-Za-z0-9._-]*" | sed "s|^$2||" | sort -u
}

# Reads the four first-party digests into DIG_<name> from the release.
stage_digests() {
  local envf="$STAGE/server-images.env" man="$STAGE/release-manifest.json" k v base
  if [ -n "$IMAGES_ENV" ]; then
    cp "$IMAGES_ENV" "$envf"
    RELEASE_NAME="local:$(basename "$IMAGES_ENV")"
  else
    if [ "$RELEASE" = latest ]; then base="$RELEASES/latest/download"; else base="$RELEASES/download/$RELEASE"; fi
    fetch "$base/server-images.env" "$envf"
    fetch "$base/release-manifest.json" "$man"
    RELEASE_NAME="$(sed -n 's/^[[:space:]]*"version":[[:space:]]*"\([^"]*\)".*/\1/p' "$man" | sed -n 1p)"
    [ -n "$RELEASE_NAME" ] || die "release-manifest.json has no version."
  fi
  # Only KEY=sha256:<64 hex> lines and comments are accepted.
  if grep -qvE '^(#.*|[[:space:]]*|[A-Z_]+=sha256:[0-9a-f]{64})$' "$envf"; then
    die "server-images.env has a line that is not KEY=sha256:<64 hex>."
  fi
  for k in "${DIGESTS[@]}"; do
    v="$(env_get "$envf" "$k")"
    [[ "$v" =~ ^sha256:[0-9a-f]{64}$ ]] || die "the release has no digest for $k."
    if [ -f "$man" ]; then
      grep -qF "\"digest\": \"$v\"" "$man" || die "$k in server-images.env does not match release-manifest.json."
    fi
    printf -v "DIG_$k" '%s' "$v"
  done
}

render_compose() { # staged compose -> installed compose, paths replaced
  local src="$1" dst="$2" imports="$3" n
  n="$(grep -c -E '^[[:space:]]*- [^#[:space:]]+:/sources/[^[:space:]]*:ro' "$src" || true)"
  [ "$n" = 1 ] || die "docker-compose.yml has $n /sources mounts; expected exactly one to replace."
  awk -v imports="$imports" '
    /^[[:space:]]*- [^#[:space:]]+:\/sources\/[^[:space:]]*:ro/ {
      match($0, /^[[:space:]]*/); ind = substr($0, 1, RLENGTH)
      n = split(imports, lines, "\n")
      for (i = 1; i <= n; i++) if (lines[i] != "") print ind "- " lines[i] "    # juicemount-setup --import"
      next
    }
    { print }' "$src" | sed "s|$COMPOSE_DATA_PREFIX|$ROOT/|g" > "$dst"
}

import_lines() {
  local p name used=" " i out=""
  if [ ${#IMPORTS[@]} -eq 0 ]; then
    printf '%s:/sources/import:ro\n' "$ROOT/import"
    return
  fi
  for p in "${IMPORTS[@]}"; do
    p="$(abspath "$p")"
    [ -d "$p" ] || die "--import $p is not a directory."
    [[ "$p" =~ $SAFE_PATH_RE ]] || die "--import $p may contain only letters, digits and . _ - /"
    case "$p/" in "$ROOT"/*) die "--import $p is inside the JuiceMount data directory." ;; esac
    name="$(basename "$p")"; i=1
    while [[ "$used" == *" $name "* ]]; do i=$((i + 1)); name="$(basename "$p")-$i"; done
    used="$used$name "
    out="$out$p:/sources/$name:ro"$'\n'
  done
  printf '%s' "$out"
}

has_content() { [ -d "$1" ] && [ -n "$(ls -A "$1" 2>/dev/null)" ]; }

storage_capacity() {
  if [ "$MODE" = zfs ]; then
    zpool list -Hp -o size "$POOL"
  else
    df -Pk "$ROOT" | awk 'NR == 2 { printf "%.0f\n", $2 * 1024 }'
  fi
}

detect_host() {
  local a=""
  if command -v ip >/dev/null 2>&1; then
    a="$(ip -4 route get 1.1.1.1 2>/dev/null | sed -n 's/.* src \([0-9.]*\).*/\1/p' | sed -n 1p)"
  fi
  [ -n "$a" ] || a="$(hostname -I 2>/dev/null | awk '{print $1}')"
  printf '%s' "${a:-127.0.0.1}"
}

write_env() { # path
  local tmp spec k n v
  tmp="$CDIR/.env.new.$$"
  (
    umask 077
    {
      printf '# JuiceMount server settings, written by juicemount-setup on %s.\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      printf '# Every secret of this server is in this file. Keep it private (mode 0600)\n'
      printf '# and back it up with the data. Re-running juicemount-setup never changes it.\n'
      printf '# Template and meaning of each value: server/release.env.example.\n'
      printf 'JM_SETUP_RELEASE=%s\n' "$RELEASE_NAME"
      for k in "${DIGESTS[@]}"; do v="DIG_$k"; printf '%s=%s\n' "$k" "${!v}"; done
      for spec in "${SECRETS[@]}"; do
        k="${spec%%:*}" n="${spec##*:}"
        printf '%s=%s\n' "$k" "$(rand_hex "$n")"
      done
      printf 'JM_FARM_STORAGE_CAPACITY_BYTES=%s\n' "$CAPACITY"
      printf 'JM_FARM_NODE_META_URL=redis://farm@%s:30179/1\n' "$HOST"
    } > "$tmp"
  )
  # ln refuses an existing target: an install's .env is never replaced.
  ln "$tmp" "$1" 2>/dev/null || { rm -f "$tmp"; die "$1 appeared while writing it; nothing was changed."; }
  rm -f "$tmp"
  chmod 600 "$1"
}

check_env() { # path: every key present and plausible
  local spec k v min
  for spec in "${SECRETS[@]}"; do
    k="${spec%%:*}"; v="$(env_get "$1" "$k")"
    min=32; [ "$k" != MINIO_ROOT_PASSWORD ] || min=8
    [ ${#v} -ge "$min" ] || die "$1 lacks a usable $k. Restore it from your backup; juicemount-setup never generates a secret for an existing install."
    case "$v" in CHANGE*|REPLACE*|replace*) die "$1 still has a placeholder for $k." ;; esac
  done
  for k in "${DIGESTS[@]}"; do
    [[ "$(env_get "$1" "$k")" =~ ^sha256:[0-9a-f]{64}$ ]] || die "$1 lacks a sha256 digest for $k."
  done
  [[ "$(env_get "$1" JM_FARM_STORAGE_CAPACITY_BYTES)" =~ ^[0-9]+$ ]] || die "$1 lacks JM_FARM_STORAGE_CAPACITY_BYTES."
}

# ---- compose ----------------------------------------------------------------

dc() {
  local unset_args=() spec k files=(-f "$CDIR/docker-compose.yml")
  # Values come from .env only: a variable in the caller's shell would win.
  for spec in "${SECRETS[@]}" "${DIGESTS[@]}" "${OTHER_KEYS[@]}"; do unset_args+=(-u "${spec%%:*}"); done
  [ -f "$CDIR/docker-compose.override.yml" ] && files+=(-f "$CDIR/docker-compose.override.yml")
  env "${unset_args[@]}" docker compose --project-name "$PROJECT" --project-directory "$CDIR" \
    --env-file "$CDIR/.env" "${files[@]}" "$@"
}

project_containers() { docker ps -aq --filter "label=com.docker.compose.project=$PROJECT"; }

validate_compose() {
  local img src
  dc config --quiet || die "the compose configuration is invalid (message above)."
  while read -r img; do
    [ -n "$img" ] || continue
    [[ "$img" == *@sha256:* ]] || die "image $img is not pinned to a sha256 digest."
  done < <(dc config --images)
  while read -r src; do
    case "$src" in
      /mnt/zSSD/*|/mnt/storage0/*) die "the rendered compose still mounts $src." ;;
    esac
    [ -e "$src" ] || die "the rendered compose mounts $src, which does not exist."
  done < <(dc config | sed -n 's/^[[:space:]]*source:[[:space:]]*"\{0,1\}\([^"]*\)"\{0,1\}$/\1/p')
}

check_ports_free() {
  local p
  while read -r p; do
    [ -n "$p" ] || continue
    if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
      die "port $p is already in use on this host (another JuiceMount install?). Stop that service first; nothing was started."
    fi
  done < <(dc config | sed -n 's/^[[:space:]]*published:[[:space:]]*"\{0,1\}\([0-9]*\)"\{0,1\}$/\1/p' | sort -u)
}

explain_failure() {
  local svc id code
  for svc in minio-keys juicefs-init; do
    id="$(dc ps -aq "$svc" 2>/dev/null | sed -n 1p)"
    [ -n "$id" ] || continue
    code="$(docker inspect -f '{{.State.ExitCode}}' "$id" 2>/dev/null || echo '?')"
    [ "$code" != 0 ] || continue
    say "--- last lines of $svc (exit $code) ---"
    dc logs --no-color --tail 20 "$svc" >&2 2>&1 || true
    if [ "$svc" = juicefs-init ]; then
      case "$code" in
        2) say "juicefs-init exit 2: the object store is not reachable inside the stack." ;;
        3) say "juicefs-init exit 3: Redis did not answer." ;;
        4) say "juicefs-init exit 4: an object key is empty or a placeholder." ;;
        5) say "juicefs-init exit 5: the bucket URL lacks http:// or https://." ;;
        6) say "juicefs-init exit 6: juicefs format failed; read the log lines above." ;;
      esac
    else
      say "minio-keys exit $code: the object-store users could not be created; read the log lines above."
    fi
    return
  done
  say "--- service states ---"
  dc ps -a >&2 || true
}

wait_for_manager() { # port key
  local deadline code
  deadline=$(( $(date +%s) + ${JM_SETUP_WAIT_SECONDS:-300} ))
  while :; do
    code="$(printf 'X-JuiceMount-Admin-Key: %s\n' "$2" |
      curl -sS -o /dev/null -w '%{http_code}' -m 5 -H @- "http://127.0.0.1:$1/api/overview" 2>/dev/null || true)"
    case "$code" in
      200) return 0 ;;
      401) die "the Manager on port $1 refuses this install's key: another Manager owns the port, or .env was edited." ;;
    esac
    [ "$(date +%s)" -lt "$deadline" ] || { say "The Manager did not answer on port $1 in time (last HTTP status: ${code:-none})."; dc ps -a >&2 || true; dc logs --no-color --tail 30 juicemount-manager >&2 2>&1 || true; exit 1; }
    sleep 3
  done
}

confirm() {
  [ "$YES" = 1 ] && return 0
  local ans=""
  # Piped into bash, stdin is the script: ask on the terminal instead.
  ( : </dev/tty ) 2>/dev/null || die "no terminal to ask for confirmation; re-run with --yes to accept the plan above."
  printf 'Go ahead? [y/N] ' >&2
  read -r ans </dev/tty || true
  case "$ans" in y|Y|yes|YES|Yes) ;; *) die "stopped; nothing was changed." ;; esac
}

# ---- main -------------------------------------------------------------------

main() {
  parse_args "$@"
  trap cleanup EXIT
  preflight
  locate_install
  CDIR="$ROOT/compose"
  [ -n "$HOST" ] || HOST="$(detect_host)"

  local installed=0 refresh=1 sub subs datasets="" created="" s
  [ -f "$CDIR/.env" ] && installed=1
  if [ "$installed" = 1 ]; then
    # Files of an install that ever started are never rewritten: an upgrade
    # is a rollout (server/COMPOSE-ROLLOUT.md), not a re-run of setup.
    refresh=0
    if [ -z "$(project_containers)" ]; then
      refresh=1
      while read -r sub; do
        [ -n "$sub" ] || continue
        has_content "$ROOT/$sub" && { refresh=0; break; }
      done < <(data_subdirs "$CDIR/docker-compose.yml" "$ROOT/")
    fi
    if [ "$refresh" = 0 ] && { [ -n "$SOURCE$OVERRIDE$IMAGES_ENV" ] || [ ${#IMPORTS[@]} -gt 0 ] || [ "$RELEASE" != latest ]; }; then
      say "Note: $ROOT is an existing install; it keeps its saved configuration and these options are ignored."
    fi
  fi

  STAGE="$(mktemp -d)"
  if [ "$refresh" = 1 ]; then
    stage_compose
    subs="$(data_subdirs "$STAGE/src/docker-compose.yml" "$COMPOSE_DATA_PREFIX")"
    [ -n "$subs" ] || die "docker-compose.yml mounts no data directory."
    if [ "$installed" = 0 ]; then
      stage_digests
      # Data without its secrets: refuse rather than generate new ones.
      for sub in $subs; do
        has_content "$ROOT/$sub" && die "$ROOT/$sub already holds data, but $CDIR/.env (its secrets) is missing. juicemount-setup will not generate new secrets over existing data. Restore .env from your backup, or choose an empty --dir or --pool."
      done
    fi
    for sub in $subs compose; do
      if [ "$MODE" = zfs ]; then
        zfs_exists "$ROOT_DS/$sub" && continue
        has_content "$ROOT/$sub" && die "$ROOT/$sub is a folder with content, not a dataset; a new dataset would hide it."
        datasets="$datasets $ROOT_DS/$sub"
      else
        [ -d "$ROOT/$sub" ] || created="$created $sub"
      fi
    done
    if [ "$MODE" = zfs ] && ! zfs_exists "$ROOT_DS"; then datasets=" $ROOT_DS$datasets"; fi

    say ""
    say "JuiceMount setup"
    if [ "$MODE" = zfs ]; then say "  Storage:   ZFS pool $POOL, folder $ROOT"; else say "  Storage:   folder $ROOT"; fi
    [ -z "$datasets" ] || say "  Creates:   datasets$datasets"
    [ -z "$created" ] || say "  Creates:   folders under $ROOT:$created"
    if [ "$installed" = 1 ]; then
      say "  Secrets:   kept from $CDIR/.env (this install has not started yet)"
    else
      say "  Release:   $RELEASE_NAME (image digests from its release manifest)"
      say "  Secrets:   generated now, stored in $CDIR/.env (mode 0600)"
    fi
    if [ -n "$SOURCE" ]; then say "  Compose:   $SOURCE/docker-compose.yml"; else say "  Compose:   $REPO_SLUG main, server/docker-compose.yml"; fi
    [ -z "$OVERRIDE" ] || say "  Override:  $OVERRIDE"
    say "  Import:    $(import_lines | sed 's/:ro$//' | tr '\n' ' ')(read-only)"
    say "  Ports:     30179 (Redis proxy), 30151/30152 (object store), 30190 (Manager), 127.0.0.1:30180 (WebDAV)"
    say "  Manager:   http://$HOST:30190"
    [ "$MODE" != zfs ] || say "  Note:      exclude $ROOT_DS/bucket from recursive snapshot tasks (INSTALL-TrueNAS.md)."
    say ""
    [ "$DRY" = 0 ] || { say "Dry run: nothing was changed."; exit 0; }
    confirm

    if [ "$MODE" = zfs ]; then
      for s in $datasets; do zfs create -p "$s" || die "could not create dataset $s."; done
      [ "$(zfs_mountpoint "$ROOT_DS")" = "$ROOT" ] || die "dataset $ROOT_DS is not mounted at $ROOT."
    fi
    mkdir -p "$ROOT" || die "could not create $ROOT (run with sudo, or pass --dir)."
    for sub in $subs; do mkdir -p "$ROOT/$sub"; done
    [ ${#IMPORTS[@]} -gt 0 ] || mkdir -p "$ROOT/import"
    mkdir -p "$CDIR"
    chmod 700 "$CDIR"
    LOCK="$CDIR/.setup-lock"
    mkdir "$LOCK" 2>/dev/null || { LOCK=""; die "another juicemount-setup is running ($CDIR/.setup-lock); if not, remove that folder."; }

    mkdir -p "$CDIR/minio"
    render_compose "$STAGE/src/docker-compose.yml" "$CDIR/docker-compose.yml.new" "$(import_lines)"
    mv -f "$CDIR/docker-compose.yml.new" "$CDIR/docker-compose.yml"
    for s in "${SUPPORT_FILES[@]}"; do
      [ "$s" = docker-compose.yml ] || cp "$STAGE/src/$s" "$CDIR/$s"
    done
    chmod 644 "$CDIR"/docker-compose.yml "$CDIR"/redis.conf "$CDIR"/redis.acl "$CDIR"/minio/*
    chmod 755 "$CDIR/minio" "$CDIR"/minio/*.sh
    if [ -f "$STAGE/src/docker-compose.override.yml" ]; then
      cp "$STAGE/src/docker-compose.override.yml" "$CDIR/docker-compose.override.yml"
      chmod 644 "$CDIR/docker-compose.override.yml"
    fi
    if [ "$installed" = 0 ]; then
      CAPACITY="$(storage_capacity)"
      [[ "$CAPACITY" =~ ^[0-9]+$ ]] && [ "$CAPACITY" -gt 0 ] || die "could not read the storage size of $ROOT."
      write_env "$CDIR/.env"
    fi
  else
    [ "$DRY" = 0 ] || { say "Dry run: $ROOT is an installed JuiceMount; a re-run checks it and starts what is stopped."; exit 0; }
    LOCK="$CDIR/.setup-lock"
    mkdir "$LOCK" 2>/dev/null || { LOCK=""; die "another juicemount-setup is running ($CDIR/.setup-lock); if not, remove that folder."; }
    say "JuiceMount is installed in $ROOT; checking it and starting what is stopped."
  fi

  [ "$(stat -c %a "$CDIR/.env" 2>/dev/null || echo 600)" = 600 ] || chmod 600 "$CDIR/.env"
  check_env "$CDIR/.env"
  validate_compose
  [ -n "$(project_containers)" ] || check_ports_free

  say "Downloading images (first run: a few minutes)..."
  dc pull --quiet || die "could not download an image (message above). Nothing was started; run this command again once the image is available."
  say "Starting JuiceMount..."
  if ! dc up -d >&2; then
    explain_failure
    die "the stack did not start. Your settings are kept in $CDIR; run this command again after fixing the cause."
  fi
  local port key
  port="$(dc port juicemount-manager 8080 2>/dev/null | sed -n 's/.*:\([0-9][0-9]*\)$/\1/p' | sed -n 1p)"
  port="${port:-30190}"
  key="$(env_get "$CDIR/.env" JM_ADMIN_KEY)"
  say "Waiting for the Manager..."
  wait_for_manager "$port" "$key"

  printf 'Manager URL:  http://%s:%s\n' "$HOST" "$port"
  printf 'Sign-in key:  %s\n' "$key"
}

main "$@"
