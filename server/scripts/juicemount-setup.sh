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
#      <pool>/juicemount with one child dataset per data folder, on the pool
#      with the most free space. Otherwise a folder, /srv/juicemount.
#   2. Reads the latest release's release-manifest.json and server-images.env.
#      Their hashes must match the release's SHA256SUMS, the manifest version
#      must match --release, and each image digest must match the manifest's
#      digest for the same key. The compose file and every file it mounts come
#      from the manifest's commit, so files and images belong to the same
#      release. A release made before this script existed is refused.
#      Trust: these checks catch a damaged or mismatched download, not a
#      tampered one. SHA256SUMS is unsigned and arrives over the same HTTPS
#      connection. What runs is whatever the GitHub repository holds: main
#      (this script) and the release (its assets and the commit it names).
#   3. Renders that docker-compose.yml with the chosen host paths. Every other
#      image (Redis, the object store, JuiceFS) comes from that compose file
#      or an --override file; this script names no image.
#   4. Checks everything before it writes anything: storage state, other
#      JuiceMount installs, the rendered compose (${VAR:?} guards, digest-pinned
#      images, mounts) and free ports. Then it shows the plan and asks once.
#   5. Generates every secret into <data>/compose/.env (0600, folder 0700),
#      starts the stack and waits until the Manager answers.
#   6. Prints two lines on stdout: the Manager URL and the Manager sign-in key.
#
# Re-running is safe. A secret is never generated over an existing .env, and
# data is never touched. A started install is never changed: a re-run only
# starts what is stopped and prints the same two lines, and refuses when the
# running services differ from their files (setup never recreates them) or
# when options differ from the first install. Upgrading a setup-made install
# is not supported yet. Overlays in <data>/compose/overlays/*.yml (for
# example docker-compose.link.yml) are always applied.
#
# Exit codes: 0 running; 1 refused or failed (the message says why); 2 bad
# usage. Options: --help.
set -euo pipefail

REPO_SLUG="lelanddutcher/juicemount"
RAW="https://raw.githubusercontent.com/$REPO_SLUG"
RELEASES="https://github.com/$REPO_SLUG/releases"
DEFAULT_DIR="/srv/juicemount"
# The sample host paths of the canonical compose, replaced at render time.
COMPOSE_DATA_PREFIX="/mnt/zSSD/juicemount/"
# Files the compose mounts relative to its own folder.
SUPPORT_FILES=(
  docker-compose.yml redis.conf redis.acl
  minio/provision-object-keys.sh minio/keyd.sh
  minio/jm-client-policy.json minio/jm-gc-policy.json minio/jm-keyadmin-policy.json
)
# A release commit without this file predates juicemount-setup.
SETUP_MARKER="server/scripts/juicemount-setup.sh"
# name:random bytes. Hex doubles the length. JM_ADMIN_KEY is the shortest the
# Manager accepts (32); MinIO secrets are at most 40; proxy passwords are
# URL-safe and at least 32.
SECRETS=(
  JM_ADMIN_KEY:16 MINIO_ROOT_PASSWORD:20
  JM_REDIS_PROXY_PASSWORD:32 JM_PROXY_FARM_PASSWORD:32 JM_PROXY_MANAGER_PASSWORD:32
  JM_PROXY_BG_PASSWORD:32 JM_PROXY_WEBDAV_PASSWORD:32 JM_PROXY_ADMIN_PASSWORD:32
  JM_OBJ_CLIENT_SECRET_KEY:20 JM_OBJ_GC_SECRET_KEY:20
)
DIGESTS=(JM_MANAGER_DIGEST JM_FARM_DIGEST JM_FARM_RENDER_DIGEST JM_RESPPROXY_DIGEST)
SAFE_PATH_RE='^/[A-Za-z0-9._/-]*$'
FIRST_PARTY_RE='lelanddutcher/(juicemount-manager|juicemount-migrator|jm-respproxy|juicefarm)'

say() { printf '%s\n' "$*" >&2; }
die() { printf 'juicemount-setup: %s\n' "$*" >&2; exit 1; }
usage() {
  cat >&2 <<'HELP'
juicemount-setup: install a JuiceMount server with one command.
Run it as root (TrueNAS: in System > Shell). Options, all optional:
  --yes             do not ask for confirmation (needed without a terminal)
  --dry-run         run every check, show the plan, change nothing
  --pool NAME       ZFS: create <pool>/juicemount datasets on this pool
  --dir PATH        keep all data in this folder; create no dataset
  --release TAG     install release TAG (default: the latest release)
  --source DIR      developer: take the compose and its files from DIR (a
                    checkout's server/ folder); digests still come from the
                    release, or from --images-env
  --images-env FILE developer, with --source: digests from this file
  --override FILE   extra compose file on top (e.g. another object-store image)
  --import PATH     folder the Manager may copy from, read-only (repeatable)
  --project NAME    Docker Compose project name (default juicemount)
  --host ADDR       address printed in the Manager URL (default: LAN address)
A started install accepts only the options it was installed with, and
--release of the release it runs.
Guide: server/INSTALL-TrueNAS.md
HELP
  exit 2
}

YES=0 DRY=0 POOL="" DIR="" SOURCE="" RELEASE="latest" IMAGES_ENV="" OVERRIDE="" PROJECT="juicemount" HOST=""
IMPORTS=()
STAGE="" LOCK="" DC=""

cleanup() {
  [ -z "$STAGE" ] || rm -rf "$STAGE"
  [ -z "$LOCK" ] || rmdir "$LOCK" 2>/dev/null
  return 0
}

real() { # path -> absolute, .. and symlinks resolved
  realpath -m -- "$1" 2>/dev/null || die "cannot resolve the path $1"
}

refuse_system_dir() { # path label
  case "$1" in
    /|/bin|/boot|/dev|/etc|/home|/lib|/lib32|/lib64|/media|/mnt|/opt|/proc|/root|/run|/sbin|/srv|/sys|/tmp|/usr|/var|/var/lib|/var/tmp)
      die "$2 $1 is a system folder; choose a folder inside it, such as $1/juicemount." ;;
    /bin/*|/boot/*|/dev/*|/etc/*|/lib/*|/lib32/*|/lib64/*|/proc/*|/run/*|/sbin/*|/sys/*|/usr/*|/var/lib/docker|/var/lib/docker/*)
      die "$2 $1 is inside a system folder." ;;
  esac
  [[ ! "$1" =~ ^/mnt/[^/]+$ ]] || die "$2 $1 is the root of a pool; choose a folder inside it, such as $1/juicemount."
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
  [[ "$RELEASE" == latest || "$RELEASE" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][A-Za-z0-9.]+)?$ ]] || die "--release must be latest or a tag like v0.6.0"
  [ -z "$POOL" ] || [[ "$POOL" =~ ^[A-Za-z0-9][A-Za-z0-9_.:-]*$ ]] || die "--pool is not a pool name"
  [ -z "$POOL" ] || [ -z "$DIR" ] || die "use --pool or --dir, not both"
  [ -z "$HOST" ] || [[ "$HOST" =~ ^[A-Za-z0-9.:-]+$ ]] || die "--host must be a host name or address"
  if [ -n "$DIR" ]; then
    DIR="$(real "$DIR")"
    [[ "$DIR" =~ $SAFE_PATH_RE ]] || die "--dir may contain only letters, digits and . _ - /"
    refuse_system_dir "$DIR" --dir
  fi
  if [ -n "$SOURCE" ]; then
    SOURCE="$(real "$SOURCE")"
    [ -f "$SOURCE/docker-compose.yml" ] || die "--source $SOURCE has no docker-compose.yml"
  fi
  if [ -n "$IMAGES_ENV" ]; then
    [ -n "$SOURCE" ] || die "--images-env needs --source: without a release there is no commit to take the compose from."
    [ "$RELEASE" = latest ] || die "use --release or --images-env, not both"
    [ -f "$IMAGES_ENV" ] || die "--images-env $IMAGES_ENV is not a file"
    IMAGES_ENV="$(real "$IMAGES_ENV")"
  fi
  if [ -n "$OVERRIDE" ]; then
    [ -f "$OVERRIDE" ] || die "--override $OVERRIDE is not a file"
    OVERRIDE="$(real "$OVERRIDE")"
  fi
  local i
  for i in "${!IMPORTS[@]}"; do
    [ -d "${IMPORTS[$i]}" ] || die "--import ${IMPORTS[$i]} is not a folder."
    IMPORTS[i]="$(real "${IMPORTS[$i]}")"
    [[ "${IMPORTS[$i]}" =~ $SAFE_PATH_RE ]] || die "--import ${IMPORTS[$i]} may contain only letters, digits and . _ - /"
  done
  return 0
}

need() { command -v "$1" >/dev/null 2>&1 || die "$2"; }

preflight() {
  need docker "Docker is not installed. On TrueNAS, open Apps and choose a pool for apps first."
  need curl "curl is not installed."
  local t
  for t in od realpath sha256sum; do need "$t" "$t (coreutils) is not installed."; done
  docker info >/dev/null 2>&1 || die "Docker is not running, or this user may not use it: run this command as root. On TrueNAS, open Apps and choose a pool for apps first."
  docker compose version >/dev/null 2>&1 || die "Docker Compose v2 (docker compose) is not installed."
  [ -r /dev/urandom ] || die "/dev/urandom is not readable."
}

fetch() { # url dest
  curl -fsSL --proto '=https' --tlsv1.2 --retry 3 -o "$2" "$1" || die "could not download $1"
}

rand_hex() { # bytes -> variable RAND
  RAND="$(od -An -vtx1 -N"$1" /dev/urandom | tr -d ' \n')" || die "could not read random bytes"
  [[ "$RAND" =~ ^[0-9a-f]+$ ]] && [ ${#RAND} -eq $(($1 * 2)) ] || die "could not read random bytes"
}

env_get() { # file key -> last plain assignment
  sed -n "s/^$2=//p" "$1" | tail -n 1
}

sha_of() { sha256sum "$1" | cut -d' ' -f1; }
compose_version() { docker compose version --short 2>/dev/null || echo unknown; }

# ---- where the install lives ------------------------------------------------

is_truenas() { command -v midclt >/dev/null 2>&1; }
zfs_mountpoint() { zfs get -H -o value mountpoint "$1" 2>/dev/null; }
zfs_exists() { zfs list -H -o name "$1" >/dev/null 2>&1; }

pick_pool() {
  # The pool with the most free space, never the boot pool.
  zpool list -Hp -o name,free 2>/dev/null |
    awk '$1 != "boot-pool" && $1 != "freenas-boot" { if ($2 + 0 > best) { best = $2 + 0; name = $1 } } END { if (name != "") print name }'
}

# Sets MODE (zfs|dir), ROOT, ROOT_DS, POOL. Reads only.
locate_install() {
  local wd id
  ROOT_DS=""
  # An install of this project already known to Docker wins over defaults.
  if [ -z "$DIR" ] && [ -z "$POOL" ]; then
    id="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT" | sed -n 1p)"
    if [ -n "$id" ]; then
      wd="$(docker inspect -f '{{index .Config.Labels "com.docker.compose.project.working_dir"}}' "$id")"
      case "$wd" in
        */compose)
          if [ -f "$wd/.env" ]; then DIR="${wd%/compose}"
          elif cannot_enter "$wd"; then
            die "Compose project '$PROJECT' runs from $wd, which this user cannot enter (it is private to root): run this command as root."
          fi ;;
      esac
      [ -n "$DIR" ] || die "Compose project '$PROJECT' already exists ($wd) and was not made by juicemount-setup. Use --project with another name, or manage it by hand."
      # The same checks as for --dir: this folder is about to be checked,
      # started and made private.
      [[ "$DIR" =~ $SAFE_PATH_RE ]] || die "Compose project '$PROJECT' runs from $wd, a path with characters juicemount-setup does not handle; manage it by hand."
      refuse_system_dir "$DIR" "the install folder of project '$PROJECT',"
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
      local p mp name mounted key
      while read -r p; do
        case "$p" in boot-pool|freenas-boot) continue ;; esac
        zfs_exists "$p/juicemount" || continue
        while read -r name mounted key; do
          [ -n "$name" ] || continue
          if [ "$key" = unavailable ] || [ "$mounted" != yes ]; then
            die "$name exists but is locked or not mounted, so this command cannot see whether JuiceMount is installed there. Unlock or mount it first, or pass --pool or --dir; nothing was changed."
          fi
        done < <(zfs list -H -o name,mounted,keystatus -r "$p/juicemount")
        mp="$(zfs_mountpoint "$p/juicemount")" || continue
        [ -f "$mp/compose/.env" ] && { POOL="$p"; break; }
      done < <(zpool list -H -o name 2>/dev/null)
    fi
    [ -n "$POOL" ] || POOL="$(pick_pool)"
    [ -n "$POOL" ] || die "No ZFS storage pool found. Create one in Storage first, or pass --dir."
    zpool list -H -o name "$POOL" >/dev/null 2>&1 || die "ZFS pool $POOL does not exist."
    MODE=zfs ROOT_DS="$POOL/juicemount"
    if zfs_exists "$ROOT_DS"; then ROOT="$(zfs_mountpoint "$ROOT_DS")"; else ROOT="$(zfs_mountpoint "$POOL")/juicemount"; fi
    [[ "$ROOT" =~ $SAFE_PATH_RE ]] && [ "$ROOT" != /juicemount ] || die "Pool $POOL has no usable mountpoint ($ROOT)."
    return
  fi
  MODE=dir ROOT="$DEFAULT_DIR"
}

has_content() { [ -d "$1" ] && [ -n "$(ls -A "$1" 2>/dev/null)" ]; }

# A path this (non-root) user cannot enter: it exists but is not readable, or
# its nearest visible parent cannot be searched (a 0700 root owned by root).
cannot_enter() {
  local p="$1"
  [ "$(id -u)" != 0 ] || return 1
  if [ -e "$p" ]; then [ ! -r "$p" ] || [ ! -x "$p" ]; return; fi
  while [ ! -e "$p" ] && [ "$p" != / ]; do p="$(dirname "$p")"; done
  [ ! -x "$p" ] || [ ! -r "$p" ]
}
# Files anywhere below (empty folders do not count).
holds_files() { [ -d "$1" ] && [ -n "$(find "$1" -mindepth 1 ! -type d -print -quit 2>/dev/null)" ]; }

# An unmounted or locked dataset shows an empty folder; refuse instead of
# taking that folder for an empty install. A folder with content where a
# new dataset would be mounted would be hidden by it; refuse that too.
check_zfs_state() {
  local name mounted key acl
  [ "$MODE" = zfs ] || return 0
  if zfs_exists "$ROOT_DS"; then
    while read -r name mounted key acl; do
      [ -n "$name" ] || continue
      [ "$key" != unavailable ] || die "dataset $name is locked (encrypted, key not loaded). Unlock it in Datasets first; nothing was changed."
      [ "$mounted" = yes ] || die "dataset $name is not mounted, so its contents cannot be checked. Mount it first; nothing was changed."
      # NFSv4 ACLs (the TrueNAS SMB preset) can grant access that mode 0700
      # does not show, and with aclmode=restricted chmod fails.
      [ "$acl" != nfsv4 ] || die "dataset $name uses NFSv4 ACLs (the SMB preset), so juicemount-setup cannot make it private. In Datasets, set its ACL Type to POSIX and ACL Mode to Discard (or remove it and let juicemount-setup create the datasets); nothing was changed."
    done < <(zfs list -H -o name,mounted,keystatus,acltype -r "$ROOT_DS")
  else
    read -r name mounted key < <(zfs list -H -o name,mounted,keystatus -d 0 "$POOL") || true
    [ "${key:-}" != unavailable ] || die "pool $POOL is locked (encrypted, key not loaded). Unlock it first; nothing was changed."
    [ "${mounted:-}" = yes ] || die "pool $POOL is not mounted. Nothing was changed."
    if holds_files "$ROOT"; then
      die "$ROOT is a folder with content, not a dataset; a new dataset $ROOT_DS would hide it. Move it away or choose --pool or --dir; nothing was changed."
    fi
  fi
  return 0
}

# ---- staged files -----------------------------------------------------------

# Reads the release: COMMIT, RELEASE_NAME and DIG_<key>, each digest matched
# to its own key in release-manifest.json.
stage_release() {
  local base sums="$STAGE/SHA256SUMS" envf="$STAGE/server-images.env" man="$STAGE/release-manifest.json" k v m f
  if [ -n "$IMAGES_ENV" ]; then
    cp "$IMAGES_ENV" "$envf"
    RELEASE_NAME="local:$(basename "$IMAGES_ENV")" COMMIT=local
  else
    if [ "$RELEASE" = latest ]; then base="$RELEASES/latest/download"; else base="$RELEASES/download/$RELEASE"; fi
    fetch "$base/SHA256SUMS" "$sums"
    for f in release-manifest.json server-images.env; do
      fetch "$base/$f" "$STAGE/$f"
      m="$(awk -v f="$f" '$2 == f || $2 == "*" f { print $1 }' "$sums")"
      [ -n "$m" ] || die "the release's SHA256SUMS does not list $f."
      [ "$m" = "$(sha_of "$STAGE/$f")" ] || die "$f does not match the release's SHA256SUMS."
    done
    RELEASE_NAME="$(sed -n 's/^[[:space:]]*"version":[[:space:]]*"\([^"]*\)".*/\1/p' "$man" | sed -n 1p)"
    COMMIT="$(sed -n 's/^[[:space:]]*"commit":[[:space:]]*"\([0-9a-f]\{40\}\)".*/\1/p' "$man" | sed -n 1p)"
    [ -n "$RELEASE_NAME" ] && [ -n "$COMMIT" ] || die "release-manifest.json has no version or commit."
    [ "$RELEASE" = latest ] || [ "v$RELEASE_NAME" = "$RELEASE" ] ||
      die "release $RELEASE has a release-manifest.json for version $RELEASE_NAME; refusing the mismatch."
    # key=digest pairs inside "server_images", one per key.
    awk '
      /"server_images"[[:space:]]*:/ { s = 1; next }
      s && /"JM_[A-Z_]*_DIGEST"[[:space:]]*:/ { k = $0; sub(/^[^"]*"/, "", k); sub(/".*/, "", k); next }
      s && k != "" && /"digest"[[:space:]]*:/ {
        d = $0; sub(/.*"digest"[[:space:]]*:[[:space:]]*"/, "", d); sub(/".*/, "", d); print k "=" d; k = ""
      }' "$man" > "$STAGE/manifest.digests"
  fi
  # Only KEY=sha256:<64 hex> lines and comments are accepted.
  if grep -qvE '^(#.*|[[:space:]]*|[A-Z_]+=sha256:[0-9a-f]{64})$' "$envf"; then
    die "server-images.env has a line that is not KEY=sha256:<64 hex>."
  fi
  for k in "${DIGESTS[@]}"; do
    [ "$(grep -c "^$k=" "$envf")" = 1 ] || die "server-images.env must set $k exactly once."
    v="$(env_get "$envf" "$k")"
    [[ "$v" =~ ^sha256:[0-9a-f]{64}$ ]] || die "the release has no digest for $k."
    if [ "$COMMIT" != local ]; then
      [ "$(grep -c "^$k=" "$STAGE/manifest.digests")" = 1 ] || die "release-manifest.json must list $k exactly once."
      [ "$(env_get "$STAGE/manifest.digests" "$k")" = "$v" ] || die "$k in server-images.env is not the digest release-manifest.json gives for $k."
    fi
    printf -v "DIG_$k" '%s' "$v"
  done
}

# Copies or downloads the compose and the files it mounts into $STAGE/src.
stage_files() {
  local f code
  mkdir -p "$STAGE/src/minio"
  if [ -z "$SOURCE" ]; then
    code="$(curl -sS --proto '=https' --tlsv1.2 --retry 3 -o /dev/null -w '%{http_code}' "$RAW/$COMMIT/$SETUP_MARKER" 2>/dev/null || true)"
    case "$code" in
      200) ;;
      404) die "release $RELEASE_NAME predates juicemount-setup, so its files do not support it. Setup installs releases made after it was added (the first is the release after 0.5.0). For $RELEASE_NAME, use the manual install in server/INSTALL-TrueNAS.md (appendix)." ;;
      *) die "could not reach $RAW (HTTP ${code:-none})." ;;
    esac
  fi
  for f in "${SUPPORT_FILES[@]}"; do
    if [ -n "$SOURCE" ]; then
      [ -f "$SOURCE/$f" ] || die "--source $SOURCE lacks $f"
      cp "$SOURCE/$f" "$STAGE/src/$f"
    else
      fetch "$RAW/$COMMIT/server/$f" "$STAGE/src/$f"
    fi
  done
  grep -Eq "^[[:space:]]*[^#[:space:]][^#]*$COMPOSE_DATA_PREFIX" "$STAGE/src/docker-compose.yml" ||
    die "docker-compose.yml no longer uses $COMPOSE_DATA_PREFIX host paths; this setup script does not know its layout."
  acl_replay_ok "$STAGE/src/redis.acl" ||
    die "this redis.acl (${SOURCE:-release $RELEASE_NAME}) has the 0.5.0 default user line, which makes Redis drop its transactions when it replays its append-only file at a restart. juicemount-setup will not install it: use a release with the fixed server/redis.acl ($REDIS_SAFE_ORDER). Nothing was changed."
}

# Data folder names (first path element below the prefix) the compose
# mounts, from its non-comment lines.
data_subdirs() { # compose file, prefix
  grep -v '^[[:space:]]*#' "$1" | grep -o "$2[A-Za-z0-9._-]*" | sed "s|^$2||" | sort -u
}

import_lines() {
  local p name used=" " i out=""
  if [ ${#IMPORTS[@]} -eq 0 ]; then
    printf '%s:/sources/import:ro\n' "$ROOT/import"
    return
  fi
  for p in "${IMPORTS[@]}"; do
    case "$p/" in "$ROOT"/*) die "--import $p is inside the JuiceMount data folder." ;; esac
    name="$(basename "$p")"; i=1
    while [[ "$used" == *" $name "* ]]; do i=$((i + 1)); name="$(basename "$p")-$i"; done
    used="$used$name "
    out="$out$p:/sources/$name:ro"$'\n'
  done
  printf '%s' "$out"
}

render_compose() { # staged compose -> project compose with host paths
  local src="$1" dst="$2" imports n
  imports="$(import_lines)"
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

# The compose wrapper. Setup runs every compose command through it, and it is
# kept in the install so a person runs exactly the same project. Values reach
# compose only from .env: the environment is cleared except what reaches the
# Docker daemon (DOCKER_*, XDG_RUNTIME_DIR for rootless Docker, SSH_AUTH_SOCK
# for DOCKER_HOST=ssh://).
write_wrapper() { # path, project folder
  cat > "$1" <<WRAP
#!/usr/bin/env bash
# Written by juicemount-setup. Runs docker compose for the JuiceMount install
# in this folder: its .env, docker-compose.yml, docker-compose.override.yml
# and overlays/*.yml (in name order). Example: ./jm-compose ps
set -euo pipefail
d='$2'
files=(-f "\$d/docker-compose.yml")
[ ! -f "\$d/docker-compose.override.yml" ] || files+=(-f "\$d/docker-compose.override.yml")
for f in \$(LC_ALL=C ls -1 "\$d/overlays" 2>/dev/null | grep -E '[.]ya?ml\$' || true); do files+=(-f "\$d/overlays/\$f"); done
keep=("PATH=\$PATH" "HOME=\${HOME:-/root}")
for v in DOCKER_HOST DOCKER_CONTEXT DOCKER_CONFIG DOCKER_CERT_PATH DOCKER_TLS_VERIFY XDG_RUNTIME_DIR SSH_AUTH_SOCK; do
  [ -z "\${!v:-}" ] || keep+=("\$v=\${!v}")
done
exec env -i "\${keep[@]}" docker compose --project-name '$PROJECT' --project-directory "\$d" --env-file "\$d/.env" "\${files[@]}" "\$@"
WRAP
  chmod 755 "$1"
}

dc() { "$DC" "$@"; }

# Writes the check .env for validation before anything is written: the new
# digests and settings, and in place of each secret a same-length run of
# zeros, so no secret is ever in the staging folder. For an install that
# never started, its other lines (such as JM_NET_SERVER_URL) are kept.
write_check_env() { # path [existing .env]
  local spec k n v skip
  skip="^($(printf '%s|' "${DIGESTS[@]}" "${SECRETS[@]%%:*}")JM_SETUP_[A-Z_]*)="
  {
    if [ -n "${2:-}" ]; then
      grep -vE "$skip" "$2" || true
    else
      printf 'JM_FARM_STORAGE_CAPACITY_BYTES=%s\n' "$CAPACITY"
      printf 'JM_FARM_NODE_META_URL=redis://farm@%s:30179/1\n' "$HOST"
    fi
    for k in "${DIGESTS[@]}"; do v="DIG_$k"; printf '%s=%s\n' "$k" "${!v}"; done
    for spec in "${SECRETS[@]}"; do
      k="${spec%%:*}" n="${spec##*:}"
      printf '%s=%s\n' "$k" "$(printf "%0$((n * 2))d" 0)"
    done
  } > "$1"
}

# JM_FARM_STORAGE_CAPACITY_BYTES follows the compose contract: the physical
# pool size, `zpool list -Hp -o size`. That is raw size, before RAID-Z parity,
# so on RAID-Z it is larger than the usable space; the farm uses it only for
# total-capacity reporting and to scale its default 1% reserve.
storage_capacity() {
  local p
  if [ "$MODE" = zfs ]; then
    zpool list -Hp -o size "$POOL"
  else
    p="$ROOT"
    while [ ! -d "$p" ]; do p="$(dirname "$p")"; done
    df -Pk "$p" | awk 'NR == 2 { printf "%.0f\n", $2 * 1024 }'
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

# The options that decide what is installed, apart from the release. A started
# install accepts a re-run with none of them, or with exactly the same ones,
# and a --release equal to the release it runs.
options_signature() {
  local o
  o="source=$SOURCE"
  [ -z "$IMAGES_ENV" ] || o="$o images=$(sha_of "$IMAGES_ENV")"
  [ -z "$OVERRIDE" ] || o="$o override=$(sha_of "$OVERRIDE")"
  [ ${#IMPORTS[@]} -eq 0 ] || o="$o import=${IMPORTS[*]}"
  printf '%s' "$o" | sha256sum | cut -d' ' -f1
}
no_content_options() {
  [ -z "$SOURCE$IMAGES_ENV$OVERRIDE" ] && [ ${#IMPORTS[@]} -eq 0 ]
}

write_env() { # path: a new .env, never over an existing one
  local tmp spec k n v sig
  local -a lines=()
  # Every secret is generated and checked before anything is written.
  for spec in "${SECRETS[@]}"; do
    k="${spec%%:*}" n="${spec##*:}"
    rand_hex "$n"
    lines+=("$k=$RAND")
  done
  sig="$(options_signature)"
  tmp="$CDIR/.env.new.$$"
  (
    umask 077
    {
      printf '# JuiceMount server settings, written by juicemount-setup on %s.\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      printf '# Every secret of this server is in this file. Keep it private (mode 0600)\n'
      printf '# and back it up with the data. juicemount-setup never changes a secret here.\n'
      printf '# Template and meaning of each value: server/release.env.example.\n'
      printf 'JM_SETUP_RELEASE=%s\nJM_SETUP_COMMIT=%s\nJM_SETUP_OPTIONS=%s\n' "$RELEASE_NAME" "$COMMIT" "$sig"
      printf 'JM_SETUP_COMPOSE_VERSION=%s\n' "$(compose_version)"
      for k in "${DIGESTS[@]}"; do v="DIG_$k"; printf '%s=%s\n' "$k" "${!v}"; done
      printf '%s\n' "${lines[@]}"
      printf 'JM_FARM_STORAGE_CAPACITY_BYTES=%s\n' "$CAPACITY"
      printf 'JM_FARM_NODE_META_URL=redis://farm@%s:30179/1\n' "$HOST"
    } > "$tmp"
  )
  # ln refuses an existing target: an install's .env is never replaced.
  ln "$tmp" "$1" 2>/dev/null || { rm -f "$tmp"; die "$1 appeared while writing it; nothing was changed."; }
  rm -f "$tmp"
  chmod 600 "$1"
}

# Install that never started: new digests and setup records. Every other
# line, every secret among them, stays byte for byte.
update_env_release() { # path
  local tmp="$CDIR/.env.new.$$" k v line sig
  sig="$(options_signature)"
  (
    umask 077
    while IFS= read -r line || [ -n "$line" ]; do
      k="${line%%=*}"
      case "$k" in
        JM_SETUP_RELEASE) line="$k=$RELEASE_NAME" ;;
        JM_SETUP_COMMIT) line="$k=$COMMIT" ;;
        JM_SETUP_OPTIONS) line="$k=$sig" ;;
        JM_SETUP_COMPOSE_VERSION) line="$k=$(compose_version)" ;;
        JM_MANAGER_DIGEST|JM_FARM_DIGEST|JM_FARM_RENDER_DIGEST|JM_RESPPROXY_DIGEST) v="DIG_$k"; line="$k=${!v}" ;;
      esac
      printf '%s\n' "$line"
    done < "$1" > "$tmp"
    # An .env from an older setup may lack a record: add it.
    grep -q '^JM_SETUP_RELEASE=' "$tmp" || printf 'JM_SETUP_RELEASE=%s\n' "$RELEASE_NAME" >> "$tmp"
    grep -q '^JM_SETUP_COMMIT=' "$tmp" || printf 'JM_SETUP_COMMIT=%s\n' "$COMMIT" >> "$tmp"
    grep -q '^JM_SETUP_OPTIONS=' "$tmp" || printf 'JM_SETUP_OPTIONS=%s\n' "$sig" >> "$tmp"
    grep -q '^JM_SETUP_COMPOSE_VERSION=' "$tmp" || printf 'JM_SETUP_COMPOSE_VERSION=%s\n' "$(compose_version)" >> "$tmp"
  )
  for k in "${DIGESTS[@]}"; do
    v="DIG_$k"
    [ "$(env_get "$tmp" "$k")" = "${!v}" ] || { rm -f "$tmp"; die "could not record $k in $1."; }
  done
  mv -f "$tmp" "$1"
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

# ---- Redis append-only file and its ACL ---------------------------------------

# Redis rebuilds its data at start by replaying its append-only file (AOF),
# and checks each replayed transaction against the ACL's `default` user. With
# the 0.5.0 line (`user default off ... -@all`) it refuses them all, so a
# restart drops the metadata written since the last AOF rewrite
# (CHANGELOG.md, 0.5.1). True when the `default` line of the ACL in $1 ("-"
# for stdin) grants what the replay needs: every key, and every command or
# the write commands plus SELECT. Rules apply left to right, as in Redis.
acl_replay_ok() {
  awk '
    $1 == "user" && $2 == "default" {
      found = 1; keys = 0; all = 0; w = 0; sel = 0
      for (i = 3; i <= NF; i++) {
        t = $i
        if (t == "~*" || t == "allkeys") keys = 1
        else if (t == "resetkeys") keys = 0
        else if (t == "+@all" || t == "allcommands") { all = 1; w = 1; sel = 1 }
        else if (t == "-@all" || t == "nocommands") { all = 0; w = 0; sel = 0 }
        else if (t == "+@write") w = 1
        else if (t == "-@write") { w = 0; all = 0 }
        else if (t == "+select") sel = 1
        else if (t == "-select") { sel = 0; all = 0 }
      }
      ok = keys && (all || (w && sel))
    }
    END { exit (found && ok) ? 0 : 1 }' "$1"
}

REDIS_SAFE_ORDER="CHANGELOG.md, 0.5.1, \"Upgrading the Redis proxy and Manager\""

# Started install: never let setup start or recreate a Redis that would
# replay its AOF under the 0.5.0 ACL. Setup never replaces the ACL itself.
check_redis_acl() {
  local id running=false rendered="" file_old=0 last_old=0
  acl_replay_ok "$CDIR/redis.acl" || file_old=1
  id="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT" --filter "label=com.docker.compose.service=redis" | sed -n 1p)"
  if [ -n "$id" ]; then
    running="$(docker inspect -f '{{.State.Running}}' "$id" 2>/dev/null || echo false)"
    # The ACL the container last rendered at its start (also when stopped).
    rendered="$(docker cp "$id:/run/jm-redis/users.acl" - 2>/dev/null | tar -xO 2>/dev/null || true)"
    if [ -n "$rendered" ] && ! printf '%s\n' "$rendered" | acl_replay_ok -; then last_old=1; fi
  fi
  [ "$file_old$last_old" != 00 ] || return 0
  if [ "$running" = true ]; then
    if [ "$last_old" = 1 ]; then
      say "WARNING: Redis is running with the 0.5.0 ACL. This run leaves it alone, but its next restart (a NAS reboot counts) will drop recent metadata."
    else
      say "WARNING: $CDIR/redis.acl has the 0.5.0 ACL, and Redis will use it at its next start (a NAS reboot counts), which will drop recent metadata. This run leaves the running Redis alone."
    fi
    say "Update it in the order of $REDIS_SAFE_ORDER: keep Redis running; copy $ROOT/redis somewhere safe; run BGREWRITEAOF and wait until INFO persistence shows aof_rewrite_scheduled:0, aof_rewrite_in_progress:0 and aof_last_bgrewrite_status:ok; only then update $CDIR/redis.acl and restart Redis."
    return 0
  fi
  if [ "$file_old" = 1 ]; then
    say "Redis is not running, and $CDIR/redis.acl has the 0.5.0 ACL. Starting Redis would replay its append-only file under that ACL and drop every transaction since its last rewrite, so juicemount-setup will not start or recreate it."
  else
    say "Redis is not running, and it was last started with the 0.5.0 ACL. That start may already have dropped transactions, and starting it now with the fixed ACL would replay them over newer writes, so juicemount-setup will not start or recreate it."
  fi
  say "What to do ($REDIS_SAFE_ORDER):"
  say "  1. Copy $ROOT/redis (its appendonlydir) somewhere safe. It may be the only complete record."
  say "  2. If this Redis was never started under the 0.5.0 ACL since its append-only file was last rewritten, put the fixed server/redis.acl in $CDIR and start it alone: $CDIR/jm-compose up -d redis. Then run this command again."
  say "  3. Otherwise do not start it with either ACL: restarting it under 0.5.0 already lost changes, and starting it with the new ACL now would replay them over newer writes. Recovering is a manual restore from the copy."
  say "  For a Redis that is running under the 0.5.0 ACL: keep it running, copy $ROOT/redis, run BGREWRITEAOF and wait until INFO persistence shows aof_rewrite_scheduled:0, aof_rewrite_in_progress:0 and aof_last_bgrewrite_status:ok, then update the ACL and restart it."
  die "nothing was started or changed."
}

# ---- checks (read only) -----------------------------------------------------

project_containers() { docker ps -aq --filter "label=com.docker.compose.project=$PROJECT"; }

# A Compose project name belongs to one folder. Containers of $PROJECT that
# run from another folder belong to another install: never take them over.
check_project_owner() {
  local id wd
  while read -r id; do
    [ -n "$id" ] || continue
    wd="$(docker inspect -f '{{index .Config.Labels "com.docker.compose.project.working_dir"}}' "$id")"
    [ "$wd" = "$CDIR" ] || die "Compose project '$PROJECT' already runs from $wd, not $CDIR. juicemount-setup never takes over another install. To check that install, run this command without --dir and --pool; to make a separate one, pass another --project. Nothing was changed."
  done < <(project_containers)
}

# Root is needed for datasets and for a root-owned install; a non-root user
# with Docker access may install into a folder it can write.
check_access() {
  local p
  if { [ -e "$CDIR" ] && [ ! -r "$CDIR" ]; } || { [ ! -e "$CDIR" ] && cannot_enter "$CDIR" && [ -e "$ROOT" ]; }; then
    die "$CDIR exists but this user cannot read it (it is private to root): run this command as root."
  fi
  [ "$(id -u)" != 0 ] || return 0
  [ "$MODE" != zfs ] || die "creating and checking ZFS datasets needs root: run this command as root."
  p="$ROOT"
  while [ ! -e "$p" ]; do p="$(dirname "$p")"; done
  [ -w "$p" ] || die "$p is not writable by this user: run this command as root, or pass a --dir you can write."
}

# The data root is private (0700): below it are the object store (with its
# IAM file holding the jm-gc secret), the metadata and the settings. Containers
# reach their folders through bind mounts and never cross the root itself.
secure_root() { # fix|check
  local m
  m="$(stat -c %a "$ROOT")"
  [ $((8#$m & 8#077)) -ne 0 ] || return 0
  if [ "$1" = check ]; then
    say "Note: $ROOT is mode $m (other users can enter it); a real run makes it 700."
    return 0
  fi
  chmod 00700 "$ROOT" || die "could not make $ROOT private (chmod 00700); on TrueNAS check the dataset's ACL mode. Nothing else was changed."
  say "Note: $ROOT was mode $m; it is now 700 (private to its owner)."
}

validate_compose() { # root under which missing mount sources will be created ("" = none)
  local img src
  dc config --quiet || die "the compose configuration is invalid (message above); nothing was changed."
  while read -r img; do
    [ -n "$img" ] || continue
    [[ "$img" == *@sha256:* ]] || die "image $img is not pinned to a sha256 digest."
  done < <(dc config --images)
  while read -r src; do
    case "$src" in
      /mnt/zSSD/*|/mnt/storage0/*) die "the rendered compose still mounts $src." ;;
    esac
    [ -e "$src" ] && continue
    if [ -n "$1" ]; then case "$src/" in "$1"/*) continue ;; esac; fi
    die "the rendered compose mounts $src, which does not exist."
  done < <(dc config | sed -n 's/^[[:space:]]*source:[[:space:]]*"\{0,1\}\([^"]*\)"\{0,1\}$/\1/p')
}

# Another JuiceMount on this host (a TrueNAS app such as ix-juicemount, a
# hand-made Compose project, or loose containers) would fight over ports
# and data.
check_no_other_juicemount() {
  local proj img name
  local -a ids=()
  mapfile -t ids < <(docker ps -aq)
  [ ${#ids[@]} -gt 0 ] || return 0
  # .Config.Image keeps the reference as written; `docker ps` shows only an
  # image ID for an image pinned by digest alone. "|", not a tab: a tab is
  # IFS whitespace, so an empty project field would shift the fields.
  while IFS='|' read -r proj img name; do
    [ "$proj" != "$PROJECT" ] || continue
    if [ "$proj" = ix-juicemount ] || [[ "$img" =~ $FIRST_PARTY_RE ]]; then
      die "another JuiceMount is on this host: container $name (project ${proj:-none}, image $img). juicemount-setup installs only where none runs; nothing was changed."
    fi
  done < <(docker inspect -f '{{index .Config.Labels "com.docker.compose.project"}}|{{.Config.Image}}|{{.Name}}' "${ids[@]}")
}

# "proto port host_ip" for each published port of the rendered compose.
published_ports() {
  dc config | awk '
    function flush() { if (pub != "") print (proto == "" ? "tcp" : proto), pub, (hip == "" ? "*" : hip); pub = proto = hip = "" }
    /^[[:space:]]*- / || /^[^[:space:]]/ || /^  [^[:space:]]/ { flush() }
    /^[[:space:]]*(- )?published:/ { v = $0; sub(/.*published:[[:space:]]*/, "", v); gsub(/"/, "", v); pub = v }
    /^[[:space:]]*(- )?protocol:/ { v = $0; sub(/.*protocol:[[:space:]]*/, "", v); proto = v }
    /^[[:space:]]*(- )?host_ip:/ { v = $0; sub(/.*host_ip:[[:space:]]*/, "", v); hip = v }
    END { flush() }' | sort -u
}

port_in_use() { # proto port: a listener on any address
  if command -v ss >/dev/null 2>&1; then
    if [ "$1" = udp ]; then [ -n "$(ss -Hlun "sport = :$2" 2>/dev/null)" ]; else [ -n "$(ss -Hltn "sport = :$2" 2>/dev/null)" ]; fi
  else
    # Without ss only TCP on this host's loopback can be tested.
    [ "$1" = tcp ] && (exec 3<>"/dev/tcp/127.0.0.1/$2") 2>/dev/null
  fi
}

check_ports_free() {
  local proto p
  while read -r proto p _; do
    [ -n "$p" ] || continue
    if port_in_use "$proto" "$p"; then
      die "port $p/$proto is already in use on this host. Stop that service first; nothing was changed."
    fi
  done < <(published_ports)
}

ports_summary() {
  published_ports | awk '{ printf "%s%s/%s ", ($3 == "*" ? "" : $3 ":"), $2, $1 }'
}

# A started install is never recreated: if a service's container was made
# from a different configuration (an overlay applied by hand and not kept in
# overlays/, an edited .env), refuse instead of reverting it.
check_no_drift() {
  local svc want id have drift=""
  while read -r svc want; do
    [ -n "$svc" ] || continue
    id="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT" --filter "label=com.docker.compose.service=$svc" | sed -n 1p)"
    [ -n "$id" ] || continue
    have="$(docker inspect -f '{{index .Config.Labels "com.docker.compose.config-hash"}}' "$id")"
    [ "$have" = "$want" ] || drift="$drift $svc"
  done < <(dc config --hash '*')
  [ -z "$drift" ] && return 0
  local was now note=""
  was="$(env_get "$CDIR/.env" JM_SETUP_COMPOSE_VERSION)"; now="$(compose_version)"
  [ -z "$was" ] || [ "$was" = "$now" ] || note=" Docker Compose was $was when this install was made and is $now now, so cause 2 is likely."
  local advice="If the files are as intended, '$CDIR/jm-compose up -d' recreates those services from them and keeps the data."
  case " $drift " in *" redis "*)
    local others
    others="$(printf '%s\n' $drift | grep -vx redis | tr '\n' ' ')"
    advice="Redis is among them: do not run a bare '$CDIR/jm-compose up -d', which would restart it and replay its append-only file. ${others:+Recreate the others with '$CDIR/jm-compose up -d --no-deps ${others% }'. }Restart Redis only in the order of $REDIS_SAFE_ORDER: keep it running, copy $ROOT/redis, run BGREWRITEAOF and wait until INFO persistence shows aof_rewrite_scheduled:0, aof_rewrite_in_progress:0 and aof_last_bgrewrite_status:ok, then recreate it with '$CDIR/jm-compose up -d --no-deps redis'." ;;
  esac
  die "the running services$drift do not match what this install's files would start, and juicemount-setup never recreates a started install. Two causes are possible: 1) the files changed after the services started (an overlay applied by hand and not kept in $CDIR/overlays/, or an edited .env or compose file); 2) Docker Compose was upgraded and computes its configuration hash differently.$note Check whether a file in $CDIR (overlays/, .env, docker-compose*.yml) changed since the services started; 'docker compose up --dry-run' is no help here, as it reports every pull_policy: always service as changed. $advice Nothing was changed."
}

# ---- start ------------------------------------------------------------------

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
        4) say "juicefs-init exit 4: an object key is empty or a placeholder." ;;
        5) say "juicefs-init exit 5: the bucket URL lacks http:// or https://." ;;
        6) say "juicefs-init exit 6: juicefs format failed (also when the Redis proxy does not answer); read the log lines above." ;;
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

take_lock() {
  LOCK="$CDIR/.setup-lock"
  mkdir "$LOCK" 2>/dev/null || { LOCK=""; die "another juicemount-setup is running ($CDIR/.setup-lock); if not, remove that folder."; }
}

start_and_report() {
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

# ---- main -------------------------------------------------------------------

main() {
  parse_args "$@"
  trap cleanup EXIT
  preflight
  locate_install
  CDIR="$ROOT/compose"
  [ -n "$HOST" ] || HOST="$(detect_host)"
  check_access
  check_project_owner
  STAGE="$(mktemp -d)"
  check_zfs_state

  local installed=0 started=0 sub subs datasets="" created="" s P
  [ -f "$CDIR/.env" ] && installed=1
  if [ "$installed" = 1 ]; then
    [ -z "$(project_containers)" ] || started=1
    if [ "$started" = 0 ]; then
      while read -r sub; do
        [ -n "$sub" ] || continue
        if has_content "$ROOT/$sub"; then started=1; break; fi
      done < <(data_subdirs "$CDIR/docker-compose.yml" "$ROOT/")
    fi
  fi

  if [ "$started" = 1 ]; then
    # A started install is never changed, and never upgraded.
    if { [ "$RELEASE" != latest ] && [ "$RELEASE" != "v$(env_get "$CDIR/.env" JM_SETUP_RELEASE)" ]; } ||
       { ! no_content_options && [ "$(options_signature)" != "$(env_get "$CDIR/.env" JM_SETUP_OPTIONS)" ]; }; then
      die "$ROOT is a started install. juicemount-setup never changes it, and upgrading a setup-made install is not supported yet. Run it again with no options, or with exactly the options of the first install; nothing was changed."
    fi
    DC="$CDIR/jm-compose"
    if [ ! -x "$DC" ]; then DC="$STAGE/jm-compose"; write_wrapper "$DC" "$CDIR"; fi
    check_env "$CDIR/.env"
    validate_compose ""
    check_no_drift
    check_redis_acl
    if [ "$DRY" = 1 ]; then
      [ -z "$(env_get "$CDIR/.env" JM_SETUP_RELEASE)" ] || secure_root check
      say "Dry run: $ROOT is a started install; a re-run starts what is stopped and changes nothing."; exit 0
    fi
    take_lock
    # Only a folder whose .env setup wrote is made private.
    [ -z "$(env_get "$CDIR/.env" JM_SETUP_RELEASE)" ] || secure_root fix
    say "JuiceMount is installed in $ROOT; checking it and starting what is stopped."
    start_and_report
    return
  fi

  # Fresh, or installed but never started: stage, render and check
  # everything before the first write.
  stage_release
  stage_files
  subs="$(data_subdirs "$STAGE/src/docker-compose.yml" "$COMPOSE_DATA_PREFIX")"
  [ -n "$subs" ] || die "docker-compose.yml mounts no data folder."
  if [ "$installed" = 0 ]; then
    for sub in $subs; do
      if has_content "$ROOT/$sub"; then
        die "$ROOT/$sub already holds data, but $CDIR/.env (its secrets) is missing. juicemount-setup will not generate new secrets over existing data. Restore .env from your backup, or choose an empty --dir or --pool; nothing was changed."
      fi
    done
    if holds_files "$ROOT"; then
      die "$ROOT already holds files but no JuiceMount settings ($CDIR/.env). juicemount-setup installs only into an empty folder; choose an empty --dir or --pool. Nothing was changed."
    fi
  fi
  for sub in $subs compose; do
    if [ "$MODE" = zfs ]; then
      zfs_exists "$ROOT_DS/$sub" && continue
      if has_content "$ROOT/$sub"; then die "$ROOT/$sub is a folder with content, not a dataset; a new dataset would hide it. Nothing was changed."; fi
      datasets="$datasets $ROOT_DS/$sub"
    else
      [ -d "$ROOT/$sub" ] || created="$created $sub"
    fi
  done
  if [ "$MODE" = zfs ] && ! zfs_exists "$ROOT_DS"; then datasets=" $ROOT_DS$datasets"; fi

  P="$STAGE/compose"
  mkdir -p "$P/minio" "$P/overlays"
  render_compose "$STAGE/src/docker-compose.yml" "$P/docker-compose.yml"
  for s in "${SUPPORT_FILES[@]}"; do [ "$s" = docker-compose.yml ] || cp "$STAGE/src/$s" "$P/$s"; done
  if [ -n "$OVERRIDE" ]; then cp "$OVERRIDE" "$P/docker-compose.override.yml"
  elif [ -f "$CDIR/docker-compose.override.yml" ]; then cp "$CDIR/docker-compose.override.yml" "$P/"; fi
  [ ! -d "$CDIR/overlays" ] || cp -R "$CDIR/overlays/." "$P/overlays/"
  CAPACITY="$(storage_capacity)"
  [[ "$CAPACITY" =~ ^[0-9]+$ ]] && [ "$CAPACITY" -gt 0 ] || die "could not read the storage size for $ROOT."
  if [ "$installed" = 1 ]; then write_check_env "$P/.env" "$CDIR/.env"; else write_check_env "$P/.env"; fi
  DC="$P/jm-compose"
  write_wrapper "$DC" "$P"
  validate_compose "$ROOT"
  check_no_other_juicemount
  [ -n "$(project_containers)" ] || check_ports_free

  say ""
  say "JuiceMount setup"
  if [ "$MODE" = zfs ]; then say "  Storage:   ZFS pool $POOL, folder $ROOT"; else say "  Storage:   folder $ROOT"; fi
  [ -z "$datasets" ] || say "  Creates:   datasets$datasets"
  [ -z "$created" ] || say "  Creates:   folders under $ROOT:$created"
  if [ -n "$SOURCE" ]; then
    say "  Compose:   $SOURCE/docker-compose.yml (developer mode)"
    say "  Images:    release $RELEASE_NAME"
  else
    say "  Release:   $RELEASE_NAME (commit $COMMIT): compose, files and image digests"
  fi
  if [ "$installed" = 1 ]; then
    say "  Secrets:   kept from $CDIR/.env (this install has not started yet)"
  else
    say "  Secrets:   generated now, stored in $CDIR/.env (mode 0600)"
  fi
  [ -z "$OVERRIDE" ] || say "  Override:  $OVERRIDE"
  say "  Import:    $(import_lines | sed 's/:ro$//' | tr '\n' ' ')(read-only)"
  say "  Ports:     $(ports_summary)"
  say "  Manager:   http://$HOST:30190"
  [ "$MODE" != zfs ] || say "  Note:      exclude $ROOT_DS/bucket from recursive snapshot tasks (INSTALL-TrueNAS.md)."
  say ""
  [ "$DRY" = 0 ] || { say "Dry run: every check passed; nothing was changed."; exit 0; }
  confirm

  # ---- first write ----
  if [ "$MODE" = zfs ]; then
    # POSIX ACLs with aclmode=discard (the TrueNAS "Generic" preset): an
    # NFSv4/SMB parent must not pass on ACL entries that mode 0700 hides.
    local -a zopts=(-o acltype=posixacl)
    if zfs get -H -o value aclmode "$POOL" >/dev/null 2>&1; then zopts+=(-o aclmode=discard); fi
    for s in $datasets; do zfs create -p "${zopts[@]}" "$s" || die "could not create dataset $s."; done
    [ "$(zfs_mountpoint "$ROOT_DS")" = "$ROOT" ] || die "dataset $ROOT_DS is not mounted at $ROOT."
  fi
  mkdir -p "$ROOT" || die "could not create $ROOT (run this command as root, or pass --dir)."
  # 00700, not 700: chmod keeps a directory's setgid bit (inherited from a
  # setgid parent) unless the mode has a fifth digit.
  chmod 00700 "$ROOT" || die "could not make $ROOT private (chmod 00700); on TrueNAS check the dataset's ACL mode."
  [ $((8#$(stat -c %a "$ROOT") & 8#077)) -eq 0 ] || die "$ROOT is mode $(stat -c %a "$ROOT") after chmod 00700, so other users can still enter it; check its ACL."
  for sub in $subs; do mkdir -p "$ROOT/$sub"; done
  [ ${#IMPORTS[@]} -gt 0 ] || mkdir -p "$ROOT/import"
  mkdir -p "$CDIR/minio" "$CDIR/overlays"
  chmod 700 "$CDIR"
  take_lock
  for s in "${SUPPORT_FILES[@]}"; do
    cp "$P/$s" "$CDIR/$s.new" && mv -f "$CDIR/$s.new" "$CDIR/$s"
  done
  [ ! -f "$P/docker-compose.override.yml" ] || cp "$P/docker-compose.override.yml" "$CDIR/docker-compose.override.yml"
  chmod 644 "$CDIR"/docker-compose*.yml "$CDIR"/redis.conf "$CDIR"/redis.acl "$CDIR"/minio/*
  chmod 755 "$CDIR/minio" "$CDIR"/minio/*.sh
  write_wrapper "$CDIR/jm-compose" "$CDIR"
  DC="$CDIR/jm-compose"
  if [ "$installed" = 0 ]; then write_env "$CDIR/.env"; else update_env_release "$CDIR/.env"; fi
  check_env "$CDIR/.env"
  dc config --quiet || die "the installed compose configuration is invalid (message above)."
  start_and_report
}

# A download cut short must not run: bash runs this brace group only once it
# has read the closing brace, so a partial last line is a syntax error.
{ main "$@"; exit $?; }
