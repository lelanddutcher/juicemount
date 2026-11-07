#!/usr/bin/env bash
# Offline tests for juicemount-setup.sh. No Docker daemon, network, ZFS or
# TrueNAS is contacted: docker, curl, zfs, zpool and midclt are fakes that log
# their calls. The end-to-end run against real Docker is the CI job
# `setup-e2e` in .github/workflows/ci.yml.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$HERE/juicemount-setup.sh"
SERVER="$(cd "$HERE/.." && pwd)"
WORK="$(mktemp -d)"
LISTENER=""
trap '[ -z "$LISTENER" ] || kill "$LISTENER" 2>/dev/null; rm -rf "$WORK"' EXIT
export FAKE_LOG="$WORK/calls.log" FAKE_DIR="$WORK/fake" FAKE_SERVER="$SERVER"
export FAKE_COMMIT=1111111111111111111111111111111111111111
mkdir -p "$WORK/bin" "$FAKE_DIR"

pass=0
ok() { pass=$((pass + 1)); printf 'ok %d - %s\n' "$pass" "$1"; }
fail() { printf 'not ok - %s\n' "$1" >&2; [ -f "$WORK/err" ] && sed 's/^/# /' "$WORK/err" >&2; exit 1; }

# ---- fixtures -----------------------------------------------------------
hex64() { printf "$1%.0s" {1..64}; }
write_release() { # env-manager-char env-farm-char version
  {
    echo "JM_MANAGER_DIGEST=sha256:$(hex64 "$1")"
    echo "JM_FARM_DIGEST=sha256:$(hex64 "$2")"
    echo "JM_FARM_RENDER_DIGEST=sha256:$(hex64 c)"
    echo "JM_RESPPROXY_DIGEST=sha256:$(hex64 d)"
  } > "$FAKE_DIR/server-images.env"
  cat > "$FAKE_DIR/release-manifest.json" <<JSON
{
  "build_number": "99",
  "commit": "$FAKE_COMMIT",
  "server_images": {
    "JM_FARM_DIGEST": {
      "digest": "sha256:$(hex64 b)",
      "source_commit": "$FAKE_COMMIT"
    },
    "JM_FARM_RENDER_DIGEST": {
      "digest": "sha256:$(hex64 c)"
    },
    "JM_MANAGER_DIGEST": {
      "digest": "sha256:$(hex64 a)"
    },
    "JM_RESPPROXY_DIGEST": {
      "digest": "sha256:$(hex64 d)"
    }
  },
  "version": "${3:-9.9.9}"
}
JSON
  (cd "$FAKE_DIR" && sha256sum release-manifest.json server-images.env > SHA256SUMS)
}

# ---- fakes --------------------------------------------------------------
cat > "$WORK/bin/curl" <<'SH'
#!/usr/bin/env bash
# Fake curl: release assets from $FAKE_DIR, repository files at
# $FAKE_COMMIT from $FAKE_SERVER, the Manager probe answers $FAKE_HTTP.
printf 'curl %s\n' "$*" >> "$FAKE_LOG"
out="" url="" fmt=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift 2 ;;
    -w) fmt="$2"; shift 2 ;;
    -m|-H|--proto|--retry) shift 2 ;;
    -*) shift ;;
    *) url="$1"; shift ;;
  esac
done
raw="https://raw.githubusercontent.com/lelanddutcher/juicemount/$FAKE_COMMIT/"
case "$url" in
  http://127.0.0.1:*) cat >/dev/null; printf '%s' "${FAKE_HTTP:-200}"; exit 0 ;;
  */releases/*/SHA256SUMS) src="$FAKE_DIR/SHA256SUMS" ;;
  */releases/*/server-images.env) src="$FAKE_DIR/server-images.env" ;;
  */releases/*/release-manifest.json) src="$FAKE_DIR/release-manifest.json" ;;
  "${raw}server/scripts/juicemount-setup.sh")
    if [ -n "${FAKE_OLD_RELEASE:-}" ]; then printf 404; else printf 200; fi; exit 0 ;;
  "${raw}server/"*) src="$FAKE_SERVER/${url#"${raw}server/"}" ;;
  *) echo "fake curl: unexpected $url" >&2; [ -z "$fmt" ] || printf 000; exit 22 ;;
esac
[ -f "$src" ] || exit 22
cp "$src" "$out"
SH

cat > "$WORK/bin/docker" <<'SH'
#!/usr/bin/env bash
# Fake docker. $FAKE_DIR/running holds the project folder once `up` ran.
printf 'docker %s\n' "$*" >> "$FAKE_LOG"
case "$1" in
  info) exit 0 ;;
  ps)
    case "$*" in
      *com.docker.compose.service=redis*) [ ! -f "$FAKE_DIR/running" ] || [ -n "${FAKE_NO_REDIS:-}" ] || echo cid-redis ;;
      *) [ -f "$FAKE_DIR/running" ] && echo cid-1 ;;
    esac
    [ "$*" != "ps -aq" ] || [ -z "${FAKE_OTHER:-}" ] || echo cid-other
    exit 0 ;;
  cp)
    # docker cp <id>:/run/jm-redis/users.acl -  -> a tar stream of the file
    [ -n "${FAKE_RENDERED_ACL:-}" ] || exit 1
    t="$(mktemp -d)"; printf '%s\n' "$FAKE_RENDERED_ACL" > "$t/users.acl"
    tar -cf - -C "$t" users.acl; rm -rf "$t"; exit 0 ;;
  inspect)
    case "$*" in
      *working_dir*) cat "$FAKE_DIR/running" ;;
      *config-hash*)
        if [ "${*: -1}" = cid-redis ]; then echo "${FAKE_HAVE_HASH_REDIS:-hash-1}"; else echo "${FAKE_HAVE_HASH:-hash-1}"; fi ;;
      *State.Running*) echo "${FAKE_REDIS_RUNNING:-true}" ;;
      *Config.Image*)
        for id in "${@:4}"; do
          case "$id" in
            cid-1) echo 'juicemount|ghcr.io/lelanddutcher/juicemount-manager@sha256:x|/ours' ;;
            cid-other) printf '%s\n' "$FAKE_OTHER" ;;
          esac
        done ;;
      *ExitCode*) [ "${*: -1}" = cid-juicefs-init ] && echo "${FAKE_INIT_EXIT:-0}" || echo 0 ;;
    esac
    exit 0 ;;
  compose) ;;
  *) exit 0 ;;
esac
shift
if [ "$1" = version ]; then [ "${2:-}" != --short ] || echo "${FAKE_COMPOSE_VERSION:-5.0.0}"; exit 0; fi
for v in JM_ADMIN_KEY COMPOSE_PROFILES COMPOSE_FILE JM_RESPPROXY_MODE JM_AUDIT JM_OBJ_PER_MAC_SERVER JM_SMART_FOLDERS; do
  [ -z "${!v+x}" ] || echo "LEAK $v" >> "$FAKE_LOG"
done
files=() dir="" envf=""
while [ $# -gt 0 ]; do
  case "$1" in
    --project-name) shift 2 ;;
    --project-directory) dir="$2"; shift 2 ;;
    --env-file) envf="$2"; shift 2 ;;
    -f) files+=("$2"); shift 2 ;;
    *) break ;;
  esac
done
cmd="$1"; shift
case "$cmd $*" in
  "config --quiet")
    grep -q '^JM_ADMIN_KEY=.\{32,\}$' "$envf" || { echo 'required variable JM_ADMIN_KEY is missing' >&2; exit 1; } ;;
  "config --images")
    grep -h '^[[:space:]]*image:' "${files[@]}" | sed 's/^[[:space:]]*image:[[:space:]]*//' |
      sed 's/\${\(JM_[A-Z_]*_DIGEST\):?[^}]*}/<\1>/' | while read -r img; do
        case "$img" in *'<'*'>'*)
          k="${img#*<}"; k="${k%>*}"; v="$(sed -n "s/^$k=//p" "$envf")"; img="${img%%<*}$v" ;;
        esac
        echo "$img"
      done ;;
  "config --hash *") printf 'juicemount-manager hash-1\nredis hash-1\n' ;;
  "config ")
    grep -v '^[[:space:]]*#' "${files[0]}" | sed -n 's/^[[:space:]]*- \([./][^:]*\):.*/\1/p' | while read -r s; do
      case "$s" in ./*) s="$dir/${s#./}" ;; esac
      echo "        source: $s"
    done
    printf '      - mode: ingress\n        target: 80\n        published: "%s"\n        protocol: %s\n' "${FAKE_PORT:-1}" "${FAKE_PORT_PROTO:-tcp}" ;;
  "pull --quiet") [ "${FAKE_FAIL:-}" = pull ] && { echo 'pull denied' >&2; exit 1; }; exit 0 ;;
  "up -d")
    [ "${FAKE_FAIL:-}" = up ] && { echo 'juicefs-init did not complete successfully' >&2; exit 1; }
    printf '%s\n' "$dir" > "$FAKE_DIR/running" ;;
  "ps -aq "*) echo "cid-${*: -1}" ;;
  "port juicemount-manager 8080") echo 0.0.0.0:30190 ;;
  logs*) echo '[precheck-3] empty object key' ;;
esac
exit 0
SH

# Setup needs root for ZFS; the fake root lives in its own folder so a test
# can leave it out.
mkdir -p "$WORK/bin-root"
cat > "$WORK/bin-root/id" <<'SH'
#!/usr/bin/env bash
if [ "${1:-}" = -u ]; then echo 0; else exec /usr/bin/id "$@"; fi
SH
chmod +x "$WORK/bin-root/id"
cat > "$WORK/bin/midclt" <<'SH'
#!/usr/bin/env bash
exit 0
SH
cat > "$WORK/bin/zpool" <<'SH'
#!/usr/bin/env bash
# Fake pools: boot-pool (largest, must be skipped), tank, big.
printf 'zpool %s\n' "$*" >> "$FAKE_LOG"
pools="boot-pool tank big"
case "$*" in
  "list -Hp -o name,free") printf 'boot-pool 9000000000000\ntank 100\nbig 5000\n' ;;
  "list -H -o name") printf '%s\n' $pools ;;
  "list -H -o name "*) [[ " $pools " == *" ${*: -1} "* ]] ;;
  "list -Hp -o size "*) echo 7000000000000 ;;
  *) exit 1 ;;
esac
SH
cat > "$WORK/bin/zfs" <<'SH'
#!/usr/bin/env bash
# Fake datasets in $FAKE_DIR/datasets; dataset N is mounted at
# $FAKE_DIR/mnt/N. $FAKE_UNMOUNTED / $FAKE_LOCKED name one dataset.
printf 'zfs %s\n' "$*" >> "$FAKE_LOG"
db="$FAKE_DIR/datasets"; touch "$db"
known() { case "$1" in boot-pool|tank|big) return 0 ;; esac; grep -qx "$1" "$db"; }
state() { # name [with acltype]
  local m=yes k=- a=posix
  [ "$1" != "${FAKE_UNMOUNTED:-}" ] || m=no
  [ "$1" != "${FAKE_LOCKED:-}" ] || { m=no; k=unavailable; }
  [ "$1" != "${FAKE_NFSV4:-}" ] || a=nfsv4
  if [ -n "${2:-}" ]; then printf '%s\t%s\t%s\t%s\n' "$1" "$m" "$k" "$a"; else printf '%s\t%s\t%s\n' "$1" "$m" "$k"; fi
}
n="${*: -1}"
case "$*" in
  "get -H -o value mountpoint "*) known "$n" && echo "$FAKE_DIR/mnt/$n" ;;
  "list -H -o name "*) known "$n" ;;
  "list -H -o name,mounted,keystatus -d 0 "*) known "$n" && state "$n" ;;
  "get -H -o value aclmode "*) [ -z "${FAKE_NO_ACLMODE:-}" ] && known "$n" && echo discard ;;
  "list -H -o name,mounted,keystatus,acltype -r "*)
    known "$n" || exit 1
    state "$n" a; grep "^$n/" "$db" | while read -r c; do state "$c" a; done ;;
  "list -H -o name,mounted,keystatus -r "*)
    known "$n" || exit 1
    state "$n"; grep "^$n/" "$db" | while read -r c; do state "$c"; done ;;
  "create -p "*) echo "$n" >> "$db"; mkdir -p "$FAKE_DIR/mnt/$n"; chmod g+s "$FAKE_DIR/mnt/$n" ;;
  *) exit 1 ;;
esac
SH
# docker runs under the setup's `env -i`, so every fake reads its settings
# from a file the run helpers write (the FAKE_* variables of that run).
for f in "$WORK/bin/"*; do sed -i "2i . '$WORK/fake.env'" "$f"; done
chmod +x "$WORK/bin/"*
PLAIN="$WORK/bin-plain"
mkdir -p "$PLAIN"
for t in curl docker; do ln -sf "$WORK/bin/$t" "$PLAIN/$t"; done

# run: TrueNAS fakes on PATH. run_plain: a plain Docker host.
save_fakes() { export -p | grep -E '^declare -x FAKE_' > "$WORK/fake.env" || true; }
run() {
  save_fakes
  set +e
  PATH="$WORK/bin-root:$WORK/bin:$PATH" setsid bash "$SCRIPT" --host nas.example "$@" > "$WORK/out" 2> "$WORK/err" < /dev/null
  rc=$?
  set -e
}
run_plain() {
  save_fakes
  set +e
  PATH="$PLAIN:$PATH" setsid bash "$SCRIPT" --host nas.example "$@" > "$WORK/out" 2> "$WORK/err" < /dev/null
  rc=$?
  set -e
}
reset() {
  rm -rf "$FAKE_DIR" "$WORK/data"; mkdir -p "$FAKE_DIR"; : > "$FAKE_LOG"; write_release a b
  unset FAKE_FAIL FAKE_HTTP FAKE_INIT_EXIT FAKE_OLD_RELEASE FAKE_OTHER FAKE_PORT FAKE_PORT_PROTO FAKE_HAVE_HASH FAKE_UNMOUNTED FAKE_LOCKED FAKE_COMPOSE_VERSION
  unset FAKE_NO_REDIS FAKE_RENDERED_ACL FAKE_HAVE_HASH_REDIS FAKE_REDIS_RUNNING FAKE_NFSV4 FAKE_NO_ACLMODE
}
envval() { sed -n "s/^$2=//p" "$1"; }
secrets_of() { grep -E '^(JM_ADMIN_KEY|MINIO_ROOT_PASSWORD|JM_[A-Z_]*_PASSWORD|JM_OBJ_[A-Z]*_SECRET_KEY)=' "$1"; }
nothing_written() { [ ! -e "$1" ] && ! grep -q 'compose .* up\|zfs create' "$FAKE_LOG"; }
D="$WORK/data"

# 1. Fresh install in a folder: files from the release's commit, digests
#    from its manifest; a caller's compose variables never reach compose.
reset
JM_ADMIN_KEY=caller-value-must-not-reach-compose-000000 COMPOSE_PROFILES=per-mac COMPOSE_FILE=/x.yml \
  JM_RESPPROXY_MODE=enforce JM_AUDIT=on JM_OBJ_PER_MAC_SERVER=1 JM_SMART_FOLDERS=on run_plain --yes --dir "$D"
[ "$rc" = 0 ] || fail "fresh install exits 0 (rc=$rc)"
[ "$(wc -l < "$WORK/out")" = 2 ] || fail "stdout is exactly two lines"
grep -qx 'Manager URL:  http://nas.example:30190' "$WORK/out" || fail "Manager URL line"
key="$(sed -n 's/^Sign-in key:  //p' "$WORK/out")"
[[ "$key" =~ ^[0-9a-f]{32}$ ]] || fail "sign-in key is 32 hex characters"
ok "fresh install prints exactly the Manager URL and the sign-in key"

grep -q "raw.githubusercontent.com/lelanddutcher/juicemount/$FAKE_COMMIT/server/docker-compose.yml" "$FAKE_LOG" &&
  ! grep -q 'juicemount/main/' "$FAKE_LOG" || fail "files come from the release commit, not main"
ok "compose and every mounted file come from the manifest's commit"

! grep -q LEAK "$FAKE_LOG" || fail "a caller variable reached docker compose: $(grep LEAK "$FAKE_LOG" | sort -u | tr '\n' ' ')"
ok "the caller's environment (COMPOSE_*, JM_*) never reaches docker compose"

E="$D/compose/.env"
[ "$(stat -c %a "$D/compose")" = 700 ] && [ "$(stat -c %a "$E")" = 600 ] || fail "compose dir 0700, .env 0600"
[ "$(envval "$E" JM_ADMIN_KEY)" = "$key" ] || fail "printed key is the stored key"
for kv in MINIO_ROOT_PASSWORD:40 JM_REDIS_PROXY_PASSWORD:64 JM_PROXY_FARM_PASSWORD:64 JM_PROXY_MANAGER_PASSWORD:64 \
  JM_PROXY_BG_PASSWORD:64 JM_PROXY_WEBDAV_PASSWORD:64 JM_PROXY_ADMIN_PASSWORD:64 JM_OBJ_CLIENT_SECRET_KEY:40 JM_OBJ_GC_SECRET_KEY:40; do
  v="$(envval "$E" "${kv%%:*}")"
  [[ "$v" =~ ^[0-9a-f]+$ ]] && [ ${#v} = "${kv##*:}" ] || fail "${kv%%:*} is ${kv##*:} hex characters"
done
[ "$(secrets_of "$E" | cut -d= -f2 | sort | uniq -d)" = "" ] || fail "every secret is distinct"
[ "$(envval "$E" JM_MANAGER_DIGEST)" = "sha256:$(hex64 a)" ] && [ "$(envval "$E" JM_FARM_DIGEST)" = "sha256:$(hex64 b)" ] || fail "digests from the release"
[ "$(envval "$E" JM_SETUP_RELEASE)" = 9.9.9 ] && [ "$(envval "$E" JM_SETUP_COMMIT)" = "$FAKE_COMMIT" ] || fail "release and commit recorded"
[[ "$(envval "$E" JM_FARM_STORAGE_CAPACITY_BYTES)" =~ ^[1-9][0-9]*$ ]] || fail "capacity is a positive integer"
[ "$(envval "$E" JM_FARM_NODE_META_URL)" = 'redis://farm@nas.example:30179/1' ] || fail "farm node URL is user@host only"
ok "secrets have the lengths the stack requires; digests, release and commit recorded; file modes"

C="$D/compose/docker-compose.yml"
grep -v '^[[:space:]]*#' "$C" > "$WORK/active.yml"
! grep -q '/mnt/zSSD\|/mnt/storage0' "$WORK/active.yml" || fail "no sample path left"
grep -q -- "- $D/redis:/data" "$C" && grep -q -- "- $D/import:/sources/import:ro" "$C" || fail "paths rendered into the data folder"
for f in redis.conf redis.acl minio/provision-object-keys.sh minio/jm-gc-policy.json; do cmp -s "$SERVER/$f" "$D/compose/$f" || fail "$f copied"; done
diff <(grep -v '^[[:space:]]*-.*:/' "$SERVER/docker-compose.yml" | grep -v '/mnt/zSSD') \
     <(grep -v '^[[:space:]]*-.*:/' "$C" | grep -v "$D") > /dev/null || fail "only host paths differ from the canonical compose"
for s in bucket redis cache manager-state respproxy objkeys headscale juicefarm-state import compose/overlays; do [ -d "$D/$s" ] || fail "$s created"; done
[ -x "$D/compose/jm-compose" ] || fail "compose wrapper installed"
grep -q SSH_AUTH_SOCK "$D/compose/jm-compose" || fail "wrapper keeps SSH_AUTH_SOCK for ssh:// Docker hosts"
[ "$(stat -c %a "$D")" = 700 ] || fail "data root is 0700 (was $(stat -c %a "$D"))"
[ "$(envval "$E" JM_SETUP_COMPOSE_VERSION)" = 5.0.0 ] || fail "compose version recorded"
grep -q '^  Ports:     1/tcp $' "$WORK/err" || fail "plan shows the ports the compose publishes"
ok "compose rendered (only host paths change), mounted files copied, folders and wrapper created"

# 2. Re-runs: nothing regenerated, same key, nothing downloaded.
sum="$(cd "$D/compose" && sha256sum .env docker-compose.yml redis.conf minio/*)"
: > "$FAKE_LOG"
run_plain --dir "$D"
[ "$rc" = 0 ] || fail "re-run exits 0 (rc=$rc)"
[ "$(cd "$D/compose" && sha256sum .env docker-compose.yml redis.conf minio/*)" = "$sum" ] || fail "re-run leaves every file unchanged"
[ "$(sed -n 's/^Sign-in key:  //p' "$WORK/out")" = "$key" ] || fail "re-run prints the same key"
! grep -q 'raw.githubusercontent\|releases/' "$FAKE_LOG" || fail "re-run downloads nothing"
: > "$FAKE_LOG"
run_plain
[ "$rc" = 0 ] && [ "$(cd "$D/compose" && sha256sum .env docker-compose.yml redis.conf minio/*)" = "$sum" ] || fail "re-run without options finds the install"
chmod 755 "$D"
run_plain --dir "$D" --release v9.9.9
[ "$rc" = 0 ] && [ "$(stat -c %a "$D")" = 700 ] && grep -q "was mode 755; it is now 700" "$WORK/err" || fail "re-run with --release of the installed release, root made private again"
[ "$(cd "$D/compose" && sha256sum .env docker-compose.yml redis.conf minio/*)" = "$sum" ] || fail "that re-run changes no file"
ok "re-runs (with --dir, with no options, with --release of the installed release) change nothing; a loosened root is made 0700 again"

# 3. Started install: identical options accepted, other options refused.
run_plain --dir "$D" --release v1.2.3
[ "$rc" = 1 ] && grep -q 'upgrading a setup-made install is not supported yet' "$WORK/err" || fail "--release on a started install refused"
run_plain --dir "$D" --override "$SERVER/docker-compose.link.yml"
[ "$rc" = 1 ] && grep -q 'never changes it' "$WORK/err" || fail "--override on a started install refused"
[ "$(cd "$D/compose" && sha256sum .env docker-compose.yml redis.conf minio/*)" = "$sum" ] || fail "refusals change nothing"
ok "a started install refuses options that would change or upgrade it"

# 4. Overlays in compose/overlays are always applied.
cp "$SERVER/docker-compose.link.yml" "$D/compose/overlays/docker-compose.link.yml"
: > "$FAKE_LOG"
run_plain --dir "$D"
[ "$rc" = 0 ] && grep -q -- "-f $D/compose/overlays/docker-compose.link.yml up -d" "$FAKE_LOG" || fail "overlay applied on re-run"
ok "a re-run keeps overlays from compose/overlays/ (e.g. Link)"

# 5. Drift: a service made from other files is never recreated.
FAKE_HAVE_HASH=hash-by-hand run_plain --dir "$D"
[ "$rc" = 1 ] && grep -q 'running services juicemount-manager do not match' "$WORK/err" && grep -q 'Docker Compose was upgraded' "$WORK/err" &&
  ! grep -q 'cause 2 is likely' "$WORK/err" || fail "drift refused, naming both causes"
FAKE_HAVE_HASH=hash-by-hand FAKE_COMPOSE_VERSION=6.1.0 run_plain --dir "$D"
[ "$rc" = 1 ] && grep -q 'Docker Compose was 5.0.0 when this install was made and is 6.1.0 now, so cause 2 is likely' "$WORK/err" || fail "drift names a changed compose version"
ok "a re-run refuses when a running service differs from the install's files"
rm "$D/compose/overlays/docker-compose.link.yml"
FAKE_HAVE_HASH_REDIS=hash-by-hand FAKE_HAVE_HASH=hash-by-hand run_plain --dir "$D"
[ "$rc" = 1 ] && grep -q 'Redis is among them: do not run a bare' "$WORK/err" &&
  grep -q -- "jm-compose up -d --no-deps juicemount-manager'" "$WORK/err" && grep -q 'aof_last_bgrewrite_status:ok' "$WORK/err" &&
  ! grep -q "If the files are as intended, '$D/compose/jm-compose up -d' recreates" "$WORK/err" || fail "drift including Redis names the safe order"
ok "when Redis drifted, the advice is the safe order, never a bare up -d"

# 9b. A started install never starts a Redis that would replay under the
#     0.5.0 ACL; a running one is left alone with a warning.
OLD_ACL='user default off resetpass resetkeys resetchannels -@all'
NEW_ACL='user default off resetpass resetkeys resetchannels ~* &* +@all'
cp "$D/compose/redis.acl" "$WORK/redis.acl.new"
sed -i "s/^user default .*/$OLD_ACL/" "$D/compose/redis.acl"
sum_acl="$(cd "$D/compose" && sha256sum redis.acl .env)"
: > "$FAKE_LOG"
FAKE_REDIS_RUNNING=false run_plain --dir "$D"
[ "$rc" = 1 ] && grep -q 'Redis is not running, and .*redis.acl has the 0.5.0 ACL' "$WORK/err" && grep -q 'Copy .*/redis (its appendonlydir)' "$WORK/err" &&
  grep -q 'aof_rewrite_scheduled:0, aof_rewrite_in_progress:0 and aof_last_bgrewrite_status:ok' "$WORK/err" &&
  ! grep -q ' up -d' "$FAKE_LOG" || fail "stopped Redis with the 0.5.0 ACL file is not started"
FAKE_REDIS_RUNNING=false FAKE_NO_REDIS=1 run_plain --dir "$D"
[ "$rc" = 1 ] && grep -q 'Redis is not running' "$WORK/err" || fail "no Redis container and the 0.5.0 ACL file: refused"
FAKE_REDIS_RUNNING=true run_plain --dir "$D"
[ "$rc" = 0 ] && grep -q 'WARNING: .*redis.acl has the 0.5.0 ACL, and Redis will use it at its next start' "$WORK/err" || fail "running Redis with the 0.5.0 ACL: warning, no refusal"
[ "$(cd "$D/compose" && sha256sum redis.acl .env)" = "$sum_acl" ] || fail "setup never replaces the ACL on a started install"
cp "$WORK/redis.acl.new" "$D/compose/redis.acl"
: > "$FAKE_LOG"
FAKE_REDIS_RUNNING=false FAKE_RENDERED_ACL="$OLD_ACL" run_plain --dir "$D"
[ "$rc" = 1 ] && grep -q 'it was last started with the 0.5.0 ACL. That start may already have dropped transactions' "$WORK/err" && ! grep -q ' up -d' "$FAKE_LOG" || fail "stopped Redis last started with the 0.5.0 ACL is not started"
FAKE_REDIS_RUNNING=true FAKE_RENDERED_ACL="$OLD_ACL" run_plain --dir "$D"
[ "$rc" = 0 ] && grep -q 'WARNING: Redis is running with the 0.5.0 ACL' "$WORK/err" || fail "Redis running with the 0.5.0 ACL: warning"
: > "$FAKE_LOG"
FAKE_REDIS_RUNNING=false FAKE_RENDERED_ACL="$NEW_ACL" run_plain --dir "$D"
[ "$rc" = 0 ] && grep -q ' up -d' "$FAKE_LOG" || fail "stopped Redis with the fixed ACL is started"
ok "a stopped Redis under the 0.5.0 ACL (file or last start) is never started; a running one gets a warning; the ACL file is never replaced"

# 5b. Another folder with the same project name: never taken over, either
#     by a fresh install or by a re-run of an install in another folder.
mkdir -p "$WORK/new-empty"
run_plain --yes --dir "$WORK/new-empty"
[ "$rc" = 1 ] && grep -q "Compose project 'juicemount' already runs from $D/compose, not $WORK/new-empty/compose" "$WORK/err" &&
  [ -z "$(ls -A "$WORK/new-empty")" ] || fail "fresh install over a running project of the same name refused"
cp -a "$D" "$WORK/copy"
run_plain --dir "$WORK/copy"
[ "$rc" = 1 ] && grep -q 'already runs from' "$WORK/err" || fail "re-run of another folder's install refused"
rm -rf "$WORK/copy" "$WORK/new-empty"
ok "a project name already running from another folder is never taken over"

# 5c. Not root: a private compose folder is reported as such.
if [ "$(id -u)" != 0 ]; then
  chmod 000 "$D/compose"
  run_plain --dir "$D"
  chmod 700 "$D/compose"
  [ "$rc" = 1 ] && grep -q 'cannot read it (it is private to root): run this command as root' "$WORK/err" || fail "unreadable compose folder: run as root"
  set +e
  PATH="$WORK/bin:$PATH" setsid bash "$SCRIPT" --host nas.example --yes --pool tank > "$WORK/out" 2> "$WORK/err" < /dev/null; rc=$?
  set -e
  [ "$rc" = 1 ] && grep -q 'ZFS datasets needs root' "$WORK/err" || fail "ZFS without root refused"
  ok "without root: an unreadable install folder and ZFS mode say to run as root"
fi

# 5d. Found through the Docker label: checked like --dir; made private only
#     when setup wrote its .env.
if [ "$(id -u)" != 0 ]; then
  chmod 000 "$D"
  run_plain
  chmod 700 "$D"
  [ "$rc" = 1 ] && grep -q 'which this user cannot enter (it is private to root): run this command as root' "$WORK/err" || fail "label folder behind a private root: run as root"
fi
mkdir -p "$WORK/my data/compose"; touch "$WORK/my data/compose/.env"; cp "$FAKE_DIR/running" "$WORK/running.bak"
echo "$WORK/my data/compose" > "$FAKE_DIR/running"
run_plain
[ "$rc" = 1 ] && grep -q 'a path with characters juicemount-setup does not handle' "$WORK/err" || fail "label folder with unsafe characters refused"
cp "$WORK/running.bak" "$FAKE_DIR/running"; rm -rf "$WORK/my data"
cp "$E" "$WORK/env.bak2"; sed -i '/^JM_SETUP_RELEASE=/d' "$E"; chmod 755 "$D"
run_plain --dir "$D"
[ "$rc" = 0 ] && [ "$(stat -c %a "$D")" = 755 ] || fail "a folder whose .env setup did not write is not chmodded (rc=$rc)"
cp "$WORK/env.bak2" "$E"; chmod 700 "$D"
ok "a folder found by its Docker label is checked like --dir, says 'run as root' when private, and is chmodded only when setup wrote its .env"

# 6. A lost secret is refused, never regenerated.
cp "$E" "$WORK/env.bak"
sed -i '/^JM_PROXY_BG_PASSWORD=/d' "$E"
run_plain --dir "$D"
[ "$rc" = 1 ] && grep -q 'lacks a usable JM_PROXY_BG_PASSWORD' "$WORK/err" && ! grep -q '^JM_PROXY_BG_PASSWORD=' "$E" || fail "missing secret refused, not regenerated"
cp "$WORK/env.bak" "$E"
ok "an install missing a secret is refused, never regenerated"

# 7. Data without its .env: refused before anything is written.
reset
mkdir -p "$D/redis"; echo x > "$D/redis/appendonly.aof"
run_plain --yes --dir "$D"
[ "$rc" = 1 ] && grep -q 'already holds data' "$WORK/err" && nothing_written "$D/compose" || fail "existing data without .env refused"
reset
mkdir -p "$D/empty-skeleton"; echo notes > "$D/notes.txt"
run_plain --yes --dir "$D"
[ "$rc" = 1 ] && grep -q 'already holds files but no JuiceMount settings' "$WORK/err" && nothing_written "$D/compose" || fail "non-empty root folder refused"
rm "$D/notes.txt"
run_plain --dry-run --dir "$D"
[ "$rc" = 0 ] || fail "a root folder with only empty folders is accepted"
ok "existing data, or any file in the root folder, without its secrets is refused before anything is written"

# 8. Release checks: each digest tied to its own key, SHA256SUMS, age.
reset
write_release b a
run_plain --yes --dir "$D"
[ "$rc" = 1 ] && grep -q 'JM_MANAGER_DIGEST in server-images.env is not the digest release-manifest.json gives' "$WORK/err" && nothing_written "$D" || fail "swapped digests refused"
reset
sed -i 's/^JM_FARM_DIGEST=.*/JM_FARM_DIGEST=latest/' "$FAKE_DIR/server-images.env"
run_plain --yes --dir "$D"
[ "$rc" = 1 ] && grep -q 'does not match the release.s SHA256SUMS' "$WORK/err" && nothing_written "$D" || fail "file not matching SHA256SUMS refused"
(cd "$FAKE_DIR" && sha256sum release-manifest.json server-images.env > SHA256SUMS)
run_plain --yes --dir "$D"
[ "$rc" = 1 ] && grep -q 'not KEY=sha256' "$WORK/err" && nothing_written "$D" || fail "malformed digest refused"
reset
run_plain --yes --dir "$D" --release v9.9.8
[ "$rc" = 1 ] && grep -q 'release v9.9.8 has a release-manifest.json for version 9.9.9' "$WORK/err" && nothing_written "$D" || fail "manifest version must match --release"
FAKE_OLD_RELEASE=1 run_plain --yes --dir "$D"
[ "$rc" = 1 ] && grep -q 'predates juicemount-setup' "$WORK/err" && nothing_written "$D" || fail "old release refused"
ok "swapped digests, a file not in SHA256SUMS, a malformed digest, a version other than --release and a pre-setup release are refused"

# 8b. A release (or source) whose redis.acl has the 0.5.0 line is refused
#     before anything is written.
reset
cp -a "$SERVER" "$WORK/oldsrc"
sed -i "s/^user default .*/user default off resetpass resetkeys resetchannels -@all/" "$WORK/oldsrc/redis.acl"
run_plain --yes --dir "$D" --source "$WORK/oldsrc"
[ "$rc" = 1 ] && grep -q 'has the 0.5.0 default user line' "$WORK/err" && nothing_written "$D" || fail "0.5.0 redis.acl refused before writing"
run_plain --dry-run --dir "$D" --source "$WORK/oldsrc"
[ "$rc" = 1 ] && grep -q 'has the 0.5.0 default user line' "$WORK/err" || fail "dry run refuses the 0.5.0 redis.acl"
rm -rf "$WORK/oldsrc"
ok "a redis.acl with the 0.5.0 default line is refused before any write, also in a dry run"

# 8c. Under a setgid parent the root still ends up private.
reset
mkdir -p "$WORK/sgid"; chmod g+s "$WORK/sgid"
run_plain --yes --dir "$WORK/sgid/data"
[ "$rc" = 0 ] && [ "$(stat -c %a "$WORK/sgid/data")" = 700 ] || fail "install under a setgid parent (rc=$rc, mode $(stat -c %a "$WORK/sgid/data" 2>/dev/null))"
rm -f "$FAKE_DIR/running"; rm -rf "$WORK/sgid"
ok "under a setgid parent the root is created 0700 (setgid cleared)"

# 9. Without a terminal, --yes is required.
reset
run_plain --dir "$D"
[ "$rc" = 1 ] && grep -q 're-run with --yes' "$WORK/err" && nothing_written "$D" || fail "no terminal without --yes refused"
ok "without a terminal and --yes it stops before changing anything"

# 10. Every check runs before the first write, also in a dry run.
reset
run_plain --dry-run --dir "$D"
[ "$rc" = 0 ] && grep -q 'every check passed; nothing was changed' "$WORK/err" && nothing_written "$D" || fail "dry run writes nothing"
reset
FAKE_OTHER='ix-juicemount|ghcr.io/lelanddutcher/juicemount-manager@sha256:x|/ix-juicemount-juicemount-manager-1' run_plain --dry-run --dir "$D"
[ "$rc" = 1 ] && grep -q 'another JuiceMount is on this host' "$WORK/err" && nothing_written "$D" || fail "dry run refuses another JuiceMount"
FAKE_OTHER='|ghcr.io/lelanddutcher/jm-respproxy@sha256:x|/loose-proxy' run_plain --yes --dir "$D"
[ "$rc" = 1 ] && grep -q 'container /loose-proxy' "$WORK/err" && nothing_written "$D" || fail "loose JuiceMount container refused"
if command -v python3 >/dev/null 2>&1; then
  python3 -c 'import socket,sys,time
s=socket.socket(); s.bind(("127.0.0.1",0)); s.listen(); print(s.getsockname()[1], flush=True); time.sleep(120)' > "$WORK/port" &
  LISTENER=$!
  for _ in 1 2 3 4 5 6 7 8 9 10; do [ -s "$WORK/port" ] && break; sleep 0.2; done
  FAKE_PORT="$(cat "$WORK/port")" run_plain --dry-run --dir "$D"
  [ "$rc" = 1 ] && grep -q "port $(cat "$WORK/port")/tcp is already in use" "$WORK/err" || fail "dry run refuses a port in use"
  FAKE_PORT="$(cat "$WORK/port")" run_plain --yes --dir "$D"
  [ "$rc" = 1 ] && nothing_written "$D" || fail "port in use: nothing written"
  kill "$LISTENER"; LISTENER=""
  if command -v ss >/dev/null 2>&1; then
    python3 -c 'import socket,time
s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.bind(("0.0.0.0",0)); print(s.getsockname()[1], flush=True); time.sleep(120)' > "$WORK/uport" &
    LISTENER=$!
    for _ in 1 2 3 4 5 6 7 8 9 10; do [ -s "$WORK/uport" ] && break; sleep 0.2; done
    FAKE_PORT="$(cat "$WORK/uport")" FAKE_PORT_PROTO=udp run_plain --dry-run --dir "$D"
    [ "$rc" = 1 ] && grep -q "port $(cat "$WORK/uport")/udp is already in use" "$WORK/err" || fail "a UDP port in use on any address is refused"
    kill "$LISTENER"; LISTENER=""
  fi
fi
ok "another JuiceMount (ix-juicemount, loose containers) and a port in use are refused before any write, also in a dry run"

# 11. TrueNAS: datasets on the pool with most free space, never boot-pool.
reset
run --yes
[ "$rc" = 0 ] || fail "TrueNAS install exits 0 (rc=$rc)"
grep -qx 'zfs create -p .* big/juicemount' "$FAKE_LOG" && grep -qx 'zfs create -p .* big/juicemount/bucket' "$FAKE_LOG" &&
  grep -qx 'zfs create -p .* big/juicemount/compose' "$FAKE_LOG" || fail "datasets created on pool big"
! grep -q 'boot-pool/' "$FAKE_LOG" || fail "boot-pool untouched"
TE="$FAKE_DIR/mnt/big/juicemount/compose/.env"
[ "$(envval "$TE" JM_FARM_STORAGE_CAPACITY_BYTES)" = 7000000000000 ] || fail "capacity from zpool size"
: > "$FAKE_LOG"; rm -f "$FAKE_DIR/running"
run --pool tank --yes
[ "$rc" = 0 ] && grep -qx 'zfs create -p .* tank/juicemount' "$FAKE_LOG" || fail "--pool selects the pool"
grep -qx 'zfs create -p -o acltype=posixacl -o aclmode=discard tank/juicemount' "$FAKE_LOG" || fail "datasets created with POSIX ACLs and aclmode=discard"
[ "$(stat -c %a "$FAKE_DIR/mnt/tank/juicemount")" = 700 ] || fail "setgid dataset root made 0700"
reset
FAKE_NO_ACLMODE=1 run --pool tank --yes
[ "$rc" = 0 ] && grep -qx 'zfs create -p -o acltype=posixacl tank/juicemount' "$FAKE_LOG" || fail "without the aclmode property only acltype is set"
reset
printf 'tank/juicemount\n' > "$FAKE_DIR/datasets"; mkdir -p "$FAKE_DIR/mnt/tank/juicemount"
FAKE_NFSV4=tank/juicemount run --pool tank --yes
[ "$rc" = 1 ] && grep -q 'dataset tank/juicemount uses NFSv4 ACLs' "$WORK/err" && ! grep -q 'zfs create' "$FAKE_LOG" || fail "NFSv4 ACL dataset refused"
ok "TrueNAS: datasets on the pool with most free space (or --pool), POSIX ACLs and aclmode=discard, 0700 root; an NFSv4-ACL dataset is refused; capacity from zpool"

# 12. A non-empty root folder that the new dataset would hide.
reset
mkdir -p "$FAKE_DIR/mnt/tank/juicemount"; echo notes > "$FAKE_DIR/mnt/tank/juicemount/notes.txt"
run --pool tank --yes
[ "$rc" = 1 ] && grep -q 'juicemount is a folder with content, not a dataset; a new dataset tank/juicemount would hide it' "$WORK/err" &&
  ! grep -q 'zfs create' "$FAKE_LOG" || fail "non-empty root folder refused"
run --pool tank --dry-run
[ "$rc" = 1 ] && grep -q 'would hide it' "$WORK/err" || fail "dry run refuses the non-empty root folder"
ok "a non-empty root folder that a new dataset would hide is refused (also in a dry run)"

# 13. Unmounted or locked datasets look empty: refused.
reset
printf 'tank/juicemount\n' > "$FAKE_DIR/datasets"; mkdir -p "$FAKE_DIR/mnt/tank/juicemount"
FAKE_UNMOUNTED=tank/juicemount run --pool tank --yes
[ "$rc" = 1 ] && grep -q 'dataset tank/juicemount is not mounted' "$WORK/err" && ! grep -q 'zfs create' "$FAKE_LOG" || fail "unmounted dataset refused"
printf 'tank/juicemount/redis\n' >> "$FAKE_DIR/datasets"
FAKE_LOCKED=tank/juicemount/redis run --pool tank --yes
[ "$rc" = 1 ] && grep -q 'dataset tank/juicemount/redis is locked' "$WORK/err" || fail "locked child dataset refused"
reset
printf 'tank/juicemount\n' > "$FAKE_DIR/datasets"
FAKE_LOCKED=tank/juicemount run --yes
[ "$rc" = 1 ] && grep -q 'tank/juicemount exists but is locked or not mounted' "$WORK/err" && ! grep -q 'zfs create' "$FAKE_LOG" || fail "locked install on another pool stops the scan"
printf 'tank/juicemount/redis\n' >> "$FAKE_DIR/datasets"; mkdir -p "$FAKE_DIR/mnt/tank/juicemount/redis"
FAKE_LOCKED=tank/juicemount/redis run --yes
[ "$rc" = 1 ] && grep -q 'tank/juicemount/redis exists but is locked or not mounted' "$WORK/err" || fail "locked child dataset stops the scan"
reset
FAKE_LOCKED=tank run --pool tank --yes
[ "$rc" = 1 ] && grep -q 'pool tank is locked' "$WORK/err" || fail "locked pool refused"
ok "an unmounted or locked dataset or pool is refused, not taken for empty"

# 14. Failures name their cause and keep the settings.
reset
FAKE_FAIL=pull run_plain --yes --dir "$D"
[ "$rc" = 1 ] && grep -q 'could not download an image' "$WORK/err" && [ -f "$D/compose/.env" ] || fail "pull failure explained"
key1="$(envval "$D/compose/.env" JM_ADMIN_KEY)"
FAKE_FAIL=up FAKE_INIT_EXIT=4 run_plain --yes --dir "$D"
[ "$rc" = 1 ] && grep -q 'juicefs-init exit 4' "$WORK/err" && [ "$(envval "$D/compose/.env" JM_ADMIN_KEY)" = "$key1" ] || fail "juicefs-init exit code named, settings kept"
ok "an image that cannot be pulled, or a failed juicefs-init, stops with the cause and keeps the settings"

# 15. An install that never started takes a newer release and an override,
#     keeping every secret byte for byte.
rm -f "$FAKE_DIR/running"
secrets_before="$(secrets_of "$D/compose/.env")"
sed -i '/^JM_SETUP_COMPOSE_VERSION=/d' "$D/compose/.env"
write_release e b 9.9.10
sed -i "s/sha256:$(hex64 a)/sha256:$(hex64 e)/" "$FAKE_DIR/release-manifest.json"
(cd "$FAKE_DIR" && sha256sum release-manifest.json server-images.env > SHA256SUMS)
printf 'services:\n  minio:\n    image: example.invalid/store@sha256:%s\n' "$(hex64 f)" > "$WORK/override.yml"
: > "$FAKE_LOG"
run_plain --yes --dir "$D" --release v9.9.10 --override "$WORK/override.yml"
[ "$rc" = 0 ] || fail "never-started install re-run exits 0 (rc=$rc)"
[ "$(secrets_of "$D/compose/.env")" = "$secrets_before" ] || fail "secrets unchanged"
[ "$(envval "$D/compose/.env" JM_MANAGER_DIGEST)" = "sha256:$(hex64 e)" ] && [ "$(envval "$D/compose/.env" JM_SETUP_RELEASE)" = 9.9.10 ] || fail "--release applied"
grep -q 'releases/download/v9.9.10/' "$FAKE_LOG" || fail "the requested release was read"
cmp -s "$WORK/override.yml" "$D/compose/docker-compose.override.yml" && grep -q -- "-f $D/compose/docker-compose.override.yml" "$FAKE_LOG" || fail "override installed and used"
[ "$(stat -c %a "$D/compose/.env")" = 600 ] || fail ".env stays 0600"
[ "$(envval "$D/compose/.env" JM_SETUP_COMPOSE_VERSION)" = 5.0.0 ] || fail "a missing JM_SETUP_COMPOSE_VERSION is added"
ok "a never-started install takes --release and --override; every secret stays byte for byte"

# 16. A Manager that refuses the key is reported.
FAKE_HTTP=401 run_plain --dir "$D"
[ "$rc" = 1 ] && grep -q "refuses this install's key" "$WORK/err" || fail "401 reported"
ok "a Manager that refuses the key is reported"

# 17. A project of the same name not made by setup is left alone.
reset
mkdir -p "$WORK/other"; echo "$WORK/other" > "$FAKE_DIR/running"
run_plain --yes
[ "$rc" = 1 ] && grep -q 'was not made by juicemount-setup' "$WORK/err" || fail "foreign project refused"
ok "a Compose project of the same name made by hand is refused"

# 18. Paths: .. is resolved; / and system folders are refused.
reset
for bad in /srv/.. /tmp/a/../.. /etc/juicemount /mnt/tank /usr; do
  run_plain --yes --dir "$bad"
  [ "$rc" = 1 ] && grep -q 'system folder\|root of a pool' "$WORK/err" || fail "--dir $bad refused"
done
run_plain --yes --images-env "$FAKE_DIR/server-images.env" --dir "$D"
[ "$rc" = 1 ] && grep -q -- '--images-env needs --source' "$WORK/err" || fail "--images-env alone refused"
nothing_written "$D" || fail "path refusals write nothing"
ok "--dir resolves .. and refuses / and system folders; --images-env needs --source"

# 19. A failed random read never writes an empty secret.
reset
mkdir -p "$WORK/bin-od"; printf '#!/bin/sh\nexit 1\n' > "$WORK/bin-od/od"; chmod +x "$WORK/bin-od/od"
save_fakes
set +e
PATH="$WORK/bin-od:$PLAIN:$PATH" setsid bash "$SCRIPT" --host nas.example --yes --dir "$D" > "$WORK/out" 2> "$WORK/err" < /dev/null
rc=$?
set -e
[ "$rc" = 1 ] && grep -q 'could not read random bytes' "$WORK/err" && [ ! -e "$D/compose/.env" ] || fail "random failure writes no .env"
ok "a failed random read stops before .env is written"

# 20. Piped: --help prints the options; a download cut short runs nothing.
set +e
bash -s -- --help < "$SCRIPT" > /dev/null 2> "$WORK/err"; rc=$?
set -e
[ "$rc" = 2 ] && grep -q -- '--dry-run' "$WORK/err" || fail "piped --help prints the options"
save_fakes
for cut in 2 4 8; do
  : > "$FAKE_LOG"
  set +e
  head -c "-$cut" "$SCRIPT" | PATH="$PLAIN:$PATH" bash -s -- --yes --dir "$D" > /dev/null 2> "$WORK/err"; rc=$?
  set -e
  [ "$rc" != 0 ] && [ ! -s "$FAKE_LOG" ] || fail "a download cut by $cut bytes runs nothing"
done
ok "piped --help prints the options; a truncated download runs nothing"

echo "juicemount-setup tests: $pass passed"
