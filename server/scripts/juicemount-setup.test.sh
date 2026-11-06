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
trap 'rm -rf "$WORK"' EXIT
export FAKE_LOG="$WORK/calls.log" FAKE_DIR="$WORK/fake" FAKE_SERVER="$SERVER"
mkdir -p "$WORK/bin" "$FAKE_DIR"

pass=0
ok() { pass=$((pass + 1)); printf 'ok %d - %s\n' "$pass" "$1"; }
fail() { printf 'not ok - %s\n' "$1" >&2; [ -f "$WORK/err" ] && sed 's/^/# /' "$WORK/err" >&2; exit 1; }

# ---- fixtures -----------------------------------------------------------
hex64() { printf "$1%.0s" {1..64}; }
write_release() { # manager-digest-char
  {
    echo "JM_MANAGER_DIGEST=sha256:$(hex64 "$1")"
    echo "JM_FARM_DIGEST=sha256:$(hex64 b)"
    echo "JM_FARM_RENDER_DIGEST=sha256:$(hex64 c)"
    echo "JM_RESPPROXY_DIGEST=sha256:$(hex64 d)"
  } > "$FAKE_DIR/server-images.env"
  cat > "$FAKE_DIR/release-manifest.json" <<JSON
{
  "server_images": {
    "JM_FARM_DIGEST": { "digest": "sha256:$(hex64 b)" },
    "JM_FARM_RENDER_DIGEST": { "digest": "sha256:$(hex64 c)" },
    "JM_MANAGER_DIGEST": { "digest": "sha256:$(hex64 a)" },
    "JM_RESPPROXY_DIGEST": { "digest": "sha256:$(hex64 d)" }
  },
  "version": "9.9.9"
}
JSON
}

# ---- fakes --------------------------------------------------------------
cat > "$WORK/bin/curl" <<'SH'
#!/usr/bin/env bash
# Fake curl: release assets from $FAKE_DIR, repository files from
# $FAKE_SERVER, the Manager probe answers $FAKE_HTTP (default 200).
printf 'curl %s\n' "$*" >> "$FAKE_LOG"
out="" url=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift 2 ;;
    -w|-m|-H|--proto|--retry) shift 2 ;;
    -*) shift ;;
    *) url="$1"; shift ;;
  esac
done
case "$url" in
  http://127.0.0.1:*) cat >/dev/null; printf '%s' "${FAKE_HTTP:-200}"; exit 0 ;;
  */releases/*/server-images.env) src="$FAKE_DIR/server-images.env" ;;
  */releases/*/release-manifest.json) src="$FAKE_DIR/release-manifest.json" ;;
  https://raw.githubusercontent.com/lelanddutcher/juicemount/main/server/*)
    src="$FAKE_SERVER/${url#https://raw.githubusercontent.com/lelanddutcher/juicemount/main/server/}" ;;
  *) echo "fake curl: unexpected $url" >&2; exit 22 ;;
esac
[ -f "$src" ] || exit 22
cp "$src" "$out"
SH

cat > "$WORK/bin/docker" <<'SH'
#!/usr/bin/env bash
# Fake docker. State: $FAKE_DIR/running (containers exist once `up` ran).
printf 'docker %s\n' "$*" >> "$FAKE_LOG"
case "$1" in
  info) exit 0 ;;
  ps) [ -f "$FAKE_DIR/running" ] && echo cid-1; exit 0 ;;
  inspect)
    case "$*" in
      *working_dir*) cat "$FAKE_DIR/running" ;;
      *ExitCode*) [ "${*: -1}" = cid-juicefs-init ] && echo "${FAKE_INIT_EXIT:-0}" || echo 0 ;;
    esac
    exit 0 ;;
  compose) ;;
  *) exit 0 ;;
esac
shift
[ "$1" = version ] && exit 0
[ -z "${JM_ADMIN_KEY+x}" ] || echo "LEAK JM_ADMIN_KEY" >> "$FAKE_LOG"
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
  "config ")
    grep -v '^[[:space:]]*#' "${files[0]}" | sed -n 's/^[[:space:]]*- \([./][^:]*\):.*/\1/p' | while read -r s; do
      case "$s" in ./*) s="$dir/${s#./}" ;; esac
      echo "        source: $s"
    done
    echo "        published: \"${FAKE_PORT:-1}\"" ;;
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
# Fake datasets in $FAKE_DIR/datasets; pool P is mounted at $FAKE_DIR/mnt/P.
printf 'zfs %s\n' "$*" >> "$FAKE_LOG"
db="$FAKE_DIR/datasets"; touch "$db"
known() { case "$1" in boot-pool|tank|big) return 0 ;; esac; grep -qx "$1" "$db"; }
case "$1" in
  get) known "${*: -1}" && echo "$FAKE_DIR/mnt/${*: -1}" ;;
  list) known "${*: -1}" ;;
  create) n="${*: -1}"; echo "$n" >> "$db"; mkdir -p "$FAKE_DIR/mnt/$n" ;;
  *) exit 1 ;;
esac
SH
chmod +x "$WORK/bin/"*

run() { # args... ; sets rc, stdout in $WORK/out, stderr in $WORK/err
  set +e
  PATH="$WORK/bin:$PATH" setsid bash "$SCRIPT" --host nas.example "$@" > "$WORK/out" 2> "$WORK/err" < /dev/null
  rc=$?
  set -e
}
reset() { rm -rf "$FAKE_DIR" "$WORK/data"; mkdir -p "$FAKE_DIR"; : > "$FAKE_LOG"; write_release a; unset FAKE_FAIL FAKE_HTTP FAKE_INIT_EXIT; }
envval() { sed -n "s/^$2=//p" "$1"; }
NO_TRUENAS_PATH="$WORK/bin-plain"
mkdir -p "$NO_TRUENAS_PATH"
for t in curl docker; do ln -sf "$WORK/bin/$t" "$NO_TRUENAS_PATH/$t"; done
run_plain() { # like run, but without the TrueNAS/ZFS fakes on PATH
  set +e
  PATH="$NO_TRUENAS_PATH:$PATH" setsid bash "$SCRIPT" --host nas.example "$@" > "$WORK/out" 2> "$WORK/err" < /dev/null
  rc=$?
  set -e
}

D="$WORK/data"

# 1. Fresh install in a folder, files from GitHub main (fake), digests from
#    the release.
reset
JM_ADMIN_KEY=caller-shell-value-must-not-reach-compose-0000 run_plain --yes --dir "$D"
[ "$rc" = 0 ] || fail "fresh install exits 0 (rc=$rc)"
[ "$(wc -l < "$WORK/out")" = 2 ] || fail "stdout is exactly two lines"
grep -qx 'Manager URL:  http://nas.example:30190' "$WORK/out" || fail "Manager URL line"
key="$(sed -n 's/^Sign-in key:  //p' "$WORK/out")"
[[ "$key" =~ ^[0-9a-f]{32}$ ]] || fail "sign-in key is 32 hex characters"
ok "fresh install prints exactly the Manager URL and the sign-in key"

E="$D/compose/.env"
[ "$(stat -c %a "$D/compose")" = 700 ] && [ "$(stat -c %a "$E")" = 600 ] || fail "compose dir 0700, .env 0600"
[ "$(envval "$E" JM_ADMIN_KEY)" = "$key" ] || fail "printed key is the stored key"
for kv in MINIO_ROOT_PASSWORD:40 JM_REDIS_PROXY_PASSWORD:64 JM_PROXY_FARM_PASSWORD:64 JM_PROXY_MANAGER_PASSWORD:64 \
  JM_PROXY_BG_PASSWORD:64 JM_PROXY_WEBDAV_PASSWORD:64 JM_PROXY_ADMIN_PASSWORD:64 JM_OBJ_CLIENT_SECRET_KEY:40 JM_OBJ_GC_SECRET_KEY:40; do
  v="$(envval "$E" "${kv%%:*}")"
  [[ "$v" =~ ^[0-9a-f]+$ ]] && [ ${#v} = "${kv##*:}" ] || fail "${kv%%:*} is ${kv##*:} hex characters"
done
[ "$(grep -E '^(JM_|MINIO_)[A-Z_]*(PASSWORD|SECRET_KEY|ADMIN_KEY)=' "$E" | cut -d= -f2 | sort | uniq -d)" = "" ] || fail "every secret is distinct"
[ "$(envval "$E" JM_MANAGER_DIGEST)" = "sha256:$(hex64 a)" ] && [ "$(envval "$E" JM_RESPPROXY_DIGEST)" = "sha256:$(hex64 d)" ] || fail "digests from the release"
[ "$(envval "$E" JM_SETUP_RELEASE)" = 9.9.9 ] || fail "release version recorded"
[[ "$(envval "$E" JM_FARM_STORAGE_CAPACITY_BYTES)" =~ ^[1-9][0-9]*$ ]] || fail "capacity is a positive integer"
[ "$(envval "$E" JM_FARM_NODE_META_URL)" = 'redis://farm@nas.example:30179/1' ] || fail "farm node URL is user@host only"
ok "secrets generated with the lengths the stack requires, digests from the release, file modes"

C="$D/compose/docker-compose.yml"
grep -v '^[[:space:]]*#' "$C" > "$WORK/active.yml"
! grep -q '/mnt/zSSD\|/mnt/storage0' "$WORK/active.yml" || fail "no TrueNAS sample path left"
grep -q -- "- $D/redis:/data" "$C" && grep -q -- "- $D/import:/sources/import:ro" "$C" || fail "paths rendered into the data folder"
for f in redis.conf redis.acl minio/provision-object-keys.sh minio/jm-gc-policy.json; do cmp -s "$SERVER/$f" "$D/compose/$f" || fail "$f copied"; done
diff <(grep -v '^[[:space:]]*-.*:/' "$SERVER/docker-compose.yml" | grep -v '/mnt/zSSD') \
     <(grep -v '^[[:space:]]*-.*:/' "$C" | grep -v "$D") > /dev/null || fail "only host paths differ from the canonical compose"
for s in bucket redis cache manager-state respproxy objkeys headscale juicefarm-state import; do [ -d "$D/$s" ] || fail "$s created"; done
ok "compose rendered: only host paths changed, files it mounts copied, data folders created"

grep -q 'docker compose --project-name juicemount .* pull --quiet' "$FAKE_LOG" && grep -q 'up -d' "$FAKE_LOG" || fail "pull and up ran"
! grep -q LEAK "$FAKE_LOG" || fail "a caller's JM_ADMIN_KEY reached docker compose"
ok "values reach compose only from .env (caller shell variables are unset)"

# 2. Re-run: nothing regenerated, same key printed, no prompt needed.
sum="$(cd "$D/compose" && sha256sum .env docker-compose.yml redis.conf minio/*)"
: > "$FAKE_LOG"
run_plain --dir "$D"
[ "$rc" = 0 ] || fail "re-run exits 0 (rc=$rc)"
[ "$(cd "$D/compose" && sha256sum .env docker-compose.yml redis.conf minio/*)" = "$sum" ] || fail "re-run leaves every file unchanged"
[ "$(sed -n 's/^Sign-in key:  //p' "$WORK/out")" = "$key" ] || fail "re-run prints the same key"
! grep -q 'raw.githubusercontent\|releases/' "$FAKE_LOG" || fail "re-run downloads nothing"
ok "re-run changes nothing and prints the same two lines"

# 2b. Re-run found through Docker labels alone (no --dir).
: > "$FAKE_LOG"
run_plain
[ "$rc" = 0 ] && [ "$(cd "$D/compose" && sha256sum .env docker-compose.yml redis.conf minio/*)" = "$sum" ] || fail "re-run without --dir finds the install"
ok "re-run without options finds the install from the running project"

# 3. An install whose .env lost a secret is refused, not regenerated.
cp "$E" "$WORK/env.bak"
sed -i '/^JM_PROXY_BG_PASSWORD=/d' "$E"
run_plain --dir "$D"
[ "$rc" = 1 ] && grep -q 'lacks a usable JM_PROXY_BG_PASSWORD' "$WORK/err" || fail "missing secret refused"
! grep -q '^JM_PROXY_BG_PASSWORD=' "$E" || fail "missing secret not regenerated"
cp "$WORK/env.bak" "$E"
ok "an install missing a secret is refused, never regenerated"

# 4. Data without its .env: refused before anything is written.
reset
mkdir -p "$D/redis"; echo x > "$D/redis/appendonly.aof"
run_plain --yes --dir "$D"
[ "$rc" = 1 ] && grep -q 'already holds data' "$WORK/err" || fail "existing data without .env refused"
[ ! -e "$D/compose" ] && ! grep -q 'compose .* up' "$FAKE_LOG" || fail "nothing written or started over existing data"
ok "existing data without its secrets is refused before anything is written"

# 5. Release digests are validated.
reset
sed -i 's/^JM_FARM_DIGEST=.*/JM_FARM_DIGEST=latest/' "$FAKE_DIR/server-images.env"
run_plain --yes --dir "$D"
[ "$rc" = 1 ] && grep -q 'not KEY=sha256' "$WORK/err" && [ ! -e "$D" ] || fail "malformed digest refused"
reset
write_release e
run_plain --yes --dir "$D"
[ "$rc" = 1 ] && grep -q 'does not match release-manifest.json' "$WORK/err" && [ ! -e "$D" ] || fail "digest not in manifest refused"
ok "a digest that is malformed or absent from release-manifest.json is refused"

# 6. Without a terminal, --yes is required; nothing is written.
reset
run_plain --dir "$D"
[ "$rc" = 1 ] && grep -q 're-run with --yes' "$WORK/err" && [ ! -e "$D" ] || fail "no terminal without --yes refused"
ok "without a terminal and --yes it stops before changing anything"

# 7. Dry run changes nothing.
reset
run_plain --dry-run --dir "$D"
[ "$rc" = 0 ] && grep -q 'Dry run: nothing was changed' "$WORK/err" && [ ! -e "$D" ] || fail "dry run writes nothing"
ok "dry run shows the plan and writes nothing"

# 8. TrueNAS: datasets on the pool with most free space, never boot-pool.
reset
run --yes
[ "$rc" = 0 ] || fail "TrueNAS install exits 0 (rc=$rc)"
grep -qx 'zfs create -p big/juicemount' "$FAKE_LOG" && grep -qx 'zfs create -p big/juicemount/bucket' "$FAKE_LOG" &&
  grep -qx 'zfs create -p big/juicemount/compose' "$FAKE_LOG" || fail "datasets created on pool big"
! grep -q 'boot-pool/' "$FAKE_LOG" || fail "boot-pool untouched"
TE="$FAKE_DIR/mnt/big/juicemount/compose/.env"
[ "$(envval "$TE" JM_FARM_STORAGE_CAPACITY_BYTES)" = 7000000000000 ] || fail "capacity from zpool size"
grep -q -- "- $FAKE_DIR/mnt/big/juicemount/bucket:/data" "$FAKE_DIR/mnt/big/juicemount/compose/docker-compose.yml" || fail "bucket on the dataset"
ok "TrueNAS: datasets under <pool>/juicemount on the pool with most free space; capacity from zpool"

: > "$FAKE_LOG"; rm -f "$FAKE_DIR/running"
run --pool tank --yes
[ "$rc" = 0 ] && grep -qx 'zfs create -p tank/juicemount' "$FAKE_LOG" || fail "--pool selects the pool"
ok "--pool selects another pool"

# 9. A folder with content where a dataset would go is refused.
reset
mkdir -p "$FAKE_DIR/mnt/tank/juicemount/cache"; echo x > "$FAKE_DIR/mnt/tank/juicemount/cache/chunk"
run --pool tank --yes
[ "$rc" = 1 ] && grep -q 'already holds data' "$WORK/err" && ! grep -q 'zfs create' "$FAKE_LOG" || fail "dataset over existing content refused"
ok "a non-empty folder that a new dataset would hide is refused"

# 10. Failures name their cause and keep the settings.
reset
FAKE_FAIL=pull run_plain --yes --dir "$D"
[ "$rc" = 1 ] && grep -q 'could not download an image' "$WORK/err" && [ -f "$D/compose/.env" ] || fail "pull failure explained"
ok "an image that cannot be pulled stops before start and keeps the settings"
key1="$(envval "$D/compose/.env" JM_ADMIN_KEY)"
FAKE_FAIL=up FAKE_INIT_EXIT=4 run_plain --yes --dir "$D"
[ "$rc" = 1 ] && grep -q 'juicefs-init exit 4' "$WORK/err" || fail "juicefs-init exit code named"
[ "$(envval "$D/compose/.env" JM_ADMIN_KEY)" = "$key1" ] || fail "settings kept after failed start"
ok "a failed juicefs-init is named by its exit code; the settings are kept"

# 11. An install that never started can take a new compose (and override).
printf 'services:\n  minio:\n    image: example.invalid/store@sha256:%s\n' "$(hex64 f)" > "$WORK/override.yml"
run_plain --yes --dir "$D" --override "$WORK/override.yml"
[ "$rc" = 0 ] && cmp -s "$WORK/override.yml" "$D/compose/docker-compose.override.yml" || fail "override installed on a never-started install"
[ "$(envval "$D/compose/.env" JM_ADMIN_KEY)" = "$key1" ] || fail "secrets kept when the compose is refreshed"
grep -q -- "-f $D/compose/docker-compose.override.yml" "$FAKE_LOG" || fail "override passed to compose"
ok "a never-started install takes a new compose and --override, keeping its secrets"
# Once started, the same option is ignored.
sum="$(cd "$D/compose" && sha256sum .env docker-compose.yml docker-compose.override.yml)"
run_plain --dir "$D" --override "$SERVER/docker-compose.link.yml"
[ "$rc" = 0 ] && grep -q 'options are ignored' "$WORK/err" &&
  [ "$(cd "$D/compose" && sha256sum .env docker-compose.yml docker-compose.override.yml)" = "$sum" ] || fail "started install keeps its files"
ok "a started install ignores options that would change it"

# 12. A Manager that refuses the key is reported.
FAKE_HTTP=401 run_plain --dir "$D"
[ "$rc" = 1 ] && grep -q "refuses this install's key" "$WORK/err" || fail "401 reported"
ok "a Manager that refuses the key is reported"

# 13. A project of the same name not made by setup is left alone.
reset
mkdir -p "$WORK/other"; echo "$WORK/other" > "$FAKE_DIR/running"
run_plain --yes
[ "$rc" = 1 ] && grep -q 'was not made by juicemount-setup' "$WORK/err" || fail "foreign project refused"
ok "a Compose project of the same name made by hand is refused"

echo "juicemount-setup tests: $pass passed"
