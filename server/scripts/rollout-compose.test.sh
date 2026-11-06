#!/usr/bin/env bash
# Offline failure-injection tests. No Docker daemon, SSH, or NAS is contacted.
set -euo pipefail
SCRIPT="$(cd "$(dirname "$0")" && pwd)/rollout-compose.sh"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
REPO="$WORK/repo"
mkdir -p "$REPO/server" "$REPO/internal/version" "$WORK/bin"
cp "$(dirname "$SCRIPT")/../docker-compose.yml" "$REPO/server/docker-compose.yml"
printf 'package version\nvar Version = "0.5.0"\n' > "$REPO/internal/version/version.go"
printf 'MINIO_ROOT_PASSWORD=test-password\nJM_ADMIN_KEY=0123456789abcdef0123456789abcdef\nJM_FARM_STORAGE_CAPACITY_BYTES=100000000000\n' > "$REPO/server/.env"
git -C "$REPO" init -q
git -C "$REPO" config user.name test
git -C "$REPO" config user.email test@example.invalid
git -C "$REPO" add server/docker-compose.yml internal/version/version.go
git -C "$REPO" commit -qm fixture
export FAKE_COMMIT="$(git -C "$REPO" rev-parse HEAD)"
export OLD_M="sha256:$(printf 'a%.0s' {1..64})"
export OLD_F="sha256:$(printf 'b%.0s' {1..64})"
export OLD_R="sha256:$(printf 'c%.0s' {1..64})"
export NEW_M="sha256:$(printf 'd%.0s' {1..64})"
export NEW_F="sha256:$(printf 'e%.0s' {1..64})"
export NEW_R="sha256:$(printf 'f%.0s' {1..64})"
printf 'JM_MANAGER_DIGEST=%s\nJM_FARM_DIGEST=%s\nJM_FARM_RENDER_DIGEST=%s\n' "$OLD_M" "$OLD_F" "$OLD_R" >> "$REPO/server/.env"
export FAKE_REPO="$REPO"
export FAKE_LOG="$WORK/docker.log"
export FAKE_STATE="$WORK/state"
export FAKE_FAIL=""
export FAKE_TARGET_PIDFILE="$WORK/target.pid"
# The ACL the running Redis rendered at its start: fixed | old | unreadable.
export FAKE_RUNNING_ACL=fixed
# The AOF rewrite: ok | refused | hang | err.
export FAKE_AOF=ok
export FAKE_AOF_DIR="$WORK/aof"
printf old > "$FAKE_STATE"
: > "$FAKE_LOG"

cat > "$WORK/bin/docker" <<'DOCKER'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "docker $*" >> "$FAKE_LOG"
state="$(cat "$FAKE_STATE")"
manager='ghcr.io/lelanddutcher/juicemount-manager'
farm='ghcr.io/lelanddutcher/juicefarm'
render='ghcr.io/lelanddutcher/juicefarm-gpu'
if [ "$1" = compose ]; then
  cmd=''
  for arg in "$@"; do
    case "$arg" in ps|config|up) cmd="$arg"; break ;; esac
  done
  case "$cmd" in
    ps)
      service="${!#}"
      case "$service" in
        juicemount-manager) echo manager-id ;;
        juicefarm) echo farm-id ;;
        respproxy) [ "$FAKE_FAIL" = no_respproxy ] || echo respproxy-id ;;
        redis|minio|juicefs|juicefs-init) echo "$service-id" ;;
        *) exit 1 ;;
      esac ;;
    config)
      case " $* " in
        *' --hash '*)
          if [ "$FAKE_FAIL" = bad_hash ]; then printf '%064d\n' 1; else printf 'a%.0s' {1..64}; echo; fi ;;
        *' --format json '*)
          [ "$FAKE_FAIL" = post_verify_error ] && [ "$state" = new ] && exit 1
          if [ "$FAKE_FAIL" = post_verify_signal ] && [ "$state" = new ]; then
            while [ ! -s "$FAKE_TARGET_PIDFILE" ]; do /bin/sleep 0.01; done
            kill -TERM "$(cat "$FAKE_TARGET_PIDFILE")"
          fi
          if [ "$FAKE_FAIL" = bad_key ]; then key=changed; else key=0123456789abcdef0123456789abcdef; fi
          if [ "${JM_MANAGER_DIGEST:-}" = "$OLD_M" ]; then manager_digest="$OLD_M"; farm_digest="$OLD_F"; render_digest="$OLD_R";
          elif [ "${JM_MANAGER_DIGEST:-}" = "$NEW_M" ]; then manager_digest="$NEW_M"; farm_digest="$NEW_F"; render_digest="$NEW_R";
          else
            manager_digest="$(sed -n 's/^JM_MANAGER_DIGEST=//p' "$FAKE_REPO/server/.env")"
            farm_digest="$(sed -n 's/^JM_FARM_DIGEST=//p' "$FAKE_REPO/server/.env")"
            render_digest="$(sed -n 's/^JM_FARM_RENDER_DIGEST=//p' "$FAKE_REPO/server/.env")"
          fi
          printf '{"services":{"juicemount-manager":{"image":"%s@%s","environment":{"JM_ADMIN_KEY":"%s","JM_FARM_RENDER_IMAGE":"%s@%s"}},"juicefarm":{"image":"%s@%s"}}}\n' \
            "$manager" "$manager_digest" "$key" "$render" "$render_digest" "$farm" "$farm_digest" ;;
      esac ;;
    up)
      if [ "${JM_MANAGER_DIGEST:-}" = "$OLD_M" ]; then
        [ "$FAKE_FAIL" = rollback_fail ] && exit 1
        printf old > "$FAKE_STATE"
      else
        printf new > "$FAKE_STATE"
        [ "$FAKE_FAIL" = up_fail ] && exit 1
      fi ;;
    *) exit 1 ;;
  esac
  exit 0
fi
case "$1" in
  ps)
    case "$*" in
      *label=com.docker.compose.project=testproject*label=com.docker.compose.service=redis*status=running*)
        [ "${FAKE_REDIS_DOWN:-}" = 1 ] || echo redis-id ;;
      *) echo "unexpected ps: $*" >&2; exit 1 ;;
    esac ;;
  inspect)
    shift
    if [ "${1:-}" = -f ]; then
      template="$2"; id="$3"
      case "$template" in
        *com.docker.compose.project.working_dir*) echo "$FAKE_REPO/server" ;;
        *com.docker.compose.project.config_files*) echo "$FAKE_REPO/server/docker-compose.yml" ;;
        *com.docker.compose.project*) echo testproject ;;
        *com.docker.compose.service*)
          case "$id" in manager-id) echo juicemount-manager ;; farm-id) echo juicefarm ;; *-id) echo "${id%-id}" ;; esac ;;
        *com.docker.compose.config-hash*) printf 'a%.0s' {1..64}; echo ;;
        *State.Running*) echo true ;;
        *State.Health.Status*)
          if [ "$FAKE_FAIL" = protected_unhealthy ] && [ "$state" = new ] && [ "$id" = redis-id ]; then echo unhealthy; else echo healthy; fi ;;
        *State.Status*) echo exited ;;
        *State.ExitCode*) echo 0 ;;
        *Config.Image*)
          case "$id" in
            manager-id) if [ "$state" = new ]; then echo "$manager@$NEW_M"; else echo "$manager@$OLD_M"; fi ;;
            farm-id) if [ "$state" = new ]; then echo "$farm@$NEW_F"; else echo "$farm@$OLD_F"; fi ;;
          esac ;;
        *Config.Env*)
          printf 'JM_ADMIN_KEY=0123456789abcdef0123456789abcdef\nJM_FARM_RENDER_IMAGE=%s@%s\n' "$render" "$OLD_R" ;;
        *) echo "unexpected inspect template: $template" >&2; exit 1 ;;
      esac
    fi ;;
  image) exit 0 ;;
  pull)
    if [ "$FAKE_FAIL" = env_changed_during_pull ]; then
      printf '# operator changed this file during image pull\n' >> "$FAKE_REPO/server/.env"
    fi
    exit 0 ;;
  run)
    if [ "$FAKE_FAIL" = bad_identity ]; then echo 'wrong build'; exit 0; fi
    case "$*" in
      *juicemount-manager*) echo "juicemount-manager 0.5.0 ($FAKE_COMMIT)" ;;
      *juicefarm*) echo "jmfarm 0.5.0 ($FAKE_COMMIT)" ;;
    esac ;;
  exec)
    id="$2"
    case "$*" in
      *'cat /run/jm-redis/users.acl'*)
        [ "$id" = redis-id ] || { echo "ACL read from $id" >&2; exit 1; }
        printf 'user default off resetpass resetkeys resetchannels '
        case "$FAKE_RUNNING_ACL" in
          fixed) echo '~* &* +@all' ;;
          old) echo -- '-@all' ;;
          *) echo 'cat: no such file' >&2; exit 1 ;;
        esac
        echo 'user jm-proxy on #0000 ~* &* +@all' ;;
      *jm-redis-cli*BGREWRITEAOF*)
        [ "$id" = redis-id ] || { echo "BGREWRITEAOF sent to $id" >&2; exit 1; }
        [ "$FAKE_AOF" != refused ] || { echo 'ERR Background append only file rewriting failed to start'; exit 0; }
        # Redis counts a rewrite when it forks (aof.c), so before it ends.
        n="$(cat "$FAKE_AOF_DIR/count")"; echo $((n + 1)) > "$FAKE_AOF_DIR/count"
        case "$FAKE_AOF" in
          hang) : > "$FAKE_AOF_DIR/busy" ;;
          err) : > "$FAKE_AOF_DIR/err" ;;
        esac
        echo 'Background append only file rewriting started' ;;
      *jm-redis-cli*'INFO persistence'*)
        busy=0; status=ok; fails=0
        [ -e "$FAKE_AOF_DIR/busy" ] && busy=1
        [ -e "$FAKE_AOF_DIR/err" ] && status=err && fails=1
        printf 'aof_rewrite_in_progress:%s\r\naof_rewrite_scheduled:0\r\naof_last_bgrewrite_status:%s\r\naof_rewrites:%s\r\naof_rewrites_consecutive_failures:%s\r\n' \
          "$busy" "$status" "$(cat "$FAKE_AOF_DIR/count")" "$fails" ;;
      *--build-info*)
        if [ "$id" = manager-id ]; then binary=juicemount-manager; else binary=jmfarm; fi
        if [ "$state" = new ]; then echo "$binary 0.5.0 ($FAKE_COMMIT)"; else echo "$binary 0.4.9 (0000000000000000000000000000000000000000)"; fi ;;
      *'sh -ec'*)
        { [ "$FAKE_FAIL" = health_fail ] || [ "$FAKE_FAIL" = rollback_fail ]; } && [ "$state" = new ] && exit 1
        [ "$FAKE_FAIL" = old_unhealthy ] && [ "$state" = old ] && exit 1
        echo 200 ;;
    esac ;;
  *) echo "unexpected docker call: $*" >&2; exit 1 ;;
esac
DOCKER
chmod +x "$WORK/bin/docker"
cat > "$WORK/bin/mv" <<'MV'
#!/usr/bin/env bash
if [ "${FAKE_FAIL:-}" = env_write_fail ] && [[ "$*" == *'.rollout.'* ]] && [ "${!#}" = "$FAKE_REPO/server/.env" ]; then
  exit 1
fi
exec /bin/mv "$@"
MV
chmod +x "$WORK/bin/mv"
printf '#!/usr/bin/env bash\nexit 0\n' > "$WORK/bin/sleep"
chmod +x "$WORK/bin/sleep"
export PATH="$WORK/bin:$PATH"
args=(--repo "$REPO" --project testproject --manager-digest "$NEW_M" --farm-digest "$NEW_F" --render-digest "$NEW_R" --ack-worker-quiesced)

reset_case() {
  printf old > "$FAKE_STATE"
  rm -rf "$FAKE_AOF_DIR"; mkdir -p "$FAKE_AOF_DIR"; echo 3 > "$FAKE_AOF_DIR/count"
  export FAKE_RUNNING_ACL=fixed FAKE_AOF=ok FAKE_REDIS_DOWN=""
  : > "$FAKE_LOG"
  rm -rf "$REPO/.update-lock"
  rm -f "$FAKE_TARGET_PIDFILE"
  printf 'MINIO_ROOT_PASSWORD=test-password\nJM_ADMIN_KEY=0123456789abcdef0123456789abcdef\nJM_FARM_STORAGE_CAPACITY_BYTES=100000000000\nJM_MANAGER_DIGEST=%s\nJM_FARM_DIGEST=%s\nJM_FARM_RENDER_DIGEST=%s\n' \
    "$OLD_M" "$OLD_F" "$OLD_R" > "$REPO/server/.env"
}
assert_no_infra_mutation() {
  if grep -E 'docker (stop|rm|restart|kill) .* (redis|minio|respproxy|juicefs)' "$FAKE_LOG" >/dev/null ||
    grep -E 'docker compose .* (up|rm) .* (redis|minio|respproxy|juicefs)( |$)' "$FAKE_LOG" >/dev/null; then
    echo 'FAIL: protected service mutation' >&2; exit 1
  fi
}
rewrites() { grep -c 'jm-redis-cli BGREWRITEAOF' "$FAKE_LOG" || true; }
# Only the read-only check of which ACL the running Redis loaded.
only_acl_reads() {
  ! grep -vE '^docker ps .*com.docker.compose.service=redis|^docker exec redis-id cat /run/jm-redis/users.acl$' "$FAKE_LOG" >/dev/null
}
warned() { grep -q 'Do NOT reboot the NAS or restart Redis until a rewrite succeeds' "$WORK/out"; }
reset_case
bash "$SCRIPT" "${args[@]}" --dry-run > "$WORK/out" 2>&1
only_acl_reads && [ ! -e "$REPO/.update-lock" ] && ! warned || { cat "$FAKE_LOG" "$WORK/out"; echo 'FAIL: dry run acted, kept a lock or warned on a fixed ACL' >&2; exit 1; }
grep -q 'running Redis: fixed ACL; no AOF rewrite needed' "$WORK/out"
echo 'ok: dry run has no side effects'

# Every release exit while the running Redis has the 0.5.0 ACL and no
# rewrite ran in this run warns not to reboot: a dry run, a held lock, a
# dirty tree, a missing backup acknowledgement.
reset_case
export FAKE_RUNNING_ACL=old
bash "$SCRIPT" "${args[@]}" --dry-run > "$WORK/out" 2>&1
only_acl_reads && warned && [ "$(rewrites)" = 0 ] || { cat "$WORK/out"; echo 'FAIL: dry run on a 0.5.0 Redis acted or did not warn' >&2; exit 1; }
grep -q 'running Redis: 0.5.0 ACL; a real run rewrites its AOF online' "$WORK/out"
mkdir "$REPO/.update-lock"
if bash "$SCRIPT" "${args[@]}" --ack-redis-aof-backup > "$WORK/out" 2>&1; then echo 'FAIL: ran over a held lock' >&2; exit 1; fi
grep -q 'lock exists' "$WORK/out" && warned || { cat "$WORK/out"; echo 'FAIL: held-lock exit did not warn' >&2; exit 1; }
rmdir "$REPO/.update-lock"
printf 'dirty\n' >> "$REPO/internal/version/version.go"
if bash "$SCRIPT" "${args[@]}" --ack-redis-aof-backup > "$WORK/out" 2>&1; then echo 'FAIL: ran on a dirty tree' >&2; exit 1; fi
git -C "$REPO" checkout -q -- internal/version/version.go
grep -q 'refusing dirty tracked source' "$WORK/out" && warned || { cat "$WORK/out"; echo 'FAIL: dirty-tree exit did not warn' >&2; exit 1; }
: > "$FAKE_LOG"
if bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1; then echo 'FAIL: rewrote a 0.5.0 Redis without the backup acknowledgement' >&2; exit 1; fi
grep -q 'then re-run with --ack-redis-aof-backup. Nothing was changed.' "$WORK/out" && warned || { cat "$WORK/out"; echo 'FAIL: no backup reminder' >&2; exit 1; }
[ "$(rewrites)" = 0 ] && ! grep -qE '^docker (pull|compose .* up )' "$FAKE_LOG" && [ ! -e "$REPO/.update-lock" ] || { echo 'FAIL: acted before the backup acknowledgement' >&2; exit 1; }
echo 'ok: every release exit without a rewrite on a 0.5.0 Redis warns not to reboot'

# With the acknowledgement, the rewrite runs right after the lock, before
# every preflight check: a release that then stops for unhealthy services
# or a config-hash mismatch has already made a reboot safe, and says so by
# not warning.
for failure in old_unhealthy bad_hash; do
  reset_case
  export FAKE_RUNNING_ACL=old FAKE_FAIL="$failure"
  if bash "$SCRIPT" "${args[@]}" --ack-redis-aof-backup > "$WORK/out" 2>&1; then echo "FAIL: accepted $failure" >&2; exit 1; fi
  [ "$(rewrites)" = 1 ] && [ "$(cat "$FAKE_AOF_DIR/count")" = 4 ] || { cat "$WORK/out"; echo "FAIL: $failure exit before the rewrite" >&2; exit 1; }
  ! warned || { echo "FAIL: $failure warned after a successful rewrite" >&2; exit 1; }
  [ "$(cat "$FAKE_STATE")" = old ] && [ ! -e "$REPO/.update-lock" ] || { echo "FAIL: $failure changed services or kept the lock" >&2; exit 1; }
done
export FAKE_FAIL=""
echo 'ok: the release rewrite runs before every preflight check'

# Redis down: say so, not that it "still uses the 0.5.0 ACL".
reset_case
export FAKE_REDIS_DOWN=1
if bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1; then echo 'FAIL: release ran with Redis down' >&2; exit 1; fi
grep -q 'CRITICAL: Redis of project testproject is not running, so its AOF could not be rewritten. Nothing was changed.' "$WORK/out" &&
  grep -q 'WARNING: Redis of project testproject is not running' "$WORK/out" &&
  ! grep -q 'still has the 0.5.0 ACL' "$WORK/out" || { cat "$WORK/out"; echo 'FAIL: Redis down misreported' >&2; exit 1; }
! grep -qE '^docker (pull|compose .* up )' "$FAKE_LOG" || { echo 'FAIL: release with Redis down changed something' >&2; exit 1; }
echo 'ok: a release with Redis down says so accurately'

reset_case
if bash "$SCRIPT" "${args[@]}" --allow-aof-replay > "$WORK/out" 2>&1; then echo 'FAIL: release accepted --allow-aof-replay' >&2; exit 1; fi
grep -q 'never restarts Redis' "$WORK/out" && only_acl_reads && [ ! -e "$REPO/.update-lock" ] || { echo 'FAIL: --allow-aof-replay refusal acted' >&2; exit 1; }
echo 'ok: release rollout refuses the AOF replay override (it never restarts Redis)'

reset_case
bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1
[ "$(cat "$FAKE_STATE")" = new ] && [ ! -e "$REPO/.update-lock" ]
grep -Fx "JM_MANAGER_DIGEST=$NEW_M" "$REPO/server/.env" >/dev/null
grep -Fx "JM_FARM_DIGEST=$NEW_F" "$REPO/server/.env" >/dev/null
grep -Fx "JM_FARM_RENDER_DIGEST=$NEW_R" "$REPO/server/.env" >/dev/null
grep -Fx 'JM_ADMIN_KEY=0123456789abcdef0123456789abcdef' "$REPO/server/.env" >/dev/null
! grep -q '0123456789abcdef0123456789abcdef' "$WORK/out"
[ "$(grep -c 'docker compose .* up ' "$FAKE_LOG")" = 1 ]
grep -F -- '--no-deps --no-build --pull never --force-recreate juicemount-manager juicefarm' "$FAKE_LOG" >/dev/null
assert_no_infra_mutation
[ "$(rewrites)" = 0 ] || { echo 'FAIL: AOF rewritten although the running Redis has the fixed ACL' >&2; exit 1; }
grep -q 'no AOF rewrite needed' "$WORK/out" || { echo 'FAIL: the running ACL was not checked' >&2; exit 1; }
echo 'ok: only the two requested services are recreated after immutable preflight'

# The running Redis still has the 0.5.0 default line (or its rendered ACL
# cannot be read): the release rewrites the AOF online before any change,
# never restarts Redis, and does so again on a re-run.
for acl in old unreadable; do
  reset_case
  export FAKE_RUNNING_ACL="$acl"
  bash "$SCRIPT" "${args[@]}" --ack-redis-aof-backup > "$WORK/out" 2>&1 || { cat "$WORK/out"; echo "FAIL: release failed with a $acl running ACL" >&2; exit 1; }
  ! warned || { echo 'FAIL: warned after a successful rewrite' >&2; exit 1; }
  [ "$(cat "$FAKE_STATE")" = new ] && [ ! -e "$REPO/.update-lock" ] || { echo "FAIL: release with a $acl ACL did not complete" >&2; exit 1; }
  [ "$(rewrites)" = 1 ] && [ "$(cat "$FAKE_AOF_DIR/count")" = 4 ] || { echo "FAIL: no AOF rewrite with a $acl running ACL" >&2; exit 1; }
  c="$(grep -n 'jm-redis-cli BGREWRITEAOF' "$FAKE_LOG" | head -1 | cut -d: -f1)"
  first="$(grep -nE '^docker (pull|compose .* up )' "$FAKE_LOG" | head -1 | cut -d: -f1)"
  [ -n "$first" ] && [ "$c" -lt "$first" ] || { echo 'FAIL: AOF rewritten after the release started changing things' >&2; exit 1; }
  grep -q 'aof_last_bgrewrite_status:ok' "$WORK/out"
  assert_no_infra_mutation
  echo "ok: a release rewrites the AOF online when the running Redis ACL is $acl, and never restarts Redis"
done
# Run the release again on the same Redis (its ACL is still old; the fake
# keeps the rewrite count and resets only the services and .env).
cp -R "$FAKE_AOF_DIR" "$WORK/aof.keep"; reset_case; rm -rf "$FAKE_AOF_DIR"; mv "$WORK/aof.keep" "$FAKE_AOF_DIR"
export FAKE_RUNNING_ACL=old
bash "$SCRIPT" "${args[@]}" --ack-redis-aof-backup > "$WORK/out" 2>&1 || { cat "$WORK/out"; echo 'FAIL: release re-run failed' >&2; exit 1; }
[ "$(rewrites)" = 1 ] && [ "$(cat "$FAKE_AOF_DIR/count")" = 5 ] || { echo 'FAIL: a release re-run did not rewrite the AOF again' >&2; exit 1; }
assert_no_infra_mutation
echo 'ok: a release re-run rewrites the AOF again while the running ACL is still old'

# A rewrite that is refused, fails or times out stops the release before
# any pull or mutation, and tells the operator not to restart Redis.
for aof in refused err hang; do
  reset_case
  export FAKE_RUNNING_ACL=old FAKE_AOF="$aof"
  if JM_REDIS_COMPACT_TIMEOUT=3 bash "$SCRIPT" "${args[@]}" --ack-redis-aof-backup > "$WORK/out" 2>&1; then echo "FAIL: release went on after a $aof rewrite" >&2; exit 1; fi
  grep -q 'CRITICAL: the AOF rewrite of the running Redis did not finish. Nothing was changed.' "$WORK/out" || { cat "$WORK/out"; echo "FAIL: $aof rewrite not reported" >&2; exit 1; }
  [ "$(cat "$FAKE_STATE")" = old ] && [ ! -e "$REPO/.update-lock" ] || { echo "FAIL: $aof rewrite left changes or the lock" >&2; exit 1; }
  grep -Fx "JM_MANAGER_DIGEST=$OLD_M" "$REPO/server/.env" >/dev/null
  ! grep -qE '^docker (pull|compose .* up )' "$FAKE_LOG" || { echo "FAIL: release changed something after a $aof rewrite" >&2; exit 1; }
  grep -q 'Do NOT reboot the NAS or restart Redis until a rewrite succeeds' "$WORK/out" || { cat "$WORK/out"; echo "FAIL: $aof rewrite did not warn the operator" >&2; exit 1; }
  assert_no_infra_mutation
done
echo 'ok: a failed release rewrite stops before any change and warns against a restart'

for failure in bad_hash bad_key old_unhealthy; do
  reset_case
  export FAKE_FAIL="$failure"
  if bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1; then echo "FAIL: accepted $failure" >&2; exit 1; fi
  [ "$(cat "$FAKE_STATE")" = old ] && [ ! -e "$REPO/.update-lock" ]
  ! grep -q 'docker compose .* up ' "$FAKE_LOG"
  ! grep -q '^docker pull ' "$FAKE_LOG"
  echo "ok: $failure refused before image pull or mutation"
done

reset_case
export FAKE_FAIL=no_respproxy
if bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1; then echo 'FAIL: release rollout accepted a pre-proxy stack' >&2; exit 1; fi
grep -q 'run --cutover-proxy first' "$WORK/out"
! grep -q 'docker compose .* up ' "$FAKE_LOG"
! grep -q '^docker pull ' "$FAKE_LOG"
echo 'ok: release rollout refuses a stack without respproxy and names the cutover'
export FAKE_FAIL=""

reset_case
printf 'JM_MANAGER_DIGEST=%s\n' "$NEW_M" >> "$REPO/server/.env"
if bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1; then echo 'FAIL: accepted ambiguous persistent digests' >&2; exit 1; fi
! grep -q 'docker compose .* up ' "$FAKE_LOG"
! grep -q '^docker pull ' "$FAKE_LOG"
echo 'ok: ambiguous .env digest assignments refused before mutation'

reset_case
export FAKE_FAIL=bad_identity
if bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1; then echo 'FAIL: accepted wrong source identity' >&2; exit 1; fi
[ "$(cat "$FAKE_STATE")" = old ] && [ ! -e "$REPO/.update-lock" ]
! grep -q 'docker compose .* up ' "$FAKE_LOG"
echo 'ok: embedded image identity mismatch refused before mutation'

reset_case
export FAKE_FAIL=env_write_fail
if bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1; then echo 'FAIL: accepted failed atomic env replace' >&2; exit 1; fi
[ "$(cat "$FAKE_STATE")" = old ] && [ ! -e "$REPO/.update-lock" ]
grep -Fx "JM_MANAGER_DIGEST=$OLD_M" "$REPO/server/.env" >/dev/null
! grep -q 'docker compose .* up ' "$FAKE_LOG"
echo 'ok: failed atomic .env replacement leaves previous images and digests'
[ -z "$(find "$REPO/server" -maxdepth 1 -name '.env.rollout.*' -print)" ]

reset_case
export FAKE_FAIL=env_changed_during_pull
if bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1; then echo 'FAIL: overwrote concurrent .env edit' >&2; exit 1; fi
[ "$(cat "$FAKE_STATE")" = old ] && [ ! -e "$REPO/.update-lock" ]
grep -Fq '# operator changed this file during image pull' "$REPO/server/.env"
! grep -q 'docker compose .* up ' "$FAKE_LOG"
echo 'ok: concurrent private .env edit preserved and rollout refused'

for failure in up_fail health_fail protected_unhealthy; do
  reset_case
  export FAKE_FAIL="$failure"
  if bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1; then echo "FAIL: accepted $failure" >&2; exit 1; fi
  [ "$(cat "$FAKE_STATE")" = old ] && [ ! -e "$REPO/.update-lock" ]
  grep -Fx "JM_MANAGER_DIGEST=$OLD_M" "$REPO/server/.env" >/dev/null
  grep -Fx "JM_FARM_DIGEST=$OLD_F" "$REPO/server/.env" >/dev/null
  grep -Fx "JM_FARM_RENDER_DIGEST=$OLD_R" "$REPO/server/.env" >/dev/null
  grep -Fx 'JM_ADMIN_KEY=0123456789abcdef0123456789abcdef' "$REPO/server/.env" >/dev/null
  [ "$(grep -c 'docker compose .* up ' "$FAKE_LOG")" = 2 ]
  assert_no_infra_mutation
  echo "ok: $failure rolls back to previous immutable images"
done

reset_case
export FAKE_FAIL=post_verify_error
if bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1; then echo 'FAIL: accepted post-verify config error' >&2; exit 1; fi
[ "$(cat "$FAKE_STATE")" = old ] && [ ! -e "$REPO/.update-lock" ]
grep -Fx "JM_MANAGER_DIGEST=$OLD_M" "$REPO/server/.env" >/dev/null
[ "$(grep -c 'docker compose .* up ' "$FAKE_LOG")" = 2 ]
echo 'ok: post-verify error restores both images and persistent digests'

reset_case
export FAKE_FAIL=post_verify_signal
bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1 &
target_pid=$!
printf '%s\n' "$target_pid" > "$FAKE_TARGET_PIDFILE"
if wait "$target_pid"; then echo 'FAIL: ignored post-verify TERM' >&2; exit 1; fi
[ "$(cat "$FAKE_STATE")" = old ] && [ ! -e "$REPO/.update-lock" ]
grep -Fx "JM_MANAGER_DIGEST=$OLD_M" "$REPO/server/.env" >/dev/null
[ "$(grep -c 'docker compose .* up ' "$FAKE_LOG")" = 2 ]
echo 'ok: post-verify TERM restores both images and persistent digests'

reset_case
export FAKE_FAIL=rollback_fail
if bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1; then echo 'FAIL: accepted rollback failure' >&2; exit 1; fi
[ -e "$REPO/.update-lock" ]
grep -Fx "JM_MANAGER_DIGEST=$OLD_M" "$REPO/server/.env" >/dev/null
echo 'ok: failed rollback retains operator lock'
