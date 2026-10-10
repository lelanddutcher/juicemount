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
reset_case
bash "$SCRIPT" "${args[@]}" --dry-run > "$WORK/out"
[ ! -s "$FAKE_LOG" ] && [ ! -e "$REPO/.update-lock" ]
echo 'ok: dry run has no side effects'

reset_case
bash "$SCRIPT" "${args[@]}" > "$WORK/out"
[ "$(cat "$FAKE_STATE")" = new ] && [ ! -e "$REPO/.update-lock" ]
grep -Fx "JM_MANAGER_DIGEST=$NEW_M" "$REPO/server/.env" >/dev/null
grep -Fx "JM_FARM_DIGEST=$NEW_F" "$REPO/server/.env" >/dev/null
grep -Fx "JM_FARM_RENDER_DIGEST=$NEW_R" "$REPO/server/.env" >/dev/null
grep -Fx 'JM_ADMIN_KEY=0123456789abcdef0123456789abcdef' "$REPO/server/.env" >/dev/null
! grep -q '0123456789abcdef0123456789abcdef' "$WORK/out"
[ "$(grep -c 'docker compose .* up ' "$FAKE_LOG")" = 1 ]
grep -F -- '--no-deps --no-build --pull never --force-recreate juicemount-manager juicefarm' "$FAKE_LOG" >/dev/null
assert_no_infra_mutation
echo 'ok: only the two requested services are recreated after immutable preflight'

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
