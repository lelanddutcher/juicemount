#!/usr/bin/env bash
#
# update-server.test.sh — offline self-test for update-server.sh.
#
# No network, no docker: ssh/rsync/docker are stubbed via PATH shims that log
# every invocation. Asserts:
#   1. --dry-run executes NOTHING (zero ssh/rsync/docker calls) and prints the
#      full ordered plan (trailing-slash rsync, both image builds, recreate of
#      exactly the two target containers, verification, the infra warning).
#   2. a real run drives rsync THEN ssh, pipes the remote script over ssh
#      stdin, and that script never stops/rms/restarts redis/minio/juicefs.
#   3. the local guard refuses protected container names outright.
#   4. an untracked deployable file is refused before rsync/ssh.
#   5. failed replacement and failed health checks restore both old containers.
#
# Run: bash server/scripts/update-server.test.sh

set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
UPDATE_SH="$SCRIPT_DIR/update-server.sh"
TEST_HOST="root@192.0.2.10"
update_server() { "$UPDATE_SH" --host "$TEST_HOST" "$@"; }
FAILURES=0

fail() { echo "FAIL: $*" >&2; FAILURES=$((FAILURES + 1)); }
pass() { echo "  ok: $*"; }

# ---- workspace: fake repo + PATH shims --------------------------------------
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

REPO="$WORK/repo"
mkdir -p "$REPO/internal/version" "$REPO/server/juicefarm" "$REPO/server/juicemount-manager"
touch "$REPO/go.mod" "$REPO/server/juicefarm/Dockerfile" "$REPO/server/juicemount-manager/Dockerfile"
printf 'package version\n\nvar Version = "0.5.0"\n' > "$REPO/internal/version/version.go"
git -C "$REPO" init -q
git -C "$REPO" config user.name test
git -C "$REPO" config user.email test@example.invalid
git -C "$REPO" add .
git -C "$REPO" commit -qm fixture
FAKE_COMMIT="$(git -C "$REPO" rev-parse HEAD)"
FAKE_SHORT="${FAKE_COMMIT:0:7}"

SHIMS="$WORK/shims"
mkdir -p "$SHIMS"
CALLS="$WORK/calls.log"
SSH_STDIN="$WORK/ssh-stdin.txt"
: > "$CALLS"

for tool in ssh rsync docker; do
  cat > "$SHIMS/$tool" <<EOF
#!/usr/bin/env bash
echo "$tool \$*" >> "$CALLS"
if [ "$tool" = "ssh" ]; then
  cat > "$WORK/ssh-last"
  if grep -Fq 'mkdir .update-lock' "$WORK/ssh-last"; then
    cp "$WORK/ssh-last" "$WORK/ssh-acquire"
  elif grep -Fq 'recreate ' "$WORK/ssh-last"; then
    cp "$WORK/ssh-last" "$SSH_STDIN"
  else
    cp "$WORK/ssh-last" "$WORK/ssh-release"
  fi
fi
exit 0
EOF
  chmod +x "$SHIMS/$tool"
done
export PATH="$SHIMS:$PATH"

# =============================================================================
echo "== test 1: --dry-run prints the ordered plan and executes nothing =="
: > "$CALLS"
OUT="$(update_server --repo "$REPO" --dry-run 2>&1)" || fail "dry-run exited non-zero"

if [ -s "$CALLS" ]; then
  fail "dry-run invoked external tools:"$'\n'"$(cat "$CALLS")"
else
  pass "dry-run made zero ssh/rsync/docker calls"
fi

# Ordered plan markers (grep -n line positions must ascend).
plan_pos() { printf '%s\n' "$OUT" | grep -n -- "$1" | head -1 | cut -d: -f1; }
P_RSYNC="$(plan_pos "rsync")"
P_SLASH="$(printf '%s\n' "$OUT" | grep -n -- "$REPO/" | head -1 | cut -d: -f1)"
P_BUILD_FARM="$(plan_pos "juicefarm:rc-0.5-$FAKE_SHORT")"
P_BUILD_MGR="$(plan_pos "juicemount-manager:rc-0.5-$FAKE_SHORT")"
P_STOP="$(plan_pos "docker stop")"
P_RUN="$(plan_pos "docker run")"
P_VERIFY="$(plan_pos "/api/farm")"
P_GUARD="$(plan_pos "NEVER touch redis / minio / juicefs / respproxy")"

for var in P_RSYNC P_SLASH P_BUILD_FARM P_BUILD_MGR P_STOP P_RUN P_VERIFY P_GUARD; do
  if [ -z "${!var}" ]; then fail "dry-run plan missing marker: $var"; fi
done
if [ -n "$P_RSYNC" ] && [ -n "$P_BUILD_FARM" ] && [ -n "$P_STOP" ] && [ -n "$P_VERIFY" ]; then
  if [ "$P_RSYNC" -lt "$P_BUILD_FARM" ] && [ "$P_BUILD_FARM" -lt "$P_STOP" ] && [ "$P_STOP" -lt "$P_VERIFY" ]; then
    pass "plan is ordered: rsync → build → recreate → verify"
  else
    fail "plan out of order: rsync=$P_RSYNC build=$P_BUILD_FARM stop=$P_STOP verify=$P_VERIFY"
  fi
fi

# The trailing-slash contents-sync gotcha is explicitly encoded.
if printf '%s\n' "$OUT" | grep -q -- "$REPO/ "; then
  pass "rsync source carries the trailing slash (contents sync)"
else
  # rsync arg is %q-printed; accept quoted form too
  if printf '%s\n' "$OUT" | grep -q -- "$REPO/"; then
    pass "rsync source carries the trailing slash (contents sync)"
  else
    fail "rsync source is missing the trailing slash"
  fi
fi

# The plan must never stop/restart infra containers.
if printf '%s\n' "$OUT" | grep -E "docker (stop|rm|restart) .*(redis|minio|juicefs)" >/dev/null; then
  fail "dry-run plan touches infra containers"
else
  pass "plan never touches redis/minio/juicefs"
fi

# =============================================================================
echo "== test 2: real run = lock, rsync, deploy, unlock =="
: > "$CALLS"; : > "$SSH_STDIN"
update_server --repo "$REPO" >/dev/null 2>&1 || fail "real (shimmed) run exited non-zero"

if [ "$(grep -c '^rsync ' "$CALLS")" -eq 1 ] && [ "$(grep -c '^ssh ' "$CALLS")" -eq 3 ]; then
  pass "exactly one rsync and three SSH phases"
else
  fail "unexpected call set:"$'\n'"$(cat "$CALLS")"
fi
TOOL_ORDER="$(awk '{printf "%s ", $1}' "$CALLS")"
if [ "$TOOL_ORDER" = "ssh rsync ssh ssh " ]; then
  pass "remote lock spans rsync and deployment"
else
  fail "call order wrong: $TOOL_ORDER"
fi

# rsync: contents-slash source + default destination.
if grep -q "^rsync .*$REPO/ $TEST_HOST:/root/juicefarm-build/$" "$CALLS"; then
  pass "rsync syncs worktree CONTENTS to /root/juicefarm-build/"
else
  fail "rsync invocation unexpected: $(grep '^rsync' "$CALLS")"
fi

# ssh target.
if grep -q "^ssh $TEST_HOST bash -s$" "$CALLS"; then
  pass "remote script piped via ssh bash -s"
else
  fail "ssh invocation unexpected: $(grep '^ssh' "$CALLS")"
fi

# Remote script content assertions.
rs() { grep -Fq -- "$1" "$SSH_STDIN"; }
rs '--build-arg "JM_VERSION=$SOURCE_VERSION"' \
  && rs '--build-arg "JM_COMMIT=$SOURCE_COMMIT"' \
  && rs '-f server/juicefarm/Dockerfile -t "$FARM_IMAGE" .' \
  && pass "remote builds the farm image from server/juicefarm/Dockerfile" \
  || fail "remote farm build missing"
rs '-f server/juicemount-manager/Dockerfile -t "$MANAGER_IMAGE" .' \
  && pass "remote builds the manager image" \
  || fail "remote manager build missing"
rs 'SOURCE_VERSION=0.5.0' && rs "SOURCE_COMMIT=$FAKE_COMMIT" \
  && pass "remote build is stamped with the exact clean source identity" \
  || fail "remote exact source identity missing"
rs 'docker run --rm --entrypoint "/usr/local/bin/$binary" "$image" --build-info' \
  && pass "remote executes both built images to verify embedded identity" \
  || fail "remote image identity verification missing"
rs 'FARM_CONTAINER=juicefarm-worker' && rs 'MANAGER_CONTAINER=juicemount-manager' \
  && pass "remote targets exactly juicefarm-worker + juicemount-manager" \
  || fail "remote container targets wrong"
rs 'recreate "$FARM_CONTAINER" "$FARM_IMAGE"' && rs 'recreate "$MANAGER_CONTAINER" "$MANAGER_IMAGE"' \
  && pass "remote recreates both target containers" \
  || fail "remote recreate calls missing"
rs 'docker inspect' \
  && pass "remote introspects run args via docker inspect (not hardcoded)" \
  || fail "remote docker inspect missing"
rs '/api/farm' \
  && pass "remote verifies manager /api/farm" \
  || fail "remote manager verification missing"
rs 'docker exec "$FARM_CONTAINER" /usr/local/bin/jmfarm --build-info' \
  && rs 'docker exec "$MANAGER_CONTAINER" /usr/local/bin/juicemount-manager --build-info' \
  && pass "remote verifies running container identities after recreation" \
  || fail "running-container identity verification missing"
rs 'HARD GUARD' \
  && pass "remote carries the infra hard guard" \
  || fail "remote infra guard missing"
rs 'deployment lock is missing or belongs to another updater' \
  && pass "remote deployment requires owned lock" \
  || fail "remote deployment lock ownership check missing"
grep -Fq 'mkdir .update-lock' "$WORK/ssh-acquire" \
  && grep -Fq 'rmdir .update-lock' "$WORK/ssh-release" \
  && pass "remote lock is atomic and explicitly released" \
  || fail "remote lock acquire/release missing"

# Manager authentication must be validated before either workload is touched.
P_AUTH="$(grep -nF 'preflight_manager_auth' "$SSH_STDIN" | tail -1 | cut -d: -f1)"
P_FARM_RECREATE="$(grep -nF 'recreate "$FARM_CONTAINER" "$FARM_IMAGE"' "$SSH_STDIN" | head -1 | cut -d: -f1)"
if [ -n "$P_AUTH" ] && [ -n "$P_FARM_RECREATE" ] && [ "$P_AUTH" -lt "$P_FARM_RECREATE" ]; then
  pass "manager auth preflight runs before any workload recreation"
else
  fail "manager auth preflight is absent or too late"
fi
rs 'manager runtime JM_ADMIN_KEY is missing or shorter than 32 characters' \
  && pass "remote refuses a missing/short Manager key" \
  || fail "remote Manager-key refusal missing"

# Runtime credential values must be inherited by name, never copied into argv.
rs 'env_assignments+=("$e")' && rs 'args+=(-e "$key")' && rs 'export "$e"' \
  && pass "runtime environment values stay out of docker argv" \
  || fail "safe runtime environment forwarding missing"
if grep -F 'args+=(-e "$e")' "$SSH_STDIN" >/dev/null; then
  fail "remote still copies full KEY=value entries into docker argv"
else
  pass "remote docker argv contains environment names only"
fi

# Verification must not extract the secret onto the host or place it in curl argv.
if grep -F 'ADMIN_KEY="$(docker inspect' "$SSH_STDIN" >/dev/null; then
  fail "remote verifier extracts Manager key into a host command"
else
  pass "remote verifier does not extract Manager key on the host"
fi
rs 'curl --config -' \
  && pass "Manager verification streams the auth header over stdin" \
  || fail "Manager verification does not use stdin curl config"
rs 'map((split("=")[0]) + "=<redacted>")' \
  && pass "container config backups redact every environment value" \
  || fail "credential-redacted container config backup missing"
if grep -F 'docker inspect "$name" > "$backup"' "$SSH_STDIN" >/dev/null; then
  fail "remote still writes raw docker-inspect credentials to backup files"
else
  pass "remote never writes raw docker-inspect environment values to backups"
fi

# The remote script must never name an infra container in a mutating command.
if grep -E 'docker (stop|rm|restart|kill) .*(redis|minio|juicefs)' "$SSH_STDIN" >/dev/null; then
  fail "remote script mutates infra containers"
else
  pass "remote script never stops/rms/restarts redis/minio/juicefs"
fi

# =============================================================================
echo "== test 3: local guard refuses protected container names =="
: > "$CALLS"
if update_server --repo "$REPO" --farm-container ix-juicefs-1 >/dev/null 2>&1; then
  fail "guard let a juicefs-named container through"
else
  pass "refused --farm-container ix-juicefs-1"
fi
if [ -s "$CALLS" ]; then
  fail "guard refusal still invoked tools: $(cat "$CALLS")"
else
  pass "refusal executed nothing"
fi
if update_server --repo "$REPO" --manager-container redis >/dev/null 2>&1; then
  fail "guard let a redis-named container through"
else
  pass "refused --manager-container redis"
fi
: > "$CALLS"
if update_server --repo "$REPO" --farm-container ix-juicemount-respproxy-1 >/dev/null 2>&1; then
  fail "guard let the H-8B respproxy container through"
else
  pass "refused --farm-container ix-juicemount-respproxy-1"
fi
if [ -s "$CALLS" ]; then
  fail "respproxy refusal still invoked tools: $(cat "$CALLS")"
fi

# =============================================================================
echo "== test 4: flag overrides reach the plan =="
OUT="$(update_server --repo "$REPO" --host root@192.0.2.11 --dest /opt/build \
        --farm-container farm-x --manager-container mgr-y --dry-run 2>&1)" \
  || fail "override dry-run exited non-zero"
printf '%s\n' "$OUT" | grep -q "root@192.0.2.11" || fail "--host override not in plan"
printf '%s\n' "$OUT" | grep -q "/opt/build/" || fail "--dest override not in plan"
printf '%s\n' "$OUT" | grep -q "FARM_CONTAINER=farm-x" || fail "--farm-container override not in plan"
printf '%s\n' "$OUT" | grep -q "MANAGER_CONTAINER=mgr-y" || fail "--manager-container override not in plan"
pass "host/dest/container overrides propagate"

# =============================================================================
echo "== test 5: source identity and backup-preservation guards =="
printf 'package newsource\n' > "$REPO/untracked.go"
: > "$CALLS"
if update_server --repo "$REPO" >/dev/null 2>&1; then
  fail "untracked deployable source was accepted"
else
  pass "untracked deployable source refused"
fi
if [ -s "$CALLS" ]; then fail "untracked source guard invoked network tools"; fi
rm "$REPO/untracked.go"
OUT="$(update_server --repo "$REPO" --dry-run 2>&1)" || fail "backup guard dry-run failed"
if printf '%s\n' "$OUT" | grep -F -- 'rsync  -a' | grep -Fq -- '.update-backups/' \
  && printf '%s\n' "$OUT" | grep -F -- 'rsync  -a' | grep -Fq -- '.update-lock/'; then
  pass "rsync protects remote backups and deployment lock from --delete"
else
  fail "rsync lacks remote backup or lock protection"
fi

# =============================================================================
echo "== test 6: remote failure injection restores prior working containers =="
REMOTE_DIR="$WORK/remote"
mkdir -p "$REMOTE_DIR"
update_server --repo "$REPO" --dest "$REMOTE_DIR" >/dev/null 2>&1 \
  || fail "could not capture remote program for fault injection"

STATE="$WORK/docker-state"
export STATE FAKE_COMMIT
if bash "$WORK/ssh-acquire" >/dev/null 2>&1; then
  pass "remote lock can be acquired"
else
  fail "remote lock acquisition failed"
fi
if bash "$WORK/ssh-acquire" >/dev/null 2>&1; then
  fail "second remote lock holder was accepted"
else
  pass "second remote lock holder refused"
fi
cat > "$SHIMS/docker" <<'DOCKER_SHIM'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$STATE/calls"
cmd="$1"; shift
case "$cmd" in
  build) exit 0 ;;
  image)
    [ "$1" = inspect ] || exit 90
    if [ "${2:-}" != -f ]; then printf '[{"Config":{"Labels":{}}}]\n'; exit 0; fi
    case "$3" in *Config.Cmd*) printf 'null\n';; *Config.Entrypoint*) printf '["/entrypoint.sh"]\n';; esac
    exit 0 ;;
  inspect)
    format=""
    if [ "${1:-}" = -f ]; then format="$2"; shift 2; fi
    name="$1"
    [ -f "$STATE/$name" ] || exit 1
    case "$format" in
      *State.Running*) if [ -f "$STATE/$name.running" ]; then echo true; else echo false; fi ;;
      *Config.Labels*) if [ "${DOCKER_INJECT:-}" = compose ] && [ "$name" = juicemount-manager ]; then echo com.docker.compose.project; fi ;;
      *Config.Entrypoint*) if [ "${DOCKER_INJECT:-}" = entrypoint ] && [ "$name" = juicefarm-worker ]; then echo '["/custom"]'; else echo '["/entrypoint.sh"]'; fi ;;
      *\{\{.Image\}\}*) echo "old-$name" ;;
      *IPAMConfig*) if [ "${DOCKER_INJECT:-}" = static-ip ] && [ "$name" = juicefarm-worker ]; then echo 'static IP'; fi ;;
      *Aliases*) if [ "${DOCKER_INJECT:-}" = aliases ] && [ "$name" = juicefarm-worker ]; then echo farm-service; fi ;;
      *Config.Env*) if [[ "$name" == *manager* ]]; then echo 'JM_ADMIN_KEY=12345678901234567890123456789012'; else echo 'JM_TEST=fixture'; fi ;;
      *Config.Image*) echo "old-$name" ;;
      *RestartPolicy.Name*) echo no ;;
      *RestartPolicy.MaximumRetryCount*) echo 0 ;;
      *HostConfig.NetworkMode*) echo bridge ;;
      *HostConfig.Privileged*) echo false ;;
      *HostConfig.NanoCpus*|*HostConfig.Memory*) echo 0 ;;
      *Config.Cmd*) echo null ;;
      *Config.User*) echo '' ;;
      *Mounts*|*PortBindings*|*HostConfig.CapAdd*|*HostConfig.Devices*|*HostConfig.SecurityOpt*|*HostConfig.ExtraHosts*|*NetworkSettings.Networks*) echo '' ;;
      '') if [ "${DOCKER_INJECT:-}" = runtime-label ] && [ "$name" = juicefarm-worker ]; then printf '[{"Config":{"Env":["JM_ADMIN_KEY=secret"],"Labels":{"custom":"yes"}}}]\n'; else printf '[{"Config":{"Env":["JM_ADMIN_KEY=secret"],"Labels":{}}}]\n'; fi ;;
      *) echo "unhandled docker inspect format: $format" >&2; exit 91 ;;
    esac ;;
  run)
    if [[ " $* " == *' --entrypoint '* ]]; then
      if [[ " $* " == *'/jmfarm '* ]]; then echo "jmfarm 0.5.0 ($FAKE_COMMIT)"; else echo "juicemount-manager 0.5.0 ($FAKE_COMMIT)"; fi
      exit 0
    fi
    name=""
    previous=""
    for arg in "$@"; do
      if [ "$previous" = --name ]; then name="$arg"; break; fi
      previous="$arg"
    done
    [ -n "$name" ] || exit 92
    if [ "${DOCKER_INJECT:-}" = manager-run ] && [ "$name" = juicemount-manager ]; then exit 42; fi
    if [ "${DOCKER_INJECT:-}" = farm-run ] && [ "$name" = juicefarm-worker ]; then exit 42; fi
    echo new > "$STATE/$name"
    touch "$STATE/$name.running"
    echo "$name" ;;
  stop) name="$1"; [ -f "$STATE/$name" ] || exit 1; rm -f "$STATE/$name.running"; echo "$name" ;;
  start) name="$1"; [ -f "$STATE/$name" ] || exit 1; touch "$STATE/$name.running"; echo "$name" ;;
  rename)
    old="$1"; new="$2"
    if [ "${DOCKER_INJECT:-}" = manager-rename ] && [ "$old" = juicemount-manager ]; then exit 42; fi
    [ -f "$STATE/$old" ] && [ ! -e "$STATE/$new" ] || exit 1
    mv "$STATE/$old" "$STATE/$new"
    if [ -f "$STATE/$old.running" ]; then mv "$STATE/$old.running" "$STATE/$new.running"; fi ;;
  rm) name="$1"; [ -f "$STATE/$name" ] || exit 1; rm -f "$STATE/$name" "$STATE/$name.running"; echo "$name" ;;
  exec)
    name="$1"; shift
    [ -f "$STATE/$name.running" ] || exit 1
    if [[ " $* " == *'--build-info'* ]]; then
      if [ "$name" = juicefarm-worker ]; then echo "jmfarm 0.5.0 ($FAKE_COMMIT)"; else echo "juicemount-manager 0.5.0 ($FAKE_COMMIT)"; fi
    elif [ "${DOCKER_INJECT:-}" = manager-health ]; then echo 503
    else echo 200; fi ;;
  logs|network) exit 0 ;;
  *) echo "unhandled docker command: $cmd" >&2; exit 93 ;;
esac
DOCKER_SHIM
chmod +x "$SHIMS/docker"
printf '#!/usr/bin/env bash\nexit 0\n' > "$SHIMS/sleep"
chmod +x "$SHIMS/sleep"

fresh_state() {
  rm -rf "$STATE"
  mkdir -p "$STATE"
  echo old > "$STATE/juicefarm-worker"
  echo old > "$STATE/juicemount-manager"
  touch "$STATE/juicefarm-worker.running" "$STATE/juicemount-manager.running"
  : > "$STATE/calls"
}
assert_old_restored() {
  local label="$1"
  if [ "$(cat "$STATE/juicefarm-worker" 2>/dev/null)" = old ] \
    && [ "$(cat "$STATE/juicemount-manager" 2>/dev/null)" = old ] \
    && [ -f "$STATE/juicefarm-worker.running" ] \
    && [ -f "$STATE/juicemount-manager.running" ] \
    && ! find "$STATE" -name '*pre-update*' | grep -q .; then
    pass "$label restores both prior running containers"
  else
    fail "$label did not restore both prior running containers"
  fi
}
for injection in farm-run manager-rename manager-run manager-health; do
  fresh_state
  if DOCKER_INJECT="$injection" bash "$SSH_STDIN" > "$WORK/$injection.log" 2>&1; then
    fail "$injection unexpectedly succeeded"
  else
    assert_old_restored "$injection"
  fi
  if grep -E '^(stop|rm|rename|start) (redis|minio|juicefs)' "$STATE/calls" >/dev/null; then
    fail "$injection touched protected infrastructure"
  fi
done

for injection in compose aliases static-ip runtime-label entrypoint; do
  fresh_state
  if DOCKER_INJECT="$injection" bash "$SSH_STDIN" > "$WORK/$injection.log" 2>&1; then
    fail "$injection unsupported topology was accepted"
  elif grep -E '^(stop|rm|rename) ' "$STATE/calls" >/dev/null; then
    fail "$injection stopped a workload before refusal"
  else
    pass "$injection topology refused before workload mutation"
  fi
done

fresh_state
if bash "$SSH_STDIN" > "$WORK/success.log" 2>&1; then
  if [ "$(cat "$STATE/juicefarm-worker")" = new ] \
    && [ "$(cat "$STATE/juicemount-manager")" = new ] \
    && ! find "$STATE" -name '*pre-update*' | grep -q .; then
    pass "successful verification accepts new workloads and prunes predecessors"
  else
    fail "successful verification left unexpected container state"
  fi
else
  fail "fault-free remote update failed: $(tail -8 "$WORK/success.log")"
fi
if bash "$WORK/ssh-release" >/dev/null 2>&1; then
  pass "remote lock released after deployment"
else
  fail "remote lock release failed"
fi
fresh_state
if bash "$SSH_STDIN" > "$WORK/unlocked.log" 2>&1; then
  fail "remote deployment ran without its lock"
elif [ -s "$STATE/calls" ]; then
  fail "remote deployment touched Docker before checking its lock"
else
  pass "remote deployment refuses a missing lock before Docker activity"
fi

# =============================================================================
if [ "$FAILURES" -gt 0 ]; then
  echo "== $FAILURES assertion(s) FAILED =="
  exit 1
fi
echo "== all update-server self-tests passed =="
