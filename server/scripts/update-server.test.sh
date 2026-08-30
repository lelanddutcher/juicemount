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
#
# Run: bash server/scripts/update-server.test.sh

set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
UPDATE_SH="$SCRIPT_DIR/update-server.sh"
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
if [ "$tool" = "ssh" ]; then cat > "$SSH_STDIN"; fi
exit 0
EOF
  chmod +x "$SHIMS/$tool"
done
export PATH="$SHIMS:$PATH"

# =============================================================================
echo "== test 1: --dry-run prints the ordered plan and executes nothing =="
: > "$CALLS"
OUT="$("$UPDATE_SH" --repo "$REPO" --dry-run 2>&1)" || fail "dry-run exited non-zero"

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
P_GUARD="$(plan_pos "NEVER touch redis / minio / juicefs")"

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
echo "== test 2: real run = rsync then ssh, remote script safe + complete =="
: > "$CALLS"; : > "$SSH_STDIN"
"$UPDATE_SH" --repo "$REPO" >/dev/null 2>&1 || fail "real (shimmed) run exited non-zero"

if [ "$(grep -c '^rsync ' "$CALLS")" -eq 1 ] && [ "$(grep -c '^ssh ' "$CALLS")" -eq 1 ]; then
  pass "exactly one rsync and one ssh"
else
  fail "unexpected call set:"$'\n'"$(cat "$CALLS")"
fi
FIRST_TOOL="$(head -1 "$CALLS" | awk '{print $1}')"
SECOND_TOOL="$(sed -n '2p' "$CALLS" | awk '{print $1}')"
if [ "$FIRST_TOOL" = "rsync" ] && [ "$SECOND_TOOL" = "ssh" ]; then
  pass "rsync runs before ssh"
else
  fail "call order wrong: $FIRST_TOOL then $SECOND_TOOL"
fi

# rsync: contents-slash source + default destination.
if grep -q "^rsync .*$REPO/ root@192.168.0.197:/root/juicefarm-build/$" "$CALLS"; then
  pass "rsync syncs worktree CONTENTS to /root/juicefarm-build/"
else
  fail "rsync invocation unexpected: $(grep '^rsync' "$CALLS")"
fi

# ssh target.
if grep -q "^ssh root@192.168.0.197 bash -s$" "$CALLS"; then
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
if "$UPDATE_SH" --repo "$REPO" --farm-container ix-juicefs-1 >/dev/null 2>&1; then
  fail "guard let a juicefs-named container through"
else
  pass "refused --farm-container ix-juicefs-1"
fi
if [ -s "$CALLS" ]; then
  fail "guard refusal still invoked tools: $(cat "$CALLS")"
else
  pass "refusal executed nothing"
fi
if "$UPDATE_SH" --repo "$REPO" --manager-container redis >/dev/null 2>&1; then
  fail "guard let a redis-named container through"
else
  pass "refused --manager-container redis"
fi

# =============================================================================
echo "== test 4: flag overrides reach the plan =="
OUT="$("$UPDATE_SH" --repo "$REPO" --host root@10.0.0.5 --dest /opt/build \
        --farm-container farm-x --manager-container mgr-y --dry-run 2>&1)" \
  || fail "override dry-run exited non-zero"
printf '%s\n' "$OUT" | grep -q "root@10.0.0.5" || fail "--host override not in plan"
printf '%s\n' "$OUT" | grep -q "/opt/build/" || fail "--dest override not in plan"
printf '%s\n' "$OUT" | grep -q "FARM_CONTAINER=farm-x" || fail "--farm-container override not in plan"
printf '%s\n' "$OUT" | grep -q "MANAGER_CONTAINER=mgr-y" || fail "--manager-container override not in plan"
pass "host/dest/container overrides propagate"

# =============================================================================
if [ "$FAILURES" -gt 0 ]; then
  echo "== $FAILURES assertion(s) FAILED =="
  exit 1
fi
echo "== all update-server self-tests passed =="
