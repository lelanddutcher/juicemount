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
mkdir -p "$REPO/server/juicefarm" "$REPO/server/juicemount-manager"
touch "$REPO/go.mod" "$REPO/server/juicefarm/Dockerfile" "$REPO/server/juicemount-manager/Dockerfile"

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
P_BUILD_FARM="$(plan_pos "juicefarm:local")"
P_BUILD_MGR="$(plan_pos "juicemount-manager:local")"
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
rs() { grep -q -- "$1" "$SSH_STDIN"; }
rs 'DOCKER_BUILDKIT=1 docker build -f server/juicefarm/Dockerfile -t "$FARM_IMAGE" .' \
  && pass "remote builds the farm image from server/juicefarm/Dockerfile" \
  || fail "remote farm build missing"
rs 'docker build -f server/juicemount-manager/Dockerfile -t "$MANAGER_IMAGE" .' \
  && pass "remote builds the manager image" \
  || fail "remote manager build missing"
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
rs 'HARD GUARD' \
  && pass "remote carries the infra hard guard" \
  || fail "remote infra guard missing"

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
