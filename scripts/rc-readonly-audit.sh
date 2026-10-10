#!/usr/bin/env bash
# rc-readonly-audit.sh — non-mutating final-candidate release gate.
#
# This script only performs bounded SSH/docker/Redis/ZFS reads. It never starts,
# stops, restarts, deploys, or edits a service. A non-zero exit means that at
# least one live gate is not proven; the output intentionally omits media paths.
#
# Required environment:
#   JM_TRUENAS_HOST, JM_RENDER_HOST, JM_SSH_USER, JM_RENDER_SSH_USER, JM_SSH_KEY
#   JM_EXPECTED_MANAGER, JM_EXPECTED_CPU, JM_EXPECTED_GPU
set -euo pipefail

TRUENAS_HOST="${JM_TRUENAS_HOST:?set the TrueNAS host for this audit}"
RENDER_HOST="${JM_RENDER_HOST:?set the render host for this audit}"
SSH_USER="${JM_SSH_USER:?set the TrueNAS SSH user for this audit}"
RENDER_USER="${JM_RENDER_SSH_USER:?set the render SSH user for this audit}"
SSH_KEY="${JM_SSH_KEY:?set the SSH key path for this audit}"
EXPECTED_MANAGER="${JM_EXPECTED_MANAGER:?set the release Manager image digest}"
EXPECTED_CPU="${JM_EXPECTED_CPU:?set the release CPU image digest}"
EXPECTED_GPU="${JM_EXPECTED_GPU:?set the release GPU image digest}"
SOURCE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXPECTED_COMMIT="$(git -C "$SOURCE_ROOT" rev-parse HEAD)"

for digest in "$EXPECTED_MANAGER" "$EXPECTED_CPU" "$EXPECTED_GPU"; do
  if [[ ! "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    printf 'invalid release image digest: expected sha256:<64 lowercase hex>\n' >&2
    exit 2
  fi
done
if ! git -C "$SOURCE_ROOT" diff --quiet ||
   ! git -C "$SOURCE_ROOT" diff --cached --quiet ||
   [[ -n "$(git -C "$SOURCE_ROOT" ls-files --others --exclude-standard -- . ':(exclude)build/**')" ]]; then
  # build/ is excluded from the Docker context by .dockerignore.
  printf 'release source has uncommitted or untracked build inputs; freeze the candidate before auditing\n' >&2
  exit 2
fi

failures=0
SSH_OPTS=(-o BatchMode=yes -o PasswordAuthentication=no -o KbdInteractiveAuthentication=no -o ConnectTimeout=8 -i "$SSH_KEY")

say() { printf '%s\n' "$*"; }
pass() { say "PASS $*"; }
fail() { say "FAIL $*"; failures=$((failures + 1)); }

ssh_read() {
  local host="$1" user="$2" command="$3"
  ssh "${SSH_OPTS[@]}" "$user@$host" "$command"
}

inspect_digest() {
  local host="$1" user="$2" name="$3" expected="$4"
  local details actual state image_id revision
  details="$(ssh_read "$host" "$user" "docker inspect -f '{{.Config.Image}}|{{.State.Status}}|{{.Image}}' '$name' 2>/dev/null || true")"
  IFS='|' read -r actual state image_id <<< "$details"
  revision=""
  if [[ "$image_id" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    revision="$(ssh_read "$host" "$user" "docker image inspect -f '{{index .Config.Labels \"org.opencontainers.image.revision\"}}' '$image_id' 2>/dev/null || true")"
  fi
  if [[ "$actual" == *"@$expected" && "$state" == "running" && "$revision" == "$EXPECTED_COMMIT" ]]; then
    pass "$name digest and source commit match ($state)"
  else
    fail "$name digest, source commit, or running state mismatch (state=$state)"
  fi
}

say "JuiceMount RC read-only audit"
say "TrueNAS=$TRUENAS_HOST render=$RENDER_HOST"
say "Source commit=$EXPECTED_COMMIT"

inspect_digest "$TRUENAS_HOST" "$SSH_USER" ix-juicemount-juicemount-manager-1 "$EXPECTED_MANAGER"

net_env="$(ssh_read "$TRUENAS_HOST" "$SSH_USER" "docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' ix-juicemount-juicemount-manager-1 2>/dev/null | grep -E '^JM_NET_(HEADSCALE|NAS_NODE|ALLOW_INSECURE|ALLOW_LAN_ONLY)=' || true")"
net_value() {
  local key="$1"
  printf '%s\n' "$net_env" | awk -F= -v key="$key" '$1 == key {print substr($0, length(key) + 2)}'
}
if [[ "$(net_value JM_NET_HEADSCALE)" == "off" &&
      "$(net_value JM_NET_NAS_NODE)" == "off" &&
      "$(net_value JM_NET_ALLOW_INSECURE)" == "0" &&
      "$(net_value JM_NET_ALLOW_LAN_ONLY)" == "0" ]]; then
  pass "Manager uses the 0.5 system private route posture"
else
  fail "Manager remote posture differs from the 0.5 release configuration"
fi

cpu_name="$(ssh_read "$TRUENAS_HOST" "$SSH_USER" "docker ps --format '{{.Names}}|{{.Status}}' | awk -F'|' '(\$1 == \"ix-juicemount-juicefarm-1\" || \$1 ~ /^juicefarm-worker-rc0.5-/) && \$2 ~ /^Up / {print \$1}'")"
if [[ "$cpu_name" == "ix-juicemount-juicefarm-1" || "$cpu_name" =~ ^juicefarm-worker-rc0\.5-[A-Za-z0-9_.-]+$ ]]; then
  inspect_digest "$TRUENAS_HOST" "$SSH_USER" "$cpu_name" "$EXPECTED_CPU"
else
  fail "expected exactly one active CPU worker"
fi

inspect_digest "$RENDER_HOST" "$RENDER_USER" juicefarm-gpu-rc0.5 "$EXPECTED_GPU"

unit_enabled="$(ssh_read "$RENDER_HOST" "$RENDER_USER" "systemctl is-enabled juicefarm-gpu-rc0.5.service 2>&1 || true")"
unit_active="$(ssh_read "$RENDER_HOST" "$RENDER_USER" "systemctl is-active juicefarm-gpu-rc0.5.service 2>&1 || true")"
if [[ "$unit_enabled" == "enabled" && "$unit_active" == "active" ]]; then
  pass "GPU recovery unit is enabled and active"
else
  fail "GPU recovery unit is not enabled/active"
fi

redis_read="$(ssh_read "$TRUENAS_HOST" "$SSH_USER" "docker exec ix-juicemount-redis-1 redis-cli -n 1 LLEN juicefarm:queue:proxy:cpu; docker exec ix-juicemount-redis-1 redis-cli -n 1 LLEN juicefarm:queue:transcript:cpu; docker exec ix-juicemount-redis-1 redis-cli -n 1 LLEN juicefarm:queue:derivatives:server")"
queue_proxy="$(printf '%s\n' "$redis_read" | sed -n '1p')"
queue_transcript="$(printf '%s\n' "$redis_read" | sed -n '2p')"
queue_derivatives="$(printf '%s\n' "$redis_read" | sed -n '3p')"
if [[ "$queue_proxy" =~ ^[0-9]+$ && "$queue_transcript" =~ ^[0-9]+$ && "$queue_derivatives" =~ ^[0-9]+$ ]]; then
  pass "Redis queues readable (cpu-proxy=$queue_proxy cpu-transcript=$queue_transcript server-derivatives=$queue_derivatives)"
else
  fail "Redis queue snapshot unavailable"
fi

storage="$(ssh_read "$TRUENAS_HOST" "$SSH_USER" "docker exec ix-juicemount-redis-1 redis-cli -n 1 GET juicefarm:storage:permit; zpool list -H -p -o health,free,cap zSSD")"
if [[ "$storage" == *"available_bytes"* && "$storage" == *"ONLINE"* ]]; then
  pass "storage permit valid and zSSD ONLINE"
else
  fail "storage permit or zSSD health not proven"
fi

if [[ "$failures" -eq 0 ]]; then
  say "RESULT PASS"
else
  say "RESULT NO-SHIP ($failures gate(s) open)"
fi
exit "$failures"
