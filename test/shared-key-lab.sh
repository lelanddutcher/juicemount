#!/bin/bash
# Shared object key lab: revoke a Mac that holds the shared jm-client key,
# then rotate the key as server/COMPOSE-ROLLOUT.md ("Rotating the shared
# object key") says, against a real object store: the SILO image that
# server/docker-compose.yml pins for `minio`, provisioned by the real
# server/minio/provision-object-keys.sh (minio-keys).
#
# The Go side is TestSharedKeyRevokeRotationLab (internal/manager). It
# prints one "LAB |" row per check; any wrong row fails the test.
#
#   test/shared-key-lab.sh            # needs docker and go
#
# Every secret is random and lab-only. Secrets reach the container through
# `docker exec -e NAME` (the value comes from this process's environment,
# never argv) and mc's stdin. The container is removed on exit. Set TMPDIR
# to your scratch directory: the lab writes its files under it.
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
img="$(grep -m1 -oE 'pgsty/silo:[^[:space:]]+' "$repo/server/docker-compose.yml")"
[ -n "$img" ] || { echo "lab: no pinned SILO image in server/docker-compose.yml" >&2; exit 2; }
work="$(mktemp -d "${TMPDIR:-/tmp}/jm-sharedkey-lab.XXXXXX")"
name="jm-sharedkey-lab-$$"
cleanup() {
	docker rm -f "$name" >/dev/null 2>&1 || true
	rm -rf -- "$work"
}
trap cleanup EXIT

rand() { head -c 20 /dev/urandom | od -An -tx1 | tr -d ' \n'; }
MINIO_ROOT_PASSWORD="$(rand)" JM_OBJ_CLIENT_SECRET_KEY="$(rand)" JM_OBJ_GC_SECRET_KEY="$(rand)"
export MINIO_ROOT_PASSWORD JM_OBJ_CLIENT_SECRET_KEY JM_OBJ_GC_SECRET_KEY

docker run -d --name "$name" -p 127.0.0.1::9000 -e MINIO_ROOT_USER=juicemount -e MINIO_ROOT_PASSWORD \
	-v "$repo/server/minio:/jm-minio:ro" "$img" server /data >/dev/null
for _ in $(seq 1 60); do
	docker exec "$name" curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1 && break
	sleep 0.5
done
port="$(docker port "$name" 9000/tcp | head -1 | sed 's/.*://')"

# minio-keys, as compose runs it.
provision() {
	docker exec -e MINIO_ROOT_PASSWORD -e JM_OBJ_CLIENT_SECRET_KEY -e JM_OBJ_GC_SECRET_KEY \
		-e JM_MINIO_ENDPOINT=http://127.0.0.1:9000 -e JM_MINIO_POLICY_DIR=/jm-minio \
		"$name" /bin/bash /jm-minio/provision-object-keys.sh
}
provision >"$work/provision.log" 2>&1 || { cat "$work/provision.log" >&2; exit 2; }

# The rotation (step 3 and 4 of the guide): remove jm-client, then run
# minio-keys with the new secret. The Go test calls this with a file that
# holds the new secret.
# The root password and jm-gc secret reach it through the environment that
# go test passes on, never through the file.
cat >"$work/rotate" <<EOF
#!/bin/bash
set -euo pipefail
export JM_OBJ_CLIENT_SECRET_KEY="\$(cat "\$1")"
docker exec -i -e MINIO_ROOT_PASSWORD "$name" bash -c 'd="\$(mktemp -d)"; export MC_CONFIG_DIR="\$d"; trap "rm -rf \$d" EXIT
  printf "%s\n" "\$MINIO_ROOT_PASSWORD" | mc alias set r http://127.0.0.1:9000 juicemount >/dev/null
  mc admin user rm r jm-client'
docker exec -e MINIO_ROOT_PASSWORD -e JM_OBJ_CLIENT_SECRET_KEY -e JM_OBJ_GC_SECRET_KEY \\
  -e JM_MINIO_ENDPOINT=http://127.0.0.1:9000 -e JM_MINIO_POLICY_DIR=/jm-minio \\
  "$name" /bin/bash /jm-minio/provision-object-keys.sh >/dev/null
EOF
chmod 700 "$work/rotate"

cd "$repo"
JM_LAB_BUCKET_URL="http://127.0.0.1:$port/zpool" JM_LAB_ROTATE="$work/rotate" JM_LAB_CLIENT_SECRET="$JM_OBJ_CLIENT_SECRET_KEY" \
	go test -count=1 -race -run '^TestSharedKeyRevokeRotationLab$' -v ./internal/manager 2>&1 | grep -E 'LAB \||^(---|ok|FAIL|PASS)|_test.go:'
