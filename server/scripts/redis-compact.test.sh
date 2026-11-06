#!/usr/bin/env bash
# Live test for server/scripts/lib/redis-compact.sh, the AOF rewrite that
# every script runs before it stops or recreates the metadata Redis.
#
# The case it exists for: a 0.5.0 server whose Redis already restarted
# under the 0.5.0 ACL (`user default ... -@all`). That start refused the
# transactions it replayed, but they are still in the append-only file.
# The first start under the fixed ACL would replay them over everything
# written since. The helper rewrites the AOF under the running Redis first,
# so that start replays nothing old.
#
#   1. 0.5.0 ACL: write transaction A (LPUSH q jobA ...) and a plain SET.
#   2. Restart under the 0.5.0 ACL: A is refused and lost (the precondition).
#   3. Still 0.5.0: write transaction B. Copy the data folder (control).
#   4. Run the helper, then restart under the fixed server/redis.acl:
#      B and the plain SET survive, A stays gone, no AOF-loading-client lines.
#   5. Control: the copy, restarted under the fixed ACL without the helper,
#      brings A back. This proves step 4 tests the helper.
#
# Usage: server/scripts/redis-compact.test.sh
# Needs Docker. Uses the compose's digest-pinned Redis image.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
server=$(dirname "$here")
# shellcheck source=lib/redis-compact.sh
. "$here/lib/redis-compact.sh"

image=$(awk '
	/^  redis:/ { in_redis = 1; next }
	/^  [a-z]/ { in_redis = 0 }
	in_redis && $1 == "image:" { print $2; exit }
' "$server/docker-compose.yml")
case $image in
*@sha256:*) ;;
*) echo "redis-compact: no digest-pinned redis image in docker-compose.yml (got '$image')" >&2; exit 1 ;;
esac

project=jm-redis-compact-test-$$
work=$(mktemp -d "${TMPDIR:-/tmp}/jm-redis-compact.XXXXXX")
mkdir "$work/data" "$work/control"
chmod 777 "$work/data" "$work/control" # the image runs redis as uid 999

# Render both ACLs the way the redis service does (docker-compose.yml).
proxy_pw=compact-test-proxy-0123456789abcdef
admin_pw=compact-test-admin-0123456789abcdef
sha() { printf '%s' "$1" | sha256sum | cut -d' ' -f1; }
render() { # template, output
	sed -e '/^[[:space:]]*#/d' -e '/^[[:space:]]*$/d' \
		-e "s/#@JM_REDIS_PROXY_PASSWORD_SHA256@/#$(sha "$proxy_pw")/" \
		-e "s/#@JM_ADMIN_LOOPBACK_SHA256@/#$(sha "$admin_pw")/" "$1" >"$2"
	chmod 644 "$2"
	! grep -q '#@' "$2" || { echo "redis-compact: unrendered placeholder in $2" >&2; exit 1; }
}
sed 's/^user default .*/user default off resetpass resetkeys resetchannels -@all/' "$server/redis.acl" >"$work/old.acl.tmpl"
grep -qx 'user default off resetpass resetkeys resetchannels -@all' "$work/old.acl.tmpl"
render "$work/old.acl.tmpl" "$work/old.acl"
render "$server/redis.acl" "$work/fixed.acl"
if jm_redis_acl_replay_safe "$work/old.acl"; then echo 'redis-compact: the 0.5.0 ACL was judged replay-safe' >&2; exit 1; fi
jm_redis_acl_replay_safe "$work/fixed.acl" || { echo 'redis-compact: the shipped ACL was judged unsafe' >&2; exit 1; }
# The helper logs in as jm-admin with the secret the redis service keeps here.
printf '%s' "$admin_pw" >"$work/admin.pass"
chmod 644 "$work/admin.pass"

name=
start() { # step, data folder, acl
	name=jm-redis-compact-$$-$1
	docker run -d --rm --name "$name" \
		--label "com.docker.compose.project=$project" --label com.docker.compose.service=redis \
		-v "$2:/data" -v "$3:/users.acl:ro" -v "$work/admin.pass:/run/jm-redis/admin.pass:ro" \
		"$image" redis-server --appendonly yes --appendfsync always \
		--aclfile /users.acl >/dev/null
	for _ in $(seq 1 50); do
		if docker exec "$name" redis-cli --user jm-proxy --pass "$proxy_pw" --no-auth-warning PING 2>/dev/null | grep -q PONG; then
			return 0
		fi
		sleep 0.2
	done
	docker logs "$name" >&2 || true
	echo "redis-compact: redis did not start" >&2
	return 1
}
stop() { [ -z "$name" ] || docker stop "$name" >/dev/null 2>&1 || true; }
cleanup() {
	stop
	# The data folders belong to the image's redis user: remove them as root
	# inside a throwaway container of the same image.
	docker run --rm -v "$work:/w" --entrypoint sh "$image" -c 'rm -rf /w/data /w/control' >/dev/null 2>&1 || true
	rm -rf "$work" 2>/dev/null || true
}
trap cleanup EXIT
cli() { docker exec "$name" redis-cli --user jm-proxy --pass "$proxy_pw" --no-auth-warning "$@"; }
raw() { docker exec "$name" sh -c "printf '$1' | redis-cli" >/dev/null; }
refusals() { docker logs "$name" 2>&1 | grep -c 'AOF-loading-client' || true; }

fail=0
start 1 "$work/data" "$work/old.acl"
raw "AUTH jm-proxy $proxy_pw\r\nMULTI\r\nSET i1 attrA\r\nLPUSH q jobA\r\nEXEC\r\nSET plain 1\r\n"
stop
start 2 "$work/data" "$work/old.acl"
# Precondition: the 0.5.0 restart refused transaction A.
if [ "$(cli LLEN q)" != 0 ] || [ "$(refusals)" = 0 ]; then
	echo 'redis-compact: precondition failed: the 0.5.0 restart did not refuse the transaction' >&2
	exit 1
fi
raw "AUTH jm-proxy $proxy_pw\r\nMULTI\r\nHSET d1 name ino\r\nSET i1 attrB\r\nEXEC\r\nSELECT 1\r\nMULTI\r\nSET db1 b\r\nEXEC\r\n"
# With appendfsync always and no client writing, the files are complete.
docker run --rm -v "$work:/w" --entrypoint sh "$image" -c 'cp -a /w/data/. /w/control/'

jm_redis_before_restart "$project" "$server/redis.acl" 0 || { echo 'redis-compact: the helper failed on a running Redis' >&2; exit 1; }
info=$(docker exec "$name" sh -c 'redis-cli --user jm-admin --pass "$(cat /run/jm-redis/admin.pass)" --no-auth-warning INFO persistence' | tr -d '\r')
for f in aof_rewrite_scheduled:0 aof_rewrite_in_progress:0 aof_last_bgrewrite_status:ok; do
	printf '%s\n' "$info" | grep -qx "$f" || { echo "redis-compact: INFO persistence lacks $f after the helper" >&2; fail=1; }
done
stop
start 3 "$work/data" "$work/fixed.acl"
[ "$(cli GET i1)" = attrB ] || { echo "redis-compact: i1 is '$(cli GET i1)' after the restart, want attrB" >&2; fail=1; }
[ "$(cli HGET d1 name)" = ino ] || { echo 'redis-compact: transaction B was lost on the restart' >&2; fail=1; }
[ "$(cli -n 1 GET db1)" = b ] || { echo 'redis-compact: the database-1 transaction was lost on the restart' >&2; fail=1; }
[ "$(cli GET plain)" = 1 ] || { echo 'redis-compact: the plain SET was lost on the restart' >&2; fail=1; }
[ "$(cli LLEN q)" = 0 ] || { echo 'redis-compact: the refused transaction A came back on the restart' >&2; fail=1; }
if [ "$(refusals)" != 0 ]; then
	echo 'redis-compact: redis refused a command while loading the AOF:' >&2
	docker logs "$name" 2>&1 | grep -m3 'AOF-loading-client' >&2
	fail=1
fi
stop

# Control: the same data without the helper. If A does not come back here,
# step 4 proves nothing about the helper.
start 4 "$work/control" "$work/fixed.acl"
if [ "$(cli LLEN q)" != 1 ]; then
	echo 'redis-compact: control failed: without the helper, the fixed-ACL restart did not bring back transaction A' >&2
	fail=1
fi
stop
name=

if [ "$fail" = 0 ]; then
	echo 'redis-compact: after the helper, a fixed-ACL restart keeps every write and replays no refused transaction'
fi
exit "$fail"
