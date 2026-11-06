#!/usr/bin/env bash
# Redis replays its append-only file (AOF) at every start. A MULTI/EXEC
# transaction in that file must come back after a restart under the ACL
# that server/redis.acl ships, exactly as a plain command does. JuiceFS
# writes almost all of its file metadata in such transactions.
#
# Redis 7.4 checks a replayed EXEC against the `default` user. With
# `default ... -@all` it refuses every replayed transaction with
# "NOPERM ACLs rules changed ...", so a restart silently drops the
# metadata written since the last AOF rewrite. This test runs the
# compose's own pinned Redis image twice on one data folder.
#
# Usage: server/scripts/redis-aof-acl.test.sh [ACL-TEMPLATE]
# Needs Docker. Leaves its scratch folder under ${TMPDIR:-/tmp}.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
server=$(dirname "$here")
template=${1:-$server/redis.acl}

image=$(awk '
	/^  redis:/ { in_redis = 1; next }
	/^  [a-z]/ { in_redis = 0 }
	in_redis && $1 == "image:" { print $2; exit }
' "$server/docker-compose.yml")
case $image in
*@sha256:*) ;;
*) echo "redis-aof-acl: no digest-pinned redis image in docker-compose.yml (got '$image')" >&2; exit 1 ;;
esac

work=$(mktemp -d "${TMPDIR:-/tmp}/jm-redis-aof-acl.XXXXXX")
mkdir "$work/data"
chmod 777 "$work/data" # the image runs redis as uid 999

# Render the template the way the redis service does (docker-compose.yml):
# drop comment and blank lines, and each #@NAME@ placeholder becomes
# #<sha256 of a secret>. The test logs in as jm-proxy.
proxy_pw=aof-acl-test-proxy-0123456789abcdef
admin_pw=aof-acl-test-admin-0123456789abcdef
sha() { printf '%s' "$1" | sha256sum | cut -d' ' -f1; }
sed -e '/^[[:space:]]*#/d' -e '/^[[:space:]]*$/d' \
	-e "s/#@JM_REDIS_PROXY_PASSWORD_SHA256@/#$(sha "$proxy_pw")/" \
	-e "s/#@JM_ADMIN_LOOPBACK_SHA256@/#$(sha "$admin_pw")/" \
	"$template" >"$work/users.acl"
chmod 644 "$work/users.acl"
if grep -q '#@' "$work/users.acl"; then
	echo "redis-aof-acl: unrendered placeholder left in the ACL" >&2
	exit 1
fi

name=jm-redis-aof-acl-$$
start() {
	docker run -d --rm --name "$name" \
		-v "$work/data:/data" -v "$work/users.acl:/users.acl:ro" \
		"$image" redis-server --appendonly yes --appendfsync always \
		--aclfile /users.acl >/dev/null
	for _ in $(seq 1 50); do
		if docker exec "$name" redis-cli --user jm-proxy --pass "$proxy_pw" --no-auth-warning PING 2>/dev/null | grep -q PONG; then
			return 0
		fi
		sleep 0.2
	done
	docker logs "$name" >&2 || true
	echo "redis-aof-acl: redis did not start" >&2
	return 1
}
stop() { docker stop "$name" >/dev/null 2>&1 || true; }
trap stop EXIT
cli() { docker exec "$name" redis-cli --user jm-proxy --pass "$proxy_pw" --no-auth-warning "$@"; }

start
# One WATCHed transaction shaped like a JuiceFS metadata write, plus a
# plain command as the control.
docker exec "$name" sh -c "printf 'AUTH jm-proxy $proxy_pw\r\nWATCH i1\r\nMULTI\r\nHSET d1 name ino\r\nSET i1 attr\r\nLPUSH q job\r\nEXEC\r\nSET plain 1\r\n' | redis-cli" >/dev/null
stop
sleep 1
start

fail=0
[ "$(cli GET plain)" = 1 ] || { echo "redis-aof-acl: the plain SET did not survive the restart" >&2; fail=1; }
[ "$(cli GET i1)" = attr ] || { echo "redis-aof-acl: the transaction's SET was lost on restart" >&2; fail=1; }
[ "$(cli HGET d1 name)" = ino ] || { echo "redis-aof-acl: the transaction's HSET was lost on restart" >&2; fail=1; }
[ "$(cli LLEN q)" = 1 ] || { echo "redis-aof-acl: the transaction's LPUSH was lost on restart" >&2; fail=1; }
if docker logs "$name" 2>&1 | grep -q 'AOF-loading-client'; then
	echo "redis-aof-acl: redis refused a command while loading the AOF:" >&2
	docker logs "$name" 2>&1 | grep -m3 'AOF-loading-client' >&2
	fail=1
fi
# The default user must still refuse unauthenticated clients.
if ! docker exec "$name" redis-cli PING 2>&1 | grep -q NOAUTH; then
	echo "redis-aof-acl: an unauthenticated client was not refused with NOAUTH" >&2
	fail=1
fi
if docker exec "$name" redis-cli --user default --pass x --no-auth-warning PING 2>&1 | grep -q PONG; then
	echo "redis-aof-acl: logging in as default succeeded" >&2
	fail=1
fi
[ "$fail" = 0 ] && echo "redis-aof-acl: transactions survive a restart; default stays closed"
exit "$fail"
