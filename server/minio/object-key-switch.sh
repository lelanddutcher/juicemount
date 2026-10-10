#!/bin/bash
# H-8B object-key switch (COMPOSE-ROLLOUT.md, "Object keys", step B).
#
#   object-key-switch.sh clear --all-macs-have-keys
#       Empties the object key in the volume `setting` key, so no storage
#       secret is left in Redis. Every juicefs client must then supply its
#       key as MINIO_ACCESS_KEY / MINIO_SECRET_KEY. Refuses unless every
#       server client already carries its key and is mounted.
#   object-key-switch.sh rollback [--root]
#       Writes a key back into `setting`: jm-gc by default, or the MinIO
#       root user with --root (the exact pre-switch state).
#
# Run on the NAS host after loading server/.env into the environment
# (set -a; . server/.env; set +a). Secrets reach `docker exec` by NAME
# (-e VAR), never as values on the host command line. `juicefs config`
# runs in the juicefs container as the `admin` principal through the
# proxy. Known exposure: `rollback` must hand the secret to juicefs as
# --secret-key (stock juicefs has no other input for it), so it is in that
# one process's argv inside the container for the second it runs.
set -eu

DOCKER="${DOCKER:-docker}"
META="${JM_SWITCH_META_URL:-redis://admin@respproxy:6379/1}"

die() { echo "[object-key-switch] $*" >&2; exit 2; }

container() {
	local ids
	ids="$("$DOCKER" ps -q --filter "label=com.docker.compose.service=$1")" || die "docker ps failed"
	[ -n "$ids" ] || die "service $1 is not running"
	[ "$(printf '%s\n' "$ids" | wc -l | tr -d ' ')" = 1 ] || die "more than one $1 container is running"
	printf '%s' "$ids"
}

# has_env <container> <var>: the variable is set and non-empty inside.
has_env() {
	"$DOCKER" exec "$1" sh -c "[ -n \"\${$2:-}\" ]" || die "container $1 has no $2: put the object keys on every server client first"
}

need() {
	local v
	for v in "$@"; do [ -n "${!v:-}" ] || die "$v is not set (load server/.env first)"; done
}

mode="${1:-}"
shift || true
case "$mode" in
clear)
	[ "${1:-}" = --all-macs-have-keys ] ||
		die "clear needs --all-macs-have-keys: a Mac without its jm-client key stalls in upload retries for minutes once setting is empty"
	need JM_PROXY_ADMIN_PASSWORD JM_OBJ_GC_ACCESS_KEY JM_OBJ_GC_SECRET_KEY
	J="$(container juicefs)"
	M="$(container juicemount-manager)"
	F="$(container juicefarm)"
	has_env "$J" JM_OBJ_GC_ACCESS_KEY
	has_env "$J" JM_OBJ_CLIENT_ACCESS_KEY
	has_env "$M" JM_OBJ_GC_ACCESS_KEY
	has_env "$F" MINIO_ACCESS_KEY
	"$DOCKER" exec "$J" mountpoint -q /jfs || die "the juicefs container is not mounted"
	"$DOCKER" exec "$F" mountpoint -q /jfs || die "the juicefarm container is not mounted"
	export META_PASSWORD="$JM_PROXY_ADMIN_PASSWORD" MINIO_ACCESS_KEY="$JM_OBJ_GC_ACCESS_KEY" MINIO_SECRET_KEY="$JM_OBJ_GC_SECRET_KEY"
	# The storage test that `juicefs config` runs uses the env key (jm-gc).
	"$DOCKER" exec -e META_PASSWORD -e MINIO_ACCESS_KEY -e MINIO_SECRET_KEY "$J" \
		juicefs config "$META" --access-key "" --secret-key "" --yes ||
		die "juicefs config failed; setting is unchanged"
	out="$("$DOCKER" exec -e META_PASSWORD "$J" juicefs status "$META")" || die "juicefs status failed"
	[[ "$out" != *'"AccessKey"'* && "$out" != *'"SecretKey"'* ]] || die "setting still holds an object key"
	echo "[object-key-switch] setting holds no object key"
	;;
rollback)
	need JM_PROXY_ADMIN_PASSWORD
	if [ "${1:-}" = --root ]; then
		need MINIO_ROOT_PASSWORD
		export JM_RB_AK="${MINIO_ROOT_USER:-juicemount}" JM_RB_SK="$MINIO_ROOT_PASSWORD"
	else
		need JM_OBJ_GC_ACCESS_KEY JM_OBJ_GC_SECRET_KEY
		export JM_RB_AK="$JM_OBJ_GC_ACCESS_KEY" JM_RB_SK="$JM_OBJ_GC_SECRET_KEY"
	fi
	J="$(container juicefs)"
	export META_PASSWORD="$JM_PROXY_ADMIN_PASSWORD" JM_RB_META="$META"
	"$DOCKER" exec -e META_PASSWORD -e JM_RB_AK -e JM_RB_SK -e JM_RB_META "$J" sh -c \
		'juicefs config "$JM_RB_META" --access-key "$JM_RB_AK" --secret-key "$JM_RB_SK" --yes' ||
		die "juicefs config failed; setting is unchanged"
	echo "[object-key-switch] setting holds the $JM_RB_AK key again"
	;;
*)
	die "usage: object-key-switch.sh clear --all-macs-have-keys | rollback [--root]"
	;;
esac
