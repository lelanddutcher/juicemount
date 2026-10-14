#!/bin/bash
# Create the two JuiceMount object-store users in MinIO, each with exactly
# one policy. Safe to re-run: policies are rewritten, users get their
# current secret, and the end state is verified, not assumed.
#
#   jm-client  jm-client-policy.json  GetObject, PutObject, ListBucket.
#              No DeleteObject, no bucket or admin actions. Macs and the
#              farm get this key.
#   jm-gc      jm-gc-policy.json      adds DeleteObject and multipart
#              cleanup. Server bg mount and Manager only.
#
# Per-Mac object keys (rollout step M1, only when JM_OBJ_PER_MAC_SERVER=1;
# nothing below changes otherwise):
#   jm-macs      policy jm-client (the same canned policy, so it cannot
#                drift). Parent of every per-Mac service account. Its own
#                secret is random, made here, and never stored or handed
#                out: the user exists only so the service accounts have a
#                parent. Created once; later runs only re-check its policy.
#   jm-keyadmin  jm-keyadmin-policy.json: create, remove and list service
#                accounts. No S3 action, no UpdateServiceAccount (MinIO
#                checks only the action for an update, so it would let the
#                holder reset any account's secret). Held only by minio-keyd.
#   What jm-keyadmin does NOT enforce: "keyd mints only under jm-macs" and
#   "a keyd key cannot delete objects" are enforced by keyd.sh (hard-coded
#   parent, name check), not by this credential. MinIO's AddServiceAccount
#   accepts any non-root target user (design A7, read in the 2021 source),
#   so a stolen jm-keyadmin secret could mint a key under jm-gc, which can
#   delete. Residual: verify against the pinned MinIO release.
#
# A per-Mac failure (bad JM_OBJ_PER_MAC_SERVER value, bad key admin secret,
# missing tr/head, mc error) is logged loudly and skips only the per-Mac
# part: this script still exits 0 once jm-client and jm-gc are verified,
# because juicefs-init waits for it and a full `up` must still mount. A
# failure for jm-client or jm-gc exits 2.
#
# Neither key is ever stored in the volume `setting` key: after the H-8B
# object-key switch `setting` holds no key at all and every juicefs client
# reads MINIO_ACCESS_KEY / MINIO_SECRET_KEY from its environment.
#
# Runs in the `minio-keys` compose service (minio image: bash + mc), or by
# hand inside the minio container. No secret is ever on a command line:
# the root credential reaches mc through a private native config.json, and
# each user's secret through mc's stdin. The pinned mc does not URL-decode
# credentials from MC_HOST_<alias>, so percent-encoding there changes them.
#
# Environment:
#   MINIO_ROOT_USER            default juicemount
#   MINIO_ROOT_PASSWORD        required
#   JM_MINIO_ENDPOINT          default http://127.0.0.1:9000
#   JM_OBJ_CLIENT_ACCESS_KEY   default jm-client
#   JM_OBJ_CLIENT_SECRET_KEY   required, 32-40 chars [A-Za-z0-9]
#   JM_OBJ_GC_ACCESS_KEY       default jm-gc
#   JM_OBJ_GC_SECRET_KEY       required, 32-40 chars [A-Za-z0-9]
#   JM_OBJ_PER_MAC_SERVER      1 = also create jm-macs and jm-keyadmin;
#                              0 or unset = no; anything else is reported
#   JM_OBJ_KEYADMIN_ACCESS_KEY default jm-keyadmin
#   JM_OBJ_KEYADMIN_SECRET_KEY required when JM_OBJ_PER_MAC_SERVER=1, 32-40 chars [A-Za-z0-9]
#   JM_MINIO_POLICY_DIR        default: this script's directory
#   MC                         default mc
#
# Uses bash builtins, mkdir/rm and mc; the per-Mac parent secret also uses
# tr and head (the minio image is ubi-micro, which has coreutils).
set -eu

MC="${MC:-mc}"
ROOT_USER="${MINIO_ROOT_USER:-juicemount}"
ROOT_PASS="${MINIO_ROOT_PASSWORD:-}"
ENDPOINT="${JM_MINIO_ENDPOINT:-http://127.0.0.1:9000}"
CLIENT_AK="${JM_OBJ_CLIENT_ACCESS_KEY:-jm-client}"
CLIENT_SK="${JM_OBJ_CLIENT_SECRET_KEY:-}"
GC_AK="${JM_OBJ_GC_ACCESS_KEY:-jm-gc}"
GC_SK="${JM_OBJ_GC_SECRET_KEY:-}"
PER_MAC="${JM_OBJ_PER_MAC_SERVER:-}"
MACS_AK=jm-macs # hard-coded: minio-keyd mints only under this parent
KEYADMIN_AK="${JM_OBJ_KEYADMIN_ACCESS_KEY:-jm-keyadmin}"
KEYADMIN_SK="${JM_OBJ_KEYADMIN_SECRET_KEY:-}"
src="${BASH_SOURCE[0]:-.}"
[[ "$src" == */* ]] && src="${src%/*}" || src=.
POLICY_DIR="${JM_MINIO_POLICY_DIR:-$src}"
ALIAS=jmkeys

die() { echo "[object-keys] $*" >&2; exit 2; }
# per_mac_skip reason: report loudly; jm-client and jm-gc still proceed.
PER_MAC_SKIP=''
per_mac_skip() {
	echo "[object-keys] ERROR: per-Mac object keys NOT provisioned: $*" >&2
	PER_MAC_SKIP="$*"
}

[ -n "$ROOT_PASS" ] || die "MINIO_ROOT_PASSWORD is not set"
for name in JM_OBJ_CLIENT_SECRET_KEY JM_OBJ_GC_SECRET_KEY; do
	val="${!name:-}"
	[[ "$val" =~ ^[A-Za-z0-9]{32,40}$ ]] ||
		die "$name must be 32-40 characters of A-Z a-z 0-9 (openssl rand -hex 20)"
done
for ak in "$CLIENT_AK" "$GC_AK"; do
	[[ "$ak" =~ ^[a-z0-9-]{3,20}$ ]] || die "access key '$ak' must be 3-20 characters of a-z 0-9 -"
done
[ "$CLIENT_AK" != "$GC_AK" ] || die "client and gc access keys must differ"
[ "$CLIENT_SK" != "$GC_SK" ] || die "client and gc secret keys must differ"
[ "$CLIENT_AK" != "$ROOT_USER" ] && [ "$GC_AK" != "$ROOT_USER" ] || die "an object key must not be the MinIO root user"
for f in jm-client-policy.json jm-gc-policy.json; do
	[ -r "$POLICY_DIR/$f" ] || die "missing $POLICY_DIR/$f"
done
client_policy="$(<"$POLICY_DIR/jm-client-policy.json")"
[[ "$client_policy" != *Delete* ]] || die "jm-client-policy.json grants a Delete action; refusing"
# per_mac_check: the per-Mac inputs, before any mc call. Prints why not.
per_mac_check() {
	[[ "$KEYADMIN_SK" =~ ^[A-Za-z0-9]{32,40}$ ]] ||
		{ echo "JM_OBJ_KEYADMIN_SECRET_KEY must be 32-40 characters of A-Z a-z 0-9 (openssl rand -hex 20)"; return 1; }
	[[ "$KEYADMIN_AK" =~ ^[a-z0-9-]{3,20}$ ]] ||
		{ echo "access key '$KEYADMIN_AK' must be 3-20 characters of a-z 0-9 -"; return 1; }
	local ak
	for ak in "$CLIENT_AK" "$GC_AK" "$ROOT_USER"; do
		[ "$ak" != "$MACS_AK" ] || { echo "'$MACS_AK' is reserved for the per-Mac key parent"; return 1; }
	done
	for ak in "$CLIENT_AK" "$GC_AK" "$ROOT_USER" "$MACS_AK"; do
		[ "$ak" != "$KEYADMIN_AK" ] || { echo "the key admin must not be '$ak'"; return 1; }
	done
	[ "$KEYADMIN_SK" != "$CLIENT_SK" ] && [ "$KEYADMIN_SK" != "$GC_SK" ] ||
		{ echo "JM_OBJ_KEYADMIN_SECRET_KEY must differ from the client and gc secrets"; return 1; }
	[ -r "$POLICY_DIR/jm-keyadmin-policy.json" ] || { echo "missing $POLICY_DIR/jm-keyadmin-policy.json"; return 1; }
	local keyadmin_policy
	keyadmin_policy="$(<"$POLICY_DIR/jm-keyadmin-policy.json")"
	[[ "$keyadmin_policy" != *s3:* && "$keyadmin_policy" != *'admin:*'* && "$keyadmin_policy" != *'"*"'* ]] ||
		{ echo "jm-keyadmin-policy.json grants an S3 or wildcard action; refusing"; return 1; }
	return 0
}
case "$PER_MAC" in
0 | '') PER_MAC=0 ;;
1)
	if ! why="$(per_mac_check)"; then
		per_mac_skip "$why"
		PER_MAC=0
	fi
	;;
*)
	per_mac_skip "JM_OBJ_PER_MAC_SERVER='${PER_MAC//[^A-Za-z0-9 ]/?}' is not 1 or 0 (unset means 0)"
	PER_MAC=0
	;;
esac

# JSON string contents, byte by byte. Bash variables cannot contain NUL;
# every other control byte is escaped, including newlines and DEL. This is
# deliberately Bash-only: the minio image has no jq or Python dependency.
json_string() {
	local LC_ALL=C s="$1" out='' c hex i
	for ((i = 0; i < ${#s}; i++)); do
		c="${s:i:1}"
		case "$c" in
		'"') out+='\"' ;;
		'\') out+='\\' ;;
		[[:cntrl:]])
			printf -v hex '%04x' "'$c"
			out+="\\u$hex" ;;
		*) out+="$c" ;;
		esac
	done
	printf '%s' "$out"
}
[[ "$ENDPOINT" =~ ^https?://.+$ ]] || die "JM_MINIO_ENDPOINT must start with http:// or https://"

# Never use an inherited alias/config override, and never remove a caller's
# MC_CONFIG_DIR. mkdir is exclusive, so an existing path cannot be adopted.
unset MC_HOST_jmkeys MC_CONFIG_ENV_FILE
config_parent="${TMPDIR:-/tmp}"
[ -d "$config_parent" ] && [ ! -L "$config_parent" ] || die "TMPDIR must be an existing directory, not a symlink"
umask 077
owned_config_dir=''
for ((attempt = 0; attempt < 16; attempt++)); do
	candidate="$config_parent/jm-mc-$$-$RANDOM-$RANDOM"
	if mkdir -m 0700 -- "$candidate" 2>/dev/null; then
		owned_config_dir="$candidate"
		break
	fi
done
[ -n "$owned_config_dir" ] || die "cannot create a private mc config directory"
cleanup_mc_config() {
	local status=$?
	trap - EXIT
	if ! rm -rf -- "$owned_config_dir"; then
		echo '[object-keys] ERROR: could not remove private mc config' >&2
		[ "$status" -ne 0 ] || status=2
	fi
	exit "$status"
}
trap cleanup_mc_config EXIT
export MC_CONFIG_DIR="$owned_config_dir"
if ! printf '{"version":"10","aliases":{"%s":{"url":"%s","accessKey":"%s","secretKey":"%s","api":"S3v4","path":"auto"}}}\n' \
	"$ALIAS" "$(json_string "$ENDPOINT")" "$(json_string "$ROOT_USER")" "$(json_string "$ROOT_PASS")" >"$owned_config_dir/config.json"; then
	die "cannot write private mc config"
fi

# verify_user <access-key> <policy-name>: exactly that policy, enabled.
verify_user() {
	local ak="$1" pol="$2" info got
	info="$("$MC" --json admin user info "$ALIAS" "$ak")" || die "user info $ak failed"
	[[ "$info" =~ \"policyName\":[[:space:]]*\"([^\"]*)\" ]] && got="${BASH_REMATCH[1]}" || got=''
	[ "$got" = "$pol" ] || die "user $ak has policy '$got', want exactly '$pol' (detach the extra policy by hand)"
	[[ "$info" =~ \"userStatus\":[[:space:]]*\"enabled\" ]] || die "user $ak is not enabled"
	echo "[object-keys] $ak: policy $pol, enabled"
}

# ensure_user <access-key> <secret-variable-name> <policy-name>
ensure_user() {
	local ak="$1" skvar="$2" pol="$3"
	"$MC" admin policy create "$ALIAS" "$pol" "$POLICY_DIR/$pol-policy.json" >/dev/null ||
		die "policy create $pol failed"
	# mc reads the secret from stdin when it is not on the command line.
	printf '%s\n' "${!skvar}" | "$MC" admin user add "$ALIAS" "$ak" >/dev/null || die "user add $ak failed"
	# attach fails when the policy is already attached; the check below
	# is what decides success.
	"$MC" admin policy attach "$ALIAS" "$pol" --user "$ak" >/dev/null 2>&1 || true
	verify_user "$ak" "$pol"
}

# ensure_parent <access-key> <policy-name>: a user whose secret nobody
# needs. Created with a random secret only when absent, so a re-run never
# touches the parent of live service accounts.
ensure_parent() {
	local ak="$1" pol="$2" sk
	if ! "$MC" --json admin user info "$ALIAS" "$ak" >/dev/null 2>&1; then
		sk="$(LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 40)" || true
		[[ "$sk" =~ ^[A-Za-z0-9]{40}$ ]] || die "could not generate a secret for $ak"
		printf '%s\n' "$sk" | "$MC" admin user add "$ALIAS" "$ak" >/dev/null || die "user add $ak failed"
		sk=''
	fi
	"$MC" admin policy attach "$ALIAS" "$pol" --user "$ak" >/dev/null 2>&1 || true
	verify_user "$ak" "$pol"
}

# The restricted format credential cannot create buckets. Bootstrap only the
# fixed policy bucket with the root alias; --ignore-existing preserves its data.
# mc's makeBucketMessage is status/bucket/region (region is empty). Match the
# entire response: exit 0 alone, a success substring, or another target is not
# proof. Keep errors private: mc diagnostics may contain endpoint details.
ensure_bucket() {
	local reply success
	reply="$("$MC" --json mb --ignore-existing "$ALIAS/zpool" 2>"$owned_config_dir/bucket.err")" ||
		die "cannot ensure zpool bucket; check root access and storage availability before retrying setup"
	success='^[[:space:]]*\{[[:space:]]*"status"[[:space:]]*:[[:space:]]*"success"[[:space:]]*,[[:space:]]*"bucket"[[:space:]]*:[[:space:]]*"jmkeys/zpool"[[:space:]]*,[[:space:]]*"region"[[:space:]]*:[[:space:]]*""[[:space:]]*\}[[:space:]]*$'
	[[ "$reply" =~ $success ]] ||
		die "zpool bucket was not verified by mc; check root access, storage readiness and supported mc version"
	echo "[object-keys] zpool: bucket ready"
}

ensure_bucket
ensure_user "$CLIENT_AK" CLIENT_SK jm-client
ensure_user "$GC_AK" GC_SK jm-gc
if [ "$PER_MAC" = 1 ]; then
	# A subshell, so a die inside fails the per-Mac part only.
	if ! (
		ensure_parent "$MACS_AK" jm-client || exit 2
		ensure_user "$KEYADMIN_AK" KEYADMIN_SK jm-keyadmin || exit 2
	); then
		per_mac_skip "jm-macs / jm-keyadmin provisioning failed (see the line above)"
	fi
fi
if [ -n "$PER_MAC_SKIP" ]; then
	echo "[object-keys] jm-client and jm-gc are ready; per-Mac keys are OFF until this is fixed: $PER_MAC_SKIP" >&2
fi
exit 0
