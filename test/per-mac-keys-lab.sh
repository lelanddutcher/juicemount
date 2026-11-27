#!/bin/bash
# Per-Mac object key lab: the Manager's principal store with per-Mac keys on
# (JM_OBJ_PER_MAC=1), the real minio-keyd (server/minio/keyd.sh, in its own
# container, as compose runs it) and a real object store: the SILO image that
# server/docker-compose.yml pins for `minio`, provisioned by the real
# server/minio/provision-object-keys.sh with JM_OBJ_PER_MAC_SERVER=1.
#
# The Go side is TestPerMacKeyLab (internal/manager). It prints one "LAB |"
# row per check with the value and the target; a row that misses its target
# fails the test.
#
#   test/per-mac-keys-lab.sh            # needs docker and go
#   JM_LAB_TRIALS=5 test/per-mac-keys-lab.sh   # fewer trials (default 20)
#
# Every secret is random and lab-only. Secrets reach the containers through
# `docker run/exec -e NAME` (the value comes from this process's
# environment, never argv) and mc's stdin. At the end the lab searches its
# own output, and keyd's log, for every secret it generated or the Manager
# received; one hit fails the run. The containers, their network and the
# work folder are removed on exit. Set TMPDIR to your scratch directory: the
# lab writes its files under it.
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
img="$(grep -m1 -oE 'pgsty/silo:[^[:space:]]+' "$repo/server/docker-compose.yml")"
[ -n "$img" ] || { echo "lab: no pinned SILO image in server/docker-compose.yml" >&2; exit 2; }
work="$(mktemp -d "${TMPDIR:-/tmp}/jm-permac-lab.XXXXXX")"
chmod 700 "$work"
name="jm-permac-lab-$$"
silo="$name-silo" keyd="$name-keyd" net="$name-net"
cleanup() {
	docker rm -f "$keyd" "$silo" >/dev/null 2>&1 || true
	docker network rm "$net" >/dev/null 2>&1 || true
	rm -rf -- "$work"
}
trap cleanup EXIT

rand() { head -c 20 /dev/urandom | od -An -tx1 | tr -d ' \n'; }
MINIO_ROOT_PASSWORD="$(rand)" JM_OBJ_CLIENT_SECRET_KEY="$(rand)" JM_OBJ_GC_SECRET_KEY="$(rand)"
JM_OBJ_KEYADMIN_SECRET_KEY="$(rand)"
export MINIO_ROOT_PASSWORD JM_OBJ_CLIENT_SECRET_KEY JM_OBJ_GC_SECRET_KEY JM_OBJ_KEYADMIN_SECRET_KEY

docker network create "$net" >/dev/null
# A fixed host port: `docker restart` (the SILO restart case) would give a
# port picked by docker a new number.
port=''
for _ in $(seq 1 50); do
	p=$((20000 + RANDOM % 20000))
	(exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null || { port="$p"; break; }
done
[ -n "$port" ] || { echo "lab: no free port" >&2; exit 2; }
# As compose runs `minio` (MINIO_STS_DURATION included).
docker run -d --name "$silo" --network "$net" -p "127.0.0.1:$port:9000" \
	-e MINIO_ROOT_USER=juicemount -e MINIO_ROOT_PASSWORD -e MINIO_API_REQUESTS_DEADLINE=10m -e MINIO_STS_DURATION=15m \
	-v "$repo/server/minio:/jm-minio:ro" "$img" server /data >/dev/null
silo_up() {
	for _ in $(seq 1 120); do
		docker exec "$silo" curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1 && return 0
		sleep 0.5
	done
	echo "lab: SILO did not become healthy" >&2
	return 1
}
silo_up

# minio-keys, as compose runs it, with step M1 on.
docker exec -e MINIO_ROOT_PASSWORD -e JM_OBJ_CLIENT_SECRET_KEY -e JM_OBJ_GC_SECRET_KEY -e JM_OBJ_KEYADMIN_SECRET_KEY \
	-e JM_OBJ_PER_MAC_SERVER=1 -e JM_MINIO_ENDPOINT=http://127.0.0.1:9000 -e JM_MINIO_POLICY_DIR=/jm-minio \
	"$silo" /bin/bash /jm-minio/provision-object-keys.sh >"$work/provision.log" 2>&1 ||
	{ cat "$work/provision.log" >&2; exit 2; }
grep -q 'jm-keyadmin: policy jm-keyadmin, enabled' "$work/provision.log" ||
	{ cat "$work/provision.log" >&2; echo "lab: per-Mac users not provisioned" >&2; exit 2; }

# minio-keyd, as compose runs it: same image, only the key admin secret,
# the objkeys folder shared with the Manager. It runs as this user (not
# root, as in compose) only so the Go test can read the files it writes.
objdir="$work/objkeys"
mkdir -m 0700 "$objdir"
docker create --name "$keyd" --network "$net" --user "$(id -u):$(id -g)" --stop-timeout 60 \
	-e JM_OBJ_PER_MAC_SERVER=1 -e JM_MINIO_ENDPOINT="http://$silo:9000" -e JM_OBJ_KEYADMIN_SECRET_KEY \
	-e JM_OBJKEYS_DIR=/var/lib/jm-objkeys -v "$objdir:/var/lib/jm-objkeys" -v "$repo/server/minio:/jm-minio:ro" \
	--entrypoint /bin/bash "$img" /jm-minio/keyd.sh >/dev/null
docker start "$keyd" >/dev/null

# ctl: what the Go test does to the containers. Secrets come from this
# process's environment (inherited through go test) or stdin, never argv.
# mc replies pass through a filter that hides any secretKey or sessionToken.
cat >"$work/ctl" <<EOF
#!/bin/bash
set -euo pipefail
hide() { sed -E 's/"(secretKey|sessionToken|SecretAccessKey|SessionToken)"[[:space:]]*:[[:space:]]*"[^"]*"/"\1":"(hidden)"/g'; }
# mc_as <access key> <mc args...>: the secret on stdin; alias "a".
mc_as() {
	local ak="\$1"; shift
	docker exec -i -e LAB_AK="\$ak" "$silo" bash -c 'd="\$(mktemp -d)"; export MC_CONFIG_DIR="\$d"; trap "rm -rf \$d" EXIT
	  IFS= read -r sk; printf "%s\n" "\$sk" | mc alias set a http://127.0.0.1:9000 "\$LAB_AK" >/dev/null; sk=""
	  mc --json "\$@"' mc "\$@" 2>&1 | hide
}
case "\$1" in
keyd-stop) docker stop "$keyd" >/dev/null ;;
keyd-start) docker start "$keyd" >/dev/null ;;
keyd-log) docker logs "$keyd" 2>&1 ;;
silo-restart) docker restart -t 5 "$silo" >/dev/null
	for _ in \$(seq 1 120); do docker exec "$silo" curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1 && exit 0; sleep 0.25; done
	exit 1 ;;
mc-as) shift; ak="\$1"; shift; mc_as "\$ak" "\$@" ;;
mc-root) shift; printf '%s\n' "\$MINIO_ROOT_PASSWORD" | mc_as juicemount "\$@" ;;
mc-keyadmin) shift; printf '%s\n' "\$JM_OBJ_KEYADMIN_SECRET_KEY" | mc_as jm-keyadmin "\$@" ;;
# keyadmin-probe <parent>: as jm-keyadmin, create a service account under
# <parent>, then use it (inside the container; its secret never leaves) for
# an admin call and a bucket listing, and remove it as root.
keyadmin-probe) shift
	printf '%s\n%s\n' "\$JM_OBJ_KEYADMIN_SECRET_KEY" "\$MINIO_ROOT_PASSWORD" |
	docker exec -i -e LAB_PARENT="\$1" "$silo" bash -c 'd="\$(mktemp -d)"; export MC_CONFIG_DIR="\$d"; trap "rm -rf \$d" EXIT
	  IFS= read -r ka; IFS= read -r root
	  printf "%s\n" "\$ka" | mc alias set k http://127.0.0.1:9000 jm-keyadmin >/dev/null; ka=""
	  printf "%s\n" "\$root" | mc alias set r http://127.0.0.1:9000 juicemount >/dev/null; root=""
	  ak="labprobe\$RANDOM"
	  rep="\$(mc --json admin user svcacct add k "\$LAB_PARENT" --access-key "\$ak" 2>&1)"
	  if [[ "\$rep" != *"\"status\": \"success\""* && "\$rep" != *"\"status\":\"success\""* ]]; then echo "create=refused"; exit 0; fi
	  [[ "\$rep" =~ \"secretKey\":[[:space:]]*\"([^\"]*)\" ]] && sk="\${BASH_REMATCH[1]}"; rep=""
	  printf "%s\n" "\$sk" | mc alias set p http://127.0.0.1:9000 "\$ak" >/dev/null; sk=""
	  echo "create=allowed"
	  if mc admin user ls p >/dev/null 2>&1; then echo "admin_user_list=allowed"; else echo "admin_user_list=refused"; fi
	  if mc ls p/zpool >/dev/null 2>&1; then echo "bucket_list=allowed"; else echo "bucket_list=refused"; fi
	  if echo x | mc pipe p/zpool/labprobe-del >/dev/null 2>&1 && mc rm p/zpool/labprobe-del >/dev/null 2>&1; then echo "delete=allowed"; else echo "delete=refused"; fi
	  mc admin user svcacct rm r "\$ak" >/dev/null 2>&1 || echo "cleanup=failed"' 2>&1 | hide ;;
*) echo "ctl: unknown \$1" >&2; exit 2 ;;
esac
EOF
chmod 700 "$work/ctl"

out="$work/out.txt"
seen="$work/seen-secrets"
cd "$repo"
set +e
JM_LAB_BUCKET_URL="http://127.0.0.1:$port/zpool" JM_LAB_ENDPOINT="http://127.0.0.1:$port" JM_LAB_CTL="$work/ctl" \
	JM_LAB_OBJKEYS_DIR="$objdir" JM_LAB_SEEN="$seen" JM_LAB_TRIALS="${JM_LAB_TRIALS:-20}" \
	go test -count=1 -race -timeout 30m -run '^TestPerMacKeyLab$' -v ./internal/manager >"$out" 2>&1
rc=$?
set -e
"$work/ctl" keyd-log >"$work/keyd.log" 2>&1 || true
grep -E 'LAB \||^(---|ok|FAIL|PASS)|_test.go:|panic' "$out" || true

# No secret in the output, keyd's log or any file kept (raw/ copies these).
leaks=0
{
	printf '%s\n' "$MINIO_ROOT_PASSWORD" "$JM_OBJ_CLIENT_SECRET_KEY" "$JM_OBJ_GC_SECRET_KEY" "$JM_OBJ_KEYADMIN_SECRET_KEY"
	[ ! -f "$seen" ] || cat "$seen"
} | while IFS= read -r s; do
	[ -n "$s" ] || continue
	if grep -qF -- "$s" "$out" "$work/keyd.log" "$work/provision.log"; then echo LEAK; fi
done >"$work/leaks"
leaks="$(grep -c LEAK "$work/leaks" || true)"
nsec=$((4 + $( [ -f "$seen" ] && wc -l <"$seen" || echo 0 )))
echo "LAB | secrets found in lab output, keyd log, provision log         | want 0 of $nsec | got $leaks"
if [ -n "${JM_LAB_KEEP:-}" ]; then
	mkdir -p "$JM_LAB_KEEP"
	[ "$leaks" = 0 ] && cp "$out" "$JM_LAB_KEEP/go-test.txt" && cp "$work/keyd.log" "$JM_LAB_KEEP/keyd.log" &&
		cp "$work/provision.log" "$JM_LAB_KEEP/provision.log"
fi
[ "$leaks" = 0 ] || exit 1
exit "$rc"
