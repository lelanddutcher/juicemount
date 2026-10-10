#!/usr/bin/env bash
# Offline tests for the H-8B port swap in rollout-compose.sh:
#   --cutover-proxy   pre-proxy compose (Redis on 30179) -> proxy compose
#                     (Redis internal, jm-respproxy on 30179), with an
#                     automatic staged rollback to the pre-proxy compose
#   --proxy-rollback  proxy -> direct Redis on 30179 with the rollback ACL
#                     (plan section 3, rollback step 3)
#   --proxy-restore   direct -> proxy again
# No Docker daemon, SSH, or NAS is contacted: docker is a PATH shim that
# models which compose topology each service was last created from and
# refuses a second bind of host port 30179.
set -euo pipefail
SCRIPT="$(cd "$(dirname "$0")" && pwd)/rollout-compose.sh"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
REPO="$WORK/repo"
mkdir -p "$REPO/server/respproxy" "$REPO/internal/version" "$WORK/bin"
SRC_SERVER="$(cd "$(dirname "$SCRIPT")/.." && pwd)"
printf 'package version\nvar Version = "0.5.0"\n' > "$REPO/internal/version/version.go"
git -C "$REPO" init -q
git -C "$REPO" config user.name test
git -C "$REPO" config user.email test@example.invalid
# Commit 1: the pre-proxy compose (Redis publishes 30179 itself).
cat > "$REPO/server/docker-compose.yml" <<'PRE'
services:
  redis:
    image: redis:7.4-alpine@sha256:ff02b58f971e7d7d156a1267e283fcbbeee91773b6aa36c49dac28ecfe28eadf
    ports:
      - "30179:6379"
PRE
git -C "$REPO" add server/docker-compose.yml internal/version/version.go
git -C "$REPO" commit -qm pre-proxy
PRE_REF="$(git -C "$REPO" rev-parse HEAD)"
# The every-mode gate commit, and an unrelated commit that does not descend
# from it (a proxy built from another line of history).
git -C "$REPO" commit -q --allow-empty -m 'respproxy: every-mode gate'
GATE_REF="$(git -C "$REPO" rev-parse HEAD)"
UNRELATED_REF="$(git -C "$REPO" commit-tree "$(git -C "$REPO" hash-object -t tree /dev/null)" -m unrelated)"
# Commit 2: the real proxy compose and rollback override from this tree.
cp "$SRC_SERVER/docker-compose.yml" "$REPO/server/docker-compose.yml"
cp "$SRC_SERVER/redis.conf" "$SRC_SERVER/redis.acl" "$REPO/server/"
cp "$SRC_SERVER/respproxy/docker-compose.rollback.yml" "$REPO/server/respproxy/"
printf '# test gate\n%s\n' "$GATE_REF" > "$REPO/server/respproxy/min-gate-commit"
git -C "$REPO" add server
git -C "$REPO" commit -qm proxy
export FAKE_COMMIT="$(git -C "$REPO" rev-parse HEAD)"
export FAKE_RP_REV="$FAKE_COMMIT"   # proxy image built from the release commit

export OLD_M="sha256:$(printf 'a%.0s' {1..64})"
export OLD_F="sha256:$(printf 'b%.0s' {1..64})"
export OLD_R="sha256:$(printf 'c%.0s' {1..64})"
export NEW_M="sha256:$(printf 'd%.0s' {1..64})"
export NEW_F="sha256:$(printf 'e%.0s' {1..64})"
export NEW_R="sha256:$(printf 'f%.0s' {1..64})"
export RP_D="sha256:$(printf '1%.0s' {1..64})"
export FAKE_REPO="$REPO"
export FAKE_LOG="$WORK/docker.log"
export FAKE_STATE="$WORK/state"
export FAKE_FAIL=""
export FAKE_ACL="$WORK/redis-rollback.acl"
SECRET_FARM='farmsecret-0123456789abcdef0123456789abcdef'
export FAKE_MAC_LINE='user mac-4f1c2a9e7b3d on #9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08 ~* &* +@all'

cat > "$WORK/bin/docker" <<'DOCKER'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "docker $*" >> "$FAKE_LOG"
manager='ghcr.io/lelanddutcher/juicemount-manager'
farm='ghcr.io/lelanddutcher/juicefarm'
render='ghcr.io/lelanddutcher/juicefarm-gpu'
st() { cat "$FAKE_STATE/$1" 2>/dev/null || true; }
if [ "$1" = compose ]; then
  topo=proxy files=''
  prev=''
  for a in "$@"; do
    if [ "$prev" = -f ]; then
      files="$files $a"
      case "$a" in *pre-proxy*|*previous-compose*) topo=pre ;; *docker-compose.rollback.yml) topo=direct ;; esac
    fi
    prev="$a"
  done
  cmd=''
  for a in "$@"; do case "$a" in ps|config|up|rm|exec|stop) cmd="$a"; break ;; esac; done
  svc="${!#}"
  case "$cmd" in
    ps) [ -n "$(st "$svc")" ] && echo "$svc-id" || true ;;
    config)
      case " $* " in
        *' --hash '*) printf 'a%.0s' {1..64}; echo ;;
        *' --format json '*)
          md="$(sed -n 's/^JM_MANAGER_DIGEST=//p' "$FAKE_REPO/server/.env")"
          fd="$(sed -n 's/^JM_FARM_DIGEST=//p' "$FAKE_REPO/server/.env")"
          rd="$(sed -n 's/^JM_FARM_RENDER_DIGEST=//p' "$FAKE_REPO/server/.env")"
          [ -n "${JM_MANAGER_DIGEST:-}" ] && md="$JM_MANAGER_DIGEST" && fd="$JM_FARM_DIGEST" && rd="$JM_FARM_RENDER_DIGEST"
          stage="$(sed -n 's/^JM_RESPPROXY_STAGE_BIRTH=//p' "$FAKE_REPO/server/.env")"
          case "$stage" in
            \"*\"|\'*\') stage="${stage:1:${#stage}-2}" ;;
          esac
          if [ "${JM_RESPPROXY_STAGE_BIRTH+x}" = x ]; then stage="${JM_RESPPROXY_STAGE_BIRTH:-false}"; fi
          [ -n "$stage" ] || stage=false
          jq -n --arg manager "$manager@$md" --arg farm "$farm@$fd" --arg render "$render@$rd" --arg stage "$stage" \
            '{services:{"juicemount-manager":{image:$manager,environment:{JM_ADMIN_KEY:"0123456789abcdef0123456789abcdef",JM_FARM_RENDER_IMAGE:$render}},juicefarm:{image:$farm},respproxy:{environment:{JM_RESPPROXY_STAGE_BIRTH:$stage}}}}' ;;
      esac ;;
    stop) ;;
    rm)
      [ "$svc" = respproxy ] || { echo "unexpected rm of $svc" >&2; exit 1; }
      rm -f "$FAKE_STATE/respproxy" ;;
    up)
      for s in redis respproxy juicefs-init juicefs juicemount-manager juicefarm minio; do
        case " $* " in *" $s "*|*" $s") ;; *) continue ;; esac
        # Model the single host port 30179.
        if [ "$s" = respproxy ] && { [ "$(st redis)" = pre ] || [ "$(st redis)" = direct ]; }; then
          echo 'Bind for 0.0.0.0:30179 failed: port is already allocated' >&2; exit 1
        fi
        if [ "$s" = redis ] && [ "$topo" != proxy ] && [ -n "$(st respproxy)" ]; then
          echo 'Bind for 0.0.0.0:30179 failed: port is already allocated' >&2; exit 1
        fi
        [ "$topo" = proxy ] && [ "$s" = respproxy ] && [ "$FAKE_FAIL" = respproxy_start ] && exit 1
        printf '%s' "$topo" > "$FAKE_STATE/$s"
        if [ "$s" = juicemount-manager ] || [ "$s" = juicefarm ]; then
          if [ "${JM_MANAGER_DIGEST:-}" = "$NEW_M" ]; then printf new > "$FAKE_STATE/$s.img"; else printf old > "$FAKE_STATE/$s.img"; fi
        fi
      done ;;
    *) echo "unexpected compose call: $*" >&2; exit 1 ;;
  esac
  exit 0
fi
case "$1" in
  inspect)
    shift
    [ "${1:-}" = -f ] || exit 0
    template="$2"; id="$3"; svc="${id%-id}"
    case "$template" in
      *com.docker.compose.project.working_dir*) echo "$FAKE_REPO/server" ;;
      *com.docker.compose.project.config_files*) echo "$FAKE_REPO/server/docker-compose.yml" ;;
      *com.docker.compose.project*) echo testproject ;;
      *com.docker.compose.service*) echo "$svc" ;;
      *com.docker.compose.config-hash*) printf 'a%.0s' {1..64}; echo ;;
      *State.Running*) [ -n "$(st "$svc")" ] && echo true || echo false ;;
      *State.Health.Status*)
        if [ "$svc" = respproxy ] && [ "$FAKE_FAIL" = respproxy_unhealthy ]; then echo unhealthy; else echo healthy; fi ;;
      *State.Status*) echo exited ;;
      *State.ExitCode*) echo 0 ;;
      *PortBindings*)
        case "$svc:$(st "$svc")" in
          redis:pre|redis:direct|respproxy:proxy) echo '{"6379/tcp":[{"HostIp":"","HostPort":"30179"}]}' ;;
          *) echo '{}' ;;
        esac ;;
      *Config.Image*)
        case "$svc" in
          juicemount-manager) if [ "$(st juicemount-manager.img)" = new ]; then echo "$manager@$NEW_M"; else echo "$manager@$OLD_M"; fi ;;
          juicefarm) if [ "$(st juicefarm.img)" = new ]; then echo "$farm@$NEW_F"; else echo "$farm@$OLD_F"; fi ;;
          *) echo "img-$svc" ;;
        esac ;;
      *Config.Env*)
        printf 'JM_ADMIN_KEY=0123456789abcdef0123456789abcdef\nJM_FARM_RENDER_IMAGE=%s@%s\n' "$render" "$OLD_R" ;;
      *) echo "unexpected inspect template: $template" >&2; exit 1 ;;
    esac ;;
  image)
    case "$*" in *org.opencontainers.image.revision*) printf '%s\n' "$FAKE_RP_REV" ;; esac
    exit 0 ;;
  pull) exit 0 ;;
  run)
    case "$*" in
      *juicemount-manager*) echo "juicemount-manager 0.5.0 ($FAKE_COMMIT)" ;;
      *juicefarm*) echo "jmfarm 0.5.0 ($FAKE_COMMIT)" ;;
    esac ;;
  exec)
    shift
    [ "$1" = -i ] && shift
    id="$1"; svc="${id%-id}"
    case "$*" in
      *--build-info*)
        if [ "$svc" = juicemount-manager ]; then b=juicemount-manager; else b=jmfarm; fi
        if [ "$(st "$svc.img")" = new ]; then echo "$b 0.5.0 ($FAKE_COMMIT)"; else echo "$b 0.4.9 (0000000000000000000000000000000000000000)"; fi ;;
      *'HSET jmlease:v1:cfg mode passthrough'*)
        [ "$svc" = redis ] || { echo "mode reset sent to $svc" >&2; exit 1; }
        printf passthrough > "$FAKE_STATE/cfg-mode"; echo 0 ;;
      *rollback-macs.acl*)
        [ "$FAKE_FAIL" = no_fragment ] && { echo 'cat: no such file' >&2; exit 1; }
        # Same shape the Manager writes: a comment header, then one line per Mac.
        printf '# Enrolled Macs as plain Redis ACL users for the H-8B severity-3 rollback.\n# Written by juicemount-manager.\n%s\n' "$FAKE_MAC_LINE" ;;
      *jm-rollback-users.acl*) cat > "$FAKE_ACL" ;;
      *'sh -ec'*) echo 200 ;;
      *) echo "unexpected exec: $*" >&2; exit 1 ;;
    esac ;;
  *) echo "unexpected docker call: $*" >&2; exit 1 ;;
esac
DOCKER
chmod +x "$WORK/bin/docker"
printf '#!/usr/bin/env bash\nexit 0\n' > "$WORK/bin/sleep"
chmod +x "$WORK/bin/sleep"
export PATH="$WORK/bin:$PATH"
unset JM_RESPPROXY_STAGE_BIRTH

write_env() { # manager farm render digests
  {
    printf 'MINIO_ROOT_PASSWORD=test-password\nJM_ADMIN_KEY=0123456789abcdef0123456789abcdef\nJM_FARM_STORAGE_CAPACITY_BYTES=100000000000\n'
    printf 'JM_MANAGER_DIGEST=%s\nJM_FARM_DIGEST=%s\nJM_FARM_RENDER_DIGEST=%s\nJM_RESPPROXY_DIGEST=%s\n' "$1" "$2" "$3" "$RP_D"
    printf 'JM_REDIS_PROXY_PASSWORD=proxysecret-0123456789abcdef0123456789abcdef\n'
    printf 'JM_PROXY_FARM_PASSWORD=%s\n' "$SECRET_FARM"
    printf 'JM_PROXY_MANAGER_PASSWORD=managersecret-0123456789abcdef0123456789ab\n'
    printf 'JM_PROXY_BG_PASSWORD=bgsecret-0123456789abcdef0123456789abcdef01\n'
    printf 'JM_PROXY_WEBDAV_PASSWORD=webdavsecret-0123456789abcdef0123456789abcd\n'
    printf 'JM_PROXY_ADMIN_PASSWORD=adminsecret-0123456789abcdef0123456789abcde\n'
  } > "$REPO/server/.env"
}
set_topology() { # topology for every service; respproxy only in proxy
  rm -rf "$FAKE_STATE"; mkdir -p "$FAKE_STATE"
  for s in redis juicefs-init juicefs juicemount-manager juicefarm minio; do printf '%s' "$1" > "$FAKE_STATE/$s"; done
  printf old > "$FAKE_STATE/juicemount-manager.img"; printf old > "$FAKE_STATE/juicefarm.img"
  [ "$1" = proxy ] && printf proxy > "$FAKE_STATE/respproxy"
  : > "$FAKE_LOG"; rm -f "$FAKE_ACL"
  rm -rf "$REPO/.update-lock"
  export FAKE_FAIL=""
}
assert_topology() { # expected topology for redis + dependents, expected respproxy presence
  local want="$1" s
  for s in redis juicefs-init juicefs juicemount-manager juicefarm; do
    [ "$(cat "$FAKE_STATE/$s")" = "$want" ] || { echo "FAIL: $s is $(cat "$FAKE_STATE/$s"), want $want" >&2; exit 1; }
  done
  if [ "$want" = proxy ]; then
    [ "$(cat "$FAKE_STATE/respproxy" 2>/dev/null)" = proxy ] || { echo 'FAIL: respproxy not running' >&2; exit 1; }
  else
    [ ! -e "$FAKE_STATE/respproxy" ] || { echo 'FAIL: respproxy still holds 30179' >&2; exit 1; }
  fi
  [ "$(cat "$FAKE_STATE/minio")" != "" ]
  ! grep -E 'docker compose .* up .* minio( |$)' "$FAKE_LOG" >/dev/null || { echo 'FAIL: minio recreated' >&2; exit 1; }
}
line_of() { { grep -n -E "$1" "$FAKE_LOG" || true; } | head -1 | cut -d: -f1; }

cut=(--repo "$REPO" --project testproject --cutover-proxy --previous-ref "$PRE_REF"
  --manager-digest "$NEW_M" --farm-digest "$NEW_F" --render-digest "$NEW_R" --ack-worker-quiesced --ack-macs-drained)

# 1. The port swap: Redis gives up 30179 before the proxy binds it.
set_topology pre; write_env "$OLD_M" "$OLD_F" "$OLD_R"
bash "$SCRIPT" "${cut[@]}" > "$WORK/out" 2>&1 || { cat "$WORK/out"; echo 'FAIL: cutover failed' >&2; exit 1; }
assert_topology proxy
r="$(line_of 'compose .* up .* redis$')"; p="$(line_of 'compose .* up .* respproxy$')"
[ -n "$r" ] && [ -n "$p" ] && [ "$r" -lt "$p" ] || { echo 'FAIL: proxy started before Redis released 30179' >&2; exit 1; }
q="$(line_of 'compose .* stop .*juicefarm juicemount-manager juicefs$')"; m="$(line_of 'HSET jmlease:v1:cfg mode passthrough')"
[ -n "$q" ] && [ "$q" -lt "$r" ] || { echo 'FAIL: volume clients not quiesced before Redis restarted' >&2; exit 1; }
[ -n "$m" ] && [ "$r" -lt "$m" ] && [ "$m" -lt "$p" ] || { echo 'FAIL: mode not forced to passthrough between Redis and proxy start' >&2; exit 1; }
grep -Fx "JM_MANAGER_DIGEST=$NEW_M" "$REPO/server/.env" >/dev/null
[ ! -e "$REPO/.update-lock" ]
! grep -q 'secret-0123456789' "$WORK/out"
echo 'ok: cutover moves 30179 from Redis to the proxy and recreates every volume client'

# 2. Staged rollback: the proxy never becomes healthy -> pre-proxy compose
# restored (Redis publishes 30179 again, old images and digests back).
for failure in respproxy_unhealthy respproxy_start; do
  set_topology pre; write_env "$OLD_M" "$OLD_F" "$OLD_R"
  export FAKE_FAIL="$failure"
  if bash "$SCRIPT" "${cut[@]}" > "$WORK/out" 2>&1; then echo "FAIL: cutover accepted $failure" >&2; exit 1; fi
  assert_topology pre
  [ "$(cat "$FAKE_STATE/juicemount-manager.img")" = old ] || { echo 'FAIL: old Manager image not restored' >&2; exit 1; }
  grep -Fx "JM_MANAGER_DIGEST=$OLD_M" "$REPO/server/.env" >/dev/null || { echo 'FAIL: .env digests not restored' >&2; exit 1; }
  [ ! -e "$REPO/.update-lock" ] || { cat "$WORK/out"; echo 'FAIL: lock kept after a successful rollback' >&2; exit 1; }
  rm_line="$(line_of 'compose .* rm .*respproxy')"; redis_back="$(line_of 'compose .*pre-proxy.* up .* redis$')"
  [ -n "$rm_line" ] && [ -n "$redis_back" ] && [ "$rm_line" -lt "$redis_back" ] || { echo 'FAIL: rollback re-published Redis before freeing 30179' >&2; exit 1; }
  echo "ok: $failure rolls the port swap back to the pre-proxy compose"
done

# 3. A cutover on an already cut-over stack is refused before any mutation.
set_topology proxy; write_env "$OLD_M" "$OLD_F" "$OLD_R"
if bash "$SCRIPT" "${cut[@]}" > "$WORK/out" 2>&1; then echo 'FAIL: second cutover accepted' >&2; exit 1; fi
! grep -q 'compose .* up ' "$FAKE_LOG" || { echo 'FAIL: refused cutover mutated services' >&2; exit 1; }
echo 'ok: cutover refuses a stack where Redis no longer publishes 30179'

# 3b. The proxy image must contain the every-mode gate (min-gate-commit).
for rev in "$PRE_REF" "$UNRELATED_REF" ""; do
  set_topology pre; write_env "$OLD_M" "$OLD_F" "$OLD_R"
  export FAKE_RP_REV="$rev"
  if bash "$SCRIPT" "${cut[@]}" > "$WORK/out" 2>&1; then echo "FAIL: cutover accepted proxy revision '$rev'" >&2; exit 1; fi
  ! grep -q 'compose .* up \|compose .* stop ' "$FAKE_LOG" || { echo "FAIL: proxy revision '$rev' refused only after mutation" >&2; exit 1; }
  [ ! -e "$REPO/.update-lock" ]
done
export FAKE_RP_REV="$GATE_REF"
set_topology pre; write_env "$OLD_M" "$OLD_F" "$OLD_R"
bash "$SCRIPT" "${cut[@]}" > "$WORK/out" 2>&1 || { cat "$WORK/out"; echo 'FAIL: cutover refused the gate commit itself' >&2; exit 1; }
export FAKE_RP_REV="$FAKE_COMMIT"
echo 'ok: cutover refuses a proxy image older than, or unrelated to, the every-mode gate'

# 4. Severity-3 rollback: proxy out of the path, Redis on 30179 with every
# principal mirrored as a plain ACL user (sha256 only, never plaintext).
set_topology proxy; write_env "$OLD_M" "$OLD_F" "$OLD_R"
bash "$SCRIPT" --repo "$REPO" --project testproject --proxy-rollback --ack-worker-quiesced --ack-macs-drained > "$WORK/out" 2>&1 || { cat "$WORK/out"; echo 'FAIL: proxy rollback failed' >&2; exit 1; }
assert_topology direct
farm_sha="$(printf '%s' "$SECRET_FARM" | shasum -a 256 | cut -d' ' -f1)"
grep -Fx "user farm on #$farm_sha ~* &* +@all" "$FAKE_ACL" >/dev/null || { cat "$FAKE_ACL"; echo 'FAIL: farm principal not mirrored' >&2; exit 1; }
for u in manager bg webdav admin; do grep -q "^user $u on #[0-9a-f]\{64\} ~\* &\* +@all$" "$FAKE_ACL" || { echo "FAIL: $u not mirrored" >&2; exit 1; }; done
grep -Fx "$FAKE_MAC_LINE" "$FAKE_ACL" >/dev/null || { echo 'FAIL: enrolled Mac not mirrored' >&2; exit 1; }
! grep -q 'secret-0123456789' "$FAKE_ACL" "$WORK/out" || { echo 'FAIL: plaintext secret leaked' >&2; exit 1; }
rm_line="$(line_of 'compose .* rm .*respproxy')"; redis_line="$(line_of 'compose .*rollback.yml.* up .* redis$')"
[ -n "$rm_line" ] && [ -n "$redis_line" ] && [ "$rm_line" -lt "$redis_line" ]
[ ! -e "$REPO/.update-lock" ]
echo 'ok: proxy rollback publishes Redis on 30179 with the mirrored principal ACL'

# 5. ...and back to the proxy. The stored mode (enforce before the
# rollback) must not resume over sessions the new proxy does not own.
printf enforce > "$FAKE_STATE/cfg-mode"; : > "$FAKE_LOG"
bash "$SCRIPT" --repo "$REPO" --project testproject --proxy-restore --ack-worker-quiesced --ack-macs-drained > "$WORK/out" 2>&1 || { cat "$WORK/out"; echo 'FAIL: proxy restore failed' >&2; exit 1; }
assert_topology proxy
[ "$(cat "$FAKE_STATE/cfg-mode")" = passthrough ] || { echo 'FAIL: --proxy-restore resumed the stored enforce mode' >&2; exit 1; }
m="$(line_of 'HSET jmlease:v1:cfg mode passthrough')"; p="$(line_of 'compose .* up .* respproxy$')"
[ -n "$m" ] && [ "$m" -lt "$p" ] || { echo 'FAIL: proxy started before the mode reset' >&2; exit 1; }
echo 'ok: proxy restore moves 30179 back to the proxy in passthrough'

# A transition may enter the proxy topology on either the forward path or
# automatic rollback. Refuse opted-in or malformed resolved stage birth before
# any service, Redis mode, digest, or ACL mutation. The shim resolves quoted
# .env values and shell overrides as Docker Compose would.
for mode in cutover proxy-rollback proxy-restore; do
  case "$mode" in
    cutover) initial=pre; target=proxy; args=("${cut[@]}") ;;
    proxy-rollback) initial=proxy; target=direct; args=(--repo "$REPO" --project testproject --proxy-rollback --ack-worker-quiesced --ack-macs-drained) ;;
    proxy-restore) initial=direct; target=proxy; args=(--repo "$REPO" --project testproject --proxy-restore --ack-worker-quiesced --ack-macs-drained) ;;
  esac
  for source in quoted-env shell-override malformed-env shell-newline; do
    set_topology "$initial"; write_env "$OLD_M" "$OLD_F" "$OLD_R"
    case "$source" in
      quoted-env) printf "JM_RESPPROXY_STAGE_BIRTH='true'\n" >> "$REPO/server/.env" ;;
      shell-override) printf "JM_RESPPROXY_STAGE_BIRTH='false'\n" >> "$REPO/server/.env" ;;
      malformed-env) printf "JM_RESPPROXY_STAGE_BIRTH='TRUE'\n" >> "$REPO/server/.env" ;;
      shell-newline) printf "JM_RESPPROXY_STAGE_BIRTH='false'\n" >> "$REPO/server/.env" ;;
    esac
    cp "$REPO/server/.env" "$WORK/env.before"
    rm -rf "$WORK/state.before"; cp -R "$FAKE_STATE" "$WORK/state.before"
    if [ "$source" = shell-override ]; then
      if JM_RESPPROXY_STAGE_BIRTH=true bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1; then
        echo "FAIL: $mode accepted shell-overridden stage birth" >&2; exit 1
      fi
    elif [ "$source" = shell-newline ]; then
      if JM_RESPPROXY_STAGE_BIRTH=$'false\n' bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1; then
        echo "FAIL: $mode accepted trailing-newline stage birth" >&2; exit 1
      fi
    elif bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1; then
      echo "FAIL: $mode accepted $source stage birth" >&2; exit 1
    fi
    if [ "$source" = malformed-env ] || [ "$source" = shell-newline ]; then
      grep -F 'resolved JM_RESPPROXY_STAGE_BIRTH must be true or false' "$WORK/out" >/dev/null || { cat "$WORK/out"; echo "FAIL: $mode malformed value lacked diagnostic" >&2; exit 1; }
    else
      grep -F 'disable JM_RESPPROXY_STAGE_BIRTH before changing topology' "$WORK/out" >/dev/null || { cat "$WORK/out"; echo "FAIL: $mode stage birth lacked operator instruction" >&2; exit 1; }
    fi
    cmp -s "$WORK/env.before" "$REPO/server/.env" || { echo "FAIL: $mode changed .env before refusal" >&2; exit 1; }
    diff -qr "$WORK/state.before" "$FAKE_STATE" >/dev/null || { echo "FAIL: $mode changed topology or port ownership before refusal" >&2; exit 1; }
    [ ! -e "$FAKE_ACL" ] && [ ! -e "$REPO/.update-lock" ] || { echo "FAIL: $mode wrote ACL or retained lock" >&2; exit 1; }
    if grep -E '^docker (pull|run) |^docker compose .* (stop|rm|up)( |$)|^docker exec .*HSET jmlease:v1:cfg mode passthrough' "$FAKE_LOG" >/dev/null; then
      echo "FAIL: $mode mutated services after resolved stage-birth refusal" >&2; exit 1
    fi
    assert_topology "$initial"
    ! grep -q 'secret-0123456789' "$WORK/out" || { echo "FAIL: $mode leaked a synthetic password" >&2; exit 1; }
  done

  # The resolved value wins: a shell false override permits the existing
  # passthrough-safe transition even when the quoted .env value is true.
  for override in false empty; do
    set_topology "$initial"; write_env "$OLD_M" "$OLD_F" "$OLD_R"
    printf "JM_RESPPROXY_STAGE_BIRTH='true'\n" >> "$REPO/server/.env"
    if [ "$override" = false ]; then
      JM_RESPPROXY_STAGE_BIRTH=false bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1 || {
        cat "$WORK/out"; echo "FAIL: $mode ignored resolved false shell override" >&2; exit 1;
      }
    else
      JM_RESPPROXY_STAGE_BIRTH= bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1 || {
        cat "$WORK/out"; echo "FAIL: $mode ignored empty shell override's false default" >&2; exit 1;
      }
    fi
    assert_topology "$target"
    [ ! -e "$REPO/.update-lock" ]
    echo "ok: $mode resolves stage birth before mutation and honors the $override shell override"
  done
done

# 5a. --proxy-restore checks the gate too.
set_topology direct; rm -f "$FAKE_STATE/respproxy"; write_env "$OLD_M" "$OLD_F" "$OLD_R"
export FAKE_RP_REV="$PRE_REF"
if bash "$SCRIPT" --repo "$REPO" --project testproject --proxy-restore --ack-worker-quiesced --ack-macs-drained > "$WORK/out" 2>&1; then
  echo 'FAIL: proxy restore accepted a proxy image without the every-mode gate' >&2; exit 1
fi
grep -q 'predates the every-mode gate' "$WORK/out"
! grep -q 'compose .* up ' "$FAKE_LOG"
export FAKE_RP_REV="$FAKE_COMMIT"
echo 'ok: proxy restore refuses a proxy image without the every-mode gate'

# 5b. Topology modes refuse without the Mac drain acknowledgement.
if bash "$SCRIPT" --repo "$REPO" --project testproject --proxy-rollback --ack-worker-quiesced > "$WORK/out" 2>&1; then
  echo 'FAIL: proxy rollback ran without --ack-macs-drained' >&2; exit 1
fi
grep -q 'drain its spool' "$WORK/out"
echo 'ok: topology modes require the Mac drain acknowledgement'

# 6. A malformed Mac line from the Manager is refused before anything moves.
set_topology proxy; write_env "$OLD_M" "$OLD_F" "$OLD_R"
export FAKE_MAC_LINE='user evil on nopass ~* &* +@all'
if bash "$SCRIPT" --repo "$REPO" --project testproject --proxy-rollback --ack-worker-quiesced --ack-macs-drained > "$WORK/out" 2>&1; then
  echo 'FAIL: accepted a passwordless rollback user' >&2; exit 1
fi
! grep -q 'compose .* up ' "$FAKE_LOG"
echo 'ok: proxy rollback refuses an unsafe rollback ACL line before mutation'

# 7. An unreadable Mac fragment must not silently drop every enrolled Mac.
set_topology proxy; write_env "$OLD_M" "$OLD_F" "$OLD_R"
export FAKE_MAC_LINE='user mac-4f1c2a9e7b3d on #9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08 ~* &* +@all'
export FAKE_FAIL=no_fragment
if bash "$SCRIPT" --repo "$REPO" --project testproject --proxy-rollback --ack-worker-quiesced --ack-macs-drained > "$WORK/out" 2>&1; then
  echo 'FAIL: proxy rollback ran without the enrolled Macs' >&2; exit 1
fi
! grep -q 'compose .* up ' "$FAKE_LOG"
export FAKE_FAIL=""
echo 'ok: proxy rollback refuses when the Manager fragment cannot be read'
