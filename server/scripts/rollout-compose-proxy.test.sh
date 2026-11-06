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
        # A Redis that exits right after its recreate (crash loop).
        if [ "$s" = redis ]; then
          rm -f "$FAKE_STATE/redis.down"
          [ "$topo" = "${FAKE_REDIS_DIES:-}" ] && : > "$FAKE_STATE/redis.down"
        fi
        if [ "$s" = juicemount-manager ] || [ "$s" = juicefarm ]; then
          if [ "${JM_MANAGER_DIGEST:-}" = "$NEW_M" ]; then printf new > "$FAKE_STATE/$s.img"; else printf old > "$FAKE_STATE/$s.img"; fi
        fi
      done ;;
    *) echo "unexpected compose call: $*" >&2; exit 1 ;;
  esac
  exit 0
fi
case "$1" in
  ps)
    # Only the running redis of the project (lib/redis-compact.sh).
    case "$*" in
      *label=com.docker.compose.project=testproject*label=com.docker.compose.service=redis*status=running*)
        # Only this checkout's Redis: the working directory filter must name it.
        # A filtered lookup must name this checkout; an unfiltered one is the
        # other-directory probe.
        case "$*" in
          *"label=com.docker.compose.project.working_dir=$FAKE_REPO/server"*) ;;
          *label=com.docker.compose.project.working_dir=*) echo "ps for another checkout: $*" >&2; exit 1 ;;
        esac
        # FAKE_REDIS_PS_LIMIT=N: Redis is found by the first N lookups only
        # (it stops after the preflight read).
        # (The counter lives outside FAKE_STATE, which tests compare.)
        pscalls="$(cat "$FAKE_STATE.ps-calls" 2>/dev/null || true)"; pscalls=$(( ${pscalls:-0} + 1 )); printf '%s' "$pscalls" > "$FAKE_STATE.ps-calls"
        if [ -n "${FAKE_REDIS_PS_LIMIT:-}" ] && [ "$pscalls" -gt "$FAKE_REDIS_PS_LIMIT" ]; then exit 0; fi
        [ -n "$(st redis)" ] && [ ! -e "$FAKE_STATE/redis.down" ] && echo redis-id
        exit 0 ;;
    esac
    echo "unexpected ps: $*" >&2; exit 1 ;;
  inspect)
    shift
    [ "${1:-}" = -f ] || exit 0
    template="$2"; id="$3"; svc="${id%-id}"
    case "$template" in
      *com.docker.compose.project.working_dir*) echo "$FAKE_REPO/server" ;;
      *com.docker.compose.project.config_files*) echo "$FAKE_REPO/server/docker-compose.yml" ;;
      *com.docker.compose.project*) echo testproject ;;
      *com.docker.compose.service*) echo "$svc" ;;
      *'.Destination "/data"'*) echo /mnt/zSSD/juicemount/redis ;;
      *com.docker.compose.config-hash*) printf 'a%.0s' {1..64}; echo ;;
      *State.Running*)
        if [ -n "$(st "$svc")" ] && { [ "$svc" != redis ] || [ ! -e "$FAKE_STATE/redis.down" ]; }; then echo true; else echo false; fi ;;
      *State.Health.Status*)
        if [ "$svc" = redis ] && [ -e "$FAKE_STATE/redis.down" ]; then echo unhealthy; elif [ "$svc" = respproxy ] && [ "$FAKE_FAIL" = respproxy_unhealthy ]; then echo unhealthy; else echo healthy; fi ;;
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
      *jm-redis-cli*BGREWRITEAOF*)
        [ "$svc" = redis ] || { echo "BGREWRITEAOF sent to $svc" >&2; exit 1; }
        calls="$(st aof-calls)"; calls=$(( ${calls:-0} + 1 )); printf '%s' "$calls" > "$FAKE_STATE/aof-calls"
        # FAKE_AOF_REFUSE_FROM=N: the Nth and later requests are refused
        # (2: the forward rewrite works, the rollback's is refused).
        if [ "$FAKE_FAIL" = bgrewrite_refused ] || { [ -n "${FAKE_AOF_REFUSE_FROM:-}" ] && [ "$calls" -ge "$FAKE_AOF_REFUSE_FROM" ]; }; then
          echo 'ERR Background append only file rewriting failed to start'; exit 0
        fi
        # Redis counts a rewrite when it forks (aof.c), so before it ends.
        n="$(st aof-rewrites)"; printf '%s' "$(( ${n:-0} + 1 ))" > "$FAKE_STATE/aof-rewrites"
        case "$FAKE_FAIL" in
          rewrite_hang) printf 1 > "$FAKE_STATE/aof-busy" ;;
          rewrite_err) printf err > "$FAKE_STATE/aof-status" ;;
          rewrite_killed) ;; # the child was stopped: status stays ok, no new base file
          *) b="$(st aof-base)"; printf '%s' "$(( ${b:-1} + 1 ))" > "$FAKE_STATE/aof-base" ;;
        esac
        echo 'Background append only file rewriting started' ;;
      *'cat /run/jm-redis/users.acl'*)
        # The pre-proxy compose runs Redis without an ACL file.
        [ "$(st redis)" != pre ] || { echo 'cat: no such file' >&2; exit 1; }
        [ "${FAKE_RUNNING_ACL:-fixed}" != unreadable ] || { echo 'cat: permission denied' >&2; exit 1; }
        if [ "${FAKE_RUNNING_ACL:-fixed}" = old ]; then echo 'user default off resetpass resetkeys resetchannels -@all'
        else echo 'user default off resetpass resetkeys resetchannels ~* &* +@all'; fi ;;
      *jm-redis-snapshot*)
        [ "$FAKE_FAIL" = info_fail ] && exit 1
        busy="$(st aof-busy)"; status="$(st aof-status)"; n="$(st aof-rewrites)"; b="$(st aof-base)"
        fails=0; [ "$status" = err ] && fails=1
        printf 'aof_enabled:1\r\naof_rewrite_in_progress:%s\r\naof_rewrite_scheduled:0\r\naof_last_bgrewrite_status:%s\r\naof_rewrites:%s\r\naof_rewrites_consecutive_failures:%s\r\njm_base_seq:%s\n' \
          "${busy:-0}" "${status:-ok}" "${n:-0}" "$fails" "${b:-1}" ;;
      *jm-redis-cli*'CONFIG GET aclfile'*)
        # The pre-proxy compose runs Redis without an ACL file.
        if [ "$(st redis)" = pre ]; then printf 'aclfile\n\n'; else printf 'aclfile\n/run/jm-redis/users.acl\n'; fi ;;
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
  rm -rf "$FAKE_STATE" "$FAKE_STATE.ps-calls"; mkdir -p "$FAKE_STATE"
  for s in redis juicefs-init juicefs juicemount-manager juicefarm minio; do printf '%s' "$1" > "$FAKE_STATE/$s"; done
  printf old > "$FAKE_STATE/juicemount-manager.img"; printf old > "$FAKE_STATE/juicefarm.img"
  printf 7 > "$FAKE_STATE/aof-rewrites"
  [ "$1" = proxy ] && printf proxy > "$FAKE_STATE/respproxy"
  : > "$FAKE_LOG"; rm -f "$FAKE_ACL"
  rm -rf "$REPO/.update-lock"
  export FAKE_FAIL="" FAKE_REDIS_DIES="" FAKE_AOF_REFUSE_FROM="" FAKE_RUNNING_ACL=fixed FAKE_REDIS_PS_LIMIT=
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
# The Redis AOF is rewritten, and the rewrite confirmed, before the first
# service stop and before every Redis recreate.
assert_compacted_first() {
  local c i s u
  c="$(line_of 'jm-redis-cli BGREWRITEAOF')"; i="$(line_of 'jm-redis-snapshot')"
  s="$(line_of '^docker compose .* (stop|rm|up) ')"; u="$(line_of 'compose .* up .* redis$')"
  [ -n "$c" ] && [ -n "$s" ] && [ -n "$u" ] && [ "$c" -lt "$s" ] && [ "$c" -lt "$u" ] || {
    cat "$FAKE_LOG"; echo 'FAIL: Redis stopped or recreated before its AOF was rewritten' >&2; exit 1;
  }
  [ "$(awk -v c="$c" 'NR > c && /jm-redis-snapshot/' "$FAKE_LOG" | wc -l | tr -d ' ')" -ge 1 ] && [ "$i" -lt "$c" ] || {
    echo 'FAIL: the rewrite was not confirmed through INFO persistence' >&2; exit 1;
  }
  [ "$(grep -c 'jm-redis-cli BGREWRITEAOF' "$FAKE_LOG")" -ge "$(grep -cE 'compose .* up .* redis$' "$FAKE_LOG")" ] || {
    echo 'FAIL: a Redis recreate had no AOF rewrite of its own' >&2; exit 1;
  }
}

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
assert_compacted_first
grep -q 'aof_last_bgrewrite_status:ok' "$WORK/out" || { echo 'FAIL: cutover did not report the finished AOF rewrite' >&2; exit 1; }
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
  assert_compacted_first
  # The rollback compacts the new Redis again before it recreates the old one.
  second="$(grep -n 'jm-redis-cli BGREWRITEAOF' "$FAKE_LOG" | sed -n 2p | cut -d: -f1)"
  forward_redis="$(line_of 'compose .* up .* redis$')"
  [ -n "$second" ] && [ "$forward_redis" -lt "$second" ] && [ "$second" -lt "$rm_line" ] || {
    echo 'FAIL: rollback recreated Redis without rewriting the AOF first' >&2; exit 1;
  }
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
assert_compacted_first
echo 'ok: proxy rollback publishes Redis on 30179 with the mirrored principal ACL'

# 5. ...and back to the proxy. The stored mode (enforce before the
# rollback) must not resume over sessions the new proxy does not own.
printf enforce > "$FAKE_STATE/cfg-mode"; : > "$FAKE_LOG"
bash "$SCRIPT" --repo "$REPO" --project testproject --proxy-restore --ack-worker-quiesced --ack-macs-drained > "$WORK/out" 2>&1 || { cat "$WORK/out"; echo 'FAIL: proxy restore failed' >&2; exit 1; }
assert_topology proxy
[ "$(cat "$FAKE_STATE/cfg-mode")" = passthrough ] || { echo 'FAIL: --proxy-restore resumed the stored enforce mode' >&2; exit 1; }
m="$(line_of 'HSET jmlease:v1:cfg mode passthrough')"; p="$(line_of 'compose .* up .* respproxy$')"
[ -n "$m" ] && [ "$m" -lt "$p" ] || { echo 'FAIL: proxy started before the mode reset' >&2; exit 1; }
assert_compacted_first
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

# 8. The AOF rewrite gates every topology change: refused, timed out,
# failed, or unreadable, the change stops with nothing stopped or recreated.
for mode in cutover proxy-rollback proxy-restore; do
  case "$mode" in
    cutover) initial=pre; args=("${cut[@]}") ;;
    proxy-rollback) initial=proxy; args=(--repo "$REPO" --project testproject --proxy-rollback --ack-worker-quiesced --ack-macs-drained) ;;
    proxy-restore) initial=direct; args=(--repo "$REPO" --project testproject --proxy-restore --ack-worker-quiesced --ack-macs-drained) ;;
  esac
  for failure in bgrewrite_refused rewrite_hang rewrite_err info_fail; do
    set_topology "$initial"; write_env "$OLD_M" "$OLD_F" "$OLD_R"
    cp "$REPO/server/.env" "$WORK/env.before"
    export FAKE_FAIL="$failure"
    if JM_REDIS_COMPACT_TIMEOUT=3 bash "$SCRIPT" "${args[@]}" > "$WORK/out" 2>&1; then
      echo "FAIL: $mode went on after $failure" >&2; exit 1
    fi
    if grep -E '^docker compose .* (stop|rm|up)( |$)|HSET jmlease:v1:cfg' "$FAKE_LOG" >/dev/null; then
      cat "$FAKE_LOG"; echo "FAIL: $mode stopped or recreated a service after $failure" >&2; exit 1
    fi
    grep -q 'refusing to stop or recreate Redis: its AOF rewrite did not finish' "$WORK/out" &&
      grep -q 'topology change refused before any service stopped; the stack is unchanged' "$WORK/out" || {
        cat "$WORK/out"; echo "FAIL: $mode $failure lacked an accurate refusal message" >&2; exit 1;
      }
    if [ "$failure" = rewrite_hang ]; then
      grep -q 'did not end within 3 s' "$WORK/out" || { cat "$WORK/out"; echo 'FAIL: timeout not reported' >&2; exit 1; }
    fi
    cmp -s "$WORK/env.before" "$REPO/server/.env" || { echo "FAIL: $mode $failure left .env changed" >&2; exit 1; }
    [ ! -e "$REPO/.update-lock" ] || { cat "$WORK/out"; echo "FAIL: $mode $failure kept the lock" >&2; exit 1; }
    ! grep -q CRITICAL "$WORK/out" || { cat "$WORK/out"; echo "FAIL: $mode $failure attempted a rollback" >&2; exit 1; }
    assert_topology "$initial"
  done
  echo "ok: $mode stops with nothing stopped when the AOF rewrite is refused, times out, fails or cannot be confirmed"
done
export FAKE_FAIL=""

# 9. Redis already down at the rollback: nothing to compact, so the script
# says the start replays the AOF. The pre-proxy compose has no ACL file.
set_topology pre; write_env "$OLD_M" "$OLD_F" "$OLD_R"
export FAKE_REDIS_DIES=proxy
if bash "$SCRIPT" "${cut[@]}" > "$WORK/out" 2>&1; then echo 'FAIL: cutover accepted a Redis that never came up' >&2; exit 1; fi
assert_topology pre
grep -q 'is not running, so its AOF was not rewritten' "$WORK/out" || { cat "$WORK/out"; echo 'FAIL: skipped compaction not reported' >&2; exit 1; }
[ ! -e "$REPO/.update-lock" ]
echo 'ok: a rollback over a stopped Redis says the start replays the AOF'

# 10. ...and refuses to start Redis while server/redis.acl still has the
# 0.5.0 default line, unless --allow-aof-replay is given.
sed -i.bak 's/^user default .*/user default off resetpass resetkeys resetchannels -@all/' "$REPO/server/redis.acl"
rm -f "$REPO/server/redis.acl.bak"
git -C "$REPO" commit -qam 'the 0.5.0 default line'
restore=(--repo "$REPO" --project testproject --proxy-restore --ack-worker-quiesced --ack-macs-drained)
set_topology direct; rm -f "$FAKE_STATE/respproxy"; write_env "$OLD_M" "$OLD_F" "$OLD_R"
export FAKE_REDIS_DIES=proxy
if bash "$SCRIPT" "${restore[@]}" > "$WORK/out" 2>&1; then echo 'FAIL: proxy restore accepted a Redis that never came up' >&2; exit 1; fi
grep -q "refusing to start Redis: .*redis.acl still has the 0.5.0" "$WORK/out" || { cat "$WORK/out"; echo 'FAIL: old ACL not refused' >&2; exit 1; }
! grep -E 'compose .*rollback.yml.* up .* redis$' "$FAKE_LOG" >/dev/null || { echo 'FAIL: Redis started under the 0.5.0 ACL without the override' >&2; exit 1; }
[ -e "$REPO/.update-lock" ] || { echo 'FAIL: refused rollback released the lock' >&2; exit 1; }
set_topology direct; rm -f "$FAKE_STATE/respproxy"; write_env "$OLD_M" "$OLD_F" "$OLD_R"
export FAKE_REDIS_DIES=proxy
if bash "$SCRIPT" "${restore[@]}" --allow-aof-replay > "$WORK/out" 2>&1; then echo 'FAIL: proxy restore accepted a Redis that never came up' >&2; exit 1; fi
grep -q 'WARNING: .*Starting anyway (--allow-aof-replay)' "$WORK/out" || { cat "$WORK/out"; echo 'FAIL: override not reported' >&2; exit 1; }
assert_topology direct
[ ! -e "$REPO/.update-lock" ]
git -C "$REPO" revert --no-edit HEAD >/dev/null
# The fixture HEAD moved: the images report the new release commit.
FAKE_COMMIT="$(git -C "$REPO" rev-parse HEAD)"
FAKE_RP_REV="$FAKE_COMMIT"
export FAKE_COMMIT FAKE_RP_REV
echo 'ok: Redis does not start under the 0.5.0 ACL after a skipped compaction unless --allow-aof-replay is given'

# 11. The forward rewrite works, then the forward change fails and the
# rollback's own rewrite is refused. The forward rewrite already removed
# every refused transaction, so the rollback warns and still restores the
# stack; it must not stop half done.
set_topology pre; write_env "$OLD_M" "$OLD_F" "$OLD_R"
export FAKE_FAIL=respproxy_unhealthy FAKE_AOF_REFUSE_FROM=2
if bash "$SCRIPT" "${cut[@]}" > "$WORK/out" 2>&1; then echo 'FAIL: cutover accepted an unhealthy proxy' >&2; exit 1; fi
assert_topology pre
[ "$(grep -c 'jm-redis-cli BGREWRITEAOF' "$FAKE_LOG")" = 2 ] || { echo 'FAIL: the rollback did not try its own rewrite' >&2; exit 1; }
grep -q 'WARNING: this AOF rewrite did not finish. Going on' "$WORK/out" || { cat "$WORK/out"; echo 'FAIL: the rollback did not say it went on without a rewrite' >&2; exit 1; }
! grep -q 'CRITICAL' "$WORK/out" || { cat "$WORK/out"; echo 'FAIL: the rollback stopped half done' >&2; exit 1; }
[ ! -e "$REPO/.update-lock" ]
echo 'ok: a refused rewrite in the rollback warns and the rollback still restores the stack'

# 12. While the running Redis has the 0.5.0 ACL, a topology change needs
# --ack-redis-aof-backup (the rewrite deletes the only copy of earlier
# losses), and refuses before anything otherwise.
set_topology proxy; write_env "$OLD_M" "$OLD_F" "$OLD_R"
export FAKE_RUNNING_ACL=old
rb=(--repo "$REPO" --project testproject --proxy-rollback --ack-worker-quiesced --ack-macs-drained)
if bash "$SCRIPT" "${rb[@]}" > "$WORK/out" 2>&1; then echo 'FAIL: rewrite of a 0.5.0 Redis without the backup acknowledgement' >&2; exit 1; fi
grep -q 'Copy /mnt/zSSD/juicemount/redis/appendonlydir somewhere safe first (CHANGELOG 0.5.1, step 2), then re-run with --ack-redis-aof-backup. Nothing was changed.' "$WORK/out" || { cat "$WORK/out"; echo 'FAIL: no backup reminder naming the host folder' >&2; exit 1; }
grep -q 'WARNING: the running Redis still has the 0.5.0 ACL, and this run did not rewrite its AOF. Do NOT reboot' "$WORK/out" || { cat "$WORK/out"; echo 'FAIL: a topology refusal did not warn against a reboot' >&2; exit 1; }
! grep -qE 'BGREWRITEAOF|^docker compose .* (stop|rm|up)( |$)' "$FAKE_LOG" || { echo 'FAIL: acted before the backup acknowledgement' >&2; exit 1; }
[ ! -e "$FAKE_ACL" ] && [ ! -e "$REPO/.update-lock" ] || { echo 'FAIL: wrote the rollback ACL or kept the lock before the acknowledgement' >&2; exit 1; }
bash "$SCRIPT" "${rb[@]}" --ack-redis-aof-backup > "$WORK/out" 2>&1 || { cat "$WORK/out"; echo 'FAIL: acknowledged rollback failed' >&2; exit 1; }
assert_topology direct
assert_compacted_first
! grep -q 'Do NOT reboot' "$WORK/out" || { echo 'FAIL: warned after a successful rewrite' >&2; exit 1; }
echo 'ok: a topology change on a 0.5.0 Redis needs --ack-redis-aof-backup'

# 13. The helper's wait, against scripted INFO persistence answers that
# follow Redis 7.4 (aof.c): aof_rewrites rises when a rewrite forks,
# aof_rewrite_scheduled clears only on that fork, the status changes only
# when a rewrite ends. One line per read: scheduled in_progress status
# count base (the AOF manifest's base file sequence number). Each case fails if the helper skips the check it names.
SEQ="$WORK/seq"; mkdir -p "$SEQ/bin"
cat > "$SEQ/bin/docker" <<'SEQDOCKER'
#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  ps\ *) echo redis-id ;;
  *jm-redis-cli*BGREWRITEAOF*)
    echo BGREWRITEAOF >> "$SEQ/log"
    n=$(( $(grep -c BGREWRITEAOF "$SEQ/log") ))
    sed -n "${n}p" "$SEQ/replies" ;;
  *jm-redis-snapshot*)
    echo INFO >> "$SEQ/log"
    n=$(( $(grep -c INFO "$SEQ/log") ))
    last=$(wc -l < "$SEQ/info" | tr -d ' ')
    [ "$n" -le "$last" ] || n="$last"
    read -r sched prog status count base < <(sed -n "${n}p" "$SEQ/info")
    printf 'aof_rewrite_in_progress:%s\r\naof_rewrite_scheduled:%s\r\naof_last_bgrewrite_status:%s\r\naof_rewrites:%s\r\naof_rewrites_consecutive_failures:0\r\njm_base_seq:%s\n' "$prog" "$sched" "$status" "$count" "$base" ;;
  *) echo "unexpected: $*" >&2; exit 1 ;;
esac
SEQDOCKER
chmod +x "$SEQ/bin/docker"
seq_case() { # name, want (ok|fail), replies (|-separated), info lines...
  local name="$1" want="$2" replies="$3" got=ok
  shift 3
  : > "$SEQ/log"
  printf '%s\n' "$replies" | tr '|' '\n' > "$SEQ/replies"
  printf '%s\n' "$@" > "$SEQ/info"
  if ! (export SEQ PATH="$SEQ/bin:$PATH"; . "$(dirname "$SCRIPT")/lib/redis-compact.sh"; JM_REDIS_COMPACT_TIMEOUT=20 jm_redis_compact testproject) > "$WORK/seq.out" 2>&1; then got=fail; fi
  [ "$got" = "$want" ] || { cat "$WORK/seq.out"; echo "FAIL: helper case '$name': want $want, got $got" >&2; exit 1; }
  # Every scripted answer was read: the helper did not stop early.
  [ "$(grep -c INFO "$SEQ/log")" -ge "$#" ] || { cat "$SEQ/log"; echo "FAIL: helper case '$name' returned after $(grep -c INFO "$SEQ/log") of $# INFO reads" >&2; exit 1; }
}
started='Background append only file rewriting started'
scheduled='Background append only file rewriting scheduled'
busy='ERR Background append only file rewriting already in progress'
# A BGSAVE runs, so the rewrite is only scheduled; it forks, then ends.
seq_case scheduled-behind-bgsave ok "$scheduled" '0 0 ok 5 2' '1 0 ok 5 2' '1 0 ok 5 2' '0 1 ok 6 2' '0 0 ok 6 3'
# The scheduled flag cleared with no fork (stopAppendOnly, server.c): only
# the counter shows that no rewrite ran yet.
seq_case scheduled-cleared-without-fork ok "$scheduled" '0 0 ok 5 2' '0 0 ok 5 2' '0 0 ok 5 2' '0 1 ok 6 2' '0 0 ok 6 3'
# An automatic rewrite forked and ended after the baseline while ours is
# still scheduled: only the scheduled flag shows ours has not run.
seq_case auto-rewrite-while-scheduled ok "$scheduled" '0 0 ok 5 2' '1 0 ok 6 3' '1 0 ok 6 3' '0 1 ok 7 3' '0 0 ok 7 4'
# A rewrite is already running when the helper starts (its count already
# rose): wait for it to end, then ask for a new one.
seq_case already-running ok "$started" '0 1 ok 6 2' '0 1 ok 6 2' '0 0 ok 6 3' '0 1 ok 7 3' '0 0 ok 7 4'
[ "$(grep -c BGREWRITEAOF "$SEQ/log")" = 1 ] && [ "$(sed -n 4p "$SEQ/log")" = BGREWRITEAOF ] || { cat "$SEQ/log"; echo 'FAIL: asked for a rewrite while another was running' >&2; exit 1; }
# A rewrite forked between the helper's read and its request.
seq_case already-in-progress-reply ok "$busy|$started" '0 0 ok 5 2' '0 1 ok 6 2' '0 0 ok 6 3' '0 1 ok 7 3' '0 0 ok 7 4'
[ "$(grep -c BGREWRITEAOF "$SEQ/log")" = 2 ] || { echo 'FAIL: did not ask again after "already in progress"' >&2; exit 1; }
# A scheduled rewrite that then fails, after earlier failures (the
# scheduled request reset Redis's consecutive-failure counter): fail now,
# not at the timeout.
seq_case scheduled-then-failed fail "$scheduled" '0 0 err 5 2' '1 0 err 5 2' '0 1 err 6 2' '0 0 err 6 2'
grep -q 'the AOF rewrite failed (aof_last_bgrewrite_status:err)' "$WORK/seq.out" || { cat "$WORK/seq.out"; echo 'FAIL: failed rewrite not reported as such' >&2; exit 1; }
[ "$(grep -c INFO "$SEQ/log")" -le 5 ] || { echo 'FAIL: a failed rewrite was only noticed at the timeout' >&2; exit 1; }
# A rewrite child stopped by a signal (SIGUSR1, or CONFIG SET appendonly no):
# the status stays ok and the count rose, but no new base file was written,
# so the old AOF files, refused transactions included, are still there.
seq_case child-killed fail "$started" '0 0 ok 5 2' '0 1 ok 6 2' '0 0 ok 6 2'
grep -q 'ended without a new base file' "$WORK/seq.out" || { cat "$WORK/seq.out"; echo 'FAIL: a killed rewrite was not reported as such' >&2; exit 1; }
echo 'ok: the helper waits for a scheduled, already-running or raced rewrite, and reports a failed or killed one at once'

# 14. The 0.5.0 ACL was read right after the lock, then Redis stopped before
# the forward stage. Nothing can be rewritten, and starting it under the
# fixed ACL would replay refused transactions: refuse with nothing stopped
# or recreated, and warn.
for mode in proxy-rollback proxy-restore; do
  case "$mode" in proxy-rollback) initial=proxy ;; proxy-restore) initial=direct ;; esac
  set_topology "$initial"; write_env "$OLD_M" "$OLD_F" "$OLD_R"
  export FAKE_RUNNING_ACL=old FAKE_REDIS_PS_LIMIT=1
  if bash "$SCRIPT" --repo "$REPO" --project testproject "--$mode" --ack-worker-quiesced --ack-macs-drained --ack-redis-aof-backup > "$WORK/out" 2>&1; then
    echo "FAIL: $mode recreated a Redis it could not rewrite" >&2; exit 1
  fi
  grep -q 'refusing to start Redis: it is not running, so its AOF cannot be rewritten' "$WORK/out" &&
    grep -q 'topology change refused before any service stopped; the stack is unchanged' "$WORK/out" &&
    grep -q 'WARNING: Redis of project testproject (this checkout) is not running' "$WORK/out" || {
      cat "$WORK/out"; echo "FAIL: $mode did not refuse and warn for a Redis that stopped after the 0.5.0 read" >&2; exit 1;
    }
  ! grep -qE 'BGREWRITEAOF|^docker compose .* (stop|rm|up)( |$)' "$FAKE_LOG" || { echo "FAIL: $mode acted on a Redis it could not rewrite" >&2; exit 1; }
  ! grep -q 'already removed every transaction' "$WORK/out" || { echo "FAIL: $mode claimed a rewrite that never ran" >&2; exit 1; }
  assert_topology "$initial"
  [ ! -e "$REPO/.update-lock" ] || { echo "FAIL: $mode kept the lock" >&2; exit 1; }
done
echo 'ok: a Redis that stops after the 0.5.0 read is refused before anything stops'

# 15. Every mode warns while the running Redis may have the 0.5.0 ACL and
# nothing was rewritten: a failed forward rewrite, a usage error (a mistyped
# flag), and an unreadable users.acl with its own wording.
set_topology proxy; write_env "$OLD_M" "$OLD_F" "$OLD_R"
export FAKE_RUNNING_ACL=old FAKE_FAIL=bgrewrite_refused
if bash "$SCRIPT" "${rb[@]}" --ack-redis-aof-backup > "$WORK/out" 2>&1; then echo 'FAIL: rollback ran after a refused rewrite' >&2; exit 1; fi
grep -q 'Do NOT reboot the NAS or restart Redis until a rewrite succeeds' "$WORK/out" || { cat "$WORK/out"; echo 'FAIL: a failed forward rewrite did not warn' >&2; exit 1; }
export FAKE_FAIL=""
: > "$FAKE_LOG"
if bash "$SCRIPT" "${rb[@]}" --ack-redis-aof-backpu > "$WORK/out" 2>&1; then echo 'FAIL: accepted a mistyped flag' >&2; exit 1; fi
grep -q '^usage:' "$WORK/out" && grep -q 'Do NOT reboot the NAS or restart Redis until a rewrite succeeds' "$WORK/out" || { cat "$WORK/out"; echo 'FAIL: a usage error did not warn' >&2; exit 1; }
# The bad flag before --project: the early scan still finds the project.
if bash "$SCRIPT" --bogus --project testproject --repo "$REPO" --proxy-rollback > "$WORK/out" 2>&1; then echo 'FAIL: accepted --bogus' >&2; exit 1; fi
grep -q 'Do NOT reboot the NAS or restart Redis until a rewrite succeeds' "$WORK/out" || { cat "$WORK/out"; echo 'FAIL: a bad flag before --project did not warn' >&2; exit 1; }
! grep -qvE '^docker ps |^docker exec redis-id cat /run/jm-redis/users.acl$' "$FAKE_LOG" || { cat "$FAKE_LOG"; echo 'FAIL: a usage error did more than read the ACL' >&2; exit 1; }
if bash "$SCRIPT" --project testproject --repo "$REPO" --proxy-rollback --manager-digest sha256:bad > "$WORK/out" 2>&1; then echo 'FAIL: accepted digests in a topology mode' >&2; exit 1; fi
grep -q 'do not pass digests' "$WORK/out" && grep -q 'Do NOT reboot' "$WORK/out" || { cat "$WORK/out"; echo 'FAIL: a mode/digest mismatch did not warn' >&2; exit 1; }
export FAKE_RUNNING_ACL=unreadable
if bash "$SCRIPT" "${rb[@]}" --ack-redis-aof-backpu > "$WORK/out" 2>&1; then echo 'FAIL: accepted a mistyped flag' >&2; exit 1; fi
grep -q 'WARNING: the running Redis may still have the 0.5.0 ACL (could not read users.acl)' "$WORK/out" || { cat "$WORK/out"; echo 'FAIL: unreadable users.acl worded as certain' >&2; exit 1; }
echo 'ok: every mode, usage errors included, warns while Redis may have the 0.5.0 ACL and nothing was rewritten'
