#!/usr/bin/env bash
# lab.sh — start, configure and stop the JuiceMount Link lab.
#
#   lab.sh up                  make the lab CA and secrets, build, start,
#                              route the Manager, wait for Link, seed S3
#   lab.sh case NAME           apply a case from cases.tsv (router switches,
#                              NAS networking); prints the case as JSON
#   lab.sh shape PROFILE       apply a remote-lab profile to the wan
#   lab.sh pair                mint a one-off pairing code (prints it)
#   lab.sh probe SITE ARGS...  run lib/probe.py in home-mac or away-mac
#                              (SITE: home|away); a pairing code is minted
#                              for "connect"
#   lab.sh restart SERVICE     restart a container (home-router, manager,
#                              link-node...)
#   lab.sh uplink SITE up|down take a site's internet link down or bring it
#                              back (SITE: home|away)
#   lab.sh wait-link           wait until Link is ready on the Manager
#   lab.sh relay-cpu           the relay's CPU time so far, microseconds
#   lab.sh exec SERVICE CMD... run a command in a lab container
#   lab.sh status              containers, router rules, wan qdiscs
#   lab.sh down                remove every container, network and volume of
#                              the lab, and its .state directory
#
# Everything lives in Docker project "${JMLINK_PROJECT:-jmlinklab}". The
# secrets and the lab CA are throwaway, in .state/ (git-ignored, mode 700).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE="$HERE/.state"
ENV_FILE="$STATE/lab.env"
export JMLINK_PROJECT="${JMLINK_PROJECT:-jmlinklab}"
SERVER_URL="https://link.lab.example:30193"

log() { printf '[link-lab] %s\n' "$*" >&2; }
die() { log "$*"; exit 2; }

# Where the NAS node runs (cases.tsv nas_net): host (the node container on
# the NAS host's network, JM_NET_NAS_NODE=container) or bridge (in the
# Manager, JM_NET_NAS_NODE=on).
nas_mode() { cat "$STATE/nas_net" 2>/dev/null || echo host; }

dc() {
  [ -f "$ENV_FILE" ] || die "the lab is not set up: run lab.sh up"
  local node=container
  [ "$(nas_mode)" = host ] || node=on
  JMLINK_COMMIT="$(git -C "$HERE" rev-parse HEAD 2>/dev/null || printf '%040d' 0)" JMLINK_NAS_NODE="$node" \
    docker compose -f "$HERE/compose.yml" --env-file "$ENV_FILE" "$@"
}
cid() { local c; c="$(dc ps -q "$1")"; [ -n "$c" ] || die "service $1 is not running"; printf '%s' "$c"; }
env_get() { sed -n "s/^$1=//p" "$ENV_FILE"; }

gen_env() {
  [ -f "$ENV_FILE" ] && return 0
  mkdir -p "$STATE"
  chmod 700 "$STATE"
  local f k
  f="$(mktemp "$STATE/lab.env.XXXXXX")"
  {
    for k in JM_REDIS_PROXY_PASSWORD JM_PROXY_FARM_PASSWORD JM_PROXY_MANAGER_PASSWORD \
             JM_PROXY_BG_PASSWORD JM_PROXY_WEBDAV_PASSWORD JM_PROXY_ADMIN_PASSWORD \
             JMLINK_S3_PASSWORD JMLINK_ADMIN_KEY; do
      printf '%s=%s\n' "$k" "$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
    done
    printf 'JMLINK_S3_USER=jmlinklab\n'
  } > "$f"
  chmod 600 "$f"
  mv "$f" "$ENV_FILE"
}

# The lab CA and two server certificates (Headscale's control URL and the
# relay), made with openssl in the tools image as the calling user, so the
# files stay removable. The clients trust ca.crt; nothing else does.
gen_pki() {
  [ -f "$STATE/pki/ca.crt" ] && return 0
  mkdir -p "$STATE/pki"
  # shellcheck disable=SC2016
  docker run --rm --user "$(id -u):$(id -g)" -v "$STATE/pki:/pki" -w /pki --entrypoint /bin/sh jmlinklab:local -ec '
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 30 \
      -keyout ca.key -out ca.crt -subj "/CN=JuiceMount Link lab CA" \
      -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
    for name in link.lab.example derp.lab.example; do
      openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
        -keyout "$name.key" -out "$name.csr" -subj "/CN=$name" 2>/dev/null
      printf "subjectAltName=DNS:%s\nextendedKeyUsage=serverAuth\nbasicConstraints=CA:FALSE\n" "$name" > ext
      openssl x509 -req -in "$name.csr" -CA ca.crt -CAkey ca.key -CAcreateserial -days 30 \
        -extfile ext -out "$name.crt" 2>/dev/null
      rm -f "$name.csr" ext
    done
    rm -f ca.key ca.srl
    chmod 644 *.crt; chmod 600 *.key'
}

# The Manager's default route (via nas-host, the NAS's Docker NAT), set from
# a throwaway helper in its network namespace so the Manager itself needs no
# NET_ADMIN. Again after each Manager (re)start.
manager_route() {
  docker run --rm --network "container:$(cid manager)" --cap-add NET_ADMIN --entrypoint /bin/sh \
    jmlinklab:local -ec '
      for d in /sys/class/net/*; do d=${d##*/}; [ "$d" = lo ] || ethtool -K "$d" tso off gso off gro off >/dev/null 2>&1 || true; done
      ip route replace default via 10.232.11.2'
}

# Where the NAS node runs:
#   host    the node container (link-node) in nas-host's namespace: on the
#           home LAN at the NAS's address, as host networking on a NAS
#   bridge  the node in the Manager, behind the NAS's Docker NAT (the layout
#           before the node container)
# A change recreates the Manager (JM_NET_NAS_NODE) and waits for Link. The
# node container is stopped before the in-Manager node starts: the two hold
# the same identity and must not run at once.
nas_net() {
  local mode=$1 was
  was="$(nas_mode)"
  printf '%s\n' "$mode" > "$STATE/nas_net"
  router_conf nas-host "NAS_NODE=$mode"
  if [ "$mode" = host ]; then
    dc up -d --no-deps manager link-node >&2
  else
    dc stop link-node >&2
    dc up -d --no-deps manager >&2
  fi
  manager_route
  if [ "$was" = "$mode" ] && [ "$mode" = host ]; then
    # A fresh node per case, so a port mapping or endpoint from the previous
    # case's router does not carry over.
    docker restart -t 2 "$(cid link-node)" >/dev/null
  fi
  wait_link
}

# NAS-side containers send through routers that shape and drop: turn off
# their segmentation offloads so the WAN sees real packets (as remote-lab).
nas_offloads_off() {
  local svc
  for svc in respproxy silo; do
    docker run --rm --network "container:$(cid "$svc")" --cap-add NET_ADMIN --entrypoint /bin/sh jmlinklab:local \
      -c 'ethtool -K eth0 tso off gso off gro off >/dev/null 2>&1 || true'
  done
}

manager_api() { # manager_api METHOD PATH
  printf 'header = "X-JuiceMount-Admin-Key: %s"\n' "$(env_get JMLINK_ADMIN_KEY)" |
    docker exec -i "$(cid manager)" curl --config - -fsS -X "$1" "http://127.0.0.1:8080$2"
}

wait_link() {
  local out=""
  for _ in $(seq 120); do
    out="$(manager_api GET /api/net/link 2>/dev/null || true)"
    if printf '%s' "$out" | grep -Eq '"data_plane_ready"[[:space:]]*:[[:space:]]*true'; then return 0; fi
    sleep 1
  done
  printf '%s\n' "$out" >&2
  dc logs --tail 60 manager >&2
  die "Link did not become ready on the Manager"
}

# The probe objects in SILO, written from nas-host on the NAS network.
seed_s3() {
  # shellcheck disable=SC2016
  LINK_S3_USER="$(env_get JMLINK_S3_USER)" LINK_S3_PASS="$(env_get JMLINK_S3_PASSWORD)" \
    docker exec -e LINK_S3_USER -e LINK_S3_PASS "$(cid nas-host)" sh -ec '
      s3() { curl -sS --fail --aws-sigv4 aws:amz:us-east-1:s3 --user "$LINK_S3_USER:$LINK_S3_PASS" "$@"; }
      for _ in $(seq 60); do curl -fsS -o /dev/null http://10.232.11.30:9000/minio/health/live && break; sleep 1; done
      s3 -X PUT http://10.232.11.30:9000/linklab >/dev/null 2>&1 || true
      head -c 67108864 /dev/urandom > /var/lib/get-64m.bin
      s3 -T /var/lib/get-64m.bin http://10.232.11.30:9000/linklab/get-64m.bin >/dev/null
      rm -f /var/lib/get-64m.bin'
}

router_conf() { # router_conf SERVICE CONF-TEXT
  printf '%s\n' "$2" | docker exec -i "$(cid "$1")" sh -c 'mkdir -p /etc/lab && cat > /etc/lab/router.conf'
  docker exec "$(cid "$1")" /lab/lib/router.sh apply
}

# cases.tsv columns: name site home_nat fwd_ctrl fwd_udp hairpin split_dns
#                    away_nat cgnat nas_net portmap expect
apply_case() {
  local line
  line="$(awk -v c="$1" '$1 == c { print; exit }' "$HERE/cases.tsv")"
  [ -n "$line" ] || die "unknown case '$1' (see cases.tsv)"
  # shellcheck disable=SC2086
  set -- $line
  local name=$1 site=$2 home_nat=$3 fwd_ctrl=$4 fwd_udp=$5 hairpin=$6 split=$7 \
        away_nat=$8 cgnat=$9 nasnet=${10} portmap=${11} expect=${12}
  local nas_ip=10.232.10.5 tcp="" udp="" dns=""
  [ "$fwd_ctrl" = off ] || tcp="30193>$nas_ip:30193"
  [ "$fwd_udp" = off ] || udp="41641>$nas_ip:41641"
  [ "$split" = off ] || dns="link.lab.example/$nas_ip"
  router_conf home-router "NAT=$home_nat
FWD_TCP=\"$tcp\"
FWD_UDP=\"$udp\"
HAIRPIN=$hairpin
SPLIT_DNS=\"$dns\"
PORTMAP=$portmap"
  local uplink=direct cgn_nat=hard
  if [ "$cgnat" != off ]; then uplink=carrier cgn_nat=$cgnat; fi
  router_conf away-router "NAT=$away_nat
UPLINK=$uplink"
  router_conf cgn "NAT=$cgn_nat"
  # The NAS networking last: a port mapping the NAS node asks for must come
  # from this case's router, not the previous one's.
  nas_net "$nasnet"
  printf '%s\n' "$name" > "$STATE/case"
  printf '{"case":"%s","site":"%s","home_nat":"%s","fwd_ctrl":"%s","fwd_udp":"%s","hairpin":"%s","split_dns":"%s","away_nat":"%s","cgnat":"%s","nas_net":"%s","portmap":"%s","expect":"%s"}\n' \
    "$name" "$site" "$home_nat" "$fwd_ctrl" "$fwd_udp" "$hairpin" "$split" "$away_nat" "$cgnat" "$nasnet" "$portmap" "$expect"
}

pair() {
  local out
  out="$(manager_api POST "/api/net/pair?device=link-lab")" || die "pairing request failed"
  printf '%s' "$out" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["code"]) if d.get("ok") else sys.exit(1)' \
    || die "the Manager refused to pair: $(printf '%s' "$out" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("error",""))')"
}

probe() {
  local site=$1; shift
  case "$site" in home|away) ;; *) die "probe SITE must be home or away" ;; esac
  local code=""
  [ "${1:-}" != connect ] || code="$(pair)"
  LINK_CODE="$code" LINK_REDIS_USER=admin LINK_REDIS_PASS="$(env_get JM_PROXY_ADMIN_PASSWORD)" \
  LINK_S3_USER="$(env_get JMLINK_S3_USER)" LINK_S3_PASS="$(env_get JMLINK_S3_PASSWORD)" \
    docker exec -e LINK_CODE -e LINK_REDIS_USER -e LINK_REDIS_PASS -e LINK_S3_USER -e LINK_S3_PASS \
      "$(cid "$site-mac")" python3 /lab/lib/probe.py "$@"
}

cmd="${1:-}"; shift || true
case "$cmd" in
  up)
    command -v docker >/dev/null || die "docker is required"
    command -v python3 >/dev/null || die "python3 is required"
    gen_env
    log "building images (project $JMLINK_PROJECT)"
    dc build --quiet >&2
    gen_pki
    log "starting the lab"
    # The node container last (nas_net): it is healthy only once Link runs,
    # which needs the Manager's route.
    printf 'host\n' > "$STATE/nas_net"
    # shellcheck disable=SC2046
    dc up -d --wait --quiet-pull $(dc config --services | grep -vx link-node) >&2
    manager_route
    nas_net host
    nas_offloads_off
    wait_link
    seed_s3
    log "up: Link is ready; control URL $SERVER_URL"
    ;;
  case) [ $# -eq 1 ] || die "usage: lab.sh case NAME"; apply_case "$1" ;;
  shape) [ $# -eq 1 ] || die "usage: lab.sh shape PROFILE"; docker exec "$(cid wan)" /remote-lab/lib/shape.sh "$1" ;;
  pair) pair ;;
  probe) [ $# -ge 2 ] || die "usage: lab.sh probe SITE ARGS..."; probe "$@" ;;
  restart)
    [ $# -eq 1 ] || die "usage: lab.sh restart SERVICE"
    docker restart -t 2 "$(cid "$1")" >/dev/null
    if [ "$1" = manager ]; then manager_route; fi
    ;;
  uplink)
    [ $# -eq 2 ] || die "usage: lab.sh uplink home|away up|down"
    case "$1" in home|away) ;; *) die "uplink SITE must be home or away" ;; esac
    case "$2" in
      down) docker exec "$(cid "$1-router")" /lab/lib/router.sh uplink-down ;;
      up) docker exec "$(cid "$1-router")" /lab/lib/router.sh apply ;;
      *) die "uplink STATE must be up or down" ;;
    esac
    ;;
  wait-link) wait_link ;;
  relay-cpu) docker exec "$(cid relay)" awk '$1 == "usage_usec" { print $2 }' /sys/fs/cgroup/cpu.stat ;;
  exec) [ $# -ge 2 ] || die "usage: lab.sh exec SERVICE CMD..."; s=$1; shift; docker exec "$(cid "$s")" "$@" ;;
  status)
    dc ps
    echo "NAS node: $(nas_mode)"
    for r in nas-host home-router away-router cgn; do
      echo "== $r"; docker exec "$(cid "$r")" iptables -t nat -S
    done
    docker exec "$(cid wan)" tc qdisc show
    ;;
  down)
    if [ -f "$ENV_FILE" ]; then dc down --volumes --remove-orphans --timeout 3 >&2; fi
    rm -rf "$STATE"
    log "down: project $JMLINK_PROJECT removed"
    ;;
  *) sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
