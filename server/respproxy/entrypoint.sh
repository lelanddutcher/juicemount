#!/bin/sh
# jm-respproxy container entrypoint (H-8B L5).
#
# 1. Renders the proxy principals file /etc/jm-respproxy/principals.json:
#    the server principals (farm, manager, bg, webdav, admin) are hashed
#    from their JM_PROXY_*_PASSWORD secrets at every start; the Mac
#    principals the Manager wrote there are kept verbatim, and so are the
#    format version, people (person grants) and restrict_unassigned of a
#    version-2 file. The Manager and this script serialize on
#    .principals.lock and both replace the file atomically with mode 0600.
# 2. Writes the proxy config to /run/jm-respproxy/config.json (container
#    local: the shared directory cannot redirect the upstream). The
#    per-person audit log goes to $DIR/audit (JM_RESPPROXY_AUDIT_DIR),
#    which the Manager reads, with its health file beside it
#    ($DIR/audit.health). JM_AUDIT: "auto" (default) starts logging when
#    the first person exists, "on" logs from the start, "off" never.
#    JM_AUDIT_RETAIN_MB / JM_AUDIT_RETAIN_DAYS bound its size and age
#    (default 2048 MB / 400 days; -1 = no limit): the oldest segments go.
#    smart_folder_roots names the smart-folder roots the Manager runs, from
#    the variables the Manager reads, read the way it reads them: none
#    unless JM_SMART_FOLDERS=on; then JM_SMART_FOLDERS_ROOT (default
#    Recents), and with JM_CONTINUITY=on also
#    JM_SMART_FOLDERS_COLLECTIONS_ROOT (default Collections; left out when
#    it is the Recents root). The manager principal may remove an empty
#    view folder it created directly under one, and nothing else at
#    project depth. The proxy leaves out an invalid name.
# 3. Execs jm-respproxy with the applied-reload handshake enabled. The
#    proxy owns polling, exact-byte validation, session revocation, applied
#    receipts and heartbeat. A successful signal alone is never an ACK.
#    Changing this contract requires bumping server/respproxy/min-gate-commit.
#
# Secrets reach `jm-respproxy hash` on stdin only, never argv or logs.
set -eu
umask 077

DIR="${JM_RESPPROXY_DIR:-/etc/jm-respproxy}"
RUN_DIR="${JM_RESPPROXY_RUN_DIR:-/run/jm-respproxy}"
PRINCIPALS="$DIR/principals.json"
LOCK="$DIR/.principals.lock"
CONFIG="$RUN_DIR/config.json"
MODE="${JM_RESPPROXY_MODE:-passthrough}"
STAGE_BIRTH="${JM_RESPPROXY_STAGE_BIRTH:-false}"
UPSTREAM="${JM_RESPPROXY_UPSTREAM:-redis:6379}"
LISTEN="${JM_RESPPROXY_LISTEN:-0.0.0.0:6379}"
STATS="${JM_RESPPROXY_STATS:-127.0.0.1:9180}"
PROXY_BIN="${JM_RESPPROXY_BIN:-jm-respproxy}"
AUDIT_DIR="${JM_RESPPROXY_AUDIT_DIR:-$DIR/audit}"
AUDIT="${JM_AUDIT:-auto}"
AUDIT_RETAIN_MB="${JM_AUDIT_RETAIN_MB:-0}"
AUDIT_RETAIN_DAYS="${JM_AUDIT_RETAIN_DAYS:-0}"
[ "$AUDIT_DIR" != off ] || AUDIT=off

log() { printf '[jm-respproxy-entrypoint] %s\n' "$*" >&2; }
die() { log "$*"; exit 2; }

trim() { printf '%s' "$1" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'; }
switch_on() { # the Manager's reading of an on/off variable
  case "$(trim "$1" | tr '[:upper:]' '[:lower:]')" in on|1|true|yes) return 0 ;; esac
  return 1
}
smart_roots() { # JSON array of the smart-folder roots the Manager runs
  switch_on "${JM_SMART_FOLDERS:-}" || { echo '[]'; return; }
  r="$(trim "${JM_SMART_FOLDERS_ROOT:-}")"; [ -n "$r" ] || r=Recents
  c=""
  if switch_on "${JM_CONTINUITY:-}"; then
    c="$(trim "${JM_SMART_FOLDERS_COLLECTIONS_ROOT:-}")"; [ -n "$c" ] || c=Collections
  fi
  jq -cn --arg r "$r" --arg c "$c" '[$r] + (if $c == "" or ($c | ascii_downcase) == ($r | ascii_downcase) then [] else [$c] end)'
}

case "$MODE" in passthrough|observe|enforce) ;; *) die "JM_RESPPROXY_MODE must be passthrough, observe or enforce" ;; esac
case "$AUDIT" in auto|on) ;; off) AUDIT_DIR="" ;; *) die "JM_AUDIT must be auto, on or off" ;; esac
for v in "$AUDIT_RETAIN_MB" "$AUDIT_RETAIN_DAYS"; do
  case "$v" in -1|0|[1-9]|[1-9][0-9]|[1-9][0-9][0-9]|[1-9][0-9][0-9][0-9]|[1-9][0-9][0-9][0-9][0-9]|[1-9][0-9][0-9][0-9][0-9][0-9]) ;;
  *) die "JM_AUDIT_RETAIN_MB and JM_AUDIT_RETAIN_DAYS must be -1 (no limit), 0 (default) or a whole number up to 999999" ;; esac
done
case "$STAGE_BIRTH" in true|false) ;; *) die "JM_RESPPROXY_STAGE_BIRTH must be true or false" ;; esac
[ "$STAGE_BIRTH" != true ] || [ "$MODE" = enforce ] ||
  die "JM_RESPPROXY_STAGE_BIRTH=true requires JM_RESPPROXY_MODE=enforce"

check_secret() { # name
  eval "v=\${$1:-}"
  case "$v" in ''|*[!A-Za-z0-9._~-]*) die "$1 must be set to a URL-safe value (openssl rand -hex 32)" ;; esac
  [ "${#v}" -ge 32 ] || die "$1 is shorter than 32 characters"
}
check_secret JM_RESPPROXY_UPSTREAM_PASSWORD
for k in FARM MANAGER BG WEBDAV ADMIN; do check_secret "JM_PROXY_${k}_PASSWORD"; done

mkdir -p "$DIR" "$RUN_DIR"
chmod 0700 "$DIR" 2>/dev/null || true

hash_secret() { # env-var name -> PHC argon2id on stdout
  eval "printf '%s\n' \"\$$1\"" | "$PROXY_BIN" hash
}

render_principals() {
  server="[]"
  for spec in farm:farm:FARM manager:manager:MANAGER bg:bg:BG webdav:webdav:WEBDAV admin:admin:ADMIN; do
    name="${spec%%:*}"; rest="${spec#*:}"; kind="${rest%%:*}"; var="JM_PROXY_${rest#*:}_PASSWORD"
    h="$(hash_secret "$var")" || die "hashing the $name secret failed"
    server="$(printf '%s' "$server" | jq -c --arg n "$name" --arg k "$kind" --arg h "$h" '. + [{name:$n, kind:$k, argon2id:$h}]')"
  done
  (
    flock -x 9
    macs="[]"
    # Keep the people (person grants) and restrict_unassigned of a version-2
    # file: dropping them here would lift every grant until the Manager next
    # writes the file.
    rest="{}"
    if [ -s "$PRINCIPALS" ]; then
      macs="$(jq -c '[.principals[]? | select(.kind == "mac")]' "$PRINCIPALS")" ||
        die "existing $PRINCIPALS is not valid JSON; refusing to drop enrolled Macs"
      rest="$(jq -c '{version, people, restrict_unassigned} | with_entries(select(.value != null))' "$PRINCIPALS")" ||
        die "existing $PRINCIPALS is not valid JSON; refusing to drop person grants"
    fi
    tmp="$(mktemp "$DIR/.principals.json.tmp-XXXXXX")"
    if ! jq -n --argjson s "$server" --argjson m "$macs" --argjson r "$rest" '$r + {principals: ($s + $m)}' > "$tmp"; then
      rm -f "$tmp"; die "rendering principals failed"
    fi
    chmod 0600 "$tmp"
    mv -f "$tmp" "$PRINCIPALS"
    log "principals: 5 server + $(printf '%s' "$macs" | jq 'length') enrolled Mac(s)"
  ) 9>>"$LOCK"
}

render_config() {
  jq -n --arg listen "$LISTEN" --arg up "$UPSTREAM" --arg stats "$STATS" --arg p "$PRINCIPALS" --arg mode "$MODE" --argjson stage_birth "$STAGE_BIRTH" \
    --arg audit "$AUDIT_DIR" --arg audit_mode "$AUDIT" --argjson retain_mb "$AUDIT_RETAIN_MB" --argjson retain_days "$AUDIT_RETAIN_DAYS" \
    --argjson smart_roots "$(smart_roots)" '{
    listen: $listen, upstream: $up, upstream_user: "jm-proxy",
    upstream_password_env: "JM_RESPPROXY_UPSTREAM_PASSWORD",
    stats: $stats, principals: $p, mode: $mode, stage_birth: $stage_birth, reload_handshake: true,
    audit_dir: $audit, audit: $audit_mode, audit_retain_mb: $retain_mb, audit_retain_days: $retain_days,
    smart_folder_roots: $smart_roots }' > "$CONFIG.tmp"
  mv -f "$CONFIG.tmp" "$CONFIG"
}

render_principals
render_config
# The proxy needs only its upstream secret; drop the principal secrets.
unset JM_PROXY_FARM_PASSWORD JM_PROXY_MANAGER_PASSWORD JM_PROXY_BG_PASSWORD JM_PROXY_WEBDAV_PASSWORD JM_PROXY_ADMIN_PASSWORD

# Keep one original process identity and let the proxy join its shutdown.
exec "$PROXY_BIN" -config "$CONFIG"
