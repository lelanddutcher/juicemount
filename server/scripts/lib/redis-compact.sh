# shellcheck shell=bash
# Compact the metadata Redis's append-only file (AOF) before anything stops
# or recreates Redis. Source this file; it defines functions only.
#
# Redis rebuilds its data at every start by replaying the AOF. Under the
# 0.5.0 ACL (`user default off ... -@all`), Redis 7.4 refuses every replayed
# MULTI/EXEC and multi-write script, so a restart loses JuiceFS metadata.
# On a server that already had such a lossy start, the first start under the
# fixed ACL replays those refused transactions over newer writes and corrupts
# the folder tree. BGREWRITEAOF under the still-running Redis replaces the
# AOF with what Redis holds now, so the next start replays nothing old.
#
#   jm_redis_before_restart PROJECT ACL_FILE ALLOW_REPLAY
#     Call immediately before a script stops or recreates Redis. Returns 0
#     when it is safe to go on, 1 when the caller must stop without touching
#     anything. ACL_FILE is the template the next Redis start renders (empty:
#     that Redis runs without an ACL file). ALLOW_REPLAY=1 is the operator's
#     explicit override for the one refusal below.
#
#     Redis running: BGREWRITEAOF as jm-admin (no login on a Redis without an
#       ACL file), then wait until INFO persistence shows
#       aof_rewrite_scheduled:0, aof_rewrite_in_progress:0,
#       aof_last_bgrewrite_status:ok and a higher aof_rewrites count.
#       Any error or JM_REDIS_COMPACT_TIMEOUT seconds (default 600): fail.
#     Redis not running: nothing can be compacted, so say that the next start
#       replays the whole AOF. Refuse when ACL_FILE would still refuse
#       replayed transactions, unless ALLOW_REPLAY=1.

jm_redis_compact_log() { printf 'redis-compact: %s\n' "$*" >&2; }

# The running redis container of a Compose project. Prints its ID. Returns 3
# when no redis container of the project is running.
jm_redis_find() { # project
  local ids n
  ids="$(docker ps -q --no-trunc --filter "label=com.docker.compose.project=$1" \
    --filter label=com.docker.compose.service=redis --filter status=running)" || {
    jm_redis_compact_log "cannot list the containers of Compose project $1"; return 1;
  }
  n="$(printf '%s\n' "$ids" | sed '/^$/d' | wc -l | tr -d ' ')"
  case "$n" in
    0) return 3 ;;
    1) printf '%s\n' "$ids" ;;
    *) jm_redis_compact_log "Compose project $1 runs $n redis containers; expected one"; return 1 ;;
  esac
}

# redis-cli inside the container. The jm-admin secret is read inside the
# container and passed in the environment, never in argv. A Redis started
# without /run/jm-redis/admin.pass (the pre-proxy compose) has no ACL file
# and takes no login.
jm_redis_cli() { # container, redis-cli args...
  local c="$1"
  shift
  docker exec "$c" sh -c '
    if [ -f /run/jm-redis/admin.pass ]; then
      REDISCLI_AUTH="$(cat /run/jm-redis/admin.pass)" || exit 1
      export REDISCLI_AUTH
      exec redis-cli --no-auth-warning --user jm-admin "$@"
    fi
    exec redis-cli "$@"' jm-redis-cli "$@"
}

jm_redis_info_field() { # info text, field
  printf '%s\n' "$1" | tr -d '\r' | sed -n "s/^$2://p" | head -1
}

# Rewrite the AOF of the running Redis and wait for the rewrite to finish.
# Returns 0 when it finished, 3 when Redis is not running, 1 on any failure.
jm_redis_compact() { # project
  local project="$1" c rc info reply before failures_before sched prog status count failures i
  local timeout="${JM_REDIS_COMPACT_TIMEOUT:-600}"
  [[ "$timeout" =~ ^[1-9][0-9]*$ ]] || { jm_redis_compact_log "JM_REDIS_COMPACT_TIMEOUT must be a whole number of seconds"; return 1; }
  c="$(jm_redis_find "$project")" || { rc=$?; return "$rc"; }
  info="$(jm_redis_cli "$c" INFO persistence)" || { jm_redis_compact_log "cannot read INFO persistence from Redis $c as jm-admin"; return 1; }
  before="$(jm_redis_info_field "$info" aof_rewrites)"
  failures_before="$(jm_redis_info_field "$info" aof_rewrites_consecutive_failures)"
  [[ "$before" =~ ^[0-9]+$ ]] || { jm_redis_compact_log "Redis $c did not report aof_rewrites in INFO persistence"; return 1; }
  [[ "$failures_before" =~ ^[0-9]+$ ]] || failures_before=0
  reply="$(jm_redis_cli "$c" BGREWRITEAOF 2>&1)" || true
  case "$reply" in
    *'rewriting started'*|*'rewriting scheduled'*|*'already in progress'*) ;;
    *)
      jm_redis_compact_log "Redis refused BGREWRITEAOF: $(printf '%s' "$reply" | tr -d '\r' | head -1)"
      return 1 ;;
  esac
  for ((i = 0; i <= timeout; i++)); do
    [ "$i" = 0 ] || sleep 1
    info="$(jm_redis_cli "$c" INFO persistence)" || continue
    sched="$(jm_redis_info_field "$info" aof_rewrite_scheduled)"
    prog="$(jm_redis_info_field "$info" aof_rewrite_in_progress)"
    status="$(jm_redis_info_field "$info" aof_last_bgrewrite_status)"
    count="$(jm_redis_info_field "$info" aof_rewrites)"
    failures="$(jm_redis_info_field "$info" aof_rewrites_consecutive_failures)"
    [ "$sched" = 0 ] && [ "$prog" = 0 ] || continue
    if [ "$status" = ok ] && [[ "$count" =~ ^[0-9]+$ ]] && [ "$count" -gt "$before" ]; then
      jm_redis_compact_log "AOF rewritten (aof_rewrite_scheduled:0, aof_rewrite_in_progress:0, aof_last_bgrewrite_status:ok)"
      return 0
    fi
    if [ "$status" = err ] && [[ "$failures" =~ ^[0-9]+$ ]] && [ "$failures" -gt "$failures_before" ]; then
      jm_redis_compact_log "the AOF rewrite failed (aof_last_bgrewrite_status:err); see the Redis log"
      return 1
    fi
  done
  jm_redis_compact_log "the AOF rewrite did not finish within ${timeout} s (aof_rewrite_scheduled:${sched:-?}, aof_rewrite_in_progress:${prog:-?}, aof_last_bgrewrite_status:${status:-?}); raise JM_REDIS_COMPACT_TIMEOUT for a large keyspace"
  return 1
}

# 0 when the ACL file lets Redis replay transactions from the AOF: no
# `user default` line, or one whose final rules hold every command (+@all or
# allcommands, with no later minus rule) and every key (~* or allkeys). The
# 0.5.0 line `user default off resetpass resetkeys resetchannels -@all`
# fails. A narrower grant also fails here, on purpose: refusing is the safe
# side.
jm_redis_acl_replay_safe() { # acl file
  [ -f "$1" ] || { jm_redis_compact_log "ACL file $1 is missing"; return 1; }
  awk '
    /^[[:space:]]*#/ { next }
    $1 == "user" && $2 == "default" {
      cmds = 0; keys = 0
      for (i = 3; i <= NF; i++) {
        t = $i
        if (t == "reset") { cmds = 0; keys = 0 }
        else if (t == "+@all" || t == "allcommands") cmds = 1
        else if (t == "nocommands" || t ~ /^-/) cmds = 0
        else if (t == "~*" || t == "allkeys") keys = 1
        else if (t == "resetkeys") keys = 0
      }
      if (!cmds || !keys) bad = 1
    }
    END { exit bad }
  ' "$1"
}

jm_redis_before_restart() { # project, acl file (may be empty), allow replay (0|1)
  local project="$1" acl="$2" allow="${3:-0}" rc
  jm_redis_compact "$project" && return 0
  rc=$?
  if [ "$rc" != 3 ]; then
    jm_redis_compact_log "refusing to stop or recreate Redis: the AOF was not compacted. Nothing was stopped."
    return 1
  fi
  jm_redis_compact_log "Redis of project $project is not running, so its AOF was not compacted. Its next start replays the whole AOF, including any transactions an earlier start refused."
  [ -n "$acl" ] || return 0
  jm_redis_acl_replay_safe "$acl" && return 0
  if [ "$allow" = 1 ]; then
    jm_redis_compact_log "WARNING: $acl still has the 0.5.0 'user default ... -@all' line, so this start drops every replayed transaction. Starting anyway (--allow-aof-replay)."
    return 0
  fi
  jm_redis_compact_log "refusing to start Redis: $acl still has the 0.5.0 'user default ... -@all' line, so Redis would drop every replayed transaction (JuiceFS metadata). Install the fixed server/redis.acl first (CHANGELOG 0.5.1), or pass --allow-aof-replay to accept that loss."
  return 1
}
