# shellcheck shell=bash
# Rewrite (compact) the metadata Redis's append-only file (AOF) before
# anything stops or recreates Redis, or before a release leaves a fixed ACL
# staged for its next start. Source this file; it defines functions only.
#
# Redis rebuilds its data at every start by replaying the AOF. Under the
# 0.5.0 ACL (`user default off ... -@all`), Redis 7.4 refuses every replayed
# MULTI/EXEC and multi-write script, so a restart loses JuiceFS metadata.
# The refused transactions stay in the AOF. The first start under the fixed
# ACL replays them over newer writes and corrupts the folder tree.
# BGREWRITEAOF under the running Redis replaces the AOF with what Redis
# holds now, so the next start replays nothing old. The rewrite also deletes
# the old AOF files, the only copy of the changes lost at that earlier start
# (CHANGELOG 0.5.1, step 2: copy appendonlydir first).
#
# JM_REDIS_WORKING_DIR (optional): only a redis container whose Compose
# working directory label is this path counts, so a second checkout or a
# mistyped project name never touches another deployment's Redis.
# JM_REDIS_REWROTE is set to 1 by every rewrite that really finished in this
# shell; callers use it, not their own bookkeeping, to say "rewritten".
#
#   jm_redis_before_restart PROJECT ACL_FILE ALLOW_REPLAY ON_FAIL ON_DOWN REASON
#     Call immediately before a script stops or recreates Redis. Returns 0
#     when it may go on, 1 when it must not stop or recreate Redis. It never
#     says what the caller already stopped; the caller does. ACL_FILE is the
#     template the next start renders (empty: no ACL file).
#     Redis running: rewrite. If that fails, ON_FAIL=refuse returns 1;
#       ON_FAIL=warn logs REASON (why going on is safe) and goes on.
#     Redis not running: ON_DOWN=refuse returns 1 (the caller cannot vouch
#       for the AOF); ON_DOWN=start says the start replays the AOF as it is.
#     After a warn or a start: refuse while ACL_FILE still has the 0.5.0
#       default line, unless ALLOW_REPLAY=1 (the operator accepts that the
#       start drops every replayed transaction).
#
#   jm_redis_running_acl_state PROJECT
#     What the running Redis loaded at its start: fixed, old (the 0.5.0
#     default line in /run/jm-redis/users.acl), noacl (Redis runs without an
#     ACL file, as in the pre-proxy compose: it never refused a replayed
#     transaction), unreadable, or down. Returns 1 when it cannot tell.
#
#   jm_redis_compact_if_old_acl PROJECT ACK_BACKUP
#     For a release, which never restarts Redis but leaves the fixed ACL
#     staged for its next start (a NAS reboot included). Returns
#       0  fixed or noacl (no rewrite needed), or the rewrite finished.
#       4  old and ACK_BACKUP is not 1: nothing was done, because the
#          rewrite would delete the only copy of earlier losses.
#       3  Redis is not running.  1  the rewrite failed, or no answer.
#     unreadable is rewritten with no acknowledgement (rewriting is the safe
#     side); JM_REDIS_WORKING_DIR keeps that to this checkout's Redis.

jm_redis_compact_log() { printf 'redis-compact: %s\n' "$*" >&2; }

# The running redis container of a Compose project. Prints its ID. Returns 3
# when no such container is running. With JM_REDIS_WORKING_DIR set, a redis
# container of the same project that runs from another directory is an
# error (1), not "not running": it is another deployment, and nothing here
# may act on it or conclude anything from it.
jm_redis_find() { # project
  local ids n other
  local -a filters=(--filter "label=com.docker.compose.project=$1"
    --filter label=com.docker.compose.service=redis --filter status=running)
  [ -z "${JM_REDIS_WORKING_DIR:-}" ] ||
    filters+=(--filter "label=com.docker.compose.project.working_dir=$JM_REDIS_WORKING_DIR")
  ids="$(docker ps -q --no-trunc "${filters[@]}")" || {
    jm_redis_compact_log "cannot list the containers of Compose project $1"; return 1;
  }
  n="$(printf '%s\n' "$ids" | sed '/^$/d' | wc -l | tr -d ' ')"
  if [ "$n" = 0 ] && [ -n "${JM_REDIS_WORKING_DIR:-}" ]; then
    other="$(docker ps -q --no-trunc "${filters[@]:0:6}" | sed '/^$/d' | head -1)" || other=""
    if [ -n "$other" ]; then
      jm_redis_compact_log "refusing: a redis container of Compose project $1 runs from another directory ($(docker inspect -f '{{ index .Config.Labels "com.docker.compose.project.working_dir" }}' "$other" 2>/dev/null || echo unknown)), not from $JM_REDIS_WORKING_DIR. Check --project and --repo."
      return 1
    fi
  fi
  case "$n" in
    0) return 3 ;;
    1) printf '%s\n' "$ids" ;;
    *) jm_redis_compact_log "Compose project $1 runs $n redis containers; expected one"; return 1 ;;
  esac
}

# The jm-admin login inside the container: the secret is read there and
# passed in the environment, never in argv. A Redis started without
# /run/jm-redis/admin.pass (the pre-proxy compose) has no ACL file and takes
# no login.
_JM_REDIS_LOGIN='
  if [ -f /run/jm-redis/admin.pass ]; then
    REDISCLI_AUTH="$(cat /run/jm-redis/admin.pass)" || exit 1
    export REDISCLI_AUTH
    rcli() { redis-cli --no-auth-warning --user jm-admin "$@"; }
  else
    rcli() { redis-cli "$@"; }
  fi'

jm_redis_cli() { # container, redis-cli args...
  local c="$1"
  shift
  docker exec "$c" sh -c "$_JM_REDIS_LOGIN"'
    rcli "$@"' jm-redis-cli "$@"
}

# INFO persistence plus the AOF base file's sequence number from the
# manifest (jm_base_seq), read in one docker exec so they belong together.
jm_redis_snapshot() { # container
  docker exec "$1" sh -c "$_JM_REDIS_LOGIN"'
    rcli INFO persistence || exit 1
    dir="$(rcli CONFIG GET dir | sed -n 2p)"
    adir="$(rcli CONFIG GET appenddirname | sed -n 2p)"
    afile="$(rcli CONFIG GET appendfilename | sed -n 2p)"
    # Only the live manifest: a crash can leave temp-<name>.manifest behind.
    base="$(cat "$dir/$adir/$afile.manifest" 2>/dev/null |
      awk "{ s = \"\"; for (i = 1; i < NF; i++) if (\$i == \"seq\") s = \$(i + 1); if (\$NF == \"b\") print s }" | tail -1)"
    printf "jm_base_seq:%s\n" "$base"' jm-redis-snapshot
}

jm_redis_info_field() { # info text, field
  printf '%s\n' "$1" | tr -d '\r' | sed -n "s/^$2://p" | head -1
}

# Rewrite the AOF of the running Redis and wait for the rewrite to finish.
# Returns 0 when it finished, 3 when Redis is not running, 1 on any failure.
jm_redis_compact() { # project
  local c rc
  c="$(jm_redis_find "$1")" || { rc=$?; return "$rc"; }
  jm_redis_compact_container "$c"
}

# One snapshot into _jm_sched, _jm_prog, _jm_status, _jm_count, _jm_base.
_jm_redis_poll() { # container
  local info
  info="$(jm_redis_snapshot "$1")" || return 1
  _jm_sched="$(jm_redis_info_field "$info" aof_rewrite_scheduled)"
  _jm_prog="$(jm_redis_info_field "$info" aof_rewrite_in_progress)"
  _jm_status="$(jm_redis_info_field "$info" aof_last_bgrewrite_status)"
  _jm_count="$(jm_redis_info_field "$info" aof_rewrites)"
  _jm_base="$(jm_redis_info_field "$info" jm_base_seq)"
  [[ "$_jm_sched$_jm_prog" =~ ^[01][01]$ && "$_jm_count" =~ ^[0-9]+$ && "$_jm_base" =~ ^[0-9]+$ ]] || {
    jm_redis_compact_log "Redis $1 did not report aof_rewrite_scheduled, aof_rewrite_in_progress, aof_rewrites and an AOF manifest with a base file"
    return 1
  }
}

# How Redis 7.4 reports a rewrite (aof.c): aof_rewrites rises when the
# rewrite forks, not when it ends. aof_rewrite_scheduled clears only on that
# fork. aof_last_bgrewrite_status changes only when a rewrite ends, and a
# rewrite child killed by a signal (SIGUSR1, or CONFIG SET appendonly no)
# leaves it ok. A scheduled BGREWRITEAOF resets
# aof_rewrites_consecutive_failures, so that counter is not used. Only a
# rewrite that replaced the base file removed the old files, so:
#   1. While another rewrite runs (in progress, or a BGREWRITEAOF answered
#      "already in progress"), wait for it to end, then ask again: it may
#      have started before the changes this rewrite must cover.
#   2. Baselines right before our BGREWRITEAOF: aof_rewrites and the base
#      file's sequence number in the AOF manifest.
#   3. Done when aof_rewrite_scheduled:0, aof_rewrite_in_progress:0 and
#      aof_rewrites above its baseline (a rewrite forked after our request
#      and ended). Success needs aof_last_bgrewrite_status:ok AND a new base
#      file; anything else is a failure.
# One deadline of JM_REDIS_COMPACT_TIMEOUT seconds (default 600) covers all.
jm_redis_compact_container() { # running redis container
  local c="$1" reply baseline="" base0="" ticks=0 waited=0
  local timeout="${JM_REDIS_COMPACT_TIMEOUT:-600}"
  [[ "$timeout" =~ ^[1-9][0-9]*$ ]] || { jm_redis_compact_log "JM_REDIS_COMPACT_TIMEOUT must be a whole number of seconds"; return 1; }
  _jm_sched="" _jm_prog="" _jm_status="" _jm_count="" _jm_base=""
  while [ "$ticks" -le "$timeout" ]; do
    [ "$ticks" = 0 ] || sleep 1
    ticks=$((ticks + 1))
    _jm_redis_poll "$c" || continue
    if [ -z "$baseline" ]; then
      if [ "$_jm_prog" = 1 ]; then
        [ "$waited" = 1 ] || jm_redis_compact_log "a rewrite is already running; waiting for it to end, then asking for a new one"
        waited=1
        continue
      fi
      baseline="$_jm_count" base0="$_jm_base"
      reply="$(jm_redis_cli "$c" BGREWRITEAOF 2>&1)" || true
      case "$reply" in
        *'rewriting started'*|*'rewriting scheduled'*) ;;
        *'already in progress'*)
          baseline=""
          [ "$waited" = 1 ] || jm_redis_compact_log "a rewrite started just before ours; waiting for it to end, then asking again"
          waited=1
          continue ;;
        *)
          jm_redis_compact_log "Redis refused BGREWRITEAOF: $(printf '%s' "$reply" | tr -d '\r' | head -1)"
          return 1 ;;
      esac
      continue
    fi
    [ "$_jm_sched" = 0 ] && [ "$_jm_prog" = 0 ] && [ "$_jm_count" -gt "$baseline" ] || continue
    if [ "$_jm_status" = ok ] && [ "$_jm_base" -gt "$base0" ]; then
      # shellcheck disable=SC2034 # read by the scripts that source this file
      JM_REDIS_REWROTE=1
      jm_redis_compact_log "AOF rewritten (aof_rewrite_scheduled:0, aof_rewrite_in_progress:0, aof_last_bgrewrite_status:ok, base file $base0 -> $_jm_base)"
      return 0
    fi
    if [ "$_jm_status" = ok ]; then
      jm_redis_compact_log "the AOF rewrite ended without a new base file (still $base0): its child was stopped (a signal, or CONFIG SET appendonly no), so the old AOF files remain. See the Redis log."
    else
      jm_redis_compact_log "the AOF rewrite failed (aof_last_bgrewrite_status:${_jm_status:-?}); see the Redis log"
    fi
    return 1
  done
  if [ -z "$baseline" ]; then
    jm_redis_compact_log "no AOF rewrite could be started within ${timeout} s: another rewrite was still running (aof_rewrite_in_progress:${_jm_prog:-?}), or INFO persistence and the AOF manifest could not be read. See the Redis log; raise JM_REDIS_COMPACT_TIMEOUT if a large rewrite is simply slow."
  elif [ "$_jm_sched" = 1 ]; then
    jm_redis_compact_log "the AOF rewrite was still only scheduled after ${timeout} s (aof_rewrite_scheduled:1, aof_last_bgrewrite_status:${_jm_status:-?}): Redis waits for a background save to end, or retries a rewrite that could not start. See the Redis log."
  else
    jm_redis_compact_log "the AOF rewrite did not end within ${timeout} s (aof_rewrite_in_progress:${_jm_prog:-?}); raise JM_REDIS_COMPACT_TIMEOUT for a large keyspace, or see the Redis log"
  fi
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
  jm_redis_acl_text_replay_safe < "$1"
}
jm_redis_acl_text_replay_safe() { # acl text on stdin
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
  '
}

jm_redis_running_acl_state() { # project
  local c rc acl aclfile
  c="$(jm_redis_find "$1")" || {
    rc=$?
    [ "$rc" = 3 ] || return 1
    echo down
    return 0
  }
  if acl="$(docker exec "$c" cat /run/jm-redis/users.acl 2>/dev/null)" && [ -n "$acl" ]; then
    if printf '%s\n' "$acl" | jm_redis_acl_text_replay_safe; then echo fixed; else echo old; fi
    return 0
  fi
  # No rendered ACL: is Redis running without an ACL file at all?
  if aclfile="$(jm_redis_cli "$c" CONFIG GET aclfile 2>/dev/null)" &&
    [ "$(printf '%s\n' "$aclfile" | tr -d '\r' | sed -n 1p)" = aclfile ] &&
    [ -z "$(printf '%s\n' "$aclfile" | tr -d '\r' | sed -n 2p)" ]; then
    echo noacl
  else
    echo unreadable
  fi
}

# The host folder that holds Redis's appendonlydir (its /data mount).
jm_redis_data_path() { # project
  local c src
  c="$(jm_redis_find "$1")" || return 1
  src="$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Source}}{{end}}{{end}}' "$c")" || return 1
  [ -n "$src" ] || return 1
  printf '%s/appendonlydir\n' "$src"
}

jm_redis_compact_if_old_acl() { # project, ack backup (0|1)
  local state c
  state="$(jm_redis_running_acl_state "$1")" || return 1
  case "$state" in
    fixed|noacl)
      jm_redis_compact_log "the running Redis loaded an ACL that replays transactions ($state); no AOF rewrite needed"
      return 0 ;;
    down) return 3 ;;
    old)
      [ "${2:-0}" = 1 ] || return 4
      jm_redis_compact_log "the running Redis still has the 0.5.0 'user default ... -@all' line: rewriting its AOF now, without a restart" ;;
    unreadable)
      jm_redis_compact_log "the running Redis may still have the 0.5.0 ACL (could not read users.acl): rewriting its AOF now, without a restart" ;;
  esac
  c="$(jm_redis_find "$1")" || return 1
  jm_redis_compact_container "$c"
}

jm_redis_before_restart() { # project, acl file, allow replay, on fail (refuse|warn), on down (start|refuse), reason
  local project="$1" acl="$2" allow="${3:-0}" on_fail="${4:-refuse}" on_down="${5:-start}" reason="${6:-}" rc
  jm_redis_compact "$project" && return 0
  rc=$?
  if [ "$rc" = 3 ] && [ "$on_down" = refuse ]; then
    jm_redis_compact_log "refusing to start Redis: it is not running, so its AOF cannot be rewritten, and the Redis this run checked did not show the fixed ACL. Its next start could replay transactions an earlier start refused."
    return 1
  elif [ "$rc" = 3 ]; then
    jm_redis_compact_log "Redis of project $project is not running, so its AOF was not rewritten. Its next start replays the whole AOF as it is."
  elif [ "$on_fail" = warn ]; then
    jm_redis_compact_log "WARNING: this AOF rewrite did not finish. Going on: $reason"
  else
    jm_redis_compact_log "refusing to stop or recreate Redis: its AOF rewrite did not finish"
    return 1
  fi
  [ -n "$acl" ] || return 0
  jm_redis_acl_replay_safe "$acl" && return 0
  if [ "$allow" = 1 ]; then
    jm_redis_compact_log "WARNING: $acl still has the 0.5.0 'user default ... -@all' line, so this start drops every replayed transaction. Starting anyway (--allow-aof-replay)."
    return 0
  fi
  jm_redis_compact_log "refusing to start Redis: $acl still has the 0.5.0 'user default ... -@all' line, so Redis would drop every replayed transaction (JuiceFS metadata). Install the fixed server/redis.acl first (CHANGELOG 0.5.1), or pass --allow-aof-replay to accept that loss."
  return 1
}
