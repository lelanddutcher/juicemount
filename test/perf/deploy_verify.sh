#!/bin/bash
# deploy_verify.sh — unattended app deploy with self-verify + auto-rollback.
#
#   deploy_verify.sh <new-app-path> <known-good-app-path>
#
# Procedure (every step bounded; a wedged mount can never hang this script):
#   1. CLEAN STOP (the restart-procedure rule: kill the app AND juicefs AND
#      unmount FUSE + NFS, or the next startup hangs):
#        graceful quit → pkill app → pkill juicefs → umount NFS + FUSE (+ -f)
#   2. LAUNCH the new .app (open -a).
#   3. SELF-VERIFY within 180s, all four:
#        a. control plane /health = 200 AND every component "ok"
#        b. FUSE identity: fuse-internal is a JuiceFS/macfuse mount, NOT local disk
#        c. NFS mount lists: bounded ls /Volumes/zpool returns entries
#        d. backend reach: redis+minio components ok (covers tunnel + LocalNetwork)
#   4. On failure: ROLLBACK — clean stop again, launch the known-good app,
#      re-verify (same gates). Exit 0 = new build live; 1 = rolled back OK;
#      2 = rollback ALSO failed (mount left stopped — human needed).
set -u

# LaunchServices must receive a bundle path, not an application name. Keep both
# inputs absolute so the process-identity check below matches the command path
# macOS records for a persistently launched bundle.
absolute_app_path(){
  local app="$1"
  (cd "$(dirname "$app")" && printf '%s/%s\n' "$PWD" "$(basename "$app")")
}

NEW_APP="$(absolute_app_path "$1")"
GOOD_APP="$(absolute_app_path "$2")"
CP=http://127.0.0.1:11050
FI="$HOME/.juicemount/fuse-internal"
LOG(){ echo "[deploy $(date +%H:%M:%S)] $*"; }
bounded(){ perl -e 'alarm shift; exec @ARGV' "$@"; }
proc_count(){ pgrep -f "$1" 2>/dev/null | wc -l | tr -d ' '; }

product_mount_count(){
  local count
  count="$(mount | grep -c -e ' on /Volumes/zpool ' -e " on $FI " || true)"
  if [ -n "${ALT_MOUNT:-}" ]; then
    count=$(( count + $(mount | grep -c " on $ALT_MOUNT " || true) ))
  fi
  echo "$count"
}

# Match the product executable rather than one bundle name. Release validation
# deliberately launches versioned copies (for example "JuiceMount RC abc.app")
# next to the installed app; matching only "JuiceMount.app" leaves those copies
# alive during rollback, holding ports and the shared tsnet state directory.
product_app_pids(){
  local pid command
  while read -r pid command; do
    case "$command" in
      */Contents/MacOS/JuiceMount|*/Contents/MacOS/JuiceMount\ *) echo "$pid" ;;
    esac
  done < <(ps -axo pid=,command=)
}

product_app_count(){
  local pids
  pids="$(product_app_pids)"
  [ -z "$pids" ] && { echo 0; return; }
  printf '%s\n' "$pids" | wc -l | tr -d ' '
}

app_pids_for(){
  local app="$1" want="$1/Contents/MacOS/JuiceMount" pid command
  while read -r pid command; do
    case "$command" in
      "$want"|"$want "*) echo "$pid" ;;
    esac
  done < <(ps -axo pid=,command=)
}

unmount_path(){
  local target="$1"
  bounded 10 umount "$target" 2>/dev/null && return 0
  bounded 10 umount -f "$target" 2>/dev/null && return 0
  # A GUI-launched app's NFS mount may require Disk Arbitration even for the
  # same logged-in user (plain umount returns EPERM). diskutil performs that
  # authenticated user-session unmount without requiring sudo.
  bounded 15 diskutil unmount force "$target" >/dev/null 2>&1
}

clean_stop(){
  LOG "clean stop: graceful quit"
  # Apple Events can block forever when the app's main thread is trapped behind
  # a wedged Finder/NFS request. The rollback harness itself must remain bounded.
  bounded 8 osascript -e 'tell application id "com.juicemount.app" to quit' >/dev/null 2>&1 || true
  for i in $(seq 1 12); do [ "$(product_app_count)" -eq 0 ] && break; sleep 1; done
  local app_pids
  app_pids="$(product_app_pids)"
  if [ -n "$app_pids" ]; then
    LOG "terminate surviving app pids: $(echo "$app_pids" | tr '\n' ' ')"
    kill -TERM $app_pids 2>/dev/null || true
    for i in $(seq 1 10); do [ "$(product_app_count)" -eq 0 ] && break; sleep 1; done
  fi
  app_pids="$(product_app_pids)"
  if [ -n "$app_pids" ]; then
    LOG "force kill surviving app pids: $(echo "$app_pids" | tr '\n' ' ')"
    kill -KILL $app_pids 2>/dev/null || true
    sleep 2
  fi
  # juicefs must die too or the next launch hangs on a stale FUSE session
  pgrep -f 'juicefs.*mount' >/dev/null && { LOG "kill juicefs"; pkill -f 'juicefs.*mount'; sleep 2; }
  mount | grep -q ' on /Volumes/zpool ' && { LOG "umount NFS"; unmount_path /Volumes/zpool || true; }
  [ -n "${ALT_MOUNT:-}" ] && mount | grep -q " on $ALT_MOUNT " && { LOG "umount ALT $ALT_MOUNT"; unmount_path "$ALT_MOUNT" || true; }
  mount | grep -q " on $FI " && { LOG "umount FUSE"; unmount_path "$FI" || true; }
  sleep 1
  local remaining_apps remaining_juicefs remaining_mounts
  remaining_apps="$(product_app_count)"
  remaining_juicefs="$(proc_count 'juicefs.*mount')"
  remaining_mounts="$(product_mount_count)"
  LOG "stopped: app=$remaining_apps juicefs=$remaining_juicefs mounts=$remaining_mounts"
  [ "$remaining_apps" -eq 0 ] && [ "$remaining_juicefs" -eq 0 ] && [ "$remaining_mounts" -eq 0 ]
}

verify(){ # verify <budget-seconds> → 0 ok / 1 fail
  local budget="$1" t0=$(date +%s)
  while [ $(( $(date +%s) - t0 )) -lt "$budget" ]; do
    local code=$(bounded 6 curl -s -m5 -o /dev/null -w '%{http_code}' $CP/health 2>/dev/null)
    if [ -n "${ALT_MOUNT:-}" ]; then
      # Haunted-canonical-mount mode: the app's own /Volumes/zpool mount will
      # EBUSY (kernel ghost); nfs component stays degraded by design. Gate on
      # control-plane answering (200 or 503) + FUSE identity, then self-mount
      # the alternate and require IT to list.
      if [ "$code" = "200" ] || [ "$code" = "503" ]; then
        local fuseid=$(df "$FI" 2>/dev/null | tail -1 | grep -cE 'JuiceFS|macfuse|fuse')
        if [ "$fuseid" -ge 1 ]; then
          mount | grep -q "$ALT_MOUNT" || {
            LOG "mounting ALT $ALT_MOUNT"
            sudo -n mkdir -p "$ALT_MOUNT" 2>/dev/null
            bounded 25 sudo -n mount_nfs -o "port=11049,mountport=11049,hard,intr,timeo=400,retrans=2,nolocks,locallocks,rsize=1048576,wsize=1048576,readahead=16,acregmin=3600,acregmax=3600,acdirmin=3,acdirmax=15,vers=3,tcp" 127.0.0.1:/ "$ALT_MOUNT" 2>/dev/null
          }
          local lists=$(bounded 8 ls "$ALT_MOUNT" 2>/dev/null | head -1 | grep -c .)
          LOG "verify(ALT): health=$code fuseid=$fuseid alt_lists=$lists"
          [ "$lists" -ge 1 ] && return 0
        else
          LOG "verify(ALT): health=$code fuseid=$fuseid (waiting for FUSE)"
        fi
      else
        LOG "verify: health=$code (waiting)"
      fi
    elif [ "$code" = "200" ]; then
      local comps=$(bounded 6 curl -s -m5 $CP/health 2>/dev/null | python3 -c 'import sys,json;d=json.load(sys.stdin);cs=d.get("components",{});print("ok" if cs and all(v=="ok" for v in cs.values()) else "bad")' 2>/dev/null)
      local fuseid=$(df "$FI" 2>/dev/null | tail -1 | grep -cE 'JuiceFS|macfuse|fuse')
      local lists=$(bounded 8 ls /Volumes/zpool 2>/dev/null | head -1 | grep -c .)
      LOG "verify: health=200 comps=$comps fuseid=$fuseid lists=$lists"
      [ "$comps" = "ok" ] && [ "$fuseid" -ge 1 ] && [ "$lists" -ge 1 ] && return 0
    else
      LOG "verify: health=$code (waiting)"
    fi
    sleep 6
  done
  return 1
}

verify_stable(){ # verify_stable <app-path> -> 0 only if bundle stays live
  local app="$1" settle="${STABILITY_SECONDS:-20}"
  LOG "stability gate: waiting ${settle}s for persistent bundle process"
  sleep "$settle"
  if [ -z "$(app_pids_for "$app")" ]; then
    LOG "stability gate: app process disappeared"
    return 1
  fi
  verify 12
}

launch(){
  LOG "launch $1"
  open -n "$1" 2>/dev/null
  # 07-10 hardening: LaunchServices can silently no-op right after a kill -9 of
  # the prior instance (stale LaunchServices running-state) — Batch D never
  # started and the whole deploy "failed" with a healthy binary. Verify the
  # process EXISTS within 10s; fall back to launching the naked binary.
  for i in $(seq 1 10); do
    [ -n "$(app_pids_for "$1")" ] && { LOG "launch confirmed (open)"; return 0; }
    sleep 1
  done
  LOG "open -na did not start the app — naked-binary fallback"
  nohup "$1/Contents/MacOS/JuiceMount" >/dev/null 2>&1 &
  disown
  sleep 3
  [ -n "$(app_pids_for "$1")" ] && LOG "launch confirmed (naked)" || LOG "LAUNCH FAILED ENTIRELY"
}

LOG "=== DEPLOY $NEW_APP (rollback: $GOOD_APP) ==="
clean_stop || { LOG "!!!!! CLEAN STOP FAILED — refusing to launch a second app instance"; exit 2; }
launch "$NEW_APP"
if verify "${VERIFY_BUDGET:-180}" && verify_stable "$NEW_APP"; then
  LOG "=== NEW BUILD LIVE + VERIFIED ==="
  exit 0
fi
LOG "!!! new build FAILED verification — ROLLING BACK"
clean_stop || { LOG "!!!!! ROLLBACK CLEAN STOP FAILED — refusing to launch a second app instance"; exit 2; }
launch "$GOOD_APP"
if verify "${VERIFY_BUDGET:-180}" && verify_stable "$GOOD_APP"; then
  LOG "=== ROLLBACK OK — known-good restored ==="
  exit 1
fi
LOG "!!!!! ROLLBACK ALSO FAILED — mount left down, human needed"
exit 2
