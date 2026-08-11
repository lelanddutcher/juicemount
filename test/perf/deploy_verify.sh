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
NEW_APP="$1"; GOOD_APP="$2"
CP=http://127.0.0.1:11050
FI="$HOME/.juicemount/fuse-internal"
LOG(){ echo "[deploy $(date +%H:%M:%S)] $*"; }
bounded(){ perl -e 'alarm shift; exec @ARGV' "$@"; }

clean_stop(){
  LOG "clean stop: graceful quit"
  osascript -e 'tell application "JuiceMount" to quit' >/dev/null 2>&1
  for i in $(seq 1 12); do pgrep -f 'JuiceMount.app/Contents/MacOS' >/dev/null || break; sleep 1; done
  pgrep -f 'JuiceMount.app/Contents/MacOS' >/dev/null && { LOG "force kill app"; pkill -9 -f 'JuiceMount.app/Contents/MacOS'; sleep 2; }
  # juicefs must die too or the next launch hangs on a stale FUSE session
  pgrep -f 'juicefs.*mount' >/dev/null && { LOG "kill juicefs"; pkill -f 'juicefs.*mount'; sleep 2; }
  mount | grep -q 'zpool on /Volumes' && { LOG "umount NFS"; bounded 10 umount /Volumes/zpool 2>/dev/null || bounded 10 umount -f /Volumes/zpool 2>/dev/null; }
  [ -n "${ALT_MOUNT:-}" ] && mount | grep -q "$ALT_MOUNT" && { LOG "umount ALT $ALT_MOUNT"; bounded 10 umount "$ALT_MOUNT" 2>/dev/null || bounded 10 umount -f "$ALT_MOUNT" 2>/dev/null; }
  mount | grep -q "$FI" && { LOG "umount FUSE"; bounded 10 umount "$FI" 2>/dev/null || bounded 10 umount -f "$FI" 2>/dev/null; }
  sleep 1
  LOG "stopped: app=$(pgrep -cf 'JuiceMount.app/Contents/MacOS' || echo 0) juicefs=$(pgrep -cf 'juicefs.*mount' || echo 0) mounts=$(mount | grep -c -e 'zpool on /Volumes' -e "$FI")"
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

launch(){
  LOG "launch $1"
  open -a "$1" 2>/dev/null
  # 07-10 hardening: `open -a` can silently no-op right after a kill -9 of
  # the prior instance (stale LaunchServices running-state) — Batch D never
  # started and the whole deploy "failed" with a healthy binary. Verify the
  # process EXISTS within 10s; fall back to launching the naked binary.
  for i in $(seq 1 10); do
    pgrep -f "$1/Contents/MacOS" >/dev/null && { LOG "launch confirmed (open)"; return 0; }
    sleep 1
  done
  LOG "open -a did not start the app — naked-binary fallback"
  nohup "$1/Contents/MacOS/JuiceMount" >/dev/null 2>&1 &
  disown
  sleep 3
  pgrep -f "$1/Contents/MacOS" >/dev/null && LOG "launch confirmed (naked)" || LOG "LAUNCH FAILED ENTIRELY"
}

LOG "=== DEPLOY $NEW_APP (rollback: $GOOD_APP) ==="
clean_stop
launch "$NEW_APP"
if verify "${VERIFY_BUDGET:-180}"; then
  LOG "=== NEW BUILD LIVE + VERIFIED ==="
  exit 0
fi
LOG "!!! new build FAILED verification — ROLLING BACK"
clean_stop
launch "$GOOD_APP"
if verify "${VERIFY_BUDGET:-180}"; then
  LOG "=== ROLLBACK OK — known-good restored ==="
  exit 1
fi
LOG "!!!!! ROLLBACK ALSO FAILED — mount left down, human needed"
exit 2
