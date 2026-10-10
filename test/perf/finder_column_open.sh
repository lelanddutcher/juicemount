#!/bin/bash
# finder_column_open.sh — L4 of the latency ladder: measure REAL Finder
# column-view populate time for a target folder.
#
# Opens the target in a Finder window, forces COLUMN view (`icl` = column),
# then polls `count of items of the target folder` until it stabilizes, timing
# from just-before-open to stable. Closes the window afterwards.
#
# NOTE ON HONESTY: Finder caches aggressively across opens. To get a COLD open
# we (a) close any window showing the target, (b) sleep > the 5s juicefs TTL,
# and (c) additionally quit/relaunch is NOT done (too disruptive) — so this is
# "Finder warm-process, caches-expired" cold, which is representative of the
# user reopening a not-recently-browsed folder in an already-running Finder.
# We report the caveat rather than overclaim a pristine-cold number.
#
# Column view forces Finder to GETATTR every child (to draw the row + the
# chevron for subfolders) and, in a media folder, to request Quick Look
# thumbnails — that is the cost we are decomposing.
#
# Bounded to ~35s per open. READ-ONLY navigation; opening a MEDIA folder in
# column view WILL make Finder read small amounts of file bytes for thumbnails
# (intentional; noted).
#
# Usage: finder_column_open.sh <target_dir> [label] [cool_seconds]
set -u
TARGET="${1:?usage: finder_column_open.sh <dir> [label] [cool_s]}"
LABEL="${2:-target}"
COOL="${3:-7}"

# POSIX path -> AppleScript. Finder wants an alias/POSIX file.
read -r -d '' OSA <<APPLESCRIPT
on run
  set target_posix to "$TARGET"
  set t_folder to (POSIX file target_posix) as alias
  tell application "Finder"
    -- close any window already showing the target (force cold-ish)
    activate
    try
      repeat with w in (every Finder window)
        try
          if (target of w) as alias is t_folder then close w
        end try
      end repeat
    end try
  end tell
  delay 0.3
  -- cool caches
  do shell script "sleep $COOL"
  -- timestamped open
  set t0 to (do shell script "python3 -c 'import time;print(\"%.6f\"%time.time())'")
  tell application "Finder"
    set w to make new Finder window to t_folder
    set current view of w to column view
    set bounds of w to {80, 80, 1200, 900}
  end tell
  -- poll item count until stable (2 identical reads) or timeout
  set stableCount to -1
  set sameRuns to 0
  set stableAt to "timeout"
  repeat 120 times
    delay 0.1
    set c to -1
    try
      tell application "Finder" to set c to (count of items of t_folder)
    end try
    if c = stableCount and c > 0 then
      set sameRuns to sameRuns + 1
      if sameRuns >= 3 then
        set stableAt to (do shell script "python3 -c 'import time;print(\"%.6f\"%time.time())'")
        exit repeat
      end if
    else
      set sameRuns to 0
    end if
    set stableCount to c
  end repeat
  set t_first_probe to (do shell script "python3 -c 'import time;print(\"%.6f\"%time.time())'")
  -- leave window open ~1s so thumbnails render, then close
  delay 1.0
  tell application "Finder"
    try
      close w
    end try
  end tell
  return t0 & "|" & stableAt & "|" & stableCount
end run
APPLESCRIPT

echo "=== L4 Finder column-view open  [$LABEL] ==="
echo "  target: $TARGET  (cool ${COOL}s)"
RES=$(perl -e 'alarm 45; exec @ARGV' osascript -e "$OSA" 2>&1)
echo "  raw: $RES"
python3 - "$RES" <<'PY'
import sys
raw=sys.argv[1]
try:
    t0,stable,cnt=raw.split("|")
    t0=float(t0)
    if stable=="timeout":
        print("  RESULT: count=%s  populate=TIMEOUT (>12s, did not stabilize)"%cnt)
    else:
        print("  RESULT: count=%s  open->stable = %.0f ms"%(cnt,(float(stable)-t0)*1000))
except Exception as e:
    print("  parse error:",e,"raw=",raw)
PY
