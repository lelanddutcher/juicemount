#!/bin/sh
# repair-derivative-dir-ownership.sh — make existing per-inode derivative
# directories as writable as the tree they live in.
#
# WHAT THIS IS FOR
#
# `.juicemount/derivatives/<inode>/` is written by TWO producers: the farm,
# which runs as root inside a container on the server, and the desktop client,
# which runs as the logged-in user. Until 2026-08-11 both created these
# directories at a hardcoded 0755, so whichever produced a given asset FIRST
# owned the directory and locked the other out of it permanently.
#
# On the volume where this was found: 12,016 root-owned directories against
# 2,780 client-owned, and client derivative uploads failing with
# `permission denied` while the parent `.juicemount/derivatives/` was
# 501:20 drwxrwxr-x the whole time. The tree was writable; only the children
# were not.
#
# New directories now inherit their parent (internal/derivatives/openat.go).
# This script repairs the ones created before that.
#
# ────────────────────────────────────────────────────────────────────────────
# RUN THIS ONLY AFTER THE CLIENT CARRYING THE CLOBBER GUARD IS INSTALLED.
#
# The permission error is currently the only thing preventing a client
# contribution from REPLACING a farm artifact of the same name, and those are
# not the same artifact — a client waveform.json is a 2,000-pixel preview
# where the farm's is 149,166-pixel full resolution. Failed spool rows are
# requeued on reconnect (nfs/drainer.go), so making these directories writable
# while an unguarded client is running hands it exactly the overwrite this
# whole change exists to prevent. Verify first:
#
#     curl -s http://127.0.0.1:11050/whoami
#
# and confirm the running build postdates the guard.
# ────────────────────────────────────────────────────────────────────────────
#
# WHAT IT CHANGES
#
# Directory ownership and mode ONLY, copied from the parent derivatives dir.
# Blob ownership inside each directory is deliberately left alone: root keeps
# its files, root can still write regardless of mode, and the clobber guard —
# not the filesystem — is what protects the contents.
#
# USAGE
#
#   ./repair-derivative-dir-ownership.sh --mount /jfs                 # dry run
#   ./repair-derivative-dir-ownership.sh --mount /jfs --apply
#
# Run it wherever the volume is mounted BY A PROCESS THAT CAN CHOWN — i.e. as
# root on the server, or inside the farm container:
#
#   docker exec <farm-container> sh -c '...'
#
# The client cannot repair these itself: chown needs root or ownership, and it
# has neither.

# POSIX sh, not bash: this has to run inside the farm container, whose shell is
# dash/busybox. There are no pipes here, so pipefail bought nothing anyway.
set -eu

MOUNT=""
APPLY=0

while [ $# -gt 0 ]; do
  case "$1" in
    --mount) MOUNT="${2:-}"; shift 2 ;;
    --apply) APPLY=1; shift ;;
    -h|--help) sed -n '2,60p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [ -z "$MOUNT" ]; then
  echo "error: --mount <path-to-volume> is required" >&2
  exit 2
fi

ROOT="$MOUNT/.juicemount/derivatives"
if [ ! -d "$ROOT" ]; then
  echo "error: $ROOT is not a directory — is the volume mounted here?" >&2
  exit 1
fi

# The parent is the authority. Everything below is made to match it, so a
# locked-down derivatives tree stays locked down.
OWNER="$(stat -c '%u' "$ROOT" 2>/dev/null || stat -f '%u' "$ROOT")"
GROUP="$(stat -c '%g' "$ROOT" 2>/dev/null || stat -f '%g' "$ROOT")"
PMODE="$(stat -c '%a' "$ROOT" 2>/dev/null || stat -f '%Lp' "$ROOT")"

echo "derivatives root : $ROOT"
echo "target ownership : ${OWNER}:${GROUP}  mode ${PMODE}   (copied from the parent)"
echo

total=0
mismatched=0
changed=0
# Split the count, because the two cases are not equally interesting. A
# WRONG-OWNER directory is the actual bug: the other producer cannot write
# there at all. A mode-only difference is cosmetic — the owner can still write,
# and root always can — and is repaired purely so old and new directories look
# the same.
wrongowner=0
modeonly=0

for d in "$ROOT"/*/; do
  [ -d "$d" ] || continue
  total=$((total + 1))
  u="$(stat -c '%u' "$d" 2>/dev/null || stat -f '%u' "$d")"
  g="$(stat -c '%g' "$d" 2>/dev/null || stat -f '%g' "$d")"
  m="$(stat -c '%a' "$d" 2>/dev/null || stat -f '%Lp' "$d")"

  if [ "$u" = "$OWNER" ] && [ "$g" = "$GROUP" ] && [ "$m" = "$PMODE" ]; then
    continue
  fi
  mismatched=$((mismatched + 1))
  if [ "$u" != "$OWNER" ] || [ "$g" != "$GROUP" ]; then
    wrongowner=$((wrongowner + 1))
  else
    modeonly=$((modeonly + 1))
  fi

  if [ "$APPLY" = "1" ]; then
    if chown "${OWNER}:${GROUP}" "$d" && chmod "$PMODE" "$d"; then
      changed=$((changed + 1))
    else
      echo "FAILED: $d" >&2
    fi
  elif [ "$mismatched" -le 5 ]; then
    echo "would repair: $d  (${u}:${g} mode ${m})"
  fi
done

echo
echo "directories scanned  : $total"
echo "needing repair       : $mismatched"
echo "  wrong owner        : $wrongowner   <- the bug: the other producer is locked out"
echo "  mode differs only  : $modeonly   <- cosmetic: owner and root can already write"
if [ "$APPLY" = "1" ]; then
  echo "repaired            : $changed"
  if [ "$changed" -ne "$mismatched" ]; then
    echo "WARNING: $((mismatched - changed)) directories could not be repaired — are you root?" >&2
    exit 1
  fi
else
  echo
  echo "DRY RUN — nothing changed. Re-run with --apply once the guarded client is installed."
fi
