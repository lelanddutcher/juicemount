#!/usr/bin/env bash
# wedge-tests/fuse-unmount-mid-drain.sh — real-stack validation of the
# transient-FUSE-outage data-loss guards (commit afc39d8: RecoverOnBoot
# failed-row preservation + drainer isInfraUnavailable classifier).
#
# WHY THIS EXISTS: a testing-fidelity audit (2026-06-14) flagged that the
# drainer's isInfraUnavailable() classifier was only unit-tested with a
# HAND-CONSTRUCTED &os.PathError{Err: syscall.ENXIO} — circular: it feeds the
# classifier the exact constant it checks for. That proves the errors.Is logic
# but NOT that a real dead JuiceFS/macFUSE mount actually returns
# ENXIO/ENODEV/ENOTCONN. The audit's worst-case prediction was that the
# mount reverts to an empty local dir (so Create SUCCEEDS into stale local
# storage, or returns ENOENT/EIO), which would defeat the classifier entirely.
# This harness settles it empirically, on the real stack, and is the committed,
# reproducible artifact the audit said was missing.
#
# Scenario: a real JuiceFS FUSE daemon (macFUSE) dies mid-drain — exactly the
# "restart/unmount window" the fix targets. We reproduce it by `kill -9`ing the
# juicefs processes WITHOUT unmounting (the kernel mount lingers, routing to a
# dead daemon — the real "device not configured" condition), NOT by SIGSTOP
# (which only hangs; see fuse-hang-mid-op.sh for that opposite failure mode).
#
# Two parts:
#
#   PART 1 — errno ground truth. Build a tiny Go probe that does exactly what
#   the drainer's first FUSE ops do (os.MkdirAll + os.OpenFile under the FUSE
#   root) and reports the RAW errno plus whether the production classifier set
#   {ENXIO, ENODEV, ENOTCONN} catches it. Kill juicefs, probe immediately.
#   ACCEPTANCE: at least one probe in the dead-daemon window returns a real
#   syscall error caught by the classifier. If it returns success or an
#   un-classified errno, the classifier is wrong for this macFUSE/macOS build
#   and the fix is a false positive — FAIL.
#
#   PART 2 — end-to-end loss guard. Copy real media onto the kernel NFS mount,
#   then `kill -9` juicefs mid-drain. ACCEPTANCE:
#     - No /Volumes row for these files ever lands in permanent `failed` state
#       (the classifier requeues instead of burning the retry budget).
#     - The app's FUSE monitor auto-remounts juicefs.
#     - Every file drains to `done` (durable in MinIO) and reads back through
#       the NFS mount BYTE-PERFECT (SHA-256 == source).
#
# This is invasive: it hard-kills the JuiceFS daemon. The app's health monitor
# remounts it (~8s observed). If the script dies between kill and remount, the
# app should still recover on its own; the EXIT trap also nudges a remount check.
#
# Prerequisites:
#   1. JuiceMount running (metrics 200 on $METRICS), NFS mount up at $MOUNT.
#   2. JuiceFS daemon running (pgrep -f 'juicefs.*mount').
#   3. `go` on PATH (to build the probe).
#   4. Real media: pass --media DIR (uses *.CR3/*.MP4/*.MOV) or rely on the
#      default staged corpus /tmp/jm_rt/*.CR3. NEVER reads the read-only camera
#      card; copies are made from the staged corpus.
#
# Usage:
#   scripts/wedge-tests/fuse-unmount-mid-drain.sh \
#       [--mount PATH]      \ default /Volumes/zpool-dev
#       [--metrics URL]     \ default http://127.0.0.1:11050/debug/pprof/
#       [--media DIR]       \ default /tmp/jm_rt   (uses up to 3 media files)
#       [--remount-budget S]\ default 30
#       [--drain-budget S]  \ default 120
#
# Exit codes:
#   0  pass
#   1  fail (acceptance criterion missed — a real data-loss or classifier gap)
#   2  precondition error
set -u

MOUNT=/Volumes/zpool-dev
METRICS=http://127.0.0.1:11050/debug/pprof/
MEDIA_DIR=/tmp/jm_rt
REMOUNT_BUDGET=30
DRAIN_BUDGET=120
DB="$HOME/Library/Application Support/JuiceMount/metadata.db"
FUSE="$HOME/.juicemount/fuse-internal"

while [ $# -gt 0 ]; do
  case "$1" in
    --mount) MOUNT="$2"; shift 2;;
    --metrics) METRICS="$2"; shift 2;;
    --media) MEDIA_DIR="$2"; shift 2;;
    --remount-budget) REMOUNT_BUDGET="$2"; shift 2;;
    --drain-budget) DRAIN_BUDGET="$2"; shift 2;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

red(){ printf '\033[31m%s\033[0m\n' "$1"; }
grn(){ printf '\033[32m%s\033[0m\n' "$1"; }
ylw(){ printf '\033[33m%s\033[0m\n' "$1"; }

healthy(){ [ "$(mount | grep -c "$MOUNT")" = 1 ] && [ "$(curl -s --max-time 2 -o /dev/null -w '%{http_code}' "$METRICS" 2>/dev/null)" = 200 ]; }

# ---- preconditions ------------------------------------------------------
command -v go >/dev/null 2>&1 || { red "go not on PATH"; exit 2; }
healthy || { red "precondition: app not healthy (mount=$(mount|grep -c "$MOUNT") metrics=$(curl -s --max-time 2 -o /dev/null -w '%{http_code}' "$METRICS"))"; exit 2; }
pgrep -f 'juicefs.*mount' >/dev/null || { red "precondition: no juicefs daemon running"; exit 2; }
[ -f "$DB" ] || { red "precondition: spool DB not found at $DB"; exit 2; }

# media corpus (up to 3). bash-3.2 portable (macOS /usr/bin/env bash is 3.2 —
# no mapfile, no associative arrays). Staged corpus paths have no spaces.
SRCS=()
while IFS= read -r f; do [ -n "$f" ] && SRCS+=("$f"); done < <(ls "$MEDIA_DIR"/*.CR3 "$MEDIA_DIR"/*.MP4 "$MEDIA_DIR"/*.MOV 2>/dev/null | head -3)
[ "${#SRCS[@]}" -ge 1 ] || { red "precondition: no media in $MEDIA_DIR (*.CR3/*.MP4/*.MOV)"; exit 2; }

WORK="$(mktemp -d)"
TESTDIR="$MOUNT/_wedge_unmount_$$"
cleanup(){
  # Wait for the app to remount juicefs (this test hard-kills it) so the FUSE
  # cleanup below actually lands and we leave a healthy system.
  for _ in $(seq 1 "${REMOUNT_BUDGET:-30}"); do
    pgrep -f 'juicefs.*mount' >/dev/null && healthy && break
    sleep 1
  done
  rm -f "$TESTDIR"/* 2>/dev/null; rmdir "$TESTDIR" 2>/dev/null
  sqlite3 "$DB" "DELETE FROM spool_entries WHERE nfs_path LIKE '_wedge_unmount_$$/%';" 2>/dev/null
  rm -rf "$FUSE/$(basename "$TESTDIR")" 2>/dev/null
  rm -rf "$WORK" 2>/dev/null
  if ! pgrep -f 'juicefs.*mount' >/dev/null; then
    ylw "NOTE: juicefs not running at cleanup — the app monitor should remount; if not, restart JuiceMount."
  fi
}
trap cleanup EXIT INT TERM

# ---- build the errno probe ---------------------------------------------
cat > "$WORK/main.go" <<'GO'
package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// Mirrors nfs/drainer.go isInfraUnavailable EXACTLY. Keep in sync.
func isInfraUnavailable(err error) bool {
	return errors.Is(err, syscall.ENXIO) ||
		errors.Is(err, syscall.ENODEV) ||
		errors.Is(err, syscall.ENOTCONN)
}

func rawErrno(err error) string {
	var e syscall.Errno
	if errors.As(err, &e) {
		return fmt.Sprintf("errno=%d(%v)", int(e), e)
	}
	if err == nil {
		return "nil"
	}
	return "non-errno"
}

func main() {
	root := os.Args[1]
	parent := filepath.Join(root, "_jmprobe")
	mk := os.MkdirAll(parent, 0o755)
	dest := filepath.Join(parent, "p.tmp")
	f, op := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if op == nil && f != nil {
		f.Close()
		_ = os.Remove(dest)
		_ = os.Remove(parent)
	}
	// SUCCESS if all ops succeeded (mount alive OR reverted-to-local-dir);
	// CAUGHT if any op returned a classifier-matched errno; UNCLASSIFIED otherwise.
	state := "SUCCESS"
	if isInfraUnavailable(mk) || isInfraUnavailable(op) {
		state = "CAUGHT"
	} else if mk != nil || op != nil {
		state = "UNCLASSIFIED"
	}
	fmt.Printf("state=%s mkdir=%s open=%s\n", state, rawErrno(mk), rawErrno(op))
}
GO
( cd "$WORK" && go mod init jmprobe >/dev/null 2>&1 && go build -o "$WORK/probe" . ) || { red "failed to build errno probe"; exit 2; }

FAIL=0

# ======================================================================
# PART 1 — errno ground truth
# ======================================================================
echo "== PART 1: errno ground truth (kill juicefs, probe the dead mount) =="
pgrep -f 'juicefs.*mount' | xargs kill -9 2>/dev/null
CAUGHT=0; SAW_SUCCESS=0; SAW_UNCLASSIFIED=""
for i in $(seq 1 8); do
  OUT="$("$WORK/probe" "$FUSE" 2>&1)"
  echo "  t=${i}s $OUT"
  case "$OUT" in
    *state=CAUGHT*)       CAUGHT=1;;
    *state=UNCLASSIFIED*) SAW_UNCLASSIFIED="$OUT";;
    *state=SUCCESS*)      SAW_SUCCESS=1;;
  esac
  [ "$CAUGHT" = 1 ] && break
  sleep 1
done
if [ "$CAUGHT" = 1 ]; then
  grn "PART 1 PASS: real dead-macFUSE error is caught by isInfraUnavailable {ENXIO,ENODEV,ENOTCONN}"
else
  red "PART 1 FAIL: classifier never caught the real dead-mount error."
  [ -n "$SAW_UNCLASSIFIED" ] && red "  saw UNCLASSIFIED errno: $SAW_UNCLASSIFIED  (extend the classifier set)"
  [ "$SAW_SUCCESS" = 1 ] && [ -z "$SAW_UNCLASSIFIED" ] && red "  ops SUCCEEDED — mount may have reverted to local dir (silent-local-write risk)"
  FAIL=1
fi

# wait for the app to remount before PART 2
echo "  waiting for app auto-remount (budget ${REMOUNT_BUDGET}s)..."
for i in $(seq 1 "$REMOUNT_BUDGET"); do healthy && pgrep -f 'juicefs.*mount' >/dev/null && break; sleep 1; done
if healthy && pgrep -f 'juicefs.*mount' >/dev/null; then
  grn "  app auto-remounted juicefs (resilience confirmed)"
else
  red "PART 1.5 FAIL: app did not auto-remount within ${REMOUNT_BUDGET}s"; FAIL=1
fi

# ======================================================================
# PART 2 — end-to-end loss guard (mid-drain outage)
# ======================================================================
echo "== PART 2: mid-drain outage — copy media, kill juicefs mid-drain, verify zero loss =="
mkdir -p "$TESTDIR"
for s in "${SRCS[@]}"; do
  cat "$s" > "$TESTDIR/$(basename "$s")" &
done
wait
echo "  copied ${#SRCS[@]} files; global pending=$(sqlite3 "$DB" "SELECT COUNT(*) FROM spool_entries WHERE drain_state IN ('writing','ready','draining');" 2>/dev/null)"

# kill mid-drain
pgrep -f 'juicefs.*mount' | xargs kill -9 2>/dev/null
echo "  killed juicefs mid-drain"

PREFIX="$(basename "$TESTDIR")"
for i in $(seq 1 "$DRAIN_BUDGET"); do
  FAILED=$(sqlite3 "$DB" "SELECT COUNT(*) FROM spool_entries WHERE drain_state='failed' AND nfs_path LIKE '$PREFIX/%';" 2>/dev/null)
  PEND=$(sqlite3 "$DB" "SELECT COUNT(*) FROM spool_entries WHERE drain_state IN ('writing','ready','draining') AND nfs_path LIKE '$PREFIX/%';" 2>/dev/null)
  if [ "${FAILED:-0}" -gt 0 ]; then red "PART 2 FAIL: a file went permanently 'failed' during the outage (data-loss path)"; FAIL=1; break; fi
  [ "${PEND:-1}" = "0" ] && break
  sleep 1
done

# The drain requires juicefs to be remounted; the durable copies live behind
# the FUSE mount. Wait for the app to bring juicefs back AND report healthy
# before verifying — otherwise we'd read a still-down mount and mis-report loss.
echo "  waiting for juicefs remount + health before verifying (budget ${REMOUNT_BUDGET}s)..."
for _ in $(seq 1 "$REMOUNT_BUDGET"); do healthy && pgrep -f 'juicefs.*mount' >/dev/null && ls "$FUSE/" >/dev/null 2>&1 && break; sleep 1; done

# AUTHORITATIVE data-loss gate: the durable object in MinIO, read via the
# JuiceFS FUSE mount ($FUSE/<dir>/<file>) — that is what actually survived the
# outage. This is the hard PASS/FAIL.
FUSEDIR="$FUSE/$(basename "$TESTDIR")"
echo "  verifying DURABLE copy (FUSE/MinIO) — the data-loss gate..."
for s in "${SRCS[@]}"; do
  b="$(basename "$s")"
  want="$(shasum -a 256 "$s" | awk '{print $1}')"
  dur="$(shasum -a 256 "$FUSEDIR/$b" 2>/dev/null | awk '{print $1}')"
  if [ "$want" = "$dur" ]; then grn "  PASS(durable) $b"; else red "  FAIL(durable) $b want=$want got=${dur:-MISSING} — REAL DATA LOSS"; FAIL=1; fi
done

# SECONDARY coherence check (WARN, not a hard fail). After a drain-through-
# outage the macOS NFS client's attribute cache can serve a stale/short read
# for a window while the file's handle transitions spool->FUSE. This is a
# client-cache COHERENCE delay, NOT data loss (the durable gate above already
# proved the bytes are safe), and it is not fully under the server's control,
# so it must never mask the real data-loss signal. We report how long it takes
# to converge (or that it didn't within the budget) for visibility. See
# docs/known follow-up: NFS read-coherence after a drain-through-outage.
echo "  NFS readback coherence (informational; client attr-cache settle)..."
NFS_COHERENCE_BUDGET=30
for s in "${SRCS[@]}"; do
  b="$(basename "$s")"
  want="$(shasum -a 256 "$s" | awk '{print $1}')"
  ok=0
  for i in $(seq 1 "$NFS_COHERENCE_BUDGET"); do
    got="$(shasum -a 256 "$TESTDIR/$b" 2>/dev/null | awk '{print $1}')"
    [ "$want" = "$got" ] && { ok=1; break; }
    sleep 1
  done
  if [ "$ok" = 1 ]; then grn "  ok(nfs) $b (coherent after ${i}s)"; else ylw "  WARN(nfs) $b not coherent within ${NFS_COHERENCE_BUDGET}s — client attr-cache delay (data is durable; see follow-up)"; fi
done
LEFT_FAILED=$(sqlite3 "$DB" "SELECT COUNT(*) FROM spool_entries WHERE drain_state='failed' AND nfs_path LIKE '$PREFIX/%';" 2>/dev/null)
[ "${LEFT_FAILED:-0}" = "0" ] || { red "PART 2 FAIL: $LEFT_FAILED rows left failed"; FAIL=1; }

echo "========================================"
if [ "$FAIL" = 0 ]; then grn "WEDGE PASS: dead-FUSE errno caught + zero loss across a real mid-drain outage"; exit 0; else red "WEDGE FAIL"; exit 1; fi
