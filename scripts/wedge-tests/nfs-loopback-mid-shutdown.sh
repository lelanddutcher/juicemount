#!/usr/bin/env bash
# wedge-tests/nfs-loopback-mid-shutdown.sh — closes tier-1.2 iter B/3.
#
# Scenario: a long-running read is in flight against the JuiceMount
# NFS loopback, then JuiceMount Stop fires. The acceptance criterion
# (per docs/ROADMAP/tier-1-stability.md iter B):
#
#   "Trigger Stop while a 5GB read is in flight. Expect: read errors
#    cleanly (NFS server drains and closes), unmount succeeds, no
#    kernel mount-table residue."
#
# This harness exercises Stop via the POST /stop admin endpoint
# (added in iter 12) rather than the Swift menu-bar button, so the
# whole flow is scriptable. After Stop, it optionally exercises
# /force-eject and confirms the mount is no longer in the kernel
# table.
#
# IMPORTANT: this test is SINGLE-SHOT per JuiceMount lifecycle.
# After it runs, the JuiceMount NFS server is in the stopped state.
# Restart via the Swift app's Start button (or another iteration of
# automation that knows how to restart). The harness does NOT try
# to auto-restart — Start needs cgo-side initialization that the
# Swift app handles.
#
# Prerequisites:
#
#   1. JuiceMount mount up AND the running binary supports POST /stop.
#      Pre-iter-12 binaries fall through to the catch-all index
#      handler for any unknown path; the precondition check rejects
#      stale binaries by sending GET /stop and expecting 405 (Method
#      Not Allowed). 200 means stale binary — abort, do not stop.
#
#   2. Passwordless sudo for /sbin/umount (covered by the same
#      sudoers entry as the offline-resilience harness).
#
#   3. A probe file >= 1 GB on the mount that takes >2× PRE_READ_DELAY
#      to fully stream (default: auto-discovered).
#
# Usage:
#
#   scripts/wedge-tests/nfs-loopback-mid-shutdown.sh \
#       [--mount PATH]              \ default /Volumes/zpool-dev
#       [--metrics URL]             \ default http://127.0.0.1:11050
#       [--probe-file PATH]         \ default: auto-discover a >=1GB file
#       [--read-budget SECONDS]     \ default 10  (server drain budget)
#       [--stop-respond-budget S]   \ default 5   (time for /health to start failing)
#       [--pre-read-delay-ms MS]    \ default 500 (let cat start streaming)
#       [--no-force-eject]          \ skip the /force-eject + residue-check phase
#
# Exit codes:
#   0  pass (or inconclusive — see WARN line)
#   1  fail (acceptance criterion missed)
#   2  precondition error
#
# Emits trailing `[PASS|FAIL|WARN] nfs-loopback-mid-shutdown: <details>`.

set -euo pipefail

# --- defaults ---
MOUNT="${MOUNT:-/Volumes/zpool-dev}"
METRICS="${METRICS:-http://127.0.0.1:11050}"
PROBE_FILE=""
READ_BUDGET_S=10
STOP_RESPOND_BUDGET_S=5
PRE_READ_DELAY_MS=500
SKIP_FORCE_EJECT=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mount)             MOUNT="$2"; shift 2 ;;
        --metrics)           METRICS="$2"; shift 2 ;;
        --probe-file)        PROBE_FILE="$2"; shift 2 ;;
        --read-budget)       READ_BUDGET_S="$2"; shift 2 ;;
        --stop-respond-budget) STOP_RESPOND_BUDGET_S="$2"; shift 2 ;;
        --pre-read-delay-ms) PRE_READ_DELAY_MS="$2"; shift 2 ;;
        --no-force-eject)    SKIP_FORCE_EJECT=true; shift ;;
        -h|--help)           grep '^#' "$0" | sed 's/^# \?//'; exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

# Normalize mount path: strip trailing slash so the residue-check
# pattern `mount | grep -q " $MOUNT "` matches correctly. Without
# this, --mount /Volumes/zpool-dev/ would silently false-negative on
# the precondition check AND the post-eject residue check (the latter
# would silently report no residue even when the mount is still listed).
MOUNT="${MOUNT%/}"

# --- helpers ---
ts_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }
log()  { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
pass() { printf '\033[32m[PASS]\033[0m %s\n' "$*"; }
fail() { printf '\033[31m[FAIL]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[WARN]\033[0m %s\n' "$*"; }

CAT_PID=""

# Cleanup: kill the background cat if still alive. We do NOT try to
# restart JuiceMount here — that's intentionally the user's call (or
# the calling automation's). The "leaves server stopped" property is
# part of the test's contract.
cleanup() {
    if [[ -n "$CAT_PID" ]] && kill -0 "$CAT_PID" 2>/dev/null; then
        kill -KILL "$CAT_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

# --- preconditions ---
log "== preconditions =="
log "  mount:                 $MOUNT"
log "  metrics:               $METRICS"
log "  read budget:           ${READ_BUDGET_S}s   (cat must error within this many s of /stop)"
log "  stop-respond budget:   ${STOP_RESPOND_BUDGET_S}s   (/health must start failing within this)"
log "  force-eject phase:     $([ "$SKIP_FORCE_EJECT" = "true" ] && echo SKIPPED || echo enabled)"
log ""

if ! mount | grep -q " $MOUNT "; then
    fail "mount $MOUNT is not active"
    echo "[FAIL] nfs-loopback-mid-shutdown: precondition (mount inactive)"
    exit 2
fi
log "  ✓ mount is active"

# /stop endpoint sniff: GET /stop on a fresh binary returns 405; on a
# stale binary the catch-all handler returns 200 with text/plain. One
# curl call captures both status and headers so we don't open a TOCTOU
# window between two requests.
stop_probe=$(curl -s -D - -o /dev/null -X GET "$METRICS/stop" --max-time 2 2>/dev/null || echo "")
stop_probe_status=$(printf '%s\n' "$stop_probe" | head -1 | awk '{print $2}')
stop_probe_ctype=$(printf '%s\n' "$stop_probe" | grep -i '^content-type:' | head -1 | tr -d '\r' | awk '{print tolower($2)}')
if [[ "$stop_probe_status" != "405" ]]; then
    fail "POST /stop not supported on the running binary (GET returned ${stop_probe_status:-no-response}, ctype=$stop_probe_ctype)"
    fail "  expected 405 from a fresh binary that has handleStopHTTP (iter 12)."
    fail "  the running binary is likely pre-iter-12 — quit JuiceMount and re-open build/JuiceMount.app"
    echo "[FAIL] nfs-loopback-mid-shutdown: precondition (stale binary — no /stop endpoint)"
    exit 2
fi
log "  ✓ POST /stop endpoint detected (GET returns 405 — fresh binary)"

# Health must be OK before we start. A degraded health (e.g. Redis
# already unreachable, auto-offline already engaged) would confuse the
# test — we couldn't tell whether the read errored because /stop tore
# down the server or because something was already broken.
health_status=$(curl -s -o /dev/null -w '%{http_code}' "$METRICS/health" --max-time 2 2>/dev/null || echo "000")
if [[ "$health_status" != "200" ]]; then
    fail "/health returned $health_status (expected 200). Mount is degraded — fix before running this test."
    fail "  current health: $(curl -s $METRICS/health --max-time 2 2>/dev/null | head -200)"
    echo "[FAIL] nfs-loopback-mid-shutdown: precondition (server health degraded)"
    exit 2
fi
log "  ✓ /health reports healthy"

# Auto-rotate probe file (reuse the same /tmp state file as the MinIO
# harness — same rotation guarantee). Default >= 1 GB so cat takes
# meaningfully longer than the test window even with prefetched chunks.
LAST_PROBE_STATE="/tmp/jmwedge-last-probe"
if [[ -z "$PROBE_FILE" ]]; then
    log "  no --probe-file specified; auto-discovering (>=1GB, rotating)..."
    last_used=""
    [[ -f "$LAST_PROBE_STATE" ]] && last_used=$(cat "$LAST_PROBE_STATE" 2>/dev/null)
    while IFS= read -r candidate; do
        if [[ -n "$candidate" && "$candidate" != "$last_used" ]]; then
            PROBE_FILE="$candidate"
            break
        fi
    done < <(find "$MOUNT" -type f -size +1G -not -path '*/.*' 2>/dev/null)
    if [[ -z "$PROBE_FILE" ]]; then
        PROBE_FILE="$last_used"
    fi
    if [[ -z "$PROBE_FILE" ]]; then
        fail "no probe file found under $MOUNT (need a file >= 1GB)"
        echo "[FAIL] nfs-loopback-mid-shutdown: precondition (no large probe file)"
        exit 2
    fi
fi
echo "$PROBE_FILE" >"$LAST_PROBE_STATE" 2>/dev/null || true
if [[ ! -f "$PROBE_FILE" ]]; then
    fail "probe file does not exist: $PROBE_FILE"
    echo "[FAIL] nfs-loopback-mid-shutdown: precondition (probe-file missing)"
    exit 2
fi
PROBE_SIZE=$(stat -f%z "$PROBE_FILE" 2>/dev/null || echo 0)
log "  ✓ probe file: $PROBE_FILE ($((PROBE_SIZE / 1024 / 1024)) MB)"

# --- the test ---
log ""
log "== nfs-loopback-mid-shutdown =="

# Start the streaming read.
log "  starting background cat..."
cat "$PROBE_FILE" >/dev/null 2>&1 &
CAT_PID=$!

# Let cat get past the open() and into actual byte streaming.
sleep $(printf '%.3f' "$(echo "scale=3; $PRE_READ_DELAY_MS / 1000" | bc)")

if ! kill -0 "$CAT_PID" 2>/dev/null; then
    early_exit=0; wait "$CAT_PID" 2>/dev/null || early_exit=$?
    warn "cat finished before /stop could fire (exit $early_exit) — probe file likely cached"
    echo "[WARN] nfs-loopback-mid-shutdown: inconclusive (probe finished before /stop)"
    exit 0
fi
log "  ✓ cat streaming (PID $CAT_PID)"

# Trigger /stop. The handler returns immediately ({"ok":true,
# "stopping":true}) and spawns a goroutine that waits 100ms then
# tears down. So the response should be fast, then the server
# starts dying about 100ms later.
log "  POSTing /stop..."
stop_response=$(curl -fsS -X POST "$METRICS/stop" --max-time 3 2>&1 || echo "ERROR: curl failed")
stop_engaged_ms=$(ts_ms)
log "  ✓ /stop response: $stop_response"
if [[ "$stop_response" != *'"stopping":true'* ]]; then
    fail "unexpected /stop response — server may not have begun teardown"
    echo "[FAIL] nfs-loopback-mid-shutdown: /stop did not return expected JSON"
    exit 1
fi

# Wait for cat to exit. We give it READ_BUDGET_S — the server's
# graceful drain may take a few seconds after /stop fires; the kernel
# NFS client then sees the connection close and propagates an error
# to the read syscall.
read_deadline_ms=$((stop_engaged_ms + READ_BUDGET_S * 1000))
cat_exit=0
cat_wedged=false
while kill -0 "$CAT_PID" 2>/dev/null; do
    if [[ $(ts_ms) -gt $read_deadline_ms ]]; then
        cat_wedged=true
        kill -TERM "$CAT_PID" 2>/dev/null || true
        sleep 0.5
        kill -KILL "$CAT_PID" 2>/dev/null || true
        break
    fi
    sleep 0.1
done
wait "$CAT_PID" 2>/dev/null || cat_exit=$?
read_elapsed_ms=$(($(ts_ms) - stop_engaged_ms))
read_elapsed_s=$(printf '%.2f' "$(echo "scale=3; $read_elapsed_ms / 1000" | bc)")
CAT_PID=""
log "  cat exit: $cat_exit  after ${read_elapsed_ms}ms post-stop  (wedged=$cat_wedged)"

# Inconclusive guard: if cat exited 0 (clean EOF) without us killing
# it, the probe file was small enough to fully stream from cache
# between the alive-check and /stop firing. We have NO evidence the
# server drained reads under wedge — the test couldn't observe it.
# Mirror the MinIO harness's inconclusive branch rather than report
# a structurally-valid-but-meaningless PASS.
if [[ "$cat_wedged" == "false" && "$cat_exit" == "0" ]]; then
    warn "cat exited 0 — probe was likely fully cached and finished before /stop drained"
    warn "  the server may still need restart (it's stopped). Click Start in JuiceMount."
    echo "[WARN] nfs-loopback-mid-shutdown: inconclusive (cat exited 0 in ${read_elapsed_s}s)"
    exit 0
fi

# Wait for /health to start failing. The metrics server is the first
# thing torn down in stopServerLocked, so /health should become
# unreachable (connection refused) within a fraction of a second.
log "  polling /health for teardown..."
health_failed_ms=0
health_deadline_ms=$((stop_engaged_ms + STOP_RESPOND_BUDGET_S * 1000))
while true; do
    now_ms=$(ts_ms)
    if [[ $now_ms -gt $health_deadline_ms ]]; then
        break
    fi
    health_code=$(curl -s -o /dev/null -w '%{http_code}' "$METRICS/health" --max-time 1 2>/dev/null || echo "000")
    if [[ "$health_code" == "000" ]] || [[ "$health_code" == "503" ]]; then
        health_failed_ms=$((now_ms - stop_engaged_ms))
        break
    fi
    sleep 0.2
done
if [[ $health_failed_ms -gt 0 ]]; then
    log "  ✓ /health stopped responding after ${health_failed_ms}ms (budget ${STOP_RESPOND_BUDGET_S}s)"
else
    log "  /health still responding after ${STOP_RESPOND_BUDGET_S}s"
fi

# Optional: /force-eject to clean up the mount table.
ejected=false
residue=false
if [[ "$SKIP_FORCE_EJECT" == "false" ]]; then
    log ""
    log "  /force-eject (clean up kernel mount table)..."
    # /force-eject lives on the same listener as /stop — but /stop just
    # tore down the metrics server. We expect this to fail with
    # connection refused. That's actually correct behavior — soft-stop
    # tears down everything including the listener.
    fe_code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$METRICS/force-eject" --max-time 3 2>/dev/null || echo "000")
    log "  /force-eject HTTP status: $fe_code"
    if [[ "$fe_code" == "000" ]]; then
        log "  metrics listener is down (expected after /stop). Falling back to direct umount."
        # The mount is still in the kernel table. Use the same
        # passwordless-sudo umount path that /force-eject would have used.
        if sudo -n /sbin/umount -f -t nfs "$MOUNT" 2>/dev/null; then
            ejected=true
            log "  ✓ direct umount succeeded"
        elif sudo -n /sbin/umount -f "$MOUNT" 2>/dev/null; then
            ejected=true
            log "  ✓ direct umount (unscoped -f) succeeded"
        else
            log "  direct umount failed — mount may need manual cleanup"
        fi
    elif [[ "$fe_code" == "200" ]]; then
        ejected=true
    fi

    # Residue check: mount table should not show $MOUNT.
    if mount | grep -q " $MOUNT "; then
        residue=true
        log "  RESIDUE: $MOUNT still in mount table"
    else
        log "  ✓ no kernel mount-table residue"
    fi
fi

# --- verdict ---
#
# PASS requires ALL of:
#   - cat exited (cat_wedged=false) — server drained reads, didn't hang
#   - cat exited within READ_BUDGET_S of /stop firing
#   - /health stopped responding within STOP_RESPOND_BUDGET_S
#   - if force-eject phase ran: mount table is clean

verdict_pass=true
verdict_details=""
read_budget_ms=$((READ_BUDGET_S * 1000))

if [[ "$cat_wedged" == "true" ]]; then
    verdict_pass=false
    verdict_details="cat never exited after /stop — server did not drain reads (wedged for ${READ_BUDGET_S}s+)"
elif [[ $read_elapsed_ms -gt $read_budget_ms ]]; then
    verdict_pass=false
    verdict_details="cat took ${read_elapsed_s}s to error (budget ${READ_BUDGET_S}s)"
fi

if [[ $health_failed_ms -eq 0 ]]; then
    msg="/health still responding after ${STOP_RESPOND_BUDGET_S}s — metrics server didn't tear down"
    if [[ "$verdict_pass" == "true" ]]; then
        verdict_pass=false
        verdict_details="$msg"
    else
        verdict_details="$verdict_details; $msg"
    fi
fi

if [[ "$SKIP_FORCE_EJECT" == "false" ]] && [[ "$residue" == "true" ]]; then
    msg="kernel mount table still shows $MOUNT after force-eject"
    if [[ "$verdict_pass" == "true" ]]; then
        verdict_pass=false
        verdict_details="$msg"
    else
        verdict_details="$verdict_details; $msg"
    fi
fi

log ""
log "  NOTE: JuiceMount NFS server is now in the stopped state. Restart"
log "        via the Swift app's Start button before running other tests."
log ""

if [[ "$verdict_pass" == "true" ]]; then
    pass "nfs-loopback-mid-shutdown: read errored in ${read_elapsed_s}s; /health down in ${health_failed_ms}ms; eject=$ejected residue=$residue"
    echo "[PASS] nfs-loopback-mid-shutdown: read=${read_elapsed_s}s health_down=${health_failed_ms}ms ejected=$ejected residue=$residue"
    exit 0
else
    fail "nfs-loopback-mid-shutdown: $verdict_details"
    echo "[FAIL] nfs-loopback-mid-shutdown: $verdict_details (read=${read_elapsed_s}s health_down=${health_failed_ms}ms)"
    exit 1
fi
