# Shared helpers for 11-workloads/* perf scripts. Sourced, not executed.
#
# Contract — every workload script:
#   1. source this file to get snapshot_metrics_to / build_summary
#   2. snapshot before the workload (snapshot_metrics_to "$ART/before.json")
#   3. run the workload for DURATION seconds
#   4. snapshot after (snapshot_metrics_to "$ART/after.json")
#   5. call build_summary "$ART/before.json" "$ART/after.json" "$ART/summary.json"
#
# All output goes to $ART (per-run artifact dir provided by orchestrator
# or by lib.sh's phase_init). The summary.json is the regression-check
# input.

# JMCTL path — built into /tmp by orchestrator at start of run.
JMCTL="${JMCTL:-/tmp/jmctl}"

# Snapshot full /metrics JSON to a file.
snapshot_metrics_to() {
    local out="$1"
    curl -sS --max-time 5 "http://${JM_METRICS_ADDR:-127.0.0.1:11050}/metrics" > "$out"
}

# Snapshot JM log line count (so we can grep just the workload window).
log_line_count() {
    wc -l < "$HOME/Library/Logs/JuiceMount/juicemount.log" 2>/dev/null || echo 0
}

# Count FromHandle STALE events written to the JM log between two line numbers.
stale_events_in_range() {
    local start="$1"; local end="$2"
    if [ -z "$start" ] || [ -z "$end" ] || [ "$end" -le "$start" ]; then
        echo 0
        return
    fi
    sed -n "${start},${end}p" "$HOME/Library/Logs/JuiceMount/juicemount.log" 2>/dev/null \
        | grep -c "FromHandle STALE"
}

# Build summary.json from before/after metrics snapshots.
# Args: before.json  after.json  out.json  [workload_name]  [duration_sec]  [stale_count]
build_summary() {
    local before="$1"; local after="$2"; local out="$3"
    local workload="${4:-unknown}"; local dur="${5:-0}"; local stale="${6:-0}"
    local sha
    sha="$(cd "$(dirname "$0")"/../../.. && git rev-parse --short HEAD 2>/dev/null || echo unknown)"

    python3 - "$before" "$after" "$out" "$workload" "$dur" "$sha" "$stale" <<'PY'
import json, sys, datetime
before_p, after_p, out_p, workload, dur, sha, stale = sys.argv[1:]
with open(before_p) as f: b = json.load(f)
with open(after_p) as f:  a = json.load(f)
def num(d, k, default=0):
    v = d.get(k, default)
    return v if isinstance(v,(int,float)) else default
bytes_read_delta    = num(a, "bytes_read")    - num(b, "bytes_read")
bytes_written_delta = num(a, "bytes_written") - num(b, "bytes_written")
rpc_errors_delta    = num(a, "rpc_errors")    - num(b, "rpc_errors")
rpcs_out = {}
for k, ra in (a.get("rpcs") or {}).items():
    rb = (b.get("rpcs") or {}).get(k) or {}
    cd = num(ra,"count") - num(rb,"count")
    if cd <= 0:
        continue
    rpcs_out[k] = {
        "count_delta": cd,
        "mean_us": ra.get("mean_us"),
        "p50_us":  ra.get("p50_us"),
        "p95_us":  ra.get("p95_us"),
        "p99_us":  ra.get("p99_us"),
        "max_us":  ra.get("max_us"),
    }
dur_f = float(dur or 1.0)
summary = {
    "workload":           workload,
    "build_sha":          sha,
    "duration_sec":       float(dur_f),
    "started_at":         datetime.datetime.utcfromtimestamp(
                              datetime.datetime.utcnow().timestamp() - dur_f
                          ).isoformat() + "Z",
    "bytes_read_delta":   bytes_read_delta,
    "bytes_written_delta": bytes_written_delta,
    "throughput_MBps":    round((bytes_read_delta + bytes_written_delta) / dur_f / 1_000_000, 2),
    "rpcs":               rpcs_out,
    "rpc_errors_delta":   rpc_errors_delta,
    "from_handle_stale_events": int(stale),
}
with open(out_p, "w") as f:
    json.dump(summary, f, indent=2)
print(out_p)
PY
}
