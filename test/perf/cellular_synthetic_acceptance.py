#!/usr/bin/env python3
"""Bounded, metadata-only synthetic-cellular acceptance runner.

The default invocation is a dry run: it validates the local test assets and
prints the plan without touching the app, mount, backend, or network.  Passing
``--lifecycle`` explicitly permits a reversible app restart.  In that mode the
runner launches JuiceMount behind ``test/latency-proxy.py``, which applies real
latency/bandwidth impairment to Redis and enforces a hard aggregate byte cap.

Only the mount root is enumerated.  Directory-entry names are counted and then
discarded; no child is statted or opened, and no source path or endpoint is
written to stdout or the result document. Object-store, cache-miss, cold-fetch,
thumbnail, and prefetch counters are remote-hydration gates. Local cached NFS
READs from macOS remain visible in the report but are not mislabeled as
cellular downloads when the backing-store counters prove that no remote bytes
were fetched.

This is a synthetic metadata-link test, not a substitute for a final real
cellular run.  Every result says that explicitly.
"""

import argparse
import concurrent.futures
import errno
import ipaddress
import json
import math
import os
import pathlib
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

from lib import harness as perf_harness


ROOT = pathlib.Path(__file__).resolve().parents[2]
PROXY_SOURCE = ROOT / "test" / "latency-proxy.py"
DEFAULT_BUILD_APP = ROOT / "build" / "JuiceMount.app"
DEFAULT_INSTALLED_APP = pathlib.Path("/Applications/JuiceMount.app")
DEFAULT_MOUNT = "/Volumes/zpool"
DEFAULT_CONTROL = "http://127.0.0.1:11050"
DEFAULT_PROXY_LISTEN = "127.0.0.1:16379"
CONTENT_GUARD_ENV = "JM_TEST_REFUSE_FUSE_CONTENT_READS"
TEST_ONLY_ENV_KEYS = (
    "JM_DEBUG_META_ADDR",
    "JM_NET_FAKE_LINK",
    CONTENT_GUARD_ENV,
)

MIN_BUDGET_BYTES = 256 * 1024
MAX_BUDGET_BYTES = 32 * 1024 * 1024
MIN_RECONNECT_BUDGET = 128 * 1024
# The mandatory NFS write-readiness check writes exactly one 4 KiB sentinel,
# fsyncs it, and deletes it. On macOS the NFS client can also materialize one
# 4 KiB AppleDouble companion for that sentinel. These are the only startup
# write shapes admitted below: downloads remain forbidden, and the NFS
# mutation counters must independently match either the sentinel-only or the
# sentinel-plus-AppleDouble signature rather than an arbitrary upload.
READINESS_PROBE_PAYLOAD_BYTES = 4 * 1024
READINESS_PROBE_APPLEDOUBLE_BYTES = 4 * 1024
READINESS_PROBE_MAX_WRITE_BYTES = (
    READINESS_PROBE_PAYLOAD_BYTES + READINESS_PROBE_APPLEDOUBLE_BYTES)
READINESS_PROBE_SIGNATURES = (
    (READINESS_PROBE_PAYLOAD_BYTES, 1, 1, 1),
    (READINESS_PROBE_MAX_WRITE_BYTES, 2, 2, 2),
)
REQUIRED_BACKEND_COUNTERS = (
    "object_get_bytes", "object_put_bytes", "cache_miss_bytes", "meta_ops",
)
# Backend counters are scraped at most once every two seconds. Requiring an
# unchanged data-plane fingerprint for longer than that interval prevents the
# probe's asynchronous publish from leaking into the navigation measurement.
STARTUP_QUIESCENCE_SEC = 2.5
STARTUP_QUIESCENCE_TIMEOUT_SEC = 15.0
PRODUCT_RE = re.compile(r"^(.*?/Contents/MacOS/JuiceMount)(?:\s|$)")

# App quit is asynchronous: AppKit returns terminateLater while the Go bridge
# disarms its write verifier, unmounts NFS, drains the server/spool, and
# unmounts FUSE. The noninteractive NFS escalation alone has bounded attempts
# of 5 + 5 + 15 + 15 seconds before its privileged fallback, so the old 8 s
# grace guaranteed SIGTERM could land in a legitimate teardown. Ninety seconds
# covers those attempts plus verifier/server drain headroom while still bounding
# acceptance cleanup well below an unattended human-scale wait.
QUIT_REQUEST_TIMEOUT_SEC = 8.0
GRACEFUL_PRODUCT_TEARDOWN_SEC = 90.0
PRODUCT_TERM_GRACE_SEC = 15.0
PRODUCT_KILL_GRACE_SEC = 5.0


class AcceptanceError(RuntimeError):
    """Expected failure whose stable code is safe to publish."""

    def __init__(self, code):
        super().__init__(code)
        self.code = code


class SafeArgumentParser(argparse.ArgumentParser):
    """Argparse variant that never echoes a private CLI value on errors."""

    def error(self, _message):
        raise AcceptanceError("argument_parse_failed")


def _safe_get_json(control, path, timeout=4.0):
    """Read one loopback JSON endpoint; never return exception text."""
    try:
        req = urllib.request.Request(control.rstrip("/") + path,
                                     headers={"Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        try:
            return json.load(error)
        except Exception:
            return None
    except Exception:
        return None


def _percentile(values, pct):
    xs = sorted(float(value) for value in values)
    if not xs:
        return None
    if len(xs) == 1:
        return xs[0]
    rank = (len(xs) - 1) * pct / 100.0
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return xs[lo]
    return xs[lo] + (xs[hi] - xs[lo]) * (rank - lo)


def _timing_stats(values):
    xs = [round(float(value), 3) for value in values]
    if not xs:
        return {"n": 0, "min_ms": None, "p50_ms": None,
                "p95_ms": None, "max_ms": None}
    base = perf_harness.stats(xs)
    return {
        "n": base["n"],
        "min_ms": base["min"],
        "p50_ms": round(base["median"], 3),
        "p95_ms": round(_percentile(xs, 95), 3),
        "max_ms": base["max"],
    }


def _validate_loopback_http(value):
    parsed = urllib.parse.urlparse(value)
    try:
        host = ipaddress.ip_address(parsed.hostname or "")
    except ValueError:
        raise AcceptanceError("control_must_be_numeric_loopback")
    if parsed.scheme != "http" or not host.is_loopback or parsed.path not in ("", "/"):
        raise AcceptanceError("control_must_be_loopback_http")
    if parsed.username or parsed.password or not parsed.port:
        raise AcceptanceError("control_url_invalid")
    return value.rstrip("/")


def _split_host_port(value, code):
    """Parse the proxy's deliberately simple host:port syntax."""
    if not value or any(token in value for token in ("/", "@", " ")):
        raise AcceptanceError(code)
    host, separator, port_text = value.rpartition(":")
    if not separator or not host:
        raise AcceptanceError(code)
    try:
        port = int(port_text)
    except ValueError:
        raise AcceptanceError(code)
    if not 1 <= port <= 65535:
        raise AcceptanceError(code)
    return host, port


def _validate_proxy_listen(value):
    host, port = _split_host_port(value, "proxy_listen_invalid")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        raise AcceptanceError("proxy_listen_must_be_numeric_loopback")
    if not address.is_loopback:
        raise AcceptanceError("proxy_listen_must_be_loopback")
    return host, port


def _validate_redis_target(value):
    return _split_host_port(value, "redis_target_invalid")


def _loopback_port_available(value):
    host, port = _validate_proxy_listen(value)
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind((host, port))
        return True
    except OSError:
        return False
    finally:
        probe.close()


def _validate_output_location(json_out, mount_root):
    if not json_out:
        return
    output = pathlib.Path(json_out).expanduser().resolve()
    mount = pathlib.Path(mount_root).expanduser().resolve()
    try:
        under_mount = os.path.commonpath((str(output), str(mount))) == str(mount)
    except ValueError:
        under_mount = False
    if under_mount:
        raise AcceptanceError("result_file_must_not_be_on_mount")


def _proxy_contract_ok():
    try:
        source = PROXY_SOURCE.read_text(encoding="utf-8")
    except OSError:
        return False
    required = ("--max-forward-bytes", "--stats-file", "--delay-ms",
                "--jitter-ms", "--down-mbps", "--up-mbps")
    return all(flag in source for flag in required)


def _base_report(args, mode):
    return {
        "schema": 1,
        "name": "juicemount_cellular_metadata_acceptance",
        "status": "DRY_RUN" if mode == "dry-run" else "RUNNING",
        "mode": mode,
        "measurement": {
            "simulated": True,
            "scope": "metadata_link_only",
            "real_cellular": False,
            "comparable_to_real_cellular": False,
            "profile": {
                "rtt_ms": round(args.delay_ms * 2.0, 3),
                "jitter_ms_each_direction": args.jitter_ms,
                "down_mbps": args.down_mbps,
                "up_mbps": args.up_mbps,
                "loss_pct": 0.0,
            },
        },
        "cold_scope": {
            "process_restart": True,
            "persistent_metadata_and_block_cache_preserved": True,
            "zero_app_and_mount_state_required_before_launch": True,
            "all_startup_timings_share_launch_command_origin": True,
        },
        "safety": {
            "metadata_only": True,
            "metadata_only_workload_generated_by_runner": True,
            "ambient_local_cached_nfs_reads_reported_separately": True,
            "metadata_only_scope_warning": (
                "the runner opens no child content; macOS may still issue "
                "local-cache NFS reads, which are reported separately"),
            "source_paths_in_output": False,
            "opens_media": False,
            "metadata_proxy_hard_forwarded_byte_budget": args.budget_bytes,
            "object_bytes_hard_limited": True,
            "zero_fuse_content_fallthrough_guard": True,
            "object_download_bytes_post_hoc_zero_gate": True,
            "navigation_remote_object_get_and_put_zero_gate": True,
            "readiness_probe_upload_allowance_bytes":
                READINESS_PROBE_MAX_WRITE_BYTES,
            "default_is_non_mutating": True,
            "lifecycle_explicitly_authorized": bool(args.lifecycle),
        },
        "failures": [],
    }


def _assert_report_redacted(report, sensitive_values):
    """Fail closed before emitting an artifact containing private topology."""
    encoded = json.dumps(report, sort_keys=True)
    for value in sensitive_values:
        if value and str(value) in encoded:
            raise AcceptanceError("result_redaction_guard_failed")
    forbidden_keys = {"path", "source_path", "mount_root", "redis_target",
                      "proxy_listen", "control_url", "app_path", "endpoint"}

    def visit(value):
        if isinstance(value, dict):
            if forbidden_keys.intersection(value):
                raise AcceptanceError("result_redaction_guard_failed")
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(report)


def _emit(report, args):
    sensitive = (args.mount_root, args.redis_target, args.proxy_listen,
                 args.control, args.app)
    _assert_report_redacted(report, sensitive)
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.json_out:
        _validate_output_location(args.json_out, args.mount_root)
        output = pathlib.Path(args.json_out).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(output.name + ".tmp")
        temporary.write_text(text, encoding="utf-8")
        os.replace(temporary, output)
    sys.stdout.write(text)


def _dry_run(args):
    report = _base_report(args, "dry-run")
    app = _resolve_test_app(args.app, [])
    report["prerequisites"] = {
        "bounded_proxy_contract": _proxy_contract_ok(),
        "app_bundle_available": bool(app and _app_binary(app).is_file()),
        "lifecycle_target_supplied": bool(args.redis_target),
    }
    report["planned_coverage"] = [
        "verified_zero_app_nfs_and_fuse_prelaunch_state",
        "launch_to_process_control_serving_and_first_root_timings",
        "process_cold_control_readiness",
        "serving_and_warm_phase_transition",
        "cached_root_metadata_navigation",
        "concurrent_root_metadata_navigation",
        "bounded_metadata_link_reconnect" if args.reconnect else "reconnect_opted_out",
        "zero_remote_hydration_and_bounded_readiness_upload_gate",
        "zero_fuse_content_fallthrough_guard",
        "reported_ambient_local_cached_nfs_reads",
        "normal_app_state_restore",
    ]
    report["status"] = "DRY_RUN"
    return report


def _enumerate_root_metadata(root):
    """Time one READDIR and discard every entry without reading its name."""
    started = time.perf_counter()
    count = 0
    try:
        with os.scandir(root) as entries:
            for _entry in entries:
                count += 1
    except OSError:
        raise AcceptanceError("root_metadata_navigation_failed")
    return (time.perf_counter() - started) * 1000.0, count


def _navigation_sample(root, samples, concurrency, per_worker,
                       launch_started=None):
    cold_ms, cold_count = _enumerate_root_metadata(root)
    first_root_from_launch_ms = None
    if launch_started is not None:
        first_root_from_launch_ms = round(
            (time.monotonic() - launch_started) * 1000.0, 3)
    warm = []
    counts = [cold_count]
    for _ in range(samples):
        elapsed, count = _enumerate_root_metadata(root)
        warm.append(elapsed)
        counts.append(count)

    concurrent_times = []

    def work():
        local = []
        for _ in range(per_worker):
            local.append(_enumerate_root_metadata(root))
        return local

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            for rows in pool.map(lambda _index: work(), range(concurrency)):
                for elapsed, count in rows:
                    concurrent_times.append(elapsed)
                    counts.append(count)
    except AcceptanceError:
        raise
    except Exception:
        raise AcceptanceError("concurrent_metadata_navigation_failed")

    result = {
        "cold_root_ms": round(cold_ms, 3),
        "warm_root": _timing_stats(warm),
        "concurrent_root": _timing_stats(concurrent_times),
        "workers": concurrency,
        "requests_per_worker": per_worker,
        "entry_count": cold_count,
        "entry_count_consistent": len(set(counts)) == 1,
    }
    if first_root_from_launch_ms is not None:
        result["first_cached_root_from_launch_ms"] = first_root_from_launch_ms
    return result


def _counter(snapshot, *keys):
    value = snapshot
    for key in keys:
        if not isinstance(value, dict):
            return 0
        value = value.get(key, 0)
    if (isinstance(value, bool) or
            not isinstance(value, (int, float)) or
            not math.isfinite(value)):
        return 0
    return value


def _backend_counters_available(snapshot):
    backend = snapshot.get("backend") if isinstance(snapshot, dict) else None
    if not isinstance(backend, dict):
        return False
    for key in REQUIRED_BACKEND_COUNTERS:
        value = backend.get(key)
        # bool is an int subclass but is never a valid byte/activity counter.
        if (isinstance(value, bool) or
                not isinstance(value, (int, float)) or
                not math.isfinite(value) or value < 0):
            return False
    return True


def _metrics_gate(before, after):
    backend_available = (_backend_counters_available(before) and
                         _backend_counters_available(after))

    def delta(*keys):
        return int(_counter(after, *keys) - _counter(before, *keys))

    def backend_delta(key):
        # Missing/partial/nonfinite backend schemas are an explicit gate
        # failure. Do not attempt arithmetic on them or manufacture a usable
        # delta; zero here is diagnostic only and cannot pass while the
        # availability flag is false.
        return delta("backend", key) if backend_available else 0

    result = {
        "backend_counters_available": backend_available,
        "object_get_bytes_delta": backend_delta("object_get_bytes"),
        "object_put_bytes_delta": backend_delta("object_put_bytes"),
        "cache_miss_bytes_delta": backend_delta("cache_miss_bytes"),
        "meta_ops_delta": backend_delta("meta_ops"),
        "nfs_read_bytes_delta": delta("bytes_read"),
        "nfs_read_rpc_delta": delta("rpcs", "READ", "count"),
        "cold_subread_delta": delta("read_cold_subread"),
        "warm_subread_delta": delta("read_warm_subread"),
        "thumbnail_hydration_delta": delta("thumb_warm_hydrated"),
        "prefetched_block_delta": delta("readahead_prefetched_blocks"),
        "readdir_mirror_hit_delta": delta("readdir_mirror_hit"),
        "redis_round_trips_delta": delta("redis", "round_trips"),
    }
    result["local_cached_nfs_read_observed"] = (
        result["nfs_read_bytes_delta"] != 0 or
        result["nfs_read_rpc_delta"] != 0 or
        result["warm_subread_delta"] != 0)
    result["remote_hydration_observed"] = _remote_hydration_observed(result)
    return result


def _remote_hydration_observed(counters):
    """Return true only for counters that prove remote content hydration."""
    return any(int(counters.get(key, 0) or 0) != 0 for key in (
        "object_get_bytes_delta", "cache_miss_bytes_delta",
        "cold_subread_delta", "thumbnail_hydration_delta",
        "prefetched_block_delta"))


def _startup_data_fingerprint(snapshot):
    """Return only counters that can reveal startup data-plane activity."""
    return (
        int(_counter(snapshot, "backend", "object_get_bytes")),
        int(_counter(snapshot, "backend", "object_put_bytes")),
        int(_counter(snapshot, "backend", "cache_miss_bytes")),
        int(_counter(snapshot, "bytes_read")),
        int(_counter(snapshot, "bytes_written")),
        int(_counter(snapshot, "rpcs", "READ", "count")),
        int(_counter(snapshot, "rpcs", "WRITE", "count")),
        int(_counter(snapshot, "rpcs", "CREATE", "count")),
        int(_counter(snapshot, "rpcs", "REMOVE", "count")),
        int(_counter(snapshot, "rpc_errors")),
        int(_counter(snapshot, "read_cold_subread")),
        int(_counter(snapshot, "read_warm_subread")),
        int(_counter(snapshot, "thumb_warm_hydrated")),
        int(_counter(snapshot, "readahead_prefetched_blocks")),
    )


def _wait_startup_data_quiescent(control, initial):
    """Wait through one complete backend-scrape interval before navigation."""
    started = time.monotonic()
    deadline = started + STARTUP_QUIESCENCE_TIMEOUT_SEC
    current = initial
    fingerprint = (_startup_data_fingerprint(current)
                   if _backend_counters_available(current) else None)
    stable_since = started
    while time.monotonic() < deadline:
        time.sleep(0.5)
        sample = _safe_get_json(control, "/metrics", timeout=2)
        if not _backend_counters_available(sample):
            continue
        candidate = _startup_data_fingerprint(sample)
        now = time.monotonic()
        if candidate != fingerprint:
            fingerprint = candidate
            stable_since = now
        current = sample
        if now - stable_since >= STARTUP_QUIESCENCE_SEC:
            return current, round((now - started) * 1000.0, 3)
    raise AcceptanceError("startup_data_plane_not_quiescent")


def _startup_probe_gate(snapshot):
    """Admit the bounded write probe and local reads; reject remote hydration.

    This deliberately does not infer attribution from object bytes alone. The
    upload allowance is valid only when the NFS counters show the 4 KiB probe's
    create/write/remove signature, optionally paired with its one bounded
    AppleDouble companion. NFS READ and warm-cache counters are reported but
    are not network evidence. Object GETs, cache misses, cold subreads,
    thumbnail hydration, or readahead remain failures even at one byte.
    """
    available = _backend_counters_available(snapshot)
    observed = {
        "object_get_bytes": int(_counter(snapshot, "backend", "object_get_bytes")),
        "object_put_bytes": int(_counter(snapshot, "backend", "object_put_bytes")),
        "cache_miss_bytes": int(_counter(snapshot, "backend", "cache_miss_bytes")),
        "nfs_read_bytes": int(_counter(snapshot, "bytes_read")),
        "nfs_write_bytes": int(_counter(snapshot, "bytes_written")),
        "nfs_read_rpcs": int(_counter(snapshot, "rpcs", "READ", "count")),
        "nfs_write_rpcs": int(_counter(snapshot, "rpcs", "WRITE", "count")),
        "nfs_create_rpcs": int(_counter(snapshot, "rpcs", "CREATE", "count")),
        "nfs_remove_rpcs": int(_counter(snapshot, "rpcs", "REMOVE", "count")),
        "rpc_errors": int(_counter(snapshot, "rpc_errors")),
        "cold_subreads": int(_counter(snapshot, "read_cold_subread")),
        "warm_subreads": int(_counter(snapshot, "read_warm_subread")),
        "thumbnail_hydrations": int(_counter(snapshot, "thumb_warm_hydrated")),
        "prefetched_blocks": int(_counter(snapshot, "readahead_prefetched_blocks")),
    }
    remote_hydration = any(observed[key] != 0 for key in (
        "object_get_bytes", "cache_miss_bytes", "cold_subreads",
        "thumbnail_hydrations", "prefetched_blocks"))
    local_cached_read = any(observed[key] != 0 for key in (
        "nfs_read_bytes", "nfs_read_rpcs", "warm_subreads"))
    mutation_shape = (
        observed["nfs_write_bytes"], observed["nfs_write_rpcs"],
        observed["nfs_create_rpcs"], observed["nfs_remove_rpcs"])
    mutation_signature = (
        mutation_shape in READINESS_PROBE_SIGNATURES and
        observed["rpc_errors"] == 0)
    upload_in_envelope = (
        0 <= observed["object_put_bytes"] <= READINESS_PROBE_MAX_WRITE_BYTES)
    return {
        "backend_counters_available": available,
        "expected_probe_payload_bytes": READINESS_PROBE_PAYLOAD_BYTES,
        "appledouble_companion_allowance_bytes":
            READINESS_PROBE_APPLEDOUBLE_BYTES,
        "max_probe_upload_bytes": READINESS_PROBE_MAX_WRITE_BYTES,
        "observed": observed,
        "mutation_signature_matches": mutation_signature,
        "remote_hydration_observed": remote_hydration,
        "local_cached_nfs_read_observed": local_cached_read,
        "upload_within_probe_envelope": upload_in_envelope,
        "status": "PASS" if (available and mutation_signature and
                              upload_in_envelope and
                              not remote_hydration) else "FAIL",
    }


def _startup_probe_failure_codes(gate):
    """Translate the startup gate into stable failures without aborting.

    Once startup traffic is quiescent it is safe and diagnostically useful to
    continue the metadata-only navigation and reconnect phases. The final run
    still fails closed for every startup anomaly, while retaining the later
    measurements needed to distinguish startup contamination from navigation
    or recovery regressions.
    """
    failures = []
    if not gate.get("backend_counters_available"):
        failures.append("backend_byte_counters_unavailable")
    if gate.get("remote_hydration_observed"):
        failures.append("startup_object_download_observed")
    if not gate.get("upload_within_probe_envelope"):
        failures.append("startup_upload_exceeded_readiness_probe")
    if not gate.get("mutation_signature_matches"):
        failures.append("startup_readiness_probe_signature_invalid")
    return failures


def _process_rows():
    try:
        output = subprocess.run(
            ["/bin/ps", "-axo", "pid=,ppid=,command="],
            capture_output=True, text=True, timeout=5, check=True).stdout
    except Exception:
        raise AcceptanceError("process_inventory_failed")
    rows = []
    for line in output.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) != 3:
            continue
        try:
            rows.append((int(parts[0]), int(parts[1]), parts[2]))
        except ValueError:
            continue
    return rows


def _product_processes(rows=None):
    products = []
    for pid, _ppid, command in rows or _process_rows():
        match = PRODUCT_RE.match(command)
        if match:
            products.append((pid, pathlib.Path(match.group(1))))
    return products


def _exact_product_present(pid, binary):
    """Require one captured PID to still run the exact candidate binary."""
    try:
        wanted = pathlib.Path(binary).resolve()
    except OSError:
        return False
    for running_pid, running_binary in _product_processes():
        if running_pid != pid:
            continue
        try:
            return running_binary.resolve() == wanted
        except OSError:
            return False
    return False


def _descendants(rows, parents):
    pending = set(parents)
    found = set()
    changed = True
    while changed:
        changed = False
        for pid, ppid, _command in rows:
            if ppid in pending and pid not in found:
                found.add(pid)
                pending.add(pid)
                changed = True
    return found


def _app_binary(bundle):
    return pathlib.Path(bundle) / "Contents" / "MacOS" / "JuiceMount"


def _bundle_from_binary(binary):
    return pathlib.Path(binary).parents[2]


def _resolve_test_app(explicit, products):
    if explicit:
        return pathlib.Path(explicit).expanduser().resolve()
    if len(products) == 1:
        return _bundle_from_binary(products[0][1]).resolve()
    if _app_binary(DEFAULT_BUILD_APP).is_file():
        return DEFAULT_BUILD_APP.resolve()
    if _app_binary(DEFAULT_INSTALLED_APP).is_file():
        return DEFAULT_INSTALLED_APP.resolve()
    return None


def _wait_pids_gone(pids, timeout):
    deadline = time.monotonic() + timeout
    remaining = set(pids)
    while remaining and time.monotonic() < deadline:
        for pid in list(remaining):
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                remaining.discard(pid)
            except PermissionError:
                pass
        if remaining:
            time.sleep(0.25)
    return not remaining


def _signal_pids(pids, sig):
    for pid in sorted(set(pids)):
        try:
            os.kill(pid, sig)
        except (ProcessLookupError, PermissionError):
            pass


def _matching_original_products(original_products):
    """Return only still-live PIDs with the same captured bundle binary."""
    expected = {}
    for pid, binary in original_products:
        try:
            expected[pid] = pathlib.Path(binary).resolve()
        except OSError:
            continue
    survivors = set()
    for pid, binary in _product_processes():
        if pid not in expected:
            continue
        try:
            if pathlib.Path(binary).resolve() == expected[pid]:
                survivors.add(pid)
        except OSError:
            continue
    return survivors


def _mount_line(target):
    try:
        output = subprocess.run(["/sbin/mount"], capture_output=True, text=True,
                                timeout=5, check=True).stdout
    except Exception:
        raise AcceptanceError("mount_inventory_failed")
    marker = " on " + target + " "
    return next((line for line in output.splitlines() if marker in line), None)


def _mount_owned_by_product(line, target, kind):
    """Recognize only JuiceMount's exact loopback NFS / private FUSE mounts."""
    if not line or (" on " + target + " ") not in line:
        return False
    lowered = line.lower()
    if kind == "nfs":
        source = line.split(" on ", 1)[0].lower()
        loopback = (source.startswith("127.0.0.1:") or
                    source.startswith("localhost:") or
                    source.startswith("[::1]:"))
        return loopback and "(nfs" in lowered
    if kind == "fuse":
        return any(marker in lowered for marker in ("macfuse", "juicefs", "(fuse"))
    return False


def _mounted(target):
    return _mount_line(target) is not None


def _control_tcp_state(control):
    """Return closed/open/unknown for the numeric loopback control port.

    A malformed response or HTTP error still proves a listener exists, so JSON
    probing is not an absence test. Only an explicit TCP connection refusal is
    accepted as proof that the port is closed; timeouts and other socket errors
    remain unknown and fail the cold-start gate.
    """
    value = _validate_loopback_http(control)
    parsed = urllib.parse.urlparse(value)
    address = ipaddress.ip_address(parsed.hostname or "")
    family = socket.AF_INET6 if address.version == 6 else socket.AF_INET
    try:
        probe = socket.socket(family, socket.SOCK_STREAM)
    except OSError:
        return "unknown"
    try:
        try:
            probe.settimeout(0.5)
            result = probe.connect_ex((str(address), parsed.port))
        except OSError:
            return "unknown"
    finally:
        probe.close()
    if result == 0:
        return "open"
    if result == errno.ECONNREFUSED:
        return "closed"
    return "unknown"


def _cold_start_precondition(control, mount_root):
    """Prove the launch starts with no app, target NFS, or private FUSE.

    Cleanup returning successfully is not itself release evidence. Capture a
    fresh process and kernel-mount inventory immediately before launch and also
    require the loopback control endpoint to be absent. The report contains
    booleans/counts only; it never emits a mount path or endpoint.
    """
    fuse_internal = os.path.expanduser("~/.juicemount/fuse-internal")
    product_count = len(_product_processes())
    nfs_present = _mount_line(mount_root) is not None
    fuse_present = _mount_line(fuse_internal) is not None
    control_tcp_state = _control_tcp_state(control)
    passed = (product_count == 0 and not nfs_present and not fuse_present and
              control_tcp_state == "closed")
    return {
        "status": "PASS" if passed else "FAIL",
        "product_process_count": product_count,
        "target_nfs_mount_present": nfs_present,
        "private_fuse_mount_present": fuse_present,
        "control_tcp_port_state": control_tcp_state,
        "control_tcp_port_proven_closed": control_tcp_state == "closed",
    }


def _require_cold_start_precondition(control, mount_root):
    state = _cold_start_precondition(control, mount_root)
    if state["status"] != "PASS":
        raise AcceptanceError("cold_start_precondition_failed")
    return state


def _wait_for_product_teardown(original_products, mount_targets, timeout):
    """Wait until the captured app and both of its mounts are explicitly gone.

    A PID exit alone is insufficient: a hard localhost NFS mount can remain in
    the kernel table after its server dies. Conversely, mounts can disappear
    before AppKit finishes the remaining server/FUSE drain. Require both facts
    before declaring graceful teardown complete. Inventory failures are treated
    as unknown, never optimistic success.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        products_gone = not _matching_original_products(original_products)
        try:
            mounts_gone = all(_mount_line(target) is None
                              for target in mount_targets)
        except AcceptanceError:
            mounts_gone = False
        if products_gone and mounts_gone:
            return True
        time.sleep(0.5)
    return False


def _unmount(target, kind):
    line = _mount_line(target)
    if line is None:
        return True
    if not _mount_owned_by_product(line, target, kind):
        raise AcceptanceError("mount_ownership_unverified")
    commands = (["/sbin/umount", target], ["/sbin/umount", "-f", target],
                ["/usr/sbin/diskutil", "unmount", "force", target])
    for command in commands:
        try:
            completed = subprocess.run(command, stdout=subprocess.DEVNULL,
                                       stderr=subprocess.DEVNULL, timeout=15)
            if completed.returncode == 0 and not _mounted(target):
                return True
        except Exception:
            continue
    return not _mounted(target)


def _clean_stop(mount_root):
    # Validate ownership before stopping anything. A typo in --mount-root must
    # never turn the lifecycle flag into a force-unmount of an unrelated disk.
    fuse_internal = os.path.expanduser("~/.juicemount/fuse-internal")
    for target, kind in ((mount_root, "nfs"), (fuse_internal, "fuse")):
        line = _mount_line(target)
        if line is not None and not _mount_owned_by_product(line, target, kind):
            raise AcceptanceError("mount_ownership_unverified")

    rows = _process_rows()
    products = _product_processes(rows)
    product_pids = {pid for pid, _binary in products}
    child_pids = _descendants(rows, product_pids)
    child_commands = {pid: command for pid, _ppid, command in rows
                      if pid in child_pids}

    if product_pids:
        try:
            subprocess.run(
                ["/usr/bin/osascript", "-e",
                 'tell application id "com.juicemount.app" to quit'],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=QUIT_REQUEST_TIMEOUT_SEC)
        except Exception:
            pass
        graceful = _wait_for_product_teardown(
            products, (mount_root, fuse_internal),
            GRACEFUL_PRODUCT_TEARDOWN_SEC)
        if not graceful:
            # Re-resolve the original PID+binary identities after the long
            # grace. Never signal a newly-launched JuiceMount or a reused PID.
            survivors = _matching_original_products(products)
            _signal_pids(survivors, signal.SIGTERM)
            _wait_pids_gone(survivors, PRODUCT_TERM_GRACE_SEC)
            survivors = _matching_original_products(products)
            _signal_pids(survivors, signal.SIGKILL)
            _wait_pids_gone(survivors, PRODUCT_KILL_GRACE_SEC)

    # Only descendants captured from the exact product process and still
    # carrying the exact same command are touched. The long graceful wait makes
    # guarding against PID reuse important.
    current_commands = {pid: command for pid, _ppid, command in _process_rows()}
    live_children = {pid for pid, command in child_commands.items()
                     if current_commands.get(pid) == command}
    if live_children:
        _signal_pids(live_children, signal.SIGTERM)
        _wait_pids_gone(live_children, 4)
    current_commands = {pid: command for pid, _ppid, command in _process_rows()}
    live_children = {pid for pid, command in child_commands.items()
                     if current_commands.get(pid) == command}
    if live_children:
        _signal_pids(live_children, signal.SIGKILL)
        _wait_pids_gone(live_children, 2)

    # A different/new product process appearing during the bounded wait is not
    # ours to kill or unmount underneath. Fail before the fallback unmounts.
    if _product_processes():
        raise AcceptanceError("clean_stop_failed")

    root_ok = _unmount(mount_root, "nfs")
    fuse_ok = _unmount(fuse_internal, "fuse")
    if _product_processes() or not root_ok or not fuse_ok:
        raise AcceptanceError("clean_stop_failed")


def _launch_bundle(bundle, environment=None, direct=False, started_at=None):
    binary = _app_binary(bundle)
    if not binary.is_file():
        raise AcceptanceError("app_bundle_unavailable")
    env = os.environ.copy()
    # Never let a test-only override survive the direct-launch fallback used to
    # restore the normal app. The test arm adds these keys back explicitly.
    for key in TEST_ONLY_ENV_KEYS:
        env.pop(key, None)
    if environment:
        env.update(environment)
    child = None
    expected_pid = None
    try:
        if direct:
            child = subprocess.Popen(
                [str(binary)], env=env, stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL, start_new_session=True)
            expected_pid = getattr(child, "pid", None)
            if not isinstance(expected_pid, int) or expected_pid <= 0:
                raise AcceptanceError("app_launch_pid_unavailable")
        else:
            subprocess.run(["/usr/bin/open", "-n", str(bundle)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                           timeout=12, check=False, env=env)
    except Exception:
        raise AcceptanceError("app_launch_failed")

    observation_origin = (time.monotonic() if started_at is None else
                          started_at)
    deadline = time.monotonic() + 12
    wanted = binary.resolve()
    while time.monotonic() < deadline:
        if child is not None and child.poll() is not None:
            raise AcceptanceError("app_exited_during_start")
        matches = []
        for running_pid, running_binary in _product_processes():
            if expected_pid is not None and running_pid != expected_pid:
                continue
            try:
                if running_binary.resolve() == wanted:
                    matches.append(running_pid)
            except OSError:
                continue
        if len(matches) > 1:
            raise AcceptanceError("multiple_app_processes_running")
        if len(matches) == 1:
            if child is not None and child.poll() is not None:
                raise AcceptanceError("app_exited_during_start")
            return {
                "pid": matches[0],
                "binary": wanted,
                # Internal lifetime handle only; callers must never serialize
                # it. Keeping the Popen object lets startup detect/reap an
                # exact candidate that exits after first ps visibility.
                "process": child,
                "visible_ms": round(
                    (time.monotonic() - observation_origin) * 1000.0, 3),
            }
        time.sleep(0.25)
    if not direct:
        # LaunchServices can retain stale running state after a stop.  The
        # direct fallback uses the normal environment, never the test one.
        return _launch_bundle(bundle, environment=None, direct=True,
                              started_at=observation_origin)
    raise AcceptanceError("app_process_not_started")


class ProxyController:
    def __init__(self, args, tempdir):
        self.args = args
        self.tempdir = pathlib.Path(tempdir)
        self.process = None
        self.stats_file = None
        self.segment = 0

    def start(self, budget, connect_readiness_probe=True):
        if self.process is not None:
            raise AcceptanceError("proxy_already_running")
        self.segment += 1
        self.stats_file = self.tempdir / ("proxy-%d.json" % self.segment)
        command = [
            sys.executable, str(PROXY_SOURCE),
            "--listen", self.args.proxy_listen,
            "--target", self.args.redis_target,
            "--delay-ms", str(self.args.delay_ms),
            "--jitter-ms", str(self.args.jitter_ms),
            "--down-mbps", str(self.args.down_mbps),
            "--up-mbps", str(self.args.up_mbps),
            "--max-forward-bytes", str(budget),
            "--stats-file", str(self.stats_file),
            "--seed", str(self.args.seed),
            "--quiet",
        ]
        try:
            self.process = subprocess.Popen(command, stdout=subprocess.DEVNULL,
                                            stderr=subprocess.DEVNULL,
                                            start_new_session=True)
        except Exception:
            self.process = None
            raise AcceptanceError("proxy_launch_failed")
        host, port = _validate_proxy_listen(self.args.proxy_listen)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.process = None
                raise AcceptanceError("proxy_exited_during_start")
            if not connect_readiness_probe:
                # The proxy binds before its stats writer starts. Waiting for
                # the first snapshot therefore proves that it reached the
                # listening loop without opening a client connection. The
                # real-cellular runner uses this mode so its accounting proxy
                # cannot pre-warm the remote Redis/Tailscale path before the
                # single launch clock begins.
                if self.snapshot() is not None:
                    return
                time.sleep(0.1)
                continue
            try:
                with socket.create_connection((host, port), timeout=0.25):
                    return
            except OSError:
                time.sleep(0.1)
        self.stop()
        raise AcceptanceError("proxy_not_ready")

    def snapshot(self):
        if not self.stats_file:
            return None
        try:
            value = json.loads(self.stats_file.read_text(encoding="utf-8"))
        except Exception:
            return None
        allowed = ("connections_total", "connections_active", "bytes_up",
                   "bytes_down", "bytes_total", "bursts_up", "bursts_down",
                   "budget_exceeded", "elapsed_sec", "simulated")
        return {key: value.get(key) for key in allowed}

    def stop(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=3)
            self.process = None
        deadline = time.monotonic() + 1.0
        snapshot = self.snapshot()
        while snapshot is None and time.monotonic() < deadline:
            time.sleep(0.05)
            snapshot = self.snapshot()
        if snapshot is None:
            raise AcceptanceError("proxy_stats_unavailable")
        return snapshot


def _wait_startup(control, timeout, started_at=None, expected_product=None):
    # A release startup measurement begins immediately before the launch
    # command, not after LaunchServices/direct launch has already found the
    # process. Keep the optional default for focused unit callers.
    started = time.monotonic() if started_at is None else started_at
    deadline = started + timeout
    transitions = []
    first_control = None
    while time.monotonic() < deadline:
        candidate_child = ((expected_product or {}).get("process")
                           if expected_product is not None else None)
        if candidate_child is not None and candidate_child.poll() is not None:
            raise AcceptanceError("app_exited_during_startup")
        if (expected_product is not None and
                not _exact_product_present(expected_product["pid"],
                                           expected_product["binary"])):
            raise AcceptanceError("app_exited_during_startup")
        elapsed_ms = (time.monotonic() - started) * 1000.0
        warmup = _safe_get_json(control, "/warmup", timeout=2)
        if warmup is not None and first_control is None:
            first_control = elapsed_ms
        if isinstance(warmup, dict):
            phase = warmup.get("phase")
            if phase in ("starting", "indexing", "warming", "steady"):
                if not transitions or transitions[-1]["phase"] != phase:
                    transitions.append({"phase": phase,
                                        "elapsed_ms": round(elapsed_ms, 3)})
        health = _safe_get_json(control, "/health", timeout=2)
        offline = _safe_get_json(control, "/offline", timeout=2)
        metrics = _safe_get_json(control, "/metrics", timeout=2)
        components = health.get("components") if isinstance(health, dict) else None
        health_ok = (isinstance(components, dict) and bool(components) and
                     all(value == "ok" for value in components.values()))
        offline_ok = (isinstance(offline, dict) and
                      not offline.get("offline") and
                      not offline.get("auto_offline") and
                      not offline.get("user_offline"))
        network = metrics.get("network") if isinstance(metrics, dict) else None
        simulated = isinstance(network, dict) and network.get("simulated") is True
        content_guard = (isinstance(network, dict) and
                         network.get("fuse_content_read_guard") is True)
        serving = isinstance(warmup, dict) and warmup.get("serving") is True
        if serving and health_ok and offline_ok and simulated and content_guard:
            if candidate_child is not None and candidate_child.poll() is not None:
                raise AcceptanceError("app_exited_during_startup")
            if (expected_product is not None and
                    not _exact_product_present(expected_product["pid"],
                                               expected_product["binary"])):
                raise AcceptanceError("app_exited_during_startup")
            return {
                "control_ready_ms": round(
                    first_control if first_control is not None else elapsed_ms,
                    3),
                "serving_ready_ms": round(elapsed_ms, 3),
                "phase_transitions": transitions,
                "ready_phase": warmup.get("phase"),
                "offline_cleared": True,
                "synthetic_policy_confirmed": True,
                "zero_fuse_content_guard_confirmed": True,
            }, metrics
        time.sleep(0.5)
    raise AcceptanceError("startup_readiness_timeout")


def _wait_offline(control, wanted, timeout):
    started = time.monotonic()
    deadline = started + timeout
    while time.monotonic() < deadline:
        state = _safe_get_json(control, "/offline", timeout=2)
        auto = isinstance(state, dict) and state.get("auto_offline") is True
        effective = isinstance(state, dict) and state.get("offline") is True
        if wanted and auto and effective:
            return round((time.monotonic() - started) * 1000.0, 3)
        if not wanted and isinstance(state, dict) and not auto and not effective:
            return round((time.monotonic() - started) * 1000.0, 3)
        time.sleep(0.5)
    raise AcceptanceError("offline_engage_timeout" if wanted else
                          "offline_recovery_timeout")


def _wait_normal_ready(control, timeout):
    """Verify that restoration produced a serving, healthy normal mount."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        warmup = _safe_get_json(control, "/warmup", timeout=2)
        health = _safe_get_json(control, "/health", timeout=2)
        offline = _safe_get_json(control, "/offline", timeout=2)
        metrics = _safe_get_json(control, "/metrics", timeout=2)
        components = health.get("components") if isinstance(health, dict) else None
        healthy = (isinstance(components, dict) and bool(components) and
                   all(value == "ok" for value in components.values()))
        online = (isinstance(offline, dict) and
                  not offline.get("offline") and
                  not offline.get("auto_offline") and
                  not offline.get("user_offline"))
        network = metrics.get("network") if isinstance(metrics, dict) else None
        normal_policy = (isinstance(network, dict) and
                         network.get("simulated") is not True and
                         network.get("fuse_content_read_guard") is not True)
        if (isinstance(warmup, dict) and warmup.get("serving") is True and
                healthy and online and normal_policy):
            return True
        time.sleep(0.5)
    return False


def _wait_recovery_ready(control, timeout):
    """Require the banner state, health, and serving state to all recover."""
    started = time.monotonic()
    deadline = started + timeout
    while time.monotonic() < deadline:
        offline = _safe_get_json(control, "/offline", timeout=2)
        warmup = _safe_get_json(control, "/warmup", timeout=2)
        health = _safe_get_json(control, "/health", timeout=2)
        components = health.get("components") if isinstance(health, dict) else None
        healthy = (isinstance(components, dict) and bool(components) and
                   all(value == "ok" for value in components.values()))
        online = (isinstance(offline, dict) and
                  not offline.get("offline") and
                  not offline.get("auto_offline"))
        serving = isinstance(warmup, dict) and warmup.get("serving") is True
        if online and healthy and serving:
            return round((time.monotonic() - started) * 1000.0, 3)
        time.sleep(0.5)
    raise AcceptanceError("recovery_readiness_timeout")


def _stop_test_environment(args, proxy, segments):
    """Stop the impaired app before withdrawing its metadata route.

    Stopping the proxy first makes the test app enter auto-offline while its
    NFS/FUSE shutdown is beginning.  Besides contaminating the last metrics,
    that manufactured outage has reproduced a readable-but-unwritable mount
    on the immediately-restored normal app. Keep the bounded route alive until
    the exact product process has completed teardown, then stop the proxy.
    """
    cleanup_ok = False
    try:
        _clean_stop(args.mount_root)
        cleanup_ok = True
    except AcceptanceError:
        # Restoration retries the scoped cleanup after the proxy is stopped.
        # This helper never turns a partial stop into permission to launch a
        # second product process.
        pass

    proxy_error = None
    if proxy is not None:
        try:
            segments.append(proxy.stop())
        except AcceptanceError as error:
            proxy_error = error.code
    return cleanup_ok, proxy_error


def _restore_initial_state(args, original_running, original_bundle,
                           cleanup_verified=False):
    """Best-effort rescue that relaunches even after partial cleanup failure."""
    cleanup_ok = cleanup_verified
    if not cleanup_ok:
        try:
            _clean_stop(args.mount_root)
            cleanup_ok = True
        except AcceptanceError:
            # Continue to process identity below. clean_stop may have
            # successfully terminated the test app before an unmount
            # verification failed.
            pass

    products = _product_processes()
    if not original_running:
        return cleanup_ok and not products, cleanup_ok
    if original_bundle is None:
        return False, cleanup_ok

    wanted = _app_binary(original_bundle).resolve()
    if products:
        if len(products) != 1:
            return False, cleanup_ok
        try:
            if products[0][1].resolve() != wanted:
                return False, cleanup_ok
        except OSError:
            return False, cleanup_ok
    else:
        try:
            # Direct launch is deliberate for restoration: it guarantees that
            # the exact original bundle receives the sanitized environment.
            # LaunchServices may retain stale running identity after the test
            # process exits and does not offer a reliable environment contract.
            _launch_bundle(original_bundle, environment=None, direct=True)
        except AcceptanceError:
            return False, cleanup_ok
    return _wait_normal_ready(args.control, args.startup_timeout), cleanup_ok


def _remaining_budget(total, segments):
    used = sum(int(segment.get("bytes_total") or 0) for segment in segments)
    return max(0, int(total) - used)


def _proxy_summary(segments, budget):
    forwarded = sum(int(segment.get("bytes_total") or 0) for segment in segments)
    return {
        "segments": len(segments),
        "connections_total": sum(
            int(segment.get("connections_total") or 0) for segment in segments),
        "bytes_up": sum(int(segment.get("bytes_up") or 0)
                        for segment in segments),
        "bytes_down": sum(int(segment.get("bytes_down") or 0)
                          for segment in segments),
        "forwarded_bytes": forwarded,
        "budget_bytes": budget,
        "budget_exceeded": (forwarded > budget or
                            any(bool(segment.get("budget_exceeded"))
                                for segment in segments)),
    }


def _evaluate(report, args):
    failures = report["failures"]
    cold_start = report.get("cold_start_precondition") or {}
    startup = report.get("startup") or {}
    navigation = report.get("navigation") or {}
    metrics = report.get("traffic_gate") or {}
    proxy = report.get("metadata_proxy") or {}

    if cold_start.get("status") != "PASS":
        failures.append("cold_start_precondition_missing_or_failed")
    if (startup.get("timing_origin") != "launch_command" or
            startup.get("process_present_ms") is None or
            startup.get("control_ready_ms") is None or
            startup.get("serving_ready_ms") is None):
        failures.append("cold_start_timing_incomplete")
    if navigation.get("first_cached_root_from_launch_ms") is None:
        failures.append("first_cached_root_timing_missing")
    if navigation.get("cold_root_ms", float("inf")) > args.cold_root_max_ms:
        failures.append("cold_root_latency_over_budget")
    if (navigation.get("warm_root") or {}).get("p95_ms", float("inf")) > args.warm_p95_ms:
        failures.append("warm_root_latency_over_budget")
    if ((navigation.get("concurrent_root") or {}).get("p95_ms", float("inf")) >
            args.concurrent_p95_ms):
        failures.append("concurrent_root_latency_over_budget")
    if not navigation.get("entry_count_consistent"):
        failures.append("root_entry_count_changed_during_test")
    if navigation.get("entry_count", 0) <= 0:
        failures.append("root_metadata_listing_empty")
    if not metrics.get("backend_counters_available"):
        failures.append("backend_byte_counters_unavailable")
    if metrics.get("remote_hydration_observed"):
        failures.append("remote_hydration_observed")
    # Navigation is read-only. A remote PUT cannot be attributed to the
    # metadata workload and remains a hard failure even if all reads were local.
    if metrics.get("object_put_bytes_delta", 0) != 0:
        failures.append("navigation_object_upload_observed")
    if metrics.get("readdir_mirror_hit_delta", 0) <= 0:
        failures.append("local_mirror_coverage_missing")
    warm = report.get("warm_transition") or {}
    if (not warm.get("serving_after_navigation") or
            warm.get("phase_after_navigation") == "starting" or
            warm.get("phase_after_navigation") == "unknown"):
        failures.append("warm_serving_transition_incomplete")
    if proxy.get("budget_exceeded"):
        failures.append("metadata_byte_budget_exceeded")
    if proxy.get("forwarded_bytes", args.budget_bytes + 1) > args.budget_bytes:
        failures.append("metadata_byte_budget_exceeded")
    if args.reconnect and (report.get("reconnect") or {}).get("status") != "PASS":
        failures.append("reconnect_coverage_incomplete")

    report["failures"] = sorted(set(failures))
    report["status"] = "PASS" if not report["failures"] else "FAIL"


def _run_lifecycle(args):
    report = _base_report(args, "lifecycle")
    segments = []
    proxy = None
    temp = tempfile.TemporaryDirectory(prefix="jm-cellular-synthetic-")
    original_products = []
    original_bundle = None
    original_running = False
    test_app = None
    launch_started = None
    lifecycle_changed = False
    restore_ok = False
    run_error = None

    try:
        if not _proxy_contract_ok():
            raise AcceptanceError("bounded_proxy_contract_missing")
        _validate_loopback_http(args.control)
        _validate_proxy_listen(args.proxy_listen)
        _validate_redis_target(args.redis_target)
        _validate_output_location(args.json_out, args.mount_root)
        if not MIN_BUDGET_BYTES <= args.budget_bytes <= MAX_BUDGET_BYTES:
            raise AcceptanceError("byte_budget_out_of_safe_range")
        if not _loopback_port_available(args.proxy_listen):
            raise AcceptanceError("proxy_listen_port_in_use")

        original_products = _product_processes()
        if len(original_products) > 1:
            raise AcceptanceError("multiple_app_processes_running")
        original_running = len(original_products) == 1
        if original_running:
            original_bundle = _bundle_from_binary(original_products[0][1]).resolve()
            state = _safe_get_json(args.control, "/offline", timeout=3)
            if isinstance(state, dict) and state.get("user_offline"):
                raise AcceptanceError("user_offline_enabled")
            warmup = _safe_get_json(args.control, "/warmup", timeout=3)
            health = _safe_get_json(args.control, "/health", timeout=3)
            components = health.get("components") if isinstance(health, dict) else None
            initially_healthy = (isinstance(components, dict) and bool(components) and
                                 all(value == "ok" for value in components.values()))
            if not initially_healthy or not isinstance(warmup, dict) or not warmup.get("serving"):
                raise AcceptanceError("initial_normal_app_not_ready")
        test_app = _resolve_test_app(args.app, original_products)
        if test_app is None or not _app_binary(test_app).is_file():
            raise AcceptanceError("app_bundle_unavailable")

        # clean_stop can partially succeed before a later unmount verification
        # fails.  Mark the lifecycle changed *before* entering it so the finally
        # block still restores the original normal app in that case.
        lifecycle_changed = True
        _clean_stop(args.mount_root)
        report["cold_start_precondition"] = _require_cold_start_precondition(
            args.control, args.mount_root)
        proxy = ProxyController(args, temp.name)
        proxy.start(args.budget_bytes)
        fake_profile = ("class=metered,rtt=%sms,bw_down=%sMbps,"
                        "bw_up=%sMbps,loss=0%%" %
                        (args.delay_ms * 2.0, args.down_mbps, args.up_mbps))
        launch_started = time.monotonic()
        launched_product = _launch_bundle(test_app, {
            "JM_DEBUG_META_ADDR": args.proxy_listen,
            "JM_NET_FAKE_LINK": fake_profile,
            CONTENT_GUARD_ENV: "1",
        }, direct=True, started_at=launch_started)
        process_present_ms = launched_product["visible_ms"]

        startup, startup_metrics = _wait_startup(
            args.control, args.startup_timeout, started_at=launch_started,
            expected_product=launched_product)
        startup["process_present_ms"] = process_present_ms
        startup["timing_origin"] = "launch_command"
        report["startup"] = startup
        startup_metrics, quiescent_ms = _wait_startup_data_quiescent(
            args.control, startup_metrics)
        report["startup"]["data_plane_quiescent_ms"] = quiescent_ms
        startup_gate = _startup_probe_gate(startup_metrics)
        report["startup_traffic_gate"] = startup_gate
        report["failures"].extend(_startup_probe_failure_codes(startup_gate))

        before = startup_metrics
        report["navigation"] = _navigation_sample(
            args.mount_root, args.samples, args.concurrency, args.per_worker,
            launch_started=launch_started)
        post_warmup = _safe_get_json(args.control, "/warmup", timeout=3) or {}
        report["warm_transition"] = {
            "serving_after_navigation": post_warmup.get("serving") is True,
            "phase_after_navigation": post_warmup.get("phase")
            if post_warmup.get("phase") in ("starting", "indexing", "warming", "steady")
            else "unknown",
        }

        if args.reconnect:
            first = proxy.stop()
            proxy = None
            segments.append(first)
            remaining = _remaining_budget(args.budget_bytes, segments)
            if first.get("budget_exceeded") or remaining < MIN_RECONNECT_BUDGET:
                report["reconnect"] = {
                    "status": "SKIP",
                    "reason_code": "insufficient_remaining_byte_budget",
                }
            else:
                engage_ms = None
                offline_navigation = None
                reconnect_error = None
                try:
                    engage_ms = _wait_offline(args.control, True,
                                              args.reconnect_engage_timeout)
                    elapsed, count = _enumerate_root_metadata(args.mount_root)
                    offline_navigation = {
                        "root_ms": round(elapsed, 3),
                        "entry_count": count,
                    }
                except AcceptanceError as error:
                    reconnect_error = error
                finally:
                    proxy = ProxyController(args, temp.name)
                    # Preserve segment numbering in the artifact filename only;
                    # no endpoint or source data is ever stored there.
                    proxy.segment = 1
                    proxy.start(remaining)
                if reconnect_error is not None:
                    raise reconnect_error
                recover_ms = _wait_recovery_ready(
                    args.control, args.reconnect_recover_timeout)
                recovered_elapsed, recovered_count = _enumerate_root_metadata(
                    args.mount_root)
                report["reconnect"] = {
                    "status": "PASS",
                    "auto_offline_engage_ms": engage_ms,
                    "online_recovery_ms": recover_ms,
                    "cached_navigation_while_offline": offline_navigation,
                    "post_recovery_navigation": {
                        "root_ms": round(recovered_elapsed, 3),
                        "entry_count": recovered_count,
                    },
                }
        else:
            report["reconnect"] = {"status": "SKIP",
                                   "reason_code": "explicitly_disabled"}

        after = _safe_get_json(args.control, "/metrics", timeout=5)
        if not isinstance(after, dict):
            raise AcceptanceError("final_metrics_unavailable")
        report["traffic_gate"] = _metrics_gate(before, after)

    except AcceptanceError as error:
        run_error = error.code
    except (KeyboardInterrupt, InterruptedError):
        run_error = "interrupted"
    except Exception:
        run_error = "unexpected_runner_error"
    finally:
        cleanup_ok = True
        if lifecycle_changed:
            # Preserve the impaired Redis route until the test app has fully
            # unmounted and exited. Pulling the route first induces an
            # artificial auto-offline transition during teardown and can leave
            # the next loopback NFS session readable but unable to complete its
            # write-readiness probe.
            cleanup_ok, proxy_error = _stop_test_environment(
                args, proxy, segments)
            proxy = None
            if proxy_error is not None and run_error is None:
                run_error = proxy_error
            restore_ok, cleanup_ok = _restore_initial_state(
                args, original_running, original_bundle,
                cleanup_verified=cleanup_ok)
            if not restore_ok and run_error is None:
                run_error = "normal_app_restore_failed"
        else:
            if proxy is not None:
                try:
                    segments.append(proxy.stop())
                except AcceptanceError as error:
                    if run_error is None:
                        run_error = error.code
                proxy = None
            restore_ok = True
        temp.cleanup()

    report["metadata_proxy"] = _proxy_summary(segments, args.budget_bytes)
    report["restore"] = {
        "initial_running_state": original_running,
        "initial_state_restored": restore_ok,
        # Teardown and normal-app readiness are separate facts. A healthy
        # original app can fail to come back even after the impaired process,
        # proxy, and test-only environment have all been removed.
        "test_environment_removed": cleanup_ok,
        "cleanup_verified": cleanup_ok,
    }
    if run_error:
        report["failures"].append(run_error)
    if not restore_ok:
        report["failures"].append("normal_app_restore_failed")
    if not cleanup_ok:
        report["failures"].append("test_cleanup_not_verified")
    if "navigation" in report and "traffic_gate" in report:
        _evaluate(report, args)
    else:
        report["failures"] = sorted(set(report["failures"]))
        report["status"] = "FAIL"
    return report


def _parse_args(argv=None):
    parser = SafeArgumentParser(description=__doc__)
    parser.add_argument("--lifecycle", action="store_true",
                        help="explicitly authorize reversible app stop/start and run")
    parser.add_argument("--app", default="",
                        help="app bundle under test (never recorded in results)")
    parser.add_argument("--mount-root", default=DEFAULT_MOUNT,
                        help="mount root to enumerate; child names are never emitted")
    parser.add_argument("--control", default=DEFAULT_CONTROL,
                        help="loopback control-plane URL")
    parser.add_argument("--redis-target", default="",
                        help="metadata host:port; required only with --lifecycle")
    parser.add_argument("--proxy-listen", default=DEFAULT_PROXY_LISTEN,
                        help="numeric loopback host:port")
    parser.add_argument("--delay-ms", type=float, default=90.0,
                        help="synthetic one-way metadata latency")
    parser.add_argument("--jitter-ms", type=float, default=20.0)
    parser.add_argument("--down-mbps", type=float, default=8.0)
    parser.add_argument("--up-mbps", type=float, default=2.0)
    parser.add_argument("--budget-bytes", type=int, default=4 * 1024 * 1024,
                        help="hard aggregate metadata proxy cap (max 32 MiB)")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--samples", type=int, default=8)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--per-worker", type=int, default=2)
    parser.add_argument("--no-reconnect", dest="reconnect", action="store_false",
                        help="skip the bounded proxy-loss/recovery phase")
    parser.set_defaults(reconnect=True)
    parser.add_argument("--startup-timeout", type=float, default=180.0)
    parser.add_argument("--reconnect-engage-timeout", type=float, default=75.0)
    parser.add_argument("--reconnect-recover-timeout", type=float, default=90.0)
    parser.add_argument("--cold-root-max-ms", type=float, default=3000.0)
    parser.add_argument("--warm-p95-ms", type=float, default=250.0)
    parser.add_argument("--concurrent-p95-ms", type=float, default=500.0)
    parser.add_argument("--json-out", default="",
                        help="optional local result file; writing on the mount is refused")
    args = parser.parse_args(argv)

    numeric = (args.delay_ms, args.jitter_ms, args.down_mbps, args.up_mbps,
               args.startup_timeout, args.reconnect_engage_timeout,
               args.reconnect_recover_timeout, args.cold_root_max_ms,
               args.warm_p95_ms, args.concurrent_p95_ms)
    if any(value < 0 or not math.isfinite(value) for value in numeric):
        raise AcceptanceError("numeric_argument_invalid")
    if args.delay_ms <= 0:
        raise AcceptanceError("delay_must_be_positive")
    if args.samples < 1 or args.concurrency < 1 or args.per_worker < 1:
        raise AcceptanceError("sample_argument_invalid")
    if args.concurrency > 16 or args.samples > 64 or args.per_worker > 16:
        raise AcceptanceError("sample_argument_out_of_safe_range")
    if not MIN_BUDGET_BYTES <= args.budget_bytes <= MAX_BUDGET_BYTES:
        raise AcceptanceError("byte_budget_out_of_safe_range")
    _validate_output_location(args.json_out, args.mount_root)
    if args.lifecycle and not args.redis_target:
        raise AcceptanceError("redis_target_required_for_lifecycle")
    return args


def main(argv=None):
    try:
        args = _parse_args(argv)
        report = _run_lifecycle(args) if args.lifecycle else _dry_run(args)
        _emit(report, args)
        return 0 if report["status"] in ("PASS", "DRY_RUN") else 1
    except AcceptanceError as error:
        # No argparse values, source paths, endpoint names, or exception text.
        sys.stdout.write(json.dumps({
            "schema": 1,
            "name": "juicemount_cellular_metadata_acceptance",
            "status": "FAIL",
            "measurement": {"simulated": True,
                            "scope": "metadata_link_only",
                            "real_cellular": False},
            "failures": [error.code],
        }, indent=2, sort_keys=True) + "\n")
        return 2


if __name__ == "__main__":
    sys.exit(main())
