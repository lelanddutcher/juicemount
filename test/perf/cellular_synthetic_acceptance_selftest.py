#!/usr/bin/env python3
"""Dependency-free safety tests for cellular_synthetic_acceptance.py."""

import contextlib
import errno
import importlib.util
import inspect
import io
import json
import pathlib
import sys
import types
from unittest import mock


HERE = pathlib.Path(__file__).resolve().parent
SOURCE = HERE / "cellular_synthetic_acceptance.py"


def load_runner():
    sys.dont_write_bytecode = True
    spec = importlib.util.spec_from_file_location("cellular_acceptance", SOURCE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class UninspectableEntry:
    @property
    def name(self):
        raise AssertionError("entry name must not be inspected")

    @property
    def path(self):
        raise AssertionError("entry path must not be inspected")


class FakeScandir:
    def __init__(self, count):
        self.entries = [UninspectableEntry() for _ in range(count)]

    def __enter__(self):
        return iter(self.entries)

    def __exit__(self, _kind, _value, _traceback):
        return False


class FakeProcess:
    def __init__(self):
        self.returncode = None

    def poll(self):
        return self.returncode


class FakeAppProcess(FakeProcess):
    def __init__(self, pid=4321, returncode=None):
        super().__init__()
        self.pid = pid
        self.returncode = returncode


class FakeControlSocket:
    def __init__(self, result):
        self.result = result
        self.closed = False

    def settimeout(self, _seconds):
        pass

    def connect_ex(self, _address):
        return self.result

    def close(self):
        self.closed = True


class FakeSocket:
    def __enter__(self):
        return self

    def __exit__(self, _kind, _value, _traceback):
        return False


def assert_raises_code(runner, code, function, *args):
    try:
        function(*args)
    except runner.AcceptanceError as error:
        assert error.code == code, (error.code, code)
    else:
        raise AssertionError("expected AcceptanceError(%s)" % code)


def main():
    runner = load_runner()

    stats = runner._timing_stats([1, 2, 3, 4, 5])
    assert stats["n"] == 5
    assert stats["p50_ms"] == 3
    assert stats["p95_ms"] == 4.8

    # The navigation primitive may enumerate entries but may not inspect names,
    # paths, metadata, or bytes.
    with mock.patch.object(runner.os, "scandir", return_value=FakeScandir(7)):
        elapsed, count = runner._enumerate_root_metadata("private-test-value")
    assert elapsed >= 0 and count == 7
    primitive = inspect.getsource(runner._enumerate_root_metadata)
    for forbidden in ("open(", "lstat", ".name", ".path"):
        assert forbidden not in primitive

    # The first cached root completion is measured from the launch-command
    # origin, not merely as an isolated READDIR duration.
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(
            runner.os, "scandir", return_value=FakeScandir(7)))
        stack.enter_context(mock.patch.object(runner.time, "monotonic",
                                               return_value=12.5))
        navigation = runner._navigation_sample(
            "private-test-value", 1, 1, 1, launch_started=10.0)
    assert navigation["first_cached_root_from_launch_ms"] == 2500.0

    before = {
        "backend": {"object_get_bytes": 10, "object_put_bytes": 20,
                    "cache_miss_bytes": 30, "meta_ops": 40},
        "bytes_read": 50,
        "rpcs": {"READ": {"count": 60}},
        "readdir_mirror_hit": 70,
        "redis": {"round_trips": 80},
    }
    after = {
        "backend": {"object_get_bytes": 10, "object_put_bytes": 20,
                    "cache_miss_bytes": 30, "meta_ops": 44},
        "bytes_read": 50,
        "rpcs": {"READ": {"count": 60}},
        "readdir_mirror_hit": 75,
        "redis": {"round_trips": 86},
    }
    gate = runner._metrics_gate(before, after)
    assert gate["backend_counters_available"] is True
    assert gate["object_get_bytes_delta"] == 0
    assert gate["nfs_read_rpc_delta"] == 0
    assert gate["remote_hydration_observed"] is False
    assert gate["local_cached_nfs_read_observed"] is False
    assert gate["readdir_mirror_hit_delta"] == 5
    assert gate["redis_round_trips_delta"] == 6

    local_after = json.loads(json.dumps(after))
    local_after["bytes_read"] = 114738
    local_after["rpcs"]["READ"]["count"] = 155
    local_after["read_warm_subread"] = 9
    local_gate = runner._metrics_gate(before, local_after)
    assert local_gate["nfs_read_bytes_delta"] == 114688
    assert local_gate["nfs_read_rpc_delta"] == 95
    assert local_gate["warm_subread_delta"] == 9
    assert local_gate["local_cached_nfs_read_observed"] is True
    assert local_gate["remote_hydration_observed"] is False

    remote_after = json.loads(json.dumps(local_after))
    remote_after["backend"]["object_get_bytes"] = 11
    remote_after["backend"]["cache_miss_bytes"] = 31
    assert runner._metrics_gate(
        before, remote_after)["remote_hydration_observed"] is True

    invalid_backends = (
        {},
        {"object_get_bytes": 0},
        {"object_get_bytes": 0, "object_put_bytes": 0,
         "cache_miss_bytes": 0, "meta_ops": float("nan")},
        {"object_get_bytes": 0, "object_put_bytes": 0,
         "cache_miss_bytes": -1, "meta_ops": 0},
    )
    for invalid in invalid_backends:
        invalid_before = json.loads(json.dumps(before))
        invalid_before["backend"] = invalid
        assert runner._backend_counters_available(invalid_before) is False
        assert runner._metrics_gate(
            invalid_before, after)["backend_counters_available"] is False
        invalid_after = json.loads(json.dumps(after))
        invalid_after["backend"] = invalid
        assert runner._metrics_gate(
            before, invalid_after)["backend_counters_available"] is False

    # Startup may upload only the mandatory 4 KiB write-readiness sentinel and
    # macOS's optional 4 KiB AppleDouble companion. It must carry one of those
    # two exact NFS mutation signatures and may not download even one byte.
    startup_probe = {
        "backend": {"object_get_bytes": 0, "object_put_bytes": 4096,
                    "cache_miss_bytes": 0, "meta_ops": 1},
        "bytes_read": 0,
        "bytes_written": 4096,
        "rpc_errors": 0,
        "rpcs": {"READ": {"count": 0}, "WRITE": {"count": 1},
                 "CREATE": {"count": 1}, "REMOVE": {"count": 1}},
        "read_cold_subread": 0,
        "read_warm_subread": 0,
        "thumb_warm_hydrated": 0,
        "readahead_prefetched_blocks": 0,
    }
    startup_gate = runner._startup_probe_gate(startup_probe)
    assert startup_gate["status"] == "PASS"
    assert startup_gate["mutation_signature_matches"] is True
    assert startup_gate["remote_hydration_observed"] is False
    assert startup_gate["local_cached_nfs_read_observed"] is False

    startup_invalid_backends = (
        {},
        {"object_get_bytes": 0},
        {"object_get_bytes": 0, "object_put_bytes": 4096,
         "cache_miss_bytes": 0, "meta_ops": float("inf")},
        {"object_get_bytes": 0, "object_put_bytes": 4096,
         "cache_miss_bytes": 0, "meta_ops": -1},
    )
    for invalid in startup_invalid_backends:
        invalid_probe = json.loads(json.dumps(startup_probe))
        invalid_probe["backend"] = invalid
        invalid_gate = runner._startup_probe_gate(invalid_probe)
        assert invalid_gate["backend_counters_available"] is False
        assert invalid_gate["status"] == "FAIL"

    appledouble_probe = json.loads(json.dumps(startup_probe))
    appledouble_probe["backend"]["object_put_bytes"] = 8192
    appledouble_probe["bytes_written"] = 8192
    appledouble_probe["rpcs"]["WRITE"]["count"] = 2
    appledouble_probe["rpcs"]["CREATE"]["count"] = 2
    appledouble_probe["rpcs"]["REMOVE"]["count"] = 2
    appledouble_gate = runner._startup_probe_gate(appledouble_probe)
    assert appledouble_gate["status"] == "PASS"
    assert appledouble_gate["mutation_signature_matches"] is True
    assert appledouble_gate["appledouble_companion_allowance_bytes"] == 4096

    one_download = json.loads(json.dumps(startup_probe))
    one_download["backend"]["object_get_bytes"] = 1
    assert runner._startup_probe_gate(one_download)["status"] == "FAIL"
    assert runner._startup_probe_gate(one_download)["remote_hydration_observed"] is True

    # NFS reads can be satisfied entirely by the local cache. Preserve and
    # report those counters, but do not call them remote cellular downloads.
    one_nfs_read = json.loads(json.dumps(startup_probe))
    one_nfs_read["bytes_read"] = 483328
    one_nfs_read["rpcs"]["READ"]["count"] = 261
    one_nfs_read["read_warm_subread"] = 7
    local_read_gate = runner._startup_probe_gate(one_nfs_read)
    assert local_read_gate["status"] == "PASS"
    assert local_read_gate["remote_hydration_observed"] is False
    assert local_read_gate["local_cached_nfs_read_observed"] is True

    one_cold_subread = json.loads(json.dumps(startup_probe))
    one_cold_subread["read_cold_subread"] = 1
    assert runner._startup_probe_gate(one_cold_subread)["status"] == "FAIL"
    assert runner._startup_probe_gate(
        one_cold_subread)["remote_hydration_observed"] is True

    oversized_upload = json.loads(json.dumps(startup_probe))
    oversized_upload["backend"]["object_put_bytes"] = 8193
    assert runner._startup_probe_gate(oversized_upload)["status"] == "FAIL"

    mismatched_appledouble = json.loads(json.dumps(appledouble_probe))
    mismatched_appledouble["rpcs"]["REMOVE"]["count"] = 1
    assert runner._startup_probe_gate(
        mismatched_appledouble)["status"] == "FAIL"

    unattributed_upload = json.loads(json.dumps(startup_probe))
    unattributed_upload["bytes_written"] = 0
    unattributed_upload["rpcs"]["WRITE"]["count"] = 0
    unattributed_upload["rpcs"]["CREATE"]["count"] = 0
    unattributed_upload["rpcs"]["REMOVE"]["count"] = 0
    assert runner._startup_probe_gate(unattributed_upload)["status"] == "FAIL"

    # Startup anomalies remain release-gate failures but no longer throw and
    # discard the navigation/reconnect evidence collected later in the run.
    startup_failures = runner._startup_probe_failure_codes(
        runner._startup_probe_gate(one_download))
    assert startup_failures == ["startup_object_download_observed"]
    assert runner._startup_probe_failure_codes(startup_gate) == []

    # Quiescence must span longer than the backend scraper's two-second cache,
    # otherwise a delayed readiness PUT could be charged to navigation.
    class FakeClock:
        def __init__(self):
            self.value = 0.0

        def monotonic(self):
            value = self.value
            self.value += 0.5
            return value

    clock = FakeClock()
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(runner.time, "monotonic",
                                               side_effect=clock.monotonic))
        stack.enter_context(mock.patch.object(runner.time, "sleep"))
        stack.enter_context(mock.patch.object(runner, "_safe_get_json",
                                               return_value=startup_probe))
        quiescent, elapsed_ms = runner._wait_startup_data_quiescent(
            "loopback-test-control", startup_probe)
    assert quiescent is startup_probe
    assert elapsed_ms >= runner.STARTUP_QUIESCENCE_SEC * 1000

    assert_raises_code(runner, "proxy_listen_must_be_loopback",
                       runner._validate_proxy_listen, "192.0.2.10:9999")
    assert_raises_code(runner, "redis_target_invalid",
                       runner._validate_redis_target,
                       "redis://credential@example.invalid:6379")
    assert_raises_code(runner, "redis_target_required_for_lifecycle",
                       runner._parse_args, ["--lifecycle"])
    assert_raises_code(runner, "byte_budget_out_of_safe_range",
                       runner._parse_args, ["--budget-bytes", "999999999"])
    assert_raises_code(runner, "delay_must_be_positive",
                       runner._parse_args, ["--delay-ms", "0"])
    assert_raises_code(runner, "result_file_must_not_be_on_mount",
                       runner._parse_args,
                       ["--mount-root", "/Volumes/private",
                        "--json-out", "/Volumes/private/result.json"])

    args = runner._parse_args([
        "--redis-target", "example.invalid:6379",
        "--mount-root", "private-test-value",
        "--app", "private-app-value",
    ])
    dry = runner._dry_run(args)
    runner._assert_report_redacted(
        dry, (args.mount_root, args.redis_target, args.app, args.control,
              args.proxy_listen))
    encoded = json.dumps(dry)
    assert "private-test-value" not in encoded
    assert "example.invalid" not in encoded
    assert dry["status"] == "DRY_RUN"
    assert dry["measurement"]["simulated"] is True
    assert dry["safety"]["object_bytes_hard_limited"] is True
    assert dry["safety"]["zero_fuse_content_fallthrough_guard"] is True
    assert dry["safety"]["object_download_bytes_post_hoc_zero_gate"] is True
    assert dry["safety"]["readiness_probe_upload_allowance_bytes"] == 8192
    assert dry["safety"][
        "ambient_local_cached_nfs_reads_reported_separately"] is True
    assert dry["safety"][
        "navigation_remote_object_get_and_put_zero_gate"] is True
    assert "local-cache NFS reads" in dry["safety"][
        "metadata_only_scope_warning"]

    # Local cached NFS reads remain visible but do not fail navigation. Remote
    # writes are still forbidden because this workload is strictly read-only.
    evaluation = {
        "failures": [],
        "cold_start_precondition": {"status": "PASS"},
        "startup": {
            "timing_origin": "launch_command",
            "process_present_ms": 1,
            "control_ready_ms": 2,
            "serving_ready_ms": 3,
        },
        "navigation": {
            "cold_root_ms": 10,
            "first_cached_root_from_launch_ms": 11,
            "warm_root": {"p95_ms": 10},
            "concurrent_root": {"p95_ms": 10},
            "entry_count_consistent": True,
            "entry_count": 7,
        },
        "traffic_gate": local_gate,
        "warm_transition": {
            "serving_after_navigation": True,
            "phase_after_navigation": "steady",
        },
        "metadata_proxy": {"budget_exceeded": False, "forwarded_bytes": 1},
        "reconnect": {"status": "PASS"},
    }
    runner._evaluate(evaluation, args)
    assert evaluation["status"] == "PASS"
    assert evaluation["traffic_gate"][
        "local_cached_nfs_read_observed"] is True

    upload_evaluation = json.loads(json.dumps(evaluation))
    upload_evaluation["failures"] = []
    upload_evaluation["traffic_gate"]["object_put_bytes_delta"] = 1
    runner._evaluate(upload_evaluation, args)
    assert upload_evaluation["status"] == "FAIL"
    assert "navigation_object_upload_observed" in upload_evaluation["failures"]

    leaking = dict(dry)
    leaking["accidental"] = "private-test-value/clip.mov"
    assert_raises_code(runner, "result_redaction_guard_failed",
                       runner._assert_report_redacted, leaking,
                       ("private-test-value",))

    # Proxy construction must carry the hard budget into the child command.
    captured = []

    def fake_popen(command, **_kwargs):
        captured.append(command)
        return FakeProcess()

    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(runner.subprocess, "Popen",
                                               side_effect=fake_popen))
        stack.enter_context(mock.patch.object(runner.socket, "create_connection",
                                               return_value=FakeSocket()))
        controller = runner.ProxyController(args, "/tmp")
        controller.start(123456)
    command = captured[0]
    budget_index = command.index("--max-forward-bytes") + 1
    assert command[budget_index] == "123456"
    assert "--stats-file" in command and "--quiet" in command

    # Real-link startup can wait for the post-bind stats snapshot instead of
    # connecting to the listener. That must not open or pre-warm the remote
    # backend path, while the default synthetic behavior above stays intact.
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(runner.subprocess, "Popen",
                                               side_effect=fake_popen))
        connect = stack.enter_context(mock.patch.object(
            runner.socket, "create_connection",
            side_effect=AssertionError("non-connecting readiness required")))
        controller = runner.ProxyController(args, "/tmp")
        stack.enter_context(mock.patch.object(
            controller, "snapshot", return_value={"bytes_total": 0}))
        controller.start(123456, connect_readiness_probe=False)
    connect.assert_not_called()

    segments = [{"connections_total": 2, "bytes_up": 40,
                 "bytes_down": 60, "bytes_total": 100,
                 "budget_exceeded": False},
                {"connections_total": 1, "bytes_up": 20,
                 "bytes_down": 30, "bytes_total": 50,
                 "budget_exceeded": False}]
    assert runner._remaining_budget(200, segments[:1]) == 100
    summary = runner._proxy_summary(segments, 200)
    assert summary["forwarded_bytes"] == 150
    assert summary["connections_total"] == 3
    assert summary["bytes_up"] == 60 and summary["bytes_down"] == 90
    assert summary["budget_exceeded"] is False
    assert runner._proxy_summary(segments, 149)["budget_exceeded"] is True

    root = "/Volumes/product-test"
    assert runner._mount_owned_by_product(
        "127.0.0.1:/ on %s (nfs, local)" % root, root, "nfs")
    assert not runner._mount_owned_by_product(
        "/dev/disk9s1 on %s (apfs, local)" % root, root, "nfs")
    fuse = "/Users/test/.juicemount/fuse-internal"
    assert runner._mount_owned_by_product(
        "JuiceFS:test on %s (macfuse, local)" % fuse, fuse, "fuse")

    # Failed JSON is not proof of absence. An HTTP-error or malformed-response
    # listener still accepts TCP, so only ECONNREFUSED counts as closed.
    open_socket = FakeControlSocket(0)
    with mock.patch.object(runner.socket, "socket", return_value=open_socket):
        assert runner._control_tcp_state(
            "http://127.0.0.1:11050") == "open"
    assert open_socket.closed is True
    closed_socket = FakeControlSocket(errno.ECONNREFUSED)
    with mock.patch.object(runner.socket, "socket", return_value=closed_socket):
        assert runner._control_tcp_state(
            "http://127.0.0.1:11050") == "closed"
    unknown_socket = FakeControlSocket(errno.ETIMEDOUT)
    with mock.patch.object(runner.socket, "socket", return_value=unknown_socket):
        assert runner._control_tcp_state(
            "http://127.0.0.1:11050") == "unknown"

    # A successful clean-stop is followed by an independent, immediately
    # prelaunch proof: no product process, target NFS, private FUSE, or stale
    # loopback control endpoint may remain.
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(runner, "_product_processes",
                                               return_value=[]))
        stack.enter_context(mock.patch.object(runner, "_mount_line",
                                               return_value=None))
        stack.enter_context(mock.patch.object(runner, "_control_tcp_state",
                                               return_value="closed"))
        cold = runner._require_cold_start_precondition(
            "loopback-test-control", root)
    assert cold == {
        "status": "PASS",
        "product_process_count": 0,
        "target_nfs_mount_present": False,
        "private_fuse_mount_present": False,
        "control_tcp_port_state": "closed",
        "control_tcp_port_proven_closed": True,
    }

    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(
            runner, "_product_processes",
            return_value=[(123, pathlib.Path("/tmp/Test.app/Contents/MacOS/JuiceMount"))]))
        stack.enter_context(mock.patch.object(runner, "_mount_line",
                                               return_value=None))
        stack.enter_context(mock.patch.object(runner, "_control_tcp_state",
                                               return_value="closed"))
        assert_raises_code(runner, "cold_start_precondition_failed",
                           runner._require_cold_start_precondition,
                           "loopback-test-control", root)

    # Direct launch visibility comes from an independent ps inventory of the
    # exact Popen PID and exact candidate binary. It is timed from the caller's
    # launch-command origin, not from _launch_bundle return.
    fake_binary = pathlib.Path("/usr/bin/true")
    candidate = FakeAppProcess(pid=4321)
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(runner, "_app_binary",
                                               return_value=fake_binary))
        stack.enter_context(mock.patch.object(runner.subprocess, "Popen",
                                               return_value=candidate))
        stack.enter_context(mock.patch.object(
            runner, "_product_processes",
            return_value=[(4321, fake_binary)]))
        stack.enter_context(mock.patch.object(
            runner.time, "monotonic", side_effect=[10.0, 10.0, 10.25]))
        launched = runner._launch_bundle(
            pathlib.Path("/tmp/Candidate.app"), direct=True,
            started_at=10.0)
    assert launched["pid"] == 4321
    assert launched["binary"] == fake_binary.resolve()
    assert launched["visible_ms"] == 250.0

    # A candidate that exits before appearing in the independent process
    # inventory is a startup failure, never a delayed visibility sample.
    exited = FakeAppProcess(pid=4321, returncode=1)
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(runner, "_app_binary",
                                               return_value=fake_binary))
        stack.enter_context(mock.patch.object(runner.subprocess, "Popen",
                                               return_value=exited))
        stack.enter_context(mock.patch.object(runner, "_product_processes",
                                               return_value=[]))
        stack.enter_context(mock.patch.object(
            runner.time, "monotonic", side_effect=[10.0, 10.0]))
        assert_raises_code(runner, "app_exited_during_start",
                           runner._launch_bundle,
                           pathlib.Path("/tmp/Candidate.app"), None, True, 10.0)

    # The captured PID/binary must remain present until serving readiness.
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(runner.time, "monotonic",
                                               return_value=10.1))
        stack.enter_context(mock.patch.object(runner, "_exact_product_present",
                                               return_value=False))
        assert_raises_code(
            runner, "app_exited_during_startup", runner._wait_startup,
            "loopback-test-control", 10.0, 10.0,
            {"pid": 4321, "binary": fake_binary})

    # A stale ps row cannot mask that the exact Popen child has already exited.
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(runner.time, "monotonic",
                                               return_value=10.1))
        stack.enter_context(mock.patch.object(runner, "_exact_product_present",
                                               return_value=True))
        assert_raises_code(
            runner, "app_exited_during_startup", runner._wait_startup,
            "loopback-test-control", 10.0, 10.0,
            {"pid": 4321, "binary": fake_binary,
             "process": FakeAppProcess(pid=4321, returncode=1)})
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(
            runner, "_mount_line",
            return_value="/dev/disk9s1 on %s (apfs, local)" % root))
        inventory = stack.enter_context(mock.patch.object(runner, "_process_rows"))
        assert_raises_code(runner, "mount_ownership_unverified",
                           runner._clean_stop, root)
    inventory.assert_not_called()

    # AppKit's quit reply is asynchronous. A valid hard-stop can spend more
    # than eight seconds in the product's bounded unmount/drain sequence, so a
    # process that finishes at 45 s must still count as graceful and receive no
    # TERM/KILL from the runner.
    class TeardownClock:
        def __init__(self):
            self.value = 0.0

        def monotonic(self):
            return self.value

        def sleep(self, seconds):
            self.value += seconds

    teardown_clock = TeardownClock()
    captured_product = [(4321, pathlib.Path("/tmp/Original.app/Contents/MacOS/JuiceMount"))]

    def late_products():
        return captured_product if teardown_clock.value < 45 else []

    def late_mount(_target):
        if teardown_clock.value < 45:
            return "127.0.0.1:/ on /Volumes/product-test (nfs, local)"
        return None

    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(runner.time, "monotonic",
                                               side_effect=teardown_clock.monotonic))
        stack.enter_context(mock.patch.object(runner.time, "sleep",
                                               side_effect=teardown_clock.sleep))
        stack.enter_context(mock.patch.object(runner, "_product_processes",
                                               side_effect=late_products))
        stack.enter_context(mock.patch.object(runner, "_mount_line",
                                               side_effect=late_mount))
        graceful = runner._wait_for_product_teardown(
            captured_product, (root, fuse),
            runner.GRACEFUL_PRODUCT_TEARDOWN_SEC)
    assert graceful is True
    assert teardown_clock.value >= 45 > 8

    # Integration of the same contract into _clean_stop: when the explicit
    # process+mount wait succeeds, the runner performs no forced signal. The
    # grace handed to that wait must cover all four noninteractive unmount
    # attempt ceilings, not the old eight-second window.
    product_binary = pathlib.Path("/tmp/Original.app/Contents/MacOS/JuiceMount")

    integration_clock = TeardownClock()
    clean_rows = [(4321, 1, str(product_binary))]

    def clean_process_rows():
        return clean_rows if integration_clock.value < 45 else []

    def clean_mount(target):
        if integration_clock.value >= 45:
            return None
        if target == root:
            return "127.0.0.1:/ on %s (nfs, local)" % root
        return "JuiceFS:test on %s (macfuse, local)" % target

    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(runner, "_mount_line",
                                               side_effect=clean_mount))
        stack.enter_context(mock.patch.object(runner, "_process_rows",
                                               side_effect=clean_process_rows))
        stack.enter_context(mock.patch.object(
            runner.time, "monotonic", side_effect=integration_clock.monotonic))
        stack.enter_context(mock.patch.object(
            runner.time, "sleep", side_effect=integration_clock.sleep))
        signals = stack.enter_context(mock.patch.object(runner, "_signal_pids"))
        stack.enter_context(mock.patch.object(runner, "_unmount",
                                               return_value=True))
        stack.enter_context(mock.patch.object(runner.subprocess, "run"))
        runner._clean_stop(root)
    assert runner.GRACEFUL_PRODUCT_TEARDOWN_SEC >= 5 + 5 + 15 + 15
    assert integration_clock.value >= 45 > runner.QUIT_REQUEST_TIMEOUT_SEC
    signals.assert_not_called()

    # A partial cleanup failure must not prevent rescue relaunch once the test
    # process is gone. Report cleanup separately from restoration readiness.
    restore_args = types.SimpleNamespace(
        mount_root=root, control="http://127.0.0.1:11050",
        startup_timeout=1)
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(
            runner, "_clean_stop",
            side_effect=runner.AcceptanceError("clean_stop_failed")))
        stack.enter_context(mock.patch.object(runner, "_product_processes",
                                               return_value=[]))
        launch = stack.enter_context(mock.patch.object(runner, "_launch_bundle"))
        stack.enter_context(mock.patch.object(runner, "_wait_normal_ready",
                                               return_value=True))
        restored, cleanup = runner._restore_initial_state(
            restore_args, True, pathlib.Path("/tmp/Original.app"))
    assert restored is True and cleanup is False
    assert launch.call_count == 1

    # A cleanup already completed while the proxy was still alive must not be
    # repeated after proxy withdrawal. Restoration proceeds directly to the
    # exact original bundle.
    with contextlib.ExitStack() as stack:
        clean = stack.enter_context(mock.patch.object(runner, "_clean_stop"))
        stack.enter_context(mock.patch.object(runner, "_product_processes",
                                               return_value=[]))
        direct_launch = stack.enter_context(
            mock.patch.object(runner, "_launch_bundle"))
        stack.enter_context(mock.patch.object(runner, "_wait_normal_ready",
                                               return_value=True))
        restored, cleanup = runner._restore_initial_state(
            restore_args, True, pathlib.Path("/tmp/Original.app"),
            cleanup_verified=True)
    assert restored is True and cleanup is True
    clean.assert_not_called()
    assert direct_launch.call_args.kwargs["direct"] is True

    # Lifecycle teardown must preserve the impaired route until after the test
    # app has stopped. Pulling the proxy first manufactures an offline
    # transition during NFS/FUSE teardown and has left the restored loopback
    # mount readable but unable to complete a write-readiness probe.
    events = []

    class OrderedProxy:
        def stop(self):
            events.append("proxy")
            return {"bytes_total": 1, "budget_exceeded": False}

    with mock.patch.object(
            runner, "_clean_stop",
            side_effect=lambda _root: events.append("app")):
        ordered_segments = []
        cleanup, proxy_error = runner._stop_test_environment(
            restore_args, OrderedProxy(), ordered_segments)
    assert cleanup is True and proxy_error is None
    assert events == ["app", "proxy"], events
    assert ordered_segments == [{"bytes_total": 1, "budget_exceeded": False}]

    # Both LaunchServices and direct launch paths receive an environment with
    # every test-only key removed. In particular the new FUSE read guard must
    # never escape the bounded acceptance child into the restored normal app.
    launch_calls = []

    def capture_run(command, **kwargs):
        launch_calls.append((command, kwargs))
        return types.SimpleNamespace(returncode=0)

    fake_binary = pathlib.Path("/usr/bin/true")
    inherited = {
        "JM_DEBUG_META_ADDR": "private-test-value",
        "JM_NET_FAKE_LINK": "private-test-value",
        runner.CONTENT_GUARD_ENV: "1",
    }
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.dict(runner.os.environ, inherited))
        stack.enter_context(mock.patch.object(runner, "_app_binary",
                                               return_value=fake_binary))
        stack.enter_context(mock.patch.object(
            runner, "_product_processes",
            return_value=[(123, fake_binary)]))
        stack.enter_context(mock.patch.object(runner.subprocess, "run",
                                               side_effect=capture_run))
        runner._launch_bundle(pathlib.Path("/tmp/Original.app"), direct=False)
    launch_env = launch_calls[0][1]["env"]
    for key in runner.TEST_ONLY_ENV_KEYS:
        assert key not in launch_env

    # Normal readiness is fail-closed while the acceptance-only content guard
    # is still visible, even when all component and serving states are green.
    normal_payloads = {
        "/warmup": {"serving": True},
        "/health": {"components": {"redis": "ok", "minio": "ok",
                                      "fuse": "ok", "nfs": "ok"}},
        "/offline": {"offline": False, "auto_offline": False,
                     "user_offline": False},
        "/metrics": {"network": {"simulated": False,
                                   "fuse_content_read_guard": False}},
    }
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(
            runner, "_safe_get_json",
            side_effect=lambda _control, path, timeout=2: normal_payloads[path]))
        assert runner._wait_normal_ready("loopback-test-control", 1) is True

    guarded_payloads = json.loads(json.dumps(normal_payloads))
    guarded_payloads["/metrics"]["network"][
        "fuse_content_read_guard"] = True
    clock = FakeClock()
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(runner.time, "monotonic",
                                               side_effect=clock.monotonic))
        stack.enter_context(mock.patch.object(runner.time, "sleep"))
        stack.enter_context(mock.patch.object(
            runner, "_safe_get_json",
            side_effect=lambda _control, path, timeout=2: guarded_payloads[path]))
        assert runner._wait_normal_ready("loopback-test-control", 1) is False

    # Main's default invocation must remain a non-mutating plan.  The report is
    # self-labeling and contains no private CLI value.
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        status = runner.main(["--mount-root", "private-test-value",
                              "--app", "private-app-value"])
    result = json.loads(output.getvalue())
    assert status == 0 and result["status"] == "DRY_RUN"
    assert result["safety"]["lifecycle_explicitly_authorized"] is False
    assert result["safety"]["metadata_proxy_hard_forwarded_byte_budget"] == 4 * 1024 * 1024
    assert "private-test-value" not in output.getvalue()

    print("cellular_synthetic_acceptance_selftest: PASS")


if __name__ == "__main__":
    main()
