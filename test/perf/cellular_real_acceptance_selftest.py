#!/usr/bin/env python3
"""Dependency-free safety tests for cellular_real_acceptance.py."""

import contextlib
import importlib.util
import inspect
import io
import json
import pathlib
import sys
import types
from unittest import mock


HERE = pathlib.Path(__file__).resolve().parent
SOURCE = HERE / "cellular_real_acceptance.py"


def load_runner():
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(HERE))
    try:
        spec = importlib.util.spec_from_file_location("cellular_real", SOURCE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.pop(0)


def assert_raises_code(runner, code, function, *args):
    try:
        function(*args)
    except runner.AcceptanceError as error:
        assert error.code == code, (error.code, code)
    else:
        raise AssertionError("expected AcceptanceError(%s)" % code)


def completed(stdout=""):
    return types.SimpleNamespace(stdout=stdout, returncode=0)


def inventory_runner(port_blocks, ifconfigs, target_route="utun6",
                     default_route="utun6", tailscale=True,
                     tailscale_route=True, tailscale_interface=True):
    hardware = "\n\n".join(port_blocks) + "\n"
    ps = ("99 1 /Applications/Tailscale.app/Contents/MacOS/Tailscale\n"
          if tailscale else "99 1 /usr/bin/other\n")
    primary_routes = ["192.168.0.0/24"] if tailscale_route else []
    tailscale_status = json.dumps({
        "BackendState": "Running",
        "Self": {"Online": True, "TailscaleIPs": ["100.64.0.10"]},
        "Peer": {"peer-key": {
            "Online": True,
            "TailscaleIPs": ["100.64.0.20"],
            "PrimaryRoutes": primary_routes,
        }},
    })

    def run(command, **_kwargs):
        if command[:3] == ["/sbin/route", "-n", "get"]:
            interface = default_route if command[-1] == "default" else target_route
            return completed("   interface: %s\n" % interface)
        if command == ["/usr/sbin/networksetup", "-listallhardwareports"]:
            return completed(hardware)
        if (len(command) == 3 and
                pathlib.Path(command[0]).name.lower() == "tailscale" and
                command[1:] == ["status", "--json"]):
            return completed(tailscale_status)
        if command[:1] == ["/sbin/ifconfig"]:
            if command[-1].startswith("utun"):
                address = ("100.64.0.10" if tailscale_interface else
                           "10.255.255.2")
                return completed(
                    "flags=8051<UP,POINTOPOINT,RUNNING,MULTICAST>\n"
                    "\tinet %s --> %s netmask 0xffffffff\n" %
                    (address, address))
            return completed(ifconfigs[command[-1]])
        if command == ["/bin/ps", "-axo", "pid=,ppid=,command="]:
            return completed(ps)
        raise AssertionError("unexpected command shape: %r" % (command,))

    return run


def active_ifconfig(address, mask="0xffffff00"):
    return ("flags=8863<UP,BROADCAST,SMART,RUNNING,SIMPLEX,MULTICAST>\n"
            "\tinet %s netmask %s broadcast 255.255.255.255\n"
            "\tstatus: active\n" % (address, mask))


def main():
    runner = load_runner()

    assert runner._classify_hardware_port("Wi-Fi") == "wifi"
    assert runner._classify_hardware_port(
        "Thunderbolt Ethernet Slot 2") == "wired"
    assert runner._classify_hardware_port("iPhone USB") == "cellular"
    assert runner._classify_hardware_port("Bluetooth PAN") == "other"

    # The known invalid baseline must fail even though both the NAS and default
    # route are on a utun. The physical path is active 10G Ethernet and the NAS
    # remains on the directly attached /24.
    ethernet_ports = [
        "Hardware Port: Thunderbolt Ethernet Slot 2\n"
        "Device: en21\nEthernet Address: aa:bb:cc:dd:ee:ff"
    ]
    ethernet_ifconfigs = {"en21": active_ifconfig("192.168.0.60")}
    with mock.patch.object(
            runner.subprocess, "run",
            side_effect=inventory_runner(ethernet_ports, ethernet_ifconfigs)):
        evidence = runner._collect_route_evidence(
            "192.168.0.197:30179", "wifi-hotspot")
    assert evidence["status"] == "FAIL"
    assert evidence["target_route_is_private_overlay"] is True
    assert evidence["target_on_physical_subnet"] is True
    assert "wired_underlay_active" in evidence["failure_codes"]
    encoded = json.dumps(evidence)
    for private in ("en21", "utun6", "192.168.0.60", "192.168.0.197",
                    "Thunderbolt Ethernet Slot 2", "aa:bb:cc:dd:ee:ff"):
        assert private not in encoded

    # A single active Wi-Fi interface, explicitly attested as the phone's
    # hotspot, with a private route advertised by an online Tailscale peer and
    # no physical subnet overlap reaches the prelaunch gate. Final acceptance
    # still requires Network.framework's expensive/constrained product proof.
    wifi_ports = [
        "Hardware Port: Wi-Fi\nDevice: en0\n"
        "Ethernet Address: 11:22:33:44:55:66"
    ]
    wifi_ifconfigs = {"en0": active_ifconfig("172.20.10.2", "0xfffffff0")}
    with mock.patch.object(
            runner.subprocess, "run",
            side_effect=inventory_runner(wifi_ports, wifi_ifconfigs)):
        good = runner._require_route_evidence(
            "192.168.0.197:30179", "wifi-hotspot")
    assert good["status"] == "PASS"
    assert good["active_physical_types"] == {
        "cellular": 0, "wifi": 1, "wired": 0, "other": 0}
    assert good["tailscale_route_owner_linked"] is True
    assert good["tailscale_route_interface_owned"] is True
    assert good["cellular_cost_proof_deferred_to_product"] is True
    assert good["aggregate_tunnel_counters_used_for_attribution"] is False

    # A Tailscale-looking process beside another VPN's utun cannot claim the
    # route. Local Tailscale status must link the target to a selected online
    # peer route or the currently selected online exit node.
    with mock.patch.object(
            runner.subprocess, "run",
            side_effect=inventory_runner(wifi_ports, wifi_ifconfigs,
                                         tailscale_route=False)):
        wrong_tunnel = runner._collect_route_evidence(
            "192.168.0.197:30179", "wifi-hotspot")
    assert "tailscale_route_owner_not_linked" in wrong_tunnel["failure_codes"]

    with mock.patch.object(
            runner.subprocess, "run",
            side_effect=inventory_runner(wifi_ports, wifi_ifconfigs,
                                         tailscale_interface=False)):
        foreign_utun = runner._collect_route_evidence(
            "192.168.0.197:30179", "wifi-hotspot")
    assert "tailscale_route_interface_not_owned" in \
        foreign_utun["failure_codes"]

    # Direct LAN routing is never accepted as a Tailscale cellular pass.
    with mock.patch.object(
            runner.subprocess, "run",
            side_effect=inventory_runner(wifi_ports, wifi_ifconfigs,
                                         target_route="en0",
                                         default_route="en0")):
        direct = runner._collect_route_evidence(
            "192.168.0.197:30179", "wifi-hotspot")
    assert "target_route_not_private_overlay" in direct["failure_codes"]

    with mock.patch.object(
            runner.subprocess, "run",
            side_effect=inventory_runner(wifi_ports, wifi_ifconfigs,
                                         target_route="utun6",
                                         default_route="utun9")):
        competing_vpn = runner._collect_route_evidence(
            "192.168.0.197:30179", "wifi-hotspot")
    assert "default_overlay_not_target_tunnel" in \
        competing_vpn["failure_codes"]

    # More than one active physical interface is ambiguous even if one is the
    # operator-attested hotspot.
    ambiguous_ports = wifi_ports + [
        "Hardware Port: Bluetooth PAN\nDevice: en8\n"
        "Ethernet Address: 66:55:44:33:22:11"
    ]
    ambiguous_configs = dict(wifi_ifconfigs)
    ambiguous_configs["en8"] = active_ifconfig("172.21.1.2")
    with mock.patch.object(
            runner.subprocess, "run",
            side_effect=inventory_runner(ambiguous_ports, ambiguous_configs)):
        ambiguous = runner._collect_route_evidence(
            "192.168.0.197:30179", "wifi-hotspot")
    assert "physical_underlay_ambiguous" in ambiguous["failure_codes"]
    assert "wifi_hotspot_underlay_not_proved" in ambiguous["failure_codes"]

    assert_raises_code(
        runner, "redis_target_must_be_numeric_private_address",
        runner._private_target, "example.test:6379")
    assert_raises_code(
        runner, "redis_target_must_be_numeric_private_address",
        runner._private_target, "8.8.8.8:6379")

    class FakeSocket:
        def __init__(self, status):
            self.status = status

        def settimeout(self, _timeout):
            pass

        def connect_ex(self, _target):
            return self.status

        def close(self):
            pass

    with mock.patch.object(runner.socket, "socket",
                           return_value=FakeSocket(61)):
        assert runner._control_tcp_listener_present(
            "http://127.0.0.1:11050") is False
    with mock.patch.object(runner.socket, "socket",
                           return_value=FakeSocket(0)):
        assert runner._control_tcp_listener_present(
            "http://127.0.0.1:11050") is True

    cold = {
        "status": "PASS",
        "product_process_count": 0,
        "target_nfs_mount_present": False,
        "private_fuse_mount_present": False,
        "control_endpoint_responding": False,
    }
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(
            runner.shared, "_require_cold_start_precondition",
            return_value=dict(cold)))
        stack.enter_context(mock.patch.object(
            runner, "_control_tcp_listener_present", return_value=False))
        proved = runner._require_zero_prelaunch(
            "http://127.0.0.1:11050", "/Volumes/test")
    assert proved["status"] == "PASS"
    assert proved["control_tcp_listener_present"] is False

    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(
            runner.shared, "_require_cold_start_precondition",
            return_value=dict(cold)))
        stack.enter_context(mock.patch.object(
            runner, "_control_tcp_listener_present", return_value=True))
        assert_raises_code(runner, "control_listener_still_present",
                           runner._require_zero_prelaunch,
                           "http://127.0.0.1:11050", "/Volumes/test")

    # Real startup requires non-simulated metrics and the hard FUSE guard. All
    # times are derived from the supplied launch origin.
    payloads = {
        "/warmup": {"serving": True, "phase": "steady"},
        "/health": {"healthy": True,
                    "components": {"redis": "ok", "minio": "ok",
                                   "fuse": "ok", "nfs": "ok"}},
        "/offline": {"offline": False, "auto_offline": False,
                     "user_offline": False},
        "/metrics": {"redis": {"dials": 1, "round_trips": 2},
                     "network": {"simulated": False,
                                    "fuse_content_read_guard": True,
                                    "class": "slow", "have_rtt": True,
                                    "have_bandwidth": False,
                                    "rtt_ms": 175.2,
                                    "high_latency": True,
                                    "backend_route_tunneled": True,
                                    "route_ceiling_applied": True,
                                    "route_ceiling_class": "slow",
                                    "host_network_cost_observed": True,
                                    "host_network_expensive": True,
                                    "host_network_constrained": False}},
    }
    product = {"pid": 42, "binary": pathlib.Path("/tmp/Test")}
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(
            runner.time, "monotonic", side_effect=[10.1, 10.2, 10.3]))
        stack.enter_context(mock.patch.object(
            runner, "_only_expected_product_present", return_value=True))
        stack.enter_context(mock.patch.object(
            runner.shared, "_safe_get_json",
            side_effect=lambda _control, path, timeout=2: payloads[path]))
        startup, metrics = runner._wait_real_startup(
            "loopback-test-control", 2, 10.0, product)
    assert startup["control_ready_ms"] == 200.0
    assert startup["serving_ready_ms"] == 300.0
    assert startup["real_link_policy_confirmed"] is True
    assert startup["original_backend_route_policy_confirmed"] is True
    assert startup["positive_measured_rtt_confirmed"] is True
    assert startup["cellular_cost_policy_confirmed"] is True
    assert startup["redis_control_observed"] is True
    assert startup["zero_fuse_content_guard_confirmed"] is True
    assert metrics == payloads["/metrics"]

    link = runner._safe_link_metrics(payloads["/metrics"])
    assert runner._real_policy_proof(link) == {
        "private_route_policy": True,
        "positive_measured_rtt": True,
        "cellular_cost_policy": True,
        "real_guarded_policy": True,
    }
    missing_route = dict(link)
    missing_route["backend_route_tunneled"] = None
    assert runner._real_policy_proof(missing_route)[
        "real_guarded_policy"] is False
    zero_rtt = dict(link)
    zero_rtt["rtt_ms"] = 0
    assert runner._real_policy_proof(zero_rtt)[
        "real_guarded_policy"] is False
    simulated = dict(link)
    simulated["simulated"] = True
    assert runner._real_policy_proof(simulated)[
        "real_guarded_policy"] is False
    ordinary_wifi = dict(link)
    ordinary_wifi["host_network_expensive"] = False
    ordinary_wifi["host_network_constrained"] = False
    assert runner._real_policy_proof(ordinary_wifi)[
        "cellular_cost_policy"] is False
    assert runner._real_policy_proof(ordinary_wifi)[
        "real_guarded_policy"] is False

    assert runner._complete_online_schema(
        payloads["/health"], payloads["/offline"]) == (True, True)
    assert runner._complete_online_schema(
        {"healthy": True, "components": {"redis": "ok"}},
        payloads["/offline"])[0] is False
    assert runner._complete_online_schema(payloads["/health"], {})[1] is False
    assert runner._redis_control_observed(payloads["/metrics"]) is True
    assert runner._redis_control_observed({"redis": {"dials": 0,
                                                       "round_trips": 2}}) is False

    with mock.patch.object(
            runner.shared, "_product_processes",
            return_value=[(42, pathlib.Path("/tmp/Test"))]):
        assert runner._only_expected_product_present(product) is True
    with mock.patch.object(
            runner.shared, "_product_processes",
            return_value=[(42, pathlib.Path("/tmp/Test")),
                          (43, pathlib.Path("/tmp/Test"))]):
        assert runner._only_expected_product_present(product) is False

    args = runner._parse_args([])
    assert args.delay_ms == args.jitter_ms == 0
    assert args.down_mbps == args.up_mbps == 0
    assert args.budget_bytes == 4 * 1024 * 1024
    assert args.outage_recovery is False

    # A passing report needs positive bidirectional traffic through the exact
    # bounded proxy. Zero proxy traffic means the runtime ignored/bypassed
    # JM_DEBUG_META_ADDR and attribution is unknown, never a zero-byte pass.
    evaluation = {
        "failures": [],
        "cold_start_precondition": {
            "status": "PASS", "product_process_count": 0,
            "target_nfs_mount_present": False,
            "private_fuse_mount_present": False,
            "control_endpoint_responding": False,
            "control_tcp_listener_present": False,
        },
        "route_and_underlay_evidence": {
            "status": "PASS", "stable_after_zero_state": True,
        },
        "startup": {
            "timing_origin": "launch_command", "process_present_ms": 1,
            "control_ready_ms": 2, "serving_ready_ms": 3,
            "real_link_policy_confirmed": True,
            "original_backend_route_policy_confirmed": True,
            "positive_measured_rtt_confirmed": True,
            "cellular_cost_policy_confirmed": True,
            "redis_control_observed": True,
            "zero_fuse_content_guard_confirmed": True,
        },
        "navigation": {
            "first_cached_root_from_launch_ms": 4, "cold_root_ms": 1,
            "warm_root": {"p95_ms": 1},
            "concurrent_root": {"p95_ms": 1},
            "entry_count_consistent": True, "entry_count": 1,
        },
        "traffic_gate": {
            "backend_counters_available": True,
            "object_get_bytes_delta": 0, "object_put_bytes_delta": 0,
            "cache_miss_bytes_delta": 0, "cold_subread_delta": 0,
            "thumbnail_hydration_delta": 0, "prefetched_block_delta": 0,
            "readdir_mirror_hit_delta": 1,
        },
        "metadata_proxy": {
            "budget_exceeded": False, "forwarded_bytes": 20,
            "connections_total": 1, "bytes_up": 10, "bytes_down": 10,
        },
        "warm_transition": {
            "serving_after_navigation": True, "phase_after_navigation": "steady",
        },
        "outage_recovery": {"status": "SKIP"},
    }
    runner._evaluate(evaluation, args)
    assert evaluation["status"] == "PASS", evaluation["failures"]
    bypass = json.loads(json.dumps(evaluation))
    bypass["failures"] = []
    bypass["metadata_proxy"].update({
        "forwarded_bytes": 0, "connections_total": 0,
        "bytes_up": 0, "bytes_down": 0,
    })
    runner._evaluate(bypass, args)
    assert bypass["status"] == "FAIL"
    assert "redis_proxy_attribution_missing" in bypass["failures"]

    assert_raises_code(
        runner, "explicit_cellular_confirmation_required",
        runner._parse_args,
        ["--lifecycle", "--redis-target", "192.168.1.10:6379",
         "--underlay-kind", "wifi-hotspot"])
    rejected = io.StringIO()
    with contextlib.redirect_stdout(rejected):
        rejected_status = runner.main([
            "--lifecycle", "--redis-target", "192.168.1.10:6379",
            "--underlay-kind", "wifi-hotspot"])
    assert rejected_status == 2
    assert "192.168.1.10" not in rejected.getvalue()
    rejected_doc = json.loads(rejected.getvalue())
    assert rejected_doc["measurement"]["measurement_performed"] is False
    assert rejected_doc["measurement"]["real_cellular"] is False

    lifecycle_source = inspect.getsource(runner._run_lifecycle)
    assert "JM_NET_FAKE_LINK" not in lifecycle_source
    assert "shared.CONTENT_GUARD_ENV" in lifecycle_source
    assert "launch_started = time.monotonic()" in lifecycle_source
    assert "started_at=launch_started" in lifecycle_source
    assert "shared._navigation_sample" in lifecycle_source
    assert lifecycle_source.count(
        "connect_readiness_probe=False") == 2
    assert lifecycle_source.rfind(
        "launch_route = _require_route_evidence") < lifecycle_source.rfind(
            "report[\"cold_start_precondition\"] = _require_zero_prelaunch")
    assert lifecycle_source.rfind(
        "report[\"cold_start_precondition\"] = _require_zero_prelaunch") < \
        lifecycle_source.index("launch_started = time.monotonic()")

    report_args = types.SimpleNamespace(
        mount_root="/Volumes/private-test", redis_target="192.168.0.197:30179",
        proxy_listen="127.0.0.1:16379", control="http://127.0.0.1:11050",
        app="/private/Test.app")
    runner._assert_report_redacted({"evidence": good}, report_args)
    assert_raises_code(
        runner, "result_redaction_guard_failed",
        runner._assert_report_redacted,
        {"evidence": {"interface_classification": "utun6"}}, report_args)

    # Default execution is a plan. It may inspect local files, but must not
    # inventory routes, interfaces, processes, mounts, or listeners.
    output = io.StringIO()
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(
            runner.subprocess, "run",
            side_effect=AssertionError("dry-run must not execute commands")))
        with contextlib.redirect_stdout(output):
            status = runner.main([])
    dry = json.loads(output.getvalue())
    assert status == 0 and dry["status"] == "DRY_RUN"
    assert dry["measurement"]["target_kind"] == "real_cellular"
    assert dry["measurement"]["measurement_performed"] is False
    assert dry["measurement"]["real_cellular"] is False
    assert dry["measurement"]["private_route_proved"] is False
    assert dry["safety"]["lifecycle_explicitly_authorized"] is False
    assert dry["safety"][
        "metadata_control_hard_forwarded_byte_budget"] == 4 * 1024 * 1024
    assert dry["safety"][
        "aggregate_tunnel_counters_used_for_attribution"] is False

    print("cellular_real_acceptance_selftest: PASS")


if __name__ == "__main__":
    main()
