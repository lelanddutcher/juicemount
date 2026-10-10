#!/usr/bin/env python3
"""Fail-closed, low-data, real-cellular JuiceMount acceptance runner.

The default invocation is a non-mutating dry run. A live run requires both
``--lifecycle`` and the exact operator attestation documented by ``--help``.
It proves that the tested launch begins with no JuiceMount process, target NFS
mount, private FUSE mount, or control listener; starts one monotonic clock
immediately before launching the candidate; and measures process, control,
serving, and first-root readiness from that same origin.

Only the mount root is enumerated. Entry names and paths are never inspected or
emitted. The acceptance-only FUSE content guard is mandatory. Redis is routed
through the existing bounded TCP proxy with no artificial delay, jitter, or
rate shaping, giving the real link a hard 4 MiB metadata/control budget.
Aggregate utun counters are explicitly rejected as attribution because they
include unrelated VPN traffic.

An outage/recovery phase is disabled by default and runs only when separately
authorized with ``--outage-recovery``. This script never toggles a system
network interface or Tailscale; the optional phase withdraws only its bounded
Redis proxy.
"""

import ipaddress
import json
import math
import os
import pathlib
import re
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse

import cellular_synthetic_acceptance as shared


NAME = "juicemount_real_cellular_private_route_acceptance"
CELLULAR_CONFIRMATION = "CELLULAR_TAILSCALE_LOW_DATA"
REAL_BUDGET_BYTES = 4 * 1024 * 1024
ALLOWED_UNDERLAYS = ("wifi-hotspot", "usb-tether", "system-cellular")
TAILSCALE_COMMANDS = (
    "/Applications/Tailscale.app/Contents/MacOS/Tailscale",
    "/opt/homebrew/bin/tailscale",
    "/usr/local/bin/tailscale",
)
REQUIRED_HEALTH_COMPONENTS = frozenset(("redis", "minio", "fuse", "nfs"))


AcceptanceError = shared.AcceptanceError
SafeArgumentParser = shared.SafeArgumentParser


def _run_text(command, code):
    try:
        completed = subprocess.run(command, capture_output=True, text=True,
                                   timeout=8, check=True)
    except Exception:
        raise AcceptanceError(code)
    return completed.stdout


def _route_interface(destination):
    output = _run_text(["/sbin/route", "-n", "get", destination],
                       "route_inventory_failed")
    match = re.search(r"^\s*interface:\s*(\S+)\s*$", output, re.MULTILINE)
    if not match:
        raise AcceptanceError("route_inventory_failed")
    return match.group(1)


def _classify_hardware_port(label):
    value = label.strip().lower()
    if any(token in value for token in
           ("iphone", "cellular", "mobile broadband", "wwan", "modem")):
        return "cellular"
    if any(token in value for token in ("wi-fi", "wifi", "airport")):
        return "wifi"
    if any(token in value for token in
           ("ethernet", "thunderbolt bridge", "usb lan")):
        return "wired"
    return "other"


def _hardware_ports():
    """Return device/type pairs while discarding labels and MAC addresses."""
    output = _run_text(["/usr/sbin/networksetup", "-listallhardwareports"],
                       "hardware_port_inventory_failed")
    result = []
    label = None
    for raw in output.splitlines():
        line = raw.strip()
        if line.startswith("Hardware Port:"):
            label = line.split(":", 1)[1].strip()
        elif line.startswith("Device:") and label is not None:
            device = line.split(":", 1)[1].strip()
            if device and not device.startswith(("utun", "lo", "awdl", "llw")):
                result.append((device, _classify_hardware_port(label)))
            label = None
    if not result:
        raise AcceptanceError("hardware_port_inventory_failed")
    return result


def _hex_netmask(value):
    try:
        if value.lower().startswith("0x"):
            return str(ipaddress.IPv4Address(int(value, 16)))
        return str(ipaddress.IPv4Address(value))
    except ValueError:
        raise AcceptanceError("physical_interface_inventory_failed")


def _physical_interface_state(device, kind):
    output = _run_text(["/sbin/ifconfig", device],
                       "physical_interface_inventory_failed")
    header = output.splitlines()[0] if output.splitlines() else ""
    flags_active = "UP" in header and "RUNNING" in header
    status_match = re.search(r"^\s*status:\s*(\S+)\s*$", output,
                             re.MULTILINE)
    status_active = status_match is not None and status_match.group(1) == "active"
    networks = []
    for match in re.finditer(
            r"^\s*inet\s+(\S+)\s+netmask\s+(\S+)", output, re.MULTILINE):
        try:
            address = ipaddress.IPv4Address(match.group(1))
            if address.is_loopback or address.is_link_local:
                continue
            mask = _hex_netmask(match.group(2))
            networks.append(ipaddress.IPv4Network((str(address), mask),
                                                   strict=False))
        except (ValueError, AcceptanceError):
            raise AcceptanceError("physical_interface_inventory_failed")
    return {
        "device": device,
        "kind": kind,
        # `status: active` is enough to prove a live physical link even when
        # its underlay is IPv6-only. Requiring an IPv4 address here would let a
        # wired IPv6 Tailscale transport masquerade as "no wired underlay".
        "active": status_active or (bool(networks) and flags_active),
        "networks": networks,
    }


def _tailscale_process_present():
    for _pid, _ppid, command in shared._process_rows():
        lowered = command.lower()
        if ("/tailscale.app/" in lowered or "io.tailscale." in lowered or
                re.search(r"(?:^|/)tailscale(?:\s|$)", lowered)):
            return True
    return False


def _tailscale_status():
    """Read LocalAPI-backed CLI status and retain no topology identifiers."""
    for binary in TAILSCALE_COMMANDS:
        try:
            completed = subprocess.run(
                [binary, "status", "--json"], capture_output=True, text=True,
                timeout=8, check=True)
            status = json.loads(completed.stdout)
        except Exception:
            continue
        if isinstance(status, dict):
            return status
    raise AcceptanceError("tailscale_status_unavailable")


def _route_list_contains_target(values, target):
    if not isinstance(values, list):
        return False
    for raw in values:
        if not isinstance(raw, str):
            continue
        try:
            network = ipaddress.ip_network(
                raw if "/" in raw else raw + "/32", strict=False)
        except ValueError:
            continue
        if isinstance(network, ipaddress.IPv4Network) and target in network:
            return True
    return False


def _tunnel_addresses(interface):
    output = _run_text(["/sbin/ifconfig", interface],
                       "tunnel_interface_inventory_failed")
    addresses = set()
    for match in re.finditer(r"^\s*inet6?\s+(\S+)", output, re.MULTILINE):
        raw = match.group(1).split("%", 1)[0]
        try:
            address = ipaddress.ip_address(raw)
        except ValueError:
            continue
        if not address.is_loopback and not address.is_link_local:
            addresses.add(address)
    if not addresses:
        raise AcceptanceError("tunnel_interface_inventory_failed")
    return addresses


def _tailscale_route_proof(status, target, tunnel_addresses):
    """Bind the private target to an online Tailscale peer or exit node."""
    if not isinstance(status, dict):
        return {
            "status_available": False,
            "backend_running": False,
            "self_online": False,
            "route_interface_owned": False,
            "route_owner_linked": False,
            "selected_exit_node_online": False,
        }
    self_status = status.get("Self")
    self_online = (isinstance(self_status, dict) and
                   self_status.get("Online") is True and
                   isinstance(self_status.get("TailscaleIPs"), list) and
                   bool(self_status.get("TailscaleIPs")))
    self_addresses = set()
    if isinstance(self_status, dict):
        for raw in self_status.get("TailscaleIPs") or ():
            try:
                self_addresses.add(ipaddress.ip_address(raw))
            except (TypeError, ValueError):
                continue
    route_interface_owned = bool(self_addresses.intersection(
        tunnel_addresses or set()))
    exit_status = status.get("ExitNodeStatus")
    selected_exit_online = (
        isinstance(exit_status, dict) and
        exit_status.get("Online") is True and
        (bool(exit_status.get("ID")) or
         bool(exit_status.get("TailscaleIPs"))))
    peer_route = False
    peers = status.get("Peer")
    if isinstance(peers, dict):
        peers = peers.values()
    elif not isinstance(peers, list):
        peers = ()
    for peer in peers:
        if not isinstance(peer, dict) or peer.get("Online") is not True:
            continue
        if (_route_list_contains_target(peer.get("TailscaleIPs"), target) or
                _route_list_contains_target(peer.get("PrimaryRoutes"), target)):
            peer_route = True
            break
    backend_running = status.get("BackendState") == "Running"
    return {
        "status_available": True,
        "backend_running": backend_running,
        "self_online": self_online,
        "route_interface_owned": route_interface_owned,
        "route_owner_linked": (backend_running and self_online and
                               route_interface_owned and
                               (peer_route or selected_exit_online)),
        "selected_exit_node_online": selected_exit_online,
    }


def _private_target(value):
    host, _port = shared._validate_redis_target(value)
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        raise AcceptanceError("redis_target_must_be_numeric_private_address")
    if not isinstance(address, ipaddress.IPv4Address):
        # Physical-subnet exclusion below is deliberately IPv4-only. Refuse an
        # IPv6 target instead of claiming that a direct LAN route was excluded.
        raise AcceptanceError("redis_target_must_be_numeric_private_address")
    shared_space = address in ipaddress.IPv4Network("100.64.0.0/10")
    if not (address.is_private or shared_space):
        raise AcceptanceError("redis_target_must_be_numeric_private_address")
    return host, address


def _collect_route_evidence(redis_target, expected_underlay):
    """Collect only categorical evidence; never return an interface or IP."""
    target_host, target_address = _private_target(redis_target)
    target_route = _route_interface(target_host)
    default_route = _route_interface("default")
    states = [_physical_interface_state(device, kind)
              for device, kind in _hardware_ports()]
    active = [state for state in states if state["active"]]
    counts = {kind: sum(1 for state in active if state["kind"] == kind)
              for kind in ("cellular", "wifi", "wired", "other")}
    target_on_physical_subnet = any(
        isinstance(target_address, ipaddress.IPv4Address) and
        any(target_address in network for network in state["networks"])
        for state in active)
    target_overlay = target_route.startswith("utun")
    default_overlay = default_route.startswith("utun")
    default_overlay_matches_target = (default_overlay and
                                      default_route == target_route)
    default_matches_active = any(state["device"] == default_route
                                 for state in active)
    tailscale = _tailscale_process_present()
    tunnel_addresses = (_tunnel_addresses(target_route)
                        if target_overlay else set())
    tailscale_proof = _tailscale_route_proof(
        _tailscale_status(), target_address, tunnel_addresses)

    failures = []
    if not target_overlay:
        failures.append("target_route_not_private_overlay")
    if target_on_physical_subnet:
        failures.append("target_still_on_physical_lan")
    if not tailscale:
        failures.append("tailscale_process_not_present")
    if not tailscale_proof["backend_running"]:
        failures.append("tailscale_backend_not_running")
    if not tailscale_proof["self_online"]:
        failures.append("tailscale_self_not_online")
    if not tailscale_proof["route_interface_owned"]:
        failures.append("tailscale_route_interface_not_owned")
    if not tailscale_proof["route_owner_linked"]:
        failures.append("tailscale_route_owner_not_linked")
    if counts["wired"]:
        failures.append("wired_underlay_active")
    if len(active) != 1:
        failures.append("physical_underlay_ambiguous")
    if not default_overlay and not default_matches_active:
        failures.append("default_route_underlay_unresolved")
    if default_overlay and not default_overlay_matches_target:
        failures.append("default_overlay_not_target_tunnel")
    if expected_underlay == "wifi-hotspot":
        if counts != {"cellular": 0, "wifi": 1, "wired": 0, "other": 0}:
            failures.append("wifi_hotspot_underlay_not_proved")
    else:
        if counts != {"cellular": 1, "wifi": 0, "wired": 0, "other": 0}:
            failures.append("cellular_underlay_not_proved")

    failures = sorted(set(failures))
    return {
        "status": "PASS" if not failures else "FAIL",
        "failure_codes": failures,
        "operator_underlay_kind": expected_underlay,
        "active_physical_interface_count": len(active),
        "active_physical_types": counts,
        "target_route_is_private_overlay": target_overlay,
        "target_on_physical_subnet": target_on_physical_subnet,
        "default_route_is_private_overlay": default_overlay,
        "default_overlay_matches_target_tunnel":
            default_overlay_matches_target,
        "default_route_resolves_to_active_underlay": default_matches_active,
        "tailscale_process_present": tailscale,
        "tailscale_status_available": tailscale_proof["status_available"],
        "tailscale_backend_running": tailscale_proof["backend_running"],
        "tailscale_self_online": tailscale_proof["self_online"],
        "tailscale_route_interface_owned":
            tailscale_proof["route_interface_owned"],
        "tailscale_route_owner_linked": tailscale_proof["route_owner_linked"],
        "tailscale_selected_exit_node_online":
            tailscale_proof["selected_exit_node_online"],
        # Network.framework's sanitized expensive/constrained signal is
        # required from the launched product before acceptance. A Wi-Fi device
        # plus operator text alone is never proof of a phone hotspot.
        "cellular_cost_proof_deferred_to_product": True,
        "aggregate_tunnel_counters_used_for_attribution": False,
    }


def _require_route_evidence(redis_target, expected_underlay):
    evidence = _collect_route_evidence(redis_target, expected_underlay)
    if evidence["status"] != "PASS":
        raise AcceptanceError(evidence["failure_codes"][0])
    return evidence


def _control_tcp_listener_present(control):
    shared._validate_loopback_http(control)
    parsed = urllib.parse.urlparse(control)
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.settimeout(0.5)
    try:
        status = probe.connect_ex((parsed.hostname, parsed.port))
    except OSError:
        raise AcceptanceError("control_listener_inventory_failed")
    finally:
        probe.close()
    if status == 0:
        return True
    # ECONNREFUSED is the only expected proof of an absent loopback listener.
    if status not in (61, 111):
        raise AcceptanceError("control_listener_inventory_failed")
    return False


def _require_zero_prelaunch(control, mount_root):
    state = shared._require_cold_start_precondition(control, mount_root)
    tcp_present = _control_tcp_listener_present(control)
    state["control_tcp_listener_present"] = tcp_present
    if tcp_present:
        state["status"] = "FAIL"
        raise AcceptanceError("control_listener_still_present")
    return state


def _safe_link_metrics(metrics):
    network = metrics.get("network") if isinstance(metrics, dict) else None
    if not isinstance(network, dict):
        return {
            "available": False,
            "simulated": None,
            "content_guard": None,
            "class": "unknown",
            "have_rtt": False,
            "have_bandwidth": False,
            "rtt_ms": None,
            "bandwidth_mbps": None,
            "high_latency": None,
            "backend_route_tunneled": None,
            "route_ceiling_applied": None,
            "route_ceiling_class": "",
            "host_network_cost_observed": None,
            "host_network_expensive": None,
            "host_network_constrained": None,
        }

    def finite_number(key):
        value = network.get(key)
        if isinstance(value, (int, float)) and math.isfinite(value):
            return round(float(value), 3)
        return None

    link_class = network.get("class")
    if link_class not in ("metered", "slow", "medium", "fast", "unknown"):
        link_class = "unknown"
    return {
        "available": True,
        "simulated": network.get("simulated")
        if isinstance(network.get("simulated"), bool) else None,
        "content_guard": network.get("fuse_content_read_guard")
        if isinstance(network.get("fuse_content_read_guard"), bool) else None,
        "class": link_class,
        "have_rtt": network.get("have_rtt") is True,
        "have_bandwidth": network.get("have_bandwidth") is True,
        "rtt_ms": finite_number("rtt_ms"),
        "bandwidth_mbps": finite_number("bandwidth_mbps"),
        "high_latency": network.get("high_latency")
        if isinstance(network.get("high_latency"), bool) else None,
        "backend_route_tunneled": network.get("backend_route_tunneled")
        if isinstance(network.get("backend_route_tunneled"), bool) else None,
        "route_ceiling_applied": network.get("route_ceiling_applied")
        if isinstance(network.get("route_ceiling_applied"), bool) else None,
        "route_ceiling_class": network.get("route_ceiling_class")
        if network.get("route_ceiling_class") in ("", "slow") else "invalid",
        "host_network_cost_observed": network.get(
            "host_network_cost_observed")
        if isinstance(network.get("host_network_cost_observed"), bool) else None,
        "host_network_expensive": network.get("host_network_expensive")
        if isinstance(network.get("host_network_expensive"), bool) else None,
        "host_network_constrained": network.get("host_network_constrained")
        if isinstance(network.get("host_network_constrained"), bool) else None,
    }


def _real_policy_proof(link):
    route = (link.get("backend_route_tunneled") is True and
             link.get("route_ceiling_applied") is True and
             link.get("route_ceiling_class") == "slow")
    rtt = (link.get("have_rtt") is True and
           isinstance(link.get("rtt_ms"), (int, float)) and
           math.isfinite(link["rtt_ms"]) and link["rtt_ms"] > 0)
    guarded = (link.get("simulated") is False and
               link.get("content_guard") is True)
    cellular_cost = (
        link.get("host_network_cost_observed") is True and
        (link.get("host_network_expensive") is True or
         link.get("host_network_constrained") is True))
    return {
        "private_route_policy": route,
        "positive_measured_rtt": rtt,
        "cellular_cost_policy": cellular_cost,
        "real_guarded_policy": route and rtt and guarded and cellular_cost,
    }


def _complete_online_schema(health, offline):
    components = health.get("components") if isinstance(health, dict) else None
    healthy = (
        isinstance(components, dict) and
        health.get("healthy") is True and
        REQUIRED_HEALTH_COMPONENTS.issubset(components) and
        all(value == "ok" for value in components.values()))
    online = (
        isinstance(offline, dict) and
        all(key in offline and offline[key] is False
            for key in ("offline", "auto_offline", "user_offline")))
    return healthy, online


def _redis_control_observed(metrics):
    redis = metrics.get("redis") if isinstance(metrics, dict) else None
    return (isinstance(redis, dict) and
            isinstance(redis.get("dials"), (int, float)) and
            redis.get("dials") > 0 and
            isinstance(redis.get("round_trips"), (int, float)) and
            redis.get("round_trips") > 0)


def _only_expected_product_present(expected_product):
    products = shared._product_processes()
    if len(products) != 1 or products[0][0] != expected_product["pid"]:
        return False
    try:
        return (products[0][1].resolve() ==
                pathlib.Path(expected_product["binary"]).resolve())
    except OSError:
        return False


def _wait_real_startup(control, timeout, launch_started, expected_product):
    deadline = launch_started + timeout
    transitions = []
    first_control_ms = None
    while time.monotonic() < deadline:
        if not _only_expected_product_present(expected_product):
            raise AcceptanceError("app_exited_during_startup")
        warmup = shared._safe_get_json(control, "/warmup", timeout=2)
        # Timestamp the completed response, not the moment immediately before
        # issuing it. On a slow link (or a busy local control server), the
        # latter can understate readiness by the entire request duration.
        control_observed_ms = (time.monotonic() - launch_started) * 1000.0
        if warmup is not None and first_control_ms is None:
            first_control_ms = control_observed_ms
        if isinstance(warmup, dict):
            phase = warmup.get("phase")
            if phase in ("starting", "indexing", "warming", "steady"):
                if not transitions or transitions[-1]["phase"] != phase:
                    transitions.append({"phase": phase,
                                        "elapsed_ms": round(
                                            control_observed_ms, 3)})
        health = shared._safe_get_json(control, "/health", timeout=2)
        offline = shared._safe_get_json(control, "/offline", timeout=2)
        metrics = shared._safe_get_json(control, "/metrics", timeout=2)
        healthy, online = _complete_online_schema(health, offline)
        link = _safe_link_metrics(metrics)
        # JM_DEBUG_META_ADDR intentionally sends Redis through loopback for
        # accounting. These three fields prove product policy still resolved
        # the original private backend route rather than misclassifying the
        # proxy as a fast local link.
        policy = _real_policy_proof(link)
        real_guarded = policy["real_guarded_policy"]
        redis_control = _redis_control_observed(metrics)
        serving = isinstance(warmup, dict) and warmup.get("serving") is True
        if serving and healthy and online and real_guarded and redis_control:
            if not _only_expected_product_present(expected_product):
                raise AcceptanceError("app_exited_during_startup")
            # Serving readiness is complete only after every required control
            # response and the final exact-process identity check succeed.
            serving_observed_ms = ((time.monotonic() - launch_started) *
                                   1000.0)
            return ({
                "control_ready_ms": round(
                    first_control_ms
                    if first_control_ms is not None else serving_observed_ms,
                    3),
                "serving_ready_ms": round(serving_observed_ms, 3),
                "phase_transitions": transitions,
                "ready_phase": warmup.get("phase"),
                "offline_and_banner_cleared": True,
                "real_link_policy_confirmed": True,
                "original_backend_route_policy_confirmed": True,
                "positive_measured_rtt_confirmed": True,
                "cellular_cost_policy_confirmed": True,
                "redis_control_observed": True,
                "zero_fuse_content_guard_confirmed": True,
                "link_measurement": link,
            }, metrics)
        time.sleep(0.5)
    raise AcceptanceError("startup_readiness_timeout")


def _base_report(args, mode):
    return {
        "schema": 1,
        "name": NAME,
        "status": "DRY_RUN" if mode == "dry-run" else "RUNNING",
        "mode": mode,
        "measurement": {
            "simulated": False,
            "target_kind": "real_cellular",
            "measurement_performed": False,
            "real_cellular": False,
            "scope": "root_metadata_only",
            "private_route_target": "tailscale",
            "private_route_proved": False,
            "operator_cellular_confirmation":
                args.confirm_cellular == CELLULAR_CONFIRMATION,
        },
        "cold_scope": {
            "process_restart": True,
            "persistent_metadata_and_block_cache_preserved": True,
            "zero_process_nfs_fuse_and_control_listener_required": True,
            "all_startup_timings_share_launch_command_origin": True,
            "connection_transition_is_not_accepted_as_cold_start": True,
        },
        "safety": {
            "default_is_non_mutating": True,
            "lifecycle_explicitly_authorized": bool(args.lifecycle),
            "outage_recovery_explicitly_authorized": bool(args.outage_recovery),
            "metadata_only": True,
            "opens_child_content": False,
            "emits_entry_names_or_source_paths": False,
            "zero_fuse_content_fallthrough_guard": True,
            "redis_accounting_proxy_has_artificial_impairment": False,
            "metadata_control_hard_forwarded_byte_budget": REAL_BUDGET_BYTES,
            "aggregate_tunnel_counters_used_for_attribution": False,
            "traffic_attribution": (
                "bounded_redis_proxy_and_app_backend_counters"),
        },
        "failures": [],
    }


def _assert_report_redacted(report, args):
    shared._assert_report_redacted(
        report, (args.mount_root, args.redis_target, args.proxy_listen,
                 args.control, args.app))
    encoded = json.dumps(report, sort_keys=True)
    forbidden_patterns = (
        r"\b(?:en|utun|bridge|awdl|llw)\d+\b",
        r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
        r"Hardware Port:",
        r"Ethernet Address:",
        r"Current Wi-Fi Network:",
    )
    if any(re.search(pattern, encoded, re.IGNORECASE)
           for pattern in forbidden_patterns):
        raise AcceptanceError("result_redaction_guard_failed")


def _emit(report, args):
    _assert_report_redacted(report, args)
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.json_out:
        shared._validate_output_location(args.json_out, args.mount_root)
        output = pathlib.Path(args.json_out).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(output.name + ".tmp")
        temporary.write_text(text, encoding="utf-8")
        os.replace(temporary, output)
    sys.stdout.write(text)


def _dry_run(args):
    report = _base_report(args, "dry-run")
    app = shared._resolve_test_app(args.app, [])
    report["prerequisites"] = {
        "bounded_proxy_contract": shared._proxy_contract_ok(),
        "app_bundle_available": bool(app and shared._app_binary(app).is_file()),
        "lifecycle_target_supplied": bool(args.redis_target),
        "cellular_attestation_supplied":
            args.confirm_cellular == CELLULAR_CONFIRMATION,
        "underlay_kind_supplied": args.underlay_kind in ALLOWED_UNDERLAYS,
    }
    report["planned_coverage"] = [
        "machine_and_operator_proved_cellular_tailscale_private_route",
        "verified_zero_app_nfs_private_fuse_and_control_listener",
        "one_launch_clock_for_process_control_serving_and_first_root",
        "root_only_cold_warm_and_concurrent_metadata_enumeration",
        "hard_content_read_guard_and_zero_remote_hydration",
        "four_mib_bounded_zero_impairment_redis_accounting_proxy",
        "optional_explicit_proxy_outage_recovery"
        if args.outage_recovery else "outage_recovery_not_authorized",
        "normal_app_state_restore",
    ]
    return report


def _evaluate(report, args):
    failures = list(report.get("failures") or [])
    cold = report.get("cold_start_precondition") or {}
    route = report.get("route_and_underlay_evidence") or {}
    startup = report.get("startup") or {}
    nav = report.get("navigation") or {}
    traffic = report.get("traffic_gate") or {}
    proxy = report.get("metadata_proxy") or {}

    if cold.get("status") != "PASS" or cold.get("product_process_count") != 0:
        failures.append("cold_start_precondition_missing_or_failed")
    if (cold.get("target_nfs_mount_present") or
            cold.get("private_fuse_mount_present") or
            cold.get("control_endpoint_responding") or
            cold.get("control_tcp_listener_present")):
        failures.append("cold_start_zero_state_not_proved")
    if route.get("status") != "PASS" or not route.get("stable_after_zero_state"):
        failures.append("cellular_private_route_not_proved")
    if (startup.get("timing_origin") != "launch_command" or
            startup.get("process_present_ms") is None or
            startup.get("control_ready_ms") is None or
            startup.get("serving_ready_ms") is None or
            nav.get("first_cached_root_from_launch_ms") is None):
        failures.append("cold_start_timing_incomplete")
    if not startup.get("real_link_policy_confirmed"):
        failures.append("real_link_policy_not_confirmed")
    if not startup.get("original_backend_route_policy_confirmed"):
        failures.append("original_backend_route_policy_not_confirmed")
    if not startup.get("positive_measured_rtt_confirmed"):
        failures.append("positive_measured_rtt_not_confirmed")
    if not startup.get("cellular_cost_policy_confirmed"):
        failures.append("cellular_cost_policy_not_confirmed")
    if not startup.get("redis_control_observed"):
        failures.append("redis_control_not_observed")
    if not startup.get("zero_fuse_content_guard_confirmed"):
        failures.append("content_read_guard_not_confirmed")
    if nav.get("cold_root_ms", float("inf")) > args.cold_root_max_ms:
        failures.append("cold_root_latency_over_budget")
    if (nav.get("warm_root") or {}).get("p95_ms", float("inf")) > args.warm_p95_ms:
        failures.append("warm_root_latency_over_budget")
    if ((nav.get("concurrent_root") or {}).get("p95_ms", float("inf")) >
            args.concurrent_p95_ms):
        failures.append("concurrent_root_latency_over_budget")
    if not nav.get("entry_count_consistent") or nav.get("entry_count", 0) <= 0:
        failures.append("root_metadata_enumeration_invalid")
    if not traffic.get("backend_counters_available"):
        failures.append("backend_byte_counters_unavailable")
    zero_keys = (
        "object_get_bytes_delta", "object_put_bytes_delta",
        "cache_miss_bytes_delta", "cold_subread_delta",
        "thumbnail_hydration_delta", "prefetched_block_delta",
    )
    if any(int(traffic.get(key, 0) or 0) != 0 for key in zero_keys):
        failures.append("content_plane_activity_observed")
    if traffic.get("readdir_mirror_hit_delta", 0) <= 0:
        failures.append("local_mirror_coverage_missing")
    if proxy.get("budget_exceeded") or proxy.get(
            "forwarded_bytes", REAL_BUDGET_BYTES + 1) > REAL_BUDGET_BYTES:
        failures.append("metadata_control_byte_budget_exceeded")
    if (proxy.get("connections_total", 0) <= 0 or
            proxy.get("bytes_up", 0) <= 0 or
            proxy.get("bytes_down", 0) <= 0 or
            proxy.get("forwarded_bytes", 0) <= 0):
        failures.append("redis_proxy_attribution_missing")
    warm = report.get("warm_transition") or {}
    if (not warm.get("serving_after_navigation") or
            warm.get("phase_after_navigation") in (None, "unknown", "starting")):
        failures.append("warm_serving_transition_incomplete")
    outage = report.get("outage_recovery") or {}
    if args.outage_recovery and outage.get("status") != "PASS":
        failures.append("outage_recovery_coverage_incomplete")
    if not args.outage_recovery and outage.get("status") != "SKIP":
        failures.append("unauthorized_outage_recovery_activity")

    report["failures"] = sorted(set(failures))
    report["status"] = "PASS" if not report["failures"] else "FAIL"


def _run_lifecycle(args):
    report = _base_report(args, "lifecycle")
    segments = []
    proxy = None
    temp = tempfile.TemporaryDirectory(prefix="jm-cellular-real-")
    original_bundle = None
    original_running = False
    lifecycle_changed = False
    restore_ok = False
    cleanup_ok = True
    run_error = None

    try:
        if args.confirm_cellular != CELLULAR_CONFIRMATION:
            raise AcceptanceError("explicit_cellular_confirmation_required")
        if not shared._proxy_contract_ok():
            raise AcceptanceError("bounded_proxy_contract_missing")
        shared._validate_loopback_http(args.control)
        shared._validate_proxy_listen(args.proxy_listen)
        shared._validate_redis_target(args.redis_target)
        shared._validate_output_location(args.json_out, args.mount_root)
        if not shared._loopback_port_available(args.proxy_listen):
            raise AcceptanceError("proxy_listen_port_in_use")

        initial_route = _require_route_evidence(args.redis_target,
                                                args.underlay_kind)
        original_products = shared._product_processes()
        if len(original_products) > 1:
            raise AcceptanceError("multiple_app_processes_running")
        original_running = len(original_products) == 1
        if original_running:
            original_bundle = shared._bundle_from_binary(
                original_products[0][1]).resolve()
            offline = shared._safe_get_json(args.control, "/offline", timeout=3)
            if isinstance(offline, dict) and offline.get("user_offline"):
                raise AcceptanceError("user_offline_enabled")
        test_app = shared._resolve_test_app(args.app, original_products)
        if test_app is None or not shared._app_binary(test_app).is_file():
            raise AcceptanceError("app_bundle_unavailable")

        lifecycle_changed = True
        shared._clean_stop(args.mount_root)
        cold = _require_zero_prelaunch(args.control, args.mount_root)
        final_route = _require_route_evidence(args.redis_target,
                                              args.underlay_kind)
        route_stable = initial_route == final_route
        if not route_stable:
            raise AcceptanceError("route_changed_during_zero_state_transition")

        # The accounting proxy is intentionally zero-impairment. RTT and
        # bandwidth are supplied by the real cellular/Tailscale path only.
        proxy = shared.ProxyController(args, temp.name)
        # A TCP readiness connection would itself dial Redis through
        # Tailscale, pre-warming the path before launch_started. The proxy's
        # first stats snapshot is written only after bind/listen succeeds, so
        # use that non-connecting proof for a true cold-start clock.
        proxy.start(REAL_BUDGET_BYTES, connect_readiness_probe=False)
        # Complete every potentially slow route/status inventory first. The
        # final zero-state proof below must be immediately adjacent to Popen;
        # otherwise an auto-relaunch during route inventory can own the shared
        # control port while the candidate timing falsely succeeds.
        launch_route = _require_route_evidence(args.redis_target,
                                               args.underlay_kind)
        route_stable = (route_stable and launch_route == final_route)
        launch_route["stable_after_zero_state"] = route_stable
        report["route_and_underlay_evidence"] = launch_route
        if not route_stable:
            raise AcceptanceError("route_changed_before_launch")
        report["measurement"]["real_cellular"] = True
        report["measurement"]["private_route_proved"] = True
        # Re-prove zero process, mounts, and both HTTP/TCP control immediately
        # before the launch clock. Only local dictionary assignments intervene.
        report["cold_start_precondition"] = _require_zero_prelaunch(
            args.control, args.mount_root)
        launch_started = time.monotonic()
        launched = shared._launch_bundle(test_app, {
            "JM_DEBUG_META_ADDR": args.proxy_listen,
            shared.CONTENT_GUARD_ENV: "1",
        }, direct=True, started_at=launch_started)
        startup, startup_metrics = _wait_real_startup(
            args.control, args.startup_timeout, launch_started, launched)
        startup["process_present_ms"] = launched["visible_ms"]
        startup["timing_origin"] = "launch_command"
        report["startup"] = startup

        startup_metrics, quiescent_ms = shared._wait_startup_data_quiescent(
            args.control, startup_metrics)
        startup["data_plane_quiescent_ms"] = quiescent_ms
        startup_gate = shared._startup_probe_gate(startup_metrics)
        report["startup_traffic_gate"] = startup_gate
        report["failures"].extend(
            shared._startup_probe_failure_codes(startup_gate))

        before = startup_metrics
        report["navigation"] = shared._navigation_sample(
            args.mount_root, args.samples, args.concurrency, args.per_worker,
            launch_started=launch_started)
        warmup = shared._safe_get_json(args.control, "/warmup", timeout=3) or {}
        phase = warmup.get("phase")
        report["warm_transition"] = {
            "serving_after_navigation": warmup.get("serving") is True,
            "phase_after_navigation": phase
            if phase in ("starting", "indexing", "warming", "steady")
            else "unknown",
        }

        if args.outage_recovery:
            first = proxy.stop()
            proxy = None
            segments.append(first)
            remaining = shared._remaining_budget(REAL_BUDGET_BYTES, segments)
            if first.get("budget_exceeded") or remaining < shared.MIN_RECONNECT_BUDGET:
                report["outage_recovery"] = {
                    "status": "SKIP",
                    "reason_code": "insufficient_remaining_byte_budget",
                }
            else:
                outage_error = None
                engage_ms = None
                offline_ms = None
                offline_count = None
                try:
                    engage_ms = shared._wait_offline(
                        args.control, True, args.reconnect_engage_timeout)
                    offline_ms, offline_count = shared._enumerate_root_metadata(
                        args.mount_root)
                except AcceptanceError as error:
                    outage_error = error
                finally:
                    # Re-establish the bounded Redis route before propagating
                    # any outage-phase failure. Stopping an app while its route
                    # is absent is the lifecycle race this harness must never
                    # recreate.
                    proxy = shared.ProxyController(args, temp.name)
                    proxy.segment = 1
                    proxy.start(remaining, connect_readiness_probe=False)
                if outage_error is not None:
                    raise outage_error
                recover_ms = shared._wait_recovery_ready(
                    args.control, args.reconnect_recover_timeout)
                recovered_ms, recovered_count = shared._enumerate_root_metadata(
                    args.mount_root)
                report["outage_recovery"] = {
                    "status": "PASS",
                    "auto_offline_engage_ms": engage_ms,
                    "cached_root_while_offline_ms": round(offline_ms, 3),
                    "cached_root_while_offline_entry_count": offline_count,
                    "online_recovery_ms": recover_ms,
                    "post_recovery_root_ms": round(recovered_ms, 3),
                    "post_recovery_root_entry_count": recovered_count,
                }
        else:
            report["outage_recovery"] = {
                "status": "SKIP",
                "reason_code": "not_explicitly_authorized",
            }

        after = shared._safe_get_json(args.control, "/metrics", timeout=5)
        if not isinstance(after, dict):
            raise AcceptanceError("final_metrics_unavailable")
        report["traffic_gate"] = shared._metrics_gate(before, after)
        report["measurement"]["measurement_performed"] = True

    except AcceptanceError as error:
        run_error = error.code
    except (KeyboardInterrupt, InterruptedError):
        run_error = "interrupted"
    except Exception:
        run_error = "unexpected_runner_error"
    finally:
        if lifecycle_changed:
            cleanup_ok, proxy_error = shared._stop_test_environment(
                args, proxy, segments)
            proxy = None
            if proxy_error and run_error is None:
                run_error = proxy_error
            restore_ok, cleanup_ok = shared._restore_initial_state(
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
            restore_ok = True
        temp.cleanup()

    report["metadata_proxy"] = shared._proxy_summary(
        segments, REAL_BUDGET_BYTES)
    report["restore"] = {
        "initial_running_state": original_running,
        "initial_state_restored": restore_ok,
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
                        help="authorize the reversible app stop/start lifecycle")
    parser.add_argument("--confirm-cellular", default="",
                        help="must equal CELLULAR_TAILSCALE_LOW_DATA for a live run")
    parser.add_argument("--underlay-kind", choices=ALLOWED_UNDERLAYS, default="",
                        help="operator-attested physical cellular underlay")
    parser.add_argument("--outage-recovery", action="store_true",
                        help="also authorize bounded Redis withdrawal/recovery")
    parser.add_argument("--app", default="")
    parser.add_argument("--mount-root", default=shared.DEFAULT_MOUNT)
    parser.add_argument("--control", default=shared.DEFAULT_CONTROL)
    parser.add_argument("--redis-target", default="")
    parser.add_argument("--proxy-listen", default=shared.DEFAULT_PROXY_LISTEN)
    parser.add_argument("--samples", type=int, default=8)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--per-worker", type=int, default=2)
    parser.add_argument("--startup-timeout", type=float, default=180.0)
    parser.add_argument("--reconnect-engage-timeout", type=float, default=75.0)
    parser.add_argument("--reconnect-recover-timeout", type=float, default=90.0)
    parser.add_argument("--cold-root-max-ms", type=float, default=3000.0)
    parser.add_argument("--warm-p95-ms", type=float, default=250.0)
    parser.add_argument("--concurrent-p95-ms", type=float, default=500.0)
    parser.add_argument("--json-out", default="")
    args = parser.parse_args(argv)

    # Attributes consumed by the shared bounded ProxyController. All zeroes
    # mean accounting-only: no synthetic latency, jitter, or pacing.
    args.delay_ms = 0.0
    args.jitter_ms = 0.0
    args.down_mbps = 0.0
    args.up_mbps = 0.0
    args.seed = 1
    args.budget_bytes = REAL_BUDGET_BYTES
    numeric = (args.startup_timeout, args.reconnect_engage_timeout,
               args.reconnect_recover_timeout, args.cold_root_max_ms,
               args.warm_p95_ms, args.concurrent_p95_ms)
    if any(value <= 0 or not math.isfinite(value) for value in numeric):
        raise AcceptanceError("numeric_argument_invalid")
    if (args.samples < 1 or args.concurrency < 1 or args.per_worker < 1 or
            args.samples > 64 or args.concurrency > 16 or args.per_worker > 16):
        raise AcceptanceError("sample_argument_out_of_safe_range")
    shared._validate_output_location(args.json_out, args.mount_root)
    if args.lifecycle:
        if args.confirm_cellular != CELLULAR_CONFIRMATION:
            raise AcceptanceError("explicit_cellular_confirmation_required")
        if not args.redis_target:
            raise AcceptanceError("redis_target_required_for_lifecycle")
        if args.underlay_kind not in ALLOWED_UNDERLAYS:
            raise AcceptanceError("underlay_kind_required_for_lifecycle")
    return args


def main(argv=None):
    try:
        args = _parse_args(argv)
        report = _run_lifecycle(args) if args.lifecycle else _dry_run(args)
        _emit(report, args)
        return 0 if report["status"] in ("PASS", "DRY_RUN") else 1
    except AcceptanceError as error:
        sys.stdout.write(json.dumps({
            "schema": 1,
            "name": NAME,
            "status": "FAIL",
            "measurement": {
                "simulated": False,
                "target_kind": "real_cellular",
                "measurement_performed": False,
                "real_cellular": False,
                "scope": "root_metadata_only",
            },
            "failures": [error.code],
        }, indent=2, sort_keys=True) + "\n")
        return 2


if __name__ == "__main__":
    sys.exit(main())
