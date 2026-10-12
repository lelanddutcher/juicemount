#!/usr/bin/env python3
"""PRIVATE DRAFT: native ARM64 build, scans and ten ordinary compatibility cases.

No registry push, auth-bypass probe, migration, or production
qualification. Failed/uncertain operations are not retried. Private fixture
state is never an artifact; only the explicit evidence allowlist is exported.
"""
import sys

# This guard precedes imports with any possibility of creating cache files.
if sys.flags.optimize:
    sys.stderr.write("HOLD: optimized Python is not admitted\n")
    raise SystemExit(2)

import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import secrets
import shutil
import signal
import stat
import subprocess
import tempfile
import time


PARENT = "857bdb7acfc563108d8120645ebe18649edec2dc"
PARENT_TREE = "390c0e42a1ae1a2db038e89a92b4dc90b1f2afa1"
BRANCH = "refs/heads/codex/0.5-storage-arm64-857bdb7"
TEST_SOURCE_SHA = "0b42a6e0ed8f2267f2d14d06fe295812a81016cdbb33f8efec1109b2835d389e"
BASE = ("docker.io/pgsty/silo:RELEASE.2026-09-16T00-00-00Z@sha256:"
        "1ff329e4507f742a8437f1773d43cf6adb0131477074dd45c770d1a1db19e99c")
BASE_ID = "sha256:a9590da2668a2589dee6e1f8754c755074695794aaba389b6c7e78199688e2d5"
# Exact reviewed base config defaults. These contain file names, not secrets;
# root mcli uses --config-dir and production provisioning owns its MC_CONFIG_DIR.
BASE_ENV = [
    "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "MINIO_ACCESS_KEY_FILE=access_key", "MINIO_SECRET_KEY_FILE=secret_key",
    "MINIO_ROOT_USER_FILE=access_key", "MINIO_ROOT_PASSWORD_FILE=secret_key",
    "MINIO_KMS_SECRET_KEY_FILE=kms_master_key", "MINIO_CONFIG_ENV_FILE=config.env",
    "HOME=/tmp", "MC_CONFIG_DIR=/tmp/.mc",
]
CHANGED_PATHS = sorted([
    ".github/workflows/ci.yml", "server/minio/qualify-storage-candidate.py",
])
CHECKS = [
    "fresh_prefix", "client_zero_put_read_range_head",
    "client_unicode_put_read_range_head", "client_4MiB_put_read_range_head",
    "create_only_absent_and_existing", "client_multipart_completion",
    "gc_multipart_abort", "client_delete_denied_and_bytes_preserved",
    "client_exact_paginated_inventory", "gc_delete_and_absence",
]
IDENTITIES = {
    "mcli": {
        "url": "https://github.com/pgsty/mc.git",
        "revision": "e952aa78f10a2b77dd525a2b7e3143bcda0cd377",
        "tree": "d4a0c88cbadd0eb5e17f678476bee251b9fa5032",
        "module": "github.com/minio/mc",
        "original": {
            "go.mod": "ae5dcbe8107b30b1e07e52e4cb2a3d2376d1b61785df44cc1d66863b16126e9b",
            "go.sum": "3f64064b18d6ec3a0a64aa9e69f5d1b01fd773a57251fde62942ccb2b0b1a4af",
        },
        "patched": {
            "go.mod": "3945119e5c74b0bcdea778c2f46f414a99d6a5fd79ad8eb132c0f3de1c1fc8c6",
            "go.sum": "747a263bbbfe655f26d06e5848e112ee6c9a0fcafd92eb6f5efc86e730521c4e",
        },
        "patch_sha256": "650bd2fc5bccb8d910fce3c9ee27f0b5874a90ce4bc94eb190ec06cee691a388",
    },
    "silo": {
        "url": "https://github.com/pgsty/silo.git",
        "revision": "2a4d51406b7ed87af5fe6fe0f801f3290f96eb3c",
        "tree": "5d4ad13b3ff4f23b38d346b0b4a19fb8ed97b686",
        "module": "github.com/minio/minio",
        "original": {
            "go.mod": "61cdae6906fa55c92028a898e32ff8f704e941c8a9b0f7c5e84688e2854055fb",
            "go.sum": "da1b142985760aea3d8d1570df5f2133c4cc1fa5fea6221103d8e60c36dc3417",
        },
        "patched": {
            "go.mod": "15678b8fbc78e839da7b24fecd4ac9c42ae0921f68d4b57612e09a1118b15613",
            "go.sum": "140d249124a1a2658ce80206229ea4c21fdddef4a0aa47452ea9411e5993f2ad",
        },
        "patch_sha256": "88f97d09ce56a8837cab629f650bf2e9b67c7e31533d9e8fd79bb206fe9fc5ce",
    },
}
ARTIFACTS = {'silo-modules.jsonl', 'mcli-graph-summary.json', 'mcli-modules.jsonl', 'silo-packages.jsonl', 'compatibility-result.json', 'scanner-provenance.json', 'native-preflight.json', 'candidate-image.tar.gz', 'mcli-source-govulncheck.stderr', 'provenance.json', 'silo-packages.stderr', 'scan-summary.json', 'mcli-source-govulncheck.jsonl', 'mcli-binary-govulncheck.jsonl', 'silo-modules.stderr', 'silo-binary-govulncheck.stderr', 'compatibility-buildinfo.txt', 'mcli-packages.jsonl', 'candidate-image.json', 'silo-module.patch', 'mcli-buildinfo.txt', 'silo-buildinfo.txt', 'silo-source-govulncheck.stderr', 'scanner-module.json', 'result.json', 'mcli-packages.stderr', 'scanner-buildinfo.txt', 'mcli-modules.stderr', 'silo-source-govulncheck.jsonl', 'mcli-binary-govulncheck.stderr', 'compatibility-status.txt', 'silo-binary-govulncheck.jsonl', 'artifact-manifest.json', 'mcli-module.patch', 'candidate-Dockerfile', 'silo-graph-summary.json'}
BASE_RUNTIME = {'ArgsEscaped': True, 'Cmd': ['silo'], 'Entrypoint': ['/usr/bin/docker-entrypoint.sh'], 'ExposedPorts': {'9000/tcp': {}}, 'Volumes': {'/data': {}}, 'WorkingDir': '/'}
REMOVED_IDS = {'GO-2026-6605', 'GO-2026-6607', 'GO-2026-6612', 'GO-2026-6600', 'GO-2026-6599', 'GO-2026-6608', 'GO-2026-6609', 'GO-2026-6610', 'GO-2026-6617', 'GO-2026-6613', 'GO-2026-6611', 'GO-2026-6603'}
KNOWN_OPEN_IDS = {'GO-2026-4803', 'GO-2026-5757', 'GO-2026-5932', 'GO-2026-4896', 'GO-2026-5284', 'GO-2026-5415', 'GO-2026-5437', 'GO-2025-3495', 'GO-2026-4779'}
SCANNER_MODULE_SUM = "h1:clG4qBU6zH5VKjti8n5j8BBuYzoSha392xXMkXS351U="
MIGRATION_SOURCE_SHA = "c0726403375d371da7c16d0490da054f1a6f396c71a82056ec824a933cf50414"
LABEL = "org.juicemount.candidate-fixture"
PASS = "PASS_NATIVE_ARM64_ORDINARY_COMPATIBILITY_SECURITY_AND_MIGRATION_OPEN"


class Hold(Exception):
    pass


def require(condition, reason):
    if not condition:
        raise Hold(reason)


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def owned_dir(path):
    info = path.lstat()
    require(path.is_absolute() and path.resolve(strict=True) == path,
            "directory must be canonical without symlinks")
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid(), "directory owner mismatch")
    require(stat.S_IMODE(info.st_mode) == 0o700, "directory must be private 0700")


def write_new(path, data):
    payload = data if isinstance(data, bytes) else data.encode()
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def write_json(path, value):
    write_new(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def clean_env():
    # HOME retains its original meaning. Every credential/config-using tool gets
    # explicit private configuration; nothing is inherited merely by prefix.
    result = {"PATH": os.environ["PATH"], "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}
    if "HOME" in os.environ:
        result["HOME"] = os.environ["HOME"]
    result.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null",
                  GIT_TERMINAL_PROMPT="0", GIT_CONFIG_COUNT="1",
                  GIT_CONFIG_KEY_0="core.hooksPath", GIT_CONFIG_VALUE_0="/dev/null")
    return result


def command(args, env, *, cwd=None, timeout=45, expected=0):
    # Never expose command output or arguments in an exception. Docker config
    # inspection and all runtime outputs can contain credential material.
    process = subprocess.Popen([str(a) for a in args], cwd=cwd, env=env,
                               stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, start_new_session=True)
    try:
        output, error = process.communicate(timeout=timeout)
    except BaseException:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate(timeout=5)
        raise Hold("subprocess interrupted or timed out; outcome may be unknown") from None
    if expected is not None:
        require(process.returncode == expected,
                "subprocess exited " + str(process.returncode) + "; raw output withheld")
    return process.returncode, output, error


def git(repo, env, *args):
    return command(["git", "-c", "credential.helper=", "-C", repo, *args], env)[1].decode().strip()


def classify_readiness(exit_code, output, error):
    """Pure, credential-free oracle; caller supplies timing/owned-CID evidence."""
    result = {"outcome": "HOLD", "category": "unknown_error", "json_status": "unknown",
              "exit_code": exit_code, "stdout_sha256": hashlib.sha256(output).hexdigest(),
              "stderr_sha256": hashlib.sha256(error).hexdigest()}
    if len(output) > 1024 * 1024 or len(error) > 1024 * 1024:
        result["category"] = "oversized_response"
        return result
    try:
        payload = json.loads(output.decode("utf-8"))
    except (ValueError, UnicodeError):
        result["category"] = "malformed_json"
        return result
    if not isinstance(payload, dict):
        result["category"] = "malformed_json"
        return result
    status = payload.get("status")
    result["json_status"] = status if status in ["success", "error"] else "unknown"
    # Authentication/authorization evidence wins over any transient phrase.
    searchable = json.dumps(payload.get("error", {})) + error.decode("utf-8", errors="replace")
    if re.search(r"access[ _-]?denied|invalidaccesskeyid|signaturedoesnotmatch|unauthorized|forbidden|"
                 r"authentication|authorization|invalid credentials|invalid access key|invalid secret key|"
                 r"invalid token|(?:statusCode|status_code|httpStatusCode)[\" ]*:[ ]*(?:401|403)",
                 searchable, re.IGNORECASE):
        result["category"] = "authentication_or_authorization_error"
        return result
    if error.strip():
        result["category"] = "unexpected_stderr"
        return result
    if status == "success":
        info = payload.get("info")
        servers = info.get("servers") if isinstance(info, dict) else None
        if exit_code != 0 or payload.get("error"):
            result["category"] = "success_with_error_or_nonzero_exit"
        elif not isinstance(servers, list) or not servers or not all(isinstance(v, dict) and v for v in servers):
            result["category"] = "missing_required_server_info"
        else:
            result.update(outcome="PASS", category="authenticated_admin_info")
        return result
    if status != "error" or not isinstance(payload.get("error"), str):
        result["category"] = "unknown_status_or_error_shape"
        return result
    if exit_code not in [0, 1]:
        result["category"] = "unexpected_exit"
        return result
    # Pinned mcli admin-info serializes ServerInfo's Error() as a JSON string.
    # Other error shapes and arbitrary substrings never authorize a retry.
    message = payload["error"]
    connection = re.compile(r'(?:Get "http://127\.0\.0\.1:9000/minio/admin/v[0-9]+/info(?:\?[^"\s]*)?": )?'
                            r'dial tcp 127\.0\.0\.1:9000: connect: connection refused')
    initialization = {"Server not initialized, please try again.",
                      "Server not initialized yet, please try again."}
    if connection.fullmatch(message):
        result.update(outcome="RETRY", category="loopback_connection_refused")
    elif message in initialization:
        result.update(outcome="RETRY", category="server_initializing")
    return result


def safe_container_state(obj):
    """Only documented scalars; Docker State.Error is hashed, never exported."""
    state = obj.get("State", {})
    status = state.get("Status")
    result = {"status": status if status in ["created", "restarting", "running", "removing", "paused", "exited", "dead"] else "unknown"}
    for field in ["Running", "Paused", "Restarting", "OOMKilled", "Dead"]:
        result[field] = state.get(field) if isinstance(state.get(field), bool) else None
    for field in ["Pid", "ExitCode"]:
        result[field] = state.get(field) if type(state.get(field)) is int else None
    for field in ["StartedAt", "FinishedAt"]:
        value = state.get(field)
        result[field] = value if isinstance(value, str) and re.fullmatch(r"[0-9T:.Z+\-]+", value) else None
    result["error_sha256"] = hashlib.sha256(str(state.get("Error", "")).encode()).hexdigest()
    return result



def unique_json_pairs(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, "duplicate JSON object key")
        value[key] = item
    return value


def invalid_json_constant(value):
    raise Hold("non-finite JSON number")


def parse_json_stream(raw):
    """Offset parser: all records and every tail byte are accounted for."""
    require(isinstance(raw, bytes), "JSON stream must be bytes")
    try:
        text = raw.decode("utf-8")
        decoder = json.JSONDecoder(object_pairs_hook=unique_json_pairs, parse_constant=invalid_json_constant)
        records, offset = [], 0
        while offset < len(text):
            while offset < len(text) and text[offset] in " \t\r\n":
                offset += 1
            if offset == len(text):
                break
            value, end = decoder.raw_decode(text, offset)
            require(end > offset and isinstance(value, dict), "JSON record must be an object")
            records.append(value)
            offset = end
    except (ValueError, UnicodeError):
        raise Hold("malformed or truncated JSON stream") from None
    require(bool(records), "empty JSON stream")
    return records


def validate_graph(records, root_module):
    require(isinstance(records, list) and bool(records), "package graph absent")
    packages = {}
    for item in records:
        require(isinstance(item, dict), "package graph record malformed")
        path = item.get("ImportPath")
        require(isinstance(path, str) and bool(path) and path not in packages, "missing or duplicate package identity")
        require(not item.get("Error") and not item.get("DepsErrors") and not item.get("Incomplete"), "package errors or incomplete nodes")
        imports = item.get("Imports", [])
        require(isinstance(imports, list) and all(isinstance(v, str) and v for v in imports)
                and len(imports) == len(set(imports)), "package imports malformed or duplicate")
        mapping = item.get("ImportMap", {})
        require(isinstance(mapping, dict) and all(isinstance(k, str) and isinstance(v, str) for k, v in mapping.items()), "package import map malformed")
        packages[path] = item
    require(root_module in packages and packages[root_module].get("Name") == "main", "expected main root absent")
    edges = {}
    for path, item in packages.items():
        mapping = item.get("ImportMap", {})
        edges[path] = [mapping.get(v, v) for v in item.get("Imports", [])]
        require(all(v in packages for v in edges[path]), "unresolved native package import edge")
    reachable, pending = set(), [root_module]
    while pending:
        path = pending.pop()
        if path not in reachable:
            reachable.add(path)
            pending.extend(edges[path])
    require(reachable == set(packages), "package graph exceeds root closure")
    openpgp = sorted(v for v in reachable if "openpgp" in v.lower().split("/"))
    require(not openpgp, "native root imports OpenPGP")
    return {"root_module": root_module, "complete": True, "package_count": len(packages),
            "package_paths": sorted(packages), "openpgp_imports": openpgp}


def validate_modules(records, identity):
    require(isinstance(records, list) and bool(records), "native module records absent")
    modules = {}
    for item in records:
        require(isinstance(item, dict) and not item.get("Error"), "module record incomplete")
        path = item.get("Path")
        require(isinstance(path, str) and path and path not in modules, "module identity absent or duplicate")
        modules[path] = item
    root = identity["module"]
    require(root in modules and modules[root].get("Main") is True and not modules[root].get("Version")
            and not modules[root].get("Replace"), "module root differs")
    require(sum(item.get("Main") is True for item in modules.values()) == 1, "ambiguous main modules")
    for path in ["golang.org/x/net", "golang.org/x/crypto"]:
        require(path in modules and not modules[path].get("Replace") and isinstance(modules[path].get("Version"), str)
                and modules[path]["Version"], "required native module identity absent")
    require(modules["golang.org/x/net"]["Version"] == "v0.60.0", "selected native x/net differs")
    return {path: item.get("Version", "") for path, item in modules.items()}


def validate_scan(records, mode, identity, graph):
    """Actual finding.osv is adjudicated; catalog presence alone is not a finding."""
    require(mode in ["source", "binary"] and isinstance(records, list) and records, "scan mode/records invalid")
    require(graph.get("complete") is True and graph.get("root_module") == identity["module"]
            and graph.get("openpgp_imports") == [], "native graph binding incomplete")
    configs, sboms, findings, catalog = [], [], [], {}
    catalog_record_count = 0
    for record in records:
        require(isinstance(record, dict) and len(record) == 1, "scan protocol record malformed")
        kind, value = next(iter(record.items()))
        require(kind in ["config", "SBOM", "osv", "finding", "progress"] and isinstance(value, dict), "unknown scan protocol message")
        if kind == "config": configs.append(value)
        elif kind == "SBOM": sboms.append(value)
        elif kind == "finding": findings.append(value)
        elif kind == "osv":
            key = value.get("id")
            require(isinstance(key, str) and re.fullmatch(r"GO-[0-9]{4}-[0-9]+", key), "OSV catalog identity absent")
            catalog_record_count += 1
            canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
            require(key not in catalog or catalog[key] == canonical, "conflicting repeated OSV catalog content")
            catalog[key] = canonical
        else:
            require(isinstance(value.get("message"), str), "progress message malformed")
    require(len(configs) == 1 and len(sboms) == 1 and "config" in records[0], "scan config/SBOM absent or duplicated")
    config, sbom = configs[0], sboms[0]
    expected = {"protocol_version": "v1.0.0", "scanner_name": "govulncheck", "scanner_version": "v1.8.0",
                "db": "https://vuln.go.dev", "scan_level": "symbol", "scan_mode": mode}
    require(all(config.get(k) == v for k, v in expected.items()), "scanner configuration differs")
    require(isinstance(config.get("db_last_modified"), str)
            and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z", config["db_last_modified"]), "scan DB version absent")
    require((mode != "source" or config.get("go_version") == "go1.27.2")
            and ("go_version" not in config or config["go_version"] == "go1.27.2"), "scanner source toolchain differs")
    require(sbom.get("go_version") == "go1.27.2" and isinstance(sbom.get("modules"), list) and sbom["modules"], "scan SBOM/toolchain incomplete")
    modules, module_records = {}, {}
    for item in sbom["modules"]:
        require(isinstance(item, dict), "SBOM module malformed")
        path = item.get("path")
        require(isinstance(path, str) and path, "SBOM module identity absent")
        require(isinstance(item.get("version", ""), str), "SBOM module version malformed")
        canonical = json.dumps(item, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
        require(path not in module_records or module_records[path] == canonical, "conflicting repeated SBOM module content")
        module_records[path] = canonical
        modules[path] = item.get("version", "")
    root = identity["module"]
    require(root in modules and modules.get("golang.org/x/net") == "v0.60.0", "SBOM root/x-net differs")
    require(sbom.get("roots") == [root], "SBOM roots differ from exact expected root")
    require(modules.get("stdlib") == ("v1.27.2" if mode == "source" else "go1.27.2"), "SBOM standard library differs")
    if mode == "source":
        require(modules[root] == "", "source SBOM main version differs")
    else:
        require(re.fullmatch(r"v0\.0\.0-[0-9]{14}-" + re.escape(identity["revision"][:12]) + r"\+dirty", modules[root]), "binary SBOM source revision differs")
    native_versions = graph.get("module_versions", {})
    require(native_versions.get("golang.org/x/net") == modules["golang.org/x/net"], "native module/SBOM x-net differs")
    counts = {}
    for finding in findings:
        key, trace = finding.get("osv"), finding.get("trace")
        require(isinstance(key, str) and key in catalog, "finding lacks associated OSV record")
        require(isinstance(trace, list) and trace and all(isinstance(frame, dict) and frame for frame in trace), "finding trace empty or malformed")
        require(all(isinstance(frame.get("module"), str) and frame["module"] for frame in trace), "finding trace module missing")
        require(all(all(field not in frame or isinstance(frame[field], str) for field in ["version", "package", "function", "receiver"])
                    for frame in trace), "finding trace scalar malformed")
        counts[key] = counts.get(key, 0) + 1
        require(key not in REMOVED_IDS, "removed Go/net advisory present in native findings")
        require(key in KNOWN_OPEN_IDS, "unknown native finding requires review")
        if key == "GO-2026-5932":
            version = native_versions.get("golang.org/x/crypto")
            require(isinstance(version, str) and version and modules.get("golang.org/x/crypto") == version, "OpenPGP SBOM/native module version differs")
            require(all(frame.get("module") == "golang.org/x/crypto" and frame.get("version") == version
                        and not frame.get("package") and not frame.get("function") for frame in trace), "OpenPGP trace not bound module-only evidence")
    return {"mode": mode, "complete_protocol": True, "config": config, "sbom_module_count": len(modules),
            "sbom_module_record_count": len(sbom["modules"]), "sbom_distinct_module_count": len(modules),
            "root_module": root, "catalog_record_count": catalog_record_count, "catalog_distinct_id_count": len(catalog),
            "finding_record_count": len(findings),
            "finding_counts": counts, "finding_ids": sorted(counts), "removed_ids_absent": sorted(REMOVED_IDS),
            "known_findings_disposition": "OPEN; ordinary qualification only", "security_acceptance": "OPEN"}


def format_finalization_failure(first_red, error, secrets, phase="finalization"):
    """Pure, fixed-schema CI-log receipt; redact dynamic text before capping it.

    A publication write can fail after a partial write; that phase is explicitly
    UNKNOWN. Finalization failures precede all publication output. No raw error
    repr/str or arbitrary prior metadata is serialized.
    """
    needles = sorted({value for value in secrets if isinstance(value, str) and value}, key=len, reverse=True) if isinstance(secrets, (list, tuple)) else []
    def bounded(value, limit, fallback):
        text = value if isinstance(value, str) else fallback
        for secret in needles:
            text = text.replace(secret, "[REDACTED]")
        return text if len(text) <= limit else text[:limit - 14] + "...[truncated]"
    phase = phase if phase in ["finalization", "publication"] else "finalization"
    raw_type = type(error).__name__
    error_type = raw_type if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", raw_type) else "Exception"
    error_type = bounded(error_type, 64, "Exception")
    expected_hold = isinstance(error, Hold)
    reason = error.args[0] if expected_hold and error.args and isinstance(error.args[0], str) else error_type
    failure = {"classification": "HOLD" if expected_hold else "UNEXPECTED_EXCEPTION",
               "exception_type": error_type, "reason": bounded(reason, 192, "failure")}
    prior_present = first_red is not None
    if isinstance(first_red, dict):
        retained = {"stage": bounded(first_red.get("stage"), 80, "prior failure"),
                    "reason": bounded(first_red.get("reason"), 192, "prior failure metadata unavailable")}
    elif prior_present:
        retained = {"stage": "prior failure", "reason": "prior failure metadata unavailable"}
    else:
        retained = {"stage": "evidence " + phase, "reason": failure["reason"]}
    receipt = {"schema": 1, "status": "HOLD", "failure_phase": phase,
               "prior_first_red_present": prior_present, "first_red": retained, "failure": failure,
               "evidence_ready": False if phase == "finalization" else None,
               "artifact_publication": "WITHHELD" if phase == "finalization" else "UNKNOWN",
               "private_staging": "NOT_EXPORTED_BY_CONTROLLER", "retry": False}
    # ensure_ascii escapes control characters into one line; fixed fields and
    # bounded dynamic values keep the encoded receipt below 8192 bytes.
    encoded = json.dumps(receipt, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False)
    require(len(encoded.encode("ascii")) <= 8192, "failure receipt exceeds fixed bound")
    return encoded

def preflight():
    require(len(sys.argv) == 1, "runner takes no arguments")
    require(sys.platform == "linux" and platform.machine() == "aarch64", "Linux ARM64 required")
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted"
            and os.environ.get("RUNNER_OS") == "Linux"
            and os.environ.get("RUNNER_ARCH") == "ARM64", "GitHub-hosted Linux ARM64 context required")
    require(os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch", "manual dispatch required")
    require(os.environ.get("GITHUB_REPOSITORY", "").lower() == "lelanddutcher/juicemount",
            "repository context differs")
    require(os.environ.get("GITHUB_REF") == BRANCH, "diagnostic branch differs")
    require(os.environ.get("GITHUB_RUN_ATTEMPT") == "1", "no automatic or manual retry of this receipt")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    source_sha = os.environ.get("GITHUB_SHA", "")
    require(re.fullmatch(r"[0-9]+", run_id) and re.fullmatch(r"[0-9a-f]{40}", source_sha),
            "run/source identity missing")
    repo = Path(os.environ["GITHUB_WORKSPACE"])
    require(repo.is_absolute() and repo.resolve(strict=True) == repo, "canonical checkout required")
    require(Path(__file__).resolve() == repo / "server/minio/qualify-storage-candidate.py",
            "runner is not in admitted checkout")
    env = clean_env()
    require(git(repo, env, "rev-parse", "HEAD") == source_sha, "GITHUB_SHA differs from checkout")
    require(git(repo, env, "show", "-s", "--format=%P", "HEAD") == PARENT, "exact single parent required")
    require(git(repo, env, "rev-parse", PARENT + "^{tree}") == PARENT_TREE, "parent tree differs")
    require(not git(repo, env, "status", "--porcelain", "--untracked-files=all"), "checkout must be clean")
    require(sorted(git(repo, env, "diff", "--name-only", PARENT, "HEAD").splitlines()) == CHANGED_PATHS,
            "diagnostic commit changed an unapproved path")
    for relative in CHANGED_PATHS:
        item = repo / relative
        require(item.is_file() and not item.is_symlink(), "diagnostic file must be regular")
    require(sha(repo / "server/minio/storage_compatibility_test.go") == TEST_SOURCE_SHA,
            "reviewed compatibility source differs")
    for name, identity in IDENTITIES.items():
        require(sha(repo / ("server/minio/candidate-patches/" + name + "-module.patch"))
                == identity["patch_sha256"], "reviewed module patch differs")
    env.update(GOTOOLCHAIN="local", GOWORK="off", GOENV="off", GOOS="linux", GOARCH="arm64", GOARM64="v8.0", CGO_ENABLED="0", GOFLAGS="", GOEXPERIMENT="")
    require(command(["go", "version"], env)[1].decode().strip() == "go version go1.27.2 linux/arm64",
            "exact Go 1.27.2 linux/arm64 toolchain required")
    native = json.loads(command(["go", "env", "-json", "GOHOSTOS", "GOHOSTARCH", "GOOS", "GOARCH", "GOARM64", "CGO_ENABLED", "GOFLAGS", "GOEXPERIMENT", "GOVERSION"], env)[1])
    require(native == {"GOHOSTOS": "linux", "GOHOSTARCH": "arm64", "GOOS": "linux", "GOARCH": "arm64", "GOARM64": "v8.0", "CGO_ENABLED": "0", "GOFLAGS": "", "GOEXPERIMENT": "", "GOVERSION": "go1.27.2"}, "native Go host/target/options differ")
    docker = json.loads(command(["docker", "version", "--format", "{{json .Server}}"], env)[1])
    require(docker.get("Os") == "linux" and docker.get("Arch") == "arm64", "native Docker server required")
    require(sha(repo / "server/minio/qualify-storage-migration.py") == MIGRATION_SOURCE_SHA, "migration source differs")
    runner_temp = Path(os.environ["RUNNER_TEMP"])
    require(runner_temp.is_absolute() and runner_temp.resolve(strict=True) == runner_temp,
            "canonical RUNNER_TEMP required")
    require(runner_temp.is_dir() and runner_temp.stat().st_uid == os.geteuid(), "RUNNER_TEMP owner differs")
    require(shutil.disk_usage(runner_temp).free >= 12 * 1024**3, "at least 12 GiB free required")
    output_file = Path(os.environ["GITHUB_OUTPUT"])
    output_info = output_file.lstat()
    require(stat.S_ISREG(output_info.st_mode) and output_info.st_uid == os.geteuid()
            and not output_file.is_symlink(), "owned GITHUB_OUTPUT required")
    return repo, env, runner_temp, output_file, run_id, source_sha, native, {k: docker.get(k) for k in ["Os", "Arch", "Version", "ApiVersion"]}


class Qualification:
    def __init__(self, admission):
        self.repo, self.env, temp, output_file, self.run_id, self.source_sha, native, docker = admission
        os.umask(0o077)
        self.root = Path(tempfile.mkdtemp(prefix="jm-storage-candidate-" + self.run_id + "-", dir=temp))
        owned_dir(self.root)
        self.evidence = self.root / "evidence"
        self.private = self.root / "private"
        for folder in [self.evidence, self.private]:
            folder.mkdir(mode=0o700)
            owned_dir(folder)
        for name in ["tmp", "gotmp", "gopath", "gocache", "docker", "data", "control", "extract"]:
            (self.private / name).mkdir(mode=0o700)
        for name in ["tmp", "policies", "mc-root"]:
            (self.private / "control" / name).mkdir(mode=0o700)
        self.env.update(TMPDIR=str(self.private / "tmp"), GOTMPDIR=str(self.private / "gotmp"),
                        GOPATH=str(self.private / "gopath"), GOCACHE=str(self.private / "gocache"),
                        GOTOOLCHAIN="local", GOWORK="off", GOENV="off", CGO_ENABLED="0",
                        GOOS="linux", GOARCH="arm64", GOPROXY="https://proxy.golang.org",
                        GOSUMDB="sum.golang.org", DOCKER_CONFIG=str(self.private / "docker"))
        self.output_file = output_file
        self.binary_identities = {}
        self.source_dirs = {}
        self.scan_records = {}
        self.stage = "admitted"
        self.result = {"schema": 1, "status": "HOLD", "source_sha": self.source_sha,
                       "parent": PARENT, "run_id": self.run_id, "stages": [],
                       "limits": ["ordinary compatibility only", "security acceptance open",
                                  "fresh-install acceptance open", "migration and restore open",
                                  "ARM64 migration and full security open", "mounted/provider acceptance open",
                                  "no registry publication"],
                       "container_retirement": [], "private_fixture": "not created"}
        self.image_id = None
        self.container = None
        self.container_name = None
        self.owner = secrets.token_hex(32)
        self.create_attempted = False
        self.secrets = []
        write_json(self.evidence / "native-preflight.json", {"go": native, "docker_server": docker,
                   "host": {"platform": sys.platform, "machine": platform.machine(), "runner_arch": os.environ.get("RUNNER_ARCH")}})
        write_json(self.evidence / "provenance.json", {
            "source_sha": self.source_sha, "parent": PARENT, "parent_tree": PARENT_TREE,
            "diagnostic_paths": {p: sha(self.repo / p) for p in CHANGED_PATHS},
            "compatibility_source_sha256": TEST_SOURCE_SHA, "source_module_identities": IDENTITIES,
            "base": BASE, "base_config_id": BASE_ID, "toolchain": "go1.27.2 linux/arm64",
            "trial_scope": "native ARM64 build, four scans and ten ordinary cases; no migration",
            "build_environment": {k: self.env[k] for k in ["CGO_ENABLED", "GOOS", "GOARCH", "GOARM64", "GOFLAGS", "GOEXPERIMENT", "GOTOOLCHAIN", "GOWORK", "GOENV"]},
            "policy_hashes": {name: sha(self.repo / "server/minio" / name) for name in
                              ["provision-object-keys.sh", "jm-client-policy.json", "jm-gc-policy.json"]},
        })

    def step(self, name):
        self.stage = name
        print("STAGE " + name, flush=True)

    def passed(self):
        self.result["stages"].append(self.stage)

    def call(self, args, **kwargs):
        return command(args, kwargs.pop("env", self.env), **kwargs)

    def docker_json(self, *args):
        return json.loads(self.call(["docker", *args])[1])

    def verify_container(self, *, running=None):
        require(self.create_attempted and self.container_name and self.image_id, "container ownership unavailable")
        objects = self.docker_json("inspect", self.container or self.container_name)
        require(isinstance(objects, list) and len(objects) == 1, "container inspect ambiguous")
        obj = objects[0]
        require(re.fullmatch(r"[0-9a-f]{64}", obj.get("Id", "")), "container ID invalid")
        require(self.container is None or obj["Id"] == self.container, "created container ID changed")
        require(obj.get("Name") == "/" + self.container_name, "container name changed")
        require(obj.get("Image") == self.image_id, "container image changed")
        require(obj.get("Config", {}).get("Labels", {}).get(LABEL) == self.owner, "container owner label changed")
        require(obj.get("Config", {}).get("User") == str(os.getuid()) + ":" + str(os.getgid()), "container user changed")
        mounts = obj.get("Mounts", [])
        expected_mounts = {(str(self.private / "data"), "/data", True),
                           (str(self.private / "control"), "/control", True)}
        require(len(mounts) == 2 and all(m.get("Type") == "bind" for m in mounts)
                and {(m.get("Source"), m.get("Destination"), m.get("RW")) for m in mounts} == expected_mounts,
                "container bind mounts differ")
        for name in ["data", "control"]:
            owned_dir(self.private / name)
        host = obj.get("HostConfig", {})
        require(host.get("NetworkMode") == "bridge" and not host.get("Privileged")
                and not host.get("PublishAllPorts"), "container host configuration differs")
        bindings = host.get("PortBindings") or {}
        if self.container_phase == "extract":
            require(not bindings, "extraction container unexpectedly publishes ports")
        else:
            require(set(bindings) == {"9000/tcp"} and len(bindings["9000/tcp"]) == 1
                    and bindings["9000/tcp"][0].get("HostIp") == "127.0.0.1",
                    "only literal loopback object port may be published")
        require(not host.get("RestartPolicy", {}).get("Name") or host["RestartPolicy"]["Name"] == "no",
                "fixture restart policy differs")
        if running is not None:
            require(obj.get("State", {}).get("Running") is running, "owned container running state differs")
        self.container = obj["Id"]
        return obj

    def create_container(self, phase, *, env=None):
        require(self.container is None and not self.create_attempted, "previous container not retired")
        self.container_phase = phase
        self.container_name = "jm-candidate-" + phase + "-" + secrets.token_hex(12)
        options = ["docker", "create", "--name", self.container_name, "--label", LABEL + "=" + self.owner,
                   "--network", "bridge", "--restart", "no", "--user", str(os.getuid()) + ":" + str(os.getgid()),
                   "--mount", "type=bind,src=" + str(self.private / "data") + ",dst=/data",
                   "--mount", "type=bind,src=" + str(self.private / "control") + ",dst=/control"]
        if phase == "runtime":
            options += ["--publish", "127.0.0.1::9000"]
            for key in ["MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_BROWSER", "MINIO_UPDATE"]:
                options += ["--env", key]
        options += [self.image_id, "server", "--address", ":9000", "--console-address", "127.0.0.1:9001", "/data"]
        self.create_attempted = True
        _, out, _ = self.call(options, env=env or self.env)
        candidate_id = out.decode().strip()
        require(re.fullmatch(r"[0-9a-f]{64}", candidate_id), "create returned no exact container ID")
        self.container = candidate_id
        self.verify_container(running=False)

    def retire_container(self):
        if not self.create_attempted:
            return
        retirement = {"phase": self.container_phase, "status": "RETAINED_UNCERTAIN"}
        self.result["container_retirement"].append(retirement)
        obj = self.verify_container()
        retirement["container_id"] = self.container
        retirement["before_stop"] = safe_container_state(obj)
        retirement["stop_attempted"] = bool(obj["State"]["Running"])
        stop_ok = True
        if obj["State"]["Running"]:
            # Infinite Docker grace timeout: controller timeout never SIGKILLs
            # the server and never permits removal of an uncertain fixture.
            try:
                code, out, err = self.call(["docker", "stop", "--time", "-1", self.container],
                                           timeout=35, expected=None)
                retirement["stop_exit_code"] = code
                retirement["stop_stdout_sha256"] = hashlib.sha256(out).hexdigest()
                retirement["stop_stderr_sha256"] = hashlib.sha256(err).hexdigest()
                stop_ok = code == 0
            except Hold:
                retirement["stop_outcome"] = "INTERRUPTED_OR_TIMEOUT"
                stop_ok = False
        obj = self.verify_container()
        retirement["after_stop"] = safe_container_state(obj)
        # Capture the wait outcome BEFORE judging a nonzero server exit.
        wait_ok = obj["State"].get("Status") == "created"
        if obj["State"].get("Status") == "created":
            retirement["wait_outcome"] = "NOT_STARTED"
        elif not obj["State"].get("Running"):
            try:
                code, out, err = self.call(["docker", "wait", self.container], timeout=5, expected=None)
                retirement["wait_cli_exit_code"] = code
                retirement["wait_stdout_sha256"] = hashlib.sha256(out).hexdigest()
                retirement["wait_stderr_sha256"] = hashlib.sha256(err).hexdigest()
                number = out.decode("ascii", errors="replace").strip()
                value = int(number) if re.fullmatch(r"-?[0-9]+", number) else None
                retirement["wait_container_exit_code"] = value
                retirement["wait_outcome"] = "OBSERVED" if value is not None else "MALFORMED"
                wait_ok = code == 0 and value == 0
            except Hold:
                retirement["wait_outcome"] = "INTERRUPTED_OR_TIMEOUT"
                wait_ok = False
        else:
            retirement["wait_outcome"] = "NOT_JOINED_STILL_RUNNING"
        obj = self.verify_container()
        retirement["after_wait"] = safe_container_state(obj)
        require(stop_ok and wait_ok and not obj["State"].get("Running")
                and obj["State"].get("Status") in ["created", "exited"]
                and obj["State"].get("ExitCode") == 0 and not obj["State"].get("OOMKilled"),
                "fixture did not stop and join normally; retained")
        self.verify_container(running=False)
        self.call(["docker", "rm", self.container])
        retirement["status"] = "REMOVED_AFTER_NORMAL_STOP" if obj["State"]["Status"] == "exited" else "REMOVED_NEVER_STARTED"
        self.container = None
        self.container_name = None
        self.create_attempted = False


    def capacity(self, stage):
        free = shutil.disk_usage(self.root).free
        self.result.setdefault("capacity", []).append({"stage": stage, "free_bytes": free, "minimum_bytes": 12 * 1024**3})
        require(free >= 12 * 1024**3, "at least 12 GiB free required before heavy stage")

    def verify_binaries(self):
        require(set(self.binary_identities) == set(IDENTITIES), "native binary identities incomplete")
        for name, identity in self.binary_identities.items():
            path = self.private / "build" / name
            require(path.is_file() and not path.is_symlink() and path.stat().st_size == identity["bytes"]
                    and sha(path) == identity["sha256"], "native built/scanned binary changed")

    def verify_source_modules(self, name):
        checkout, identity = self.source_dirs[name], IDENTITIES[name]
        require(git(checkout, self.env, "rev-parse", "HEAD") == identity["revision"], "scan source revision changed")
        require([line.strip() for line in git(checkout, self.env, "status", "--porcelain", "--untracked-files=all").splitlines()]
                == ["M go.mod", "M go.sum"], "scan changed source beyond reviewed modules")
        require(all(sha(checkout / filename) == expected for filename, expected in identity["patched"].items()), "scan changed patched module bytes")

    def scan_binaries(self):
        self.step("native scanner identity and four complete scans")
        self.capacity("native graphs and scans")
        self.verify_binaries()
        tool_dir = self.private / "scanner-bin"
        tool_dir.mkdir(mode=0o700)
        install_env = dict(self.env, GOBIN=str(tool_dir))
        _, module_out, module_err = self.call(["go", "mod", "download", "-json", "golang.org/x/vuln@v1.8.0"], cwd=self.private, env=install_env, timeout=180)
        write_new(self.evidence / "scanner-module.json", module_out)
        module = json.loads(module_out)
        require(not module_err.strip() and not module.get("Error") and module.get("Path") == "golang.org/x/vuln"
                and module.get("Version") == "v1.8.0" and module.get("Sum") == SCANNER_MODULE_SUM, "scanner module identity differs")
        self.call(["go", "install", "golang.org/x/vuln/cmd/govulncheck@v1.8.0"], cwd=self.private, env=install_env, timeout=600)
        scanner = tool_dir / "govulncheck"
        info = self.call(["go", "version", "-m", scanner])[1].decode()
        require(info.splitlines()[0] == str(scanner) + ": go1.27.2", "scanner toolchain differs")
        for marker in ["\tpath\tgolang.org/x/vuln/cmd/govulncheck", "\tmod\tgolang.org/x/vuln\tv1.8.0\t" + SCANNER_MODULE_SUM,
                       "\tbuild\tGOOS=linux", "\tbuild\tGOARCH=arm64", "\tbuild\tCGO_ENABLED=0"]:
            require(marker in info, "scanner build identity incomplete")
        write_new(self.evidence / "scanner-buildinfo.txt", info.replace(str(scanner), "<govulncheck>"))
        scanner_identity = {"sha256": sha(scanner), "bytes": scanner.stat().st_size, "module_sum": SCANNER_MODULE_SUM,
                            "install_command": ["go", "install", "golang.org/x/vuln/cmd/govulncheck@v1.8.0"], "toolchain": "go1.27.2 linux/arm64"}
        write_json(self.evidence / "scanner-provenance.json", scanner_identity)
        self.result["scanner"] = scanner_identity
        database_versions = set()
        for name, identity in IDENTITIES.items():
            checkout = self.source_dirs[name]
            self.verify_source_modules(name)
            graph_args = ["go", "list", "-mod=readonly", "-deps", "-json", "."]
            graph_code, graph_raw, graph_err = self.call(graph_args, cwd=checkout, timeout=300, expected=None)
            write_new(self.evidence / (name + "-packages.jsonl"), graph_raw)
            write_new(self.evidence / (name + "-packages.stderr"), graph_err)
            require(graph_code == 0 and not graph_err.strip(), "native package graph command incomplete")
            graph = validate_graph(parse_json_stream(graph_raw), identity["module"])
            module_args = ["go", "list", "-mod=readonly", "-m", "-json", "all"]
            module_code, module_raw, module_err = self.call(module_args, cwd=checkout, timeout=180, expected=None)
            write_new(self.evidence / (name + "-modules.jsonl"), module_raw)
            write_new(self.evidence / (name + "-modules.stderr"), module_err)
            require(module_code == 0 and not module_err.strip(), "native module list command incomplete")
            graph["module_versions"] = validate_modules(parse_json_stream(module_raw), identity)
            graph.update(package_command=graph_args, module_command=module_args, package_exit=graph_code, module_exit=module_code,
                         package_sha256=sha(self.evidence / (name + "-packages.jsonl")), modules_sha256=sha(self.evidence / (name + "-modules.jsonl")))
            write_json(self.evidence / (name + "-graph-summary.json"), graph)
            for mode in ["source", "binary"]:
                self.verify_binaries()
                require(sha(scanner) == scanner_identity["sha256"], "scanner executable changed")
                target = "." if mode == "source" else self.private / "build" / name
                args = [scanner, "-format=json", "-scan=symbol", "-mode=" + mode, target]
                code, raw, err = self.call(args, cwd=checkout, env=dict(self.env, GOFLAGS="-mod=readonly"), timeout=900 if mode == "source" else 300, expected=None)
                prefix = name + "-" + mode + "-govulncheck"
                write_new(self.evidence / (prefix + ".jsonl"), raw)
                write_new(self.evidence / (prefix + ".stderr"), err)
                record = {"target": name, "mode": mode, "exit_code": code, "complete": False,
                          "command": ["<govulncheck>", "-format=json", "-scan=symbol", "-mode=" + mode, "." if mode == "source" else "<" + name + ">"],
                          "cwd": "<source-" + name + ">", "scanner_sha256": scanner_identity["sha256"],
                          "source_revision": identity["revision"], "native_binary": dict(self.binary_identities[name]),
                          "report_sha256": sha(self.evidence / (prefix + ".jsonl")), "stderr_sha256": sha(self.evidence / (prefix + ".stderr"))}
                self.result.setdefault("scan_attempts", []).append(record)
                require(code == 0 and not err.strip(), "native scanner process incomplete or failed")
                summary = validate_scan(parse_json_stream(raw), mode, identity, graph)
                record.update(summary)
                record["complete"] = True
                self.scan_records[name + "-" + mode] = record
                database_versions.add(summary["config"]["db_last_modified"])
                self.verify_source_modules(name)
                self.verify_binaries()
        require(len(self.scan_records) == 4 and len(database_versions) == 1, "four complete scans of one reported DB version required")
        write_json(self.evidence / "scan-summary.json", {"schema": 1, "scans": self.scan_records, "scan_count": 4,
                   "security_acceptance": "OPEN", "base_layer_scan": "NOT_PERFORMED",
                   "finding_disposition": "known findings OPEN; removed and unknown finding IDs refused before runtime"})
        self.passed()

    def build_binaries(self):
        self.step("exact reviewed source builds")
        self.capacity("native builds")
        build_dir = self.private / "build"
        for name, identity in IDENTITIES.items():
            checkout = self.private / ("source-" + name)
            checkout.mkdir(mode=0o700)
            git(checkout, self.env, "init", "--quiet")
            git(checkout, self.env, "remote", "add", "origin", identity["url"])
            self.call(["git", "-c", "credential.helper=", "-C", checkout, "fetch", "--depth=1",
                       "origin", identity["revision"]], timeout=180)
            git(checkout, self.env, "checkout", "--detach", "--quiet", identity["revision"])
            require(git(checkout, self.env, "remote", "get-url", "origin") == identity["url"], "source URL differs")
            require(git(checkout, self.env, "rev-parse", "HEAD") == identity["revision"], "source revision differs")
            require(git(checkout, self.env, "rev-parse", "HEAD^{tree}") == identity["tree"], "source tree differs")
            require(not git(checkout, self.env, "status", "--porcelain", "--untracked-files=all"), "source checkout dirty")
            for filename, expected in identity["original"].items():
                require(sha(checkout / filename) == expected, "original module bytes differ")
            patch = self.repo / ("server/minio/candidate-patches/" + name + "-module.patch")
            git(checkout, self.env, "apply", "--check", str(patch))
            git(checkout, self.env, "apply", str(patch))
            for filename, expected in identity["patched"].items():
                require(sha(checkout / filename) == expected, "patched module bytes differ")
            changes = git(checkout, self.env, "status", "--porcelain", "--untracked-files=all").splitlines()
            require([line.strip() for line in changes] == ["M go.mod", "M go.sum"],
                    "source changes exceed reviewed modules")
            binary = build_dir / name
            self.call(["go", "build", "-mod=readonly", "-trimpath", "-buildvcs=true", "-o", binary, "."],
                      cwd=checkout, timeout=900)
            info = self.call(["go", "version", "-m", binary])[1].decode()
            require(info.splitlines()[0] == str(binary) + ": go1.27.2", "built toolchain differs")
            for marker in ["\tpath\t" + identity["module"], "\tdep\tgolang.org/x/net\tv0.60.0\t",
                           "\tbuild\tvcs.revision=" + identity["revision"], "\tbuild\tvcs.modified=true",
                           "\tbuild\tCGO_ENABLED=0", "\tbuild\tGOOS=linux", "\tbuild\tGOARCH=arm64"]:
                require(marker in info, "required buildinfo marker absent")
            write_new(self.evidence / (name + "-buildinfo.txt"), info.replace(str(binary), "<" + name + ">"))
            write_new(self.evidence / (name + "-module.patch"), patch.read_bytes())
            actual = sha(binary)
            self.binary_identities[name] = {"sha256": actual, "bytes": binary.stat().st_size}
            self.result.setdefault("built_binaries", {})[name] = dict(self.binary_identities[name])
            self.source_dirs[name] = checkout
            for filename, expected in identity["patched"].items():
                require(sha(checkout / filename) == expected, "build changed reviewed modules")
        self.passed()


    def build_image(self):
        self.step("immutable candidate image build and extracted binary identity")
        self.capacity("image build/export")
        require(set(self.scan_records) == {"mcli-source", "mcli-binary", "silo-source", "silo-binary"}, "four native scans required")
        self.verify_binaries()
        self.call(["docker", "pull", "--platform", "linux/arm64", BASE], timeout=240)
        base = self.docker_json("image", "inspect", BASE)[0]
        require(base.get("Id") == BASE_ID and base.get("Os") == "linux" and base.get("Architecture") == "arm64",
                "base image platform/config identity differs")
        require(base.get("Config", {}).get("Labels", {}).get("org.opencontainers.image.revision")
                == IDENTITIES["silo"]["revision"], "base source revision differs")
        require(base.get("Config", {}).get("Env") == BASE_ENV,
                "base image environment differs from exact reviewed defaults")
        require(all(base["Config"].get(k) == v for k, v in BASE_RUNTIME.items()), "base runtime defaults differ")
        dockerfile = ("FROM " + BASE + "\nCOPY --chmod=0755 silo /usr/bin/silo\n"
                      "COPY --chmod=0755 mcli /usr/bin/mcli\n"
                      "LABEL org.juicemount.candidate.source=\"" + self.source_sha + "\"\n"
                      "LABEL org.juicemount.candidate.scope=\"ordinary-compatibility-security-and-migration-open\"\n"
                      "LABEL org.juicemount.candidate.silo-sha256=\"" + self.binary_identities["silo"]["sha256"] + "\"\n"
                      "LABEL org.juicemount.candidate.mcli-sha256=\"" + self.binary_identities["mcli"]["sha256"] + "\"\n")
        write_new(self.private / "build/Dockerfile", dockerfile)
        write_new(self.evidence / "candidate-Dockerfile", dockerfile)
        iid = self.private / "image-id"
        self.call(["docker", "build", "--network=none", "--platform", "linux/arm64", "--iidfile", iid,
                   str(self.private / "build")], timeout=240)
        self.image_id = iid.read_text().strip()
        require(re.fullmatch(r"sha256:[0-9a-f]{64}", self.image_id), "candidate image ID invalid")
        candidate = self.docker_json("image", "inspect", self.image_id)[0]
        require(candidate.get("Id") == self.image_id and candidate.get("Os") == "linux"
                and candidate.get("Architecture") == "arm64", "candidate platform/config identity differs")
        require(candidate["Config"].get("Entrypoint") == base["Config"].get("Entrypoint")
                and candidate["Config"].get("Env") == base["Config"].get("Env"), "candidate changed runtime defaults")
        require(all(candidate["Config"].get(k) == v for k, v in BASE_RUNTIME.items()), "candidate runtime defaults differ")
        labels = candidate["Config"].get("Labels", {})
        require(labels.get("org.juicemount.candidate.source") == self.source_sha, "candidate provenance label differs")
        require(all(labels.get("org.juicemount.candidate." + n + "-sha256") == self.binary_identities[n]["sha256"] for n in IDENTITIES), "candidate binary labels differ")
        self.create_container("extract")
        for name in ["silo", "mcli"]:
            self.verify_container(running=False)
            extracted = self.private / "extract" / name
            self.call(["docker", "cp", self.container + ":/usr/bin/" + name, extracted])
            require(extracted.is_file() and not extracted.is_symlink()
                    and sha(extracted) == self.binary_identities[name]["sha256"], "image binary differs from scanned build")
        link = self.private / "extract/mc"
        self.call(["docker", "cp", self.container + ":/usr/bin/mc", link])
        require(link.is_symlink() and os.readlink(link) in ["mcli", "/usr/bin/mcli"], "packaged mc symlink differs")
        followed = self.private / "extract/mc-followed"
        self.call(["docker", "cp", "-L", self.container + ":/usr/bin/mc", followed])
        require(sha(followed) == self.binary_identities["mcli"]["sha256"], "packaged mc target differs")
        self.retire_container()
        self.passed()
        self.step("export verified candidate before credentials")
        tar = self.private / "candidate-image.tar"
        self.call(["docker", "image", "save", "--output", tar, self.image_id], timeout=120)
        exported = self.evidence / "candidate-image.tar.gz"
        with tar.open("rb") as source, exported.open("xb") as target:
            with gzip.GzipFile(filename="", mode="wb", fileobj=target, mtime=0) as compressed:
                shutil.copyfileobj(source, compressed, 1024 * 1024)
        tar.unlink()
        image_record = {"config_id": self.image_id, "base": BASE, "base_config_id": BASE_ID,
                        "platform": "linux/arm64", "archive_sha256": sha(exported),
                        "archive_bytes": exported.stat().st_size,
                        "extracted_binary_sha256": {n: self.binary_identities[n]["sha256"] for n in IDENTITIES},
                        "source_sha": self.source_sha, "scan_summary_sha256": sha(self.evidence / "scan-summary.json"), "qualification_receipt": "result.json",
                        "ordinary_compatibility_at_export": "NOT_RUN",
                        "security_and_migration": "OPEN"}
        write_json(self.evidence / "candidate-image.json", image_record)
        self.result["candidate_image"] = image_record
        self.passed()

    def compile_test(self):
        self.step("compile reviewed bundled JuiceFS object test")
        self.test_binary = self.private / "storage-compatibility.test"
        self.call(["go", "test", "-c", "-mod=readonly", "-tags", "storageacceptance", "-o", self.test_binary,
                   "./server/minio"], cwd=self.repo, timeout=600)
        require(not git(self.repo, self.env, "status", "--porcelain", "--untracked-files=all"), "test compile changed checkout")
        info = self.call(["go", "version", "-m", self.test_binary])[1].decode()
        require(info.splitlines()[0] == str(self.test_binary) + ": go1.27.2", "test binary toolchain differs")
        for marker in ["\tbuild\tGOOS=linux", "\tbuild\tGOARCH=arm64", "\tbuild\tCGO_ENABLED=0"]:
            require(marker in info, "native test architecture marker absent")
        self.test_binary_sha = sha(self.test_binary)
        write_new(self.evidence / "compatibility-buildinfo.txt", info.replace(str(self.test_binary), "<storage-compatibility.test>"))
        self.result["compatibility_binary_sha256"] = self.test_binary_sha
        self.passed()

    def container_exec(self, args, variables=None, *, timeout=45, expected=0):
        self.verify_container(running=True)
        values = variables or {}
        allowed = {"MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "JM_MINIO_ENDPOINT", "JM_MINIO_POLICY_DIR",
                   "JM_OBJ_CLIENT_SECRET_KEY", "JM_OBJ_GC_SECRET_KEY", "JM_OBJ_PER_MAC_SERVER", "TMPDIR", "MC"}
        require(set(values) <= allowed, "unexpected container exec variable")
        launch = ["docker", "exec", "-i"]
        for key in sorted(values):
            launch += ["--env", key]
        clear = [key for key in ["MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_BROWSER", "MINIO_UPDATE"] if key not in values]
        # Parameter expansion never embeds credential values in the command.
        launch += [self.container, "/bin/bash", "-c", "unset " + " ".join(clear) + "; exec \"$@\"", "--", *args]
        return self.call(launch, env=dict(self.env, **values), timeout=timeout, expected=expected)

    def mc(self, *args, expected=0, timeout=30):
        return self.container_exec(["/usr/bin/mc", "--config-dir", "/control/mc-root", *args],
                                   expected=expected, timeout=timeout)

    def fixture(self):
        self.step("fresh owned fixture and authenticated readiness")
        require(not any((self.private / "data").iterdir()), "fixture data is not empty")
        # Credentials follow verified image import/extraction and test compilation.
        root_secret, client_secret, gc_secret = (secrets.token_hex(20) for _ in range(3))
        require(len({root_secret, client_secret, gc_secret}) == 3, "credential collision")
        self.secrets = [root_secret, client_secret, gc_secret]
        for name in ["provision-object-keys.sh", "jm-client-policy.json", "jm-gc-policy.json"]:
            write_new(self.private / "control/policies" / name, (self.repo / "server/minio" / name).read_bytes())
        write_json(self.private / "control/mc-root/config.json", {
            "version": "10", "aliases": {"trial": {"url": "http://127.0.0.1:9000", "accessKey": "juicemount",
            "secretKey": root_secret, "api": "S3v4", "path": "auto"}}})
        server_env = dict(self.env, MINIO_ROOT_USER="juicemount", MINIO_ROOT_PASSWORD=root_secret,
                          MINIO_BROWSER="off", MINIO_UPDATE="off")
        self.create_container("runtime", env=server_env)
        self.result["private_fixture"] = "retained privately; never uploaded"
        self.verify_container(running=False)
        self.call(["docker", "start", self.container])
        started = time.monotonic()
        deadline = started + 45
        attempts = []
        self.result["readiness_attempts"] = attempts
        while True:
            remaining = deadline - time.monotonic()
            require(remaining > 0, "authenticated readiness deadline exceeded")
            obj = self.verify_container()
            attempt = {"attempt": len(attempts) + 1, "elapsed_start_ms": round((time.monotonic() - started) * 1000),
                       "container_state": safe_container_state(obj), "outcome": "HOLD", "category": "command_incomplete"}
            attempts.append(attempt)
            require(obj["State"].get("Running") is True, "owned storage container exited before readiness")
            try:
                code, out, err = self.mc("--json", "admin", "info", "trial", expected=None, timeout=min(5, remaining))
                attempt.update(classify_readiness(code, out, err))
            finally:
                attempt["elapsed_end_ms"] = round((time.monotonic() - started) * 1000)
            if attempt["outcome"] == "PASS":
                require(time.monotonic() <= deadline, "authenticated success arrived after readiness deadline")
                break
            require(attempt["outcome"] == "RETRY", "readiness HOLD: " + attempt["category"])
            remaining = deadline - time.monotonic()
            require(remaining > 0, "authenticated readiness deadline exceeded")
            time.sleep(min(0.3, remaining))
        obj = self.verify_container(running=True)
        ports = obj.get("NetworkSettings", {}).get("Ports", {})
        published = {key: value for key, value in ports.items() if value}
        require(set(published) == {"9000/tcp"} and len(published["9000/tcp"]) == 1,
                "unexpected published runtime ports")
        binding = published["9000/tcp"][0]
        require(binding.get("HostIp") == "127.0.0.1" and re.fullmatch(r"[0-9]+", binding.get("HostPort", "")),
                "runtime port is not literal IPv4 loopback")
        port = int(binding["HostPort"])
        require(1 <= port <= 65535, "runtime port out of range")
        endpoint = "http://127.0.0.1:" + str(port) + "/zpool"
        self.result["fixture"] = {"container_id": self.container, "image_id": self.image_id,
                                  "owner_label": self.owner, "endpoint": endpoint,
                                  "readiness": "owned_running_and_authenticated_admin_info"}
        self.passed()
        self.step("production two-role provisioning")
        self.mc("mb", "trial/zpool")
        variables = {"MC": "/usr/bin/mc", "MINIO_ROOT_USER": "juicemount", "MINIO_ROOT_PASSWORD": root_secret,
                     "JM_MINIO_ENDPOINT": "http://127.0.0.1:9000", "JM_MINIO_POLICY_DIR": "/control/policies",
                     "JM_OBJ_CLIENT_SECRET_KEY": client_secret, "JM_OBJ_GC_SECRET_KEY": gc_secret,
                     "JM_OBJ_PER_MAC_SERVER": "0", "TMPDIR": "/control/tmp"}
        self.container_exec(["/bin/bash", "/control/policies/provision-object-keys.sh"], variables, timeout=60)
        for name in ["jm-client", "jm-gc"]:
            user = json.loads(self.mc("--json", "admin", "user", "info", "trial", name)[1])
            require(user.get("policyName") == name and user.get("userStatus") == "enabled",
                    "provisioned user policy/status differs")
        self.result["provisioning"] = {"roles": ["jm-client", "jm-gc"], "per_mac_server": False,
                                       "production_script": "PASS", "exact_policy_status": "PASS"}
        self.passed()
        return endpoint, client_secret, gc_secret

    def compatibility(self, endpoint, client_secret, gc_secret):
        self.step("ordinary bundled object compatibility all ten cases")
        acceptance = self.private / "acceptance"
        acceptance.mkdir(mode=0o700)
        owned_dir(acceptance)
        nonce = secrets.token_hex(32)
        fingerprint = lambda key, secret: hashlib.sha256((key + "\0" + secret).encode()).hexdigest()
        admission = {"schema": 1, "purpose": "juicemount-storage-compatibility", "disposable": True,
                     "endpoint": endpoint, "nonce": nonce, "created_unix": int(time.time()),
                     "client_sha256": fingerprint("jm-client", client_secret),
                     "gc_sha256": fingerprint("jm-gc", gc_secret), "test_name": "TestStorageCompatibility",
                     "test_binary_sha256": self.test_binary_sha}
        write_json(acceptance / "admission.json", admission)
        test_env = clean_env()
        test_env.update(TMPDIR=str(self.private / "tmp"), JM_STORAGE_ACCEPTANCE_ROOT=str(acceptance),
                        JM_STORAGE_ACCEPTANCE_NONCE=nonce, JM_STORAGE_ACCEPTANCE_CLIENT_ACCESS_KEY="jm-client",
                        JM_STORAGE_ACCEPTANCE_CLIENT_SECRET_KEY=client_secret,
                        JM_STORAGE_ACCEPTANCE_GC_ACCESS_KEY="jm-gc", JM_STORAGE_ACCEPTANCE_GC_SECRET_KEY=gc_secret)
        self.verify_container(running=True)
        code, out, err = self.call([self.test_binary, "-test.run=^TestStorageCompatibility$", "-test.count=1",
                                   "-test.parallel=1", "-test.v", "-test.timeout=5m"], env=test_env,
                                  cwd=self.repo, timeout=315, expected=None)
        text = (out + err).decode(errors="replace")
        # Save only structural test lines, never raw diagnostics or SDK errors.
        lines = [line.strip() for line in text.splitlines() if re.fullmatch(
            r"\s*(?:=== RUN\s+TestStorageCompatibility(?:/[A-Za-z0-9_]+)?|"
            r"--- (?:PASS|FAIL|SKIP): TestStorageCompatibility(?:/[A-Za-z0-9_]+)? \([0-9.]+s\)|PASS|FAIL)", line)]
        write_new(self.evidence / "compatibility-status.txt", "exit_code=" + str(code) + "\n" + "\n".join(lines) + "\n")
        self.result["compatibility_exit_code"] = code
        result_file = acceptance / "result.json"
        require(result_file.is_file() and not result_file.is_symlink(), "compatibility receipt absent")
        receipt = json.loads(result_file.read_text())
        allowed = {"schema", "nonce", "prefix", "state", "checks", "http_events", "test_binary_sha256",
                   "declared_checks", "object_cleanup_state"}
        require(set(receipt) == allowed, "unexpected compatibility receipt fields")
        # Exact test code writes only this schema; validate it before exporting.
        encoded = json.dumps(receipt)
        require(not any(secret in encoded for secret in self.secrets), "credential found in receipt; withheld")
        write_json(self.evidence / "compatibility-result.json", receipt)
        self.result["compatibility"] = {"state": receipt.get("state"), "checks": receipt.get("checks"),
                                        "object_cleanup_state": receipt.get("object_cleanup_state")}
        require((acceptance / "consumed.json").is_file(), "admission was not consumed")
        require(code == 0 and "--- SKIP:" not in text and "--- FAIL:" not in text, "test process failed or skipped")
        observed_passes = re.findall(r"(?m)^\s*--- PASS: TestStorageCompatibility/([A-Za-z0-9_]+) \(", text)
        require(observed_passes == CHECKS, "go test output does not prove all ten ordered subtests")
        require(len(re.findall(r"(?m)^--- PASS: TestStorageCompatibility \(", text)) == 1
                and len(re.findall(r"(?m)^PASS$", text)) == 1, "top-level test PASS absent or repeated")
        require(receipt.get("schema") == 1 and receipt.get("nonce") == nonce
                and receipt.get("test_binary_sha256") == self.test_binary_sha,
                "compatibility receipt identity differs")
        require(receipt.get("state") == "PASS" and receipt.get("checks") == CHECKS
                and receipt.get("declared_checks") == 10 and receipt.get("object_cleanup_state") == "COMPLETE",
                "compatibility receipt does not prove ten cases and complete object cleanup")
        events = receipt.get("http_events")
        require(isinstance(events, list) and bool(events), "no observed object HTTP requests")
        require(all(isinstance(e, dict) and e.get("transport_failed") is False
                    and isinstance(e.get("status"), int) and 200 <= e["status"] <= 499 for e in events),
                "HTTP receipt contains unknown transport/server result")
        require({e.get("case") for e in events} ==
                {"zero", "unicode", "4MiB", "create-only", "multipart", "abort", "prefix-list"},
                "HTTP receipt does not cover the seven declared object labels")
        self.verify_container(running=True)
        self.passed()

    def finalize(self):
        try:
            self.retire_container()
        except BaseException as exc:
            self.result["retirement_error"] = exc.args[0] if isinstance(exc, Hold) and exc.args else type(exc).__name__
            self.result.setdefault("first_red", {"stage": "owned fixture retirement", "reason": self.result["retirement_error"]})
            self.result["status"] = "HOLD"
        self.result["last_stage"] = self.stage
        self.result["private_state_exported"] = False
        self.result["full_security"] = "OPEN"
        self.result["closure"] = {"blocker_groups_closed": 0, "blocker_groups_total": 3, "founder_workflows_closed": 0, "founder_total": 7}
        write_json(self.evidence / "result.json", self.result)
        existing = list(self.evidence.iterdir())
        require(all(p.name in ARTIFACTS and p.name != "artifact-manifest.json" and p.is_file() and not p.is_symlink()
                    and p.stat().st_uid == os.geteuid() and stat.S_IMODE(p.stat().st_mode) == 0o600 for p in existing), "artifact custody/allowlist violated")
        needles = [secret.encode() for secret in self.secrets]
        overlap = max([len(v) for v in needles] + [1]) - 1
        for path in existing:
            tail = b""
            with path.open("rb") as source:
                while True:
                    chunk = source.read(1024 * 1024)
                    if not chunk: break
                    combined = tail + chunk
                    require(not any(secret in combined for secret in needles), "credential found in evidence; publication withheld")
                    tail = combined[-overlap:] if overlap else b""
        manifest = {"schema": 1, "source_sha": self.source_sha, "platform": "linux/arm64",
                    "files": {p.name: {"sha256": sha(p), "bytes": p.stat().st_size} for p in sorted(existing)},
                    "private_fixture_excluded": True, "retirement_outcome_recorded": True}
        write_json(self.evidence / "artifact-manifest.json", manifest)
        require({p.name for p in self.evidence.iterdir()} == set(manifest["files"]) | {"artifact-manifest.json"}, "artifact set changed before publication")
        require(all(sha(self.evidence / name) == value["sha256"] and (self.evidence / name).stat().st_size == value["bytes"]
                    for name, value in manifest["files"].items()), "artifact bytes changed before publication")
        self.evidence_validated = True

    def publish_evidence(self):
        # Invoked only after finalize returns successfully; no retry. A partial
        # filesystem write failure cannot establish whether readiness was emitted.
        require(getattr(self, "evidence_validated", False) is True, "evidence publication precedes validation")
        descriptor = os.open(self.output_file, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "w") as output:
            output.write("evidence_dir=" + str(self.evidence) + "\nevidence_ready=true\n")


def main():
    trial = None
    try:
        admission = preflight()
        trial = Qualification(admission)
        def interrupted(signum, frame):
            raise Hold("controller interrupted; first outcome retained")
        signal.signal(signal.SIGTERM, interrupted)
        signal.signal(signal.SIGINT, interrupted)
        trial.build_binaries()
        trial.scan_binaries()
        trial.build_image()
        trial.compile_test()
        fixture = trial.fixture()
        trial.compatibility(*fixture)
        trial.result["status"] = PASS
    except BaseException as exc:
        if trial is None:
            print("HOLD: preflight or private-root admission failed (" + type(exc).__name__ + ")", flush=True)
            return 1
        trial.result["status"] = "HOLD"
        trial.result["first_red"] = {"stage": trial.stage, "reason": exc.args[0] if isinstance(exc, Hold) and exc.args else type(exc).__name__}
    finally:
        if trial is not None:
            try:
                trial.finalize()
            except BaseException as exc:
                print("HOLD_RECEIPT " + format_finalization_failure(trial.result.get("first_red"), exc, trial.secrets), flush=True)
                return 1
            try:
                trial.publish_evidence()
            except BaseException as exc:
                print("HOLD_RECEIPT " + format_finalization_failure(trial.result.get("first_red"), exc, trial.secrets, phase="publication"), flush=True)
                return 1
    print(trial.result["status"], flush=True)
    return 0 if trial.result["status"] == PASS else 1


if __name__ == "__main__":
    raise SystemExit(main())
