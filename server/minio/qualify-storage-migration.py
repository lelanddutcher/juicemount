#!/usr/bin/env python3
"""One cloud-only stopped MinIO -> Silo -> untouched-snapshot restore trial.

Only ordinary authenticated operations on a fresh disposable fixture. No SDK
rerun, image rebuild, registry publication, security probes or production changes.
"""
import sys
if sys.flags.optimize:
    sys.stderr.write("HOLD: optimized Python is not admitted\n")
    raise SystemExit(2)

import datetime as dt
import errno
import hashlib
import hmac
import json
import os
from pathlib import Path
import platform
import re
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import tempfile
import time
import types
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

PARENT = "1926003336f6aa76da5c46c7fc52b7f5c8df6974"
PARENT_TREE = "bcfcd82a284ae0daa046dab4157a5ed3ed4ec0ee"
BRANCH = "refs/heads/codex/0.5-storage-migration-r2-1926003"
HELPER_SHA = "fd21c2a9174eac69dcd29ef26cdcd2a17ade3fe63249c70a47e8964e493e53c4"
PATHS = [".github/workflows/ci.yml", "server/minio/qualify-storage-migration.py"]
URL = "http://127.0.0.1:19000"
ACTIVE = "mac-0123456789ab-1"
PRE_REVOKED = "mac-0123456789ac-1"
PASS = "PASS_STOPPED_SINGLE_NODE_MIGRATION_RESTORE_SECURITY_AND_MOUNTED_ACCEPTANCE_OPEN"
PHASES = [
    "verify original immutable artifact before image load",
    "load verified archive and extract packaged binaries without starting",
    "verify retained legacy inputs and four owned socket controls",
    "legacy provisioning and seeded access",
    "stopped complete snapshot and disjoint copies",
    "first Silo start inherited IAM and objects before repair",
    "candidate reprovision post-upgrade write and revocation",
    "Silo restart preserves objects and revocations",
    "legacy snapshot restore before reconciliation",
    "restored keyd state remains usable",
    "normal retirement and immutable source preservation",
]
LEGACY = {
    "minio": {
        "url": "https://github.com/minio/minio.git",
        "revision": "827004cd6d3da8f49a5320c94ae74ae128156ed6",
        "module": "github.com/minio/minio",
        "go.mod": "1653dc86c87c79625fddcac3d120e039772a6f5eacf57fdfa4ca1263001a61d7",
        "go.sum": "4508802c4fb08a2a14abe557a6ea10cdffed8c33f6b80ef3d88c62b02380d953",
        "sha256": "3e0c506e74fa02a6e9788402b51b4eecbae4d2a301d6f8508db8d308ce5df29d",
    },
    "mc": {
        "url": "https://github.com/minio/mc.git",
        "revision": "cc8758c88da31b95dde7b0b6997980ebfb8b98c4",
        "module": "github.com/minio/mc",
        "go.mod": "91a5bc05ad8590ffe7697028f25875ec5ad8582e85d80bb416876021bef2a23a",
        "go.sum": "51e4747efc81524863faba0ecfffd2a0cf115007dbcd0622edeeb98bda5ecada",
        "sha256": "bf2961e6ecee008ff853d905fbf0752d46ae7699b91ecd4e7ced7b9691f9bd43",
    },
}
ROLE = {"root": "juicemount", "client": "jm-client", "gc": "jm-gc",
        "keyadmin": "jm-keyadmin", "active": ACTIVE, "pre-revoked": PRE_REVOKED}
POLICIES = {"jm-client": "jm-client", "jm-gc": "jm-gc",
            "jm-macs": "jm-client", "jm-keyadmin": "jm-keyadmin"}
ARTIFACTS = {"result.json", "provenance.json", "candidate-image-reference.json",
             "phase-evidence.json", "artifact-manifest.json"}
LEGACY_SOURCE = "1926003336f6aa76da5c46c7fc52b7f5c8df6974"
LEGACY_RUN = "38219780080"
LEGACY_ARTIFACT = "storage-migration-legacy-inputs-38219780080-1"
LEGACY_IMPORT_DIR = "storage-migration-legacy-import-38219780080-1"
LEGACY_MANIFEST_SHA = "a17e9191daf424082ee9d048aa115aac398423da9f1ef443d222b994398d61b6"
LEGACY_BYTES = {"minio": 157127351, "mc": 42984924}
LEGACY_TREES = {"minio": "2589ae46ba8ca031131c65b655306850045fccc6",
                "mc": "9592ee60ee40ce8dc73ed1df08032da4fbda442b"}
H = None


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    # IAM policy collections are unordered. Normalize order only, preserving
    # every field, value, duplicate and scalar/list distinction.
    if isinstance(value, dict):
        return {k: canonical(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return sorted((canonical(v) for v in value),
                      key=lambda v: json.dumps(v, sort_keys=True, separators=(",", ":")))
    return value


def canonical_bytes(value):
    return json.dumps(canonical(value), sort_keys=True, separators=(",", ":")).encode()


def classify_readiness(exit_code, output, error):
    """Pure oracle for pinned mc's string-shaped admin-info error at port19000."""
    result = {"outcome": "HOLD", "category": "unknown_error", "exit_code": exit_code,
              "stdout_sha256": digest(output), "stderr_sha256": digest(error)}
    if len(output) > 1024 * 1024 or len(error) > 1024 * 1024:
        result["category"] = "oversized_response"
        return result
    try:
        obj = json.loads(output.decode("utf-8"))
    except (ValueError, UnicodeError):
        result["category"] = "malformed_json"
        return result
    if not isinstance(obj, dict):
        result["category"] = "malformed_json"
        return result
    status = obj.get("status")
    result["json_status"] = status if status in ["success", "error"] else "unknown"
    search = json.dumps(obj.get("error", {})) + error.decode(errors="replace")
    if re.search(r"access[ _-]?denied|invalidaccesskeyid|signaturedoesnotmatch|unauthorized|forbidden|"
                 r"authentication|authorization|invalid credentials|invalid access key|invalid secret key|"
                 r"invalid token|(?:statusCode|status_code|httpStatusCode)[\" ]*:[ ]*(?:401|403)",
                 search, re.IGNORECASE):
        result["category"] = "authentication_or_authorization_error"
        return result
    if error.strip():
        result["category"] = "unexpected_stderr"
        return result
    if status == "success":
        info = obj.get("info")
        servers = info.get("servers") if isinstance(info, dict) else None
        if exit_code != 0 or obj.get("error"):
            result["category"] = "success_with_error_or_nonzero_exit"
        elif not isinstance(servers, list) or not servers or not all(isinstance(v, dict) and v for v in servers):
            result["category"] = "missing_required_server_info"
        else:
            result.update(outcome="PASS", category="authenticated_admin_info")
        return result
    if status != "error" or not isinstance(obj.get("error"), str) or exit_code not in [0, 1]:
        result["category"] = "unknown_status_error_shape_or_exit"
        return result
    message = obj["error"]
    connection = (r'(?:Get "http://127\.0\.0\.1:19000/minio/admin/v[0-9]+/info(?:\?[^"\s]*)?": )?'
                  r'dial tcp 127\.0\.0\.1:19000: connect: connection refused')
    if re.fullmatch(connection, message):
        result.update(outcome="RETRY", category="loopback_connection_refused")
    elif message in {"Server not initialized, please try again.",
                     "Server not initialized yet, please try again."}:
        result.update(outcome="RETRY", category="server_initializing")
    return result


TCP_STATES = {
    "01": "ESTABLISHED", "02": "SYN_SENT", "03": "SYN_RECV", "04": "FIN_WAIT1",
    "05": "FIN_WAIT2", "06": "TIME_WAIT", "07": "CLOSE", "08": "CLOSE_WAIT",
    "09": "LAST_ACK", "0A": "LISTEN", "0B": "CLOSING", "0C": "NEW_SYN_RECV",
}
PORT_CONTROL_CASES = ["vacant", "active_loopback", "active_wildcard", "observed_time_wait"]


def scoped_socket_states(port):
    """Read only the requested local port; infer no PID or listener ownership."""
    result = {"status": "KNOWN", "local_port": port, "rows": []}
    try:
        for protocol in ["tcp", "tcp6"]:
            with open("/proc/net/" + protocol, "rb") as stream:
                data = stream.read(2 * 1024**2 + 1)
            if len(data) > 2 * 1024**2:
                raise ValueError("socket table bound")
            lines = data.decode("ascii").splitlines()
            if not lines or not {"local_address", "st"} <= set(lines[0].split()):
                raise ValueError("socket table header absent or unknown")
            for line in lines[1:]:
                fields = line.split()
                if len(fields) < 10 or not re.fullmatch(r"[0-9A-Fa-f]{8,32}:[0-9A-Fa-f]{4}", fields[1]):
                    raise ValueError("socket table shape")
                address, local_port = fields[1].split(":")
                if int(local_port, 16) != port:
                    continue
                state = fields[3].upper()
                if not re.fullmatch(r"[0-9A-F]{2}", state):
                    raise ValueError("socket state shape")
                raw = bytes.fromhex(address)
                if protocol == "tcp" and len(raw) == 4:
                    packed = raw[::-1]
                    scope = "wildcard" if packed == b"\0"*4 else "loopback" if packed[0] == 127 else "other"
                elif protocol == "tcp6" and len(raw) == 16:
                    packed = b"".join(raw[i:i+4][::-1] for i in range(0, 16, 4))
                    scope = ("wildcard" if packed == b"\0"*16 else "loopback" if packed == b"\0"*15+b"\1"
                             or packed[:12] == b"\0"*10+b"\xff\xff" and packed[12] == 127 else "other")
                else:
                    raise ValueError("address shape")
                result["rows"].append({"protocol": protocol, "local_scope": scope,
                                       "state": TCP_STATES.get(state, "UNKNOWN"), "state_hex": state})
                if len(result["rows"]) > 64:
                    raise ValueError("scoped socket row bound")
    except (OSError, ValueError, UnicodeError) as exc:
        result.update(status="UNKNOWN", error_category=type(exc).__name__,
                      errno=exc.errno if isinstance(exc, OSError) else None)
    return result


def port_admission(port):
    """A non-reserving Linux bind probe, matching Go's SO_REUSEADDR semantics.

    Only empty or TIME_WAIT scoped tables are admissible. A bind error never
    establishes a listener or ownership. Startup still requires the exact owned
    server handle/CID and authenticated readiness after this probe closes.
    """
    H.require(sys.platform == "linux" and type(port) is int and 1 <= port <= 65535,
              "Linux scoped port admission required")
    start = time.monotonic()
    receipt = {"port": port, "address": "127.0.0.1", "outcome": "HOLD",
               "category": "UNCLASSIFIED", "before": scoped_socket_states(port),
               "reuseaddr": None, "reuseport": None, "bind_succeeded": False, "errno": None}
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            receipt["reuseaddr"] = probe.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR)
            receipt["reuseport"] = probe.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT)
            if receipt["reuseaddr"] != 1 or receipt["reuseport"] != 0:
                receipt["category"] = "UNEXPECTED_SOCKET_OPTIONS"
            else:
                probe.bind(("127.0.0.1", port))
                receipt["bind_succeeded"] = True
    except OSError as exc:
        receipt["errno"] = exc.errno
    receipt["after"] = scoped_socket_states(port)
    receipt["elapsed_ms"] = round((time.monotonic() - start) * 1000)
    observations = [receipt["before"], receipt["after"]]
    rows = [row for observation in observations for row in observation["rows"]]
    if any(observation["status"] != "KNOWN" for observation in observations):
        receipt["category"] = "UNKNOWN_SOCKET_TABLE"
    elif any(row["state"] == "LISTEN" for row in rows):
        receipt["category"] = "ACTIVE_LISTENER_OBSERVED"
    elif any(row["state"] != "TIME_WAIT" for row in rows):
        receipt["category"] = "OTHER_OR_UNKNOWN_SOCKET_STATE"
    elif receipt["reuseaddr"] != 1 or receipt["reuseport"] != 0:
        receipt["category"] = "UNEXPECTED_SOCKET_OPTIONS"
    elif not receipt["bind_succeeded"]:
        receipt["category"] = "BIND_ERROR_CAUSE_UNRESOLVED"
    elif receipt["elapsed_ms"] > 2000:
        receipt["category"] = "ADMISSION_TIME_BOUND_EXCEEDED"
    else:
        receipt.update(outcome="ADMIT", category="CLOSED_ADDRESS_REUSABLE" if rows else "VACANT")
    return receipt


def bare_bind_control(port):
    """Only invoked on a port from this controller's closed connection."""
    receipt = {"port": port, "bind_succeeded": False, "errno": None,
               "reuseaddr": None, "reuseport": None}
    start = time.monotonic()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            receipt["reuseaddr"] = probe.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR)
            receipt["reuseport"] = probe.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT)
            probe.bind(("127.0.0.1", port))
            receipt["bind_succeeded"] = True
    except OSError as exc:
        receipt["errno"] = exc.errno
    receipt["elapsed_ms"] = round((time.monotonic() - start) * 1000)
    return receipt


def load_helpers():
    path = Path(__file__).resolve().with_name("qualify-storage-candidate.py")
    data = path.read_bytes()
    if path.is_symlink() or digest(data) != HELPER_SHA:
        raise RuntimeError("hash-bound candidate helper differs")
    module = types.ModuleType("retained_candidate_helpers")
    module.__file__ = str(path)
    exec(compile(data, str(path), "exec"), module.__dict__)
    return module


def preflight():
    H.require(len(sys.argv) == 1 and sys.dont_write_bytecode, "isolated no-bytecode invocation required")
    H.require(sys.platform == "linux" and platform.machine() == "x86_64", "Linux amd64 required")
    H.require(os.environ.get("GITHUB_ACTIONS") == "true"
              and os.environ.get("RUNNER_OS") == "Linux" and os.environ.get("RUNNER_ARCH") == "X64",
              "GitHub-hosted Linux x64 required")
    H.require(os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
              and os.environ.get("GITHUB_REF") == BRANCH
              and os.environ.get("GITHUB_RUN_ATTEMPT") == "1", "exact first manual diagnostic run required")
    H.require(os.environ.get("GITHUB_REPOSITORY", "").lower() == "lelanddutcher/juicemount",
              "repository context differs")
    source = os.environ.get("GITHUB_SHA", "")
    run = os.environ.get("GITHUB_RUN_ID", "")
    H.require(re.fullmatch(r"[0-9a-f]{40}", source) and re.fullmatch(r"[0-9]+", run), "source/run missing")
    repo = Path(os.environ["GITHUB_WORKSPACE"])
    H.require(repo.is_absolute() and repo.resolve(strict=True) == repo
              and Path(__file__).resolve() == repo / PATHS[1], "canonical admitted checkout required")
    env = H.clean_env()
    H.require(H.git(repo, env, "rev-parse", "HEAD") == source, "checkout source differs")
    H.require(H.git(repo, env, "show", "-s", "--format=%P", "HEAD") == PARENT, "single parent differs")
    H.require(H.git(repo, env, "rev-parse", PARENT + "^{tree}") == PARENT_TREE, "parent tree differs")
    H.require(not H.git(repo, env, "status", "--porcelain", "--untracked-files=all"), "checkout not clean")
    H.require(sorted(H.git(repo, env, "diff", "--name-only", PARENT, "HEAD").splitlines()) == PATHS,
              "diagnostic delta differs")
    for relative in PATHS + ["server/minio/qualify-storage-candidate.py",
                              "server/minio/provision-object-keys.sh", "server/minio/keyd.sh",
                              "server/minio/jm-client-policy.json", "server/minio/jm-gc-policy.json",
                              "server/minio/jm-keyadmin-policy.json", "scripts/prepare-ci-bash.sh"]:
        path = repo / relative
        H.require(path.is_file() and not path.is_symlink(), "source file not regular")
    temp = Path(os.environ["RUNNER_TEMP"])
    H.require(temp.is_absolute() and temp.resolve(strict=True) == temp and temp.is_dir()
              and temp.stat().st_uid == os.geteuid(), "RUNNER_TEMP custody differs")
    H.require(shutil.disk_usage(temp).free >= 12 * 1024**3, "at least12 GiB free required")
    bash = Path(os.environ["JM_TEST_KEYD_BASH"])
    H.require(bash.is_file() and not bash.is_symlink() and bash.resolve(strict=True) == bash
              and bash.stat().st_uid == os.geteuid() and temp in bash.parents, "private Bash custody differs")
    H.require(H.command([bash, "--version"], env)[1].splitlines()[0].startswith(b"GNU bash, version 5.3."),
              "private Bash5.3 required")
    output = Path(os.environ["GITHUB_OUTPUT"])
    info = output.lstat()
    H.require(stat.S_ISREG(info.st_mode) and not output.is_symlink() and info.st_uid == os.geteuid(),
              "owned GITHUB_OUTPUT required")
    return repo, env, temp, output, run, source, bash


def tree_manifest(root):
    H.owned_dir(root)
    result = {}
    device = root.stat().st_dev
    paths = [root]
    for directory, folders, files in os.walk(root, followlinks=False):
        paths.extend(Path(directory) / n for n in sorted(folders + files))
    for path in sorted(set(paths)):
        item = path.lstat()
        H.require(item.st_dev == device and item.st_uid == os.geteuid(), "snapshot ownership/device differs")
        row = {"mode": stat.S_IMODE(item.st_mode), "uid": item.st_uid, "gid": item.st_gid}
        if stat.S_ISDIR(item.st_mode):
            row["kind"] = "directory"
        elif stat.S_ISREG(item.st_mode):
            H.require(item.st_nlink == 1, "snapshot contains shared hardlink")
            row.update(kind="file", bytes=item.st_size, sha256=H.sha(path))
        elif stat.S_ISLNK(item.st_mode):
            target = os.readlink(path)
            H.require(not os.path.isabs(target) and root in path.resolve(strict=False).parents,
                      "snapshot symlink leaves owned root")
            row.update(kind="symlink", target=target)
        else:
            raise H.Hold("snapshot contains unsupported entry")
        result["." if path == root else path.relative_to(root).as_posix()] = row
    return result


def manifest_summary(value):
    return {"sha256": digest(canonical_bytes(value)), "entries": len(value),
            "files": sum(r["kind"] == "file" for r in value.values()),
            "directories": sum(r["kind"] == "directory" for r in value.values()),
            "symlinks": sum(r["kind"] == "symlink" for r in value.values()),
            "bytes": sum(r.get("bytes", 0) for r in value.values())}


class RefuseRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file, code, message, headers, newurl):
        raise H.Hold("object oracle redirect refused")


class Migration:
    def __init__(self, admission):
        self.repo, self.env, temp, output, self.run_id, self.source_sha, self.bash = admission
        os.umask(0o077)
        self.root = Path(tempfile.mkdtemp(prefix="jm-storage-migration-" + self.run_id + "-", dir=temp))
        H.owned_dir(self.root)
        self.private, self.evidence, self.build_inputs = [self.root / n for n in ["private", "evidence", "legacy-inputs"]]
        self.legacy_reference = self.root / "legacy-reference"
        for folder in [self.private, self.evidence, self.build_inputs, self.legacy_reference]:
            folder.mkdir(mode=0o700)
        for n in ["tmp", "docker", "data", "control", "extract",
                  "runtime", "snapshots", "seed-inputs", "probes"]:
            (self.private / n).mkdir(mode=0o700)
        self.env.update(TMPDIR=str(self.private / "tmp"), DOCKER_CONFIG=str(self.private / "docker"))
        self.stage = "admitted"
        self.result = {"schema": 1, "status": "HOLD", "run_id": self.run_id, "source_sha": self.source_sha,
                       "parent": PARENT, "declared_phases": PHASES, "stages": [],
                       "container_retirement": [], "native_retirement": [],
                       "limits": ["single-node stopped Linux amd64 data/IAM/keyd/config migration only",
                                  "security acceptance open", "full JuiceFS metadata acceptance open",
                                  "Manager/Mac/Link and mounted/provider acceptance open",
                                  "encryption/versioning/distributed/live-writer acceptance open",
                                  "arm64 acceptance open", "no production or registry changes"]}
        self.phases, self.verified_binaries = {}, {}
        self.image_id = self.container = self.container_name = None
        self.create_attempted = False
        self.retirement_attempted = False
        self.owner = secrets.token_hex(32)
        self.native = self.native_log = None
        self.native_retirement_attempted = False
        self.snapshots = None
        self.seed_manifest = None
        self.secrets = []
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}), RefuseRedirect())
        self.output_file = output
        self.published = set()
        scripts = ["server/minio/" + n for n in ["keyd.sh", "provision-object-keys.sh",
                    "jm-client-policy.json", "jm-gc-policy.json", "jm-keyadmin-policy.json"]]
        H.write_json(self.evidence / "provenance.json", {
            "schema": 1, "trial_source_sha": self.source_sha, "parent": PARENT, "parent_tree": PARENT_TREE,
            "diagnostic_paths": {p: H.sha(self.repo / p) for p in PATHS},
            "retained_helper_sha256": HELPER_SHA, "production_inputs": {p: H.sha(self.repo / p) for p in scripts},
            "bash_prerequisite_source_sha256": H.sha(self.repo / "scripts/prepare-ci-bash.sh"),
            "bash_binary_sha256": H.sha(self.bash), "legacy_identities": LEGACY,
            "legacy_input_mode": "verified retained artifact; no Go toolchain or source build",
            "image_source_sha": H.IMAGE_SOURCE, "image_archive_sha256": H.ARCHIVE_SHA,
            "image_config_id": H.IMAGE_ID, "endpoint": URL,
            "image_scope": "unchanged original artifact; no pull, image rebuild or scan",
            "sdk_scope": "prior ten-case ordinary compatibility PASS reused; not executed by this trial",
            "trial_scope": "fresh stopped snapshot migration and restore; no inherited acceptance result",
        })

    def step(self, name):
        H.require(len(self.result["stages"]) < len(PHASES)
                  and PHASES[len(self.result["stages"])] == name, "phase declaration/order differs")
        self.stage = name
        self.phases.setdefault(name, {})
        print("PHASE " + name, flush=True)

    def passed(self):
        H.require(self.stage == PHASES[len(self.result["stages"])], "phase completion differs")
        self.result["stages"].append(self.stage)
        self.phases[self.stage]["outcome"] = "PASS"

    def call(self, args, **kwargs):
        return H.command(args, kwargs.pop("env", self.env), **kwargs)

    def docker_json(self, *args):
        return json.loads(self.call(["docker", *args])[1])

    def verify_container(self, *, running=None):
        obj = H.Qualification.verify_container(self, running=running)
        if self.container_phase == "runtime":
            H.require(obj["HostConfig"]["PortBindings"] == {
                "9000/tcp": [{"HostIp": "127.0.0.1", "HostPort": "19000"}]},
                "migration fixed loopback binding differs")
        return obj

    def create_container(self, phase, *, env=None):
        self.retirement_attempted = False
        if phase == "extract":
            return H.Qualification.create_container(self, phase, env=env)
        H.require(phase == "runtime" and not self.create_attempted and self.container is None,
                  "runtime creation overlaps prior container")
        H.require(self.native is None, "runtime container overlaps native server")
        self.ports_free()
        self.container_phase = phase
        self.container_name = "jm-migration-" + secrets.token_hex(12)
        args = ["docker", "create", "--name", self.container_name,
                "--label", H.LABEL + "=" + self.owner, "--network", "bridge", "--restart", "no",
                "--user", str(os.getuid()) + ":" + str(os.getgid()),
                "--mount", "type=bind,src=" + str(self.private / "data") + ",dst=/data",
                "--mount", "type=bind,src=" + str(self.private / "control") + ",dst=/control",
                "--publish", "127.0.0.1:19000:9000"]
        for name in ["MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_BROWSER", "MINIO_UPDATE"]:
            args += ["--env", name]
        args += [self.image_id, "server", "--address", ":9000", "--console-address", "127.0.0.1:9001", "/data"]
        self.create_attempted = True
        self.container = self.call(args, env=self.server_env())[1].decode().strip()
        H.require(re.fullmatch(r"[0-9a-f]{64}", self.container), "exact created CID absent")
        self.verify_container(running=False)

    def retire_container(self):
        if not self.create_attempted:
            return
        H.require(not self.retirement_attempted, "prior uncertain container retirement retained without retry")
        self.retirement_attempted = True
        H.Qualification.retire_container(self)

    def import_image(self):
        H.Qualification.import_image(self)

    def import_legacy(self):
        self.step(PHASES[2])
        imported = Path(os.environ["RUNNER_TEMP"]) / LEGACY_IMPORT_DIR
        H.require(imported.resolve(strict=True) == imported and imported.is_dir()
                  and imported.stat().st_uid == os.geteuid(), "legacy import directory custody differs")
        expected_files = {"minio", "mc", "minio-buildinfo.txt", "mc-buildinfo.txt", "provenance.json"}
        manifest_path = imported / "artifact-manifest.json"
        H.require(manifest_path.is_file() and not manifest_path.is_symlink()
                  and H.sha(manifest_path) == LEGACY_MANIFEST_SHA, "original legacy manifest differs")
        manifest = json.loads(manifest_path.read_text())
        H.require(manifest.get("schema") == 1 and manifest.get("source_sha") == LEGACY_SOURCE
                  and manifest.get("run_id") == LEGACY_RUN and manifest.get("private_fixture_excluded") is True
                  and set(manifest.get("files", {})) == expected_files
                  and {p.name for p in imported.iterdir()} == expected_files | {"artifact-manifest.json"},
                  "original legacy manifest identity or six-file set differs")
        for name, identity in manifest["files"].items():
            path = imported / name
            H.require(path.is_file() and not path.is_symlink() and path.resolve(strict=True) == path
                      and path.stat().st_uid == os.geteuid() and path.stat().st_size == identity["bytes"]
                      and H.sha(path) == identity["sha256"], "legacy artifact member differs")
        provenance = json.loads((imported / "provenance.json").read_text())
        H.require(provenance.get("schema") == 1 and provenance.get("source_sha") == LEGACY_SOURCE
                  and provenance.get("run_id") == LEGACY_RUN
                  and provenance.get("status") == "VERIFIED_BUILD_INPUTS"
                  and provenance.get("environment") == {"CGO_ENABLED": "0", "GOOS": "linux",
                                                        "GOARCH": "amd64", "GOAMD64": "v1"}
                  and provenance.get("build_command") == "go build -mod=readonly -trimpath -buildvcs=true -o <binary> ."
                  and set(provenance.get("binaries", {})) == set(LEGACY),
                  "legacy provenance differs")
        for name, identity in LEGACY.items():
            prior = provenance["binaries"][name]
            H.require(prior == {"sha256": identity["sha256"], "bytes": LEGACY_BYTES[name],
                                "revision": identity["revision"], "tree": LEGACY_TREES[name],
                                "toolchain": "go1.26.6", "vcs_modified": False}
                      and H.sha(imported / name) == identity["sha256"], "legacy source/binary receipt differs")
            info = (imported / (name + "-buildinfo.txt")).read_text()
            lines = info.splitlines()
            H.require(lines and lines[0] == "<" + name + ">: go1.26.6", "retained legacy toolchain differs")
            fields = {}
            for line in lines[1:]:
                parts = line.strip().split("\t")
                if len(parts) >= 2 and parts[0] == "build" and "=" in parts[1]:
                    key, value = parts[1].split("=", 1)
                    fields[key] = value
            for key, value in {"CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": "amd64", "GOAMD64": "v1",
                               "vcs": "git", "vcs.revision": identity["revision"], "vcs.modified": "false",
                               "-trimpath": "true"}.items():
                H.require(fields.get(key) == value, "retained legacy buildinfo setting differs")
            H.require(any(line.strip().split("\t")[:2] == ["path", identity["module"]] for line in lines),
                      "retained legacy module path differs")
            destination = self.build_inputs / name
            H.require(not destination.exists() and not destination.is_symlink(), "legacy runtime copy already exists")
            shutil.copyfile(imported / name, destination)
            destination.chmod(0o500)
            H.require(destination.is_file() and not destination.is_symlink()
                      and destination.stat().st_uid == os.geteuid() and H.sha(destination) == identity["sha256"],
                      "legacy runtime executable copy differs")
            self.verified_binaries[name] = prior
        H.write_json(self.legacy_reference / "legacy-input-reference.json", {
            "schema": 1, "trial_source_sha": self.source_sha, "original_source_sha": LEGACY_SOURCE,
            "original_run_id": LEGACY_RUN, "original_attempt": 1, "original_artifact_id": 11733683437,
            "original_artifact_name": LEGACY_ARTIFACT, "original_manifest_sha256": LEGACY_MANIFEST_SHA,
            "original_archive_sha256_reference": "862e862f806003a3d9f107fdf2d5ecdb56a55872fe8c68c0fa1847eff0191482",
            "verification": "all six extracted members bound by exact original manifest; copied executables rehashed",
            "custody": "original executable artifact referenced; no rebuild or binary re-upload",
            "binaries": self.verified_binaries,
        })
        self.result["legacy_binaries"] = self.verified_binaries
        self.phases[self.stage]["verified_binary_names"] = sorted(self.verified_binaries)
        self.publish_artifacts(self.legacy_reference, {"legacy-input-reference.json"}, "legacy")
        self.qualify_ports()
        self.passed()

    def qualify_ports(self):
        controls = {"declared_cases": PORT_CONTROL_CASES, "completed": [], "cases": {}, "outcome": "HOLD"}
        self.phases[self.stage]["port_controls"] = controls
        for name in PORT_CONTROL_CASES:
            row = {"outcome": "HOLD", "owned_socket_handles_closed": False}
            controls["cases"][name] = row
            handles = []
            try:
                listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                handles.append(listener)
                listener.settimeout(2)
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind(("0.0.0.0" if name == "active_wildcard" else "127.0.0.1", 0))
                port = listener.getsockname()[1]
                row["port"] = port
                if name == "vacant":
                    listener.close()
                    row["admission"] = port_admission(port)
                    H.require(row["admission"]["outcome"] == "ADMIT"
                              and row["admission"]["category"] == "VACANT", "vacant socket control inconclusive")
                elif name in ["active_loopback", "active_wildcard"]:
                    listener.listen(1)
                    row["admission"] = port_admission(port)
                    H.require(row["admission"]["outcome"] == "HOLD"
                              and row["admission"]["category"] == "ACTIVE_LISTENER_OBSERVED",
                              "active listener refusal control inconclusive")
                else:
                    listener.listen(1)
                    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    handles.append(client)
                    client.settimeout(2)
                    client.bind(("127.0.0.1", 0))
                    client.connect(("127.0.0.1", port))  # Only this original owned listener.
                    accepted, peer = listener.accept()
                    handles.append(accepted)
                    accepted.settimeout(2)
                    H.require(peer == client.getsockname() and accepted.getsockname() == ("127.0.0.1", port),
                              "control connection identity differs")
                    accepted.shutdown(socket.SHUT_WR)  # Server side deliberately closes first.
                    H.require(client.recv(1) == b"", "control client did not observe owned FIN")
                    client.shutdown(socket.SHUT_WR)
                    H.require(accepted.recv(1) == b"", "control server did not observe owned FIN")
                    accepted.close()
                    client.close()
                    listener.close()
                    deadline = time.monotonic() + 2
                    samples = row["time_wait_observations"] = []
                    while True:
                        observation = scoped_socket_states(port)
                        samples.append(observation)
                        if observation["status"] != "KNOWN" or any(r["state"] == "TIME_WAIT" for r in observation["rows"]):
                            break
                        if time.monotonic() >= deadline:
                            break
                        time.sleep(0.05)
                    H.require(observation["status"] == "KNOWN" and observation["rows"]
                              and all(r["state"] == "TIME_WAIT" for r in observation["rows"]),
                              "owned TIME_WAIT counterexample not observed; inconclusive")
                    row["old_bare_bind"] = bare_bind_control(port)
                    row["admission"] = port_admission(port)
                    H.require(not row["old_bare_bind"]["bind_succeeded"]
                              and row["old_bare_bind"]["errno"] == errno.EADDRINUSE
                              and row["old_bare_bind"]["reuseaddr"] == 0
                              and row["old_bare_bind"]["reuseport"] == 0
                              and row["admission"]["outcome"] == "ADMIT"
                              and row["admission"]["category"] == "CLOSED_ADDRESS_REUSABLE",
                              "bare-bind versus TIME_WAIT reuse counterexample inconclusive")
                row["outcome"] = "PASS"
                controls["completed"].append(name)
            except OSError as exc:
                row.update(socket_error_category=type(exc).__name__, errno=exc.errno)
                raise H.Hold("owned socket control failed; scoped evidence retained") from None
            finally:
                for handle in reversed(handles):
                    handle.close()  # Never rediscover, signal, or close a foreign descriptor.
                row["owned_socket_handles_closed"] = True
        H.require(controls["completed"] == PORT_CONTROL_CASES, "four socket controls did not complete")
        controls.update(outcome="PASS", completed_count=4)

    def ports_free(self):
        observations = self.phases[self.stage].setdefault("port_admissions", [])
        for port in [19000, 19001]:
            receipt = port_admission(port)
            observations.append(receipt)
            H.require(receipt["outcome"] == "ADMIT", "loopback port admission unresolved; no foreign listener attribution")

    def server_env(self):
        return dict(self.env, MINIO_ROOT_USER="juicemount", MINIO_ROOT_PASSWORD=self.creds["root"],
                    MINIO_BROWSER="off", MINIO_UPDATE="off")

    def native_start(self, data, tag):
        H.require(self.native is None and not self.create_attempted, "native start overlaps owned server")
        H.owned_dir(data)
        self.ports_free()
        self.native_retirement_attempted = False
        log = self.private / ("native-" + tag + ".log")
        descriptor = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        self.native_log = os.fdopen(descriptor, "wb")
        self.native = subprocess.Popen(
            [str(self.build_inputs / "minio"), "server", "--address", "127.0.0.1:19000",
             "--console-address", "127.0.0.1:19001", str(data)],
            env=self.server_env(), stdin=subprocess.DEVNULL,
            stdout=self.native_log, stderr=subprocess.STDOUT, start_new_session=True)
        self.readiness(native=True)

    def native_stop(self):
        if self.native is None:
            return
        H.require(not self.native_retirement_attempted, "prior native retirement uncertainty retained without retry")
        self.native_retirement_attempted = True
        process = self.native  # Original Popen handle, never a discovered PID/name.
        receipt = {"pid": process.pid, "before_returncode": process.poll(), "status": "RETAINED_UNCERTAIN"}
        self.result["native_retirement"].append(receipt)
        if process.poll() is None:
            process.terminate()
            receipt["signal"] = "TERM"
        try:
            code = process.wait(timeout=35)
        except subprocess.TimeoutExpired:
            receipt["wait_outcome"] = "STILL_RUNNING_NO_FORCE"
            raise H.Hold("native server did not join; no forced termination") from None
        receipt.update(wait_outcome="JOINED", returncode=code)
        H.require(code == 0, "native server exited nonzero; state retained")
        receipt["status"] = "JOINED_ZERO"  # Process fact precedes any independent port admission.
        self.native_log.close()
        self.native = self.native_log = None
        self.ports_free()

    def mc(self, *args, who="root", **kwargs):
        return self.call([self.build_inputs / "mc", "--config-dir", self.auth / ROLE[who], *args], **kwargs)

    def mc_json(self, *args, who="root"):
        code, output, error = self.mc("--json", *args, who=who, expected=None)
        receipt = {"role": who, "operation": " ".join(str(a) for a in args[:3]),
                   "exit_code": code, "stdout_sha256": digest(output), "stderr_sha256": digest(error),
                   "category": "UNVALIDATED"}
        self.phases[self.stage].setdefault("admin_calls", []).append(receipt)
        H.require(len(output) <= 1024**2 and len(error) <= 1024**2, "admin JSON response exceeds bound")
        try:
            value = json.loads(output.decode("utf-8"))
        except (ValueError, UnicodeError):
            receipt["category"] = "MALFORMED_JSON"
            raise H.Hold("admin JSON response malformed") from None
        receipt["json_status"] = (value.get("status") if isinstance(value, dict)
                                  and value.get("status") in ["success", "error"] else "unknown")
        receipt["category"] = "REJECTED_OR_UNRECOGNIZED"
        H.require(code == 0 and not error.strip() and isinstance(value, dict)
                  and value.get("status") == "success" and not value.get("error"),
                  "authenticated mc JSON operation did not report success")
        receipt["category"] = "PASS"
        return value

    def mc_read(self, key, expected, who):
        code, output, error = self.mc("cat", "trial/zpool/" + key, who=who, expected=None)
        receipt = {"role": who, "key": key, "exit_code": code, "bytes": len(output),
                   "expected_bytes": len(expected), "sha256": digest(output),
                   "expected_sha256": digest(expected), "stderr_sha256": digest(error), "outcome": "HOLD"}
        self.phases[self.stage].setdefault("legacy_mc_reads", []).append(receipt)
        H.require(code == 0 and output == expected, "constant legacy mc credential read differs")
        receipt["outcome"] = "PASS"

    def readiness(self, *, native=False):
        receipts = self.phases[self.stage].setdefault("readiness", [])
        start = time.monotonic()
        while time.monotonic() - start < 45:
            if native:
                H.require(self.native is not None and self.native.poll() is None, "owned native server exited")
            else:
                self.verify_container(running=True)
            code, output, error = self.mc("--json", "admin", "info", "trial", expected=None, timeout=5)
            row = classify_readiness(code, output, error)
            row["elapsed_ms"] = round((time.monotonic() - start) * 1000)
            receipts.append(row)
            if row["outcome"] == "PASS":
                if native:
                    H.require(self.native.poll() is None, "native server exited after readiness")
                else:
                    self.verify_container(running=True)
                return
            H.require(row["outcome"] == "RETRY", "authenticated readiness unknown or rejected")
            time.sleep(0.2)
        raise H.Hold("owned server readiness exceeded45 seconds")

    def config(self, name, secret):
        folder = self.auth / name
        folder.mkdir(mode=0o700)
        H.write_json(folder / "config.json", {"version": "10", "aliases": {"trial": {
            "url": URL, "accessKey": name, "secretKey": secret, "api": "S3v4", "path": "auto"}}})

    def provision(self, binary):
        env = dict(self.server_env(), MC=str(binary), JM_MINIO_ENDPOINT=URL,
                   JM_MINIO_POLICY_DIR=str(self.repo / "server/minio"),
                   JM_OBJ_CLIENT_SECRET_KEY=self.creds["client"], JM_OBJ_GC_SECRET_KEY=self.creds["gc"],
                   JM_OBJ_KEYADMIN_SECRET_KEY=self.creds["keyadmin"], JM_OBJ_PER_MAC_SERVER="1")
        self.call([self.bash, self.repo / "server/minio/provision-object-keys.sh"], env=env, timeout=90)
        # Exit0 alone is explicitly insufficient; callers prove all four users,
        # policy documents and actual per-Mac behavior before completing a phase.

    def keyd(self, keys, seq, wanted, remove=(), *, candidate=False):
        pending = keys / ("desired-" + secrets.token_hex(8) + ".next")
        H.write_json(pending, {"seq": seq, "keys": [{"name": n, "state": "active"} for n in wanted],
                               "remove": list(remove)})
        pending.replace(keys / "desired.json")
        env = dict(self.env, MC=str(self.private / "extract/mcli" if candidate else self.build_inputs / "mc"),
                   JM_OBJ_PER_MAC_SERVER="1", JM_OBJ_KEYADMIN_SECRET_KEY=self.creds["keyadmin"],
                   JM_MINIO_ENDPOINT=URL, JM_OBJKEYS_DIR=str(keys), JM_KEYD_ONCE="1")
        self.call([self.bash, self.repo / "server/minio/keyd.sh"], env=env, timeout=90)
        applied = json.loads((keys / "applied.json").read_text())
        rows = applied.get("keys")
        H.require(applied.get("seq") == seq and not applied.get("error") and isinstance(rows, list)
                  and sorted(v.get("name") for v in rows) == sorted(wanted),
                  "keyd receipt sequence or accounts differ")
        H.require(all(v.get("exists") is True and v.get("status") == "on"
                      and v.get("parent") == "jm-macs" and v.get("implied_policy") is True for v in rows),
                  "keyd receipt parent/status/policy differs")
        self.phases[self.stage].setdefault("keyd", []).append({
            "seq": seq, "active_names": sorted(wanted), "removed_names": sorted(remove),
            "client_binary_sha256": H.sha(Path(env["MC"])), "applied_verified": True})

    def denied(self, *args, who, revoked=False):
        code, output, error = self.mc("--json", *args, who=who, expected=None)
        body = (output + error).decode(errors="replace")
        pattern = (r"InvalidAccessKeyId|InvalidAccessKeyID|Access Key Id you provided does not exist"
                   if revoked else r"AccessDenied|Access [Dd]enied")
        recognized = bool(re.search(pattern, body))
        receipt = {
            "role": who, "operation": args[0], "category": "revoked_key" if revoked else "access_denied",
            "exit_code": code, "recognized_expected_denial": recognized,
            "response_sha256": digest(output + error), "outcome": "HOLD"}
        self.phases[self.stage].setdefault("denials", []).append(receipt)
        H.require(code != 0 and recognized, "expected ordinary access refusal not observed")
        receipt["outcome"] = "PASS"

    def iam(self, *, active):
        rows, policy_digests = {}, {}
        receipt = {"users": rows, "policies": policy_digests, "outcome": "HOLD"}
        self.phases[self.stage].setdefault("iam_observations", []).append(receipt)
        for policy in sorted(set(POLICIES.values())):
            target = self.private / "probes" / (secrets.token_hex(12) + ".json")
            expected = json.loads((self.repo / "server/minio" / (policy + "-policy.json")).read_text())
            policy_row = {"expected_canonical_sha256": digest(canonical_bytes(expected)), "outcome": "HOLD"}
            policy_digests[policy] = policy_row
            self.mc_json("admin", "policy", "info", "--policy-file", target, "trial", policy)
            policy_row["regular_output_present"] = target.is_file() and not target.is_symlink()
            H.require(target.is_file() and not target.is_symlink(), "policy output absent")
            policy_row.update(output_sha256=H.sha(target), output_bytes=target.stat().st_size)
            H.require(target.stat().st_size <= 1024**2, "policy output exceeds bound")
            actual = json.loads(target.read_text())
            policy_row["actual_canonical_sha256"] = digest(canonical_bytes(actual))
            H.require(canonical(actual) == canonical(expected), "actual IAM policy document differs")
            policy_row["outcome"] = "PASS"
        for user, policy in POLICIES.items():
            user_row = {"expected_policy": policy, "expected_status": "enabled", "outcome": "HOLD"}
            rows[user] = user_row
            row = self.mc_json("admin", "user", "info", "trial", user)
            user_row.update(actual_policy=(row.get("policyName") if isinstance(row.get("policyName"), str)
                                           and row.get("policyName") in POLICIES else "UNRECOGNIZED"),
                            actual_status=(row.get("userStatus") if row.get("userStatus") in ["enabled", "disabled"]
                                           else "UNRECOGNIZED"),
                            observed_fields_sha256=digest(canonical_bytes({
                                k: row.get(k) for k in ["policyName", "userStatus"]})))
            H.require(row.get("policyName") == policy and row.get("userStatus") == "enabled",
                      "IAM user attachment/status differs")
            user_row["outcome"] = "PASS"
        if active:
            account_row = {"name": ACTIVE, "expected_parent": "jm-macs", "expected_status": "on", "outcome": "HOLD"}
            receipt["active_account"] = account_row
            account = self.mc_json("admin", "user", "svcacct", "info", "trial", ACTIVE, who="keyadmin")
            account_row.update(name_matches=account.get("accessKey") == ACTIVE,
                               parent_matches=account.get("parentUser") == "jm-macs",
                               actual_status=(account.get("accountStatus") if account.get("accountStatus") in ["on", "off"]
                                              else "UNRECOGNIZED"),
                               implied_policy=account.get("impliedPolicy") if type(account.get("impliedPolicy")) is bool else None,
                               observed_fields_sha256=digest(canonical_bytes({k: account.get(k) for k in
                                   ["accessKey", "parentUser", "impliedPolicy", "accountStatus"]})))
            H.require(account.get("accessKey") == ACTIVE and account.get("parentUser") == "jm-macs"
                      and account.get("impliedPolicy") is True and account.get("accountStatus") == "on",
                      "active service account identity/parent/status differs")
            account_row["outcome"] = "PASS"
            self.mc_read(self.present, self.seed[self.present], "active")
        else:
            self.denied("stat", "trial/zpool/" + self.present, who="active", revoked=True)
        self.denied("stat", "trial/zpool/" + self.present, who="pre-revoked", revoked=True)
        self.denied("rm", "trial/zpool/" + self.present, who="client")
        self.mc_read(self.present, self.seed[self.present], "client")
        self.mc_read(self.present, self.seed[self.present], "gc")
        receipt.update(outcome="PASS", active_credential_state="enabled" if active else "revoked",
                       pre_revoked_credential_state="revoked", client_delete_refused_bytes_preserved=True,
                       gc_credential_exact_read=True, keyadmin_authenticated_account_info=active)

    def sigv4_get(self, key="", *, range_pair=None, query=None):
        path = "/zpool" + ("/" + urllib.parse.quote(key, safe="/~") if key else "")
        query_string = "&".join(urllib.parse.quote(k, safe="~") + "=" + urllib.parse.quote(v, safe="~")
                                for k, v in sorted((query or {}).items()))
        timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        day, empty = timestamp[:8], digest(b"")
        headers = {"host": "127.0.0.1:19000", "x-amz-date": timestamp, "x-amz-content-sha256": empty}
        if range_pair:
            offset, length = range_pair
            headers["range"] = "bytes=" + str(offset) + "-" + str(offset + length - 1)
        signed = ";".join(sorted(headers))
        request_text = ("GET\n" + path + "\n" + query_string + "\n"
                        + "".join(k + ":" + headers[k] + "\n" for k in sorted(headers)) + "\n" + signed + "\n" + empty)
        scope = day + "/us-east-1/s3/aws4_request"
        text = "AWS4-HMAC-SHA256\n" + timestamp + "\n" + scope + "\n" + digest(request_text.encode())
        signing = ("AWS4" + self.creds["root"]).encode()
        for part in [day, "us-east-1", "s3", "aws4_request"]:
            signing = hmac.new(signing, part.encode(), hashlib.sha256).digest()
        signature = hmac.new(signing, text.encode(), hashlib.sha256).hexdigest()
        headers["authorization"] = ("AWS4-HMAC-SHA256 Credential=juicemount/" + scope
                                     + ", SignedHeaders=" + signed + ", Signature=" + signature)
        request = urllib.request.Request(URL + path + ("?" + query_string if query_string else ""), headers=headers)
        observation = {"key": key, "range": list(range_pair) if range_pair else None, "category": "STARTED"}
        self.phases[self.stage].setdefault("http_observations", []).append(observation)
        try:
            try:
                response = self.http.open(request, timeout=20)
            except urllib.error.HTTPError as rejected:
                response = rejected  # Preserve status/body hash; never expose raw error/body.
            with response:
                data = response.read(5 * 1024**2 + 1)
                observation.update(http_status=response.status, bytes=len(data), sha256=digest(data),
                                   category="HTTP_ERROR" if response.status >= 400 else "HTTP_RESPONSE")
                for name, pattern in [("Content-Length", r"[0-9]{1,20}"),
                                      ("Content-Range", r"bytes [0-9]{1,20}-[0-9]{1,20}/[0-9]{1,20}")]:
                    value = response.headers.get(name, "")
                    observation[name] = value if re.fullmatch(pattern, value) else None
                    observation[name + "_sha256"] = digest(value.encode())
                H.require(len(data) <= 5 * 1024**2, "object response exceeds fixture bound")
                return response.status, response.headers, data
        except (OSError, urllib.error.URLError):
            observation["category"] = "TRANSPORT_ERROR"
            raise H.Hold("ordinary object read failed; raw network error withheld") from None

    def objects(self, expected):
        receipt = {"expected_inventory": {k: len(v) for k, v in expected.items()},
                   "inventory_outcome": "HOLD", "objects": {}, "outcome": "HOLD"}
        self.phases[self.stage].setdefault("object_observations", []).append(receipt)
        status, _, raw = self.sigv4_get(query={"list-type": "2"})
        receipt.update(inventory_http_status=status, inventory_body_sha256=digest(raw), inventory_body_bytes=len(raw))
        H.require(status == 200, "inventory HTTP status differs")
        document = ET.fromstring(raw)
        receipt["inventory_not_truncated"] = document.findtext("{*}IsTruncated") == "false"
        H.require(document.findtext("{*}IsTruncated") == "false", "inventory truncated")
        pairs = [(v.findtext("{*}Key"), int(v.findtext("{*}Size"))) for v in document.findall("{*}Contents")]
        receipt.update(actual_inventory_entries=len(pairs), actual_inventory_sha256=digest(canonical_bytes(pairs)),
                       inventory_matches=len(dict(pairs)) == len(pairs)
                       and dict(pairs) == {k: len(v) for k, v in expected.items()})
        H.require(len(dict(pairs)) == len(pairs) and dict(pairs) == {k: len(v) for k, v in expected.items()},
                  "exact bucket inventory differs")
        receipt["inventory_outcome"] = "PASS"
        observed = receipt["objects"]
        for key, value in expected.items():
            row = {"expected_bytes": len(value), "expected_sha256": digest(value), "ranges": [], "outcome": "HOLD"}
            observed[key] = row
            status, headers, raw = self.sigv4_get(key)
            row.update(http_status=status, bytes=len(raw), sha256=digest(raw),
                       declared_length_matches=headers.get("Content-Length") == str(len(value)))
            H.require(status == 200 and raw == value and headers.get("Content-Length") == str(len(value)),
                      "complete object bytes/length/status differ")
            self.mc_read(key, value, "client")
            ranges = row["ranges"]
            if value:
                for offset, length in [(0, 1), (max(0, len(value) // 2 - 31), min(63, len(value))),
                                       (len(value) - 1, 1)]:
                    status, headers, part = self.sigv4_get(key, range_pair=(offset, length))
                    content_range = "bytes " + str(offset) + "-" + str(offset + length - 1) + "/" + str(len(value))
                    part_row = {"offset": offset, "expected_length": length, "bytes": len(part), "http_status": status,
                                "expected_content_range": content_range,
                                "content_range_matches": headers.get("Content-Range") == content_range,
                                "declared_length_matches": headers.get("Content-Length") == str(length),
                                "expected_sha256": digest(value[offset:offset + length]),
                                "sha256": digest(part), "outcome": "HOLD"}
                    ranges.append(part_row)
                    H.require(status == 206 and headers.get("Content-Range") == content_range
                              and headers.get("Content-Length") == str(length)
                              and part == value[offset:offset + length], "range status/header/bytes differ")
                    part_row["outcome"] = "PASS"
            row.update(outcome="PASS", legacy_mc_exact_read=True)
        receipt["outcome"] = "PASS"

    def seed_legacy(self):
        self.step(PHASES[3])
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        self.creds = {n: "".join(secrets.choice(alphabet) for _ in range(40))
                      for n in ["root", "client", "gc", "keyadmin"]}
        H.require(len(set(self.creds.values())) == 4, "generated credential collision")
        self.secrets.extend(self.creds.values())
        self.old = {n: self.private / "runtime" / (n + "-old") for n in ["data", "keys", "auth"]}
        for path in self.old.values():
            path.mkdir(mode=0o700)
        self.auth = self.old["auth"]
        for role in ["root", "client", "gc", "keyadmin"]:
            self.config(ROLE[role], self.creds[role])
        prefix = "migration-" + secrets.token_hex(24)
        self.seed = {prefix + "/zero.bin": b"", prefix + "/素材/clip 01.bin": secrets.token_bytes(997),
                     prefix + "/4KiB.bin": secrets.token_bytes(4096),
                     prefix + "/4MiB.bin": secrets.token_bytes(4 * 1024**2)}
        self.present = prefix + "/4KiB.bin"
        self.post_key = prefix + "/post-upgrade.bin"
        self.payload_paths = {}
        for index, (key, value) in enumerate(self.seed.items()):
            path = self.private / "seed-inputs" / str(index)
            H.write_new(path, value)
            self.payload_paths[key] = path
        self.seed_manifest = tree_manifest(self.private / "seed-inputs")
        self.native_start(self.old["data"], "seed")
        self.mc("mb", "trial/zpool")
        self.provision(self.build_inputs / "mc")
        self.keyd(self.old["keys"], 1, [ACTIVE, PRE_REVOKED])
        for name in [ACTIVE, PRE_REVOKED]:
            value = json.loads((self.old["keys"] / "results" / (name + ".json")).read_text())
            secret = value.get("secret_key")
            H.require(value.get("name") == name and isinstance(secret, str)
                      and 16 <= len(secret) <= 128 and "\n" not in secret, "generated account secret invalid")
            H.require(secret not in self.secrets, "generated account secret collision")
            self.secrets.append(secret)
            self.config(name, secret)
        self.keyd(self.old["keys"], 2, [ACTIVE, PRE_REVOKED])
        for name in [ACTIVE, PRE_REVOKED]:
            (self.old["keys"] / "results" / (name + ".json")).unlink()
        self.keyd(self.old["keys"], 3, [ACTIVE], [PRE_REVOKED])
        for key, path in self.payload_paths.items():
            self.mc("cp", path, "trial/zpool/" + key, who="client")
        self.iam(active=True)
        self.objects(self.seed)
        self.passed()

    def clone(self, source, target):
        H.require(not target.exists() and not target.is_symlink(), "snapshot target exists")
        shutil.copytree(source, target, symlinks=True, copy_function=shutil.copy2)
        H.require(tree_manifest(source) == tree_manifest(target), "snapshot copy bytes/metadata differ")

    def preserve_sources(self):
        if self.seed_manifest is not None:
            H.require(tree_manifest(self.private / "seed-inputs") == self.seed_manifest, "original seed bytes changed")
        if self.snapshots is not None:
            for name, original in self.snapshots.items():
                H.require(tree_manifest(self.old[name]) == original, "stopped original source changed")
                H.require(tree_manifest(self.private / "snapshots" / name) == original,
                          "untouched rollback snapshot changed")

    def snapshot(self):
        self.step(PHASES[4])
        self.native_stop()
        H.require((self.old["data"] / ".minio.sys").is_dir()
                  and not (self.old["data"] / ".minio.sys").is_symlink(), "legacy system/IAM store missing")
        self.snapshots = {name: tree_manifest(path) for name, path in self.old.items()}
        self.upgrade, self.restore = {}, {}
        H.owned_dir(self.private / "data")
        H.require(not list((self.private / "data").iterdir()), "extraction data directory not empty")
        (self.private / "data").rmdir()  # Only the verified empty, never-started extraction bind.
        for name, source in self.old.items():
            snapshot = self.private / "snapshots" / name
            self.clone(source, snapshot)
            self.upgrade[name] = (self.private / "data" if name == "data"
                                  else self.private / "runtime" / (name + "-upgrade"))
            self.restore[name] = self.private / "runtime" / (name + "-restore")
            self.clone(snapshot, self.upgrade[name])
            self.clone(snapshot, self.restore[name])
        all_paths = list(self.old.values()) + list(self.upgrade.values()) + list(self.restore.values())
        all_paths += [self.private / "snapshots" / n for n in self.snapshots]
        H.require(len(set(all_paths)) == len(all_paths)
                  and all(a not in b.parents for a in all_paths for b in all_paths if a != b),
                  "migration stores overlap")
        self.phases[self.stage]["snapshots"] = {n: manifest_summary(v) for n, v in self.snapshots.items()}
        self.preserve_sources()
        self.passed()

    def silo_start(self):
        self.create_container("runtime")
        self.verify_container(running=False)
        self.call(["docker", "start", self.container])
        self.readiness()
        self.phases[self.stage]["runtime_container_id"] = self.container

    def first_silo(self):
        self.step(PHASES[5])
        self.auth = self.upgrade["auth"]
        self.silo_start()
        self.iam(active=True)
        self.objects(self.seed)
        self.phases[self.stage]["before_any_provision_or_keyd"] = True
        self.preserve_sources()
        self.passed()

    def upgrade_operations(self):
        self.step(PHASES[6])
        self.provision(self.private / "extract/mcli")
        self.iam(active=True)
        value = secrets.token_bytes(8193)
        payload = self.private / "post-upgrade-input"
        H.write_new(payload, value)
        self.mc("cp", payload, "trial/zpool/" + self.post_key, who="active")
        self.post = dict(self.seed, **{self.post_key: value})
        self.keyd(self.upgrade["keys"], 4, [], [ACTIVE], candidate=True)
        self.iam(active=False)
        self.objects(self.post)
        self.preserve_sources()
        self.passed()

    def restart_silo(self):
        self.step(PHASES[7])
        previous = self.container
        self.retire_container()
        self.ports_free()
        self.preserve_sources()
        self.silo_start()
        H.require(self.container != previous, "restarted container custody not distinct")
        self.phases[self.stage]["restart_kind"] = "normal stop/wait0 then recreate same image over same upgrade copy"
        self.iam(active=False)
        self.objects(self.post)
        self.retire_container()
        self.ports_free()
        self.preserve_sources()
        self.passed()

    def restore_legacy(self):
        self.step(PHASES[8])
        self.preserve_sources()
        for name, path in self.restore.items():
            H.require(tree_manifest(path) == self.snapshots[name], "restore copy changed before first legacy open")
        self.auth = self.restore["auth"]
        self.native_start(self.restore["data"], "restore")
        self.iam(active=True)
        self.objects(self.seed)
        self.phases[self.stage]["before_any_keyd_or_provision"] = True
        self.result["rollback_observations"] = {
            "post_upgrade_write": "ABSENT_FROM_EXACT_RESTORED_INVENTORY",
            "post_upgrade_revocation": "REVERSED_ACTIVE_CREDENTIAL_READS_ORIGINAL_BYTES",
            "pre_upgrade_revocation": "PRESERVED_DENIAL_ON_PRESENT_OBJECT",
            "limitation": "Snapshot rollback loses writes and reverses revocations made after the snapshot.",
        }
        self.passed()
        self.step(PHASES[9])
        self.keyd(self.restore["keys"], 3, [ACTIVE])
        self.iam(active=True)
        self.objects(self.seed)
        self.passed()

    def finish(self):
        self.step(PHASES[10])
        self.native_stop()
        H.require(self.native is None and not self.create_attempted, "owned server still exists")
        self.preserve_sources()
        H.require(not H.git(self.repo, self.env, "status", "--porcelain", "--untracked-files=all"),
                  "trial changed source checkout")
        self.phases[self.stage]["originals_and_snapshots"] = "UNCHANGED_AFTER_FINAL_RETIREMENT"
        self.phases[self.stage]["seed_sha256"] = manifest_summary(self.seed_manifest)["sha256"]
        self.passed()
        H.require(self.result["stages"] == PHASES, "declared/executed phase mismatch")
        self.result["status"] = PASS

    def record_red(self, exc, stage=None):
        reason = str(exc) if isinstance(exc, H.Hold) else type(exc).__name__
        if any(secret in reason for secret in self.secrets):
            reason = "failure reason withheld because it contains credential material"
        self.result.setdefault("first_red", {"stage": stage or self.stage, "reason": reason})
        self.result["status"] = "HOLD"

    def publish_artifacts(self, folder, allowed, kind):
        # Staging has no workflow output. Validate every file, finish the
        # manifest, then publish its path/readiness together as the last step.
        H.require(kind in ["legacy", "evidence"] and kind not in self.published, "artifact already published")
        H.owned_dir(folder)
        files = sorted(folder.iterdir())
        H.require(files and all(p.name in allowed and p.name != "artifact-manifest.json"
                  and p.is_file() and not p.is_symlink() and p.stat().st_uid == os.geteuid() for p in files),
                  "artifact allowlist differs")
        for path in files:
            if path.name in LEGACY:
                H.require(kind == "legacy" and H.sha(path) == LEGACY[path.name]["sha256"],
                          "published legacy executable differs")
            else:
                value = path.read_text()
                H.require(not any(form in value for secret in self.secrets
                                  for form in [secret, json.dumps(secret)[1:-1], urllib.parse.quote(secret, safe="")]),
                          "credential in staged evidence; publication refused")
        H.write_json(folder / "artifact-manifest.json", {
            "schema": 1, "source_sha": self.source_sha, "run_id": self.run_id,
            "private_fixture_excluded": True,
            "files": {p.name: {"sha256": H.sha(p), "bytes": p.stat().st_size} for p in files},
        })
        descriptor = os.open(self.output_file, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "w") as stream:
            stream.write(kind + "_dir=" + str(folder) + "\n" + kind + "_ready=true\n")
        self.published.add(kind)

    def finalization_failure(self, exc):
        self.record_red(exc, "evidence finalization")
        # Never advertise failed staging even if it contains a written PASS.
        # A distinct minimal receipt carries the original RED when publishable.
        failure = self.root / "finalization-failure"
        failure.mkdir(mode=0o700)
        H.write_json(failure / "result.json", {
            "schema": 1, "status": "HOLD", "source_sha": self.source_sha, "run_id": self.run_id,
            "first_red": self.result["first_red"], "finalization_error_category": type(exc).__name__,
            "failed_staging_exported": False, "private_state_exported": False,
            "legacy_reference_published": "legacy" in self.published,
        })
        self.publish_artifacts(failure, {"result.json"}, "evidence")

    def finalize(self):
        # Each failed retirement is retained, not retried or escalated to force.
        if self.native is not None and not self.native_retirement_attempted:
            try:
                self.native_stop()
            except BaseException as exc:
                self.record_red(exc, "native retirement")
        if self.create_attempted and not self.retirement_attempted:
            try:
                self.retire_container()
            except BaseException as exc:
                self.record_red(exc, "container retirement")
        try:
            self.preserve_sources()
        except BaseException as exc:
            self.record_red(exc, "final immutable-source check")
        self.result.update(last_stage=self.stage, private_state_exported=False,
                           private_fixture="retained until ephemeral runner disposal",
                           verified_legacy_binaries=self.verified_binaries)
        H.require(self.result["status"] != PASS or (self.native is None and not self.create_attempted
                  and self.result["stages"] == PHASES and "first_red" not in self.result),
                  "PASS cannot coexist with incomplete phases or retirement")
        H.write_json(self.evidence / "phase-evidence.json", {"schema": 1, "phases": self.phases})
        H.write_json(self.evidence / "result.json", self.result)
        self.publish_artifacts(self.evidence, ARTIFACTS, "evidence")


def main():
    global H
    trial = None
    try:
        H = load_helpers()
        trial = Migration(preflight())
        def interrupted(signum, frame):
            raise H.Hold("controller interrupted; first outcome retained")
        signal.signal(signal.SIGTERM, interrupted)
        signal.signal(signal.SIGINT, interrupted)
        trial.import_image()
        trial.import_legacy()
        trial.seed_legacy()
        trial.snapshot()
        trial.first_silo()
        trial.upgrade_operations()
        trial.restart_silo()
        trial.restore_legacy()
        trial.finish()
    except BaseException as exc:
        if trial is None:
            sys.stderr.write("HOLD: migration preflight failed; private output withheld\n")
            return 1
        trial.record_red(exc)
    finally:
        if trial is not None:
            try:
                trial.finalize()
            except BaseException as exc:
                try:
                    trial.finalization_failure(exc)
                except BaseException:
                    trial.result["status"] = "HOLD"
                    sys.stderr.write("HOLD: evidence publication failed; private output withheld\n")
    if trial is not None:
        print(trial.result["status"], flush=True)
        return 0 if trial.result["status"] == PASS else 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())


