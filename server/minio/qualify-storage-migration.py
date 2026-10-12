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

PARENT = "e5887b772b7014032e983b74ac95a94c7c59af69"
PARENT_TREE = "fe1eb5e6fec881d8bbc37f965e30e4a7afc70ae4"
BRANCH = "refs/heads/codex/0.5-storage-migration-e5887b7"
HELPER_SHA = "fd21c2a9174eac69dcd29ef26cdcd2a17ade3fe63249c70a47e8964e493e53c4"
PATHS = [".github/workflows/ci.yml", "server/minio/qualify-storage-migration.py"]
URL = "http://127.0.0.1:19000"
ACTIVE = "mac-0123456789ab-1"
PRE_REVOKED = "mac-0123456789ac-1"
PASS = "PASS_STOPPED_SINGLE_NODE_MIGRATION_RESTORE_SECURITY_AND_MOUNTED_ACCEPTANCE_OPEN"
PHASES = [
    "verify original immutable artifact before image load",
    "load verified archive and extract packaged binaries without starting",
    "rebuild and retain exact legacy binaries",
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
BUILD_ARTIFACTS = {"minio", "mc", "minio-buildinfo.txt", "mc-buildinfo.txt",
                   "provenance.json", "artifact-manifest.json"}
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
    env.update(GOTOOLCHAIN="local", GOWORK="off", GOENV="off")
    H.require(H.command(["go", "version"], env)[1].decode().strip() == "go version go1.26.6 linux/amd64",
              "exact legacy Go1.26.6 required")
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
        for folder in [self.private, self.evidence, self.build_inputs]:
            folder.mkdir(mode=0o700)
        for n in ["tmp", "gotmp", "gopath", "gocache", "docker", "data", "control", "extract",
                  "sources", "built", "runtime", "snapshots", "seed-inputs", "probes"]:
            (self.private / n).mkdir(mode=0o700)
        self.env.update(TMPDIR=str(self.private / "tmp"), GOTMPDIR=str(self.private / "gotmp"),
                        GOPATH=str(self.private / "gopath"), GOCACHE=str(self.private / "gocache"),
                        GOTOOLCHAIN="local", GOWORK="off", GOENV="off", CGO_ENABLED="0",
                        GOOS="linux", GOARCH="amd64", GOAMD64="v1", GOPROXY="https://proxy.golang.org",
                        GOSUMDB="sum.golang.org", DOCKER_CONFIG=str(self.private / "docker"))
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

    def build_legacy(self):
        self.step(PHASES[2])
        for name, identity in LEGACY.items():
            source = self.private / "sources" / name
            source.mkdir(mode=0o700)
            H.git(source, self.env, "init", "--quiet")
            H.git(source, self.env, "remote", "add", "origin", identity["url"])
            self.call(["git", "-c", "credential.helper=", "-C", source, "fetch", "--depth=1",
                       "origin", identity["revision"]], timeout=180)
            H.git(source, self.env, "checkout", "--quiet", "--detach", "FETCH_HEAD")
            H.require(H.git(source, self.env, "rev-parse", "HEAD") == identity["revision"]
                      and H.git(source, self.env, "remote", "get-url", "origin") == identity["url"]
                      and not H.git(source, self.env, "status", "--porcelain", "--untracked-files=all"),
                      "legacy source identity differs")
            for filename in ["go.mod", "go.sum"]:
                path = source / filename
                H.require(path.is_file() and not path.is_symlink() and H.sha(path) == identity[filename],
                          "legacy module input differs")
            binary = self.private / "built" / name
            self.call(["go", "build", "-mod=readonly", "-trimpath", "-buildvcs=true",
                       "-o", binary, "."], cwd=source, timeout=900)
            H.require(not H.git(source, self.env, "status", "--porcelain", "--untracked-files=all"),
                      "legacy build changed source")
            H.require(binary.is_file() and not binary.is_symlink() and H.sha(binary) == identity["sha256"],
                      "legacy binary differs from prior receipt; no further build or runtime")
            info = self.call(["go", "version", "-m", binary])[1].decode()
            lines = info.splitlines()
            H.require(lines[0] == str(binary) + ": go1.26.6", "legacy binary toolchain differs")
            fields = {}
            for line in lines[1:]:
                parts = line.strip().split("\t")
                if len(parts) >= 2 and parts[0] == "build" and "=" in parts[1]:
                    key, value = parts[1].split("=", 1)
                    fields[key] = value
            for key, value in {"CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": "amd64",
                               "GOAMD64": "v1", "vcs": "git", "vcs.revision": identity["revision"],
                               "vcs.modified": "false", "-trimpath": "true"}.items():
                H.require(fields.get(key) == value, "legacy buildinfo setting differs")
            H.require(any(line.strip().split("\t")[:2] == ["path", identity["module"]] for line in lines),
                      "legacy module path differs")
            shutil.copy2(binary, self.build_inputs / name)
            H.write_new(self.build_inputs / (name + "-buildinfo.txt"), info.replace(str(binary), "<" + name + ">"))
            self.verified_binaries[name] = {
                "sha256": H.sha(binary), "bytes": binary.stat().st_size, "revision": identity["revision"],
                "tree": H.git(source, self.env, "rev-parse", "HEAD^{tree}"), "toolchain": "go1.26.6",
                "vcs_modified": False,
            }
        H.write_json(self.build_inputs / "provenance.json", {
            "schema": 1, "source_sha": self.source_sha, "run_id": self.run_id,
            "purpose": "verified legacy executables; independent of migration outcome",
            "status": "VERIFIED_BUILD_INPUTS", "binaries": self.verified_binaries,
            "build_command": "go build -mod=readonly -trimpath -buildvcs=true -o <binary> .",
            "environment": {"CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": "amd64", "GOAMD64": "v1"},
        })
        self.result["legacy_binaries"] = self.verified_binaries
        self.phases[self.stage]["verified_binary_names"] = sorted(self.verified_binaries)
        # Publish verified build custody before creating any runtime credential.
        self.publish_artifacts(self.build_inputs, BUILD_ARTIFACTS, "legacy")
        self.passed()

    def ports_free(self):
        for port in [19000, 19001]:
            with socket.socket() as probe:
                try:
                    probe.bind(("127.0.0.1", port))
                except OSError:
                    raise H.Hold("fixture loopback port occupied; foreign listener not touched") from None

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
        self.native_log.close()
        self.native = self.native_log = None
        self.ports_free()
        receipt["status"] = "JOINED_ZERO"

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
            "legacy_inputs_published": "legacy" in self.published,
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
        trial.build_legacy()
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

