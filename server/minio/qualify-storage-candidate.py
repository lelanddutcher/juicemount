#!/usr/bin/env python3
"""Hash-bound import and container-custody helpers for native ARM64 migration.

The migration controller owns admission, provenance and all eleven phases.
Direct execution is refused; retained compatibility methods are never called.
Only the original qualified image artifact is imported, without rebuild or scan.
"""
import sys

# This guard precedes imports with any possibility of creating cache files.
if sys.flags.optimize:
    sys.stderr.write("HOLD: optimized Python is not admitted\n")
    raise SystemExit(2)

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


PARENT = "410b49ec153f81354728f41c81c80ff676df6c50"
PARENT_TREE = "c408e101642d615de471c305a4752070df76f8e4"
BRANCH = "refs/heads/codex/0.5-storage-trial-410b49e"
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
        "binary_sha256": "a376e0bf9cbf74644fd21cdd68194a64cd042d72df45be5b8935ca3c1bff44a8",
        "binary_bytes": 41005853,
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
        "binary_sha256": "99cec0faf03e826bdcdca0badbf62280d15039d7ba851561487857c388d98b26",
        "binary_bytes": 127733479,
    },
}
IMAGE_ID = "sha256:754c7ed1ea9af679e2a5325b566ca6a03235139ea285bd495c9c352fe2bb7a9f"
IMAGE_SOURCE = "6ce4e427f12f938ef69001cd25801f2d3f493f3f"  # Original image build, not migration trial.
IMPORT_RUN = "38241710613"
IMPORT_ARTIFACT = "storage-candidate-arm64-38241710613-1"
IMPORT_DIR = "storage-candidate-arm64-import-38241710613-1"
IMPORT_MANIFEST_SHA = "c14164e6b5877476dfb168d1c8e79968cd4636a5fb1e1a5c4bb22d86b660bb92"
IMPORT_METADATA_SHA = "15e12779ff197b3892ef23afc447c4768dcd65517e7ce9e65ab827f2064abb91"
ARCHIVE_SHA = "1e53bdaa320901532c5f13f75467fc2310419c194d70a4b6652c0d991189f9d3"
ARCHIVE_BYTES = 126502689
IMPORT_FILES = {
    "candidate-Dockerfile",
    "candidate-image.json",
    "candidate-image.tar.gz",
    "compatibility-buildinfo.txt",
    "compatibility-result.json",
    "compatibility-status.txt",
    "mcli-binary-govulncheck.jsonl",
    "mcli-binary-govulncheck.stderr",
    "mcli-buildinfo.txt",
    "mcli-graph-summary.json",
    "mcli-module.patch",
    "mcli-modules.jsonl",
    "mcli-modules.stderr",
    "mcli-packages.jsonl",
    "mcli-packages.stderr",
    "mcli-source-govulncheck.jsonl",
    "mcli-source-govulncheck.stderr",
    "native-preflight.json",
    "provenance.json",
    "result.json",
    "scan-summary.json",
    "scanner-buildinfo.txt",
    "scanner-module.json",
    "scanner-provenance.json",
    "silo-binary-govulncheck.jsonl",
    "silo-binary-govulncheck.stderr",
    "silo-buildinfo.txt",
    "silo-graph-summary.json",
    "silo-module.patch",
    "silo-modules.jsonl",
    "silo-modules.stderr",
    "silo-packages.jsonl",
    "silo-packages.stderr",
    "silo-source-govulncheck.jsonl",
    "silo-source-govulncheck.stderr",
}
ARTIFACTS = {
    "result.json", "provenance.json", "candidate-image-reference.json",
    "compatibility-buildinfo.txt", "compatibility-result.json",
    "compatibility-status.txt", "artifact-manifest.json",
}
LABEL = "org.juicemount.candidate-fixture"
PASS = "PASS_ORDINARY_COMPATIBILITY_SECURITY_AND_MIGRATION_OPEN"


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


def require_arm64_elf(path):
    require(path.is_file() and not path.is_symlink() and path.stat().st_uid == os.geteuid(),
            "native binary must be an owned regular file")
    with path.open("rb") as stream:
        header = stream.read(64)
    require(len(header) == 64 and header[:7] == b"\x7fELF\x02\x01\x01"
            and int.from_bytes(header[16:18], "little") in [2, 3]
            and int.from_bytes(header[18:20], "little") == 183
            and int.from_bytes(header[20:24], "little") == 1,
            "native binary is not ELF64 little-endian AArch64")


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


def preflight():
    require(len(sys.argv) == 1, "runner takes no arguments")
    require(sys.platform == "linux" and platform.machine() == "x86_64", "Linux amd64 required")
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("RUNNER_OS") == "Linux"
            and os.environ.get("RUNNER_ARCH") == "X64", "GitHub-hosted Linux x64 context required")
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
    env.update(GOTOOLCHAIN="local", GOWORK="off", GOENV="off")
    require(command(["go", "version"], env)[1].decode().strip() == "go version go1.27.2 linux/amd64",
            "exact Go 1.27.2 linux/amd64 toolchain required")
    runner_temp = Path(os.environ["RUNNER_TEMP"])
    require(runner_temp.is_absolute() and runner_temp.resolve(strict=True) == runner_temp,
            "canonical RUNNER_TEMP required")
    require(runner_temp.is_dir() and runner_temp.stat().st_uid == os.geteuid(), "RUNNER_TEMP owner differs")
    require(shutil.disk_usage(runner_temp).free >= 12 * 1024**3, "at least 12 GiB free required")
    output_file = Path(os.environ["GITHUB_OUTPUT"])
    output_info = output_file.lstat()
    require(stat.S_ISREG(output_info.st_mode) and output_info.st_uid == os.geteuid()
            and not output_file.is_symlink(), "owned GITHUB_OUTPUT required")
    return repo, env, runner_temp, output_file, run_id, source_sha


class Qualification:
    def __init__(self, admission):
        self.repo, self.env, temp, output_file, self.run_id, self.source_sha = admission
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
                        GOOS="linux", GOARCH="amd64", GOPROXY="https://proxy.golang.org",
                        GOSUMDB="sum.golang.org", DOCKER_CONFIG=str(self.private / "docker"))
        self.stage = "admitted"
        self.result = {"schema": 1, "status": "HOLD", "source_sha": self.source_sha,
                       "parent": PARENT, "run_id": self.run_id, "stages": [],
                       "limits": ["ordinary compatibility only", "security acceptance open",
                                  "fresh-install acceptance open", "migration and restore open",
                                  "arm64 acceptance open", "mounted/provider acceptance open",
                                  "no registry publication"],
                       "container_retirement": [], "private_fixture": "not created"}
        self.image_id = None
        self.container = None
        self.container_name = None
        self.owner = secrets.token_hex(32)
        self.create_attempted = False
        self.secrets = []
        descriptor = os.open(output_file, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "w") as output:
            output.write("evidence_dir=" + str(self.evidence) + "\n")
        write_json(self.evidence / "provenance.json", {
            "source_sha": self.source_sha, "parent": PARENT, "parent_tree": PARENT_TREE,
            "diagnostic_paths": {p: sha(self.repo / p) for p in CHANGED_PATHS},
            "compatibility_source_sha256": TEST_SOURCE_SHA, "prior_reviewed_build_identities": IDENTITIES,
            "base": BASE, "base_config_id": BASE_ID, "toolchain": "go1.27.2 linux/amd64",
            "trial_scope": "import original archive; no binary/image rebuild, pull, or rescan",
            "image_source_sha": IMAGE_SOURCE, "original_artifact_run_id": IMPORT_RUN,
            "original_artifact_name": IMPORT_ARTIFACT,
            "build_environment": {k: self.env[k] for k in ["CGO_ENABLED", "GOOS", "GOARCH", "GOTOOLCHAIN", "GOWORK", "GOENV"]},
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

    def import_image(self):
        self.step("verify original immutable artifact before image load")
        # The isolated environment admits only the local default Docker socket.
        native = self.call(["docker", "version", "--format", "{{.Server.Os}}/{{.Server.Arch}}"])[1].strip()
        require(native == b"linux/arm64", "native Linux ARM64 Docker daemon required")
        self.phases[self.stage]["docker_server_platform"] = "linux/arm64"
        imported = Path(os.environ["RUNNER_TEMP"]) / IMPORT_DIR
        require(imported.resolve(strict=True) == imported and imported.is_dir()
                and imported.stat().st_uid == os.geteuid(), "import directory identity differs")
        manifest_file = imported / "artifact-manifest.json"
        require(manifest_file.is_file() and not manifest_file.is_symlink()
                and sha(manifest_file) == IMPORT_MANIFEST_SHA, "original artifact manifest differs")
        manifest = json.loads(manifest_file.read_text())
        require(manifest.get("schema") == 1 and manifest.get("source_sha") == IMAGE_SOURCE
                and manifest.get("private_fixture_excluded") is True and manifest.get("platform") == "linux/arm64"
                and set(manifest.get("files", {})) == IMPORT_FILES, "original manifest identity/files differ")
        require({p.name for p in imported.iterdir()} == IMPORT_FILES | {"artifact-manifest.json"},
                "unexpected imported artifact member")
        for name, identity in manifest["files"].items():
            path = imported / name
            require(path.is_file() and not path.is_symlink() and path.resolve(strict=True) == path
                    and path.stat().st_uid == os.geteuid()
                    and path.stat().st_size == identity["bytes"] and sha(path) == identity["sha256"],
                    "imported artifact member bytes differ")
        metadata_file = imported / "candidate-image.json"
        require(sha(metadata_file) == IMPORT_METADATA_SHA, "original image metadata differs")
        metadata = json.loads(metadata_file.read_text())
        archive = imported / "candidate-image.tar.gz"
        require(archive.stat().st_size == ARCHIVE_BYTES and sha(archive) == ARCHIVE_SHA,
                "original image archive differs")
        require(metadata.get("archive_sha256") == ARCHIVE_SHA and metadata.get("archive_bytes") == ARCHIVE_BYTES
                and metadata.get("config_id") == IMAGE_ID and metadata.get("source_sha") == IMAGE_SOURCE
                and metadata.get("base") == BASE and metadata.get("base_config_id") == BASE_ID
                and metadata.get("platform") == "linux/arm64"
                and metadata.get("extracted_binary_sha256") == {n: IDENTITIES[n]["binary_sha256"] for n in IDENTITIES},
                "original image metadata identity differs")
        prior = json.loads((imported / "provenance.json").read_text())
        prior_result = json.loads((imported / "result.json").read_text())
        require(prior.get("source_sha") == IMAGE_SOURCE and prior.get("compatibility_source_sha256") == TEST_SOURCE_SHA
                and prior_result.get("source_sha") == IMAGE_SOURCE and prior_result.get("run_id") == IMPORT_RUN,
                "original source/run identity differs")
        for name, identity in IDENTITIES.items():
            require(prior_result.get("built_binaries", {}).get(name) ==
                    {"sha256": identity["binary_sha256"], "bytes": identity["binary_bytes"]},
                    "original binary receipt differs")
            require(sha(imported / (name + "-module.patch")) == identity["patch_sha256"],
                    "original module patch differs")
        reference = {"schema": 1, "repository": "lelanddutcher/juicemount",
                     "original_run_id": IMPORT_RUN, "original_attempt": 1,
                     "original_artifact_name": IMPORT_ARTIFACT, "original_artifact_id": 11743580544,
                     "original_artifact_zip_sha256_reference": "9417e5d8323f3d456972bd4947bac0d97de1b030100e698b722df27f20d4c742",
                     "original_manifest_sha256": IMPORT_MANIFEST_SHA,
                     "original_image_metadata_sha256": IMPORT_METADATA_SHA,
                     "archive_sha256": ARCHIVE_SHA, "archive_bytes": ARCHIVE_BYTES, "config_id": IMAGE_ID,
                     "image_source_sha": IMAGE_SOURCE, "trial_source_sha": self.source_sha,
                     "archive_custody": "original artifact referenced; bytes verified and not modified or re-uploaded",
                     "qualification_receipt": "result.json", "security_and_migration": "OPEN"}
        write_json(self.evidence / "candidate-image-reference.json", reference)
        self.result["candidate_image"] = reference
        self.passed()
        self.step("load verified archive and extract packaged binaries without starting")
        self.call(["docker", "image", "load", "--input", archive], timeout=180)
        self.image_id = IMAGE_ID
        candidate = self.docker_json("image", "inspect", self.image_id)[0]
        require(candidate.get("Id") == self.image_id and candidate.get("Os") == "linux"
                and candidate.get("Architecture") == "arm64", "imported candidate config/platform differs")
        config = candidate.get("Config", {})
        require(config.get("Env") == BASE_ENV and config.get("Entrypoint") == ["/usr/bin/docker-entrypoint.sh"],
                "imported candidate runtime defaults differ")
        labels = config.get("Labels", {})
        require(labels.get("org.juicemount.candidate.source") == IMAGE_SOURCE
                and labels.get("org.juicemount.candidate.silo-sha256") == IDENTITIES["silo"]["binary_sha256"]
                and labels.get("org.juicemount.candidate.mcli-sha256") == IDENTITIES["mcli"]["binary_sha256"],
                "imported candidate provenance labels differ")
        self.create_container("extract")
        for name in ["silo", "mcli"]:
            self.verify_container(running=False)
            extracted = self.private / "extract" / name
            self.call(["docker", "cp", self.container + ":/usr/bin/" + name, extracted])
            require(extracted.is_file() and not extracted.is_symlink()
                    and extracted.stat().st_size == IDENTITIES[name]["binary_bytes"]
                    and sha(extracted) == IDENTITIES[name]["binary_sha256"],
                    "image binary differs from scanned build")
            require_arm64_elf(extracted)
        link = self.private / "extract/mc"
        self.call(["docker", "cp", self.container + ":/usr/bin/mc", link])
        require(link.is_symlink() and os.readlink(link) == "mcli", "packaged mc symlink differs")
        followed = self.private / "extract/mc-followed"
        self.call(["docker", "cp", "-L", self.container + ":/usr/bin/mc", followed])
        require(sha(followed) == IDENTITIES["mcli"]["binary_sha256"], "packaged mc target differs")
        self.retire_container()
        self.passed()

    def compile_test(self):
        self.step("compile reviewed bundled JuiceFS object test")
        self.test_binary = self.private / "storage-compatibility.test"
        self.call(["go", "test", "-c", "-mod=readonly", "-tags", "storageacceptance", "-o", self.test_binary,
                   "./server/minio"], cwd=self.repo, timeout=600)
        require(not git(self.repo, self.env, "status", "--porcelain", "--untracked-files=all"), "test compile changed checkout")
        info = self.call(["go", "version", "-m", self.test_binary])[1].decode()
        require(info.splitlines()[0] == str(self.test_binary) + ": go1.27.2", "test binary toolchain differs")
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
        write_json(self.evidence / "result.json", self.result)
        existing = list(self.evidence.iterdir())
        require(all(p.name in ARTIFACTS and p.is_file() and not p.is_symlink() for p in existing),
                "artifact allowlist violated")
        write_json(self.evidence / "artifact-manifest.json", {
            "schema": 1, "source_sha": self.source_sha,
            "files": {p.name: {"sha256": sha(p), "bytes": p.stat().st_size} for p in sorted(existing)},
            "private_fixture_excluded": True,
        })


def main():
    sys.stderr.write("HOLD: migration custody helper is import-only\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
