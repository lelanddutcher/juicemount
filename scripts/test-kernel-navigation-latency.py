#!/usr/bin/env python3
"""Owned Darwin kernel NFS latency acceptance; no live NAS or app required.

DEV-ONLY EXCEPTION to the H-3 rule that JuiceMount never unmounts by path.
This harness mounts a disposable loopback NFS fixture under its own scratch
directory with `sudo -n mount_nfs`, verifies the exact mount tuple it
created, and then removes only that mount with a normal (non-force)
`sudo -n umount`. It needs a passwordless sudo grant that already exists on
the developer's Mac and refuses to start without one (`sudo -n -l` check).
JuiceMount itself never needs, offers, or documents such a grant: do not add
a sudoers rule for it, and never point this harness at a real volume. The
product's production gate (test/perf/no_path_unmount_gate_test.go) excludes
this file as a dev-only harness.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import secrets
import socket
import stat
import subprocess
import sys
import threading
import time

PAYLOAD = b"slow-read-content"
FIXTURE_TEST = "TestOwnedKernelLatencyFixture"
PHASE_INODE_BASE = {"quiet": 3000, "held": 3413, "post": 3826}


def write_json(path, value):
    pending = path.with_suffix(path.suffix + ".pending")
    pending.write_text(json.dumps(value, indent=2) + "\n")
    os.replace(pending, path)


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def identity(path):
    s = os.lstat(path)
    if not stat.S_ISDIR(s.st_mode) or stat.S_ISLNK(s.st_mode):
        raise RuntimeError(f"not an owned plain directory: {path}")
    return {"device": s.st_dev, "inode": s.st_ino, "uid": s.st_uid,
            "mode": stat.S_IMODE(s.st_mode)}


def probe(phase, mount, trace=False):
    samples = []
    control = Control(json.loads(Path("state.json").read_text())["nonce"]) if trace else None
    for kind in ("attr", "lookup", "list"):
        for i in range(4):
            name = f"{kind}-{i}" + (".bin" if kind != "list" else "")
            path = mount / phase / name
            # Each seeded group has two files, one directory and 100 children.
            group_inode = PHASE_INODE_BASE[phase] + 103 * i
            before = control.call("status")["transport"] if control else None
            begin = time.perf_counter_ns()
            if kind == "attr":
                with open(path, "rb", buffering=0) as stream:
                    first = os.fstat(stream.fileno())
                    second = os.stat(path)
                if (first.st_ino != second.st_ino or first.st_size != 33 + i
                        or second.st_size != 33 + i or not stat.S_ISREG(first.st_mode)
                        or not stat.S_ISREG(second.st_mode) or first.st_ino != group_inode + 1):
                    raise RuntimeError(f"attribute payload mismatch: {name}")
                detail = {"inode": first.st_ino, "size": first.st_size}
            elif kind == "lookup":
                first = os.stat(path)
                second = os.stat(path)
                if (first.st_ino != second.st_ino or first.st_size != 65 + i
                        or second.st_size != 65 + i or not stat.S_ISREG(first.st_mode)
                        or not stat.S_ISREG(second.st_mode) or first.st_ino != group_inode + 2):
                    raise RuntimeError(f"lookup payload mismatch: {name}")
                detail = {"inode": first.st_ino, "size": first.st_size}
            else:
                listing = {}
                with os.scandir(path) as entries:
                    for entry in entries:
                        info = entry.stat(follow_symlinks=False)
                        inode = entry.inode()
                        if not stat.S_ISREG(info.st_mode) or info.st_ino != inode:
                            raise RuntimeError(f"directory child type/inode mismatch: {entry.name}")
                        listing[entry.name] = {"inode": inode, "size": info.st_size}
                wanted = {f"item-{j:03d}.bin": {"size": j + 1, "inode": group_inode + 4 + j}
                          for j in range(100)}
                inodes = {v["inode"] for v in listing.values()}
                if (listing != wanted
                        or len(inodes) != 100 or 0 in inodes):
                    raise RuntimeError(f"directory payload mismatch: {name}")
                detail = {"children": 100, "entries": listing}
            elapsed = time.perf_counter_ns() - begin
            after = control.call("status")["transport"] if control else None
            samples.append({"kind": kind, "path": f"{phase}/{name}",
                            "elapsed_ms": elapsed / 1e6, "finished_ns": time.time_ns(),
                            "transport_before": before, "transport_after": after, **detail})
    print(json.dumps({"samples": samples}), flush=True)


class Control:
    def __init__(self, nonce):
        self.nonce = nonce
        self.lock = threading.Lock()

    def call(self, op):
        # Relative socket path avoids Darwin's short sockaddr_un path limit.
        with self.lock, socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            conn.settimeout(45 if op == "stop" else 3)
            conn.connect("control.sock")
            conn.sendall((json.dumps({"nonce": self.nonce, "op": op}) + "\n").encode())
            data = b""
            while b"\n" not in data:
                part = conn.recv(65536)
                if not part:
                    raise RuntimeError("fixture closed control without a response")
                data += part
                if len(data) > 4 * 1024 * 1024:
                    raise RuntimeError("oversized fixture response")
        result = json.loads(data.split(b"\n", 1)[0])
        if result.get("nonce") != self.nonce:
            raise RuntimeError("fixture nonce mismatch")
        if not result.get("ok"):
            raise RuntimeError(result.get("error", "fixture refused request"))
        return result


class Run:
    def __init__(self, directory, state=None):
        self.directory = directory
        self.state = state or {}
        self.control = Control(self.state["nonce"])
        self.fixture = None
        self.children = []
        self.host_command = None
        self.heartbeat_stop = threading.Event()
        self.heartbeat_errors = []
        self.log_handles = []

    def save(self):
        write_json(self.directory / "state.json", self.state)

    def assert_parent(self):
        if identity(Path(".")) != self.state["directory_identity"]:
            raise RuntimeError("private run directory identity changed")

    def snapshot(self):
        result = self.control.call("status")
        if result.get("pid") != self.state.get("fixture_pid"):
            raise RuntimeError("fixture process identity changed")
        if self.state["export"] is None:
            export = result.get("export", "")
            if (len(export) != 36 or not export.startswith("/jm-")
                    or any(c not in "0123456789abcdef" for c in export[4:])
                    or export[4:] == "0" * 32):
                raise RuntimeError("fixture returned an invalid export identity")
            self.state["export"] = export
            self.state["source"] = "127.0.0.1:" + export
            self.save()
        if result.get("export") != self.state["export"]:
            raise RuntimeError("fixture export identity changed")
        if result.get("serve_error"):
            raise RuntimeError("fixture server failed: " + result["serve_error"])
        return result

    def start_heartbeat(self):
        def beat():
            while not self.heartbeat_stop.wait(2):
                try:
                    self.snapshot()
                except Exception as error:
                    self.heartbeat_errors.append(str(error))
        threading.Thread(target=beat, daemon=True).start()

    def command(self, name, argv, timeout):
        self.assert_parent()
        log = open(self.directory / (name + ".log"), "wb")
        self.log_handles.append(log)
        proc = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True)
        self.host_command = proc
        self.state["pending_host_command"] = {"name": name, "pid": proc.pid, "argv": argv}
        self.save()
        try:
            code = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            # Do not kill only sudo and leave an unobserved mount operation.
            raise RuntimeError(f"{name} command has not settled; retaining fixture and PID {proc.pid}")
        self.state["pending_host_command"] = None
        self.host_command = None
        self.save()
        if code:
            raise RuntimeError(f"{name} exited {code}; see {name}.log")

    def child(self, name, argv):
        log = open(self.directory / (name + ".json"), "wb")
        err = open(self.directory / (name + ".stderr"), "wb")
        self.log_handles.extend([log, err])
        proc = subprocess.Popen(argv, stdout=log, stderr=err, start_new_session=True)
        self.children.append(proc)
        self.state.setdefault("probe_pids", []).append(proc.pid)
        self.save()
        return proc

    def collect(self, proc, name, timeout=10):
        try:
            code = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"owned probe {name} did not settle")
        if code:
            raise RuntimeError(f"owned probe {name} exited {code}; see {name}.stderr")
        return json.loads((self.directory / (name + ".json")).read_text())

    def adopt_mount(self, status):
        mounts = status["mounts"]
        wanted = str(self.directory / "mount")
        if (len(mounts) != 1 or mounts[0]["on"] != wanted
                or mounts[0]["type"] != "nfs" or mounts[0]["from"] != self.state["source"]
                or not (mounts[0]["flags"] & 1)
                or len(mounts[0]["fsid"]) != 2 or mounts[0]["fsid"] == [0, 0]):
            raise RuntimeError("kernel mount identity/options differ from the owned fixture")
        if self.state.get("mount_identity") not in (None, mounts[0]):
            raise RuntimeError("owned mount was replaced")
        self.state["mount_identity"] = mounts[0]
        self.save()

    def cleanup(self):
        errors = []
        try:
            self.control.call("release")
        except Exception as error:
            return {"complete": False, "errors": ["gate release unproved: " + str(error)]}
        # Every descriptor must be closed before a normal unmount is attempted.
        for proc in self.children:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                errors.append(f"probe PID {proc.pid} still alive after gate release")
        pending = self.state.get("pending_host_command")
        if pending:
            if self.host_command is not None and self.host_command.poll() is not None:
                self.state["pending_host_command"] = None
                self.host_command = None
                self.save()
            else:
                errors.append(f"host command completion unproved: {pending}")
        if errors:
            return {"complete": False, "errors": errors}
        try:
            status = self.snapshot()
            self.assert_parent()
            if status["mounts"]:
                # Adoption after a failed mount command is permitted only after
                # that exact command settled and our empty-path preflight passed.
                if not self.state.get("mount_attempted"):
                    raise RuntimeError("unexpected mount before owned mount attempt")
                self.adopt_mount(status)
                if self.snapshot()["mounts"] != [self.state["mount_identity"]]:
                    raise RuntimeError("mount tuple changed before normal unmount")
                self.command("unmount", ["sudo", "-n", "/sbin/umount",
                                          str(self.directory / "mount")], 15)
            if self.snapshot()["mounts"]:
                raise RuntimeError("mount remains after normal unmount")
            result = self.control.call("stop")
            if not result.get("stopped"):
                raise RuntimeError("fixture did not acknowledge complete shutdown")
            if self.fixture is not None:
                if self.fixture.wait(timeout=8) != 0:
                    raise RuntimeError("fixture test exited unsuccessfully")
                log = (self.directory / "fixture.log").read_text()
                if (f"--- PASS: {FIXTURE_TEST}" not in log or "--- SKIP:" in log
                        or "--- FAIL:" in log):
                    raise RuntimeError("fixture test did not report exactly a passing invocation")
            return {"complete": True, "stopped": result}
        except Exception as error:
            return {"complete": False, "errors": [str(error)]}


def qualify_samples(result):
    samples = result["samples"]
    if len(samples) != 12 or {k: sum(x["kind"] == k for x in samples)
                              for k in ("attr", "lookup", "list")} != {"attr": 4, "lookup": 4, "list": 4}:
        raise RuntimeError("incomplete probe selection")
    durations = sorted(x["elapsed_ms"] for x in samples)
    result.update(sample_count=12, p50_ms=durations[5], p99_ms=durations[-1], max_ms=durations[-1])
    if result["p99_ms"] > 100 or result["max_ms"] > 250:
        raise RuntimeError(f"kernel navigation exceeded latency budget: max={durations[-1]:.3f}ms")


def qualify_pressure_trace(receipt, trace):
    """Prove actual expected requests arrived during eviction, on one clock."""
    pressure = receipt["pressure_after"]["pressure"]
    if (not pressure["completed"] or pressure["before"] <= pressure["budget"]
            or pressure["after"] != pressure["budget"] or pressure["evicted_children"] != 1200
            or pressure["missing_pins"] != 0
            or not 0 < pressure["started_ns"] < pressure["finished_ns"]):
        raise RuntimeError("cache pressure did not produce the exact residency/identity outcome")
    if trace.get("error") or trace["pending"]:
        raise RuntimeError("transport trace is incomplete")
    generation = bytes.fromhex(receipt["state"]["export"][4:])

    def matching(sample, procedure, expected_inode):
        before, after = sample["transport_before"], sample["transport_after"]
        if (before.get("error") or after.get("error")
                or before["start_wall_ns"] != trace["start_wall_ns"]
                or after["start_wall_ns"] != trace["start_wall_ns"]):
            raise RuntimeError("probe transport clock or trace changed")
        rows = []
        for row in trace["records"]:
            if row["program"] != 100003 or row["procedure"] != procedure:
                continue
            handle = bytes.fromhex(row.get("handle", ""))
            if (len(handle) == 28 and handle[:4] == b"\0\0\0\1"
                    and handle[4:20] == generation and int.from_bytes(handle[20:], "big") == expected_inode
                    and before["now_ns"] <= row["read_ns"] <= row["handler_ns"]
                    <= row["handled_ns"] <= row["write_ns"] <= after["now_ns"]
                    and row["handler_ns"] > 0 and row["nfs_ok"]):
                rows.append(row)
        return rows

    overlaps = {kind: [] for kind in ("attr", "lookup", "list")}
    for sample in receipt["held"]["samples"]:
        kind = sample["kind"]
        index = int(sample["path"].split("-")[-1].split(".")[0])
        group = PHASE_INODE_BASE["held"] + 103 * index
        procedure, inode = {"attr": (1, group+1), "lookup": (3, PHASE_INODE_BASE["held"]), "list": (17, group+3)}[kind]
        overlaps[kind].extend({"xid": r["xid"], "connection": r["connection"], "read_ns": r["read_ns"]}
                              for r in matching(sample, procedure, inode)
                              if pressure["started_ns"] <= r["read_ns"] < pressure["finished_ns"])
    if any(not rows for rows in overlaps.values()):
        raise RuntimeError("expected mounted GETATTR/LOOKUP/READDIRPLUS did not all overlap actual eviction")
    post_lists = []
    for sample in receipt["post_eviction"]["samples"]:
        if not sample["path"].startswith("post/"):
            raise RuntimeError("post-eviction probes reused a previously visited phase")
        if sample["kind"] != "list":
            continue
        index = int(sample["path"].split("-")[-1])
        rows = matching(sample, 17, PHASE_INODE_BASE["post"] + 103*index + 3)
        if not any(r["read_ns"] > pressure["finished_ns"] for r in rows):
            raise RuntimeError("post-eviction listing lacks its own successful READDIRPLUS")
        post_lists.append(sample["path"])
    if len(post_lists) != 4:
        raise RuntimeError("post-eviction listing coverage is incomplete")
    return {"overlap": overlaps, "post_eviction_lists": post_lists,
            "eviction_ms": (pressure["finished_ns"]-pressure["started_ns"])/1e6}


def execute(args):
    if platform.system() != "Darwin":
        raise RuntimeError("this acceptance requires macOS")
    if args.cache_pressure and args.control_only:
        raise RuntimeError("cache pressure acceptance requires a mounted backend read")
    directory = Path(args.out).absolute()
    if directory != directory.resolve() or directory.exists() or directory.is_symlink():
        raise RuntimeError("provide a fresh absolute path without symlink ancestors")
    directory.mkdir(mode=0o700)
    os.chmod(directory, 0o700)
    (directory / "mount").mkdir(mode=0o700)
    (directory / "tmp").mkdir(mode=0o700)
    root = Path(__file__).resolve().parent.parent
    nonce = secrets.token_hex(16)
    state = {"nonce": nonce, "export": None,
             "directory_identity": identity(directory),
             "mount_directory_identity": identity(directory / "mount"),
             "mount_attempted": False, "mount_identity": None,
             "created_ns": time.time_ns(), "source_root": str(root)}
    state["source"] = None
    run = Run(directory, state)
    run.save()
    receipt = {"verdict": "FAIL", "scope": "kernel-only; no Finder UI/WAN/packaged/H3 atomic detach claim",
               "mode": "control-only" if args.control_only else "kernel", "os": platform.platform(),
               "state": state, "started_ns": time.time_ns()}
    fixture_started = False
    try:
        names = subprocess.check_output(["git", "ls-files", "-z", "--cached", "--others",
                                         "--exclude-standard", "--", "*.go", "go.mod", "go.sum"], cwd=root).decode().split("\0")
        names = sorted({n for n in names if n and (root / n).is_file()})
        source_hashes = {n: digest(root / n) for n in names}
        write_json(directory / "source-sha256.json", source_hashes)
        receipt["head"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
        receipt["runner_sha256"] = digest(Path(__file__).resolve())
        env = dict({k: v for k, v in os.environ.items() if not k.startswith("JM_")},
                   TMPDIR=str(directory / "tmp") + "/", JM_SERVE_FROM_SQLITE="0",
                   JM_READDIR_PAGINATED="0", JM_FTS_DEFER="0", JM_KERNEL_LATENCY_DIR=str(directory),
                   JM_KERNEL_LATENCY_NONCE=nonce)
        if args.cache_pressure:
            env.update(JM_KERNEL_LATENCY_PRESSURE="1", JM_KERNEL_LATENCY_TRACE="1", JM_DURABLE_SERVE_FALLBACK="1")
        if args.binary:
            binary = Path(args.binary).resolve(strict=True)
            provenance = json.loads(Path(str(binary) + ".provenance.json").read_text())
            if (provenance["source_hashes"] != source_hashes
                    or provenance["binary_sha256"] != digest(binary)):
                raise RuntimeError("reused fixture binary does not match the source freeze")
        else:
            binary = directory / "kernel-fixture.test"
            command = ["go", "test", "-c", "-tags", "latency_acceptance", "./nfs", "-o", str(binary)]
            write_json(directory / "build-command.json", {"argv": command, "cwd": str(root)})
            with open(directory / "build.log", "wb") as log:
                subprocess.run(command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=300)
            write_json(Path(str(binary) + ".provenance.json"),
                       {"source_hashes": source_hashes, "binary_sha256": digest(binary), "build_argv": command})
        receipt["binary"] = str(binary)
        receipt["binary_sha256"] = digest(binary)
        manifest = subprocess.check_output([str(binary), "-test.list", "^" + FIXTURE_TEST + "$"], cwd=root / "nfs", env=env, text=True)
        (directory / "manifest.txt").write_text(manifest)
        if manifest.splitlines() != [FIXTURE_TEST]:
            raise RuntimeError("fixture test selection is not exact")
        os.chdir(directory)
        run.assert_parent()
        if not args.control_only:
            for name in ("mount_nfs", "umount"):
                run.command("permission-" + name, ["sudo", "-n", "-l", "/sbin/" + name], 5)
        command = [str(binary), "-test.run", "^" + FIXTURE_TEST + "$", "-test.v", "-test.count=1", "-test.timeout=0"]
        write_json(directory / "fixture-command.json", {"argv": command, "cwd": str(directory)})
        fixture_log = open(directory / "fixture.log", "wb")
        run.log_handles.append(fixture_log)
        run.fixture = subprocess.Popen(command, cwd=directory, env=env, stdin=subprocess.DEVNULL,
                                       stdout=fixture_log, stderr=subprocess.STDOUT, start_new_session=True)
        fixture_started = True
        state["fixture_pid"] = run.fixture.pid
        run.save()
        deadline = time.monotonic() + 15
        while True:
            if run.fixture.poll() is not None:
                raise RuntimeError("fixture exited before readiness")
            try:
                ready = run.snapshot()
                break
            except (OSError, RuntimeError):
                if time.monotonic() >= deadline:
                    raise RuntimeError("fixture readiness deadline expired")
                time.sleep(0.05)
        receipt["ready"] = ready
        if not args.control_only:
            run.start_heartbeat()
        if ready["mounts"]:
            raise RuntimeError("private fixture path/export is already mounted")
        if identity(directory / "mount") != state["mount_directory_identity"] or any((directory / "mount").iterdir()):
            raise RuntimeError("fresh empty mount directory changed")
        if args.control_only:
            try:
                Control("1" * 32 if nonce == "0" * 32 else "0" * 32).call("status")
            except RuntimeError:
                receipt["wrong_nonce_rejected"] = True
            else:
                raise RuntimeError("fixture accepted an unauthorized control nonce")
            run.snapshot()  # Refusal must leave the real control channel usable.
            receipt["armed"] = run.control.call("arm")
            # No control requests during this interval: valid requests refresh
            # heartbeat. Check the watchdog's actual state after its deadline.
            time.sleep(12)
            receipt["watchdog"] = run.snapshot()
            receipt["released"] = run.control.call("release")
            if (not receipt["armed"]["gate"]["armed"]
                    or not receipt["watchdog"]["gate"]["watchdog_released"]
                    or not receipt["released"]["gate"]["released"]):
                raise RuntimeError("control/watchdog transition not acknowledged")
        else:
            host, port = ready["addr"].rsplit(":", 1)
            if host != "127.0.0.1" or not (0 < int(port) < 65536):
                raise RuntimeError("fixture did not bind an ephemeral loopback listener")
            options = f"port={port},mountport={port},vers=3,tcp,ro,nobrowse,locallocks,rdirplus,actimeo=0,soft,intr,timeo=10,retrans=2,deadtimeout=0"
            receipt["mount_options"] = options
            state["mount_attempted"] = True
            run.save()
            run.command("mount", ["sudo", "-n", "/sbin/mount_nfs", "-o", options,
                                  state["source"], str(directory / "mount")], 20)
            run.adopt_mount(run.snapshot())
            runner = str(Path(__file__).resolve())
            argv = [sys.executable, runner, "--probe", "quiet", "--mount", str(directory / "mount")]
            if args.cache_pressure:
                argv.append("--cache-pressure")
            quiet = run.collect(run.child("quiet", argv), "quiet")
            receipt["quiet"] = quiet
            qualify_samples(quiet)
            run.control.call("arm")
            slow = run.child("slow", [sys.executable, runner, "--read", str(directory / "mount" / "slow.bin")])
            deadline = time.monotonic() + 8
            while True:
                before = run.snapshot()
                if before["gate"]["admissions"] > 0:
                    break
                if time.monotonic() >= deadline or slow.poll() is not None:
                    raise RuntimeError("kernel read never entered the held backend gate")
                time.sleep(0.01)
            if before["gate"]["released"]:
                raise RuntimeError("backend gate released before probes")
            receipt["held_before"] = before
            if args.cache_pressure:
                receipt["pressure_start"] = run.control.call("evict")
            held_argv = [sys.executable, runner, "--probe", "held", "--mount", str(directory / "mount")]
            if args.cache_pressure:
                held_argv.append("--cache-pressure")
            held = run.collect(run.child("held", held_argv), "held")
            receipt["held"] = held
            after = run.snapshot()
            receipt["held_after"] = after
            if after["gate"]["released"] or after["gate"]["watchdog_released"]:
                raise RuntimeError("backend hold did not span all metadata probes")
            deltas = {p: after["rpc"].get(p, 0) - before["rpc"].get(p, 0) for p in ("1", "3", "17")}
            receipt["held_rpc_deltas"] = deltas
            if any(n <= 0 for n in deltas.values()):
                raise RuntimeError("kernel cache/RPC coverage incomplete: " + str(deltas))
            receipt["readdir_fallback_delta"] = after["rpc"].get("16", 0) - before["rpc"].get("16", 0)
            if receipt["readdir_fallback_delta"] != 0:
                raise RuntimeError("kernel substituted READDIR for required READDIRPLUS")
            qualify_samples(held)
            if args.cache_pressure:
                deadline = time.monotonic() + 10
                while True:
                    status = run.snapshot()
                    if status["pressure"]["completed"]:
                        receipt["pressure_after"] = status
                        break
                    if time.monotonic() >= deadline:
                        raise RuntimeError("production cache eviction did not finish")
                    time.sleep(0.01)
                receipt["post_eviction_before"] = run.snapshot()
                post_argv = [sys.executable, runner, "--probe", "post", "--mount", str(directory / "mount"), "--cache-pressure"]
                post = run.collect(run.child("post-eviction", post_argv), "post-eviction")
                receipt["post_eviction"] = post
                receipt["post_eviction_after"] = run.snapshot()
                if receipt["post_eviction_after"]["gate"]["released"]:
                    raise RuntimeError("backend hold did not span post-eviction probes")
                qualify_samples(post)
            released = run.control.call("release")
            receipt["release"] = released
            if any(s["finished_ns"] >= released["gate"]["release_ns"] for s in held["samples"]):
                raise RuntimeError("metadata completion did not precede backend release")
            if args.cache_pressure and any(s["finished_ns"] >= released["gate"]["release_ns"] for s in post["samples"]):
                raise RuntimeError("post-eviction metadata completion did not precede backend release")
            receipt["slow"] = run.collect(slow, "slow")
            if receipt["slow"] != {"hex": PAYLOAD.hex(), "size": len(PAYLOAD)}:
                raise RuntimeError("kernel slow-read bytes differ")
        if run.heartbeat_errors:
            raise RuntimeError("fixture heartbeat was unreliable: " + repr(run.heartbeat_errors))
        receipt["source_drift"] = [n for n, h in source_hashes.items() if digest(root / n) != h]
        if receipt["source_drift"]:
            raise RuntimeError("source changed during qualification")
        receipt["verdict"] = "CONTROL_ONLY_PASS" if args.control_only else "PASS"
    except Exception as error:
        receipt["error"] = f"{type(error).__name__}: {error}"
    finally:
        if (fixture_started and run.fixture.poll() is not None
                and not state.get("mount_attempted") and not state.get("pending_host_command")):
            cleanup = {"complete": True, "fixture_exited_before_any_mount_attempt": True}
        elif fixture_started:
            cleanup = run.cleanup()
        else:
            cleanup = {"complete": not state.get("pending_host_command"), "fixture_not_started": True}
        receipt["cleanup"] = cleanup
        if not cleanup["complete"]:
            receipt["verdict"] = "INCOMPLETE"
            receipt["recovery"] = "Fixture/data retained. Inspect state.json and logs; release via control socket. Recheck exact mount tuple before normal unmount. Never force-unmount or delete this directory."
        if args.cache_pressure and cleanup["complete"] and receipt["verdict"] == "PASS":
            try:
                receipt["pressure_proof"] = qualify_pressure_trace(receipt, json.loads((directory / "transport-trace.json").read_text()))
            except Exception as error:
                receipt["verdict"] = "FAIL"
                receipt["error"] = f"pressure trace qualification: {type(error).__name__}: {error}"
        run.heartbeat_stop.set()
        receipt["finished_ns"] = time.time_ns()
        write_json(directory / "receipt.json", receipt)
        for log in run.log_handles:
            log.close()
    print(json.dumps({"verdict": receipt["verdict"], "receipt": str(directory / "receipt.json"),
                      "error": receipt.get("error"), "cleanup_complete": cleanup["complete"]}), flush=True)
    return 0 if receipt["verdict"] in ("PASS", "CONTROL_ONLY_PASS") else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("out", nargs="?")
    parser.add_argument("--binary", help="reuse an explicitly reviewed fixture binary")
    parser.add_argument("--control-only", action="store_true", help="exercise fixture lifecycle without a mount; never reports kernel PASS")
    parser.add_argument("--cache-pressure", action="store_true", help="require real default-cache eviction overlapping mounted requests")
    parser.add_argument("--probe", choices=("quiet", "held", "post"), help=argparse.SUPPRESS)
    parser.add_argument("--mount", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--read", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.probe:
        probe(args.probe, args.mount, args.cache_pressure)
        return 0
    if args.read:
        data = args.read.read_bytes()
        print(json.dumps({"hex": data.hex(), "size": len(data)}), flush=True)
        return 0
    if not args.out:
        parser.error("a fresh evidence directory is required")
    return execute(args)


if __name__ == "__main__":
    sys.exit(main())
