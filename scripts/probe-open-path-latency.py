#!/usr/bin/env python3
"""Opt-in, read-only absolute-open versus retained-parent-openat diagnostic.

This observes one already-selected file. It never mounts, reads file content, or
claims a cold cache: each sample is labeled by order and cache reset is false.
The supervisor kills the worker at an absolute deadline, including a stuck open.
"""

import argparse
import hashlib
import json
import os
import signal
import stat
import statistics
import subprocess
import sys
import time


MAX_PAIRS = 8
MAX_DEADLINE_SECONDS = 60
OPEN_FLAGS = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
DIR_FLAGS = OPEN_FLAGS | os.O_DIRECTORY


def file_identity(info):
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns]


def parent_identity(info):
    return [info.st_dev, info.st_ino, stat.S_IFMT(info.st_mode)]


def check_parent(path, fd, expected):
    retained = parent_identity(os.fstat(fd))
    current = parent_identity(os.lstat(path))
    if retained != expected or current != expected or expected[2] != stat.S_IFDIR:
        raise RuntimeError("retained parent FD or pathname identity changed")


def identity_preflight(args):
    result = {"schema": 1, "verdict": "FAIL", "path": args.path,
              "scope": "read-only descriptor identity preflight; warms unknown cache state"}
    parent_fd = file_fd = None
    try:
        parent, name = os.path.split(args.path)
        started = time.perf_counter_ns()
        parent_fd = os.open(parent, DIR_FLAGS)
        result["parent_open_ms"] = (time.perf_counter_ns() - started) / 1e6
        expected_parent = parent_identity(os.fstat(parent_fd))
        check_parent(parent, parent_fd, expected_parent)
        started = time.perf_counter_ns()
        file_fd = os.open(name, OPEN_FLAGS, dir_fd=parent_fd)
        result["file_open_ms"] = (time.perf_counter_ns() - started) / 1e6
        info = os.fstat(file_fd)
        expected_file = file_identity(info)
        if not stat.S_ISREG(info.st_mode) or file_identity(os.lstat(args.path)) != expected_file:
            raise RuntimeError("file pathname and descriptor identity differ")
        check_parent(parent, parent_fd, expected_parent)
        result["expected_file"] = expected_file
        result["expected_parent"] = expected_parent
        result["verdict"] = "IDENTITY"
    except Exception as error:
        result["error"] = f"{type(error).__name__}: {error}"
    finally:
        if file_fd is not None:
            os.close(file_fd)
        if parent_fd is not None:
            os.close(parent_fd)
        print(json.dumps(result, separators=(",", ":")), flush=True)
    return 0 if result["verdict"] == "IDENTITY" else 2


def worker(args):
    result = {"schema": 1, "verdict": "FAIL", "path": args.path,
              "cache_reset": False, "pairs": args.pairs, "samples": [],
              "expected_file": args.expected_file,
              "expected_parent": args.expected_parent}
    parent_fd = None
    try:
        parent, name = os.path.split(args.path)
        if not name or name in (".", ".."):
            raise RuntimeError("path must name one file")
        started = time.perf_counter_ns()
        parent_fd = os.open(parent, DIR_FLAGS)
        result["parent_open_ms"] = (time.perf_counter_ns() - started) / 1e6
        started = time.perf_counter_ns()
        check_parent(parent, parent_fd, args.expected_parent)
        result["initial_parent_check_ms"] = (time.perf_counter_ns() - started) / 1e6
        for pair in range(args.pairs):
            # AB/BA alternation gives each method the first and second slot.
            order = ("absolute", "relative") if pair % 2 == 0 else ("relative", "absolute")
            for slot, method in enumerate(order):
                row = {"pair": pair, "slot": slot, "method": method,
                       "sequence": len(result["samples"]),
                       "cache_label": "first_observed_cache_unknown" if not result["samples"] else "subsequent_cache_unknown"}
                started = time.perf_counter_ns()
                check_parent(parent, parent_fd, args.expected_parent)
                row["parent_check_before_ms"] = (time.perf_counter_ns() - started) / 1e6
                fd = None
                started = time.perf_counter_ns()
                try:
                    if method == "absolute":
                        fd = os.open(args.path, OPEN_FLAGS)
                    else:
                        fd = os.open(name, OPEN_FLAGS, dir_fd=parent_fd)
                except OSError as error:
                    row["open_ms"] = (time.perf_counter_ns() - started) / 1e6
                    row["errno"] = error.errno
                    result["samples"].append(row)
                    raise
                try:
                    row["open_ms"] = (time.perf_counter_ns() - started) / 1e6
                    started = time.perf_counter_ns()
                    info = os.fstat(fd)
                    row["fstat_ms"] = (time.perf_counter_ns() - started) / 1e6
                    row["identity"] = file_identity(info)
                    row["regular"] = stat.S_ISREG(info.st_mode)
                    result["samples"].append(row)
                    if not row["regular"] or row["identity"] != args.expected_file:
                        raise RuntimeError("opened file identity differs from exact expected identity")
                    started = time.perf_counter_ns()
                    check_parent(parent, parent_fd, args.expected_parent)
                    row["parent_check_after_ms"] = (time.perf_counter_ns() - started) / 1e6
                finally:
                    os.close(fd)
        absolute = [row["open_ms"] for row in result["samples"] if row["method"] == "absolute"]
        relative = [row["open_ms"] for row in result["samples"] if row["method"] == "relative"]
        result["summary"] = {
            "absolute_median_ms": statistics.median(absolute),
            "relative_median_ms": statistics.median(relative),
            "relative_minus_absolute_by_pair_ms": [relative[i] - absolute[i] for i in range(args.pairs)],
            "interpretation": "order-balanced same-file opens; parent lstat checks may warm pathname caches; no cold-state or product-benefit claim",
        }
        result["verdict"] = "OBSERVED"
    except Exception as error:
        result["error"] = f"{type(error).__name__}: {error}"
    finally:
        if parent_fd is not None:
            os.close(parent_fd)
        print(json.dumps(result, separators=(",", ":")), flush=True)
    return 0 if result["verdict"] == "OBSERVED" else 2


def supervise(command, deadline_seconds):
    started_monotonic_ns = time.monotonic_ns()
    started_utc_ns = time.time_ns()
    proc = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, start_new_session=True)
    deadline_ns = started_monotonic_ns + int(deadline_seconds * 1e9)
    common = {"worker_pid": proc.pid, "worker_command": command,
              "started_monotonic_ns": started_monotonic_ns, "started_utc_ns": started_utc_ns,
              "reap_budget_ms": 2000}

    def finished(stdout, stderr, timed_out, reaped, kill_sent):
        end_monotonic_ns = time.monotonic_ns()
        return {**common, "exit_code": proc.returncode, "stdout": stdout, "stderr": stderr,
                "timed_out": timed_out, "worker_reaped": reaped, "kill_signal_sent": kill_sent,
                "finished_monotonic_ns": end_monotonic_ns, "finished_utc_ns": time.time_ns(),
                "elapsed_ms": (end_monotonic_ns - started_monotonic_ns) / 1e6}

    try:
        stdout, stderr = proc.communicate(timeout=max(0, (deadline_ns - time.monotonic_ns()) / 1e9))
        return finished(stdout, stderr, False, True, False)
    except subprocess.TimeoutExpired:
        kill_sent = False
        try:
            os.killpg(proc.pid, signal.SIGKILL)
            kill_sent = True
        except ProcessLookupError:
            pass  # The child exited at the deadline; communicate still reaps it.
        try:
            stdout, stderr = proc.communicate(timeout=2)
            reaped = True
        except subprocess.TimeoutExpired:
            stdout, stderr, reaped = b"", b"", False
        return finished(stdout, stderr, True, reaped, kill_sent)


def save_new(path, receipt):
    # One bounded result. Refuse overwrite of an earlier custody record.
    encoded = (json.dumps(receipt, indent=2) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
    try:
        view = memoryview(encoded)
        while view:
            view = view[os.write(fd, view):]
        os.fsync(fd)
    finally:
        os.close(fd)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", required=True, help="exact absolute file path")
    parser.add_argument("--expected-file", type=json.loads,
                        help='JSON [device,inode,size,mtime_ns] from independent identity preflight')
    parser.add_argument("--expected-parent", type=json.loads,
                        help='JSON [device,inode,directory-mode] from independent identity preflight')
    parser.add_argument("--preflight", action="store_true",
                        help="bounded read-only identity preflight; compare its file identity to canonical metadata")
    parser.add_argument("--pairs", type=int, default=4)
    parser.add_argument("--deadline-seconds", type=float, default=15)
    parser.add_argument("--out", help="new private JSON receipt path; required except --worker")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if (not os.path.isabs(args.path) or os.path.normpath(args.path) != args.path
            or not 1 <= args.pairs <= MAX_PAIRS
            or not 0 < args.deadline_seconds <= MAX_DEADLINE_SECONDS):
        parser.error("invalid path, identity, pair count, or absolute deadline")
    if not args.preflight and (
            not isinstance(args.expected_file, list) or len(args.expected_file) != 4
            or not isinstance(args.expected_parent, list) or len(args.expected_parent) != 3
            or any(type(v) is not int or v < 0 for v in args.expected_file + args.expected_parent)):
        parser.error("measurement requires exact nonnegative file and parent identities")
    if args.worker:
        return identity_preflight(args) if args.preflight else worker(args)
    if not args.out or not os.path.isabs(args.out):
        parser.error("--out must be an absolute new receipt path")
    command = [sys.executable, os.path.abspath(__file__), "--worker", "--path", args.path,
               "--pairs", str(args.pairs), "--deadline-seconds", str(args.deadline_seconds)]
    if args.preflight:
        command.append("--preflight")
    else:
        command.extend(["--expected-file", json.dumps(args.expected_file),
                        "--expected-parent", json.dumps(args.expected_parent)])
    with open(__file__, "rb") as source:
        source_hash = hashlib.file_digest(source, "sha256").hexdigest()
    observed = supervise(command, args.deadline_seconds)
    receipt = {"schema": 1, "verdict": "FAIL", "deadline_seconds": args.deadline_seconds,
               "elapsed_ms": observed["elapsed_ms"], "timed_out": observed["timed_out"],
               "worker_reaped": observed["worker_reaped"], "worker_exit_code": observed["exit_code"],
               "worker_pid": observed["worker_pid"], "worker_command": observed["worker_command"],
               "source_sha256": source_hash,
               "started_monotonic_ns": observed["started_monotonic_ns"],
               "finished_monotonic_ns": observed["finished_monotonic_ns"],
               "started_utc_ns": observed["started_utc_ns"], "finished_utc_ns": observed["finished_utc_ns"],
               "reap_budget_ms": observed["reap_budget_ms"],
               "kill_signal_sent": observed["kill_signal_sent"],
               "cleanup_state": "reaped" if observed["worker_reaped"] else "pending_exact_pid_reap",
               "scope": "read-only descriptor opens; no content reads, mount, Finder, or cache reset"}
    if observed["timed_out"]:
        receipt["error"] = ("absolute observation deadline expired; worker reaped"
                            if observed["worker_reaped"] else
                            "absolute observation deadline expired; exact worker PID cleanup remains pending")
    elif len(observed["stdout"]) > 32768 or len(observed["stderr"]) > 4096:
        receipt["error"] = "worker output exceeded reporting bound"
    else:
        try:
            receipt["observation"] = json.loads(observed["stdout"])
            wanted = "IDENTITY" if args.preflight else "OBSERVED"
            if observed["exit_code"] == 0 and receipt["observation"]["verdict"] == wanted:
                receipt["verdict"] = wanted
        except (ValueError, KeyError, TypeError) as error:
            receipt["error"] = f"invalid worker report: {error}"
        if observed["stderr"]:
            receipt["stderr"] = observed["stderr"].decode("utf-8", "replace")
    save_new(args.out, receipt)
    print(json.dumps({"verdict": receipt["verdict"], "receipt": args.out,
                      "timed_out": receipt["timed_out"], "worker_reaped": receipt["worker_reaped"]}))
    return 0 if receipt["verdict"] in ("IDENTITY", "OBSERVED") else 2


if __name__ == "__main__":
    sys.exit(main())
