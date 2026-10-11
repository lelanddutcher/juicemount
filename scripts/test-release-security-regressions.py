#!/usr/bin/env python3
"""Qualify recovery, transaction, and TLS regressions and their behavioral oracles.

Run in CI after module download. Source overlays change behavior without
modifying the checkout. A build error is never an accepted negative result.
"""
import json
import os
import signal
import sys
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
SDK = ROOT / "third_party/juicefs"
RECOVERY = "TestObjKeysRepeatedCrashBeforeResultReissues"
TLS = "TestEtcdTLSConfigTrustHostnameAndClientIdentity"
SETTLEMENT = "TestCachePathConfirmRetainsStoreAndConnectionUntilDriverSettlement"


def test(cwd, args, log, expected_failure=None, expected_passes=None):
    argv = ["go", "test", "-json", "-count=1", *args]
    with log.open("wb") as output:
        child = subprocess.Popen(argv, cwd=cwd, stdout=output,
                                 stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = child.wait(timeout=600)
        finally:
            if child.poll() is None:
                try:
                    os.killpg(child.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    child.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    child.wait()
    events = []
    for line in log.read_text().splitlines():
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    outputs = "".join(e.get("Output", "") for e in events)
    passed = [e for e in events if e.get("Action") == "pass" and e.get("Test")]
    failed = [e for e in events if e.get("Action") == "fail" and e.get("Test")]
    if expected_failure is None:
        valid = code == 0 and len(passed) == expected_passes and not failed
    else:
        name, message = expected_failure
        valid = (code == 1 and message in outputs and
                 any(e["Test"] == name for e in failed) and
                 "[build failed]" not in outputs)
    print(json.dumps({"argv": argv, "cwd": str(cwd), "exit": code,
                      "passed_tests": len(passed), "failed_tests": len(failed),
                      "expected_failure": expected_failure, "qualified": bool(valid)}), flush=True)
    if not valid:
        print(log.read_text(), flush=True)
        raise SystemExit("regression control did not produce the required evidence")


def overlay(work, name, changes):
    replacements = {}
    for i, (relative, before, after) in enumerate(changes):
        source = ROOT / relative
        text = source.read_text()
        if text.count(before) != 1:
            raise SystemExit("overlay premise changed: " + relative)
        target = work / (name + "-" + str(i) + ".go")
        target.write_text(text.replace(before, after))
        replacements[str(source)] = str(target)
    path = work / (name + ".json")
    path.write_text(json.dumps({"Replace": replacements}))
    return "-overlay=" + str(path)


def main():
    # Keep control logs with the CI job; no test service or host state is changed.
    with tempfile.TemporaryDirectory(prefix="jm-security-regression-", dir=os.environ["RUNNER_TEMP"]) as directory:
        work = Path(directory)
        test(ROOT, ["-race", "./internal/manager", "-run", "^" + RECOVERY + "$"], work / "recovery-pass.log", expected_passes=1)
        old_guard = overlay(work, "recovery-old-guard", [(
            "internal/manager/objkeys.go",
            'case next.ObjKeySecret == "" && next.ObjKeyMintSeq > 0 &&',
            'case next.ObjKeySecret == "" && !next.ObjKeyNeedsSecret && next.ObjKeyMintSeq > 0 &&')])
        test(ROOT, [old_guard, "./internal/manager", "-run", "^" + RECOVERY + "$"],
             work / "recovery-negative.log", (RECOVERY, "attempt 2 did not request a new re-create"))
        # The isolated real-SQLite fixture must retain both settlement phases,
        # and still reject a compiled implementation that leaks its connection.
        test(ROOT, ["-race", "./metadata", "-run", "^" + SETTLEMENT + "$"],
             work / "settlement-pass.log", expected_passes=3)
        leaked_conn = overlay(work, "settlement-leaked-connection", [(
            "metadata/cache_path_eligibility.go",
            "\tdefer conn.Close()\n", "\t// Negative control: omit connection release.\n")])
        test(ROOT, [leaked_conn, "./metadata", "-run", "^" + SETTLEMENT + "$/^commit$"],
             work / "settlement-negative.log",
             (SETTLEMENT + "/commit", "SQL connection still owned after Confirm returned: inUse=1"))
        # Witness the failed first pass before testing the production retry.
        # Six trials per mode exercise scheduling; a compiled no-retry mutant
        # must reach that witness and then fail the pending-row completion check.
        symlink = "TestH4OnlineStartRetriesPendingSymlinkFailure"
        selected = "^TestH4OnlineStart(MaterializesPendingOnlySymlink|RetriesPendingSymlinkFailure)$"
        witness = "initial failed attempt witnessed:"
        for mode, flags in (("normal", []), ("race", ["-race"])):
            log = work / ("symlink-" + mode + ".log")
            test(ROOT, [*flags, "-timeout=120s", "-count=6", "./nfs", "-run", selected],
                 log, expected_passes=12)
            if log.read_text().count(witness) != 6:
                raise SystemExit("symlink first-attempt witnesses missing: " + mode)
        retry_disabled = overlay(work, "symlink-no-retry", [(
            "nfs/drainer.go", "symlinkCheckNeeded = d.materializePendingSymlinks()",
            "d.materializePendingSymlinks()\n\t\t\tsymlinkCheckNeeded = false")])
        log = work / "symlink-no-retry.log"
        test(ROOT, [retry_disabled, "-timeout=120s", "./nfs", "-run", "^" + symlink + "$"],
             log, (symlink, 'pending symlink "Blocked/Current" did not reach pending=false'))
        if log.read_text().count(witness) != 1:
            raise SystemExit("symlink negative did not reach the failed-attempt witness")
        test(SDK, ["./pkg/object", "./pkg/meta", "-run", "^TestEtcdTLSConfig"], work / "tls-pass.log", expected_passes=26)
        for package, filename in [("object", "etcd.go"), ("meta", "tkv_etcd.go")]:
            lost_ca = overlay(work, package + "-lost-ca", [(
                "third_party/juicefs/pkg/" + package + "/" + filename,
                'tsinfo.TrustedCAFile = q.Get("cacert")', 'tsinfo.TrustedCAFile = ""')])
            test(SDK, [lost_ca, "./pkg/" + package, "-run", "^" + TLS + "/trusted_CA$"],
                 work / (package + "-negative.log"), (TLS + "/trusted_CA", "certificate signed by unknown authority"))


def farm_cancellation():
    # The observed readiness race was on macOS. This also avoids interpreting
    # an unreaped Linux orphan zombie as a live media-tool descendant.
    if sys.platform != "darwin":
        raise SystemExit("farm cancellation controls require macOS")
    name = "TestCommandContextCancellationKillsToolProcessGroup"
    with tempfile.TemporaryDirectory(prefix="jm-farm-cancellation-", dir=os.environ["RUNNER_TEMP"]) as directory:
        work = Path(directory)

        def witnesses(log, count):
            output = log.read_text()
            live = output.count("live tool descendant witnessed:")
            retired = output.count("tool fixture retired:")
            if live != count or retired != count:
                print(output, flush=True)
                raise SystemExit("farm fixture did not prove live setup and joined retirement")
            print(json.dumps({"fixture": log.name, "live_witnesses": live,
                              "retirement_witnesses": retired}), flush=True)

        for mode, flags in (("normal", []), ("race", ["-race"])):
            log = work / (mode + ".log")
            test(ROOT, [*flags, "-timeout=120s", "-count=6", "./internal/farm", "-run", "^" + name + "$"],
                 log, expected_passes=6)
            witnesses(log, 6)
        parent_only = overlay(work, "parent-only-cancellation", [(
            "internal/farm/command_unix.go",
            "syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)",
            "syscall.Kill(cmd.Process.Pid, syscall.SIGKILL)")])
        log = work / "parent-only-negative.log"
        test(ROOT, [parent_only, "-timeout=120s", "./internal/farm", "-run", "^" + name + "$"],
             log, (name, "survived context cancellation"))
        witnesses(log, 1)


if __name__ == "__main__":
    if sys.argv[1:] == ["--farm-cancellation"]:
        farm_cancellation()
    elif not sys.argv[1:]:
        main()
    else:
        raise SystemExit("unknown regression-control mode")
