#!/usr/bin/env python3
"""Qualify recovery, transaction, and TLS regressions and their behavioral oracles.

Run in CI after module download. Source overlays change behavior without
modifying the checkout. A build error is never an accepted negative result.
"""
import json
import os
import signal
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
        test(SDK, ["./pkg/object", "./pkg/meta", "-run", "^TestEtcdTLSConfig"], work / "tls-pass.log", expected_passes=26)
        for package, filename in [("object", "etcd.go"), ("meta", "tkv_etcd.go")]:
            lost_ca = overlay(work, package + "-lost-ca", [(
                "third_party/juicefs/pkg/" + package + "/" + filename,
                'tsinfo.TrustedCAFile = q.Get("cacert")', 'tsinfo.TrustedCAFile = ""')])
            test(SDK, [lost_ca, "./pkg/" + package, "-run", "^" + TLS + "/trusted_CA$"],
                 work / (package + "-negative.log"), (TLS + "/trusted_CA", "certificate signed by unknown authority"))


if __name__ == "__main__":
    main()
