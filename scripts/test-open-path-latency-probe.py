#!/usr/bin/env python3
"""Private local-file checks for the opt-in descriptor-open probe."""

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("probe-open-path-latency.py")
spec = importlib.util.spec_from_file_location("open_path_probe", SCRIPT)
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class OpenPathProbeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.parent = Path(self.tmp.name) / "parent"
        self.parent.mkdir()
        self.file = self.parent / "sample.bin"
        self.file.write_bytes(b"local fixture")

    def command(self, out, parent_identity=None, file_identity=None):
        return [sys.executable, str(SCRIPT), "--path", str(self.file),
                "--expected-file", json.dumps(file_identity or probe.file_identity(self.file.stat())),
                "--expected-parent", json.dumps(parent_identity or probe.parent_identity(self.parent.stat())),
                "--pairs", "4", "--deadline-seconds", "3", "--out", str(out)]

    def test_balanced_exact_local_observation(self):
        out = Path(self.tmp.name) / "observed.json"
        proc = subprocess.run(self.command(out), capture_output=True, text=True, timeout=5)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        receipt = json.loads(out.read_text())
        self.assertEqual(receipt["verdict"], "OBSERVED")
        self.assertTrue(receipt["worker_reaped"])
        self.assertFalse(receipt["timed_out"])
        rows = receipt["observation"]["samples"]
        self.assertEqual(len(rows), 8)
        self.assertEqual([r["method"] for r in rows],
                         ["absolute", "relative", "relative", "absolute"] * 2)
        self.assertEqual({r["slot"] for r in rows if r["method"] == "absolute"}, {0, 1})
        self.assertTrue(all(r["identity"] == probe.file_identity(self.file.stat()) for r in rows))
        self.assertTrue(all(r["regular"] for r in rows))
        self.assertFalse(receipt["observation"]["cache_reset"])

    def test_bounded_identity_preflight(self):
        out = Path(self.tmp.name) / "identity.json"
        proc = subprocess.run([sys.executable, str(SCRIPT), "--preflight", "--path", str(self.file),
                               "--deadline-seconds", "3", "--out", str(out)],
                              capture_output=True, text=True, timeout=5)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        receipt = json.loads(out.read_text())
        self.assertEqual(receipt["verdict"], "IDENTITY")
        self.assertEqual(receipt["observation"]["expected_file"], probe.file_identity(self.file.stat()))
        self.assertEqual(receipt["observation"]["expected_parent"], probe.parent_identity(self.parent.stat()))
        self.assertTrue(receipt["worker_reaped"])

    def test_wrong_parent_identity_fails_closed(self):
        out = Path(self.tmp.name) / "wrong-parent.json"
        wrong = probe.parent_identity(self.parent.stat())
        wrong[1] += 1
        proc = subprocess.run(self.command(out, wrong), capture_output=True, text=True, timeout=5)
        self.assertEqual(proc.returncode, 2)
        receipt = json.loads(out.read_text())
        self.assertEqual(receipt["verdict"], "FAIL")
        self.assertEqual(receipt["observation"]["samples"], [])

    def test_wrong_file_identity_fails_closed(self):
        out = Path(self.tmp.name) / "wrong-file.json"
        wrong = probe.file_identity(self.file.stat())
        wrong[1] += 1
        proc = subprocess.run(self.command(out, file_identity=wrong), capture_output=True, text=True, timeout=5)
        self.assertEqual(proc.returncode, 2)
        receipt = json.loads(out.read_text())
        self.assertEqual(receipt["verdict"], "FAIL")
        self.assertEqual(len(receipt["observation"]["samples"]), 1)

    def test_stuck_worker_is_killed_and_reaped(self):
        observed = probe.supervise([sys.executable, "-c", "import time; time.sleep(10)"], 0.15)
        self.assertTrue(observed["timed_out"])
        self.assertTrue(observed["worker_reaped"])
        self.assertLess(observed["elapsed_ms"], 2000)

    def test_unreaped_timeout_receipt_retains_exact_worker(self):
        class UnreapedWorker:
            pid = 424242
            returncode = None

            def communicate(self, timeout):
                raise subprocess.TimeoutExpired("fake-worker", timeout)

        out = Path(self.tmp.name) / "unreaped.json"
        argv = self.command(out)[1:]  # sys.argv starts with the script, not Python.
        with mock.patch.object(sys, "argv", argv), \
                mock.patch.object(probe.subprocess, "Popen", return_value=UnreapedWorker()), \
                mock.patch.object(probe.os, "killpg") as killpg:
            self.assertEqual(probe.main(), 2)
        killpg.assert_called_once_with(424242, probe.signal.SIGKILL)
        receipt = json.loads(out.read_text())
        self.assertEqual(receipt["verdict"], "FAIL")
        self.assertEqual(receipt["worker_pid"], 424242)
        self.assertEqual(receipt["cleanup_state"], "pending_exact_pid_reap")
        self.assertTrue(receipt["timed_out"])
        self.assertFalse(receipt["worker_reaped"])
        self.assertTrue(receipt["kill_signal_sent"])
        self.assertEqual(receipt["reap_budget_ms"], 2000)
        self.assertEqual(receipt["worker_command"][0], sys.executable)
        self.assertLessEqual(receipt["started_monotonic_ns"], receipt["finished_monotonic_ns"])
        self.assertLessEqual(receipt["started_utc_ns"], receipt["finished_utc_ns"])


if __name__ == "__main__":
    unittest.main()
