#!/usr/bin/env python3
"""Exercise the production hardened-runtime detector without real signing."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parent / "verify-codesign-runtime.sh"


class VerifyCodesignRuntime(unittest.TestCase):
    def setUp(self):
        scratch = Path(os.environ["P96_PRIVATE_TMPDIR"]).resolve(strict=True)
        self.assertTrue(scratch.is_dir() and not scratch.is_symlink())
        self.temp = tempfile.TemporaryDirectory(prefix="codesign-runtime-", dir=scratch)
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.output = self.base / "codesign-output"
        fake_bin = self.base / "bin"
        fake_bin.mkdir()
        codesign = fake_bin / "codesign"
        codesign.write_text("#!/bin/sh\n/bin/cat \"$FAKE_CODESIGN_OUTPUT\" || exit \"$?\"\nexit \"$FAKE_CODESIGN_STATUS\"\n")
        codesign.chmod(0o700)
        self.env = os.environ.copy()
        self.env["PATH"] = str(fake_bin) + os.pathsep + self.env.get("PATH", "")
        self.env["FAKE_CODESIGN_OUTPUT"] = str(self.output)

    def invoke(self, body, status=0):
        self.output.write_text(body)
        env = self.env.copy()
        env["FAKE_CODESIGN_STATUS"] = str(status)
        return subprocess.run(["/bin/bash", str(SCRIPT), str(self.base / "fake-helper")],
                              env=env, text=True, capture_output=True, check=False, timeout=10)

    def test_runtime_with_large_trailing_output_passes(self):
        result = self.invoke("CodeDirectory v=20500 flags=0x10000(runtime) pages=42\n" + "x" * 1000000)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_missing_runtime_fails(self):
        result = self.invoke("CodeDirectory v=20500 flags=0x0(none) pages=42\n")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)

    def test_failed_codesign_with_runtime_looking_output_fails(self):
        result = self.invoke("CodeDirectory v=20500 flags=0x10000(runtime) pages=42\n", status=1)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
