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

    def invoke(self, body, status=0, identifier=None):
        self.output.write_text(body)
        env = self.env.copy()
        env["FAKE_CODESIGN_STATUS"] = str(status)
        args = ["/bin/bash", str(SCRIPT), str(self.base / "fake-helper")]
        if identifier is not None:
            args.append(identifier)
        return subprocess.run(args, env=env, text=True, capture_output=True, check=False, timeout=10)

    def test_runtime_with_large_trailing_output_passes(self):
        result = self.invoke("CodeDirectory v=20500 flags=0x10000(runtime) pages=42\n" + "x" * 1000000)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_missing_runtime_fails(self):
        result = self.invoke("CodeDirectory v=20500 flags=0x0(none) pages=42\n")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)

    def test_failed_codesign_with_runtime_looking_output_fails(self):
        result = self.invoke("CodeDirectory v=20500 flags=0x10000(runtime) pages=42\n", status=1)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)

    # The bundled agent MCP server (com.juicemount.mcp) is checked for its
    # identifier in the same codesign read as its runtime flag.
    MCP = "Executable=/x/juicemount-mcp\nIdentifier=com.juicemount.mcp\nFormat=Mach-O thin (arm64)\n"

    def test_expected_identifier_with_runtime_passes(self):
        result = self.invoke(self.MCP + "CodeDirectory v=20500 flags=0x10000(runtime) pages=42\n", identifier="com.juicemount.mcp")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_other_identifier_fails(self):
        for identifier in ("com.juicemount.unmountd", "com.juicemount", "com.juicemount.mcp.x"):
            with self.subTest(identifier=identifier):
                result = self.invoke(self.MCP + "CodeDirectory v=20500 flags=0x10000(runtime) pages=42\n", identifier=identifier)
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)

    def test_expected_identifier_without_runtime_fails(self):
        result = self.invoke(self.MCP + "CodeDirectory v=20500 flags=0x2(adhoc) pages=42\n", identifier="com.juicemount.mcp")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)

    def test_extra_argument_is_a_usage_error(self):
        self.output.write_text("CodeDirectory v=20500 flags=0x10000(runtime) pages=42\n")
        result = subprocess.run(["/bin/bash", str(SCRIPT), "a", "b", "c"], env=self.env, text=True,
                                capture_output=True, check=False, timeout=10)
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
