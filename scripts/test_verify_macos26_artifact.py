#!/usr/bin/env python3
"""Behavioral tests for the packaged macOS 26 minimum gate.

P96_PRIVATE_TMPDIR points to caller-owned scratch space; no build is needed.
"""

import os
from pathlib import Path
import plistlib
import struct
import subprocess
import sys
import tempfile
import unittest

from importlib.machinery import SourceFileLoader


HERE = Path(__file__).resolve().parent
MODULE = SourceFileLoader("macos26_gate", str(HERE / "verify-macos26-artifact.py")).load_module()


def packed_version(major):
    return major << 16


def macho(major=26, cpu=0x0100000C, subtype=0, metadata=True):
    command = (struct.pack("<IIIIII", 0x32, 24, 1, packed_version(major), packed_version(26), 0)
               if metadata else struct.pack("<II", 0x1B, 24) + bytes(16))
    header = struct.pack("<IIIIIIII", 0xFEEDFACF, cpu, subtype, 2, 1, len(command), 0, 0)
    return header + command


def fat(arm_major=26, intel_major=26, wrong_arch=False):
    arm = macho(arm_major)
    intel = macho(intel_major, 0x01000007)
    offset = 8 + 2 * 20
    table = struct.pack(">II", 0xCAFEBABE, 2)
    table += struct.pack(">IIIII", 0x0100000C, 0, offset, len(arm), 0)
    table += struct.pack(">IIIII", 0x0100000C if wrong_arch else 0x01000007, 0,
                         offset + len(arm), len(intel), 0)
    return table + arm + intel


class VerifyMacOS26Artifact(unittest.TestCase):
    def setUp(self):
        scratch = Path(os.environ["P96_PRIVATE_TMPDIR"]).resolve(strict=True)
        self.assertTrue(scratch.is_dir() and not scratch.is_symlink())
        self.temp = tempfile.TemporaryDirectory(prefix="macos26-verifier-", dir=scratch)
        self.addCleanup(self.temp.cleanup)
        self.app = Path(self.temp.name) / "JuiceMount.app"
        for relative in MODULE.PLISTS:
            path = self.app / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("wb") as stream:
                plistlib.dump({"LSMinimumSystemVersion": "26.0"}, stream)
        for relative in MODULE.EXECUTABLES:
            path = self.app / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(fat())

    def run_gate(self):
        return subprocess.run([sys.executable, "-I", "-B", str(HERE / "verify-macos26-artifact.py"),
                               str(self.app)], text=True, capture_output=True, check=False)

    def test_all_first_party_slices_and_plists_at_26_pass(self):
        result = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_each_first_party_executable_at_14_or_27_is_refused(self):
        for relative in MODULE.EXECUTABLES:
            for major in (14, 27):
                with self.subTest(relative=relative, major=major):
                    path = self.app / relative
                    path.write_bytes(macho(major))
                    result = self.run_gate()
                    self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                    self.assertIn(relative, result.stderr)
                    path.write_bytes(fat())

    def test_missing_metadata_and_missing_binary_are_refused(self):
        relative = MODULE.EXECUTABLES[1]
        path = self.app / relative
        path.write_bytes(macho(metadata=False))
        result = self.run_gate()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("minimum OS metadata", result.stderr)
        path.unlink()
        result = self.run_gate()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn(relative, result.stderr)

    def test_mixed_26_and_14_slices_and_wrong_arch_are_refused(self):
        path = self.app / MODULE.EXECUTABLES[0]
        path.write_bytes(fat(intel_major=14))
        result = self.run_gate()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("expected 26.0.0", result.stderr)
        path.write_bytes(fat(wrong_arch=True))
        result = self.run_gate()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("architecture does not match", result.stderr)

    def test_both_bundle_plists_require_valid_26_minimum(self):
        for relative in MODULE.PLISTS:
            with self.subTest(relative=relative):
                path = self.app / relative
                for value in ("14.0", "27.0", "unknown", "\u0662\u0666.\u0660",
                              "026.00.000", "26.0.1", "26.1", None):
                    with path.open("wb") as stream:
                        plistlib.dump({"LSMinimumSystemVersion": value} if value is not None else {}, stream)
                    result = self.run_gate()
                    self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                    self.assertIn(relative, result.stderr)
                with path.open("wb") as stream:
                    plistlib.dump({"LSMinimumSystemVersion": "26.0"}, stream)

    def test_plist_26_0_0_is_normalized_to_26_0(self):
        for relative in MODULE.PLISTS:
            with (self.app / relative).open("wb") as stream:
                plistlib.dump({"LSMinimumSystemVersion": "26.0.0"}, stream)
        result = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
