#!/usr/bin/env python3
"""Scoped role-aware verifier regression on an unchanged packaged artifact.

P96_SOURCE_APP and P96_PRIVATE_TMPDIR are required. The fixture symlinks
unchanged source bytes; any mutated file is first copied into owned storage.
"""
import hashlib
import os
from pathlib import Path
import plistlib
import runpy
import shutil
import struct
import subprocess
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "verify-build.sh"
HELPER = HERE / "verify-service-stamp.py"
VERSION = os.environ["P96_EXPECTED_VERSION"]
COMMIT = os.environ["P96_EXPECTED_COMMIT"]


def stale_same_length(value):
    return (("a" if value[0] != "a" else "b") + value[1:]).encode()


class VerifySplitService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = Path(os.environ["P96_SOURCE_APP"]).resolve(strict=True)
        cls.private_tmp = Path(os.environ["P96_PRIVATE_TMPDIR"]).resolve(strict=True)
        if not cls.private_tmp.is_dir() or cls.private_tmp.is_symlink():
            raise AssertionError("private temp root must be an existing real directory")
        cls.source_hashes = {
            str(p.relative_to(cls.source)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (
                cls.source / "Contents/MacOS/JuiceMount",
                cls.source / "Contents/MacOS/juicemount-service",
                cls.source / "Contents/MacOS/juicemount-mount-probe",
                cls.source / "Contents/MacOS/juicefs",
                cls.source / "Contents/MacOS/juicemount-unmountd",
                cls.source / "Contents/Library/LaunchDaemons/com.juicemount.unmountd.plist",
                cls.source / "Contents/Info.plist",
                cls.source / "Contents/PlugIns/JuiceMountThumbnails.appex/Contents/Info.plist",
                cls.source / "Contents/PlugIns/JuiceMountThumbnails.appex/Contents/MacOS/JuiceMountThumbnails",
            )
        }

    @classmethod
    def tearDownClass(cls):
        for rel, before in cls.source_hashes.items():
            after = hashlib.sha256((cls.source / rel).read_bytes()).hexdigest()
            if before != after:
                raise AssertionError(f"source artifact changed: {rel}")

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="p96-verifier-", dir=self.private_tmp)
        self.addCleanup(self.temp.cleanup)
        self.app = Path(self.temp.name) / "JuiceMount.app"
        paths = [
            "Contents/MacOS/JuiceMount",
            "Contents/MacOS/juicemount-service",
            "Contents/MacOS/juicemount-mount-probe",
            "Contents/MacOS/juicefs",
            "Contents/MacOS/juicemount-unmountd",
            "Contents/Library/LaunchDaemons/com.juicemount.unmountd.plist",
            "Contents/Info.plist",
            "Contents/PlugIns/JuiceMountThumbnails.appex/Contents/Info.plist",
            "Contents/PlugIns/JuiceMountThumbnails.appex/Contents/MacOS/JuiceMountThumbnails",
        ]
        for rel in paths:
            target = self.app / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.symlink_to(self.source / rel)

    def replace_with_copy(self, rel):
        target = self.app / rel
        source = target.resolve(strict=True)
        target.unlink()
        shutil.copyfile(source, target)
        target.chmod(0o700)
        if os.stat(target).st_ino == os.stat(source).st_ino and os.stat(target).st_dev == os.stat(source).st_dev:
            self.fail("mutation copy aliases original inode")
        return target

    def run_verifier(self):
        env = os.environ.copy()
        env["TMPDIR"] = str(self.private_tmp)
        return subprocess.run(
            ["/bin/bash", str(SCRIPT), "--app", str(self.app), "--version", VERSION, "--commit", COMMIT],
            cwd=HERE.parent,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=40,
            check=False,
        )

    def test_existing_split_artifact_passes_correct_roles(self):
        result = self.run_verifier()
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("service Version/Commit symbols match expected source", result.stdout)
        self.assertIn("build OK", result.stdout)
        self.assertIn("bundled juicefs is signed with the bundle identity", result.stdout)
        self.assertIn("bundled juicefs version: juicefs version 1.3.1+", result.stdout)
        self.assertIn("Info.plist bundled juicefs version: juicefs version 1.3.1+", result.stdout)
        self.assertIn("service has no jmdevjuicefs dev override", result.stdout)

    def test_required_executables_missing_or_nonexecutable(self):
        for rel in ("Contents/MacOS/JuiceMount", "Contents/MacOS/juicemount-service", "Contents/MacOS/juicemount-mount-probe"):
            with self.subTest(rel=rel):
                target = self.app / rel
                original = target.readlink()
                target.unlink()
                missing = self.run_verifier()
                self.assertEqual(missing.returncode, 3, missing.stdout)
                target.write_bytes(b"not executable")
                target.chmod(0o600)
                nonexec = self.run_verifier()
                self.assertEqual(nonexec.returncode, 3, nonexec.stdout)
                target.unlink()
                target.symlink_to(original)

    def test_bundled_juicefs_gate(self):
        juicefs = self.app / "Contents/MacOS/juicefs"
        juicefs.unlink()
        missing = self.run_verifier()
        self.assertEqual(missing.returncode, 1, missing.stdout)
        self.assertIn("bundled juicefs is missing or not executable", missing.stdout)
        # An unpinned juicefs (any other build, e.g. Homebrew's) must not pass.
        juicefs.write_text("#!/bin/sh\necho 'juicefs version 1.3.1+2025-12-02.e0032b2ae5e9'\n")
        juicefs.chmod(0o700)
        unpinned = self.run_verifier()
        self.assertEqual(unpinned.returncode, 1, unpinned.stdout)
        self.assertIn("bundled juicefs signature is invalid", unpinned.stdout)
        self.assertIn("bundled juicefs version: got 'juicefs version 1.3.1+2025-12-02.e0032b2ae5e9'", unpinned.stdout)
        juicefs.unlink()
        juicefs.symlink_to(self.source / "Contents/MacOS/juicefs")

    def test_dev_juicefs_override_build_is_refused(self):
        # A jmdevjuicefs build admits any configured juicefs; it must not ship.
        service = self.app / "Contents/MacOS/juicemount-service"
        service.unlink()
        subprocess.run(["go", "build", "-tags", "jmdevjuicefs", "-o", str(service), "./cmd/juicemount-mount-probe"],
                       cwd=HERE.parent, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        result = self.run_verifier()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("service was built with the jmdevjuicefs dev tag", result.stdout)

    def test_gui_cannot_stand_in_for_engine_service(self):
        service = self.app / "Contents/MacOS/juicemount-service"
        service.unlink()
        service.symlink_to(self.source / "Contents/MacOS/JuiceMount")
        result = self.run_verifier()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("MISSING:", result.stdout)

    def test_stale_symbol_bound_stamp_rejects_appended_expected_text(self):
        service = self.replace_with_copy("Contents/MacOS/juicemount-service")
        inspect = runpy.run_path(str(HELPER))["inspect"]
        stamps, locations = inspect(service)
        self.assertEqual(stamps, {"Version": VERSION, "Commit": COMMIT})
        stale = stale_same_length(VERSION)
        self.assertEqual(len(stale), len(VERSION))
        with service.open("r+b") as stream:
            stream.seek(locations["Version"])
            stream.write(stale)
            stream.seek(0, 2)
            stream.write(b"\x00" + VERSION.encode() + b"\x00" + COMMIT.encode() + b"\x00")
        result = self.run_verifier()
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("service Version/Commit symbols do not match", result.stdout)

        # Restore only the copied .str bytes, then stale the actual Go string
        # header length. The expected text remains present elsewhere in the file.
        with service.open("r+b") as stream:
            stream.seek(locations["Version"])
            stream.write(VERSION.encode())
            stream.seek(locations["VersionHeader"] + 8)
            stream.write((len(VERSION) - 1).to_bytes(8, "little"))
        result = self.run_verifier()
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("service Version/Commit symbols do not match", result.stdout)

        # Restore the header length, then change only the encoded target;
        # keep its NEXT bits and the expected .str bytes intact.
        with service.open("r+b") as stream:
            stream.seek(locations["VersionHeader"] + 8)
            stream.write(len(VERSION).to_bytes(8, "little"))
            stream.seek(locations["VersionHeader"])
            original_pointer = int.from_bytes(stream.read(8), "little")
            stream.seek(locations["VersionHeader"])
            stream.write((original_pointer ^ 1).to_bytes(8, "little"))
        result = self.run_verifier()
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("service Version/Commit symbols do not match", result.stdout)

        # Finally remove the declared fixup command from this private copy.
        # The raw encoded pointer must not be accepted as a direct address.
        with service.open("r+b") as stream:
            stream.seek(locations["VersionHeader"])
            stream.write(original_pointer.to_bytes(8, "little"))
            stream.seek(16)
            ncmds = struct.unpack("<I", stream.read(4))[0]
            command_offset = 32
            for _ in range(ncmds):
                stream.seek(command_offset)
                command, size = struct.unpack("<II", stream.read(8))
                if command == 0x80000034:
                    stream.seek(command_offset)
                    stream.write(struct.pack("<I", 0x80000035))
                    break
                command_offset += size
            else:
                self.fail("fixture lacks declared chained fixups")
        result = self.run_verifier()
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("service Version/Commit symbols do not match", result.stdout)


    def test_stale_commit_symbol_rejects_appended_expected_text(self):
        service = self.replace_with_copy("Contents/MacOS/juicemount-service")
        inspect = runpy.run_path(str(HELPER))["inspect"]
        stamps, locations = inspect(service)
        self.assertEqual(stamps, {"Version": VERSION, "Commit": COMMIT})
        with service.open("r+b") as stream:
            stream.seek(locations["Commit"])
            stream.write(stale_same_length(COMMIT))
            stream.seek(0, 2)
            stream.write(b"\x00" + VERSION.encode() + b"\x00" + COMMIT.encode() + b"\x00")
        result = self.run_verifier()
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("service Version/Commit symbols do not match", result.stdout)

    def test_gui_and_appex_plist_gates_remain(self):
        for rel, label in (
            ("Contents/Info.plist", "bundle version"),
            ("Contents/PlugIns/JuiceMountThumbnails.appex/Contents/Info.plist", "QuickLook extension version"),
        ):
            with self.subTest(rel=rel):
                plist = self.replace_with_copy(rel)
                with plist.open("rb") as stream:
                    body = plistlib.load(stream)
                body["CFBundleShortVersionString"] = stale_same_length(VERSION).decode()
                with plist.open("wb") as stream:
                    plistlib.dump(body, stream)
                result = self.run_verifier()
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertIn(label, result.stdout)
                plist.unlink()
                plist.symlink_to(self.source / rel)


if __name__ == "__main__":
    unittest.main(verbosity=2)
