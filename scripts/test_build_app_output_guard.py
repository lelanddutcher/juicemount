#!/usr/bin/env python3
"""Preflight tests on a disposable build-app.sh copy; never invoke a build."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parent / "build-app.sh"


class BuildOutputGuard(unittest.TestCase):
    def setUp(self):
        scratch = Path(os.environ["P96_PRIVATE_TMPDIR"]).resolve(strict=True)
        self.assertTrue(scratch.is_dir() and not scratch.is_symlink())
        self.temp = tempfile.TemporaryDirectory(prefix="build-output-guard-", dir=scratch)
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.project = self.base / "fake-project"
        script = self.project / "scripts/build-app.sh"
        script.parent.mkdir(parents=True)
        shutil.copyfile(SCRIPT, script)
        self.script = script
        self.sentinel = self.base / "build-or-network-command-ran"
        stub_dir = self.base / "stubs"
        stub_dir.mkdir()
        for name in ("go", "curl", "swift", "codesign"):
            stub = stub_dir / name
            stub.write_text("#!/bin/sh\nprintf invoked > '" + str(self.sentinel) + "'\nexit 99\n")
            stub.chmod(0o700)
        self.env = os.environ.copy()
        self.env.update({"PATH": str(stub_dir) + os.pathsep + self.env.get("PATH", ""),
                         "JM_VERSION": "", "JM_COMMIT": "", "JM_BUILD_NUMBER": ""})

    def invoke(self, output=None):
        env = self.env.copy()
        if output is None:
            env.pop("JM_BUILD_DIR", None)
        else:
            env["JM_BUILD_DIR"] = str(output)
        result = subprocess.run(["/bin/bash", str(self.script), "--distribution"],
                                cwd=self.project, env=env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                timeout=5, check=False)
        self.assertFalse(self.sentinel.exists(), result.stdout)
        return result

    def test_unset_keeps_legacy_path_without_starting_build(self):
        result = self.invoke()
        self.assertIn("--distribution requires explicit", result.stdout)
        self.assertFalse((self.project / "build").exists())

    def test_new_and_existing_empty_override_pass_preflight(self):
        new = self.base / "fresh-output"
        empty = self.base / "empty-output"
        empty.mkdir()
        for output in (new, empty):
            with self.subTest(output=output):
                result = self.invoke(output)
                self.assertIn("--distribution requires explicit", result.stdout)
                self.assertNotIn("ERROR: JM_BUILD_DIR", result.stdout)
        self.assertFalse(new.exists())
        self.assertEqual(list(empty.iterdir()), [])

    def test_explicit_empty_relative_project_ancestor_and_default_refused(self):
        for output in ("", "relative-output", self.project, self.project.parent,
                       Path("/"), self.project / "build"):
            with self.subTest(output=output):
                result = self.invoke(output)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("ERROR: JM_BUILD_DIR", result.stdout)
                self.assertNotIn("--distribution requires explicit", result.stdout)

    def test_app_bundle_symlink_nonempty_and_nondirectory_refused(self):
        empty = self.base / "empty"
        empty.mkdir()
        symlink = self.base / "output-link"
        symlink.symlink_to(empty, target_is_directory=True)
        app = self.base / "Existing.app"
        app.mkdir()
        app_alias = self.base / "app-parent-alias"
        app_alias.symlink_to(app, target_is_directory=True)
        nonempty = self.base / "nonempty"
        nonempty.mkdir()
        marker = nonempty / "preserve-me"
        marker.write_text("preserved")
        ordinary_file = self.base / "not-a-directory"
        ordinary_file.write_text("preserved")
        for output in (self.base / "Inside.app/output", self.base / "Existing.APP/output",
                       app_alias / "output", symlink, nonempty, ordinary_file):
            with self.subTest(output=output):
                result = self.invoke(output)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("ERROR: JM_BUILD_DIR", result.stdout)
                self.assertNotIn("--distribution requires explicit", result.stdout)
        self.assertEqual(marker.read_text(), "preserved")
        self.assertEqual(ordinary_file.read_text(), "preserved")

    def test_control_characters_in_raw_or_resolved_path_are_refused(self):
        nonempty = self.base / "nonempty"
        nonempty.mkdir()
        (nonempty / "preserve-me").write_text("preserved")
        target_parent = self.base / "parent\n"
        target_parent.mkdir()
        alias = self.base / "parent-alias"
        alias.symlink_to(target_parent, target_is_directory=True)
        for output in (str(self.project / "build") + "\n", str(nonempty) + "\n",
                       str(self.base / "carriage") + "\r", alias / "fresh-output"):
            with self.subTest(output=output):
                result = self.invoke(output)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("ERROR: JM_BUILD_DIR", result.stdout)
                self.assertNotIn("--distribution requires explicit", result.stdout)
        self.assertEqual((nonempty / "preserve-me").read_text(), "preserved")
        self.assertFalse((target_parent / "fresh-output").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
