#!/usr/bin/env python3
"""Installer command-boundary controls; no app launch, mount, or HOME override.

Run: python3 scripts/test_install.py
A separate owned signed-bundle run is needed to qualify real macOS commands.
"""
import json
import os
from pathlib import Path
import plistlib
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().with_name("install.sh")
RENAME_PROGRAM = '''
function run(argv) {
    if (argv.length !== 2) throw new Error("exclusive rename requires two paths");
    ObjC.bindFunction("renamex_np", ["int", ["char *", "char *", "unsigned int"]]);
    if ($.renamex_np(argv[0], argv[1], 4) !== 0)
        throw new Error("exclusive rename refused; source and destination retained");
}
'''
# Real file operations model partial/failed copies and no-clobber promotion.
# Process/signature/plist results are controlled, never queried from the host.
FAKE = r'''#!/usr/bin/env python3
import json, os, pathlib, plistlib, shutil, sys
P=pathlib.Path
name=P(sys.argv[0]).name
args=sys.argv[1:]
state=P(os.environ["INSTALL_TEST_STATE"])
mode=os.environ.get("INSTALL_TEST_MODE", "")
with (state/"calls").open("a") as f: f.write(json.dumps([name,args])+"\n")
if name=="ps":
    n=int((state/"ps-count").read_text())+1 if (state/"ps-count").exists() else 1
    (state/"ps-count").write_text(str(n))
    if mode=="query-failure" or (mode=="second-query-failure" and n==2): sys.exit(2)
    if mode=="empty-query": sys.exit(0)
    if mode=="malformed-query": print("not a process row");sys.exit(0)
    print("  101 /sbin/launchd")
    if mode=="running" or (mode=="second-running" and n==2): print(" 202 /Applications/JuiceMount.app/Contents/MacOS/JuiceMount")
    if mode=="serving": print(" 203 /private/owned/juicemount-service")
    if mode=="owner-child": print(" 204 /private/App.app/Contents/MacOS/juicefs")
    sys.exit(0)
if name=="plutil":
    data=plistlib.loads(P(args[-1]).read_bytes())
    print(data[args[1]]);sys.exit(0)
if name=="codesign":
    app=P(args[-1])
    if mode=="invalid-source" or (mode=="invalid-stage" and app.parent.name.startswith(".juicemount-install.")): sys.exit(1)
    sys.exit(0)
if name=="ditto":
    src,dst=map(P,args)
    if mode=="copy-failure": dst.mkdir();(dst/"partial").write_text("copy stopped");sys.exit(1)
    shutil.copytree(src,dst,symlinks=True)
    if mode=="copy-mismatch": (dst/"sentinel").write_text("different")
    sys.exit(0)
if name=="stat":
    s=P(args[-1]).stat();print(str(s.st_dev)+":"+str(s.st_ino));sys.exit(0)
if name=="mv":
    # Actual mv -n directory-container semantics, including the reviewed race.
    # This mock must never be substituted for an exclusive native rename.
    assert args[0]=="-n"
    src,dst=map(P,args[1:])
    if mode=="promotion-conflict" and src.name=="new.app":
        dst.mkdir();(dst/"foreign").write_text("do not overwrite")
    if dst.is_dir(): dst=dst/src.name
    if dst.exists() or dst.is_symlink(): sys.exit(0)
    src.rename(dst);sys.exit(0)
if name=="osascript":
    assert args[:3]==["-l","JavaScript","-e"] and len(args)==6
    assert args[3]==__RENAME_PROGRAM__
    src,dst=map(P,args[-2:])
    promotion=src.name=="new.app"
    if dst.name=="previous.app" and src.name!="new.app" and mode=="backup-failure": sys.exit(1)
    if promotion and mode=="promotion-failure": sys.exit(1)
    if promotion and mode=="promotion-conflict": dst.mkdir();(dst/"foreign").write_text("do not overwrite")
    # RENAME_EXCL refuses an existing directory, regular file, or symlink.
    if dst.exists() or dst.is_symlink(): sys.exit(1)
    src.rename(dst);sys.exit(0)
if name in {"pkill","kill","umount","diskutil","launchctl"}:
    raise AssertionError("forbidden operation: "+name)
raise AssertionError("unexpected command: "+name)
'''


class InstallerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="juicemount-install-test-")
        self.root = Path(self.temp.name).resolve()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        fake = self.bin / "fake"
        fake.write_text(FAKE.replace("__RENAME_PROGRAM__", repr(RENAME_PROGRAM)).replace("#!/usr/bin/env python3", "#!" + os.sys.executable, 1))
        fake.chmod(0o755)
        for name in ("ps", "plutil", "codesign", "ditto", "stat", "mv", "osascript", "pkill", "kill", "umount", "diskutil", "launchctl"):
            (self.bin / name).symlink_to(fake)
        self.source = self.root / "Source.app"
        self.dest = self.root / "Installed.app"
        self.bundle(self.source, "new")
        self.bundle(self.dest, "old")

    def tearDown(self):
        self.temp.cleanup()

    def bundle(self, path, sentinel):
        (path / "Contents/MacOS").mkdir(parents=True)
        (path / "Contents/Info.plist").write_bytes(plistlib.dumps({"CFBundleIdentifier": "com.juicemount.app", "CFBundleExecutable": "JuiceMount"}))
        exe = path / "Contents/MacOS/JuiceMount"
        exe.write_text("signed executable fixture\n")
        exe.chmod(0o755)
        (path / "sentinel").write_text(sentinel)

    def run_install(self, mode="", *extra, source=None, destination=None):
        env = os.environ.copy()  # HOME intentionally unchanged.
        env.update(PATH=str(self.bin)+":/usr/bin:/bin", INSTALL_TEST_STATE=str(self.root), INSTALL_TEST_MODE=mode)
        return subprocess.run(["/bin/bash", str(SCRIPT), "--source", str(source or self.source), "--destination", str(destination or self.dest), *extra], env=env, capture_output=True, text=True, timeout=15)

    def stages(self):
        return list(self.root.glob(".juicemount-install.*"))

    def calls(self):
        path = self.root / "calls"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def test_refusal_before_destination_changes(self):
        for mode in ("running", "serving", "owner-child", "query-failure", "empty-query", "malformed-query", "invalid-source"):
            with self.subTest(mode=mode):
                r = self.run_install(mode)
                self.assertNotEqual(r.returncode, 0, r.stdout+r.stderr)
                self.assertEqual((self.dest/"sentinel").read_text(), "old")
                self.assertEqual(self.stages(), [])
                self.assertFalse(any(n in {"ditto", "mv", "osascript", "pkill", "kill", "umount", "launchctl"} for n, _ in self.calls()))

    def test_copy_validation_and_second_process_gate_preserve_old(self):
        for mode in ("copy-failure", "invalid-stage", "copy-mismatch", "second-running", "second-query-failure"):
            with self.subTest(mode=mode):
                (self.root/"ps-count").unlink(missing_ok=True)
                r = self.run_install(mode)
                self.assertNotEqual(r.returncode, 0, r.stdout+r.stderr)
                self.assertEqual((self.dest/"sentinel").read_text(), "old")
                self.assertGreater(len(self.stages()), 0)
                self.assertFalse(any(n in {"mv", "osascript"} for n, _ in self.calls()))

    def test_failed_backup_move_preserves_current_bundle(self):
        r = self.run_install("backup-failure")
        self.assertNotEqual(r.returncode, 0, r.stdout+r.stderr)
        self.assertEqual((self.dest/"sentinel").read_text(), "old")
        self.assertEqual((self.stages()[0]/"new.app/sentinel").read_text(), "new")

    def test_wrong_bundle_identity_is_refused(self):
        plist = self.source/"Contents/Info.plist"
        data = plistlib.loads(plist.read_bytes())
        data["CFBundleIdentifier"] = "com.example.other"
        plist.write_bytes(plistlib.dumps(data))
        self.assertNotEqual(self.run_install().returncode, 0)
        self.assertEqual((self.dest/"sentinel").read_text(), "old")
        self.assertEqual(self.stages(), [])

    def test_failed_promotion_restores_old_and_keeps_stage(self):
        r = self.run_install("promotion-failure")
        self.assertNotEqual(r.returncode, 0, r.stdout+r.stderr)
        self.assertEqual((self.dest/"sentinel").read_text(), "old")
        self.assertEqual((self.stages()[0]/"new.app/sentinel").read_text(), "new")

    def test_conflicting_destination_is_not_overwritten_on_failure(self):
        r = self.run_install("promotion-conflict")
        self.assertNotEqual(r.returncode, 0, r.stdout+r.stderr)
        self.assertEqual((self.dest/"foreign").read_text(), "do not overwrite")
        self.assertEqual(sorted(p.name for p in self.dest.iterdir()), ["foreign"])
        self.assertEqual((self.stages()[0]/"previous.app/sentinel").read_text(), "old")
        self.assertEqual((self.stages()[0]/"new.app/sentinel").read_text(), "new")

    def test_success_retains_old_and_promotes_new(self):
        r = self.run_install()
        self.assertEqual(r.returncode, 0, r.stdout+r.stderr)
        self.assertEqual((self.dest/"sentinel").read_text(), "new")
        self.assertEqual((self.stages()[0]/"previous.app/sentinel").read_text(), "old")
        signatures = [args for name, args in self.calls() if name == "codesign"]
        self.assertEqual(len(signatures), 2)
        for args in signatures:
            self.assertTrue(args[args.index("-R")+1].startswith("=anchor apple generic"))
            self.assertIn("PKUE74YS72", args[args.index("-R")+1])
            self.assertIn('identifier "com.juicemount.app"', args[args.index("-R")+1])
        self.assertEqual(sum(n == "ps" for n, _ in self.calls()), 2)

    def test_destination_named_previous_app_has_distinct_backup(self):
        destination = self.root/"previous.app"
        self.bundle(destination, "old-named-previous")
        r = self.run_install(destination=destination)
        self.assertEqual(r.returncode, 0, r.stdout+r.stderr)
        self.assertEqual((destination/"sentinel").read_text(), "new")
        self.assertEqual((self.stages()[0]/"previous.app/sentinel").read_text(), "old-named-previous")
        self.assertFalse((self.stages()[0]/"new.app").exists())

    def test_fresh_destination(self):
        fresh = self.root / "Fresh.app"
        r = self.run_install(destination=fresh)
        self.assertEqual(r.returncode, 0, r.stdout+r.stderr)
        self.assertEqual((fresh/"sentinel").read_text(), "new")
        self.assertFalse((self.stages()[0]/"previous.app").exists())

    def test_unsafe_paths_and_launchd_refused(self):
        link = self.root / "Link.app"
        link.symlink_to(self.dest, target_is_directory=True)
        cases = [dict(destination=link), dict(source=self.dest), dict(destination=self.source/"Nested.app"), dict(destination=self.root/".."/"Elsewhere.app")]
        for kwargs in cases:
            with self.subTest(kwargs=kwargs):
                self.assertNotEqual(self.run_install(**kwargs).returncode, 0)
        self.assertNotEqual(self.run_install("", "--launchd").returncode, 0)
        self.assertEqual((self.dest/"sentinel").read_text(), "old")
        self.assertEqual(self.stages(), [])
        self.assertEqual(self.calls(), [])


if __name__ == "__main__":
    unittest.main()
